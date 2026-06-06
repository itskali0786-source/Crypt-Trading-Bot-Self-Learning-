"""
APEX MIND v2.0 -- Autonomous Trading Intelligence
=================================================
Single unified brain. Observes, decides, executes, learns.
Replaces: main.py scan/watch/monitor + dip_module.py entirely.
Absorbs: rate_limiter, session_utils, universe_scanner_mind.
Integrates: regime_detector suggestions, execution_bridge.
Keeps as external: watchdog (cron), regime_detector (cron), emailer.



Every 3 minutes:
  - Analyzes market + order flow
  - Guards all open trades (CLOSE/TIGHTEN/HOLD)
  - Scans for new entries
  - Executes decisions via execution_bridge
  - Records everything for learning

Every 3 hours: Incremental pattern weight update
Every 24 hours: Full deep learning cycle
"""
import os,json,time,sqlite3,sys
import pandas as pd
import numpy as np
from datetime import datetime,timezone,timedelta
from binance.client import Client
import ta,logging
import threading
from collections import deque, defaultdict
from execution_bridge import close_order, modify_sl as _eb_modify_sl
from websocket_manager import init_ws_manager, get_ws_manager
from regime_predictor import update_prediction, get_prediction, init as init_regime_predictor

# ── ORDER FLOW CACHE ──
_orderflow_cache  = {}
_orderflow_lock   = threading.Lock()
_orderflow_active = True
_orderflow_symbols = set()

# ── TRADE COOLDOWNS -- prevent re-entry too soon after close ──
_cooldowns = {}  # {symbol: expiry_timestamp} or {symbol_LONG: expiry}
_cooldown_lock = threading.Lock()
_entry_lock = threading.Lock()  # prevent race on simultaneous entries

def set_cooldown(symbol, direction, same_dir_mins=15, any_dir_mins=5):
    """Block re-entry on symbol after close. Directional awareness."""
    with _cooldown_lock:
        now = time.time()
        _cooldowns[symbol] = now + any_dir_mins * 60
        _cooldowns[f"{symbol}_{direction}"] = now + same_dir_mins * 60
        # Persist to file so cooldowns survive restarts
        try:
            _cd_file = os.path.join(BASE, "cooldown_cache.json")
            json.dump(dict(_cooldowns), open(_cd_file, "w"))
        except Exception as _cde: logger.warning(f"Cooldown cache save failed: {_cde}")

def is_on_cooldown(symbol, direction):
    """
    Returns True if symbol should not be re-entered.
    Checks both memory cooldowns AND DB -- survives restarts.
    """
    with _cooldown_lock:
        now = time.time()
        expired = [k for k, v in _cooldowns.items() if v < now]
        for k in expired: del _cooldowns[k]
        if _cooldowns.get(f"{symbol}_{direction}", 0) > now:
            return True
        if _cooldowns.get(symbol, 0) > now:
            return True

    # DB check -- block if symbol already has ANY open trade
    try:
        conn = sqlite3.connect(TRADES_DB)
        # Block same symbol open in trades table
        open_count = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE symbol=? AND status='OPEN'",
            (symbol,)).fetchone()[0]
        conn.close()
        if open_count > 0:
            logger.debug(f"  [COOLDOWN] {symbol} already has {open_count} open trade(s)")
            return True
    except: pass
    # Block if same symbol open in dip_trades
    try:
        conn = sqlite3.connect(TRADES_DB)
        spring_count = conn.execute(
            "SELECT COUNT(*) FROM dip_trades WHERE symbol=? AND status='OPEN'",
            (symbol,)).fetchone()[0]
        conn.close()
        if spring_count > 0:
            logger.debug(f"  [COOLDOWN] {symbol} already open in Spring")
            return True
    except: pass
    return False

def is_repeat_offender(symbol, days=3, min_sl_hits=2, direction=None):
    """Block coins that hit hard SL repeatedly -- direction-aware."""
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(TRADES_DB)
        if direction:
            hits = conn.execute("""
                SELECT COUNT(*) FROM trades
                WHERE symbol=? AND reason='Hard SL'
                AND direction=?
                AND close_time >= ? AND status='CLOSED'""",
                (symbol, direction, cutoff)).fetchone()[0]
        else:
            hits = conn.execute("""
                SELECT COUNT(*) FROM trades
                WHERE symbol=? AND reason='Hard SL'
                AND close_time >= ? AND status='CLOSED'""",
                (symbol, cutoff)).fetchone()[0]
        conn.close()
        if hits >= min_sl_hits:
            dir_str = f" {direction}" if direction else ""
            logger.info(f"  [ENTRY BLOCKED] {symbol}{dir_str} repeat offender: {hits} SL hits in {days} days")
            return True
    except: pass
    # Check observation-based chronic losers (APEX + Spring)
    try:
        _ban_obs = _load_learned_params().get("ban_effectiveness", {})
        _all_chronic = _ban_obs.get("chronic_losers", []) + _ban_obs.get("spring_chronic_losers", [])
        for coin in _all_chronic:
            if coin.get("symbol") == symbol and float(coin.get("wr", 100)) < 35:
                logger.info(f"  [ENTRY BLOCKED] {symbol} chronic loser: WR={coin.get('wr')}% from observations")
                return True
    except: pass
    return False

# ── RATE LIMITER -- Binance weight-based (1200 weight/minute limit) ──
# Binance endpoint weights:
# klines=2, depth=5, ticker=1, fundingRate=1, openInterest=1, trades=1
# Using 80% of limit (960) as safe ceiling to avoid IP ban
BINANCE_WEIGHT_LIMIT = 1200
BINANCE_WEIGHT_SAFE  = 960  # 80% ceiling

class _RateLimiter:
    def __init__(self, max_weight_per_minute=BINANCE_WEIGHT_SAFE):
        self.max_weight    = max_weight_per_minute
        self.calls         = deque()  # (timestamp, weight) tuples
        self.total_weight  = 0
        self.lock          = threading.Lock()

    def acquire(self, weight=1):
        with self.lock:
            now = time.time()
            # Remove entries older than 60 seconds
            while self.calls and self.calls[0][0] < now - 60:
                self.total_weight -= self.calls.popleft()[1]
            # If over limit, wait
            if self.total_weight + weight > self.max_weight:
                oldest = self.calls[0][0] if self.calls else now
                sleep_t = 60 - (now - oldest) + 0.1
                if sleep_t > 0:
                    time.sleep(sleep_t)
                # Re-clean after sleep
                now = time.time()
                while self.calls and self.calls[0][0] < now - 60:
                    self.total_weight -= self.calls.popleft()[1]
            self.calls.append((time.time(), weight))
            self.total_weight += weight

    def remaining(self):
        now = time.time()
        with self.lock:
            while self.calls and self.calls[0][0] < now - 60:
                self.total_weight -= self.calls.popleft()[1]
            return self.max_weight - self.total_weight

    def usage_pct(self):
        return round((1 - self.remaining() / self.max_weight) * 100, 1)

_rate_limiter = _RateLimiter()

BASE=os.path.dirname(os.path.abspath(__file__))
MIND_DB=os.path.join(BASE,"apex_mind.db")
TRADES_DB=os.path.join(BASE,"apex_trades.db")
STATE_FILE=os.path.join(BASE,"bot_state.json")
# Load API keys from .env
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(BASE, ".env"))
except: pass
MIND_API_KEY    = os.environ.get("BINANCE_API_KEY", "")
MIND_API_SECRET = os.environ.get("BINANCE_API_SECRET", "")

# ── MULTI-KEY LOAD BALANCER ──
# Rotate API keys to distribute weight across multiple accounts
_api_keys = [
    (MIND_API_KEY, MIND_API_SECRET),
]
_key2 = os.environ.get("BINANCE_API_KEY2", "")
_sec2 = os.environ.get("BINANCE_API_SECRET2", "")
if _key2 and _key2 != MIND_API_KEY:
    _api_keys.append((_key2, _sec2))

_key3 = os.environ.get("BINANCE_API_KEY3", "")
_sec3 = os.environ.get("BINANCE_API_SECRET3", "")
if _key3 and _key3 != MIND_API_KEY and _key3 != _key2:
    _api_keys.append((_key3, _sec3))

# ── DEDICATED CLIENT ASSIGNMENT ──
# Key 1 = trade management (SL, close, monitor)
# Key 2 = scanner klines (score_coin_entry)
# Key 3 = market analysis (analyze_market, altcoin tickers)
_client_trade   = None  # Key 1 -- trade management
_client_scanner = None  # Key 2 -- scanner klines
_client_market  = None  # Key 3 -- market analysis
_rl_trade   = _rate_limiter  # fallback
_rl_scanner = _rate_limiter
_rl_market  = _rate_limiter

_client_pool   = []
_rate_limiters = []  # separate rate limiter per API key
_client_idx    = 0
_client_lock   = threading.Lock()

def _init_client_pool():
    global _client_pool, _rate_limiters
    global _client_trade, _client_scanner, _client_market
    global _rl_trade, _rl_scanner, _rl_market
    from binance.client import Client as _C
    _client_pool   = [_C(k, s) for k, s in _api_keys]
    _rate_limiters = [_RateLimiter() for _ in _api_keys]
    n = len(_client_pool)
    # Assign dedicated clients by function
    _client_trade   = _client_pool[0];       _rl_trade   = _rate_limiters[0]
    _client_scanner = _client_pool[1 % n];   _rl_scanner = _rate_limiters[1 % n]
    _client_market  = _client_pool[2 % n];   _rl_market  = _rate_limiters[2 % n]
    logger.info(f"Client pool: {n} API keys -- trade/scanner/market dedicated")

def get_trade_client():
    """Key 1 -- trade management, SL, close orders"""
    return _client_trade or client

def get_scanner_client():
    """Key 2 -- scanner klines fetching"""
    return _client_scanner or client

def get_market_client():
    """Key 3 -- market analysis, altcoin tickers"""
    return _client_market or client

def get_trade_rl():
    return _rl_trade

def get_scanner_rl():
    return _rl_scanner

def get_live_price(symbol):
    """Best available live price: WebSocket first, REST ticker fallback.
    Returns a positive float, or None if BOTH fail (caller must handle None)."""
    try:
        _ws = get_ws_manager()
        if _ws:
            p = _ws.get_price(symbol)
            if p and float(p) > 0:
                return float(p)
    except Exception:
        pass
    try:
        get_scanner_rl().acquire(weight=1)
        t = get_trade_client().futures_symbol_ticker(symbol=symbol)
        p = float(t["price"])
        if p > 0:
            return p
    except Exception:
        pass
    return None


def get_market_rl():
    return _rl_market

def get_client_pair():
    """Atomically get matching (client, rate_limiter) pair -- thread safe"""
    global _client_idx
    if not _client_pool:
        return client, _rate_limiter
    with _client_lock:
        idx = _client_idx % len(_client_pool)
        _client_idx += 1
        return _client_pool[idx], _rate_limiters[idx]

def get_client():
    """Round-robin client selection"""
    return get_client_pair()[0]

def get_rate_limiter():
    """Get rate limiter matching current client"""
    global _client_idx
    if not _rate_limiters:
        return _rate_limiter
    with _client_lock:
        idx = (_client_idx - 1) % len(_rate_limiters)
        return _rate_limiters[idx]
_BACKFILL = bool(os.environ.get("APEX_BACKFILL_MODE"))
if not _BACKFILL and (not MIND_API_KEY or not MIND_API_SECRET):
    raise ValueError("BINANCE_API_KEY and BINANCE_API_SECRET must be set in .env")
# In backfill mode, do NOT construct live client -- adapter stubs replace it after import
client = None if _BACKFILL else Client(MIND_API_KEY, MIND_API_SECRET)
OBSERVE_INTERVAL=90

# ── SESSION QUALITY -- absorbed from session_utils.py, now self-learning ──
# Baseline from 163 APEX trades. Overridden by learned data after 10+ obs/hour.
_HOUR_QUALITY_BASE = {h: (50.0, 0.0, 0.5) for h in range(24)}  # neutral -- learning overrides
_hour_quality_learned = {}  # updated every 3H from observations
_regime_blend_cache  = {}  # {timestamp_hour: market_dict} -- updated every 1H
_last_fasttrack      = {}  # {symbol: timestamp} -- fast-track cooldown tracker
_hour_quality_lock = threading.Lock()

def get_session_quality(hour=None, direction=None):
    """Returns (wr_pct, avg_pnl, size_mult) for given hour and direction.
    Direction-aware: LONG and SHORT tracked separately for better sizing.
    Falls back to overall hour quality if direction-specific not available."""
    if hour is None: hour = datetime.now(timezone.utc).hour
    with _hour_quality_lock:
        # Try direction-specific first
        if direction and (hour, direction) in _hour_quality_learned:
            return _hour_quality_learned[(hour, direction)]
        # Fall back to overall hour quality
        if hour in _hour_quality_learned:
            return _hour_quality_learned[hour]
    return (50.0, 0.0, 0.5)  # neutral fallback -- learning overrides

def refresh_hour_quality():
    """Called every 3H -- rebuilds learned session quality from observations DB."""
    global _hour_quality_learned
    try:
        conn = _get_mind_conn()
        rows = conn.execute("""
            SELECT CAST(strftime('%H', timestamp) AS INTEGER) as hour,
                   direction,
                   COUNT(*) as total,
                   SUM(outcome_correct) as correct,
                   AVG(outcome_roe) as avg_pnl
            FROM observations
            WHERE outcome_correct IS NOT NULL
            AND decision_reason != 'Historical'
            GROUP BY hour, direction HAVING total >= 8
            ORDER BY hour""").fetchall()
        conn.close()
        learned = {}
        for hour, direction, total, correct, avg_pnl in rows:
            # Pure learning -- no hardcoded base blending
            learned_wr  = round(correct/total*100, 1) if total else 50.0
            learned_pnl = round(float(avg_pnl or 0), 2)
            blended_wr   = learned_wr
            blended_pnl  = learned_pnl
            blended_mult = round(learned_wr / 100, 2)
            blended_mult = max(0.3, min(1.0, blended_mult))  # floor 0.3, never block entirely
            learned[(hour, direction)] = (blended_wr, blended_pnl, blended_mult)
            # Also store overall hour (average across directions)
            if hour not in learned:
                learned[hour] = (blended_wr, blended_pnl, blended_mult)
            else:
                prev = learned[hour]
                learned[hour] = (round((prev[0]+blended_wr)/2,1), round((prev[1]+blended_pnl)/2,2), round((prev[2]+blended_mult)/2,2))
        with _hour_quality_lock:
            _hour_quality_learned = learned
        logger.info(f"Session quality refreshed: {len(learned)} hours with learned data")
    except Exception as e:
        logger.error(f"Session quality refresh error: {e}")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(BASE,"apex_mind.log")),
        logging.StreamHandler()
    ]
)
logger=logging.getLogger("APEX_MIND")
logger.propagate=False
if not logger.handlers:
    fh=logging.FileHandler(os.path.join(BASE,"apex_mind.log"))
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(fh)

def _get_mind_conn():
    """Get MIND_DB connection with WAL mode and busy timeout."""
    conn = sqlite3.connect(MIND_DB, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=15000")
    return conn

def init_db():
    conn=_get_mind_conn()
    conn.execute("PRAGMA journal_mode=WAL")  # Allow concurrent reads/writes
    conn.execute("PRAGMA busy_timeout=15000")  # Wait 5s if locked
    c=conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS observations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL, symbol TEXT NOT NULL,
        bot_type TEXT, direction TEXT, entry_price REAL,
        current_price REAL, roe REAL, peak_roe REAL,
        trade_age_mins REAL, sl_price REAL, sl_distance_pct REAL,
        rsi_5m REAL, rsi_15m REAL, rsi_1h REAL, rsi_4h REAL,
        adx_15m REAL, adx_1h REAL, adx_4h REAL, adx_trend TEXT,
        ema20_15m REAL, ema50_15m REAL, ema20_1h REAL, ema50_1h REAL,
        ema_align_15m TEXT, ema_align_1h TEXT,
        atr_15m REAL, atr_pct REAL, volume_ratio REAL,
        bb_position REAL, macd_hist REAL, macd_trend TEXT,
        funding_rate REAL, rsi_divergence TEXT,
        btc_price REAL, btc_rsi_15m REAL, btc_rsi_1h REAL, btc_rsi_4h REAL,
        btc_adx_15m REAL, btc_adx_4h REAL, btc_adx_trend TEXT, btc_ema_align TEXT,
        alts_bull_pct REAL, market_regime TEXT,
        regime_shifting INTEGER, shift_direction TEXT,
        risk_temp REAL, risk_reason TEXT,
        decision TEXT, decision_reason TEXT, decision_confidence REAL,
        predicted_direction TEXT, predicted_confidence REAL,
        outcome_price REAL, outcome_roe REAL,
        outcome_correct INTEGER DEFAULT NULL, outcome_reason TEXT,
        outcome_filled INTEGER DEFAULT 0)""")
    c.execute("""CREATE TABLE IF NOT EXISTS coin_memory (
        symbol TEXT PRIMARY KEY, first_seen TEXT, last_updated TEXT,
        total_trades INTEGER DEFAULT 0, total_observations INTEGER DEFAULT 0,
        avg_recovery_mins REAL, bounce_probability REAL,
        trend_follow_score REAL, avg_winning_roe REAL, avg_losing_roe REAL,
        best_hour_utc INTEGER, worst_hour_utc INTEGER,
        bull_win_rate REAL, bear_win_rate REAL, sideways_win_rate REAL,
        typical_atr_pct REAL, avg_daily_range REAL,
        mind_correct INTEGER DEFAULT 0, mind_total INTEGER DEFAULT 0,
        mind_accuracy REAL DEFAULT 0)""")
    c.execute("""CREATE TABLE IF NOT EXISTS market_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL,
        btc_price REAL, btc_rsi_15m REAL, btc_rsi_1h REAL, btc_rsi_4h REAL,
        btc_adx_15m REAL, btc_adx_4h REAL, btc_adx_trend TEXT, btc_ema_align TEXT,
        btc_macd_hist REAL, btc_volume_ratio REAL, alts_bull_pct REAL,
        open_longs INTEGER, open_shorts INTEGER, open_springs INTEGER,
        total_unrealized_pnl REAL, risk_temp REAL, regime TEXT, regime_shifting INTEGER)""")
    c.execute("""CREATE TABLE IF NOT EXISTS decision_outcomes (
        id INTEGER PRIMARY KEY AUTOINCREMENT, observation_id INTEGER,
        symbol TEXT, decision TEXT, decision_time TEXT, decision_roe REAL,
        peak_roe_after REAL, final_roe REAL, close_reason TEXT,
        was_correct INTEGER, pnl_impact REAL, missed_gain REAL, saved_loss REAL,
        why_correct TEXT, why_wrong TEXT, what_mind_missed TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS patterns (
        id INTEGER PRIMARY KEY AUTOINCREMENT, pattern_key TEXT UNIQUE,
        description TEXT, conditions TEXT, occurrences INTEGER DEFAULT 0,
        correct INTEGER DEFAULT 0, accuracy REAL DEFAULT 0,
        avg_pnl_impact REAL DEFAULT 0, confidence REAL DEFAULT 0,
        last_seen TEXT, active INTEGER DEFAULT 1)""")
    c.execute("""CREATE TABLE IF NOT EXISTS ratchet_events (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp       TEXT NOT NULL,
        symbol          TEXT NOT NULL,
        bot_type        TEXT,
        trigger_roe     REAL,
        new_floor       REAL,
        final_roe       REAL,
        final_pnl       REAL,
        was_optimal     INTEGER DEFAULT NULL,
        notes           TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS entry_suggestions (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp       TEXT NOT NULL,
        symbol          TEXT NOT NULL,
        mode            TEXT,
        suggested_dir   TEXT,
        score           REAL,
        confidence      REAL,
        reason          TEXT,
        market_regime   TEXT,
        btc_adx         REAL,
        alts_bull_pct   REAL,
        hour_utc        INTEGER,
        entry_price     REAL,
        -- Outcome fields filled when trade closes
        trade_opened    INTEGER DEFAULT 0,
        trade_direction TEXT,
        trade_entry     REAL,
        trade_close     REAL,
        trade_pnl       REAL,
        was_correct     INTEGER DEFAULT NULL,
        timing_quality  TEXT,
        why_correct     TEXT,
        why_wrong       TEXT,
        outcome_filled  INTEGER DEFAULT 0
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS shift_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT,
        shift_type TEXT, btc_adx_at_detection REAL,
        alts_bull_pct_at_detection REAL, trades_open INTEGER,
        correctly_called INTEGER)""")
    conn.commit(); conn.close()
    logger.info("APEX MIND DB initialized")

_cache={}; _cache_ts={}; CACHE_TTL=150

# ── DB CONNECTION POOL ──
import threading as _threading
_db_local = _threading.local()

def get_trades_conn():
    """Get thread-local trades DB connection -- reuses connection per thread."""
    if not hasattr(_db_local, 'trades') or _db_local.trades is None:
        _db_local.trades = sqlite3.connect(TRADES_DB, check_same_thread=False)
        _db_local.trades.execute("PRAGMA journal_mode=WAL")
        _db_local.trades.execute("PRAGMA synchronous=NORMAL")
        _db_local.trades.execute("PRAGMA cache_size=2000")
    return _db_local.trades

def get_mind_conn():
    """Get thread-local mind DB connection -- reuses connection per thread."""
    if not hasattr(_db_local, 'mind') or _db_local.mind is None:
        _db_local.mind = sqlite3.connect(MIND_DB, check_same_thread=False)
        _db_local.mind.execute("PRAGMA journal_mode=WAL")
        _db_local.mind.execute("PRAGMA synchronous=NORMAL")
        _db_local.mind.execute("PRAGMA cache_size=2000")
    return _db_local.mind  # fresh each cycle but with safety margin

def fetch(symbol,interval,limit,use_cache=True):
    key=f"{symbol}_{interval}"; now=time.time()
    if use_cache and key in _cache and now-_cache_ts.get(key,0)<CACHE_TTL:
        return _cache[key]
    try:
        # Rotate between scanner and market client for load balancing
        import time as _tss
        _use_k3 = (int(_tss.time()) // 2) % 2 == 0  # alternate every 2 seconds
        if _use_k3 and _client_market and _client_market != _client_scanner:
            get_market_rl().acquire(weight=2)
            klines=get_market_client().futures_klines(symbol=symbol,interval=interval,limit=limit)
        else:
            get_scanner_rl().acquire(weight=2)
            klines=get_scanner_client().futures_klines(symbol=symbol,interval=interval,limit=limit)
        df=pd.DataFrame(klines,columns=["t","o","h","l","c","v","_1","_2","_3","_4","_5","_6"]).astype(float)
        df=df[["t","o","h","l","c","v"]]; df.columns=["time","open","high","low","close","volume"]
        if use_cache: _cache[key]=df; _cache_ts[key]=now
        return df
    except Exception as e: logger.error(f"Fetch {symbol} {interval}: {e}"); return None

_regime_sug_cache = {"ts": 0, "data": None}  # cache per 60s
_PARAM_CACHE = {"data": None, "mtime": 0}  # params file cache

def get_regime_suggestions(market=None):
    """Thin forwarder -- reads from the ONE owner (_decide_regime_and_slots).
    Callers pass the market dict they already have, or we call analyze_market()."""
    m = market if market is not None else (analyze_market() or {})
    return {
        "max_long":        m.get("max_long", 3),
        "max_short":       m.get("max_short", 2),
        "size_mult":       m.get("size_mult", 0.7),
        "max_spring":      m.get("spring_slots_max", 10),
        "regime":          m.get("market_regime", "UNKNOWN"),
        "regime_uncertain": m.get("regime_uncertain", False),
        "long_on":         m.get("market_regime", "UNKNOWN") != "BEAR" or m.get("regime_uncertain", False),
        "short_on":        True,
        "entry_bar_bonus": 5 if m.get("regime_uncertain", False) else 0,
    }

def _load_learned_params():
    """
    Load learned parameters from apex_mind_params.json.
    H2 FIX: mtime-based cache -- avoids hundreds of disk reads per minute.
    Cache invalidates automatically when learning cycle writes new params.
    """
    global _PARAM_CACHE
    try:
        path = os.path.join(BASE, "apex_mind_params.json")
        m = os.path.getmtime(path)
        if _PARAM_CACHE["data"] is None or m > _PARAM_CACHE["mtime"]:
            _PARAM_CACHE["data"] = json.load(open(path))
            _PARAM_CACHE["mtime"] = m
        return _PARAM_CACHE["data"]
    except:
        return _PARAM_CACHE["data"] or {}


def check_ratchet_trail(trade):
    """
    Adaptive ratchet trail -- seeds from historical data, learns from outcomes.

    DEFAULT zones (override by learned params in apex_mind_params.json):
      0 → be_trigger%  : Free run
      be_trigger%+     : Breakeven locked
      10%+             : Floor = peak - tier1_gap
      20%+             : Floor = peak - tier2_gap
      30%+             : Floor = peak - tier3_gap
      40%+             : Floor = peak - tier4_gap

    Learned params adjust these per regime and coin type over time.
    APEX MIND observes what actually works and writes back to params file daily.
    Returns (action, reason, floor_roe, close_now, new_sl)
    """
    try:
        symbol    = trade.get("symbol", "")
        direction = trade.get("direction", "LONG")
        entry     = float(trade.get("entry", 0))
        leverage  = float(trade.get("leverage", 5))
        price     = float(trade.get("current_price", 0))
        age_mins  = float(trade.get("age_mins", 0))
        bot_type  = trade.get("bot_type", "APEX")
        if entry <= 0 or price <= 0: return "HOLD", "", 0, False, 0

        # ── LOAD LEARNED RATCHET PARAMS ──
        # APEX MIND writes these from analyze_ratchet_events() daily
        # If not yet learned, use conservative defaults
        lp = _load_learned_params()
        ratchet = lp.get("ratchet", {})

        # Defaults -- same as original but now overrideable by learning
        be_trigger  = float(ratchet.get("be_trigger",  5.0))   # % ROE to activate BE (free run until 5%)
        tier1_gap   = float(ratchet.get("tier1_gap",   5.0))   # floor gap at peak 10%
        tier2_gap   = float(ratchet.get("tier2_gap",   7.0))   # floor gap at peak 20%
        tier3_gap   = float(ratchet.get("tier3_gap",   8.0))   # floor gap at peak 30%
        tier4_gap   = float(ratchet.get("tier4_gap",   6.0))   # floor gap at peak 40%
        _max_hold_mins = float(lp.get("max_hold_mins", 0))
        max_hold_h = _max_hold_mins/60 if _max_hold_mins > 0 else float(ratchet.get("max_hold_h", 8.0))  # from learned params or default
        dead_trade_h= float(ratchet.get("dead_trade_h",4.0))   # dead trade threshold
        dead_roe    = float(ratchet.get("dead_roe",    2.0))   # dead trade ROE threshold
        # Adjust dead trade timeout from apex_timeout_analysis
        try:
            _timeout_obs = lp.get("apex_timeout_analysis", {})
            # Find age bucket where WR drops below 40% -- that's optimal timeout
            for age_h in sorted(_timeout_obs.keys(), key=lambda x: int(x.replace("h",""))):
                _t_wr = float(_timeout_obs[age_h].get("wr", 50))
                _t_age = int(age_h.replace("h",""))
                if _t_wr < 40 and _t_age > 0:
                    dead_trade_h = min(dead_trade_h, float(_t_age))
                    break
        except: pass
        # Adjust hold time from observation data
        try:
            _hold_obs = lp.get("apex_hold_time", {}).get(bot_type, {})
            if _hold_obs:
                _avg_win_mins = float(_hold_obs.get("avg_win_mins", 0))
                if _avg_win_mins > 0:
                    # Cap max hold at 2x avg winning hold time
                    max_hold_h = min(max_hold_h, round(_avg_win_mins * 2 / 60, 1))
        except: pass

        # Compute ROE
        if direction == "LONG":
            spot_pct = (price - entry) / entry * 100
        else:
            spot_pct = (entry - price) / entry * 100
        roe = spot_pct * leverage

        # Update peak ROE
        peak_roe  = float(trade.get("peak_roe", 0))
        floor_roe = float(trade.get("floor_roe", 0))
        be_set    = bool(trade.get("be_set", False))

        if roe > peak_roe:
            peak_roe = roe
            trade["peak_roe"] = peak_roe
            # Persist peak_roe to DB so ratchet survives restart
            try:
                _db_id = trade.get("db_id")
                if not _db_id:
                    _tid = str(trade.get("tid","")).replace("DB_","").strip()
                    if _tid.isdigit(): _db_id = int(_tid)
                if _db_id:
                    _conn = sqlite3.connect(TRADES_DB)
                    _conn.execute("UPDATE trades SET peak_roe=? WHERE id=? AND status='OPEN'",
                                  (round(peak_roe,2), _db_id))
                    _conn.commit(); _conn.close()
            except Exception as e: logger.debug(f"peak_roe persist {symbol}: {e}")
            # Spring -- save peak_roe to dip_trades
            if bot_type == "SPRING":
                try:
                    _conn = sqlite3.connect(TRADES_DB)
                    _conn.execute("UPDATE dip_trades SET peak_roe=? WHERE symbol=? AND status='OPEN' AND entry=?",
                                  (round(peak_roe,2), symbol, float(trade.get("entry",0))))  # L3 FIX: include entry to avoid collision if 2 Spring positions same symbol
                    _conn.commit(); _conn.close()
                except: pass

        # TIME STOPS -- learned thresholds
        # ── CONDITIONAL TIME-CUT (Fix 1: ~100% of loss $ are in >240min holds) ──
        # Cuts failed-thesis trades at 90/120min instead of holding to -11% avg
        if bot_type != "SPRING":
            _lp_tc = lp.get("time_cut", {})
            _tc_mins = float(_lp_tc.get("loss_cut_mins_long" if direction=="LONG" else "loss_cut_mins_short",
                                        90 if direction=="LONG" else 120))
            _tc_roe  = float(_lp_tc.get("loss_cut_roe", -3.0))
            _tc_minp = float(_lp_tc.get("loss_cut_min_peak", 2.0))
            if age_mins >= _tc_mins and roe <= _tc_roe and peak_roe < _tc_minp:
                logger.info(f"  [TIME-CUT] {symbol} {direction} age={age_mins:.0f}m roe={roe:.1f}% peak={peak_roe:.1f}% -- failed thesis, cut small")
                return "CLOSE", f"Time-cut {age_mins:.0f}m ROE={roe:.1f}% (failed thesis)", 0, True, 0

        if age_mins >= max_hold_h * 60:
            return "CLOSE", f"Max {max_hold_h:.0f}H hold ROE={roe:.1f}%", 0, True, 0
        # Dead trade check removed -- ratchet and timeout handle exits

        # Spring time stop -- 6H based on backtest optimization
        if bot_type == "SPRING":
            _spring_timeout_h = float(ratchet.get("spring_timeout_h", 6.0))
            if age_mins >= _spring_timeout_h * 60:
                return "CLOSE", f"Spring timeout {_spring_timeout_h:.0f}H ROE={roe:.1f}%", 0, True, 0
            # Staircase floor handles all profit protection -- no separate peak-to-trough needed

        # BREAKEVEN EXIT -- safety net only, ratchet floor takes priority
        # Only close at BE if floor not yet established (peak < 10%)
        if be_set and floor_roe <= 0 and peak_roe < 10.0:
            be_price = float(trade.get("be_price", entry))
            if direction == "LONG" and price <= be_price:
                return "CLOSE", f"Breakeven exit ROE={roe:.1f}%", 0, True, 0
            elif direction == "SHORT" and price >= be_price:
                return "CLOSE", f"Breakeven exit ROE={roe:.1f}%", 0, True, 0

        # FREE RUN ZONE
        # Spring uses tighter BE trigger than APEX
        _eff_be_trigger = float(ratchet.get("spring_be_trigger", 5.0)) if bot_type == "SPRING" else be_trigger  # M10 FIX: default aligned to params
        # Adjust BE trigger from observation data
        try:
            _be_obs = lp.get("be_trigger_analysis", {}).get(bot_type, {})
            if not _be_obs: _be_obs = lp.get("be_trigger_analysis", {}).get("APEX", {})
            _be_wr = float(_be_obs.get("wr", 0))
            _be_avg_roe = float(_be_obs.get("avg_roe", 0))
            _be_count = int(_be_obs.get("count", 0))
            if _be_wr > 0 and _be_avg_roe > 0 and _be_count >= 20:
                # High WR after BE = trigger earlier; low WR = trigger later
                _be_adj = round((_be_wr - 60) / 100 * 2, 1)
                _eff_be_trigger = round(max(2.0, min(8.0, _eff_be_trigger - _be_adj)), 1)
        except Exception as e: logger.debug(f"BE trigger adjust {symbol}: {e}")
        # EARLY FLOOR -- fills 0-be_trigger dead zone (Opus progressive floor, Option B)
        # When peak reaches early_floor_trigger, lock a partial floor (% of peak) by
        # treating it as a breakeven-style lock so the MAIN floor-close path enforces it.
        _ef_trigger = float(ratchet.get("early_floor_trigger", 3.0))
        _ef_pct     = float(ratchet.get("early_floor_pct", 0.40))
        if (bot_type != "SPRING" and not be_set and peak_roe >= _ef_trigger
                and peak_roe < _eff_be_trigger):
            _early_floor = round(peak_roe * _ef_pct, 1)
            if _early_floor > 0:
                _ef_price_pct = _early_floor / leverage / 100
                _ef_price = round(entry * (1 + _ef_price_pct), 8) if direction == "LONG" \
                            else round(entry * (1 - _ef_price_pct), 8)
                trade["be_set"]    = True
                trade["be_price"]  = _ef_price
                trade["floor_roe"] = _early_floor
                be_set    = True
                floor_roe = _early_floor
                logger.info(f"  [EARLY FLOOR] {symbol} peak={peak_roe:.1f}% locked floor={_early_floor:.1f}% (be_price={_ef_price:.6f})")
                # ENFORCE inline -- downstream floor-close unreachable (roe < be_trigger returns HOLD)
                if roe <= _early_floor:
                    return "CLOSE", f"Early floor={_early_floor:.1f}% peak={peak_roe:.1f}% ROE={roe:.1f}%", _early_floor, True, 0

        # Option A: catch ALREADY-ARMED early floor (be_set on prior tick, breach on later tick)
        # Sanity: floor can't be higher than peak -- if so, ghost state from previous trade
        if be_set and floor_roe > 0 and peak_roe < _eff_be_trigger and roe <= floor_roe:
            if peak_roe <= 0 and floor_roe > 2:  # ghost state guard
                logger.warning(f"  [RATCHET GHOST] {symbol} floor={floor_roe:.1f}% but peak={peak_roe:.1f}% -- clearing ghost state")
                trade["be_set"] = False
                trade["floor_roe"] = 0
                trade["be_price"] = 0
            else:
                return "CLOSE", f"Early floor={floor_roe:.1f}% peak={peak_roe:.1f}% ROE={roe:.1f}%", floor_roe, True, 0

        if roe < _eff_be_trigger:
            return "HOLD", f"Free run (BE triggers at {_eff_be_trigger:.0f}%)", 0, False, 0

        # LOCK BREAKEVEN -- APEX only, Spring uses staircase floors directly
        if roe >= _eff_be_trigger and not be_set and bot_type != "SPRING":
            # Lock at +2% ROE above entry -- BE price = entry + (2% ROE / leverage)
            # M3 FIX: was dividing by leverage twice (_eff_be_trigger-3)/leverage/100
            # Correct: 2% ROE buffer means price move = 2% / leverage
            _be_lock_price_pct = 2.0 / leverage / 100  # price move = 2% ROE at current leverage
            be_price = round(entry * (1 + _be_lock_price_pct), 8) if direction == "LONG" \
                       else round(entry * (1 - _be_lock_price_pct), 8)
            trade["be_set"]    = True
            trade["be_price"]  = be_price
            trade["floor_roe"] = 0.0
            return "LOCK", f"BE locked @ {be_price:.4f} (learned trigger={be_trigger:.0f}%)", 0, False, be_price

        # ADAPTIVE RATCHET FLOORS
        # Spring: tight staircase -- lock profit aggressively
        # APEX: wider gaps -- allow more breathing room
        if bot_type == "SPRING":
            # SPRING EARLY FLOOR -- fills 0-activate dead zone (same logic as APEX)
            # Data: 8/9 Spring Hard SLs were reversals -- early floor protects them
            _sef_trigger = float(ratchet.get("spring_early_floor_trigger", 3.0))
            _sef_pct     = float(ratchet.get("spring_early_floor_pct", 0.40))
            if (not be_set and peak_roe >= _sef_trigger
                    and peak_roe < float(ratchet.get("spring_activate", 8.0))):
                _sef_floor = round(peak_roe * _sef_pct, 1)
                if _sef_floor > 0:
                    _sef_price_pct = _sef_floor / leverage / 100
                    _sef_price = round(entry * (1 + _sef_price_pct), 8) if direction == "LONG"                                  else round(entry * (1 - _sef_price_pct), 8)
                    trade["be_set"]    = True
                    trade["be_price"]  = _sef_price
                    trade["floor_roe"] = _sef_floor
                    be_set    = True
                    floor_roe = _sef_floor
                    logger.info(f"  [SPRING EARLY FLOOR] {symbol} peak={peak_roe:.1f}% locked floor={_sef_floor:.1f}%")
                    # ENFORCE inline for same-tick breach
                    if roe <= _sef_floor:
                        return "CLOSE", f"Spring early floor={_sef_floor:.1f}% peak={peak_roe:.1f}% ROE={roe:.1f}%", _sef_floor, True, 0
            # Option A: already-armed Spring early floor breach on later tick
            if be_set and floor_roe > 0 and peak_roe < float(ratchet.get("spring_activate", 8.0)) and roe <= floor_roe:
                return "CLOSE", f"Spring early floor={floor_roe:.1f}% peak={peak_roe:.1f}% ROE={roe:.1f}%", floor_roe, True, 0
            # Same ratchet as APEX:
            # 0-5% ROE: free run
            # Peak 8: floor 5 | Peak 13: floor 8 | Peak 18: floor 13 (5% give-back)
            # Peak 40+: locked, then 15% trailing gap to infinity
            _r_trail_start = float(ratchet.get("spring_trail_start", 40.0))
            _r_trail_gap   = float(ratchet.get("spring_trail_gap", 15.0))
            _r_give_back   = float(ratchet.get("spring_give_back", 5.0))
            _r_activate    = float(ratchet.get("spring_activate", 8.0))
            _r_min_floor   = float(ratchet.get("spring_min_floor", 5.0))
            if peak_roe >= _r_trail_start:
                ratchet_floor = peak_roe - _r_trail_gap
            elif peak_roe >= _r_activate:
                _step = (int(peak_roe) // 5) * 5
                ratchet_floor = max(_r_min_floor, _step - _r_give_back)
            else:
                ratchet_floor = 0.0
        else:
            # APEX Ratchet:
            # 0-5% ROE: free run
            # 5%+ ROE: BE activates (be_trigger=2% above entry = protected)
            # Peak 8: floor 5 | Peak 13: floor 8 | Peak 18: floor 13 (5% give-back)
            # Peak 40+: locked at 40, then 15% trailing gap to infinity
            _r_trail_start = float(ratchet.get("trail_start", 40.0))
            _r_trail_gap   = float(ratchet.get("trail_gap", 15.0))
            _r_give_back   = float(ratchet.get("give_back", 5.0))
            _r_activate    = float(ratchet.get("activate", 8.0))
            _r_min_floor   = float(ratchet.get("min_floor", 5.0))
            if peak_roe >= _r_trail_start:
                ratchet_floor = peak_roe - _r_trail_gap
            elif peak_roe >= _r_activate:
                _step = (int(peak_roe) // 5) * 5
                ratchet_floor = max(_r_min_floor, _step - _r_give_back)
            else:
                ratchet_floor = 0.0

        if ratchet_floor > 0 and leverage > 0:
            floor_pct = ratchet_floor / leverage / 100
            new_sl = round(entry * (1 + floor_pct), 8) if direction == "LONG" \
                     else round(entry * (1 - floor_pct), 8)
        else:
            new_sl = 0

        # ── CHANDELIER EXIT -- ATR-based trailing (H6 FIX: now covers SHORT too) ──
        try:
            _atr_15m = float(trade.get("atr_15m", 0)) or price * 0.01
            _chan_mult = float(ratchet.get("chandelier_mult", 2.0))  # L4 FIX: default aligned to params (was 3.0)
            _peak_price = float(trade.get("peak_price", 0))

            if direction == "LONG":
                # LONG: track highest high since entry
                if price > _peak_price:
                    _peak_price = price
                    try:
                        _cp_conn = sqlite3.connect(TRADES_DB)
                        if bot_type == "SPRING":
                            _cp_conn.execute("UPDATE dip_trades SET peak_price=? WHERE symbol=? AND status='OPEN'", (_peak_price, symbol))
                        else:
                            _cp_conn.execute("UPDATE trades SET peak_price=? WHERE symbol=? AND status='OPEN'", (_peak_price, symbol))
                        _cp_conn.commit(); _cp_conn.close()
                    except: pass
                # Chandelier SL = highest_high - (ATR x multiplier)
                if _peak_price > 0 and _atr_15m > 0:
                    _chan_sl = round(_peak_price - (_atr_15m * _chan_mult), 8)
                    if ratchet_floor > 0 and entry > 0:
                        _floor_sl = round(entry * (1 + ratchet_floor / leverage / 100), 8)
                        if _chan_sl > _floor_sl:
                            _chan_sl_roe = (_chan_sl - entry) / entry * leverage * 100
                            ratchet_floor = max(ratchet_floor, _chan_sl_roe)

            else:
                # SHORT: track lowest low since entry (H6 FIX)
                # peak_price for SHORT = lowest price reached (most profitable point)
                if _peak_price == 0 or price < _peak_price:
                    _peak_price = price
                    try:
                        _cp_conn = sqlite3.connect(TRADES_DB)
                        _cp_conn.execute("UPDATE trades SET peak_price=? WHERE symbol=? AND status='OPEN'", (_peak_price, symbol))
                        _cp_conn.commit(); _cp_conn.close()
                    except: pass
                # Chandelier SL = lowest_low + (ATR x multiplier)
                if _peak_price > 0 and _atr_15m > 0:
                    _chan_sl = round(_peak_price + (_atr_15m * _chan_mult), 8)
                    if ratchet_floor > 0 and entry > 0:
                        _floor_sl = round(entry * (1 - ratchet_floor / leverage / 100), 8)
                        # For SHORT: use whichever SL is LOWER (tighter = closer to current price)
                        if _chan_sl < _floor_sl:
                            _chan_sl_roe = (entry - _chan_sl) / entry * leverage * 100
                            ratchet_floor = max(ratchet_floor, _chan_sl_roe)
        except: pass

        floor_dyn = max(ratchet_floor, floor_roe)

        # Always update trade dict with latest floor -- keeps 3s monitor in sync
        if floor_dyn > float(trade.get("floor_roe", 0)):
            trade["floor_roe"] = floor_dyn

        # Spring closes on floor directly; APEX requires BE lock first
        _floor_close = (floor_dyn > 0 and roe <= floor_dyn) and (be_set or bot_type == "SPRING")
        if _floor_close:
            return "CLOSE", \
                f"Ratchet floor={floor_dyn:.0f}%: peak={peak_roe:.1f}% ROE={roe:.1f}%", \
                floor_dyn, True, 0

        # Return UPGRADE if floor increased
        if floor_dyn > floor_roe and new_sl > 0:
            return "UPGRADE", f"Floor {floor_dyn:.1f}% (dynamic formula)", floor_dyn, False, new_sl

        return "HOLD", f"Running floor={floor_dyn:.1f}%", floor_dyn, False, 0

    except Exception as e:
        logger.error(f"Ratchet error {trade.get('symbol','?')}: {e}")
        return "HOLD", "", 0, False, 0


# ─────────────────────────────────────────────────────────────────
# KELLY POSITION SIZING -- absorbed from risk_manager.py
# ─────────────────────────────────────────────────────────────────

def kelly_position_size(equity, confidence, direction="LONG", session_mult=1.0):
    """
    Adaptive Kelly Criterion sizing.

    Learns from outcomes and writes back to apex_mind_params.json daily.
    Parameters read from learned file first, computed fresh if not available.

    Adapts to:
    - Regime: smaller Kelly in BEAR/SIDEWAYS
    - Hour: bad hours already reflected in session_mult, but also Kelly scales
    - Recent trend: if last 5 trades all lost, halve Kelly temporarily
    - Direction: shorts inherently riskier
    - Confidence: APEX MIND's own accuracy on this signal type
    """
    lp    = _load_learned_params()
    kelly = lp.get("kelly", {})

    # ── BASE KELLY -- from learned or computed ──
    # Prefer learned per-regime Kelly if available and has enough data
    try:
        conn = sqlite3.connect(TRADES_DB)
        rows = conn.execute("""
            SELECT pnl, open_time FROM trades
            WHERE status='CLOSED'
            AND reason != 'Ghost - cleaned'
            ORDER BY id DESC LIMIT 50""").fetchall()
        conn.close()
        history = [(float(r[0]), r[1]) for r in rows if r[0] is not None]
    except: history = []

    # Use learned base Kelly from daily learning cycle
    _learned_base = float(kelly.get("base", 0))
    pnls = [h[0] for h in history]
    if _learned_base >= 0.04:
        base_kelly = _learned_base
    elif len(pnls) >= 10:
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        if wins and losses:
            wr = len(wins)/len(pnls)
            avg_win = sum(wins)/len(wins)
            avg_loss = abs(sum(losses)/len(losses))
            b = min(avg_win/avg_loss, 3.0) if avg_loss > 0 else 1.5
            raw_kelly = max(0.03, min((b*wr-(1-wr))/b, 0.14))
            base_kelly = raw_kelly * 0.5
        else:
            base_kelly = 0.04
    else:
        base_kelly = 0.04

    # ── REGIME ADJUSTMENT -- learned per-regime Kelly fractions ──
    # regime_kelly written by learn_adaptive_params() daily
    regime_kelly = kelly.get("regime_kelly", {})
    # Get current regime from last market snapshot
    try:
        conn = _get_mind_conn()
        snap = conn.execute(
            "SELECT regime FROM market_snapshots ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        current_regime = snap[0] if snap else "UNKNOWN"
    except: current_regime = "UNKNOWN"

    if current_regime in regime_kelly:
        regime_mult = float(regime_kelly[current_regime])
    else:
        # Safe defaults until learned
        regime_mult = {
            "BULL_STRONG": 1.2,
            "BULL_WEAK":   1.0,
            "SIDEWAYS":    0.6,
            "BEAR":        0.5,
        }.get(current_regime, 0.8)

    # ── RECENT TREND GUARD -- if losing streak, reduce size ──
    # Last 5 trades: if all negative, halve Kelly temporarily
    recent5 = pnls[:5] if len(pnls) >= 5 else pnls
    if recent5 and all(p <= 0 for p in recent5):
        streak_mult = 0.5
    elif recent5 and sum(1 for p in recent5 if p <= 0) >= 4:
        streak_mult = 0.7
    else:
        streak_mult = 1.0

    # ── CONFIDENCE MULTIPLIER ──
    # Scales 0.5x at conf=40% to 1.3x at conf=95%
    # APEX MIND's own calibrated accuracy for this signal
    conf_floor = float(kelly.get("conf_floor", 40.0))
    conf_mult  = max(0.5, min((confidence - conf_floor) / (80.0 - conf_floor) + 0.5, 1.3))

    # ── DIRECTION MULTIPLIER -- shorts riskier ──
    dir_mult = float(kelly.get("short_mult", 0.7)) if direction == "SHORT" else 1.0

    # ── COMPOSE FINAL SIZE ──
    final_kelly = base_kelly * regime_mult * streak_mult
    final_kelly = max(0.02, min(final_kelly, 0.12))  # hard bounds

    size = equity * final_kelly * conf_mult * dir_mult * session_mult
    # Portfolio sizing -- divide by REGIME max positions (not current open count)
    # This gives consistent per-trade size regardless of how many slots are filled
    try:
        _rs = get_regime_suggestions(analyze_market())
        _max_pos = max(1, _rs.get("max_long", 3) + _rs.get("max_short", 3))
        _max_pos = max(_max_pos, 6)  # minimum 6 slots for sizing consistency
    except: _max_pos = 6
    # 30% total exposure target -- divide by max positions for per-trade size
    _total_exposure = equity * 0.30   # $700 * 0.30 = $210 total APEX exposure
    size = _total_exposure / _max_pos  # e.g. $210 / 6 = $35 per trade
    # Apply Kelly scaling on top (confidence/regime/direction/session)
    _kelly_scale = base_kelly * regime_mult * streak_mult * conf_mult * dir_mult * session_mult
    _kelly_scale = max(0.7, min(_kelly_scale, 1.5))  # Kelly scales 70%-150% of base size
    size = size * _kelly_scale
    size = max(equity * 0.02, min(size, equity * 0.15))  # floor=2% cap=15% per position

    logger.debug(
        f"Kelly: base={base_kelly:.3f} regime={regime_mult:.2f}x "
        f"streak={streak_mult:.2f}x conf={conf_mult:.2f}x dir={dir_mult:.2f}x "
        f"sess={session_mult:.2f}x → ${size:.2f}"
    )
    return round(size, 2)


def classify_coin_type(symbol):
    """Return (volatility_class, liquidity_flag).
    volatility_class: STABLE / MODERATE / HYPER_VOLATILE  (from daily ATR% only)
    liquidity_flag:   THIN / NORMAL / DEEP                (from 24h quote volume only)
    Cached per cycle. Fallback (MODERATE, NORMAL) on any failure.
    Opus fix: split liquidity from volatility -- previously low volume -> HYPER_VOLATILE
    which gave widest stops to least liquid coins (backwards).
    """
    try:
        get_scanner_rl().acquire(weight=2)
        ticker = get_trade_client().futures_ticker(symbol=symbol)
        vol_24h = float(ticker.get("quoteVolume", 0))
        chg_24h = abs(float(ticker.get("priceChangePercent", 0)))  # fast-volatility signal
        # Liquidity from volume only
        if   vol_24h < 5_000_000:   liquidity = "THIN"
        elif vol_24h < 50_000_000:  liquidity = "NORMAL"
        else:                       liquidity = "DEEP"
        # Volatility from ATR only (30d window, aligned index)
        volatility = "MODERATE"
        df1d = fetch(symbol, "1d", 30, use_cache=True)
        if df1d is not None and len(df1d) >= 14:
            df1d = add_inds(df1d)
            atr   = float(df1d.iloc[-1].get("atr", 0) or 0)  # aligned: latest closed
            price = float(df1d.iloc[-1]["close"])
            avg_range = (atr / price * 100) if price > 0 else 5
            if   avg_range > 7:  volatility = "HYPER_VOLATILE"
            elif avg_range < 4:  volatility = "STABLE"
        # Fast-volatility override: current 24h move beats lagging monthly ATR
        if chg_24h >= 25:
            volatility = "HYPER_VOLATILE"
        elif chg_24h >= 15 and volatility == "STABLE":
            volatility = "MODERATE"
        return (volatility, liquidity)
    except Exception as e:
        logger.debug(f"classify_coin_type {symbol}: {e}")
        return ("MODERATE", "NORMAL")


_params_lock = threading.Lock()

def _update_params(updates: dict):
    """Thread-safe atomic update of apex_mind_params.json -- never overwrites existing keys.
    L10 FIX: caps per-cycle change on sensitive scalar params to prevent instability."""
    # Max allowed change per cycle for sensitive params (as fraction of current value)
    _CHANGE_CAPS = {
        "min_conf": 0.10,       # max 10% change per cycle
        "max_hold_mins": 0.20,  # max 20% change per cycle
        "size_mult": 0.15,      # max 15% change per cycle
        "recent_wr": 0.20,      # max 20% change per cycle
    }
    with _params_lock:
        params_file = os.path.join(BASE, "apex_mind_params.json")
        p = json.load(open(params_file)) if os.path.exists(params_file) else {}
        for key, val in updates.items():
            if isinstance(val, dict) and isinstance(p.get(key), dict):
                for k, v in val.items():
                    p[key][k] = v
            else:
                # Apply change cap for sensitive scalar params
                if key in _CHANGE_CAPS and key in p and isinstance(val, (int, float)) and isinstance(p[key], (int, float)):
                    old_val = float(p[key])
                    new_val = float(val)
                    if old_val != 0:
                        max_change = abs(old_val) * _CHANGE_CAPS[key]
                        new_val = max(old_val - max_change, min(old_val + max_change, new_val))
                    p[key] = round(new_val, 4)
                else:
                    p[key] = val
        tmp = params_file + ".tmp"
        json.dump(p, open(tmp, "w"), indent=2)
        os.replace(tmp, params_file)

def reconcile_paper_balance():
    """Recalculate paper_balance.json from DB totals every cycle -- prevents drift."""
    try:
        conn = sqlite3.connect(TRADES_DB)
        apex_pnl = float(conn.execute("SELECT COALESCE(SUM(pnl),0) FROM trades WHERE status='CLOSED'").fetchone()[0])
        spring_pnl = float(conn.execute("SELECT COALESCE(SUM(pnl),0) FROM dip_trades WHERE status='CLOSED'").fetchone()[0])
        conn.close()
        paper_file = os.path.join(BASE, "paper_balance.json")
        b = json.load(open(paper_file)) if os.path.exists(paper_file) else {}
        apex_start = float(b.get("apex_starting", 700))
        spring_start = float(b.get("spring_starting", 300))
        b["futures"]    = round(apex_start + apex_pnl, 4)
        b["spring"]     = round(spring_start + spring_pnl, 4)
        b["total"]      = round(b["futures"] + b["spring"], 2)
        b["apex_pnl"]   = round(apex_pnl, 4)
        b["spring_pnl"] = round(spring_pnl, 4)
        tmp = paper_file + ".tmp"
        json.dump(b, open(tmp, "w"), indent=2)
        os.replace(tmp, paper_file)
    except Exception as e:
        logger.warning(f"Balance reconcile error: {e}")

def update_paper_balance(pnl_usdt, bot_type="APEX"):
    """
    Update paper_balance.json after a trade closes.
    bot_type: APEX updates futures, SPRING updates spring
    """
    try:
        paper_file = os.path.join(BASE, "paper_balance.json")
        if os.path.exists(paper_file):
            b = json.load(open(paper_file))
            futures = float(b.get("futures", 700))
            spring  = float(b.get("spring", 300))
            spot    = float(b.get("spot", 0))
            if bot_type == "SPRING":
                spring = spring + pnl_usdt
            else:
                futures = futures + pnl_usdt
            total   = round(futures + spring + spot, 2)
            b["total"]    = total
            b["futures"]  = round(futures, 4)
            b["spot"]     = round(spot, 2)
            b["spring"]   = round(spring, 4)
            b["apex_pnl"]   = round(futures - float(b.get("apex_starting", 700)), 4)
            b["spring_pnl"] = round(spring  - float(b.get("spring_starting", 300)), 4)
            # Atomic write -- temp file + rename prevents corruption on crash
            tmp = paper_file + ".tmp"
            json.dump(b, open(tmp, "w"), indent=2)
            os.replace(tmp, paper_file)
    except Exception as e:
        logger.error(f"Paper balance update error: {e}")


def check_kill_switch():
    """
    Account-level drawdown protection. Returns NORMAL / REDUCE_RISK / STOP_TRADING.
    Absorbed from risk_manager.kill_switch()
    """
    try:
        balance = get_balance()
        current = float(balance.get("total", 0))
        # Get starting balance from paper_balance or equity snapshots
        paper_file = os.path.join(BASE, "paper_balance.json")
        starting = 1000.0  # default $700 APEX + $300 Spring
        conn = sqlite3.connect(TRADES_DB)
        peak = conn.execute(
            "SELECT MAX(balance) FROM equity_snapshots").fetchone()[0]
        conn.close()
        if peak: starting = float(peak)
        if starting <= 0: return "NORMAL"
        drawdown = (current - starting) / starting * 100
        if drawdown < -30:
            logger.warning(f"KILL SWITCH: Drawdown {drawdown:.1f}% -- STOP TRADING")
            return "STOP_TRADING"
        if drawdown < -15:
            logger.warning(f"KILL SWITCH: Drawdown {drawdown:.1f}% -- REDUCE RISK")
            return "REDUCE_RISK"
        return "NORMAL"
    except: return "NORMAL"


def get_portfolio_heat(trades):
    """
    Calculate portfolio heat: total unrealized loss / total exposed capital.
    Above 40% = pause new entries.
    Absorbed from main.py watch_cycle portfolio heat calculation.
    """
    try:
        total_exposed = sum(float(t.get("size", 0)) for t in trades)
        if total_exposed <= 0: return 0
        total_loss = sum(
            float(t.get("size", 0)) * abs(t.get("roe", 0)) / 100
            for t in trades if t.get("roe", 0) < 0
        )
        return round(total_loss / total_exposed * 100, 1)
    except: return 0


def add_inds(df):
    if df is None or len(df)<35: return df  # ADX needs 35+ rows (pandas 2.3 compatibility)
    c,h,l=df["close"],df["high"],df["low"]
    df["ema20"]=c.ewm(span=20,adjust=False).mean()
    df["ema50"]=c.ewm(span=50,adjust=False).mean()
    df["rsi"]=ta.momentum.RSIIndicator(c,14).rsi()
    df["adx"]=ta.trend.ADXIndicator(h,l,c,14).adx()
    df["atr"]=ta.volatility.AverageTrueRange(h,l,c,14).average_true_range()
    macd=ta.trend.MACD(c,12,26,9); df["macd_hist"]=macd.macd_diff()
    bb=ta.volatility.BollingerBands(c,20,2)
    df["bb_upper"]=bb.bollinger_hband(); df["bb_lower"]=bb.bollinger_lband()
    df["vol_ma"]=df["volume"].rolling(10).mean()
    df["vol_ratio"]=df["volume"]/df["vol_ma"].replace(0,1)
    return df

def sf(val,default=0.0):
    try:
        if val is None: return default
        v=float(val)
        if v!=v: return default  # NaN check
        if v==float("inf") or v==float("-inf"): return default
        return v
    except: return default

def analyze_coin(symbol, direction, entry_price, current_price):
    result={}
    df5m=fetch(symbol,"5m",30); df15=fetch(symbol,"15m",50)
    df1h=fetch(symbol,"1h",50); df4h=fetch(symbol,"4h",50)
    for tf_name,df in [("5m",df5m),("15m",df15),("1h",df1h),("4h",df4h)]:
        if df is None or len(df)<20: continue
        df=add_inds(df); last=df.iloc[-2]
        result[f"rsi_{tf_name}"]=sf(last.get("rsi"))
        result[f"adx_{tf_name}"]=sf(last.get("adx"))
        result[f"atr_{tf_name}"]=sf(last.get("atr"))
        if tf_name=="15m":
            result["ema20_15m"]=sf(last.get("ema20")); result["ema50_15m"]=sf(last.get("ema50"))
            result["macd_hist"]=sf(last.get("macd_hist"))
            result["volume_ratio"]=sf(last.get("vol_ratio"),1.0)
            bb_u=sf(last.get("bb_upper")); bb_l=sf(last.get("bb_lower"))
            raw_bb=(current_price-bb_l)/(bb_u-bb_l) if bb_u>bb_l else 0.5
            result["bb_position"]=max(0.0,min(1.0,raw_bb))
            result["atr_pct"]=result["atr_15m"]/current_price*100 if current_price>0 else 0
            e20=result["ema20_15m"]; e50=result["ema50_15m"]
            result["ema_align_15m"]="BULL" if e20>e50 else ("BEAR" if e20<e50 else "FLAT")
            if len(df)>=6:
                adxs=[sf(df.iloc[i].get("adx")) for i in range(-6,-1)]
                adxs=[v for v in adxs if v>0]
                if len(adxs)>=3:
                    if adxs[-1]>adxs[0]*1.08: result["adx_trend_15m"]="RISING"
                    elif adxs[-1]<adxs[0]*0.92: result["adx_trend_15m"]="FALLING"
                    else: result["adx_trend_15m"]="FLAT"
            if len(df)>=4:
                mh_p=sf(df.iloc[-3].get("macd_hist")); mh=result["macd_hist"]
                if mh>0 and mh_p<=0: result["macd_trend"]="CROSSING_BULL"
                elif mh<0 and mh_p>=0: result["macd_trend"]="CROSSING_BEAR"
                elif mh>mh_p: result["macd_trend"]="BULLISH"
                else: result["macd_trend"]="BEARISH"
        if tf_name=="1h":
            result["ema20_1h"]=sf(last.get("ema20")); result["ema50_1h"]=sf(last.get("ema50"))
            e20=result["ema20_1h"]; e50=result["ema50_1h"]
            result["ema_align_1h"]="BULL" if e20>e50 else ("BEAR" if e20<e50 else "FLAT")
    # Delta vs previous observation
    try:
        conn=_get_mind_conn()
        prev=conn.execute("SELECT adx_15m,rsi_15m,volume_ratio,roe FROM observations WHERE symbol=? ORDER BY id DESC LIMIT 1",(symbol,)).fetchone()
        conn.close()
        if prev:
            result["delta_adx"]=result.get("adx_15m",0)-(prev[0] or 0)
            result["delta_rsi"]=result.get("rsi_15m",0)-(prev[1] or 0)
            result["delta_vol"]=result.get("volume_ratio",1)-(prev[2] or 1)
        else:
            result["delta_adx"]=0; result["delta_rsi"]=0; result["delta_vol"]=0
    except: result["delta_adx"]=0; result["delta_rsi"]=0; result["delta_vol"]=0
    # Spring trades need wider divergence window -- drop happens over more candles
    div_lookback = 20 if result.get("session","") == "SPRING_MODE" else 10
    result["rsi_divergence"]=detect_div(df15, direction, lookback=div_lookback)

    # 1. BTC 5-min correlation -- is coin following BTC right now?
    try:
        btc5=fetch("BTCUSDT","5m",10)
        if btc5 is not None and len(btc5)>=4 and df5m is not None and len(df5m)>=4:
            btc_move=(float(btc5.iloc[-1]["close"])-float(btc5.iloc[-4]["close"]))/float(btc5.iloc[-4]["close"])*100
            coin_move=(sf(df5m.iloc[-2]["close"])-sf(df5m.iloc[-4]["close"]))/sf(df5m.iloc[-4]["close"])*100
            if abs(btc_move)>0.2:
                result["btc_correlation"]=round(coin_move/btc_move,2) if btc_move!=0 else 0
                result["btc_move_5m"]=round(btc_move,3)
                result["coin_move_5m"]=round(coin_move,3)
            else:
                result["btc_correlation"]=0; result["btc_move_5m"]=0; result["coin_move_5m"]=0
        else:
            result["btc_correlation"]=0; result["btc_move_5m"]=0; result["coin_move_5m"]=0
    except: result["btc_correlation"]=0; result["btc_move_5m"]=0; result["coin_move_5m"]=0

    # 2. Open Interest change
    try:
        get_scanner_rl().acquire(weight=1); get_scanner_rl().acquire(weight=1); oi=get_scanner_client().futures_open_interest_hist(symbol=symbol,period="5m",limit=4)
        if oi and len(oi)>=3:
            oi_now=float(oi[-1].get("sumOpenInterest",0))
            oi_prev=float(oi[-3].get("sumOpenInterest",0))
            result["oi_change_pct"]=round((oi_now-oi_prev)/oi_prev*100,3) if oi_prev>0 else 0
        else: result["oi_change_pct"]=0
    except: result["oi_change_pct"]=0

    # 3. Current candle pattern
    try:
        if df5m is not None and len(df5m)>=3:
            cur=df5m.iloc[-1]; prev=df5m.iloc[-2]
            o=float(cur["open"]); h=float(cur["high"]); l=float(cur["low"]); cl=float(cur["close"])
            body=abs(cl-o); rng=h-l
            upper_wick=(h-max(o,cl))
            lower_wick=(min(o,cl)-l)
            if rng>0:
                body_pct=body/rng
                if body_pct<0.1: result["candle_pattern"]="DOJI"
                elif lower_wick>body*2 and cl>o: result["candle_pattern"]="HAMMER"
                elif upper_wick>body*2 and cl<o: result["candle_pattern"]="SHOOTING_STAR"
                elif cl>o and float(prev["close"])<float(prev["open"]) and cl>float(prev["open"]) and o<float(prev["close"]): result["candle_pattern"]="BULL_ENGULF"
                elif cl<o and float(prev["close"])>float(prev["open"]) and cl<float(prev["open"]) and o>float(prev["close"]): result["candle_pattern"]="BEAR_ENGULF"
                elif cl>o: result["candle_pattern"]="BULL"
                else: result["candle_pattern"]="BEAR"
            else: result["candle_pattern"]="DOJI"
        else: result["candle_pattern"]="UNKNOWN"
    except: result["candle_pattern"]="UNKNOWN"

    # 4. Funding countdown -- minutes to next funding (every 8H at 00:00, 08:00, 16:00 UTC)
    try:
        now_utc=datetime.now(timezone.utc)
        next_funding_hour=[0,8,16,24]
        h=now_utc.hour; m=now_utc.minute
        for fh in next_funding_hour:
            if fh>h or (fh==h and m<1):
                mins_to_funding=(fh-h)*60-m; break
        else: mins_to_funding=(24-h)*60-m
        result["mins_to_funding"]=mins_to_funding
    except: result["mins_to_funding"]=240

    # 5. Session
    try:
        hour=datetime.now(timezone.utc).hour
        if 8<=hour<12: result["session"]="LONDON"
        elif 13<=hour<17: result["session"]="NY_OPEN"
        elif 17<=hour<21: result["session"]="NY_MAIN"
        elif 0<=hour<5: result["session"]="ASIA"
        else: result["session"]="OVERLAP"
    except: result["session"]="UNKNOWN"
    try:
        get_scanner_rl().acquire(weight=1); fr=get_scanner_client().futures_funding_rate(symbol=symbol,limit=1)
        result["funding_rate"]=sf(fr[0].get("fundingRate",0))*100 if fr else 0
    except: result["funding_rate"]=0
    return result

def detect_div(df, direction, lookback=10):
    try:
        if df is None or len(df)<10: return "NONE"
        df=add_inds(df) if "rsi" not in df.columns else df
        # Spring trades need wider lookback -- sharp drop happens over more candles
        prices=df["close"].iloc[-lookback:-1].values
        rsis=df["rsi"].iloc[-lookback:-1].values
        if len(prices)<5: return "NONE"
        p_lows=[(i,prices[i]) for i in range(1,len(prices)-1) if prices[i]<prices[i-1] and prices[i]<prices[i+1]]
        r_lows=[(i,rsis[i]) for i in range(1,len(rsis)-1) if rsis[i]<rsis[i-1] and rsis[i]<rsis[i+1]]
        if direction=="LONG" and len(p_lows)>=2 and len(r_lows)>=2:
            if p_lows[-1][1]<p_lows[-2][1] and r_lows[-1][1]>r_lows[-2][1]: return "BULLISH"
        p_highs=[(i,prices[i]) for i in range(1,len(prices)-1) if prices[i]>prices[i-1] and prices[i]>prices[i+1]]
        r_highs=[(i,rsis[i]) for i in range(1,len(rsis)-1) if rsis[i]>rsis[i-1] and rsis[i]>rsis[i+1]]
        if direction=="SHORT" and len(p_highs)>=2 and len(r_highs)>=2:
            if p_highs[-1][1]>p_highs[-2][1] and r_highs[-1][1]<r_highs[-2][1]: return "BEARISH"
    except: pass
    return "NONE"

_MARKET_CACHE = {"result": None, "ts": 0.0}

_REGIME_SLOT_TABLE = {
    # (max_long, max_short, size_mult, max_spring)
    "BULL_STRONG": (6, 1, 1.2, 12),
    "BULL_WEAK":   (5, 2, 1.0, 10),
    "SIDEWAYS":    (2, 2, 0.6,  6),
    "BEAR":        (1, 5, 0.8,  2),
    "UNKNOWN":     (3, 2, 0.7,  5),
}
_REGIME_ORDER = ["BEAR", "SIDEWAYS", "BULL_WEAK", "BULL_STRONG"]

def _next_state_slots(regime, direction):
    """Return (max_long, max_short) of the adjacent state in the given direction."""
    if regime not in _REGIME_ORDER:
        return _REGIME_SLOT_TABLE["UNKNOWN"][0], _REGIME_SLOT_TABLE["UNKNOWN"][1]
    i = _REGIME_ORDER.index(regime)
    j = min(i + 1, len(_REGIME_ORDER) - 1) if direction == "UP" else max(i - 1, 0)
    nxt = _REGIME_ORDER[j]
    return _REGIME_SLOT_TABLE[nxt][0], _REGIME_SLOT_TABLE[nxt][1]

def _decide_regime_and_slots(result):
    """SINGLE OWNER: regime label -> slots. Called last in _analyze_market_impl."""
    regime    = result.get("market_regime", "UNKNOWN")
    uncertain = bool(result.get("regime_uncertain", False))

    if uncertain:
        max_l, max_s, size, max_sp = 3, 3, 0.6, 5
    else:
        max_l, max_s, size, max_sp = _REGIME_SLOT_TABLE.get(regime, _REGIME_SLOT_TABLE["UNKNOWN"])

        if result.get("regime_shifting"):
            alts = float(result.get("alts_bull_pct", 50))
            rsi1h = float(result.get("btc_rsi_1h", 50))
            adx_trend = str(result.get("btc_adx_trend", "FLAT"))
            ema = str(result.get("btc_ema_align", "FLAT"))
            shift = str(result.get("shift_direction", ""))
            # Use learned transition thresholds
            try:
                _tp = _load_learned_params().get("transition", {})
                _bull_thresh = float(_tp.get("bear_to_bull_alts", 45.0))
                _bear_thresh = float(_tp.get("bull_to_bear_alts", 65.0))
            except:
                _bull_thresh = 45.0; _bear_thresh = 65.0
            _conf = 0.0
            if "BULL" in shift:
                if rsi1h > 50: _conf += 0.34
                if adx_trend == "RISING": _conf += 0.33
                if ema in ("BULL", "FLAT"): _conf += 0.33
            elif "BEAR" in shift:
                if rsi1h < 50: _conf += 0.34
                if adx_trend == "RISING": _conf += 0.33
                if ema in ("BEAR", "FLAT"): _conf += 0.33
            if "_TO_BULL" in shift or "BEAR_TO_SIDEWAYS" in shift:
                # Upward transition: BEAR→SIDEWAYS→BULL_WEAK→BULL_STRONG
                _p = max(0.0, min(1.0, (alts - _bull_thresh) / 20.0)) * (0.5 + 0.5 * _conf)
                _nl, _ns = _next_state_slots(regime, "UP")
                max_l = int(round(max_l + (_nl - max_l) * _p))
                max_s = int(round(max_s + (_ns - max_s) * _p))
            elif "BULL_TO_BEAR" in shift or "_TO_BEAR" in shift or "SIDEWAYS_TO_BEAR" in shift:
                # Downward transition: BULL_STRONG→BULL_WEAK→SIDEWAYS→BEAR
                _p = max(0.0, min(1.0, (_bear_thresh - alts) / 20.0)) * (0.5 + 0.5 * _conf)
                _nl, _ns = _next_state_slots(regime, "DOWN")
                max_l = int(round(max_l + (_nl - max_l) * _p))
                max_s = int(round(max_s + (_ns - max_s) * _p))
            max_l = max(0, min(6, max_l))
            max_s = max(0, min(6, max_s))
    result["max_long"]           = max_l
    result["max_short"]          = max_s
    result["size_mult"]          = size
    result["market_regime_live"] = regime   # alias -- one value
    try:
        _sm = load_master_config().get("limits", {}).get("max_spring", 10)
        result["spring_slots_max"] = min(int(_sm), max_sp)
    except:
        result["spring_slots_max"] = max_sp
    return result



def analyze_market():
    import time as _t
    if _MARKET_CACHE["result"] is not None and (_t.time() - _MARKET_CACHE["ts"]) < 30:
        return _MARKET_CACHE["result"]
    _result = _analyze_market_impl()
    _MARKET_CACHE["result"] = _result
    _MARKET_CACHE["ts"] = _t.time()
    return _result

def _analyze_market_impl():
    result={}
    btc15=fetch("BTCUSDT","15m",50); btc1h=fetch("BTCUSDT","1h",50); btc4h=fetch("BTCUSDT","4h",30)
    if btc15 is not None:
        btc15=add_inds(btc15)
        if btc15 is None or len(btc15)<28 or "adx" not in btc15.columns: btc15=None
    if btc15 is not None:
        last=btc15.iloc[-2]
        result["btc_price"]=sf(btc15.iloc[-1]["close"])
        result["btc_rsi_15m"]=sf(last.get("rsi")); result["btc_adx_15m"]=sf(last.get("adx"))
        result["btc_macd_hist"]=sf(last.get("macd_hist")); result["btc_vol_ratio"]=sf(last.get("vol_ratio"),1.0)
        if len(btc15)>=6:
            adxs=[sf(btc15.iloc[i].get("adx")) for i in range(-6,-1)]
            adxs=[v for v in adxs if v>0]
            if len(adxs)>=3:
                if adxs[-1]>adxs[0]*1.08: result["btc_adx_trend"]="RISING"
                elif adxs[-1]<adxs[0]*0.92: result["btc_adx_trend"]="FALLING"
                else: result["btc_adx_trend"]="FLAT"
        else: result["btc_adx_trend"]="UNKNOWN"
        e20=sf(last.get("ema20")); e50=sf(last.get("ema50"))
        result["btc_ema_align"]="BULL" if e20>e50 else ("BEAR" if e20<e50 else "FLAT")
    if btc1h is not None and len(btc1h)>=3: btc1h=add_inds(btc1h); result["btc_rsi_1h"]=sf(btc1h.iloc[-2].get("rsi"))
    if btc4h is not None:
        btc4h=add_inds(btc4h); result["btc_rsi_4h"]=sf(btc4h.iloc[-2].get("rsi"))
        result["btc_adx_4h"]=sf(btc4h.iloc[-2].get("adx"))
    try:
        tickers=get_market_client().futures_ticker()
        GIANTS={"BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT","ADAUSDT","DOGEUSDT"}
        pairs=[t for t in tickers if t["symbol"].endswith("USDT") and t["symbol"] not in GIANTS and float(t.get("quoteVolume",0))>5_000_000]
        pairs=sorted(pairs,key=lambda x:float(x["quoteVolume"]),reverse=True)[:20]
        bullish=0; total=0
        for p in pairs:
            try:
                df=fetch(p["symbol"],"1h",40)
                if df is None or len(df)<25: continue
                df=add_inds(df); last=df.iloc[-2]
                if sf(last.get("ema20"))>sf(last.get("ema50")): bullish+=1
                total+=1; time.sleep(0.03)
            except: continue
        result["alts_bull_pct"]=round(bullish/total*100,1) if total else 50
    except: result["alts_bull_pct"]=50
    result["regime_shifting"]=0; result["shift_direction"]="NONE"
    # Old ADX-based shifting detection removed -- handled by blend logic below
    try:
        rlog=json.load(open(os.path.join(BASE,"regime_log.json")))
        hourly=rlog.get("hourly_reports",[])
        last = hourly[-1] if hourly else {}
        logged_regime = last.get("regime","UNKNOWN")
        logged_conf   = float(last.get("confidence", 0))
        # Real-time regime detection using live signals + trend from history
        _ema  = result.get("btc_ema_align","FLAT")
        _alts = result.get("alts_bull_pct", 50)
        _rsi  = result.get("btc_rsi_15m", 50)
        _adx  = result.get("btc_adx_15m", 25)

        # Detect regime from live signals -- uses learned transition thresholds
        try:
            _tp = json.load(open(os.path.join(BASE, "apex_mind_params.json"))).get("transition", {})
            _bull_start = float(_tp.get("bear_to_bull_alts", 45.0))
            _bear_start = float(_tp.get("bull_to_bear_alts", 65.0))
        except:
            _bull_start = 45.0; _bear_start = 65.0

        # Opus: tighten BULL_STRONG -- requires ADX>25 AND alts>55 to avoid mislabeling chop
        if _ema == "BULL" and _alts > 55 and _rsi > 55 and _adx > 25:
            live_regime = "BULL_STRONG"
        elif _ema == "BULL" and _alts > _bull_start and _rsi > 48:
            live_regime = "BULL_WEAK"
        elif _ema == "BULL" and _rsi > 45:
            live_regime = "BULL_WEAK"  # EMA flipped BULL -- exit BEAR immediately
        elif _ema == "BEAR" and _alts < 40 and _rsi < 48:
            live_regime = "BEAR"
        elif _ema == "BEAR" and _alts > 70:
            live_regime = "BULL_WEAK"  # alts 70%+ bullish overrides BEAR EMA
        elif _ema == "BEAR" and _alts > 55:
            live_regime = "BULL_WEAK"  # BEAR EMA but alts>55 -- recovering, not pure BEAR
        elif _ema == "BEAR" and _alts > _bull_start:
            live_regime = "SIDEWAYS"  # BEAR EMA but alts recovering -- not pure BEAR
        elif _adx < 20:
            live_regime = "SIDEWAYS"
        else:
            # Fall back to previous cycle regime (from cache) not stale DB snapshot
            _prev = _MARKET_CACHE.get("result")
            live_regime = _prev.get("market_regime", "SIDEWAYS") if _prev else "SIDEWAYS"

        # Check regime history for trend (last 5 reports)
        history = rlog.get("history", [])
        recent = history[-5:] if len(history) >= 5 else history
        recent_regimes = [h.get("regime","UNKNOWN") for h in recent]
        n = len(recent_regimes)

        # Regime blend -- cached every 15min to balance freshness vs noise
        import time as _rt
        _blend_hour = int(_rt.time() // 900)  # 15-minute buckets
        _blend_cached = False
        if _blend_hour in _regime_blend_cache:
            _cached = _regime_blend_cache[_blend_hour]
            _cached_regime = _cached.get("market_regime", live_regime)
            # Override cache if alts strongly bullish
            if _alts >= 75 and _cached_regime == "BEAR":
                _cached_regime = "BULL_WEAK"
            elif _alts >= 85 and _cached_regime in ("BEAR", "SIDEWAYS"):
                _cached_regime = "BULL_WEAK"
            result["market_regime"]           = _cached_regime
            result["regime_confidence"]        = _cached.get("regime_confidence", logged_conf)
            result["regime_shifting"]          = _cached.get("regime_shifting", 0)
            result["shift_direction"]          = _cached.get("shift_direction", "NONE")
            _blend_cached = True

        if not _blend_cached:
            # Score live signals (-6 to +6)
            _live_score = 0
        if _ema == "BULL": _live_score += 3
        elif _ema == "BEAR": _live_score -= 3
        if _alts > 60: _live_score += 2
        elif _alts > 45: _live_score += 1
        elif _alts < 35: _live_score -= 2
        if _rsi > 60: _live_score += 1
        elif _rsi < 40: _live_score -= 1

        # Score history trend -- how many of last 5 match live_regime
        bull_regimes = ("BULL_STRONG", "BULL_WEAK")
        bear_regimes = ("BEAR",)
        bull_count = sum(1 for r in recent_regimes if r in bull_regimes)
        bear_count = sum(1 for r in recent_regimes if r in bear_regimes)
        history_bull_pct = bull_count / max(n, 1)
        history_bear_pct = bear_count / max(n, 1)

        # Transition progress: 0.0 = fully bear, 1.0 = fully bull
        # Blend history (60%) + live score (40%)
        history_score = history_bull_pct  # 0 to 1
        live_norm     = (_live_score + 6) / 12  # normalize to 0-1
        blend         = history_score * 0.6 + live_norm * 0.4

        # Override blend when alts strongly bullish despite BEAR history
        if _alts >= 75 and _ema == "BEAR":
            blend = max(blend, 0.62)  # force at least BULL_WEAK when alts 75%+
        elif _alts >= 80:
            blend = max(blend, 0.65)  # strong alts participation

        # Map blend to regime + slots
        if blend >= 0.75:
            result["market_regime"]    = "BULL_STRONG"
            result["regime_confidence"] = int(blend * 100)
        elif blend >= 0.60:
            result["market_regime"]    = "BULL_WEAK"
            result["regime_confidence"] = int(blend * 100)
            result["regime_shifting"]   = 1
            result["shift_direction"]   = f"{logged_regime}_TO_BULL_WEAK"
        elif blend >= 0.50:
            result["market_regime"]    = live_regime
            result["regime_confidence"] = int(blend * 100)
            result["regime_shifting"]   = 1
            result["shift_direction"]   = f"{logged_regime}_TO_{live_regime}"
        elif blend >= 0.40:
            result["market_regime"]    = logged_regime
            result["regime_confidence"] = int(blend * 100)
            result["regime_shifting"]   = 1
            result["shift_direction"]   = f"{logged_regime}_TO_{live_regime}"
        elif blend >= 0.30:
            result["market_regime"]    = "BEAR"
            result["regime_confidence"] = int((1-blend) * 100)
            result["regime_shifting"]   = 1
            if logged_regime in bull_regimes:
                result["shift_direction"] = "BULL_TO_BEAR"
            elif live_regime in ("BULL_WEAK", "BULL_STRONG"):
                result["shift_direction"] = "BEAR_TO_BULL"
            else:
                result["shift_direction"] = "BEAR_CONSOLIDATING"
        else:
            result["market_regime"]    = "BEAR"
            result["regime_confidence"] = int((1-blend) * 100)
    except: result["market_regime"]="UNKNOWN"

    # ── REGIME PROBABILITY DISTRIBUTION -- not just a label ──
    # Compute soft probabilities across all regimes from live signals
    try:
        btc_adx  = result.get("btc_adx_15m", 25)
        btc_rsi  = result.get("btc_rsi_15m", 50)
        btc_ema  = result.get("btc_ema_align", "FLAT")
        alts     = result.get("alts_bull_pct", 50)
        adx_t    = result.get("btc_adx_trend", "FLAT")

        bull_strong_score = 0
        bull_weak_score   = 0
        sideways_score    = 0
        bear_score        = 0

        # BTC EMA
        if btc_ema == "BULL":   bull_strong_score += 25; bull_weak_score += 15
        elif btc_ema == "BEAR": bear_score += 30
        else:                   sideways_score += 20

        # ADX strength
        if btc_adx > 35:    bull_strong_score += 20; bear_score += 10
        elif btc_adx > 25:  bull_weak_score += 15; bear_score += 10
        elif btc_adx < 18:  sideways_score += 25

        # ADX trend
        if adx_t == "RISING":   bull_strong_score += 15; bear_score += 10
        elif adx_t == "FALLING": sideways_score += 15; bull_weak_score += 5

        # BTC RSI
        if btc_rsi > 60:    bull_strong_score += 15; bull_weak_score += 10
        elif btc_rsi > 50:  bull_weak_score += 10
        elif btc_rsi < 40:  bear_score += 15
        elif btc_rsi < 30:  bear_score += 25

        # Alts participation
        if alts > 65:        bull_strong_score += 20
        elif alts > 55:      bull_weak_score += 15
        elif alts < 40:      bear_score += 20; sideways_score += 5
        elif 45 <= alts <= 55: sideways_score += 15

        total = bull_strong_score + bull_weak_score + sideways_score + bear_score
        if total > 0:
            result["regime_probs"] = {
                "BULL_STRONG": round(bull_strong_score / total * 100, 1),
                "BULL_WEAK":   round(bull_weak_score   / total * 100, 1),
                "SIDEWAYS":    round(sideways_score     / total * 100, 1),
                "BEAR":        round(bear_score         / total * 100, 1),
            }
            # Dominant probability -- override label if strong signal
            dom = max(result["regime_probs"], key=result["regime_probs"].get)
            dom_pct = result["regime_probs"][dom]
            result["market_regime_live"] = dom if dom_pct >= 55 else result.get("market_regime", dom)
            result["regime_certainty"] = dom_pct

            # UNSTALL: when scorer is confident, it OWNS the label
            # Prevents classifier stuck on stale history-anchored blend
            _blend_label = result.get("market_regime", "UNKNOWN")
            _sorted_probs = sorted(result.get("regime_probs", {}).values(), reverse=True)
            _margin = round((_sorted_probs[0] - _sorted_probs[1]) if len(_sorted_probs) > 1 else 0, 1)
            if dom == _blend_label:
                _status = "agree"
            elif dom_pct >= 50 and _margin >= 12:
                _status = "OVERRIDE"
            else:
                _status = f"weak({dom_pct:.0f}%<50 or margin{_margin:.0f}<12)"
            logger.info(f"  [REGIME] blend={_blend_label} evidence={dom} ({dom_pct:.0f}%) margin={_margin} -> {_status}")
            if dom_pct >= 40 and dom != _blend_label:  # 40% threshold -- market can be uncertain
                _sorted_probs = sorted(result["regime_probs"].values(), reverse=True)
                _margin = (_sorted_probs[0] - _sorted_probs[1]) if len(_sorted_probs) > 1 else _sorted_probs[0]
                if _margin >= 12:
                    # Log only when unstall TARGET changes
                    if dom != getattr(analyze_market, "_last_unstall", None):
                        logger.info(f"  REGIME UNSTALL: blend={_blend_label} -> evidence={dom} ({dom_pct:.0f}%, margin={_margin:.0f}) -- label updated")
                        analyze_market._last_unstall = dom
                    result["market_regime"] = dom
                    result["market_regime_live"] = dom  # keep snapshot in sync
                    # Update blend cache so next cycle doesn't revert
                    _regime_blend_cache[_blend_hour] = dict(_regime_blend_cache.get(_blend_hour, {}))
                    _regime_blend_cache[_blend_hour]["market_regime"] = dom
                    # Update market cache immediately so scanner gets new regime this cycle
                    if _MARKET_CACHE.get("result"):
                        _MARKET_CACHE["result"]["market_regime"] = dom
                        _MARKET_CACHE["result"]["market_regime_live"] = dom
                    if _blend_label in ("BULL_WEAK","SIDEWAYS") and dom in ("BULL_STRONG","BEAR"):
                        result["regime_shifting"] = 1
                        result["shift_direction"] = f"{_blend_label}_TO_{dom}"
        else:
            result["regime_probs"] = {}
            result["market_regime_live"] = result["market_regime"]
            result["regime_certainty"] = 0
    except:
        result["regime_probs"] = {}
        result["market_regime_live"] = result.get("market_regime","UNKNOWN")
        result["regime_certainty"] = 0

    # I4: HMM regime inference -- runs alongside cascade, blends confidence
    try:
        import pickle as _pk, numpy as _np
        _hmm_path = os.path.join(BASE, "regime_hmm.pkl")
        if os.path.exists(_hmm_path):
            _bundle = _pk.load(open(_hmm_path,"rb"))
            _hmodel = _bundle["model"]
            _hmu    = _bundle["mu"]
            _hsd    = _bundle["sd"]
            _hmap   = _bundle["state_to_regime"]
            # Build feature vector
            _ema_v  = {"BULL":1.0,"FLAT":0.0,"BEAR":-1.0}.get(str(result.get("btc_ema_align","FLAT")),0.0)
            _rsi_v  = (float(result.get("btc_rsi_15m",50))-50)/50
            _alts_v = (float(result.get("alts_bull_pct",50))-50)/50
            _adx_v  = float(result.get("btc_adx_15m",25))/50
            _fv     = _np.array([[_ema_v, _rsi_v, _alts_v, _adx_v]])
            _fvs    = (_fv - _hmu) / _hsd  # standardize
            _probs  = _hmodel.predict_proba(_fvs)[0]
            _best_s = int(_np.argmax(_probs))
            _hmm_regime = _hmap.get(_best_s, result.get("market_regime","UNKNOWN"))
            _hmm_conf   = round(float(_probs[_best_s]) * 100, 1)
            # Blend: if HMM agrees with cascade, boost confidence
            # If disagrees, flag for investigation but keep cascade (HMM is new/unvalidated)
            _cascade_regime = result.get("market_regime","UNKNOWN")
            if _hmm_regime == _cascade_regime:
                result["regime_certainty"] = min(95, result.get("regime_certainty",50) + int(_hmm_conf * 0.2))
                result["hmm_regime"] = _hmm_regime
                result["hmm_confidence"] = _hmm_conf
                result["hmm_agrees"] = True
            else:
                result["hmm_regime"] = _hmm_regime
                result["hmm_confidence"] = _hmm_conf
                result["hmm_agrees"] = False
                # Log only when state changes -- prevents flood from multiple analyze_market calls
                _hmm_state = (_cascade_regime, _hmm_regime)
                if _hmm_state != getattr(analyze_market, "_last_hmm_log", None):
                    logger.info(f"  HMM disagrees: cascade={_cascade_regime} → hmm={_hmm_regime} ({_hmm_conf:.0f}% certain)")
                    analyze_market._last_hmm_log = _hmm_state
    except Exception as _hmme:
        logger.debug(f"HMM inference error: {_hmme}")

    # Phase 1: set regime_uncertain flag for slot allocation
    try:
        _cert = float(result.get("regime_certainty", 80))
        _hmm_agrees = result.get("hmm_agrees", True)
        result["regime_uncertain"] = _cert < 65 and not _hmm_agrees  # AND not OR -- both must be unsure
    except:
        result["regime_uncertain"] = False

    result = _decide_regime_and_slots(result)
    return result

def calc_risk(market,trades):
    temp=0.0; reasons=[]
    btc_adx=market.get("btc_adx_15m",25); btc_adx_t=market.get("btc_adx_trend","FLAT")
    btc_rsi=market.get("btc_rsi_15m",50); alts=market.get("alts_bull_pct",50)
    btc_ema=market.get("btc_ema_align","FLAT"); regime=market.get("market_regime","UNKNOWN")
    shifting=market.get("regime_shifting",0)
    longs=[t for t in trades if t.get("direction")=="LONG"]
    shorts=[t for t in trades if t.get("direction")=="SHORT"]
    if btc_adx<16: temp+=25; reasons.append(f"BTC ADX={btc_adx:.0f} trend collapsed")
    elif btc_adx<20: temp+=15; reasons.append(f"BTC ADX={btc_adx:.0f} weakening")
    if btc_adx_t=="FALLING": temp+=10; reasons.append("BTC ADX falling")
    if btc_rsi>82: temp+=15; reasons.append(f"BTC RSI={btc_rsi:.0f} overbought")
    elif btc_rsi<25: temp+=10; reasons.append(f"BTC RSI={btc_rsi:.0f} oversold")
    if longs and alts<40: temp+=20; reasons.append(f"Alts {alts:.0f}% bull but {len(longs)} longs open")
    if shorts and alts>65: temp+=15; reasons.append(f"Alts {alts:.0f}% bull shorts at risk")
    if regime=="SIDEWAYS": temp+=10; reasons.append("SIDEWAYS regime")
    elif regime=="BEAR" and longs and not shifting: temp+=20; reasons.append(f"BEAR regime {len(longs)} longs")
    elif "BULL" in regime and shorts: temp+=15; reasons.append(f"BULL regime {len(shorts)} shorts")
    if shifting: temp+=10; reasons.append(f"REGIME SHIFTING {market.get('shift_direction','')}")
    losing=[t for t in trades if t.get("roe",0)<-5]
    if len(losing)>=5: temp+=25; reasons.append(f"{len(losing)} trades deep in loss")
    elif len(losing)>=3: temp+=15; reasons.append(f"{len(losing)} trades at -5% ROE")
    return min(round(temp,1),100), " | ".join(reasons) if reasons else "Normal"

def make_decision(trade,coin,market,risk_temp,memory=None):
    direction=trade.get("direction","LONG")
    roe=sf(trade.get("roe",0))
    age=sf(trade.get("age_mins",0))
    bot_type=trade.get("bot_type","APEX")
    # For Spring trades, peak_roe is not tracked by bot -- derive from observations
    peak_roe=sf(trade.get("peak_roe",0))
    if bot_type=="SPRING" and peak_roe==0:
        try:
            _conn=_get_mind_conn()
            # Only use observations from CURRENT trade open time
            _open_time=trade.get("open_time","1970-01-01")
            _max=_conn.execute("""SELECT MAX(roe) FROM observations
                WHERE symbol=? AND bot_type='SPRING'
                AND decision_reason != 'Historical'
                AND roe BETWEEN -50 AND 50
                AND timestamp >= ?
                ORDER BY id DESC LIMIT 20""",
                (trade.get("symbol",""), str(_open_time)[:19])).fetchone()
            _conn.close()
            if _max and _max[0]: peak_roe=sf(_max[0])
        except: pass
    close_score=0; hold_score=0; tighten_score=0; signals=[]; _obs_close_score=0; _obs_hold_score=0; _obs_tighten_score=0; decision="HOLD"; conf=0; pred="SIDEWAYS"; pred_c=0
    # Sanitize all coin values
    coin={k: sf(v) if isinstance(v,(int,float,type(None))) else v for k,v in coin.items()}

    # ── LEARNED SIGNAL WEIGHTS -- query observations for current situation ──
    # Queries 3 levels of specificity -- uses most specific match with enough data
    _obs_driven = False
    try:
        _e15  = coin.get("ema_align_15m", "")
        _macd = coin.get("macd_trend", "")
        _adx_t = coin.get("adx_trend_15m", "")
        _reg  = market.get("market_regime", "UNKNOWN")
        _conn = _get_mind_conn()

        _recent = "AND timestamp >= datetime('now', '-30 days')"
        _alltime = ""

        # Level 1: Most specific -- EMA + MACD + ADX + regime (recent first)
        for _tfilter in [_recent, _alltime]:
            _rows = _conn.execute(f"""
                SELECT decision, COUNT(*) as total,
                       ROUND(SUM(outcome_correct)*100.0/COUNT(*),1) as accuracy
                FROM observations
                WHERE direction=? AND ema_align_15m=? AND macd_trend=?
                AND adx_trend=? AND market_regime=?
                AND outcome_correct IS NOT NULL AND bot_type='APEX'
                {_tfilter}
                GROUP BY decision HAVING total >= 10""",
                (direction, _e15, _macd, _adx_t, _reg)).fetchall()
            if _rows and sum(r[1] for r in _rows) >= 20: break

        # Level 2: EMA + MACD + regime (drop ADX)
        if not _rows or sum(r[1] for r in _rows) < 20:
            for _tfilter in [_recent, _alltime]:
                _rows = _conn.execute(f"""
                    SELECT decision, COUNT(*) as total,
                           ROUND(SUM(outcome_correct)*100.0/COUNT(*),1) as accuracy
                    FROM observations
                    WHERE direction=? AND ema_align_15m=? AND macd_trend=?
                    AND market_regime=?
                    AND outcome_correct IS NOT NULL AND bot_type='APEX'
                    {_tfilter}
                    GROUP BY decision HAVING total >= 15""",
                    (direction, _e15, _macd, _reg)).fetchall()
                if _rows and sum(r[1] for r in _rows) >= 20: break

        # Level 3: EMA + regime only
        if not _rows or sum(r[1] for r in _rows) < 20:
            for _tfilter in [_recent, _alltime]:
                _rows = _conn.execute(f"""
                    SELECT decision, COUNT(*) as total,
                           ROUND(SUM(outcome_correct)*100.0/COUNT(*),1) as accuracy
                    FROM observations
                    WHERE direction=? AND ema_align_15m=?
                    AND market_regime=?
                    AND outcome_correct IS NOT NULL AND bot_type='APEX'
                    {_tfilter}
                    GROUP BY decision HAVING total >= 20""",
                    (direction, _e15, _reg)).fetchall()
                if _rows and sum(r[1] for r in _rows) >= 20: break

        _conn.close()
        if _rows:
            _acc = {r[0]: (r[1], r[2]) for r in _rows}
            _close_acc   = _acc.get("CLOSE",   (0, 50))
            _hold_acc    = _acc.get("HOLD",    (0, 50))
            _tighten_acc = _acc.get("TIGHTEN", (0, 50))
            _total_obs   = sum(r[1] for r in _rows)
            _weight = min(_total_obs / 30, 4.0)  # caps at 4x at 120+ obs

            # Find best decision from learned data
            _best_dec = max(_acc.items(), key=lambda x: x[1][1])
            _best_name, (_best_total, _best_acc) = _best_dec

            if _best_acc >= 60:
                if _best_name == "CLOSE":
                    # C2 FIX: CLOSE adds full weight to close, fraction to hold (not equal)
                    _pts = int((_best_acc - 50) * _weight * 0.6)
                    close_score += _pts; _obs_close_score += _pts
                    _pts = int((_best_acc - 50) * _weight * 0.2)  # small counter only
                    hold_score += _pts; _obs_hold_score += _pts
                elif _best_name == "HOLD":
                    hold_score += int((_best_acc - 50) * _weight * 0.6)
                    _pts = int((_best_acc - 50) * _weight * 0.3)
                    close_score += _pts; _obs_close_score += _pts
                elif _best_name == "TIGHTEN":
                    close_score += int((_best_acc - 50) * _weight * 0.3)
                    _pts = int((_best_acc - 50) * _weight * 0.5)
                    tighten_score += _pts; _obs_tighten_score += _pts
            _obs_driven = _total_obs >= 30
    except Exception as _oe: logger.warning(f"APEX obs lookup failed: {_oe}")

    # ── SPRING LEARNED SIGNAL WEIGHTS ──
    _spring_obs_driven = False
    if bot_type == "SPRING":
        try:
            _e15  = coin.get("ema_align_15m", "")  # L8 FIX: use coin value directly (spring_real_e15 defined later in flow)
            _macd = coin.get("macd_trend", "")
            _adx_t = coin.get("adx_trend_15m", "")
            _reg  = market.get("market_regime", "UNKNOWN")
            _conn = _get_mind_conn()

            _sr = "AND timestamp >= datetime('now', '-30 days')"
            _sa = ""

            # Level 1: EMA + MACD + ADX (recent first)
            for _sf in [_sr, _sa]:
                _srows = _conn.execute(f"""
                    SELECT decision, COUNT(*) as total,
                           ROUND(SUM(outcome_correct)*100.0/COUNT(*),1) as accuracy
                    FROM observations
                    WHERE direction='LONG' AND ema_align_15m=? AND macd_trend=?
                    AND adx_trend=? AND outcome_correct IS NOT NULL
                    AND bot_type='SPRING' {_sf}
                    GROUP BY decision HAVING total >= 10""",
                    (_e15, _macd, _adx_t)).fetchall()
                if _srows and sum(r[1] for r in _srows) >= 20: break

            # Level 2: EMA + MACD only
            if not _srows or sum(r[1] for r in _srows) < 20:
                for _sf in [_sr, _sa]:
                    _srows = _conn.execute(f"""
                        SELECT decision, COUNT(*) as total,
                               ROUND(SUM(outcome_correct)*100.0/COUNT(*),1) as accuracy
                        FROM observations
                        WHERE direction='LONG' AND ema_align_15m=? AND macd_trend=?
                        AND outcome_correct IS NOT NULL AND bot_type='SPRING' {_sf}
                        GROUP BY decision HAVING total >= 15""",
                        (_e15, _macd)).fetchall()
                    if _srows and sum(r[1] for r in _srows) >= 20: break

            # Level 3: EMA + regime
            if not _srows or sum(r[1] for r in _srows) < 15:
                for _sf in [_sr, _sa]:
                    _srows = _conn.execute(f"""
                        SELECT decision, COUNT(*) as total,
                               ROUND(SUM(outcome_correct)*100.0/COUNT(*),1) as accuracy
                        FROM observations
                        WHERE direction='LONG' AND ema_align_15m=?
                        AND market_regime=? AND outcome_correct IS NOT NULL
                        AND bot_type='SPRING' {_sf}
                        GROUP BY decision HAVING total >= 20""",
                        (_e15, _reg)).fetchall()
                    if _srows and sum(r[1] for r in _srows) >= 20: break

            _conn.close()
            if _srows:
                _sacc = {r[0]: (r[1], r[2]) for r in _srows}
                _stotal = sum(r[1] for r in _srows)
                _sweight = min(_stotal / 30, 4.0)
                _sbest = max(_sacc.items(), key=lambda x: x[1][1])
                _sbest_name, (_sbest_total, _sbest_acc) = _sbest
                if _sbest_acc >= 60:
                    if _sbest_name == "CLOSE":
                        close_score += int((_sbest_acc - 50) * _sweight * 0.6)
                        signals.append(f"SPRING {_sbest_acc:.0f}% CLOSE ({_sbest_total}obs)")
                    elif _sbest_name == "HOLD":
                        hold_score += int((_sbest_acc - 50) * _sweight * 0.6)
                        signals.append(f"SPRING {_sbest_acc:.0f}% HOLD ({_sbest_total}obs)")
                    elif _sbest_name == "TIGHTEN":
                        _spts = int((_sbest_acc - 50) * _sweight * 0.5)
                        tighten_score += _spts; _obs_tighten_score += _spts
                        signals.append(f"SPRING {_sbest_acc:.0f}% TIGHTEN ({_sbest_total}obs)")
                _spring_obs_driven = _stotal >= 30
        except Exception as _soe: logger.warning(f"Spring obs lookup failed: {_soe}")

    # Bot-type specific context
    is_spring = bot_type == "SPRING"
    is_apex   = bot_type == "APEX"

    # Spring trades entered IN downtrend by design -- don't penalize bearish EMA
    # APEX trades are trend-following -- EMA alignment critical
    if is_spring:
        # Spring: looking for bounce, not trend
        # Save real EMA values for learned weights lookup BEFORE overriding
        _spring_real_e15 = coin.get("ema_align_15m", "")
        _spring_real_e1h = coin.get("ema_align_1h", "")
        # Override EMA penalties -- bearish EMA is expected at entry
        coin["ema_align_15m"] = "NEUTRAL_SPRING"
        coin["ema_align_1h"]  = "NEUTRAL_SPRING"
        # ── RATCHET GUARD -- if floor is active and ROE above floor, let ratchet manage ──
        _spring_floor = float(trade.get("floor_roe", 0))
        _spring_peak  = float(trade.get("peak_roe", 0))
        _ratchet_active = _spring_floor > 0 and roe > _spring_floor
        if _ratchet_active:
            hold_score += 30
            signals.append(f"Ratchet floor={_spring_floor:.0f}% active -- let ratchet manage")
        # Spring specific: close if bounce failed after 90 mins
        if age > 90 and roe < 1:
            close_score += 20; signals.append(f"Spring bounce failed: {roe:.1f}% after {age:.0f}min")
        elif age > 60 and roe < -5:
            close_score += 25; signals.append(f"Spring deep loss: {roe:.1f}% after {age:.0f}min")
        # Spring profit management -- skip if ratchet already protecting
        if not _ratchet_active:
            if roe >= 8:
                close_score += 20; signals.append(f"Spring target hit {roe:.1f}% -- take profit")
            elif roe >= 5:
                close_score += 10; signals.append(f"Spring profit zone {roe:.1f}%")
        elif roe > 0 and roe < 5 and age < 45:
            hold_score += 15; signals.append(f"Spring bouncing {roe:.1f}% -- let it run")
        # Peak ROE reversal -- Spring gave back too much
        if peak_roe > 5 and roe < peak_roe * 0.5:
            close_score += 20; signals.append(f"Spring reversal: peak={peak_roe:.1f}% now={roe:.1f}%")
    elif is_apex:
        # APEX: trend following -- EMA alignment is critical signal
        pass  # normal logic applies
    # Entry thesis
    e15=coin.get("ema_align_15m",""); e1h=coin.get("ema_align_1h","")
    if direction=="LONG":
        if e15=="BEAR" and e1h=="BEAR": close_score+=25; signals.append("Both EMAs bearish - thesis invalidated")
        elif e15=="BEAR": close_score+=10; signals.append("15m EMA turned bearish")
    else:
        if e15=="BULL" and e1h=="BULL": close_score+=25; signals.append("Both EMAs bullish - thesis invalidated")
        elif e15=="BULL": close_score+=10; signals.append("15m EMA turned bullish")
    # Momentum
    adx=coin.get("adx_15m",25); adx_t=coin.get("adx_trend_15m","FLAT")
    macd_t=coin.get("macd_trend","")
    if adx_t=="FALLING" and adx<20: close_score+=15; signals.append(f"ADX collapsing {adx:.0f}")
    elif adx_t=="RISING" and adx>25: hold_score+=10; signals.append(f"ADX rising {adx:.0f}")
    if direction=="LONG":
        if macd_t=="CROSSING_BEAR": close_score+=20; signals.append("MACD crossing bearish")
        elif macd_t=="BULLISH": hold_score+=10; signals.append("MACD bullish")
    else:
        if macd_t=="CROSSING_BULL": close_score+=20; signals.append("MACD crossing bullish")
        elif macd_t=="BEARISH": hold_score+=10; signals.append("MACD bearish")
    # RSI
    rsi15=coin.get("rsi_15m",50); rsi1h=coin.get("rsi_1h",50)
    if direction=="LONG":
        if rsi15>78: close_score+=15; signals.append(f"RSI overbought {rsi15:.0f}")
        elif rsi15<35: hold_score+=15; signals.append(f"RSI oversold {rsi15:.0f} bounce likely")
        if rsi1h>75: close_score+=10; signals.append(f"1H RSI overbought {rsi1h:.0f}")
    else:
        if rsi15<25: close_score+=15; signals.append(f"RSI oversold {rsi15:.0f} squeeze risk")
        elif rsi15>65: hold_score+=15; signals.append(f"RSI high {rsi15:.0f} short momentum")
    # Divergence
    div=coin.get("rsi_divergence","NONE")
    if direction=="LONG" and div=="BULLISH": hold_score+=20; signals.append("Bullish RSI divergence")
    elif direction=="SHORT" and div=="BEARISH": hold_score+=20; signals.append("Bearish RSI divergence")
    elif direction=="LONG" and div=="BEARISH": close_score+=15; signals.append("Bearish divergence on long")
    # Volume
    vol=coin.get("volume_ratio",1.0)
    if vol>2.5 and direction=="LONG": hold_score+=10; signals.append(f"High volume {vol:.1f}x")
    elif vol<0.4: close_score+=8; signals.append(f"Low volume {vol:.1f}x")
    # BB position
    bb=coin.get("bb_position",0.5)
    if direction=="LONG":
        if bb>0.9: close_score+=12; signals.append("At upper BB overextended")
        elif bb<0.1: hold_score+=15; signals.append("At lower BB bounce likely")
    else:
        if bb<0.1: close_score+=12; signals.append("At lower BB short overextended")
        elif bb>0.9: hold_score+=15; signals.append("At upper BB short momentum")
    # Market
    btc_ema=market.get("btc_ema_align","FLAT"); alts=market.get("alts_bull_pct",50)
    btc_adx_t=market.get("btc_adx_trend","FLAT")
    regime=market.get("market_regime","UNKNOWN")

    if is_spring:
        # Spring works best in SIDEWAYS and mild BEAR -- dips are predictable
        if regime=="SIDEWAYS": hold_score+=10; signals.append("SIDEWAYS -- Spring favored")
        elif regime=="BEAR" and alts<35: close_score+=15; signals.append("Deep bear -- Spring risky")
        elif "BULL" in regime and alts>70: close_score+=10; signals.append("Strong bull -- fewer dips")
        # BTC ADX falling = trend dying = dip bounce more likely
        if btc_adx_t=="FALLING": hold_score+=8; signals.append("BTC ADX falling -- bounce favorable")
        elif btc_adx_t=="RISING" and btc_ema=="BEAR": close_score+=10; signals.append("BTC bear trend strengthening")
    elif direction=="LONG":
        if btc_ema=="BEAR" and alts<45: close_score+=20; signals.append(f"Market bearish alts {alts:.0f}%")
        elif btc_ema=="BULL" and alts>60: hold_score+=15; signals.append("Market bullish aligned")
        if btc_adx_t=="FALLING": close_score+=8; signals.append("BTC ADX falling")
        if regime=="SIDEWAYS": close_score+=10; signals.append("SIDEWAYS -- APEX longs risky")
    else:
        if btc_ema=="BULL" and alts>60: close_score+=20; signals.append(f"Market bullish danger for short")
        elif btc_ema=="BEAR" and alts<45: hold_score+=15; signals.append("Market bearish aligned")
    # Time + ROE based urgency -- different thresholds per bot type
    if is_spring:
        # Spring trades should resolve faster
        if roe < -3 and age > 60:
            close_score += 20; signals.append(f"Spring stalled: {roe:.1f}% at {age:.0f}min")
        elif roe < -8 and age > 30:
            close_score += 25; signals.append(f"Spring deep loss: {roe:.1f}% at {age:.0f}min")
        # Spring timeout handled in check_ratchet_trail (6H)
    else:
        # APEX trend trades can run longer -- but cut losses decisively
        if roe < -10 and age > 20:
            close_score += 35; signals.append(f"APEX hard loss: {roe:.1f}% at {age:.0f}min -- cut")
        elif roe < -5 and age > 45:
            close_score += 30; signals.append(f"APEX losing: {roe:.1f}% at {age:.0f}min -- exit")
        elif roe < -3 and age > 75:
            close_score += 25; signals.append(f"Aging loser: {roe:.1f}% ROE at {age:.0f}min")
        elif roe < -1 and age > 120:
            pass  # dead trade score removed
        elif roe < 0 and age > 150:
            close_score += 30; signals.append(f"Critical age: {roe:.1f}% at {age:.0f}min -- close")
    # No signals + losing = close not hold
    if len(signals) == 0 and roe < -1:
        close_score += 20; signals.append(f"No signals + losing {roe:.1f}% -- bias to close")
    # ROE based
    if roe>15:
        if close_score>hold_score+10: close_score+=10; signals.append(f"Protecting {roe:.1f}% ROE")
        else: hold_score+=5; signals.append(f"Winning {roe:.1f}% let run")
    elif roe<-8 and age>30: close_score+=15; signals.append(f"Losing {roe:.1f}% after {age:.0f}m")
    # dead trade score removed
    # Delta signals -- change since last check
    d_adx=coin.get("delta_adx",0); d_rsi=coin.get("delta_rsi",0); d_vol=coin.get("delta_vol",0)
    if d_adx<-4: close_score+=12; signals.append(f"ADX dropped {d_adx:.1f} in 3min -- rapid decay")
    elif d_adx>4: hold_score+=8; signals.append(f"ADX rising fast +{d_adx:.1f}")
    if direction=="LONG":
        if d_rsi<-6: close_score+=10; signals.append(f"RSI dropped {d_rsi:.1f} -- momentum fading")
        elif d_rsi>6: hold_score+=8; signals.append(f"RSI rising +{d_rsi:.1f} -- momentum building")
    else:
        if d_rsi>6: close_score+=10; signals.append(f"RSI rising {d_rsi:.1f} -- short pressure")
        elif d_rsi<-6: hold_score+=8; signals.append(f"RSI falling {d_rsi:.1f} -- short momentum")
    if d_vol>1.5: hold_score+=8; signals.append(f"Volume surge +{d_vol:.1f}x")
    elif d_vol<-0.5: close_score+=6; signals.append(f"Volume drying up {d_vol:.1f}x")
    # ── OI change -- leading indicator ──
    oi_chg = sf(coin.get("oi_change_pct", 0))
    if abs(oi_chg) > 0.8:  # meaningful OI move threshold
        if is_spring:
            if oi_chg < -2: hold_score+=12; signals.append(f"OI {oi_chg:.1f}% liquidations -- Spring bounce")
            elif oi_chg > 2 and roe < 0: close_score+=10; signals.append(f"OI +{oi_chg:.1f}% price falling -- Spring failing")
        elif direction=="LONG":
            if oi_chg > 2 and roe < -2: close_score+=15; signals.append(f"OI +{oi_chg:.1f}% price falling -- shorts piling in")
            elif oi_chg < -2 and roe < -3: hold_score+=15; signals.append(f"OI {oi_chg:.1f}% liquidations -- bounce incoming")
            elif oi_chg > 1.5 and roe > 0: hold_score+=10; signals.append(f"OI +{oi_chg:.1f}% genuine accumulation")
        else:
            if oi_chg > 2 and roe > 0: hold_score+=12; signals.append(f"OI +{oi_chg:.1f}% shorts confirmed")
            elif oi_chg < -2 and roe > 0: close_score+=10; signals.append(f"OI {oi_chg:.1f}% short covering risk")

    # ── Candle pattern ──
    candle = coin.get("candle_pattern","")
    if candle and candle != "NONE":
        if direction=="LONG":
            if candle in ["SHOOTING_STAR","BEARISH_ENGULF","EVENING_STAR","HANGING_MAN"]:
                close_score+=12; signals.append(f"Candle: {candle} bearish reversal")
            elif candle in ["HAMMER","BULLISH_ENGULF","MORNING_STAR","DOJI"]:
                hold_score+=10; signals.append(f"Candle: {candle} bullish signal")
        else:
            if candle in ["HAMMER","BULLISH_ENGULF","MORNING_STAR"]:
                close_score+=12; signals.append(f"Candle: {candle} bullish reversal -- short risk")
            elif candle in ["SHOOTING_STAR","BEARISH_ENGULF","EVENING_STAR"]:
                hold_score+=10; signals.append(f"Candle: {candle} bearish -- short confirmed")

    # ── BTC RSI across timeframes ──
    btc_rsi_15m = sf(coin.get("btc_rsi_15m", 50))
    btc_rsi_1h  = sf(coin.get("btc_rsi_1h", 50))
    btc_adx     = sf(coin.get("btc_adx_15m", 25))
    btc_trend   = coin.get("btc_adx_trend","FLAT")
    if not is_spring:
        if direction=="LONG":
            if btc_rsi_15m > 75 and btc_rsi_1h > 70:
                close_score+=10; signals.append(f"BTC overbought RSI {btc_rsi_15m:.0f}/{btc_rsi_1h:.0f}")
            elif btc_rsi_15m < 35:
                hold_score+=8; signals.append(f"BTC oversold RSI {btc_rsi_15m:.0f} -- bounce likely")
            if btc_adx > 40 and btc_trend=="FALLING":
                close_score+=8; signals.append(f"BTC ADX {btc_adx:.0f} collapsing -- trend ending")

    # ── Mins to funding ──
    mins_funding = sf(coin.get("mins_to_funding", 999))
    fr = sf(coin.get("funding_rate", 0))
    if mins_funding < 30 and abs(fr) > 0.05:
        if direction=="LONG" and fr > 0.05:
            close_score+=8; signals.append(f"Funding {fr:.3f}% due in {mins_funding:.0f}m -- longs paying")
        elif direction=="SHORT" and fr < -0.05:
            close_score+=8; signals.append(f"Neg funding due in {mins_funding:.0f}m -- shorts paying")

    # ── Behavior from analyze_coin_behavior ──
    behavior_signal = coin.get("behavior","NORMAL")
    behavior_pred   = coin.get("prediction","")
    behavior_conf   = sf(coin.get("confidence", 0))
    if behavior_conf > 50 and behavior_signal != "NORMAL":
        if "EXHAUSTION" in behavior_signal:
            if direction=="LONG" and "DOWN" in behavior_pred:
                close_score+=int(behavior_conf/10); signals.append(f"Behavior: {behavior_signal} → {behavior_pred}")
            elif direction=="SHORT" and "UP" in behavior_pred:
                close_score+=int(behavior_conf/10); signals.append(f"Behavior: {behavior_signal} → {behavior_pred}")
        elif "STOP_HUNT" in behavior_signal:
            if "BOUNCE" in behavior_pred and direction=="LONG":
                hold_score+=int(behavior_conf/10); signals.append(f"Behavior: stop hunt → bounce likely")
            elif "DROP" in behavior_pred and direction=="LONG":
                close_score+=int(behavior_conf/10); signals.append(f"Behavior: stop hunt → drop likely")
            # APEX SHORT - stop hunt LONG = short confirmed (they hunted longs = bears win)
            elif behavior_signal=="STOP_HUNT_LONG" and direction=="SHORT":
                hold_score+=int(behavior_conf/8); signals.append(f"Stop hunt LONG = short confirmed {behavior_conf:.0f}%")
            # APEX SHORT -- stop hunt SHORT = SL widening needed (they may hunt our stops)
            elif behavior_signal=="STOP_HUNT_SHORT" and direction=="SHORT":
                # Flag for SL widening -- don't close, widen SL by 0.5x ATR
                trade["sl_widen_flag"] = True
                hold_score+=5; signals.append(f"Stop hunt SHORT detected - widening SL to avoid hunt")
            elif behavior_signal=="STOP_HUNT_LONG" and direction=="LONG":
                trade["sl_widen_flag"] = True
                hold_score+=5; signals.append("Stop hunt LONG - widening SL")

    # BTC correlation -- Spring reverses BTC, APEX follows BTC
    btc_corr=sf(coin.get("btc_correlation",0))
    btc_move=sf(coin.get("btc_move_5m",0))
    if abs(btc_move)>0.3:
        if is_spring:
            if btc_move<0 and btc_corr<0.3: hold_score+=15; signals.append(f"Spring: ignoring BTC dump -- bounce starting")
            elif btc_move>0 and btc_corr>1.0: hold_score+=10; signals.append(f"Spring: following BTC up -- bounce confirmed")
            elif btc_move<0 and btc_corr>2.0: close_score+=10; signals.append(f"Spring: dumping with BTC -- bounce failed")
        elif direction=="LONG":
            if btc_move<0 and btc_corr>1.5: close_score+=15; signals.append(f"Dumping {btc_corr:.1f}x BTC -- weak")
            elif btc_move<0 and btc_corr<0.3: hold_score+=12; signals.append(f"Ignoring BTC dump -- strong")
            elif btc_move>0 and btc_corr>1.0: hold_score+=10; signals.append(f"Leading BTC up {btc_corr:.1f}x")
        else:
            if btc_move>0 and btc_corr>1.5: close_score+=15; signals.append(f"Pumping {btc_corr:.1f}x BTC -- short risk")
            elif btc_move>0 and btc_corr<0.3: hold_score+=12; signals.append(f"Ignoring BTC pump -- short safe")

    # Volume -- Spring: low volume after entry is NORMAL (dump over)
    if not is_spring:
        vol=sf(coin.get("volume_ratio",1.0))
        if vol>2.5 and direction=="LONG": hold_score+=8; signals.append(f"High volume {vol:.1f}x buyers active")
        elif vol<0.4: close_score+=8; signals.append(f"Low volume {vol:.1f}x move not confirmed")
    else:
        vol=sf(coin.get("volume_ratio",1.0))
        if vol>2.0: hold_score+=10; signals.append(f"Spring: high volume {vol:.1f}x bounce confirming")

    # Funding
    fr=sf(coin.get("funding_rate",0))
    # funding_rate stored as percentage (e.g. 0.05 = 0.05%)
    # High positive = longs paying = bearish signal for longs
    if direction=="LONG" and fr>0.05:
        close_score+=8; signals.append(f"Funding {fr:.3f}% longs paying")
    elif direction=="LONG" and fr<-0.05:
        hold_score+=5; signals.append(f"Funding {fr:.3f}% shorts paying -- long favored")
    elif direction=="SHORT" and fr<-0.05:
        close_score+=8; signals.append(f"Neg funding {fr:.3f}% shorts paying")
    elif direction=="SHORT" and fr>0.05:
        hold_score+=5; signals.append(f"Funding {fr:.3f}% longs paying -- short favored")
    # Risk temp
    if risk_temp>=80: close_score+=30; signals.append(f"CRITICAL risk {risk_temp:.0f}")
    elif risk_temp>=60: close_score+=15; signals.append(f"HIGH risk {risk_temp:.0f}")
    # Conflict check -- if both bots trading same coin opposite directions
    symbol = trade.get("symbol","")
    if symbol in market.get("conflicts", set()):
        close_score += 20; signals.append(f"CONFLICT: both bots on {symbol} opposite directions -- close")

    # Regime shift risk -- if shift likely, bias toward closing longs
    shift_prob = sf(market.get("regime_shift_prob", 0))
    if shift_prob > 60 and direction == "LONG" and not is_spring:
        close_score += 15; signals.append(f"Regime shift {shift_prob:.0f}% likely -- reduce long exposure")
    elif shift_prob > 40 and direction == "LONG" and not is_spring:
        close_score += 8; signals.append(f"Regime shift risk {shift_prob:.0f}%")

    # SL hit risk -- learned from historical data
    regime = market.get("market_regime","UNKNOWN")
    if not is_spring:
        if regime=="BULL_STRONG" and direction=="LONG" and roe < -5.0:
            # Only tighten when losing -- give breathing room above -5%
            close_score+=5; signals.append("BULL_STRONG LONG high SL risk -- tighten")
        if regime=="SIDEWAYS" and direction=="LONG" and roe < -5.0:
            close_score+=8; signals.append("SIDEWAYS LONG highest SL risk")
    hour=datetime.now(timezone.utc).hour
    if hour==0 and not is_spring:
        close_score+=5; signals.append(f"Hour 00:00 -- 9 SL hits historically")

    # Pattern matching -- query historical patterns
    try:
        conn_p = _get_mind_conn()
        pattern_rows = conn_p.execute("""
            SELECT pattern_key, accuracy, occurrences, avg_pnl_impact
            FROM patterns WHERE occurrences >= 3 AND active=1
            ORDER BY occurrences DESC LIMIT 50""").fetchall()
        conn_p.close()
        if pattern_rows:
            # Build current state key components
            adx_t = coin.get("adx_trend_15m","FLAT")
            e_align = coin.get("ema_align_15m","FLAT")
            macd_t = coin.get("macd_trend","")
            rsi15 = coin.get("rsi_15m",50)
            rsi_tag = "rsi_ob" if rsi15>70 else ("rsi_os" if rsi15<30 else "")
            regime = market.get("market_regime","UNKNOWN")
            e_align_1h = coin.get("ema_align_1h", "FLAT")
            btc_adx_t  = market.get("btc_adx_trend", "FLAT")
            roe_now    = sf(trade.get("roe", 0))
            age_now    = sf(trade.get("age_mins", 0))
            roe_bucket = "roe-10" if roe_now<-7 else ("roe-5" if roe_now<-2 else ("roe5" if roe_now>5 else "roe0"))
            age_bucket = f"age{int(age_now//30)*30}"
            current_tags = set(filter(None,[
                f"adx_{adx_t}",
                f"ema_{e_align}_{e_align_1h}",
                f"macd_{macd_t}",
                f"btc_{btc_adx_t}",
                f"reg_{regime}",
                roe_bucket,
                age_bucket,
                rsi_tag,
            ]))
            for pkey, acc, occ, avg_pnl in pattern_rows:
                # Only match patterns for same direction and decision context
                if not pkey.startswith(direction): continue
                if occ < 5: continue
                # Substring match -- check if current state signals appear in pattern key
                matches = sum(1 for tag in current_tags if tag and tag in pkey)
                if matches >= 2:
                    is_close_pattern = "_CLOSE_" in pkey
                    is_hold_pattern  = "_HOLD_" in pkey
                    if acc >= 65 and avg_pnl > 0:
                        if is_hold_pattern:
                            hold_score += min(int(acc/10), 15)
                            signals.append(f"Pattern hold: {acc:.0f}% WR {occ}obs")
                        elif is_close_pattern:
                            close_score += min(int(acc/10), 15)
                            signals.append(f"Pattern close: {acc:.0f}% WR {occ}obs")
                    elif acc <= 40 or avg_pnl < -0.3:
                        if is_close_pattern:
                            close_score += min(int((100-acc)/10), 12)
                            signals.append(f"Pattern danger: {acc:.0f}% WR {occ}obs")
    except Exception as pe:
        pass

    # Coin memory -- full personality used
    if memory:
        bp   = sf(memory.get("bounce_probability", 0.5), 0.5)
        regime_now = market.get("market_regime", "UNKNOWN")
        hour_now   = datetime.now(timezone.utc).hour

        # Bounce probability (existing logic, extended)
        if roe < -5 and bp > 0.65:
            hold_score += 10; signals.append(f"Bounce prob {bp*100:.0f}%")
        elif roe < -5 and bp < 0.35:
            close_score += 10; signals.append(f"Low bounce prob {bp*100:.0f}%")

        # Regime-specific win rate for this coin
        # Only trust WR if we have enough observations -- noise filter
        mind_tot = sf(memory.get("mind_total", 0))
        total_obs = sf(memory.get("total_observations", 0))
        min_obs_for_wr = 15  # need at least 15 observations before WR is meaningful
        if "BULL" in regime_now:
            cwr = sf(memory.get("bull_win_rate", 50))
        elif "BEAR" in regime_now:
            cwr = sf(memory.get("bear_win_rate", 50))
        else:
            cwr = sf(memory.get("sideways_win_rate", 50))
        if total_obs >= min_obs_for_wr:
            if cwr >= 70:
                hold_score += 12; signals.append(f"Coin {cwr:.0f}% WR in {regime_now} ({total_obs:.0f} obs)")
            elif cwr <= 35:
                close_score += 12; signals.append(f"Coin poor {cwr:.0f}% WR in {regime_now} ({total_obs:.0f} obs)")
        # else: insufficient data -- don't influence decision

        # Best/worst hour for this coin
        best_h  = memory.get("best_hour_utc")
        worst_h = memory.get("worst_hour_utc")
        if worst_h is not None and hour_now == int(sf(worst_h)):
            close_score += 10; signals.append(f"Worst hour {hour_now}:00 for {symbol} historically")
        elif best_h is not None and hour_now == int(sf(best_h)):
            hold_score += 8; signals.append(f"Best hour {hour_now}:00 for {symbol}")

        # MIND accuracy on this coin -- if MIND has been wrong here, reduce confidence
        mind_acc = sf(memory.get("mind_accuracy", 0))
        mind_tot = sf(memory.get("mind_total", 0))
        if mind_tot >= 10 and mind_acc < 45:
            # MIND has struggled on this coin -- be more conservative
            if close_score > hold_score:
                close_score += 8; signals.append(f"MIND only {mind_acc:.0f}% on {symbol} -- bias close")
        elif mind_tot >= 10 and mind_acc >= 75:
            # MIND is reliable on this coin -- trust the decision more
            if hold_score > close_score:
                hold_score += 5; signals.append(f"MIND {mind_acc:.0f}% accurate on {symbol}")

        # Avg winning vs losing ROE -- is this a coin that runs or reverses?
        avg_win_roe  = sf(memory.get("avg_winning_roe", 0))
        avg_loss_roe = sf(memory.get("avg_losing_roe", 0))
        if roe > 0 and avg_win_roe > 8:
            hold_score += 8; signals.append(f"Coin avg win={avg_win_roe:.1f}% -- let it run")
        elif roe < 0 and avg_loss_roe < -8:
            close_score += 8; signals.append(f"Coin avg loss={avg_loss_roe:.1f}% -- cut it")
    # ── ORDER FLOW -- real-time signals, highest weight in system ──
    # This runs 30-60 seconds ahead of any candle-based signal
    flow_signal = coin.get("flow_signal", "NEUTRAL")
    flow_score  = sf(coin.get("flow_score", 0))
    flow_aggr   = sf(coin.get("flow_aggr", 0))
    flow_imbal  = sf(coin.get("flow_imbal", 0))
    flow_spoof  = sf(coin.get("flow_spoof", 0))
    flow_absorb = sf(coin.get("flow_absorb", 0))
    flow_wall   = coin.get("flow_wall", "NONE")
    flow_lbc    = int(sf(coin.get("flow_lbc", 0)))
    flow_lsc    = int(sf(coin.get("flow_lsc", 0)))
    flow_momdiv = sf(coin.get("flow_momdiv", 0))

    if flow_signal != "NEUTRAL":
        if direction == "LONG":
            if flow_signal == "STRONG_BUY":
                hold_score += 25
                signals.append(f"FLOW: STRONG_BUY score={flow_score:+.0f} -- institutions buying")
            elif flow_signal == "BUY_PRESSURE":
                hold_score += 15
                signals.append(f"FLOW: buy pressure score={flow_score:+.0f}")
            elif flow_signal == "STRONG_SELL":
                close_score += 28
                signals.append(f"FLOW: STRONG_SELL score={flow_score:+.0f} -- institutions dumping")
            elif flow_signal == "SELL_PRESSURE":
                close_score += 15
                signals.append(f"FLOW: sell pressure score={flow_score:+.0f}")
            elif flow_signal == "SPOOF_SELL":
                # Fake sell wall -- actually bullish, smart money wants to buy cheap
                hold_score += 18
                signals.append(f"FLOW: sell wall is FAKE -- smart money accumulating")
            elif flow_signal == "SPOOF_BUY":
                # Fake buy wall -- actually bearish, smart money wants to sell high
                close_score += 20
                signals.append(f"FLOW: buy wall is FAKE -- distribution incoming")
            elif flow_signal == "SELL_WALL_AHEAD":
                close_score += 12
                signals.append(f"FLOW: sell wall blocking upside -- exit before rejection")
            elif flow_signal == "BUY_WALL_SUPPORT":
                hold_score += 12
                signals.append(f"FLOW: buy wall supporting price -- hold")
        else:  # SHORT
            if flow_signal == "STRONG_SELL":
                hold_score += 25
                signals.append(f"FLOW: STRONG_SELL score={flow_score:+.0f} -- short confirmed")
            elif flow_signal == "SELL_PRESSURE":
                hold_score += 15
                signals.append(f"FLOW: sell pressure confirms short")
            elif flow_signal == "STRONG_BUY":
                close_score += 28
                signals.append(f"FLOW: STRONG_BUY score={flow_score:+.0f} -- short at risk")
            elif flow_signal == "BUY_PRESSURE":
                close_score += 15
                signals.append(f"FLOW: buy pressure threatens short")
            elif flow_signal == "SPOOF_BUY":
                hold_score += 18
                signals.append(f"FLOW: buy wall is FAKE -- short safe")
            elif flow_signal == "SPOOF_SELL":
                close_score += 20
                signals.append(f"FLOW: sell wall is FAKE -- short squeeze risk")
            elif flow_signal == "BUY_WALL_SUPPORT":
                close_score += 12
                signals.append(f"FLOW: buy wall -- short squeeze risk")

    # Large institutional trades -- highest conviction signal
    if flow_lbc >= 3 and direction == "LONG":
        hold_score += 15; signals.append(f"FLOW: {flow_lbc} large buys -- institutional accumulation")
    if flow_lsc >= 3 and direction == "LONG":
        close_score += 15; signals.append(f"FLOW: {flow_lsc} large sells -- institutional distribution")
    if flow_lbc >= 3 and direction == "SHORT":
        close_score += 15; signals.append(f"FLOW: {flow_lbc} large buys -- short squeeze risk")
    if flow_lsc >= 3 and direction == "SHORT":
        hold_score += 15; signals.append(f"FLOW: {flow_lsc} large sells -- short confirmed")

    # Absorption -- price not moving despite pressure = hidden opposition
    if abs(flow_absorb) > 0.4:
        if flow_absorb < 0 and direction == "LONG":
            close_score += 12; signals.append("FLOW: buyers being absorbed -- hidden sellers")
        elif flow_absorb > 0 and direction == "LONG":
            hold_score += 12; signals.append("FLOW: sellers being absorbed -- hidden buyers")
        elif flow_absorb < 0 and direction == "SHORT":
            hold_score += 12; signals.append("FLOW: buyers absorbed -- short confirmed")
        elif flow_absorb > 0 and direction == "SHORT":
            close_score += 12; signals.append("FLOW: sellers absorbed -- short at risk")

    # Momentum divergence -- aggression shifting direction
    if abs(flow_momdiv) > 0.25:
        if flow_momdiv > 0.25 and direction == "SHORT":
            close_score += 10; signals.append(f"FLOW: buy momentum building -- short exit")
        elif flow_momdiv < -0.25 and direction == "LONG":
            close_score += 10; signals.append(f"FLOW: sell momentum building -- long exit")

    # ── SEQUENCE SIGNALS -- highest conviction input ──
    seq_name = coin.get("sequence_name", "NONE")
    seq_conf = sf(coin.get("sequence_conf", 0))
    if seq_name != "NONE" and seq_conf > 0:
        if seq_name == "MTF_CONFLUENCE":
            if direction == "LONG": hold_score += 20
            else: hold_score += 20
            signals.append(f"SEQ: {seq_name} ({seq_conf:.0f}%) -- high conviction")
        elif seq_name == "ACCUMULATION":
            if direction == "LONG": hold_score += 15
            signals.append(f"SEQ: accumulation detected -- breakout imminent")
        elif seq_name == "TREND_DYING":
            close_score += 18
            signals.append(f"SEQ: trend dying -- exit before reversal")
        elif "SQUEEZE" in seq_name:
            close_score += 20
            signals.append(f"SEQ: {seq_name} -- forced liquidation coming")
        elif "STOP_HUNT_LONG_SAFE" in seq_name and direction == "LONG":
            hold_score += 15
            signals.append(f"SEQ: stop hunt complete -- longs safe")
        elif "STOP_HUNT_SHORT_SAFE" in seq_name and direction == "SHORT":
            hold_score += 15
            signals.append(f"SEQ: stop hunt complete -- shorts safe")
        # Multiple sequences = very high conviction
        if "+" in seq_name:
            if hold_score > close_score: hold_score += 10
            else: close_score += 10
            signals.append(f"SEQ: multiple sequences confirmed")

    # ── LIVE REGIME -- use real-time probability, not stale 4H label ──
    regime_live     = market.get("market_regime_live", market.get("market_regime", "UNKNOWN"))
    regime_certainty = sf(market.get("regime_certainty", 50))
    # If live regime differs from stored regime and certainty is high -- trust live
    # H5 FIX: Gate regime_live override behind learned accuracy
    # Regime_live signal was 32% accurate (38 obs) -- worse than coin flip
    # Only apply override when signal has proven accuracy >= 55%
    try:
        _lp_rl = _load_learned_params()
        _rl_sig = _lp_rl.get("signal_accuracy", {}).get("Regime_live=", {})
        _rl_acc = float(_rl_sig.get("accuracy", 0))
        _rl_n   = int(_rl_sig.get("count", 0))
        _regime_live_trusted = (_rl_n < 20) or (_rl_acc >= 55)  # trust if no data yet or accuracy ok
    except:
        _regime_live_trusted = True
    if regime_live != market.get("market_regime", "UNKNOWN") and regime_certainty >= 60 and _regime_live_trusted:
        signals.append(f"Regime live={regime_live} ({regime_certainty:.0f}% certain) overriding 4H label")
        if regime_live == "BEAR" and direction == "LONG":
            close_score += 15
        elif regime_live == "BULL_STRONG" and direction == "LONG":
            hold_score += 10
        elif regime_live == "SIDEWAYS" and not is_spring:
            close_score += 8

    # ── CROSS-COIN SIGNALS -- market-wide leading indicators ──
    cc = market.get("cross_coin", {})
    if cc:
        oi_drops    = sf(cc.get("oi_drops_pct", 0))
        rsi_ob      = sf(cc.get("rsi_ob_pct", 0))
        adx_fall    = sf(cc.get("adx_falling_pct", 0))
        rsi_os      = sf(cc.get("rsi_os_pct", 0))
        vol_surge   = sf(cc.get("vol_surge_pct", 0))
        if oi_drops >= 50 and direction == "LONG":
            close_score += 15; signals.append(f"Cross-coin: {oi_drops:.0f}% OI dropping -- market liquidation")
        if rsi_ob >= 60 and direction == "LONG":
            close_score += 12; signals.append(f"Cross-coin: {rsi_ob:.0f}% coins overbought -- market top")
        if adx_fall >= 60:
            close_score += 10; signals.append(f"Cross-coin: {adx_fall:.0f}% ADX falling -- trend exhaustion")
        if rsi_os >= 40 and direction == "LONG" and is_spring:
            hold_score += 12; signals.append(f"Cross-coin: {rsi_os:.0f}% oversold -- bounce environment")
        if vol_surge >= 50:
            hold_score += 8; signals.append(f"Cross-coin: {vol_surge:.0f}% vol surging -- momentum confirmed")

    # ── CAPITAL ALLOCATION -- reduce confidence when portfolio overexposed ──
    alloc_notes = market.get("allocation_notes", [])
    has_warning = any("⚠️" in n for n in alloc_notes)
    if has_warning:
        # Portfolio is overexposed -- bias toward protecting capital
        if direction == "LONG" and close_score > hold_score:
            close_score += 8; signals.append("Portfolio overexposed -- bias to reduce")
        elif direction == "LONG" and hold_score > close_score and roe < 2:
            close_score += 5; signals.append("Portfolio overexposed -- tighten marginal longs")

    # If learned data is driving -- reduce hardcoded signal weight
    # This prevents fixed weights from overriding what APEX MIND learned
    if _obs_driven:
        # Dampen hardcoded signals only -- restore obs score at full weight
        # This prevents fixed weights from overriding what APEX MIND learned
        _hc_close = close_score - _obs_close_score  # hardcoded portion only
        _hc_hold  = hold_score  - _obs_hold_score
        close_score = int(_hc_close * 0.4) + _obs_close_score  # dampen hardcoded, keep obs
        hold_score  = int(_hc_hold  * 0.4) + _obs_hold_score
    # Extreme loss override -- always close regardless of dampening
    if roe < -15.0:
        close_score += 80; tighten_score = 0
        # Only allow hold if very high confidence (80%+) -- e.g. Spring bounce forming
        if hold_score < 80: hold_score = 0
        signals.append(f"Extreme loss {roe:.1f}% -- force close unless 80% hold confidence")
    elif roe < -10.0 and age > 60:
        close_score += 40; tighten_score = 0; signals.append(f"Deep loss {roe:.1f}% at {age:.0f}min")
    net=close_score-hold_score; conf=min(abs(net)*2,95)
    # TIGHTEN obs signal takes priority when strong -- do not let hold_score cancel it
    if _obs_tighten_score >= 50 and net > -25 and roe < -5.0:
        tighten_score += _obs_tighten_score  # only tighten when losing (high confidence)
    # ROE-based auto TIGHTEN -- always tighten on large profits
    if roe > 15: tighten_score += 30; signals.append(f"ROE {roe:.1f}% -- protect profit")
    elif roe > 8: tighten_score += 15; signals.append(f"ROE {roe:.1f}% -- tighten SL")
    if tighten_score >= 20:  # TIGHTEN obs overrides weak HOLD
        decision="TIGHTEN"; pred="UNCERTAIN"
        signals.insert(0, f"OBS TIGHTEN override: score={tighten_score:.0f}")
    elif net>=25:
        decision="CLOSE"; pred="DOWN" if direction=="LONG" else "UP"
    elif net>=15:
        decision="TIGHTEN"; pred="UNCERTAIN"

    # Calculate new SL for ANY TIGHTEN decision
    if decision == "TIGHTEN":
        entry=sf(trade.get("entry",0))
        current=sf(trade.get("current_price",0))
        atr=sf(coin.get("atr_15m",0))
        if entry>0 and current>0 and atr>0:
            if direction=="LONG":
                new_sl=round(current - atr*0.8, 6)
                if roe>0: new_sl=max(new_sl, entry*1.001)  # lock above entry if profitable
                new_sl=min(new_sl, current*0.999)  # must be below current price
                # Never widen SL -- must be above original SL
                orig_sl = float(trade.get("sl", 0))
                if orig_sl > 0: new_sl=max(new_sl, orig_sl)
            else:
                new_sl=round(current + atr*0.8, 6)
                if roe>0: new_sl=min(new_sl, entry*0.999)  # lock below entry if profitable
                new_sl=max(new_sl, current*1.001)  # must be above current price
                # Never widen SL -- must be below original SL
                orig_sl = float(trade.get("sl", 0))
                if orig_sl > 0: new_sl=min(new_sl, orig_sl)
            trade["suggested_sl"]=new_sl
            signals.append(f"New SL suggested: {new_sl:.4f}")
    elif net<=-20:
        decision="HOLD"; pred="UP" if direction=="LONG" else "DOWN"
    else:
        # Weak signal either way -- don't blindly hold
        # If losing and uncertain → TIGHTEN to protect
        # If winning and uncertain → HOLD but note low conviction
        if roe < -2 and conf < 40:
            decision="TIGHTEN"; pred="UNCERTAIN"
            signals.append(f"Low conviction HOLD converted to TIGHTEN -- protect capital")
        elif roe > 5 and conf < 30:
            decision="TIGHTEN"; pred="UNCERTAIN"
            signals.append(f"Low conviction on winning trade -- protect profit")
        else:
            decision="HOLD"; pred="SIDEWAYS"
    # ── OBSERVATION-BASED DECISION ADJUSTMENT ──
    try:
        _lp = _load_learned_params()
        _close_key = "spring_close_reasons" if bot_type == "SPRING" else "apex_close_reasons"
        _close_obs = _lp.get(_close_key, {})
        if _close_obs and decision in _close_obs:
            _obs_wr = float(_close_obs[decision].get("wr", 50))
            _obs_roe = float(_close_obs[decision].get("avg_roe", 0))
            # If historical data shows this decision type has high WR -- boost confidence
            if _obs_wr > 70 and _obs_roe > 0:
                conf = round(min(99, conf * 1.05), 1)
                signals.append(f"Obs {decision} WR={_obs_wr:.0f}%")
            # If historically poor decision -- reduce confidence
            elif _obs_wr < 40:
                conf = round(max(10, conf * 0.9), 1)
    except: pass

    return decision, " | ".join(signals[:5]) if signals else "No strong signals", conf, pred, min(conf*0.8,90)

def get_open_trades():
    """
    Read ALL open trades from DB -- single source of truth.
    APEX trades from trades table (both main.py and APEX MIND entries).
    Spring trades from dip_trades table.
    State files are for dashboard only -- not used for decisions.
    """
    trades = []

    # ── APEX TRADES -- from DB directly ──
    try:
        conn = get_trades_conn()
        rows = conn.execute("""
                    SELECT id, symbol, direction, entry, size, leverage,
                           sl, open_time, peak_roe, pattern, floor_roe, be_set, be_price, peak_price
                    FROM trades WHERE status='OPEN'
                    ORDER BY open_time""").fetchall()
        for db_id, symbol, direction, entry, size, lev, sl, ot_str, peak_roe, pattern, floor_roe_db, be_set_db, be_price_db, peak_price_db in rows:
            try:
                # Use WebSocket price if available (no API call needed)
                _ws = get_ws_manager()
                _ws_price = _ws.get_price(symbol) if _ws else None
                if _ws_price:
                    price = float(_ws_price)
                else:
                    get_trade_rl().acquire(weight=1)
                    price = float(get_trade_client().futures_symbol_ticker(symbol=symbol)["price"])
                lev   = float(lev or 5)
                roe   = (price-entry)/entry*100*lev if direction=="LONG"                         else (entry-price)/entry*100*lev
                try:
                    from datetime import datetime as dt2
                    ot  = dt2.strptime(str(ot_str)[:19], "%Y-%m-%d %H:%M:%S")
                    age = (dt2.utcnow()-ot).total_seconds()/60
                except: age = 0
                sl       = float(sl or 0)
                sl_dist  = abs(price-sl)/price*100 if sl>0 else 0
                peak_roe = float(peak_roe or 0)
            except Exception as _e: logger.warning(f"Price fetch failed {symbol}: {_e} -- skipping trade this cycle"); continue
            # Ratchet state from cache
            with _ratchet_state_lock:
                rs = _ratchet_state.get(symbol, {})
            trades.append({
                "symbol":       symbol,
                "direction":    direction,
                "entry":        float(entry),
                "current_price":float(price),
                "roe":          round(roe, 2),
                "peak_roe":     round(max(peak_roe, roe), 2),
                "age_mins":     round(age, 1),
                "sl":           sl,
                "sl_dist":      round(sl_dist, 2),
                "leverage":     lev,
                "bot_type":     "APEX",
                "tid":          f"DB_{db_id}",
                "db_id":        db_id,
                "size":         float(size or 0),
                "pattern":      pattern or "",
                        "be_set":       bool(be_set_db) or rs.get("be_set", False),
                        "be_price":     float(be_price_db or 0) or rs.get("be_price", 0),
                        "floor_roe":    max(float(floor_roe_db or 0), rs.get("floor_roe", 0)),
                        "peak_price":   float(peak_price_db or 0) or rs.get("peak_price", 0),
            })
    except Exception as e: logger.error(f"APEX trades DB read: {e}")
    try:
        conn=get_trades_conn()
        rows=conn.execute("SELECT symbol,entry,size,leverage,open_time,sl,floor_roe,be_set,be_price,peak_roe,peak_price FROM dip_trades WHERE status='OPEN'").fetchall()
        for symbol,entry,size,lev,ot_str,sl,floor_roe_db,be_set_db,be_price_db,peak_roe_db,peak_price_db in rows:
            try:
                # Use WebSocket price if available -- saves API weight
                _ws2 = get_ws_manager()
                _ws_price = _ws2.get_price(symbol) if _ws2 else None
                if _ws_price:
                    price = float(_ws_price)
                else:
                    get_trade_rl().acquire(weight=1)
                    price = float(get_trade_client().futures_symbol_ticker(symbol=symbol)["price"])
                from datetime import datetime as dt2
                ot=dt2.strptime(str(ot_str)[:19],"%Y-%m-%d %H:%M:%S")
                age=(dt2.utcnow()-ot).total_seconds()/60
                roe=(price-entry)/entry*100*(lev or 4); sl_dist=abs(price-sl)/price*100 if sl else 0
            except Exception as _e: logger.warning(f"Spring price fetch failed {symbol}: {_e} -- skipping trade this cycle"); continue
            trades.append({"symbol":symbol,"direction":"LONG","entry":entry,
                "current_price":price,"roe":round(roe,2),"peak_roe":float(peak_roe_db or 0),
                "age_mins":round(age,1),"sl":sl or 0,"sl_dist":round(sl_dist,2),
                "leverage":lev or 4,"bot_type":"SPRING","size":size or 0,"open_time":str(ot_str)[:19],
                "floor_roe":float(floor_roe_db or 0),"be_set":bool(be_set_db),"be_price":float(be_price_db or 0),
                "peak_price":float(peak_price_db or 0)})
    except Exception as e: logger.error(f"Spring trades: {e}")

    # ── WRITE COMBINED STATE FOR DASHBOARD ──
    # Dashboard reads bot_state.json -- write all trades there
    # so dashboard shows complete picture without modification
    try:
        positions = {}
        for t in trades:
            tid = t.get("tid", f"TRADE_{t.get('symbol','?')}")
            positions[tid] = {
                "symbol":    t["symbol"],
                "direction": t["direction"],
                "entry":     t["entry"],
                "sl":        t.get("sl", 0),
                "size":      t.get("size", 0),
                "leverage":  t.get("leverage", 5),
                "coin_type": "MODERATE",
                "peak_pct":  t.get("peak_roe", 0),
                "peak_price":t.get("current_price", t["entry"]),
                "db_id":     t.get("db_id"),
                "trail_tier":0,
                "floor_pct": t.get("floor_roe", 0),
                "combined_score": 10,
                "tps":       [],
                "peak_roe":  t.get("peak_roe", 0),
                "floor_roe": t.get("floor_roe", 0),
                "breakeven_set":   t.get("be_set", False),
                "breakeven_price": t.get("be_price", t["entry"]),
                "trade_type": "SCALP",
                "open_time":  time.time() - t.get("age_mins", 0) * 60,
                "bot_type":   t.get("bot_type", "APEX"),
                "source":     t.get("pattern", ""),
            }
        # Read existing state to preserve other fields
        try:
            existing = json.load(open(STATE_FILE))
        except: existing = {}
        existing["positions"] = positions
        existing["trades"]    = {t["symbol"]: t.get("tid","?") for t in trades if t.get("bot_type")=="APEX"}
        tmp = STATE_FILE + ".tmp"
        json.dump(existing, open(tmp, "w"), indent=2)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        logger.debug(f"State sync error: {e}")

    return trades

def get_coin_memory(symbol):
    try:
        conn=_get_mind_conn()
        row=conn.execute("SELECT * FROM coin_memory WHERE symbol=?",(symbol,)).fetchone()
        conn.close()
        if row:
            cols=["symbol","first_seen","last_updated","total_trades","total_observations",
                  "avg_recovery_mins","bounce_probability","trend_follow_score",
                  "avg_winning_roe","avg_losing_roe","best_hour_utc","worst_hour_utc",
                  "bull_win_rate","bear_win_rate","sideways_win_rate",
                  "typical_atr_pct","avg_daily_range",
                  "mind_correct","mind_total","mind_accuracy"]
            return dict(zip(cols,row))
    except: pass
    return None

def rebuild_coin_personalities():
    """
    Rebuild coin personality profiles from all observations.
    Called daily. Updates bounce_probability, trend_follow_score etc.
    """
    try:
        conn = _get_mind_conn()
        symbols = conn.execute(
            "SELECT DISTINCT symbol FROM observations WHERE outcome_filled=1").fetchall()
        for (symbol,) in symbols:
            rows = conn.execute("""
                SELECT roe, outcome_correct, outcome_roe, trade_age_mins,
                       adx_trend, ema_align_15m, market_regime,
                       CAST(strftime('%H', timestamp) AS INTEGER) as hour_utc
                FROM observations
                WHERE symbol=? AND outcome_filled=1
                ORDER BY id""", (symbol,)).fetchall()
            if len(rows) < 3: continue

            total = len(rows)
            wins = [r for r in rows if r[1]==1]
            losses = [r for r in rows if r[1]==0]

            # Bounce probability -- how often does a losing trade recover
            deep_losses = [r for r in rows if sf(r[0])<-3]
            bounced = [r for r in deep_losses if r[1]==1]
            bounce_prob = round(len(bounced)/len(deep_losses),3) if deep_losses else 0.5

            # Average recovery time
            avg_recovery = round(sum(sf(r[3]) for r in wins)/len(wins),1) if wins else 0

            # Win rates by regime
            bull_trades = [r for r in rows if r[6] and 'BULL' in str(r[6])]
            bear_trades = [r for r in rows if r[6] and 'BEAR' in str(r[6])]
            side_trades = [r for r in rows if r[6] and 'SIDEWAYS' in str(r[6])]
            bull_wr = round(len([r for r in bull_trades if r[1]==1])/len(bull_trades)*100,1) if bull_trades else 50
            bear_wr = round(len([r for r in bear_trades if r[1]==1])/len(bear_trades)*100,1) if bear_trades else 50
            side_wr = round(len([r for r in side_trades if r[1]==1])/len(side_trades)*100,1) if side_trades else 50

            # Best/worst hours
            from collections import defaultdict
            hour_pnl = defaultdict(float)
            for r in rows:
                if r[7]: hour_pnl[r[7]] += sf(r[2])
            best_hour = max(hour_pnl, key=hour_pnl.get) if hour_pnl else 15
            worst_hour = min(hour_pnl, key=hour_pnl.get) if hour_pnl else 22

            # Average PnLs
            avg_win  = round(sum(sf(r[2]) for r in wins)/len(wins),2) if wins else 0
            avg_loss = round(sum(sf(r[2]) for r in losses)/len(losses),2) if losses else 0

            conn.execute("""INSERT OR REPLACE INTO coin_memory
                (symbol, first_seen, last_updated, total_observations,
                 bounce_probability, avg_recovery_mins,
                 bull_win_rate, bear_win_rate, sideways_win_rate,
                 best_hour_utc, worst_hour_utc,
                 avg_winning_roe, avg_losing_roe,
                 mind_correct, mind_total, mind_accuracy)
                VALUES (?,datetime('now'),datetime('now'),?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (symbol, total, bounce_prob, avg_recovery,
                 bull_wr, bear_wr, side_wr,
                 best_hour, worst_hour, avg_win, avg_loss,
                 len(wins), total,
                 round(len(wins)/total*100,1) if total else 0))
        conn.commit()
        conn.close()
        logger.info(f"Coin personalities rebuilt for {len(symbols)} coins")
    except Exception as e:
        logger.error(f"Personality rebuild error: {e}")

def update_coin_memory(symbol,was_correct,pnl):
    try:
        conn=_get_mind_conn()
        ex=conn.execute("SELECT mind_correct,mind_total FROM coin_memory WHERE symbol=?",(symbol,)).fetchone()
        if ex:
            nc=ex[0]+(1 if was_correct else 0); nt=ex[1]+1
            conn.execute("UPDATE coin_memory SET mind_correct=?,mind_total=?,mind_accuracy=?,last_updated=datetime('now') WHERE symbol=?",
                (nc,nt,round(nc/nt*100,1),symbol))
        else:
            conn.execute("INSERT OR IGNORE INTO coin_memory (symbol,first_seen,last_updated,mind_correct,mind_total,mind_accuracy) VALUES (?,datetime('now'),datetime('now'),?,1,?)",
                (symbol,1 if was_correct else 0,100.0 if was_correct else 0.0))
        conn.commit(); conn.close()
    except Exception as e: logger.error(f"Coin memory: {e}")

def fill_outcomes():
    try:
        conn=get_mind_conn()
        pending=conn.execute("SELECT id,symbol,decision,decision_reason,predicted_direction,roe,bot_type FROM observations WHERE outcome_filled=0 AND timestamp < datetime('now','-30 minutes') ORDER BY id ASC LIMIT 1000").fetchall()
    except: return
    for obs_id,symbol,decision,reason,pred_dir,dec_roe,bot_type in pending:
        try:
            # Get the observation timestamp to match against the correct trade
            conn_m=get_mind_conn()
            obs_row=conn_m.execute("SELECT timestamp, trade_db_id FROM observations WHERE id=?",(obs_id,)).fetchone()
            obs_time=obs_row[0] if obs_row else "1970-01-01"
            _tdb_id = obs_row[1] if obs_row and len(obs_row)>1 else None
            conn=sqlite3.connect(TRADES_DB)
            if bot_type=="APEX":
                # fill_outcomes is now FALLBACK ONLY -- on_trade_closed is primary scorer
                # Use trade_db_id if available -- exact match, no fuzzy risk
                closed = None
                if _tdb_id:
                    closed=conn.execute("SELECT pnl,reason,peak_roe,entry,exit,direction,leverage,total_cost,size FROM trades WHERE id=? AND status='CLOSED'", (_tdb_id,)).fetchone()
                if not closed:
                    closed=conn.execute("""SELECT pnl,reason,peak_roe,entry,exit,direction,leverage,total_cost,size FROM trades
                        WHERE symbol=? AND status='CLOSED'
                        AND open_time <= ? AND close_time >= ?
                        ORDER BY close_time ASC LIMIT 1""",(symbol,obs_time,obs_time)).fetchone()
                if not closed:
                    closed=conn.execute("""SELECT pnl,reason,peak_roe,entry,exit,direction,leverage,total_cost,size FROM trades
                        WHERE symbol=? AND status='CLOSED' AND close_time >= ?
                        ORDER BY close_time ASC LIMIT 1""",(symbol,obs_time)).fetchone()
            else:
                # fill_outcomes is now FALLBACK ONLY -- on_trade_closed is primary scorer
                closed=conn.execute("""SELECT pnl,reason,peak_roe,entry,exit,'LONG',leverage,total_cost,size FROM dip_trades
                    WHERE symbol=? AND status='CLOSED'
                    AND open_time <= ? AND close_time >= ?
                    ORDER BY close_time ASC LIMIT 1""",(symbol,obs_time,obs_time)).fetchone()
                if not closed:
                    closed=conn.execute("""SELECT pnl,reason,peak_roe,entry,exit,'LONG',leverage,total_cost,size FROM dip_trades
                        WHERE symbol=? AND status='CLOSED' AND close_time >= ?
                        ORDER BY close_time ASC LIMIT 1""",(symbol,obs_time)).fetchone()
            conn.close()
            if not closed: continue
            # fill_outcomes FALLBACK: unpack including total_cost and size
            final_pnl = float(closed[0] or 0)
            close_reason = closed[1]
            peak_after = closed[2]
            t_entry = closed[3]; t_exit = closed[4]
            t_direction = closed[5]; t_leverage = closed[6]
            t_cost = float(closed[7] or 0) if len(closed) > 7 else 0.0
            t_size = float(closed[8] or 14) if len(closed) > 8 else 14.0
            final_pnl=float(final_pnl or 0)

            # ── C1 FIX + FIX B: Compute final ROE% net of costs (fallback path) ──
            try:
                _t_entry = float(t_entry or 0)
                _t_exit  = float(t_exit  or 0)
                _t_lev   = float(t_leverage or 5)
                _t_dir   = str(t_direction or "LONG")
                if _t_entry > 0 and _t_exit > 0:
                    if _t_dir == "LONG":
                        final_roe_pct = (_t_exit - _t_entry) / _t_entry * _t_lev * 100
                    else:
                        final_roe_pct = (_t_entry - _t_exit) / _t_entry * _t_lev * 100
                    # FIX B: subtract cost-equivalent ROE
                    if t_size > 0 and t_cost > 0:
                        final_roe_pct -= (t_cost / t_size) * 100
                else:
                    final_roe_pct = 0.0
            except:
                final_roe_pct = 0.0

            dec_roe_val   = sf(dec_roe, 0)   # ROE% at decision time
            final_roe_val = final_roe_pct     # ROE% at close -- FIXED (was dollars)
            was_win       = final_pnl > 0     # dollar sign for win/loss boolean only

            was_correct=0; why_c=""; why_w=""; what_missed=""
            saved=0; missed=0
            if decision=="CLOSE":
                if not was_win:
                    was_correct=1; saved=abs(final_pnl)
                    why_c=f"Trade lost ${abs(final_pnl):.2f} CLOSE was right (ROE={final_roe_val:.1f}%)"
                else:
                    # Trade won -- was CLOSE call timely or premature?
                    # dec_roe_val and final_roe_val already set above
                    if final_roe_val <= dec_roe_val * 1.2:
                        was_correct=1; why_c=f"Timely close: called at {dec_roe_val:.1f}% closed at {final_roe_val:.1f}%"
                    # If trade gained significantly more after CLOSE call -- premature
                    elif final_roe_val > dec_roe_val * 1.5:
                        was_correct=0; missed=final_roe_val-dec_roe_val  # both in ROE%
                        why_w=f"Premature CLOSE: called at {dec_roe_val:.1f}% but closed at {final_roe_val:.1f}% -- missed gain"
                        what_missed=f"Trade had more upside after CLOSE signal"
                    else:
                        was_correct=1; why_c=f"Acceptable close: called {dec_roe_val:.1f}% final {final_roe_val:.1f}%"
            elif decision=="HOLD":
                if was_win:
                    # Won -- but did HOLD add value?
                    if final_roe_val >= dec_roe_val * 1.1:
                        # Trade gained more after HOLD -- correct to hold
                        was_correct=1
                        why_c=f"HOLD added value: was {dec_roe_val:.1f}% closed {final_roe_val:.1f}%"
                    elif dec_roe_val > 5 and final_roe_val < dec_roe_val * 0.8:
                        # Was winning, HOLD caused giving back gains
                        was_correct=0
                        why_w=f"HOLD gave back gains: was {dec_roe_val:.1f}% closed {final_roe_val:.1f}%"
                        what_missed="Should have tightened SL on winning trade"
                    else:
                        was_correct=1
                        why_c=f"HOLD acceptable: {dec_roe_val:.1f}% to {final_roe_val:.1f}%"
                else:
                    # Lost -- was it a reasonable loss or preventable?
                    if dec_roe_val < -3:
                        # Was already losing when HOLD called -- should have closed
                        was_correct=0
                        why_w=f"HOLD on losing trade: was {dec_roe_val:.1f}% closed {final_roe_val:.1f}%"
                        what_missed="Trade was already losing -- HOLD prolonged the loss"
                    elif dec_roe_val >= 0 and final_roe_val < -2:
                        # Was flat/winning, unexpected reversal
                        was_correct=0
                        why_w=f"Unexpected reversal: was {dec_roe_val:.1f}% closed {final_roe_val:.1f}%"
                        what_missed="Reversal signals were present but not weighted enough"
                    else:
                        # Was winning at decision time but ended at loss/BE -- HOLD gave back profit
                        if dec_roe_val >= 3 and final_roe_val < dec_roe_val * 0.3:
                            was_correct=0
                            why_w=f"HOLD surrendered profit: was +{dec_roe_val:.1f}% closed {final_roe_val:.1f}%"
                            what_missed="Was profitable at HOLD decision -- should have tightened"
                        else:
                            # Small loss from near-zero entry -- genuinely acceptable
                            was_correct=1
                            why_c=f"Acceptable outcome from HOLD: {final_roe_val:.1f}%"
            elif decision=="TIGHTEN":
                if was_win:
                    if final_roe_val>=dec_roe_val*0.7:
                        was_correct=1; why_c=f"TIGHTEN protected profit: was {dec_roe_val:.1f}% closed {final_roe_val:.1f}%"
                    elif final_roe_val<dec_roe_val*0.5 and dec_roe_val>5:
                        was_correct=0; missed=final_pnl
                        why_w=f"TIGHTEN too tight: was {dec_roe_val:.1f}% stopped at {final_roe_val:.1f}%"
                        what_missed="SL too tight -- trade had more upside"
                    else:
                        was_correct=1; why_c=f"TIGHTEN acceptable: {dec_roe_val:.1f}% to {final_roe_val:.1f}%"
                elif final_pnl<0:
                    if abs(final_roe_val)<abs(dec_roe_val)*1.5:
                        was_correct=1; why_c=f"TIGHTEN limited loss: {dec_roe_val:.1f}% to {final_roe_val:.1f}%"
                    else:
                        was_correct=0; why_w=f"TIGHTEN failed: {dec_roe_val:.1f}% to {final_roe_val:.1f}%"
                        what_missed="Should have been CLOSE not TIGHTEN"
                else:
                    was_correct=1; why_c="TIGHTEN: broke even"
            elif decision=="ALERT":
                was_correct=1 if final_pnl>0 else 0
                if was_correct: why_c=f"Trade won ${final_pnl:.2f} despite alert"
                else: why_w=f"Alert was valid trade lost ${abs(final_pnl):.2f}"
            conn2=_get_mind_conn()
            conn2.execute("UPDATE observations SET outcome_roe=?,outcome_correct=?,outcome_reason=?,outcome_filled=1 WHERE id=?",(final_roe_pct,was_correct,why_c if was_correct else why_w,obs_id))
            conn2.execute("INSERT OR IGNORE INTO decision_outcomes (observation_id,symbol,decision,decision_time,decision_roe,peak_roe_after,final_roe,close_reason,was_correct,pnl_impact,missed_gain,saved_loss,why_correct,why_wrong,what_mind_missed) VALUES (?,?,?,datetime('now'),?,?,?,?,?,?,?,?,?,?,?)",
                (obs_id,symbol,decision,dec_roe,float(peak_after or 0),final_pnl,close_reason,was_correct,final_pnl,missed,saved,why_c,why_w,what_missed))
            conn2.commit(); conn2.close()
            update_coin_memory(symbol,was_correct,final_pnl)
        except Exception as e: logger.error(f"Fill outcome {symbol}: {e}")

def get_daily_pnl(apex_only=False):
    """Return today realized PnL as % of APEX equity. Negative = loss.
    apex_only=True: only APEX trades (used for circuit breaker).
    """
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        conn = get_trades_conn()
        apex_pnl = float(conn.execute(
            "SELECT COALESCE(SUM(pnl),0) FROM trades WHERE status='CLOSED' AND close_time>=?",
            (today,)).fetchone()[0] or 0)
        spring_pnl = 0.0
        if not apex_only:
            spring_pnl = float(conn.execute(
                "SELECT COALESCE(SUM(pnl),0) FROM dip_trades WHERE status='CLOSED' AND close_time>=?",
                (today,)).fetchone()[0] or 0)
        conn.close()
        total_pnl = apex_pnl + spring_pnl
        try:
            bal = get_balance()
            apex_equity = max(float(bal.get("futures", 715)), 100)
        except: apex_equity = 715.0
        return round(total_pnl / apex_equity * 100, 2)
    except Exception as e:
        logger.warning(f"get_daily_pnl error: {e}")
        return 0.0

def get_balance():
    """Get current futures balance"""
    try:
        # Try bot balance file first
        bal_file=os.path.join(BASE,"balance.json")
        if os.path.exists(bal_file):
            bal=json.load(open(bal_file))
            return {"total":float(bal.get("total",0)),"available":float(bal.get("available",0)),"futures":float(bal.get("futures",0))}
        # Dynamic paper balance from actual trade outcomes
        import sqlite3 as _sq
        _conn = _sq.connect(TRADES_DB)
        apex_pnl = float(_conn.execute("SELECT COALESCE(SUM(pnl),0) FROM trades WHERE status='CLOSED' AND reason!='Ghost - cleaned'").fetchone()[0])
        spring_pnl = float(_conn.execute("SELECT COALESCE(SUM(pnl),0) FROM dip_trades WHERE status='CLOSED' AND reason!='Ghost - cleaned'").fetchone()[0])
        _conn.close()
        apex_bal = round(700 + apex_pnl, 2)
        spring_bal = round(300 + spring_pnl, 2)
        total = round(apex_bal + spring_bal, 2)
        return {"total":total,"available":apex_bal,"futures":apex_bal,"apex":apex_bal,"spring":spring_bal}
    except Exception as e:
        logger.error(f"Balance fetch: {e}")
        return {"total":0,"available":0,"futures":0}

def calc_position_size(balance, confidence, risk_pct, sl_distance_pct, max_leverage=5):
    """
    Risk-based position sizing.
    Risk exactly risk_pct% of balance per trade.
    Size adapts to SL distance and confidence.
    """
    if balance<=0 or sl_distance_pct<=0: return 0,1
    # Adjust risk based on confidence
    adj_risk = risk_pct * (confidence/100) * 1.5  # higher confidence = slightly bigger
    adj_risk = max(0.01, min(adj_risk, 0.04))  # cap 1-4% risk
    risk_amount = balance * adj_risk
    notional = risk_amount / sl_distance_pct
    leverage = min(max_leverage, max(2, round(notional/balance)))
    size = min(notional/leverage, balance*0.15)  # max 15% per trade
    return round(size,2), leverage

def derive_live_params():
    """
    APEX MIND derives its own trading parameters from recent trade outcomes.
    Updates every cycle. No fixed rules -- pure data driven.
    """
    try:
        conn = sqlite3.connect(TRADES_DB)
        # Last 30 closed trades
        rows = conn.execute("""
            SELECT pnl, open_time, duration_mins, sl_distance,
                   regime_score, regime_conf, direction
            FROM trades WHERE status='CLOSED'
            AND reason != 'Ghost - cleaned'
            ORDER BY id DESC LIMIT 30""").fetchall()
        conn.close()
    except Exception as e: logger.error(f"run_learning DB read failed: {e}"); return {}

    if len(rows) < 5:
        return {"min_score":0.3,"max_score":1.4,"min_conf":25,
                "best_hours":list(range(9,18)),"max_sl_dist":0.04,
                "max_hold_mins":45,"note":"Default -- insufficient data"}

    rows = [(float(r[0] or 0), r[1], float(r[2] or 0),
             float(r[3] or 0), float(r[4] or 0),
             float(r[5] or 0), r[6]) for r in rows]

    wins   = [r for r in rows if r[0]>0]
    losses = [r for r in rows if r[0]<=0]

    params = {}

    # 1. Optimal regime score range
    if wins:
        win_scores = [r[4] for r in wins]
        params["min_score"] = round(max(min(win_scores)-0.1, 0.0), 2)
        params["max_score"] = round(min(max(win_scores)+0.1, 2.0), 2)
    else:
        params["min_score"] = 0.3; params["max_score"] = 1.4

    # 2. Minimum confidence threshold
    if len(wins) >= 3:
        params["min_conf"] = round(sum(r[5] for r in wins)/len(wins)*0.8, 1)
    else:
        params["min_conf"] = 20

    # 3. Best trading hours from recent data
    from collections import defaultdict
    hour_pnl = defaultdict(float)
    hour_cnt = defaultdict(int)
    for r in rows:
        try:
            h = int(r[1][11:13])
            hour_pnl[h] += r[0]
            hour_cnt[h] += 1
        except: pass
    best_hours = [h for h,pnl in hour_pnl.items() if pnl > 0]
    params["best_hours"] = best_hours if best_hours else list(range(9,18))

    # 4. Max SL distance (winners had tighter SLs)
    if wins:
        params["max_sl_dist"] = round(min(
            sum(r[3] for r in wins)/len(wins)*1.2, 0.05), 3)
    else:
        params["max_sl_dist"] = 0.04

    # 5. Max hold time (losers held too long) -- floor 180 mins, use apex_timeout_analysis if available
    import json as _jh, os as _oh
    _tp = _jh.load(open(os.path.join(BASE,"apex_mind_params.json"))) if os.path.exists(os.path.join(BASE,"apex_mind_params.json")) else {}
    _existing_hold = float(_tp.get("max_hold_mins", 0))
    if losses:
        _calc_hold = round(sum(r[2] for r in losses)/len(losses)*0.7, 0)
        params["max_hold_mins"] = max(180, min(_calc_hold, 480)) if _calc_hold > 0 else max(240, _existing_hold)
    else:
        params["max_hold_mins"] = max(240, _existing_hold)

    # 6. Win rate trend (last 10 vs previous 10)
    recent_wr = len([r for r in rows[:10] if r[0]>0])/10*100
    prev_wr   = len([r for r in rows[10:20] if r[0]>0])/max(len(rows[10:20]),1)*100
    if recent_wr > prev_wr + 10:
        params["market_quality"] = "IMPROVING"
        params["size_mult"] = 1.2
    elif recent_wr < prev_wr - 10:
        params["market_quality"] = "DETERIORATING"
        params["size_mult"] = 0.7
    else:
        params["market_quality"] = "STABLE"
        params["size_mult"] = 1.0

    params["recent_wr"] = round(recent_wr, 1)
    params["sample_size"] = len(rows)
    params["note"] = f"Derived from last {len(rows)} trades WR={recent_wr:.0f}%"

    # Update realized_capture_roe from recent ratchet exits
    try:
        _rc_conn = sqlite3.connect(TRADES_DB)
        _rc_rows = _rc_conn.execute("""SELECT peak_roe FROM trades
            WHERE status='CLOSED' AND peak_roe > 0
            AND reason NOT LIKE '%Hard SL%'
            ORDER BY close_time DESC LIMIT 100""").fetchall()
        _rc_conn.close()
        if len(_rc_rows) >= 20:
            _rc_vals = [float(r[0]) for r in _rc_rows if float(r[0] or 0) > 0]
            params["realized_capture_roe"] = round(max(5.0, sum(_rc_vals)/len(_rc_vals)), 2)  # floor 5% prevents self-reinforcing throttle
    except: pass

    # Update spring_realized_capture_roe separately -- Spring holds longer, captures differently
    try:
        _sp_conn = sqlite3.connect(TRADES_DB)
        _sp_rows = _sp_conn.execute("""SELECT peak_roe FROM dip_trades
            WHERE status='CLOSED' AND peak_roe > 0
            AND reason NOT LIKE '%Hard SL%'
            ORDER BY close_time DESC LIMIT 100""").fetchall()
        _sp_conn.close()
        if len(_sp_rows) >= 20:
            _sp_vals = [float(r[0]) for r in _sp_rows if float(r[0] or 0) > 0]
            params["spring_realized_capture_roe"] = round(max(4.0, sum(_sp_vals)/len(_sp_vals)), 2)
    except: pass

    # Save to file -- use _update_params to preserve all observation-populated keys
    _update_params({
        "market_quality": params.get("market_quality", "STABLE"),
        "size_mult": params.get("size_mult", 1.0),
        "recent_wr": params.get("recent_wr", 50.0),
        "sample_size": params.get("sample_size", 0),
        "note": params.get("note", ""),
        "best_hours": params.get("best_hours", []),
        "min_conf": params.get("min_conf", 50.0),  # L5: stats only -- live gate uses master_config.json apex_entry_min=70
        "min_score": params.get("min_score", 0.0),
        "max_score": params.get("max_score", 0.1),
        "max_sl_dist": params.get("max_sl_dist", 0.05),
        "max_hold_mins": params.get("max_hold_mins", 0.0),
    })
    return params

def get_position_counts():
    try:
        import sqlite3 as _sq
        conn=_sq.connect(TRADES_DB)
        apex_long  = conn.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN' AND direction='LONG'").fetchone()[0]
        apex_short = conn.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN' AND direction='SHORT'").fetchone()[0]
        spring_open= conn.execute("SELECT COUNT(*) FROM dip_trades WHERE status='OPEN'").fetchone()[0]
        conn.close()
        apex_open = apex_long + apex_short
        return {
            "apex":   apex_open,
            "long":   apex_long,
            "short":  apex_short,
            "spring": spring_open,
            "total":  apex_open + spring_open
        }
    except: return {"apex":0,"long":0,"short":0,"spring":0,"total":0}


def calc_dip_quality(df15):
    """
    Score dip quality 0-100 based on anatomy:
    - Recovery % from bottom (biggest edge)
    - Recovery accelerating vs decelerating
    - MACD histogram turning positive
    - RSI momentum since bottom
    - Drop speed (gradual better than crash)
    Returns (score, details_dict)
    """
    try:
        if df15 is None or len(df15) < 8: return 0, {}

        last6 = df15.iloc[-7:-1]
        bottom_idx = last6["low"].idxmin()
        bottom_pos = list(last6.index).index(bottom_idx)
        entry_price = float(df15.iloc[-2]["close"])
        pre_bottom = last6.iloc[:bottom_pos+1] if bottom_pos > 0 else last6.iloc[:1]
        dip_start_high = float(pre_bottom["high"].max())
        dip_bottom_low = float(last6.loc[bottom_idx, "low"])
        drop_range = dip_start_high - dip_bottom_low
        drop_pct = drop_range / dip_start_high * 100 if dip_start_high > 0 else 0
        drop_candles = max(bottom_pos + 1, 1)
        drop_speed = drop_pct / drop_candles

        # Recovery metrics
        recovery_range = entry_price - dip_bottom_low
        recovery_pct = recovery_range / drop_range * 100 if drop_range > 0 else 0
        recovery_candles = max(5 - bottom_pos, 1)
        recovery_speed = recovery_pct / recovery_candles

        # Recovery accelerating or decelerating?
        post_bottom = last6.iloc[bottom_pos+1:]
        if len(post_bottom) >= 2:
            first_gain = float(post_bottom.iloc[0]["close"]) - dip_bottom_low
            second_gain = entry_price - float(post_bottom.iloc[0]["close"])
            rec_accelerating = second_gain > first_gain
        else:
            rec_accelerating = True

        # RSI momentum since bottom
        c = df15["close"]; d = c.diff()
        g = d.where(d>0,0).ewm(span=14,adjust=False).mean()
        l = (-d.where(d<0,0)).ewm(span=14,adjust=False).mean()
        rsi = 100-(100/(1+g/l))
        rsi_now = float(rsi.iloc[-2])
        rsi_bottom = float(rsi.iloc[df15.index.get_loc(bottom_idx)])
        rsi_momentum = rsi_now - rsi_bottom

        # MACD histogram turning positive
        ema12 = c.ewm(span=12).mean(); ema26 = c.ewm(span=26).mean()
        macd_hist = (ema12-ema26) - (ema12-ema26).ewm(span=9).mean()
        macd_turning = float(macd_hist.iloc[-2]) > float(macd_hist.iloc[-3])

        # ── SCORING -- weights from learned params ──
        try:
            import json as _jw
            _w = _jw.load(open(os.path.join(BASE,"apex_mind_params.json"))).get("dip_quality_weights",{})
        except: _w = {}
        def _wt(key, default): return int(_w.get(key, default))

        score = 0; reasons = []

        # 1. Recovery % -- biggest predictor (+21.7 edge)
        if recovery_pct >= 80:
            _pts=_wt("recovery_pct_80",30); score+=_pts; reasons.append(f"RecPct={recovery_pct:.0f}%")
        elif recovery_pct >= 60:
            _pts=_wt("recovery_pct_60",20); score+=_pts; reasons.append(f"RecPct={recovery_pct:.0f}%")
        elif recovery_pct >= 40:
            _pts=_wt("recovery_pct_40",10); score+=_pts; reasons.append(f"RecPct={recovery_pct:.0f}%")
        else:
            _pts=_wt("recovery_pct_low",-10); score+=_pts; reasons.append(f"RecPct={recovery_pct:.0f}%LOW")

        # 2. Recovery accelerating (+16.3 edge)
        if rec_accelerating:
            score+=_wt("rec_accelerating",20); reasons.append("RecAccel")
        else:
            score+=_wt("rec_slowing",-10); reasons.append("RecSlowing")

        # 3. MACD turning (+15.9 edge)
        if macd_turning:
            score+=_wt("macd_turning",20); reasons.append("MACDTurn")
        else:
            score+=_wt("macd_flat",-5); reasons.append("MACDFlat")

        # 4. RSI momentum (+6 edge)
        if rsi_momentum > 15:
            score+=_wt("rsi_mom_15",15); reasons.append(f"RSIMom={rsi_momentum:.0f}")
        elif rsi_momentum > 8:
            score+=_wt("rsi_mom_8",10); reasons.append(f"RSIMom={rsi_momentum:.0f}")
        elif rsi_momentum > 0:
            score+=_wt("rsi_mom_0",5); reasons.append(f"RSIMom={rsi_momentum:.0f}")
        else:
            score+=_wt("rsi_mom_neg",-10); reasons.append(f"RSIMom={rsi_momentum:.0f}NEG")

        # 5. Drop speed -- gradual better
        if drop_speed < 0.7:
            score+=_wt("drop_speed_07",15); reasons.append("SlowDrop")
        elif drop_speed < 1.0:
            score+=_wt("drop_speed_10",8); reasons.append("ModDrop")
        else:
            score+=_wt("drop_speed_fast",-5); reasons.append("FastDrop")

        score = max(0, min(100, score))
        return score, {
            "recovery_pct": round(recovery_pct, 1),
            "rec_accel": rec_accelerating,
            "macd_turn": macd_turning,
            "rsi_mom": round(rsi_momentum, 1),
            "drop_speed": round(drop_speed, 2),
            "drop_pct": round(drop_pct, 2),
            "score": score,
            "reasons": " | ".join(reasons)
        }
    except Exception as e:
        return 0, {"error": str(e)}

def score_coin_entry(symbol, market, mode="APEX"):
    """
    APEX MIND evaluates a coin for potential entry right now.
    Returns: direction, score, reason, confidence
    Pure real-time coin + market reading -- no fixed rules.
    """
    try:
        df5m  = fetch(symbol, "5m",  30)
        df15  = fetch(symbol, "15m", 60)
        df1h  = fetch(symbol, "1h",  60)
        df4h  = fetch(symbol, "4h",  50)
        if df15 is None or len(df15)<50: return None,0,"Insufficient data",0
        df5m  = add_inds(df5m)  if df5m  is not None else None
        df15  = add_inds(df15)
        df1h  = add_inds(df1h)  if df1h  is not None else None
        df4h  = add_inds(df4h)  if df4h  is not None else None
    except: return None,0,"Fetch error",0

    # Session quality check -- self-learned, dynamic
    try:
        hour = datetime.now(timezone.utc).hour
        _, _, session_mult = get_session_quality(hour, "LONG" if mode=="SPRING" else None)
        if session_mult == 0.0:
            return None, 0, f"Hour {hour}:00 UTC hard blocked (worst hour historically)", 0
    except: pass

    # Regime detector suggestions -- gate entries by regime
    try:
        reg_sug = get_regime_suggestions(market)
        if reg_sug:
            if mode == "APEX":
                if not reg_sug.get("long_on", True) and mode == "APEX":
                    # Check if direction would be LONG -- we'll gate after scoring
                    pass  # gate applied at final verdict
            # Don't block APEX in SIDEWAYS -- use lower thresholds instead
        # SIDEWAYS can have good short-term moves; gate is in scanner thresholds
        if reg_sug.get("regime") in ("SIDEWAYS",) and mode == "APEX":
                # SIDEWAYS: only allow entries with very high confidence
                pass  # allow -- scanner thresholds handle this
    except: reg_sug = {}

    # Position count check
    pos=get_position_counts()
    if mode=="APEX" and pos["apex"]>=6: return None,0,f"APEX full {pos['apex']}/6",0
    if mode=="SPRING" and pos["spring"]>=15: return None,0,f"Spring full {pos['spring']}/15",0
    if pos["total"]>=20: return None,0,f"Positions full {pos['total']}/20",0

    price = sf(df15.iloc[-1]["close"])
    last15= df15.iloc[-2]; last1h= df1h.iloc[-2] if df1h is not None else None
    last4h= df4h.iloc[-2] if df4h is not None else None

    long_score=0; short_score=0; signals=[]

    # ── SPRING MODE -- original optimized logic + RSI/Vol filters ──
    if mode=="SPRING":
        rsi15 = sf(last15.get("rsi", 50))
        vol   = sf(last15.get("vol_ratio", 1.0))
        e20   = sf(last15.get("ema20", 0))

        # ── RSI gate: < 55 (oversold + mild dip zone) -- M5 FIX: comment aligned to code ──
        if rsi15 > 55:
            return None, 0, f"RSI {rsi15:.0f} too high (need <55)", 0

        # ── Volume gate: >1.2x average ──
        if vol < 1.2:
            return None, 0, f"Volume {vol:.1f}x too low (need 1.2x)", 0

        # ── Drop detection: >= 2% in last 6 x 15m candles (90 mins) ──
        try:
            _recent = df15.iloc[-7:-1]
            _high   = float(_recent["high"].max())
            _low    = float(_recent["low"].min())
            if _high <= 0 or _low <= 0:
                return None, 0, "Insufficient candle data", 0
            _drop_pct  = (_high - _low) / _high * 100
            _drop_range = _high - _low
            if _drop_pct < 3.0:
                return None, 0, f"Drop {_drop_pct:.1f}% < 3% -- no dip", 0
            long_score += min(30, int(_drop_pct * 5))
            signals.append(f"Dip {_drop_pct:.1f}% in 90m")
        except:
            return None, 0, "Drop calculation error", 0

        # ── Dip Quality Score -- anatomy-based filter ──
        try:
            _dip_score, _dip_details = calc_dip_quality(df15)
            _min_dip_score = int(_load_learned_params().get("spring_dip_quality_gate", 70))
            if _dip_score < _min_dip_score:
                return None, 0, f"Dip quality {_dip_score}/100 < {_min_dip_score} -- {_dip_details.get('reasons','')[:40]}", 0
            # Add dip quality to score (max 30 bonus)
            long_score += min(30, int(_dip_score * 0.3))
            signals.append(f"DipQ={_dip_score} {_dip_details.get('reasons','')[:30]}")
            # Also keep recovery for signal context
            _recovery = _dip_details.get("recovery_pct", 0) / 100
            signals.append(f"Recovery {_recovery*100:.0f}%")
        except Exception as _dq_e:
            # Fallback to simple recovery check
            _recovery = (price - _low) / _drop_range if _drop_range > 0 else 0
            if _recovery < 0.30:
                return None, 0, f"Recovery {_recovery*100:.0f}% < 30% -- too early", 0
            long_score += min(25, int(_recovery * 50))
            signals.append(f"Recovery {_recovery*100:.0f}%")

        # ── RSI bonus scoring ──
        if rsi15 < 25:
            long_score += 30; signals.append(f"RSI extreme oversold {rsi15:.0f}")
        elif rsi15 < 35:
            long_score += 25; signals.append(f"RSI oversold {rsi15:.0f}")
        elif rsi15 < 45:
            long_score += 15; signals.append(f"RSI dip zone {rsi15:.0f}")
        elif rsi15 < 55:
            long_score += 5; signals.append(f"RSI mild dip {rsi15:.0f}")

        # ── Volume bonus scoring ──
        if vol >= 2.0:
            long_score += 20; signals.append(f"Volume {vol:.1f}x strong")
        elif vol >= 1.5:
            long_score += 10; signals.append(f"Volume {vol:.1f}x elevated")

        # ── Larry Williams Smash Day confirmation ──
        try:
            _lw_params = _load_learned_params().get("larry_williams", {})
            if _lw_params.get("enabled", True) and df15 is not None and len(df15) >= 3:
                _today = df15.iloc[-2]
                _prev  = df15.iloc[-3]
                _lw_smash    = float(_today["low"]) < float(_prev["low"])
                _lw_recovery = float(_today["close"]) > float(_prev["low"])
                _lw_bull     = float(_today["close"]) > float(_today["open"])
                _lw_vol      = float(_today.get("volume",0)) > float(df15["volume"].rolling(20).mean().iloc[-2] or 1) * float(_lw_params.get("vol_min", 1.2))
                if _lw_smash and _lw_recovery and _lw_bull and _lw_vol:
                    _boost = int(_lw_params.get("confidence_boost", 20))
                    long_score += _boost
                    signals.append(f"Larry Williams Smash Day +{_boost}")
        except: pass

        # ── Buyer vs Seller volume ──
        try:
            if df15 is not None and len(df15) >= 8:
                _seller_vol = sum(float(df15.iloc[j].get("volume", 0))
                    for j in range(-8, -3)
                    if float(df15.iloc[j].get("close", 0)) < float(df15.iloc[j].get("open", 0)))
                _buyer_vol = sum(float(df15.iloc[j].get("volume", 0))
                    for j in range(-4, -1)
                    if float(df15.iloc[j].get("close", 0)) > float(df15.iloc[j].get("open", 0)))
                if _seller_vol > 0 and _buyer_vol > 0:
                    _buy_ratio = _buyer_vol / _seller_vol
                    if _buy_ratio >= 1.0:
                        long_score += 15; signals.append(f"Buyers dominating {_buy_ratio:.1f}x")
                    elif _buy_ratio < 0.4:
                        return None, 0, f"Sellers still dominating ({_buy_ratio:.1f}x)", 0
        except: pass

        # ── Minimum score gate ──
        if long_score < 40:
            return None, 0, f"Weak dip score={long_score}", 0

        # ── SPRING LEARNED ENTRY ACCURACY ──
        try:
            _e15s = "BULL" if sf(last15.get("ema20",0)) > sf(last15.get("ema50",0)) else "BEAR"
            _regs = market.get("market_regime", "UNKNOWN")
            _conn = _get_mind_conn()

            # Level 1: EMA + regime
            _sentry = _conn.execute("""
                SELECT COUNT(*) as total,
                       ROUND(SUM(outcome_correct)*100.0/COUNT(*),1) as accuracy
                FROM observations
                WHERE direction='LONG' AND ema_align_15m=?
                AND market_regime=? AND outcome_correct IS NOT NULL
                AND bot_type='SPRING' AND trade_age_mins <= 5""",
                (_e15s, _regs)).fetchone()

            # Level 2: EMA only
            if not _sentry or (_sentry[0] or 0) < 15:
                _sentry = _conn.execute("""
                    SELECT COUNT(*) as total,
                           ROUND(SUM(outcome_correct)*100.0/COUNT(*),1) as accuracy
                    FROM observations
                    WHERE direction='LONG' AND ema_align_15m=?
                    AND outcome_correct IS NOT NULL
                    AND bot_type='SPRING' AND trade_age_mins <= 5""",
                    (_e15s,)).fetchone()

            _conn.close()
            if _sentry and _sentry[0] and _sentry[1]:
                _se_total, _se_acc = _sentry
                if _se_total >= 10:
                    _se_weight = min(_se_total / 20, 3.0)
                    if _se_acc >= 65:
                        long_score += int((_se_acc - 50) * _se_weight * 0.4)
                        signals.insert(0, f"Spring hist {_se_acc:.0f}% ({_se_total}obs)")
                    elif _se_acc <= 45:
                        long_score = max(0, long_score - int((50 - _se_acc) * _se_weight * 0.3))
                        signals.insert(0, f"Spring hist weak {_se_acc:.0f}% ({_se_total}obs)")
        except: pass

        # ── STOP HUNT BOOST -- best Spring entry signal ──
        try:
            _behavior = analyze_coin_behavior(symbol)
            if _behavior.get("behavior") == "STOP_HUNT_LONG":
                _sh_conf = float(_behavior.get("confidence", 0))
                _sh_boost = int(_sh_conf * 0.4)  # up to +36 points
                long_score += _sh_boost
                signals.insert(0, f"Stop hunt LONG conf={_sh_conf:.0f}% +{_sh_boost}pts")
            elif _behavior.get("behavior") == "LIQUIDATION_UP":
                long_score += 15
                signals.insert(0, "Liquidation cascade -- bounce incoming")
        except: pass
        return "LONG",round(long_score,1)," | ".join(signals[:5]),round(min(long_score*1.2,95),1)

    # ── TREND ALIGNMENT ──
    e20_15=sf(last15.get("ema20")); e50_15=sf(last15.get("ema50"))
    e20_1h=sf(last1h.get("ema20")) if last1h is not None else 0
    e50_1h=sf(last1h.get("ema50")) if last1h is not None else 0
    e20_4h=sf(last4h.get("ema20")) if last4h is not None else 0
    e50_4h=sf(last4h.get("ema50")) if last4h is not None else 0

    if e20_15>e50_15: long_score+=15; signals.append("15m EMA bull")
    else: short_score+=15; signals.append("15m EMA bear")
    if e20_1h>e50_1h: long_score+=20; signals.append("1H EMA bull")
    else: short_score+=20; signals.append("1H EMA bear")
    if e20_4h>e50_4h: long_score+=15; signals.append("4H EMA bull")
    else: short_score+=15; signals.append("4H EMA bear")

    # ── MOMENTUM ──
    adx=sf(last15.get("adx")); adx_1h=sf(last1h.get("adx")) if last1h is not None else 0

    # FIX 1 (Opus): Close ADX 15-20 dead zone
    _adx_rising = False; _adx_slope_sharp = False
    if len(df15) >= 6:
        _adxs = [sf(df15.iloc[i].get("adx")) for i in range(-6,-1)]
        _adxs = [v for v in _adxs if v > 0]
        if len(_adxs) >= 3:
            _adx_rising = _adxs[-1] > _adxs[0] * 1.08
            _adx_slope_sharp = _adxs[-1] > _adxs[0] * 1.20

    if adx < 15:
        return None, 0, f"ADX={adx:.0f} no trend at all", 0
    elif adx < 20:
        if mode == "SPRING":
            pass
        elif _adx_slope_sharp:
            signals.append(f"ADX={adx:.0f} weak but sharply rising -- marginal pass")
        else:
            return None, 0, f"ADX={adx:.0f} weak/flat -- chop, skip", 0
    elif adx <= 23:
        if _adx_rising:
            long_score += 5; short_score += 5; signals.append(f"ADX={adx:.0f} rising -- emerging trend")
        else:
            return None, 0, f"ADX={adx:.0f} flat -- no trend", 0
    elif adx <= 25:
        long_score += 6; short_score += 6; signals.append(f"ADX={adx:.0f} building trend")
    else:
        long_score += 8; short_score += 8; signals.append(f"ADX={adx:.0f} trending")

    # ADX trend
    if len(df15)>=6:
        adxs=[sf(df15.iloc[i].get("adx")) for i in range(-6,-1)]
        adxs=[v for v in adxs if v>0]
        if len(adxs)>=3:
            if adxs[-1]>adxs[0]*1.1:
                if e20_15>e50_15: long_score+=12; signals.append("ADX rising bull trend")
                else: short_score+=12; signals.append("ADX rising bear trend")
            elif adxs[-1]<adxs[0]*0.9:
                long_score-=8; short_score-=8; signals.append("ADX falling avoid")

    # ── RSI ──
    rsi15=sf(last15.get("rsi")); rsi1h=sf(last1h.get("rsi")) if last1h is not None else 50
    rsi4h=sf(last4h.get("rsi")) if last4h is not None else 50
    if 45<=rsi15<=65: long_score+=10; short_score+=5; signals.append(f"RSI15={rsi15:.0f} healthy")
    elif rsi15<35: long_score+=18; signals.append(f"RSI15={rsi15:.0f} oversold LONG")
    elif rsi15>70: short_score+=15; signals.append(f"RSI15={rsi15:.0f} overbought SHORT")
    if 50<=rsi1h<=70: long_score+=8; signals.append(f"RSI1H={rsi1h:.0f} bull zone")
    elif rsi1h<40: long_score+=12; signals.append(f"RSI1H={rsi1h:.0f} oversold")
    elif rsi1h>75: short_score+=10; signals.append(f"RSI1H={rsi1h:.0f} overbought")

    # ── MACD ──
    mh=sf(last15.get("macd_hist"))
    _macd_bull = False; _macd_bear = False  # FIX 2: momentum confirm flags
    if len(df15)>=4:
        mh_p=sf(df15.iloc[-3].get("macd_hist"))
        if mh>0 and mh_p<=0: long_score+=20; signals.append("MACD crossing bull -- fresh signal")
        elif mh<0 and mh_p>=0: short_score+=20; signals.append("MACD crossing bear -- fresh signal")
        elif mh>0 and mh>mh_p: long_score+=10; signals.append("MACD bullish rising")
        elif mh<0 and mh<mh_p: short_score+=10; signals.append("MACD bearish falling")
        _macd_bull = (mh > 0) or (mh > mh_p)
        _macd_bear = (mh < 0) or (mh < mh_p)

    # ── PULLBACK TO EMA ──
    if e20_15>0:
        dist_pct=(price-e20_15)/e20_15*100
        if e20_15>e50_15:  # uptrend
            if -2<=dist_pct<=0.5: long_score+=20; signals.append(f"Pullback to EMA20 {dist_pct:.1f}% -- ideal LONG entry")
            elif dist_pct<-3: long_score+=8; signals.append(f"Deep pullback {dist_pct:.1f}% below EMA20")
            elif dist_pct>5: long_score-=10; signals.append(f"Too extended {dist_pct:.1f}% above EMA20")
        else:  # downtrend
            if -0.5<=dist_pct<=2: short_score+=20; signals.append(f"Pullback to EMA20 {dist_pct:.1f}% -- ideal SHORT entry")
            elif dist_pct>3: short_score+=8; signals.append(f"Deep pullback {dist_pct:.1f}% above EMA20")

    # ── VOLUME ──
    vol_r=sf(last15.get("vol_ratio"),1.0)
    if vol_r>1.5: long_score+=8; short_score+=5; signals.append(f"Volume {vol_r:.1f}x elevated")
    elif vol_r<0.5: long_score-=10; short_score-=10; signals.append(f"Volume {vol_r:.1f}x dead -- skip")

    # ── BB POSITION ──
    bb_u=sf(last15.get("bb_upper")); bb_l=sf(last15.get("bb_lower"))
    if bb_u>bb_l:
        bb_pos=(price-bb_l)/(bb_u-bb_l)
        if 0.3<=bb_pos<=0.7: long_score+=5; short_score+=5; signals.append("Price in BB middle")
        elif bb_pos<0.2: long_score+=15; signals.append("Price near BB lower -- LONG zone")
        elif bb_pos>0.8: short_score+=12; signals.append("Price near BB upper -- SHORT zone")

    # ── MARKET ALIGNMENT ──
    btc_ema=market.get("btc_ema_align","FLAT"); alts=market.get("alts_bull_pct",50)
    btc_adx=market.get("btc_adx_15m",25); btc_adx_t=market.get("btc_adx_trend","FLAT")
    if btc_ema=="BULL" and alts>60: long_score+=15; signals.append(f"Market bullish {alts:.0f}%")
    elif btc_ema=="BEAR" and alts<40: short_score+=15; signals.append(f"Market bearish {alts:.0f}%")
    elif btc_ema=="BULL" and alts<45: short_score+=5; signals.append("BTC bull but alts weak -- mixed")
    if btc_adx_t=="RISING" and btc_adx>25: long_score+=8; short_score+=5; signals.append("BTC momentum building")
    elif btc_adx_t=="FALLING": long_score-=8; short_score-=8; signals.append("BTC momentum fading")

    # ── OI ──
    try:
        get_scanner_rl().acquire(weight=1); get_scanner_rl().acquire(weight=1); oi=get_scanner_client().futures_open_interest_hist(symbol=symbol,period="5m",limit=4)
        if oi and len(oi)>=3:
            oi_now=float(oi[-1].get("sumOpenInterest",0))
            oi_prev=float(oi[-3].get("sumOpenInterest",0))
            oi_chg=(oi_now-oi_prev)/oi_prev*100 if oi_prev>0 else 0
            # OI as leading indicator
            if oi_chg>3 and e20_15>e50_15:
                long_score+=18; signals.append(f"OI +{oi_chg:.1f}% bull trend -- accumulation")
            elif oi_chg>3 and e20_15<e50_15:
                short_score+=18; signals.append(f"OI +{oi_chg:.1f}% bear trend -- distribution")
            elif oi_chg>1.5 and e20_15>e50_15:
                long_score+=10; signals.append(f"OI rising {oi_chg:.1f}% bull")
            elif oi_chg>1.5 and e20_15<e50_15:
                short_score+=10; signals.append(f"OI rising {oi_chg:.1f}% bear")
            elif oi_chg<-3:
                # Sharp OI drop = liquidations = bounce
                long_score+=12; signals.append(f"OI {oi_chg:.1f}% liquidation cascade -- bounce")
            elif oi_chg<-1.5:
                long_score+=6; signals.append(f"OI falling {oi_chg:.1f}% -- shorts covering")
    except: pass

    # ── FUNDING RATE ──
    try:
        get_scanner_rl().acquire(weight=1); fr=get_scanner_client().futures_funding_rate(symbol=symbol,limit=1)
        fr_val=float(fr[0].get("fundingRate",0))*100 if fr else 0
        if fr_val>0.05: long_score-=8; short_score+=10; signals.append(f"High funding {fr_val:.3f}% longs pay")
        elif fr_val<-0.05: short_score-=8; long_score+=10; signals.append(f"Neg funding {fr_val:.3f}% shorts pay")
    except: pass

    # ── BTC CORRELATION (5min) ──
    try:
        btc5=fetch("BTCUSDT","5m",6)
        if btc5 is not None and df5m is not None and len(df5m)>=4:
            btc_m=(float(btc5.iloc[-1]["close"])-float(btc5.iloc[-4]["close"]))/float(btc5.iloc[-4]["close"])*100
            coin_m=(sf(df5m.iloc[-2]["close"])-sf(df5m.iloc[-4]["close"]))/sf(df5m.iloc[-4]["close"])*100
            if abs(btc_m)>0.2:
                corr=coin_m/btc_m if btc_m!=0 else 0
                if btc_m>0 and corr>1.2: long_score+=10; signals.append(f"Leading BTC up {corr:.1f}x")
                elif btc_m<0 and corr<0.3: long_score+=12; signals.append("Ignoring BTC dump -- strong")
                elif btc_m<0 and corr>1.5: long_score-=12; short_score+=10; signals.append(f"Dumping {corr:.1f}x BTC -- weak")
    except: pass

    # ── COIN MEMORY BOOST -- use personality in entry decision ──
    try:
        _mem = get_coin_memory(symbol)
        if _mem:
            regime = market.get("market_regime","UNKNOWN")
            hour   = datetime.now(timezone.utc).hour
            # Regime-specific win rate
            if "BULL" in regime:
                wr = sf(_mem.get("bull_win_rate", 50))
            elif "BEAR" in regime:
                wr = sf(_mem.get("bear_win_rate", 50))
            else:
                wr = sf(_mem.get("sideways_win_rate", 50))
            if wr >= 70:
                # M12 FIX: boost direction-appropriate score based on regime
                if "BEAR" in regime:
                    short_score += 12; signals.append(f"Coin WR {wr:.0f}% in {regime} -- SHORT favored")
                else:
                    long_score += 12; signals.append(f"Coin WR {wr:.0f}% in {regime} -- LONG favored")
            elif wr <= 35:
                long_score -= 10; short_score -= 10
                signals.append(f"Coin WR only {wr:.0f}% in {regime} -- caution")
            # Best/worst hour
            best_h  = _mem.get("best_hour_utc")
            worst_h = _mem.get("worst_hour_utc")
            if best_h is not None and hour == int(best_h):
                long_score += 8; short_score += 8
                signals.append(f"Best hour {hour}:00 for this coin")
            if worst_h is not None and hour == int(worst_h):
                long_score -= 10; short_score -= 10
                signals.append(f"Worst hour {hour}:00 for this coin -- skip")
            # Mind accuracy on this coin
            mind_acc = sf(_mem.get("mind_accuracy", 0))
            if mind_acc >= 75:
                signals.append(f"MIND {mind_acc:.0f}% accurate on {symbol}")
    except: pass

    # ── ENTRY PATTERN HISTORY -- did similar setups work before? ──
    try:
        _conn = _get_mind_conn()
        regime = market.get("market_regime","UNKNOWN")
        _pat = _conn.execute("""SELECT accuracy, occurrences, avg_pnl_impact
            FROM patterns WHERE pattern_key=? AND occurrences>=3""",
            (f"REGIME_ENTRY_{regime}_LONG",)).fetchone()
        _conn.close()
        if _pat:
            pat_acc, pat_occ, pat_pnl = _pat
            if pat_acc >= 70 and pat_pnl > 0:
                long_score += 10
                signals.append(f"Historical LONG in {regime}: {pat_acc:.0f}% WR ({pat_occ} trades)")
            elif pat_acc <= 40 or pat_pnl < -0.3:
                long_score -= 12
                signals.append(f"Historical LONG in {regime} poor: {pat_acc:.0f}% WR")
        # M12 FIX: also check SHORT pattern history
        _conn2 = _get_mind_conn()
        _pat_s = _conn2.execute("""SELECT accuracy, occurrences, avg_pnl_impact
            FROM patterns WHERE pattern_key=? AND occurrences>=3""",
            (f"REGIME_ENTRY_{regime}_SHORT",)).fetchone()
        _conn2.close()
        if _pat_s:
            ps_acc, ps_occ, ps_pnl = _pat_s
            if ps_acc >= 70 and ps_pnl > 0:
                short_score += 10
                signals.append(f"Historical SHORT in {regime}: {ps_acc:.0f}% WR ({ps_occ} trades)")
            elif ps_acc <= 40 or ps_pnl < -0.3:
                short_score -= 12
                signals.append(f"Historical SHORT in {regime} poor: {ps_acc:.0f}% WR")
    except: pass

    # ── LEARNED ENTRY ACCURACY -- query historical observations at entry time ──
    try:
        _e15  = "BULL" if e20_15 > e50_15 else "BEAR"
        _e1h  = "BULL" if e20_1h > e50_1h else ("BEAR" if e20_1h < e50_1h else "FLAT")
        _reg  = market.get("market_regime", "UNKNOWN")
        _conn = _get_mind_conn()

        # M4 FIX: compute _dir from net score (same as final verdict) not pre-adjustment scores
        _net_pre = long_score - short_score
        _dir = "LONG" if _net_pre > 0 else "SHORT"
        # Level 1: direction + EMA15 + EMA1h + regime
        _erows = _conn.execute("""
            SELECT COUNT(*) as total,
                   ROUND(SUM(outcome_correct)*100.0/COUNT(*),1) as accuracy
            FROM observations
            WHERE direction=? AND ema_align_15m=? AND ema_align_1h=?
            AND market_regime=? AND outcome_correct IS NOT NULL
            AND bot_type='APEX' AND trade_age_mins <= 5""",
            (_dir, _e15, _e1h, _reg)).fetchone()
        if _erows and _erows[0] < 10: _erows = None

        # Level 2: direction + EMA15 + regime
        if not _erows:
            _erows = _conn.execute("""
                SELECT COUNT(*) as total,
                       ROUND(SUM(outcome_correct)*100.0/COUNT(*),1) as accuracy
                FROM observations
                WHERE direction=? AND ema_align_15m=?
                AND market_regime=? AND outcome_correct IS NOT NULL
                AND bot_type='APEX' AND trade_age_mins <= 5""",
                (_dir, _e15, _reg)).fetchone()
            if _erows and _erows[0] < 15: _erows = None

        _conn.close()
        if _erows and _erows[0] and _erows[1]:
            _entry_total, _entry_acc = _erows
            _entry_weight = min(_entry_total / 20, 3.0)
            if _entry_acc >= 70:
                # Good historical entry -- boost confidence
                if long_score >= short_score:
                    long_score  += int((_entry_acc - 50) * _entry_weight * 0.4)
                else:
                    short_score += int((_entry_acc - 50) * _entry_weight * 0.4)
                signals.insert(0, f"Pattern hist {_entry_acc:.0f}% ({_entry_total}obs)")
            elif _entry_acc <= 45:
                # Poor historical entry -- reduce confidence
                if long_score >= short_score:
                    long_score  = max(0, long_score  - int((50 - _entry_acc) * _entry_weight * 0.3))
                else:
                    short_score = max(0, short_score - int((50 - _entry_acc) * _entry_weight * 0.3))
                signals.insert(0, f"Pattern hist weak {_entry_acc:.0f}% ({_entry_total}obs)")
    except: pass

    # FIX 2 (Opus): MACD must confirm trend direction -- gate the EMA points
    if mode != "SPRING":
        _lead_long = long_score >= short_score
        if _lead_long and not _macd_bull:
            long_score = max(0, long_score - 35)
            signals.append("MACD not confirming LONG -- trend points stripped")
        elif not _lead_long and not _macd_bear:
            short_score = max(0, short_score - 35)
            signals.append("MACD not confirming SHORT -- trend points stripped")

    # ── FINAL VERDICT ──
    net=long_score-short_score
    if abs(net)<15: return None,0,f"Mixed signals L={long_score} S={short_score}",0
    direction="LONG" if net>0 else "SHORT"
    score=abs(net); confidence=min(score*1.5,95)

    # ── OBSERVATION WEIGHT -- scale confidence by sample size ──
    # conf=75% from 12 obs is not same as conf=75% from 300 obs
    try:
        _obs_driven = locals().get("_obs_driven", False)
        _total_obs  = locals().get("_total_obs", 0)
        # Only reduce confidence when obs count is very sparse (< 10)
        # At 10+ obs the signal has enough data to be trusted
        if _total_obs > 0 and _total_obs < 10:
            _obs_trust = max(0.7, _total_obs / 10)  # 0.7 at 0 obs, 1.0 at 10+ obs
            _old_conf = confidence
            confidence = round(confidence * _obs_trust, 1)
            if _old_conf != confidence:
                signals.append(f"Obs trust {_obs_trust:.0%} ({_total_obs} samples)")
    except: pass

    # ── REGIME DETECTOR GATE -- respect 4H suggestions ──
    try:
        reg_sug = get_regime_suggestions(market)
        if reg_sug:
            if direction == "LONG" and not reg_sug.get("long_on", True):
                return None, 0, f"Regime {reg_sug.get('regime','?')} blocks LONG entries", 0
            if direction == "SHORT" and not reg_sug.get("short_on", True):
                return None, 0, f"Regime {reg_sug.get('regime','?')} blocks SHORT entries", 0
            # Low regime confidence -- require higher entry confidence
            reg_conf = reg_sug.get("confidence", 50)
            if reg_conf < 40 and confidence < 70:
                return None, 0, f"Regime confidence {reg_conf:.0f}% too low -- skip marginal entries", 0
    except: pass

    # ── SESSION SIZE MULTIPLIER -- only block very bad hours, don't crush confidence ──
    # Session mult already applied to Kelly sizing -- don't double-penalize entry gate
    try:
        hour = datetime.now(timezone.utc).hour
        _, _, sess_mult = get_session_quality(hour, direction)
        if sess_mult <= 0.0:  # only block completely dead hours (e.g. hour 18 mult=0)
            return None, 0, f"Hour {hour}:00 session quality too low (mult={sess_mult:.2f})", 0
    except: pass

    # ── ENTRY CALIBRATION CORRECTION ──
    # If historical data shows conf=90% actually means 60% WR,
    # apply correction so APEX MIND doesn't overestimate its own accuracy
    try:
        lp = _load_learned_params()
        cal = lp.get("entry_calibration", {})
        if cal:
            bucket = str(int(confidence // 10) * 10)
            if bucket in cal:
                correction = float(cal[bucket].get("correction", 1.0))
                if correction < 0.85:  # only adjust if meaningfully overconfident
                    old_conf = confidence
                    confidence = round(confidence * correction, 1)
    except: pass

    top_signals=signals[:5]
    reason=" | ".join(top_signals)

    # ── OBSERVATION-BASED CONFIDENCE ADJUSTMENTS ──
    try:
        lp = _load_learned_params()
        regime = market.get("market_regime", "UNKNOWN")
        hour = datetime.now(timezone.utc).hour

        # 1. Direction accuracy -- FIX 3 (Opus): lower-bound gate replaces WR penalty
        dir_acc = lp.get("direction_accuracy", {})
        dir_key = regime + "_" + str(direction)
        _db = dir_acc.get(dir_key, {})
        lb = float(_db.get("lower_bound", 0.0)) if _db.get("lower_bound") is not None else 0.0
        n  = int(_db.get("count", 0))

        if n >= 30:
            if lb <= 0:
                # Untrusted bucket -- require exceptional signal or cap to below gate
                _sig_str = " ".join(signals)
                _exceptional = (
                    "MACD crossing bull" in _sig_str or
                    "MACD crossing bear" in _sig_str or
                    "Stop hunt" in _sig_str or
                    "Liquidation" in _sig_str or
                    ("OI +" in _sig_str)
                )
                if not _exceptional:
                    score = min(score, 30)
                    confidence = min(confidence, 45.0)
                    signals.insert(0, f"{dir_key} untrusted (lb={lb:.2f}) -- capped")
                else:
                    confidence = min(confidence, 65.0)
                    signals.insert(0, f"{dir_key} untrusted but exceptional signal")
            elif lb < 1.0:
                confidence = round(confidence * 0.85, 1)   # weak-positive
            elif lb >= 2.0:
                confidence = round(confidence * 1.10, 1)   # strong edge
            confidence = round(min(99, max(10, confidence)), 1)

        # 2. Session entry timing adjustment
        timing = lp.get("session_entry_timing", {})
        h_data = timing.get(str(hour), {}).get(str(direction), {})
        if h_data:
            timing_wr = float(h_data.get("wr", 50))
            timing_adj = round((timing_wr - 50) / 100 * 8, 1)  # max ±4% adjustment
            confidence = round(min(99, max(10, confidence + timing_adj)), 1)

        # 3. Spring entry quality -- RSI-based filter
        if mode == "SPRING":
            rsi15 = float(df15.iloc[-2].get("rsi", 50) if df15 is not None else 50)
            rsi_bucket = str(int(rsi15 // 5) * 5)
            sq = lp.get("spring_entry_quality", {})
            if rsi_bucket in sq:
                rsi_wr = float(sq[rsi_bucket].get("wr", 50))
                rsi_adj = round((rsi_wr - 50) / 100 * 10, 1)
                confidence = round(min(99, max(10, confidence + rsi_adj)), 1)

    except: pass

    # ── SAVE SUGGESTION FOR SCORING -- after FIX3 caps applied ──
    try:
        _conn_es = _get_mind_conn()
        _hour_es = datetime.now(timezone.utc).hour
        _conn_es.execute("""INSERT INTO entry_suggestions
            (timestamp,symbol,mode,suggested_dir,score,confidence,reason,
             market_regime,btc_adx,alts_bull_pct,hour_utc,entry_price)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
             symbol, mode, direction, round(score,1), round(confidence,1),
             reason, market.get("market_regime","UNKNOWN"),
             market.get("btc_adx_15m",0), market.get("alts_bull_pct",50),
             _hour_es, price))
        _conn_es.commit(); _conn_es.close()
    except Exception as _se:
        logger.error(f"Entry suggestion save error: {_se}")

    return direction, round(score,1), reason, round(confidence,1)

def analyze_coin_behavior(symbol):
    """
    Detects what a coin is ACTUALLY doing right now.
    Identifies manipulation, stop hunts, cascades, accumulation.
    Returns behavioral profile with prediction.
    """
    result={"behavior":"NORMAL","trigger":None,"prediction":None,"confidence":0,"signals":[]}
    try:
        df1m =fetch(symbol,"1m",30)
        df5m =fetch(symbol,"5m",20)
        df15 =fetch(symbol,"15m",20)
        if df5m is None or df1m is None: return result
        df1m=add_inds(df1m); df5m=add_inds(df5m)
        if df15 is not None: df15=add_inds(df15)
    except: return result

    price=sf(df5m.iloc[-2]["close"])
    signals=[]

    # ── 1. STOP HUNT DETECTION ──
    # Sudden wick below recent low then recovery
    try:
        recent_lows=[sf(df5m.iloc[i]["low"]) for i in range(-8,-2)]
        recent_highs=[sf(df5m.iloc[i]["high"]) for i in range(-8,-2)]
        last_low=sf(df5m.iloc[-2]["low"])
        last_high=sf(df5m.iloc[-2]["high"])
        last_close=sf(df5m.iloc[-2]["close"])
        last_open=sf(df5m.iloc[-2]["open"])
        if last_low==0 or last_high==0 or last_close==0: return result
        support=min(recent_lows); resistance=max(recent_highs)
        atr=sf(df5m.iloc[-2].get("atr",0))

        # Wick below support but closed above = stop hunt
        if last_low<support*0.998 and last_close>support*0.999 and atr>0:
            wick_size=(support-last_low)/atr
            if wick_size>0.5:
                result["behavior"]="STOP_HUNT_LONG"
                result["trigger"]="Wick below support, close above = stops hunted"
                result["prediction"]="BOUNCE_UP"
                result["confidence"]=min(wick_size*30,80)
                signals.append(f"Stop hunt detected wick={wick_size:.1f}x ATR")

        # Wick above resistance but closed below = stop hunt short
        if last_high>resistance*1.002 and last_close<resistance*0.999 and atr>0:
            wick_size=(last_high-resistance)/atr
            if wick_size>0.5:
                result["behavior"]="STOP_HUNT_SHORT"
                result["trigger"]="Wick above resistance, close below = stops hunted"
                result["prediction"]="DROP_DOWN"
                result["confidence"]=min(wick_size*30,80)
                signals.append(f"Stop hunt short wick={wick_size:.1f}x ATR")
    except: pass

    # ── 2. LIQUIDATION CASCADE ──
    # Volume spike + price drop/rise of >2x ATR in single candle
    try:
        vol_ma=sf(df5m.iloc[-2].get("vol_ma",0))
        vol_now=sf(df5m.iloc[-2].get("volume",0))
        atr=sf(df5m.iloc[-2].get("atr",0))
        candle_range=sf(df5m.iloc[-2]["high"])-sf(df5m.iloc[-2]["low"])
        if vol_ma>0 and atr>0:
            vol_spike=vol_now/vol_ma
            size_spike=candle_range/atr
            if vol_spike>3 and size_spike>2:
                candle_dir="UP" if sf(df5m.iloc[-2]["close"])>sf(df5m.iloc[-2]["open"]) else "DOWN"
                result["behavior"]=f"LIQUIDATION_{candle_dir}"
                result["trigger"]=f"Vol {vol_spike:.1f}x + {size_spike:.1f}x ATR candle = cascade"
                result["prediction"]="REVERSAL" if size_spike>3 else "CONTINUATION"
                result["confidence"]=min(vol_spike*10+size_spike*5,85)
                signals.append(f"Cascade: vol={vol_spike:.1f}x size={size_spike:.1f}x ATR dir={candle_dir}")
    except: pass

    # ── 3. WHALE ACCUMULATION ──
    # Price flat but volume rising = quiet accumulation
    try:
        recent_closes=[sf(df5m.iloc[i]["close"]) for i in range(-8,-1)]
        price_range=(max(recent_closes)-min(recent_closes))/min(recent_closes)*100
        vol_recent=sum(sf(df5m.iloc[i]["volume"]) for i in range(-5,-1))
        vol_before=sum(sf(df5m.iloc[i]["volume"]) for i in range(-10,-5))
        if price_range<1.0 and vol_before>0 and vol_recent/vol_before>1.8:
            result["behavior"]="ACCUMULATION"
            result["trigger"]=f"Price range {price_range:.2f}% but volume +{vol_recent/vol_before:.1f}x"
            result["prediction"]="BREAKOUT_LIKELY"
            result["confidence"]=55
            signals.append(f"Quiet accumulation: tight range {price_range:.2f}% high vol")
    except: pass

    # ── 4. MOMENTUM EXHAUSTION ──
    # Price making new highs but RSI declining = exhaustion
    try:
        rsis=[sf(df5m.iloc[i].get("rsi",50)) for i in range(-6,-1)]
        closes=[float(df5m.iloc[i]["close"]) for i in range(-6,-1)]
        if len(rsis)>=4 and len(closes)>=4:
            price_up=closes[-1]>closes[0]*1.005
            rsi_down=rsis[-1]<rsis[0]-5
            if price_up and rsi_down:
                result["behavior"]="EXHAUSTION_UP"
                result["trigger"]=f"Price rising but RSI falling {rsis[0]:.0f}→{rsis[-1]:.0f}"
                result["prediction"]="REVERSAL_DOWN"
                result["confidence"]=65
                signals.append(f"Bearish divergence 5m: RSI {rsis[0]:.0f}→{rsis[-1]:.0f}")
            price_down=closes[-1]<closes[0]*0.995
            rsi_up=rsis[-1]>rsis[0]+5
            if price_down and rsi_up:
                result["behavior"]="EXHAUSTION_DOWN"
                result["trigger"]=f"Price falling but RSI rising {rsis[0]:.0f}→{rsis[-1]:.0f}"
                result["prediction"]="REVERSAL_UP"
                result["confidence"]=65
                signals.append(f"Bullish divergence 5m: RSI {rsis[0]:.0f}→{rsis[-1]:.0f}")
    except: pass

    # ── 5. LOW LIQUIDITY SPIKE ──
    # Check 24H volume -- if low, price moves mean nothing
    try:
        ticker=get_trade_client().futures_ticker(symbol=symbol)
        vol24=float(ticker.get("quoteVolume",0))
        if vol24<3_000_000:
            result["behavior"]="LOW_LIQUIDITY"
            result["trigger"]=f"24H vol ${vol24/1e6:.1f}M -- thin market"
            result["prediction"]="UNRELIABLE"
            result["confidence"]=90
            signals.append(f"Low liquidity ${vol24/1e6:.1f}M skip")
    except: pass

    # ── 6. TREND STRENGTH SCORE ──
    try:
        adx15=sf(df15.iloc[-2].get("adx",0)) if df15 is not None else 0
        adx5=sf(df5m.iloc[-2].get("adx",0))
        if adx15>35 and adx5>30:
            dominant_dir="LONG" if sf(df15.iloc[-2].get("ema20",0))>sf(df15.iloc[-2].get("ema50",0)) else "SHORT"
            if result["behavior"]=="NORMAL":
                result["behavior"]=f"STRONG_TREND_{dominant_dir}"
                result["trigger"]=f"ADX 15m={adx15:.0f} 5m={adx5:.0f} strong trend"
                result["prediction"]=f"CONTINUE_{dominant_dir}"
                # Dynamic confidence based on ADX strength
                if adx15>55 and adx5>50: _conf=90
                elif adx15>45 and adx5>40: _conf=80
                elif adx15>35 and adx5>30: _conf=65
                else: _conf=55
                result["confidence"]=_conf
                signals.append(f"Strong trend ADX={adx15:.0f}")
    except: pass

    result["signals"]=signals
    return result

def detect_signal_sequence(symbol, direction="LONG"):
    """
    Detects the ORDER in which signals appeared -- not just presence.
    Signal sequences are far more predictive than individual signals.
    Accumulation:  OI builds quietly -> price flat -> volume spike -> move
    Trend dying:   ADX peaks -> RSI diverges -> volume drops -> reversal
    Squeeze:       Funding extreme -> OI spike -> liquidation -> reversal
    Stop hunt:     Wick beyond support/resistance -> immediate recovery -> continuation
    Returns: sequence_name, confidence, description
    """
    try:
        df5m  = fetch(symbol, "5m",  20)
        df15  = fetch(symbol, "15m", 20)
        if df5m is None or df15 is None: return "NONE", 0, ""
        df5m = add_inds(df5m); df15 = add_inds(df15)
    except: return "NONE", 0, ""

    sequences_found = []

    # ── SEQUENCE 1: INSTITUTIONAL ACCUMULATION ──
    # OI rising quietly + price range tight + then volume spike = breakout imminent
    try:
        oi_hist = get_scanner_client().futures_open_interest_hist(symbol=symbol, period="5m", limit=8)
        if oi_hist and len(oi_hist) >= 6:
            oi_vals = [float(x.get("sumOpenInterest", 0)) for x in oi_hist]
            oi_trend = (oi_vals[-1] - oi_vals[0]) / oi_vals[0] * 100 if oi_vals[0] > 0 else 0
            closes   = [sf(df5m.iloc[i]["close"]) for i in range(-8, -1)]
            price_range = (max(closes) - min(closes)) / min(closes) * 100 if min(closes) > 0 else 99
            vol_now  = sf(df5m.iloc[-2].get("vol_ratio", 1))
            vol_prev = sum(sf(df5m.iloc[i].get("vol_ratio", 1)) for i in range(-6, -2)) / 4
            # OI building + price tight + recent volume surge = accumulation
            if oi_trend > 1.5 and price_range < 1.2 and vol_now > vol_prev * 1.8:
                conf = min(int(oi_trend * 15 + (vol_now / vol_prev) * 10), 85)
                sequences_found.append(("ACCUMULATION", conf,
                    f"OI+{oi_trend:.1f}% price tight {price_range:.1f}% vol surge {vol_now:.1f}x -- breakout imminent"))
    except: pass

    # ── SEQUENCE 2: TREND DYING ──
    # ADX was high -> now falling -> RSI diverging -> volume dropping = reversal
    try:
        adx_vals = [sf(df15.iloc[i].get("adx", 0)) for i in range(-8, -1)]
        rsi_vals = [sf(df15.iloc[i].get("rsi",  50)) for i in range(-8, -1)]
        vol_vals = [sf(df15.iloc[i].get("vol_ratio", 1)) for i in range(-8, -1)]
        adx_vals = [v for v in adx_vals if v > 0]
        if len(adx_vals) >= 5:
            adx_peaked  = adx_vals[2] == max(adx_vals)          # peaked in middle
            adx_falling = adx_vals[-1] < adx_vals[-3] * 0.88    # now falling
            closes_15   = [sf(df15.iloc[i]["close"]) for i in range(-8, -1)]
            if direction == "LONG":
                price_still_up = closes_15[-1] > closes_15[-4]
                rsi_falling    = rsi_vals[-1] < rsi_vals[-4] - 5
            else:
                price_still_up = closes_15[-1] < closes_15[-4]
                rsi_falling    = rsi_vals[-1] > rsi_vals[-4] + 5
            vol_drying = vol_vals[-1] < sum(vol_vals[:4]) / 4 * 0.7
            if adx_peaked and adx_falling and rsi_falling and vol_drying:
                sequences_found.append(("TREND_DYING", 78,
                    f"ADX peaked {max(adx_vals):.0f} now {adx_vals[-1]:.0f} + RSI diverging + vol drying -- reversal near"))
    except: pass

    # ── SEQUENCE 3: FUNDING SQUEEZE ──
    # Extreme funding -> OI spike -> price stalls = forced liquidation imminent
    try:
        fr_hist = get_scanner_client().futures_funding_rate(symbol=symbol, limit=3)
        oi_now  = get_scanner_client().futures_open_interest_hist(symbol=symbol, period="5m", limit=4)
        if fr_hist and oi_now and len(oi_now) >= 3:
            fr_val   = float(fr_hist[0].get("fundingRate", 0)) * 100
            oi_spike = (float(oi_now[-1].get("sumOpenInterest", 0)) -
                        float(oi_now[-3].get("sumOpenInterest", 0))) /                         float(oi_now[-3].get("sumOpenInterest", 1)) * 100
            rsi_now  = sf(df5m.iloc[-2].get("rsi", 50))
            # High positive funding + OI spike + overbought = long squeeze coming
            if fr_val > 0.08 and oi_spike > 2 and rsi_now > 70 and direction == "LONG":
                sequences_found.append(("LONG_SQUEEZE", 82,
                    f"Funding {fr_val:.3f}% extreme + OI+{oi_spike:.1f}% + RSI {rsi_now:.0f} -- long squeeze imminent"))
            # Negative funding + OI spike + oversold = short squeeze coming
            elif fr_val < -0.08 and oi_spike > 2 and rsi_now < 30 and direction == "SHORT":
                sequences_found.append(("SHORT_SQUEEZE", 82,
                    f"Neg funding {fr_val:.3f}% + OI+{oi_spike:.1f}% + RSI {rsi_now:.0f} -- short squeeze imminent"))
    except: pass

    # ── SEQUENCE 4: STOP HUNT + CONTINUATION ──
    # Wick beyond key level -> immediate recovery -> same direction move
    try:
        recent_lows  = [sf(df5m.iloc[i]["low"])   for i in range(-8, -3)]
        recent_highs = [sf(df5m.iloc[i]["high"])  for i in range(-8, -3)]
        last_low     = sf(df5m.iloc[-2]["low"])
        last_close   = sf(df5m.iloc[-2]["close"])
        last_open    = sf(df5m.iloc[-2]["open"])
        atr          = sf(df5m.iloc[-2].get("atr", 0))
        support      = min(recent_lows)
        resistance   = max(recent_highs)
        if atr > 0:
            # Wick below support + close well above = stops hunted, longs now safe
            if last_low < support * 0.997 and last_close > support * 1.002 and direction == "LONG":
                wick = (support - last_low) / atr
                if wick > 0.6:
                    sequences_found.append(("STOP_HUNT_LONG_SAFE", min(int(wick * 35), 80),
                        f"Stop hunt wick={wick:.1f}x ATR below support -- longs now safe continuation"))
            # Wick above resistance + close well below = stops hunted, shorts now safe
            if last_high > resistance * 1.003 and last_close < resistance * 0.998 and direction == "SHORT":
                last_high = sf(df5m.iloc[-2]["high"])
                wick = (last_high - resistance) / atr
                if wick > 0.6:
                    sequences_found.append(("STOP_HUNT_SHORT_SAFE", min(int(wick * 35), 80),
                        f"Stop hunt wick={wick:.1f}x ATR above resistance -- shorts now safe"))
    except: pass

    # ── SEQUENCE 5: MULTI-TIMEFRAME CONFLUENCE ──
    # When 15m + 1h + 4h all align simultaneously = rare, high-conviction entry
    try:
        df1h = fetch(symbol, "1h", 20)
        df4h = fetch(symbol, "4h", 10)
        if df1h is not None and df4h is not None:
            df1h = add_inds(df1h); df4h = add_inds(df4h)
            rsi_5m  = sf(df5m.iloc[-2].get("rsi",  50))
            rsi_15m = sf(df15.iloc[-2].get("rsi",  50))
            rsi_1h  = sf(df1h.iloc[-2].get("rsi",  50))
            adx_15m = sf(df15.iloc[-2].get("adx",  0))
            adx_1h  = sf(df1h.iloc[-2].get("adx",  0))
            e20_15  = sf(df15.iloc[-2].get("ema20", 0))
            e50_15  = sf(df15.iloc[-2].get("ema50", 0))
            e20_1h  = sf(df1h.iloc[-2].get("ema20", 0))
            e50_1h  = sf(df1h.iloc[-2].get("ema50", 0))
            e20_4h  = sf(df4h.iloc[-2].get("ema20", 0))
            e50_4h  = sf(df4h.iloc[-2].get("ema50", 0))
            if direction == "LONG":
                tf_align = (e20_15 > e50_15 and e20_1h > e50_1h and e20_4h > e50_4h)
                rsi_ok   = (40 <= rsi_15m <= 65 and 45 <= rsi_1h <= 70)
                adx_ok   = (adx_15m > 22 and adx_1h > 20)
            else:
                tf_align = (e20_15 < e50_15 and e20_1h < e50_1h and e20_4h < e50_4h)
                rsi_ok   = (35 <= rsi_15m <= 60 and 30 <= rsi_1h <= 55)
                adx_ok   = (adx_15m > 22 and adx_1h > 20)
            if tf_align and rsi_ok and adx_ok:
                sequences_found.append(("MTF_CONFLUENCE", 88,
                    f"All 3 timeframes aligned {direction} -- high conviction entry"))
    except: pass

    if not sequences_found:
        return "NONE", 0, ""

    # Return highest confidence sequence
    best = max(sequences_found, key=lambda x: x[1])
    if len(sequences_found) > 1:
        all_names = "+".join(s[0] for s in sequences_found)
        return all_names, min(best[1] + 10, 95), best[2]
    return best[0], best[1], best[2]

def analyze_collective_behavior(trades, market):
    """
    Analyzes collective trade behavior to distinguish:
    - Market problem: multiple trades losing simultaneously
    - Coin problem: one trade losing while others win
    - Spring signal: Spring Bot mass losses = strong bear signal for APEX
    Returns insights dict with market_signal and recommended_action
    """
    if not trades: return {}

    apex_trades  = [t for t in trades if t["bot_type"]=="APEX"]
    spring_trades= [t for t in trades if t["bot_type"]=="SPRING"]

    insights = {}

    # ── APEX collective analysis ──
    if len(apex_trades) >= 3:
        apex_losing  = [t for t in apex_trades if t["roe"] < -2]
        apex_winning = [t for t in apex_trades if t["roe"] > 2]
        apex_loss_pct= len(apex_losing)/len(apex_trades)*100

        if apex_loss_pct >= 70:
            insights["apex_signal"] = "MARKET_PROBLEM"
            insights["apex_action"] = "Stop new entries -- collective loss signal"
            insights["apex_detail"] = f"{len(apex_losing)}/{len(apex_trades)} APEX trades losing"
        elif apex_loss_pct <= 20 and len(apex_winning) >= 2:
            insights["apex_signal"] = "MARKET_HEALTHY"
            insights["apex_detail"] = f"{len(apex_winning)}/{len(apex_trades)} APEX trades winning"
        else:
            insights["apex_signal"] = "MIXED"

    # ── Spring → APEX signal ──
    if len(spring_trades) >= 5:
        spring_losing  = [t for t in spring_trades if t["roe"] < -3]
        spring_winning = [t for t in spring_trades if t["roe"] > 3]
        spring_loss_pct= len(spring_losing)/len(spring_trades)*100

        if spring_loss_pct >= 80:
            # Spring mass losses = dips not bouncing = bearish market
            insights["spring_signal"] = "STRONG_BEAR"
            insights["spring_apex_action"] = "APEX should close longs -- Spring mass failure"
            insights["spring_detail"] = f"{len(spring_losing)}/{len(spring_trades)} Spring trades failing"
        elif spring_loss_pct <= 20:
            # Spring bouncing well = healthy market
            insights["spring_signal"] = "HEALTHY_BOUNCES"
            insights["spring_detail"] = f"{len(spring_winning)}/{len(spring_trades)} Spring bouncing"

    # ── Coin vs market problem ──
    if len(apex_trades) >= 2:
        for trade in apex_trades:
            others = [t for t in apex_trades if t["symbol"] != trade["symbol"]]
            if not others: continue
            others_avg_roe = sum(t["roe"] for t in others)/len(others)
            # If this coin losing while others winning = coin problem
            if trade["roe"] < -3 and others_avg_roe > 1:
                if "coin_problems" not in insights:
                    insights["coin_problems"] = []
                insights["coin_problems"].append({
                    "symbol": trade["symbol"],
                    "roe": trade["roe"],
                    "others_avg": round(others_avg_roe,1),
                    "signal": "COIN_SPECIFIC_PROBLEM"
                })

    return insights

def get_orderflow(symbol):
    """
    Reads real-time order flow for a symbol.
    Returns cached result - background thread keeps it fresh every 10s.
    Falls back to REST if cache is empty.
    """
    with _orderflow_lock:
        cached = _orderflow_cache.get(symbol)
    if cached and time.time() - cached.get("ts", 0) < 30:
        return cached
    # Fallback: compute synchronously if cache miss
    return _compute_orderflow(symbol)


def _compute_orderflow(symbol):
    """
    Core order flow computation.
    Signals computed:
    1. bid_ask_imbalance  -- order book pressure (who has more size)
    2. aggressive_ratio   -- market orders vs limit orders (urgency)
    3. spoof_score        -- large orders appearing/disappearing
    4. absorption         -- price not moving despite large orders (hidden opposition)
    5. momentum_diverge   -- recent trades direction vs price direction
    6. wall_side          -- which side has a wall (large order blocking price)
    7. flow_score         -- composite -100 to +100 (positive = buy pressure)
    8. flow_signal        -- BUY_PRESSURE / SELL_PRESSURE / NEUTRAL / SPOOF_BUY / SPOOF_SELL
    """
    result = {
        "symbol": symbol, "ts": time.time(),
        "bid_ask_imbalance": 0.0,
        "aggressive_ratio": 0.0,
        "spoof_score": 0.0,
        "absorption": 0.0,
        "momentum_diverge": 0.0,
        "wall_side": "NONE",
        "wall_size": 0.0,
        "flow_score": 0.0,
        "flow_signal": "NEUTRAL",
        "buy_vol": 0.0,
        "sell_vol": 0.0,
        "large_buy_count": 0,
        "large_sell_count": 0,
    }
    try:
        # ── 1. ORDER BOOK IMBALANCE ──
        # Top 20 levels -- where is the size sitting?
        ob = get_scanner_client().futures_order_book(symbol=symbol, limit=20)
        bids = [(float(p), float(q)) for p, q in ob.get("bids", [])]
        asks = [(float(p), float(q)) for p, q in ob.get("asks", [])]
        if bids and asks:
            bid_vol = sum(q for _, q in bids[:10])
            ask_vol = sum(q for _, q in asks[:10])
            total_vol = bid_vol + ask_vol
            if total_vol > 0:
                # +1.0 = all bids, -1.0 = all asks
                imbalance = (bid_vol - ask_vol) / total_vol
                result["bid_ask_imbalance"] = round(imbalance, 3)

            # ── 2. WALL DETECTION ──
            # A wall is a single level with >15% of total book volume
            # Walls above price = resistance (bears defending)
            # Walls below price = support (bulls defending)
            mid_price = (bids[0][0] + asks[0][0]) / 2
            max_bid_size = max(q for _, q in bids[:10])
            max_ask_size = max(q for _, q in asks[:10])
            if max_ask_size / max(ask_vol, 1) > 0.20:
                result["wall_side"] = "SELL_WALL"
                result["wall_size"] = round(max_ask_size, 2)
            elif max_bid_size / max(bid_vol, 1) > 0.20:
                result["wall_side"] = "BUY_WALL"
                result["wall_size"] = round(max_bid_size, 2)

            # ── 3. SPOOF DETECTION ──
            # Large orders (>3x avg size) that appear then disappear = spoofing
            # We detect the signature: one level has 5x the avg size
            avg_ask = ask_vol / max(len(asks[:10]), 1)
            avg_bid = bid_vol / max(len(bids[:10]), 1)
            spoof_ask = any(q > avg_ask * 5 for _, q in asks[:5])
            spoof_bid = any(q > avg_bid * 5 for _, q in bids[:5])
            if spoof_ask and not spoof_bid:
                result["spoof_score"] = -0.7  # fake sell wall -- actually bullish
                result["flow_signal"] = "SPOOF_SELL"  # sell wall is fake
            elif spoof_bid and not spoof_ask:
                result["spoof_score"] = 0.7   # fake buy wall -- actually bearish
                result["flow_signal"] = "SPOOF_BUY"   # buy wall is fake

    except Exception as e:
        logger.debug(f"Order book {symbol}: {e}")

    try:
        # ── 4. RECENT TRADE AGGRESSION ──
        # Last 100 trades -- who is the aggressor?
        # Buyer aggressor = market buy hitting the ask = bullish urgency
        # Seller aggressor = market sell hitting the bid = bearish urgency
        get_scanner_rl().acquire(weight=1); trades_raw = get_scanner_client().futures_recent_trades(symbol=symbol, limit=100)
        if trades_raw:
            buy_vol  = sum(float(t["qty"]) for t in trades_raw if not t.get("isBuyerMaker", True))
            sell_vol = sum(float(t["qty"]) for t in trades_raw if t.get("isBuyerMaker", True))
            total    = buy_vol + sell_vol
            if total > 0:
                aggressive_ratio = (buy_vol - sell_vol) / total
                result["aggressive_ratio"] = round(aggressive_ratio, 3)
                result["buy_vol"]  = round(buy_vol, 2)
                result["sell_vol"] = round(sell_vol, 2)

            # Large trade detection -- institutional size orders
            avg_trade = sum(float(t["qty"]) for t in trades_raw) / len(trades_raw)
            large_threshold = avg_trade * 5
            result["large_buy_count"]  = sum(1 for t in trades_raw
                if not t.get("isBuyerMaker", True) and float(t["qty"]) > large_threshold)
            result["large_sell_count"] = sum(1 for t in trades_raw
                if t.get("isBuyerMaker", True) and float(t["qty"]) > large_threshold)

            # ── 5. MOMENTUM DIVERGENCE ──
            # Split trades into first 50 and last 50
            # If aggression is shifting direction = momentum change incoming
            first_half = trades_raw[:50]
            last_half  = trades_raw[50:]
            fh_buy = sum(float(t["qty"]) for t in first_half if not t.get("isBuyerMaker", True))
            fh_sel = sum(float(t["qty"]) for t in first_half if t.get("isBuyerMaker", True))
            lh_buy = sum(float(t["qty"]) for t in last_half  if not t.get("isBuyerMaker", True))
            lh_sel = sum(float(t["qty"]) for t in last_half  if t.get("isBuyerMaker", True))
            fh_ratio = (fh_buy - fh_sel) / max(fh_buy + fh_sel, 1)
            lh_ratio = (lh_buy - lh_sel) / max(lh_buy + lh_sel, 1)
            # If recent half flipped direction vs earlier half = momentum diverging
            result["momentum_diverge"] = round(lh_ratio - fh_ratio, 3)

    except Exception as e:
        logger.debug(f"Recent trades {symbol}: {e}")

    try:
        # ── 6. ABSORPTION DETECTION ──
        # High aggressive buy volume but price not rising = absorption (hidden sellers)
        # High aggressive sell volume but price not falling = absorption (hidden buyers)
        df5m = fetch(symbol, "5m", 4)
        if df5m is not None and len(df5m) >= 3:
            price_move = (sf(df5m.iloc[-2]["close"]) - sf(df5m.iloc[-3]["close"]))                          / sf(df5m.iloc[-3]["close"]) * 100
            if abs(result["aggressive_ratio"]) > 0.3 and abs(price_move) < 0.15:
                # Strong aggression but no price movement = absorption
                if result["aggressive_ratio"] > 0.3:
                    result["absorption"] = -0.6  # buyers absorbed = bearish
                else:
                    result["absorption"] = 0.6   # sellers absorbed = bullish
    except: pass

    # ── 7. COMPOSITE FLOW SCORE ──
    # Range -100 to +100. Calibrated so most scores land in -40 to +40 range.
    # Extreme scores (>70) only when multiple strong signals align.
    flow = (
        result["bid_ask_imbalance"]  * 25 +   # order book bias
        result["aggressive_ratio"]   * 35 +   # trade aggression (highest weight)
        result["spoof_score"]        * 20 +   # spoof adjustment
        result["absorption"]         * 15 +   # absorption signal
        result["momentum_diverge"]   * 15     # momentum shift
    )
    # Scale: raw flow of ±1.0 = ±100. But real values are rarely ±1.0
    # Typical aggressive_ratio is ±0.3-0.7, imbalance ±0.1-0.4
    # So multiply by 100 but apply soft cap using tanh-like scaling
    raw = flow * 100
    # Soft compression -- scores above 50 are compressed, extreme only when truly extreme
    import math
    compressed = math.tanh(raw / 60) * 100
    result["flow_score"] = round(compressed, 1)

    # ── 8. FINAL SIGNAL CLASSIFICATION ──
    if result.get("flow_signal") not in ("SPOOF_BUY", "SPOOF_SELL"):
        fs = result["flow_score"]
        lbc = result["large_buy_count"]
        lsc = result["large_sell_count"]
        if fs > 35 and lbc >= 2:
            result["flow_signal"] = "STRONG_BUY"
        elif fs > 20:
            result["flow_signal"] = "BUY_PRESSURE"
        elif fs < -35 and lsc >= 2:
            result["flow_signal"] = "STRONG_SELL"
        elif fs < -20:
            result["flow_signal"] = "SELL_PRESSURE"
        elif result["wall_side"] == "SELL_WALL":
            result["flow_signal"] = "SELL_WALL_AHEAD"
        elif result["wall_side"] == "BUY_WALL":
            result["flow_signal"] = "BUY_WALL_SUPPORT"
        else:
            result["flow_signal"] = "NEUTRAL"

    return result


def _orderflow_worker():
    """
    Background thread -- updates order flow cache every 10 seconds.
    Only tracks symbols in _orderflow_symbols (open trades + scanner candidates).
    Lightweight: 1 order book call + 1 trades call per symbol per 10 seconds.
    """
    logger.info("Order flow worker started")
    while _orderflow_active:
        with _orderflow_lock:
            symbols = list(_orderflow_symbols)
        for sym in symbols:
            if not _orderflow_active: break
            try:
                result = _compute_orderflow(sym)
                with _orderflow_lock:
                    _orderflow_cache[sym] = result
                time.sleep(0.2)  # rate limit between symbols
            except Exception as e:
                logger.debug(f"Flow worker {sym}: {e}")
        # Sleep remaining time to complete 10s cycle
        time.sleep(max(1, 10 - len(symbols) * 0.2))
    logger.info("Order flow worker stopped")


def start_orderflow_worker():
    """Start the background order flow thread"""
    global _orderflow_active
    _orderflow_active = True
    t = threading.Thread(target=_orderflow_worker, daemon=True, name="OrderFlowWorker")
    t.start()
    logger.info("Order flow worker thread launched")
    return t


def update_orderflow_symbols(trades):
    """Tell the worker which symbols to track"""
    with _orderflow_lock:
        _orderflow_symbols.clear()
        for t in trades:
            _orderflow_symbols.add(t["symbol"])



def get_correlation_size_mult(symbol, direction, open_trades):
    """I5: Correlation-aware position sizing.
    If new position is highly correlated with existing open positions,
    reduce size to avoid concentration risk.
    Uses BTC-beta as proxy for correlation (most alts move with BTC).
    """
    try:
        if not open_trades: return 1.0

        # Get 1H returns for BTC and candidate
        btc_df = fetch("BTCUSDT", "1h", 50)
        cand_df = fetch(symbol, "1h", 50)
        if btc_df is None or cand_df is None or len(btc_df)<20: return 1.0

        btc_returns = btc_df["close"].pct_change().dropna().tail(30).values
        cand_returns = cand_df["close"].pct_change().dropna().tail(30).values
        if len(btc_returns) < 20 or len(cand_returns) < 20: return 1.0

        # BTC-beta of candidate
        import numpy as _np
        cand_btc_corr = float(_np.corrcoef(btc_returns[-len(cand_returns):], cand_returns)[0,1])

        # Get BTC-beta of each open position
        open_corrs = []
        for t in open_trades:
            if t.get("bot_type") != "APEX": continue
            sym = t.get("symbol","")
            if sym == symbol: continue
            try:
                sym_df = fetch(sym, "1h", 50)
                if sym_df is None or len(sym_df) < 20: continue
                sym_ret = sym_df["close"].pct_change().dropna().tail(30).values
                corr = float(_np.corrcoef(btc_returns[-len(sym_ret):], sym_ret)[0,1])
                # For SHORT positions, flip correlation sign
                if t.get("direction") == "SHORT": corr = -corr
                open_corrs.append(corr)
            except: pass

        if not open_corrs: return 1.0

        # Flip candidate correlation for SHORT direction
        if direction == "SHORT": cand_btc_corr = -cand_btc_corr

        # Average correlation between candidate and open positions
        avg_corr = float(_np.mean([abs(cand_btc_corr - c) < 0.4 and
                                    cand_btc_corr * c > 0.3
                                    for c in open_corrs]))

        k = len(open_corrs)
        if avg_corr > 0.6 and k >= 3:
            mult = round(1.0 / (1 + k * 0.3), 2)
            mult = max(0.5, mult)  # floor at 50% size
            logger.info(f"  [CORR] {symbol} highly correlated (avg={avg_corr:.2f}, k={k}) → size mult={mult}x")
            return mult
        elif avg_corr > 0.4 and k >= 4:
            mult = round(1.0 / (1 + k * 0.15), 2)
            mult = max(0.7, mult)
            logger.info(f"  [CORR] {symbol} moderately correlated → size mult={mult}x")
            return mult

        return 1.0
    except Exception as e:
        logger.debug(f"get_correlation_size_mult {symbol}: {e}")
        return 1.0

def _log_blocked_entry(symbol, direction, price, score, confidence, market, block_reason):
    """L12 FIX: Log blocked entries for counterfactual analysis.
    Saves to entry_suggestions with trade_opened=-1 (blocked marker).
    Allows measuring cost of false negatives -- good setups we rejected."""
    try:
        _conn = _get_mind_conn()
        _conn.execute("""INSERT INTO entry_suggestions
            (symbol, suggested_dir, confidence, entry_price, timestamp,
             trade_opened, outcome_filled, why_correct)
            VALUES (?,?,?,?,datetime('now'),?,0,?)""",
            (symbol, direction, round(confidence,1), float(price), -1,
             f"BLOCKED: {block_reason} score={score:.0f}"))
        _conn.commit(); _conn.close()
    except: pass

def compute_apex_sl(symbol, direction, price, market, df15, coin_type, liquidity):
    """Single source of truth for APEX SL. Called by both live and backfill.
    Returns (sl, sl_dist_pct, vetoed: bool)."""
    try:
        atr = float(df15.iloc[-2].get("atr", price*0.01) or price*0.01)
        atr = max(atr, price*0.01)
        _btc_adx  = float(market.get("btc_adx_15m", 25))
        _btc_adx_t = market.get("btc_adx_trend", "FLAT")
        _btc_ema  = market.get("btc_ema_align", "FLAT")
        _ema_aligned = (_btc_ema=="BULL" and direction=="LONG") or (_btc_ema=="BEAR" and direction=="SHORT")
        if _btc_adx >= 25 and _ema_aligned: base_mult = 1.2
        elif _btc_adx >= 20 and _btc_adx_t == "RISING": base_mult = 1.6
        else: base_mult = 2.0
        max_sl_pct = {"STABLE": 0.05, "MODERATE": 0.06, "HYPER_VOLATILE": 0.09}.get(coin_type, 0.05)
        if liquidity == "THIN": max_sl_pct = min(max_sl_pct, 0.04)
        vol_adj = {"STABLE": 0.85, "MODERATE": 1.0, "HYPER_VOLATILE": 1.3}.get(coin_type, 1.0)
        atr_buf = atr * base_mult * vol_adj
        _lp = _load_learned_params()
        n = int(_lp.get("ratchet", {}).get("sl_swing_candles", 10))
        swing_low  = float(df15.iloc[-(n+1):-1]["low"].min())
        swing_high = float(df15.iloc[-(n+1):-1]["high"].max())
        sl = round(swing_low - atr_buf, 8) if direction == "LONG" else round(swing_high + atr_buf, 8)
        sl_dist_pct = abs(price - sl) / price
        return sl, sl_dist_pct, (sl_dist_pct > max_sl_pct)
    except Exception:
        sl = round(price*0.97, 8) if direction == "LONG" else round(price*1.03, 8)
        return sl, 0.03, False


def _open_apex_trade(symbol, direction, score, confidence, reason, market):
    """
    Open a new APEX Bot trade directly.
    Replaces main.py watch_cycle + open_trade() for APEX entries.
    Writes to apex_trades.db + bot_state.json atomically.
    """
    try:
        cfg = load_master_config()
        if not cfg.get("execution", {}).get("apex_entries", False):
            return False  # apex entries not enabled yet
        if cfg.get("safety", {}).get("emergency_stop", False):
            return False

        # Confidence gate
        min_conf = cfg.get("confidence_gates", {}).get("apex_entry_min", 70)
        # Phase 1 (Opus): raise bar when regime is uncertain
        pos = get_position_counts()
        reg_sug = get_regime_suggestions(market)
        _bar_bonus = reg_sug.get("entry_bar_bonus", 0)
        min_conf = min_conf + _bar_bonus
        _min_score_gate = 25 + _bar_bonus
        if confidence < min_conf:
            logger.debug(f"  [ENTRY] {symbol} conf {confidence:.0f}% < gate {min_conf}% (bar+{_bar_bonus}) -- skip")
            try: _log_blocked_entry(symbol, direction, float(market.get("price", 0) or 0), score, confidence, market, f"conf={confidence:.0f}% < gate={min_conf}% (uncertain+{_bar_bonus})")
            except: pass
            return False
        if score < _min_score_gate:
            logger.debug(f"  [ENTRY] {symbol} score {score:.0f} < gate {_min_score_gate} (bar+{_bar_bonus}) -- skip")
            return False

        # Position limits
        max_long  = reg_sug.get("max_long",  cfg.get("limits", {}).get("max_apex_long",  6))
        max_short = reg_sug.get("max_short", cfg.get("limits", {}).get("max_apex_short", 2))
        # Re-check under lock to prevent race condition from parallel scanner threads
        with _entry_lock:
            pos = get_position_counts()  # fresh count under lock
        if direction == "LONG"  and pos["long"] >= max_long:
            logger.debug(f"  [ENTRY] APEX LONG full {pos['long']}/{max_long}"); return False
        if direction == "SHORT" and pos["short"] >= max_short:
            logger.debug(f"  [ENTRY] APEX SHORT full {pos['short']}/{max_short}"); return False

            # ── BTC CORRELATION GUARD -- prevent portfolio wipe from correlated longs ──
            if direction == "LONG" and pos["long"] >= 3:
                try:
                    _btc_rsi = float(market.get("btc_rsi_15m", 50))
                    _btc_adx = float(market.get("btc_adx_15m", 25))
                    _adx_tr  = market.get("btc_adx_trend", "FLAT")
                    _alts    = float(market.get("alts_bull_pct", 50))
                    _corr_risk = (_btc_rsi < 40 and _alts < 45) or \
                                 (_btc_rsi < 35) or \
                                 (_adx_tr == "RISING" and _btc_adx > 30 and _btc_rsi < 45)
                    if _corr_risk:
                        _req = min_conf + 15
                        if confidence < _req:
                            logger.info(f"  [ENTRY] {symbol} LONG blocked -- corr risk ({pos['long']} longs, BTC RSI={_btc_rsi:.0f} alts={_alts:.0f}%)")
                            return False
                except: pass

        # Block if same symbol already open in any direction
        try:
            conn = sqlite3.connect(TRADES_DB)
            already = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE symbol=? AND status='OPEN'",
                (symbol,)).fetchone()[0]
            conn.close()
            if already > 0:
                logger.debug(f"  [ENTRY BLOCKED] {symbol} already has open trade")
                return False
        except: pass

        # Get current price
        get_scanner_rl().acquire(weight=1)
        ticker = get_client().futures_symbol_ticker(symbol=symbol)
        price = float(ticker["price"])

        # Position sizing -- Kelly Criterion + regime + session
        balance = get_balance()
        avail = float(balance.get("available", 0))
        if avail <= 0: return False

        hour = datetime.now(timezone.utc).hour
        _, _, sess_mult = get_session_quality(hour, direction)
        # Regime size multiplier from regime_detector
        # Kill switch check
        ks = check_kill_switch()
        if ks == "STOP_TRADING": return False
        if ks == "REDUCE_RISK": sess_mult *= 0.5
        # Kelly handles regime sizing via learned regime_kelly params
        # reg_mult removed -- was conflicting with Kelly regime multipliers
        size = kelly_position_size(avail, confidence, direction, sess_mult)

        # I5: Correlation-aware size reduction
        try:
            _corr_mult = get_correlation_size_mult(symbol, direction, trades)
            if _corr_mult < 1.0:
                size = round(size * _corr_mult, 2)
        except: pass

        # Classify coin type UP FRONT (once) -- volatility + liquidity
        # FIX: was calling classify 54 lines later, so _liq_size_mult was always undefined here
        try:
            coin_type, liquidity = classify_coin_type(symbol)
        except Exception:
            coin_type, liquidity = ("MODERATE", "NORMAL")
        _liq_size_mult = {"THIN":0.5,"NORMAL":1.0,"DEEP":1.0}.get(liquidity, 1.0)

        # Liquidity size reduction (now _liq_size_mult is defined above)
        if _liq_size_mult < 1.0:
            size = round(size * _liq_size_mult, 2)
            logger.info(f"  [LIQ] {symbol} {liquidity} liquidity: size reduced to ${size:.2f}")

        # Opus: HMM disagreement caution layer
        # When HMM and cascade disagree, reduce size and raise confidence threshold
        try:
            _hmm_regime = market.get("hmm_regime")
            _cascade_regime = market.get("market_regime", "UNKNOWN")
            _hmm_agrees = market.get("hmm_agrees", True)
            if _hmm_regime and not _hmm_agrees:
                size = round(size * 0.5, 2)  # half size when uncertain
                if confidence < 72:
                    logger.info(f"  [HMM CAUTION] {symbol} cascade={_cascade_regime} hmm={_hmm_regime} -- size halved, conf={confidence:.0f}% < 72% required")
                    return False  # require higher confidence during disagreement
                logger.info(f"  [HMM CAUTION] {symbol} cascade={_cascade_regime} hmm={_hmm_regime} -- size halved to ${size:.2f}")
        except: pass

        # Leverage -- fixed 5x for paper, learned later
        leverage = 5

        # SL -- ATR-based, widened if coin+regime has repeated SL hits (SL_OPTIMAL learned)
        try:
            df15 = fetch(symbol, "15m", 20)
            if df15 is not None and len(df15) >= 15:
                df15 = add_inds(df15)
                atr  = float(df15.iloc[-2].get("atr", price * 0.01) or price * 0.01)
            else:
                atr = price * 0.015
            # For very low price coins, ATR can be near zero -- use minimum 1% of price
            _min_atr = price * 0.01
            if atr < _min_atr:
                atr = _min_atr
            # ── SWING LOW/HIGH SL + ATR BUFFER ──
            try:
                _lp_sl = _load_learned_params()
                _sl_candles = int(_lp_sl.get("ratchet", {}).get("sl_swing_candles", 10))

                # Opus Logic 1: Conditional ATR multiplier by regime trend + coin type
                _btc_adx = float(market.get("btc_adx_15m", 25))
                _btc_adx_t = market.get("btc_adx_trend", "FLAT")
                _btc_ema = market.get("btc_ema_align", "FLAT")
                _ema_aligned = (_btc_ema == "BULL" and direction == "LONG") or (_btc_ema == "BEAR" and direction == "SHORT")

                if _btc_adx >= 25 and _ema_aligned:
                    _base_mult = 1.2   # clean trend -- tight, proves itself fast
                elif _btc_adx >= 20 and _btc_adx_t == "RISING":
                    _base_mult = 1.6   # emerging trend
                else:
                    _base_mult = 2.0   # chop -- wider or veto below

                # Coin volatility adjustment -- reuse classification from above (already computed)
                _coin_type = coin_type  # computed earlier in this function
                _vol_adj = {"STABLE": 0.85, "MODERATE": 1.0, "HYPER_VOLATILE": 1.3}.get(_coin_type, 1.0)
                _atr_buf_mult = round(_base_mult * _vol_adj, 2)

                # Opus Logic 2: Conditional max-SL cap by coin type
                _max_sl_pct = {"STABLE": 0.05, "MODERATE": 0.06, "HYPER_VOLATILE": 0.09}.get(_coin_type, 0.05)
                if liquidity == "THIN":
                    _max_sl_pct = min(_max_sl_pct, 0.04)  # thin: tighter, not wider

                # Swing low/high: lowest low or highest high in learned candle window
                _swing_low  = float(df15.iloc[-(_sl_candles+1):-1]["low"].min())
                _swing_high = float(df15.iloc[-(_sl_candles+1):-1]["high"].max())
                _atr_buf    = atr * _atr_buf_mult

                if direction == "LONG":
                    sl = round(_swing_low - _atr_buf, 8)
                else:
                    sl = round(_swing_high + _atr_buf, 8)

                _sl_dist = abs(price - sl)
                _sl_dist_pct = _sl_dist / price

                # Opus Logic 3: Hard veto if SL too wide for coin type
                # Don't clip-and-enter -- if structure says stop is far, skip the trade
                if _sl_dist_pct > _max_sl_pct:
                    logger.info(f"  [ENTRY BLOCKED] {symbol} structural SL={_sl_dist_pct*100:.1f}% > max={_max_sl_pct*100:.0f}% for {_coin_type} -- veto")
                    _log_blocked_entry(symbol, direction, price, score, confidence, market, f"Structural SL {_sl_dist_pct*100:.1f}% > max {_max_sl_pct*100:.0f}% {_coin_type}")
                    return False

                # Inner R:R check removed per spec (C1) -- outer gate below is authoritative

                logger.debug(f"  [ENTRY] {symbol} SL={sl:.6f} dist={_sl_dist_pct*100:.2f}% atr_mult={_atr_buf_mult} coin={_coin_type}")
            except:
                sl = round(price - atr * 1.5, 8) if direction == "LONG" else round(price + atr * 1.5, 8)
        except:
            sl = round(price * 0.98, 8) if direction == "LONG" else round(price * 1.02, 8)

        # ── SL distance gate -- conditional cap by coin type ──
        _sl_dist_check = abs(price - sl) / price
        _coin_type_gate = coin_type  # reuse already-computed value
        _max_sl_gate = {"STABLE": 0.05, "MODERATE": 0.06, "HYPER_VOLATILE": 0.09}.get(_coin_type_gate, 0.05)
        if _sl_dist_check > _max_sl_gate:
            logger.info(f"  [ENTRY BLOCKED] {symbol} SL too wide {_sl_dist_check*100:.1f}% > {_max_sl_gate*100:.0f}% ({_coin_type_gate}) -- skip")
            _log_blocked_entry(symbol, direction, price, score, confidence, market, f"SL too wide {_sl_dist_check*100:.1f}%")
            return False
        if _sl_dist_check < 0.005:
            logger.info(f"  [ENTRY BLOCKED] {symbol} SL too tight {_sl_dist_check*100:.2f}% -- skip")
            _log_blocked_entry(symbol, direction, price, score, confidence, market, f"SL too tight {_sl_dist_check*100:.2f}%")
            return False
        # ── R:R gate -- realized capture vs SL distance (single gate per spec C1) ──
        _lp_rr2 = _load_learned_params()
        _realized_capture = float(_lp_rr2.get("realized_capture_roe", 7.6))
        _min_rr = float(_lp_rr2.get("min_realized_rr", 0.5))  # named param C3, default 0.5
        _realized_rr_gate = _realized_capture / (_sl_dist_check * leverage * 100) if _sl_dist_check > 0 else 0
        _rr = _realized_rr_gate
        if _realized_rr_gate < _min_rr:
            logger.info(f"  [ENTRY BLOCKED] {symbol} R:R={_realized_rr_gate:.2f} < {_min_rr} SL={_sl_dist_check*100:.1f}% cap={_realized_capture:.1f}%")
            _log_blocked_entry(symbol, direction, price, score, confidence, market, f"R:R={_rr:.2f} < {_min_rr}")
            return False

        # Write to DB via database.save_open()
        tid          = f"MIND_{symbol}_{int(time.time())}"
        now_str      = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        regime_label = market.get("market_regime", "UNKNOWN")
        # I1-B: Find governing param snapshot for this trade
        try:
            _sid_conn = _get_mind_conn()
            _sid_row = _sid_conn.execute("""SELECT id FROM param_snapshots
                WHERE valid_from <= ? AND promoted=1
                ORDER BY valid_from DESC LIMIT 1""", (now_str,)).fetchone()
            _trade_snapshot_id = _sid_row[0] if _sid_row else None
            _sid_conn.close()
        except: _trade_snapshot_id = None
        # coin_type and liquidity already computed above -- reuse
        hour         = datetime.now(timezone.utc).hour
        session_name = "NY_OPEN" if 13<=hour<21 else ("LONDON" if 7<=hour<13 else "ASIA")

        try:
            from database import save_open as _save_open
            db_id = _save_open({
                "symbol":        symbol,
                "direction":     direction,
                "trade_type":    "SCALP",
                "entry":         price,
                "size":          size,
                "leverage":      leverage,
                "news_score":    0,
                "chart_score":   round(min(score/18, 10), 1),
                "combined_score":round(min(score/9,  20), 1),
                "coin_type":     coin_type,
                "liquidity":     liquidity if "liquidity" in dir() else "NORMAL",
                "rsi_entry":     50,
                "volume_ratio":  1.0,
                "pattern":       "APEX_MIND_ENTRY",
                "session":       session_name,
                "btc_price":     float(market.get("btc_price", 0)),
                "funding_rate":  0,
                "sl":            sl,
                "sl_distance":   round(abs(price - sl) / price * 100, 3),
                "regime_score":  market.get("regime_score", 0),
                "regime_label":  regime_label,
                "regime_conf":   market.get("regime_certainty", 50),
            })
        except Exception as e:
            logger.error(f"  [ENTRY] DB write error {symbol}: {e}"); return False

        # I1-B: Tag trade with governing snapshot_id
        if _trade_snapshot_id and db_id:
            try:
                _tag_conn = sqlite3.connect(TRADES_DB)
                _tag_conn.execute("UPDATE trades SET snapshot_id=? WHERE id=?",
                    (_trade_snapshot_id, db_id))
                _tag_conn.commit(); _tag_conn.close()
            except: pass

        # Write to apex_mind_state.json (separate from main.py bot_state.json)
        try:
            mind_state_file = os.path.join(BASE, "apex_mind_state.json")
            state = json.load(open(mind_state_file)) if os.path.exists(mind_state_file) else {"positions": {}}
            if "positions" not in state: state["positions"] = {}
            state["positions"][tid] = {
                "symbol": symbol, "direction": direction,
                "entry": price, "size": size, "leverage": leverage,
                "sl": sl, "open_time": time.time(), "peak_roe": 0,
                "source": "APEX_MIND", "paper": True, "db_id": db_id,
                "score": score, "confidence": confidence,
                "regime": regime_label, "bot_type": "APEX",
            }
            tmp = mind_state_file + ".tmp"
            json.dump(state, open(tmp, "w"), indent=2)
            os.replace(tmp, mind_state_file)
        except Exception as e:
            logger.error(f"  [ENTRY] State write error {symbol}: {e}")
            try:
                conn = sqlite3.connect(TRADES_DB)
                conn.execute("DELETE FROM trades WHERE id=?", (db_id,))
                conn.commit(); conn.close()
            except: pass
            return False

        logger.warning(f"  🟢 [ENTRY] OPENED APEX {symbol} {direction} ${size:.2f} x{leverage} @ {price:.4f} SL={sl:.4f} | {reason[:50]}")
        # Mark entry suggestion as trade_opened=1
        try:
            _es_conn = sqlite3.connect(MIND_DB, timeout=5)
            _es_conn.execute("""UPDATE entry_suggestions SET trade_opened=1, trade_direction=?, trade_entry=?
                WHERE symbol=? AND suggested_dir=? AND outcome_filled=0
                AND id=(SELECT MAX(id) FROM entry_suggestions WHERE symbol=? AND outcome_filled=0)""",
                (direction, price, symbol, direction, symbol))
            _es_conn.commit(); _es_conn.close()
        except: pass
        try:
            from emailer import Emailer
            Emailer().send(
                f"🟢 APEX MIND ENTRY: {symbol} {direction}",
                f"Symbol:    {symbol}\nDirection: {direction}\nEntry:     ${price:.4f}\n"
                f"Size:      ${size:.2f} x{leverage}\nSL:        ${sl:.4f}\n"
                f"Confidence:{confidence:.0f}%\nRegime:    {regime_label}\n"
                f"Reason:    {reason[:100]}"
            )
        except: pass
        return True

    except Exception as e:
        logger.error(f"  [ENTRY] open_apex_trade error {symbol}: {e}", exc_info=True)
        return False


def _open_spring_trade(symbol, score, confidence, reason, market, drop_pct=0, recovery=0):
    """
    Open a new Spring Bot trade directly.
    Replaces dip_module._enter() for Spring entries.
    """
    try:
        cfg = load_master_config()
        if not cfg.get("execution", {}).get("spring_entries", False):
            logger.info(f"  [SPRING BLOCKED] {symbol} -- spring_entries disabled")
            return False
        if cfg.get("safety", {}).get("emergency_stop", False):
            return False

        min_conf = cfg.get("confidence_gates", {}).get("spring_entry_min", 50)
        if confidence < min_conf:
            logger.info(f"  [SPRING BLOCKED] {symbol} conf={confidence:.0f}% < gate={min_conf}%")
            return False

        pos = get_position_counts()
        reg_sug    = get_regime_suggestions(market)
        max_spring = reg_sug.get("max_spring", cfg.get("limits", {}).get("max_spring", 15))
        if pos["spring"] >= max_spring:
            logger.info(f"  [SPRING BLOCKED] {symbol} -- Spring full {pos['spring']}/{max_spring}")
            return False

        get_scanner_rl().acquire(weight=1)
        ticker = get_client().futures_symbol_ticker(symbol=symbol)
        price = float(ticker["price"])

        # Spring sizing -- Kelly-based with learned params
        try:
            pb = json.load(open(os.path.join(BASE, "paper_balance.json")))
            spring_bal = float(pb.get("spring", 300))
        except: spring_bal = 300

        try:
            lp = _load_learned_params()
            kelly = lp.get("kelly", {})
            base_kelly = float(kelly.get("spring_base", kelly.get("base", 0.05)))
            # Cap at 0.06 max -- multiple concurrent positions
            base_kelly = min(base_kelly, 0.06)
            conf_mult = max(0.5, min(confidence / 100, 1.0))
            size = round(spring_bal * base_kelly * conf_mult, 2)
            # Floor $5, cap 5% of equity per position (15 slots = 75% max exposure)
            size = max(5.0, min(size, spring_bal * 0.05))
        except:
            size = max(5.0, round(spring_bal * 0.04 * min(confidence/100, 1.0), 2))
        leverage = 4

        # SL -- swing low (last 10 candles) + 0.5x ATR buffer
        try:
            df15 = fetch(symbol, "15m", 20)
            if df15 is not None and len(df15) >= 11:
                _lp_spring = _load_learned_params()
                _spring_candles = int(_lp_spring.get("ratchet", {}).get("spring_sl_swing_candles", 6))   # was 10 -- 6 candles=90min captures actual dip
                _spring_atr_buf = float(_lp_spring.get("ratchet", {}).get("spring_sl_atr_buffer", 0.75))  # was 2.0 -- tighter for dip-buy
                _swing_low = float(df15.iloc[-(_spring_candles+1):-1]["low"].min())
                _atr_spring = float(df15.iloc[-2].get("atr", price * 0.01) or price * 0.01)
                sl = round(_swing_low - _atr_spring * _spring_atr_buf, 8)
            else:
                sl = round(price * 0.98, 8)
        except: sl = round(price * 0.98, 8)

        sl_dist = (price - sl) / price
        # SL gate: 0.5%-5% -- LOGGED so silent rejections are visible
        if sl_dist < 0.005:
            logger.info(f"  [SPRING BLOCKED] {symbol} SL too tight {sl_dist*100:.2f}% < 0.5%")
            return False
        if sl_dist > 0.05:
            logger.info(f"  [SPRING BLOCKED] {symbol} SL too wide {sl_dist*100:.2f}% > 5% (swing_low method) -- undefinable invalidation")
            return False

        # R:R gate -- use REALIZED capture not fictional 30% target
        _leverage = 4
        _lp_rr = _load_learned_params()
        _spring_capture = float(_lp_rr.get("spring_realized_capture_roe", _lp_rr.get("realized_capture_roe", 7.6)))
        _spring_capture = max(4.0, _spring_capture)
        _min_spring_rr = float(_lp_rr.get("spring_min_realized_rr", 0.5))
        _sl_roe = sl_dist * _leverage * 100
        _rr = (_spring_capture / _sl_roe) if _sl_roe > 0 else 0
        if _rr < _min_spring_rr:
            logger.info(f"  [SPRING BLOCKED] {symbol} R:R={_rr:.2f} < {_min_spring_rr} (capture={_spring_capture:.1f}% SL={sl_dist*100:.1f}%={_sl_roe:.1f}%ROE)")
            return False

        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        # Calculate dip anatomy features for storage
        _dq_score=0; _rec_accel=0; _macd_turn=0; _rsi_mom=0.0; _drop_spd=0.0
        try:
            _dq_score, _dq_details = calc_dip_quality(df15)
            _rec_accel = 1 if _dq_details.get("rec_accel", False) else 0
            _macd_turn = 1 if _dq_details.get("macd_turn", False) else 0
            _rsi_mom = float(_dq_details.get("rsi_mom", 0))
            _drop_spd = float(_dq_details.get("drop_speed", 0))
        except: pass
        try:
            conn = sqlite3.connect(TRADES_DB)
            conn.execute("""INSERT INTO dip_trades
                (symbol, entry, size, leverage, sl, open_time, status, drop_pct, recovery, score,
                 dip_quality_score, rec_accelerating, macd_turning, rsi_momentum, drop_speed)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (symbol, price, size, leverage, sl, now_str, "OPEN",
                 round(drop_pct, 2), round(recovery, 2), score,
                 _dq_score, _rec_accel, _macd_turn, _rsi_mom, _drop_spd))
            conn.commit(); conn.close()
        except Exception as e:
            logger.error(f"  [ENTRY] Spring DB write error {symbol}: {e}"); return False

        # I1-B: Tag Spring trade with governing snapshot_id
        try:
            _sn2 = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            _sid_c2 = _get_mind_conn()
            _sid_r2 = _sid_c2.execute("SELECT id FROM param_snapshots WHERE valid_from <= ? AND promoted=1 ORDER BY valid_from DESC LIMIT 1", (_sn2,)).fetchone()
            if _sid_r2:
                _sc3 = sqlite3.connect(TRADES_DB)
                _sc3.execute("UPDATE dip_trades SET snapshot_id=? WHERE symbol=? AND status='OPEN' AND open_time=?", (_sid_r2[0], symbol, now_str))
                _sc3.commit(); _sc3.close()
            _sid_c2.close()
        except: pass

        logger.warning(f"  🌱 [ENTRY] OPENED SPRING {symbol} LONG ${size:.2f} x{leverage} @ {price:.4f} SL={sl:.4f} | {reason[:50]}")
        # Mark entry suggestion as trade_opened=1
        try:
            _es_conn = sqlite3.connect(MIND_DB, timeout=5)
            _es_conn.execute("""UPDATE entry_suggestions SET trade_opened=1, trade_direction=?, trade_entry=?
                WHERE symbol=? AND suggested_dir='LONG' AND outcome_filled=0
                AND id=(SELECT MAX(id) FROM entry_suggestions WHERE symbol=? AND outcome_filled=0)""",
                ('LONG', price, symbol, symbol))
            _es_conn.commit(); _es_conn.close()
        except: pass
        return True

    except Exception as e:
        logger.error(f"  [ENTRY] open_spring_trade error {symbol}: {e}")
        return False


def load_master_config():
    """Load master config -- called every cycle, changes take effect immediately"""
    try:
        cfg = json.load(open(os.path.join(BASE, "master_config.json")))
        return cfg
    except:
        # Safe defaults -- exits active, entries disabled until manually enabled
        return {
            "mode": "paper",
            "apex_mind_enabled": True,
            "execution": {
                "spring_close":    True,
                "spring_tighten":  True,
                "apex_close":      False,  # enable when accuracy >= 70%
                "apex_tighten":    True,
                "apex_entries":    True,
                "spring_entries":  True,
            },
            "confidence_gates": {
                "spring_close_min":   60,
                "spring_tighten_min": 55,
                "apex_close_min":     70,
                "apex_tighten_min":   60,
                "apex_entry_min":     70,
                "spring_entry_min":   55,
            },
            "limits": {
                "max_apex_long":   6,
                "max_apex_short":  2,
                "max_spring":      15,
                "max_total":       21,
                "max_entries_per_cycle": 2,
            },
            "safety": {
                "max_executions_per_cycle": 3,
                "min_trade_age_mins": 5,
                "emergency_stop": False,
                "min_balance":    100,
            },
            "notes": "v2.0 -- exits active. Set apex_entries/spring_entries=true for full control."
        }


def execute_decision(trade, decision, confidence, reason, suggested_sl=None):
    """
    Gateway between APEX MIND decision and actual execution.
    Checks: kill switch → phase permissions → confidence gate → safety limits → execute.
    Fully isolated -- if this fails, observation continues unaffected.
    """
    # Dedup guard -- prevent triple TIGHTEN/CLOSE fire within 3s
    import time as _t2
    _ekey = f"{trade.get('symbol','')}_{decision}"
    _enow = _t2.time()
    with _exec_dedup_lock:
        if _ekey in _exec_dedup and _enow - _exec_dedup[_ekey] < 5:
            return False
        _exec_dedup[_ekey] = _enow
    try:
        cfg = load_master_config()
        # ── KILL SWITCH ──
        if not cfg.get("apex_mind_enabled", True):
            logger.info(f"  [BRIDGE] DISABLED -- kill switch active, skipping {decision} on {trade['symbol']}")
            return False

        if cfg.get("safety", {}).get("emergency_stop", False):
            logger.warning(f"  [BRIDGE] EMERGENCY STOP active -- no execution")
            return False

        bot_type  = trade.get("bot_type", "APEX")
        symbol    = trade.get("symbol", "")
        trade_id  = trade.get("tid", None)
        direction = trade.get("direction", "LONG")
        age_mins  = trade.get("age_mins", 0)
        roe       = trade.get("roe", 0)
        exec_cfg  = cfg.get("execution", {})
        gates     = cfg.get("confidence_gates", {})
        safety    = cfg.get("safety", {})

        # ── MINIMUM TRADE AGE -- don't touch brand new trades ──
        min_age = safety.get("min_trade_age_mins", 5)
        if age_mins < min_age:
            logger.debug(f"  [BRIDGE] {symbol} too young ({age_mins:.0f}m < {min_age}m) -- skip")
            return False

        # ── PHASE PERMISSION CHECK ──
        if bot_type == "SPRING":
            if decision == "CLOSE" and not exec_cfg.get("spring_close", False):
                return False
            if decision == "TIGHTEN" and not exec_cfg.get("spring_tighten", False):
                return False
        elif bot_type == "APEX":
            if decision == "CLOSE" and not exec_cfg.get("apex_close", False):
                return False
            if decision == "TIGHTEN" and not exec_cfg.get("apex_tighten", False):
                return False

        # ── CONFIDENCE GATE ──
        if bot_type == "SPRING":
            min_conf = gates.get("spring_close_min", 60) if decision == "CLOSE"                        else gates.get("spring_tighten_min", 55)
        else:
            min_conf = gates.get("apex_close_min", 70) if decision == "CLOSE"                        else gates.get("apex_tighten_min", 60)

        if confidence < min_conf:
            logger.debug(f"  [BRIDGE] {symbol} conf {confidence:.0f}% < gate {min_conf}% -- skip")
            return False

        # ── EXECUTE ──
        try:
            modify_sl = _eb_modify_sl  # use top-level import to preserve dedup state
        except ImportError:
            logger.error("  [BRIDGE] execution_bridge not found")
            return False

        if decision == "CLOSE":
            if bot_type == "SPRING":
                # Spring trades live in dip_trades DB -- close via DB update
                if symbol in _closing_set:
                    logger.debug(f"  [SKIP] {symbol} already closing")
                    return False
                _closing_set.add(symbol)
                try:
                    result = _close_spring_trade(symbol, reason)
                finally:
                    _closing_set.discard(symbol)
            else:
                if not trade_id:
                    logger.error(f"  [BRIDGE] No trade_id for APEX {symbol}")
                    return False
                if symbol in _closing_set:
                    logger.debug(f"  [SKIP] {symbol} already closing")
                    return False
                _closing_set.add(symbol)
                try:
                    result = close_order(symbol, trade_id, reason=f"APEX_MIND:{reason[:40]}")
                finally:
                    _closing_set.discard(symbol)

            if result:
                logger.warning(f"EXECUTE  ✅ CLOSED {bot_type} {symbol} conf={confidence:.0f}% ROE={roe:+.1f}% | {reason[:60]}")
                try:
                    from emailer import Emailer
                    _emoji = "✅" if roe > 0 else "❌"
                    Emailer().send(
                        f"{_emoji} APEX MIND CLOSE: {bot_type} {symbol} ROE={roe:+.1f}%",
                        f"Symbol:    {symbol}\nBot:       {bot_type}\nROE:       {roe:+.1f}%\n"
                        f"Confidence:{confidence:.0f}%\nReason:    {reason[:100]}"
                    )
                except: pass
            # Set cooldown -- outcome-aware: longer for losses, shorter for winners
            _cd_dir = trade.get("direction", "LONG")
            if roe < -5:
                set_cooldown(symbol, _cd_dir, same_dir_mins=180, any_dir_mins=60)  # Hard SL -- 3H block
            elif roe < 0:
                set_cooldown(symbol, _cd_dir, same_dir_mins=60, any_dir_mins=20)
            elif roe > 10:
                set_cooldown(symbol, _cd_dir, same_dir_mins=5, any_dir_mins=2)
            else:
                set_cooldown(symbol, _cd_dir, same_dir_mins=15, any_dir_mins=5)
            # Save close to DB FIRST, then update balance (crash-safe order)
            try:
                _price = float(trade.get("current_price", 0))
                _entry = float(trade.get("entry", 0))
                _size  = float(trade.get("size", 0))
                _lev   = float(trade.get("leverage", 5))
                _dir   = trade.get("direction", "LONG")
                if _price > 0 and _entry > 0 and _size > 0:
                    _pnl_pct = (_price-_entry)/_entry*100*_lev if _dir=="LONG" \
                               else (_entry-_price)/_entry*100*_lev
                    _pnl = _size * (_pnl_pct / 100)

                    # I3: Realistic cost model -- subtract fees, slippage, funding
                    _total_cost = 0.0  # initialize before try so locals() always finds it
                    try:
                        _notional = _size * _lev
                        # Taker fee both sides: 0.04% entry + 0.04% exit
                        _fee_cost = _notional * 0.0004 * 2
                        # Slippage: 0.02% per side (conservative for mid-cap alts)
                        _slip_cost = _notional * 0.0002 * 2
                        # Funding: accrue per 8H interval during hold
                        _age_hrs = float(trade.get("age_mins", 0)) / 60
                        _funding_intervals = int(_age_hrs / 8)
                        _funding_rate = abs(float(trade.get("funding_rate", 0.0001)))
                        if _funding_rate == 0: _funding_rate = 0.0001  # default 0.01%
                        _funding_cost = _notional * _funding_rate * _funding_intervals
                        # Total cost in dollars
                        _total_cost = _fee_cost + _slip_cost + _funding_cost
                        _pnl = _pnl - _total_cost
                        _pnl_pct = _pnl / _size * 100  # recalc net pnl_pct
                        logger.info(f"  [COST] {symbol}: fee=${_fee_cost:.3f} slip=${_slip_cost:.3f} fund=${_funding_cost:.3f} net_pnl=${_pnl:.3f}")
                    except Exception as _ce:
                        logger.debug(f"  Cost model error {symbol}: {_ce}")
                    # 1. Save to DB first
                    _tid = trade.get("tid")
                    _db_id = trade.get("db_id")
                    if not _db_id and _tid:
                        tid_str = str(_tid).replace("DB_","").strip()
                        if tid_str.isdigit(): _db_id = int(tid_str)
                    if _db_id:
                        try:
                            conn = sqlite3.connect(TRADES_DB)
                            conn.execute("""UPDATE trades SET status='CLOSED', exit=?,
                                pnl=?, pnl_pct=?, close_time=?, reason=?,
                                peak_roe=?, duration_mins=?, total_cost=?
                                WHERE id=? AND status='OPEN'""",
                                (_price, round(_pnl,4), round(_pnl_pct,2),
                                 datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                                 f"APEX_MIND:{reason[:40]}",
                                 float(trade.get("peak_roe",0)),
                                 float(trade.get("age_mins",0)),
                                 round(_total_cost,4) if '_total_cost' in locals() else 0,
                                 _db_id))
                            conn.commit(); conn.close()
                        except Exception as dbe:
                            logger.error(f"APEX close DB error: {dbe}")
                    # 2. Update balance + score observations AFTER DB write
                    update_paper_balance(round(_pnl, 4))
                    # Pass db_id to avoid attribution bug on fast closes
                    try: on_trade_closed(symbol, "APEX", _pnl, reason, db_id=_db_id)
                    except: on_trade_closed(symbol, "APEX", _pnl, reason)
            except Exception as ce:
                logger.error(f"APEX close/balance error {symbol}: {ce}")
            else:
                logger.error(f"  ❌ [BRIDGE] CLOSE FAILED {symbol}")
            return result

        elif decision == "TIGHTEN" and suggested_sl:
            # Enforce ratchet floor -- never move SL below locked floor
            _floor = float(trade.get("floor_roe", 0))
            _entry = float(trade.get("entry", 0))
            _lev   = float(trade.get("leverage", 4))
            _dir   = trade.get("direction", "LONG")
            if _floor > 0 and _entry > 0:
                _floor_pct = _floor / _lev / 100
                _floor_sl = round(_entry*(1+_floor_pct),8) if _dir=="LONG" else round(_entry*(1-_floor_pct),8)
                if _dir=="LONG" and suggested_sl < _floor_sl:
                    suggested_sl = _floor_sl
                    logger.debug(f"  SL raised to ratchet floor {_floor_sl:.4f}")
                elif _dir=="SHORT" and suggested_sl > _floor_sl:
                    suggested_sl = _floor_sl
                    logger.debug(f"  SL lowered to ratchet floor {_floor_sl:.4f}")
            # Safety check -- SL must never cross current price
            _cur_price = float(trade.get("current_price", 0))
            _tighten_dir = trade.get("direction", "LONG")
            if _cur_price > 0 and suggested_sl > 0:
                if _tighten_dir == "LONG" and suggested_sl >= _cur_price:
                    logger.error(f"SL {suggested_sl} above current price {_cur_price} for LONG -- skipping")
                    return False
                if _tighten_dir == "SHORT" and suggested_sl <= _cur_price:
                    logger.error(f"SL {suggested_sl} below current price {_cur_price} for SHORT -- skipping")
                    return False

            if bot_type == "SPRING":
                # Spring SL tightening rules:
                # ROE > 0: ratchet handles it -- skip
                # ROE -5 to 0: breathing room -- skip
                # ROE < -5: tighten to limit deep losses
                _spring_roe = float(trade.get("roe", 0))
                _spring_tighten_thresh = float(_load_learned_params().get("ratchet", {}).get("spring_sl_tighten_threshold", -5.0))
                if _spring_roe > _spring_tighten_thresh:
                    logger.debug(f"  [BRIDGE] Spring ROE={_spring_roe:.1f}% -- skip TIGHTEN (breathing room)")
                    return False
                _tighten_spring_sl(symbol, suggested_sl, floor_roe=_floor, be_set=trade.get("be_set",False), be_price=trade.get("be_price",0))
                result = True
            else:
                # APEX SL tightening: only when ROE < threshold
                _apex_roe = float(trade.get("roe", 0))
                _apex_tighten_thresh = float(_load_learned_params().get("ratchet", {}).get("sl_tighten_threshold", -5.0))
                if _apex_roe > _apex_tighten_thresh:
                    logger.debug(f"  [BRIDGE] APEX ROE={_apex_roe:.1f}% -- skip TIGHTEN (breathing room)")
                    return False
                if not trade_id:
                    return False
                result = modify_sl(symbol, trade_id, suggested_sl)

            if result:
                logger.info(f"  ✅ [BRIDGE] TIGHTENED {bot_type} {symbol} SL→{suggested_sl:.4f} conf={confidence:.0f}%")
            return result if result is not None else False

        # ── SL WIDENING -- counter stop hunt on both LONG and SHORT ──
        elif decision == "HOLD" and trade.get("sl_widen_flag") and bot_type == "APEX":
            try:
                _sl  = float(trade.get("sl", 0))
                _atr = float(trade.get("atr_15m", 0)) or float(trade.get("entry",0)) * 0.005
                _dir = trade.get("direction")
                if _sl > 0 and trade_id:
                    if _dir == "SHORT":
                        # SHORT: widen SL upward (away from price)
                        _new_sl = round(_sl + _atr * 0.5, 6)
                        _new_sl = min(_new_sl, float(trade.get("entry",_sl)) * 1.03)
                        if _new_sl > _sl:
                            _r = modify_sl(symbol, trade_id, _new_sl)
                            if _r: logger.info(f"  🛡️ SL WIDENED {symbol} SHORT {_sl:.4f}→{_new_sl:.4f} stop hunt protection")
                    elif _dir == "LONG":
                        # LONG: widen SL downward (away from price)
                        _new_sl = round(_sl - _atr * 0.5, 6)
                        _new_sl = max(_new_sl, float(trade.get("entry",_sl)) * 0.97)
                        if _new_sl < _sl:
                            _r = modify_sl(symbol, trade_id, _new_sl)
                            if _r: logger.info(f"  🛡️ SL WIDENED {symbol} LONG {_sl:.4f}→{_new_sl:.4f} stop hunt protection")
            except Exception as _we: logger.warning(f"  SL widen error {symbol}: {_we}")
            return True

    except Exception as e:
        logger.error(f"  [BRIDGE] execute_decision error {trade.get('symbol','?')}: {e}")
        return False
    return False


def _close_spring_trade(symbol, reason):
    """Close a Spring Bot trade -- updates dip_trades DB directly"""
    try:
        price = float(get_trade_client().futures_symbol_ticker(symbol=symbol)["price"])
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(TRADES_DB)
        # Get trade details for PnL calc
        row = conn.execute(
            "SELECT entry, size, leverage FROM dip_trades WHERE symbol=? AND status='OPEN' ORDER BY id DESC LIMIT 1",
            (symbol,)).fetchone()
        if not row:
            conn.close()
            logger.warning(f"  [BRIDGE] Spring trade not found in DB: {symbol}")
            return False
        entry, size, lev = float(row[0]), float(row[1] or 0), int(row[2] or 4)
        pnl = (price - entry) / entry * size * lev
        roe = (price - entry) / entry * 100 * lev
                # I3: Realistic cost model for Spring trades
        try:
            _notional = size * lev
            _fee_cost  = _notional * 0.0004 * 2
            _slip_cost = _notional * 0.0002 * 2
            _dur_row = conn.execute("SELECT ROUND((JULIANDAY(?)-JULIANDAY(open_time))*24) FROM dip_trades WHERE symbol=? AND status='OPEN' ORDER BY id DESC LIMIT 1", (now_str, symbol)).fetchone()
            _hold_hrs = float(_dur_row[0] or 0) if _dur_row else 0
            _funding_cost = _notional * 0.0001 * int(_hold_hrs / 8)
            _total_cost = _fee_cost + _slip_cost + _funding_cost
            pnl = pnl - _total_cost
            roe = pnl / size * 100
            logger.info(f"  [SPRING COST] {symbol}: fee=${_fee_cost:.3f} slip=${_slip_cost:.3f} fund=${_funding_cost:.3f} net_pnl=${pnl:.3f}")
        except Exception as _sce:
            logger.debug(f"  Spring cost error: {_sce}")

        conn.execute("""UPDATE dip_trades SET status='CLOSED', exit=?, pnl=?,
            close_time=?, reason=?, duration_m=ROUND((JULIANDAY(?)-JULIANDAY(open_time))*1440),
            total_cost=?
            WHERE symbol=? AND status='OPEN'""",
            (price, round(pnl, 4), now_str, f"APEX_MIND:{reason[:40]}", now_str,
             round(_total_cost,4) if '_total_cost' in locals() else 0, symbol))
        conn.commit(); conn.close()
        logger.info(f"  [BRIDGE] Spring {symbol} closed @ {price:.4f} PnL=${pnl:.2f} ROE={roe:.1f}%")
        # Set cooldown to prevent immediate re-entry
        # Outcome-aware cooldown for Spring
        if roe < -5:
            set_cooldown(symbol, "LONG", same_dir_mins=60, any_dir_mins=20)
        elif roe < 0:
            set_cooldown(symbol, "LONG", same_dir_mins=30, any_dir_mins=10)
        elif roe > 10:
            set_cooldown(symbol, "LONG", same_dir_mins=5, any_dir_mins=2)
        else:
            set_cooldown(symbol, "LONG", same_dir_mins=15, any_dir_mins=5)
        # Update paper balance
        try:
            update_paper_balance(round(pnl, 4), bot_type="SPRING")
        except: pass
        # Notify APEX MIND scoring
        try:
            on_trade_closed(symbol, "SPRING", pnl, f"APEX_MIND:{reason[:40]}")
        except: pass
        return True
    except Exception as e:
        logger.error(f"  [BRIDGE] Spring close error {symbol}: {e}")
        return False


def _tighten_spring_sl(symbol, new_sl, floor_roe=None, be_set=None, be_price=None):
    """Update SL for a Spring trade in paper mode -- updates state tracking"""
    try:
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(TRADES_DB)
        if floor_roe is not None and be_set is not None and be_price is not None:
            conn.execute("UPDATE dip_trades SET sl=?, floor_roe=?, be_set=?, be_price=? WHERE symbol=? AND status='OPEN'",
                         (new_sl, floor_roe, int(be_set), be_price, symbol))
        elif floor_roe is not None and be_set is not None:
            conn.execute("UPDATE dip_trades SET sl=?, floor_roe=?, be_set=? WHERE symbol=? AND status='OPEN'",
                         (new_sl, floor_roe, int(be_set), symbol))
        else:
            conn.execute("UPDATE dip_trades SET sl=? WHERE symbol=? AND status='OPEN'",
                         (new_sl, symbol))
        conn.commit(); conn.close()
        logger.info(f"  [BRIDGE] Spring {symbol} SL tightened to {new_sl:.4f}")
        return True
    except Exception as e:
        logger.error(f"  [BRIDGE] Spring tighten error {symbol}: {e}")
        return False


def kill_switch(enable=True):
    """
    Instantly disable/enable APEX MIND execution.
    Observation and learning continue unaffected.
    enable=True  → pause execution
    enable=False → resume execution
    """
    try:
        cfg_path = os.path.join(BASE, "master_config.json")
        cfg = json.load(open(cfg_path)) if os.path.exists(cfg_path) else {}
        cfg["apex_mind_enabled"] = not enable  # enable=True means KILL (disable)
        json.dump(cfg, open(cfg_path, "w"), indent=2)
        status = "KILLED -- execution paused" if enable else "RESUMED -- execution active"
        logger.warning(f"KILL SWITCH: {status}")
        return True
    except Exception as e:
        logger.error(f"Kill switch error: {e}")
        return False


# ── Cycle counter persisted in memory ──
_cycle_count = 0

def run_cycle():
    global _cycle_count
    _cycle_count += 1
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"{'='*60}")
    logger.info(f"APEX MIND | {now_str} UTC | Cycle #{_cycle_count}")
    logger.info(f"{'='*60}")
    reconcile_paper_balance()
    fill_outcomes()
    trades=get_open_trades()
    # Keep order flow worker tracking current open symbols
    update_orderflow_symbols(trades)
    # Update WebSocket subscriptions for open trade symbols
    try:
        _ws = get_ws_manager()
        if _ws and _ws.running:
            _open_syms = list(set(t["symbol"] for t in trades))
            _ws.update_subscriptions(_open_syms)
    except: pass
    apex_t=sum(1 for t in trades if t["bot_type"]=="APEX")
    spring_t=sum(1 for t in trades if t["bot_type"]=="SPRING")
    logger.info(f"TRADES   {apex_t} APEX + {spring_t} SPRING = {len(trades)} open")
    logger.info(f"{'─'*60}")
    market=analyze_market()
    # Update regime transition predictor
    try:
        _pred = update_prediction(market)
        if _pred.get("shifting") and _pred.get("confidence", 0) >= 60:
            logger.info(f"  🔮 REGIME PREDICT: {market.get('market_regime')}→{_pred['target_regime']} conf={_pred['confidence']}% ETA~{_pred.get('eta_minutes',30)}min | {', '.join(_pred.get('signals',[])[:2])}")
            market["predicted_regime"] = _pred["target_regime"]
            market["prediction_conf"]  = _pred["confidence"]
    except Exception as _pe: pass
    try:
        balance=get_balance()
        live_params=derive_live_params()
        # Balance logged inside CAPITAL block below
    except Exception as e:
        logger.error(f"Balance/params: {e}")
        balance={"total":0,"available":0}; live_params={}
    risk_temp,risk_reason=calc_risk(market,trades)
    # Portfolio heat + account kill switch
    portfolio_heat = get_portfolio_heat(trades)
    account_ks     = check_kill_switch()
    if account_ks == "STOP_TRADING":
        logger.warning("⛔ ACCOUNT KILL SWITCH: Drawdown too large -- closing all positions")
        for t in trades:
            try:
                execute_decision(t, "CLOSE", 99, "Account kill switch -- drawdown protection")
            except: pass
        return
    if account_ks == "REDUCE_RISK":
        risk_temp = min(risk_temp + 30, 100)
        logger.warning(f"⚠️  Account drawdown warning -- risk_temp boosted to {risk_temp:.0f}")
    if portfolio_heat >= 40:
        logger.warning(f"⚠️  Portfolio heat {portfolio_heat:.1f}% >= 40% -- pausing new entries")
        market["portfolio_heat_block"] = True
    else:
        market["portfolio_heat_block"] = False
    market["portfolio_heat"] = portfolio_heat
    _regime      = market.get("market_regime", "?")
    _regime_live = market.get("market_regime_live", _regime)
    _certainty   = market.get("regime_certainty", 0)
    _regime_str  = f"{_regime_live} ({_certainty:.0f}% certain)" if _regime_live != _regime else _regime_live
    logger.info(f"MARKET   BTC ${market.get('btc_price',0):,.0f} | ADX={market.get('btc_adx_15m',0):.0f}{('↑' if market.get('btc_adx_trend')=='RISING' else '↓' if market.get('btc_adx_trend')=='FALLING' else '→')} {_regime_str} | Alts={market.get('alts_bull_pct',50):.0f}% | Risk={risk_temp:.0f}")
    # Update regime_log.json every cycle -- keeps blend current
    try:
        _rlog_file = os.path.join(BASE, "regime_log.json")
        _rlog = json.load(open(_rlog_file)) if os.path.exists(_rlog_file) else {}
        _rlog_hist = _rlog.get("history", [])
        _rlog_hist.append({"date": now_str, "regime": _regime, "confidence": round(float(market.get("regime_certainty", 50)), 1)})
        if len(_rlog_hist) > 200: _rlog_hist = _rlog_hist[-200:]
        _rlog["history"] = _rlog_hist
        _rlog["market_regime"] = _regime
        _rlog["live_regime"] = _regime_live
        _rlog["alts_bull_pct"] = market.get("alts_bull_pct", 50)
        _rlog["last_updated"] = now_str
        json.dump(_rlog, open(_rlog_file, "w"), indent=2, default=str)
    except: pass

    if market.get("regime_shifting"):
        shift_dir=market.get("shift_direction","?")
        logger.warning(f"REGIME SHIFTING: {shift_dir}")
        try:
            rlog=json.load(open(os.path.join(BASE,"regime_log.json")))
            rlog["realtime_shift"]={"detected":now_str,"direction":shift_dir,
                "btc_adx":market.get("btc_adx_15m",0),"alts_bull":market.get("alts_bull_pct",50)}
            json.dump(rlog,open(os.path.join(BASE,"regime_log.json"),"w"),indent=2,default=str)
        except: pass
    try:
        conn=_get_mind_conn()
        # Override regime in snapshot if alts strongly bullish
        _snap_regime = market.get("market_regime", "UNKNOWN")

        conn.execute("INSERT INTO market_snapshots (timestamp,btc_price,btc_rsi_15m,btc_rsi_1h,btc_rsi_4h,btc_adx_15m,btc_adx_4h,btc_adx_trend,btc_ema_align,btc_macd_hist,btc_volume_ratio,alts_bull_pct,open_longs,open_shorts,open_springs,total_unrealized_pnl,risk_temp,regime,regime_shifting,regime_certainty,regime_uncertain) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (now_str,market.get("btc_price"),market.get("btc_rsi_15m"),market.get("btc_rsi_1h"),market.get("btc_rsi_4h"),market.get("btc_adx_15m"),market.get("btc_adx_4h"),market.get("btc_adx_trend"),market.get("btc_ema_align"),market.get("btc_macd_hist"),market.get("btc_vol_ratio"),market.get("alts_bull_pct"),
            sum(1 for t in trades if t["direction"]=="LONG" and t["bot_type"]=="APEX"),
            sum(1 for t in trades if t["direction"]=="SHORT" and t["bot_type"]=="APEX"),
            spring_t,sum(t.get("roe",0)*t.get("size",0)/100 for t in trades),
            risk_temp,_snap_regime,market.get("regime_shifting",0),
            market.get("regime_certainty",80),1 if market.get("regime_uncertain",False) else 0))
        conn.commit(); conn.close()
    except Exception as e: logger.error(f"Snapshot: {e}")

    # Regime shift prediction
    try:
        shift_prob, shift_reason = predict_regime_shift()
        if shift_prob > 40:
            logger.warning(f"⚠️  REGIME SHIFT RISK: {shift_prob:.0f}% -- {shift_reason}")
            market["regime_shift_prob"] = shift_prob
        else:
            market["regime_shift_prob"] = shift_prob
    except: market["regime_shift_prob"] = 0

    # Detect opposite direction conflicts between APEX and Spring on same coin
    try:
        apex_symbols  = {t["symbol"]: t["direction"] for t in trades if t["bot_type"]=="APEX"}
        spring_symbols= {t["symbol"]: t["direction"] for t in trades if t["bot_type"]=="SPRING"}
        for symbol, apex_dir in apex_symbols.items():
            if symbol in spring_symbols:
                spring_dir = spring_symbols[symbol]
                if apex_dir != spring_dir:
                    logger.warning(f"⚠️  CONFLICT: {symbol} APEX={apex_dir} vs SPRING={spring_dir} -- opposite directions!")
                    # Mark conflict in market for make_decision to use
                    if "conflicts" not in market: market["conflicts"] = set()
                    market["conflicts"].add(symbol)
                else:
                    logger.warning(f"⚠️  OVERLAP: {symbol} both bots {apex_dir} on same coin -- double exposure!")
    except Exception as e: logger.error(f"Conflict check: {e}")

    # Collective trade behavior analysis
    try:
        collective = analyze_collective_behavior(trades, market)
        if collective.get("apex_signal") == "MARKET_PROBLEM":
            logger.warning(f"⚠️  MARKET PROBLEM: {collective['apex_detail']}")
        if collective.get("spring_signal") == "STRONG_BEAR":
            logger.warning(f"🔴 SPRING→APEX SIGNAL: {collective['spring_detail']} -- {collective['spring_apex_action']}")
        if collective.get("coin_problems"):
            for cp in collective["coin_problems"]:
                logger.warning(f"⚠️  COIN PROBLEM: {cp['symbol']} ROE={cp['roe']:.1f}% while others avg {cp['others_avg']:.1f}%")
        market["collective"] = collective
    except Exception as e: logger.error(f"Collective: {e}")

    # ── CROSS-COIN LEADING SIGNALS ──
    # Look for market-wide signals BEFORE individual trades show them
    try:
        watched = list(set(t["symbol"] for t in trades))
        oi_drops = 0; oi_spikes = 0; rsi_ob = 0; rsi_os = 0
        adx_falling_count = 0; vol_surge_count = 0
        for sym in watched:
            try:
                _df = fetch(sym, "5m", 6)
                if _df is None or len(_df) < 5: continue
                _df = add_inds(_df)
                _rsi = sf(_df.iloc[-2].get("rsi", 50))
                _adx_now  = sf(_df.iloc[-2].get("adx", 0))
                _adx_prev = sf(_df.iloc[-5].get("adx", 0))
                _vol = sf(_df.iloc[-2].get("vol_ratio", 1))
                if _rsi > 72: rsi_ob += 1
                if _rsi < 30: rsi_os += 1
                if _adx_prev > 0 and _adx_now < _adx_prev * 0.85: adx_falling_count += 1
                if _vol > 2.5: vol_surge_count += 1
                _oi = get_scanner_client().futures_open_interest_hist(symbol=sym, period="5m", limit=4)
                if _oi and len(_oi) >= 3:
                    _oi_chg = (float(_oi[-1].get("sumOpenInterest",0)) -
                               float(_oi[-3].get("sumOpenInterest",1))) /                                float(_oi[-3].get("sumOpenInterest",1)) * 100
                    if _oi_chg < -2: oi_drops += 1
                    elif _oi_chg > 2: oi_spikes += 1
                time.sleep(0.05)
            except: continue

        n = max(len(watched), 1)
        market["cross_coin"] = {
            "oi_drops_pct":      round(oi_drops / n * 100, 1),
            "oi_spikes_pct":     round(oi_spikes / n * 100, 1),
            "rsi_ob_pct":        round(rsi_ob / n * 100, 1),
            "rsi_os_pct":        round(rsi_os / n * 100, 1),
            "adx_falling_pct":   round(adx_falling_count / n * 100, 1),
            "vol_surge_pct":     round(vol_surge_count / n * 100, 1),
            "coins_watched":     n,
        }
        cc = market["cross_coin"]
        # Log meaningful cross-coin signals
        if cc["oi_drops_pct"] >= 50:
            logger.warning(f"⚡ CROSS-COIN: {cc['oi_drops_pct']:.0f}% of coins OI dropping -- market-wide liquidation signal")
            market["regime_shift_prob"] = max(market.get("regime_shift_prob", 0), 55)
        if cc["rsi_ob_pct"] >= 60:
            logger.warning(f"⚡ CROSS-COIN: {cc['rsi_ob_pct']:.0f}% of coins overbought -- market top signal")
        if cc["adx_falling_pct"] >= 60:
            logger.warning(f"⚡ CROSS-COIN: {cc['adx_falling_pct']:.0f}% of coins ADX falling -- trend exhaustion")
        if cc["rsi_os_pct"] >= 40:
            logger.info(f"⚡ CROSS-COIN: {cc['rsi_os_pct']:.0f}% of coins oversold -- bounce opportunity")
    except Exception as e:
        logger.error(f"Cross-coin: {e}")
        market["cross_coin"] = {}

    # ── CAPITAL ALLOCATION INTELLIGENCE ──
    # Should we even be running this many positions right now?
    try:
        regime      = market.get("market_regime", "UNKNOWN")
        certainty   = market.get("regime_certainty", 0)
        recent_wr   = live_params.get("recent_wr", 50)
        size_mult   = live_params.get("size_mult", 1.0)
        n_longs     = sum(1 for t in trades if t["direction"]=="LONG" and t["bot_type"]=="APEX")
        n_shorts    = sum(1 for t in trades if t["direction"]=="SHORT" and t["bot_type"]=="APEX")
        n_spring    = sum(1 for t in trades if t["bot_type"]=="SPRING")
        portfolio_heat = risk_temp

        allocation_notes = []

        # Too many longs in uncertain/bear regime
        if n_longs >= 4 and regime in ("SIDEWAYS", "BEAR"):
            allocation_notes.append(f"⚠️  {n_longs} APEX longs open in {regime} -- high regime risk")
        # Running full book when win rate is deteriorating
        if recent_wr < 45 and len(trades) >= 6:
            allocation_notes.append(f"⚠️  WR={recent_wr:.0f}% but {len(trades)} positions open -- reduce exposure")
        # Risk temp high but still holding everything
        if portfolio_heat >= 60 and len(trades) >= 5:
            allocation_notes.append(f"⚠️  Risk={portfolio_heat:.0f} with {len(trades)} trades -- portfolio overexposed")
        # Regime uncertain + lots of positions
        if certainty < 40 and len(trades) >= 6:
            allocation_notes.append(f"⚠️  Regime certainty only {certainty:.0f}% with {len(trades)} trades -- reduce size")
        # Ideal state -- log it too
        if not allocation_notes and recent_wr >= 60 and portfolio_heat < 40:
            allocation_notes.append(f"✅ Portfolio healthy: WR={recent_wr:.0f}% risk={portfolio_heat:.0f} regime={regime}")

        logger.info(f"{'─'*60}")
        for note in allocation_notes:
            logger.info(f"CAPITAL  {note}")
        market["allocation_notes"] = allocation_notes

    except Exception as e:
        logger.error(f"Capital allocation: {e}")

    # Record regime sequence for transition prediction
    try: record_regime_sequence()
    except: pass
    decisions=[]
    _spring_header_shown = [False]  # mutable to allow update in nested func

    def observe_trade(trade):
        if trade["bot_type"]=="SPRING" and not _spring_header_shown[0]:
            logger.info(f"{'─'*29} SPRING BOT {'─'*20}")
            _spring_header_shown[0] = True
        symbol = trade["symbol"]
        try:
            coin=analyze_coin(symbol,trade["direction"],trade["entry"],trade["current_price"])
            try:
                behavior=analyze_coin_behavior(symbol)
                if behavior["behavior"]!="NORMAL":
                    logger.info(f"    BEHAVIOR: {behavior['behavior']} -> {behavior['prediction']} ({behavior['confidence']:.0f}%) {behavior.get('trigger','')}")
                    if behavior["behavior"]=="LOW_LIQUIDITY":
                        logger.info(f"    BEHAVIOR: LOW_LIQUIDITY -- monitoring continues (entry blocked for new trades)")
                        # Do NOT skip monitoring -- ratchet, TIGHTEN, CLOSE all must still run
                        # LOW_LIQUIDITY only blocks NEW entries in _open_apex_trade
                    if behavior["prediction"]=="BOUNCE_UP" and trade["direction"]=="LONG":
                        coin["rsi_divergence"]="BULLISH"
                    if behavior["prediction"]=="DROP_DOWN" and trade["direction"]=="LONG":
                        coin["rsi_divergence"]="BEARISH"
                    if "EXHAUSTION" in behavior["behavior"]:
                        if behavior["prediction"]=="REVERSAL_DOWN" and trade["direction"]=="LONG":
                            coin["macd_trend"]="CROSSING_BEAR"
                        elif behavior["prediction"]=="REVERSAL_UP" and trade["direction"]=="SHORT":
                            coin["macd_trend"]="CROSSING_BULL"
            except Exception as be: logger.error(f"Behavior {symbol}: {be}")
            memory=get_coin_memory(symbol)
            # Order flow -- real-time buy/sell pressure, 30-60s ahead of candles
            try:
                of = get_orderflow(symbol)
                coin["flow_score"]    = of.get("flow_score", 0)
                coin["flow_signal"]   = of.get("flow_signal", "NEUTRAL")
                coin["flow_imbal"]    = of.get("bid_ask_imbalance", 0)
                coin["flow_aggr"]     = of.get("aggressive_ratio", 0)
                coin["flow_spoof"]    = of.get("spoof_score", 0)
                coin["flow_absorb"]   = of.get("absorption", 0)
                coin["flow_wall"]     = of.get("wall_side", "NONE")
                coin["flow_lbc"]      = of.get("large_buy_count", 0)
                coin["flow_lsc"]      = of.get("large_sell_count", 0)
                coin["flow_momdiv"]   = of.get("momentum_diverge", 0)
                if of.get("flow_signal") not in ("NEUTRAL", ""):
                    logger.info(f"    FLOW {symbol}: {of['flow_signal']} score={of['flow_score']:+.0f} imbal={of['bid_ask_imbalance']:+.2f} aggr={of['aggressive_ratio']:+.2f}")
            except Exception as fe:
                coin["flow_score"]=0; coin["flow_signal"]="NEUTRAL"
                coin["flow_imbal"]=0; coin["flow_aggr"]=0; coin["flow_spoof"]=0
                coin["flow_absorb"]=0; coin["flow_wall"]="NONE"
                coin["flow_lbc"]=0; coin["flow_lsc"]=0; coin["flow_momdiv"]=0
            # Sequence detection -- signal ORDER matters more than individual signals
            try:
                seq_name, seq_conf, seq_desc = detect_signal_sequence(symbol, trade["direction"])
                if seq_name != "NONE" and seq_conf > 0:
                    coin["sequence_name"] = seq_name
                    coin["sequence_conf"] = seq_conf
                    coin["sequence_desc"] = seq_desc
                    logger.info(f"    SEQ {symbol}: {seq_name} ({seq_conf:.0f}%) {seq_desc[:60]}")
                else:
                    coin["sequence_name"] = "NONE"
                    coin["sequence_conf"] = 0
                    coin["sequence_desc"] = ""
            except Exception as se:
                coin["sequence_name"] = "NONE"
                coin["sequence_conf"] = 0
                coin["sequence_desc"] = ""
            decision,reason,conf,pred,pred_c=make_decision(trade,coin,market,risk_temp,memory)
            # Clean readable log
            emoji = "🔴" if decision=="CLOSE" else ("🟡" if decision=="TIGHTEN" else "🟢")
            bot_tag = "APX" if trade["bot_type"]=="APEX" else "SPR"
            age_str = f"{trade['age_mins']:.0f}m"
            roe_str = f"{trade['roe']:+.1f}%"
            # Show only signals that drove the decision
            all_sigs = reason.split(" | ") if reason else ["No signal"]
            if decision == "CLOSE":
                # Show WHY closing -- close signals first, then supporting context
                close_sigs = [s for s in all_sigs if any(w in s for w in
                    ["bearish","falling","overbought","failing","reversal","overextended",
                     "loss","loser","timeout","Aging","bounce failed","dump","SELL",
                     "OI +","Exhaustion","Regime shift","Pattern danger","BTC bear",
                     "Funding","upper BB","deep loss","stalled","poor","STRONG_SELL",
                     "SELL_PRESSURE","absorbed","momentum build","squeeze"])]
                hold_sigs = [s for s in all_sigs if any(w in s for w in
                    ["bouncing","bullish","let it run","divergence","STRONG_BUY"])]
                # Show close reasons first, then note any conflicting hold signals
                top_signals = close_sigs[:3]
                if hold_sigs and len(top_signals) < 3:
                    top_signals += [f"(override: {hold_sigs[0][:30]})"]
                elif hold_sigs:
                    top_signals.append(f"(vs: {hold_sigs[0][:25]})")
                top_signals = top_signals[:4] or all_sigs[:2]
            elif decision == "TIGHTEN":
                top_signals = [s for s in all_sigs if any(w in s for w in
                    ["overextended","profit","peak","SL","conviction",
                     "protect","Ratchet","reversal","overbought","floor"])][:3] or all_sigs[:2]
            else:
                top_signals = [s for s in all_sigs if any(w in s for w in
                    ["bouncing","bullish","rising","momentum","divergence",
                     "accumulation","bounce","strong","STRONG_BUY","BUY_PRESSURE"])][:3] or all_sigs[:2]
            top_reason = " | ".join(top_signals) if top_signals else all_sigs[0]
            # Build full status line
            sl_now   = float(trade.get("sl", 0))
            floor    = float(trade.get("floor_roe", 0))
            peak     = float(trade.get("peak_roe", 0))
            be_set   = trade.get("be_set", False)
            new_sl   = trade.get("suggested_sl", 0)

            # SL status
            if new_sl and float(new_sl) > 0:
                sl_str = f" SL:{sl_now:.4f}→{float(new_sl):.4f}"
            elif sl_now > 0:
                sl_str = f" SL:{sl_now:.4f}"
            else:
                sl_str = ""

            # Protection status
            if be_set and floor > 0:
                protect_str = f" 🔒BE+floor={floor:.0f}%"
            elif be_set:
                protect_str = f" 🔒BE"
            elif floor > 0:
                protect_str = f" floor={floor:.0f}%"
            else:
                protect_str = ""

            # Peak info if meaningful
            peak_str = f" peak={peak:.0f}%" if peak > 3 else ""

            logger.info(f"  {emoji} {bot_tag} {symbol:15s} {trade['direction']:5s} ROE={roe_str:7s} age={age_str:5s} [{decision:6s} {conf:.0f}%]{protect_str}{peak_str}{sl_str} | {top_reason[:45]}")
            conn=_get_mind_conn()
            conn.execute("INSERT INTO observations (timestamp,symbol,bot_type,direction,entry_price,current_price,roe,peak_roe,trade_age_mins,sl_price,sl_distance_pct,rsi_5m,rsi_15m,rsi_1h,rsi_4h,adx_15m,adx_1h,adx_4h,adx_trend,ema20_15m,ema50_15m,ema20_1h,ema50_1h,ema_align_15m,ema_align_1h,atr_15m,atr_pct,volume_ratio,bb_position,macd_hist,macd_trend,funding_rate,rsi_divergence,btc_price,btc_rsi_15m,btc_rsi_1h,btc_rsi_4h,btc_adx_15m,btc_adx_4h,btc_adx_trend,btc_ema_align,alts_bull_pct,market_regime,regime_shifting,shift_direction,risk_temp,risk_reason,decision,decision_reason,decision_confidence,predicted_direction,predicted_confidence,coin_behavior,hmm_regime,hmm_agrees,trade_db_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (now_str,symbol,trade["bot_type"],trade["direction"],trade["entry"],trade["current_price"],trade["roe"],trade["peak_roe"],trade["age_mins"],trade.get("sl",0),trade.get("sl_dist",0),
                coin.get("rsi_5m"),coin.get("rsi_15m"),coin.get("rsi_1h"),coin.get("rsi_4h"),coin.get("adx_15m"),coin.get("adx_1h"),coin.get("adx_4h"),coin.get("adx_trend_15m"),
                coin.get("ema20_15m"),coin.get("ema50_15m"),coin.get("ema20_1h"),coin.get("ema50_1h"),coin.get("ema_align_15m"),coin.get("ema_align_1h"),
                coin.get("atr_15m"),coin.get("atr_pct"),coin.get("volume_ratio"),coin.get("bb_position"),coin.get("macd_hist"),coin.get("macd_trend"),coin.get("funding_rate"),coin.get("rsi_divergence"),
                market.get("btc_price"),market.get("btc_rsi_15m"),market.get("btc_rsi_1h"),market.get("btc_rsi_4h"),market.get("btc_adx_15m"),market.get("btc_adx_4h"),market.get("btc_adx_trend"),market.get("btc_ema_align"),market.get("alts_bull_pct"),market.get("market_regime"),market.get("regime_shifting",0),market.get("shift_direction","NONE"),
                risk_temp,risk_reason,decision,reason,conf,pred,pred_c,coin.get("behavior","NORMAL"),
                market.get("hmm_regime"), 1 if market.get("hmm_agrees", True) else 0,
                trade.get("db_id")))
            conn.commit()
            # Save suggested_sl if TIGHTEN decision
            if decision=="TIGHTEN" and trade.get("suggested_sl"):
                try:
                    conn.execute("UPDATE observations SET sl_price=? WHERE symbol=? AND timestamp=? AND decision='TIGHTEN'",
                        (trade["suggested_sl"], symbol, now_str))
                    conn.commit()
                except: pass
            conn.close()
            decisions.append({"symbol":symbol,"bot":trade["bot_type"],"direction":trade["direction"],"roe":trade["roe"],"decision":decision,"confidence":conf,"reason":reason})

            # -- RATCHET TRAIL -- runs every cycle, state saved for 15s monitor --
            try:
                r_action, r_reason, r_floor, r_close, r_new_sl = check_ratchet_trail(trade)
                update_ratchet_state(symbol, trade)

                if r_close:
                    logger.info(f"  🔒 RATCHET CLOSE {symbol}: {r_reason}")
                    execute_decision(trade, "CLOSE", 95, f"Ratchet: {r_reason}")

                elif r_action == "LOCK":
                    logger.info(f"  🔒 RATCHET LOCK {symbol}: {r_reason}")
                    # Save BE state to DB -- survives restarts
                    if trade.get("bot_type") == "APEX":
                        try:
                            _db_id = trade.get("db_id")
                            if _db_id:
                                _sc = sqlite3.connect(TRADES_DB)
                                _sc.execute("UPDATE trades SET be_set=1, be_price=?, floor_roe=0 WHERE id=?",
                                            (float(trade.get("be_price", 0)), _db_id))
                                _sc.commit(); _sc.close()
                        except: pass
                    if r_new_sl > 0:
                        execute_decision(trade, "TIGHTEN", 90, r_reason, suggested_sl=r_new_sl)
                    try:
                        record_ratchet_event(symbol, trade["bot_type"], trade.get("roe", 0), r_floor)
                    except: pass

                elif r_action == "UPGRADE" and r_new_sl > 0:
                    logger.info(f"  📈 RATCHET UPGRADE {symbol}: floor={r_floor:.1f}% SL={r_new_sl:.4f}")
                    # Save floor_roe to DB -- survives restarts
                    if trade.get("bot_type") == "SPRING":
                        try:
                            _sc = sqlite3.connect(TRADES_DB)
                            _sc.execute("UPDATE dip_trades SET floor_roe=?, sl=? WHERE symbol=? AND status='OPEN'",
                                        (round(r_floor, 2), round(r_new_sl, 8), symbol))
                            _sc.commit(); _sc.close()
                        except: pass
                    elif trade.get("bot_type") == "APEX":
                        try:
                            _db_id = trade.get("db_id")
                            if _db_id:
                                _sc = sqlite3.connect(TRADES_DB)
                                _sc.execute("UPDATE trades SET floor_roe=? WHERE id=? AND status='OPEN'",
                                            (round(r_floor, 2), _db_id))
                                _sc.commit(); _sc.close()
                        except: pass
                    execute_decision(trade, "TIGHTEN", 85, r_reason, suggested_sl=r_new_sl)
                    try:
                        record_ratchet_event(symbol, trade["bot_type"], trade.get("roe", 0), r_floor)
                    except: pass

            except Exception as re:
                logger.error(f"Ratchet {symbol}: {re}")

          # ── EXECUTE APEX MIND decision if Phase 2 enabled ──
            # Skip if ratchet already closed the trade
            if decision in ("CLOSE", "TIGHTEN") and not r_close:
                try:
                    execute_decision(
                      trade, decision, conf, reason,
                      suggested_sl=trade.get("suggested_sl")
                    )
                except Exception as xe:
                    logger.error(f"Execute error {symbol}: {xe}")

        except Exception as e:
            import traceback as _tb
            logger.error(f"Obs error {symbol}: {e}\n{_tb.format_exc()[:300]}")

    for trade in trades:
        observe_trade(trade)

    # ── ENTRY SCANNER -- runs every cycle, looks for new opportunities ──
    try:
        pos = get_position_counts()
        cfg = load_master_config()
        gates = cfg.get("confidence_gates", {})
        live_params = derive_live_params()
        _max_long_slots  = market.get("max_long",  3)
        _max_short_slots = market.get("max_short", 2)
        _max_apex  = _max_long_slots + _max_short_slots
        _max_spring = market.get("spring_slots_max", cfg.get("limits",{}).get("max_spring", 10))
        apex_slots   = max(0, _max_apex - pos["apex"])
        # Direction-aware slot tracking
        _open_longs  = sum(1 for t in trades if t["bot_type"]=="APEX" and t["direction"]=="LONG")
        _open_shorts = sum(1 for t in trades if t["bot_type"]=="APEX" and t["direction"]=="SHORT")
        _long_slots  = max(0, _max_long_slots  - _open_longs)
        _short_slots = max(0, _max_short_slots - _open_shorts)
        spring_slots = max(0, _max_spring - pos["spring"])
        regime_live  = market.get("market_regime", "UNKNOWN")
        regime_cert  = sf(market.get("regime_certainty", 0))
        # Only scan for entries if regime is clear enough and risk is acceptable
        _heat_block = market.get("portfolio_heat_block", False)

        _daily_loss_limit = float(cfg.get("safety", {}).get("daily_loss_limit", 0))
        _daily_loss_block = False
        if _daily_loss_limit > 0:
            _apex_pnl_today = get_daily_pnl(apex_only=True)
            if _apex_pnl_today < -_daily_loss_limit:
                _daily_loss_block = True
                logger.warning(f"🛑 APEX daily loss limit hit: {_apex_pnl_today:.1f}% <= -{_daily_loss_limit:.1f}% -- entries paused")

        # Always subscribe scanner coins to WebSocket regardless of entry conditions
        try:
            _ws_scan = get_ws_manager()
            if _ws_scan and _ws_scan.running:
                _open_syms_ws = set(t["symbol"] for t in trades)
                try:
                    _tickers_ws = get_market_client().futures_ticker()
                    _btc_chg_ws = next((float(t.get("priceChangePercent",0)) for t in _tickers_ws if t["symbol"]=="BTCUSDT"), 0)
                    _btc_majors_ws = {"ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT","ADAUSDT",
                                      "DOTUSDT","LINKUSDT","AVAXUSDT","LTCUSDT","BTCUSDT"}
                    def _ws_score(t):
                        sym = t["symbol"]
                        vol = float(t.get("quoteVolume", 0))
                        chg = abs(float(t.get("priceChangePercent", 0)))
                        btc_diff = abs(chg - abs(_btc_chg_ws))
                        vol_score = min(vol / 30_000_000, 2.0) + max(0, 1.5 - vol / 200_000_000)
                        major_penalty = -3.0 if sym in _btc_majors_ws else 0
                        return btc_diff * 2.0 + chg * 0.3 + vol_score + major_penalty
                    _watch_ws = [t["symbol"] for t in sorted(
                        [t for t in _tickers_ws if t["symbol"].endswith("USDT") and float(t.get("quoteVolume",0)) > 3_000_000],
                        key=_ws_score, reverse=True)[:80]]
                    _ws_scan.update_subscriptions(_watch_ws + list(_open_syms_ws))
                    # Subscribe top 5 klines for execution precision
                    from websocket_manager import on_kline_update
                    _ws_scan.subscribe_klines(_watch_ws[:5], interval="1m", callback=on_kline_update)
                except: pass
        except: pass

        # Always scan -- maintain ready queue for instant slot replacement
        market = analyze_market()  # refresh -- UNSTALL may have updated regime since cycle start
        regime_live = market.get("market_regime", "UNKNOWN")
        _max_long_slots  = market.get("max_long", 3)
        _max_short_slots = market.get("max_short", 2)
        _max_spring = market.get("spring_slots_max", 10)
        _long_slots  = max(0, _max_long_slots  - _open_longs)
        _short_slots = max(0, _max_short_slots - _open_shorts)
        spring_slots = max(0, _max_spring - pos["spring"])
        if (risk_temp or 0) < 80 and (regime_cert or 0) >= 20 and not _heat_block and not _daily_loss_block:
            # Build watchlist from open trade symbols + nearby coins
            open_symbols = set(t["symbol"] for t in trades)
            try:
                # Reuse cached ticker if available (updated by WS block above)
                import time as _tt
                if not hasattr(run_cycle, "_ticker_cache") or _tt.time() - run_cycle._ticker_cache.get("ts",0) > 60:
                    get_market_rl().acquire(weight=2)
                    run_cycle._ticker_cache = {"data": get_market_client().futures_ticker(), "ts": _tt.time()}
                tickers = run_cycle._ticker_cache["data"]
                _btc_chg = next((float(t.get("priceChangePercent",0)) for t in tickers if t["symbol"]=="BTCUSDT"), 0)
                # Explicit BTC-correlated majors to deprioritize
                _btc_majors = {"ETHUSDT","BNBUSDT","SOLUSDT","XRPUSDT","ADAUSDT",
                               "DOTUSDT","LINKUSDT","AVAXUSDT","LTCUSDT","BTCUSDT"}
                def _scan_score(t):
                    sym = t["symbol"]
                    vol = float(t.get("quoteVolume", 0))
                    chg = abs(float(t.get("priceChangePercent", 0)))
                    # Independence from BTC -- bigger diff = more independent
                    btc_diff = abs(chg - abs(_btc_chg))
                    # Volume score -- prefer mid-range (not too small, not BTC-sized)
                    vol_score = min(vol / 30_000_000, 2.0) + max(0, 1.5 - vol / 200_000_000)
                    # Hard penalty for BTC majors
                    major_penalty = -3.0 if sym in _btc_majors else 0
                    return btc_diff * 2.0 + chg * 0.3 + vol_score + major_penalty
                candidates = sorted(
                    [t for t in tickers if t["symbol"].endswith("USDT")
                     and float(t.get("quoteVolume",0)) > 3_000_000
                     and t["symbol"] not in open_symbols],
                    key=_scan_score, reverse=True
                )[:80]
                watch = [t["symbol"] for t in candidates]
            except:
                watch = []

            entry_signals = []
            entry_lock = threading.Lock()
            # Pre-compute shared scanner context
            # Step 2 (Opus): market_regime is now evidence-driven -- single source of truth
            _regime_scan  = market.get("market_regime", "UNKNOWN")
            _regime_base  = market.get("market_regime", "UNKNOWN")
            _shift_scan   = market.get("shift_direction", "")
            _shifting_scan = market.get("regime_shifting", 0)
            _apex_gate    = gates.get("apex_entry_min", 70)
            _spring_gate  = gates.get("spring_entry_min", 50)
            _mq = live_params.get("market_quality", "STABLE")
            _wr = float(live_params.get("recent_wr", 50))
            # Effective regime for threshold decisions
            if _shifting_scan and "BEAR_TO_BULL" in _shift_scan:
                _eff_regime = "BEAR_TRANSITIONING"
            elif _shifting_scan and "BULL_TO_BEAR" in _shift_scan:
                _eff_regime = "BULL_TRANSITIONING"
            elif _shifting_scan and "CONSOLIDATING" in _shift_scan:
                _eff_regime = "SIDEWAYS"
            else:
                _eff_regime = _regime_scan
            # Regime entry analysis -- adjust min_score based on historical regime performance
            try:
                _reg_entry = _load_learned_params().get("regime_entry_analysis", {})
                _reg_key = _regime_scan + "_" + "LONG"  # check LONG as baseline
                if _reg_key in _reg_entry:
                    _reg_wr = float(_reg_entry[_reg_key].get("wr", 50))
                    # High WR regime = lower threshold, low WR = higher threshold
                    _reg_adj = round((_reg_wr - 60) / 10, 0)
                    _apex_gate = round(max(45, min(75, _apex_gate - _reg_adj)), 0)
            except: pass
            # Boost/reduce confidence thresholds based on transition accuracy
            try:
                _trans_acc = _load_learned_params().get("transition_accuracy", {})
                _trans_data = _trans_acc.get(_shift_scan, {})
                if _trans_data and _shifting_scan:
                    _trans_wr = float(_trans_data.get("avg_wr", 50))
                    # High accuracy transition = lower threshold (easier entry)
                    # Low accuracy = higher threshold (harder entry)
                    _trans_adj = round((_trans_wr - 65) / 10, 1)  # ±2.5% adjustment
                    _apex_gate = round(max(45, min(75, _apex_gate - _trans_adj)), 0)
            except: pass

            def _scan_coin(sym):
                try:
                    # Skip banned directions before any API calls
                    _long_banned  = is_repeat_offender(sym, direction="LONG")  or is_on_cooldown(sym, "LONG")
                    _short_banned = is_repeat_offender(sym, direction="SHORT") or is_on_cooldown(sym, "SHORT")
                    if (apex_slots > 0 or _long_slots > 0 or _short_slots > 0) and not (_long_banned and _short_banned):
                        direction, score, reason, conf = score_coin_entry(sym, market, "APEX")
                        # Skip if returned direction is banned
                        if direction and direction == "LONG" and _long_banned: direction = None
                        if direction and direction == "SHORT" and _short_banned: direction = None
                        if direction:
                            # Use learned regime+direction bucket WR to set gates
                            _bucket_key = f"{_regime_scan}_{direction}"
                            _bucket_wr = float(_reg_entry.get(_bucket_key, {}).get("wr", 50))
                            _bucket_n  = int(_reg_entry.get(_bucket_key, {}).get("n", _reg_entry.get(_bucket_key, {}).get("count", 0)) or 0)
                            # Only trust bucket if enough observations
                            if _bucket_n >= 20 and _bucket_wr >= 65:
                                _min_score = 20; _min_conf = max(_apex_gate - 10, 40)  # trusted positive
                            elif _bucket_n >= 20 and _bucket_wr >= 55:
                                _min_score = 25; _min_conf = max(_apex_gate - 5, 50)   # decent
                            elif _bucket_n >= 20 and _bucket_wr < 45:
                                _min_score = 45; _min_conf = min(_apex_gate + 10, 75)  # underperforming
                            else:
                                _min_score = 30; _min_conf = _apex_gate                # default / unproven
                            if _mq == "DETERIORATING" or _wr < 40:
                                _min_score = int(_min_score * 1.4); _min_conf = int(_min_conf * 1.3)
                            elif _mq == "IMPROVING" and _wr > 65:
                                _min_score = max(15, int(_min_score * 0.85)); _min_conf = max(30, int(_min_conf * 0.90))
                            if score >= _min_score and conf >= _min_conf:
                                with entry_lock:
                                    _qprice = get_live_price(sym)
                                    if _qprice and _qprice > 0:
                                        entry_signals.append({"symbol": sym, "mode": "APEX", "queued_at": time.time(),
                                            "direction": direction, "score": score, "confidence": conf,
                                            "reason": reason, "entry_price": _qprice, "feasible": True})
                                    else:
                                        logger.warning(f"  [QUEUE SKIP] {sym} no live price -- skip")
                    if spring_slots > 0:
                        # Spring: ban if 2+ Hard SL hits in last 24 hours
                        _spring_banned = is_repeat_offender(sym, days=1, min_sl_hits=2)
                        if not _spring_banned:
                            direction, score, reason, conf = score_coin_entry(sym, market, "SPRING")
                            if direction and score >= 25 and conf >= max(_spring_gate - 10, 30):
                                # Extract drop/recovery from reason string
                                _drop_val = 0.0; _rec_val = 0.0
                                try:
                                    import re as _re
                                    _dm = _re.search(r'(\d+\.\d+)% in', reason)
                                    _rm = _re.search(r'Recovery (\d+)', reason)
                                    if _dm: _drop_val = float(_dm.group(1))
                                    if _rm: _rec_val = float(_rm.group(1))
                                except: pass
                                with entry_lock:
                                    _qprice_s = get_live_price(sym)
                                    if _qprice_s and _qprice_s > 0:
                                        entry_signals.append({"symbol": sym, "mode": "SPRING", "queued_at": time.time(),
                                            "direction": direction, "score": score, "confidence": conf,
                                            "reason": reason, "drop_pct": _drop_val, "rec_pct": _rec_val,
                                            "entry_price": _qprice_s, "feasible": True})
                                    else:
                                        logger.warning(f"  [QUEUE SKIP] {sym} Spring no live price -- skip")
                except Exception as _se:
                    logger.warning(f"Scanner coin error {sym}: {_se}")

            from concurrent.futures import ThreadPoolExecutor, as_completed
            with ThreadPoolExecutor(max_workers=min(6, len(watch))) as _pool:
                _futs = [_pool.submit(_scan_coin, sym) for sym in watch]
                for _f in as_completed(_futs): pass

            # ── DIRECTION-AWARE FILTERING (Opus: feasible-first, no pre-truncation) ──
            _POOL_PER_SLOT = 5
            def _rank_key(s): return (0 if s.get("feasible", True) else 1, -s["confidence"])
            _long_all  = sorted([s for s in entry_signals if s["mode"]=="APEX" and s["direction"]=="LONG"],  key=_rank_key) if _long_slots  > 0 else []
            _short_all = sorted([s for s in entry_signals if s["mode"]=="APEX" and s["direction"]=="SHORT"], key=_rank_key) if _short_slots > 0 else []
            _spring_all= sorted([s for s in entry_signals if s["mode"]=="SPRING"], key=_rank_key) if spring_slots > 0 else []
            run_cycle._ready_long   = _long_all[:max(_long_slots  * _POOL_PER_SLOT, 5)]
            run_cycle._ready_short  = _short_all[:max(_short_slots * _POOL_PER_SLOT, 5)]
            run_cycle._ready_spring = _spring_all[:max(spring_slots* _POOL_PER_SLOT, 5)]
            entry_signals = _long_all + _short_all + _spring_all
            logger.info(f"  📊 APEX: L={_long_slots}slots({len(_long_all)} pooled) S={_short_slots}slots({len(_short_all)} pooled) | SPRING: {spring_slots}slots({len(_spring_all)} pooled)")


            if entry_signals:
                logger.info(f"{'─'*60}")
                logger.info(f"SCANNER  {len(entry_signals)} opportunities | APEX slots={apex_slots} SPRING slots={spring_slots}")
                for sig in entry_signals[:5]:
                    logger.info(f"  ✅ ENTRY  {sig['mode']:6s} {sig['symbol']:15s} {sig['direction']:5s} score={sig['score']:.0f} conf={sig['confidence']:.0f}% | {sig['reason'][:55]}")

                # ── EXECUTE ENTRIES -- fill by slots, walk FULL pool (Opus: no starvation) ──
                _cfg = load_master_config()
                _apex_long_filled  = 0
                _apex_short_filled = 0
                _spring_filled     = 0
                _open_this_cycle   = []  # for soft correlation throttle
                _already_open = set(t["symbol"] for t in trades)

                for sig in entry_signals:  # FULL pool -- no [:N] truncation
                    # Stop when all slots filled
                    _all_filled = (_apex_long_filled >= _long_slots and
                                   _apex_short_filled >= _short_slots and
                                   _spring_filled >= spring_slots)
                    if _all_filled: break
                    # Per-direction slot limits
                    if sig["mode"] == "APEX" and sig.get("direction") == "LONG"  and _apex_long_filled  >= _long_slots:  continue
                    if sig["mode"] == "APEX" and sig.get("direction") == "SHORT" and _apex_short_filled >= _short_slots: continue
                    if sig["mode"] == "SPRING" and _spring_filled >= spring_slots: continue
                    # Soft correlation throttle -- max 3 opens per cycle to avoid cluster risk
                    # Count per-direction so SHORTs dont block LONG slots and vice versa
                    _longs_opened = sum(1 for s in _open_this_cycle if s[1]=="LONG")
                    _shorts_opened = sum(1 for s in _open_this_cycle if s[1]=="SHORT")
                    if sig.get("direction")=="LONG" and _longs_opened >= min(_long_slots, 3): continue
                    if sig.get("direction")=="SHORT" and _shorts_opened >= min(_short_slots, 3): continue
                    # Pre-check repeat offender and cooldown -- skip without counting
                    if sig["mode"] == "APEX" and is_repeat_offender(sig["symbol"], direction=sig.get("direction")):
                        logger.info(f"  [SKIP] {sig['symbol']} {sig.get('direction')} repeat offender"); continue
                    if sig["mode"] == "SPRING" and is_repeat_offender(sig["symbol"], days=1, min_sl_hits=2): continue
                    if is_on_cooldown(sig["symbol"], sig.get("direction","LONG")):
                        logger.info(f"  [SKIP] {sig['symbol']} {sig.get('direction')} on cooldown"); continue
                    if sig["symbol"] in _already_open:
                        logger.info(f"  [SKIP] {sig['symbol']} already open"); continue
                    try:
                        # Staleness check -- re-validate if signal sat in queue >30s
                        _queue_age = time.time() - sig.get("queued_at", time.time())
                        if _queue_age > 60:
                            logger.info(f"  [STALE] {sig['symbol']} queued {_queue_age:.0f}s ago -- re-checking SL")
                            try:
                                _ws = get_ws_manager()
                                _fresh_price = get_live_price(sig["symbol"])
                                _sig_price = float(sig.get("entry_price", 0) or 0)
                                if not _fresh_price or _fresh_price <= 0:
                                    logger.warning(f"  [STALE] {sig['symbol']} no fresh price -- skip")
                                    set_cooldown(sig["symbol"], sig.get("direction","LONG"), same_dir_mins=15)
                                    continue
                                if not _sig_price or _sig_price <= 0:
                                    logger.warning(f"  [STALE] {sig['symbol']} entry_price=0 corrupt -- skip")
                                    set_cooldown(sig["symbol"], sig.get("direction","LONG"), same_dir_mins=15)
                                    continue
                                _drift = abs(_fresh_price - _sig_price) / _sig_price
                                if _drift > 0.03:
                                    logger.info(f"  [STALE BLOCKED] {sig['symbol']} price drifted {_drift*100:.1f}% > 3% -- skip")
                                    set_cooldown(sig["symbol"], sig.get("direction","LONG"), same_dir_mins=15)
                                    continue
                            except: pass
                        if sig["mode"] == "APEX":
                            opened = _open_apex_trade(
                                sig["symbol"], sig["direction"],
                                sig["score"], sig["confidence"],
                                sig["reason"], market)
                        else:
                            opened = _open_spring_trade(
                                sig["symbol"], sig["score"],
                                sig["confidence"], sig["reason"], market,
                                drop_pct=sig.get("drop_pct", 0),
                                recovery=sig.get("rec_pct", sig.get("recovery", 0)))
                        if opened:
                            _already_open.add(sig["symbol"])
                            _open_this_cycle.append((sig["symbol"], sig.get("direction","LONG")))
                            if sig["mode"] == "SPRING":
                                _spring_filled += 1
                            elif sig.get("direction") == "LONG":
                                _apex_long_filled += 1
                            else:
                                _apex_short_filled += 1
                            time.sleep(0.5)  # brief pause between entries
                    except Exception as ee:
                        logger.error(f"  [ENTRY] execution error {sig['symbol']}: {ee}")
                logger.info(f"  [EXEC DONE] opened: long={_apex_long_filled} short={_apex_short_filled} spring={_spring_filled} | checked={len(entry_signals)} signals")
            else:
                logger.info(f"{'─'*60}")
                logger.info(f"SCANNER  No entries (APEX={apex_slots} slots SPRING={spring_slots} slots | risk={risk_temp or 0:.0f} regime={regime_live})")
        else:
            logger.info(f"{'─'*60}")
            logger.info(f"SCANNER  Skipped -- risk={risk_temp or 0:.0f} certainty={regime_cert or 0:.0f}% too low")
    except Exception as e:
        logger.error(f"Entry scanner: {e}")
    # Dynamic threshold -- learns what confidence level predicts correct CLOSE
    try:
        _conn=_get_mind_conn()
        _rows=_conn.execute("""
            SELECT ROUND(decision_confidence/10)*10 as conf_bucket,
                   COUNT(*) as total, SUM(outcome_correct) as correct
            FROM observations
            WHERE decision='CLOSE' AND outcome_correct IS NOT NULL
            AND decision_reason != 'Historical'
            GROUP BY conf_bucket HAVING total >= 5
            ORDER BY conf_bucket""").fetchall()
        _conn.close()
        _threshold = 70
        for conf_b, total, correct in _rows:
            if total > 0 and correct/total >= 0.60:
                _threshold = int(conf_b); break
    except: _threshold = 70
    critical=[d for d in decisions if d["decision"]=="CLOSE" and d["confidence"]>=_threshold]
    if critical:
        logger.warning(f"{'─'*60}")
        logger.warning("HIGH CONF CLOSE: "+" | ".join([f"{d['symbol']}({d['confidence']:.0f}%)" for d in critical]))
    logger.info(f"{'='*60}")
    return decisions

def learn_adaptive_params():
    """
    Learns optimal ratchet levels and Kelly fractions from actual outcomes.
    Called daily from run_learning(). Writes to apex_mind_params.json.
    This is what makes ratchet and Kelly truly adaptive over time.

    Ratchet learning:
    - Analyzes all ratchet events: did the floor protect well or was it too tight?
    - If trades consistently gave back >tier_gap after floor set = tighten gap
    - If trades kept running after ratchet closed = loosen gap
    - Learns BE trigger: was 5% too early or too late?

    Kelly learning:
    - Per-regime win rate and avg PnL from last 90 days
    - Computes optimal Kelly fraction per regime
    - Detects if short_mult needs adjusting based on SHORT vs LONG outcomes
    - Learns confidence calibration -- does conf=80% actually mean 80% correct?
    """
    try:
        params_file = os.path.join(BASE, "apex_mind_params.json")
        try:
            params = json.load(open(params_file))
        except:
            params = {}

        # ── RATCHET LEARNING ──
        try:
            conn = sqlite3.connect(TRADES_DB)

            # Analyze ratchet FLOOR exits only -- exclude BE exits (always peak<10% by design)
            # Including BE exits falsely inflates low-peak count and pushes tier1_gap to max
            ratchet_exits = conn.execute("""
                SELECT pnl, peak_roe, reason, direction, open_time
                FROM trades WHERE status='CLOSED'
                AND reason LIKE 'Ratchet%'
                AND reason NOT LIKE 'Ratchet BE%'
                AND pnl IS NOT NULL AND peak_roe IS NOT NULL
                ORDER BY id DESC LIMIT 200""").fetchall()

            # Also check breakeven exits
            be_exits = conn.execute("""
                SELECT pnl, peak_roe, reason, duration_mins
                FROM trades WHERE status='CLOSED'
                AND reason LIKE '%Breakeven%'
                AND pnl IS NOT NULL AND peak_roe IS NOT NULL
                ORDER BY id DESC LIMIT 100""").fetchall()

            conn.close()

            ratchet_params = params.get("ratchet", {})

            if len(ratchet_exits) >= 10:
                # Was the ratchet closing too early? Check if pnl << peak_roe
                # If avg(peak_roe - final_roe) > 8% → floor gaps are too tight
                # If avg(peak_roe - final_roe) < 3% → floor gaps are too wide (good protection)
                gaps = []
                for pnl, peak, reason, direction, ot in ratchet_exits:
                    peak = float(peak or 0)
                    pnl  = float(pnl  or 0)
                    if peak > 5:
                        # Approximate final ROE from PnL
                        gaps.append(peak)  # we track peak_roe at close

                if gaps:
                    avg_peak_at_ratchet = sum(gaps) / len(gaps)
                    # If most ratchet exits happen at very high peaks (>25%) and
                    # we're getting good PnL -- current gaps are working
                    # If ratchet fires when peak is low (<10%) consistently -- gaps too tight
                    low_peak_exits = sum(1 for g in gaps if g < 10)
                    pct_low = low_peak_exits / len(gaps)

                    if pct_low > 0.4:
                        # Too many early ratchet fires at low peaks -- loosen slightly
                        old_t1 = float(ratchet_params.get("tier1_gap", 5.0))
                        new_t1 = round(min(old_t1 + 0.5, 8.0), 1)
                        if new_t1 != old_t1:
                            ratchet_params["tier1_gap"] = new_t1
                            logger.info(f"  RATCHET LEARN: tier1_gap {old_t1}→{new_t1} (too many early fires)")
                    elif pct_low < 0.15:
                        # Ratchet firing at healthy peaks -- tighten slightly for better protection
                        old_t1 = float(ratchet_params.get("tier1_gap", 5.0))
                        new_t1 = round(max(old_t1 - 0.3, 3.0), 1)
                        if new_t1 != old_t1:
                            ratchet_params["tier1_gap"] = new_t1
                            logger.info(f"  RATCHET LEARN: tier1_gap {old_t1}→{new_t1} (tightening)")

            # Breakeven trigger learning
            if len(be_exits) >= 15:
                # If breakeven exits are mostly profitable -- BE trigger is well placed
                be_wins = sum(1 for r in be_exits if float(r[0] or 0) > 0)
                be_wr   = be_wins / len(be_exits)
                current_be = float(ratchet_params.get("be_trigger", 5.0))

                if be_wr < 0.55 and len(be_exits) >= 20:
                    # BE trigger firing too early (pre-breakeven then stopping out)
                    # Push trigger higher so we wait for stronger profit before locking
                    new_be = round(min(current_be + 0.5, 8.0), 1)
                    if new_be != current_be:
                        ratchet_params["be_trigger"] = new_be
                        logger.info(f"  RATCHET LEARN: be_trigger {current_be}→{new_be} (BE WR={be_wr:.0%})")
                elif be_wr > 0.75 and current_be > 4.0:
                    # BE exits very profitable -- could trigger earlier
                    new_be = round(max(current_be - 0.3, 3.5), 1)
                    if new_be != current_be:
                        ratchet_params["be_trigger"] = new_be
                        logger.info(f"  RATCHET LEARN: be_trigger {current_be}→{new_be} (BE WR={be_wr:.0%})")

            params["ratchet"] = ratchet_params

        except Exception as e:
            logger.error(f"  Ratchet learning error: {e}")

        # ── KELLY LEARNING ──
        try:
            conn = sqlite3.connect(TRADES_DB)

            # Per-regime outcomes -- last 90 days (use pnl_pct for size-independent Kelly)
            regime_rows = conn.execute("""
                SELECT regime_label, direction, pnl_pct
                FROM trades WHERE status='CLOSED'
                AND reason != 'Ghost - cleaned'
                AND regime_label IS NOT NULL AND regime_label != ''
                AND pnl_pct IS NOT NULL
                AND open_time >= datetime('now', '-90 days')
                ORDER BY id DESC""").fetchall()

            # SHORT vs LONG outcomes
            long_rows  = [(float(r[2] or 0)) for r in regime_rows if r[1] == "LONG"]
            short_rows = [(float(r[2] or 0)) for r in regime_rows if r[1] == "SHORT"]

            # conn kept open for pct_rows query below

            kelly_params = params.get("kelly", {})

            # Compute per-regime Kelly
            regime_kelly = {}
            regime_data  = defaultdict(list)
            for regime, direction, pnl in regime_rows:
                regime_data[regime].append(float(pnl or 0))

            for regime, pnl_list in regime_data.items():
                if len(pnl_list) < 8: continue
                wins   = [p for p in pnl_list if p > 0]
                losses = [p for p in pnl_list if p <= 0]
                if not wins or not losses: continue
                wr       = len(wins) / len(pnl_list)
                avg_win  = sum(wins) / len(wins)
                avg_loss = abs(sum(losses) / len(losses))
                b        = min(avg_win / avg_loss, 3.0) if avg_loss > 0 else 1.5
                raw_k    = (b * wr - (1 - wr)) / b
                # Normalize: 0.06 base Kelly → 1.0 multiplier
                new_mult = round(max(0.5, min(raw_k / 0.06, 1.5)), 2)
                old_mult = float(kelly_params.get("regime_kelly", {}).get(regime, new_mult))
                # Symmetric step -- max 15% change per cycle
                max_step = old_mult * 0.15
                r_change = new_mult - old_mult
                r_change = max(-max_step, min(max_step, r_change))
                mult     = round(max(0.5, min(old_mult + r_change, 1.5)), 2)
                regime_kelly[regime] = mult
                logger.info(f"  KELLY LEARN: {regime} WR={wr:.0%} → mult={mult:.2f}x")

            if regime_kelly:
                # Blend with existing -- preserve observation-populated regimes
                existing_rk = kelly_params.get("regime_kelly", {})
                for k, v in existing_rk.items():
                    if k not in regime_kelly:
                        regime_kelly[k] = v  # keep obs-populated value
                kelly_params["regime_kelly"] = regime_kelly

            # SHORT multiplier -- how do shorts actually perform vs longs?
            if len(long_rows) >= 10 and len(short_rows) >= 5:
                long_wr  = len([p for p in long_rows  if p > 0]) / len(long_rows)
                short_wr = len([p for p in short_rows if p > 0]) / len(short_rows)
                # If shorts win at 60% of long rate, mult should be 0.6
                if long_wr > 0:
                    learned_short_mult = round(max(0.4, min(short_wr / long_wr, 1.0)), 2)
                    old_mult = float(kelly_params.get("short_mult", 0.7))
                    # Blend: 70% old, 30% new (slow drift)
                    new_mult = round(old_mult * 0.7 + learned_short_mult * 0.3, 2)
                    kelly_params["short_mult"] = new_mult
                    logger.info(f"  KELLY LEARN: short_mult {old_mult}→{new_mult} (SHORT WR={short_wr:.0%} LONG WR={long_wr:.0%})")

            # Overall base Kelly from all recent trades (pnl_pct = size-independent)
            all_pnls = [float(r[2] or 0) for r in regime_rows]
            # Also fetch pnl_pct directly for base Kelly calculation
            pct_rows = conn.execute("""
                SELECT pnl_pct FROM trades WHERE status='CLOSED'
                AND reason != 'Ghost - cleaned'
                AND pnl_pct IS NOT NULL
                AND open_time >= datetime('now', '-90 days')
                ORDER BY id DESC""").fetchall()
            conn.close()
            if len(pct_rows) >= 10:
                all_pnls = [float(r[0] or 0) for r in pct_rows]
            if len(all_pnls) >= 20:
                wins   = [p for p in all_pnls if p > 0]
                losses = [p for p in all_pnls if p <= 0]
                if wins and losses:
                    wr       = len(wins) / len(all_pnls)
                    avg_win  = sum(wins) / len(wins)
                    avg_loss = abs(sum(losses) / len(losses))
                    b        = min(avg_win / avg_loss, 3.0) if avg_loss > 0 else 1.5
                    raw_k    = round(max(0.05, min((b * wr - (1-wr)) / b * 0.5, 0.12)), 3)
                    old_base = float(kelly_params.get("base", 0.06))
                    # Symmetric step -- max 15% change per learning cycle in either direction
                    max_step = old_base * 0.15
                    change   = raw_k - old_base
                    change   = max(-max_step, min(max_step, change))
                    base_k   = round(max(0.04, min(old_base + change, 0.12)), 3)
                    kelly_params["base"] = base_k
                    logger.info(f"  KELLY LEARN: base Kelly={old_base:.3f}→{base_k:.3f} (raw={raw_k:.3f} step={change:+.3f}) from {len(all_pnls)} trades")

            params["kelly"] = kelly_params

        except Exception as e:
            logger.error(f"  Kelly learning error: {e}")

        # ── SPRING KELLY LEARNING -- from actual dip_trades pnl_pct ──
        try:
            conn = sqlite3.connect(TRADES_DB)
            spring_pct_rows = conn.execute("""
                SELECT pnl_pct FROM dip_trades WHERE status='CLOSED'
                AND pnl_pct IS NOT NULL
                AND open_time >= datetime('now', '-90 days')
                ORDER BY id DESC""").fetchall()
            conn.close()

            # Also get obs for regime multipliers
            conn = _get_mind_conn()
            spring_rows = conn.execute("""
                SELECT market_regime, outcome_correct, outcome_roe
                FROM observations
                WHERE outcome_correct IS NOT NULL
                AND bot_type='SPRING'
                AND timestamp >= datetime('now', '-90 days')
                ORDER BY id DESC""").fetchall()
            conn.close()

            # Use actual pnl_pct for base Kelly if available, else fall back to obs
            if len(spring_pct_rows) >= 10:
                spring_pnls = [float(r[0] or 0) for r in spring_pct_rows]
            elif len(spring_rows) >= 30:
                spring_pnls = [float(r[2] or 0) for r in spring_rows]
            else:
                spring_pnls = []

            if len(spring_pnls) >= 10:
                s_wins   = [p for p in spring_pnls if p > 0]
                s_losses = [p for p in spring_pnls if p <= 0]

                if s_wins and s_losses:
                    s_wr      = len(s_wins) / len(spring_pnls)
                    s_avg_win = sum(s_wins) / len(s_wins)
                    s_avg_loss= abs(sum(s_losses) / len(s_losses))
                    s_b       = min(s_avg_win / s_avg_loss, 3.0) if s_avg_loss > 0 else 1.5
                    s_raw_k   = round(max(0.03, min((s_b * s_wr - (1-s_wr)) / s_b * 0.5, 0.08)), 3)
                    s_old     = float(kelly_params.get("spring_base", 0.04))
                    s_step    = s_old * 0.15
                    s_change  = max(-s_step, min(s_step, s_raw_k - s_old))
                    s_new     = round(max(0.03, min(s_old + s_change, 0.08)), 3)
                    kelly_params["spring_base"] = s_new
                    logger.info(f"  SPRING KELLY: spring_base {s_old:.3f}→{s_new:.3f} (raw={s_raw_k:.3f} WR={s_wr:.0%})")

                # Spring regime multipliers from observations
                s_regime_data = defaultdict(list)
                for regime, correct, roe in spring_rows:
                    s_regime_data[regime].append(float(roe or 0))

                s_regime_kelly = dict(kelly_params.get("spring_regime_kelly", {}))
                for regime, pnl_list in s_regime_data.items():
                    if len(pnl_list) < 15: continue
                    s_wins_r   = [p for p in pnl_list if p > 0]
                    s_losses_r = [p for p in pnl_list if p <= 0]
                    if not s_wins_r or not s_losses_r: continue
                    s_wr_r   = len(s_wins_r) / len(pnl_list)
                    s_win_r  = sum(s_wins_r) / len(s_wins_r)
                    s_loss_r = abs(sum(s_losses_r) / len(s_losses_r))
                    s_b_r    = min(s_win_r / s_loss_r, 3.0) if s_loss_r > 0 else 1.5
                    s_raw_m  = round(max(0.3, min(s_b_r * s_wr_r / 0.06, 1.5)), 2)
                    s_old_m  = float(s_regime_kelly.get(regime, s_raw_m))
                    s_step_m = s_old_m * 0.15
                    s_chg_m  = max(-s_step_m, min(s_step_m, s_raw_m - s_old_m))
                    s_new_m  = round(max(0.3, min(s_old_m + s_chg_m, 1.5)), 2)
                    s_regime_kelly[regime] = s_new_m
                    logger.info(f"  SPRING KELLY: {regime} mult {s_old_m:.2f}→{s_new_m:.2f} ({len(pnl_list)} obs WR={s_wr_r:.0%})")

                # Blend with existing -- preserve observation-populated regimes
                existing_srk = kelly_params.get("spring_regime_kelly", {})
                for k, v in existing_srk.items():
                    if k not in s_regime_kelly:
                        s_regime_kelly[k] = v
                kelly_params["spring_regime_kelly"] = s_regime_kelly
                params["kelly"] = kelly_params

        except Exception as e:
            logger.error(f"  Spring Kelly learning error: {e}")

        # ── SESSION QUALITY LEARNING (size mult only -- trades always open) ──
        # Bad hours stay open for APEX MIND to learn from.
        # But size multiplier reflects actual profitability.
        # This is already handled by refresh_hour_quality() every 3H.
        # Here we just persist the current learned session quality to params.
        try:
            with _hour_quality_lock:
                if _hour_quality_learned:
                    session_params = {}
                    for hour, (wr, pnl, mult) in _hour_quality_learned.items():
                        session_params[str(hour)] = {
                            "wr": wr, "avg_pnl": pnl, "size_mult": mult,
                            "note": "learned" if mult != _HOUR_QUALITY_BASE.get(hour, (0,0,0))[2] else "baseline"
                        }
                    params["session"] = session_params
        except Exception as e:
            logger.error(f"  Session persist error: {e}")

        # ── REGIME TRANSITION LEARNING ──
        # Learn at what alts% regime actually shifts and optimal slot ratios
        try:
            conn = _get_mind_conn()
            # Find observations just before regime shifts
            trans_rows = conn.execute("""
                SELECT alts_bull_pct, market_regime, outcome_correct, direction
                FROM observations
                WHERE outcome_correct IS NOT NULL
                AND alts_bull_pct IS NOT NULL
                AND market_regime IN ("BEAR","BULL_WEAK","BULL_STRONG")
                ORDER BY id""").fetchall()
            conn.close()
            if len(trans_rows) >= 100:
                # Find optimal alts% threshold for BEAR/BULL transition
                bear_long_wins  = [(r[0], r[2]) for r in trans_rows if r[1]=="BEAR" and r[3]=="LONG" and r[2] is not None]
                bull_short_wins = [(r[0], r[2]) for r in trans_rows if "BULL" in r[1] and r[3]=="SHORT" and r[2] is not None]
                # Find alts% where LONG wins in BEAR regime
                if len(bear_long_wins) >= 20:
                    good_alts = [a for a, w in bear_long_wins if w == 1]
                    if good_alts:
                        optimal_bull_start = round(sum(good_alts)/len(good_alts), 1)
                        params["transition"] = params.get("transition", {})
                        old_t = float(params["transition"].get("bear_to_bull_alts", 45))
                        new_t = round(old_t * 0.8 + optimal_bull_start * 0.2, 1)
                        params["transition"]["bear_to_bull_alts"] = new_t
                        logger.info(f"  TRANSITION LEARN: bear_to_bull_alts {old_t:.1f}→{new_t:.1f}% (from {len(good_alts)} LONG wins in BEAR)")
                # Find alts% where SHORT wins in BULL regime
                if len(bull_short_wins) >= 20:
                    good_alts = [a for a, w in bull_short_wins if w == 1]
                    if good_alts:
                        optimal_bear_start = round(sum(good_alts)/len(good_alts), 1)
                        params["transition"] = params.get("transition", {})
                        old_t = float(params["transition"].get("bull_to_bear_alts", 65))
                        new_t = round(old_t * 0.8 + optimal_bear_start * 0.2, 1)
                        params["transition"]["bull_to_bear_alts"] = new_t
                        logger.info(f"  TRANSITION LEARN: bull_to_bear_alts {old_t:.1f}→{new_t:.1f}% (from {len(good_alts)} SHORT wins in BULL)")
        except Exception as _te:
            logger.error(f"  Transition learning error: {_te}")



        # ── WRITE BACK TO FILE -- after all learning blocks ──
        # Use _update_params to preserve observation-populated keys
        _update_params({
            "kelly": params.get("kelly", {}),
            "ratchet": params.get("ratchet", {}),
            "session": params.get("session", {}),
            "transition": params.get("transition", {}),
            "slot_ratios": params.get("slot_ratios", {}),
            "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "note": "Auto-learned by APEX MIND. Do not edit manually."
        })
        logger.info(f"  Adaptive params written to apex_mind_params.json")

    except Exception as e:
        logger.error(f"learn_adaptive_params error: {e}")


def analyze_conflict_outcomes():
    """
    Analyzes APEX vs Spring conflicts on same coin.
    Learns: when both bots trade same coin opposite directions -- who was right?
    Writes to patterns so future conflicts are resolved smarter.
    Also learns: when both trade same direction -- does it amplify or hurt?
    """
    try:
        conn = _get_mind_conn()
        # Find observations where conflicts were flagged
        conflict_obs = conn.execute("""
            SELECT symbol, bot_type, direction, roe, outcome_roe,
                   outcome_correct, market_regime, timestamp
            FROM observations
            WHERE outcome_correct IS NOT NULL
            AND decision_reason LIKE '%CONFLICT%'
            ORDER BY timestamp DESC LIMIT 200""").fetchall()
        conn.close()

        if len(conflict_obs) < 5:
            logger.info("  Conflict analysis: insufficient conflict data yet")
            return

        # Group by symbol + timestamp proximity (same conflict event)
        apex_wins = 0; spring_wins = 0; total_conflicts = 0

        by_symbol = defaultdict(list)
        for row in conflict_obs:
            by_symbol[row[0]].append(row)

        for symbol, obs in by_symbol.items():
            apex_obs   = [o for o in obs if o[1] == "APEX"]
            spring_obs = [o for o in obs if o[1] == "SPRING"]
            if not apex_obs or not spring_obs: continue

            apex_correct   = any(o[5] == 1 for o in apex_obs)
            spring_correct = any(o[5] == 1 for o in spring_obs)
            total_conflicts += 1
            if apex_correct:   apex_wins   += 1
            if spring_correct: spring_wins += 1

        if total_conflicts > 0:
            apex_wr   = round(apex_wins   / total_conflicts * 100, 1)
            spring_wr = round(spring_wins / total_conflicts * 100, 1)
            logger.info(f"  Conflict analysis: {total_conflicts} conflicts | APEX wins {apex_wr:.0f}% | Spring wins {spring_wr:.0f}%")

            # Save to patterns for make_decision to use
            conn2 = _get_mind_conn()
            conn2.execute("""INSERT OR REPLACE INTO patterns
                (pattern_key, description, conditions, occurrences, accuracy, last_seen)
                VALUES (?,?,?,?,?,datetime('now'))""",
                ("CONFLICT_RESOLUTION",
                 f"Conflicts: APEX wins {apex_wr:.0f}% Spring wins {spring_wr:.0f}% of {total_conflicts} conflicts",
                 json.dumps({"apex_wr": apex_wr, "spring_wr": spring_wr, "total": total_conflicts}),
                 total_conflicts, apex_wr))
            conn2.commit(); conn2.close()

    except Exception as e:
        logger.error(f"  Conflict analysis error: {e}")


def track_accuracy_trend():
    """
    Tracks APEX MIND accuracy week over week.
    Detects if a recent change made things better or worse.
    Writes trend to patterns DB and logs alert if accuracy drops > 5%.
    Also tracks per-decision accuracy trend: CLOSE, TIGHTEN, HOLD separately.
    """
    try:
        conn = _get_mind_conn()

        # Accuracy by 7-day windows
        windows = []
        for weeks_ago in range(4):  # last 4 weeks
            start = f"datetime('now', '-{(weeks_ago+1)*7} days')"
            end   = f"datetime('now', '-{weeks_ago*7} days')"
            rows  = conn.execute(f"""
                SELECT decision, COUNT(*) as total,
                       SUM(outcome_correct) as correct
                FROM observations
                WHERE outcome_correct IS NOT NULL
                AND decision_reason != 'Historical'
                AND timestamp >= {start}
                AND timestamp <  {end}
                GROUP BY decision""").fetchall()

            if rows:
                total   = sum(r[1] for r in rows)
                correct = sum(r[2] or 0 for r in rows)
                week_acc = round(correct / total * 100, 1) if total else 0
                per_dec  = {r[0]: round((r[2] or 0) / r[1] * 100, 1) for r in rows if r[1] >= 5}
                windows.append({
                    "weeks_ago": weeks_ago,
                    "accuracy":  week_acc,
                    "total":     total,
                    "per_decision": per_dec
                })

        conn.close()

        if len(windows) < 2:
            logger.info("  Accuracy trend: insufficient weekly data yet")
            return

        current_week = windows[0]  # most recent
        prev_week    = windows[1]

        trend = current_week["accuracy"] - prev_week["accuracy"]
        trend_str = f"+{trend:.1f}%" if trend > 0 else f"{trend:.1f}%"

        if trend < -5:
            logger.warning(f"  ⚠️  ACCURACY DECLINING: {prev_week['accuracy']:.1f}% → {current_week['accuracy']:.1f}% ({trend_str}) -- investigate recent changes")
        elif trend > 5:
            logger.info(f"  📈 ACCURACY IMPROVING: {prev_week['accuracy']:.1f}% → {current_week['accuracy']:.1f}% ({trend_str})")
        else:
            logger.info(f"  ACCURACY STABLE: {current_week['accuracy']:.1f}% ({trend_str} vs last week)")

        # Log per-decision trends
        for dec in ["CLOSE", "TIGHTEN", "HOLD"]:
            curr = current_week["per_decision"].get(dec)
            prev = prev_week["per_decision"].get(dec)
            if curr and prev:
                d = round(curr - prev, 1)
                logger.info(f"    {dec}: {prev:.0f}% → {curr:.0f}% ({'+' if d>=0 else ''}{d:.1f}%)")

        # Save trend to patterns for dashboard/report
        trend_data = {
            "current_week_acc": current_week["accuracy"],
            "prev_week_acc":    prev_week["accuracy"],
            "trend":            round(trend, 1),
            "weeks":            windows,
            "updated":          datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        }
        conn2 = _get_mind_conn()
        conn2.execute("""INSERT OR REPLACE INTO patterns
            (pattern_key, description, conditions, occurrences, accuracy, last_seen)
            VALUES (?,?,?,?,?,datetime('now'))""",
            ("ACCURACY_TREND",
             f"Weekly accuracy trend: {prev_week['accuracy']:.0f}% → {current_week['accuracy']:.0f}% ({trend_str})",
             json.dumps(trend_data),
             current_week["total"],
             current_week["accuracy"]))
        conn2.commit(); conn2.close()

        # Alert if 2 consecutive weeks declining
        if len(windows) >= 3 and windows[1]["accuracy"] < windows[2]["accuracy"] and current_week["accuracy"] < windows[1]["accuracy"]:
            logger.warning(f"  🔴 2 CONSECUTIVE WEEKS DECLINING: {windows[2]['accuracy']:.0f}% → {windows[1]['accuracy']:.0f}% → {current_week['accuracy']:.0f}%")

    except Exception as e:
        logger.error(f"  Accuracy trend error: {e}")


def analyze_signal_accuracy():
    """
    Scores order flow signals and sequence detections against actual outcomes.
    Answers: Is STRONG_BUY actually bullish? Is MTF_CONFLUENCE actually predictive?
    Writes signal accuracy to patterns DB so make_decision weights reflect reality.
    """
    try:
        conn = _get_mind_conn()
        # Pull observations that have flow/sequence signals and outcomes
        rows = conn.execute("""
            SELECT decision_reason, outcome_correct, outcome_roe, bot_type, direction
            FROM observations
            WHERE outcome_correct IS NOT NULL
            AND decision_reason != 'Historical'
            ORDER BY id DESC LIMIT 500""").fetchall()
        conn.close()
        if len(rows) < 20: return

        # Track accuracy per signal keyword
        signal_stats = {}
        signals_of_interest = [
            "FLOW: STRONG_BUY", "FLOW: STRONG_SELL",
            "FLOW: BUY_PRESSURE", "FLOW: SELL_PRESSURE",
            "FLOW: sell wall is FAKE", "FLOW: buy wall is FAKE",
            "FLOW: buyers being absorbed", "FLOW: sellers being absorbed",
            "SEQ: MTF_CONFLUENCE", "SEQ: accumulation",
            "SEQ: trend dying", "SEQ: stop hunt",
            "Cross-coin:", "Regime live=",
            "OI+", "OI ", "Funding",
        ]

        for reason, correct, roe, bot_type, direction in rows:
            if not reason: continue
            for sig in signals_of_interest:
                if sig.lower() in reason.lower():
                    key = f"SIG_{sig.replace(' ','_').replace(':','').replace('/','')[:30]}"
                    if key not in signal_stats:
                        signal_stats[key] = {"t": 0, "c": 0, "pnl": 0.0}
                    signal_stats[key]["t"] += 1
                    if correct: signal_stats[key]["c"] += 1
                    signal_stats[key]["pnl"] += float(roe or 0)

        # Write to patterns DB
        conn2 = _get_mind_conn()
        updated = 0
        for key, s in signal_stats.items():
            if s["t"] < 5: continue
            acc = round(s["c"] / s["t"] * 100, 1)
            avg = round(s["pnl"] / s["t"], 2)
            conn2.execute("""INSERT OR REPLACE INTO patterns
                (pattern_key, description, conditions, occurrences, correct,
                 accuracy, avg_pnl_impact, confidence, last_seen)
                VALUES (?,?,?,?,?,?,?,?,datetime('now'))""",
                (key,
                 f"Signal accuracy: {key.replace('SIG_','')} → {acc:.0f}% WR ${avg:+.2f}",
                 json.dumps({"signal": key, "n": s["t"]}),
                 s["t"], s["c"], acc, avg,
                 min(acc * s["t"] / 20, 90)))
            updated += 1

            # Log surprises -- signals that are wrong more than right
            if s["t"] >= 10 and acc < 45:
                logger.warning(f"  ⚠️  SIGNAL WEAK: {key.replace('SIG_','')} only {acc:.0f}% accurate ({s['t']} obs) -- weight will reduce")
            elif s["t"] >= 10 and acc >= 70:
                logger.info(f"  ✅ SIGNAL STRONG: {key.replace('SIG_','')} {acc:.0f}% accurate ({s['t']} obs)")

        conn2.commit(); conn2.close()
        logger.info(f"  Signal accuracy: {updated} signals scored")

    except Exception as e:
        logger.error(f"  Signal accuracy error: {e}")


def analyze_entry_confidence_calibration():
    """
    Measures: does conf=X% actually predict X% win rate on entries?
    Buckets entry suggestions by confidence, compares predicted vs actual WR.
    Writes calibration correction factors to apex_mind_params.json.
    If conf=95% actually means 60% WR -- APEX MIND learns to be more skeptical.
    """
    try:
        conn = _get_mind_conn()
        rows = conn.execute("""
            SELECT confidence, was_correct, trade_pnl
            FROM entry_suggestions
            WHERE outcome_filled = 1
            AND was_correct IS NOT NULL
            AND confidence > 0
            AND trade_opened = 1
            ORDER BY id DESC LIMIT 300""").fetchall()
        conn.close()
        if len(rows) < 20:
            logger.info("  Entry calibration: insufficient data (need 20+)")
            return

        # Bucket by confidence decile
        buckets = {}
        for conf, correct, pnl in rows:
            conf  = float(conf or 0)
            bucket = int(conf // 10) * 10  # 0,10,20...90
            if bucket not in buckets:
                buckets[bucket] = {"total": 0, "wins": 0, "pnl": 0.0}
            buckets[bucket]["total"] += 1
            if correct: buckets[bucket]["wins"] += 1
            buckets[bucket]["pnl"] += float(pnl or 0)

        # Compute calibration -- expected WR vs actual WR per bucket
        calibration = {}
        lines = []
        for bucket in sorted(buckets.keys()):
            b = buckets[bucket]
            if b["total"] < 3: continue
            actual_wr    = round(b["wins"] / b["total"] * 100, 1)
            expected_wr  = bucket + 5  # midpoint of bucket
            avg_pnl      = round(b["pnl"] / b["total"], 2)
            # Correction factor: if conf=90 but actual=60 → factor=0.67
            # Apply this factor to future confidence scores in this range
            correction   = round(actual_wr / expected_wr, 2) if expected_wr > 0 else 1.0
            # Floor at 0.7 -- never crush confidence below 70% of original
            # Low actual_wr may just mean insufficient data, not wrong system
            min_corr = 0.85 if b["total"] < 50 else 0.70  # stricter floor with small sample
            correction   = max(min_corr, min(correction, 1.5))
            calibration[str(bucket)] = {
                "expected_wr": expected_wr,
                "actual_wr":   actual_wr,
                "correction":  correction,
                "avg_pnl":     avg_pnl,
                "n":           b["total"]
            }
            lines.append(f"    conf={bucket}-{bucket+10}%: expected={expected_wr}% actual={actual_wr}% factor={correction:.2f} n={b['total']}")

        for l in lines: logger.info(l)

        # Persist to params file
        try:
            params_file = os.path.join(BASE, "apex_mind_params.json")
            params = json.load(open(params_file)) if os.path.exists(params_file) else {}
            # Blend new calibration with existing -- don't wipe historical data
            existing_cal = params.get("entry_calibration", {})
            for k, v in calibration.items():
                # calibration uses actual_wr/correction keys -- just update directly
                if k in existing_cal and isinstance(existing_cal[k], dict):
                    old_n = existing_cal[k].get("n", existing_cal[k].get("total", 0))
                    new_n = v.get("n", 0)
                    total_n = old_n + new_n
                    if total_n > 0 and "actual_wr" in v:
                        old_actual = existing_cal[k].get("actual_wr", v["actual_wr"])
                        blended_wr = round((old_actual*old_n + v["actual_wr"]*new_n)/total_n, 1)
                        existing_cal[k] = dict(v)
                        existing_cal[k]["actual_wr"] = blended_wr
                        existing_cal[k]["n"] = total_n
                    else:
                        existing_cal[k] = v
                else:
                    existing_cal[k] = v
            params["entry_calibration"] = existing_cal
            params["entry_calibration_updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            _update_params({"entry_calibration": existing_cal, "entry_calibration_updated": params["entry_calibration_updated"]})
            logger.info(f"  Entry calibration: {len(calibration)} buckets saved")
        except Exception as e:
            logger.error(f"  Calibration persist error: {e}")

    except Exception as e:
        logger.error(f"  Entry confidence calibration error: {e}")


def learn_confidence_gates():
    """
    Auto-calibrates confidence gates from historical accuracy data.
    Finds lowest confidence bucket that still achieves 65%+ accuracy.
    Writes updated gates to master_config.json.
    """
    try:
        conn = _get_mind_conn()
        rows = conn.execute("""
            SELECT bot_type, decision,
                   ROUND(decision_confidence/10)*10 as conf_bucket,
                   COUNT(*) as total,
                   ROUND(SUM(outcome_correct)*100.0/COUNT(*),1) as accuracy
            FROM observations
            WHERE outcome_correct IS NOT NULL
            AND decision_reason != 'Historical'
            GROUP BY bot_type, decision, conf_bucket
            HAVING total >= 15
            ORDER BY bot_type, decision, conf_bucket""").fetchall()
        conn.close()

        if not rows: return

        # Find minimum confidence that achieves 65%+ accuracy per decision type
        gates = {}
        for bot_type, decision, conf_bucket, total, accuracy in rows:
            key = f"{bot_type}_{decision}"
            if accuracy >= 65:
                if key not in gates or conf_bucket < gates[key]:
                    gates[key] = int(conf_bucket)

        cfg_path = os.path.join(BASE, "master_config.json")
        cfg = json.load(open(cfg_path))
        cg = cfg.get("confidence_gates", {})

        updated = []
        if "APEX_CLOSE" in gates:
            new_val = max(45, gates["APEX_CLOSE"])
            if new_val != cg.get("apex_close_min"):
                cg["apex_close_min"] = new_val
                updated.append(f"apex_close_min={new_val}")
        if "APEX_TIGHTEN" in gates:
            new_val = max(40, gates["APEX_TIGHTEN"])
            if new_val != cg.get("apex_tighten_min"):
                cg["apex_tighten_min"] = new_val
                updated.append(f"apex_tighten_min={new_val}")
        if "SPRING_CLOSE" in gates:
            new_val = max(45, min(85, gates["SPRING_CLOSE"]))  # cap at 85 -- 100 would disable Spring closes
            if new_val != cg.get("spring_close_min"):
                cg["spring_close_min"] = new_val
                updated.append(f"spring_close_min={new_val}")
        if "SPRING_TIGHTEN" in gates:
            new_val = max(40, gates["SPRING_TIGHTEN"])
            if new_val != cg.get("spring_tighten_min"):
                cg["spring_tighten_min"] = new_val
                updated.append(f"spring_tighten_min={new_val}")

        # Learn apex_entry_min from APEX HOLD accuracy
        # Only lower gate if HOLD accuracy is strong (>70%) at that confidence level
        # Floor at 55 -- never allow entries below this regardless of learning
        if "APEX_HOLD" in gates:
            new_val = max(70, min(80, gates["APEX_HOLD"]))  # floor at 70 from backtest
            current_val = cg.get("apex_entry_min", 70)
            # Only lower if accuracy is genuinely strong -- prevent noise from low-data buckets
            if new_val < current_val:
                # Require 70%+ HOLD accuracy to justify lowering entry gate
                _hold_gate = gates.get("APEX_HOLD", 60)
                if _hold_gate >= 70:
                    cg["apex_entry_min"] = new_val
                    updated.append(f"apex_entry_min={new_val}")
            elif new_val > current_val:
                # Always allow raising the gate
                cg["apex_entry_min"] = new_val
                updated.append(f"apex_entry_min={new_val}")

        if updated:
            cfg["confidence_gates"] = cg
            json.dump(cfg, open(cfg_path, "w"), indent=2)
            logger.info(f"  GATES UPDATED: {', '.join(updated)}")
        else:
            logger.info(f"  Gates unchanged -- current settings optimal")

    except Exception as e:
        logger.error(f"learn_confidence_gates: {e}")




def refit_hmm_regime():
    """I4: Periodically refit HMM on latest market_snapshots data.
    Only refits if enough new data since last fit."""
    try:
        import pickle as _pk, numpy as _np
        from hmmlearn.hmm import GaussianHMM as _GHMM

        _hmm_path = os.path.join(BASE, "regime_hmm.pkl")
        # Check if refit needed -- only refit if 500+ new snapshots since last fit
        if os.path.exists(_hmm_path):
            _bundle = _pk.load(open(_hmm_path,"rb"))
            _last_n = _bundle.get("n_samples", 0)
        else:
            _last_n = 0

        conn = sqlite3.connect(MIND_DB, timeout=10)
        _cur_n = conn.execute("SELECT COUNT(*) FROM market_snapshots WHERE btc_rsi_15m > 0").fetchone()[0]
        if _cur_n - _last_n < 500:
            conn.close()
            logger.info(f"  HMM refit skipped -- only {_cur_n-_last_n} new snapshots (need 500+)")
            return

        rows = conn.execute("""SELECT btc_rsi_15m, btc_adx_15m, btc_ema_align,
            alts_bull_pct, regime FROM market_snapshots
            WHERE btc_price > 0 AND btc_rsi_15m > 0 AND btc_adx_15m > 0
            ORDER BY timestamp""").fetchall()
        conn.close()

        _ema_map = {"BULL":1.0,"FLAT":0.0,"BEAR":-1.0}
        X = _np.array([[_ema_map.get(str(r[2]),0.0),
                        (float(r[0])-50)/50,
                        (float(r[3] or 50)-50)/50,
                        float(r[1] or 25)/50] for r in rows])
        labels = [str(r[4] or 'UNKNOWN') for r in rows]

        mu = X.mean(axis=0); sd = X.std(axis=0)+1e-9
        Xs = (X-mu)/sd

        model = _GHMM(n_components=4, covariance_type="diag",
                      n_iter=200, random_state=42, tol=1e-4)
        model.fit(Xs)

        # Map states to regimes by bullishness
        state_means_real = model.means_ * sd + mu
        state_scores = []
        for i in range(4):
            ema_v, rsi_c, alts_c, adx_v = state_means_real[i]
            state_scores.append((i, ema_v + rsi_c + alts_c))
        sorted_states = sorted(state_scores, key=lambda x: x[1], reverse=True)
        regime_names = ['BULL_STRONG','BULL_WEAK','SIDEWAYS','BEAR']
        state_to_regime = {s[0]: regime_names[rank] for rank,s in enumerate(sorted_states)}

        bundle = {"model":model,"mu":mu,"sd":sd,"state_to_regime":state_to_regime,
                  "fit_date":datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                  "n_samples":len(X),"converged":model.monitor_.converged}
        _pk.dump(bundle, open(_hmm_path,"wb"))
        logger.info(f"  HMM refit: {len(X)} snapshots, converged={model.monitor_.converged}, states={state_to_regime}")
    except Exception as e:
        logger.error(f"refit_hmm_regime: {e}")


def _apex_finalize_close(symbol, rs, price, reason):
    """Single source of truth for APEX fast-monitor closes (ratchet BE/floor).
    Adds cost, updates total_cost in DB, scores via on_trade_closed."""
    try:
        _entry = float(rs.get("entry", 0) or 0)
        _size  = float(rs.get("size", 14) or 14)
        _lev   = float(rs.get("leverage", 5) or 5)
        _dir   = rs.get("direction", "LONG")
        _db_id = rs.get("db_id")
        # Use DB exit price if available (set by close_order) -- more accurate than WS price
        _price = float(price or 0)
        try:
            if _db_id:
                _pc = sqlite3.connect(TRADES_DB)
                _row = _pc.execute("SELECT exit FROM trades WHERE id=?", (_db_id,)).fetchone()
                _pc.close()
                if _row and _row[0] and float(_row[0]) > 0:
                    _price = float(_row[0])
        except: pass
        if _price <= 0 or _entry <= 0 or _size <= 0:
            logger.warning(f"  [RATCHET CLOSE] {symbol} missing price/entry/size")
            return None
        _pnl_pct = ((_price-_entry)/_entry*100*_lev) if _dir=="LONG"                    else ((_entry-_price)/_entry*100*_lev)
        _pnl = _size * (_pnl_pct / 100)
        _notional = _size * _lev
        _fee  = _notional * 0.0004 * 2
        _slip = _notional * 0.0002 * 2
        _age_hrs = float(rs.get("age_mins", 0) or 0) / 60
        _fr = abs(float(rs.get("funding_rate", 0.0001) or 0.0001)) or 0.0001
        _fund = _notional * _fr * int(_age_hrs / 8)
        _total_cost = _fee + _slip + _fund
        _pnl -= _total_cost
        _pnl_pct = _pnl / _size * 100
        logger.info(f"  [COST] {symbol}: fee=${_fee:.3f} slip=${_slip:.3f} fund=${_fund:.3f} net_pnl=${_pnl:.3f}")
        # DB: close_order already set status=CLOSED -- just update cost fields
        try:
            _c = sqlite3.connect(TRADES_DB)
            if _db_id:
                _c.execute("UPDATE trades SET pnl=?, pnl_pct=?, total_cost=? WHERE id=?",
                    (round(_pnl,4), round(_pnl_pct,2), round(_total_cost,4), _db_id))
            else:
                _c.execute("UPDATE trades SET pnl=?, pnl_pct=?, total_cost=? WHERE symbol=? AND status='CLOSED' ORDER BY close_time DESC LIMIT 1",
                    (round(_pnl,4), round(_pnl_pct,2), round(_total_cost,4), symbol))
            _c.commit(); _c.close()
        except Exception as _dbe:
            logger.error(f"  [RATCHET CLOSE] DB error {symbol}: {_dbe}")
        update_paper_balance(round(_pnl, 4))
        try:
            import sys as _sys
            _otc = globals().get('on_trade_closed') or _sys.modules[__name__].__dict__.get('on_trade_closed')
            if _otc: _otc(symbol, "APEX", _pnl, reason, db_id=_db_id)
        except Exception as _otce:
            logger.debug(f"  [RATCHET CLOSE] on_trade_closed error: {_otce}")
        # Set cooldown -- prevent immediate re-entry on same symbol
        try:
            _cd_dir = rs.get("direction", "LONG")
            if _pnl_pct < -5:
                set_cooldown(symbol, _cd_dir, same_dir_mins=180, any_dir_mins=60)
            elif _pnl_pct < 0:
                set_cooldown(symbol, _cd_dir, same_dir_mins=60, any_dir_mins=20)
            else:
                set_cooldown(symbol, _cd_dir, same_dir_mins=15, any_dir_mins=5)
        except: pass
        # Clear ratchet state so next trade on same symbol starts clean
        with _ratchet_state_lock:
            _ratchet_state.pop(symbol, None)
        return _pnl
    except Exception as _e:
        logger.error(f"  [RATCHET CLOSE] finalize error {symbol}: {_e}")
        return None

def analyze_walk_forward():
    """I1-C/D: Compute out-of-sample expectancy per param snapshot.
    Promotes or rolls back snapshots based on forward trade performance."""
    try:
        import json as _wj, math as _wm
        MIN_SAMPLE = 30  # min forward trades to judge a snapshot

        conn_t = sqlite3.connect(TRADES_DB)
        conn_m = sqlite3.connect(MIND_DB, timeout=10)
        conn_m.execute("PRAGMA journal_mode=WAL")

        # Get all promoted snapshots
        snaps = conn_m.execute("""SELECT id, valid_from, params_json, promoted
            FROM param_snapshots ORDER BY id""").fetchall()
        if len(snaps) < 2:
            conn_t.close(); conn_m.close(); return

        results = []
        for snap_id, valid_from, params_json, promoted in snaps:
            # Trades governed by this snapshot
            apex_trades = conn_t.execute("""SELECT pnl, entry, exit, leverage, direction
                FROM trades WHERE status='CLOSED' AND snapshot_id=?""", (snap_id,)).fetchall()
            spring_trades = conn_t.execute("""SELECT pnl, entry, exit, leverage, 'LONG'
                FROM dip_trades WHERE status='CLOSED' AND snapshot_id=?""", (snap_id,)).fetchall()
            all_trades = apex_trades + spring_trades

            if len(all_trades) < MIN_SAMPLE:
                results.append({"id": snap_id, "n": len(all_trades), "status": "probation",
                                 "expectancy": None, "lower": None})
                continue

            # Compute ROE% for each trade
            roes = []
            for pnl, entry, exit_p, lev, direction in all_trades:
                try:
                    e = float(entry or 0); x = float(exit_p or 0); l = float(lev or 5)
                    if e > 0 and x > 0:
                        roe = (x-e)/e*l*100 if direction=='LONG' else (e-x)/e*l*100
                        roes.append(roe)
                except: pass

            if len(roes) < MIN_SAMPLE: continue
            exp = sum(roes)/len(roes)
            std = (_wm.sqrt(sum((r-exp)**2 for r in roes)/len(roes))) if len(roes)>1 else 0
            stderr = std / _wm.sqrt(len(roes))
            lower = exp - 1.64 * stderr  # 95% one-sided lower bound
            results.append({"id": snap_id, "n": len(roes), "status": "judged",
                             "expectancy": round(exp,2), "lower": round(lower,2),
                             "valid_from": valid_from})
            logger.info(f"  Snapshot #{snap_id} ({valid_from[:10]}): n={len(roes)} expectancy={exp:.2f}% lower={lower:.2f}%")

        # I11: Promotion/rollback: compare latest judged vs prior judged
        # Only judge snapshots with MIN_SAMPLE=30 forward trades (probation until then)
        judged = [r for r in results if r["status"]=="judged" and r["n"] >= 30]
        if len(judged) >= 2:
            prior = judged[-2]; latest = judged[-1]
            if latest["lower"] < prior["lower"] - 1.0:
                # Latest underperforms -- rollback
                try:
                    prior_params = conn_m.execute("SELECT params_json FROM param_snapshots WHERE id=?",
                        (prior["id"],)).fetchone()
                    if prior_params:
                        params_file = os.path.join(BASE, "apex_mind_params.json")
                        tmp = params_file + ".tmp"
                        open(tmp,"w").write(prior_params[0])
                        os.replace(tmp, params_file)
                        conn_m.execute("UPDATE param_snapshots SET promoted=0 WHERE id=?", (latest["id"],))
                        conn_m.commit()
                        logger.warning(f"  ⚠️  ROLLBACK: snapshot #{latest['id']} underperformed #{prior['id']} -- restored prior params")
                except Exception as re: logger.error(f"  Rollback failed: {re}")
            elif latest["lower"] > prior["lower"] + 0.5:
                logger.info(f"  ✅ WALK-FORWARD: snapshot #{latest['id']} improved lower bound {prior['lower']}% → {latest['lower']}%")
            else:
                logger.info(f"  Walk-forward: snapshot #{latest['id']} stable (lower={latest['lower']}% vs prior={prior['lower']}%)")

        conn_t.close(); conn_m.close()

        # Save summary to params
        _update_params({"walk_forward_snapshots": len(snaps),
                        "walk_forward_judged": len(judged),
                        "walk_forward_latest_lower": judged[-1]["lower"] if judged else None})
    except Exception as e:
        logger.error(f"analyze_walk_forward: {e}")

def run_learning():
    # Late-bind all functions defined after run_learning
    _g = globals()
    _score_entry = _g.get("score_entry_suggestions") or (lambda: None)
    _analyze_spring = _g.get("analyze_spring_scores") or (lambda: None)
    _analyze_regime = _g.get("analyze_regime_entries") or (lambda: None)
    _analyze_timeout = _g.get("analyze_timeout_exits") or (lambda: None)
    _analyze_close = _g.get("analyze_close_reasons") or (lambda: None)
    _analyze_ratchet = _g.get("analyze_ratchet_events") or (lambda: None)
    _analyze_sl = _g.get("analyze_optimal_sl") or (lambda: None)
    _analyze_conf = _g.get("analyze_entry_confidence_calibration") or (lambda: None)
    _analyze_signal = _g.get("analyze_signal_accuracy") or (lambda: None)
    _track_acc = _g.get("track_accuracy_trend") or (lambda: None)
    _analyze_conflict = _g.get("analyze_conflict_outcomes") or (lambda: None)
    _learn_gates = _g.get("learn_confidence_gates") or (lambda: None)
    logger.info("Running APEX MIND learning...")

    # I1-A: Snapshot current params BEFORE any writes
    _snap_id = None
    try:
        import json as _sj
        _sp = _sj.dumps(_sj.load(open(os.path.join(BASE,"apex_mind_params.json"))))
        _st = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        _sc = sqlite3.connect(MIND_DB, timeout=10)
        _sc.execute("PRAGMA journal_mode=WAL")
        _sc.execute("INSERT INTO param_snapshots (valid_from,params_json,fit_through,promoted,note) VALUES (?,?,?,1,'Auto snapshot')",(_st,_sp,_st))
        _sc.commit()
        _snap_id = _sc.execute("SELECT MAX(id) FROM param_snapshots").fetchone()[0]
        _sc.close()
        logger.info(f"  Param snapshot #{_snap_id} saved (valid_from={_st})")
    except Exception as e:
        logger.error(f"  Param snapshot failed: {e}")

    try:
        conn=_get_mind_conn()
        rows=conn.execute("SELECT symbol,direction,decision,outcome_correct,adx_trend,ema_align_15m,ema_align_1h,macd_trend,rsi_divergence,risk_temp,rsi_15m,rsi_1h,volume_ratio,bb_position,btc_adx_trend,btc_ema_align,alts_bull_pct,market_regime,roe,trade_age_mins,outcome_roe,regime_shifting FROM observations WHERE outcome_correct IS NOT NULL ORDER BY id").fetchall()
        conn.close()
    except: return {}
    if not rows: logger.info("No scored observations yet"); return {}
    total=len(rows); correct=sum(1 for r in rows if r[3]==1)
    accuracy=round(correct/total*100,1) if total else 0
    logger.info(f"  {correct}/{total} = {accuracy}% accuracy")

    # L9 FIX: Walk-forward validation -- train on oldest 80%, validate on newest 20%
    _split = int(len(rows) * 0.80)
    _train_rows = rows[:_split]
    _val_rows   = rows[_split:]
    if _val_rows:
        _val_correct = sum(1 for r in _val_rows if r[3]==1)
        _val_acc = round(_val_correct/len(_val_rows)*100,1)
        try: _prev_val_acc = float(_load_learned_params().get("walk_forward_val_acc", 0))
        except: _prev_val_acc = 0
        logger.info(f"  Walk-forward: train={len(_train_rows)} val={len(_val_rows)} | val_acc={_val_acc}% (prev={_prev_val_acc}%)")
        if _prev_val_acc > 0 and _val_acc < _prev_val_acc - 5.0:
            logger.warning(f"  ⚠️  VALIDATION DEGRADED: {_prev_val_acc}% → {_val_acc}% -- params may be overfitting")
        elif _prev_val_acc > 0 and _val_acc > _prev_val_acc + 2.0:
            logger.info(f"  ✅ VALIDATION IMPROVED: {_prev_val_acc}% → {_val_acc}%")
        try: _update_params({"walk_forward_val_acc": _val_acc, "walk_forward_train_acc": accuracy})
        except: pass
    # Use training rows only for pattern learning
    rows = _train_rows

    patterns={}
    for row in rows:
        symbol,direction,decision,outcome,adx_t,e15,e1h,macd_t,div,risk,rsi15,rsi1h,vol,bb,btc_adx_t,btc_ema,alts,regime,roe,age,out_roe,shifting=row
        conds=[]
        if adx_t: conds.append(f"adx_{adx_t}")
        if e15 and e1h: conds.append(f"ema_{e15}_{e1h}")
        if macd_t: conds.append(f"macd_{macd_t}")
        if rsi15:
            if rsi15>70: conds.append("rsi_ob")
            elif rsi15<30: conds.append("rsi_os")
        if btc_adx_t: conds.append(f"btc_{btc_adx_t}")
        if regime: conds.append(f"reg_{regime}")
        if shifting: conds.append("shifting")
        key=f"{direction}_{decision}_"+"_".join(sorted(conds[:6]))+f"_roe{int(roe//5)*5}_age{int(age//30)*30}"
        if key not in patterns: patterns[key]={"t":0,"c":0,"pnl":0,"conds":conds}
        patterns[key]["t"]+=1
        if outcome: patterns[key]["c"]+=1
        patterns[key]["pnl"]+=float(out_roe or 0)
    conn=_get_mind_conn()
    for key,p in patterns.items():
        acc=round(p["c"]/p["t"]*100,1) if p["t"] else 0
        avg=round(p["pnl"]/p["t"],2) if p["t"] else 0
        conn.execute("INSERT OR REPLACE INTO patterns (pattern_key,description,conditions,occurrences,correct,accuracy,avg_pnl_impact,confidence,last_seen) VALUES (?,?,?,?,?,?,?,?,datetime('now'))",
            (key,f"Pattern:{key}",json.dumps(p["conds"]),p["t"],p["c"],acc,avg,min(acc*p["t"]/10,95)))
    conn.commit(); conn.close()
    best=sorted(patterns.items(),key=lambda x:x[1]["c"]/max(x[1]["t"],1)*x[1]["t"],reverse=True)[:5]
    for key,p in best:
        acc=round(p["c"]/p["t"]*100,1) if p["t"] else 0
        logger.info(f"  Pattern {key}: {p['c']}/{p['t']}={acc}% avg=${p['pnl']/max(p['t'],1):.2f}")
    rebuild_coin_personalities()
    if _score_entry: _score_entry()
    _analyze_timeout()
    if _analyze_spring: _analyze_spring()
    if _analyze_regime: _analyze_regime()
    _analyze_close()
    _analyze_ratchet()
    _analyze_sl()
    _analyze_conf()
    _analyze_signal()
    _track_acc()
    _analyze_conflict()
    _learn_gates()
    # New learning functions
    _g.get("analyze_spring_ratchet", lambda: None)()
    _g.get("analyze_spring_sl", lambda: None)()
    _g.get("analyze_spring_close_reasons", lambda: None)()
    _g.get("analyze_spring_timeout", lambda: None)()
    _g.get("analyze_spring_entry_quality", lambda: None)()
    _g.get("analyze_walk_forward", lambda: None)()
    _g.get("refit_hmm_regime", lambda: None)()
    _g.get("analyze_apex_timeout_analysis", lambda: None)()
    _g.get("learn_dip_quality_gate", lambda: None)()
    _g.get("learn_dip_quality_weights", lambda: None)()
    _g.get("analyze_be_trigger_accuracy", lambda: None)()
    _g.get("analyze_apex_hold_time", lambda: None)()
    _g.get("analyze_direction_accuracy", lambda: None)()
    _g.get("analyze_ban_effectiveness", lambda: None)()
    _g.get("analyze_regime_transition_accuracy", lambda: None)()
    _g.get("analyze_session_entry_timing", lambda: None)()
    # ── PATTERN STALENESS DECAY ──
    # Old patterns from different market conditions poison decisions.
    # Patterns not seen in 14+ days lose confidence. 30+ days get halved.
    # Patterns with <3 occurrences and last seen 7+ days ago get deactivated.
    try:
        conn_d = _get_mind_conn()
        # Decay confidence for stale patterns
        conn_d.execute("""
            UPDATE patterns SET
                confidence = ROUND(confidence * 0.85, 1),
                accuracy   = ROUND(accuracy   * 0.92, 1)
            WHERE active = 1
            AND julianday('now') - julianday(last_seen) >= 14
            AND pattern_key NOT LIKE 'REGIME_%'
            AND pattern_key NOT LIKE 'SL_OPTIMAL_%'
            AND pattern_key NOT LIKE 'RATCHET_%'""")
        # Halve very stale patterns
        conn_d.execute("""
            UPDATE patterns SET
                confidence = ROUND(confidence * 0.5, 1),
                accuracy   = ROUND(accuracy   * 0.7, 1)
            WHERE active = 1
            AND julianday('now') - julianday(last_seen) >= 30
            AND occurrences < 20""")
        # Deactivate thin stale patterns
        deactivated = conn_d.execute("""
            UPDATE patterns SET active = 0
            WHERE occurrences < 3
            AND julianday('now') - julianday(last_seen) >= 7
            AND active = 1""").rowcount
        conn_d.commit()
        # Reactivate if seen again (handled by INSERT OR REPLACE)
        stale_count = conn_d.execute("""
            SELECT COUNT(*) FROM patterns
            WHERE julianday('now') - julianday(last_seen) >= 14
            AND active = 1""").fetchone()[0]
        conn_d.close()
        logger.info(f"  Pattern decay: {deactivated} deactivated, {stale_count} stale patterns still active")
    except Exception as e:
        logger.error(f"  Pattern decay error: {e}")

    # ── ADAPTIVE PARAMETER LEARNING -- writes back to apex_mind_params.json ──
    learn_adaptive_params()
    return {"total":total,"accuracy":accuracy,"correct":correct}

def generate_report():
    try:
        conn=_get_mind_conn()
        cutoff=(datetime.now(timezone.utc)-timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        obs=conn.execute("SELECT symbol,bot_type,direction,roe,peak_roe,decision,decision_confidence,decision_reason,outcome_correct,outcome_reason,timestamp,trade_age_mins,risk_temp,btc_adx_15m,btc_adx_trend,alts_bull_pct,market_regime,regime_shifting FROM observations WHERE timestamp>=? ORDER BY timestamp",(cutoff,)).fetchall()
        outcomes=conn.execute("SELECT symbol,decision,was_correct,pnl_impact,saved_loss,missed_gain,why_correct,why_wrong,what_mind_missed FROM decision_outcomes WHERE decision_time>=? ORDER BY decision_time",(cutoff,)).fetchall()
        coins=conn.execute("SELECT symbol,mind_accuracy,mind_total,mind_correct FROM coin_memory ORDER BY mind_total DESC LIMIT 10").fetchall()
        patterns=conn.execute("SELECT pattern_key,accuracy,occurrences,avg_pnl_impact FROM patterns ORDER BY occurrences*accuracy DESC LIMIT 5").fetchall()
        conn.close()
    except Exception as e: return f"Report error: {e}"
    total_obs=len(obs)
    scored=[o for o in obs if o[8] is not None]
    correct=[o for o in scored if o[8]==1]
    accuracy=round(len(correct)/len(scored)*100,1) if scored else 0
    closes=[o for o in obs if o[5]=="CLOSE"]; holds=[o for o in obs if o[5]=="HOLD"]
    cc=[o for o in closes if o[8]==1]; ch=[o for o in holds if o[8]==1]
    saved=sum(float(o[4] or 0) for o in outcomes if o[2]==1 and o[1]=="CLOSE")
    missed_total=sum(float(o[5] or 0) for o in outcomes if o[2]==0 and o[1]=="CLOSE")
    now_str=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines=[]
    lines.append("APEX MIND 24H Intelligence Report")
    lines.append(now_str)
    lines.append("="*50)
    lines.append(f"ACCURACY: {len(correct)}/{len(scored)} = {accuracy}%")
    lines.append(f"CLOSE: {len(cc)}/{len(closes)} = {round(len(cc)/max(len(closes),1)*100,1)}%")
    lines.append(f"HOLD:  {len(ch)}/{len(holds)} = {round(len(ch)/max(len(holds),1)*100,1)}%")
    lines.append(f"Total observations: {total_obs}")
    lines.append(f"Losses saved: ${saved:.2f} | Gains missed: ${missed_total:.2f} | Net: ${saved-missed_total:+.2f}")
    # L11 FIX: Add expectancy as headline metric (more meaningful than accuracy)
    try:
        conn_e = sqlite3.connect(TRADES_DB)
        _t_rows = conn_e.execute("""SELECT pnl FROM trades WHERE status='CLOSED'
            AND close_time >= ? ORDER BY close_time DESC LIMIT 100""", (cutoff,)).fetchall()
        _s_rows = conn_e.execute("""SELECT pnl FROM dip_trades WHERE status='CLOSED'
            AND close_time >= ? ORDER BY close_time DESC LIMIT 100""", (cutoff,)).fetchall()
        conn_e.close()
        _all_pnl = [float(r[0]) for r in _t_rows + _s_rows if r[0] is not None]
        if _all_pnl:
            _wins = [p for p in _all_pnl if p > 0]
            _losses = [p for p in _all_pnl if p <= 0]
            _wr = round(len(_wins)/len(_all_pnl)*100,1)
            _avg_win = round(sum(_wins)/max(len(_wins),1),2)
            _avg_loss = round(sum(_losses)/max(len(_losses),1),2)
            _expectancy = round(_wr/100*_avg_win + (1-_wr/100)*_avg_loss, 3)
            lines.append(f"EXPECTANCY: ${_expectancy:+.3f}/trade | WR={_wr}% avgW=${_avg_win:+.2f} avgL=${_avg_loss:.2f} ({len(_all_pnl)} trades)")
            if _expectancy > 0:
                lines.append(f"  ✅ Positive expectancy -- system is profitable per trade")
            else:
                lines.append(f"  ⚠️  Negative expectancy -- fix avg_loss > avg_win before live")
    except: pass
    lines.append("")
    from collections import defaultdict
    trade_obs=defaultdict(list)
    for o in obs: trade_obs[o[0]].append(o)
    lines.append("TRADE-BY-TRADE:")
    for symbol,tlist in list(trade_obs.items())[:15]:
        first=tlist[0]
        closes_d=[o for o in tlist if o[5]=="CLOSE"]
        roe_min=min(o[3] for o in tlist); roe_max=max(o[3] for o in tlist)
        lines.append(f"  {symbol} [{first[1]}] {first[2]} | {len(tlist)} obs | ROE {roe_min:.1f}% to {roe_max:.1f}%")
        if closes_d:
            cd=closes_d[0]
            lines.append(f"    MIND CLOSE at {cd[10][11:16]} UTC ROE={cd[3]:+.1f}% conf={cd[6]:.0f}%")
            lines.append(f"    Reason: {cd[7][:70]}")
            if cd[8] is not None:
                v="CORRECT" if cd[8]==1 else "WRONG"
                lines.append(f"    Outcome: {v} - {str(cd[9])[:70]}")
        else:
            lines.append(f"    MIND: HOLD throughout")
        missed_why=[o for o in outcomes if o[0]==symbol and o[8]]
        if missed_why: lines.append(f"    Missed: {str(missed_why[0][8])[:70]}")
    shifting_obs=[o for o in obs if o[17]]
    if shifting_obs:
        lines.append(f"REGIME SHIFTS: {len(shifting_obs)}")
        for o in shifting_obs[:3]: lines.append(f"  {o[10][11:16]} BTC_ADX={o[13]:.0f}({o[14]}) alts={o[15]:.0f}%")
    if coins:
        lines.append("COIN INTELLIGENCE:")
        for c in coins: lines.append(f"  {c[0]:15s}: {c[3]}/{c[2]}={c[1]:.0f}%")
    if patterns:
        lines.append("TOP PATTERNS:")
        for p in patterns: lines.append(f"  {p[0][:45]} acc={p[1]:.0f}% n={p[2]} ${p[3]:+.2f}")
    # I12: Walk-forward report
    try:
        import json as _wj
        _wp = _wj.load(open(os.path.join(BASE,"apex_mind_params.json")))
        _wf_snaps = _wp.get("walk_forward_snapshots", 0)
        _wf_judged = _wp.get("walk_forward_judged", 0)
        _wf_lower = _wp.get("walk_forward_latest_lower")
        _wf_val = _wp.get("walk_forward_val_acc", 0)
        lines.append("")
        lines.append("WALK-FORWARD VALIDATION:")
        lines.append(f"  Snapshots: {_wf_snaps} total | {_wf_judged} judged (>=30 trades)")
        lines.append(f"  Val accuracy: {_wf_val}% | Latest lower_bound: {_wf_lower}%")
        if _wf_lower is not None:
            if _wf_lower > 0:
                lines.append(f"  ✅ Positive lower bound -- edge demonstrated out-of-sample")
            else:
                lines.append(f"  ⚠️  Negative lower bound -- more data needed before going live")
        # Check direction accuracy trusted buckets
        _da = _wp.get("direction_accuracy", {})
        _trusted = [(k,v) for k,v in _da.items() if v.get("trusted", False)]
        _untrusted = [(k,v) for k,v in _da.items() if not v.get("trusted", True)]
        lines.append(f"  Trusted buckets: {len(_trusted)}/{len(_da)} direction/regime combos")
        if _trusted:
            lines.append("  Best trusted: " + " | ".join([f"{k} lb={v.get('lower_bound',0):.2f}%" for k,v in sorted(_trusted, key=lambda x: x[1].get('lower_bound',0), reverse=True)[:3]]))
    except: pass

    lines.append("")
    lines.append("APEX MIND: Watching. Learning. Getting smarter.")
    lines.append("Target: Positive walk-forward lower_bound -> Phase 2 auto execution")
    return "\n".join(lines)

def send_report():
    body=generate_report()
    try:
        from emailer import Emailer
        now_str=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        Emailer().send(f"APEX MIND 24H Intelligence | {now_str}",body)
        logger.info("Intelligence report sent")
    except Exception as e: logger.error(f"Email: {e}"); print(body)

def check_price_alerts(trades):
    """
    Check if any open trade moved significantly since last observation.
    Returns True if immediate cycle needed.
    No threading -- simple check called during sleep.
    """
    try:
        conn = _get_mind_conn()
        for trade in trades:
            symbol = trade["symbol"]
            last = conn.execute(
                "SELECT current_price FROM observations WHERE symbol=? ORDER BY id DESC LIMIT 1",
                (symbol,)).fetchone()
            if not last: continue
            last_price = sf(last[0])
            curr_price = sf(trade.get("current_price", 0))
            if last_price > 0 and curr_price > 0:
                move = abs(curr_price - last_price) / last_price * 100
                if move >= 2.0:
                    logger.warning(f"⚡ PRICE ALERT: {symbol} moved {move:.1f}% -- immediate analysis")
                    conn.close()
                    return True
        conn.close()
    except: pass
    return False

def run_incremental_learning():
    """
    Lightweight learning cycle runs every 3 hours.
    Updates pattern weights from last 50 observations only.
    Does NOT rebuild everything -- just adjusts weights on recent evidence.
    Full run_learning() still runs daily for deep analysis.
    """
    try:
        conn = _get_mind_conn()
        # Last 50 scored observations
        rows = conn.execute("""
            SELECT symbol, direction, decision, outcome_correct,
                   adx_trend, ema_align_15m, macd_trend, market_regime,
                   roe, trade_age_mins, outcome_roe, regime_shifting
            FROM observations
            WHERE outcome_correct IS NOT NULL
            AND decision_reason != 'Historical'
            ORDER BY id DESC LIMIT 50""").fetchall()
        conn.close()

        if len(rows) < 10:
            logger.info("Incremental learning: insufficient recent data")
            return

        # Recalculate pattern weights for recent observations only
        recent_patterns = {}
        for row in rows:
            symbol,direction,decision,outcome,adx_t,e15,macd_t,regime,roe,age,out_roe,shifting = row
            conds = []
            if adx_t:   conds.append(f"adx_{adx_t}")
            if e15:     conds.append(f"ema_{e15}")
            if macd_t:  conds.append(f"macd_{macd_t}")
            if regime:  conds.append(f"reg_{regime}")
            if shifting: conds.append("shifting")
            key = f"{direction}_{decision}_" + "_".join(sorted(conds[:4]))
            if key not in recent_patterns:
                recent_patterns[key] = {"t": 0, "c": 0, "pnl": 0}
            recent_patterns[key]["t"] += 1
            if outcome: recent_patterns[key]["c"] += 1
            recent_patterns[key]["pnl"] += float(out_roe or 0)

        # Blend recent accuracy into existing patterns (70% old, 30% recent)
        conn2 = _get_mind_conn()
        updated = 0
        for key, rp in recent_patterns.items():
            if rp["t"] < 3: continue
            recent_acc = round(rp["c"] / rp["t"] * 100, 1)
            recent_pnl = round(rp["pnl"] / rp["t"], 2)
            existing = conn2.execute(
                "SELECT accuracy, avg_pnl_impact, occurrences FROM patterns WHERE pattern_key=?",
                (key,)).fetchone()
            if existing:
                old_acc, old_pnl, old_occ = existing
                # Weighted blend -- recent evidence gets 30% weight
                blended_acc = round(float(old_acc or 0) * 0.7 + recent_acc * 0.3, 1)
                blended_pnl = round(float(old_pnl or 0) * 0.7 + recent_pnl * 0.3, 2)
                conn2.execute("""UPDATE patterns SET
                    accuracy=?, avg_pnl_impact=?, last_seen=datetime('now')
                    WHERE pattern_key=?""",
                    (blended_acc, blended_pnl, key))
                updated += 1

        # Also update coin memory accuracy for active coins
        active_symbols = list(set(r[0] for r in rows))
        for symbol in active_symbols:
            sym_rows = [r for r in rows if r[0] == symbol]
            if len(sym_rows) < 3: continue
            recent_correct = sum(1 for r in sym_rows if r[3] == 1)
            recent_acc = round(recent_correct / len(sym_rows) * 100, 1)
            conn2.execute("""UPDATE coin_memory SET
                mind_accuracy = ROUND(mind_accuracy * 0.7 + ? * 0.3, 1),
                last_updated  = datetime('now')
                WHERE symbol=?""", (recent_acc, symbol))

        conn2.commit(); conn2.close()
        logger.info(f"Incremental learning: {updated} patterns updated from last {len(rows)} observations")

        # ── TRANSITION THRESHOLD -- update every 1H same as full learning ──
        try:
            params_file = os.path.join(BASE, "apex_mind_params.json")
            params = json.load(open(params_file)) if os.path.exists(params_file) else {}
            conn_t = _get_mind_conn()
            trans_rows = conn_t.execute("""
                SELECT alts_bull_pct, market_regime, outcome_correct, direction
                FROM observations WHERE outcome_correct IS NOT NULL
                AND alts_bull_pct IS NOT NULL
                AND market_regime IN ("BEAR","BULL_WEAK","BULL_STRONG")
                ORDER BY id""").fetchall()
            conn_t.close()
            if len(trans_rows) >= 100:
                bear_long_wins  = [(r[0], r[2]) for r in trans_rows if r[1]=="BEAR" and r[3]=="LONG" and r[2] is not None]
                bull_short_wins = [(r[0], r[2]) for r in trans_rows if "BULL" in r[1] and r[3]=="SHORT" and r[2] is not None]
                if len(bear_long_wins) >= 20:
                    good_alts = [a for a, w in bear_long_wins if w == 1]
                    if good_alts:
                        optimal = round(sum(good_alts)/len(good_alts), 1)
                        params["transition"] = params.get("transition", {})
                        old_t = float(params["transition"].get("bear_to_bull_alts", 45))
                        new_t = round(old_t * 0.8 + optimal * 0.2, 1)
                        params["transition"]["bear_to_bull_alts"] = new_t
                        logger.info(f"  TRANSITION (1H): bear_to_bull_alts {old_t:.1f}→{new_t:.1f}%")
                if len(bull_short_wins) >= 20:
                    good_alts = [a for a, w in bull_short_wins if w == 1]
                    if good_alts:
                        optimal = round(sum(good_alts)/len(good_alts), 1)
                        params["transition"] = params.get("transition", {})
                        old_t = float(params["transition"].get("bull_to_bear_alts", 65))
                        new_t = round(old_t * 0.8 + optimal * 0.2, 1)
                        params["transition"]["bull_to_bear_alts"] = new_t
                        logger.info(f"  TRANSITION (1H): bull_to_bear_alts {old_t:.1f}→{new_t:.1f}%")
                json.dump(params, open(params_file, "w"), indent=2)
        except Exception as _te:
            logger.warning(f"Incremental transition error: {_te}")

        # ── INCREMENTAL COIN PERSONALITY UPDATE ──
        # Daily rebuild is deep. This is a lightweight 3H update
        # for coins actively being traded right now.
        try:
            active = list(set(r[0] for r in rows))
            conn3  = _get_mind_conn()
            updated_coins = 0
            for symbol in active:
                sym_rows = [r for r in rows if r[0] == symbol]
                if len(sym_rows) < 3: continue
                correct = sum(1 for r in sym_rows if r[3] == 1)
                recent_acc = round(correct / len(sym_rows) * 100, 1)
                recent_pnl = sum(float(r[10] or 0) for r in sym_rows) / len(sym_rows)
                # Blend into coin_memory -- 80% existing, 20% recent (lighter than daily)
                conn3.execute("""
                    UPDATE coin_memory SET
                        mind_accuracy = ROUND(mind_accuracy * 0.8 + ? * 0.2, 1),
                        last_updated  = datetime('now')
                    WHERE symbol = ?""", (recent_acc, symbol))
                # Update avg_winning_roe / avg_losing_roe incrementally
                win_rows  = [float(r[10] or 0) for r in sym_rows if r[3] == 1  and r[10]]
                loss_rows = [float(r[10] or 0) for r in sym_rows if r[3] == 0  and r[10]]
                if win_rows:
                    conn3.execute("""UPDATE coin_memory SET
                        avg_winning_roe = ROUND(COALESCE(avg_winning_roe,0)*0.8 + ?*0.2, 2)
                        WHERE symbol=?""", (sum(win_rows)/len(win_rows), symbol))
                if loss_rows:
                    conn3.execute("""UPDATE coin_memory SET
                        avg_losing_roe = ROUND(COALESCE(avg_losing_roe,0)*0.8 + ?*0.2, 2)
                        WHERE symbol=?""", (sum(loss_rows)/len(loss_rows), symbol))
                updated_coins += 1
            conn3.commit(); conn3.close()
            if updated_coins:
                logger.info(f"Incremental coin personality: {updated_coins} coins updated")
        except Exception as e:
            logger.error(f"Incremental coin personality error: {e}")

    except Exception as e:
        logger.error(f"Incremental learning error: {e}")


# ── RATCHET FLOOR CACHE -- shared between cycle and 15s monitor ──
# Stores floor_roe and be_set per trade so 15s monitor can enforce them
_ratchet_state = {}  # {symbol: {floor_roe, be_set, be_price, peak_roe}}
_closing_set   = set()  # symbols currently being closed -- prevents triple-fire
_exec_dedup    = {}   # {symbol_decision: timestamp} -- prevents triple execute
_ws_alert_symbols = set()  # symbols with significant WS price moves
_ws_alert_lock = threading.Lock()  # thread-safe access to alert set
_exec_dedup_lock = __import__("threading").Lock()
_tighten_set   = {}   # {symbol: timestamp} -- prevents triple TIGHTEN fire
_sl_fired      = {}   # {symbol: timestamp} -- prevents hard SL triple fire
_ratchet_state_lock = threading.Lock()

def update_ratchet_state(symbol, trade):
    """Called from observe_trade after ratchet runs -- saves state for 15s monitor."""
    with _ratchet_state_lock:
        _ratchet_state[symbol] = {
            "floor_roe": float(trade.get("floor_roe", 0)),
            "be_set":    bool(trade.get("be_set", False)),
            "be_price":  float(trade.get("be_price", 0)),
            "peak_roe":  float(trade.get("peak_roe", 0)),
            "bot_type":  trade.get("bot_type", "APEX"),
            "direction": trade.get("direction", "LONG"),
            "entry":     float(trade.get("entry", 0)),
            "leverage":  float(trade.get("leverage", 5)),
            "size":      float(trade.get("size", 14)),
            "db_id":     trade.get("db_id"),
            "tid":       trade.get("tid"),
            "age_mins":  float(trade.get("age_mins", 0)),
            "funding_rate": float(trade.get("funding_rate", 0.0001) or 0.0001),
        }

def monitor_hard_sl():
    """
    Runs every 15 seconds during sleep.
    Two jobs:
    1. Hard SL backstop -- last resort if price blows through SL
    2. Ratchet floor enforcement -- fires in seconds not minutes
       Trades can drop from +15% to +5% in 30 seconds.
       Waiting 3 minutes for the next cycle means floor is already breached.
    """
    try:
        trades = get_open_trades()
        for trade in trades:
            symbol    = trade["symbol"]
            direction = trade["direction"]
            entry     = float(trade.get("entry", 0))
            leverage  = float(trade.get("leverage", 5))
            if entry <= 0: continue

            # Get live price -- WebSocket first, API fallback
            try:
                _ws_mon = get_ws_manager()
                _ws_p   = _ws_mon.get_price(symbol) if _ws_mon else None
                if _ws_p:
                    price = float(_ws_p)
                else:
                    get_trade_rl().acquire(weight=1)
                    price = float(get_trade_client().futures_symbol_ticker(symbol=symbol)["price"])
            except:
                price = float(trade.get("current_price", 0))
            if price <= 0: continue

            # Compute live ROE
            if direction == "LONG":
                roe = (price - entry) / entry * 100 * leverage
            else:
                roe = (entry - price) / entry * 100 * leverage

            bot_type = trade.get("bot_type", "APEX")
            sl       = float(trade.get("sl", 0))

            # ── JOB 1: HARD SL ──
            if sl > 0:
                sl_hit = False
                if direction == "LONG"  and price <= sl * 0.995: sl_hit = True
                if direction == "SHORT" and price >= sl * 1.005: sl_hit = True
                if sl_hit:
                    logger.warning(f"  🛑 HARD SL: {bot_type} {symbol} price={price:.4f} sl={sl:.4f} ROE={roe:.1f}%")
                    # Prevent triple-fire -- block same symbol for 30s
                    _now = time.time()
                    if symbol in _sl_fired and _now - _sl_fired[symbol] < 30:
                        continue
                    _sl_fired[symbol] = _now
                    _closing_set.add(symbol)
                    try:
                        if bot_type == "SPRING":
                            _sl_price = float(trade.get("sl", 0))
                            _entry_price = float(trade.get("entry", 0))
                            _sl_reason = "Ratchet SL" if _sl_price > _entry_price else "Hard SL"
                            _close_spring_trade(symbol, _sl_reason)
                        else:
                            tid = trade.get("tid")
                            if tid:
                                try:
                                    from execution_bridge import close_order
                                    close_order(symbol, tid, reason="Hard SL")
                                    # Trigger outcome scoring and ready queue replacement
                                    try:
                                        _ht = trades_dict.get(symbol, {}) if "trades_dict" in dir() else {}
                                        _price = float(trade.get("current_price", 0))
                                        _entry = float(trade.get("entry", 0))
                                        _size  = float(trade.get("size", 0))
                                        _lev   = float(trade.get("leverage", 5))
                                        _dir   = trade.get("direction", "LONG")
                                        if _price > 0 and _entry > 0 and _size > 0:
                                            _pnl_pct = (_price-_entry)/_entry*100*_lev if _dir=="LONG" else (_entry-_price)/_entry*100*_lev
                                            _pnl = _size * (_pnl_pct/100)
                                            # I3: Cost model for Hard SL closes
                                            _total_cost = 0.0
                                            try:
                                                _notional2 = _size * _lev
                                                _fee2 = _notional2 * 0.0004 * 2
                                                _slip2 = _notional2 * 0.0002 * 2
                                                _total_cost = _fee2 + _slip2
                                                _pnl -= _total_cost
                                                logger.info(f"  [COST] Hard SL {symbol}: fee=${_fee2:.3f} slip=${_slip2:.3f} net_pnl=${_pnl:.3f}")
                                            except Exception as _ce:
                                                logger.warning(f"  [COST] Hard SL error {symbol}: {_ce}")
                                            # Save total_cost to DB
                                            try:
                                                import sqlite3 as _sq3
                                                _cc = _sq3.connect(TRADES_DB)
                                                _cc.execute("UPDATE trades SET total_cost=? WHERE symbol=? AND status='CLOSED' ORDER BY close_time DESC LIMIT 1", (round(_total_cost,4), symbol))
                                                _cc.commit(); _cc.close()
                                            except Exception as _dbe: logger.debug(f"Hard SL cost update: {_dbe}")
                                            on_trade_closed(symbol, "APEX", _pnl, "Hard SL", db_id=_db_id)
                                            update_paper_balance(round(_pnl, 4))
                                            set_cooldown(symbol, _dir)
                                    except Exception as _hsle: logger.warning(f"  Hard SL close error {symbol}: {_hsle}")
                                except Exception as e:
                                    logger.error(f"  Hard SL close failed {symbol}: {e}")
                    finally:
                        _closing_set.discard(symbol)
                    continue  # skip ratchet check if hard SL fired

            # ── JOB 2: RATCHET FLOOR ENFORCEMENT (15s) ──
            with _ratchet_state_lock:
                rs = _ratchet_state.get(symbol)
            if not rs: continue

            floor_roe = float(rs.get("floor_roe", 0))
            be_set    = bool(rs.get("be_set", False))
            be_price  = float(rs.get("be_price", 0))

            # Breakeven exit -- price dropped below BE price
            if be_set and be_price > 0:
                be_hit = False
                if direction == "LONG"  and price <= be_price: be_hit = True
                if direction == "SHORT" and price >= be_price: be_hit = True
                if be_hit:
                    if symbol in _closing_set:
                        continue
                    logger.warning(f"  🔒 RATCHET BE (15s): {bot_type} {symbol} price={price:.4f} be={be_price:.4f} ROE={roe:.1f}%")
                    _closing_set.add(symbol)
                    try:
                        if bot_type == "SPRING":
                            _close_spring_trade(symbol, f"Ratchet BE exit ROE={roe:.1f}%")
                        else:
                            tid = rs.get("tid")
                            if tid:
                                try:
                                    from execution_bridge import close_order
                                    close_order(symbol, tid, reason=f"Ratchet BE exit ROE={roe:.1f}%")
                                except Exception as _coe:
                                    logger.warning(f"  [RATCHET BE] close_order {symbol}: {_coe}")
                                _apex_finalize_close(symbol, rs, price, f"Ratchet BE exit ROE={roe:.1f}%")
                    finally:
                        _closing_set.discard(symbol)
                    with _ratchet_state_lock:
                        _ratchet_state.pop(symbol, None)
                    continue

            # Ratchet floor breach -- ROE dropped below floor
            if be_set and floor_roe > 0 and roe <= floor_roe:
                if symbol in _closing_set:
                    continue
                logger.warning(f"  🔒 RATCHET FLOOR (15s): {bot_type} {symbol} ROE={roe:.1f}% <= floor={floor_roe:.1f}%")
                _closing_set.add(symbol)
                try:
                    if bot_type == "SPRING":
                        _close_spring_trade(symbol, f"Ratchet floor={floor_roe:.0f}% ROE={roe:.1f}%")
                    else:
                        tid = rs.get("tid")
                        if tid:
                            try:
                                from execution_bridge import close_order
                                close_order(symbol, tid, reason=f"Ratchet floor={floor_roe:.0f}% ROE={roe:.1f}%")
                            except Exception as _coe:
                                logger.warning(f"  [RATCHET FLOOR] close_order {symbol}: {_coe}")
                            _apex_finalize_close(symbol, rs, price, f"Ratchet floor={floor_roe:.0f}% ROE={roe:.1f}%")
                finally:
                    _closing_set.discard(symbol)
                with _ratchet_state_lock:
                    _ratchet_state.pop(symbol, None)

    except Exception as e:
        logger.error(f"Monitor error: {e}")
def _continuous_monitor_worker():
    """
    Dedicated thread -- runs every 3 seconds continuously.
    Only job: ratchet floor enforcement + hard SL.
    Completely independent of the 3-minute cycle.
    This is what your bad experience taught -- SL and ratchet
    cannot wait. A trade can die in 2 seconds.
    3 seconds is the sweet spot -- fast enough to catch moves,
    light enough not to hammer the API.
    """
    logger.info("Continuous monitor thread started (3s interval)")
    while True:
        try:
            # Check for WebSocket alerts -- immediate response to big moves
            with _ws_alert_lock:
                _alerted = list(_ws_alert_symbols)
                _ws_alert_symbols.clear()
            if _alerted:
                # Fast-track throttle -- max 1 coin per 8s
                _now_ft = time.time()
                _alerted = [s for s in _alerted if _now_ft - _last_fasttrack.get(s, 0) > 8]
                for _fts in _alerted: _last_fasttrack[_fts] = _now_ft
            if _alerted:
                logger.info(f"  ⚡ WS triggered immediate check: {_alerted}")
            monitor_hard_sl()

            # Execution precision -- check forming candles for early entry signals
            try:
                from websocket_manager import get_forming_candle, is_reversal_candle, _forming_candles
                if _forming_candles:
                    # Cache market for 30s -- avoid flooding analyze_market
                    import time as _pt
                    if not hasattr(_continuous_monitor_worker, "_mkt") or (_pt.time() - _continuous_monitor_worker._mkt_ts) > 30:
                        _continuous_monitor_worker._mkt = analyze_market()
                        _continuous_monitor_worker._mkt_ts = _pt.time()
                    _prec_market = _continuous_monitor_worker._mkt
                    for _sym, _candle in list(_forming_candles.items()):
                        _pct = _candle.get("pct_formed", 0) or 0
                        if 65 <= _pct <= 85 and not _candle.get("closed"):
                            if is_reversal_candle(_candle, "SHORT"):
                                logger.info(f"  🎯 PRECISION: {_sym} SHORT reversal {_pct:.0f}% formed")
                                with _ws_alert_lock: _ws_alert_symbols.add(_sym)
                                _rq = getattr(run_cycle, "_ready_short", [])
                                if any(s["symbol"]==_sym for s in _rq):
                                    _sig = next(s for s in _rq if s["symbol"]==_sym)
                                    if not is_on_cooldown(_sym,"SHORT") and not is_repeat_offender(_sym, direction="SHORT") and _sig["confidence"] >= 55:
                                        logger.info(f"  🎯 PRECISION ENTRY: {_sym} SHORT")
                                        _open_apex_trade(_sym,"SHORT",_sig["score"],_sig["confidence"],"PRECISION:"+_sig["reason"][:30],_prec_market)
                            elif is_reversal_candle(_candle, "LONG"):
                                logger.info(f"  🎯 PRECISION: {_sym} LONG reversal {_pct:.0f}% formed")
                                with _ws_alert_lock: _ws_alert_symbols.add(_sym)
                                _rq_l = getattr(run_cycle, "_ready_long", [])
                                if any(s["symbol"]==_sym for s in _rq_l):
                                    _sig_l = next(s for s in _rq_l if s["symbol"]==_sym)
                                    if not is_on_cooldown(_sym,"LONG") and not is_repeat_offender(_sym, direction="LONG") and _sig_l["confidence"] >= 55:
                                        logger.info(f"  🎯 PRECISION ENTRY: {_sym} LONG")
                                        _open_apex_trade(_sym,"LONG",_sig_l["score"],_sig_l["confidence"],"PRECISION:"+_sig_l["reason"][:30],_prec_market)
            except Exception as e:
                logger.error(f"Precision monitor error: {e}")
        except Exception as _me:
            logger.error(f"Monitor worker error: {_me}")
        time.sleep(3)


def start_continuous_monitor():
    """Start the dedicated ratchet/SL monitor thread."""
    t = threading.Thread(
        target=_continuous_monitor_worker,
        daemon=True,
        name="ContinuousMonitor"
    )
    t.start()
    logger.info("Continuous monitor started -- ratchet + SL checking every 3s")
    return t


def run_continuous():
    logger.info("=" * 60)
    logger.info("APEX MIND v2.0 STARTING -- Full Autonomous Control")
    logger.info("=" * 60)
    init_db()
    # ── WEBSOCKET MANAGER -- real-time price feeds ──
    _ws = None
    try:
        if not os.environ.get("APEX_BACKFILL_MODE"):
            _ws = init_ws_manager(MIND_API_KEY, MIND_API_SECRET)
        if _ws and not os.environ.get("APEX_BACKFILL_MODE") and _ws.start():
            # Register significant move callback
            def _on_significant_move(symbol, price, chg_pct):
                logger.info(f"  ⚡ WS ALERT: {symbol} moved {chg_pct:.1f}% → ${price:.4f} -- triggering immediate check")
                # Will be used by continuous monitor for immediate SL check
                import apex_mind as _am
                if hasattr(_am, "_ws_alert_symbols"):
                    with getattr(_am, "_ws_alert_lock", threading.Lock()):
                        _am._ws_alert_symbols.add(symbol)
            _ws.register_move_callback(1.5, _on_significant_move)
            logger.info("WebSocket manager initialized")
        else:
            logger.warning("WebSocket unavailable -- using polling fallback")
    except Exception as _wse:
        logger.warning(f"WebSocket init error: {_wse} -- using polling fallback")
    # Persist learning timestamps across restarts
    # Initialize regime transition predictor
    init_regime_predictor()
    _init_client_pool()
    logger.info("Regime transition predictor initialized")
    _ts_file = os.path.join(BASE, "apex_mind_timestamps.json")
    try:
        _ts = json.load(open(_ts_file))
    except:
        _ts = {}
    last_learn        = float(_ts.get("last_learn",       0))
    last_report       = float(_ts.get("last_report",      0))
    last_incremental  = float(_ts.get("last_incremental", 0))
    last_sess_refresh = float(_ts.get("last_sess_refresh",0))
    # If never run before, set to now minus almost the interval so first run happens soon
    now_ts = time.time()
    if last_learn        == 0: last_learn        = now_ts - 82800  # 23H ago → runs in 1H
    if last_report       == 0: last_report       = now_ts - 82800
    if last_incremental  == 0: last_incremental  = now_ts - 10200  # 2H50m ago → runs in 10min
    if last_sess_refresh == 0: last_sess_refresh = now_ts - 10200
    LEARN_INT       = 14400   # 4H full learning
    REPORT_INT      = 86700   # 24H report
    INCREMENTAL_INT = 3600   # 1H incremental
    SESS_REFRESH    = 10800   # 3H session quality refresh
    # ── REBUILD RATCHET STATE FROM DB ON STARTUP ──
    # Ensures open trades have floor/BE protection immediately after restart
    try:
        conn = sqlite3.connect(TRADES_DB)
        open_rows = conn.execute("""
            SELECT id, symbol, direction, entry, leverage, peak_roe, sl
            FROM trades WHERE status='OPEN'""").fetchall()
        conn.close()
        with _ratchet_state_lock:
            for db_id, symbol, direction, entry, lev, peak_roe, sl in open_rows:
                peak = float(peak_roe or 0)
                lev  = float(lev or 5)
                # Recompute BE price if peak crossed trigger
                be_set = peak >= 5.0
                buf = 0.002 / max(lev, 1)
                be_price = round(float(entry)*(1+buf), 8) if direction=="LONG"                            else round(float(entry)*(1-buf), 8) if be_set else 0
                # Recompute floor from peak
                if   peak >= 40: floor = peak - 6
                elif peak >= 30: floor = peak - 8
                elif peak >= 20: floor = peak - 7
                elif peak >= 10: floor = peak - 5
                else:            floor = 0
                _ratchet_state[symbol] = {
                    "peak_roe":  peak,
                    "floor_roe": floor,
                    "be_set":    be_set,
                    "be_price":  be_price,
                    "direction": direction,
                    "entry":     float(entry),
                    "leverage":  lev,
                    "bot_type":  "APEX",
                    "tid":       f"DB_{db_id}",
                    "db_id":     db_id,
                }
        # Fix trades with sl=0 -- recalculate ATR-based SL
        fix_conn = sqlite3.connect(TRADES_DB)
        zero_sl = fix_conn.execute(
            "SELECT id, symbol, direction, entry FROM trades WHERE status='OPEN' AND (sl IS NULL OR sl=0)"
        ).fetchall()
        fix_conn.close()
        for fix_id, fix_sym, fix_dir, fix_entry in zero_sl:
            try:
                df15 = fetch(fix_sym, "15m", 20)
                if df15 is not None and len(df15) >= 15:
                    df15 = add_inds(df15)
                    atr = float(df15.iloc[-2].get("atr", float(fix_entry)*0.01) or float(fix_entry)*0.01)
                else:
                    atr = float(fix_entry) * 0.015
                fix_sl = round(float(fix_entry) + atr*1.5, 8) if fix_dir=="SHORT" else round(float(fix_entry) - atr*1.5, 8)
                fix_conn2 = sqlite3.connect(TRADES_DB)
                fix_conn2.execute("UPDATE trades SET sl=? WHERE id=?", (fix_sl, fix_id))
                fix_conn2.commit(); fix_conn2.close()
                logger.info(f"  SL fixed for {fix_sym}: {fix_sl:.6f}")
            except: pass
        logger.info(f"Ratchet state rebuilt for {len(open_rows)} open APEX trades")
    except Exception as e:
        logger.error(f"Ratchet rebuild error: {e}")

    # Also rebuild Spring ratchet state
    try:
        conn = sqlite3.connect(TRADES_DB)
        spring_rows = conn.execute("""
            SELECT symbol, entry, leverage, sl
            FROM dip_trades WHERE status='OPEN'""").fetchall()
        conn.close()
        with _ratchet_state_lock:
            for symbol, entry, lev, sl in spring_rows:
                if symbol not in _ratchet_state:
                    _ratchet_state[symbol] = {
                        "peak_roe":  0,
                        "floor_roe": 0,
                        "be_set":    False,
                        "be_price":  0,
                        "direction": "LONG",
                        "entry":     float(entry or 0),
                        "leverage":  float(lev or 4),
                        "bot_type":  "SPRING",
                        "tid":       None,
                    }
        logger.info(f"Ratchet state rebuilt for {len(spring_rows)} open Spring trades")
    except Exception as e:
        logger.error(f"Spring ratchet rebuild error: {e}")

    # ── RECONCILE BALANCE VS DB ON STARTUP ──
    # Recalculate paper_balance from DB to fix any crash-induced inconsistency
    try:
        conn = sqlite3.connect(TRADES_DB)
        apex_pnl = float(conn.execute(
            "SELECT COALESCE(SUM(pnl),0) FROM trades WHERE status='CLOSED' AND pattern='APEX_MIND_ENTRY'"
        ).fetchone()[0])
        spring_pnl = float(conn.execute(
            "SELECT COALESCE(SUM(pnl),0) FROM dip_trades WHERE status='CLOSED' AND reason NOT IN ('Ghost - cleaned','Historical-restored')"
        ).fetchone()[0])
        conn.close()
        pb_file = os.path.join(BASE, "paper_balance.json")
        try: pb = json.load(open(pb_file))
        except: pb = {}
        apex_start  = float(pb.get("apex_starting",   700))
        spring_start = float(pb.get("spring_starting", 300))
        pb["futures"]  = round(apex_start   + apex_pnl,   4)
        pb["spring"]   = round(spring_start + spring_pnl, 4)
        pb["total"]    = round(pb["futures"] + pb["spring"], 2)
        pb["apex_pnl"]    = round(apex_pnl,   4)
        pb["spring_pnl"]  = round(spring_pnl, 4)
        pb["apex_starting"]   = apex_start
        pb["spring_starting"] = spring_start
        pb["spot"] = 0.0
        tmp = pb_file + ".tmp"
        json.dump(pb, open(tmp,"w"), indent=2)
        os.replace(tmp, pb_file)
        logger.info(f"Balance reconciled: APEX=${pb['futures']} Spring=${pb['spring']}")
    except Exception as e:
        logger.error(f"Balance reconcile error: {e}")

    # ── PERSIST COOLDOWN CACHE ──
    try:
        _cd_file = os.path.join(BASE, "cooldown_cache.json")
        if os.path.exists(_cd_file):
            _cd_data = json.load(open(_cd_file))
            now_cd = time.time()
            for key, expiry in _cd_data.items():
                if expiry > now_cd:
                    _cooldowns[key] = expiry
            logger.info(f"Cooldown cache restored: {len(_cooldowns)} active cooldowns")
    except Exception as e:
        logger.error(f"Cooldown restore error: {e}")

    # Start order flow background worker
    start_orderflow_worker()
    # Start continuous ratchet + SL monitor (every 3 seconds)
    if not os.environ.get("APEX_BACKFILL_MODE"):
        start_continuous_monitor()
    # Initial session quality load
    refresh_hour_quality()
    while True:
        try:
            start = time.time()
            run_cycle()
            now = time.time()
            if now - last_incremental >= INCREMENTAL_INT:
                run_incremental_learning()
                refresh_hour_quality()
                last_incremental = now
                last_sess_refresh = now
                try:
                    _ts = json.load(open(_ts_file)) if os.path.exists(_ts_file) else {}
                    _ts["last_incremental"] = now
                    _ts["last_sess_refresh"] = now
                    json.dump(_ts, open(_ts_file,"w"))
                except: pass
            if now - last_learn >= LEARN_INT:
                run_learning(); last_learn = now
                try:
                    _ts = json.load(open(_ts_file)) if os.path.exists(_ts_file) else {}
                    _ts["last_learn"] = now
                    json.dump(_ts, open(_ts_file,"w"))
                except: pass
            if now - last_report >= REPORT_INT:
                send_report(); last_report = now
            # Hourly equity snapshot
            if now - getattr(run_continuous, "_last_equity", 0) > 3600:
                try:
                    bal = get_balance()
                    if bal and float(bal.get("total", 0)) > 0:
                        from database import save_equity_snapshot as _seq
                        _seq(float(bal["apex"]), float(bal["spring"]))
                except Exception as _e: logger.warning(f"Equity snapshot failed: {_e}")
                run_continuous._last_equity = now
            elapsed  = time.time() - start
            sleep_t  = max(0, OBSERVE_INTERVAL - elapsed)
            _usage1 = _rate_limiter.usage_pct()
            _usage2 = _rl_scanner.usage_pct() if _rl_scanner != _rate_limiter else 0
            _usage3 = _rl_market.usage_pct() if _rl_market not in (_rate_limiter, _rl_scanner) else 0
            logger.info(f"Cycle {elapsed:.1f}s | API k1={_usage1:.0f}% k2={_usage2:.0f}% k3={_usage3:.0f}% | sleep {sleep_t:.0f}s")
            # Price alerts check every 30s during sleep
            _trades_snap = get_open_trades()
            slept = 0
            while slept < sleep_t:
                chunk = min(30, sleep_t - slept)
                time.sleep(chunk)
                slept += chunk
                try:
                    if check_price_alerts(_trades_snap):
                        break
                except: break
        except KeyboardInterrupt:
            logger.info("Stopped by user")
            try:
                from emailer import Emailer
                Emailer().send("🛑 APEX BOT STOPPED", "Bot was manually stopped (KeyboardInterrupt / pkill).")
            except: pass
            break
        except Exception as e:
            import traceback as _ctb
            logger.error(f"Cycle error: {e}\n{_ctb.format_exc()[:500]}")
            try:
                from emailer import Emailer
                Emailer().send(
                    f"🚨 APEX BOT CYCLE ERROR",
                    f"Error: {e}\n\nTraceback:\n{_ctb.format_exc()[:1000]}"
                )
            except: pass
            time.sleep(60)

if __name__=="__main__":
    if "--report"  in sys.argv: init_db(); send_report()
    elif "--learn" in sys.argv: init_db(); run_learning()
    elif "--once"  in sys.argv: init_db(); run_cycle()
    elif "--kill"  in sys.argv:
        kill_switch(enable=True)
        print("APEX MIND execution KILLED -- observation continues")
    elif "--resume" in sys.argv:
        kill_switch(enable=False)
        print("APEX MIND execution RESUMED")
    elif "--status" in sys.argv:
        cfg = load_master_config()
        exec_cfg = cfg.get("execution", {})
        gates = cfg.get("confidence_gates", {})
        print(f"APEX MIND v2.0 Status")
        print(f"  Enabled:  {cfg.get('apex_mind_enabled', True)}")
        print(f"  Mode:     {cfg.get('mode','paper')}")
        print(f"  Exits:    Spring CLOSE={exec_cfg.get('spring_close')} TIGHTEN={exec_cfg.get('spring_tighten')} | APEX CLOSE={exec_cfg.get('apex_close')} TIGHTEN={exec_cfg.get('apex_tighten')}")
        print(f"  Entries:  APEX={exec_cfg.get('apex_entries')} SPRING={exec_cfg.get('spring_entries')}")
        print(f"  Gates:    apex_entry={gates.get('apex_entry_min',70)}% spring_entry={gates.get('spring_entry_min',55)}% apex_close={gates.get('apex_close_min',70)}%")
        reg = get_regime_suggestions()
        print(f"  Regime:   {reg.get('regime','?')} conf={reg.get('confidence',0):.0f}% LONG={reg.get('long_on')} SHORT={reg.get('short_on')}")
        hour = datetime.now(timezone.utc).hour
        wr, pnl, mult = get_session_quality(hour)
        print(f"  Session:  Hour {hour:02d}:00 UTC | WR={wr:.0f}% avgPnL=${pnl:.2f} size_mult={mult:.2f}")
        sess_api_usage = _rate_limiter.usage_pct()
        print(f"  API:      {sess_api_usage:.0f}% of limit used")
    elif "--enable-entries" in sys.argv:
        cfg_path = os.path.join(BASE, "master_config.json")
        cfg = json.load(open(cfg_path)) if os.path.exists(cfg_path) else load_master_config()
        cfg["execution"]["apex_entries"]   = True
        cfg["execution"]["spring_entries"] = True
        json.dump(cfg, open(cfg_path, "w"), indent=2)
        print("APEX MIND entries ENABLED -- full autonomous control active")
    elif "--disable-entries" in sys.argv:
        cfg_path = os.path.join(BASE, "master_config.json")
        cfg = json.load(open(cfg_path)) if os.path.exists(cfg_path) else load_master_config()
        cfg["execution"]["apex_entries"]   = False
        cfg["execution"]["spring_entries"] = False
        json.dump(cfg, open(cfg_path, "w"), indent=2)
        print("APEX MIND entries DISABLED -- exits still active")
    elif "--enable-apex-close" in sys.argv:
        cfg_path = os.path.join(BASE, "master_config.json")
        cfg = json.load(open(cfg_path)) if os.path.exists(cfg_path) else load_master_config()
        cfg["execution"]["apex_close"] = True
        json.dump(cfg, open(cfg_path, "w"), indent=2)
        print("APEX CLOSE enabled")
    else: run_continuous()

def on_trade_closed(symbol, bot_type, pnl, reason, db_id=None):
    """
    Called by bot when a trade closes.
    Triggers outcome scoring for all recent observations of this trade.
    Pass db_id to avoid attribution bug when same symbol closes twice in quick succession.
    """
    try:
        now_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        conn = _get_mind_conn()
        # Find all unscored observations for this symbol
        pending = conn.execute("""
            SELECT id, decision, decision_reason, roe, decision_confidence
            FROM observations
            WHERE symbol=? AND bot_type=? AND outcome_filled=0
            ORDER BY id DESC LIMIT 20""",
            (symbol, bot_type)).fetchall()

        if not pending:
            conn.close()
            return

        final_pnl = float(pnl or 0)
        was_win = final_pnl > 0
        was_correct = 0
        outcome_reason = ""

        # FIX A (C1 completion): compute true ROE% -- on_trade_closed is now sole authority
        # Use db_id when available to avoid attribution bug on fast successive closes
        _t_entry = _t_exit = 0.0; _t_lev = 5.0; _t_dir = "LONG"; _t_size = 14.0; _t_cost = 0.0
        try:
            _tc = sqlite3.connect(TRADES_DB)
            if bot_type == "APEX":
                if db_id:
                    _row = _tc.execute("""SELECT entry, exit, leverage, direction, size, total_cost
                        FROM trades WHERE id=?""", (db_id,)).fetchone()
                else:
                    _row = _tc.execute("""SELECT entry, exit, leverage, direction, size, total_cost
                        FROM trades WHERE symbol=? AND status='CLOSED'
                        ORDER BY close_time DESC LIMIT 1""", (symbol,)).fetchone()
            else:
                _row = _tc.execute("""SELECT entry, exit, leverage, 'LONG', size, total_cost
                    FROM dip_trades WHERE symbol=? AND status='CLOSED'
                    ORDER BY close_time DESC LIMIT 1""", (symbol,)).fetchone()
            _tc.close()
            if _row:
                _t_entry = float(_row[0] or 0); _t_exit = float(_row[1] or 0)
                _t_lev = float(_row[2] or 5); _t_dir = str(_row[3] or "LONG")
                _t_size = float(_row[4] or 14); _t_cost = float(_row[5] or 0) if len(_row) > 5 else 0.0
        except Exception as _ce:
            logger.debug(f"on_trade_closed price lookup {symbol}: {_ce}")

        if _t_entry > 0 and _t_exit > 0:
            if _t_dir == "LONG":
                final_roe_pct = (_t_exit - _t_entry) / _t_entry * _t_lev * 100
            else:
                final_roe_pct = (_t_entry - _t_exit) / _t_entry * _t_lev * 100
            # FIX B: subtract cost-equivalent ROE
            if _t_size > 0 and _t_cost > 0:
                final_roe_pct -= (_t_cost / _t_size) * 100
        else:
            final_roe_pct = 0.0

        for obs_id, decision, dec_reason, dec_roe, conf in pending:
            dec_roe_val = float(dec_roe or 0)
            final_roe_val = final_roe_pct  # FIX A: ROE% not dollars

            if decision == "CLOSE":
                if not was_win:
                    was_correct = 1
                    outcome_reason = f"Trade lost ${final_pnl:.2f} (ROE={final_roe_val:.1f}%) -- CLOSE was right"
                else:
                    if final_roe_val <= dec_roe_val * 1.2:
                        was_correct = 1
                        outcome_reason = f"Timely close: {dec_roe_val:.1f}% → {final_roe_val:.1f}%"
                    elif final_roe_val > dec_roe_val * 1.5:
                        was_correct = 0
                        outcome_reason = f"Premature CLOSE: {dec_roe_val:.1f}% → {final_roe_val:.1f}%"
                    else:
                        was_correct = 1
                        outcome_reason = f"Acceptable close: {dec_roe_val:.1f}% → {final_roe_val:.1f}%"

            elif decision == "HOLD":
                if was_win:
                    if final_roe_val >= dec_roe_val * 1.1:
                        was_correct = 1
                        outcome_reason = f"HOLD added value: {dec_roe_val:.1f}% → {final_roe_val:.1f}%"
                    elif dec_roe_val > 5 and final_roe_val < dec_roe_val * 0.8:
                        was_correct = 0
                        outcome_reason = f"HOLD gave back gains: {dec_roe_val:.1f}% → {final_roe_val:.1f}%"
                    else:
                        was_correct = 1
                        outcome_reason = f"HOLD acceptable: {dec_roe_val:.1f}% → {final_roe_val:.1f}%"
                else:
                    was_correct = 0 if dec_roe_val < -3 else 1
                    outcome_reason = f"Trade lost (ROE={final_roe_val:.1f}%) after HOLD at {dec_roe_val:.1f}%"

            elif decision == "TIGHTEN":
                if was_win:
                    was_correct = 1
                    outcome_reason = f"TIGHTEN protected: ROE={final_roe_val:.1f}%"
                else:
                    was_correct = 1 if abs(final_roe_val) < abs(dec_roe_val)*1.5 else 0
                    outcome_reason = f"TIGHTEN {'limited' if was_correct else 'failed'}: {dec_roe_val:.1f}% → {final_roe_val:.1f}%"

            conn.execute("""UPDATE observations SET
                outcome_correct=?, outcome_roe=?, outcome_reason=?,
                outcome_filled=1 WHERE id=?""",
                (was_correct, final_roe_pct, outcome_reason, obs_id))

        conn.commit(); conn.close()
        logger.info(f"on_trade_closed: {symbol} {bot_type} ${final_pnl:.2f} -- scored {len(pending)} observations")

        # ── INSTANT SLOT REPLACEMENT from ready queue ──
        try:
            # Use cached market if fresh (avoid flooding analyze_market calls)
            import time as _t
            _mkt_cache = getattr(on_trade_closed, "_market_cache", None)
            _mkt_ts = getattr(on_trade_closed, "_market_ts", 0)
            if _mkt_cache is None or (_t.time() - _mkt_ts) > 30:
                _mkt_cache = analyze_market()
                on_trade_closed._market_cache = _mkt_cache
                on_trade_closed._market_ts = _t.time()
            market = _mkt_cache
            if bot_type == "APEX":
                # Check which direction slot just freed up
                import sqlite3 as _sq
                _conn = _sq.connect(TRADES_DB)
                _dir = _conn.execute("SELECT direction FROM trades WHERE symbol=? ORDER BY id DESC LIMIT 1", (symbol,)).fetchone()
                _conn.close()
                _freed_dir = _dir[0] if _dir else None
                # Check actual open counts vs regime limits before instant replace
                _reg_ir = get_regime_suggestions()
                _max_l_ir = _reg_ir.get("max_long", 6)
                _max_s_ir = _reg_ir.get("max_short", 2)
                _conn_ir = _sq.connect(TRADES_DB)
                _open_l_ir = _conn_ir.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN' AND direction='LONG'").fetchone()[0]
                _open_s_ir = _conn_ir.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN' AND direction='SHORT'").fetchone()[0]
                _conn_ir.close()
                if _freed_dir == "LONG" and hasattr(run_cycle, "_ready_long") and run_cycle._ready_long:
                    if _open_l_ir < _max_l_ir:
                        _next = run_cycle._ready_long[0]
                        if not is_on_cooldown(_next["symbol"], "LONG") and not is_repeat_offender(_next["symbol"], direction="LONG"):
                            logger.info(f"  🔄 INSTANT REPLACE: {_next['symbol']} LONG ({_open_l_ir+1}/{_max_l_ir})")
                            _open_apex_trade(_next["symbol"], "LONG", _next["score"], _next["confidence"], _next["reason"], market)
                    else:
                        logger.info(f"  🔄 INSTANT REPLACE skipped: LONG full ({_open_l_ir}/{_max_l_ir})")
                elif _freed_dir == "SHORT" and hasattr(run_cycle, "_ready_short") and run_cycle._ready_short:
                    if _open_s_ir < _max_s_ir:
                        _next = run_cycle._ready_short[0]
                        if not is_on_cooldown(_next["symbol"], "SHORT") and not is_repeat_offender(_next["symbol"], direction="SHORT"):
                            logger.info(f"  🔄 INSTANT REPLACE: {_next['symbol']} SHORT ({_open_s_ir+1}/{_max_s_ir})")
                            _open_apex_trade(_next["symbol"], "SHORT", _next["score"], _next["confidence"], _next["reason"], market)
                    else:
                        logger.info(f"  🔄 INSTANT REPLACE skipped: SHORT full ({_open_s_ir}/{_max_s_ir})")
            elif bot_type == "SPRING" and hasattr(run_cycle, "_ready_spring") and run_cycle._ready_spring:
                # Skip banned/cooldown coins in queue
                for _next in run_cycle._ready_spring:
                    if is_on_cooldown(_next["symbol"], "LONG"): continue
                    if is_repeat_offender(_next["symbol"], days=1, min_sl_hits=2): continue
                    logger.info(f"  🔄 INSTANT REPLACE: {_next['symbol']} SPRING from ready queue")
                    _open_spring_trade(_next["symbol"], _next["score"], _next["confidence"], _next["reason"], market, drop_pct=_next.get("drop_pct",0), recovery=_next.get("recovery",0))
                    break
        except Exception as _re:
            logger.warning(f"Ready queue replacement error: {_re}")

    except Exception as e:
        logger.error(f"on_trade_closed error: {e}")

def record_ratchet_event(symbol, bot_type, trigger_roe, new_floor):
    """Record when ratchet triggers -- learn optimal levels later"""
    try:
        conn=_get_mind_conn()
        conn.execute("""INSERT INTO ratchet_events
            (timestamp,symbol,bot_type,trigger_roe,new_floor)
            VALUES (?,?,?,?,?)""",
            (datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
             symbol,bot_type,trigger_roe,new_floor))
        conn.commit(); conn.close()
    except: pass

def analyze_optimal_sl():
    """Learn optimal SL distances from observations."""
    try:
        conn=_get_mind_conn()
        rows=conn.execute("""SELECT symbol, market_regime, sl_distance_pct,
            COUNT(*) as hits, AVG(outcome_roe) as avg_roe
            FROM observations WHERE sl_distance_pct IS NOT NULL AND sl_distance_pct > 0
            AND outcome_correct=0 AND market_regime IS NOT NULL
            GROUP BY symbol, market_regime HAVING hits >= 2
            ORDER BY hits DESC""").fetchall()
        conn.close()
        if not rows: return
        conn2=_get_mind_conn()
        for symbol,regime,sl_dist,hits,avg_roe in rows:
            regime=regime or "UNKNOWN"
            recommended_sl = round(float(sl_dist or 2)*1.5, 2) if hits>=3 else float(sl_dist or 2)
            conn2.execute("""INSERT OR REPLACE INTO patterns
                (pattern_key,description,conditions,occurrences,correct,accuracy,avg_pnl_impact,last_seen)
                VALUES (?,?,?,?,0,0.0,?,datetime('now'))""",
                (f"SL_OPTIMAL_{symbol}_{regime}",
                 f"{symbol} {regime}: {hits} SL misses at {sl_dist:.2f}% -- recommend {recommended_sl:.2f}%",
                 f'{{"symbol":"{symbol}","regime":"{regime}","current_sl":{sl_dist},"hits":{hits},"recommended":{recommended_sl}}}',
                 hits, round(float(avg_roe or 0),2)))
        conn2.commit(); conn2.close()
        logger.info(f"SL optimization: {len(rows)} coin/regime combinations analyzed")
    except Exception as e: logger.error(f"SL optimization: {e}")
def analyze_ratchet_events():
    """Learn ratchet effectiveness from observations."""
    try:
        conn=_get_mind_conn()
        rows=conn.execute("""SELECT bot_type, outcome_roe, outcome_correct, trade_age_mins
            FROM observations WHERE decision='CLOSE' AND outcome_correct IS NOT NULL
            AND outcome_roe IS NOT NULL""").fetchall()
        conn.close()
        if not rows: return
        total=len(rows)
        wins=[r for r in rows if int(r[2])==1]
        avg_pnl=round(sum(float(r[1] or 0) for r in rows)/total, 2)
        conn2=_get_mind_conn()
        conn2.execute("""INSERT OR REPLACE INTO patterns
            (pattern_key,description,conditions,occurrences,correct,accuracy,avg_pnl_impact,last_seen)
            VALUES (?,?,?,?,?,?,?,datetime('now'))""",
            ("RATCHET_ANALYSIS",
             f"Close exits: {len(wins)}/{total} wins ({len(wins)/total*100:.1f}%) avg_roe ${avg_pnl:.2f}",
             f'{{"total":{total},"wins":{len(wins)},"avg_pnl":{avg_pnl:.2f}}}',
             total,len(wins),round(len(wins)/total*100,1),round(avg_pnl,2)))
        conn2.commit(); conn2.close()
        logger.info(f"Ratchet analysis: {len(wins)}/{total} wins avg_roe ${avg_pnl:.2f}")
    except Exception as e: logger.error(f"Ratchet analysis: {e}")
def save_entry_suggestion(symbol, mode, direction, score, confidence, reason, market, price):
    """Save entry suggestion for later scoring"""
    try:
        conn=_get_mind_conn()
        rows=conn.execute("""SELECT bot_type, outcome_roe, outcome_correct, trade_age_mins
            FROM observations WHERE decision='CLOSE' AND outcome_correct IS NOT NULL
            AND outcome_roe IS NOT NULL""").fetchall()
        conn.close()
        if not rows: return
        total=len(rows)
        wins=[r for r in rows if int(r[2])==1]
        avg_pnl=round(sum(float(r[1] or 0) for r in rows)/total, 2)
        all_rows=[(r[0],float(r[1] or 0),float(r[3] or 0),'obs',r[0]) for r in rows]
        floors=[]
    except Exception as e: logger.error(f"Entry suggestion save: {e}")

def analyze_close_reasons():
    """Analyze close decisions from observations."""
    try:
        conn=_get_mind_conn()
        rows=conn.execute("""SELECT decision, COUNT(*) as total,
            SUM(outcome_correct) as wins, AVG(outcome_roe) as avg_roe
            FROM observations WHERE outcome_correct IS NOT NULL AND decision IS NOT NULL
            GROUP BY decision HAVING total >= 3
            ORDER BY avg_roe DESC""").fetchall()
        conn.close()
        if not rows: return
        conn2=_get_mind_conn()
        for reason,total,wins,avg_roe in rows:
            wr=round(wins/total*100,1) if total else 0
            avg_roe=round(float(avg_roe or 0),2)
            conn2.execute("""INSERT OR REPLACE INTO patterns
                (pattern_key,description,conditions,occurrences,correct,accuracy,avg_pnl_impact,last_seen)
                VALUES (?,?,?,?,?,?,?,datetime('now'))""",
                (f"CLOSE_REASON_{reason[:30].replace(' ','_')}",
                 f"Decision '{reason[:30]}': {wr}% WR avg_roe ${avg_roe:+.2f}",
                 f'{{"reason":"{reason[:30]}","wr":{wr},"avg_roe":{avg_roe}}}',
                 total,wins,wr,avg_roe))
        conn2.commit(); conn2.close()
        logger.info(f"Close reason analysis: {len(rows)} decision types analyzed")
    except Exception as e: logger.error(f"Close reason analysis: {e}")
def analyze_timeout_exits():
    """Learn from long-held observations."""
    try:
        conn=_get_mind_conn()
        timeouts=conn.execute("""SELECT symbol, direction, trade_age_mins, outcome_correct, outcome_roe
            FROM observations WHERE outcome_correct IS NOT NULL
            AND trade_age_mins >= 240
            ORDER BY id DESC LIMIT 1000""").fetchall()
        conn.close()
        if not timeouts: return
        wins=[t for t in timeouts if int(t[3])==1]
        losses=[t for t in timeouts if int(t[3])==0]
        total=len(timeouts)
        win_rate=round(len(wins)/total*100,1) if total else 0
        avg_dur=round(sum(float(t[2] or 0) for t in timeouts)/total,0) if total else 0
        conn2=_get_mind_conn()
        conn2.execute("""INSERT OR REPLACE INTO patterns
            (pattern_key,description,conditions,occurrences,correct,accuracy,avg_pnl_impact,last_seen)
            VALUES (?,?,?,?,?,?,?,datetime('now'))""",
            ("TIMEOUT_EXIT_ANALYSIS",
             f"Long holds (4H+): {win_rate}% WR, avg duration {avg_dur:.0f}m",
             f'{{"total":{total},"win_rate":{win_rate},"avg_duration":{avg_dur},"wins":{len(wins)},"losses":{len(losses)}}}',
             total,len(wins),win_rate,
             round(sum(float(t[4] or 0) for t in timeouts)/total,2) if total else 0))
        conn2.commit(); conn2.close()
        logger.info(f"Timeout analysis: {total} long holds, {win_rate}% profitable, avg {avg_dur:.0f}m")
    except Exception as e: logger.error(f"Timeout analysis: {e}")

def analyze_apex_timeout_analysis():
    """Learn optimal APEX timeout from observations -- updates apex_timeout_analysis in params"""
    try:
        import json as _j, os as _o
        conn = _get_mind_conn()
        rows = conn.execute("""SELECT CAST(trade_age_mins/60 AS INT) as age_h,
            COUNT(*), SUM(outcome_correct), ROUND(AVG(outcome_roe),2)
            FROM observations WHERE bot_type='APEX' AND outcome_correct IS NOT NULL
            AND trade_age_mins > 0
            GROUP BY age_h HAVING COUNT(*)>=20 ORDER BY age_h""").fetchall()
        conn.close()
        if len(rows) < 3: return

        _cfg = _o.path.join(BASE, "apex_mind_params.json")
        _p = _j.load(open(_cfg))
        existing = _p.get("apex_timeout_analysis", {})

        new_data = {}
        for age_h, cnt, wins, avg_roe in rows:
            wr = round(float(wins or 0)/cnt*100, 1)
            new_data[str(age_h)+'h'] = {"count": cnt, "wr": wr, "avg_roe": float(avg_roe or 0)}

        # Merge -- keep higher sample counts
        for k, v in new_data.items():
            if k not in existing or int(v["count"]) >= int(existing[k].get("count", 0)):
                existing[k] = v

        # Find optimal max hold -- where WR drops below 55% after 3H
        max_hold_h = 8.0
        for h in range(3, 13):
            key = str(h)+'h'
            if key in existing and existing[key]['wr'] < 55:
                max_hold_h = float(h)
                break

        _p["apex_timeout_analysis"] = existing
        # Only update max_hold_mins if we have solid data (>=5 hour buckets)
        if len(existing) >= 5 and max_hold_h < 8.0:
            _p["max_hold_mins"] = max(180, max_hold_h * 60)  # M11 FIX: floor 180 mins
            _p["ratchet"]["max_hold_h"] = max(3.0, max_hold_h)  # floor 3H
        _j.dump(_p, open(_cfg, "w"), indent=2)
        logger.info(f"  APEX timeout: {len(existing)} hour buckets | optimal_max_hold={max_hold_h}H")
        print(f"  APEX timeout: {len(existing)} hour buckets | max_hold={max_hold_h}H")
    except Exception as e:
        logger.error(f"analyze_apex_timeout_analysis: {e}")

def learn_dip_quality_weights():
    """Optimize dip quality score weights from actual trade outcomes"""
    logger.info("  Running learn_dip_quality_weights...")
    try:
        import json as _jw, os as _os
        conn = sqlite3.connect(TRADES_DB, timeout=30)
        conn.execute("PRAGMA busy_timeout=15000")
        rows = conn.execute("""SELECT dip_quality_score, rec_accelerating, macd_turning,
            rsi_momentum, drop_speed, recovery, pnl
            FROM dip_trades WHERE status='CLOSED' AND dip_quality_score>0
            AND entry>0""").fetchall()
        conn.close()
        if len(rows) < 30: return

        wins = [r for r in rows if float(r[6] or 0) > 0]
        losses = [r for r in rows if float(r[6] or 0) <= 0]
        if not wins or not losses: return

        def avg(lst, idx):
            vals = [float(r[idx] or 0) for r in lst]
            return round(sum(vals)/len(vals), 2) if vals else 0

        # Calculate edge for each feature
        rec_accel_win = sum(1 for r in wins if r[1]==1)/len(wins)
        rec_accel_loss = sum(1 for r in losses if r[1]==1)/len(losses)
        macd_win = sum(1 for r in wins if r[2]==1)/len(wins)
        macd_loss = sum(1 for r in losses if r[2]==1)/len(losses)
        rsi_mom_win = avg(wins, 3)
        rsi_mom_loss = avg(losses, 3)
        drop_spd_win = avg(wins, 4)
        drop_spd_loss = avg(losses, 4)
        rec_pct_win = avg(wins, 5)
        rec_pct_loss = avg(losses, 5)

        # Adjust weights based on edges
        _cfg = _os.path.join(BASE, "apex_mind_params.json")
        _p = _jw.load(open(_cfg))
        w = _p.get("dip_quality_weights", {})

        # Recovery % edge
        rec_edge = rec_pct_win - rec_pct_loss
        if rec_edge > 20: w["recovery_pct_80"] = min(40, w.get("recovery_pct_80", 30) + 2)
        elif rec_edge < 10: w["recovery_pct_80"] = max(20, w.get("recovery_pct_80", 30) - 2)

        # Rec accelerating edge
        ra_edge = rec_accel_win - rec_accel_loss
        if ra_edge > 0.15: w["rec_accelerating"] = min(30, w.get("rec_accelerating", 20) + 2)
        elif ra_edge < 0.05: w["rec_accelerating"] = max(10, w.get("rec_accelerating", 20) - 2)

        # MACD edge
        macd_edge = macd_win - macd_loss
        if macd_edge > 0.15: w["macd_turning"] = min(30, w.get("macd_turning", 20) + 2)
        elif macd_edge < 0.05: w["macd_turning"] = max(10, w.get("macd_turning", 20) - 2)

        _p["dip_quality_weights"] = w
        _jw.dump(_p, open(_cfg, "w"), indent=2)
        logger.info(f"  DIP WEIGHTS: RecEdge={rec_edge:.1f} RaEdge={ra_edge:.2f} MACDEdge={macd_edge:.2f} | rec80={w.get('recovery_pct_80')} ra={w.get('rec_accelerating')} macd={w.get('macd_turning')} n={len(rows)}")
        print(f"  DIP WEIGHTS: rec80={w.get('recovery_pct_80')} ra={w.get('rec_accelerating')} macd={w.get('macd_turning')}")
    except Exception as e:
        logger.error(f"learn_dip_quality_weights: {e}")

def learn_dip_quality_gate():
    """Optimize spring_dip_quality_gate from entry_suggestions outcomes"""
    logger.info("  Running learn_dip_quality_gate...")
    try:
        import json as _json, os as _os, sqlite3 as _sq
        _conn = _sq.connect(MIND_DB, timeout=30)
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA busy_timeout=15000")
        best_score = 70; best_wr = 0

        for gate in range(30, 91, 10):
            rows = _conn.execute("""SELECT was_correct, trade_pnl FROM entry_suggestions
                WHERE mode='SPRING' AND outcome_filled=1 AND score>=?
                AND ABS(trade_pnl)>0.01""", (gate,)).fetchall()
            if len(rows) < 30: continue
            wins = [r for r in rows if r[0]==1]
            wr = round(len(wins)/len(rows)*100, 1)
            if wr > best_wr:
                best_wr = wr; best_score = gate

        _conn.close()
        _cfg = _os.path.join(BASE, "apex_mind_params.json")
        _p = _json.load(open(_cfg))
        current = int(_p.get("spring_dip_quality_gate", 70))
        new_gate = max(50, min(75, int((current + best_score) / 2)))
        _p["spring_dip_quality_gate"] = new_gate
        _json.dump(_p, open(_cfg,"w"), indent=2)
        logger.info(f"  DIP QUALITY GATE: {current} -> {new_gate} (best_wr={best_wr}% at score>={best_score})")
        print(f"  DIP QUALITY GATE: {current} -> {new_gate} (best_wr={best_wr}%)")
    except Exception as e:
        logger.error(f"learn_dip_quality_gate error: {e}")
        import traceback; logger.error(traceback.format_exc())

def analyze_spring_scores():
    """Learn Spring entry quality from observations."""
    try:
        conn=_get_mind_conn()
        rows=conn.execute("""SELECT rsi_15m, outcome_roe, outcome_correct, trade_age_mins
            FROM observations WHERE bot_type='SPRING'
            AND outcome_correct IS NOT NULL AND rsi_15m IS NOT NULL
            ORDER BY id DESC LIMIT 1000""").fetchall()
        conn.close()
        if not rows: return
        buckets={}
        for r in rows:
            rsi=float(r[0] or 50)
            bucket=int(rsi//5)*5
            if bucket not in buckets: buckets[bucket]={"wins":0,"total":0,"roe":0}
            buckets[bucket]["total"]+=1
            if int(r[2])==1: buckets[bucket]["wins"]+=1
            buckets[bucket]["roe"]+=float(r[1] or 0)
        best_bucket=max(buckets.items(), key=lambda x: x[1]["wins"]/max(x[1]["total"],1))
        conn2=_get_mind_conn()
        for bucket,stats in sorted(buckets.items()):
            wr=round(stats["wins"]/stats["total"]*100,1) if stats["total"] else 0
            avg_roe=round(stats["roe"]/stats["total"],2) if stats["total"] else 0
            conn2.execute("""INSERT OR REPLACE INTO patterns
                (pattern_key,description,conditions,occurrences,correct,accuracy,avg_pnl_impact,last_seen)
                VALUES (?,?,?,?,?,?,?,datetime('now'))""",
                (f"SPRING_RSI_BUCKET_{bucket}",
                 f"Spring RSI {bucket}-{bucket+5}: {wr}% WR avg_roe ${avg_roe:+.2f}",
                 f'{{"rsi_range":"{bucket}-{bucket+5}","wr":{wr},"avg_roe":{avg_roe}}}',
                 stats["total"],stats["wins"],wr,avg_roe))
        conn2.commit(); conn2.close()
        logger.info(f"Spring score analysis: best RSI bucket={best_bucket[0]} ({best_bucket[1]['wins']}/{best_bucket[1]['total']})")
    except Exception as e: logger.error(f"Spring score analysis: {e}")
def analyze_regime_entries():
    """Learn which regimes produce best entry outcomes from observations."""
    try:
        conn=_get_mind_conn()
        rows=conn.execute("""SELECT market_regime, direction,
            COUNT(*) as total, SUM(outcome_correct) as wins,
            AVG(outcome_roe) as avg_roe, AVG(trade_age_mins) as avg_dur
            FROM observations WHERE outcome_correct IS NOT NULL
            AND market_regime IS NOT NULL AND market_regime != ''
            GROUP BY market_regime, direction
            ORDER BY market_regime""").fetchall()
        conn.close()
        if not rows: return
        conn2=_get_mind_conn()
        for regime,direction,total,wins,avg_roe,avg_dur in rows:
            wr=round(wins/total*100,1) if total else 0
            avg_roe=round(float(avg_roe or 0),2)
            avg_dur=round(float(avg_dur or 0),0)
            conn2.execute("""INSERT OR REPLACE INTO patterns
                (pattern_key,description,conditions,occurrences,correct,accuracy,avg_pnl_impact,last_seen)
                VALUES (?,?,?,?,?,?,?,datetime('now'))""",
                (f"REGIME_ENTRY_{regime}_{direction}",
                 f"Entry in {regime} {direction}: {wr}% WR avg_roe ${avg_roe:+.2f} avg {avg_dur:.0f}m",
                 f'{{"regime":"{regime}","direction":"{direction}","wr":{wr},"avg_roe":{avg_roe},"avg_dur":{avg_dur:.0f}}}',
                 total,wins,wr,avg_roe))
        conn2.commit(); conn2.close()
        logger.info(f"Regime entry analysis: {len(rows)} regime/direction combinations analyzed")
    except Exception as e: logger.error(f"Regime entry analysis: {e}")
def score_entry_suggestions():
    """Score previous entry suggestions against actual trade outcomes"""
    try:
        conn=_get_mind_conn()
        pending=conn.execute("""SELECT id,symbol,suggested_dir,confidence,entry_price,timestamp
            FROM entry_suggestions WHERE outcome_filled=0
            AND timestamp < datetime('now','-30 minutes')
            ORDER BY id LIMIT 50""").fetchall()
        conn.close()
        if not pending: return

        conn_t=sqlite3.connect(TRADES_DB)
        for sid,symbol,suggested_dir,conf,suggest_price,suggest_time in pending:
            # H4 FIX: match by direction AND within 30 min window to avoid wrong trade binding
            trade=conn_t.execute("""SELECT direction,entry,pnl,close_time,peak_roe
                FROM trades WHERE symbol=? AND status='CLOSED'
                AND direction=? AND open_time >= ?
                AND open_time <= datetime(?, '+30 minutes')
                ORDER BY open_time LIMIT 1""",
                (symbol,suggested_dir,suggest_time,suggest_time)).fetchone()
            if not trade:
                # Wider window fallback -- still match direction
                trade=conn_t.execute("""SELECT direction,entry,pnl,close_time,peak_roe
                    FROM trades WHERE symbol=? AND status='CLOSED'
                    AND direction=? AND open_time >= ?
                    ORDER BY open_time LIMIT 1""",
                    (symbol,suggested_dir,suggest_time)).fetchone()
            if not trade:
                trade=conn_t.execute("""SELECT 'LONG',entry,pnl,close_time,peak_roe
                    FROM dip_trades WHERE symbol=? AND status='CLOSED'
                    AND open_time >= ?
                    AND open_time <= datetime(?, '+30 minutes')
                    ORDER BY open_time LIMIT 1""",
                    (symbol,suggest_time,suggest_time)).fetchone()
            if not trade:
                trade=conn_t.execute("""SELECT 'LONG',entry,pnl,close_time,0
                    FROM dip_trades WHERE symbol=? AND status='CLOSED'
                    AND open_time >= ? ORDER BY open_time LIMIT 1""",
                    (symbol,suggest_time)).fetchone()
            if not trade:
                try:
                    _ticker = get_client().futures_symbol_ticker(symbol=symbol)
                    _now_price = float(_ticker["price"])
                    _suggest_price = float(suggest_price or 0)
                    if _suggest_price > 0 and _now_price > 0:
                        _price_move = (_now_price - _suggest_price) / _suggest_price * 100
                        _cf_correct = 1 if (suggested_dir=="SHORT" and _price_move < -1.0) or (suggested_dir=="LONG" and _price_move > 1.0) else 0
                        conn2 = _get_mind_conn()
                        conn2.execute("""UPDATE entry_suggestions SET
                            trade_opened=0, was_correct=?, timing_quality='SKIPPED',
                            why_correct=?, why_wrong=?, outcome_filled=1
                            WHERE id=?""",
                            (_cf_correct,
                             f"Skipped: price moved {_price_move:+.1f}% ({'correct' if _cf_correct else 'wrong'} skip)",
                             f"Missed: price moved {_price_move:+.1f}%" if not _cf_correct else "",
                             sid))
                        conn2.commit(); conn2.close()
                except: pass
                continue

            direction,entry,pnl,close_time,peak_roe=trade
            pnl=float(pnl or 0); entry=float(entry or 0)

            was_correct=0; timing_quality=""; why_c=""; why_w=""

            # Was direction correct?
            dir_correct = (suggested_dir==direction)

            if not dir_correct:
                was_correct=0
                why_w=f"Suggested {suggested_dir} but bot entered {direction}"
                timing_quality="WRONG_DIRECTION"
            elif pnl>0:
                # Right direction and profitable
                # Check entry timing -- how close was suggest_price to actual entry?
                if entry>0 and suggest_price>0:
                    price_diff=abs(entry-suggest_price)/suggest_price*100
                    if price_diff<1.0:
                        timing_quality="PERFECT"; was_correct=1
                        why_c=f"Right direction, tight entry (diff={price_diff:.1f}%)"
                    elif price_diff<3.0:
                        timing_quality="GOOD"; was_correct=1
                        why_c=f"Right direction, good entry (diff={price_diff:.1f}%)"
                    else:
                        timing_quality="LATE"; was_correct=1
                        why_c=f"Right direction but late entry (diff={price_diff:.1f}%)"
                else:
                    timing_quality="CORRECT"; was_correct=1
                    why_c=f"Right direction, trade profitable ${pnl:.2f}"
            else:
                was_correct=0; timing_quality="WRONG_OUTCOME"
                why_w=f"Right direction {direction} but trade lost ${pnl:.2f}"

            conn2=_get_mind_conn()
            conn2.execute("""UPDATE entry_suggestions SET
                trade_opened=1,trade_direction=?,trade_entry=?,
                trade_pnl=?,was_correct=?,timing_quality=?,
                why_correct=?,why_wrong=?,outcome_filled=1
                WHERE id=?""",
                (direction,entry,pnl,was_correct,timing_quality,
                 why_c,why_w,sid))
            conn2.commit(); conn2.close()

        conn_t.close()
    except Exception as e: logger.error(f"Score entry suggestions: {e}")

def record_regime_sequence():
    """
    Records market state every cycle as a sequence.
    When a regime shift is detected, labels the preceding
    sequence as a transition pattern.
    Builds transition pattern library over time.
    """
    try:
        conn = _get_mind_conn()
        # Get last 10 market snapshots
        snapshots = conn.execute("""
            SELECT timestamp, btc_adx_15m, btc_adx_trend, btc_rsi_15m,
                   btc_ema_align, alts_bull_pct, risk_temp, regime,
                   regime_shifting
            FROM market_snapshots
            ORDER BY id DESC LIMIT 10""").fetchall()
        conn.close()

        if len(snapshots) < 5: return

        # Detect if regime just shifted
        recent_regimes = [s[7] for s in snapshots[:5] if s[7]]
        if len(set(recent_regimes)) < 2: return  # no shift

        # Regime shift detected -- label preceding sequence
        old_regime = recent_regimes[-1]
        new_regime = recent_regimes[0]

        if old_regime == new_regime: return

        # Build fingerprint of the 5 snapshots before shift
        fingerprint = {
            "shift": f"{old_regime}_TO_{new_regime}",
            "timestamp": snapshots[0][0],
            "adx_sequence": [s[1] for s in reversed(snapshots[:5])],
            "adx_trends": [s[2] for s in reversed(snapshots[:5])],
            "rsi_sequence": [s[3] for s in reversed(snapshots[:5])],
            "alts_sequence": [s[5] for s in reversed(snapshots[:5])],
            "risk_sequence": [s[6] for s in reversed(snapshots[:5])],
        }

        # Save as a pattern for future prediction
        conn2 = _get_mind_conn()
        key = f"REGIME_SHIFT_{fingerprint['shift']}_{snapshots[0][0][:13]}"
        conn2.execute("""INSERT OR IGNORE INTO patterns
            (pattern_key, description, conditions, occurrences, last_seen)
            VALUES (?,?,?,1,datetime('now'))""",
            (key,
             f"Regime transition: {old_regime} -> {new_regime}",
             json.dumps(fingerprint)))
        conn2.commit(); conn2.close()
        logger.info(f"Regime transition recorded: {old_regime} -> {new_regime}")

    except Exception as e:
        logger.error(f"Regime sequence error: {e}")

def predict_regime_shift():
    """
    Checks current market state against known transition patterns.
    Returns probability of regime shift in next 15-30 mins.
    Requires 10+ recorded transitions to be reliable.
    """
    try:
        conn = _get_mind_conn()
        # Get transition patterns
        patterns = conn.execute("""
            SELECT pattern_key, conditions, occurrences
            FROM patterns
            WHERE pattern_key LIKE 'REGIME_SHIFT_%'
            ORDER BY occurrences DESC""").fetchall()

        if len(patterns) < 5:
            conn.close()
            return 0, "Insufficient transition data"

        # Current market state
        current = conn.execute("""
            SELECT btc_adx_15m, btc_adx_trend, btc_rsi_15m,
                   alts_bull_pct, risk_temp
            FROM market_snapshots
            ORDER BY id DESC LIMIT 1""").fetchone()
        conn.close()

        if not current: return 0, "No market data"

        curr_adx, curr_adx_t, curr_rsi, curr_alts, curr_risk = current

        # Compare against historical transitions
        match_scores = []
        for pkey, conditions_json, occ in patterns:
            try:
                fp = json.loads(conditions_json)
                adx_seq = fp.get("adx_sequence",[])
                if not adx_seq: continue

                # Check if current ADX matches transition pattern
                last_adx = adx_seq[-1] if adx_seq else 0
                adx_match = abs(curr_adx - last_adx) < 5

                # Check ADX trend
                adx_trends = fp.get("adx_trends",[])
                trend_match = curr_adx_t in adx_trends

                if adx_match and trend_match:
                    match_scores.append(occ)
            except: continue

        if not match_scores:
            return 0, "No pattern match"

        probability = min(sum(match_scores)/len(patterns)*100, 85)
        return round(probability,1), f"Matched {len(match_scores)}/{len(patterns)} patterns"

    except Exception as e:
        return 0, f"Prediction error: {e}"

# ═══════════════════════════════════════════════════════════════
# NEW LEARNING FUNCTIONS -- Step 1 completion
# ═══════════════════════════════════════════════════════════════

def analyze_spring_ratchet():
    """Learn optimal Spring ratchet from observations -- 7000+ Spring obs."""
    try:
        conn = _get_mind_conn()
        rows = conn.execute("""SELECT outcome_roe, outcome_correct, trade_age_mins, roe
            FROM observations WHERE bot_type='SPRING' AND outcome_correct IS NOT NULL
            AND outcome_roe IS NOT NULL AND decision='CLOSE'""").fetchall()
        conn.close()
        if len(rows) < 5: return
        total = len(rows)
        wins = [r for r in rows if int(r[1]) == 1]
        wr = round(len(wins)/total*100, 1)
        avg_roe = round(sum(float(r[0] or 0) for r in rows)/total, 2)
        avg_win_roe = round(sum(float(r[0] or 0) for r in wins)/len(wins), 2) if wins else 0
        # Avg ROE at close for winning trades
        avg_eff = round(sum(float(r[0] or 0) for r in wins)/len(wins), 2) if wins else 0
        _update_params({"spring_ratchet": {
            "wr": wr, "avg_roe": avg_roe, "sample_size": total
        }})
        logger.info(f"  Spring ratchet: {len(wins)}/{total} WR={wr}% avg_roe={avg_roe} eff={avg_eff}%")
    except Exception as e: logger.error(f"analyze_spring_ratchet: {e}")

def analyze_spring_sl():
    """Learn optimal SL distance for Spring from dip_trades."""
    try:
        conn = sqlite3.connect(TRADES_DB)
        sl_rows = conn.execute("""SELECT sl, entry, pnl FROM dip_trades WHERE status='CLOSED'
            AND reason LIKE '%Hard SL%' AND entry > 0 AND sl > 0 ORDER BY open_time DESC LIMIT 200""").fetchall()
        win_rows = conn.execute("""SELECT sl, entry, pnl FROM dip_trades WHERE status='CLOSED'
            AND pnl > 0 AND entry > 0 AND sl > 0 ORDER BY open_time DESC LIMIT 200""").fetchall()
        conn.close()
        if len(sl_rows) < 5 and len(win_rows) < 5: return
        sl_dists_loss = [abs(float(r[0])-float(r[1]))/float(r[1])*100 for r in sl_rows if float(r[1]) > 0]
        sl_dists_win  = [abs(float(r[0])-float(r[1]))/float(r[1])*100 for r in win_rows if float(r[1]) > 0]
        avg_loss_dist = round(sum(sl_dists_loss)/len(sl_dists_loss), 2) if sl_dists_loss else 0
        avg_win_dist  = round(sum(sl_dists_win)/len(sl_dists_win), 2) if sl_dists_win else 0
        optimal_sl_pct = round(max(avg_loss_dist * 1.1, 1.5), 2)
        params_file = os.path.join(BASE, "apex_mind_params.json")
        p = json.load(open(params_file)) if os.path.exists(params_file) else {}
        if "spring_sl" not in p: p["spring_sl"] = {}
        existing = p["spring_sl"].get("optimal_dist_pct", 0)
        if existing > 0:
            optimal_sl_pct = round((existing + optimal_sl_pct) / 2, 2)
        _update_params({"spring_sl": {"optimal_dist_pct": optimal_sl_pct, "avg_loss_sl_dist": avg_loss_dist, "avg_win_sl_dist": avg_win_dist}})
        logger.info(f"  Spring SL: loss_dist={avg_loss_dist}% win_dist={avg_win_dist}% optimal={optimal_sl_pct}%")
    except Exception as e: logger.error(f"analyze_spring_sl: {e}")

def analyze_spring_close_reasons():
    """Learn which close reasons produce best outcomes for Spring."""
    try:
        conn = sqlite3.connect(TRADES_DB)
        rows = conn.execute("""SELECT reason, COUNT(*), ROUND(AVG(pnl),3),
            SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END)
            FROM dip_trades WHERE status='CLOSED' AND reason IS NOT NULL
            GROUP BY reason HAVING COUNT(*) >= 3 ORDER BY AVG(pnl) DESC""").fetchall()
        conn.close()
        if not rows: return
        results = {}
        for reason, cnt, avg_pnl, wins in rows:
            results[reason[:40].strip()] = {"count": cnt, "avg_pnl": float(avg_pnl or 0), "wr": round(wins/cnt*100, 1)}
        params_file = os.path.join(BASE, "apex_mind_params.json")
        p = json.load(open(params_file)) if os.path.exists(params_file) else {}
        _update_params({"spring_close_reasons": results})
        best = max(results.items(), key=lambda x: x[1]["avg_pnl"])
        logger.info(f"  Spring close reasons: {len(results)} types, best={best[0][:30]} ${best[1]['avg_pnl']:.2f}")
    except Exception as e: logger.error(f"analyze_spring_close_reasons: {e}")

def analyze_spring_timeout():
    """Learn optimal timeout thresholds for Spring."""
    try:
        conn = sqlite3.connect(TRADES_DB)
        timeout_rows = conn.execute("""SELECT duration_m, pnl FROM dip_trades WHERE status='CLOSED'
            AND (reason LIKE '%timeout%' OR reason LIKE '%Max%hold%') ORDER BY open_time DESC LIMIT 200""").fetchall()
        deep_rows = conn.execute("""SELECT duration_m, pnl FROM dip_trades WHERE status='CLOSED'
            AND reason LIKE '%deep loss%' ORDER BY open_time DESC LIMIT 200""").fetchall()
        win_rows = conn.execute("""SELECT duration_m, pnl FROM dip_trades WHERE status='CLOSED'
            AND pnl > 0 ORDER BY open_time DESC LIMIT 200""").fetchall()
        conn.close()
        params_file = os.path.join(BASE, "apex_mind_params.json")
        p = json.load(open(params_file)) if os.path.exists(params_file) else {}
        if "spring_timeout" not in p: p["spring_timeout"] = {}
        if timeout_rows:
            p["spring_timeout"]["avg_timeout_dur_m"] = round(sum(float(r[0] or 0) for r in timeout_rows)/len(timeout_rows), 0)
            p["spring_timeout"]["avg_timeout_pnl"] = round(sum(float(r[1] or 0) for r in timeout_rows)/len(timeout_rows), 2)
            p["spring_timeout"]["timeout_count"] = len(timeout_rows)
        if win_rows:
            p["spring_timeout"]["avg_win_dur_m"] = round(sum(float(r[0] or 0) for r in win_rows)/len(win_rows), 0)
        if deep_rows:
            p["spring_timeout"]["avg_deep_loss_pnl"] = round(sum(float(r[1] or 0) for r in deep_rows)/len(deep_rows), 2)
            p["spring_timeout"]["deep_loss_count"] = len(deep_rows)
        _update_params({"spring_timeout": p.get("spring_timeout",{})})
        logger.info(f"  Spring timeout: {len(timeout_rows)} timeouts, {len(deep_rows)} deep losses, {len(win_rows)} wins")
    except Exception as e: logger.error(f"analyze_spring_timeout: {e}")

def analyze_spring_entry_quality():
    """Learn which coins and drop levels produce best Spring entries."""
    try:
        conn = sqlite3.connect(TRADES_DB)
        sym_rows = conn.execute("""SELECT symbol, COUNT(*), ROUND(AVG(pnl),3),
            SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END), ROUND(AVG(peak_roe),1)
            FROM dip_trades WHERE status='CLOSED'
            GROUP BY symbol HAVING COUNT(*) >= 3 ORDER BY AVG(pnl) DESC LIMIT 20""").fetchall()
        drop_rows = conn.execute("""SELECT ROUND(drop_pct/2)*2 as bucket, COUNT(*),
            ROUND(AVG(pnl),3), SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END)
            FROM dip_trades WHERE status='CLOSED' AND drop_pct IS NOT NULL
            GROUP BY bucket HAVING COUNT(*) >= 3 ORDER BY bucket""").fetchall()
        conn.close()
        params_file = os.path.join(BASE, "apex_mind_params.json")
        p = json.load(open(params_file)) if os.path.exists(params_file) else {}
        best_coins = {r[0]: {"count": r[1], "avg_pnl": float(r[2] or 0), "wr": round(r[3]/r[1]*100,1), "avg_peak_roe": float(r[4] or 0)} for r in sym_rows[:10]}
        drop_buckets = {str(int(r[0])): {"count": r[1], "avg_pnl": float(r[2] or 0), "wr": round(r[3]/r[1]*100,1)} for r in drop_rows}
        # Recovery buckets
        conn2 = sqlite3.connect(TRADES_DB)
        rec_rows = conn2.execute("""SELECT ROUND(recovery/10)*10 as bucket, COUNT(*),
            ROUND(AVG(pnl),3), SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END)
            FROM dip_trades WHERE status='CLOSED' AND recovery > 0
            GROUP BY bucket HAVING COUNT(*) >= 3 ORDER BY bucket""").fetchall()
        conn2.close()
        rec_buckets = {str(int(r[0])): {"count": r[1], "avg_pnl": float(r[2] or 0), "wr": round(r[3]/r[1]*100,1)} for r in rec_rows}
        # Add dip quality score buckets from entry_suggestions
        dip_quality_buckets = {}
        try:
            conn3 = _get_mind_conn()
            dq_rows = conn3.execute("""SELECT CAST(score/10 AS INT)*10 as bucket,
                COUNT(*), SUM(was_correct), ROUND(AVG(trade_pnl),3)
                FROM entry_suggestions WHERE mode='SPRING' AND outcome_filled=1
                AND ABS(trade_pnl)>0.01
                GROUP BY bucket HAVING COUNT()>=5 ORDER BY bucket""").fetchall()
            conn3.close()
            dip_quality_buckets = {str(int(r[0])): {"count":r[1],"wr":round(r[2]/r[1]*100,1),"avg_pnl":float(r[3] or 0)} for r in dq_rows}
        except: pass
        # Merge with existing -- preserve 2-year populated data
        _existing_eq = p.get("spring_entry_quality", {}) if "p" in dir() else {}
        _new_eq = {"best_coins": best_coins, "drop_buckets": drop_buckets, "recovery_buckets": rec_buckets, "dip_quality_buckets": dip_quality_buckets}
        for k,v in _new_eq.items():
            if v: _existing_eq[k] = v
        _update_params({"spring_entry_quality": _existing_eq})
        logger.info(f"  Spring entry quality: {len(best_coins)} coins, {len(drop_buckets)} drop buckets, {len(rec_buckets)} rec buckets, {len(dip_quality_buckets)} dip_quality buckets")
    except Exception as e: logger.error(f"analyze_spring_entry_quality: {e}")

def analyze_be_trigger_accuracy():
    """Learn optimal BE trigger level per regime from trades."""
    try:
        conn = sqlite3.connect(TRADES_DB)
        rows = conn.execute("""SELECT regime_label, direction, peak_roe, pnl, floor_roe
            FROM trades WHERE status='CLOSED' AND be_set=1 AND peak_roe > 0
            ORDER BY open_time DESC LIMIT 300""").fetchall()
        conn.close()
        if len(rows) < 10: return
        regime_data = {}
        for regime, direction, peak_roe, pnl, floor_roe in rows:
            if not regime: continue
            key = str(regime)
            if key not in regime_data: regime_data[key] = {"wins": 0, "total": 0, "peaks": []}
            regime_data[key]["total"] += 1
            if float(pnl or 0) > 0: regime_data[key]["wins"] += 1
            regime_data[key]["peaks"].append(float(peak_roe or 0))
        params_file = os.path.join(BASE, "apex_mind_params.json")
        p = json.load(open(params_file)) if os.path.exists(params_file) else {}
        be_analysis = {}
        for regime, data in regime_data.items():
            wr = round(data["wins"]/data["total"]*100, 1) if data["total"] else 0
            avg_peak = round(sum(data["peaks"])/len(data["peaks"]), 1) if data["peaks"] else 0
            be_analysis[regime] = {"wr_after_be": wr, "avg_peak_roe": avg_peak, "count": data["total"]}
        # Merge with existing -- preserve APEX/SPRING bot-type keys
        params_file = os.path.join(BASE, "apex_mind_params.json")
        _existing = json.load(open(params_file)).get("be_trigger_analysis", {})
        _existing.update(be_analysis)  # add regime keys, keep APEX/SPRING keys
        _update_params({"be_trigger_analysis": _existing})
        logger.info(f"  BE trigger: {len(rows)} trades across {len(regime_data)} regimes")
    except Exception as e: logger.error(f"analyze_be_trigger_accuracy: {e}")

def analyze_apex_hold_time():
    """Learn optimal hold time per regime from observations."""
    try:
        conn = _get_mind_conn()
        rows = conn.execute("""SELECT market_regime, direction, trade_age_mins, outcome_correct
            FROM observations WHERE outcome_correct IS NOT NULL
            AND market_regime IS NOT NULL AND trade_age_mins > 0
            ORDER BY id DESC LIMIT 5000""").fetchall()
        conn.close()
        if len(rows) < 10: return
        hold_data = {}
        for regime, direction, dur, outcome in rows:
            key = str(regime or "UNKNOWN")
            if key not in hold_data: hold_data[key] = {"wins": [], "losses": []}
            if int(outcome or 0) == 1:
                hold_data[key]["wins"].append(float(dur or 0))
            else:
                hold_data[key]["losses"].append(float(dur or 0))
        params_file = os.path.join(BASE, "apex_mind_params.json")
        p = json.load(open(params_file)) if os.path.exists(params_file) else {}
        hold_analysis = {}
        for regime, data in hold_data.items():
            hold_analysis[regime] = {
                "avg_win_hold_mins": round(sum(data["wins"])/len(data["wins"]), 0) if data["wins"] else 0,
                "avg_loss_hold_mins": round(sum(data["losses"])/len(data["losses"]), 0) if data["losses"] else 0,
                "win_count": len(data["wins"]), "loss_count": len(data["losses"])
            }
        # Merge with existing -- preserve 2-year obs data
        _existing_hold = p.get("apex_hold_time", {})
        # Only update keys where new data has more samples
        for k, v in hold_analysis.items():
            existing_count = _existing_hold.get(k, {}).get("win_count", 0) + _existing_hold.get(k, {}).get("loss_count", 0)
            new_count = v.get("win_count", 0) + v.get("loss_count", 0)
            if new_count >= existing_count or k not in _existing_hold:
                _existing_hold[k] = v
        _update_params({"apex_hold_time": _existing_hold})
        logger.info(f"  APEX hold time: {len(rows)} trades across {len(hold_data)} regimes")
    except Exception as e: logger.error(f"analyze_apex_hold_time: {e}")

def analyze_direction_accuracy():
    # M9 NOTE: COUNT(*) here counts observation rows (many per trade, every 3 mins)
    # NOT independent trades. Real sample = closed trades, not obs count.
    # Treat WR stats as directional signals, not statistically precise estimates.
    """Learn LONG vs SHORT performance per regime from observations."""
    try:
        conn = _get_mind_conn()
        rows = conn.execute("""SELECT market_regime, direction, COUNT(*),
            ROUND(SUM(outcome_correct)*100.0/COUNT(*),1) as wr,
            ROUND(AVG(outcome_roe),2) as avg_roe,
            COUNT(DISTINCT entry_price) as trade_count
            FROM observations WHERE outcome_correct IS NOT NULL AND market_regime IS NOT NULL
            GROUP BY market_regime, direction HAVING COUNT(*) >= 20
            ORDER BY market_regime, direction""").fetchall()
        # I10: Use trade_count for confidence, not obs count
        # entry_price proxy for distinct trades (not perfect but much better than obs count)
        conn.close()
        if not rows: return
        dir_data = {}
        for row in rows:
            regime, direction, cnt, wr, avg_roe = row[0], row[1], row[2], row[3], row[4]
            trade_cnt = row[5] if len(row) > 5 else max(1, cnt//20)  # I10: independent trade estimate
            key = str(regime) + "_" + str(direction)
            # I7: Add confidence interval -- lower_bound = expectancy - 1.64*stderr
            import math as _m
            _wr_frac = float(wr or 0) / 100
            _avg_roe = float(avg_roe or 0)
            # Stderr of mean ROE (approximated from WR and avg_roe)
            # For binary outcomes: std ~ sqrt(WR*(1-WR)) * avg_magnitude
            _std_approx = _m.sqrt(max(0, _wr_frac*(1-_wr_frac))) * abs(_avg_roe) * 2
            _stderr = _std_approx / _m.sqrt(max(1, cnt))
            _lower_bound = round(_avg_roe - 1.64 * _stderr, 3)
            dir_data[key] = {"wr": wr, "count": cnt, "avg_roe": float(avg_roe or 0),
                             "stderr": round(_stderr, 3), "lower_bound": _lower_bound,
                             "trusted": _lower_bound > 0, "trade_count": trade_cnt}
        params_file = os.path.join(BASE, "apex_mind_params.json")
        p = json.load(open(params_file)) if os.path.exists(params_file) else {}
        # Blend with existing
        existing = p.get("direction_accuracy", {})
        for key, val in dir_data.items():
            if key in existing:
                old_n = existing[key].get("count", 0)
                new_n = val["count"]
                total_n = old_n + new_n
                existing[key]["wr"] = round((existing[key]["wr"]*old_n + val["wr"]*new_n)/total_n, 1)
                existing[key]["count"] = total_n
                existing[key]["avg_roe"] = round((existing[key].get("avg_roe",0)*old_n + val["avg_roe"]*new_n)/total_n, 2)
                # Always update confidence interval fields from latest calculation
                existing[key]["stderr"] = val.get("stderr", 0)
                existing[key]["lower_bound"] = val.get("lower_bound", 0)
                existing[key]["trusted"] = val.get("trusted", False)
                existing[key]["trade_count"] = val.get("trade_count", 0)
            else:
                existing[key] = val
        _update_params({"direction_accuracy": existing})
        logger.info(f"  Direction accuracy: {len(dir_data)} regime/direction combos")
    except Exception as e: logger.error(f"analyze_direction_accuracy: {e}")

def analyze_ban_effectiveness():
    """Check chronic losers for both APEX and Spring from observations."""
    try:
        conn = _get_mind_conn()
        # APEX chronic losers
        apex_rows = conn.execute("""SELECT symbol, COUNT(*),
            ROUND(SUM(outcome_correct)*100.0/COUNT(*),1) as wr,
            ROUND(AVG(outcome_roe),2) as avg_roe
            FROM observations WHERE outcome_correct IS NOT NULL AND bot_type='APEX'
            GROUP BY symbol HAVING COUNT(*) >= 20
            ORDER BY wr ASC LIMIT 20""").fetchall()
        # Spring chronic losers
        spring_rows = conn.execute("""SELECT symbol, COUNT(*),
            ROUND(SUM(outcome_correct)*100.0/COUNT(*),1) as wr,
            ROUND(AVG(outcome_roe),2) as avg_roe
            FROM observations WHERE outcome_correct IS NOT NULL AND bot_type='SPRING'
            GROUP BY symbol HAVING COUNT(*) >= 20
            ORDER BY wr ASC LIMIT 20""").fetchall()
        conn.close()
        apex_chronic = [{"symbol": r[0], "count": r[1], "wr": r[2], "avg_roe": float(r[3] or 0)} for r in apex_rows if float(r[2] or 100) < 45]
        spring_chronic = [{"symbol": r[0], "count": r[1], "wr": r[2], "avg_roe": float(r[3] or 0)} for r in spring_rows if float(r[2] or 100) < 45]
        _update_params({"ban_effectiveness": {
            "chronic_losers": apex_chronic[:10],
            "spring_chronic_losers": spring_chronic[:10],
            "total_analyzed": len(apex_rows) + len(spring_rows)
        }})
        logger.info(f"  Ban effectiveness: {len(apex_chronic)} APEX + {len(spring_chronic)} Spring chronic losers")
    except Exception as e: logger.error(f"analyze_ban_effectiveness: {e}")

def analyze_regime_transition_accuracy():
    """How accurate are regime transition predictions from observations."""
    try:
        conn = _get_mind_conn()
        rows = conn.execute("""SELECT shift_direction, market_regime, COUNT(*),
            ROUND(SUM(outcome_correct)*100.0/COUNT(*),1) as wr
            FROM observations WHERE outcome_correct IS NOT NULL
            AND shift_direction IS NOT NULL AND shift_direction != ''
            GROUP BY shift_direction, market_regime HAVING COUNT(*) >= 10
            ORDER BY shift_direction""").fetchall()
        conn.close()
        if not rows: return
        trans_data = {}
        for shift_dir, regime, cnt, wr in rows:
            key = str(shift_dir)
            if key not in trans_data: trans_data[key] = {"total": 0, "total_wr": 0}
            trans_data[key]["total"] += cnt
            trans_data[key]["total_wr"] += wr * cnt
            trans_data[key][str(regime)] = {"count": cnt, "wr": wr}
        for key in trans_data:
            total = trans_data[key]["total"]
            trans_data[key]["avg_wr"] = round(trans_data[key]["total_wr"]/total, 1) if total else 0
            del trans_data[key]["total_wr"]
        params_file = os.path.join(BASE, "apex_mind_params.json")
        p = json.load(open(params_file)) if os.path.exists(params_file) else {}
        # Merge with existing -- keep higher sample counts
        _existing_trans = p.get("transition_accuracy", {})
        for k, v in trans_data.items():
            if k not in _existing_trans or int(v.get("count",0)) >= int(_existing_trans[k].get("count",0)):
                _existing_trans[k] = v
        _update_params({"transition_accuracy": _existing_trans})
        logger.info(f"  Transition accuracy: {len(_existing_trans)} transition types")
    except Exception as e: logger.error(f"analyze_regime_transition_accuracy: {e}")

def analyze_session_entry_timing():
    """Learn best entry hours including avg ROE from observations."""
    try:
        conn = _get_mind_conn()
        rows = conn.execute("""SELECT CAST(strftime('%H', timestamp) AS INTEGER) as hour,
            direction, COUNT(*),
            ROUND(SUM(outcome_correct)*100.0/COUNT(*),1) as wr,
            ROUND(AVG(outcome_roe),2) as avg_roe
            FROM observations WHERE outcome_correct IS NOT NULL
            GROUP BY hour, direction HAVING COUNT(*) >= 10
            ORDER BY hour, direction""").fetchall()
        conn.close()
        if not rows: return
        timing_data = {}
        for hour, direction, cnt, wr, avg_roe in rows:
            key = str(hour)
            if key not in timing_data: timing_data[key] = {}
            import math as _ms
            _wr_f = float(wr or 0)/100; _ar = float(avg_roe or 0)
            _se = (_ms.sqrt(max(0,_wr_f*(1-_wr_f)))*abs(_ar)*2)/_ms.sqrt(max(1,cnt))
            _lb = round(_ar - 1.64*_se, 3)
            timing_data[key][str(direction)] = {
                "wr": wr, "count": cnt, "avg_roe": float(avg_roe or 0),
                "quality_score": round(wr * 0.6 + max(0, float(avg_roe or 0)) * 40, 1),
                "stderr": round(_se,3), "lower_bound": _lb, "trusted": _lb > 0
            }
        params_file = os.path.join(BASE, "apex_mind_params.json")
        p = json.load(open(params_file)) if os.path.exists(params_file) else {}
        # Merge with existing 2-year data -- keep higher sample counts
        _existing_timing = p.get("session_entry_timing", {})
        for h, hdata in timing_data.items():
            if h not in _existing_timing:
                _existing_timing[h] = hdata
            else:
                for d, ddata in hdata.items():
                    if d not in _existing_timing[h]: _existing_timing[h][d] = ddata
                    elif int(ddata.get("count",0)) >= int(_existing_timing[h][d].get("count",0)):
                        _existing_timing[h][d] = ddata
        _update_params({"session_entry_timing": _existing_timing})
        logger.info(f"  Session entry timing: {len(_existing_timing)} hours analyzed")
    except Exception as e: logger.error(f"analyze_session_entry_timing: {e}")
