import os
os.environ["APEX_BACKFILL_MODE"] = "1"      # MUST precede any apex_bot import
os.environ.setdefault("BINANCE_API_KEY", "backfill")
os.environ.setdefault("BINANCE_API_SECRET", "backfill")

"""
Module 5 (COMPLETE): Walk-forward simulator with full position lifecycle.
score_coin_entry returns 4-TUPLE: (direction, score, reason, confidence).
check_ratchet_trail returns 5-TUPLE: (action, reason, floor, should_close, new_sl).
"""
import sys, os, json, logging, sqlite3
sys.path.insert(0, '/home/ubuntu/apex_bot/backfill')
sys.path.insert(1, '/home/ubuntu/apex_bot')
from config import STORE_DIR, START, END, TAKER_FEE, SLIPPAGE
from adapter import install_adapter, set_sim_time, _INTERVAL_MS, _load, _price_at

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger('SIMULATE')
import pandas as pd
from datetime import datetime, timezone

BACKFILL_DB = "/home/ubuntu/apex_bot/backfill/backfill_results.db"
APEX_MARGIN = 14.0
LEVERAGE    = 5.0
MAX_APEX    = 6
STEP_BARS   = 6  # 90-min cycle -- 2x faster

def init_db():
    c = sqlite3.connect(BACKFILL_DB)
    c.execute("""CREATE TABLE IF NOT EXISTS bf_observations(
        id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, symbol TEXT, bot_type TEXT,
        direction TEXT, entry_price REAL, market_regime TEXT, decision TEXT,
        decision_confidence REAL, score REAL,
        outcome_roe REAL, outcome_correct INTEGER, outcome_reason TEXT,
        outcome_filled INTEGER DEFAULT 0, source TEXT DEFAULT 'BACKFILL')""")
    c.execute("""CREATE TABLE IF NOT EXISTS bf_completed_months(
        month TEXT PRIMARY KEY, symbols_count INTEGER, obs_count INTEGER, trades_count INTEGER)""")
    c.execute("""CREATE TABLE IF NOT EXISTS bf_trades(
        id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, direction TEXT,
        entry REAL, exit REAL, size REAL, leverage REAL, sl REAL,
        open_time TEXT, close_time TEXT, pnl REAL, pnl_pct REAL, total_cost REAL,
        peak_roe REAL, reason TEXT, status TEXT DEFAULT 'CLOSED', source TEXT DEFAULT 'BACKFILL')""")
    c.commit(); c.close()

def next_bar_open(symbol, ts):
    df = _load(symbol, "15m")
    if df is None: return None
    future = df[df["time"] > ts]
    if len(future) == 0: return None
    return float(future.iloc[0]["open"])

def roe_at(symbol, ts, entry, direction, lev):
    p = _price_at(symbol, ts)
    if p <= 0 or entry <= 0: return 0.0, p
    move = (p - entry) / entry if direction == "LONG" else (entry - p) / entry
    return move * lev * 100, p

def preload_cache(symbols):
    for s in symbols:
        _load(s, "15m")

def run_simulation(symbols=None, start=START, end=END):
    log.info(f"Simulation: {start} -> {end}")
    init_db()
    am = install_adapter()

    open_trades = {}
    def _sim_open_trades():
        out = []
        for sym, t in open_trades.items():
            out.append({"id": 0, "symbol": sym, "direction": t["direction"],
                        "entry": t["entry"], "size": t["size"], "leverage": t["leverage"],
                        "sl": t["sl"], "open_time": t["open_time"], "peak_roe": t["peak_roe"],
                        "pattern": "", "floor_roe": t["floor_roe"], "be_set": t["be_set"],
                        "be_price": t["be_price"], "peak_price": t["peak_price"],
                        "current_price": t["current_price"], "roe": t["roe"],
                        "age_mins": t["age_mins"], "bot_type": "APEX",
                        "atr_15m": t.get("atr_15m", 0), "tid": None, "db_id": None})
        return out
    am.get_open_trades = _sim_open_trades

    universe = json.load(open(os.path.join(STORE_DIR, "universe.json")))
    months = sorted([m for m in universe if start[:7] <= m <= end[:7]])

    obs_count = trade_count = 0

    for month in months:
        # Resume: skip already completed months
        _done = sqlite3.connect(BACKFILL_DB).execute(
            "SELECT month FROM bf_completed_months WHERE month=?", (month,)).fetchone()
        if _done:
            log.info(f"  Skipping {month} (already completed)")
            continue
        month_syms = universe[month][:80]  # match live scanner universe size
        if symbols:
            month_syms = [s for s in month_syms if s in symbols]
        if not month_syms: continue
        preload_cache(month_syms)

        df_ts = None
        for s in month_syms:
            df_ts = _load(s, "15m")
            if df_ts is not None: break
        if df_ts is None: continue
        lo = int(pd.Timestamp(month).timestamp()*1000)
        hi = int((pd.Timestamp(month)+pd.DateOffset(months=1)).timestamp()*1000)
        timeline = df_ts[df_ts["time"].between(lo, hi)]["time"].tolist()
        log.info(f"Month {month}: {len(month_syms)} syms, {len(timeline)} bars")

        total_steps = len(timeline[::STEP_BARS])
        for step_i, ts in enumerate(timeline[::STEP_BARS]):
            if step_i % 100 == 0:
                log.info(f"  {month} step {step_i}/{total_steps} open={len(open_trades)}")
            set_sim_time(ts)
            now_str = datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            # 1) MANAGE OPEN POSITIONS
            for sym in list(open_trades.keys()):
                t = open_trades[sym]
                roe, p = roe_at(sym, ts, t["entry"], t["direction"], t["leverage"])
                t["roe"] = roe; t["current_price"] = p
                t["peak_roe"] = max(t["peak_roe"], roe)
                t["age_mins"] = (ts - t["_open_ts"]) / 60000.0
                action, reason, floor, should_close, new_sl = am.check_ratchet_trail(t)
                # Step 3: intrabar SL enforcement using bar low/high
                try:
                    _bars = am.fetch(sym, "15m", 2)
                    _bar_low  = float(_bars.iloc[-1]["low"])  if _bars is not None and len(_bars)>=1 else p
                    _bar_high = float(_bars.iloc[-1]["high"]) if _bars is not None and len(_bars)>=1 else p
                except Exception: _bar_low = p; _bar_high = p
                hard_sl_hit = (t["direction"]=="LONG"  and _bar_low  <= t["sl"]) or \
                              (t["direction"]=="SHORT" and _bar_high >= t["sl"])
                if should_close or hard_sl_hit:

                    close_reason = "Hard SL" if hard_sl_hit and not should_close else reason
                    fill = t["sl"] if hard_sl_hit else (next_bar_open(sym, ts) or p)
                    move = (fill-t["entry"])/t["entry"] if t["direction"]=="LONG" else (t["entry"]-fill)/t["entry"]
                    realized_roe = move * t["leverage"] * 100
                    notional = t["size"] * t["leverage"]
                    cost = notional*TAKER_FEE*2 + notional*SLIPPAGE*2
                    cost_roe = (cost / t["size"]) * 100
                    realized_roe -= cost_roe
                    pnl = t["size"] * (realized_roe/100)
                    was_correct = 1 if realized_roe > 0 else 0
                    c = sqlite3.connect(BACKFILL_DB)
                    c.execute("""INSERT INTO bf_trades(symbol,direction,entry,exit,size,leverage,sl,
                        open_time,close_time,pnl,pnl_pct,total_cost,peak_roe,reason,status,source)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'CLOSED','BACKFILL')""",
                        (sym,t["direction"],t["entry"],fill,t["size"],t["leverage"],t["sl"],
                         t["open_time"],now_str,round(pnl,4),round(realized_roe,2),
                         round(cost,4),round(t["peak_roe"],2),close_reason[:50]))
                    c.execute("""UPDATE bf_observations SET outcome_roe=?,outcome_correct=?,
                        outcome_reason=?,outcome_filled=1
                        WHERE symbol=? AND outcome_filled=0 AND bot_type='APEX'""",
                        (round(realized_roe,2),was_correct,close_reason[:50],sym))
                    c.commit(); c.close()
                    trade_count += 1
                    del open_trades[sym]

            # 2) SCAN FOR ENTRIES
            try:
                market = am.analyze_market()
            except Exception as e:
                log.debug(f"analyze_market {ts}: {e}"); market = None
            if not market: continue
            if len(open_trades) >= MAX_APEX: continue

            for sym in month_syms:
                if sym in open_trades: continue
                if len(open_trades) >= MAX_APEX: break
                try:
                    direction, score, reason, conf = am.score_coin_entry(sym, market, "APEX")
                except Exception: continue
                if not direction: continue
                if not (score >= 25 and conf >= 70): continue
                fill = next_bar_open(sym, ts)
                if not fill or fill <= 0: continue
                # Step 2: use real SL + veto (Opus realistic-SL spec)
                try:
                    df15_sl = am.fetch(sym, "15m", 30)
                    if df15_sl is not None and len(df15_sl) >= 12:
                        df15_sl = am.add_inds(df15_sl)
                        _ct, _liq = am.classify_coin_type(sym)
                        sl, _sl_dist, _vetoed = am.compute_apex_sl(sym, direction, fill, market, df15_sl, _ct, _liq)
                        if _vetoed:
                            continue  # live would skip -- backfill skips too
                    else:
                        sl = round(fill*0.97,8) if direction=="LONG" else round(fill*1.03,8)
                except Exception:
                    sl = round(fill*0.97,8) if direction=="LONG" else round(fill*1.03,8)
                open_trades[sym] = {
                    "direction":direction,"entry":fill,"size":APEX_MARGIN,"leverage":LEVERAGE,
                    "sl":sl,"open_time":now_str,"_open_ts":ts,"peak_roe":0.0,"roe":0.0,
                    "current_price":fill,"age_mins":0.0,"floor_roe":0.0,"be_set":False,
                    "be_price":0.0,"peak_price":fill,"atr_15m":0.0}
                c = sqlite3.connect(BACKFILL_DB)
                c.execute("""INSERT INTO bf_observations(timestamp,symbol,bot_type,direction,
                    entry_price,market_regime,decision,decision_confidence,score,
                    outcome_filled,source) VALUES(?,?,?,?,?,?,?,?,?,0,'BACKFILL')""",
                    (now_str,sym,"APEX",direction,fill,market.get("market_regime"),
                     "ENTRY",conf,score))
                c.commit(); c.close()
                obs_count += 1

        # Mark month complete for resume
        _mc = sqlite3.connect(BACKFILL_DB)
        _mc.execute("INSERT OR REPLACE INTO bf_completed_months VALUES(?,?,?,?)",
            (month, len(month_syms), obs_count, trade_count))
        _mc.commit(); _mc.close()
        log.info(f"  {month}: obs={obs_count} closed_trades={trade_count} open={len(open_trades)} [SAVED]")

    log.info(f"DONE: {obs_count} observations, {trade_count} closed trades -> {BACKFILL_DB}")

if __name__ == "__main__":
    syms = sys.argv[1:] if len(sys.argv) > 1 else ["BTCUSDT"]
    run_simulation(symbols=syms, start="2025-01-01", end="2025-01-31")
    c = sqlite3.connect(BACKFILL_DB)
    n_o = c.execute("SELECT COUNT(*) FROM bf_observations").fetchone()[0]
    n_t = c.execute("SELECT COUNT(*) FROM bf_trades").fetchone()[0]
    filled = c.execute("SELECT COUNT(*) FROM bf_observations WHERE outcome_filled=1").fetchone()[0]
    wr = c.execute("SELECT ROUND(AVG(CASE WHEN pnl>0 THEN 1.0 ELSE 0 END)*100,1) FROM bf_trades").fetchone()[0]
    print(f"obs={n_o} (filled={filled}) trades={n_t} WR={wr}%")
    c.close()
