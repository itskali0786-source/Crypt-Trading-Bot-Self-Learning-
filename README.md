═══════════════════════════════════════════════════════════════
APEX MIND — COMPLETE BOT CONTEXT
Last Updated: 2026-06-06 (Session 8 + continued)
═══════════════════════════════════════════════════════════════

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
INFRASTRUCTURE
━━━━━
Main file:    apex_mind.py (~9500+ lines)
DBs:          apex_trades.db, apex_mind.db
Backfill DB:  backfill/backfill_results.db
Swap:         2GB active
Dashboard:    https://--------duckdns.org (Caddy port 443)
Watchdog:     ENABLED (crontab, every 5 min)

START/STOP COMMANDS:
cd ~/apex_bot && nohup python3 apex_mind.py >> apex_mind.log 2>&1 & echo $! > apex_mind.pid

RESTART (syntax check first):
python3 -c "import ast; ast.parse(open('/home/ubuntu/apex_bot/apex_mind.py').read()); print('Syntax OK')" && pkill -f apex_mind.py; sleep 3; cd ~/apex_bot && nohup python3 apex_mind.py >> apex_mind.log 2>&1 & echo $! > apex_mind.pid && echo "Bot PID: $(cat apex_mind.pid)"

BACKUP:
cd ~ && tar -czf apex_bot_BACKUP_$(date +%Y%m%d_%H%M).tar.gz --exclude='apex_bot/venv' --exclude='apex_bot/__pycache__' --exclude='apex_bot/*.log' --exclude='apex_bot/*.tar.gz' --exclude='apex_bot/backfill/data' apex_bot/ && ls -t apex_bot_BACKUP_*.tar.gz | tail -n +3 | xargs rm -f

BACKFILL RUN:
cd ~/apex_bot/backfill && python3 -u -c "
import os
os.environ['APEX_BACKFILL_MODE'] = '1'
os.environ.setdefault('BINANCE_API_KEY', 'backfill')
os.environ.setdefault('BINANCE_API_SECRET', 'backfill')
import sys
sys.path.insert(0, '/home/ubuntu/apex_bot/backfill')
sys.path.insert(1, '/home/ubuntu/apex_bot')
from simulate import run_simulation
run_simulation(start='2024-01-01', end='2025-05-31')
" >> /tmp/backfill_full.log 2>&1 &

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BOT ARCHITECTURE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

TWO STRATEGIES:
1. APEX Bot — directional momentum, LONG+SHORT, 5x leverage
2. Spring Bot — dip-buying, LONG only, 4x leverage

CORE PHILOSOPHY:
- Learns from outcomes, not hardcoded rules
- Observations → outcomes → learned params → better decisions
- Walk-forward validation with param snapshots

MAIN LOOP (run_cycle, ~90s):
1. analyze_market() → regime, slots, BTC data
2. get_open_trades() → monitor existing positions
3. execute_decision() → HOLD/TIGHTEN/CLOSE each trade
4. Scanner → score top 80 coins → entry signals
5. Entry execution → fill slots with qualified signals
6. Incremental learning every 3hrs, full learning every 24hrs

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
REGIME SYSTEM (FULLY CONSOLIDATED - Session 8)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SINGLE OWNER: _decide_regime_and_slots(result)
- Called LAST in _analyze_market_impl() before return
- Reads market_regime (set by cascade + UNSTALL)
- Sets: max_long, max_short, size_mult, spring_slots_max, market_regime_live

REGIME TABLE:
_REGIME_SLOT_TABLE = {
    "BULL_STRONG": (6, 1, 1.2, 12),  # max_long, max_short, size_mult, max_spring
    "BULL_WEAK":   (5, 2, 1.0, 10),
    "SIDEWAYS":    (2, 2, 0.6,  6),
    "BEAR":        (1, 5, 0.8,  2),
    "UNKNOWN":     (3, 2, 0.7,  5),
}
# When regime_uncertain=True (HMM disagrees): (3, 3, 0.6, 5) — symmetric

TRANSITION GRADIENTS:
- regime_shifting=True → interpolates between adjacent states
- Uses: alts_bull_pct, btc_rsi_1h, btc_adx_trend, btc_ema_align
- Learned thresholds from apex_mind_params.json (bear_to_bull_alts, bull_to_bear_alts)
- Path: BEAR → SIDEWAYS → BULL_WEAK → BULL_STRONG (and reverse)

REGIME DETECTION FLOW:
1. Cascade: BTC EMA/ADX/RSI/alts → live_regime
2. _regime_blend_cache (15-min buckets) → blend label
3. Probability scorer → regime_probs (BEAR/SIDEWAYS/BULL_WEAK/BULL_STRONG %)
4. UNSTALL: if dom_pct >= 40% AND margin >= 12 AND dom != blend → override
5. HMM: sets hmm_regime, hmm_agrees, regime_uncertain
6. _decide_regime_and_slots: final slots from regime

UNSTALL CACHE UPDATE:
When UNSTALL fires → updates _MARKET_CACHE["result"] immediately
→ Next analyze_market() call gets updated regime

REGIME LOG (every cycle):
[REGIME] blend=X evidence=Y (Z%) margin=M -> agree/OVERRIDE/weak

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
HMM (Hidden Markov Model)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Separate from cascade, runs independently
- Sets: hmm_regime, hmm_agrees, hmm_confidence
- regime_uncertain = True when: cert < 65% AND hmm disagrees
- When uncertain: symmetric 3L/3S slots, entry_bar_bonus=+5
- HMM CAUTION: size halved when cascade≠hmm
- Log: "HMM disagrees: cascade=X → hmm=Y (Z% certain)"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ENTRY SYSTEM
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SCANNER:
- Top 80 coins by: btc_diff × 2.0 + chg × 0.3 + vol_score - major_penalty
- Min volume: $3M quote volume
- Excludes: open positions, BTC majors (penalized)
- ThreadPoolExecutor (6 workers) — all 80 scanned in parallel

ENTRY GATES (LEARNED — not hardcoded):
- _bucket_key = f"{regime}_{direction}"
- Reads from regime_entry_analysis in apex_mind_params.json
- bucket_wr >= 65% + n >= 20 → easy gate (score=20, conf=gate-10)
- bucket_wr >= 55% + n >= 20 → decent gate (score=25, conf=gate-5)
- bucket_wr < 45% + n >= 20 → high gate (score=45, conf=gate+10)
- else → default gate (score=30, conf=gate)

POOL SYSTEM:
- _POOL_PER_SLOT = 5 (5 signals per available slot per direction)
- Long pool: max(_long_slots × 5, 5) signals
- Short pool: max(_short_slots × 5, 5) signals
- Spring pool: max(spring_slots × 5, 5) signals
- Direction pools independent — longs never blocked by shorts

EXECUTION ORDER (per signal):
1. _all_filled check → break if all slots full
2. Per-direction slot limits
3. Correlation throttle (max 3 per direction per cycle)
4. Repeat offender / cooldown check
5. Already open check
6. Stale check (>60s in queue → re-validate price)
7. _open_apex_trade() or _open_spring_trade()

SL COMPUTATION (compute_apex_sl helper):
- swing_low/high: last 10 candles
- ATR buffer: atr × base_mult × vol_adj
- base_mult: 1.2 (strong trend) / 1.6 (emerging) / 2.0 (chop)
- vol_adj: STABLE=0.85, MODERATE=1.0, HYPER_VOLATILE=1.3
- max_sl_pct: STABLE=5.5%, MODERATE=7%, HYPER_VOLATILE=10%
- THIN liquidity: min(max_sl_pct, 4%)
- VETO if sl_dist_pct > max_sl_pct

COIN CLASSIFICATION (classify_coin_type):
- Volatility: daily ATR% (STABLE<4%, MODERATE 4-7%, HYPER_VOLATILE>7%)
- FAST OVERRIDE: chg_24h >= 25% → HYPER_VOLATILE
- FAST OVERRIDE: chg_24h >= 15% AND STABLE → MODERATE
- Liquidity: vol_24h (THIN<$5M, NORMAL $5-50M, DEEP>$50M)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EXIT SYSTEM (check_ratchet_trail)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EXIT PRIORITY ORDER (top = highest priority):
a. Extreme loss force close (roe < -15%)
b. CONDITIONAL TIME-CUT (Fix 1 — THE $288 fix):
   - LONG: age >= 90min AND roe <= -3% AND peak_roe < 2%
   - SHORT: age >= 120min AND roe <= -3% AND peak_roe < 2%
   - Log: [TIME-CUT] symbol direction age=Xm roe=Y% peak=Z%
c. Hard timeout (max_hold_h, default 8H — backstop)
d. Early floor (be_set=True + roe <= floor_roe)
e. Ratchet floor (tiered: big>12%, mid 5-12%, small<5%)
f. BE exit

RATCHET TIERS:
- peak >= 12%: give_back_big = 5.0% (let big winners run)
- peak >= 5%:  give_back_mid = 3.0% (tighter — was leaking 60%)  [NOT YET APPLIED]
- peak < 5%:   give_back_small = 2.0%

EARLY FLOOR:
- Arms at peak_roe >= 3%: floor = peak × 0.4 (locks 40% of gain)
- Option A: if be_set + floor > 0 + roe <= floor → CLOSE
- Guard: if peak_roe <= 0 AND floor > 2 → ghost state, clear

GHOST STATE FIX:
- _ratchet_state.pop(symbol) on trade close (prevents stale state)
- Ghost state guard in check_ratchet_trail

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
KELLY SIZING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Formula: (equity × 0.30 / max_pos) × kelly_scale
- max_pos = max(regime_max_long + regime_max_short, 6) — consistent sizing
- kelly_scale = base × regime_mult × streak × conf_mult × dir_mult × session_mult
- Range: 0.7x - 1.5x
- Per-trade bounds: floor=2% of equity, cap=15% of equity
- Total exposure: 30% of equity

KNOWN ISSUE (Opus Finding #1):
- Kelly formula computes proper Kelly then overwrites with exposure model
- Actual sizing is flat 30%/positions × scale (not true Kelly)
- Accepted as-is for now — scheduled for cleanup

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LEARNING SYSTEM
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

INCREMENTAL (every 3hrs):
- Updates: pattern accuracy, coin personalities, session timing

FULL LEARNING (every 24hrs — learn_adaptive_params):
- Kelly base, regime multipliers
- Ratchet tier gaps (tier1-4)
- BE trigger, max hold time
- Transition thresholds (bear_to_bull_alts, bull_to_bear_alts)
- Spring dip quality weights
- Regime entry analysis (WR per regime+direction combo)
- Confidence calibration
- Direction accuracy
- Walk-forward validation + param snapshots

OUTCOME SCORING (on_trade_closed — PRIMARY):
- Writes outcome_roe, outcome_correct to observations
- Uses exact trade_db_id for matching (no fuzzy)
- fill_outcomes = FALLBACK ONLY

WALK-FORWARD:
- Param snapshots saved after each learn cycle
- Judged after 30+ forward trades
- Lower bound = expectancy - 1.64 × stderr (95% CI)
- GO-LIVE GATE: lower bound > 0%
- Current: lower bound ≈ -0.35% (NOT ready)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
COST MODEL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Taker fee: 0.04% × 2 (open + close)
- Slippage: 0.02% × 2
- Total cost: 0.12% of notional per trade
- Applied in: _apex_finalize_close, execute_decision

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RISK MANAGEMENT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

RISK TEMPERATURE (0-100):
- BTC ADX < 16: +25 (trend collapsed)
- BTC ADX falling: +15
- Open longs in BEAR: +20
- Various portfolio heat factors
- >= 80: entries blocked

ENTRY BLOCKS:
- Chronic loser: WR < 33% from observations (min 5 obs)
- Repeat offender: 2+ Hard SL hits in 3 days
- Cooldown: after close (15min same-dir, 5min any-dir)
- Spring ban: 2+ Hard SL in 1 day

CORRELATION GUARD:
- LONG + 3+ open longs + BTC RSI < 40 + alts < 45 → require conf+15%

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SPRING BOT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Dip-buy strategy: coin drops X%, recovers Y% → enter LONG
- Score: dip quality + recovery acceleration + MACD + RSI
- Spring slots: BEAR=2, SIDEWAYS=6, BULL_WEAK=10, BULL_STRONG=12
- SL: swing low method, max 5% for STABLE
- Same ratchet as APEX (check_ratchet_trail)
- Time-cut: NOT applied to Spring (by design)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BACKFILL SYSTEM
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Files: /home/ubuntu/apex_bot/backfill/
- config.py, download.py, store.py, universe.py, adapter.py, simulate.py
DB: backfill_results.db (bf_observations, bf_trades, bf_completed_months)

REALISTIC SL (Opus spec — implemented):
1. compute_apex_sl() helper: shared between live + backfill
2. Veto: trades whose structural SL > max_sl_pct not taken
3. Intrabar enforcement: bar low/high for SL detection
4. Fill at SL price on hard SL hit

RESULTS (realistic SL, with today's fixes):
2023: n=2664  WR=38.8% avgROE=-0.41% sumPnL=-$153 (NEGATIVE)
2024: n=11631 WR=45.5% avgROE=+0.19% sumPnL=+$310 (POSITIVE)
2025: n=1430  WR=44.5% avgROE=-0.41% sumPnL=-$82  (NEGATIVE, partial)
→ MIXED verdict — edge fragile/regime-dependent
→ 2024-2025 re-run with exit fixes in progress

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EXIT DIAGNOSTIC RESULTS (n=610, last 30 days)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EV = -$0.009/trade, WR=61.5%, avgWin=$0.754, avgLoss=$-1.227
Payoff ratio: 0.61

LOSERS ANATOMY:
- Small peak then lost: n=94, avgROE=-10.8%, sumPnL=-$138 (BIGGEST LEAK)
- Straight loss: n=72, avgROE=-11.8%, sumPnL=-$111
- Peaked>3% then lost: n=55, avgROE=-5.7%, sumPnL=-$39

CRITICAL: 219 trades held >240min → -$288 total (100% of loss $)
Fix 1 (time-cut) directly attacks this bucket.

HOURLY PATTERNS (best hours):
- 00:00, 03:00, 14:00, 15:00, 19:00, 21:00

HOURLY PATTERNS (worst hours):
- 01:00, 05:00, 07:00, 16:00 (SHORT -$18!), 20:00

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BUGS FIXED (Session 8)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. ✅ return→if/else in _scan_coin (was killing scanner mid-scan)
2. ✅ Regime soup → single owner _decide_regime_and_slots
3. ✅ market_regime_live aliased to market_regime
4. ✅ UNSTALL blend cache update + _MARKET_CACHE update
5. ✅ UNSTALL threshold lowered to 40% (from 50%)
6. ✅ _regime_blend_cache updated on UNSTALL
7. ✅ set_cooldown wrong param (minutes= → same_dir_mins=)
8. ✅ Spring recovery field (rec_pct not recovery)
9. ✅ _PARAM_CACHE not defined → added + global declaration
10. ✅ slot_ratios learning deleted (dead code)
11. ✅ Slot blend deleted → reads from owner
12. ✅ get_regime_suggestions() → thin forwarder reading market dict
13. ✅ transition_*_slots deleted → handled by _decide_regime_and_slots
14. ✅ Cross-direction sort deleted (buried longs under shorts)
15. ✅ Pool gating: 5 signals per slot per direction
16. ✅ Transition gradients: learned thresholds from params
17. ✅ Ghost ratchet state: _ratchet_state.pop on finalize_close
18. ✅ Wrong PnL in _apex_finalize_close: use DB exit price
19. ✅ Cooldown added to _apex_finalize_close
20. ✅ Ghost state guard (floor>2% but peak=0 → clear)
21. ✅ classify_coin_type: THIN ordering bug fixed
22. ✅ classify_coin_type: fast-volatility override (chg_24h)
23. ✅ compute_apex_sl() helper extracted (live + backfill share)
24. ✅ THIN SL ordering bug fixed (line 4811)
25. ✅ Kelly: market passed to get_regime_suggestions
26. ✅ Spring regime-based slots (BEAR=2, SIDEWAYS=6, etc)
27. ✅ HMM log: debug → info (visible every cycle)
28. ✅ fill_outcomes: trade_db_id exact matching
29. ✅ Learned entry gates (regime_entry_analysis WR)
30. ✅ BEAR_LONG gate: uses learned WR (was hardcoded high bar)
31. ✅ Kelly session filter removed (was 7am-10pm only)
32. ✅ Fix 1: Conditional time-cut on losers
33. ✅ Max pos floor (consistent sizing regardless of open count)
34. ✅ btc1h length guard (index 16 out of bounds)
35. ✅ Market refresh before scanner (gets latest regime)
36. ✅ "n" vs "count" key mismatch in learned gates

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PENDING / NEXT SESSION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

IMMEDIATE:
1. Re-run exit_diagnostic.py after 3-5 days (verify avgLoss dropped)
2. Read backfill 2024-2025 re-run results (with exit fixes)
3. Fix 2: Tiered give-back (after Fix 1 verified)

ARCHITECTURE:
4. Entry worker thread (independent of cycle)
5. Regime-based confidence gates per regime
6. Hour/direction based entry blocks (after 50+ trades/hour)
7. Dynamic edge detection: edge_score(hour, regime, direction) rolling 7-day
8. Fix 2: Tiered give-back (peak 5-12% → tighter trail)

CLEANUP (batch on calm day):
9. 97 bare except: pass → logged exceptions in money paths
10. Kelly formula: make honest (currently flat exposure, not Kelly)
11. Dead ratchet params notation
12. Fix transition interpolation overlap
13. Fix 4: Learned time-cut thresholds

GO-LIVE CHECKLIST:
- Lower bound > 0% (currently ≈ -0.35%)
- exit_diagnostic avgLoss < $0.60
- Backfill shows positive across 2+ eras
- Start very small (1-2% real capital)
- Maker orders

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
KEY FILES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
/home/ubuntu/apex_bot/apex_mind.py          — Main bot
/home/ubuntu/apex_bot/master_config.json    — Gates, limits, safety
/home/ubuntu/apex_bot/apex_mind_params.json — Learned params
/home/ubuntu/apex_bot/backfill/simulate.py  — Backfill simulator
/home/ubuntu/apex_bot/exit_diagnostic.py    — Exit analysis tool
/home/ubuntu/apex_bot/apex_trades.db        — Trade history
/home/ubuntu/apex_bot/apex_mind.db          — Observations, snapshots

CONFIDENCE GATES (master_config.json):
- apex_entry_min: 70%
- spring_entry_min: 50%
- apex_close_min: 80%
- apex_tighten_min: 40%

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CURRENT STATUS (2026-06-06 ~19:00 UTC)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Balance: APEX=$691 (PnL=-$9) | Spring=$307 (PnL=+$7) | Total≈$998
Regime: SIDEWAYS (BTC ADX=11, Alts=10-20%)
Val accuracy: 69.7%
Lower bound: ≈ -0.35% (needs to cross 0 before go-live)
Backfill: 2024-2025 re-run in progress (with exit fixes)
Bot: RUNNING (paper trading mode)
Watchdog: ENABLED

IMPORTANT WARNINGS:
- DO NOT go live until lower bound > 0%
- Backfill shows MIXED verdict (2024 positive, 2023+2025 negative)
- Exit fixes (time-cut) just deployed — need 3-5 days to measure
- Spring loses heavily in deep BEAR (now capped at 2 slots in BEAR)
═══════════════════════════════════════════════════════════════

16
