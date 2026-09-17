# Seattle Rain Tracker — Kalshi Weather Trading System

Automated weather-derivative trading on Kalshi prediction markets. Bets daily
high-temperature bracket markets across ~17 US cities using GFS + ECMWF model
forecasts, per-city bias/sigma correction, Kelly sizing, and A/B/C signal
grading. Started as a Seattle rainfall tracker (that tab still exists).

## Stack & deploy

- Single-file Python HTTP server: `server.py` (no framework, `http.server`)
- Frontend: `dashboard.html` (single file, vanilla JS, DM Mono / cream style)
- PostgreSQL on Railway; app auto-deploys from `main` on push
- Live app: https://seattle-rainfall-production.up.railway.app

**Deploy = git push to main.** Railway picks it up automatically (1–2 min).
Always run `python3 -m py_compile server.py` before committing. For
dashboard.html, syntax-check every `<script>` block and verify tag balance.

## Live trading (REAL MONEY — be careful)

The auto-trader places real Kalshi orders when armed. Config is DB-backed and
hot-swappable — no redeploy needed for config changes.

**Night Before (`strategy='nb'`) — DISARMED TO SIM 2026-09-17 after a
−$271.75 drawdown. Do not re-arm without clearing the bar below.** D-1
evening: buy YES on tomorrow's high brackets 25–60¢ when the 3-model
ensemble's P(win) beats the ask by ≥0.15; $50 units, rise top-ups, no new
signals after local midnight. Backtest +0.287/$ (N=111, referee-verified, see
memory `d1-evening-edge`). Config keys `nb_*`; arm via the Live tab's NIGHT
BEFORE subtab (OFF/SIM/LIVE).

*Why it was disarmed (2026-09-11..15, 13 settled live tickets, 2 wins):* the
ensemble turned cold in September (pooled D-1 bias −1.20°F vs −0.17/+0.06/−0.24
in Jun/Jul/Aug) and the flat rolling-21 bias could not track it — phoenix was
corrected by −1.91°F against a realised −4.15°F. The market did not change:
its Brier is flat all four months and its implied high was near-unbiased
(−0.12°F, MAE 1.17) while the model centre ran −0.59°F cold at MAE 1.65
(paired t=+3.86). The model is rejected binomially (3 wins in 14 vs its own
p̄=0.497, p=0.030); the market is not (vs ask 0.299, p=0.357). Full write-up in
memory `nb-edge-time-stability`, which also records that the pooled +0.29/$
hides a 1st-half +0.73 vs 2nd-half −0.06 split.

**RE-ARM BAR — all four, or it stays in SIM:**
1. Pooled D-1 ensemble bias back within ±0.5°F for **10 consecutive days**, and
   no traded city showing a regime divergence above 1.0°F.
2. **≥30 shadow tickets** logged post-2026-09-17 under the current gates, with
   cumulative shadow EV/$ > 0 (EV per dollar, not win rate, not raw ROI).
3. Shadow tickets spread over **≥8 distinct target dates and ≥4 cities** — a
   single heat dome is one observation, not eight. 66% of the losing week's
   stake sat in phoenix+las_vegas+austin.
4. Re-arm at **half size** (`nb_unit_dollars` 25) for the first 10 live days.

`nb_dd_stop_dollars` (150) auto-disarms to SIM on a trailing-7-day breach and
leaves `nb_enabled` on so shadow logging survives. Re-arming is always manual.

*Known interaction, by design, not a bug:* the breaker reads realised PnL by
**target date**, so the September drawdown stays inside its window for a
while. Arming before **2026-09-22** means the breaker trips on the very next
cycle and puts the sleeve straight back into SIM with only a KILL log line to
show for it. Trailing-7d by as-of date: 09-17 −$261.92, 09-19 −$282.62,
09-20 −$243.67, 09-21 −$215.59, **09-22 −$77.65 (first day inside the
threshold)**, 09-23 +$9.84. This is aligned with the re-arm bar above, which
needs ~10 clean days anyway — but do not mistake the auto-disarm for a new
failure if you arm early.

**Two filters that look right and are REFUTED by the live tape — do not add
them back without new evidence:** raising `nb_gap_min` (gap≥0.25 leaves 2
tickets at −1.000/$; the week's largest gap was its largest loss) and a
market-agreement gate (worse at every threshold — on 09-14 phoenix the model
and the market agreed to 0.07°F and both were 2.2°F cold).

The forecast, tail, and gap sleeves are retired
(tail/gap edges were data-bug artifacts — memory `market-efficiency-audit-2026-09`;
forecast +24% was the T off-by-one). All four data bugs are fixed in code+DB.

- Arm: `GET /auto-trader/go-live` · Kill: `GET /auto-trader/kill`
- Config: `GET/POST /auto-trader/config`
- State: `GET /debug/live` · Results: `GET /debug/live-results`
- Current settings (2026-08-13): `daily_cap_dollars: 75`, `max_per_ticker: 25`
  (a true cross-cycle cap since 030e8df — enforced against live_trades
  history; a ticker embeds its date so this is per-ticker-per-day),
  `max_per_fill: 5`, `min_fill_dollars: 1`, whitelist = las_vegas, denver,
  phoenix, houston, boston, san_francisco. DC dropped (−10¢/$ YES-only over
  47 paper bets); SF added (+63¢/$, sigma clamp raised 2026-08-13).
  First live fills 2026-08-13: 43x KXHIGHTLV-26AUG14-B91.5 @ ~31¢, 100%
  fill rate, 0 slippage. Daily cap rolls at UTC midnight (5pm Vegas) —
  known quirk, accepted for now.
- Sizing loop per ticker: re-fetch BBO → re-grade at new price → stop when
  grade < min_grade; fill = min(book at ask, max_per_fill, kelly remaining);
  fills below min_fill_dollars stop the loop. min_fill must be ≤ max_per_fill
  or nothing ever trades.
- Env vars (Railway): `KALSHI_KEY_ID`, `KALSHI_PRIVATE_KEY`, `QUERY_TOKEN`

Never change live-trading behavior, caps, or sizing logic without being
explicitly asked. Never arm/disarm without an explicit instruction.

## DB access

- `POST /admin/query` with `{"token": <QUERY_TOKEN>, "sql": "SELECT ..."}` —
  SELECT-only.
- Schema migrations MUST be standalone autocommit endpoints
  (`/admin/migrate-*` pattern), never DDL bundled inside a shared
  transaction — silent rollbacks were a recurring failure mode.
- `GET /admin/migrate-live-trades?token=...` applies live_trades columns
  eagerly.
- `GET /admin/migrate-trade-fees?token=...` recomputes `live_trades.pnl` under
  the correct Kalshi fee (`ceil(0.07*C*P*(1-P))` charged at execution on wins
  AND losses). Idempotent — recomputes from fill data, never from prior pnl.
  Run 2026-09-17: 472 rows, nb −$15.51 (the other three sleeves had already
  been rescored and moved by ≤$0.10).

## Roadmap (3 tiers — NO NEW STRATEGIES until a tier completes)

1. **Tier 1 (current):** accumulate ~150+ settled calibration_snapshots;
   per-city bias + sigma calibration for GFS/ECMWF/blend by horizon (d0/d1);
   validate `mkt_rank_conf`; identify trustworthy vs blacklist cities.
2. **Tier 2:** backtest engine on `price_history`; A/B variants; timing edge.
3. **Tier 3:** 30-day live min-size test, then scale.

## Key learnings (do not relearn these the hard way)

- EV per dollar is the metric, not win rate or raw ROI.
- Sigma was ~65% too narrow globally before per-city calibration (bias +
  sigma factor, shrinkage to global means, clamped) — that fix is core to v2.
- Combo rows poison calibration SQL (inflate z-std) — always filter them.
- `fillable_a` from `/orderbook` is structurally unreliable: it only shows
  resting limit orders. Market-maker liquidity appears only in `/markets` BBO.
- NO-side betting: tested and dead on raw + corrected model, train + test.
  Don't revisit.
- Rule changes (blacklists, multipliers) wait for sufficient sample size.
- Slippage break-even varies per bet with its edge — no universal cents figure.
- `hours_to_cutoff` is logged but never gates or sizes trades.
- Fee conventions diverged for months: the settle path booked 2%-of-payout on
  winners and NOTHING on losers, while the entry gate compares a fee-FREE edge
  to `nb_gap_min`. Settlement is fixed; the entry gate is still fee-free, so
  `nb_gap_min` is not an EV threshold and backtests quoting it are flattered by
  ~3-4c/$ at 25-40c entries.
- A regime guard must measure drift against SELF-EXTENDED (live) residuals
  only. Comparing two windows of the combined list cannot work while the
  shipped calibration file is ~96% of it — both windows move together. The
  spec's last10-vs-last21 form never fired once on the losing week.
- Bigger model-vs-market disagreement is NOT a better bet. When the forecast is
  biased, the largest gaps are the largest errors.

## Dashboard

Tabs: Scanner · Rainfall Tracker · Positions · Live · Analysis.
Live tab has strategy subtabs: FORECAST BASED · NIGHT BEFORE.
(Auto-trader, History, Backtest tabs were removed; the "Next Degree Up" subtab
was removed when the gap sleeve was retired.)

Rule: dashboard changes get a visual preview/mockup for approval before
pushing. Server-only changes don't need mockups.

## Working style (Ryan's rules)

- Terse replies. Lead with the answer/result. No step-by-step walkthroughs
  unless explicitly asked. No plan narration before acting — do it, report it.
- Debug, audit, and test thoroughly before committing anything.
- Analysis: short, results-focused.
- Don't agree just to agree — push back with reasons when warranted.
- Full copyable URLs, never relative paths.
