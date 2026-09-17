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

**Three selective filters tested badly on the Sept tape — but the tape cannot
adjudicate any of them, and none shipped:** raising `nb_gap_min`, a
market-agreement gate, and a fee-aware bar. With **16 settled tickets, 3
winners and a −0.63/$ base rate, a filter dropping 4 tickets forfeits a winner
61% of the time by luck alone** (35% at 2 dropped, 79% at 6). "Tested badly"
is the default outcome here, not evidence. The agreement gate is still the
most clearly wrong of the three on mechanism — on 09-14 phoenix the model and
market agreed to 0.07°F and both ran 2.2°F cold, so consensus is not accuracy
— but none of the three should be re-proposed OR ruled out on this tape. Judge
them on shadow data once calibration is healthy (see the gap-bar note in Key
learnings — and note that the "inversion" there is a mechanism hypothesis on
n=2, not a measured effect).

*Audit residue, 2026-09-17 multi-agent pass (125 agents, 9 findings survived
3-lens adversarial verification, 62 refuted). Nothing below is fixed; three
are explicitly NOT worth fixing.*
- **REFUTED, do not re-investigate: there is no product mismatch.**
  `nb_calibration.json` reproduces from an independent previous-runs fetch of
  gfs_seamless+ecmwf_ifs025+icon_seamless at mean offset **+0.002F, r=1.000,
  bit-exact on 1442/1445 city-days**, once the reconstruction uses the live
  aggregation order (per-model daily max, THEN mean of 3 — not max of the
  hourly ensemble mean, which runs 0.31F cold and fooled the first pass). The
  live pipeline is +0.15F WARMER than the calibration substrate, n.s. once
  date-clustered, and the wrong sign to explain a cold failure.
- **The KDE shrink formula is wrong, and should be left alone for now.**
  `server.py` uses `sd/sqrt(sd^2+h^2)` where the variance-matched form is
  `sqrt(1-h^2/sd^2)`, over-dispersing by **+1.06% in sd** for every city. It
  lowered p_model by ~0.003 on all 16 live legs — i.e. it made the sleeve
  CONSERVATIVE, worth about −$3 across the window. Correcting it RAISES
  p_model and therefore fires more trades, so it cannot be fixed in isolation
  without re-deriving `nb_gap_min`. Fix it as part of a recalibration, never
  on its own.
- **The dip subsystem is inert.** `at_check_exits` excludes `strategy='nb'`,
  so `dip_flag` gates nothing that runs; the 667 dip rows on the losing week
  were pure write traffic. The 09-14 reference fix was correct but changed 0
  of 10 bans.
- **LATENT, fix before re-arming: `_nb_place` discards the 'unknown' fill
  status.** `at_confirm_fill` deliberately returns the FULL requested size on
  an API blip (fail-closed for the spend cap, correct). But `_nb_place` then
  computes `status = "filled" if filled >= count`, so 'unknown' becomes
  'filled'; `px_c` falls back to the IOC limit; and the row settles as a real
  position. One firing on a max-overlay entry fabricates ~$58 of PnL and
  poisons that ticker's dip and rung references for life. The forecast sleeve
  does NOT have this bug — it records `fill_status='unknown'` properly (4 such
  rows, all 2026-08-13). NB has never hit it (0 rows where
  avg_fill_price_c = paper_ask_c+3), and it CANNOT fire in SIM, since
  `_nb_place` returns before the confirm path when `is_live` is false.
- **The "losses were one-sided" observation is NOT significant.** 7 of 9
  distinct losing tickers had the realized high above the bracket, but the
  two-sided sign test gives **p=0.18**. Do not re-centre brackets on it. The
  cold-bias conclusion does not rest on this — it rests on the direct residual
  measurement (Sept −1.20F pooled vs −0.17/+0.06/−0.24 before) and the paired
  comparison against the market's implied high (t=+3.86).
- **`nb_signals` is ~50x pseudo-replicated** — 888 rows on the losing week
  were 17 ticker-days, 13 of them settled. Never treat its rows as
  independent observations.

*The best evidence the mechanism is real, and its limit.* The vintage placebo
in `analysis2/edge_highs/results.md:49-54` runs the identical rule on a STALE
forecast: fresh p1 +0.287/$ vs stale p2 **+0.045/$ (P0=0.40)**, with only 47
of 171 triggers overlapping, and d0_pre9 (next morning) +0.11 n.s. If the
signal were purely our own forecast error, the *worse* vintage should generate
more spurious triggers and lose more — p2 is measurably worse (max MAE 2.05°F
vs 1.66°F, paired t=12.9) yet earns ~0 rather than going negative. **Caveat,
and it is not resolvable from these numbers:** "a fresher run is more
accurate, so its disagreements are more often right" fits the same data and is
a model-quality story, not an information-timing one. Only the time structure
separates them — onset at run publication, holds to local midnight, collapses
after — and that is suggestive, not proof. None of it was measured on
September data, so it says nothing about whether the mechanism is alive now.
That is what the shadow period is for.

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
- **Changing the data under an analysis invalidates every statistic computed
  on it, and backfills are the dangerous case because they feel like
  correctness fixes rather than data changes.** The 2026-09-17 fee backfill
  was unambiguously a fix, and it silently broke a statistic computed hours
  earlier the same day (see the rho note below). Rerun anything downstream
  after a backfill, and when publishing a figure, say which vintage it was
  computed on.
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
  spec's last10-vs-last21 form fires 0 times across the nine city-days the
  sleeve actually traded (worst: miami 09-12 at 0.84F vs a 1F trigger;
  phoenix 0.52-0.78F all week), yet it DOES cross 1F for oklahoma_city
  (1.21F) and for miami on 09-16/17 — days with no position. It fires where
  there is nothing to protect and is silent where the money is.
- **Do NOT carry "never tighten the gap bar" into a healthy calibration.** In
  the healthy 5-month walk-forward, high gaps are the GOOD ones: gap 0.15-0.20
  returns −0.02/$ (n=22), 0.20-0.25 +0.16 (n=26), **0.25-0.30 +0.97 at a 71%
  win rate (n=17)**, 0.30+ +0.60 (n=34) — consistent with memory
  `d1-evening-edge` ("gaps ≥0.30 win 61% at +0.62/$"). On the Sept cold tape
  the 0.25+ bucket lost everything, but that is **n=2**, so it is a hypothesis,
  not a measurement.
  *Mechanism, if the inversion is real:* a systematically cold centre puts its
  largest apparent edges on exactly the brackets it is most wrong about, since
  the error and the apparent edge are one quantity with opposite signs — which
  would turn every gap-based filter anti-predictive at once.
  **CAUTION — an earlier version of this file quoted rho(gap, return/$) of
  +0.148 healthy vs −0.841 broken. BOTH WERE WRONG.** Two separate defects,
  and the second one is a trap that survives the obvious fix:
  1. *Tie mishandling.* Pre-fee, every loser returns exactly −1.0/$ because
     (0−ask)/ask = −1 regardless of ask, so 13 of 16 values tie. A rank
     routine that breaks ties by list order is not Spearman.
  2. *Fee contamination, post-backfill.* After `/admin/migrate-trade-fees` a
     total loss books −(cost + fee), so **return/$ = −1 − 0.07·(1−p)** — a
     deterministic function of the fill price. The losers UNTIE into a pure
     ask-ordering: among the 13 losers, **rho(ask, stored return/$) = +0.976**.
     That looks like legitimate variation, average-rank handling will not
     touch it, and it injects a **+0.144** artifact into the gap statistic.
  **Therefore: on all-or-nothing outcomes, never rank-correlate
  return-per-dollar — tied or untied. Use the binary win/loss indicator, or
  compare bucket means.** The safe figure, which two independent
  reconstructions agree on, is **rho(gap, win) = −0.293 at n=13 (critical
  0.55)**; healthy is +0.197 at n=99 (critical 0.198). Neither is significant.
  Note that a rerun against the backfilled table will NOT reproduce a
  pre-backfill return/$ figure — the fee changed the statistic, not the data.
  The inversion is a plausible mechanism supported by the bucket contrast, NOT
  a demonstrated correlation.
  Do not build a monitor on rho: walked forward over the live tape it never
  flips sign (−0.40 at 4 tickets drifting to −0.31 at 12), and at ~1.5
  triggers/day n=50 takes five weeks while the Sept break did its damage in
  five days. The divergence guard measures the INPUT and fired on phoenix in
  four days; rho measures the OUTPUT and inherits all the variance of a
  3-winners-in-13 stream. Where rho could earn its keep is as a RE-ARM
  criterion over accumulated shadow tickets, where n can actually grow — it is
  failure-mode-agnostic, so it would catch sigma being too tight or the
  freshness edge decaying, which the bias-only divergence guard cannot see.

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
