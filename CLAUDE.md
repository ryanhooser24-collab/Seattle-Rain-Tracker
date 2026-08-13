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

- Arm: `GET /auto-trader/go-live` · Kill: `GET /auto-trader/kill`
- Config: `GET/POST /auto-trader/config`
- State: `GET /debug/live` · Results: `GET /debug/live-results`
- Current settings (2026-08-13): `daily_cap_dollars: 25`, `max_per_fill: 5`,
  `min_fill_dollars: 1`, whitelist = las_vegas, denver, phoenix, houston,
  boston, san_francisco. DC dropped (−10¢/$ YES-only over 47 paper bets);
  SF added (+63¢/$, sigma clamp raised for it 2026-08-13). Caps raised
  before first-ever live fill — pipeline was unproven at the time; watch
  early fills/slippage in /debug/live-results.
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

## Dashboard

Tabs: Scanner · Rainfall Tracker · Positions · Live · Analysis.
(Auto-trader, History, Backtest tabs were removed; advanced knobs live in the
Live tab's collapsible Advanced section.)

Rule: dashboard changes get a visual preview/mockup for approval before
pushing. Server-only changes don't need mockups.

## Working style (Ryan's rules)

- Terse replies. Lead with the answer/result. No step-by-step walkthroughs
  unless explicitly asked. No plan narration before acting — do it, report it.
- Debug, audit, and test thoroughly before committing anything.
- Analysis: short, results-focused.
- Don't agree just to agree — push back with reasons when warranted.
- Full copyable URLs, never relative paths.
