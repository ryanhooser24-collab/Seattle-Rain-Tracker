# Updated: 2026-04-30 05:05 UTC
"""
Seattle Rain Kalshi Tracker - Local Server
==========================================
Setup:
  1. pip3 install requests beautifulsoup4
  2. Add your Kalshi API key below
  3. Run: python3 server.py
  4. Open dashboard.html in your browser

The server runs at http://localhost:8765
"""

import json
import os
import re
import traceback
import socketserver
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

class ThreadingHTTPServer(socketserver.ThreadingMixIn, HTTPServer):
    """Handle each HTTP request in a separate thread."""
    daemon_threads = True
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date

try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

# ── CONFIG ────────────────────────────────────────────────────────────────────
KALSHI_KEY_ID     = os.environ.get("KALSHI_KEY_ID", "") or os.environ.get("KALSHI_API_KEY", "")
KALSHI_PRIVATE_KEY = os.environ.get("KALSHI_PRIVATE_KEY", "")

def kalshi_auth_headers(method, path):
    """Generate RSA-signed headers for Kalshi API v2."""
    import datetime, base64
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.hazmat.backends import default_backend

    ts_ms = str(int(datetime.datetime.now().timestamp() * 1000))
    path_no_query = path.split("?")[0]
    msg = (ts_ms + method.upper() + path_no_query).encode("utf-8")

    # Normalize newlines — Railway may store literal \n as \\n
    pem = KALSHI_PRIVATE_KEY
    if "\\n" in pem:
        pem = pem.replace("\\n", "\n")
    if "BEGIN" not in pem:
        raise ValueError("KALSHI_PRIVATE_KEY missing PEM header")

    private_key = serialization.load_pem_private_key(
        pem.encode("utf-8"), password=None, backend=default_backend()
    )

    # Kalshi uses RSA-PSS with SHA256
    sig = private_key.sign(
        msg,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=32
        ),
        hashes.SHA256()
    )
    sig_b64 = base64.b64encode(sig).decode("utf-8")

    return {
        "KALSHI-ACCESS-KEY":       KALSHI_KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": sig_b64,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "Content-Type":            "application/json",
    }
PORT           = int(os.environ.get("PORT", 8765))
DATABASE_URL   = os.environ.get("DATABASE_URL", "")   # Railway Postgres

# Open-Meteo — free, no API key required, stable documented API
OM_DAILY_URL  = "https://api.open-meteo.com/v1/forecast"
OM_HOURLY_URL = "https://api.open-meteo.com/v1/forecast"
OM_PREV_URL   = "https://previous-runs-api.open-meteo.com/v1/forecast"

# ── MULTI-CITY CONFIG ─────────────────────────────────────────────────────────
# Each city needs:
#   icao_code     — ICAO airport code for WU (pinned to same station as Kalshi)
#   nws_site      — NWS office code for CLI report
#   nws_issuedby  — NWS station for CLI report
#   kalshi_series — Kalshi series ticker
#   lat/lon       — for Open-Meteo backtest queries
#   regime        — "frontal" | "frontal_seasonal" | "mediterranean" | "mixed"
#   tradeable_months — months where WU forecast is reliable enough to trade
#   days_in_month — callable returning days in month (31 for march etc)
CITIES = {
    "seattle": {
        "icao_code":     "KSEA",
        "nws_site":      "SEW",
        "nws_issuedby":  "SEA",
        "kalshi_series": "KXRAINSEAM",
        "lat": 47.441, "lon": -122.3,   "tz": "America/Los_Angeles",
        "regime":        "frontal",
        "tradeable_months": list(range(1, 13)),
        "label":         "Seattle, WA",
        "state": "WA", "iem_network": "WA_ASOS",
    },
    "portland": {
        "icao_code":     "KPDX",
        "nws_site":      "PQR",
        "nws_issuedby":  "PDX",
        "kalshi_series": "KXRAINPDXM",
        "lat": 45.589, "lon": -122.6,   "tz": "America/Los_Angeles",
        "regime":        "frontal",
        "tradeable_months": list(range(1, 13)),
        "label":         "Portland, OR",
        "state": "OR", "iem_network": "OR_ASOS",
    },
    "san_francisco": {
        "icao_code":     "KSFO",
        "nws_site":      "MTR",
        "nws_issuedby":  "SFO",
        "kalshi_series": "KXRAINSFO",
        "lat": 37.619, "lon": -122.375, "tz": "America/Los_Angeles",
        "regime":        "frontal_seasonal",
        "tradeable_months": [11, 12, 1, 2, 3, 4],
        "label":         "San Francisco, CA",
        "state": "CA", "iem_network": "CA_ASOS",
    },
    "los_angeles": {
        "icao_code":     "KLAX",
        "nws_site":      "LOX",
        "nws_issuedby":  "LAX",
        "kalshi_series": "KXRAINLAXM",
        "lat": 33.938, "lon": -118.408, "tz": "America/Los_Angeles",
        "regime":        "mediterranean",
        "tradeable_months": [11, 12, 1, 2, 3, 4],
        "label":         "Los Angeles, CA",
        "state": "CA", "iem_network": "CA_ASOS",
    },
    "new_york": {
        "icao_code":     "KNYC",
        "nws_site":      "OKX",
        "nws_issuedby":  "NYC",
        "kalshi_series": "KXRAINNYCM",
        "lat": 40.779, "lon": -73.969,  "tz": "America/New_York",
        "regime":        "mixed",
        "tradeable_months": [10, 11, 12, 1, 2, 3, 4],
        "label":         "New York, NY",
        "state": "NY", "iem_network": "NY_ASOS",
    },
    "chicago": {
        "icao_code":     "KMDW",          # Kalshi settles on Midway, NOT O'Hare
        "nws_site":      "LOT",
        "nws_issuedby":  "MDW",           # Midway CLI, not ORD
        "kalshi_series": "KXRAINCHIM",
        "lat": 41.786, "lon": -87.752,  "tz": "America/Chicago",
        "regime":        "mixed",
        "tradeable_months": [10, 11, 12, 1, 2, 3, 4],
        "label":         "Chicago, IL",
        "state": "IL", "iem_network": "IL_ASOS",
    },
    "miami": {
        "icao_code":     "KMIA",
        "nws_site":      "MFL",
        "nws_issuedby":  "MIA",
        "kalshi_series": "KXRAINMIAM",
        "lat": 25.795, "lon": -80.287,  "tz": "America/New_York",
        "regime":        "convective",
        "tradeable_months": [11, 12, 1, 2, 3, 4],
        "label":         "Miami, FL",
        "state": "FL", "iem_network": "FL_ASOS",
    },
    "denver": {
        "icao_code":     "KDEN",
        "nws_site":      "BOU",
        "nws_issuedby":  "DEN",
        "kalshi_series": "KXRAINDENM",
        "lat": 39.861, "lon": -104.673, "tz": "America/Denver",
        "regime":        "mixed",
        "tradeable_months": list(range(1, 13)),
        "label":         "Denver, CO",
        "state": "CO", "iem_network": "CO_ASOS",
    },
}

# ── TEMPERATURE CITIES ────────────────────────────────────────────────────────
# Each entry: kalshi_high / kalshi_low = Kalshi series tickers
# nws_station = ICAO used by NWS CLI for settlement (must match Kalshi rules)
# σ_d1 / σ_d0 = forecast error (°F RMSE) at D+1 and D+0 horizons (bias-corrected)
# These are research-based starting values; /temp/calibrate refines them.

# Cities temporarily blacklisted from auto-trading.
# Calibration data still logged and settled — blacklisted cities are excluded
# from bet placement only. Remove a city once 30+ settled calibration rows exist
# and bias is confirmed trustworthy.
AUTO_TRADER_CITY_BLACKLIST = {
    "phoenix",   # Systematic April cold bias — 3 consecutive losses, models ~2-3°F cold
}

TEMP_CITIES = {
    "nyc": {
        "label": "New York", "nws_station": "KNYC",
        "kalshi_high": "KXHIGHNY",  "kalshi_low": "KXLOWTNYC",
        "lat": 40.779, "lon": -73.969, "tz": "America/New_York",
        "σ_d1": 1.8, "σ_d0": 1.3,
        "warm_bias_gfs": 0.4, "warm_bias_ecmwf": 0.2,
    },
    "chicago": {
        "label": "Chicago", "nws_station": "KMDW",
        "kalshi_high": "KXHIGHCHI", "kalshi_low": "KXLOWTCHI",
        "lat": 41.786, "lon": -87.752, "tz": "America/Chicago",
        "σ_d1": 2.0, "σ_d0": 1.4,
        "warm_bias_gfs": 0.8, "warm_bias_ecmwf": 0.4,
    },
    "miami": {
        "label": "Miami", "nws_station": "KMIA",
        "kalshi_high": "KXHIGHMIA",  "kalshi_low": "KXLOWTMIA",
        "lat": 25.795, "lon": -80.287, "tz": "America/New_York",
        "σ_d1": 1.5, "σ_d0": 1.0,
        "warm_bias_gfs": 0.3, "warm_bias_ecmwf": 0.1,
    },
    "los_angeles": {
        "label": "Los Angeles", "nws_station": "KLAX",
        "kalshi_high": "KXHIGHLAX",  "kalshi_low": "KXLOWTLAX",
        "lat": 33.938, "lon": -118.408, "tz": "America/Los_Angeles",
        "σ_d1": 1.6, "σ_d0": 1.1,
        "warm_bias_gfs": 0.5, "warm_bias_ecmwf": 0.3,
    },
    "austin": {
        "label": "Austin", "nws_station": "KAUS",
        "kalshi_high": "KXHIGHAUS",  "kalshi_low": "KXLOWTAUS",
        "lat": 30.194, "lon": -97.670, "tz": "America/Chicago",
        "σ_d1": 2.1, "σ_d0": 1.5,
        "warm_bias_gfs": 0.9, "warm_bias_ecmwf": 0.5,
    },
    "phoenix": {
        "label": "Phoenix", "nws_station": "KPHX",
        "kalshi_high": "KXHIGHTPHX", "kalshi_low": None,
        "lat": 33.434, "lon": -112.008, "tz": "America/Phoenix",
        "σ_d1": 1.7, "σ_d0": 1.2,
        "warm_bias_gfs": 0.6, "warm_bias_ecmwf": 0.4,
    },
    "san_francisco": {
        "label": "San Francisco", "nws_station": "KSFO",
        "kalshi_high": "KXHIGHTSFO", "kalshi_low": None,
        "lat": 37.619, "lon": -122.375, "tz": "America/Los_Angeles",
        "σ_d1": 1.9, "σ_d0": 1.3,
        "warm_bias_gfs": 0.7, "warm_bias_ecmwf": 0.4,
    },
    "atlanta": {
        "label": "Atlanta", "nws_station": "KATL",
        "kalshi_high": "KXHIGHTATL", "kalshi_low": None,
        "lat": 33.640, "lon": -84.427, "tz": "America/New_York",
        "σ_d1": 2.0, "σ_d0": 1.4,
        "warm_bias_gfs": 0.7, "warm_bias_ecmwf": 0.4,
    },
    "washington_dc": {
        "label": "Washington DC", "nws_station": "KDCA",
        "kalshi_high": "KXHIGHTDC",  "kalshi_low": None,
        "lat": 38.852, "lon": -77.037, "tz": "America/New_York",
        "σ_d1": 1.9, "σ_d0": 1.3,
        "warm_bias_gfs": 0.5, "warm_bias_ecmwf": 0.3,
    },
    "denver": {
        "label": "Denver", "nws_station": "KDEN",
        "kalshi_high": "KXHIGHDEN",  "kalshi_low": "KXLOWTDEN",
        "lat": 39.861, "lon": -104.673, "tz": "America/Denver",
        "σ_d1": 2.3, "σ_d0": 1.6,
        "warm_bias_gfs": 1.0, "warm_bias_ecmwf": 0.6,
    },
    "houston": {
        "label": "Houston", "nws_station": "KHOU",
        "kalshi_high": "KXHIGHTHOU", "kalshi_low": None,
        "lat": 29.645, "lon": -95.279, "tz": "America/Chicago",
        "σ_d1": 2.2, "σ_d0": 1.5,
        "warm_bias_gfs": 0.8, "warm_bias_ecmwf": 0.5,
    },
    "minneapolis": {
        "label": "Minneapolis", "nws_station": "KMSP",
        "kalshi_high": "KXHIGHTMIN", "kalshi_low": None,
        "lat": 44.882, "lon": -93.229, "tz": "America/Chicago",
        "σ_d1": 2.4, "σ_d0": 1.7,
        "warm_bias_gfs": 1.1, "warm_bias_ecmwf": 0.7,
    },
    "boston": {
        "label": "Boston", "nws_station": "KBOS",
        "kalshi_high": "KXHIGHTBOS", "kalshi_low": None,
        "lat": 42.362, "lon": -71.006, "tz": "America/New_York",
        "σ_d1": 1.9, "σ_d0": 1.3,
        "warm_bias_gfs": 0.4, "warm_bias_ecmwf": 0.2,
    },
    "las_vegas": {
        "label": "Las Vegas", "nws_station": "KLAS",
        "kalshi_high": "KXHIGHTLV",  "kalshi_low": None,
        "lat": 36.080, "lon": -115.152, "tz": "America/Los_Angeles",
        "σ_d1": 1.8, "σ_d0": 1.2,
        "warm_bias_gfs": 0.7, "warm_bias_ecmwf": 0.4,
    },
    "philadelphia": {
        "label": "Philadelphia", "nws_station": "KPHL",
        "kalshi_high": "KXHIGHPHIL", "kalshi_low": "KXLOWTPHIL",
        "lat": 39.873, "lon": -75.241, "tz": "America/New_York",
        "σ_d1": 1.9, "σ_d0": 1.3,
        "warm_bias_gfs": 0.5, "warm_bias_ecmwf": 0.3,
    },
    "oklahoma_city": {
        "label": "Oklahoma City", "nws_station": "KOKC",
        "kalshi_high": "KXHIGHTOKC", "kalshi_low": None,
        "lat": 35.393, "lon": -97.601, "tz": "America/Chicago",
        "σ_d1": 2.3, "σ_d0": 1.6,
        "warm_bias_gfs": 0.9, "warm_bias_ecmwf": 0.6,
    },
    "seattle": {
        "label": "Seattle", "nws_station": "KSEA",
        "kalshi_high": "KXHIGHTSEA", "kalshi_low": None,
        "lat": 47.441, "lon": -122.3,  "tz": "America/Los_Angeles",
        "σ_d1": 1.8, "σ_d0": 1.2,
        "warm_bias_gfs": 0.4, "warm_bias_ecmwf": 0.2,
    },
}

# ── TEMP BIAS CACHE (per-station, populated by /temp/calibrate) ───────────────
_TEMP_BIAS_CACHE   = {}   # city_key -> {"gfs_bias": float, "ecmwf_bias": float, "σ_d1": float, "σ_d0": float}
_TEMP_SNAPSHOT_TTL  = 180   # 3 min cache — keeps markets fresh, avoids stale settled data
_TEMP_SCAN_CACHE   = {}   # city_key -> {"ts": float, "result": dict}

# ── TEMP FORECAST FUNCTIONS ───────────────────────────────────────────────────

def fetch_temp_forecast(city_key, horizon="d1"):
    """
    Fetch GFS and ECMWF temperature forecasts for a temp city.
    horizon: "d1" = tomorrow (D+1), "d0" = today (D+0)
    Returns dict with gfs_high, ecmwf_high, best_high, gfs_low, ecmwf_low,
    best_low, spread_high, spread_low, target_date, model_run_ts.

    Uses Open-Meteo's multi-model API — no API key, no rotation risk.
    Fetches temperature_2m_max and temperature_2m_min converted to °F.
    """
    import time as _t
    cfg = TEMP_CITIES.get(city_key)
    if not cfg:
        return {"ok": False, "error": f"Unknown temp city: {city_key}"}

    try:
        import pytz
        from datetime import datetime as dt_cls, timedelta
        tz_name   = cfg["tz"]
        local_tz  = pytz.timezone(tz_name)
        now_local = dt_cls.utcnow().replace(tzinfo=pytz.utc).astimezone(local_tz)
        today_str = now_local.strftime("%Y-%m-%d")

        if horizon == "d0":
            target_date = today_str
        else:
            target_date = (now_local + timedelta(days=1)).strftime("%Y-%m-%d")

        results = {}
        errors  = []

        # Fetch three models: gfs, ecmwf, best_match (blend)
        model_map = {
            "gfs":        "gfs_seamless",
            "ecmwf":      "ecmwf_ifs",
            "best":       "best_match",
        }

        import concurrent.futures as _cf
        def fetch_model(model_key, model_str):
            params = {
                "latitude":  cfg["lat"],
                "longitude": cfg["lon"],
                "daily":     "temperature_2m_max,temperature_2m_min",
                "timezone":  tz_name,
                "start_date": target_date,
                "end_date":   target_date,
                "models":     model_str,
            }
            r = requests.get("https://api.open-meteo.com/v1/forecast",
                             params=params, timeout=6)
            r.raise_for_status()
            d = r.json()
            hi_c = (d.get("daily", {}).get("temperature_2m_max") or [None])[0]
            lo_c = (d.get("daily", {}).get("temperature_2m_min") or [None])[0]
            if hi_c is None or lo_c is None:
                raise ValueError(f"{model_key}: no data for {target_date}")
            # Convert °C → °F, round to 1 decimal
            hi_f = round(hi_c * 9/5 + 32, 1)
            lo_f = round(lo_c * 9/5 + 32, 1)
            return model_key, hi_f, lo_f

        ex = _cf.ThreadPoolExecutor(max_workers=3)
        try:
            futures = {ex.submit(fetch_model, k, v): k for k, v in model_map.items()}
            for f in _cf.as_completed(futures, timeout=10):
                try:
                    k, hi, lo = f.result()
                    results[k] = {"high": hi, "low": lo}
                except Exception as e:
                    errors.append(str(e))
        finally:
            ex.shutdown(wait=False)

        if not results:
            return {"ok": False, "error": "; ".join(errors)}

        # Bias correction disabled — insufficient settled trade data to calibrate reliably.
        # Re-enable once 30+ settled trades per city are available.
        bias_cache = _TEMP_BIAS_CACHE.get(city_key, {})
        gfs_bias_high   = 0.0
        ecmwf_bias_high = 0.0
        blend_bias_high = 0.0
        gfs_bias_low    = 0.0
        ecmwf_bias_low  = 0.0

        gfs_raw    = results.get("gfs",   {})
        ecmwf_raw  = results.get("ecmwf", {})
        best_raw   = results.get("best",  {})

        gfs_hi_adj   = round(gfs_raw.get("high",  0) - gfs_bias_high,   1) if gfs_raw   else None
        gfs_lo_adj   = round(gfs_raw.get("low",   0) - gfs_bias_low,    1) if gfs_raw   else None
        ecmwf_hi_adj = round(ecmwf_raw.get("high",0) - ecmwf_bias_high, 1) if ecmwf_raw else None
        ecmwf_lo_adj = round(ecmwf_raw.get("low", 0) - ecmwf_bias_low,  1) if ecmwf_raw else None
        blend_hi_adj = round(best_raw.get("high", 0) - blend_bias_high, 1) if best_raw  else None
        blend_lo_adj = round(best_raw.get("low",  0) - 0.0,             1) if best_raw  else None

        # ── Use best model per city (GFS vs ECMWF, by lowest bc_rmse) ─────────
        # Simple selection until trigger-moment data justifies seasonal/spread rules.
        best_model = bias_cache.get("best_model", "average")

        if best_model == "gfs" and gfs_hi_adj is not None:
            best_hi = gfs_hi_adj
            best_lo = gfs_lo_adj
        elif best_model == "ecmwf" and ecmwf_hi_adj is not None:
            best_hi = ecmwf_hi_adj
            best_lo = ecmwf_lo_adj
        elif best_model == "blend" and blend_hi_adj is not None:
            best_hi = blend_hi_adj
            best_lo = blend_lo_adj
        else:
            hi_vals = [v for v in [gfs_hi_adj, ecmwf_hi_adj, blend_hi_adj] if v is not None]
            lo_vals = [v for v in [gfs_lo_adj, ecmwf_lo_adj, blend_lo_adj] if v is not None]
            best_hi = round(sum(hi_vals) / len(hi_vals), 1) if hi_vals else None
            best_lo = round(sum(lo_vals) / len(lo_vals), 1) if lo_vals else None
            best_model = "average"

        # Outlier override: REMOVED — implement analytically once trigger-moment data justifies it.

        # Spread still computed across all three (shows disagreement regardless of which is best)
        hi_all = [v for v in [gfs_hi_adj, ecmwf_hi_adj, blend_hi_adj] if v is not None]
        lo_all = [v for v in [gfs_lo_adj, ecmwf_lo_adj, blend_lo_adj] if v is not None]
        spread_hi = round(max(hi_all) - min(hi_all), 1) if len(hi_all) >= 2 else 0.0
        spread_lo = round(max(lo_all) - min(lo_all), 1) if len(lo_all) >= 2 else 0.0

        # σ for this horizon from cache, else config fallback
        σ_key = "σ_d0" if horizon == "d0" else "σ_d1"
        σ     = bias_cache.get(σ_key, cfg.get(σ_key, 2.0))

        return {
            "ok":           True,
            "city":         city_key,
            "label":        cfg["label"],
            "target_date":  target_date,
            "horizon":      horizon,
            "nws_station":  cfg["nws_station"],
            "gfs_high_raw": gfs_raw.get("high"),
            "gfs_low_raw":  gfs_raw.get("low"),
            "ecmwf_high_raw": ecmwf_raw.get("high"),
            "ecmwf_low_raw":  ecmwf_raw.get("low"),
            "gfs_high":     gfs_hi_adj,
            "gfs_low":      gfs_lo_adj,
            "ecmwf_high":   ecmwf_hi_adj,
            "ecmwf_low":    ecmwf_lo_adj,
            "blend_high":   blend_hi_adj,
            "blend_low":    blend_lo_adj,
            "best_high":    best_hi,
            "best_low":     best_lo,
            "best_model":   best_model,
            "spread_high":  spread_hi,
            "spread_low":   spread_lo,
            "gfs_bias_applied":   gfs_bias_high,
            "ecmwf_bias_applied": ecmwf_bias_high,
            "blend_bias_applied": blend_bias_high,
            "sigma":        σ,
            "errors":       errors,
            "fetched_at":   _t.time(),
            "tz":           cfg.get("tz", "America/Chicago"),
        }

    except Exception as e:
        return {"ok": False, "error": str(e)}


def fetch_temp_kalshi_markets(city_key, market_type="high"):
    """
    Fetch open bracket markets for a temp city from Kalshi.
    market_type: "high" or "low"
    Returns list of brackets with ticker, low_temp, high_temp, yes_ask,
    yes_bid, no_ask, no_bid, spread, yes_ask_size, volume_24h, open_interest.
    """
    cfg = TEMP_CITIES.get(city_key)
    if not cfg:
        return {"ok": False, "error": f"Unknown temp city: {city_key}", "markets": []}

    series = cfg["kalshi_high"] if market_type == "high" else cfg.get("kalshi_low")
    if not series:
        return {"ok": True, "markets": [], "note": f"No {market_type} series for {city_key}"}

    if not KALSHI_KEY_ID:
        return {"ok": False, "error": "Kalshi key not configured", "markets": []}

    try:
        path = "/trade-api/v2/markets"
        url  = f"{KALSHI_BASE}/markets"
        params = {"series_ticker": series, "status": "open", "limit": 20}
        headers = kalshi_auth_headers("GET", path)
        r = requests.get(url, params=params, headers=headers, timeout=6)
        r.raise_for_status()
        raw = r.json()

        markets = []
        for m in raw.get("markets", []):
            ticker  = m.get("ticker", "")
            title   = m.get("title", "")

            # Parse settlement date from ticker e.g. KXHIGHMIA-26APR10-B76.5
            # Format is YYMMMDD where MMM = JAN/FEB/MAR/APR etc.
            ticker_date = None
            td_match = re.search(r"-(\d{2})([A-Z]{3})(\d{2})-", ticker, re.IGNORECASE)
            if td_match:
                try:
                    from datetime import datetime as _dtp
                    yr  = int(td_match.group(1)) + 2000
                    mon = td_match.group(2).upper()
                    day = int(td_match.group(3))
                    mon_map = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
                               "JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
                    if mon in mon_map:
                        ticker_date = f"{yr:04d}-{mon_map[mon]:02d}-{day:02d}"
                except Exception:
                    pass

            # Parse bracket from title e.g. "54° to 55°F" or "Above 57°F" or "Below 50°F"
            lo_temp = hi_temp = None
            bracket_label = title

            # Try floor_strike / cap_strike first — most reliable source
            # Kalshi stores bracket bounds as numeric fields
            fs_floor = m.get("floor_strike")
            fs_cap   = m.get("cap_strike")
            if fs_floor is not None and fs_cap is not None:
                try:
                    lo_temp = float(fs_floor)
                    hi_temp = float(fs_cap)
                    bracket_label = f"{int(lo_temp)}–{int(hi_temp)}°F"
                except Exception:
                    pass

            # Fall back to parsing title text
            if lo_temp is None and hi_temp is None:
                # Handles: "85-86°", "85–86°F", "85 to 86°F", "85°F to 86°F"
                rng = re.search(
                    r"(\d+\.?\d*)\s*(?:°F?)?\s*[-–to]+\s*(\d+\.?\d*)\s*°?F?",
                    title, re.IGNORECASE)
                if rng:
                    lo_temp = float(rng.group(1))
                    hi_temp = float(rng.group(2))
                    # Sanity: lo < hi and both in reasonable temp range
                    if lo_temp < hi_temp and 0 <= lo_temp <= 130:
                        bracket_label = f"{int(lo_temp)}–{int(hi_temp)}°F"
                    else:
                        lo_temp = hi_temp = None

            if lo_temp is None and hi_temp is None:
                above = re.search(r"(?:above|>|or above)\s*(\d+\.?\d*)", title, re.IGNORECASE)
                below = re.search(r"(?:below|<|or below)\s*(\d+\.?\d*)", title, re.IGNORECASE)
                # IMPORTANT: Kalshi's tail bracket title says "<48" or "below 48"
                # but the bracket actually wins on NWS reporting 47 or below
                # (Kalshi's UI confirms: "47° or below"). The threshold in the
                # title is the EXCLUSIVE upper bound, not the highest winning
                # integer. We normalize lo/hi to the winning integers so that
                # bracket_prob's half-integer shift (upper = hi + 0.5) and the
                # settlement check (NWS_value <= hi_temp) both give correct
                # answers. Display label preserves Kalshi's convention.
                if above:
                    raw_threshold = float(above.group(1))
                    lo_temp = raw_threshold + 1   # lowest winning integer (e.g. ">95" wins on 96+)
                    hi_temp = None
                    bracket_label = f">{int(raw_threshold)}°F"
                elif below:
                    raw_threshold = float(below.group(1))
                    lo_temp = None
                    hi_temp = raw_threshold - 1   # highest winning integer (e.g. "<48" wins on ≤47)
                    bracket_label = f"<{int(raw_threshold)}°F"

            # Last resort: functional_strike field
            if lo_temp is None and hi_temp is None:
                fs = m.get("functional_strike", "") or ""
                fs_lo = re.search(r"(\d+\.?\d*)\s*(?:to|-)\s*(\d+\.?\d*)", str(fs))
                if fs_lo:
                    lo_temp = float(fs_lo.group(1))
                    hi_temp = float(fs_lo.group(2))
                    bracket_label = f"{int(lo_temp)}–{int(hi_temp)}°F"

            yes_ask   = float(m.get("yes_ask_dollars", 0) or 0)
            yes_bid   = float(m.get("yes_bid_dollars", 0) or 0)
            no_ask    = float(m.get("no_ask_dollars", 0)  or 0)
            no_bid    = float(m.get("no_bid_dollars", 0)  or 0)
            spread    = round(max(0.0, yes_ask - yes_bid), 4)
            yes_ask_sz = float(m.get("yes_ask_size_fp", 0) or 0)
            yes_bid_sz = float(m.get("yes_bid_size_fp", 0) or 0)
            open_int   = float(m.get("open_interest_fp", 0) or 0)
            vol_24h    = float(m.get("volume_24h_fp",    0) or 0)
            volume     = float(m.get("volume_fp",        0) or 0)
            close_time = m.get("close_time", "")

            markets.append({
                "ticker":        ticker,
                "title":         title,
                "bracket_label": bracket_label,
                "lo_temp":       lo_temp,
                "hi_temp":       hi_temp,
                "yes_ask":       yes_ask,
                "yes_bid":       yes_bid,
                "no_ask":        no_ask,
                "no_bid":        no_bid,
                "spread":        spread,
                "yes_ask_size":  yes_ask_sz,
                "yes_bid_size":  yes_bid_sz,
                "open_interest": open_int,
                "volume_24h":    vol_24h,
                "volume":        volume,
                "close_time":    close_time,
                "ticker_date":   ticker_date,
            })

        # Sort by lo_temp ascending
        markets.sort(key=lambda x: (x["lo_temp"] or x["hi_temp"] or 0))
        return {"ok": True, "markets": markets, "series": series}

    except Exception as e:
        return {"ok": False, "error": str(e), "markets": []}


def analyze_temp_brackets(markets, forecast, market_type="high"):
    """
    Score each bracket against the bias-adjusted forecast.
    Uses a normal distribution centred on best_high (or best_low) with
    sigma from the forecast dict.  Computes:
      model_prob   — P(settlement falls in this bracket)
      gap_c        — model_prob*100 - yes_ask*100 (in cents)
      net_gap_c    — gap_c minus bid/ask spread cost
      kelly_frac   — half-Kelly fraction
      kelly_size   — dollar size at kelly_frac × $100 bankroll unit
      edge_ratio   — gap_c / sigma (σ-adjusted edge signal)
      grade        — A / B / C / skip
    """
    from math import erf, sqrt
    def normcdf(x):
        return 0.5 * (1 + erf(x / sqrt(2)))
    def bracket_prob(lo, hi, mu, sigma):
        """
        P(settlement falls in bracket) given NWS CLI always reports whole integers.

        Since settlement = round(actual_temp) to nearest integer, the correct
        probability mass for bracket [lo, hi] is:
            P(lo - 0.5 < X < hi + 0.5) for a N(mu, sigma) distribution.

        Examples with mu=79.3, sigma=1.0:
          bracket 85-86: P(84.5 < X < 86.5) ≈ 0.0%  ← correct (6 sigma away)
          bracket 79-80: P(78.5 < X < 80.5) ≈ 68%   ← correct (spans 1 sigma each side)
          edge ">90":    P(X > 89.5)                  ← correct for "90 or above"
          edge "<78":    P(X < 78.5)                  ← correct for "77 or below"

        Without the ±0.5 adjustment, bracket 85-86 would compute P(85 < X < 86)
        which is ~38% when mu=85.5 — the same as any 1-sigma bracket — even though
        the settlement integer 85 and 86 are both inside this range.
        """
        if sigma <= 0:
            # Degenerate: certain outcome
            mu_int = round(mu)
            if lo is None and hi is None:  return 1.0
            if lo is None:  return 1.0 if mu_int <= round(hi) else 0.0
            if hi is None:  return 1.0 if mu_int >= round(lo) else 0.0
            return 1.0 if round(lo) <= mu_int <= round(hi) else 0.0
        # Continuous approximation with half-integer shift
        # lo/hi represent the included integers at the bracket edges
        upper = hi + 0.5 if hi is not None else float('inf')
        lower = lo - 0.5 if lo is not None else float('-inf')
        p_hi = normcdf((upper - mu) / sigma) if hi is not None else 1.0
        p_lo = normcdf((lower - mu) / sigma) if lo is not None else 0.0
        return round(p_hi - p_lo, 4)

    mu    = forecast.get("best_high") if market_type == "high" else forecast.get("best_low")
    sigma = forecast.get("sigma", 2.0)
    spread_hi = forecast.get("spread_high" if market_type == "high" else "spread_low", 0)

    # Global sigma inflation factor — compensates for hindcast RMSE being too tight.
    # Calibrated σ comes from Open-Meteo analysis runs, not true D+1 forecast skill.
    # Default 1.3× — adjust down as calibration data accumulates.
    SIGMA_INFLATION = 1.3
    sigma = round(sigma * SIGMA_INFLATION, 2)

    # Get city_key from forecast for regional adjustments
    city_key = forecast.get("city", "")

    # Spring seasonal multiplier for volatile transition-season cities.
    # Midwest/NE cities in March-May have much higher actual forecast error than
    # hindcast RMSE suggests — frontal boundaries shift unpredictably.
    # Phoenix/LV/Miami are stable year-round, no multiplier needed.
    from datetime import date as _date_cls
    _month = _date_cls.today().month
    _SPRING_VOLATILE = {
        'minneapolis', 'chicago', 'oklahoma_city', 'denver',
        'boston', 'nyc', 'philadelphia', 'washington_dc', 'atlanta'
    }
    if city_key in _SPRING_VOLATILE and _month in (3, 4, 5):
        sigma = round(sigma * 1.5, 2)

    # Regional minimum sigma floors — prevent catastrophic overconfidence.
    # Floors are set by climate zone and season volatility, not model output.
    # These represent the minimum believable forecast uncertainty for each region.
    _SIGMA_FLOORS = {
        # Midwest: high spring volatility, frontal boundaries volatile
        'minneapolis':   1.5,
        'chicago':       1.2,
        'oklahoma_city': 1.2,
        # East Coast: moderate spring volatility
        'nyc':           1.0,
        'philadelphia':  1.0,
        'washington_dc': 1.0,
        'boston':        1.0,
        'atlanta':       1.0,
        # Mountain: convective volatility in spring
        'denver':        0.8,
        # Stable desert/tropical — low floors justified
        'phoenix':       0.4,
        'las_vegas':     0.6,
        'miami':         0.6,
        # Variable coasts — marine layer makes forecasting hard
        'san_francisco': 1.5,
        'los_angeles':   1.2,
        'seattle':       1.5,
        # Gulf Coast
        'houston':       0.8,
        'austin':        0.8,
    }
    _floor = _SIGMA_FLOORS.get(city_key, 0.8)
    if sigma < _floor:
        sigma = _floor

    # Inflate σ further by model spread in quadrature.
    # When GFS and ECMWF disagree, today's forecast is genuinely harder than
    # the historical RMSE baseline.
    # Formula: σ_eff = sqrt(σ² + (spread/2)²)
    from math import sqrt as _sqrt
    if spread_hi and spread_hi > 0:
        sigma = round(_sqrt(sigma**2 + (spread_hi / 2)**2), 2)

    if mu is None:
        for m in markets:
            m["model_prob"] = None; m["gap_c"] = 0; m["net_gap_c"] = 0
            m["grade"] = "skip"; m["edge_ratio"] = 0
        return markets

    analyzed = []
    for m in markets:
        lo  = m.get("lo_temp")
        hi  = m.get("hi_temp")
        ask = m.get("yes_ask", 0)
        bid = m.get("yes_bid", 0)
        spr = round((ask - bid) * 100)  # spread in cents

        # ── Skip settled / stale markets ─────────────────────────────────────
        # 1. Market priced at or below 2¢ YES = already settled worthless, or
        #    effectively dead. Never trade these — the "edge" is fake.
        # 2. Market priced at or above 98¢ YES = already settled in-the-money.
        # 3. close_time in the past = market closed, no longer tradeable.
        if ask <= 0.02 or ask >= 0.98:
            m["model_prob"] = None; m["gap_c"] = 0; m["net_gap_c"] = 0
            m["grade"] = "skip"; m["edge_ratio"] = 0
            m["skip_reason"] = "settled"
            analyzed.append(m)
            continue

        close_time = m.get("close_time", "")
        if close_time:
            try:
                from datetime import datetime as _dt, timezone as _tz
                ct = _dt.fromisoformat(close_time.replace("Z", "+00:00"))
                if ct < _dt.now(_tz.utc):
                    m["model_prob"] = None; m["gap_c"] = 0; m["net_gap_c"] = 0
                    m["grade"] = "skip"; m["edge_ratio"] = 0
                    m["skip_reason"] = "expired"
                    analyzed.append(m)
                    continue
            except Exception:
                pass

        # Compute hours until 6 AM local cutoff for this city.
        # After 6 AM local, intraday observations start and market gains info edge.
        hours_to_cutoff = None
        try:
            import pytz as _pytz
            from datetime import datetime as _dt2, timezone as _tz2
            tz_name   = forecast.get("tz", "America/Chicago")
            local_tz  = _pytz.timezone(tz_name)
            now_local = _dt2.now(_tz2.utc).astimezone(local_tz)
            target    = forecast.get("target_date", "")
            if target:
                from datetime import date as _date2, timedelta as _td
                tgt = _date2.fromisoformat(target)
                # 6 AM local on target date
                cutoff_local = local_tz.localize(
                    _dt2(tgt.year, tgt.month, tgt.day, 6, 0, 0))
                hours_to_cutoff = round((cutoff_local - now_local).total_seconds() / 3600, 1)
        except Exception:
            pass
        m["hours_to_cutoff"] = hours_to_cutoff

        # ── Also skip if the target_date in the forecast is today or past ────
        # This catches D+1 markets where the date has rolled over to today/yesterday.
        target_date = forecast.get("target_date", "")
        if target_date:
            try:
                from datetime import date as _date
                td = _date.fromisoformat(target_date)
                if td < _date.today():
                    m["model_prob"] = None; m["gap_c"] = 0; m["net_gap_c"] = 0
                    m["grade"] = "skip"; m["edge_ratio"] = 0
                    m["skip_reason"] = "past_date"
                    analyzed.append(m)
                    continue
            except Exception:
                pass

        # ── Guard: skip if bracket bounds couldn't be parsed ────────────────
        # lo=None AND hi=None means the title regex failed entirely.
        # bracket_prob(None, None, mu, σ) = 1.0 — a fake 100% — so we must skip.
        if lo is None and hi is None:
            m["model_prob"] = None; m["gap_c"] = 0; m["net_gap_c"] = 0
            m["grade"] = "skip"; m["edge_ratio"] = 0
            m["skip_reason"] = "unparseable_bracket"
            analyzed.append(m)
            continue

        # ── Guard: skip if ticker date doesn't match forecast target_date ────
        # Kalshi opens next-day markets before midnight. If the scanner is running
        # at 12:11 AM on April 10, it might see April 11 markets already listed
        # as "open". We only want markets for our forecast's target date.
        ticker_date = m.get("ticker_date")
        fc_target   = forecast.get("target_date", "")
        if ticker_date and fc_target and ticker_date != fc_target:
            m["model_prob"] = None; m["gap_c"] = 0; m["net_gap_c"] = 0
            m["grade"] = "skip"; m["edge_ratio"] = 0
            m["skip_reason"] = f"wrong_date:{ticker_date}"
            analyzed.append(m)
            continue

        # Compute probability as weighted average of per-model probabilities.
        # This correctly handles cases where models straddle the bracket boundary.
        # Blending forecasts first then computing P() loses that information.
        # e.g. GFS=54.5°F (below 56°F) and ECMWF=57.3°F (above 56°F) should give
        # ~50% probability, not 65% from the blended average of 55.9°F.
        mu_gfs_v   = forecast.get("gfs_high")   if market_type == "high" else forecast.get("gfs_low")
        mu_ecmwf_v = forecast.get("ecmwf_high") if market_type == "high" else forecast.get("ecmwf_low")
        sigma_base = forecast.get("sigma", 2.0)

        if mu_gfs_v is not None and mu_ecmwf_v is not None:
            # Use per-model sigma (base only, no spread inflation — spread IS the disagreement)
            p_gfs   = bracket_prob(lo, hi, mu_gfs_v,   sigma_base)
            p_ecmwf = bracket_prob(lo, hi, mu_ecmwf_v, sigma_base)
            prob    = round((p_gfs + p_ecmwf) / 2, 4)
        else:
            # Fall back to blended mu with spread-inflated sigma
            prob = bracket_prob(lo, hi, mu, sigma)

        gap_c     = round((prob - ask) * 100)
        net_gap_c = max(0, gap_c - spr)
        edge_ratio = round(gap_c / sigma, 3) if sigma > 0 else 0.0

        # Kelly: f = (p*b - q) / b where b = (1-ask)/ask (payout ratio)
        if ask > 0 and ask < 1:
            b        = (1 - ask) / ask
            kelly    = (prob * b - (1 - prob)) / b
            kelly_h  = max(0.0, round(kelly * 0.5, 3))   # half-Kelly
            kelly_sz = round(kelly_h * 100, 2)             # $ per $100 bankroll unit
        else:
            kelly_h  = 0.0
            kelly_sz = 0.0

        # ── Liquidity scoring ─────────────────────────────────────────────
        # Pass 1: quick top-of-book score to filter obvious skips fast
        ask_sz  = m.get("yes_ask_size", 0)
        oi      = m.get("open_interest", 0)
        vol_24h = m.get("volume_24h", 0)
        liq_pts = (20 if spr <= 1 else 12 if spr <= 3 else 5 if spr <= 5 else 0) + \
                  (20 if ask_sz >= 500 else 12 if ask_sz >= 100 else 4 if ask_sz >= 20 else 0) + \
                  (15 if oi >= 2000 else 8 if oi >= 500 else 3 if oi > 0 else 0) + \
                  (10 if vol_24h >= 500 else 5 if vol_24h >= 100 else 0)
        liq_grade_quick = "A" if liq_pts >= 55 else "B" if liq_pts >= 35 else "C" if liq_pts >= 18 else "D"

        # Pass 2: full orderbook liq for any signal with meaningful edge
        # Only call if signal has edge (avoid wasting API calls on obvious skips)
        ob_liq      = None
        fillable_a  = 0.0
        liq_grade   = liq_grade_quick
        if net_gap_c > 0 and edge_ratio >= 0.04 and KALSHI_KEY_ID and prob >= 0.25:
            ticker_str = m.get("ticker", "")
            if ticker_str:
                ob_liq = fetch_full_orderbook_liq(ticker_str, mu, sigma, lo, hi)
                if ob_liq:
                    liq_grade  = ob_liq["liq_grade"]
                    fillable_a = ob_liq["fillable_a_dollars"]

        # Kelly cap: use A-grade fillable if we have it, else top-of-book cap
        if ob_liq and fillable_a > 0:
            kelly_sz_capped = min(kelly_sz, fillable_a)
        elif ask_sz > 0 and ask > 0:
            kelly_sz_capped = min(kelly_sz, round(ask_sz * ask, 2))
        else:
            kelly_sz_capped = kelly_sz
        book_limited = kelly_sz_capped < kelly_sz

        # Is this bracket below the model center? (tail YES bet — longshot)
        is_tail_bet = (hi is not None and hi <= mu) or (lo is None and hi is not None and hi < mu)

        # ── Structural quality checks ─────────────────────────────────────────
        # 1. Does any individual model point inside the bracket?
        #    If no model falls within [lo, hi], the probability comes entirely
        #    from distribution tails — the models agree the bracket is unlikely.
        #    Get individual model values from forecast dict.
        bracket_width = (hi - lo) if (hi is not None and lo is not None) else 99
        mu_gfs   = forecast.get("gfs_high")   if market_type == "high" else forecast.get("gfs_low")
        mu_ecmwf = forecast.get("ecmwf_high") if market_type == "high" else forecast.get("ecmwf_low")
        mu_blend = forecast.get("blend_high") if market_type == "high" else forecast.get("blend_low")

        def _inside(v):
            if v is None: return False
            # NWS settles on whole integers. lo_temp <= NWS <= hi_temp.
            # Allow 0.49°F slop so a forecast of 79.4 counts as inside 79-80
            # but 78.5 does NOT (it belongs to the 78-79 bracket).
            lo_ok = lo is None or v >= lo - 0.49
            hi_ok = hi is None or v <= hi + 0.49
            return lo_ok and hi_ok

        any_model_inside = _inside(mu_gfs) or _inside(mu_ecmwf) or _inside(mu_blend)

        # 2. Model spread vs bracket width
        #    When spread ≥ bracket width, models disagree about which bracket wins
        #    — the "edge" is really betting on which model is right.
        raw_spread = spread_hi if spread_hi else 0
        # For open-ended brackets (no lo or no hi), use 2°F as reference width
        # since that's the standard bracket width. A 6°F spread on <66°F is
        # just as dangerous as a 6°F spread on a 66–68°F bracket.
        # Bracket "65-66" wins on integers 65 AND 66 (inclusive), so the
        # actual winning temperature range is 64.5–66.5°F = 2°F wide, not
        # the integer difference of 1°F. Use (hi - lo + 1) to account for
        # both endpoints being inclusive winners. Without this fix, model
        # spreads of ~0.9°F incorrectly trigger spread_exceeds_bracket on
        # standard 2-integer brackets and downgrade A signals to B.
        bracket_width_ref = (hi - lo + 1) if (hi is not None and lo is not None) else 2.0
        spread_exceeds_bracket = (raw_spread > 0 and
                                  raw_spread >= bracket_width_ref * 0.75)

        # Grade: base rules
        # A: strong edge ratio, good liquidity, model says >50% likely (favorite mispricing)
        # B: decent edge ratio, OR model says 35-50% (edge exists but model not confident)
        # C: weak edge, borderline
        # skip: no edge, too uncertain, or structurally weak
        if net_gap_c <= 0:
            grade = "skip"
        elif prob < 0.20:
            grade = "skip"
        elif is_tail_bet and prob < 0.35:
            grade = "skip"
        elif edge_ratio >= 0.12 and prob >= 0.50 and net_gap_c >= 8:
            grade = "A"   # require at least 8¢ net edge — prevents σ-unit inflation
        elif edge_ratio >= 0.07 and prob >= 0.35 and net_gap_c >= 5:
            grade = "B"
        elif edge_ratio >= 0.04 and prob >= 0.25:
            grade = "C"
        else:
            grade = "skip"

        # Apply structural penalties after base grade
        if grade in ("A", "B", "C"):
            # No model points at the bracket → cap at B
            if not any_model_inside and grade == "A":
                grade = "B"
            # Spread ≥ 75% of bracket width → downgrade one level
            if spread_exceeds_bracket:
                grade = {"A": "B", "B": "C", "C": "skip"}.get(grade, grade)
            # Open-ended <X°F bracket: require best model center ≥ 1.5σ below ceiling.
            # Open-ended clearance check.
            # Always fires when mu is near or beyond the boundary.
            # Required clearance scales with spread — tighter when models agree,
            # wider when models disagree (spread inflates uncertainty).
            # Minimum 0.5σ clearance even when spread is zero.
            clearance_mult = max(0.5, min(1.5, 0.5 + raw_spread / 4.0))
            if lo is None and hi is not None and sigma and sigma > 0:
                required_clearance = hi - clearance_mult * sigma
                if mu >= required_clearance:
                    grade = {"A": "B", "B": "C", "C": "skip"}.get(grade, grade)
                    m["skip_reason"] = m.get("skip_reason","") + " open_lt_insufficient_clearance"
            if hi is None and lo is not None and sigma and sigma > 0:
                required_clearance = lo + clearance_mult * sigma
                if mu <= required_clearance:
                    grade = {"A": "B", "B": "C", "C": "skip"}.get(grade, grade)
                    m["skip_reason"] = m.get("skip_reason","") + " open_gt_insufficient_clearance"

        m.update({
            "model_prob":        prob,
            "mu":                mu,
            "sigma":             sigma,
            "gap_c":             gap_c,
            "net_gap_c":         net_gap_c,
            "spread_c":          spr,
            "kelly_frac":        kelly_h,
            "kelly_size":        kelly_sz_capped,
            "kelly_size_uncapped": kelly_sz,
            "book_limited":      book_limited,
            "ask_size":          ask_sz,
            "edge_ratio":        edge_ratio,
            "liq_grade":         liq_grade,
            "liq_pts":           liq_pts,
            "liq_skip":          liq_grade == "D",  # auto-trader skips D, scanner shows all
            "fillable_a":        round(fillable_a, 2),
            "is_tail_bet":       is_tail_bet,
            "any_model_inside":  any_model_inside,
            "spread_exceeds_bracket": spread_exceeds_bracket,
            "open_clearance_f":  round(hi - mu, 1) if (lo is None and hi is not None) else
                                 round(mu - lo, 1) if (hi is None and lo is not None) else None,
            "open_clearance_req": round(1.5 * sigma, 1) if sigma else None,
            "grade":             grade,
            "actionable":        grade in ("A", "B"),
        })
        analyzed.append(m)

    # ── Market rank confirmation ──────────────────────────────────────────────
    # Rank all brackets by market price (yes_ask) descending.
    # Market's #1 bracket = highest yes_ask = market's most confident outcome.
    #
    # Grade adjustment rules (activated based on observed loss pattern):
    #   top1         — model in market's #1 bracket → grade bumps one level
    #   top2         — model in market's #2 bracket → grade unchanged
    #   outside_top2 — model outside top 2 → grade drops one level
    #   tail_only    — single bracket series → no adjustment (can't rank)
    #   skip         — already skip → unchanged
    #
    # Grade bump/drop scale:
    #   skip → skip (can never bump a skip)
    #   C → B (bump) or skip (drop)
    #   B → A (bump) or C (drop)
    #   A → A (already max) or B (drop)
    _grade_up   = {"A": "A", "B": "A", "C": "B", "skip": "skip"}
    _grade_down = {"A": "B", "B": "C", "C": "skip", "skip": "skip"}

    # Only rank tradeable, non-skip markets. Skip-graded markets include
    # settled-stale ones (ask >= 0.98) which would otherwise dominate the #1
    # rank with their near-100% prices, pushing real markets to #2+ and
    # blocking auto-trader on otherwise valid A signals.
    _tradeable_asks = [x for x in analyzed
                       if 0.02 < x.get("yes_ask", 0) < 0.98
                       and x.get("grade") != "skip"]
    _is_tail_series = len(_tradeable_asks) <= 1

    if not _is_tail_series:
        _all_asks = sorted(_tradeable_asks, key=lambda x: -x.get("yes_ask", 0))
        _top1_key = (_all_asks[0].get("lo_temp"), _all_asks[0].get("hi_temp")) if len(_all_asks) > 0 else None
        _top2_key = (_all_asks[1].get("lo_temp"), _all_asks[1].get("hi_temp")) if len(_all_asks) > 1 else None
    else:
        _top1_key = _top2_key = None

    for m in analyzed:
        if m.get("grade") == "skip":
            m["mkt_rank_conf"] = "skip"
            continue
        if _is_tail_series:
            m["mkt_rank_conf"] = "tail_only"
            # No grade adjustment for tail series
            continue
        _key = (m.get("lo_temp"), m.get("hi_temp"))
        if _key == _top1_key:
            m["mkt_rank_conf"] = "top1"
            m["grade"] = _grade_up[m["grade"]]
        elif _key == _top2_key:
            m["mkt_rank_conf"] = "top2"
            # No adjustment — grade stays as-is
        else:
            m["mkt_rank_conf"] = "outside_top2"
            m["grade"] = _grade_down[m["grade"]]
        # Re-sync actionable after grade change
        m["actionable"] = m["grade"] in ("A", "B")

    # Sort: A first, then by gap_c descending
    grade_order = {"A": 0, "B": 1, "C": 2, "skip": 3}
    analyzed.sort(key=lambda x: (grade_order.get(x["grade"], 4), -x["gap_c"]))
    return analyzed


def detect_combo_signals(all_markets, forecast):
    """
    Find adjacent bracket pairs where both have independent positive edge.
    When the model center sits near a bracket boundary, two adjacent brackets
    can each have positive edge — buying both covers a wider range at combined
    probability much closer to 100% than either bracket alone.

    Conditions for a valid combo:
    1. Both brackets have net_gap_c > 0 independently (both positive edge)
    2. Brackets are contiguous — hi of A == lo of B (or one is open-ended tail)
    3. Combined probability >= 0.85 (meaningful coverage)
    4. Combined cost still leaves positive edge vs combined probability

    Returns list of combo signal dicts, sorted by combined_net_edge_c desc.
    """
    from math import erf, sqrt as _sqrt

    def normcdf(x):
        return 0.5 * (1 + erf(x / _sqrt(2)))

    def bracket_prob_combined(lo, hi, mu, sigma):
        """P(actual < hi) for open-left or P(lo <= actual < hi) for bounded."""
        if sigma <= 0: return 0.0
        lo_p = normcdf((lo - 0.5 - mu) / sigma) if lo is not None else 0.0
        hi_p = normcdf((hi + 0.5 - mu) / sigma) if hi is not None else 1.0
        return max(0.0, hi_p - lo_p)

    mu    = forecast.get("best_high")
    sigma = forecast.get("sigma", 2.0)
    if mu is None or sigma <= 0:
        return []

    # Only consider brackets with positive independent edge (not skip)
    candidates = [m for m in all_markets
                  if m.get("net_gap_c", 0) > 0
                  and m.get("grade") not in ("skip",)
                  and m.get("yes_ask", 0) > 0.02
                  and m.get("yes_ask", 0) < 0.98]

    combos = []
    seen   = set()

    for i, a in enumerate(candidates):
        for b in candidates:
            if b is a: continue
            ta = a.get("ticker", "")
            tb = b.get("ticker", "")
            pair_key = tuple(sorted([ta, tb]))
            if pair_key in seen: continue

            lo_a = a.get("lo_temp")
            hi_a = a.get("hi_temp")
            lo_b = b.get("lo_temp")
            hi_b = b.get("hi_temp")

            # Check contiguity: hi_a == lo_b (A is lower bracket, B is upper)
            # Also handle: A is open-ended tail (<X), B starts at X
            # Contiguity: brackets are adjacent if hi of lower == lo of upper,
            # OR they differ by 1 (Kalshi stores e.g. 82-83 as hi=83, 84-85 as lo=84)
            # Use threshold of 1.5 to handle both cases cleanly.
            is_contiguous = False
            if hi_a is not None and lo_b is not None and abs(hi_a - lo_b) <= 1.5:
                if lo_b >= hi_a:  # B is above A
                    is_contiguous = True
            if not is_contiguous and hi_b is not None and lo_a is not None and abs(hi_b - lo_a) <= 1.5:
                if lo_a >= hi_b:  # A is above B — swap so A is always lower
                    is_contiguous = True
                    a, b = b, a
                    lo_a, hi_a, lo_b, hi_b = lo_b, hi_b, lo_a, hi_a
                    ta, tb = tb, ta
            # Open-ended tail (<X) adjacent to bounded bracket starting at X or X+1
            if not is_contiguous and lo_a is None and hi_a is not None and lo_b is not None:
                if abs(hi_a - lo_b) <= 1.5 and lo_b >= hi_a:
                    is_contiguous = True
            if not is_contiguous and lo_b is None and hi_b is not None and lo_a is not None:
                if abs(hi_b - lo_a) <= 1.5 and lo_a >= hi_b:
                    is_contiguous = True
                    a, b = b, a
                    lo_a, hi_a, lo_b, hi_b = lo_b, hi_b, lo_a, hi_a
                    ta, tb = tb, ta

            if not is_contiguous:
                continue

            seen.add(pair_key)

            # Combined coverage: lo_a to hi_b (or open-ended)
            combined_lo = lo_a  # None if tail on left
            combined_hi = hi_b  # None if open-ended on right

            # Combined probability — area under normal between combined bounds
            combined_prob = bracket_prob_combined(combined_lo, combined_hi, mu, sigma)

            if combined_prob < 0.85:
                continue  # not enough combined coverage to bother

            # Combined cost (buying both YES contracts)
            cost_a = a.get("yes_ask", 0)
            cost_b = b.get("yes_ask", 0)
            combined_cost = cost_a + cost_b  # total spend per $1 payout on winner

            # Combined edge: model probability of combined range vs total cost
            # If combined_prob = 0.97 and combined_cost = 0.20, edge = +77¢
            combined_gap_c = round((combined_prob - combined_cost) * 100)

            # Spread cost: use worst spread of the two brackets
            spr_a = max(0, round((a.get("yes_ask",0) - a.get("yes_bid",0)) * 100))
            spr_b = max(0, round((b.get("yes_ask",0) - b.get("yes_bid",0)) * 100))
            combined_spr  = spr_a + spr_b
            combined_net  = combined_gap_c - combined_spr

            if combined_net <= 0:
                continue

            # Kelly sizing for combo: treat as single bet
            # combined_cost is the total outlay per $1 payout
            # EV = combined_prob * (1 - combined_cost) - (1 - combined_prob) * combined_cost
            if 0 < combined_cost < 1:
                b_odds  = (1 - combined_cost) / combined_cost
                kelly   = (combined_prob * b_odds - (1 - combined_prob)) / b_odds
                kelly_h = max(0.0, round(kelly * 0.5, 3))
                kelly_sz = round(kelly_h * 100, 2)
            else:
                kelly_h = kelly_sz = 0.0

            # Grade by combined net edge and combined probability
            edge_ratio_combo = round(combined_net / sigma, 3) if sigma > 0 else 0
            if combined_net >= 15 and combined_prob >= 0.90 and edge_ratio_combo >= 0.12:
                combo_grade = "A"
            elif combined_net >= 8 and combined_prob >= 0.85 and edge_ratio_combo >= 0.07:
                combo_grade = "B"
            else:
                continue  # not worth surfacing

            # Build label
            if combined_lo is None:
                combo_label = f"<{int(combined_hi)}°F"
            elif combined_hi is None:
                combo_label = f">{int(combined_lo)}°F"
            else:
                combo_label = f"{int(combined_lo)}–{int(combined_hi)}°F"

            combos.append({
                "is_combo":        True,
                "ticker":          f"COMBO:{ta}+{tb}",   # synthetic ticker for display
                "tickers":         [ta, tb],
                "bracket_label":   combo_label,
                "leg_a":           a,
                "leg_b":           b,
                "lo_temp":         combined_lo,
                "hi_temp":         combined_hi,
                "combined_prob":   round(combined_prob, 4),
                "combined_cost_c": round(combined_cost * 100),
                "combined_net_edge_c": combined_net,
                "combined_gap_c":  combined_gap_c,
                "net_gap_c":       combined_net,   # for sort compatibility
                "gap_c":           combined_gap_c,
                "yes_ask":         combined_cost,
                "model_prob":      round(combined_prob, 4),
                "mu":              mu,
                "sigma":           sigma,
                "kelly_frac":      kelly_h,
                "kelly_size":      kelly_sz,
                "edge_ratio":      edge_ratio_combo,
                "grade":           combo_grade,
                "actionable":      True,
                "liq_grade":       min(a.get("liq_grade","C"), b.get("liq_grade","C"),
                                       key=lambda x: {"A":0,"B":1,"C":2,"D":3}.get(x,4)),
                "hours_to_cutoff": min(
                    a.get("hours_to_cutoff") or 999,
                    b.get("hours_to_cutoff") or 999),
                "mkt_rank_conf":   "combo",
            })

    combos.sort(key=lambda x: -x["combined_net_edge_c"])
    return combos


def scan_temp_city(city_key, horizon="d1"):
    """
    Full pipeline for one temp city: forecast + both market types + analysis.
    Returns unified result dict suitable for /temp/scan response.
    """
    import time as _t
    cfg = TEMP_CITIES.get(city_key)
    if not cfg:
        return {"ok": False, "city": city_key, "error": "Unknown city"}

    # Check in-memory cache (avoid hammering Kalshi + OM on every request)
    cache_key = f"{city_key}-{horizon}"
    cached    = _TEMP_SCAN_CACHE.get(cache_key)
    if cached and (_t.time() - cached["ts"]) < _TEMP_SNAPSHOT_TTL:
        return cached["result"]

    import concurrent.futures as _cf
    try:
        ex = _cf.ThreadPoolExecutor(max_workers=2)
        try:
            f_fc   = ex.submit(fetch_temp_forecast, city_key, horizon)
            f_high = ex.submit(fetch_temp_kalshi_markets, city_key, "high")
            # LOW markets suppressed — overnight low timing is ambiguous until
            # we build hourly-based low forecast. PIN: add LOW back when ready.
            try: fc   = f_fc.result(timeout=12)
            except Exception as e: fc = {"ok": False, "error": str(e)}
            try: high = f_high.result(timeout=8)
            except Exception as e: high = {"ok": False, "markets": [], "error": str(e)}
            low = {"ok": False, "markets": [], "suppressed": True}
        finally:
            ex.shutdown(wait=False)

        if not fc.get("ok"):
            return {"ok": False, "city": city_key, "label": cfg["label"], "error": fc.get("error", "forecast failed")}

        # Score brackets — HIGH only. LOW suppressed (PIN: add back with hourly low forecast)
        high_markets = analyze_temp_brackets(high.get("markets", []), fc, "high") if high.get("ok") else []
        low_markets  = []

        # Drop settled/expired/past-date brackets from the response entirely
        high_markets = [m for m in high_markets if m.get("grade") != "skip" or m.get("skip_reason") not in ("settled","expired","past_date")]
        low_markets  = [m for m in low_markets  if m.get("grade") != "skip" or m.get("skip_reason") not in ("settled","expired","past_date")]

        # Detect combo signals — adjacent bracket pairs with combined positive edge
        combo_signals = detect_combo_signals(high_markets, fc)

        # Best edge across both market types (including combos)
        all_mkts = [m for m in high_markets + low_markets if m.get("actionable")]
        all_actionable = all_mkts + [c for c in combo_signals if c.get("actionable")]
        best_edge  = max((m["net_gap_c"] for m in all_actionable), default=0)
        best_grade = "A" if any(m["grade"] == "A" for m in all_actionable) else \
                     "B" if any(m["grade"] == "B" for m in all_actionable) else "none"

        result = {
            "ok":             True,
            "city":           city_key,
            "label":          cfg["label"],
            "nws_station":    cfg["nws_station"],
            "horizon":        horizon,
            "forecast":       fc,
            "high_markets":   high_markets,
            "low_markets":    low_markets,
            "combo_signals":  combo_signals,
            "low_suppressed": horizon == "d1",
            "best_edge_c":    best_edge,
            "best_grade":     best_grade,
            "actionable_count": len(all_actionable),
        }

        _TEMP_SCAN_CACHE[cache_key] = {"ts": _t.time(), "result": result}

        # Persist snapshots to DB (best-effort, non-blocking)
        try:
            conn = get_db()
            if conn:
                with conn.cursor() as cur:
                    for mtype, mkts in [("high", high_markets), ("low", low_markets)]:
                        for m in mkts:
                            if not m.get("ticker"): continue
                            try:
                                cur.execute("""
                                    INSERT INTO temp_snapshots
                                    (city, nws_station, target_date, horizon, market_type,
                                     ticker, bracket_label, lo_temp, hi_temp,
                                     gfs_forecast, ecmwf_forecast, best_forecast,
                                     sigma, spread_models, model_prob, yes_ask,
                                     gap_c, net_gap_c, edge_ratio, kelly_frac,
                                     grade, liq_grade, open_interest, volume_24h,
                                     hours_to_cutoff)
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                    ON CONFLICT DO NOTHING
                                """, (
                                    city_key, cfg["nws_station"], fc["target_date"], horizon, mtype,
                                    m["ticker"], m.get("bracket_label"),
                                    m.get("lo_temp"), m.get("hi_temp"),
                                    fc.get("gfs_high"),  fc.get("ecmwf_high"),  fc.get("best_high"),
                                    fc.get("sigma"),     fc.get("spread_high" if mtype=="high" else "spread_low"),
                                    m.get("model_prob"), m.get("yes_ask"),
                                    m.get("gap_c"),      m.get("net_gap_c"),
                                    m.get("edge_ratio"), m.get("kelly_frac"),
                                    m.get("grade"),      m.get("liq_grade"),
                                    int(m.get("open_interest",0)), int(m.get("volume_24h",0)),
                                    m.get("hours_to_cutoff"),
                                ))
                            except Exception as e:
                                # Log first occurrence per city to avoid flooding
                                if not _SCAN_ERR_LOGGED.get(city_key):
                                    import datetime as _dt
                                    err_entry = {
                                        "city": city_key,
                                        "error": str(e),
                                        "ticker": m.get("ticker"),
                                        "timestamp": _dt.datetime.utcnow().isoformat(),
                                    }
                                    with _SCAN_ERR_LOCK:
                                        _SCAN_ERR_LOG.insert(0, err_entry)
                                        _SCAN_ERR_LOG[:] = _SCAN_ERR_LOG[:50]
                                    print(f"  ⚠️  temp_snapshots INSERT failed [{city_key}]: {e}")
                                    _SCAN_ERR_LOGGED[city_key] = True
                conn.commit()
                conn.close()
        except Exception as e:
            print(f"  ⚠️  temp_snapshots DB connection/commit failed for {city_key}: {e}")

        return result

    except Exception as e:
        return {"ok": False, "city": city_key, "label": cfg.get("label",""), "error": str(e)}


# ── NWS CLI TEMPERATURE PARSER ───────────────────────────────────────────────
# NWS station code → (site, issuedby) for CLI temperature reports
# Mirrors the settlement station Kalshi uses for each city
NWS_TEMP_CLI = {
    "KNYC": ("OKX", "NYC"),   # Central Park, New York
    "KMDW": ("LOT", "MDW"),   # Midway Airport, Chicago
    "KMIA": ("MFL", "MIA"),   # Miami Intl
    "KLAX": ("LOX", "LAX"),   # LA Intl
    "KAUS": ("EWX", "AUS"),   # Austin-Bergstrom
    "KPHX": ("PSR", "PHX"),   # Phoenix Sky Harbor
    "KSFO": ("MTR", "SFO"),   # San Francisco Intl
    "KATL": ("FFC", "ATL"),   # Atlanta Hartsfield
    "KDCA": ("LWX", "DCA"),   # Reagan National, DC
    "KDEN": ("BOU", "DEN"),   # Denver Intl
    "KHOU": ("HGX", "HOU"),   # Houston Hobby
    "KMSP": ("MPX", "MSP"),   # Minneapolis-St Paul
    "KBOS": ("BOX", "BOS"),   # Boston Logan
    "KLAS": ("VEF", "LAS"),   # Las Vegas McCarran
    "KPHL": ("PHI", "PHL"),   # Philadelphia Intl
    "KOKC": ("OUN", "OKC"),   # Oklahoma City
    "KSEA": ("SEW", "SEA"),   # Seattle-Tacoma
}

def fetch_nws_temp_cli(nws_station, version=1):
    """
    Fetch NWS Daily Climate Report (CLI) for a temperature station.
    Parses the daily maximum and minimum temperature.
    Returns: {"ok": True, "high": float, "low": float, "date": str,
              "issued": str, "is_final": bool, "source": str}

    The CLI is issued ~7 AM local time the morning after the observation day.
    During DST, the NWS 24-hr window runs midnight-to-midnight *standard* time,
    so the high temperature can appear up to 1 AM the following calendar day.
    """
    cli_info = NWS_TEMP_CLI.get(nws_station)
    if not cli_info:
        return {"ok": False, "error": f"No CLI config for {nws_station}"}

    site, issuedby = cli_info
    base_url = (f"https://forecast.weather.gov/product.php"
                f"?site={site}&issuedby={issuedby}&product=CLI&format=txt")

    headers = {"User-Agent": "Mozilla/5.0"}
    last_err = "No versions tried"

    for ver in range(1, 6):
        try:
            url = base_url if ver == 1 else f"{base_url}&version={ver}"
            r   = requests.get(url, headers=headers, timeout=8)
            r.raise_for_status()

            soup = BeautifulSoup(r.text, "html.parser")
            pre  = soup.find("pre")
            raw  = pre.get_text() if pre else r.text

            # ── Parse issued time ─────────────────────────────────────────────
            issued_match = re.search(
                r"(\d{3,4}\s+(?:AM|PM)\s+\w+\s+\w+\s+\w+\s+\d+\s+\d{4})", raw, re.IGNORECASE)
            issued_str  = issued_match.group(1).strip() if issued_match else None
            issued_hour = None
            is_final    = False

            if issued_str:
                h_m = re.search(r"(\d{3,4})\s+(AM|PM)", issued_str, re.IGNORECASE)
                if h_m:
                    raw_h = int(h_m.group(1))
                    ampm  = h_m.group(2).upper()
                    hh    = (raw_h // 100) % 12 + (12 if ampm == "PM" else 0)
                    if ampm == "AM" and raw_h // 100 == 12:
                        hh = 0
                    issued_hour = hh
                    is_final    = (4 <= hh <= 12)  # morning report = final daily

            # ── Parse date ────────────────────────────────────────────────────
            date_match = re.search(
                r"CLIMATE (?:SUMMARY|REPORT) FOR\s+([\w\s,]+\d{4})", raw, re.IGNORECASE)
            cli_date = date_match.group(1).strip() if date_match else "Unknown"

            # ── Parse temperatures ────────────────────────────────────────────
            # NWS CLI temperature section looks like:
            #
            # TEMPERATURE (F)
            # MAXIMUM          54           56          56
            # MINIMUM          28           34          35
            #
            # We want the first (observed) column.
            temp_section = re.search(
                r"TEMPERATURE\s*\(F\)(.*?)(?:PRECIPITATION|SNOWFALL|DEGREE|WIND|$)",
                raw, re.IGNORECASE | re.DOTALL)
            section_txt = temp_section.group(1) if temp_section else raw

            max_match = re.search(
                r"MAXIMUM\s+(\d+)", section_txt, re.IGNORECASE)
            min_match = re.search(
                r"MINIMUM\s+(\d+)", section_txt, re.IGNORECASE)

            if not max_match or not min_match:
                last_err = f"v{ver}: could not parse MAXIMUM/MINIMUM from CLI"
                continue

            high_f = float(max_match.group(1))
            low_f  = float(min_match.group(1))

            # Sanity checks — °F range for US cities
            if not (-60 <= high_f <= 135) or not (-60 <= low_f <= 135):
                last_err = f"v{ver}: temperature out of range ({high_f}/{low_f})"
                continue
            if low_f > high_f:
                last_err = f"v{ver}: low {low_f} > high {high_f} — bad parse"
                continue

            return {
                "ok":        True,
                "high":      high_f,
                "low":       low_f,
                "date":      cli_date,
                "issued":    issued_str,
                "issued_hour": issued_hour,
                "is_final":  is_final,
                "station":   nws_station,
                "source":    f"NWS CLI {issuedby}",
                "version":   ver,
                "raw_snippet": section_txt[:300],
            }

        except Exception as e:
            last_err = str(e)
            continue

    return {"ok": False, "error": f"All CLI versions failed: {last_err}",
            "station": nws_station}


# ── AUTO-SETTLEMENT ENGINE ────────────────────────────────────────────────────
# Background thread polls NWS CLI for all temp cities each morning and
# auto-fills settled_temp + settled_correct in temp_snapshots.
#
# Design:
#   • Runs every SETTLE_POLL_INTERVAL seconds (default: 3600 = hourly)
#   • Between SETTLE_WINDOW_START and SETTLE_WINDOW_END ET (7 AM – 2 PM)
#     to catch the final CLI report without hammering NWS overnight
#   • Looks for temp_snapshots rows where:
#       target_date = yesterday  AND  settled_temp IS NULL
#   • Fetches NWS CLI for the station, marks rows settled_correct
#   • Logs results to _SETTLE_LOG for the /temp/settle-log endpoint

import threading as _threading
import time as _time_mod

SETTLE_POLL_INTERVAL  = 3600        # seconds between settlement poll runs
SETTLE_WINDOW_START   = 7           # 7 AM ET — CLI is usually posted by now
SETTLE_WINDOW_END     = 14          # 2 PM ET — stop checking once market is closing
_SETTLE_LOG           = []          # last N settlement results, in memory
_SETTLE_LOCK          = _threading.Lock()
_SETTLE_THREAD        = None
_LAST_SETTLE_RUN      = {}          # date_str → result dict, prevents duplicate runs
_PROP_LOG             = []          # last N _paper_trade_settle results — propagation tracking
_PROP_LOCK            = _threading.Lock()
_SCAN_ERR_LOG         = []          # last N temp_snapshots INSERT failures
_SCAN_ERR_LOCK        = _threading.Lock()
_SCAN_ERR_LOGGED      = {}          # city_key → True (dedupes per-city per-process)

# ── AUTO-TRADER GLOBALS ───────────────────────────────────────────────────────
_AT_THREAD   = None
_AT_LOCK     = _threading.Lock()
_AT_LOG      = []          # in-memory log, last 500 entries
_AT_ENABLED  = False       # master on/off — persisted in DB config table

# Default config — overridden by DB config table after first UI save
_AT_CONFIG = {
    "enabled":        False,
    "min_grade":      "A",
    "horizons":       ["d0", "d1"],
    "kelly_mult":     0.50,
    "bankroll_unit":  100.0,
    "max_per_fill":   25.0,
    "max_per_ticker": 75.0,
    "max_positions":  10,
    "max_per_city":   2,
    "min_volume":     200,
    "scan_interval":  300,   # seconds between scans
    "min_fill_dollars": 5.0, # skip execution if Kelly budget < this
}


def at_log(level, msg, ticker=None, city=None, extra=None):
    """Append one line to the in-memory AT log. DB write is batched separately."""
    import time as _t
    entry = {
        "ts":     _t.time(),
        "ts_str": __import__("datetime").datetime.utcnow().strftime("%H:%M:%S"),
        "level":  level,
        "msg":    msg,
        "ticker": ticker,
        "city":   city,
        "extra":  extra or {},
        "_flushed": False,
    }
    with _AT_LOCK:
        _AT_LOG.append(entry)
        if len(_AT_LOG) > 500:
            _AT_LOG.pop(0)


def at_flush_log_to_db():
    """Write any un-flushed in-memory log entries to DB. Call after each cycle."""
    try:
        with _AT_LOCK:
            unflushed = [e for e in _AT_LOG if not e.get("_flushed")]
        if not unflushed:
            return
        conn = get_db()
        if not conn:
            return
        with conn.cursor() as cur:
            for e in unflushed:
                cur.execute("""
                    INSERT INTO auto_trader_log (ts, level, msg, ticker, city, extra)
                    VALUES (to_timestamp(%s), %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (e["ts"], e["level"], e["msg"], e["ticker"], e["city"],
                      __import__("json").dumps(e["extra"] or {})))
                e["_flushed"] = True
        conn.commit()
        conn.close()
    except Exception:
        pass


def at_place_order(ticker, side, count, yes_price_c):
    """
    Place a single order on Kalshi. Returns (ok, order_dict, error_str).
    """
    try:
        payload = {
            "ticker":    ticker,
            "side":      side,
            "action":    "buy",
            "type":      "limit",
            "count":     count,
            "yes_price": yes_price_c,
        }
        r = requests.post(
            f"{KALSHI_BASE}/orders",
            headers=kalshi_auth_headers("POST", "/trade-api/v2/orders"),
            json=payload, timeout=10
        )
        # Safe JSON parse — API occasionally returns non-JSON on errors
        try:
            resp = r.json()
        except Exception:
            resp = {"raw": r.text[:200]}

        if r.ok:
            return True, resp, None
        else:
            return False, None, f"HTTP {r.status_code}: {resp}"
    except Exception as e:
        return False, None, str(e)


def at_fetch_market(ticker):
    """Fetch live orderbook for one ticker. Returns dict with yes_ask, yes_ask_size or None."""
    try:
        r = requests.get(
            f"{KALSHI_BASE}/markets/{ticker}/orderbook",
            headers=kalshi_auth_headers("GET", f"/trade-api/v2/markets/{ticker}/orderbook"),
            timeout=8
        )
        if not r.ok:
            return None
        ob = r.json().get("orderbook", {})
        yes_asks = ob.get("yes", [])   # [[price_c, size], ...]
        if not yes_asks:
            return None
        best = sorted(yes_asks, key=lambda x: x[0])[0]
        return {"yes_ask": best[0] / 100.0, "yes_ask_size": best[1]}
    except Exception:
        return None


def fetch_full_orderbook_liq(ticker, mu, sigma, lo, hi, min_edge_ratio=0.07):
    """
    Fetch full YES orderbook for ticker and compute grade-A-fillable dollars.
    Walks each price level, recomputes edge_ratio at that price, sums dollars
    available while grade stays A (edge_ratio >= 0.12 and prob >= 0.50).

    Returns dict:
        fillable_a_dollars  — dollars fillable while grade = A
        fillable_b_dollars  — dollars fillable while grade = A or B
        best_ask_c          — best ask in cents
        levels              — list of {price_c, size, prob, edge_ratio, grade}
        liq_grade           — A/B/C/D based on fillable_a_dollars
    """
    from math import erf, sqrt as _sqrt
    try:
        r = requests.get(
            f"{KALSHI_BASE}/markets/{ticker}/orderbook",
            headers=kalshi_auth_headers("GET", f"/trade-api/v2/markets/{ticker}/orderbook"),
            timeout=8
        )
        if not r.ok:
            return None
        yes_asks = r.json().get("orderbook", {}).get("yes", [])
        if not yes_asks:
            return None

        # Sort ascending by price
        levels_raw = sorted(yes_asks, key=lambda x: x[0])

        def _cdf(x):
            if sigma <= 0: return 1.0 if x > mu else 0.0
            return 0.5 * (1 + erf((x - mu) / (sigma * _sqrt(2))))

        def _prob(lo, hi):
            upper = hi + 0.5 if hi is not None else float('inf')
            lower = lo - 0.5 if lo is not None else float('-inf')
            p_hi = _cdf(upper) if hi is not None else 1.0
            p_lo = _cdf(lower) if lo is not None else 0.0
            return max(0.0, p_hi - p_lo)

        fillable_a = 0.0
        fillable_b = 0.0
        levels_out = []
        best_ask_c = levels_raw[0][0] if levels_raw else None

        for price_c, size in levels_raw:
            ask = price_c / 100.0
            if ask <= 0 or ask >= 1:
                continue
            prob = _prob(lo, hi)
            gap_c = round((prob - ask) * 100)
            er = round(gap_c / sigma, 3) if sigma > 0 else 0

            if er >= 0.12 and prob >= 0.50:
                lvl_grade = "A"
            elif er >= 0.07 and prob >= 0.35:
                lvl_grade = "B"
            else:
                lvl_grade = "C"

            dollars = round(size * ask, 2)
            if lvl_grade == "A":
                fillable_a += dollars
            if lvl_grade in ("A", "B"):
                fillable_b += dollars

            levels_out.append({
                "price_c": price_c, "size": size,
                "prob": round(prob, 3), "edge_ratio": er,
                "grade": lvl_grade, "dollars": dollars
            })

        # Liq grade based on A-grade fillable dollars
        if fillable_a >= 75:
            liq_grade = "A"
        elif fillable_a >= 25:
            liq_grade = "B"
        elif fillable_a >= 5:
            liq_grade = "C"
        else:
            liq_grade = "D"

        return {
            "fillable_a_dollars": round(fillable_a, 2),
            "fillable_b_dollars": round(fillable_b, 2),
            "best_ask_c":         best_ask_c,
            "levels":             levels_out,
            "liq_grade":          liq_grade,
        }
    except Exception:
        return None


def at_get_open_positions():
    """Return list of open weather position tickers and city counts."""
    try:
        r = requests.get(
            f"{KALSHI_BASE}/portfolio/positions",
            headers=kalshi_auth_headers("GET", "/trade-api/v2/portfolio/positions"),
            timeout=10, params={"limit": 200, "count_filter": "position"}
        )
        if not r.ok:
            return [], {}
        positions = r.json().get("market_positions", [])
        weather = [p for p in positions
                   if re.search(r"RAIN|KXHIGH|KXLOWT|KXLOWS", p.get("ticker", ""))]
        city_counts = {}
        for p in weather:
            t = p.get("ticker", "")
            # Extract city from ticker e.g. KXHIGHMIA → MIA
            m = re.match(r"KXHIGH(\w{2,4})-|KXLOWT(\w{2,4})-|KXLOWS(\w{2,4})-", t)
            city = (m.group(1) or m.group(2) or m.group(3) or "UNK") if m else "UNK"
            city_counts[city] = city_counts.get(city, 0) + 1
        return weather, city_counts
    except Exception:
        return [], {}


def at_execute_signal(signal, cfg, open_positions, city_counts, ticker_spent):
    """
    Execute the fill loop for one signal.
    Walks the order book while grade holds at min_grade.
    Kelly is calculated once at scan time and used as the total budget
    for this ticker across all fills — not recalculated per level.

    For combo signals (is_combo=True): places two separate orders, one per leg,
    splitting the Kelly budget evenly. Both legs must pass grade check.
    Returns number of fills placed.
    """
    # ── Combo signal handler ────────────────────────────────────────────────
    if signal.get("is_combo"):
        leg_a   = signal.get("leg_a", {})
        leg_b   = signal.get("leg_b", {})
        city_key = signal.get("city_key", "")
        at_log("SCAN", f"Combo signal: {leg_a.get('ticker')} + {leg_b.get('ticker')} "
               f"combined_prob={signal.get('combined_prob',0):.1%} "
               f"net_edge={signal.get('combined_net_edge_c')}¢",
               city=city_key)
        # Split Kelly budget evenly across both legs
        combo_budget = min(
            signal.get("kelly_size", 0.0) or 0.0,
            cfg.get("max_per_ticker", 75.0)
        ) * (cfg.get("bankroll_unit", 100.0) / 100.0) * (cfg.get("kelly_mult", 0.5) / 0.5)
        leg_budget = round(combo_budget / 2, 2)
        signal["kelly_size"] = leg_budget

        fills = 0
        for leg in [leg_a, leg_b]:
            leg["city_key"] = city_key
            leg["forecast"] = signal.get("forecast", {})
            leg_signal = dict(signal)
            leg_signal.update(leg)
            leg_signal["is_combo"] = False   # prevent recursion
            leg_signal["kelly_size"] = leg_budget
            fills += at_execute_signal(leg_signal, cfg, open_positions, city_counts, ticker_spent)
        return fills

    ticker    = signal.get("ticker", "")
    city_key  = signal.get("city_key", "")
    grade     = signal.get("grade", "")
    mu        = signal.get("mu")
    sigma     = signal.get("sigma")
    forecast  = signal.get("forecast", {})

    # City code for concentration limit
    m = re.match(r"KXHIGH(\w{2,4})-|KXLOWT(\w{2,4})-|KXLOWS(\w{2,4})-", ticker)
    city_code = (m.group(1) or m.group(2) or m.group(3) or city_key[:4].upper()) if m else city_key[:4].upper()

    min_grade  = cfg.get("min_grade", "A")
    grade_rank = {"A": 0, "B": 1, "C": 2, "D": 3, "skip": 99}
    fills      = 0
    spent_this_ticker = ticker_spent.get(ticker, 0.0)

    # Use scan-time Kelly as the total budget for this ticker.
    # Recalculating per level would allow over-deployment on weak signals.
    # Scale by config bankroll_unit / 100 to respect current settings.
    scan_kelly_sz  = signal.get("kelly_size", 0.0) or 0.0
    # Rescale if bankroll_unit differs from default $100
    bankroll_scale = cfg.get("bankroll_unit", 100.0) / 100.0
    kelly_mult_scale = cfg.get("kelly_mult", 0.5) / 0.5  # scale vs default 0.5x
    kelly_budget   = round(scan_kelly_sz * bankroll_scale * kelly_mult_scale, 2)
    kelly_budget   = min(kelly_budget, cfg.get("max_per_ticker", 75.0))

    # Min fill check — if Kelly budget is below threshold, skip entirely
    min_fill = cfg.get("min_fill_dollars", 5.0)
    if kelly_budget < min_fill:
        at_log("SKIP",
               f"{ticker} Kelly budget ${kelly_budget:.2f} < min_fill ${min_fill:.2f} — skipping",
               ticker=ticker, city=city_key)
        return 0

    at_log("SCAN", f"Evaluating {ticker} grade={grade} mu={mu}°F kelly_budget=${kelly_budget:.2f}",
           ticker=ticker, city=city_key)

    while True:
        # Portfolio-level checks
        if len(open_positions) >= cfg.get("max_positions", 10):
            at_log("SKIP", f"Max positions reached ({cfg['max_positions']})", ticker=ticker)
            break
        if city_counts.get(city_code, 0) >= cfg.get("max_per_city", 2):
            at_log("SKIP", f"City limit reached for {city_code}", ticker=ticker, city=city_key)
            break

        # Kelly budget is the binding constraint — replaces max_per_ticker in the loop
        kelly_remain = kelly_budget - spent_this_ticker
        if kelly_remain <= 0:
            at_log("SKIP", f"{ticker} Kelly budget exhausted (${kelly_budget:.2f} spent)",
                   ticker=ticker)
            break

        # Fetch live market at current price
        market = at_fetch_market(ticker)
        if not market:
            # Empty orderbook = market not yet open for trading. Skip entirely.
            at_log("SKIP", f"{ticker} orderbook empty — market not yet open for trading",
                   ticker=ticker, city=city_key)
            break

        ask      = market["yes_ask"]
        ask_size = market["yes_ask_size"]
        ask_c    = round(ask * 100)

        # Recalculate grade at current ask price
        if mu is not None and sigma and sigma > 0 and ask > 0 and ask < 1:
            from math import erf, sqrt as _sqrt
            lo = signal.get("lo_temp")
            hi = signal.get("hi_temp")

            def _cdf(x):
                return 0.5 * (1 + erf((x - mu) / (sigma * _sqrt(2))))

            if lo is None and hi is not None:
                prob = _cdf(hi + 0.5)
            elif hi is None and lo is not None:
                prob = 1 - _cdf(lo - 0.5)
            else:
                prob = _cdf(hi + 0.5) - _cdf(lo - 0.5)

            gap_c      = round((prob - ask) * 100)
            net_gap_c  = max(0, gap_c - round(signal.get("spread_c", 1)))
            edge_ratio = round(net_gap_c / sigma, 3) if sigma > 0 else 0

            if net_gap_c <= 0:
                live_grade = "skip"
            elif edge_ratio >= 0.12 and prob >= 0.50 and net_gap_c >= 8:
                live_grade = "A"
            elif edge_ratio >= 0.07 and prob >= 0.35 and net_gap_c >= 5:
                live_grade = "B"
            else:
                live_grade = "C"
        else:
            live_grade = grade
            prob       = signal.get("model_prob", 0)

        # Stop if grade degraded below threshold
        if grade_rank.get(live_grade, 99) > grade_rank.get(min_grade, 0):
            at_log("SKIP", f"{ticker} grade degraded to {live_grade} at {ask_c}¢ — stopping",
                   ticker=ticker, city=city_key)
            break

        # Size this fill: capped by book depth, max_per_fill, and remaining Kelly budget
        book_cap = round(ask_size * ask, 2)
        fill_sz  = min(book_cap, cfg.get("max_per_fill", 25.0), kelly_remain)
        fill_sz  = round(fill_sz, 2)

        if fill_sz < min_fill:
            at_log("SKIP", f"{ticker} fill size ${fill_sz:.2f} < min_fill ${min_fill:.2f} — stopping",
                   ticker=ticker)
            break

        # Convert dollars to contract count
        count = max(1, int(fill_sz / ask))

        # Place the order
        ok, order, err = at_place_order(ticker, "yes", count, ask_c)
        if not ok:
            at_log("ERR", f"Order failed for {ticker}: {err}", ticker=ticker, city=city_key)
            break

        cost = round(count * ask, 2)
        spent_this_ticker += cost
        ticker_spent[ticker] = spent_this_ticker
        fills += 1

        # Update open positions count optimistically
        if ticker not in [p.get("ticker") for p in open_positions]:
            open_positions.append({"ticker": ticker})
            city_counts[city_code] = city_counts.get(city_code, 0) + 1

        at_log("PLACE",
               f"Placed {count} × {ticker} @ {ask_c}¢ = ${cost:.2f} | grade={live_grade} "
               f"prob={round(prob*100)}% | budget ${kelly_budget:.2f} spent ${spent_this_ticker:.2f}",
               ticker=ticker, city=city_key,
               extra={"count": count, "ask_c": ask_c, "cost": cost,
                      "grade": live_grade, "prob": round(prob, 3),
                      "kelly_budget": kelly_budget, "spent": spent_this_ticker})

        # Write to model_forecasts for later accuracy analysis
        try:
            conn = get_db()
            if conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO model_forecasts
                            (city, nws_station, target_date, gfs_high, ecmwf_high,
                             spread_gfs_ecmwf)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (city, target_date) DO UPDATE SET
                            gfs_high  = COALESCE(EXCLUDED.gfs_high, model_forecasts.gfs_high),
                            ecmwf_high= COALESCE(EXCLUDED.ecmwf_high, model_forecasts.ecmwf_high)
                    """, (
                        city_key,
                        forecast.get("nws_station", ""),
                        forecast.get("target_date", ""),
                        forecast.get("gfs_high"),
                        forecast.get("ecmwf_high"),
                        signal.get("spread_models"),
                    ))
                conn.commit()
                conn.close()
        except Exception:
            pass

    return fills


def run_auto_trader_cycle(force=False):
    """
    One full scan-and-execute cycle.
    Called by the scheduler every N seconds, or manually via /auto-trader/run.
    force=True bypasses the enabled check for manual testing.
    """
    global _AT_CONFIG
    cfg = dict(_AT_CONFIG)   # snapshot config

    if not force and not cfg.get("enabled", False):
        return

    at_log("SCAN", f"Cycle start — scanning {len(TEMP_CITIES)} cities "
           f"horizons={cfg['horizons']} min_grade={cfg['min_grade']}")

    # Get current open positions
    open_positions, city_counts = at_get_open_positions()

    if len(open_positions) >= cfg.get("max_positions", 5):
        at_log("SKIP", f"Max positions already open ({len(open_positions)}) — skipping cycle")
        return

    total_fills   = 0
    ticker_spent  = {}   # tracks spend per ticker across fills this cycle

    for horizon in cfg.get("horizons", ["d0", "d1"]):
        for city_key in TEMP_CITIES:
            # Skip blacklisted cities — calibration still runs, bets do not
            if city_key in AUTO_TRADER_CITY_BLACKLIST:
                at_log("SKIP", f"{city_key} is blacklisted — skipping auto-trade (calibration continues)", city=city_key)
                continue
            try:
                result = scan_temp_city(city_key, horizon)
                if not result.get("ok"):
                    continue

                all_markets = result.get("high_markets", []) + result.get("low_markets", []) + result.get("combo_signals", [])
                for signal in all_markets:
                    g = signal.get("grade", "skip")
                    if g == "skip" or not signal.get("actionable"):
                        continue

                    # Only filter on grade — the execution loop handles
                    # sizing, book depth, and per-fill grade re-evaluation
                    grade_rank = {"A": 0, "B": 1, "C": 2}
                    min_rank   = grade_rank.get(cfg.get("min_grade", "A"), 0)
                    if grade_rank.get(g, 99) > min_rank:
                        continue

                    # Enrich signal with forecast for model_forecasts logging
                    signal["forecast"] = result.get("forecast", {})
                    signal["city_key"] = city_key

                    fills = at_execute_signal(
                        signal, cfg, open_positions, city_counts, ticker_spent)
                    total_fills += fills

            except Exception as e:
                at_log("ERR", f"Error scanning {city_key}/{horizon}: {e}", city=city_key)

    at_log("SCAN", f"Cycle complete — {total_fills} fill(s) placed")
    at_flush_log_to_db()  # batch write cycle entries to DB


def _auto_trader_scheduler():
    """Background thread — runs run_auto_trader_cycle() every scan_interval seconds."""
    import time as _t
    at_log("SCAN", "Auto-trader scheduler started")
    while True:
        try:
            if _AT_CONFIG.get("enabled", False):
                run_auto_trader_cycle()
        except Exception as e:
            at_log("ERR", f"Scheduler error: {e}")
        interval = _AT_CONFIG.get("scan_interval", 300)
        _t.sleep(interval)


def start_auto_trader_scheduler():
    """Launch the background auto-trader thread (daemon)."""
    global _AT_THREAD
    if _AT_THREAD and _AT_THREAD.is_alive():
        return
    _AT_THREAD = _threading.Thread(
        target=_auto_trader_scheduler, daemon=True, name="AutoTrader")
    _AT_THREAD.start()
    print("  🤖 Auto-trader scheduler started")


def at_load_config_from_db():
    """Load persisted config from DB on startup."""
    global _AT_CONFIG
    try:
        conn = get_db()
        if not conn:
            return
        with conn.cursor() as cur:
            cur.execute("SELECT key, value FROM auto_trader_config")
            rows = cur.fetchall()
        conn.close()
        for key, val in rows:
            if key in _AT_CONFIG:
                # Cast to correct type
                orig = _AT_CONFIG[key]
                if isinstance(orig, bool):
                    _AT_CONFIG[key] = val == "true"
                elif isinstance(orig, int):
                    _AT_CONFIG[key] = int(float(val))
                elif isinstance(orig, float):
                    _AT_CONFIG[key] = float(val)
                elif isinstance(orig, list):
                    _AT_CONFIG[key] = __import__("json").loads(val)
                else:
                    _AT_CONFIG[key] = val
    except Exception as e:
        print(f"  ⚠️  at_load_config_from_db: {e}")


def run_auto_settlement(force=False):
    """
    Pull NWS CLI for yesterday's date for all temp cities and mark
    temp_snapshot rows as settled_correct.

    Called from the background scheduler thread and from /temp/auto-settle.
    force=True bypasses the time-window check (useful for manual trigger).
    """
    import pytz
    from datetime import datetime as dt_cls, timedelta

    et_tz    = pytz.timezone("America/New_York")
    now_et   = dt_cls.utcnow().replace(tzinfo=pytz.utc).astimezone(et_tz)
    hour_et  = now_et.hour
    today_et = now_et.strftime("%Y-%m-%d")

    if not force and not (SETTLE_WINDOW_START <= hour_et < SETTLE_WINDOW_END):
        return {"ok": True, "skipped": True,
                "reason": f"Outside settlement window ({hour_et} ET, window {SETTLE_WINDOW_START}–{SETTLE_WINDOW_END})"}

    # Target: yesterday in ET (the date whose high is in this morning's CLI)
    yesterday = (now_et - timedelta(days=1)).strftime("%Y-%m-%d")

    # Avoid re-running for the same date more than once per day — but only
    # short-circuit if a previous run successfully settled ALL cities that
    # had unsettled snapshots. Partial success must NOT block retry: NWS CLI
    # publishes between 6–9 AM ET per city, so the 7 AM scheduler run often
    # catches some cities before their CLI is up. Without retry, those cities
    # never get settled. (Bug fix: previously this short-circuited on any
    # successful run, leaving late-publishing cities permanently unsettled.)
    prev = _LAST_SETTLE_RUN.get(yesterday, {})
    if (not force and prev.get("ok")
        and prev.get("fully_settled") is True):
        return {"ok": True, "skipped": True,
                "reason": f"All cities fully settled for {yesterday}"}

    conn = get_db()
    if not conn:
        return {"ok": False, "error": "No DB connection"}

    results   = []
    settled   = 0
    attempted = 0

    try:
        # Find all cities that have unsettled snapshots for yesterday
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT city, nws_station
                FROM temp_snapshots
                WHERE target_date = %s AND settled_temp IS NULL
            """, (yesterday,))
            cities_needed = cur.fetchall()

        if not cities_needed:
            msg = {"ok": True, "date": yesterday, "settled": 0,
                   "fully_settled": True,  # nothing to do = fully done
                   "note": "No unsettled snapshots for this date"}
            with _SETTLE_LOCK:
                _SETTLE_LOG.insert(0, {**msg, "run_at": now_et.isoformat()})
                _SETTLE_LOG[:] = _SETTLE_LOG[:50]
            _LAST_SETTLE_RUN[yesterday] = msg
            conn.close()
            return msg

        import concurrent.futures as _cf
        def settle_city(city_key, station):
            cli = fetch_nws_temp_cli(station)
            if not cli.get("ok"):
                return {"city": city_key, "station": station, "ok": False,
                        "error": cli.get("error")}
            return {"city": city_key, "station": station, "ok": True,
                    "high": cli["high"], "low": cli["low"],
                    "date": cli["date"], "is_final": cli.get("is_final", False)}

        ex = _cf.ThreadPoolExecutor(max_workers=8)
        try:
            futs = {ex.submit(settle_city, ck, st): (ck, st)
                    for ck, st in cities_needed}
            done, _ = _cf.wait(futs, timeout=60)
            for f in done:
                try:
                    res = f.result()
                    results.append(res)
                except Exception as e:
                    results.append({"ok": False, "error": str(e)})
        finally:
            ex.shutdown(wait=False)

        # Write settlements to DB
        with conn.cursor() as cur:
            for res in results:
                if not res.get("ok"):
                    continue
                city    = res["city"]
                high_f  = res["high"]
                low_f   = res["low"]
                attempted += 1

                # Mark HIGH brackets
                # NWS CLI reports whole integers. Bracket "85–86" wins on 85 OR 86.
                # Correct condition: lo_temp <= actual <= hi_temp (both inclusive).
                # Edge brackets: lo_temp IS NULL means "below hi_temp" (wins if actual <= hi)
                #                hi_temp IS NULL means "above lo_temp" (wins if actual >= lo)
                cur.execute("""
                    UPDATE temp_snapshots
                    SET settled_temp    = %s,
                        settled_correct = (
                            (lo_temp IS NULL OR %s >= lo_temp) AND
                            (hi_temp IS NULL OR %s <= hi_temp)
                        )
                    WHERE city = %s AND target_date = %s
                      AND market_type = 'high' AND settled_temp IS NULL
                """, (high_f, high_f, high_f, city, yesterday))
                settled += cur.rowcount

                # Mark LOW brackets (same inclusive logic)
                cur.execute("""
                    UPDATE temp_snapshots
                    SET settled_temp    = %s,
                        settled_correct = (
                            (lo_temp IS NULL OR %s >= lo_temp) AND
                            (hi_temp IS NULL OR %s <= hi_temp)
                        )
                    WHERE city = %s AND target_date = %s
                      AND market_type = 'low' AND settled_temp IS NULL
                """, (low_f, low_f, low_f, city, yesterday))
                settled += cur.rowcount

        conn.commit()

    except Exception as e:
        try: conn.close()
        except: pass
        return {"ok": False, "error": str(e)}
    finally:
        try: conn.close()
        except: pass

    # fully_settled = every city we tried to settle returned ok=True from CLI fetch.
    # When this is False, the next scheduler tick will retry the failed cities.
    expected_cities = len(cities_needed)
    successful_cities = sum(1 for r in results if r.get("ok"))
    fully_settled = (successful_cities >= expected_cities and expected_cities > 0)

    summary = {
        "ok":              True,
        "date":             yesterday,
        "settled":          settled,
        "attempted":        attempted,
        "expected_cities":  expected_cities,
        "successful_cities": successful_cities,
        "fully_settled":    fully_settled,
        "cities":           results,
        "run_at":           now_et.isoformat(),
    }
    with _SETTLE_LOCK:
        _SETTLE_LOG.insert(0, summary)
        _SETTLE_LOG[:] = _SETTLE_LOG[:50]
    _LAST_SETTLE_RUN[yesterday] = summary
    print(f"  ✅ Auto-settle {yesterday}: {settled} brackets settled across {attempted} cities")
    return summary


def _settlement_scheduler():
    """Background thread: polls every SETTLE_POLL_INTERVAL seconds."""
    import time as _t
    print("  🕐 Auto-settlement scheduler started")
    while True:
        try:
            res = run_auto_settlement()
            # Propagate settlements from temp_snapshots → calibration_snapshots
            # + paper_trades. Run on EVERY scheduler tick (not just when new
            # settlements landed) so we backfill any prior settlements that
            # failed to propagate. _paper_trade_settle is a no-op if there's
            # nothing to update.
            _paper_trade_settle()
        except Exception as e:
            print(f"  ⚠️  Settlement scheduler error: {e}")
        _t.sleep(SETTLE_POLL_INTERVAL)


def start_settlement_scheduler():
    """Launch the background settlement thread (daemon — dies with server)."""
    global _SETTLE_THREAD
    if _SETTLE_THREAD and _SETTLE_THREAD.is_alive():
        return
    _SETTLE_THREAD = _threading.Thread(
        target=_settlement_scheduler, daemon=True, name="TempSettler")
    _SETTLE_THREAD.start()


# ── CALIBRATE-ON-STARTUP HELPER ───────────────────────────────────────────────
# Also schedule a weekly re-calibration to keep bias/σ fresh.
_LAST_CALIBRATE_DATE = None
_CALIBRATE_ERRORS    = []   # populated when calibration fails, cleared on success

def maybe_auto_calibrate():
    """
    Run /temp/calibrate for all cities on startup (non-blocking thread)
    and every Sunday night. Uses 90 days of historical data.
    """
    import threading as _t2
    import time as _t

    def _do_calibrate():
        global _LAST_CALIBRATE_DATE
        import pytz
        from datetime import datetime as dt_cls
        et_tz   = pytz.timezone("America/New_York")
        today_s = dt_cls.utcnow().replace(tzinfo=pytz.utc).astimezone(et_tz).strftime("%Y-%m-%d")

        if _LAST_CALIBRATE_DATE == today_s:
            return   # already ran today
        print("  🔧 Auto-calibrating temp bias/σ for all cities (90d)...")
        cities = list(TEMP_CITIES.keys())
        from math import sqrt
        import concurrent.futures as _cf

        def cal_one(city_key):
            """
            Calibrate one city using bulk date-range queries — one API call per
            model rather than one per day.  Avoids closure/loop-variable bugs
            and is ~90x faster than per-day fetching.
            """
            cfg = TEMP_CITIES[city_key]
            try:
                import pytz as _pytz
                from datetime import timedelta
                tz_name  = cfg["tz"]
                local_tz = _pytz.timezone(tz_name)
                now      = dt_cls.utcnow().replace(tzinfo=_pytz.utc).astimezone(local_tz)

                # Date range: 90 days ago → 2 days ago (yesterday's CLI may not be posted yet)
                end_dt   = now - timedelta(days=2)
                start_dt = now - timedelta(days=91)
                start_s  = start_dt.strftime("%Y-%m-%d")
                end_s    = end_dt.strftime("%Y-%m-%d")

                def _fetch_bulk(model_str, use_archive=False):
                    """Fetch daily max temps for the full date range in one call."""
                    base = ("https://archive-api.open-meteo.com/v1/archive"
                            if use_archive else
                            "https://api.open-meteo.com/v1/forecast")
                    p = {
                        "latitude":   cfg["lat"],
                        "longitude":  cfg["lon"],
                        "daily":      "temperature_2m_max",
                        "timezone":   tz_name,
                    }
                    if use_archive:
                        # Archive API: use explicit date range, no past_days
                        p["start_date"] = start_s
                        p["end_date"]   = end_s
                    else:
                        # Forecast API: past_days cannot be combined with start/end_date
                        # Request enough past days to cover our window, then filter
                        p["models"]    = model_str
                        p["past_days"] = 92
                        p["forecast_days"] = 1   # minimize future data returned
                    resp = requests.get(base, params=p, timeout=20)
                    d    = resp.json()
                    if d.get("error"):
                        raise ValueError(f"Open-Meteo error: {d.get('reason', d.get('error'))}")
                    dates = d.get("daily", {}).get("time", [])
                    vals  = d.get("daily", {}).get("temperature_2m_max", [])
                    # Filter to our target date range and convert °C → °F
                    result = {}
                    for date_str, val_c in zip(dates, vals):
                        if val_c is not None and start_s <= date_str <= end_s:
                            result[date_str] = round(val_c * 9/5 + 32, 1)
                    return result

                # Fetch all four in parallel: GFS, ECMWF, Blend, Archive actuals
                ex2 = _cf.ThreadPoolExecutor(max_workers=4)
                try:
                    fg = ex2.submit(_fetch_bulk, "gfs_seamless",  False)
                    fe = ex2.submit(_fetch_bulk, "ecmwf_ifs",     False)
                    fb = ex2.submit(_fetch_bulk, "best_match",    False)
                    fa = ex2.submit(_fetch_bulk, None,            True)
                    gfs_map    = fg.result(timeout=30)
                    ecmwf_map  = fe.result(timeout=30)
                    blend_map  = fb.result(timeout=30)
                    actual_map = fa.result(timeout=30)
                finally:
                    ex2.shutdown(wait=False)

                errs_gfs = []; errs_ecmwf = []; errs_blend = []
                for date_s, actual in actual_map.items():
                    if date_s in gfs_map:
                        errs_gfs.append(gfs_map[date_s] - actual)
                    if date_s in ecmwf_map:
                        errs_ecmwf.append(ecmwf_map[date_s] - actual)
                    if date_s in blend_map:
                        errs_blend.append(blend_map[date_s] - actual)

                def _stats(e):
                    if not e: return 0.0, cfg["σ_d1"]
                    n = len(e)
                    return round(sum(e)/n, 2), round(sqrt(sum(x**2 for x in e)/n), 2)

                gb, gr = _stats(errs_gfs)
                eb, er = _stats(errs_ecmwf)
                bb, br = _stats(errs_blend)

                # ECMWF mirror detection — RMSE < 0.3°F means same data as archive
                ecmwf_is_mirror = (er < 0.3 and len(errs_ecmwf) >= 5)
                if ecmwf_is_mirror:
                    eb = 0.0; er = 99.0  # exclude from best-model selection

                # ── Pick best model per city based on lowest bias-corrected RMSE ──
                # Bias-corrected RMSE: after removing the mean error, what's left?
                # This is the true forecast skill — a biased model can still be
                # good after correction. We want the model with tightest residuals.
                def _bc_rmse(errs, bias):
                    if not errs or len(errs) < 5: return 99.0
                    bc = [e - bias for e in errs]
                    return round(sqrt(sum(x**2 for x in bc) / len(bc)), 2)

                bc_rmse_gfs   = _bc_rmse(errs_gfs,   gb)
                bc_rmse_ecmwf = _bc_rmse(errs_ecmwf, eb) if not ecmwf_is_mirror else 99.0
                bc_rmse_blend = _bc_rmse(errs_blend, bb)

                candidates = {
                    "gfs":   (bc_rmse_gfs,   gb, gr),
                    "ecmwf": (bc_rmse_ecmwf, eb, er),
                    "blend": (bc_rmse_blend, bb, br),
                }
                best_model = min(candidates, key=lambda k: candidates[k][0])
                best_bc_rmse, best_bias, best_rmse = candidates[best_model]

                # σ_d1 = RMSE of the best model (already bias-corrected = bc_rmse)
                σ_d1 = round(best_bc_rmse, 2) if best_bc_rmse < 90 else cfg["σ_d1"]

                _TEMP_BIAS_CACHE[city_key] = {
                    "gfs_bias":       gb,
                    "ecmwf_bias":     eb,
                    "blend_bias":     bb,
                    "gfs_rmse":       gr,
                    "ecmwf_rmse":     er if not ecmwf_is_mirror else 0.0,
                    "blend_rmse":     br,
                    "gfs_bc_rmse":    bc_rmse_gfs,
                    "ecmwf_bc_rmse":  bc_rmse_ecmwf,
                    "blend_bc_rmse":  bc_rmse_blend,
                    "best_model":     best_model,   # "gfs", "ecmwf", or "blend"
                    "best_model_bias": best_bias,
                    "σ_d1":           σ_d1,
                    "σ_d0":           round(σ_d1 * 0.70, 2),
                    "n_days":         len(errs_gfs),
                    "calibrated_at":  _t.time(),
                }
                print(f"    ✓ {city_key}: best={best_model} (bc_rmse={best_bc_rmse}°F) "
                      f"gfs={bc_rmse_gfs} ecmwf={bc_rmse_ecmwf} blend={bc_rmse_blend} σ={σ_d1}°F")
                return city_key, True
            except Exception as e:
                print(f"    ✗ {city_key} calibration failed: {e}")
                return city_key, False

        _cal_errors = []
        ex = _cf.ThreadPoolExecutor(max_workers=4)
        try:
            futs = {ex.submit(cal_one, ck): ck for ck in cities}
            done, _ = _cf.wait(futs, timeout=600)
            ok = 0
            for f in done:
                try:
                    city_k, success = f.result()
                    if success:
                        ok += 1
                    else:
                        _cal_errors.append(city_k)
                except Exception as e:
                    _cal_errors.append(str(e))
        finally:
            ex.shutdown(wait=False)

        if ok > 0:
            _LAST_CALIBRATE_DATE = today_s   # only mark done if at least one city succeeded
            _CALIBRATE_ERRORS.clear()
            print(f"  ✅ Auto-calibration complete: {ok}/{len(cities)} cities updated")
        else:
            _CALIBRATE_ERRORS[:] = _cal_errors[:10]
            print(f"  ⚠️  Auto-calibration: 0/{len(cities)} cities succeeded — errors: {_cal_errors[:3]}")
            # Don't set _LAST_CALIBRATE_DATE so it retries next poll

    t = _t2.Thread(target=_do_calibrate, daemon=True, name="TempCalibrate")
    t.start()


# Active city for this deployment — change via CITY env var on Railway
# e.g. set CITY=portland on your Portland deployment
ACTIVE_CITY  = os.environ.get("CITY", "seattle")
CITY_CFG     = CITIES.get(ACTIVE_CITY, CITIES["seattle"])
WU_ICAO_CODE = CITY_CFG["icao_code"]
KALSHI_SERIES = CITY_CFG["kalshi_series"]
KALSHI_BASE  = "https://api.elections.kalshi.com/trade-api/v2"
NWS_URL      = (f"https://forecast.weather.gov/product.php"
                f"?site={CITY_CFG['nws_site']}"
                f"&issuedby={CITY_CFG['nws_issuedby']}"
                f"&product=CLI&format=txt")
# ─────────────────────────────────────────────────────────────────────────────


def validate_forecast_data(data, days, total, city_cfg=None):
    """
    Run sanity checks on WU forecast data.
    Returns (is_valid, warning_message)
    """
    cfg = city_cfg or CITY_CFG
    warnings = []
    today = datetime.now()
    month = today.month

    # 1. Check we got enough days
    if len(days) < 5:
        return False, "WU returned fewer than 5 forecast days — data incomplete"

    # 2. Check first forecast date matches today or yesterday (stale data check)
    if days:
        first_date = days[0].get("date", "")
        if first_date:
            try:
                forecast_date = datetime.strptime(first_date, "%Y-%m-%d")
                delta = abs((today - forecast_date).days)
                if delta > 2:
                    warnings.append(f"Forecast appears stale - first date is {first_date}")
            except Exception:
                pass

    # 3. Zero-rain sanity check during expected wet season for this city
    # Only flag if city has a defined wet season and we're in it
    wet_months = cfg.get("tradeable_months", [])
    if wet_months and month in wet_months and total == 0.0:
        label = cfg.get("label", "this city")
        return False, f"WU returned 0.00\" total during {label} wet season - likely bad data"

    # 4. Unreasonably high values (>15" in 7 days would be record-breaking for any US city)
    if total > 15.0:
        return False, f"WU returned {total}\" over 7 days - unrealistically high, likely bad data"

    # 5. Check qpf field actually exists and has numeric values
    for d in days:
        qpf_val = d.get("qpf")
        if qpf_val is not None and not isinstance(qpf_val, (int, float)):
            return False, "WU returned non-numeric QPF values — data structure may have changed"

    # 6. Check API key wasn't rotated (would return error JSON instead of forecast)
    if not data.get("validTimeLocal"):
        return False, "WU response missing expected fields — API key may have rotated, use manual entry"

    if warnings:
        return True, warnings[0]  # Valid but with a warning
    return True, None


def fetch_wu_forecast(city_cfg=None):
    """
    Fetch 10-day daily QPF from Open-Meteo (replaces Weather Underground).
    Open-Meteo uses GFS + ECMWF — same underlying models as WU.
    Free, stable, no API key, no rotation risk.
    Returns same structure as old WU function for backward compatibility.
    """
    cfg = city_cfg or CITY_CFG
    try:
        params = {
            "latitude":      cfg["lat"],
            "longitude":     cfg["lon"],
            "daily":         "precipitation_sum",
            "timezone":      cfg["tz"],
            "forecast_days": 16,
            "models":        "best_match",
        }
        r = requests.get(OM_DAILY_URL, params=params, timeout=6)
        r.raise_for_status()
        data = r.json()

        if "error" in data:
            return {"ok": False, "error": data["error"], "days": [], "total_forecast": 0,
                    "source": "Open-Meteo", "needs_manual": False}

        dates = data.get("daily", {}).get("time", [])
        precip = data.get("daily", {}).get("precipitation_sum", [])

        days = []
        for i, date_str in enumerate(dates):
            qpf_mm = float(precip[i] or 0) if i < len(precip) else 0.0
            qpf_in = round(qpf_mm / 25.4, 3)  # mm → inches
            days.append({
                "date":      date_str,
                "dayOfWeek": "",
                "qpf":       qpf_in,
                "narrative": "",
            })

        # Validation: sanity check values
        total = round(sum(d["qpf"] for d in days), 2)
        now_month = datetime.now().month
        wet_months = cfg.get("tradeable_months", [])
        if wet_months and now_month in wet_months and total == 0.0:
            return {"ok": False,
                    "error": f"Open-Meteo returned 0.00 inches during {cfg.get('label','')} wet season - check coordinates",
                    "days": [], "total_forecast": 0, "source": "Open-Meteo", "needs_manual": False}
        if total > 15.0:
            return {"ok": False, "error": f"Open-Meteo returned {total} inches - unrealistically high",
                    "days": [], "total_forecast": 0, "source": "Open-Meteo", "needs_manual": False}

        return {
            "ok":            True,
            "days":          days,
            "total_forecast": total,
            "source":        "Open-Meteo (GFS/ECMWF best match)",
            "warning":       None,
            "needs_manual":  False,
        }

    except requests.exceptions.Timeout:
        return {"ok": False, "error": "Open-Meteo timed out", "days": [], "total_forecast": 0,
                "source": "Open-Meteo", "needs_manual": False}
    except Exception as e:
        return {"ok": False, "error": f"Open-Meteo fetch failed: {str(e)}", "days": [], "total_forecast": 0,
                "source": "Open-Meteo", "needs_manual": False}


def fetch_nws_mtd(city_cfg=None):
    """Scrape NWS Daily Climate Report for MTD precipitation.
    Only accepts overnight reports (issued 0-4 AM local).
    If the latest version is an afternoon report, tries earlier versions.
    """
    cfg = city_cfg or CITY_CFG
    base_url = (f"https://forecast.weather.gov/product.php"
                f"?site={cfg['nws_site']}"
                f"&issuedby={cfg['nws_issuedby']}"
                f"&product=CLI&format=txt")
    issuedby = cfg.get("nws_issuedby", "SEA")
    label    = cfg.get("label", "Seattle, WA")
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        last_err = "No versions tried"
        for version in range(1, 6):
            try:
                url = base_url if version == 1 else f"{base_url}&version={version}"
                r = requests.get(url, headers=headers, timeout=6)
                r.raise_for_status()
                text = r.text

                soup = BeautifulSoup(text, "html.parser")
                pre  = soup.find("pre")
                raw  = pre.get_text() if pre else text

                mtd_match     = re.search(r"MONTH TO DATE\s+([\d\.T]+)", raw, re.IGNORECASE)
                today_match   = re.search(r"(?:YESTERDAY|TODAY)\s+([\d\.T]+)\s", raw, re.IGNORECASE)
                date_match    = re.search(r"CLIMATE SUMMARY FOR\s+([\w\s]+\d{4})", raw, re.IGNORECASE)
                issued_match  = re.search(r"(\d{3,4}\s+(?:AM|PM)\s+\w+\s+\w+\s+\w+\s+\d+\s+\d{4})", raw, re.IGNORECASE)
                valid_match   = re.search(r"VALID\s+\w+\s+AS\s+OF\s+([\d:]+\s*(?:AM|PM)?\s*\w+\s*TIME)", raw, re.IGNORECASE)

                issued_time = issued_match.group(1).strip() if issued_match else None
                valid_time  = valid_match.group(1).strip() if valid_match else None
                mtd   = float(mtd_match.group(1))   if mtd_match   and mtd_match.group(1)   != "T" else 0.0
                today = float(today_match.group(1)) if today_match and today_match.group(1) != "T" else 0.0
                date  = date_match.group(1).strip() if date_match else "Unknown"

                is_finalized = False
                issued_hour  = None
                if issued_time:
                    h_match = re.search(r"(\d{3,4})\s+(AM|PM)", issued_time, re.IGNORECASE)
                    if h_match:
                        raw_h = int(h_match.group(1))
                        ampm  = h_match.group(2).upper()
                        hour  = (raw_h // 100) % 12 + (12 if ampm == "PM" else 0)
                        if ampm == "AM" and raw_h // 100 == 12:
                            hour = 0
                        issued_hour  = hour
                        is_finalized = (0 <= hour <= 4)

                        # Skip afternoon/evening reports — unreliable today field
                        if hour >= 6:
                            last_err = f"v{version} is afternoon ({issued_time}), trying older"
                            continue

                return {
                    "ok":           True,
                    "mtd":          mtd,
                    "today":        today,
                    "date":         date,
                    "issued":       issued_time,
                    "issued_hour":  issued_hour,
                    "is_finalized": is_finalized,
                    "mtd_type":     "finalized" if is_finalized else "preliminary",
                    "valid_as_of":  valid_time,
                    "source":       f"NWS CLI {issuedby} ({label})",
                    "version_used": version,
                }
            except Exception as ve:
                last_err = str(ve)
                continue

        return {"ok": False, "error": f"All CLI versions unusable: {last_err}",
                "mtd": 0.0, "today": 0.0, "date": "", "source": "NWS CLI"}

    except Exception as e:
        return {"ok": False, "error": str(e), "mtd": 0.0, "today": 0.0, "date": "", "source": "NWS CLI"}


# ── CLM ACTUALS CACHE ─────────────────────────────────────────────────────────
import time as _time
_CLM_CACHE = {}
_CLM_CACHE_TS = {}
_CLM_CACHE_TTL = 6 * 3600

# ── SIGMA CACHE (populated from /calibrate/all) ───────────────────────────────
_SIGMA_CACHE = {}   # city_key -> base_sigma (single float, full-month RMSE)

# Horizon scaling: multiply base sigma by these factors as days_remaining increases.
# Shared across all cities — D-1 is tightest, D-10 is loosest.
# Based on typical NWP skill decay: ~40% more uncertainty per doubling of lead time.
HORIZON_SCALE = {0:0.05, 1:0.55, 2:0.65, 3:0.75, 4:0.85, 5:0.95,
                 6:1.05, 7:1.15, 8:1.25, 9:1.35, 10:1.4}

# Fallback base sigma per city if not yet calibrated (inches, full-month RMSE)
_SIGMA_BASE_FALLBACK = 1.2   # conservative default until calibrated

# ── BIAS CACHE (populated from /calibrate/all) ────────────────────────────────
_BIAS_CACHE = {}


def update_sigma_from_backtest(city_key, summary):
    """Store single base sigma from monthly RMSE. Horizon scaling applied in get_sigma()."""
    if not summary:
        return
    # New simple format: summary["monthly"]["rmse"]
    if "monthly" in summary:
        _SIGMA_CACHE[city_key] = summary["monthly"]["rmse"]
        return
    # Legacy per-horizon format fallback: use d3 as representative base
    for key in ["d3", "d5", "d7", "d1"]:
        if key in summary and summary[key].get("rmse"):
            _SIGMA_CACHE[city_key] = summary[key]["rmse"]
            return


def get_sigma(city_key, days_remaining):
    """Return (sigma, bias) for city at given days_remaining horizon."""
    base  = _SIGMA_CACHE.get(city_key, _SIGMA_BASE_FALLBACK)
    scale = HORIZON_SCALE.get(min(max(days_remaining, 0), 10), 1.0)
    sigma = round(base * scale, 3)
    # Bias: use monthly bias (same for all horizons)
    bias  = _BIAS_CACHE.get(f"{city_key}-monthly",
            _BIAS_CACHE.get(f"{city_key}-d3", 0.0))
    return sigma, bias


def fetch_nws_clm_actuals(city_key):
    """Fetch historical monthly precipitation actuals from NWS CLM product."""
    cfg = CITIES.get(city_key)
    if not cfg:
        return {}
    now = _time.time()
    if city_key in _CLM_CACHE and (now - _CLM_CACHE_TS.get(city_key, 0)) < _CLM_CACHE_TTL:
        return _CLM_CACHE[city_key]

    actuals = {}
    headers = {"User-Agent": "Mozilla/5.0"}
    MONTH_MAP = {"JANUARY":1,"FEBRUARY":2,"MARCH":3,"APRIL":4,"MAY":5,"JUNE":6,
                 "JULY":7,"AUGUST":8,"SEPTEMBER":9,"OCTOBER":10,"NOVEMBER":11,"DECEMBER":12}

    # CLM uses site=NWS (unlike CLI which uses site=<office>)
    clm_url = (f"https://forecast.weather.gov/product.php"
               f"?site=NWS"
               f"&issuedby={cfg['nws_issuedby']}"
               f"&product=CLM&format=txt")

    for version in range(1, 16):
        try:
            url = clm_url + f"&version={version}"
            r = requests.get(url, headers=headers, timeout=8)
            if not r.ok:
                break
            soup = BeautifulSoup(r.text, "html.parser")
            pre  = soup.find("pre")
            raw  = pre.get_text() if pre else r.text

            month_match = re.search(r"FOR THE MONTH OF\s+([A-Z]+)\s+(\d{4})", raw, re.IGNORECASE)
            if not month_match:
                month_match = re.search(
                    r"CLIMATE SUMMARY[^.]*?\s+(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|"
                    r"JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+(\d{4})",
                    raw, re.IGNORECASE)
            if not month_match:
                continue

            month_name = month_match.group(1).upper()
            year       = int(month_match.group(2))
            month_num  = MONTH_MAP.get(month_name)
            if not month_num:
                continue
            month_key = f"{year}-{month_num:02d}"

            precip_section = re.search(
                r"PRECIPITATION \(INCHES\)(.*?)(?:SNOWFALL|DEGREE DAYS|WIND|$)",
                raw, re.IGNORECASE | re.DOTALL)
            section_text = precip_section.group(1) if precip_section else raw

            total_match = re.search(
                r"^[ \t]*TOTALS?\s+([\d\.]+|T)\b",
                section_text, re.IGNORECASE | re.MULTILINE)
            if not total_match:
                continue

            val_str = total_match.group(1).strip()
            actuals[month_key] = 0.0 if val_str == "T" else float(val_str)
        except Exception:
            continue

    HARDCODED = {
        "seattle": {
            "2024-10": 2.15, "2024-11": 4.86, "2024-12": 5.50,
            "2025-01": 1.92, "2025-11": 5.71, "2025-12": 7.37,
            "2026-01": 3.19, "2026-02": 2.92,
        }
    }
    merged = {**HARDCODED.get(city_key, {}), **actuals}
    _CLM_CACHE[city_key]    = merged
    _CLM_CACHE_TS[city_key] = now
    return merged


def fetch_wu_hourly(city_cfg=None):
    """
    Fetch hourly QPF from Open-Meteo for today (current hour through midnight).
    Replaces Weather Underground hourly — same purpose, stable free API.
    Returns same structure as old WU hourly function.
    """
    cfg = city_cfg or CITY_CFG
    try:
        import pytz
        tz_name   = cfg.get("tz", "America/Los_Angeles")
        local_tz  = pytz.timezone(tz_name)
        now_local = datetime.utcnow().replace(
            tzinfo=pytz.utc).astimezone(local_tz)
        today_str = now_local.strftime("%Y-%m-%d")
        now_hour  = now_local.hour

        params = {
            "latitude":      cfg["lat"],
            "longitude":     cfg["lon"],
            "hourly":        "precipitation",
            "timezone":      cfg["tz"],
            "start_date":    today_str,
            "end_date":      today_str,
            "models":        "best_match",
        }
        r = requests.get(OM_HOURLY_URL, params=params, timeout=6)
        r.raise_for_status()
        data = r.json()

        if "error" in data:
            return {"ok": False, "error": data["error"], "hours": [], "today_remaining": 0}

        times  = data.get("hourly", {}).get("time", [])
        precip = data.get("hourly", {}).get("precipitation", [])

        today_hours = []
        for i, t in enumerate(times):
            if not t:
                continue
            hour_num = int(t[11:13])
            # IEM covers midnight → last completed observation (~1 hour lag)
            # OM covers current hour → midnight — no overlap risk due to IEM lag
            # Flow: NWS overnight MTD (through midnight) → IEM (midnight→now) → OM hourly (now→midnight)
            if hour_num >= now_hour:
                qpf_mm = float(precip[i] or 0) if i < len(precip) else 0.0
                qpf_in = round(qpf_mm / 25.4, 3)
                today_hours.append({
                    "time": t[11:16],
                    "hour": hour_num,
                    "qpf":  qpf_in,
                })

        today_remaining = round(sum(h["qpf"] for h in today_hours), 2)

        return {
            "ok":              True,
            "hours":           today_hours,
            "today_remaining": today_remaining,
            "source":          "Open-Meteo hourly",
        }

    except Exception as e:
        return {"ok": False, "error": str(e), "hours": [], "today_remaining": 0}


def fetch_iem_gap(nws_issued_str, city_cfg=None):
    """
    Fetch IEM ASOS precipitation from the NWS CLI issued time to now.

    The NWS CLI is issued shortly after the ASOS DSM closes (midnight LST).
    During DST this is around 1:00-1:30 AM local wall-clock time.
    We anchor the IEM gap at the actual NWS issued time rather than a
    hardcoded hour, so we never double-count rain already in the NWS MTD.
    """
    cfg = city_cfg or CITY_CFG
    iem_station = cfg["icao_code"][1:]
    try:
        import pytz
        from datetime import datetime as dt_cls, timedelta

        tz_name   = cfg.get("tz", "America/Los_Angeles")
        local_tz  = pytz.timezone(tz_name)
        now_utc   = dt_cls.utcnow().replace(tzinfo=pytz.utc)
        now_local = now_utc.astimezone(local_tz)
        is_dst    = bool(now_local.dst())

        # Determine gap start from actual NWS issued time when available.
        # NWS CLI issued_str format: "126 AM PDT TUE MAR 24 2026" or similar.
        # Fall back to DST-aware default (1 AM DST / midnight ST) if not parseable.
        gap_start_hour = 1 if is_dst else 0
        gap_start_minute = 0

        if nws_issued_str:
            try:
                # Parse "126 AM PDT TUE MAR 24 2026" → hour=1, minute=26
                parts = nws_issued_str.strip().split()
                time_str = parts[0]  # e.g. "126" or "100" or "53"
                ampm     = parts[1].upper() if len(parts) > 1 else "AM"
                if len(time_str) <= 2:
                    h, m = int(time_str), 0
                else:
                    h, m = int(time_str[:-2]), int(time_str[-2:])
                if ampm == "PM" and h != 12:
                    h += 12
                elif ampm == "AM" and h == 12:
                    h = 0
                # Only use if it looks like an overnight/early-morning issue time
                if 0 <= h <= 5:
                    gap_start_hour   = h
                    gap_start_minute = m
            except Exception:
                pass  # fall back to default

        gap_start_str = f"{gap_start_hour:02d}:{gap_start_minute:02d}"
        today_str     = now_local.strftime("%Y-%m-%d")

        # Sanity: if current time is before gap start, return zero
        now_minutes = now_local.hour * 60 + now_local.minute
        gap_minutes = gap_start_hour * 60 + gap_start_minute
        if now_minutes < gap_minutes:
            return {
                "ok": True, "gap_total": 0.0, "readings": [],
                "gap_start": gap_start_str,
                "gap_end": now_local.strftime("%I:%M %p"),
                "source": "IEM ASOS (pre-gap-start)"
            }

        url = (
            f"https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
            f"?station={iem_station}"
            f"&data=p01i"
            f"&year1={now_local.year}&month1={now_local.month:02d}&day1={now_local.day:02d}"
            f"&hour1={gap_start_hour}&min1={gap_start_minute}"
            f"&year2={now_local.year}&month2={now_local.month:02d}&day2={now_local.day:02d}"
            f"&hour2={now_local.hour}&min2={now_local.minute}"
            f"&tz={tz_name}"
            f"&format=comma&latlon=no&direct=no&report_type=1"
        )

        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=6)
        r.raise_for_status()

        lines = r.text.strip().split("\n")

        # Skip comment/header lines starting with #
        data_lines = [l for l in lines if l and not l.startswith("#") and l != "station,valid,p01i"]

        # p01i is the accumulated precipitation since the last hourly reset.
        # Within each hour it's cumulative (0.01, 0.02, 0.05... = still only 0.05" that hour).
        # Correct approach: group by hour, take the MAX value per hour (= last before reset),
        # then sum those hourly maxes for the true total.
        hourly = {}  # hour_key -> max p01i seen in that hour
        readings = []

        for line in data_lines:
            parts = line.strip().split(",")
            if len(parts) < 3:
                continue
            station, valid_time, precip = parts[0], parts[1], parts[2]
            if precip in ("M", "T", ""):
                continue
            try:
                p = float(precip)
                if p <= 0:
                    continue
                # hour_key = "YYYY-MM-DD HH" — group all readings in same hour
                hour_key = valid_time[:13] if len(valid_time) >= 13 else valid_time[:10]
                hourly[hour_key] = max(hourly.get(hour_key, 0.0), p)
                readings.append({"time": valid_time, "precip": round(p, 2)})
            except ValueError:
                continue

        # Sum the per-hour maxes — this is the true accumulated precipitation
        gap_total = round(sum(hourly.values()), 2)

        # Also expose hourly breakdown for display
        hourly_summary = [{"hour": k, "precip": round(v, 2)} for k, v in sorted(hourly.items())]

        # Sanity check — corrected total should rarely exceed 3" in a partial day
        # (Seattle record full-day is 5.02"; partial day gap should be well under that)
        if gap_total > 4.0:
            return {
                "ok": False,
                "error": f"IEM gap total {gap_total}\" exceeds sanity threshold after hourly correction — check data",
                "gap_total": 0.0,
                "readings": [],
                "hourly_summary": hourly_summary,
                "gap_start": gap_start_str,
                "gap_end": now_local.strftime("%I:%M %p")
            }

        return {
            "ok":             True,
            "gap_total":      gap_total,
            "readings":       readings,       # raw 5-min observations for display
            "hourly_summary": hourly_summary, # max per hour — what was actually summed
            "reading_count":  len(readings),
            "hour_count":     len(hourly),
            "gap_start":      gap_start_str,
            "gap_end":        now_local.strftime("%I:%M %p"),
            "source":         f"IEM {iem_station} ASOS"
        }

    except Exception as e:
        return {
            "ok":        False,
            "error":     str(e),
            "gap_total": 0.0,
            "readings":  [],
            "gap_start": None,
            "gap_end":   None
        }


def fetch_kalshi_markets(city_cfg=None):
    """Fetch open rain markets from Kalshi API for given city."""
    cfg = city_cfg or CITY_CFG
    try:
        if not KALSHI_KEY_ID:
            return {"ok": False, "error": "Kalshi key not configured", "markets": []}

        path = "/trade-api/v2/markets"
        url = f"{KALSHI_BASE}/markets"
        params = {"series_ticker": cfg["kalshi_series"], "status": "open", "limit": 100}
        headers = kalshi_auth_headers("GET", path)
        r = requests.get(url, params=params, headers=headers, timeout=6)
        r.raise_for_status()
        data = r.json()

        markets = []
        for m in data.get("markets", []):
            subtitle = m.get("subtitle", "")
            ticker   = m.get("ticker", "")

            # Try multiple fields to get the inch threshold
            # 1. cap_strike or floor_strike (numeric, in cents or inches)
            # 2. functional_strike string e.g. "> 4.00"
            # 3. subtitle text fallback
            # 4. ticker fallback e.g. KXRAINSEAM-26MAR-B4
            inches = None

            # Try floor_strike / cap_strike (Kalshi stores in dollars = inches here)
            for field in ["floor_strike", "cap_strike"]:
                val = m.get(field)
                if val is not None:
                    try:
                        inches = float(val)
                        break
                    except Exception:
                        pass

            # Try functional_strike string e.g. "> 4.00" or "Above 4"
            if inches is None:
                fs = m.get("functional_strike", "") or ""
                fs_match = re.search(r"([\d\.]+)", str(fs))
                if fs_match:
                    inches = float(fs_match.group(1))

            # Try subtitle
            if inches is None and subtitle:
                sub_match = re.search(r"([\d\.]+)", subtitle)
                if sub_match:
                    inches = float(sub_match.group(1))

            # Try ticker e.g. KXRAINSEAM-26MAR-B4 -> 4
            if inches is None and ticker:
                tick_match = re.search(r"-B([\d\.]+)$", ticker)
                if tick_match:
                    inches = float(tick_match.group(1))

            # Build a clean label
            if inches is not None:
                strike_type = m.get("strike_type", "greater")
                op = "Above" if strike_type in ("greater", "greater_or_equal") else "Below"
                label = f"{op} {inches:.0f} inches"
            else:
                label = subtitle or ticker

            yes_ask   = float(m.get("yes_ask_dollars", 0) or 0)
            yes_bid   = float(m.get("yes_bid_dollars", 0) or 0)
            no_ask    = float(m.get("no_ask_dollars", 0) or 0)
            no_bid    = float(m.get("no_bid_dollars", 0) or 0)
            spread    = max(0.0, round(yes_ask - yes_bid, 4))

            # Size at top of book
            yes_ask_sz = float(m.get("yes_ask_size_fp", 0) or 0)
            yes_bid_sz = float(m.get("yes_bid_size_fp", 0) or 0)
            open_int   = float(m.get("open_interest_fp", 0) or 0)
            volume     = float(m.get("volume_fp", 0) or 0)
            volume_24h = float(m.get("volume_24h_fp", 0) or 0)

            markets.append({
                "ticker":       ticker,
                "title":        m.get("title", ""),
                "subtitle":     subtitle,
                "label":        label,
                "inches":       inches,
                "strike_type":  m.get("strike_type", "greater"),
                "yes_ask":      yes_ask,
                "no_ask":       no_ask,
                "yes_bid":      yes_bid,
                "no_bid":       no_bid,
                "spread":       spread,
                "yes_ask_size": yes_ask_sz,
                "yes_bid_size": yes_bid_sz,
                "open_interest": open_int,
                "volume":       volume,
                "volume_24h":   volume_24h,
                "last_price":   float(m.get("last_price_dollars", 0) or 0),
                "close_time":   m.get("close_time", ""),
            })

        # Sort by inch threshold
        markets.sort(key=lambda x: x["inches"] if x["inches"] else 0)
        return {"ok": True, "markets": markets}

    except Exception as e:
        return {"ok": False, "error": str(e), "markets": []}


def confidence_weight(days_remaining):
    """
    Forecast confidence increases as month-end approaches.
    At 10 days out: 0.0 (low confidence, WU is noisy)
    At  7 days out: 0.3 (starting to get useful)
    At  3 days out: 0.7 (high confidence)
    At  1 day  out: 0.9 (very high confidence)
    At  0 days out: 1.0 (settled)

    Based on empirical WU accuracy — inside 7 days forecast error
    drops significantly. This curve is tunable as we collect data.
    """
    if days_remaining <= 0:
        return 1.0
    if days_remaining >= 10:
        return 0.0
    # Nonlinear curve — steeper confidence gain inside 7 days
    if days_remaining <= 7:
        return round(1.0 - (days_remaining / 8) ** 0.7, 3)
    else:
        # 8-10 days: very low, linear falloff
        return round(0.05 * (10 - days_remaining) / 2, 3)


def liquidity_score(market):
    """
    Score 0-100 combining five execution factors.
    Gates the raw gap — a great gap in an illiquid market is not actionable.

    Factor 1 — Spread tightness     (25 pts) tight = market maker present
    Factor 2 — Exit depth YES bid   (25 pts) can you get out before settlement?
    Factor 3 — Open interest        (20 pts) total engagement = price reliability
    Factor 4 — Volume 24h           (15 pts) recent activity = live market
    Factor 5 — Book symmetry        (15 pts) mirror book = market maker signal
    """
    spread     = market.get("spread", 0.10)
    yes_bid_sz = market.get("yes_bid_size", 0)
    open_int   = market.get("open_interest", 0)
    vol_24h    = market.get("volume_24h", 0)
    yes_bid    = market.get("yes_bid", 0)
    no_ask     = market.get("no_ask", 0)

    spread_c = round(spread * 100)
    f1 = 25 if spread_c <= 0 else 22 if spread_c == 1 else 18 if spread_c == 2 else 12 if spread_c == 3 else 6 if spread_c == 4 else 0
    f2 = 25 if yes_bid_sz >= 2000 else 20 if yes_bid_sz >= 500 else 14 if yes_bid_sz >= 100 else 6 if yes_bid_sz >= 20 else 0
    f3 = 20 if open_int >= 5000 else 14 if open_int >= 1000 else 8 if open_int >= 200 else 3 if open_int > 0 else 0
    f4 = 15 if vol_24h >= 1000 else 10 if vol_24h >= 200 else 5 if vol_24h >= 50 else 0
    pair_sum = round((yes_bid + no_ask) * 100)
    f5 = 15 if 95 <= pair_sum <= 105 else 10 if 90 <= pair_sum <= 110 else 5 if 80 <= pair_sum <= 120 else 0

    score = f1 + f2 + f3 + f4 + f5
    yes_ask = market.get("yes_ask", 0.85)

    return {
        "score":         score,
        "grade":         "A" if score >= 75 else "B" if score >= 55 else "C" if score >= 35 else "D",
        "spread_pts":    f1,
        "exit_pts":      f2,
        "oi_pts":        f3,
        "vol_pts":       f4,
        "symmetry_pts":  f5,
        "spread_c":      spread_c,
        "exit_depth":    round(yes_bid_sz),
        "open_interest": round(open_int),
        "vol_24h":       round(vol_24h),
        "market_maker":  f5 >= 10,
        "max_deploy":    round(min(open_int * 0.10 * yes_ask, 2000)),
    }


def analyze_value(markets, projected_total, days_remaining=10, true_mtd=None,
                  nws_is_finalized=True, city_key="seattle", month_num=None):
    """
    Three-mode probability + liquidity scoring.
    Sigma comes from backtest RMSE cache (falls back to hardcoded table).
    Bias correction applied when calibration data is available.
    """
    import datetime as _dt
    from math import erf, sqrt
    def normcdf(x):
        return 0.5 * (1 + erf(x / sqrt(2)))

    if month_num is None:
        month_num = _dt.date.today().month
    sigma, bias = get_sigma(city_key, days_remaining)
    adjusted_proj = projected_total - bias
    conf  = confidence_weight(days_remaining)
    analyzed = []

    for m in markets:
        inches = m.get("inches")
        if inches is None:
            m["edge"] = "unknown"; m["edge_detail"] = ""
            m["weighted_edge"] = 0; m["confidence"] = conf
            analyzed.append(m); continue

        cushion = round((true_mtd or 0) - inches, 2) if true_mtd is not None else None
        yes_ask = m.get("yes_ask", 0)

        # Skip markets already fully priced — no edge to capture
        if yes_ask >= 0.99:
            m["edge"] = "HOLD"; pass  # keep existing mode if set; m["model_prob"] = 0.99
            m["cushion"] = cushion; m["gap_c"] = 0; m["net_gap_c"] = 0
            m["decision"] = "SKIP — fully priced"; m["actionable"] = False
            m["confidence"] = conf; m["weighted_edge"] = 0
            m["liquidity"] = liquidity_score(m); m["sigma"] = sigma
            m["margin"] = round(projected_total - inches, 2)
            analyzed.append(m); continue

        # Three-mode probability
        # Pipeline already corrects for preliminary NWS: base = MTD - today + IEM gap fill.
        # Result is equally reliable regardless of report timing, so threshold is always 0.10".
        if days_remaining >= 15:
            settled_min = 0.02
        elif days_remaining >= 8:
            settled_min = 0.05
        elif days_remaining >= 3:
            settled_min = 0.10
        else:
            settled_min = 0.15

        if cushion is not None and cushion >= settled_min:
            mode = "settled"
            model_prob = 0.99
        elif cushion is not None and cushion >= -0.20:
            mode = "near-certain"
            model_prob = round(min(0.97, 0.90 + cushion / 0.5 * 0.07), 3)
        else:
            mode = "probabilistic"
            if sigma > 0:
                model_prob = round(normcdf((adjusted_proj - inches) / sigma), 3)
            else:
                model_prob = 1.0 if projected_total >= inches else 0.0

        gap        = round(model_prob - yes_ask, 3)       # probability gap
        gap_c      = round(gap * 100)                      # in cents

        # Legacy weighted_edge for backward compatibility
        margin        = round(projected_total - inches, 2)
        weighted_edge = round(margin * conf, 3)

        if gap_c >= 12:
            edge = "STRONG_YES"
        elif gap_c >= 6:
            edge = "LEAN_YES"
        elif gap_c <= -12:
            edge = "STRONG_NO"
        elif gap_c <= -6:
            edge = "LEAN_NO"
        else:
            edge = "HOLD"

        # Liquidity scoring
        liq        = liquidity_score(m)
        friction_c = liq["spread_c"]
        net_gap_c  = max(0, gap_c - friction_c)
        actionable = net_gap_c >= 5 and liq["grade"] in ("A", "B", "C")

        # Simple decision: BUY / HOLD / SKIP
        if actionable and edge in ("STRONG_YES", "LEAN_YES"):
            decision = "BUY YES"
        elif actionable and edge in ("STRONG_NO", "LEAN_NO"):
            decision = "BUY NO"
        elif liq["grade"] == "D":
            decision = "SKIP — illiquid"
        else:
            decision = "HOLD"

        no_ask       = m.get("no_ask", 0)
        no_gap       = round((1 - model_prob) - no_ask, 3)
        no_gap_c     = round(no_gap * 100)
        no_net_gap_c = max(0, no_gap_c - friction_c)

        m["edge"]          = edge
        m["mode"]          = mode
        m["model_prob"]    = model_prob
        m["cushion"]       = cushion
        m["gap_c"]         = gap_c
        m["no_gap_c"]      = no_gap_c
        m["no_net_gap_c"]  = no_net_gap_c
        m["margin"]        = margin
        m["confidence"]    = conf
        m["weighted_edge"] = weighted_edge
        m["sigma"]         = round(sigma, 3)
        m["bias"]          = round(bias, 3)
        m["adj_proj"]      = round(adjusted_proj, 2)
        m["liquidity"]     = liq
        m["net_gap_c"]     = net_gap_c
        m["actionable"]    = actionable
        m["decision"]      = decision
        m["edge_detail"]   = (
            f"{mode} · model {int(model_prob*100)}% YES / {int((1-model_prob)*100)}% NO"
            f" vs {int(yes_ask*100)}¢ YES / {int(no_ask*100)}¢ NO"
            f" · sigma={sigma:.2f} bias={bias:+.2f} · {net_gap_c}¢ net · liq {liq['grade']}"
        )
        analyzed.append(m)

    return analyzed


# ── POSTGRES — snapshot storage ───────────────────────────────────────────────

def get_db():
    """Return a psycopg2 connection or None if unavailable."""
    if not PSYCOPG2_AVAILABLE or not DATABASE_URL:
        return None
    try:
        return psycopg2.connect(DATABASE_URL, sslmode="require", connect_timeout=5)
    except Exception as e:
        print(f"  ⚠️  DB connect failed: {e}")
        return None


def ensure_tables():
    """
    Create tables on first run. Safe to call on every startup.
    Two tables:
      forecast_snapshots — one row per (month, days_remaining), written once/day
      month_settlements  — filled manually/via endpoint when NWS settles a month
    """
    conn = get_db()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS forecast_snapshots (
                    id               SERIAL PRIMARY KEY,
                    city             TEXT NOT NULL DEFAULT 'seattle',
                    month            TEXT NOT NULL,
                    snapshot_date    DATE NOT NULL,
                    days_remaining   INTEGER NOT NULL,
                    true_mtd         NUMERIC(6,2),
                    wu_remaining     NUMERIC(6,2),
                    projected_eom    NUMERIC(6,2),
                    sigma_estimate   NUMERIC(5,3),
                    confidence       NUMERIC(5,3),
                    wu_days_used     INTEGER,
                    created_at       TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (city, month, days_remaining)
                );
            """)
            # Intraday snapshots — written every /data call, captures model vs market
            # This powers the projection vs market chart
            cur.execute("""
                CREATE TABLE IF NOT EXISTS intraday_snapshots (
                    id               SERIAL PRIMARY KEY,
                    city             TEXT NOT NULL DEFAULT 'seattle',
                    month            TEXT NOT NULL,
                    snapshot_ts      TIMESTAMPTZ DEFAULT NOW(),
                    days_remaining   INTEGER NOT NULL,
                    true_mtd         NUMERIC(6,2),
                    projected_eom    NUMERIC(6,2),
                    sigma_estimate   NUMERIC(5,3),
                    model_prob_5     NUMERIC(5,3),
                    model_prob_6     NUMERIC(5,3),
                    model_prob_7     NUMERIC(5,3),
                    kalshi_yes_5     NUMERIC(5,3),
                    kalshi_yes_6     NUMERIC(5,3),
                    kalshi_yes_7     NUMERIC(5,3),
                    gap_5            NUMERIC(5,3),
                    gap_6            NUMERIC(5,3),
                    gap_7            NUMERIC(5,3)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS month_settlements (
                    id              SERIAL PRIMARY KEY,
                    month           TEXT NOT NULL UNIQUE,   -- e.g. '2026-03'
                    settled_total   NUMERIC(6,2) NOT NULL,
                    settled_date    DATE,
                    notes           TEXT,
                    created_at      TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            # Temperature market snapshots — one row per bracket per scan
            cur.execute("""
                CREATE TABLE IF NOT EXISTS temp_snapshots (
                    id              SERIAL PRIMARY KEY,
                    city            TEXT NOT NULL,
                    nws_station     TEXT NOT NULL,
                    target_date     DATE NOT NULL,
                    horizon         TEXT NOT NULL DEFAULT 'd1',
                    scan_ts         TIMESTAMPTZ DEFAULT NOW(),
                    market_type     TEXT NOT NULL,  -- 'high' or 'low'
                    ticker          TEXT NOT NULL,
                    bracket_label   TEXT,
                    lo_temp         NUMERIC(5,1),
                    hi_temp         NUMERIC(5,1),
                    gfs_forecast    NUMERIC(5,1),
                    ecmwf_forecast  NUMERIC(5,1),
                    best_forecast   NUMERIC(5,1),
                    sigma           NUMERIC(5,2),
                    spread_models   NUMERIC(5,2),
                    model_prob      NUMERIC(5,3),
                    yes_ask         NUMERIC(5,3),
                    gap_c           INTEGER,
                    net_gap_c       INTEGER,
                    edge_ratio      NUMERIC(6,3),
                    kelly_frac      NUMERIC(5,3),
                    grade           TEXT,
                    liq_grade       TEXT,
                    open_interest   INTEGER,
                    volume_24h      INTEGER,
                    settled_temp    NUMERIC(5,1),
                    settled_correct BOOLEAN,
                    hours_to_cutoff NUMERIC(5,1)  -- hours until 6 AM local at time of scan
                );
            """)
            # Migrate existing temp_snapshots if column missing
            cur.execute("""
                ALTER TABLE temp_snapshots
                    ADD COLUMN IF NOT EXISTS hours_to_cutoff NUMERIC(5,1);
            """)
            # Multi-model forecast accuracy table.
            # One row per city per date. Stores all model forecasts + actual.
            # Used for per-city per-model RMSE analysis across spread buckets.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS model_forecasts (
                    id              SERIAL PRIMARY KEY,
                    city            TEXT NOT NULL,
                    nws_station     TEXT NOT NULL,
                    target_date     DATE NOT NULL,
                    actual_high     NUMERIC(5,1),    -- NWS CLI actual
                    gfs_high        NUMERIC(5,1),    -- GFS seamless
                    ecmwf_high      NUMERIC(5,1),    -- ECMWF IFS
                    nbm_high        NUMERIC(5,1),    -- NOAA National Blend of Models
                    graphcast_high  NUMERIC(5,1),    -- Google GraphCast
                    gem_high        NUMERIC(5,1),    -- Canadian GEM
                    icon_high       NUMERIC(5,1),    -- German ICON
                    spread_gfs_ecmwf NUMERIC(5,2),  -- abs(gfs - ecmwf)
                    fetched_at      TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (city, target_date)
                );
            """)
            # Add new model columns if upgrading from older schema
            for col in ['nbm_high','graphcast_high','gem_high','icon_high','spread_gfs_ecmwf']:
                cur.execute(f"""
                    ALTER TABLE model_forecasts
                        ADD COLUMN IF NOT EXISTS {col} NUMERIC(5,1);
                """)
            # View: per-model RMSE by city and spread bucket
            cur.execute("""
                CREATE OR REPLACE VIEW model_accuracy AS
                SELECT
                    city,
                    CASE
                        WHEN spread_gfs_ecmwf <= 1.0 THEN '0-1°F'
                        WHEN spread_gfs_ecmwf <= 2.0 THEN '1-2°F'
                        WHEN spread_gfs_ecmwf <= 3.0 THEN '2-3°F'
                        WHEN spread_gfs_ecmwf <= 4.0 THEN '3-4°F'
                        WHEN spread_gfs_ecmwf <= 5.0 THEN '4-5°F'
                        ELSE '5°F+'
                    END AS spread_bucket,
                    COUNT(*) AS n,
                    ROUND(SQRT(AVG(POWER(gfs_high - actual_high, 2))), 2)       AS gfs_rmse,
                    ROUND(SQRT(AVG(POWER(ecmwf_high - actual_high, 2))), 2)     AS ecmwf_rmse,
                    ROUND(SQRT(AVG(POWER(nbm_high - actual_high, 2))), 2)       AS nbm_rmse,
                    ROUND(SQRT(AVG(POWER(graphcast_high - actual_high, 2))), 2) AS graphcast_rmse,
                    ROUND(SQRT(AVG(POWER(gem_high - actual_high, 2))), 2)       AS gem_rmse,
                    ROUND(SQRT(AVG(POWER(icon_high - actual_high, 2))), 2)      AS icon_rmse,
                    ROUND(AVG(gfs_high - actual_high), 2)                       AS gfs_bias,
                    ROUND(AVG(ecmwf_high - actual_high), 2)                     AS ecmwf_bias,
                    ROUND(AVG(nbm_high - actual_high), 2)                       AS nbm_bias,
                    ROUND(AVG(graphcast_high - actual_high), 2)                 AS graphcast_bias,
                    ROUND(AVG(gem_high - actual_high), 2)                       AS gem_bias,
                    ROUND(AVG(icon_high - actual_high), 2)                      AS icon_bias
                FROM model_forecasts
                WHERE actual_high IS NOT NULL
                  AND spread_gfs_ecmwf IS NOT NULL
                GROUP BY city, spread_bucket
                ORDER BY city, spread_bucket;
            """)
            # View: model accuracy by city, horizon, grade
            cur.execute("""
                CREATE OR REPLACE VIEW temp_backtest AS
                SELECT
                    city,
                    horizon,
                    grade,
                    COUNT(*)                                            AS n,
                    ROUND(AVG(CASE WHEN settled_correct THEN 1.0 ELSE 0.0 END), 3) AS win_rate,
                    ROUND(AVG(model_prob::numeric), 3)                    AS avg_model_prob,
                    ROUND(AVG(yes_ask::numeric), 3)                       AS avg_market_price,
                    ROUND(AVG(gap_c), 1)                                AS avg_gap_c,
                    ROUND(AVG(net_gap_c), 1)                            AS avg_net_gap_c,
                    ROUND(AVG(edge_ratio::numeric), 3)                    AS avg_edge_ratio,
                    ROUND(STDDEV(CASE WHEN settled_correct THEN 1.0 ELSE 0.0 END), 3) AS win_rate_stddev
                FROM temp_snapshots
                WHERE settled_correct IS NOT NULL
                GROUP BY city, horizon, grade
                ORDER BY city, horizon, grade;
            """)
            # View joining snapshots with settlements for calibration analysis
            cur.execute("""
                CREATE OR REPLACE VIEW forecast_accuracy AS
                SELECT
                    s.month,
                    s.days_remaining,
                    s.snapshot_date,
                    s.projected_eom,
                    s.confidence,
                    s.wu_days_used,
                    m.settled_total,
                    ROUND(s.projected_eom - m.settled_total, 2) AS wu_error,
                    ROUND(ABS(s.projected_eom - m.settled_total), 2) AS abs_error
                FROM forecast_snapshots s
                LEFT JOIN month_settlements m USING (month)
                ORDER BY s.month, s.days_remaining DESC;
            """)
            # Auto-trader config — one row per setting key
            cur.execute("""
                CREATE TABLE IF NOT EXISTS auto_trader_config (
                    key        TEXT PRIMARY KEY,
                    value      TEXT NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            # Auto-trader execution log — indexed for fast 3-day queries
            cur.execute("""
                CREATE TABLE IF NOT EXISTS auto_trader_log (
                    id         BIGSERIAL PRIMARY KEY,
                    ts         TIMESTAMPTZ DEFAULT NOW(),
                    level      TEXT NOT NULL,
                    msg        TEXT NOT NULL,
                    ticker     TEXT,
                    city       TEXT,
                    extra      JSONB DEFAULT '{}'
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS auto_trader_log_ts_idx
                    ON auto_trader_log (ts DESC);
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS paper_trades (
                    id             BIGSERIAL PRIMARY KEY,
                    scan_ts        TIMESTAMPTZ DEFAULT NOW(),
                    city           TEXT NOT NULL,
                    nws_station    TEXT,
                    target_date    DATE NOT NULL,
                    horizon        TEXT,
                    ticker         TEXT NOT NULL,
                    bracket_label  TEXT,
                    lo_temp        NUMERIC(5,1),
                    hi_temp        NUMERIC(5,1),
                    grade          TEXT,
                    model_prob     NUMERIC(6,4),
                    yes_ask        NUMERIC(6,4),
                    mu             NUMERIC(6,2),
                    sigma          NUMERIC(6,3),
                    net_gap_c      INTEGER,
                    kelly_size     NUMERIC(8,2),
                    hours_to_cutoff NUMERIC(5,1),
                    mkt_rank_conf  TEXT,           -- top1/top2/outside_top2/skip
                    -- Model details
                    gfs_high       NUMERIC(5,1),   -- raw GFS forecast
                    ecmwf_high     NUMERIC(5,1),   -- raw ECMWF forecast
                    model_spread   NUMERIC(5,2),   -- abs(gfs - ecmwf)
                    edge_ratio     NUMERIC(8,3),   -- gap_c / sigma
                    gap_c          INTEGER,        -- raw edge before spread cost
                    spread_c       INTEGER,        -- bid/ask spread cost in cents
                    kelly_frac     NUMERIC(6,4),   -- raw half-kelly fraction
                    -- Market details at scan time
                    yes_bid        NUMERIC(6,4),   -- best bid
                    liq_grade      TEXT,           -- A/B/C/D liquidity grade
                    open_interest  INTEGER,        -- open interest at scan time
                    volume_24h     INTEGER,        -- 24h volume at scan time
                    fillable_a     NUMERIC(8,2),   -- fillable dollars at A-grade price
                    -- Structural flags
                    is_tail_bet         BOOLEAN,   -- model center below bracket
                    any_model_inside    BOOLEAN,   -- any model points at bracket
                    spread_exceeds_bracket BOOLEAN,-- model spread >= bracket width
                    book_limited        BOOLEAN,   -- kelly capped by book depth
                    -- Settlement fields (filled in later)
                    settled_temp   NUMERIC(5,1),
                    settled_correct BOOLEAN,
                    settled_ts     TIMESTAMPTZ
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS paper_trades_target_date_idx
                    ON paper_trades (target_date DESC);
            """)
            # Deduplicate existing rows before creating unique index
            # Keep the row with the latest scan_ts per (ticker, target_date)
            cur.execute("""
                DELETE FROM paper_trades
                WHERE id NOT IN (
                    SELECT DISTINCT ON (ticker, target_date) id
                    FROM paper_trades
                    ORDER BY ticker, target_date, scan_ts DESC
                );
            """)
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS paper_trades_ticker_date_idx
                    ON paper_trades (ticker, target_date);
            """)
            # calibration_snapshots — all grades, all signals
            # Used to compute rolling 30-day per-city forecast bias.
            # Bias re-enabled automatically once >= 30 settled rows exist
            # within the last 30 days for a given city.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS calibration_snapshots (
                    id             BIGSERIAL PRIMARY KEY,
                    scan_ts        TIMESTAMPTZ DEFAULT NOW(),
                    city           TEXT NOT NULL,
                    nws_station    TEXT,
                    target_date    DATE NOT NULL,
                    horizon        TEXT,
                    ticker         TEXT NOT NULL,
                    bracket_label  TEXT,
                    lo_temp        NUMERIC(5,1),
                    hi_temp        NUMERIC(5,1),
                    grade          TEXT,
                    model_prob     NUMERIC(6,4),
                    yes_ask        NUMERIC(6,4),
                    mu             NUMERIC(6,2),   -- raw model center (no bias applied)
                    sigma          NUMERIC(6,3),
                    net_gap_c      INTEGER,
                    kelly_size     NUMERIC(8,2),
                    hours_to_cutoff NUMERIC(5,1),
                    mkt_rank_conf  TEXT,           -- top1/top2/outside_top2/skip
                    -- Model details
                    gfs_high       NUMERIC(5,1),   -- raw GFS forecast
                    ecmwf_high     NUMERIC(5,1),   -- raw ECMWF forecast
                    model_spread   NUMERIC(5,2),   -- abs(gfs - ecmwf)
                    edge_ratio     NUMERIC(8,3),   -- gap_c / sigma
                    gap_c          INTEGER,        -- raw edge before spread cost
                    spread_c       INTEGER,        -- bid/ask spread cost in cents
                    kelly_frac     NUMERIC(6,4),   -- raw half-kelly fraction
                    -- Market details at scan time
                    yes_bid        NUMERIC(6,4),   -- best bid
                    liq_grade      TEXT,           -- A/B/C/D liquidity grade
                    open_interest  INTEGER,        -- open interest at scan time
                    volume_24h     INTEGER,        -- 24h volume at scan time
                    fillable_a     NUMERIC(8,2),   -- fillable dollars at A-grade price
                    -- Structural flags
                    is_tail_bet         BOOLEAN,   -- model center below bracket
                    any_model_inside    BOOLEAN,   -- any model points at bracket
                    spread_exceeds_bracket BOOLEAN,-- model spread >= bracket width
                    book_limited        BOOLEAN,   -- kelly capped by book depth
                    settled_temp   NUMERIC(5,1),   -- NWS CLI actual
                    settled_correct BOOLEAN,
                    settled_ts     TIMESTAMPTZ
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS calibration_snapshots_city_date_idx
                    ON calibration_snapshots (city, target_date DESC);
            """)
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS calibration_snapshots_ticker_date_idx
                    ON calibration_snapshots (ticker, target_date);
            """)
            # price_history — continuous price/forecast snapshots every scan cycle
            # Used to measure lag between model forecast shifts and market repricing.
            # Every bracket for every city logged on every scan — no grade filter.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS price_history (
                    id              BIGSERIAL PRIMARY KEY,
                    scan_ts         TIMESTAMPTZ DEFAULT NOW(),
                    city            TEXT NOT NULL,
                    target_date     DATE NOT NULL,
                    horizon         TEXT,           -- d0 or d1
                    ticker          TEXT NOT NULL,
                    bracket_label   TEXT,
                    lo_temp         NUMERIC(5,1),
                    hi_temp         NUMERIC(5,1),
                    -- Market prices at scan time
                    yes_ask         NUMERIC(6,4),
                    yes_bid         NUMERIC(6,4),
                    volume_24h      INTEGER,
                    open_interest   INTEGER,
                    -- Model forecast at scan time
                    mu              NUMERIC(6,2),   -- model center (best_high)
                    gfs_high        NUMERIC(5,1),
                    ecmwf_high      NUMERIC(5,1),
                    sigma           NUMERIC(6,3),
                    -- Derived
                    model_prob      NUMERIC(6,4),
                    net_gap_c       INTEGER,
                    grade           TEXT
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS price_history_ticker_ts_idx
                    ON price_history (ticker, scan_ts DESC);
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS price_history_city_date_idx
                    ON price_history (city, target_date, scan_ts DESC);
            """)
        conn.commit()
        print("  ✅ DB tables ready")
    except Exception as e:
        print(f"  ⚠️  DB setup failed: {e}")
    finally:
        conn.close()


def maybe_write_snapshot(city, month, days_remaining, true_mtd, wu_remaining,
                          projected_eom, confidence, wu_days_used, sigma=None,
                          kalshi_markets=None):
    """
    Write one daily snapshot row per (city, month, days_remaining).
    Also writes an intraday snapshot on every call for the chart.
    Only active inside the 10-day window.
    """
    if days_remaining > 10 or days_remaining < 0:
        return
    conn = get_db()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            # Daily snapshot (one per horizon, idempotent)
            cur.execute("""
                INSERT INTO forecast_snapshots
                    (city, month, snapshot_date, days_remaining, true_mtd,
                     wu_remaining, projected_eom, sigma_estimate, confidence, wu_days_used)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (month, days_remaining) DO NOTHING;
            """, (
                month, date.today(), days_remaining, true_mtd,
                wu_remaining, projected_eom, confidence, wu_days_used
            ))
        conn.commit()
    except Exception as e:
        print(f"  ⚠️  Snapshot write failed: {e}")
    finally:
        conn.close()


def fetch_snapshots(month=None):
    """Fetch all snapshots, optionally filtered to a month."""
    conn = get_db()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if month:
                cur.execute(
                    "SELECT * FROM forecast_snapshots WHERE month=%s ORDER BY days_remaining DESC",
                    (month,)
                )
            else:
                cur.execute(
                    "SELECT * FROM forecast_snapshots ORDER BY month DESC, days_remaining DESC LIMIT 60"
                )
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        print(f"  ⚠️  Snapshot fetch failed: {e}")
        return []
    finally:
        conn.close()


def fetch_accuracy_view():
    """Fetch the forecast_accuracy view for calibration display."""
    conn = get_db()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM forecast_accuracy")
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        print(f"  ⚠️  Accuracy view fetch failed: {e}")
        return []
    finally:
        conn.close()


def record_settlement(month, settled_total, settled_date=None, notes=""):
    """
    Record actual NWS settlement for a month.
    Called via POST /settle endpoint from the dashboard.
    """
    conn = get_db()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO month_settlements (month, settled_total, settled_date, notes)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (month) DO UPDATE
                  SET settled_total=%s, settled_date=%s, notes=%s;
            """, (
                month, settled_total, settled_date or date.today(), notes,
                settled_total, settled_date or date.today(), notes
            ))
        conn.commit()
        return True
    except Exception as e:
        print(f"  ⚠️  Settlement record failed: {e}")
        return False
    finally:
        conn.close()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        print(f"  [{datetime.now().strftime('%H:%M:%S')}] {format % args}")

    def send_json(self, data, status=200):
        import decimal as _decimal
        class _Enc(json.JSONEncoder):
            def default(self, o):
                if isinstance(o, _decimal.Decimal): return float(o)
                if hasattr(o, 'isoformat'): return o.isoformat()
                return super().default(o)
        body = json.dumps(data, cls=_Enc).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        path = urlparse(self.path).path

        if path == "/settle":
            # Record actual NWS settlement for a month
            # Body: {"month": "2026-03", "settled_total": 6.42, "notes": "..."}
            try:
                length = int(self.headers.get("Content-Length", 0))
                body   = json.loads(self.rfile.read(length))
                month  = body.get("month")
                total  = body.get("settled_total")
                notes  = body.get("notes", "")
                if not month or total is None:
                    self.send_json({"ok": False, "error": "month and settled_total required"}, 400)
                    return
                ok = record_settlement(month, float(total), notes=notes)
                self.send_json({"ok": ok, "month": month, "settled_total": total})
                print(f"  📌 Settlement recorded: {month} = {total}\"")
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, 400)
        elif path == "/admin/query":
            import os as _os
            try:
                length = int(self.headers.get("Content-Length", 0))
                body   = json.loads(self.rfile.read(length).decode()) if length else {}
                token  = body.get("token", "")
                sql    = body.get("sql", "").strip()
                expected = _os.environ.get("QUERY_TOKEN", "")
                if not expected:
                    self.send_json({"ok": False, "error": "QUERY_TOKEN not set in environment"})
                elif token != expected:
                    self.send_json({"ok": False, "error": "Invalid token"})
                elif not sql.upper().startswith("SELECT"):
                    self.send_json({"ok": False, "error": "Only SELECT queries allowed"})
                else:
                    conn_q = get_db()
                    if not conn_q:
                        self.send_json({"ok": False, "error": "No DB"})
                    else:
                        with conn_q.cursor() as cur:
                            cur.execute(sql)
                            cols = [d[0] for d in cur.description]
                            rows = cur.fetchall()
                            import decimal as _dec
                            import datetime as _dt
                            def _ser(v):
                                if isinstance(v, _dec.Decimal): return float(v)
                                if isinstance(v, (_dt.date, _dt.datetime)): return str(v)
                                return v
                            data = [dict(zip(cols, [_ser(c) for c in r])) for r in rows]
                        conn_q.close()
                        self.send_json({"ok": True, "rows": data, "count": len(data)})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)})
        else:
            self.send_response(404)
            self.end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        path = urlparse(self.path).path

        if path == "/data":
            # Parse city from query string — ?city=portland, defaults to seattle
            qs       = parse_qs(urlparse(self.path).query)
            city_key = qs.get("city", [ACTIVE_CITY])[0].lower()
            city_cfg = CITIES.get(city_key, CITIES[ACTIVE_CITY])

            print(f"\n📡 Fetching data for {city_cfg['label']}...")

            import concurrent.futures as _cf
            _ex = _cf.ThreadPoolExecutor(max_workers=5)
            try:
                _f_wu      = _ex.submit(fetch_wu_forecast, city_cfg)
                _f_hourly  = _ex.submit(fetch_wu_hourly,   city_cfg)
                _f_nws     = _ex.submit(fetch_nws_mtd,     city_cfg)
                _f_kalshi  = _ex.submit(fetch_kalshi_markets, city_cfg)
                # NWS must finish before IEM (IEM needs nws.issued)
                try:    nws = _f_nws.result(timeout=8)
                except Exception: nws = {"ok": False, "mtd": 0.0, "today": 0.0, "issued": None, "is_finalized": False}
                _f_iem = _ex.submit(fetch_iem_gap, nws.get("issued"), city_cfg)
                try:    wu        = _f_wu.result(timeout=8)
                except Exception: wu        = {"ok": False, "total_forecast": 0, "days": []}
                try:    wu_hourly = _f_hourly.result(timeout=8)
                except Exception: wu_hourly = {"ok": False, "today_remaining": 0}
                try:    kalshi    = _f_kalshi.result(timeout=8)
                except Exception: kalshi    = {"ok": False, "markets": []}
                try:    iem       = _f_iem.result(timeout=8)
                except Exception: iem       = {"ok": False, "gap_total": 0}
            finally:
                _ex.shutdown(wait=False)  # Never block — stalled threads die in background

            # True MTD assembly:
            # If NWS FINALIZED (issued 0-4 AM): MTD is through yesterday midnight, safe base.
            # If NWS PRELIMINARY (intraday): MTD includes today's rain so far.
            #   Subtract today_val to get through-yesterday base; IEM fills today's gap.
            #   This avoids double-counting today's rain in both NWS MTD and IEM gap.
            nws_is_fin    = nws.get("is_finalized", False)
            nws_today_val = nws.get("today", 0) or 0
            mtd_raw       = nws.get("mtd", 0)
            nws_base_mtd  = mtd_raw if nws_is_fin else round(mtd_raw - nws_today_val, 2)
            gap_total     = iem.get("gap_total", 0) if iem.get("ok") else 0
            true_mtd      = round(nws_base_mtd + gap_total, 2)

            # Today's projected EOD = true MTD + WU hourly remainder tonight
            today_remaining = wu_hourly.get("today_remaining", 0) if wu_hourly.get("ok") else 0
            today_eod       = round(true_mtd + today_remaining, 2)

            # 10-day projected total uses true MTD + OM forecast for remaining days THIS MONTH ONLY
            # total_forecast includes up to 16 days — must cap to days remaining in month
            import calendar
            now_dt         = datetime.now()
            days_in_month  = calendar.monthrange(now_dt.year, now_dt.month)[1]
            days_remaining = days_in_month - now_dt.day
            month_prefix   = now_dt.strftime("%Y-%m")

            # Sum only OM daily forecast days that fall within the current month
            wu_days_this_month = [
                d for d in wu.get("days", [])
                if d.get("date", "").startswith(month_prefix)
            ]
            wu_remaining = round(sum(d.get("qpf", 0) for d in wu_days_this_month), 2)

            # Only show full month-end projection if WU covers remainder
            wu_covers_eom = days_remaining <= 10
            projected     = round(true_mtd + wu_remaining, 2) if wu_covers_eom else None

            # Last WU forecast date for partial projection label
            wu_days      = wu.get("days", [])
            last_wu_date = wu_days[-1].get("date", "") if wu_days else ""

            # Confidence weight for this horizon
            conf       = confidence_weight(days_remaining)
            month_key  = datetime.now().strftime("%Y-%m")

            # WU days actually used (capped at days remaining)
            wu_days_used = min(days_remaining, len(wu.get("days", [])))

            # Analyze Kalshi market value — three-mode probability + liquidity scoring
            if kalshi.get("ok"):
                proj_for_signal = projected if wu_covers_eom else round(true_mtd + wu_remaining, 2)
                kalshi["markets"] = analyze_value(
                    kalshi["markets"], proj_for_signal, days_remaining,
                    true_mtd=true_mtd, city_key=city_key, month_num=now_dt.month,
                )

            # Use calibrated sigma from cache (falls back to base × horizon scale)
            sigma_est, _ = get_sigma(city_key, days_remaining)

            # Write snapshot — always inside 10-day window (intraday too)
            if wu_covers_eom and projected is not None:
                maybe_write_snapshot(
                    city         = city_key,
                    month        = f"{city_key}-{month_key}",
                    days_remaining = days_remaining,
                    true_mtd     = true_mtd,
                    wu_remaining = wu_remaining,
                    projected_eom = projected,
                    confidence   = conf,
                    wu_days_used = wu_days_used,
                    sigma        = sigma_est,
                    kalshi_markets = kalshi.get("markets", []),
                )

            result = {
                "timestamp":        datetime.now().isoformat(),
                "city":             city_key,
                "city_label":       city_cfg["label"],
                "city_regime":      city_cfg["regime"],
                "wu":               wu,
                "wu_hourly":        wu_hourly,
                "nws":              nws,
                "iem":              iem,
                "kalshi":           kalshi,
                "mtd":              true_mtd,
                "gap_total":        gap_total,
                "true_mtd":         true_mtd,
                    "nws_is_finalized":  nws.get("is_finalized", True),
                    "nws_mtd_type":      nws.get("mtd_type", "unknown"),
                    "nws_issued_hour":   nws.get("issued_hour"),
                "today_remaining":  today_remaining,
                "today_eod":        today_eod,
                "projected":        projected,
                "wu_covers_eom":    wu_covers_eom,
                "last_wu_date":     last_wu_date,
                "month":            datetime.now().strftime("%B %Y"),
                "days_remaining":   days_remaining,
                "confidence":       conf,
                "forecast_sigma":   round(get_sigma(city_key, days_remaining)[0], 3),
                "db_connected":     bool(DATABASE_URL and PSYCOPG2_AVAILABLE),
            }

            print(f"  ✅ NWS: {true_mtd}\" | IEM gap: +{gap_total}\" | True MTD: {true_mtd}\" | EOD proj: {today_eod}\" | WU 10-day: {wu_remaining}\" | conf: {conf}")
            self.send_json(result)

        elif path == "/ping":
            # Diagnostic: time each external source for one city
            import time
            city_key = parse_qs(urlparse(self.path).query).get("city", ["seattle"])[0]
            cfg = CITIES.get(city_key, CITIES["seattle"])
            results = {}
            for name, fn, args in [
                ("open_meteo_daily", fetch_wu_forecast, [cfg]),
                ("open_meteo_hourly", fetch_wu_hourly,  [cfg]),
                ("nws_cli",          fetch_nws_mtd,     [cfg]),
                ("kalshi",           fetch_kalshi_markets, [cfg]),
                ("iem_asos",         fetch_iem_gap,     [None, cfg]),
            ]:
                t0 = time.time()
                try:
                    r = fn(*args)
                    elapsed = round(time.time() - t0, 2)
                    results[name] = {"ok": r.get("ok", False), "elapsed_s": elapsed,
                                     "error": r.get("error") if not r.get("ok") else None}
                except Exception as e:
                    results[name] = {"ok": False, "elapsed_s": round(time.time()-t0,2), "error": str(e)}
            self.send_json({"city": city_key, "sources": results})

        elif path == "/scan":
            # Server-side parallel scan of all cities — much faster than browser parallel
            import concurrent.futures, calendar
            now_dt = datetime.now()
            days_in_month = calendar.monthrange(now_dt.year, now_dt.month)[1]
            days_remaining = days_in_month - now_dt.day

            def fetch_city(city_key):
                try:
                    cfg = CITIES.get(city_key)
                    if not cfg: return None
                    # Fetch all 5 sources in parallel within the city
                    inner = concurrent.futures.ThreadPoolExecutor(max_workers=5)
                    try:
                        f_wu      = inner.submit(fetch_wu_forecast, cfg)
                        f_hourly  = inner.submit(fetch_wu_hourly, cfg)
                        f_nws     = inner.submit(fetch_nws_mtd, cfg)
                        f_kalshi  = inner.submit(fetch_kalshi_markets, cfg)
                        # NWS needed for IEM — wait for it with 5s timeout
                        try:
                            nws = f_nws.result(timeout=5)
                        except Exception:
                            nws = {"ok": False, "mtd": 0, "issued": None}
                        f_iem = inner.submit(fetch_iem_gap, nws.get("issued"), cfg)
                        try: wu       = f_wu.result(timeout=5)
                        except Exception: wu = {"ok": False, "total_forecast": 0, "days": []}
                        try: wu_hourly = f_hourly.result(timeout=5)
                        except Exception: wu_hourly = {"ok": False, "today_remaining": 0}
                        try: kalshi   = f_kalshi.result(timeout=5)
                        except Exception: kalshi = {"ok": False, "markets": []}
                        try: iem      = f_iem.result(timeout=5)
                        except Exception: iem = {"ok": False, "gap_total": 0}
                    finally:
                        inner.shutdown(wait=False)

                    # Same double-count fix: preliminary NWS includes today's rain already
                    _nws_fin    = nws.get("is_finalized", False)
                    _nws_today  = nws.get("today", 0) or 0
                    _mtd_raw    = nws.get("mtd", 0)
                    _base_mtd   = _mtd_raw if _nws_fin else round(_mtd_raw - _nws_today, 2)
                    gap         = iem.get("gap_total", 0) if iem.get("ok") else 0
                    true_mtd    = round(_base_mtd + gap, 2)
                    today_rem = wu_hourly.get("today_remaining", 0) if wu_hourly.get("ok") else 0
                    today_eod = round(true_mtd + today_rem, 2)
                    # Cap to days within current month only — OM returns 16 days including next month
                    month_prefix = datetime.now().strftime("%Y-%m")
                    wu_days_this_month = [
                        d for d in wu.get("days", [])
                        if d.get("date", "").startswith(month_prefix)
                    ]
                    wu_remaining = round(sum(d.get("qpf", 0) for d in wu_days_this_month), 2)
                    wu_covers = days_remaining <= 10
                    projected = round(true_mtd + wu_remaining, 2) if wu_covers else None
                    proj_signal = projected if wu_covers else round(true_mtd + wu_remaining, 2)
                    if kalshi.get("ok"):
                        kalshi["markets"] = analyze_value(
                            kalshi["markets"], proj_signal, days_remaining,
                            true_mtd=true_mtd,
                            nws_is_finalized=nws.get("is_finalized", True),
                            city_key=city_key,
                            month_num=datetime.now().month,
                        )
                    return {
                        "city":           city_key,
                        "city_label":     cfg["label"],
                        "ok":             nws.get("ok", False),
                        "true_mtd":       true_mtd,
                        "nws_is_finalized": nws.get("is_finalized", True),
                        "nws_mtd_type":   nws.get("mtd_type", "unknown"),
                        "today_eod":      today_eod,
                        "projected":      projected,
                        "days_remaining": days_remaining,
                        "wu_covers_eom":  wu_covers,
                        "markets":        kalshi.get("markets", []),
                        "kalshi_ok":      kalshi.get("ok", False),
                    }
                except Exception as e:
                    return {"city": city_key, "ok": False, "error": str(e), "markets": []}

            # Hard 25s wall-clock timeout on entire scan — Railway kills at 60s
            ex = concurrent.futures.ThreadPoolExecutor(max_workers=8)
            try:
                futures = {ex.submit(fetch_city, ck): ck for ck in CITIES.keys()}
                city_results = []
                done, not_done = concurrent.futures.wait(futures, timeout=25)
                for f in done:
                    try:
                        result = f.result()
                        if result:
                            city_results.append(result)
                    except Exception:
                        pass
                for f, ck in futures.items():
                    if f not in done:
                        city_results.append({"city": ck, "ok": False,
                                             "error": "timeout", "markets": []})
            finally:
                ex.shutdown(wait=False)  # Don't block on timed-out threads

            self.send_json({
                "ok": True,
                "cities": [r for r in city_results if r],
                "days_remaining": days_remaining,
                "timestamp": datetime.now().isoformat(),
            })


        elif path == "/backtest":
            # Open-Meteo Previous Runs backtest
            # ?city=seattle&months=6  → returns per-horizon error stats
            # Uses Previous Runs API to get what GFS predicted at D-1..D-7
            # Compare against NWS actual monthly settlements from Postgres
            qs      = parse_qs(urlparse(self.path).query)
            city_key = qs.get("city", ["seattle"])[0]
            months_back = int(qs.get("months", ["6"])[0])
            cfg = CITIES.get(city_key, CITIES["seattle"])

            try:
                from datetime import date, timedelta
                import calendar as cal_mod

                results = []
                today = date.today()

                # Fetch live NWS CLM actuals (cached 6h)
                settlements = fetch_nws_clm_actuals(city_key)
                # Also try Postgres if available (adds any manually entered settlements)
                conn = get_db()
                if conn:
                    try:
                        with conn.cursor() as cur:
                            cur.execute("""
                                SELECT month, settled_total FROM month_settlements
                                WHERE month LIKE %s ORDER BY month DESC LIMIT %s
                            """, (f"{city_key}-%", months_back))
                            for row in cur.fetchall():
                                settlements[row[0]] = float(row[1])
                        conn.close()
                    except Exception:
                        pass

                # For each past complete month, fetch Previous Runs at D-1..D-7
                for m_back in range(1, months_back + 1):
                    # Get month
                    month_date = today.replace(day=1)
                    for _ in range(m_back):
                        month_date = (month_date - timedelta(days=1)).replace(day=1)

                    year  = month_date.year
                    month = month_date.month
                    days_in_month = cal_mod.monthrange(year, month)[1]
                    last_day = date(year, month, days_in_month)
                    month_key     = f"{city_key}-{year}-{month:02d}"  # for Postgres
                    actual_key    = f"{year}-{month:02d}"

                    actual = settlements.get(actual_key) or settlements.get(month_key)

                    # Fetch IEM daily totals via daily summary API — clean calendar-day values
                    station = cfg["icao_code"][1:]
                    network = cfg.get("iem_network", f"{cfg.get('state','WA')}_ASOS")
                    try:
                        iem_r = requests.get(
                            "https://mesonet.agron.iastate.edu/api/1/daily.json",
                            params={"station": station, "network": network,
                                    "year": year, "month": month},
                            headers={"User-Agent": "Mozilla/5.0"}, timeout=12)
                        iem_r.raise_for_status()
                        iem_data = iem_r.json()
                        daily_in = {}
                        for rec in iem_data.get("data", []):
                            day_str = rec.get("day", "")[:10]
                            p = rec.get("precip")
                            if p is not None and day_str:
                                try:
                                    daily_in[day_str] = float(p)
                                except (ValueError, TypeError):
                                    daily_in[day_str] = 0.0
                    except Exception:
                        daily_in = {}

                    # Fetch at each lead time: banked_inches + OM_forecast_remaining
                    horizon_data = {}
                    for lead in [1, 3, 5, 7]:
                        bank_through_day = days_in_month - lead
                        if bank_through_day < 1:
                            horizon_data[f"d{lead}"] = None
                            continue
                        banked_inches = sum(
                            daily_in.get(f"{year}-{month:02d}-{d:02d}", 0.0)
                            for d in range(1, bank_through_day + 1)
                        )
                        try:
                            _pvar = f"precipitation_previous_day{lead}"
                            r = requests.get(OM_PREV_URL, params={
                                "latitude":      cfg["lat"],
                                "longitude":     cfg["lon"],
                                "hourly":        _pvar,
                                "timezone":      cfg["tz"],
                                "past_days":     lead,
                                "forecast_days": 16,
                            }, timeout=12)
                            r.raise_for_status()
                            data   = r.json()
                            hours  = data.get("hourly", {}).get("time", [])
                            precip = data.get("hourly", {}).get(_pvar, [])
                            forecast_mm = sum(
                                float(p or 0) for h, p in zip(hours, precip)
                                if h and f"{year}-{month:02d}-" in h
                                and int(h[8:10]) > bank_through_day
                            )
                            horizon_data[f"d{lead}"] = round(banked_inches + forecast_mm / 25.4, 2)
                        except Exception:
                            horizon_data[f"d{lead}"] = None

                    results.append({
                        "month":    f"{year}-{month:02d}",
                        "city":     city_key,
                        "actual":   actual,
                        "horizons": horizon_data,
                        "errors": {
                            f"d{lead}": round(horizon_data.get(f"d{lead}",0) - actual, 2)
                            if actual is not None and horizon_data.get(f"d{lead}") is not None
                            else None
                            for lead in [1, 3, 5, 7]
                        } if actual is not None else {}
                    })

                # Compute summary stats per horizon
                summary = {}
                for lead in [1, 3, 5, 7]:
                    key = f"d{lead}"
                    errs = [r["errors"].get(key) for r in results if r.get("errors") and r["errors"].get(key) is not None]
                    if errs:
                        n = len(errs)
                        summary[key] = {
                            "n":         n,
                            "mean_err":  round(sum(errs)/n, 3),
                            "mae":       round(sum(abs(e) for e in errs)/n, 3),
                            "rmse":      round((sum(e**2 for e in errs)/n)**0.5, 3),
                            "bias":      "WET" if sum(errs)/n > 0.1 else "DRY" if sum(errs)/n < -0.1 else "NEUTRAL",
                            "sigma":     round((sum(e**2 for e in errs)/n)**0.5, 3),
                        }

                # Auto-update sigma cache from RMSE
                update_sigma_from_backtest(city_key, summary)

                self.send_json({
                    "ok":            True,
                    "city":          city_key,
                    "months":        months_back,
                    "results":       results,
                    "summary":       summary,
                    "sigma_updated": city_key in _SIGMA_CACHE,
                    "note":          "RMSE values now used as sigma in live probability model"
                })

            except Exception as e:
                self.send_json({"ok": False, "error": str(e)})

        elif path == "/backtest/iem":
            # IEM gap fill accuracy backtest
            # Measures: how well does IEM hourly data match the next morning's NWS finalized total?
            # Method: for each past day, take (NWS_day_N + IEM_gap_fill) vs NWS_day_N+1
            # ?city=seattle&days=30
            qs       = parse_qs(urlparse(self.path).query)
            city_key = qs.get("city", ["seattle"])[0]
            days_back = int(qs.get("days", ["30"])[0])
            cfg = CITIES.get(city_key, CITIES["seattle"])

            try:
                from datetime import date as dt_date, timedelta
                results = []
                iem_station = cfg["icao_code"][1:]  # KSEA → SEA

                # Fetch IEM daily precipitation for the last N days
                end_dt   = dt_date.today()
                start_dt = end_dt - timedelta(days=days_back + 2)
                params = {
                    "station": iem_station,
                    "data":    "p01i",
                    "year1":   start_dt.year, "month1": start_dt.month, "day1": start_dt.day,
                    "year2":   end_dt.year,   "month2": end_dt.month,   "day2": end_dt.day,
                    "tz":      cfg.get("tz", "America/Los_Angeles"),
                    "format":  "json", "latlon": "no", "missing": "M", "trace": "T",
                    "direct":  "no",
                }
                r = requests.get(
                    "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py",
                    params=params, timeout=10
                )
                r.raise_for_status()
                raw = r.json()
                # Group hourly readings by date
                from collections import defaultdict
                daily_totals = defaultdict(float)
                for obs in raw.get("data", []):
                    t = obs.get("valid", "")
                    p = obs.get("p01i", "M")
                    if t and p not in ("M", "T", None):
                        try:
                            date_str = t[:10]
                            daily_totals[date_str] += float(p)
                        except Exception:
                            pass

                # Also fetch NWS CLI archive via IEM API for the same period
                nws_r = requests.get(
                    f"https://mesonet.agron.iastate.edu/api/1/nws/cli.json",
                    params={"station": cfg["nws_issuedby"], "days": days_back + 5},
                    timeout=10
                )
                nws_data = {}
                if nws_r.ok:
                    for entry in nws_r.json().get("data", []):
                        d = entry.get("date", "")
                        p = entry.get("precip", None)
                        if d and p is not None:
                            try: nws_data[d] = float(p)
                            except Exception: pass

                # Compare: IEM daily sum vs NWS official for same day
                errors = []
                for date_str, iem_total in sorted(daily_totals.items()):
                    iem_in = round(iem_total, 2)
                    nws_val = nws_data.get(date_str)
                    if nws_val is not None:
                        err = round(iem_in - nws_val, 3)
                        errors.append(err)
                        results.append({
                            "date":    date_str,
                            "iem":     iem_in,
                            "nws":     nws_val,
                            "error":   err,
                            "pct_err": round(err / nws_val * 100, 1) if nws_val > 0.05 else None,
                        })

                summary = {}
                if errors:
                    n    = len(errors)
                    mean = round(sum(errors)/n, 3)
                    mae  = round(sum(abs(e) for e in errors)/n, 3)
                    rmse = round((sum(e**2 for e in errors)/n)**0.5, 3)
                    # Only on rainy days (NWS > 0.05")
                    rainy = [r for r in results if r["nws"] > 0.05]
                    rainy_errs = [r["error"] for r in rainy]
                    summary = {
                        "n": n, "mean_err": mean, "mae": mae, "rmse": rmse,
                        "bias": "WET" if mean > 0.02 else "DRY" if mean < -0.02 else "NEUTRAL",
                        "rainy_days": len(rainy),
                        "rainy_mae": round(sum(abs(e) for e in rainy_errs)/len(rainy_errs), 3) if rainy_errs else None,
                        "interpretation": (
                            f"IEM is on average {abs(mean)}\" {'above' if mean>0 else 'below'} NWS official. "
                            f"On rainy days (>{0.05}\"), mean absolute error is {summary.get('rainy_mae','?')}\". "
                            f"This is the gap fill error you should expect in true_mtd."
                        )
                    }
                    summary["interpretation"] = (
                        f"IEM is on average {abs(mean):.3f}\" {'above' if mean>0 else 'below'} NWS official. "
                        f"RMSE = {rmse}\". Rainy day MAE = {rainy_mae if (rainy_mae:=summary.get('rainy_mae')) else '?'}\". "
                        f"Bias: {summary['bias']}."
                    )

                self.send_json({"ok": True, "city": city_key, "days": days_back,
                                "results": results[-60:], "summary": summary})

            except Exception as e:
                self.send_json({"ok": False, "error": str(e)})

        elif path == "/backtest/corrections":
            # NWS correction frequency backtest
            # Fetches multiple CLI versions for recent days and compares them
            # to detect how often and by how much NWS revises its MTD
            qs       = parse_qs(urlparse(self.path).query)
            city_key = qs.get("city", ["seattle"])[0]
            cfg = CITIES.get(city_key, CITIES["seattle"])

            try:
                # Fetch last 50 versions of the CLI and look for CCA (corrected) products
                cli_url = (
                    f"https://forecast.weather.gov/product.php"
                    f"?site={cfg['nws_site']}&issuedby={cfg['nws_issuedby']}"
                    f"&product=CLI&format=txt&version=1"
                )
                headers = {"User-Agent": "Mozilla/5.0"}
                # Fetch versions 1–10 and compare MTD values
                versions = []
                for v in range(1, 11):
                    try:
                        url = cli_url.replace("&version=1", f"&version={v}")
                        r = requests.get(url, headers=headers, timeout=5)
                        if not r.ok: break
                        from bs4 import BeautifulSoup as BS
                        soup = BS(r.text, "html.parser")
                        pre = soup.find("pre")
                        raw = pre.get_text() if pre else r.text
                        mtd_m = re.search(r"MONTH TO DATE\s+([\d\.T]+)", raw, re.IGNORECASE)
                        issued_m = re.search(r"(\d{3,4}\s+(?:AM|PM)\s+\w+\s+\w+\s+\w+\s+\d+\s+\d{4})", raw, re.IGNORECASE)
                        cca = "CORRECTED" in raw or "CCA" in raw[:50]
                        if mtd_m:
                            versions.append({
                                "version": v,
                                "mtd": float(mtd_m.group(1)) if mtd_m.group(1) != "T" else 0.0,
                                "issued": issued_m.group(1).strip() if issued_m else None,
                                "is_correction": cca,
                            })
                    except Exception:
                        break

                # Find corrections: MTD changed between versions
                corrections = []
                for i in range(1, len(versions)):
                    prev = versions[i]    # older version (higher number = older)
                    curr = versions[i-1]  # newer version
                    delta = round(curr["mtd"] - prev["mtd"], 2)
                    if abs(delta) >= 0.05:  # threshold for meaningful change
                        corrections.append({
                            "from_version": prev["version"],
                            "to_version":   curr["version"],
                            "from_mtd":     prev["mtd"],
                            "to_mtd":       curr["mtd"],
                            "delta":        delta,
                            "direction":    "UP" if delta > 0 else "DOWN",
                            "is_cca":       curr["is_correction"],
                        })

                self.send_json({
                    "ok":          True,
                    "city":        city_key,
                    "versions":    versions,
                    "corrections": corrections,
                    "correction_count": len(corrections),
                    "note": "Corrections ≥0.05\" in MTD across the last 10 CLI versions. Covers recent days only."
                })

            except Exception as e:
                self.send_json({"ok": False, "error": str(e)})

        elif path.startswith("/orderbook/"):
            ticker = path.split("/orderbook/")[1].strip("/")
            if not ticker:
                self.send_json({"ok": False, "error": "No ticker"})
            else:
                try:
                    ob_path = f"/trade-api/v2/markets/{ticker}/orderbook"
                    r = requests.get(
                        f"{KALSHI_BASE}/markets/{ticker}/orderbook",
                        headers=kalshi_auth_headers("GET", ob_path), timeout=8)
                    r.raise_for_status()
                    ob = r.json().get("orderbook", {})
                    EDGE_CEILING = 97
                    depth, total_contracts, total_cost = [], 0, 0.0
                    # YES asks are derived from NO bids: YES ask = 100 - no_bid
                    # NO bids are sorted ascending (lowest first), so highest NO bid = best YES ask
                    # Iterate descending to get best (lowest) YES ask first
                    no_bids = sorted(ob.get("no", []), key=lambda x: int(x[0]), reverse=True)
                    for level in no_bids:
                        no_price_c = int(level[0])
                        yes_ask_c  = 100 - no_price_c   # implied YES ask
                        size       = int(level[1])
                        if yes_ask_c > EDGE_CEILING:
                            continue  # skip levels where YES ask exceeds ceiling
                        if yes_ask_c <= 0:
                            continue
                        cost = round(yes_ask_c / 100 * size, 2)
                        depth.append({"price_c": yes_ask_c, "size": size, "cost": cost,
                                      "no_bid_c": no_price_c})
                        total_contracts += size; total_cost += cost
                    self.send_json({"ok": True, "ticker": ticker, "depth": depth,
                        "total_contracts": total_contracts, "total_cost": round(total_cost, 2),
                        "edge_ceiling_c": EDGE_CEILING})
                except Exception as e:
                    self.send_json({"ok": False, "error": str(e)})

        elif path == "/orders/auto":
            qs = parse_qs(urlparse(self.path).query)
            ticker = qs.get("ticker", [""])[0]
            side   = qs.get("side", ["yes"])[0].lower()
            if not ticker or not KALSHI_KEY_ID:
                self.send_json({"ok": False, "error": "ticker and API key required"})
            else:
                try:
                    EDGE_CEILING = 97; MAX_PCT = float(qs.get("cap", ["0.70"])[0])
                    bal_r = requests.get(f"{KALSHI_BASE}/portfolio/balance",
                        headers=kalshi_auth_headers("GET", "/trade-api/v2/portfolio/balance"), timeout=8)
                    bal_r.raise_for_status(); bal = bal_r.json()
                    cash    = float(bal.get("balance", 0) or 0) / 100
                    port    = float(bal.get("portfolio_value", 0) or 0) / 100  # in cents like balance
                    budget  = min(cash, port * MAX_PCT)
                    ob_r = requests.get(f"{KALSHI_BASE}/markets/{ticker}/orderbook",
                        headers=kalshi_auth_headers("GET", f"/trade-api/v2/markets/{ticker}/orderbook"), timeout=8)
                    ob_r.raise_for_status()
                    ob_data = ob_r.json().get("orderbook", {})
                    orders, spent = [], 0.0
                    if side == "yes":
                        # YES asks = 100 - NO bids, sorted descending by NO bid = ascending YES ask
                        no_bids = sorted(ob_data.get("no", []), key=lambda x: int(x[0]), reverse=True)
                        for level in no_bids:
                            no_price_c = int(level[0])
                            price_c    = 100 - no_price_c  # implied YES ask
                            size       = int(level[1])
                            if price_c > EDGE_CEILING or price_c <= 0:
                                continue
                            level_cost = price_c / 100 * size
                            if spent + level_cost > budget:
                                partial = int((budget - spent) / (price_c / 100))
                                if partial > 0:
                                    orders.append({"price_c": price_c, "count": partial,
                                        "cost": round(partial * price_c / 100, 2)})
                                    spent += partial * price_c / 100
                                break
                            orders.append({"price_c": price_c, "count": size,
                                "cost": round(level_cost, 2)})
                            spent += level_cost
                    else:
                        # Buying NO: NO asks = 100 - YES bids
                        yes_bids = sorted(ob_data.get("yes", []), key=lambda x: int(x[0]), reverse=True)
                        for level in yes_bids:
                            yes_price_c = int(level[0])
                            price_c     = 100 - yes_price_c  # implied NO ask
                            size        = int(level[1])
                            if price_c > EDGE_CEILING or price_c <= 0:
                                continue
                            level_cost = price_c / 100 * size
                            if spent + level_cost > budget:
                                partial = int((budget - spent) / (price_c / 100))
                                if partial > 0:
                                    orders.append({"price_c": price_c, "count": partial,
                                        "cost": round(partial * price_c / 100, 2)})
                                    spent += partial * price_c / 100
                                break
                            orders.append({"price_c": price_c, "count": size,
                                "cost": round(level_cost, 2)})
                            spent += level_cost
                    self.send_json({"ok": True, "ticker": ticker, "side": side,
                        "cash": round(cash, 2), "portfolio": round(port, 2),
                        "budget": round(budget, 2), "orders": orders,
                        "total_contracts": sum(o["count"] for o in orders),
                        "total_cost": round(spent, 2)})
                except Exception as e:
                    self.send_json({"ok": False, "error": str(e)})

        elif path == "/orders" and self.command == "POST":
            if not KALSHI_KEY_ID:
                self.send_json({"ok": False, "error": "No Kalshi API key"})
            else:
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body   = json.loads(self.rfile.read(length))
                    ticker      = body.get("ticker", "")
                    side        = body.get("side", "yes").lower()
                    count       = int(body.get("count", 0))
                    yes_price_c = int(body.get("yes_price_c", 0))
                    if not ticker or count <= 0 or yes_price_c <= 0:
                        self.send_json({"ok": False, "error": "ticker, count, yes_price_c required"}); return
                    if yes_price_c > 97:
                        self.send_json({"ok": False, "error": "yes_price_c exceeds 97c ceiling"}); return
                    payload = {"ticker": ticker, "side": side, "action": "buy",
                               "type": "limit", "count": count, "yes_price": yes_price_c}
                    r = requests.post(f"{KALSHI_BASE}/orders",
                        headers=kalshi_auth_headers("POST", "/trade-api/v2/orders"),
                        json=payload, timeout=10)
                    resp = r.json()
                    if r.ok:
                        self.send_json({"ok": True, "ticker": ticker, "side": side,
                            "count": count, "price_c": yes_price_c, "order": resp})
                    else:
                        self.send_json({"ok": False, "error": resp, "status": r.status_code})
                except Exception as e:
                    self.send_json({"ok": False, "error": str(e)})

        elif path == "/debug/positions":
            try:
                pos_r = requests.get(f"{KALSHI_BASE}/portfolio/positions",
                    headers=kalshi_auth_headers("GET", "/trade-api/v2/portfolio/positions"),
                    timeout=10, params={"limit": 10, "count_filter": "position"})
                pos_r.raise_for_status()
                raw = pos_r.json()
                self.send_json({"ok": True, "raw_positions": raw.get("market_positions", [])[:3],
                    "count": len(raw.get("market_positions", []))})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)})

        elif path == "/auto-trader/config":
            if self.command == "POST":
                # Save settings from UI
                try:
                    length  = int(self.headers.get("Content-Length", 0))
                    body    = json.loads(self.rfile.read(length))
                    conn    = get_db()
                    if not conn:
                        self.send_json({"ok": False, "error": "No DB"}); return
                    with conn.cursor() as cur:
                        for key, val in body.items():
                            if key in _AT_CONFIG:
                                cur.execute("""
                                    INSERT INTO auto_trader_config (key, value, updated_at)
                                    VALUES (%s, %s, NOW())
                                    ON CONFLICT (key) DO UPDATE
                                        SET value=%s, updated_at=NOW()
                                """, (key, json.dumps(val) if isinstance(val, (list, dict)) else str(val),
                                      json.dumps(val) if isinstance(val, (list, dict)) else str(val)))
                                # Update in-memory config immediately
                                _AT_CONFIG[key] = val
                    conn.commit()
                    conn.close()
                    # Handle enabled toggle
                    if "enabled" in body:
                        _AT_CONFIG["enabled"] = bool(body["enabled"])
                        if body["enabled"]:
                            start_auto_trader_scheduler()
                            at_log("SCAN", "Auto-trader enabled via UI")
                        else:
                            at_log("SCAN", "Auto-trader disabled via UI")
                    self.send_json({"ok": True, "config": _AT_CONFIG})
                except Exception as e:
                    self.send_json({"ok": False, "error": str(e)})
            else:
                # GET — return current config + scheduler status
                self.send_json({
                    "ok":     True,
                    "config": _AT_CONFIG,
                    "scheduler_alive": _AT_THREAD is not None and _AT_THREAD.is_alive(),
                })

        elif path == "/auto-trader/log":
            # Return last 3 days of execution log — DB first, in-memory fallback
            try:
                conn = get_db()
                if not conn:
                    # Fall back to in-memory log
                    with _AT_LOCK:
                        rows = list(reversed(_AT_LOG[-500:]))
                    self.send_json({"ok": True, "log": rows, "count": len(rows),
                                    "source": "memory"})
                    return
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT ts, level, msg, ticker, city, extra
                        FROM auto_trader_log
                        WHERE ts > NOW() - INTERVAL '3 days'
                        ORDER BY ts DESC
                        LIMIT 500
                    """)
                    cols = [d[0] for d in cur.description]
                    rows = []
                    for r in cur.fetchall():
                        row = dict(zip(cols, r))
                        row["ts"] = row["ts"].isoformat() if row["ts"] else None
                        row["extra"] = row["extra"] or {}
                        rows.append(row)
                conn.close()
                # Merge in-memory entries newer than latest DB entry
                if rows:
                    latest_db_ts = rows[0].get("ts", "")
                else:
                    latest_db_ts = ""
                with _AT_LOCK:
                    mem_rows = [e for e in _AT_LOG
                                if str(e.get("ts","")) > latest_db_ts]
                rows = mem_rows + rows
                self.send_json({"ok": True, "log": rows, "count": len(rows),
                                "source": "db"})
            except Exception as e:
                # Last resort — in-memory only
                with _AT_LOCK:
                    rows = list(reversed(_AT_LOG[-500:]))
                self.send_json({"ok": True, "log": rows, "count": len(rows),
                                "source": "memory", "db_error": str(e)})

        elif path == "/auto-trader/run":
            # Manually trigger one cycle (bypasses enabled check for testing)
            try:
                import threading as _t2
                t = _t2.Thread(target=run_auto_trader_cycle,
                               kwargs={"force": True}, daemon=True)
                t.start()
                self.send_json({"ok": True, "msg": "Cycle started in background"})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)})

        elif path == "/scan/run":
            # Manually trigger one background scan cycle (paper trades + snapshots)
            try:
                import threading as _t2
                t = _t2.Thread(target=_run_background_scan, daemon=True)
                t.start()
                self.send_json({"ok": True, "msg": "Background scan started — check /paper-trades in ~60s"})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)})

        elif path == "/debug/settlements":
            try:
                qs    = parse_qs(urlparse(self.path).query)
                limit = int(qs.get("limit", [20])[0])
                r = requests.get(f"{KALSHI_BASE}/portfolio/settlements",
                    headers=kalshi_auth_headers("GET", "/trade-api/v2/portfolio/settlements"),
                    timeout=10, params={"limit": limit})
                r.raise_for_status()
                raw = r.json()
                settlements = raw.get("settlements", [])
                self.send_json({"ok": True, "raw_settlements": settlements,
                    "keys": list(settlements[0].keys()) if settlements else [],
                    "count": len(settlements)})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)})

        elif path == "/backtest/actuals":
            qs       = parse_qs(urlparse(self.path).query)
            city_key = qs.get("city", ["seattle"])[0]
            force    = qs.get("force", ["0"])[0] == "1"
            if force:
                _CLM_CACHE_TS[city_key] = 0
            try:
                actuals = fetch_nws_clm_actuals(city_key)
                cfg = CITIES.get(city_key, {})
                self.send_json({"ok": True, "city": city_key, "actuals": actuals,
                    "count": len(actuals), "cached": not force,
                    "clm_url": f"https://forecast.weather.gov/product.php?site=NWS&issuedby={cfg.get('nws_issuedby','?')}&product=CLM&format=txt&version=1"})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)})

        elif path == "/backtest/calibration":
            qs          = parse_qs(urlparse(self.path).query)
            city_key    = qs.get("city", ["seattle"])[0]
            months_back = int(qs.get("months", ["6"])[0])
            cfg         = CITIES.get(city_key, CITIES["seattle"])
            try:
                from datetime import date, timedelta
                import calendar as cal_mod
                actuals = fetch_nws_clm_actuals(city_key)
                today   = date.today()
                horizon_errors = {f"d{l}": [] for l in [1, 3, 5, 7]}
                for m_back in range(1, months_back + 1):
                    month_date = today.replace(day=1)
                    for _ in range(m_back):
                        month_date = (month_date - timedelta(days=1)).replace(day=1)
                    year = month_date.year; month = month_date.month
                    actual_key = f"{year}-{month:02d}"
                    actual = actuals.get(actual_key)
                    if actual is None: continue
                    days_in_month = cal_mod.monthrange(year, month)[1]
                    for lead in [1, 3, 5, 7, 10]:
                        try:
                            _pvar = f"precipitation_previous_day{min(lead, 7)}"
                            r = requests.get(OM_PREV_URL, params={
                                "latitude": cfg["lat"], "longitude": cfg["lon"],
                                "hourly": _pvar, "timezone": cfg["tz"],
                                "past_days": lead, "forecast_days": 16,
                            }, timeout=12)
                            r.raise_for_status(); data = r.json()
                            hours  = data.get("hourly", {}).get("time", [])
                            precip = data.get("hourly", {}).get(_pvar, [])
                            month_mm = sum(float(p or 0) for h, p in zip(hours, precip)
                                         if h and h[:7] == f"{year}-{month:02d}")
                            error = round(month_mm / 25.4 - actual, 2)
                            horizon_errors[f"d{lead}"].append(error)
                        except Exception: continue
                bias_summary = {}
                for key, errs in horizon_errors.items():
                    if not errs: continue
                    n = len(errs); mean = sum(errs)/n
                    bias_summary[key] = {"n": n, "mean_err": round(mean, 3),
                        "mae": round(sum(abs(e) for e in errs)/n, 3),
                        "rmse": round((sum(e**2 for e in errs)/n)**0.5, 3),
                        "bias_dir": "OVER" if mean > 0.05 else "UNDER" if mean < -0.05 else "NEUTRAL"}
                    _BIAS_CACHE[f"{city_key}-{key}"] = round(mean, 3)
                self.send_json({"ok": True, "city": city_key, "bias": bias_summary,
                    "bias_cache_updated": True})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)})

        elif path == "/calibrate/all":
            # Runs backtest (sigma) + bias calibration for all 8 cities in parallel.
            # Call this once after every deploy to restore in-memory caches.
            # ?months=N to control how many months of history to use (default 12)
            import concurrent.futures as _cf
            from datetime import date as _date, timedelta as _td
            import calendar as _cal

            qs          = parse_qs(urlparse(self.path).query)
            months_back = int(qs.get("months", ["12"])[0])

            def calibrate_city(city_key):
                cfg = CITIES.get(city_key)
                result = {"city": city_key, "sigma_ok": False, "bias_ok": False,
                          "sigma_months": 0, "bias_horizons": 0, "errors": []}
                try:
                    actuals = fetch_nws_clm_actuals(city_key)
                    today   = _date.today()
                    errors  = []   # (om_monthly_forecast - clm_actual) per month

                    for m_back in range(1, months_back + 1):
                        month_date = today.replace(day=1)
                        for _ in range(m_back):
                            month_date = (month_date - _td(days=1)).replace(day=1)
                        year  = month_date.year
                        month = month_date.month
                        actual = actuals.get(f"{year}-{month:02d}")
                        if actual is None:
                            continue

                        days_in_month = _cal.monthrange(year, month)[1]
                        start = f"{year}-{month:02d}-01"
                        end   = f"{year}-{month:02d}-{days_in_month:02d}"

                        # Fetch OM historical forecast for the full month
                        # archive-api gives the assembled forecast (best-match model)
                        try:
                            r = requests.get(
                                "https://archive-api.open-meteo.com/v1/archive",
                                params={
                                    "latitude":      cfg["lat"],
                                    "longitude":     cfg["lon"],
                                    "daily":         "precipitation_sum",
                                    "timezone":      cfg["tz"],
                                    "start_date":    start,
                                    "end_date":      end,
                                },
                                timeout=15
                            )
                            r.raise_for_status()
                            data = r.json()
                            daily_mm = data.get("daily", {}).get("precipitation_sum", [])
                            month_mm = sum(float(v or 0) for v in daily_mm)
                            om_inches = round(month_mm / 25.4, 3)
                            error = round(om_inches - actual, 3)
                            errors.append(error)
                        except Exception as ex:
                            result["errors"].append(f"{year}-{month:02d}: {str(ex)[:60]}")

                    if not errors:
                        return result

                    n     = len(errors)
                    mean  = round(sum(errors) / n, 3)
                    rmse  = round((sum(e**2 for e in errors) / n) ** 0.5, 3)
                    mae   = round(sum(abs(e) for e in errors) / n, 3)

                    # Store single sigma. Horizon scaling is applied in get_sigma() via
                    # HORIZON_SCALE — no need to compute per-horizon here.
                    _BIAS_CACHE[f"{city_key}-monthly"] = mean
                    # Also store per-horizon keys for compatibility
                    for d in [1, 3, 5, 7]:
                        _BIAS_CACHE[f"{city_key}-d{d}"] = mean

                    summary = {"monthly": {"n": n, "rmse": rmse, "mean_err": mean, "mae": mae,
                                           "bias": "WET" if mean > 0.05 else "DRY" if mean < -0.05 else "NEUTRAL"}}
                    update_sigma_from_backtest(city_key, summary)
                    result["sigma_ok"]      = city_key in _SIGMA_CACHE
                    result["sigma_months"]  = n
                    result["bias_ok"]       = True
                    result["bias_horizons"] = 4   # d1,d3,d5,d7 all use same monthly bias
                    result["summary"]       = summary

                except Exception as e:
                    result["errors"].append(str(e))

                return result

            try:
                city_keys = list(CITIES.keys())
                ex = _cf.ThreadPoolExecutor(max_workers=8)
                try:
                    futures = {ex.submit(calibrate_city, ck): ck for ck in city_keys}
                    results = []
                    done, _ = _cf.wait(futures, timeout=120)
                    for f in done:
                        try:
                            results.append(f.result())
                        except Exception as e:
                            results.append({"city": futures[f], "error": str(e)})
                finally:
                    ex.shutdown(wait=False)

                all_ok = all(r.get("sigma_ok") and r.get("bias_ok") for r in results)
                self.send_json({
                    "ok":      True,
                    "cities":  results,
                    "all_ok":  all_ok,
                    "sigma_cache_size": len(_SIGMA_CACHE),
                    "bias_cache_size":  len(_BIAS_CACHE),
                    "note":    "Sigma and bias caches updated for all cities. In-memory only — re-run after each deploy."
                })
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)})

        elif path == "/temp/scan":
            # Full scan: all temp cities, both HIGH and LOW, D+1 horizon by default
            # Optional ?horizon=d0  to run same-day scan
            # Optional ?city=chicago to scan a single city
            import concurrent.futures as _cf
            qs       = parse_qs(urlparse(self.path).query)
            horizon  = qs.get("horizon", ["d1"])[0]
            city_req = qs.get("city", [None])[0]
            cities   = [city_req] if city_req and city_req in TEMP_CITIES else list(TEMP_CITIES.keys())

            try:
                ex = _cf.ThreadPoolExecutor(max_workers=8)
                try:
                    futures = {ex.submit(scan_temp_city, ck, horizon): ck for ck in cities}
                    results = []
                    done, _ = _cf.wait(futures, timeout=60)
                    for f in done:
                        try:
                            r = f.result()
                            if r: results.append(r)
                        except Exception as e:
                            results.append({"ok": False, "city": futures[f], "error": str(e)})
                finally:
                    ex.shutdown(wait=False)

                # Sort: A-grade first, then B, then by best_edge_c desc
                grade_order = {"A": 0, "B": 1, "none": 2}
                results.sort(key=lambda x: (grade_order.get(x.get("best_grade","none"), 3), -x.get("best_edge_c", 0)))

                total_actionable = sum(r.get("actionable_count", 0) for r in results)
                self.send_json({
                    "ok":                True,
                    "horizon":           horizon,
                    "cities":            results,
                    "city_count":        len(results),
                    "total_actionable":  total_actionable,
                    "low_suppressed_d1": horizon == "d1",
                    "timestamp":         datetime.now().isoformat(),
                })
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)})

        elif path == "/temp/city":
            # Single city detail — refreshes without cache for live dashboard view
            qs      = parse_qs(urlparse(self.path).query)
            city    = qs.get("city", ["nyc"])[0]
            horizon = qs.get("horizon", ["d1"])[0]
            # Bust cache so dashboard always gets fresh data
            cache_key = f"{city}-{horizon}"
            _TEMP_SCAN_CACHE.pop(cache_key, None)
            result = scan_temp_city(city, horizon)
            self.send_json(result)

        elif path == "/temp/calibrate":
            # Backtest: pull 90 days of Open-Meteo historical D+1 forecasts vs NWS CLI actuals
            # and compute per-station warm bias and σ.
            # Writes to _TEMP_BIAS_CACHE (in-memory; re-run after each deploy).
            # Usage: GET /temp/calibrate?city=chicago  or omit city for all cities.
            import concurrent.futures as _cf
            import time as _t
            from math import sqrt
            qs       = parse_qs(urlparse(self.path).query)
            city_req = qs.get("city", [None])[0]
            cities   = [city_req] if city_req and city_req in TEMP_CITIES else list(TEMP_CITIES.keys())
            days_back = int(qs.get("days", ["90"])[0])

            def calibrate_temp_city(city_key):
                """
                Bulk-fetch 90 days of GFS, ECMWF, and archive actuals in
                3 API calls total (not 270).  Computes warm bias and RMSE.
                """
                cfg = TEMP_CITIES[city_key]
                try:
                    import pytz
                    from datetime import datetime as dt_cls, timedelta
                    from math import sqrt
                    tz_name  = cfg["tz"]
                    local_tz = pytz.timezone(tz_name)
                    now      = dt_cls.utcnow().replace(tzinfo=pytz.utc).astimezone(local_tz)

                    end_dt   = now - timedelta(days=2)
                    start_dt = now - timedelta(days=days_back + 1)
                    start_s  = start_dt.strftime("%Y-%m-%d")
                    end_s    = end_dt.strftime("%Y-%m-%d")

                    def _bulk(model_str, use_archive=False):
                        base = ("https://archive-api.open-meteo.com/v1/archive"
                                if use_archive else
                                "https://api.open-meteo.com/v1/forecast")
                        p = {"latitude": cfg["lat"], "longitude": cfg["lon"],
                             "daily": "temperature_2m_max", "timezone": tz_name}
                        if use_archive:
                            # Archive API supports explicit date range
                            p["start_date"] = start_s
                            p["end_date"]   = end_s
                        else:
                            # Forecast API: past_days cannot be combined with start/end_date
                            p["models"]        = model_str
                            p["past_days"]     = days_back + 3
                            p["forecast_days"] = 1
                        r2 = requests.get(base, params=p, timeout=20)
                        d2 = r2.json()
                        if d2.get("error"):
                            raise ValueError(f"Open-Meteo: {d2.get('reason', d2.get('error'))}")
                        dates = d2.get("daily", {}).get("time", [])
                        vals  = d2.get("daily", {}).get("temperature_2m_max", [])
                        return {ds: round(v*9/5+32, 1) for ds, v in zip(dates, vals)
                                if v is not None and start_s <= ds <= end_s}

                    import concurrent.futures as _cf2
                    ex2 = _cf2.ThreadPoolExecutor(max_workers=3)
                    try:
                        fg = ex2.submit(_bulk, "gfs_seamless", False)
                        fe = ex2.submit(_bulk, "ecmwf_ifs",    False)
                        fa = ex2.submit(_bulk, None,           True)
                        gfs_map    = fg.result(timeout=30)
                        ecmwf_map  = fe.result(timeout=30)
                        actual_map = fa.result(timeout=30)
                    finally:
                        ex2.shutdown(wait=False)

                    errors_high_gfs   = []
                    errors_high_ecmwf = []
                    # Consensus/divergence split — days where GFS and ECMWF agree within 2°F
                    # vs days where they diverge by >3°F. Separate RMSE per bucket tells us
                    # which model to trust when they disagree.
                    errors_gfs_consensus   = []
                    errors_ecmwf_consensus = []
                    errors_gfs_diverge     = []
                    errors_ecmwf_diverge   = []

                    for ds, actual in actual_map.items():
                        gfs_v   = gfs_map.get(ds)
                        ecmwf_v = ecmwf_map.get(ds)
                        if gfs_v is not None:
                            errors_high_gfs.append(gfs_v - actual)
                        if ecmwf_v is not None:
                            errors_high_ecmwf.append(ecmwf_v - actual)
                        # Split by model agreement
                        if gfs_v is not None and ecmwf_v is not None:
                            spread = abs(gfs_v - ecmwf_v)
                            if spread <= 2.0:
                                errors_gfs_consensus.append(gfs_v - actual)
                                errors_ecmwf_consensus.append(ecmwf_v - actual)
                            elif spread >= 3.0:
                                errors_gfs_diverge.append(gfs_v - actual)
                                errors_ecmwf_diverge.append(ecmwf_v - actual)

                    if not any(actual_map.values()):
                        return {"city": city_key, "ok": False, "error": "No actuals"}

                    def stats(errs):
                        if not errs or len(errs) < 3: return None, None
                        n    = len(errs)
                        bias = round(sum(errs) / n, 2)
                        rmse = round(sqrt(sum(e**2 for e in errs) / n), 2)
                        return bias, rmse

                    errors_gfs   = []
                    errors_ecmwf = []

                    for ds, actual in actual_map.items():
                        if actual is None: continue
                        gfs_v   = gfs_map.get(ds)
                        ecmwf_v = ecmwf_map.get(ds)
                        if gfs_v   is not None: errors_gfs.append(gfs_v - actual)
                        if ecmwf_v is not None: errors_ecmwf.append(ecmwf_v - actual)

                    gfs_bias,   gfs_rmse   = stats(errors_gfs)
                    ecmwf_bias, ecmwf_rmse = stats(errors_ecmwf)

                    # Exclude ECMWF if it mirrors archive (same data source)
                    ecmwf_is_mirror = (ecmwf_rmse is not None and ecmwf_rmse < 0.3
                                       and len(errors_ecmwf) >= 5)
                    if ecmwf_is_mirror:
                        ecmwf_bias = 0.0
                        ecmwf_rmse = None

                    # Best model: lowest RMSE wins
                    if gfs_rmse and ecmwf_rmse:
                        best_model = "gfs" if gfs_rmse <= ecmwf_rmse else "ecmwf"
                    elif gfs_rmse:
                        best_model = "gfs"
                    elif ecmwf_rmse:
                        best_model = "ecmwf"
                    else:
                        best_model = "average"

                    rmse_vals = [v for v in [gfs_rmse, ecmwf_rmse] if v and v > 0.1]
                    σ_d1 = round(sum(rmse_vals) / len(rmse_vals), 2) if rmse_vals else cfg["σ_d1"]
                    σ_d0 = round(σ_d1 * 0.70, 2)

                    # ── Bias source selection ─────────────────────────────
                    # Prefer calibration_snapshots (real trade data, rolling 30d)
                    # over archive API (unreliable — model overwrites prior runs).
                    # Bias only applied when >= 30 settled rows in last 30 days.
                    BIAS_MIN_TRADES = 30
                    live_gfs_bias   = 0.0
                    live_ecmwf_bias = 0.0
                    live_n          = 0
                    bias_source     = "disabled"

                    conn_cal = get_db()
                    if conn_cal:
                        try:
                            with conn_cal.cursor() as cur_cal:
                                cur_cal.execute("""
                                    SELECT
                                        COUNT(*) AS n,
                                        AVG(mu - settled_temp) AS raw_bias
                                    FROM calibration_snapshots
                                    WHERE city = %s
                                      AND settled_temp IS NOT NULL
                                      AND scan_ts >= NOW() - INTERVAL '30 days'
                                """, (city_key,))
                                row = cur_cal.fetchone()
                                if row and row[0] >= BIAS_MIN_TRADES and row[1] is not None:
                                    live_n        = int(row[0])
                                    # mu is the raw (unbiased) model center.
                                    # raw_bias = avg(mu - actual): positive means model runs warm.
                                    # We store as gfs_bias/ecmwf_bias since both use same mu.
                                    live_gfs_bias   = round(float(row[1]), 2)
                                    live_ecmwf_bias = round(float(row[1]), 2)
                                    bias_source     = f"calibration_snapshots (n={live_n})"
                        except Exception:
                            pass
                        finally:
                            conn_cal.close()

                    _TEMP_BIAS_CACHE[city_key] = {
                        "gfs_bias":    live_gfs_bias,
                        "ecmwf_bias":  live_ecmwf_bias,
                        "gfs_rmse":    gfs_rmse,
                        "ecmwf_rmse":  ecmwf_rmse,
                        "best_model":  best_model,
                        "σ_d1":        σ_d1,
                        "σ_d0":        σ_d0,
                        "n_days":      len(errors_gfs),
                        "bias_n":      live_n,
                        "bias_source": bias_source,
                        "calibrated_at": _t.time(),
                    }

                    return {
                        "city":        city_key,
                        "ok":          True,
                        "n_days":      len(errors_gfs),
                        "gfs_bias":    live_gfs_bias,
                        "ecmwf_bias":  live_ecmwf_bias,
                        "gfs_rmse":    gfs_rmse,
                        "ecmwf_rmse":  ecmwf_rmse,
                        "best_model":  best_model,
                        "σ_d1":        σ_d1,       "σ_d0":       σ_d0,
                        "bias_n":      live_n,
                        "bias_source": bias_source,
                        "bias_active": live_n >= 30,
                    }

                except Exception as e:
                    return {"city": city_key, "ok": False, "error": str(e)}

            try:
                ex = _cf.ThreadPoolExecutor(max_workers=4)
                try:
                    futures = {ex.submit(calibrate_temp_city, ck): ck for ck in cities}
                    results = []
                    done, _ = _cf.wait(futures, timeout=300)
                    for f in done:
                        try:    results.append(f.result())
                        except Exception as e: results.append({"city": futures[f], "ok": False, "error": str(e)})
                finally:
                    ex.shutdown(wait=False)

                self.send_json({
                    "ok":      True,
                    "days_back": days_back,
                    "cities":  results,
                    "cache_size": len(_TEMP_BIAS_CACHE),
                    "note":    "Bias cache updated in-memory. Re-run after each deploy.",
                })
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)})

        elif path == "/temp/auto-settle":
            # Manually trigger auto-settlement (bypasses time-window check).
            # Useful on first deploy or if the scheduler missed a day.
            # GET /temp/auto-settle           → run for yesterday
            # GET /temp/auto-settle?date=2026-04-07  → run for specific date (override)
            qs       = parse_qs(urlparse(self.path).query)
            override = qs.get("date", [None])[0]
            try:
                if override:
                    # Run settlement for a specific past date
                    conn = get_db()
                    if not conn:
                        self.send_json({"ok": False, "error": "No DB"})
                    else:
                        results = []; settled = 0
                        with conn.cursor() as cur:
                            cur.execute("""
                                SELECT DISTINCT city, nws_station FROM temp_snapshots
                                WHERE target_date = %s AND settled_temp IS NULL
                            """, (override,))
                            cities_needed = cur.fetchall()

                        import concurrent.futures as _cf
                        def _settle_one(ck, st):
                            cli = fetch_nws_temp_cli(st)
                            return {"city": ck, "station": st, **cli}

                        ex = _cf.ThreadPoolExecutor(max_workers=8)
                        try:
                            futs = {ex.submit(_settle_one, ck, st): (ck, st)
                                    for ck, st in cities_needed}
                            done, _ = _cf.wait(futs, timeout=60)
                            for f in done:
                                try: results.append(f.result())
                                except Exception as e: results.append({"ok": False, "error": str(e)})
                        finally:
                            ex.shutdown(wait=False)

                        with conn.cursor() as cur:
                            for res in results:
                                if not res.get("ok"): continue
                                for mtype, actual in [("high", res.get("high")), ("low", res.get("low"))]:
                                    if actual is None: continue
                                    cur.execute("""
                                        UPDATE temp_snapshots
                                        SET settled_temp = %s,
                                            settled_correct = (
                                                (lo_temp IS NULL OR %s >= lo_temp) AND
                                                (hi_temp IS NULL OR %s <= hi_temp))
                                        WHERE city=%s AND target_date=%s
                                          AND market_type=%s AND settled_temp IS NULL
                                    """, (actual, actual, actual, res["city"], override, mtype))
                                    settled += cur.rowcount
                        conn.commit(); conn.close()
                        # Propagate the override-date settlements through to
                        # calibration_snapshots + paper_trades.
                        _paper_trade_settle()
                        self.send_json({"ok": True, "date": override,
                                        "settled": settled, "cities": results})
                else:
                    result = run_auto_settlement(force=True)
                    # Propagate after every manual trigger.
                    _paper_trade_settle()
                    self.send_json(result)
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)})

        elif path == "/temp/settle-log":
            # Return the in-memory settlement log (last 50 runs)
            with _SETTLE_LOCK:
                log = list(_SETTLE_LOG)
            self.send_json({
                "ok":          True,
                "log":         log,
                "count":       len(log),
                "scheduler_alive": _SETTLE_THREAD is not None and _SETTLE_THREAD.is_alive(),
                "last_settle_run": _LAST_SETTLE_RUN,
            })

        elif path == "/paper-trades":
            # Paper trading results — all A-grade signals logged by background scanner
            conn = get_db()
            if not conn:
                self.send_json({"ok": False, "error": "No DB"})
            else:
                try:
                    qs     = parse_qs(urlparse(self.path).query)
                    limit  = int(qs.get("limit", [200])[0])
                    city   = qs.get("city", [None])[0]
                    with conn.cursor() as cur:
                        where = "WHERE city = %s" if city else ""
                        args  = [city] if city else []
                        cur.execute(f"""
                            SELECT
                                city, target_date, ticker, bracket_label,
                                grade, model_prob, yes_ask, mu, sigma,
                                net_gap_c, kelly_size, hours_to_cutoff,
                                settled_temp, settled_correct, scan_ts
                            FROM paper_trades
                            {where}
                            ORDER BY scan_ts DESC
                            LIMIT %s
                        """, args + [limit])
                        cols = [d[0] for d in cur.description]
                        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
                        # Summary stats
                        settled = [r for r in rows if r["settled_correct"] is not None]
                        wins    = [r for r in settled if r["settled_correct"]]
                        cur.execute(f"""
                            SELECT
                                city,
                                COUNT(*) AS n,
                                COUNT(settled_correct) AS settled,
                                SUM(CASE WHEN settled_correct THEN 1 ELSE 0 END) AS wins,
                                ROUND(AVG(CASE WHEN settled_correct IS NOT NULL
                                    THEN (CASE WHEN settled_correct THEN 1.0 ELSE 0.0 END)
                                END)::numeric * 100, 1) AS win_pct,
                                ROUND(AVG(model_prob::numeric) * 100, 1) AS avg_prob,
                                ROUND(AVG(yes_ask::numeric) * 100, 1) AS avg_ask_c
                            FROM paper_trades
                            {where}
                            GROUP BY city
                            ORDER BY settled DESC
                        """, args)
                        city_cols = [d[0] for d in cur.description]
                        city_rows = [dict(zip(city_cols, r)) for r in cur.fetchall()]
                    conn.close()
                    # Serialize dates
                    for r in rows:
                        for k, v in r.items():
                            if hasattr(v, 'isoformat'): r[k] = v.isoformat()
                    self.send_json({
                        "ok": True,
                        "trades": rows,
                        "total": len(rows),
                        "settled": len(settled),
                        "wins": len(wins),
                        "win_rate": round(len(wins)/len(settled)*100, 1) if settled else None,
                        "by_city": city_rows,
                    })
                except Exception as e:
                    self.send_json({"ok": False, "error": str(e)})

        elif path == "/temp/calibration":
            # Probability calibration curve from temp_snapshots.
            conn = get_db()
            if not conn:
                self.send_json({"ok": False, "error": "No DB",
                                "psycopg2_available": PSYCOPG2_AVAILABLE,
                                "has_db_url": bool(DATABASE_URL)})
            else:
                try:
                    qs   = parse_qs(urlparse(self.path).query)
                    city = qs.get("city", [None])[0]
                    with conn.cursor() as cur:
                        base = """
                            SELECT
                                ROUND(model_prob::numeric * 10) / 10.0 AS prob_bucket,
                                COUNT(*)                                AS n,
                                ROUND(AVG(CASE WHEN settled_correct THEN 1.0 ELSE 0.0 END)::numeric, 3)
                                                                        AS actual_win_rate,
                                ROUND(AVG(model_prob::numeric), 3)      AS avg_prob,
                                ROUND(AVG(yes_ask::numeric), 3)         AS avg_market_price,
                                ROUND(AVG(edge_ratio::numeric), 3)      AS avg_edge_ratio,
                                STRING_AGG(DISTINCT grade, ',' ORDER BY grade) AS grades
                            FROM temp_snapshots
                            WHERE settled_correct IS NOT NULL
                              AND model_prob IS NOT NULL
                        """
                        if city:
                            cur.execute(base + " AND city=%s GROUP BY prob_bucket ORDER BY prob_bucket", (city,))
                        else:
                            cur.execute(base + " GROUP BY prob_bucket ORDER BY prob_bucket")
                        cols = [d[0] for d in cur.description]
                        rows = [dict(zip(cols, r)) for r in cur.fetchall()]

                        # Also get overall stats
                        cur.execute("""
                            SELECT
                                COUNT(*) AS total,
                                SUM(CASE WHEN settled_correct THEN 1 ELSE 0 END) AS wins,
                                ROUND(AVG(CASE WHEN settled_correct THEN 1.0 ELSE 0.0 END)::numeric, 3) AS overall_win_rate,
                                ROUND(AVG(model_prob::numeric), 3) AS avg_model_prob,
                                -- Brier score: lower = better calibrated
                                ROUND(AVG(POWER(model_prob::numeric - CASE WHEN settled_correct THEN 1.0 ELSE 0.0 END, 2))::numeric, 4) AS brier_score
                            FROM temp_snapshots
                            WHERE settled_correct IS NOT NULL AND model_prob IS NOT NULL
                        """ + (" AND city=%s" if city else ""), ([city] if city else []))
                        summary = dict(zip([d[0] for d in cur.description], cur.fetchone() or []))

                    conn.close()
                    for r in rows:
                        for k, v in r.items():
                            if hasattr(v, '__float__'): r[k] = float(v)
                    for k, v in summary.items():
                        if hasattr(v, '__float__'): summary[k] = float(v)

                    self.send_json({
                        "ok":      True,
                        "city":    city or "all",
                        "summary": summary,
                        "buckets": rows,
                        "note":    "prob_bucket is model probability rounded to nearest 0.1. "
                                   "A well-calibrated model has actual_win_rate ≈ prob_bucket."
                    })
                except Exception as e:
                    self.send_json({"ok": False, "error": str(e)})

        elif path == "/temp/history":
            # Return weather trade history by joining Kalshi settlements
            # with temp_snapshots to get model predictions + actual temps.
            # The frontend merges this with /pnl for P&L data.
            conn = get_db()
            if not conn:
                self.send_json({"ok": False, "error": "No DB"})
            else:
                try:
                    qs    = parse_qs(urlparse(self.path).query)
                    limit = int(qs.get("limit", [200])[0])
                    with conn.cursor() as cur:
                        # Get best snapshot per ticker (most recent scan)
                        cur.execute("""
                            SELECT DISTINCT ON (ticker)
                                ticker, city, nws_station, target_date,
                                bracket_label, lo_temp, hi_temp,
                                gfs_forecast, ecmwf_forecast, best_forecast,
                                sigma, model_prob, yes_ask,
                                gap_c, net_gap_c, edge_ratio, kelly_frac,
                                grade, liq_grade,
                                settled_temp, settled_correct, scan_ts
                            FROM temp_snapshots
                            WHERE grade IN ('A','B','C')
                            ORDER BY ticker, scan_ts DESC
                            LIMIT %s
                        """, (limit,))
                        cols = [d[0] for d in cur.description]
                        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
                    conn.close()
                    # Serialize dates/decimals
                    for r in rows:
                        for k, v in r.items():
                            if hasattr(v, 'isoformat'): r[k] = v.isoformat()
                            elif hasattr(v, '__float__'): r[k] = float(v)
                    self.send_json({"ok": True, "snapshots": rows, "count": len(rows)})
                except Exception as e:
                    self.send_json({"ok": False, "error": str(e)})

        elif path == "/temp/status":
            # Quick system health for the temp pipeline
            import pytz
            from datetime import datetime as dt_cls, timedelta
            et_tz   = pytz.timezone("America/New_York")
            now_et  = dt_cls.utcnow().replace(tzinfo=pytz.utc).astimezone(et_tz)
            yesterday = (now_et - timedelta(days=1)).strftime("%Y-%m-%d")

            # Count unsettled snapshots for yesterday
            unsettled = 0
            total_snaps = 0
            conn = get_db()
            if conn:
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT
                                COUNT(*) FILTER (WHERE target_date = %s) AS yesterday_total,
                                COUNT(*) FILTER (WHERE target_date = %s AND settled_temp IS NULL) AS yesterday_unsettled,
                                COUNT(*) FILTER (WHERE settled_temp IS NOT NULL) AS all_settled,
                                COUNT(*) AS all_total
                            FROM temp_snapshots
                        """, (yesterday, yesterday))
                        row = cur.fetchone()
                        if row:
                            yesterday_total, unsettled, all_settled, total_snaps = row
                finally:
                    conn.close()

            self.send_json({
                "ok":               True,
                "scheduler_alive":  _SETTLE_THREAD is not None and _SETTLE_THREAD.is_alive(),
                "calibrated_cities": len(_TEMP_BIAS_CACHE),
                "total_cities":     len(TEMP_CITIES),
                "bias_cache":       {k: {kk: round(vv,3) if isinstance(vv,float) else vv
                                         for kk,vv in v.items() if kk != "calibrated_at"}
                                     for k,v in _TEMP_BIAS_CACHE.items()},
                "yesterday":        yesterday,
                "yesterday_snapshots": yesterday_total if conn else "no DB",
                "yesterday_unsettled": unsettled,
                "total_snapshots":  total_snaps,
                "last_calibrate_date": _LAST_CALIBRATE_DATE,
                "settle_poll_interval_min": SETTLE_POLL_INTERVAL // 60,
                "settle_window_et": f"{SETTLE_WINDOW_START}AM\u2013{SETTLE_WINDOW_END-12}PM",
                "db_connected":     bool(DATABASE_URL and PSYCOPG2_AVAILABLE),
                "db_action_needed": not bool(DATABASE_URL),
                "db_setup_note":    (
                    "Add Postgres: Railway project \u2192 + New \u2192 Database \u2192 PostgreSQL. "
                    "Railway auto-injects DATABASE_URL. Redeploy after adding."
                ) if not DATABASE_URL else None,
                "calibration_errors": _CALIBRATE_ERRORS or None,
                "calibration_debug_url": "/temp/calibrate-test?city=chicago&days=7",
            })

        elif path == "/temp/backtest":
            # Return the temp_backtest aggregated view
            conn = get_db()
            if not conn:
                self.send_json({"ok": False, "error": "No DB"})
            else:
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT * FROM temp_backtest")
                        cols    = [desc[0] for desc in cur.description]
                        rows    = [dict(zip(cols, row)) for row in cur.fetchall()]
                    conn.close()
                    for r in rows:
                        for k, v in r.items():
                            if hasattr(v, "__float__"): r[k] = float(v)
                    self.send_json({"ok": True, "rows": rows, "count": len(rows)})
                except Exception as e:
                    self.send_json({"ok": False, "error": str(e)})

        elif path == "/temp/snapshots":
            # Return raw temp_snapshots for a city+date range, for manual analysis
            qs     = parse_qs(urlparse(self.path).query)
            city   = qs.get("city",  [None])[0]
            limit  = int(qs.get("limit", ["200"])[0])
            conn   = get_db()
            if not conn:
                self.send_json({"ok": False, "error": "No DB"})
            else:
                try:
                    with conn.cursor() as cur:
                        if city:
                            cur.execute("""
                                SELECT * FROM temp_snapshots WHERE city=%s
                                ORDER BY target_date DESC, scan_ts DESC LIMIT %s
                            """, (city, limit))
                        else:
                            cur.execute("""
                                SELECT * FROM temp_snapshots
                                ORDER BY target_date DESC, scan_ts DESC LIMIT %s
                            """, (limit,))
                        cols = [desc[0] for desc in cur.description]
                        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
                    conn.close()
                    for r in rows:
                        for k, v in r.items():
                            if hasattr(v, "isoformat"): r[k] = v.isoformat()
                            elif hasattr(v, "__float__"): r[k] = float(v)
                    self.send_json({"ok": True, "rows": rows, "count": len(rows)})
                except Exception as e:
                    self.send_json({"ok": False, "error": str(e)})

        elif path == "/portfolio":
            # Fetch live Kalshi balance and positions
            if not KALSHI_KEY_ID:
                self.send_json({"ok": False, "error": "No Kalshi API key"})
            else:
                try:
                    # Balance
                    bal_r = requests.get(
                        f"{KALSHI_BASE}/portfolio/balance",
                        headers=kalshi_auth_headers("GET", "/trade-api/v2/portfolio/balance"),
                        timeout=10
                    )
                    bal_r.raise_for_status()
                    bal = bal_r.json()

                    # Positions
                    pos_r = requests.get(
                        f"{KALSHI_BASE}/portfolio/positions",
                        headers=kalshi_auth_headers("GET", "/trade-api/v2/portfolio/positions"),
                        timeout=10,
                        params={"limit": 100, "count_filter": "position"}
                    )
                    pos_r.raise_for_status()
                    pos_data = pos_r.json()

                    positions = []
                    for p in pos_data.get("market_positions", []):
                        qty     = float(p.get("position_fp", 0) or 0)
                        cost    = float(p.get("total_traded_dollars", 0) or 0)
                        r_pnl   = float(p.get("realized_pnl_dollars", 0) or 0)
                        mkt_exp = float(p.get("market_exposure_dollars", 0) or 0)
                        fees    = float(p.get("fees_paid_dollars", 0) or 0)
                        ticker  = p.get("ticker", "")
                        # avg price in cents: cost (dollars) / contracts * 100
                        avg_c   = round((cost / max(abs(qty), 1)) * 100, 1) if qty != 0 else 0
                        # unrealized: market_exposure is current value, cost is what was paid
                        unrealized = round(mkt_exp - cost, 2)
                        positions.append({
                            "ticker":          ticker,
                            "market_title":    p.get("market_title", "") or ticker,
                            "yes_contracts":   qty,
                            "avg_yes_price_c": avg_c,
                            "avg_yes_price":   round(cost / max(abs(qty), 1), 4) if qty != 0 else 0,
                            "market_value":    round(mkt_exp, 2),
                            "realized_pnl":    round(r_pnl, 2),
                            "unrealized_pnl":  unrealized,
                            "total_cost":      round(cost, 2),
                            "payout_if_right": round(abs(qty) * 1.0, 2),
                            "fees":            round(fees, 2),
                        })

                    self.send_json({
                        "ok": True,
                        "balance":    float(bal.get("balance", 0) or 0) / 100,
                        "portfolio_value": float(bal.get("portfolio_value", 0) or 0) / 100,
                        "positions":  positions,
                        "count":      len(positions),
                    })
                except Exception as e:
                    self.send_json({"ok": False, "error": str(e)})

        elif path == "/pnl":
            # Fetch trade history for P&L chart
            # Returns settled trades for history tab
            if not KALSHI_KEY_ID:
                self.send_json({"ok": False, "error": "No Kalshi API key"})
            else:
                try:
                    qs     = parse_qs(urlparse(self.path).query)
                    limit  = int(qs.get("limit", [200])[0])
                    r = requests.get(
                        f"{KALSHI_BASE}/portfolio/settlements",
                        headers=kalshi_auth_headers("GET", "/trade-api/v2/portfolio/settlements"),
                        timeout=10,
                        params={"limit": limit}
                    )
                    r.raise_for_status()
                    data = r.json()

                    trades = []
                    cumulative = 0.0
                    for s in reversed(data.get("settlements", [])):
                        revenue    = float(s.get("revenue", 0) or 0) / 100
                        yes_cost   = float(s.get("yes_total_cost_dollars", 0) or 0)
                        no_cost    = float(s.get("no_total_cost_dollars",  0) or 0)
                        fee_cost   = float(s.get("fee_cost", 0) or 0)
                        yes_count  = float(s.get("yes_count_fp", 0) or 0)
                        no_count   = float(s.get("no_count_fp",  0) or 0)
                        ticker     = s.get("ticker", "")

                        # Determine side, cost, avg price
                        if yes_count > 0:
                            side      = "YES"
                            cost      = yes_cost
                            contracts = yes_count
                            avg_c     = round((yes_cost / max(yes_count, 1)) * 100, 1)
                        elif no_count > 0:
                            side      = "NO"
                            cost      = no_cost
                            contracts = no_count
                            avg_c     = round((no_cost / max(no_count, 1)) * 100, 1)
                        else:
                            side      = "?"
                            cost      = 0.0
                            contracts = 0
                            avg_c     = 0.0

                        # Kalshi revenue = winnings received (0 for a loss).
                        # True P&L = revenue - cost paid (including fees).
                        # fee_cost is already in dollars like yes_total_cost_dollars.
                        total_cost = cost + fee_cost
                        pnl     = round(revenue - total_cost, 2)
                        outcome = "win" if pnl > 0 else "loss"
                        cumulative += pnl

                        trades.append({
                            "date":         s.get("settled_time", "")[:10],
                            "ticker":       ticker,
                            "market_title": s.get("market_title", "") or ticker,
                            "side":         side,
                            "contracts":    round(contracts, 0),
                            "avg_entry_c":  avg_c,
                            "cost":         round(total_cost, 2),
                            "revenue":      round(revenue, 2),
                            "pnl":          round(pnl, 2),
                            "cumulative":   round(cumulative, 2),
                            "outcome":      outcome,
                        })

                    # Return newest first
                    trades.reverse()
                    self.send_json({
                        "ok":        True,
                        "trades":    trades,
                        "total_pnl": round(cumulative, 2),
                        "count":     len(trades),
                    })
                except Exception as e:
                    self.send_json({"ok": False, "error": str(e)})

        elif path == "/" or path == "/dashboard":
            try:
                with open("dashboard.html", "rb") as f:
                    content_html = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(content_html)
            except Exception:
                self.send_response(404)
                self.end_headers()

        elif path == "/admin/query":
            # Secured SQL query endpoint for external analysis.
            # Accepts POST with JSON body: {"sql": "SELECT ...", "token": "..."}
            # Read-only — SELECT only, no INSERT/UPDATE/DELETE/DROP.
            # Token must match QUERY_TOKEN env var (set in Railway Variables).
            import os as _os
            try:
                length = int(self.headers.get("Content-Length", 0))
                body   = json.loads(self.rfile.read(length).decode()) if length else {}
                token  = body.get("token", "")
                sql    = body.get("sql", "").strip()
                expected = _os.environ.get("QUERY_TOKEN", "")
                if not expected:
                    self.send_json({"ok": False, "error": "QUERY_TOKEN not set in environment"})
                elif token != expected:
                    self.send_json({"ok": False, "error": "Invalid token"})
                elif not sql.upper().startswith("SELECT"):
                    self.send_json({"ok": False, "error": "Only SELECT queries allowed"})
                else:
                    conn_q = get_db()
                    if not conn_q:
                        self.send_json({"ok": False, "error": "No DB"})
                    else:
                        with conn_q.cursor() as cur:
                            cur.execute(sql)
                            cols = [d[0] for d in cur.description]
                            rows = cur.fetchall()
                            # Convert to list of dicts, handle non-serializable types
                            import decimal as _dec
                            import datetime as _dt
                            def _ser(v):
                                if isinstance(v, _dec.Decimal): return float(v)
                                if isinstance(v, (_dt.date, _dt.datetime)): return str(v)
                                return v
                            data = [dict(zip(cols, [_ser(c) for c in r])) for r in rows]
                        conn_q.close()
                        self.send_json({"ok": True, "rows": data, "count": len(data)})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)})

        elif path == "/admin/setup-db":
            # Run each table creation independently so one failure doesn't block others
            results = {}
            conn = get_db()
            if not conn:
                self.send_json({"ok": False, "error": "No DB"})
            else:
                tables = [
                    ("forecast_snapshots", "CREATE TABLE IF NOT EXISTS forecast_snapshots (id SERIAL PRIMARY KEY, city TEXT, month TEXT, snapshot_date DATE, days_remaining INTEGER, true_mtd NUMERIC(6,2), wu_remaining NUMERIC(6,2), projected_eom NUMERIC(6,2), confidence TEXT, wu_days_used INTEGER, sigma NUMERIC(6,3), created_at TIMESTAMPTZ DEFAULT NOW())"),
                    ("intraday_snapshots", "CREATE TABLE IF NOT EXISTS intraday_snapshots (id SERIAL PRIMARY KEY, city TEXT, month TEXT, snapshot_ts TIMESTAMPTZ DEFAULT NOW(), true_mtd NUMERIC(6,2), wu_remaining NUMERIC(6,2), projected_eom NUMERIC(6,2), confidence TEXT)"),
                    ("month_settlements", "CREATE TABLE IF NOT EXISTS month_settlements (id SERIAL PRIMARY KEY, city TEXT NOT NULL, month TEXT NOT NULL, actual_total NUMERIC(6,2), settled_date DATE, notes TEXT, UNIQUE(city, month))"),
                    ("temp_snapshots", """CREATE TABLE IF NOT EXISTS temp_snapshots (
                        id SERIAL PRIMARY KEY, city TEXT NOT NULL, nws_station TEXT NOT NULL,
                        target_date DATE NOT NULL, horizon TEXT NOT NULL DEFAULT 'd1',
                        scan_ts TIMESTAMPTZ DEFAULT NOW(), market_type TEXT NOT NULL,
                        ticker TEXT NOT NULL, bracket_label TEXT,
                        lo_temp NUMERIC(5,1), hi_temp NUMERIC(5,1),
                        gfs_forecast NUMERIC(5,1), ecmwf_forecast NUMERIC(5,1),
                        best_forecast NUMERIC(5,1), sigma NUMERIC(5,2),
                        spread_models NUMERIC(5,2), model_prob NUMERIC(5,3),
                        yes_ask NUMERIC(5,3), gap_c INTEGER, net_gap_c INTEGER,
                        edge_ratio NUMERIC(6,3), kelly_frac NUMERIC(5,3),
                        grade TEXT, liq_grade TEXT, open_interest INTEGER, volume_24h INTEGER,
                        settled_temp NUMERIC(5,1), settled_correct BOOLEAN,
                        hours_to_cutoff NUMERIC(5,1))"""),
                    ("model_forecasts", "CREATE TABLE IF NOT EXISTS model_forecasts (id SERIAL PRIMARY KEY, city TEXT NOT NULL, nws_station TEXT NOT NULL DEFAULT '', target_date DATE NOT NULL, actual_high NUMERIC(5,1), gfs_high NUMERIC(5,1), ecmwf_high NUMERIC(5,1), nbm_high NUMERIC(5,1), graphcast_high NUMERIC(5,1), gem_high NUMERIC(5,1), icon_high NUMERIC(5,1), spread_gfs_ecmwf NUMERIC(5,2), UNIQUE(city, target_date))"),
                    ("auto_trader_config", "CREATE TABLE IF NOT EXISTS auto_trader_config (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TIMESTAMPTZ DEFAULT NOW())"),
                    ("auto_trader_log", "CREATE TABLE IF NOT EXISTS auto_trader_log (id BIGSERIAL PRIMARY KEY, ts TIMESTAMPTZ DEFAULT NOW(), level TEXT NOT NULL, msg TEXT NOT NULL, ticker TEXT, city TEXT, extra JSONB DEFAULT '{}')"),
                    ("paper_trades", "CREATE TABLE IF NOT EXISTS paper_trades (id BIGSERIAL PRIMARY KEY, scan_ts TIMESTAMPTZ DEFAULT NOW(), city TEXT NOT NULL, nws_station TEXT, target_date DATE NOT NULL, horizon TEXT, ticker TEXT NOT NULL, bracket_label TEXT, lo_temp NUMERIC(5,1), hi_temp NUMERIC(5,1), grade TEXT, model_prob NUMERIC(6,4), yes_ask NUMERIC(6,4), mu NUMERIC(6,2), sigma NUMERIC(6,3), net_gap_c INTEGER, kelly_size NUMERIC(8,2), hours_to_cutoff NUMERIC(5,1), mkt_rank_conf TEXT, gfs_high NUMERIC(5,1), ecmwf_high NUMERIC(5,1), model_spread NUMERIC(5,2), edge_ratio NUMERIC(8,3), gap_c INTEGER, spread_c INTEGER, kelly_frac NUMERIC(6,4), yes_bid NUMERIC(6,4), liq_grade TEXT, open_interest INTEGER, volume_24h INTEGER, fillable_a NUMERIC(8,2), is_tail_bet BOOLEAN, any_model_inside BOOLEAN, spread_exceeds_bracket BOOLEAN, book_limited BOOLEAN, settled_temp NUMERIC(5,1), settled_correct BOOLEAN, settled_ts TIMESTAMPTZ)"),
                    ("calibration_snapshots", "CREATE TABLE IF NOT EXISTS calibration_snapshots (id BIGSERIAL PRIMARY KEY, scan_ts TIMESTAMPTZ DEFAULT NOW(), city TEXT NOT NULL, nws_station TEXT, target_date DATE NOT NULL, horizon TEXT, ticker TEXT NOT NULL, bracket_label TEXT, lo_temp NUMERIC(5,1), hi_temp NUMERIC(5,1), grade TEXT, model_prob NUMERIC(6,4), yes_ask NUMERIC(6,4), mu NUMERIC(6,2), sigma NUMERIC(6,3), net_gap_c INTEGER, kelly_size NUMERIC(8,2), hours_to_cutoff NUMERIC(5,1), mkt_rank_conf TEXT, gfs_high NUMERIC(5,1), ecmwf_high NUMERIC(5,1), model_spread NUMERIC(5,2), edge_ratio NUMERIC(8,3), gap_c INTEGER, spread_c INTEGER, kelly_frac NUMERIC(6,4), yes_bid NUMERIC(6,4), liq_grade TEXT, open_interest INTEGER, volume_24h INTEGER, fillable_a NUMERIC(8,2), is_tail_bet BOOLEAN, any_model_inside BOOLEAN, spread_exceeds_bracket BOOLEAN, book_limited BOOLEAN, settled_temp NUMERIC(5,1), settled_correct BOOLEAN, settled_ts TIMESTAMPTZ)"),
                ]
                for name, sql in tables:
                    try:
                        with conn.cursor() as cur:
                            cur.execute(sql)
                        conn.commit()
                        results[name] = "ok"
                    except Exception as e:
                        conn.rollback()
                        results[name] = str(e)
                conn.close()
                all_ok = all(v == "ok" for v in results.values())
                # Run migrations: deduplicate paper_trades and create unique index
                try:
                    conn2 = get_db()
                    if conn2:
                        with conn2.cursor() as cur:
                            cur.execute("""
                                DELETE FROM paper_trades
                                WHERE id NOT IN (
                                    SELECT DISTINCT ON (ticker, target_date) id
                                    FROM paper_trades
                                    ORDER BY ticker, target_date, scan_ts DESC
                                )
                            """)
                            cur.execute("""
                                CREATE UNIQUE INDEX IF NOT EXISTS paper_trades_ticker_date_idx
                                ON paper_trades (ticker, target_date)
                            """)
                        conn2.commit()
                        conn2.close()
                        results["paper_trades_dedup"] = "ok"
                except Exception as e:
                    results["paper_trades_dedup"] = str(e)
                # Create unique index on calibration_snapshots
                try:
                    conn3 = get_db()
                    if conn3:
                        with conn3.cursor() as cur:
                            cur.execute("""
                                CREATE UNIQUE INDEX IF NOT EXISTS calibration_snapshots_ticker_date_idx
                                ON calibration_snapshots (ticker, target_date)
                            """)
                        conn3.commit()
                        conn3.close()
                        results["calibration_snapshots_idx"] = "ok"
                except Exception as e:
                    results["calibration_snapshots_idx"] = str(e)
                # Create price_history table if missing
                try:
                    conn_ph = get_db()
                    if conn_ph:
                        with conn_ph.cursor() as cur:
                            cur.execute("""
                                CREATE TABLE IF NOT EXISTS price_history (
                                    id BIGSERIAL PRIMARY KEY,
                                    scan_ts TIMESTAMPTZ DEFAULT NOW(),
                                    city TEXT NOT NULL, target_date DATE NOT NULL,
                                    horizon TEXT, ticker TEXT NOT NULL,
                                    bracket_label TEXT, lo_temp NUMERIC(5,1), hi_temp NUMERIC(5,1),
                                    yes_ask NUMERIC(6,4), yes_bid NUMERIC(6,4),
                                    volume_24h INTEGER, open_interest INTEGER,
                                    mu NUMERIC(6,2), gfs_high NUMERIC(5,1),
                                    ecmwf_high NUMERIC(5,1), sigma NUMERIC(6,3),
                                    model_prob NUMERIC(6,4), net_gap_c INTEGER, grade TEXT
                                )
                            """)
                            cur.execute("""
                                CREATE INDEX IF NOT EXISTS price_history_ticker_ts_idx
                                ON price_history (ticker, scan_ts DESC)
                            """)
                            cur.execute("""
                                CREATE INDEX IF NOT EXISTS price_history_city_date_idx
                                ON price_history (city, target_date, scan_ts DESC)
                            """)
                        conn_ph.commit()
                        conn_ph.close()
                        results["price_history"] = "ok"
                except Exception as e:
                    results["price_history"] = str(e)

                # Migration: add all new columns to existing tables if missing
                _new_columns = [
                    ("mkt_rank_conf",          "TEXT"),
                    ("gfs_high",               "NUMERIC(5,1)"),
                    ("ecmwf_high",             "NUMERIC(5,1)"),
                    ("model_spread",           "NUMERIC(5,2)"),
                    ("edge_ratio",             "NUMERIC(8,3)"),
                    ("gap_c",                  "INTEGER"),
                    ("spread_c",               "INTEGER"),
                    ("kelly_frac",             "NUMERIC(6,4)"),
                    ("yes_bid",                "NUMERIC(6,4)"),
                    ("liq_grade",              "TEXT"),
                    ("open_interest",          "INTEGER"),
                    ("volume_24h",             "INTEGER"),
                    ("fillable_a",             "NUMERIC(8,2)"),
                    ("is_tail_bet",            "BOOLEAN"),
                    ("any_model_inside",       "BOOLEAN"),
                    ("spread_exceeds_bracket", "BOOLEAN"),
                    ("book_limited",           "BOOLEAN"),
                ]
                try:
                    conn4 = get_db()
                    if conn4:
                        with conn4.cursor() as cur:
                            for tbl in ("paper_trades", "calibration_snapshots"):
                                for col, dtype in _new_columns:
                                    cur.execute(f"""
                                        ALTER TABLE {tbl}
                                        ADD COLUMN IF NOT EXISTS {col} {dtype}
                                    """)
                        conn4.commit()
                        conn4.close()
                        results["schema_migration"] = "ok"
                except Exception as e:
                    results["schema_migration"] = str(e)

                # ── Tail bracket migration (one-time, idempotent) ─────────
                # Bug: parser previously set hi_temp = X for "<X°F" brackets,
                # but the actual winning condition is NWS report ≤ X-1
                # (Kalshi UI: "<48°F" market = "47° or below"). Same bug for
                # ">X°F" → lo_temp = X but should be X+1.
                # Migration corrects existing rows where one bound is NULL
                # AND the non-NULL bound matches the threshold in the label
                # (the pre-fix signature). Idempotent: after running, the
                # match condition no longer holds.
                migration_results = {}
                for tbl in ("temp_snapshots", "paper_trades", "calibration_snapshots"):
                    try:
                        conn5 = get_db()
                        if not conn5:
                            migration_results[tbl] = "no DB"
                            continue
                        with conn5.cursor() as cur:
                            # Fix "<X°F" tail: hi_temp = X → hi_temp = X-1
                            cur.execute(f"""
                                UPDATE {tbl}
                                SET hi_temp = hi_temp - 1
                                WHERE lo_temp IS NULL
                                  AND hi_temp IS NOT NULL
                                  AND bracket_label ~ '^<[0-9]+\u00b0F$'
                                  AND hi_temp = SUBSTRING(bracket_label FROM '^<([0-9]+)\u00b0F$')::numeric
                            """)
                            below_fixed = cur.rowcount
                            # Fix ">X°F" tail: lo_temp = X → lo_temp = X+1
                            cur.execute(f"""
                                UPDATE {tbl}
                                SET lo_temp = lo_temp + 1
                                WHERE hi_temp IS NULL
                                  AND lo_temp IS NOT NULL
                                  AND bracket_label ~ '^>[0-9]+\u00b0F$'
                                  AND lo_temp = SUBSTRING(bracket_label FROM '^>([0-9]+)\u00b0F$')::numeric
                            """)
                            above_fixed = cur.rowcount
                        conn5.commit()
                        conn5.close()
                        migration_results[tbl] = {
                            "below_fixed": below_fixed,
                            "above_fixed": above_fixed,
                        }
                    except Exception as e:
                        migration_results[tbl] = f"error: {e}"
                results["tail_bracket_migration"] = migration_results

                self.send_json({"ok": all_ok, "tables": results})

        elif path == "/debug/cal-log":
            # Full settlement+propagation diagnostic. Surfaces:
            #  - calibration_snapshots state (total / settled / per city/date)
            #  - temp_snapshots state (total / settled / per city/date)
            #  - propagation gap (settled in temp_snapshots but NOT in cal_snap)
            #  - last 20 _PROP_LOG entries (errors, rowcounts, eligible pairs)
            try:
                conn = get_db()
                if not conn:
                    self.send_json({"ok": False, "error": "No DB"})
                else:
                    out = {"ok": True}
                    with conn.cursor() as cur:
                        # ── Table existence ────────────────────────────────
                        cur.execute("""
                            SELECT table_name FROM information_schema.tables
                            WHERE table_name IN ('calibration_snapshots',
                                                 'temp_snapshots',
                                                 'paper_trades')
                        """)
                        tables = {r[0] for r in cur.fetchall()}
                        out["tables_exist"] = sorted(tables)

                        # ── calibration_snapshots ──────────────────────────
                        if "calibration_snapshots" in tables:
                            cur.execute("""
                                SELECT COUNT(*),
                                       COUNT(settled_temp),
                                       COUNT(*) FILTER
                                       (WHERE scan_ts >= NOW() - INTERVAL '30 days')
                                FROM calibration_snapshots
                            """)
                            t, s, l30 = cur.fetchone()
                            out["calibration_snapshots"] = {
                                "total": t, "settled": s, "last_30d": l30,
                                "settled_pct": round(100.0 * s / t, 1) if t else 0.0,
                            }
                            cur.execute("""
                                SELECT grade, COUNT(*)
                                FROM calibration_snapshots
                                GROUP BY grade
                            """)
                            out["calibration_snapshots"]["by_grade"] = {
                                (r[0] or "null"): r[1] for r in cur.fetchall()
                            }

                        # ── temp_snapshots ─────────────────────────────────
                        if "temp_snapshots" in tables:
                            cur.execute("""
                                SELECT COUNT(*), COUNT(settled_temp)
                                FROM temp_snapshots
                            """)
                            t, s = cur.fetchone()
                            out["temp_snapshots"] = {"total": t, "settled": s}
                            cur.execute("""
                                SELECT city, target_date,
                                       COUNT(*) AS rows,
                                       COUNT(settled_temp) AS settled
                                FROM temp_snapshots
                                WHERE settled_temp IS NOT NULL
                                GROUP BY city, target_date
                                ORDER BY target_date DESC, city
                                LIMIT 30
                            """)
                            out["temp_snapshots"]["settled_by_city_date"] = [
                                {"city": r[0], "target_date": str(r[1]),
                                 "rows": r[2], "settled": r[3]}
                                for r in cur.fetchall()
                            ]

                        # ── Propagation gap (the diagnostic gold) ──────────
                        if {"temp_snapshots", "calibration_snapshots"} <= tables:
                            cur.execute("""
                                SELECT t.city, t.target_date,
                                       t.temp_settled,
                                       COALESCE(c.cal_total, 0) AS cal_total,
                                       COALESCE(c.cal_settled, 0) AS cal_settled
                                FROM (
                                    SELECT city, target_date,
                                           COUNT(*) AS temp_settled
                                    FROM temp_snapshots
                                    WHERE settled_temp IS NOT NULL
                                    GROUP BY city, target_date
                                ) t
                                LEFT JOIN (
                                    SELECT city, target_date,
                                           COUNT(*) AS cal_total,
                                           COUNT(settled_temp) AS cal_settled
                                    FROM calibration_snapshots
                                    GROUP BY city, target_date
                                ) c USING (city, target_date)
                                ORDER BY t.target_date DESC, t.city
                                LIMIT 50
                            """)
                            out["propagation_gap"] = [
                                {"city": r[0], "target_date": str(r[1]),
                                 "temp_settled": r[2], "cal_total": r[3],
                                 "cal_settled": r[4],
                                 "gap": r[3] - r[4]}  # how many cal rows still NULL
                                for r in cur.fetchall()
                            ]

                    conn.close()

                    # ── In-memory propagation log ──────────────────────────
                    with _PROP_LOCK:
                        out["recent_prop_runs"] = list(_PROP_LOG[:20])
                    with _SETTLE_LOCK:
                        out["recent_settle_runs"] = list(_SETTLE_LOG[:5])
                    with _SCAN_ERR_LOCK:
                        out["scan_errors"] = list(_SCAN_ERR_LOG[:20])

                    self.send_json(out)
            except Exception as e:
                import traceback
                self.send_json({"ok": False, "error": str(e),
                                "traceback": traceback.format_exc()})

        elif path == "/health":
            self.send_json({"ok": True, "message": "Server running"})

        else:
            self.send_response(404)
            self.end_headers()


_SCAN_THREAD        = None
_SCAN_INTERVAL_SECS = 4 * 3600   # scan every 4 hours


def _background_scan_scheduler():
    """
    Background thread — runs scan_temp_city for all cities every 4 hours.
    Writes ALL brackets (all grades) to temp_snapshots for calibration.
    Completely independent of the auto-trader.
    """
    import time as _t
    print("  📊 Background scan scheduler started")
    _t.sleep(120)  # stagger 2 min after startup
    while True:
        try:
            _run_background_scan()
        except Exception as e:
            print(f"  ⚠️  Background scan error: {e}")
        _t.sleep(_SCAN_INTERVAL_SECS)


def _price_history_log(city_key, fc, markets):
    """
    Log price + forecast snapshot for every bracket every scan cycle.
    No grade filter — logs all brackets including skip.
    This builds the time series needed to measure:
      - How fast markets reprice after model forecast shifts
      - Whether price momentum predicts continued movement
      - The lag window between HRRR/GFS updates and market repricing
    """
    try:
        conn = get_db()
        if not conn: return
        rows = []
        for m in markets:
            if not m.get("ticker"): continue
            rows.append((
                city_key,
                fc.get("target_date"),
                fc.get("horizon"),
                m["ticker"],
                m.get("bracket_label"),
                m.get("lo_temp"),
                m.get("hi_temp"),
                m.get("yes_ask"),
                m.get("yes_bid"),
                m.get("volume_24h"),
                m.get("open_interest"),
                m.get("mu") or fc.get("best_high"),
                fc.get("gfs_high"),
                fc.get("ecmwf_high"),
                m.get("sigma") or fc.get("sigma"),
                m.get("model_prob"),
                m.get("net_gap_c"),
                m.get("grade"),
            ))
        if not rows: return
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO price_history
                (city, target_date, horizon, ticker, bracket_label,
                 lo_temp, hi_temp, yes_ask, yes_bid, volume_24h,
                 open_interest, mu, gfs_high, ecmwf_high, sigma,
                 model_prob, net_gap_c, grade)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, rows)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"  ⚠️  _price_history_log error: {e}")


def _paper_trade_log(city_key, fc, markets):
    """
    Log A-grade signals as paper trades (bet tracking) AND log all grades
    to calibration_snapshots (bias calibration data).

    paper_trades  — A-grade only, tracks simulated bet performance
    calibration_snapshots — all grades, used to compute per-city forecast bias
                            once 30+ settled rows exist within a rolling 30-day window

    Both tables skip signals past cutoff (hours_to_cutoff < 0).
    """
    try:
        conn = get_db()
        if not conn: return
        with conn.cursor() as cur:
            for m in markets:
                if not m.get("ticker"): continue
                htc = m.get("hours_to_cutoff")
                if htc is not None and htc < 0: continue

                # Compute model spread for logging
                _gfs_h   = fc.get("gfs_high")
                _ecmwf_h = fc.get("ecmwf_high")
                _spread  = round(abs(_gfs_h - _ecmwf_h), 2) if (_gfs_h and _ecmwf_h) else None

                row = (
                    city_key,
                    fc.get("nws_station"),
                    fc.get("target_date"),
                    fc.get("horizon"),
                    m["ticker"],
                    m.get("bracket_label"),
                    m.get("lo_temp"),
                    m.get("hi_temp"),
                    m.get("grade"),
                    m.get("model_prob"),
                    m.get("yes_ask"),
                    m.get("mu"),
                    m.get("sigma"),
                    m.get("net_gap_c"),
                    m.get("kelly_size"),
                    m.get("hours_to_cutoff"),
                    m.get("mkt_rank_conf"),
                    # Model details
                    _gfs_h,
                    _ecmwf_h,
                    _spread,
                    m.get("edge_ratio"),
                    m.get("gap_c"),
                    m.get("spread_c"),
                    m.get("kelly_frac"),
                    # Market details
                    m.get("yes_bid"),
                    m.get("liq_grade"),
                    m.get("open_interest"),
                    m.get("volume_24h"),
                    m.get("fillable_a"),
                    # Structural flags
                    m.get("is_tail_bet"),
                    m.get("any_model_inside"),
                    m.get("spread_exceeds_bracket"),
                    m.get("book_limited"),
                )

                _cols = """city, nws_station, target_date, horizon, ticker,
                         bracket_label, lo_temp, hi_temp, grade,
                         model_prob, yes_ask, mu, sigma, net_gap_c,
                         kelly_size, hours_to_cutoff, mkt_rank_conf,
                         gfs_high, ecmwf_high, model_spread,
                         edge_ratio, gap_c, spread_c, kelly_frac,
                         yes_bid, liq_grade, open_interest, volume_24h, fillable_a,
                         is_tail_bet, any_model_inside, spread_exceeds_bracket, book_limited"""
                _vals = ",".join(["%s"] * 33)

                # paper_trades — A-grade only (bet simulation)
                if m.get("grade") == "A":
                    cur.execute(f"""
                        INSERT INTO paper_trades ({_cols})
                        VALUES ({_vals})
                        ON CONFLICT (ticker, target_date) DO NOTHING
                    """, row)

                # calibration_snapshots — all grades (bias + market rank calibration)
                cur.execute(f"""
                    INSERT INTO calibration_snapshots ({_cols})
                    VALUES ({_vals})
                    ON CONFLICT (ticker, target_date) DO NOTHING
                """, row)

        conn.commit()
        conn.close()
    except Exception as e:
        print(f"  ⚠️  _paper_trade_log error: {e}")


def _paper_trade_settle():
    """
    Settle open paper trades and calibration_snapshots by matching against
    temp_snapshots.settled_temp. Called after every successful run_auto_settlement
    AND at the end of each background scan.

    Records every run in _PROP_LOG (in-memory ring buffer, surfaces in
    /debug/cal-log) and prints to Railway logs. Previously failed silently —
    that hid every propagation error since deployment.
    """
    import datetime as _dt
    run_at = _dt.datetime.utcnow().isoformat()
    result = {"run_at": run_at, "ok": False, "rowcounts": {}, "error": None}
    try:
        conn = get_db()
        if not conn:
            result["error"] = "No DB connection"
            print(f"  ⚠️  _paper_trade_settle: no DB")
            with _PROP_LOCK:
                _PROP_LOG.insert(0, result)
                _PROP_LOG[:] = _PROP_LOG[:50]
            return

        settle_sql = """
            SET
                settled_temp    = ts.settled_temp,
                settled_correct = CASE
                    WHEN {tbl}.lo_temp IS NULL AND {tbl}.hi_temp IS NOT NULL
                        THEN ts.settled_temp <= {tbl}.hi_temp
                    WHEN {tbl}.hi_temp IS NULL AND {tbl}.lo_temp IS NOT NULL
                        THEN ts.settled_temp >= {tbl}.lo_temp
                    ELSE ts.settled_temp >= {tbl}.lo_temp
                     AND ts.settled_temp <= {tbl}.hi_temp
                END,
                settled_ts = NOW()
            FROM (
                SELECT DISTINCT ON (city, target_date)
                    city, target_date, settled_temp
                FROM temp_snapshots
                WHERE settled_temp IS NOT NULL
                ORDER BY city, target_date, scan_ts DESC
            ) ts
            WHERE {tbl}.city        = ts.city
              AND {tbl}.target_date = ts.target_date
              AND {tbl}.settled_temp IS NULL
        """

        with conn.cursor() as cur:
            # Pre-check: how many (city,target_date) pairs are eligible to propagate
            cur.execute("""
                SELECT COUNT(DISTINCT (city, target_date))
                FROM temp_snapshots
                WHERE settled_temp IS NOT NULL
            """)
            result["eligible_pairs"] = cur.fetchone()[0]

            for tbl in ("paper_trades", "calibration_snapshots"):
                cur.execute(f"UPDATE {tbl} " + settle_sql.format(tbl=tbl))
                result["rowcounts"][tbl] = cur.rowcount
        conn.commit()
        conn.close()
        result["ok"] = True
        print(f"  📌 _paper_trade_settle: {result['rowcounts']} "
              f"(eligible {result['eligible_pairs']} pairs)")
    except Exception as e:
        import traceback
        result["error"] = str(e)
        result["traceback"] = traceback.format_exc()
        print(f"  ⚠️  _paper_trade_settle FAILED: {e}")
        traceback.print_exc()
    finally:
        with _PROP_LOCK:
            _PROP_LOG.insert(0, result)
            _PROP_LOG[:] = _PROP_LOG[:50]


def _run_background_scan():
    import time as _t
    start = _t.time()
    total = 0
    for horizon in ["d0", "d1"]:
        for city_key in TEMP_CITIES:
            try:
                result = scan_temp_city(city_key, horizon)
                if result.get("ok"):
                    fc = result.get("forecast", {})
                    all_mkts = result.get("high_markets", []) + result.get("low_markets", [])
                    # Price history — log ALL brackets every scan for time series analysis
                    _price_history_log(city_key, fc, all_mkts)
                    # Calibration — log all grades for bias/market rank analysis
                    _paper_trade_log(city_key, fc, all_mkts)
                    # Log combo signals as A-grade paper trades if applicable
                    for combo in result.get("combo_signals", []):
                        if combo.get("grade") in ("A", "B"):
                            _paper_trade_log(city_key, fc, [combo])
                total += 1
            except Exception:
                pass
    _paper_trade_settle()
    print(f"  📊 Background scan: {total} cities in {round(_t.time()-start,1)}s")


def start_background_scan_scheduler():
    global _SCAN_THREAD
    if _SCAN_THREAD and _SCAN_THREAD.is_alive():
        return
    _SCAN_THREAD = _threading.Thread(
        target=_background_scan_scheduler, daemon=True, name="BgScanner")
    _SCAN_THREAD.start()


if __name__ == "__main__":
    ensure_tables()
    start_settlement_scheduler()       # auto-settle yesterday's temp snapshots each morning
    maybe_auto_calibrate()             # calibrate bias/σ on startup (non-blocking)
    at_load_config_from_db()           # restore auto-trader config from DB
    start_auto_trader_scheduler()      # always-on scheduler (runs only when enabled=True)
    start_background_scan_scheduler()  # log all brackets to temp_snapshots every 4hrs
    print(f"""
╔══════════════════════════════════════════╗
║   Seattle Rain Kalshi Tracker — Server   ║
╚══════════════════════════════════════════╝
  Running at: http://localhost:{PORT}
  Endpoint:   http://localhost:{PORT}/data
  Snapshots:  http://localhost:{PORT}/snapshots
  Accuracy:   http://localhost:{PORT}/accuracy
  DB:         {"✅ Connected" if DATABASE_URL else "⚠️  No DATABASE_URL set"}

  Open dashboard.html in your browser.
  Press Ctrl+C to stop.
""")
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.serve_forever()
