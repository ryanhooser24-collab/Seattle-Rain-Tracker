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
    },
    "san_francisco": {
        "icao_code":     "KSFO",
        "nws_site":      "MTR",
        "nws_issuedby":  "SFO",
        "kalshi_series": "KXRAINSFO",   # confirm — may not exist yet
        "lat": 37.619, "lon": -122.375, "tz": "America/Los_Angeles",
        "regime":        "frontal_seasonal",
        "tradeable_months": [11, 12, 1, 2, 3, 4],
        "label":         "San Francisco, CA",
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
    },
}

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
    """Scrape NWS Daily Climate Report for MTD precipitation."""
    cfg = city_cfg or CITY_CFG
    nws_url = (f"https://forecast.weather.gov/product.php"
               f"?site={cfg['nws_site']}"
               f"&issuedby={cfg['nws_issuedby']}"
               f"&product=CLI&format=txt")
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(nws_url, headers=headers, timeout=6)
        r.raise_for_status()
        text = r.text

        # Extract the plain text product
        soup = BeautifulSoup(text, "html.parser")
        pre  = soup.find("pre")
        if not pre:
            # Try finding text between tags
            raw = text
        else:
            raw = pre.get_text()

        # Look for MONTH TO DATE precipitation line
        mtd_match = re.search(
            r"MONTH TO DATE\s+([\d\.T]+)",
            raw, re.IGNORECASE
        )
        today_match = re.search(
            r"(?:YESTERDAY|TODAY)\s+([\d\.T]+)\s",
            raw, re.IGNORECASE
        )
        date_match = re.search(
            r"CLIMATE SUMMARY FOR\s+([\w\s]+\d{4})",
            raw, re.IGNORECASE
        )

        # Extract exact report issuance time e.g. "620 PM PDT SUN MAR 08 2026"
        issued_match = re.search(
            r"(\d{3,4}\s+(?:AM|PM)\s+\w+\s+\w+\s+\w+\s+\d+\s+\d{4})",
            raw, re.IGNORECASE
        )
        issued_time = issued_match.group(1).strip() if issued_match else None

        # "VALID TODAY AS OF 0500 PM LOCAL TIME"
        valid_match = re.search(
            r"VALID\s+\w+\s+AS\s+OF\s+([\d:]+\s*(?:AM|PM)?\s*\w+\s*TIME)",
            raw, re.IGNORECASE
        )
        valid_time = valid_match.group(1).strip() if valid_match else None

        mtd   = float(mtd_match.group(1)) if mtd_match and mtd_match.group(1) != "T" else 0.0
        today = float(today_match.group(1)) if today_match and today_match.group(1) != "T" else 0.0
        date  = date_match.group(1).strip() if date_match else "Unknown"

        issuedby = cfg.get("nws_issuedby", "SEA")
        label    = cfg.get("label", "Seattle, WA")

        # Classify report as finalized (overnight 12AM-4AM) or preliminary (intraday)
        # Finalized DSM fires at 00:15 AM; CLI follows ~1-2AM. Afternoon CLIs are intraday.
        is_finalized = False
        issued_hour = None
        if issued_time:
            import re as _re
            h_match = _re.search(r"(\d{3,4})\s+(AM|PM)", issued_time, _re.IGNORECASE)
            if h_match:
                raw_h = int(h_match.group(1))
                ampm  = h_match.group(2).upper()
                hour  = (raw_h // 100) % 12 + (12 if ampm == "PM" else 0)
                if ampm == "AM" and raw_h // 100 == 12:
                    hour = 0
                issued_hour = hour
                # Finalized = issued between midnight and 4 AM local time
                is_finalized = (0 <= hour <= 4)

        # MTD reliability: finalized report is authoritative; intraday is preliminary
        mtd_type = "finalized" if is_finalized else "preliminary"

        return {
            "ok":          True,
            "mtd":         mtd,
            "today":       today,
            "date":        date,
            "issued":      issued_time,
            "issued_hour": issued_hour,
            "is_finalized": is_finalized,
            "mtd_type":    mtd_type,
            "valid_as_of": valid_time,
            "source":      "NWS CLI" + issuedby + " (" + label + ")",
        }

    except Exception as e:

        return {"ok": False, "error": str(e), "mtd": 0.0, "today": 0.0, "date": "", "source": "NWS CLI"}


# ── CLM ACTUALS CACHE ─────────────────────────────────────────────────────────
import time as _time
_CLM_CACHE = {}
_CLM_CACHE_TS = {}
_CLM_CACHE_TTL = 6 * 3600

# ── SIGMA CACHE (populated from /backtest RMSE) ───────────────────────────────
_SIGMA_CACHE = {}
_SIGMA_FALLBACK = {10:1.4,9:1.3,8:1.2,7:1.0,6:0.85,5:0.7,4:0.55,3:0.4,2:0.25,1:0.15,0:0.05}

# ── BIAS CACHE (populated from /backtest/calibration) ────────────────────────
_BIAS_CACHE = {}


def update_sigma_from_backtest(city_key, summary):
    if not summary:
        return
    horizon_map = {1:"d1",2:"d1",3:"d3",4:"d3",5:"d5",6:"d5",7:"d7",8:"d7",9:"d10",10:"d10"}
    sigma_table = dict(_SIGMA_FALLBACK)
    for days, key in horizon_map.items():
        if key in summary and summary[key].get("rmse") is not None:
            sigma_table[days] = summary[key]["rmse"]
    _SIGMA_CACHE[city_key] = sigma_table


def get_sigma(city_key, days_remaining):
    table = _SIGMA_CACHE.get(city_key, _SIGMA_FALLBACK)
    sigma = table.get(min(days_remaining, 10), 1.4)
    bias  = _BIAS_CACHE.get(f"{city_key}-d{min(days_remaining, 7)}", 0.0)
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
    Fetch IEM ASOS precipitation from midnight (local) to now.

    NWS MTD covers midnight-to-midnight in LOCAL STANDARD TIME.
    During DST the ASOS DSM day runs 1AM-to-1AM local time (clock-wall).
    So the IEM anchor is:
      - Standard time (Nov-Mar approx): midnight (00:00 local)
      - Daylight time (Mar-Nov approx): 1:00 AM local (DST shift)
    We detect DST by checking if the city timezone is currently observing DST.
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

        # Detect DST: ASOS DSM counts midnight-to-midnight in standard time.
        # During DST, wall-clock midnight = standard time 11 PM the night before,
        # so the DSM day starts at wall-clock 1:00 AM.
        is_dst = bool(now_local.dst())
        gap_start_hour = 1 if is_dst else 0
        gap_start_str  = "01:00" if is_dst else "00:00"

        today_str = now_local.strftime("%Y-%m-%d")

        # Sanity: if current time is before gap_start_hour (e.g. 12:30 AM during DST)
        # IEM gap would be negative — return zero gracefully
        if now_local.hour < gap_start_hour:
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
            f"&hour1={gap_start_hour}&min1=0"
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

        readings = []
        gap_total = 0.0

        for line in data_lines:
            parts = line.strip().split(",")
            if len(parts) < 3:
                continue
            station, valid_time, precip = parts[0], parts[1], parts[2]
            if precip == "M" or precip == "":
                continue  # missing — skip
            try:
                p = float(precip)
                if p > 0:
                    readings.append({"time": valid_time, "precip": round(p, 2)})
                    gap_total += p
            except ValueError:
                continue

        gap_total = round(gap_total, 2)

        # Sanity check — gap fill covers midnight→now, up to ~18 hours.
        # 5" threshold: rare but possible during atmospheric rivers (Seattle Nov 2006: 15.63"/month)
        # 3" was too low and would silently zero out heavy rain events.
        if gap_total > 5.0:
            return {
                "ok": False,
                "error": f"IEM returned unrealistic gap total {gap_total} - ignoring",
                "gap_total": 0.0,
                "readings": [],
                "gap_start": gap_start_str,
                "gap_end": now_local.strftime("%I:%M %p")
            }

        return {
            "ok":        True,
            "gap_total": gap_total,
            "readings":  readings[-20:],  # last 20 readings for display
            "gap_start": gap_start_str,
            "gap_end":   now_local.strftime("%I:%M %p"),
            "source":    "IEM KSEA ASOS"
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
        body = json.dumps(data).encode()
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
        else:
            self.send_response(404)
            self.end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
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

            # Sigma estimates by days remaining (calibrated via Open-Meteo backtest)
            SIGMA_TABLE = {
                10:1.4, 9:1.3, 8:1.2, 7:1.0, 6:0.85, 5:0.7,
                4:0.55, 3:0.4, 2:0.25, 1:0.15, 0:0.0
            }
            sigma_est = SIGMA_TABLE.get(min(days_remaining, 10), 1.5)

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
                "db_connected":     bool(DATABASE_URL and PSYCOPG2_AVAILABLE),
            }

            print(f"  ✅ NWS: {true_mtd}\" | IEM gap: +{gap_total}\" | True MTD: {true_mtd}\" | EOD proj: {today_eod}\" | WU 10-day: {wu_remaining}\" | conf: {conf}")
            self.send_json(result)

        elif path == "/snapshots":
            # Return forecast snapshots for current or specified month
            qs = parse_qs(urlparse(self.path).query)
            month = qs.get("month", [None])[0]
            rows = fetch_snapshots(month)
            # Convert date objects to strings for JSON
            for r in rows:
                for k, v in r.items():
                    if hasattr(v, 'isoformat'):
                        r[k] = v.isoformat()
            self.send_json({"ok": True, "snapshots": rows, "count": len(rows)})

        elif path == "/chart-data":
            # Return intraday snapshots for projection vs market chart
            qs     = parse_qs(urlparse(self.path).query)
            city   = qs.get("city", ["seattle"])[0]
            month  = qs.get("month", [datetime.now().strftime("%Y-%m")])[0]
            conn   = get_db()
            if not conn:
                self.send_json({"ok": False, "error": "No DB", "rows": []})
            else:
                try:
                    import psycopg2.extras
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        cur.execute("""
                            SELECT snapshot_ts, days_remaining, true_mtd,
                                   projected_eom, sigma_estimate,
                                   model_prob_5, model_prob_6, model_prob_7,
                                   kalshi_yes_5, kalshi_yes_6, kalshi_yes_7,
                                   gap_5, gap_6, gap_7
                            FROM intraday_snapshots
                            WHERE city=%s AND month LIKE %s
                            ORDER BY snapshot_ts ASC
                        """, (city, f"{city}-{month}%"))
                        rows = cur.fetchall()
                        result = []
                        for r in rows:
                            d = dict(r)
                            for k,v in d.items():
                                if hasattr(v,'isoformat'): d[k]=v.isoformat()
                                elif hasattr(v,'__float__'): d[k]=float(v)
                            result.append(d)
                    conn.close()
                    self.send_json({"ok": True, "rows": result, "count": len(result)})
                except Exception as e:
                    self.send_json({"ok": False, "error": str(e), "rows": []})

        elif path == "/accuracy":
            # Return forecast_accuracy view — all months with settlements
            rows = fetch_accuracy_view()
            for r in rows:
                for k, v in r.items():
                    if hasattr(v, 'isoformat'):
                        r[k] = v.isoformat()
                    elif hasattr(v, '__float__'):
                        r[k] = float(v)
            self.send_json({"ok": True, "rows": rows, "count": len(rows)})

        elif path == "/debug/kalshi":
            try:
                url = f"{KALSHI_BASE}/markets"
                params = {"series_ticker": KALSHI_SERIES, "status": "open", "limit": 10}
                hdrs = kalshi_auth_headers("GET", "/trade-api/v2/markets")
                r = requests.get(url, params=params, headers=hdrs, timeout=6)
                raw = r.json()
                mkts = raw.get("markets", [])
                debug = []
                for m in mkts[:3]:
                    debug.append({k: m.get(k) for k in [
                        "ticker","title","subtitle","strike_type",
                        "floor_strike","cap_strike","functional_strike",
                        "yes_ask_dollars","no_ask_dollars"
                    ]})
                self.send_json({"total": len(mkts), "sample": debug})
            except Exception as e:
                self.send_json({"error": str(e)})

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

                    # Fetch at each lead time
                    horizon_data = {}
                    for lead in [1, 3, 5, 7, 10]:
                        target_date = last_day - timedelta(days=lead)
                        try:
                            # Open-Meteo Previous Runs API
                            # past_days=N returns what was predicted N days ago
                            _pvar = f"precipitation_sum_previous_day{min(lead, 7)}"
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
                            month_total_mm = sum(
                                float(p or 0) for h, p in zip(hours, precip)
                                if h and h[:7] == f"{year}-{month:02d}"
                            )
                            horizon_data[f"d{lead}"] = round(month_total_mm / 25.4, 2)
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
                            for lead in [1, 3, 5, 7, 10]
                        } if actual is not None else {}
                    })

                # Compute summary stats per horizon
                summary = {}
                for lead in [1, 3, 5, 7, 10]:
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
                            # This is the empirical sigma — replace hardcoded table with this
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
                    for level in ob.get("yes", []):
                        price_c, size = int(level[0]), int(level[1])
                        if price_c > EDGE_CEILING: break
                        cost = round(price_c / 100 * size, 2)
                        depth.append({"price_c": price_c, "size": size, "cost": cost})
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
                    EDGE_CEILING = 97; MAX_PCT = 0.40
                    bal_r = requests.get(f"{KALSHI_BASE}/portfolio/balance",
                        headers=kalshi_auth_headers("GET", "/trade-api/v2/portfolio/balance"), timeout=8)
                    bal_r.raise_for_status(); bal = bal_r.json()
                    cash    = float(bal.get("balance", 0) or 0) / 100
                    port    = float(bal.get("portfolio_value", 0) or 0) / 100
                    budget  = min(cash, port * MAX_PCT)
                    ob_r = requests.get(f"{KALSHI_BASE}/markets/{ticker}/orderbook",
                        headers=kalshi_auth_headers("GET", f"/trade-api/v2/markets/{ticker}/orderbook"), timeout=8)
                    ob_r.raise_for_status()
                    asks = ob_r.json().get("orderbook", {}).get(side, [])
                    orders, spent = [], 0.0
                    for level in asks:
                        price_c, size = int(level[0]), int(level[1])
                        if price_c > EDGE_CEILING: break
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
                horizon_errors = {f"d{l}": [] for l in [1, 3, 5, 7, 10]}
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
                            _pvar = f"precipitation_sum_previous_day{min(lead, 7)}"
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

        elif path == "/debug/om":
            # Test Open-Meteo Previous Runs API - try different variable formats
            results = {}
            base_params = {
                "latitude": 47.441, "longitude": -122.3,
                "timezone": "America/Los_Angeles",
                "past_days": 1, "forecast_days": 3,
            }
            # Test candidates based on docs example: temperature_2m_previous_day1
            # So precipitation equivalent should be: precipitation_previous_day1
            candidates = [
                ("hourly", "precipitation_previous_day1"),
                ("hourly", "rain_previous_day1"),
                ("daily",  "precipitation_sum"),   # day 0 only, no previous day suffix for daily?
                ("hourly", "precipitation"),        # just current data, no previous
            ]
            for param_key, var_name in candidates:
                try:
                    p = {**base_params, param_key: var_name}
                    r = requests.get(OM_PREV_URL, params=p, timeout=8)
                    ok = r.ok
                    preview = r.text[:200] if not r.ok else f"OK - keys: {list(r.json().get(param_key,{}).keys())[:5]}"
                    results[f"{param_key}={var_name}"] = {"ok": ok, "status": r.status_code, "preview": preview}
                except Exception as e:
                    results[f"{param_key}={var_name}"] = {"ok": False, "error": str(e)}
            self.send_json({"ok": True, "tests": results})

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
                    # ── 1. Backtest → sigma ──────────────────────────────────
                    actuals = fetch_nws_clm_actuals(city_key)
                    today   = _date.today()
                    horizon_data_all = {f"d{l}": [] for l in [1, 3, 5, 7, 10]}
                    months_used = 0

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
                        last_day = _date(year, month, days_in_month)

                        om_errors = []
                        for lead in [1, 3, 5, 7, 10]:
                            try:
                                _pvar = f"precipitation_sum_previous_day{min(lead, 7)}"
                                r = requests.get(OM_PREV_URL, params={
                                    "latitude":      cfg["lat"],
                                    "longitude":     cfg["lon"],
                                    "hourly":        _pvar,
                                    "timezone":      cfg["tz"],
                                    "past_days":     lead,
                                    "forecast_days": 16,
                                }, timeout=15)
                                if not r.ok:
                                    om_errors.append(f"d{lead}: HTTP {r.status_code} - {r.text[:100]}")
                                    continue
                                data = r.json()
                                if "error" in data:
                                    om_errors.append(f"d{lead}: {data['error']}")
                                    continue
                                hours  = data.get("hourly", {}).get("time", [])
                                precip = data.get("hourly", {}).get(_pvar, [])
                                month_mm = sum(float(p or 0) for h, p in zip(hours, precip)
                                             if h and h[:7] == f"{year}-{month:02d}")
                                error = round(month_mm / 25.4 - actual, 2)
                                horizon_data_all[f"d{lead}"].append(error)
                            except Exception as ex:
                                om_errors.append(f"d{lead}: {str(ex)[:80]}")
                        if om_errors and months_used == 0:
                            # Surface first OM error so it's visible in UI
                            result["errors"].extend(om_errors[:2])
                        months_used += 1

                    # Build summary and update sigma cache
                    summary = {}
                    for lead in [1, 3, 5, 7, 10]:
                        key  = f"d{lead}"
                        errs = horizon_data_all[key]
                        if not errs:
                            continue
                        n    = len(errs)
                        rmse = round((sum(e**2 for e in errs) / n) ** 0.5, 3)
                        mean = round(sum(errs) / n, 3)
                        summary[key] = {
                            "n":        n,
                            "rmse":     rmse,
                            "mean_err": mean,
                            "mae":      round(sum(abs(e) for e in errs) / n, 3),
                            "bias":     "WET" if mean > 0.1 else "DRY" if mean < -0.1 else "NEUTRAL",
                        }
                        # Update bias cache
                        _BIAS_CACHE[f"{city_key}-{key}"] = mean

                    update_sigma_from_backtest(city_key, summary)
                    result["sigma_ok"]     = city_key in _SIGMA_CACHE
                    result["sigma_months"] = months_used
                    result["bias_ok"]      = True
                    result["bias_horizons"] = len(summary)
                    result["summary"]      = summary

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
                        # Kalshi returns _dollars fields already in dollars (strings)
                        qty     = float(p.get("position_fp", 0) or 0)
                        cost    = float(p.get("total_traded_dollars", 0) or 0)
                        r_pnl   = float(p.get("realized_pnl_dollars", 0) or 0)
                        mkt_exp = float(p.get("market_exposure_dollars", 0) or 0)
                        fees    = float(p.get("fees_paid_dollars", 0) or 0)
                        positions.append({
                            "ticker":        p.get("ticker", ""),
                            "market_title":  p.get("market_title", "") or p.get("ticker", ""),
                            "yes_contracts": qty,
                            "avg_yes_price": round(cost / max(qty, 1), 2),
                            "market_value":  round(mkt_exp, 2),
                            "realized_pnl":  round(r_pnl, 2),
                            "unrealized_pnl": 0.0,  # calculated from settled trades only
                            "total_cost":    round(cost, 2),
                            "payout":        round(fees, 2),
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
            # Returns settled trades grouped by date for charting
            if not KALSHI_KEY_ID:
                self.send_json({"ok": False, "error": "No Kalshi API key"})
            else:
                try:
                    qs     = parse_qs(urlparse(self.path).query)
                    limit  = int(qs.get("limit", [100])[0])
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
                        pnl = float(s.get("revenue", 0) or 0) / 100
                        cumulative += pnl
                        trades.append({
                            "date":       s.get("updated_ts", "")[:10],
                            "ticker":     s.get("ticker", ""),
                            "pnl":        round(pnl, 2),
                            "cumulative": round(cumulative, 2),
                            "no_total":   float(s.get("no_total_cost", 0) or 0) / 100,
                            "yes_total":  float(s.get("yes_total_cost", 0) or 0) / 100,
                        })

                    self.send_json({
                        "ok": True,
                        "trades":     trades,
                        "total_pnl":  round(cumulative, 2),
                        "count":      len(trades),
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

        elif path == "/health":
            self.send_json({"ok": True, "message": "Server running"})

        else:
            self.send_response(404)
            self.end_headers()


if __name__ == "__main__":
    ensure_tables()
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
