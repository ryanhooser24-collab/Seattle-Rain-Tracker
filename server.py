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
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
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
KALSHI_API_KEY = os.environ.get("KALSHI_API_KEY", "")
PORT           = int(os.environ.get("PORT", 8765))
DATABASE_URL   = os.environ.get("DATABASE_URL", "")   # Railway Postgres

# WU internal API key
WU_API_KEY = "e1f10a1e78da46f5b10a1e78da96f525"

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
        "lat": 47.441, "lon": -122.3,
        "regime":        "frontal",
        "tradeable_months": list(range(1, 13)),
        "label":         "Seattle, WA",
    },
    "portland": {
        "icao_code":     "KPDX",
        "nws_site":      "PQR",
        "nws_issuedby":  "PDX",
        "kalshi_series": "KXRAINPDXM",
        "lat": 45.589, "lon": -122.6,
        "regime":        "frontal",
        "tradeable_months": list(range(1, 13)),
        "label":         "Portland, OR",
    },
    "san_francisco": {
        "icao_code":     "KSFO",
        "nws_site":      "MTR",
        "nws_issuedby":  "SFO",
        "kalshi_series": "KXRAINSFO",   # confirm — may not exist yet
        "lat": 37.619, "lon": -122.375,
        "regime":        "frontal_seasonal",
        "tradeable_months": [11, 12, 1, 2, 3, 4],
        "label":         "San Francisco, CA",
    },
    "los_angeles": {
        "icao_code":     "KLAX",
        "nws_site":      "LOX",
        "nws_issuedby":  "LAX",
        "kalshi_series": "KXRAINLAXM",
        "lat": 33.938, "lon": -118.408,
        "regime":        "mediterranean",
        "tradeable_months": [11, 12, 1, 2, 3, 4],
        "label":         "Los Angeles, CA",
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


def validate_wu_data(data, days, total):
    """
    Run sanity checks on WU forecast data.
    Returns (is_valid, warning_message)
    """
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
                    warnings.append(f"Forecast appears stale — first date is {first_date}")
            except:
                pass

    # 3. Zero-rain sanity check for Seattle wet season (Oct–May)
    # If total QPF is 0.00" during wet season, likely bad data
    wet_season = month in [10, 11, 12, 1, 2, 3, 4, 5]
    if wet_season and total == 0.0:
        return False, "WU returned 0.00\" total for all 7 days during Seattle wet season — likely bad data, use manual entry"

    # 4. Unreasonably high values (>15" in 7 days would be record-breaking)
    if total > 15.0:
        return False, f"WU returned {total}\" over 7 days — unrealistically high, likely bad data"

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


def fetch_wu_forecast():
    """Fetch 7-day QPF forecast from Weather Underground internal API."""
    try:
        url = (
            f"https://api.weather.com/v3/wx/forecast/daily/10day"
            f"?apiKey={WU_API_KEY}"
            f"&icaoCode={WU_ICAO_CODE}"
            f"&language=en-US&units=e&format=json"
        )
        headers = {
            "Origin": "https://www.wunderground.com",
            "Referer": "https://www.wunderground.com/",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36"
        }
        r = requests.get(url, headers=headers, timeout=10)

        # Detect API key rotation (401 = unauthorized, 403 = forbidden)
        if r.status_code in (401, 403):
            return {
                "ok": False,
                "error": "WU API key has rotated — manual entry required",
                "days": [], "total_forecast": 0, "source": "Weather Underground",
                "needs_manual": True
            }

        r.raise_for_status()
        data = r.json()

        # Check for error response disguised as 200
        if "errors" in data or "statusCode" in data:
            return {
                "ok": False,
                "error": f"WU returned error response: {data.get('errors') or data.get('statusCode')}",
                "days": [], "total_forecast": 0, "source": "Weather Underground",
                "needs_manual": True
            }

        dates   = data.get("validTimeLocal", [])
        qpf     = data.get("qpf", [])
        dow     = data.get("dayOfWeek", [])
        narr    = data.get("narrative", [])

        days = []
        for i in range(len(dates)):
            date_str = dates[i][:10] if dates[i] else ""
            days.append({
                "date":      date_str,
                "dayOfWeek": dow[i] if i < len(dow) else "",
                "qpf":       float(qpf[i]) if i < len(qpf) and qpf[i] is not None else 0.0,
                "narrative": narr[i] if i < len(narr) else "",
            })

        total = round(sum(d["qpf"] for d in days), 2)

        # Run validation
        is_valid, warning = validate_wu_data(data, days, total)
        if not is_valid:
            return {
                "ok": False,
                "error": warning,
                "days": [], "total_forecast": 0,
                "source": "Weather Underground",
                "needs_manual": True
            }

        return {
            "ok": True,
            "days": days,
            "total_forecast": total,
            "source": "Weather Underground",
            "warning": warning,  # May be None or a non-fatal warning string
            "needs_manual": False
        }

    except requests.exceptions.Timeout:
        return {"ok": False, "error": "WU request timed out — try refreshing", "days": [], "total_forecast": 0, "source": "Weather Underground", "needs_manual": True}
    except requests.exceptions.ConnectionError:
        return {"ok": False, "error": "Cannot reach WU — check internet connection", "days": [], "total_forecast": 0, "source": "Weather Underground", "needs_manual": True}
    except Exception as e:
        return {"ok": False, "error": f"WU fetch failed: {str(e)}", "days": [], "total_forecast": 0, "source": "Weather Underground", "needs_manual": True}


def fetch_nws_mtd():
    """Scrape NWS CLISEA Daily Climate Report for MTD precipitation."""
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(NWS_URL, headers=headers, timeout=10)
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

        return {
            "ok":          True,
            "mtd":         mtd,
            "today":       today,
            "date":        date,
            "issued":      issued_time,
            "valid_as_of": valid_time,
            "source":      "NWS CLISEA (Seattle-Tacoma)"
        }

    except Exception as e:
        return {"ok": False, "error": str(e), "mtd": 0.0, "today": 0.0, "date": "", "source": "NWS CLISEA"}


def fetch_wu_hourly():
    """
    Fetch WU hourly forecast for today from current hour through midnight.
    Used to complete today's projected EOD total alongside IEM actuals.
    Returns hourly QPF list and today's remaining total.
    """
    try:
        url = (
            f"https://api.weather.com/v3/wx/forecast/hourly/2day"
            f"?apiKey={WU_API_KEY}"
            f"&icaoCode={WU_ICAO_CODE}"
            f"&language=en-US&units=e&format=json"
        )
        headers = {
            "Origin": "https://www.wunderground.com",
            "Referer": "https://www.wunderground.com/",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/145.0.0.0 Safari/537.36"
        }
        r = requests.get(url, headers=headers, timeout=10)

        if r.status_code in (401, 403):
            return {"ok": False, "error": "WU hourly API key rotated", "hours": [], "today_remaining": 0}

        r.raise_for_status()
        data = r.json()

        times = data.get("validTimeLocal", [])
        qpf   = data.get("qpf", [])

        now_local  = datetime.now()
        today_str  = now_local.strftime("%Y-%m-%d")
        now_hour   = now_local.hour

        # Filter to remaining hours today (from current hour through 11 PM)
        today_hours = []
        for i, t in enumerate(times):
            if not t:
                continue
            # Parse hour from validTimeLocal e.g. "2026-03-18T20:00:00-0700"
            hour_date = t[:10]
            hour_num  = int(t[11:13])
            if hour_date == today_str and hour_num >= now_hour:
                p = float(qpf[i]) if i < len(qpf) and qpf[i] is not None else 0.0
                today_hours.append({
                    "time":  t[11:16],  # HH:MM
                    "hour":  hour_num,
                    "qpf":   round(p, 2)
                })

        today_remaining = round(sum(h["qpf"] for h in today_hours), 2)

        return {
            "ok":             True,
            "hours":          today_hours,
            "today_remaining": today_remaining,
            "source":         "Weather Underground hourly"
        }

    except Exception as e:
        return {"ok": False, "error": str(e), "hours": [], "today_remaining": 0}


def fetch_iem_gap(nws_issued_str):
    """
    Fetch IEM KSEA precipitation since the NWS report time.
    Uses Iowa Environmental Mesonet ASOS data — same KSEA instrument,
    0.01" precision, no rounding issues.

    nws_issued_str: e.g. "618 PM PDT TUE MAR 17 2026"
    Returns gap total in inches and list of hourly readings.
    """
    try:
        from datetime import datetime, timezone, timedelta
        import csv
        from io import StringIO

        now_local = datetime.now()
        today_str = now_local.strftime("%Y-%m-%d")

        # Parse NWS issued time to get gap start
        # Format: "618 PM PDT TUE MAR 17 2026" or "1020 AM PDT..."
        gap_start_hour = None
        gap_start_str  = None

        if nws_issued_str:
            # Extract time portion e.g. "618 PM" or "1020 AM"
            time_match = re.search(
                r"(\d{3,4})\s+(AM|PM)",
                nws_issued_str, re.IGNORECASE
            )
            if time_match:
                raw_time = time_match.group(1)
                ampm     = time_match.group(2).upper()
                # Parse HHMM or HMM
                if len(raw_time) == 3:
                    h, m = int(raw_time[0]), int(raw_time[1:])
                else:
                    h, m = int(raw_time[:2]), int(raw_time[2:])
                if ampm == "PM" and h != 12:
                    h += 12
                elif ampm == "AM" and h == 12:
                    h = 0
                gap_start_hour = h
                gap_start_str  = f"{h:02d}:{m:02d}"

        # If we can't parse, use 17:00 as fallback
        if gap_start_hour is None:
            gap_start_hour = 17
            gap_start_str  = "17:00"

        # Build IEM request — get today's precip data from gap start to now
        # p01i = precipitation in last 1 minute interval, in inches
        url = (
            f"https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
            f"?station=SEA"
            f"&data=p01i"
            f"&year1={now_local.year}&month1={now_local.month:02d}&day1={now_local.day:02d}"
            f"&hour1={gap_start_hour}&min1=0"
            f"&year2={now_local.year}&month2={now_local.month:02d}&day2={now_local.day:02d}"
            f"&hour2={now_local.hour}&min2={now_local.minute}"
            f"&tz=America/Los_Angeles"
            f"&format=comma&latlon=no&direct=no&report_type=1"
        )

        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=15)
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

        # Sanity check — gap > 3" in a few hours is unrealistic
        if gap_total > 3.0:
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


def fetch_kalshi_markets():
    """Fetch open Seattle rain markets from Kalshi API."""
    try:
        if not KALSHI_API_KEY:
            return {"ok": False, "error": "Kalshi API key not set", "markets": []}

        url = f"{KALSHI_BASE}/markets"
        params = {"series_ticker": KALSHI_SERIES, "status": "open", "limit": 100}
        headers = {
            "Authorization": f"Bearer {KALSHI_API_KEY}",
            "Content-Type": "application/json"
        }
        r = requests.get(url, params=params, headers=headers, timeout=10)
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
                    except:
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

            markets.append({
                "ticker":     ticker,
                "title":      m.get("title", ""),
                "subtitle":   subtitle,
                "label":      label,
                "inches":     inches,
                "strike_type": m.get("strike_type", "greater"),
                "yes_ask":    float(m.get("yes_ask_dollars", 0) or 0),
                "no_ask":     float(m.get("no_ask_dollars", 0) or 0),
                "yes_bid":    float(m.get("yes_bid_dollars", 0) or 0),
                "no_bid":     float(m.get("no_bid_dollars", 0) or 0),
                "last_price": float(m.get("last_price_dollars", 0) or 0),
                "volume":     m.get("volume_fp", "0"),
                "close_time": m.get("close_time", ""),
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
        return round(1.0 - (days_remaining / 7) ** 0.7, 3)
    else:
        # 8-10 days: very low, linear falloff
        return round(0.05 * (10 - days_remaining) / 2, 3)


def analyze_value(markets, projected_total, days_remaining=10):
    """
    Confidence-weighted edge signal.

    margin         = projection - strike  (how far we clear the bar)
    confidence     = f(days_remaining)    (how much we trust the forecast)
    weighted_edge  = margin × confidence

    Signal tiers (weighted_edge):
      >= +0.40  → STRONG YES
      >= +0.15  → LEAN YES
      <= -0.40  → STRONG NO
      <= -0.15  → LEAN NO
      else      → HOLD (margin exists but confidence too low)
    """
    conf = confidence_weight(days_remaining)
    analyzed = []

    for m in markets:
        inches = m.get("inches")
        if inches is None:
            m["edge"] = "unknown"
            m["edge_detail"] = ""
            m["weighted_edge"] = 0
            m["confidence"] = conf
            analyzed.append(m)
            continue

        margin        = round(projected_total - inches, 2)
        weighted_edge = round(margin * conf, 3)

        if weighted_edge >= 0.40:
            edge = "STRONG_YES"
        elif weighted_edge >= 0.15:
            edge = "LEAN_YES"
        elif weighted_edge <= -0.40:
            edge = "STRONG_NO"
        elif weighted_edge <= -0.15:
            edge = "LEAN_NO"
        else:
            edge = "HOLD"

        m["edge"]          = edge
        m["margin"]        = margin
        m["confidence"]    = conf
        m["weighted_edge"] = weighted_edge
        m["edge_detail"]   = (
            f"Proj {projected_total:.2f}\" vs {inches:.0f}\" strike "
            f"({margin:+.2f}\") × {int(conf*100)}% conf = {weighted_edge:+.3f}"
        )
        analyzed.append(m)

    return analyzed


# ── POSTGRES — snapshot storage ───────────────────────────────────────────────

def get_db():
    """Return a psycopg2 connection or None if unavailable."""
    if not PSYCOPG2_AVAILABLE or not DATABASE_URL:
        return None
    try:
        return psycopg2.connect(DATABASE_URL, sslmode="require")
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
                    month            TEXT NOT NULL,          -- e.g. '2026-03'
                    snapshot_date    DATE NOT NULL,
                    days_remaining   INTEGER NOT NULL,
                    true_mtd         NUMERIC(6,2),
                    wu_remaining     NUMERIC(6,2),
                    projected_eom    NUMERIC(6,2),
                    confidence       NUMERIC(5,3),
                    wu_days_used     INTEGER,
                    created_at       TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (month, days_remaining)          -- one row per horizon
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


def maybe_write_snapshot(month, days_remaining, true_mtd, wu_remaining,
                          projected_eom, confidence, wu_days_used):
    """
    Write one snapshot row per (month, days_remaining).
    The UNIQUE constraint silently skips duplicates — safe to call on every /data request.
    Only writes when inside the 10-day window (days_remaining <= 10).
    """
    if days_remaining > 10 or days_remaining < 0:
        return
    conn = get_db()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO forecast_snapshots
                    (month, snapshot_date, days_remaining, true_mtd,
                     wu_remaining, projected_eom, confidence, wu_days_used)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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
            print("\n📡 Fetching all data sources...")

            wu         = fetch_wu_forecast()
            wu_hourly  = fetch_wu_hourly()
            nws        = fetch_nws_mtd()
            kalshi     = fetch_kalshi_markets()

            # Fetch IEM gap fill — from NWS report time to now
            iem = fetch_iem_gap(nws.get("issued"))

            # True MTD = NWS MTD + IEM gap
            mtd           = nws.get("mtd", 0)
            gap_total     = iem.get("gap_total", 0) if iem.get("ok") else 0
            true_mtd      = round(mtd + gap_total, 2)

            # Today's projected EOD = true MTD + WU hourly remainder tonight
            today_remaining = wu_hourly.get("today_remaining", 0) if wu_hourly.get("ok") else 0
            today_eod       = round(true_mtd + today_remaining, 2)

            # 10-day projected total uses true MTD + WU 10-day remainder
            wu_remaining   = wu.get("total_forecast", 0)
            days_remaining = 31 - datetime.now().day

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

            # Analyze Kalshi market value — confidence-weighted signal
            if kalshi.get("ok"):
                proj_for_signal = projected if wu_covers_eom else round(true_mtd + wu_remaining, 2)
                kalshi["markets"] = analyze_value(
                    kalshi["markets"], proj_for_signal, days_remaining
                )

            # Write daily snapshot (once per days_remaining value, idempotent)
            if wu_covers_eom and projected is not None:
                maybe_write_snapshot(
                    month       = month_key,
                    days_remaining = days_remaining,
                    true_mtd    = true_mtd,
                    wu_remaining = wu_remaining,
                    projected_eom = projected,
                    confidence  = conf,
                    wu_days_used = wu_days_used,
                )

            result = {
                "timestamp":        datetime.now().isoformat(),
                "wu":               wu,
                "wu_hourly":        wu_hourly,
                "nws":              nws,
                "iem":              iem,
                "kalshi":           kalshi,
                "mtd":              mtd,
                "gap_total":        gap_total,
                "true_mtd":         true_mtd,
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

            print(f"  ✅ NWS: {mtd}\" | IEM gap: +{gap_total}\" | True MTD: {true_mtd}\" | EOD proj: {today_eod}\" | WU 10-day: {wu_remaining}\" | conf: {conf}")
            self.send_json(result)

        elif path == "/snapshots":
            # Return forecast snapshots for current or specified month
            from urllib.parse import parse_qs
            qs = parse_qs(urlparse(self.path).query)
            month = qs.get("month", [None])[0]
            rows = fetch_snapshots(month)
            # Convert date objects to strings for JSON
            for r in rows:
                for k, v in r.items():
                    if hasattr(v, 'isoformat'):
                        r[k] = v.isoformat()
            self.send_json({"ok": True, "snapshots": rows, "count": len(rows)})

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
                hdrs = {"Authorization": f"Bearer {KALSHI_API_KEY}"}
                r = requests.get(url, params=params, headers=hdrs, timeout=10)
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

        elif path == "/" or path == "/dashboard":
            try:
                with open("dashboard.html", "rb") as f:
                    content_html = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(content_html)
            except:
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
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    server.serve_forever()
