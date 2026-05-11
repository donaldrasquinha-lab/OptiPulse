"""
PCP & OI Scanner — Streamlit App
==================================
Single-file Nifty 50 Put-Call Parity & OI Analysis Dashboard.

Run:  pip install streamlit requests plotly pandas && streamlit run app.py

Features:
  • Scanner: All 50 stocks — PCP violations, OI, PCR, IV, signal classification
  • Heatmap: Visual PCP violation intensity across Nifty 50
  • Nifty Index OI: Full index option chain with OI bars, OI change, data table
  • Option Chain: Per-stock drilldown with call/put OI distribution, PCP profile
  • Rate limiter: Token bucket 50 calls/60s sliding window
  • Staggered fetch: 5 symbols/2s batches
  • Exponential backoff: 5s → 60s on API errors
  • Cache: TTL-based per endpoint
  • Simulation: Works instantly without API token
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import math
import time
import random
from datetime import datetime, timedelta
from collections import deque

try:
    import requests as http_req
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

# ═══════════════════════════════════════════════════════════════
#  PAGE CONFIG
# ═══════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="PCP & OI Scanner",
    page_icon="Σ",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Dark theme CSS
st.markdown("""
<style>
    .stApp {background-color: #060a10;}
    section[data-testid="stSidebar"] {background-color: #0b1018;}
    .metric-card {
        background: #0b1018; border: 1px solid rgba(255,255,255,.06);
        border-radius: 10px; padding: 14px 16px; text-align: center;
    }
    .metric-label {font-size: 10px; color: #6b7a8d; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 4px;}
    .metric-value {font-size: 22px; font-weight: 700;}
    .bullish {color: #00e676;} .bearish {color: #ff1744;}
    .arb {color: #ffd600;} .neutral {color: #6b7a8d;}
    .blue {color: #448aff;} .purple {color: #d500f9;}
    div[data-testid="stMetricValue"] {font-size: 20px;}
    .signal-badge {
        display: inline-block; padding: 2px 10px; border-radius: 12px;
        font-size: 11px; font-weight: 600;
    }
    .sig-BULLISH {background: rgba(0,230,118,.1); border: 1px solid rgba(0,230,118,.3); color: #00e676;}
    .sig-BEARISH {background: rgba(255,23,68,.1); border: 1px solid rgba(255,23,68,.3); color: #ff1744;}
    .sig-ARBITRAGE {background: rgba(255,214,0,.1); border: 1px solid rgba(255,214,0,.3); color: #ffd600;}
    .sig-NEUTRAL {background: rgba(255,255,255,.03); border: 1px solid rgba(255,255,255,.08); color: #6b7a8d;}
</style>
""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
#  NIFTY 50 INSTRUMENTS
# ═══════════════════════════════════════════════════════════════
NIFTY50 = {
    "RELIANCE":"NSE_EQ|INE002A01018","TCS":"NSE_EQ|INE467B01029","HDFCBANK":"NSE_EQ|INE040A01034",
    "INFY":"NSE_EQ|INE009A01021","ICICIBANK":"NSE_EQ|INE090A01021","HINDUNILVR":"NSE_EQ|INE030A01027",
    "ITC":"NSE_EQ|INE154A01025","SBIN":"NSE_EQ|INE062A01020","BHARTIARTL":"NSE_EQ|INE397D01024",
    "KOTAKBANK":"NSE_EQ|INE237A01028","LT":"NSE_EQ|INE018A01030","AXISBANK":"NSE_EQ|INE238A01034",
    "BAJFINANCE":"NSE_EQ|INE296A01024","ASIANPAINT":"NSE_EQ|INE021A01026","MARUTI":"NSE_EQ|INE585B01010",
    "TITAN":"NSE_EQ|INE280A01028","SUNPHARMA":"NSE_EQ|INE044A01036","ULTRACEMCO":"NSE_EQ|INE481G01011",
    "WIPRO":"NSE_EQ|INE075A01022","NESTLEIND":"NSE_EQ|INE239A01016","HCLTECH":"NSE_EQ|INE860A01027",
    "TATAMOTORS":"NSE_EQ|INE155A01022","NTPC":"NSE_EQ|INE733E01010","POWERGRID":"NSE_EQ|INE752E01010",
    "M&M":"NSE_EQ|INE101A01026","TATASTEEL":"NSE_EQ|INE081A01020","ONGC":"NSE_EQ|INE213A01029",
    "JSWSTEEL":"NSE_EQ|INE019A01038","ADANIPORTS":"NSE_EQ|INE742F01042","COALINDIA":"NSE_EQ|INE522F01014",
    "BAJAJFINSV":"NSE_EQ|INE918I01018","GRASIM":"NSE_EQ|INE047A01021","TECHM":"NSE_EQ|INE669C01036",
    "DRREDDY":"NSE_EQ|INE089A01023","HINDALCO":"NSE_EQ|INE038A01020","CIPLA":"NSE_EQ|INE059A01026",
    "BPCL":"NSE_EQ|INE029A01011","DIVISLAB":"NSE_EQ|INE361B01024","APOLLOHOSP":"NSE_EQ|INE437A01024",
    "EICHERMOT":"NSE_EQ|INE066A01021","TATACONSUM":"NSE_EQ|INE192A01025","SBILIFE":"NSE_EQ|INE123W01016",
    "BRITANNIA":"NSE_EQ|INE216A01030","INDUSINDBK":"NSE_EQ|INE095A01012","HEROMOTOCO":"NSE_EQ|INE158A01026",
    "HDFCLIFE":"NSE_EQ|INE795G01014","BAJAJ-AUTO":"NSE_EQ|INE917I01010","UPL":"NSE_EQ|INE628A01036",
    "LTIM":"NSE_EQ|INE214T01019","SHRIRAMFIN":"NSE_EQ|INE721A01013",
}
NIFTY_INDEX_KEY = "NSE_INDEX|Nifty 50"
UPSTOX_BASE = "https://api.upstox.com"
ALL_SYMS = list(NIFTY50.keys())

# ═══════════════════════════════════════════════════════════════
#  RATE LIMITER (persists in session_state)
# ═══════════════════════════════════════════════════════════════
def get_rate_limiter():
    if "rl_timestamps" not in st.session_state:
        st.session_state.rl_timestamps = deque()
        st.session_state.rl_total = 0
        st.session_state.rl_throttled = 0
    return st.session_state

def rate_acquire(max_per_min=50):
    s = get_rate_limiter()
    now = time.time()
    while s.rl_timestamps and s.rl_timestamps[0] < now - 60:
        s.rl_timestamps.popleft()
    if len(s.rl_timestamps) < max_per_min:
        s.rl_timestamps.append(now)
        s.rl_total += 1
        return True
    s.rl_throttled += 1
    wait = s.rl_timestamps[0] + 60 - now
    time.sleep(min(wait + 0.1, 2.0))
    return rate_acquire(max_per_min)

def rate_window():
    s = get_rate_limiter()
    now = time.time()
    while s.rl_timestamps and s.rl_timestamps[0] < now - 60:
        s.rl_timestamps.popleft()
    return len(s.rl_timestamps)

# ═══════════════════════════════════════════════════════════════
#  CACHE (session_state)
# ═══════════════════════════════════════════════════════════════
def cache_get(key, ttl=12):
    c = st.session_state.get("cache", {})
    if key in c:
        ts, data = c[key]
        if time.time() - ts < ttl:
            return data
    return None

def cache_set(key, data):
    if "cache" not in st.session_state:
        st.session_state.cache = {}
    st.session_state.cache[key] = (time.time(), data)

# ═══════════════════════════════════════════════════════════════
#  BACKOFF (session_state)
# ═══════════════════════════════════════════════════════════════
def backoff_check(ep):
    bo = st.session_state.get("backoff", {})
    if ep in bo:
        fails, last, wait = bo[ep]
        if time.time() - last < wait:
            return True
    return False

def backoff_fail(ep):
    bo = st.session_state.setdefault("backoff", {})
    if ep in bo:
        fails, _, prev = bo[ep]
        bo[ep] = (fails + 1, time.time(), min(prev * 2, 60))
    else:
        bo[ep] = (1, time.time(), 5)

def backoff_ok(ep):
    bo = st.session_state.get("backoff", {})
    bo.pop(ep, None)

# ═══════════════════════════════════════════════════════════════
#  UPSTOX API CALLER
# ═══════════════════════════════════════════════════════════════
def upstox_get(url, token, ttl=12, label=""):
    ck = f"{url}|{token[:8]}"
    cached = cache_get(ck, ttl)
    if cached is not None:
        return cached, True

    ep = url.split("?")[0].replace(UPSTOX_BASE, "")
    if backoff_check(ep):
        raise Exception(f"Backing off {ep}")

    rate_acquire()
    headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": f"Bearer {token}"}
    try:
        r = http_req.get(url, headers=headers, timeout=12)
        if r.status_code == 429:
            backoff_fail(ep)
            raise Exception("429 Rate limited")
        if r.status_code >= 500:
            backoff_fail(ep)
            raise Exception(f"Server {r.status_code}")
        r.raise_for_status()
        data = r.json()
        backoff_ok(ep)
        cache_set(ck, data)
        return data, False
    except http_req.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code in (401, 403):
            raise Exception(f"Auth error {e.response.status_code}")
        backoff_fail(ep)
        raise
    except (http_req.exceptions.Timeout, http_req.exceptions.ConnectionError) as e:
        backoff_fail(ep)
        raise Exception(f"Network error: {e}")

# ═══════════════════════════════════════════════════════════════
#  COMPUTATION HELPERS
# ═══════════════════════════════════════════════════════════════
def compute_pcp(c, p, s, k, d, r=0.065):
    T = max(d, 1) / 365
    v = (c - p) - (s - k * math.exp(-r * T))
    return {"violation": round(v, 4), "pct": round(v / s * 100, 4) if s else 0}

def days_to_expiry(exp_str):
    try:
        exp = datetime.strptime(exp_str, "%Y-%m-%d").replace(hour=15, minute=30)
        return max(0, (exp - datetime.now()).days + 1)
    except:
        return 7

def auto_expiries():
    today = datetime.now()
    exps = []
    d = today
    while d.weekday() != 3:
        d += timedelta(days=1)
    if today.weekday() == 3 and today.hour >= 15 and today.minute >= 30:
        d += timedelta(days=7)
    for _ in range(6):
        exps.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=7)
    m, y = today.month, today.year
    for _ in range(3):
        ld = datetime(y, m + 1, 1) - timedelta(days=1) if m < 12 else datetime(y + 1, 1, 1) - timedelta(days=1)
        while ld.weekday() != 3:
            ld -= timedelta(days=1)
        s = ld.strftime("%Y-%m-%d")
        if s not in exps and ld > today:
            exps.append(s)
        m += 1
        if m > 12: m = 1; y += 1
    return sorted(set(exps))

def process_chain(sym, rows, expiry):
    if not rows:
        return None
    spot = rows[0].get("underlying_spot_price", 0)
    atm = min(rows, key=lambda r: abs(r["strike_price"] - spot))["strike_price"]
    d = days_to_expiry(expiry)
    chain = []
    for r in rows:
        K = r["strike_price"]
        cm = (r.get("call_options") or {}).get("market_data") or {}
        pm = (r.get("put_options") or {}).get("market_data") or {}
        cg = (r.get("call_options") or {}).get("option_greeks") or {}
        pg = (r.get("put_options") or {}).get("option_greeks") or {}
        cl, pl = cm.get("ltp", 0) or 0, pm.get("ltp", 0) or 0
        co, po = cm.get("oi", 0) or 0, pm.get("oi", 0) or 0
        p = compute_pcp(cl, pl, spot, K, d)
        chain.append({
            "Strike": K, "Spot": spot, "Call LTP": cl, "Put LTP": pl,
            "Call OI": co, "Put OI": po,
            "Call OI Chg": co - (cm.get("prev_oi", 0) or 0),
            "Put OI Chg": po - (pm.get("prev_oi", 0) or 0),
            "Call Vol": cm.get("volume", 0) or 0,
            "Put Vol": pm.get("volume", 0) or 0,
            "PCP ₹": p["violation"], "PCP %": p["pct"],
            "Call IV": cg.get("iv", 0) or 0, "Put IV": pg.get("iv", 0) or 0,
            "PCR": round(po / max(co, 1), 4),
        })
    tc = sum(c["Call OI"] for c in chain)
    tp = sum(c["Put OI"] for c in chain)
    tcc = sum(c["Call OI Chg"] for c in chain)
    tpc = sum(c["Put OI Chg"] for c in chain)
    mv = max(chain, key=lambda c: abs(c["PCP %"])) if chain else chain[0]
    pcr = round(tp / max(tc, 1), 4)
    atm_row = next((c for c in chain if c["Strike"] == atm), chain[len(chain) // 2])
    sig = "NEUTRAL"
    if pcr > 1.3 and tpc > 5000: sig = "BULLISH"
    elif pcr < 0.7 and tcc > 5000: sig = "BEARISH"
    elif abs(mv["PCP %"]) > 0.5: sig = "ARBITRAGE"
    return {
        "Symbol": sym, "Spot": spot, "ATM": atm, "IV": atm_row.get("Call IV", 0),
        "PCR": pcr, "Call OI": tc, "Put OI": tp,
        "Call OI Chg": tcc, "Put OI Chg": tpc,
        "Max PCP %": mv["PCP %"], "Max PCP ₹": mv["PCP ₹"],
        "Violation Strike": mv["Strike"], "Signal": sig,
        "chain": chain, "expiry": expiry,
    }

# ═══════════════════════════════════════════════════════════════
#  SIMULATION
# ═══════════════════════════════════════════════════════════════
def sim_stock(sym):
    h = sum(ord(c) for c in sym)
    sp = 500 + (h % 4500) + (random.random() - 0.5) * 50
    atm = round(sp / 50) * 50
    dt, iv = 7 + random.randint(0, 20), 15 + random.random() * 35
    chain = []
    for i in range(-5, 6):
        K = atm + i * 50
        cP = max(0.05, max(sp - K, 0) + sp * 0.02 * (1 + random.random() * 0.5))
        pP = max(0.05, max(K - sp, 0) + sp * 0.02 * (1 + random.random() * 0.5))
        p = compute_pcp(cP, pP, sp, K, dt)
        cO, pO = random.randint(5000, 55000), random.randint(5000, 55000)
        chain.append({
            "Strike": K, "Spot": sp, "Call LTP": round(cP, 2), "Put LTP": round(pP, 2),
            "Call OI": cO, "Put OI": pO,
            "Call OI Chg": random.randint(-12000, 15000), "Put OI Chg": random.randint(-12000, 15000),
            "Call Vol": random.randint(1000, 20000), "Put Vol": random.randint(1000, 20000),
            "PCP ₹": p["violation"], "PCP %": p["pct"],
            "Call IV": round(iv + (random.random() - 0.5) * 10, 2),
            "Put IV": round(iv + (random.random() - 0.5) * 10, 2),
            "PCR": round(pO / max(cO, 1), 4),
        })
    tc = sum(c["Call OI"] for c in chain); tp = sum(c["Put OI"] for c in chain)
    tcc = sum(c["Call OI Chg"] for c in chain); tpc = sum(c["Put OI Chg"] for c in chain)
    mv = max(chain, key=lambda c: abs(c["PCP %"]))
    pcr = round(tp / max(tc, 1), 4)
    sig = "NEUTRAL"
    if pcr > 1.3 and tpc > 5000: sig = "BULLISH"
    elif pcr < 0.7 and tcc > 5000: sig = "BEARISH"
    elif abs(mv["PCP %"]) > 0.5: sig = "ARBITRAGE"
    return {
        "Symbol": sym, "Spot": round(sp, 2), "ATM": atm, "IV": round(iv, 1),
        "PCR": pcr, "Call OI": tc, "Put OI": tp,
        "Call OI Chg": tcc, "Put OI Chg": tpc,
        "Max PCP %": mv["PCP %"], "Max PCP ₹": mv["PCP ₹"],
        "Violation Strike": mv["Strike"], "Signal": sig,
        "chain": chain, "expiry": "sim",
    }

def sim_nifty_oi(expiry):
    sp = 24200 + (random.random() - 0.5) * 400
    atm = round(sp / 50) * 50
    chain = []
    for i in range(-15, 16):
        K = atm + i * 50
        d = abs(i)
        cO = int((random.random() * 80000 + 20000) * (1 + d * 0.1))
        pO = int((random.random() * 80000 + 20000) * (1 + d * 0.08))
        chain.append({
            "Strike": K, "Call OI": cO, "Put OI": pO,
            "Call OI Chg": random.randint(-20000, 20000),
            "Put OI Chg": random.randint(-20000, 20000),
            "Call Vol": random.randint(10000, 100000),
            "Put Vol": random.randint(10000, 100000),
            "Call LTP": round(max(0.05, max(sp - K, 0) + 100 * random.random()), 2),
            "Put LTP": round(max(0.05, max(K - sp, 0) + 100 * random.random()), 2),
            "Call IV": round(12 + random.random() * 20, 1),
            "Put IV": round(12 + random.random() * 20, 1),
            "PCR": round(pO / max(cO, 1), 4),
        })
    tc = sum(c["Call OI"] for c in chain); tp = sum(c["Put OI"] for c in chain)
    mc = max(chain, key=lambda c: c["Call OI"])
    mp = max(chain, key=lambda c: c["Put OI"])
    return {"spot": round(sp, 2), "chain": chain, "totalCallOI": tc, "totalPutOI": tp,
            "pcr": round(tp / max(tc, 1), 4), "maxCallStrike": mc, "maxPutStrike": mp, "expiry": expiry}

# ═══════════════════════════════════════════════════════════════
#  LIVE FETCH (staggered)
# ═══════════════════════════════════════════════════════════════
def fetch_live_data(token, expiry, symbols, progress_bar=None):
    results, errors = [], []
    total = len(symbols) + 1
    done = 0

    # 1. Nifty Index OI
    noi = None
    try:
        url = f"{UPSTOX_BASE}/v2/option/chain?instrument_key={NIFTY_INDEX_KEY}&expiry_date={expiry}"
        data, _ = upstox_get(url, token, 12, "NIFTY_OI")
        if data.get("status") == "success" and data.get("data"):
            rows = data["data"]
            spot = rows[0].get("underlying_spot_price", 0) if rows else 0
            chain = []
            for r in rows:
                cm = (r.get("call_options") or {}).get("market_data") or {}
                pm = (r.get("put_options") or {}).get("market_data") or {}
                cg = (r.get("call_options") or {}).get("option_greeks") or {}
                pg = (r.get("put_options") or {}).get("option_greeks") or {}
                co, po = cm.get("oi", 0) or 0, pm.get("oi", 0) or 0
                chain.append({
                    "Strike": r["strike_price"], "Call OI": co, "Put OI": po,
                    "Call OI Chg": co - (cm.get("prev_oi", 0) or 0),
                    "Put OI Chg": po - (pm.get("prev_oi", 0) or 0),
                    "Call Vol": cm.get("volume", 0) or 0, "Put Vol": pm.get("volume", 0) or 0,
                    "Call LTP": cm.get("ltp", 0) or 0, "Put LTP": pm.get("ltp", 0) or 0,
                    "Call IV": cg.get("iv", 0) or 0, "Put IV": pg.get("iv", 0) or 0,
                    "PCR": round(po / max(co, 1), 4),
                })
            tc = sum(c["Call OI"] for c in chain); tp = sum(c["Put OI"] for c in chain)
            mc = max(chain, key=lambda c: c["Call OI"]) if chain else {}
            mp = max(chain, key=lambda c: c["Put OI"]) if chain else {}
            noi = {"spot": spot, "chain": chain, "totalCallOI": tc, "totalPutOI": tp,
                   "pcr": round(tp / max(tc, 1), 4), "maxCallStrike": mc, "maxPutStrike": mp, "expiry": expiry}
    except Exception as e:
        errors.append(f"NIFTY_OI: {e}")
    done += 1
    if progress_bar:
        progress_bar.progress(done / total, f"Nifty OI fetched. Scanning {len(symbols)} stocks...")

    # 2. Staggered stock fetch (5 per batch, 2s delay)
    BATCH = 5
    for i in range(0, len(symbols), BATCH):
        batch = symbols[i:i + BATCH]
        for sym in batch:
            ik = NIFTY50.get(sym)
            if not ik:
                errors.append(f"{sym}: no key"); done += 1; continue
            try:
                url = f"{UPSTOX_BASE}/v2/option/chain?instrument_key={ik}&expiry_date={expiry}"
                data, cached = upstox_get(url, token, 12, sym)
                if data.get("status") == "success" and data.get("data"):
                    p = process_chain(sym, data["data"], expiry)
                    if p:
                        results.append(p)
                else:
                    errors.append(f"{sym}: empty")
            except Exception as e:
                errors.append(f"{sym}: {e}")
            done += 1
            if progress_bar:
                progress_bar.progress(done / total, f"{done}/{total} — {sym}")
        if i + BATCH < len(symbols):
            time.sleep(2.0)

    return results, noi, errors

# ═══════════════════════════════════════════════════════════════
#  PLOTLY THEME
# ═══════════════════════════════════════════════════════════════
PLT_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="#060a10",
    plot_bgcolor="#0b1018",
    font=dict(family="DM Mono, monospace", color="#b0bac8"),
    margin=dict(l=40, r=20, t=40, b=40),
)
GREEN = "#00e676"
RED = "#ff1744"
YELLOW = "#ffd600"
BLUE = "#448aff"

# ═══════════════════════════════════════════════════════════════
#  SIDEBAR
# ═══════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## Σ PCP & OI Scanner")

    mode = st.radio("Mode", ["Simulated", "Live (Upstox API)"], horizontal=True)
    is_live = mode == "Live (Upstox API)"

    token = ""
    if is_live:
        token = st.text_input("Upstox Access Token", type="password", placeholder="Paste token...")
        if not token:
            st.warning("Paste your Upstox access_token to go live.")

    expiries = auto_expiries()
    expiry = st.selectbox("Expiry", expiries, format_func=lambda x: f"{x}  ({days_to_expiry(x)}d)")

    st.markdown("---")
    st.markdown("**Symbols to Scan**")
    preset = st.radio("Preset", ["Top 10", "All 50", "Custom"], horizontal=True)
    if preset == "Top 10":
        symbols = ALL_SYMS[:10]
    elif preset == "All 50":
        symbols = ALL_SYMS
    else:
        symbols = st.multiselect("Pick symbols", ALL_SYMS, default=ALL_SYMS[:10])

    st.markdown("---")
    vt = st.slider("PCP Violation Threshold %", 0.1, 1.5, 0.3, 0.05)

    st.markdown("---")
    st.markdown("**Rate Limiting**")
    st.caption(f"API calls in window: {rate_window()}/50/min")
    st.caption(f"Total calls: {st.session_state.get('rl_total', 0)} | Throttled: {st.session_state.get('rl_throttled', 0)}")
    bo = st.session_state.get("backoff", {})
    if bo:
        active = {k: v for k, v in bo.items() if time.time() - v[1] < v[2]}
        if active:
            st.caption(f"⚠ Backoffs: {len(active)}")

    scan_btn = st.button("🔄 Scan Now", use_container_width=True, type="primary")

# ═══════════════════════════════════════════════════════════════
#  MAIN — DATA FETCH
# ═══════════════════════════════════════════════════════════════
if scan_btn or "data" not in st.session_state:
    if is_live and token and HAS_REQUESTS:
        progress = st.progress(0, "Starting live scan...")
        results, noi, errors = fetch_live_data(token, expiry, symbols, progress)
        progress.empty()
        st.session_state.data = results
        st.session_state.noi = noi
        st.session_state.errors = errors
        st.session_state.last_update = datetime.now()
    else:
        st.session_state.data = [sim_stock(s) for s in symbols]
        st.session_state.noi = sim_nifty_oi(expiry)
        st.session_state.errors = []
        st.session_state.last_update = datetime.now()

data = st.session_state.get("data", [])
noi = st.session_state.get("noi", None)
errors = st.session_state.get("errors", [])
last_update = st.session_state.get("last_update", None)

# ═══════════════════════════════════════════════════════════════
#  HEADER STATUS
# ═══════════════════════════════════════════════════════════════
hcol1, hcol2 = st.columns([3, 1])
with hcol1:
    badge = "🟢 LIVE" if is_live and token else "🟠 SIMULATED"
    st.markdown(f"### {badge} · {len(data)} stocks · {expiry} ({days_to_expiry(expiry)}d)")
with hcol2:
    if last_update:
        st.caption(f"Updated: {last_update.strftime('%H:%M:%S IST')}")

if errors:
    with st.expander(f"⚠ {len(errors)} error(s)", expanded=False):
        for e in errors[:10]:
            st.caption(f"• {e}")

# ═══════════════════════════════════════════════════════════════
#  STATS ROW
# ═══════════════════════════════════════════════════════════════
if data:
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    bull = len([d for d in data if d["Signal"] == "BULLISH"])
    bear = len([d for d in data if d["Signal"] == "BEARISH"])
    arb = len([d for d in data if abs(d["Max PCP %"]) > vt])
    avg_pcr = sum(d["PCR"] for d in data) / len(data)
    avg_iv = sum(d["IV"] for d in data) / len(data)
    npcr = noi["pcr"] if noi else 0

    c1.metric("▲ Bullish", bull)
    c2.metric("▼ Bearish", bear)
    c3.metric("⚡ Arb Alerts", arb)
    c4.metric("◎ Avg PCR", f"{avg_pcr:.2f}")
    c5.metric("σ Avg IV", f"{avg_iv:.1f}%")
    c6.metric("Ⓝ Nifty PCR", f"{npcr:.2f}")

# ═══════════════════════════════════════════════════════════════
#  TABS
# ═══════════════════════════════════════════════════════════════
tab_scan, tab_heat, tab_noi = st.tabs(["📊 Scanner", "🔥 Heatmap", "📈 Nifty Index OI"])

# ─── SCANNER TAB ────────────────────────────────────────────────
with tab_scan:
    if data:
        fc1, fc2 = st.columns([1, 3])
        with fc1:
            sig_filter = st.selectbox("Signal", ["ALL", "BULLISH", "BEARISH", "ARBITRAGE"])
        with fc2:
            search = st.text_input("Search symbol", "", placeholder="e.g. RELIANCE")

        filtered = data
        if sig_filter != "ALL":
            if sig_filter == "ARBITRAGE":
                filtered = [d for d in filtered if d["Signal"] == "ARBITRAGE" or abs(d["Max PCP %"]) > vt]
            else:
                filtered = [d for d in filtered if d["Signal"] == sig_filter]
        if search:
            filtered = [d for d in filtered if search.upper() in d["Symbol"].upper()]

        df = pd.DataFrame([{
            "Symbol": d["Symbol"], "Spot ₹": f"{d['Spot']:.2f}", "IV %": f"{d['IV']:.1f}",
            "PCR": f"{d['PCR']:.2f}", "Call OI": d["Call OI"], "Put OI": d["Put OI"],
            "Call Δ": d["Call OI Chg"], "Put Δ": d["Put OI Chg"],
            "PCP %": f"{d['Max PCP %']:+.3f}", "@ Strike": d["Violation Strike"],
            "Signal": d["Signal"],
        } for d in filtered])

        if not df.empty:
            st.dataframe(
                df, use_container_width=True, hide_index=True,
                column_config={
                    "Signal": st.column_config.TextColumn(width="small"),
                    "Call OI": st.column_config.NumberColumn(format="%d"),
                    "Put OI": st.column_config.NumberColumn(format="%d"),
                }
            )

            # Drilldown
            sel_sym = st.selectbox("Drill into option chain", ["—"] + [d["Symbol"] for d in filtered])
            if sel_sym != "—":
                stock = next((d for d in filtered if d["Symbol"] == sel_sym), None)
                if stock and stock.get("chain"):
                    st.markdown(f"### {stock['Symbol']} · Spot ₹{stock['Spot']:.2f} · ATM ₹{stock['ATM']} · IV {stock['IV']:.1f}%")

                    chain_df = pd.DataFrame(stock["chain"])
                    # OI Chart
                    fig = make_subplots(rows=1, cols=2, subplot_titles=("OI Distribution", "PCP Violation %"))
                    fig.add_trace(go.Bar(x=chain_df["Strike"], y=chain_df["Call OI"], name="Call OI", marker_color=GREEN, opacity=0.7), row=1, col=1)
                    fig.add_trace(go.Bar(x=chain_df["Strike"], y=chain_df["Put OI"], name="Put OI", marker_color=RED, opacity=0.7), row=1, col=1)
                    colors = [GREEN if v > 0 else RED for v in chain_df["PCP %"]]
                    fig.add_trace(go.Bar(x=chain_df["Strike"], y=chain_df["PCP %"], name="PCP %", marker_color=colors), row=1, col=2)
                    fig.update_layout(**PLT_LAYOUT, height=350, barmode="group", showlegend=True,
                                      legend=dict(orientation="h", y=-0.15))
                    st.plotly_chart(fig, use_container_width=True)

                    # Chain table
                    display_cols = ["Strike", "Call OI", "Call OI Chg", "Call Vol", "Call LTP", "Call IV",
                                    "Put LTP", "Put IV", "Put Vol", "Put OI Chg", "Put OI", "PCP ₹", "PCP %"]
                    st.dataframe(chain_df[display_cols], use_container_width=True, hide_index=True)
        else:
            st.info("No stocks match the current filter.")
    else:
        st.info("Click **Scan Now** to load data.")

# ─── HEATMAP TAB ────────────────────────────────────────────────
with tab_heat:
    if data:
        hm_data = sorted(data, key=lambda d: abs(d["Max PCP %"]), reverse=True)
        syms = [d["Symbol"] for d in hm_data]
        pcts = [d["Max PCP %"] for d in hm_data]
        colors_hm = [GREEN if p > 0 else RED for p in pcts]

        fig = go.Figure(go.Bar(
            x=syms, y=pcts,
            marker_color=colors_hm, marker_opacity=0.8,
            text=[f"{p:+.3f}%" for p in pcts],
            textposition="outside", textfont=dict(size=9),
        ))
        fig.update_layout(**PLT_LAYOUT, height=500, title="PCP Violation % — Nifty 50",
                          yaxis_title="PCP %", xaxis_tickangle=-45)
        fig.add_hline(y=vt, line_dash="dash", line_color=YELLOW, annotation_text=f"Threshold {vt}%")
        fig.add_hline(y=-vt, line_dash="dash", line_color=YELLOW)
        st.plotly_chart(fig, use_container_width=True)

        # PCR heatmap
        pcr_vals = [d["PCR"] for d in hm_data]
        fig2 = go.Figure(go.Bar(
            x=syms, y=pcr_vals,
            marker_color=[GREEN if p > 1 else RED if p < 0.8 else BLUE for p in pcr_vals],
            marker_opacity=0.8,
        ))
        fig2.update_layout(**PLT_LAYOUT, height=350, title="PCR by Symbol",
                           yaxis_title="PCR", xaxis_tickangle=-45)
        fig2.add_hline(y=1.0, line_dash="dash", line_color="#ffffff33", annotation_text="PCR = 1.0")
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("Click **Scan Now** to load data.")

# ─── NIFTY INDEX OI TAB ────────────────────────────────────────
with tab_noi:
    if noi:
        nc1, nc2, nc3, nc4, nc5, nc6 = st.columns(6)
        nc1.metric("Spot", f"₹{noi['spot']:.2f}")
        nc2.metric("Total Call OI", f"{noi['totalCallOI']:,}")
        nc3.metric("Total Put OI", f"{noi['totalPutOI']:,}")
        nc4.metric("PCR (OI)", f"{noi['pcr']:.2f}")
        nc5.metric("Max Call OI @", f"₹{noi['maxCallStrike'].get('Strike', '—')}")
        nc6.metric("Max Put OI @", f"₹{noi['maxPutStrike'].get('Strike', '—')}")

        ndf = pd.DataFrame(noi["chain"])

        # OI Bar chart
        fig = go.Figure()
        fig.add_trace(go.Bar(x=ndf["Strike"], y=ndf["Call OI"], name="Call OI", marker_color=GREEN, opacity=0.6))
        fig.add_trace(go.Bar(x=ndf["Strike"], y=ndf["Put OI"], name="Put OI", marker_color=RED, opacity=0.6))
        fig.update_layout(**PLT_LAYOUT, height=400, barmode="group",
                          title=f"Nifty 50 Option Chain OI · {noi['expiry']}",
                          xaxis_title="Strike", yaxis_title="Open Interest",
                          legend=dict(orientation="h", y=-0.15))
        # ATM marker
        atm_strike = round(noi["spot"] / 50) * 50
        fig.add_vline(x=atm_strike, line_dash="dash", line_color=YELLOW, annotation_text="ATM")
        st.plotly_chart(fig, use_container_width=True)

        # OI Change chart
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(x=ndf["Strike"], y=ndf["Call OI Chg"], name="Call OI Δ",
                              marker_color=[GREEN if v > 0 else "rgba(0,230,118,0.3)" for v in ndf["Call OI Chg"]]))
        fig2.add_trace(go.Bar(x=ndf["Strike"], y=ndf["Put OI Chg"], name="Put OI Δ",
                              marker_color=[RED if v > 0 else "rgba(255,23,68,0.3)" for v in ndf["Put OI Chg"]]))
        fig2.update_layout(**PLT_LAYOUT, height=350, barmode="group",
                           title="OI Change (Today vs Previous)", xaxis_title="Strike", yaxis_title="OI Change",
                           legend=dict(orientation="h", y=-0.15))
        st.plotly_chart(fig2, use_container_width=True)

        # Full table
        st.markdown("#### Full Chain Data")
        st.dataframe(ndf, use_container_width=True, hide_index=True)
    else:
        st.info("Click **Scan Now** to load Nifty OI data.")

# ═══════════════════════════════════════════════════════════════
#  FOOTER
# ═══════════════════════════════════════════════════════════════
st.markdown("---")
st.caption(
    f"**PCP:** C − P = S − K·e⁻ʳᵀ · **Expiry:** Auto-computed Thursdays from {datetime.now().strftime('%d/%m/%Y')} · "
    f"**Engine:** 50 calls/min bucket · 5 syms/2s stagger · Exp backoff 5→60s · "
    f"{'LIVE via Upstox v2+v3' if is_live and token else 'Simulated data'}"
)
