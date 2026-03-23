# -*- coding: utf-8 -*-
"""
Operational Market Dashboard Generator
- output: docs/index.html, docs/state.json
- tabs: MAIN / US / KR / NEWS
- MAIN 12 cards:
  S&P500, Nasdaq100, KOSPI, KOSDAQ,
  VIX, VKOSPI, US10Y, 2Y-10Y,
  USD/KRW, Dollar Index, Gold, HY OAS

필요 패키지:
pip install pandas numpy requests yfinance
"""

import os
import io
import json
import math
import time
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import requests
import yfinance as yf

warnings.filterwarnings("ignore")

# =========================================================
# Config
# =========================================================
KST = timezone(timedelta(hours=9))

DOCS_DIR = "docs"
INDEX_FILE = os.path.join(DOCS_DIR, "index.html")
STATE_FILE = os.path.join(DOCS_DIR, "state.json")
KRX_CACHE_FILE = os.path.join(DOCS_DIR, "krx_cache.json")

YF_PERIOD = "2y"
REQUEST_TIMEOUT = 20
USE_AUTO_ADJUST = True

# yfinance tickers
TICKERS = {
    "SPY": "SPY",
    "QQQ": "QQQ",
    "VIX": "^VIX",
    "KOSPI": "^KS11",
    "KOSDAQ": "^KQ11",
    "EWY": "EWY",
    "DXY": "DX-Y.NYB",
    "GOLD": "GC=F",
    "WTI": "CL=F",
    "USDKRW": "KRW=X",    # USD/KRW proxy
    "RSP": "RSP",         # breadth proxy for SPY
    "QQEW": "QQEW",       # breadth proxy for QQQ
}

FRED_SERIES = {
    "DGS10": "DGS10",
    "DGS2": "DGS2",
    "HY_OAS": "BAMLH0A0HYM2",
}

os.makedirs(DOCS_DIR, exist_ok=True)

# =========================================================
# Data classes
# =========================================================
@dataclass
class AssetResult:
    asset: str
    total_score: int
    signal: str
    original_signal: str
    module_scores: Dict[str, int]
    module_meta: Dict[str, dict]
    guardrail_reasons: List[str]

# =========================================================
# Time / I/O helpers
# =========================================================
def now_kst() -> datetime:
    return datetime.now(KST)

def fmt_ts_kst(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = now_kst()
    return dt.strftime("%Y-%m-%d %H:%M KST")

def read_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def write_json(path: str, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

# =========================================================
# Generic helpers
# =========================================================
def safe_float(x, default=np.nan):
    try:
        if x is None:
            return default
        if isinstance(x, str) and x.strip() == "":
            return default
        return float(x)
    except Exception:
        return default

def pct_change(curr, prev):
    curr = safe_float(curr)
    prev = safe_float(prev)
    if pd.isna(curr) or pd.isna(prev) or prev == 0:
        return np.nan
    return (curr / prev - 1.0) * 100.0

def bp_change(curr, prev):
    curr = safe_float(curr)
    prev = safe_float(prev)
    if pd.isna(curr) or pd.isna(prev):
        return np.nan
    return (curr - prev) * 100.0

def last_valid(series: pd.Series, n: int = 1):
    s = series.dropna()
    if len(s) < n:
        return np.nan
    return s.iloc[-n]

def rolling_mean(series: pd.Series, n: int):
    return series.rolling(n).mean()

def sign_label(score: int) -> str:
    if score >= 70:
        return "매수"
    elif score >= 40:
        return "보유"
    return "매도"

def fmt_num(v, nd=2):
    v = safe_float(v)
    if pd.isna(v):
        return "—"
    return f"{v:,.{nd}f}"

def fmt_pct(v, nd=2):
    v = safe_float(v)
    if pd.isna(v):
        return "—"
    return f"{v:+.{nd}f}%"

def fmt_bp(v, nd=1):
    v = safe_float(v)
    if pd.isna(v):
        return "—"
    return f"{v:+.{nd}f}bp"

def fmt_bil_krw(v):
    """억/조 단위 표시"""
    v = safe_float(v)
    if pd.isna(v):
        return "—"
    sign = "+" if v > 0 else ""
    abs_v = abs(v)
    if abs_v >= 10000:
        return f"{sign}{v/10000:.1f}조"
    return f"{sign}{v:,.0f}억"

def rgba_hex(hex_color: str, alpha: float) -> str:
    h = hex_color.replace("#", "")
    if len(h) != 6:
        return f"rgba(255,255,255,{alpha})"
    r = int(h[0:2], 16)
    g = int(h[2:4], 16)
    b = int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"

# =========================================================
# Sanity check / fallback
# =========================================================
def validate_range(name: str, value) -> bool:
    value = safe_float(value)
    if pd.isna(value):
        return False

    ranges = {
        "SPY": (50, 2000),
        "QQQ": (50, 2000),
        "KOSPI": (500, 10000),
        "KOSDAQ": (100, 3000),
        "VIX": (0, 200),
        "VKOSPI": (0, 200),
        "US10Y": (0, 10),
        "CURVE_2S10S": (-10, 10),
        "USDKRW": (500, 3000),
        "DXY": (70, 130),
        "GOLD": (500, 10000),
        "HY_OAS": (0, 25),
        "WTI": (-20, 300),
    }
    lo, hi = ranges.get(name, (-1e18, 1e18))
    return lo <= value <= hi

def validate_pct_jump(name: str, current, previous) -> bool:
    current = safe_float(current)
    previous = safe_float(previous)
    if pd.isna(current) or pd.isna(previous) or previous == 0:
        return True

    pct = abs((current / previous - 1.0) * 100.0)
    limits = {
        "SPY": 8.0,
        "QQQ": 10.0,
        "KOSPI": 8.0,
        "KOSDAQ": 10.0,
        "VIX": 80.0,
        "VKOSPI": 80.0,
        "USDKRW": 5.0,
        "DXY": 3.0,
        "GOLD": 6.0,
        "WTI": 20.0,
    }
    return pct <= limits.get(name, 20.0)

def validate_bp_jump(name: str, current, previous) -> bool:
    current = safe_float(current)
    previous = safe_float(previous)
    if pd.isna(current) or pd.isna(previous):
        return True

    bp = abs((current - previous) * 100.0)
    limits = {
        "US10Y": 40.0,
        "CURVE_2S10S": 60.0,
        "HY_OAS": 100.0,
    }
    return bp <= limits.get(name, 1000.0)

def sanitize_series_point(name: str, current, previous_good=None, bp_metric=False):
    """
    returns: value, ok_flag, msg
    """
    if not validate_range(name, current):
        return previous_good, False, f"{name}: range_fail"

    if previous_good is not None and not pd.isna(previous_good):
        ok = validate_bp_jump(name, current, previous_good) if bp_metric else validate_pct_jump(name, current, previous_good)
        if not ok:
            return previous_good, False, f"{name}: jump_fail"

    return current, True, ""

# =========================================================
# Fetchers
# =========================================================
def safe_download_yf(ticker: str, period="2y", auto_adjust=True, retries=3) -> pd.DataFrame:
    for _ in range(retries):
        try:
            df = yf.download(
                tickers=ticker,
                period=period,
                interval="1d",
                auto_adjust=auto_adjust,
                progress=False,
                threads=False,
            )
            if df is None or df.empty:
                time.sleep(1)
                continue
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            return df
        except Exception:
            time.sleep(1)
    return pd.DataFrame()

def fetch_yf_all() -> Dict[str, pd.DataFrame]:
    out = {}
    for key, ticker in TICKERS.items():
        out[key] = safe_download_yf(ticker, period=YF_PERIOD, auto_adjust=USE_AUTO_ADJUST)
    return out

def fetch_fred_series(series_id: str) -> pd.Series:
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    if len(df.columns) < 2:
        return pd.Series(dtype=float)
    df.columns = ["DATE", series_id]
    df["DATE"] = pd.to_datetime(df["DATE"])
    df[series_id] = pd.to_numeric(df[series_id], errors="coerce")
    return df.set_index("DATE")[series_id].dropna()

def fetch_fred_all() -> Dict[str, pd.Series]:
    out = {}
    for key, sid in FRED_SERIES.items():
        try:
            out[key] = fetch_fred_series(sid)
        except Exception:
            out[key] = pd.Series(dtype=float)
    return out

# =========================================================
# KRX cache
# =========================================================
def read_krx_cache() -> dict:
    return read_json(KRX_CACHE_FILE, {})

def get_cache_section(cache: dict, key: str) -> dict:
    if not isinstance(cache, dict):
        return {}
    return cache.get(key, {}) if isinstance(cache.get(key, {}), dict) else {}

# =========================================================
# Scoring modules - US
# =========================================================
def score_trend_us(close: pd.Series, asset: str) -> Tuple[int, dict]:
    c = last_valid(close)
    ma50 = last_valid(rolling_mean(close, 50))
    ma200 = last_valid(rolling_mean(close, 200))
    prev20 = last_valid(close, 21)
    ret20 = pct_change(c, prev20)

    max_score = 35 if asset == "SPY" else 30
    score = 0
    if safe_float(c, np.nan) > safe_float(ma50, np.inf):
        score += 15 if asset == "SPY" else 12
    if safe_float(c, np.nan) > safe_float(ma200, np.inf):
        score += 15 if asset == "SPY" else 12
    if safe_float(ret20, -999) > 0:
        score += 5 if asset == "SPY" else 6

    return int(score), {
        "close": safe_float(c),
        "ma50": safe_float(ma50),
        "ma200": safe_float(ma200),
        "ret20": safe_float(ret20),
        "max_score": max_score,
    }

def score_vix(vix_close: pd.Series) -> Tuple[int, dict]:
    vix = last_valid(vix_close)
    prev5 = last_valid(vix_close, 6)
    chg5 = pct_change(vix, prev5)
    ma20 = last_valid(rolling_mean(vix_close, 20))
    ratio20 = vix / ma20 if not pd.isna(ma20) and ma20 != 0 else np.nan
    high10 = vix_close.dropna().tail(10).max() if len(vix_close.dropna()) >= 10 else np.nan
    off10 = pct_change(vix, high10)

    if pd.isna(vix):
        score = 10
    elif vix < 18:
        score = 25
    elif vix < 25:
        score = 15
    else:
        score = 5

    return int(score), {
        "vix": safe_float(vix),
        "vix_5d_chg": safe_float(chg5),
        "vix_ratio20": safe_float(ratio20),
        "off_10d_high": safe_float(off10),
        "max_score": 25,
    }

def score_tactical_us(close: pd.Series) -> Tuple[int, dict]:
    c = last_valid(close)
    ma20 = last_valid(rolling_mean(close, 20))
    prev10 = last_valid(close, 11)
    ret10 = pct_change(c, prev10)
    dist20 = pct_change(c, ma20)

    last5 = close.dropna().tail(5)
    above_5dma = int(len(last5) >= 5 and c > last5.mean())

    last4 = close.dropna().tail(4)
    two_of_three_up = 0
    if len(last4) >= 4:
        diffs = last4.diff().dropna()
        two_of_three_up = int((diffs > 0).tail(3).sum() >= 2)

    score = 0
    if safe_float(dist20, -999) > -5:
        score += 5
    if safe_float(ret10, -999) > -3:
        score += 5
    if above_5dma:
        score += 3
    if two_of_three_up:
        score += 2

    return int(score), {
        "dist20": safe_float(dist20),
        "ret10": safe_float(ret10),
        "above_5dma": above_5dma,
        "two_of_three_up": two_of_three_up,
        "max_score": 15,
    }

def score_breadth_us(proxy_ratio: pd.Series, max_score: int) -> Tuple[int, dict]:
    c = last_valid(proxy_ratio)
    ma20 = last_valid(rolling_mean(proxy_ratio, 20))
    ret20 = pct_change(c, last_valid(proxy_ratio, 21))

    if pd.isna(c) or pd.isna(ma20):
        bucket = "neutral"
        approx = np.nan
        score = max_score // 2
    elif c > ma20 and safe_float(ret20, -999) > 0:
        bucket = "strong"
        approx = 65.0
        score = max_score
    elif c > ma20:
        bucket = "neutral"
        approx = 50.0
        score = max_score // 2 + 1
    else:
        bucket = "weak"
        approx = 35.0
        score = max_score // 3

    return int(score), {
        "bucket": bucket,
        "approx_pct": safe_float(approx),
        "ratio": safe_float(c),
        "ratio_ma20": safe_float(ma20),
        "ret20": safe_float(ret20),
        "max_score": max_score,
    }

def score_rates_us(dgs10: pd.Series, asset: str) -> Tuple[int, dict]:
    cur = last_valid(dgs10)
    prev20 = last_valid(dgs10, 21)
    delta20_bp = bp_change(cur, prev20)

    max_score = 10 if asset == "SPY" else 20
    if pd.isna(delta20_bp):
        score = max_score // 2
    elif asset == "SPY":
        score = 0 if delta20_bp >= 40 else 2 if delta20_bp >= 20 else 8
    else:
        score = 0 if delta20_bp >= 40 else 4 if delta20_bp >= 20 else 12

    return int(score), {
        "dgs10": safe_float(cur),
        "delta20_bp": safe_float(delta20_bp),
        "max_score": max_score,
    }

# =========================================================
# Scoring modules - KR
# =========================================================
def score_trend_kr(close: pd.Series, asset: str) -> Tuple[int, dict]:
    c = last_valid(close)
    ma50 = last_valid(rolling_mean(close, 50))
    ma200 = last_valid(rolling_mean(close, 200))
    prev20 = last_valid(close, 21)
    ret20 = pct_change(c, prev20)

    max_score = 40 if asset == "KOSPI" else 36
    score = 0
    if safe_float(c, np.nan) > safe_float(ma50, np.inf):
        score += 16 if asset == "KOSPI" else 14
    if safe_float(c, np.nan) > safe_float(ma200, np.inf):
        score += 16 if asset == "KOSPI" else 14
    if safe_float(ret20, -999) > 0:
        score += 8

    return int(score), {
        "close": safe_float(c),
        "ma50": safe_float(ma50),
        "ma200": safe_float(ma200),
        "ret20": safe_float(ret20),
        "max_score": max_score,
    }

def score_vkospi(cache: dict) -> Tuple[int, dict]:
    s = get_cache_section(cache, "VKOSPI")
    cur = safe_float(s.get("current"))
    chg5 = safe_float(s.get("chg_5d"))
    ratio20 = safe_float(s.get("ratio20"))
    off10 = safe_float(s.get("off_10d_high"))

    if pd.isna(cur):
        score = 10
    elif cur < 20:
        score = 27
    elif cur < 35:
        score = 22
    elif cur < 50:
        score = 16
    else:
        score = 8

    return int(score), {
        "vkospi": cur,
        "vkospi_5d_chg": chg5,
        "vkospi_ratio20": ratio20,
        "off_10d_high": off10,
        "max_score": 27,
    }

def score_tactical_kr(close: pd.Series) -> Tuple[int, dict]:
    c = last_valid(close)
    ma20 = last_valid(rolling_mean(close, 20))
    prev10 = last_valid(close, 11)
    ret10 = pct_change(c, prev10)
    dist20 = pct_change(c, ma20)

    last5 = close.dropna().tail(5)
    above_5dma = int(len(last5) >= 5 and c > last5.mean())

    last4 = close.dropna().tail(4)
    two_of_three_up = 0
    if len(last4) >= 4:
        diffs = last4.diff().dropna()
        two_of_three_up = int((diffs > 0).tail(3).sum() >= 2)

    score = 0
    if safe_float(dist20, -999) > -5:
        score += 6
    if safe_float(ret10, -999) > -3:
        score += 6
    if above_5dma:
        score += 3
    if two_of_three_up:
        score += 2

    return int(score), {
        "dist20": safe_float(dist20),
        "ret10": safe_float(ret10),
        "above_5dma": above_5dma,
        "two_of_three_up": two_of_three_up,
        "max_score": 17,
    }

def score_leadership_kr(kosdaq_close: pd.Series, kospi_close: pd.Series, asset: str) -> Tuple[int, dict]:
    rel = (kosdaq_close / kospi_close).dropna()
    c = last_valid(rel)
    ma20 = last_valid(rolling_mean(rel, 20))
    ret20 = pct_change(c, last_valid(rel, 21))

    if asset == "KOSPI":
        max_score = 6
        if not pd.isna(c) and not pd.isna(ma20) and c < ma20:
            bucket = "kosdaq_weak"
            approx = 28.0
            score = 6
        else:
            bucket = "neutral"
            approx = 50.0
            score = 3
    else:
        max_score = 12
        if not pd.isna(c) and not pd.isna(ma20) and c > ma20 and safe_float(ret20, -999) > 0:
            bucket = "strong"
            approx = 65.0
            score = 12
        elif not pd.isna(c) and not pd.isna(ma20) and c > ma20:
            bucket = "neutral"
            approx = 50.0
            score = 6
        else:
            bucket = "weak"
            approx = 28.0
            score = 1

    return int(score), {
        "bucket": bucket,
        "approx_pct": safe_float(approx),
        "rel": safe_float(c),
        "rel_ma20": safe_float(ma20),
        "rel_ret20": safe_float(ret20),
        "max_score": max_score,
    }

def score_turnover_kr(asset: str, cache: dict) -> Tuple[int, dict]:
    s = get_cache_section(cache, asset)
    current = safe_float(s.get("turnover_trillion"))
    ma20_ratio = safe_float(s.get("turnover_ma20_ratio"))
    chg5 = safe_float(s.get("turnover_5d_chg"))
    max_score = 12

    if pd.isna(current):
        return 6, {"current": np.nan, "ma20_ratio": np.nan, "chg5": np.nan, "max_score": max_score}

    score = 5
    if not pd.isna(ma20_ratio):
        if ma20_ratio >= 1.05:
            score += 4
        elif ma20_ratio >= 0.95:
            score += 2
    if not pd.isna(chg5):
        if chg5 > 10:
            score += 3
        elif chg5 > -5:
            score += 1

    return int(min(score, max_score)), {
        "current": current,
        "ma20_ratio": ma20_ratio,
        "chg5": chg5,
        "max_score": max_score,
    }

def score_flow_kr(asset: str, cache: dict) -> Tuple[int, dict]:
    s = get_cache_section(cache, asset)
    f1 = safe_float(s.get("foreign_1d_bil"))
    f5 = safe_float(s.get("foreign_5d_bil"))
    f20 = safe_float(s.get("foreign_20d_bil"))
    f5ratio = safe_float(s.get("foreign_5d_ratio"))

    max_score = 20 if asset == "KOSPI" else 18
    score = 0
    if not pd.isna(f1) and f1 > 0:
        score += 5
    if not pd.isna(f5) and f5 > 0:
        score += 7
    if not pd.isna(f20) and f20 > 0:
        score += 6
    if not pd.isna(f5ratio) and f5ratio > 0:
        score += 2

    return int(min(score, max_score)), {
        "foreign_1d_bil": f1,
        "foreign_5d_bil": f5,
        "foreign_20d_bil": f20,
        "foreign_5d_ratio": f5ratio,
        "max_score": max_score,
    }

def score_fx_usdkrw(usdkrw_close: pd.Series) -> Tuple[int, dict]:
    cur = last_valid(usdkrw_close)
    prev20 = last_valid(usdkrw_close, 21)
    ret20 = pct_change(cur, prev20)

    if pd.isna(ret20):
        score = 10
    elif ret20 >= 4:
        score = 3
    elif ret20 >= 2:
        score = 8
    else:
        score = 15

    return int(score), {
        "usdkrw": safe_float(cur),
        "usdkrw_ret20": safe_float(ret20),
        "max_score": 20,
    }

def score_oil_wti(wti_close: pd.Series) -> Tuple[int, dict]:
    cur = last_valid(wti_close)
    prev20 = last_valid(wti_close, 21)
    ret20 = pct_change(cur, prev20)

    if pd.isna(ret20):
        score = 6
    elif ret20 >= 20:
        score = 2
    elif ret20 >= 10:
        score = 5
    else:
        score = 8

    return int(score), {
        "wti": safe_float(cur),
        "wti_ret20": safe_float(ret20),
        "max_score": 10,
    }

# =========================================================
# Result builders
# =========================================================
def build_us_results(mkt: dict, fred: dict) -> Dict[str, AssetResult]:
    out = {}

    spy_close = mkt["SPY"]["Close"].dropna()
    qqq_close = mkt["QQQ"]["Close"].dropna()
    vix_close = mkt["VIX"]["Close"].dropna()
    rsp_close = mkt["RSP"]["Close"].dropna()
    qqew_close = mkt["QQEW"]["Close"].dropna()
    dgs10 = fred["DGS10"].dropna()

    configs = [
        ("SPY", spy_close, (rsp_close / spy_close).dropna(), 15),
        ("QQQ", qqq_close, (qqew_close / qqq_close).dropna(), 10),
    ]

    for asset, close, breadth_proxy, breadth_max in configs:
        trend_score, trend_meta = score_trend_us(close, asset)
        vix_score, vix_meta = score_vix(vix_close)
        tactical_score, tactical_meta = score_tactical_us(close)
        breadth_score, breadth_meta = score_breadth_us(breadth_proxy, breadth_max)
        rates_score, rates_meta = score_rates_us(dgs10, asset)

        module_scores = {
            "trend": trend_score,
            "vix": vix_score,
            "tactical": tactical_score,
            "breadth": breadth_score,
            "rates": rates_score,
        }
        module_meta = {
            "trend": trend_meta,
            "vix": vix_meta,
            "tactical": tactical_meta,
            "breadth": breadth_meta,
            "rates": rates_meta,
        }

        total = int(sum(module_scores.values()))
        original_signal = sign_label(total)
        signal = original_signal
        guardrail = []

        c = safe_float(trend_meta["close"])
        ma50 = safe_float(trend_meta["ma50"])
        ma200 = safe_float(trend_meta["ma200"])
        ret20 = safe_float(trend_meta["ret20"])
        delta20_bp = safe_float(rates_meta["delta20_bp"])

        if not pd.isna(c) and not pd.isna(ma50) and not pd.isna(ma200) and not pd.isna(ret20):
            if c < ma50 and c < ma200 and ret20 < 0:
                signal = "매도"
                guardrail.append("50/200일선 하회 + 20일 수익률 음수 → 매도 우선")

        if asset == "QQQ" and not pd.isna(delta20_bp) and delta20_bp >= 20:
            if signal == "매수":
                signal = "보유"
            elif signal == "보유":
                signal = "매도"
            guardrail.append("10년물 20일 급등 → QQQ 한 단계 하향")

        out[asset] = AssetResult(
            asset=asset,
            total_score=total,
            signal=signal,
            original_signal=original_signal,
            module_scores=module_scores,
            module_meta=module_meta,
            guardrail_reasons=guardrail,
        )

    return out

def build_kr_results(mkt: dict, krx_cache: dict) -> Dict[str, AssetResult]:
    out = {}

    kospi_close = mkt["KOSPI"]["Close"].dropna()
    kosdaq_close = mkt["KOSDAQ"]["Close"].dropna()
    usdkrw_close = mkt["USDKRW"]["Close"].dropna()
    wti_close = mkt["WTI"]["Close"].dropna()

    for asset, close in [("KOSPI", kospi_close), ("KOSDAQ", kosdaq_close)]:
        trend_score, trend_meta = score_trend_kr(close, asset)
        vkospi_score, vkospi_meta = score_vkospi(krx_cache)
        tactical_score, tactical_meta = score_tactical_kr(close)
        leadership_score, leadership_meta = score_leadership_kr(kosdaq_close, kospi_close, asset)
        turnover_score, turnover_meta = score_turnover_kr(asset, krx_cache)
        flow_score, flow_meta = score_flow_kr(asset, krx_cache)
        fx_score, fx_meta = score_fx_usdkrw(usdkrw_close)
        oil_score, oil_meta = score_oil_wti(wti_close)

        module_scores = {
            "trend": trend_score,
            "vkospi": vkospi_score,
            "tactical": tactical_score,
            "leadership": leadership_score,
            "turnover": turnover_score,
            "flow": flow_score,
            "fx": fx_score,
            "oil": oil_score,
        }
        module_meta = {
            "trend": trend_meta,
            "vkospi": vkospi_meta,
            "tactical": tactical_meta,
            "leadership": leadership_meta,
            "turnover": turnover_meta,
            "flow": flow_meta,
            "fx": fx_meta,
            "oil": oil_meta,
        }

        total = int(sum(module_scores.values()))
        original_signal = sign_label(total)
        signal = original_signal
        guardrail = []

        if asset == "KOSPI":
            if safe_float(flow_meta["foreign_1d_bil"], 0) < 0:
                guardrail.append("외국인 1일 순매도 → 매수 제한")
            if safe_float(flow_meta["foreign_5d_bil"], 0) < 0 and safe_float(flow_meta["foreign_20d_bil"], 0) < 0:
                guardrail.append("5일·20일 누적 수급 동반 부진 → 매수 제한")
            if safe_float(fx_meta["usdkrw_ret20"], 0) >= 2 and safe_float(flow_meta["foreign_5d_bil"], 0) < 0:
                guardrail.append("원/달러 급등 + 단기 수급 약세 → 매수 제한")

        if asset == "KOSDAQ":
            if leadership_meta["bucket"] == "weak":
                guardrail.append("코스닥 리더십 약세 → 매수 제한")

        if guardrail and signal == "매수":
            signal = "보유"

        out[asset] = AssetResult(
            asset=asset,
            total_score=total,
            signal=signal,
            original_signal=original_signal,
            module_scores=module_scores,
            module_meta=module_meta,
            guardrail_reasons=guardrail,
        )

    return out

# =========================================================
# Macro summary for MAIN (with sanity check)
# =========================================================
def build_macro_summary(mkt: dict, fred: dict, prev_state: dict, us_results: dict, kr_results: dict) -> dict:
    prev_macro = prev_state.get("macro", {}) if isinstance(prev_state, dict) else {}
    validation_log = {}

    # FRED
    dgs10 = fred["DGS10"].dropna()
    dgs2 = fred["DGS2"].dropna()
    hy = fred["HY_OAS"].dropna()

    us10y_raw = last_valid(dgs10)
    us10y, ok, msg = sanitize_series_point("US10Y", us10y_raw, prev_macro.get("us10y"), bp_metric=True)
    validation_log["US10Y"] = {"ok": ok, "msg": msg}

    curve_series = (dgs10.align(dgs2, join="inner")[0] - dgs10.align(dgs2, join="inner")[1]).dropna()
    curve_raw = last_valid(curve_series)
    curve, ok, msg = sanitize_series_point("CURVE_2S10S", curve_raw, prev_macro.get("curve_2s10s"), bp_metric=True)
    validation_log["CURVE_2S10S"] = {"ok": ok, "msg": msg}

    hy_raw = last_valid(hy)
    hy_oas, ok, msg = sanitize_series_point("HY_OAS", hy_raw, prev_macro.get("hy_oas"), bp_metric=True)
    validation_log["HY_OAS"] = {"ok": ok, "msg": msg}

    # YF
    dxy_close = mkt["DXY"]["Close"].dropna()
    gold_close = mkt["GOLD"]["Close"].dropna()

    dxy_raw = last_valid(dxy_close)
    dxy, ok, msg = sanitize_series_point("DXY", dxy_raw, prev_macro.get("dxy"))
    validation_log["DXY"] = {"ok": ok, "msg": msg}

    gold_raw = last_valid(gold_close)
    gold, ok, msg = sanitize_series_point("GOLD", gold_raw, prev_macro.get("gold"))
    validation_log["GOLD"] = {"ok": ok, "msg": msg}

    out = {
        "us10y": safe_float(us10y),
        "us10y_1d_bp": safe_float(bp_change(us10y, last_valid(dgs10, 2))),
        "us10y_5d_bp": safe_float(bp_change(us10y, last_valid(dgs10, 6))),
        "us10y_20d_bp": safe_float(bp_change(us10y, last_valid(dgs10, 21))),

        "curve_2s10s": safe_float(curve),
        "curve_2s10s_1d_bp": safe_float(bp_change(curve, last_valid(curve_series, 2))),
        "curve_2s10s_5d_bp": safe_float(bp_change(curve, last_valid(curve_series, 6))),

        "dxy": safe_float(dxy),
        "dxy_1d": safe_float(pct_change(dxy, last_valid(dxy_close, 2))),
        "dxy_5d": safe_float(pct_change(dxy, last_valid(dxy_close, 6))),
        "dxy_ma20": safe_float(last_valid(rolling_mean(dxy_close, 20))),

        "gold": safe_float(gold),
        "gold_1d": safe_float(pct_change(gold, last_valid(gold_close, 2))),
        "gold_5d": safe_float(pct_change(gold, last_valid(gold_close, 6))),

        "hy_oas": safe_float(hy_oas),
        "hy_oas_1d_bp": safe_float(bp_change(hy_oas, last_valid(hy, 2))),
        "hy_oas_5d_bp": safe_float(bp_change(hy_oas, last_valid(hy, 6))),

        "validation": validation_log,
    }
    return out

# =========================================================
# HTML rendering
# =========================================================
def color_for_signal(sig: str) -> str:
    if sig == "매수":
        return "#00d084"
    if sig == "보유":
        return "#f5c842"
    return "#e53e3e"

def badge_style(sig: str) -> str:
    color = color_for_signal(sig)
    return f"background:{rgba_hex(color,0.12)};color:{color};border:1px solid {color}44;"

def module_row(label: str, score: int, max_score: int) -> str:
    ratio = 0 if max_score == 0 else score / max_score
    color = "#00d084" if ratio >= 0.67 else "#f5c842" if ratio >= 0.34 else "#e53e3e"
    width = ratio * 100
    return f"""<div class="mod-row"><span class="mod-label">{label}</span><span class="mod-score">{score}<span style="color:#4a4a6a">/{max_score}</span></span><div style="background:#1a1a2e;border-radius:4px;height:8px;width:100%;overflow:hidden;"><div style="height:100%;width:{width:.1f}%;background:{color};border-radius:4px;"></div></div></div>"""

def make_card(result: AssetResult, max_map: dict) -> str:
    signal_color = color_for_signal(result.signal)

    label_map = {
        "trend": "추세",
        "vix": "VIX",
        "vkospi": "VKOSPI",
        "tactical": "전술",
        "breadth": "Breadth",
        "leadership": "리더십",
        "turnover": "거래대금",
        "flow": "수급",
        "rates": "금리",
        "fx": "환율",
        "oil": "유가",
    }

    rows = []
    for k, label in label_map.items():
        if k in result.module_scores and k in max_map:
            rows.append(module_row(label, result.module_scores[k], max_map[k]))

    original_signal_html = ""
    if result.signal != result.original_signal:
        original_signal_html = f'<span style="color:#6060a0;font-size:11px;">원신호: {result.original_signal}</span>'

    detail_html = ""

    if result.asset in ("SPY", "QQQ"):
        t = result.module_meta["trend"]
        v = result.module_meta["vix"]
        tt = result.module_meta["tactical"]
        b = result.module_meta["breadth"]
        r = result.module_meta["rates"]

        detail_html = f"""
        <div class="details-grid">
          <div class="detail-group"><div class="section-label">추세</div>
            <div class="detail-row"><span>종가</span><span>{fmt_num(t['close'])}</span></div>
            <div class="detail-row"><span>MA50</span><span>{fmt_num(t['ma50'])}</span></div>
            <div class="detail-row"><span>MA200</span><span>{fmt_num(t['ma200'])}</span></div>
            <div class="detail-row"><span>20일 수익률</span><span>{fmt_pct(t['ret20'])}</span></div></div>
          <div class="detail-group"><div class="section-label">VIX</div>
            <div class="detail-row"><span>현재</span><span>{fmt_num(v['vix'])}</span></div>
            <div class="detail-row"><span>5일 변화</span><span>{fmt_pct(v['vix_5d_chg'])}</span></div>
            <div class="detail-row"><span>MA20 대비</span><span>{fmt_num(v['vix_ratio20'],3)}</span></div>
            <div class="detail-row"><span>10일고점 대비</span><span>{fmt_pct(v['off_10d_high'])}</span></div></div>
          <div class="detail-group"><div class="section-label">전술</div>
            <div class="detail-row"><span>20일선 이격</span><span>{fmt_pct(tt['dist20'])}</span></div>
            <div class="detail-row"><span>10일 수익률</span><span>{fmt_pct(tt['ret10'])}</span></div>
            <div class="detail-row"><span>5일선 돌파</span><span>{"✓" if tt["above_5dma"] else "<span style='color:#4a4a6a;'>✗</span>"}</span></div>
            <div class="detail-row"><span>3일 중 2일 상승</span><span>{"✓" if tt["two_of_three_up"] else "<span style='color:#4a4a6a;'>✗</span>"}</span></div></div>
          <div class="detail-group"><div class="section-label">Breadth / 금리</div>
            <div class="detail-row"><span>Breadth 버킷</span><span>{b['bucket']}</span></div>
            <div class="detail-row"><span>Approx 비율</span><span>{fmt_num(b['approx_pct'])}%</span></div>
            <div class="detail-row"><span>US 10Y</span><span>{fmt_num(r['dgs10'])}%</span></div>
            <div class="detail-row"><span>20일 변화</span><span>{fmt_bp(r['delta20_bp'])}</span></div></div>
        </div>
        """

    elif result.asset in ("KOSPI", "KOSDAQ"):
        t = result.module_meta["trend"]
        v = result.module_meta["vkospi"]
        tt = result.module_meta["tactical"]
        l = result.module_meta["leadership"]
        f = result.module_meta["flow"]
        to = result.module_meta["turnover"]
        fx = result.module_meta["fx"]
        oil = result.module_meta["oil"]

        detail_html = f"""
        <div class="details-grid">
          <div class="detail-group"><div class="section-label">추세</div>
            <div class="detail-row"><span>종가</span><span>{fmt_num(t['close'])}</span></div>
            <div class="detail-row"><span>MA50</span><span>{fmt_num(t['ma50'])}</span></div>
            <div class="detail-row"><span>MA200</span><span>{fmt_num(t['ma200'])}</span></div>
            <div class="detail-row"><span>20일 수익률</span><span>{fmt_pct(t['ret20'])}</span></div></div>
          <div class="detail-group"><div class="section-label">VKOSPI</div>
            <div class="detail-row"><span>현재</span><span>{fmt_num(v['vkospi'])}</span></div>
            <div class="detail-row"><span>5일 변화</span><span>{fmt_pct(v['vkospi_5d_chg'])}</span></div>
            <div class="detail-row"><span>MA20 대비</span><span>{fmt_num(v['vkospi_ratio20'],3)}</span></div>
            <div class="detail-row"><span>10일고점 대비</span><span>{fmt_pct(v['off_10d_high'])}</span></div></div>
          <div class="detail-group"><div class="section-label">전술 / 리더십</div>
            <div class="detail-row"><span>20일선 이격</span><span>{fmt_pct(tt['dist20'])}</span></div>
            <div class="detail-row"><span>10일 수익률</span><span>{fmt_pct(tt['ret10'])}</span></div>
            <div class="detail-row"><span>리더십 버킷</span><span>{l['bucket']}</span></div>
            <div class="detail-row"><span>추정 리더십</span><span>{fmt_num(l['approx_pct'])}%</span></div></div>
          <div class="detail-group"><div class="section-label">수급</div>
            <div class="detail-row"><span>1일 외국인</span><span>{fmt_bil_krw(f['foreign_1d_bil'])}</span></div>
            <div class="detail-row"><span>5일 외국인</span><span>{fmt_bil_krw(f['foreign_5d_bil'])}</span></div>
            <div class="detail-row"><span>20일 외국인</span><span>{fmt_bil_krw(f['foreign_20d_bil'])}</span></div>
            <div class="detail-row"><span>5일비율</span><span>{fmt_pct(f['foreign_5d_ratio'])}</span></div></div>
          <div class="detail-group"><div class="section-label">거래대금</div>
            <div class="detail-row"><span>현재</span><span>{fmt_num(to['current'])}조</span></div>
            <div class="detail-row"><span>MA20 대비</span><span>{fmt_num(to['ma20_ratio'],3)}</span></div>
            <div class="detail-row"><span>5일 변화</span><span>{fmt_pct(to['chg5'])}</span></div></div>
          <div class="detail-group"><div class="section-label">환율 / 유가</div>
            <div class="detail-row"><span>USD/KRW</span><span>{fmt_num(fx['usdkrw'])}</span></div>
            <div class="detail-row"><span>환율 20일</span><span>{fmt_pct(fx['usdkrw_ret20'])}</span></div>
            <div class="detail-row"><span>WTI</span><span>{fmt_num(oil['wti'])}</span></div>
            <div class="detail-row"><span>WTI 20일</span><span>{fmt_pct(oil['wti_ret20'])}</span></div></div>
        </div>
        """

    guardrail_html = ""
    if result.guardrail_reasons:
        items = "".join(f"<li>{x}</li>" for x in result.guardrail_reasons)
        guardrail_html = f"""<div class="guardrail-box"><div class="section-label">⚠ 가드레일 발동</div><ul style="margin:6px 0 0 16px;padding:0;color:#f5c842;font-size:12px;">{items}</ul></div>"""

    return f"""
    <div class="asset-card" style="border-top:3px solid {signal_color};">
      <div class="card-header">
        <div><div class="asset-name">{result.asset}</div></div>
        <div style="text-align:right;">
          <div class="signal-badge" style="{badge_style(result.signal)}">{result.signal}</div>
          {original_signal_html}
        </div>
      </div>
      <div class="score-display"><span style="color:{signal_color};font-size:42px;font-weight:700;letter-spacing:-2px;">{result.total_score}</span><span style="color:#4a4a6a;font-size:20px;">/100</span></div>
      <div style="background:#1a1a2e;border-radius:4px;height:8px;width:100%;overflow:hidden;"><div style="height:100%;width:{result.total_score:.1f}%;background:{signal_color};border-radius:4px;"></div></div>
      <div class="modules-section">{''.join(rows)}</div>
      {detail_html}
      {guardrail_html}
    </div>
    """

def generate_html(us_results, kr_results, us_updated, kr_updated, macro):
    us_max = {
        "SPY": {"trend": 35, "vix": 25, "tactical": 15, "breadth": 15, "rates": 10},
        "QQQ": {"trend": 30, "vix": 25, "tactical": 15, "breadth": 10, "rates": 20},
    }
    kr_max = {
        "KOSPI":  {"trend": 40, "vkospi": 27, "tactical": 17, "leadership":  6, "turnover": 12, "flow": 20, "fx": 20, "oil": 10},
        "KOSDAQ": {"trend": 36, "vkospi": 27, "tactical": 17, "leadership": 12, "turnover": 12, "flow": 18, "fx": 20, "oil": 10},
    }

    us_cards = "".join(make_card(r, us_max.get(a, {})) for a, r in us_results.items())
    kospi_card = make_card(kr_results["KOSPI"], kr_max["KOSPI"]) if "KOSPI" in kr_results else ""
    kosdaq_card = make_card(kr_results["KOSDAQ"], kr_max["KOSDAQ"]) if "KOSDAQ" in kr_results else ""

    ewy_card = '''
<div class="asset-card" style="border-top:3px solid #60a5fa;grid-column:1/-1;">
  <div class="card-header">
    <div>
      <div class="asset-name" style="font-size:22px;">EWY &nbsp;·&nbsp; EWYUSDT Perp</div>
      <div style="font-size:12px;color:#9090b8;margin-top:4px;">Binance USDⓈ-M Futures · 24시간 라인차트</div>
    </div>
    <a href="ewy.html" target="_blank"
       style="font-family:'IBM Plex Mono',monospace;font-size:12px;padding:7px 16px;
              border-radius:8px;background:#1f2937;color:#f9fafb;
              border:1px solid #374151;text-decoration:none;">전체 보기 ↗</a>
  </div>
  <iframe src="ewy.html"
          style="width:100%;height:520px;border:none;border-radius:10px;
                 background:#0b1220;margin-top:8px;"
          loading="lazy"></iframe>
</div>'''

    def status_to_badge(status):
        return "양호" if status == "good" else "경계" if status == "warn" else "위험"

    def classify_equity(result):
        if result.total_score >= 70:
            return "good"
        elif result.total_score >= 40:
            return "warn"
        return "bad"

    def classify_vol(cur, ratio20):
        if pd.isna(cur):
            return "warn"
        if safe_float(cur, 0) >= 25 or safe_float(ratio20, 0) >= 1.15:
            return "bad"
        elif safe_float(cur, 0) >= 18 or safe_float(ratio20, 0) >= 1.00:
            return "warn"
        return "good"

    def classify_vkospi(cur, ratio20):
        if pd.isna(cur):
            return "warn"
        if safe_float(cur, 0) >= 50 or safe_float(ratio20, 0) >= 1.15:
            return "bad"
        elif safe_float(cur, 0) >= 35 or safe_float(ratio20, 0) >= 1.00:
            return "warn"
        return "good"

    def classify_rates(delta20_bp):
        if pd.isna(delta20_bp):
            return "warn"
        if delta20_bp >= 40:
            return "bad"
        elif delta20_bp >= 20:
            return "warn"
        return "good"

    def classify_fx(ret20):
        if pd.isna(ret20):
            return "warn"
        if ret20 >= 4:
            return "bad"
        elif ret20 >= 2:
            return "warn"
        return "good"

    spy = us_results["SPY"]
    qqq = us_results["QQQ"]
    kospi = kr_results["KOSPI"]
    kosdaq = kr_results["KOSDAQ"]

    main_cards = [
        {
            "section": "Equities",
            "label": "S&P 500",
            "value": fmt_num(spy.module_meta["trend"]["close"]),
            "d1": "—",
            "d5": fmt_pct(spy.module_meta["tactical"]["ret10"]),
            "aux": f"MA50 {'상회' if safe_float(spy.module_meta['trend']['close']) > safe_float(spy.module_meta['trend']['ma50']) else '하회'}",
            "status": classify_equity(spy),
        },
        {
            "section": "Equities",
            "label": "Nasdaq 100",
            "value": fmt_num(qqq.module_meta["trend"]["close"]),
            "d1": "—",
            "d5": fmt_pct(qqq.module_meta["tactical"]["ret10"]),
            "aux": f"MA50 {'상회' if safe_float(qqq.module_meta['trend']['close']) > safe_float(qqq.module_meta['trend']['ma50']) else '하회'}",
            "status": classify_equity(qqq),
        },
        {
            "section": "Equities",
            "label": "KOSPI",
            "value": fmt_num(kospi.module_meta["trend"]["close"]),
            "d1": "—",
            "d5": fmt_pct(kospi.module_meta["tactical"]["ret10"]),
            "aux": f"MA50 {'상회' if safe_float(kospi.module_meta['trend']['close']) > safe_float(kospi.module_meta['trend']['ma50']) else '하회'}",
            "status": classify_equity(kospi),
        },
        {
            "section": "Equities",
            "label": "KOSDAQ",
            "value": fmt_num(kosdaq.module_meta["trend"]["close"]),
            "d1": "—",
            "d5": fmt_pct(kosdaq.module_meta["tactical"]["ret10"]),
            "aux": f"MA50 {'상회' if safe_float(kosdaq.module_meta['trend']['close']) > safe_float(kosdaq.module_meta['trend']['ma50']) else '하회'}",
            "status": classify_equity(kosdaq),
        },

        {
            "section": "Vol / Rates",
            "label": "VIX",
            "value": fmt_num(spy.module_meta["vix"]["vix"]),
            "d1": "—",
            "d5": fmt_pct(spy.module_meta["vix"]["vix_5d_chg"]),
            "aux": f"MA20 대비 {fmt_num(spy.module_meta['vix']['vix_ratio20'],3)}",
            "status": classify_vol(spy.module_meta["vix"]["vix"], spy.module_meta["vix"]["vix_ratio20"]),
        },
        {
            "section": "Vol / Rates",
            "label": "VKOSPI",
            "value": fmt_num(kospi.module_meta["vkospi"]["vkospi"]),
            "d1": "—",
            "d5": fmt_pct(kospi.module_meta["vkospi"]["vkospi_5d_chg"]),
            "aux": f"MA20 대비 {fmt_num(kospi.module_meta['vkospi']['vkospi_ratio20'],3)}",
            "status": classify_vkospi(kospi.module_meta["vkospi"]["vkospi"], kospi.module_meta["vkospi"]["vkospi_ratio20"]),
        },
        {
            "section": "Vol / Rates",
            "label": "US 10Y",
            "value": f"{fmt_num(macro['us10y'],2)}%",
            "d1": fmt_bp(macro["us10y_1d_bp"]),
            "d5": fmt_bp(macro["us10y_5d_bp"]),
            "aux": f"20일 {fmt_bp(macro['us10y_20d_bp'])}",
            "status": classify_rates(macro["us10y_20d_bp"]),
        },
        {
            "section": "Vol / Rates",
            "label": "2Y-10Y",
            "value": f"{fmt_num(macro['curve_2s10s'],2)}%",
            "d1": fmt_bp(macro["curve_2s10s_1d_bp"]),
            "d5": fmt_bp(macro["curve_2s10s_5d_bp"]),
            "aux": "Steepening" if safe_float(macro["curve_2s10s_5d_bp"], 0) > 0 else "Flattening",
            "status": "good" if safe_float(macro["curve_2s10s_5d_bp"], 0) > 5 else "warn",
        },

        {
            "section": "FX / Safe Haven",
            "label": "USD/KRW",
            "value": fmt_num(kospi.module_meta["fx"]["usdkrw"]),
            "d1": "—",
            "d5": "—",
            "aux": f"20일 {fmt_pct(kospi.module_meta['fx']['usdkrw_ret20'])}",
            "status": classify_fx(kospi.module_meta["fx"]["usdkrw_ret20"]),
        },
        {
            "section": "FX / Safe Haven",
            "label": "Dollar Index",
            "value": fmt_num(macro["dxy"]),
            "d1": fmt_pct(macro["dxy_1d"]),
            "d5": fmt_pct(macro["dxy_5d"]),
            "aux": "20일선 상회" if safe_float(macro["dxy"]) > safe_float(macro["dxy_ma20"], 1e9) else "20일선 하회",
            "status": "warn" if safe_float(macro["dxy"]) > safe_float(macro["dxy_ma20"], 1e9) else "good",
        },
        {
            "section": "FX / Safe Haven",
            "label": "Gold",
            "value": fmt_num(macro["gold"], 1),
            "d1": fmt_pct(macro["gold_1d"]),
            "d5": fmt_pct(macro["gold_5d"]),
            "aux": "안전자산 강세" if safe_float(macro["gold_5d"], 0) > 0 else "중립",
            "status": "good" if safe_float(macro["gold_5d"], 0) > 0 else "warn",
        },
        {
            "section": "FX / Credit",
            "label": "HY OAS",
            "value": f"{fmt_num(macro['hy_oas'],2)}%",
            "d1": fmt_bp(macro["hy_oas_1d_bp"]),
            "d5": fmt_bp(macro["hy_oas_5d_bp"]),
            "aux": "신용 경계" if safe_float(macro["hy_oas"], 0) >= 3.5 else "신용 안정",
            "status": "bad" if safe_float(macro["hy_oas"], 0) >= 4.0 else "warn" if safe_float(macro["hy_oas"], 0) >= 3.5 else "good",
        },
    ]

    main_cards_html = "".join(
        f'''
        <div class="main-card">
          <div>
            <div class="main-card-top">
              <div>
                <div class="main-section">{c["section"]}</div>
                <div class="main-title">{c["label"]}</div>
              </div>
              <div class="mini-badge {c["status"]}">{status_to_badge(c["status"])}</div>
            </div>
            <div class="main-value">{c["value"]}</div>
            <div class="main-metrics">
              <div class="row"><span>1일 변화</span><span>{c["d1"]}</span></div>
              <div class="row"><span>5일 변화</span><span>{c["d5"]}</span></div>
              <div class="row"><span>판단</span><span>{c["aux"]}</span></div>
            </div>
          </div>
        </div>
        '''
        for c in main_cards
    )

    score = 0
    for c in main_cards:
        if c["status"] == "good":
            score += 1
        elif c["status"] == "bad":
            score -= 1

    if score >= 3:
        regime_text = "RISK-ON"
        regime_class = "riskon"
        regime_desc = "주식/변동성/신용 전반이 비교적 안정적입니다."
    elif score <= -3:
        regime_text = "RISK-OFF"
        regime_class = "riskoff"
        regime_desc = "금리·달러·변동성 부담이 우세합니다."
    else:
        regime_text = "NEUTRAL"
        regime_class = "neutral"
        regime_desc = "강세와 약세 신호가 혼재합니다."

    alerts = []
    if safe_float(macro["us10y_20d_bp"], 0) >= 20:
        alerts.append("US10Y 상승 → 성장주 밸류에이션 부담")
    if safe_float(kospi.module_meta["fx"]["usdkrw_ret20"], 0) >= 2:
        alerts.append("USD/KRW 급등 → 한국 자산 리스크 가중")
    if safe_float(macro["hy_oas"], 0) >= 3.5:
        alerts.append("HY 스프레드 확대 → 신용시장 점검 필요")
    if not alerts:
        alerts = ["특이 경보 없음", "리스크 신호 혼재", "US/KR 상세탭 병행 확인"]

    alerts_html = "".join(f"<li>{x}</li>" for x in alerts[:3])

    return f'''<!DOCTYPE html>
<html lang="ko"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Market Decision Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans+KR:wght@300;400;600&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#0d0d14;color:#d4d4e0;font-family:"IBM Plex Sans KR",sans-serif;min-height:100vh;padding:32px 16px}}
.page-header{{max-width:960px;margin:0 auto 28px;border-bottom:1px solid #252538;padding-bottom:20px}}
.page-title{{font-family:"IBM Plex Mono",monospace;font-size:22px;font-weight:600;color:#f0f0f8;letter-spacing:-0.5px}}

.tab-bar{{max-width:960px;margin:0 auto;display:flex;gap:0;border-bottom:2px solid #252538;flex-wrap:wrap}}
.tab-btn{{font-family:"IBM Plex Mono",monospace;font-size:14px;font-weight:600;padding:12px 28px;
          border:none;background:#161622;color:#7070a0;cursor:pointer;
          border-bottom:3px solid transparent;margin-bottom:-2px;
          transition:color .15s,border-color .15s,background .15s;letter-spacing:0.5px;
          -webkit-appearance:none;-moz-appearance:none;appearance:none}}
.tab-btn:hover{{color:#c0c0e0;background:#1e1e30}}
.tab-btn.active{{color:#f0f0f8;border-bottom-color:#5b9bd5;background:#0d0d14}}
.tab-link{{font-family:"IBM Plex Mono",monospace;font-size:14px;font-weight:600;
          padding:12px 28px;background:#161622;color:#7070a0;text-decoration:none;
          display:flex;align-items:center;border-bottom:3px solid transparent;
          transition:color .15s,border-color .15s,background .15s}}
.tab-link:hover{{color:#c0c0e0;background:#1e1e30}}
.tab-content{{display:none}}.tab-content.active{{display:block}}

.sub-tab-bar{{max-width:960px;margin:16px auto 0;display:flex;gap:0;border-bottom:1px solid #252538}}
.sub-tab-btn{{font-family:"IBM Plex Mono",monospace;font-size:12px;font-weight:600;
              padding:8px 24px;border:none;background:transparent;color:#7070a0;
              cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-1px;
              transition:color .15s,border-color .15s;
              -webkit-appearance:none;appearance:none}}
.sub-tab-btn:hover{{color:#c0c0e0}}
.sub-tab-btn.active{{color:#f0f0f8;border-bottom-color:#60a5fa}}
.sub-tab-content{{display:none}}.sub-tab-content.active{{display:block}}

.night-btn-wrap{{max-width:960px;margin:14px auto 0;display:flex;justify-content:flex-end}}
.night-btn{{font-family:"IBM Plex Mono",monospace;font-size:12px;font-weight:600;
            padding:8px 18px;border-radius:8px;background:#1a1a2e;color:#60a5fa;
            border:1px solid #60a5fa44;text-decoration:none;
            display:inline-flex;align-items:center;gap:6px;
            transition:background .15s,border-color .15s}}
.night-btn:hover{{background:#1f1f38;border-color:#60a5fa88}}

.update-bar{{max-width:960px;margin:20px auto 28px;display:flex;gap:12px;flex-wrap:wrap}}
.update-badge{{font-family:"IBM Plex Mono",monospace;font-size:11px;padding:6px 14px;
               border-radius:6px;border:1px solid #252538;background:#161622;
               display:flex;align-items:center;gap:8px}}
.update-badge .label{{color:#8888aa;font-weight:600}}
.update-badge .time{{color:#b0b0cc}}

.cards-container{{max-width:960px;margin:0 auto;display:grid;
                  grid-template-columns:repeat(auto-fit,minmax(420px,1fr));gap:24px}}
.asset-card{{background:#13131f;border:1px solid #252538;border-radius:14px;padding:28px;transition:border-color .2s}}
.asset-card:hover{{border-color:#353558}}
.card-header{{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:20px}}
.asset-name{{font-family:"IBM Plex Mono",monospace;font-size:30px;font-weight:700;color:#f0f0f8;letter-spacing:-1px}}
.signal-badge{{font-family:"IBM Plex Mono",monospace;font-size:17px;font-weight:700;padding:7px 20px;border-radius:8px;letter-spacing:1px}}
.score-display{{margin:16px 0 8px;font-family:"IBM Plex Mono",monospace;line-height:1}}
.modules-section{{margin:20px 0;display:flex;flex-direction:column;gap:9px}}
.mod-row{{display:grid;grid-template-columns:80px 58px 1fr;align-items:center;gap:10px}}
.mod-label{{font-size:11px;color:#9090b8;font-family:"IBM Plex Mono",monospace;font-weight:600}}
.mod-score{{font-family:"IBM Plex Mono",monospace;font-size:13px;color:#c0c0d8;text-align:right;font-weight:600}}
.details-grid{{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-top:20px;padding-top:20px;border-top:1px solid #252538}}
.detail-group{{display:flex;flex-direction:column;gap:6px}}
.section-label{{font-family:"IBM Plex Mono",monospace;font-size:10px;color:#5b5b80;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:6px;font-weight:700}}
.detail-row{{display:flex;justify-content:space-between;font-size:12.5px;font-family:"IBM Plex Mono",monospace;padding:2px 0;border-bottom:1px solid #1a1a28}}
.detail-row span:first-child{{color:#9090b8}}
.detail-row span:last-child{{color:#e0e0f0;font-weight:600}}
.guardrail-box{{margin-top:16px;padding:12px 14px;background:rgba(245,200,66,0.07);border:1px solid rgba(245,200,66,0.25);border-radius:8px}}
.guardrail-box .section-label{{color:#f5c842}}
.footer{{max-width:960px;margin:48px auto 0;text-align:center;font-size:11px;color:#353558;font-family:"IBM Plex Mono",monospace;line-height:2}}

.main-wrap{{max-width:960px;margin:0 auto}}
.main-hero{{display:grid;grid-template-columns:1.05fr 1.35fr;gap:18px;margin-bottom:24px}}
.hero-card{{background:#13131f;border:1px solid #252538;border-radius:14px;padding:22px}}
.hero-label{{font-family:"IBM Plex Mono",monospace;font-size:11px;color:#8c8cab;margin-bottom:10px;letter-spacing:1px;text-transform:uppercase}}
.hero-value{{font-family:"IBM Plex Mono",monospace;font-size:34px;font-weight:700;margin-bottom:8px}}
.hero-value.riskon{{color:#22c55e}}
.hero-value.neutral{{color:#f5c842}}
.hero-value.riskoff{{color:#e53e3e}}
.hero-desc{{font-size:14px;color:#b8b8d2;line-height:1.5}}
.alert-list{{margin:8px 0 0 18px;color:#d9d9ea;font-size:13px;line-height:1.7}}
.main-grid{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:18px}}
.main-card{{background:#13131f;border:1px solid #252538;border-radius:14px;padding:18px;min-height:190px;display:flex;flex-direction:column;justify-content:space-between}}
.main-card-top{{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}}
.main-section{{font-family:"IBM Plex Mono",monospace;font-size:10px;color:#6f6f96;letter-spacing:1px;text-transform:uppercase;margin-bottom:6px}}
.main-title{{font-family:"IBM Plex Mono",monospace;font-size:18px;font-weight:700;color:#f0f0f8}}
.main-value{{font-family:"IBM Plex Mono",monospace;font-size:30px;font-weight:700;color:#f0f0f8;margin:16px 0 14px}}
.main-metrics{{display:flex;flex-direction:column;gap:6px}}
.main-metrics .row{{display:flex;justify-content:space-between;font-size:12.5px;font-family:"IBM Plex Mono",monospace;border-bottom:1px solid #1a1a28;padding-bottom:4px}}
.main-metrics .row span:first-child{{color:#9090b8}}
.main-metrics .row span:last-child{{color:#e0e0f0;font-weight:600}}
.mini-badge{{font-family:"IBM Plex Mono",monospace;font-size:11px;font-weight:700;border-radius:999px;padding:6px 10px;white-space:nowrap}}
.mini-badge.good{{background:rgba(34,197,94,0.12);border:1px solid rgba(34,197,94,0.28);color:#22c55e}}
.mini-badge.warn{{background:rgba(245,200,66,0.12);border:1px solid rgba(245,200,66,0.28);color:#f5c842}}
.mini-badge.bad{{background:rgba(229,62,62,0.12);border:1px solid rgba(229,62,62,0.28);color:#e53e3e}}

@media(max-width:1100px){{
  .main-grid{{grid-template-columns:repeat(2,minmax(0,1fr))}}
  .main-hero{{grid-template-columns:1fr}}
}}
@media(max-width:520px){{
  .cards-container{{grid-template-columns:1fr}}
  .details-grid{{grid-template-columns:1fr}}
  .tab-btn{{padding:10px 20px;font-size:13px}}
  .sub-tab-btn{{padding:7px 14px;font-size:11px}}
  .main-grid{{grid-template-columns:1fr}}
}}
</style></head><body>

<div class="page-header">
  <div class="page-title">MARKET DECISION DASHBOARD</div>
</div>

<div class="tab-bar">
  <button class="tab-btn active" data-tab="main">🌐 &nbsp;MAIN</button>
  <button class="tab-btn" data-tab="us">🇺🇸 &nbsp;US</button>
  <button class="tab-btn" data-tab="kr">🇰🇷 &nbsp;KR</button>
  <a href="news.html" class="tab-link">NEWS</a>
</div>

<div class="update-bar">
  <div class="update-badge"><span class="label">🇺🇸 US 업데이트</span><span class="time">{us_updated}</span></div>
  <div class="update-badge"><span class="label">🇰🇷 KR 업데이트</span><span class="time">{kr_updated}</span></div>
</div>

<div id="tab-main" class="tab-content active">
  <div class="main-wrap">
    <div class="main-hero">
      <div class="hero-card">
        <div class="hero-label">Global Regime</div>
        <div class="hero-value {regime_class}">{regime_text}</div>
        <div class="hero-desc">{regime_desc}</div>
      </div>
      <div class="hero-card">
        <div class="hero-label">오늘의 경보</div>
        <ul class="alert-list">{alerts_html}</ul>
      </div>
    </div>
    <div class="main-grid">{main_cards_html}</div>
  </div>
</div>

<div id="tab-us" class="tab-content">
  <div class="cards-container">{us_cards}</div>
</div>

<div id="tab-kr" class="tab-content">
  <div class="night-btn-wrap">
    <a class="night-btn" href="night_futures.html" target="_blank">🌙 야간선물 Night Futures ↗</a>
  </div>

  <div class="sub-tab-bar">
    <button class="sub-tab-btn active" data-subtab="kospi">KOSPI</button>
    <button class="sub-tab-btn" data-subtab="kosdaq">KOSDAQ</button>
    <button class="sub-tab-btn" data-subtab="ewy">EWY</button>
  </div>

  <div id="subtab-kospi" class="sub-tab-content active">
    <div class="cards-container" style="margin-top:24px;">{kospi_card}</div>
  </div>
  <div id="subtab-kosdaq" class="sub-tab-content">
    <div class="cards-container" style="margin-top:24px;">{kosdaq_card}</div>
  </div>
  <div id="subtab-ewy" class="sub-tab-content">
    <div class="cards-container" style="margin-top:24px;">{ewy_card}</div>
  </div>
</div>

<div class="footer">
  <p>score ≥ 70 → 매수 &nbsp;|&nbsp; 40–69 → 보유 &nbsp;|&nbsp; &lt; 40 → 매도</p>
  <p>US: 매 거래일 06:30 KST &nbsp;|&nbsp; KR: 매 거래일 16:30 KST</p>
</div>

<script>
(function(){{
  document.querySelectorAll('.tab-btn[data-tab]').forEach(function(btn){{
    btn.addEventListener('click', function(){{
      var name = this.getAttribute('data-tab');
      document.querySelectorAll('.tab-content').forEach(function(el){{el.classList.remove('active');}});
      document.querySelectorAll('.tab-btn[data-tab]').forEach(function(el){{el.classList.remove('active');}});
      document.getElementById('tab-' + name).classList.add('active');
      this.classList.add('active');
      try{{localStorage.setItem('lastTab', name);}}catch(e){{}}
    }});
  }});

  document.querySelectorAll('.sub-tab-btn[data-subtab]').forEach(function(btn){{
    btn.addEventListener('click', function(){{
      var name = this.getAttribute('data-subtab');
      document.querySelectorAll('.sub-tab-content').forEach(function(el){{el.classList.remove('active');}});
      document.querySelectorAll('.sub-tab-btn[data-subtab]').forEach(function(el){{el.classList.remove('active');}});
      document.getElementById('subtab-' + name).classList.add('active');
      this.classList.add('active');
      try{{localStorage.setItem('lastSubTab', name);}}catch(e){{}}
    }});
  }});

  try{{
    var last = localStorage.getItem('lastTab');
    if(last){{ var t = document.querySelector('[data-tab="' + last + '"]'); if(t) t.click(); }}
    var lastSub = localStorage.getItem('lastSubTab');
    if(lastSub){{ var s = document.querySelector('[data-subtab="' + lastSub + '"]'); if(s) s.click(); }}
  }}catch(e){{}}
}})();
</script>
</body></html>'''

# =========================================================
# Main
# =========================================================
def main():
    prev_state = read_json(STATE_FILE, {})

    print("[INFO] Fetching yfinance data...")
    mkt = fetch_yf_all()

    print("[INFO] Fetching FRED data...")
    fred = fetch_fred_all()

    print("[INFO] Reading KRX cache...")
    krx_cache = read_krx_cache()

    print("[INFO] Building US results...")
    us_results = build_us_results(mkt, fred)

    print("[INFO] Building KR results...")
    kr_results = build_kr_results(mkt, krx_cache)

    print("[INFO] Building macro summary...")
    macro = build_macro_summary(mkt, fred, prev_state, us_results, kr_results)

    us_updated = fmt_ts_kst()
    kr_updated = fmt_ts_kst()

    print("[INFO] Generating HTML...")
    html = generate_html(us_results, kr_results, us_updated, kr_updated, macro)
    with open(INDEX_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    state = {
        "updated_kst": fmt_ts_kst(),
        "macro": macro,
        "us_results": {
            k: {
                "asset": v.asset,
                "total_score": v.total_score,
                "signal": v.signal,
                "original_signal": v.original_signal,
                "module_scores": v.module_scores,
                "module_meta": v.module_meta,
                "guardrail_reasons": v.guardrail_reasons,
            } for k, v in us_results.items()
        },
        "kr_results": {
            k: {
                "asset": v.asset,
                "total_score": v.total_score,
                "signal": v.signal,
                "original_signal": v.original_signal,
                "module_scores": v.module_scores,
                "module_meta": v.module_meta,
                "guardrail_reasons": v.guardrail_reasons,
            } for k, v in kr_results.items()
        },
    }
    write_json(STATE_FILE, state)

    print(f"[DONE] wrote {INDEX_FILE}")
    print(f"[DONE] wrote {STATE_FILE}")

if __name__ == "__main__":
    main()
