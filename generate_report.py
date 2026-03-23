# -*- coding: utf-8 -*-
"""
Operational Market Dashboard Generator
- output: docs/index.html, docs/state.json
- tabs: MAIN / US / KR / NEWS
"""

import os
import io
import json
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
TOKEN_CACHE_FILE = "tmp/kis_token_cache.json"

YF_PERIOD = "2y"
REQUEST_TIMEOUT = 20
USE_AUTO_ADJUST = True

KIS_APP_KEY    = os.environ.get("KIS_APP_KEY", "")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "")
KIS_BASE_URL   = "https://openapi.koreainvestment.com:9443"

TICKERS = {
    "SPY": "SPY", "QQQ": "QQQ", "VIX": "^VIX",
    "KOSPI": "^KS11", "KOSDAQ": "^KQ11",
    "EWY": "EWY", "DXY": "DX-Y.NYB", "GOLD": "GC=F",
    "WTI": "CL=F", "USDKRW": "KRW=X",
    "RSP": "RSP",
    "GSPC": "^GSPC",
    "NDX": "^NDX", "QQEW": "QQEW",
}

FRED_SERIES = {
    "DGS10": "DGS10",
    "DGS2": "DGS2",
    "HY_OAS": "BAMLH0A0HYM2",
}

os.makedirs(DOCS_DIR, exist_ok=True)
os.makedirs("tmp", exist_ok=True)

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
# Helpers
# =========================================================
def now_kst() -> datetime:
    return datetime.now(KST)

def fmt_ts_kst(dt=None) -> str:
    if dt is None:
        dt = now_kst()
    return dt.strftime("%Y-%m-%d %H:%M KST")

def today_kst_str() -> str:
    return datetime.now(KST).strftime("%Y%m%d")

def read_json(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def write_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

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
    curr, prev = safe_float(curr), safe_float(prev)
    if pd.isna(curr) or pd.isna(prev) or prev == 0:
        return np.nan
    return (curr / prev - 1.0) * 100.0

def bp_change(curr, prev):
    curr, prev = safe_float(curr), safe_float(prev)
    if pd.isna(curr) or pd.isna(prev):
        return np.nan
    return (curr - prev) * 100.0

def last_valid(series, n=1):
    s = series.dropna()
    if len(s) < n:
        return np.nan
    return s.iloc[-n]

def rolling_mean(series, n):
    return series.rolling(n).mean()

def sign_label(score):
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
    v = safe_float(v)
    if pd.isna(v):
        return "—"
    sign = "+" if v > 0 else ""
    abs_v = abs(v)
    if abs_v >= 10000:
        return f"{sign}{v/10000:.1f}조"
    return f"{sign}{v:,.0f}억"

def rgba_hex(hex_color, alpha):
    h = hex_color.replace("#", "")
    if len(h) != 6:
        return f"rgba(255,255,255,{alpha})"
    r, g, b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
    return f"rgba({r},{g},{b},{alpha})"

# =========================================================
# KIS Token Manager (1일 1회 발급, 캐시 재사용)
# =========================================================
def _load_token_cache():
    try:
        c = read_json(TOKEN_CACHE_FILE, {})
        token = c.get("access_token", "")
        expires = c.get("expires_at_kst", "")
        if not token or not expires:
            return None, None
        exp_dt = datetime.strptime(expires, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
        if datetime.now(KST) < exp_dt - timedelta(minutes=30):
            return token, exp_dt
        return None, None
    except Exception:
        return None, None

def _save_token_cache(token, expires_at_kst):
    os.makedirs("tmp", exist_ok=True)
    write_json(TOKEN_CACHE_FILE, {
        "access_token": token,
        "expires_at_kst": expires_at_kst.strftime("%Y-%m-%d %H:%M:%S"),
        "issued_at_kst": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
    })

def _issue_new_token():
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        raise RuntimeError("KIS_APP_KEY / KIS_APP_SECRET 환경변수 없음")
    r = requests.post(
        f"{KIS_BASE_URL}/oauth2/tokenP",
        json={"grant_type": "client_credentials",
              "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET},
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    token = data.get("access_token", "")
    expires_str = data.get("access_token_token_expired", "")  # "YYYY-MM-DD HH:MM:SS"
    if not token:
        raise RuntimeError(f"토큰 발급 실패: {data}")
    try:
        exp_dt = datetime.strptime(expires_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
    except Exception:
        exp_dt = datetime.now(KST) + timedelta(hours=23)
    _save_token_cache(token, exp_dt)
    print(f"  [KIS] 신규 토큰 발급 완료 (만료: {exp_dt.strftime('%Y-%m-%d %H:%M')} KST)")
    return token

def get_kis_token():
    """캐시 유효 시 재사용, 만료 시 1회 발급"""
    token, _ = _load_token_cache()
    if token:
        print("  [KIS] 캐시 토큰 재사용")
        return token
    return _issue_new_token()

# =========================================================
# KIS 수급 조회
# =========================================================
def fetch_kis_investor_flow(token, market_name, run_date):
    """
    KIS API: 시장별 투자자 매매 동향 (일별)
    market_name: "KOSPI" or "KOSDAQ"
    """
    market_map = {
        "KOSPI":  {"market": "KSP", "iscd": "0001"},
        "KOSDAQ": {"market": "KSQ", "iscd": "1001"},
    }
    info = market_map[market_name]
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "FHPTJ04040000",
        "custtype": "P",
    }
    params = {
        "FID_COND_MRKT_DIV_CODE": "U",
        "FID_INPUT_ISCD": info["iscd"],
        "FID_INPUT_DATE_1": run_date,
        "FID_INPUT_ISCD_1": info["market"],
        "FID_INPUT_DATE_2": run_date,
        "FID_INPUT_ISCD_2": info["iscd"],
    }
    r = requests.get(
        f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-investor-daily-by-market",
        headers=headers, params=params, timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    if data.get("rt_cd") != "0":
        raise RuntimeError(f"{market_name} 수급 조회 실패: {data.get('msg1')}")
    output = data.get("output", [])
    if not output:
        return pd.DataFrame()
    df = pd.DataFrame(output)
    for c in df.columns:
        if c != "stck_bsop_date":
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["stck_bsop_date"] = df["stck_bsop_date"].astype(str)
    return df.sort_values("stck_bsop_date", ascending=False).reset_index(drop=True)

def calc_flow_metrics(df, turnover_recent):
    """
    df: KIS 수급 DataFrame (최신순)
    turnover_recent: 최근 거래대금 (억원, float)
    returns dict with foreign_1d_bil, foreign_5d_bil, foreign_20d_bil, foreign_5d_ratio
    """
    empty = {"foreign_1d_bil": np.nan, "foreign_5d_bil": np.nan,
             "foreign_20d_bil": np.nan, "foreign_5d_ratio": np.nan}
    if df.empty or "frgn_ntby_tr_pbmn" not in df.columns:
        return empty

    # 단위: 백만원 → 억원 변환
    frgn = df["frgn_ntby_tr_pbmn"].dropna()
    if len(frgn) == 0:
        return empty

    f1  = float(frgn.iloc[0]) / 100.0  # 1일 외국인 순매수 (억원)
    f5  = float(frgn.iloc[:5].sum()) / 100.0 if len(frgn) >= 5 else np.nan
    f20 = float(frgn.iloc[:20].sum()) / 100.0 if len(frgn) >= 20 else np.nan

    # 5일 비율: 5일 누적 / (거래대금 × 5)
    f5_ratio = np.nan
    if not pd.isna(f5) and not pd.isna(turnover_recent) and turnover_recent != 0:
        f5_ratio = f5 / (turnover_recent * 5) * 100.0  # %

    return {
        "foreign_1d_bil": round(f1, 1),
        "foreign_5d_bil": round(f5, 1) if not pd.isna(f5) else np.nan,
        "foreign_20d_bil": round(f20, 1) if not pd.isna(f20) else np.nan,
        "foreign_5d_ratio": round(f5_ratio, 2) if not pd.isna(f5_ratio) else np.nan,
    }

# =========================================================
# KRX Cache helpers
# =========================================================
def read_krx_cache():
    return read_json(KRX_CACHE_FILE, {})

def cache_series_to_pd(cache, key):
    """날짜별 dict → pd.Series (index: pd.Timestamp, 최신순 정렬)"""
    d = cache.get(key, {})
    if not d:
        return pd.Series(dtype=float)
    s = pd.Series({pd.Timestamp(k): float(v) for k, v in d.items()}).sort_index()
    return s

def calc_turnover_metrics(cache, asset):
    """
    거래대금 시계열에서 직접 계산
    returns: (current_eok, ma20_ratio, chg5_pct, recent_eok)
    """
    key = f"turnover_{asset}"
    s = cache_series_to_pd(cache, key)
    if len(s) < 2:
        return np.nan, np.nan, np.nan, np.nan
    # 원 → 억원
    s_eok = s / 1e8
    curr = float(s_eok.iloc[-1])
    # MA20
    ma20 = float(s_eok.rolling(20).mean().iloc[-1]) if len(s_eok) >= 20 else np.nan
    ma20_ratio = curr / ma20 if not pd.isna(ma20) and ma20 != 0 else np.nan
    # 5일 변화율
    prev5 = float(s_eok.iloc[-6]) if len(s_eok) >= 6 else np.nan
    chg5 = pct_change(curr, prev5)
    # 조 단위 (표시용)
    curr_tril = curr / 10000.0
    return curr_tril, ma20_ratio, chg5, curr

def calc_vkospi_metrics(cache):
    """
    VKOSPI 시계열에서 직접 계산
    """
    s = cache_series_to_pd(cache, "vkospi")
    if len(s) < 2:
        return np.nan, np.nan, np.nan, np.nan
    cur = float(s.iloc[-1])
    prev5 = float(s.iloc[-6]) if len(s) >= 6 else np.nan
    chg5 = pct_change(cur, prev5)
    ma20 = float(s.rolling(20).mean().iloc[-1]) if len(s) >= 20 else np.nan
    ratio20 = cur / ma20 if not pd.isna(ma20) and ma20 != 0 else np.nan
    high10 = float(s.tail(10).max()) if len(s) >= 10 else np.nan
    off10 = pct_change(cur, high10)
    return cur, chg5, ratio20, off10

# =========================================================
# Fetchers
# =========================================================
def safe_download_yf(ticker, period="2y", auto_adjust=True, retries=3):
    for _ in range(retries):
        try:
            df = yf.download(
                tickers=ticker, period=period, interval="1d",
                auto_adjust=auto_adjust, progress=False, threads=False,
            )
            if df is None or df.empty:
                time.sleep(1); continue
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            return df
        except Exception:
            time.sleep(1)
    return pd.DataFrame()

def fetch_yf_all():
    out = {}
    for key, ticker in TICKERS.items():
        out[key] = safe_download_yf(ticker, period=YF_PERIOD, auto_adjust=USE_AUTO_ADJUST)
    return out

def fetch_fred_series(series_id):
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

def fetch_fred_all():
    out = {}
    for key, sid in FRED_SERIES.items():
        try:
            out[key] = fetch_fred_series(sid)
        except Exception:
            out[key] = pd.Series(dtype=float)
    return out

# =========================================================
# Scoring - US
# =========================================================
def score_trend_us(close, asset):
    c = last_valid(close); ma50 = last_valid(rolling_mean(close,50)); ma200 = last_valid(rolling_mean(close,200))
    prev20 = last_valid(close,21); ret20 = pct_change(c, prev20)
    max_score = 35 if asset=="SPY" else 30; score = 0
    if safe_float(c,np.nan) > safe_float(ma50,np.inf): score += 15 if asset=="SPY" else 12
    if safe_float(c,np.nan) > safe_float(ma200,np.inf): score += 15 if asset=="SPY" else 12
    if safe_float(ret20,-999) > 0: score += 5 if asset=="SPY" else 6
    return int(score), {"close":safe_float(c),"ma50":safe_float(ma50),"ma200":safe_float(ma200),"ret20":safe_float(ret20),"max_score":max_score}

def score_vix(vix_close):
    vix = last_valid(vix_close); prev5 = last_valid(vix_close,6); chg5 = pct_change(vix,prev5)
    ma20 = last_valid(rolling_mean(vix_close,20)); ratio20 = vix/ma20 if not pd.isna(ma20) and ma20!=0 else np.nan
    high10 = vix_close.dropna().tail(10).max() if len(vix_close.dropna())>=10 else np.nan; off10 = pct_change(vix,high10)
    if pd.isna(vix): score = 10
    elif vix < 18: score = 25
    elif vix < 25: score = 15
    else: score = 5
    return int(score), {"vix":safe_float(vix),"vix_5d_chg":safe_float(chg5),"vix_ratio20":safe_float(ratio20),"off_10d_high":safe_float(off10),"max_score":25}

def score_tactical_us(close):
    c = last_valid(close); ma20 = last_valid(rolling_mean(close,20)); prev10 = last_valid(close,11)
    ret10 = pct_change(c,prev10); dist20 = pct_change(c,ma20)
    last5 = close.dropna().tail(5); above_5dma = int(len(last5)>=5 and c>last5.mean())
    last4 = close.dropna().tail(4); two_of_three_up = 0
    if len(last4)>=4:
        diffs = last4.diff().dropna(); two_of_three_up = int((diffs>0).tail(3).sum()>=2)
    score = 0
    if safe_float(dist20,-999)>-5: score+=5
    if safe_float(ret10,-999)>-3: score+=5
    if above_5dma: score+=3
    if two_of_three_up: score+=2
    return int(score), {"dist20":safe_float(dist20),"ret10":safe_float(ret10),"above_5dma":above_5dma,"two_of_three_up":two_of_three_up,"max_score":15}

def score_breadth_us(proxy_ratio, max_score):
    c = last_valid(proxy_ratio); ma20 = last_valid(rolling_mean(proxy_ratio,20)); ret20 = pct_change(c,last_valid(proxy_ratio,21))
    if pd.isna(c) or pd.isna(ma20): bucket="neutral"; approx=np.nan; score=max_score//2
    elif c>ma20 and safe_float(ret20,-999)>0: bucket="strong"; approx=65.0; score=max_score
    elif c>ma20: bucket="neutral"; approx=50.0; score=max_score//2+1
    else: bucket="weak"; approx=35.0; score=max_score//3
    return int(score), {"bucket":bucket,"approx_pct":safe_float(approx),"ratio":safe_float(c),"ratio_ma20":safe_float(ma20),"ret20":safe_float(ret20),"max_score":max_score}

def score_rates_us(dgs10, asset):
    cur = last_valid(dgs10); prev20 = last_valid(dgs10,21); delta20_bp = bp_change(cur,prev20)
    max_score = 10 if asset=="SPY" else 20
    if pd.isna(delta20_bp): score = max_score//2
    elif asset=="SPY": score = 0 if delta20_bp>=40 else 2 if delta20_bp>=20 else 8
    else: score = 0 if delta20_bp>=40 else 4 if delta20_bp>=20 else 12
    return int(score), {"dgs10":safe_float(cur),"delta20_bp":safe_float(delta20_bp),"max_score":max_score}

# =========================================================
# Scoring - KR
# =========================================================
def score_trend_kr(close, asset):
    c = last_valid(close); ma50 = last_valid(rolling_mean(close,50)); ma200 = last_valid(rolling_mean(close,200))
    prev20 = last_valid(close,21); ret20 = pct_change(c,prev20)
    max_score = 40 if asset=="KOSPI" else 36; score = 0
    if safe_float(c,np.nan) > safe_float(ma50,np.inf): score += 16 if asset=="KOSPI" else 14
    if safe_float(c,np.nan) > safe_float(ma200,np.inf): score += 16 if asset=="KOSPI" else 14
    if safe_float(ret20,-999)>0: score+=8
    return int(score), {"close":safe_float(c),"ma50":safe_float(ma50),"ma200":safe_float(ma200),"ret20":safe_float(ret20),"max_score":max_score}

def score_vkospi_from_cache(cache):
    """krx_cache의 vkospi 시계열에서 직접 계산"""
    cur, chg5, ratio20, off10 = calc_vkospi_metrics(cache)
    if pd.isna(cur): score = 10
    elif cur < 20: score = 27
    elif cur < 35: score = 22
    elif cur < 50: score = 16
    else: score = 8
    return int(score), {"vkospi":safe_float(cur),"vkospi_5d_chg":safe_float(chg5),"vkospi_ratio20":safe_float(ratio20),"off_10d_high":safe_float(off10),"max_score":27}

def score_tactical_kr(close):
    c = last_valid(close); ma20 = last_valid(rolling_mean(close,20)); prev10 = last_valid(close,11)
    ret10 = pct_change(c,prev10); dist20 = pct_change(c,ma20)
    last5 = close.dropna().tail(5); above_5dma = int(len(last5)>=5 and c>last5.mean())
    last4 = close.dropna().tail(4); two_of_three_up = 0
    if len(last4)>=4:
        diffs = last4.diff().dropna(); two_of_three_up = int((diffs>0).tail(3).sum()>=2)
    score = 0
    if safe_float(dist20,-999)>-5: score+=6
    if safe_float(ret10,-999)>-3: score+=6
    if above_5dma: score+=3
    if two_of_three_up: score+=2
    return int(score), {"dist20":safe_float(dist20),"ret10":safe_float(ret10),"above_5dma":above_5dma,"two_of_three_up":two_of_three_up,"max_score":17}

def score_leadership_kr(kosdaq_close, kospi_close, asset):
    rel = (kosdaq_close/kospi_close).dropna()
    c = last_valid(rel); ma20 = last_valid(rolling_mean(rel,20)); ret20 = pct_change(c,last_valid(rel,21))
    if asset=="KOSPI":
        max_score=6
        if not pd.isna(c) and not pd.isna(ma20) and c<ma20: bucket="kosdaq_weak"; approx=28.0; score=6
        else: bucket="neutral"; approx=50.0; score=3
    else:
        max_score=12
        if not pd.isna(c) and not pd.isna(ma20) and c>ma20 and safe_float(ret20,-999)>0: bucket="strong"; approx=65.0; score=12
        elif not pd.isna(c) and not pd.isna(ma20) and c>ma20: bucket="neutral"; approx=50.0; score=6
        else: bucket="weak"; approx=28.0; score=1
    return int(score), {"bucket":bucket,"approx_pct":safe_float(approx),"rel":safe_float(c),"rel_ma20":safe_float(ma20),"rel_ret20":safe_float(ret20),"max_score":max_score}

def score_turnover_kr_from_cache(asset, cache):
    """krx_cache의 turnover 시계열에서 직접 계산"""
    curr_tril, ma20_ratio, chg5, curr_eok = calc_turnover_metrics(cache, asset)
    max_score = 12
    if pd.isna(curr_tril):
        return 6, {"current":np.nan,"ma20_ratio":np.nan,"chg5":np.nan,"max_score":max_score}
    score = 5
    if not pd.isna(ma20_ratio):
        if ma20_ratio>=1.05: score+=4
        elif ma20_ratio>=0.95: score+=2
    if not pd.isna(chg5):
        if chg5>10: score+=3
        elif chg5>-5: score+=1
    return int(min(score,max_score)), {"current":safe_float(curr_tril),"ma20_ratio":safe_float(ma20_ratio),"chg5":safe_float(chg5),"max_score":max_score}

def score_flow_kr(flow_metrics, asset):
    """KIS API로 수집한 수급 metrics로 점수 계산"""
    f1  = safe_float(flow_metrics.get("foreign_1d_bil"))
    f5  = safe_float(flow_metrics.get("foreign_5d_bil"))
    f20 = safe_float(flow_metrics.get("foreign_20d_bil"))
    f5r = safe_float(flow_metrics.get("foreign_5d_ratio"))
    max_score = 20 if asset=="KOSPI" else 18
    score = 0
    if not pd.isna(f1)  and f1>0:  score+=5
    if not pd.isna(f5)  and f5>0:  score+=7
    if not pd.isna(f20) and f20>0: score+=6
    if not pd.isna(f5r) and f5r>0: score+=2
    return int(min(score,max_score)), {"foreign_1d_bil":f1,"foreign_5d_bil":f5,"foreign_20d_bil":f20,"foreign_5d_ratio":f5r,"max_score":max_score}

def score_fx_usdkrw(usdkrw_close):
    cur = last_valid(usdkrw_close); prev20 = last_valid(usdkrw_close,21); ret20 = pct_change(cur,prev20)
    if pd.isna(ret20): score=10
    elif ret20>=4: score=3
    elif ret20>=2: score=8
    else: score=15
    return int(score), {"usdkrw":safe_float(cur),"usdkrw_ret20":safe_float(ret20),"max_score":20}

def score_oil_wti(wti_close):
    cur = last_valid(wti_close); prev20 = last_valid(wti_close,21); ret20 = pct_change(cur,prev20)
    if pd.isna(ret20): score=6
    elif ret20>=20: score=2
    elif ret20>=10: score=5
    else: score=8
    return int(score), {"wti":safe_float(cur),"wti_ret20":safe_float(ret20),"max_score":10}

# =========================================================
# Result builders
# =========================================================
def build_us_results(mkt, fred):
    out = {}
    spy_close = mkt["SPY"]["Close"].dropna(); qqq_close = mkt["QQQ"]["Close"].dropna()
    vix_close = mkt["VIX"]["Close"].dropna(); rsp_close = mkt["RSP"]["Close"].dropna()
    qqew_close = mkt["QQEW"]["Close"].dropna(); dgs10 = fred["DGS10"].dropna()
    gspc_close = mkt["GSPC"]["Close"].dropna() if not mkt["GSPC"].empty else spy_close * 8.6
    ndx_close  = mkt["NDX"]["Close"].dropna()  if not mkt["NDX"].empty  else qqq_close * 40.0
    configs = [
        ("SPY", spy_close, (rsp_close/spy_close).dropna(), 15),
        ("QQQ", qqq_close, (qqew_close/qqq_close).dropna(), 10),
    ]
    for asset, close, breadth_proxy, breadth_max in configs:
        ts,tm = score_trend_us(close,asset); vs,vm = score_vix(vix_close)
        xs,xm = score_tactical_us(close); bs,bm = score_breadth_us(breadth_proxy,breadth_max)
        rs,rm = score_rates_us(dgs10,asset)
        module_scores = {"trend":ts,"vix":vs,"tactical":xs,"breadth":bs,"rates":rs}
        module_meta   = {"trend":tm,"vix":vm,"tactical":xm,"breadth":bm,"rates":rm}
        total = int(sum(module_scores.values())); original_signal = sign_label(total)
        signal = original_signal; guardrail = []
        c=safe_float(tm["close"]); ma50=safe_float(tm["ma50"]); ma200=safe_float(tm["ma200"])
        ret20=safe_float(tm["ret20"]); delta20_bp=safe_float(rm["delta20_bp"])
        if not pd.isna(c) and not pd.isna(ma50) and not pd.isna(ma200) and not pd.isna(ret20):
            if c<ma50 and c<ma200 and ret20<0:
                signal="매도"; guardrail.append("50/200일선 하회 + 20일 수익률 음수 → 매도 우선")
        if asset=="QQQ" and not pd.isna(delta20_bp) and delta20_bp>=20:
            if signal=="매수": signal="보유"
            elif signal=="보유": signal="매도"
            guardrail.append("10년물 20일 급등 → QQQ 한 단계 하향")
        out[asset] = AssetResult(asset=asset,total_score=total,signal=signal,original_signal=original_signal,
                                  module_scores=module_scores,module_meta=module_meta,guardrail_reasons=guardrail)
    return out

def build_kr_results(mkt, krx_cache):
    out = {}
    kospi_close  = mkt["KOSPI"]["Close"].dropna()
    kosdaq_close = mkt["KOSDAQ"]["Close"].dropna()
    usdkrw_close = mkt["USDKRW"]["Close"].dropna()
    wti_close    = mkt["WTI"]["Close"].dropna()

    # ── KIS 수급 수집 (1회 호출, 두 시장 공유) ──────────────
    flow_metrics = {"KOSPI": {}, "KOSDAQ": {}}
    try:
        token    = get_kis_token()
        run_date = today_kst_str()
        for mkt_name in ["KOSPI", "KOSDAQ"]:
            df = fetch_kis_investor_flow(token, mkt_name, run_date)
            # 해당 시장의 최근 거래대금 (억원)
            _, _, _, curr_eok = calc_turnover_metrics(krx_cache, mkt_name)
            flow_metrics[mkt_name] = calc_flow_metrics(df, curr_eok)
            f = flow_metrics[mkt_name]
            print(f"  [FLOW] {mkt_name}: 1일={fmt_bil_krw(f.get('foreign_1d_bil'))} "
                  f"5일={fmt_bil_krw(f.get('foreign_5d_bil'))} "
                  f"20일={fmt_bil_krw(f.get('foreign_20d_bil'))}")
    except Exception as e:
        print(f"  [KIS WARN] 수급 조회 실패: {e} → 수급 0점 처리")

    for asset, close in [("KOSPI", kospi_close), ("KOSDAQ", kosdaq_close)]:
        ts,tm  = score_trend_kr(close, asset)
        vs,vm  = score_vkospi_from_cache(krx_cache)
        xs,xm  = score_tactical_kr(close)
        ls,lm  = score_leadership_kr(kosdaq_close, kospi_close, asset)
        tos,tom = score_turnover_kr_from_cache(asset, krx_cache)
        fs,fm  = score_flow_kr(flow_metrics.get(asset, {}), asset)
        fxs,fxm = score_fx_usdkrw(usdkrw_close)
        os_,om = score_oil_wti(wti_close)

        module_scores = {"trend":ts,"vkospi":vs,"tactical":xs,"leadership":ls,
                         "turnover":tos,"flow":fs,"fx":fxs,"oil":os_}
        module_meta   = {"trend":tm,"vkospi":vm,"tactical":xm,"leadership":lm,
                         "turnover":tom,"flow":fm,"fx":fxm,"oil":om}
        total = int(sum(module_scores.values())); original_signal = sign_label(total)
        signal = original_signal; guardrail = []

        if asset=="KOSPI":
            if safe_float(fm["foreign_1d_bil"],0)<0: guardrail.append("외국인 1일 순매도 → 매수 제한")
            if safe_float(fm["foreign_5d_bil"],0)<0 and safe_float(fm["foreign_20d_bil"],0)<0:
                guardrail.append("5일·20일 누적 수급 동반 부진 → 매수 제한")
            if safe_float(fxm["usdkrw_ret20"],0)>=2 and safe_float(fm["foreign_5d_bil"],0)<0:
                guardrail.append("원/달러 급등 + 단기 수급 약세 → 매수 제한")
        if asset=="KOSDAQ":
            if lm["bucket"]=="weak": guardrail.append("코스닥 리더십 약세 → 매수 제한")
        if guardrail and signal=="매수":
            signal="보유"

        out[asset] = AssetResult(asset=asset,total_score=total,signal=signal,original_signal=original_signal,
                                  module_scores=module_scores,module_meta=module_meta,guardrail_reasons=guardrail)
    return out

# =========================================================
# Macro summary
# =========================================================
def build_macro_summary(mkt, fred, prev_state):
    dgs10 = fred["DGS10"].dropna(); dgs2 = fred["DGS2"].dropna(); hy = fred["HY_OAS"].dropna()
    us10y = last_valid(dgs10)
    curve_series = (dgs10.align(dgs2,join="inner")[0] - dgs10.align(dgs2,join="inner")[1]).dropna()
    curve = last_valid(curve_series); hy_oas = last_valid(hy)
    dxy_close = mkt["DXY"]["Close"].dropna(); gold_close = mkt["GOLD"]["Close"].dropna()
    dxy = last_valid(dxy_close); gold = last_valid(gold_close)
    return {
        "us10y": safe_float(us10y),
        "us10y_1d_bp": safe_float(bp_change(us10y, last_valid(dgs10,2))),
        "us10y_5d_bp": safe_float(bp_change(us10y, last_valid(dgs10,6))),
        "us10y_20d_bp": safe_float(bp_change(us10y, last_valid(dgs10,21))),
        "curve_2s10s": safe_float(curve),
        "curve_2s10s_1d_bp": safe_float(bp_change(curve, last_valid(curve_series,2))),
        "curve_2s10s_5d_bp": safe_float(bp_change(curve, last_valid(curve_series,6))),
        "dxy": safe_float(dxy),
        "dxy_1d": safe_float(pct_change(dxy, last_valid(dxy_close,2))),
        "dxy_5d": safe_float(pct_change(dxy, last_valid(dxy_close,6))),
        "dxy_ma20": safe_float(last_valid(rolling_mean(dxy_close,20))),
        "gold": safe_float(gold),
        "gold_1d": safe_float(pct_change(gold, last_valid(gold_close,2))),
        "gold_5d": safe_float(pct_change(gold, last_valid(gold_close,6))),
        "hy_oas": safe_float(hy_oas),
        "hy_oas_1d_bp": safe_float(bp_change(hy_oas, last_valid(hy,2))),
        "hy_oas_5d_bp": safe_float(bp_change(hy_oas, last_valid(hy,6))),
    }

# =========================================================
# HTML (원본 generate_html 그대로 유지 - 변경 없음)
# =========================================================
def color_for_signal(sig):
    if sig=="매수": return "#00d084"
    if sig=="보유": return "#f5c842"
    return "#e53e3e"

def badge_style(sig):
    color = color_for_signal(sig)
    return f"background:{rgba_hex(color,0.12)};color:{color};border:1px solid {color}44;"

def module_row(label, score, max_score):
    ratio = 0 if max_score==0 else score/max_score
    color = "#00d084" if ratio>=0.67 else "#f5c842" if ratio>=0.34 else "#e53e3e"
    width = ratio*100
    return f'''<div class="mod-row"><span class="mod-label">{label}</span><span class="mod-score">{score}<span style="color:#4a4a6a">/{max_score}</span></span><div style="background:#1a1a2e;border-radius:4px;height:8px;width:100%;overflow:hidden;"><div style="height:100%;width:{width:.1f}%;background:{color};border-radius:4px;"></div></div></div>'''

def make_card(result, max_map):
    signal_color = color_for_signal(result.signal)
    label_map = {"trend":"추세","vix":"VIX","vkospi":"VKOSPI","tactical":"전술","breadth":"Breadth",
                 "leadership":"리더십","turnover":"거래대금","flow":"수급","rates":"금리","fx":"환율","oil":"유가"}
    rows = []
    for k,label in label_map.items():
        if k in result.module_scores and k in max_map:
            rows.append(module_row(label, result.module_scores[k], max_map[k]))
    original_signal_html = ""
    if result.signal!=result.original_signal:
        original_signal_html = f'<span style="color:#6060a0;font-size:11px;">원신호: {result.original_signal}</span>'
    detail_html = ""
    if result.asset in ("SPY","QQQ"):
        t=result.module_meta["trend"]; v=result.module_meta["vix"]
        tt=result.module_meta["tactical"]; b=result.module_meta["breadth"]; r=result.module_meta["rates"]
        detail_html = f"""<div class="details-grid">
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
        </div>"""
    elif result.asset in ("KOSPI","KOSDAQ"):
        t=result.module_meta["trend"]; v=result.module_meta["vkospi"]
        tt=result.module_meta["tactical"]; l=result.module_meta["leadership"]
        f=result.module_meta["flow"]; to=result.module_meta["turnover"]
        fx=result.module_meta["fx"]; oil=result.module_meta["oil"]
        detail_html = f"""<div class="details-grid">
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
        </div>"""
    guardrail_html = ""
    if result.guardrail_reasons:
        items = "".join(f"<li>{x}</li>" for x in result.guardrail_reasons)
        guardrail_html = f"""<div class="guardrail-box"><div class="section-label">⚠ 가드레일 발동</div><ul style="margin:6px 0 0 16px;padding:0;color:#f5c842;font-size:12px;">{items}</ul></div>"""
    return f"""<div class="asset-card" style="border-top:3px solid {signal_color};">
      <div class="card-header">
        <div><div class="asset-name">{result.asset}</div></div>
        <div style="text-align:right;"><div class="signal-badge" style="{badge_style(result.signal)}">{result.signal}</div>{original_signal_html}</div>
      </div>
      <div class="score-display"><span style="color:{signal_color};font-size:42px;font-weight:700;letter-spacing:-2px;">{result.total_score}</span><span style="color:#4a4a6a;font-size:20px;">/100</span></div>
      <div style="background:#1a1a2e;border-radius:4px;height:8px;width:100%;overflow:hidden;"><div style="height:100%;width:{result.total_score:.1f}%;background:{signal_color};border-radius:4px;"></div></div>
      <div class="modules-section">{''.join(rows)}</div>
      {detail_html}{guardrail_html}
    </div>"""

def generate_html(us_results, kr_results, us_updated, kr_updated, macro, mkt=None):
    us_max = {
        "SPY": {"trend":35,"vix":25,"tactical":15,"breadth":15,"rates":10},
        "QQQ": {"trend":30,"vix":25,"tactical":15,"breadth":10,"rates":20},
    }
    kr_max = {
        "KOSPI":  {"trend":40,"vkospi":27,"tactical":17,"leadership":6,"turnover":12,"flow":20,"fx":20,"oil":10},
        "KOSDAQ": {"trend":36,"vkospi":27,"tactical":17,"leadership":12,"turnover":12,"flow":18,"fx":20,"oil":10},
    }
    us_cards    = "".join(make_card(r,us_max.get(a,{})) for a,r in us_results.items())
    kospi_card  = make_card(kr_results["KOSPI"],  kr_max["KOSPI"])  if "KOSPI"  in kr_results else ""
    kosdaq_card = make_card(kr_results["KOSDAQ"], kr_max["KOSDAQ"]) if "KOSDAQ" in kr_results else ""
    ewy_card = '''<div class="asset-card" style="border-top:3px solid #60a5fa;grid-column:1/-1;">
  <div class="card-header">
    <div><div class="asset-name" style="font-size:22px;">EWY &nbsp;·&nbsp; EWYUSDT Perp</div>
         <div style="font-size:12px;color:#9090b8;margin-top:4px;">Binance USDⓈ-M Futures · 24시간 라인차트</div></div>
    <a href="ewy.html" target="_blank" style="font-family:'IBM Plex Mono',monospace;font-size:12px;padding:7px 16px;border-radius:8px;background:#1f2937;color:#f9fafb;border:1px solid #374151;text-decoration:none;">전체 보기 ↗</a>
  </div>
  <iframe src="ewy.html" style="width:100%;height:520px;border:none;border-radius:10px;background:#0b1220;margin-top:8px;" loading="lazy"></iframe>
</div>'''

    def status_to_badge(s): return "양호" if s=="good" else "경계" if s=="warn" else "위험"
    def classify_equity(r): return "good" if r.total_score>=70 else "warn" if r.total_score>=40 else "bad"
    def classify_vol(cur,r20):
        if pd.isna(cur): return "warn"
        if safe_float(cur,0)>=25 or safe_float(r20,0)>=1.15: return "bad"
        elif safe_float(cur,0)>=18 or safe_float(r20,0)>=1.00: return "warn"
        return "good"
    def classify_vkospi(cur,r20):
        if pd.isna(cur): return "warn"
        if safe_float(cur,0)>=50 or safe_float(r20,0)>=1.15: return "bad"
        elif safe_float(cur,0)>=35 or safe_float(r20,0)>=1.00: return "warn"
        return "good"
    def classify_rates(d20):
        if pd.isna(d20): return "warn"
        if d20>=40: return "bad"
        elif d20>=20: return "warn"
        return "good"
    def classify_fx(r20):
        if pd.isna(r20): return "warn"
        if r20>=4: return "bad"
        elif r20>=2: return "warn"
        return "good"

    spy=us_results["SPY"]; qqq=us_results["QQQ"]
    kospi=kr_results["KOSPI"]; kosdaq=kr_results["KOSDAQ"]

    # GSPC / NDX 시계열 (MAIN 카드용)
    if mkt is None:
        mkt = {k: pd.DataFrame() for k in TICKERS}
    gspc_close = mkt["GSPC"]["Close"].dropna() if "GSPC" in mkt and not mkt["GSPC"].empty else pd.Series(dtype=float)
    ndx_close  = mkt["NDX"]["Close"].dropna()  if "NDX"  in mkt and not mkt["NDX"].empty  else pd.Series(dtype=float)
    main_cards = [
        {"section":"Equities","label":"S&P 500","value":fmt_num(last_valid(gspc_close)),"d1":fmt_pct(pct_change(last_valid(gspc_close),last_valid(gspc_close,2))),"d5":fmt_pct(spy.module_meta["tactical"]["ret10"]),"aux":f"MA50 {'상회' if safe_float(spy.module_meta['trend']['close'])>safe_float(spy.module_meta['trend']['ma50']) else '하회'}","status":classify_equity(spy)},
        {"section":"Equities","label":"Nasdaq 100","value":fmt_num(last_valid(ndx_close)),"d1":fmt_pct(pct_change(last_valid(ndx_close),last_valid(ndx_close,2))),"d5":fmt_pct(qqq.module_meta["tactical"]["ret10"]),"aux":f"MA50 {'상회' if safe_float(qqq.module_meta['trend']['close'])>safe_float(qqq.module_meta['trend']['ma50']) else '하회'}","status":classify_equity(qqq)},
        {"section":"Equities","label":"KOSPI","value":fmt_num(kospi.module_meta["trend"]["close"]),"d1":fmt_pct(pct_change(kospi.module_meta["trend"]["close"],last_valid(mkt["KOSPI"]["Close"].dropna(),2))),"d5":fmt_pct(kospi.module_meta["tactical"]["ret10"]),"aux":f"MA50 {'상회' if safe_float(kospi.module_meta['trend']['close'])>safe_float(kospi.module_meta['trend']['ma50']) else '하회'}","status":classify_equity(kospi)},
        {"section":"Equities","label":"KOSDAQ","value":fmt_num(kosdaq.module_meta["trend"]["close"]),"d1":fmt_pct(pct_change(kosdaq.module_meta["trend"]["close"],last_valid(mkt["KOSDAQ"]["Close"].dropna(),2))),"d5":fmt_pct(kosdaq.module_meta["tactical"]["ret10"]),"aux":f"MA50 {'상회' if safe_float(kosdaq.module_meta['trend']['close'])>safe_float(kosdaq.module_meta['trend']['ma50']) else '하회'}","status":classify_equity(kosdaq)},
        {"section":"Vol / Rates","label":"VIX","value":fmt_num(spy.module_meta["vix"]["vix"]),"d1":fmt_pct(pct_change(spy.module_meta["vix"]["vix"],last_valid(mkt["VIX"]["Close"].dropna(),2))),"d5":fmt_pct(spy.module_meta["vix"]["vix_5d_chg"]),"aux":f"MA20 대비 {fmt_num(spy.module_meta['vix']['vix_ratio20'],3)}","status":classify_vol(spy.module_meta["vix"]["vix"],spy.module_meta["vix"]["vix_ratio20"])},
        {"section":"Vol / Rates","label":"VKOSPI","value":fmt_num(kospi.module_meta["vkospi"]["vkospi"]),"d1":fmt_pct(pct_change(kospi.module_meta["vkospi"]["vkospi"],safe_float(list(read_krx_cache().get("vkospi",{}).values())[-2] if len(read_krx_cache().get("vkospi",{}))>=2 else None))),"d5":fmt_pct(kospi.module_meta["vkospi"]["vkospi_5d_chg"]),"aux":f"MA20 대비 {fmt_num(kospi.module_meta['vkospi']['vkospi_ratio20'],3)}","status":classify_vkospi(kospi.module_meta["vkospi"]["vkospi"],kospi.module_meta["vkospi"]["vkospi_ratio20"])},
        {"section":"Vol / Rates","label":"US 10Y","value":f"{fmt_num(macro['us10y'],2)}%","d1":fmt_bp(macro["us10y_1d_bp"]),"d5":fmt_bp(macro["us10y_5d_bp"]),"aux":f"20일 {fmt_bp(macro['us10y_20d_bp'])}","status":classify_rates(macro["us10y_20d_bp"])},
        {"section":"Vol / Rates","label":"2Y-10Y","value":f"{fmt_num(macro['curve_2s10s'],2)}%","d1":fmt_bp(macro["curve_2s10s_1d_bp"]),"d5":fmt_bp(macro["curve_2s10s_5d_bp"]),"aux":"Steepening" if safe_float(macro["curve_2s10s_5d_bp"],0)>0 else "Flattening","status":"good" if safe_float(macro["curve_2s10s_5d_bp"],0)>5 else "warn"},
        {"section":"FX / Safe Haven","label":"USD/KRW","value":fmt_num(kospi.module_meta["fx"]["usdkrw"]),"d1":fmt_pct(pct_change(kospi.module_meta["fx"]["usdkrw"],last_valid(mkt["USDKRW"]["Close"].dropna(),2))),"d5":fmt_pct(pct_change(kospi.module_meta["fx"]["usdkrw"],last_valid(mkt["USDKRW"]["Close"].dropna(),6))),"aux":f"20일 {fmt_pct(kospi.module_meta['fx']['usdkrw_ret20'])}","status":classify_fx(kospi.module_meta["fx"]["usdkrw_ret20"])},
        {"section":"FX / Safe Haven","label":"Dollar Index","value":fmt_num(macro["dxy"]),"d1":fmt_pct(macro["dxy_1d"]),"d5":fmt_pct(macro["dxy_5d"]),"aux":"20일선 상회" if safe_float(macro["dxy"])>safe_float(macro["dxy_ma20"],1e9) else "20일선 하회","status":"warn" if safe_float(macro["dxy"])>safe_float(macro["dxy_ma20"],1e9) else "good"},
        {"section":"FX / Safe Haven","label":"Gold","value":fmt_num(macro["gold"],1),"d1":fmt_pct(macro["gold_1d"]),"d5":fmt_pct(macro["gold_5d"]),"aux":"안전자산 강세" if safe_float(macro["gold_5d"],0)>0 else "중립","status":"good" if safe_float(macro["gold_5d"],0)>0 else "warn"},
        {"section":"FX / Credit","label":"HY OAS","value":f"{fmt_num(macro['hy_oas'],2)}%","d1":fmt_bp(macro["hy_oas_1d_bp"]),"d5":fmt_bp(macro["hy_oas_5d_bp"]),"aux":"신용 경계" if safe_float(macro["hy_oas"],0)>=3.5 else "신용 안정","status":"bad" if safe_float(macro["hy_oas"],0)>=4.0 else "warn" if safe_float(macro["hy_oas"],0)>=3.5 else "good"},
    ]
    main_cards_html = "".join(f'''<div class="main-card"><div><div class="main-card-top"><div><div class="main-section">{c["section"]}</div><div class="main-title">{c["label"]}</div></div><div class="mini-badge {c["status"]}">{status_to_badge(c["status"])}</div></div><div class="main-value">{c["value"]}</div><div class="main-metrics"><div class="row"><span>1일 변화</span><span>{c["d1"]}</span></div><div class="row"><span>5일 변화</span><span>{c["d5"]}</span></div><div class="row"><span>판단</span><span>{c["aux"]}</span></div></div></div></div>''' for c in main_cards)

    score = sum(1 if c["status"]=="good" else -1 if c["status"]=="bad" else 0 for c in main_cards)
    if score>=3: regime_text="RISK-ON"; regime_class="riskon"; regime_desc="주식/변동성/신용 전반이 비교적 안정적입니다."
    elif score<=-3: regime_text="RISK-OFF"; regime_class="riskoff"; regime_desc="금리·달러·변동성 부담이 우세합니다."
    else: regime_text="NEUTRAL"; regime_class="neutral"; regime_desc="강세와 약세 신호가 혼재합니다."

    alerts = []
    if safe_float(macro["us10y_20d_bp"],0)>=20: alerts.append("US10Y 상승 → 성장주 밸류에이션 부담")
    if safe_float(kospi.module_meta["fx"]["usdkrw_ret20"],0)>=2: alerts.append("USD/KRW 급등 → 한국 자산 리스크 가중")
    if safe_float(macro["hy_oas"],0)>=3.5: alerts.append("HY 스프레드 확대 → 신용시장 점검 필요")
    if not alerts: alerts=["특이 경보 없음","리스크 신호 혼재","US/KR 상세탭 병행 확인"]
    alerts_html = "".join(f"<li>{x}</li>" for x in alerts[:3])

    css = """*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{background:#0d0d14;color:#d4d4e0;font-family:"IBM Plex Sans KR",sans-serif;min-height:100vh;padding:32px 16px}
.page-header{max-width:960px;margin:0 auto 28px;border-bottom:1px solid #252538;padding-bottom:20px}
.page-title{font-family:"IBM Plex Mono",monospace;font-size:22px;font-weight:600;color:#f0f0f8;letter-spacing:-0.5px}
.tab-bar{max-width:960px;margin:0 auto;display:flex;gap:0;border-bottom:2px solid #252538;flex-wrap:wrap}
.tab-btn{font-family:"IBM Plex Mono",monospace;font-size:14px;font-weight:600;padding:12px 28px;border:none;background:#161622;color:#7070a0;cursor:pointer;border-bottom:3px solid transparent;margin-bottom:-2px;transition:color .15s,border-color .15s,background .15s;letter-spacing:0.5px;-webkit-appearance:none;-moz-appearance:none;appearance:none}
.tab-btn:hover{color:#c0c0e0;background:#1e1e30}
.tab-btn.active{color:#f0f0f8;border-bottom-color:#5b9bd5;background:#0d0d14}
.tab-link{font-family:"IBM Plex Mono",monospace;font-size:14px;font-weight:600;padding:12px 28px;background:#161622;color:#7070a0;text-decoration:none;display:flex;align-items:center;border-bottom:3px solid transparent;transition:color .15s,border-color .15s,background .15s}
.tab-link:hover{color:#c0c0e0;background:#1e1e30}
.tab-content{display:none}.tab-content.active{display:block}
.sub-tab-bar{max-width:960px;margin:16px auto 0;display:flex;gap:0;border-bottom:1px solid #252538}
.sub-tab-btn{font-family:"IBM Plex Mono",monospace;font-size:12px;font-weight:600;padding:8px 24px;border:none;background:transparent;color:#7070a0;cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-1px;transition:color .15s,border-color .15s;-webkit-appearance:none;appearance:none}
.sub-tab-btn:hover{color:#c0c0e0}
.sub-tab-btn.active{color:#f0f0f8;border-bottom-color:#60a5fa}
.sub-tab-content{display:none}.sub-tab-content.active{display:block}
.night-btn-wrap{max-width:960px;margin:14px auto 0;display:flex;justify-content:flex-end}
.night-btn{font-family:"IBM Plex Mono",monospace;font-size:12px;font-weight:600;padding:8px 18px;border-radius:8px;background:#1a1a2e;color:#60a5fa;border:1px solid #60a5fa44;text-decoration:none;display:inline-flex;align-items:center;gap:6px;transition:background .15s,border-color .15s}
.night-btn:hover{background:#1f1f38;border-color:#60a5fa88}
.update-bar{max-width:960px;margin:20px auto 28px;display:flex;gap:12px;flex-wrap:wrap}
.update-badge{font-family:"IBM Plex Mono",monospace;font-size:11px;padding:6px 14px;border-radius:6px;border:1px solid #252538;background:#161622;display:flex;align-items:center;gap:8px}
.update-badge .label{color:#8888aa;font-weight:600}
.update-badge .time{color:#b0b0cc}
.cards-container{max-width:960px;margin:0 auto;display:grid;grid-template-columns:repeat(auto-fit,minmax(420px,1fr));gap:24px}
.asset-card{background:#13131f;border:1px solid #252538;border-radius:14px;padding:28px;transition:border-color .2s}
.asset-card:hover{border-color:#353558}
.card-header{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:20px}
.asset-name{font-family:"IBM Plex Mono",monospace;font-size:30px;font-weight:700;color:#f0f0f8;letter-spacing:-1px}
.signal-badge{font-family:"IBM Plex Mono",monospace;font-size:17px;font-weight:700;padding:7px 20px;border-radius:8px;letter-spacing:1px}
.score-display{margin:16px 0 8px;font-family:"IBM Plex Mono",monospace;line-height:1}
.modules-section{margin:20px 0;display:flex;flex-direction:column;gap:9px}
.mod-row{display:grid;grid-template-columns:80px 58px 1fr;align-items:center;gap:10px}
.mod-label{font-size:11px;color:#9090b8;font-family:"IBM Plex Mono",monospace;font-weight:600}
.mod-score{font-family:"IBM Plex Mono",monospace;font-size:13px;color:#c0c0d8;text-align:right;font-weight:600}
.details-grid{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-top:20px;padding-top:20px;border-top:1px solid #252538}
.detail-group{display:flex;flex-direction:column;gap:6px}
.section-label{font-family:"IBM Plex Mono",monospace;font-size:10px;color:#5b5b80;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:6px;font-weight:700}
.detail-row{display:flex;justify-content:space-between;font-size:12.5px;font-family:"IBM Plex Mono",monospace;padding:2px 0;border-bottom:1px solid #1a1a28}
.detail-row span:first-child{color:#9090b8}
.detail-row span:last-child{color:#e0e0f0;font-weight:600}
.guardrail-box{margin-top:16px;padding:12px 14px;background:rgba(245,200,66,0.07);border:1px solid rgba(245,200,66,0.25);border-radius:8px}
.guardrail-box .section-label{color:#f5c842}
.footer{max-width:960px;margin:48px auto 0;text-align:center;font-size:11px;color:#353558;font-family:"IBM Plex Mono",monospace;line-height:2}
.main-wrap{max-width:960px;margin:0 auto}
.main-hero{display:grid;grid-template-columns:1.05fr 1.35fr;gap:18px;margin-bottom:24px}
.hero-card{background:#13131f;border:1px solid #252538;border-radius:14px;padding:22px}
.hero-label{font-family:"IBM Plex Mono",monospace;font-size:11px;color:#8c8cab;margin-bottom:10px;letter-spacing:1px;text-transform:uppercase}
.hero-value{font-family:"IBM Plex Mono",monospace;font-size:34px;font-weight:700;margin-bottom:8px}
.hero-value.riskon{color:#22c55e}.hero-value.neutral{color:#f5c842}.hero-value.riskoff{color:#e53e3e}
.hero-desc{font-size:14px;color:#b8b8d2;line-height:1.5}
.alert-list{margin:8px 0 0 18px;color:#d9d9ea;font-size:13px;line-height:1.7}
.main-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:18px}
.main-card{background:#13131f;border:1px solid #252538;border-radius:14px;padding:18px;min-height:190px;display:flex;flex-direction:column;justify-content:space-between}
.main-card-top{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}
.main-section{font-family:"IBM Plex Mono",monospace;font-size:10px;color:#6f6f96;letter-spacing:1px;text-transform:uppercase;margin-bottom:6px}
.main-title{font-family:"IBM Plex Mono",monospace;font-size:18px;font-weight:700;color:#f0f0f8}
.main-value{font-family:"IBM Plex Mono",monospace;font-size:30px;font-weight:700;color:#f0f0f8;margin:16px 0 14px}
.main-metrics{display:flex;flex-direction:column;gap:6px}
.main-metrics .row{display:flex;justify-content:space-between;font-size:12.5px;font-family:"IBM Plex Mono",monospace;border-bottom:1px solid #1a1a28;padding-bottom:4px}
.main-metrics .row span:first-child{color:#9090b8}
.main-metrics .row span:last-child{color:#e0e0f0;font-weight:600}
.mini-badge{font-family:"IBM Plex Mono",monospace;font-size:11px;font-weight:700;border-radius:999px;padding:6px 10px;white-space:nowrap}
.mini-badge.good{background:rgba(34,197,94,0.12);border:1px solid rgba(34,197,94,0.28);color:#22c55e}
.mini-badge.warn{background:rgba(245,200,66,0.12);border:1px solid rgba(245,200,66,0.28);color:#f5c842}
.mini-badge.bad{background:rgba(229,62,62,0.12);border:1px solid rgba(229,62,62,0.28);color:#e53e3e}
@media(max-width:1100px){.main-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.main-hero{grid-template-columns:1fr}}
@media(max-width:520px){.cards-container{grid-template-columns:1fr}.details-grid{grid-template-columns:1fr}.tab-btn{padding:10px 20px;font-size:13px}.sub-tab-btn{padding:7px 14px;font-size:11px}.main-grid{grid-template-columns:1fr}}"""

    js = """(function(){
  document.querySelectorAll('.tab-btn[data-tab]').forEach(function(btn){
    btn.addEventListener('click', function(){
      var name = this.getAttribute('data-tab');
      document.querySelectorAll('.tab-content').forEach(function(el){el.classList.remove('active');});
      document.querySelectorAll('.tab-btn[data-tab]').forEach(function(el){el.classList.remove('active');});
      document.getElementById('tab-' + name).classList.add('active');
      this.classList.add('active');
      try{localStorage.setItem('lastTab', name);}catch(e){}
    });
  });
  document.querySelectorAll('.sub-tab-btn[data-subtab]').forEach(function(btn){
    btn.addEventListener('click', function(){
      var name = this.getAttribute('data-subtab');
      document.querySelectorAll('.sub-tab-content').forEach(function(el){el.classList.remove('active');});
      document.querySelectorAll('.sub-tab-btn[data-subtab]').forEach(function(el){el.classList.remove('active');});
      document.getElementById('subtab-' + name).classList.add('active');
      this.classList.add('active');
      try{localStorage.setItem('lastSubTab', name);}catch(e){}
    });
  });
  try{
    var last = localStorage.getItem('lastTab');
    if(last){ var t = document.querySelector('[data-tab="' + last + '"]'); if(t) t.click(); }
    var lastSub = localStorage.getItem('lastSubTab');
    if(lastSub){ var s = document.querySelector('[data-subtab="' + lastSub + '"]'); if(s) s.click(); }
  }catch(e){}
})();"""

    return f'''<!DOCTYPE html>
<html lang="ko"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Market Decision Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans+KR:wght@300;400;600&display=swap" rel="stylesheet">
<style>{css}</style></head><body>
<div class="page-header"><div class="page-title">MARKET DECISION DASHBOARD</div></div>
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
      <div class="hero-card"><div class="hero-label">Global Regime</div><div class="hero-value {regime_class}">{regime_text}</div><div class="hero-desc">{regime_desc}</div></div>
      <div class="hero-card"><div class="hero-label">오늘의 경보</div><ul class="alert-list">{alerts_html}</ul></div>
    </div>
    <div class="main-grid">{main_cards_html}</div>
  </div>
</div>
<div id="tab-us" class="tab-content"><div class="cards-container">{us_cards}</div></div>
<div id="tab-kr" class="tab-content">
  <div class="night-btn-wrap"><a class="night-btn" href="night_futures.html" target="_blank">🌙 야간선물 Night Futures ↗</a></div>
  <div class="sub-tab-bar">
    <button class="sub-tab-btn active" data-subtab="kospi">KOSPI</button>
    <button class="sub-tab-btn" data-subtab="kosdaq">KOSDAQ</button>
    <button class="sub-tab-btn" data-subtab="ewy">EWY</button>
  </div>
  <div id="subtab-kospi" class="sub-tab-content active"><div class="cards-container" style="margin-top:24px;">{kospi_card}</div></div>
  <div id="subtab-kosdaq" class="sub-tab-content"><div class="cards-container" style="margin-top:24px;">{kosdaq_card}</div></div>
  <div id="subtab-ewy" class="sub-tab-content"><div class="cards-container" style="margin-top:24px;">{ewy_card}</div></div>
</div>
<div class="footer">
  <p>score ≥ 70 → 매수 &nbsp;|&nbsp; 40–69 → 보유 &nbsp;|&nbsp; &lt; 40 → 매도</p>
  <p>US: 매 거래일 06:30 KST &nbsp;|&nbsp; KR: 매 거래일 16:30 KST</p>
</div>
<script>{js}</script>
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
    macro = build_macro_summary(mkt, fred, prev_state)

    ts = fmt_ts_kst()
    print("[INFO] Generating HTML...")
    html = generate_html(us_results, kr_results, ts, ts, macro, mkt=mkt)
    with open(INDEX_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    state = {
        "updated_kst": ts,
        "macro": macro,
        "us_results": {k: {"asset":v.asset,"total_score":v.total_score,"signal":v.signal,
                            "original_signal":v.original_signal,"module_scores":v.module_scores,
                            "module_meta":v.module_meta,"guardrail_reasons":v.guardrail_reasons}
                       for k,v in us_results.items()},
        "kr_results": {k: {"asset":v.asset,"total_score":v.total_score,"signal":v.signal,
                            "original_signal":v.original_signal,"module_scores":v.module_scores,
                            "module_meta":v.module_meta,"guardrail_reasons":v.guardrail_reasons}
                       for k,v in kr_results.items()},
    }
    write_json(STATE_FILE, state)
    print(f"[DONE] wrote {INDEX_FILE}")
    print(f"[DONE] wrote {STATE_FILE}")

if __name__ == "__main__":
    main()
