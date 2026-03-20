# -*- coding: utf-8 -*-
import warnings
warnings.filterwarnings("ignore")

import os, json, time, re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from io import BytesIO

import numpy as np
import pandas as pd
import yfinance as yf
import requests

KST = timezone(timedelta(hours=9))
USE_AUTO_ADJUST = True
YF_PERIOD = "2y"
REQUEST_TIMEOUT = 15

KRX_API_KEY    = os.environ.get("KRX_API_KEY", "")
KRX_JSESSIONID = os.environ.get("KRX_JSESSIONID", None)
KIS_APP_KEY    = os.environ.get("KIS_APP_KEY", "")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "")
RUN_MODE = os.environ.get("RUN_MODE", "ALL")  # "US", "KR", "ALL"


# =========================================================
# Helpers
# =========================================================
def get_asof_compact():
    return pd.Timestamp.today().strftime("%Y%m%d")

def safe_download_yf(tickers, period="2y", auto_adjust=True):
    df = yf.download(tickers=tickers, period=period, interval="1d",
                     auto_adjust=auto_adjust, progress=False, threads=True, group_by="ticker")
    if df.empty: raise ValueError("Empty DataFrame.")
    if isinstance(df.columns, pd.MultiIndex):
        out_list = []
        for t in tickers:
            if t not in df.columns.get_level_values(0): continue
            sub = df[t].copy()
            if "Close" in sub.columns: out_list.append(sub["Close"].rename(t))
        if not out_list: raise ValueError("No Close columns.")
        return pd.concat(out_list, axis=1).sort_index()
    if "Close" in df.columns and len(tickers) == 1:
        return df[["Close"]].rename(columns={"Close": tickers[0]}).sort_index()
    raise ValueError("Unexpected format.")

def safe_download_one_of(candidates, period="2y", auto_adjust=True):
    for t in candidates:
        try:
            df = safe_download_yf([t], period=period, auto_adjust=auto_adjust)
            s = df[t].dropna()
            if len(s) >= 60: return t, s
        except: pass
    raise RuntimeError(f"All failed: {candidates}")

def rolling_slope(series, lookback=10):
    s = series.dropna()
    if len(s) < lookback + 1: return np.nan
    return float(s.iloc[-1] - s.iloc[-lookback - 1])

def pct_change_n(series, n):
    s = series.dropna()
    if len(s) < n + 1: return np.nan
    return float(s.iloc[-1] / s.iloc[-n - 1] - 1.0)

def classify_signal(score):
    if score >= 70: return "매수"
    elif score >= 40: return "보유"
    return "매도"

def downgrade_signal(signal):
    order = ["매도","보유","매수"]
    return order[max(0, order.index(signal) - 1)]

def cap_signal(signal, max_signal):
    order = {"매도":0,"보유":1,"매수":2}
    return signal if order[signal] <= order[max_signal] else max_signal

def floor_signal(signal, min_signal):
    order = {"매도":0,"보유":1,"매수":2}
    return signal if order[signal] >= order[min_signal] else min_signal

def clean_num_series(s):
    return pd.to_numeric(
        s.astype(str).str.replace(",","",regex=False).str.replace(" ","",regex=False)
         .replace({"":np.nan,"-":np.nan,"None":np.nan,"nan":np.nan}), errors="coerce")

def normalize_mkt_id(market):
    return {"KOSPI":"STK","STK":"STK","KOSDAQ":"KSQ","KSQ":"KSQ"}.get(str(market).upper().strip(), market)

def extract_first_list_from_json(js):
    if isinstance(js, list): return pd.DataFrame(js)
    if isinstance(js, dict):
        for _, v in js.items():
            if isinstance(v, list): return pd.DataFrame(v)
    return pd.DataFrame()

def get_business_dates(end_date, n):
    bdays = pd.bdate_range(end=pd.Timestamp(end_date), periods=n)
    return [d.strftime("%Y%m%d") for d in bdays]

def calc_window_start(end_date, window_bdays):
    return get_business_dates(end_date, window_bdays)[0]

def normalize_colname(name):
    return re.sub(r"[\s\(\)\[\]_/.\-]", "", str(name)).strip().lower()

def find_matching_col(df, candidates):
    col_map = {normalize_colname(c): c for c in df.columns}
    for cand in candidates:
        if normalize_colname(cand) in col_map: return col_map[normalize_colname(cand)]
    return None

def first_matching_value(df, row_mask, col_candidates):
    col = find_matching_col(df, col_candidates)
    if col is None: return np.nan
    sub = clean_num_series(df.loc[row_mask, col]).dropna()
    return float(sub.iloc[0]) if len(sub) > 0 else np.nan


# =========================================================
# KRX Cache
# =========================================================
CACHE_FILE = "docs/krx_cache.json"

def load_krx_cache():
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE,"r",encoding="utf-8") as f:
                return json.load(f)
    except: pass
    return {}

def save_krx_cache(cache):
    os.makedirs("docs",exist_ok=True)
    with open(CACHE_FILE,"w",encoding="utf-8") as f:
        json.dump(cache,f,ensure_ascii=False)

def series_to_cache(s: pd.Series) -> dict:
    return {str(k.date()): v for k,v in s.items()}

def cache_to_series(d: dict, name: str) -> pd.Series:
    return pd.Series(
        {pd.Timestamp(k): float(v) for k,v in d.items()},
        name=name
    ).sort_index()


# =========================================================
# KRX Data Provider
# =========================================================
class KRXDataProvider:
    def __init__(self, api_key, jsessionid=None, timeout=REQUEST_TIMEOUT):
        self.api_key = api_key; self.jsessionid = jsessionid; self.timeout = timeout
        self.session = requests.Session()
        self.openapi_base = "https://data-dbg.krx.co.kr/svc/apis"
        self.referer_flow = "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020301"

    def _openapi_get(self, path, params):
        r = self.session.get(self.openapi_base + path,
            headers={"AUTH_KEY":self.api_key,"Accept":"application/json","User-Agent":"Mozilla/5.0"},
            params=params, timeout=self.timeout)
        r.raise_for_status(); return r.json()

    def _fetch_vkospi_one_day(self, bas_dd):
        js = self._openapi_get("/idx/drvprod_dd_trd", {"basDd": bas_dd})
        df = extract_first_list_from_json(js)
        if df.empty: return None
        name_col = next((c for c in ["IDX_NM","IDX_NM_KOR","ITEM_NM","지수명"] if c in df.columns), None)
        value_col = next((c for c in ["CLSPRC_IDX","CLSPRC","TDD_CLSPRC","CLOSE","종가"] if c in df.columns), None)
        if not name_col or not value_col: return None
        nm = df[name_col].astype(str)
        sub = df[nm.str.contains("변동성",na=False) & nm.str.contains("코스피",na=False)]
        if sub.empty: sub = df[nm.str.contains("KOSPI",case=False,na=False) & nm.str.contains("VOL",case=False,na=False)]
        if sub.empty: return None
        val = pd.to_numeric(sub.iloc[0][value_col], errors="coerce")
        return float(val) if pd.notna(val) else None

    def _fetch_turnover_one_day(self, bas_dd, market):
        mkt = normalize_mkt_id(market)
        path = "/sto/stk_bydd_trd" if mkt=="STK" else "/sto/ksq_bydd_trd"
        js = self._openapi_get(path, {"basDd": bas_dd})
        df = extract_first_list_from_json(js)
        if df.empty: return None
        val_col = next((c for c in ["ACC_TRDVAL","TDD_TRDVAL","TRDVAL","TOT_TRDVAL"] if c in df.columns), None)
        if not val_col:
            vc = [c for c in df.columns if "VAL" in c.upper()]
            val_col = vc[0] if len(vc)==1 else None
        if not val_col: return None
        return float(clean_num_series(df[val_col]).sum())

    def load_vkospi_series(self, asof_date, cache):
        cache_key = "vkospi"
        cached = cache.get(cache_key, {})
        biz_dates = get_business_dates(asof_date, 260)
        missing = [d for d in biz_dates if d not in cached]
        new_count = 0
        for bas_dd in missing:
            try:
                val = self._fetch_vkospi_one_day(bas_dd)
                if val is not None:
                    cached[bas_dd] = val
                    new_count += 1
                time.sleep(0.05)
            except: continue
        cache[cache_key] = cached
        if new_count > 0: print(f"  VKOSPI: {new_count}일 신규 수집 (캐시: {len(cached)}일)")
        else: print(f"  VKOSPI: 캐시 사용 ({len(cached)}일)")
        s = cache_to_series({d: cached[d] for d in biz_dates if d in cached}, "VKOSPI")
        if s.empty: raise RuntimeError("VKOSPI empty.")
        return s

    def load_turnover_series(self, asof_date, market, cache):
        cache_key = f"turnover_{market}"
        cached = cache.get(cache_key, {})
        biz_dates = get_business_dates(asof_date, 260)
        missing = [d for d in biz_dates if d not in cached]
        new_count = 0
        for bas_dd in missing:
            try:
                val = self._fetch_turnover_one_day(bas_dd, market)
                if val is not None:
                    cached[bas_dd] = val
                    new_count += 1
                time.sleep(0.03)
            except: continue
        cache[cache_key] = cached
        name = f"{market}_turnover"
        if new_count > 0: print(f"  {name}: {new_count}일 신규 수집")
        else: print(f"  {name}: 캐시 사용")
        s = cache_to_series({d: cached[d] for d in biz_dates if d in cached}, name)
        if s.empty: raise RuntimeError(f"{name} empty.")
        return s

    def load_flow_snapshot(self, asof_date, market, window):
        """KIS OpenAPI로 투자자별 순매수 거래대금 조회"""
        # window일치 확보를 위해 충분히 넉넉하게 시작일 설정 (영업일 기준 여유있게 2배)
        start_dt = (pd.Timestamp(asof_date) - pd.tseries.offsets.BDay(window * 2)).strftime("%Y%m%d")
        try:
            token_r = requests.post(
                "https://openapi.koreainvestment.com:9443/oauth2/tokenP",
                json={"grant_type":"client_credentials","appkey":KIS_APP_KEY,"appsecret":KIS_APP_SECRET},
                timeout=20
            )
            token_r.raise_for_status()
            token = token_r.json()["access_token"]

            market_info = {"KOSPI":{"iscd":"0001","mkt":"KSP"},"KOSDAQ":{"iscd":"1001","mkt":"KSQ"}}
            info = market_info.get(market, market_info["KOSPI"])

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
                "FID_INPUT_DATE_1": start_dt,
                "FID_INPUT_ISCD_1": info["mkt"],
                "FID_INPUT_DATE_2": asof_date,
                "FID_INPUT_ISCD_2": info["iscd"],
            }
            r = requests.get(
                "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-investor-daily-by-market",
                headers=headers, params=params, timeout=20
            )
            r.raise_for_status()
            data = r.json()
            if data.get("rt_cd") != "0":
                raise RuntimeError(f"KIS API 오류: {data}")

            output = data.get("output", [])
            if not output:
                raise RuntimeError("KIS 빈 데이터")

            df = pd.DataFrame(output)
            for c in df.columns:
                if c != "stck_bsop_date":
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            df = df.sort_values("stck_bsop_date", ascending=False).reset_index(drop=True)

            # 최근 window 영업일치 슬라이스 (단위: 백만원 → /100 = 억원)
            n = min(window, len(df))
            foreign_net  = float(df["frgn_ntby_tr_pbmn"].iloc[:n].sum()) / 100
            inst_net     = float(df["orgn_ntby_tr_pbmn"].iloc[:n].sum()) / 100
            combined_net = foreign_net + inst_net
            print(f"  [FLOW] {market} {window}D ({n}행): 외국인={foreign_net:.0f}억, 기관={inst_net:.0f}억")
        except Exception as e:
            print(f"[FLOW WARN] {market} {window}D: {e}")
            foreign_net = inst_net = combined_net = np.nan
        return {"window":window,"foreign_net_buy":foreign_net,"institution_net_buy":inst_net,"combined_net_buy":combined_net}


# =========================================================
# US Scoring
# =========================================================
def score_trend_spy(price):
    close=price.dropna(); c=close.iloc[-1]
    ma50=close.rolling(50).mean().iloc[-1]; ma200=close.rolling(200).mean().iloc[-1]
    slope50=rolling_slope(close.rolling(50).mean(),10); ret20=pct_change_n(close,20)
    score=(15 if c>ma200 else 0)+(10 if c>ma50 else 0)+(5 if slope50>0 else 0)+(5 if ret20>0 else 0)
    return score, {"close":c,"ma50":ma50,"ma200":ma200,"ret20":ret20,"slope50":slope50}

def score_trend_qqq(price):
    close=price.dropna(); c=close.iloc[-1]
    ma50=close.rolling(50).mean().iloc[-1]; ma200=close.rolling(200).mean().iloc[-1]
    slope50=rolling_slope(close.rolling(50).mean(),10); ret20=pct_change_n(close,20)
    score=(12 if c>ma200 else 0)+(8 if c>ma50 else 0)+(5 if slope50>0 else 0)+(5 if ret20>0 else 0)
    return score, {"close":c,"ma50":ma50,"ma200":ma200,"ret20":ret20,"slope50":slope50}

def score_vix(vix):
    v=vix.dropna(); curr=v.iloc[-1]; chg5=pct_change_n(v,5)
    ma20=v.rolling(20).mean().iloc[-1]; ratio20=curr/ma20 if pd.notna(ma20) and ma20!=0 else np.nan
    high10=v.rolling(10).max().iloc[-1]; dd=(curr/high10-1.0) if pd.notna(high10) and high10!=0 else np.nan
    score=0
    if curr<13: score+=2
    elif curr<17: score+=5
    elif curr<=25: score+=8
    elif curr<=35: score+=6
    else: score+=3
    if chg5<-0.10: score+=7
    elif chg5<0: score+=5
    elif chg5<=0.10: score+=2
    if pd.notna(ratio20):
        if 0.9<=ratio20<=1.15: score+=4
        elif 1.15<ratio20<=1.4: score+=5
        elif ratio20>1.4: score+=1
        else: score+=2
    if pd.notna(dd):
        if dd<=-0.15: score+=5
        elif dd<=-0.05: score+=3
    return score, {"vix":curr,"vix_5d_chg":chg5,"vix_ma20":ma20,"vix_ratio20":ratio20,"vix_dd_from_10d_high":dd}

def score_tactical_us(price, asset):
    close=price.dropna(); c=close.iloc[-1]
    ma20=close.rolling(20).mean().iloc[-1]; ma5=close.rolling(5).mean().iloc[-1]
    dist20=c/ma20-1.0 if pd.notna(ma20) and ma20!=0 else np.nan; ret10=pct_change_n(close,10)
    prev_close=close.iloc[-2] if len(close)>=2 else np.nan
    prev_ma5=close.rolling(5).mean().iloc[-2] if len(close)>=5 else np.nan
    cross5=pd.notna(prev_close) and pd.notna(prev_ma5) and (prev_close<=prev_ma5) and (c>ma5)
    two_of_3=int((close.diff().tail(3)>0).sum())>=2
    low5=close.tail(5).min() if len(close)>=5 else np.nan
    rebound=pd.notna(low5) and low5>0 and ((c/low5-1.0)>=0.03)
    score=0
    if asset=="SPY":
        if dist20<=-0.06: score+=6
        elif dist20<=-0.02: score+=4
        elif dist20<=0.04: score+=2
    else:
        if dist20<=-0.08: score+=6
        elif dist20<=-0.03: score+=4
        elif dist20<=0.05: score+=2
    if cross5 or two_of_3 or rebound: score+=5
    score+=(4 if ret10<=0.06 else 0) if asset=="SPY" else (4 if ret10<=0.08 else 0)
    return score, {"dist20":dist20,"ret10":ret10,"cond_cross_5dma":cross5,"cond_2_of_3_up":two_of_3,"cond_rebound_3pct":rebound}

def score_breadth_proxy(proxy_ratio, asset):
    s=proxy_ratio.dropna()
    if len(s)<60:
        return (8 if asset=="SPY" else 5), {"proxy_ratio":np.nan,"proxy_ma50":np.nan,"proxy_roc20":np.nan,"bucket":"neutral_fallback","approx_breadth":0.50}
    curr=s.iloc[-1]; ma50=s.rolling(50).mean().iloc[-1]; roc20=pct_change_n(s,20)
    ss=(1 if curr>ma50 else 0)+(1 if roc20>0 else 0)
    if asset=="SPY":
        if ss==2: score,bucket,ab=15,"strong",0.70
        elif ss==1: score,bucket,ab=8,"neutral",0.50
        else: score,bucket,ab=2,"weak",0.28
    else:
        if ss==2: score,bucket,ab=10,"strong",0.70
        elif ss==1: score,bucket,ab=5,"neutral",0.50
        else: score,bucket,ab=1,"weak",0.28
    return score, {"proxy_ratio":curr,"proxy_ma50":ma50,"proxy_roc20":roc20,"bucket":bucket,"approx_breadth":ab}

def score_rates_spy(dgs10):
    y=dgs10.dropna(); curr=y.iloc[-1]
    d20=(curr-y.iloc[-21])*100 if len(y)>=21 else np.nan
    score=0
    if pd.notna(d20):
        if d20<=-20: score+=6
        elif d20<=10: score+=4
        elif d20<=30: score+=2
    score+=4 if curr<4.0 else (2 if curr<=4.5 else 0)
    return score, {"dgs10":curr,"delta20_bp":d20}

def score_rates_qqq(dgs10):
    y=dgs10.dropna(); curr=y.iloc[-1]
    d20=(curr-y.iloc[-21])*100 if len(y)>=21 else np.nan
    score=0
    if pd.notna(d20):
        if d20<=-20: score+=12
        elif d20<=10: score+=8
        elif d20<=30: score+=4
    score+=8 if curr<4.0 else (4 if curr<=4.5 else 0)
    return score, {"dgs10":curr,"delta20_bp":d20}

def apply_guardrails_us(base_signal, trend_meta, vix_meta, breadth_meta, rate_meta, asset):
    reasons=[]; signal=base_signal
    close,ma50,ma200=trend_meta["close"],trend_meta["ma50"],trend_meta["ma200"]
    ret20=trend_meta["ret20"]; vix_5d=vix_meta["vix_5d_chg"]
    ab=breadth_meta.get("approx_breadth",np.nan); d20=rate_meta.get("delta20_bp",np.nan)
    if (close<ma200) and (vix_5d>0):
        signal=cap_signal(signal,"보유"); reasons.append("200일선 하회 + VIX 상승 → 매수 제한")
    if (close<ma50) and (close<ma200) and (ret20<0):
        signal="매도"; reasons.append("50/200일선 하회 + 20일 수익률 음수 → 매도 우선")
    if pd.notna(ab) and ab<0.30:
        signal=cap_signal(signal,"보유"); reasons.append("breadth 약세 → 매수 제한")
    if asset=="QQQ" and pd.notna(d20) and d20>30:
        signal=downgrade_signal(signal); reasons.append("10년물 20일 급등 → QQQ 한 단계 하향")
    if (close>ma200) and (vix_5d<-0.10) and (pd.notna(ab) and ab>=0.50):
        signal=floor_signal(signal,"보유"); reasons.append("상승추세 + VIX 진정 + breadth 중립 이상 → 최소 보유")
    return signal, reasons


# =========================================================
# KR Scoring
# =========================================================
def score_trend_kospi(price):
    close=price.dropna(); c=close.iloc[-1]
    ma50=close.rolling(50).mean().iloc[-1]; ma200=close.rolling(200).mean().iloc[-1]
    slope50=rolling_slope(close.rolling(50).mean(),10); ret20=pct_change_n(close,20)
    score=(18 if c>ma200 else 0)+(12 if c>ma50 else 0)+(5 if slope50>0 else 0)+(5 if ret20>0 else 0)
    return score, {"close":c,"ma50":ma50,"ma200":ma200,"ret20":ret20,"slope50":slope50}

def score_trend_kosdaq(price):
    close=price.dropna(); c=close.iloc[-1]
    ma50=close.rolling(50).mean().iloc[-1]; ma200=close.rolling(200).mean().iloc[-1]
    slope50=rolling_slope(close.rolling(50).mean(),10); ret20=pct_change_n(close,20)
    score=(16 if c>ma200 else 0)+(10 if c>ma50 else 0)+(5 if slope50>0 else 0)+(5 if ret20>0 else 0)
    return score, {"close":c,"ma50":ma50,"ma200":ma200,"ret20":ret20,"slope50":slope50}

def score_vkospi(vkospi):
    v=vkospi.dropna(); curr=v.iloc[-1]; chg5=pct_change_n(v,5)
    ma20=v.rolling(20).mean().iloc[-1]; ratio20=curr/ma20 if pd.notna(ma20) and ma20!=0 else np.nan
    high10=v.rolling(10).max().iloc[-1]; dd=(curr/high10-1.0) if pd.notna(high10) and high10!=0 else np.nan
    score=0
    if curr<15: score+=2
    elif curr<20: score+=6
    elif curr<=28: score+=10
    elif curr<=38: score+=7
    else: score+=2
    if chg5<-0.10: score+=8
    elif chg5<0: score+=5
    elif chg5<=0.10: score+=2
    if pd.notna(ratio20):
        if 0.9<=ratio20<=1.15: score+=4
        elif 1.15<ratio20<=1.4: score+=5
        elif ratio20>1.4: score+=1
        else: score+=2
    if pd.notna(dd):
        if dd<=-0.15: score+=5
        elif dd<=-0.05: score+=3
    return score, {"vkospi":curr,"vkospi_5d_chg":chg5,"vkospi_ratio20":ratio20,"vkospi_dd_from_10d_high":dd}

def score_tactical_kr(price, asset):
    close=price.dropna(); c=close.iloc[-1]
    ma20=close.rolling(20).mean().iloc[-1]; ma5=close.rolling(5).mean().iloc[-1]
    dist20=c/ma20-1.0 if pd.notna(ma20) and ma20!=0 else np.nan; ret10=pct_change_n(close,10)
    prev_close=close.iloc[-2] if len(close)>=2 else np.nan
    prev_ma5=close.rolling(5).mean().iloc[-2] if len(close)>=5 else np.nan
    cross5=pd.notna(prev_close) and pd.notna(prev_ma5) and (prev_close<=prev_ma5) and (c>ma5)
    two_of_3=int((close.diff().tail(3)>0).sum())>=2
    low5=close.tail(5).min() if len(close)>=5 else np.nan
    rebound=pd.notna(low5) and low5>0 and ((c/low5-1.0)>=0.03)
    score=0
    if asset=="KOSPI":
        if dist20<=-0.07: score+=7
        elif dist20<=-0.03: score+=5
        elif dist20<=0.04: score+=2
    else:
        if dist20<=-0.08: score+=7
        elif dist20<=-0.04: score+=5
        elif dist20<=0.05: score+=2
    if cross5 or two_of_3 or rebound: score+=6
    score+=(4 if ret10<=0.06 else 0) if asset=="KOSPI" else (4 if ret10<=0.08 else 0)
    return score, {"dist20":dist20,"ret10":ret10,"cond_cross_5dma":cross5,"cond_2_of_3_up":two_of_3,"cond_rebound_3pct":rebound}

def score_leadership(rs_ratio, asset):
    s=rs_ratio.dropna()
    if len(s)<60:
        return (3 if asset=="KOSPI" else 5), {"ratio":np.nan,"ma50":np.nan,"roc20":np.nan,"bucket":"neutral_fallback","approx_leadership":0.50}
    curr=s.iloc[-1]; ma50=s.rolling(50).mean().iloc[-1]; roc20=pct_change_n(s,20)
    ss=(1 if curr>ma50 else 0)+(1 if roc20>0 else 0)
    if asset=="KOSDAQ":
        if ss==2: score,bucket,al=12,"strong",0.70
        elif ss==1: score,bucket,al=6,"neutral",0.50
        else: score,bucket,al=1,"weak",0.28
    else:
        if ss==2: score,bucket,al=1,"kosdaq_strong",0.70
        elif ss==1: score,bucket,al=3,"neutral",0.50
        else: score,bucket,al=6,"kosdaq_weak",0.28
    return score, {"ratio":curr,"ma50":ma50,"roc20":roc20,"bucket":bucket,"approx_leadership":al}

def score_turnover(turnover_series, asset):
    s=turnover_series.dropna(); curr=s.iloc[-1]
    ma20=s.rolling(20).mean().iloc[-1]; ratio20=curr/ma20 if pd.notna(ma20) and ma20!=0 else np.nan; roc5=pct_change_n(s,5)
    score=0
    if pd.notna(ratio20):
        if 0.90<=ratio20<1.10: score+=4
        elif 1.10<=ratio20<1.40: score+=8
        elif 1.40<=ratio20<1.80: score+=6
        else: score+=2
    if pd.notna(roc5):
        if roc5>0.15: score+=4
        elif roc5>0: score+=2
    if asset=="KOSDAQ" and score>0: score=min(score+1,12)
    return score, {"turnover":curr,"turnover_ma20":ma20,"turnover_ratio20":ratio20,"turnover_roc5":roc5}

def score_flow_by_market(flow_1d, flow_5d, flow_20d, turnover_series, asset):
    tc=float(turnover_series.dropna().iloc[-1]) if len(turnover_series.dropna()) else np.nan
    f1,i1,c1=flow_1d["foreign_net_buy"],flow_1d["institution_net_buy"],flow_1d["combined_net_buy"]
    f5,i5,c5=flow_5d["foreign_net_buy"],flow_5d["institution_net_buy"],flow_5d["combined_net_buy"]
    f20,i20,c20=flow_20d["foreign_net_buy"],flow_20d["institution_net_buy"],flow_20d["combined_net_buy"]
    c5r=c5/(tc*5) if pd.notna(tc) and tc!=0 else np.nan
    c20r=c20/(tc*20) if pd.notna(tc) and tc!=0 else np.nan
    score=0
    if pd.notna(f1) and f1>0: score+=2
    if pd.notna(i1) and i1>0: score+=1
    if pd.notna(f1) and pd.notna(i1) and f1>0 and i1>0: score+=2
    if pd.notna(c5r):
        if c5r>0.020: score+=6
        elif c5r>0.005: score+=4
        elif c5r>0: score+=2
        elif c5r>-0.010: score+=1
    if pd.notna(c20r):
        if c20r>0.015: score+=6
        elif c20r>0.003: score+=4
        elif c20r>0: score+=2
        elif c20r>-0.010: score+=1
    if pd.notna(f5) and f5>0: score+=1
    if pd.notna(f20) and f20>0: score+=2
    if asset=="KOSDAQ" and score>0: score=min(score+1,18)
    return score, {"flow_1d":flow_1d,"flow_5d":flow_5d,"flow_20d":flow_20d,
                   "turnover_curr":tc,"combined_5d_ratio":c5r,"combined_20d_ratio":c20r}

def score_fx_usdkrw(usdkrw, asset):
    s=usdkrw.dropna(); curr=s.iloc[-1]; ma50=s.rolling(50).mean().iloc[-1]
    ret20=pct_change_n(s,20); ret5=pct_change_n(s,5)
    score=8 if curr<ma50 else 2
    if pd.notna(ret20):
        if ret20<-0.03: score+=8
        elif ret20<0: score+=5
        elif ret20<=0.03: score+=2
    if pd.notna(ret5):
        if ret5<0: score+=4
        elif ret5<=0.02: score+=2
    if asset=="KOSDAQ" and score>0: score=max(score-1,0)
    return score, {"usdkrw":curr,"usdkrw_ma50":ma50,"usdkrw_ret20":ret20,"usdkrw_ret5":ret5}

def score_oil_wti(oil, asset):
    s=oil.dropna(); curr=s.iloc[-1]; ret20=pct_change_n(s,20); ret5=pct_change_n(s,5); ma50=s.rolling(50).mean().iloc[-1]
    score=0
    if pd.notna(ret20):
        if ret20<-0.10: score+=6
        elif ret20<=0.05: score+=4
        elif ret20<=0.15: score+=2
    if pd.notna(ret5):
        score+=(0 if ret5>0.10 else (2 if ret5>=-0.05 else 1))
    if pd.notna(ma50): score+=(4 if curr<=ma50*1.05 else 1)
    if asset=="KOSDAQ" and score>0: score=max(score-1,0)
    return score, {"wti":curr,"wti_ret20":ret20,"wti_ret5":ret5,"wti_ma50":ma50}

def apply_guardrails_kr(base_signal, asset, trend_meta, v_meta, leadership_meta, flow_meta, fx_meta, oil_meta):
    reasons=[]; signal=base_signal
    close,ma50,ma200,ret20=trend_meta["close"],trend_meta["ma50"],trend_meta["ma200"],trend_meta["ret20"]
    v_5d=v_meta["vkospi_5d_chg"]; leadership=leadership_meta.get("approx_leadership",np.nan)
    f1=flow_meta["flow_1d"]["foreign_net_buy"]; i1=flow_meta["flow_1d"]["institution_net_buy"]
    c5r=flow_meta["combined_5d_ratio"]; c20r=flow_meta["combined_20d_ratio"]
    usdkrw_ret20=fx_meta.get("usdkrw_ret20",np.nan)
    if (close<ma50) and (close<ma200) and (ret20<0):
        signal="매도"; reasons.append("50/200일선 하회 + 20일 수익률 음수 → 매도 우선")
    if (close<ma200) and (v_5d>0):
        signal=cap_signal(signal,"보유"); reasons.append("200일선 하회 + VKOSPI 상승 → 매수 제한")
    if asset=="KOSDAQ" and pd.notna(leadership) and leadership<0.30:
        signal=cap_signal(signal,"보유"); reasons.append("코스닥 리더십 약세 → 매수 제한")
    if pd.notna(f1) and pd.notna(i1) and f1<0 and i1<0:
        signal=cap_signal(signal,"보유"); reasons.append("외국인·기관 1일 동시 순매도 → 매수 제한")
    if pd.notna(c5r) and pd.notna(c20r) and c5r<-0.01 and c20r<-0.005:
        signal=cap_signal(signal,"보유"); reasons.append("5일·20일 누적 수급 동반 부진 → 매수 제한")
    if pd.notna(usdkrw_ret20) and usdkrw_ret20>0.03 and pd.notna(c5r) and c5r<0:
        signal=cap_signal(signal,"보유"); reasons.append("원/달러 급등 + 단기 수급 약세 → 매수 제한")
    if (close>ma200) and (v_5d<-0.10):
        signal=floor_signal(signal,"보유"); reasons.append("상승추세 + 변동성 진정 → 최소 보유")
    return signal, reasons


@dataclass
class AssetResult:
    asset: str
    total_score: int
    raw_signal: str
    final_signal: str
    module_scores: Dict
    module_meta: Dict
    guardrail_reasons: List[str]


# =========================================================
# Engines
# =========================================================
def build_us_results():
    prices = safe_download_yf(["SPY","QQQ","^VIX","RSP"])
    try: qqqew_ticker, qqqew_series = safe_download_one_of(["QQEW","QQQE","QEW"])
    except: qqqew_ticker="QQQ_FALLBACK"; qqqew_series=prices["QQQ"].copy()
    prices["QEW_PROXY"] = qqqew_series
    spy,qqq,vix,rsp = prices["SPY"].dropna(),prices["QQQ"].dropna(),prices["^VIX"].dropna(),prices["RSP"].dropna()
    qew = prices["QEW_PROXY"].dropna()
    dgs10_raw = yf.download("^TNX", period="2y", interval="1d", auto_adjust=True, progress=False)
    dgs10 = (dgs10_raw["Close"]["^TNX"] if isinstance(dgs10_raw.columns, pd.MultiIndex) else dgs10_raw["Close"])
    dgs10 = pd.to_numeric(dgs10, errors="coerce").dropna().sort_index()
    idx = spy.index.union(qqq.index).union(vix.index).sort_values()
    dgs10_a = dgs10.reindex(idx).ffill().dropna()
    results = {}
    for asset, price, bf, rates_fn, trend_fn in [
        ("SPY", spy, (rsp/spy).dropna(), score_rates_spy, score_trend_spy),
        ("QQQ", qqq, (qew/qqq).dropna(), score_rates_qqq, score_trend_qqq),
    ]:
        ts,tm=trend_fn(price); vs,vm=score_vix(vix); xs,xm=score_tactical_us(price,asset)
        bs,bm=score_breadth_proxy(bf,asset); rs,rm=rates_fn(dgs10_a)
        total=int(ts+vs+xs+bs+rs); raw=classify_signal(total)
        final,reasons=apply_guardrails_us(raw,tm,vm,bm,rm,asset)
        results[asset]=AssetResult(asset=asset,total_score=total,raw_signal=raw,final_signal=final,
            module_scores={"trend":ts,"vix":vs,"tactical":xs,"breadth":bs,"rates":rs},
            module_meta={"trend":tm,"vix":vm,"tactical":xm,"breadth":bm,"rates":rm},
            guardrail_reasons=reasons)
    return results

def build_kr_results():
    prices = safe_download_yf(["^KS11","^KQ11","KRW=X","CL=F"])
    kospi,kosdaq=prices["^KS11"].dropna(),prices["^KQ11"].dropna()
    usdkrw,wti=prices["KRW=X"].dropna(),prices["CL=F"].dropna()
    lr=(kosdaq/kospi).dropna(); ac=get_asof_compact()
    krx=KRXDataProvider(api_key=KRX_API_KEY,jsessionid=KRX_JSESSIONID)

    # 캐시 로드
    cache = load_krx_cache()

    vkospi=krx.load_vkospi_series(ac, cache)
    kt=krx.load_turnover_series(ac,"KOSPI", cache)
    kqt=krx.load_turnover_series(ac,"KOSDAQ", cache)

    # 캐시 저장 (VKOSPI + 거래대금 저장)
    save_krx_cache(cache)

    # KIS 수급: 토큰 1회 발급 → KOSPI/KOSDAQ 공유
    def _fetch_kis_flow_all_windows(market, asof, token):
        """KIS API 1회 호출로 1D/5D/20D 수급 스냅샷 모두 반환"""
        start_dt = (pd.Timestamp(asof) - pd.tseries.offsets.BDay(60)).strftime("%Y%m%d")
        empty = lambda w: {"window":w,"foreign_net_buy":np.nan,"institution_net_buy":np.nan,"combined_net_buy":np.nan}
        try:
            info = {"KOSPI":{"iscd":"0001","mkt":"KSP"},"KOSDAQ":{"iscd":"1001","mkt":"KSQ"}}[market]
            hdrs = {"content-type":"application/json; charset=utf-8",
                    "authorization":f"Bearer {token}","appkey":KIS_APP_KEY,
                    "appsecret":KIS_APP_SECRET,"tr_id":"FHPTJ04040000","custtype":"P"}
            params = {"FID_COND_MRKT_DIV_CODE":"U","FID_INPUT_ISCD":info["iscd"],
                      "FID_INPUT_DATE_1":start_dt,"FID_INPUT_ISCD_1":info["mkt"],
                      "FID_INPUT_DATE_2":asof,"FID_INPUT_ISCD_2":info["iscd"]}
            r = requests.get(
                "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-investor-daily-by-market",
                headers=hdrs, params=params, timeout=20)
            data = r.json()
            if data.get("rt_cd") != "0": raise RuntimeError(f"KIS 오류: {data}")
            df = pd.DataFrame(data["output"])
            for c in df.columns:
                if c != "stck_bsop_date": df[c] = pd.to_numeric(df[c], errors="coerce")
            df = df.sort_values("stck_bsop_date", ascending=False).reset_index(drop=True)
            def snap(w):
                n = min(w, len(df))
                fg  = float(df["frgn_ntby_tr_pbmn"].iloc[:n].sum()) / 100  # 백만원 → 억원
                ins = float(df["orgn_ntby_tr_pbmn"].iloc[:n].sum()) / 100
                print(f"  [FLOW] {market} {w}D({n}행): 외국인={fg:.0f}억, 기관={ins:.0f}억")
                return {"window":w,"foreign_net_buy":fg,"institution_net_buy":ins,"combined_net_buy":fg+ins}
            return snap(1), snap(5), snap(20)
        except Exception as e:
            print(f"[FLOW WARN] {market}: {e}")
            return empty(1), empty(5), empty(20)

    # 토큰 1회만 발급
    try:
        _kis_token = requests.post(
            "https://openapi.koreainvestment.com:9443/oauth2/tokenP",
            json={"grant_type":"client_credentials","appkey":KIS_APP_KEY,"appsecret":KIS_APP_SECRET},
            timeout=20
        ).json()["access_token"]
        print(f"  [KIS] 토큰 발급 성공")
    except Exception as e:
        _kis_token = ""
        print(f"  [KIS WARN] 토큰 발급 실패: {e}")

    kf1,kf5,kf20 = _fetch_kis_flow_all_windows("KOSPI",  ac, _kis_token)
    qf1,qf5,qf20 = _fetch_kis_flow_all_windows("KOSDAQ", ac, _kis_token)
    results={}
    for asset,price,turnover,flow_1d,flow_5d,flow_20d,trend_fn in [
        ("KOSPI",kospi,kt,kf1,kf5,kf20,score_trend_kospi),
        ("KOSDAQ",kosdaq,kqt,qf1,qf5,qf20,score_trend_kosdaq),
    ]:
        ts,tm=trend_fn(price); vs,vm=score_vkospi(vkospi); xs,xm=score_tactical_kr(price,asset)
        ls,lm=score_leadership(lr,asset); ms,mm=score_turnover(turnover,asset)
        fs,fm=score_flow_by_market(flow_1d,flow_5d,flow_20d,turnover,asset)
        fxs,fxm=score_fx_usdkrw(usdkrw,asset); os_,om=score_oil_wti(wti,asset)
        total=int(ts+vs+xs+ls+ms+fs+fxs+os_); raw=classify_signal(total)
        final,reasons=apply_guardrails_kr(raw,asset,tm,vm,lm,fm,fxm,om)
        results[asset]=AssetResult(asset=asset,total_score=total,raw_signal=raw,final_signal=final,
            module_scores={"trend":ts,"vkospi":vs,"tactical":xs,"leadership":ls,"turnover":ms,"flow":fs,"fx":fxs,"oil":os_},
            module_meta={"trend":tm,"vkospi":vm,"tactical":xm,"leadership":lm,"turnover":mm,"flow":fm,"fx":fxm,"oil":om},
            guardrail_reasons=reasons)
    return results


# =========================================================
# HTML Helpers
# =========================================================
def signal_color(signal):
    return {"매수":"#00d084","보유":"#f5c842","매도":"#e53e3e"}.get(signal,"#888")

def signal_bg(signal):
    return {"매수":"rgba(0,208,132,0.12)","보유":"rgba(245,200,66,0.12)","매도":"rgba(229,62,62,0.12)"}.get(signal,"rgba(136,136,136,0.1)")

def score_bar(score, max_score=100):
    pct=min(100,score/max_score*100)
    color="#00d084" if pct>=70 else ("#f5c842" if pct>=40 else "#e53e3e")
    return f'<div style="background:#1a1a2e;border-radius:4px;height:8px;width:100%;overflow:hidden;"><div style="height:100%;width:{pct:.1f}%;background:{color};border-radius:4px;"></div></div>'

def fmt(v, fmt_str=".2f"):
    if v is None or (isinstance(v,float) and np.isnan(v)): return "<span style='color:#4a4a6a'>—</span>"
    if fmt_str=="pct": return f"{v*100:.2f}%"
    if fmt_str=="bp": return f"{v:+.1f}bp"
    if fmt_str=="억":
        # 값이 이미 억원 단위로 전달됨
        if abs(v) >= 10000: return f"{v/10000:.1f}조"
        return f"{v:+,.0f}억"
    return f"{v:{fmt_str}}"

def bool_badge(v):
    return "<span style='color:#00d084;font-weight:700;'>✓</span>" if v else "<span style='color:#4a4a6a;'>✗</span>"

def make_card(r, ms):
    col=signal_color(r.final_signal); bg=signal_bg(r.final_signal)
    is_kr = r.asset in ("KOSPI","KOSDAQ")
    guardrail_html=""
    if r.guardrail_reasons:
        items="".join(f"<li>{g}</li>" for g in r.guardrail_reasons)
        guardrail_html=f'<div class="guardrail-box"><div class="section-label">⚠ 가드레일 발동</div><ul style="margin:6px 0 0 16px;padding:0;color:#f5c842;font-size:12px;">{items}</ul></div>'
    labels={"trend":"추세","vix":"VIX","vkospi":"VKOSPI","tactical":"전술","breadth":"Breadth",
            "leadership":"리더십","turnover":"거래대금","flow":"수급","rates":"금리","fx":"환율","oil":"유가"}
    module_rows="".join(
        f'<div class="mod-row"><span class="mod-label">{labels.get(mod,mod)}</span><span class="mod-score">{s}<span style="color:#4a4a6a">/{ms.get(mod,20)}</span></span>{score_bar(s,ms.get(mod,20))}</div>'
        for mod,s in r.module_scores.items())
    raw_eq=f'<span style="color:#6060a0;font-size:11px;">원신호: {r.raw_signal}</span>' if r.raw_signal!=r.final_signal else ""
    tm=r.module_meta["trend"]
    if is_kr:
        vm=r.module_meta["vkospi"]; xm=r.module_meta["tactical"]; lm=r.module_meta["leadership"]
        mm=r.module_meta["turnover"]; fm=r.module_meta["flow"]; fxm=r.module_meta["fx"]; om=r.module_meta["oil"]
        f1=fm["flow_1d"]; f5=fm["flow_5d"]; f20=fm["flow_20d"]
        detail_html=f'''<div class="details-grid">
          <div class="detail-group"><div class="section-label">추세</div>
            <div class="detail-row"><span>종가</span><span>{fmt(tm["close"])}</span></div>
            <div class="detail-row"><span>MA50</span><span>{fmt(tm["ma50"])}</span></div>
            <div class="detail-row"><span>MA200</span><span>{fmt(tm["ma200"])}</span></div>
            <div class="detail-row"><span>20일 수익률</span><span>{fmt(tm["ret20"],"pct")}</span></div></div>
          <div class="detail-group"><div class="section-label">VKOSPI</div>
            <div class="detail-row"><span>현재</span><span>{fmt(vm["vkospi"])}</span></div>
            <div class="detail-row"><span>5일 변화</span><span>{fmt(vm["vkospi_5d_chg"],"pct")}</span></div>
            <div class="detail-row"><span>MA20 대비</span><span>{fmt(vm["vkospi_ratio20"],".3f")}</span></div>
            <div class="detail-row"><span>10일고점 대비</span><span>{fmt(vm["vkospi_dd_from_10d_high"],"pct")}</span></div></div>
          <div class="detail-group"><div class="section-label">전술 / 리더십</div>
            <div class="detail-row"><span>20일선 이격</span><span>{fmt(xm["dist20"],"pct")}</span></div>
            <div class="detail-row"><span>10일 수익률</span><span>{fmt(xm["ret10"],"pct")}</span></div>
            <div class="detail-row"><span>리더십 버킷</span><span>{lm["bucket"]}</span></div>
            <div class="detail-row"><span>추정 리더십</span><span>{fmt(lm["approx_leadership"],"pct")}</span></div></div>
          <div class="detail-group"><div class="section-label">수급</div>
            <div class="detail-row"><span>1일 합산</span><span>{fmt(f1["combined_net_buy"],"억")}</span></div>
            <div class="detail-row"><span>5일 합산</span><span>{fmt(f5["combined_net_buy"],"억")}</span></div>
            <div class="detail-row"><span>20일 합산</span><span>{fmt(f20["combined_net_buy"],"억")}</span></div>
            <div class="detail-row"><span>5일비율</span><span>{fmt(fm["combined_5d_ratio"],"pct")}</span></div></div>
          <div class="detail-group"><div class="section-label">거래대금</div>
            <div class="detail-row"><span>현재</span><span>{fmt(mm["turnover"],"억")}</span></div>
            <div class="detail-row"><span>MA20 대비</span><span>{fmt(mm["turnover_ratio20"],".3f")}</span></div>
            <div class="detail-row"><span>5일 변화</span><span>{fmt(mm["turnover_roc5"],"pct")}</span></div></div>
          <div class="detail-group"><div class="section-label">환율 / 유가</div>
            <div class="detail-row"><span>USD/KRW</span><span>{fmt(fxm["usdkrw"])}</span></div>
            <div class="detail-row"><span>환율 20일</span><span>{fmt(fxm["usdkrw_ret20"],"pct")}</span></div>
            <div class="detail-row"><span>WTI</span><span>{fmt(om["wti"])}</span></div>
            <div class="detail-row"><span>WTI 20일</span><span>{fmt(om["wti_ret20"],"pct")}</span></div></div>
        </div>'''
    else:
        vm=r.module_meta["vix"]; xm=r.module_meta["tactical"]; bm=r.module_meta["breadth"]; rm=r.module_meta["rates"]
        detail_html=f'''<div class="details-grid">
          <div class="detail-group"><div class="section-label">추세</div>
            <div class="detail-row"><span>종가</span><span>{fmt(tm["close"])}</span></div>
            <div class="detail-row"><span>MA50</span><span>{fmt(tm["ma50"])}</span></div>
            <div class="detail-row"><span>MA200</span><span>{fmt(tm["ma200"])}</span></div>
            <div class="detail-row"><span>20일 수익률</span><span>{fmt(tm["ret20"],"pct")}</span></div></div>
          <div class="detail-group"><div class="section-label">VIX</div>
            <div class="detail-row"><span>현재</span><span>{fmt(vm["vix"])}</span></div>
            <div class="detail-row"><span>5일 변화</span><span>{fmt(vm["vix_5d_chg"],"pct")}</span></div>
            <div class="detail-row"><span>MA20 대비</span><span>{fmt(vm["vix_ratio20"],".3f")}</span></div>
            <div class="detail-row"><span>10일고점 대비</span><span>{fmt(vm["vix_dd_from_10d_high"],"pct")}</span></div></div>
          <div class="detail-group"><div class="section-label">전술</div>
            <div class="detail-row"><span>20일선 이격</span><span>{fmt(xm["dist20"],"pct")}</span></div>
            <div class="detail-row"><span>10일 수익률</span><span>{fmt(xm["ret10"],"pct")}</span></div>
            <div class="detail-row"><span>5일선 돌파</span><span>{bool_badge(xm["cond_cross_5dma"])}</span></div>
            <div class="detail-row"><span>3일 중 2일 상승</span><span>{bool_badge(xm["cond_2_of_3_up"])}</span></div></div>
          <div class="detail-group"><div class="section-label">Breadth / 금리</div>
            <div class="detail-row"><span>Breadth 버킷</span><span>{bm["bucket"]}</span></div>
            <div class="detail-row"><span>Approx 비율</span><span>{fmt(bm["approx_breadth"],"pct")}</span></div>
            <div class="detail-row"><span>US 10Y</span><span>{fmt(rm["dgs10"])}%</span></div>
            <div class="detail-row"><span>20일 변화</span><span>{fmt(rm["delta20_bp"],"bp")}</span></div></div>
        </div>'''
    return f'''<div class="asset-card" style="border-top:3px solid {col};">
      <div class="card-header">
        <div><div class="asset-name">{r.asset}</div></div>
        <div style="text-align:right;"><div class="signal-badge" style="background:{bg};color:{col};border:1px solid {col}44;">{r.final_signal}</div>{raw_eq}</div>
      </div>
      <div class="score-display"><span style="color:{col};font-size:42px;font-weight:700;letter-spacing:-2px;">{r.total_score}</span><span style="color:#4a4a6a;font-size:20px;">/100</span></div>
      {score_bar(r.total_score)}
      <div class="modules-section">{module_rows}</div>
      {detail_html}{guardrail_html}
    </div>'''


# =========================================================
# HTML Generation
# =========================================================
def generate_html(us_results, kr_results, us_updated, kr_updated):
    us_max={"SPY":{"trend":35,"vix":25,"tactical":15,"breadth":15,"rates":10},
            "QQQ":{"trend":30,"vix":25,"tactical":15,"breadth":10,"rates":20}}
    kr_max={"KOSPI":{"trend":40,"vkospi":27,"tactical":17,"leadership":6,"turnover":12,"flow":20,"fx":20,"oil":10},
            "KOSDAQ":{"trend":36,"vkospi":27,"tactical":17,"leadership":12,"turnover":12,"flow":18,"fx":20,"oil":10}}
    us_cards="".join(make_card(r,us_max.get(a,{})) for a,r in us_results.items())
    kr_cards="".join(make_card(r,kr_max.get(a,{})) for a,r in kr_results.items())
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

/* ── 탭 ── */
.tab-bar{{max-width:960px;margin:0 auto;display:flex;gap:0;border-bottom:2px solid #252538}}
.tab-btn{{font-family:"IBM Plex Mono",monospace;font-size:14px;font-weight:600;
  padding:12px 36px;border:none;background:#161622;color:#7070a0;
  cursor:pointer;border-bottom:3px solid transparent;margin-bottom:-2px;
  transition:color .15s,border-color .15s;letter-spacing:0.5px;
  -webkit-appearance:none;-moz-appearance:none;appearance:none}}
.tab-btn:hover{{color:#c0c0e0;background:#1e1e30}}
.tab-btn.active{{color:#f0f0f8;border-bottom-color:#5b9bd5;background:#0d0d14}}
.tab-content{{display:none}}.tab-content.active{{display:block}}

/* ── 업데이트 바 ── */
.update-bar{{max-width:960px;margin:20px auto 28px;display:flex;gap:12px;flex-wrap:wrap}}
.update-badge{{font-family:"IBM Plex Mono",monospace;font-size:11px;padding:6px 14px;border-radius:6px;
  border:1px solid #252538;background:#161622;display:flex;align-items:center;gap:8px}}
.update-badge .label{{color:#8888aa;font-weight:600}}
.update-badge .time{{color:#b0b0cc}}

/* ── 카드 ── */
.cards-container{{max-width:960px;margin:0 auto;display:grid;grid-template-columns:repeat(auto-fit,minmax(420px,1fr));gap:24px}}
.asset-card{{background:#13131f;border:1px solid #252538;border-radius:14px;padding:28px;transition:border-color .2s}}
.asset-card:hover{{border-color:#353558}}
.card-header{{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:20px}}
.asset-name{{font-family:"IBM Plex Mono",monospace;font-size:30px;font-weight:700;color:#f0f0f8;letter-spacing:-1px}}
.signal-badge{{font-family:"IBM Plex Mono",monospace;font-size:17px;font-weight:700;padding:7px 20px;border-radius:8px;letter-spacing:1px}}
.score-display{{margin:16px 0 8px;font-family:"IBM Plex Mono",monospace;line-height:1}}

/* ── 모듈 바 ── */
.modules-section{{margin:20px 0;display:flex;flex-direction:column;gap:9px}}
.mod-row{{display:grid;grid-template-columns:80px 58px 1fr;align-items:center;gap:10px}}
.mod-label{{font-size:11px;color:#9090b8;font-family:"IBM Plex Mono",monospace;font-weight:600}}
.mod-score{{font-family:"IBM Plex Mono",monospace;font-size:13px;color:#c0c0d8;text-align:right;font-weight:600}}

/* ── 세부 정보 ── */
.details-grid{{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-top:20px;padding-top:20px;border-top:1px solid #252538}}
.detail-group{{display:flex;flex-direction:column;gap:6px}}
.section-label{{font-family:"IBM Plex Mono",monospace;font-size:10px;color:#5b5b80;text-transform:uppercase;
  letter-spacing:1.5px;margin-bottom:6px;font-weight:700}}
.detail-row{{display:flex;justify-content:space-between;font-size:12.5px;font-family:"IBM Plex Mono",monospace;
  padding:2px 0;border-bottom:1px solid #1a1a28}}
.detail-row span:first-child{{color:#9090b8}}
.detail-row span:last-child{{color:#e0e0f0;font-weight:600}}

/* ── 가드레일 ── */
.guardrail-box{{margin-top:16px;padding:12px 14px;background:rgba(245,200,66,0.07);
  border:1px solid rgba(245,200,66,0.25);border-radius:8px}}
.guardrail-box .section-label{{color:#f5c842}}

/* ── 푸터 ── */
.footer{{max-width:960px;margin:48px auto 0;text-align:center;font-size:11px;
  color:#353558;font-family:"IBM Plex Mono",monospace;line-height:2}}
@media(max-width:520px){{
  .cards-container{{grid-template-columns:1fr}}
  .details-grid{{grid-template-columns:1fr}}
  .tab-btn{{padding:10px 20px;font-size:13px}}
}}
</style></head><body>
<div class="page-header">
  <div class="page-title">MARKET DECISION DASHBOARD</div>
</div>

<div class="tab-bar">
  <button class="tab-btn active" data-tab="us">🇺🇸 &nbsp;US</button>
  <button class="tab-btn" data-tab="kr">🇰🇷 &nbsp;KR</button>
</div>

<div class="update-bar">
  <div class="update-badge"><span class="label">🇺🇸 US 업데이트</span><span class="time">{us_updated}</span></div>
  <div class="update-badge"><span class="label">🇰🇷 KR 업데이트</span><span class="time">{kr_updated}</span></div>
</div>

<div id="tab-us" class="tab-content active"><div class="cards-container">{us_cards}</div></div>
<div id="tab-kr" class="tab-content"><div class="cards-container">{kr_cards}</div></div>

<div class="footer">
  <p>score ≥ 70 → 매수 &nbsp;|&nbsp; 40–69 → 보유 &nbsp;|&nbsp; &lt; 40 → 매도</p>
  <p>US: 매 거래일 06:30 KST &nbsp;|&nbsp; KR: 매 거래일 16:30 KST</p>
</div>

<script>
(function(){{
  var btns = document.querySelectorAll('.tab-btn');
  btns.forEach(function(btn){{
    btn.addEventListener('click', function(){{
      var name = this.getAttribute('data-tab');
      document.querySelectorAll('.tab-content').forEach(function(el){{el.classList.remove('active');}});
      document.querySelectorAll('.tab-btn').forEach(function(el){{el.classList.remove('active');}});
      document.getElementById('tab-' + name).classList.add('active');
      this.classList.add('active');
      try{{localStorage.setItem('lastTab', name);}}catch(e){{}}
    }});
  }});
  try{{
    var last = localStorage.getItem('lastTab');
    if(last){{
      var t = document.querySelector('[data-tab="' + last + '"]');
      if(t) t.click();
    }}
  }}catch(e){{}}
}})();
</script>
</body></html>'''


# =========================================================
# State (업데이트 시간 개별 유지)
# =========================================================
STATE_FILE = "docs/state.json"

def load_state():
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE,"r",encoding="utf-8") as f: return json.load(f)
    except: pass
    return {"us_updated":"—","kr_updated":"—","us_results":None,"kr_results":None}

def save_state(state):
    os.makedirs("docs",exist_ok=True)
    with open(STATE_FILE,"w",encoding="utf-8") as f: json.dump(state,f,ensure_ascii=False,indent=2)

def _clean(v):
    if isinstance(v, bool): return bool(v)
    if isinstance(v, np.bool_): return bool(v)
    if isinstance(v, np.integer): return int(v)
    if isinstance(v, np.floating): return None if np.isnan(v) else float(v)
    if isinstance(v, float): return None if np.isnan(v) else v
    if isinstance(v, dict): return {kk: _clean(vv) for kk, vv in v.items()}
    if isinstance(v, list): return [_clean(i) for i in v]
    return v

def results_to_json(results):
    out={}
    for asset,r in results.items():
        out[asset]={"asset":r.asset,"total_score":r.total_score,"raw_signal":r.raw_signal,
                    "final_signal":r.final_signal,"module_scores":r.module_scores,
                    "module_meta":_clean(r.module_meta),"guardrail_reasons":r.guardrail_reasons}
    return out

def json_to_results(data):
    if not data: return {}
    results={}
    for asset,d in data.items():
        results[asset]=AssetResult(asset=d["asset"],total_score=d["total_score"],raw_signal=d["raw_signal"],
            final_signal=d["final_signal"],module_scores=d["module_scores"],module_meta=d["module_meta"],
            guardrail_reasons=d["guardrail_reasons"])
    return results


# =========================================================
# Main
# =========================================================
if __name__ == "__main__":
    now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")
    state = load_state()

    if RUN_MODE in ("US","ALL"):
        print("📊 US 데이터 수집 중...")
        us_results = build_us_results()
        state["us_updated"] = now_kst
        state["us_results"] = results_to_json(us_results)
        for asset,r in us_results.items(): print(f"  {asset}: {r.total_score}점 | {r.final_signal}")
    else:
        us_results = json_to_results(state.get("us_results"))

    if RUN_MODE in ("KR","ALL"):
        if not KRX_API_KEY:
            print("[SKIP] KRX_API_KEY 없음")
            kr_results = json_to_results(state.get("kr_results"))
        else:
            print("📊 KR 데이터 수집 중...")
            kr_results = build_kr_results()
            state["kr_updated"] = now_kst
            state["kr_results"] = results_to_json(kr_results)
            for asset,r in kr_results.items(): print(f"  {asset}: {r.total_score}점 | {r.final_signal}")
    else:
        kr_results = json_to_results(state.get("kr_results"))

    save_state(state)
    os.makedirs("docs",exist_ok=True)
    html = generate_html(us_results, kr_results, state["us_updated"], state["kr_updated"])
    with open("docs/index.html","w",encoding="utf-8") as f: f.write(html)
    print(f"\n✓ 완료 ({now_kst})")
