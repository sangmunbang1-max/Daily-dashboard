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

KRX_API_KEY = os.environ.get("KRX_API_KEY", "")
KRX_JSESSIONID = os.environ.get("KRX_JSESSIONID", None)
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
        start_date = calc_window_start(asof_date, window)
        try:
            mkt = normalize_mkt_id(market)
            otp_url = "https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
            dl_url = "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
            hdrs = {"accept":"text/plain, */*; q=0.01","content-type":"application/x-www-form-urlencoded; charset=UTF-8",
                    "origin":"https://data.krx.co.kr","referer":self.referer_flow,"user-agent":"Mozilla/5.0","x-requested-with":"XMLHttpRequest"}
            cookies = {"lang":"ko_KR","mdc.client_session":"true"}
            if self.jsessionid: cookies["JSESSIONID"] = self.jsessionid
            payload = {"locale":"ko_KR","inqTpCd":"1","trdVolVal":"2","askBid":"3","mktId":mkt,
                       "strtDd":start_date,"endDd":asof_date,"share":"2","money":"3","csvxls_isNo":"false",
                       "name":"fileDown","url":"dbms/MDC/STAT/standard/MDCSTAT02201"}
            r_otp = self.session.post(otp_url, headers=hdrs, cookies=cookies, data=payload, timeout=self.timeout)
            r_otp.raise_for_status()
            otp_code = r_otp.text.strip()
            if not otp_code: raise RuntimeError("OTP empty.")
            r_csv = self.session.post(dl_url, headers={"Referer":self.referer_flow,"User-Agent":"Mozilla/5.0"},
                                      cookies=cookies, data={"code":otp_code}, timeout=self.timeout)
            r_csv.raise_for_status()
            df = None
            for enc in ["cp949","euc-kr","utf-8-sig"]:
                try: df = pd.read_csv(BytesIO(r_csv.content), encoding=enc); break
                except: continue
            if df is None or df.empty: raise RuntimeError("CSV parse fail.")
            df.columns = [str(c).strip() for c in df.columns]
            ic = find_matching_col(df, ["투자자구분","투자자","INVST_NM"])
            if ic and ic != "투자자": df = df.rename(columns={ic:"투자자"})
            net_col = find_matching_col(df, ["순매수거래대금","순매수 거래대금","순매수대금","순매수금액"])
            if not net_col:
                bc = find_matching_col(df, ["매수거래대금","매수대금","매수금액"])
                sc_ = find_matching_col(df, ["매도거래대금","매도대금","매도금액"])
                if bc and sc_:
                    for c in df.columns:
                        if c != "투자자": df[c] = clean_num_series(df[c])
                    df["순매수거래대금"] = df[bc].fillna(0) - df[sc_].fillna(0)
                    net_col = "순매수거래대금"
            for c in df.columns:
                if c != "투자자": df[c] = clean_num_series(df[c])
            investor = df["투자자"].astype(str).str.strip()
            foreign_net = first_matching_value(df, investor.str.contains("외국인",na=False), [net_col])
            inst_net = first_matching_value(df, investor.str.contains("기관합계|기관",na=False), [net_col])
            combined_net = np.nansum([foreign_net, inst_net])
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

    kf1=krx.load_flow_snapshot(ac,"KOSPI",1); kf5=krx.load_flow_snapshot(ac,"KOSPI",5); kf20=krx.load_flow_snapshot(ac,"KOSPI",20)
    qf1=krx.load_flow_snapshot(ac,"KOSDAQ",1); qf5=krx.load_flow_snapshot(ac,"KOSDAQ",5); qf20=krx.load_flow_snapshot(ac,"KOSDAQ",20)
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
    if v is None or (isinstance(v,float) and np.isnan(v)): return "<span style='color:#333'>—</span>"
    if fmt_str=="pct": return f"{v*100:.2f}%"
    if fmt_str=="bp": return f"{v:+.1f}bp"
    if fmt_str=="억":
        if abs(v)>=1e8: return f"{v/1e8:.1f}억"
        return f"{v/1e6:.0f}백만"
    return f"{v:{fmt_str}}"

def bool_badge(v):
    return "<span style='color:#00d084;'>✓</span>" if v else "<span style='color:#333;'>✗</span>"

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
        f'<div class="mod-row"><span class="mod-label">{labels.get(mod,mod)}</span><span class="mod-score">{s}<span style="color:#222">/{ms.get(mod,20)}</span></span>{score_bar(s,ms.get(mod,20))}</div>'
        for mod,s in r.module_scores.items())
    raw_eq=f'<span style="color:#333;font-size:11px;">원신호: {r.raw_signal}</span>' if r.raw_signal!=r.final_signal else ""
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
      <div class="score-display"><span style="color:{col};font-size:42px;font-weight:700;letter-spacing:-2px;">{r.total_score}</span><span style="color:#333;font-size:20px;">/100</span></div>
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
body{{background:#0a0a0f;color:#c8c8d8;font-family:"IBM Plex Sans KR",sans-serif;min-height:100vh;padding:32px 16px}}
.page-header{{max-width:960px;margin:0 auto 28px;border-bottom:1px solid #1a1a2e;padding-bottom:20px}}
.page-title{{font-family:"IBM Plex Mono",monospace;font-size:22px;font-weight:600;color:#e8e8f0}}
.tab-bar{{max-width:960px;margin:0 auto 0;display:flex;gap:4px;border-bottom:1px solid #1a1a2e;position:relative;z-index:10}}
.tab-btn{{font-family:"IBM Plex Mono",monospace;font-size:13px;font-weight:600;padding:10px 28px;border:none;
  background:transparent;color:#888;cursor:pointer !important;border-bottom:2px solid transparent;margin-bottom:-1px;
  transition:all .2s;pointer-events:auto !important;position:relative;z-index:11;outline:none}}
.tab-btn.active{{color:#e8e8f0;border-bottom-color:#e8e8f0}}
.tab-btn:hover{{color:#ccc}}
.tab-content{{display:none}}.tab-content.active{{display:block}}
.update-bar{{max-width:960px;margin:16px auto 24px;display:flex;gap:16px;flex-wrap:wrap}}
.update-badge{{font-family:"IBM Plex Mono",monospace;font-size:11px;padding:5px 14px;border-radius:4px;
  border:1px solid #1a1a2e;color:#555;display:flex;align-items:center;gap:6px}}
.update-badge .label{{color:#333}}.update-badge .time{{color:#888}}
.cards-container{{max-width:960px;margin:0 auto;display:grid;grid-template-columns:repeat(auto-fit,minmax(420px,1fr));gap:24px}}
.asset-card{{background:#0f0f1a;border:1px solid #1a1a2e;border-radius:12px;padding:28px;transition:border-color .2s}}
.asset-card:hover{{border-color:#2a2a3e}}
.card-header{{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:20px}}
.asset-name{{font-family:"IBM Plex Mono",monospace;font-size:28px;font-weight:600;color:#e8e8f0;letter-spacing:-1px}}
.signal-badge{{font-family:"IBM Plex Mono",monospace;font-size:18px;font-weight:600;padding:6px 18px;border-radius:6px;letter-spacing:1px}}
.score-display{{margin:16px 0 8px;font-family:"IBM Plex Mono",monospace;line-height:1}}
.modules-section{{margin:20px 0;display:flex;flex-direction:column;gap:8px}}
.mod-row{{display:grid;grid-template-columns:80px 55px 1fr;align-items:center;gap:10px}}
.mod-label{{font-size:11px;color:#333;font-family:"IBM Plex Mono",monospace}}
.mod-score{{font-family:"IBM Plex Mono",monospace;font-size:13px;color:#555;text-align:right}}
.details-grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:20px;padding-top:20px;border-top:1px solid #1a1a2e}}
.detail-group{{display:flex;flex-direction:column;gap:5px}}
.section-label{{font-family:"IBM Plex Mono",monospace;font-size:10px;color:#222;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px}}
.detail-row{{display:flex;justify-content:space-between;font-size:12px;color:#444;font-family:"IBM Plex Mono",monospace}}
.detail-row span:last-child{{color:#888}}
.guardrail-box{{margin-top:16px;padding:12px;background:rgba(245,200,66,0.05);border:1px solid rgba(245,200,66,0.15);border-radius:6px}}
.footer{{max-width:960px;margin:40px auto 0;text-align:center;font-size:11px;color:#1a1a2e;font-family:"IBM Plex Mono",monospace;line-height:1.8}}
@media(max-width:480px){{.cards-container{{grid-template-columns:1fr}}.details-grid{{grid-template-columns:1fr}}}}
</style></head><body>
<div class="page-header">
  <div class="page-title">MARKET DECISION DASHBOARD</div>
</div>
<div class="tab-bar">
  <button class="tab-btn active" onclick="switchTab('us',this)">🇺🇸 US</button>
  <button class="tab-btn" onclick="switchTab('kr',this)">🇰🇷 KR</button>
</div>
<div class="update-bar">
  <div class="update-badge"><span class="label">🇺🇸 US 업데이트</span><span class="time">{us_updated}</span></div>
  <div class="update-badge"><span class="label">🇰🇷 KR 업데이트</span><span class="time">{kr_updated}</span></div>
</div>
<div id="tab-us" class="tab-content active"><div class="cards-container">{us_cards}</div></div>
<div id="tab-kr" class="tab-content"><div class="cards-container">{kr_cards}</div></div>
<div class="footer">
  <p>score ≥ 70 → 매수 &nbsp;|&nbsp; 40–69 → 보유 &nbsp;|&nbsp; &lt; 40 → 매도</p>
  <p style="margin-top:4px;">US: 매 거래일 06:30 KST &nbsp;|&nbsp; KR: 매 거래일 16:30 KST</p>
</div>
<script>
function switchTab(name,btn){{
  document.querySelectorAll('.tab-content').forEach(el=>el.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(el=>el.classList.remove('active'));
  document.getElementById('tab-'+name).classList.add('active');
  btn.classList.add('active');
  try{{localStorage.setItem('activeTab',name);}}catch(e){{}}
}}
try{{
  var saved=localStorage.getItem('activeTab');
  if(saved){{var btn=document.querySelector('.tab-btn[onclick*="\''+saved+'\'"]');if(btn)switchTab(saved,btn);}}
}}catch(e){{}}
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
