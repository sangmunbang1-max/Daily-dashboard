# -*- coding: utf-8 -*-
import warnings
warnings.filterwarnings("ignore")

import os
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List

import numpy as np
import pandas as pd
import yfinance as yf

USE_AUTO_ADJUST = True
YF_PERIOD = "2y"
KST = timezone(timedelta(hours=9))

def safe_download_yf(tickers, period="2y", auto_adjust=True):
    df = yf.download(tickers=tickers, period=period, interval="1d", auto_adjust=auto_adjust, progress=False, threads=True, group_by="ticker")
    if df.empty:
        raise ValueError("Empty DataFrame.")
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

def load_fred_10y():
    df = yf.download("^TNX", period="2y", interval="1d", auto_adjust=True, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        s = df["Close"]["^TNX"]
    else:
        s = df["Close"]
    s = pd.to_numeric(s, errors="coerce").dropna().sort_index()
    return s

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
    order = ["매도", "보유", "매수"]
    return order[max(0, order.index(signal) - 1)]

def cap_signal(signal, max_signal):
    order = {"매도": 0, "보유": 1, "매수": 2}
    return signal if order[signal] <= order[max_signal] else max_signal

def floor_signal(signal, min_signal):
    order = {"매도": 0, "보유": 1, "매수": 2}
    return signal if order[signal] >= order[min_signal] else min_signal

def score_trend_spy(price):
    close = price.dropna()
    c = close.iloc[-1]
    ma50 = close.rolling(50).mean().iloc[-1]
    ma200 = close.rolling(200).mean().iloc[-1]
    slope50 = rolling_slope(close.rolling(50).mean(), 10)
    ret20 = pct_change_n(close, 20)
    score = 0
    score += 15 if c > ma200 else 0
    score += 10 if c > ma50 else 0
    score += 5 if slope50 > 0 else 0
    score += 5 if ret20 > 0 else 0
    return score, {"close": c, "ma50": ma50, "ma200": ma200, "ret20": ret20, "slope50": slope50}

def score_trend_qqq(price):
    close = price.dropna()
    c = close.iloc[-1]
    ma50 = close.rolling(50).mean().iloc[-1]
    ma200 = close.rolling(200).mean().iloc[-1]
    slope50 = rolling_slope(close.rolling(50).mean(), 10)
    ret20 = pct_change_n(close, 20)
    score = 0
    score += 12 if c > ma200 else 0
    score += 8 if c > ma50 else 0
    score += 5 if slope50 > 0 else 0
    score += 5 if ret20 > 0 else 0
    return score, {"close": c, "ma50": ma50, "ma200": ma200, "ret20": ret20, "slope50": slope50}

def score_vix(vix):
    v = vix.dropna()
    curr = v.iloc[-1]
    chg5 = pct_change_n(v, 5)
    ma20 = v.rolling(20).mean().iloc[-1]
    ratio20 = curr / ma20 if pd.notna(ma20) and ma20 != 0 else np.nan
    high10 = v.rolling(10).max().iloc[-1]
    dd = (curr / high10 - 1.0) if pd.notna(high10) and high10 != 0 else np.nan
    score = 0
    if curr < 13: score += 2
    elif curr < 17: score += 5
    elif curr <= 25: score += 8
    elif curr <= 35: score += 6
    else: score += 3
    if chg5 < -0.10: score += 7
    elif chg5 < 0: score += 5
    elif chg5 <= 0.10: score += 2
    if pd.notna(ratio20):
        if 0.9 <= ratio20 <= 1.15: score += 4
        elif 1.15 < ratio20 <= 1.4: score += 5
        elif ratio20 > 1.4: score += 1
        else: score += 2
    if pd.notna(dd):
        if dd <= -0.15: score += 5
        elif dd <= -0.05: score += 3
    return score, {"vix": curr, "vix_5d_chg": chg5, "vix_ma20": ma20, "vix_ratio20": ratio20, "vix_high10": high10, "vix_dd_from_10d_high": dd}

def score_tactical(price, asset):
    close = price.dropna()
    c = close.iloc[-1]
    ma20 = close.rolling(20).mean().iloc[-1]
    ma5 = close.rolling(5).mean().iloc[-1]
    dist20 = c / ma20 - 1.0 if pd.notna(ma20) and ma20 != 0 else np.nan
    ret10 = pct_change_n(close, 10)
    prev_close = close.iloc[-2] if len(close) >= 2 else np.nan
    prev_ma5 = close.rolling(5).mean().iloc[-2] if len(close) >= 5 else np.nan
    cross5 = pd.notna(prev_close) and pd.notna(prev_ma5) and (prev_close <= prev_ma5) and (c > ma5)
    last3 = close.diff().tail(3)
    two_of_3 = int((last3 > 0).sum()) >= 2
    low5 = close.tail(5).min() if len(close) >= 5 else np.nan
    rebound = pd.notna(low5) and low5 > 0 and ((c / low5 - 1.0) >= 0.03)
    score = 0
    if asset == "SPY":
        if dist20 <= -0.06: score += 6
        elif dist20 <= -0.02: score += 4
        elif dist20 <= 0.04: score += 2
    else:
        if dist20 <= -0.08: score += 6
        elif dist20 <= -0.03: score += 4
        elif dist20 <= 0.05: score += 2
    if cross5 or two_of_3 or rebound: score += 5
    if asset == "SPY": score += 4 if ret10 <= 0.06 else 0
    else: score += 4 if ret10 <= 0.08 else 0
    return score, {"dist20": dist20, "ret10": ret10, "ma20": ma20, "ma5": ma5, "cond_cross_5dma": cross5, "cond_2_of_3_up": two_of_3, "cond_rebound_3pct": rebound}

def score_breadth_proxy(proxy_ratio, asset):
    s = proxy_ratio.dropna()
    if len(s) < 60:
        score = 8 if asset == "SPY" else 5
        return score, {"proxy_ratio": np.nan, "proxy_ma50": np.nan, "proxy_roc20": np.nan, "bucket": "neutral_fallback", "approx_breadth": 0.50}
    curr = s.iloc[-1]
    ma50 = s.rolling(50).mean().iloc[-1]
    roc20 = pct_change_n(s, 20)
    ss = (1 if curr > ma50 else 0) + (1 if roc20 > 0 else 0)
    if asset == "SPY":
        if ss == 2: score, bucket, ab = 15, "strong", 0.70
        elif ss == 1: score, bucket, ab = 8, "neutral", 0.50
        else: score, bucket, ab = 2, "weak", 0.28
    else:
        if ss == 2: score, bucket, ab = 10, "strong", 0.70
        elif ss == 1: score, bucket, ab = 5, "neutral", 0.50
        else: score, bucket, ab = 1, "weak", 0.28
    return score, {"proxy_ratio": curr, "proxy_ma50": ma50, "proxy_roc20": roc20, "bucket": bucket, "approx_breadth": ab}

def score_rates_spy(dgs10):
    y = dgs10.dropna()
    curr = y.iloc[-1]
    d20 = (curr - y.iloc[-21]) * 100 if len(y) >= 21 else np.nan
    score = 0
    if pd.notna(d20):
        if d20 <= -20: score += 6
        elif d20 <= 10: score += 4
        elif d20 <= 30: score += 2
    score += 4 if curr < 4.0 else (2 if curr <= 4.5 else 0)
    return score, {"dgs10": curr, "delta20_bp": d20}

def score_rates_qqq(dgs10):
    y = dgs10.dropna()
    curr = y.iloc[-1]
    d20 = (curr - y.iloc[-21]) * 100 if len(y) >= 21 else np.nan
    score = 0
    if pd.notna(d20):
        if d20 <= -20: score += 12
        elif d20 <= 10: score += 8
        elif d20 <= 30: score += 4
    score += 8 if curr < 4.0 else (4 if curr <= 4.5 else 0)
    return score, {"dgs10": curr, "delta20_bp": d20}

def apply_guardrails(base_signal, trend_meta, vix_meta, breadth_meta, rate_meta, asset):
    reasons = []
    signal = base_signal
    close, ma50, ma200 = trend_meta["close"], trend_meta["ma50"], trend_meta["ma200"]
    ret20 = trend_meta["ret20"]
    vix_5d = vix_meta["vix_5d_chg"]
    ab = breadth_meta.get("approx_breadth", np.nan)
    d20 = rate_meta.get("delta20_bp", np.nan)
    if (close < ma200) and (vix_5d > 0):
        signal = cap_signal(signal, "보유")
        reasons.append("200일선 하회 + VIX 상승 → 매수 제한")
    if (close < ma50) and (close < ma200) and (ret20 < 0):
        signal = "매도"
        reasons.append("50/200일선 하회 + 20일 수익률 음수 → 매도 우선")
    if pd.notna(ab) and (ab < 0.30):
        signal = cap_signal(signal, "보유")
        reasons.append("breadth 약세 → 매수 제한")
    if asset == "QQQ" and pd.notna(d20) and (d20 > 30):
        signal = downgrade_signal(signal)
        reasons.append("10년물 20일 급등 → QQQ 한 단계 하향")
    if (close > ma200) and (vix_5d < -0.10) and (pd.notna(ab) and ab >= 0.50):
        signal = floor_signal(signal, "보유")
        reasons.append("상승추세 + VIX 진정 + breadth 중립 이상 → 최소 보유")
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

def build_results():
    prices = safe_download_yf(["SPY", "QQQ", "^VIX", "RSP"], period=YF_PERIOD, auto_adjust=USE_AUTO_ADJUST)
    try:
        qqqew_ticker, qqqew_series = safe_download_one_of(["QQEW", "QQQE", "QEW"])
    except:
        qqqew_ticker = "QQQ_FALLBACK"
        qqqew_series = prices["QQQ"].copy()
    prices["QEW_PROXY"] = qqqew_series
    dgs10 = load_fred_10y()
    spy, qqq = prices["SPY"].dropna(), prices["QQQ"].dropna()
    vix = prices["^VIX"].dropna()
    rsp = prices["RSP"].dropna()
    qew = prices["QEW_PROXY"].dropna()
    rsp_ratio = (rsp / spy).dropna()
    qew_ratio = (qew / qqq).dropna()
    idx = spy.index.union(qqq.index).union(vix.index).sort_values()
    dgs10_a = dgs10.reindex(idx).ffill().dropna()
    results = {}
    for asset, price, bf, rates_fn, trend_fn in [
        ("SPY", spy, rsp_ratio, score_rates_spy, score_trend_spy),
        ("QQQ", qqq, qew_ratio, score_rates_qqq, score_trend_qqq),
    ]:
        ts, tm = trend_fn(price)
        vs, vm = score_vix(vix)
        xs, xm = score_tactical(price, asset)
        bs, bm = score_breadth_proxy(bf, asset)
        rs, rm = rates_fn(dgs10_a)
        total = int(ts + vs + xs + bs + rs)
        raw = classify_signal(total)
        final, reasons = apply_guardrails(raw, tm, vm, bm, rm, asset)
        results[asset] = AssetResult(
            asset=asset, total_score=total, raw_signal=raw, final_signal=final,
            module_scores={"trend": ts, "vix": vs, "tactical": xs, "breadth_proxy": bs, "rates": rs},
            module_meta={"trend": tm, "vix": vm, "tactical": xm, "breadth_proxy": bm, "rates": rm,
                         "proxy_info": {"breadth_ticker": "RSP" if asset == "SPY" else qqqew_ticker}},
            guardrail_reasons=reasons
        )
    return results

def signal_color(signal):
    return {"매수": "#00d084", "보유": "#f0b429", "매도": "#e53e3e"}.get(signal, "#888")

def signal_bg(signal):
    return {"매수": "rgba(0,208,132,0.12)", "보유": "rgba(240,180,41,0.12)", "매도": "rgba(229,62,62,0.12)"}.get(signal, "rgba(136,136,136,0.1)")

def score_bar(score, max_score=100):
    pct = min(100, score / max_score * 100)
    color = "#00d084" if pct >= 70 else ("#f0b429" if pct >= 40 else "#e53e3e")
    return f'<div style="background:#1a1a2e;border-radius:4px;height:8px;width:100%;overflow:hidden;"><div style="height:100%;width:{pct:.1f}%;background:{color};border-radius:4px;"></div></div>'

def fmt(v, fmt_str=".2f"):
    if v is None or (isinstance(v, float) and np.isnan(v)): return "<span style='color:#555'>—</span>"
    if fmt_str == "pct": return f"{v*100:.2f}%"
    if fmt_str == "bp": return f"{v:+.1f}bp"
    return f"{v:{fmt_str}}"

def bool_badge(v):
    return "<span style='color:#00d084;font-size:11px;'>✓</span>" if v else "<span style='color:#444;font-size:11px;'>✗</span>"

def generate_html(results):
    now_kst = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    cards = ""
    for asset, r in results.items():
        tm = r.module_meta["trend"]; vm = r.module_meta["vix"]
        xm = r.module_meta["tactical"]; bm = r.module_meta["breadth_proxy"]
        rm = r.module_meta["rates"]; sc = r.module_scores
        col = signal_color(r.final_signal); bg = signal_bg(r.final_signal)
        guardrail_html = ""
        if r.guardrail_reasons:
            items = "".join(f"<li>{g}</li>" for g in r.guardrail_reasons)
            guardrail_html = f'<div class="guardrail-box"><div class="section-label">⚠ 가드레일 발동</div><ul style="margin:6px 0 0 16px;padding:0;color:#f0b429;font-size:12px;">{items}</ul></div>'
        max_scores = {
            "SPY": {"trend":35,"vix":25,"tactical":15,"breadth_proxy":15,"rates":10},
            "QQQ": {"trend":30,"vix":25,"tactical":15,"breadth_proxy":10,"rates":20}
        }
        ms = max_scores.get(asset, {k:25 for k in sc})
        module_rows = ""
        for mod, label in [("trend","추세"),("vix","VIX"),("tactical","전술"),("breadth_proxy","Breadth"),("rates","금리")]:
            s = sc[mod]; mx = ms.get(mod, 25)
            module_rows += f'<div class="mod-row"><span class="mod-label">{label}</span><span class="mod-score">{s}<span style="color:#444">/{mx}</span></span>{score_bar(s, mx)}</div>'
        raw_eq = f'<span style="color:#555;font-size:11px;">원신호: {r.raw_signal}</span>' if r.raw_signal != r.final_signal else ""
        cards += f'''
        <div class="asset-card" style="border-top:3px solid {col};">
          <div class="card-header">
            <div><div class="asset-name">{asset}</div><div style="color:#555;font-size:12px;margin-top:2px;">{r.module_meta["proxy_info"]["breadth_ticker"]} breadth proxy</div></div>
            <div style="text-align:right;"><div class="signal-badge" style="background:{bg};color:{col};border:1px solid {col}33;">{r.final_signal}</div>{raw_eq}</div>
          </div>
          <div class="score-display"><span style="color:{col};font-size:42px;font-weight:700;letter-spacing:-2px;">{r.total_score}</span><span style="color:#333;font-size:20px;">/100</span></div>
          {score_bar(r.total_score)}
          <div class="modules-section">{module_rows}</div>
          <div class="details-grid">
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
              <div class="detail-row"><span>3일 중 2일 상승</span><span>{bool_badge(xm["cond_2_of_3_up"])}</span></div>
              <div class="detail-row"><span>5일저점 +3%</span><span>{bool_badge(xm["cond_rebound_3pct"])}</span></div></div>
            <div class="detail-group"><div class="section-label">Breadth / 금리</div>
              <div class="detail-row"><span>Breadth 버킷</span><span>{bm["bucket"]}</span></div>
              <div class="detail-row"><span>Approx 비율</span><span>{fmt(bm["approx_breadth"],"pct")}</span></div>
              <div class="detail-row"><span>US 10Y</span><span>{fmt(rm["dgs10"])}%</span></div>
              <div class="detail-row"><span>20일 변화</span><span>{fmt(rm["delta20_bp"],"bp")}</span></div></div>
          </div>
          {guardrail_html}
        </div>'''
    return f'''<!DOCTYPE html>
<html lang="ko"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>US Market Decision Tool</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans+KR:wght@300;400;600&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#0a0a0f;color:#c8c8d8;font-family:"IBM Plex Sans KR",sans-serif;min-height:100vh;padding:32px 16px}}
.page-header{{max-width:960px;margin:0 auto 40px;border-bottom:1px solid #1e1e2e;padding-bottom:24px}}
.page-title{{font-family:"IBM Plex Mono",monospace;font-size:22px;font-weight:600;color:#e8e8f0;letter-spacing:-0.5px}}
.page-subtitle{{font-size:12px;color:#444;margin-top:6px;font-family:"IBM Plex Mono",monospace}}
.timestamp{{font-family:"IBM Plex Mono",monospace;font-size:11px;color:#333;margin-top:12px}}
.cards-container{{max-width:960px;margin:0 auto;display:grid;grid-template-columns:repeat(auto-fit,minmax(420px,1fr));gap:24px}}
.asset-card{{background:#0f0f1a;border:1px solid #1a1a2e;border-radius:12px;padding:28px}}
.card-header{{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:20px}}
.asset-name{{font-family:"IBM Plex Mono",monospace;font-size:28px;font-weight:600;color:#e8e8f0;letter-spacing:-1px}}
.signal-badge{{font-family:"IBM Plex Mono",monospace;font-size:18px;font-weight:600;padding:6px 18px;border-radius:6px;letter-spacing:1px}}
.score-display{{margin:16px 0 8px;font-family:"IBM Plex Mono",monospace;line-height:1}}
.modules-section{{margin:20px 0;display:flex;flex-direction:column;gap:8px}}
.mod-row{{display:grid;grid-template-columns:70px 60px 1fr;align-items:center;gap:10px}}
.mod-label{{font-size:11px;color:#555;font-family:"IBM Plex Mono",monospace}}
.mod-score{{font-family:"IBM Plex Mono",monospace;font-size:13px;color:#888;text-align:right}}
.details-grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:20px;padding-top:20px;border-top:1px solid #1a1a2e}}
.detail-group{{display:flex;flex-direction:column;gap:5px}}
.section-label{{font-family:"IBM Plex Mono",monospace;font-size:10px;color:#333;text-transform:uppercase;letter-spacing:1px;margin-bottom:4px}}
.detail-row{{display:flex;justify-content:space-between;font-size:12px;color:#666;font-family:"IBM Plex Mono",monospace}}
.detail-row span:last-child{{color:#999}}
.guardrail-box{{margin-top:16px;padding:12px;background:rgba(240,180,41,0.05);border:1px solid rgba(240,180,41,0.15);border-radius:6px}}
.footer{{max-width:960px;margin:40px auto 0;text-align:center;font-size:11px;color:#222;font-family:"IBM Plex Mono",monospace;line-height:1.8}}
</style></head><body>
<div class="page-header">
  <div class="page-title">US MARKET DECISION TOOL</div>
  <div class="page-subtitle">SPY · QQQ  |  Trend / VIX / Tactical / Breadth / Rates</div>
  <div class="timestamp">Updated: {now_kst} ({now_utc})</div>
</div>
<div class="cards-container">{cards}</div>
<div class="footer">
  <p>score ≥ 70 → 매수 &nbsp;|&nbsp; 40–69 → 보유 &nbsp;|&nbsp; &lt; 40 → 매도</p>
  <p style="margin-top:4px;">자동 업데이트: 매 거래일 06:30 KST (GitHub Actions)</p>
</div></body></html>'''

if __name__ == "__main__":
    print("데이터 수집 중...")
    results = build_results()
    os.makedirs("docs", exist_ok=True)
    html = generate_html(results)
    with open("docs/index.html", "w", encoding="utf-8") as f:
        f.write(html)
    print(f"완료 ({datetime.now(KST).strftime('%Y-%m-%d %H:%M KST')})")
    for asset, r in results.items():
        print(f"  {asset}: {r.total_score}점 | {r.final_signal}")
