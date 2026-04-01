import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import os
from datetime import datetime

st.set_page_config(page_title="US Macro Dashboard", page_icon="📊", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
  html, body, [class*="css"] { font-family: 'Inter', sans-serif; background-color: #f0efec; }
  .main { background-color: #f0efec; }
  .block-container { padding: 1rem 2rem 2rem 2rem; max-width: 1400px; }
  .dash-header {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
    border-radius: 12px; padding: 24px 32px; margin-bottom: 20px;
    display: flex; justify-content: space-between; align-items: center;
  }
  .dash-title { color: #fff; font-size: 1.6rem; font-weight: 700; margin: 0; }
  .dash-subtitle { color: #a0aec0; font-size: 0.85rem; margin-top: 4px; }
  .dash-time { color: #68d391; font-size: 0.8rem; text-align: right; }
  .chart-card {
    background: #fff; border-radius: 10px; padding: 20px;
    border: 1px solid #e2e8f0; box-shadow: 0 1px 3px rgba(0,0,0,0.06); margin-bottom: 16px;
  }
  .chart-card table td, .chart-card table th { color: #1e293b !important; }
  .warn-box {
    background: #fffbeb; border-left: 4px solid #f6ad55;
    border-radius: 6px; padding: 12px 16px; font-size: 0.82rem; color: #744210; margin-top: 12px;
  }
  .data-badge {
    display: inline-block; padding: 2px 8px; border-radius: 4px;
    font-size: 0.7rem; font-weight: 600; margin-left: 6px; vertical-align: middle;
  }
  .badge-live { background: #dcfce7; color: #16a34a; }
  .badge-static { background: #fef9c3; color: #854d0e; }
  [data-testid="metric-container"] {
    background: white; border: 1px solid #e2e8f0; border-radius: 10px;
    padding: 12px 14px; box-shadow: 0 1px 3px rgba(0,0,0,0.06);
  }
  [data-testid="metric-container"] label,
  [data-testid="metric-container"] [data-testid="stMetricLabel"] p,
  [data-testid="metric-container"] [data-testid="stMetricLabel"] span {
    color: #475569 !important; font-size: 0.7rem !important;
    font-weight: 600 !important; text-transform: uppercase; letter-spacing: 0.04em;
  }
  [data-testid="stMetricValue"],
  [data-testid="stMetricValue"] > div,
  [data-testid="stMetricValue"] span {
    color: #0f172a !important; font-size: 1.4rem !important;
    font-weight: 700 !important;
  }
  [data-testid="stMetricDelta"],
  [data-testid="stMetricDelta"] span,
  [data-testid="stMetricDelta"] p {
    font-size: 0.7rem !important;
  }
  #MainMenu {visibility: hidden;} footer {visibility: hidden;} header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ─── FRED API ────────────────────────────────────────────────────────────────
FRED_KEY = os.getenv("FRED_API_KEY", "")
LIVE = FRED_KEY != ""  # 여기서 먼저 정의

@st.cache_data(ttl=3600)
def fred(series_id: str, limit: int = 40) -> pd.DataFrame:
    if not FRED_KEY:
        return pd.DataFrame()
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&limit={limit}&sort_order=desc")
    try:
        r = requests.get(url, timeout=10)
        df = pd.DataFrame(r.json()["observations"])
        df["date"] = pd.to_datetime(df["date"])
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return df.sort_values("date").dropna(subset=["value"])
    except:
        return pd.DataFrame()

def latest(df: pd.DataFrame, default):
    return float(df["value"].iloc[-1]) if not df.empty else default

def fmt_date(df: pd.DataFrame) -> str:
    if df.empty: return ""
    d = df["date"].iloc[-1]
    return d.strftime("%b '%y")

# ─── 자동갱신 데이터 (FRED) ──────────────────────────────────────────────────
cpi_df    = fred("CPIAUCSL", 36)   # CPI (36개월 = YoY 계산 후 24개월 확보)
core_df   = fred("CPILFESL", 36)   # Core CPI
ffr_df    = fred("FEDFUNDS", 30)   # Fed Funds Rate (30개월)
t10_df    = fred("DGS10", 730)     # 10Y Treasury (일별 → 2년치로 FFR과 기간 통일)
unemp_df  = fred("UNRATE", 24)     # 실업률
gdp_df    = fred("A191RL1Q225SBEA", 16)  # 실질 GDP 성장률 (QoQ SAAR)
deficit_df = fred("FYFSD", 10)     # 연방 재정 흑/적자 ($B)
debt_gdp_df = fred("GFDEGDQ188S", 20)   # 국가부채/GDP (%)
trade_df  = fred("BOPGSTB", 20)    # 무역수지 (백만달러 → /1000 = $B)
ism_df    = fred("NAPM", 24)       # ISM 제조업 PMI (NAPM = ISM 구명칭)

# 최신값 추출
cpi_val   = latest(cpi_df, 2.4)
core_val  = latest(core_df, 2.5)
ffr_val   = latest(ffr_df, 3.625)
t10_val   = latest(t10_df, 4.29)
unemp_val = latest(unemp_df, 4.1)
gdp_val   = latest(gdp_df, 0.7)
deficit_val = latest(deficit_df, -1780)   # $B (음수=적자)
debt_gdp_val = latest(debt_gdp_df, 99.8)
trade_val = latest(trade_df, -191) / 1000   # 백만달러 → 십억달러($B)
ISM_PMI   = round(latest(ism_df, 52.4), 1)  # FRED NAPM 자동갱신

# ─── CPI YoY 계산 (FRED는 level 제공, YoY는 직접 계산) ──────────────────────
def yoy(df):
    if df.empty or len(df) < 13: return None
    latest_val = df["value"].iloc[-1]
    yr_ago = df["value"].iloc[-13]
    return round((latest_val / yr_ago - 1) * 100, 1)

cpi_yoy  = yoy(cpi_df)  or 2.4
core_yoy = yoy(core_df) or 2.5

# ─── 색상 팔레트 ─────────────────────────────────────────────────────────────
C = {"blue":"#3b82f6","red":"#ef4444","green":"#22c55e","orange":"#f97316",
     "purple":"#8b5cf6","teal":"#14b8a6","gray":"#94a3b8","dark":"#1e293b","amber":"#f59e0b"}

def chart_layout(title):
    return dict(
        title=dict(text=title, font=dict(size=13, color=C["dark"], family="Inter"), x=0),
        plot_bgcolor="#fff", paper_bgcolor="#fff",
        font=dict(family="Inter", size=11, color="#1e293b"),
        margin=dict(l=40, r=20, t=44, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                    font=dict(size=11, color="#1e293b"), itemsizing="constant", itemwidth=40),
        xaxis=dict(showgrid=False, linecolor="#e2e8f0", tickfont=dict(size=10, color="#1e293b")),
        yaxis=dict(gridcolor="#f1f5f9", linecolor="#e2e8f0", tickfont=dict(size=10, color="#1e293b")),
        height=300,
    )

# ─── 헤더 ────────────────────────────────────────────────────────────────────
now_str = datetime.now().strftime("%Y-%m-%d %H:%M KST")
data_status = "FRED API 자동갱신 🟢" if LIVE else "⚠ API 키 없음 (정적 데이터)"
st.markdown(f"""
<div class="dash-header">
  <div>
    <div class="dash-title">🇺🇸 US Macro Dashboard</div>
    <div class="dash-subtitle">Inflation · Monetary Policy · Labor · Growth · Fiscal · Trade</div>
  </div>
  <div class="dash-time">
    Last updated<br><strong>{now_str}</strong><br>
    <span style="color:#90cdf4;font-size:0.75rem;">{data_status}</span>
  </div>
</div>
""", unsafe_allow_html=True)

# ─── KPI 카드 (HTML 직접 렌더링 — Streamlit 테마 무관) ─────────────────────
def kpi_card(label, value, delta, color="#16a34a"):
    return f"""<div style="background:white;border:1px solid #e2e8f0;border-radius:10px;
padding:14px 16px;box-shadow:0 1px 3px rgba(0,0,0,0.06);margin-bottom:4px;">
<div style="color:#64748b;font-size:0.68rem;font-weight:600;text-transform:uppercase;
letter-spacing:0.05em;margin-bottom:6px;">{label}</div>
<div style="color:#0f172a;font-size:1.35rem;font-weight:700;margin-bottom:4px;">{value}</div>
<div style="color:{color};font-size:0.7rem;font-weight:500;">{delta}</div>
</div>"""

kpi_data = [
    ("CPI YoY",        f"{cpi_yoy}%",        f"FRED 자동 | {fmt_date(cpi_df)}"),
    ("Core CPI",       f"{core_yoy}%",       f"FRED 자동 | {fmt_date(core_df)}"),
    ("Fed Funds Rate", f"{ffr_val:.2f}%",    f"FRED 자동 | {fmt_date(ffr_df)}"),
    ("10Y Treasury",   f"{t10_val:.2f}%",    f"FRED 자동 | {fmt_date(t10_df)}"),
    ("Unemployment",   f"{unemp_val:.1f}%",  f"FRED 자동 | {fmt_date(unemp_df)}"),
    ("ISM Mfg PMI",    f"{ISM_PMI}",         f"FRED(NAPM) | {fmt_date(ism_df)}" if LIVE and not ism_df.empty else "⚠ 수동 업데이트"),
    ("GDP QoQ SAAR",   f"{gdp_val:+.1f}%",   f"FRED 자동 | {fmt_date(gdp_df)}"),
    ("Trade Balance",  f"${trade_val:.1f}B",  f"FRED 자동 | {fmt_date(trade_df)}"),
]

cols_r1 = st.columns(4)
cols_r2 = st.columns(4)
for i, (label, val, delta) in enumerate(kpi_data):
    col = cols_r1[i] if i < 4 else cols_r2[i-4]
    col.markdown(kpi_card(label, val, delta), unsafe_allow_html=True)

st.markdown("---")

# ─── 탭 ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["📈 인플레이션 & 금리", "💼 고용 & 성장", "🏛 재정 & 부채", "🌐 무역 & 경상수지"])

# ══ Tab 1 ════════════════════════════════════════════════════════════════════
with tab1:
    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        fig = go.Figure()
        if LIVE and not cpi_df.empty:
            cpi_m = cpi_df.copy(); cpi_m["yoy"] = cpi_m["value"].pct_change(12)*100
            core_m = core_df.copy(); core_m["yoy"] = core_m["value"].pct_change(12)*100
            # pct_change(12) 후 처음 12개월은 NaN → dropna 후 최근 24개월
            cpi_m = cpi_m.dropna(subset=["yoy"]).tail(24)
            core_m = core_m.dropna(subset=["yoy"]).tail(24)
            fig.add_trace(go.Scatter(x=cpi_m["date"], y=cpi_m["yoy"].round(1),
                mode="lines+markers", name="CPI YoY", line=dict(color=C["red"], width=2.5), marker=dict(size=4)))
            fig.add_trace(go.Scatter(x=core_m["date"], y=core_m["yoy"].round(1),
                mode="lines+markers", name="Core CPI", line=dict(color=C["orange"], width=2.5, dash="dot"), marker=dict(size=4)))
        else:
            dates = ["2024-03","2024-06","2024-09","2024-12","2025-03","2025-06","2025-09","2025-12","2026-01","2026-02"]
            fig.add_trace(go.Scatter(x=dates, y=[3.5,3.0,2.4,2.9,2.6,2.5,2.3,2.4,2.4,2.4],
                mode="lines+markers", name="CPI YoY", line=dict(color=C["red"], width=2.5), marker=dict(size=4)))
            fig.add_trace(go.Scatter(x=dates, y=[3.8,3.4,3.3,3.2,3.0,2.9,2.7,2.6,2.5,2.5],
                mode="lines+markers", name="Core CPI", line=dict(color=C["orange"], width=2.5, dash="dot"), marker=dict(size=4)))
        fig.add_hline(y=2.0, line_dash="dash", line_color=C["green"], line_width=1.5,
                      annotation_text="Fed 목표 2%", annotation_position="bottom right",
                      annotation_font=dict(size=10, color=C["green"]))
        fig.update_layout(**chart_layout("CPI & Core CPI (YoY %)"))
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        fig2 = go.Figure()
        if LIVE and not ffr_df.empty:
            fig2.add_trace(go.Scatter(x=ffr_df["date"], y=ffr_df["value"],
                mode="lines", name="Fed Funds Rate", line=dict(color=C["blue"], width=2.5),
                fill="tozeroy", fillcolor="rgba(59,130,246,0.08)"))
        else:
            dates_ffr = ["2024-07","2024-09","2024-11","2024-12","2025-01","2025-06","2025-09","2025-10","2025-12","2026-01","2026-03"]
            fig2.add_trace(go.Scatter(x=dates_ffr, y=[5.50,5.25,4.75,4.50,4.50,4.25,4.00,3.875,3.625,3.625,3.625],
                mode="lines+markers", name="Fed Funds Rate", line=dict(color=C["blue"], width=2.5),
                fill="tozeroy", fillcolor="rgba(59,130,246,0.08)"))
        if LIVE and not t10_df.empty:
            fig2.add_trace(go.Scatter(x=t10_df["date"], y=t10_df["value"],
                mode="lines", name="10Y Treasury", line=dict(color=C["purple"], width=2, dash="dot")))
        else:
            dates_t10 = ["2024-03","2024-06","2024-09","2024-12","2025-03","2025-06","2025-09","2025-12","2026-01","2026-03"]
            fig2.add_trace(go.Scatter(x=dates_t10, y=[4.20,4.36,3.75,4.57,4.25,4.30,4.15,4.57,4.52,4.29],
                mode="lines+markers", name="10Y Treasury", line=dict(color=C["purple"], width=2, dash="dot")))
        fig2.update_layout(**chart_layout("금리 경로: FFR vs 10Y Treasury (%)"))
        st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

# ══ Tab 2 ════════════════════════════════════════════════════════════════════
with tab2:
    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        fig3 = go.Figure()
        if LIVE and not unemp_df.empty:
            fig3.add_trace(go.Scatter(x=unemp_df["date"].tail(24), y=unemp_df["value"].tail(24),
                mode="lines+markers", name="실업률", line=dict(color=C["teal"], width=2.5), marker=dict(size=4),
                fill="tozeroy", fillcolor="rgba(20,184,166,0.08)"))
        else:
            dates_u = ["2024-03","2024-06","2024-09","2024-12","2025-03","2025-06","2025-09","2025-12","2026-01","2026-02"]
            fig3.add_trace(go.Scatter(x=dates_u, y=[3.8,4.1,4.1,4.2,4.2,4.1,4.2,4.1,4.0,4.1],
                mode="lines+markers", name="실업률", line=dict(color=C["teal"], width=2.5), marker=dict(size=4),
                fill="tozeroy", fillcolor="rgba(20,184,166,0.08)"))
        fig3.add_hline(y=4.0, line_dash="dash", line_color=C["gray"], line_width=1,
                       annotation_text="자연실업률 ~4%", annotation_position="top right",
                       annotation_font=dict(size=10, color="#1e293b"))
        fig3.update_layout(**chart_layout("실업률 (%)"))
        st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        if LIVE and not gdp_df.empty:
            gdp_plot = gdp_df.tail(10)
            qs = gdp_plot["date"].dt.to_period("Q").astype(str)
            gv = gdp_plot["value"].tolist()
        else:
            qs = ["Q1'24","Q2'24","Q3'24","Q4'24","Q1'25","Q2'25","Q3'25","Q4'25"]
            gv = [1.4, 3.0, 3.1, 2.4, -0.3, 2.2, 4.4, 0.7]
        colors_gdp = [C["green"] if v >= 0 else C["red"] for v in gv]
        fig4 = go.Figure(go.Bar(x=list(qs), y=gv, marker_color=colors_gdp,
            text=[f"{v:+.1f}%" for v in gv], textposition="outside",
            textfont=dict(size=10, color="#1e293b")))
        fig4.add_hline(y=0, line_color=C["gray"], line_width=1)
        fig4.update_layout(**chart_layout("실질 GDP 성장률 (연율 %, QoQ SAAR)"))
        fig4.update_yaxes(range=[-2, 6])
        st.plotly_chart(fig4, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    col3, col4 = st.columns(2)
    with col3:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        fig5 = go.Figure()
        if LIVE and not ism_df.empty:
            fig5.add_trace(go.Scatter(x=ism_df["date"].tail(24), y=ism_df["value"].tail(24),
                mode="lines+markers", name="ISM Mfg PMI",
                line=dict(color=C["amber"], width=2.5), marker=dict(size=5)))
        else:
            dates_ism = ["2024-09","2024-10","2024-11","2024-12","2025-01","2025-02","2025-03","2025-06","2025-09","2025-12","2026-01","2026-02"]
            vals_ism  = [47.2,46.5,48.4,49.3,50.9,50.3,49.0,48.5,50.2,49.8,50.9,52.4]
            fig5.add_trace(go.Scatter(x=dates_ism, y=vals_ism, mode="lines+markers", name="ISM Mfg PMI",
                line=dict(color=C["amber"], width=2.5), marker=dict(size=5)))
        fig5.add_hline(y=50, line_dash="dash", line_color=C["gray"], line_width=1.5,
                       annotation_text="확장/수축 기준 50", annotation_position="top right",
                       annotation_font=dict(size=10, color="#1e293b"))
        ism_title = "ISM 제조업 PMI 🟢 FRED(NAPM) 자동" if LIVE and not ism_df.empty else "ISM 제조업 PMI ⚠ 정적"
        fig5.update_layout(**chart_layout(ism_title))
        fig5.update_yaxes(range=[44,60])
        st.plotly_chart(fig5, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    with col4:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        gdp_comp = {"항목": ["PCE", "민간투자", "정부지출", "순수출"], "비중(%)": [69, 16, 18, -3]}
        fig6 = go.Figure(go.Bar(x=gdp_comp["항목"], y=gdp_comp["비중(%)"],
            marker_color=[C["blue"],C["green"],C["orange"],C["red"]],
            text=[f"{v}%" for v in gdp_comp["비중(%)"]],
            textposition="outside", textfont=dict(size=11, color="#1e293b")))
        fig6.update_layout(**chart_layout("GDP 지출 구성 (Q4'25, %)"))
        st.plotly_chart(fig6, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

# ══ Tab 3 ════════════════════════════════════════════════════════════════════
with tab3:
    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        fig7 = make_subplots(specs=[[{"secondary_y": True}]])
        if LIVE and not deficit_df.empty:
            df_def = deficit_df.tail(8)
            def_years = df_def["date"].dt.year.astype(str).tolist()
            def_vals  = df_def["value"].tolist()  # $B, 음수=적자
            def_abs   = [abs(v) for v in def_vals]
        else:
            def_years = ["FY20","FY21","FY22","FY23","FY24","FY25"]
            def_vals  = [-3.1, -2.8, -5.4, -6.3, -6.1, -5.9]
            def_abs   = [3100, 2800, 1375, 1695, 1833, 1780]
        fig7.add_trace(go.Bar(x=def_years, y=def_abs, name="재정적자 ($B)",
            marker_color=C["red"], opacity=0.7), secondary_y=False)
        if not LIVE:
            fig7.add_trace(go.Scatter(x=def_years, y=def_vals, mode="lines+markers",
                name="GDP 대비 (%)", line=dict(color=C["orange"], width=2.5), marker=dict(size=6)),
                secondary_y=True)
        fig7.update_layout(**chart_layout("연방 재정적자 ($B)"))
        fig7.update_yaxes(title_text="$B", secondary_y=False, tickfont=dict(size=10, color="#1e293b"))
        if not LIVE:
            fig7.update_yaxes(title_text="GDP %", secondary_y=True, tickfont=dict(size=10, color="#1e293b"))
        st.plotly_chart(fig7, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        fig8 = go.Figure()
        if LIVE and not debt_gdp_df.empty:
            fig8.add_trace(go.Scatter(x=debt_gdp_df["date"].tail(20), y=debt_gdp_df["value"].tail(20),
                mode="lines", name="국가부채/GDP (%)",
                line=dict(color=C["purple"], width=2.5),
                fill="tozeroy", fillcolor="rgba(139,92,246,0.08)"))
        else:
            fy = ["FY20","FY21","FY22","FY23","FY24","FY25"]
            fig8.add_trace(go.Scatter(x=fy, y=[126.1,126.4,121.3,118.2,100.8,99.8],
                mode="lines+markers", name="국가부채/GDP (%)",
                line=dict(color=C["purple"], width=2.5), marker=dict(size=6),
                fill="tozeroy", fillcolor="rgba(139,92,246,0.08)"))
        fig8.add_hline(y=100, line_dash="dash", line_color=C["red"], line_width=1.5,
                       annotation_text="GDP 100% 임계선", annotation_position="top right",
                       annotation_font=dict(size=10, color=C["red"]))
        fig8.update_layout(**chart_layout("국가부채 / GDP (%)"))
        st.plotly_chart(fig8, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    # 재정 테이블
    deficit_b = abs(deficit_val) if LIVE else 1780
    deficit_gdp = round(deficit_b / 280000 * 100, 1) if LIVE else 5.9
    st.markdown(f"""
    <div class="chart-card">
      <b style="color:#1e293b;font-size:0.9rem;">📋 연방 재정 핵심 지표
        <span class="data-badge {'badge-live' if LIVE else 'badge-static'}">
          {'🟢 FRED 자동갱신' if LIVE else '⚠ 정적 데이터'}
        </span>
      </b>
      <table style="width:100%;margin-top:12px;font-size:0.85rem;border-collapse:collapse;">
        <tr style="background:#f1f5f9;">
          <th style="padding:10px;text-align:left;border-bottom:2px solid #cbd5e1;color:#1e293b;font-weight:600;">항목</th>
          <th style="padding:10px;text-align:right;border-bottom:2px solid #cbd5e1;color:#1e293b;font-weight:600;">수치</th>
          <th style="padding:10px;text-align:right;border-bottom:2px solid #cbd5e1;color:#1e293b;font-weight:600;">비고</th>
        </tr>
        <tr><td style="padding:10px;border-bottom:1px solid #e2e8f0;color:#1e293b;">총 세입 (FY25)</td>
            <td style="padding:10px;text-align:right;border-bottom:1px solid #e2e8f0;color:#1e293b;font-weight:500;">$5.2T</td>
            <td style="padding:10px;text-align:right;border-bottom:1px solid #e2e8f0;color:#16a34a;font-weight:500;">관세 수입 +$118B ⚠정적</td></tr>
        <tr style="background:#fafafa;"><td style="padding:10px;border-bottom:1px solid #e2e8f0;color:#1e293b;">총 지출 (FY25)</td>
            <td style="padding:10px;text-align:right;border-bottom:1px solid #e2e8f0;color:#1e293b;font-weight:500;">$7.0T</td>
            <td style="padding:10px;text-align:right;border-bottom:1px solid #e2e8f0;color:#dc2626;font-weight:500;">구조적 지출 증가 ⚠정적</td></tr>
        <tr><td style="padding:10px;border-bottom:1px solid #e2e8f0;color:#1e293b;">순이자비용</td>
            <td style="padding:10px;text-align:right;border-bottom:1px solid #e2e8f0;color:#1e293b;font-weight:500;">$1.0T+</td>
            <td style="padding:10px;text-align:right;border-bottom:1px solid #e2e8f0;color:#dc2626;font-weight:500;">사상 최초 $1조 돌파 ⚠정적</td></tr>
        <tr style="background:#fff1f2;"><td style="padding:10px;color:#1e293b;font-weight:700;">재정적자 (FRED)</td>
            <td style="padding:10px;text-align:right;color:#dc2626;font-weight:700;">${deficit_b:,.0f}B</td>
            <td style="padding:10px;text-align:right;color:#dc2626;font-weight:600;">{'🟢 자동갱신' if LIVE else '⚠ 정적'}</td></tr>
      </table>
    </div>
    """, unsafe_allow_html=True)

# ══ Tab 4 ════════════════════════════════════════════════════════════════════
with tab4:
    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        if LIVE and not trade_df.empty:
            trade_plot = trade_df.tail(10)
            trade_dates = trade_plot["date"].dt.to_period("Q").astype(str).tolist()
            trade_vals_plot = trade_plot["value"].tolist()
        else:
            trade_dates = ["Q1'24","Q2'24","Q3'24","Q4'24","Q1'25","Q2'25","Q3'25","Q4'25"]
            trade_vals_plot = [-205,-215,-220,-235,-240,-225,-239,-191]
        max_abs = max(abs(v) for v in trade_vals_plot)
        trade_colors = [f"rgba(239,68,68,{0.45+0.55*abs(v)/max_abs:.2f})" for v in trade_vals_plot]
        fig9 = go.Figure(go.Bar(x=trade_dates, y=trade_vals_plot,
            marker_color=trade_colors, marker_line=dict(color="#dc2626", width=1),
            text=[f"${v:.0f}B" for v in trade_vals_plot],
            textposition="outside", textfont=dict(size=10, color="#1e293b")))
        fig9.add_hline(y=0, line_color=C["gray"], line_width=1)
        fig9.update_layout(**chart_layout("무역수지 추이 ($B) 🟢 FRED 자동" if LIVE else "무역수지 추이 ($B) ⚠ 정적"))
        fig9.update_yaxes(range=[-300, 30])
        st.plotly_chart(fig9, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        partners = ["EU","China","Mexico","Vietnam","Japan","Canada"]
        deficits = [218.8,202.1,196.9,178.2,68.5,63.2]
        p_colors = ["#ef4444","#f97316","#eab308","#22c55e","#3b82f6","#8b5cf6"]
        fig10 = go.Figure(go.Bar(y=partners, x=[-v for v in deficits], orientation="h",
            marker_color=p_colors, opacity=0.85,
            text=[f"-${v}B" for v in deficits],
            textposition="outside", textfont=dict(size=10, color="#1e293b")))
        fig10.update_layout(**chart_layout("2025 주요 무역 적자국 ($B) ⚠ 정적"))
        fig10.update_xaxes(range=[-260,0])
        st.plotly_chart(fig10, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("""
    <div class="chart-card">
      <b style="color:#1e293b;font-size:0.9rem;">🌐 미국 순국제투자포지션 (NIIP) — Q4 2025 <span class="data-badge badge-static">⚠ 정적 데이터 (BEA 분기 발표)</span></b>
      <table style="width:100%;margin-top:12px;font-size:0.85rem;border-collapse:collapse;">
        <tr style="background:#f1f5f9;">
          <th style="padding:10px 12px;text-align:left;border-bottom:2px solid #cbd5e1;color:#1e293b;font-weight:600;">항목</th>
          <th style="padding:10px 12px;text-align:right;border-bottom:2px solid #cbd5e1;color:#1e293b;font-weight:600;">금액</th>
          <th style="padding:10px 12px;text-align:right;border-bottom:2px solid #cbd5e1;color:#1e293b;font-weight:600;">비고</th>
        </tr>
        <tr><td style="padding:10px 12px;border-bottom:1px solid #e2e8f0;color:#1e293b;">해외 자산 (총계)</td>
            <td style="padding:10px 12px;text-align:right;border-bottom:1px solid #e2e8f0;color:#1e293b;font-weight:500;">$42.96T</td>
            <td style="padding:10px 12px;text-align:right;border-bottom:1px solid #e2e8f0;color:#16a34a;font-weight:500;">주식·직접투자 위주</td></tr>
        <tr style="background:#fafafa;"><td style="padding:10px 12px;border-bottom:1px solid #e2e8f0;color:#1e293b;">해외 부채 (총계)</td>
            <td style="padding:10px 12px;text-align:right;border-bottom:1px solid #e2e8f0;color:#1e293b;font-weight:500;">$70.49T</td>
            <td style="padding:10px 12px;text-align:right;border-bottom:1px solid #e2e8f0;color:#dc2626;font-weight:500;">국채·회사채 보유</td></tr>
        <tr style="background:#fff1f2;"><td style="padding:10px 12px;color:#1e293b;font-weight:700;">순 포지션 (NIIP)</td>
            <td style="padding:10px 12px;text-align:right;color:#dc2626;font-weight:700;">-$27.54T</td>
            <td style="padding:10px 12px;text-align:right;color:#dc2626;font-weight:600;">GDP 대비 약 -100%</td></tr>
      </table>
    </div>
    """, unsafe_allow_html=True)

# ─── 데이터 현황 요약 ─────────────────────────────────────────────────────────
st.markdown(f"""
<div class="warn-box">
  <strong>📊 데이터 갱신 현황</strong><br>
  🟢 <b>FRED 자동갱신 (1시간 캐시):</b> CPI·Core CPI·FFR·10Y Treasury·실업률·실질GDP·재정적자·국가부채/GDP·무역수지<br>
  ⚠ <b>수동 업데이트 필요:</b> ISM 제조업 PMI (FRED 미제공) · NIIP (BEA 분기 발표) · 주요 적자국 순위 · GDP 지출 구성 · 총 세입/지출<br>
  현재 상태: <b>{'FRED API 연결됨 🟢' if LIVE else 'API 키 없음 — Streamlit Secrets에 FRED_API_KEY 추가 필요 ⚠'}</b>
</div>
""", unsafe_allow_html=True)
