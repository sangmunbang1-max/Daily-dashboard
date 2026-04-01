import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import requests
import os
from datetime import datetime, timedelta
import json

# ─── Page Config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="US Macro Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ─── CSS (HTML 디자인 동일 재현) ──────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

  html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
    background-color: #f0efec;
  }
  .main { background-color: #f0efec; }
  .block-container { padding: 1rem 2rem 2rem 2rem; max-width: 1400px; }

  /* Header */
  .dash-header {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
    border-radius: 12px;
    padding: 24px 32px;
    margin-bottom: 20px;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  .dash-title { color: #fff; font-size: 1.6rem; font-weight: 700; margin: 0; }
  .dash-subtitle { color: #a0aec0; font-size: 0.85rem; margin-top: 4px; }
  .dash-time { color: #68d391; font-size: 0.8rem; text-align: right; }

  /* KPI Cards */
  .kpi-card {
    background: #fff;
    border-radius: 10px;
    padding: 18px 20px;
    border: 1px solid #e2e8f0;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    margin-bottom: 8px;
  }
  .kpi-label { font-size: 0.72rem; color: #718096; font-weight: 500; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 6px; }
  .kpi-value { font-size: 1.6rem; font-weight: 700; color: #1a202c; margin-bottom: 4px; }
  .kpi-delta-pos { font-size: 0.78rem; color: #38a169; font-weight: 500; }
  .kpi-delta-neg { font-size: 0.78rem; color: #e53e3e; font-weight: 500; }
  .kpi-delta-neu { font-size: 0.78rem; color: #718096; font-weight: 500; }

  /* Chart Card */
  .chart-card {
    background: #fff;
    border-radius: 10px;
    padding: 20px;
    border: 1px solid #e2e8f0;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    margin-bottom: 16px;
  }

  /* Warning */
  .warn-box {
    background: #fffbeb;
    border-left: 4px solid #f6ad55;
    border-radius: 6px;
    padding: 12px 16px;
    font-size: 0.82rem;
    color: #744210;
    margin-top: 12px;
  }

  /* Hide Streamlit default elements */
  #MainMenu {visibility: hidden;}
  footer {visibility: hidden;}
  header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ─── FRED API ────────────────────────────────────────────────────────────────
FRED_API_KEY = os.getenv("FRED_API_KEY", "")

@st.cache_data(ttl=3600)
def fetch_fred(series_id: str, limit: int = 60) -> pd.DataFrame:
    if not FRED_API_KEY:
        return pd.DataFrame()
    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}&api_key={FRED_API_KEY}"
        f"&file_type=json&limit={limit}&sort_order=desc"
    )
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
        df = pd.DataFrame(data["observations"])
        df["date"] = pd.to_datetime(df["date"])
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return df.sort_values("date").dropna(subset=["value"])
    except Exception:
        return pd.DataFrame()

# ─── Static Fallback Data ────────────────────────────────────────────────────
def static_data():
    return {
        "cpi": {
            "dates": ["2024-03","2024-06","2024-09","2024-12","2025-03","2025-06","2025-09","2025-12","2026-01","2026-02"],
            "values": [3.5, 3.0, 2.4, 2.9, 2.6, 2.5, 2.3, 2.4, 2.4, 2.4],
            "core":   [3.8, 3.4, 3.3, 3.2, 3.0, 2.9, 2.7, 2.6, 2.5, 2.5],
        },
        "ffr": {
            "dates":  ["2024-07","2024-09","2024-11","2024-12","2025-01","2025-06","2025-09","2025-10","2025-12","2026-01","2026-03"],
            "values": [5.50, 5.25, 4.75, 4.50, 4.50, 4.25, 4.00, 3.875, 3.625, 3.625, 3.625],
        },
        "ust10": {
            "dates":  ["2024-03","2024-06","2024-09","2024-12","2025-03","2025-06","2025-09","2025-12","2026-01","2026-03"],
            "values": [4.20, 4.36, 3.75, 4.57, 4.25, 4.30, 4.15, 4.57, 4.52, 4.29],
        },
        "unemp": {
            "dates":  ["2024-03","2024-06","2024-09","2024-12","2025-03","2025-06","2025-09","2025-12","2026-01","2026-02"],
            "values": [3.8, 4.1, 4.1, 4.2, 4.2, 4.1, 4.2, 4.1, 4.0, 4.1],
        },
        "gdp": {
            "quarters": ["Q1'24","Q2'24","Q3'24","Q4'24","Q1'25","Q2'25","Q3'25","Q4'25"],
            "values":   [1.4, 3.0, 3.1, 2.4, -0.3, 2.2, 4.4, 0.7],
        },
        "ism": {
            "dates":  ["2024-09","2024-10","2024-11","2024-12","2025-01","2025-02","2025-03","2025-06","2025-09","2025-12","2026-01","2026-02"],
            "values": [47.2, 46.5, 48.4, 49.3, 50.9, 50.3, 49.0, 48.5, 50.2, 49.8, 50.9, 52.4],
        },
        "trade": {
            "quarters": ["Q1'24","Q2'24","Q3'24","Q4'24","Q1'25","Q2'25","Q3'25","Q4'25"],
            "values":   [-205, -215, -220, -235, -240, -225, -239, -191],
        },
        "deficit": {
            "years":  ["FY20","FY21","FY22","FY23","FY24","FY25"],
            "values": [-3.1, -2.8, -5.4, -6.3, -6.1, -5.9],
            "abs":    [3100, 2800, 1375, 1695, 1833, 1780],
        },
        "debt_gdp": {
            "years":  ["FY20","FY21","FY22","FY23","FY24","FY25"],
            "values": [126.1, 126.4, 121.3, 118.2, 100.8, 99.8],
        },
    }

SD = static_data()

# ─── Color Palette ───────────────────────────────────────────────────────────
C = {
    "blue":   "#3b82f6",
    "red":    "#ef4444",
    "green":  "#22c55e",
    "orange": "#f97316",
    "purple": "#8b5cf6",
    "teal":   "#14b8a6",
    "gray":   "#94a3b8",
    "dark":   "#1e293b",
    "amber":  "#f59e0b",
}

def chart_layout(title):
    return dict(
        title=dict(text=title, font=dict(size=13, color=C["dark"], family="Inter"), x=0),
        plot_bgcolor="#fff",
        paper_bgcolor="#fff",
        font=dict(family="Inter", size=11, color="#1e293b"),
        margin=dict(l=10, r=10, t=40, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10)),
        xaxis=dict(showgrid=False, linecolor="#e2e8f0", tickfont=dict(size=10, color="#1e293b")),
        yaxis=dict(gridcolor="#f1f5f9", linecolor="#e2e8f0", tickfont=dict(size=10, color="#1e293b")),
        height=280,
    )

# ─── Header ──────────────────────────────────────────────────────────────────
now_str = datetime.now().strftime("%Y-%m-%d %H:%M KST")
st.markdown(f"""
<div class="dash-header">
  <div>
    <div class="dash-title">🇺🇸 US Macro Dashboard</div>
    <div class="dash-subtitle">Inflation · Monetary Policy · Labor · Growth · Fiscal · Trade</div>
  </div>
  <div class="dash-time">
    Last updated<br><strong>{now_str}</strong><br>
    <span style="color:#90cdf4;font-size:0.75rem;">{'FRED API' if FRED_API_KEY else '⚠ Static data (no API key)'}</span>
  </div>
</div>
""", unsafe_allow_html=True)

# ─── KPI Cards ───────────────────────────────────────────────────────────────
kpis = [
    ("CPI YoY", "2.4%", "→ Jan '26 동일", "neu"),
    ("Core CPI", "2.5%", "▼ 2021년 이후 최저권", "pos"),
    ("Fed Funds Rate", "3.50–3.75%", "→ Mar '26 동결", "neu"),
    ("10Y Treasury", "4.29%", "▲ +18bp (Mar '26)", "neg"),
    ("Unemployment", "4.1%", "→ Feb '26", "neu"),
    ("ISM Mfg PMI", "52.4", "▲ 2연속 확장", "pos"),
    ("GDP (Q4'25)", "+0.7%", "▼ Q3 4.4%에서 둔화", "neg"),
    ("Trade Deficit", "-$191B", "▲ Q4'25 개선 (Q3 -$239B)", "pos"),
]

cols = st.columns(8)
for col, (label, val, delta, sign) in zip(cols, kpis):
    delta_cls = f"kpi-delta-{sign}"
    col.markdown(f"""
    <div class="kpi-card">
      <div class="kpi-label">{label}</div>
      <div class="kpi-value">{val}</div>
      <div class="{delta_cls}">{delta}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# ─── Tabs ────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["📈 인플레이션 & 금리", "💼 고용 & 성장", "🏛 재정 & 부채", "🌐 무역 & 경상수지"])

# ══ Tab 1 ════════════════════════════════════════════════════════════════════
with tab1:
    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=SD["cpi"]["dates"], y=SD["cpi"]["values"],
            mode="lines+markers", name="CPI YoY", line=dict(color=C["red"], width=2.5),
            marker=dict(size=5)))
        fig.add_trace(go.Scatter(x=SD["cpi"]["dates"], y=SD["cpi"]["core"],
            mode="lines+markers", name="Core CPI", line=dict(color=C["orange"], width=2.5, dash="dot"),
            marker=dict(size=5)))
        fig.add_hline(y=2.0, line_dash="dash", line_color=C["green"], line_width=1.5,
                      annotation_text="Fed 목표 2%", annotation_position="bottom right",
                      annotation_font=dict(size=10, color=C["green"]))
        fig.update_layout(**chart_layout("CPI & Core CPI (YoY %)"))
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=SD["ffr"]["dates"], y=SD["ffr"]["values"],
            mode="lines+markers", name="Fed Funds Rate (midpoint)",
            line=dict(color=C["blue"], width=2.5), marker=dict(size=5),
            fill="tozeroy", fillcolor="rgba(59,130,246,0.08)"))
        fig2.add_trace(go.Scatter(x=SD["ust10"]["dates"], y=SD["ust10"]["values"],
            mode="lines+markers", name="10Y Treasury",
            line=dict(color=C["purple"], width=2, dash="dot"), marker=dict(size=5)))
        fig2.update_layout(**chart_layout("금리 경로: FFR vs 10Y Treasury (%)"))
        st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

# ══ Tab 2 ════════════════════════════════════════════════════════════════════
with tab2:
    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        fig3 = go.Figure()
        fig3.add_trace(go.Scatter(x=SD["unemp"]["dates"], y=SD["unemp"]["values"],
            mode="lines+markers", name="실업률 (%)",
            line=dict(color=C["teal"], width=2.5), marker=dict(size=5),
            fill="tozeroy", fillcolor="rgba(20,184,166,0.08)"))
        fig3.add_hline(y=4.0, line_dash="dash", line_color=C["gray"], line_width=1,
                       annotation_text="자연실업률 ~4%", annotation_position="top right",
                       annotation_font=dict(size=10))
        fig3.update_layout(**chart_layout("실업률 (%)"))
        st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        gdp_colors = [C["green"] if v >= 0 else C["red"] for v in SD["gdp"]["values"]]
        fig4 = go.Figure(go.Bar(
            x=SD["gdp"]["quarters"], y=SD["gdp"]["values"],
            marker_color=gdp_colors, name="Real GDP Growth",
            text=[f"{v:+.1f}%" for v in SD["gdp"]["values"]],
            textposition="outside", textfont=dict(size=10)
        ))
        fig4.add_hline(y=0, line_color=C["gray"], line_width=1)
        fig4.update_layout(**chart_layout("실질 GDP 성장률 (연율 %, QoQ SAAR)"))
        fig4.update_yaxes(range=[-2, 6])
        st.plotly_chart(fig4, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    col3, col4 = st.columns(2)
    with col3:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        fig5 = go.Figure()
        fig5.add_trace(go.Scatter(x=SD["ism"]["dates"], y=SD["ism"]["values"],
            mode="lines+markers", name="ISM Mfg PMI",
            line=dict(color=C["amber"], width=2.5), marker=dict(size=5)))
        fig5.add_hline(y=50, line_dash="dash", line_color=C["gray"], line_width=1.5,
                       annotation_text="확장/수축 기준선 50", annotation_position="top right",
                       annotation_font=dict(size=10))
        fig5.update_layout(**chart_layout("ISM 제조업 PMI"))
        fig5.update_yaxes(range=[44, 56])
        st.plotly_chart(fig5, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    with col4:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        gdp_components = {
            "항목": ["PCE", "민간투자", "정부지출", "순수출"],
            "비중(%)": [69, 16, 18, -3],
        }
        colors_comp = [C["blue"], C["green"], C["orange"], C["red"]]
        fig6 = go.Figure(go.Bar(
            x=gdp_components["항목"], y=gdp_components["비중(%)"],
            marker_color=colors_comp,
            text=[f"{v}%" for v in gdp_components["비중(%)"]],
            textposition="outside", textfont=dict(size=11)
        ))
        fig6.update_layout(**chart_layout("GDP 지출 구성 (Q4'25 기준, %)"))
        st.plotly_chart(fig6, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

# ══ Tab 3 ════════════════════════════════════════════════════════════════════
with tab3:
    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        fig7 = make_subplots(specs=[[{"secondary_y": True}]])
        fig7.add_trace(go.Bar(
            x=SD["deficit"]["years"], y=SD["deficit"]["abs"],
            name="재정적자 ($B)", marker_color=C["red"],
            opacity=0.7
        ), secondary_y=False)
        fig7.add_trace(go.Scatter(
            x=SD["deficit"]["years"], y=SD["deficit"]["values"],
            mode="lines+markers", name="GDP 대비 (%)",
            line=dict(color=C["orange"], width=2.5), marker=dict(size=6)
        ), secondary_y=True)
        fig7.update_layout(**chart_layout("연방 재정적자 ($B & GDP 대비 %)"))
        fig7.update_yaxes(title_text="$B", secondary_y=False, tickfont=dict(size=10, color="#1e293b"))
        fig7.update_yaxes(title_text="GDP %", secondary_y=True, tickfont=dict(size=10, color="#1e293b"))
        st.plotly_chart(fig7, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        fig8 = go.Figure()
        fig8.add_trace(go.Scatter(
            x=SD["debt_gdp"]["years"], y=SD["debt_gdp"]["values"],
            mode="lines+markers", name="국가부채/GDP (%)",
            line=dict(color=C["purple"], width=2.5), marker=dict(size=6),
            fill="tozeroy", fillcolor="rgba(139,92,246,0.08)"
        ))
        fig8.add_hline(y=100, line_dash="dash", line_color=C["red"], line_width=1.5,
                       annotation_text="GDP 100% 임계선", annotation_position="top right",
                       annotation_font=dict(size=10, color=C["red"]))
        fig8.update_layout(**chart_layout("국가부채 / GDP (%)"))
        st.plotly_chart(fig8, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    # Fiscal KPIs
    st.markdown("""
    <div class="chart-card">
      <b style="color:#1e293b;font-size:0.9rem;">📋 FY2025 재정 핵심 지표</b>
      <table style="width:100%;margin-top:12px;font-size:0.85rem;border-collapse:collapse;">
        <tr style="background:#f8fafc;"><th style="padding:8px;text-align:left;border-bottom:1px solid #e2e8f0;">항목</th><th style="padding:8px;text-align:right;border-bottom:1px solid #e2e8f0;">수치</th><th style="padding:8px;text-align:right;border-bottom:1px solid #e2e8f0;">비고</th></tr>
        <tr><td style="padding:8px;border-bottom:1px solid #f1f5f9;">총 세입</td><td style="padding:8px;text-align:right;border-bottom:1px solid #f1f5f9;">$5.2T</td><td style="padding:8px;text-align:right;border-bottom:1px solid #f1f5f9;color:#38a169;">관세 수입 +$118B</td></tr>
        <tr><td style="padding:8px;border-bottom:1px solid #f1f5f9;">총 지출</td><td style="padding:8px;text-align:right;border-bottom:1px solid #f1f5f9;">$7.0T</td><td style="padding:8px;text-align:right;border-bottom:1px solid #f1f5f9;color:#e53e3e;">구조적 지출 증가</td></tr>
        <tr><td style="padding:8px;border-bottom:1px solid #f1f5f9;">순이자비용</td><td style="padding:8px;text-align:right;border-bottom:1px solid #f1f5f9;">$1.0T+</td><td style="padding:8px;text-align:right;border-bottom:1px solid #f1f5f9;color:#e53e3e;">사상 최초 $1조 돌파</td></tr>
        <tr><td style="padding:8px;">재정적자</td><td style="padding:8px;text-align:right;">$1.78T</td><td style="padding:8px;text-align:right;color:#e53e3e;">GDP 대비 5.9%</td></tr>
      </table>
    </div>
    """, unsafe_allow_html=True)

# ══ Tab 4 ════════════════════════════════════════════════════════════════════
with tab4:
    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        trade_colors = [C["red"] if v < 0 else C["green"] for v in SD["trade"]["values"]]
        # 적자 크기에 따라 색상 강도 조절
        max_def = max(abs(v) for v in SD["trade"]["values"])
        trade_colors = []
        for v in SD["trade"]["values"]:
            intensity = int(180 + 75 * (1 - abs(v)/max_def))
            trade_colors.append(f"rgba(239, 68, 68, {0.5 + 0.5*abs(v)/max_def:.2f})")
        fig9 = go.Figure(go.Bar(
            x=SD["trade"]["quarters"], y=SD["trade"]["values"],
            marker_color=trade_colors,
            marker_line=dict(color="#dc2626", width=1),
            text=[f"${v}B" for v in SD["trade"]["values"]],
            textposition="outside", textfont=dict(size=10, color="#1e293b")
        ))
        fig9.add_hline(y=0, line_color=C["gray"], line_width=1)
        fig9.update_layout(**chart_layout("경상수지 추이 ($B)"))
        fig9.update_yaxes(range=[-280, 20])
        st.plotly_chart(fig9, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        partners = ["EU", "China", "Mexico", "Vietnam", "Japan", "Canada"]
        deficits = [218.8, 202.1, 196.9, 178.2, 68.5, 63.2]
        partner_colors = ["#ef4444", "#f97316", "#eab308", "#22c55e", "#3b82f6", "#8b5cf6"]
        fig10 = go.Figure(go.Bar(
            y=partners, x=[-v for v in deficits],
            orientation="h",
            marker_color=partner_colors, opacity=0.85,
            text=[f"-${v}B" for v in deficits],
            textposition="outside", textfont=dict(size=10, color="#1e293b")
        ))
        fig10.update_layout(**chart_layout("2025 주요 무역 적자국 ($B)"))
        fig10.update_xaxes(range=[-260, 0])
        st.plotly_chart(fig10, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    # NIIP Table
    st.markdown("""
    <div class="chart-card">
      <b style="color:#1e293b;font-size:0.9rem;">🌐 미국 순국제투자포지션 (NIIP) — Q4 2025</b>
      <table style="width:100%;margin-top:12px;font-size:0.85rem;border-collapse:collapse;">
        <tr style="background:#f1f5f9;">
          <th style="padding:10px 12px;text-align:left;border-bottom:2px solid #cbd5e1;color:#1e293b;font-weight:600;">항목</th>
          <th style="padding:10px 12px;text-align:right;border-bottom:2px solid #cbd5e1;color:#1e293b;font-weight:600;">금액</th>
          <th style="padding:10px 12px;text-align:right;border-bottom:2px solid #cbd5e1;color:#1e293b;font-weight:600;">비고</th>
        </tr>
        <tr>
          <td style="padding:10px 12px;border-bottom:1px solid #e2e8f0;color:#1e293b;">해외 자산 (총계)</td>
          <td style="padding:10px 12px;text-align:right;border-bottom:1px solid #e2e8f0;color:#1e293b;font-weight:500;">$42.96T</td>
          <td style="padding:10px 12px;text-align:right;border-bottom:1px solid #e2e8f0;color:#16a34a;font-weight:500;">주식·직접투자 위주</td>
        </tr>
        <tr style="background:#fafafa;">
          <td style="padding:10px 12px;border-bottom:1px solid #e2e8f0;color:#1e293b;">해외 부채 (총계)</td>
          <td style="padding:10px 12px;text-align:right;border-bottom:1px solid #e2e8f0;color:#1e293b;font-weight:500;">$70.49T</td>
          <td style="padding:10px 12px;text-align:right;border-bottom:1px solid #e2e8f0;color:#dc2626;font-weight:500;">국채·회사채 보유</td>
        </tr>
        <tr style="background:#fff1f2;">
          <td style="padding:10px 12px;color:#1e293b;font-weight:700;">순 포지션 (NIIP)</td>
          <td style="padding:10px 12px;text-align:right;color:#dc2626;font-weight:700;">-$27.54T</td>
          <td style="padding:10px 12px;text-align:right;color:#dc2626;font-weight:600;">GDP 대비 약 -100%</td>
        </tr>
      </table>
    </div>
    """, unsafe_allow_html=True)

# ─── Warning Footer ──────────────────────────────────────────────────────────
st.markdown("""
<div class="warn-box">
  ⚠️ <strong>데이터 주의사항:</strong>
  2025년 10–11월 고용 데이터는 연방정부 셧다운 영향으로 결측/추정치 포함.
  GDP Q1'25 -0.3%는 수입 급증 + 정부지출 감소 반영.
  FRED API 키 미설정 시 정적 데이터(최근 업데이트 기준) 표시.
  Sources: BLS, BEA, Fed, CBO, CRFB, Trading Economics.
</div>
""", unsafe_allow_html=True)
