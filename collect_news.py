# -*- coding: utf-8 -*-
"""
Daily news briefing — 비용 0원 버전
NewsAPI로 헤드라인 수집 → docs/news.html 직접 생성
ANTHROPIC_API_KEY 불필요
"""
import os, json, requests
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "")
DOCS_DIR = "docs"
DATA_DIR  = f"{DOCS_DIR}/data"


# ── 뉴스 수집 ──────────────────────────────────────────────
def fetch_news() -> dict:
    """NewsAPI로 미·한국 카테고리별 헤드라인 수집"""
    categories = {
        "us_market":  ("US stock market economy Fed", "en"),
        "kr_market":  ("Korea KOSPI economy exchange rate", "en"),
        "geopolitics":("oil energy Middle East Iran war", "en"),
        "tech":       ("AI semiconductor Nvidia Samsung chip", "en"),
    }

    if not NEWSAPI_KEY:
        print("[WARN] NEWSAPI_KEY 없음 → 빈 결과")
        return {k: [] for k in categories}

    since = (datetime.now(KST) - timedelta(days=2)).strftime("%Y-%m-%d")
    result = {}

    for key, (query, lang) in categories.items():
        try:
            r = requests.get(
                "https://newsapi.org/v2/everything",
                params={
                    "q":        query,
                    "language": lang,
                    "sortBy":   "publishedAt",
                    "pageSize": 6,
                    "from":     since,
                },
                headers={"X-Api-Key": NEWSAPI_KEY},
                timeout=15,
            )
            r.raise_for_status()
            articles = r.json().get("articles", [])
            result[key] = [
                {
                    "title":       a.get("title", "").split(" - ")[0].strip(),
                    "description": (a.get("description") or "")[:120],
                    "source":      a.get("source", {}).get("name", ""),
                    "url":         a.get("url", ""),
                    "publishedAt": a.get("publishedAt", "")[:10],
                }
                for a in articles
                if a.get("title") and "[Removed]" not in a.get("title", "")
            ]
            print(f"  [{key}] {len(result[key])}건 수집")
        except Exception as e:
            print(f"  [{key}] 수집 오류: {e}")
            result[key] = []

    return result


# ── HTML 생성 ──────────────────────────────────────────────
categories = {
    "us_market":    ("US stock market S&P500 Nasdaq Dow earnings", "en"),
    "kr_market":    ("Korea KOSPI KOSDAQ economy Samsung Hyundai", "en"),
    "macro":        ("Federal Reserve interest rate inflation GDP CPI", "en"),
    "bonds":        ("treasury bonds yield credit fixed income", "en"),
    "commodities":  ("oil gold copper wheat LNG energy commodities", "en"),
    "fx":           ("dollar yen euro yuan won currency forex", "en"),
    "geopolitics":  ("trade war tariff sanctions geopolitics Middle East", "en"),
    "tech":         ("AI semiconductor Nvidia TSMC chip data center", "en"),
}
def fmt_date(iso: str) -> str:
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(KST).strftime("%m/%d")
    except:
        return iso[:10] if iso else ""

def generate_html(news: dict, generated_at: str) -> str:
    sections_html = ""

    for key, (title, color) in SECTION_META.items():
        articles = news.get(key, [])
        if not articles:
            items_html = '<p style="color:#4a4a6a;font-size:13px;padding:12px 0;">수집된 뉴스가 없습니다.</p>'
        else:
            items_html = ""
            for a in articles:
                date_str = fmt_date(a.get("publishedAt", ""))
                desc = a.get("description", "")
                desc_html = f'<p class="art-desc">{desc}</p>' if desc else ""
                url = a.get("url", "")
                link_open  = f'<a href="{url}" target="_blank" rel="noopener" style="text-decoration:none;color:inherit;">' if url else "<span>"
                link_close = "</a>" if url else "</span>"
                items_html += f"""
                <div class="art-item">
                  {link_open}
                  <div class="art-header">
                    <span class="art-source">{a.get('source','')}</span>
                    <span class="art-date">{date_str}</span>
                  </div>
                  <p class="art-title">{a.get('title','')}</p>
                  {desc_html}
                  {link_close}
                </div>"""

        sections_html += f"""
        <div class="section-card" style="border-top:3px solid {color};">
          <div class="section-title" style="color:{color};">{title}</div>
          {items_html}
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Daily News Brief</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans+KR:wght@300;400;600&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#0d0d14;color:#d4d4e0;font-family:"IBM Plex Sans KR",sans-serif;min-height:100vh;padding:32px 16px}}
.page-header{{max-width:1100px;margin:0 auto 28px;border-bottom:1px solid #252538;padding-bottom:20px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px}}
.page-title{{font-family:"IBM Plex Mono",monospace;font-size:22px;font-weight:600;color:#f0f0f8;letter-spacing:-0.5px}}
.nav-link{{font-family:"IBM Plex Mono",monospace;font-size:12px;color:#5b9bd5;text-decoration:none;border:1px solid #252538;padding:6px 14px;border-radius:6px}}
.nav-link:hover{{background:#161622}}
.meta-bar{{max-width:1100px;margin:0 auto 24px;font-family:"IBM Plex Mono",monospace;font-size:11px;color:#5b5b80}}
.grid{{max-width:1100px;margin:0 auto;display:grid;grid-template-columns:repeat(4,1fr);gap:14px}}
@media(max-width:900px){{.grid{{grid-template-columns:1fr 1fr}}}}
@media(max-width:520px){{.grid{{grid-template-columns:1fr}}}}
.section-card{{background:#13131f;border:1px solid #252538;border-radius:12px;padding:20px 20px 12px}}
.section-title{{font-family:"IBM Plex Mono",monospace;font-size:13px;font-weight:600;margin-bottom:14px}}
.art-item{{padding:10px 0;border-bottom:1px solid #1a1a28}}
.art-item:last-child{{border-bottom:none}}
.art-item:hover .art-title{{color:#c0d8f0}}
.art-header{{display:flex;justify-content:space-between;margin-bottom:4px}}
.art-source{{font-family:"IBM Plex Mono",monospace;font-size:10px;color:#5b5b80;font-weight:600}}
.art-date{{font-family:"IBM Plex Mono",monospace;font-size:10px;color:#4a4a6a}}
.art-title{{font-size:13px;font-weight:600;color:#e0e0f0;line-height:1.5;margin-bottom:4px}}
.art-desc{{font-size:12px;color:#8888aa;line-height:1.6}}
.footer{{max-width:1100px;margin:32px auto 0;text-align:center;font-size:11px;color:#353558;font-family:"IBM Plex Mono",monospace;line-height:2}}
</style>
</head>
<body>
<div class="page-header">
  <div class="page-title">DAILY NEWS BRIEF</div>
  <a href="index.html" class="nav-link">← DASHBOARD</a>
</div>
<div class="meta-bar">업데이트: {generated_at} &nbsp;|&nbsp; NewsAPI</div>
<div class="grid">
  {sections_html}
</div>
<div class="footer">
  <p>매일 07:30 KST 자동 갱신</p>
  <p><a href="index.html" style="color:#5b9bd5;text-decoration:none;">← Market Decision Dashboard</a></p>
</div>
</body>
</html>"""


# ── 메인 ──────────────────────────────────────────────────
if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)

    now = datetime.now(KST)
    generated_at = now.strftime("%Y-%m-%d %H:%M KST")
    print(f"[NEWS] 시작: {generated_at}")

    # 1. 뉴스 수집
    news = fetch_news()

    # 2. JSON 저장 (백업용)
    with open(f"{DATA_DIR}/news_latest.json", "w", encoding="utf-8") as f:
        json.dump({"generated_at_kst": generated_at, "news": news}, f,
                  ensure_ascii=False, indent=2)

    # 3. HTML 저장
    html = generate_html(news, generated_at)
    with open(f"{DOCS_DIR}/news.html", "w", encoding="utf-8") as f:
        f.write(html)

    print(f"✓ 완료: docs/news.html ({generated_at})")
