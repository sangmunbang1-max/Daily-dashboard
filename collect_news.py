# -*- coding: utf-8 -*-
import os, json, re, requests
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

KST = timezone(timedelta(hours=9))
DOCS_DIR = "docs"
DATA_DIR  = f"{DOCS_DIR}/data"

# ── RSS 피드 정의 ──────────────────────────────────────────
RSS_FEEDS = {
    "us_market": [
        "https://feeds.reuters.com/reuters/businessNews",
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^GSPC&region=US&lang=en-US",
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=SPY&region=US&lang=en-US",
    ],
    "kr_market": [
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^KS11&region=US&lang=en-US",
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=005930.KS&region=US&lang=en-US",
        "https://www.hankyung.com/feed/finance",
        "https://news.einfomax.co.kr/rss/allArticle.xml",
    ],
    "macro": [
        "https://feeds.reuters.com/reuters/businessNews",
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^TNX&region=US&lang=en-US",
        "https://www.hankyung.com/feed/economy",
    ],
    "bonds": [
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=TLT&region=US&lang=en-US",
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=HYG&region=US&lang=en-US",
    ],
    "commodities": [
        "https://feeds.reuters.com/reuters/businessNews",
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=CL=F&region=US&lang=en-US",
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=GC=F&region=US&lang=en-US",
        "https://www.hankyung.com/feed/economy",
    ],
    "fx": [
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=USDKRW=X&region=US&lang=en-US",
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=DX-Y.NYB&region=US&lang=en-US",
        "https://www.hankyung.com/feed/finance",
    ],
    "geopolitics": [
        "https://feeds.reuters.com/reuters/worldNews",
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=USO&region=US&lang=en-US",
        "https://www.yonhapnews.co.kr/rss/economy.xml",
    ],
    "tech": [
        "https://feeds.reuters.com/reuters/technologyNews",
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=NVDA&region=US&lang=en-US",
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=TSM&region=US&lang=en-US",
        "https://www.hankyung.com/feed/it",
    ],
}

# ── 키워드 필터 ────────────────────────────────────────────
KEYWORDS = {
    "us_market":   ["stock", "nasdaq", "s&p", "dow", "wall street", "equity", "market", "earnings", "nyse"],
    "kr_market":   ["korea", "kospi", "kosdaq", "samsung", "hyundai", "seoul", "korean", "south korea", "won", "코스피", "코스닥", "삼성", "현대", "증시", "주식"],
    "macro":       ["fed", "federal reserve", "inflation", "gdp", "interest rate", "cpi", "economy", "recession", "연준", "금리", "인플레", "경제성장", "기준금리"],
    "bonds":       ["treasury", "bond", "yield", "debt", "credit", "fixed income", "국채", "채권", "금리", "수익률"],
    "commodities": ["oil", "crude", "gold", "copper", "lng", "energy", "commodity", "opec", "brent", "wti", "유가", "원유", "금값", "에너지", "원자재"],
    "fx":          ["dollar", "currency", "forex", "yuan", "yen", "euro", "won", "exchange rate", "환율", "달러", "원화", "외환"],
    "geopolitics": ["tariff", "trade", "war", "sanction", "iran", "china", "geopolit", "conflict", "nato", "관세", "무역", "전쟁", "제재", "지정학"],
    "tech":        ["ai", "semiconductor", "nvidia", "chip", "tech", "tsmc", "intel", "data center", "인공지능", "반도체", "엔비디아", "테크"],
}

# ── 섹션 메타 ──────────────────────────────────────────────
SECTION_META = {
    "us_market":   ("🇺🇸 미국 증시",      "#5b9bd5"),
    "kr_market":   ("🇰🇷 한국 증시",      "#00d084"),
    "macro":       ("🏦 거시경제 · 금리",  "#f97316"),
    "bonds":       ("📊 채권 · 크레딧",   "#a78bfa"),
    "commodities": ("🛢 원자재 · 에너지", "#f5c842"),
    "fx":          ("💱 환율 · 외환",     "#34d399"),
    "geopolitics": ("⚡ 지정학 · 무역",   "#f87171"),
    "tech":        ("💡 AI · 반도체",     "#c084fc"),
}


# ── RSS 파싱 ───────────────────────────────────────────────
def parse_rss(url: str, timeout: int = 10) -> list:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; NewsFetcher/1.0)"}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        items = root.findall(".//item")
        result = []
        for item in items[:20]:
            title = (item.findtext("title") or "").strip()
            link  = (item.findtext("link")  or "").strip()
            desc  = (item.findtext("description") or "").strip()
            pub   = (item.findtext("pubDate") or "").strip()
            desc  = re.sub(r"<[^>]+>", "", desc)[:150]
            title = re.sub(r"<[^>]+>", "", title)
            if title and "[Removed]" not in title:
                result.append({
                    "title":       title,
                    "description": desc,
                    "url":         link,
                    "source":      url.split("/")[2].replace("feeds.", "").replace("www.", ""),
                    "publishedAt": pub[:16],
                })
        return result
    except Exception as e:
        print(f"    RSS 오류 ({url[:50]}): {e}")
        return []


# ── 번역 ──────────────────────────────────────────────────
def translate_to_korean(text: str) -> str:
    """Google Translate 비공식 API — 무료, 키 불필요"""
    if not text or not text.strip():
        return text
    # 이미 한국어면 번역 안 함 (한글 비율 20% 이상)
    korean_chars = sum(1 for c in text if '\uAC00' <= c <= '\uD7A3')
    if korean_chars / max(len(text), 1) > 0.2:
        return text
    try:
        r = requests.get(
            "https://translate.googleapis.com/translate_a/single",
            params={
                "client": "gtx",
                "sl":     "auto",
                "tl":     "ko",
                "dt":     "t",
                "q":      text[:300],
            },
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=5,
        )
        r.raise_for_status()
        result = r.json()
        translated = "".join(part[0] for part in result[0] if part[0])
        return translated
    except Exception:
        return text  # 번역 실패 시 원문 반환


# ── 뉴스 수집 ──────────────────────────────────────────────
def fetch_news() -> dict:
    result: dict = {k: [] for k in SECTION_META}
    print("  RSS 수집 중...")
    raw_pool: list = []
    fetched_urls: set = set()

    for key, urls in RSS_FEEDS.items():
        for url in urls:
            if url not in fetched_urls:
                items = parse_rss(url)
                raw_pool.extend(items)
                fetched_urls.add(url)

    print(f"  총 {len(raw_pool)}개 기사 수집")
    raw_pool.sort(key=lambda x: x.get("publishedAt", ""), reverse=True)

    seen_titles: set = set()
    for article in raw_pool:
        text = (article["title"] + " " + article["description"]).lower()
        for key, kws in KEYWORDS.items():
            if len(result[key]) >= 5:
                continue
            if any(kw in text for kw in kws):
                title_key = article["title"][:40]
                if title_key not in seen_titles:
                    # 영어 기사 → 한국어 번역
                    article["title"]       = translate_to_korean(article["title"])
                    article["description"] = translate_to_korean(article["description"])
                    result[key].append(article)
                    seen_titles.add(title_key)
                    break

    for key, arts in result.items():
        print(f"  [{key}] {len(arts)}건")

    return result


# ── HTML 생성 ──────────────────────────────────────────────
def generate_html(news: dict, generated_at: str) -> str:
    sections_html = ""
    for key, (title, color) in SECTION_META.items():
        articles = news.get(key, [])
        if not articles:
            items_html = '<p style="color:#4a4a6a;font-size:13px;padding:12px 0;">수집된 뉴스가 없습니다.</p>'
        else:
            items_html = ""
            for a in articles:
                desc = a.get("description", "")
                desc_html = f'<p class="art-desc">{desc}</p>' if desc else ""
                url = a.get("url", "")
                lo = f'<a href="{url}" target="_blank" rel="noopener" style="text-decoration:none;color:inherit;">' if url else "<span>"
                lc = "</a>" if url else "</span>"
                items_html += f"""<div class="art-item">
  {lo}
  <div class="art-header">
    <span class="art-source">{a.get('source','')}</span>
    <span class="art-date">{a.get('publishedAt','')}</span>
  </div>
  <p class="art-title">{a.get('title','')}</p>
  {desc_html}
  {lc}
</div>"""

        sections_html += f"""<div class="section-card" style="border-top:3px solid {color};">
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
.section-card{{background:#13131f;border:1px solid #252538;border-radius:12px;padding:18px 18px 10px}}
.section-title{{font-family:"IBM Plex Sans KR",sans-serif;font-size:13px;font-weight:600;margin-bottom:14px}}
.art-item{{padding:9px 0;border-bottom:1px solid #1a1a28}}
.art-item:last-child{{border-bottom:none}}
.art-header{{display:flex;justify-content:space-between;margin-bottom:3px}}
.art-source{{font-family:"IBM Plex Mono",monospace;font-size:10px;color:#5b5b80;font-weight:600}}
.art-date{{font-family:"IBM Plex Mono",monospace;font-size:10px;color:#4a4a6a}}
.art-title{{font-size:13px;font-weight:600;color:#e0e0f0;line-height:1.6;margin-bottom:3px}}
.art-title:hover{{color:#c0d8f0}}
.art-desc{{font-size:12px;color:#8888aa;line-height:1.6}}
.footer{{max-width:1100px;margin:32px auto 0;text-align:center;font-size:11px;color:#353558;font-family:"IBM Plex Mono",monospace;line-height:2}}
</style>
</head>
<body>
<div class="page-header">
  <div class="page-title">DAILY NEWS BRIEF</div>
  <a href="index.html" class="nav-link">← DASHBOARD</a>
</div>
<div class="meta-bar">업데이트: {generated_at} &nbsp;|&nbsp; Reuters · Yahoo Finance · 한국경제 · 연합뉴스</div>
<div class="grid">{sections_html}</div>
<div class="footer">
  <p>매일 07:30 KST 자동 갱신 &nbsp;|&nbsp; API 키 불필요 · 완전 무료</p>
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

    news = fetch_news()

    with open(f"{DATA_DIR}/news_latest.json", "w", encoding="utf-8") as f:
        json.dump({"generated_at_kst": generated_at, "news": news},
                  f, ensure_ascii=False, indent=2)

    html = generate_html(news, generated_at)
    with open(f"{DOCS_DIR}/news.html", "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[NEWS] 완료: docs/news.html ({generated_at})")
