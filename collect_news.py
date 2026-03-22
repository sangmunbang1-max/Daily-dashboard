# -*- coding: utf-8 -*-
import os, json, re, requests
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

KST = timezone(timedelta(hours=9))
DOCS_DIR = "docs"
DATA_DIR  = f"{DOCS_DIR}/data"

# ── RSS 피드 ───────────────────────────────────────────────
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

# ── 애널리스트 관점 키워드 가중치 ─────────────────────────
# high(5점): 시장에 즉각적 영향, 가격 움직임 직결
# medium(3점): 중요하지만 간접적 영향
# low(1점): 배경 컨텍스트 제공
KEYWORDS = {
    "us_market": {
        "high": [
            "s&p 500", "s&p500", "nasdaq", "dow jones", "fomc", "fed rate",
            "rate hike", "rate cut", "market crash", "market rally",
            "earnings beat", "earnings miss", "guidance cut", "guidance raise",
            "correction", "bear market", "bull market", "코스피", "뉴욕증시",
        ],
        "medium": [
            "wall street", "stock market", "equity", "ipo", "buyback",
            "short squeeze", "vix", "volatility", "sector rotation",
            "주식시장", "증시", "주가", "상승", "하락",
        ],
        "low": [
            "stock", "shares", "invest", "portfolio", "index",
        ],
    },
    "kr_market": {
        "high": [
            "코스피", "코스닥", "삼성전자", "sk하이닉스", "외국인 순매도",
            "외국인 순매수", "기관 순매도", "기관 순매수", "프로그램 매매",
            "kospi", "kosdaq", "samsung electronics", "hynix",
            "circuit breaker", "사이드카",
        ],
        "medium": [
            "한국증시", "국내증시", "코스피200", "현대차", "lg에너지솔루션",
            "셀트리온", "카카오", "네이버", "korean stock", "seoul",
            "수급", "거래대금", "시가총액",
        ],
        "low": [
            "korea", "korean", "한국 주식", "상장",
        ],
    },
    "macro": {
        "high": [
            "federal reserve", "fomc", "jerome powell", "rate decision",
            "interest rate hike", "interest rate cut", "cpi", "pce",
            "inflation data", "gdp growth", "recession", "stagflation",
            "연준", "기준금리", "금리 인상", "금리 인하", "소비자물가",
            "경기침체", "스태그플레이션",
        ],
        "medium": [
            "monetary policy", "central bank", "quantitative tightening",
            "qt", "qe", "yield curve", "inverted yield", "payrolls",
            "unemployment", "nonfarm", "한국은행", "기준금리",
            "물가", "고용", "실업률",
        ],
        "low": [
            "economy", "economic", "macro", "fiscal", "경제",
        ],
    },
    "bonds": {
        "high": [
            "treasury yield", "10-year yield", "2-year yield",
            "yield curve inversion", "bond selloff", "credit spread",
            "default", "junk bond", "investment grade downgrade",
            "국채 금리", "장단기 금리차", "신용등급 강등",
        ],
        "medium": [
            "treasury", "bond auction", "duration", "tlt", "hyg",
            "corporate bond", "high yield", "ig spread",
            "채권", "국고채", "회사채", "금리차",
        ],
        "low": [
            "fixed income", "debt", "coupon", "maturity",
        ],
    },
    "commodities": {
        "high": [
            "oil price", "crude oil", "brent", "wti", "opec cut",
            "opec production", "gold price", "gold rally",
            "energy crisis", "supply disruption", "lng shortage",
            "유가", "원유", "금값", "opec 감산", "에너지 위기",
            "lng 공급",
        ],
        "medium": [
            "copper", "iron ore", "wheat", "corn", "commodity",
            "energy", "natural gas", "oil inventory",
            "원자재", "천연가스", "구리", "곡물",
        ],
        "low": [
            "fuel", "barrel", "석유", "에너지",
        ],
    },
    "fx": {
        "high": [
            "dollar index", "dxy", "usd/krw", "won weakens", "won strengthens",
            "currency intervention", "fx intervention", "yen weakness",
            "dollar surge", "dollar drop", "환율", "원화 약세", "원화 강세",
            "외환시장 개입", "달러 강세", "달러 약세",
        ],
        "medium": [
            "forex", "currency", "exchange rate", "euro", "yen", "yuan",
            "renminbi", "emerging market currency",
            "외환", "달러", "엔화", "위안화", "유로화",
        ],
        "low": [
            "dollar", "usd", "fx", "통화",
        ],
    },
    "geopolitics": {
        "high": [
            "trade war", "tariff hike", "sanctions", "military conflict",
            "strait of hormuz", "taiwan strait", "north korea missile",
            "iran nuclear", "oil embargo", "supply chain disruption",
            "관세 인상", "무역 전쟁", "호르무즈 해협", "북한 미사일",
            "이란 제재", "공급망 붕괴",
        ],
        "medium": [
            "tariff", "trade tension", "geopolitical", "nato", "g7", "g20",
            "imf warning", "world bank", "chip ban", "export control",
            "무역 긴장", "지정학", "수출 통제", "반도체 수출 규제",
        ],
        "low": [
            "trade", "sanction", "conflict", "war", "무역", "제재",
        ],
    },
    "tech": {
        "high": [
            "nvidia earnings", "ai chip", "hbm", "tsmc production",
            "samsung hbm", "openai", "anthropic", "ai investment",
            "chip shortage", "semiconductor export ban",
            "엔비디아 실적", "hbm4", "삼성 hbm", "ai 투자",
            "반도체 수출 규제", "tsmc 생산",
        ],
        "medium": [
            "nvidia", "tsmc", "amd", "intel", "arm", "ai model",
            "data center", "generative ai", "llm", "gpu",
            "반도체", "인공지능", "데이터센터", "엔비디아", "sk하이닉스 hbm",
        ],
        "low": [
            "tech", "semiconductor", "chip", "ai", "테크", "ai",
        ],
    },
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
    if not text or not text.strip():
        return text
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
        return text


# ── 뉴스 수집 (점수제) ────────────────────────────────────
def score_article(title: str, kw_groups: dict) -> int:
    """제목에서만 키워드 매칭 → 가중치 점수 계산"""
    t = title.lower()
    score = 0
    score += sum(5 for kw in kw_groups.get("high",   []) if kw in t)
    score += sum(3 for kw in kw_groups.get("medium", []) if kw in t)
    score += sum(1 for kw in kw_groups.get("low",    []) if kw in t)
    return score


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

    # 최신 기사 우선 정렬
    raw_pool.sort(key=lambda x: x.get("publishedAt", ""), reverse=True)

    # 카테고리별 점수 계산
    # {카테고리: [(score, article), ...]}
    category_scored: dict = {k: [] for k in SECTION_META}
    seen_titles: set = set()

    for article in raw_pool:
        title_key = article["title"][:60]
        if title_key in seen_titles:
            continue
        seen_titles.add(title_key)

        for key, kw_groups in KEYWORDS.items():
            score = score_article(article["title"], kw_groups)
            if score > 0:
                category_scored[key].append((score, article.copy()))

    # 카테고리별 점수 높은 순 상위 5건 선정 → 번역
    for key in SECTION_META:
        # 점수 내림차순 → 같은 점수면 최신순 유지
        category_scored[key].sort(key=lambda x: x[0], reverse=True)
        selected = category_scored[key][:5]
        for score, article in selected:
            article["title"]       = translate_to_korean(article["title"])
            article["description"] = translate_to_korean(article["description"])
            article["score"]       = score  # 디버그용
            result[key].append(article)

    for key, arts in result.items():
        scores = [a["score"] for a in arts]
        print(f"  [{key}] {len(arts)}건 | 점수: {scores}")

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
                score = a.get("score", 0)
                # 점수에 따라 중요도 배지
                if score >= 10:
                    badge = f'<span class="badge-high">HOT</span>'
                elif score >= 5:
                    badge = f'<span class="badge-mid">KEY</span>'
                else:
                    badge = ""
                lo = f'<a href="{url}" target="_blank" rel="noopener" style="text-decoration:none;color:inherit;">' if url else "<span>"
                lc = "</a>" if url else "</span>"
                items_html += f"""<div class="art-item">
  {lo}
  <div class="art-header">
    <span class="art-source">{a.get('source','')}</span>
    <span style="display:flex;align-items:center;gap:5px;">
      {badge}
      <span class="art-date">{a.get('publishedAt','')}</span>
    </span>
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
.art-header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px}}
.art-source{{font-family:"IBM Plex Mono",monospace;font-size:10px;color:#5b5b80;font-weight:600}}
.art-date{{font-family:"IBM Plex Mono",monospace;font-size:10px;color:#4a4a6a}}
.art-title{{font-size:13px;font-weight:600;color:#e0e0f0;line-height:1.6;margin-bottom:3px}}
.art-title:hover{{color:#c0d8f0}}
.art-desc{{font-size:12px;color:#8888aa;line-height:1.6}}
.badge-high{{font-family:"IBM Plex Mono",monospace;font-size:9px;font-weight:700;padding:2px 5px;border-radius:3px;background:rgba(229,62,62,0.2);color:#e53e3e;border:1px solid #e53e3e44}}
.badge-mid{{font-family:"IBM Plex Mono",monospace;font-size:9px;font-weight:700;padding:2px 5px;border-radius:3px;background:rgba(245,200,66,0.15);color:#f5c842;border:1px solid #f5c84244}}
.footer{{max-width:1100px;margin:32px auto 0;text-align:center;font-size:11px;color:#353558;font-family:"IBM Plex Mono",monospace;line-height:2}}
</style>
</head>
<body>
<div class="page-header">
  <div class="page-title">DAILY NEWS BRIEF</div>
  <a href="index.html" class="nav-link">← DASHBOARD</a>
</div>
<div class="meta-bar">업데이트: {generated_at} &nbsp;|&nbsp; Reuters · Yahoo Finance · 한국경제 · 연합뉴스 &nbsp;|&nbsp; 점수 기반 선별</div>
<div class="grid">{sections_html}</div>
<div class="footer">
  <p>HOT = 점수 10+ &nbsp;|&nbsp; KEY = 점수 5~9 &nbsp;|&nbsp; 매일 07:30 KST 자동 갱신</p>
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
