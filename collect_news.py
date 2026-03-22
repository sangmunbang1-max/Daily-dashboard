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
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=RKLB&region=US&lang=en-US",
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=PLTR&region=US&lang=en-US",
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=IONQ&region=US&lang=en-US",
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=005930.KS&region=US&lang=en-US",
        "https://www.hankyung.com/feed/it",
    ],
}

# ── 애널리스트 관점 키워드 가중치 ─────────────────────────
KEYWORDS = {
    "us_market": {
        "high": [
            "s&p 500", "s&p500", "nasdaq", "dow jones", "fomc", "fed rate",
            "rate hike", "rate cut", "market crash", "market rally",
            "earnings beat", "earnings miss", "guidance cut", "guidance raise",
            "correction", "bear market", "bull market", "뉴욕증시",
        ],
        "medium": [
            "wall street", "stock market", "equity", "ipo", "buyback",
            "short squeeze", "vix", "volatility", "sector rotation",
            "주식시장", "증시", "주가",
        ],
        "low": ["stock", "shares", "invest", "portfolio", "index"],
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
        "low": ["korea", "korean", "한국 주식", "상장"],
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
            "yield curve", "inverted yield", "payrolls", "unemployment",
            "nonfarm", "한국은행", "물가", "고용", "실업률",
        ],
        "low": ["economy", "economic", "macro", "fiscal", "경제"],
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
            "corporate bond", "high yield", "채권", "국고채", "회사채",
        ],
        "low": ["fixed income", "debt", "coupon", "maturity"],
    },
    "commodities": {
        "high": [
            "oil price", "crude oil", "brent", "wti", "opec cut",
            "opec production", "gold price", "gold rally",
            "energy crisis", "supply disruption", "lng shortage",
            "유가", "원유", "금값", "opec 감산", "에너지 위기", "lng 공급",
        ],
        "medium": [
            "copper", "iron ore", "wheat", "corn", "commodity",
            "energy", "natural gas", "oil inventory",
            "원자재", "천연가스", "구리", "곡물",
        ],
        "low": ["fuel", "barrel", "석유", "에너지"],
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
        "low": ["dollar", "usd", "fx", "통화"],
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
            "imf warning", "chip ban", "export control",
            "무역 긴장", "지정학", "수출 통제", "반도체 수출 규제",
        ],
        "low": ["trade", "sanction", "conflict", "war", "무역", "제재"],
    },
    "tech": {
        "high": [
            # 반도체
            "nvidia earnings", "ai chip", "hbm", "hbm4", "tsmc production",
            "samsung hbm", "chip shortage", "semiconductor export ban",
            "semiconductor earnings", "memory chip", "nand", "dram",
            "엔비디아 실적", "삼성 hbm", "반도체 수출 규제", "tsmc 생산",
            "메모리 반도체", "시스템 반도체", "sk하이닉스 실적",
            # AI
            "openai", "anthropic", "ai investment", "ai model launch",
            "ai 투자", "생성형 ai",
            # 우주
            "spacex", "starship", "rocket launch", "space launch",
            "nasa", "blue origin", "satellite launch",
            "스페이스x", "우주 발사", "로켓 발사",
            # 로봇 · 자율주행
            "humanoid robot", "figure ai", "boston dynamics",
            "tesla optimus", "autonomous vehicle", "self-driving",
            "waymo", "인간형 로봇", "자율주행",
            # 양자 · 방산
            "quantum computing", "quantum computer", "hypersonic",
            "양자컴퓨터", "극초음속",
        ],
        "medium": [
            "nvidia", "tsmc", "amd", "intel", "arm", "qualcomm",
            "samsung semiconductor", "sk hynix",
            "data center", "generative ai", "llm", "gpu",
            "robotics", "automation", "robot", "drone",
            "space exploration", "starlink", "satellite",
            "palantir", "nuclear fusion", "clean energy tech",
            "반도체", "인공지능", "데이터센터", "엔비디아",
            "로봇", "드론", "위성", "핵융합", "파운드리",
        ],
        "low": [
            "tech", "semiconductor", "chip", "ai", "테크",
            "space", "rocket", "우주", "로켓",
        ],
    },
}

# ── 섹션 메타 ──────────────────────────────────────────────
SECTION_META = {
    "us_market":   ("🇺🇸 미국 증시",            "#5b9bd5"),
    "kr_market":   ("🇰🇷 한국 증시",            "#00d084"),
    "macro":       ("🏦 거시경제 · 금리",        "#f97316"),
    "bonds":       ("📊 채권 · 크레딧",         "#a78bfa"),
    "commodities": ("🛢 원자재 · 에너지",       "#f5c842"),
    "fx":          ("💱 환율 · 외환",           "#34d399"),
    "geopolitics": ("⚡ 지정학 · 무역",         "#f87171"),
    "tech":        ("💡 반도체 · AI · 미래산업", "#c084fc"),
}


# ── RSS pubDate 파싱 → KST datetime ───────────────────────
def parse_pub_date(pub_str: str):
    if not pub_str:
        return None
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%a, %d %b %Y %H:%M %z",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(pub_str.strip(), fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(KST)
        except:
            continue
    return None


# ── RSS 파싱 ───────────────────────────────────────────────
def parse_rss(url: str, timeout: int = 10) -> list:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; NewsFetcher/1.0)"}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        result = []
        for item in root.findall(".//item")[:30]:
            title = re.sub(r"<[^>]+>", "", (item.findtext("title") or "")).strip()
            link  = (item.findtext("link") or "").strip()
            desc  = re.sub(r"<[^>]+>", "", (item.findtext("description") or ""))[:150].strip()
            pub   = (item.findtext("pubDate") or "").strip()
            if title and "[Removed]" not in title:
                result.append({
                    "title":           title,
                    "description":     desc,
                    "url":             link,
                    "source":          url.split("/")[2].replace("feeds.", "").replace("www.", ""),
                    "publishedAt_raw": pub,
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
            params={"client": "gtx", "sl": "auto", "tl": "ko", "dt": "t", "q": text[:300]},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=5,
        )
        r.raise_for_status()
        return "".join(part[0] for part in r.json()[0] if part[0])
    except Exception:
        return text


# ── 키워드 점수 계산 (제목만) ──────────────────────────────
def score_article(title: str, kw_groups: dict) -> int:
    t = title.lower()
    score  = sum(5 for kw in kw_groups.get("high",   []) if kw in t)
    score += sum(3 for kw in kw_groups.get("medium", []) if kw in t)
    score += sum(1 for kw in kw_groups.get("low",    []) if kw in t)
    return score


# ── 뉴스 수집 ──────────────────────────────────────────────
def fetch_news() -> dict:
    result: dict = {k: [] for k in SECTION_META}

    # 수집 기간: 전일 07:00 ~ 당일 07:00 KST
    now_kst      = datetime.now(KST)
    cutoff_end   = now_kst.replace(hour=7, minute=0, second=0, microsecond=0)
    if now_kst < cutoff_end:
        cutoff_end -= timedelta(days=1)
    cutoff_start = cutoff_end - timedelta(days=1)

    print(f"  수집 기간: {cutoff_start.strftime('%m/%d %H:%M')} ~ {cutoff_end.strftime('%m/%d %H:%M')} KST")

    # RSS 수집
    print("  RSS 수집 중...")
    raw_pool: list = []
    fetched_urls: set = set()
    for key, urls in RSS_FEEDS.items():
        for url in urls:
            if url not in fetched_urls:
                raw_pool.extend(parse_rss(url))
                fetched_urls.add(url)

    print(f"  총 {len(raw_pool)}개 기사 수집 (필터 전)")

    # 시간 필터
    filtered_pool = []
    for article in raw_pool:
        pub_dt = parse_pub_date(article.get("publishedAt_raw", ""))
        if pub_dt and cutoff_start <= pub_dt <= cutoff_end:
            article["publishedAt"] = pub_dt.strftime("%m/%d %H:%M")
            article["_pub_dt"]     = pub_dt
            filtered_pool.append(article)

    print(f"  시간 필터 후: {len(filtered_pool)}개")

    # 기사 부족 시 48시간으로 자동 확장
    if len(filtered_pool) < 15:
        print("  [확장] 기사 부족 → 48시간으로 범위 확장")
        cutoff_start = cutoff_end - timedelta(days=2)
        filtered_pool = []
        for article in raw_pool:
            pub_dt = parse_pub_date(article.get("publishedAt_raw", ""))
            if pub_dt and cutoff_start <= pub_dt <= cutoff_end:
                article["publishedAt"] = pub_dt.strftime("%m/%d %H:%M")
                article["_pub_dt"]     = pub_dt
                filtered_pool.append(article)
        print(f"  확장 후: {len(filtered_pool)}개")

    # 최신순 정렬
    filtered_pool.sort(
        key=lambda x: x.get("_pub_dt", datetime.min.replace(tzinfo=KST)),
        reverse=True
    )

    # 중복 제거
    seen: set = set()
    deduped = []
    for a in filtered_pool:
        k = a["title"][:60]
        if k not in seen:
            seen.add(k)
            deduped.append(a)

    # 카테고리별 점수 계산 → 상위 5건 선정 → 번역
    category_scored: dict = {k: [] for k in SECTION_META}
    for article in deduped:
        for key, kw_groups in KEYWORDS.items():
            s = score_article(article["title"], kw_groups)
            if s > 0:
                category_scored[key].append((s, article.copy()))

    for key in SECTION_META:
        category_scored[key].sort(key=lambda x: x[0], reverse=True)
        for score, article in category_scored[key][:5]:
            article["title"]       = translate_to_korean(article["title"])
            article["description"] = translate_to_korean(article["description"])
            article["score"]       = score
            article.pop("_pub_dt", None)
            article.pop("publishedAt_raw", None)
            result[key].append(article)

    for key, arts in result.items():
        scores = [a["score"] for a in arts]
        print(f"  [{key}] {len(arts)}건 | 점수: {scores}")

    return result


# ── HTML 생성 ──────────────────────────────────────────────
def generate_html(news: dict, generated_at: str, period_str: str) -> str:
    sections_html = ""
    for key, (title, color) in SECTION_META.items():
        articles = news.get(key, [])
        if not articles:
            items_html = '<p style="color:#6060a0;font-size:13px;padding:12px 0;">해당 기간 수집된 뉴스가 없습니다.</p>'
        else:
            items_html = ""
            for a in articles:
                desc      = a.get("description", "")
                desc_html = f'<p class="art-desc">{desc}</p>' if desc else ""
                url       = a.get("url", "")
                score     = a.get("score", 0)
                badge     = '<span class="badge-high">HOT</span>' if score >= 10 else \
                            '<span class="badge-mid">KEY</span>'  if score >= 5  else ""
                lo = f'<a href="{url}" target="_blank" rel="noopener" style="text-decoration:none;color:inherit;">' if url else "<span>"
                lc = "</a>" if url else "</span>"
                items_html += f"""<div class="art-item">
  {lo}
  <div class="art-header">
    <span class="art-source">{a.get('source','')}</span>
    <span style="display:flex;align-items:center;gap:5px;">{badge}<span class="art-date">{a.get('publishedAt','')}</span></span>
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
.meta-bar{{max-width:1100px;margin:0 auto 24px;display:flex;flex-direction:column;gap:4px;font-family:"IBM Plex Mono",monospace;font-size:11px;color:#a0a0c0}}
.meta-period{{color:#d0d0e8;font-size:12px;font-weight:500}}
.grid{{max-width:1100px;margin:0 auto;display:grid;grid-template-columns:repeat(4,1fr);gap:14px}}
@media(max-width:900px){{.grid{{grid-template-columns:1fr 1fr}}}}
@media(max-width:520px){{.grid{{grid-template-columns:1fr}}}}
.section-card{{background:#13131f;border:1px solid #252538;border-radius:12px;padding:18px 18px 10px}}
.section-title{{font-family:"IBM Plex Sans KR",sans-serif;font-size:13px;font-weight:600;margin-bottom:14px}}
.art-item{{padding:9px 0;border-bottom:1px solid #1a1a28}}
.art-item:last-child{{border-bottom:none}}
.art-header{{display:flex;justify-content:space-between;align-items:center;margin-bottom:4px}}
.art-source{{font-family:"IBM Plex Mono",monospace;font-size:10px;color:#5b5b80;font-weight:600}}
.art-date{{font-family:"IBM Plex Mono",monospace;font-size:10px;color:#6060a0}}
.art-title{{font-size:13px;font-weight:600;color:#e0e0f0;line-height:1.6;margin-bottom:3px}}
.art-title:hover{{color:#c0d8f0}}
.art-desc{{font-size:12px;color:#8888aa;line-height:1.6}}
.badge-high{{font-family:"IBM Plex Mono",monospace;font-size:9px;font-weight:700;padding:2px 5px;border-radius:3px;background:rgba(229,62,62,0.2);color:#e53e3e;border:1px solid #e53e3e44}}
.badge-mid{{font-family:"IBM Plex Mono",monospace;font-size:9px;font-weight:700;padding:2px 5px;border-radius:3px;background:rgba(245,200,66,0.15);color:#f5c842;border:1px solid #f5c84244}}
.footer{{max-width:1100px;margin:32px auto 0;text-align:center;font-size:11px;color:#9090b8;font-family:"IBM Plex Mono",monospace;line-height:2}}
</style>
</head>
<body>
<div class="page-header">
  <div class="page-title">DAILY NEWS BRIEF</div>
  <a href="index.html" class="nav-link">← DASHBOARD</a>
</div>
<div class="meta-bar">
  <span class="meta-period">수집 기간: {period_str}</span>
  <span>생성: {generated_at} &nbsp;|&nbsp; Reuters · Yahoo Finance · 한국경제 · 연합뉴스 &nbsp;|&nbsp; 점수 기반 선별</span>
</div>
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
    now          = datetime.now(KST)
    generated_at = now.strftime("%Y-%m-%d %H:%M KST")

    cutoff_end   = now.replace(hour=7, minute=0, second=0, microsecond=0)
    if now < cutoff_end:
        cutoff_end -= timedelta(days=1)
    cutoff_start = cutoff_end - timedelta(days=1)
    period_str   = f"{cutoff_start.strftime('%m/%d %H:%M')} ~ {cutoff_end.strftime('%m/%d %H:%M')} KST"

    print(f"[NEWS] 시작: {generated_at}")

    news = fetch_news()

    with open(f"{DATA_DIR}/news_latest.json", "w", encoding="utf-8") as f:
        json.dump({
            "generated_at_kst": generated_at,
            "period":           period_str,
            "news":             news,
        }, f, ensure_ascii=False, indent=2)

    html = generate_html(news, generated_at, period_str)
    with open(f"{DOCS_DIR}/news.html", "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[NEWS] 완료: docs/news.html ({generated_at})")
