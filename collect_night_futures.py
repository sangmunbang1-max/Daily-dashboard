# -*- coding: utf-8 -*-
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from kis_token_manager import get_valid_kis_token

KST = timezone(timedelta(hours=9))

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "docs" / "data" / "night_futures"
LATEST_FILE = DATA_DIR / "latest.json"


def ensure_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def now_kst():
    return datetime.now(KST)


def make_empty_series():
    # 각 포인트 형식:
    # {"time": "19:00", "price": 352.15, "change": 1.25, "change_pct": 0.36}
    return {
        "kospi": [],
        "kosdaq": [],
    }


def make_empty_summary():
    return {
        "last": None,
        "change": None,
        "change_pct": None,
        "high": None,
        "low": None,
        "points": 0,
    }


def make_payload_stub():
    now = now_kst()
    biz_date = now.strftime("%Y-%m-%d")

    return {
        "generated_at_kst": now.isoformat(),
        "biz_date": biz_date,
        "session": "night",
        "source": "KIS",
        "note": "stub payload before live API binding",
        "series": make_empty_series(),
        "summary": {
            "kospi": make_empty_summary(),
            "kosdaq": make_empty_summary(),
        },
    }


def save_payload(payload: dict):
    ensure_dir()
    with open(LATEST_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def main():
    # 아직은 실데이터 호출 전 단계
    # 단, 공통 토큰 구조 연결은 여기서 확인 가능
    _kis_token = get_valid_kis_token()

    payload = make_payload_stub()
    payload["token_status"] = "ready" if _kis_token else "missing"

    save_payload(payload)
    print(f"[OK] saved stub night futures payload -> {LATEST_FILE}")


if __name__ == "__main__":
    main()
