# -*- coding: utf-8 -*-
import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from kis_token_manager import get_valid_kis_token

KST = timezone(timedelta(hours=9))

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "docs" / "data" / "night_futures"
LATEST_FILE = DATA_DIR / "latest.json"

KIS_FO_QUOTE_URL = os.environ.get(
    "KIS_FO_QUOTE_URL",
    "https://openapi.koreainvestment.com:9443/uapi/domestic-futureoption/v1/quotations/display-board-futures",
)

KIS_APP_KEY = os.environ.get("KIS_APP_KEY", "")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "")

TR_ID = "FHPIF05030200"
MRKT_DIV_CODE = "F"
SCR_DIV_CODE = "20503"

# 스펙 기준:
# 공백(""): KOSPI200
# KQI     : KOSDAQ150
KOSPI_MARKET_CLASS = ""
KOSDAQ_MARKET_CLASS = "KQI"


def ensure_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def now_kst() -> datetime:
    return datetime.now(KST)


def kst_hhmm(dt: Optional[datetime] = None) -> str:
    dt = dt or now_kst()
    return dt.strftime("%H:%M")


def today_kst_str(dt: Optional[datetime] = None) -> str:
    dt = dt or now_kst()
    return dt.strftime("%Y-%m-%d")


def get_night_biz_date(dt: Optional[datetime] = None) -> str:
    """
    야간세션 기준 거래일
    - 18:00 ~ 23:59 : 당일
    - 00:00 ~ 05:59 : 전일
    """
    dt = dt or now_kst()
    if dt.hour < 6:
        dt = dt - timedelta(days=1)
    return dt.strftime("%Y-%m-%d")


def is_night_session(dt: Optional[datetime] = None) -> bool:
    """
    야간세션 시간인지 판정
    - 18:00 ~ 23:59
    - 00:00 ~ 05:59
    """
    dt = dt or now_kst()
    return (dt.hour >= 18) or (dt.hour < 6)


def load_existing_payload() -> Dict[str, Any]:
    if LATEST_FILE.exists():
        try:
            with open(LATEST_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                if isinstance(loaded, dict):
                    return loaded
        except Exception:
            pass
    return make_empty_payload()


def save_payload(payload: Dict[str, Any]) -> None:
    ensure_dir()
    with open(LATEST_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def make_empty_series() -> Dict[str, List[Dict[str, Any]]]:
    return {"kospi": [], "kosdaq": []}


def make_empty_summary() -> Dict[str, Any]:
    return {
        "last": None,
        "change": None,
        "change_pct": None,
        "high": None,
        "low": None,
        "points": 0,
    }


def make_empty_payload() -> Dict[str, Any]:
    now = now_kst()
    return {
        "generated_at_kst": now.isoformat(),
        "biz_date": get_night_biz_date(now),
        "session": "night",
        "source": "KIS",
        "note": "night futures snapshot",
        "token_status": "missing",
        "series": make_empty_series(),
        "summary": {
            "kospi": make_empty_summary(),
            "kosdaq": make_empty_summary(),
        },
        "meta": {
            "mode": "snapshot",
            "interval_minutes": 30,
        },
    }


def safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None

    if isinstance(v, (int, float)):
        if isinstance(v, float) and math.isnan(v):
            return None
        return float(v)

    s = str(v).strip().replace(",", "")
    if s == "":
        return None

    try:
        return float(s)
    except Exception:
        return None


def calc_summary(points: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not points:
        return make_empty_summary()

    prices = [p.get("price") for p in points if p.get("price") is not None]
    last_point = points[-1]

    return {
        "last": last_point.get("price"),
        "change": last_point.get("change"),
        "change_pct": last_point.get("change_pct"),
        "high": max(prices) if prices else None,
        "low": min(prices) if prices else None,
        "points": len(points),
    }


def upsert_point(points: List[Dict[str, Any]], new_point: Dict[str, Any]) -> List[Dict[str, Any]]:
    snap_time = new_point["time"]
    updated: List[Dict[str, Any]] = []
    replaced = False

    for p in points:
        if p.get("time") == snap_time:
            updated.append(new_point)
            replaced = True
        else:
            updated.append(p)

    if not replaced:
        updated.append(new_point)

    updated.sort(key=lambda x: x.get("time", ""))
    return updated


def is_configured() -> bool:
    return all([
        bool(KIS_FO_QUOTE_URL),
        bool(KIS_APP_KEY),
        bool(KIS_APP_SECRET),
    ])


def build_headers(access_token: str) -> Dict[str, str]:
    return {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {access_token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": TR_ID,
        "custtype": "P",
    }


def build_params(market_class_code: str) -> Dict[str, str]:
    return {
        "FID_COND_MRKT_DIV_CODE": MRKT_DIV_CODE,
        "FID_COND_SCR_DIV_CODE": SCR_DIV_CODE,
        "FID_COND_MRKT_CLS_CODE": market_class_code,
    }


def request_market_snapshot(market_class_code: str, access_token: str) -> Dict[str, Any]:
    resp = requests.get(
        KIS_FO_QUOTE_URL,
        headers=build_headers(access_token),
        params=build_params(market_class_code),
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def extract_best_output_row(js: Dict[str, Any]) -> Dict[str, Any]:
    rt_cd = str(js.get("rt_cd", ""))
    if rt_cd != "0":
        raise RuntimeError(f"KIS 응답 오류: rt_cd={js.get('rt_cd')} msg={js.get('msg1')}")

    output = js.get("output")
    if output is None:
        output = js.get("output1")

    if isinstance(output, dict):
        return output

    if not isinstance(output, list):
        raise RuntimeError(f"output 형식 이상: {js}")

    rows = [row for row in output if isinstance(row, dict)]
    if not rows:
        raise RuntimeError(f"output 비어있음: {js}")

    # 근월물 우선:
    # 1) 잔존일수 최소
    # 2) 거래량 최대
    def sort_key(row: Dict[str, Any]):
        rmnn = safe_float(row.get("hts_rmnn_dynu"))
        vol = safe_float(row.get("acml_vol"))
        rmnn_val = rmnn if rmnn is not None else 999999
        vol_val = vol if vol is not None else -1
        return (rmnn_val, -vol_val)

    rows.sort(key=sort_key)
    return rows[0]


def normalize_snapshot(row: Dict[str, Any], label: str, snap_time: str) -> Dict[str, Any]:
    price = safe_float(row.get("futs_prpr"))
    change = safe_float(row.get("futs_prdy_vrss"))
    change_pct = safe_float(row.get("futs_prdy_ctrt"))

    if price is None:
        raise RuntimeError(f"{label}: futs_prpr 현재가 없음.")

    return {
        "time": snap_time,
        "price": price,
        "change": change,
        "change_pct": change_pct,
    }


def append_snapshot(payload: Dict[str, Any], key: str, point: Dict[str, Any]) -> None:
    series = payload.setdefault("series", make_empty_series())
    current_points = series.get(key, [])
    series[key] = upsert_point(current_points, point)

    summary = payload.setdefault(
        "summary",
        {"kospi": make_empty_summary(), "kosdaq": make_empty_summary()},
    )
    summary[key] = calc_summary(series[key])


def reset_if_new_session_day(payload: Dict[str, Any], biz_date: str) -> Dict[str, Any]:
    if payload.get("biz_date") != biz_date:
        new_payload = make_empty_payload()
        new_payload["biz_date"] = biz_date
        return new_payload
    return payload


def main() -> None:
    now = now_kst()

    # 세션 외 시간에는 저장 안 함
    if not is_night_session(now):
        print("[SKIP] outside night session")
        return

    biz_date = get_night_biz_date(now)
    snap_time = kst_hhmm(now)

    payload = load_existing_payload()
    payload = reset_if_new_session_day(payload, biz_date)

    payload["generated_at_kst"] = now.isoformat()
    payload["biz_date"] = biz_date
    payload["session"] = "night"
    payload["source"] = "KIS"
    payload.setdefault("meta", {})
    payload["meta"]["mode"] = "snapshot"
    payload["meta"]["interval_minutes"] = 30

    if not is_configured():
        payload["token_status"] = "config_missing"
        payload["note"] = "set KIS_FO_QUOTE_URL and KIS credentials"
        save_payload(payload)
        print("[WARN] collect_night_futures.py not fully configured.")
        return

    try:
        access_token = get_valid_kis_token()
        payload["token_status"] = "ready" if access_token else "missing"

        # KOSPI200
        kospi_js = request_market_snapshot(KOSPI_MARKET_CLASS, access_token)
        kospi_row = extract_best_output_row(kospi_js)
        kospi_point = normalize_snapshot(kospi_row, "KOSPI", snap_time)
        append_snapshot(payload, "kospi", kospi_point)

        # KOSDAQ150
        kosdaq_js = request_market_snapshot(KOSDAQ_MARKET_CLASS, access_token)
        kosdaq_row = extract_best_output_row(kosdaq_js)
        kosdaq_point = normalize_snapshot(kosdaq_row, "KOSDAQ", snap_time)
        append_snapshot(payload, "kosdaq", kosdaq_point)

        payload["note"] = "live snapshot updated"

    except Exception as e:
        payload["note"] = "snapshot update failed"
        print(f"[ERROR] {e}")

    save_payload(payload)
    print(f"[OK] night futures payload saved -> {LATEST_FILE}")


if __name__ == "__main__":
    main()
