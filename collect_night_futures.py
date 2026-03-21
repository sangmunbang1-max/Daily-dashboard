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

# KOSPI200 = 공백
# KOSDAQ150 = KQI
KOSPI_MARKET_CLASS = ""
KOSDAQ_MARKET_CLASS = "KQI"


def ensure_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def now_kst() -> datetime:
    return datetime.now(KST)


def kst_hhmm(dt: Optional[datetime] = None) -> str:
    dt = dt or now_kst()
    return dt.strftime("%H:%M")


def load_existing_payload() -> Dict[str, Any]:
    if LATEST_FILE.exists():
        try:
            with open(LATEST_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
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
        "biz_date": now.strftime("%Y-%m-%d"),
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
            "tr_id": TR_ID,
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

    prices = [p["price"] for p in points if p.get("price") is not None]
    highs = [p["high"] for p in points if p.get("high") is not None]
    lows = [p["low"] for p in points if p.get("low") is not None]
    last_point = points[-1] if points else {}

    return {
        "last": last_point.get("price"),
        "change": last_point.get("change"),
        "change_pct": last_point.get("change_pct"),
        "high": max(highs) if highs else (max(prices) if prices else None),
        "low": min(lows) if lows else (min(prices) if prices else None),
        "points": len(points),
    }


def upsert_point(points: List[Dict[str, Any]], new_point: Dict[str, Any]) -> List[Dict[str, Any]]:
    t = new_point["time"]
    replaced = False
    out: List[Dict[str, Any]] = []

    for p in points:
        if p.get("time") == t:
            out.append(new_point)
            replaced = True
        else:
            out.append(p)

    if not replaced:
        out.append(new_point)

    out.sort(key=lambda x: x.get("time", ""))
    return out


def is_configured() -> bool:
    bad = {""}
    return (
        KIS_FO_QUOTE_URL not in bad
        and KIS_APP_KEY not in bad
        and KIS_APP_SECRET not in bad
    )


def request_market_snapshot(market_class_code: str, access_token: str) -> Dict[str, Any]:
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {access_token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": TR_ID,
        "custtype": "P",
    }

    params = {
        "FID_COND_MRKT_DIV_CODE": MRKT_DIV_CODE,
        "FID_COND_SCR_DIV_CODE": SCR_DIV_CODE,
        "FID_COND_MRKT_CLS_CODE": market_class_code,
    }

    resp = requests.get(
        KIS_FO_QUOTE_URL,
        headers=headers,
        params=params,
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def extract_first_output_row(js: Dict[str, Any]) -> Dict[str, Any]:
    rt_cd = str(js.get("rt_cd", ""))
    if rt_cd != "0":
        raise RuntimeError(f"KIS 응답 오류: rt_cd={js.get('rt_cd')} msg={js.get('msg1')}")

    output1 = js.get("output1", [])
    if not isinstance(output1, list) or not output1:
        raise RuntimeError(f"output1 비어있음: {js}")

    return output1[0]


def normalize_snapshot(row: Dict[str, Any], label: str, snap_time: str) -> Dict[str, Any]:
    price = safe_float(row.get("futs_prpr"))
    change = safe_float(row.get("futs_prdy_vrss"))
    change_pct = safe_float(row.get("futs_prdy_ctrt"))
    high = safe_float(row.get("futs_hgpr"))
    low = safe_float(row.get("futs_lwpr"))

    if price is None:
        raise RuntimeError(f"{label}: futs_prpr 현재가 없음. row={row}")

    return {
        "time": snap_time,
        "price": price,
        "change": change,
        "change_pct": change_pct,
        "high": high,
        "low": low,
    }


def append_snapshot(payload: Dict[str, Any], key: str, point: Dict[str, Any]) -> None:
    series = payload.setdefault("series", make_empty_series())
    current = series.get(key, [])
    series[key] = upsert_point(current, point)

    summary = payload.setdefault(
        "summary",
        {"kospi": make_empty_summary(), "kosdaq": make_empty_summary()},
    )
    summary[key] = calc_summary(series[key])


def main() -> None:
    payload = load_existing_payload()
    now = now_kst()

    payload["generated_at_kst"] = now.isoformat()
    payload["biz_date"] = now.strftime("%Y-%m-%d")
    payload["session"] = "night"
    payload["source"] = "KIS"

    if not is_configured():
        payload["token_status"] = "config_missing"
        payload["note"] = "set KIS_FO_QUOTE_URL and KIS credentials"
        save_payload(payload)
        print("[WARN] collect_night_futures.py not fully configured.")
        return

    access_token = get_valid_kis_token()
    payload["token_status"] = "ready" if access_token else "missing"

    snap_time = kst_hhmm(now)

    # KOSPI200 야간선물 스냅샷
    kospi_js = request_market_snapshot(KOSPI_MARKET_CLASS, access_token)
    kospi_row = extract_first_output_row(kospi_js)
    kospi_point = normalize_snapshot(kospi_row, "KOSPI", snap_time)
    append_snapshot(payload, "kospi", kospi_point)

    # KOSDAQ150 야간선물 스냅샷
    kosdaq_js = request_market_snapshot(KOSDAQ_MARKET_CLASS, access_token)
    kosdaq_row = extract_first_output_row(kosdaq_js)
    kosdaq_point = normalize_snapshot(kosdaq_row, "KOSDAQ", snap_time)
    append_snapshot(payload, "kosdaq", kosdaq_point)

    payload["note"] = "live snapshot updated"
    save_payload(payload)
    print(f"[OK] night futures snapshot saved -> {LATEST_FILE}")


if __name__ == "__main__":
    main()
