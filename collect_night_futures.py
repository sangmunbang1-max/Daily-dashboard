# -*- coding: utf-8 -*-
import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from kis_token_manager import get_valid_kis_token

KST = timezone(timedelta(hours=9))

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "docs" / "data" / "night_futures"
LATEST_FILE = DATA_DIR / "latest.json"

TMP_DIR = BASE_DIR / "tmp"
RAW_KOSPI_FILE = TMP_DIR / "night_futures_raw_kospi.json"
RAW_KOSDAQ_FILE = TMP_DIR / "night_futures_raw_kosdaq.json"
DEBUG_KOSPI_FILE = TMP_DIR / "night_futures_debug_kospi.json"
DEBUG_KOSDAQ_FILE = TMP_DIR / "night_futures_debug_kosdaq.json"

KIS_FO_QUOTE_URL = os.environ.get(
    "KIS_FO_QUOTE_URL",
    "https://openapi.koreainvestment.com:9443/uapi/domestic-futureoption/v1/quotations/display-board-futures",
)

KIS_APP_KEY = os.environ.get("KIS_APP_KEY", "")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "")

TR_ID = "FHPIF05030200"
MRKT_DIV_CODE = "F"
SCR_DIV_CODE = "20503"

# 스펙 기준
# ""   : KOSPI200
# KQI  : KOSDAQ150
KOSPI_MARKET_CLASS = ""
KOSDAQ_MARKET_CLASS = "KQI"

# 디버그 저장 여부
DEBUG_SAVE_RAW = True


def ensure_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)


def now_kst() -> datetime:
    return datetime.now(KST)


def kst_hhmm(dt: Optional[datetime] = None) -> str:
    dt = dt or now_kst()
    return dt.strftime("%H:%M")


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


def save_json(path: Path, obj: Any) -> None:
    ensure_dir()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def save_payload(payload: Dict[str, Any]) -> None:
    save_json(LATEST_FILE, payload)


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
        "last_symbol": None,
        "last_name": None,
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


def safe_int(v: Any) -> Optional[int]:
    fv = safe_float(v)
    if fv is None:
        return None
    try:
        return int(fv)
    except Exception:
        return None


def get_first_nonempty(row: Dict[str, Any], keys: List[str]) -> Any:
    for key in keys:
        if key in row:
            val = row.get(key)
            if val is None:
                continue
            if isinstance(val, str) and val.strip() == "":
                continue
            return val
    return None


def pick_float_from_keys(row: Dict[str, Any], keys: List[str]) -> Tuple[Optional[float], Optional[str]]:
    for key in keys:
        if key in row:
            val = safe_float(row.get(key))
            if val is not None:
                return val, key
    return None, None


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
        "last_symbol": last_point.get("symbol"),
        "last_name": last_point.get("name"),
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
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()


def get_output_rows(js: Dict[str, Any]) -> List[Dict[str, Any]]:
    rt_cd = str(js.get("rt_cd", ""))
    if rt_cd != "0":
        raise RuntimeError(f"KIS 응답 오류: rt_cd={js.get('rt_cd')} msg={js.get('msg1')}")

    output = js.get("output")
    if output is None:
        output = js.get("output1")

    if isinstance(output, dict):
        return [output]

    if isinstance(output, list):
        rows = [row for row in output if isinstance(row, dict)]
        if rows:
            return rows

    raise RuntimeError(f"output 형식 이상 또는 비어있음: {js}")


def infer_symbol(row: Dict[str, Any]) -> Optional[str]:
    return get_first_nonempty(
        row,
        [
            "futs_shrn_iscd",
            "pdno",
            "mksc_shrn_iscd",
            "iscd",
            "hts_kor_isnm_code",
            "symbol",
            "code",
        ],
    )


def infer_name(row: Dict[str, Any]) -> Optional[str]:
    return get_first_nonempty(
        row,
        [
            "hts_kor_isnm",
            "prdt_name",
            "prdt_abrv_name",
            "korean_name",
            "name",
            "isu_nm",
        ],
    )


def infer_rmnn_days(row: Dict[str, Any]) -> Optional[int]:
    for key in ["hts_rmnn_dynu", "rmnn_dynu", "remn_days", "rest_days"]:
        val = safe_int(row.get(key))
        if val is not None:
            return val
    return None


def infer_volume(row: Dict[str, Any]) -> Optional[float]:
    for key in ["acml_vol", "cum_vol", "vol", "tr_volume"]:
        val = safe_float(row.get(key))
        if val is not None:
            return val
    return None


def score_row(row: Dict[str, Any]) -> Tuple[int, int, float]:
    """
    낮을수록 우선
    1) 잔존일수 최소
    2) 거래량 최대
    3) 가격 존재 row 우선
    """
    rmnn = infer_rmnn_days(row)
    vol = infer_volume(row)

    price, _ = pick_float_from_keys(
        row,
        [
            "futs_prpr",
            "stck_prpr",
            "cur_prc",
            "last",
            "close",
            "price",
        ],
    )

    rmnn_val = rmnn if rmnn is not None else 999999
    vol_val = vol if vol is not None else -1.0
    price_penalty = 0 if price is not None else 1

    return (rmnn_val, price_penalty, -vol_val)


def extract_best_output_row(js: Dict[str, Any], market_label: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    rows = get_output_rows(js)
    rows_sorted = sorted(rows, key=score_row)
    best = rows_sorted[0]

    # 디버그용 상위 몇 개 후보 저장
    preview = []
    for row in rows_sorted[:5]:
        preview.append({
            "symbol": infer_symbol(row),
            "name": infer_name(row),
            "rmnn_days": infer_rmnn_days(row),
            "volume": infer_volume(row),
            "price_candidates": {
                "futs_prpr": row.get("futs_prpr"),
                "stck_prpr": row.get("stck_prpr"),
                "cur_prc": row.get("cur_prc"),
                "last": row.get("last"),
                "close": row.get("close"),
                "price": row.get("price"),
            },
            "change_candidates": {
                "futs_prdy_vrss": row.get("futs_prdy_vrss"),
                "prdy_vrss": row.get("prdy_vrss"),
                "change": row.get("change"),
            },
            "change_pct_candidates": {
                "futs_prdy_ctrt": row.get("futs_prdy_ctrt"),
                "prdy_ctrt": row.get("prdy_ctrt"),
                "change_pct": row.get("change_pct"),
            },
        })

    return best, preview


def normalize_snapshot(row: Dict[str, Any], label: str, snap_time: str) -> Dict[str, Any]:
    price, price_key = pick_float_from_keys(
        row,
        ["futs_prpr", "stck_prpr", "cur_prc", "last", "close", "price"],
    )
    change, change_key = pick_float_from_keys(
        row,
        ["futs_prdy_vrss", "prdy_vrss", "change"],
    )
    change_pct, change_pct_key = pick_float_from_keys(
        row,
        ["futs_prdy_ctrt", "prdy_ctrt", "change_pct"],
    )
    base_price, base_price_key = pick_float_from_keys(
        row,
        ["futs_sdpr", "stck_sdpr", "base_price", "base_pric", "bfdy_clpr"],
    )

    symbol = infer_symbol(row)
    name = infer_name(row)
    rmnn_days = infer_rmnn_days(row)
    volume = infer_volume(row)

    if price is None:
        raise RuntimeError(f"{label}: 현재가 후보 필드에서 가격을 찾지 못함.")

    return {
        "time": snap_time,
        "price": price,
        "change": change,
        "change_pct": change_pct,
        "symbol": symbol,
        "name": name,
        "rmnn_days": rmnn_days,
        "volume": volume,
        "base_price": base_price,
        "field_map": {
            "price": price_key,
            "change": change_key,
            "change_pct": change_pct_key,
            "base_price": base_price_key,
        },
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


def save_debug_bundle(
    raw_path: Path,
    debug_path: Path,
    market_label: str,
    js: Dict[str, Any],
    selected_row: Dict[str, Any],
    preview_rows: List[Dict[str, Any]],
    point: Dict[str, Any],
) -> None:
    if DEBUG_SAVE_RAW:
        save_json(raw_path, js)

    debug_obj = {
        "market": market_label,
        "saved_at_kst": now_kst().isoformat(),
        "url": KIS_FO_QUOTE_URL,
        "tr_id": TR_ID,
        "selected_summary": {
            "symbol": point.get("symbol"),
            "name": point.get("name"),
            "price": point.get("price"),
            "change": point.get("change"),
            "change_pct": point.get("change_pct"),
            "base_price": point.get("base_price"),
            "rmnn_days": point.get("rmnn_days"),
            "volume": point.get("volume"),
            "field_map": point.get("field_map"),
        },
        "top_candidates": preview_rows,
        "selected_row_full": selected_row,
    }
    save_json(debug_path, debug_obj)


def update_one_market(
    payload: Dict[str, Any],
    market_key: str,
    market_label: str,
    market_class_code: str,
    access_token: str,
    snap_time: str,
    raw_file: Path,
    debug_file: Path,
) -> None:
    js = request_market_snapshot(market_class_code, access_token)
    row, preview_rows = extract_best_output_row(js, market_label)
    point = normalize_snapshot(row, market_label, snap_time)
    append_snapshot(payload, market_key, point)
    save_debug_bundle(raw_file, debug_file, market_label, js, row, preview_rows, point)


def main() -> None:
    ensure_dir()
    now = now_kst()

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
    payload["meta"]["debug_files"] = {
        "raw_kospi": str(RAW_KOSPI_FILE.relative_to(BASE_DIR)),
        "raw_kosdaq": str(RAW_KOSDAQ_FILE.relative_to(BASE_DIR)),
        "debug_kospi": str(DEBUG_KOSPI_FILE.relative_to(BASE_DIR)),
        "debug_kosdaq": str(DEBUG_KOSDAQ_FILE.relative_to(BASE_DIR)),
    }

    if not is_configured():
        payload["token_status"] = "config_missing"
        payload["note"] = "set KIS_FO_QUOTE_URL and KIS credentials"
        save_payload(payload)
        print("[WARN] collect_night_futures.py not fully configured.")
        return

    try:
        access_token = get_valid_kis_token()
        payload["token_status"] = "ready" if access_token else "missing"

        update_one_market(
            payload=payload,
            market_key="kospi",
            market_label="KOSPI",
            market_class_code=KOSPI_MARKET_CLASS,
            access_token=access_token,
            snap_time=snap_time,
            raw_file=RAW_KOSPI_FILE,
            debug_file=DEBUG_KOSPI_FILE,
        )

        update_one_market(
            payload=payload,
            market_key="kosdaq",
            market_label="KOSDAQ",
            market_class_code=KOSDAQ_MARKET_CLASS,
            access_token=access_token,
            snap_time=snap_time,
            raw_file=RAW_KOSDAQ_FILE,
            debug_file=DEBUG_KOSDAQ_FILE,
        )

        payload["note"] = "live snapshot updated"

    except Exception as e:
        payload["note"] = f"snapshot update failed: {e}"
        print(f"[ERROR] {e}")

    save_payload(payload)
    print(f"[OK] night futures payload saved -> {LATEST_FILE}")
    print(f"[OK] debug files saved under -> {TMP_DIR}")


if __name__ == "__main__":
    main()
