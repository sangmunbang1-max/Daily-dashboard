# -*- coding: utf-8 -*-
import asyncio
import json
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import websockets

KST = timezone(timedelta(hours=9))

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "docs" / "data" / "night_futures"
LATEST_FILE = DATA_DIR / "latest.json"

TMP_DIR = BASE_DIR / "tmp"
DEBUG_DIR = TMP_DIR / "night_futures_ws"

APPROVAL_URL = os.environ.get(
    "KIS_APPROVAL_URL",
    "https://openapi.koreainvestment.com:9443/oauth2/Approval",
)
WS_URL = os.environ.get(
    "KIS_WS_URL",
    "ws://ops.koreainvestment.com:21000",
)

KIS_APP_KEY = os.environ.get("KIS_APP_KEY", "")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "")

# 문서 기준: KRX야간선물 실시간종목체결
WS_TR_ID = "H0MFCNT0"

# 반드시 실제 야간선물 종목코드로 넣어야 함
# 예시는 placeholder. 네 계정/마스터 기준 실제 코드로 GitHub Secrets에 넣는 걸 권장.
KOSPI_TR_KEY = os.environ.get("KIS_NIGHT_KOSPI_TR_KEY", "").strip()
KOSDAQ_TR_KEY = os.environ.get("KIS_NIGHT_KOSDAQ_TR_KEY", "").strip()  # 선택

# 수신 대기 시간
WS_WAIT_SECONDS = int(os.environ.get("KIS_WS_WAIT_SECONDS", "12"))

# 디버그 저장
DEBUG_RAW_FRAMES_FILE = DEBUG_DIR / "raw_frames.json"
DEBUG_LAST_MESSAGES_FILE = DEBUG_DIR / "last_messages.json"


FIELD_NAMES_H0MFCNT0 = [
    "FUTS_SHRN_ISCD",
    "BSOP_HOUR",
    "FUTS_PRDY_VRSS",
    "PRDY_VRSS_SIGN",
    "FUTS_PRDY_CTRT",
    "FUTS_PRPR",
    "FUTS_OPRC",
    "FUTS_HGPR",
    "FUTS_LWPR",
    "LAST_CNQN",
    "ACML_VOL",
    "ACML_TR_PBMN",
    "HTS_THPR",
    "MRKT_BASIS",
    "DPRT",
    "NMSC_FCTN_STPL_PRC",
    "FMSC_FCTN_STPL_PRC",
    "SPEAD_PRC",
    "HTS_OTST_STPL_QTY",
    "OTST_STPL_QTY_ICDC",
    "OPRC_HOUR",
    "OPRC_VRSS_PRPR_SIGN",
    "OPRC_VRSS_NMIX_PRPR",
    "HGPR_HOUR",
    "HGPR_VRSS_PRPR_SIGN",
    "HGPR_VRSS_NMIX_PRPR",
    "LWPR_HOUR",
    "LWPR_VRSS_PRPR_SIGN",
    "LWPR_VRSS_NMIX_PRPR",
    "SHNU_RATE",
    "CTTR",
    "ESDG",
    "OTST_STPL_RGBF_QTY_ICDC",
    "THPR_BASIS",
    "FUTS_ASKP1",
    "FUTS_BIDP1",
    "ASKP_RSQN1",
    "BIDP_RSQN1",
    "SELN_CNTG_CSNU",
    "SHNU_CNTG_CSNU",
    "NTBY_CNTG_CSNU",
    "SELN_CNTG_SMTN",
    "SHNU_CNTG_SMTN",
    "TOTAL_ASKP_RSQN",
    "TOTAL_BIDP_RSQN",
    "PRDY_VOL_VRSS_ACML_VOL_RATE",
    "DYNM_MXPR",
    "DYNM_LLAM",
    "DYNM_PRC_LIMT_YN",
]


def ensure_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)


def now_kst() -> datetime:
    return datetime.now(KST)


def kst_hhmm(dt: Optional[datetime] = None) -> str:
    dt = dt or now_kst()
    return dt.strftime("%H:%M")


def get_night_biz_date(dt: Optional[datetime] = None) -> str:
    dt = dt or now_kst()
    if dt.hour < 6:
        dt = dt - timedelta(days=1)
    return dt.strftime("%Y-%m-%d")


def is_night_session(dt: Optional[datetime] = None) -> bool:
    dt = dt or now_kst()
    return (dt.hour >= 18) or (dt.hour < 6)


def safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        if isinstance(v, float) and math.isnan(v):
            return None
        return float(v)
    s = str(v).strip().replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def save_json(path: Path, obj: Any) -> None:
    ensure_dir()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


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
        "open": None,
        "session_high": None,
        "session_low": None,
        "ask1": None,
        "bid1": None,
        "spread": None,
        "acml_vol": None,
    }


def make_empty_payload() -> Dict[str, Any]:
    now = now_kst()
    return {
        "generated_at_kst": now.isoformat(),
        "biz_date": get_night_biz_date(now),
        "session": "night",
        "source": "KIS_WEBSOCKET_H0MFCNT0",
        "note": "night futures websocket snapshot",
        "token_status": "missing",
        "series": make_empty_series(),
        "summary": {
            "kospi": make_empty_summary(),
            "kosdaq": make_empty_summary(),
        },
        "meta": {
            "mode": "snapshot",
            "interval_minutes": 30,
            "ws_tr_id": WS_TR_ID,
        },
    }


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


def reset_if_new_session_day_or_source_changed(payload: Dict[str, Any], biz_date: str) -> Dict[str, Any]:
    current_source = payload.get("source")
    target_source = "KIS_WEBSOCKET_H0MFCNT0"

    if payload.get("biz_date") != biz_date:
        new_payload = make_empty_payload()
        new_payload["biz_date"] = biz_date
        return new_payload

    if current_source != target_source:
        new_payload = make_empty_payload()
        new_payload["biz_date"] = biz_date
        return new_payload

    return payload


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


def calc_summary(points: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not points:
        return make_empty_summary()

    prices = [p.get("price") for p in points if p.get("price") is not None]
    last_point = points[-1]

    ask1 = last_point.get("ask1")
    bid1 = last_point.get("bid1")
    spread = None
    if ask1 is not None and bid1 is not None:
        spread = round(ask1 - bid1, 4)

    return {
        "last": last_point.get("price"),
        "change": last_point.get("change"),
        "change_pct": last_point.get("change_pct"),
        "high": max(prices) if prices else None,
        "low": min(prices) if prices else None,
        "points": len(points),
        "last_symbol": last_point.get("symbol"),
        "last_name": last_point.get("name"),
        "open": last_point.get("open"),
        "session_high": last_point.get("session_high"),
        "session_low": last_point.get("session_low"),
        "ask1": ask1,
        "bid1": bid1,
        "spread": spread,
        "acml_vol": last_point.get("acml_vol"),
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


def get_approval_key() -> str:
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        raise RuntimeError("KIS_APP_KEY / KIS_APP_SECRET missing")

    body = {
        "grant_type": "client_credentials",
        "appkey": KIS_APP_KEY,
        "secretkey": KIS_APP_SECRET,
    }
    resp = requests.post(
        APPROVAL_URL,
        headers={"content-type": "application/json; charset=utf-8"},
        data=json.dumps(body),
        timeout=20,
    )
    resp.raise_for_status()
    js = resp.json()

    approval_key = js.get("approval_key")
    if not approval_key:
        raise RuntimeError(f"approval_key not found: {js}")
    return approval_key


def build_subscribe_message(approval_key: str, tr_key: str) -> str:
    return json.dumps(
        {
            "header": {
                "approval_key": approval_key,
                "custtype": "P",
                "tr_type": "1",
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id": WS_TR_ID,
                    "tr_key": tr_key,
                }
            },
        }
    )


def parse_h0mfcnt0_body(body_text: str) -> Dict[str, Any]:
    values = body_text.split("^")
    result: Dict[str, Any] = {}
    for i, field_name in enumerate(FIELD_NAMES_H0MFCNT0):
        result[field_name] = values[i] if i < len(values) else ""
    return result


def parse_ws_frame(text: str) -> Tuple[Optional[str], Optional[str], Dict[str, Any]]:
    """
    반환값:
    - msg_type: data / ack / ping / json / unknown
    - tr_id
    - parsed dict
    """
    text = text.strip()
    if not text:
        return "unknown", None, {}

    # 일반 JSON ack/error
    if text.startswith("{"):
        try:
            js = json.loads(text)
        except Exception:
            return "json", None, {"raw": text}

        tr_id = None
        if isinstance(js, dict):
            header = js.get("header", {})
            body = js.get("body", {})
            tr_id = header.get("tr_id") or body.get("tr_id") or js.get("tr_id")
        return "ack", tr_id, js

    # 실시간 데이터: 보통 0|TR_ID|...|payload 형태
    parts = text.split("|")
    if len(parts) >= 4:
        msg_code = parts[0]
        tr_id = parts[1]
        payload = parts[-1]

        if tr_id == WS_TR_ID and msg_code in ("0", "1"):
            parsed = parse_h0mfcnt0_body(payload)
            return "data", tr_id, parsed

        return "unknown", tr_id, {"raw": text}

    return "unknown", None, {"raw": text}


def normalize_trade_snapshot(parsed: Dict[str, Any], label: str, snap_time: str, tr_key: str) -> Dict[str, Any]:
    price = safe_float(parsed.get("FUTS_PRPR"))
    change = safe_float(parsed.get("FUTS_PRDY_VRSS"))
    change_pct = safe_float(parsed.get("FUTS_PRDY_CTRT"))
    open_price = safe_float(parsed.get("FUTS_OPRC"))
    high_price = safe_float(parsed.get("FUTS_HGPR"))
    low_price = safe_float(parsed.get("FUTS_LWPR"))
    ask1 = safe_float(parsed.get("FUTS_ASKP1"))
    bid1 = safe_float(parsed.get("FUTS_BIDP1"))
    acml_vol = safe_float(parsed.get("ACML_VOL"))
    last_cnqn = safe_float(parsed.get("LAST_CNQN"))

    if price is None:
        raise RuntimeError(f"{label}: FUTS_PRPR missing in websocket message")

    symbol = parsed.get("FUTS_SHRN_ISCD") or tr_key
    bsop_hour = parsed.get("BSOP_HOUR", "")

    return {
        "time": snap_time,
        "bsop_hour": bsop_hour,
        "price": price,
        "change": change,
        "change_pct": change_pct,
        "symbol": symbol,
        "name": label,
        "open": open_price,
        "session_high": high_price,
        "session_low": low_price,
        "ask1": ask1,
        "bid1": bid1,
        "acml_vol": acml_vol,
        "last_cnqn": last_cnqn,
        "tr_key": tr_key,
        "raw_fields": {
            "FUTS_PRPR": parsed.get("FUTS_PRPR"),
            "FUTS_PRDY_VRSS": parsed.get("FUTS_PRDY_VRSS"),
            "FUTS_PRDY_CTRT": parsed.get("FUTS_PRDY_CTRT"),
            "FUTS_OPRC": parsed.get("FUTS_OPRC"),
            "FUTS_HGPR": parsed.get("FUTS_HGPR"),
            "FUTS_LWPR": parsed.get("FUTS_LWPR"),
            "FUTS_ASKP1": parsed.get("FUTS_ASKP1"),
            "FUTS_BIDP1": parsed.get("FUTS_BIDP1"),
            "ACML_VOL": parsed.get("ACML_VOL"),
            "LAST_CNQN": parsed.get("LAST_CNQN"),
        },
    }


async def receive_snapshots_once(
    targets: List[Tuple[str, str]],
    wait_seconds: int,
) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    approval_key = get_approval_key()

    latest_by_key: Dict[str, Dict[str, Any]] = {}
    raw_frames: List[Dict[str, Any]] = []
    parsed_messages: List[Dict[str, Any]] = []

    async with websockets.connect(
        WS_URL,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,
        max_size=2**22,
    ) as ws:
        for _, tr_key in targets:
            sub_msg = build_subscribe_message(approval_key, tr_key)
            await ws.send(sub_msg)

        deadline = asyncio.get_running_loop().time() + wait_seconds

        while asyncio.get_running_loop().time() < deadline:
            timeout_left = deadline - asyncio.get_running_loop().time()
            if timeout_left <= 0:
                break

            try:
                text = await asyncio.wait_for(ws.recv(), timeout=timeout_left)
            except asyncio.TimeoutError:
                break

            if isinstance(text, bytes):
                try:
                    text = text.decode("utf-8", errors="ignore")
                except Exception:
                    text = str(text)

            msg_type, tr_id, parsed = parse_ws_frame(text)

            raw_frames.append(
                {
                    "received_at_kst": now_kst().isoformat(),
                    "msg_type": msg_type,
                    "tr_id": tr_id,
                    "raw": text,
                }
            )

            if msg_type == "ack":
                parsed_messages.append(
                    {
                        "received_at_kst": now_kst().isoformat(),
                        "msg_type": "ack",
                        "tr_id": tr_id,
                        "parsed": parsed,
                    }
                )
                continue

            if msg_type == "data" and parsed:
                futs_shrn_iscd = (parsed.get("FUTS_SHRN_ISCD") or "").strip()
                parsed_messages.append(
                    {
                        "received_at_kst": now_kst().isoformat(),
                        "msg_type": "data",
                        "tr_id": tr_id,
                        "symbol": futs_shrn_iscd,
                        "parsed": parsed,
                    }
                )
                latest_by_key[futs_shrn_iscd] = parsed

    return latest_by_key, raw_frames, parsed_messages


def save_payload(payload: Dict[str, Any]) -> None:
    save_json(LATEST_FILE, payload)


def validate_config() -> None:
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        raise RuntimeError("KIS_APP_KEY / KIS_APP_SECRET missing")

    if not KOSPI_TR_KEY and not KOSDAQ_TR_KEY:
        raise RuntimeError("At least one of KIS_NIGHT_KOSPI_TR_KEY / KIS_NIGHT_KOSDAQ_TR_KEY is required")


def build_targets() -> List[Tuple[str, str]]:
    targets: List[Tuple[str, str]] = []
    if KOSPI_TR_KEY:
        targets.append(("kospi", KOSPI_TR_KEY))
    if KOSDAQ_TR_KEY:
        targets.append(("kosdaq", KOSDAQ_TR_KEY))
    return targets


def choose_latest_message_for_tr_key(
    parsed_messages: List[Dict[str, Any]],
    tr_key: str,
) -> Optional[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for item in parsed_messages:
        if item.get("msg_type") != "data":
            continue
        parsed = item.get("parsed", {})
        symbol = (parsed.get("FUTS_SHRN_ISCD") or "").strip()
        if symbol == tr_key:
            candidates.append(parsed)

    if not candidates:
        return None
    return candidates[-1]


def main() -> None:
    ensure_dir()
    now = now_kst()

    if not is_night_session(now):
        print("[SKIP] outside night session")
        return

    biz_date = get_night_biz_date(now)
    snap_time = kst_hhmm(now)

    payload = load_existing_payload()
    payload = reset_if_new_session_day_or_source_changed(payload, biz_date)
    
    payload["generated_at_kst"] = now.isoformat()
    payload["biz_date"] = biz_date
    payload["session"] = "night"
    payload["source"] = "KIS_WEBSOCKET_H0MFCNT0"
    payload["meta"] = {
        "mode": "snapshot",
        "interval_minutes": 30,
        "ws_tr_id": WS_TR_ID,
        "ws_url": WS_URL,
        "wait_seconds": WS_WAIT_SECONDS,
        "debug_files": {
            "raw_frames": str(DEBUG_RAW_FRAMES_FILE.relative_to(BASE_DIR)),
            "last_messages": str(DEBUG_LAST_MESSAGES_FILE.relative_to(BASE_DIR)),
        },
        "configured_tr_keys": {
            "kospi": KOSPI_TR_KEY if KOSPI_TR_KEY else None,
            "kosdaq": KOSDAQ_TR_KEY if KOSDAQ_TR_KEY else None,
        },
    }

    try:
        validate_config()
        payload["token_status"] = "approval_pending"

        targets = build_targets()
        latest_by_key, raw_frames, parsed_messages = asyncio.run(
            receive_snapshots_once(targets, WS_WAIT_SECONDS)
        )

        save_json(DEBUG_RAW_FRAMES_FILE, raw_frames)
        save_json(DEBUG_LAST_MESSAGES_FILE, parsed_messages)

        payload["token_status"] = "ready"

        market_to_trkey = {
            "kospi": KOSPI_TR_KEY,
            "kosdaq": KOSDAQ_TR_KEY,
        }
        market_to_label = {
            "kospi": "KOSPI Night Futures",
            "kosdaq": "KOSDAQ Night Futures",
        }

        got_any = False

        for market_key, tr_key in market_to_trkey.items():
            if not tr_key:
                continue

            parsed = latest_by_key.get(tr_key)
            if not parsed:
                parsed = choose_latest_message_for_tr_key(parsed_messages, tr_key)

            if not parsed:
                print(f"[WARN] no websocket trade message for {market_key} / tr_key={tr_key}")
                continue

            point = normalize_trade_snapshot(
                parsed=parsed,
                label=market_to_label[market_key],
                snap_time=snap_time,
                tr_key=tr_key,
            )
            append_snapshot(payload, market_key, point)
            got_any = True

        if got_any:
            payload["note"] = "live websocket snapshot updated"
        else:
            payload["note"] = "websocket connected but no matching trade message received; check tr_key"

    except Exception as e:
        payload["token_status"] = "error"
        payload["note"] = f"snapshot update failed: {e}"
        print(f"[ERROR] {e}")

    save_payload(payload)
    print(f"[OK] saved -> {LATEST_FILE}")
    print(f"[OK] debug -> {DEBUG_DIR}")


if __name__ == "__main__":
    main()
