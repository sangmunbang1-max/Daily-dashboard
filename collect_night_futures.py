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

DEFAULT_WS_URLS = [
    "ws://ops.koreainvestment.com:21000",
    "wss://ops.koreainvestment.com:21000",
]
WS_URLS = [
    x.strip()
    for x in os.environ.get("KIS_WS_URLS", ",".join(DEFAULT_WS_URLS)).split(",")
    if x.strip()
]

KIS_APP_KEY = os.environ.get("KIS_APP_KEY", "")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "")

TR_TRADE = "H0MFCNT0"  # 야간선물 실시간종목체결
TR_QUOTE = "H0MFASP0"  # 야간선물 실시간호가
TARGET_SOURCE = "KIS_WEBSOCKET_H0MFCNT0_H0MFASP0"

KOSPI_TR_KEY = os.environ.get("KIS_NIGHT_KOSPI_TR_KEY", "").strip()
KOSDAQ_TR_KEY = os.environ.get("KIS_NIGHT_KOSDAQ_TR_KEY", "").strip()

WS_WAIT_SECONDS = int(os.environ.get("KIS_WS_WAIT_SECONDS", "20"))

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

FIELD_NAMES_H0MFASP0 = [
    "FUTS_SHRN_ISCD",
    "BSOP_HOUR",
    "FUTS_ASKP1",
    "FUTS_ASKP2",
    "FUTS_ASKP3",
    "FUTS_ASKP4",
    "FUTS_ASKP5",
    "FUTS_BIDP1",
    "FUTS_BIDP2",
    "FUTS_BIDP3",
    "FUTS_BIDP4",
    "FUTS_BIDP5",
    "ASKP_CSNU1",
    "ASKP_CSNU2",
    "ASKP_CSNU3",
    "ASKP_CSNU4",
    "ASKP_CSNU5",
    "BIDP_CSNU1",
    "BIDP_CSNU2",
    "BIDP_CSNU3",
    "BIDP_CSNU4",
    "BIDP_CSNU5",
    "ASKP_RSQN1",
    "ASKP_RSQN2",
    "ASKP_RSQN3",
    "ASKP_RSQN4",
    "ASKP_RSQN5",
    "BIDP_RSQN1",
    "BIDP_RSQN2",
    "BIDP_RSQN3",
    "BIDP_RSQN4",
    "BIDP_RSQN5",
    "TOTAL_ASKP_CSNU",
    "TOTAL_BIDP_CSNU",
    "TOTAL_ASKP_RSQN",
    "TOTAL_BIDP_RSQN",
    "TOTAL_ASKP_RSQN_ICDC",
    "TOTAL_BIDP_RSQN_ICDC",
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
        "source_type": None,
    }


def make_empty_payload() -> Dict[str, Any]:
    now = now_kst()
    return {
        "generated_at_kst": now.isoformat(),
        "biz_date": get_night_biz_date(now),
        "session": "night",
        "source": TARGET_SOURCE,
        "note": "night futures websocket snapshot",
        "token_status": "missing",
        "series": make_empty_series(),
        "summary": {
            "kospi": make_empty_summary(),
            "kosdaq": make_empty_summary(),
        },
        "meta": {
            "mode": "snapshot",
            "interval_minutes": 15,
            "trade_tr_id": TR_TRADE,
            "quote_tr_id": TR_QUOTE,
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
    if payload.get("biz_date") != biz_date:
        new_payload = make_empty_payload()
        new_payload["biz_date"] = biz_date
        return new_payload

    if payload.get("source") != TARGET_SOURCE:
        new_payload = make_empty_payload()
        new_payload["biz_date"] = biz_date
        return new_payload

    return payload


def migrate_legacy_points(payload: Dict[str, Any]) -> Dict[str, Any]:
    series = payload.get("series", {})
    for market_key in ("kospi", "kosdaq"):
        points = series.get(market_key, [])
        cleaned = []
        for p in points:
            if not isinstance(p, dict):
                continue
            is_ws_point = (
                "tr_key" in p
                or "bsop_hour" in p
                or "raw_fields" in p
                or "source_type" in p
            )
            if is_ws_point:
                cleaned.append(p)
        series[market_key] = cleaned
    payload["series"] = series
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
        "source_type": last_point.get("source_type"),
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


def build_subscribe_message(approval_key: str, tr_id: str, tr_key: str) -> str:
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
                    "tr_id": tr_id,
                    "tr_key": tr_key,
                }
            },
        }
    )


def parse_pipe_payload(text: str) -> Tuple[str, str, str]:
    parts = text.split("|")
    if len(parts) < 4:
        raise ValueError("invalid pipe payload")
    return parts[0], parts[1], parts[-1]


def parse_fixed_fields(body_text: str, field_names: List[str]) -> Dict[str, Any]:
    values = body_text.split("^")
    result: Dict[str, Any] = {}
    for i, field_name in enumerate(field_names):
        result[field_name] = values[i] if i < len(values) else ""
    return result


def parse_ws_frame(text: str) -> Tuple[str, Optional[str], Dict[str, Any]]:
    text = text.strip()
    if not text:
        return "unknown", None, {}

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

    try:
        msg_code, tr_id, payload = parse_pipe_payload(text)
    except Exception:
        return "unknown", None, {"raw": text}

    if msg_code not in ("0", "1"):
        return "unknown", tr_id, {"raw": text}

    if tr_id == TR_TRADE:
        parsed = parse_fixed_fields(payload, FIELD_NAMES_H0MFCNT0)
        return "trade", tr_id, parsed

    if tr_id == TR_QUOTE:
        parsed = parse_fixed_fields(payload, FIELD_NAMES_H0MFASP0)
        return "quote", tr_id, parsed

    return "unknown", tr_id, {"raw": text}


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
        raise RuntimeError(f"{label}: FUTS_PRPR missing in trade message")

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
        "source_type": "trade",
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


def normalize_quote_snapshot(
    parsed: Dict[str, Any],
    label: str,
    snap_time: str,
    tr_key: str,
    previous_point: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    ask1 = safe_float(parsed.get("FUTS_ASKP1"))
    bid1 = safe_float(parsed.get("FUTS_BIDP1"))

    if ask1 is None and bid1 is None:
        return None

    if ask1 is not None and bid1 is not None:
        price = round((ask1 + bid1) / 2.0, 4)
    else:
        price = ask1 if ask1 is not None else bid1

    prev_change = previous_point.get("change") if previous_point else None
    prev_change_pct = previous_point.get("change_pct") if previous_point else None
    prev_open = previous_point.get("open") if previous_point else None
    prev_high = previous_point.get("session_high") if previous_point else None
    prev_low = previous_point.get("session_low") if previous_point else None
    prev_acml_vol = previous_point.get("acml_vol") if previous_point else None

    symbol = parsed.get("FUTS_SHRN_ISCD") or tr_key
    bsop_hour = parsed.get("BSOP_HOUR", "")

    return {
        "time": snap_time,
        "bsop_hour": bsop_hour,
        "price": price,
        "change": prev_change,
        "change_pct": prev_change_pct,
        "symbol": symbol,
        "name": label,
        "open": prev_open,
        "session_high": prev_high,
        "session_low": prev_low,
        "ask1": ask1,
        "bid1": bid1,
        "acml_vol": prev_acml_vol,
        "last_cnqn": None,
        "tr_key": tr_key,
        "source_type": "quote_fallback",
        "raw_fields": {
            "FUTS_ASKP1": parsed.get("FUTS_ASKP1"),
            "FUTS_BIDP1": parsed.get("FUTS_BIDP1"),
            "TOTAL_ASKP_RSQN": parsed.get("TOTAL_ASKP_RSQN"),
            "TOTAL_BIDP_RSQN": parsed.get("TOTAL_BIDP_RSQN"),
        },
    }


async def receive_snapshots_once(
    subscribe_items: List[Tuple[str, str, str]],
    wait_seconds: int,
) -> Tuple[Dict[str, Dict[str, Dict[str, Any]]], List[Dict[str, Any]], List[Dict[str, Any]], str]:
    approval_key = get_approval_key()
    last_error = None

    for ws_url in WS_URLS:
        latest_messages = {
            "trade": {},
            "quote": {},
        }
        raw_frames: List[Dict[str, Any]] = []
        parsed_messages: List[Dict[str, Any]] = []

        try:
            async with websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_size=2**22,
            ) as ws:
                for _, tr_id, tr_key in subscribe_items:
                    sub_msg = build_subscribe_message(approval_key, tr_id, tr_key)
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
                            "ws_url": ws_url,
                            "msg_type": msg_type,
                            "tr_id": tr_id,
                            "raw": text,
                        }
                    )

                    if msg_type == "ack":
                        parsed_messages.append(
                            {
                                "received_at_kst": now_kst().isoformat(),
                                "ws_url": ws_url,
                                "msg_type": "ack",
                                "tr_id": tr_id,
                                "parsed": parsed,
                            }
                        )
                        continue

                    if msg_type in ("trade", "quote") and parsed:
                        symbol = (parsed.get("FUTS_SHRN_ISCD") or "").strip()
                        parsed_messages.append(
                            {
                                "received_at_kst": now_kst().isoformat(),
                                "ws_url": ws_url,
                                "msg_type": msg_type,
                                "tr_id": tr_id,
                                "symbol": symbol,
                                "parsed": parsed,
                            }
                        )
                        if symbol:
                            latest_messages[msg_type][symbol] = parsed

            return latest_messages, raw_frames, parsed_messages, ws_url

        except Exception as e:
            last_error = f"{ws_url} -> {type(e).__name__}: {e}"
            continue

    raise RuntimeError(f"all websocket urls failed: {last_error}")


def save_payload(payload: Dict[str, Any]) -> None:
    save_json(LATEST_FILE, payload)


def validate_config() -> None:
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        raise RuntimeError("KIS_APP_KEY / KIS_APP_SECRET missing")
    if not KOSPI_TR_KEY and not KOSDAQ_TR_KEY:
        raise RuntimeError("At least one of KIS_NIGHT_KOSPI_TR_KEY / KIS_NIGHT_KOSDAQ_TR_KEY is required")


def build_subscribe_items() -> List[Tuple[str, str, str]]:
    items: List[Tuple[str, str, str]] = []

    if KOSPI_TR_KEY:
        items.append(("kospi", TR_TRADE, KOSPI_TR_KEY))

    if KOSDAQ_TR_KEY:
        items.append(("kosdaq", TR_TRADE, KOSDAQ_TR_KEY))
        items.append(("kosdaq", TR_QUOTE, KOSDAQ_TR_KEY))

    return items


def choose_latest_message_for_symbol(
    parsed_messages: List[Dict[str, Any]],
    msg_type: str,
    symbol: str,
) -> Optional[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for item in parsed_messages:
        if item.get("msg_type") != msg_type:
            continue
        parsed = item.get("parsed", {})
        recv_symbol = (parsed.get("FUTS_SHRN_ISCD") or "").strip()
        if recv_symbol == symbol:
            candidates.append(parsed)

    if not candidates:
        return None
    return candidates[-1]


def get_last_point(payload: Dict[str, Any], market_key: str) -> Optional[Dict[str, Any]]:
    points = payload.get("series", {}).get(market_key, [])
    if not points:
        return None
    return points[-1]


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
    payload = migrate_legacy_points(payload)

    payload["generated_at_kst"] = now.isoformat()
    payload["biz_date"] = biz_date
    payload["session"] = "night"
    payload["source"] = TARGET_SOURCE
    payload["meta"] = {
        "mode": "snapshot",
        "interval_minutes": 15,
        "trade_tr_id": TR_TRADE,
        "quote_tr_id": TR_QUOTE,
        "ws_urls_tried": WS_URLS,
        "connected_ws_url": None,
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

        subscribe_items = build_subscribe_items()
        latest_messages, raw_frames, parsed_messages, connected_ws_url = asyncio.run(
            receive_snapshots_once(subscribe_items, WS_WAIT_SECONDS)
        )

        save_json(DEBUG_RAW_FRAMES_FILE, raw_frames)
        save_json(DEBUG_LAST_MESSAGES_FILE, parsed_messages)

        payload["token_status"] = "ready"
        payload["meta"]["connected_ws_url"] = connected_ws_url

        payload["meta"]["received_symbols"] = {
            "trade": sorted(list(latest_messages["trade"].keys())),
            "quote": sorted(list(latest_messages["quote"].keys())),
        }

        market_to_trkey = {
            "kospi": KOSPI_TR_KEY,
            "kosdaq": KOSDAQ_TR_KEY,
        }
        market_to_label = {
            "kospi": "KOSPI Night Futures",
            "kosdaq": "KOSDAQ Night Futures",
        }

        market_status: Dict[str, str] = {}

        for market_key, tr_key in market_to_trkey.items():
            if not tr_key:
                market_status[market_key] = "tr_key_missing"
                continue

            trade_parsed = latest_messages["trade"].get(tr_key)
            if not trade_parsed:
                trade_parsed = choose_latest_message_for_symbol(parsed_messages, "trade", tr_key)

            quote_parsed = latest_messages["quote"].get(tr_key)
            if not quote_parsed:
                quote_parsed = choose_latest_message_for_symbol(parsed_messages, "quote", tr_key)

            point = None

            if trade_parsed:
                point = normalize_trade_snapshot(
                    parsed=trade_parsed,
                    label=market_to_label[market_key],
                    snap_time=snap_time,
                    tr_key=tr_key,
                )
                append_snapshot(payload, market_key, point)
                market_status[market_key] = "trade_ok"
                continue

            if quote_parsed:
                prev_point = get_last_point(payload, market_key)
                point = normalize_quote_snapshot(
                    parsed=quote_parsed,
                    label=market_to_label[market_key],
                    snap_time=snap_time,
                    tr_key=tr_key,
                    previous_point=prev_point,
                )
                if point:
                    append_snapshot(payload, market_key, point)
                    market_status[market_key] = "quote_fallback_ok"
                    continue

            market_status[market_key] = f"no_trade_or_quote_for_{tr_key}"
            print(f"[WARN] no trade/quote message for {market_key} / tr_key={tr_key}")

        payload["meta"]["market_status"] = market_status
        payload["note"] = f"market_status={market_status}"

        for market_key in ("kospi", "kosdaq"):
            points = payload.get("series", {}).get(market_key, [])
            payload.setdefault("summary", {})
            payload["summary"][market_key] = calc_summary(points)

    except Exception as e:
        payload["token_status"] = "error"
        payload["note"] = f"snapshot update failed: {type(e).__name__}: {e}"
        print(f"[ERROR] {type(e).__name__}: {e}")

    save_payload(payload)
    print(f"[OK] saved -> {LATEST_FILE}")
    print(f"[OK] debug -> {DEBUG_DIR}")


if __name__ == "__main__":
    main()
