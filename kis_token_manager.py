# -*- coding: utf-8 -*-
import os
import json
from datetime import datetime, timedelta, timezone

import requests

KST = timezone(timedelta(hours=9))

KIS_APP_KEY = os.environ.get("KIS_APP_KEY", "")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "")

# 나중에 GitHub Actions / Secrets / Variables 쪽에서 주입할 수 있도록 미리 열어둠
ENV_ACCESS_TOKEN = os.environ.get("KIS_ACCESS_TOKEN", "")
ENV_ACCESS_TOKEN_EXPIRES_AT = os.environ.get("KIS_ACCESS_TOKEN_EXPIRES_AT_KST", "")

TOKEN_CACHE_FILE = "tmp/kis_token_cache.json"
TOKEN_URL = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"


def _ensure_dir(path: str):
    dirpath = os.path.dirname(path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)


def _load_env_token_cache():
    if ENV_ACCESS_TOKEN and ENV_ACCESS_TOKEN_EXPIRES_AT:
        return {
            "access_token": ENV_ACCESS_TOKEN,
            "issued_at_kst": "",
            "expires_at_kst": ENV_ACCESS_TOKEN_EXPIRES_AT,
            "source": "env",
        }
    return {}


def _load_token_cache():
    try:
        if os.path.exists(TOKEN_CACHE_FILE):
            with open(TOKEN_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_token_cache(data: dict):
    _ensure_dir(TOKEN_CACHE_FILE)
    with open(TOKEN_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _is_token_valid(cache: dict, now=None, safety_minutes: int = 30) -> bool:
    now = now or datetime.now(KST)

    access_token = cache.get("access_token", "")
    expires_at = cache.get("expires_at_kst", "")

    if not access_token or not expires_at:
        return False

    try:
        exp_dt = datetime.fromisoformat(expires_at).replace(tzinfo=KST)
    except Exception:
        return False

    return now < (exp_dt - timedelta(minutes=safety_minutes))


def _request_new_token() -> dict:
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        raise RuntimeError("KIS_APP_KEY / KIS_APP_SECRET 환경변수가 없습니다.")

    resp = requests.post(
        TOKEN_URL,
        headers={"content-type": "application/json"},
        json={
            "grant_type": "client_credentials",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
        },
        timeout=15,
    )
    resp.raise_for_status()
    js = resp.json()

    access_token = js.get("access_token", "")
    if not access_token:
        raise RuntimeError(f"토큰 응답 이상: {js}")

    # KIS 일반 REST 기준 1일 유효 전제
    now = datetime.now(KST)
    expires_at = now + timedelta(hours=24)

    cache = {
        "access_token": access_token,
        "issued_at_kst": now.isoformat(),
        "expires_at_kst": expires_at.isoformat(),
        "source": "request",
    }
    _save_token_cache(cache)
    return cache


def get_valid_kis_token(force_refresh: bool = False) -> str:
    if force_refresh:
        cache = _request_new_token()
        return cache["access_token"]

    # 1순위: 환경변수에서 받은 토큰
    env_cache = _load_env_token_cache()
    if _is_token_valid(env_cache):
        return env_cache["access_token"]

    # 2순위: 로컬 캐시 파일
    file_cache = _load_token_cache()
    if _is_token_valid(file_cache):
        return file_cache["access_token"]

    # 3순위: 새 발급
    cache = _request_new_token()
    return cache["access_token"]
