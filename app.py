"""
local-llm-watchdog: queue-backed execution layer for exo.

Responsibilities:
- monitor the distributed exo deployment and expose state/model APIs
- act as the only ingress for local-LLM-bound work
- serialize upstream exo requests through a persistent FIFO queue
- apply prompt/context budgets per workload profile
- recover from backend failures via instance repair and remote host restarts
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

EXO_ENDPOINTS_RAW = os.getenv(
    "EXO_ENDPOINTS",
    "http://192.168.20.1:52415,http://192.168.20.2:52415,http://192.168.20.3:52415",
)
EXO_CONTROL_ENDPOINT = os.getenv("EXO_CONTROL_ENDPOINT", "").strip()
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "15"))
RESTART_COOLDOWN = int(os.getenv("RESTART_COOLDOWN_SECONDS", "120"))
AUTO_RESTART = os.getenv("AUTO_RESTART", "true").lower() in {"1", "true", "yes"}
EXO_DESIRED_MODEL = os.getenv(
    "EXO_DESIRED_MODEL",
    "mlx-community/Qwen3-30B-A3B-4bit",
)
EXO_SHARDING = os.getenv("EXO_SHARDING", "Pipeline")
EXO_INSTANCE_META = os.getenv("EXO_INSTANCE_META", "MlxRing")
EXO_MIN_NODES = int(os.getenv("EXO_MIN_NODES", "3"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

WATCHDOG_DB_PATH = Path(os.getenv("WATCHDOG_DB_PATH", "/data/watchdog.db"))
WATCHDOG_MAX_PROMPT_CHARS = int(os.getenv("WATCHDOG_MAX_PROMPT_CHARS", "9000"))
WATCHDOG_MAX_GEO_CONTEXT_CHARS = int(os.getenv("WATCHDOG_MAX_GEO_CONTEXT_CHARS", "1200"))
WATCHDOG_MAX_ARTICLE_CHARS = int(os.getenv("WATCHDOG_MAX_ARTICLE_CHARS", "900"))
WATCHDOG_NEWS_MIN_CHARS = int(os.getenv("WATCHDOG_NEWS_MIN_CHARS", "900"))
WATCHDOG_NEWS_MAX_TOKENS = int(os.getenv("WATCHDOG_NEWS_MAX_TOKENS", "96"))
WATCHDOG_MINIMAX_MIN_OUTPUT_TOKENS = int(
    os.getenv("WATCHDOG_MINIMAX_MIN_OUTPUT_TOKENS", "512")
)
WATCHDOG_HOURLY_MAX_ITEMS = int(os.getenv("WATCHDOG_HOURLY_MAX_ITEMS", "8"))
WATCHDOG_HOURLY_REDUCE_MAX_CHARS = int(os.getenv("WATCHDOG_HOURLY_REDUCE_MAX_CHARS", "5000"))
WATCHDOG_HOURLY_REDUCE_BATCH_SIZE = int(os.getenv("WATCHDOG_HOURLY_REDUCE_BATCH_SIZE", "4"))
WATCHDOG_HOURLY_SINGLE_PASS_MAX_ITEMS = int(
    os.getenv("WATCHDOG_HOURLY_SINGLE_PASS_MAX_ITEMS", "4")
)
WATCHDOG_HOURLY_SINGLE_PASS_MAX_CHARS = int(
    os.getenv("WATCHDOG_HOURLY_SINGLE_PASS_MAX_CHARS", "1800")
)
WATCHDOG_JOB_STALL_SECONDS = int(os.getenv("WATCHDOG_JOB_STALL_SECONDS", "600"))
WATCHDOG_HOURLY_PREFLIGHT_TIMEOUT_SECONDS = int(
    os.getenv("WATCHDOG_HOURLY_PREFLIGHT_TIMEOUT_SECONDS", "600")
)
WATCHDOG_CLUSTER_FORM_TIMEOUT_SECONDS = int(
    os.getenv("WATCHDOG_CLUSTER_FORM_TIMEOUT_SECONDS", "600")
)
WATCHDOG_REMOTE_RESTART_GRACE_SECONDS = int(
    os.getenv("WATCHDOG_REMOTE_RESTART_GRACE_SECONDS", "45")
)
WATCHDOG_SSH_CONFIG = Path(os.getenv("WATCHDOG_SSH_CONFIG", "/run/watchdog-ssh/config"))
WATCHDOG_SSH_KNOWN_HOSTS = Path(
    os.getenv("WATCHDOG_SSH_KNOWN_HOSTS", "/run/watchdog-ssh/known_hosts")
)
WATCHDOG_SSH_IDENTITY_FILE = Path(
    os.getenv("WATCHDOG_SSH_IDENTITY_FILE", "/run/watchdog-secrets/id_exo")
)
WATCHDOG_WAIT_CHUNK_SECONDS = int(os.getenv("WATCHDOG_WAIT_CHUNK_SECONDS", "25"))
WATCHDOG_ACTIVE_JOB_RECOVERY_GRACE_SECONDS = int(
    os.getenv("WATCHDOG_ACTIVE_JOB_RECOVERY_GRACE_SECONDS", "90")
)
WATCHDOG_TCP_PROBE_TIMEOUT_SECONDS = float(
    os.getenv("WATCHDOG_TCP_PROBE_TIMEOUT_SECONDS", "3")
)
WATCHDOG_HTTP_TIMEOUT_SECONDS = float(
    os.getenv("WATCHDOG_HTTP_TIMEOUT_SECONDS", "10")
)
WATCHDOG_PROBE_TIMEOUT_SECONDS = float(
    os.getenv("WATCHDOG_PROBE_TIMEOUT_SECONDS", "90")
)
WATCHDOG_NODE_STALE_SECONDS = int(os.getenv("WATCHDOG_NODE_STALE_SECONDS", "45"))
WATCHDOG_DAILY_RESTART_ENABLED = os.getenv(
    "WATCHDOG_DAILY_RESTART_ENABLED", "false"
).lower() in {"1", "true", "yes"}
WATCHDOG_DAILY_RESTART_HOUR = int(os.getenv("WATCHDOG_DAILY_RESTART_HOUR", "4"))
WATCHDOG_DAILY_RESTART_MINUTE = int(os.getenv("WATCHDOG_DAILY_RESTART_MINUTE", "15"))
WATCHDOG_DAILY_RESTART_TZ_NAME = os.getenv(
    "WATCHDOG_DAILY_RESTART_TZ", "America/Los_Angeles"
).strip() or "America/Los_Angeles"
WATCHDOG_DAILY_RESTART_CHECK_SECONDS = int(
    os.getenv("WATCHDOG_DAILY_RESTART_CHECK_SECONDS", "30")
)
WATCHDOG_PROVIDER_SECRET_NAME = os.getenv(
    "WATCHDOG_PROVIDER_SECRET_NAME", "exo-watchdog-llm-provider-secrets"
).strip()
WATCHDOG_NAMESPACE = os.getenv("WATCHDOG_NAMESPACE", "").strip()
WATCHDOG_K8S_TOKEN_PATH = Path(
    os.getenv(
        "WATCHDOG_K8S_TOKEN_PATH",
        "/var/run/secrets/kubernetes.io/serviceaccount/token",
    )
)
WATCHDOG_K8S_NAMESPACE_PATH = Path(
    os.getenv(
        "WATCHDOG_K8S_NAMESPACE_PATH",
        "/var/run/secrets/kubernetes.io/serviceaccount/namespace",
    )
)
WATCHDOG_K8S_CA_PATH = Path(
    os.getenv(
        "WATCHDOG_K8S_CA_PATH",
        "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
    )
)
WATCHDOG_K8S_API_TIMEOUT_SECONDS = float(
    os.getenv("WATCHDOG_K8S_API_TIMEOUT_SECONDS", "5")
)

try:
    WATCHDOG_DAILY_RESTART_TZ = ZoneInfo(WATCHDOG_DAILY_RESTART_TZ_NAME)
except Exception:  # noqa: BLE001
    WATCHDOG_DAILY_RESTART_TZ = timezone.utc
    WATCHDOG_DAILY_RESTART_TZ_NAME = "UTC"

DEFAULT_REMOTE_HOSTS_JSON = json.dumps(
    [
        {
            "name": "exo-126",
            "ssh_target": "magi1@10.0.0.126",
            "health_host": "192.168.20.1",
            "restart_cmd": "pkill -f exo_daemon 2>/dev/null; "
            "pkill -f '/exo/.venv/bin/exo' 2>/dev/null; "
            "sleep 2; nohup ~/exo/exo_daemon.sh > /dev/null 2>&1 & "
            "disown",
        },
        {
            "name": "exo-127",
            "ssh_target": "wenjieliu@10.0.0.127",
            "health_host": "192.168.20.2",
            "restart_cmd": "pkill -f exo_daemon 2>/dev/null; "
            "pkill -f '/exo/.venv/bin/exo' 2>/dev/null; "
            "sleep 2; nohup ~/exo/exo_daemon.sh > /dev/null 2>&1 & "
            "disown",
        },
        {
            "name": "exo-128",
            "ssh_target": "magi2@10.0.0.128",
            "health_host": "192.168.20.3",
            "restart_cmd": "pkill -f exo_daemon 2>/dev/null; "
            "pkill -f '/exo/.venv/bin/exo' 2>/dev/null; "
            "sleep 2; nohup ~/exo/exo_daemon.sh > /dev/null 2>&1 & "
            "disown",
        },
    ]
)
WATCHDOG_REMOTE_HOSTS_JSON = os.getenv(
    "WATCHDOG_REMOTE_HOSTS_JSON", DEFAULT_REMOTE_HOSTS_JSON
)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("local-llm-watchdog")

WAITABLE_STATUSES = {"queued", "running"}
TERMINAL_STATUSES = {"completed", "failed"}
WATCHDOG_ALLOWED_PROFILES = {
    "generic_chat",
    "ui_summary",
    "ui_translate",
    "news_article_summary",
    "hourly_news_brief",
}
WATCHDOG_ALLOWED_BACKENDS = {
    "exo",
    "openai_compatible",
    "anthropic",
    "codex_cli",
    "claude_code_cli",
}
WATCHDOG_POLICY_VERSION = "2026-03-19-routing-v3"
WATCHDOG_VISIBLE_PROVIDER_CONFIG_IDS = {
    "exo",
    "minimax",
    "codex-cli",
    "claude-code-cli",
}
WATCHDOG_HOST_NODE_ROOT = Path(os.getenv("WATCHDOG_HOST_NODE_ROOT", "/run/host-node"))
WATCHDOG_HOST_AUTH_ROOT = Path(os.getenv("WATCHDOG_HOST_AUTH_ROOT", "/run/host-auth"))
WATCHDOG_CLI_HOME = Path(os.getenv("WATCHDOG_CLI_HOME", "/run/cli-home"))
WATCHDOG_CODEX_HOME = Path(
    os.getenv("WATCHDOG_CODEX_HOME", str(WATCHDOG_CLI_HOME / ".codex"))
)
WATCHDOG_CLAUDE_HOME = Path(
    os.getenv("WATCHDOG_CLAUDE_HOME", str(WATCHDOG_CLI_HOME / ".claude"))
)
WATCHDOG_CODEX_EMPTY_DIR = Path(
    os.getenv("WATCHDOG_CODEX_EMPTY_DIR", "/run/codex-empty")
)
WATCHDOG_CLI_STATUS_MAX_AGE_SECONDS = float(
    os.getenv("WATCHDOG_CLI_STATUS_MAX_AGE_SECONDS", "15")
)


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

@dataclass
class RemoteHost:
    name: str
    ssh_target: str
    health_host: str | None
    restart_cmd: str

    @property
    def ip(self) -> str:
        if self.health_host:
            return self.health_host
        target = self.ssh_target.split("@", 1)[-1]
        return target.split(":", 1)[0]

    @property
    def role(self) -> str:
        return "control" if self.ip == EXO_CONTROL_HOST else "peer"

    @property
    def listen_port(self) -> int:
        return EXO_API_PORT


@dataclass
class WatchdogState:
    last_state: dict[str, Any] = field(default_factory=dict)
    last_poll_ts: float = 0.0
    last_poll_ok: bool = False
    last_restart_ts: float = 0.0
    restart_count: int = 0
    anomalies: list[str] = field(default_factory=list)
    models: list[str] = field(default_factory=list)
    last_known_models: list[str] = field(default_factory=list)
    queue_depth: int = 0
    active_job: str | None = None
    oldest_waiting_age_seconds: float | None = None
    job_failures_consecutive: int = 0
    last_remote_restart_host: str | None = None
    recovery_state: str = "idle"
    recovery_error: str | None = None
    instance_to_host_map: dict[str, list[str]] = field(default_factory=dict)
    host_health: dict[str, dict[str, Any]] = field(default_factory=dict)
    peer_health: dict[str, dict[str, Any]] = field(default_factory=dict)
    runners: dict[str, str] = field(default_factory=dict)
    instances: dict[str, str] = field(default_factory=dict)
    backend: str = "exo"
    desired_model: str = EXO_DESIRED_MODEL
    model: str | None = None
    api_endpoint: str | None = None
    instance_status: str = "missing"
    last_scheduled_restart_ts: float = 0.0


_state = WatchdogState()
_state.api_endpoint = EXO_CONTROL_ENDPOINT
_poll_lock = asyncio.Lock()
_recovery_lock = asyncio.Lock()
_queue_wakeup = asyncio.Event()
_job_events: dict[str, asyncio.Event] = {}
_db_lock = Lock()
_cli_provider_status_lock = Lock()
_cli_provider_status_cache: dict[str, dict[str, Any]] = {}
_cli_provider_status_checked_at: dict[str, float] = {}


class WatchdogExecutionError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int = 502,
        error_type: str = "execution_error",
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_type = error_type
_db_conn: sqlite3.Connection | None = None
_provider_secret_cache_lock = Lock()
_provider_secret_cache: dict[str, dict[str, str]] = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _json_loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


def _now() -> float:
    return time.time()


def _parse_endpoint_list(raw: str) -> list[str]:
    endpoints: list[str] = []
    for chunk in raw.split(","):
        value = chunk.strip()
        if not value:
            continue
        if "://" not in value:
            value = f"http://{value}"
        endpoints.append(value.rstrip("/"))
    return endpoints


def _memory_field_bytes(memory: dict[str, Any], key: str) -> int | None:
    if not isinstance(memory, dict):
        return None
    value = memory.get(key)
    if not isinstance(value, dict):
        return None
    bytes_value = value.get("inBytes", value.get("bytes"))
    return int(bytes_value) if isinstance(bytes_value, (int, float)) else None


EXO_ENDPOINTS = _parse_endpoint_list(EXO_ENDPOINTS_RAW)
if not EXO_ENDPOINTS:
    raise RuntimeError("EXO_ENDPOINTS must contain at least one endpoint")
if not EXO_CONTROL_ENDPOINT:
    EXO_CONTROL_ENDPOINT = EXO_ENDPOINTS[0]
_control_parts = urlsplit(EXO_CONTROL_ENDPOINT)
EXO_CONTROL_HOST = _control_parts.hostname or "192.168.20.1"
EXO_API_PORT = _control_parts.port or (443 if _control_parts.scheme == "https" else 52415)


def _configured_host_ips() -> list[str]:
    hosts: list[str] = []
    for endpoint in EXO_ENDPOINTS:
        host = urlsplit(endpoint).hostname
        if host:
            hosts.append(host)
    return list(dict.fromkeys(hosts))


def _remote_host_for_ip(ip: str) -> RemoteHost | None:
    for host in REMOTE_HOSTS:
        if host.ip == ip:
            return host
    return None


def _control_remote_host() -> RemoteHost | None:
    return _remote_host_for_ip(EXO_CONTROL_HOST)


def _peer_remote_hosts() -> list[RemoteHost]:
    peers = []
    for host in REMOTE_HOSTS:
        if host.role == "peer":
            peers.append(host)
    return peers


def _active_job_age_seconds() -> float | None:
    if not _state.active_job:
        return None
    snapshot = _get_job_snapshot(_state.active_job)
    started_at = snapshot.get("started_at") if snapshot else None
    if not started_at:
        return None
    return max(0.0, _now() - float(started_at))


def _should_recover_during_active_job() -> bool:
    active_age = _active_job_age_seconds()
    if active_age is None or active_age < WATCHDOG_ACTIVE_JOB_RECOVERY_GRACE_SECONDS:
        return False
    return any(not info.get("healthy", False) for info in _state.host_health.values())


def _current_api_base() -> str:
    return (_state.api_endpoint or EXO_CONTROL_ENDPOINT).rstrip("/")


def _job_event(job_id: str) -> asyncio.Event:
    event = _job_events.get(job_id)
    if event is None:
        event = asyncio.Event()
        _job_events[job_id] = event
    return event


def _estimate_tokens_from_text(text: str) -> int:
    return max(1, len(text) // 4) if text else 0


def _parse_model_choice(requested_model: str | None) -> str:
    requested = (requested_model or "").strip()
    if requested:
        if requested in _state.models or requested in _state.last_known_models:
            return requested
        active_models = _state.models or _state.last_known_models
        if len(active_models) == 1:
            replacement = active_models[0]
            log.info(
                "Remapping requested model %s to active model %s",
                requested,
                replacement,
            )
            return replacement
        return requested
    if _state.models:
        return _state.models[0]
    if _state.last_known_models:
        return _state.last_known_models[0]
    return EXO_DESIRED_MODEL


def _strip_think(text: str) -> str:
    return re.sub(r"<think>.*?</think>\s*", "", text or "", flags=re.DOTALL).strip()


def _truncate_text(
    value: str,
    limit: int,
    field_name: str,
    truncated_fields: list[str],
) -> str:
    text = (value or "").strip()
    if len(text) <= limit:
        return text
    truncated_fields.append(field_name)
    return text[:limit].rstrip()


def _normalize_headline(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s]", " ", (value or "").lower())).strip()


def _dedupe_headlines(headlines: list[str]) -> list[str]:
    seen: list[set[str]] = []
    unique: list[str] = []

    for headline in headlines:
        normalized = _normalize_headline(headline)
        words = {word for word in normalized.split(" ") if len(word) >= 4}
        if not words:
            continue

        is_duplicate = False
        for prior in seen:
            similarity = len(words & prior) / max(1, min(len(words), len(prior)))
            if similarity > 0.6:
                is_duplicate = True
                break

        if not is_duplicate:
            seen.append(words)
            unique.append(headline)

    return unique


def _dedupe_hourly_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen_titles: set[str] = set()
    unique: list[dict[str, Any]] = []

    for item in items:
        title = str(item.get("title") or "").strip()
        key = title.lower()
        if not title or key in seen_titles:
            continue
        seen_titles.add(key)
        unique.append(
            {
                "source": str(item.get("source") or "").strip(),
                "title": title,
                "link": str(item.get("link") or "").strip(),
            }
        )
    return unique


def _build_openai_response(
    model: str,
    content: str,
    usage: dict[str, int] | None = None,
) -> dict[str, Any]:
    clean_content = _strip_think(content)
    payload_usage = usage or {
        "prompt_tokens": _estimate_tokens_from_text(clean_content),
        "completion_tokens": _estimate_tokens_from_text(clean_content),
        "total_tokens": _estimate_tokens_from_text(clean_content) * 2,
    }
    return {
        "id": f"chatcmpl-{uuid.uuid4().hex}",
        "object": "chat.completion",
        "created": int(_now()),
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": clean_content},
                "finish_reason": "stop",
            }
        ],
        "usage": payload_usage,
    }


def _content_from_openai_response(payload: dict[str, Any]) -> str:
    choices = payload.get("choices") or [{}]
    message = choices[0].get("message") or {} if choices else {}
    return str(message.get("content") or "").strip()


def _parse_remote_hosts() -> list[RemoteHost]:
    try:
        raw_hosts = json.loads(WATCHDOG_REMOTE_HOSTS_JSON)
    except json.JSONDecodeError as exc:
        log.error("Invalid WATCHDOG_REMOTE_HOSTS_JSON: %s", exc)
        return []

    hosts: list[RemoteHost] = []
    for entry in raw_hosts:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or "").strip()
        target = str(entry.get("ssh_target") or "").strip()
        health_host = str(entry.get("health_host") or "").strip() or None
        restart_cmd = str(entry.get("restart_cmd") or "").strip()
        if name and target and restart_cmd:
            hosts.append(
                RemoteHost(
                    name=name,
                    ssh_target=target,
                    health_host=health_host,
                    restart_cmd=restart_cmd,
                )
            )
    return hosts


REMOTE_HOSTS = _parse_remote_hosts()
REMOTE_HOST_IPS = {host.ip for host in REMOTE_HOSTS}


# ---------------------------------------------------------------------------
# Kubernetes Secret-backed provider secret storage
# ---------------------------------------------------------------------------

def _watchdog_namespace() -> str:
    if WATCHDOG_NAMESPACE:
        return WATCHDOG_NAMESPACE
    try:
        return WATCHDOG_K8S_NAMESPACE_PATH.read_text(encoding="utf-8").strip()
    except Exception:  # noqa: BLE001
        return ""


def _k8s_provider_secret_store_enabled() -> bool:
    return bool(
        WATCHDOG_PROVIDER_SECRET_NAME
        and os.getenv("KUBERNETES_SERVICE_HOST", "").strip()
        and WATCHDOG_K8S_TOKEN_PATH.exists()
        and WATCHDOG_K8S_CA_PATH.exists()
        and _watchdog_namespace()
    )


def _k8s_api_base_url() -> str:
    host = os.getenv("KUBERNETES_SERVICE_HOST", "").strip()
    port = os.getenv("KUBERNETES_SERVICE_PORT", "443").strip() or "443"
    if not host:
        raise RuntimeError("Kubernetes API host is not configured")
    return f"https://{host}:{port}"


def _k8s_auth_headers() -> dict[str, str]:
    token = WATCHDOG_K8S_TOKEN_PATH.read_text(encoding="utf-8").strip()
    if not token:
        raise RuntimeError("Kubernetes service account token is empty")
    return {"Authorization": f"Bearer {token}"}


def _k8s_provider_secret_path() -> str:
    namespace = _watchdog_namespace()
    if not namespace:
        raise RuntimeError("watchdog namespace is not available")
    return f"/api/v1/namespaces/{namespace}/secrets/{WATCHDOG_PROVIDER_SECRET_NAME}"


def _provider_secret_keys(provider_config_id: str) -> dict[str, str]:
    normalized = re.sub(
        r"[^A-Za-z0-9._-]",
        "_",
        _normalize_provider_config_id(provider_config_id),
    )
    return {
        "api_key": f"{normalized}.api_key",
        "login_auth": f"{normalized}.login_auth",
    }


def _encode_k8s_secret_value(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("ascii")


def _decode_k8s_secret_value(value: Any) -> str:
    if not value:
        return ""
    try:
        return base64.b64decode(str(value)).decode("utf-8")
    except Exception:  # noqa: BLE001
        return ""


def _get_cached_provider_secret(provider_config_id: str) -> dict[str, str] | None:
    with _provider_secret_cache_lock:
        cached = _provider_secret_cache.get(provider_config_id)
        return dict(cached) if cached is not None else None


def _set_cached_provider_secret(
    provider_config_id: str,
    *,
    api_key: str,
    login_auth: str,
) -> dict[str, str]:
    payload = {
        "api_key": str(api_key or ""),
        "login_auth": str(login_auth or ""),
    }
    with _provider_secret_cache_lock:
        _provider_secret_cache[provider_config_id] = dict(payload)
    return payload


def _read_provider_secret_from_k8s(provider_config_id: str) -> dict[str, str]:
    if not _k8s_provider_secret_store_enabled():
        return {"api_key": "", "login_auth": ""}
    path = _k8s_provider_secret_path()
    with httpx.Client(
        base_url=_k8s_api_base_url(),
        headers=_k8s_auth_headers(),
        verify=str(WATCHDOG_K8S_CA_PATH),
        timeout=WATCHDOG_K8S_API_TIMEOUT_SECONDS,
    ) as client:
        response = client.get(path)
        if response.status_code == 404:
            return {"api_key": "", "login_auth": ""}
        response.raise_for_status()
        resource = response.json()
    data = resource.get("data") or {}
    keys = _provider_secret_keys(provider_config_id)
    return {
        "api_key": _decode_k8s_secret_value(data.get(keys["api_key"])),
        "login_auth": _decode_k8s_secret_value(data.get(keys["login_auth"])),
    }


def _load_provider_secret(
    provider_config_id: str,
    *,
    fallback_api_key: str = "",
    fallback_login_auth: str = "",
) -> dict[str, str]:
    normalized_id = _normalize_provider_config_id(provider_config_id)
    if _k8s_provider_secret_store_enabled():
        try:
            secret = _read_provider_secret_from_k8s(normalized_id)
            if (
                not secret["api_key"]
                and not secret["login_auth"]
                and (fallback_api_key or fallback_login_auth)
            ):
                secret = {
                    "api_key": str(fallback_api_key or ""),
                    "login_auth": str(fallback_login_auth or ""),
                }
            return _set_cached_provider_secret(
                normalized_id,
                api_key=secret["api_key"],
                login_auth=secret["login_auth"],
            )
        except Exception as exc:  # noqa: BLE001
            cached = _get_cached_provider_secret(normalized_id)
            if cached is not None:
                log.warning(
                    "falling back to cached provider secret for %s after Kubernetes API error: %s",
                    normalized_id,
                    exc,
                )
                return cached
            log.warning(
                "falling back to legacy provider secret storage for %s after Kubernetes API error: %s",
                normalized_id,
                exc,
            )
    return _set_cached_provider_secret(
        normalized_id,
        api_key=str(fallback_api_key or ""),
        login_auth=str(fallback_login_auth or ""),
    )


def _upsert_provider_secret_in_k8s(
    provider_config_id: str,
    *,
    api_key: str,
    login_auth: str,
) -> bool:
    if not _k8s_provider_secret_store_enabled():
        return False
    normalized_id = _normalize_provider_config_id(provider_config_id)
    namespace = _watchdog_namespace()
    resource_path = _k8s_provider_secret_path()
    collection_path = f"/api/v1/namespaces/{namespace}/secrets"
    keys = _provider_secret_keys(normalized_id)
    with httpx.Client(
        base_url=_k8s_api_base_url(),
        headers=_k8s_auth_headers(),
        verify=str(WATCHDOG_K8S_CA_PATH),
        timeout=WATCHDOG_K8S_API_TIMEOUT_SECONDS,
    ) as client:
        existing_response = client.get(resource_path)
        if existing_response.status_code == 404:
            existing = None
        else:
            existing_response.raise_for_status()
            existing = existing_response.json()
        data = dict((existing or {}).get("data") or {})
        if api_key:
            data[keys["api_key"]] = _encode_k8s_secret_value(api_key)
        else:
            data.pop(keys["api_key"], None)
        if login_auth:
            data[keys["login_auth"]] = _encode_k8s_secret_value(login_auth)
        else:
            data.pop(keys["login_auth"], None)
        if existing is None and not data:
            _set_cached_provider_secret(
                normalized_id,
                api_key=api_key,
                login_auth=login_auth,
            )
            return True
        payload = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": WATCHDOG_PROVIDER_SECRET_NAME,
                "namespace": namespace,
            },
            "type": "Opaque",
            "data": data,
        }
        if existing is not None:
            resource_version = str(
                (existing.get("metadata") or {}).get("resourceVersion") or ""
            ).strip()
            if resource_version:
                payload["metadata"]["resourceVersion"] = resource_version
            response = client.put(resource_path, json=payload)
        else:
            response = client.post(collection_path, json=payload)
        response.raise_for_status()
    _set_cached_provider_secret(
        normalized_id,
        api_key=api_key,
        login_auth=login_auth,
    )
    return True


def _migrate_provider_secrets_to_k8s(conn: sqlite3.Connection) -> None:
    if not _k8s_provider_secret_store_enabled():
        return
    rows = conn.execute(
        "SELECT id, api_key, login_auth FROM llm_provider_configs"
    ).fetchall()
    for row in rows:
        provider_id = str(row["id"])
        legacy_api_key = str(row["api_key"] or "")
        legacy_login_auth = str(row["login_auth"] or "")
        if not legacy_api_key and not legacy_login_auth:
            continue
        try:
            current = _read_provider_secret_from_k8s(provider_id)
            _upsert_provider_secret_in_k8s(
                provider_id,
                api_key=current["api_key"] or legacy_api_key,
                login_auth=current["login_auth"] or legacy_login_auth,
            )
            conn.execute(
                """
                UPDATE llm_provider_configs
                SET api_key = '', login_auth = ''
                WHERE id = ?
                """,
                (provider_id,),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "failed to migrate provider secrets for %s into Kubernetes Secret storage: %s",
                provider_id,
                exc,
            )


# ---------------------------------------------------------------------------
# SQLite persistence
# ---------------------------------------------------------------------------

def _db() -> sqlite3.Connection:
    global _db_conn
    if _db_conn is None:
        WATCHDOG_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(WATCHDOG_DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        _db_conn = conn
    return _db_conn


def _db_init() -> None:
    conn = _db()
    with _db_lock:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                source TEXT NOT NULL,
                profile TEXT NOT NULL,
                status TEXT NOT NULL,
                request_json TEXT NOT NULL,
                response_json TEXT,
                error_json TEXT,
                input_chars INTEGER NOT NULL,
                estimated_tokens INTEGER NOT NULL,
                truncated_fields_json TEXT NOT NULL,
                model TEXT,
                client_request_id TEXT,
                created_at REAL NOT NULL,
                queued_at REAL NOT NULL,
                started_at REAL,
                completed_at REAL,
                wait_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS job_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                ts REAL NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT
            );

            CREATE TABLE IF NOT EXISTS host_state (
                host_name TEXT PRIMARY KEY,
                ssh_target TEXT NOT NULL,
                last_restart_ts REAL,
                last_result_json TEXT,
                last_error TEXT,
                runner_ids_json TEXT DEFAULT '[]'
            );

            CREATE TABLE IF NOT EXISTS recovery_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                stage TEXT NOT NULL,
                host_name TEXT,
                payload_json TEXT
            );

            CREATE TABLE IF NOT EXISTS llm_routes (
                id TEXT PRIMARY KEY,
                source_pattern TEXT NOT NULL,
                profile_pattern TEXT NOT NULL,
                backend TEXT NOT NULL,
                provider TEXT NOT NULL,
                provider_config_id TEXT,
                model TEXT,
                base_url TEXT,
                api_key_env TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                priority INTEGER NOT NULL DEFAULT 100,
                notes TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS llm_provider_configs (
                id TEXT PRIMARY KEY,
                label TEXT NOT NULL,
                backend TEXT NOT NULL,
                provider TEXT NOT NULL,
                endpoint TEXT,
                api_key TEXT,
                login_auth TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                notes TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            """
        )
        job_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
        }
        route_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(llm_routes)").fetchall()
        }
        if "client_request_id" not in job_columns:
            conn.execute("ALTER TABLE jobs ADD COLUMN client_request_id TEXT")
        if "provider_config_id" not in route_columns:
            conn.execute("ALTER TABLE llm_routes ADD COLUMN provider_config_id TEXT")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_client_request_id ON jobs(client_request_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_llm_routes_match ON llm_routes(enabled, priority)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_llm_routes_provider_config ON llm_routes(provider_config_id)"
        )
        for host in REMOTE_HOSTS:
            conn.execute(
                """
                INSERT INTO host_state (host_name, ssh_target)
                VALUES (?, ?)
                ON CONFLICT(host_name) DO UPDATE SET ssh_target = excluded.ssh_target
                """,
                (host.name, host.ssh_target),
            )
        _seed_default_llm_provider_configs(conn)
        _seed_default_llm_routes(conn)
        _migrate_provider_secrets_to_k8s(conn)
        conn.execute(
            """
            UPDATE llm_routes
            SET provider_config_id = 'exo'
            WHERE (provider_config_id IS NULL OR provider_config_id = '')
              AND backend = 'exo'
            """
        )
        conn.execute(
            """
            UPDATE llm_routes
            SET provider_config_id = 'minimax'
            WHERE (provider_config_id IS NULL OR provider_config_id = '')
              AND provider = 'minimax'
            """
        )
        conn.execute(
            """
            UPDATE llm_routes
            SET provider_config_id = 'openai'
            WHERE (provider_config_id IS NULL OR provider_config_id = '')
              AND provider = 'openai'
            """
        )
        conn.execute(
            """
            UPDATE llm_routes
            SET provider_config_id = 'claude'
            WHERE (provider_config_id IS NULL OR provider_config_id = '')
              AND provider IN ('claude', 'anthropic')
            """
        )
        conn.commit()


def _default_llm_provider_configs() -> list[dict[str, Any]]:
    return [
        {
            "id": "exo",
            "label": "Exo Local Cluster",
            "backend": "exo",
            "provider": "exo",
            "endpoint": "",
            "api_key": "",
            "login_auth": "",
            "enabled": True,
            "notes": "Uses the watchdog-managed Exo cluster. Leave endpoint blank unless you need a preferred override.",
        },
        {
            "id": "minimax",
            "label": "MiniMax",
            "backend": "openai_compatible",
            "provider": "minimax",
            "endpoint": "https://api.minimax.io/v1",
            "api_key": "",
            "login_auth": "",
            "enabled": True,
            "notes": "Default MiniMax endpoint. Change it to https://api.minimaxi.com/v1 if you want the mainland China endpoint.",
        },
        {
            "id": "codex-cli",
            "label": "Codex CLI",
            "backend": "codex_cli",
            "provider": "codex_cli",
            "endpoint": "",
            "api_key": "",
            "login_auth": "",
            "enabled": True,
            "notes": "Uses the host's Codex CLI login. Blank model means use the CLI default model.",
        },
        {
            "id": "claude-code-cli",
            "label": "Claude Code CLI",
            "backend": "claude_code_cli",
            "provider": "claude_code_cli",
            "endpoint": "",
            "api_key": "",
            "login_auth": "",
            "enabled": True,
            "notes": "Uses the host's Claude Code login. Blank model means use the CLI default model.",
        },
    ]


def _default_llm_routes() -> list[dict[str, Any]]:
    return [
        {
            "id": "default-fallback",
            "source_pattern": "*",
            "profile_pattern": "*",
            "backend": "exo",
            "provider": "exo",
            "provider_config_id": "exo",
            "model": "",
            "base_url": "",
            "api_key_env": "",
            "enabled": True,
            "priority": 10,
            "notes": "Fallback route for any watchdog-managed request.",
        },
        {
            "id": "arb-validator-chat",
            "source_pattern": "arb-validator",
            "profile_pattern": "generic_chat",
            "backend": "exo",
            "provider": "exo",
            "provider_config_id": "exo",
            "model": "",
            "base_url": "",
            "api_key_env": "",
            "enabled": True,
            "priority": 200,
            "notes": "Validator pair checks. Point this to any managed gateway for a dedicated verifier model.",
        },
        {
            "id": "news-collector-summary",
            "source_pattern": "news-collector",
            "profile_pattern": "news_article_summary",
            "backend": "exo",
            "provider": "exo",
            "provider_config_id": "exo",
            "model": "",
            "base_url": "",
            "api_key_env": "",
            "enabled": True,
            "priority": 200,
            "notes": "News article summarization.",
        },
        {
            "id": "news-reporter-hourly",
            "source_pattern": "news-reporter-hourly",
            "profile_pattern": "hourly_news_brief",
            "backend": "exo",
            "provider": "exo",
            "provider_config_id": "exo",
            "model": "",
            "base_url": "",
            "api_key_env": "",
            "enabled": True,
            "priority": 205,
            "notes": "Hourly Discord brief route. Exo Watchdog auto-selects the active Exo model.",
        },
        {
            "id": "worldmonitor-summary",
            "source_pattern": "worldmonitor-api",
            "profile_pattern": "ui_summary",
            "backend": "exo",
            "provider": "exo",
            "provider_config_id": "exo",
            "model": "",
            "base_url": "",
            "api_key_env": "",
            "enabled": True,
            "priority": 220,
            "notes": "Worldmonitor summary route.",
        },
        {
            "id": "worldmonitor-translate",
            "source_pattern": "worldmonitor-api",
            "profile_pattern": "ui_translate",
            "backend": "exo",
            "provider": "exo",
            "provider_config_id": "exo",
            "model": "",
            "base_url": "",
            "api_key_env": "",
            "enabled": True,
            "priority": 220,
            "notes": "Worldmonitor translation route.",
        },
        {
            "id": "guardian-triage",
            "source_pattern": "k8s-guardian-triage",
            "profile_pattern": "generic_chat",
            "backend": "exo",
            "provider": "exo",
            "provider_config_id": "exo",
            "model": "",
            "base_url": "",
            "api_key_env": "",
            "enabled": True,
            "priority": 220,
            "notes": "Guardian triage model route.",
        },
        {
            "id": "guardian-decision",
            "source_pattern": "k8s-guardian-decision",
            "profile_pattern": "generic_chat",
            "backend": "exo",
            "provider": "exo",
            "provider_config_id": "exo",
            "model": "",
            "base_url": "",
            "api_key_env": "",
            "enabled": True,
            "priority": 230,
            "notes": "Guardian decision model route.",
        },
        {
            "id": "job-autoflow-documents",
            "source_pattern": "job-autoflow-documents",
            "profile_pattern": "generic_chat",
            "backend": "exo",
            "provider": "exo",
            "provider_config_id": "exo",
            "model": "",
            "base_url": "",
            "api_key_env": "",
            "enabled": True,
            "priority": 210,
            "notes": "Job Autoflow resume and cover-letter tailoring route.",
        },
        {
            "id": "x-bot-posts",
            "source_pattern": "x-bot",
            "profile_pattern": "generic_chat",
            "backend": "openai_compatible",
            "provider": "minimax",
            "provider_config_id": "minimax",
            "model": "MiniMax-M2.5-highspeed",
            "base_url": "https://api.minimax.io/v1",
            "api_key_env": "MINIMAX_API_KEY",
            "enabled": True,
            "priority": 220,
            "notes": "X bot posting route pinned to the current MiniMax model.",
        },
    ]


def _normalize_route_id(raw: Any) -> str:
    route_id = str(raw or "").strip()
    if not route_id:
        raise ValueError("route id is required")
    if not re.fullmatch(r"[A-Za-z0-9._:-]+", route_id):
        raise ValueError("route id may only contain letters, numbers, dot, underscore, colon, or hyphen")
    return route_id[:120]


def _normalize_route_pattern(raw: Any) -> str:
    value = str(raw or "*").strip() or "*"
    return value[:120]


def _normalize_provider_config_id(raw: Any) -> str:
    provider_id = str(raw or "").strip()
    if not provider_id:
        raise ValueError("provider config id is required")
    if not re.fullmatch(r"[A-Za-z0-9._:-]+", provider_id):
        raise ValueError("provider config id may only contain letters, numbers, dot, underscore, colon, or hyphen")
    return provider_id[:120]


def _is_cli_backend(backend: str) -> bool:
    return backend in {"codex_cli", "claude_code_cli"}


def _codex_binary_path() -> Path:
    return WATCHDOG_HOST_NODE_ROOT / "bin" / "codex"


def _claude_binary_path() -> Path:
    return WATCHDOG_HOST_NODE_ROOT / "bin" / "claude"


def _host_codex_auth_path() -> Path:
    return WATCHDOG_HOST_AUTH_ROOT / ".codex" / "auth.json"


def _host_codex_models_cache_path() -> Path:
    return WATCHDOG_HOST_AUTH_ROOT / ".codex" / "models_cache.json"


def _host_claude_credentials_path() -> Path:
    return WATCHDOG_HOST_AUTH_ROOT / ".claude" / ".credentials.json"


def _runtime_codex_auth_path() -> Path:
    return WATCHDOG_CODEX_HOME / "auth.json"


def _runtime_codex_models_cache_path() -> Path:
    return WATCHDOG_CODEX_HOME / "models_cache.json"


def _runtime_claude_credentials_path() -> Path:
    return WATCHDOG_CLAUDE_HOME / ".credentials.json"


def _sync_optional_runtime_file(source: Path, destination: Path) -> bool:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not source.exists():
        destination.unlink(missing_ok=True)
        return False
    shutil.copy2(source, destination)
    try:
        destination.chmod(0o600)
    except Exception:  # noqa: BLE001
        pass
    return True


def _prepare_cli_runtime() -> None:
    WATCHDOG_CLI_HOME.mkdir(parents=True, exist_ok=True)
    WATCHDOG_CODEX_HOME.mkdir(parents=True, exist_ok=True)
    WATCHDOG_CLAUDE_HOME.mkdir(parents=True, exist_ok=True)
    WATCHDOG_CODEX_EMPTY_DIR.mkdir(parents=True, exist_ok=True)
    _sync_optional_runtime_file(_host_codex_auth_path(), _runtime_codex_auth_path())
    _sync_optional_runtime_file(
        _host_codex_models_cache_path(),
        _runtime_codex_models_cache_path(),
    )
    _sync_optional_runtime_file(
        _host_claude_credentials_path(),
        _runtime_claude_credentials_path(),
    )


def _cli_provider_status(provider_config_id: str, *, force: bool = False) -> dict[str, Any]:
    provider_id = _normalize_provider_config_id(provider_config_id)
    if provider_id not in {"codex-cli", "claude-code-cli"}:
        return {}

    now = _now()
    with _cli_provider_status_lock:
        checked_at = float(_cli_provider_status_checked_at.get(provider_id) or 0.0)
        cached = _cli_provider_status_cache.get(provider_id)
    if cached and not force and (now - checked_at) < WATCHDOG_CLI_STATUS_MAX_AGE_SECONDS:
        return dict(cached)

    _prepare_cli_runtime()
    if provider_id == "codex-cli":
        binary_path = _codex_binary_path()
        auth_path = _host_codex_auth_path()
        label = "Codex CLI"
    else:
        binary_path = _claude_binary_path()
        auth_path = _host_claude_credentials_path()
        label = "Claude Code CLI"

    binary_present = binary_path.exists() and os.access(binary_path, os.X_OK)
    auth_present = auth_path.exists()
    if not binary_present:
        status = "missing_binary"
        message = f"{label} binary not found at {binary_path}"
    elif not auth_present:
        status = "missing_auth"
        message = f"{label} auth file not found at {auth_path}"
    else:
        status = "ready"
        message = f"{label} is ready on this host."

    payload = {
        "binary_present": binary_present,
        "auth_present": auth_present,
        "status": status,
        "status_message": message,
        "checked_at": now,
    }
    with _cli_provider_status_lock:
        _cli_provider_status_cache[provider_id] = dict(payload)
        _cli_provider_status_checked_at[provider_id] = now
    return payload


def _row_to_provider_config(row: sqlite3.Row, *, include_secrets: bool = True) -> dict[str, Any]:
    secrets = _load_provider_secret(
        str(row["id"]),
        fallback_api_key=str(row["api_key"] or ""),
        fallback_login_auth=str(row["login_auth"] or ""),
    )
    payload = {
        "id": str(row["id"]),
        "label": str(row["label"]),
        "backend": str(row["backend"]),
        "provider": str(row["provider"]),
        "endpoint": str(row["endpoint"] or ""),
        "enabled": bool(row["enabled"]),
        "notes": str(row["notes"] or ""),
        "created_at": float(row["created_at"]),
        "updated_at": float(row["updated_at"]),
        "has_api_key": bool(secrets["api_key"].strip()),
        "has_login_auth": bool(secrets["login_auth"].strip()),
    }
    runtime_status = _cli_provider_status(payload["id"])
    if runtime_status:
        payload.update(runtime_status)
    if include_secrets:
        payload["api_key"] = secrets["api_key"]
        payload["login_auth"] = secrets["login_auth"]
    return payload


def _public_provider_config(provider: dict[str, Any] | None) -> dict[str, Any] | None:
    if provider is None:
        return None
    public = dict(provider)
    public.pop("api_key", None)
    public.pop("login_auth", None)
    return public


def _public_route(route: dict[str, Any]) -> dict[str, Any]:
    public = dict(route)
    public.pop("api_key", None)
    public.pop("login_auth", None)
    provider_config = public.get("provider_config")
    if isinstance(provider_config, dict):
        public["provider_config"] = _public_provider_config(provider_config)
    return public


def _seed_default_llm_provider_configs(conn: sqlite3.Connection) -> None:
    now = _now()
    for provider in _default_llm_provider_configs():
        conn.execute(
            """
            INSERT OR IGNORE INTO llm_provider_configs (
                id, label, backend, provider, endpoint, api_key, login_auth,
                enabled, notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                provider["id"],
                provider["label"],
                provider["backend"],
                provider["provider"],
                provider["endpoint"],
                provider["api_key"],
                provider["login_auth"],
                1 if provider["enabled"] else 0,
                provider["notes"],
                now,
                now,
            ),
        )


def _list_llm_provider_configs(
    *,
    include_disabled: bool = True,
    include_secrets: bool = True,
) -> list[dict[str, Any]]:
    query = "SELECT * FROM llm_provider_configs"
    if not include_disabled:
        query += " WHERE enabled = 1"
    query += " ORDER BY id ASC"
    with _db_lock:
        rows = _db().execute(query).fetchall()
    return [
        _row_to_provider_config(row, include_secrets=include_secrets)
        for row in rows
        if str(row["id"]) in WATCHDOG_VISIBLE_PROVIDER_CONFIG_IDS
    ]


def _get_llm_provider_config(
    provider_config_id: str,
    *,
    include_secrets: bool = True,
) -> dict[str, Any] | None:
    normalized_id = _normalize_provider_config_id(provider_config_id)
    with _db_lock:
        row = _db().execute(
            "SELECT * FROM llm_provider_configs WHERE id = ?",
            (normalized_id,),
        ).fetchone()
    if row is None:
        return None
    return _row_to_provider_config(row, include_secrets=include_secrets)


def _validate_provider_config_payload(provider_config_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    normalized_id = _normalize_provider_config_id(provider_config_id)
    existing = _get_llm_provider_config(normalized_id)
    backend = str(payload.get("backend") or "openai_compatible").strip().lower()
    if backend not in WATCHDOG_ALLOWED_BACKENDS:
        raise ValueError(f"unsupported provider backend: {backend}")

    label = str(payload.get("label") or normalized_id).strip()[:120]
    provider_default = normalized_id.replace("-", "_")
    provider = str(payload.get("provider") or provider_default).strip()[:80]
    endpoint = str(payload.get("endpoint") or "").strip().rstrip("/")
    incoming_api_key = payload.get("api_key")
    incoming_login_auth = payload.get("login_auth")
    clear_api_key = bool(payload.get("clear_api_key", False))
    clear_login_auth = bool(payload.get("clear_login_auth", False))
    if clear_api_key:
        api_key = ""
    elif incoming_api_key is None or str(incoming_api_key) == "":
        api_key = str((existing or {}).get("api_key") or "")
    else:
        api_key = str(incoming_api_key)
    if clear_login_auth:
        login_auth = ""
    elif incoming_login_auth is None or str(incoming_login_auth) == "":
        login_auth = str((existing or {}).get("login_auth") or "")
    else:
        login_auth = str(incoming_login_auth)
    enabled = bool(payload.get("enabled", True))
    notes = str(payload.get("notes") or "").strip()[:500]

    if _is_cli_backend(backend):
        endpoint = ""
        api_key = ""
        login_auth = ""
    elif backend != "exo" and not endpoint:
        raise ValueError("provider endpoint is required for non-Exo providers")

    return {
        "id": normalized_id,
        "label": label or normalized_id,
        "backend": backend,
        "provider": provider or provider_default,
        "endpoint": endpoint,
        "api_key": api_key,
        "login_auth": login_auth,
        "enabled": enabled,
        "notes": notes,
    }


def _upsert_llm_provider_config(provider_config_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    provider = _validate_provider_config_payload(provider_config_id, payload)
    now = _now()
    use_k8s_secret_store = _k8s_provider_secret_store_enabled()
    if use_k8s_secret_store:
        _upsert_provider_secret_in_k8s(
            provider["id"],
            api_key=provider["api_key"],
            login_auth=provider["login_auth"],
        )
    with _db_lock:
        conn = _db()
        conn.execute(
            """
            INSERT INTO llm_provider_configs (
                id, label, backend, provider, endpoint, api_key, login_auth,
                enabled, notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                label = excluded.label,
                backend = excluded.backend,
                provider = excluded.provider,
                endpoint = excluded.endpoint,
                api_key = excluded.api_key,
                login_auth = excluded.login_auth,
                enabled = excluded.enabled,
                notes = excluded.notes,
                updated_at = excluded.updated_at
            """,
            (
                provider["id"],
                provider["label"],
                provider["backend"],
                provider["provider"],
                provider["endpoint"],
                "" if use_k8s_secret_store else provider["api_key"],
                "" if use_k8s_secret_store else provider["login_auth"],
                1 if provider["enabled"] else 0,
                provider["notes"],
                now,
                now,
            ),
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM llm_provider_configs WHERE id = ?",
            (provider["id"],),
        ).fetchone()
    if row is None:
        raise RuntimeError(f"failed to persist provider config {provider['id']}")
    return _row_to_provider_config(row)


def _route_specificity(route: dict[str, Any]) -> tuple[int, int]:
    source_score = 1 if route.get("source_pattern") != "*" else 0
    profile_score = 1 if route.get("profile_pattern") != "*" else 0
    return source_score, profile_score


def _row_to_route(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "source_pattern": str(row["source_pattern"]),
        "profile_pattern": str(row["profile_pattern"]),
        "backend": str(row["backend"]),
        "provider": str(row["provider"]),
        "provider_config_id": str(row["provider_config_id"] or ""),
        "model": str(row["model"] or ""),
        "base_url": str(row["base_url"] or ""),
        "api_key_env": str(row["api_key_env"] or ""),
        "enabled": bool(row["enabled"]),
        "priority": int(row["priority"] or 100),
        "notes": str(row["notes"] or ""),
        "created_at": float(row["created_at"]),
        "updated_at": float(row["updated_at"]),
    }


def _seed_default_llm_routes(conn: sqlite3.Connection) -> None:
    now = _now()
    for route in _default_llm_routes():
        conn.execute(
            """
            INSERT OR IGNORE INTO llm_routes (
                id, source_pattern, profile_pattern, backend, provider, provider_config_id,
                model, base_url, api_key_env, enabled, priority, notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                route["id"],
                route["source_pattern"],
                route["profile_pattern"],
                route["backend"],
                route["provider"],
                route.get("provider_config_id") or "",
                route["model"],
                route["base_url"],
                route["api_key_env"],
                1 if route["enabled"] else 0,
                route["priority"],
                route["notes"],
                now,
                now,
            ),
        )


def _list_llm_routes(*, include_disabled: bool = True) -> list[dict[str, Any]]:
    query = "SELECT * FROM llm_routes"
    params: tuple[Any, ...] = ()
    if not include_disabled:
        query += " WHERE enabled = 1"
    query += " ORDER BY priority DESC, id ASC"
    with _db_lock:
        rows = _db().execute(query, params).fetchall()
    return [_row_to_route(row) for row in rows]


def _validate_route_payload(route_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    normalized_id = _normalize_route_id(route_id)
    source_pattern = _normalize_route_pattern(payload.get("source_pattern"))
    profile_pattern = _normalize_route_pattern(payload.get("profile_pattern"))
    if profile_pattern != "*" and profile_pattern not in WATCHDOG_ALLOWED_PROFILES:
        raise ValueError(f"unsupported profile pattern: {profile_pattern}")

    provider_config_id = str(payload.get("provider_config_id") or "").strip()
    provider_config = _get_llm_provider_config(provider_config_id) if provider_config_id else None
    if provider_config_id and provider_config is None:
        raise ValueError(f"unknown provider config: {provider_config_id}")

    backend = str(
        payload.get("backend")
        or (provider_config.get("backend") if provider_config else "exo")
    ).strip().lower()
    if backend not in WATCHDOG_ALLOWED_BACKENDS:
        raise ValueError(f"unsupported backend: {backend}")

    provider_default = str(
        (provider_config.get("provider") if provider_config else None)
        or ("exo" if backend == "exo" else "gateway")
    ).strip()
    provider = str(payload.get("provider") or provider_default).strip()[:80]
    model = str(payload.get("model") or "").strip()[:240]
    if backend == "exo":
        model = ""
    base_url = str(payload.get("base_url") or "").strip().rstrip("/")
    api_key_env = str(payload.get("api_key_env") or "").strip()[:120]
    if api_key_env and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", api_key_env):
        raise ValueError("api_key_env must be a valid environment variable name")
    if (
        backend not in {"exo", "codex_cli", "claude_code_cli"}
        and not (base_url or (provider_config and provider_config.get("endpoint")))
    ):
        raise ValueError("managed routes require a provider endpoint")

    try:
        priority = int(payload.get("priority", 100))
    except (TypeError, ValueError) as exc:
        raise ValueError("priority must be an integer") from exc

    return {
        "id": normalized_id,
        "source_pattern": source_pattern,
        "profile_pattern": profile_pattern,
        "backend": backend,
        "provider": provider or provider_default,
        "provider_config_id": provider_config_id,
        "model": model,
        "base_url": base_url,
        "api_key_env": api_key_env,
        "enabled": bool(payload.get("enabled", True)),
        "priority": max(0, min(priority, 10_000)),
        "notes": str(payload.get("notes") or "").strip()[:500],
    }


def _upsert_llm_route(route_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    route = _validate_route_payload(route_id, payload)
    now = _now()
    with _db_lock:
        conn = _db()
        existing = conn.execute(
            "SELECT created_at FROM llm_routes WHERE id = ?",
            (route["id"],),
        ).fetchone()
        created_at = float(existing["created_at"]) if existing else now
        conn.execute(
            """
            INSERT INTO llm_routes (
                id, source_pattern, profile_pattern, backend, provider, provider_config_id,
                model, base_url, api_key_env, enabled, priority, notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                source_pattern = excluded.source_pattern,
                profile_pattern = excluded.profile_pattern,
                backend = excluded.backend,
                provider = excluded.provider,
                provider_config_id = excluded.provider_config_id,
                model = excluded.model,
                base_url = excluded.base_url,
                api_key_env = excluded.api_key_env,
                enabled = excluded.enabled,
                priority = excluded.priority,
                notes = excluded.notes,
                updated_at = excluded.updated_at
            """,
            (
                route["id"],
                route["source_pattern"],
                route["profile_pattern"],
                route["backend"],
                route["provider"],
                route["provider_config_id"],
                route["model"],
                route["base_url"],
                route["api_key_env"],
                1 if route["enabled"] else 0,
                route["priority"],
                route["notes"],
                created_at,
                now,
            ),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM llm_routes WHERE id = ?", (route["id"],)).fetchone()
    if row is None:
        raise RuntimeError(f"failed to persist route {route['id']}")
    return _row_to_route(row)


def _delete_llm_route(route_id: str) -> bool:
    normalized_id = _normalize_route_id(route_id)
    with _db_lock:
        conn = _db()
        cursor = conn.execute("DELETE FROM llm_routes WHERE id = ?", (normalized_id,))
        conn.commit()
    return bool(cursor.rowcount)


def _match_llm_route(source: str, profile: str) -> dict[str, Any]:
    requested_source = str(source or "unknown").strip() or "unknown"
    requested_profile = str(profile or "generic_chat").strip() or "generic_chat"
    routes = _list_llm_routes(include_disabled=False)
    matches = [
        route
        for route in routes
        if route["enabled"]
        and route["source_pattern"] in {"*", requested_source}
        and route["profile_pattern"] in {"*", requested_profile}
    ]
    if not matches:
        return {
            "id": "runtime-fallback",
            "source_pattern": "*",
            "profile_pattern": "*",
            "backend": "exo",
            "provider": "exo",
            "provider_config_id": "exo",
            "model": "",
            "base_url": "",
            "api_key_env": "",
            "enabled": True,
            "priority": 0,
            "notes": "Synthetic runtime fallback.",
            "created_at": _now(),
            "updated_at": _now(),
        }
    matches.sort(
        key=lambda route: (
            route["priority"],
            *_route_specificity(route),
            route["updated_at"],
        ),
        reverse=True,
    )
    return matches[0]


def _resolve_llm_route(
    source: str,
    profile: str,
    requested_model: str | None = None,
) -> dict[str, Any]:
    route = dict(_match_llm_route(source, profile))
    provider_config_id = str(route.get("provider_config_id") or "").strip()
    provider_config = (
        _get_llm_provider_config(provider_config_id)
        if provider_config_id
        else None
    )
    if provider_config and not provider_config.get("enabled", True):
        raise ValueError(f"provider config {provider_config_id} is disabled")

    backend = str(
        (provider_config.get("backend") if provider_config else None)
        or route.get("backend")
        or "exo"
    ).strip().lower()
    provider = str(
        (provider_config.get("provider") if provider_config else None)
        or route.get("provider")
        or "exo"
    ).strip()
    route_model = str(route.get("model") or "").strip()
    raw_requested_model = str(requested_model or "").strip()
    provider_endpoint = str(
        (provider_config.get("endpoint") if provider_config else None) or ""
    ).strip().rstrip("/")

    if backend == "exo":
        resolved_model = _parse_model_choice(route_model or raw_requested_model)
        effective_base_url = provider_endpoint
    elif backend in {"codex_cli", "claude_code_cli"}:
        resolved_model = route_model or raw_requested_model
        effective_base_url = ""
    else:
        resolved_model = route_model or raw_requested_model
        effective_base_url = provider_endpoint or str(route.get("base_url") or "").strip().rstrip("/")
        if not effective_base_url:
            raise ValueError(f"route {route['id']} is missing base_url")
        if not resolved_model:
            raise ValueError(f"route {route['id']} is missing model")

    route["backend"] = backend
    route["provider"] = provider or ("exo" if backend == "exo" else "gateway")
    route["resolved_model"] = resolved_model
    route["effective_base_url"] = effective_base_url
    route["provider_config"] = (
        {
            "id": provider_config["id"],
            "label": provider_config["label"],
            "backend": provider_config["backend"],
            "provider": provider_config["provider"],
            "endpoint": provider_config["endpoint"],
            "has_api_key": bool(provider_config.get("has_api_key")),
            "has_login_auth": bool(provider_config.get("has_login_auth")),
        }
        if provider_config
        else None
    )
    if provider_config:
        route["api_key"] = str(provider_config.get("api_key") or "")
        route["login_auth"] = str(provider_config.get("login_auth") or "")
    route["source"] = str(source or "unknown").strip() or "unknown"
    route["profile"] = str(profile or "generic_chat").strip() or "generic_chat"
    route["requested_model"] = raw_requested_model
    return route


def _record_job_event(job_id: str, event_type: str, payload: Any = None) -> None:
    with _db_lock:
        _db().execute(
            """
            INSERT INTO job_events (job_id, ts, event_type, payload_json)
            VALUES (?, ?, ?, ?)
            """,
            (job_id, _now(), event_type, _json_dumps(payload) if payload is not None else None),
        )
        _db().commit()


def _record_recovery_event(stage: str, payload: Any = None, host_name: str | None = None) -> None:
    with _db_lock:
        _db().execute(
            """
            INSERT INTO recovery_events (ts, stage, host_name, payload_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                _now(),
                stage,
                host_name,
                _json_dumps(payload) if payload is not None else None,
            ),
        )
        _db().commit()


def _latest_recovery_event_ts(stage: str) -> float | None:
    with _db_lock:
        row = _db().execute(
            "SELECT MAX(ts) AS ts FROM recovery_events WHERE stage = ?",
            (stage,),
        ).fetchone()
    if not row or row["ts"] is None:
        return None
    return float(row["ts"])


def _daily_restart_schedule_label() -> str:
    return (
        f"{WATCHDOG_DAILY_RESTART_HOUR:02d}:{WATCHDOG_DAILY_RESTART_MINUTE:02d} "
        f"{WATCHDOG_DAILY_RESTART_TZ_NAME}"
    )


def _peer_hosts_then_control() -> list[RemoteHost]:
    hosts = list(_peer_remote_hosts())
    control = _control_remote_host()
    if control is not None:
        hosts.append(control)
    return hosts


def _refresh_queue_metrics() -> None:
    with _db_lock:
        conn = _db()
        queue_depth = int(
            conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE status IN ('queued', 'running')"
            ).fetchone()[0]
        )
        queued_row = conn.execute(
            "SELECT MIN(queued_at) AS oldest FROM jobs WHERE status = 'queued'"
        ).fetchone()

    _state.queue_depth = queue_depth
    if queued_row and queued_row["oldest"]:
        _state.oldest_waiting_age_seconds = max(0.0, _now() - float(queued_row["oldest"]))
    else:
        _state.oldest_waiting_age_seconds = None


def _normalize_client_request_id(raw: Any, source: str) -> str:
    value = str(raw or "").strip()
    if value:
        return value[:200]
    source_slug = re.sub(r"[^A-Za-z0-9_.:-]+", "-", source).strip("-") or "watchdog"
    return f"{source_slug}:{uuid.uuid4()}"


def _job_headers(snapshot: dict[str, Any] | None) -> dict[str, str]:
    if not snapshot:
        return {}
    headers: dict[str, str] = {}
    job_id = str(snapshot.get("id") or "").strip()
    client_request_id = str(snapshot.get("client_request_id") or "").strip()
    if job_id:
        headers["X-Watchdog-Job-Id"] = job_id
    if client_request_id:
        headers["X-Client-Request-Id"] = client_request_id
    return headers


def _recover_jobs_after_watchdog_restart() -> None:
    now = _now()
    with _db_lock:
        conn = _db()
        rows = conn.execute(
            "SELECT id FROM jobs WHERE status = 'running' ORDER BY created_at"
        ).fetchall()
        job_ids = [row["id"] for row in rows]
        if job_ids:
            conn.executemany(
                """
                UPDATE jobs
                SET status = 'queued', started_at = NULL, queued_at = ?
                WHERE id = ?
                """,
                [(now, job_id) for job_id in job_ids],
            )
            conn.executemany(
                """
                INSERT INTO job_events (job_id, ts, event_type, payload_json)
                VALUES (?, ?, 'requeued_after_watchdog_restart', ?)
                """,
                [(job_id, now, _json_dumps({"reason": "watchdog_restart"})) for job_id in job_ids],
            )
        conn.commit()

    for job_id in job_ids:
        _job_event(job_id)
    if job_ids:
        _queue_wakeup.set()
    _refresh_queue_metrics()


def _row_to_job_snapshot(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "kind": row["kind"],
        "source": row["source"],
        "profile": row["profile"],
        "client_request_id": row["client_request_id"],
        "status": row["status"],
        "input_chars": row["input_chars"],
        "estimated_tokens": row["estimated_tokens"],
        "truncated_fields": _json_loads(row["truncated_fields_json"], []),
        "model": row["model"],
        "created_at": row["created_at"],
        "queued_at": row["queued_at"],
        "started_at": row["started_at"],
        "completed_at": row["completed_at"],
        "wait_count": row["wait_count"],
        "result": _json_loads(row["response_json"], None),
        "error": _json_loads(row["error_json"], None),
    }


def _get_job_snapshot(job_id: str) -> dict[str, Any] | None:
    with _db_lock:
        row = _db().execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return _row_to_job_snapshot(row) if row else None


def _enqueue_job(
    *,
    kind: str,
    source: str,
    profile: str,
    client_request_id: str,
    payload: dict[str, Any],
    input_chars: int,
    estimated_tokens: int,
    truncated_fields: list[str],
    model: str | None,
) -> dict[str, Any]:
    job_id = str(uuid.uuid4())
    now = _now()
    with _db_lock:
        _db().execute(
            """
            INSERT INTO jobs (
                id, kind, source, profile, status, request_json, response_json, error_json,
                input_chars, estimated_tokens, truncated_fields_json, model, client_request_id,
                created_at, queued_at, started_at, completed_at, wait_count
            ) VALUES (?, ?, ?, ?, 'queued', ?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 0)
            """,
            (
                job_id,
                kind,
                source,
                profile,
                _json_dumps(payload),
                input_chars,
                estimated_tokens,
                _json_dumps(truncated_fields),
                model,
                client_request_id,
                now,
                now,
            ),
        )
        _db().execute(
            """
            INSERT INTO job_events (job_id, ts, event_type, payload_json)
            VALUES (?, ?, 'queued', ?)
            """,
            (
                job_id,
                now,
                _json_dumps(
                    {
                        "source": source,
                        "profile": profile,
                        "client_request_id": client_request_id,
                    }
                ),
            ),
        )
        _db().commit()
    _refresh_queue_metrics()
    _queue_wakeup.set()
    return _get_job_snapshot(job_id) or {"id": job_id, "status": "queued"}


def _claim_next_job() -> dict[str, Any] | None:
    now = _now()
    with _db_lock:
        conn = _db()
        row = conn.execute(
            "SELECT * FROM jobs WHERE status = 'queued' ORDER BY created_at ASC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        job_id = row["id"]
        conn.execute(
            "UPDATE jobs SET status = 'running', started_at = ? WHERE id = ?",
            (now, job_id),
        )
        conn.execute(
            """
            INSERT INTO job_events (job_id, ts, event_type, payload_json)
            VALUES (?, ?, 'started', NULL)
            """,
            (job_id, now),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()

    _state.active_job = job_id
    _refresh_queue_metrics()
    return _row_to_job_snapshot(row)


def _finish_job_success(job_id: str, result: dict[str, Any], model: str | None) -> dict[str, Any]:
    now = _now()
    with _db_lock:
        conn = _db()
        conn.execute(
            """
            UPDATE jobs
            SET status = 'completed', response_json = ?, error_json = NULL,
                model = ?, completed_at = ?
            WHERE id = ?
            """,
            (_json_dumps(result), model, now, job_id),
        )
        conn.execute(
            """
            INSERT INTO job_events (job_id, ts, event_type, payload_json)
            VALUES (?, ?, 'completed', ?)
            """,
            (job_id, now, _json_dumps({"model": model})),
        )
        conn.commit()

    _state.active_job = None
    _state.job_failures_consecutive = 0
    _state.recovery_state = "idle"
    _state.recovery_error = None
    _refresh_queue_metrics()
    _job_event(job_id).set()
    return _get_job_snapshot(job_id) or {"id": job_id, "status": "completed"}


def _finish_job_failure(job_id: str, error: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    with _db_lock:
        conn = _db()
        conn.execute(
            """
            UPDATE jobs
            SET status = 'failed', error_json = ?, completed_at = ?
            WHERE id = ?
            """,
            (_json_dumps(error), now, job_id),
        )
        conn.execute(
            """
            INSERT INTO job_events (job_id, ts, event_type, payload_json)
            VALUES (?, ?, 'failed', ?)
            """,
            (job_id, now, _json_dumps(error)),
        )
        conn.commit()

    _state.active_job = None
    _state.job_failures_consecutive += 1
    _refresh_queue_metrics()
    _job_event(job_id).set()
    return _get_job_snapshot(job_id) or {"id": job_id, "status": "failed"}


def _increment_wait_count(job_id: str) -> None:
    with _db_lock:
        _db().execute("UPDATE jobs SET wait_count = wait_count + 1 WHERE id = ?", (job_id,))
        _db().commit()


def _host_state_update(host_name: str, *, ts: float | None, result: Any = None, error: str | None = None) -> None:
    with _db_lock:
        _db().execute(
            """
            UPDATE host_state
            SET last_restart_ts = ?, last_result_json = ?, last_error = ?
            WHERE host_name = ?
            """,
            (
                ts,
                _json_dumps(result) if result is not None else None,
                error,
                host_name,
            ),
        )
        _db().commit()


def _update_host_runner_state(runner_candidates: dict[str, list[str]]) -> None:
    host_to_runners: dict[str, list[str]] = {host.name: [] for host in REMOTE_HOSTS}
    for runner_id, candidates in runner_candidates.items():
        for host in REMOTE_HOSTS:
            if host.ip in candidates:
                host_to_runners[host.name].append(runner_id)
    with _db_lock:
        conn = _db()
        for host in REMOTE_HOSTS:
            conn.execute(
                "UPDATE host_state SET runner_ids_json = ? WHERE host_name = ?",
                (_json_dumps(sorted(host_to_runners[host.name])), host.name),
            )
        conn.commit()


# ---------------------------------------------------------------------------
# SSH runtime
# ---------------------------------------------------------------------------

def _prepare_ssh_runtime() -> None:
    WATCHDOG_SSH_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    WATCHDOG_SSH_KNOWN_HOSTS.parent.mkdir(parents=True, exist_ok=True)

    config_lines = [
        "Host *",
        "  BatchMode yes",
        "  ServerAliveInterval 10",
        "  ServerAliveCountMax 3",
        "  ConnectTimeout 10",
        "  StrictHostKeyChecking yes",
        f"  UserKnownHostsFile {WATCHDOG_SSH_KNOWN_HOSTS}",
        "  IdentitiesOnly yes",
    ]
    if WATCHDOG_SSH_IDENTITY_FILE.exists():
        config_lines.append(f"  IdentityFile {WATCHDOG_SSH_IDENTITY_FILE}")
    WATCHDOG_SSH_CONFIG.write_text("\n".join(config_lines) + "\n", encoding="utf-8")

    if WATCHDOG_SSH_KNOWN_HOSTS.exists() and WATCHDOG_SSH_KNOWN_HOSTS.stat().st_size > 0:
        return

    # Scan both health_host IPs and ssh_target IPs for known_hosts
    ssh_ips = set()
    for host in REMOTE_HOSTS:
        ssh_ips.add(host.ip)
        target_ip = host.ssh_target.split("@", 1)[-1].split(":", 1)[0]
        ssh_ips.add(target_ip)
    ips = sorted(ssh_ips)
    if not ips:
        return

    try:
        proc = subprocess.run(
            ["ssh-keyscan", "-H", *ips],
            check=True,
            capture_output=True,
            text=True,
            timeout=20,
        )
        WATCHDOG_SSH_KNOWN_HOSTS.write_text(proc.stdout, encoding="utf-8")
        log.info("Prepared SSH known_hosts for %s", ", ".join(ips))
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to prepare SSH known_hosts: %s", exc)


def _ssh_enabled() -> bool:
    return bool(REMOTE_HOSTS) and WATCHDOG_SSH_CONFIG.exists() and WATCHDOG_SSH_IDENTITY_FILE.exists()


# ---------------------------------------------------------------------------
# Llama.cpp API helpers
# ---------------------------------------------------------------------------

def _should_force_no_think(model: str, requested: dict[str, Any]) -> bool:
    if "qwen3" not in (model or "").lower():
        return False
    enable_thinking = requested.get("enable_thinking")
    if isinstance(enable_thinking, str):
        return enable_thinking.strip().lower() not in {"1", "true", "yes", "on"}
    return bool(enable_thinking) is False


def _inject_no_think(messages: Any) -> Any:
    if not isinstance(messages, list):
        return messages
    patched: list[Any] = [{"role": "system", "content": "/no_think"}]
    inserted = True
    for message in messages:
        if not isinstance(message, dict):
            patched.append(message)
            continue
        next_message = dict(message)
        content = next_message.get("content")
        role = str(next_message.get("role") or "")
        if (
            not inserted
            and role in {"system", "user"}
            and isinstance(content, str)
            and "/no_think" not in content
        ):
            next_message["content"] = f"/no_think\n{content}"
            inserted = True
        patched.append(next_message)
    return patched

def _sanitize_chat_request(
    request_body: dict[str, Any],
    *,
    allow_blank_model: bool = False,
) -> dict[str, Any]:
    request = dict(request_body)
    requested_enable_thinking = request.get("enable_thinking")
    for key in (
        "enable_thinking",
        "watchdog_profile",
        "watchdog_input",
        "source",
        "client_request_id",
        "kind",
        "profile",
    ):
        request.pop(key, None)
    request["model"] = str(request.get("model") or "").strip()
    if not request["model"] and not allow_blank_model:
        raise RuntimeError("chat request is missing model")
    if _should_force_no_think(
        request["model"],
        {"enable_thinking": requested_enable_thinking},
    ):
        request["messages"] = _inject_no_think(request.get("messages"))
    request.setdefault("stream", False)
    return request


def _text_from_chat_message_content(content: Any) -> str:
    if isinstance(content, list):
        text = "\n".join(
            str(block.get("text") or "")
            for block in content
            if isinstance(block, dict) and block.get("type") == "text"
        ).strip()
        return text
    return str(content or "").strip()


async def _exo_json_request(
    client: httpx.AsyncClient,
    base_url: str,
    path: str,
    *,
    timeout: float = WATCHDOG_HTTP_TIMEOUT_SECONDS,
    allow_statuses: set[int] | None = None,
) -> tuple[int, Any, str]:
    response = await client.get(f"{base_url.rstrip('/')}{path}", timeout=timeout)
    text = response.text
    allowed = allow_statuses or {200}
    if response.status_code not in allowed:
        response.raise_for_status()
    try:
        data = response.json() if text else None
    except json.JSONDecodeError:
        data = None
    return response.status_code, data, text


def _parse_models_payload(payload: Any) -> list[str]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            models = [
                str(item.get("id") or "").strip()
                for item in data
                if isinstance(item, dict)
            ]
            return [model for model in models if model]
        models = payload.get("models")
        if isinstance(models, list):
            return [str(model).strip() for model in models if str(model).strip()]
    if isinstance(payload, list):
        return [str(model).strip() for model in payload if str(model).strip()]
    return []


def _dedupe_models(values: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _provider_api_key_env(provider_config_id: str) -> str:
    normalized_id = _normalize_provider_config_id(provider_config_id)
    routes = [
        route
        for route in _list_llm_routes()
        if str(route.get("provider_config_id") or "").strip() == normalized_id
    ]
    routes.sort(
        key=lambda route: (
            int(route.get("priority") or 0),
            float(route.get("updated_at") or 0.0),
        ),
        reverse=True,
    )
    for route in routes:
        api_key_env = str(route.get("api_key_env") or "").strip()
        if api_key_env:
            return api_key_env
    return ""


def _provider_fallback_models(provider_config: dict[str, Any]) -> list[str]:
    provider_config_id = str(provider_config.get("id") or "").strip()
    route_models = [
        str(route.get("model") or "").strip()
        for route in _list_llm_routes()
        if str(route.get("provider_config_id") or "").strip() == provider_config_id
    ]
    if str(provider_config.get("backend") or "").strip().lower() == "exo":
        return _dedupe_models(
            list(_state.models or [])
            + list(_state.last_known_models or [])
            + [EXO_DESIRED_MODEL]
            + route_models
        )
    if str(provider_config.get("provider") or "").strip().lower() == "minimax":
        return _dedupe_models(
            [
                "MiniMax-M2.7-highspeed",
                "MiniMax-M2.7",
                "MiniMax-M2.5-highspeed",
                "MiniMax-M2.5",
                "MiniMax-M2.1-highspeed",
                "MiniMax-M2.1",
                "MiniMax-M2",
            ]
            + route_models
        )
    if str(provider_config.get("backend") or "").strip().lower() == "codex_cli":
        return _dedupe_models(
            _read_codex_model_slugs()
            + ["gpt-5.4", "gpt-5", "gpt-5-codex", "gpt-5-codex-mini"]
            + route_models
        )
    if str(provider_config.get("backend") or "").strip().lower() == "claude_code_cli":
        return _dedupe_models(
            ["sonnet", "opus", "claude-sonnet-4-6", "claude-opus-4-1"] + route_models
        )
    return _dedupe_models(route_models)


def _read_codex_model_slugs() -> list[str]:
    source = _host_codex_models_cache_path()
    if not source.exists():
        return []
    try:
        data = json.loads(source.read_text())
    except Exception:  # noqa: BLE001
        return []
    raw_models = data.get("models") if isinstance(data, dict) else []
    if not isinstance(raw_models, list):
        return []
    parsed: list[str] = []
    for item in raw_models:
        if not isinstance(item, dict):
            continue
        slug = str(item.get("slug") or item.get("id") or "").strip()
        if slug:
            parsed.append(slug)
    return _dedupe_models(parsed)


async def _fetch_exo_provider_models() -> tuple[list[str], str]:
    ordered_bases = _dedupe_models(
        [_state.api_endpoint, EXO_CONTROL_ENDPOINT] + list(EXO_ENDPOINTS)
    )
    async with httpx.AsyncClient(timeout=WATCHDOG_HTTP_TIMEOUT_SECONDS) as client:
        for base_url in ordered_bases:
            try:
                _, payload, _ = await _exo_json_request(
                    client,
                    base_url,
                    "/models",
                    allow_statuses={200},
                )
                models = _dedupe_models(_parse_models_payload(payload))
                if models:
                    return models, "live"
            except Exception:  # noqa: BLE001
                continue
    state_models = _dedupe_models(list(_state.models or []) + list(_state.last_known_models or []))
    if state_models:
        return state_models, "watchdog_state"
    return [], "fallback"


async def _fetch_openai_compatible_provider_models(
    provider_config: dict[str, Any],
) -> tuple[list[str], str]:
    base_url = str(provider_config.get("endpoint") or "").strip().rstrip("/")
    if not base_url:
        return [], "fallback"
    model_urls = _dedupe_models(
        [f"{base_url}/models"]
        + ([f"{base_url}/v1/models"] if not base_url.endswith("/v1") else [])
    )
    headers: dict[str, str] = {}
    _apply_managed_auth_headers(
        headers,
        {
            "api_key": str(provider_config.get("api_key") or ""),
            "login_auth": str(provider_config.get("login_auth") or ""),
            "api_key_env": _provider_api_key_env(str(provider_config.get("id") or "")),
        },
        auth_kind="openai",
    )
    async with httpx.AsyncClient(timeout=WATCHDOG_HTTP_TIMEOUT_SECONDS) as client:
        last_error: Exception | None = None
        for model_url in model_urls:
            try:
                response = await client.get(model_url, headers=headers)
                response.raise_for_status()
                models = _dedupe_models(_parse_models_payload(response.json()))
                if models:
                    return models, "live"
            except Exception as exc:  # noqa: BLE001
                last_error = exc
        if last_error is not None:
            raise last_error
    return [], "fallback"


async def _fetch_codex_cli_provider_models(
    provider_config: dict[str, Any],
) -> tuple[list[str], str]:
    del provider_config
    models = _read_codex_model_slugs()
    if models:
        return models, "local_cache"
    return [], "fallback"


async def _fetch_claude_code_cli_provider_models(
    provider_config: dict[str, Any],
) -> tuple[list[str], str]:
    del provider_config
    return ["sonnet", "opus", "claude-sonnet-4-6", "claude-opus-4-1"], "cli_aliases"


async def _fetch_provider_models(
    provider_config: dict[str, Any],
) -> tuple[list[str], str]:
    backend = str(provider_config.get("backend") or "").strip().lower()
    if backend == "exo":
        return await _fetch_exo_provider_models()
    if backend == "openai_compatible":
        return await _fetch_openai_compatible_provider_models(provider_config)
    if backend == "codex_cli":
        return await _fetch_codex_cli_provider_models(provider_config)
    if backend == "claude_code_cli":
        return await _fetch_claude_code_cli_provider_models(provider_config)
    return [], "fallback"


async def _tcp_probe(host: str, port: int) -> tuple[bool, str | None]:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=WATCHDOG_TCP_PROBE_TIMEOUT_SECONDS,
        )
        writer.close()
        await writer.wait_closed()
        del reader
        return True, None
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def _candidate_api_bases() -> list[str]:
    ordered: list[str] = []
    for value in [_state.api_endpoint, EXO_CONTROL_ENDPOINT, *EXO_ENDPOINTS]:
        if value and value not in ordered:
            ordered.append(value)
    healthy = [
        data.get("endpoint")
        for data in _state.peer_health.values()
        if data.get("reachable") and data.get("endpoint")
    ]
    for value in healthy:
        if value and value not in ordered:
            ordered.append(value)
    return ordered


def _parse_iso_age_seconds(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return max(0.0, _now() - float(value))
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, _now() - dt.timestamp())


def _node_ip_map_from_state(state_payload: dict[str, Any]) -> dict[str, str]:
    node_network = state_payload.get("nodeNetwork") or state_payload.get("node_network") or {}
    configured = set(_configured_host_ips())
    mapping: dict[str, str] = {}
    if not isinstance(node_network, dict):
        return mapping

    for node_id, info in node_network.items():
        if not isinstance(info, dict):
            continue
        interfaces = info.get("interfaces") or []
        thunderbolt_ip = None
        fallback_ip = None
        configured_ip = None
        for interface in interfaces:
            if not isinstance(interface, dict):
                continue
            ip_address = str(
                interface.get("ipAddress")
                or interface.get("ip_address")
                or ""
            ).strip()
            if not ip_address:
                continue
            iface_type = str(
                interface.get("interfaceType")
                or interface.get("interface_type")
                or "unknown"
            ).strip()
            if ip_address in configured:
                configured_ip = ip_address
            if iface_type == "thunderbolt" and thunderbolt_ip is None:
                thunderbolt_ip = ip_address
            if fallback_ip is None:
                fallback_ip = ip_address
        selected_ip = thunderbolt_ip or configured_ip or fallback_ip
        if selected_ip:
            mapping[str(node_id)] = selected_ip
    return mapping


def _extract_state_instances(state_payload: dict[str, Any]) -> list[dict[str, Any]]:
    node_ip_map = _node_ip_map_from_state(state_payload)
    instances_raw = state_payload.get("instances") or {}
    parsed: list[dict[str, Any]] = []
    if not isinstance(instances_raw, dict):
        return parsed

    for instance_id, wrapped in instances_raw.items():
        if not isinstance(wrapped, dict):
            continue
        inner = None
        instance_type = None
        for key in ("MlxRingInstance", "MlxJacclInstance"):
            value = wrapped.get(key)
            if isinstance(value, dict):
                inner = value
                instance_type = key
                break
        if inner is None:
            inner = wrapped
            instance_type = str(wrapped.get("type") or "Unknown")

        shard_assignments = inner.get("shardAssignments") or inner.get("shard_assignments") or {}
        model_id = str(shard_assignments.get("modelId") or shard_assignments.get("model_id") or "").strip()
        node_to_runner = shard_assignments.get("nodeToRunner") or shard_assignments.get("node_to_runner") or {}
        node_ids = [str(node_id).strip() for node_id in node_to_runner.keys() if str(node_id).strip()]
        hosts = [node_ip_map[node_id] for node_id in node_ids if node_id in node_ip_map]
        parsed.append(
            {
                "instance_id": str(instance_id),
                "type": instance_type,
                "model": model_id,
                "node_ids": node_ids,
                "hosts": list(dict.fromkeys(hosts)),
                "node_to_runner": {str(k): str(v) for k, v in node_to_runner.items()},
            }
        )
    return parsed


async def _fetch_endpoint_state(
    client: httpx.AsyncClient,
    endpoint: str,
) -> dict[str, Any]:
    host = urlsplit(endpoint).hostname or endpoint
    probe: dict[str, Any] = {
        "endpoint": endpoint,
        "ip": host,
        "reachable": False,
        "healthy": False,
        "status_code": None,
        "last_error": None,
    }
    try:
        status_code, payload, _ = await _exo_json_request(
            client,
            endpoint,
            "/state",
            allow_statuses={200},
        )
        probe["status_code"] = status_code
        probe["reachable"] = True
        probe["healthy"] = status_code == 200 and isinstance(payload, dict)
        probe["payload"] = payload if isinstance(payload, dict) else {}
    except Exception as exc:  # noqa: BLE001
        probe["last_error"] = str(exc)
    return probe


async def _call_exo_chat(
    request_body: dict[str, Any],
    *,
    stall_timeout: float | None = None,
    ensure_model_loaded: bool = True,
    preferred_base_url: str | None = None,
) -> dict[str, Any]:
    timeout_seconds = stall_timeout or WATCHDOG_JOB_STALL_SECONDS
    payload = _sanitize_chat_request(request_body)
    if ensure_model_loaded and not await _ensure_desired_model_loaded():
        raise RuntimeError(f"desired exo model {EXO_DESIRED_MODEL} is not active")
    async with httpx.AsyncClient(timeout=None) as client:
        errors: list[str] = []
        candidate_bases = _candidate_api_bases()
        if preferred_base_url:
            ordered_bases = [preferred_base_url.rstrip("/")]
            ordered_bases.extend(base for base in candidate_bases if base.rstrip("/") != preferred_base_url.rstrip("/"))
        else:
            ordered_bases = candidate_bases
        for base_url in ordered_bases:
            try:
                response = await asyncio.wait_for(
                    client.post(
                        f"{base_url.rstrip('/')}/v1/chat/completions",
                        json=payload,
                    ),
                    timeout=timeout_seconds,
                )
                response.raise_for_status()
                data = response.json()
                content = _strip_think(_content_from_openai_response(data))
                if not content and int(payload.get("max_tokens") or 0) > 0:
                    raise RuntimeError("exo returned empty response")
                _state.api_endpoint = base_url
                return data
            except asyncio.TimeoutError:
                errors.append(f"{base_url}: stalled beyond {int(timeout_seconds)}s")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{base_url}: {exc}")
        raise RuntimeError("; ".join(errors) or "exo chat failed")


def _apply_managed_auth_headers(
    headers: dict[str, str],
    route: dict[str, Any],
    *,
    auth_kind: str,
) -> None:
    api_key = str(route.get("api_key") or "").strip()
    api_key_env = str(route.get("api_key_env") or "").strip()
    if not api_key and api_key_env:
        api_key = os.getenv(api_key_env, "").strip()
        if not api_key:
            raise RuntimeError(
                f"route {route.get('id') or 'unknown'} requires env var {api_key_env}"
            )

    if api_key:
        if auth_kind == "anthropic":
            headers["x-api-key"] = api_key
        else:
            headers["Authorization"] = f"Bearer {api_key}"

    login_auth = str(route.get("login_auth") or "").strip()
    if login_auth:
        headers["X-Login-Auth"] = login_auth


def _apply_openai_compatible_request_overrides(
    payload: dict[str, Any],
    route: dict[str, Any],
) -> dict[str, Any]:
    provider = str(route.get("provider") or route.get("provider_config_id") or "").strip().lower()
    if provider != "minimax":
        return payload

    adjusted = dict(payload)
    try:
        requested_max_tokens = int(adjusted.get("max_tokens") or 0)
    except (TypeError, ValueError):
        requested_max_tokens = 0
    if requested_max_tokens < WATCHDOG_MINIMAX_MIN_OUTPUT_TOKENS:
        adjusted["max_tokens"] = WATCHDOG_MINIMAX_MIN_OUTPUT_TOKENS
    return adjusted


def _cli_prompt_from_openai_request(payload: dict[str, Any]) -> tuple[str | None, str]:
    system_prompt, messages = _anthropic_messages_from_openai_request(payload)
    lines: list[str] = []
    if system_prompt:
        lines.extend(["SYSTEM:", system_prompt.strip(), ""])
    lines.append("CONVERSATION:")
    for message in messages:
        role = str(message.get("role") or "user").strip().lower()
        label = "ASSISTANT" if role == "assistant" else "USER"
        lines.append(f"{label}:")
        lines.append(str(message.get("content") or "").strip())
        lines.append("")
    lines.append("Return only the next assistant message. Do not use tools.")
    return system_prompt, "\n".join(lines).strip()


def _cli_subprocess_env() -> dict[str, str]:
    _prepare_cli_runtime()
    env = os.environ.copy()
    env["HOME"] = str(WATCHDOG_CLI_HOME)
    env["CODEX_HOME"] = str(WATCHDOG_CODEX_HOME)
    env["PATH"] = ":".join(
        entry
        for entry in [str(WATCHDOG_HOST_NODE_ROOT / "bin"), env.get("PATH", "")]
        if entry
    )
    return env


def _require_cli_provider_ready(provider_config_id: str, label: str) -> None:
    status = _cli_provider_status(provider_config_id, force=True)
    if not status.get("binary_present"):
        raise WatchdogExecutionError(
            f"{label} binary is not available on this host",
            status_code=503,
            error_type="missing_binary",
        )
    if not status.get("auth_present"):
        raise WatchdogExecutionError(
            f"{label} is not authenticated on this host",
            status_code=503,
            error_type="missing_auth",
        )


async def _run_cli_command(
    args: list[str],
    *,
    cwd: Path,
    timeout_seconds: float,
) -> tuple[int, str, str]:
    try:
        process = await asyncio.create_subprocess_exec(
            *args,
            cwd=str(cwd),
            env=_cli_subprocess_env(),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError as exc:
        raise WatchdogExecutionError(
            f"CLI binary not found: {args[0]}",
            status_code=503,
            error_type="missing_binary",
        ) from exc

    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError as exc:
        process.kill()
        await process.communicate()
        raise WatchdogExecutionError(
            f"CLI request timed out after {int(timeout_seconds)}s",
            status_code=504,
            error_type="timeout",
        ) from exc
    return (
        int(process.returncode or 0),
        stdout.decode("utf-8", errors="replace"),
        stderr.decode("utf-8", errors="replace"),
    )


def _raise_cli_failure(
    label: str,
    returncode: int,
    stdout_text: str,
    stderr_text: str,
) -> None:
    combined = "\n".join(part for part in [stderr_text.strip(), stdout_text.strip()] if part).strip()
    lower = combined.lower()
    if any(token in lower for token in ["not authenticated", "not logged in", "login", "auth required"]):
        raise WatchdogExecutionError(
            f"{label} is not authenticated on this host",
            status_code=503,
            error_type="missing_auth",
        )
    detail = combined or f"{label} exited with code {returncode}"
    raise WatchdogExecutionError(
        f"{label} command failed: {detail}",
        status_code=502,
        error_type="command_failed",
    )


async def _call_openai_compatible_chat(
    request_body: dict[str, Any],
    route: dict[str, Any],
    *,
    stall_timeout: float | None = None,
) -> dict[str, Any]:
    timeout_seconds = stall_timeout or WATCHDOG_JOB_STALL_SECONDS
    payload = _apply_openai_compatible_request_overrides(
        _sanitize_chat_request(request_body),
        route,
    )
    base_url = str(route.get("effective_base_url") or route.get("base_url") or "").strip().rstrip("/")
    if not base_url:
        raise RuntimeError(f"route {route.get('id') or 'unknown'} is missing base_url")

    headers = {"Content-Type": "application/json"}
    _apply_managed_auth_headers(headers, route, auth_kind="bearer")

    async with httpx.AsyncClient(timeout=None) as client:
        response = await asyncio.wait_for(
            client.post(
                f"{base_url}/chat/completions",
                json=payload,
                headers=headers,
            ),
            timeout=timeout_seconds,
        )
    response.raise_for_status()
    data = response.json()
    content = _content_from_openai_response(data)
    if not content and int(payload.get("max_tokens") or 0) > 0:
        raise RuntimeError("managed LLM route returned empty response")
    return data


def _anthropic_messages_from_openai_request(payload: dict[str, Any]) -> tuple[str | None, list[dict[str, str]]]:
    system_parts: list[str] = []
    messages: list[dict[str, str]] = []
    for item in payload.get("messages") or []:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "user").strip().lower()
        text = _text_from_chat_message_content(item.get("content"))
        if not text:
            continue
        if role == "system":
            system_parts.append(text)
            continue
        if role not in {"user", "assistant"}:
            role = "user"
        messages.append({"role": role, "content": text})

    if not messages:
        messages.append({"role": "user", "content": "Hello."})
    return ("\n\n".join(system_parts).strip() or None), messages


async def _call_codex_cli_chat(
    request_body: dict[str, Any],
    route: dict[str, Any],
    *,
    stall_timeout: float | None = None,
) -> dict[str, Any]:
    timeout_seconds = stall_timeout or WATCHDOG_JOB_STALL_SECONDS
    payload = _sanitize_chat_request(request_body, allow_blank_model=True)
    _require_cli_provider_ready(str(route.get("provider_config_id") or "codex-cli"), "Codex CLI")
    _, prompt = _cli_prompt_from_openai_request(payload)
    args = [
        str(_codex_binary_path()),
        "exec",
        "--json",
        "--ephemeral",
        "--skip-git-repo-check",
        "--sandbox",
        "read-only",
        "-C",
        str(WATCHDOG_CODEX_EMPTY_DIR),
        "-c",
        'model_reasoning_effort="medium"',
    ]
    model = str(payload.get("model") or route.get("resolved_model") or "").strip()
    if model:
        args.extend(["--model", model])
    args.append(prompt)
    returncode, stdout_text, stderr_text = await _run_cli_command(
        args,
        cwd=WATCHDOG_CODEX_EMPTY_DIR,
        timeout_seconds=timeout_seconds,
    )
    if returncode != 0:
        _raise_cli_failure("Codex CLI", returncode, stdout_text, stderr_text)

    content = ""
    usage_payload: dict[str, Any] = {}
    error_message = ""
    for raw_line in stdout_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        event_type = str(event.get("type") or "").strip()
        if event_type == "item.completed":
            item = event.get("item") or {}
            if str(item.get("type") or "").strip() == "agent_message":
                text = str(item.get("text") or "").strip()
                if text:
                    content = text
        elif event_type == "turn.completed":
            usage_payload = event.get("usage") or {}
        elif event_type in {"error", "turn.failed"}:
            error_message = str(event.get("message") or event.get("error") or "").strip()

    if error_message:
        _raise_cli_failure("Codex CLI", returncode, error_message, stderr_text)
    if not content and int(payload.get("max_tokens") or 0) > 0:
        raise WatchdogExecutionError(
            "Codex CLI returned an empty response",
            status_code=502,
            error_type="empty_response",
        )
    input_tokens = int(usage_payload.get("input_tokens") or 0)
    output_tokens = int(usage_payload.get("output_tokens") or 0)
    return _build_openai_response(
        model=model or "codex-cli-default",
        content=content,
        usage={
            "prompt_tokens": input_tokens,
            "completion_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
        },
    )


async def _call_claude_code_cli_chat(
    request_body: dict[str, Any],
    route: dict[str, Any],
    *,
    stall_timeout: float | None = None,
) -> dict[str, Any]:
    timeout_seconds = stall_timeout or WATCHDOG_JOB_STALL_SECONDS
    payload = _sanitize_chat_request(request_body, allow_blank_model=True)
    _require_cli_provider_ready(
        str(route.get("provider_config_id") or "claude-code-cli"),
        "Claude Code CLI",
    )
    system_prompt, prompt = _cli_prompt_from_openai_request(payload)
    args = [
        str(_claude_binary_path()),
        "-p",
        prompt,
        "--output-format",
        "json",
        "--tools",
        "",
    ]
    model = str(payload.get("model") or route.get("resolved_model") or "").strip()
    if system_prompt:
        args.extend(["--system-prompt", system_prompt])
    if model:
        args.extend(["--model", model])
    returncode, stdout_text, stderr_text = await _run_cli_command(
        args,
        cwd=WATCHDOG_CODEX_EMPTY_DIR,
        timeout_seconds=timeout_seconds,
    )
    if returncode != 0:
        _raise_cli_failure("Claude Code CLI", returncode, stdout_text, stderr_text)

    try:
        data = json.loads(stdout_text.strip())
    except json.JSONDecodeError as exc:
        raise WatchdogExecutionError(
            f"Claude Code CLI returned invalid JSON: {stdout_text.strip() or stderr_text.strip()}",
            status_code=502,
            error_type="invalid_response",
        ) from exc

    if bool(data.get("is_error")):
        _raise_cli_failure(
            "Claude Code CLI",
            returncode,
            str(data.get("result") or ""),
            stderr_text,
        )

    content = str(data.get("result") or "").strip()
    if not content and int(payload.get("max_tokens") or 0) > 0:
        raise WatchdogExecutionError(
            "Claude Code CLI returned an empty response",
            status_code=502,
            error_type="empty_response",
        )

    usage = data.get("usage") or {}
    input_tokens = int(usage.get("input_tokens") or 0)
    output_tokens = int(usage.get("output_tokens") or 0)
    resolved_model = model or next(iter((data.get("modelUsage") or {}).keys()), "") or "claude-code-cli-default"
    return _build_openai_response(
        model=resolved_model,
        content=content,
        usage={
            "prompt_tokens": input_tokens,
            "completion_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
        },
    )


async def _call_anthropic_chat(
    request_body: dict[str, Any],
    route: dict[str, Any],
    *,
    stall_timeout: float | None = None,
) -> dict[str, Any]:
    timeout_seconds = stall_timeout or WATCHDOG_JOB_STALL_SECONDS
    payload = _sanitize_chat_request(request_body)
    base_url = str(route.get("effective_base_url") or route.get("base_url") or "").strip().rstrip("/")
    if not base_url:
        raise RuntimeError(f"route {route.get('id') or 'unknown'} is missing base_url")

    system_prompt, messages = _anthropic_messages_from_openai_request(payload)
    request_payload: dict[str, Any] = {
        "model": str(payload.get("model") or route.get("resolved_model") or "").strip(),
        "messages": messages,
        "max_tokens": int(payload.get("max_tokens") or 1024),
    }
    if system_prompt:
        request_payload["system"] = system_prompt
    if payload.get("temperature") is not None:
        request_payload["temperature"] = payload.get("temperature")
    if payload.get("top_p") is not None:
        request_payload["top_p"] = payload.get("top_p")

    headers = {
        "Content-Type": "application/json",
        "anthropic-version": "2023-06-01",
    }
    _apply_managed_auth_headers(headers, route, auth_kind="anthropic")

    async with httpx.AsyncClient(timeout=None) as client:
        response = await asyncio.wait_for(
            client.post(
                f"{base_url}/messages",
                json=request_payload,
                headers=headers,
            ),
            timeout=timeout_seconds,
        )
    response.raise_for_status()
    data = response.json()
    text_parts = [
        str(block.get("text") or "")
        for block in (data.get("content") or [])
        if isinstance(block, dict) and block.get("type") == "text"
    ]
    content = "\n".join(part for part in text_parts if part).strip()
    if not content and int(request_payload.get("max_tokens") or 0) > 0:
        raise RuntimeError("managed Claude route returned empty response")

    usage = data.get("usage") or {}
    input_tokens = int(usage.get("input_tokens") or 0)
    output_tokens = int(usage.get("output_tokens") or 0)
    return _build_openai_response(
        model=str(data.get("model") or request_payload["model"]),
        content=content,
        usage={
            "prompt_tokens": input_tokens,
            "completion_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
        },
    )


async def _call_routed_chat(
    request_body: dict[str, Any],
    route: dict[str, Any],
    *,
    stall_timeout: float | None = None,
) -> dict[str, Any]:
    backend = str(route.get("backend") or "exo").strip().lower()
    if backend == "openai_compatible":
        return await _call_openai_compatible_chat(
            request_body,
            route,
            stall_timeout=stall_timeout,
        )
    if backend == "anthropic":
        return await _call_anthropic_chat(
            request_body,
            route,
            stall_timeout=stall_timeout,
        )
    if backend == "codex_cli":
        return await _call_codex_cli_chat(
            request_body,
            route,
            stall_timeout=stall_timeout,
        )
    if backend == "claude_code_cli":
        return await _call_claude_code_cli_chat(
            request_body,
            route,
            stall_timeout=stall_timeout,
        )
    return await _call_exo_chat(
        request_body,
        stall_timeout=stall_timeout,
        ensure_model_loaded=True,
        preferred_base_url=str(route.get("effective_base_url") or "").strip() or None,
    )


async def _probe_exo() -> bool:
    model = _parse_model_choice(None)
    try:
        payload = await _call_exo_chat(
            {
                "model": model,
                "messages": [{"role": "user", "content": "Reply with OK only."}],
                "temperature": 0,
                "max_tokens": 8,
            },
            stall_timeout=WATCHDOG_PROBE_TIMEOUT_SECONDS,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("Probe completion failed: %s", exc)
        return False
    return _strip_think(_content_from_openai_response(payload)).upper().startswith("OK")


async def _poll_once(trigger_recovery: bool = False) -> None:
    async with _poll_lock:
        anomalies: list[str] = []
        peer_health: dict[str, dict[str, Any]] = {}
        selected_payload: dict[str, Any] | None = None
        selected_endpoint: str | None = None

        async with httpx.AsyncClient(timeout=None) as client:
            peer_probes = await asyncio.gather(
                *[_fetch_endpoint_state(client, endpoint) for endpoint in EXO_ENDPOINTS],
                return_exceptions=False,
            )

        for probe in peer_probes:
            endpoint = str(probe.get("endpoint") or "")
            host = str(probe.get("ip") or urlsplit(endpoint).hostname or endpoint)
            peer_health[host] = probe
            if probe.get("reachable") and isinstance(probe.get("payload"), dict):
                if selected_endpoint is None:
                    selected_endpoint = endpoint
                    selected_payload = probe["payload"]
                if endpoint == _state.api_endpoint:
                    selected_endpoint = endpoint
                    selected_payload = probe["payload"]

        host_health: dict[str, dict[str, Any]] = {}
        resolved_models = list(_state.last_known_models)
        resolved_model = _state.model or EXO_DESIRED_MODEL
        instance_to_host_map = dict(_state.instance_to_host_map)
        instance_status = _state.instance_status

        if selected_payload is None or selected_endpoint is None:
            anomalies.append("exo cluster unreachable")
            for host in REMOTE_HOSTS:
                probe = peer_health.get(host.ip, {})
                host_health[host.ip] = {
                    "ip": host.ip,
                    "name": host.name,
                    "ssh_target": host.ssh_target,
                    "role": host.role,
                    "listen_port": host.listen_port,
                    "reachable": bool(probe.get("reachable")),
                    "status_code": probe.get("status_code"),
                    "last_error": probe.get("last_error"),
                    "healthy": False,
                }
            _state.last_poll_ok = False
        else:
            _state.last_poll_ok = True
            _state.api_endpoint = selected_endpoint
            _state.last_state = selected_payload

            active_instances = _extract_state_instances(selected_payload)
            active_models = [
                instance["model"]
                for instance in active_instances
                if instance.get("model")
            ]
            resolved_models = list(dict.fromkeys(active_models))
            if resolved_models:
                _state.last_known_models = list(resolved_models)
            resolved_model = (
                EXO_DESIRED_MODEL
                if EXO_DESIRED_MODEL in resolved_models
                else (resolved_models[0] if resolved_models else EXO_DESIRED_MODEL)
            )
            instance_to_host_map = {}
            for instance in active_instances:
                model_id = str(instance.get("model") or "").strip()
                if not model_id:
                    continue
                hosts = [str(host).strip() for host in instance.get("hosts") or [] if str(host).strip()]
                if hosts:
                    instance_to_host_map.setdefault(model_id, [])
                    for host_ip in hosts:
                        if host_ip not in instance_to_host_map[model_id]:
                            instance_to_host_map[model_id].append(host_ip)

            # Build runners dict {runner_id: status} and instances dict {instance_id: model_id}
            runners: dict[str, str] = {}
            instances: dict[str, str] = {}
            for instance in active_instances:
                iid = str(instance.get("instance_id") or "")
                model_id = str(instance.get("model") or "")
                if iid:
                    instances[iid] = model_id
                for _node_id, runner_val in (instance.get("node_to_runner") or {}).items():
                    runner_str = str(runner_val)
                    # runner_val may be a status string like "RunnerRunning" or a dict
                    if isinstance(runner_val, dict):
                        rid = str(runner_val.get("id") or runner_val.get("runner_id") or _node_id)
                        status = str(runner_val.get("status") or "Unknown")
                    else:
                        rid = _node_id
                        status = runner_str if runner_str else "Unknown"
                    runners[rid] = status

            node_identities = selected_payload.get("nodeIdentities") or selected_payload.get("node_identities") or {}
            node_memory = selected_payload.get("nodeMemory") or selected_payload.get("node_memory") or {}
            last_seen = selected_payload.get("lastSeen") or selected_payload.get("last_seen") or {}
            node_ip_map = _node_ip_map_from_state(selected_payload)
            ip_to_node = {ip: node_id for node_id, ip in node_ip_map.items()}

            for host in REMOTE_HOSTS:
                probe = peer_health.get(host.ip, {})
                node_id = ip_to_node.get(host.ip)
                identity = node_identities.get(node_id or "", {}) if isinstance(node_identities, dict) else {}
                memory = node_memory.get(node_id or "", {}) if isinstance(node_memory, dict) else {}
                last_seen_value = last_seen.get(node_id or "") if isinstance(last_seen, dict) else None
                last_seen_age = _parse_iso_age_seconds(last_seen_value)
                healthy = bool(probe.get("reachable")) and node_id is not None and (
                    last_seen_age is None or last_seen_age <= WATCHDOG_NODE_STALE_SECONDS
                )
                host_health[host.ip] = {
                    "ip": host.ip,
                    "name": host.name,
                    "ssh_target": host.ssh_target,
                    "role": host.role,
                    "listen_port": host.listen_port,
                    "endpoint": probe.get("endpoint"),
                    "reachable": bool(probe.get("reachable")),
                    "status_code": probe.get("status_code"),
                    "last_error": probe.get("last_error"),
                    "node_id": node_id,
                    "friendly_name": identity.get("friendlyName") if isinstance(identity, dict) else None,
                    "ram_available_bytes": _memory_field_bytes(memory, "ramAvailable"),
                    "swap_available_bytes": _memory_field_bytes(memory, "swapAvailable"),
                    "last_seen_age_seconds": last_seen_age,
                    "healthy": healthy,
                }
                if not healthy:
                    detail = probe.get("last_error") or (
                        f"last seen {int(last_seen_age)}s ago"
                        if last_seen_age is not None
                        else "not present in state"
                    )
                    anomalies.append(f"exo peer {host.ip} unhealthy: {detail}")

            if EXO_DESIRED_MODEL in instance_to_host_map:
                desired_hosts = instance_to_host_map.get(EXO_DESIRED_MODEL, [])
                if desired_hosts and all(host_health.get(host_ip, {}).get("healthy") for host_ip in desired_hosts):
                    instance_status = "active"
                else:
                    instance_status = "degraded"
                    anomalies.append(f"desired model degraded: {EXO_DESIRED_MODEL}")
            elif resolved_models:
                instance_status = "wrong_model"
                anomalies.append(
                    f"desired model not active: {EXO_DESIRED_MODEL} (active: {', '.join(resolved_models)})"
                )
            else:
                instance_status = "missing"
                anomalies.append("exo has no active instances")

        _state.last_poll_ts = _now()
        _state.anomalies = list(dict.fromkeys(anomalies))
        _state.models = resolved_models
        _state.model = resolved_model
        _state.instance_to_host_map = instance_to_host_map
        _state.instance_status = instance_status
        _state.host_health = host_health
        _state.runners = runners
        _state.instances = instances
        _state.peer_health = {
            host: {key: value for key, value in probe.items() if key != "payload"}
            for host, probe in peer_health.items()
        }
        if _state.last_poll_ok and not _state.anomalies and _state.recovery_state in {"warming_up", "polling"}:
            _state.recovery_state = "idle"
            _state.recovery_error = None

        if _state.anomalies:
            log.warning("Anomalies detected: %s", _state.anomalies)
        else:
            log.debug("exo healthy: endpoint=%s model=%s", _state.api_endpoint, resolved_model)

    if trigger_recovery and AUTO_RESTART and _state.anomalies and (
        _state.active_job is None or _should_recover_during_active_job()
    ):
        await _attempt_recovery("poll_anomaly")
    elif trigger_recovery and _state.instance_status == "missing" and _state.active_job is None:
        # Auto-place the desired model when it's missing, even without AUTO_RESTART.
        # This handles clean cluster restarts where nodes are healthy but no instance was placed.
        all_healthy = all(
            _state.host_health.get(h.ip, {}).get("healthy")
            for h in REMOTE_HOSTS
        ) if REMOTE_HOSTS else True
        if all_healthy and _switch_to_healthy_endpoint():
            base_url = _current_api_base()
            if base_url:
                try:
                    log.info("Auto-placing desired model (missing, all nodes healthy)")
                    await _place_desired_model(base_url)
                except Exception as exc:  # noqa: BLE001
                    log.warning("Auto-place failed: %s", exc)


async def _poll_loop() -> None:
    while True:
        await _poll_once(trigger_recovery=True)
        await asyncio.sleep(POLL_INTERVAL)


# ---------------------------------------------------------------------------
# Recovery logic
# ---------------------------------------------------------------------------

def _hosts_needing_restart() -> list[RemoteHost]:
    hosts: list[RemoteHost] = []
    for host in REMOTE_HOSTS:
        if not _state.host_health.get(host.ip, {}).get("healthy"):
            hosts.append(host)
    return hosts


async def _run_remote_cmd(host: RemoteHost, cmd: str) -> dict[str, Any]:
    """Execute an arbitrary command on a remote host via SSH."""
    if not _ssh_enabled():
        raise RuntimeError("SSH runtime is not configured")

    ssh_command = [
        "ssh",
        "-tt",
        "-F",
        str(WATCHDOG_SSH_CONFIG),
        host.ssh_target,
    ]
    proc = await asyncio.create_subprocess_exec(
        *ssh_command,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    steps = [step.strip() for step in cmd.split(";") if step.strip()]
    session_commands = steps + [
        "echo __WATCHDOG_CMD_DONE__:$?",
        "exit",
    ]

    async def _read_stream(stream: asyncio.StreamReader) -> str:
        chunks: list[bytes] = []
        while True:
            chunk = await stream.read(4096)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks).decode("utf-8", errors="replace").strip()

    stdout_task = asyncio.create_task(_read_stream(proc.stdout))
    stderr_task = asyncio.create_task(_read_stream(proc.stderr))

    try:
        if proc.stdin is None:
            raise RuntimeError(f"SSH stdin is unavailable for {host.name}")
        for command in session_commands:
            proc.stdin.write(f"{command}\n".encode("utf-8"))
            await proc.stdin.drain()
            await asyncio.sleep(0.35)
        proc.stdin.close()
        await proc.stdin.wait_closed()
    except Exception:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        await proc.wait()
        stdout_task.cancel()
        stderr_task.cancel()
        raise

    try:
        await asyncio.wait_for(proc.wait(), timeout=90)
    except asyncio.TimeoutError as exc:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        await proc.wait()
        raise RuntimeError(f"SSH command timed out for {host.name}") from exc

    stdout = await stdout_task
    stderr = await stderr_task

    result = {
        "host": host.name,
        "ssh_target": host.ssh_target,
        "returncode": proc.returncode,
        "stdout": stdout,
        "stderr": stderr,
    }
    if proc.returncode != 0:
        raise RuntimeError(
            f"SSH command failed for {host.name}: {result['stderr'] or result['stdout'] or proc.returncode}"
        )
    return result


async def _restart_remote_host(host: RemoteHost) -> dict[str, Any]:
    if not _ssh_enabled():
        raise RuntimeError("SSH runtime is not configured")

    ssh_command = [
        "ssh",
        "-tt",
        "-F",
        str(WATCHDOG_SSH_CONFIG),
        host.ssh_target,
    ]
    proc = await asyncio.create_subprocess_exec(
        *ssh_command,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    restart_steps = [
        step.strip()
        for step in host.restart_cmd.split(";")
        if step.strip()
    ]
    session_commands = restart_steps + [
        "echo __WATCHDOG_RESTART_DONE__:$?",
        "exit",
    ]

    async def _read_stream(stream: asyncio.StreamReader) -> str:
        chunks: list[bytes] = []
        while True:
            chunk = await stream.read(4096)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks).decode("utf-8", errors="replace").strip()

    stdout_task = asyncio.create_task(_read_stream(proc.stdout))
    stderr_task = asyncio.create_task(_read_stream(proc.stderr))

    try:
        if proc.stdin is None:
            raise RuntimeError(f"SSH stdin is unavailable for {host.name}")
        for command in session_commands:
            proc.stdin.write(f"{command}\n".encode("utf-8"))
            await proc.stdin.drain()
            await asyncio.sleep(0.35)
        proc.stdin.close()
        await proc.stdin.wait_closed()
    except Exception:
        proc.kill()
        await proc.wait()
        stdout_task.cancel()
        stderr_task.cancel()
        raise

    try:
        await asyncio.wait_for(proc.wait(), timeout=90)
    except asyncio.TimeoutError as exc:
        proc.kill()
        await proc.wait()
        raise RuntimeError(f"SSH restart timed out for {host.name}") from exc

    stdout = await stdout_task
    stderr = await stderr_task

    result = {
        "host": host.name,
        "ssh_target": host.ssh_target,
        "returncode": proc.returncode,
        "stdout": stdout,
        "stderr": stderr,
    }
    if proc.returncode != 0:
        raise RuntimeError(
            f"SSH restart failed for {host.name}: {result['stderr'] or result['stdout'] or proc.returncode}"
        )
    return result


async def _restart_hosts_until_healthy(hosts: list[RemoteHost], stage: str) -> bool:
    for host in hosts:
        try:
            result = await _restart_remote_host(host)
            _state.last_remote_restart_host = host.name
            _state.last_restart_ts = _now()
            _state.restart_count += 1
            _host_state_update(host.name, ts=_state.last_restart_ts, result=result, error=None)
            _record_recovery_event(stage, result, host_name=host.name)
        except Exception as exc:  # noqa: BLE001
            _state.recovery_error = str(exc)
            _host_state_update(host.name, ts=None, result=None, error=str(exc))
            _record_recovery_event(
                f"{stage}_failed",
                {"error": str(exc)},
                host_name=host.name,
            )
            continue

        await asyncio.sleep(WATCHDOG_REMOTE_RESTART_GRACE_SECONDS)
        await _poll_once(trigger_recovery=False)
        if _state.last_poll_ok and not _state.anomalies and await _probe_exo():
            _state.recovery_state = "idle"
            _state.recovery_error = None
            return True
    return False


async def _restart_hosts_sequential(hosts: list[RemoteHost], stage: str) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for host in hosts:
        result = await _restart_remote_host(host)
        _state.last_remote_restart_host = host.name
        _state.last_restart_ts = _now()
        _state.restart_count += 1
        _host_state_update(host.name, ts=_state.last_restart_ts, result=result, error=None)
        _record_recovery_event(stage, result, host_name=host.name)
        results.append({"host": host.name, "result": result})
        await asyncio.sleep(WATCHDOG_REMOTE_RESTART_GRACE_SECONDS)
        await _poll_once(trigger_recovery=False)
    return results


async def _wait_for_healthy_cluster(timeout_seconds: float, reason: str = "") -> bool:
    """Poll until all nodes are healthy and the desired model is active.

    Returns True if the cluster becomes healthy within *timeout_seconds*, False otherwise.
    Polls every 30 s so the caller doesn't have to busy-wait.
    """
    deadline = _now() + timeout_seconds
    poll_interval = 30.0
    while _now() < deadline:
        await _poll_once(trigger_recovery=False)
        if _state.instance_status == "active" and not _state.anomalies:
            log.info("Cluster healthy%s: endpoint=%s model=%s", f" for {reason}" if reason else "", _state.api_endpoint, _state.model)
            return True
        remaining = int(deadline - _now())
        log.info(
            "Waiting for healthy cluster%s (%ds remaining): status=%s anomalies=%s",
            f" [{reason}]" if reason else "",
            remaining,
            _state.instance_status,
            _state.anomalies,
        )
        await asyncio.sleep(min(poll_interval, max(0.0, deadline - _now())))
    return False


async def _run_scheduled_daily_restart() -> None:
    hosts = _peer_hosts_then_control()
    if not hosts:
        return

    _state.recovery_state = "scheduled_daily_restart"
    _state.recovery_error = None
    _record_recovery_event(
        "scheduled_daily_restart_start",
        {"hosts": [host.name for host in hosts], "schedule": _daily_restart_schedule_label()},
    )
    restart_results: list[dict[str, Any]] = []
    try:
        restart_results = await _restart_hosts_sequential(hosts, "scheduled_daily_restart_host")
        # Wait for all nodes to rediscover each other via mDNS and form the cluster.
        # mDNS query interval in exo's libp2p is 25 min, so nodes that miss the initial
        # announcement may take a long time; we poll aggressively here so we detect the
        # moment the cluster forms and don't block the job queue unnecessarily.
        cluster_formed = await _wait_for_healthy_cluster(
            WATCHDOG_CLUSTER_FORM_TIMEOUT_SECONDS, "scheduled_daily_restart"
        )
        if not cluster_formed:
            log.warning(
                "Cluster did not fully form within %ds after daily restart; proceeding with model load anyway",
                WATCHDOG_CLUSTER_FORM_TIMEOUT_SECONDS,
            )
        model_ready = await _ensure_desired_model_loaded()
        await _poll_once(trigger_recovery=False)
        probe_ok = await _probe_exo()
        _state.last_scheduled_restart_ts = _now()
        _record_recovery_event(
            "scheduled_daily_restart_completed",
            {
                "hosts": [host.name for host in hosts],
                "model_ready": model_ready,
                "probe_ok": probe_ok,
                "results": restart_results,
            },
        )
        if model_ready and probe_ok:
            _state.recovery_state = "idle"
            _state.recovery_error = None
        else:
            _state.recovery_state = "degraded"
            _state.recovery_error = (
                "daily restart finished but model probe did not recover cleanly"
            )
    except Exception as exc:  # noqa: BLE001
        _state.recovery_state = "degraded"
        _state.recovery_error = str(exc)
        _record_recovery_event(
            "scheduled_daily_restart_failed",
            {"error": str(exc), "results": restart_results},
        )


async def _daily_restart_loop() -> None:
    _state.last_scheduled_restart_ts = (
        _latest_recovery_event_ts("scheduled_daily_restart_completed") or 0.0
    )
    if not _state.last_scheduled_restart_ts:
        now_local = datetime.now(WATCHDOG_DAILY_RESTART_TZ)
        scheduled_today = now_local.replace(
            hour=WATCHDOG_DAILY_RESTART_HOUR,
            minute=WATCHDOG_DAILY_RESTART_MINUTE,
            second=0,
            microsecond=0,
        )
        if now_local >= scheduled_today:
            # Do not "catch up" a missed restart immediately on watchdog startup.
            _state.last_scheduled_restart_ts = scheduled_today.timestamp()
            log.info(
                "Skipping catch-up daily restart on startup; next automatic restart will run on the next scheduled day"
            )
    while True:
        try:
            now_local = datetime.now(WATCHDOG_DAILY_RESTART_TZ)
            scheduled_today = now_local.replace(
                hour=WATCHDOG_DAILY_RESTART_HOUR,
                minute=WATCHDOG_DAILY_RESTART_MINUTE,
                second=0,
                microsecond=0,
            )
            last_local_date = None
            if _state.last_scheduled_restart_ts:
                last_local_date = datetime.fromtimestamp(
                    _state.last_scheduled_restart_ts,
                    WATCHDOG_DAILY_RESTART_TZ,
                ).date()

            if now_local >= scheduled_today and last_local_date != now_local.date():
                _refresh_queue_metrics()
                if _state.active_job is None and _state.queue_depth == 0:
                    await _run_scheduled_daily_restart()
                else:
                    log.info(
                        "Daily exo restart deferred: active_job=%s queue_depth=%s",
                        _state.active_job,
                        _state.queue_depth,
                    )
            await asyncio.sleep(max(5, WATCHDOG_DAILY_RESTART_CHECK_SECONDS))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.exception("Daily restart loop failed: %s", exc)
            await asyncio.sleep(max(5, WATCHDOG_DAILY_RESTART_CHECK_SECONDS))


def _instance_ids_for_model(state_payload: dict[str, Any], model_id: str) -> list[str]:
    return [
        str(instance["instance_id"])
        for instance in _extract_state_instances(state_payload)
        if str(instance.get("model") or "").strip() == model_id
    ]


async def _delete_instance(base_url: str, instance_id: str) -> None:
    async with httpx.AsyncClient(timeout=WATCHDOG_HTTP_TIMEOUT_SECONDS) as client:
        response = await client.delete(f"{base_url.rstrip('/')}/instance/{instance_id}")
        response.raise_for_status()


async def _place_desired_model(base_url: str) -> None:
    payload = {
        "model_id": EXO_DESIRED_MODEL,
        "sharding": EXO_SHARDING,
        "instance_meta": EXO_INSTANCE_META,
        "min_nodes": EXO_MIN_NODES,
    }
    async with httpx.AsyncClient(timeout=WATCHDOG_HTTP_TIMEOUT_SECONDS) as client:
        response = await client.post(f"{base_url.rstrip('/')}/place_instance", json=payload)
        response.raise_for_status()


async def _ensure_desired_model_loaded() -> bool:
    if _state.instance_status == "active" and EXO_DESIRED_MODEL in _state.instance_to_host_map:
        return True

    base_url = _current_api_base()
    async with httpx.AsyncClient(timeout=WATCHDOG_HTTP_TIMEOUT_SECONDS) as client:
        _, state_payload, _ = await _exo_json_request(client, base_url, "/state", allow_statuses={200})
    if not isinstance(state_payload, dict):
        return False

    active_instances = _extract_state_instances(state_payload)
    desired_ids = [instance["instance_id"] for instance in active_instances if instance.get("model") == EXO_DESIRED_MODEL]
    other_ids = [instance["instance_id"] for instance in active_instances if instance.get("model") and instance.get("model") != EXO_DESIRED_MODEL]

    for instance_id in other_ids:
        try:
            await _delete_instance(base_url, instance_id)
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed to delete stale exo instance %s: %s", instance_id, exc)

    if not desired_ids:
        await _place_desired_model(base_url)

    deadline = _now() + WATCHDOG_PROBE_TIMEOUT_SECONDS
    while _now() < deadline:
        await asyncio.sleep(5)
        await _poll_once(trigger_recovery=False)
        if _state.instance_status == "active" and EXO_DESIRED_MODEL in _state.instance_to_host_map:
            return True
    return False


def _switch_to_healthy_endpoint() -> bool:
    if _state.api_endpoint and _state.host_health.get(urlsplit(_state.api_endpoint).hostname or "", {}).get("healthy"):
        return True
    for endpoint in EXO_ENDPOINTS:
        host = urlsplit(endpoint).hostname or ""
        if _state.host_health.get(host, {}).get("healthy"):
            _state.api_endpoint = endpoint
            return True
    return False


async def _attempt_recovery(reason: str) -> None:
    if not AUTO_RESTART:
        return

    async with _recovery_lock:
        if (
            reason == "poll_anomaly"
            and _state.last_restart_ts
            and _now() - _state.last_restart_ts < RESTART_COOLDOWN
        ):
            remaining = int(RESTART_COOLDOWN - (_now() - _state.last_restart_ts))
            log.info("Recovery cooldown active, %ds remaining", max(0, remaining))
            return

        _state.recovery_state = "polling"
        _state.recovery_error = None
        _record_recovery_event("recovery_start", {"reason": reason})

        await _poll_once(trigger_recovery=False)
        if _state.last_poll_ok and not _state.anomalies and await _probe_exo():
            _state.recovery_state = "idle"
            return

        if _switch_to_healthy_endpoint():
            _state.recovery_state = "placing_instance"
            try:
                if await _ensure_desired_model_loaded():
                    if await _probe_exo():
                        _state.recovery_state = "idle"
                        _state.recovery_error = None
                        return
            except Exception as exc:  # noqa: BLE001
                _state.recovery_error = str(exc)
                _record_recovery_event("place_instance_failed", {"error": str(exc)})

        unhealthy_hosts = _hosts_needing_restart()
        if unhealthy_hosts:
            _state.recovery_state = "restart_unhealthy_hosts"
            if await _restart_hosts_until_healthy(unhealthy_hosts, "restart_unhealthy_host"):
                return

        control_host = _control_remote_host()
        peer_hosts = _peer_remote_hosts()
        _state.recovery_state = "rolling_restart"
        ordered_hosts = [host for host in [control_host, *peer_hosts] if host is not None]
        if await _restart_hosts_until_healthy(ordered_hosts, "rolling_restart"):
            return
        _state.recovery_state = "degraded"


# ---------------------------------------------------------------------------
# Job normalization
# ---------------------------------------------------------------------------

def _prepare_generic_chat_request(
    raw_request: dict[str, Any],
    *,
    model: str,
) -> tuple[dict[str, Any], int, int, list[str], str]:
    truncated_fields: list[str] = []
    request = dict(raw_request)

    messages = request.get("messages")
    if not isinstance(messages, list) or not messages:
        raise ValueError("messages are required for generic chat")

    normalized_messages: list[dict[str, str]] = []
    total_chars = 0
    for index, message in enumerate(messages):
        role = str((message or {}).get("role") or "user")
        content = str((message or {}).get("content") or "")
        remaining = max(0, WATCHDOG_MAX_PROMPT_CHARS - total_chars)
        if remaining <= 0:
            truncated_fields.append(f"messages[{index}]")
            break
        trimmed = _truncate_text(content, remaining, f"messages[{index}]", truncated_fields)
        total_chars += len(trimmed)
        normalized_messages.append({"role": role, "content": trimmed})

    request["model"] = model
    request["messages"] = normalized_messages
    request.setdefault("temperature", 0.2)
    request.setdefault("max_tokens", 512)
    request.setdefault("enable_thinking", False)
    return request, total_chars, _estimate_tokens_from_text(_json_dumps(request)), truncated_fields, model


def _prepare_ui_chat_request(
    *,
    headlines: list[str],
    mode: str,
    geo_context: str,
    variant: str,
    lang: str,
    model: str,
) -> tuple[dict[str, Any], int, int, list[str], str]:
    truncated_fields: list[str] = []
    cleaned_headlines = [
        _truncate_text(str(item or ""), 160, f"headlines[{index}]", truncated_fields)
        for index, item in enumerate(headlines[:8])
    ]
    unique_headlines = _dedupe_headlines([item for item in cleaned_headlines if item])
    if not unique_headlines:
        raise ValueError("headlines are required")

    geo = _truncate_text(geo_context or "", WATCHDOG_MAX_GEO_CONTEXT_CHARS, "geoContext", truncated_fields)
    headline_text = "\n".join(f"{index + 1}. {headline}" for index, headline in enumerate(unique_headlines))
    intel_section = f"\n\n{geo}" if geo else ""
    is_tech_variant = variant == "tech"
    date_prefix = time.strftime("%Y-%m-%d", time.gmtime())
    date_context = (
        f"Current date: {date_prefix}."
        if is_tech_variant
        else (
            f"Current date: {date_prefix}. Donald Trump is the current US President "
            "(second term, inaugurated Jan 2025)."
        )
    )
    lang_instruction = (
        f"\nIMPORTANT: Output the summary in {lang.upper()} language."
        if lang and lang != "en"
        else ""
    )

    if mode == "brief":
        if is_tech_variant:
            system_prompt = (
                f"{date_context}\n\n"
                "Summarize the key tech/startup development in 2-3 sentences.\n"
                "Rules:\n"
                "- Focus ONLY on technology, startups, AI, funding, product launches, or developer news\n"
                "- IGNORE political news, trade policy, tariffs, government actions unless directly about tech regulation\n"
                "- Lead with the company/product/technology name\n"
                '- Start directly: "OpenAI announced...", "A new $50M Series B...", "GitHub released..."\n'
                f"- No bullet points, no meta-commentary{lang_instruction}"
            )
        else:
            system_prompt = (
                f"{date_context}\n\n"
                "Summarize the key development in 2-3 sentences.\n"
                "Rules:\n"
                "- Lead with WHAT happened and WHERE - be specific\n"
                '- NEVER start with "Breaking news", "Good evening", "Tonight", or TV-style openings\n'
                '- Start directly with the subject: "Iran\'s regime...", "The US Treasury...", "Protests in..."\n'
                "- CRITICAL FOCAL POINTS are the main actors - mention them by name\n"
                "- If focal points show news + signals convergence, that's the lead\n"
                f"- No bullet points, no meta-commentary{lang_instruction}"
            )
        user_prompt = f"Summarize the top story:\n{headline_text}{intel_section}"
        max_tokens = 512
    elif mode == "analysis":
        if is_tech_variant:
            system_prompt = (
                f"{date_context}\n\n"
                "Analyze the tech/startup trend in 2-3 sentences.\n"
                "Rules:\n"
                "- Focus ONLY on technology implications: funding trends, AI developments, market shifts, product strategy\n"
                "- IGNORE political implications, trade wars, government unless directly about tech policy\n"
                "- Lead with the insight for tech industry\n"
                "- Connect to startup ecosystem, VC trends, or technical implications"
            )
            user_prompt = f"What's the key tech trend or development?\n{headline_text}{intel_section}"
        else:
            system_prompt = (
                f"{date_context}\n\n"
                "Provide analysis in 2-3 sentences. Be direct and specific.\n"
                "Rules:\n"
                "- Lead with the insight - what's significant and why\n"
                '- NEVER start with "Breaking news", "Tonight", "The key/dominant narrative is"\n'
                '- Start with substance: "Iran faces...", "The escalation in...", "Multiple signals suggest..."\n'
                "- CRITICAL FOCAL POINTS are your main actors - explain WHY they matter\n"
                "- If focal points show news-signal correlation, flag as escalation\n"
                "- Connect dots, be specific about implications"
            )
            user_prompt = f"What's the key pattern or risk?\n{headline_text}{intel_section}"
        max_tokens = 512
    elif mode == "translate":
        target_lang = variant or lang or "en"
        system_prompt = (
            f"You are a professional news translator. Translate the following news headlines/summaries into {target_lang}.\n"
            "- Maintain the original tone and journalistic style.\n"
            '- Do NOT add any conversational filler (e.g., "Here is the translation").\n'
            "- Output ONLY the translated text.\n"
            f"- If the text is already in {target_lang}, return it as is."
        )
        user_prompt = f"Translate to {target_lang}:\n{unique_headlines[0]}"
        max_tokens = 256
    else:
        system_prompt = (
            f"{date_context}\n\n"
            + (
                "Synthesize tech news in 2 sentences. Focus on startups, AI, funding, products. "
                "Ignore politics unless directly about tech regulation."
                if is_tech_variant
                else (
                    "Synthesize in 2 sentences max. Lead with substance. NEVER start with "
                    '"Breaking news" or "Tonight" - just state the insight directly. '
                    "CRITICAL focal points with news-signal convergence are significant."
                )
            )
            + lang_instruction
        )
        user_prompt = f"Key takeaway:\n{headline_text}{intel_section}"
        max_tokens = 256

    request = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.3,
        "max_tokens": max_tokens,
        "top_p": 0.9,
        "enable_thinking": False,
    }
    input_chars = len("\n".join(unique_headlines)) + len(geo)
    estimated_tokens = _estimate_tokens_from_text(_json_dumps(request))
    return request, input_chars, estimated_tokens, truncated_fields, request["model"]


def _prepare_news_article_request(
    *,
    title: str,
    content: str,
    model: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, int, int, list[str], str]:
    truncated_fields: list[str] = []
    clean_title = _truncate_text(title or "", 320, "title", truncated_fields)
    clean_content = _truncate_text(content or "", WATCHDOG_MAX_ARTICLE_CHARS, "content", truncated_fields)
    input_chars = len(clean_title) + len(clean_content)

    if len(clean_content) < WATCHDOG_NEWS_MIN_CHARS:
        synthetic = _build_openai_response(
            model=model,
            content="",
            usage={"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        )
        return None, synthetic, input_chars, _estimate_tokens_from_text(clean_content), truncated_fields, model

    prompt = (
        "Summarize this news article in 1-2 concise sentences. "
        "Focus only on what happened and why it matters. "
        "Do NOT add opinions, speculation, or extra background.\n\n"
        f"Title: {clean_title}\n\n{clean_content}"
    )
    request = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0,
        "max_tokens": WATCHDOG_NEWS_MAX_TOKENS,
        "enable_thinking": False,
    }
    estimated_tokens = _estimate_tokens_from_text(_json_dumps(request))
    return request, None, input_chars, estimated_tokens, truncated_fields, model


def _prepare_chat_job_spec(body: dict[str, Any], source: str) -> tuple[dict[str, Any], int, int, list[str], str, str]:
    profile = str(body.get("profile") or body.get("watchdog_profile") or "generic_chat").strip() or "generic_chat"
    if profile not in WATCHDOG_ALLOWED_PROFILES:
        raise ValueError(f"unsupported profile: {profile}")
    route = _resolve_llm_route(source, profile, str(body.get("model") or ""))

    if profile in {"ui_summary", "ui_translate"}:
        input_payload = body.get("input") or body.get("watchdog_input") or {}
        if not isinstance(input_payload, dict):
            raise ValueError("input must be an object")
        request, input_chars, estimated_tokens, truncated_fields, model = _prepare_ui_chat_request(
            headlines=[str(item) for item in (input_payload.get("headlines") or [])],
            mode=str(input_payload.get("mode") or "brief"),
            geo_context=str(input_payload.get("geoContext") or ""),
            variant=str(input_payload.get("variant") or "full"),
            lang=str(input_payload.get("lang") or "en"),
            model=str(route["resolved_model"]),
        )
        payload = {"request": request, "synthetic_response": None, "route": route}
        return payload, input_chars, estimated_tokens, truncated_fields, model, profile

    if profile == "news_article_summary":
        input_payload = body.get("input") or body.get("watchdog_input") or {}
        if not isinstance(input_payload, dict):
            raise ValueError("input must be an object")
        request, synthetic, input_chars, estimated_tokens, truncated_fields, resolved_model = _prepare_news_article_request(
            title=str(input_payload.get("title") or ""),
            content=str(input_payload.get("content") or ""),
            model=str(route["resolved_model"]),
        )
        payload = {"request": request, "synthetic_response": synthetic, "route": route}
        return payload, input_chars, estimated_tokens, truncated_fields, resolved_model, profile

    raw_request = dict(body.get("request") or body)
    raw_request.pop("kind", None)
    raw_request.pop("source", None)
    raw_request.pop("profile", None)
    raw_request.pop("input", None)
    raw_request.pop("watchdog_profile", None)
    raw_request.pop("watchdog_input", None)
    request, input_chars, estimated_tokens, truncated_fields, model = _prepare_generic_chat_request(
        raw_request,
        model=str(route["resolved_model"]),
    )
    payload = {"request": request, "synthetic_response": None, "route": route}
    return payload, input_chars, estimated_tokens, truncated_fields, model, profile


def _prepare_hourly_job_spec(
    body: dict[str, Any],
    source: str,
) -> tuple[dict[str, Any], int, int, list[str], str]:
    input_payload = body.get("input") or {}
    if not isinstance(input_payload, dict):
        raise ValueError("input must be an object")

    truncated_fields: list[str] = []
    raw_items = input_payload.get("items") or []
    if not isinstance(raw_items, list):
        raise ValueError("items must be an array")

    generated_at = _truncate_text(
        str(input_payload.get("generatedAt") or ""),
        120,
        "generatedAt",
        truncated_fields,
    )
    requested_max_items = int(input_payload.get("maxItems") or WATCHDOG_HOURLY_MAX_ITEMS)
    max_items = max(1, min(requested_max_items, WATCHDOG_HOURLY_MAX_ITEMS))

    items = _dedupe_hourly_items(raw_items)[:max_items]
    if not items:
        raise ValueError("items are required")

    normalized_items: list[dict[str, str]] = []
    input_chars = len(generated_at)
    for index, item in enumerate(items):
        normalized = {
            "source": _truncate_text(item["source"], 120, f"items[{index}].source", truncated_fields),
            "title": _truncate_text(item["title"], 240, f"items[{index}].title", truncated_fields),
            "link": _truncate_text(item["link"], 500, f"items[{index}].link", truncated_fields),
        }
        input_chars += len(normalized["source"]) + len(normalized["title"]) + len(normalized["link"])
        normalized_items.append(normalized)

    payload = {"generatedAt": generated_at, "items": normalized_items}
    estimated_tokens = _estimate_tokens_from_text(_json_dumps(payload))
    route = _resolve_llm_route(source, "hourly_news_brief", str(body.get("model") or ""))
    payload["route"] = route
    model = str(route["resolved_model"])
    return payload, input_chars, estimated_tokens, truncated_fields, model


# ---------------------------------------------------------------------------
# Job execution
# ---------------------------------------------------------------------------

async def _run_chat_job(job: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    request = payload.get("request")
    synthetic_response = payload.get("synthetic_response")
    route = dict(payload.get("route") or {})
    model = str(route.get("resolved_model") or job.get("model") or _parse_model_choice(None))
    provider = str(route.get("provider") or "exo")
    backend = str(route.get("backend") or "exo")
    route_id = str(route.get("id") or "")

    if synthetic_response is not None:
        return {
            "response": synthetic_response,
            "model": model,
            "provider": provider,
            "backend": backend,
            "route_id": route_id,
        }

    if not request:
        raise RuntimeError("chat job missing request")

    response = await _call_routed_chat(request, route)
    return {
        "response": response,
        "model": response.get("model") or model,
        "provider": provider,
        "backend": backend,
        "route_id": route_id,
    }


def _extract_first_json(text: str) -> dict[str, Any] | None:
    match = re.search(r"\{[^{}]*\}", text or "", flags=re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None


async def _map_hourly_item(model: str, item: dict[str, str], route: dict[str, Any]) -> dict[str, Any]:
    prompt = (
        "你是新闻编辑。直接输出JSON结果，不要思考过程。\n"
        "请把下面这条新闻压缩成摘要块。\n"
        '输出严格为JSON：{"one":"(20-35字)","bullets":["(20-35字)"],"risk":"(15-30字)","link":"..."}\n\n'
        f"source: {item['source']}\n"
        f"title: {item['title']}\n"
        f"link: {item['link']}"
    )
    request = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens": 280,
        "enable_thinking": False,
    }
    try:
        response = await _call_routed_chat(request, route)
        content = _strip_think(_content_from_openai_response(response))
        parsed = _extract_first_json(content)
        if parsed:
            parsed.setdefault("link", item["link"])
            return parsed
    except Exception as exc:  # noqa: BLE001
        log.warning("Hourly map failed for %s: %s", item["title"], exc)

    return {"one": item["title"], "bullets": [], "risk": "", "link": item["link"]}


def _join_reduce_blocks(blocks: list[str]) -> str:
    return "\n".join(blocks)


async def _reduce_hourly_blocks(
    *,
    model: str,
    route: dict[str, Any],
    generated_at: str,
    blocks: list[str],
    final_pass: bool,
    max_points: int,
) -> str:
    if final_pass:
        prompt = (
            "你是新闻主编。直接输出最终简报，不要思考过程。\n"
            "基于下列逐条摘要块，写一份中文每小时新闻简报。\n"
            "输出要求：\n"
            "- 标题行（含时间）\n"
            f"- 最多{max_points}条要点，且绝不能超过输入事件数量（每条30-50字）\n"
            "- 2-3句风险/影响观察\n"
            "- 总长度<=1000字\n"
            "- 最多附3个最重要链接\n\n"
            "硬性规则：\n"
            "- 只能使用输入中已经出现的公司、国家、政策、事件与链接\n"
            "- 如果输入只有少量条目，就只写这些条目，不得补充、扩写或猜测其他新闻\n"
            "- 不得编造任何未在输入中出现的人名、机构、国家或事件\n\n"
            f"时间: {generated_at}\n\n"
            "逐条摘要(JSONL或中间汇总):\n"
            f"{_join_reduce_blocks(blocks)}"
        )
        max_tokens = 1024
    else:
        prompt = (
            "你是新闻主编。直接输出中间汇总，不要思考过程。\n"
            "基于下列摘要块，写一份可继续合并的中文中间汇总。\n"
            "输出要求：\n"
            f"- 最多{max_points}条要点（每条20-40字）\n"
            "- 1句风险观察\n"
            "- 总长度<=450字\n\n"
            "硬性规则：\n"
            "- 只能重组输入里的事实，不得补充新事件\n"
            "- 如果输入条目少，就维持少量条目，不得凑数\n\n"
            f"时间: {generated_at}\n\n"
            "输入摘要:\n"
            f"{_join_reduce_blocks(blocks)}"
        )
        max_tokens = 600

    request = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
        "max_tokens": max_tokens,
        "enable_thinking": False,
    }
    response = await _call_routed_chat(request, route)
    content = _strip_think(_content_from_openai_response(response))
    if not content:
        raise RuntimeError("local LLM returned empty hourly summary")
    return content


async def _run_hourly_single_pass(
    *,
    model: str,
    route: dict[str, Any],
    generated_at: str,
    items: list[dict[str, str]],
) -> str:
    prompt_lines = []
    bullet_lines = []
    link_lines = []
    for index, item in enumerate(items, 1):
        source = str(item["source"]).strip() or "Unknown"
        title = _truncate_text(str(item["title"]), 120, f"single_pass_title[{index}]", [])
        link = str(item["link"]).strip()
        prompt_lines.append(f"{index}. [{source}] {title}")
        bullet_lines.append(f"- [{source}] {title}")
        if link and len(link_lines) < 3:
            link_lines.append(f"{len(link_lines) + 1}. {link}")

    prompt = (
        "你是新闻编辑。直接输出一段极短的中文导语，不要思考过程。\n"
        "基于下列新闻标题，写 2-3 句中文总览，点出共同主题与主要风险。\n"
        "输出要求：\n"
        "- 总长度<=160字\n"
        "- 不要逐条复述标题\n"
        "- 只能使用输入里已有事实，不得补充背景或新事实\n\n"
        f"时间: {generated_at}\n\n"
        "输入标题:\n"
        f"{'\n'.join(prompt_lines)}"
    )
    request = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens": 160,
        "enable_thinking": False,
    }
    response = await _call_routed_chat(request, route)
    content = _strip_think(_content_from_openai_response(response))
    if not content:
        raise RuntimeError("local LLM returned empty hourly summary")

    sections = [
        f"每小时新闻简报 {generated_at}".strip(),
        "",
        content.strip(),
        "",
        "重点:",
        "\n".join(bullet_lines),
    ]
    if link_lines:
        sections.extend(["", "链接:", "\n".join(link_lines)])
    return "\n".join(section for section in sections if section is not None)


async def _run_hourly_job(job: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    route = dict(payload.get("route") or {})
    model = str(route.get("resolved_model") or job.get("model") or _parse_model_choice(None))
    items = payload.get("items") or []
    generated_at = str(payload.get("generatedAt") or "")
    if not items:
        raise RuntimeError("hourly job missing items")

    # Pre-flight: wait for the cluster to be fully healthy before issuing any LLM
    # calls.  After a daily restart (or any recovery), nodes need time to rediscover
    # each other via mDNS and load the model.  Running the job against a degraded
    # cluster would burn the stall timeout and fail, so we wait here instead.
    if _state.instance_status != "active" or _state.anomalies:
        cluster_ok = await _wait_for_healthy_cluster(
            WATCHDOG_HOURLY_PREFLIGHT_TIMEOUT_SECONDS, "hourly_job"
        )
        if not cluster_ok:
            raise RuntimeError(
                f"cluster not healthy after {WATCHDOG_HOURLY_PREFLIGHT_TIMEOUT_SECONDS}s pre-flight wait: "
                f"status={_state.instance_status} anomalies={_state.anomalies}"
            )

    total_input_chars = sum(
        len(str(item.get("source") or "")) + len(str(item.get("title") or "")) + len(str(item.get("link") or ""))
        for item in items
    )
    if (
        len(items) <= WATCHDOG_HOURLY_SINGLE_PASS_MAX_ITEMS
        and total_input_chars <= WATCHDOG_HOURLY_SINGLE_PASS_MAX_CHARS
    ):
        summary = await _run_hourly_single_pass(
            model=model,
            route=route,
            generated_at=generated_at,
            items=items,
        )
        return {
            "summary": summary,
            "model": model,
            "provider": str(route.get("provider") or "exo"),
            "backend": str(route.get("backend") or "exo"),
            "route_id": str(route.get("id") or ""),
            "generatedAt": generated_at,
            "itemCount": len(items),
            "llmMapSuccessCount": 0,
            "strategy": "single_pass",
        }

    mapped_blocks: list[str] = []
    llm_map_success = 0
    for item in items:
        mapped = await _map_hourly_item(model, item, route)
        if mapped.get("one") != item["title"]:
            llm_map_success += 1
        mapped_blocks.append(_json_dumps(mapped))

    working_blocks = mapped_blocks
    while (
        len(working_blocks) > WATCHDOG_HOURLY_REDUCE_BATCH_SIZE
        or len(_join_reduce_blocks(working_blocks)) > WATCHDOG_HOURLY_REDUCE_MAX_CHARS
    ):
        next_blocks: list[str] = []
        for index in range(0, len(working_blocks), WATCHDOG_HOURLY_REDUCE_BATCH_SIZE):
            batch = working_blocks[index : index + WATCHDOG_HOURLY_REDUCE_BATCH_SIZE]
            reduced = await _reduce_hourly_blocks(
                model=model,
                route=route,
                generated_at=generated_at,
                blocks=batch,
                final_pass=False,
                max_points=len(batch),
            )
            next_blocks.append(reduced)
        working_blocks = next_blocks

    summary = await _reduce_hourly_blocks(
        model=model,
        route=route,
        generated_at=generated_at,
        blocks=working_blocks,
        final_pass=True,
        max_points=len(items),
    )
    return {
        "summary": summary,
        "model": model,
        "provider": str(route.get("provider") or "exo"),
        "backend": str(route.get("backend") or "exo"),
        "route_id": str(route.get("id") or ""),
        "generatedAt": generated_at,
        "itemCount": len(items),
        "llmMapSuccessCount": llm_map_success,
        "strategy": "map_reduce",
    }


async def _execute_job(job: dict[str, Any]) -> dict[str, Any]:
    with _db_lock:
        row = _db().execute("SELECT request_json FROM jobs WHERE id = ?", (job["id"],)).fetchone()
    request_payload = _json_loads(row["request_json"] if row else None, {})

    if job["kind"] == "chat_completion":
        return await _run_chat_job(job, request_payload)
    if job["kind"] == "hourly_news_brief":
        return await _run_hourly_job(job, request_payload)
    raise RuntimeError(f"unsupported job kind: {job['kind']}")


async def _worker_loop() -> None:
    while True:
        job = _claim_next_job()
        if job is None:
            _queue_wakeup.clear()
            job = _claim_next_job()
            if job is None:
                await _queue_wakeup.wait()
                continue
            _queue_wakeup.set()

        try:
            result = await _execute_job(job)
        except httpx.HTTPStatusError as exc:
            error = {
                "type": "http_status",
                "status_code": exc.response.status_code,
                "message": exc.response.text,
            }
            _finish_job_failure(job["id"], error)
            await _attempt_recovery("job_failure")
            continue
        except WatchdogExecutionError as exc:
            error = {
                "type": exc.error_type,
                "status_code": exc.status_code,
                "message": str(exc),
            }
            _finish_job_failure(job["id"], error)
            await _attempt_recovery("job_failure")
            continue
        except Exception as exc:  # noqa: BLE001
            error = {"type": "execution_error", "message": str(exc)}
            _finish_job_failure(job["id"], error)
            await _attempt_recovery("job_failure")
            continue

        _finish_job_success(job["id"], result, result.get("model"))


# ---------------------------------------------------------------------------
# Wait helpers
# ---------------------------------------------------------------------------

async def _wait_for_job(job_id: str, timeout_seconds: float | None) -> dict[str, Any]:
    snapshot = _get_job_snapshot(job_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    if snapshot["status"] in TERMINAL_STATUSES:
        return snapshot

    event = _job_event(job_id)
    try:
        if timeout_seconds is None:
            await event.wait()
        else:
            await asyncio.wait_for(event.wait(), timeout=timeout_seconds)
    except asyncio.TimeoutError:
        pass

    snapshot = _get_job_snapshot(job_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return snapshot


def _job_response_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {"job": snapshot}


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    _db_init()
    _prepare_ssh_runtime()
    _prepare_cli_runtime()
    _cli_provider_status("codex-cli", force=True)
    _cli_provider_status("claude-code-cli", force=True)
    _recover_jobs_after_watchdog_restart()
    poll_task = asyncio.create_task(_poll_loop())
    worker_task = asyncio.create_task(_worker_loop())
    daily_restart_task = (
        asyncio.create_task(_daily_restart_loop())
        if WATCHDOG_DAILY_RESTART_ENABLED
        else None
    )
    yield
    tasks = [poll_task, worker_task]
    if daily_restart_task is not None:
        tasks.append(daily_restart_task)
    for task in tasks:
        task.cancel()
    for task in tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="local-llm-watchdog", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/status")
async def status():
    _refresh_queue_metrics()
    return {
        "backend": _state.backend,
        "policy_version": WATCHDOG_POLICY_VERSION,
        "ok": _state.last_poll_ok and len(_state.anomalies) == 0,
        "last_poll_ts": _state.last_poll_ts,
        "last_poll_ok": _state.last_poll_ok,
        "anomalies": _state.anomalies,
        "desired_model": _state.desired_model,
        "model": _state.model,
        "models": _state.models,
        "api_endpoint": _state.api_endpoint,
        "peer_health": _state.peer_health,
        "restart_count": _state.restart_count,
        "last_restart_ts": _state.last_restart_ts,
        "auto_restart_enabled": AUTO_RESTART,
        "queue_depth": _state.queue_depth,
        "active_job": _state.active_job,
        "oldest_waiting_age_seconds": _state.oldest_waiting_age_seconds,
        "job_failures_consecutive": _state.job_failures_consecutive,
        "last_remote_restart_host": _state.last_remote_restart_host,
        "daily_restart_enabled": WATCHDOG_DAILY_RESTART_ENABLED,
        "daily_restart_schedule": _daily_restart_schedule_label(),
        "last_scheduled_restart_ts": _state.last_scheduled_restart_ts or None,
        "recovery_state": _state.recovery_state,
        "recovery_error": _state.recovery_error,
        "instance_to_host_map": _state.instance_to_host_map,
        "instance_status": _state.instance_status,
        "host_health": _state.host_health,
        "runners": _state.runners,
        "instances": _state.instances,
    }


@app.get("/models")
async def models():
    if not _state.last_poll_ok:
        raise HTTPException(status_code=503, detail="Local LLM unreachable or not yet polled")
    return {
        "models": _state.models,
        "count": len(_state.models),
        "last_poll_ts": _state.last_poll_ts,
    }


@app.post("/restart")
async def restart(host_name: str | None = None):
    if host_name:
        host = next(
            (
                item
                for item in REMOTE_HOSTS
                if item.name == host_name or item.ip == host_name
            ),
            None,
        )
        if host is None:
            raise HTTPException(status_code=404, detail=f"Host {host_name} not found")
    else:
        host = _control_remote_host()
        if host is None:
            raise HTTPException(status_code=404, detail="Ingress host is not configured")

    result = await _restart_remote_host(host)
    _state.last_restart_ts = _now()
    _state.restart_count += 1
    _state.last_remote_restart_host = host.name
    _host_state_update(host.name, ts=_state.last_restart_ts, result=result, error=None)
    asyncio.create_task(_poll_once(trigger_recovery=False))
    return {"restarted": True, "detail": result, "host": host.name}


@app.post("/host/stop")
async def host_stop(host_name: str):
    """Stop exo processes on a remote host via SSH."""
    host = next(
        (item for item in REMOTE_HOSTS if item.name == host_name or item.ip == host_name),
        None,
    )
    if host is None:
        raise HTTPException(status_code=404, detail=f"Host {host_name} not found")
    # Extract only the kill/pkill steps from restart_cmd (skip nohup/start/sleep steps)
    stop_parts = [
        step.strip()
        for step in host.restart_cmd.split(";")
        if step.strip() and any(k in step for k in ("kill", "pkill"))
    ]
    stop_cmd = "; ".join(stop_parts) + "; true" if stop_parts else "pkill -f '/exo/.venv/bin/exo' 2>/dev/null; true"
    # Use direct SSH command (not interactive -tt stdin) for reliability
    ssh_args = [
        "ssh", "-T",
        "-F", str(WATCHDOG_SSH_CONFIG),
        "-o", "ConnectTimeout=10",
        host.ssh_target,
        stop_cmd,
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *ssh_args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
        result = {
            "host": host.name,
            "returncode": proc.returncode,
            "stdout": stdout.decode(errors="replace").strip(),
            "stderr": stderr.decode(errors="replace").strip(),
        }
    except Exception as e:
        result = {"note": "stop command failed", "detail": str(e)}
    asyncio.create_task(_poll_once(trigger_recovery=False))
    return {"stopped": True, "detail": result, "host": host.name}


@app.post("/host/start")
async def host_start(host_name: str):
    """Start exo daemon on a remote host via SSH."""
    host = next(
        (item for item in REMOTE_HOSTS if item.name == host_name or item.ip == host_name),
        None,
    )
    if host is None:
        raise HTTPException(status_code=404, detail=f"Host {host_name} not found")
    start_cmd = "nohup ~/exo/exo_daemon.sh > /dev/null 2>&1 & sleep 2; echo started"
    ssh_args = [
        "ssh", "-T",
        "-F", str(WATCHDOG_SSH_CONFIG),
        "-o", "ConnectTimeout=10",
        host.ssh_target,
        start_cmd,
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *ssh_args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
        result = {
            "host": host.name,
            "returncode": proc.returncode,
            "stdout": stdout.decode(errors="replace").strip(),
            "stderr": stderr.decode(errors="replace").strip(),
        }
    except Exception as e:
        result = {"note": "start command failed", "detail": str(e)}
    asyncio.create_task(_poll_once(trigger_recovery=False))
    return {"started": True, "detail": result, "host": host.name}


@app.post("/poll")
async def poll_now():
    await _poll_once(trigger_recovery=False)
    return {"ok": True, "anomalies": _state.anomalies, "models": _state.models}


@app.get("/router/routes")
async def router_routes():
    return {
        "policy_version": WATCHDOG_POLICY_VERSION,
        "allowed_profiles": sorted(WATCHDOG_ALLOWED_PROFILES),
        "allowed_backends": sorted(WATCHDOG_ALLOWED_BACKENDS),
        "routes": _list_llm_routes(),
    }


@app.get("/router/providers")
async def router_providers():
    _cli_provider_status("codex-cli", force=True)
    _cli_provider_status("claude-code-cli", force=True)
    return {
        "policy_version": WATCHDOG_POLICY_VERSION,
        "providers": _list_llm_provider_configs(include_secrets=False),
    }


@app.get("/router/provider-models")
async def router_provider_models(provider_config_id: str):
    try:
        provider_config = _get_llm_provider_config(provider_config_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if provider_config is None:
        raise HTTPException(status_code=404, detail=f"Provider config {provider_config_id} not found")

    backend = str(provider_config.get("backend") or "").strip().lower()
    fallback_models = _provider_fallback_models(provider_config)
    blank_label = (
        "Auto-selected by Exo Watchdog"
        if backend == "exo"
        else (
            "Use CLI default model"
            if _is_cli_backend(backend)
            else "Select provider model"
        )
    )
    if backend == "exo":
        return {
            "ok": True,
            "policy_version": WATCHDOG_POLICY_VERSION,
            "provider_config_id": provider_config["id"],
            "source": "automatic",
            "blank_label": blank_label,
            "models": [],
        }
    try:
        models, source = await _fetch_provider_models(provider_config)
        if not models:
            models = fallback_models
            source = "fallback"
        return {
            "ok": True,
            "policy_version": WATCHDOG_POLICY_VERSION,
            "provider_config_id": provider_config["id"],
            "source": source,
            "blank_label": blank_label,
            "models": models,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "policy_version": WATCHDOG_POLICY_VERSION,
            "provider_config_id": provider_config["id"],
            "source": "fallback",
            "blank_label": blank_label,
            "models": fallback_models,
            "error": str(exc),
        }


@app.get("/router/resolve")
async def router_resolve(source: str, profile: str = "generic_chat", model: str | None = None):
    try:
        route = _resolve_llm_route(source, profile, model)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"policy_version": WATCHDOG_POLICY_VERSION, "route": _public_route(route)}


@app.put("/router/routes/{route_id}")
async def router_upsert(route_id: str, request: Request):
    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Body must be a JSON object")
    try:
        route = _upsert_llm_route(route_id, body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "route": route}


@app.put("/router/providers/{provider_config_id}")
async def router_upsert_provider(provider_config_id: str, request: Request):
    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Body must be a JSON object")
    try:
        provider = _upsert_llm_provider_config(provider_config_id, body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"ok": True, "provider": _public_provider_config(provider)}


@app.delete("/router/routes/{route_id}")
async def router_delete(route_id: str):
    try:
        deleted = _delete_llm_route(route_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Route {route_id} not found")
    return {"ok": True, "deleted": route_id}


@app.get("/state/raw")
async def raw_state():
    if not _state.last_state:
        raise HTTPException(status_code=503, detail="No state available yet")
    return JSONResponse(_state.last_state)


@app.post("/v1/jobs")
async def create_job(request: Request):
    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Body must be a JSON object")

    kind = str(body.get("kind") or "").strip()
    source = str(body.get("source") or request.headers.get("x-watchdog-source") or "unknown").strip()
    client_request_id = _normalize_client_request_id(
        body.get("client_request_id")
        or request.headers.get("x-client-request-id")
        or request.headers.get("x-request-id"),
        source,
    )
    if kind not in {"chat_completion", "hourly_news_brief"}:
        raise HTTPException(status_code=400, detail="Unsupported job kind")

    try:
        if kind == "chat_completion":
            payload, input_chars, estimated_tokens, truncated_fields, model, profile = _prepare_chat_job_spec(
                body, source
            )
        else:
            payload, input_chars, estimated_tokens, truncated_fields, model = _prepare_hourly_job_spec(
                body,
                source,
            )
            profile = "hourly_news_brief"
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    snapshot = _enqueue_job(
        kind=kind,
        source=source,
        profile=profile,
        client_request_id=client_request_id,
        payload=payload,
        input_chars=input_chars,
        estimated_tokens=estimated_tokens,
        truncated_fields=truncated_fields,
        model=model,
    )
    return JSONResponse(_job_response_payload(snapshot), headers=_job_headers(snapshot))


@app.get("/v1/jobs/{job_id}")
async def get_job(job_id: str):
    snapshot = _get_job_snapshot(job_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return JSONResponse(_job_response_payload(snapshot), headers=_job_headers(snapshot))


@app.get("/v1/jobs/{job_id}/wait")
async def wait_job(job_id: str, timeout_seconds: float = WATCHDOG_WAIT_CHUNK_SECONDS):
    _increment_wait_count(job_id)
    snapshot = await _wait_for_job(job_id, max(0.0, timeout_seconds))
    return JSONResponse(_job_response_payload(snapshot), headers=_job_headers(snapshot))


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Body must be a JSON object")

    source = str(body.get("source") or request.headers.get("x-watchdog-source") or "openai").strip()
    client_request_id = _normalize_client_request_id(
        body.get("client_request_id")
        or request.headers.get("x-client-request-id")
        or request.headers.get("x-request-id"),
        source,
    )
    try:
        payload, input_chars, estimated_tokens, truncated_fields, model, profile = _prepare_chat_job_spec(
            body, source
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    snapshot = _enqueue_job(
        kind="chat_completion",
        source=source,
        profile=profile,
        client_request_id=client_request_id,
        payload=payload,
        input_chars=input_chars,
        estimated_tokens=estimated_tokens,
        truncated_fields=truncated_fields,
        model=model,
    )

    while True:
        snapshot = await _wait_for_job(snapshot["id"], timeout_seconds=None)
        if snapshot["status"] == "completed":
            result = snapshot["result"] or {}
            response_payload = result.get("response")
            if not isinstance(response_payload, dict):
                raise HTTPException(status_code=502, detail="Watchdog completed without response payload")
            return JSONResponse(response_payload, headers=_job_headers(snapshot))
        if snapshot["status"] == "failed":
            error = snapshot["error"] or {}
            detail = str(error.get("message") or "Watchdog job failed")
            status_code = int(error.get("status_code") or 502)
            return JSONResponse(
                {"detail": detail},
                status_code=status_code if 400 <= status_code <= 599 else 502,
                headers=_job_headers(snapshot),
            )
