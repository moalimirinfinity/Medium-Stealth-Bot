import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from medium_stealth_bot.redaction import redact_payload


def new_run_id(prefix: str) -> str:
    token = uuid4().hex[:8]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}_{stamp}_{token}"


def write_run_artifact(*, artifacts_dir: Path, run_id: str, payload: dict[str, Any]) -> Path:
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = artifacts_dir / f"{timestamp}_{run_id}.json"
    sanitized = redact_payload(payload)
    path.write_text(json.dumps(sanitized, indent=2, sort_keys=True), encoding="utf-8")
    latest_path = artifacts_dir / "latest.json"
    latest_path.write_text(json.dumps(sanitized, indent=2, sort_keys=True), encoding="utf-8")
    return path


def read_latest_run_artifact(
    artifacts_dir: Path,
    *,
    exclude_commands: set[str] | None = None,
) -> tuple[dict[str, Any], Path] | None:
    excluded = {item.strip().lower() for item in (exclude_commands or set()) if item.strip()}

    def _is_allowed(payload: dict[str, Any]) -> bool:
        if not excluded:
            return True
        command = str(payload.get("command", "")).strip().lower()
        return command not in excluded

    latest_path = artifacts_dir / "latest.json"
    if latest_path.exists():
        latest_payload = _load_json(latest_path)
        if _is_allowed(latest_payload):
            return latest_payload, latest_path

    candidates = sorted(
        path for path in artifacts_dir.glob("*.json") if path.name != "latest.json"
    )
    if not candidates:
        return None
    for path in reversed(candidates):
        payload = _load_json(path)
        if _is_allowed(payload):
            return payload, path
    return None


def _load_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}
