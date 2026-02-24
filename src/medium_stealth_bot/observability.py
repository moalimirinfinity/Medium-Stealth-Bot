import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


def new_run_id(prefix: str) -> str:
    token = uuid4().hex[:8]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}_{stamp}_{token}"


def write_run_artifact(*, artifacts_dir: Path, run_id: str, payload: dict[str, Any]) -> Path:
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = artifacts_dir / f"{timestamp}_{run_id}.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    latest_path = artifacts_dir / "latest.json"
    latest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def read_latest_run_artifact(artifacts_dir: Path) -> tuple[dict[str, Any], Path] | None:
    latest_path = artifacts_dir / "latest.json"
    if latest_path.exists():
        return _load_json(latest_path), latest_path

    candidates = sorted(
        path for path in artifacts_dir.glob("*.json") if path.name != "latest.json"
    )
    if not candidates:
        return None
    latest = candidates[-1]
    return _load_json(latest), latest


def _load_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}
