#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

DATE_PATTERN = re.compile(r"_(\d{4}-\d{2}-\d{2})\.json$")


def _load_json(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected object JSON in {path}")
    return payload


def _extract_date(path: str) -> datetime | None:
    match = DATE_PATTERN.search(path)
    if not match:
        return None
    return datetime.strptime(match.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    manifest_path = repo_root / "captures" / "manifest.json"
    max_age_days = int(os.environ.get("CAPTURE_MAX_AGE_DAYS", "30"))

    errors: list[str] = []
    if not manifest_path.exists():
        errors.append(f"missing_manifest:{manifest_path}")
        print("\n".join(errors))
        return 1

    manifest = _load_json(manifest_path)
    canonical_capture = manifest.get("canonicalCapture")
    canonical_ops = manifest.get("canonicalOps")
    files = manifest.get("files")

    if not isinstance(canonical_capture, str):
        errors.append("invalid_manifest_field:canonicalCapture")
    if not isinstance(canonical_ops, str):
        errors.append("invalid_manifest_field:canonicalOps")
    if not isinstance(files, list):
        errors.append("invalid_manifest_field:files")
        files = []

    manifest_paths = {
        str(item.get("path"))
        for item in files
        if isinstance(item, dict) and isinstance(item.get("path"), str)
    }
    canonical_marked = {
        str(item.get("path"))
        for item in files
        if isinstance(item, dict)
        and isinstance(item.get("path"), str)
        and bool(item.get("isCanonical"))
    }

    for rel in (canonical_capture, canonical_ops):
        if not isinstance(rel, str):
            continue
        if rel not in manifest_paths:
            errors.append(f"manifest_files_missing_pointer:{rel}")
        if rel not in canonical_marked:
            errors.append(f"manifest_pointer_not_marked_canonical:{rel}")
        abs_path = repo_root / rel
        if not abs_path.exists():
            errors.append(f"missing_canonical_file:{abs_path}")

    implementation_path = "captures/final/implementation_ops_2026-02-24.json"
    if implementation_path not in manifest_paths:
        errors.append(f"manifest_files_missing_implementation_registry:{implementation_path}")
    if not (repo_root / implementation_path).exists():
        errors.append(f"missing_implementation_registry:{implementation_path}")

    today = datetime.now(timezone.utc)
    for rel in (canonical_capture, canonical_ops):
        if not isinstance(rel, str):
            continue
        stamped = _extract_date(rel)
        if stamped is None:
            errors.append(f"cannot_parse_capture_date:{rel}")
            continue
        age_days = (today - stamped).days
        if age_days > max_age_days:
            errors.append(f"capture_too_old:{rel}:age_days={age_days}:max={max_age_days}")

    if isinstance(canonical_capture, str) and isinstance(canonical_ops, str):
        capture_date = _extract_date(canonical_capture)
        ops_date = _extract_date(canonical_ops)
        if capture_date and ops_date and capture_date.date() != ops_date.date():
            errors.append(
                "canonical_capture_ops_date_mismatch:"
                f"capture={capture_date.date()} ops={ops_date.date()}"
            )

    if errors:
        print("Capture integrity check failed:")
        for item in errors:
            print(f"- {item}")
        return 1

    print("Capture integrity check passed")
    print(f"- manifest: {manifest_path}")
    print(f"- max_age_days: {max_age_days}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
