#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
FINAL_DIR = REPO_ROOT / "captures" / "final"
MANIFEST_PATH = REPO_ROOT / "captures" / "manifest.json"
REMOVED_KEYS = {"pageUrl", "requestBodyPreview", "variables"}


def normalize_visit_url(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return "https://medium.com/post/redacted"
    parsed = urlparse(raw if "://" in raw else f"https://medium.com{raw}")
    path = parsed.path or "/"
    lowered = path.lower()

    if lowered.startswith("/_/graphql"):
        return "https://medium.com/_/graphql"

    if lowered in {"/user/profile", "/user/followers", "/user/following", "/post/redacted"}:
        return f"https://medium.com{lowered}"

    if lowered in {"/me/followers", "/me/following"}:
        return f"https://medium.com{lowered}"
    if lowered.startswith("/me/"):
        return "https://medium.com/me/profile"

    if lowered.startswith("/tag/"):
        parts = path.split("/")
        slug = parts[2] if len(parts) > 2 and parts[2] else "general"
        return f"https://medium.com/tag/{slug}"

    if lowered.startswith("/@"):
        if lowered.endswith("/followers"):
            return "https://medium.com/user/followers"
        if lowered.endswith("/following"):
            return "https://medium.com/user/following"
        return "https://medium.com/user/profile"

    return "https://medium.com/post/redacted"


def normalize_request_url(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return "https://medium.com/_/graphql"
    parsed = urlparse(raw if "://" in raw else f"https://medium.com{raw}")
    if parsed.path.endswith("/_/graphql"):
        return "https://medium.com/_/graphql"
    return normalize_visit_url(raw)


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in values:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _sanitize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return _sanitize_dict(value, is_root=False)
    if isinstance(value, list):
        return [_sanitize_value(item) for item in value]
    return value


def _sanitize_dict(payload: dict[str, Any], *, is_root: bool) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for key, value in payload.items():
        if key in REMOVED_KEYS:
            continue

        if key == "requestHeadersSubset" and isinstance(value, dict):
            sanitized[key] = {
                header_key: header_value
                for header_key, header_value in value.items()
                if str(header_key).lower() != "referer"
            }
            continue

        if key == "visits" and isinstance(value, list):
            cleaned = [normalize_visit_url(item) for item in value if isinstance(item, str)]
            sanitized[key] = _dedupe_preserve_order(cleaned)
            continue

        if key == "samplePageUrls" and isinstance(value, list):
            cleaned = [normalize_visit_url(item) for item in value if isinstance(item, str)]
            sanitized[key] = _dedupe_preserve_order(cleaned)
            continue

        if key == "sampleRequestUrls" and isinstance(value, list):
            cleaned = [normalize_request_url(item) for item in value if isinstance(item, str)]
            sanitized[key] = _dedupe_preserve_order(cleaned)
            continue

        sanitized[key] = _sanitize_value(value)

    if is_root:
        sanitized["sanitizedForPublicPush"] = True
    return sanitized


def sanitize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _sanitize_dict(payload, is_root=True)


def _load_json(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Expected object JSON: {path}")
    return raw


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _regenerate_manifest() -> None:
    manifest = _load_json(MANIFEST_PATH) if MANIFEST_PATH.exists() else {}
    canonical_capture = str(manifest.get("canonicalCapture") or "captures/final/live_capture_2026-02-24.json")
    canonical_ops = str(manifest.get("canonicalOps") or "captures/final/live_ops_2026-02-24.json")
    canonical_implementation = str(
        manifest.get("canonicalImplementationOps") or "captures/final/implementation_ops_2026-02-24.json"
    )
    old_files = {
        str(item.get("path")): item
        for item in manifest.get("files", [])
        if isinstance(item, dict) and isinstance(item.get("path"), str)
    }

    files: list[dict[str, Any]] = []
    for path in sorted(FINAL_DIR.glob("*.json")):
        rel = str(path.relative_to(REPO_ROOT))
        payload = _load_json(path)
        old = old_files.get(rel, {})

        operation_names = payload.get("operationNames")
        mutation_names = payload.get("mutationNames")
        operation_count = (
            len(operation_names)
            if isinstance(operation_names, list)
            else int(old.get("operationNames", 0))
        )
        mutation_count = (
            len(mutation_names)
            if isinstance(mutation_names, list)
            else int(old.get("mutationNames", 0))
        )

        files.append(
            {
                "path": rel,
                "operationNames": operation_count,
                "mutationNames": mutation_count,
                "evidenceLevel": str(old.get("evidenceLevel", "sanitized_public_capture")),
                "isCanonical": bool(
                    old.get("isCanonical")
                    or rel in {canonical_capture, canonical_ops, canonical_implementation}
                ),
            }
        )

    manifest_out = {
        "generatedAt": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "canonicalCapture": canonical_capture,
        "canonicalOps": canonical_ops,
        "canonicalImplementationOps": canonical_implementation,
        "files": files,
    }
    _write_json(MANIFEST_PATH, manifest_out)


def main() -> int:
    if not FINAL_DIR.exists():
        raise SystemExit(f"Missing captures directory: {FINAL_DIR}")

    for path in sorted(FINAL_DIR.glob("*.json")):
        payload = _load_json(path)
        sanitized = sanitize_payload(payload)
        _write_json(path, sanitized)
        print(f"sanitized: {path.relative_to(REPO_ROOT)}")

    _regenerate_manifest()
    print(f"regenerated: {MANIFEST_PATH.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
