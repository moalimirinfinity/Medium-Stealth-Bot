#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from sanitize_captures import FINAL_DIR, REMOVED_KEYS, normalize_request_url, normalize_visit_url


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected object JSON in {path}")
    return payload


def _walk(node: Any, *, prefix: str = "$") -> list[tuple[str, str]]:
    issues: list[tuple[str, str]] = []
    if isinstance(node, dict):
        for key, value in node.items():
            path = f"{prefix}.{key}"
            if key in REMOVED_KEYS:
                issues.append((path, "forbidden_key_present"))

            if key == "requestHeadersSubset" and isinstance(value, dict):
                for header in value:
                    if str(header).lower() == "referer":
                        issues.append((path, "forbidden_referer_present"))

            if key == "visits" and isinstance(value, list):
                for idx, item in enumerate(value):
                    if not isinstance(item, str):
                        issues.append((f"{path}[{idx}]", "visit_not_string"))
                        continue
                    if item != normalize_visit_url(item):
                        issues.append((f"{path}[{idx}]", "visit_not_normalized"))

            if key == "samplePageUrls" and isinstance(value, list):
                for idx, item in enumerate(value):
                    if not isinstance(item, str):
                        issues.append((f"{path}[{idx}]", "sample_page_url_not_string"))
                        continue
                    if item != normalize_visit_url(item):
                        issues.append((f"{path}[{idx}]", "sample_page_url_not_normalized"))

            if key == "sampleRequestUrls" and isinstance(value, list):
                for idx, item in enumerate(value):
                    if not isinstance(item, str):
                        issues.append((f"{path}[{idx}]", "sample_request_url_not_string"))
                        continue
                    if item != normalize_request_url(item):
                        issues.append((f"{path}[{idx}]", "sample_request_url_not_normalized"))

            issues.extend(_walk(value, prefix=path))
        return issues

    if isinstance(node, list):
        for idx, item in enumerate(node):
            issues.extend(_walk(item, prefix=f"{prefix}[{idx}]"))
    return issues


def main() -> int:
    if not FINAL_DIR.exists():
        print(f"Missing captures directory: {FINAL_DIR}")
        return 1

    errors: list[str] = []
    for path in sorted(FINAL_DIR.glob("*.json")):
        payload = _load_json(path)
        marker = payload.get("sanitizedForPublicPush")
        if marker is not True:
            errors.append(f"{path}:missing_sanitizedForPublicPush=true")

        for location, reason in _walk(payload):
            errors.append(f"{path}:{reason}:{location}")

    if errors:
        print("Capture sanitization check failed:")
        for item in errors:
            print(f"- {item}")
        return 1

    print("Capture sanitization check passed")
    print(f"- directory: {FINAL_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
