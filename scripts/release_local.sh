#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/release_local.sh <version> [--no-push]

Examples:
  scripts/release_local.sh 0.2.0
  scripts/release_local.sh 0.2.0 --no-push
EOF
}

if [[ $# -gt 0 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
  usage
  exit 0
fi

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

VERSION="$1"
shift

PUSH_CHANGES="true"
if [[ $# -gt 0 ]]; then
  case "$1" in
    --no-push)
      PUSH_CHANGES="false"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
fi

if [[ $# -gt 0 ]]; then
  echo "Unexpected trailing arguments: $*"
  usage
  exit 1
fi

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Version must be semver-like: <major>.<minor>.<patch>"
  exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Git tree must be clean before release."
  exit 1
fi

TAG="v${VERSION}"
if git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null; then
  echo "Tag already exists: ${TAG}"
  exit 1
fi

echo "Running release checks..."
uv sync --group dev
uv run python -m compileall -q src
CAPTURE_MAX_AGE_DAYS=45 uv run python scripts/check_capture_integrity.py
uv run python scripts/check_capture_sanitization.py
uv run python scripts/check_response_contract_paths.py
uv run pytest -q
CONTRACT_REGISTRY_VALIDATE_RESPONSE_FIELDS=true uv run bot contracts --tag programming --no-execute-reads
uv run bot profile-validate --env-path .env.production.example

echo "Updating version files to ${VERSION}..."
python - "$VERSION" <<'PY'
from pathlib import Path
import re
import sys

version = sys.argv[1]
root = Path(".")

pyproject = root / "pyproject.toml"
init_py = root / "src" / "medium_stealth_bot" / "__init__.py"

pyproject_text = pyproject.read_text(encoding="utf-8")
pyproject_text_new = re.sub(
    r'^version = ".*"$',
    f'version = "{version}"',
    pyproject_text,
    count=1,
    flags=re.MULTILINE,
)
if pyproject_text_new == pyproject_text:
    raise SystemExit("Failed to update version in pyproject.toml")
pyproject.write_text(pyproject_text_new, encoding="utf-8")

init_text = init_py.read_text(encoding="utf-8")
init_text_new = re.sub(
    r'^__version__ = ".*"$',
    f'__version__ = "{version}"',
    init_text,
    count=1,
    flags=re.MULTILINE,
)
if init_text_new == init_text:
    raise SystemExit("Failed to update __version__ in __init__.py")
init_py.write_text(init_text_new, encoding="utf-8")
PY

git add pyproject.toml src/medium_stealth_bot/__init__.py
git commit -m "chore(release): prepare ${TAG}"
git tag -a "${TAG}" -m "Release ${TAG}"

if [[ "${PUSH_CHANGES}" == "true" ]]; then
  git push origin HEAD
  git push origin "${TAG}"
  echo "Release commit and tag pushed: ${TAG}"
else
  echo "Created release commit and tag locally (not pushed): ${TAG}"
fi
