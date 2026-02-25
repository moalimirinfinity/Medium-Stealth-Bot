#!/usr/bin/env bash
set -euo pipefail

ENV_FILE=".env.production"
TAG="${BOT_TAG:-programming}"

usage() {
  cat <<'EOF'
Usage: scripts/run_daily_live.sh [--env-file <path>] [--tag <slug>]

Options:
  --env-file <path>   Env profile to load (default: .env.production)
  --tag <slug>        Tag slug for the run (default: $BOT_TAG or "programming")
  -h, --help          Show help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file)
      ENV_FILE="$2"
      shift 2
      ;;
    --tag)
      TAG="$2"
      shift 2
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
done

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: $ENV_FILE"
  exit 1
fi

mkdir -p .data/scheduler
LOCK_DIR=".data/scheduler/run_daily_live.lock"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "Another scheduled run is already in progress. Exiting."
  exit 0
fi
cleanup() {
  rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup EXIT

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

timestamp="$(date -u +"%Y%m%dT%H%M%SZ")"
log_file=".data/scheduler/run_daily_live_${timestamp}.log"

{
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] scheduler run started"
  echo "env_file=$ENV_FILE"
  echo "tag=$TAG"

  kill_switch="$(printf '%s' "${OPERATOR_KILL_SWITCH:-false}" | tr '[:upper:]' '[:lower:]')"
  if [[ "$kill_switch" == "true" ]]; then
    echo "OPERATOR_KILL_SWITCH=true; skipping run."
    exit 0
  fi

  uv run bot profile-validate --env-path "$ENV_FILE"
  uv run bot start --quick-live --dry-run-first --tag "$TAG"
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] scheduler run completed"
} 2>&1 | tee -a "$log_file"
