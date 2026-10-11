#!/usr/bin/env bash
# Build, validate, deploy and run the trade_pipeline Databricks Asset Bundle.
#
# Usage: ./scripts/deploy.sh <build|validate|deploy|run> [extra arguments for `run`]
#   build     build the wheel locally into dist/ and list its modules (optional check)
#   validate  validate databricks.yml and the job YAML
#   deploy    validate, then deploy; the Bundle builds and uploads the wheel itself
#   run       start the job and wait for it; extra arguments go to `databricks bundle run`
#
# Environment: DATABRICKS_TARGET (default: dev). Authentication is the Databricks CLI's own:
# set DATABRICKS_CONFIG_PROFILE (or configure a DEFAULT profile). No credentials live here.
set -euo pipefail

TARGET="${DATABRICKS_TARGET:-dev}"
JOB="trade_pipeline" # resource key in jobs/resources/trade_pipeline.job.yml

cd "$(dirname "$0")/.." # repo root, so the paths in databricks.yml resolve

usage() {
  sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
}

require_cli() {
  command -v databricks >/dev/null || { echo "ERROR: databricks CLI not found on PATH" >&2; exit 1; }
}

build() {
  echo "==> Building wheel into dist/"
  rm -f dist/*.whl
  python -m pip wheel . --no-deps --wheel-dir dist
  echo "==> Wheel contents:"
  python -m zipfile -l dist/*.whl | grep '\.py'
}

validate() {
  require_cli
  echo "==> Validating bundle (target: $TARGET)"
  databricks bundle validate -t "$TARGET"
}

deploy() {
  validate
  echo "==> Deploying bundle (target: $TARGET); the wheel is built and uploaded by the bundle"
  databricks bundle deploy -t "$TARGET"
  echo "==> Deployed. Start the job with: ./scripts/deploy.sh run"
}

run() {
  require_cli
  echo "==> Running job '$JOB' (target: $TARGET)"
  databricks bundle run "$JOB" -t "$TARGET" "$@"
}

command="${1:-}"
[ -n "$command" ] && shift
case "$command" in
  build) build ;;
  validate) validate ;;
  deploy) deploy ;;
  run) run "$@" ;;
  *) usage ;;
esac
