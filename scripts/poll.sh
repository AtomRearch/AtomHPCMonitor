#!/usr/bin/env bash
# For a cluster-approved periodic runner. Does not submit any computation.
set -euo pipefail
PROJECT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
ACCOUNT=$(id -un)
CONFIG=${ATOMHPC_CONFIG:-"$PROJECT_DIR/config.local.json"}
PYTHON_BIN=${ATOMHPC_PYTHON:-python3}
cd "$PROJECT_DIR"
exec "$PYTHON_BIN" -m atomhpcmonitor --config "$CONFIG" --account "$ACCOUNT" poll
