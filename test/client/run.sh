#!/usr/bin/env bash
# Thin wrapper around generate.py. All defaults can be overridden via env
# vars, and any extra args are forwarded to the Python script (argparse
# last-wins, so `--batch-size 500` on the CLI beats the default below).
#
# Examples:
#   ./test/run.sh                              # 100 rec/batch, one batch / 100ms, forever
#   ./test/run.sh --batches 50                 # stop after 50 batches
#   ./test/run.sh --batch-size 500 --interval 0.05
#   ANCHOR_URL=http://prod:8080 ./test/run.sh
#   ./test/run.sh --dry-run                    # print one sample record

set -euo pipefail
cd "$(dirname "$0")"

exec python3 generate.py \
    --url "${ANCHOR_URL:-http://localhost:8080}" \
    --namespace "${ANCHOR_NS:-demo}" \
    --table "${ANCHOR_TABLE:-events}" \
    --batch-size "${BATCH_SIZE:-100}" \
    --interval "${INTERVAL:-0.1}" \
    "$@"
