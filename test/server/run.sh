#!/usr/bin/env bash
# Start anchor using test/server/config.yaml.
#
# Copy the sample first:
#   cp test/server/config.yaml.sample test/server/config.yaml
# then edit catalog / warehouse / storage creds.
#
# For load testing (much faster than the default debug build):
#   CARGO_ARGS=--release ./test/server/run.sh
#
# Extra args after `--` go to the binary (currently only a config path,
# but kept for symmetry).

set -euo pipefail
cd "$(dirname "$0")/../.."

CONFIG=test/server/config.yaml
if [[ ! -f "$CONFIG" ]]; then
    echo "error: $CONFIG not found. Run:" >&2
    echo "  cp test/server/config.yaml.sample $CONFIG" >&2
    echo "and edit it first." >&2
    exit 1
fi

exec cargo run ${CARGO_ARGS:-} -p anchor -- "$CONFIG"
