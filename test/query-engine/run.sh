#!/usr/bin/env bash
# Start a single-node Trino container pointed at this folder's etc/ config.
# Trino is used to create the target Iceberg table and to run verification
# queries against it — anchor itself only writes.
#
# Default host port: 8090 (anchor uses 8080). Override with TRINO_PORT=...
#
# The catalog in etc/catalog/rest.properties reaches the Iceberg REST
# catalog and MinIO on the host via host.docker.internal. The --add-host
# flag makes that work on Linux Docker as well as Docker Desktop.

set -euo pipefail
cd "$(dirname "$0")"

TRINO_PORT="${TRINO_PORT:-8090}"
TRINO_IMAGE="${TRINO_IMAGE:-trinodb/trino}"

if docker ps -a --format '{{.Names}}' | grep -q '^trino$'; then
    echo "trino container exists — removing it" >&2
    docker rm -f trino >/dev/null
fi

docker run --name trino  \
    -p "${TRINO_PORT}:8080" \
    --add-host=host.docker.internal:host-gateway \
    --volume "$PWD/etc:/etc/trino" \
    "$TRINO_IMAGE"
