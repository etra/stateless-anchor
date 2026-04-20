# Trino for creating tables and verifying writes

Anchor is write-only — it does not create Iceberg tables and has no read
path. Trino fills both gaps: we apply the schema DDL through it, and we
query the table after a load test to confirm records landed.

## What's here

- `run.sh` — starts a single-node Trino container; mounts `etc/` as
  `/etc/trino`. Host port defaults to `8090` (anchor owns `8080`).
- `etc/` — Trino config. `node.properties`, `jvm.config`, `config.properties`,
  `log.properties` are the Trino minimums; `catalog/rest.properties`
  defines the Iceberg REST catalog connection.

## Prerequisites

Trino's container needs to reach both the Iceberg REST catalog and the
S3-compatible object store. The catalog config assumes both run on the
host and uses `host.docker.internal` (which works on Docker Desktop;
`run.sh` adds `--add-host=host.docker.internal:host-gateway` so it also
works on Linux Docker).

If you run the catalog + MinIO in Docker on a user-defined network,
change the hosts in `etc/catalog/rest.properties` (and run Trino on the
same network or with container-name resolution).

Expected endpoints (match `test/server/config.yaml.sample`):
- REST catalog: `http://host.docker.internal:8181`
- MinIO / S3:   `http://host.docker.internal:9000`
- Creds:        `minioadmin` / `minioadmin`
- Warehouse:    `s3://warehouse`

## Usage

```sh
# start trino
./run.sh

# open the CLI against the Iceberg catalog
docker exec -it trino trino --catalog rest --schema demo

# or apply the table DDL non-interactively
docker exec -i trino trino -f - < ../client/create_table.sql

# stop / clean up
docker rm -f trino
```

## Example verification queries

After running `./test/client/run.sh` against anchor for a bit:

```sql
-- row count and partition count
SELECT count(*) AS rows, count(DISTINCT ts) AS partitions
FROM rest.demo.events;

-- how many duplicate event_ids are in the table
SELECT COUNT(*) AS duplicates
FROM (
    SELECT event_id
    FROM rest.demo.events
    GROUP BY event_id
    HAVING COUNT(*) > 1
);

-- sample recent events
SELECT ts, event_id, event_time, event_type, country
FROM rest.demo.events
ORDER BY event_time DESC
LIMIT 20;

-- verify partitioning is happening — one file per partition, roughly
SELECT partition, record_count, file_count
FROM rest.demo."events$partitions";
```

## Port / catalog naming notes

- Host port for Trino: **8090** by default. `TRINO_PORT=9999 ./run.sh` to change.
- Catalog name: **`rest`** (derived from the properties filename). This is
  what lets the DDL in `test/client/create_table.sql` reference
  `rest.demo.events` without Trino-specific rewrites.
