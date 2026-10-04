# map — scripts/dev/pg

## Purpose

The one Compose file `make pg-up` starts. Image `postgres:16-alpine`, logical
replication on from the first run, bounded to two cores and two gigabytes,
labelled `repark.disposable=1`.

## Contents

- [compose.yaml](compose.yaml) — the `postgres` service: server flags, password
  `repark`, `127.0.0.1::5432`, shm 256m, `pids_limit` and `deploy.resources.limits.pids`
  both 256 (Compose v2 requires the two to match), health check `pg_isready`.

## Pointers

- Up: [../map.md](../map.md)
