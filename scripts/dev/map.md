# map — scripts/dev

## Purpose

C-0 (2026-10-02): operator scripts for the disposable live-database container. One
Postgres per `up`, never an Airflow-managed database. Logical replication is on from
the first run so the 1.7 harness changes nothing here.

## Contents

- [pg/](pg/map.md) — the Compose service: `postgres:16-alpine`, `wal_level=logical`,
  resource limits, health check, random localhost port.
- [pg_disposable.sh](pg_disposable.sh) — `up` / `down` / `url` / `reap`. Project name
  `repark-pg-<user>-<pid>`. `up` exits 3 when four disposable containers already run.
  `reap` removes labelled containers older than two hours, and any labelled container that is not running (with its anonymous volume and project network) whatever its age: the recovery after a reboot, when the `/tmp` or `$XDG_RUNTIME_DIR` state file is gone (owner, 2026-10-03).

## Pointers

- Up: [../map.md](../map.md)
- Live-cell contract: [../../docs/testing.md](../../docs/testing.md)
