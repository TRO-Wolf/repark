# map — python/repark-parity/tests/live_db

## Purpose

C-0 (2026-10-02): live Postgres cells for the 1.6 connector units and the 1.7
capture harness. Each test gets a schema, a publication name and a slot name
that carry one random tag; all three drop on exit. The five cdc S0 pins are
`xfail(strict=True)` until the producer arrives. Cells skip when
`REPARK_PG_URL` is unset.

## Contents

- `__init__.py` — empty package marker.
- `conftest.py` — `pg_live` and `pg_live_factory`; unique names; drop the
  publication, the slot and the schema on exit. `psycopg` is imported after
  the `REPARK_PG_URL` skip so CI collection without the extra stays green.
- `test_c0_cdc_scenarios.py` — `test_fixture_isolates_names` (green) and the
  five S0 pins: crash after commit, snapshot-to-WAL handover, replay and
  duplicate delivery, schema change plus partial update image, lost slot.

- `test_c2_read.py` — **C-2d (2026-10-07):** the mounted source and the `read_postgres`
  door against C-0 (`sslmode = "disable"`): every mapped type equals psycopg's reading; `read.jdbc`
  and `format("postgres")` with `query` equal it; the partitioned-read arguments refuse
  (`CONNECT-DECL-pg-partitioned-read`); DDL refuses read-only and DML refuses not-implemented
  through the Spark door, and DDL answers `CONNECT-DECL-pg-ddl` through `repark.sql`, with
  nothing written; `ping()` succeeds and names the source when unreachable; `timestamp` is
  placed in the session zone, kept as the wall clock under `prefer_timestamp_ntz`, and a gap
  wall clock refuses leaving no busy backend; EXPLAIN shows the pushed and residual split
  through `spark.sql`, the DataFrame and `repark.sql` `EXPLAIN VERBOSE` (placeholders, never
  the value, never the endpoint). pins: c-2/C-105
- `test_c2_federated.py` — **C-2d (2026-10-07):** sketch §5.4: an Iceberg memory-catalog
  table joined with a Postgres table on an integer key and a `timestamptz`, one filter pushed
  and one residual, through `spark.sql` and the DataFrame join, equal to the psycopg + pyarrow
  join; EXPLAIN has one `IcebergTableScan`, one `PostgresScanExec` and the hash join above; the
  physical plan, normalised, equals the committed expectation. pins: c-2/C-106
- `test_c2_credentials.py` — **C-2d (2026-10-07):** sketch §5.7: a role with a random
  32-hex password mounted by key, by URL, with a wrong password, on a closed port and through a
  one-connection pool; a subprocess at `RUST_LOG=trace` with Python logging at `DEBUG` drives
  `sources()`, `ping()`, EXPLAIN (both formats), an authentication failure, an unreachable
  host, a missing relation, a refused `NaN`, a lock timeout, a pool timeout and a `read.jdbc`
  URL with a password; neither password nor any URL form appears on stdout or stderr.
  pins: c-2/C-107

## Pointers

- Up: [../map.md](../map.md)
- Container: [../../../../scripts/dev/pg/map.md](../../../../scripts/dev/pg/map.md)
- Contract: [../../../../docs/testing.md](../../../../docs/testing.md)
