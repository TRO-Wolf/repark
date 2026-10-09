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

- `test_c3_partitioned.py` — **C-3 fold 1 (2026-10-08):** the refusal cell's incomplete
  `format("postgres")` case now expects Spark's all-or-none sentence. pins: c-3/C-006
- `test_c3_partitioned.py` — **C-3 fold 1 (2026-10-08):**
  `test_a_partition_column_resolves_in_another_case_quoted_or_bare`: six spellings of a
  `"Mixed"` column partition on it; with `"Mixed"` and `"mixed"` both present an exact spelling
  picks its own column and an inexact one refuses as ambiguous. pins: c-3/C-011
- `test_c3_partitioned.py` — **C-3 fold 1 (2026-10-08):**
  `test_num_partitions_is_sparks_int_and_strides_have_a_ceiling`: `3000000000` is
  `NumberFormatException` through `jdbc()` and `format("postgres")`; `2147483647` over a span
  of 3 reads on three strides; 10 000 strides plan; 10 001, asked for or left after the shrink,
  refuse naming the row and the ceiling. pins: c-3/C-010
- `test_c3_partitioned.py` — **C-3 fold 2 (2026-10-08):**
  `test_the_keyword_door_hands_str_and_int_values_to_the_engine_door`: `jdbc(column=...)` with
  str and with int bounds plans the strides the `format("postgres")` door plans and reads the
  same rows; `numPartitions="4"` works and `"3000000000"` or `" 4"` refuse as `NumberFormat`;
  date, timestamp and timestamptz columns with date text refuse naming the row; `"abc"`
  refuses as the properties door does; an incomplete set refuses in Spark's sentence.
  pins: c-3/C-006
- `test_c3_partitioned.py` — **C-3 fold 1 (2026-10-08):** the declared cell adds Spark's own
  spellings: a `date`, `timestamp` and `timestamptz` column with date or timestamp text as
  bounds, through `format("postgres")` and through `properties`, refuses naming the row and
  never the bound; a date bound on an integer column is `NumberFormatException`.
  pins: c-3/C-009
- `c3_bench.py` — **C-3 (2026-10-08):** the benchmark harness of order R-3, not collected by
  pytest (no `test_` prefix, no test function). `load` creates `c3_bench.mixed` (10M rows by
  default, `C3_BENCH_ROWS`; nine columns of mixed types, a primary key) in the container
  `REPARK_PG_URL` names; `run` times `SELECT *` on RePark (unpartitioned, 4 strides, 8 strides
  on a pool of 8, 8 strides on the default pool), ConnectorX `read_sql` with an Arrow return
  (unpartitioned, 4, 8) and pandas `read_sql` over SQLAlchemy, each case `C3_BENCH_RUNS` times
  (default 3) in its own fresh interpreter, and prints a JSON report: per case the runs, the
  median, rows per second and the returned schema, then RePark's rows per second over
  ConnectorX's at the same setting; `drop` removes the schema. Run it from a scratch
  environment holding a release `repark` wheel, `connectorx`, `pandas`, `sqlalchemy`,
  `psycopg2-binary` and `pyarrow`, never from `.venv`, and hold the build lock around `run`.
  Delete the scratch environment before committing: the pre-commit `taplo` step walks the
  whole clone and fails on the TOML files inside third-party packages.
  The recorded run and its caveats are the C-3 ledger's §4; ConnectorX's `read_sql` is the
  bar order R-4 cites (§5). pins: c-3/C-007, C-008
- `test_c3_partitioned.py` — **C-3 (2026-10-07):** partitioned reads through the read door,
  on a session with no mounted source. `spark.read.jdbc` with Spark's four arguments,
  `format("postgres")` with the four options, the four spellings inside `properties`,
  `read_postgres` with a column in another case and a partitioned `dbtable` subquery each equal
  psycopg's reading of fourteen columns over `int4`, `int8` and `int2` columns, with 16
  strides on the default pool and with bounds wholly outside the data; NULL and out-of-bounds
  rows arrive exactly once; Spark's refusals keep Spark's class and sentence (the all-or-none
  rule, reversed bounds, `query` with a column, a missing column, `text` and `boolean`
  columns, a bound that is not a 64-bit integer); the one-stride cases read unpartitioned;
  `predicates` and the `date`, timestamp, `numeric` and float columns refuse naming
  `CONNECT-DECL-pg-partitioned-read`; filters with pushdown on and off, projection and limits
  compose; 16 strides on `pool_max_size = 2` open two connections and leave none busy; and
  twelve reads return every id exactly once while a second session moves rows across the
  strides. pins: c-3/C-006
- `test_c2_read.py` — **C-2d (2026-10-07):** the mounted source and the `read_postgres`
  door against C-0 (`sslmode = "disable"`): every mapped type equals psycopg's reading; `read.jdbc`
  and `format("postgres")` with `query` equal it (the partitioned-read cell moved to
  `test_c3_partitioned.py` with C-3); DDL refuses read-only and DML refuses not-implemented
  through the Spark door, and DDL answers `CONNECT-DECL-pg-ddl` through `repark.sql`, with
  nothing written; `ping()` succeeds and names the source when unreachable; `timestamp` is
  placed in the session zone, kept as the wall clock under `prefer_timestamp_ntz`, and a gap
  wall clock refuses leaving no busy backend; EXPLAIN shows the pushed and residual split
  through `spark.sql`, the DataFrame and `repark.sql` `EXPLAIN VERBOSE` (placeholders, never
  the value, never the endpoint). pins: c-2/C-105
  **Fold 1 (2026-10-07):** `test_a_java_form_session_zone_places_the_wall_clock_at_its_offset`
  (six cells: `Z`, `UT`, `GMT+8`, `UTC+05:30`, `-8`, `+3`) places a wall clock at the
  canonical offset, equal to the `TIMESTAMP` literal. pins: c-2/C-110
  `test_ddl_and_dml_refuse_through_both_doors` adds `CREATE SCHEMA`, `CREATE SCHEMA IF NOT
  EXISTS` and `CREATE DATABASE` under `pg` on `repark.sql`; no schema reaches Postgres.
  pins: c-2/C-112
- `test_c2_catalog_and_limit.py` — **C-2d fold 1 (2026-10-07):** a New York session mounting `pg`, and `unpushed`
  with `pushdown_limit = "false"`, in Spark's display style. A new file, so `test_c2_read.py`
  stays under its ceiling. pins: c-2/C-117
  - `test_catalog_apis_answer_for_a_mounted_source` (N5): `tableExists` is `True` for an
    existing relation and `False` for a missing one; `SHOW TABLES IN pg.<schema>` and `SHOW
    SCHEMAS IN pg` are empty; `writeTo` and `saveAsTable` refuse with the read-only text and
    `CONNECT-DECL-pg-ddl`; nothing is created. pins: c-2/C-115
  - `test_a_limit_never_reaches_a_refused_value_past_it` (N4) uses the verifier's 200 000-row
    table with `NaN` at row 150 000. `LIMIT 5` returns 5 rows with `pushed_limit=5`, and
    `show(3)` prints 3 rows and `limit(5)` returns 5, with `pushDownLimit` on and off. A
    `count(n)` refuses.
  - `test_a_refused_value_fails_only_a_read_that_reaches_its_row` (N4): with the limit kept in
    the engine, `limit(3)` over a `NaN` in row 4 and `limit(4)` over a gap wall clock in row 5
    return their rows, and the full reads refuse naming their rows. pins: c-2/C-114
  - `test_a_read_postgres_frame_names_its_relation_in_the_plan` (S3): `read.jdbc`'s logical plan
    names `<schema>.vals` and a `query` frame names `jdbc`, never `?table?`. pins: c-2/C-116
- `test_c2_federated.py` — **C-2d (2026-10-07):** sketch §5.4: an Iceberg memory-catalog
  table joined with a Postgres table on an integer key and a `timestamptz`, one filter pushed
  and one residual, through `spark.sql` and the DataFrame join, equal to the psycopg + pyarrow
  join; EXPLAIN has one `IcebergTableScan`, one `PostgresScanExec` and the hash join above; the
  physical plan, normalised, equals the committed expectation. pins: c-2/C-106
  **Fold 1 (2026-10-07):** `test_a_wall_clock_past_2099_matches_the_timestamp_literal`: in New
  York, `2099-07-01`, `2100-07-01` and `2100-01-15` at noon read the same `unix_micros` as the
  Iceberg `TIMESTAMP` literals; the equality filter on `2100-07-01 12:00` returns 1 row and the
  join on the timestamp returns all 3. pins: c-2/C-109
- `test_c2_credentials.py` — **C-2d (2026-10-07):** sketch §5.7: a role with a random
  32-hex password mounted by key, by URL, with a wrong password, on a closed port and through a
  one-connection pool; a subprocess at `RUST_LOG=trace` with Python logging at `DEBUG` drives
  `sources()`, `ping()`, EXPLAIN (both formats), an authentication failure, an unreachable
  host, a missing relation, a refused `NaN`, a lock timeout, a pool timeout and a `read.jdbc`
  URL with a password; neither password nor any URL form appears on stdout or stderr.
  pins: c-2/C-107
- `test_c4_write.py` — **C-4 step 2 (2026-10-09):** Postgres INSERT routing on the Spark
  door and the writer, fourteen cells: rows land through VALUES and SELECT with an empty-frame
  return; every declared type round-trips byte-identical through the read path (`nu` by
  value: the door writes the Arrow scale, the seed carries dscale 1); reordered lists write
  by name and unlisted columns default; a duplicate key stores nothing and records nothing;
  a DST-zone session writes read timestamps back as their wall clocks; a view and an
  INSERT-rule target take the row path and say why on both doors; a named GENERATED ALWAYS
  identity column refuses with the core text on both doors; default and `write.path=row`
  writer writes agree byte for byte; a write-only role names the missing SELECT privilege
  and an unmapped column names its row even when unnamed (the two owner questions);
  non-append modes name the modes row and UPDATE names the upsert row while REPLACE parses
  nowhere here and MERGE/CTAS keep the pg-ddl text.
  pins: c-4/C-013, C-014
- `test_c2_read.py` — **C-4 step 2 (2026-10-09):** `test_ddl_and_dml_refuse_through_both_doors`
  drops its INSERT case (append INSERT writes now; the write pins live in
  `test_c4_write.py`); UPDATE and DELETE keep refusing. pins: c-4/C-013

## Pointers

- Up: [../map.md](../map.md)
- Container: [../../../../scripts/dev/pg/map.md](../../../../scripts/dev/pg/map.md)
- Contract: [../../../../docs/testing.md](../../../../docs/testing.md)
