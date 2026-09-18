# map — fixtures/torture/data/ice_v3_write_default_1 (committed Iceberg tables)

## Purpose

Eleven Spark-written Iceberg **format-version 3** tables whose added columns
carry `initial-default` / `write-default`, checked in so JVM-free CI can pin
RePark filling omitted columns from `write_default` on every write path — the
ICE-V3-WRITE-DEFAULT-1 scenario. Spark 4.1.2 refuses
`ALTER TABLE … ADD COLUMN … DEFAULT`, so the defaults arrive through the Java
`UpdateSchema` API (`record.py`). Written 2026-09-17 by PySpark 4.1.2 +
iceberg-spark-runtime-4.1_2.13:1.11.0 on a Hadoop catalog at a local
filesystem warehouse.

RePark follows the absolute paths baked into the metadata — a copy at any
other path is unreadable — so the suite copies each table onto exactly
`/tmp/repark-ice-v3-write-default-1/ns/<table>` under a directory lock, the
same contract the `ice_spark_table_1` fixture uses, and removes the copy when
the test exits. `.crc` sidecars are stripped (Hadoop checksum files the reader
never needs). Every defaulted table carries one post-add Spark write, so the
current snapshot sits on the post-add schema: a schema-only head (no snapshot
after the Java `addColumn`) reads pre-add rows as NULL on this engine while
Spark reads the initial default — a read-path gap outside this unit, worked
around here by construction.

## Contents

- `ns/defaults/` — `(id INT, name STRING)` + `c INT` default 5; two seed rows.
  The main table: column-list INSERT, MERGE NOT MATCHED, `writeTo`/`saveAsTable`
  append, `DEFAULT` keyword (VALUES and SELECT position), explicit NULLs, the
  arity-error shapes, `insertInto`, the extra-column refusal, and the trailing
  `INSERT OVERWRITE` cell all run here.
- `ns/strdef/` — `(id INT)` + `s STRING` default `'hi'`; one pre-add seed row.
- `ns/decdef/` — `(id INT)` + `d DECIMAL(10,2)` default `3.14`; one explicit
  seed row. Spark writes the omitted-default row but cannot read it back
  (`Cannot cast default value to long: 3.14`); the pin asserts RePark's filled
  value and the live tier re-asserts Spark's read error.
- `ns/temporal/` — `(id INT)` + `dt DATE` default `2024-10-04` and `ts`
  `TIMESTAMPTZ` default `2024-10-04T00:00:00+00:00`; one pre-add seed row.
- `ns/differ/` — `(id INT)` + `e INT` with initial-default 5 and write-default 7
  (`updateColumnDefault`); one pre-add seed row reading 5.
- `ns/nodefault/` — `(id INT, name STRING, note STRING)`, no defaults; one row.
  The unchanged-behavior control.
- `ns/required/` — `(id INT, req INT NOT NULL)`, no default; one row. Omitted
  writes refuse on both engines.
- `ns/pdflt/` — `(id INT, name STRING) PARTITIONED BY (id)` + `c INT` default 5;
  two pre-add seed rows and one post-add row. The L-01 partition-overwrite
  table: dynamic `PARTITION (id) (cols)`, static `PARTITION (id = 10) (name)`,
  the partitioned whole-table column list, and `overwritePartitions()` run here
  (round 5, run 21b, ruling Q-21b-3).
- `ns/dfltow/` — `(id INT, name STRING)` + `c INT` default 5. The L-03 table:
  `DEFAULT` as a value on `INSERT OVERWRITE`, VALUES and named-list forms
  (ruling Q-21b-4).
- `ns/dfltsat/` — the same shape, kept separate because the L-04 cell REPLACES
  it: Spark's `saveAsTable(mode="overwrite")` on an Iceberg table drops `c` and
  narrows the schema to the frame, which `truth.json`'s `schema_after` records
  (ruling Q-21b-5).
- `ns/nodef/` — `(id INT, name STRING, c INT)`, NO default, one seed row. The
  roll-call table: a missing nullable column without a default is accepted and
  written NULL on both writer surfaces (ruling Q-21b-6).
- `truth.json` — the writer record: banner, per-table schema plus seed outcome,
  the 39 oracle cells (full-table rows or the error class and message), and
  `schema_after` for the two tables whose schema the cells change. Six of the
  cells — `V01_overwrite_partitions_missing_defaulted_column`, the three
  `V02_*` DEFAULT-outside-the-INSERT-list refusals and the two `MIX_*`
  static-plus-dynamic PARTITION cells — were measured 2026-09-17 by the
  orchestrator's `probe_wd2.py` (same Spark 4.1.2 + Iceberg 1.11.0 banner, its
  own `pdflt` / `dflt` / `p2` tables) and copied in verbatim (run 21b round 2,
  rulings Q-21b-8 … Q-21b-10); `record.py` does not re-record them.
- `record.py` — the recording script (Spark-only; run it, never a copy, from
  the repository root under the probe JVM). Re-recording is deterministic in
  rows, schema and errors; file names carry fresh UUIDs. Ruff-clean
  (2026-09-17): import, `next()`, and formatter wraps only — the recorded
  bytes are untouched. It strips `.crc` sidecars itself, so the checked-in tree
  is exactly what a re-run produces. Re-recorded 2026-09-17 (round 5, run 21b):
  every pre-existing cell came back identical apart from a Py4J gateway object
  id inside one recorded error message.
- `map.md` — this file.
