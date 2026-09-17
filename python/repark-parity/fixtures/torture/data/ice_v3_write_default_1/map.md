# map — fixtures/torture/data/ice_v3_write_default_1 (committed Iceberg tables)

## Purpose

Seven Spark-written Iceberg **format-version 3** tables whose added columns
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
- `truth.json` — the writer record: banner, per-table schema plus seed outcome,
  and the 22 oracle cells (full-table rows or the error class and message).
- `record.py` — the recording script (Spark-only; run it, never a copy, from
  the repository root under the probe JVM). Re-recording is deterministic in
  rows, schema and errors; file names carry fresh UUIDs.
- `map.md` — this file.
