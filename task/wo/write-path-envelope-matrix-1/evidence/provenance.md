# Provenance — write-path envelope matrix (measurement only)

Base: `origin/main` at `a4284f4d`; branch `docs/write-path-envelope-matrix-1`. The RePark leg is the release
wheel built from `fdc61f26`; `a4284f4d` differs from it only in documents, so the wheel stands for `main`.
Paths below are placeholders: `<scratch>` is the session scratch directory, `<warehouse>` a per-shard catalog
root, `<oracle-python>` the PySpark 4.1.2 interpreter, `<ivy-jar>` the Iceberg Spark runtime jar,
`<venv-main-fdc61f26>` the RePark venv, `<main-wheel>` the built wheel, `<jdk-17>` the Zulu 17 JDK.

## Artifacts and their hashes

| Leg | Artifact | sha256 (wheel, or jar) | Python | pyarrow | Version string |
|---|---|---|---|---|---|
| `spark` | Spark 4.1.2 + Iceberg `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, `local[1]`, UI off, log level ERROR, `IcebergSparkSessionExtensions`, a Hadoop catalog with a private name | jar `d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242` | 3.12.3 (JDK 17.0.15) | 25.0.1 | spark 4.1.2 |
| `repark-main-fdc61f26` | `repark-1.5.5-cp312-abi3-manylinux_2_39_x86_64.whl` (release build of `fdc61f26`) | `07f94d4dcf9852b7a6f895382efe4fdc3bbb6c1d090c807e15d9191850ead479` | 3.12.3 | 26.0.1 | 1.5.5 |

The first JSONL line of every shard (`"record": "leg"`) records the same facts, the session time zone, the CREATE
and seed statement templates per door and kind, and, on the RePark native shard, the resolved native predicate
spellings with every attempt.

## Commands run (placeholders for paths)

One shell script ran every shard serially, deleting each shard's warehouse afterwards. Spark first, then RePark.
Shards: `spark-sql:select`, `spark-sql:delete`, `spark-sql:update`, `spark-sql:insert`, `spark-sql:ctas`,
`spark-sql:merge`, `filter-str f-expr`, and on the RePark leg also `native`.

```
JAVA_HOME=<jdk-17> <oracle-python> -I probe_envelope.py --engine spark --label spark \
    --warehouse <warehouse> --out raw/spark-<shard>.jsonl --ivy-jar <ivy-jar> --only <tokens>
<venv-main-fdc61f26>/bin/python -I probe_envelope.py --engine repark --label repark-main-fdc61f26 \
    --warehouse <warehouse> --out raw/repark-main-<shard>.jsonl --wheel <main-wheel> --only <tokens>
python3 -I probe_envelope.py --render --spark raw/spark-*.jsonl --main raw/repark-main-*.jsonl --tables-dir .
uvx ruff@0.15.22 format probe_envelope.py && uvx ruff@0.15.22 check probe_envelope.py
```

Cell counts: Spark 720 (answered 677, refused 43); RePark 936 (answered 807, refused 123, not expressible 6).
The whole matrix was run twice from the same script; the label counts were identical. The committed raw files
are the second run. Wall clock: the Spark leg about 12 minutes, the RePark leg about 3 minutes.

## Plan

36 predicates (`p01`–`p36`, texts in the table file) by six seats by doors. `spark-sql` runs `select` on v2-cow and
the five write seats on v2-cow, v2-mor and v3 (36 + 540 cells); `filter-str` and `f-expr` run `select` and
`insert` (`.write.insertInto`) on v2-cow (144 cells); `native` runs all six seats on v2-cow (216 cells, RePark
only). Every cell uses fresh tables named after the cell: `t`, and `t2` (insert) or `t3` (CTAS).

## Per-cell protocol

Create `t` (and `t2` for the insert seat), seed it with one `INSERT ... VALUES` of eight rows, read `t`
(`SELECT * ... ORDER BY id`), run the door's own `SELECT id FROM t WHERE P` as the cell's reference select
(`pre_select`), run the seat statement, read `t` again, and for the insert and CTAS seats read the target.
Snapshot count and `current-snapshot-id` come from the newest metadata JSON. On v3 the lineage column
`_last_updated_sequence_number` is read per id before and after (`lineage_before`, `lineage_after`;
`lineage_advanced` is the ids whose value changed). Rows are stored as `[id, digest, flag]`, where the digest is a
hash of the normalised non-`id`, non-`flag` columns, so equal data compares equal across engines (floats keep NaN,
infinities and the sign of zero; decimals are normalised; time stamps are converted to UTC). The seat's id set is:
the selected ids (`select`), the ids whose rows disappeared (`delete`), the ids whose `flag` became `'z'`
(`update`, `merge`), the ids in the target table (`insert`, `ctas`, the DataFrame `insertInto`). `damaged` is the
ids whose other columns changed or whose flag changed to something other than `'z'` (write seats on `t`), or
whose copied row differs from the same engine's source row (insert and CTAS). `lost` is the ids that
disappeared from `t` on an update or merge.

## Label rules (mechanical; implemented in `--render`)

For a facade cell against the Spark cell with the same key: either side panicking is `PANIC`; both refusing is
`BOTH-REFUSED`; RePark accepting where Spark refused is `ACCEPTED-WHERE-SPARK-REFUSED`; the reverse is
`REFUSED-WHERE-SPARK-ACCEPTED`. When both accept: a `select` cell whose id set differs is `WRONG-READ`; a write
cell whose id set, `damaged` or `lost` differs from Spark's, or whose id set differs from RePark's own
`pre_select`, is `WRONG-WRITE`; otherwise a v3 cell whose `lineage_advanced` differs is `LINEAGE-DIFFER`.
`EXTRA-COMMIT` is RePark adding more snapshots than Spark to the source table or the target table (or any
snapshot on a `select`); `FEWER-COMMIT` is the reverse. A cell with none of these is `EQUAL`.
Native cells are judged against the native door's own `select`: a write seat whose id set differs from the
native `select` is `SEAT-INCONSISTENT`, one that damaged other data is `WRONG-WRITE`.
`KNOWN-EMPTY-DML` is added to a write-seat cell that has `WRONG-WRITE`, `EXTRA-COMMIT`, `FEWER-COMMIT`,
`LINEAGE-DIFFER` or `SEAT-INCONSISTENT` when its predicate is one whose `SELECT` Spark's optimised plan folds to
`LocalRelation <empty>` (read from the Spark leg, field `plan_empty`; only `p04` does).

## Deviations from the brief and choices the brief left open

- **Labels added.** `NATIVE-REFUSED` (the native door refused a statement; the brief's list has no label for a
  native refusal), `FEWER-COMMIT` (the reverse of `EXTRA-COMMIT`; Spark commits an empty snapshot where RePark
  commits none), and `SETUP-REFUSED` (table creation or seeding refused; none occurred).
- **Seed values the brief left open.** Columns not named in a row's description are `NULL`. Row 3's `st` is
  `(3, NULL)`; row 4's `i` is `NULL`. Row 8 repeats row 1 with `id` 8 and `flag` `'f8'`. `d` values are written as
  `CAST('NaN' ...)`, `CAST('-0.0' ...)`, `CAST('Infinity' ...)` so the sign of zero survives parsing.
- **Time zone.** Both engines run with `spark.sql.session.timeZone = America/New_York`, so the seed's
  `2026-03-08 02:30` falls in the daylight-saving gap on both. The RePark facade defaults to UTC and takes the
  setting from the builder; the leg header records `session_time_zone`. The native door is left at its own
  default and has no `current_timezone()` routine (`native_time_zone` is `null`).
- **Table kinds.** `v3` uses copy-on-write for delete, update and merge, as the Step 0.3 probe did; `v2-mor` uses
  merge-on-read for all three. `write.merge.mode` is set alongside the delete and update modes.
- **Native door.** `CREATE TABLE` refuses a `MAP` column (`Unsupported SQL type Map`), so the native `t` has no
  `m` column and `p34` (`m['k'] = 1`) is `NOT-EXPRESSIBLE` on every native seat (6 cells). Native column types are
  the Spark spellings, which the door accepts. Each predicate's native spelling is resolved once at the start of the
  native shard: the Trino-style spelling is tried first (`IS NOT DISTINCT FROM`, `CAST(... AS VARCHAR)`,
  `is_nan`, `contains`, `cardinality`, `arr[1]`, `TIMESTAMP` for the NTZ literal), then the Spark spelling; a
  candidate is taken when it is not refused at parse or analysis time (a run-time refusal still counts as
  expressible). The resolved spellings are in the native shard's header (`spelling_resolution`) and in each native
  cell's `spelling` field. The native door's reference is Trino and no Trino oracle exists, so native cells are
  judged only against the door's own `select`.
- **MERGE text.** The target is aliased `t` and the source `s`, as in the brief; the fresh table's real name is
  used where the brief writes `t`.
- **CTAS.** `CREATE TABLE t3 AS SELECT * FROM t WHERE P` with no `USING` clause, on every door.
- **Insert target.** `t2` is created with `t`'s columns and table kind before the statement.
- **Ordering of the render.** The render step was edited (an information section was added to the summary, and one
  lint fix) while the second run was in progress; no measurement code changed after that run began.
