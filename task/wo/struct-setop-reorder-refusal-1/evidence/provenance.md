# Provenance — set-operation domain table (measurement only)

Base: `origin/main`, now `a4284f4d7`. The measured RePark wheel is the release build of `fdc61f262`.
Between the two commits `main` changed three files: `scripts/check_map_size.py` (one size-baseline
number, for `task/map.md`), `task/map.md`, and the hand-off document
`task/handoff-frontier-2026-11-07.md`. No product code moved. Paths below are placeholders:
`<warehouse-spark>` and `<warehouse-repark>` are per-leg catalog roots, `<oracle-python>` the PySpark
4.1.2 interpreter, `<ivy-jar>` the Iceberg Spark runtime jar, `<venv-main-fdc61f26>` the RePark venv,
`<main-wheel>` the built wheel, `<jdk-17>` the Zulu 17 JDK.

## Artifacts and their hashes

| Leg | Artifact | sha256 | Interpreter or venv | Versions |
|---|---|---|---|---|
| `spark` | Spark 4.1.2 + `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` | jar `d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242` | `<oracle-python>` (JDK 17.0.15), Python 3.12.3 | spark 4.1.2; `spark.sql.ansi.enabled` true; `spark.sql.caseSensitive` false (both defaults, not changed) |
| `repark-main-fdc61f26` | `repark-1.5.5-cp312-abi3-manylinux_2_39_x86_64.whl` (release build of `fdc61f26`) | `07f94d4dcf9852b7a6f895382efe4fdc3bbb6c1d090c807e15d9191850ead479` | `<venv-main-fdc61f26>`, Python 3.12.3, pyarrow 26.0.1 | repark 1.5.5 (main has not bumped its version) |

The first line of each raw file records these values. The RePark header records `wheel_file`,
`wheel_sha256`, `repark_file` (the venv copy, scrubbed) and `native_config`. The Spark header records
the jar and its hash, the JDK, `ansi_enabled` and `case_sensitive`. The PyPI leg was not run (the brief
says it was identical on every Step 0.3 cell).

## Commands run (placeholders for paths)

```
JAVA_HOME=<jdk-17> <oracle-python> -I probe_setop.py --engine spark --label spark \
    --warehouse <warehouse-spark> --out raw/spark.jsonl --ivy-jar <ivy-jar>
<venv-main-fdc61f26>/bin/python -I probe_setop.py --engine repark --label repark-main-fdc61f26 \
    --warehouse <warehouse-repark> --out raw/repark-main-fdc61f26.jsonl --wheel <main-wheel>
python3 -I probe_setop.py --render --spark raw/spark.jsonl --main raw/repark-main-fdc61f26.jsonl \
    --table setop-table.md
uvx ruff@0.15.22 format probe_setop.py && uvx ruff@0.15.22 check probe_setop.py
```

Each leg runs against an empty warehouse. The committed raw files come from the last full run of each
leg, after the final edit to `probe_setop.py`. Before that run the by-name reading was drafted to
refuse on any missing field; the RePark leg showed NULL-filled by-name pairing for `partial-overlap`,
so the reading was corrected and both legs were run again. The by-name reading was then made to ignore
case, as Spark does by default, and both legs were run a last time. The outcome counts did not change
across these runs; only the predicted column and the pairing columns moved.

Cell counts per leg (each leg: 200 cells = 80 `sql` door + 80 `native` door + 40 `df` door):

- Spark: 72 answered, 48 refused, 80 `ORACLE-MISSING` (the `native` door has no Spark reference).
- RePark: 147 answered, 49 refused, 4 `SETUP-REFUSED` (all four `map-value-reordered` `native` insert
  cells), 0 panics.

## Shapes: the spellings run

Spark spelling is used on the `sql` and `df` doors; the `native` door spells arrays as `ARRAY[...]` and
maps as `MAP(ARRAY['k'], ARRAY[...])`. The INSERT table's column is `s` with the left operand's type.
The right operand for INTERSECT and EXCEPT is the set-right variant the brief asks for; the UNION,
UNION ALL and DataFrame cells use the table's right operand.

| Shape id | Left (column type of the INSERT table) | Right (UNION, UNION ALL, DataFrame) | Right (INTERSECT, EXCEPT) |
| --- | --- | --- | --- |
| `same-order` | `named_struct('a', 1, 'b', 2)` (`STRUCT<a: INT, b: INT>`) | `named_struct('a', 3, 'b', 4)` | `named_struct('a', 1, 'b', 2)` |
| `reordered` | `named_struct('a', 1, 'b', 2)` (`STRUCT<a: INT, b: INT>`) | `named_struct('b', 3, 'a', 4)` | `named_struct('b', 2, 'a', 1)` |
| `reordered-typed` | `named_struct('a', 1, 'b', 'x')` (`STRUCT<a: INT, b: STRING>`) | `named_struct('b', 'y', 'a', 2)` | `named_struct('b', 'x', 'a', 1)` |
| `disjoint-names` | `named_struct('a', 1, 'b', 2)` (`STRUCT<a: INT, b: INT>`) | `named_struct('c', 3, 'd', 4)` | `named_struct('c', 1, 'd', 2)` |
| `partial-overlap` | `named_struct('a', 1, 'b', 2)` (`STRUCT<a: INT, b: INT>`) | `named_struct('a', 3, 'c', 4)` | `named_struct('a', 1, 'c', 2)` |
| `case-reordered` | `named_struct('a', 1, 'b', 2)` (`STRUCT<a: INT, b: INT>`) | `named_struct('B', 3, 'A', 4)` | `named_struct('B', 2, 'A', 1)` |
| `nested-reordered` | `named_struct('x', named_struct('a', 1, 'b', 2))` (`STRUCT<x: STRUCT<a: INT, b: INT>>`) | `named_struct('x', named_struct('b', 3, 'a', 4))` | `named_struct('x', named_struct('b', 2, 'a', 1))` |
| `array-reordered` | `array(named_struct('a', 1, 'b', 2))` (`ARRAY<STRUCT<a: INT, b: INT>>`) | `array(named_struct('b', 3, 'a', 4))` | `array(named_struct('b', 2, 'a', 1))` |
| `map-value-reordered` | `map('k', named_struct('a', 1, 'b', 2))` (`MAP<STRING, STRUCT<a: INT, b: INT>>`) | `map('k', named_struct('b', 3, 'a', 4))` | `map('k', named_struct('b', 2, 'a', 1))` |
| `three-way` | `named_struct('a', 1, 'b', 2, 'c', 3)` (`STRUCT<a: INT, b: INT, c: INT>`) | `named_struct('c', 4, 'a', 5, 'b', 6)` | `named_struct('c', 3, 'a', 1, 'b', 2)` |

Native spellings that differ: `array-reordered` uses `ARRAY[named_struct(...)]` and `map-value-reordered`
uses `MAP(ARRAY['k'], ARRAY[named_struct(...)])`; the other shapes are spelled as on the Spark door.

## Readings and the set-right operands

- `by-position`: the i-th right field is paired with the i-th left field, at every struct level.
- `by-name`: fields are paired by name, ignoring case as Spark does by default; a left field with no
  right field of that name is NULL; when no name of a struct matches, the reading is a refusal.
- The set-right operands make the two readings differ for INTERSECT and EXCEPT wherever the shape
  allows it: the right row matches the left row under one reading and not under the other. For
  `same-order` no reading can differ, so the set-right operand matches the left row (a control that
  returns one row under both readings).
- The predicted columns in the domain table are the two readings' rows, in the left operand's field
  order. The pairing columns are derived by comparing each measured result (or persisted row set)
  with the two predicted rows, with scalars compared as text so that a widened `INT` to `STRING` union
  still matches the value it came from.

## Deviations from the brief

1. The brief's label list has no `SETUP-REFUSED`. The four native `map-value-reordered` INSERT cells
   refuse at CREATE (`ParseException` on `MAP<STRING, STRUCT<...>>`, the native door's DDL cannot spell
   a map type). Their raw outcome is `SETUP-REFUSED` with stage `create` and the message is shown in the
   measured column. Their label is `ORACLE-MISSING` because the native door has no Spark cell, as the
   brief specifies. No label was added.
2. The DataFrame routes build their operands with `SELECT <x> AS s` through the same session's SQL,
   then call `DataFrame.union` or `DataFrame.unionByName`, and for the INSERT form
   `write.insertInto`. The native door has no DataFrame route, so it runs only the four SQL operations.
3. The `case-reordered` shape ran under the default case setting only (Spark `caseSensitive` false). Its
   by-name reading ignores case, so it predicts an answer; RePark refuses that shape, so RePark is
   case-sensitive here, while Spark's `unionByName` pairs the fields case-insensitively.
4. The RePark native session and the facade were both built with `repark.sql.allowCreateFormatVersion3`
   set to true, as in the step-0.3 probe. No other RePark setting was changed, and the header does not
   record a RePark ANSI or case setting.
5. The INSERT target is a fresh v2 copy-on-write table with no seed rows; the table is created with the
   same DDL on each door (`USING iceberg` and `TBLPROPERTIES` on the Spark and facade doors; plain
   `CREATE TABLE` on the native door, as the native door cannot take either).

## Unit C control rows, explicit UNION DISTINCT and the comparison cells

Base: `origin/main` at `03f925c51` when the lane was cut. The two legs below reuse the artifacts of the
section above: the same Spark 4.1.2 with jar `d6ea6c5d…`, and the same release wheel `07f94d4d…`
(`fdc61f26`). The shared helpers are imported by path from `probe_setop.py`; the seed rows and the
four-route struct spelling come from `task/wo/empty-dml-shared-guard-1/evidence/probe.py`.

Commands run (placeholders for paths as above; `<scratch>` is the measurement scratch directory):

```
JAVA_HOME=<jdk-17> <oracle-python> -I probe_setop_controls.py --engine spark --label spark \
    --warehouse <scratch>/spark-warehouse --out raw/controls-spark.jsonl --ivy-jar <ivy-jar>
<venv-main-fdc61f26>/bin/python -I probe_setop_controls.py --engine repark \
    --label repark-main-fdc61f26 --warehouse <scratch>/repark-warehouse \
    --out raw/controls-repark-main-fdc61f26.jsonl --wheel <main-wheel>
<oracle-python> -I probe_setop_controls.py --render
uvx ruff@0.15.22 format probe_setop_controls.py && uvx ruff@0.15.22 check probe_setop_controls.py
```

Each leg runs against an empty warehouse, one Spark JVM at a time. Each raw file has 253 lines: one
leg header and 252 cells, which are 200 identical-struct controls, 40 explicit `UNION DISTINCT` cells and
12 comparison cells (3 predicates on 4 routes). `controls-table.md` is rendered from the two raw files,
and the existing UNION cells are read from the two precedent raw files.

Cell outcomes per leg:

- Spark: 134 answered, 18 refused, 100 `ORACLE-MISSING` (the native door: 80 controls and 20
  `UNION DISTINCT` cells), 0 panics.
- RePark (release wheel): 227 answered, 20 refused, 5 `SETUP-REFUSED` (the native `map-value-reordered`
  inserts: 4 controls and 1 `UNION DISTINCT` cell, the native CREATE of a map type), 0 panics.

Decisions and deviations from the brief:

1. The identical-struct control right operand is the left operand's structure (same field names, order
   and types) with different values: each integer plus two, each string `y`. The same right operand is
   used for `INTERSECT` and `EXCEPT`, not the set-right variant of the existing table, because a control
   must hold the structure fixed and the values distinct. Both readings predict the same rows, so the
   pairing columns read `n/a`.
2. Explicit `UNION DISTINCT` runs on the sql and native doors, read and insert, for the ten existing
   shapes with their existing right operands. The `UNION` column beside it is the existing table's cell,
   read from the precedent raw files and not re-run. The column "RePark UNION DISTINCT same as RePark
   UNION" compares the outcome and rows (read: schema too; insert: snapshot change too).
3. The three audit comparison cells are the WHERE predicate of `INSERT INTO t SELECT * FROM t WHERE
   <predicate>`, the brief's statement for the first. The other two are treated the same way. The table
   is `id BIGINT, v STRING`, seeded with `(1, 'a'), (2, 'b')` (the empty-dml probe's seed and columns).
   The routes are `sql` (the SQL statement), `filter-str` (`filter("<predicate>")`), `f-expr`
   (`filter(F.expr("<predicate>"))`) and `typed` (`F.struct` with aliased fields, the typed spelling of
   `named_struct`). Both engines run the same routes; the RePark native door is not used here.
4. The native-door controls and `UNION DISTINCT` cells have no Spark reference and read `ORACLE-MISSING`;
   they are measured on RePark only.
5. The release wheel is measured, so a debug assertion is unmeasured. No cell on either leg panicked.

Results, as labels for the RePark cell against the Spark cell:

- Controls: 114 `EQUAL`, 6 `BOTH-REFUSED`, 80 `ORACLE-MISSING`. The six `BOTH-REFUSED` cells are the
  sql-door `ctl-map-value-reordered` cells for `UNION`, `INTERSECT` and `EXCEPT` (read and insert). Spark
  refuses every set operation on a `MAP` column (`SET_OPERATION_ON_MAP_TYPE`), and RePark refuses them
  too. No control on the df door or the sql door for the other nine shapes diverges.
- `UNION DISTINCT`: 2 `EQUAL` (`same-order`, sql door), 18 non-EQUAL (sql door), 20 `ORACLE-MISSING`
  (native door). Each of the 40 cells carries the same label set as the existing `UNION` cell for its
  shape, door and form (checked against the precedent raw files), and each RePark `UNION DISTINCT` cell
  matches its RePark `UNION` twin on outcome and rows.
- Comparison cells: `c3` (duplicate names) is `EQUAL` on all four routes, both engines persisting the
  seed rows. `c1` and `c2` are `ACCEPTED-WHERE-SPARK-REFUSED` on all four routes: Spark refuses with
  `DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES` because the struct field names differ in order or set, while
  RePark accepts and inserts a second copy of the `(1, 'a')` row in both `c1` and `c2`.

Up: [../map.md](../map.md).
