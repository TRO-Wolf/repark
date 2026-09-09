# Unit ledger — SQL-DESCRIBE-1 · `DESCRIBE [TABLE] [EXTENDED|FORMATTED]` on Iceberg tables (step 1: measurement)

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when SQL-DESCRIBE-1 merges, or when the owner closes the slate row.

**Unit:** SQL-DESCRIBE-1 · **Date:** 2026-09-09 · **Executor:** Muse Spark (muse-spark-1.3-contributor), Actor, step 1 ·
**Branch:** `feat/sql-describe-1` · **Base:** `0a94cbc3`
**Model:** muse-spark-1.3-contributor
**Registry:** `docs/spark-sql-iceberg-parity.md` (row filed in step 3, not in this step).
**risk_tier:** standard.

Step 1 is a measurement only. No parser, no executor, no facade test, no file under
`crates/` is touched in this step. Red-first does not apply to a measurement step: there is
no pin yet, so there is no red run to paste; the oracle capture below is the deliverable that
step 2 builds from. Every clause stays OPEN until step 3's pins are green; there is no
`COVERAGE_ATTESTATION` block in this step.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | D-1 intercept scope is measurable: plain `DESCRIBE t` and `DESCRIBE TABLE t` captures exist side by side on the same Iceberg table. | Oracle capture blocks for both spellings. | OPEN |
| C-002 | D-2 output shape is the measured Spark shape: three columns `col_name`, `data_type`, `comment`, rows in Spark's order, captured verbatim with schema. | Oracle capture blocks plus schema lines. | OPEN |
| C-003 | D-3 type spelling is observed, not typed from memory: the capture shows how Spark spells `bigint`, `string`, `timestamp` in `DESCRIBE` rows. | Oracle capture `data_type` column values. | OPEN |
| C-004 | D-4 missing-table behaviour is captured: the exception class and its full message on a missing table. | Oracle capture missing-table block. | OPEN |
| C-005 | D-5 `Table Properties` rendering is observed: the capture shows how `k=v` renders under EXTENDED/FORMATTED. | Oracle capture extended block `Table Properties` row. | OPEN |
| C-006 | D-6 `FORMATTED` versus `EXTENDED` is decided by measurement: byte-identical or exactly how they differ. | Oracle capture comparison lines. | OPEN |
| C-007 | Step-1 deliverable is complete: six captures plus schemas, the D-2/D-6 reading, no engine edits, map lockstep. | This ledger plus `staging/map.md` row. | OPEN |
## Oracle capture (live Spark 4.1.2, 2026-09-09 03:58 UTC)

pyspark 4.1.2. One Spark session for the whole capture.

Warehouse: temporary directory (private, removed after the run). Catalog `local`, namespace `dsns1`, table `t1`.

Table DDL:

```
CREATE TABLE local.dsns1.t1 (id BIGINT COMMENT 'the row identifier', name STRING, ts TIMESTAMP) USING iceberg PARTITIONED BY (days(ts)) TBLPROPERTIES ('k'='v')
```

### plain DESCRIBE

Statement: `DESCRIBE local.dsns1.t1`

Schema:

```
field name='col_name' type=string nullable=False
field name='data_type' type=string nullable=False
field name='comment' type=string nullable=True
```

Rows (6, in order, `None` shown as `None`):

```
tuple('id', 'bigint', 'the row identifier')
tuple('name', 'string', None)
tuple('ts', 'timestamp', None)
tuple('', '', '')
tuple('# Partitioning', '', '')
tuple('Part 0', 'days(ts)', '')
```

### DESCRIBE TABLE

Statement: `DESCRIBE TABLE local.dsns1.t1`

Schema:

```
field name='col_name' type=string nullable=False
field name='data_type' type=string nullable=False
field name='comment' type=string nullable=True
```

Rows (6, in order, `None` shown as `None`):

```
tuple('id', 'bigint', 'the row identifier')
tuple('name', 'string', None)
tuple('ts', 'timestamp', None)
tuple('', '', '')
tuple('# Partitioning', '', '')
tuple('Part 0', 'days(ts)', '')
```

### DESC EXTENDED

Statement: `DESC EXTENDED local.dsns1.t1`

Schema:

```
field name='col_name' type=string nullable=False
field name='data_type' type=string nullable=False
field name='comment' type=string nullable=True
```

Rows (22, in order, `None` shown as `None`):

```
tuple('id', 'bigint', 'the row identifier')
tuple('name', 'string', None)
tuple('ts', 'timestamp', None)
tuple('', '', '')
tuple('# Partitioning', '', '')
tuple('Part 0', 'days(ts)', '')
tuple('', '', '')
tuple('# Metadata Columns', '', '')
tuple('_spec_id', 'int', '')
tuple('_partition', 'struct<ts_day:date>', '')
tuple('_file', 'string', '')
tuple('_pos', 'bigint', '')
tuple('_deleted', 'boolean', '')
tuple('', '', '')
tuple('# Detailed Table Information', '', '')
tuple('Name', 'local.dsns1.t1', '')
tuple('Type', 'MANAGED', '')
tuple('Location', '/tmp/tmp4tkgkqwm/dsns1/t1', '')
tuple('Provider', 'iceberg', '')
tuple('Owner', 'john', '')
tuple('Table Properties', '[current-snapshot-id=none,format=iceberg/parquet,format-version=2,k=v,write.parquet.compression-codec=zstd]', '')
tuple('Statistics', '0 bytes, 0 rows', None)
```

### DESCRIBE TABLE FORMATTED

Statement: `DESCRIBE TABLE FORMATTED local.dsns1.t1`

Schema:

```
field name='col_name' type=string nullable=False
field name='data_type' type=string nullable=False
field name='comment' type=string nullable=True
```

Rows (22, in order, `None` shown as `None`):

```
tuple('id', 'bigint', 'the row identifier')
tuple('name', 'string', None)
tuple('ts', 'timestamp', None)
tuple('', '', '')
tuple('# Partitioning', '', '')
tuple('Part 0', 'days(ts)', '')
tuple('', '', '')
tuple('# Metadata Columns', '', '')
tuple('_spec_id', 'int', '')
tuple('_partition', 'struct<ts_day:date>', '')
tuple('_file', 'string', '')
tuple('_pos', 'bigint', '')
tuple('_deleted', 'boolean', '')
tuple('', '', '')
tuple('# Detailed Table Information', '', '')
tuple('Name', 'local.dsns1.t1', '')
tuple('Type', 'MANAGED', '')
tuple('Location', '/tmp/tmp4tkgkqwm/dsns1/t1', '')
tuple('Provider', 'iceberg', '')
tuple('Owner', 'john', '')
tuple('Table Properties', '[current-snapshot-id=none,format=iceberg/parquet,format-version=2,k=v,write.parquet.compression-codec=zstd]', '')
tuple('Statistics', '0 bytes, 0 rows', None)
```

### DESCRIBE TABLE EXTENDED

Statement: `DESCRIBE TABLE EXTENDED local.dsns1.t1`

Schema:

```
field name='col_name' type=string nullable=False
field name='data_type' type=string nullable=False
field name='comment' type=string nullable=True
```

Rows (22, in order, `None` shown as `None`):

```
tuple('id', 'bigint', 'the row identifier')
tuple('name', 'string', None)
tuple('ts', 'timestamp', None)
tuple('', '', '')
tuple('# Partitioning', '', '')
tuple('Part 0', 'days(ts)', '')
tuple('', '', '')
tuple('# Metadata Columns', '', '')
tuple('_spec_id', 'int', '')
tuple('_partition', 'struct<ts_day:date>', '')
tuple('_file', 'string', '')
tuple('_pos', 'bigint', '')
tuple('_deleted', 'boolean', '')
tuple('', '', '')
tuple('# Detailed Table Information', '', '')
tuple('Name', 'local.dsns1.t1', '')
tuple('Type', 'MANAGED', '')
tuple('Location', '/tmp/tmp4tkgkqwm/dsns1/t1', '')
tuple('Provider', 'iceberg', '')
tuple('Owner', 'john', '')
tuple('Table Properties', '[current-snapshot-id=none,format=iceberg/parquet,format-version=2,k=v,write.parquet.compression-codec=zstd]', '')
tuple('Statistics', '0 bytes, 0 rows', None)
```

### missing table

Statement: `DESCRIBE TABLE local.dsns1.no_such_table`

Exception class: pyspark.errors.exceptions.captured.AnalysisException

Full message:

```
[TABLE_OR_VIEW_NOT_FOUND] The table or view `local`.`dsns1`.`no_such_table` cannot be found. Verify the spelling and correctness of the schema and catalog.
If you did not qualify the name with a schema, verify the current_schema() output, or qualify the name with the correct schema and catalog.
To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS. SQLSTATE: 42P01; line 1 pos 15;
'DescribeRelation false, [col_name#83, data_type#84, comment#85]
+- 'UnresolvedTableOrView [local, dsns1, no_such_table], DESCRIBE TABLE, true

```

Traceback tail:

```
pyspark.errors.exceptions.captured.AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `local`.`dsns1`.`no_such_table` cannot be found. Verify the spelling and correctness of the schema and catalog.
If you did not qualify the name with a schema, verify the current_schema() output, or qualify the name with the correct schema and catalog.
To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS. SQLSTATE: 42P01; line 1 pos 15;
'DescribeRelation false, [col_name#83, data_type#84, comment#85]
+- 'UnresolvedTableOrView [local, dsns1, no_such_table], DESCRIBE TABLE, true


```

Measured comparisons (row lists, in order):

```
plain == DESCRIBE TABLE: True
DESC EXTENDED == DESCRIBE TABLE EXTENDED: True
FORMATTED == DESCRIBE TABLE EXTENDED: True
DESC EXTENDED == FORMATTED: True
```

Capture done; the single Spark session is stopped.

## What the capture says about D-2 and D-6

measured: the output has exactly the three columns `col_name`, `data_type`, `comment`, all
of type `string` — but NOT all nullable. `col_name` and `data_type` report `nullable=False`;
only `comment` reports `nullable=True`. This contradicts the card's D-2 parenthetical
"(all nullable, as Spark reports them)"; step 2 must pin `comment` nullable and the other
two non-nullable.

measured: section headers appear in this order. Plain `DESCRIBE` (and `DESCRIBE TABLE`)
emits the column rows, one blank row, `# Partitioning`, then `Part 0` with `days(ts)`.
EXTENDED/FORMATTED emit the same head, then a blank row and `# Metadata Columns` with five
rows (`_spec_id` int, `_partition` struct<ts_day:date>, `_file` string, `_pos` bigint,
`_deleted` boolean), then a blank row and `# Detailed Table Information` with `Name`,
`Type` (`MANAGED`), `Location`, `Provider` (`iceberg`), `Owner`, `Table Properties` —
followed by one more row the card did not list: `Statistics` with `0 bytes, 0 rows` and a
null comment. Step 2 must decide the `Statistics` row from this capture, not from the card.

measured: `FORMATTED` is byte-identical to `EXTENDED`. All four row-list comparisons are
`True`: plain equals `DESCRIBE TABLE`, `DESC EXTENDED` equals `DESCRIBE TABLE EXTENDED`,
and `FORMATTED` equals both extended spellings. D-6 is confirmed; no hand-back on D-6.

measured: plain `DESCRIBE t` does not differ from `DESCRIBE TABLE t` — identical rows and
identical schemas. D-1 has both spellings captured on the same table.

measured: the missing-table statement raises `AnalysisException`
(`pyspark.errors.exceptions.captured.AnalysisException`) at `session.sql()` time, and its
message carries `[TABLE_OR_VIEW_NOT_FOUND]` with SQLSTATE `42P01`. D-4 is confirmed at the
oracle level.

measured type spellings in `data_type`: `bigint`, `string`, `timestamp` for the declared
columns; `int`, `struct<ts_day:date>`, `boolean` in the metadata columns. The column comment
lands in `comment`; uncommented columns report null, not empty string.

measured: `Table Properties` renders as a bracketed comma list in key order —
`[current-snapshot-id=none,format=iceberg/parquet,format-version=2,k=v,write.parquet.compression-codec=zstd]`.
The engine adds its defaults around the user property. The table held no rows, so the
snapshot id reads `none` and `Statistics` reads `0 bytes, 0 rows`; a table with data would
spell both differently, which step 3's fixture must keep in mind. `Owner` reports the OS
user (`john`) and `Location` the warehouse path — both environment-dependent, never pinned
literally.

No finding in this capture contradicts D-2's section families or D-6; the two deltas above
(non-nullable `col_name`/`data_type`, the extra `Statistics` row) extend the card, they do
not block step 2.


## PARKED at step 2 (orchestrator, 2026-09-09)

Step 2 halted before writing any code, on the exact condition the card's "Hand back when" names:
**D-3 points at an existing shared Iceberg-to-Spark DDL type spelling, and none exists** that
`repark-spark` can use. What the worker found, searching from the home crate:

- `crates/repark-spark/src/create_table.rs` `sql_type_to_iceberg` maps the **forward** direction,
  SQL text to Iceberg types. There is no reverse.
- The fork's `PrimitiveType` `Display` spells **`long`**, not `bigint`.
- The only Spark-DDL-like spellings in Rust are private Arrow-based helpers in `repark-python`,
  a crate `repark-spark` cannot depend on — and their top-level key also spells `Int64` as
  **`long`**, which contradicts the step-1 live capture, where Spark's `DESCRIBE` reports
  **`bigint`**.

The card forbids writing a second spelling table, so the step stopped rather than inventing one.

**The question for the owner.** Where should the single canonical Iceberg-to-Spark DDL spelling
live so `repark-spark` can use it without creating a second table — promoted to a shared helper in
the home crate, or re-homed from `repark-python` to somewhere `repark-spark` may depend on? And,
because the two existing spellings disagree, which spelling is canonical: `DESCRIBE`'s measured
`bigint`, or the `long` that the fork's `Display` and the `repark-python` helper both produce?

The orchestrator did not rule this: a cross-crate decision about where a canonical, Spark-visible
type spelling lives is outside the overnight decision grant (§6 of the runbook parks "any public
name or signature" and anything touching crate structure).

**State on this branch.** Step 1 is complete and independently verified — the orchestrator re-ran
the capture against live Spark 4.1.2 and it matched every row, every schema and every nullability
flag, including the two places where the measurement contradicts the card (`col_name` and
`data_type` are `nullable=False`; there is an extra `Statistics` row). Every clause stays **OPEN**.
Nothing of step 2 was written; the tree is clean at `8da0dbaa`.
