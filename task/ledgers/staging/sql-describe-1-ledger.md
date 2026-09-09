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

## Step 2 (I tier, 2026-09-09) — D-3 move, parser, executor

**Model:** muse-spark-1.3-contributor. R-9 (owner ruling 2026-09-09) resolved the parked
question: the DDL spelling moves into `repark-spark` as `spark_ddl_type_name`, `repark-python`
calls it, `long` stays for `printSchema`.

Red first: the step-2 pins in `crates/repark-spark/src/tests/describe_table.rs` were written
before any implementation and run against the base tree. Red output (verbatim):

```
error[E0425]: cannot find function `try_parse_describe_table` in module `crate::describe_show`
   --> crates/repark-spark/src/tests/describe_table.rs:137:35
    |
137 |             crate::describe_show::try_parse_describe_table(sql).is_none(),
    |                                   ^^^^^^^^^^^^^^^^^^^^^^^^
```

Two commits: D-3 (`spark_type_names.rs`, the Python call site, `make verify` green) then the
parser plus executor (`describe_show.rs`, the router arm, `tests/describe_table.rs`).

## Step-2 clause table (Rust pins; the Python pins land in step 3)

| ID | Clause | Rust pin | Verdict |
|---|---|---|---|
| S2-001 | D-1 intercept scope: plain `DESCRIBE t` on an Iceberg table takes the new path; temp views, DataFusion-native tables, metadata suffixes, and unregistered catalogs fall through unchanged. | `describe_table_plain_matches_step_one_capture_rows`, `describe_table_temp_view_falls_through_to_datafusion`, `describe_table_unregistered_catalog_falls_through_unchanged`, `describe_table_parser_leaves_non_table_forms_alone`, plus unchanged Z6 pin `describe_table_is_not_shadowed_by_the_namespace_intercept` | PROVEN |
| S2-002 | D-2 output shape: three `string` columns, `comment` nullable, the other two not; rows in Spark's order against the step-1 capture. | `describe_table_plain_matches_step_one_capture_rows` (verbatim 6 rows), `describe_table_extended_emits_metadata_and_detail_sections` (22 rows) | PROVEN |
| S2-003 | D-3 type spelling: `bigint` for `Long`, `timestamp` for Iceberg `timestamptz`, via the shared `spark_ddl_type_name`. | `spark_ddl_type_name_spells_describe_primitives`, `spark_ddl_type_name_spells_nested_types`, plus the unchanged Python `arrow_type_key_*` twins proving the move kept `printSchema` output identical | PROVEN |
| S2-004 | D-4 missing table raises `AnalysisException` with `[TABLE_OR_VIEW_NOT_FOUND]`. | `describe_table_missing_table_raises_table_or_view_not_found` (Plan variant, Spark condition, qualified name) | PROVEN |
| S2-005 | D-5 `Table Properties` renders the bracketed key-ordered list with namespace-path redaction. | `describe_table_properties_redact_secrets`, `describe_table_extended_emits_metadata_and_detail_sections` (`k=v`, `current-snapshot-id=none`) | PROVEN |
| S2-006 | D-6 `FORMATTED` is accepted as a synonym of `EXTENDED`. | `describe_table_formatted_is_byte_identical_to_extended`, `describe_table_parser_accepts_plain_and_extended_forms` | PROVEN |

Step-1 clauses C-001…C-007 stay OPEN until step 3's facade pins are green.

Measured deltas carried forward for step 3: `Owner` reads the OS user and `Location` the
warehouse path (Rust pins assert presence, never literal values); `Table Properties` on an
engine-created table carries the engine's stored properties plus a live
`current-snapshot-id` (`none` when empty) — the live leg must compare against what this
engine stores, not Spark's defaults, byte for byte; `Statistics` on a table with data reads
`total-records` / `total-files-size` from the current snapshot summary, which no JVM-free
Rust pin can check against Spark (empty-table `0 bytes, 0 rows` is pinned). Partition
spellings beyond `days(ts)` (`years`/`months`/`hours`/`identity`/`bucket(n, col)`/
`truncate(w, col)`) follow Spark's SQL function grammar but only `days(ts)` is measured;
unpartitioned tables omit the `# Partitioning` section (unmeasured, Spark-standard).
