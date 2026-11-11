# G6 design: the flat live matrix (scalar subset, format v2, Glue and S3 Tables)

Clerk design, 2026-11-11, Claude Haiku 5.5. Nothing here is built or run. No AWS call, no build, no repository file changed.

Tags: **READ** (a file, line or runbook section, at `origin/main` f341171a in the read-only clone), **MEASURED** (a command, shown), **INFERRED** (a reasoning step, to be checked by the pilot).

Not ruled and not decided here: the label wording, the float-key scope, C-090, the audit shape, the unit's name. The module and job names below are proposals.

## 0. Summary

| Item | Value | Tag |
|---|---|---|
| Predicates in scope | 28 (p01 to p26, p35, p36) | READ, probe_envelope.py:88-127 |
| Door and seat combinations per catalog | 30 | section 2 |
| Cells per catalog | 30 x 28 = 840 | section 2 |
| Cells, both catalogs | 1,680 | arithmetic |
| Class A (committed Spark value exists) | 15 combinations, 420 cells per catalog | section 4 |
| Class B (live Spark oracle needed) | 15 combinations, 420 cells per catalog, weight 2 | section 4 |
| Minutes per catalog | 21 x c, where c is seconds per cell | section 3 |
| Minutes per catalog at c = 5.7 / 10.6 / 17.5 s | 120 / 223 / 368 | section 3 |
| Jobs | 8 per catalog, 16 in total, each under its timeout with a 30 % margin | section 3 |
| Stops for the owner | S-1 (S3 Tables object listing), S-2 (workflow merge), S-3 (any AccessDenied) | section 6 |

## 1. Axes

### 1.1 Predicates (the 28 in scope)

Taken from `task/wo/write-path-envelope-matrix-1/evidence/probe_envelope.py` at lines 88-127 (READ). The ids and texts are kept exactly, so the label refers to the same cells as the committed matrix.

| id | predicate | id | predicate |
|---|---|---|---|
| p01 | `i = 1` | p14 | `isnan(d)` |
| p02 | `i <> 1` | p15 | `d = 0.0` |
| p03 | `i IN (1, NULL)` | p16 | `d > 0` |
| p04 | `i NOT IN (1, NULL)` | p17 | `s = ''` |
| p05 | `i IS NULL` | p18 | `s = 'a '` |
| p06 | `i BETWEEN 1 AND 2` | p19 | `s LIKE 'a%'` |
| p07 | `i <=> NULL` | p20 | `upper(s) = 'A'` |
| p08 | `i + 1 > 0` | p21 | `length(s) = 0` |
| p09 | `CAST(i AS STRING) = '1'` | p22 | `b` |
| p10 | `l = 9223372036854775807` | p23 | `b IS NOT TRUE` |
| p11 | `dec = 1.10` | p24 | `dt = DATE '1970-01-01'` |
| p12 | `dec > 1.1` | p25 | `ts > TIMESTAMP '2026-01-01 00:00:00'` |
| p13 | `d = CAST('NaN' AS DOUBLE)` | p26 | `ntz = TIMESTAMP_NTZ '2026-03-08 02:30:00'` |
| p35 | `coalesce(i, 0) = 0` | p36 | `CASE WHEN i = 1 THEN true END` |

Out of scope: p27 to p34 (`st.x`, `st IS NULL`, `st.y IS NULL`, `st = named_struct(...)`, `array_contains`, `size`, `element_at`, `m['k']`). They touch STRUCT, ARRAY or MAP columns, which the label excludes (Fable brief section 2.2, READ).

The scalar seed is the committed seed (probe_envelope.py:SPARK_SEED, READ) with the `st`, `arr` and `m` columns removed. Eight rows, including the NULL, NaN, `-0.0`, `Infinity` and `0.0` rows the predicates need. Schema:
`id BIGINT NOT NULL, i INT, l BIGINT, dec DECIMAL(10,2), d DOUBLE, s STRING, b BOOLEAN, dt DATE, ts TIMESTAMP, ntz TIMESTAMP_NTZ, flag STRING`.
Its types are exactly the label's list (Fable brief section 2.1), so no other type is covered.

### 1.2 Doors and seats: committed or missing

Committed-matrix facts are READ from probe_envelope.py `build_plan` (lines 321-345) and `seat_text` / `df_text` (lines 414-439), and from the raw files (MEASURED by counting records per door, seat and layout, from `git show origin/main:.../raw/*.jsonl`).

| Door | Seat | In committed matrix? | Design |
|---|---|---|---|
| SQL text (`spark.sql`) | SELECT WHERE | yes, v2-cow | in |
| SQL text | DELETE WHERE | yes, v2-cow, v2-mor, v3 | in (v2 only) |
| SQL text | UPDATE WHERE (`SET flag = 'z'`) | yes, cow, mor, v3 | in |
| SQL text | UPDATE SET (predicate inside the SET value) | **missing** | **add**: `UPDATE t SET flag = CASE WHEN <p> THEN 'z' ELSE flag END` |
| SQL text | INSERT ... SELECT WHERE | yes | in |
| SQL text | CTAS ... SELECT WHERE | yes | in (see note 1.3) |
| SQL text | MERGE WHEN MATCHED AND `<p>` | yes | in |
| SQL text | MERGE ON `<p>` | **missing** (the committed ON is `t.id = s.id`) | **add**: `ON t.id = s.id AND <p>` |
| `filter(str)` | SELECT | yes, cow | in |
| `filter(str)` | INSERT via `write.insertInto` | yes, cow | in |
| `F.expr` | SELECT | yes, cow | in |
| `F.expr` | INSERT via `write.insertInto` | yes, cow | in |
| `selectExpr` | SELECT (predicate as a projected boolean, then `where`) | **missing** (facade: `dataframe/core.py:1922`, READ) | **add** |
| Typed Column API | SELECT via `filter(Column)` | **missing** | **add** |
| Typed Column API | INSERT via `write.insertInto` | **missing** | **add** |
| DataFrame writer V2 | `writeTo(t).append()` | **missing** (`writer_readwriter.py:852`, READ) | **add** (Column predicate) |
| DataFrame writer V2 | `writeTo(t).overwrite(cond)` (predicate delete plus append) | **missing** (`writer_readwriter.py:868`, READ) | **add**, cow and mor |
| DataFrame writer V2 | `writeTo(t).createOrReplace()` (CTAS equivalent) | **missing** (`writer_readwriter.py:814`, READ) | **add**, one combination |
| DataFrame merge | `df.mergeInto(t, <cond>)`, `whenMatched(<p>)` | **missing** (`merge.py:88`, `core.py:2930`, READ) | **add**: ON and WHEN, cow and mor |
| DataFrame | DELETE, UPDATE | no API in the facade (MEASURED: no `def delete` or `def update` outside merge clauses, grep) | **struck** |
| DataFrame | `overwritePartitions`, full-table `mode('overwrite')` | missing | **not covered**: no predicate seat |
| DataFrame | `saveAsTable` | not checked | **not covered** (not checked) |
| Native door (`repark.sql()`) | all seats | yes, 1.5.5 | **excluded** by the label (Fable 2.2) |

Layouts: the committed matrix runs DML seats on v2-cow, v2-mor and v3, but SELECT and the DataFrame seats on v2-cow only (MEASURED, raw record counts). The design keeps v2-cow and v2-mor. v3 is excluded by the label. SELECT and DataFrame seats stay v2-cow only, the committed convention.

Catalogs: Glue and S3 Tables are **missing** from the committed matrix. Every committed leg runs on a local catalog (`probe_ice` for Spark, `ice` for RePark; READ, probe `Services` note in `findings.md`/`provenance.md`).

### 1.3 Note on CTAS layouts

READ, probe_envelope.py:416-427: the CTAS target `t3` is created without TBLPROPERTIES. The layout axis on CTAS therefore varies the **source** table. The target uses default properties. The design keeps both source layouts (the committed convention). The MOR CTAS cells add no new target layout, and they are the first thing to strike if the budget is cut (28 cells per catalog).

## 2. Cell list and arithmetic

Each combination runs its 28 predicates, one cell per predicate.

| No. | Door | Seat | Layout | Class | Cells per catalog |
|---|---|---|---|---|---|
| 1 | SQL | SELECT WHERE | cow | A | 28 |
| 2 | SQL | DELETE WHERE | cow | A | 28 |
| 3 | SQL | DELETE WHERE | mor | A | 28 |
| 4 | SQL | UPDATE WHERE | cow | A | 28 |
| 5 | SQL | UPDATE WHERE | mor | A | 28 |
| 6 | SQL | UPDATE SET predicate | cow | B | 28 |
| 7 | SQL | UPDATE SET predicate | mor | B | 28 |
| 8 | SQL | INSERT SELECT WHERE | cow | A | 28 |
| 9 | SQL | INSERT SELECT WHERE | mor | A | 28 |
| 10 | SQL | CTAS SELECT WHERE (source cow) | cow | A | 28 |
| 11 | SQL | CTAS SELECT WHERE (source mor) | mor | A | 28 |
| 12 | SQL | MERGE WHEN MATCHED AND | cow | A | 28 |
| 13 | SQL | MERGE WHEN MATCHED AND | mor | A | 28 |
| 14 | SQL | MERGE ON | cow | B | 28 |
| 15 | SQL | MERGE ON | mor | B | 28 |
| 16 | `filter(str)` | SELECT | cow | A | 28 |
| 17 | `F.expr` | SELECT | cow | A | 28 |
| 18 | `selectExpr` | SELECT | cow | B | 28 |
| 19 | Column API | SELECT | cow | B | 28 |
| 20 | `filter(str)` | INSERT via insertInto | cow | A | 28 |
| 21 | `F.expr` | INSERT via insertInto | cow | A | 28 |
| 22 | Column API | INSERT via insertInto | cow | B | 28 |
| 23 | Column API | `writeTo.append()` | cow | B | 28 |
| 24 | `F.expr` | `writeTo.overwrite(cond)` | cow | B | 28 |
| 25 | `F.expr` | `writeTo.overwrite(cond)` | mor | B | 28 |
| 26 | `F.expr` | `mergeInto` ON | cow | B | 28 |
| 27 | `F.expr` | `mergeInto` ON | mor | B | 28 |
| 28 | `F.expr` | `mergeInto` whenMatched | cow | B | 28 |
| 29 | `F.expr` | `mergeInto` whenMatched | mor | B | 28 |
| 30 | Column API | `writeTo.createOrReplace()` | cow | B | 28 |

Arithmetic, per catalog:
- Class A: combinations 1-5, 8-13, 16, 17, 20, 21 = 15 combinations; 15 x 28 = **420 cells**.
- Class B: combinations 6, 7, 14, 15, 18, 19, 22-30 = 15 combinations; 15 x 28 = **420 cells**.
- Total: 30 x 28 = **840 cells per catalog**; 840 x 2 = **1,680 cells** for both catalogs.

Struck, with the reason:
- SET seat in DELETE, and any SET or ON seat in SELECT or INSERT: the statement has no such clause.
- DataFrame DELETE and DataFrame UPDATE: the facade has no such method (MEASURED, grep). The only delete and update in the facade are clauses inside `mergeInto`, which the design covers.
- SELECT on mor: no DML runs before the read. The mor read after a DML is the final-state readback of the DML seats.
- Two predicates in one cell (for example a WHERE plus a SET predicate): not a seat, and it doubles the search with no new seat.
- Predicates p27 to p34: excluded by the label (section 1.1).
- The native door, v3 and nanosecond types: excluded by the label; they add no cells here.

## 2a. Float-key cells

Added 2026-11-11 after the owner's "Go with the defaults" ruling, which puts float grouping and join keys in scope (READ, the Fable brief's section 4.1 and the float-key evaluation `3-signed-zero-pr-b.md` section 4a). These cells are outside the 30 x 28 predicate matrix: they test the key position, not a filter predicate.

Shapes, each run once for DOUBLE and once for FLOAT:

| No. | Shape | Position |
|---|---|---|
| K1 | `SELECT d, count(*) FROM t GROUP BY d` | GROUP BY |
| K2 | `SELECT DISTINCT d FROM t` | DISTINCT |
| K3 | `SELECT count(DISTINCT d) FROM t` | COUNT(DISTINCT) |
| K4 | `SELECT t.id FROM t JOIN u ON t.d = u.d` | inner join |
| K5 | `SELECT t.id FROM t LEFT SEMI JOIN u ON t.d = u.d` | left semi |
| K6 | `SELECT t.id FROM t LEFT ANTI JOIN u ON t.d = u.d` | left anti |
| K7 | `SELECT t.id, u.id FROM t FULL OUTER JOIN u ON t.d = u.d` | full outer |
| K8 | `SELECT id, rank() OVER (PARTITION BY d ORDER BY id) FROM t` | window PARTITION BY |
| K9 | `SELECT d FROM t INTERSECT SELECT d FROM u` | INTERSECT |
| K10 | `SELECT d FROM t EXCEPT SELECT d FROM u` | EXCEPT |
| K11 | `SELECT d FROM t UNION SELECT d FROM u` | UNION DISTINCT |
| K12 | `SELECT id FROM t WHERE d IN (SELECT d FROM u)` | IN-subquery |
| W1 | `MERGE INTO t USING u AS s ON t.d = s.d WHEN MATCHED THEN UPDATE SET flag = 'z'` | MERGE ON |
| W2 | `DELETE FROM t WHERE d IN (SELECT d FROM u)` | DELETE WHERE IN |

The count: 12 query shapes and 2 write shapes, times 2 float types, is **28 cells per catalog and 56 for both catalogs**.

Data: `t` holds both zeros and one non-zero value. `u` holds only the sign that the join or set operation must match against: `-0.0` alone for the one-sign cells, so that a miss on `+0.0` shows. The cells that need both signs on the build side use `u` holding `0.0` and `-0.0`.

Truth: Spark SQL semantics, where `0.0 = -0.0` and both zeros group, join and distinct together. The value comes from a live Spark query over the same table on the same catalog (Class B, section 4.2). The Spark-on-Iceberg pruning caveat (`docs/` note on zero-only files) applies only to filters on zero-only files, not to keys, so it does not change these expected values.

Cost: Class B weight 2c, as in section 3.2 (INFERRED; the pilot measures it). Per catalog, 56c cell-seconds, so 56c / 60 minutes:

| c | Minutes per catalog | Both catalogs, serial |
|---|---|---|
| 5.7 s | 5.3 | 10.6 |
| 10.6 s | 9.9 | 19.8 |
| 17.5 s (planning) | 16.3 | 32.7 |

The float-key group is one selector value, `floatkeys` (`REPARK_FLAT_GROUP`). Its cells are expected open until SIGNED-ZERO-NORMALIZE-1 PR B; the known-open dict in the module names that unit.

## 3. Run time

### 3.1 Measured per-test time of the existing legs

All numbers from `gh run view <id> --json jobs` and `gh run view --job <id> --log` (MEASURED), grepping the pytest summary line.

| Run (date, schedule) | Leg | Tests | pytest time | Seconds per test | Job wall |
|---|---|---|---|---|---|
| 42989488172 (11-11) | U commit-unknown | 96 | 1,019.7 s | 10.6 | 21.1 min |
| 42785056326 (11-10) | U commit-unknown | 96 | 1,680.4 s | 17.5 | 34.0 min |
| 42588225037 (11-09) | U commit-unknown | 96 | 1,456.9 s | 15.2 | 29.6 min |
| 42785056326 (11-10) | X cross-release | 40 | 227.1 s | 5.7 | 6.1 min |
| 42588225037 (11-09) | X cross-release | 40 | 382.4 s | 9.6 | 9.8 min |
| 42785056326 (11-10) | acceptance (live) | 11 | 360.1 s | 32.7 | 9.6 min |
| 42588225037 (11-09) | acceptance (live) | 11 | 288.6 s | 26.2 | 7.8 min |

Job overhead outside pytest (setup, wheel install, cleanup): about 1 to 1.5 min, from the U and live jobs of 11-11 and 11-10 (MEASURED).

Correction to the brief: section 4.3 says leg U "runs 96 tests in about 49 minutes". The measured pytest time was 17 to 28 min across the three nights, and the longest job wall was 34 min. The 49-minute figure is not reproduced, so this design uses the measurements.

### 3.2 Planning figure

Leg U has 96 cells of the same shape as a G6 cell (2 catalogs x 2 formats x 3 doors x 8 cells, READ `test_aws_acceptance_commit_unknown.py`). Its per-test time is the highest measured, so it sets the upper planning figure. Leg X sets the lower figure.

- c = 5.7 s (lowest measured per test), c = 10.6 s (11-11), c = 17.5 s (highest measured).
- Class A cell costs c (one RePark statement, one Spark read of the final table).
- Class B cell costs 2c (one RePark statement, plus one Spark statement on a second table, plus reads). This weight is **INFERRED**. The pilot in section 3.4 measures it.

Units: 15 x 28 x 1 + 15 x 28 x 2 = 1,260 cell-seconds per catalog. Minutes per catalog = 1,260 x c / 60 = **21 x c**:

| c | Minutes per catalog | Both catalogs, serial |
|---|---|---|
| 5.7 s | 120 | 240 |
| 10.6 s | 223 | 446 |
| 17.5 s (planning) | 368 | 735 |

Both catalogs run in parallel jobs, so the wall time is set by the longest job, not by the sum.

### 3.3 Job split (per catalog; the same eight jobs run on Glue and on S3 Tables)

Expected minutes use c = 17.5 s, the planning figure. Timeout = (expected + 2 min setup) x 1.3, rounded up, so each job keeps a 30 % margin.

| Job | Combinations | Cells | Weight (units) | Expected min at 17.5 s | Timeout (min) |
|---|---|---|---|---|---|
| J1 SQL DML: DELETE, UPDATE WHERE, UPDATE SET | 2-7 | 168 | 224 | 65.3 | 88 |
| J2 SQL MERGE: WHEN, ON | 12-15 | 112 | 168 | 49.0 | 67 |
| J3a SQL SELECT, INSERT, CTAS | 1, 8-11 | 140 | 140 | 40.8 | 56 |
| J3b DataFrame `writeTo.overwrite` | 24, 25 | 56 | 112 | 32.7 | 46 |
| J4a DataFrame SELECT and insertInto, committed doors | 16, 17, 20, 21 | 112 | 112 | 32.7 | 46 |
| J4b DataFrame SELECT and write, new doors | 18, 19, 22, 23 | 112 | 224 | 65.3 | 88 |
| J5a DataFrame `mergeInto` | 26-29 | 112 | 224 | 65.3 | 88 |
| J5b DataFrame `createOrReplace` | 30 | 28 | 56 | 16.3 | 24 |
| **Total per catalog** | 30 | **840** | **1,260** | **367.5** | |

Every timeout is at most 88 min, under the workflow's 90-minute convention (READ: `aws-acceptance.yml` uses `timeout-minutes: 90` for the integrity legs). If the owner rules a higher cap, J1, J4b and J5a can merge into one job.

Sixteen jobs (8 per catalog) run after the single `build` job (21.1 min measured on 11-11). Wall time is roughly 21 + 88 = 109 min, at the planning figure.

### 3.4 Pilot before the full run

Before any timeout is fixed, run one predicate (p01) through all 30 combinations on each catalog: 30 cells per catalog, 60 in total.
- Measure c for Class A combinations and the weight of Class B combinations.
- If the measured c is below 17.5 s, re-split the jobs to the new rate. If it is above, stop and re-plan, because the 90-minute convention would be exceeded.

## 4. Expected values, per cell class

### 4.1 Class A: a committed Spark leg exists for the same statement and seed

Combinations 1-5, 8-13, 16, 17, 20, 21 (15 combinations). The committed legs are READ: `task/wo/write-path-envelope-matrix-1/evidence/raw/spark-sql-*.jsonl` and `spark-df.jsonl` (MEASURED: 109-line and 145-line files, 108 and 144 cells, with the v2 subset used).

Source of the expected value: the committed Spark record for the same door, seat, layout and predicate. The value compared is `ids` (the surviving or selected id set), plus `lost` and `damaged` for DML. This is a value answer, which does not depend on the catalog.

Precondition, checked in each cell before the statement: the live seed's scalar columns equal the committed seed's scalar columns. If they differ, the cell is refused with a setup error, not labelled.

Live check, same cell: Spark reads the RePark final table on the same catalog (one read, a `.snapshots`, `.data_files` and `.delete_files` read, and `DESCRIBE`). The read must return the committed `ids`.

Commit counts for Class A: compared to the committed Spark snapshot delta on **Glue only**, as INFERRED equivalence (one commit per statement). The pilot in section 3.4 measures Spark's live snapshot delta for the p01 cells on Glue to confirm it. On S3 Tables, commit counts are record-only (section 4.3).

### 4.2 Class B: no committed Spark value (new seat or new door)

Combinations 6, 7, 14, 15, 18, 19, 22-30 (15 combinations): UPDATE SET, MERGE ON, `selectExpr`, Column API, `writeTo`, `mergeInto`.

Source of the expected value: a **live Spark oracle** on the same catalog. A second table, seeded with the same rows, runs the same statement through Spark. Its rows, schema, snapshot delta and `ids` are the expected values. This is why Class B costs 2c.

The committed legs do not cover these seats (READ: probe_envelope.py:416-439 has no such texts), so the design does not reuse them.

### 4.3 Values that neither source can supply

1. **Snapshot counts and commit counts on S3 Tables.** The service makes its own maintenance commits. The existing legs relax the count for this reason (READ: `test_aws_acceptance.py:538`, `counts relaxed for service commits`). Record only. A cell on S3 Tables cannot be labelled EXTRA-COMMIT or FEWER-COMMIT.
2. **Data-file and delete-file counts on both catalogs.** The committed Spark records carry only snapshot ids and counts (MEASURED: the key list of `spark-sql-delete.jsonl` has `snapshot_before` and `snapshot_after` and no file counts). File layout depends on the writer, so Spark's count is not an oracle for RePark's. Record only.
3. **Full schema.** Not in the committed records (MEASURED, same key list). The design reads the schema from Spark on the final table (Class A) or the oracle table (Class B), so schema is not in this list. Listed for completeness.

### 4.4 The committed Spark refusal (value from the committed legs)

p08 `i + 1 > 0` is refused by Spark on every committed seat (MEASURED: 20 Spark cells refused, all p08; error text `[ARITHMETIC_OVERFLOW] integer overflow`, READ from the committed record). The seed's row 5 has `i = 2147483647`. The expected Spark value is therefore a **refusal**, not an answer. In this design it is class A where the committed leg exists, and class B otherwise: the Spark oracle must refuse too.

## 5. What each cell records

Per cell, one JSON record (the same shape as the committed legs, plus the fields the committed legs lack, section 4.3):

1. **Rows:** the sorted list of surviving or selected `id` values, and the full row values for the scalar columns.
2. **Full schema:** `DESCRIBE` of the final table, from Spark, as name, type and nullability.
3. **Persisted state:** the snapshot id before and after, and the snapshot count before and after (the committed fields).
4. **Files:** the data-file count and the delete-file count after the statement, from Spark's `.data_files` and `.delete_files` metadata tables. The runbook records these tables as readable on existing S3 Tables tables (READ: `docs/tier2-aws.md` section 2, measured 2026-10-24).
5. **One label:** exactly one of the committed label spellings, in the committed order. Outcomes: `ANSWERED`, `REFUSED`, `PANIC`, `SETUP-REFUSED`, `NOT-EXPRESSIBLE`. Labels: `EQUAL`, `WRONG-READ`, `WRONG-WRITE`, `SEAT-INCONSISTENT`, `LINEAGE-DIFFER`, `EXTRA-COMMIT`, `FEWER-COMMIT`, `ACCEPTED-WHERE-SPARK-REFUSED`, `REFUSED-WHERE-SPARK-ACCEPTED`, `BOTH-REFUSED`, `KNOWN-EMPTY-DML` (a prefix), `ORACLE-MISSING` (READ: probe_envelope.py `spark_labels` and `label_all`, lines 873-963). `NATIVE-REFUSED` does not apply, the native door is excluded.

The label rule is the committed one, unchanged: the design does not add labels.

## 6. Scratch discipline and permissions

### 6.1 Naming and cleanup (as #1180 set them, READ PR #1180)

- Table names: `testing_r<run_id>a<attempt>_<leg>_<cell>`, the pattern the workflow already uses (READ: `aws-acceptance.yml` RUN_PREFIX lines). `<leg>` is a short new name per job (for example `fmj1`). Names are lowercase only, since Glue folds case. The design makes no case claim, so C-090 is not touched.
- Namespace: the existing scratch namespace on each catalog (READ: `testing_repark_acceptance`, runbook section 2).
- Per-job cleanup step: the existing `scripts/aws_acceptance_cleanup.py --run-prefix "${RUN_PREFIX}" --execute` (READ), with the job's own `RUN_PREFIX`.
- Backstop: the existing 24-hour backstop catches any prefix a cancelled job leaves (READ, backstop job).

### 6.2 AWS actions the cells need

Each action is in the runbook's role statements (READ: `docs/tier2-aws.md` section 2, 2026-10-24 revision) and the cleanup script or workflow uses it. "Exercised" means a nightly leg that ran green on 11-09 to 11-11 uses it (MEASURED: `gh run list` conclusions; the code path is READ).

| Catalog | Action | Resource (scratch-scoped) | Used by the cells | Exercised by current legs |
|---|---|---|---|---|
| Glue | `glue:GetDatabase` | scratch database | session setup | yes |
| Glue | `glue:CreateTable`, `glue:UpdateTable`, `glue:GetTable` | scratch database and `testing_*` tables | create, replace, read | yes (U, X, acceptance) |
| Glue | `glue:DeleteTable`, `glue:BatchDeleteTable` | the `testing_*` pattern in its own statement | cleanup | yes (cleanup script) |
| Glue | `glue:GetTables` | catalog-wide, read-only statement | list cells, cross-read discovery | yes (cleanup script, cross-read) |
| Glue | `s3:PutObject`, `s3:GetObject` | warehouse scratch prefix | writes, metadata reads | yes |
| Glue | `s3:ListBucket` | warehouse scratch prefix | leftover objects (Glue only) | yes (cleanup script, `list_objects_v2`) |
| Glue | `s3:DeleteObject` | warehouse scratch prefix (OD-3 exception) | cleanup of scratch files | yes (cleanup script) |
| S3 Tables | `s3tables:GetTableBucket`, `GetNamespace`, `ListNamespaces` | bucket, unconditioned | session setup | yes |
| S3 Tables | `s3tables:CreateTable`, `ListTables` | bucket, namespace condition | create, list | yes (cleanup script `list_s3tables`) |
| S3 Tables | `s3tables:GetTable`, `GetTableMetadataLocation`, `UpdateTableMetadataLocation` | table ARN, namespace condition | commits, reads | yes |
| S3 Tables | `s3tables:GetTableData`, `PutTableData` | table ARN, namespace condition | data reads and writes | yes |
| S3 Tables | `s3tables:DeleteTable` | table ARN, namespace condition | cleanup, CTAS drop-on-abort | yes (cleanup script `delete_s3tables`) |

Not needed by the design, and so not requested: `glue:DeleteDatabase`, `s3tables:DeleteNamespace`, `s3tables:DeleteTableBucket`, `s3tables:GetTableMaintenanceConfiguration` and `PutTableMaintenanceConfiguration`. Any of these would be a request, which is a stop.

Not in the role, and not requested: an object listing under an S3 Tables table location. The runbook names no such action for service-managed table storage. See stop S-1.

### 6.3 Stops for the owner

- **S-1.** Listing objects under an S3 Tables table location needs an action the role does not grant (READ: runbook section 2 names `GetTableData` and `PutTableData`, and no list or get action on the service bucket). The design therefore does not request it. The G5 leftover read on S3 Tables records "not listable with the role", and the owner decides whether to grant a read action. A denied call is a stop, not a design choice.
- **S-2.** The workflow change (the new `leg` choice values, the eight jobs and the `needs: build` edge) is the owner's to merge (Fable brief section 4.3). A `workflow_dispatch` runs the workflow file on the default branch, so the leg cannot be dispatched until that change is on `main` (INFERRED from the GitHub dispatch model; the workflow already refuses non-main refs, READ).
- **S-3.** Any `AccessDenied` in a cell, on any action in table 6.2, is a stop. The design does not widen the role.
- **S-4 (question, not a permission stop).** The label's float-key scope is not ruled. The DOUBLE predicates (p13 to p16) are filter predicates, not keys, so this design does not depend on that ruling. The owner should still confirm it before the label is written.

## 7. Delivery shape

- **Test module (proposed name):** `python/repark/tests/parity/test_aws_acceptance_flat_matrix.py`. It holds the 28-predicate table, the 30-combination table and one parametrised test per (catalog, combination, predicate) cell. It reuses the existing helpers: `run_table_prefix()` from the acceptance modules, and the Spark-session factory pattern of `test_aws_acceptance_cross_read.py` (`_spark_session_for_catalog`, READ).
- **Catalog selection:** the S3 Tables cells skip when `TABLE_BUCKET_ARN` is unset, which is the existing convention (READ). Glue-only jobs leave it unset, so they run only Glue cells.
- **Workflow change (owner's):** one `leg` choice value per job (eight per catalog, sixteen in total), each job `needs: build`, `environment: aws-acceptance`, the leg's `REPARK_ACCEPT_LEG`, the same `RUN_PREFIX` pattern and the per-job cleanup. Dispatch-only, not in the nightly schedule. This is a proposal for the owner to merge.
- **Order of landing:**
  1. A test-only PR to `main` with the G6 module and the G5 module. It touches only `python/repark/tests/parity/`.
  2. The workflow PR, which the owner merges (S-2).
  3. A dispatch from `main` of the pilot leg (section 3.4), then the full legs. Each run id is recorded in the label's evidence block.
- **Record:** the cells write one JSON artifact per job (the workflow already uploads artifacts, READ, the ICE-READ-PERF step), so the evidence block can cite the run id and cell counts.

## 8. Known scalar findings the run must reproduce or show fixed

| Finding | Committed evidence (1.5.5 unless noted) | Expected outcome in this matrix |
|---|---|---|
| p15 `d = 0.0` | 20 cells: WRONG-READ 3, WRONG-WRITE 17 (MEASURED from `findings.md`, READ). The matrix evaluation reports the same on 1.5.6 (READ, not re-run here). The seed row 4 is `-0.0`, row 6 is `0.0`; RePark keeps only row 6 | Before the signed-zero fix: WRONG-READ on SELECT combinations and WRONG-WRITE on DML combinations. After it: EQUAL. **The status of the fix conflicts**: Fable brief 4.1 says G1 is "in flight"; the matrix evaluation says SIGNED-ZERO-NORMALIZE-1 "exists only as a design sketch". The design cannot assume G1 is merged. The candidate wheel must state which it is. |
| p04 `i NOT IN (1, NULL)` | KNOWN-EMPTY-DML with FEWER-COMMIT on 9 UPDATE, CTAS and MERGE cells (READ `findings.md`, ruling D of 2026-11-08) | Still expected. The label must state it as a declared divergence (ruling D). It is not expected to be fixed by this unit. |
| p36 `CASE WHEN i = 1 THEN true END` | 6 REFUSED-WHERE-SPARK-ACCEPTED cells, all p36 (MEASURED from the committed raw files: DELETE cow, mor and v3; UPDATE cow, mor and v3). The v2 subset is **4 cells**. No card found by the matrix evaluation | Expected to refuse again on the v2 DELETE and UPDATE WHERE cells. Each needs a card (clerk). The refusal does not block the label. The UPDATE SET and MERGE ON predicates (new seats) may refuse too; the run records it. |
| p08 `i + 1 > 0` (not in the brief) | Spark refuses on every seat (ARITHMETIC_OVERFLOW on row 5). Committed: BOTH-REFUSED on 20 cells (MEASURED from the raw files) | Expected BOTH-REFUSED on every combination. An ACCEPTED-WHERE-SPARK-REFUSED label would be a finding. |

The committed matrix's scalar subset, as measured from the raw files (MEASURED with one join on door, seat, layout and predicate): 560 combinations of RePark and Spark cells on the SQL and DataFrame doors, 534 both answered, 20 both refused (all p08), 6 REFUSED-WHERE-SPARK-ACCEPTED (all p36), 0 ACCEPTED-WHERE-SPARK-REFUSED. Labels for WRONG-READ and WRONG-WRITE (p15) are judged on the values, not on the outcome, so they are not in this count.

## 9. Claims I could not check

1. The 49-minute leg U figure in the brief (not reproduced; section 3.1).
2. That the Spark oracle weight is 2c for Class B (INFERRED; the pilot measures it).
3. That Spark's snapshot delta on Glue equals the committed local delta (INFERRED; the pilot measures it).
4. The `writeTo.overwrite(cond)`, `writeTo.createOrReplace()` and `mergeInto` paths on the facade on Glue and S3 Tables. Their RePark behaviour is unmeasured. A refusal is a finding, not a block.
5. Whether `DataFrame.saveAsTable` exists on the facade. Not checked; listed as not covered.
6. The `MERGE ON` predicate with a source alias named `s` (the committed WHEN form already uses it and ran; the ON form is unmeasured).
7. Whether the `aws-acceptance` environment needs a per-run approval. Not read.
8. The status of the signed-zero fix (section 8). Conflicting sources.
9. The per-cell time c. Only the existing legs are measured; no G6 cell has run.
