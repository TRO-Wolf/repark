# NESTED-LITERAL-SEAM-1 step 0: sketch (2026-10-27)

Status: sketch for the order section 2 step 0, written on branch `feat/nested-literal-seam-1`
at `e03a714c` by the Muse MAX sketch lane. No product code, no test, no map and no
ledger row changed in this round. The build (step 1) waits for the orchestrator's rulings
on section 6. This file closes when the unit merges; it then moves to `docs/history/`
with the unit's other records.

Method. Oracle: live Spark 4.1.2 (`local[1]`, one JVM per zone run, sequential zones),
Iceberg 1.11.0 Hadoop catalog, banner `version 4.1.2` with session timezone
`America/New_York` then `Asia/Kolkata`, Java 17. Head: `maturin develop --profile ci`
at `e03a714c`, facade sessions, memory catalogs. Warehouses were `mktemp -d` directories,
deleted after each run. Micros below are `unix_micros` reads unless noted. Shorthand:
UTC-mid `1767312000000000`, NY-mid `1767330000000000`, Kol-mid `1767292200000000`.

## 1. The four shapes, reproduced on head and on Spark

CREATE and SEED follow the census corpus (`_write_spine_1_shapes`,
`_write_spine_1_spellings`); table names below are shortened to `t`.

F-2 struct (D8, census cell `merge_update_whole.cow.struct_ltz.date.<zone>`).
`CREATE TABLE t (id INT, v STRUCT<a: TIMESTAMP>, k INT) USING iceberg`;
seed `(1, NULL, 0)`.
WRITE: `MERGE INTO t USING write_spine_1_source s ON t.id = s.id WHEN MATCHED THEN
UPDATE SET t.v = named_struct('a', DATE '2026-01-02')`. READ: `unix_micros(v.a)`.

F-2 array (supporting; same cell with `array_ltz`).
WRITE: `... SET t.v = array(DATE '2026-01-02')`. READ: `unix_micros(v[0])`.

F-3 (D10, census cell `update_whole_where.cow.struct_ltz.date.<zone>`).
WRITE: `UPDATE t SET v = named_struct('a', DATE '2026-01-02') WHERE id >= 0`.

P25 (D10 partitioned, outside the census).
`CREATE TABLE t (id INT, st STRUCT<a: TIMESTAMP>, k INT) USING iceberg PARTITIONED BY (k)`;
seed `(1, NULL, 0)`.
WRITE: `UPDATE t SET st = named_struct('a', DATE '2026-01-02') WHERE k = 0`.
READ: `unix_micros(st.a)`.

F-5 (D3, outside the census). `CREATE TABLE t (id INT, m MAP<TIMESTAMP, STRING>)`.
Literal WRITE: `INSERT INTO t BY NAME SELECT 1 AS id, map(DATE '2026-01-02', 'x') AS m`.
Column WRITE: `INSERT INTO t BY NAME SELECT 1 AS id, map(c_date, 'x') AS m FROM
write_spine_1_source`. READ: `unix_micros(map_keys(m)[0])` (works on both engines).

| shape | New York head | New York Spark | Kolkata head | Kolkata Spark |
|---|---|---|---|---|
| F-2 struct | 1767312000000000 | 1767330000000000 | 1767312000000000 | 1767292200000000 |
| F-2 array | 1767312000000000 | 1767330000000000 | 1767312000000000 | 1767292200000000 |
| F-3 | 1767312000000000 | 1767330000000000 | 1767312000000000 | 1767292200000000 |
| P25 | 1767312000000000 | 1767330000000000 | 1767312000000000 | 1767292200000000 |
| F-5 literal | 1767312000000000 | 1767330000000000 | 1767312000000000 | 1767292200000000 |
| F-5 column | 1767312000000000 | 1767330000000000 | 1767312000000000 | 1767292200000000 |

Every head value is the UTC reading; every Spark value is the session midnight.
The F-5 column form matches the F-5 literal form on both engines in both zones.

## 2. Causes

### F-2: the seamed literal under the row-merge CASE

Reproduced with the column-reference guard removed (one uncommitted edit, reverted):
struct and map STORED the session midnight; array and array_struct FAILED with
`Arrow error: Invalid argument error: column types must match schema types, expected
List(..., field: 'element', metadata: {"PARQUET:field_id": "4"}) but found List(...,
field: 'element')`. The full chain, each link measured:

1. The analyzer's type coercion wraps the THEN branch in an outer CAST whose target
carries the ELSE branch's element metadata:
`datafusion-optimizer-54.1.0/src/analyzer/type_coercion.rs:1026`
(`coerce_case_expression`), common type from
`datafusion-expr-54.1.0/src/type_coercion/other.rs:65` (starts at the ELSE type).
Observed analyzed shape: `CAST(arrow_cast(...) AS List(...element meta field_id=4))`.
2. The stage seams inside `arrow_cast` (`conform_stage.rs:470-511`).
3. The optimizer's `SimplifyExpressions` rule
(`datafusion-optimizer-54.1.0/src/simplify_expressions/simplify_exprs.rs:53`) folds
`array(DATE ...)` / `named_struct(...)` over literals to a Scalar literal
(`ConstEvaluator`, `expr_simplifier.rs:482`, folds only expressions without column
references) and simplifies `arrow_cast` away (`ArrowCastFunc::simplify`,
`datafusion-functions-54.1.0/src/core/arrow_cast.rs:155-176`, returns the seam call
bare when source type equals target type). Optimized THEN:
`CAST(seam(List-scalar) AS List-with-meta)`.
4. The physical output schema carries the metadata: `Expr::Case::get_type`
(`datafusion-expr-54.1.0/src/expr_schema.rs:146-157`) returns the first THEN type,
which is the outer-CAST target WITH element metadata.
5. At execution the THEN branch evaluates to `ColumnarValue::Scalar` (the seam's
Scalar path, `zone_seam.rs:538-544`). A `ScalarValue::List` holds a bare values
array; when the CASE kernel (`arrow-select-58.4.0`, `merge.rs`/`zip.rs`
`MutableArrayData::new(vec![truthy, falsy], ...)` takes the output List field from
the truthy side) materializes the projected list, the element field has no
`PARQUET:field_id`, while the schema promises it. `RecordBatch::try_new` fails
(`arrow-array-58.4.0/src/record_batch.rs:385`). The literal cell never reaches the
sink (proven by a temporary sink probe, reverted).
6. The column variant evaluates the THEN branch to an Array; `cast_list` rebuilds
the list with the target element field WITH metadata
(`arrow-cast-58.4.0/src/cast/list.rs:308-339`) and matches the schema.
7. Struct and map pass because Iceberg scans carry no `PARQUET:field_id` on struct
children (measured: scan child metadata is None), so there is no promise to break;
their seamed values are correct.

The plan step that drops the metadata is the optimizer fold in step 3
(`expr_simplifier.rs:482`, `ConstEvaluator`): it turns the THEN branch from an
array expression into a Scalar, and the Scalar round trip cannot carry the List
element field the schema promises.

Why a plain SELECT with the same CASE keeps working: only absent scan metadata.
A plain SELECT over a metadata-carrying scan with the same CASE fails identically
(proven by a temporary MemTable repro: literal FAILED with the same error, column
passed). The step-2 observation holds for metadata-free scans (memory-catalog
tables, source views) and for struct targets, not for the write path, whose
rewrite scan always carries element field ids.

Fix mechanism for step 1 (F1): when the stage seams a scalar-foldable arg (no
column references) under a CASE outer-CAST, strip nested field metadata from that
outer CAST target, aligning the schema promise with scalar execution. Values are
unchanged; the staging sink re-attaches write-schema ids through
`convert_uuid_column` (`uuid_presentation.rs:111-143`). The column path is untouched.

### F-3: the TRY_CAST fold

The stage runs on the ANALYZED plan, where `TRY_CAST(instant-literal AS TIMESTAMP_NTZ)`
is still unfolded (measured). The fold to the UTC wall (`TimestampMicrosecond(
1767323045123456, None)`, micros preserved, zone-unaware) happens in the optimizer's
`SimplifyExpressions` (same rule as F-2 step 3), AFTER the stage. Unguarded, the stage
seams the struct (partial type NTZ to instant is a zone leaf); the seam then reads the
UTC wall in the session zone: `1767341045123456` against Spark's `1767323045123456`
(a New York round trip is the identity in Spark). The folding plan pass is the
optimizer `SimplifyExpressions` const-fold.

Analyzer pre-stage behavior that shapes the rule (all measured on analyzed plans):
`CAST(instant AS TIMESTAMP_NTZ)` is peeled to the bare instant by
`peel_naive_cast_of_ltz_producer` (`instant_ts.rs:644`); `CAST(DATE AS TIMESTAMP_NTZ)`
is rewritten to the zone-aware `to_timestamp(DATE)`; `TRY_CAST(text AS TIMESTAMP_NTZ)`
is rewritten by `rewrite_string_try_cast` (`spark_string_timestamp.rs:135`).
Only `TRY_CAST` of non-string sources survives to the stage as a `TryCast` node.

Structural note (measured with a temporary stage probe, reverted): F-3 is site B too.
Whole-struct D10 assignments arrive pre-wrapped as
`arrow_cast(store_pair(nonnull(named_struct(...))))` (whole-struct text,
`nested_assign.rs:400-402`); the analyzed source type is already the target, so the
frame form finds no partial and only `seam_nested_arrow_casts` (with its column guard)
can seam it. D10 array/map assignments have no pre-wrap, so the unguarded frame form
already seamed their literals in step 2; that is why only struct cells stayed on D10.
D8 wraps every THEN branch (`merge_sql.rs:262-263`); D7 has no site-B wrap, so its
literals already moved.

## 3. The seam rule (replaces the column-reference guard)

Verdict per nested expression kind, each with the Spark cells that justify it and the
pin that will hold it. ZS-9 stays: rows marked refuse keep their exact refusal.

| # | kind | verdict | reason | pin |
|---|---|---|---|---|
| R1 | column read | SEAM | existing step-2 behavior | step-2 pins hold |
| R2 | date literal | SEAM | F-2/F-3: Spark stores session midnight, head stores UTC (section 1) | NEW Rust pins, D8 CASE + D10 struct, both zones, recorded Spark values |
| R3 | wall literal | REFUSE (ZS-9 stays) | W1/W1m: head refuses with the ZS-9 marker, Spark stores; F4 is step-4 scope | refusal text pins hold |
| R4 | `named_struct`/`map`/`array` constant | SEAM per leaf | F-3/P25: date leaves convert; wall-leaf constants refuse pre-stage via ZS-9 | NEW pins (F-3 struct, P25 partitioned target) |
| R5 | scalar subquery of date type | SEAM | W3: Spark session midnight, head UTC midnight, both zones | NEW pin |
| R6 | scalar subquery of wall type | REFUSE (ZS-9 stays) | W2: head refuses, Spark stores | refusal pin holds |
| R7 | explicit `CAST(instant AS wall)` | DO NOT SEAM (nothing reaches the seam) | analyzer peels to the instant pre-stage (`instant_ts.rs:644`); C1 already EQUAL both zones | C1 pin holds |
| R8 | `TRY_CAST(instant AS wall)` | DO NOT SEAM that leaf | zone-losing fold: the unseamed UTC reading IS Spark's value (T1 EQUAL both zones); seaming double-converts (M6: `1767341045123456`) | existing T1 pin holds |
| R9 | `TRY_CAST(date AS wall)` | SEAM | zone-correct fold (a date is a wall); TD1 silent today: Spark session midnight, head UTC, both zones | NEW pin (TD1 statement) |
| R10 | `TRY_CAST(wall AS wall)` | SEAM | identity fold; TW silent today: head `1767323045123456` (NY); Spark value derived from T2's identical wall text | NEW pin (TW statement; Spark value re-measured at build) |
| R11 | `TRY_CAST(text AS wall)` | N/A (analyzer rewrites) | `rewrite_string_try_cast`; T2 already EQUAL both zones | T2 pin holds |
| R12 | List under CASE, scalar-foldable arg | SEAM + strip outer-CAST metadata (F1) | section 2 F-2: without the strip the scalar path fails `try_new` | NEW pin: D8 array literal stores session midnight, both zones |
| R13 | map key pairs | reach the seam (section 4) | F-5 + K1/K2 | NEW pins (section 4) |

Mechanism. Delete the `column_refs().is_empty()` skip in `seam_nested_arrow_casts`
(`conform_stage.rs:481-486`). Seam every nested `arrow_cast` with a zone partial,
EXCEPT: walk the arg; any nested `TRY_CAST` whose source type is a tz-aware
microsecond instant and whose target is a microsecond wall marks that leaf EXCLUDED
(the partial keeps the source type there; `convert` short-circuits identical types,
`zone_seam.rs:381-383`, returning the UTC reading untouched). Per-leaf exclusion, not
whole-node skip, so a struct holding both a date leaf and a try-cast instant converts
the date and leaves the instant. R3/R6-wall/R6-D8/R8-D8 never reach the stage (ZS-9
fires first: C1m/T1m/TD1m/CD1m refuse on D8, measured); the D8/D10 ZS-9 asymmetry
(D10 lets CAST/TRY_CAST forms through) is recorded as measured and unchanged.

## 4. The map-key arm

Two sites convert map values only and clone keys today:

- `zone_partial` Map arm (`conform_stage.rs:589-606`): `partial_field` on `sources[1]`
only; `sources[0]` cloned.
- The seam UDF: `converted_type` Map arm (`zone_seam.rs:168-185`, value only) and
`convert` Map arm (`zone_seam.rs:432-447`): `convert(map.values(), ...)` while
`map.keys()` is cloned unchanged.

Smallest change. In `zone_partial`, partial the key field too
(`partial_field(&sources[0], &targets[0])`, converted if either side converts). The
key pair then reaches the seam wherever value pairs do, because both the frame form
(`nested_partial`) and the site-B form (`seam_nested_arrow_casts`) flow through
`zone_partial`; no call-site change. In the seam UDF, convert `map.keys()` against
the partial key field when it differs (else clone, mirroring the `convert`
short-circuit), in both `converted_type` and `convert`. The sorted-keys flag stays
`*sorted` as today: every zone conversion (date/wall to instant, instant to wall) is
an order-preserving affine shift, so a sorted key order survives conversion.

Measured ground. `MAP<TIMESTAMP, STRING>`: date keys silent-divergent, literal and
column (section 1, F-5). `MAP<TIMESTAMP_NTZ, STRING>`: date keys already EQUAL
(K1/K2: both engines store wall `2026-01-02 00:00:00`, both zones; date to wall needs
no zone, so the arm must leave them untouched, which falls out of `is_zone_leaf`);
instant keys refuse via ZS-9/R-011 marker 1 while Spark stores the session wall (K5:
NY `2026-01-01 22:04:05.123456`, Kol `2026-01-02 08:34:05.123456`), step-4 scope.
Wall/instant `TIMESTAMP` keys refuse (P21/P22, step-2 record).

Scope note: the order's step 1 files are `conform_stage.rs` plus modules under
`conform_stage/` only, but the seam UDF lives in `repark-functions/src/zone_seam.rs`.
A stage-only key conversion (entries round trip through `map_entries`/
`map_from_entries`) is expressible yet fragile next to column-typed maps. See Q1.

## 5. Expected census moves, new cells, budget

The 40 cells that move DIFFERENT to EQUAL (exact ids from
`write_spine_1_main_7a789fc5.json` minus `write_spine_1_step2_moves.json`,
F5-shaped, verified 32 + 8):

D8 (32): `merge_update_whole{,_literal}.{cow,mor}.{struct_ltz,array_ltz,map_ltz,
array_struct_ltz}.date.{new_york,kolkata}`:

- merge_update_whole.cow.array_ltz.date.kolkata
- merge_update_whole.cow.array_ltz.date.new_york
- merge_update_whole.cow.array_struct_ltz.date.kolkata
- merge_update_whole.cow.array_struct_ltz.date.new_york
- merge_update_whole.cow.map_ltz.date.kolkata
- merge_update_whole.cow.map_ltz.date.new_york
- merge_update_whole.cow.struct_ltz.date.kolkata
- merge_update_whole.cow.struct_ltz.date.new_york
- merge_update_whole.mor.array_ltz.date.kolkata
- merge_update_whole.mor.array_ltz.date.new_york
- merge_update_whole.mor.array_struct_ltz.date.kolkata
- merge_update_whole.mor.array_struct_ltz.date.new_york
- merge_update_whole.mor.map_ltz.date.kolkata
- merge_update_whole.mor.map_ltz.date.new_york
- merge_update_whole.mor.struct_ltz.date.kolkata
- merge_update_whole.mor.struct_ltz.date.new_york
- merge_update_whole_literal.cow.array_ltz.date.kolkata
- merge_update_whole_literal.cow.array_ltz.date.new_york
- merge_update_whole_literal.cow.array_struct_ltz.date.kolkata
- merge_update_whole_literal.cow.array_struct_ltz.date.new_york
- merge_update_whole_literal.cow.map_ltz.date.kolkata
- merge_update_whole_literal.cow.map_ltz.date.new_york
- merge_update_whole_literal.cow.struct_ltz.date.kolkata
- merge_update_whole_literal.cow.struct_ltz.date.new_york
- merge_update_whole_literal.mor.array_ltz.date.kolkata
- merge_update_whole_literal.mor.array_ltz.date.new_york
- merge_update_whole_literal.mor.array_struct_ltz.date.kolkata
- merge_update_whole_literal.mor.array_struct_ltz.date.new_york
- merge_update_whole_literal.mor.map_ltz.date.kolkata
- merge_update_whole_literal.mor.map_ltz.date.new_york
- merge_update_whole_literal.mor.struct_ltz.date.kolkata
- merge_update_whole_literal.mor.struct_ltz.date.new_york

D10 (8): `update_whole_where.{cow,mor}.struct_ltz.{date,col_date}.{new_york,kolkata}`:

- update_whole_where.cow.struct_ltz.col_date.kolkata
- update_whole_where.cow.struct_ltz.col_date.new_york
- update_whole_where.cow.struct_ltz.date.kolkata
- update_whole_where.cow.struct_ltz.date.new_york
- update_whole_where.mor.struct_ltz.col_date.kolkata
- update_whole_where.mor.struct_ltz.col_date.new_york
- update_whole_where.mor.struct_ltz.date.kolkata
- update_whole_where.mor.struct_ltz.date.new_york

New cells. P25: new spelling `update_whole_where_part` (D10, `partitioned=True`,
`UPDATE {table} SET v = {value_bare} WHERE k = 0`, full shape matrix, +480 cells);
expected moves among them: the F5 set on the partitioned target
(`{struct,array,map,array_struct}_ltz` by `date,col_date`, cow/mor, ny/kol: 32 cells)
DIFFERENT to EQUAL. F-5: new key-packed targets `map_ltz_key`/`map_ntz_key` plus new
spelling `insert_map_key` (D3 BY NAME, restricted to the key targets, +48 cells);
expected moves: `map_ltz_key` by `date,col_date` in ny/kol (4 cells) DIFFERENT to
EQUAL; `map_ntz_key` date cells stay EQUAL (K1/K2 guards); wall/instant/str key
cells keep their baseline status (ZS-9 refusals and recorded answers).
F-2/F-3 need no new census cells (the 40 cover the axis); TD1/TW/C1m/T1m shapes are
held by Rust pins, not census cells (no TRY_CAST/CAST source vocabulary).

No other cell moves. In particular the 16 VALUES-subquery F1 cells (D1/D4,
error-class mismatch, different mechanism), the 12 native-door F5-shaped cells
(D15/D16, native-door zone-ruling scope), every EQUAL/REFUSED/ERROR cell, and the
whole 976-cell ZS-9 set keep their recorded answers.

File-size budget. `conform_stage.rs` is 615 lines against the default ceiling 1000
(`make check-rust-file-size`; not an EXCEPTIONS row): 385 lines of headroom.
`conform_stage/tests.rs` is 329 lines (671 headroom); `zone_seam.rs` is 676 lines
(324 headroom). Expected step-1 delta is about 120-180 product lines (rule R1-R13,
F1 strip, key arm) plus about 150 test lines in `tests.rs`: no split needed. If the
budget tightens, the cohesive seam is the zone half of `conform_stage.rs`
(`nested_partial`, `seam_nested_arrow_casts`, `seam_call`, `zone_partial`,
`is_container`, `is_zone_leaf`, `partial_field`, lines 449-612) moved pure into
`conform_stage/seam.rs`, with a `map.md` row; tests already live apart.

## 6. Questions

- Q1. Widen step 1 files to `repark-functions/src/zone_seam.rs` (Map arms of
`converted_type` and `convert`, about 10 lines mirroring the value arms)?
Premise: the key pair cannot convert inside the seam UDF without it. Lean: yes;
the stage-only entries round trip is fragile on column-typed maps.
- Q2. Seam TD1 (`TRY_CAST(date)`, R9) and TW (`TRY_CAST(wall)`, R10), silent today,
with Rust pins and no census cells? Premise: Spark stores both; nothing new stays
silent. Lean: yes, seam both.
- Q3. C1m/T1m/TD1m/CD1m (CAST/TRY_CAST under the D8 CASE) stay ZS-9-refused?
Premise: ZS-9 stays per the order section 1; the D8/D10 refusal asymmetry is
recorded as measured. Lean: yes, stays refused; step-4 scope.
- Q4. Adopt F1 (strip outer-CAST metadata on scalar-foldable seamed args)?
Premise: section 2 shows the scalar path cannot deliver the metadata promise and
the sink re-attaches it. Lean: yes.
- Q5. Approve the new census shapes (`update_whole_where_part` with `WHERE k = 0`;
`insert_map_key` with key-packed targets restricted to that spelling)? Lean: yes
as drawn in section 5.
- Q6. TRY_CAST-instant exclusion per leaf (R8 mechanism) rather than whole-node
skip? Premise: mixed structs need per-leaf precision; `convert` already
short-circuits identical types. Lean: yes, per leaf.
