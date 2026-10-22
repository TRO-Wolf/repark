# ZONE-SEAM-1 — step 0: the sketch

**Date:** 2026-10-21. **Model:** Claude Opus 5.5 (`claude-opus-5-5`). **Base:** main `b7a20167`
(after #1137). **Order:** [order.md](order.md), adopted by the owner on 2026-10-20. **Card:**
[CONTAINER-CAST-ZONE-1](../../roadmap/mid-term/container-cast-zone-1-card-2026-10-20.md). **Pin:**
the ZONE-ORACLE-1 corpus ([report](../zone-oracle-1/evidence/corpus-main-754fb6e5.md)). Written
before any product code; one measurement made (§1). This file closes when the unit's pull
request merges.

**Verdict of step 0: the seam is buildable inside `repark-functions` with no change in
`repark-core` or the Iceberg write path and no ceiling raised.** It closes 313 DIFFERENT cells of
the corpus by construction (283 in step 2, 30 in step 3), 20 listed REFUSED/ERROR cells more if
Q2 is adopted, and leaves a residue of 95 DIFFERENT and 10 REFUSED cells that are not zone
defects (§8). Seven premises of the order are corrected (§2). Five questions go to the
orchestrator before step 1 (§10); none of them is a §5 halt.

## 1. The one measurement: the corpus on the base

`_record_zone_oracle_1.py`'s own `replay` and `classify_all`, called once from a scratch driver
on the `ci`-profile build of `b7a20167` (`maturin develop --profile ci`), under
`systemd-run --user --scope -p MemoryMax=40G`, `taskset -c 0-25`, `CARGO_BUILD_JOBS=12`, load
6.0 at the start. Corpus hash `72bef6e6…` equals the fixture's. Replay wall: 5.6 s.

| zone | EQUAL | DIFFERENT | REFUSED | ERROR |
|---|---:|---:|---:|---:|
| UTC | 265 | 57 | 4 | 2 |
| America/New_York | 215 | 107 | 4 | 2 |
| Asia/Kolkata | 227 | 95 | 4 | 2 |
| +05:30 | 227 | 95 | 4 | 2 |
| Etc/UTC | 265 | 57 | 4 | 2 |
| **all (1,640)** | **1,199** | **411** | **20** | **10** |

The non-EQUAL set equals the committed `zone_oracle_1_main_difference.json` cell for cell (0
left, 0 joined, 0 changed). **Main's counts moved since the order was written:** the order's
400 / 20 / 70 is the report on `754fb6e5`; PR #1131 (MAP-COLUMN-PANIC-1) re-recorded the list at
1,199 / 411 / 20 / 10 (its provenance names `5c8c811e`), and nothing merged since (#1124, #1137)
moved a cell. Step 1's red-first replay starts from these counts.

## 2. Corrections to the order's premises (confirmed on `b7a20167`)

1. **Line counts** of §2 hold on this base: `timestamp_ntz_cast.rs` 490, `ntz_cast_lower.rs` 86,
   `cast_map.rs` 449, `session_time_zone.rs` 383; also `json/to_json.rs` 475,
   `write/ntz_store.rs` 392, `write/update_cast.rs` 478, `write/nested_ns_gate.rs` 489,
   `router/nested_ns.rs` 849, `instant_ts.rs` 942, `timestamp_cast.rs` 936. `analyzer.rs` is
   1,150, an exception row of `scripts/check_rust_file_size.py`: it takes no line.
2. **`rewrite_ntz_target_cast` is not a general rewrite.** Its one caller is
   `retarget_dml_store_casts` (the top-level store cast of a DML statement), called from
   `SparkLtzTimestampCast::analyze` (`instant_ts.rs` line 539) and skipped under
   `spark.sql.timestampType=TIMESTAMP_NTZ`. The seam reuses the rule, not that function (§3).
3. **The SQL leaf `TRY_CAST(x AS TIMESTAMP_NTZ)` is session-aware on this base:**
   `bare.inst.try_ntz` is EQUAL in all five zones (the AST lowering sends `TRY_CAST` to
   `__repark_try_cast_timestamp_ntz__`). The leaf `TRY_CAST` defect is elsewhere: an instant to
   `STRING`, `DATE` or `BIGINT`, a wall to `STRING` or `TIMESTAMP`, a date or a `BIGINT` to
   `TIMESTAMP` (`SparkExprSemantics` checks a `TryCast`'s legality and rewrites nothing).
4. **`to_json` of a `TIMESTAMP_NTZ` leaf is not only a trailing `Z`.** The kernel renders the
   wall as if it were a UTC instant in the session zone: `struct.wall.to_json` answers
   `{"a":"2026-01-02T03:04:05.123Z"}` in UTC, `{"a":"2026-01-01T22:04:05.123-05:00"}` in New
   York and `{"a":"2026-01-02T08:34:05.123+05:30"}` in Kolkata. Spark answers
   `{"a":"2026-01-02T03:04:05.123"}` in every zone.
5. **The map container cast is not an `Expr::Cast` in the plan.** Every door splices
   `CAST(… AS MAP<…>)` into `__repark_cast_map__(value, '<type text>', …)` at the text level
   (`cast_map::rewrite_map_casts`, called by both SQL routers and the DataFrame expression
   builder). The seam meets maps at that call (§3).
6. **The §4 gate "zero DIFFERENT on the timestamp corpus" cannot be met by this unit's scope.**
   95 DIFFERENT and 10 REFUSED cells are error-condition, function-legality and `from_json`
   defects that no zone conversion reaches (§8). Question Q1.
7. **The native `repark.sql()` door has no session-aware leaf cast to reuse.** Its session
   (`NativeDoorTypes` in `repark-python` `session_runtime.rs`, and the `repark-sql` dialect)
   installs DataFusion's analyzer rules, `PlannedTimestampCast` (integer sources only) and the
   unit widen, never `SparkExprSemantics` or `SparkLtzTimestampCast`. Its container cast already
   answers what its own leaf cast answers. Neither the corpus nor the U-8 matrix measures it
   (both run `ReparkSession.sql`, the Spark door). Question Q5.

## 3. Placement and signature

**Where it runs.** In `SparkLtzTimestampCast` (`crates/repark-functions/src/instant_ts.rs`), the
analyzer rule that already holds the session-zone leaf rewrites, as the first call of
`rewrite_cast` (line 578), before the `timestamp_type.is_ntz()` branch, so both settings of the
knob take it. That rule runs after `SparkExprSemantics` (container legality, the leaf string,
date and numeric rewrites) and before `TypeCoercion`, on every plan node and in every subquery
(`transform_up_with_subqueries`). The Spark SQL door and the DataFrame door reach it: the
Spark extension adds `repark_functions::analyzer_rules()` (`repark-spark` `extension.rs` line
132), the passthrough route and CTAS analyse eagerly (`spark_ast.rs` line 272, `ctas.rs` line
209), and so does a DataFrame (`repark-python` `dataframe/mod.rs` line 183). The native
`repark.sql()` door does not (§2.7, Q5).
`instant_ts.rs` grows by about four lines (942 → about 946, ceiling 1000).

**What it is.** A new module `crates/repark-functions/src/zone_seam.rs` (one `pub mod` line in
`lib.rs`, 183 → 184 of its 186) with its tests and `map.md` under `zone_seam/`:

```rust
pub(crate) fn rewrite_zone_seam(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>>;
pub fn zone_seam_udf(try_cast: bool) -> Arc<ScalarUDF>;
fn seam_target(source: &DataType, target: &DataType) -> Option<DataType>;
```

- `seam_target` walks source and target together (struct field by position, list element of
  every list layout, map value) and returns the **partial type**: the source type with each
  seam-pair leaf (§4) replaced by its target leaf type and every other leaf left as the source
  has it. `None` when no position holds a seam pair.
- `rewrite_zone_seam` matches three shapes and returns the rest unchanged:
  - `Expr::Cast(v, T)` with a container source: `Cast(zone_seam(v, P), T)`, where `P` is the
    partial type; when `P == T`, `zone_seam(v, T)` alone.
  - `Expr::TryCast(v, T)`: the same with the try twin and an outer `TryCast`; at depth 0 (a
    bare leaf) for the leaf `TRY_CAST` pairs of §4.
  - `__repark_cast_map__(v, '<type>', …)`: `__repark_cast_map__(zone_seam(v, P), '<type>', …)`.
  The outer cast then does exactly what it does today for every non-seam member (Arrow's cast,
  the map cast's key legality and Spark leaf casts), and is the identity at every seam leaf. So
  every other member keeps its native cast by construction, and container legality stays where
  `SparkExprSemantics` already ran it.
- `zone_seam_udf` (`__repark_zone_seam__` and `__repark_try_zone_seam__`) carries `P` as a
  second literal argument (the Arrow type text, read back with `DataType::from_str`, the
  `arrow_cast` convention), so it needs no per-plan state. `return_field_from_args` refuses a
  refusing pair at planning (§4, rows 9–10). `invoke_with_args` walks the Arrow value: struct
  children, list `values()`, map entry values; at a seam leaf it **invokes the leaf UDF the
  depth-0 rewrite emits** on that child array, with the call's own `config_options` (session
  zone, ANSI), and rebuilds the parent with its offsets and null buffer. Before a leaf call it
  masks the child with the parents' nulls, so a value hidden under a NULL struct or list never
  reaches an ANSI kernel. No conversion kernel is written: every seam leaf is computed by the
  function the bare leaf cast already calls.

**Function site (step 2b).** `to_utc_timestamp(x, tz)` and `from_utc_timestamp(x, tz)` are
DataFusion-Spark UDFs. Where argument 0 has type `TIMESTAMP_NTZ` (and, if Q2 is adopted,
`DATE` or untyped `NULL`), the same `rewrite_zone_seam` wraps it in the conversion Spark's
implicit cast applies, the row 5 / row 7 leaf of §4, so the function receives an instant and
returns `TIMESTAMP`. Composition checked on the fixture: Spark's
`to_utc_timestamp(<wall>, 'Asia/Kolkata')` in New York, `1767321245123456`, is the leaf cast
`1767341045123456` read as a Kolkata wall; `bare.inst.to_utc_timestamp` is already EQUAL.

**`to_json` site (step 3).** `json/to_json.rs`, `write_scalar` line 183: the branch
`DataType::Timestamp(unit, _)` renders every timestamp with the session offset. A naive leaf
(`Timestamp(_, None)`) takes a new branch that renders its wall with no zone in the same
millisecond form, `%Y-%m-%dT%H:%M:%S%.3f`, Spark's `{"a":"2026-01-02T03:04:05.123"}`. The
zoned branch is untouched (`*.inst.to_json` is EQUAL in all five zones). Map keys go through
the same function. About ten lines; 475 → about 485.

## 4. The (source leaf, target leaf) pairs the seam converts

Every pair below is microsecond. For every row the corpus holds Spark's answer, the bare leaf
`CAST` is EQUAL in all five zones, and **Spark's container answer equals Spark's bare-leaf
answer in every recorded cell** (read from the fixture, 28 pair groups: container `CAST` against
leaf `CAST`, container `TRY_CAST` against leaf `TRY_CAST`). The one pair where they differ,
`DATE` to `BIGINT` (container `CAST_WITH_CONF_SUGGESTION`, leaf `CAST_WITH_FUNC_SUGGESTION`), is
not a seam pair (§5).

| # | source leaf (Arrow / Spark) | target leaf | leaf UDF the seam invokes | `CAST` fails | `TRY_CAST` fails |
|--:|---|---|---|---|---|
| 1 | `Timestamp(µs, UTC)` / `TIMESTAMP` | `Timestamp(µs, None)` / `TIMESTAMP_NTZ` | `__repark_cast_timestamp_ntz__` (try: `__repark_try_cast_timestamp_ntz__`) | never in range; out of range as the leaf (ANSI raises) | NULL, the try twin's `tolerate` |
| 2 | `TIMESTAMP` | `Utf8` / `STRING` | `spark_timestamp_to_string_udf` | never | never: the `CAST` kernel serves both |
| 3 | `TIMESTAMP` | `Date32` / `DATE` | `spark_timestamp_to_date_udf` | never | never: the `CAST` kernel |
| 4 | `TIMESTAMP` | `Int64` / `BIGINT` | `spark_epoch_seconds_floor_udf`, then `Int64` | never (seconds fit) | never: the `CAST` kernel |
| 5 | `Timestamp(µs, None)` / `TIMESTAMP_NTZ` | `TIMESTAMP` | `to_timestamp_udf` (`instant_ts.rs`) | a wall the zone table cannot place (unheld) | the `CAST` kernel; a wall it cannot place must answer NULL, pinned in step 1, else a §5 halt |
| 6 | `Utf8` / `STRING` | `TIMESTAMP` | `to_timestamp_udf` (try: `try_to_timestamp_udf`, what `rewrite_string_try_cast` emits) | malformed text: `CAST_INVALID_INPUT` under ANSI, as the leaf | NULL, the try kernel |
| 7 | `Date32` / `DATE` | `TIMESTAMP` | `to_timestamp_udf` | never in range | the `CAST` kernel |
| 8 | `TIMESTAMP_NTZ` | `STRING` | `spark_timestamp_to_string_udf` | never | never: the `CAST` kernel |
| 9 | `TIMESTAMP_NTZ` | `BIGINT` | none: refused at planning | `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`, the leaf's condition (`cast_legality`) | the same refusal (Spark refuses `TRY_CAST` of a type mismatch) |
| 10 | `Int64` / `BIGINT` | `TIMESTAMP_NTZ` | none: refused at planning by the NTZ UDF's `cast_refusal` | `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION` | the same refusal |
| 11 | `BIGINT` (leaf `TRY_CAST` only) | `TIMESTAMP` | the leaf `CAST` rewrite's `Timestamp(Second)` widening | — | overflow answers NULL (unheld) |

Rows 1–4 are the order's §0 (a zoned leaf to a naive timestamp, a string, a date) plus `BIGINT`,
which the card's "Retires" names. Rows 5–7 are the reverse direction, the corpus's shape 1
("wall, string or date to TIMESTAMP answers the UTC reading"). Row 8 is the corpus's format
defect for a wall. Rows 9–10 keep the card's "through `BIGINT`" row and Spark's refusal; the
refusal text names the container types as Spark does (`cast_map::spark_sql_name`), so the
`cast_legality` function is reached by making its module `pub(crate)` (one token in
`analyzer.rs`, no line). Row 11 and rows 2, 3, 4, 5, 7, 8 under `TRY_CAST` are the leaf
`TRY_CAST` defect of §2.3; at depth 0 the seam routes the bare `TryCast` to the same leaf the
bare `CAST` takes, because for these pairs nothing can fail on an in-range value.

## 5. What the seam does not convert

- **Unheld by the corpus (recorded, not guessed; §5 halt 1):** an instant to `INT`, `SMALLINT`,
  `TINYINT`, `FLOAT`, `DOUBLE` or `DECIMAL` inside a container (the corpus targets `BIGINT`
  only); a map **key** holding a timestamp (the corpus keys are string literals); a container
  cast to a scalar `STRING` (`CAST(<struct> AS STRING)`); any pair under
  `spark.sql.timestampType=TIMESTAMP_NTZ`; a `BIGINT` source other than `BIGINT` to
  `TIMESTAMP_NTZ` (row 10 is `BIGINT` only). These keep their native cast. Question Q3.
- **`timestamp_ns` at either end:** excluded by the NS create-door ruling and R-015; a
  nanosecond leaf keeps its refusal and its kernels.
- **EQUAL natively and left native:** an instant to `TIMESTAMP`, a wall to `TIMESTAMP_NTZ` or
  `DATE`, a string or date to `TIMESTAMP_NTZ`, `DATE` or `STRING` (every one EQUAL in every
  container and zone on the base).
- **`DATE` to `BIGINT`, `STRING` to `BIGINT`:** error-condition defects, not zone ones (§8).
- **Casts `TypeCoercion` inserts after the rule** (a `CASE` or `array()` that unifies a zoned
  and a naive member, a set operation): the seam does not see them. Named for the verifier.
- **The write path's own store casts** (`update_cast::nested_leaf_cast_sql` and
  `store_assignment_cast_sql`, which build `arrow_cast(…)` text): `arrow_cast` becomes an
  `Expr::Cast` only in the optimizer, after the analyzer, so the seam does not reach a store
  conversion and no Iceberg write file changes (§5 halt 3).

## 6. The gate arms of R-011 and R-015 once the seam lands (named, not removed)

R-011's micros gate is `refuse_zoned_micros` → `proven_naive` / `naive_columns` in
`crates/repark-spark/src/router/nested_ns.rs`. The plan it walks comes from `planned()` →
`execute_calibrated` → the passthrough route, which analyses eagerly, so the seam has run on it.

| arm | where | after the seam |
|---|---|---|
| A2, a `CAST`/`TRY_CAST` from a type holding a zoned leaf to one holding a naive leaf in the column's lineage | `casts_a_zoned_value_to_naive` (line 703), used at lines 375 and 416; `nested_ns_gate::casts_zoned_to_naive` (line 156) | **unreachable for its purpose:** every cast that turns a zoned µs leaf into a naive µs leaf at one position is rewritten before the gate reads the plan. It stays reachable only for a cast with a zoned leaf and a naive leaf at different positions (no conversion: an over-refusal on main too) and for a naive nanosecond target; neither can make a UTC wall. GATE-RETIRE-1 candidate. |
| A1, the planned type of a value that reaches the column holds a zoned timestamp | `holds_zoned_timestamp` at lines 374 and 415 | **reachable and load-bearing.** The store conversion of a zoned container into a naive column is the write path's `arrow_cast` (§5), outside the seam. |
| A3, the plain `UPDATE` whole-struct assignment holding a `TIMESTAMP_NTZ` cast | `shifts_in_an_update` (line 719), used at line 382 | reachable; an `UPDATE` write-path defect (the session offset added inside an assigned struct), not a cast. |
| A4, unplannable statement or `PREPARE` with a parameter | lines 125–127, 345 | reachable; unrelated. |
| the zone arm | `session_is_utc` (line 73) | unchanged. |
| R-015, `refuse_nested_ns_supply` | `nested_ns_gate.rs` line 282 | unchanged: it reads the target column's type only. No R-015 arm becomes unreachable. |

**Consequence for the U-8 door matrix (step 6).** A2 is what refuses the matrix sources
`ts_struct_cast` and `ts_struct_cast_column` outside UTC today (44 + 44 and 36 + 36 cells on the
U-8 head record). With the seam those statements no longer hold the native cast, so up to 160
cells move from the refusal to a stored value. The expected value is the session wall the
`ntz_cast` source stores (`1767305045123456` in New York); any other value is a §5 halt.
Question Q4. **By reading, unmeasured:** a map container cast never matched A2 on main
(`__repark_cast_map__` is not an `Expr::Cast`), so an `INSERT` of `CAST(map('k', x) AS
MAP<STRING, TIMESTAMP_NTZ>)` outside UTC passes the gate on main and stores the UTC wall; the
seam corrects the value. The refusal hint's sentence "a CAST of the whole struct, array or map
is refused" (`nested_ns_gate.rs` `MICROS_HINT`) goes stale where A2 stops matching; it lives in
the Iceberg write path, so it waits for GATE-RETIRE-1 and the R-011 status line says so.

## 7. The card's tables, by step

| card row | closed in | how |
|---|---|---|
| plain `SELECT`: `CAST(array(x) AS ARRAY<TIMESTAMP_NTZ>)[0]`, the struct, the map literal | step 2 | row 1; corpus `array.inst.ccast_ntz`, `struct.inst.ccast_ntz`, `map.inst.ccast_ntz` |
| control `CAST(x AS TIMESTAMP_NTZ)` | unchanged | depth-0 `CAST` is not touched |
| through `STRING` (`INSERT`, MERGE update, `ARRAY`) | step 2 | row 2, then the native string-to-NTZ parse (EQUAL); corpus `through.struct_inst_string_ntz`, `through.array_inst_string_ntz`, `through.map_inst_string_ntz` |
| the leaf cast over a string the container made | step 2 | row 2; `through.leaf_over_array_inst` |
| through `DATE` | step 2 | row 3; `through.struct_inst_date_ntz`, `through.array_inst_date_ntz` |
| through `BIGINT` (Spark refuses) | step 2 | row 4 then row 10: refused at planning, no file; `through.struct_inst_bigint_ntz` |
| `from_json(to_json(…), 'v TIMESTAMP_NTZ, …')` (Spark NULL) | **not closed** | `to_json` of the instant is already Spark's; `from_json` into `TIMESTAMP_NTZ` accepts a text with an offset where Spark answers NULL. Residue; Q1 |
| the six table-creating writers | step 2, pinned step 4 | CTAS analyses eagerly; the writers store what the `SELECT` yields |
| the cached frame written with `writeTo().append()` | step 2, pinned step 4 | the frame's analyzed plan holds the seam |

## 8. Corpus cells this unit closes, by id

A cell id is `<expression>.<zone tag>`; the five letters are UTC, New York, Kolkata, +05:30,
Etc/UTC on the base (`E` EQUAL, `D` DIFFERENT, `R` REFUSED, `X` ERROR). Every listed letter that
is not `E` must be `E` on the head.

**Step 2, container casts (63 expressions, 219 DIFFERENT cells):** `array.col_inst.ccast_ntz`
EDDDE, `array.col_inst.ccast_string` DDDDD, `array.col_wall.ccast_ltz` EDDDE,
`array.date.ccast_ltz` EDDDE, `array.inst.ccast_bigint` DDDDD, `array.inst.ccast_date` EDEEE,
`array.inst.ccast_ntz` EDDDE, `array.inst.ccast_string` DDDDD, `array.inst.ctry_date` EDEEE,
`array.inst.ctry_ntz` EDDDE, `array.inst.ctry_string` DDDDD, `array.str.ccast_ltz` EDDDE,
`array.wall.ccast_bigint` DDDDD, `array.wall.ccast_ltz` EDDDE, `array.wall.ccast_string` DDDDD,
`array.wall.ctry_ltz` EDDDE, `array2.inst.ccast_bigint` DDDDD, `array2.inst.ccast_date` EDEEE,
`array2.inst.ccast_ntz` EDDDE, `array2.inst.ccast_string` DDDDD, `array2.wall.ccast_ltz` EDDDE,
`array_struct.inst.ccast_bigint` DDDDD, `array_struct.inst.ccast_date` EDEEE,
`array_struct.inst.ccast_ntz` EDDDE, `array_struct.inst.ccast_string` DDDDD,
`array_struct.wall.ccast_ltz` EDDDE, `map.col_inst.ccast_ntz` EDDDE, `map.col_inst.ccast_string`
DDDDD, `map.col_wall.ccast_ltz` EDDDE, `map.date.ccast_ltz` EDDDE, `map.inst.ccast_bigint` DDDDD,
`map.inst.ccast_date` EDEEE, `map.inst.ccast_ntz` EDDDE, `map.inst.ccast_string` DDDDD,
`map.inst.ctry_date` EDEEE, `map.inst.ctry_ntz` EDDDE, `map.inst.ctry_string` DDDDD,
`map.str.ccast_ltz` EDDDE, `map.wall.ccast_bigint` DDDDD, `map.wall.ccast_ltz` EDDDE,
`map.wall.ccast_string` DDDDD, `map.wall.ctry_ltz` EDDDE, `struct.col_inst.ccast_ntz` EDDDE,
`struct.col_inst.ccast_string` DDDDD, `struct.col_wall.ccast_ltz` EDDDE, `struct.date.ccast_ltz`
EDDDE, `struct.inst.ccast_bigint` DDDDD, `struct.inst.ccast_date` EDEEE, `struct.inst.ccast_ntz`
EDDDE, `struct.inst.ccast_string` DDDDD, `struct.inst.ctry_date` EDEEE, `struct.inst.ctry_ntz`
EDDDE, `struct.inst.ctry_string` DDDDD, `struct.str.ccast_ltz` EDDDE, `struct.wall.ccast_bigint`
DDDDD, `struct.wall.ccast_ltz` EDDDE, `struct.wall.ccast_string` DDDDD, `struct.wall.ctry_ltz`
EDDDE, `struct2.inst.ccast_bigint` DDDDD, `struct2.inst.ccast_date` EDEEE,
`struct2.inst.ccast_ntz` EDDDE, `struct2.inst.ccast_string` DDDDD, `struct2.wall.ccast_ltz` EDDDE.

**Step 2, chained casts (8 expressions, 22 DIFFERENT cells):** `through.array_inst_date_ntz`
EDEEE, `through.array_inst_string_ntz` EDDDE, `through.leaf_over_array_inst` EDDDE,
`through.map_inst_string_ntz` EDDDE, `through.struct_inst_bigint_ntz` DDDDD,
`through.struct_inst_date_ntz` EDEEE, `through.struct_inst_string_ntz` EDDDE,
`through.struct_wall_string_ltz` EDDDE.

**Step 2, leaf `TRY_CAST` (7 expressions, 27 DIFFERENT cells):** `bare.date.try_ltz` EDDDE,
`bare.epoch.try_ltz` DDDDD, `bare.inst.try_bigint` DDDDD, `bare.inst.try_date` EDEEE,
`bare.inst.try_string` DDDDD, `bare.wall.try_ltz` EDDDE, `bare.wall.try_string` DDDDD.

**Step 2b, function site (3 expressions, 15 DIFFERENT cells):** `bare.col_wall.to_utc_timestamp`
DDDDD, `bare.wall.from_utc_timestamp` DDDDD, `bare.wall.to_utc_timestamp` DDDDD. **If Q2 is
adopted, 4 more (10 ERROR, 10 REFUSED):** `bare.date.from_utc_timestamp` XXXXX,
`bare.date.to_utc_timestamp` XXXXX, `bare.null.from_utc_timestamp` RRRRR,
`bare.null.to_utc_timestamp` RRRRR.

**Step 3, `to_json` (6 expressions, 30 DIFFERENT cells):** `array.wall.to_json`,
`array2.wall.to_json`, `array_struct.wall.to_json`, `map.wall.to_json`, `struct.wall.to_json`,
`struct2.wall.to_json`, each DDDDD. **Watched, not claimed:** `through.json_wall_ltz` EDDDE
closes if `from_json` reads a zoneless text in the session zone; `through.json_wall_ntz` (EEEEE)
must stay EQUAL when the text it parses loses its offset.

**Residue, not this unit (21 expressions: 95 DIFFERENT, 10 REFUSED):**
`bare.str.cast_bigint`, `bare.col_str.cast_bigint`, `struct.str.ccast_bigint`,
`array.str.ccast_bigint` (Spark's `CAST_INVALID_INPUT`, RePark's Arrow error without the
condition); `struct.date.ccast_bigint`, `array.date.ccast_bigint`, `map.date.ccast_bigint`
(Spark refuses with `CAST_WITH_CONF_SUGGESTION`; the leaf refuses with
`CAST_WITH_FUNC_SUGGESTION`); `bare.{wall,str,date,epoch,col_wall,col_str}.unix_micros`;
`bare.epoch.{to_timestamp_ntz,to_utc_timestamp,from_utc_timestamp,date_format,to_date}`
(function legality); `bare.{inst,wall}.date_format_micros` (REFUSED, the `S` letter);
`through.json_inst_ntz` (`from_json` into `TIMESTAMP_NTZ`).

**Projected head counts** (step 6 measures them): EQUAL 1,512, DIFFERENT 98 (95 residue + the 3
watched), REFUSED 20, ERROR 10; with Q2, EQUAL 1,532, REFUSED 10, ERROR 0. No EQUAL cell is
expected to move: depth-0 `CAST`, the `inner` level, every zoned-branch `to_json` and every
natively EQUAL pair are untouched.

## 9. §5 halts checked

| halt | result |
|---|---|
| a pair whose Spark answer the corpus does not hold | none converted: every pair of §4 is held; the unheld pairs of §5 stay native and are recorded (Q3) |
| a leaf whose session-aware answer would differ from the existing leaf cast's | none: for rows 1–10 the seam invokes the bare leaf `CAST`'s own function, which is EQUAL in every zone; Spark's container answer equals its leaf answer in every recorded cell. The leaf `TRY_CAST` answers change on purpose (the defect of §2.3) to the leaf `CAST`'s answer, for pairs that cannot fail on an in-range value |
| a change in `repark-core` or the Iceberg write path | none needed: the zone is read through `repark_functions::session_time_zone`; store casts stay outside the seam (§5); the stale hint waits for GATE-RETIRE-1 |
| a ceiling that would rise | none: new file under `repark-functions`; `instant_ts.rs` 942 → about 946 and `to_json.rs` 475 → about 485 of 1000; `lib.rs` 183 → 184 of 186; `analyzer.rs` one token, no line; `cast_map.rs` and `timestamp_ntz_cast.rs` untouched. `make check-comment-density` does not exist on this base |
| any EQUAL cell that moves | none expected (§8); measured on main and head in step 6 with the U-8 matrix |
| a cost over the gate | not measured in step 0; step 6 (the U-8 verifier's six statements, 10-run medians, ≤ 1.05x) |

## 10. Questions for the orchestrator (premise, lean)

- **Q1 — the §4 replay gate.** *Premise:* §4 asks for zero DIFFERENT on the corpus; after this
  unit 95 DIFFERENT cells remain (§8 residue), none of them a zone conversion, and the
  `from_json` row of the card is one of them. *Lean:* read the gate as "zero DIFFERENT among
  the cells §8 assigns to this unit, and the residue exactly the §8 list"; card the residue as
  two small units (error conditions and function legality; `from_json` into `TIMESTAMP_NTZ`).
- **Q2 — `to_utc_timestamp` / `from_utc_timestamp` of a `DATE` or an untyped `NULL`.**
  *Premise:* Spark applies the same implicit cast to `TIMESTAMP` as for a wall; the corpus holds
  the answers (`bare.date.*` ERROR, `bare.null.*` REFUSED on the base); the order names the wall
  only. *Lean:* include (rows 7 and a typed NULL at the same site; 20 listed cells to EQUAL).
- **Q3 — numeric targets other than `BIGINT`, and map keys.** *Premise:* the leaf cast answers
  them (TZ-5 for numerics) but this corpus does not hold the container answers, and the halt
  says record, do not guess. *Lean:* keep them native in this unit; extend ZONE-ORACLE-1 with
  `INT`, `DOUBLE`, `DECIMAL` targets and a timestamp map key, then a one-line widening of
  `seam_target`.
- **Q4 — R-011 refusals the seam lifts.** *Premise:* the gate reads the analyzed plan, so up to
  160 U-8 matrix cells (`ts_struct_cast`, `ts_struct_cast_column`, outside UTC) move from the
  refusal to the stored session wall, the value Spark stores (R-011's documented oracle). *Lean:*
  accept them as listed refusal→EQUAL moves (the owner's "refuse only what Spark refuses"),
  record them in R-011's status line in step 7, and leave the arm and the hint to GATE-RETIRE-1.
- **Q5 — the `repark.sql()` door in step 4.** *Premise:* §2.7; the order's step 4 lists
  `repark.sql()` beside the Spark SQL and DataFrame doors. *Lean:* step 4 pins the native door
  as an unchanged control (its container cast equals its leaf cast before and after); making the
  native door's casts session-aware is a decision about that door's semantics (ADR 0002), not
  this unit. The alternative, the seam rule added to `NativeDoorTypes`, would also need the leaf
  rewrites there, which is new semantics on that door.

## 11. Steps 1–7, as the order orders them

1. Red first: a ledger `task/ledgers/staging/zone-seam-1-ledger.md` lists §8's cells by id;
   Rust pins per pair of §4 (×`CAST`/`TRY_CAST` × UTC, New York, Kolkata), SQL through
   `planned_context` and `analyzer_rules()` as the `instant_ts` tests do, red on `b7a20167`; the
   try-failure pins of rows 1, 5, 6 and 11.
2. The seam (`zone_seam.rs` + the `rewrite_cast` call), then 2b, the function site.
3. `to_json`.
4. Pins green on the Spark SQL door and the DataFrame door (`repark.sql()` per Q5): §7's rows, the six
   writers, the cached frame; controls (leaf casts, UTC sessions, a wall source, the
   `timestamp_ns` refusal of R-015) unchanged.
5. Mutants: each §4 row removed alone reds a pin; the `rewrite_cast` call removed reds the
   corpus.
6. Never worse: the corpus and the U-8 door matrix on main and head; cost.
7. Records: the card, R-011 and R-015 status lines, the v1.5.6 notes line, `map.md`s,
   `zone_oracle_1_main_difference.json` re-recorded on the head with the recorder's `--main`, the
   ledger moved to `completed/`.

Up: [map.md](map.md).
