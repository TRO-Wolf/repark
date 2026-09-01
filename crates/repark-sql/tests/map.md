# map — repark-sql/tests

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

## Purpose

Integration tests for the ANSI door. The end-to-end battery is in `../src/tests.rs`; this directory
holds behavior observed from outside the crate.

## Contents

- `ansi_door_string_literals.rs` — **SQP-1 control (C-006):** the native/ANSI door keeps generic
  literal semantics (backslash literal, `\'` does not lex, raw strings refuse) — Spark-only (ADR-0002).
- `parser_productions.rs` — pins stock-parser support and the productions that still require
  pre-parse recognition (`ALTER … SET PROPERTIES`, `ALTER … EXECUTE`, and `FOR … AS OF`).

- `session_timestamp_type_ansi_door.rs` — **Q10:** ANSI-door cell of
  `spark.sql.timestampType=TIMESTAMP_NTZ` on a Spark-extended session
  (`sql_with(AnsiDialect)`). Literal + CAST agree with the Spark door, value
  AND Arrow type (naive µs).
- `declared_sorted_tighten.rs` — ANSI CREATE, VIEW, and SELECT INTO refuse tightened plans
  before publication. Derived expressions, subqueries, cached views, default-catalog names,
  and lazy view hops are covered. Nullable projections and ordinary CTAS remain allowed.
- `session_wiring.rs` — the door's REACHABILITY: `AnsiDialect` installed on a real
  `ReparkSession` through `ReparkSessionBuilder::with_sql_dialect`, driving schema DDL, CTAS,
  INSERT and a typed read through `session.sql`, plus a refusal that must survive the session
  boundary. It lives out here because "reachable through a session" is precisely what a unit
  test calling `AnsiDialect.execute(...)` on a bare `SessionContext` cannot show
  (`surfaces::SQL_DIALECT_SEAM`). **A11:**
  `ansi_column_def_nanosecond_timestamp_shapes_refuse` — native ANSI column-def
  `CREATE TABLE` refuses nanosecond-precision timestamps at DDL time (bare
  `TIMESTAMP`, `TIMESTAMP(9)`, WITH/WITHOUT TIME ZONE twins; needle = column +
  precision 9 + `TIMESTAMP(6)`). Positive control:
  `ansi_column_def_timestamp_6_create_is_unchanged`. Spark-door `TIMESTAMP` →
  Iceberg `timestamptz` is documented, not changed. CTAS / ALTER stay out of
  this unit.

- `introspection.rs` — `SHOW TABLES` / `DESCRIBE` / `information_schema` delegate through a
  configured session. Metadata tables stay queryable but remain hidden from enumeration (RP-5
  F-8: fork `table_names`, no engine shim). The
  time-travel test checks cleanup of both `__repark_ansi_tt_*` and `__repark_tt_*` names.
  pins: rp-5-fork-repin/C-003

- `ta_toll.rs` (Q11) — `TaExtension` on a **native** session, one kernel driven through
  ANSI-door SQL as a window function and compared `f64::to_bits` against the recorded C TA-Lib
  golden, plus the non-literal-period refuse and the "absent until you opt in" row. Needs the
  `repark-ta` dev-dep (feature `datafusion`). The SQL same-OVER fusion pin covers named `OVER w`
  and inline same-spec plans, each with one
  `WindowAggExec`; an intervening filter between two live windows stacks two. Same SQL
  shapes as `../../repark-spark/tests/ta_window.rs`.

- `cross_door_int_overflow.rs` — **F-Y10-1 C-003:** INT add overflow raises on both
  doors at default ANSI; Spark `ansi=false` wraps while ANSI still raises.
  pins: f-y10-1-int-overflow/C-003
- `cross_door.rs` (Q13) — the **two-session** cross-door protocol: a native
  `AnsiDialect` session and a Spark-extended `SparkDialect` session, each over its OWN in-memory
  catalog, compared on the Arrow path (value AND type). Rows: CTAS, INSERT, ALTER (schema
  evolution + table rename), MERGE, time travel, identifier case folding, the single-session
  legality boundary (pure catalog DDL), the session-scope guard rail that explains why one
  session cannot do this job, **G-7b decimal128** (`cross_door_decimal_add_same_precision_scale_bit_exact`,
  `cross_door_decimal_mul_money_by_quantity_bit_exact` — same SQL through both doors, schema +
  nullability + raw i128 equal; corpus rows `add_same_precision_scale` /
  `mul_money_by_quantity`), **G12 three-valued logic** (`cross_door_tvl_true_and_null_is_null`,
  `cross_door_tvl_case_when_null_predicate` — portable SQL, Boolean/Int32 type + nullability +
  value equal across doors; corpus rows `and_true_null_is_null` / `case_when_null_predicate`;
  no Spark-only `<=>`), and
  `cross_door_g3e8_refusals_render_identically` (the permanent v1 valve: mixed AND/OR, nested,
  scalar, ANY/ALL, UPDATE NOT IN; IN / NOT IN / EXISTS /
  correlated IN / UPDATE IN execute) plus executed columns
  `cross_door_g3e8_not_in_delete_executes_identically`,
  `cross_door_g3e8_exists_delete_executes_identically`,
  `cross_door_g3e8_correlated_in_delete_executes_identically`,
  `cross_door_g3e8_update_in_executes_identically`, which compares a
  **rendered refusal string** rather than a result: the G3-E8 valve is implemented twice (no
  door→door product edge), and this is the only pin that can see the two copies drift, including
  the rendered TARGET that the per-door message pins cannot. **G11:** six **INTENDED** door-vs-door value
  divergences (correctness, not parity — Spark is not the ANSI oracle): integer `/` (truncate
  vs float), integer `/ 0` (**U5:** both raise; names kept), float `/ 0` (**U5:** IEEE +Inf vs
  Spark `DIVIDE_BY_ZERO`; names kept), decimal `/ 0` (**U5:** both raise; names kept), default
  `ORDER BY ASC` (NULLS LAST vs FIRST), default `ORDER BY DESC`
  (NULLS FIRST vs LAST). Each row asserts both doors' actual Arrow outputs side by side; the
  one-sentence reason is the test's doc comment. Needs the `repark-spark` dev-dep — the only
  place either door may name the other, and legal because the crate-DAG guard scopes layering
  to normal edges. pins: f-y10-1-int-overflow/C-003

  Its case-folding row (`cross_door_identifier_case_folding_agrees_unquoted_and_diverges_quoted`)
  is a **declared-divergence test**: it names the registry row it defends
  — [`../../../docs/spark-sql-iceberg-parity.md`](../../../docs/spark-sql-iceberg-parity.md) §3
  row ID-1, the registry's first declared row (decision D3) — and asserts the refusal *text*, not
  merely that an error occurred (`No field named` **and** the quoted `"ID"`: the class of failure
  and the identifier, because a bare `contains("ID")` also matches `INVALID` / `UUID` and would
  attribute nothing), so the row stays attributable to identifier resolution. It reds
  if the divergence silently disappears, which is the point: the registry holds the semantics and
  this test holds the registry honest.

- `session_timezone_ansi_door.rs` — the ANSI-door cell of the
  session-timezone matrix. On ONE Spark-extended session at a non-UTC zone, `sql_with(AnsiDialect)`
  and the Spark door return the same calendar fields, value AND Arrow type — a legal
  single-session row, because what is measured is that the DOOR does not change the answer
  (extensions are session-scoped). It also pins the honest negative: an extension-free session is
  stock DataFusion and reads the stored zone, which is a property of that profile rather than a
  Spark divergence. Needs the same `repark-spark` dev-dep as `cross_door.rs`, for the same
  reason — the reverse edge does not exist, so this cell can only be built here.

- `timestamp_cast_ansi_door.rs` — the ANSI-door cell of the
  `CAST(TIMESTAMP AS <numeric>)` epoch-seconds matrix, built here for the same crate-DAG reason as
  `session_timezone_ansi_door.rs`. On ONE Spark-extended session both doors scale a timestamp cast
  to epoch SECONDS, value AND Arrow type, including the negative FRACTIONAL second where Spark
  floors (`-0.5 s → -1`). Its second pin is the honest negative profile control
  evidence: a bare, extension-free session still returns the raw nanosecond tick
  (`-1800000000000`), which is correct for that profile.

- `ansi_door_values.rs` — ANSI-door-only value pins. Standard SQL
  is the oracle (ruling: correctness, not Spark parity). Rows on a native `AnsiDialect`
  session, Arrow path, value AND type: CAST overflow raises, integer `/` truncates, integer
  `/ 0` raises, `SUM` skips NULLs, default `ORDER BY ASC` is `NULLS LAST`, implicit
  string→number coercion refuses, **F-Y10-1** INT `+` overflow raises
  (`ansi_door_int32_add_overflow_raises`) and untyped `1 + 1` /
  `2147483647 + 1` stay Int64; unaliased `x + 1` keeps the BinaryExpr name.
  Helpers do **not** call `install_integer_overflow` (session build must).
  Does **not** edit `timestamp_cast_ansi_door.rs` or
  `session_timezone_ansi_door.rs`. Identifier case folding is registry ID-1 (cited, not
  duplicated). Cargo wires this file as its
  own integration-test binary — this crate has no `tests/mod.rs`.
  pins: f-y10-1-int-overflow/C-003

- `ansi_door_join_null_keys.rs` — Native-profile NULL-key join
  pin (INNER / LEFT / LEFT SEMI / LEFT ANTI). G11: standard-SQL 3VL (`NULL = NULL` is
  unknown). Spark 4.1.2 agrees (G4 corpus); documented, not a parity claim. Matrix cite:
  `ansi_door_null_keys_never_match_inner_left_semi_anti`.

- `ansi_door_window_frames.rs` — Native-profile ROWS/RANGE
  frame-value pins. Numeric frames document agreement with G5; unit-less `RANGE 1
  PRECEDING` over DATE is DF-native **months** (Spark reads **days**). Matrix cite:
  `ansi_door_rows_and_range_frame_values`.

- `ansi_door_float_agg.rs` — Native-profile float-aggregation pins —
  `f64::to_bits` `sum`/`avg` at `target_partitions` 1/2/8 over the catastrophic-cancellation
  fixture, plus stability and the p=8 spread disclosure. Matrix cite:
  `ansi_door_sum_f64_bits_at_target_partitions_1`.

## Pointers

- Up: [../map.md](../map.md). Seam and session-scope rule:
  `docs/design/session-api.md`.

## Debug

- `ansi_door_string_literals.rs` is byte-frozen (sha256) by
  `python/repark-parity/tests/test_pr_245_revalidation_record.py`; any edit, a comment rewrap
  included, reds that record — revert, never re-hash.
| Symptom | First check |
|---|---|
| `m2_productions_still_need_a_pre_parse_recognizer` RED | The parser now reaches a production expected to require recognition; inspect the matching handler and parser version |
| A stock production stopped parsing | The matching handler may be unreachable; check the DataFusion/sqlparser version bump |
| `session_wiring` RED on the catalog-visible read | The dialect is probably not installed (session default fell back to `DataFusionDialect`, whose CTAS makes a `MemTable`) |
| `ansi_column_def_nanosecond_timestamp_shapes_refuse` RED | The A11 DDL refuse moved or the Iceberg v2 write-path residual came back. Check `create_table::refuse_nanosecond_timestamp_columns` — the needle must name the column, precision 9, and `TIMESTAMP(6)` |
| `ansi_column_def_timestamp_6_create_is_unchanged` RED | A µs-precision CREATE started failing or the Arrow type is no longer `timestamp[us]`. Do not "fix" it by remapping ns → µs (that is the Spark door, not this refuse) |
| `introspection` RED with "not supported unless information_schema is enabled" | The repark-core builder→`SessionConfig` plumbing (`apply_datafusion_config_keys`) regressed; check `cargo test -p repark-core --lib builder_datafusion` first |
| `cross_door` RED on ONE door only | The doors' lowerings drifted — that is the row doing its job (design §6 R3). Compare the two handlers, do not relax the assertion |
| `cross_door_g3e8_refusals_render_identically` RED | The duplicated G3-E8 valve drifted: one door's message text or its target derivation changed without the other's. Fix the copy, never the assertion. |
| `cross_door_identifier_case_folding_*` RED because a quoted wrong-case identifier now RESOLVES | repark has CONVERGED on Apache Spark. Retire `docs/spark-sql-iceberg-parity.md` §3 row ID-1 in the same change (a new dated decision supersedes D3) — never relax the assertion |
| a G11 `cross_door_integer_division_*` / `cross_door_order_by_*` / `cross_door_*_div_by_zero_*` row RED | An intended door split moved. Do not retarget the ANSI half at Spark. |
| `ansi_door_cast_overflow_int_to_tinyint_raises` RED because the CAST wraps or nulls | CAST overflow stopped raising. That is a correctness regression on the ANSI door; do not absorb it into F-Y10-1 (that finding is *arithmetic* wrap, not CAST) |
| `ansi_door_null_keys_never_match_inner_left_semi_anti` RED | 3VL on JOIN keys moved. Do not retarget the ANSI half at Spark. |
| `ansi_door_rows_and_range_frame_values` RED | a ROWS/RANGE frame value moved. Unit-less DATE RANGE `[10,30,60]` is DF-native months; do not "fix" it to Spark's days |
| `ansi_door_sum_f64_bits_at_target_partitions_*` RED | Native-door float-agg bits drifted. Same fixture as G7; do not fudge a bit pattern |
| `extensions_are_session_scoped_not_dialect_scoped` RED | Extension scoping changed. Every `TwoSession` matrix row in BOTH doors needs re-reading before anything else |
| `ansi_door_and_spark_door_agree_under_a_non_utc_session` RED on ONE door | The session timezone stopped being session-scoped (it rides `ConfigOptions`, which every door on the session shares). Check `repark_functions::session_time_zone` and `SparkExtension::configure` before touching the row |
| `a_native_session_without_the_spark_extension_reads_the_stored_zone` RED | A zone-aware extractor leaked into the extension-less profile — check what registers UDFs on a bare session |
| `ta_toll` RED on bit-exactness | Compare against `crates/repark-ta/tests/goldens.rs` first — if THAT is green, the divergence is in the window-UDF wrapper or the door, not the kernel |
| `sql_*_window_agg_exec` RED | DataFusion same-OVER fusion / intervening-filter stacking moved. Do not drop `ema5` from the stacked SELECT (DCE fakes a fused count). |

First checks: `cargo test -p repark-sql --test parser_productions`,
`cargo test -p repark-sql --test session_wiring`, `--test introspection`, `--test ta_toll`,
`--test cross_door`, `--test ansi_door_values`, `--test ansi_door_join_null_keys`,
`--test ansi_door_window_frames`, `--test ansi_door_float_agg`.
Escalate to: [../map.md#debug](../map.md).
