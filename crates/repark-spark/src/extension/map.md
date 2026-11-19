# map — repark-spark/src/extension

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

File-backed tests for `SparkExtension` (`../extension.rs`): the `configure` hook's
`repark.sql.*` `ConfigExtension` install (including the fail-loud unparsable-value
contract and **V3-2** `allowCreateFormatVersion3`), **the Spark-door `spark.sql.ansi.enabled` carrier** (U5 / Q10=A — default TRUE;
`notabool` fail-louds; ANSI door never calls this hook), **the Spark-door
`parse_float_as_decimal=true` default** (DEC-1 / U2 — bare
floating-point SQL literals infer DECIMAL, matching Spark; ANSI door never calls this hook),
**and its session-timezone carrier install** (this hook is the ONE place
`repark-core`'s resolved zone reaches `repark-functions`' extractors, because it is the only crate
that depends on both and the reverse edge is forbidden), and the `register` hook's
function-registry + analyzer-rule installation + the composed
`repark_ta::TaExtension` (pinned bit-exact against the
`repark_ta` kernel).

## Contents

- `tests.rs` — `#[cfg(test)] mod tests;` in `../extension.rs`. U2 pins:
  `configure_defaults_parse_float_as_decimal` (the option is on) and
  `configure_makes_bare_1_23_decimal128_3_2` (collect path, i128=123). U5 pins:
  `configure_defaults_ansi_enabled_true`, `configure_honors_ansi_enabled_false`,
  `configure_refuses_ansi_notabool`. **SOURCE-URL-REDACT-1-FN (2026-10-06):**
  `configure_masks_a_url_password_in_the_merge_schema_refusal` masks a URL password in
  the session-build refusal. Fold 1 masks every `Configuration` message from
  `configure` (`masking_a_configuration_message_changes_only_the_url_password`,
  `configure_masks_a_url_password_in_the_case_sensitive_refusal`,
  `configure_masks_a_url_password_in_the_ansi_refusal`).
  **Q10:** `configure_defaults_timestamp_type_ltz`,
  `configure_honors_timestamp_type_ntz`, `configure_refuses_invalid_timestamp_type`.
  **FNP-8 (2026-09-07):** `analyzer_configuration_seats_hof_preparation_and_float_stringify_before_type_coercion`
  pins both insertions (JAVA-DOUBLE-STR-1 round 2 seats `spark_float_stringify` pre-coercion;
  the LIKE/CASE/coalesce pins prove the seat) and proves every other default rule remains ordered;
  `analyzer_configuration_refuses_a_missing_type_coercion_rule` pins the loud failure.
  **DECIMAL-CACHE-1 (2026-09-15):** the same contract test now pins a third pre-coercion
  seat, `spark_decimal_precision` immediately before `type_coercion` (name kept so the
  FNP-8 citation still resolves); the facade `* Int32` min-precision pins prove the seat.
  **DECIMAL-CACHE-1 round 2 (2026-09-15):** the filter additionally passes
  `spark_negate_null_decimal` through (first in `analyzer_rules()`, not a pre-coercion
  insert, so the seating contract is unchanged).
  **SQL-LITERAL-TYPING-1 (2026-09-16):** the same contract test now pins a fourth
  pre-coercion seat, `spark_integral_literal` immediately before `type_coercion`
  (name kept); the BL-20 door pins prove the seat.
  **Remediation round 1 (2026-09-16):** the seat stays fourth, immediately
  before `type_coercion` (a first-among-pre-coercion seat was measured and
  reverted with the 24 broken FNP-8 HOF pins as evidence). `register`
  installs the post-coercion rules minus the subsumed late
  `spark_integer_literal`.
  pins: sql-literal-typing-1/L-001, L-002
  **WO INTDIV-1 (2026-09-28):** the same contract test pins a fifth pre-coercion seat,
  `spark_fractional_division` immediately before `higher_order_preparation` (the four
  existing offsets are unchanged), and the filter passes it through.
  **CROSS-JOIN-CONDITION-1 fold 2 (2026-10-08):** the same contract test pins a sixth
  pre-coercion seat, `join_condition_refusals` between `spark_integral_literal` and
  `type_coercion`, and the filter passes it through.
  **Ruling 112 (2026-10-15):** the same contract test names `spark_timestamp_unit_widen`
  among the insertions and asserts its seat before `type_coercion` at the position the
  configured list holds, immediately before `spark_fractional_division`. The existing
  assertions keep their entries in their present order.
  **MAP-STRUCT-KEY-SUBSCRIPT-1 fix round 1 (2026-11-02):** the same contract test pins a
  `spark_map_subscript` seat immediately before `type_coercion`, after `join_condition_refusals`,
  and the filter passes it through.
- `session_catalog.rs` — **CATALOG-1 (2026-09-26):** `apply_default_catalog(session, name)`
  (registry and carrier, a no-op once `USE` pinned the current catalog),
  `with_configured_defaults` (the build carrier from `spark.sql.defaultCatalog`),
  `to_datafusion_error`, and `guard_statement`: when the current catalog does not resolve, one
  token pass refuses a dotted first part equal to it, a `current_catalog()` / `current_database()`
  / `current_schema()` call, a bare `SHOW NAMESPACES|DATABASES|SCHEMAS|TABLES|VIEWS|FUNCTIONS`, or
  a one-part `USE`. Otherwise it returns before tokenizing. A refused catalog name in
  `USE` / `SHOW … IN|FROM` operand position, or as the first part of a two- or three-part
  dotted name, raises its refusal first (skipped entirely when no refusal is stored).
- `decimal_float_widen.rs` (and `decimal_float_widen/tests.rs`) — **DECIMAL-DOUBLE-UNION-COERCION-1 part 1 (2026-11-15):** seat E, `SparkDecimalFloatWiden` (`spark_decimal_float_widen`). On the Spark door it casts a DECIMAL operand beside a FLOAT/DOUBLE to `double` in comparisons, `IN`, `BETWEEN`, CASE, `coalesce` / `nvl` / `greatest` / `least` / `array`, `if` and `nvl2`, `__repark_create_map__` values, `IN (subquery)`, equi-join keys, set-operation inputs and the `INTERSECT` / `EXCEPT DISTINCT` semi or anti join; `nullif` keeps `a`'s type, and a plan with no pair comes back untouched. `tests.rs` (5 tests) runs over a plain DataFusion context; the facade pins are `python/repark/tests/sql/test_decimal_double_coercion_1.py`.
  **DECIMAL-DOUBLE-UNION-COERCION-1 part 2 (2026-11-16):**
  - **The stale outer union (Q3):** `rewrite_union` rebuilds a union whose own inputs no longer mix but whose schema
    still differs from its first input's, so a child seat E already widened is never cast back (left-deep, right-deep
    and mixed `UNION` / `UNION ALL` of three or more branches with the DECIMAL first, in subqueries, CTEs, views and
    MERGE sources).
  - **`DecimalInRanges` (`spark_decimal_in_ranges`)**, an optimizer rule added by `ctx.add_optimizer_rule` in
    `SparkExtension::register`, so it runs at the **end** of each optimizer pass, after `push_down_filter`. Bottom
    up, on every node's expressions (`Filter`, `Projection`, join `ON` and filters, `TableScan.filters`), it rewrites
    `__repark_decimal_to_double__(d) [NOT] IN (<Float64 literals>)` by `decimal_in_form` over
    `decimal_range_of_double` (the K2 preimage, with `-0.0` searched as `+0.0`, NaN and ±Infinity matching nothing,
    and a constant at the column's largest double reaching the top): form 1, a decimal `IN` list, when every range is
    one decimal wide (always for precision 15 or less); form 2, the members plus `d >= lo AND d < hi` per wider
    range joined by `OR`, while those ranges number at most `DECIMAL_IN_RANGE_LIMIT` (32, owner Q1 (a)); otherwise
    the per-row form. A NULL item adds `OR NULL`, an emptied list is `CASE WHEN d IS NULL THEN NULL ELSE FALSE END`,
    and `NOT IN` is `NOT` of the same build. A column or subquery item keeps the per-row form. **Part 2 delta
    (2026-11-16):** `decimal_in_form` stops at the 33rd distinct wide range and returns the per-row form, so a long
    list that ends per-row no longer computes every constant's preimage on each optimizer pass.
  - `tests.rs` adds the three-or-more-branch unions and, in `in_ranges`, the brute force: lengths 1 to 64 at
    `(10,2)`, `(20,6)`, `(38,18)` and `(38,10)`, `IN` and `NOT IN`, against a Rust oracle over K2 row for row, the
    plan shape per form, form 1 for every `(p, s)` with `p <= 15`, and the limit of 32.
  - **The retirement (critic item 9):** `widen_decimal_against_float` is gone from `../spark_literal_typing.rs`; seat E
    answers its four literal shapes (comparison, `IN`, `BETWEEN`, simple `CASE`). The gate pinned them through
    `F.expr` on both doors before and after; no native-door answer moved, so no fallback was kept.
  **DECIMAL-COMMON-TYPE-1 (2026-11-16):** the same single detect walk and rewrite walk now also cast two decimals,
  or an integer and a decimal, to Spark's least common type when `range + scale > 38`, through the shared function
  in `decimal_common_type.rs`: comparisons and join keys (an integer literal keeps its own digits), `BETWEEN` (split
  into its two comparisons), `IN` lists and `IN` subqueries (the integer's form), `CASE` (a simple `CASE` whose pairs
  differ turns searched), `if`, `coalesce`, `nvl`, `nvl2`, `nullif`, `greatest`, `least`, `array`, `map` keys and
  values, `map_concat`, `array_contains`, unions bottom up per binary node with struct fields, and the set-operation
  joins. An `INSERT` whose conform projection no longer matches the table type gets its cast back
  (`conform_insert`). The legacy flag `spark.sql.legacy.decimal.retainFractionDigitsOnTruncate=true` skips the
  decimal rule only. `tests.rs` adds the union rounding order, the `IN` mode and the legacy flag; the facade pins are
  `python/repark/tests/types/test_decimal_common_type_1.py`.
- `decimal_common_type.rs` (and `decimal_common_type/tests.rs`) — **DECIMAL-COMMON-TYPE-1 (2026-11-16):**
  `spark_decimal_common_type` is Spark's `DecimalType.boundedPreferIntegralDigits` over the operand types (integers
  in Spark's forms, an integer literal at its own digits in `Mode::Compare`), `None` unless `range + scale > 38`;
  `spark_decimal_type` answers the type always (seat V). `target_of_types` adds the double class of part 1 and the
  field-by-field `Shape` of structs, arrays and maps; `apply` and `retarget` cast an operand to it. `conform_insert` lives
  here too; the legacy flag's carrier is `repark-functions`' `decimal_legacy.rs`. `tests.rs` pins the 14 pairs, the
  literal digits, and the struct, list and map shapes.


- `../extension.rs` — **STRUCT-COMPARE-BY-POSITION-1 (2026-11-10):** seats
  `struct_operands_by_position` after `spark_map_subscript`, registers `DuplicateFieldOperands`
  in `register`, and re-exports `struct_position` toward the root (`lib.rs` holds at 156).
  `tests.rs` pins the seat index and, by name, the `DuplicateFieldOperands` registration. **DECIMAL-DOUBLE-UNION-COERCION-1 part 1 (2026-11-15):** `configure_analyzer_rules` seats `spark_decimal_float_widen` before `type_coercion`, on the Spark door only; `tests.rs` pins its index.
- `sum_window_tests.rs` — **SUM-DEC-I128WRAP-1 (2026-11-17):** `#[cfg(test)] mod sum_window_tests;` in `../extension.rs`, the K2-R pin on a real Spark-door session: after the analyzer and optimizer, every `sum` arm (`DECIMAL(10,2)`, `DECIMAL(38,0)`, `BIGINT`, `DOUBLE`) and the decimal `try_sum` stay an `AggregateUDF` window whose sliding accumulator retracts, in both ANSI modes (so `sliding_frame_rescan` never re-scans them before `spark_avg_ansi_mode` places them); a `ROWS 3 PRECEDING` frame at p 38 raises `NUMERIC_VALUE_OUT_OF_RANGE` with ANSI on and answers `NULL` for that row only with it off. The decimal accumulators: [../../../repark-functions/src/aggregate/map.md](../../../repark-functions/src/aggregate/map.md).

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| A `repark.sql.*` key silently ignored | `configure` parses the builder conf map — key spelling vs `repark_functions::cardinality` consts |
| `year`/`hour`/`date_trunc` ignore the session timezone | `configure` must install the carrier (`repark_functions::session_time_zone::with_session_time_zone`) from `SessionBuildConf::session_time_zone`. Pinned by `configure_installs_the_resolved_session_time_zone_carrier`; the end-to-end behavior is `../../tests/session_timezone.rs` |
| `ta_ema`/`ta_adx`/… unknown on a Spark-doored session | `register` must reach `TaExtension.register(ctx)`; see [../../../repark-ta/src/extension/map.md](../../../repark-ta/src/extension/map.md) |
| Bare `1.23` is still Float64 on a Spark-doored session | `configure` must call `apply_spark_float_as_decimal`. Pinned by `configure_defaults_parse_float_as_decimal` + `configure_makes_bare_1_23_decimal128_3_2`. The ANSI door is supposed to stay Float64. |

First checks: `cargo test -p repark-spark extension::`. Escalate to: [../map.md#debug](../map.md).
