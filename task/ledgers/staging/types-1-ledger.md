# Charter ledger — TYPES-1 · the SQL door's Arrow types follow Spark

**Date:** 2026-09-05 · **Branch:** `fix/types-1` · **Base:** `origin/main`
`6eaccd5e` · **Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `V3-COV-8` (width half) BACKLOG → **FIXED**; `BL-8` BACKLOG → **FIXED**;
`G5-RANK-TYPE-1/2/3` BACKLOG → **FIXED**; `UNIX-1` BACKLOG → **FIXED**; `TY-3` re-measured
residue; residues filed honestly, never absorbed.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** Four registry rows pin the same class: the Spark SQL door hands back
DataFusion's integer/unsigned/timestamp types where Spark hands back INT/BIGINT/STRING.
A bare `1` types `Int64` (Spark: INT), so CTAS stores `long` where Spark stores `int`
(`V3-COV-8`, width half); `regr_count`/`approx_distinct` answer `UInt64`, which Spark
reads back from Parquet as `decimal(20,0)` (`BL-8`); `rank()`/`row_number()`/`ntile()`
answer `UInt64` (Spark: INT); `from_unixtime` answers TIMESTAMP (Spark: STRING).

**Not in this unit:** nullability derivation in any form (CUTOVER-SCHEMA-1 settled it on
`main` — `VALUES`/`UNION` nullability residue stays residue); the ANSI door (stock
DataFusion by design); `unix_timestamp` and `to_timestamp` (DATE-FN-1 left them);
public API names (the v1.0 freeze binds); any dependency or lockfile change.

## PROPOSITION LEDGER — TYPES-1 — 2026-09-05

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A bare integer literal in INT range types `Int32` on the Spark door — `SELECT 1`, `VALUES (1)`, CTAS `SELECT 1 AS x`, `df.select(lit(1))`, `withColumn('x', lit(1))` all agree; out of range stays `Int64`; CTAS stores Spark's `int`. | `test_types_1.py` literal section, red on the base; live legs under `REPARK_PARITY_LIVE=1`. | **OPEN** | Base probed 2026-09-05: SQL door `SELECT 1`/`VALUES`/`1+1` are `int64`; facade `lit(1)` is already `int32` (the door the SQL side must join). |
| C-002 | Integer arithmetic follows Spark: INT+INT→INT, INT+BIGINT→BIGINT; overflow errors under ANSI on and wraps under ANSI off, both modes measured on both doors. | `test_types_1.py` arithmetic section + ANSI-mode pins; live legs. | **OPEN** | Base: `i + 1` is `int32` on the SQL door (ExprPlanner), `2147483647 + 1` is `int64` (pure literals skip the overflow rule). |
| C-003 | Count-like aggregates return `BIGINT`/`Int64` on the SQL door: `count(*)`, `count(x)`, `count(DISTINCT x)`, `approx_count_distinct`, `regr_count`, `count_if`; Arrow type AND `collect()` Python type pinned. | `test_types_1.py` aggregate section; `DOOR_RETURNS_UNSIGNED` ratchets to empty. | **OPEN** | Base: `count(*)` already `int64` (DF core returns Int64); `regr_count`/`approx_count_distinct` are `uint64`; SQL-door `count_if` does not resolve (facade has a shim). |
| C-004 | `sum(INT)`/`sum(BIGINT)` → `BIGINT`, `sum(DECIMAL)` → Spark's widened decimal, `bit_length`/`length`/`char_length` → INT, `grouping` → INT — pinned on value, Arrow type, and `collect()` type. | `test_types_1.py` sum/length section. | **OPEN** | Base: `sum(int)` is `int64`, `sum(decimal(10,2))` is `decimal128(20,2)`, `bit_length`/`length`/`char_length`/`grouping` already Spark-equal — pin, do not change. |
| C-005 | `rank()`, `dense_rank()`, `row_number()`, `ntile(n)` → `Int32` on both doors, with and without partitions, and inside a CTAS; `percent_rank`/`cume_dist` → `Float64`. | `test_types_1.py` rank section; window-corpus rank rows flip to equality. | **OPEN** | Base: rank family is `uint64` on the SQL door, `int32` on the facade; `percent_rank`/`cume_dist` already `float64`. |
| C-006 | `from_unixtime` returns session-zone STRING `yyyy-MM-dd HH:mm:ss` with the optional format argument, measured under UTC and a non-UTC session zone; `unix_timestamp`/`to_timestamp` unchanged. | `test_types_1.py` from_unixtime section; UNIX-1 pin flips. | **OPEN** | Base: SQL door returns `timestamp[s]`, facade returns STRING (facade keeps its 1-arg shape; the format argument is new on the SQL door). |
| C-007 | Placement: literal narrowing runs pre-coercion (a `FunctionRewrite`), unsigned casts run post-coercion (an `AnalyzerRule`), both in `repark-functions` and installed by `SparkExtension` only; `EXPLAIN` pins prove the plan carries the casts; no public name changes; nullability untouched. | `test_types_1.py` EXPLAIN pins; ANSI-door control pins stay `int64`/`uint64`-free per stock DataFusion. | **OPEN** | Custom analyzer rules run after `TypeCoercion` (read off DF 54.1 vendored source); coercion upcasts `Int32` into `Exact(Int64)` signatures, so narrowing breaks no signature. |
| C-008 | No regressions: `make verify` green, the full facade suite green with every flipped pin classified (Spark-answer flip with citation, or fixed regression), the cutover battery re-run, mutation score per rule recorded. | The suites; §8. | **OPEN** | Baselines on the unchanged lane 2026-09-05: `make verify` exit 0; facade suite 4765 passed, 208 skipped. |
| C-009 | Docs: every flipped row FIXED with date and unit id, every residue an honest row, crate maps carry the design note and pins line, `STATUS.md` and `briefs/next-sequence.md` untouched. | The gates. | **OPEN** | Rows owned: `V3-COV-8`, `BL-8`, `G5-RANK-TYPE-1/2/3`, `UNIX-1`; neighbours read: `TY-3`, `TY-4`. |

VERDICT: 9 clauses, 0 PROVEN, 9 OPEN, 0 REJECTED.

## 6. What changed

| File | Change |
|---|---|
| `task/ledgers/staging/types-1-ledger.md` | This ledger. |
| `task/ledgers/staging/map.md` | The ledger row (same commit). |

## 7. Design

### 7.1 Where each rule lives

Literals narrow in a `FunctionRewrite` (`datafusion_expr::expr_rewriter::FunctionRewrite`),
which the DF 54.1 analyzer runs before `TypeCoercion` (read off the vendored
`analyzer/mod.rs`: `ApplyFunctionRewrites` chains ahead of every rule). Pre-coercion
placement is what makes `int_col + 1` unify to INT while `CAST(x AS BIGINT) + 1` keeps
its explicit BIGINT — post-coercion the two are indistinguishable. The rewrite mirrors
the facade's `PyColumn::literal` exactly (`i32::try_from`, Int32 on fit, Int64 past it,
`-(2^31)` folds to `Int32::MIN`).

Unsigned results cast in an `AnalyzerRule` after `TypeCoercion` (casts are explicit, so
no re-coercion is needed). It mirrors the facade's `cast_unsigned_count_to_signed`
exactly: probe the UDF return type with `Int64` args, wrap in `CAST AS BIGINT` iff
unsigned. Window UDWFs probe the same way and wrap in `CAST AS INT`. The rule is a
fixpoint under repeated analysis via `transform_down` + `Stop` on already-wrapped casts.

`from_unixtime` is a scalar UDF overwriting DF core's (later registration wins),
reusing the `date_format` Java-pattern compiler and the session-zone carrier.
`count_if` is a one-arg boolean aggregate UDF returning `Int64` (the SQL door lacks
the name entirely; the facade's shim already answers `Int64`).

### 7.2 Signatures surveyed, not assumed

`coerced_from` in `datafusion-expr` 54.1 coerces `Int32` into `Exact(Int64)` (widening
only), so narrowing breaks no `Exact` signature; `date_add` (`Exact(Date32, Int32)`,
no `Int64` arm) is unplannable with an `Int64` literal on the base and becomes
plannable after narrowing. `ntile` accepts every int width; `lead`/`lag` are `Any`;
`length` (datafusion-spark) already returns `Int32`.

## 8. Mutations

Run 2026-09-05 (one rule disabled at a time; every mutation reverted, tree
verified clean after each). Rust-level mutations via
`cargo test -p repark-functions --lib`; the wiring mutation via `make develop`
plus `python/repark/tests/test_types_1.py`.

| Mutation | Disabled rule | Pins reddened | Result |
|---|---|---|---|
| M1 | `narrow_node` try_from arm (Int32→Int64) | `int64_literal_in_range_narrows_to_int32`, `select_one_answers_int32`, `values_one_answers_int32` | 3 red |
| M1b | `fold_negative_int_min` (MIN→MAX) | `negative_two_to_31_folds_to_int32_min` | 1 red |
| M2 | `signed_aggregate_functions` → empty | `unsigned_count_like_answers_int64`, `approx_alias_answers_int64`, `grouped_unsigned_count_like_answers_int64` | 3 red |
| M3 | `signed_window_functions` → empty | `rank_answers_int32_with_values_kept`, `row_number_dense_rank_ntile_answer_int32` | 2 red |
| M4 | from_unixtime `DEFAULT_PATTERN` → `yyyy/MM/dd` | `renders_epoch_in_utc`, `renders_epoch_in_new_york` | 2 red |
| M5 | count_if return type Int64→UInt64 | `counts_true_skips_false_and_null`, `empty_input_answers_zero` | 2 red |
| M6 | wiring: `SparkIntegerLiteral` unregistered from `analyzer_rules()` | 9 × `test_types_1.py` (literal widths, VALUES, CTAS, `1+1`, both overflow modes, EXPLAIN) | 9 red, 45 pass |

Score: 7 mutations, 7 bite, 0 survivors.

## 9. Live oracle

Banner, zone, and per-shape readings recorded here when a JVM slot frees (two sibling
lanes hold one each as of 2026-09-05; this lane polls with `pgrep` and never starts a
second JVM).

## 10. Implementation progress (2026-09-05)

§7's analyzer-prefix placement is superseded: narrowing now runs FIRST in
`repark_functions::analyzer_rules()` (after DataFusion's own `TypeCoercion`, which the
prefix preceded) with a CLOSING `TypeCoercion` at the list end, and the prefix
mechanism is reverted. Cause: narrowing-before-coercion made `TypeCoercion.coerce_union`
wrap narrowed branches in `CAST`s to the stale plan-build union schema, and the
division rewrite then fired inside the cast (`SELECT 5/2 UNION ALL SELECT 7/2` went
`int64 [2, 3]`). Order now: coerce on pre-narrow `Int64`, narrow, rewrite, close.
`SessionState::optimize` re-runs the analyzer, so every execution sees three passes;
the fixpoint is stable. `LIMIT` fetch/skip are exempt (the physical planner matches
bare `Int64` only). Plain-`INSERT` DML gets a post-analysis conform projection for
the narrowing-opened `(Int32 → BIGINT)` shape (`conform_insert_narrowed_ints` in
`spark_ast.rs`); every other shape passes through as before. Fallout fixed in the
same slice: `decimal_precision` default-cast arms (`(20,0)` accepts narrowed `Int32`,
`(10,0)` keeps user casts declared), `try_divide` interval divisor accepts `Int32`,
the three `integer_spark` widening pins rewritten to Spark behavior (`1+1` is `Int32`,
`INTMAX+1` raises under ANSI and wraps when ANSI is off, matching the typed path),
the door-parity ratchet 22 → 21 (`from_unixtime` converged), bindings/cross-door
helpers to `Int32` (cross-door catalog setups neutralized with declared `BIGINT`).
`make verify` green.

Known residue (out of scope, observed not fixed): SQL-text `UNION` of small literals
answers `BIGINT` (the stale plan-build union schema; base behaves the same — Spark
answers `INT`); legacy-mode `INTMAX+1` wraps where Spark answers `NULL` (chartered by
C-002, same as the typed path).

## 11. Facade triage (2026-09-05)

The full facade suite on the TYPES-1 tree reads 76 failed / 4743 passed /
210 skipped (`.facade-types1b.log`). Classification rule applied per failure: a flip
is lawful only when the new answer is the Spark answer, cited to the Spark behavior
the pin names; otherwise the production change reverts. Rulings so far:

| File | Ruling |
|---|---|
| `test_window_parity.py` | Five no-engine tiers (`ntile`, start-vs-end, `naive_row_number`, cache, warn) deleted as stale Spark-2.x-era duplicates of `lead`/`lag`-block-disabled semantics covered by pinned rows below; the dead `TYPE_DISC` lead-in deleted with the converged
ranking disclosures (1481→1422 lines); one overlap row keeps a corrected assertion. Corrected: `ROWS`-bounded `last()` with null-top ordering answers NULL on both engines (Spark: `last(null, true)` skips nulls; all-null peers → NULL), so `_repark_last_all_null_rows` now wraps `F.when(overlap, NULL).otherwise(last)` (+2 lines in `core.py`, 6303→6305). Spark-source change `lead_in_frame.c` documents the same `last` rewrite. |
| `test_session_config_knobs.py` | lawful flips, Spark answers pinned |
| `test_display_styles.py` | lawful flips, Spark answers pinned |
| `test_catalog_flow.py`, `test_explode_rewrite.py`, `test_fnp5_aggregates.py`, `test_iceberg_hygiene.py`, `test_integer_overflow_parity.py`, `test_lrs3_registered_divergences.py`, `test_lrs4_door_domain.py`, `test_maintenance_call.py`, `test_sql_passthrough_parity.py`, `test_time_travel.py`, `test_union_distinct.py`, `_acceptance.py`, `_v3_statement_coverage_*`, `docs/spark-sql-iceberg-parity.md`, `bench/windows/roster.py`, `test_w0_window_bench.py` | triaged, flips classified per file |

`check_lib_py` EXCEPTIONS amendments: `core.py` 6303→6305 (+2 — the
null-top `where()` wrap cannot collapse line-neutral under the 100-char Ruff
ceiling; an INCREASE, owner approval requested at merge), `test_window_parity.py`
1481→1422 (ratchet down). Mirrored in `test_cap_1_source_file_line_cap.py`.
