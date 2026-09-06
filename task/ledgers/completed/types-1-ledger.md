# Charter ledger — TYPES-1 · the SQL door's Arrow types follow Spark

**Date:** 2026-09-05 · **Branch:** `fix/types-1` · **Base:** `origin/main`
`6eaccd5e` · **Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `V3-COV-8` (width half) BACKLOG → **FIXED**; `BL-8` BACKLOG → **FIXED**;
`G5-RANK-TYPE-1/2/3` BACKLOG → **FIXED**; `UNIX-1` BACKLOG → **FIXED**; `TY-3` re-measured
residue; residues filed honestly, never absorbed. Round 4 (2026-09-05): `TY-7`, `TY-8`,
`TY-9`, `TY-10` filed; C-004/C-007 restated; §7.1/§9/§11 corrected (§13).

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
| C-001 | A bare integer literal in INT range types `Int32` on the Spark door — `SELECT 1`, `VALUES (1)`, CTAS `SELECT 1 AS x`, `df.select(lit(1))`, `withColumn('x', lit(1))` all agree; out of range stays `Int64`; CTAS stores Spark's `int`. | `test_types_1.py` literal section, red on the base; live legs under `REPARK_PARITY_LIVE=1`. | **PROVEN** | `test_types_1.py` literal section green (5 widths, VALUES, both df doors, CTAS `int32`); live leg equal on 4.1.2/UTC; UNION-literal width residue filed TY-6. |
| C-002 | Integer arithmetic follows Spark: INT+INT→INT, INT+BIGINT→BIGINT; overflow errors under ANSI on and wraps under ANSI off, both modes measured on both doors. | `test_types_1.py` arithmetic section + ANSI-mode pins; live legs. | **PROVEN** | Widening pins + ANSI-on raise / ANSI-off wrap on both doors green; live legs equal ANSI-on (`i+1`, `b+1`, `1+1`) and ANSI-off (`INTMAX+1` wraps `(int32, False, [-2147483648])` both engines). |
| C-003 | Count-like aggregates return `BIGINT`/`Int64` on the SQL door: `count(*)`, `count(x)`, `count(DISTINCT x)`, `approx_count_distinct`, `regr_count`, `count_if`; Arrow type AND `collect()` Python type pinned. | `test_types_1.py` aggregate section; `DOOR_RETURNS_UNSIGNED` ratchets to empty. | **PROVEN** | Aggregate section green (Arrow type + `collect()` type); `DOOR_RETURNS_UNSIGNED` is empty; live legs full-match except approx/regr nullability, pinned as `(True, False)` under BL-18. |
| C-004 | `sum` over TINYINT/SMALLINT/INT/BIGINT → `BIGINT`, `sum(DECIMAL)` → Spark's widened decimal, `bit_length`/`length`/`char_length` → INT, `grouping` → INT on repark (Spark: TINYINT, grouping-set-only — disclosed TY-8) — pinned on value, Arrow type, and `collect()` type. | `test_types_1.py` sum/length section. | **PROVEN** | Sum/length/grouping pins green on value, Arrow type and `collect()` type; live legs full-match `sum` widths incl. TINYINT/SMALLINT, `sum(decimal)`, `bit_length`, `length`; grouping ROLLUP/SETS values match with the `(int32, int8)` carve-out, plain-GROUP-BY acceptance disclosed (TY-8, §13). |
| C-005 | `rank()`, `dense_rank()`, `row_number()`, `ntile(n)` → `Int32` on both doors, with and without partitions, and inside a CTAS; `percent_rank`/`cume_dist` → `Float64`. | `test_types_1.py` rank section; window-corpus rank rows flip to equality. | **PROVEN** | Rank-family pins green (both doors, partitioned, CTAS `int32`, int cells); live legs full-match all of `rank`/`dense_rank`/`row_number`/`ntile`/`percent_rank`/`cume_dist`. |
| C-006 | `from_unixtime` returns session-zone STRING `yyyy-MM-dd HH:mm:ss` with the optional format argument, measured under UTC and a non-UTC session zone; `unix_timestamp`/`to_timestamp` unchanged. | `test_types_1.py` from_unixtime section; UNIX-1 pin flips. | **PROVEN** | `from_unixtime` pins green (UTC + format + New York, both doors; always nullable like Spark after the live red); `unix_timestamp`/`to_timestamp` unchanged; live legs full-match. |
| C-007 | Placement: literal narrowing is the `AnalyzerRule` `SparkIntegerLiteral`, first in `repark_functions::analyzer_rules()` (after DataFusion's own `TypeCoercion`) with a closing `TypeCoercion`; unsigned→signed answers come from UDF-registration wrappers (`signed_aggregate_functions`, `signed_window_functions`); installed on the Spark door only; `EXPLAIN` pins prove the plan carries the rewrites; no public name changes; nullability untouched. | `test_types_1.py` EXPLAIN pins; ANSI-door control pins stay `int64`/`uint64`-free per stock DataFusion. | **PROVEN** | EXPLAIN pins green (`Int32(1)`, `__repark_spark_int_add__`); ANSI-door control `int64`; placement per §10 (post-coercion narrow + closing coercion, Spark-door install only); no public renames. |
| C-008 | No regressions: `make verify` green, the full facade suite green with every flipped pin classified (Spark-answer flip with citation, or fixed regression), the dbt-adapter suite green (`make py-test-dbt`, the `preflight` gate after the facade suite — added in round 5, §14), the cutover battery re-run, mutation score per rule recorded. | The suites; §8. | **PROVEN** | `make verify` exit 0; facade 4942 passed / 206 skipped (post-fix re-run, zero flips from the fix); parity 574; cutover battery 132 / 99; mutations 7/7 bite (§8); 76 triage flips classified (§11); dbt-adapter suite 59 passed / 1 skipped (cursor `description` int64→int32 flip = the Spark answer per C-001, retyped `ffae551d`). |
| C-009 | Docs: every flipped row FIXED with date and unit id, every residue an honest row, crate maps carry the design note and pins line, `STATUS.md` and `briefs/next-sequence.md` untouched. | The gates. | **PROVEN** | `V3-COV-8`, `BL-8`, `G5-RANK-TYPE-1/2/3`, `UNIX-1` FIXED with date + unit; `TY-3` narrowed; residues `TY-6`, `BL-18` filed; crate maps + test map carry notes; `STATUS.md`, `briefs/next-sequence.md` untouched. |

VERDICT: 9 clauses, 9 PROVEN, 0 OPEN, 0 REJECTED.

## 6. What changed

| Area | Change |
|---|---|
| `crates/repark-functions` | `spark_result_types.rs` (+tests): `SparkIntegerLiteral` narrowing, `SignedAggregate`/`SignedWindow` wrappers; `count_if.rs`, `spark_from_unixtime.rs` new UDFs; `integer_spark`/`decimal_precision`/`datetime`/`timestamp_cast`/`try_invert`/`expr_fn`/`aggregate` conform; `lib.rs` rule wiring. |
| `crates/repark-spark` | `spark_ast.rs` plain-`INSERT` INT→BIGINT conform projection. |
| `crates/repark-python`, `crates/repark-sql` | Door-parity ratchet 22→21, dispatch/nullability conform, cross-door and bindings pins. |
| `python/repark` | `test_types_1.py` (54 pins + 4 live legs); facade-suite triage flips; `core.py` `CAST(__repark_rn AS BIGINT)` in `sample`/`randomSplit` (+2 lines, absorbed back to 6303 in round 4). |
| `python/repark-parity` | Stale no-engine window tiers removed; bench roster/map conform. |
| `docs`, `scripts`, `task` | Registry rows FIXED/filed; `check_lib_py` baselines amended; this ledger. |

## 7. Design

### 7.1 Where each rule lives

Literals narrow in `SparkIntegerLiteral`, an `AnalyzerRule`
(`datafusion::optimizer::AnalyzerRule`) running FIRST in
`repark_functions::analyzer_rules()` — after DataFusion's own `TypeCoercion`, which runs
in DataFusion's analyzer pass ahead of ours — with a CLOSING `TypeCoercion` at the list
end (§10: post-coercion placement keeps `SELECT 5/2 UNION ALL SELECT 7/2` at `int64
[2, 3]`; pre-coercion narrowing let the division rewrite fire inside union CASTs). The
rule walks each plan's expressions bottom-up (`transform_up`, `NamePreserver` keeping
unaliased display names), narrowing `Int64` literals that fit `i32::try_from` to `Int32`
(`-(2^31)` folds to `Int32::MIN`), rebuilding `VALUES` rows, and exempting `LIMIT`
fetch/skip (the physical planner matches bare `Int64` only). It mirrors the facade's
`PyColumn::literal` exactly (Int32 on fit, Int64 past it).

Unsigned results never enter an analyzer rule: `SignedAggregate` wraps `regr_count` and
`approx_distinct` (UDF registration, later wins) and converts the accumulator's `UInt64`
to `Int64` at `evaluate`, refusing values past `i64::MAX`; `SignedWindow` wraps
`rank`/`dense_rank`/`row_number`/`ntile` and answers `Int32` from `field()` plus the
partition evaluators. Both wrappers delegate name, aliases, and signature to the inner
DataFusion kernels, so plans carry no CASTs and no fixpoint argument is needed.

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

Banner (2026-09-05): `BANNER spark=4.1.2 zone=UTC` (`build_spark_engine`,
`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `TZ=UTC`). JVM run beside at most one
sibling; this lane's gateways all exited (verified via `pgrep` after each run).

Shared seed `(i int, b bigint, s string)` × `[(1, 10, 'a'), (2, 20, 'b'),
(3, 30, None)]` behind `types1_probe` on both engines. Shape cells are
`(Arrow type, nullable, values)`.

| Query | repark | Spark 4.1.2 | Standing |
|---|---|---|---|
| `SELECT 1` / `2147483648` / `i+1` / `b+1` / `1+1` | — | — | full match, live leg |
| `count(*)` / `count(s)` / `count(DISTINCT s)` / `count_if` / `sum(i)` | — | — | full match, live leg |
| `sum(decimal(10,2))` | `(decimal128(20, 2), True, [60.00])` | same | full match, live leg |
| `bit_length(s)` / `length(s)` (non-null) | `(int32, True, …)` | same | full match, live leg |
| `rank` / `dense_rank` / `row_number` / `ntile(2)` | `(int32, False, …)` | same | full match, live leg |
| `percent_rank` / `cume_dist` | `(double, False, …)` | same | full match, live leg |
| `from_unixtime(0)` + `'yyyy/MM/dd'` shape | `(string, True, …)` | same | full match, live leg |
| `approx_count_distinct(s)` | `(int64, True, [2])` | `(int64, False, [2])` | type+value match; nullability pinned (BL-18) |
| `regr_count(b, i)` | `(int64, True, [3])` | `(int64, False, [3])` | type+value match; nullability pinned (BL-18) |
| `INTMAX+1` ANSI on | raises `ARITHMETIC_OVERFLOW` | raises `ARITHMETIC_OVERFLOW` | match (repark pin + Spark probe; no live leg) |
| `INTMAX+1` ANSI off | `(int32, False, [-2147483648])` | same | full match, live leg |
| `UNION ALL` of `1`, `2` | `(int64, False, {1, 2})` | `(int32, False, [1, 2])` | residue, filed TY-6 |
| `grouping(i)` under plain `GROUP BY` | `(int32, False, [0, 0, 0])` | raises `UNSUPPORTED_GROUPING_EXPRESSION` | disclosed TY-8 in round 4 (§13); acceptance + INT-vs-TINYINT pinned |

Live gate: `REPARK_PARITY_LIVE=1 pytest test_parity_live.py test_types_1.py` —
177 passed (includes the disclosure co-collection pin and 4 TYPES-1 live legs).

## 10. Implementation progress (2026-09-05)

§7.1's original pre-coercion sketch is superseded: narrowing now runs FIRST in
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
answers `INT`); filed as registry row TY-6. The earlier note claiming legacy-mode
`INTMAX+1` wraps where Spark answers `NULL` was wrong: live-measured 2026-09-05, both
engines answer `('int32', False, [-2147483648])` — no residue there.

## 11. Facade triage (2026-09-05)

The full facade suite on the TYPES-1 tree reads 76 failed / 4743 passed /
210 skipped (`.facade-types1b.log`). Classification rule applied per failure: a flip
is lawful only when the new answer is the Spark answer, cited to the Spark behavior
the pin names; otherwise the production change reverts. Rulings so far:

| File | Ruling |
|---|---|
| `test_window_parity.py` | The dead `TYPE_DISC` lead-in deleted with eight converged `uint64` SQL-door tiers (→ `None`, 1481→1422 lines); no test function deleted, no assertion corrected. `core.py` gains `CAST(__repark_rn AS BIGINT)` in the `sample` and `randomSplit` hash arithmetic — `row_number()` narrowed to Int32, so the LCG product keeps 64-bit arithmetic (+2 lines, 6303→6305; round 4 absorbs them back to 6303, §13). (Round-4 correction, F2: the close-out text's `last()`/`_repark_last_all_null_rows` account, its five deleted tiers, and its `lead_in_frame.c` citation were never in the diff — verified against `d49db25b`.) |
| `test_session_config_knobs.py` | lawful flips, Spark answers pinned |
| `test_display_styles.py` | lawful flips, Spark answers pinned |
| `test_catalog_flow.py`, `test_explode_rewrite.py`, `test_fnp5_aggregates.py`, `test_iceberg_hygiene.py`, `test_integer_overflow_parity.py`, `test_lrs3_registered_divergences.py`, `test_lrs4_door_domain.py`, `test_maintenance_call.py`, `test_sql_passthrough_parity.py`, `test_time_travel.py`, `test_union_distinct.py`, `_acceptance.py`, `_v3_statement_coverage_*`, `docs/spark-sql-iceberg-parity.md`, `bench/windows/roster.py`, `test_w0_window_bench.py` | triaged, flips classified per file |

`check_lib_py` EXCEPTIONS amendments: `core.py` 6303→6305 (+2 — each `CAST` splits one
f-string line, which cannot rejoin under the 100-char Ruff ceiling; an INCREASE, owner
approval requested at merge), `test_window_parity.py` 1481→1422 (ratchet down). Mirrored
in `test_cap_1_source_file_line_cap.py`. Round 4 (F10) absorbs the increase: one import
joined, 6305→6303 in both tables, no approval needed.

## 12. Coverage attestation (close-out, 2026-09-05)

Sequential single-session mode: this attestation is a procedural self-review, not
an independent context. Every category below was attacked with a nameable input
or declared N/A with cause.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: types-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Clauses C-001..C-009 walked one by one against behavior; every clause carries repark pins plus live-oracle legs (C-001..C-006) or named controls (C-007..C-009).
      artifacts: [python/repark/tests/test_types_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: INT_MIN/INT_MAX and out-of-range literals, empty-input count_if, malformed calls (boolean from_unixtime, non-boolean and distinct count_if) all pinned.
      artifacts: [crates/repark-functions/src/spark_result_types/tests.rs, crates/repark-functions/src/count_if.rs, crates/repark-functions/src/spark_from_unixtime.rs, python/repark/tests/test_types_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: ANSI-on overflow raises ARITHMETIC_OVERFLOW on both doors; ANSI-off wraps; UDF misuse refuses at plan time with the function named.
      artifacts: [python/repark/tests/test_types_1.py, crates/repark-functions/src/spark_from_unixtime.rs]
    - id: AT-4
      status: N/A
      justification: Pure analyzer rules and scalar/aggregate/window UDFs; no shared mutable state, no ordering assumption, no concurrency beyond DataFusion's own executor.
    - id: AT-5
      status: N/A
      justification: No privileged action, no credential or secret handling, no deserialization, no path traversal; the diff touches type derivation only.
    - id: AT-6
      status: ATTACKED
      evidence: CTAS commits int32 for literal and rank shapes (V3-COV-8); count-likes answer Int64 so Parquet round-trips avoid decimal(20,0) (BL-8); no public name changes.
      artifacts: [python/repark/tests/test_types_1.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: N/A
      justification: The narrowing pass is one linear tree walk per analysis; wrappers delegate to the inner kernels. No unbounded growth and no system-breaking perf defect; no perf claim is made.
    - id: AT-8
      status: ATTACKED
      evidence: DataFusion 54.1 analyzer-order and coercion behavior read off vendored source, not presumed; rules install on the Spark door only (ANSI-door controls stay stock); Spark error classes matched live.
      artifacts: [python/repark/tests/test_types_1.py, crates/repark-functions/src/lib.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Every failure path names its cause: ARITHMETIC_OVERFLOW, from_unixtime/count_if plan refusals, signed-wrapper internal mismatches. No silent wrong answer on any probed shape.
      artifacts: [crates/repark-functions/src/spark_result_types.rs, python/repark/tests/test_types_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Mutations M1-M6 (7 shots incl. the MIN-fold and the end-to-end wiring kill) all bite, 0 survivors; every added branch has a nameable reddening input; quantified clauses pin per entry point.
      artifacts: [python/repark/tests/test_types_1.py]
  complete: true
```

```yaml
DELIVERY_SIGNOFF:
  pr_unit: types-1
  artifacts_verified:
    ledger: PASS (C-001..C-009 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10)
    findings_ledger: PASS (none open)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS
  status_update: STATUS.md untouched; V3-COV-8, BL-8, G5-RANK-TYPE-1/2/3, UNIX-1 FIXED; TY-3 narrowed; TY-6, BL-18 filed
  verdict: ACCEPTED
  rejection_route: N/A
SHIPPED_FLAG_REGISTER:
  pr_unit: types-1
  flags: []
  count: 0
```

## 13. Round 4 — the critic's eleven findings (2026-09-05)

One JVM at a time (`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `TZ=UTC`); every probe
process exited (verified via `pgrep` after each run). Banner for both round-4 probe runs:
`BANNER spark=4.1.2 zone=UTC`. Shape cells are `(Arrow type, nullable, values)` on the
shared `(i int, b bigint, s string)` seed behind `types1_probe`.

| id | Disposition |
|---|---|
| F1 | The saturation premise is refuted by measurement: Spark WRAPS out-of-range seconds (`i64::MAX` → `-1s`), exactly like the shipped `wrapping_mul`. The one real gap was the `+` Java emits for 5+-digit years (`+51190-09-21`): fixed in the shared Java-pattern year arm (`count >= 4`, digits past width). All eight cells pinned on both doors + live leg. |
| F2 | §11 rewritten to the verified diff: eight converged tiers → `None`, `TYPE_DISC` deleted, no `last()` change anywhere; `core.py` +2 is the two `__repark_rn` BIGINT casts. The triage commit message's `last()` line stays wrong (no amends); this ledger is the corrected record. |
| F3 | C-007, §7.1, BL-8 restated to the built mechanism (narrowing = `AnalyzerRule`, unsigned = UDF-registration wrappers); G5-RANK-TYPE-1's rationale carried the same fiction and is restated with it. |
| F4 | Pins for CASE/COALESCE/IF/array/struct/map/UNION-BIGINT/DECIMAL on both doors (map + IF are SQL-only: no facade spelling exists); live legs full-match except the TY-7 carve-out, IF/map nullability carve-outs (pre-existing DF-derive shapes, CUTOVER-SCHEMA-1 domain, no new rows), and struct/array naming notes (§13.1). |
| F5 | ROLLUP/GROUPING-SETS shape pinned for the type: repark INT vs Spark TINYINT (TY-8 carve-out); plain-GROUP-BY acceptance disclosed (TY-8). Refusal/type-fix judged not cheap-safe: the INT comes from DataFusion's `ResolveGroupingFunction` expansion and multi-arg arity is lost after expansion. |
| F6 | Three citations repointed to `types-1/C-002`; supersession noted here: archived `2026-08-31-f-y10-1-int-overflow` C-002 declares untyped arithmetic the intended Int64 split — TYPES-1 narrowed it to Int32. |
| F7 | `sum` over TINYINT/SMALLINT pinned on both doors + live; `ntile(BIGINT)` acceptance disclosed (TY-9); NULL/negative/fractional `from_unixtime` cells pinned under F1; `EEE` + `dd/MM/yyyy HH:mm` pinned on both doors + live. |
| F8 | `polars.py` `row_number` cast to BIGINT (index back to `int64`); pinned with offset values. |
| F9 | Premise stale: no duplicate CREATE line exists in the tree (lines 99–100 are NAMESPACE + TABLE), and repark RAISES on re-create (`AnalysisException: ... already exists`). Probed, no change, no row. |
| F10 | Absorbed: one import joined, `core.py` 6305→6303 in both cap tables; maps corrected to the real change. |
| F11 | Verified: `crates/repark-functions/map.md` already carries the full rule order (`SparkIntegerLiteral` first … closing `TypeCoercion`); the `///` line left verbatim, no change. |

### 13.1 Round-4 oracle cells

Full-match rows carry live legs; carve-outs pin the differing pair.

| Query | repark | Spark 4.1.2 | Standing |
|---|---|---|---|
| `from_unixtime` of ±15e12, 20e12, `i64::MAX/MIN`, `-1`, `1.5`, NULL | `(string, True, …)` | same, incl. `+51190`/`+111192` signed years | full match, live leg |
| `CASE WHEN i > 1 THEN 1 ELSE 0` | `(int32, False, [0, 1, 1])` | same | full match, live leg |
| `COALESCE(NULL::BIGINT, 1)` | `(int64, False, [1])` | same | full match, live leg |
| `COALESCE(NULL::INT, 1)` | `(int64, False, [1])` | `(int32, False, [1])` | residue TY-7, carve-out |
| `IF(i > 1, 1, 0)` | `(int32, True, [0, 1, 1])` | `(int32, False, [0, 1, 1])` | nullability carve-out, no row |
| `array(1, 2)` | element `int32`, `[[1, 2]]` | same (element non-null) | element+value match, live leg |
| `struct(1, 'a')` | `struct<c0, c1>` int32/string | `struct<col1, col2>` int32/string | field types + ordered values match, live leg; names follow DF |
| `map(1, 'a')` (SQL) | `(map<int32,string>, True, …)` | `(map<int32,string>, False, …)` | nullability carve-out, no row; facade has no `create_map` |
| `1 UNION ALL 2::BIGINT` ordered | `(int64, False, [1, 2])` | same, both doors | full match, live leg |
| `decimal(10,2) + 1` (SQL, both orders) | `(decimal128(11, 2), True, …)` | same | full match, live leg |
| facade `decimal(10,2) + lit(1)` | `(decimal128(13, 2), True, …)` | `(decimal128(11, 2), True, …)` | residue TY-10 |
| `sum(tinyint)` / `sum(smallint)` | `(int64, True, [6])` | same, both doors | full match, live leg |
| `ntile(2::BIGINT)` (SQL; facades take `int`) | `(int32, False, [1, 1, 2])` | raises `DATATYPE_MISMATCH` | disclosed TY-9 |
| `grouping(i)` ROLLUP/SETS ordered | `(int32, False, [0, 0, 0, 1])` | `(int8, False, [0, 0, 0, 1])` | carve-out TY-8 |
| `grouping(i)` plain `GROUP BY` / 2-arg | accepted `(int32, …)` | raises `UNSUPPORTED_…` / `WRONG_NUM_ARGS` | disclosed TY-8 |
| `from_unixtime(0, 'EEE' / 'dd/MM/yyyy HH:mm')` | `Thu` / `01/01/1970 00:00` | same, both doors | full match, live leg |
| `from_unixtime('1970-01-02')` | raises (optimizer error) | raises `CAST_INVALID_INPUT` | observed, no pin |
| `with_row_index()` | `(int64, False, [0, 1])` | no Spark equivalent | swept + pinned |

### 13.2 Size-gate notes (round 4)

`datetime.rs` grows 1704→1709 (+5, the year-sign arm) — an INCREASE, owner approval
requested at merge. Absorption was attempted first and proven impossible: the file is
rustfmt-packed (mechanical scan for joinable assignments and paren triplets found zero;
match/if arms are already minimal), and the arm is the minimal readable general form (a
shorter `10^count`-threshold spelling trades the increase for unstatable range reasoning).
Moving the arm out of the pattern renderer would scatter a cohesive unit across modules.
Both cap tables carry 1709. (superseded by §14/N4 — 1700 via `spark_year_pad.rs`)

### 13.3 Merge-triage (gate-driven, outside the eleven)

The round-4 gate run surfaced three red pins in `test_perf_agg_avg_1.py` (landed on
`main` after the TYPES-1 triage, merged in at `07e14435`): VALUES-literal group keys
pinned `int64` where the narrowed door answers `int32` — the Spark answer per C-001's
live legs. Lawful flips with `types-1/C-001` citations under the §11 classification
rule. Pre-existing at the round-4 base; untouched by the eleven findings.

## 14. Round 5 — the executing critic's four findings (2026-09-05)

One JVM at a time (`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `TZ=UTC`); every probe
process exited (verified via `pgrep` after each run; the crashed first spread probe
left one gateway behind — killed by PID, verified gone). Banner for all round-5 probe
runs: `BANNER spark=4.1.2 zone=UTC`. The critic confirmed the round-3 wrapping claim
cell for cell; the round-3 saturation claim was wrong. Shape cells are
`(Arrow type, nullable, values)`.

| id | Disposition |
|---|---|
| N1 | The year arm now pads digits and re-attaches the sign (`-0499`, `-0002`), and `yy` is `abs(year) % 100` (`-499` → `99` — the shipped `rem_euclid` answered `01`, caught by the round-5 unit test against the measured oracle); 5+-digit positives keep `+`. Four UTC cells (default, `yyyy`, `yy`) pinned on both doors plus a New York cell; live legs on the UTC default/`yyyy`/`yy` cells. Bite: the new pins fail 5× on the pre-fix native module; the Rust wrap test fails on the stashed old arm (`-499-…` vs `-0499-…`). |
| N2 | §6 and the dataframe `map.md` rewritten to the real change (`CAST(__repark_rn AS BIGINT)` in `sample`/`randomSplit`), as §11 already reads. |
| N3 | C-008's gate roster and evidence carry `make py-test-dbt`: the cursor `description` flip int64→int32 is the Spark answer per C-001, retyped in `ffae551d`. |
| N4 | `datetime.rs` 1709→1700 via the `spark_year_pad.rs` extraction (§13.2's scatter objection superseded by the brief's sanctioned helper-module out); both cap tables mirrored in the fix commit. |

### 14.1 Round-5 oracle cells

Measured ANSI on/off × UTC/America/New_York; `from_unixtime` is ANSI-independent and
zone-dependent only in wall-clock fields. Full-match rows carry live legs.

| Query | repark | Spark 4.1.2 | Standing |
|---|---|---|---|
| `from_unixtime(-77900000000)` (UTC) | `(string, True, ['-0499-06-13 15:06:40'])` | same | full match, live leg |
| `from_unixtime(-77900000000, 'yyyy' / 'yy')` | `'-0499'` / `'99'` | same | full match, live legs |
| `from_unixtime(-62200000000)` (UTC) | `(string, True, ['-0002-12-17 14:13:20'])` | same | full match, live leg |
| `from_unixtime(-62200000000, 'yyyy' / 'yy')` | `'-0002'` / `'02'` | same | full match, live legs |
| `from_unixtime(-77900000000)` (New York) | `'-0499-06-13 10:10:38'` | same | full match, always-run pin |
| `from_unixtime(-62200000000)` (New York) | `'-0002-12-17 09:17:18'` | same | full match, oracle only |
| `'yy'` spread, 27 years −2500…51190 | `abs(year) % 100`, zero-padded | same | rule fit; pinned cells carry live legs |
