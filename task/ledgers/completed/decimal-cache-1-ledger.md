# Unit ledger — DECIMAL-CACHE-1 · decimal arithmetic that overflows 38 digits refuses `.eager()` / `.cache()` / `.persist()`

**Unit:** DECIMAL-CACHE-1 · **Date:** 2026-09-15 · **Branch:** `feat/decimal-cache-1` · **Base:** `origin/main`
**Model:** muse-spark-1.3-contributor
**Policy:** [../../../AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** Owner report (Windows 1.4.1 notebook, reproduced on the published 1.4.1 wheel):
`df.withColumns({"new_price": F.col("price") * 5}).eager()` on a `DECIMAL(38,10)` frame raises
`AnalysisException: Error during planning: Mismatch between schema and batches`, while `collect()`
and `show()` succeed. Card
[../../roadmap/mid-term/decimal-cache-1-card-2026-09-15.md](../../roadmap/mid-term/decimal-cache-1-card-2026-09-15.md).
The cache-view step (`repark-core` `session/temp_views.rs`, `materialize_dataframe_as_cache_view`)
pins collected physical batches in a DataFusion `MemTable` under the logical Arrow schema, and
`MemTable::try_new` refuses on any field mismatch — so only materializing actions notice the seam.
Oracle: `/tmp/oc-worker/qd-decimal/oracle-decimal-cells.json` (live PySpark 4.1.2, ANSI on), copied
into the pin file as a fixture.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Not in this unit:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.toml`, `Cargo.lock`,
`pyproject.toml`, `uv.lock`, `python/repark/src/repark/spark/functions*.py`, `dataframe/**`,
`column.py`, `session/**`, `catalog.py`, `types.py`, the SQL parser/planner under
`crates/repark-core/src/sql` and planner, Iceberg float/binary fidelity, nested-field
`withColumns`, post-`toDF` name resolution, `F.lit(Decimal(...))` (all filed as cards, out of scope).

## PROPOSITION LEDGER — DECIMAL-CACHE-1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The seam is measured: for `price * 5`, `price + 1`, `price * price` over a `DECIMAL(38,10)` MemTable, the unanalyzed frame schema field, the analyzed logical field, and the first physical batch's Arrow field are recorded, and the difference is named. | Rust measurement test printing all three fields per query | **PROVEN** | Evidence C-001 table below. `cargo test -p repark-functions --lib decimal_precision::tests::measure_cache_seam_logical_vs_physical_decimal_field -- --nocapture`, 2026-09-15, debug native. |
| C-002 | The physical plan's output field equals the logical field (type, nullability, metadata) for every oracle cell; the fix lands in Rust (`repark-functions` / `repark-core`), narrow, with no new global wrapper and no Python cast. | Rust fix + `make verify`; oracle-cell pins in `test_decimal_cache_1.py` | **PROVEN** | `SparkDecimalPrecision` is now also seated before DataFusion's default `type_coercion` (one insert in `analyzer_rules_with_higher_order_preparation`, beside the two existing pre-coercion seats; the appended seat stays, guarded idempotent). The early seat sees bare integer literals before `TypeCoercion` pre-wraps them (`Int32 -> (10,0)`), so facade `price * 5` min-precisions to `(38,8)`; explicit user casts are `Cast` nodes, never bare literals, so `user_cast...keeps_declared_precision` still holds. New pins in `decimal_precision.rs` run a prod-like assembly (`Analyzer::new().rules` + prep + `analyzer_rules()`): facade `* Int32(5)` -> `(38,8)` `88280000000`, facade `+ Int32(1)` -> `(38,9)`, facade `* price` -> `(38,6)`, SQL `* 5` -> `(38,8)`, explicit `CAST(5 AS DECIMAL(10,0)) * price` -> `(38,6)` (cast preserved), `price * CAST(5 AS DECIMAL(1,0))` -> `(38,8)`; every pin asserts logical == physical (name, type, nullability, metadata) and the value. `cargo test -p repark-functions --lib decimal_precision::`: 26 passed; full `--lib`: 543 passed. `test_decimal_cache_1.py`: 61 passed on the debug native; release-native gates pending. Related flip: TY-10 (`test_facade_decimal_plus_literal_skips_min_precision`) went red `(13,2)` -> `(11,2)` exactly as predicted and was flipped to the Spark equality (name kept). Related collateral: the Spark assembly contract test (`analyzer_configuration_seats_hof_..._before_type_coercion`) pins the exact pre-coercion order, so it went red on the new seat and was extended to pin all three seats (name kept; an archived java-double-str-1 ledger cites it). Release-native gates: `make verify` green (ci + workspace Rust tests, including the repark-spark lib with the extended order contract); full `python/repark/tests`: 7601 passed, 368 skipped (sanctioned tiers), 0 failed; `make check-ledgers` + `make check-map-sync` green. |
| C-003 | A deliberately drifted batch (type promoted, metadata dropped) conforms through the cache-view step before `MemTable::try_new` (same-type pass-through, `cast_with_options` with `safe: false`, logical nullability and metadata). | Rust test in `temp_views` | **PROVEN** | `conform_batches_to_schema` in `temp_views.rs`; pins `drifted_batch_conforms_to_the_logical_field` (`(38,6)` no-meta -> `(38,8)` meta, `882800000 -> 88280000000`) and `same_type_batch_passes_through_untouched` (pointer-identical pass-through). `cargo test -p repark-core --lib session::temp_views`: 4 passed; `session::` cohort 122 passed. |
| C-004 | An uncastable batch refuses with `Error::Analysis` naming both fields, e.g. `cache materialize: column 'n' is decimal(38,10) in the executed batches but decimal(38,6) in the plan schema`. | Rust test asserting the message | **PROVEN** | Pins `uncastable_batch_refuses_naming_both_fields` (`Error::Analysis`, `cache materialize: column 'n' is Utf8 in the executed batches but decimal(38,8) in the plan schema`) and `column_count_mismatch_refuses`. Same runs as C-003. Note: the measured direction on current main is batches-carry-Spark-type vs plan-carries-unanalyzed-type; the card's example had them flipped (1.4.1-vintage, illustrative). |
| C-005 | Every oracle cell is pinned through `.eager()`, `.cache().collect()`, `.persist().collect()` and `collect()` on the facade (`withColumns`) and the SQL door (`spark.sql`), values as `Decimal` strings and types via `df.schema[...]`; plus the original report shape `withColumns({"new_price": F.col("price") * 5}).eager()` on a `DECIMAL(38,10)` frame. | `python/repark/tests/test_decimal_cache_1.py` green on the release native | **PROVEN** | `test_decimal_cache_1.py`: 60 parametrized cells + the report shape. Release native (`maturin develop --release` after the Rust fixes): `test_decimal_cache_1.py` + `test_df_eager_1.py` + `test_eager_budget_1.py` + `test_types_1.py`: 177 passed, 7 skipped (sanctioned live-oracle tier, `REPARK_PARITY_LIVE` unset). Fixture `decimal_cache_1_oracle.json` is a verbatim copy of the live-PySpark oracle. |
| C-006 | Registry row in `docs/spark-sql-iceberg-parity.md` under the decimal section, FIXED with the pin. | Registry diff citing the pin | **PROVEN** | New `### DEC-10` row (refusal + min-precision halves, oracle table, pin cites) and the TY-10 flip to FIXED with the flipped pin. |
| C-007 | Facade `-decimal(p,s)` keeps the child type like Spark and like the SQL door (no `lit(0) - x` retyping through the pre-coercion seat). | `test_decimal_cache_1.py` `-p` cells on both doors + every action | **PROVEN** | R2 evidence below. Bite-proof: the two facade `-p` pins failed pre-fix (`decimal(11,2)` vs `(10,2)`; SQL door already green), green post-fix. Basis: Spark `UnaryMinus` keeps the child type (logic-critic live measurement, PySpark 4.1.2); values cross-checked against the SQL door. `test_decimal_cache_1.py` + `test_types_1.py`: 147 passed, 7 sanctioned live-oracle skips. |
| C-008 | Cache conformance resolves each plan field by name: a reordered equal-arity batch conforms with values under the right names; a missing plan field and a duplicate batch name refuse. | Rust tests in `temp_views` | **PROVEN** | R2 evidence below. Pins `reordered_equal_arity_batch_conforms_by_name`, `missing_name_batch_refuses`, `duplicate_name_batch_refuses`, `extra_batch_column_projects_away` (superset projects, mirroring `resolve_projection`). `cargo test -p repark-core --lib session::temp_views`: 9 passed. |
| C-009 | Both unpinned refusal arms refuse with the named two-field `Error::Analysis`: a conform-time cast overflow names both types plus the cast error text; a non-nullable plan field over an array containing nulls refuses (residual rebuild failures are `Analysis`, never bare engine errors). | Rust tests in `temp_views` | **PROVEN** | R2 evidence below. Pins `overflowing_cast_refuses_with_cast_error` (`decimal(10,2)` 176.56 over `decimal(2,1)` refuses with both types plus the Arrow overflow text) and `null_in_non_nullable_plan_field_refuses`; the `Utf8` pin now asserts the appended cast error text. Same run as C-008. |
| C-010 | One analyzer pass per cache materialize: the analyzed plan is optimized and executed directly, never cloned-and-analyzed a second time. | 200-col eager timing before/after on the release native, median of 5 | **PROVEN** | R2 evidence below. BEFORE (S2-21 bench2, release, 6390ea13): eager 31.1 ms vs toArrow 8.2 ms; AFTER (this round, release, two runs): eager 30.1/30.5 ms vs toArrow 8.4/8.7 ms. Full `python/repark/tests` green on the new path (behavior-identical execution). |
| C-011 | No double batch rebuild under tighten: a batch whose schema already matches returns untouched, so tighten-stamped batches skip the second `try_new`. | Code structure + unchanged end-to-end cache suites | **PROVEN** | R2 evidence below. The skip is behavior-identical by construction (same column `Arc`s, same schema `Arc`); no new pin can bite on it, so the pre-existing declareSorted/eager/cache suites hold it. Same full-suite run as C-010. |
| C-012 | Facade/SQL negated null decimals return a typed null (no `Internal error`): null scalars for decimal/int/bigint/double and null rows in all-null columns, on collect and eager. | `test_decimal_cache_1.py` null cells + null-column test; Rust unit tests beside the rule | **PROVEN** | Round-2 evidence below. Fail-before: only the two decimal null-scalar pins failed pre-fix (facade + SQL door); int/bigint/double scalars and all null-column cases already passed. `SparkNegateNullDecimal` (first in `analyzer_rules()`) folds `Negative` over null decimals to a typed null literal. `test_decimal_cache_1.py` + `test_types_1.py`: 159 passed, 7 sanctioned live-oracle skips; `negate_null` Rust tests: 9 passed. |

## Evidence

### C-001 measurement (2026-09-15, `repark-functions` test ctx: `SessionContext::new` + decimal
planner + `analyzer_rules()`; `v` is a one-row `DECIMAL(38,10)` MemTable, `price = 176.56`)

| Probe | Unanalyzed `frame.schema()` (what `register_collected_memtable` pins) | Analyzed (`analyze_eagerly`, what `df.schema` reports) | Physical (first collected batch) |
|---|---|---|---|
| SQL `price * 5` (Int64 literal) | `Decimal128(38, 8)` nullable | `Decimal128(38, 8)` nullable | `Decimal128(38, 8)` nullable |
| SQL `price + 1` | `Decimal128(38, 9)` nullable | `Decimal128(38, 9)` nullable | `Decimal128(38, 9)` nullable |
| SQL `price * price` | `Decimal128(38, 6)` nullable | `Decimal128(38, 6)` nullable | `Decimal128(38, 6)` nullable |
| Facade `price * Int32(5)` (`binary_expr` + `select`, the `withColumns` shape) | `Decimal128(38, 10)` nullable | `Decimal128(38, 6)` nullable | `Decimal128(38, 6)` nullable |
| Facade `price * Int64(5)` | `Decimal128(38, 10)` nullable | `Decimal128(38, 8)` nullable | `Decimal128(38, 8)` nullable |
| Facade `price + Int32(1)` | `Decimal128(38, 10)` nullable | `Decimal128(38, 9)` nullable | `Decimal128(38, 9)` nullable |

Two findings. First, the refusal: for every facade-built plan the cache view pins the
unanalyzed `(38,10)` schema under analyzed physical batches, so `MemTable::try_new` refuses
with `Mismatch between schema and batches` on `.eager()` / `.cache()` / `.persist()` while
`collect()` succeeds. The SQL door analyzes at plan time, so its three rows agree and it never
refuses. Second, the wrong type: facade `* Int32(5)` analyzes to `(38,6)` where the oracle
demands `(38,8)`. Root cause of the second: DataFusion's default `TypeCoercion` (which runs
before every Spark rule) pre-wraps the Int32 literal as `CAST(5 AS DECIMAL(10,0))` per
`coerce_numeric_type_to_decimal128` (`Int32 | UInt32 -> (10,0)`, verified in
`datafusion-expr-common-54.1.0` `type_coercion/binary.rs`), and `is_default_integer_to_decimal`
no longer recognizes `(10,0) <- Int32`: #386 moved Int32 to the `(20,0)` row to protect the
explicit-cast pin `user_cast_decimal_times_decimal_keeps_declared_precision` (`(21,2)`), filing
the facade remainder as TY-10. Restoring the row is therefore unsafe: a restored row would
min-precision the user's explicit `CAST(5 AS DECIMAL(10,0))` to `(12,2)`, trading one Spark
divergence for another. The fix must run before DataFusion's `TypeCoercion`, where bare
literals are still bare: C-002. The unanalyzed-vs-analyzed pin selection is the C-003/C-004 fix.

### R2 remediation round (2026-09-15)

Scope amendment: the remediation brief explicitly authorizes one edit each in
`crates/repark-python/src/column/display.rs` (`unary_neg`) and
`python/repark/src/repark/spark/column.py` (the `__neg__` docstring, reworded in place at
the exact 1532-line baseline) — the ledger's "Not in this unit" line predates that brief.
No dependency, manifest, workflow, or registry file was touched.

C-007 (L-001): the facade encoded `-col` as `lit(0_i32) - col`, which the new pre-coercion
seat min-precisioned to `(1,0)`, retyping `-decimal(10,2)` to `(11,2)` (and `(38,10)` to
`(38,9)`) where Spark `UnaryMinus` keeps the child type. `unary_neg` now emits
`Expr::Negative` of the child; display/SQL fragments unchanged. Fail-before: the two new
facade `-p` pins failed on the pre-fix native while the SQL-door `-p` pins passed (the
critic's table reproduced as pins). Fixture: two appended `-p` cells with a `basis` key;
the 30 live cells are byte-untouched. Existing `__neg__` pins (int64 exact width, null
preservation, float) stayed green — `Negative` preserves those types too. L-004 rode the
same pin file: `_assert_every_action` now asserts `.cache().collect()` against the cell
`cache_value` (all 32 cells) while eager/persist/collect assert their own value fields.

C-008 (L-002): `conform_batch_to_schema` resolves each plan field by name (case-sensitive,
like `lineage_columns.rs::resolve_projection`), so a reordered equal-arity batch conforms
with values under the right names. A plan field missing from the batch refuses with the
two-field message (`... is missing in the executed batches but ...`); a batch name
occurring twice refuses (`... appears {n} times ...`). Extra batch columns project away
(the old arity-count guard is gone; its fewer-columns direction is now the missing-name
refusal). Case-sensitivity and projection match `resolve_projection`; duplicate refusal is
stricter than its last-wins `HashMap` by explicit brief order.

C-009 (L-003): the cast-failure message now appends the Arrow error text after the two
field types (`... in the plan schema: {error}`); the pre-existing `Utf8` pin asserts the
full text. Overflow arm: `decimal(10,2)` 176.56 over `decimal(2,1)` refuses with both
types plus `Invalid argument error: 176.6 is too large to store in a Decimal128 of
precision 2. Max is 9.9`. Null arm: the null scan runs over the conformed columns before
`try_new`, so a non-nullable plan field over an array containing nulls refuses with the
two-field nullability message even if Arrow ever stops validating; any residual `try_new`
failure is `Error::Analysis` naming both schemas, never a bare engine error.

C-010 (P2-1): `register_collected_memtable` ran `Analyzer::execute_and_check` on a plan
clone and then `execute_stream` analyzed the original again (four `SparkDecimalPrecision`
walks per eager with the dual seat). It now optimizes the analyzed plan
(`optimizer().optimize`, which runs optimizer rules only — analyze lives in
`SessionState::optimize`, not in `Optimizer::optimize`, verified in
`datafusion-54.1.0` `session_state.rs`) and executes that physical plan with the same
`TaskContext::from(&state)` and the same coalescing single-partition `execute_stream`
`DataFrame::execute_stream` uses (verified in `dataframe/mod.rs` + `execution_plan.rs`).
One structural `analyzed.clone()` feeds the by-value optimizer; no second analyzer pass.
Timing (release native, DF shape `spark.range(10).select(id AS c0..c199)`, median of 5,
warmup 1, the exact bench2 construction): BEFORE eager 31.1 ms vs toArrow 8.2 ms
(bench2.json at 6390ea13); AFTER eager 30.1 ms / 30.5 ms (two runs) vs toArrow 8.4 ms /
8.7 ms. The box was shared (load ~14-17, another worker's release build) for the AFTER
runs; the BEFORE box state is unrecorded, so the honest claim is directional (~1 ms on a
plan-dominated shape), not a ratio. The small win is expected: the shape is dominated by
parse/optimizer/physical plus the pre-existing eager post-`count()` that fills
`_eager_shape` (out of scope, named in the S2-21 report) — the removed analyzer stack was
only ever a few ms here, and the 1 M-row produce path never showed it. Every `.eager()` /
`.cache()` / `.persist()` / temp-view pin passes on the new path, so execution is
behavior-identical.

C-011 (P2-2): `conform_batch_to_schema` returns an untouched `batch.clone()` when the
batch schema already equals the target schema, so tighten-stamped batches (which carry
the reminted schema `Arc`) skip the second `try_new`; the common same-schema path drops
from one `try_new` to none. Behavior-identical by construction: same column `Arc`s, same
schema `Arc`, no cast kernel can fire on equal types — so no new unit pin can bite on the
skip, and the declareSorted/eager/cache end-to-end suites (green in the full run) hold it.

P3-1 (declined): no Decimal128 probe was added — the probe would itself walk the plan, so
it trades one tree walk for another, and the review measured decimal-vs-int planning
0.17 ms apart. Revisit only with a measured plan-dominated regression.

P3-2: half-done by construction. The P2-2 skip is the pass-through (no `RecordBatch`
alloc when schemas match). The Vec is still borrowed, not consumed: `clippy::needless_pass_by_value` (a `-D warnings` gate) mandates the borrow, and the overlap it preserves is metadata-only on the common path — transient 2x survives only on the exceptional cast path. Retiring that remainder needs a sanctioned per-site escape with its own review, not a passenger on this round.

### Round-2 finding L-101 (2026-09-15)

DataFusion's scalar `Negative` kernel (`datafusion-common-54.1.0` `scalar/mod.rs`
`arithmetic_negate`) answers null `Int*`/`Float*` scalars but has no null-decimal arms,
so a constant-folded `Decimal128(None,p,s)` fails `Internal error` instead of returning
null. Spark `UnaryMinus` propagates null. Both doors hit it (the SQL door pre-existing:
`SELECT -CAST(NULL AS DECIMAL(10,2))` failed identically). The old `0 - x` encoding
returned null with the wrong type; the L-001 `Expr::Negative` encoding returned the
right type for columns but newly shared this scalar hole with SQL.

Fix: new `SparkNegateNullDecimal` analyzer rule (in `decimal_precision.rs`, tests in the
canonical child `decimal_precision/negate_null_tests.rs` — a new top-level module would
have pushed `lib.rs` past its exact 175-line ceiling, and this file is 990/1000 after),
first in `analyzer_rules()` so it sees the raw facade/SQL shape `Negative(Cast(null-lit))`
before const-folding. It folds `Negative` over a null decimal — a null decimal literal
directly, through `Cast`/`TryCast` to a decimal target, and through nested negatives
(`-(-null)` is null) — to a null literal of that type, preserving literal metadata and
the pre-rewrite display name; recompute runs only when a node rewrote. Valued decimals,
null/valued non-decimals, non-decimal cast targets, and bare untyped `-NULL` pass
through untouched (their kernels already agree with Spark or refuse loudly as before).

Fail-before: the two decimal null-scalar pins failed pre-fix on both doors while the
int/bigint/double scalar pins and every null-column pin passed. Rust pins (9): fold of
cast/literal/try-cast/double-negative/wide-target shapes plus untouched pins for valued
decimal, null int, null string, and non-decimal cast. Verified live: the exact L-101
repro returns `decimal(10,2)` / `[Row(n=None)]` on facade and SQL, collect and eager.

Float scope note: `-(float null)` propagates `None` correctly, but the reported width is
`double` — that label comes from the pre-existing registered LOGICAL-WIDTH-1 divergence
(`arrow_type_key` collapses `Float32`→`double`; `CAST(1.5 AS FLOAT)` reports `double`
with no negation involved), so float carries no C-012 pin rather than pinning the
diverged label against the registry row that already owns it.

### Out of scope observed (not in this unit)

- Literal-only decimal arithmetic can disagree on nullability (analyzed nullable, physical
  non-null): the SQL planner marks `Cast` fields nullable and the optimizer constant-folds
  the literals away. Pre-existing, in both analyzer assemblies, and no oracle cell is
  literal-only (every cell involves the nullable price column). Left as found.
- `F.lit(Decimal(...))` refusal and the other filed cards in the unit card's out-of-scope
  list were not touched.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: decimal-cache-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-001 measured, not assumed — the unanalyzed, analyzed and physical fields were printed per probe on a DECIMAL(38,10) MemTable before any fix (facade Int32 analyzed (38,6); unanalyzed (38,10) under physical batches), and each finding drove its fix. The C-005 report-shape pin reproduces the owner report, which refused on the published wheel; the TY-10 pin and the assembly order pin were observed red after the fix exactly as predicted and flipped with citations intact.
      artifacts: [task/ledgers/staging/decimal-cache-1-ledger.md, python/repark/tests/test_decimal_cache_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: All 30 oracle cells exercised (six input types under * 5, + 1, - 1, * price, * CAST(5 AS DECIMAL(1,0))), including the clamp shapes (38,6), (38,17) and (21,4); the conformance cells cover drifted (38,6)->(38,8), uncastable Utf8 and arity mismatch. R2 adds the two -p cells (both doors, every action) and the reorder/missing/duplicate/superset/overflow/null conformance pins. Round 2 adds four null-scalar cells (decimal/int/bigint/double, both doors, every action) and the all-null-column test (both doors, collect and eager); float is scope-cut per the registered LOGICAL-WIDTH-1 row. Every pin asserts Arrow value AND type per cell.
      artifacts: [python/repark/tests/test_decimal_cache_1.py, python/repark/tests/decimal_cache_1_oracle.json, crates/repark-core/src/session/temp_views.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The uncastable batch refuses with Error::Analysis naming both fields plus the cast error text (message pinned verbatim); the arity guard is replaced by name-matched refusal (missing name, duplicate name, both pinned verbatim) with superset projection; overflow and null-under-non-nullable arms refuse with the two-field message (both pinned verbatim); every fault stays AnalysisException on the facade. Pre-existing refusal posture elsewhere untouched.
      artifacts: [crates/repark-core/src/session/temp_views.rs]
    - id: AT-4
      status: ATTACKED
      evidence: No collateral — full repark-functions lib (543 passed), core session cohort (122), repark-spark lib with the extended order contract, make verify (ci + workspace Rust tests), the release-native trio (177 passed, 7 sanctioned live-oracle skips) and the full python suite (7601 passed, 368 sanctioned skips, 0 failed) all green in-session. R2 so far: the unit pin files on the rebuilt release native (147 passed, 7 sanctioned live-oracle skips) and session::temp_views (9 passed); make verify, the full python suite, check-ledgers and check-map-sync re-run before close. Round 2: make verify, the unit pin files on the rebuilt release native (159 passed, 7 sanctioned skips), negate_null Rust tests (9 passed), check-ledgers, check-ledger-grammar and check-map-sync — all green.
      artifacts: [task/ledgers/staging/decimal-cache-1-ledger.md]
    - id: AT-5
      status: N/A
      justification: No privileged action, no secret, no deserialization surface — plan typing plus Arrow casts over already-collected batches.
    - id: AT-6
      status: ATTACKED
      evidence: Arrow value AND type pinned per oracle cell — values as Decimal strings off collect(), types via df.schema[...] on every action (eager, cache, persist, collect) and both doors; the Rust pins assert logical == physical field (name, type, nullability, metadata) plus the unscaled value.
      artifacts: [python/repark/tests/test_decimal_cache_1.py, crates/repark-functions/src/decimal_precision.rs]
    - id: AT-7
      status: ATTACKED
      evidence: R2 removes one analyzer stack per materialize (four SparkDecimalPrecision walks per eager become two) and the second try_new under tighten / on matching schemas. 200-col eager before/after on the release native (median of 5): 31.1 ms before, 30.1/30.5 ms after (two runs, shared box, load ~14-17); toArrow 8.2 ms before, 8.4/8.7 ms after. P3-1 declined (a probe walks the plan too); P3-2's Vec-borrow remainder kept by the clippy gate, metadata-only overlap on the common path.
      artifacts: [crates/repark-core/src/session/temp_views.rs, task/ledgers/staging/decimal-cache-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: DataFusion 54.1.0 behavior verified against the vendored crate source (Int32|UInt32 -> (10,0) coercion, cast_to no-op on equal types, Analyzer::new rule order, with_analyzer_rules replace semantics); file-size ceilings unmoved, new files under the default ceiling.
      artifacts: [crates/repark-functions/src/decimal_precision.rs, crates/repark-core/src/session/temp_views.rs, python/repark/tests/test_decimal_cache_1.py]
    - id: AT-9
      status: ATTACKED
      evidence: The new refusal message (cache materialize: column ... is ... in the executed batches but ... in the plan schema) is pinned verbatim; R2 extends it with the missing/duplicate/nullable arms and the appended cast error text (all pinned verbatim); all other failure families unchanged.
      artifacts: [crates/repark-core/src/session/temp_views.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first held where red was obtainable — the TY-10 pin and the assembly order pin failed on the changed tree exactly as predicted before their flip; the pre-fix measurement recorded the wrong types in the ledger; every added branch (pass-through, cast, refusal, arity guard) has a named pin, so no dead branch ships. R2 red-first: both facade -p pins failed pre-fix (11,2 vs 10,2) with the SQL door green; the overflow pin was written against a placeholder and pinned only after the real Arrow text was observed; every added R2 branch (reorder, missing, duplicate, superset, overflow, null) has a named pin. The C-011 skip is behavior-identical by construction, so no pin can bite on it — stated, not pinned. Round-2 red-first: only the two decimal null-scalar pins failed pre-fix (both doors); int/bigint/double scalars and null columns already passed, so the fix is proven to bite exactly the L-101 arm.
      artifacts: [python/repark/tests/test_types_1.py, crates/repark-spark/src/extension/tests.rs, task/ledgers/staging/decimal-cache-1-ledger.md, python/repark/tests/test_decimal_cache_1.py]
```
