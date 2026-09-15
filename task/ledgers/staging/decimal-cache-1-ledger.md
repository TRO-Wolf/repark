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
      evidence: All 30 oracle cells exercised (six input types under * 5, + 1, - 1, * price, * CAST(5 AS DECIMAL(1,0))), including the clamp shapes (38,6), (38,17) and (21,4); the conformance cells cover drifted (38,6)->(38,8), uncastable Utf8 and arity mismatch. Every pin asserts Arrow value AND type per cell.
      artifacts: [python/repark/tests/test_decimal_cache_1.py, python/repark/tests/decimal_cache_1_oracle.json, crates/repark-core/src/session/temp_views.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The uncastable batch refuses with Error::Analysis naming both fields (message pinned verbatim); the arity guard refuses with both counts; both faults stay AnalysisException on the facade. Pre-existing refusal posture elsewhere untouched.
      artifacts: [crates/repark-core/src/session/temp_views.rs]
    - id: AT-4
      status: ATTACKED
      evidence: No collateral — full repark-functions lib (543 passed), core session cohort (122), repark-spark lib with the extended order contract, make verify (ci + workspace Rust tests), the release-native trio (177 passed, 7 sanctioned live-oracle skips) and the full python suite (7601 passed, 368 sanctioned skips, 0 failed) all green in-session.
      artifacts: [task/ledgers/staging/decimal-cache-1-ledger.md]
    - id: AT-5
      status: N/A
      justification: No privileged action, no secret, no deserialization surface — plan typing plus Arrow casts over already-collected batches.
    - id: AT-6
      status: ATTACKED
      evidence: Arrow value AND type pinned per oracle cell — values as Decimal strings off collect(), types via df.schema[...] on every action (eager, cache, persist, collect) and both doors; the Rust pins assert logical == physical field (name, type, nullability, metadata) plus the unscaled value.
      artifacts: [python/repark/tests/test_decimal_cache_1.py, crates/repark-functions/src/decimal_precision.rs]
    - id: AT-7
      status: N/A
      justification: No data-size loop added in the steady state — conformance is a pointer-identical pass-through when schemas agree (pinned), and the added analyzer pass is plan-size work over the materialized plan.
    - id: AT-8
      status: ATTACKED
      evidence: DataFusion 54.1.0 behavior verified against the vendored crate source (Int32|UInt32 -> (10,0) coercion, cast_to no-op on equal types, Analyzer::new rule order, with_analyzer_rules replace semantics); file-size ceilings unmoved, new files under the default ceiling.
      artifacts: [crates/repark-functions/src/decimal_precision.rs, crates/repark-core/src/session/temp_views.rs, python/repark/tests/test_decimal_cache_1.py]
    - id: AT-9
      status: ATTACKED
      evidence: The new refusal message (cache materialize: column ... is ... in the executed batches but ... in the plan schema) is pinned verbatim; all other failure families unchanged.
      artifacts: [crates/repark-core/src/session/temp_views.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first held where red was obtainable — the TY-10 pin and the assembly order pin failed on the changed tree exactly as predicted before their flip; the pre-fix measurement recorded the wrong types in the ledger; every added branch (pass-through, cast, refusal, arity guard) has a named pin, so no dead branch ships.
      artifacts: [python/repark/tests/test_types_1.py, crates/repark-spark/src/extension/tests.rs, task/ledgers/staging/decimal-cache-1-ledger.md]
```
