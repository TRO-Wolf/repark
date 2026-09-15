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
| C-002 | The physical plan's output field equals the logical field (type, nullability, metadata) for every oracle cell; the fix lands in Rust (`repark-functions` / `repark-core`), narrow, with no new global wrapper and no Python cast. | Rust fix + `make verify`; oracle-cell pins in `test_decimal_cache_1.py` | OPEN | — |
| C-003 | A deliberately drifted batch (type promoted, metadata dropped) conforms through the cache-view step before `MemTable::try_new` (same-type pass-through, `cast_with_options` with `safe: false`, logical nullability and metadata). | Rust test in `temp_views` | OPEN | — |
| C-004 | An uncastable batch refuses with `Error::Analysis` naming both fields, e.g. `cache materialize: column 'n' is decimal(38,10) in the executed batches but decimal(38,6) in the plan schema`. | Rust test asserting the message | OPEN | — |
| C-005 | Every oracle cell is pinned through `.eager()`, `.cache().collect()`, `.persist().collect()` and `collect()` on the facade (`withColumns`) and the SQL door (`spark.sql`), values as `Decimal` strings and types via `df.schema[...]`; plus the original report shape `withColumns({"new_price": F.col("price") * 5}).eager()` on a `DECIMAL(38,10)` frame. | `python/repark/tests/test_decimal_cache_1.py` green on the release native | OPEN | — |
| C-006 | Registry row in `docs/spark-sql-iceberg-parity.md` under the decimal section, FIXED with the pin. | Registry diff citing the pin | OPEN | — |

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
