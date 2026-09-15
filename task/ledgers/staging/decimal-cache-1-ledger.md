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
| C-001 | The seam is measured: for `SELECT price * 5`, `price + 1`, `price * price` over a `DECIMAL(38,10)` MemTable, the logical DFSchema field for the output and the first physical batch's Arrow field are recorded, and the difference is named. | Rust measurement test in `temp_views` (or scratch example) printing both fields per query | OPEN | — |
| C-002 | The physical plan's output field equals the logical field (type, nullability, metadata) for every oracle cell; the fix lands in Rust (`repark-functions` / `repark-core`), narrow, with no new global wrapper and no Python cast. | Rust fix + `make verify`; oracle-cell pins in `test_decimal_cache_1.py` | OPEN | — |
| C-003 | A deliberately drifted batch (type promoted, metadata dropped) conforms through the cache-view step before `MemTable::try_new` (same-type pass-through, `cast_with_options` with `safe: false`, logical nullability and metadata). | Rust test in `temp_views` | OPEN | — |
| C-004 | An uncastable batch refuses with `Error::Analysis` naming both fields, e.g. `cache materialize: column 'n' is decimal(38,10) in the executed batches but decimal(38,6) in the plan schema`. | Rust test asserting the message | OPEN | — |
| C-005 | Every oracle cell is pinned through `.eager()`, `.cache().collect()`, `.persist().collect()` and `collect()` on the facade (`withColumns`) and the SQL door (`spark.sql`), values as `Decimal` strings and types via `df.schema[...]`; plus the original report shape `withColumns({"new_price": F.col("price") * 5}).eager()` on a `DECIMAL(38,10)` frame. | `python/repark/tests/test_decimal_cache_1.py` green on the release native | OPEN | — |
| C-006 | Registry row in `docs/spark-sql-iceberg-parity.md` under the decimal section, FIXED with the pin. | Registry diff citing the pin | OPEN | — |

## Evidence

### C-001 measurement

(to be recorded)
