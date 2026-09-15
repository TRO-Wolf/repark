# DECIMAL-CACHE-1 — decimal arithmetic that overflows 38 digits refuses `.eager()` / `.cache()` / `.persist()`

**Filed:** 2026-09-15 (owner report from a Windows 1.4.1 notebook over an S3 Tables silver table; reproduced on the
published 1.4.1 wheel with a two-row `createDataFrame`). **Severity:** P1 — a silent-shape error on a common finance
schema (`DECIMAL(38,10)` is what Spark's JDBC ingestion writes for unscaled NUMERIC columns) that blocks every
materializing action while `collect()` and `show()` succeed.

## The defect

```python
df = spark.createDataFrame([(1, Decimal("176.56"))], "id LONG, price DECIMAL(38,10)")
df.withColumns({"new_price": F.col("price") * 5}).eager()
# AnalysisException: Error during planning: Mismatch between schema and batches
```

`.collect()` on the same frame answers `Decimal('882.800000')` and `df.schema` names `new_price` as
`decimal(38,6)`. **That type is also wrong.** PySpark 4.1.2 answers `decimal(38,8)` / `882.80000000` on the facade,
the SQL door and through `.cache()` (oracle cell `decimal(38,10) *5`): the integer literal `5` is `DECIMAL(1,0)` under
Spark's min-precision rule, so `(38,10) * (1,0)` overflows to precision 40 and the adjusted scale is
`max(38 - 30, 6) = 8`. RePark's `(38,6)` is the answer for a `DECIMAL(10,0)` literal, so the integer-literal
min-precision half of `SparkDecimalPrecision` is not reaching this plan. `price + 1` → Spark `decimal(38,9)`,
RePark `decimal(38,9)` (agrees); `price * price` → both `decimal(38,6)` (agrees, yet eager still fails). The cache-view step
behind `.eager()` / `.cache()` / `.persist()` (`repark-core` `session/temp_views.rs`,
`materialize_dataframe_as_cache_view`) collects the batches from the physical plan and pins them in a DataFusion
`MemTable` under the DataFrame's logical Arrow schema. `MemTable::try_new` refuses when any batch field differs
from the logical field in name, type, nullability or metadata. So the physical batches for this expression do not
carry the logical `decimal(38,6)` field, and only the materializing actions notice.

Measured on 1.4.1 (`/tmp/oc-worker/qd-decimal/repro10.log` shape): every arithmetic whose Spark result type needs
the precision-overflow adjustment fails — `decimal(38,10) * 5`, `decimal(38,10) + 1`, `decimal(38,10) * price`,
`decimal(38,6) * price`, `decimal(30,10) * 5`, `decimal(28,6) * price`, `decimal(20,10) * price`. Every arithmetic
that stays under 38 digits passes (`decimal(10,2)` in every shape, `decimal(38,6) * 5`, `decimal(28,6) * 5`).
An explicit `.cast("decimal(38,6)")` on the result makes the same plan pass, which is the user workaround.

The Spark rule lives in `crates/repark-functions/src/decimal_precision.rs` (`SparkDecimalPrecision`, an
`AnalyzerRule` that wraps the arithmetic in a `Cast` to Spark's type). Where the physical field diverges from the
logical one — the optimizer unwrapping the cast, the physical `BinaryExpr` recomputing DataFusion's own result type,
a nullability or metadata difference on the cast's output field — is the first thing this unit measures.

## Oracle

`/tmp/oc-worker/qd-decimal/oracle-decimal-cells.json`: live PySpark 4.1.2 cells (ANSI on) for the six input types
above under `* 5`, `+ 1`, `- 1`, `* price`, `* CAST(5 AS DECIMAL(1,0))` on the facade, the SQL door and after
`.cache()` — result type and value for each. RePark must answer every cell byte-for-byte on both doors and through
the cache view.

## Deliverables

1. **Physical parity (Rust, `repark-functions` / `repark-core`):** the physical plan's output field for Spark
   decimal arithmetic equals the logical field — type, nullability, metadata — for every oracle cell. Fix the seam
   where they diverge; do not paper over it with a cast in Python.
2. **Cache-view conformance (Rust, `repark-core` `session/temp_views.rs`):** before `MemTable::try_new`, conform
   each batch to the logical schema the way `crates/repark-iceberg/src/catalog/lineage_columns.rs::conform_batch`
   does (same-type columns pass through, castable columns cast with `safe: false`, field metadata and nullability
   taken from the logical field). When a column cannot be cast, refuse with an `AnalysisException` that prints both
   schemas — the logical field and the batch field that differ — instead of DataFusion's one-line message.
3. **Pins:** `python/repark/tests/test_decimal_cache_1.py` — every oracle cell through `.eager()`, `.cache()`,
   `.persist()` and `collect()` on both doors; a Rust test in `temp_views` for the conformance step with a
   deliberately drifted batch (type promoted, metadata dropped) and one for the uncastable refusal message.
4. **Registry:** a row in `docs/spark-sql-iceberg-parity.md` under the decimal section, FIXED with the pin.

## Out of scope (file as cards, do not fix here)

Found in the same sweep on 1.4.1: Iceberg `float` reads as `double` and `binary` as `string` (type fidelity);
`withColumns({"x": F.col("s.x")})` does not resolve a nested field; a column reference after `toDF("ID", "PRICE")`
resolves the old name; `F.lit(Decimal(...))` is refused where PySpark accepts it.
