# POLARS-IS-DUPLICATED-1 — `is_duplicated()` on both `F.col` and `rp.col`: answers as Polars, and `.filter(...)` over it keeps every repeated row

**Filed:** 2026-09-28 by the orchestrating session on the owner's ruling of the same day ("lets go ahead and add the .filter(col('somecolumn').is_duplicated()) function added to v1.5.2"). **Target: v1.5.2.** This one expression is pulled ahead of full Polars functions parity (v1.9 on the release roadmap). The rest of that release is unchanged.

## What the user writes

```python
import repark.polars as rp
from repark import functions as F

frame.filter(rp.col("somecolumn").is_duplicated())
df.filter(F.col("somecolumn").is_duplicated())
```

Today both `rp.col` and `F.col` return a repark `Column`, which has no `is_duplicated`, so both calls raise `AttributeError`.

## Polars semantics (measured on polars 1.43.2, 2026-09-28)

`is_duplicated()` is a boolean mask that is true on **every** occurrence of a value that appears more than once, not only on the second and later occurrences. For `a = [1, 2, 2, null, null, 3, 1]` the mask is `[T, T, T, T, T, F, T]`.
- **Nulls:** null equals null, so two nulls are duplicated. A single null is false.
- **Floats:** NaN equals NaN, and `0.0` equals `-0.0`. For `b = [1.0, NaN, NaN, 0.0, -0.0, null, 2.0]` the mask is `[F, T, T, T, T, F, F]`.
- **Strings:** comparison is exact, so `"A"` and `"a"` are not duplicates.
- **Row order:** `.filter` keeps the rows in their original order.

## Scope

- `Column.is_duplicated()` on **both doors** (owner ruling, 2026-09-28: "we're going to extend PySpark so I need our F.col ability to do this as well as the rp.col"):
  - the PySpark door: `F.col` / `df["c"]` / `df.c`, usable in `filter`/`where`, `select` and `withColumn(s)`;
  - the Polars-style door: `repark.polars` / `rp.col`, usable in `filter`, `select` and `with_columns`.
  One Rust implementation serves both. On the PySpark door this is a **documented RePark extension**: PySpark has no such method. List it in the extensions section of the API docs, so that parity reports do not count it as a PySpark difference.
- Composition: `~F.col("c").is_duplicated()` and `&` / `|` with other predicates work in `filter`.
- Both doors give the same answers, because Spark's grouping equality matches the Polars semantics above: null groups with null, NaN with NaN, and -0.0 with 0.0. Measure this on Spark 4.1.2 (`count(*) OVER (PARTITION BY c) > 1`) before relying on it.
- The mask is a whole-frame count per value, like `count(*) OVER (PARTITION BY c) > 1`. Nulls count as one group and NaN as one group. Spark refuses a window function in WHERE, so `filter` must lower to a projected mask column and then a filter, with the helper column dropped. The work happens in Rust (Rust-first ruling), and the Python side only forwards the call.
- Measure first: record Polars' answers for each cell below (INT, BIGINT, DOUBLE with NaN/±0.0/null, STRING with case twins, DATE, and an empty frame) with real polars as the oracle, then pin RePark against them.

## Owner ruling on doors (2026-09-28)

Both doors, as above. This replaces the earlier default of the Polars door only.

## Done condition

Every measured cell matches real polars on both doors through `filter`, `select` and `with_columns`/`withColumn`, including row order. No existing PySpark-door answer changes. A mutation that marks only later occurrences (`row_number() > 1`) turns at least one pin red.

## Out of scope

`is_unique`, `is_first_distinct`, `is_last_distinct` and the multi-column `DataFrame.is_duplicated()`. These stay with the v1.9 Polars functions parity release unless the owner pulls them in.
