# DECIMAL-COMMON-TYPE-1 — step 0 domain table

Measured 2026-11-16 (about 21:20–21:28 UTC) on main `e50ed17f`'s package against Spark 4.1.2, ANSI off and on.
Every number below is produced by [evidence/probes/analyze.py](evidence/probes/analyze.py) and
[evidence/probes/grids.py](evidence/probes/grids.py) from the raw cells in [evidence/raw/](evidence/raw/map.md);
provenance and commands are in [evidence/map.md](evidence/map.md).

## The data

One table of 20 columns, written to Parquet by each engine from string literals (no decimal common type is
involved in the setup) and read back, split in two:

- **`t`, rows 1–6:** halves, negative halves, values that round once and twice (`1.45`, `0.49`, `0.5…01`), NULL.
- **`tx`, rows 7–8:** values at the full 38 digits and the type's edge (`DECIMAL(38,0)` ±(10³⁸−1),
  `BIGINT` ±2⁶³, `INT` ±2³¹, `SMALLINT`, `TINYINT` edges, `DECIMAL(38,38)` ±0.99…9).

The 14 type pairs (`x` beside `y`), with Spark's common type from `boundedPreferIntegralDigits`:

| pair | x | y | family | Spark | DataFusion (main) |
|---|---|---|---|---|---|
| `ab` | `DECIMAL(38,0)` | `DECIMAL(38,38)` | past 38 | `decimal(38,0)` | `decimal(38,38)` |
| `ac` | `DECIMAL(38,0)` | `DECIMAL(10,2)` | past 38 | `decimal(38,0)` | `decimal(38,2)` |
| `ad` | `DECIMAL(38,0)` | `DECIMAL(38,1)` | past 38 | `decimal(38,0)` | `decimal(38,1)` |
| `bc` | `DECIMAL(38,38)` | `DECIMAL(10,2)` | past 38 | `decimal(38,30)` | `decimal(38,38)` |
| `ef` | `DECIMAL(20,0)` | `DECIMAL(20,20)` | past 38 | `decimal(38,18)` | `decimal(38,20)` |
| `gh` | `DECIMAL(38,10)` | `DECIMAL(38,20)` | past 38 | `decimal(38,10)` | `decimal(38,20)` |
| `i64h` | `BIGINT` | `DECIMAL(38,20)` | past 38, integer | `decimal(38,18)` | `decimal(38,20)` |
| `i32w30` | `INT` | `DECIMAL(38,30)` | past 38, integer | `decimal(38,28)` | `decimal(38,30)` |
| `i16w34` | `SMALLINT` | `DECIMAL(38,34)` | past 38, integer | `decimal(38,33)` | `decimal(38,34)` |
| `i8w36` | `TINYINT` | `DECIMAL(38,36)` | past 38, integer | `decimal(38,35)` | `decimal(38,36)` |
| `c0c` | `DECIMAL(10,0)` | `DECIMAL(10,2)` | control | `decimal(12,2)` | `decimal(12,2)` |
| `i32c` | `INT` | `DECIMAL(10,2)` | control, integer | `decimal(12,2)` | `decimal(12,2)` |
| `i64c` | `BIGINT` | `DECIMAL(10,2)` | control, integer | `decimal(22,2)` | `decimal(22,2)` |
| `k28k4` | `DECIMAL(28,2)` | `DECIMAL(10,4)` | control | `decimal(30,4)` | `decimal(30,4)` |

The Spark column of this table is measured, not only computed: every pair's `union_all` cell returns that type.

## Verdicts

Each RePark cell is paired with Spark's cell of the same key and ANSI mode:

- `agree` — same rows and types (a value that differs only in PySpark's display of a tiny decimal, `5.0E-20`
  against `5E-20`, with equal types, counts as agree; it is the oracle's Python conversion, not a stored value);
- `wrong_answer` (W), `wrong_type` (T, equal values, other type), `wrong_write` (S, the stored rows differ);
- `over_raise` (R, RePark raises where Spark answers), `missed_raise` (M), `both_raise` (B).

## Headline

**Totals:** 1,248 RePark Spark-door cells per ANSI mode (Spark-door SQL, the DataFrame API, `F.expr`, Iceberg
stores and DML, the legacy flag) and 907 native-door cells, each paired with Spark's cell in both modes.

**The common-type cells** (the cause this unit owns; every cell below except the causes of
[Other causes](#other-causes-measured-beside-the-family)):

| door | ANSI | cells | agree | wrong answer | wrong type | wrong write | over-raise | both raise |
|---|---|---|---|---|---|---|---|---|
| Spark door | off | 874 | 49 | 356 | 7 | 38 | 424 | 0 |
| Spark door | on | 874 | 49 | 356 | 7 | 38 | 418 | 6 |
| native `repark.sql()` | off | 655 | 43 | 258 | 7 | — | 347 | 0 |
| native `repark.sql()` | on | 655 | 43 | 258 | 7 | — | 341 | 6 |

- **Revised after the critic** ([§After the critic](#after-the-critic-revision-2026-11-16)): with the `EXCEPT`/`INTERSECT`
  membership cells counted in the family and 52 new cells, the Spark door has **946 common-type cells per mode, 890
  wrong with ANSI off** (402 wrong answers, 9 wrong types, 38 wrong writes, 439 refusals, 2 missed refusals) and 884
  wrong plus 8 both-raise with ANSI on; the native door 632 of 675. The rest of this section is the first count.
- **825 of 874 Spark-door cells are wrong in each mode** (819 plus 6 that both engines raise with ANSI on). The 49
  that agree are rows whose values cannot tell the two types apart (`lt` on `ad` and `ef`), the three integer
  literal controls (`h = 1`, `w30 = 1`, `c = CAST(1 AS BIGINT)`), `v_control`, and the constant cells over `tx` whose
  column is the side that keeps its digits.
- **Every control cell agrees** in the core positions (161 of 161 per mode on the Spark door); the controls'
  disagreements are the other causes below.
- **ANSI changes no common-type verdict** except where Spark itself raises: the overflow literals
  (`CAST('1.5' AS DECIMAL(38,38))`, a 39-digit literal) and `a + c` on the `tx` edge rows.

### The position grid, table `t`, Spark-door SQL

ANSI on is identical cell for cell. Blank: not measured for that pair (the core ten positions run on every pair;
the full list on `ab`, `ac`, `gh`, `i64h`, `i32w30` and the control `c0c`).

| position | ab | ac | ad | bc | ef | gh | i64h | i32w30 | i16w34 | i8w36 | c0c | i32c | i64c | k28k4 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `eq` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `eq_rev` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `ne` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `lt` | R | W | = | R | = | W | W | W | W | W | = | = | = | = |
| `le` | R | W |  |  |  | W | = | W |  |  | = |  |  |  |
| `gt` | R | W |  |  |  | W | = | W |  |  | = |  |  |  |
| `nseq` (`<=>`) | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `between` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `between_rev` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `in1` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `in_rev` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `not_in` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `case` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `case_simple` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `if` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `coalesce` | W | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `nvl` | W | W |  |  |  | W | W | W |  |  | T |  |  |  |
| `nvl2` | R | T |  |  |  | T | T | T |  |  | = |  |  |  |
| `nullif` | R | W |  |  |  | W | W | W |  |  | T |  |  |  |
| `greatest` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `least` | R | W |  |  |  | W | T | W |  |  | = |  |  |  |
| `array` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `map` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `array_contains` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `array_union` | B | M |  |  |  | M | M | M |  |  | M |  |  |  |
| `map_concat` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `lag` default | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `lead` default | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `union_all` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `union_all_rev` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `union` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `union3` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `except` | R | W |  |  |  | W | W | W |  |  | T |  |  |  |
| `intersect` | R | W |  |  |  | W | W | W |  |  | T |  |  |  |
| `where_eq` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `where_eq_rev` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `where_lt` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `where_in` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `where_between` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `join_eq` | R | W | W | R | W | W | W | W | W | W | = | = | = | = |
| `join_nseq` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `join_full_using` | R | R |  |  |  | R | R | R |  |  | R |  |  |  |
| `in_subquery` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `not_in_subquery` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |
| `scalar_subquery` | R | W |  |  |  | W | = | W |  |  | = |  |  |  |
| `exists_corr` | R | W |  |  |  | W | W | W |  |  | = |  |  |  |

- **`ab` and `bc` raise everywhere:** DataFusion picks `decimal(38,38)`, which cannot hold `1` or `1.25`
  (`Arrow error: … is too large to store in a Decimal128 of precision 38`). `coalesce(b, a)` answers only
  because no `b` row is NULL beside a non-NULL `a`.
- **Every other past-38 pair answers the exact comparison**, so Spark's rounded comparison differs: `a = c` is
  true for (1, 1.25), (2, 1.50), (−2, −1.50), (1, 0.50), (0, 0.49) on Spark and false on main; `i64 = h` is true for
  (1, 1.00000000000000000049) on Spark only.
- **The `tx` grid** ([evidence/raw/grids.md](evidence/raw/grids.md)) raises on main at every position of every
  past-38 pair except `coalesce`, `nvl` and the `CASE`/`if` rows whose chosen branch fits: a `BIGINT` near 2⁶³ cast
  to `decimal(38,20)`, an `INT` near 2³¹ cast to `decimal(38,30)`, a 38-digit `DECIMAL(38,0)` cast to
  `decimal(38,2)`. Spark answers each (its common type keeps every integer digit).

### Constants, both directions (Spark-door SQL, `WHERE` and the select list, `t` and `tx`)

| cell | Spark `WHERE` ids | main | note |
|---|---|---|---|
| `a = CAST('1.25' AS DECIMAL(10,2))` and its mirror | 1, 4 | none | the constant rounds to 1; `tx` raises on main |
| `c = CAST('1' AS DECIMAL(38,0))` and its mirror | 1, 4, 6 | none | **the column loses digits**: #1345's case |
| `c < CAST('1' AS DECIMAL(38,0))` | 3, 5 | 3, 4, 5 | 0.50 rounds to 1 |
| `c >= …`, `c <=> …`, `c IN (1, 2)`, `c NOT IN (1, 2)`, `c BETWEEN 1 AND 1`, `1 BETWEEN c AND c` | Spark's rounded set | exact set | all wrong |
| `a IN (CAST('1.25' AS …(10,2)), CAST('2.5' AS …))` | 1, 4 | none | |
| `a = 1.25`, `1.25 = a`, `a = 1.5`, `a > 0.5`, `a IN (1.25, 2.5)` | 1 4 / 1 4 / 2 / 2 / 1 4 | none / none / none / 1 2 4 / none | **a plain literal beside a `DECIMAL(38,0)` column**; `tx` raises on main |
| `d = 1.45` | 1 | none | `(38,1)` beside `(3,2)` is `(38,1)`: 1.45 rounds to 1.5 |
| `b = CAST('1' AS DECIMAL(38,0))`, `a = CAST('0.5' AS DECIMAL(38,38))` | 1, 4 | raises | |
| `h = CAST(1 AS BIGINT)` | 5 | none | `(20,0)` beside `(38,20)` is `(38,18)` |
| `i64 = CAST('1.00000000000000000049' AS DECIMAL(38,20))`, `i32 = CAST('1.0…049' AS DECIMAL(38,30))` | 1, 5 | none | |
| `gg = CAST('1.00000000005' AS DECIMAL(38,20))`, `h = CAST('1.0000000001' AS DECIMAL(38,10))` | 1 | none | |
| `h = 1`, `w30 = 1`, `c = CAST(1 AS BIGINT)` | none | none | **agree**: Spark types an integer *literal* in a comparison at its own precision (`decimal(1,0)`), not `INT`'s `decimal(10,0)`; `w30 = 1` would round on Spark otherwise |

The DataFrame and `F.expr` constant cells (`col("c") == lit("1").cast("decimal(38,0)")` both ways, `isin`,
`between`, `col("a") == Decimal("1.25")`, `isin(Decimal("1.25"), Decimal("2.5"))`, `F.expr("a = 1.25")`) answer as
the SQL cells: 10 wrong per mode on `t`; on `tx` six agree (the column-loses-digits cells, and `F.expr("a = 1.25")`,
which finds no row on both engines where the SQL spelling raises on main) and four raise.

### Three or more types (Spark-door SQL)

Spark rounds **once per binary node, bottom up**, so the answer depends on the shape:

| cell | Spark | main |
|---|---|---|
| `c UNION ALL d UNION ALL a` (flat), `(c ∪ d) ∪ a`, `a ∪ (c ∪ d)`, a subquery of `c ∪ d` | `decimal(38,0)`; 0.49 → **1**, 1.45 → **2** (through `(38,1)`: 0.5, 1.5) | `decimal(38,2)`, exact |
| `a UNION ALL c UNION ALL d` ([side probe](evidence/raw/side-spark-false.jsonl)) | 0.49 → 0, 1.45 → 1 (the inner node is `(38,0)`) | `decimal(38,2)`, exact |
| `df.union(df).union(df)` over c, d, a | as the flat SQL union: double rounding | exact |
| `coalesce(c, d, a)` | 0.49 → 0, 1.45 → 1 (one rounding) | `decimal(38,2)`, exact |
| `coalesce(coalesce(c, d), a)` | 0.49 → **1**, 1.45 → **2** | exact |
| `greatest`, `least`, `array`, `CASE`, `c IN (d, a)` over three | one rounding to `decimal(38,0)` | exact, `decimal(38,2)`; `IN` false where Spark is true |
| `i64 ∪ h ∪ c` | `decimal(38,18)` | `decimal(38,20)` |

All 24 cells (12 on `t`, 12 on `tx`) are wrong on main in both modes: 14 wrong answers and 10 raises.

### Aggregates

| cell | Spark | main |
|---|---|---|
| `HAVING sum(k28) = CAST('2.505' AS DECIMAL(10,4))` | g = 1 (2.51) | no rows |
| `HAVING sum(k28) = max(k4)` | g = 0, 1 | no rows |
| `HAVING avg(k28) = max(a)` | g = 1 (0.836667 rounds to 1) | no rows |
| `sum(k28) = max(k4)` in the select list | true, true | false, false |
| `sum(k28) UNION ALL k4` | `decimal(38,2)` (2.505 → 2.51) | `decimal(38,4)` |
| `coalesce(sum(k28), max(k4))` | `decimal(38,2)` | `decimal(38,4)` (wrong type) |

`sum` of a `DECIMAL(28,2)` is `decimal(38,2)` on both engines; beside a `DECIMAL(10,4)` the pair is past 38.

### Multi-row `VALUES` (seat V)

| cell | Spark | main |
|---|---|---|
| `VALUES (DECIMAL(38,0) 1), (DECIMAL(10,2) 1.25)` and the reverse order | `decimal(38,0)`: 1, 1 | `decimal(38,2)`: 1.00, 1.25 |
| a NULL row between them; a plain `1.25` literal row; a second `INT` column | as above | as above |
| `VALUES (BIGINT 1), (DECIMAL(38,20) 1.0…049)` | `decimal(38,18)` | `decimal(38,20)` |
| `VALUES (DECIMAL(38,38) 0.5), (DECIMAL(10,2) 1.25)` | `decimal(38,30)` | raises (`simplify_expressions`) |
| three types (1.45, 1.5, 1) | `decimal(38,0)`: 1, 1, 2 (one rounding per row) | `decimal(38,2)` |
| `VALUES (1.25 DECIMAL(10,2)) UNION ALL SELECT a` | `decimal(38,0)` | `decimal(38,2)` |
| `VALUES (1), (DECIMAL(10,2) 1.25)` (the c2 cell) | `decimal(12,2)` | `decimal(22,2)`: **another cause**, below |
| control `VALUES (DECIMAL(10,0)), (DECIMAL(10,2))` | `decimal(12,2)` | agrees |

### Literals, NULL, overflow, structs

- `CAST('1' AS DECIMAL(38,0)) UNION ALL CAST('0.5' AS DECIMAL(38,38))` (the card's cell): Spark 1, 1; main refuses
  at planning (`Optimizer rule 'simplify_expressions' failed`).
- `a UNION ALL CAST('1.25' AS DECIMAL(10,2))`, `a UNION ALL NULL UNION ALL c`: wrong answers.
- **Overflow literals:** `CAST('1.5' AS DECIMAL(38,38)) UNION ALL a`, `WHERE a = CAST('1.5' AS DECIMAL(38,38))`, a
  39-digit literal beside `c`, `coalesce(c, CAST('1.5' AS DECIMAL(38,38)))`: Spark answers NULL for the literal
  with ANSI off and raises `NUMERIC_VALUE_OUT_OF_RANGE` / `NUMERIC_OUT_OF_SUPPORTED_RANGE` with ANSI on; main
  refuses in both modes at planning. With ANSI on they are `both_raise` except `coalesce`, which Spark answers in
  `decimal(38,30)` because the literal branch is never taken.
- `named_struct('f', a) UNION ALL named_struct('f', c)`: Spark `struct<f:decimal(38,0)>`, main
  `struct<f:decimal(38,2)>` — **struct fields are in the family.**
- `a + c` (arithmetic, its own rule): agrees, a control.

### Windows, `PIVOT`, grouping sets

- `GROUP BY GROUPING SETS ((coalesce(a, c)), ())` and `ROLLUP (coalesce(c, a))`: wrong answers (the key is the
  common type); `GROUPING SETS ((a), (c))` keeps each type and agrees.
- `RANGE` frames with a **decimal offset of another decimal type** (`ORDER BY a RANGE BETWEEN CAST('0.5' AS
  DECIMAL(10,2)) PRECEDING`, and `c` / `h` keys with a `DECIMAL(38,0)` offset): main refuses (`Invalid window frame:
  frame offsets for RANGE must be either a numeric value, a string value…`), and it refuses **the same-type offset
  too** ([side probe](evidence/raw/side-repark-false.jsonl), `range_same_type`). A plain literal offset (`1.5`,
  `0.5`) agrees. Spark casts the offset to the key's type, not to a common type: not this family.
- `PIVOT`: main refuses the syntax (`Unsupported ast node Pivot`), any types. Not this family.

## Doors

| door | common-type cells per mode | agree | wrong answer | wrong type | wrong write | over-raise (off) | note |
|---|---|---|---|---|---|---|---|
| Spark-door SQL | 655 | 43 | 276 | 7 | — | 329 | the grid, constants, three types, aggregates, `VALUES`, literals, grouping |
| DataFrame API | 136 | 4 | 55 | 0 | — | 77 | `union`, `unionByName`, `when`/`otherwise`, `isin(col)`, `between(col, col)`, `filter`, `select ==`, `coalesce`, `greatest`, `join` on 7 pairs, both tables; 8 constant cells |
| `F.expr` | 40 | 2 | 20 | 0 | — | 18 | `F.expr("x = y")`, `F.expr("coalesce(y, x)")`, `filter(F.expr(…))` on 7 pairs; two constants |
| Iceberg stores and DML | 43 | 0 | 5 | 0 | 38 | 0 | every statement, below |
| native `repark.sql()` | 655 | 43 | 258 | 7 | — | 347 | the Spark-door SQL cells over a view of the same rows; the same classes, plus refusals of Spark-only names (`if`, `array`, `map_concat`) |

The DataFrame door's control pair (`c0c`) agrees in all 20 cells per mode, so the door adds no cause of its own.

## Iceberg (local v2 hadoop catalog): every wrong write

Every statement ran in both ANSI modes with the same verdict; the `DELETE`/`UPDATE`/`MERGE` statements ran on
copy-on-write and merge-on-read tables. Stored rows are read back after the statement.

**Wrong writes, 38 per mode:**

1. `INSERT INTO x DECIMAL(38,2) SELECT a UNION ALL SELECT c` — Spark stores 1.00, 1.00, 2.00, −2.00, 0.00, 1.00…;
   main stores 1.25, 1.50, −1.50, 0.50, 0.49, 1.45.
2. `INSERT INTO x DECIMAL(38,20) SELECT i64 UNION ALL SELECT h` — Spark rounds `h` at 18 places
   (0.00000000004999999999 → 0.00000000005000000000); main stores the 20 places.
3. `INSERT … SELECT gg UNION ALL SELECT h` — Spark rounds `h` at 10 places.
4. `INSERT INTO x DECIMAL(38,38) SELECT b … UNION ALL SELECT c …` — Spark rounds `b` at 30 places; main keeps 38.
5. `INSERT … SELECT coalesce(CASE … c END, a)` — Spark stores `c` rounded to integers.
6. `INSERT … SELECT CASE WHEN … THEN a ELSE c END` — the same.
7. `INSERT INTO x DECIMAL(38,2) VALUES (DECIMAL(38,0) 1), (DECIMAL(10,2) 1.25)` — Spark 1.00, 1.00; main 1.00, 1.25.
8. `INSERT INTO ids SELECT id … WHERE a = c` — Spark inserts 5 rows, main none.
9. `CTAS … AS SELECT a UNION ALL SELECT c` — Spark `decimal(38,0)` integers; main `decimal(38,2)` exact.
10. `CTAS … AS SELECT id, coalesce(c, a)` — the same.
11. `CTAS … AS SELECT i64 UNION ALL SELECT h` — Spark `decimal(38,18)`, main `decimal(38,20)`.
12. `CREATE OR REPLACE TABLE … AS SELECT a UNION ALL SELECT c` — as 9.
13. `DELETE … WHERE a = CAST('1.25' AS DECIMAL(10,2))` — Spark deletes ids 1, 4; main none. (cow, mor)
14. `DELETE … WHERE c = CAST('1' AS DECIMAL(38,0))` — Spark deletes 1, 4, 6; main none. (cow, mor)
15. `DELETE … WHERE a = c` — Spark deletes 1–5; main none. (cow, mor)
16. `DELETE … WHERE a = 1.25` — Spark deletes 1, 4; main none. (cow, mor)
17. `DELETE … WHERE a IN (SELECT c FROM src)` — Spark deletes 1–5; main none. (cow, mor)
18. `UPDATE … WHERE c = CAST('1' AS DECIMAL(38,0))` — Spark updates 1, 4, 6; main none. (cow, mor)
19. `UPDATE … WHERE c IN (CAST('1' …(38,0)), CAST('2' …))` — Spark 1, 2, 4, 6; main none. (cow, mor)
20. `UPDATE … WHERE i64 = h` — Spark updates 5; main none. (cow, mor)
21. `MERGE … USING (c of ids 1, 2) ON t.a = s.c` — Spark matches ids 1, 2, 4; main none. (cow, mor)
22. `MERGE … USING (a of id 1) ON t.c = s.a` — Spark matches 1, 4, 6; main none. (cow, mor)
23. `MERGE … WHEN NOT MATCHED THEN INSERT` from `a UNION ALL c` — Spark inserts 1.00, 1.00; main 1.00, 1.25. (cow, mor)
24. A table partitioned by `c`: `DELETE … WHERE c = CAST('1' AS DECIMAL(38,0))` — Spark deletes 1, 4, 6; main none.
    (cow, mor)
25. A table partitioned by `a`: `DELETE … WHERE a = CAST('1.25' AS DECIMAL(10,2))` — Spark deletes 1, 4; main none.
    (cow, mor)

**Wrong reads of partitioned tables, 5 per mode:** `c = CAST('1' AS DECIMAL(38,0))` (Spark 1, 4, 6), `c IN (1, 2)`
as `DECIMAL(38,0)` (1, 2, 4, 6), `a = CAST('1.25' AS DECIMAL(10,2))` (1, 4), `a = 1.25` (1, 4), `h = CAST(1 AS
BIGINT)` (5); main reads none in each. No partition was pruned to a wrong answer that a plain table answered right:
the partitioned and plain answers are the same on main.

`UPDATE … SET a2 = coalesce(CASE … END, a)` refuses on main (`No field named __common_expr_1`) for mixed and
same-type operands alike: another cause, below.

## The legacy flag

`spark.sql.legacy.decimal.retainFractionDigitsOnTruncate=true` set by `SET` on both engines (both accept it and
read it back as `true`; `RESET` restores the default on both), then the three core positions on all 14 pairs, both
tables, seven extra cells and an Iceberg insert — 94 cells per mode:

| ANSI | agree | over-raise | both raise | wrong answer |
|---|---|---|---|---|
| off | 64 | 28 | 0 | 2 |
| on | 64 | 0 | 28 | 2 |

- **In range, the legacy rule is DataFusion's rule:** every pair whose values fit agrees in type and value (the
  card's `A UNION ALL C` answers `decimal(38,2)` 1.00, 1.25 on both; `a = 1.25` matches nothing on both).
- **Overflowing pairs differ by ANSI mode.** Spark's legacy type keeps the scale and can overflow: with ANSI off it
  answers **NULL** for the value that does not fit (`VALUES (0.5 (38,38)), (1.25 (10,2))` → 0.5, NULL; `a = b`
  → NULL rows), and with ANSI on it raises `NUMERIC_VALUE_OUT_OF_RANGE`. Main raises an Arrow error in both modes.
  So with the flag on, the 28 overflow cells are over-raises with ANSI off and both-raise (different text) with
  ANSI on.
- **The two wrong answers** are the after-`RESET` cells: Spark returns to its default rule, main stays on its own.
  They are this unit's ordinary cells.

## Other causes measured beside the family

Each is a cause the control pair `c0c` (or a same-type cell) shows too, so it is not this unit's rule:

| cause | cells | Spark | main | owner |
|---|---|---|---|---|
| `nvl` / `ifnull` over decimals | `nvl` row, side `nvl_same`, `nvl_c0c`, `ifnull_c0c` | the decimal common type | **`double`** (DataFusion's `nvl` signature lists no decimal) — wrong type, and wrong values past 15–17 digits | new card |
| `nullif(x, y)` result type | `nullif` row | `x`'s type | the comparison's common type | sweep family 6 |
| `EXCEPT` / `INTERSECT` result type | `except`, `intersect` rows of `c0c` | the common type | the left side's type | sweep family 5; the past-38 rows are **wrong membership**, this unit's (revision M4) |
| `array_union` of two element types | `array_union` row | refuses (`BINARY_ARRAY_DIFF_TYPES`) | answers | new card (S3) |
| `lag`/`lead` with a column default | `lag`, `lead` rows | answers | refuses (`only support Literal types`), same type too | new card |
| `JOIN … USING (k)` over two **unaliased** derived tables | `join_full_using` row, side `using_same_type`, revision `m2/using_*` without an alias | answers | refuses (`duplicate unqualified field name k`) for every join type, same type too | new card, scoped to this shape; aliased `USING` plans and is this unit's (revision M2) |
| `RANGE` frame with a decimal offset | `w_range_*_dec`, side `range_same_type` | answers | refuses, same type too | new card |
| `PIVOT` | `pivot_*` | answers | refuses the syntax | existing gap |
| `UPDATE … SET x = coalesce(CASE …, y)` | `update_set_coalesce`, side | answers | refuses (`__common_expr_1`), same type too | new card |
| `avg` of a decimal | `agg/sum_type`, side `avg_k28` | 2.51 / 3 = **0.836667** | **0.836666** (truncates) | new card (wrong answer, S2) |
| an `INT` literal in `VALUES` | `values/v_int_c` (the c2 cell) | `INT` → `decimal(12,2)` | the literal is `BIGINT` → `decimal(22,2)` | joins this unit's function (integer forms), below 38 |

## After the critic (revision, 2026-11-16)

The Muse inventory critic (`VERDICT: GAPS`, no cell contradicted, no citation wrong) named four missed seats or
shapes and a list of pin candidates. Each was re-measured here, on main and Spark 4.1.2, both ANSI modes, by
[evidence/probes/run_revision.py](evidence/probes/run_revision.py) (63 cells per run; raw in
`evidence/raw/revision-*.jsonl`). **No verdict depends on the ANSI mode.** The first step-0 grid's `except` and
`intersect` rows of the past-38 pairs are also moved from "other causes" into the family: they are wrong **rows**,
not only a wrong type (its `union` row, already in the family, is `UNION DISTINCT`).

**Revised totals, Spark door, per mode:** 946 common-type cells (874 + 20 set-operation cells + 52 revision cells);
ANSI off 54 agree, 402 wrong answers, 9 wrong types, 38 wrong writes, 439 refusals, 2 missed refusals, 2 both-raise
(**890 wrong**); ANSI on the same with 433 refusals and 8 both-raise (**884 wrong**). Native door: 632 of 675.

### M1 — decimal map keys (`create_map`, `map_concat`)

The walk's `MAP_CONSTRUCTORS` takes value positions only (`decimal_float_widen.rs:27`, `:108-110`); keys reach
`comparison_coercion` in `create_map.rs:63` and `map_concat.rs:80` unseen.

| cell (`t`) | Spark | main |
|---|---|---|
| `map_concat(map(a,'x'), map(c,'y'))`, id 1 | raises `DUPLICATED_MAP_KEY` (1.25 rounds to 1) | `map<decimal(38,2),string>` {1.00, 1.25} |
| `map(a,'x', c,'y')`, id 1 | raises `DUPLICATED_MAP_KEY` | {1.00, 1.25} |
| the same over ids 1–5 | raises `DUPLICATED_MAP_KEY` | raises `NULL_MAP_KEY` (no key is NULL in those rows) |
| `map_concat(map(a,'x'), map(c+5,'y'))`, `map(a,'x', c+5,'y')`, `map_keys(…)`, ids 1–5 | `map<decimal(38,0),…>` {1, 6}… | raises `NULL_MAP_KEY` |
| controls: `map_concat(map(a,'x'), map(a+5,'y'))`, `map(a,'x')`, ids 1–5 | answers | agrees |

So the keys are in the family twice over: a rounded collision must raise as Spark does, and the mixed-key shape
raises a NULL-key error on main that the same-type control does not.

### M2 — `USING` joins

| cell (`a` beside `c` as `k`) | Spark | main |
|---|---|---|
| `(…) l JOIN (…) r USING (k)`, aliased, inner | 9 matches, key `decimal(38,0)` | **no rows**, key `decimal(38,0)` |
| aliased `LEFT JOIN … USING (k)` | 10 rows, matched | every left row unmatched |
| aliased `FULL OUTER JOIN … USING (k)` | 10 rows, matched | 12 rows, none matched |
| aliased `FULL OUTER … USING (k)` selecting only `k` | `-2 0 1×6 2 null` | `-2 0 1 1 2` and 7 NULLs |
| aliased inner, `BIGINT` beside `DECIMAL(38,20)` | ids (1, 5), (5, 5) | no rows |
| aliased inner, same type (control) | answers | agrees |
| **unaliased** derived tables, inner, left, full, distinct or duplicate names, same type too (7 cells) | answers | refuses (`duplicate unqualified field name k`) |
| DataFrame `l.join(r, "k")` | 9 matches | no rows |

The refusal is the unaliased shape, any join type and any types; it is a card of its own. Aliased `USING` and the
DataFrame `join(…, "k")` plan through `coerce_join` and are this unit's (the walk's `rewrite_join` sees the keys).

### M3 — integer literals in `IN` take the integer's form

| cell (`t`) | Spark | main |
|---|---|---|
| `CAST('1.0…049' AS DECIMAL(38,30)) IN (1)` | true, every row | false |
| `CAST('1.0…049' AS DECIMAL(38,30)) IN (id)` and `… = id` | true at id 1 | false |
| `w30 IN (1)` (select and `WHERE`), `w30 IN (1, 2)`, `w30 IN (CAST(1 AS BIGINT))`, DataFrame `isin(1)` | ids 1, 5 | none |
| `w30 NOT IN (1)` | 2, 3, 4 | 1–5 |
| `w30 IN (i32)`, `w30 IN (SELECT i32 FROM t)` | 1, 4, 5 | 4 |
| `w36 IN (1)` | 1, 5 | none |
| `w30 = 1`, `w36 = 1`, `CASE w30 WHEN 1`, `w30 BETWEEN 1 AND 1`, `h IN (1)` | agree | agree |

So Spark's literal exception (rule 3 of the sketch) holds for `=`, `<=>`, simple `CASE` and `BETWEEN` but **not for
`IN`**: an `IN` list (and an `IN` subquery) folds an integer literal at `INT`'s `decimal(10,0)` form, so
`w30 IN (1)` rounds `w30` to 28 places. The shared function needs an `IN` mode. `h IN (1)` agrees because
`(10,0)` beside `(38,20)` still fits 38.

### M4 — set-operation membership, and `UNION DISTINCT`

| cell | Spark | main |
|---|---|---|
| `a INTERSECT DISTINCT c`, `c INTERSECT a`, DataFrame `intersect` | `-2, 0, 1, 2` (`decimal(38,0)`) | no rows (`c INTERSECT a` keeps `decimal(10,2)`) |
| `a EXCEPT DISTINCT c`, DataFrame `subtract` | `null` only | `-2, 0, 1, 2, null` |
| `c EXCEPT a` | no rows | all six `c` values |
| `i64 INTERSECT h` | `1.000000000000000000`, `null` (`decimal(38,18)`) | `null` (`bigint`) |
| `a UNION DISTINCT c` | `-2, 0, 1, 2, null` | 11 exact rows (`decimal(38,2)`) |

The first grid's `union` position is already `UNION DISTINCT` on six pairs; the explicit spelling answers alike.

### Pin candidates (each differs on main, both modes)

| cell | Spark | main | seat |
|---|---|---|---|
| two-field struct union `named_struct('f', a, 'g', c) ∪ named_struct('f', c, 'g', a)` | `struct<f:decimal(38,0),g:decimal(38,0)>` [1,1] [2,2] | `decimal(38,2)` fields | struct recursion |
| `named_struct('f', a) = named_struct('f', c)` | raises | raises | **agrees**: a both-raise pin only |
| `max(CASE … a ELSE c END)`, `min(…)` | `decimal(38,0)` 2 / −2 | `decimal(38,2)` 2.00 / −2.00 | `CASE` inside an aggregate |
| `sum(CASE … a ELSE c END)` | 2 | 2.25 | the same |
| `sum(v)` over `a UNION ALL c` | 5 | 5.69 | union under an aggregate |
| `CAST('1' AS DECIMAL(38,0)) BETWEEN c AND c` | 1, 4, 6 | none | `widen_between` (step 0's `a1_between_c`) |
| `map('k', a, 'j', c)`, `map_concat(map('x', a), map('y', c))` | `map<string,decimal(38,0)>` 1, 1 | `decimal(38,2)` 1.00, 1.25 | map values |
| `c = CAST('1' AS DECIMAL(38,0))` and mirror; `a = CAST('1.25' AS DECIMAL(10,2))` and mirror | 1, 4, 6 / 1, 4 | none | #1345's shield (step 0's 18 cells) |
| `a = CAST(1.5 AS DECIMAL(10,2))` | id 2 | none | `FoldSparkNumericCasts` folds the cast literal after coercion: the fold must round half up |
| DataFrame `unionByName(…, allowMissingColumns=True)` | `decimal(38,0)` | `decimal(38,2)` | `union_by_name` |

## Exclusions and sampling

- **Sampling.** The ten core positions ran on all 14 pairs; the 46 positions on six pairs (`ab`, `ac`, `gh`,
  `i64h`, `i32w30`, `c0c`); the other pairs are uniform with these in every core position, as the grid shows.
  The DataFrame and `F.expr` cells ran on seven pairs; the Iceberg cells on the `ac`, `i64h`, `gh` and `bc` shapes;
  the legacy cells on the three core positions.
- **Not measured:** decimal struct fields in comparisons (only the union), `MERGE` source unions of three types,
  `PIVOT` value lists (main refuses `PIVOT`), the native door's Iceberg statements, and `Decimal256` inputs (no
  Spark type maps to it).
- **Row order** is not compared: every answer's rows are sorted before pairing.

Up: [map.md](map.md).
