# AVG-DECIMAL-ROUND-1 — step 0 domain table (with DECIMAL-DIVIDE-LITERAL-1)

Measured 2026-11-17 on main `a792ce47` (the package built from `7bf32859`, whose product tree differs from
`a792ce47` only in `map.md` files) against Spark 4.1.2 with Iceberg 1.11.0, ANSI on and off, the session zone
pinned. Documents and probes only. Every cell's raw answer, both types and the verdict are in
[evidence/raw/](evidence/raw/map.md); the letter grids below are copied from `grids.md` there. Provenance and
commands: [evidence/map.md](evidence/map.md).

## The data

- **Table `t` (`avg`)**: 13 decimal columns `c<p>_<s>` — `(10,0)`, `(10,2)`, `(10,6)`, `(28,0)`, `(28,2)`,
  `(28,10)`, `(34,0)`, `(34,4)`, `(34,30)`, `(38,0)`, `(38,2)`, `(38,18)`, `(38,36)` — plus `i INT`, `b BIGINT`,
  `x DOUBLE`. Each group `g` holds the same unscaled pattern in every column, so a quotient lands at the same place
  relative to the column's last digit:

  | group | unscaled values | what it tests |
  |---|---|---|
  | g1 | 1, 2 | terminates (1.5) |
  | g2, g4 | 1, 1, 0 / −1, −1, 0 | ⅔ and −⅔: rounds away from zero |
  | g3 | 1, 0, 0 | ⅓: truncation and rounding agree |
  | g5, g6 | 1 / −1 and 31 zeros | 1/32, an exact half at `s + 5`, and its negative |
  | g7 | 1, NULL, 2, NULL, 2 | NULLs skipped (5/3) |
  | g8 | NULL, NULL | all-NULL group |
  | g9, g11 | 251, 0, 0 / −251, 0, 0 | the card's `2.51 / 3` and its negative |
  | g10 | −7, 2, −1 | negative, terminates |
  | g12 | 6 and 10 zeros | 6/11 = 0.5454…: a store into a narrower column rounds twice |
  | g13, g14 | 1, 2, 5 / −1, −2, −5 | 8/3, three distinct values (for `DISTINCT` and 3-row frames) |
  | g20 | 10^p − 1 twice | the column maximum: the sum passes 38 digits at p = 38 |
  | g21, g23 | v, v, v ± 1 with v = 10^(min(p,34)−1) | wide values, ⅓ |
  | g22 | 9·10^(min(p,34)−1) twice | the sum fits, the sum × 10^4 passes `i128` at p ≥ 34 |
  | g24 | 5·10^(p−1) twice | the sum is exactly 10^p: passes 38 digits at p = 38 |

  A value that does not fit a column is NULL there. `t` is one Parquet file; `t4` holds the same rows in four
  files (the partial-state merge).
- **Table `v` (division)**: 10 decimal columns `d<p>_<s>` — `(10,0)`, `(10,2)`, `(18,4)`, `(20,2)`, `(28,2)`,
  `(28,10)`, `(34,4)`, `(38,0)`, `(38,2)`, `(38,10)` — over the unscaled rows 30, 100, 251, −251, 1, 1000, NULL,
  777, 6 and the column maximum (row 10), plus `bi INT`, `bl BIGINT`, `bs SMALLINT`, `by TINYINT`, all 3.

## Letters

`=` equal answer and type · `T` the type differs, the values are equal · `t` the type is equal and every differing
value is Spark's minus one unit toward zero (truncation) · `V` another value difference (wider type with more
digits, fewer digits, or wrong rows) · `R` RePark raises where Spark answers · `S` Spark raises where RePark
answers · `B` both raise · `-` not run. A cell is one query; "wrong" below means not `=`, and `B` is counted apart.

## Headline

| run (Spark door) | cells | `=` | `T` | `t` | `V` | `R` | `S` | `B` |
|---|---|---|---|---|---|---|---|---|
| `avg`, ANSI on | 616 | 143 | 0 | 413 | 39 | 13 | 0 | 8 |
| `avg`, ANSI off | 616 | 143 | 0 | 413 | 39 | 21 | 0 | 0 |
| division and controls, ANSI on | 456 | 176 | 46 | 0 | 169 | 12 | 8 | 45 |
| division and controls, ANSI off | 456 | 215 | 46 | 0 | 177 | 18 | 0 | 0 |
| native door, all families, ANSI on | 268 | 64 | 15 | 98 | 51 | 21 | 1 | 18 |
| native door, all families, ANSI off | 268 | 64 | 15 | 98 | 52 | 39 | 0 | 0 |

(Counted from `verdicts.jsonl`; `avg` includes the `INT`, `BIGINT` and `DOUBLE` controls, all `=`.)

- **`avg`**: every position that averages a decimal truncates, on every door, in both modes, at every precision.
  No position rounds. The type is always Spark's. Sums near the limit raise where Spark answers.
- **Division**: on the Spark door's SQL and `F.expr`, an integer literal beside a decimal takes its type's width,
  not its digits; the DataFrame operator `/` answers `double` for any decimal operand; `try_divide` of a decimal by
  an integer casts the decimal to an integer.

## `avg` — Spark-door SQL, one group per cell (`SELECT avg(c) FROM t WHERE g = k`)

ANSI on; ANSI off differs only in rows g20 and g24 (every `B` becomes `R`: Spark answers NULL, RePark raises).

| group | c10_0 | c10_2 | c10_6 | c28_0 | c28_2 | c28_10 | c34_0 | c34_4 | c34_30 | c38_0 | c38_2 | c38_18 | c38_36 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| g1 | = | = | = | = | = | = | = | = | = | = | = | = | = |
| g2 | t | t | t | t | t | t | t | t | t | t | t | t | t |
| g3 | = | = | = | = | = | = | = | = | = | = | = | = | = |
| g4 | t | t | t | t | t | t | t | t | t | t | t | t | t |
| g5 | t | t | t | t | t | t | t | t | t | t | t | t | = |
| g6 | t | t | t | t | t | t | t | t | t | t | t | t | = |
| g7 | t | t | t | t | t | t | t | t | t | t | t | t | t |
| g8 | = | = | = | = | = | = | = | = | = | = | = | = | = |
| g9 | t | t | t | t | t | t | t | t | t | t | t | t | t |
| g10 | = | = | = | = | = | = | = | = | = | = | = | = | = |
| g11 | t | t | t | t | t | t | t | t | t | t | t | t | t |
| g12 | t | t | t | t | t | t | t | t | t | t | t | t | t |
| g13 | t | t | t | t | t | t | t | t | t | t | t | t | t |
| g14 | t | t | t | t | t | t | t | t | t | t | t | t | t |
| g20 | = | = | = | = | = | = | R | R | R | B | B | B | B |
| g21 | = | = | = | = | = | = | = | = | = | = | = | = | = |
| g22 | = | = | = | = | = | = | R | R | R | R | R | R | = |
| g23 | = | = | = | = | = | = | = | = | = | = | = | = | = |
| g24 | = | = | = | = | = | = | = | = | = | B | B | B | B |

- **Truncation, 128 cells per mode**: `DECIMAL(10,2)` g9 is `0.836667` on Spark and `0.836666` on RePark (the
  card's cell); g2 `0.006667` / `0.006666`; g4 `-0.006667` / `-0.006666` (Spark rounds away from zero); g5 `0.000313`
  / `0.000312` (the exact half rounds up); g12 `0.005455` / `0.005454`. At `(38,36)` the result type is
  `decimal(38,38)`, so g5 and g6 (`3.125e-38`) round down on Spark too.
- **R, sums near the limit** (Spark answers, RePark raises `Execution error: Arithmetic Overflow in
  AvgAccumulator` with no condition): g20 at p = 34 (Spark `9999999999999999999999999999999999.0000`) and g22 at
  p = 34 and 38 (Spark `9000000000000000000000000000000000.0000`). The sum fits Spark's `decimal(38, s)` buffer;
  RePark's `sum × 10^4` passes `i128` first.
- **g20 and g24 at p = 38** (the sum passes 38 digits): ANSI on, both raise — Spark `[ARITHMETIC_OVERFLOW] Overflow
  in sum of decimal`, RePark the condition-less `Arithmetic Overflow in AvgAccumulator`; ANSI off, Spark answers
  `NULL` and RePark raises.
- g21 and g23 (wide values over ⅓) round down, so they agree; the wide round-up case is not in the table (sampled
  out: the averager is one function whatever the magnitude).

## `avg` — forms (both modes alike)

Each cell is one query over the 14 ordinary groups; `t` means every differing group is a one-unit truncation.

| door | form | c10_0 | c10_2 | c10_6 | c28_0 | c28_2 | c28_10 | c34_0 | c34_4 | c34_30 | c38_0 | c38_2 | c38_18 | c38_36 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| sql | `GROUP BY g` | t | t | t | t | t | t | t | t | t | t | t | t | t |
| sql | same over four files (`t4`) | t | t | t | t | t | t | t | t | t | t | t | t | t |
| sql | `mean` | t | t | t | t | t | t | t | t | t | t | t | t | t |
| sql | `avg(DISTINCT …)` | t | t | t | t | t | t | t | t | t | t | t | t | t |
| sql | `try_avg` | t | t | t | t | t | t | t | t | t | t | t | t | t |
| sql | window, whole partition | t | t | t | t | t | t | t | t | t | t | t | t | t |
| sql | window, `ROWS 1 PRECEDING` | = | = | = | = | = | = | = | = | = | = | = | = | = |
| sql | window, `ROWS 2 PRECEDING` | t | t | t | t | t | t | t | t | t | t | t | t | t |
| sql | window, running `ROWS UNBOUNDED` | t | t | t | t | t | t | t | t | t | t | t | t | t |
| sql | window, `RANGE 2 PRECEDING` | t | t | t | t | t | t | t | t | t | t | t | t | t |
| sql | `HAVING avg(x) = <Spark's value>` | V | V | V | V | V | V | V | V | V | V | V | V | V |
| sql | `HAVING avg(x) * count(x) > sum(x)` | V | V | V | = | = | V | = | = | V | = | = | V | V |
| sql | `avg(x) + max(x)`, `avg(x) - 1`, `avg(x) * 2` | V | V | V | V | V | V | V | V | V | V | V | V | V |
| sql | `avg(x)` beside a decimal constant | t | t | t | t | t | t | t | t | t | t | t | t | t |
| sql | control: `sum(x) / count(x)` | = | = | = | = | = | = | = | = | = | = | = | = | = |
| dataframe | `F.avg`, `F.mean`, `GroupedData.avg`, `GroupedData.mean`, `agg({x: "avg"})`, `agg({x: "mean"})`, `select(F.avg)`, `repartition(4)` first, `F.expr("avg(x)")`, `Window` whole and `rowsBetween(-2, 0)` | t | t | t | t | t | t | t | t | t | t | t | t | t |

- The `ROWS 1 PRECEDING` frame averages two values, which always terminate one digit past the column; it is `=`
  by the data, not by the path (the 3-row frame is `t`).
- **Wrong rows, not only digits**: `HAVING avg(c10_2) = <Spark's half-up value>` keeps 13 groups on Spark and 3 on
  RePark (g1, g3, g10, the terminating ones). `HAVING avg(x) * count(x) > sum(x)` keeps Spark's rounded-up
  positives (g2, g5, g7, g9, g12, g13) and RePark's truncated negatives (g4, g6, g11, g14) at `(10,2)`.
- **Controls**: `avg` of `INT`, `BIGINT` and `DOUBLE` (grouped, 2-row frame, `DISTINCT`) answers `double` alike.
  `sum(x) / count(x)` divides a decimal by a `BIGINT` column, so both type the divisor `decimal(20,0)` and round
  half up: `=` everywhere. It is the evidence that RePark's division kernel already rounds as Spark does.
- **`describe` and `summary`** over groups 2, 9 and 12 (exact strings, `mean` row; the other rows agree):

  | column | Spark `mean` | RePark `mean` |
  |---|---|---|
  | `c10_2` (`describe`, `summary("count","mean","stddev","min","max")`) | `0.152353` | `0.152352` |
  | `c28_2` | `0.152353` | `0.152352` |
  | `c38_18` | `0.0000000000000001523529` | `0.0000000000000001523528` |
  | `c10_0` | `15.2353` | `15.2352` |

  `summary()` with no arguments raises on RePark (`UnsupportedOperationException`: no percentile rows, the
  disclosed gap); Spark answers. That refusal is not this unit's.

## Division — the Spark door (DECIMAL-DIVIDE-LITERAL-1)

ANSI on. ANSI off changes only the `B` and `S` cells: both engines answer NULL for the overflowing row 10, so a
`B` becomes `=` (or `T`/`V` where the type or digits differ anyway), except the DataFrame `/` at `d38_0` and `d38_2`
(`S` becomes `V`: RePark's `double` answers `3.333333333333333e+35`) and `d * 3`, `d * -3`, `col(d) * 3` at `d38_0`
and `d38_2` (`B` becomes `R`: RePark raises `Arrow error: Arithmetic overflow`, Spark answers NULL).

| door | cell | d10_0 | d10_2 | d18_4 | d20_2 | d28_2 | d28_10 | d34_4 | d38_0 | d38_2 | d38_10 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| sql | `d / 3` | V | V | V | V | V | V | V | B | B | = |
| sql | `d / 3S` | = | V | V | V | V | V | V | B | B | = |
| sql | `d / 3Y` | = | = | = | = | = | = | = | B | B | = |
| sql | `d / 3L` | V | V | V | V | V | V | V | B | B | = |
| sql | `d / 100` | T | T | T | T | T | T | = | B | B | = |
| sql | `d / -3` | V | V | V | V | V | V | V | B | B | = |
| sql | `d / 3000000000` | V | V | V | V | = | = | = | = | = | = |
| sql | `d / 30000000000000000000` (a decimal literal) | = | = | = | = | = | = | = | = | = | = |
| sql | `d / 11` | V | V | V | V | V | V | V | B | B | = |
| sql | control `d / 3.0` | = | = | = | = | = | = | = | B | B | = |
| sql | `3 / d` | T | T | T | T | V | V | V | V | V | V |
| sql | `3S / d` | T | T | T | T | T | V | V | V | V | V |
| sql | `3Y / d` | = | = | = | = | = | = | = | = | = | = |
| sql | `3L / d` | T | T | V | V | V | V | V | V | V | V |
| sql | `100 / d`, `11 / d`, `-3 / d` | T | T | T | T | V | V | V | V | V | V |
| sql | `3000000000 / d` | T | T | V | V | V | V | V | V | V | V |
| sql | `30000000000000000000 / d`, `3.0 / d` | = | = | = | = | = | = | = | = | = | = |
| sql | `d / CAST(3 AS INT)`, `d / CAST(3 AS BIGINT)`, `d / bi`, `d / bl`, `d / bs`, `d / by` | = | = | = | = | = | = | = | B | B | = |
| sql | `WHERE d / 3 = round(d / 3, 6)` | V | V | V | V | V | = | V | B | B | V |
| sql | `CASE WHEN d / 3 = round(d / 3, 6) …` | V | V | V | V | V | = | V | B | B | V |
| sql | `try_divide(d, 3)` | R | V | R | R | R | R | R | R | R | R |
| sql | control `d % 3` | T | T | T | T | T | T | T | T | T | T |
| sql | control `d % bi` | = | = | = | = | = | = | = | = | = | = |
| sql | control `d * 3`, `d * -3` | = | = | = | = | = | = | = | B | B | R |
| sql | control `d + 3` | = | = | = | = | = | = | = | B | B | = |
| sql | control `d - 3` | = | = | = | = | = | = | = | = | = | = |
| dataframe | `F.expr("d / 3")`, `selectExpr("d / 3")` | V | V | V | V | V | V | V | B | B | = |
| dataframe | `col(d) / 3`, `col(d) / F.lit(3)`, `col(d) / col("bi")` | V | V | V | V | V | V | V | S | S | V |
| dataframe | `3 / col(d)` | V | V | V | V | V | V | V | V | V | V |
| dataframe | `where(col(d) / 3 == round(col(d) / 3, 6))` | V | V | V | V | V | V | V | S | S | V |
| dataframe | control `col(d) * 3` | = | = | = | = | = | = | = | B | B | R |

**Types, Spark / RePark** (`d10_2`, then `d28_2` where it differs):

| cell | `d10_2` | `d28_2` |
|---|---|---|
| `d / 3`, `d / 11`, `d / -3`, `d / 100` | `decimal(14,6)` / `decimal(21,13)` | `decimal(32,6)` / `decimal(38,12)` |
| `d / 3S` | `decimal(14,6)` / `decimal(16,8)` | `decimal(32,6)` / `decimal(34,8)` |
| `d / 3L` | `decimal(14,6)` / `decimal(31,23)` | `decimal(32,6)` / `decimal(38,12)` |
| `d / 3000000000` | `decimal(21,13)` / `decimal(31,23)` | equal |
| `3 / d` | `decimal(14,11)` / `decimal(23,11)` | `decimal(32,29)` / `decimal(38,26)` |
| `3L / d` | `decimal(14,11)` / `decimal(33,11)` | `decimal(32,29)` / `decimal(38,16)` |
| `d % 3` | `decimal(3,2)` / `decimal(10,2)` | `decimal(3,2)` / `decimal(12,2)` |
| `col(d) / 3` (DataFrame) | `decimal(14,6)` / **`double`** | `decimal(32,6)` / **`double`** |
| `try_divide(d, 3)` | `decimal(14,6)` / **`double`** | — (raises) |
| `1.0 / 3` (no column) | `decimal(7,6)` / `decimal(13,12)` | — |

- **The rule, measured**: Spark types an `INT`, `SMALLINT` or `BIGINT` literal beside a decimal by its digits
  (`3`, `3S`, `3L`, `-3` → `decimal(1,0)`; `100` → `decimal(3,0)`; `3000000000` → `decimal(10,0)`), and a `TINYINT`
  literal by its type (`3Y` → `decimal(3,0)`: `DataTypeUtils.fromLiteral` has `Short`, `Integer` and `Long` arms and
  no `Byte` arm). RePark types the literal of `/` and `%` by its integer type (`INT` → `decimal(10,0)`,
  `SMALLINT` → `(5,0)`, `BIGINT` → `(20,0)`, `TINYINT` → `(3,0)`, so `3Y` agrees). Typed operands
  (`CAST(3 AS INT)`, columns) are `decimal(10,0)` on both and agree.
- **DEC-5 is partial (measured beside, `run_extra.py`)**: multiplication and addition take the digits for a plain
  `INT` literal only. `d10_2 * 3S` is `decimal(12,2)` on Spark and `decimal(16,2)` on RePark; `d10_2 * 3L` is
  `decimal(12,2)` against `decimal(31,2)`; `d10_2 * 3Y` and `d10_2 + 3Y` agree (`decimal(14,2)`, `decimal(11,2)`).
  The suffixed literal is the same seat as the division literal.
- **Digits**: when the quotient does not terminate, RePark answers more digits (`0.8366666666667` against
  `0.836667`: `longer_quotient`, every one the half-up rounding of Spark's) at narrow precisions, and **fewer**
  digits at p ≥ 28 on the literal-left side (`3 / d28_2` row 3: Spark `1.19521912350597609561752988048`, RePark
  `1.19521912350597609561752988`), because the wider literal pushes the result past 38 digits and the adjusted
  scale drops. `d / 100` terminates, so it is `T`.
- **Wrong rows**: `WHERE d10_2 / 3 = 0.836667` keeps row 3 on Spark and nothing on RePark; `CASE WHEN d10_2 / 3 >
  0.8366667` answers `hi` on Spark and `lo` on RePark; the `round(…, 6)` comparisons keep every row on Spark and only
  the terminating ones on RePark.
- **The DataFrame operator `/` answers `double` for every decimal operand** (literal, `F.lit`, an `INT` column,
  literal on the left): `col("d10_2") / 3` is `0.09999999999999999` for `0.30`, and row 10 of `d38_2` answers
  `3.333333333333333e+35` where Spark raises (ANSI on) or answers NULL (ANSI off). This is a second seat, not the
  literal's: `F.expr` and `selectExpr` go through SQL typing and show only the literal width.
- **`try_divide(d, 3)` casts the decimal to `INT`**: `try_divide(2.51, 3)` is `0.6666666666666666` (that is 2 / 3),
  `double`; a value past `INT` raises `Cast error: value of 9999999999 is out of range Int32`. Measured beside it
  (`run_extra.py`, ANSI on): `try_add(d10_2, 1)` answers `INT` `1, 2, 3` where Spark answers `decimal(11,2)` `1.30,
  2.00, 3.51`, and `try_multiply(d10_2, 3)` answers `0, 3, 6` where Spark answers `0.90, 3.00, 7.53`;
  `try_divide(d, CAST(3 AS DECIMAL(1,0)))` agrees. A third seat (the `try_*` operand coercion).
- **The session-view `h / 2` cell (R-INTDIV-2)**, `h = d10_2 / 2`: Spark `decimal(16,8)` in every scope; RePark
  `decimal(32,24)` in the derived table, CTE, temporary view and flat `d10_2 / 2 / 2`, and `decimal(25,17)` in the
  session-catalog view (a view over an Iceberg table). Values equal (`/ 2` terminates): `T` in all five.
- **Overflow at 38 digits**: the `B` cells are Spark `[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]` and RePark the same
  head on the row-10 maximum; ANSI off both answer NULL for that row and the cell is `=` except where a type
  differs. `d38_10 * 3` raises `Arrow error: Arithmetic overflow` on RePark in both modes where Spark answers
  `decimal(38,8)` — a multiplication cell, outside both units (see "Other causes").

## Iceberg (local v2 hadoop catalog): every wrong write

Copy-on-write unless marked; merge-on-read was run for `MERGE` (both families) and `UPDATE` (division) and stores
the same rows. Both modes alike except where noted. Target columns: `n DECIMAL(10,5)` (narrower than the result)
and `w DECIMAL(24,10)` (wider).

| statement | source | stored on Spark | stored on RePark |
|---|---|---|---|
| `CREATE TABLE … AS SELECT g, avg(c) …` | `c10_0`, `c10_2`, `c28_2`, `c38_18` | schema equal (`decimal(14,4)`, `(14,6)`, `(32,6)`, `(38,22)`); g9 of `c10_2` `0.836667` | same schema; `0.836666` — every non-terminating group truncated |
| `INSERT … SELECT g, avg(c), avg(c)` into `n`, `w` | `c10_2` | g9 `0.83667`, `0.8366670000`; g12 `0.00546`, `0.0054550000` | g9 `0.83667`, `0.8366660000`; g12 **`0.00545`**, `0.0054540000` |
| same | `c10_0` | g12 `0.54550`, `0.5455000000` | g12 `0.54540`, `0.5454000000` |
| `MERGE … UPDATE SET n = s.m, w = s.m … INSERT …` from a grouped `avg` | `c10_2`, `c10_0` (cow and mor) | as the `INSERT` | as the `INSERT` |
| `CREATE TABLE … AS SELECT d / 3 AS q, d / 11 AS q11` | `d10_2` | `decimal(14,6)`, `decimal(14,6)`; row 3 `0.836667` | **`decimal(21,13)`**, **`decimal(21,13)`**; `0.8366666666667` |
| same | `d28_2` | `decimal(32,6)` | **`decimal(38,12)`** |
| same | `d38_2` | ANSI on: both raise (row 10); ANSI off: `decimal(38,6)`, equal | |
| `INSERT … SELECT id, d, d / 11, d / 3` into `n`, `w` | `d10_2`, `d28_2` | row 3 `w` `0.8366670000`; row 9 (`0.06 / 11`) `n` `0.00546` | row 3 `w` `0.8366666667`; row 9 `n` **`0.00545`** |
| `UPDATE … SET n = d / 11, w = d / 3` (cow and mor) | `d10_2`, `d28_2` | as the `INSERT` | as the `INSERT` |
| `MERGE … UPDATE SET n = s.d / 11, w = s.d / 3 … INSERT …` (cow and mor) | `d10_2`, `d28_2` | as the `INSERT` | as the `INSERT` |
| `INSERT`, `UPDATE`, `MERGE` | `d38_2` | `decimal(38,6)` both: equal | |

- **Double rounding through the wider type**: Spark rounds the quotient at its own scale, then the store rounds
  again: `0.06 / 11` is `0.005455` (scale 6), stored at scale 5 as `0.00546`; RePark's `0.0054545454545` stores
  `0.00545`. The same for `avg` g12: Spark `0.005455` → `0.00546`, RePark `0.005454` → `0.00545`. A narrower target
  does not hide the defect.
- **Wrong writes, named**: avg CTAS ×4, avg `INSERT` ×2, avg `MERGE` ×4 (two columns, cow and mor); division CTAS
  ×2 (wrong stored type and digits), division `INSERT` ×2, `UPDATE` ×4 and `MERGE` ×4 (`d10_2`, `d28_2`, cow and
  mor). 22 wrong writes per mode; the 6 `d38_2` statements agree.

## The native door `repark.sql()` (a sample; not the label's door)

Paired with Spark in both modes over `VALUES` views of the same rows; columns `c10_0`, `c10_2`, `c28_2`, `c34_4`,
`c38_0`, `c38_18` and `d10_0`, `d10_2`, `d28_2`, `d38_2`.

- `avg` truncates exactly as on the Spark door (grouped, ungrouped, `DISTINCT`, `mean`, the 3-row and running
  frames, `HAVING`); g20 and g22 raise as there; `try_avg` and `try_divide` are not registered
  (`UNRESOLVED_ROUTINE`).
- Division is DataFusion's own operator (EXPLAIN: `d10_2 / Decimal128(Some(3),20,0)`): `d10_2 / 3` is
  `decimal(14,6)` (Spark's type) but truncated — `0.836666` (`t`); `d10_0 / 3` and typed operands differ in type and
  digits; `3 / d` differs everywhere; the `round(…, 6)` comparisons raise `Invalid comparison operation:
  Decimal128(15, 6) == …`; the row-10 maximum of `d38_2` raises `Arrow error: Arithmetic overflow` where Spark
  answers NULL (ANSI off).

## The confs (a sample, both engines, both modes)

The conf runs keep the `avg` grouped, ungrouped and 3-row-frame forms (286 cells), the `F.avg` and `F.expr`
DataFrame forms, and every division cell. Both engines accept the `SET` and read the value back.

- **`spark.sql.legacy.literal.pickMinimumPrecision=false`**: Spark then types the literal by its integer type, and
  **Spark's division agrees with RePark** — 281 of 293 SQL division cells `=` (ANSI off; the rest are `try_divide`),
  against 152 of 298 under the default. Multiplication turns: Spark's `d10_2 * 3` widens to `decimal(21,2)` and RePark
  stays `decimal(12,2)` (`T` on six columns, `V` on `d28_10`, and the 38-digit columns raise on RePark as under the
  default). So RePark today behaves as if the flag were `false` for `/` and `%`, and
  `true` for `*`, `+` and `-`, whatever it is set to.
- **`spark.sql.decimalOperations.allowPrecisionLoss=false`**: RePark ignores it. Spark's lossless formula moves the
  wide results (division SQL ANSI on: 96 `V`, 53 `S` — Spark raises where the narrower result type cannot hold row
  10 — against 101 `V`, 0 `S` under the default); multiplication and addition at 38 digits differ too.
- `avg` is unchanged by either conf on both engines (every `avg` verdict equals the default run's).

## Other causes measured beside the family

| cell | Spark | RePark | belongs to |
|---|---|---|---|
| `avg(DISTINCT c10_2)` beside `count(c10_2)` | answers (`0.005000`, `0.026667`) | `This feature is not implemented: DistinctAvgAccumulator` | the `avg` seat (`accumulator` refuses `is_distinct`; DataFusion rewrites only an all-distinct aggregate list) |
| `avg(DISTINCT c10_2) OVER (…)` | `DISTINCT_WINDOW_FUNCTION_UNSUPPORTED` | `DistinctAvgAccumulator` not implemented | both refuse; class differs |
| `try_add(d, 1)`, `try_multiply(d, 3)`, `try_divide(d, 3)` | decimal | the decimal cast to `INT` | the `try_*` coercion |
| `d38_10 * 3` (and `* -3`) | `decimal(38,8)` | `Arrow error: Arithmetic overflow` | decimal multiplication at 38 digits |
| `summary()` with no statistics | answers | refuses (no percentile rows) | `summary`, disclosed |

## After the inventory critic (revision, 2026-11-17)

The critic (`GAPS`) contradicted no cell above. Its findings were re-measured here with `run_revision.py` on the
same tables, both engines, both ANSI modes (778 cells per run; raw in `evidence/raw/rev/`), and `mean` and the
new shapes were added. Letters as above.

| family (Spark door) | cells per mode | ANSI on | ANSI off |
|---|---|---|---|
| `mean` (SQL every `avg` position, `F.expr`, `selectExpr`, `agg` dict, Iceberg CTAS) | 470 | `=` 121, `t` 273, `V` 33, `R` 35, `B` 8 | `=` 121, `t` 273, `V` 33, `R` 43 |
| `avg` / `mean` `(DISTINCT x)` beside `count(x)` | 26 | `R` 13 (`avg`), `t` 13 (`mean`) | the same |
| shapes (grouping sets, `UNION ALL`, `HAVING` with a truncated constant, `avg(d / 3)`, folding) | 55 | `=` 14, `t` 12, `V` 11, `R` 5, `B` 13 | `=` 20, `t` 12, `V` 11, `R` 12 |
| `mod`, `pmod`, `%` (SQL and DataFrame), DataFrame `round(col / 3, 2)`, `F.expr` folding | 100 | `=` 8, `T` 83, `V` 5, `S` 2, `B` 2 | `=` 10, `T` 83, `V` 7 |
| `round(d, 1)`, `round(d / 3, 2)` | 20 | `=` 1, `T` 17, `B` 2 | `=` 1, `T` 19 |
| `try_add`, `try_subtract`, `try_multiply`, `try_divide`, `try_mod` | 100 | `T` 8, `V` 47, `R` 45 | the same |
| the sum past 38 digits (`s04`) | 3 | `=` 2, `B` 1 | `=` 2, `R` 1 |
| Iceberg `WHERE d / 3 = 0.836667` | 3 | `V` 2, `B` 1 | `V` 2, `=` 1 |

### M1 — `mean` is DataFusion's `Avg`, not RePark's averager

- **Every `mean` position truncates as `avg` does**: ungrouped (128 `t`, the same group grid), `GROUP BY`,
  `DISTINCT`, every window frame, `HAVING`, beside a constant, `agg(F.expr("mean(x)"))`, `selectExpr("mean(x)")`,
  `agg({x: "mean"})`, Iceberg CTAS (`c10_2` g9 stored `0.836666` against `0.836667`). The values agree with `avg`'s
  cells because DataFusion's `Avg` truncates the same way (`div_wrapping`), so the step 0 table could not tell the
  two accumulators apart.
- **The proof that it is another accumulator**: `mean(DISTINCT x), count(x)` **answers** (truncated: g13 `0.026666`
  against Spark's `0.026667`, all 13 columns), where `avg(DISTINCT x), count(x)` raises `DistinctAvgAccumulator`
  (13 columns, both modes). On the native door both answer, truncated (`n05`'s shape: `t` on `c10_2` and `c28_2`).
- `F.mean` and `GroupedData.mean` (step 0's DataFrame rows) go through `F.avg` and so reach RePark's averager;
  the SQL name `mean` does not.
- **Bare aggregates in a DataFrame `select`** (`select(F.expr("mean(x)"))`, `select(F.expr("avg(x)"))`, no
  `groupBy`) raise `This feature is not implemented: Physical plan does not support logical expression
  AggregateFunction(…)` where Spark answers (26 `R` per mode). The same refusal as FEXPR-COUNT-STAR-AGG-1 (M5);
  `selectExpr("avg(x)")` answers (truncated).

### Shapes (the critic's §4)

- `GROUPING SETS ((g), ())`, `ROLLUP(g)`, `CUBE(g)`, `UNION ALL` of two averages: `t` on `c10_2`, `c28_2`,
  `c38_18`, both modes (the grand-total row truncates too).
- **`HAVING avg(x) = <the truncated value>`**: Spark keeps the terminating groups only (g1, g3, g10); RePark keeps
  13 groups — the mirror of step 0's `having_eq`.
- **`avg(d / 3)` and `mean(d / 3)`**: the division's wrong type feeds the average. `d10_2`: Spark `decimal(18,10)`
  `3703704.4122221111`, RePark `decimal(25,17)` `3703704.41222222222221111`; at p ≥ 28 RePark raises `Arithmetic
  Overflow in AvgAccumulator` where Spark answers (`d28_2`, `d28_10` in both modes; ANSI off also `d34_4` and
  `d38_10`, where Spark answers NULL). Control
  `avg(d * 2)` agrees below p = 34.
- **Pins that agree (keep them agreeing)**: `d / (1 + 2)` is `decimal(21,13)` on both engines (SQL and `F.expr`):
  Spark folds `1 + 2` after its coercion, so the integer is not a literal when the type is picked; A2 must stay
  before `type_coercion` and leave a folded expression alone.
- Iceberg: `SELECT id FROM <iceberg table> WHERE d10_2 / 3 = 0.836667` keeps row 3 on Spark and nothing on RePark
  (`d10_2`, `d28_2`, both modes).

### M3 — `mod()`, `pmod()`, `%`

`d10_2`, Spark / RePark, both modes (values equal everywhere; `T` on all 10 columns of each form):

| cell | Spark | RePark |
|---|---|---|
| `d % 3` (SQL and `col(d) % 3`) | `decimal(3,2)` | `decimal(10,2)` |
| `mod(d, 3)`, `pmod(d, 3)`, `pmod(d, -3)` (SQL and `F.expr`) | `decimal(3,2)` | `decimal(12,2)` |
| `F.pmod(col(d), F.lit(3))` | `decimal(3,2)` | `decimal(11,2)` |
| `pmod(d28_2, 3)` | `decimal(3,2)` | `decimal(28,2)` |

On the native door `mod` and `pmod` do not resolve (`UNRESOLVED_ROUTINE`); `%` is `T`.

### M4 — `round`

`round(d10_2, 1)`: Spark `decimal(10,1)`, RePark `decimal(10,2)`; `round(d10_2 / 3, 2)`: Spark `decimal(11,2)`,
RePark `decimal(21,13)`; values equal (`T`). The DataFrame `round(col(d) / 3, 2)` is `double` on RePark (the
DataFrame `/` seat), `decimal(11,2)` on Spark, with `V` at p ≥ 20 (`0.1` against `0.10` is equal; wide rows lose
digits in `double`). Held by the existing card DECIMAL-ROUND-RESULT-TYPE-1 (S3).

### `try_*` with a decimal and an integer

Rows 1–9 (row 10, the column maximum, makes every `try_*` raise `Cast error: value of 9999999999 is out of range
Int32` on RePark: the `R` cells). Spark / RePark at `d10_2`:

| call | Spark | RePark |
|---|---|---|
| `try_add(d, 1)` | `decimal(11,2)`, row 3 `3.51` | `int`, `3` |
| `try_subtract(d, 1)` | `decimal(11,2)`, `1.51` | `int`, `1` |
| `try_multiply(d, 3)` | `decimal(12,2)`, `7.53` | `int`, `6` |
| `try_divide(d, 3)` | `decimal(14,6)`, `0.836667` | `double`, `0.6666666666666666` |
| `try_mod(d, 3)` | `decimal(3,2)`, `2.51` | `int`, `2` |

At scale 0 (`d10_0`, `d38_0`) the values agree and only the type differs (`T`, 8 cells). Every other width is `V`.

### The sum past 38 digits is not sticky

`s04` (`decimal(38,0)` rows `MAX38`, `MAX38`, `-MAX38`, `-MAX38`, `5`): `avg` and `mean` answer `1.0000` on both
engines, both modes — Spark's running sum passes 38 digits and comes back (`DecimalAddNoOverflowCheck` is an
unbounded `plus`; only the final `toPrecision` checks), and RePark's `i128` sum wraps and unwraps. Only a sum still
past 38 digits at the end overflows (`MAX38`, `MAX38`, `5`): ANSI on Spark `[ARITHMETIC_OVERFLOW] Overflow in sum
of decimals` (`B`), ANSI off Spark `NULL`, RePark raises (`R`). So A1's sum must be unbounded and checked once, at
`evaluate` (sketch §4).

## Exclusions and sampling

- Window frames run over one column order (`id`); `RANGE` by the decimal itself is not run.
- Spark's non-sticky sum is measured ungrouped, one partition; a grouped multi-partition sum past 38 digits that
  comes back is not run.
- The confs are sampled (above); Iceberg runs four `avg` and three division sources; the native door runs six
  `avg` and four division columns.
- `avg` over an interval is not run; `avg(d / 3)` was added in the revision.
- The wide round-up case (a large magnitude with ⅔) is not run; g21 and g23 cover wide magnitudes rounding down.

Up: [map.md](map.md).
