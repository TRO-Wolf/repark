# CAST-LITERAL-UNWRAP-1 — step 0: the measured sketch

**Date:** 2026-11-16. **Author:** the step-0 agent (`claude-opus-5-5`, effort high). **Measured on:** main
`83df6be1`'s package against Spark 4.1.2, ANSI off and on. Documents and probes only; no product code.
**Rulings carried:** [rulings-2026-11-16.md](rulings-2026-11-16.md) (S1, label-blocking, the order).

## 1. The fault

DataFusion's expression simplifier moves a cast from a column onto the constant it is compared with whenever the
constant converts exactly to the column's type, without asking whether the column's cast can lose information
([inventory.md](inventory.md) A1-A3). `CAST(c AS INT) = 1` over a `DECIMAL(10,2)` column runs as `c = 1.00`;
Spark truncates each value first and keeps 1.00, 1.25, 1.50 and 1.99. The cast kernel is right: compared with a
column holding the same constant, which nothing can unwrap, RePark answers every in-range cell as Spark does
([domain-table.md](domain-table.md#kernel-check)).

**Headline cells** (the temp view; every one repeats on Parquet, both Iceberg tables, the DataFrame API, `F.expr`,
`filter(str)` and the native door; full table in [domain-table.md](domain-table.md#headline-cells)):

| column, values | cell | Spark 4.1.2 | main |
|---|---|---|---|
| `DECIMAL(10,2)`: 0.50, 1.00, 1.25, 1.50, 1.99, … | `CAST(c AS INT) = 1` | 1.00, 1.25, 1.50, 1.99 | **1.00** |
| the same | `CAST(c AS DECIMAL(10,0)) = 1` | 0.50, 1.00, 1.25 | **1.00** |
| the same | `CAST(c AS DECIMAL(10,1)) = 1.3` | 1.25 | **no rows** |
| the same, Iceberg v2, copy-on-write and merge-on-read | `DELETE … WHERE CAST(c AS INT) > 1` | deletes 2.50 and 99999999.99 | **also deletes 1.25, 1.50, 1.99** |
| `BIGINT`: 1, …, 2147483648, 4294967297, … | `CAST(b AS INT) = 1` | ANSI off: 1 and 4294967297 (Spark wraps); ANSI on: raises `CAST_OVERFLOW` | **1, both modes, no raise** |
| `DECIMAL(10,3)`: 1.000, 9.994, 9.995, -9.995 | `CAST(c AS DECIMAL(3,2)) = 9.99` | ANSI off: 9.994; ANSI on: raises | **no rows, no raise** |

**Counts.** 3,702 RePark cells per ANSI mode (3,632 reads and 70 DML statements), 1,759 native-door cells, 232
kernel cells per engine and mode.

| ANSI | agree | both raise | wrong answer | wrong write | missed raise | over-raise |
|---|---|---|---|---|---|---|
| off | 1,610 | 0 | **1,320** | **38** | 0 | 734 |
| on | 1,491 | 492 | **711** | **28** | **738** | 242 |

Every wrong answer, wrong write and missed raise is in the four numeric families (1,642 cells per mode), except ten
DataFrame `datetime` cells excluded as a probe artifact ([domain-table.md](domain-table.md#exclusions)). Every
over-raise is a kernel refusal, not the unwrap (§6).

## 2. Reach, by reading and by measurement

- **Families (measured).** Reached: DECIMAL to every integer type; DECIMAL to a smaller scale; DECIMAL to a smaller
  precision; `BIGINT` to `INT` and `INT` to `SMALLINT`. Numeric cells per mode: ANSI off 1,310 wrong answers,
  38 wrong writes, 4 over-raises, 290 agree; ANSI on 701 wrong answers, 28 wrong writes, 738 missed raises,
  4 both raise, 171 agree. **Not reached:** FLOAT and DOUBLE sources (DataFusion does not unwrap floats,
  inventory A6), STRING sources (A6), `TIMESTAMP` / `TIMESTAMP_NTZ` to `DATE` (guarded, A7), and the three lossless
  controls: among their 2,060 cells per mode, no wrong answer or write beyond the ten excluded DataFrame
  `datetime` cells.
- **Operators and shapes.** Every operator, `<=>` included, on both sides; `IN`, `NOT IN`, `BETWEEN`, `NOT BETWEEN`;
  integer, decimal, string, typed-`CAST` and folding constants. A decimal constant against an integer cast
  (`CAST(c AS INT) = 1.0`) is unwrapped twice, through the coercion's widening cast.
- **Positions.** `WHERE`, the select list, `CASE WHEN`, `HAVING` and the cast term in `JOIN … ON`.
- **Doors.** Spark-door SQL, the DataFrame API (`==`, `>`, `lit ==`, `isin`, `between`, select), `F.expr`,
  `filter(str)` and the native door (338 wrong answers and 344 missed raises against Spark ANSI on).
- **Storage.** The temp view, Parquet, the Iceberg table written one row per file and the one partitioned on the cast
  column answer **identically**: 435 cells exist on all four, and none differs across storage on either engine.
  Pruning neither adds nor removes an error; the scans prune by the predicate already unwrapped (inventory C5).
- **DML (every wrong write named).** ANSI off, both copy-on-write and merge-on-read: `dec_int` `DELETE` `=` and `>`,
  `UPDATE`, `MERGE ON`, `MERGE WHEN MATCHED AND`; `dec_dec0` the same five; `dec_dec1` four (its `DELETE >` agrees);
  `bigint_int` the same five: 19 statements, 38 writes. ANSI on: `dec_int`, `dec_dec0` (five each) and `dec_dec1`
  (four), 28 writes; and `bigint_int`'s five commit rows where Spark raises `CAST_OVERFLOW` (10 missed raises).
  `ts_date` agrees; `str_int` and `str_date` refuse on RePark (kernel, §6).
- **ANSI on hides the raise.** Moving the cast onto the constant means the overflowing values are never cast, so
  Spark's `CAST_OVERFLOW` / `NUMERIC_VALUE_OUT_OF_RANGE` never happens: all 738 missed raises.
- **The kernel check.** With nothing to unwrap (a column holding the constant), RePark matches Spark on every
  in-range kind in both modes, and on every overflow kind with ANSI on (bar one cell each for `TINYINT` and
  `SMALLINT`, where Spark decides `> 127` by the type's top without casting). With ANSI off it refuses on every
  overflow kind where Spark wraps or answers NULL, and on STRING to `INT`, DECIMAL and `DATE`. **So a fix that
  keeps the kernel leaves exactly those as refusals** (owner question 2).

## 3. Seats

Two seats. Both must hold the fix **before the first `simplify_expressions`**, survive the signed-zero seat's second
simplifier pass ([inventory.md](inventory.md) D2) and survive DataFusion's physical simplifier at execution (B1-B5),
which today has nothing left to unwrap but would unwrap any narrowing `CastExpr` a fix kept.

### Seat W (recommended): a shield with a `preimage`, Fable's wrap

**What.** One core analyzer rule, appended to the shared list in `crates/repark-core/src/session/df_guards.rs`
(`analyzer_rules_with_df_54_1_rule_guards`), so it runs after `type_coercion` and after every door's own analyzer
rules, on the Spark door, the DataFrame API, `F.expr`, `filter(str)`, the native door and the DML predicate paths
alike. It finds every `Cast` / `TryCast` whose source type can lose information in the target type (the narrowing
predicate below) and that is an operand of a comparison (`=`, `<>`, `<`, `<=`, `>`, `>=`, `<=>`, `IS [NOT] DISTINCT
FROM`), an `IN` list or a `BETWEEN`, directly or under a chain of lossless casts (the coercion of `CAST(c AS INT) =
1.0` yields `CAST(CAST(c AS INT) AS DECIMAL(11,1)) = 1.0`, and the outer widening cast is unwrapped first). It
wraps that cast in an identity function, `__repark_cast_shield__(CAST(c AS T))`:

- **Evaluation does not change.** The function returns its argument; the `CastExpr` inside is the one main runs,
  so ANSI raises, NULLs and kernel defects stay exactly as they are (the kernel check says the kernel matches Spark
  in range; [§6](#6-side-observations) lists where it does not).
- **Nothing can unwrap it.** Neither the logical arm (A1-A4) nor the physical one (B1) matches a function call, and
  the Iceberg converter keeps it as a residual (C3).
- **Its `preimage` gives the exact range.** For `shield(CAST(x AS T)) <op> k` with `k` a non-NULL literal of `T`,
  `preimage` returns `[lo, hi)` in `x`'s type, and DataFusion's own preimage arm (A9) writes `x >= lo AND x < hi`
  (or the operator's half). That form is a bare column against literals, so Parquet statistics, page indexes,
  Iceberg manifests and file metrics all prune by it (B6, C1). The answers:
  - DECIMAL(p,s) to an integer type (truncation toward zero), `k > 0`: `[k, k+1)`; `k < 0`: `[k-1+u, k+u)`; `k = 0`:
    `[-1+u, 1)`, where `u` is one unit in the last place, `10^-s`;
  - DECIMAL(p,s) to DECIMAL(p2,s2), `s2 < s` (half up, away from zero), with `h = 5·10^-(s2+1)`: `k > 0`:
    `[k-h, k+h)`; `k < 0`: `[k-h+u, k+h+u)`; `k = 0`: `[-h+u, h)`;
  - each bound clipped to `x`'s range; a range that falls outside it gives the empty or the full answer as
    DataFusion's preimage arm already handles for an interval outside the column;
  - **`preimage` declines** (the shield stays per row) when the cast can overflow for some value of `x`'s type:
    integer to a narrower integer, `DECIMAL(p,s)` whose integer digits exceed the target's (`DECIMAL(10,2)` to
    `TINYINT`, `DECIMAL(38,0)` to `INT`, `DECIMAL(10,2)` to `DECIMAL(3,2)`). Spark raises there under ANSI and
    wraps or answers NULL without it, and no single range says either.
- **The narrowing predicate** (exact, by type): an integer to a narrower integer; a decimal to any integer type
  when `s > 0` or its integer digits exceed the target's; a decimal to a decimal with `s2 < s` or `p2 - s2 < p - s`.
  FLOAT and DOUBLE sources and targets, STRING, and timestamp-to-date casts are not wrapped: DataFusion does not
  unwrap them (A6, A7), and the measurement agrees.

**Costs.** A plan with no narrowing cast pays one `exists` walk over its expressions in the analyzer, and nothing
at execution. A shielded filter whose `preimage` answers runs as the range, as fast as main's wrong plan or
faster; one whose `preimage` declines evaluates the cast per row, as Spark does. #1339 measured the same mechanism
(`__repark_decimal_to_double__`, `crates/repark-functions/src/bool_decimal.rs:492`).

**Risks.** The shield must also cover `TryCast`; must not be inserted twice (the walk recognises its own name);
must keep the expression's display name (`NamePreserver`, as #1341's rules do) so a select-list column keeps
Spark's name; and the native door must register the function (`register_native_door_functions`) as the Spark door
does. A narrowing cast reached only through a function call (`abs(CAST(c AS INT)) = 1`) is not unwrapped by
DataFusion and needs no shield.

### Seat P: refuse the unwrap inside DataFusion (patched crates)

**What.** Patch DataFusion 54.1.0 at its three seats: the logical support tests
(`unwrap_cast.rs:107-185`), the physical `try_unwrap_cast_binary` (`simplifier/unwrap_cast.rs:58-140`) and a new
`casts.rs` helper `is_narrowing_cast(from, to)`, in the shape DataFusion 55 already uses for its two temporal
guards. Ship it as `[patch.crates-io]` copies of `datafusion-expr-common`, `datafusion-optimizer` and
`datafusion-physical-expr`, and send the same change upstream.

**For.** The root cause, at the one place every door, every simplifier pass (D1, D2) and every physical seat
(B1-B5) passes through; no plan-shape change; the smallest diff; it retires when upstream takes it.

**Against.** RePark builds DataFusion from crates.io today; three vendored crates are new maintenance on every
DataFusion bump until upstream merges, and an owner decision. The cast stays a per-row `CastExpr`: Parquet still
prunes through B6 (min/max cast, correct for a monotone cast), but the Iceberg converter refuses a narrowing
strip (C2), so `CAST(c AS INT) = 1` over a large Iceberg table becomes a full scan, slower than main's wrong plan.

### Cost cases a build must run

- **C1, no narrowing cast:** planning time of the like-for-like set (C-012) and a 200-predicate planning micro
  without casts, head over main, at most **1.05x**.
- **C2, a cast filter over a large table:** 10 million rows of `DECIMAL(10,2)` in Parquet, `WHERE CAST(c AS INT) =
  1` and `< 1`, head against main (wrong but fast) and against the hand-written range `c >= 1 AND c < 2`; head at
  most 1.05x of the hand-written range.
- **C3, pruning applies:** an Iceberg v2 table of 100 files partitioned or sorted on `c`, the same filters: files
  and row groups scanned on head equal those of the hand-written range (Seat W), counted from the scan metrics.
- **C4, the per-row fallback:** `BIGINT` to `INT` and `DECIMAL(10,2)` to `TINYINT` over 10 million rows; report
  the time beside main's, no bar (main's answer is wrong there).

### How each seat serves DECIMAL-COMMON-TYPE-1

That unit casts each operand to Spark's common decimal type before `type_coercion`; where a column is the operand
that loses digits (`DECIMAL(10,2)` beside `DECIMAL(38,0)` becomes `CAST(c AS DECIMAL(38,0))`, ruling 3) and the
other side is a constant, its cast is exactly this unit's narrowing cast.

- **Seat W** runs after `type_coercion`, so it sees that unit's casts as it sees a written one and shields them
  without a line of that unit's code; the constant cells then answer Spark's rounded comparison and prune by the
  exact range. That unit's step 0 must re-measure its constant cells on W's head.
- **Seat P** keeps that unit's casts in place too, but per row, with no Iceberg pruning.

## 4. The unit split

**Recommendation: one unit, numeric only** — DECIMAL to an integer, DECIMAL to a smaller scale or precision, and an
integer to a narrower integer.

- The four numeric families hold every unwrap wrong cell: 1,352 of 1,642 cells per mode disagree with ANSI off and
  1,467 with ANSI on, on every door, position, storage and DML verb.
- STRING and temporal casts have **zero** unwrap wrongs (DataFusion does not unwrap them). Splitting them off would
  carve out an empty unit. Their non-agreeing cells are kernel refusals: STRING 498 cells with ANSI off and 242 with
  ANSI on; FLOAT and DOUBLE edges 232 with ANSI off. They belong to kernel cards (owner question 3), not here.
- One seat serves the whole numeric family, so a numeric split (integers apart from decimals) buys nothing either.

## 5. Owner questions

1. **Seat W or seat P?** Default **W** (the shield with a `preimage`). It fixes every door and position by
   construction, keeps exact pruning on Parquet and Iceberg, and needs no vendored DataFusion. Rests on:
   inventory B1-B5, C1-C2, D1-D2; the 435 storage-identical cells; the headline cells.
2. **ANSI-off overflow after the fix.** With W (or P), the cast stays the kernel's, and with ANSI off the kernel
   refuses where Spark wraps or answers NULL: `CAST(b AS INT) = 1` over 4294967297 refuses instead of answering
   the wrong rows. Default: **accept the refusal in this unit** (no silent wrong answer remains) and card the kernel
   gap as ANSI-OFF-NARROWING-OVERFLOW-1 (S3, a refusal, not label-blocking; beside CAST-FLOAT-DECIMAL-OVERFLOW-NULL-1).
   Rests on: the kernel check, ANSI off, `bigint_int`, `int_smallint`, `dec_tinyint_overflow`,
   `dec38_int_overflow`, `dec_prec_overflow`, `dec_round_overflow` (0 of 8 held cells each).
3. **STRING casts on the Spark door.** `CAST(s AS INT)` over `' 1'`, `'01'`, `'1.0'`, `'+1'` refuses with an Arrow
   cast error where Spark answers, in both modes for DECIMAL and `DATE` targets and with ANSI off for `INT`; the
   native door refuses STRING to `DOUBLE` too; `DELETE`, `UPDATE` and `MERGE` with such a predicate refuse. Default:
   **card it as STRING-CAST-LENIENT-1, S2** (a refusal on an ordinary cast, not silent), **not label-blocking**.
   Rests on: `str_int`, `str_int_invalid`, `str_dec`, `str_date` (all doors), `str_double` (native only), the
   `str_int` / `str_date` DML rows.
4. **Spark's top-of-type comparison.** With ANSI on, Spark answers `CAST(c AS TINYINT) > 127` with no rows and
   no raise over values that overflow; with W the cast is evaluated and raises. Default: **leave it out of this
   unit and pin it as a known divergence**, carded S3. Rests on: `dec_tinyint_overflow` `c1/gt/r`, `c1/lt/l` and
   the DataFrame `col_gt` cells; `int_smallint` `c1/gt/r`, `c1/lt/l`; the kernel check's 7-of-8 rows.
5. **The cost bar.** Default: C1 at **1.05x** on planning for plans with no narrowing cast; C2 and C3 at 1.05x of
   the hand-written range; C4 reported without a bar. Rests on: §3's cost cases.
6. **The side observations** (§6). Default: card EXPLAIN-TABLE-RESOLVE-1 (S3) and PARQUET-APPEND-STRING-VIEW-1
   (S3); check the zone records for the DataFrame `datetime` literal and card it only if none covers it.

## 6. Side observations

1. **`EXPLAIN` does not resolve the table the `SELECT` reads (Fable's note): reproduced.** After `CREATE TABLE p USING
   parquet AS …` on the Spark door, `SELECT … FROM p` answers, but `EXPLAIN SELECT c FROM p` raises
   `table 'datafusion.public.p' not found`, `EXPLAIN SELECT c FROM default.p` raises `'datafusion.default.p' not
   found`, and only `spark_catalog.default.p` explains. `EXPLAIN EXTENDED` does not parse (`Expected: an SQL
   statement, found: EXTENDED`). Spark explains all five. Default: **card EXPLAIN-TABLE-RESOLVE-1, S3.**
   Evidence: [evidence/raw/explain-side-repark.txt](evidence/raw/explain-side-repark.txt),
   [evidence/raw/explain-side-spark.txt](evidence/raw/explain-side-spark.txt).
   - Seen in passing: that `CREATE TABLE … USING parquet` table is scanned by an `IcebergTableScan` on RePark and
     by a Parquet `FileScan` on Spark. Not chased.
2. **A second Parquet append of a cast string refuses.** `SELECT 1 AS id, CAST('1' AS STRING) AS c` appended twice
   to one path: the second append raises `cannot append column 'c': type mismatch source=string vs
   path=string_view`; a plain `'a'` literal and an `INT` append twice fine; Spark appends all three. Default:
   **card PARQUET-APPEND-STRING-VIEW-1, S3.** Evidence: [evidence/raw/append-side-repark.txt](evidence/raw/append-side-repark.txt),
   [evidence/raw/append-side-spark.txt](evidence/raw/append-side-spark.txt).
3. **Kernel refusals, not this unit** (they decide what the fix leaves; owner questions 2 and 3):
   - ANSI off, a narrowing numeric cast that overflows refuses (Arrow `out of range`, `too large to store`) where
     Spark wraps to an integer target (`CAST(4294967297 AS INT)` is 1, from `BIGINT` or `DECIMAL(38,0)`) or answers
     NULL for a decimal target;
   - ANSI off, `DOUBLE` NaN, ±Infinity and ±1e20 to `INT` refuse (Spark: 0, the type's top or bottom), and
     `DOUBLE` to a too-small DECIMAL refuses (already carded: CAST-FLOAT-DECIMAL-OVERFLOW-NULL-1 and
     CAST-INFINITY-TO-DECIMAL-NULL-1);
   - STRING to `INT`, DECIMAL and `DATE` refuse on the texts Spark trims or parses; the native door also refuses
     STRING to `DOUBLE`, which the Spark door answers.
4. **A DataFrame `datetime` literal.** `F.col("c").cast("timestamp") == datetime(2024, 1, 1)`: PySpark reads the naive
   `datetime` in the driver's zone (America/New_York here), RePark in the session zone (UTC), so the ten `date_ts`
   DataFrame cells per mode differ. Excluded from this unit's counts' meaning; see owner question 6.

## 7. What step 0 did not do

- No cost was measured; the cost cases above are for the build.
- The native door was measured on views only: it refuses local Parquet DDL and has no catalog configured here.
- Released versions (1.5.5, 1.5.6 on PyPI) were not measured; the code has been in DataFusion since before the
  54.1 pin, so they are expected to answer as main.
- Upstream DataFusion past 55.2.0 was not read (no network); a search of the 55.2.0 sources shows no narrowing
  guard.

Up: [map.md](map.md).
