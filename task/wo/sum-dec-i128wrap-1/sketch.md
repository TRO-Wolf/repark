# SUM-DEC-I128WRAP-1 — step 0 sketch (measured)

**Date:** 2026-11-17. **Builder:** Claude Opus 5.5 (`claude-opus-5-5`), step 0 only: documents and probes, no product
code. **Rulings carried:** owner, 2026-11-17 (evening), verbatim: "dispatch Opus xhigh effort levels for these
fixes, then use Opus 5.5 medium verifiers for the time and not muse, at least until these things are closed out",
after Fable's reply whose §7 found this overflow; that it dispatches this unit now is the orchestrator's reading,
not the owner's words. Owner, 2026-11-17, verbatim "Defaults, merge 1355": AVG-DECIMAL-ROUND-1 merged with its exact
running sum, which this unit builds on. The label ICE-V2-FLAT-AWS-LABEL-1 is scoped to ANSI on (ruling A1).
Registry row: `### SUM-DEC-I128WRAP-1` in [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
(backlog since 2026-09-10). Measured on main `6f80793b` against Spark 4.1.2:
[domain-table.md](domain-table.md); seats: [inventory.md](inventory.md); raw: [evidence/](evidence/map.md).

## 1. The faults

1. **A decimal `sum` past `decimal(38, s)` answers instead of raising**, on every Spark-door position, in both ANSI
   modes. DataFusion's `Sum` keeps a native `i128` sum, wraps silently, and never checks the result against its
   own declared type. Three shapes:
   - **past the type, inside `i128`:** a 39-digit value in a `decimal(38, s)` column (`M + 1` answers
     `100000000000000000000000000000000000000`; at scale 38, `1.000…0`);
   - **wrapped, 39 digits:** `M + M` answers `-140282366920938463463374607431768211458`;
   - **wrapped into 38 digits:** `3 × M` answers `-40282366920938463463374607431768211459`, a plausible value of the
     right type and the wrong sign and size.
   Spark raises `ARITHMETIC_OVERFLOW` ("Overflow in sum of decimals. Use 'try_sum' …") with ANSI on and answers
   NULL with ANSI off. Reached at p = 38 with two rows and at p = 34 with 10,001 rows; unreachable below 10^10 rows
   at p ≤ 28.
2. **The wrapped 38-digit value is committed to Iceberg** by `CTAS`, `INSERT … SELECT` and `MERGE` (copy-on-write
   and merge-on-read): 8 wrong writes per mode. 39-digit values are refused by the writer (`DataInvalid`), a
   different error, but a failed `INSERT` leaves them in uncommitted data files (2 per mode).
3. **The sticky class.** Spark's grouped buffer checks the running sum after every row, and a partial at the
   shuffle; a group that passes 38 digits and comes back raises (on) or is NULL (off). RePark answers the exact
   value. The `sum` twin of AVG-GROUPED-SUM-STICKY-1.
4. **`try_sum` nulls sums Spark answers.** datafusion-spark's `try_sum` checks after every row in every path, so an
   ungrouped or whole-partition window sum that passes 38 digits and comes back is NULL where Spark answers the
   exact sum (20 cells per mode).
5. **Downstream of 1:** `HAVING sum(x) > 0` keeps or drops the wrong groups; `CAST(sum(x) AS STRING)` prints a
   39-digit value with 38 digits (10^38 as `'1' + 37 zeros`); `sum(x) + 1` raises `NUMERIC_VALUE_OUT_OF_RANGE` for
   a 39-digit sum and adds to a 38-digit wrap.
6. **Beside, not this unit's unless a seat is shared:** a `BIGINT` `sum` wraps with ANSI on where Spark raises
   "long overflow" (17 cells); `F.sum_distinct` is missing (FNP-AGG-1); the native door has DataFusion's `sum` and
   no `try_sum`.

## 2. Spark's `Sum`, read from the 4.1.2 jar and measured

Read with `javap -c -p` from `spark-catalyst_2.13-4.1.2.jar` (classes unzipped into a scratch directory; excerpts
in [evidence/jar/javap-excerpts.txt](evidence/jar/javap-excerpts.txt)). Class
`org.apache.spark.sql.catalyst.expressions.aggregate.Sum(child, NumericEvalContext evalContext)`; `Sum(child)` takes
`NumericEvalContext.fromSQLConf(SQLConf.get)`, so the eval mode is the session's ANSI flag at analysis.

- **`resultType`**: `DecimalType(p, s)` → `DecimalType.bounded(p + 10, s)`; integral → `LongType`; the ANSI intervals
  keep their type; everything else `DoubleType`.
- **`shouldTrackIsEmpty`**: true for a decimal result; for an integral or ANSI-interval result only when
  `evalMode == TRY`.
- **Buffer** (`aggBufferAttributes`): `sum` (the result type) and, when tracked, `isEmpty` (boolean).
  `initialValues`: `sum = Literal.default(resultType)` (zero) and `isEmpty = true`; untracked, `sum = NULL`.
- **`add(left, right)`**: a decimal left operand → `DecimalAddNoOverflowCheck(left, right, left.dataType)`, whose
  `nullSafeEval` is `Numeric.plus` (unbounded); otherwise `Add(left, right, evalContext)`.
- **`updateExpressions`** (tracked, nullable child): `sum = If(child IS NULL, sum, add(sum,
  KnownNotNull(child).cast(resultType)))`, `isEmpty = isEmpty AND child IS NULL`. Untracked:
  `coalesce(add(coalesce(sum, zero), child.cast(resultType)), sum)`.
- **`mergeExpressions`** (tracked): `sum = If((NOT isEmpty.left AND sum.left IS NULL) OR (NOT isEmpty.right AND
  sum.right IS NULL), NULL, add(KnownNotNull(sum.left), KnownNotNull(sum.right)))`, `isEmpty = isEmpty.left AND
  isEmpty.right`. A NULL sum with a non-empty buffer means "overflowed".
- **`evaluateExpression`**, decimal: `If(isEmpty, NULL, CheckOverflowInSum(sum, resultType, nullOnOverflow = evalMode
  != ANSI, context))`; integral `TRY`: `If(isEmpty, NULL, sum)`; otherwise `sum`.
- **`CheckOverflowInSum.eval`**: a NULL sum → NULL when `nullOnOverflow`, else
  `QueryExecutionErrors.overflowInSumOfDecimalError(context, "try_sum")` (measured: `[ARITHMETIC_OVERFLOW] Overflow in
  sum of decimals. Use 'try_sum' to tolerate overflow and return NULL instead. … SQLSTATE: 22003`); otherwise
  `Decimal.toPrecision(p, s, ROUND_HALF_UP, nullOnOverflow, context)`, which raises
  `NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION` when the value does not fit.
- **`try_sum`**: `TrySumExpressionBuilder$.build` returns `Sum(child, NumericEvalContext(EvalMode.TRY))`; there is no
  `TrySum` class in 4.1.2. For a decimal, `TRY` differs from `LEGACY` only in being non-ANSI; for an integral sum it
  tracks `isEmpty` and `Add` in `TRY` mode returns NULL on overflow, which then sticks.
- **Where the check lands — `UnsafeRow.setDecimal(ordinal, value, precision)`**: for precision > 18, `value == null ||
  !value.changePrecision(precision, value.scale())` → `setNullAt`. So a sum held in an `UnsafeRow` becomes NULL the
  moment it leaves `decimal(38, s)`, and `DecimalAddNoOverflowCheck` keeps it NULL. Measured (domain table §3):
  - a **grouped** hash aggregate holds its buffer in an `UnsafeRow`: checked after every row, sticky;
  - an **ungrouped** partial holds it in generated-code variables: unchecked until the partial's value enters the
    shuffle row (checked there), and the merged total is checked by `toPrecision` at evaluate;
  - a **window** frame's buffer is not an `UnsafeRow`: each output row is checked at evaluate, not sticky;
  - so the outcome of a pass-and-come-back sum depends on the path, the row order and the partition layout, and an
    ungrouped total past 38 digits raises `ARITHMETIC_OVERFLOW` from one partition but
    `NUMERIC_VALUE_OUT_OF_RANGE` from two (`mp/ungrouped_p2/g22`).
- **Integral `sum`** uses `Add` with the eval context per row: ANSI raises `[ARITHMETIC_OVERFLOW] long overflow. Use
  'try_add' …` at the first overflowing row, even when the sum would come back (`int/grouped/g44`); `LEGACY` wraps.

The avg build's "per partial across partitions" reading stands, with one correction: its "two files" case ran as
one Spark split (small files share a split), so what it measured there was the per-row grouped check; the
per-partial check is measured here with real partitions (`mp/…`, 4 and 2 partitions).

## 3. Headline counts (Spark door; details in the domain table)

| | cells per mode | silent, ANSI on | silent, ANSI off | loud other (on / off) |
|---|---|---|---|---|
| decimal `sum` (12 columns, 14 groups, 6 positions, 12 DataFrame forms, Iceberg) | 1,656 | **522** (cells carrying X 90, W 70, w 53, k 88; window and all-group cells carry untyped tags) | **532** | 51 / 45 |
| decimal `try_sum` | 399 | 26 (20 over-nulls, 6 layout) | 28 (20, 8) | 0 |
| Iceberg wrong writes | 50 statements | 8 committed + 2 uncommitted files | the same | 10 |
| controls (`BIGINT` overflow) | 150 | 17 | 0 | 0 |
| native door decimal `sum` (sample) | 358 | 142 | 136 | 13 / 13 |

All silent decimal cells are at p ≥ 34; every cell at p 10, 18, 19, 28 agrees (bar the missing `F.sum_distinct`).

## 4. Seat proposals

### Seat A (recommended): a RePark `sum` / `try_sum` aggregate on the avg unit's exact sum

**A1 — the aggregate.** A `SparkSum` UDAF, named `sum`, in `aggregate::functions()` (registered after
`with_default_features` and datafusion-spark, so it replaces DataFusion's `sum` by name on the SQL door, `F.expr`
and `selectExpr`, R-1 and R-4), and the DataFrame arm `function_dispatch.rs:938` returns it (R-3, which builds the
UDAF by value). Its decimal arm reuses `avg_decimal.rs`:
- **accumulate** with `WideDecimal::add_carry` / `sub_carry` (native low part plus a wrap count, exact to `i192`),
  in update, merge and retract;
- **check** with a `SparkDecimalSum` sibling of `SparkDecimalAverage`: `|exact| < 10^min(38, p + 10)` → the value at
  `(min(38, p + 10), s)`; otherwise NULL (`null_on_overflow`) or `ARITHMETIC_OVERFLOW` with the `try_sum` hint (a
  `sum_overflow_error` parameterised by the hint; window rows raise `NUMERIC_VALUE_OUT_OF_RANGE`, as Spark's do);
- **where it checks**, mirroring §2 (owner question 2): the `GroupsAccumulator` checks after every row with an
  in-memory sticky flag per group (Spark's `UnsafeRow`), the row `Accumulator` (ungrouped) checks its partial at
  `state()` and the total at `evaluate`, and windows check at each `evaluate`;
- **state**: a partial that left the type must reach the final as "overflowed", which DataFusion's one-column state
  (NULL = empty) cannot say. Either Spark's shape (a second `isEmpty`/count column, measured 1.19x on avg's K1 for
  a third column when partial aggregation is skipped) or a sentinel in the declared column (`i128::MIN`, never a
  valid 38-digit value) with NULL kept for "empty". The build measures both and keeps the one under 1.05x;
- **non-decimal types** delegate to DataFusion's `Sum` unchanged in unit 1 (return type, accumulators, and
  `simplify_expr_op_literal`, so the integer linear rewrite of A-7 survives).

**A2 — `try_sum`.** The same UDAF in `Try` mode, named `try_sum`, registered over datafusion-spark's and returned by
`try_sum_udaf()` (`aggregate.rs:49`): NULL wherever `sum` would raise, with the same check locations. That fixes the
20 over-null cells (ungrouped, whole window) and keeps the grouped per-row NULL Spark has. It also gives `try_sum` a
`GroupsAccumulator` and a retract path, which it lacks today (T-3).

**A3 — the ANSI arm.** `SparkAvgAnsiMode` (`aggregate.rs:297-349`) gains a downcast arm for `SparkSum` (rename it
for both); ANSI off swaps `Ansi` for `Legacy` (NULL on overflow). The view-settings gap (F1) applies as it does to
`avg`.

**Covers** every position in the domain table: SQL ungrouped, grouped, all groups, empty, `s4`/`p2`/`big` partials,
`DISTINCT` (A-6 carries the UDAF through `SingleDistinctToGroupBy`; a mixed `DISTINCT` needs the UDAF's own distinct
accumulator, a set plus the exact sum), every window frame and `ParallelWindowExec`, `HAVING`, arithmetic over the
result, CTE, subquery, temp views, every DataFrame form, and every Iceberg source (I-1) — the store then never sees
a wrapped or 39-digit value, so the 8 committed wrong writes and the 2 orphan files go. **Not covered:** the native
door (R-5; owner question 3) and `F.sum_distinct` (FNP-AGG-1).

**Can `avg_decimal.rs` serve?** Yes for the arithmetic: `WideDecimal`, `add_batch` / `sub_batch` and the
`DecimalAvgGroups` layout (`sums`, `wraps`, `overflowed`, `GroupNullState`) carry over unchanged; the check is a
new, smaller type (no division). The ANSI rule needs one downcast arm. Two differences from `avg`: `avg`'s state has
a count to tell an overflowed partial from an empty one and `sum`'s has not (above); and `avg` checks once at the
end by ruling (a) while A1 checks per row in the grouped path (owner question 2).

**Cost risk.** `add_batch` uses Arrow's vectorised `sum` only while a batch cannot wrap (`safe_rows`, from the
input precision); at p = 38 `safe_rows` is 1, so every value goes through `add_carry` one at a time, where
DataFusion's `SumAccumulator` sums the batch vectorised. The ungrouped 10M-row p = 38 case may miss 1.05x on that
alone. Mitigation for the build: bound the batch by its measured magnitude (Arrow `min`/`max`, vectorised) instead
of the type's, so ordinary data keeps the vectorised path.

### Seat B (alternative): an analyzer rewrite to a wide sum and a check

**B1** — an analyzer rule rewrites a decimal `sum(x)` (and `try_sum`) to `__repark_spark_sum_check__(sum(CAST(x AS
decimal256(p + 10, s))))`, the scalar UDF narrowing to `decimal(min(38, p + 10), s)` with NULL or the Spark error;
at p ≤ 28 the cast is skipped (no wrap below 1.7·10^10 rows) and only the check is added. Trade-offs:
- an `i256` per-group sum cost the avg build K1 1.48x, so the p = 38 grouped case likely misses 1.05x;
- every decimal `sum` changes plan shape: the display name `sum(x)`, `HAVING` and `ORDER BY` references, window
  expressions (the check wraps each window column) and `DISTINCT` must survive the rewrite;
- it checks only the final value: no per-row grouped check, so the sticky cells stay (ruling (a) by construction),
  and it does not fix `try_sum`'s over-nulls unless `try_sum` gets the same rewrite;
- an analyzer rule must sit in every analyzer list (S-3: the session, `build_expr_context`), and the view family
  (F1) applies to it.

### One seat or two?

One accumulator decides every value (inventory: A-2 to A-5, T-2), so Seat A is one seat with two registrations
(by name, by value) and one ANSI arm. Seat B touches more plan surface for the same values at a higher cost.

### Cost cases a build must run (at most 1.05x on held rows)

- **K1** a grouped decimal `sum` over at least 100,000 groups, at `(10,2)` and `(38,0)` (the per-row check and the
  state: DataFusion skips partial aggregation at this cardinality, so every row's state is shipped);
- **K2** a window `sum`, `ROWS BETWEEN 3 PRECEDING AND CURRENT ROW` (retract) and a running frame;
- **K3** an ungrouped decimal `sum` over 10 million rows, at p 10 and p 38 (the vectorised batch path);
- **K4** non-decimal controls: grouped and ungrouped `sum` of `BIGINT` and `DOUBLE` (delegated, must not move), and
  `sum(i + 1), sum(i + 2)` of an integer (the linear rewrite stays);
- **K5** (reported) grouped `try_sum` of a decimal (a `GroupsAccumulator` replaces one row accumulator per group);
- **C-012**, like for like.

## 5. The unit split

**Two units.** The decimal `sum` and `try_sum` share one seat (A1, A2) and go together. The `BIGINT` ANSI overflow
shares only the registration (the same UDAF name) and has its own arithmetic (a checked `i64` add in `Ansi` mode,
Spark's "long overflow" text) and its own cost case on the hottest aggregate there is; folding it would put a
non-decimal cost risk in a decimal unit.

1. **SUM-DEC-I128WRAP-1** (A1 to A3): decimal `sum` and `try_sum` on the Spark door, both ANSI modes. Fixes 522 / 532
   silent `sum` cells, 20 `try_sum` over-nulls per mode, and these wrong writes: the committed wrapped value of
   `ice/ctas/wrap`, `ice/insert/wrap`, `ice/merge_cow/wrap`, `ice/merge_mor/wrap` at `c38_0` and `c38_38`, and the
   uncommitted 39-digit files of `ice/insert/all/c38_0`, `c38_38` (the writer is never handed such a value).
2. **SUM-BIGINT-ANSI-OVERFLOW-1** (proposed card): the integer arm of the same UDAF in `Ansi` mode; 17 Spark-door
   cells and 15 native ones with ANSI on (`int/*/g42`, `g43`, `g44`, `intdf/F.sum`).

Not folded: the native door (owner question 3), `F.sum_distinct` (FNP-AGG-1), the orphan-file behaviour of a failed
Iceberg write in general (owner question 5), the BIGINT statistics assertion (DECIMAL-CAST-SELECTIVITY-1, reached without a cast) and
the decimal-to-string notation (domain table §10).

## 6. Owner questions (each with a default)

1. **Severity and label status.** Default: **S1, label-blocking**, by the class rule (a silent wrong answer on the
   Spark door with ANSI on): 522 cells where Spark raises and RePark answers a wrapped or 39-digit value
   (`sum/grouped/g6/c38_0`, `g7`, `ungrouped/g5`, `grouped_big/g31`, `g32`, every DataFrame form), and 8 committed
   wrong writes per mode (`ice/*/wrap/c38_*`).
2. **The per-row grouped check against AVG-GROUPED-SUM-STICKY-1.** Spark checks a grouped sum after every row and is
   sticky; ruling (a) let `avg` answer the exact value instead. Default: **`sum` and `try_sum` follow Spark in the
   grouped accumulator** (an in-memory flag, no state column, so the reason (a) was chosen for `avg`, a 1.19x state
   cost, does not arise; and today's equal grouped `try_sum` cells stay equal), while `avg` keeps ruling (a). The
   alternative is ruling (a) for `sum` too: the 88 cells tagged `k` (ANSI on) stay (carded under AVG-GROUPED-SUM-STICKY-1, widened),
   and grouped `try_sum` of a pass-back group would move from equal to different (`try/grouped/g10`, `g11`). Cells:
   `sum/grouped/g10`, `g11`, `grouped_s4/g12`, `mp/grouped_p2/g21`, `g24`, `grouped_big/g33`, `beside/g10`.
3. **The native door.** Its `sum` is DataFusion's and it has no `try_sum` (142 / 136 silent cells, 84 unresolved).
   Default: **card it, S2, not label-blocking** (the label is the Spark door's): NATIVE-DECIMAL-SUM-1, or a widening
   of NATIVE-DECIMAL-AVG-DIVIDE-1, the records clerk's call. Registering A1 on the native door later is one line in
   `register_native_door_functions`.
4. **`BIGINT` overflow with ANSI on** (17 cells, the same class rule). Default: **a second unit,
   SUM-BIGINT-ANSI-OVERFLOW-1, S1 by the same class rule**, after this one (§5).
5. **Orphan files after a failed Iceberg write** (`ice/insert/all/c38_0`, `c38_38`: the writer checks statistics
   after the file is written; the table is right, the warehouse holds files with 39-digit values). Default: **card
   it, S3, not label-blocking**; after unit 1 no such value reaches the writer, but any other `DataInvalid` after a
   file is written would leave the same orphan.
6. **The ungrouped total past 38 digits across partitions** raises `NUMERIC_VALUE_OUT_OF_RANGE` on Spark, but
   `ARITHMETIC_OVERFLOW` from one partition. Default: RePark raises **`ARITHMETIC_OVERFLOW`** for every aggregate
   position and `NUMERIC_VALUE_OUT_OF_RANGE` for window rows; the two-partition ungrouped text is an accepted
   difference (`mp/ungrouped_p2/g22`).

## 7. What this step 0 did not settle

- A mixed `DISTINCT` (`sum(DISTINCT x)` beside an aggregate of another argument) reaches DataFusion's
  `DistinctSumAccumulator` (A-5); its wrap is read, not measured.
- Spark's `p2` and `s4` answers in the default pass are one-split answers; pass-and-come-back cells depend on row
  order and layout on both engines, so the build pins only single-batch, single-file cells for them.
- The sentinel versus a second state column (A1) is the build's measurement.
- The probes name the session zone in code (`lib.py`, copied from the avg unit's probes).
