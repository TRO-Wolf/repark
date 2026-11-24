# SUM-DEC-I128WRAP-1 — step 0 sketch (measured)

**Date:** 2026-11-17. **Builder:** Claude Opus 5.5 (`claude-opus-5-5`), step 0 only: documents and probes, no product
code. **Rulings carried:** owner, 2026-11-17 (evening), verbatim: "dispatch Opus xhigh effort levels for these
fixes, then use Opus 5.5 medium verifiers for the time and not muse, at least until these things are closed out",
after Fable's reply whose §7 found this overflow; that it dispatches this unit now is the orchestrator's reading,
not the owner's words. Owner, 2026-11-17, verbatim "Defaults, merge 1355": AVG-DECIMAL-ROUND-1 merged with its exact
running sum, which this unit builds on. The label ICE-V2-FLAT-AWS-LABEL-1 is scoped to ANSI on (ruling A1).
Registry row: `### SUM-DEC-I128WRAP-1` in [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
(backlog since 2026-09-10). Measured on main `6f80793b` against Spark 4.1.2:
[domain-table.md](domain-table.md); seats: [inventory.md](inventory.md); raw: [evidence/](evidence/).

**Revised 2026-11-17** by a fresh builder (Claude Opus 5.5, `claude-opus-5-5`) after the critic's verdict (Opus 5.5
medium, GAPS), every claim re-measured on the same main and Spark: the writer's bounds-only precision check (§1,
§5, Q5), the missing seats (inventory A-9, R-8 to R-12, I-5), the grouped marker (§4, Q2), the missed shapes and the
view cells (domain table §12), the cost list (§4), the shuffle-row writer (§2) and the observations (§8). §9 records
where the revision agrees and disagrees with the critic. Step 0's counts are unchanged; the revision's are counted
apart (§3).

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
   and merge-on-read): 8 wrong writes per mode. With default properties, 39-digit values are refused by the writer
   (`DataInvalid`), a different error, but a failed `INSERT` leaves them in uncommitted data files (2 per mode).
   **Corrected in the revision:** the writer checks precision only from min/max bounds (inventory I-3). Under
   `write.metadata.metrics.default` = `none` or `counts`, or past the 100th column of a table with no metrics
   property, the **39-digit values commit**: 14 more wrong writes per mode (domain table §12.3), and Spark 4.1.2
   then fails every scan that touches those rows with `NUMERIC_VALUE_OUT_OF_RANGE`, in both of its ANSI modes
   (§12.4). Iceberg's Java writer checks every value (inventory I-5).
3. **The sticky class.** Spark's grouped buffer checks the running sum after every row, and a partial at the
   shuffle; a group that passes 38 digits and comes back raises (on) or is NULL (off). RePark answers the exact
   value. The `sum` twin of AVG-GROUPED-SUM-STICKY-1.
4. **`try_sum` nulls sums Spark answers.** datafusion-spark's `try_sum` checks after every row in every path, so an
   ungrouped or whole-partition window sum that passes 38 digits and comes back is NULL where Spark answers the
   exact sum (20 cells per mode; the revision adds the ungrouped DataFrame `F.try_sum`, 4 per mode).
5. **Downstream of 1:** `HAVING sum(x) > 0` keeps or drops the wrong groups; `CAST(sum(x) AS STRING)` prints a
   39-digit value with 38 digits (10^38 as `'1' + 37 zeros`); `sum(x) + 1` raises `NUMERIC_VALUE_OUT_OF_RANGE` for
   a 39-digit sum and adds to a 38-digit wrap. The revision adds (domain table §12.1): `HAVING sum(x) < 0` keeps a
   wrapped group; a `ROLLUP`, `CUBE` or `GROUPING SETS` total passes 38 digits while every group fits and answers a
   39-digit total; `FILTER (WHERE …)`, `sum(CASE …)`, `sum(x * 1)`, a sum of a cast from `DOUBLE`, the mixed
   `DISTINCT`, an ungrouped `sum` over `UNION ALL`, and the DataFrame `pivot`, `rollup`, `cube`, `groupingSets` and
   `F.call_function("sum")` all answer the wrap or the 39-digit value.
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
    shuffle row (checked there), and the merged total is checked by `toPrecision` at evaluate. **Corrected in the
    revision:** that row is written by the generated unsafe projection, not by `UnsafeRow.setDecimal`.
    `GenerateUnsafeProjection$.writeElement` emits `<writer>.write(<ordinal>, <value>, <precision>, <scale>)` for a
    `DecimalType.Fixed` (constant pool `\u0001.write(\u0001, \u0001, \u0001, \u0001);`), and
    `codegen.UnsafeRowWriter.write(int, Decimal, int, int)` applies the same rule for precision > 18:
    `changePrecision(precision, scale)` (bytecode offset 87) fails → `BitSetMethods.set` (offset 103), the null bit
    ([evidence/jar/javap-revision.txt](evidence/jar/javap-revision.txt)). The behaviour is unchanged;
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

**The revision's cells** (domain table §12; counted apart, so the rows above are unchanged):

| | cells per mode | silent, ANSI on | silent, ANSI off | other (on / off) |
|---|---|---|---|---|
| missed shapes (grouping sets, totals, `pivot`, `FILTER`, `CASE`, `* 1`, `DOUBLE` cast, `HAVING < 0`, mixed `DISTINCT`, `UNION ALL`, `call_function`, ungrouped `F.try_sum`) | 60 | **52** | **52** | 0 / 0 (8 equal controls) |
| views across an ANSI flip (`sum` 7, `avg` 3) | 10 | 8 | 7 | 1 / 2 |
| Iceberg under `none` / `counts` / `full` and 100 / 101 columns | 22 statements | **14 committed** 39-digit values | **14 committed** | 8 / 8 refused |
| Spark reading RePark's tables (per Spark mode) | 176 queries over 44 tables | every scan touching the 28 bad tables' rows fails, in both Spark modes | | |
| seen beside, not this unit's | 7 | 0 | 2 | 7 / 5 |

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
- **where it checks**, mirroring §2 (owner question 2): the `GroupsAccumulator` checks after every row and is sticky
  per group (Spark's `UnsafeRow`), the row `Accumulator` (ungrouped) checks its partial at `state()` and the total
  at `evaluate`, and windows check at each `evaluate`, not sticky;
- **the overflow marker (revised; Q2).** Step 0 said the grouped check needed no state column. It does: RePark runs
  a grouped aggregate as Partial then FinalPartitioned (`explain/distinct_mixed`, `RoundRobinBatch(6)`, even over
  one file), and at 100,000 groups DataFusion skips partial aggregation, so every row becomes a state
  (`convert_to_state`). A group or partial that overflowed must reach the final step marked "overflowed", which
  DataFusion's one-column state (NULL = empty) cannot say. Two ways, and one lever:
  - a **sentinel** in the declared column (`i128::MIN`, never a valid 38-digit value) with NULL kept for "empty".
    It is an out-of-precision value, so the build must prove it survives spill (Arrow IPC), every
    `validate_decimal_precision` on the path, `RepartitionExec`, and the distributed codec (R-11), not assume it;
  - a **second state column** (Spark's `isEmpty`), measured 1.19x on avg's K1 for avg's third column when partial
    aggregation is skipped;
  - **with ANSI on, raise at once** at the first overflowing row (grouped) or partial (`state()`): the query outcome
    is Spark's (Spark evaluates every group, so any overflowed group raises `ARITHMETIC_OVERFLOW`), and the ANSI
    UDAF keeps a one-column state. Only the `Legacy` and `Try` UDAFs (ANSI off, `try_sum`) need the marker, and A3
    swaps the UDAF by mode at analysis, so each mode has its own state schema. The build measures K1 in both modes;
- **windows (revised).** RePark's `SlidingFrameReScan` (inventory A-9) re-sums every frame of an aggregate whose
  sliding accumulator does not retract. The seat must give **every** arm a sliding accumulator with
  `retract_batch` and `supports_retract_batch() == true`: the decimal arm (exact low part plus wrap count, so
  retraction stays exact), every delegated arm (`BIGINT`, `DOUBLE`, …, DataFusion's `SlidingSumAccumulator`
  behind it), and `try_sum`. Two reasons: the O(n·w) re-scan; and the measured rule order puts
  `sliding_frame_rescan` before `spark_avg_ansi_mode`, whose downcast skips a `WindowUDF`, so a re-scanned window
  would keep ANSI mode with ANSI off. A running frame uses the plain accumulator and calls `evaluate` once per row,
  so `evaluate` must not consume the state;
- **the doors (revised).** By name: the SQL door, `F.expr` / `selectExpr` (R-4) and the DataFrame `cube`, `rollup`,
  `groupingSets` (R-8, which lower to SQL text). By value: the `:938` arm (R-3), which also carries `pivot` (R-9)
  and `F.call_function("sum")` (R-10). The registration and the arm are both required;
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
a wrapped or 39-digit value, so the 8 committed wrong writes and the 2 orphan files go. The revision's shapes
(domain table §12.1) sit on the same accumulators and doors: grouping sets and their totals (A-3 per grouping
set, by name through R-8 on the DataFrame door), `FILTER`, `CASE`, `* 1`, the `DOUBLE` cast and `UNION ALL` (A-2,
A-3), `HAVING < 0` (A-3), the mixed `DISTINCT` (the UDAF's own distinct accumulator, replacing A-5), `pivot` and
`call_function` (the `:938` arm), and the ungrouped `F.try_sum` (A2). With them go the revision's 14 committed
39-digit writes per mode, because the writer is never handed such a value from `sum`; the writer's own hole (I-3)
stays for any other producer (§5). **Not covered:** the native door (R-5; owner question 3) and `F.sum_distinct`
(FNP-AGG-1).

**What the seat shares with `avg` (revised).** Share only the arithmetic: `WideDecimal` with `add_carry` /
`sub_carry` (`avg_decimal.rs:22-147`) and `add_batch` / `sub_batch` (`:319-357`). Do **not** share avg's
accumulators: `DecimalAvgAccumulator` (`:365-458`) and `DecimalAvgGroups` (`:467-688`) encode ruling (a) (one check
at the end) and a count-bearing state whose `state()` ships NULL for an overflowed partial, told apart from empty
by the count (`:424-436`). `sum` has no count and checks per row, so a shared struct would need a check-placement
mode, and without one either `avg` starts raising or nulling on a grouped pass-back (reversing ruling (a)) or `sum`
inherits ruling (a). Write `sum`'s accumulators as their own types, with the layout idea (`sums`, `wraps`,
`overflowed`, `GroupNullState`) copied, not called. The check is a new, smaller type (no division); the ANSI rule
needs one downcast arm. **Build gate:** avg's 330 cells per mode (domain table §8) stay byte-for-byte unchanged, both
modes, with the pins of AVG-DECIMAL-ROUND-1.

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
  state: DataFusion skips partial aggregation at this cardinality, so every row's state is shipped), **with the
  chosen marker, ANSI on and ANSI off** (revised: on, the one-column eager-raise state; off, the sentinel or the
  second column);
- **K2** a window `sum`, `ROWS BETWEEN 3 PRECEDING AND CURRENT ROW` (retract) and a running frame, **at `(10,2)` and
  `(38,0)`** (revised), plus a wide frame (`ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW`) whose time must scale
  with n, not n·w;
- **K2-R, no frame re-scanned** (revised). Measured: EXPLAIN cannot prove it. Today's `try_sum`, which
  `SlidingFrameReScan` does re-scan (it has no retract, and its sliding cells agree), prints `SAME TEXT AS ABOVE`
  after `sliding_frame_rescan` in EXPLAIN VERBOSE, and the same `BoundedWindowAggExec: wdw=[try_sum(…) ROWS …]`
  text as `sum` (`explain/rescan/*`). The proof is a test on the analyzed plan: for every arm (decimal at p 10 and
  38, `BIGINT`, `DOUBLE`, `try_sum`), the window function stays `WindowFunctionDefinition::AggregateUDF` and its
  sliding accumulator answers `supports_retract_batch() == true`; plus K2's wide frame;
- **K3** an ungrouped decimal `sum` over 10 million rows, at p 10 and p 38 (the vectorised batch path), **and at
  p 35 or 36** (revised: `safe_rows` is about 1,701 at p 35 and 170 at p 36, so these are the magnitude-bound
  mitigation's own path, where a batch of 8,192 rows splits). Avg's K3 `d38` row (1.026) does **not** bound this:
  its baseline was main's previous RePark `avg`, not DataFusion's vectorised `SumAccumulator`, and its `d38` data
  sat below `10^26` (`task/wo/avg-decimal-round-1/evidence/built/cost.md`);
- **K4** non-decimal controls: grouped and ungrouped `sum` of `BIGINT` and `DOUBLE` (delegated, must not move), and
  `sum(i + 1), sum(i + 2)` of an integer (the linear rewrite stays);
- **K5** (reported) grouped `try_sum` of a decimal (a `GroupsAccumulator` replaces one row accumulator per group);
- **C-012**, like for like.

## 5. The unit split

**Two units, and a writer card** (revision: the third class is the writer's, not `sum`'s). The decimal `sum` and
`try_sum` share one seat (A1, A2) and go together. The `BIGINT` ANSI overflow
shares only the registration (the same UDAF name) and has its own arithmetic (a checked `i64` add in `Ansi` mode,
Spark's "long overflow" text) and its own cost case on the hottest aggregate there is; folding it would put a
non-decimal cost risk in a decimal unit.

1. **SUM-DEC-I128WRAP-1** (A1 to A3): decimal `sum` and `try_sum` on the Spark door, both ANSI modes. Fixes 522 / 532
   silent `sum` cells, 20 `try_sum` over-nulls per mode, and these wrong writes: the committed wrapped value of
   `ice/ctas/wrap`, `ice/insert/wrap`, `ice/merge_cow/wrap`, `ice/merge_mor/wrap` at `c38_0` and `c38_38`, and the
   uncommitted 39-digit files of `ice/insert/all/c38_0`, `c38_38` (the writer is never handed such a value).
   Revision: also the 52 silent shape cells per mode and the 7 `sum` view cells (domain table §12.1, §12.2), and
   **the committed 39-digit writes** of `ice/{insert,ctas,merge_cow,merge_mor}/metrics_{none,counts}/c38_0`,
   `ice/{insert,ctas}/metrics_{none,counts}/c38_38` and `ice/{ctas,insert}/wide101/c38_0` (14 per mode), whose
   only measured source is `sum`.
2. **SUM-BIGINT-ANSI-OVERFLOW-1** (proposed card): the integer arm of the same UDAF in `Ansi` mode; 17 Spark-door
   cells and 15 native ones with ANSI on (`int/*/g42`, `g43`, `g44`, `intdf/F.sum`).
3. **ICE-DECIMAL-WRITE-PRECISION-1** (proposed card, revision): the fork's writer checks a decimal's precision only
   from min/max bounds (I-3), where Iceberg's Java writer checks every value (I-5). Its own small unit, in the
   fork's `parquet_writer.rs` or in RePark's write path before the batch reaches the fork, independent of unit 1:
   a per-value `validate_decimal_precision` of every `Decimal128` column (vectorised), whatever the metrics mode.
   Cells: the 14 committed writes above, the Spark cross-read (domain table §12.4), and the `full` and 100-column
   controls. Severity: Q5.

Not folded: the native door (owner question 3), `F.sum_distinct` (FNP-AGG-1), the orphan-file behaviour of a failed
Iceberg write in general (owner question 5), the BIGINT statistics assertion (DECIMAL-CAST-SELECTIVITY-1, reached without a cast) and
the decimal-to-string notation (domain table §10).

## 6. Owner questions (each with a default)

**Ruled** (owner, 2026-11-17, verbatim "defaults on both", adopting the default under each question below).

1. **Severity and label status.** **Ruled** ("defaults on both"; adopted: S1, label-blocking). Default: **S1, label-blocking**, by the class rule (a silent wrong answer on the
   Spark door with ANSI on): 522 cells where Spark raises and RePark answers a wrapped or 39-digit value
   (`sum/grouped/g6/c38_0`, `g7`, `ungrouped/g5`, `grouped_big/g31`, `g32`, every DataFrame form), 52 more shape
   cells from the revision (`rev/rollup_total/g1_4`, `rev/having_lt/g1_7`, `rev/union/g4_14`, `rev/df_pivot/g1_7`,
   …), and **the wrong writes, restated in the revision** (both modes):
   - **committed, wrapped into 38 digits** (any metrics mode), 8: `ice/{ctas,insert,merge_cow,merge_mor}/wrap/c38_0`
     and `…/c38_38`;
   - **committed, 39 digits** (revision), 14: `ice/{insert,ctas,merge_cow,merge_mor}/metrics_{none,counts}/c38_0`,
     `ice/{insert,ctas}/metrics_{none,counts}/c38_38` (the table property `write.metadata.metrics.default`), and
     `ice/{ctas,insert}/wide101/c38_0` (default properties, the sum in column 101). **Spark 4.1.2 cannot read these
     tables:** every scan touching such a row fails with `NUMERIC_VALUE_OUT_OF_RANGE`, with Spark's ANSI on or off
     (domain table §12.4);
   - **uncommitted orphan files with 39-digit values**, 2 (`ice/insert/all/c38_0`, `c38_38`; the revision's
     `metrics_full` and `wide100` `INSERT`s are the same case).
2. **The per-row grouped check against AVG-GROUPED-SUM-STICKY-1 (restated in the revision).** **Ruled** ("defaults on both"; adopted: option (i), follow Spark, raising at once with ANSI on; ANSI off and `try_sum` carry a marker, chosen by K1 at 1.05x or less; `avg` keeps ruling (a)). Spark checks a grouped
   sum after every row and is sticky; ruling (a) let `avg` answer the exact value instead. Step 0's default said this
   needed "an in-memory flag, no state column". **That premise was wrong:** a group that overflowed in a partial (or
   in a row state, when DataFusion skips partial aggregation at 100,000 groups) must reach the final step marked
   "overflowed", which is the same marker `avg`'s rejected option needed (sketch §4, A1). Options:
   - (i) **follow Spark, raising at once with ANSI on** (the grouped accumulator at the first overflowing row, the
     partial at `state()`): no marker with ANSI on, and the query outcome equals Spark's; ANSI off and `try_sum` carry
     a marker (a sentinel or a second state column, the build's K1 in both modes decides, at most 1.05x);
   - (ii) follow Spark with a marker in every mode (the same, plus the marker's cost with ANSI on);
   - (iii) ruling (a) for `sum` too: the 88 cells tagged `k` (ANSI on) stay (carded under AVG-GROUPED-SUM-STICKY-1,
     widened), and grouped `try_sum` of a pass-back group moves from equal to different (`try/grouped/g10`, `g11`).
   Default: **(i)**, `avg` keeping ruling (a). With ANSI on, `SELECT sum(x), avg(x) … GROUP BY` over a pass-back group
   then raises, as Spark does; with ANSI off, `sum` is NULL and `avg` exact (the carded `avg` difference only). If
   K1 cannot hold the ANSI-off marker at 1.05x, the build stops and reports, and the owner chooses between paying it
   and (iii) for ANSI off. Cells: `sum/grouped/g10`, `g11`, `grouped_s4/g12`, `mp/grouped_p2/g21`, `g24`,
   `grouped_big/g33`, `beside/g10`, `try/grouped/g10`, `g11`.
3. **The native door.** **Ruled** ("defaults on both"; adopted: card it at S2, not label-blocking, as NATIVE-DECIMAL-SUM-1). Its `sum` is DataFusion's and it has no `try_sum` (142 / 136 silent cells, 84 unresolved).
   Default: **card it, S2, not label-blocking** (the label is the Spark door's): NATIVE-DECIMAL-SUM-1, or a widening
   of NATIVE-DECIMAL-AVG-DIVIDE-1, the records clerk's call. Registering A1 on the native door later is one line in
   `register_native_door_functions`.
4. **`BIGINT` overflow with ANSI on** (17 cells, the same class rule). **Ruled** ("defaults on both"; adopted: a second unit, SUM-BIGINT-ANSI-OVERFLOW-1, S1). Default: **a second unit,
   SUM-BIGINT-ANSI-OVERFLOW-1, S1 by the same class rule**, after this one (§5).
5. **Orphan files, and the writer's bounds-only check (restated in the revision).** **Ruled** ("defaults on both"; adopted: (a) card at S3, not label-blocking; (b) ICE-DECIMAL-WRITE-PRECISION-1 at S1, label-blocking until a per-value check lands). Two classes:
   - (a) **orphans after a failed write** (`ice/insert/all/c38_0`, `c38_38`; revision `ice/insert/metrics_full/*`,
     `ice/insert/wide100/c38_0`): the writer checks after the file is written, so a refused `INSERT` leaves data files
     under the table holding the 39-digit values; the table is right. Default: **card it, S3, not label-blocking**.
   - (b) **ICE-DECIMAL-WRITE-PRECISION-1** (proposed, §5): under `none`, `counts`, or past column 100, the writer
     commits values past the column's precision, and the table becomes **unreadable by Spark**. Default: **S1, and
     label-blocking until a per-value check lands**, for three reasons. First, the damage outlasts the statement: a
     committed snapshot that the reference engine cannot scan blocks every later reader of the table (only
     file-pruned filters and metadata `count(*)` answer), and repairing it means rewriting or expiring snapshots,
     which is worse than one wrong answer. Second, Iceberg's Java writer refuses such a value per value (I-5), so
     this is a divergence from the reference writer, not only a missing guard. Third, unit 1 removes the one producer
     measured (`sum`), but no inventory shows it is the only one: any kernel that hands the writer an unvalidated
     `Decimal128` would commit the same way. The fix is small and independent of unit 1 (one vectorised precision
     check per decimal column per batch, with its own cost case). Alternative (the critic's): **S2, not
     label-blocking once unit 1 lands**, on the reading that no Spark-door route remains. Cells: domain table
     §12.3, §12.4.
6. **The ungrouped total past 38 digits across partitions (restated in the revision).** **Ruled** ("defaults on both"; adopted: mirror Spark's rule, `ARITHMETIC_OVERFLOW` for an overflowed partial or a single-phase aggregate, `NUMERIC_VALUE_OUT_OF_RANGE` for a merged total whose partials fit). Spark raises
   `NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION` when every partial fits and the merged total does not, and
   `ARITHMETIC_OVERFLOW` when a partial (or the only partition) overflows. The revision shows that this is a common
   shape, not only a multi-file read: an ungrouped `sum` over `UNION ALL` (`rev/union/g4_14`: two partials) and an
   ungrouped mixed `DISTINCT` (`rev/distinct_mixed_ungrouped/g5`) raise `NUMERIC_VALUE_OUT_OF_RANGE`; one leg that
   overflows by itself (`rev/union_partial/g5_14`) raises `ARITHMETIC_OVERFLOW`. Default (changed): **mirror Spark's
   rule**: `ARITHMETIC_OVERFLOW` for an overflowed partial and for a single-phase aggregate (no `merge_batch` call),
   `NUMERIC_VALUE_OUT_OF_RANGE` for a merged total whose partials all fit. By the measured layouts this gives Spark's
   condition on `ungrouped/g5` (one partition on both), `mp/ungrouped_p2/g22` (two files, two partials on both) and
   `rev/union/g4_14` (two scans); where RePark's partitioning differs from Spark's splits the condition can still
   differ, so the build pins only those layouts. Alternative (step 0's): `ARITHMETIC_OVERFLOW` at every aggregate
   position, the `NUMERIC_VALUE_OUT_OF_RANGE` cells an accepted difference. Windows raise `NUMERIC_VALUE_OUT_OF_RANGE`
   per row in both.

**No Q7 (views across an ANSI flip; revision).** `sum` adds nothing that TEMP-VIEW-SESSION-SETTINGS-1 does not
cover. Measured (domain table §12.2): `sum`'s views answer the wrap in every mode today (no ANSI arm), and `avg`,
whose ANSI arm A3 extends, already shows the card's class: created on and read off, Spark raises and RePark answers
NULL; created off and read on, Spark answers NULL and RePark raises; SQL and DataFrame-made views alike. The one
condition: A3 must live in the same analysis-time rule as `avg`'s (`SparkAvgAnsiMode`, renamed), reading the flag
from the analyzer config, so that the view unit's fix covers `sum` and `avg` at once. The cells `rev/view_flip/g5`,
`g7`, `rev/dfview_flip/g5`, `g7` and `rev/view_flip_avg/g5` are for that unit's table; the card measures SQL views
only, so the DataFrame-made views may be new to it.

## 7. What this step 0 did not settle

- Spark's `p2` and `s4` answers in the default pass are one-split answers; pass-and-come-back cells depend on row
  order and layout on both engines, so the build pins only single-batch, single-file cells for them.
- The marker for ANSI off and `try_sum` (a sentinel or a second state column, A1) is the build's measurement, as is
  the sentinel's survival through spill and the codec.
- Whether another Spark-door producer can hand the Iceberg writer an out-of-precision `Decimal128` (Q5 b) was not
  inventoried; the native door's Iceberg writes were not measured.
- The probes name the session zone in code (`lib.py`, copied from the avg unit's probes; the revision's probes
  reuse it and add no zone name).
- Settled in the revision: the mixed `DISTINCT` (A-5) is measured, silent.

## 8. Seen beside, not this unit's (revision)

Observations for the records clerk, each measured in both modes (domain table §12.5), with the card found by `grep`
of `task/roadmap/` and `docs/spark-sql-iceberg-parity.md`:

- **RePark has no SQL `PIVOT`** (`obs/pivot_sql/g1_7`: `Unsupported ast node Pivot`; Spark answers). No card found;
  PIVOT-COLUMN-ORDER-1 is the DataFrame pivot's column order.
- **A string-to-decimal cast past the precision raises an Arrow error in both modes** (`obs/caststr_col`,
  `obs/caststr_col_sum`; Spark raises `NUMERIC_VALUE_OUT_OF_RANGE` with ANSI on and answers NULL with it off). The
  literal form (`obs/caststr_lit`) fails in `simplify_expressions` at planning: DECIMAL-LITERAL-OVERFLOW-1. The column
  form has no exact card (STRING-CAST-LENIENT-1 covers unparsable texts).
- **`summary()` without arguments is unsupported** (`obs/summary/g5`): registry EX-DF-15.
- **`describe` prints `e37` where Spark prints `E37`** (`obs/describe/g5`, `g13`, ANSI off). No card found; the
  AVG-DECIMAL-ROUND-1 card counts 4 `describe` text cells, likely the same notation.
- RePark's `groupingSets(*cols)` takes columns and adds `()` itself, where PySpark 4.1 takes a list of grouping sets
  first (inventory R-8). Not measured as a cell; the API shape differs.

## 9. The revision against the critic (2026-11-17)

The critic (Opus 5.5 medium, verdict GAPS) was re-measured, not copied: every claim below ran on main and Spark in
this revision or was read at its cited lines.

| item | the critic said | this revision |
|---|---|---|
| I-3 and §6 | the writer refuses only with bounds; `none` / `counts` commit 39 digits; >100 columns by reading | **agree, measured**: 12 metrics cells and the 101-column pair commit, the 100-column and `full` controls refuse; Spark cannot read them in either Spark mode. Added: MERGE (copy-on-write, merge-on-read) commits too, and Java's writer checks per value (I-5) |
| Q5 default for the writer class | S2, not label-blocking once unit 1 lands | **disagree on the default**: S1, label-blocking until a per-value check lands (Q5 b); the critic's is the alternative |
| `SlidingFrameReScan` | missing seat; every arm must retract | **agree**, and two additions: the rule runs before the ANSI arm, whose downcast skips the swapped window; EXPLAIN cannot show the swap (measured) |
| `cube` / `rollup` / `groupingSets` by name, `pivot` and `call_function` by value | missing seats | **agree, measured**; `call_function` reaches `F.sum` because `_scalar("sum")` raises first (`route/scalar_sum`) |
| `repark-distributed`, `repark-connect` | note; no seat | **agree** (read) |
| Q2 premise | the grouped check needs a state marker | **agree**; added the eager raise with ANSI on, so only ANSI off and `try_sum` need one |
| missed shapes | silent | **agree, measured**: 52 of 60 per mode silent; 8 are equal controls |
| `UNION ALL` (Q6) | Spark raises `NUMERIC_VALUE_OUT_OF_RANGE`; accepted difference or mirror | **agree on the cell**; default changed to mirroring Spark's rule (Q6) |
| views | add Q7 | **disagree**: no Q7; `sum` adds nothing the view unit lacks once A3 shares `avg`'s rule; `avg` already shows the class (measured) |
| cost list | add K3 p 35/36, K2 at two precisions, an EXPLAIN proof, K1 with the marker; avg's 1.026 does not bound `sum` | **agree**, except the proof: EXPLAIN cannot show a re-scan (measured), so K2-R tests the analyzed plan |
| shared code | only `WideDecimal` and `add_batch` / `sub_batch`; gate on avg's 330 cells | **agree** (§4) |
| §2 `UnsafeRowWriter` | the shuffle row is written by `UnsafeRowWriter.write(int, Decimal, int, int)` | **agree**, read from the jar, with the emitting `GenerateUnsafeProjection$` constant |
| observations | four | **agree, measured**, with the cards found |
