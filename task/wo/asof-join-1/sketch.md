# ASOF-JOIN-1 — step 0: a measured design sketch

**What:** PySpark's as-of join, `DataFrame._joinAsOf`, and `pyspark.pandas.merge_asof` built on it, against RePark.
Not Iceberg time travel. **Asked by the owner on 2026-11-20** ("Yeah go ahead"); pandas was installed in the oracle
the same morning by the owner's approval, so `merge_asof` is measured too. **Measured** on main `9a48932f` (product
of `a4d8857f`) and Spark 4.1.2, session zone UTC, both ANSI modes (Spark's grid with ANSI on only, see §8); documents and probes only, nothing built.

**In one paragraph:** Spark lowers the call to a correlated scalar subquery of `min_by(struct(right.*), distance)`
over the right rows that pass the as-of condition, and the optimizer turns that into a Cartesian product of the
left side's distinct as-of values with the right side, an aggregate and a left outer join back. RePark has no door
at all: `df._joinAsOf` raises `ATTRIBUTE_NOT_SUPPORTED` and there is no `pyspark.pandas`. Two workarounds a RePark
user can write today give Spark's rows in every one of 756 grid cells up to the pick among tied rows, and
Spark's own pick among ties changes with input order and partitioning. The domain table has 3,523 main cells over 3,868 Spark records cells.
The proposed seat is a Rust plan builder that lowers the call to a sort-bound union and window fill (seat a), with
a dedicated sort-merge operator (seat b) held for later. Proposed rating **S3**.

## 1. Spark's rule, from the jar and from live runs

**The surface.** `left._joinAsOf(other, leftAsOfColumn, rightAsOfColumn, on=None, how=None, *, tolerance=None,
allowExactMatches=True, direction="backward")` (oracle `pyspark/sql/classic/dataframe.py:754`; the Connect client
builds `plan.AsOfJoin`, `pyspark/sql/connect/dataframe.py:752`). It calls the JVM `Dataset.joinAsOf`. Python
asserts `how` is a string and `tolerance` a `Column` (`AssertionError tolerance should be Column` for a bare `2`);
`how=None` means `inner`; `on` as a string or list of strings goes to the `usingColumns` overload, `on` as a
Column or list of Columns is `&`-reduced. `on=[]` fails in Python (`IndexError`). There is no public PySpark name;
Scala has `Dataset.joinAsOf`; SQL has none (the lexer has no `ASOF` token; §1.8).

**1.1 The condition** (`AsOfJoin$.makeAsOfCond`, read from bytecode; `l`, `r` are the as-of columns, `t` the
tolerance):

| direction | `allowExactMatches=True` | `allowExactMatches=False` |
|---|---|---|
| backward | `l >= r`, with `t`: `AND r >= l - t` | `l > r`, with `t`: `AND r > l - t` |
| forward | `l <= r`, with `t`: `AND r <= l + t` | `l < r`, with `t`: `AND r < l + t` |
| nearest | `TRUE`, with `t`: `r >= l - t AND r <= l + t` | `NOT (l = r)`, with `t`: `AND r > l - t AND r < l + t` |

**The tolerance bound is inclusive only when `allowExactMatches=True`;** with `False` the bound is exclusive too.
Measured on the grid: a right row exactly `t` away matches with `True` and does not with `False`, in every type.

**1.2 The pick** (`makeOrderingExpr`): the distance is `l - r` (backward), `r - l` (forward), or
`IF(l > r, l - r, r - l)` (nearest), and the right row with the smallest distance wins. `RewriteAsOfJoin` builds
`Project(left.output :+ ScalarSubquery(Aggregate([], [min_by(struct(right.*), distance)], Filter(cond AND on,
right))))`; for `inner` it adds `Filter(IsNotNull(__right__))`; then each right column is `GetStructField` of the
struct. `min_by` skips NULL distances.

**1.3 Ties.** Two right rows at the same distance (a duplicate right as-of value, or a `nearest` tie between one
row before and one after) go to whichever row `min_by` meets first, and that depends on the physical plan. Measured
(`T` group, 2 left rows against 5 right rows with three duplicates at one value and a `nearest` tie): Spark's pick
changes with the right side's input order (as given, reversed, rotated) and with its partitioning and the shuffle
partition count, and repeating the same plan in the same session gave the same pick in 72 of 72 configurations. On the grid Spark's `nearest` tie went to the row
after 238 times and before 140 times. **Spark has no tie rule a user can rely on.** pandas'
own `merge_asof` takes the backward row on a `nearest` tie; pyspark.pandas does not, because it lowers to
`_joinAsOf` (§1.7).

**1.4 Keys and NULLs.** String `on` builds `left(c) === right(c)` per name (`classic.Dataset` `$anonfun$joinAsOf$1`):
**not null-safe**, so a NULL key never matches; `on=left.k1.eqNullSafe(right.k1)` does match NULL keys (measured, `C`
group). A NULL as-of value on either side never matches (the condition is NULL, and the distance is NULL, which
`min_by` skips), so `left` keeps the row with NULL right columns and `inner` drops it. `on=None` matches on the as-of
column alone, across all keys. A non-equality Column `on` (`left.k1 != right.k1`) is allowed.

**1.5 Types and errors** (`E` and `M` groups; [domain-table.md](domain-table.md) §3 to §4):
- The as-of column may be any type for which `>=` and `-` resolve: `TIMESTAMP`, `TIMESTAMP_NTZ`, `DATE`, `BIGINT`,
  `INT`, `DOUBLE`, `DECIMAL` all answer. `STRING` and `BOOLEAN` raise `DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE` (the
  distance subtracts them).
- The tolerance: an integer for integers and `DATE` (days), a day-time or year-month interval for timestamps and
  dates; an integer on a timestamp raises `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`; a `DOUBLE` on a `DATE` raises the
  same; a `DOUBLE` on a `BIGINT`, a `DECIMAL` on a `DOUBLE` and a string literal `"2"` on a `BIGINT` all answer.
- A negative tolerance and a NULL tolerance raise `AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE` (the assertion
  `t >= default(type)` is NULL, which fails the check); zero is allowed. A non-foldable tolerance (a column) raises
  `INTERNAL_ERROR` (Spark evaluates it before the foldable check).
- `direction` is lower-cased (`"BACKWARD"`, `"Nearest"` answer); anything else raises
  `AS_OF_JOIN.UNSUPPORTED_DIRECTION`; `None` raises a JVM `NullPointerException`.
- `how`: `inner` and `left` (and their spellings `left_outer`, `leftouter`, any case) answer; `right`, `full`,
  `outer`, `cross`, `semi`, `anti` raise `IllegalArgumentException requirement failed: Unsupported as-of join type
  <RightOuter|…>`; an unknown name raises `UNSUPPORTED_JOIN_TYPE`.
- A missing as-of or key column raises `UNRESOLVED_COLUMN.WITH_SUGGESTION`. `allowExactMatches="yes"` fails in Py4J.
- Mixed types across the sides follow the binary-operator coercion: `INT`/`BIGINT`, `INT`/`DOUBLE`,
  `BIGINT`/`DECIMAL`, `DOUBLE`/`DECIMAL` answer; DATE>BIGINT AnalysisException DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES on Spark; every other measured pair answers.

**1.6 ANSI.** The distance and the tolerance arithmetic follow the ANSI mode. With as-of values near the integer
bounds (`O` group) ANSI on raises `ARITHMETIC_OVERFLOW` from the distance even where a closer row exists; ANSI off
wraps, so ANSI on raises in 20 of 24 bound cells and ANSI off in 0. Off the bounds, by mode: 8 cells outside the bounds group differ by mode: E 8).

**1.7 The output.** `left.*` then `right.*` in order, every column kept, so `on="k1"` yields two `k1` columns and
`select("k1")` raises `AMBIGUOUS_REFERENCE`; the right columns are nullable for `left`. A self join works
(`resolveSelfJoinCondition`).

**`pyspark.pandas.merge_asof`** (oracle `pyspark/pandas/namespace.py:3063`, measured in both modes, `P` group):
it resolves `on` or `left_on`/`right_on` (one as-of column each; a second raises `can only asof on a key`), `by` or
`left_by`/`right_by` (equal length; one alone raises `missing right_by`/`missing left_by`), prefixes the right side's
columns, wraps a non-Column tolerance in `F.lit`, and calls `left._joinAsOf(..., how="left")`. The output keeps one
copy of each `on`/`by` column named on both sides and suffixes every other shared name (`_x`, `_y` by default).
**In all 114 of 114 comparable cells per mode it gave the same left-to-right matches as the `_joinAsOf` call it
lowers to, ties included.** Its errors are Python `ValueError`s for argument shape, and Spark's own
(`AS_OF_JOIN.TOLERANCE_IS_NON_NEGATIVE`, `AS_OF_JOIN.UNSUPPORTED_DIRECTION`) for values; a string tolerance `"2"`
answers. It does not fail under ANSI on (`compute.fail_on_ansi_mode` untouched).

**1.8 SQL.** No `ASOF JOIN`: `l ASOF JOIN r ON …` parses `ASOF` as a table alias of `l` and runs a plain inner join;
Snowflake's `ASOF JOIN r MATCH_CONDITION (…)` and `LEFT ASOF JOIN` raise `PARSE_SYNTAX_ERROR`.

**1.9 The plan** ([evidence/raw/explain-spark.txt](evidence/raw/explain-spark.txt), one per direction, with and
without keys). The decorrelated subquery becomes: the left side's **distinct as-of values** `CartesianProduct` the
right rows under the as-of condition (the key equality is not in this join; it moves to the aggregate's grouping and
to the final join), a `SortAggregate` of `min_by` grouped by that value and the right keys, and a `SortMergeJoin
LeftOuter` back to the left rows on `t <=> t AND keys`. **The work is quadratic in the number of distinct left as-of
values times the right rows, keys or no keys** (§5.4).

## 2. RePark today

- **No door.** `df._joinAsOf` reaches `DataFrame.__getattr__` (`core.py:1571`) and raises `PySparkAttributeError
  [ATTRIBUTE_NOT_SUPPORTED]`. No Rust function, logical node, rule or operator names an as-of join; DataFusion 54.1.0
  has none ([inventory.md](inventory.md) §3). `pandas_api` refuses (`[NOT_IMPLEMENTED]`) and there is no
  `pyspark.pandas` module, so `merge_asof` has no door either.
- **SQL.** `l ASOF JOIN r ON …` raises `ParserError("Expected: MATCH_CONDITION …")` (sqlparser 0.62 reads `ASOF` as
  Snowflake's keyword, not an alias; Spark answers); Snowflake's spelling parses and then raises `Unsupported JOIN
  operator AsOf`.
- **What a rewrite could use:** hash and nested-loop joins with a band filter, `row_number`, `first`/`last` with
  `ignorenulls` over bounded frames, `unionByName`. **Not available:** `min_by`/`max_by` (`UNRESOLVED_ROUTINE` in SQL
  and `F.expr`, no `F` function), `monotonically_increasing_id` (refused, R-FN-BATCH4), and the correlated
  non-equality scalar subquery Spark's rewrite produces (`Optimizer rule 'scalar_subquery_to_join' failed caused by
  Schema error`, `S` group) — so RePark cannot copy Spark's plan as it stands.
- **Workaround W1** (`probes/asof.py` `w1`): rename the right columns, join on the as-of condition plus key
  equality plus `IS NOT NULL` on both as-of columns, then keep `row_number() = 1` per left key ordered by the
  distance. Needs a unique left key (none can be minted without `monotonically_increasing_id`).
- **Workaround W2** (`w2`): union the left rows and the right rows (right rows with a NULL as-of value or key
  dropped), fill the nearest right row's index and as-of value with `last(..., ignorenulls)` over the preceding rows
  and `first(..., ignorenulls)` over the following rows per key, with a side order that puts equal as-of values on
  the right side of the bound for `allowExactMatches`, apply the tolerance and the `nearest` choice, and equi-join
  back to the right row. Sort-bound; no Cartesian product.

## 3. The domain in short

[domain-table.md](domain-table.md) has every count; the totals:

| pair | cells | verdicts |
|---|---|---|
| main-D | 1280 | MISSING 1280 |
| main-W1 | 1116 | EQUAL 318, TIE 710, WRONG 56, RAISE 32 |
| main-W2 | 972 | EQUAL 564, TIE 368, WRONG 36, RAISE 4 |
| main-P | 135 | MISSING 135 |
| main-SQL | 14 | EQUAL 4, RAISE 10 |
| main-F.expr | 2 | RAISE 2 |
| main-F | 2 | RAISE 2 |
| main-DataFrame | 2 | RAISE 2 |
| **all main cells** | **3523** | the direct call 1280, the workarounds 2088, `merge_asof` 135, SQL and helpers 20 |

Spark records 3868 and main records 5692 (every surface, both modes, `EXPLAIN` included). Groups by main cells: C 26, E 138, G 2268, M 288, N 72, O 144, P 135, S 20, T 432.

- **The model.** `probes/model.py` (the rule of §1.1 to §1.4 in plain Python) agrees with Spark's `_joinAsOf` on
  960 of 960 grid and tie cells: every left row present once, its right row inside the tie set.
- **The workarounds on Spark itself** match Spark's `_joinAsOf` in every grid cell up to ties (W1 EQUAL 436, TIE 320; W2 EQUAL 476, TIE 280), so a
  difference on main is the engine's, not the formulation's.
- **On main**, W1 and W2 run in every grid cell of all seven types and both modes: W1 EQUAL 210, TIE 546; W2 EQUAL 476, TIE 280. W2 is
  deterministic and equals W2 on Spark in every grid cell (EQUAL 756).
- Off the grid on main: mixed types W1 EQUAL 24, TIE 60, WRONG 32, RAISE 28; integer bounds W1 EQUAL 28, WRONG 16, RAISE 4, W2 EQUAL 16, WRONG 28, RAISE 4; NaN and infinities W1 EQUAL 16, WRONG 8, W2 EQUAL 16, WRONG 8; ties W1 EQUAL 40, TIE 104, W2 EQUAL 56, TIE 88. The same workarounds on Spark: mixed W1 EQUAL 136, WRONG 8; bounds W1 EQUAL 32, WRONG 16, W2 EQUAL 18, WRONG 30; NaN W1 EQUAL 20, WRONG 4, W2 EQUAL 16, WRONG 8 ([domain-table.md](domain-table.md) §4 to §6 and §11).

## 4. What a seat must get right (from the cells)

1. The condition table of §1.1, including the exclusive tolerance bound under `allowExactMatches=False`.
2. Non-null-safe keys from string `on`; Column `on` as given (null-safe or non-equality allowed).
3. NULL as-of values never match; `inner` drops, `left` keeps.
4. The output: `left.*` then `right.*`, both key columns, right side nullable for `left`.
5. The error conditions of §1.5 at analysis time (before any row is read), including the join-type requirement and
   the tolerance assertion.
6. ANSI arithmetic in the distance and the tolerance (Q7).
7. A tie rule (Q4): Spark has none; the seat should be deterministic and inside the tie set.

## 5. Seats

Both seats are Rust-first: the facade method is a thin adapter (argument shapes and the PySpark asserts) over one
`repark._native` call; the rule, the validation, the plan and the errors live in `repark-core`. Neither touches an
existing join, rule or operator path, so **C-012 is untouched** (no rule runs on a plan without an as-of node, and
the existing `join`/`crossJoin` code is not edited).

### 5.1 Seat (a) — a Rust plan builder that lowers to the union and window fill (default)

`repark_core::session::df_guards::asof_join::plan(left, right, spec) -> LogicalPlan` builds W2's shape natively:
project both sides to (keys, as-of value, side, payload), `UNION ALL`, two `last_value(... IGNORE NULLS)` window
passes (ascending for the preceding row, descending for the following row, so both stream), the tolerance and
`nearest` projection, and the right payload carried as a struct (no join back). The validation of §1.5 runs first.

- **Product files (cap 6):** (1) `python/repark/src/repark/spark/dataframe/asof_join.py` (new adapter); (2)
  `dataframe/core.py` (one line-neutral binding, with `scripts/check_lib_py.py`'s row ratcheted down if lines are
  folded); (3) `crates/repark-python/src/dataframe/mod.rs` (one `#[pymethods]` entry, 816 → about 850 of 1000);
  (4) `crates/repark-core/src/session/df_guards/asof_join.rs` (new: spec, validation, plan); (5)
  `crates/repark-core/src/session/df_guards.rs` (one `mod` line). Tests and records outside the cap: a facade test
  file pinning the domain table's cells, the `dir(DataFrame)` snapshot (`_dfcore_1_expected.py:383`), a Rust unit
  test file, the registry row, the touched `map.md` rows.
- **Cells moved:** every direct cell Spark answers or raises with a named condition, MISSING → EQUAL, with tied
  cells EQUAL to the tie set (Q4): 1280 direct cells (1170 where Spark answers, 110 where it raises). `merge_asof` stays MISSING (Q3).
- **Memory:** one sort of `n + m` rows per window pass (DataFusion's external sort spills); the windows stream
  (`BoundedWindowAggExec`, preceding-only frames); peak measured for W2 today: 0.62 GB (no keys), 0.99 GB (keys).
- **`EXPLAIN`:** `SortExec` → `BoundedWindowAggExec` (preceding fill) → `SortExec` (descending) →
  `BoundedWindowAggExec` (following fill) → `ProjectionExec` (tolerance, pick) → `FilterExec` (left side; `inner`).
  W2's plan today is in [evidence/raw/explain-main.txt](evidence/raw/explain-main.txt).
- **Cost:** §5.4; about W2's time, which is sort-bound.

### 5.2 Seat (b) — a dedicated sort-merge `AsOfJoinExec`

A `UserDefinedLogicalNodeCore` `AsOfJoinNode` (two inputs, the spec) like `UnpivotNode` (`stack.rs:115`), planned
by one more downcast in `StackExtensionPlanner::plan_extension` (`stack/planner.rs:36`) into `AsOfJoinExec`:
`required_input_ordering` = (keys, as-of value) on both inputs, hash-partitioned by the keys; a cursor walk keeps the
last right row at or before each left row and the next one after it, applies the tolerance and the `nearest`
choice, and emits the left row with its pick.

- **Product files (cap 8):** seat (a)'s (1) to (3), plus (4) `crates/repark-core/src/stack/asof_join.rs` (the node),
  (5) `crates/repark-core/src/stack/asof_exec.rs` (the operator), (6) `stack/planner.rs` (the downcast), (7)
  `crates/repark-core/src/stack.rs` (`mod` lines), (8) the validation module.
- **Cells moved:** the same as seat (a).
- **Memory:** the two input sorts (spillable) plus, per key, the run of right rows at one as-of value (the
  duplicates); no window state, no union.
- **`EXPLAIN`:** `AsOfJoinExec` over two `SortExec`s over `RepartitionExec` by the keys.
- **Cost:** below W2 (two sorts of `n` rows instead of two sorts of `n + m`, no join back); §5.4.
- **Trade-off against (a):** about twice the code, a new operator to review (spill, batch boundaries, partition
  edges), and a tie rule written by hand; it pays only if W2's time is not good enough.

### 5.3 Rejected: Spark's own shape

Lowering to a range join plus an aggregate or `row_number` (W1's shape, or Spark's `min_by` plan) is quadratic
without keys and needs `min_by` or a minted row id; Spark's correlated subquery fails in RePark today (§2).

### 5.4 Cost (2M left rows × 2M right rows; ANSI on; `probes/cost.py`)

Data: `spark.range(2M)`; left `t = 3i`, right `t = 3i + 1`; **with keys** 200,000 string keys (10 rows a key a side),
backward, no tolerance; **without keys** backward with tolerance 2 (one match a row). Each sample is one process
under the exclusive measure lock on cores 32–47; the action is `count` and `sum(rid)` over the result. Spark runs
`local[16]`, 16 shuffle partitions, 16 GB driver.

| engine | surface | rows a side | keys | seconds per sample | median | peak RSS | count, sum(rid) |
|---|---|---|---|---|---|---|---|
| main | W1 | 2,000,000 | yes | 0.857, 0.693, 0.667 | 0.693 | 1.72 GB | 2000000 1619999100000  |
| main | W2 | 2,000,000 | yes | 0.629, 0.497, 0.483 | 0.497 | 0.99 GB | 2000000 1619999100000  |
| main | W2 | 2,000,000 | no (tolerance 2) | 2.162, 2.04, 1.953 | 2.04 | 0.62 GB | 2000000 1999997000001  |
| main | W2 | 2,000,000 | no (tolerance 2) | 2.141, 1.986, 2.012 | 2.012 | 0.62 GB | 2000000 1999997000001 repeat |
| main | W1 | 20,000 | no (tolerance 2) | 0.885, 0.821 | 0.885 | 0.3 GB | 20000 199970001  |
| main | W1 | 40,000 | no (tolerance 2) | 3.051 | 3.051 | 0.33 GB | 40000 799940001  |
| Spark | D | 20,000 | yes | 75.654, 71.15 | 75.654 | 17.17 GB | 20000 None  |
| Spark | D | 20,000 | no (tolerance 2) | 3.752, 1.531 | 3.752 | 1.3 GB | 20000 199970001  |
| Spark | W2 | 2,000,000 | yes | 4.682 | 4.682 | 2.8 GB | 2000000 1619999100000  |

**Reading.** With keys, main's W1 takes 0.693 s and W2 0.497 s at 2M × 2M. Without keys W2 takes 2.04 s (one window partition), while W1 is a nested-loop band join: 0.885 s at 20,000 rows a side and 3.051 s at 40,000 (×3.4 for ×2 rows, quadratic), so about 2.5 h at 2M. Spark's `_joinAsOf` is quadratic in both shapes (§1.9): 75.654 s with keys and 3.752 s without at 20,000 rows a side, so its 2M × 2M wall time scales to roughly 210 h and 10 h (×10,000 for ×100 rows); it was not sampled at 2M. Seat (a) builds W2's plan natively and is estimated at W2's time or a little under (no join back; the struct rides the fill); seat (b) is estimated below that, at about the cost of sorting each side once (not measured, nothing is built). Neither seat changes a cost case of C-012.

## 6. The unit split (one round each)

- **Unit 1 (seat a):** the `_joinAsOf` door, the validation, the plan builder, and the pins: the grid cells
  (seven types, both modes), the error cells, the call forms, the mixed types, the bounds and the NaN cells, with
  tied cells pinned to the tie set and to a repeat. One build round, one review round; no product file outside the
  cap of 6.
- **Unit 2 (seat b), only if the owner wants it after unit 1's cost:** swap the lowering for `AsOfJoinExec` behind
  the same door; behaviour-identical, the same pins, plus spill and partition-edge tests.
- **Not a unit here:** `merge_asof` (Q3), the SQL `ASOF` alias cell (Q8), and the side findings (Q9).

## 7. Questions (each rules from the table)

For a Fable medium consult; each rules from [domain-table.md](domain-table.md) alone.

- **Q1 — the rating.** (A) **S3** — a missing capability: RePark refuses loudly (`ATTRIBUTE_NOT_SUPPORTED`, never a
  wrong row), two workarounds give Spark's rows on the grid up to ties, and it is a private PySpark API off the
  label path; (B) S2 — trades-to-quotes joins are common in the time-series work RePark targets and the workarounds
  are intricate (W2) or quadratic (W1); (C) unrated backlog. **Default A.** Cells: §1 (`main-D` all MISSING), §2
  (no WRONG on the grid), the cost table in [sketch.md](sketch.md) §5.4.
- **Q2 — the name.** (A) **`_joinAsOf` only**, PySpark's exact private signature (keyword-only `tolerance`,
  `allowExactMatches`, `direction`); skipped by the example-drift gate (`check_example_coverage.py:228`), no freeze
  row; (B) a public `joinAsOf` only (Scala's name; PySpark has none, so code ported from PySpark would not find it);
  (C) both, the public one an alias. **Default A**; (C) is additive later at any minor (`docs/release.md` policy).
  Cells: §8 (the call forms: names, Column `on`, as-of expressions, self join).
- **Q3 — is `merge_asof` in scope for the unit?** (A) **out:** RePark has no pandas-on-Spark door (`pandas_api`
  refuses; no `repark.spark.pandas`), and `merge_asof` is `_joinAsOf(how="left")` underneath with the same matches
  in every comparable cell, so unit 1's door is the whole engine work; the `P` cells stay MISSING as the reference
  for a later pandas-API decision; (B) in, as a stand-alone `repark.spark.pandas.merge_asof` over RePark DataFrames
  (not drop-in: no pandas-on-Spark frame exists to pass it); (C) in, with a pandas-on-Spark layer (a program, not a
  unit). **Default A.** Cells: §10 (`main-P` all MISSING; `spark-PD` EQUAL).
- **Q4 — ties.** Spark has no rule: its pick moves with the right side's input order, partitioning and shuffle
  partitions (§7). (A) **a deterministic rule inside the tie set:** a `nearest` tie goes to the backward row (pandas'
  rule), duplicate right rows at one as-of value go to the first in a stable sort of the right input; the pins
  assert tie-set membership and repeat stability, and the registry row records the TIE class as a documented
  divergence; (B) no rule: pins assert membership only; (C) mirror Spark's pick (not possible: it is not a
  function of the data). **Default A.** Cells: §7, the TIE counts of §1 and §2.
- **Q5 — the seat.** (A) **seat (a)**, the union and window-fill lowering in Rust, one unit; (B) seat (b), the
  `AsOfJoinExec` operator, first; (C) (a) now, (b) as unit 2 only if the owner wants the cost after unit 1.
  **Default C** (which ships A). Cells: the cost table ([sketch.md](sketch.md) §5.4), §2.
- **Q6 — Spark's unnamed errors.** Spark raises a JVM `NullPointerException` for `direction=None`,
  `INTERNAL_ERROR` for a column tolerance, Python `IndexError` for `on=[]` and a Py4J error for a non-boolean
  `allowExactMatches`. (A) **match every named condition exactly** (`AS_OF_JOIN.*`, `UNRESOLVED_COLUMN.*`,
  `UNSUPPORTED_JOIN_TYPE`, `DATATYPE_MISMATCH.*`, the join-type requirement) **and raise a clean typed error where
  Spark's is internal** (`PySparkTypeError` / `PySparkValueError` naming the argument); (B) mirror the exception
  classes too. **Default A.** Cells: §3 (`dir_none`, `tol_column`, `on_empty_list`, `exact_string`).
- **Q7 — overflow in the distance.** ANSI on raises `ARITHMETIC_OVERFLOW` when as-of values sit near the integer
  bounds, even where a closer row exists; ANSI off wraps. (A) **mirror both modes** (the seat computes Spark's
  distance with RePark's ANSI arithmetic); (B) compute the distance without overflow in both modes (diverges, never
  raises). **Default A.** Cells: §5 (the `O` group).
- **Q8 — the SQL `ASOF` alias.** Spark runs `l ASOF JOIN r ON …` as an inner join aliasing `l` as `ASOF`; RePark's
  parser rejects it. (A) **out of this unit; card it** as an SQL-door parse divergence; (B) fix here. **Default A.**
  Cells: §9 (`asof_as_alias`).
- **Q9 — the side findings.** (A) **card both separately:** a correlated scalar subquery with a non-equality
  predicate fails in `scalar_subquery_to_join` (Spark answers), and `df.select([cols])` raises (PySpark accepts a
  list); (B) leave them. **Default A.** Cells: §9 (`correlated_non_equi_scalar`, `select_list`).

## 8. What the brief got wrong, or did not know

- **"`RewriteAsOfJoin` turns it into a join plus an aggregate or a window."** It builds a correlated scalar subquery
  over `min_by`; the optimizer's decorrelation then makes a Cartesian product plus an aggregate plus a left outer
  join. No window appears.
- **"`tolerance`: whether the bound is inclusive."** It depends on `allowExactMatches`: inclusive with `True`,
  exclusive with `False` (§1.1).
- **"`pyspark.pandas.merge_asof` cannot be measured."** Superseded by the orchestrator's update the same morning
  (pandas 2.3.3 in the oracle); measured (§1.7).
- **"What RePark already offers that a rewrite could use: range joins, window functions, `max_by`."** `max_by` and
  `min_by` do not exist on RePark; DataFusion's only range-join operator (`PiecewiseMergeJoinExec`) is experimental,
  off by default and refuses equi-keys; RePark's range join is a hash join with a band filter, or a nested-loop join.
- **Not measured:** Spark's grid with ANSI off (the turn budget; main ran it, and no Spark group outside the bounds depends on the mode). The Spark Python process ran with `TZ=UTC` so collected `TIMESTAMP` values render in the session zone, as main's do; the first pass without it was discarded.
- **"With and without keys" for Spark's cost:** Spark's plan is quadratic with keys too (§1.9), so its 2M × 2M wall
  time is not measurable in a sample window; §5.4 measures it at 20,000 rows and scales it.
