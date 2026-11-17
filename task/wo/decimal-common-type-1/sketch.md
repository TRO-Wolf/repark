# DECIMAL-COMMON-TYPE-1 — step 0 sketch (measured)

**Unit:** DECIMAL-COMMON-TYPE-1 (renamed from DECIMAL-DECIMAL-UNION-WIDEN-1), S1, label-blocking, ruled
2026-11-16 ("defaults", about 14:40 UTC; step 0 "go", about 20:55 UTC). Card:
[../../roadmap/mid-term/decimal-decimal-union-widen-1-card-2026-11-16.md](../../roadmap/mid-term/decimal-decimal-union-widen-1-card-2026-11-16.md).
**Measured** on main `e50ed17f` (CAST-LITERAL-UNWRAP-1, #1345, merged) against Spark 4.1.2, both ANSI modes;
documents and probes only. Author: Claude Opus 5.5 (`claude-opus-5-5`).

## 1. The fault

RePark takes DataFusion's decimal common type, which keeps the larger scale and caps the precision at 38
(`get_wider_decimal_type`, `binary.rs:1160`; `create_decimal128_type`, `:1508`). Spark 4 keeps the integer digits
and cuts the scale (`DecimalType.boundedPreferIntegralDigits`). The two differ only when `range + scale > 38`.
Where they differ, main either **raises** (DataFusion's type cannot hold the integer digits: `DECIMAL(38,0)` beside
`DECIMAL(38,38)`, or a 38-digit value beside a `DECIMAL(10,2)`) or **answers exactly** where Spark rounds to its
common type first: `DECIMAL(38,0)` 1 equals `DECIMAL(10,2)` 1.25 on Spark (ruled: RePark matches it).

## 2. Spark's rule, as measured

Every statement below rests on cells in the [domain table](domain-table.md):

1. **Two decimals:** `scale = max(s1, s2)`, `range = max(p1 − s1, p2 − s2)`; `(range + scale, scale)` when that
   fits 38, else `(38, max(0, scale − (range + scale − 38)))`. All 14 pairs' union types match it.
2. **An integer operand** takes Spark's decimal form: `TINYINT` (3,0), `SMALLINT` (5,0), `INT` (10,0), `BIGINT`
   (20,0) — `i64h`, `i32w30`, `i16w34`, `i8w36` and `h = CAST(1 AS BIGINT)` round as these predict.
3. **An integer literal in a binary comparison** (`=`, `<=>`, simple `CASE`, `BETWEEN`) takes its own digits, not
   its type's form: `w30 = 1` and `h = 1` do not round on Spark (with `INT`'s (10,0), `w30 = 1` would be true at
   row 1). **In an `IN` list or `IN` subquery it takes the integer's form** (revision M3: `w30 IN (1)` is true at
   rows 1 and 5 on Spark). The function needs the literal and the position, not only the type.
4. **Unions round once per binary node, bottom up.** `c ∪ d ∪ a` rounds 0.49 → 0.5 → 1 and 1.45 → 1.5 → 2 (through
   the inner node's `(38,1)`); `a ∪ c ∪ d` rounds 0.49 → 0 and 1.45 → 1. The DataFrame chain `union().union()` does
   the same. RePark's initial plan nests binary unions in Spark's shape
   ([evidence/raw/explain-union-repark.txt](evidence/raw/explain-union-repark.txt)), so a bottom-up rewrite of each
   node reproduces it.
5. **Functions, `CASE`, `IN` and multi-row `VALUES` fold all operands at once and round once** (`coalesce(c, d, a)`,
   `VALUES` of three types); a nested `coalesce(coalesce(c, d), a)` rounds twice, which the bottom-up walk gives.
6. **The common type never overflows** (the integer digits always fit); only a literal that does not fit its own
   written type overflows, as a cast does (NULL with ANSI off, `NUMERIC_VALUE_OUT_OF_RANGE` with ANSI on).
7. **Struct fields** take the rule field by field (`named_struct` union).
8. **Arithmetic** has its own rule and agrees today (`a + c`).

## 3. Headline counts

Spark door, per ANSI mode: **874 common-type cells, 825 wrong** with ANSI off (356 wrong answers, 7 wrong types,
**38 wrong writes**, 424 refusals Spark answers) and 819 wrong plus 6 both-raise with ANSI on. Native door: 655
cells, 612 wrong. Controls: 161 of 161 agree in the core positions. Per door: Spark-door SQL 612 of 655, DataFrame
132 of 136, `F.expr` 38 of 40, Iceberg 43 of 43 (38 wrong writes, 5 wrong partitioned reads). Every wrong write is
named in [domain-table.md](domain-table.md#iceberg-local-v2-hadoop-catalog-every-wrong-write).

**Revised after the critic** ([domain-table.md](domain-table.md#after-the-critic-revision-2026-11-16)): 946
common-type cells per mode, **890 wrong** with ANSI off and 884 plus 8 both-raise with ANSI on, once the set-operation
membership cells and the 52 revision cells (map keys, `USING`, `IN` literals, set operations, pin candidates) are
counted; native 632 of 675.

**Reach (measured, not reasoned):** a plain literal beside a `DECIMAL(38,0)` column (`a = 1.25`, `a > 0.5`,
`a IN (1.25, 2.5)`) answers unlike Spark, and **raises** on main when the column holds 37 or 38 integer digits;
`HAVING sum(k28) = <DECIMAL(10,4)>` and `avg(k28) = max(a)` miss Spark's groups; `BIGINT` beside a `DECIMAL(38,20)`
raises for any value past 18 digits.

## 4. Seat A (recommended; Fable's): one shared function inside `SparkDecimalFloatWiden`'s walk

**The function.** A new module file beside the walk (`crates/repark-spark/src/extension/decimal_common_type.rs`,
under the 912-line `decimal_float_widen.rs`), with one entry, `spark_decimal_common_type(operands) ->
Option<DataType>`:

- inputs are the operand types, the position's mode (binary comparison, `IN`, or unification), and per operand
  whether it is an integer literal and its digits (§2 rule 3: own digits in a binary comparison, the type's form in
  the `IN` mode);
- integers map to Spark's forms (§2 rule 2), then the fold of §2 rule 1 runs over all operands at once;
- it returns `None` (no rewrite) when every operand already has the result type, or when DataFusion's rule gives
  the same type, so the rewrite only fires when `range + scale > 38` or an integer literal needs its own digits;
- struct types recurse field by field (§2 rule 7); arrays and maps by element.

**The walk.** `SparkDecimalFloatWiden` keeps its single detect walk (`plan_widens`, `decimal_float_widen.rs:237`)
and its single rewrite walk (`transform_up_with_subqueries(rewrite_plan)`, `:37`):

- `expr_widens` (`:126`), `case_widens` (`:171`), `union_positions` (`:250`) and `join_widening` (`:372`) ask, beside
  today's `mixed` (decimal and float), "does the shared function return a type?";
- the cheap gate first: the function is consulted only when, with integers mapped to their forms, the position
  holds an operand of precision 20 or more (two operands below 20 sum to at most 38), so a plan of small decimals
  costs one comparison per operand more than today; an integer literal never makes the function pick a narrower
  scale than DataFusion's (§2 rule 3: `w30 = 1` stays `decimal(38,30)`);
- the rewrites reuse today's shapes with a target type instead of `double`: `widen_pair`/`widen_all` cast each
  operand to the result type; `rewrite_union` projects each branch (`project_to_double`, `:277`, generalised);
  `rewrite_join` casts keys and projects both sides of `EXCEPT`/`INTERSECT`; `widen_in_subquery` projects the
  subquery; `nullif` keeps its `CASE` form;
- the function lists grow by `map_concat` and `array_contains` (measured wrong, not in the lists today);
  `array_union` stays out, since Spark refuses two element types there (§9);
- decimal beside float keeps priority: a position with a float is part 1's `double`, unchanged.

**The five explicit changes** beyond today's lists (after the critic): (1) seat V's gate at `consume.rs:278`;
(2) `map_concat` and `array_contains` join the function lists; (3) struct fields recurse in `union_positions`
(`:250`) and the function; (4) **map keys**: `MAP_CONSTRUCTORS` (`:27`, `function_positions` `:108-110`) gains the
key positions `(0..arity).step_by(2)` and `map_concat`'s argument maps their key type, and a key collision after
rounding raises `DUPLICATED_MAP_KEY` as Spark does (revision M1); (5) an **`IN` mode** in the shared function, so an
integer literal in an `IN` list or `IN` subquery takes its type's form while `=` keeps its own digits (revision M3).
Aliased `USING` joins and set-operation membership need no new seat (`rewrite_join`, `:395`, already sees their keys;
revision M2, M4), only pins.

**Seat V.** `column_placement` (`values_marker/consume.rs:254`) calls the same function for an all-decimal or
integer column (today it returns `None` at `:278` unless decimal and float mix). `Standalone` retypes the field and
casts each cell; `Target` (`INSERT … VALUES`, `type_insert_source`, `:90`) casts each cell to Spark's type and then
the planner's cast to the target, so `INSERT … VALUES (1), (1.25)` stores 1.00, 1.00 as Spark does. The cells are
marked, so an integer literal cell is known (the c2 cell `VALUES (1), (1.25)` types `INT` → `decimal(12,2)`).

**#1345.** The casts the walk adds are ordinary `Expr::Cast` nodes before `type_coercion`; the shield runs after it
and wraps the column-loses-digits casts against constants (inventory). No change to the shield.

**Positions covered** (every position of the domain table):

| position | how |
|---|---|
| `=`, `<>`, `<`, `<=`, `>`, `>=`, `<=>`, `BETWEEN`, `IN` list, `NOT IN`, simple and searched `CASE`, `if`, `nvl2`, `coalesce`, `greatest`, `least`, `array`, `map`, `nullif` | today's expression positions |
| `array_contains`, `map_concat` | added to the function lists |
| `map` and `map_concat` **keys** | change (4); collisions raise `DUPLICATED_MAP_KEY` |
| `IN` lists and `IN` subqueries with integer literals or integer columns | change (5), the `IN` mode |
| aliased `JOIN … USING (k)` (inner, left, full), DataFrame `join(…, "k")` | `rewrite_join` on the `USING` keys |
| `nvl` / `ifnull` | the walk casts, but DataFusion's `nvl` then makes `double` (§9, a separate card); the cast is harmless |
| `UNION`, `UNION ALL`, nested, `EXCEPT`, `INTERSECT`, struct fields in a union | `rewrite_union` per binary node; `rewrite_join`'s set-operation form; the function's struct recursion |
| join keys, `IN (subquery)`, `NOT IN (subquery)`, scalar subquery, correlated `EXISTS` | `rewrite_join`; `widen_in_subquery`; the comparison inside the subquery's filter |
| `HAVING`, grouping keys (`GROUPING SETS`, `ROLLUP` over `coalesce`) | the filter's and projection's expressions |
| multi-row `VALUES`, `INSERT … VALUES` | seat V |
| `lag`/`lead` defaults, `USING` over two unaliased derived tables, `RANGE` offsets, `PIVOT` | refused on main for other causes (§9); the rewrite applies once those land (`lag`'s default is an argument after the first) |
| DataFrame, `F.expr`, Iceberg `INSERT`/CTAS/RTAS/`DELETE`/`UPDATE`/`MERGE` | the same session analyzer (inventory) |
| native `repark.sql()` | not covered (no Spark extension); carded, as parts 1 and 2 did |

**Trade-offs.** One walk and one detect pass, as Fable recommends, so the planning cost of plans with no pair past
38 is the gate's comparisons only; part 1's shapes are reused, so the rewrite is small. The cost is that
`decimal_float_widen.rs` grows its class test from two classes to "a type the function returns", and the module
name no longer says what it does (a rename is a later pure move).

## 5. Seat B (alternative): a retarget rule after `type_coercion`

A rule inserted right after `type_coercion` finds the casts DataFusion inserted to its wider decimal type and
retargets them to Spark's type, reading each cast's source type.

- **For:** it sees DataFusion's own decision at every seat, including the ones no list names (`map_concat`,
  `array_contains`, `array_union`, any function added later), and `VALUES`' planned casts.
- **Against:** a cast DataFusion inserted cannot be told from one the user wrote (`CAST(a AS DECIMAL(38,38)) = b`),
  so it needs a marker on every coercion cast or it rewrites user casts — a correctness risk, not a cost; a union's
  schema is already fixed, so it rebuilds projections and the stale-schema path; it runs a second walk on every plan
  (part 2 halted on planning cost); the bottom-up union order is lost once coercion ran top-down through the fold
  (`coerce_union_schema_with_schema` folds all branches at once, `type_coercion.rs:1190`), so the double rounding of
  nested unions would need reconstruction. Not recommended.

A patched DataFusion (`get_wider_decimal_type` with Spark's rule) was considered and set aside: it would change the
native door's answers too, and carry a fork of `datafusion-expr-common`.

## 6. The legacy flag

`spark.sql.legacy.decimal.retainFractionDigitsOnTruncate=true`: **skip the decimal-pair test** (Fable's lean).
Measured: under the flag, Spark's type is DataFusion's, and 64 of 94 cells per mode agree on main already. The 28
overflowing cells differ: Spark answers NULL for the value that does not fit with ANSI off and raises with ANSI on,
where main raises an Arrow error in both modes. That is a store-cast question under a legacy flag, **not fixed in
this unit**: card it (owner question 3). The build needs the flag's value at analysis time; RePark accepts `SET`,
reads it back and `RESET`s it (measured), but whether the analyzer can read it is a build question — if the session
carries no such value, the build adds the carrier with `SET`/`RESET` pins (NAN-MINMAX-RESET-1 is the precedent).

## 7. Cost cases a build must run

- **K1, no pair past 38, held at 1.05x:** planning time, head over main, of (a) the like-for-like set (C-012),
  (b) a 200-comparison filter of `DECIMAL(10,2)` and `DECIMAL(12,4)` columns, (c) a 32-branch `UNION ALL` of
  `DECIMAL(18,2)` and `DECIMAL(20,4)`, (d) a 10,000-row two-column `VALUES` of `DECIMAL(10,2)` (part 2's
  `plan_values10k_none` shape), and (e) part 1's and part 2's held planning rows
  ([../decimal-double-union-coercion-1/evidence/fix/cost.md](../decimal-double-union-coercion-1/evidence/fix/cost.md),
  [part2/cost.md](../decimal-double-union-coercion-1/evidence/part2/cost.md): `plan_values10k`,
  `plan_values10k_none`, `plan_values10k_dbl`, `ins_values10k_dec`, `ins_values10k_dbl`, `merge_values1000`, the
  per-row `IN` rows).
- **K2, pairs past 38, reported:** the same shapes with `DECIMAL(38,0)` beside `DECIMAL(10,2)`; execution time of
  a 10-million-row `UNION ALL` and of `WHERE a = c`, head beside main (main's answers are wrong, so no bar).
- **K3, the constant cells keep #1345's pruning:** an Iceberg v2 table of 100 files partitioned on a
  `DECIMAL(10,2)` column, `WHERE c = CAST('1' AS DECIMAL(38,0))`: files scanned equal those of the hand-written
  range `c >= 0.50 AND c < 1.50`.

- **K4, wide `CASE` (after the critic), held at 1.05x:** planning time of a 200-branch searched `CASE` over
  `DECIMAL(10,2)` and `DECIMAL(12,4)` results (the walk visits every branch), head over main.
- **K5, DML planning (after the critic), held at 1.05x:** planning plus execution of a `DELETE` and an `UPDATE` with
  a 200-comparison predicate on an Iceberg v2 table of no pair past 38: `predicate_dml.rs:376` re-plans the predicate
  through the session, so the walk runs again on that path.

## 8. How sweep families 3 to 6 (and 10) join the same function later

The function's contract is "Spark's least common type of these operands at this position". Each family is a new
input class or a new position rule behind the same entry, not a new walk:

- **3 (an integer beside a FLOAT under ANSI):** a float class in the fold (`int + float` is `double` on Spark);
  it needs ANSI as an input, which the walk already has access to through the session config.
- **4 (FLOAT beside DECIMAL in arithmetic, `float / float`):** arithmetic positions, today outside the walk; the
  function would add an arithmetic mode beside the comparison mode.
- **5 (`EXCEPT`/`INTERSECT` keep the left type):** the set-operation form already projects both sides; the output
  type becomes the function's result instead of the left side's.
- **6 (`nullif` returns the common type):** the `CASE` form keeps the comparison in the common type and returns the
  first argument cast back to its own type.
- **10 (`VALUES` typing of integers):** seat V's integer-literal input (the c2 cell), already in this unit if the
  owner agrees (question 4).

## 9. The unit split

**Recommendation: one unit for the core family now**, the Spark door only, with these positions carded:

- **In the unit** (890 wrong cells per mode on the Spark door after the critic, all 38 wrong writes): comparisons,
  `IN` (with integer literals in the `IN` mode), `BETWEEN`, `CASE`, `if`, `coalesce`, `greatest`/`least`, `array`,
  `map` and `map_concat` keys and values, `array_contains`, `nvl2`, `nullif`'s comparison, unions (`UNION DISTINCT`
  too) and set operations with their **membership rows** (`INTERSECT`, `EXCEPT`, DataFrame `intersect`/`subtract`),
  struct fields, joins including **aliased `USING`** (inner, left, full) and DataFrame `join(…, "k")`, subqueries,
  `HAVING`, aggregates over `CASE` and unions, and grouping keys,
  multi-row `VALUES` and `INSERT … VALUES`, the DataFrame and `F.expr` doors, and every Iceberg statement named in
  the domain table. They share one function and one walk; splitting them would put two rules on one seat.
- **Carded, other causes** (each refuses or answers unlike Spark for the control pair too): `nvl`/`ifnull` making
  `double` (NVL-DECIMAL-DOUBLE-1, wrong type, S2, **linked to the parked
  [NVL-TYPE-COERCION-2](../../roadmap/mid-term/nvl-type-coercion-2-card-2026-10-02.md)**, whose mode-aware `nvl`
  rewrite is the other half of the same function's typing, so `nvl` is fixed once, one way); `lag`/`lead` with a
  column default (refuses); `JOIN … USING` over two **unaliased** derived tables (refuses for every join type and
  same types too; the card is scoped to exactly that shape); `RANGE` frames with a decimal offset (refuses);
  `UPDATE … SET x = coalesce(CASE …)` (`__common_expr_1`, refuses); `avg` of a decimal truncating instead of
  rounding half up (AVG-DECIMAL-ROUND-1, **wrong answer**, S2); `array_union` accepting two element types (S3);
  `PIVOT` (existing gap).
- **Carded, this family's other doors:** the native door (612 of 655 cells wrong; DECIMAL-COMMON-TYPE-NATIVE-1) and
  the legacy flag's overflow NULLs (§6).

## 10. Owner questions

1. **Scope: one unit, Spark door, with the carded positions in §9?** Default **yes**, now including aliased
   `USING`, set-operation membership, `IN` literals and map keys. Cells: the grid (every position wrong on the same
   seat), the 38 wrong writes, the revision cells `rev/m1/*`–`rev/m4/*`, and the control rows showing the carded
   causes are independent (`nvl_same`, `range_same_type`, `using_same_type`, `rev/m2/using_full_dup_names_same_type`,
   `lag_same_type_default`, `avg_k28`).
2. **Double rounding of nested unions: match Spark?** Default **yes, match it** (one rounding per binary node, so
   `c ∪ d ∪ a` stores 1 for 0.49 and 2 for 1.45, while `a ∪ c ∪ d` stores 0 and 1). Cells: `three/u3_flat`,
   `side/union_flat_a_c_d`, `side/df_union_chain_c_d_a`, `three/coalesce_nested`. The alternative, one rounding
   over the flattened union, answers unlike Spark for the first shape.
3. **The legacy flag's overflow: card, not fix?** Default **card** (LEGACY-DECIMAL-TRUNCATE-OVERFLOW-1, S3): the
   flag skips the rule; the 28 overflow cells keep raising where Spark answers NULL with ANSI off. Cells:
   `legacy/pair/*/tx/*`, `legacy/extra/v_bc`, `legacy/extra/u_const_ab`.
4. **The c2 cell (an `INT` literal in `VALUES` typed `BIGINT`) in this unit?** Default **yes**, through seat V's
   integer-literal input: it is one line of the same function and seat V is changed anyway. Cells: `values/v_int_c`
   (`decimal(22,2)` against Spark's `decimal(12,2)`). The alternative leaves it to sweep family 10.
5. **Struct fields in this unit?** Default **yes for unions** (measured `literal/u_struct`, two-field
   `rev/pin/struct_pos`); struct comparisons raise on both engines (`rev/pin/struct_cmp`), a both-raise pin only.
6. **The overflow literals:** main refuses at planning in both modes where Spark answers NULL (ANSI off) or raises
   (ANSI on). Default: **the unit's casts make them answer as Spark's cast does** (ANSI off: the literal is NULL; ANSI
   on: `NUMERIC_VALUE_OUT_OF_RANGE`), pinned; if the planning refusal comes from the literal's own cast and not the
   common type, card it instead of growing the unit. An overflow literal in an `IN` list follows the `IN` mode
   (integer forms). Cells: `literal/u_overflow_lit`, `where_overflow_lit`, `u_overflow39`, `coalesce_overflow_lit`.
7. **New cards** for the side findings (NVL-DECIMAL-DOUBLE-1 S2 linked to NVL-TYPE-COERCION-2, AVG-DECIMAL-ROUND-1
   S2 (a wrong answer), the four refusals S3 with the `USING` card scoped to unaliased derived tables,
   ARRAY-UNION-TYPES-1 S3, DECIMAL-COMMON-TYPE-NATIVE-1 S2, LEGACY-DECIMAL-TRUNCATE-OVERFLOW-1 S3): file them in the
   records round after this step 0. Default **yes**; none is label-blocking by this step 0's evidence except
   AVG-DECIMAL-ROUND-1, which is a wrong answer on a scalar decimal column and is put to the owner.

## 11. After the critic

The Muse inventory critic's verdict was `GAPS`: no table cell contradicted, about 40 citations checked and none wrong,
all seven owner-question defaults supported, seat A's bottom-up exactness verified by reading code, and seat B's
rejection strengthened (the analyzer's union schema is one n-ary left-to-right fold,
`coerce_union_schema_with_schema`, `type_coercion.rs:1190`, so per-node rounding cannot be recovered after it). Its
four findings were re-measured here ([domain-table.md](domain-table.md#after-the-critic-revision-2026-11-16)); one
was corrected by the re-measure: inner `USING` plans only when the derived tables are aliased — unaliased, every
join type refuses, same types too.

Priority list for the build brief:

- **P1, `IN` literals:** the `IN` mode (change 5) with pins `w30 IN (1)` (select and `WHERE`), `w30 IN (1, 2)`,
  `NOT IN`, `w30 IN (i32)`, `w30 IN (SELECT i32 …)`, `CAST('1.0…049' AS DECIMAL(38,30)) IN (1)` / `IN (id)` /
  `= id`, DataFrame `isin(1)`; and the agreeing `w30 = 1`, `CASE w30 WHEN 1`, `BETWEEN 1 AND 1` stay agreeing.
- **P2, `USING`:** aliased inner, left and full `USING` and DataFrame `join(…, "k")` in the unit with pins; the
  refusal card scoped to `USING` over two unaliased derived tables.
- **P3, map keys:** change (4) with a `DUPLICATED_MAP_KEY` pin for `map(a,'x', c,'y')` and `map_concat` at id 1,
  and answer pins for the non-colliding `c + 5` shapes (main raises `NULL_MAP_KEY` there today).
- **P4, set operations:** `INTERSECT`/`EXCEPT` membership pins (SQL `DISTINCT` and plain spellings, both orders,
  DataFrame `intersect`/`subtract`, `i64 ∩ h`) and one `UNION DISTINCT` pin.
- **P5, the shield and the fold:** the 18 column-loses-digits constant cells of step 0 (the critic's `shield_*`
  cells among them) pinned on the head; and a benign interaction to pin, not change: `FoldSparkNumericCasts`
  (`spark_typed.rs:22`, after coercion) folds a cast literal, so the walk's cast of `CAST(1.5 AS DECIMAL(10,2))` to
  `decimal(38,0)` is folded there and must round half up (`rev/pin/fold_rounded_literal`: Spark id 2).
- **P6, cost:** K4 (wide `CASE`) and K5 (DML planning) beside K1–K3.
- **P7, `nvl`:** NVL-DECIMAL-DOUBLE-1 links the parked NVL-TYPE-COERCION-2 so `nvl` is fixed once, one way.

Further pin candidates the critic named, all measured different on main: the two-field struct union, `max`/`min`/`sum`
over a mixed `CASE`, `sum` over a mixed union, the literal `BETWEEN` two decimal columns, `map` and `map_concat`
values, and `unionByName(allowMissingColumns=True)`.

## 12. What this step 0 did not settle

- Whether the analyzer can read the legacy flag (§6).
- `MERGE` source unions of three types; the build's step 0 measures them. (Struct comparisons and `IN` lists with
  integer literals were measured after the critic.)
- DataFusion 55 (the next bump) was not read for this seat.

Up: [map.md](map.md).
