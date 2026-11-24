# DECIMAL-COMMON-TYPE-1 — the bar (head `7bf32859`)

**Measured** 2026-11-16 on the package `pkg-dct-7bf32859` (`BUILT_FROM` `7bf328595f91310b9495fc7e86291b3fa4379671`,
built from the committed tree) against main `33bfde4e` (package `pkg-cu-88999454`, product tree equal) and Spark
4.1.2, both ANSI modes, by Claude Opus 5.5 (`claude-opus-5-5`).

## Method

- Step 0's whole cell set re-ran on the head: `run_cells.py`, `run_side.py` and `run_revision.py` in both ANSI modes
  and `run_native.py` ([../probes/](../probes/), unchanged), by [probes/run_repark.sh](probes/run_repark.sh),
  then `compact.py` and `analyze.py` against step 0's Spark raw files (Spark does not change). Main's verdicts are
  step 0's [../raw/verdicts.jsonl](../raw/verdicts.jsonl); main's native door ran a second time to show which error
  texts vary run to run.
- [probes/bar.py](probes/bar.py) pairs head and main cell by cell, names every non-EQUAL common-type cell, and
  renders a struct value alike on both engines (PySpark returns a `Row`, RePark a `dict`; the types and values
  are equal — 6 cells: `literal/u_struct` on `t` and `tx`, `rev/pin/struct_pos`, both modes).
- Raw: [raw/](raw/). The full table is [raw/bar-table.md](raw/bar-table.md), the other causes cell for cell
  [raw/other-causes.md](raw/other-causes.md).

## Result

**0 EQUAL→WRONG and 0 EQUAL→RAISE**, on the Spark door and the native door, both modes.

Common-type cells, Spark door (step 0's 946 per mode, after the critic):

| ANSI | cells | agree | both raise | wrong answer | wrong type | wrong write | over-raise | missed raise |
|---|---|---|---|---|---|---|---|---|
| off, main | 946 | 54 | 2 | 402 | 9 | 38 | 439 | 2 |
| off, head | 946 | **928** | 4 | 2 | 0 | **0** | 12 | 0 |
| on, main | 946 | 54 | 8 | 402 | 9 | 38 | 433 | 2 |
| on, head | 946 | **928** | 10 | 2 | 0 | **0** | 6 | 0 |

All 38 wrong writes are EQUAL (Iceberg `INSERT … UNION ALL`, `INSERT … VALUES`, CTAS, RTAS, `DELETE`, `UPDATE`,
`MERGE`, copy-on-write and merge-on-read, partitioned `DELETE`), and the 5 partitioned reads. The 161 control
cells in the core positions agree as on main; the legacy flag's 94 cells per mode answer as main's except the two
after-`RESET` cells, which now agree (66 agree, 28 overflow cells as main's).

## Every non-EQUAL common-type cell, named (18 per mode)

| class | cells (each in both ANSI modes) | verdict |
|---|---|---|
| **Ruling 6, carded instead:** the literal's own cast refuses at planning (`SELECT CAST('1.5' AS DECIMAL(38,38))` refuses alone, on main and the head); carded as **DECIMAL-LITERAL-OVERFLOW-1** (new, for the records clerk; beside ANSI-OFF-NARROWING-OVERFLOW-1) | `literal/u_overflow_lit` (`t`, `tx`), `literal/where_overflow_lit` (`t`, `tx`), `literal/u_overflow39` (`t`, `tx`), `literal/coalesce_overflow_lit` (`t`, `tx`) | over-raise with ANSI off; both raise with ANSI on, except `coalesce_overflow_lit` (Spark never takes the literal branch) |
| **Revision M1, as ruled:** a key collision after rounding raises `DUPLICATED_MAP_KEY` | `rev/m1/create_map_keys_id1`, `rev/m1/map_concat_keys_id1` | both raise, the same class |
| **Ruling 5, pin only** | `rev/pin/struct_cmp` | both raise |
| **AVG-DECIMAL-ROUND-1** (side card): the `HAVING` row now matches; the shown `avg(k28)` truncates | `agg/having_avg_max/t` | wrong answer (0.836666 against 0.836667) |
| **MAP-FILTER-ORDER** (new finding): the map constructor is evaluated above a filter the physical plan moved after it and meets row 6's NULL key; `map(a, 'x', CAST(gg AS DECIMAL(38,0)) + 5, 'y')`, no common type involved, raises the same on main | `rev/m1/create_map_keys_apart`, `rev/m1/map_concat_keys_apart`, `rev/m1/map_keys_of_concat`; `rev/m1/map_concat_keys_all` (both raise, `NULL_MAP_KEY` against Spark's `DUPLICATED_MAP_KEY`) | over-raise (3), both raise (1) |
| **FEXPR-DECIMAL-LITERAL** (new finding): `F.expr("a = 1.25")` types `1.25` as `double`, so the pair is decimal beside double, not two decimals | `dfconst/fexpr_a_eq_lit/t` | wrong answer |
| **DECIMAL-CAST-SELECTIVITY** (new finding): DataFusion's filter-selectivity assertion (`Selectivity is out of limit: inf`) on a cast of a `BIGINT` column whose statistics reach ±2⁶³; `CAST(i64 AS DECIMAL(38,18)) = CAST('1.0…' AS DECIMAL(38,18))` raises the same on main | `const/i64_eq_hlit/tx/where` | over-raise (the select-list spelling agrees) |

Every one of them was non-EQUAL on main too.

## Outside the common-type cause

The other causes keep main's verdicts cell for cell ([raw/other-causes.md](raw/other-causes.md)): `nvl` making
`double` (NVL-DECIMAL-DOUBLE-1), `array_union` (ARRAY-UNION-TYPES-1), `lag`/`lead` defaults, `USING` over unaliased
derived tables, `RANGE` offsets, `PIVOT`, `UPDATE … SET coalesce(CASE …)`, `avg` (AVG-DECIMAL-ROUND-1), `nullif`'s and
`EXCEPT`/`INTERSECT`'s result types on the control pair (sweep families 6 and 5), and the legacy flag's 28 overflow
cells (LEGACY-DECIMAL-TRUNCATE-OVERFLOW-1).

## The native door

Unchanged: all 912 cells answer as main's (same rows and types, or a raise where main raises). Four cells raise
with a different text (`pair/ab/t/except`, `pair/ab/t/intersect`, `pair/i32w30/tx/except`, `pair/i32w30/tx/intersect`);
main's own second run differs from step 0's in three of them ([raw/main-native-rerun.jsonl](raw/main-native-rerun.jsonl)),
because the first overflowing row depends on the partition order.

## The union rounding proof

`c ∪ d ∪ a` (SQL `u3_flat`, `u3_left`, `u3_sub` and the DataFrame chain `union().union()`) answers as Spark does,
0.49 → 0.5 → 1 and 1.45 → 1.5 → 2 through the inner node's `decimal(38,1)`, and `a ∪ c ∪ d`
(`u3_a_c_d`, the chain `a, c, d`) answers 0.49 → 0 and 1.45 → 1; `coalesce(c, d, a)` rounds once and
`coalesce(coalesce(c, d), a)` twice. Pinned on the SQL door and in the DataFrame chain
(`python/repark/tests/types/test_decimal_common_type_1.py`, `shape/u3_*`, `dfshape/union_chain_*`) and in Rust
(`nested_unions_round_once_per_binary_node_bottom_up`).

## The pins, red and green

`python/repark/tests/types/test_decimal_common_type_1.py`, 651 cells × 2 ANSI modes against
[../../../../../python/repark/tests/types/decimal_common_type_1_spark_oracle.json](../../../../../python/repark/tests/types/decimal_common_type_1_spark_oracle.json)
(recorded by [probes/record_oracle.py](probes/record_oracle.py) on Spark 4.1.2, both modes):

- **main** `pkg-cu-88999454`: 1078 failed, 226 passed (the 226 are the controls, the legacy sample, the agreeing
  integer-literal comparisons and the both-raise struct comparison);
- **head** `pkg-dct-7bf32859`: 1304 passed.
