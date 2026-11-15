# Part 2 step 0 — the measured table (main `a06c1088`, Spark 4.1.2, both ANSI modes)

Every cell below has the same verdict, and the same answers, in both ANSI modes. Provenance and commands are in
[map.md](map.md). "dc6b" is main `dc6b4c94`, the main before part 1.

## 1. The sketch's joined cells (6,855), re-joined on `a06c1088`

The sketch's and step 0's runners were re-run on main's package; Spark is step 0′'s fresh 2026-11-15 recording.
The verifier fuzz and the structured `d = v` set (part 1's sets) were not re-run; their rows are the recorded part-1
head, which the delta equals on every answer.

| part | wrong on `a06c1088`, per mode | moved since dc6b |
|---|---|---|
| part 2 (seat V) | 83 | 1 fixed per mode (`ins_values_dec_round`), 0 broken |
| part 1 | 139 (all outside the seats; part 1's hand-back) | 925 fixed per mode |

The 83, by family (both modes): `values/*` 35 and `values_rev/*` 35 (15 wrong values, 15 raises, 5 type-only each);
`extra/values_three_rows`, `extra/values_two_cols`, `extra/values_nan`; `select/values_A_planner_cast` and
`select/values_C_user_cast_vs_double`; the writes `byname_values`, `ow_select_values`, `ow_values`, `rtas_values`,
`select_values_string`, `values_dec38`, `values_string` and `merge_values_src`. Rows: [cells/values-cells-a06c.jsonl](cells/values-cells-a06c.jsonl).

**Correction to part 1's attribution.** `merge_union_src` and `merge_union_big` are not the MERGE source seat. Their
source is a three-branch `UNION ALL` whose first branch is the decimal, and that shape is wrong in a plain `SELECT`
too (section 2, `union/*`). The MERGE source with a two-branch union is right on `a06c1088`.

## 2. The new cells (`probes/p2_cells.py`, 287 per mode)

| family | cells | dc6b → `a06c1088` |
|---|---|---|
| `in/` `IN` and `NOT IN` of 1 to 20 constants at `DECIMAL(10,2)` and `DECIMAL(38,18)`, NULL, NaN, ±Infinity, ±0.0, duplicates, FLOAT, exponent literals, mixed DECIMAL/INT items, a column item | 224 | **all 224 EQUAL on `a06c1088`** (dc6b: 62 equal, 50 wrong, 112 raise) |
| `union/` three- and four-branch unions | 10 | 5 fixed by part 1; **4 wrong on both**: `all3_dec_first`, `all4_dec_first`, `distinct3_dec_first`, `all3_in_subquery`; the control `all2` equal |
| `frag/` `F.expr`, `filter(str)`, `selectExpr` subqueries over `VALUES` or a union (critic M1) | 11 | 5 fixed by part 1, **3 broken by part 1** (`fexpr_filter_z_in_values`, `fexpr_filter_exists_values`, `filter_str_z_in_values`), 2 wrong on both (the scalar subqueries), 1 raises on both (select-list `IN (subquery)`, IN-SUBQUERY-PROJECTION-1) |
| `dml/` MERGE sources (subquery, derived `VALUES`, temp views, `NOT MATCHED … INSERT`, `ON`) and `DELETE` / `UPDATE … WHERE … IN (VALUES …)`, copy-on-write and merge-on-read | 24 | 12 wrong on both (every MERGE whose source holds a mixed `VALUES` or a decimal-first three-branch union, views included); 8 RePark refusals (every `DELETE` / `UPDATE` with a subquery in `WHERE` refuses, where Spark answers); 4 raise on both (`IN (VALUES …)` without `SELECT`, and the derived `VALUES … AS s(id, c)` source: Spark's parse error, RePark's `No field named s.id`) |
| `range/` RANGE frames over a `DATE` key from a mixed `VALUES` (`restate_range_frames_and_replan`) | 4 | 4 wrong on both: the `VALUES` is `decimal(30,15)` with or without the re-plan |
| `trip/` tripwire shapes | 10 | 6 equal (user casts, all-double, all-decimal, single row, INT beside DOUBLE, 1,000 rows with one double), 3 wrong on both (DECIMAL beside FLOAT, NULL first, a CTE), 1 raise on both (select-list `IN (subquery)`) |
| `ins/` `INSERT … VALUES` into DECIMAL and DOUBLE targets | 4 | 2 fixed by #1335/part 1, 2 equal on both |

The three cells part 1 broke are all of one shape: a DOUBLE constant tested against a mixed `VALUES` (or `EXISTS`
over it). On dc6b both sides truncated to `decimal(30,15)` and matched; seat E now compares in `double` against the
planner's truncated `VALUES`. Seat V fixes the `VALUES`, so all three return to Spark's answer.

## 3. What the `IN` rewrite must keep

- **Plan shapes on `a06c1088`** (`plan/*` rows): an `IN` of three plans as three decimal ranges joined by `OR`
  (the preimage hook); an `IN` of four keeps `__repark_decimal_to_double__(d) IN (SET)`, at both scales.
- **Answers:** all 224 `in/` cells are Spark's answer on `a06c1088`, including `-0.0` (matches `0.00`), NaN and
  ±Infinity (match nothing), a NULL item (three-valued) and the column item. They are the rewrite's red-free bar.

## 4. The `IN` cost rows (`probes/in_cost.py`, 100M rows, 3 alternating runs, median, cores 32-47)

| row | `a06c1088` (s) | dc6b (s) | ratio | `a06c1088` form |
|---|---|---|---|---|
| `d10 IN` 1 / 3 / 4 / 20 / 1,000 DOUBLE | 0.048 / 0.053 / 0.207 / 0.212 / 0.316 | 0.048 / 0.050 / 0.052 / 0.067 / 0.332 | 1.00 / 1.06 / **3.95** / **3.14** / 0.95 | ranges for 1 and 3, per-row above |
| `d10 NOT IN` 4 / 20 | 0.209 / 0.217 | 0.166 / 0.186 | **1.26** / **1.17** | per-row |
| `d38 IN` 1 / 3 / 4 / 20 / 1,000 | 0.049 / 0.057 / 0.516 / 0.533 / 0.649 | 0.048 / 0.054 / 0.059 / 0.078 / 0.399 | 1.03 / 1.06 / **8.70** / **6.87** / **1.63** | ranges for 1 and 3, per-row above |
| `d38 NOT IN` 4 / 20 | 0.528 / 0.535 | 0.225 / 0.241 | **2.35** / **2.22** | per-row |
| decimal `IN` list, `d10` 4 / 20 / 1,000 | 0.055 / 0.065 / 0.366 | | 1.05 / 0.96 / 1.10 against dc6b's `IN` | the rewrite's first form |
| decimal `IN` list, `d38` 4 / 20 / 1,000 | 0.057 / 0.077 / 0.428 | | 0.95 / 1.00 / 1.07 | |
| ranges joined by `OR`, `d38`, 1 / 2 / 4 / 8 / 16 / 32 / 64 | 0.045 / 0.050 / 0.058 / 0.073 / 0.110 / 0.201 / 0.470 | | 4 ranges: 0.97 of dc6b's `IN` 4 | the rewrite's second form |
| ranges joined by `OR`, `d10`, 1 / 2 / 4 / 8 / 16 / 32 / 64 | 0.043 / 0.045 / 0.050 / 0.064 / 0.094 / 0.176 / 0.401 | | | empty ranges; the cost of the comparisons only |
| planning, `d10 IN` 1,000 DOUBLE, `LIMIT 0`, 20 reps | 0.084 | 0.149 | 0.56 | |

Answers are equal on both builds in every row. The `OR` cost grows by about 4.5 ms per range per 100M rows; it stays
under the per-row form up to about 32 ranges at `d10` and 64 at `d38`, and at or under 1.05x of dc6b's `IN` only up
to 4 ranges.

Up: [map.md](map.md).
