# LISTVIEW-INNER-JOIN-KEY-1: a DataFrame key-name join on a ListView key joins on the key

**Grade:** B (guided). **Builder:** Claude Opus 5.5 (`claude-opus-5-5`).
**Review:** Opus 5.5 and Muse MAX in parallel (R7). The cap is one build, one critique, one delta and one delta
re-check. The scoped-review rule (2026-11-09) and the named-seat rule (2026-11-10) bind.
**Card:** [listview-inner-join-key-1-card-2026-11-11.md](../../roadmap/mid-term/listview-inner-join-key-1-card-2026-11-11.md).
**Release:** blocks v1.5.7.

## Rulings this order carries (owner, verbatim)

- **"Yes, let's get LISTVIEW-INNER-JOIN-KEY-1 fixed."** (2026-11-11). The fix blocks v1.5.7.
- **"Go with the defaults"** (2026-11-11), adopting the path-forward page's questions 1 to 13. Question 8 binds
  here.
- **Still binding:**
  - the insertion inventory and the type-variant inventory (2026-11-09);
  - the table-completeness check, the named-seat rule and the "consumers of the pairing" row (2026-11-10).

## Clause: the step-0 table

The step-0 table has 326 cells on main and 300 on Spark 4.1.2, measured 2026-11-11 (scratch
`listview-step0-table/`; build item 1 commits it under `evidence/step0/`).

**Wrong on main:**

| Form | Cells | What happens |
|---|---|---|
| Inner `df.join(other, "k")` and `df.join(other, ["k"])` on `ListView` / `LargeListView` keys (of `Int64`, `Utf8` and a struct) and on a struct holding a `ListView` | 28 | `CROSS-PRODUCT`. EXPLAIN shows `CrossJoinExec`, and the key never reaches the plan. |
| `df.join(other, ["k", "j"])` | 7 | `WRONG-ANSWER`: it joins on `j` alone. |
| An aggregate over the join | 7 | `WRONG-ANSWER`. |
| `df.join(other, "k").writeTo(...).append()` | 7 | `WRONG-WRITE`: the cross product is persisted. |
| SQL `JOIN … USING (k)` on these keys | 7 | `REFUSED-WHERE-SPARK-ANSWERED` (`UNRESOLVED_COLUMN`). |

**Correct on main, as controls:**
- `ON` joins;
- outer, semi and anti joins;
- every plain `List`, `LargeList` and `FixedSizeList` key;
- `INSERT … SELECT` over an `ON` join.

### Table-completeness check (Q3)

| Named seat | Table rows | Disposition |
|---|---|---|
| DataFrame key-name join, string and list forms, six join types | `df_str`, `df_list` | carried |
| A key list with a second key | `df_two` | carried |
| DataFrame expression joins | `df_expr`, `df_expr_and` | carried, as controls |
| SQL `USING` and `ON` over frames | `sql_using`, `sql_on` | carried |
| Writes through `writeTo` and `INSERT … SELECT` | DML rows | carried |
| `lateralJoin` with key names | — | **add** (item 1) |
| The native door | `native_*` | **struck in writing.** It has no DataFrame key-name join, and its 26 cells could not see the frames. One `arrow_cast` `ON` and `USING` cell per kind is added as a control (item 1). |
| Polars and pandas build routes | — | **add** (item 1): one inner key-name cell per route that can produce a `ListView` |

### Type-variant additions (item 1)

DataFusion 54.1's `can_hash` (`datafusion-expr` `utils.rs:863`) is false for these, so each one gets the inner key-name
join row, the key-list row and the `writeTo` row:
- `ListView`, `LargeListView`;
- `Union`;
- `RunEndEncoded`;
- `Dictionary` over a non-hashable value;
- a `List`, `LargeList`, `FixedSizeList`, struct or map that holds any of the above.

A type that no facade or Polars/pandas route can produce as a column is struck in writing, with the route tried.

**Mixed `ListView` × `List` key-name joins:** 24 cells, measured on 1.5.6 by the 2026-11-11 evaluation. RePark refuses
them where Spark answers. Item 1 re-measures them on main and puts them in the table.
- **Ruled (owner, 2026-11-11, "Go with the defaults", question 8):** they go in the unit's table. If the fix makes
  them answer at no extra cost, they are pinned as fixed. Otherwise they are carded, since a refusal does not block.

## Design

**Item 1 confirms the root cause on main before any code is written.**
- Capture EXPLAIN (logical and physical) for every failing form: `df_str`, `df_list`, `df_two`, the aggregate, and
  `writeTo`.
- **Leading cause, from the 2026-11-11 evaluation (read on 1.5.6 source; the orchestrator re-read both sites on main
  `f341171a`):**
  - the key-name join builder hands the keys to `LogicalPlanBuilder::join` as `Join.on` pairs
    (`df_guards/case_bind.rs:653`);
  - DataFusion 54.1's `eliminate_cross_join` `find_inner_join` keeps a pair only when `can_hash` is true, and drops
    the rest without a filter;
  - only `Inner` joins pass through that rule, which fits the table (outer, semi and anti are EQUAL).
  - Confirm it on main with the rule disabled for one cell, and the key restored.
- **Other candidates, to rule out in writing:**
  - `join_using`;
  - the key projection after the join;
  - the facade schema reporting `ListView` as `string`.
- The fix sits on RePark's side, before planning. A fork change is a halt.
  - Candidate shapes: non-hashable keys go into the join filter, not `on`; or the keys are cast to their hashable
    kind for the join.
  - The hand-back names the chosen shape and its effect on the mixed-kind cells. **The design is ruled before item 2.**

**Bindings that hold whatever the root cause:**
1. A key-name join must join on every key it names, on every join type, or refuse with Spark's class. It must never
   silently drop a key.
2. The fix must not change any plain-`List` cell, or any of the struct units' pins (#1283, #1290, #1292, #1295).
3. SQL `USING` on a `ListView` key answers as Spark does, or stays refused and is carded if the root cause sits
   elsewhere.
4. The facade's `string` schema report for `ListView` columns is fixed in this unit if it is the cause; otherwise it is
   carded.

## Pins

- A facade pin over the table's DataFrame cells.
- A Rust pin at the owning layer.
- The write cells.
- Both build profiles (the ci profile and release).
- Every expected value comes from a measured Spark cell. Tests go in the same commit as the code they pin.

## Halts

- A root cause inside DataFusion that would need a fork change.
- Any plain-`List` cell, or any struct-unit pin, that moves.
- The cost bar exceeded: ≤ 1.05x on a DataFrame key-name join probe, median of 3.

## Items (commit after each)

1. **The root cause and the completed table. No product code.**
   - Commit the 2026-11-11 step-0 table under `evidence/step0/`, scrubbed of absolute paths, with its probe passing
     ruff on its own.
   - Confirm the leading cause on main, as the Design section says.
   - Measure the table additions on main and Spark 4.1.2:
     - `lateralJoin`;
     - the native `arrow_cast` controls;
     - the Polars and pandas routes;
     - the type-variant additions;
     - the 24 mixed-kind cells.
   - **Hand back** with the cause, the owning layer, the proposed fix shape, and its predicted effect on every row.
     The design is ruled before item 2.
2. **The fix and its pins**, as ruled.
3. **The after-table** on the branch head, the revert checks (each seat's pin goes red with that seat reverted), and
   the struct units' pins re-run unchanged.
4. **Gates**, as in the brief: the facade in the ci profile and in release.

**Scoped-label note (owner "Go", 2026-11-11):** this unit is gate G2 of ICE-V2-FLAT-AWS-LABEL-1. It does not block that
label, but it ships in v1.5.7, the wheel the label names.

## Rulings after item 1 (orchestrator, as the order's author, 2026-11-11)

Item 1, at `2e3cd8cc`, confirmed the cause on main and completed the table: 251 cells on main and on Spark 4.1.2,
plus 45 plan records (`evidence/item1/cause.md`, `table.md`).

1. **Shape B is ruled. A non-hashable key is cast to its hashable kind for the join, on every join type.**
   - One helper in repark-core, next to `join_on_named_keys` (`df_guards/case_bind.rs`). It casts a key only when
     `can_hash` is false for its type, at any depth: ListView and LargeListView to their List kinds, RunEndEncoded
     to its value type, and the containers recursively.
   - When every key is hashable, the existing `LogicalPlanBuilder::join` call stays byte for byte. Otherwise the keys
     go through `join_with_expr_keys`.
   - Output columns keep their original types; only the key expressions are cast.
   - Shape A, the keys in the join filter, is rejected: it falls back to a nested-loop join and works only for inner
     joins.
2. **A Union key is refused** with a named RePark error. No route can produce one, so the refusal is pinned at the
   Rust layer only.
3. **The lateral seat (`crates/repark-python/src/subquery.rs`, the `on_names` join) is in this unit.** Binding 1 covers
   every key-name join. The same helper applies there.
   - The lateral over-acceptance, where Spark refuses key-name `lateralJoin`, is not changed. Refusing it would move
     the plain List lateral cells, which binding 2 forbids. Card LATERAL-USING-ACCEPT-1.
4. **SQL `USING` and `NATURAL` on these keys: the cause is DataFusion's `join_using`.** The right key is looked up in
   the left schema, and the fallback ignores the join type. Under binding 3:
   - the refusal is pinned as it stands;
   - card LISTVIEW-SQL-USING-1;
   - the records clerk drafts an upstream DataFusion report on both `join_using` defects. Its text goes to the owner
     for approval before filing (as with R5). This unit files nothing outward.
5. **The facade's `string` schema for ListView, LargeListView and RunEndEncoded columns** (`types.py`) is not the
   cause. Card FACADE-LISTVIEW-SCHEMA-1 (binding 4).
6. **`crossJoin().where(l.k == r.k)`** refuses for every key kind, List included (`No field named l.k`). It is outside
   the clause: card CROSSJOIN-WHERE-QUALIFIED-1.
7. **The new failing variants are in the pins:** RunEndEncoded keys (Int64 and Utf8); List, LargeList and
   FixedSizeList of ListView; a struct holding a RunEndEncoded field; a List of RunEndEncoded.
8. **Cost.** The plain List key-name join, the unchanged path, must cost at most 1.05x: median of 3, interleaved, base
   against head, inside `flock -x` on the measure lock. The cast path's time is reported beside it with no bar,
   because on base that path returned a cross product.
9. **The 24 mixed-kind cells** become EQUAL under shape B and are pinned as fixed (owner, question 8). Their `known`
   markers in the struct units' pin files change to the measured Spark rows; that is the only change allowed in those
   files.
