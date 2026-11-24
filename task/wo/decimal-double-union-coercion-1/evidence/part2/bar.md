# Part 2 bar

This is the head of part 2, measured against Spark 4.1.2 in both ANSI modes, beside main and main before part 1. The
result: **no cell moves from EQUAL to WRONG or to RAISE** in any set, against either base.

## Builds and method

- **Head:** the scratchpad package `pkg-p2-6be38a8e`. Its `BUILT_FROM` holds `6be38a8e`, and it was built from that
  commit's tree in a clean worktree with `maturin build --profile release`. The later commit `7906b0e3` is
  lint-only (allow attributes, one signature change, no logic change), so every answer below holds for it.
- **a06c:** main `a06c1088`, the package `pkg-ddu1-7b21bde3` (its product tree equals `a06c1088`'s).
- **dc6b:** main before part 1, `pkg-main-dc6b4c94`.
- **Oracle:** PySpark 4.1.2 on Zulu JDK 17, `local[1]`, session zone UTC, writes through
  `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` into a private hadoop catalog per run.
- **When:** 2026-11-16, between about 04:25 and 05:20 UTC. Each probe held the shared measure lock.
- **The verdict per cell:** EQUAL → EQUAL, WRONG → EQUAL (a fix), EQUAL → WRONG, EQUAL → RAISE, or still wrong.

## The sets

| set | cells per mode | against a06c | against dc6b |
|---|---|---|---|
| `p2_cells` (step 0) | 287 | 249 EQUAL → EQUAL, 28 fixed, 10 still wrong | 78 EQUAL → EQUAL, 199 fixed, 10 still wrong |
| `build_cells` | 64 | 24 EQUAL → EQUAL, 33 fixed, 7 still wrong | 19 EQUAL → EQUAL, 38 fixed, 7 still wrong |
| `fam` (critic) | 62 | 43 EQUAL → EQUAL, 18 fixed, 1 still wrong | 28 EQUAL → EQUAL, 33 fixed, 1 still wrong |
| `bound` (critic) | 6 | 5 EQUAL → EQUAL, 1 fixed | 3 EQUAL → EQUAL, 3 fixed |
| `identity` (critic) | 3 | 3 still wrong | 3 still wrong |
| `f3` (critic) | 11 | 4 EQUAL → EQUAL, 7 still wrong | 3 EQUAL → EQUAL, 1 fixed, 7 still wrong |
| part 1's preimage cells | 1,209 | 1,161 EQUAL → EQUAL, 0 moved, 48 still wrong | — |

The counts are the same in both ANSI modes.

**Step 0's joined cells** (6,855 in all, both modes): 4,822 equal on both builds, 172 fixed by the head, 1,861 wrong
on both, and 0 regressed. No cell that is wrong on both builds changed its answer. The coercion replay and the
`#888` replay each show 0 moves.

## Every cell still wrong

Each one refuses on a06c and on the head, and the two error texts are identical.

- **The DML `VALUES` link (Q2, "refuses, unchanged"):**
  - `p2_cells` `dml/<cow|mor>/dml_delete_in_values_v`, `dml_delete_in_values_d`, `dml_update_in_values_v` and
    `dml_delete_exists_values_v` (8 cells);
  - `build_cells` `dml/<delete|update>_in_values_v/<cow|mor>` (4);
  - `f3` `del_in_sub`, `del_exists`, `del_scalar`, `upd_in_sub` and `upd_exists` (5);
  - `identity` `del/id_in_select`, `del/id_eq_scalar` and `upd/id_in_select` (3).

  Each is a `DELETE` or `UPDATE` with a `WHERE` subquery that the identity path declines. The cards are
  `repark-sql-update-subquery-where-1` and UPDATE-SUBQUERY-STORE-1.
- **`INTERSECT ALL` and `EXCEPT ALL`** (`build_cells` `setop/intersect_all` and `setop/except_all`). These refuse
  loudly by design.
- **An `IN` subquery in a select list** (`fam` `frag/selectexpr_z_in_values`, and `p2_cells`
  `frag/fexpr_select_in_values` and `trip/in_subquery_values`). The physical planner refuses it ("Physical plan does
  not support logical expression InSubquery").
- **A recursive-CTE anchor** (`build_cells` `values/rcte_anchor`). The planner refuses `r.n + 2` as an Int64 against
  the anchor's Int32 column. Seat V skips a `VALUES` under `RecursiveQuery`.
- **`f3` `plan/in3` and `plan/in4`.** These are `EXPLAIN` text cells, which never equal Spark's plan text.
- **The 48 preimage cells** are `<literal> IS DISTINCT FROM <column>`, which refuses with `UNRESOLVED_COLUMN`
  (DISTINCT-FROM-LITERAL-LEFT-1, pre-existing).

## Caught panics

The probes' standard error holds 10 caught panics ("primitive array", `arrow-array-58.4.1/src/cast.rs:849`):
- `run_select.py` gives 3 per mode;
- `run_revision.py` gives 2 per mode.

a06c gives exactly the same counts from the same probes. They are the recursive-CTE projection panic,
RCTE-PROJECTION-PRUNE-1, and the process survives each one.

## The delta (2026-11-16)

The delta's head is the package `pkg-p2-12101e9b` (`BUILT_FROM` `12101e9b`, built from the committed tree with
`maturin build --profile release`). Every set was re-run on it in both ANSI modes, against a06c and Spark 4.1.2.
**No cell moves from EQUAL to WRONG or to RAISE in any set, and no native-door cell moves.**

| set | cells per mode | against a06c |
|---|---|---|
| `p2_cells`, `build_cells`, `fam`, `bound`, `identity`, `f3` | 287, 64, 62, 6, 3, 11 | the same counts as the build, above, cell for cell |
| part 1's preimage cells | 1,209 | 1,161 EQUAL → EQUAL, 48 still wrong, 0 moved |
| step 0's joined cells | 6,855 in all | 0 coercion moves, 0 replay moves |
| `delta_cells.py` `f3/` (F3's family: 4 seats and `filter(str)` × `IN`, `NOT IN`, `EXISTS`, `NOT EXISTS`, a scalar comparison × 4 probes, and 7 pins) | 107 | 33 EQUAL → EQUAL, 50 fixed, 24 still wrong |
| `delta_cells.py` `f2/` (F2's family: 25 outer-reference shapes) | 25 | 1 EQUAL → EQUAL, 20 fixed, 4 still wrong |
| `delta_cells.py` `f1/` (F1: 38 stray-marker shapes) | 38 | 37 EQUAL → EQUAL (the same error text), 1 still wrong |
| `delta_cells.py` `nat/` (the native door, head against a06c) | 160 | 160 identical |
| verifier 1's sets (`v_df`, `v_in`, `v_pow`, `v_retire`, `v_spec`, `v_stray`, `v_tgt`, `v_tgt2`, `v_trip`, `v_uni`, `v_union`, both modes) | 736 in all | 544 EQUAL → EQUAL, 110 fixed, 82 still wrong |
| verifier 1's native and retirement-native sets | 20 | 20 identical |
| verifier 2's sets (`attack_in`, `attack_reg11`, `attack_stray`, `attack_trip`, `attack_union`, both modes) | 880 in all | 806 EQUAL → EQUAL, 56 fixed, 18 still wrong |
| verifier 2's `attack_native` | 212 | 212 identical |

Every verifier stray cell that was EQUAL → WRONG on the build (`v_df` `stray_*`, `v_stray`, `native_stray`,
`attack_trip` `stray/*`, `attack_stray` `s/nested`, `s/upper_name`, `attack_native` `nat/stray_*`) is EQUAL → EQUAL
with a06c's exact text. The 18 F3 cells and the 18 F2 raises of the build are fixed.

**The delta's still-wrong cells**, each the same answer as a06c's:
- `f3/{in,notin}/*/{fexpr_select,fexpr_withcolumn,selectexpr}` (24): an `IN` subquery in a select list. The physical
  planner refuses it, as listed above. On the `F.expr` seats the error text now prints the probe as `Float64`, where
  a06c printed the `decimal(30,15)` it had cast it to.
- `f2/lateral_union_inside`, `lateral_agg`, `scalar_select`, `scalar_select_agg`: each refuses on a06c and on the head
  with the same text (a `LATERAL` with a union inside, a `LATERAL` aggregate, a select-list scalar subquery, a scalar
  subquery over a `VALUES` alias).
- `f1/explain`: `EXPLAIN` of a stray marker. Spark answers the error as plan text; a06c and the head raise it, with
  the same text.
- The verifiers' still-wrong cells are the pre-existing ones their verdicts list: `v_in`'s DECIMAL(38,0) and (38,38)
  lists, `v_pow`'s `POW` underflow, `v_spec`'s `0.1+POW(2,-55)`, `EXPLAIN` text, CACHE TABLE, DESCRIBE QUERY and
  PREPARE, the RCTE type error, and `attack_trip`'s `stmt/execute` and `view/ice`, which answer `double` where Spark
  refuses the DDL, as on the build.

Raw: `raw/delta-<spark|a06c|head|new>-<mode>.jsonl` (`head` is the build's `pkg-p2-7906b0e3`, `new` the delta's),
`raw/delta-verdicts-vs-a06c.txt`, `raw/verifier-sets-vs-a06c.txt`, `raw/preimage-delta-verdicts.txt` and
`raw/p2b-delta-verdicts-vs-a06c.txt`.

## Raw sets

[raw/](raw/).
