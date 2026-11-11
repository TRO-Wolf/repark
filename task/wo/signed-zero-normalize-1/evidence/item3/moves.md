# Item 3: after-tables (base `68b09d18` release build against head `015fda5ae` release build)

Legs re-run with the committed probes (`<scratch>/sz-runners/legs.sh`); labels by `item1/labels.py`; raw head legs in
[raw/](raw/). Every count is cells.

| Leg | Moves |
|---|---|
| Domain, clause rows | D1 PRUNED-WRONG→EQUAL 15, WRONG-WRITE→EQUAL 1; D2 →EQUAL 21; D3 WRONG-READ→EQUAL 5; D4 →EQUAL 47; D5 →EQUAL 63; D6 WRONG-READ→EQUAL 2, WRONG-READ→WRONG-READ (names) 3, REFUSED→WRONG-READ (names) 1; D7 PRUNED-WRONG→EQUAL 55, WRONG-WRITE→EQUAL 16; D13 REFUSED→EQUAL 2 |
| Domain, D7 refusals | PRUNED-WRONG→REFUSED 3 (`D7-update-2-v3`, `-4-cow`, `-7-v3`, zsplit) and EQUAL→REFUSED 1 (`D7-update-8-v3`), all `Mismatch between schema and batches` (`UPDATE-SCHEMA-BATCH-MISMATCH-1`) |
| Domain, outside the clause, to Spark's outcome | D12 PRUNED-WRONG→EQUAL 4 (`D12-{select,delete}-{cow,mor}`, partition `d`), D8-8 2 (`CASE d WHEN 0.0`, spark-sql REFUSED→EQUAL, native WRONG→EQUAL), D9-25 PRUNED-WRONG→EQUAL 1 |
| Additions | A1 →EQUAL 50; A2 →EQUAL 13 (native and Spark door); A3 →EQUAL 28 (10 of them were refusals, F2) |
| Table B | c1, c2, c4, c5, c6 WRONG-READ→EQUAL 16 each; c3 →EQUAL 12; m2, m3 WRONG-WRITE→EQUAL 6 each; c7, p15 rows unchanged in label |
| Extension | 56 of 58 cells change answer; `E1-dml-update-f1-v3`, `-f2-v3` and `-f5-v3` go from the committed one-row write to `UPDATE-SCHEMA-BATCH-MISMATCH-1` (below); their v2-cow twins reach the truth in this run; E6 REFUSED→ANSWERED |
| p15 (envelope) | 26 cells; the comparison's six keys had folded seat and spelling. Against the truth `{4, 6}` ([p15_labels.py](p15_labels.py)): select WRONG-READ→EQUAL 4; delete, update, `INSERT … SELECT`, CTAS and MERGE WRONG-WRITE→EQUAL 20, on spark-sql × three kinds, `filter-str`, `F.expr` and native; native CTAS and MERGE REFUSED→REFUSED 2, as on base. Every cell is literal-class and none is a part B key site |

## `UPDATE-SCHEMA-BATCH-MISMATCH-1` (accepted for part A)

A float-zero UPDATE over zsplit's two zero-only files now matches both files, so it reaches the card's path: a wrong
write becomes a refusal. No fix in this unit.

| Cell | Base | Head |
|---|---|---|
| `D7-update-2-v3`, `D7-update-4-cow`, `D7-update-7-v3` | PRUNED-WRONG (row 4 kept) | REFUSED |
| `D7-update-8-v3` | EQUAL (the card's own flaky cell) | REFUSED |
| `E1-dml-update-f1-v3`, `-f2-v3`, `-f5-v3` | one wrong row | REFUSED |
| `E1-dml-update-f1-v2cow`, `-f2-v2cow`, `-f5-v2cow` | one wrong row | the truth in this run; exposed at the v2-cow rate |

50 repeats per kind of `UPDATE … SET flag = 'upd' WHERE d <=> 0.0` on zsplit
([schema_batch_repeat.py](schema_batch_repeat.py), `raw/repeat-*.json`):

| Kind | Base refusals | Head refusals |
|---|---|---|
| v2-cow | 27 / 50 | 22 / 50 |
| v2-mor | 0 / 50 | 0 / 50 |
| v3 | 29 / 50 | 30 / 50 |

The per-statement rate is unchanged.

## The moved coercion pins (ruling 5)

`python/repark/tests/types/test_decimal_float_coercion_1.py` failed its two sweep tests in item 2's first release
facade run, and passed on the rerun. **The cause is deterministic, not a flake.** That first run executed the test as it
stood before item 2, whose 17 zero-bound legs pinned base's totalOrder answers, against the item 2 build. The legs were
moved before the rerun. Measured on this head's release build:

- the current test passes 10 runs out of 10;
- the pre-item-2 version (`0c18b6e84`) fails exactly `test_sql_door_sweep_matches_spark` and
  `test_df_door_sweep_matches_spark`, every time.

The ci-profile runs are in `handback-notes.md`, with the gates.

**Every moved expectation is a measured Spark cell.**

- In the facade test, each moved leg asserts `_oracle_ids(key)`: the recorded Spark 4.1.2 cell in
  `decimal_float_coercion_1_spark_oracle.json`. That covers the 17 `_DIVERGE_SQL` and `_DIVERGE_DF` legs and
  `df_d_str`.
- In `crates/repark-spark/tests/decimal_float_coercion.rs`, the 17 cases of `signed_zero_comparisons_answer_like_spark`
  now assert their `spark` column. [dfc_oracle_check.py](dfc_oracle_check.py) matches each one to the same oracle by
  predicate: 17 equal, 0 mismatches.
- The `EXPLAIN` pin there is plan text (F4: `Filter` lines may show the bound form), not a Spark cell.

#1292's, #1295's, unit C's and #1290's tables are replayed cell by cell by their committed pins (the Rust and
facade suites): green in debug and in the release facade run.
