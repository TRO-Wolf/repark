# Card OCC-DISJOINT-MERGE-FLAKE-1: eight concurrent disjoint MERGEs sometimes commit two instead of one

**Date:** 2026-10-14. **Filed by:** Muse Spark (muse-spark-1.3-contributor), clerk, from the orchestrator's carried list.

**Status:** open, not scheduled.

**Retires:** when repeated runs settle whether the second winner is a product race or a test assumption, and the test or the product changes accordingly with a dated row.

## Why

`python/repark/tests/test_ice_occ_scoped_1.py::test_disjoint_key_merges_refuse_like_spark`
with parameter `[3-serializable-dataframe]` failed on CI on 2026-10-07 17:40 UTC with
`assert (2, 6) == (1, 7)`: RePark committed 2 of 8 where Spark's recording commits 1.
The losers raise `DataInvalid => Found conflicting files that can contain records
matching TRUE`, which is the expected loser shape; only the winner count moved.

## Known facts

- The test fires eight MERGEs at once, one per key (`MERGE INTO … ON t.id = s.id WHEN
  MATCHED THEN UPDATE SET v = s.v` for ids 0–7), released together through a
  `threading.Barrier`, on a merge-on-read table under serializable isolation, through
  the DataFrame door (`mergeInto`).
- The `ON` clause is a join equality with no target-only conjunct, so the conflict
  filter is `TRUE` by ledger clause C-012 of
  `task/ledgers/staging/ice-occ-scoped-1-ledger.md`: eight MERGEs on disjoint keys with
  no target-only conjunct commit 1 of 8, and every loser raises Spark's serializable /
  snapshot message with filter `TRUE`, on both isolation levels and both doors.
- The assertion compares winner and loser counts against Spark's recorded cell
  (`committed` 1 of 8) and then checks the row totals; the retry budget and the loser
  message class are the same pins that pass on green runs.
- How two winners can happen: each MERGE validates against the snapshot it scanned,
  then commits. Two threads can both finish the conflict check against the same base
  snapshot before either commits, so the second committer's validation never sees the
  first committer's new files and both land.
- Unmeasured: how often the double win reproduces, and whether Spark's own 1-of-8
  recording is itself the stable answer under repetition.

## The open question

Product race (the validation window between the conflict check and the commit admits
two winners and must close) or test assumption (the test pins exactly one winner while
concurrent commit ordering legitimately varies run to run)? The card records both
readings and rules neither out.

## The ask

- Replay the eight-MERGE storm N times on this tree and record the winner-count
  distribution with the loser messages of each run.
- If two winners recur, the commit path owns a product race and this card hands it a
  measured repro.
- If one winner holds across the repetitions, re-examine what the CI run's timing
  assumed and whether the pin needs a stated tolerance with a dated row.
