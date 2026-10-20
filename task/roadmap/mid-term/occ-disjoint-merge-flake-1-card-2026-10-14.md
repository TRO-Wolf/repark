# Card OCC-DISJOINT-MERGE-FLAKE-1: eight concurrent disjoint MERGEs sometimes commit two instead of one

**Date:** 2026-10-14. **Filed by:** Muse Spark (muse-spark-1.3-contributor), clerk, from the orchestrator's carried list.

**Status:** open. Measured 2026-10-18 by unit OCC-DISJOINT-MERGE-FLAKE-1 (legitimate ordering); waits on the owner's ruling on the pin's tolerance.

**Retires:** when repeated runs settle whether the second winner is a product race or a test assumption, and the test or the product changes accordingly with a dated row.

## Why

`python/repark/tests/writes/test_ice_occ_scoped_1.py::test_disjoint_key_merges_refuse_like_spark`
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

## Seen again, 2026-10-16

On 2026-10-16 at 11:52 UTC the same test failed with the parameter `[3-snapshot-dataframe]`,
on the first CI attempt of PR #1085 (run 38895233903, facade shard 2): `assert (2, 6) == (1, 7)`,
"RePark committed 2 of 8, Spark 1". The second attempt passed. PR #1085 changes CI only, so
the tree was main's. The shape is the same as on 2026-10-07, now under snapshot isolation as
well as serializable.

The orchestrator has put the eight-MERGE storm on the list for U-9 (ICE-INTEGRITY-PINS-1) as a
drill cell: repeated replays under both isolation levels and through both doors, with the
winner count of each run. It also asks whether the retry after a commit conflict runs the
conflict check again. This follows a suggestion the owner passed on, 2026-10-16. It is not a
ruling, and the card stays unscheduled until U-9's brief is set.

## Seen twice more, then measured, 2026-10-18

Two more CI sightings on 2026-10-18, same assertion and loser shape: `[2-serializable-dataframe]`
at 19:39 UTC (run 39157964851) and `[3-serializable-dataframe]` at 20:35 UTC (run 39162834196,
facade shard 2: 1 failed, 5739 passed in 149 s). All four sightings are the DataFrame door.

The owner authorised the unit the same day. Its ledger is
[occ-disjoint-merge-flake-1-ledger.md](../../ledgers/staging/occ-disjoint-merge-flake-1-ledger.md);
the tables and the `path:line` reading live there.

**Answer to the open question: legitimate ordering, not a product race.**

- The commit path cannot land two MERGEs that pinned the same base. The fork validates on every
  commit attempt against a fresh catalog load, the retry after `CatalogCommitConflicts`
  included, and the in-memory catalog swaps the pointer under one lock against the base it
  validated. The existing pin
  `crates/repark-iceberg/src/write/merge/tests/occ_scoped.rs::disjoint_key_merges_on_an_unpartitioned_table_still_conflict_on_true`
  lands the racing write after the victim's validation and before its swap; it is green.
- A second winner appears when one MERGE loads its base after another has committed. That
  schedule is serial, and both engines accept it. With one thread held back 300 ms, RePark
  commits 2 of 8 in 75 of 80 runs over the eight cells. The late thread is always a winner. Its
  MERGE call always follows the first winner's return. On v3 its snapshot rewrites the first
  winner's deletion vector, so its scan read the first winner's snapshot. Live Spark 4.1.2 with
  one thread held back 3 s commits 2 of 8 in 40 of 40 runs.
- With no injection, the storm committed exactly 1 of 8 in 870 harness runs and 120 runs of the
  real test id: idle, on 4 cores shared with three facade-suite processes, and with four
  busy-loop hogs added. Spark committed 1 of 8 in 120 of 120 barrier runs. The CI double win did
  not reproduce on this box.
- Why the DataFrame door: both doors run the same `execute_merge`. The DataFrame door does more
  work under the GIL between the barrier and the MERGE's table load, so its threads start later
  and further apart. A stall of one thread longer than the winner's whole MERGE (40 ms or more
  of slack here, less on a release wheel) makes the schedule serial. A full GC run on one thread
  at the barrier made the SQL door commit 2 of 8 in 10 of 10 runs, so the SQL door is not
  immune.

**What changes:** nothing in the product and nothing in the pin. The pin's tolerance is the
owner's ruling; the options and their costs are in the ledger's "Questions for the owner". The
card stays open until that ruling.
