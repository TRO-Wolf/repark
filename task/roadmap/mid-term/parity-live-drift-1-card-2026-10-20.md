# Card PARITY-LIVE-DRIFT-1: the live-parity nightly is red on one cell, the ICE-META-DELETE-1 live-oracle recorder

**Date:** 2026-10-20. **Filed by:** Claude (claude-opus-5-5), clerk, step 0 of the 2026-10-20
Iceberg re-prioritisation.

**Status:** open. Not scheduled. The 2026-10-20 plan names the unit that takes it,
PARITY-LIVE-RERECORD-1, as step 1 of the one Opus 5.5 lane; that plan awaits the owner's adopt.

**Retires:** when the drifting cells are explained (a Spark behaviour change re-recorded with a
dated note, or a recorder defect fixed) and the `parity-live` nightly is green two mornings in
a row (the 10-17 replan's decision D5, carried as the 2026-10-20 plan's decision P5; both await
the owner).

## What fails

One test of the `parity-live` workflow (`.github/workflows/parity-live.yml`, job "live PySpark
oracle (facade + REPARK_PARITY_LIVE)"):

```
FAILED python/repark/tests/test_ice_meta_delete_1.py::test_live_oracle_fixture_reproduces - AssertionError: assert 1 == 0
meta-delete cells checked=72 drift=52
1 failed, 20560 passed, 24 skipped, 151 xfailed, 267 warnings in 7573.29s (2:06:13)
```

That is run `39217661518` (2026-10-19, main `476586cd`), read with `gh run view
--log-failed`. Since #1113 (`1199cf94`, 2026-10-19) the files live under the writes subsystem
directory: the test `python/repark/tests/writes/test_ice_meta_delete_1.py`, the recorder
`python/repark/tests/writes/_record_ice_meta_delete_1.py`, the committed fixture
`python/repark/tests/writes/ice_meta_delete_1_spark_oracle.json` (recorded 2026-09-19, Spark
4.1.2, Iceberg 1.11.0, 72 cells). The test calls the recorder's `main(["--check", …])`, which
re-runs Spark on every cell and exits 1 on any cell that differs from the fixture.

## Since when

`gh run list --workflow parity-live.yml`: green on 2026-10-12 (`572130dd`) and 2026-10-13
(`8c51fbc4`); red every morning from 2026-10-14 (`d96028ee`) through 2026-10-19 (runs
`38529013387`, `38699373552`, `38867482238`, `39013569991`, `39107636861`, `39217661518`).
The 2026-10-20 run `39387205143` (main `085d77e8`) was still in progress when this card was
written; its result is not recorded here.

## The cell

The first drifting cell is `all_rows_nopred_v2_cow`. The 10-19 run drifts on 52 of 72 cells:
13 shapes, each on format v2 and v3, copy-on-write and merge-on-read. Live Spark and the
fixture agree on the rows (`[]`); they differ in the snapshot summaries. The fixture's first
seed append (`INSERT … VALUES (1,'x','a'),(2,'x','b'),(3,'y','c')`) adds one data file; the
live run adds two, so in `all_rows_nopred_v2_cow` every later `total-data-files` and the DELETE's `deleted-data-files`
are one higher (fixture 2, live 3).

## Two candidate causes

1. **A real Spark behaviour change.** Re-record the fixture with the recorder and add a dated
   note to the fixture's provenance and the ICE-META-DELETE-1 record naming what changed.
   Against it, by reading: the workflow and the fixture both name Spark 4.1.2 and Iceberg
   1.11.0.
2. **A recorder defect.** The recorder builds its session with
   `SparkSession.builder.master("local[1]") … .getOrCreate()`. `getOrCreate` returns any
   session already live in the process and keeps that session's master. The first red morning
   is the first run after `d96028ee` (ICE-SPARK-CROSS-READ-1, U-10), whose cross-read helper
   builds a `local[2]` session with `getOrCreate`, and a three-row `VALUES` on two cores can
   write two files where one core writes one. This is a lead by reading main `085d77e8`; it
   was not run, and other live modules also build `local[2]` sessions. If it holds, the fix is
   in the recorder (its own session, or a file-count-independent comparison), not a
   re-record.

## What the 2026-10-20 plan asks

Unit PARITY-LIVE-RERECORD-1, step 1 of the lane (Tue 10-20 to Wed 10-21, Opus medium, about
half a day): explain the drift, then either re-record with a dated note or fix the recorder;
exit proof is the nightly green the next morning. Gate D5/P5: two green mornings before any
label PR. The plan awaits the owner's adopt.

## Out of scope

The other 20,560 passing tests of the run; the `aws-acceptance` workflow (12/12 through 10-19
per the plan); any product change to DELETE.
