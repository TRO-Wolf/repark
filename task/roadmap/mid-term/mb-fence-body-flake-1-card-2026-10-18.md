# Card MB-FENCE-BODY-FLAKE-1: the two-driver fence pin saw a fenced foreachBatch body run

**Date:** 2026-10-18. **Filed by:** Claude (claude-opus-5-5), executor of unit MB-FENCE-BODY-FLAKE-1.

**Status:** the fenced-body race is fixed in the unit's pull request (ledger
[mb-fence-body-flake-1](../../ledgers/staging/mb-fence-body-flake-1-ledger.md)). One question
stays open: the second way the same pin fails, its 10 s bound, below.

**Retires:** when the unit's pull request merges and the owner rules on the open question (keep
the bound and file the slow-disk ending under PA-15, or another dated ruling).

## Why

CI run 39157958057 (PR #1106, no Rust change, so main's tree), job "Rust test run
(workspace)", 2026-10-18 19:26 UTC, attempt 1:

```
test microbatch::fence_tests::two_drivers_on_one_sink_land_each_epoch_exactly_once ... FAILED
assertion `left == right` failed: a fenced body ran: foreachBatch: true, round 6: Ok(true) Err(Fenced { query: QueryId(8060f9e1-15c6-5a38-a2e6-a62aba189656), epoch: Epoch(0), winner: RunId(e432e6f4-7b41-4b1a-8d08-e2c010f06fba) })
  left: [0, 0, 1, 2]
 right: [0, 1, 2]
```

Epoch 0 ran in both drivers' bodies: the winner's and the fenced driver's. No epoch was missing.

## Known facts (measured 2026-10-18)

- **The cause is in the product.** `Run::enter_scope` (`crates/repark-core/src/microbatch/run.rs`)
  loaded the sink and only then entered the process-wide sink scope. A second driver in the same
  process could load the sink, the first driver could then commit epoch 0 and leave the scope,
  and the second driver entered the scope with the old handle. Its resume-point check read that
  old handle, found no epoch 0, and ran the body. The trailing stamp's epoch check then refused
  it `Fenced`. The MB-3 ledger D-19 claims the opposite for every race inside one process: "A
  driver fenced at the batch's resume-point check never runs the body".
- **A deterministic pin reproduces the CI failure exactly.**
  `fence_tests.rs::a_winner_that_commits_during_the_batch_load_never_lets_a_fenced_body_run` parks
  the second driver's batch load after the catalog has answered, runs the first driver to the
  end, then lets the second go on. On main it fails every time with CI's text: `Ok(true)
  Err(Fenced { epoch: Epoch(0), .. })`, `left: [0, 0, 1, 2]`. The `toTable` half stays green on
  main because the append fence refuses the stale append.
- **The fix.** The batch enters the scope first, under the sink uuid fixed at `register`, and
  loads the sink under it. Another driver in the process commits only inside the scope, so the
  resume-point check now sees every such commit. A sink whose uuid changed is re-entered under
  its new uuid, as main did. No contract changes: the body still runs before the stamp (D-2) and
  is still at-least-once against another process (D-19).
- **The replays.** See the ledger's C-005 for the counts on main and on the fix, idle and
  loaded, by the test binary and by nextest.

## The open question: the 10 s bound

The same pin also fails a second way on this host, on main and on the fix alike:
`lifecycle_tests.rs:131: the query ends within the bound: Elapsed(())`. That is the `ended`
helper's 10 s bound, the PA-15 family
([production audit](../../../docs/cutover/production-audit-2026-10-11.md) PA-15).

Measured 2026-10-18 on this host, one run of the pin under `strace -f -T -e trace=fsync`:
192 `fsync` calls, p50 12 ms, p90 16 ms, p99 0.82 s, max 1.76 s, 8.8 s of a 21.3 s run, with
10.7 s of CPU. A batch writes its Iceberg files to a local warehouse, and each file write ends
in an `fsync`, so a few slow `fsync`s on a busy disk add seconds to a batch. `/proc/pressure/io`
read `full avg300` 13 % while other work shared the disk. A query that drains three such
batches can pass 10 s. This is slow per-batch work on a slow disk, one of the three causes
PA-15's required result names. In the measured run the CPU time and the `fsync` time together
nearly fill the wall time, so this unit found no wait in the driver behind the slow runs; it
did not look for one beyond that.

The owner's rules forbid raising the bound or serialising the test. The choice for the owner:
keep the bound and record this measurement under PA-15 as its classification, or rule
otherwise.
