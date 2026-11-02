# Unit ledger — MB-FENCE-BODY-FLAKE-1 · a fenced foreachBatch body never runs inside one process

**Scope (2026-10-18):** the CI failure of
`microbatch::fence_tests::two_drivers_on_one_sink_land_each_epoch_exactly_once` on run
39157958057 (PR #1106, main's tree): on the `foreachBatch` door a driver ended `Fenced` at
epoch 0 after its body had run epoch 0. Measure it, find whether the product or the pin is
wrong, and fix the product only if step 0 proves a gap.

**Date:** 2026-10-18 · **Branch:** `fix/mb-fence-body-flake-1` · **Base:** `origin/main`
`02bf6fe5` · **Model:** Claude (claude-opus-5-5) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Card:** [../../roadmap/mid-term/mb-fence-body-flake-1-card-2026-10-18.md](../../roadmap/mid-term/mb-fence-body-flake-1-card-2026-10-18.md).
The clauses it rests on: the [MB-3 ledger](mb-3-ledger.md) D-19 and C-002, C-005, C-022.
**This ledger closes when the unit's pull request merges.**

## PROPOSITION LEDGER — MB-FENCE-BODY-FLAKE-1 — 2026-10-18

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Step 0, on main `02bf6fe5`: a second driver of a query in the same process runs its `foreachBatch` body for an epoch that the first driver committed, when the first driver commits that epoch and leaves the sink's scope while the second driver loads the sink for its batch. `Run::enter_scope` loads the sink, then enters the scope, so the batch's resume-point check reads a handle from before the commit; the trailing stamp's epoch check then refuses the batch `Fenced`. This is CI's failure, and it breaks MB-3 D-19: "A driver fenced at the batch's resume-point check never runs the body, which is every race inside one process." | A deterministic pin that holds the window open, red on main with CI's text; the path read with file:line. | **PROVEN** | `fence_tests.rs::a_winner_that_commits_during_the_batch_load_never_lets_a_fenced_body_run`, red on main 4 of 4: `a fenced body ran: foreachBatch: true, scope free at the load: true: Ok(true) Err(Fenced { .., epoch: Epoch(0), winner: RunId(..) })`, `left: [0, 0, 1, 2]`, `right: [0, 1, 2]`. CI's attempt-1 log: `left: [0, 0, 1, 2]`, `right: [0, 1, 2]`, the same endings. The path is §1. The `toTable` half of the pin is green on main: the second driver's precheck passes on the old handle, and the append fence refuses its commit `Fenced` (measured: `foreachBatch: false, scope free at the load: true: Ok(true) Err(Fenced { .., epoch: Epoch(0), .. })`). |
| C-002 | A batch enters the sink's scope before it loads the sink. Every commit of another query in the process happens inside that scope, so the handle the resume-point check reads shows it, and a driver that another run of its query has fenced at an epoch never runs its body for that epoch, on both doors. The contract does not change: the body still runs before the trailing stamp (MB-3 D-2) and stays at-least-once against a driver in another process (D-19). | The C-001 pin green on the fix; the two-driver pin unchanged and green; main's order restored as a mutant reds the C-001 pin. | **PROVEN** | `run.rs` `enter_scope`: `BatchScope::enter` under `QueryShared::sink_uuid` (read at `register`), then `load_sink`. The C-001 pin is green on the fix on both doors (the second driver's load parks inside its own scope, so the first driver runs after it and its body sees no epoch). `two_drivers_on_one_sink_land_each_epoch_exactly_once` is byte-identical to main and green (C-005). Mutation MAIN-ORDER (`run.rs` from `origin/main`, the new pins kept): `fence_tests` reads 6 passed, 1 failed, the C-001 pin. |
| C-003 | A sink replaced between two batches still takes the next batch under its own scope, as on main: when the table loaded under the scope has another uuid, the batch leaves that scope, enters the new uuid's scope and loads again. | A pin that replaces the sink between batches, green on main and on the fix; the re-entry removed as a mutant reds it. | **PROVEN** | `fence_tests.rs::a_sink_replaced_between_batches_takes_the_next_batch_under_its_own_scope` (both doors, `ProcessingTime(0)`): epoch 0 lands, the sink is dropped and created again at a new location, a second file is inserted, epoch 1 lands on the new table (`[1]`), `stop` returns `Stopped` with durable epoch 1, the body sees `[(0, [1]), (1, [2])]` or the sink holds `[2]`. Green on main (MAIN-ORDER mutant) and on the fix. Mutation MISMATCH (the batch keeps the handle whatever its uuid): red 1, `epoch 1 never landed on the replaced sink` (by reading, the stamp then claims no scope and the batch ends `RecoveryRequired(UnstampedSinkCommit)`). |
| C-004 | Nothing else moves: the sink loads per batch are the same count (MB-3 C-022), and the other microbatch pins pass. | The reload pin unchanged; the `microbatch` module green. | **PROVEN** | `reload_tests.rs::the_driver_loads_the_sink_fresh_for_every_batch` unchanged and green: the batch still loads once inside `enter_scope`, now after the scope is held. `cargo test -p repark-core --lib microbatch::` on the fix: 89 passed, 0 failed. |
| C-005 | The replays of the two-driver pin, idle and loaded, by the test binary and by nextest, on main and on the fix, are recorded with every failure message. | The counts and messages, §2. | **PROVEN** | §2. |
| C-006 | The same pin fails a second way on this host, on main and on the fix alike: `lifecycle_tests.rs:131: the query ends within the bound: Elapsed(())`, the `ended` helper's 10 s bound (the PA-15 family). In the measured runs the CPU time and the `fsync` time on the local warehouse together nearly fill the wall time. This unit does not change the bound; the card carries the question. | The failure counts per lane; one run under `strace -T` and one under `/usr/bin/time`. | **PROVEN** | §2: on main 11 of 209 runs red, on the fix 3 of 64, every one this message. §3: 192 `fsync` calls in one run, p50 12 ms, p99 0.82 s, max 1.76 s, 8.8 s of a 21.3 s run with 10.7 s of CPU; a run of 32.2 s wall had 11.3 s of CPU. |
| C-007 | The unit's gates (2026-10-18) on the branch merged with `origin/main` `cfb20a16` (#1105's nextest `rust-test`, #1103's fork repin): `make verify` green; `make preflight` not run (a `repark-core`-only change). | Both `make verify` runs, with their exit codes. | **PROVEN** | Run 1: exit 2. `make ci` green; nextest `7671 tests run: 7670 passed (11 slow), 1 failed, 97 skipped`, the failure `microbatch::lifecycle_tests::two_sessions_on_one_sink_wait_for_the_scope` in 28.3 s, `lifecycle_tests.rs:131:10: the query ends within the bound: Elapsed(())` (PA-15, the C-006 mode, the whole workspace on 26 cores). That test alone, 15 runs each on four cores, main's `enter_scope` against the fix: 15 green and 15 green, 1980–2524 ms and 1998–2551 ms. Run 2: exit 0, nextest `7671 tests run: 7671 passed (4 slow), 97 skipped`, the doctests green; the four pins of this unit pass in it. |

## 1. Step 0 — the path on main `02bf6fe5`

`crates/repark-core/src/microbatch/run.rs`:

- `run_batch` 303–371 builds the stamp (310–321) and calls `enter_scope` (322).
- `enter_scope` 213–236 loads the sink (219) and then enters the scope (220,
  `BatchScope::enter(TableUuid::of(&sink), ..)`); on `SinkBusy` it waits `polling_delay` and
  loads again (222–227).
- The resume-point check (325–333) reads that handle; `already_durable` (373–397) returns
  `Fenced` (386–391) with no body call.
- `foreach_batch` 435–456 runs the body (443), loads the sink again (449) and commits the
  trailing stamp under the scope token (450–451). The guard drops at 354.

`crates/repark-iceberg/src/write/sink_offsets.rs`: the scope is a process-global map keyed by
the sink's table uuid (69–75, `enter` 114–133, `Drop` 193–197). The trailing stamp's claim
(`claim_checked` 143–175) runs `epoch_check` (268–298), which refuses `Fenced` at 289–297; the
append fence refuses the same at the commit (`commit_stamp_only` 425–481).

The window: the second driver's load (219) reads the metadata from before the first driver's
stamp commit; the first driver commits and drops its guard (354); the second driver's
`enter` (220) succeeds on the old handle; its check (325) finds no epoch 0; its body runs (443);
its reload (449) and `epoch_check` refuse it `Fenced { epoch 0, winner }`.

The other reading, an epoch missing because the winner's body ran after the test read `seen()`,
is ruled out: `ended` returns from `await_termination` (`driver.rs` 804–834), which waits for the
`done` signal that `Run::drive` (85–91) sends after `trigger_loop` returns, and `Probe` records
`seen` inside the body (`lifecycle_tests.rs` 64–73). CI's `left` also holds every epoch.

## 2. Replays (2026-10-18)

Each run is one process running only
`microbatch::fence_tests::two_drivers_on_one_sink_land_each_epoch_exactly_once` (two doors,
twenty rounds each, so forty races per run), from a copy of the test binary so that rebuilds
did not touch it. Every loop was bounded by count. The host is shared: another agent's build
and its CPU-bound processes ran beside these lanes, and `/proc/pressure/io` read between 8 %
and 17 % `full` over the session.

| Lane | Tree | Cores | Beside it | Runs | Red | Red messages |
|---|---|---|---|---|---|---|
| burn | main `02bf6fe5` | 0–3 | another agent's four CPU-bound processes pinned to 0–3 | 27 | 2 | 2 × `lifecycle_tests.rs:131:10: the query ends within the bound: Elapsed(())` |
| idle | main | 16–19 | nothing of this unit's | 76 | 2 | 2 × the same |
| loaded | main | 4–7 | the whole `repark-core` lib suite looped on the same four cores (21 suite runs, one of them red, its output not kept) | 100 | 7 | 7 × the same |
| nextest | main | 8–11 | nothing of this unit's | 6 | 0 | — |
| fix | the fix (first form, see below) | 20–23 | nothing of this unit's | 64 | 3 | 3 × the same |

On main: **209 runs, 8,360 races, 11 red, all of them the 10 s bound, and no fenced body**. The
fenced-body failure did not reproduce by timing in these runs; the C-001 pin reproduces it
every time. The run time spread was wide: 13 s to 171 s on the idle lane (p50 22 s), 13 s to
179 s on the fix lane (p50 26 s).

`--test-threads=1` against the default is not a separate lane: each run executes one test, so
the libtest thread count does not change it; the pin's own runtime has four worker threads in
both. nextest runs each test in its own process, which is what these lanes do; the 6 nextest
runs on main were run as `cargo nextest run -p repark-core --lib -E 'test(two_drivers_on_one_sink)'`
(cargo-nextest 0.9.143) from a worktree of main that shared this clone's target directory. The
lane stopped at 6 because the shared target directory made the clone's next build reuse
main's test binary; the worktree was removed and the clone rebuilt from its own sources before
any further measurement.

The fix lane ran the fix's first form, which kept the batch's handle past `catalog_timeout`
when the sink's uuid kept changing; the final form drops that clause. The two forms are the
same on this pin, which replaces no sink.

Before the lanes, one run of the whole `microbatch` module of the fix's first form on eight
shared cores was red on the two-driver pin with the same 10 s message; the module run of the
final form on four cores was green (C-004).

## 3. The 10 s bound and the disk

One run of the pin on main, cores 24–25, under `strace -f -T -e trace=fsync`: 21.3 s wall,
10.7 s CPU, 192 `fsync` calls taking 8.8 s in total: p50 12 ms, p90 16 ms, p99 0.82 s, max
1.76 s. Under `strace -f -w -c` another run spent 6.1 s of 18.5 s in 192 `fsync` calls. Four
runs under `/usr/bin/time` on cores 20–23: 32.2 s wall with 11.3 s CPU, 17.9 / 11.2,
13.3 / 11.1, 17.2 / 11.1. One run with the round time and each driver's progress printed (a
temporary print, removed): slow rounds were slow in `addBatch`, the door's own time, 1.0 s to
3.4 s for a one-row batch where a fast round takes about 0.1 s.

Every Iceberg file the pin writes goes to the fixture's local warehouse under the system temp
directory, and each write ends in an `fsync`. When the shared disk is busy, a few slow `fsync`s
add seconds to a batch, and a query that drains three batches passes `ended`'s 10 s bound. That
is the "expensive per-batch work" reading of PA-15's required result, on this host. This unit
does not change the bound, does not move the warehouse and does not serialise the test (the
owner's standing rules); the card asks the owner for the ruling.

## 4. Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: mb-fence-body-flake-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: MB-3 D-19's in-process sentence is walked as written; the C-001 pin is that sentence for two drivers in one process, red on main and green on the fix.
      artifacts: [crates/repark-core/src/microbatch/fence_tests.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Both doors; the scope free and the scope held at the parked load; a sink whose uuid changes between batches.
      artifacts: [crates/repark-core/src/microbatch/fence_tests.rs]
    - id: AT-3
      status: ATTACKED
      evidence: A load that fails under the scope returns its error and the guard drops with it; the SinkBusy wait, the stop during the wait and the catalog timeout keep their pins in lifecycle_tests.rs and timeout_tests.rs, green in the module run.
      artifacts: [crates/repark-core/src/microbatch/run.rs, crates/repark-core/src/microbatch/lifecycle_tests.rs, crates/repark-core/src/microbatch/timeout_tests.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The race is the unit. The fix orders scope entry before the load, and the pin holds the window with a oneshot park rather than timing; replays on main and on the fix are counted.
      artifacts: [crates/repark-core/src/microbatch/run.rs, crates/repark-core/src/microbatch/fence_tests.rs]
    - id: AT-5
      status: N/A
      justification: No input, secret, credential or permission path changes; the scope key and the catalog calls are the same.
    - id: AT-6
      status: ATTACKED
      evidence: Each epoch is stamped once and the toTable rows land once on both doors in both pins; the stamp, the fence and the record format are unchanged.
      artifacts: [crates/repark-core/src/microbatch/fence_tests.rs]
    - id: AT-7
      status: ATTACKED
      evidence: The load count per batch is unchanged (reload pin); a replaced sink costs one more load; the scope is held for the duration of one load longer than before.
      artifacts: [crates/repark-core/src/microbatch/reload_tests.rs]
    - id: AT-8
      status: ATTACKED
      evidence: The foreachBatch contract (D-2, OQ-2a-2) and the toTable guarantee (D-19) are unchanged; the refusals keep their variants.
      artifacts: [crates/repark-core/src/microbatch/fence_tests.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The C-001 pin's message names the door, whether the scope was free at the load, and both endings, so a red run says which order happened.
      artifacts: [crates/repark-core/src/microbatch/fence_tests.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Mutation MAIN-ORDER reds the C-001 pin; mutation MISMATCH reds the replaced-sink pin, so the uuid re-entry branch has a nameable input.
      artifacts: [crates/repark-core/src/microbatch/fence_tests.rs]
  complete: true
```

Moved to completed on 2026-10-30 by the staging-ledger lifecycle round (governance step 5): the unit merged on main as `476586cd` (2026-10-18, PR #1108); clauses as written.
