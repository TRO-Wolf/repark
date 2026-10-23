# Unit ledger — SPILL-REACH-FAIR-POOL-FLAKE-1 · the small-fair-pool spill probes get a pool the loaded join probe fits in

**Date:** 2026-10-22 · **Branch:** `fix/spill-reach-fair-pool-flake-1` · **Base:** `a556289d` (`main`)
**Model:** claude-sonnet-5-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Order:** [../../wo/spill-reach-fair-pool-flake-1/map.md](../../wo/spill-reach-fair-pool-flake-1/map.md).
**Card:** [../../roadmap/mid-term/spill-reach-fair-pool-flake-1-card-2026-10-14.md](../../roadmap/mid-term/spill-reach-fair-pool-flake-1-card-2026-10-14.md).

**Retires:** this ledger moves to `../completed/` when clause C-006 is closed by a read of the CI shard that holds
`test_t2_spill_reach.py` on this pull request's head and after it (or by the owner declining that read).

**Why now.** `test_sort_merge_join_spills_under_small_fair_pool` refused with `ExternalSorterMerge` on CI on 2026-10-05
(shard 2), 2026-10-13 (shard 0) and 2026-10-22 (shard 3, PR #1142, run 39763165319, one re-run allowed by the owner). Owner
ruling 2026-10-22 ("Re run", then "go with your recommendations"): measure the flake and size the pool. The bound of no
assertion is raised, nothing is skipped or marked xfail, and no run is repeated to find a pass.

**Not in this unit:** `crates/`, `.github/`, `STATUS.md`, any assertion of the file, the two 16M pins, the batch size of
8192, `sort_spill_reservation_bytes`, and the hangs (carded apart as FAIR-POOL-HANG-1).

## PROPOSITION LEDGER — SPILL-REACH-FAIR-POOL-FLAKE-1 — 2026-10-22

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | At the 64 MiB default the join probe refuses under load and not alone: 0 of 30 fresh-process runs alone, 4 of 67 repeats of `pytest python/repark/tests/perf -n 16` (6.0 %); the sibling `test_distinct_spills_under_small_fair_pool` refused 0 of 30 and 0 of 67. | Step A, local, ci-profile module: 30 alone runs and 68 loaded repeats of both probes with a record per run. | PROVEN | Every refusal is `Failed to allocate additional ... for ExternalSorterMerge[0\|1]` from the 64.0 MB fair pool, the card's signature. Local only: the runner rows are `NOT RUN` (C-006). Evidence: [step-a-summary.tsv](../../wo/spill-reach-fair-pool-flake-1/evidence/step-a-summary.tsv), [step-a-local-alone.tsv](../../wo/spill-reach-fair-pool-flake-1/evidence/step-a-local-alone.tsv), [step-a-local-loaded.tsv](../../wo/spill-reach-fair-pool-flake-1/evidence/step-a-local-loaded.tsv). |
| C-002 | The 64 MiB default is thin: in all seven refusals recorded in full (four of step A, three of the stress) the request exceeded the bytes the pool quoted as available, and in three the figure was zero; and alone the join probe refuses 20 of 20 runs at 48M, 9 of 20 at 56M and 0 of 20 at 64M and at 72M. | The request, held and available bytes parsed from each refusal; a 20-run alone sweep over four pool sizes. | PROVEN | Requests 262 KB to 2.3 MB; the unspillable `ExternalSorterMerge` reservation (seeded with the 1 MiB `sort_spill_reservation_bytes`) is refused when spillable consumers hold their fair share, so the pool size is the margin. The join's own `peak_mem_used` reaches 52 MiB of the 64 MiB pool. The pool-wide peak is not readable from Python. Evidence: [step-a-local-pool-sweep.tsv](../../wo/spill-reach-fair-pool-flake-1/evidence/step-a-local-pool-sweep.tsv), [step-a-local-hang-diag.tsv](../../wo/spill-reach-fair-pool-flake-1/evidence/step-a-local-hang-diag.tsv), [step-b-stress-spill-records.tsv](../../wo/spill-reach-fair-pool-flake-1/evidence/step-b-stress-spill-records.tsv). |
| C-003 | At 72M, the smallest candidate, the sweep ordered for step B meets its criterion: 0 refusals and 0 assertion failures in 20 alone runs and 30 loaded repeats of the whole file, and every passing run of the two spill probes spills (join 14 to 23, distinct 7 to 10). | 4 pools (64M control, 72M, 80M, 96M) taking turns; per pool 20 alone runs and 30 `-n 16` repeats beside `python/repark/tests/perf/`. | PROVEN | All nine tests passed in 20 of 20 alone and 30 of 30 loaded runs at 72M (one loaded repeat was killed by a slow catalog-IO test of another file after this file's nine had passed). 80M and 96M also refused 0 times (join 10 to 14 spills). The 64M control refused 0 of 59 in this protocol too, so the protocol alone cannot tell 64M from 72M (C-004). Evidence: [step-b-sweep-summary.tsv](../../wo/spill-reach-fair-pool-flake-1/evidence/step-b-sweep-summary.tsv), [step-b-sweep-spill-counts.tsv](../../wo/spill-reach-fair-pool-flake-1/evidence/step-b-sweep-spill-counts.tsv), [step-b-runs.tsv](../../wo/spill-reach-fair-pool-flake-1/evidence/step-b-runs.tsv). |
| C-004 | 72M is better than 64M on the evidence pooled across conditions: 64M refused the join probe 7 of 280 loaded-type runs (4 of 67 step A, 0 of 29 sweep, 3 of 184 stress), 72M 0 of 221 (0 of 30 sweep, 0 of 191 stress); one-sided Fisher p = 0.016, 95 % upper bound for 72M 1.3 %. | 200 separate processes of the file per pool, 20 at a time, beside the sweep. | PROVEN | The conditions differ (a shared box at load 2 to 77, `-n 16` versus 20 concurrent processes), and within the stress alone 3 of 184 against 0 of 191 is p = 0.12, so the support is the pooled count plus the dose along the pool size, not one clean experiment. Evidence: [step-b-stress-summary.tsv](../../wo/spill-reach-fair-pool-flake-1/evidence/step-b-stress-summary.tsv), [step-b-stress-spill-counts.tsv](../../wo/spill-reach-fair-pool-flake-1/evidence/step-b-stress-spill-counts.tsv). |
| C-005 | The change is one knob and every assertion is unchanged: the default of `_small_fair_session` goes from `"64M"` to `"72M"`; the two 16M pins still pass `"16M"`; the batch size 8192, the partitions, the reservation and every `assert` are byte for byte what they were. The module docstring's "64 MiB pool" now reads 72 and the helper docstring's "the same 64 MiB pool" now reads "the same pool"; no docstring gains a sentence. | `git diff origin/main...HEAD -- python/repark/tests/perf/test_t2_spill_reach.py`: three changed lines and nothing else. | PROVEN | All nine tests pass at the new default in 20 alone runs and 30 loaded repeats (C-003); the join probe at 72M spills 13 to 23 times, so the probe keeps its teeth against the 8192-row calibration. |
| C-006 | The first CI run of the shard that holds `test_t2_spill_reach.py` on this pull request's head passes, and the flake does not return at 72M on CI. | A read of that run and of later runs of the same shard; the runner half of step A, which was not measured. | PROVEN | The push of the throwaway diagnostic branch (`tmp/spill-flake-diag`) and its draft pull request were denied by the permission system on 2026-10-22 and not retried; the criterion was adapted by the owner's word to the local loaded sweep. The patch is kept as [diag-branch.patch](../../wo/spill-reach-fair-pool-flake-1/evidence/diag-branch.patch), not applied. Closing question: does the shard still refuse, and if it does, is 80M the next candidate (0 refusals in 229 loaded-type runs, 1 of 200 stress processes hung against 10 at 72M)? | Read 2026-10-23: on the 72M head `44f4be98c` facade shard 0 (the shard holding the file) passed on the pull request's first run (2026-10-22 18:48 UTC); on the merged 80M head `c88d434a` main's smoke run 39876158818 and CI run 39876158786 both passed, shard 0 included. Two runner samples, one per pool; no refusal and no hang on the runner at either. The pull request's own final-head run was cancelled by the merge (orchestrator note, merge script fixed the same day), so main's run is the sample of record.
| C-007 | A second symptom exists and is not fixed here: tests of this file sometimes never return from the native call (`to_arrow_batches` or `to_arrow`), at a rate that falls with the pool and does not reach zero at 72M; in the stress 18, 10, 1 and 1 of 200 processes hung at 64M, 72M, 80M and 96M, and once in 20 alone runs at 64M. | The stacks and counts of every hung run in step A and step B. | PROVEN | The finding is the symptom and its dose, not a cause; no native stack was taken. It is carded as [FAIR-POOL-HANG-1](../../roadmap/mid-term/fair-pool-hang-under-load-1-card-2026-10-22.md) and may be the stall of FACADE-HANG-98-1. Evidence: [hang-samples.md](../../wo/spill-reach-fair-pool-flake-1/evidence/hang-samples.md), [step-b-stress-summary.tsv](../../wo/spill-reach-fair-pool-flake-1/evidence/step-b-stress-summary.tsv). |
| C-008 | Orchestrator ruling 2026-10-22, under the owner's "go with your recommendations": the default is 80M. It supersedes the 72M of C-003 and C-005, which stay as measured. At 80M, in the tables already measured: 0 refusals and 0 assertion failures in 20 alone runs and 30 loaded repeats of the whole file, 0 of 199 join-probe refusals in the stress, the join probe still spills 12 to 14 times and the distinct probe 6 to 8, so every `spill_count > 0` assertion is unchanged and keeps its teeth; and the pool-dependent hangs fall from 16 of 200 stress processes at 64M and 9 at 72M to 1 at 80M. | The sweep and stress rows at 80M; the final file on the new head: 10 alone runs and the perf directory once under `-n 16`. | PROVEN | Reason: the ordered criterion (smallest pool with zero refusals and spills in every run) could not separate 64M from 72M on a quieter box, so the deciding measurement is the stress table; in CI a hang costs a 45-minute job timeout and an owner-ruled re-run, while a refusal costs one red cell. The diff is the default argument (`"80M"`) and the docstring digit that names the pool. The final file on the new head, at a box load of 45 to 68 from other builders: 10 of 10 alone runs passed all nine tests, and `python/repark/tests/perf` under `-n 16` gave 604 passed, 24 skipped in 410 s. Evidence: [step-b-sweep-summary.tsv](../../wo/spill-reach-fair-pool-flake-1/evidence/step-b-sweep-summary.tsv), [step-b-stress-summary.tsv](../../wo/spill-reach-fair-pool-flake-1/evidence/step-b-stress-summary.tsv), [step-b-final-file-gates-80m.tsv](../../wo/spill-reach-fair-pool-flake-1/evidence/step-b-final-file-gates-80m.tsv). |

## Step A, the 64M default (local)

| Condition | Test | Runs | Pass | Refused | Spill count of passes (min / median / max) |
|---|---|---|---|---|---|
| alone, fresh process | join probe | 30 | 30 | 0 | 22 / 45.5 / 50 |
| alone, fresh process | distinct probe | 30 | 30 | 0 | 8 / 10 / 10 |
| `-n 16` beside `python/repark/tests/perf/` | join probe | 67 | 63 | 4 | 16 / 43 / 50 |
| `-n 16` beside `python/repark/tests/perf/` | distinct probe | 67 | 67 | 0 | 8 / 10 / 11 |
| runner, alone and under shard load | both | not run | | | |

## Step B, the sweep (4 pools taking turns)

| Pool | Alone: all nine passed | Loaded: all nine passed | Refusals | Join probe spills (min / median / max) | Distinct probe spills | Hung in this file (loaded, alone) |
|---|---|---|---|---|---|---|
| 64M (control) | 19 of 20 | 27 of 30 | 0 | 21 / 47 / 50 | 8 / 9 / 10 | 3, 1 |
| 72M | 20 of 20 | 30 of 30 | 0 | 14 / 15 / 23 | 7 / 8 / 10 | 0, 0 |
| 80M | 20 of 20 | 29 of 30 | 0 | 12 / 13 / 14 | 6 / 8 / 8 | 1 (16M pin), 0 |
| 96M | 20 of 20 | 29 of 30 | 0 | 10 / 10 / 12 | 6 / 8 / 8 | 1 (16M pin), 0 |

## Stress supplement (200 processes per pool, 20 at a time)

| Pool | Join probe refused | Join probe spills | Processes hung | In tests that use the pool under test |
|---|---|---|---|---|
| 64M | 3 of 184 | 14 / 20 / 50 | 18 | 16 |
| 72M | 0 of 191 | 13 / 15 / 17 | 10 | 9 |
| 80M | 0 of 199 | 12 / 13 / 14 | 1 | 1 |
| 96M | 0 of 200 | 10 / 10 / 11 | 1 | 0 |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: spill-reach-fair-pool-flake-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-001 to C-004 are each read against a count, not a paraphrase. The refusal rate at the old
        default, its dose along the pool size, the ordered sweep and the pooled comparison are tables in
        the evidence directory; the weakness of the ordered sweep (its control refuses nothing either) is
        stated in C-003 and answered in C-004.
      artifacts: [task/wo/spill-reach-fair-pool-flake-1/evidence/step-b-sweep-summary.tsv, task/wo/spill-reach-fair-pool-flake-1/evidence/step-b-stress-summary.tsv]
    - id: AT-2
      status: ATTACKED
      evidence: The size is tested at the extremes of the assertion, not the middle. The probe's teeth are the
        spill count above zero; the lowest count in any passing run at 72M is 7 (distinct) and 13 (join), and
        96M still spills (10), so the move does not let the sort finish in memory under the 8192-row batch.
      artifacts: [task/wo/spill-reach-fair-pool-flake-1/evidence/step-b-sweep-spill-counts.tsv]
    - id: AT-3
      status: ATTACKED
      evidence: The failure path is the subject. The refusals are parsed down to the consumer, the bytes
        requested, held and quoted as available, and the hang sites down to the Python frame that did not
        return; the product behaviour behind both is carded, not changed.
      artifacts: [task/wo/spill-reach-fair-pool-flake-1/evidence/hang-samples.md, task/wo/spill-reach-fair-pool-flake-1/evidence/step-a-local-hang-diag.tsv]
    - id: AT-4
      status: ATTACKED
      evidence: The race is the finding. The refusal and the hang appear under contention and fall with the
        pool; the stress runs 20 processes at once on 26 cores to reproduce the contention the shard has.
        Each sweep run is a fresh process or a fresh xdist session, so no session state is shared across
        runs.
      artifacts: [task/wo/spill-reach-fair-pool-flake-1/evidence/step-b-stress-summary.tsv, task/wo/spill-reach-fair-pool-flake-1/evidence/step-b-runs.tsv]
    - id: AT-5
      status: N/A
      justification: A test-default change; no input from outside the repository, no network, no secrets, no stored value.
    - id: AT-6
      status: N/A
      justification: No schema, file format or user-visible value changes; the pool is a test parameter.
    - id: AT-7
      status: ATTACKED
      evidence: The resource claim is the pool. At 72M the probe still spills in every passing run, and the two
        16M pins that assert the refusal of a hash join and of array_agg keep 16M and keep passing, so the
        suite still proves both that a bounded pool spills and that it refuses where it cannot.
      artifacts: [python/repark/tests/perf/test_t2_spill_reach.py, task/wo/spill-reach-fair-pool-flake-1/evidence/step-b-sweep-summary.tsv]
    - id: AT-8
      status: ATTACKED
      evidence: The code change is three lines of one test file, two of them docstring numbers. Every
        assertion, both 16M pins, the batch size, the partitions and the reservation are untouched, and the
        comment gate prints nothing.
      artifacts: [python/repark/tests/perf/test_t2_spill_reach.py]
    - id: AT-9
      status: ATTACKED
      evidence: Every touched directory's map.md carries the change with its pins citation, the work order and its
        evidence map exist, the card's status line is appended and not rewritten, the hang card has its own
        row, and the release-notes draft carries the one line.
      artifacts: [python/repark/tests/perf/map.md, python/repark/tests/map.md, task/wo/spill-reach-fair-pool-flake-1/map.md, task/roadmap/mid-term/v1-5-6-release-notes-draft.md]
    - id: AT-10
      status: ATTACKED
      evidence: The mutation is the old default. The same nine tests at 64M refuse the join probe 7 of 280 times
        and hang 18 of 200 stress processes against 0 of 221 and 10 of 200 at 72M, and the 56M and 48M
        alone cells refuse 9 and 20 of 20, so the pool size is the lever and the pin has a nameable input on
        which it changes.
      artifacts: [task/wo/spill-reach-fair-pool-flake-1/evidence/step-a-local-pool-sweep.tsv, task/wo/spill-reach-fair-pool-flake-1/evidence/step-b-stress-spill-counts.tsv]
  complete: true
```
