# Unit ledger — REPLACE-LINEAR-PIN-1 · the replace memory pin measures the operation, not the wheel

**Date:** 2026-10-22 · **Branch:** `fix/replace-linear-pin-1` · **Base:** `0512470d` (`main`)
**Model:** claude-sonnet-5-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Order:** [../../wo/replace-linear-pin-1/map.md](../../wo/replace-linear-pin-1/map.md).
**Predecessor:** [../completed/replace-linear-1-ledger.md](replace-linear-1-ledger.md) (C-002 is the pin this unit re-measures; that ledger is not edited).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** On pull request #1140 `test_replace_dict_depth40_memory_linear` failed on three heads (replace delta
9,031,680 B, 10,006,528 B and 10,002,432 B against a bound of 8,388,608 B) and passed on one, with no change to
`replace`. Owner ruling 2026-10-22 ("go with recommendations"): the pin is re-measured in its own unit and #1140 waits
behind it. The bound is never raised, the pin never skipped or marked xfail, and no run repeated to find a pass.

**Not in this unit:** `STATUS.md`, `crates/`, `.github/`, the bound, the floor, the depth, the headroom, any
re-run of CI to fish for a pass.

## PROPOSITION LEDGER — REPLACE-LINEAR-PIN-1 — 2026-10-22

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The shift that fails the pin on #1140 is file-backed pages inside the native module's mapping, not anonymous memory and not the code under test: on the runner the replace delta is `RssFile` growth equal to the `_native` mapping's growth, `RssAnon` grows 74 to 106 KB in every build, and two builds of the same `.so` read differently. | The eight-row runner table below, from facade shard 1 of draft pull requests #1144 and #1145 (two builds each). | PROVEN | `dHWM = dRssAnon + dRssFile` and `dRssFile = d(_native Rss)` hold byte for byte in all eight rows. #1140 builds 1 and 2 share `.so` sha256 prefix `11fad1e8`; replace file growth 8,257,536 B then 9,904,128 B, flat 4,587,520 B then 3,670,016 B, native pages mapped before 104,550,400 B then 150,470,656 B. Evidence: [step-a-table.tsv](../../wo/replace-linear-pin-1/evidence/step-a-table.tsv), [step-a-diag-lines.txt](../../wo/replace-linear-pin-1/evidence/step-a-diag-lines.txt). |
| C-002 | With the child running the same mode once at depth 1 and collecting it before it reads `before`, the pin's deltas stop carrying the mode's first-touch code pages: over ten local runs the flat control reads about 0.79 MB and replace about 0.95 MB, against 4.85 MB and 7.78 MB without the warm-up; the pin passes on the runner. | `test_replace_dict_depth40_memory_linear` ten times alone and the dataframe directory once under `-n 8`; the ten local deltas for both modes with and without the warm-up; one runner read of the final head. | PROVEN | Local, with the warm-up: flat 786,432 to 794,624 B (file 720,896 B, anon 65,536 to 73,728 B), replace 942,080 to 950,272 B (file 851,968 B, anon 90,112 to 98,304 B), bound 8,388,608 B on all ten. Without it: flat 4,849,664 to 4,857,856 B, replace 7,778,304 to 7,786,496 B. Removing the warm-up adds 6,836,224 B to replace and 4,063,232 B to flat, all of it file pages. What is left with the warm-up is under 1 MB of file pages that only the 40-wide statement touches, plus about 0.09 MB anon. Runner, final head with the warm-up (draft #1146, facade shard 1, the same cached `.so` prefix `b3012cd5` as main build 2): flat 344,064 B (anon 81,920 B, file 262,144 B), replace 491,520 B (anon 102,400 B, file 389,120 B) against a bound of 8,388,608 B, pin passed; main build 2 read 4,734,976 B and 7,593,984 B on the same binary. Pin file 10/10 passed; dataframe directory 1655 passed, 4 skipped. Evidence: [local-with-warmup.tsv](../../wo/replace-linear-pin-1/evidence/local-with-warmup.tsv), [local-without-warmup.tsv](../../wo/replace-linear-pin-1/evidence/local-without-warmup.tsv), [pin-file-10-runs.txt](../../wo/replace-linear-pin-1/evidence/pin-file-10-runs.txt), [dataframe-dir-n8.txt](../../wo/replace-linear-pin-1/evidence/dataframe-dir-n8.txt). |
| C-003 | The bound and the floor are unchanged, and the pin still catches what it was built to catch: `_DEPTH`, `_HEADROOM`, `_DELTA_FLOOR` and `max(2 × control, floor)` are byte for byte what they were, and the only change to `_WORKER` is the warm-up. With the warm-up the control sits far under the floor, so the 8 MiB floor is the effective bound, and C-002 of REPLACE-LINEAR-1 recorded a blow-up (22.9 MB at N=12, 290.6 MB at N=16, death in `case_when` at N=40 on the base tree) far above it. | `git diff origin/main...HEAD -- python/repark/tests/dataframe/test_replace_linear_1.py`: five added lines inside `_WORKER` and nothing else. | PROVEN | The diff adds one `if mode == "flat"` / `else` block between `resource.setrlimit` and `before = peak_rss_bytes()`; `_run_worker`, the assertions, the three constants and the bound expression are untouched. In the worst local run the deltas with the warm-up are 10.6 times (flat, 794,624 B) and 8.8 times (replace, 950,272 B) under the 8,388,608 B floor. |

## Runner table (step A)

Draft pull requests #1144 (main `0512470d`) and #1145 (the #1140 head `46ab4255`), each with two builds; the child
printed the five `/proc/self/status` fields and the resident size of the `_native` mapping (summed from
`/proc/self/smaps`) before and after each mode. Bytes; d is after minus before; the pin delta is `VmHWM`.

| Sources | Build (`.so` sha256 prefix) | Mode | Bound | Pin delta | dRssAnon | dRssFile | d `_native` Rss | RssFile before | `_native` Rss before | Pin |
|---|---|---|---|---|---|---|---|---|---|---|
| main | 1 (`37f7f878`) | flat | none | 4,730,880 | 73,728 | 4,657,152 | 4,657,152 | 243,441,664 | 155,611,136 | |
| main | 1 | replace | 9,461,760 | 7,598,080 | 106,496 | 7,491,584 | 7,491,584 | 243,560,448 | 155,611,136 | pass |
| main | 2 (`b3012cd5`) | flat | none | 4,734,976 | 77,824 | 4,657,152 | 4,657,152 | 243,376,128 | 155,611,136 | |
| main | 2 | replace | 9,469,952 | 7,593,984 | 102,400 | 7,491,584 | 7,491,584 | 243,609,600 | 155,611,136 | pass |
| #1140 head | 1 (`11fad1e8`) | flat | none | 4,665,344 | 77,824 | 4,587,520 | 4,587,520 | 154,112,000 | 104,550,400 | |
| #1140 head | 1 | replace | 9,330,688 | 8,359,936 | 102,400 | 8,257,536 | 8,257,536 | 154,243,072 | 104,550,400 | pass |
| #1140 head | 2 (`11fad1e8`) | flat | none | 3,743,744 | 73,728 | 3,670,016 | 3,670,016 | 238,399,488 | 150,470,656 | |
| #1140 head | 2 | replace | 8,388,608 | 10,006,528 | 102,400 | 9,904,128 | 9,904,128 | 238,395,392 | 150,470,656 | FAIL |

`RssShmem` is 0 and `VmRSS` equals `VmHWM` at every read. The #1140 builds share one `.so`, so the 1.65 MB spread in
replace file growth between them is runner state (page-cache and fault granularity), not link order; the main builds,
which differ by sha, read identical native-mapping figures. The flat control carries 3.7 to 4.7 MB of file pages of its
own (the warm-up select does not run the 40-wide path), which is why a low control lowered `2 × control` to the floor in
the failing run while replace read high.

## Mutation check (not committed)

The test file from `origin/main` is the warm-up removed. Ten local runs of each variant on the same wheel are in the
two `local-*.tsv` files: the replace delta falls from about 7.78 MB to about 0.95 MB and the flat control from about
4.85 MB to about 0.79 MB when the warm-up is present, and the entire difference is `RssFile`; `RssAnon` is the same
within 30 KB. The pin file passes either way on this box because 7.78 MB sits under the 9.7 MB bound its own control
sets; the runner is where the variance crossed the floor.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: replace-linear-pin-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-001 to C-003 are each read against a measurement, not a paraphrase. The runner table
        proves the shift is file pages; the ten local runs prove the warm-up removes it; the diff proves
        the bound, floor, depth and headroom are untouched.
      artifacts: [task/wo/replace-linear-pin-1/evidence/step-a-table.tsv, task/wo/replace-linear-pin-1/evidence/local-with-warmup.tsv]
    - id: AT-2
      status: ATTACKED
      evidence: The warm-up runs at depth 1 in both modes, the smallest input of each statement shape,
        and the measured statement keeps depth 40. The flat and replace children are separate
        interpreters, so neither warm-up warms the other.
      artifacts: [python/repark/tests/dataframe/test_replace_linear_1.py]
    - id: AT-3
      status: N/A
      justification: A test-only change to a subprocess measurement; no failure or retry path of the product is touched, and the pin's own failure assertions are unchanged.
    - id: AT-4
      status: N/A
      justification: Each worker is a fresh interpreter run serially by the test; no shared or concurrent state is added.
    - id: AT-5
      status: N/A
      justification: No input from outside the repository, no network, no secrets; the child reads only its own /proc status.
    - id: AT-6
      status: N/A
      justification: No stored value, schema or file format changes; nothing a user stores or reads is touched.
    - id: AT-7
      status: ATTACKED
      evidence: The pin's purpose is the resource claim. With the warm-up the floor is the effective
        bound, and C-002 of REPLACE-LINEAR-1 recorded 22.9 MB at N=12 and 290.6 MB at N=16 on the base
        tree, so a return of the exponential still fails the pin by a wide margin.
      artifacts: [task/ledgers/completed/replace-linear-1-ledger.md, task/wo/replace-linear-pin-1/evidence/local-with-warmup.tsv]
    - id: AT-8
      status: ATTACKED
      evidence: The change is five lines inside the worker string. The bound expression, the three
        constants and the assertions are byte for byte unchanged; the docstring, the module header and
        the comment gate are untouched.
      artifacts: [python/repark/tests/dataframe/test_replace_linear_1.py]
    - id: AT-9
      status: ATTACKED
      evidence: Every touched directory's map.md carries the change with its pins citation, the work
        order and its evidence map exist, and the release-notes draft carries the one line.
      artifacts: [python/repark/tests/dataframe/map.md, python/repark/tests/map.md, task/wo/replace-linear-pin-1/map.md, task/roadmap/mid-term/v1-5-6-release-notes-draft.md]
    - id: AT-10
      status: ATTACKED
      evidence: The mutation (warm-up removed) is measured on the same wheel for ten runs and the delta
        rises by the code-page amount in both modes, so the warm-up is not decorative. Branch liveness
        holds: the added block has a nameable input on which it changes the output, every first run of
        a mode.
      artifacts: [task/wo/replace-linear-pin-1/evidence/local-without-warmup.tsv, task/wo/replace-linear-pin-1/evidence/local-with-warmup.tsv]
  complete: true
```

Moved to completed on 2026-10-30 by the staging-ledger lifecycle round (governance step 5): the unit merged on main as `9d873074` (2026-10-22, PR #1148); clauses as written.
