# Card CROSS-READ-SAMPLING-FLAKE-1: the cross-read sampling test drops a phase under host load

**Date:** 2026-10-30. **Filed by:** the staging-ledger lifecycle round (governance step 5).

**Status:** OPEN. Grade C. Not chartered; awaiting the owner's charter.

**What the test does.** `python/repark-parity/tests/test_cross_read_diagnostics.py::test_sampling_preserves_test_status_and_targets_both_phases` (18 parameter cells: 3 test exits by 2 profiler exits by 3 leg jobs) runs the cross-read watchdog shell from `aws-acceptance.yml` against fixture processes (a 0.4 s fake pytest split into two 0.2 s phases; a sampler tick of about 0.04 s) and asserts one `py-spy` sample lands in each phase, the test exit status is preserved, and all children are reaped (the `/proc` check). Its nondeterminism is wall-clock only.

**Measured.** On 2026-10-30 one of the 18 cells failed in a MAP-GOVERNANCE-1 records round whose branch did not touch the file, and the clerk re-ran it without the owner's word (recorded as a breach). Earlier, a verifier S2 on the pull request that parametrised it three times per leg saw a different cell fail per run (1 to 2 of 39) at load average about 45, and pass on rerun. A read-only Muse measurement at `7882504c` on an idle box ran the target 60 times (1,080 subtests) with 0 failures, and the whole file once under default order and once with `-x`, 39 passed each. `pytest-randomly` is not installed.

**Hypothesis (labelled as such).** Under host load a phase is starved of its sample, or the `/proc` reaping check races teardown.

**Lead for the unit that takes it.** Size the phases relative to a measured tick, or assert the sampler by count rather than by phase.

**Rule.** A one-off failure in a file the branch did not touch is reported with its output, never re-run by an agent.

**Retires.** When the sampler assertion is deterministic under load on the probe, or the charter declines it.
