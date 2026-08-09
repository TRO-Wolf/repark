# map — task/port/

## Purpose

Port-execution accounting artifacts — the working ledgers the copy-then-re-home port maintains
across phases (as opposed to the plan itself, which lives in
[../../docs/port/PLAN.md](../../docs/port/PLAN.md)).

## Contents

- [deferred-tests.md](deferred-tests.md) — the deferred-test manifest for the phase-1 cone: every
  v1 test not yet ported, with its target phase, under the hard reconciliation rule
  (ported ∪ deferred) = v1 totals at the pinned SHA. PR-B/PR-C filled the per-crate sections;
  re-pointed 2026-08-07 to the phase-2 PR slate (rows carry PR-2/3a/3b/4 targets; the 4
  postgres/excel rows moved to the post-milestone-one bucket now recorded in
  [../../STATUS.md](../../STATUS.md) "Deferred capabilities").

- [deferred-python-tests.txt](deferred-python-tests.txt) — the **machine-readable** deferral
  allowlist for the Python facade suite (phase-3 PR-5, EC-4): one pytest node id per line, `#`
  comments ignored. This file is the ONLY subtraction input the census comparator accepts
  (`compat/compare_reports.py --deferred task/port/deferred-python-tests.txt`, subtracted from the
  BASELINE side only) — there is no flag or env var by which a row leaves the diff without
  appearing here. Its prose half is the "Python — the facade suite" section of
  [deferred-tests.md](deferred-tests.md); the two are bound by
  `python/repark-parity/tests/test_deferred_ledger.py`.
- [added-python-tests.txt](added-python-tests.txt) — the mirror ADDITIONS allowlist (phase-3
  PR-6): facade node ids that exist in v2 but NOT at the pin (v2-only capabilities), subtracted
  from the CANDIDATE side. Reconciliation identity `(v2_collected − added) ∪ deferred =
  pin_collected`. First entries: the two tier-2 AWS placeholder-bucket-guard pins. The
  comparator's `--added` handling lands in PR-7 (its first census-comparison consumer).

## I want to...

| ...do this | go to |
|---|---|
| See which v1 tests are deferred and to what phase | [deferred-tests.md](deferred-tests.md) |
| Feed the census comparator its allowlist | [deferred-python-tests.txt](deferred-python-tests.txt) |
| Add or remove a Python deferral | Edit BOTH halves — the txt and the prose section — then run `pytest python/repark-parity/tests/test_deferred_ledger.py` |
| Read the census/relocation rules the manifest serves | [../../docs/design/session-api.md](../../docs/design/session-api.md) §7 + [../../docs/testing.md](../../docs/testing.md) "Relocation discipline" |
| Read the port plan / phases | [../../docs/port/PLAN.md](../../docs/port/PLAN.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: per-unit ledgers live in [../](../map.md) (one `<unit>-ledger.md` per delivered unit).

## Debug

- `test_deferred_ledger.py` reds on "ids that are ALSO ported": a node id is listed here **and**
  still present in `python/repark/tests` — it would be subtracted from the baseline while running
  here, a silent gate hole. Excise the test or drop the row.
- `test_deferred_ledger.py` reds on "absent from the recorded pin collection": the id does not
  name a real v1 node (a typo, or a node id from a different rootdir) — the subtraction would
  remove nothing.
- Manifest and `cargo test --workspace -- --list` disagree: the reconciliation rule in
  [deferred-tests.md](deferred-tests.md) is the gate — fix the manifest or the port, never the
  rule. Escalate to: [../map.md#debug](../map.md).
