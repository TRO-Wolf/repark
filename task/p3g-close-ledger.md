# Unit ledger — P3G: phase-3 close = milestone one

**Unit:** phase-3 PR-7 · **Brief:**
[../briefs/phase-3-python-facade.md](../briefs/phase-3-python-facade.md) §1 "PR-7" · **Design:**
[../docs/design/python-facade.md](../docs/design/python-facade.md) §6.6 · **Port-Source:** v1
`main` @ `fc3f48102` · **Status:** IN FLIGHT (acceptance PASSED — all four cohorts byte-flat, exit 0)

## Scope

The acceptance PR: run the v2 census across all four cohorts, compare against the committed
pin baseline through the instrument, reconcile the three populations, re-baseline PLAN.md, and
close the phase with a retrospective. Orchestrator-run (the v2 census is a local procedure with
scratch venvs + a network sparse clone; never delegated to agents with env-var access).

- **Comparator `--added` handling** (`python/repark-parity/compat/compare_reports.py`): the
  mirror of `--deferred` — a checked-in additions ledger (`task/port/added-python-tests.txt`),
  subtracted from the CANDIDATE side, echoed, junit-canonicalized, and added to the frozen
  option set (the ledger-only-subtraction property test still holds). Four new unit tests
  (added-subtracts-candidate-only both directions). First real consumer: this PR's facade
  comparison.
- **The v2 acceptance run** committed under `task/census/v2-<sha>/` (four cohorts + manifests),
  redacted through `compat.redact` (format-aware), same environment as the pin baseline.
- **Comparator outputs** for all four cohorts (pasted below), each exit 0.
- **Reconciliation** appended: `(v2_collected − added) ∪ deferred = pin_collected`, all
  populations.
- **PLAN.md re-baselined**: the stale `135/345 · 42/171 · 41/167` table replaced by the recorded
  freeze-point counts (142/345 · 44/171 · 87/167 · facade 2,509) with a pointer to the committed
  runs.
- todo.md phase-3 → DONE with the SEPMO retrospective; lessons appended.

## The acceptance run (v2, four cohorts)

Apache cohorts, byte-identical to the pin baseline through the census comparator:

| Cohort | pin (baseline) | v2 (candidate) | comparator |
|---|---|---|---|
| classic | 142/345 | 142/345 | IDENTICAL, exit 0 |
| expand | 44/171 | 44/171 | IDENTICAL, exit 0 |
| expand2 | 87/167 | 87/167 | IDENTICAL, exit 0 |
| facade | 2,471 passed + 46 skipped (2,517 junit) | 2,459 passed + 46 skipped (2,499 collected) | IDENTICAL, exit 0 |

Facade cohort: `(v2_collected − added:2) ∪ deferred:12 = pin_collected:2,509`.

Verbatim comparator verdicts (invocations in `docs/port/census.md` §5; baseline
`task/census/baseline-fc3f48102/`, candidate `task/census/v2-a5be8a7/`):

- **classic** (census mode, no deferrals): `sorted-rendering byte comparison: IDENTICAL` — `VERDICT: empty diff — exit 0`.
- **expand** / **expand2** (census mode): both `IDENTICAL` — `exit 0`.
- **facade** (junit mode, `--deferred deferred-python-tests.txt --added added-python-tests.txt`):
  `deferred_subtracted: 12`, `added_subtracted: 2`, `pass: v1=2459 v2=2459`,
  `appeared: 0`, `vanished: 0`, `IDENTICAL` — `exit 0`.

## Reconciliation (ported ∪ deferred ∪ added = pin, all populations)

- **Facade (the load-bearing one):** v2 collected 2,499 = pin 2,509 − 12 deferred + 2 added;
  the comparator subtracts both ledgers and the diff is byte-empty. `(v2_collected − added) ∪
  deferred = pin_collected` holds exactly.
- **Rust populations:** each crate's `cargo test -- --list` name-set was verified empty-diff
  against the pin (or diff = the enumerated declared additions) at its landing PR — repark-ml
  (identity + the PR-3 ml-design pin), repark-python (identity + the 5 PR-3 declared additions),
  repark-core (the 2 B-1 pins + the EngineRuntime test, all declared in p3c/p3e), repark-common/
  iceberg/functions/ta/spark unchanged. No name moved or vanished across the phase; every
  addition is enumerated in its ledger.
- **Parity harness:** the 64-name unit suite is unchanged; the comparator/`--classic`/`--added`
  additions are new-code tests declared in p3d/p3g, not ports.

## Gate results

`make ci` / `make test` / `make py-test` (145) / `make py-lock-check` / `make check-lib-py` all exit 0; 11 workflows parse; both hygiene passes (added-lines content + log metadata) zero over the branch range. The v2 census artifacts parse (JSON + JUnit XML) and carry zero forbidden-pattern hits after `compat.redact`.

## Milestone-one declaration (user-side)

Recorded here for the operator to action after merge (design §11 / PLAN.md):
- v1 declared bugfix-only.
- Cutover sequencing under single-writer-per-table settled.
- First tagged PyPI release (trusted publishers, `docs/release.md`; the `repark.sql`-is-a-module
  release-prep gate; the PyPI `repark` name is already reserved — existing-project publisher flow).
- The first `workflow_dispatch` of parity-live and aws-acceptance (design §11 acceptance steps).
