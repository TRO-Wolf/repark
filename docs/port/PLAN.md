# The V2 port plan — copy-then-re-home

How the private v1 repository's engine becomes this repo's engine. Settled 2026-08-06; this file
is the single home for the port's phases, rules, and acceptance gate.

> **Status note, 2026-08-09.** The port is **COMPLETE** (milestone one, 2026-08-08). This file
> stays live because the relocation discipline and the census acceptance gate are still defined
> against it, and because the acceptance identity below is how a future census is judged. Current
> state — including what happens next — is [STATUS.md](../../STATUS.md); the port's execution
> record (briefs, unit ledgers, retrospectives) is archived at
> [../history/port-v2/](../history/port-v2/README.md). The "Open item: cutover" below is now
> carried forward as the production-pipeline cutover inventory in STATUS.md "Current milestone".

## The shape: copy-then-re-home

Each code phase starts from a **literal copy** of the relevant v1 crates/packages, then re-homes
them **commit by commit** — renames, re-layering, and the V2 design passes land as reviewable
increments on top of a faithful copy, never as a rewrite. Two invariants:

- **Every intermediate state is runnable.** Each commit in a re-home series builds, passes
  `make ci`, and (once tests exist) passes the test suite. No "broken until the last commit"
  series.
- **Tests port with their names.** Relocation follows `docs/testing.md` "Relocation discipline":
  move-only diffs prove test-name identity mechanically (`--list` / `--collect-only` diff empty);
  anything that renames a test path ships alone as a declared-rename unit with an explicit
  old-name → new-name map.

## The four phases

- **Phase 0 — bootstrap (complete).** Gates before code: testing contract, mechanical gates,
  map.md discipline, agent contracts, SEPMO, tier-1 CI — ported and green on an empty workspace.
  Post-merge: branch protection with required checks; registry-side trusted-publisher
  configuration (maintainer action, `docs/release.md`).
- **Phase 1 — engine core.** `repark-core` (the Session-centric internal engine API — the one
  deliberate design pass of the port) + `repark-iceberg` (from v1's catalog + write crates) +
  the iceberg-rust fork pin (`[patch]`, rev-pinned) + the Rust unit-test tier.
- **Phase 2 — the two SQL doors.** `repark-spark` (Spark-dialect, ported) + `repark-sql`
  (ANSI/Trino-style native dialect — the Iceberg DDL design pass). `dbt-repark` can start in
  parallel once this lands.
- **Phase 3 — Python facade + parity = milestone one.** `repark-ml` (the native estimator
  kernels the ML facade binds — scheduled into phase 3 by the settled design,
  `docs/design/python-facade.md` §4 Q3), `repark-python` as a thin adapter, the
  PySpark facade, the parity harness, and the census machinery. Gate: v1's full suite green on V2.

## The acceptance gate: census multiset, byte-flat

The port's completion claim is mechanical, not narrative. The pyspark-compat census — the same
runner, the same module cohorts — must produce **byte-flat multiset-compared results across the
two repos**.

**Baseline = the recorded freeze-point run**, not a remembered number. The pin baseline is
committed under `task/census/baseline-fc3f48102/` and the v2 acceptance run under
`task/census/v2-<sha>/`; the comparator (`python/repark-parity/compat/compare_reports.py`)
subtracts the deferred ledger from the baseline side and the additions ledger from the candidate
side and requires a byte-flat diff. The recorded freeze-point counts (the earlier hand-written
`135/345 · 42/171 · 41/167` here were stale — design §5 F2):

| Cohort | Freeze-point count (pin) |
|---|---|
| classic | 142/345 |
| expand | 44/171 |
| expand2 | 87/167 |
| full-extras facade cohort | 2,509 collected / 2,471 passed + 46 skipped (2,517 junit outcomes) |

Cohort denominators are never blended. The reconciliation identity is
`(v2_collected − added) ∪ deferred = pin_collected`; zero *unattributed* movement is the bar
(movement is acceptable only when it maps by name to a deferred or added ledger entry). Because
tests port with their names (relocation discipline), the multiset comparison is well-defined.

## v1 freeze and release posture

- **v1 freezes to bugfix-only at milestone one** (phase 3 accepted). Until then v1 remains the
  production engine and the two repos run in parallel.
- **Public ≠ released.** Phases 1–3 churn freely in public; the API-forever clock starts at the
  first tagged PyPI release, held until milestone one. See `docs/release.md`.

## Open item: cutover

During the parallel-run window, production workloads must respect **single-writer-per-table**:
a given Iceberg table is written by v1 or by V2, never both. The cutover sequencing (which
workloads move when, and the rollback story) is an open item to be settled before milestone one.
