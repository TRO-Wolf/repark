# map — docs/

## Purpose

Engineering contracts, decision records, settled designs, the port plan, release-engineering
notes, and per-tier operating manuals for this repo.

## Contents

- [testing.md](testing.md) — the mandatory testing contract (tests-with-code hard block,
  test-per-change, divergence-class claims, calibration-per-domain, the entry-point matrix,
  relocation discipline, the forbidden list). Read before any code change.
- [design/](design/map.md) — settled design documents, one per deliberate design pass
  ([design/session-api.md](design/session-api.md): the phase-1 Session API — crate layout,
  seams, forced-edit ledger, omissions ledger, server landing map;
  [design/sql-doors.md](design/sql-doors.md): the phase-2 two-SQL-doors design —
  delegate-first crate layout, Q1–Q15 ANSI rulings, seam freeze, census + matrix discipline;
  [design/python-facade.md](design/python-facade.md): the phase-3 Python binding + facade +
  census design — ten edit classes, Q1–Q10 rulings, the census/acceptance procedure, the
  seven-PR slate).
- [port/](port/map.md) — the V2 port plan ([port/PLAN.md](port/PLAN.md)): copy-then-re-home
  rules, the four phases, the census multiset acceptance gate, the v1-freeze trigger; and the
  recorded census procedure ([port/census.md](port/census.md)): the environment recipe, both
  sides' cohort argument vectors, the mandatory stability run + quarantine rule, the full-extras
  facade cohort definition, the comparator + attribution rule, and the golden-corpus `basis:`
  designations.
- [adr/](adr/map.md) — Architecture Decision Records (dated, append-only "why" docs): the owned
  iceberg-rust fork, the two SQL doors, the copy-then-re-home port, server-prep disciplines, the
  deferred session decomposition.
- [history/](history/map.md) — the **archive**: closed campaigns, off the normal read path, each
  admitted only after a promotion audit proved every rule it carries also lives in a current
  document. Today it holds [history/port-v2/](history/port-v2/map.md) — the v1 → v2 port's four
  phase briefs, seventeen unit ledgers, execution log with its retrospectives, and the
  [promotion ledger](history/port-v2/promotion-ledger.md). Nothing here has to be read to work in
  this repository.
- [skills/](skills/map.md) — per-model-tier operating manuals (Opus / Sonnet / Haiku).
- [tier2-aws.md](tier2-aws.md) — operator runbook for the tier-2 live-AWS workflow: environment,
  the repo+branch+environment-scoped OIDC trust policy, the create-only/no-delete IAM posture,
  the S3 lifecycle expiry, variable/secret names, first-dispatch acceptance.
- [release.md](release.md) — release engineering (documentation only this phase): PyPI /
  crates.io trusted publishing setup, bootstrap-token revocation, the first-tag hard blockers
  (incl. the `repark.sql` re-home gate), open items.

## I want to...

| ...do this | go to |
|---|---|
| Understand the testing rules | [testing.md](testing.md) |
| Understand the phase-1 crate layout / Session API | [design/session-api.md](design/session-api.md) |
| Understand the phase-2 SQL doors / ANSI surface | [design/sql-doors.md](design/sql-doors.md) |
| Understand the phase-3 port / census gate / edit classes | [design/python-facade.md](design/python-facade.md) |
| See the port phases / acceptance gate | [port/PLAN.md](port/PLAN.md) |
| Run a census / compare two runs | [port/census.md](port/census.md) |
| Understand why a load-bearing decision was made | [adr/map.md](adr/map.md) |
| Read the manual for your tier | [skills/map.md](skills/map.md) |
| Set up trusted publishing / plan a release | [release.md](release.md) |
| Find out how the engine got here (the port record) | [history/port-v2/README.md](history/port-v2/README.md) |
| Check where a rule from an archived document lives now | [history/port-v2/promotion-ledger.md](history/port-v2/promotion-ledger.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../AGENTS.md](../AGENTS.md) is the project contract; these docs expand parts of it.

## Debug

First checks: if a rule is unclear, [testing.md](testing.md) + [../AGENTS.md](../AGENTS.md) are
authoritative. Escalate to: [../map.md#debug](../map.md).
