# map — docs/

## Purpose

Engineering contracts, decision records, settled designs, the port plan, release-engineering
notes, per-tier operating manuals, and the one user-facing set ([guide/](guide/map.md)) for this
repo.

## Contents

- [guide/](guide/map.md) — the **user-facing guides** (the only directory here written for a user
  rather than a contributor): install + first session, session/conf, the DataFrame API, the two SQL
  doors. Nothing there is authoritative — each guide links the document that owns the fact (the
  divergence registry, [../STATUS.md](../STATUS.md), the ADRs) and every snippet it shows was
  executed against a built module.
- [artifacts/](artifacts/map.md) — **published report pages**: dated, standalone HTML copies of the
  status and roadmap pages the orchestrating session publishes as Claude artifacts (first:
  the 2026-08-30 roadmap status). Renderings only — every page cites the documents that own
  its facts, and a filed page is never edited; a later report is a new file.
- [testing.md](testing.md) — the mandatory testing contract (tests-with-code hard block,
  test-per-change, divergence-class claims, calibration-per-domain, the entry-point matrix,
  relocation discipline, the forbidden list). Read before any code change.
- [fork-sync.md](fork-sync.md) — the iceberg-rust fork **sync contract**: pin-bump procedure
  (`make bump-fork-pin`), green-before-pinnable, upstream-flows-through-the-fork, the
  weekly `fork-sync-drift` drift report with its thresholds, and the dated **pin-history**
  table (one row per dedicated bump PR; started 2026-08-15; RP-1 `5e7b2e4` on 2026-08-23).
- [spark-sql-iceberg-parity.md](spark-sql-iceberg-parity.md) — the **divergence registry**: the
  single home for how repark differs from Apache Spark. Four fields per row (repark's behavior,
  Spark's behavior with its oracle basis, the `path::test_name` that pins it, the rationale) and
  two classes — DECLARED (§2 statement-surface gaps by surface, §3 identifier resolution, §4 type
  and value semantics, §5 facade drop-in semantics) and BACKLOG (§7 "Known Spark-parity
  divergences"; `MOR-1` retired 2026-08-23 / RP-1). §6 is the lifecycle: how a row is added, mirrored and retired, and the boundary
  with [../STATUS.md](../STATUS.md) — STATUS holds issue *state*, this file holds *semantics*, and
  neither restates the other. §8 is the drop-in disclosure rationale table. Cited by name from
  ~16 live sites (Rust refusal messages, facade docstrings, facade tests), and indexed in
  [../repo-manifest.toml](../repo-manifest.toml) so a move is a red gate. A row without a live pin
  is not admitted.
- [design/](design/map.md) — settled design documents, one per deliberate design pass
  ([design/session-api.md](design/session-api.md): the phase-1 Session API — crate layout,
  seams, forced-edit ledger, omissions ledger, server landing map;
  [design/sql-doors.md](design/sql-doors.md): the phase-2 two-SQL-doors design —
  delegate-first crate layout, Q1–Q15 ANSI rulings, seam freeze, census + matrix discipline;
  [design/session-extension-conf-seam.md](design/session-extension-conf-seam.md): the dated
  superseding note the 2026-08-08 seam freeze requires, for `SessionExtension::configure`'s
  argument change;
  [design/python-facade.md](design/python-facade.md): the phase-3 Python binding + facade +
  census design — ten edit classes, Q1–Q10 rulings, the census/acceptance procedure, the
  seven-PR slate;
  [design/product-contract.md](design/product-contract.md): product-honesty contracts for
  Catalog-API-only introspection (G3-E3), one-`sql()`-one-commit (G3-E4), and post-DDL
  catalog visibility (G3-E7) — every claim cites a real test or pinned refusal).
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
  document. It holds [history/port-v2/](history/port-v2/map.md) — the v1 → v2 port's four
  phase briefs, seventeen unit ledgers, execution log with its retrospectives, and the
  [promotion ledger](history/port-v2/promotion-ledger.md) — and
  [history/frontdoor/](history/frontdoor/map.md) — the Agent-Agnostic Front-Door campaign's design,
  slate, unit ledger and [retrospective](history/frontdoor/retrospective.md). Nothing here has to be
  read to work in this repository.
- [tier2-aws.md](tier2-aws.md) — operator runbook for the tier-2 live-AWS workflow: environment,
  the repo+branch+environment-scoped OIDC trust policy, the scratch-only IAM posture
  (§2 — Glue still no-delete; **OD-3 `s3:DeleteObject` on the warehouse scratch prefix**;
  OD-3b's S3 Tables statements applied 2026-08-28, measured by MW-10 on format v2 —
  PutTableData measured **allow** by the first owner dispatch, 2026-08-30;
  including the **separate catalog-wide read-only `glue:GetDatabases`/`glue:GetTables`
  statement** that registration's provider walk requires, which cannot be scratch-scoped),
  the S3 lifecycle expiry, variable/secret names (§4 — **environment scope preferred**,
  repository level equivalent), first-dispatch acceptance (§5 — including the stale-namespace
  pre-check: an existing scratch database is adopted idempotently and keeps its OLD
  `LocationUri`). Corrections carried back from the first green live run, 2026-08-10; the run's
  status itself lives in [../STATUS.md](../STATUS.md).
- [release.md](release.md) — release engineering (wired and proven): PyPI trusted-publishing
  setup, bootstrap-token revocation, the structural crates.io deferral, what the first tags
  settled, and the still-open items. Which versions shipped is [../STATUS.md](../STATUS.md);
  the cut-a-tag runbook is [../.agents/skills/publish-pypi/SKILL.md](../.agents/skills/publish-pypi/SKILL.md).

## I want to...

| ...do this | go to |
|---|---|
| Learn to *use* repark (install, session, DataFrame, SQL doors) | [guide/map.md](guide/map.md) |
| Understand the testing rules | [testing.md](testing.md) |
| Find out how repark differs from Apache Spark, and why | [spark-sql-iceberg-parity.md](spark-sql-iceberg-parity.md) |
| Record a divergence (or retire one) | [spark-sql-iceberg-parity.md](spark-sql-iceberg-parity.md) §6 |
| Understand the phase-1 crate layout / Session API | [design/session-api.md](design/session-api.md) |
| Understand the phase-2 SQL doors / ANSI surface | [design/sql-doors.md](design/sql-doors.md) |
| See why the frozen `SessionExtension` seam changed | [design/session-extension-conf-seam.md](design/session-extension-conf-seam.md) |
| Understand the phase-3 port / census gate / edit classes | [design/python-facade.md](design/python-facade.md) |
| See product contracts (list_tables / sql() boundary / post-DDL visibility) | [design/product-contract.md](design/product-contract.md) |
| See the port phases / acceptance gate | [port/PLAN.md](port/PLAN.md) |
| Run a census / compare two runs | [port/census.md](port/census.md) |
| Understand why a load-bearing decision was made | [adr/map.md](adr/map.md) |
| Read the portable engineering method | [../.agents/skills/engineering-method/SKILL.md](../.agents/skills/engineering-method/SKILL.md) |
| Set up trusted publishing / plan a release | [release.md](release.md) |
| Find out how the engine got here (the port record) | [history/port-v2/README.md](history/port-v2/README.md) |
| Read the Front-Door campaign's record and retrospective | [history/frontdoor/README.md](history/frontdoor/README.md) |
| Check where a rule from an archived document lives now | [history/port-v2/promotion-ledger.md](history/port-v2/promotion-ledger.md) (port) · [history/frontdoor/retrospective.md](history/frontdoor/retrospective.md) "Promotion check" (front door) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../AGENTS.md](../AGENTS.md) is the project contract; these docs expand parts of it.

## Debug

First checks: if a rule is unclear, [testing.md](testing.md) + [../AGENTS.md](../AGENTS.md) are
authoritative. Escalate to: [../map.md#debug](../map.md).

| Symptom | First check |
|---|---|
| A refusal message cites a `spark-sql-iceberg-parity.md` section you cannot find | The citing site and the registry drifted. The document is indexed in `repo-manifest.toml`, so it exists; re-read [spark-sql-iceberg-parity.md](spark-sql-iceberg-parity.md) §1 for the section layout and fix whichever side is wrong |
| A divergence is described in two places | One of them is wrong by construction — [spark-sql-iceberg-parity.md](spark-sql-iceberg-parity.md) §6 states the boundary: STATUS holds state, the registry holds semantics |
