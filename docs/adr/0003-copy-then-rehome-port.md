# ADR 0003 — Copy-then-re-home port in four phases

- **Status:** Accepted (2026-08-06)
- **Deciders:** project owner + Claude
- **Related:** [../port/PLAN.md](../port/PLAN.md) (the operational plan this ADR anchors),
  [../testing.md](../testing.md) "Relocation discipline", [../../PROJECT.md](../../PROJECT.md).

## Context

RePark V2 lands in a fresh public repository with a new crate skeleton, while the private v1
repository serves production today. Two porting strategies were considered: rewrite into the new
skeleton (risks silent behavior drift and unverifiable "done"), or start Spark-shaped and extract
the native API later (explicitly rejected — deferred extractions don't happen, and a public API is
forever). A fresh-history public repo also removes the old make-public history-audit problem: no
sensitive operator data has ever existed in this git history, by construction.

## Decision

1. **Copy-then-re-home.** Each phase starts from a literal copy of v1's code, re-homed into the
   target skeleton commit by commit, so **every intermediate state is runnable** and
   census-checkable. Tests port **with their names** (relocation discipline in
   [../testing.md](../testing.md)).
2. **Four phases:**
   - **Phase 0 — bootstrap** (this phase): process assets before code — governance contracts,
     testing contract, mechanical gates, map.md guard, SEPMO, tier-1 CI — all green on an empty
     workspace.
   - **Phase 1 — engine core:** `repark-core` (the Session-centric internal engine API — the one
     deliberate design pass) + `repark-iceberg` (from v1's catalog + write crates) + the fork pin +
     the Rust unit-test tier.
   - **Phase 2 — the two SQL doors:** `repark-spark` (port) + `repark-sql` (ANSI/Trino-style
     Iceberg DDL design pass). dbt-repark can start in parallel once this lands.
   - **Phase 3 — Python facade + parity = milestone one:** `repark-python` thin adapter, the
     PySpark facade, the parity harness, census machinery. Gate: **v1's full test suite green on
     V2.**
3. **Acceptance gate: the census multiset, byte-flat across repos** — the v1 test census counts
   (classic 135 files / 345 tests, expand 42/171, expand2 41/167, plus the full-extras facade
   count) must be reproduced exactly by the ported tree; a missing or renamed test is a gate
   failure, not a footnote.
4. **v1 freezes to bugfix-only at milestone one** (end of phase 3 — the full v1 suite green on V2,
   not merely the Rust core), so there is never a window where neither repo can serve production;
   all feature work then moves here.
5. **Public ≠ released.** Phases 1–3 churn freely in public; the API-forever clock starts at the
   first tagged PyPI release, held until milestone one.

## Consequences

- **Positive:** behavior preservation is checkable at every commit (runnable intermediates + the
  census gate); the port cannot silently drop tests or semantics; production always has a serving
  repo.
- **Cost:** re-homing is slower than rewriting, and the census gate forbids "tidying while
  porting" — refactors come after milestone one, as their own units.
- **Open item (tracked, not resolved here):** the production cutover plan — per-job migration of
  the operator's pipelines from v1 to V2. Both engines share the same Iceberg tables, so
  parallel-run is low-risk, but write-path jobs need a **single-writer-per-table** rule during the
  window.
