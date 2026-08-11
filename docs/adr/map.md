# map — docs/adr/

## Purpose

Architecture Decision Records — short, dated, append-only docs capturing load-bearing decisions and
*why* they were made, so future work (and reorganizations) does not silently undo them. One file per
decision, numbered `NNNN-slug.md`. Supersede with a new ADR rather than rewriting an old one.

## Contents

- [0001-own-iceberg-fork.md](0001-own-iceberg-fork.md) — **own** the `TRO-Wolf/iceberg-rust` fork as
  a sibling sub-project (separate repo, never vendored; rev-pin via `[patch.crates-io]` from port
  phase 1); its `iceberg-datafusion` consumed as a supported product surface; MERGE stays
  RePark-owned; DataFusion never forked.
- [0002-two-sql-doors.md](0002-two-sql-doors.md) — ANSI/Trino-style native dialect + Spark-dialect
  facade; no blended parser; shared Iceberg machinery beneath both; dual-spelling rule (+ one test
  row per door) for new SQL surface.
- [0003-copy-then-rehome-port.md](0003-copy-then-rehome-port.md) — the four port phases
  (bootstrap → engine core → two doors → facade + parity), the census-multiset acceptance gate,
  v1-freeze at milestone one, public ≠ released.
- [0004-server-prep-disciplines.md](0004-server-prep-disciplines.md) — everything-through-Session +
  bindings-as-thin-adapter; the three deferred server problems (credential vending, Python UDFs,
  resource policy); distribution deferred behind the `ExecutionBackend` seam.
- [0005-defer-session-decomposition.md](0005-defer-session-decomposition.md) — **Deferred**: the
  internal `ReparkSession` decomposition into named services is driver-gated (PyO3 pressure, a
  second `ExecutionBackend`, cancellation / per-query resource policy, server-protocol needs),
  never scheduled; the intended shape and the discharge-note requirement are recorded there.
- [0006-hide-iceberg-metadata-tables-from-enumeration.md](0006-hide-iceberg-metadata-tables-from-enumeration.md)
  — the fork's synthesized `$`-metadata names are hidden from `SHOW TABLES` /
  `information_schema` at the **catalog layer** (`MetadataProjectionSchemaProvider::table_names`),
  never in a door parser; they stay addressable by name (the Trino shape). Records the evidence
  (both reference engines hide them; the live tier has no Iceberg and so cannot observe it), the
  rejected "keep and declare" alternative, and the fork-repin removal/breakage criteria.

## I want to...

| ...do this | go to |
|---|---|
| Understand the owned-fork model (why + wiring) | [0001-own-iceberg-fork.md](0001-own-iceberg-fork.md) |
| Understand the two SQL dialects / add SQL surface | [0002-two-sql-doors.md](0002-two-sql-doors.md) |
| Understand the port sequencing + its acceptance gate | [0003-copy-then-rehome-port.md](0003-copy-then-rehome-port.md) (+ [../port/PLAN.md](../port/PLAN.md)) |
| Understand Session/bindings rules or the distributed posture | [0004-server-prep-disciplines.md](0004-server-prep-disciplines.md) |
| Know whether to refactor `ReparkSession` (and what would unlock it) | [0005-defer-session-decomposition.md](0005-defer-session-decomposition.md) |
| Understand why `$`-metadata tables do not show in `SHOW TABLES` (and still resolve) | [0006-hide-iceberg-metadata-tables-from-enumeration.md](0006-hide-iceberg-metadata-tables-from-enumeration.md) |
| Record a new load-bearing decision | add `NNNN-slug.md` here (Status/Context/Decision/Consequences) + a Contents row |
| See the project intent these decisions serve | [../../PROJECT.md](../../PROJECT.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../../PROJECT.md](../../PROJECT.md) (intent), [../../AGENTS.md](../../AGENTS.md) (the
  contract an ADR may amend), [../port/PLAN.md](../port/PLAN.md) (the port plan ADR-0003 anchors),
  [../../task/lessons.md](../../task/lessons.md).

## Debug

First checks: an ADR records intent at a point in time — if code and an ADR disagree, the code is
truth; add a superseding ADR. Escalate to: [../map.md](../map.md) `## Debug`.
