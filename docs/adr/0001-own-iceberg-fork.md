# ADR 0001 — Own the iceberg-rust fork; never fork DataFusion

- **Status:** Accepted (2026-08-06; carries forward decisions made 2026-06-06 and 2026-07-01 in the
  private v1 repository — this ADR condenses that history for the public repo).
- **Deciders:** project owner + Claude
- **Related:** [../../PROJECT.md](../../PROJECT.md), [../../AGENTS.md](../../AGENTS.md)
  "Version-pin contract", and — in the fork repo `TRO-Wolf/iceberg-rust` —
  `docs/ENGINE_CONTRACT.md` (the engine-facing seam RePark consumes) and
  `docs/parity/GAP_MATRIX.md` (the only capability-status record).

## Context

Upstream iceberg-rust seals its write-extension seams: the transaction-action and commit machinery
needed to build a full write engine (`MERGE`, overwrite, snapshot management, evolution) is
`pub(crate)` and cannot be reached from the public API. Building the write engine as a consumer of
the registry crate was tried in v1 and abandoned: first as a one-line patch, then — once the scope
of the write surface was clear — by pivoting to a fork-and-own program. `TRO-Wolf/iceberg-rust` is
a Rust-native implementation targeting **1:1 capability parity with Java `iceberg-core`**,
maintained indefinitely; its Phase 2 delivered the native write/commit action surface.

## Decision

1. **Own the fork.** `TRO-Wolf/iceberg-rust` is a **sibling sub-project RePark owns**, not an
   untouchable upstream. It stays a **separate repository, never vendored** into this repo; end
   users never see it — wheels compile it in. The engine-agnostic table-format work (write
   actions, schema/partition evolution, snapshot management, views, maintenance) lives **in the
   fork**; RePark carries only engine-flavored adapters.
2. **Rev-pin via `[patch.crates-io]`.** When the workspace gains crates (port phase 1), the whole
   `iceberg*` family (`iceberg`, `iceberg-datafusion`, `iceberg-catalog-glue`,
   `iceberg-catalog-s3tables`) is sourced from the fork by a **rev-pinned** git dependency, with
   `Cargo.lock` checked in. A proof-test naming a fork-only public symbol pins the patch so a
   silent fall-back to the registry crate cannot compile.
3. **The fork's `iceberg-datafusion` is a supported product surface, not a reference.** RePark uses
   its `IcebergCatalogProvider` / `IcebergTableProvider` for scan + `INSERT` + `DELETE` + `UPDATE`
   (copy-on-write and merge-on-read) instead of re-implementing partition-aware writers. The
   boundary rule: **anything provable by the fork's Java interop oracle lives in the fork
   (including engine-generic DataFusion execs); anything engine-flavored (SQL dialects, function
   semantics, session policy, facades) lives in RePark.**
4. **MERGE INTO stays RePark-owned** (fork `ENGINE_CONTRACT.md` §6): source↔target join, clause
   application, and the cardinality-violation guard are built here, committing through the fork's
   public actions. Capabilities the fork does not yet expose are raised as items in the fork's
   queue, not worked around here.
5. **DataFusion is never forked.** It remains a normal upstream dependency. Family bumps
   (`datafusion` + `datafusion-spark` + `arrow*`/`parquet` + `rust-toolchain.toml` + the fork rev)
   move in lockstep; upstream majors are skipped until the fork moves its base, with a dated
   take/skip decision per release.

## Consequences

- **Positive:** the full Iceberg write / evolution / maintenance surface is reachable through
  natively-supported, Java-parity actions in code we own — no fragile public-API workarounds, no
  hand-rolled commit machinery, one oracle-tested DML implementation instead of two drifting
  copies. RePark's surface stays a thin, parity-tested translation layer.
- **Cost:** we maintain a real fork indefinitely, coordinated across repos, and pay a sync-spike
  per family bump. The fork's `iceberg-datafusion` public API is consumer-facing; fork-side changes
  there follow its ENGINE_CONTRACT.
- **No exit trigger:** ownership is the long-term state. Upstream `apache/iceberg-rust`
  improvements are cherry-picked opportunistically; mergeability is not a constraint.
