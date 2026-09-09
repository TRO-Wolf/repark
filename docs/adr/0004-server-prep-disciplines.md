# ADR 0004 — Server-prep disciplines; distribution deferred behind ExecutionBackend

- **Status:** Accepted (2026-08-06)
- **Deciders:** project owner + Claude
- **Related:** [../../PROJECT.md](../../PROJECT.md) "Distributed posture",
  [../../AGENTS.md](../../AGENTS.md) "Hard rules", [../../SECURITY.md](../../SECURITY.md).

## Context

RePark is an embedded, single-node engine today, but two futures are already visible: an optional
**persistent server mode** with an Arrow Flight SQL endpoint (so BI tools and JDBC/ODBC clients can
connect), and — much further out — distributed execution. Retrofitting a server onto an engine that
reads environment state at query time or keeps global mutable state is a rewrite; two cheap
disciplines adopted from day one keep server mode an *adapter* instead.

## Decision

1. **Everything-through-Session.** No global mutable state; no environment reads at query time;
   credentials and FileIO are held per-session. Anything a query needs, it gets from its `Session`.
2. **Bindings-as-thin-adapter.** There is **one internal engine API**; PyO3 (`repark-python`) and
   the future Flight SQL handler are both thin adapters over it. No behavior lives in a binding.
3. **Three hard server problems are consciously deferred** to the server milestone, not solved
   speculatively now:
   - per-session **credential vending** (catalog-issued, scoped credentials);
   - **Python UDFs under a shared server** (the Rust-native UDF path stays first-class);
   - **per-query resource policy** (memory budgets, admission control, cancellation).
   Governance features (column masking, row-level filters, audit) implicitly depend on server
   mode — they are only enforceable by a server — and wait for it.
4. **Distribution stays deferred behind the `ExecutionBackend` seam.** The seam is the commitment;
   the engine behind it is not chosen yet. Posture: **fleet-parallel → server mode (Flight SQL) →
   distributed single-query only if a query outgrows one box.** Fleet-parallel (many embedded
   engines, one Iceberg catalog as coordinator) is free today and covers backtest/parameter-sweep
   scale-out. At decision time evaluate library-style distribution (preserves the Session
   architecture and the Iceberg write path) against cluster-style Ballista — whose protobuf plan
   serialization our custom Iceberg write/commit nodes cannot satisfy, so writes stay
   coordinator-side regardless. Do not build Ballista-for-writes.

## Consequences

- **Positive:** server mode becomes an adapter over the existing engine API; the engine is testable
  without ambient state; multiple bindings (Python now; Flight SQL, and cheaply R/Julia/Node via
  Arrow FFI, later) cannot drift because none of them carry logic.
- **Cost:** mild day-one friction — configuration must thread through `Session` even where a global
  would be quicker, and code review must police env reads and binding thickness (the Python
  thinness gate returns with the bindings in port phase 3).
- **Guard:** a feature that "just needs a quick global" or "reads an env var at query time"
  contradicts this ADR; route it through `Session` or write a superseding ADR.

## Disposition — the Ballista audit (2026-09-08)

Decision 4's Ballista sentence rested on a premise that
[the Ballista audit](../../task/roadmap/epic-term/ballista-audit-2026-09-08.md) measured at
upstream tag `54.1.0` (`f4e66525`) and **corrected**: the protobuf format does not by itself
refuse custom plan nodes. `BallistaPhysicalExtensionCodec` is a single override slot
(`ballista/core/src/serde/mod.rs`), so an Iceberg write/commit node can travel — through a
delegating wrapper, not an additive registration.

**The ruling stands, on its other ground.** Writes stay coordinator-side because a commit must
not issue from a task, not because a plan cannot be encoded: executors produce files, one
authority commits. "Do not build Ballista-for-writes" is unchanged; only its stated reason
narrows, from "cannot serialize" to "must not commit from tasks". The audit's §26.D and §27
carry the evidence by file and line; its recommendation is to DEPEND on the four Ballista
crates at a pinned tag rather than import them.
