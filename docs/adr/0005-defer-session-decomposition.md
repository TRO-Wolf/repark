# ADR 0005 — Defer the internal ReparkSession decomposition; driver-gated

- **Status:** Deferred (2026-08-09)
- **Deciders:** project owner + Claude
- **Related:** [../../STATUS.md](../../STATUS.md) "Architectural risks" + "Deferred capabilities"
  (the current-state record this ADR is linked from),
  [0004-server-prep-disciplines.md](0004-server-prep-disciplines.md) (everything-through-Session —
  the discipline that makes the session the accretion point),
  [../design/session-api.md](../design/session-api.md) (the engine-API design),
  [../../ARCHITECTURE.md](../../ARCHITECTURE.md) "Runtime flow 1 — session construction".

## Context

`ReparkSession` (`crates/repark-core/src/session.rs`) is the engine's one policy object, and by
design: ADR-0004's everything-through-Session rule says anything a query needs it gets from its
session, so every capability that is neither SQL grammar nor table format accretes here — runtime
and memory-pool construction, `*.sql.catalog.*` parsing and catalog registration, object-store
wiring, temp views, reader options, time travel, and the routing of `sql()` into a `SqlDialect`.
It is the crate's largest source file, and it grows with each capability.

The Agent-Agnostic Front-Door design review proposed decomposing it into named internal services
(RuntimeFactory / CatalogManager / ObjectStoreRegistry / TemporaryViewManager / QueryService /
SemanticProfile) — [../history/frontdoor/agent-agnostic-frontdoor.md](../history/frontdoor/agent-agnostic-frontdoor.md)
§3 row 10.2, deferred there as "Real engineering, not doc/agnostic work," and named again in that
design's §5 non-goals. Its own recommendation was to wait for a concrete driver. This ADR records that
ruling — the deferral, its reason, and its exit condition — rather than the refactor.

The asymmetry that decides it: a **public** surface is forever, so it gets its deliberate design
pass *before* first publication (ADR-0002 decision 5, ADR-0003 phase 1); **internal** structure is
reversible at any time, so it is shaped best by a real requirement. Decomposing with no driver
invents seams that speculation must then maintain — each one needs tests, a `map.md` entry, and a
reviewer's attention forever, and the first real caller usually wants a different cut.

## Decision

1. **Deferred, not rejected.** The internal decomposition is a recognised, recorded unit of work;
   it is simply not executed now, and no part of it is executed opportunistically alongside other
   work.
2. **The intended shape is recorded so the eventual unit starts from a sketch, not a blank page:**
   `RuntimeFactory` (config validation + `SessionConfig`/`RuntimeEnv` assembly), `CatalogManager`
   (spec parsing, registration, the `CatalogRegistry`), `ObjectStoreRegistry` (the `s3://`/`s3a://`
   store + credential bridge), `TemporaryViewManager` (the temp-view family), `QueryService`
   (`EngineContext` assembly + dialect dispatch), `SemanticProfile` (the door-facing knobs). These
   are **internal** services: the public `ReparkSession` surface does not change when it happens.
3. **Driver-gated — executed only when a concrete driver arrives, never on a schedule.** The
   triggers, precisely:
   - **PyO3 pressure** — the binding needs a *part* of session state (holding, cloning, or
     lifetime-managing one service independent of the whole session) and would otherwise reach past
     `ReparkSession` or duplicate its policy. A thin adapter that only forwards is not this trigger.
   - **A second `ExecutionBackend`** — any real backend beyond `SingleNodeBackend`, which forces
     the split between *what the session decides* and *where it executes* (the seam's honest limits:
     [../../ARCHITECTURE.md](../../ARCHITECTURE.md) "`ExecutionBackend` — what the seam is,
     honestly").
   - **Cancellation / per-query resource policy** — a query needs a scoped object with its own
     lifecycle (cancel, memory budget, admission), which is `QueryService` by another name. This is
     one of ADR-0004's three deferred server problems.
   - **Server-protocol needs** — a persistent server (Flight SQL) managing many client sessions:
     per-session credential vending, per-connection catalogs, session lifecycle independent of the
     process.
   A trigger fires when the work is *actually in flight*, not when it is imagined; naming a trigger
   in a plan is not the trigger.
4. **Meanwhile, keep the accretion honest without pre-splitting the type.** New session policy that
   already stands alone lands in its own module — `catalog_config.rs`, `object_store_s3.rs`,
   `catalog_state.rs`, `time_travel.rs`, `read_options.rs` are the existing precedent — instead of
   growing `session.rs`. That is not the decomposition; it is what keeps the deferral cheap.
5. **When a trigger does fire:** the unit is behavior-preserving (no public-surface change), lands
   with its tests in the same commit ([../testing.md](../testing.md)), updates the touched `map.md`
   files, and appends a **discharge note** to this ADR naming the driver that fired and the shape
   actually built — the note is the record that the gate was honored, not bypassed.

## Consequences

- **Positive:** no speculative seams to maintain; the type stays honestly described rather than
  prematurely carved; when the split comes it is shaped by a real caller's requirement, which is
  the one thing a speculative split cannot get right.
- **Cost:** `session.rs` stays large and keeps growing, so a change there costs more reading than
  it would in a decomposed crate, and the eventual unit gets bigger the longer it waits. The
  mitigation is decision 4 plus the two current-state entries in
  [../../STATUS.md](../../STATUS.md), which keep the debt visible instead of silent.
- **Guard:** a PR that decomposes `ReparkSession` without naming a fired trigger contradicts this
  ADR — name the driver and append the discharge note, or write a superseding ADR. Equally, a PR
  that pushes *more* unrelated policy into `session.rs` when it could stand alone contradicts
  decision 4.
