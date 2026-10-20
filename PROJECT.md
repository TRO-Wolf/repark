# PROJECT.md — RePark (project intent & north star)

> **Purpose of this file.** Orient any agent or contributor to *what RePark is, why it exists, and
> the decisions that must not be undone*. The single authoritative day-to-day contract is
> [AGENTS.md](AGENTS.md) (which holds the precedence chain; [CLAUDE.md](CLAUDE.md) is a thin
> tool adapter), with a `map.md` in every directory. **This file states INTENT; the contract states
> MECHANICS** — if they conflict, reconcile, never silently discard intent.

## What RePark is

A pure-Rust, **no-JVM** single-node data engine with the deepest Apache Iceberg support of any
single-node engine. Positioning: **"Trino's SQL, DuckDB's deployment model, deepest Iceberg
support"** — a combination no current engine occupies. One `pip install`, no cluster, no daemon.
Built **on** Apache DataFusion + Arrow + our **owned iceberg-rust fork** (all pure-Rust and
JVM-free) with native PyO3 bindings. Compute runs in Rust; data crosses the Python boundary as
Apache Arrow, zero-copy.

Two user-facing **doors**, no blended parser:

- **Native door** — a lazy DataFrame API with an inspectable optimizer, plus `repark.sql()`
  speaking an **ANSI, Trino-style** dialect (catalog-determines-format CTAS, `WITH (…)` table
  properties, `FOR VERSION AS OF` time travel, maintenance as callable ops).
- **Spark facade door** — a near-drop-in PySpark facade whose `.sql()` keeps the **Spark dialect**
  unchanged, so existing PySpark pipelines migrate with only the import line changed.

The Iceberg machinery (commit semantics, MERGE, snapshots, evolution) is shared beneath both doors;
the dialect layers are thin translators.

## Goals

- Full Iceberg integration against AWS **Glue (primary)** + **S3 Tables (secondary)**. **The v1.0
  north star (owner-set 2026-08-23) is full production-grade Iceberg format-v3 support** —
  deletion vectors, row lineage, the v3 types — defined, with the acceptance gate that decides
  the v1.0 tag, in
  [task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md](task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md)
  — **reached 2026-09-03**. The release-by-release path from v0.6 through 3.0 (owner-set
  2026-08-29; v0.7–v0.10 shifted to 1.1–1.4 when v1.0.0 cut at the north-star gate) lives in
  [task/roadmap/epic-term/release-roadmap-2026-08-29.md](task/roadmap/epic-term/release-roadmap-2026-08-29.md).
  Each active release line names one primary outcome — see "Primary outcome by release line".
  **Future goal (owner-set 2026-10-12, unscheduled): the fastest known query engine on S3
  Tables** — defined as a measurable claim in
  [task/roadmap/epic-term/s3-tables-fastest-north-star-2026-10-12.md](task/roadmap/epic-term/s3-tables-fastest-north-star-2026-10-12.md);
  no lane runs on it until the owner schedules it.
- Single binary / single `pip install` — no JVM, no cluster, no daemon; cold start under a second.
- Predictable memory via spill-to-disk by default. (*The "never OOM on data larger than RAM"
  claim is measured, not assumed: DataFusion's operator spill coverage is partial, so the honest
  goal is "spills where the engine can, documented where it cannot", and the document is the
  180-cell truth table in [docs/perf/spill-matrix-baseline.md](docs/perf/spill-matrix-baseline.md)
  — which operators spill, which refuse cleanly, and which take no memory-pool reservation at
  all and so are not bounded by the pool. The standing coverage matrix since v1.3 is
  [docs/perf/spill-coverage-matrix-2026-09-11.md](docs/perf/spill-coverage-matrix-2026-09-11.md):
  24 of 27 cells stable, three named with their upstream DataFusion issues.*)
- Zero-copy interop — anything that speaks Arrow is a first-class citizen.
- Reproducibility: same query + same snapshot = same bytes, every time.
- **TA serving performance at parity with `polars_talib`** on the shapes that matter
  (many-symbols windows, wide multi-indicator SELECT statements, last-row serving) — reached
  **golden-safe**: bit-exact numerics are non-negotiable, so the wins come from
  allocation/copy/dispatch/plan-shape work, never math reordering; every optimization is
  gated on a recorded benchmark baseline (measure first, then implement). `unsafe` stays
  workspace-forbidden; a per-module exception is a last rung reached only on flamegraph
  evidence that safe restructuring cannot match, and carries Miri + fuzz gates of its own.
- Long-term ease of maintenance: strict testing contract, mechanical gates, one owned fork. The
  process serves the product: a gate earns its place by the defect class it catches.

## Primary outcome by release line

One organising outcome per active line, owner-set, pointing at the card that defines its
acceptance; a line may carry several acceptance checks, but one outcome.

- **1.5.x** — one-to-one Spark–Iceberg parity on the Spark door over the inventory in
  [task/roadmap/mid-term/ice-parity-inventory-2026-09-19.md](task/roadmap/mid-term/ice-parity-inventory-2026-09-19.md).
- **1.6** — Postgres connectivity (`repark-connect`) as a chartered seam; no Trino connector.
- **1.7** — micro-batch CDC from bronze to silver, with the durability, recovery and replay
  guarantees defined in
  [task/roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md](task/roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md).
- **1.8** — the inference readers (smart CSV, Excel, JSON) as their own crate, `repark-io`; the
  crate layout in
  [task/roadmap/epic-term/crate-layout-1-8-2026-10-01.md](task/roadmap/epic-term/crate-layout-1-8-2026-10-01.md)
  is the means, not the outcome.

## Priority order when goals collide

Inside one unit the higher goal wins and the loser is named, dated and pointed at a card — never
traded silently:

1. **Data safety.** Persisted-data semantics change only with compatibility tests against data
   the shipped engine wrote, an explicit assessment of existing rows, and a documented, tested
   repair, migration or recovery procedure, including its limits.
2. **Correctness against the oracle.** The Spark door answers as live Spark does; the native door
   as its own contract says. Measured, never inferred from reviewer consensus.
3. **The performance ceiling.** A unit that moves a reference workload past the ceiling its card
   sets does not merge (see "Performance posture").
4. **Parity completeness.** A divergence that cannot close inside 1–3 becomes a dated registry
   row; that is the honest outcome, not a failure.
5. **Surface breadth.** New names and features come last.

## What production-grade means

A capability claims production-grade only with evidence scoped to what it touches:

- measured on the AWS tier (Glue, S3 Tables) wherever it touches a catalog or object store;
- its failure modes named — what a failed commit leaves behind, which errors raise, which paths
  refuse cleanly;
- no silent degradation — a path the engine cannot handle refuses by name; a skipped, quarantined
  or degraded path is a user-visible contract, never only a log line;
- every known divergence a dated row in the registry,
  [docs/spark-sql-iceberg-parity.md](docs/spark-sql-iceberg-parity.md);
- a committed baseline for its reference workload (next section);
- for CDC, the guarantees the CDC north-star card defines: crash recovery, durability of
  acknowledged data, replay, bounded resources, observable progress.

The term's definition and review rubric: [docs/production-grade.md](docs/production-grade.md).

## Performance posture

Performance is advanced, not only defended. Each load-bearing surface has a reference workload;
its baseline is committed under the index [docs/perf/map.md](docs/perf/map.md) and its acceptance
target lives in the approved performance card, never here. Regression ceilings are set per card
by the owner, measured by the protocol recorded with that card's evidence, and a measured
trade-off is accepted only by a dated owner carve-out. Ceilings are floors of discipline, not the
goal.

| Surface | Reference workload |
|---|---|
| Iceberg scan | a full scan measured against the equivalent Parquet scan; statistics folds only where the spec makes them exact, the scan path kept |
| Iceberg DML on Glue | INSERT, MERGE, UPDATE on a Glue table, metadata round trips per statement counted |
| Analytics | grouped aggregation and windows at 1e6–1e7 rows against DuckDB on the same data |
| TA serving | many-symbol windows, wide multi-indicator SELECT, last-row serving against `polars_talib` |
| dbt-repark | the reference dbt project end to end |

## Differentiators

The reasons someone picks this over DuckDB or Polars; everything else is table stakes. Each one
names its evidence; a differentiator without a pointer is aspirational and says so.

- Deepest Iceberg support of any single-node engine, including V3 features — evidence: coverage
  of the parity inventory ([STATUS.md](STATUS.md) counts, the registry
  [docs/spark-sql-iceberg-parity.md](docs/spark-sql-iceberg-parity.md)); "deepest" against other
  engines is unmeasured.
- A native TA (technical-analysis) function library, bit-exact and fast, built in Rust — evidence:
  [crates/repark-ta/benches/map.md](crates/repark-ta/benches/map.md) and the TA rows of
  [docs/perf/map.md](docs/perf/map.md).
- Aggressive, intelligent schema inference and a CSV reader that handles real-world mess —
  evidence: the measured torture run
  [docs/perf/torture-1-2026-09-11.md](docs/perf/torture-1-2026-09-11.md) against
  [docs/design/torture-suite.md](docs/design/torture-suite.md).
- First-class Excel read/write — aspirational: `repark-io` arrives on the 1.8 layout.
- ML that trains directly off Iceberg tables out-of-core, no extraction step — aspirational: 2.x
  on the release roadmap.
- The PySpark facade: migrate existing pipelines without rewrites — evidence: the facade suite
  and the 1.5.x parity outcome above.
- The fastest known query engine on S3 Tables — a future goal, unscheduled: no AWS measurement
  is recorded on main today.

## Non-negotiable invariants (do not undo)

- **iceberg-rust is forked & owned; DataFusion is built ON (not forked).** The
  `TRO-Wolf/iceberg-rust` fork is a **sibling sub-project we own** (1:1 Java `iceberg-core`
  parity), a **separate repo, never vendored**; the engine-agnostic table-format work lives in the
  fork. `[patch.crates-io]` + rev-pin is the wiring (from phase 1). See
  [docs/adr/0001-own-iceberg-fork.md](docs/adr/0001-own-iceberg-fork.md). Ownership carries
  the sync obligations in [docs/fork-sync.md](docs/fork-sync.md): pin bumps only by their own PR,
  upstream drift tracked against its thresholds.
- **Two honest SQL doors, no blended parser** —
  [docs/adr/0002-two-sql-doors.md](docs/adr/0002-two-sql-doors.md).
- **Server-prep disciplines from day one** (everything-through-Session, bindings-as-thin-adapter) —
  [docs/adr/0004-server-prep-disciplines.md](docs/adr/0004-server-prep-disciplines.md).
- **No PyIceberg in any form**; **no Sail / pysail**. Own-the-stack.
- **`unsafe_code = "forbid"`** workspace-wide EXCEPT `crates/repark-python` (landed phase-3 PR-3;
  the crate sets a local `unsafe_code = "allow"` because PyO3 macros expand to `unsafe`).
- **Tests land with the code** (same commit); `cargo test --workspace` is the test command.
- **`map.md` in every directory, in lockstep** with code changes.
- **Pin one DataFusion family** across all DF-touching crates; `Cargo.toml` is the SSOT;
  `Cargo.lock` checked in.
- **Distribution is deferred** behind the `ExecutionBackend` seam.
- **[AGENTS.md](AGENTS.md) is the single authoritative contract** (it holds the precedence chain);
  tool adapters ([CLAUDE.md](CLAUDE.md), [.agents/](.agents/map.md)) carry no authoritative facts and
  cannot drift.

## Target architecture

The crate DAG, its declared edge kinds and the crate destination through 1.8 live in
[ARCHITECTURE.md](ARCHITECTURE.md) and
[task/roadmap/epic-term/crate-layout-1-8-2026-10-01.md](task/roadmap/epic-term/crate-layout-1-8-2026-10-01.md);
this file does not restate them. The shape that must not be undone: tiered crates with no
door-to-door edge, bindings reaching inward only, two Python surfaces (`python/repark` native,
`python/repark/spark` facade) over one engine, `dbt-repark` as a separate package embedding the
engine in the dbt process, v1 crates re-homed rather than rewritten, and DataFusion as the engine
under everything.

## Not now, and why

Declined or deferred on purpose; each line is dated so it is not re-litigated by inference.

- **Distributed single-query execution** — behind the `ExecutionBackend` seam; no query has
  outgrown one box (standing).
- **Server mode** (Arrow Flight SQL) — an adapter later; the server-prep disciplines keep it
  cheap (standing).
- **Continuous streaming** — micro-batch only, bronze→silver (1.7); the open decisions are
  owner-held (2026-10-11).
- **A Trino connector** — struck from the 1.6 charter (2026-10-01).
- **PyIceberg, Sail** — own-the-stack (standing).
- **External code contributions** — issues welcome, patches gated; revisited at the second-user
  review (see "Year one").

## Distributed posture

**Fleet-parallel → server mode → distributed-if-needed.** Fleet-parallel (many embedded engines
against one Iceberg catalog; the catalog's commit protocol is the coordinator) covers
backtest/parameter-sweep scale-out with zero engine work. Server mode (an Arrow Flight SQL
endpoint) is later an adapter, not a rewrite, because of the server-prep disciplines. Distributed
single-query execution comes only if a query outgrows one box, behind the `ExecutionBackend` seam.

## Year one

Priorities serve **the operator's production data-engineering workloads (Airflow + Iceberg +
dbt)**. Load-bearing surfaces, in order: (1) the SQL engine with Iceberg DML, (2) **dbt-repark**
(engine embedded in the dbt process — dbt-duckdb precedent), (3) the lazy DataFrame API for Airflow
tasks. The PySpark facade's year-one job: migrate existing pipelines without rewrites.

**The second-user review.** Year one optimises for one known workload, which is why its
requirements are true and its baselines are real. The first external workload running in
production against the engine triggers a review — of opening external patches, of the
additive-only freeze on native-door names, of ML 2.x funding — each a separate decision, none
automatic. Until then the native door is not widened for hypothetical users.

## Validation & documentation roadmap (owner-set, 2026-08-16)

Four workstreams the owner has named as roadmap commitments. Each ships twice: as repo
artifacts (docs / datasets / test suites) AND as runnable Jupyter notebooks.

1. **Full examples documentation.** A worked example for every public function and
   transformation — every `F.*` function, every DataFrame method, every TA kernel, every
   reader/writer — kept executable so drift fails loudly rather than rotting in prose.

2. **Torture-test dataset suite** (each dataset ≥1M rows, generators checked in so the data
   is reproducible, never committed as blobs):
   - *Nested reading + `dynamicFlatten`*: deep struct/list nesting with mixed element types,
     lists of structs, capitalized field names, and null-typed lists.
   - *Schema-inference conflicts*: columns whose observed type shifts mid-file (int32 until
     row 500k then int64; string-vs-float halves; bool-looking ints; date-looking strings;
     as many conflict classes as we can enumerate) — the inference torture battery.
   - *Extreme types*: high-precision decimals (decimal128-scale values such as
     `102.102334252345232345233`), UUID columns, paragraph-length strings, and columns with
     embedded HTML fragments.
   - *Secrets-flagging fixture*: columns named like credentials (`apiKey`, `api_key`,
     `api_token`, `access_token`, …) carrying fake plaintext secrets — the fixture for an
     **opt-in secrets-flagging mechanism** (disabled by default; a bool conf enables
     read-time flagging/refusal). The mechanism itself is a roadmap feature this fixture
     exists to test.
   - *smartCsv expansion*: grow the messy-CSV battery (header normalization, blank cells,
     currency/decimal widths, bool spellings) far beyond the current three-row example.

3. **Full Iceberg statement coverage.** Every DML and DDL statement, and every system
   operation (`rewrite data files`-class maintenance, snapshot rollback/expiry, branch/tag
   operations — the complete procedure surface), each validated in **full comparison against
   PySpark** on the same tables; gaps land in the divergence registry, never silently.

4. **Cross-engine function validation.** Systematic comparison-and-validation batteries for
   the full `pyspark.sql.functions` surface (extending the existing parity harness), plus
   equivalent comparisons against polars and DuckDB function behavior where surfaces
   overlap — three oracles, one function matrix. Native-door tests follow RePark's declared
   contract, with Trino and DuckDB differential checks where their semantics match the surface
   under test; Spark parity does not test the native door.

## Current state

**Status is tracked in [STATUS.md](STATUS.md)** — the single source of truth for release state,
delivered capabilities, active workstreams, and deferred work. PROJECT.md states intent only; it
does not restate current state. The port that stood this repository up is recorded in
[docs/port/PLAN.md](docs/port/PLAN.md) and
[docs/adr/0003-copy-then-rehome-port.md](docs/adr/0003-copy-then-rehome-port.md).

## Conventions (summary; see [AGENTS.md](AGENTS.md) + [DEVELOPMENT.md](DEVELOPMENT.md) for the full contract)

Rust: rustfmt `max_width=100`, edition 2024, clippy `all`+`pedantic` `-D warnings`, 91-`=` banner
doc blocks on section functions, `thiserror`/`anyhow`, no panics in prod (no `unwrap`/`expect`).
Python: type hints, Pydantic v2, `pathlib`, `logging`, Ruff line-100. `make ci` is the gate;
`make verify` and `make preflight` extend it. Every directory carries a `map.md`
(Purpose / Contents / I want to… / Pointers / Debug).
