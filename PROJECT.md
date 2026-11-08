# PROJECT.md — RePark (project intent & north star)

> **Purpose of this file.** Orient any agent or contributor to *what RePark is, why it exists, and
> the decisions that must not be undone*. The single authoritative day-to-day contract is
> [AGENTS.md](AGENTS.md) (which holds the precedence chain; [CLAUDE.md](CLAUDE.md) is a thin
> tool adapter), with a `map.md` where the architecture needs one (see `docs/maps-policy.md`).
> **This file states INTENT; the contract states MECHANICS** — if they conflict, reconcile, never
> silently discard intent.

## What RePark is

A pure-Rust, **no-JVM** data engine that **replaces Apache Spark in production ETL**: embedded in
one process by default, a server when jobs are submitted from elsewhere, a cluster when one box is
not enough. Positioning: **"Spark's surface and deployment shapes without the JVM, Polars's
DataFrame API, Trino's SQL, the deepest Iceberg support."** One `pip install`; no cluster and no
daemon **required**.
Built **on** Apache DataFusion + Arrow + our **owned iceberg-rust fork** (all pure-Rust and
JVM-free) with native PyO3 bindings. Compute runs in Rust; data crosses the Python boundary as
Apache Arrow, zero-copy.

Three doors over one plan layer:

- **Native DataFrame door** (Polars's standard surface as the contract).
- **Native SQL door** (ANSI, Trino-style, DuckDB as the second reference).
- **Spark door** (the in-process PySpark facade today; the Spark Connect server at 1.12 is the same
  door reached over the wire, so an unmodified `pyspark` client submits jobs with
  `spark.remote(...)`).

The Iceberg machinery (commit semantics, MERGE, snapshots, evolution) is shared beneath both doors;
the dialect layers are thin translators. Every door produces the same DataFusion logical plan;
semantics live in plan rules, never in a door.

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
- Single binary / single `pip install` — no JVM; no cluster or daemon required; cold start under a
  second embedded. The same engine runs as `repark serve` (Spark Connect at 1.12, Flight SQL at
  2.0) and, when a query outgrows one box, distributed behind the `ExecutionBackend` seam.
- **Spark surface parity as an inventory.** Every `pyspark.sql.functions` function, DataFrame
  method and transformation in Spark 4.1.2 is either equal (measured), a named refusal, or a dated
  divergence row. The remaining count is published in STATUS.md and falls; it is never described in
  prose.
- **Polars surface parity on the native DataFrame door**, limited to Polars's own API: expressions,
  lazy frame methods, I/O for the formats Polars reads natively. Polars's Iceberg, Delta, Unity,
  Hive-metastore and HuggingFace integrations are out. Same inventory discipline as Spark.
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
| ETL job vs Spark | the reference ETL shapes (read Iceberg, join, aggregate, write Iceberg; read Postgres, transform, write Iceberg) measured against Spark 4.1.2 local mode on the same box and data; **gate: at or better than Spark**; published target: within a stated factor of Polars and DuckDB where the shape has an equivalent |
| Connector read | partitioned parallel read of one table straight into Arrow against ConnectorX, within a stated factor (the 1.6 and 1.10 read bar, moved into the intent table) |

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
- **No PyIceberg in any form.** **No Sail / PySail dependency, in code or build, ever**; own-the-stack.
  **Reading Sail's design and source as a reference is allowed and encouraged** for the Spark Connect
  server, the plan and execution split, and the compatibility-surface crate shape, each borrowed idea
  cited in the unit's `map.md`.
- **`unsafe_code = "forbid"`** workspace-wide EXCEPT `crates/repark-python` (landed phase-3 PR-3;
  the crate sets a local `unsafe_code = "allow"` because PyO3 macros expand to `unsafe`).
- **Tests land with the code** (same commit); `cargo test --workspace` is the test command.
- **`map.md` where the architecture needs one**, kept honest by the sync and size gates; lockstep with code is a warning, not a gate.
- **Pin one DataFusion family** across all DF-touching crates; `Cargo.toml` is the SSOT;
  `Cargo.lock` checked in.
- **Distribution is a planned shape, reached behind the `ExecutionBackend` seam**, after the server
  core. The seam is the commitment; the substrate is chosen by a sketch unit, not by default. Until
  then no unit introduces process-global state, places semantics in Python or pre-parse text rewrites
  where a plan rule can carry them, or adds a physical operator without a stated serialization story.
- **[AGENTS.md](AGENTS.md) is the single authoritative contract** (it holds the precedence chain);
  tool adapters ([CLAUDE.md](CLAUDE.md), [.agents/](.agents/map.md)) carry no authoritative facts and
  cannot drift.

## Semantic precedence

*Owner ruling, 2026-11-08.*

> When a brief or ruling does not settle a behaviour question, the lane applies the door's reference order and records the measured cell it used. **Spark door** (facade and Connect server): Spark 4.1.2 measured on the box, else refuse by name. **Native SQL door**: ANSI SQL as Trino implements it, else DuckDB measured, else refuse by name. **Native DataFrame door**: Polars measured, else DuckDB measured where the operation has a SQL equivalent, else refuse by name. **Connectors**: the source system's own result is the oracle; ConnectorX and ADBC define the read style. **CDC**: Flink's guarantees are the rubric (the CDC north-star card). A reference answer is adopted only from a measured cell in the unit's domain table; an unmeasured cell refuses. The behaviour is implemented in Rust at the owning layer. This order settles semantics only; it never authorizes an engine-wide change without a sketch, nor a merge, release, label or outward-facing action.

Two choices inside that text. **Ruled 2026-11-08: Trino is the first word on the native SQL door** when it and the ANSI standard differ, because Trino is a runnable oracle and the standard is not. **Ruled 2026-11-08: DuckDB is the second reference on the native DataFrame door**, for operations that have a SQL equivalent and no Polars answer; each such cell is a dated registry row.

## Legibility

*Owner ruling, 2026-11-08.*

> A file is read by a person before it is read by a gate. **Code:** the existing ceilings stand (Rust default 1,000 lines with exact-baseline exceptions that ratchet down; Python ceilings in `check_lib_py`). **Markdown:** every tracked markdown file has a line ceiling of **2,000 lines** (owner, 2026-11-08) with the same exact-baseline exception table and ratchet as `check_map_size.py`, extended from `map.md` files to all of `docs/`, `task/` and crate prose. The mechanism is the clerk unit `DOCS-SHARD-1`; no gate enforces this ceiling before it lands. **Named exemptions**, each with a reason and a split seam: the native and facade DataFrame definition files (`python/repark/src/repark/spark/dataframe/core.py` and its native counterpart), and nothing else without a ruling. **Existing offenders** are split by area with an index in a clerk unit (`DOCS-SHARD-1`), the parity registry first (18,680 lines today), then the test maps and ledgers above the ceiling. A split never changes a gate's semantics: the ledger-grammar gate and the map lockstep hooks are re-pointed in the same PR.

Current offenders for the owner's reference, from `origin/main`:

| File | Lines |
|---|---|
| `docs/spark-sql-iceberg-parity.md` | 18,680 |
| `docs/history/facade-tests/tests-map-2026-10-30.md` | 10,118 |
| `task/wo/ice-tsns-coercion-1/evidence/twins-summary.md` | 4,876 |
| `crates/repark-spark/src/tests/map.md` | 2,800 |
| `crates/repark-spark/src/map.md` | 2,524 |
| `crates/repark-iceberg/src/write/map.md` | 2,064 |
| `python/repark/src/repark/spark/dataframe/core.py` | 3,461 (proposed exemption) |
| `crates/repark-iceberg/src/write/merge/tests/streaming_scan.rs` | 3,018 (already an exact-baseline exception) |

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

- **Distributed single-query execution** — **sequenced, not declined**: after the 1.12 server core,
  substrate chosen by `DISTRIBUTED-SKETCH-1`; coordinator-side Iceberg commit is the known debt
  (ADR-0004).
- **Server mode** (Arrow Flight SQL) — **sequenced**: Spark Connect 1.12, Flight SQL 2.0.
- **Continuous streaming** — micro-batch only, bronze→silver (1.7); the open decisions are
  owner-held (2026-10-11).
- **A Trino connector** — **1.10**, not 1.6; the 2026-10-01 strike applied to the 1.6 charter only.
- **PyIceberg** — own-the-stack (standing).
- **Sail** — **no dependency**; reference allowed (2026-11-08).
- **External code contributions** — issues welcome, patches gated; revisited at the second-user
  review (see "Year one").

## Distributed posture

**Fleet-parallel → server → distributed.** Fleet-parallel (many embedded engines, one Iceberg catalog
as coordinator) is free today. The server core (1.12) carries Spark Connect first and Flight SQL
second. Distributed single-query execution follows the server core and is a destination, not a
contingency; its substrate (library-style over DataFusion, Ballista-style cluster, or owned) is
chosen by a sketch with the Iceberg write path's coordinator commit as the first constraint
(ADR-0004: do not build Ballista-for-writes).

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
`make verify` and `make preflight` extend it. A `map.md` sits where the architecture needs one
(Purpose / Contents / I want to… / Pointers / Debug).
