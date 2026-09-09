# Ballista audit — Milestone 0 of the Rust migration pilot

**Opened:** 2026-09-08. **Class:** campaign. **State:** landed (#426, 2026-09-09); Half A facts, Half B judgement and the ADR-0004 disposition are all in this document.
**Audited upstream:** Apache DataFusion Ballista tag `54.1.0`, commit `f4e66525`
(`docs: add 54.1.0 changelog and restore missing 54.x toctree entries (#2243)`),
cloned at `upstream-ballista/` (git-excluded scratch copy, never vendored).
**D-1 version pair:** upstream root `Cargo.toml` declares `datafusion = "54"`;
this workspace pins `datafusion = "54.1.0"`. Match, no delta risk.
**Standing question:** `docs/adr/0004-server-prep-disciplines.md` rules out
Ballista-for-writes because the protobuf plan serialization cannot carry
RePark's Iceberg write/commit nodes. The serialization chapter below (Half A
facts, then the disposition below) says whether an extension codec changes that.
The commit-coordinator boundary (executors produce files, one authority commits)
is kept either way. Step 2 answered it in §26.D; step 3 records the
one-line disposition.

## §26.A — Version pair and audit scope

D-1 holds with no delta risk. Upstream root `Cargo.toml` line 37 declares
`datafusion = "54"`; this workspace pins `datafusion = "54.1.0"`
(`Cargo.toml` line 88). Appendix A2 shows every Ballista member resolves
`datafusion` through the upstream workspace pin, so both trees build against
the DF 54 family and the audited tag `54.1.0` (`f4e66525`) is the newest
matching Ballista release, not a nearest-lower fallback.

Scope is the seven member directories in appendixes A1–A5 plus the two
appendix-A1 gaps closed in step 2 (appendix A6): the `python/` binding
(`python/build.rs`, `python/src/lib.rs`, `python/src/utils.rs`,
`python/src/cluster.rs`), which bears directly on the D-4 Python-API
requirement, and `dev/msrvcheck`, which is release tooling. The scratch
checkout at `upstream-ballista/` is git-excluded and never enters a commit;
all counts below were produced by the commands shown, run at that checkout.

## §26.B — Crate and line inventory (facts in appendix A1)

Complexity concentrates in two places. The scheduler is 30182 of 76209
lines (40%), and over half of that sits in `scheduler/src/state`
(16427 lines): the stage graph, stage, and task-manager files named in
appendix A5. `execution_graph.rs` alone is 3020 lines, the largest file in
the tree. The second mass is `ballista/core` (19233 lines), where the serde
layer (5721) and the shuffle-heavy `execution_plans` (8153) dominate.

The client is thin by design: 3838 lines, of which 3556 are `tests/`, so the
shipped client surface is about 300 lines over `ballista-core`. The executor
is mid-size (6090) with no `tests/` directory. Test weight is material:
20110 `#[cfg(test)]` lines are 26% of the tree, and the scheduler holds 7508
of them, which raises the cost of any future import of the scheduling core.

`ballista-cli` (11073), `benchmarks` (3079), and `examples` (2714) are
harness and demo code, not runtime. They enter no KEEP set; §26.F drops them
outright. The runtime under judgement is therefore about 59k lines
(client + core + executor + scheduler), with the scheduling state machine as
the long pole.

## §26.C — Dependency inventory (facts in appendix A2)

Per step D-8 the source is each crate's own `Cargo.toml`, not `cargo tree`,
which needs registry network access. Three judgements follow from that table.

First, the dependency story is clean at the engine layer. Every Ballista
crate builds on the workspace-pinned DF 54 family (`datafusion`,
`datafusion-proto`, `datafusion-proto-common`) plus `tonic`/`prost` transport
and `object_store`. Nothing in the runtime core reaches outside the
DataFusion/Arrow/tonic world except through the scheduler and executor
binaries. A RePark dependency on `ballista-core` + client would therefore
pull no foreign engine; the JVM-free constraint from the unification brief §1
is satisfied by construction.

Second, the scheduler and executor binaries pull an operational stack that a
dependency also inherits: `axum` + `tower-http` (REST API), `prometheus`
(metrics), `graphviz-rust` (plan display), the KEDA autoscaling proto
(appendix A3), and `sysinfo` + full `tokio` on the executor. None of this is
a veto, but it is the real cost of depending on the scheduler crate rather
than only on core + client, and it is why §26.H scopes the subset narrowly.

Third, two optional features map directly onto RePark requirements.
`spark-compat` (core + scheduler, over `datafusion-spark` 54) is the
Spark-function surface the Spark facade would meet; `substrait`
(scheduler-only, optional) is a second plan interchange next to the protobuf
path. Neither is needed for a first distributed-read pilot, so both stay off
in §26.F; they are recorded here so a later unit can switch them on without
re-auditing.

## §26.D — Serialization: protobuf files and extension codecs (facts in appendixes A3–A4)

The plan path runs through the vendored `datafusion.proto` /
`datafusion_common.proto` copies plus Ballista's own `ballista.proto`
(883 lines: jobs, stages, tasks, shuffle locations). `BallistaCodec` in
`ballista/core/src/serde/mod.rs` (lines 141–364) is generic over the
DataFusion plan representation and carries the two hooks: default
`BallistaLogicalExtensionCodec` (def line 185) and
`BallistaPhysicalExtensionCodec` (def line 352, `impl
PhysicalExtensionCodec` line 364). Measured behavior of each hook:

- Physical: exactly five node types get Ballista encodings —
  `ShuffleWriter`, `SortShuffleWriter`, `ShuffleReader`,
  `UnresolvedShuffle`, `ChaosExec` (`try_decode` lines 365–551, `try_encode`
  lines 553 onward). Any other `ExecutionPlan` hits the terminal `else` and
  encoding FAILS. There is no additive registry; new node types travel only
  inside a codec object supplied from outside.
- Logical: only the cache node plus Parquet/CSV/JSON/Arrow/Avro file-format
  codecs get Ballista encodings (lines 185–331); anything else falls through
  to the DataFusion default codec.
- Table providers are a pure passthrough to the DataFusion default codec
  (lines 292–310). Ballista adds nothing here: a custom `TableProvider`
  serializes across the cluster only if DataFusion's own codec knows it.

The override plumbing is a single slot, not a chain. `SessionConfigExt`
(`ballista/core/src/extension.rs`, `with_ballista_physical_extension_codec`
and `with_ballista_logical_extension_codec`) stores one codec object; the
getter returns the override or the Ballista default. The scheduler
(`ballista/scheduler/src/config.rs` lines 261–263) and the executor
(`ballista/executor/src/executor_process.rs` lines 183–185) each accept the
same pair of overrides. A RePark codec must therefore be a delegating
wrapper: handle RePark's nodes, delegate the five shuffle nodes (and the
logical cache/file-format nodes) back to the Ballista defaults. The
extension-point rows that prove each seat exists are appendix A4
(`PhysicalExtensionCodec` 62 hits / 10 files, `LogicalExtensionCodec` 57 / 9,
with the scheduler, executor, client-test, and example call sites listed).

ADR-0004 disposition (the header's standing question). Mechanically, YES: a
`PhysicalExtensionCodec` wrapper can carry RePark's Iceberg write/commit
nodes across the scheduler–executor boundary, so the protobuf format is not
a hard block. Architecturally, the ADR conclusion STANDS unchanged: do not
build Ballista-for-writes. Exactly-once Iceberg publication — stable batch
identity, ownership fencing, unknown-commit recovery, catalog-conflict
handling, the invariants the unification brief demands in its §8 — cannot
live inside distributed tasks no matter how they serialize. The
commit-coordinator boundary is kept either way: executors may produce data
files over the shuffle/result path, but the single commit authority stays
coordinator-side, and no write/commit node is registered in the wrapper.
Two further facts support keeping that boundary: table-provider passthrough
means RePark's Iceberg provider would need DataFusion-level codec support on
every executor, and per-executor catalog plus credential plumbing fights the
ADR's session-owned-credentials discipline.

## §26.E — Lifecycle file map: submit to result (facts in appendix A5)

The eight steps form one coherent pipeline with extension seats at both
ends. Client submit is thin: `ballista/client` (three files, ~300 shipped
lines) plus `core/src/planner.rs` (319) and `core/src/client.rs` (762).
Scheduler accept-and-plan centers on `scheduler_server/grpc.rs` (1292),
`scheduler_server/mod.rs` (1093), and `scheduler/src/planner.rs` (1934);
per-session state enters through the `SessionProvider` hook
(`scheduler_server/mod.rs` line 65), which is the seat a RePark session
builder would occupy. The stage graph (`execution_graph.rs` 3020,
`execution_stage.rs` 1333, `query_stage_scheduler.rs` 455) is the largest
and least divisible scheduling mass; task dispatch is a single 1175-line
file. Executor run spreads ~4700 lines across ten files, with the process
entry (`executor_process.rs` 1158, carrying the codec and config overrides),
the run loop (`executor.rs` 567, `execution_loop.rs` 380), and the plan
execution seat (`execution_engine.rs` 519) as the files a RePark embedding
would touch first. Shuffle write (~3360 lines, eight files) is outweighed by
shuffle read (~3940, five files), and the 2430-line `shuffle_reader.rs`
against the 828-line writer says read-path complexity — coalescing,
broadcast, multi-stream partition handling — dominates the data plane.
Results return over Arrow Flight (`flight_service.rs` 441, `collect.rs` 149).

No step is missing and no step duplicates another: submit, plan, stage,
dispatch, run, shuffle, and collect each own distinct files. The lifecycle
therefore admits the smallest-subset reading in §26.F without carving any
file in half, except the stage-graph cluster, which must be taken whole.

## §26.F — Module classification and crate-structure proposal

Per D-4 nothing is rewritten or renamed, and a divergence appears only where
a named RePark requirement drives it (streaming microbatch, Iceberg commit
coordination, Python API). Verdicts:

| Module | Verdict | Reason |
|---|---|---|
| `ballista/core` serde, shuffle `execution_plans`, `extension`, `config`, `planner`, `client`, `object_store`, `registry`, `utils` | KEEP (as dependency) | The read path: plan serialization, shuffle data plane, session/codec/config override seats, object-store registry. No RePark-owned change needed while writes stay coordinator-side. |
| `ballista/client` | KEEP (as dependency) | Thin submit surface over core; the standalone feature (optional executor + scheduler seats) is the local-first embedding shape. |
| `ballista/scheduler` state machine (`state/`, `scheduler_server/`, `planner.rs`, `task_manager.rs`) | KEEP (as dependency) | Whole-stage scheduling has no RePark counterpart; the `SessionProvider` and codec-override seats admit RePark sessions without forking. Taken whole, never carved. |
| `ballista/executor` run loop, engine, Flight service | KEEP (as dependency) | Task execution plus the `execution_engine.rs` and `executor_process.rs` override seats. |
| `ballista/scheduler` REST API (`api/`), KEDA proto + autoscaling hooks, `graphviz` display, `flight_proxy_service` | DROP | Operations/display surface with no RePark requirement behind it; pulled only if the scheduler crate is depended on, switched off where features allow. |
| `ballista-cli`, `benchmarks`, `examples` | DROP | Harness and demo code; not runtime. |
| `dev/msrvcheck` | DROP | Release tooling (71-line MSRV checker); no runtime content. |
| `python/` (`pyballista`) | DROP as a crate, WRAP as a pattern | The crate itself is not taken: it couples `pyo3` + `datafusion-python` 54 to the full scheduler/executor path (appendix A6) and would lock RePark's binding to Ballista's release train, against the thin-adapter discipline. Its *pattern* — `PyScheduler`/`PyExecutor` lifecycle classes plus `create_ballista_data_frame` shipping a serialized plan blob to a remote session — is the shape a future RePark distribution switch copies. No divergence is proposed now because no distribution unit has opened. |
| Streaming microbatch, Iceberg commit coordination | No upstream module taken; RePark-owned later | Upstream offers neither a microbatch trigger model (the unification brief §10.2 order starts with available-now batches and triggers) nor commit-coordination machinery (brief §8). When those units open they build RePark-side, coordinator-side, reusing only the KEEP transport above. |

Crate-structure proposal. RePark depends on four upstream crates at the
pinned tag — `ballista-core`, `ballista` (client), `ballista-scheduler`,
`ballista-executor` — with `spark-compat` and `substrait` off and REST/KEDA
surface uncalled. No upstream file is imported, vendored, or renamed. The
only RePark-owned code this audit anticipates, and only when a distribution
unit opens, is a thin embedding layer: a delegating codec wrapper (§26.D), a
session builder occupying the `SessionProvider` seat, and a commit-coordinator
client that keeps publication coordinator-side. That layer is a future unit's
design, not this audit's deliverable.

## §26.G — Risks

- R-1 Single-slot codec override. A RePark wrapper must re-delegate the five
  shuffle node types to the Ballista defaults (§26.D); every upstream change
  to those nodes silently bypasses a stale wrapper. Pinned-tag dependence
  plus a round-trip serde test on upgrade contains it.
- R-2 Vendored DataFusion protos. `datafusion.proto` (1529 lines) and
  `datafusion_common.proto` (687) are copies, not references; a DataFusion
  bump that changes plan serialization must move the Ballista tag in
  lockstep. Today's DF 54 match makes this latent, not active.
- R-3 Stage-graph mass. `execution_graph.rs` (3020) plus `execution_stage.rs`
  (1333) is the largest indivisible block; any behavior change needed inside
  it cannot be made through an override seat and would force a fork. The KEEP
  verdict bets no such change is needed for distributed reads.
- R-4 Table-provider passthrough. RePark's Iceberg provider gains nothing
  from Ballista's serde (appendix A3, `serde/mod.rs` lines 292–310); scan
  distribution needs a DataFusion-level provider codec on every executor, a
  cost this audit records but does not design.
- R-5 Session-owned credentials across processes. Per-executor catalog and
  credential plumbing fights the ADR-0004 everything-through-Session
  discipline; unread paths (S3, Glue) must resolve executor-side without
  leaking ambient authority. Open until a distribution unit designs it.
- R-6 No upstream commit coordination. Nothing in the tree corresponds to the
  unification brief §8 invariants (batch identity, fencing, unknown-commit
  recovery); that machinery is RePark-owned and coordinator-side whenever it
  arrives. The risk is assuming the transport provides it.
- R-7 Operational stack inheritance. Depending on the scheduler crate pulls
  `axum`, `prometheus`, `graphviz-rust`, and KEDA hooks (§26.C) into RePark's
  supply chain whether or not RePark calls them.

## §26.H — Dependency-versus-import recommendation

RECOMMENDATION: depend, not import. RePark takes `ballista-core`,
`ballista` (client), `ballista-scheduler`, and `ballista-executor` as
version-pinned dependencies at tag `54.1.0`, and owns nothing upstream. The
override seats (§26.D: single-slot codec wrappers; `SessionProvider`;
`override_config_producer` / `override_session_builder`) admit every
RePark behavior the audit found a requirement for without touching an
upstream file, and the DF 54 version match (§26.A) removes the usual reason
to fork. Importing would take ~59 runtime kilolines plus 20 kilolines of
upstream tests — including the 3020-line stage graph and its 7508 lines of
scheduler in-source tests — with no owner and no RePark requirement that
justifies the maintenance. The one condition that reopens this verdict is
measured, not speculative: a distribution unit proves that a needed behavior
change falls inside the stage-graph block (R-3) where no override seat
reaches. Until such a unit produces that evidence, the gate stays on
depend.

## §27 — Findings and open questions

Findings. F-1: the smallest coherent subset is core + client + scheduler +
executor as pinned dependencies, with the REST/KEDA/display surface dropped
and `spark-compat`/`substrait` left off (§26.F). F-2: the protobuf format
does not block custom plan nodes, but the single-slot override forces a
delegating wrapper, not an additive registration (§26.D). F-3: ADR-0004's
Ballista-for-writes ban survives the extension-codec answer; the boundary
moves from "cannot serialize" to "must not commit from tasks"
(§26.D). F-4: scan distribution still needs a DataFusion-level provider
codec plus executor-side credential plumbing, both unowned (R-4, R-5). F-5:
`pyballista` is a pattern reference, not a crate to take (appendix A6).

Open questions for the unit that first distributes a query. Q-1: how do
executors resolve S3/Glue reads under session-owned credentials (R-5)? Q-2:
is the scheduler REST API (`api/`) needed for operations, or does it stay
dropped? Q-3: does the Spark facade need `spark-compat` on, or do RePark's
own shims cover the distributed path? Q-4: what round-trip serde test guards
the codec wrapper on each Ballista repin (R-1)? None of these blocks this
audit; each names its owning future unit.

## §28 — Decision gate

The owner's plan §26–§28 text is not in this repository, so the criteria
below are derived from this card's Done condition and decisions D-1–D-4, one
line of evidence per criterion.

| Criterion | Verdict | Evidence |
|---|---|---|
| D-1 version pair identified | PASS | Tag `54.1.0` / `f4e66525`; upstream `datafusion = "54"` vs workspace `54.1.0` (§26.A). |
| Sixteen audit outputs answered with paths and numbers | PASS | Appendixes A1–A6: 187 files / 76209 lines, per-crate deps, 4 protos, 6-symbol grep with file:line, 8-step lifecycle file map, python + tooling gap closed. |
| Smallest coherent subset named | PASS | Four KEEP crates as pinned dependencies; DROP list with reasons (§26.F). |
| Serialization question answered against ADR-0004 | PASS | Codec CAN carry write nodes; ADR ban STANDS; coordinator commits either way (§26.D). |
| Commit-coordinator boundary kept | PASS | No write/commit node registered; §8-class machinery declared RePark-owned, coordinator-side (R-6, §26.F). |
| Divergences only where a RePark requirement drives them | PASS | Only streaming microbatch, commit coordination, and Python API appear, all as future-owned work (§26.F). |
| Dependency-vs-import recommendation | DEPEND | Override seats suffice; import cost ~59k + 20k test lines; reopen condition is measured evidence inside the stage-graph block (§26.H). |

## Appendix A1 — Line counts by crate and directory

Command for the per-crate totals (run at `upstream-ballista/`):

```sh
for d in ballista/client ballista/core ballista/executor ballista/scheduler ballista-cli benchmarks examples; do echo "== $d"; find $d -name '*.rs' | xargs wc -l | tail -1; done
```

| Crate (workspace member) | `.rs` files | Total lines |
|---|---|---:|
| `ballista/client` | 11 | 3838 |
| `ballista/core` | 37 | 19233 |
| `ballista/executor` | 17 | 6090 |
| `ballista/scheduler` | 59 | 30182 |
| `ballista-cli` | 46 | 11073 |
| `benchmarks` | 3 | 3079 |
| `examples` | 14 | 2714 |
| **Total** | **187** | **76209** |

Command for the `src/` versus `tests/` split (run at `upstream-ballista/`):

```sh
for d in ballista/client ballista/core ballista/executor ballista/scheduler ballista-cli benchmarks examples; do echo "== $d src:"; find $d/src -name '*.rs' | xargs wc -l | tail -1; echo "== $d tests/:"; find $d/tests -name '*.rs' | xargs wc -l | tail -1; done
```

| Crate | `src/` lines | `tests/` lines |
|---|---:|---:|
| `ballista/client` | 282 | 3556 |
| `ballista/core` | 19172 | 0 (no `tests/` dir) |
| `ballista/executor` | 6090 | 0 (no `tests/` dir) |
| `ballista/scheduler` | 29660 | 493 |
| `ballista-cli` | 11073 | 0 (no `tests/` dir) |
| `benchmarks` | 2763 | 0 (no `tests/` dir) |
| `examples` | 107 | 823 |

Command for the `#[cfg(test)]` in-source test blocks (run at `upstream-ballista/`;
brace-matching counter over every file listed by
`grep -rl '#\[cfg(test)\]' ballista ballista-cli benchmarks examples --include='*.rs'`,
73 files total):

```sh
python3 -c "<brace-matching counter; see ledger C-001 evidence>"
```

| Location | `#[cfg(test)]` lines |
|---|---:|
| `ballista/client` `tests/` files | 3129 |
| `ballista/core` `src/` | 4084 |
| `ballista/executor` `src/` | 1072 |
| `ballista/scheduler` `src/` | 7508 |
| `ballista-cli` `src/` | 2917 |
| `benchmarks` `src/` | 787 |
| `examples` `tests/` files | 613 |
| **Total `#[cfg(test)]`** | **20110** |

Largest `src/` subdirectories, command
`for s in $(find <src> -mindepth 1 -maxdepth 1 -type d); do echo -n "$s: "; find $s -name '*.rs' | xargs wc -l | tail -1; done`:

| Directory | Lines |
|---|---:|
| `ballista/scheduler/src/state` | 16427 |
| `ballista/core/src/execution_plans` | 8153 |
| `ballista/core/src/serde` | 5721 |
| `ballista/scheduler/src/scheduler_server` | 3105 |
| `ballista/scheduler/src/cluster` | 2069 |
| `ballista/scheduler/src/physical_optimizer` | 1845 |
| `ballista/scheduler/src/api` | 1354 |

## Appendix A2 — Direct dependencies per crate

Source is each crate's own `Cargo.toml` `[dependencies]` section, not
`cargo tree` (no registry network in this environment; see step D-8).
Command per crate (run at `upstream-ballista/`):

```sh
sed -n '/^\[dependencies\]/,/^\[/p' <crate>/Cargo.toml
```

| Crate | Direct dependencies |
|---|---|
| `ballista/client` (`ballista`) | `async-trait`, `ballista-core` (path), `ballista-executor` (path, optional), `ballista-scheduler` (path, optional), `datafusion` (ws), `log` (ws), `tokio` (ws), `url` (ws) |
| `ballista/core` (`ballista-core`) | `arrow-flight` (ws), `async-trait` (ws), `aws-config` (opt), `aws-credential-types` (opt), `chrono`, `clap` (ws, opt), `datafusion` (ws), `datafusion-proto` (ws), `datafusion-proto-common` (ws), `datafusion-spark` (ws, opt), `futures` (ws), `itertools`, `log` (ws), `md-5`, `object_store` (ws, opt), `parking_lot` (ws), `prost` (ws), `prost-types` (ws), `rand` (ws), `serde` (ws, derive), `tokio` (ws, rt-multi-thread), `tokio-stream` (ws), `tonic` (ws), `tonic-prost` (ws), `url` (ws), `uuid` (ws) |
| `ballista/executor` (`ballista-executor`) | `arrow` (ws), `arrow-flight` (ws), `async-trait` (ws), `ballista-core` (path), `bytesize`, `clap` (ws, opt), `dashmap` (ws), `datafusion` (ws), `datafusion-proto` (ws), `futures` (ws), `log` (ws), `lru`, `memory-stats`, `mimalloc` (ws, opt), `parking_lot` (ws), `serde` (ws, derive), `sysinfo`, `tempfile` (ws), `tokio` (ws, full), `tokio-stream` (ws), `tokio-util`, `tonic` (ws), `tracing` (ws, opt), `tracing-appender` (ws, opt), `tracing-subscriber` (ws, opt), `uuid` (ws) |
| `ballista/scheduler` (`ballista-scheduler`) | `arrow-flight` (ws), `async-trait` (ws), `axum`, `tower-http`, `ballista-core` (path), `clap` (ws, opt), `dashmap` (ws), `datafusion` (ws), `datafusion-proto` (ws), `datafusion-substrait` (ws, opt), `futures` (ws), `graphviz-rust` (opt), `http`, `insta` (ws), `itertools` (ws), `log` (ws), `object_store` (ws), `once_cell` (opt), `parking_lot` (ws), `prometheus` (opt), `prost` (ws), `prost-types` (ws), `rand` (ws), `serde` (ws, derive), `tokio` (ws, full), `tokio-stream` (ws), `tonic` (ws, router), `tonic-prost` (ws, opt), `tracing` (ws, opt), `tracing-appender` (ws, opt), `tracing-subscriber` (ws, opt), `uuid` (ws) |
| `ballista-cli` | `ballista` client (path, opt), `datafusion` (ws, opt), `datafusion-cli` (ws, opt) plus TUI/web CLI-only deps (terminal UI, WASM, HTTP client crates, all optional); always-on: `clap` (ws), `dirs`, `tracing`, `tracing-subscriber` |
| `benchmarks` | `ballista` client (path), `ballista-core` (path), `clap` (ws), `datafusion` (ws), `datafusion-proto` (ws), `env_logger` (ws), `futures` (ws), `mimalloc` (ws, opt), `rand` (ws), `serde` (ws), `serde_json`, `structopt`, `tempfile` (ws), `tokio` |
| `examples` | `rustls` (optional); dev-deps only otherwise |

`(ws)` marks a version pinned once in the workspace root `Cargo.toml`.
Internal shape: every Ballista crate depends on `ballista-core`; only the
client optionally pulls in executor and scheduler (standalone feature).

## Appendix A3 — Protobuf file inventory

Commands (run at `upstream-ballista/`):

```sh
find . -name '*.proto'
for p in $(find . -name '*.proto'); do echo -n "$p: "; wc -l < $p; done
wc -l ballista/core/src/serde/generated/ballista.rs ballista/core/src/serde/generated/mod.rs ballista/core/src/serde/mod.rs ballista/core/src/serde/scheduler/to_proto.rs ballista/core/src/serde/scheduler/from_proto.rs ballista/core/src/serde/scheduler/mod.rs ballista/core/build.rs ballista/scheduler/build.rs
```

| `.proto` file | Lines | Role |
|---|---:|---|
| `ballista/core/proto/ballista.proto` | 883 | Ballista scheduler/executor messages (jobs, stages, tasks, shuffle) |
| `ballista/core/proto/datafusion.proto` | 1529 | Vendored copy of the DataFusion plan proto consumed at this pin |
| `ballista/core/proto/datafusion_common.proto` | 687 | Vendored copy of the DataFusion common proto consumed at this pin |
| `ballista/scheduler/proto/keda.proto` | 62 | KEDA autoscaling hooks only, not the plan path |

| Generated / plumbing file | Lines | Role |
|---|---:|---|
| `ballista/core/src/serde/generated/ballista.rs` | 3129 | Checked-in `prost` output for `ballista.proto` |
| `ballista/core/src/serde/generated/mod.rs` | 30 | Includes the generated submodule |
| `ballista/core/src/serde/mod.rs` | 1280 | `BallistaCodec`, `BallistaLogicalExtensionCodec`, `BallistaPhysicalExtensionCodec` live here (lines 141–364) |
| `ballista/core/src/serde/scheduler/to_proto.rs` | 314 | Execution-plan to proto conversion |
| `ballista/core/src/serde/scheduler/from_proto.rs` | 483 | Proto to execution-plan conversion |
| `ballista/core/src/serde/scheduler/mod.rs` | 485 | Scheduler serde glue |
| `ballista/core/build.rs` | 61 | Compiles `proto/ballista.proto` with `prost`, maps `.datafusion_common` and `.datafusion` as extern paths into `datafusion_proto_common` and `datafusion_proto` |
| `ballista/scheduler/build.rs` | 29 | Scheduler build script |

The plan path therefore runs through the vendored `datafusion.proto` /
`datafusion_common.proto` copies plus the `PhysicalExtensionCodec` /
`LogicalExtensionCodec` hooks in `serde/mod.rs`. Whether an extension codec
can carry RePark's Iceberg write/commit nodes is the step-2 serialization
judgement against ADR-0004.

## Appendix A4 — Extension-point inventory by grep

Command per symbol (run at `upstream-ballista/`):

```sh
grep -rn <symbol> ballista ballista-cli benchmarks examples --include='*.rs'
```

Each row lists one file and every hit line in it. Definition sites carry
`(def)`.

### `PhysicalExtensionCodec` — 62 hits in 10 files

| File | Lines |
|---|---|
| `ballista/core/src/serde/mod.rs` | 38, 46, 142, 151, 162, 178, 352 (def `BallistaPhysicalExtensionCodec`), 353, 356, 359, 364 (`impl PhysicalExtensionCodec`), 825, 842, 876, 888, 913, 949, 992, 1045, 1090, 1140, 1236, 1262 |
| `ballista/core/src/extension.rs` | 29, 42, 137, 140, 147, 149, 392, 403, 406, 837, 839, 843, 846 |
| `ballista/executor/src/executor_process.rs` | 31, 58, 182, 184, 185, 350 |
| `ballista/client/tests/context_setup.rs` | 109, 121, 351, 358, 363 |
| `ballista/scheduler/src/config.rs` | 33, 260, 262 (`override_physical_codec`), 263 |
| `ballista/scheduler/src/state/task_manager.rs` | 49, 239, 258 |
| `ballista/scheduler/src/scheduler_process.rs` | 36, 69 |
| `ballista/core/src/execution_plans/distributed_query.rs` | 49, 336 |
| `ballista/core/src/serde/scheduler/to_proto.rs` | 111 |
| `examples/examples/mtls-cluster.rs` | 66, 317, 426 |

### `LogicalExtensionCodec` — 57 hits in 9 files

| File | Lines |
|---|---|
| `ballista/core/src/serde/mod.rs` | 31, 32, 45, 141, 150, 161, 173, 185 (def `BallistaLogicalExtensionCodec`), 186, 187, 190, 199, 217, 220, 224–228, 234, 787 |
| `ballista/core/src/extension.rs` | 29, 41, 131, 134, 143, 145, 385, 398, 401, 822, 824, 828, 831 |
| `ballista/core/src/planner.rs` | 20, 30, 43, 65, 75, 90 |
| `ballista/core/src/execution_plans/distributed_query.rs` | 47, 72, 106, 120 |
| `ballista/executor/src/executor_process.rs` | 30, 58, 183, 345 |
| `ballista/client/tests/context_setup.rs` | 121, 277 |
| `ballista/scheduler/src/config.rs` | 32, 261 (`override_logical_codec`) |
| `ballista/scheduler/src/scheduler_process.rs` | 36, 64 |
| `examples/examples/mtls-cluster.rs` | 66, 316, 425 |

### `SessionState` — 137 hits in 28 files

| File | Lines |
|---|---|
| `ballista/core/src/extension.rs` | 31, 33, 99, 101, 102, 107, 108, 114, 286, 289, 297, 314, 322, 940, 1028, 1034, 1041, 1045, 1309, 1316, 1330, 1337 |
| `examples/tests/object_store.rs` | 37, 66, 134, 164, 227, 231, 277, 416, 555, 628, 643 |
| `ballista/client/tests/context_setup.rs` | 25, 34, 75, 114, 130, 168, 209, 244, 258, 395 |
| `ballista/client/src/extension.rs` | 18, 21, 77, 87, 95, 120, 131, 151, 179 |
| `ballista/core/src/object_store.rs` | 35, 85, 87, 91, 99, 109, 113, 115 |
| `ballista/scheduler/src/state/aqe/planner.rs` | 36, 64, 97, 134, 165, 182, 553, 555, 557 |
| `ballista/scheduler/src/state/aqe/optimizer_rule/join_selection.rs` | 351, 397, 435, 475, 519, 568, 592, 620 |
| `ballista/client/tests/common/mod.rs` | 28, 132, 240, 250 |
| `ballista/client/tests/sort_shuffle.rs` | 44, 90, 113, 124 |
| `ballista/core/src/planner.rs` | 25, 108, 207, 224 |
| `ballista/core/src/utils.rs` | 27, 29, 159, 165 |
| `benchmarks/src/bin/tpch.rs` | 32, 33, 583, 985 |
| `ballista/scheduler/src/state/aqe/test/coalesce_rule.rs` | 36, 60, 357 |
| `examples/examples/mtls-cluster.rs` | 58, 76, 466 |
| `examples/examples/standalone-substrait.rs` | 34, 57, 550 |
| `ballista-cli/src/main.rs` | 29, 147, 160 |
| `ballista/scheduler/src/state/aqe/test/join_selection.rs` | 28, 71, 90 |
| `ballista/scheduler/src/state/aqe/test/mod.rs` | 39, 127, 141 |
| `ballista/core/src/registry.rs` | 19, 166, 167 |
| `ballista/executor/src/standalone.rs` | 38, 54, 161 |
| `examples/tests/common/mod.rs` | 24, 77 |
| `ballista/scheduler/src/scheduler_server/mod.rs` | 27, 65 (`SessionProvider` hook: builds `SessionState` from `SessionConfig`) |
| `ballista/scheduler/src/standalone.rs` | 34, 61 |
| `examples/examples/standalone-broadcast-join.rs` | 38, 134 |
| `examples/examples/remote-dataframe.rs` | 22, 32 |
| `examples/examples/remote-spark-functions.rs` | 20, 33 |
| `examples/examples/remote-sql.rs` | 20, 34 |
| `examples/examples/standalone-sql.rs` | 20, 32 |

### `RuntimeEnv` — 64 hits in 13 files

| File | Lines |
|---|---|
| `ballista/executor/src/runtime_cache.rs` | 24, 29, 32, 36, 41, 55, 58, 63, 66, 77, 106, 132, 135, 147 |
| `ballista/executor/src/executor.rs` | 37, 83, 109, 130, 183, 187, 208, 316, 515, 551 |
| `ballista/executor/src/executor_process.rs` | 45, 85, 100, 102, 315, 1106, 1125, 1140, 1154 |
| `ballista/core/src/object_store.rs` | 34, 60 (`new_runtime_env`), 67, 76, 88, 114 |
| `examples/tests/object_store.rs` | 35, 57, 132, 155, 228, 637 |
| `ballista/core/src/lib.rs` | 23, 68, 71, 77 (`RuntimeProducer` factory: `SessionConfig` to `RuntimeEnv`) |
| `benchmarks/src/bin/shuffle_bench.rs` | 48, 95, 237 |
| `ballista/core/src/planner.rs` | 207, 220 |
| `ballista/core/src/utils.rs` | 28, 168 |
| `ballista/core/src/extension.rs` | 32, 296 |
| `ballista/core/src/execution_plans/sort_shuffle/writer.rs` | 890, 923 |
| `benchmarks/benches/sort_shuffle.rs` | 219, 225 |
| `examples/examples/mtls-cluster.rs` | 77, 378 |

### `TableProvider` — 21 hits in 7 files

| File | Lines |
|---|---|
| `ballista/scheduler/src/scheduler_server/mod.rs` | 476, 914, 930, 959, 976 |
| `ballista/scheduler/src/test_utils.rs` | 47, 75, 78, 81 (`impl TableProvider for ExplodingTableProvider`), 98 |
| `ballista/scheduler/tests/tpch_plan_stability/stats_table.rs` | 22, 112, 126 (`impl TableProvider for TpchStatsTable`) |
| `ballista/client/tests/context_setup.rs` | 306, 316 |
| `ballista/core/src/serde/mod.rs` | 298, 306 |
| `ballista/scheduler/src/state/aqe/test/mod.rs` | 37, 117 |
| `benchmarks/src/bin/tpch.rs` | 30, 990 |

### `ObjectStore` — 29 hits in 5 files

| File | Lines |
|---|---|
| `ballista/core/src/object_store.rs` | 21, 23, 31, 40, 52, 60, 62, 64, 77, 119, 120, 122 (def `CustomObjectStoreRegistry`), 127, 137, 141, 142, 146, 170, 234 |
| `examples/tests/object_store.rs` | 211, 225, 517, 638 |
| `ballista/executor/src/executor_process.rs` | 1105, 1137, 1138 |
| `ballista-cli/src/main.rs` | 34, 175 (`InstrumentedObjectStoreRegistry`) |
| `ballista/executor/src/execution_engine.rs` | 394, 402 |

## Appendix A5 — File list per lifecycle step

Source: directory listing plus `wc -l` on the files below (run at
`upstream-ballista/`). Line counts are whole-file totals including
`#[cfg(test)]` blocks. Paths are relative to `upstream-ballista/`.

| Step | Files (lines) |
|---|---|
| Client submit | `ballista/client/src/lib.rs` (26), `ballista/client/src/extension.rs` (236), `ballista/client/src/prelude.rs` (20); `ballista/core/src/client.rs` (762), `ballista/core/src/client_pool.rs` (106), `ballista/core/src/planner.rs` (319) |
| Scheduler accept and plan | `ballista/scheduler/src/scheduler_server/grpc.rs` (1292), `ballista/scheduler/src/scheduler_server/mod.rs` (1093), `ballista/scheduler/src/scheduler_server/event.rs` (194), `ballista/scheduler/src/planner.rs` (1934), `ballista/scheduler/src/config.rs` (553), `ballista/scheduler/src/scheduler_process.rs` (182), `ballista/scheduler/src/standalone.rs` (120), `ballista/scheduler/src/state/session_manager.rs` (86), `ballista/scheduler/src/state/executor_manager.rs` (584) |
| Stage graph | `ballista/scheduler/src/state/execution_graph.rs` (3020), `ballista/scheduler/src/state/execution_stage.rs` (1333), `ballista/scheduler/src/state/mod.rs` (413), `ballista/scheduler/src/state/distributed_explain.rs` (211), `ballista/scheduler/src/state/execution_graph_dot.rs` (669), `ballista/scheduler/src/scheduler_server/query_stage_scheduler.rs` (455) |
| Task dispatch | `ballista/scheduler/src/state/task_manager.rs` (1175) |
| Executor run | `ballista/executor/src/executor.rs` (567), `ballista/executor/src/executor_process.rs` (1158), `ballista/executor/src/executor_server.rs` (962), `ballista/executor/src/execution_engine.rs` (519), `ballista/executor/src/execution_loop.rs` (380), `ballista/executor/src/cpu_bound_executor.rs` (395), `ballista/executor/src/runtime_cache.rs` (218), `ballista/executor/src/standalone.rs` (180), `ballista/executor/src/client_pool.rs` (353), `ballista/executor/src/config.rs` (276) |
| Shuffle write | `ballista/core/src/execution_plans/shuffle_writer.rs` (828), `ballista/core/src/execution_plans/shuffle_writer_trait.rs` (51), `ballista/core/src/execution_plans/unresolved_shuffle.rs` (285), `ballista/core/src/execution_plans/sort_shuffle/writer.rs` (1238), `ballista/core/src/execution_plans/sort_shuffle/spill.rs` (412), `ballista/core/src/execution_plans/sort_shuffle/buffer.rs` (183), `ballista/core/src/execution_plans/sort_shuffle/index.rs` (221), `ballista/core/src/execution_plans/sort_shuffle/config.rs` (94), `ballista/core/src/execution_plans/sort_shuffle/mod.rs` (46) |
| Shuffle read | `ballista/core/src/execution_plans/shuffle_reader.rs` (2430), `ballista/core/src/execution_plans/sort_shuffle/reader.rs` (105), `ballista/core/src/execution_plans/sort_shuffle/multi_stream_reader.rs` (279), `ballista/core/src/execution_plans/sort_shuffle/partitioned_batch_iterator.rs` (202), `ballista/core/src/execution_plans/distributed_query.rs` (933) |
| Result return | `ballista/executor/src/collect.rs` (149), `ballista/executor/src/flight_service.rs` (441) |

Outliers for step 2: the stage graph file (`execution_graph.rs`, 3020 lines)
is the largest single file in the tree, and the shuffle reader
(`shuffle_reader.rs`, 2430 lines) outweighs the shuffle writer more than 2:1.

## Appendix A6 — Python binding and dev tooling (step-2 gap closure)

The appendix-A1 seven-directory walk omits five upstream `.rs` files. Four
are Ballista's own Python binding; one is release tooling. Command (run at
`upstream-ballista/`):

```sh
wc -l python/build.rs python/src/lib.rs python/src/utils.rs python/src/cluster.rs dev/msrvcheck/src/main.rs
```

| File | Lines | Role |
|---|---|---:|
| `python/build.rs` | 20 | PyO3 build config |
| `python/src/lib.rs` | 98 | `_internal_ballista` module: `PyScheduler`/`PyExecutor` classes, `create_ballista_data_frame` (ships a serialized plan blob to a remote session) |
| `python/src/utils.rs` | 62 | `to_pyerr`, future/blocking bridges |
| `python/src/cluster.rs` | 317 | `PyScheduler`/`PyExecutor` lifecycle over `start_server` / `start_executor_process` |
| `python/` total | 497 | Separate crate `pyballista` 54.1.0 (`python/Cargo.toml`): path-deps on all four Ballista crates plus `datafusion-python` 54, `pyo3` 0.28 (`abi3-py310`), `tokio`, `tonic` |
| `dev/msrvcheck/src/main.rs` | 71 | MSRV-check release tooling; no runtime content, one sentence as charged |

Judgement pointer: the binding's weight is not its 497 lines but its
coupling — full scheduler/executor path deps plus `datafusion-python` — so
§26.F takes its pattern and drops its crate.
