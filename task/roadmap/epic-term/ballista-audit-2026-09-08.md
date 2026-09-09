# Ballista audit — Milestone 0 of the Rust migration pilot

**Opened:** 2026-09-08. **Class:** campaign. **State:** proposal; step 1 (Half A, facts) landed, step 2 (Half B, judgement) pending.
**Audited upstream:** Apache DataFusion Ballista tag `54.1.0`, commit `f4e66525`
(`docs: add 54.1.0 changelog and restore missing 54.x toctree entries (#2243)`),
cloned at `upstream-ballista/` (git-excluded scratch copy, never vendored).
**D-1 version pair:** upstream root `Cargo.toml` declares `datafusion = "54"`;
this workspace pins `datafusion = "54.1.0"`. Match, no delta risk.
**Standing question:** `docs/adr/0004-server-prep-disciplines.md` rules out
Ballista-for-writes because the protobuf plan serialization cannot carry
RePark's Iceberg write/commit nodes. The serialization chapter below (Half A
facts now, disposition in step 3) says whether an extension codec changes that.
The commit-coordinator boundary (executors produce files, one authority commits)
is kept either way.

## §26.A — Version pair and audit scope

Step 2 fills this section.

## §26.B — Crate and line inventory (facts in appendix A1)

Step 2 fills this section.

## §26.C — Dependency inventory (facts in appendix A2)

Step 2 fills this section. Per step D-8 the source is each crate's own
`Cargo.toml`, not `cargo tree`, which needs registry network access.

## §26.D — Serialization: protobuf files and extension codecs (facts in appendixes A3–A4)

Step 2 fills this section.

## §26.E — Lifecycle file map: submit to result (facts in appendix A5)

Step 2 fills this section.

## §26.F — Module classification and crate-structure proposal

Step 2 fills this section.

## §26.G — Risks

Step 2 fills this section.

## §26.H — Dependency-versus-import recommendation

Step 2 fills this section.

## §27 — Findings and open questions

Step 2 fills this section.

## §28 — Decision gate

Step 2 fills the decision-gate table, one line of evidence per criterion.

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
