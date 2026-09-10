# Distributed Milestone 1 — what it delivers

**Opened:** 2026-09-10. **Class:** campaign. **State:** in flight on `feat/ballista-m1-d`.
**Opened:** 2026-09-10. **Class:** campaign. **State:** in flight on `feat/ballista-m1-c`.
**Retires:** this file closes when Ballista Milestone 1 is accepted (M1-D merged) or the owner
closes the slate row. Archive with the campaign to `docs/history/` at that event.

Ground: [task/roadmap/epic-term/ballista-audit-2026-09-08.md](../../task/roadmap/epic-term/ballista-audit-2026-09-08.md)
§26.F–§26.H (depend on Ballista 54.1.0, do not vendor). Cards:
[task/roadmap/mid-term/cheap-tier-slate-2-2026-09-09.md](../../task/roadmap/mid-term/cheap-tier-slate-2-2026-09-09.md)
BALLISTA-M1-A…D.

The crate is `crates/repark-distributed`. Local execution always builds. Ballista sits behind
`features = ["cluster"]`, off by default.

## What Milestone 1 delivers

- **Seam.** `DistributedExecutor` with `execute` / `status` / `cancel`, `JobHandle::stream`,
  `JobStatus` including `Completed { stages, retried_stages }`. pins: ballista-m1-a/C-001
- **Local reference.** `LocalDataFusionExecutor` runs a physical plan on the session
  `SessionContext` in-process. pins: ballista-m1-a/C-002, C-003
- **In-process cluster.** `ReparkClusterExecutor` starts one scheduler and N executors on
  ephemeral ports. Session state comes from `ReparkSessionProvider`. The codec seat is
  `repark_ballista_codec()`. pins: ballista-m1-b/C-001, C-002, C-003, C-005, C-006
- **Multi-stage reads.** Hash aggregate over 4 partitions, partitioned hash join of two tables,
  and sort-merge join with `prefer_hash_join=false` each match the local executor on the same
  plan. pins: ballista-m1-c/C-001
- **Per-stage metrics on complete.** `JobStatus::Completed` carries per-stage rows, shuffle
  bytes, wall time, and attempt number, read from the scheduler execution graph (not the
  Prometheus collector, which only records job-level timestamps). pins: ballista-m1-c/C-003
- **Iceberg reads.** `IcebergScanSpec` round-trips
  `(catalog config, table identifier, snapshot id, projection, filters)` and rebuilds the
  Iceberg `TableProvider` from the session catalog (`ReparkSessionProvider`), never ambient
  authority. A memory-catalog table with 8 files scanned by two executors matches the local
  executor on `count(*)`, `sum`, and a filtered scan; both executors ran a task. Reads only.
  pins: ballista-m1-d/C-001, C-002, C-003

What this milestone does **not** deliver: Iceberg writes or a commit coordinator (Milestone 3 /
[ADR-0004](../adr/0004-server-prep-disciplines.md)), a remote (not in-process) cluster form,
deterministic task retry, S3/Glue executor credentials, and native `IcebergTableScan` serde
(see open questions).

## Iceberg reads (M1-D)

The DataFusion-level provider codec the audit's R-4 recorded is
`crates/repark-distributed/src/iceberg_provider.rs` (`IcebergScanSpec`). It encodes
`(catalog config, table identifier, snapshot id, projection, filters)` as `RPIC` bytes and
decodes them back. Rebuild looks up the table on the `SessionContext` the coordinator
handed the executors through `ReparkSessionProvider`. A vanilla DataFusion session has no
catalog and the rebuild refuses. The memory catalog is the test bed.
pins: ballista-m1-d/C-001, C-002

The two-executor pin is `tests/iceberg_scan.rs`: eight Iceberg data files, `count(*)` (with
`id + 0 >= 0` so snapshot stats cannot constant-fold), `sum(id)`, and `id >= 4`, each equal
to `LocalDataFusionExecutor`; `executor_task_counts` has both executors. Bare `count(*)` on
this table is `PlaceholderRowExec` from Iceberg stats and is not the pin.
pins: ballista-m1-d/C-003

`IcebergTableScan` is a custom `ExecutionPlan`. Ballista 54.1.0 encodes five shuffle nodes
and then falls through to `DefaultPhysicalExtensionCodec`, which is not implemented.
`PhysicalExtensionCodec` lives in `datafusion-proto`, which this crate must not add (the
same wall as M1-B C-004). Cluster plans therefore rewrite `IcebergTableScan` to parquet
file groups taken from the Iceberg `$files` list, which the default physical codec can
encode. The coordinator still plans through Iceberg; the executors read the file groups
under the session `RuntimeEnv`.

**S3/Glue residue.** Audit R-5 / Q-1: executors must resolve unread S3/Glue paths under
session-owned credentials, never ambient authority. This unit does not attempt that leg.
There are no credentials in this clone and the launcher denies `aws`. The cutover
credential design owns it.

**What the next milestone owns.** The runtime abstraction is now in place: the
`DistributedExecutor` seam, the local reference, the in-process cluster, the session
provider, the codec seat, and Iceberg **reads**. Iceberg **writes** and the commit
coordinator are Milestone 3. ADR-0004 stands: executors may produce files; one authority
commits; do not build Ballista-for-writes. A later unit that wants RePark plan nodes
(`IcebergTableScan` today, write/commit nodes later) to cross to an executor must take
`datafusion-proto` and make the codec wrapper a real delegating `PhysicalExtensionCodec`.
Until then every RePark-owned node is rewritten or it stays on the coordinator.

What this milestone does **not** deliver: Iceberg scans on the executors (M1-D), Iceberg writes
or a commit coordinator (Milestone 3 / ADR-0004), a remote (not in-process) cluster form, and
deterministic task retry (see open questions).

## Owner's plan §12 success list

The owner's plan §12 text is not in this repository (the audit recorded the same for §26–§28).
The list below is the M1 card Done conditions, checked line by line against pins. A line that is
not pinned does not read as done.

| # | Success line (from M1-A…D Done / D-rows) | Status | Pin or owner |
|---|---|---|---|
| 1 | `DistributedExecutor` + `JobHandle` + `JobStatus` exist | done | ballista-m1-a/C-001 |
| 2 | `LocalDataFusionExecutor` `range(1000)` sum equals direct DataFusion | done | ballista-m1-a/C-002 |
| 3 | Local `status` is Queued/Running before drain and Completed after | done | ballista-m1-a/C-002 |
| 4 | Local `cancel` on a long range returns in 1 s with `Cancelled` | done | ballista-m1-a/C-003 |
| 5 | `cluster` feature compiles with the four Ballista crates | done | ballista-m1-a/C-004 |
| 6 | Two in-process executors answer `sum` equal to local; both ran a task; status walks Queued → Running → Completed | done | ballista-m1-b/C-001, C-002 |
| 7 | A RePark UDF registered through the session provider resolves on an executor | done | ballista-m1-b/C-003 |
| 8 | Cluster cancel mid-flight sets `Cancelled` and no running tasks within 5 s | done | ballista-m1-b/C-005 |
| 9 | Codec install is Ballista defaults; two-executor shuffle completes through those codecs | done | ballista-m1-b/C-006 |
| 10 | Codec round-trip serde over the five shuffle nodes | **missing** | ballista-m1-b/C-004 PARKED — needs `datafusion-proto` |
| 11 | Hash aggregate over 4 partitions equals local on the same plan | done | ballista-m1-c/C-001 |
| 12 | Hash join of two tables equals local on the same plan | done | ballista-m1-c/C-001 |
| 13 | Sort-merge join with `prefer_hash_join=false` equals local on the same plan | done | ballista-m1-c/C-001 |
| 14 | Kill one executor mid-shuffle; job completes on the remaining executor; status carries retried stage count | **missing** | ballista-m1-c/C-002 OPEN — see open questions |
| 15 | `Completed` carries per-stage rows, shuffle bytes, wall time; shuffle bytes > 0 on the two-stage shape | done | ballista-m1-c/C-003 |
| 16 | Shuffle data under the session spill dir is removed on complete or cancel | **narrowed** | ballista-m1-c/C-004 — session spill dir has no shuffle files; Ballista standalone work_dir is not that dir |
| 17 | Iceberg memory-catalog scan on two executors equals local | **done** | ballista-m1-d/C-003 — `IcebergTableScan` rewritten to parquet file groups; see open question 4 |
| 17 | Iceberg memory-catalog scan on two executors equals local | **missing** | BALLISTA-M1-D, not started |

## Open questions this unit family produced

1. **M1-B C-004 — codec round-trip.** `PhysicalExtensionCodec` lives in `datafusion-proto`, which
   is not a `repark-distributed` dependency and this family may not add. Owner options (do not
   choose here): M1-C-or-later takes a `datafusion-proto` dev-dependency and the wrapper becomes a
   real delegating codec, or the wrapper is deleted and RePark uses Ballista's codec until a
   RePark plan node needs serialising. Recorded in
   [crates/repark-distributed/src/map.md](../../crates/repark-distributed/src/map.md).

2. **M1-C C-002 — deterministic retry.** Ballista 54.1.0 exposes `ChaosExec` (transient IoError,
   retryable) but the fault decision is `seed + partition` and does not change on retry, so
   probability 1.0 fails every attempt up to `task_max_failures` (4) and the job fails.
   Stopping one executor requires owning the Flight server and poll-loop `JoinHandle`s.
   `new_standalone_executor_from_builder` returns `()` and spawns both internally.
   Reimplementing that startup needs `arrow_flight`, which this crate does not depend on and
   must not add. Scheduler `remove_executor` is `pub(crate)`. No deterministic fail-once hook
   is reachable without a new dependency or an upstream API. `Completed.retried_stages` is
   wired from `stage_attempt_num` for when a later unit can inject the failure.

3. **M1-C C-004 residue — shuffle home.** `ballista-executor` 54.1.0 standalone
   (`standalone.rs` line 118) sets `work_dir` from `TempDir::new()` and drops the `TempDir`
   immediately. Shuffle files land under that OS-temp path, not the session
   `datafusion.runtime.temp_directory`. `clean_shuffle_data_loop` lives in `executor_process.rs`
   and is not started on the standalone path; poll-loop `jobs_to_clean` would clean that
   work_dir if we held it. The pin checks the session spill directory, which never receives
   those files.

4. **M1-D — `IcebergTableScan` cannot travel.** This is M1-B C-004's wall on a third node
   type. `PhysicalExtensionCodec` is in `datafusion-proto`. Ballista does not re-export the
   trait. The family must not add the crate. Without an `impl PhysicalExtensionCodec`,
   Ballista cannot encode `IcebergTableScan` (or any later RePark write/commit node). Cluster
   Iceberg reads rewrite the scan to parquet file groups so the default codec can ship them.
   That rewrite is the delivered M1 read path, not a workaround to drop later in silence.

   The codec question is not a detail. It decides whether a RePark plan node can cross to an
   executor at all. Owner options (do not choose here): a later unit takes `datafusion-proto`
   and the wrapper becomes a real delegating codec, or RePark keeps rewriting custom nodes
   into first-class DataFusion scans until a node has no such rewrite. The same choice sits
   on M1-B C-004. Recorded in
   [crates/repark-distributed/src/map.md](../../crates/repark-distributed/src/map.md).
