# Distributed Milestone 1 — what it delivers

**Opened:** 2026-09-10. **Class:** campaign. **State:** M1 delivered (M1-A…D merged);
the record continues through BALLISTA-M2-A on `feat/ballista-m2-a` — open questions 1 and
4 closed 2026-09-11, question 2 open.
**Retires:** this file closes when the distributed campaign closes or the owner
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
deterministic task retry, and S3/Glue executor credentials. Native `IcebergTableScan` serde
was on this list until BALLISTA-M2-A step 1 (2026-09-11) — the delegating codec now carries
the scan; see open questions 1 and 4.

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
`PhysicalExtensionCodec` lives in `datafusion-proto`, which this milestone could not add
(the same wall as M1-B C-004), so the M1 read path rewrote `IcebergTableScan` to parquet
file groups taken from the Iceberg `$files` list. BALLISTA-M2-A step 1 (2026-09-11, ruling
RF-9) took `datafusion-proto =54.1.0` behind `cluster` and made the codec wrapper a real
delegating `PhysicalExtensionCodec`: the scan now crosses to the executor as an `RPIC`
`IcebergScanSpec` rebuilt under session authority (open question 4). The file-group
rewrite stays — it remains the M1 read path and the fallback for a plan node with no codec
entry (BALLISTA-M2-A D-3). The coordinator still plans through Iceberg; the executors read
the file groups under the session `RuntimeEnv`.

**S3/Glue residue.** Audit R-5 / Q-1: executors must resolve unread S3/Glue paths under
session-owned credentials, never ambient authority. This unit does not attempt that leg.
There are no credentials in this clone and the launcher denies `aws`. The cutover
credential design owns it.

**What the next milestone owns.** The runtime abstraction is now in place: the
`DistributedExecutor` seam, the local reference, the in-process cluster, the session
provider, the codec seat, and Iceberg **reads**. Iceberg **writes** and the commit
coordinator are Milestone 3. ADR-0004 stands: executors may produce files; one authority
commits; do not build Ballista-for-writes. BALLISTA-M2-A step 1 took `datafusion-proto`
and made the codec wrapper a real delegating `PhysicalExtensionCodec` — `IcebergTableScan`
crosses to an executor today, and the same codec path is the seat later RePark
write/commit nodes will need.

**Scheduler placement input (measured 2026-09-11, inherited by BALLISTA-M2-A).**
Depending on `ballista-scheduler` 54.1.0 under default features always pulls `axum` +
`tower-http` and the AWS credential stack (`aws-config`, `aws-credential-types`);
`prometheus`, `graphviz-rust`, and the KEDA scaler stay out unless their features are
enabled. A Milestone 2 that moves the scheduler out of process inherits that surface, so
its placement decision says which tier it accepts. Measured in the Ballista audit §26.C /
appendix A2.

What this milestone does **not** deliver: Iceberg writes or a commit coordinator
(Milestone 3 / ADR-0004), a remote (not in-process) cluster form, and deterministic task
retry (see open question 2). Iceberg scans do run on the executors — through the file-group
rewrite in M1-D, and natively through the codec since BALLISTA-M2-A step 1.

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
| 9 | Codec install is Ballista defaults; two-executor shuffle completes through those codecs | done | ballista-m1-b/C-006 — since BALLISTA-M2-A the install is the RePark wrapper delegating to the same Ballista defaults (ballista-m2-a/C-001) |
| 10 | Codec round-trip serde over the five shuffle nodes | **done** | ballista-m2-a/C-002 — re-proves ballista-m1-b/C-004, un-parked by RF-9 |
| 11 | Hash aggregate over 4 partitions equals local on the same plan | done | ballista-m1-c/C-001 |
| 12 | Hash join of two tables equals local on the same plan | done | ballista-m1-c/C-001 |
| 13 | Sort-merge join with `prefer_hash_join=false` equals local on the same plan | done | ballista-m1-c/C-001 |
| 14 | Kill one executor mid-shuffle; job completes on the remaining executor; status carries retried stage count | **missing** | ballista-m1-c/C-002 OPEN — see open questions |
| 15 | `Completed` carries per-stage rows, shuffle bytes, wall time; shuffle bytes > 0 on the two-stage shape | done | ballista-m1-c/C-003 |
| 16 | Shuffle data under the session spill dir is removed on complete or cancel | **narrowed** | ballista-m1-c/C-004 — session spill dir has no shuffle files; Ballista standalone work_dir is not that dir |
| 17 | Iceberg memory-catalog scan on two executors equals local | **done** | ballista-m1-d/C-003 — the M1 read path rewrites `IcebergTableScan` to parquet file groups (kept as the fallback for a node with no codec entry, ballista-m2-a/C-003); the scan also crosses natively through the codec since BALLISTA-M2-A (ballista-m2-a/C-002) |

## Open questions this unit family produced

1. **M1-B C-004 — codec round-trip.** `PhysicalExtensionCodec` lives in `datafusion-proto`, which
   is not a `repark-distributed` dependency and this family may not add. Owner options (do not
   choose here): M1-C-or-later takes a `datafusion-proto` dev-dependency and the wrapper becomes a
   real delegating codec, or the wrapper is deleted and RePark uses Ballista's codec until a
   RePark plan node needs serialising. Recorded in
   [crates/repark-distributed/src/map.md](../../crates/repark-distributed/src/map.md).

   **Closed 2026-09-11 by RF-9** (review-fix-slate 2026-09-10): take `datafusion-proto` — landed
   as `=54.1.0`, an optional dependency behind `cluster`, at the BALLISTA-M2-A seed (#494). The
   wrapper is now a real delegating `PhysicalExtensionCodec`: the five Ballista shuffle nodes
   round-trip through it, every node it does not own delegates to
   `BallistaPhysicalExtensionCodec`, and `repark_ballista_codec(&provider)` installs the wrapper
   itself into `BallistaCodec::new`. What step 1 proved beyond the round-trip: encode never emits
   a spec that rebuilds to a different scan — the spec is rebuilt through the decode path on the
   codec's session context and compared field by field, and any difference refuses loud naming
   the field. The residue that remains is BALLISTA-M2-A-R-001 (text recovery — see open
   question 4). pins: ballista-m2-a/C-001, C-002

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

   **Closed 2026-09-11 by RF-9** — the same ruling as question 1. `IcebergTableScan` now
   encodes to the `RPIC` `IcebergScanSpec` (catalog spec, table identifier, frozen resolved
   snapshot id, projection, predicate) and decodes by rebuilding through
   `IcebergScanSpec::scan` against the `SessionContext` the codec carries — built from
   `ReparkSessionProvider::session_state`, never ambient authority — with the rebuilt node's
   `resolved_snapshot_id` verified against the spec's frozen value. Encode verifies before
   emitting: the spec is rebuilt on the same session context and must match the original on
   table identifier, resolved snapshot id, projected schema, predicate text and partition
   count, else the encode refuses loud naming the field.

   What crosses today and what refuses (measured, BALLISTA-M2-A pins): the plain scan and an
   `IN`-list predicate (`id IN (1, 2, 3)` renders `id IN (1, 3, 2)`) round-trip exactly and
   answer equal to the local executor on the two-executor cluster; identifiers and projected
   columns carrying `"` `\` `[` `]` refuse up front; every string-literal predicate refuses —
   the fork's predicate Display renders literals double-quoted (`name = "alpha"`), which
   re-parses as a missing column identifier — and a literal containing `] snapshot_id=`
   refuses the same way; date/timestamp literals never bind into the node's predicate at all
   (dropped at pushdown — `predicate:[]`), so those nodes round-trip exactly. Residue
   **BALLISTA-M2-A-R-001**: the encode path recovers the spec from the node's Debug/Verbose
   text plus a session-catalog probe only because `iceberg-datafusion` is not a dependency of
   this crate; the fork's `IcebergTableScan` already exposes typed accessors (`table()`,
   `snapshot_id()`, `projection()`, `predicates()` in the fork's
   `crates/integrations/datafusion/src/physical_plan/scan.rs`), so a direct optional
   dependency behind `cluster` would retire the parsing — an owner question, not this unit's
   call. The file-group rewrite stays as the M1 read path and the fallback for a node with no
   codec entry. pins: ballista-m2-a/C-002, C-003
