# map — repark-distributed

## Purpose

Distributed execution for RePark (tier 3, role `runtime`): the `DistributedExecutor` seam, the
in-process `LocalDataFusionExecutor` reference implementation, and — behind the `cluster`
feature — the Ballista-backed cluster executor. Ballista Milestone 1; the ground is
[../../task/roadmap/epic-term/ballista-audit-2026-09-08.md](../../task/roadmap/epic-term/ballista-audit-2026-09-08.md)
§26.F–§26.H, and the cards are slate 2's BALLISTA-M1-A…D.

## Design notes (card BALLISTA-M1-A, D-1 and D-2)

- **D-1 Placement.** Tier 3, role `runtime`: the crate depends on `repark-core` (the session and
  its Iceberg table providers) and nothing below tier 3 may depend on it; `repark-python` and the
  planned server crates may. The one declared internal edge and its reason live in
  [../../scripts/check_crate_dag.py](../../scripts/check_crate_dag.py) `ALLOWED_EDGES`; the role
  vocabulary there gained `runtime` for this crate. The repository manifest spells the crate's
  layer with the tier-3 name the dependency-policy SSOT uses, `surface crates` — the card's
  `layer = "runtime"` is the ROLE, not a layer spelling, and `scripts/check_manifest.py` checks
  the layer field against `TIER_NAMES`.
- **D-2 Feature flag.** Everything Ballista sits behind `features = ["cluster"]`, off by default,
  so the workspace's default build, `make verify` and the wheel are untouched. `local` is a
  default feature and always builds. The four Ballista crates are pinned `=54.1.0` in the
  workspace manifest, the release line that matches the workspace's DataFusion 54.1 pin
  (verified on crates.io at the seed commit).
- **D-3 / D-4 (step 1).** `src/executor.rs` holds `DistributedExecutor`, `JobHandle`, `JobId`,
  and `JobStatus`. `src/local.rs` holds `LocalDataFusionExecutor`: it runs a plan on the
  session `SessionContext` in-process, reports `Completed`/`Failed` after the stream drains,
  and `cancel` sets `Cancelled` without a detached spawn. pins: ballista-m1-a/C-001, C-002,
  C-003, C-004
- **BALLISTA-M1-B step 1.** `src/cluster.rs` is `ReparkClusterExecutor`: one in-process
  scheduler plus N executors on ephemeral ports (constructor arguments). Session state comes
  from `ReparkSessionProvider`; the codec seat is `repark_ballista_codec()`. The two-executor
  pin is `tests/cluster_two_executors.rs`. pins: ballista-m1-b/C-001, C-002
- **BALLISTA-M1-B step 2.** UDF-on-executor pin (`repark_times_ten` through the provider,
  vanilla session fails to resolve the same plan); cancel mid-flight (`Cancelled`, no running
  tasks within 5 s); codec install pin (`repark_ballista_codec()` matches Ballista defaults,
  and the two-executor job shuffles through those codecs). C-004 (round-trip serde over the
  five shuffle nodes) stays OPEN / PARKED: it needs `PhysicalExtensionCodec` from
  `datafusion-proto`, which this unit may not add. The owner question on the wrapper lives in
  [src/map.md](src/map.md). pins: ballista-m1-b/C-003, C-005, C-006
- **BALLISTA-M1-C step 1.** Three multi-stage shapes against the local executor on the same
  physical plan (`tests/multi_stage.rs`): hash aggregate over 4 partitions, partitioned hash
  join of two tables, sort-merge join with `prefer_hash_join=false`. Cluster row order is
  sorted before compare. Retry, metrics, and spill-dir cleanup are step 2.
  pins: ballista-m1-c/C-001

## Contents

- `Cargo.toml` — the crate manifest: the two features and the optional Ballista dependencies.
- `src/` — the crate source ([src/map.md](src/map.md)).
- `tests/` — local-executor, cluster two-executor, and multi-stage cluster pins
  ([tests/map.md](tests/map.md)).

## Pointers

- Up: [../map.md](../map.md) · The engine session: [../repark-core/map.md](../repark-core/map.md)
