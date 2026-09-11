# Unit ledger — BALLISTA-M2-A · a RePark plan node crosses to an executor (step 1)

**Retires:** this ledger moves to `../completed/` in this unit's last commit.
This file closes when BALLISTA-M2-A merges, or when the owner closes the slate row.

**Unit:** BALLISTA-M2-A step 1 · **Date:** 2026-09-11 · **Executor:** Devin (swe-2-high), Actor, step 1 ·
**Branch:** `feat/ballista-m2-a` · **Base:** `65116847`
**Model:** swe-2-high
**risk_tier:** standard.
**Path:** STANDARD.

Step 1 lands the red-first codec pins, the delegating `PhysicalExtensionCodec`
implementation, and `IcebergTableScan` travelling through the codec. Step 2 owns the design
doc (`distributed-m1.md` open questions 1 and 4), remaining map language, and ledger close-out.

**Not in this step:** `docs/design/distributed-m1.md` (step 2), `STATUS.md`,
`briefs/next-sequence.md`, `Cargo.toml`, `Cargo.lock`, `arrow_flight`.

## Proposition ledger

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `ReparkPhysicalExtensionCodec` implements `datafusion_proto::physical_plan::PhysicalExtensionCodec`, delegates every node it does not own to `BallistaPhysicalExtensionCodec`, and `repark_ballista_codec(&provider)` installs the wrapper itself (not `into_inner()`). | `repark_ballista_codec_installs_the_repark_physical_wrapper` plus the delegation proof in `ballista_shuffle_nodes_round_trip_through_the_wrapper` in `crates/repark-distributed/tests/codec.rs`. | **PROVEN** | Red first on the base tree: `installed physical codec must be the RePark wrapper, got BallistaPhysicalExtensionCodec { default_codec: DefaultPhysicalExtensionCodec }`. Green after `src/codec.rs`: installed codec Debug contains `ReparkPhysicalExtensionCodec`; the five shuffle nodes and the unowned-node refusal (`Unsupported plan node`) prove every unowned path delegates to Ballista's codec. pins: ballista-m2-a/C-001 |
| C-002 | Each of Ballista's five shuffle nodes AND an `IcebergTableScan` encode and decode through the wrapper and come back structurally equal — same node type, same schema, same partition count; for the scan the same table identifier, snapshot id, projection and filters. Re-proves BALLISTA-M1-B C-004 (`task/ledgers/completed/ballista-m1-b-ledger.md`, frozen — re-proof cited, not rewritten). | `ballista_shuffle_nodes_round_trip_through_the_wrapper` and `iceberg_table_scan_round_trips_through_the_wrapper` in `crates/repark-distributed/tests/codec.rs`. | **PROVEN** | Red first on the base tree: `wrapper refused the IcebergTableScan on encode: Internal error: Unsupported plan node, name: [IcebergTableScan] .` — the default codec refuses the scan exactly as the card predicts. Green after: five shuffle nodes + the scan round-trip; the `RPIC` payload decodes to the same catalog spec, `ice.sales.orders` identifier, frozen resolved snapshot id, `["id"]` projection and `["id >= 4"]` filter; the decoded node re-encodes byte-identically; decode rebuilds through the codec-carried session catalog and verifies the rebuilt `resolved_snapshot_id` equals the frozen one. pins: ballista-m2-a/C-002 |
| C-003 | The parquet-file-group rewrite stays and is pinned as the path for a node with no codec entry — measured: an unowned custom node is refused loud by `try_encode` and passes through `rewrite_iceberg_table_scans_as_file_groups` untouched. | `unowned_node_refuses_encode_and_passes_the_rewrite_untouched` in `crates/repark-distributed/tests/codec.rs`. | **PROVEN** | Measured: `UnownedScanExec` passes through the rewrite unchanged (`Arc::ptr_eq`) and `try_encode` refuses with `Unsupported plan node, name: [UnownedScanExec]`. The rewrite stays the distribution path only for plans whose scans it owns; a node with no codec entry refuses loud rather than travelling. pins: ballista-m2-a/C-003 |
| C-004 | No `arrow_flight`; BALLISTA-M1-C C-002 stays OPEN with its residue — reading clause: `docs/design/distributed-m1.md` keeps the flight-codec ban line and no dependency on `arrow_flight` appears anywhere in the crate graph. | Reading: cite the design-doc line; `cargo tree` check for `arrow_flight`. | **PROVEN** | `docs/design/distributed-m1.md` line 134: "Reimplementing that startup needs `arrow_flight`, which this crate does not depend on and must not add." `Cargo.toml`/`Cargo.lock` untouched; `cargo tree -p repark-distributed --features cluster` shows `arrow-flight v58.4.0` only as a transitive entry under `datafusion`/`ballista`, no direct edge. pins: ballista-m2-a/C-004 |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

## Red-first evidence (step 1, 2026-09-11)

Command: `cargo test -p repark-distributed --features cluster --test codec` on the base tree
(`65116847`, `repark_ballista_codec()` installing the Ballista default physical codec).

```
---- repark_ballista_codec_installs_the_repark_physical_wrapper stdout ----
thread 'repark_ballista_codec_installs_the_repark_physical_wrapper' panicked at
crates/repark-distributed/tests/codec.rs:261:5:
installed physical codec must be the RePark wrapper, got
BallistaPhysicalExtensionCodec { default_codec: DefaultPhysicalExtensionCodec }

---- iceberg_table_scan_round_trips_through_the_wrapper stdout ----
thread 'iceberg_table_scan_round_trips_through_the_wrapper' panicked at
crates/repark-distributed/tests/codec.rs:334:9:
wrapper refused the IcebergTableScan on encode: Internal error: Unsupported plan node,
name: [IcebergTableScan] .
```

Green after `src/codec.rs` (the delegating impl) + `src/iceberg_provider.rs`
(`IcebergScanSpec::from_scan_node`) + `src/cluster.rs` (provider into `repark_ballista_codec`):

```
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

## Mechanism notes for the Critic

- `try_encode` receives only `Arc<dyn ExecutionPlan>`; `repark-distributed` cannot name
  `IcebergTableScan` (no `iceberg-datafusion` dep — none is added). The node is recognised by
  `name()`, and the spec fields are recovered from the node's Debug/Verbose display text:
  `identifier: TableIdent`, `resolved_snapshot_id` (the frozen scanned snapshot, which the
  measured run showed can differ from the `snapshot_id` plan target), `projection`, and the
  verbose `predicate:[...]` arm. The catalog is recovered by probing
  `context.catalog_names()` → iceberg catalog providers → `schema(ns).table_names()` for the
  (namespace, table) pair; a miss or an ambiguous hit refuses loud.
- `try_decode` receives only `TaskContext`, which carries no catalogs. The codec therefore
  holds a `SessionContext` built from `ReparkSessionProvider::session_state()` — the same
  session authority handed to the executors — and rebuilds the provider through
  `IcebergScanSpec::scan` on a dedicated thread with a fresh `current_thread` runtime (the
  decode call site may already sit inside a tokio worker, so `block_on` on the ambient
  runtime is not usable).
- A decoded scan whose rebuilt `resolved_snapshot_id` differs from the spec's frozen value
  refuses loud: the executor cannot bind a pinned snapshot (`IcebergStaticTableProvider`
  lives behind the absent `iceberg-datafusion` dep), so a moved or time-travel-pinned table
  is an error, never a silent divergent read.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ballista-m2-a
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause red-first on the base tree (install pin Debugs the inner Ballista codec; the scan encode refuses `Unsupported plan node, name: [IcebergTableScan]`), then green after the delegating impl — the C-002 wire fields are asserted cell by cell (catalog name/kind, table identifier, frozen snapshot id, projection, filters) plus decoded-node name/schema/partition count and byte-identical re-encode.
      artifacts: [crates/repark-distributed/tests/codec.rs, crates/repark-distributed/src/codec.rs, crates/repark-distributed/src/iceberg_provider.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Boundary inputs exercised — a codec built on a vanilla session (no catalog), an unowned custom `ExecutionPlan` (no codec entry), a five-node delegation sweep, a scan built through the real memory-catalog bed; nested/empty namespace, missing marker, and unknown catalog kind all take the loud-refusal branches in the Debug parser.
      artifacts: [crates/repark-distributed/tests/codec.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Failure paths pinned loud — vanilla decode refuses naming the session catalog (`not registered`/`ReparkSessionProvider`), vanilla encode refuses, unowned-node encode refuses with `Unsupported plan node`, and decode refuses when the rebuilt `resolved_snapshot_id` differs from the spec's frozen value (moved or time-travel-pinned table).
      artifacts: [crates/repark-distributed/tests/codec.rs, crates/repark-distributed/src/codec.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The decode bridge deliberately spawns a joined `std::thread` with a fresh `current_thread` runtime rather than `Runtime::block_on`, because `try_decode` runs inside the executor's tokio worker where ambient `block_on` panics; the handle is always joined, errors map to `DataFusionError`, no detached task survives.
      artifacts: [crates/repark-distributed/src/codec.rs]
    - id: AT-5
      status: ATTACKED
      evidence: Decode resolves catalogs only through the codec-carried `SessionContext` built from `ReparkSessionProvider::session_state` — ambient authority is unreachable by construction and the vanilla-session refusal is pinned; the wire spec carries catalog name/kind plus the `warehouse`/`props` strings the provider Debug exposes, the same fields M1-D's spec already serialised.
      artifacts: [crates/repark-distributed/tests/codec.rs, crates/repark-distributed/src/iceberg_provider.rs]
    - id: AT-6
      status: ATTACKED
      evidence: The wire format is M1-D's `RPIC` v1 spec unchanged (magic/version checks, length bounds, trailing-byte refusal all pre-pinned); the scan's frozen `resolved_snapshot_id` — which a measured run showed can differ from the plan-time `snapshot_id` field — is what travels and is re-verified on the rebuilt node.
      artifacts: [crates/repark-distributed/src/iceberg_provider.rs, crates/repark-distributed/tests/codec.rs]
    - id: AT-7
      status: N/A
      justification: Encode is a bounded walk over session catalogs; decode spawns one joined thread per task decode — no unbounded growth, no hot-path change.
    - id: AT-8
      status: ATTACKED
      evidence: The wrapper delegates every non-owned `PhysicalExtensionCodec` method (nodes, exprs, UDF/UDAF/UDWF) to `BallistaPhysicalExtensionCodec`, so a Ballista repin keeps delegation rather than shadowing; the five-node round-trip pins the delegation contract directly. No `Cargo.toml`/`Cargo.lock`/`arrow_flight` change.
      artifacts: [crates/repark-distributed/src/codec.rs, crates/repark-distributed/tests/codec.rs]
    - id: AT-9
      status: N/A
      justification: Every new refusal is a `DataFusionError`/`repark Error` naming the node, the missing catalog, or the snapshot mismatch — DataFusion surfaces it on the task as today; no new silent path exists to alarm on.
    - id: AT-10
      status: ATTACKED
      evidence: Pins cover every new branch by name — install pin (wrapper vs inner), five-node delegation, scan round-trip including the frozen-snapshot assert, vanilla encode/decode refusals, unowned-node encode refusal plus rewrite pass-through; a mutation flipping the `node.name()` check or the `MAGIC` guard would red the suite.
      artifacts: [crates/repark-distributed/tests/codec.rs]
  complete: true
```

