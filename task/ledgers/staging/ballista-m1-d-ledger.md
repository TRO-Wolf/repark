# Unit ledger — BALLISTA-M1-D · Iceberg reads through the executors (step 2)

**Retires:** this ledger moves to `../completed/` in this unit's last commit.
This file closes when BALLISTA-M1-D merges, or when the owner closes the slate row.

**Unit:** BALLISTA-M1-D step 2 · **Date:** 2026-09-10 · **Executor:** Grok (grok-4.6), Actor, step 2 ·
**Branch:** `feat/ballista-m1-d` · **Base:** `316e6033`
**Model:** grok-4.6
**risk_tier:** standard.
**Path:** STANDARD.

Step 1 (`316e6033`) landed D-1 (`IcebergScanSpec` provider codec + round-trip), D-2
(memory catalog through `ReparkSessionProvider`), and D-3 (two-executor 8-file Iceberg
scan pins). Step 2 lands the Iceberg section and success-list line 17 in
`docs/design/distributed-m1.md`. The runtime abstraction is in place. Iceberg writes
and the commit coordinator are Milestone 3 (ADR-0004).

**Not in this unit:** Iceberg writes, commit coordinator, `STATUS.md`,
`briefs/next-sequence.md`, `.github/`, `Cargo.toml`, `Cargo.lock`,
`datafusion-proto`, `arrow_flight`, `aws`. The ledger stays in `staging/` until merge
(`ledger_lifecycle.py move` would compact the slate and STATUS, which this unit must
not edit).

## Proposition ledger

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The Iceberg `TableProvider` serialises as `(catalog config, table identifier, snapshot id, projection, filters)` and round-trips; a rebuilt provider on a session that registered the table is an Iceberg scan (reads only). | `iceberg_scan_spec_round_trips_catalog_table_snapshot_projection_and_filters`, `iceberg_scan_spec_decode_rejects_truncated_payload`, `iceberg_scan_spec_rebuilds_provider_from_session_not_ambient_catalog` in `crates/repark-distributed/tests/iceberg_scan.rs`. | **PROVEN** | Red first: `cargo test -p repark-distributed --features cluster --offline --test iceberg_scan` compile exit 101, `error[E0599]: no method named len found for reference &GenericByteArray` (missing `Array` import) and `index_of` `ArrowError` vs `engine_err`. Green after: round-trip `ok`; truncated payload refuses; rebuilt scan is `IcebergTableScan`. Writes are not in this codec. pins: ballista-m1-d/C-001 |
| C-002 | The executor resolves the catalog through the same session configuration the coordinator used, never ambient authority. The memory catalog is the test bed. | Same rebuild pin: provider rebuilds from the RePark session; a vanilla `SessionContext` refuses, naming `ReparkSessionProvider` / not registered. | **PROVEN** | Vanilla session error contains `not registered` / `ReparkSessionProvider`. Memory catalog reaches the executor through `ReparkSessionProvider::from_session`. S3/Glue executor credentials are residue (no credentials here; launcher denies `aws`). pins: ballista-m1-d/C-002 |
| C-003 | A memory-catalog Iceberg table with 8 files scanned by two executors answers the same `count(*)`, `sum`, and a filtered scan as `LocalDataFusionExecutor`, and each executor opened at least one file. | `two_executors_iceberg_count_sum_and_filtered_scan_match_local_and_each_opened_a_file`. | **PROVEN** | Red first: `SELECT count(*)` constant-folded to `ProjectionExec: expr=[8 as count(*)]` / `PlaceholderRowExec` (Iceberg stats), no `IcebergTableScan`. Green after `WHERE id + 0 >= 0` forces a scan: count/sum/`id >= 4` match local; `executor_task_counts.len() >= 2`. Cluster plans rewrite `IcebergTableScan` to parquet file groups because Ballista 54.1.0 cannot encode that node without `PhysicalExtensionCodec` (`datafusion-proto`, same wall as M1-B C-004). pins: ballista-m1-d/C-003 |

VERDICT: 3 clauses, 3 PROVEN, 0 OPEN, 0 REJECTED.

## Red-first evidence (step 1, 2026-09-10)

Command: `cargo test -p repark-distributed --features cluster --offline --test iceberg_scan -- --nocapture`

First compile of `iceberg_provider.rs` (missing `Array` import; `schema.index_of` is `ArrowError`):

```
error[E0599]: no method named `len` found for reference `&GenericByteArray<GenericStringType<i32>>` in the current scope
   --> crates/repark-distributed/src/iceberg_provider.rs:182:37
    |
182 |             for index in 0..strings.len() {
    |                                     ^^^ method not found in `&GenericByteArray<GenericStringType<i32>>`

error[E0631]: type mismatch in function arguments
   --> crates/repark-distributed/src/iceberg_provider.rs:419:51
    |
419 |         let index = schema.index_of(name).map_err(engine_err)?;
    |                                           ------- ^^^^^^^^^^
    |       expected function signature `fn(datafusion::arrow::error::ArrowError) -> _`
    |          found function signature `fn(datafusion::error::DataFusionError) -> _`

error: could not compile `repark-distributed` (lib) due to 3 previous errors
```

Exit 101.

First run of the two-executor pin (count(*) answered from Iceberg stats, no scan):

```
thread 'two_executors_iceberg_count_sum_and_filtered_scan_match_local_and_each_opened_a_file' panicked at crates/repark-distributed/tests/iceberg_scan.rs:339:9:
local Iceberg plan missing IcebergTableScan for SELECT count(*) FROM ice.sales.orders:
ProjectionExec: expr=[8 as count(*)]
  PlaceholderRowExec
test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.33s
```

Exit 101.

Green after `Array` import, `ArrowError` map, and `WHERE id + 0 >= 0` on count(*):

```
running 4 tests
test iceberg_scan_spec_decode_rejects_truncated_payload ... ok
test iceberg_scan_spec_round_trips_catalog_table_snapshot_projection_and_filters ... ok
test iceberg_scan_spec_rebuilds_provider_from_session_not_ambient_catalog ... ok
test two_executors_iceberg_count_sum_and_filtered_scan_match_local_and_each_opened_a_file ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.16s
```

## Residue

- **M1-B C-004** (codec round-trip over the five shuffle nodes) stays OPEN / PARKED; this step met the same `datafusion-proto` / `PhysicalExtensionCodec` wall for `IcebergTableScan`. The provider codec is RePark-owned bytes (`RPIC`); cluster distribution rewrites Iceberg scans to parquet file groups that Ballista's default physical codec can encode.
- **M1-C C-002** (deterministic executor-kill retry) stays OPEN; this step did not need `arrow_flight`.
- **D-2 S3/Glue leg.** Memory catalog is the test bed. An executor resolving S3/Glue under session-owned credentials (audit R-5 / Q-1) is residue until the cutover credential design lands. This clone has no credentials and the launcher denies `aws`.
- Bare `SELECT count(*)` on this 8-row table constant-folds from Iceberg stats (`PlaceholderRowExec`); the pin uses `WHERE id + 0 >= 0` so a scan remains.

## Step 2 (2026-09-10) — design-doc close

`docs/design/distributed-m1.md` now records:

- Iceberg reads in "What Milestone 1 delivers" and a dedicated Iceberg section
  (`IcebergScanSpec` round-trip, session-catalog rebuild, two-executor pins, file-group
  rewrite, S3/Glue residue).
- Success list line 17 **done** (`ballista-m1-d/C-003`), with the rewrite named.
- Open question 4: `IcebergTableScan` cannot travel without `datafusion-proto`. Same wall
  as M1-B C-004, third node type. The codec choice decides whether any RePark plan node
  can cross to an executor.
- Next milestone: Iceberg writes and the commit coordinator (Milestone 3 / ADR-0004).
  The runtime abstraction (seam, local + cluster, session provider, codec seat, Iceberg
  reads) is in place.

No product-code change this step. Clauses C-001, C-002, C-003 stay **PROVEN**.

## Coverage attestation (Actor, step 1)

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ballista-m1-d
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: IcebergScanSpec round-trips catalog config, table identifier, snapshot id, projection and filters; rebuilt provider on the RePark session is IcebergTableScan; two-executor 8-file table matches local on count/sum/filter.
      artifacts: [crates/repark-distributed/src/iceberg_provider.rs, crates/repark-distributed/tests/iceberg_scan.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Truncated payload refuses; table identifier other than catalog.namespace.table refuses; bare count(*) constant-folds from Iceberg stats so the pin uses WHERE id + 0 >= 0; eight INSERT files are counted from $files.
      artifacts: [crates/repark-distributed/tests/iceberg_scan.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Vanilla SessionContext rebuild refuses, naming ReparkSessionProvider / not registered; decode of truncated or bad-magic bytes is Error::DataFusion.
      artifacts: [crates/repark-distributed/src/iceberg_provider.rs, crates/repark-distributed/tests/iceberg_scan.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Two in-process executors under RoundRobin; executor_task_counts.len() >= 2 after the 8-file scan; codec encode/decode is pure bytes with no shared mutable state.
      artifacts: [crates/repark-distributed/tests/iceberg_scan.rs]
    - id: AT-5
      status: ATTACKED
      evidence: Rebuild looks up the catalog on the session and never constructs Glue/S3 from ambient credentials; CatalogSpec Debug already redacts secret props; S3/Glue executor credentials are residue.
      artifacts: [crates/repark-distributed/src/iceberg_provider.rs, crates/repark-distributed/tests/iceberg_scan.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Cluster vs local compare sorted cell-rows of count(*), sum(id), and id >= 4; rebuilt scan is IcebergTableScan; file count is 8.
      artifacts: [crates/repark-distributed/tests/iceberg_scan.rs]
    - id: AT-7
      status: N/A
      justification: Encode/decode is bounded by MAX_ITEM_BYTES; the two-executor pin is eight local files, not an unbounded buffer.
    - id: AT-8
      status: ATTACKED
      evidence: IcebergTableScan cannot travel without datafusion-proto (same wall as M1-B C-004); cluster plans rewrite to parquet file groups; writes and commit coordinator stay out of scope.
      artifacts: [crates/repark-distributed/src/iceberg_provider.rs, crates/repark-distributed/src/map.md]
    - id: AT-9
      status: ATTACKED
      evidence: Missing session catalog names the catalog and ReparkSessionProvider; truncated decode names truncated; empty file list names the table identifier.
      artifacts: [crates/repark-distributed/src/iceberg_provider.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first compile E0599/E0631 then count(*) PlaceholderRowExec; green after Array import, ArrowError map, and id + 0 filter; cargo test cluster 16 passed; clippy -D warnings clean.
      artifacts: [crates/repark-distributed/tests/iceberg_scan.rs, crates/repark-distributed/tests/map.md, crates/repark-distributed/src/map.md]
  complete: true
```
