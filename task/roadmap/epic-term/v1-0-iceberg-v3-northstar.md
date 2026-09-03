# V1.0 north star — full production-grade Iceberg format-v3

**Set 2026-08-23 · the owner's ruling is the merge of the change that added this file.**
**Retires:** this track leaves for [../mid-term/](../mid-term/map.md) when an intake evaluates it
into units (this bin holds direction, not unit lists); the north star itself retires when a v1.0
tag ships with §3's gate green, or when the owner re-rules it.

**The ruling.** v1.0 means **full production-grade Iceberg format-v3 support**: this engine
reads, writes, and maintains v3 tables with the confidence it has on v2 today — proven against
Spark, proven live on the two AWS catalogs where the service permits, and carrying a dated
DECLARED row for every deliberate difference. [PROJECT.md](../../../PROJECT.md) Goals states the
north star and points here; this file is the definition's single home.

## 1. Why this is credible, measured not hoped

The V3-0 audit ([design](../../../docs/design/format-v3-track.md) ·
[ledger](../../ledgers/staging/v3-0-charter-ledger.md)) ran the surfaces against a Spark-written
v3 fixture on 2026-08-21 and found the **data-read** half already correct — deletion-vector
reads return Spark's numbers exactly, and appends carry the mandatory row lineage through a full
cross-engine round trip (the lineage *columns* themselves are not yet readable — registry
`V3-ROWID-1`). The claim "deepest Iceberg support of any single-node engine" is partially true
on v3 today; this north star is about finishing the write, maintenance, and type halves and
then proving all of it where production lives.

The window is also closing: DuckDB-Iceberg 1.5.3 (2026-05-29) ships v3 reads **and writes**
including deletion vectors and variant, while Trino's v3 support is experimental with row-level
DML and `OPTIMIZE` unsupported (Trino 483 docs). The depth claim is now contested, and v3
write + maintenance depth is where it gets decided.

## 2. What "full production-grade v3" means — four pillars

1. **Write v3 natively.** Create v3 tables behind an explicit opt-in; merge-on-read DML that
   writes Puffin deletion vectors — the ratified spec **forbids new position-delete files in
   v3**, so DVs are not optional for a v3 writer; row lineage carried correctly on **every**
   write path, not only append.
2. **Maintain v3.** Every Iceberg surface the engine offers on v2 — the five `CALL system.*`
   procedures, snapshot-ref DDL, time travel — runs on v3 with Spark-compared results:
   compaction that preserves lineage and removes the deletion vectors it strands, DV-aware
   delete-file maintenance, expiry, orphan cleanup. (`rewrite_manifests` joined the roster on
   v2 — MW-6, [#230](https://github.com/TRO-Wolf/repark/pull/230); the v3 exercise remains.)
3. **v3 types and schema features.** `variant`, `geometry`, `geography`, `timestamp_ns` /
   `timestamptz_ns`, `unknown`, and column default values (`initial-default` /
   `write-default`) — each readable and writable, or a dated DECLARED row saying exactly what
   is not supported and why. Two v3 features get an explicit ruling rather than silence: table
   **encryption keys** (ratified in v3, optional per table) and **multi-argument transforms**
   (scaffolding-only in the ratified spec — no concrete transform is defined; readers must
   ignore unknown transforms when filtering).
4. **Production evidence.** Local correctness re-proven live on Glue and S3 Tables (both state
   v3 support — §5), a scale measurement beyond the 1,000-row demo, full statement-coverage
   comparison against PySpark on v3 tables, and a v3 leg in the nightly oracle, green.

## 3. The starting line (2026-08-23; measured cells cite their source)

Every row means **both SQL doors plus the facade** unless the cell says otherwise —
[docs/testing.md](../../../docs/testing.md)'s row-per-entry-point rule applies to the gate.

| Surface | Today | v1.0 requires | Owner |
|---|---|---|---|
| Read: data + deletion vectors (unpartitioned + **partitioned**, 2026-08-24 V3E-3) | ✅ Spark-exact (format-v3-track §2; V3E-3 partitioned identity `part`) | stays green + live leg | evidence (intake) |
| Read: equality deletes alongside DVs; delete-file metadata tables on v3 | ✅ Spark-exact (V3E-3, 2026-08-24) — Puffin DV + equality-delete file; Spark/facade `.delete_files` and ANSI `$delete_files` content 1/2 | stays green + live leg | evidence (intake) |
| Read: `_row_id` / `_last_updated_sequence_number` | ✅ V3-4 (2026-08-31): Spark-equal on single-table v3 reads, three doors; JOIN/CTE/subquery/time-travel refuse `V3-ROWID-2`; v1/v2 engine Schema `No field named _row_id` (`V3-ROWID-1` FIXED). RP-6 (2026-09-01): MoR UPDATE preserve-half is Spark-equal; F-7 residue retired | served as columns, Spark-equal (single-table) | V3-4 |
| Read/write: v3 types + default values | ✅ by dated DECLARED residual — V3-6 (2026-09-01): opt-in v3 CREATE takes `timestamp_ns` / `timestamptz_ns` (v2 refuses); append fills an omitted column from a schema-carried `write_default`, `initial_default` reads into pre-column files, DEFAULT DDL refuses Spark-equal; `unknown` CREATE refuse and parquet write refuse pinned (R91, RP-5). **Ruled 2026-08-25:** `geometry` / `geography` DECLARED out (registry `V3-GEO-1`); shredded-Parquet `variant` DECLARED out and binary `variant` measured refusing end to end (`V3-VARIANT-SHRED-1`, fork R88) — the residual is that dated DECLARED row | per-feature support or DECLARED | V3-6 (fork F-15) |
| Table encryption keys (v3, optional) | ✅ by dated DECLARED exclusion — **owner ruled 2026-08-24**; registry `ENC-1` (V3E-1), pinned by `v3_cow.rs::v3_create_with_encryption_key_id_still_scans_without_a_kms` (a stored `encryption.key-id` never changes a scan) | the dated DECLARED registry row | ruled |
| Write: create v3 | ✅ opt-in CREATE/CTAS (`repark.sql.allowCreateFormatVersion3`, default false; V3-2) | stays opt-in until V3-3; default remains v2 | V3-2 |
| Upgrade: v2 → v3 in place (`ALTER … SET TBLPROPERTIES`, both doors) | ✅ V3-10 (2026-09-02): behind `repark.sql.allowCreateFormatVersion3` the ALTER upgrades in place through the fork's `UpgradeFormatVersionAction` on all three doors — metadata-only, no snapshot, `next-row-id` 0, reserved key not persisted, upgrade + another key ONE commit, same-version a no-op; after it append `(1,2,1),(2,3,1),(3,4,1),(4,0,2),(5,1,2)` next-row-id 5, COW DELETE next-row-id 3 / UPDATE 6, MoR MERGE-delete ONE DV next-row-id 4, `rewrite_data_files` six ids 0–5 at seq 7, `register_table` reads v3 — registry `V3-UPGRADE-1`. Residual dated: `V3-UPGRADE-V4-1` (Spark takes `'4'`, this engine tops out at v3). **`V3-UPGRADE-DV-1` FIXED (V3-12, 2026-09-02):** a v3 MoR write over a legacy parquet position delete now merges its positions into the new DV and removes the superseded file in the same `RowDelta` — Spark-equal on three doors | the **owner ruling 2026-08-25** discharged; residuals stay dated DECLARED rows | V3-10, V3-12 |
| Write: append incl. row lineage | ✅ Spark-verified (format-v3-track §2) | stays green + live leg | evidence (intake) |
| Write: MOR DML via deletion vectors | ✅ V3-9 (2026-09-02): `V3-MOR-1` FIXED — predicate DML's V2-only delete-file gate is lifted, so `DELETE … WHERE` / `UPDATE … WHERE` on v3 write one file-scoped Puffin DV per touched data file on all three doors, created and adopted (`DELETE` `IN`/`EXISTS`/plain → `(1,0,1),(3,2,1)` next-row-id 3 added 0, 1-record DV; `NOT IN`/`NOT EXISTS` → `(2,1,1)`, 2-record DV; `UPDATE` `IN`/plain → `(1,0,1),(2,1,2),(3,2,1)` next-row-id 4 added 1, 2 data files; `write.delete.granularity` is inert on v3); V3-7 lifted MERGE matched-UPDATE/DELETE/INSERT/NMBS/mixed; RP-6 lifted plain-`WHERE` DELETE and UPDATE. v2 MoR still writes Parquet position deletes. **`V3-DV-1` FIXED (RP-7, 2026-09-02):** the fork repin to `ff4764d3` (fork **F-18**, PR `#260`) makes the shared-Puffin close Spark-equal — only the touched blob is rewritten, the sibling entry keeps its container and `content_offset`, two containers after, `removed-dvs 1` / `added-dvs 1`, and a later single-row DELETE writes 377 B instead of 19,126 B at 64 blobs **V3-11 (2026-09-02):** `V3-ROWID-3` FIXED — one commit's data files reach the manifest in ascending partition-value order, so the MoR MERGE insert reads Spark's `_row_id = 11` in 10 of 10 runs where it flapped 10/11 before; the dated DECLARED residual is **`V3-FILEORDER-1`** — this engine orders one commit's data files by ascending partition value where Spark uses the `java.util.HashMap` bucket index of the partition struct, so the two agree only on collision-free monotonic sets (`{0,1}`, `{0,1,2}`, `{0,1,2,3}`, `bucket(4,·)`) and derived `_row_id` differs on wider sets; replicating a JDK map's iteration order is declared an unmaintainable anti-feature. `F-v3-10-partition-file-order` (partitioned plain `INSERT INTO`, written by the fork's `TaskWriter`) is fork ask **F-20** **V3-12 (2026-09-02):** a touched data file's live file-scoped parquet position deletes are read back, unioned into the new DV and removed in the same `RowDelta` (`V3-UPGRADE-DV-1` FIXED); the plain-`WHERE` arm over one (`V3-UPGRADE-DV-PLAIN-1`) and a delete covering two data files, which Spark merges-without-removing but the fork's commit door cannot express (`V3-UPGRADE-DV-PART-1`, measured), stay dated DECLARED loud refusals with fork TRIGGERs | full DML including UPDATE/MERGE, round-tripped | V3-9, RP-7, V3-11, V3-12 |
| Write: COW DML on an adopted v3 table | ✅ V3-8 (2026-09-02): `V3-COW-1` FIXED — subquery-`WHERE` `DELETE … IN` / `NOT IN` / `EXISTS` / `NOT EXISTS` and `UPDATE … IN` keep stored `_row_id` on created and adopted v3 (`IN` delete `(1,0,1),(3,2,1)` next-row-id 5; `NOT IN` `(2,1,1)` next-row-id 4; `UPDATE` `(1,0,1),(2,1,2),(3,2,1)` next-row-id 6); V3-7 lifted MERGE, RP-6 first/second DELETE and UPDATE; F-rp3-c7 consumed as a two-file-seed artefact and F-v3-8-update-files as the one-vs-two-data-file artefact; owner ruling 2026-08-25 discharged | lineage carried per spec | V3-8 |
| Write/maintain: partitioned v3 | ✅ V3E-3 + RP-3 cells 3–6: partitioned DV DELETE Spark-equal on three doors | compaction proven on partitioned and spec-evolved tables | V3-5 |
| Maintain: `rewrite_data_files` | ✅ RP-4 lineage Spark-equal (`V3-LINEAGE-1` FIXED); V3-5 DV drop (`V3-DANGLE-1` FIXED, `removed_delete_files_count = 6`); F-3 option half taken | lineage through rewrite; strands no DVs; true `removed_delete_files_count` | done (V3-5) |
| Maintain: DV / delete-file maintenance | ✅ by dated DECLARED residual — V3-5: DV compact is `rewrite_data_files`; the residual is `B-MOR-3`, the deliberate refusal of `rewrite_position_delete_files` over live DVs under owner decision OD-2 (ruled 2026-08-21, re-measured 2026-09-02: Spark's own answer there is four silent zeros, so zeros cannot mean already-clean) | DV-aware compact via rewrite_data_files; position-delete rewrite still refuses live DVs | V3-5 done / B-MOR-3 stays |
| Maintain: expiry / orphans on v3 | ✅ V3E-4 (2026-08-25): expire with expirable snapshots (tag-reachable DV snapshot kept, untagged intermediate gone, MW-1 six-column schema); `remove_orphan_files` 24h floor still refuses and leaves a planted orphan. Engine-compared; live Spark triple is V3E-5 | stays + live leg | evidence (intake) |
| Maintain: `rewrite_manifests` | ✅ wired on v2 (MW-6, [#230](https://github.com/TRO-Wolf/repark/pull/230); rows MANIFEST-1/2/3) and **exercised on v3 by SCALE-v3 (2026-09-02)** — merge-on-read leg 59 manifests → 1 (0.3 s, manifest list 10,466 → 1,911 B), copy-on-write leg 10 → 1, inside the full 1e7 x 50 maintenance sequence; the Spark-compared semantics stay MANIFEST-1/2/3, measured on v2 | exercised on v3 | evidence (intake) + SCALE-v3 |
| Refs + time travel on v3 (rollback, branch/tag DDL, `AS OF` over DVs) | ✅ V3E-4 (2026-08-25): BRANCH/TAG on adopted partitioned-DV v3; `VERSION AS OF` / `FOR VERSION AS OF` over DVs matches V3E-3 Spark live set; `rollback_to_snapshot` restores it. Three doors (native DF N/A) | stays + live leg | evidence (intake) |
| Adopt: `register_table` | ✅ wired (#203); Hadoop `vN` writes FIXED RP-3 / F-14 (`V3-ADOPT-1`); S3 Tables is dated R126 (`S3T-1`, F-9) | stays | done |
| Live: Glue + S3 Tables v3 legs | ✅ LIVE-v3-M (2026-09-02): both legs green on `aws-acceptance` [run 33635288918](https://github.com/TRO-Wolf/repark/actions/runs/33635288918) (dispatched on merged `main` `8c4bc55`) — `test_v3_dv_dml_maintenance_against_glue` and `test_v3_dv_dml_maintenance_against_s3tables` over one shared body (opt-in v3 MoR CTAS partitioned by identity `part`, row-scoped `DELETE`, `MERGE`, a `_row_id` / `_last_updated_sequence_number` read, `rewrite_data_files`, `expire_snapshots`, and `register_table` on Glue only; S3 Tables is the dated gap `S3T-1` / R126). **S3 Tables accepts `format-version = 3` at CREATE** — the decision table's accepted branch, with only the service's own commit counts relaxed — and **Glue reproduced the local numbers exactly**; the local pin is `python/repark/tests/test_v3_acceptance_local.py` and the semantics are registry row `S3T-V3-1`. MW-10 is format v2 — it proves the S3 Tables expire permission, not this v3 leg. Evidence: [mw-10-s3tables-mor-ledger.md](../../ledgers/archive/2026-08/2026-08-30-mw-10-s3tables-mor-ledger.md); measured 2026-08-30, [run 33333274383](https://github.com/TRO-Wolf/repark/actions/runs/33333274383) answered PutTableData **allow**. | every green row re-proven live where the service supports it | evidence (intake) (+OD-3b / MW-10) |
| Nightly oracle: v3 leg | ✅ V3E-5 (2026-08-27) wired `v3-spark-part-dv` and `v3-spark-eq-dv` as a live triple `repark == Spark` (PySpark 4.1.2 + Iceberg 1.11.0); the CI leg was red from its first run (2026-08-28 → 2026-09-01, unqualified `CALL system.register_table`) until #300 fixed it; first green nightly 2026-09-02 (run 33575586119) | a v3 fixture leg in the nightly, green | evidence (intake) |
| Scale | ✅ SCALE-v3 (2026-09-02): the MW-7 `1e7 x 50` workload re-measured on format-v3 tables at the same knobs (8 identity partitions, 2 % touch, checkpoints every 10, 7 reps, 4 MiB target), both legs. **Counts, which no clock touches:** the merge-on-read leg holds **96 delete files against v2's 400 (0.24x)** — one Puffin deletion vector per data file, rewritten in place, so the count stops at the seeded data-file count — and **496 data files against 1,696 (0.29x)**; the full maintenance sequence ends at **zero delete files and zero delete records** where v2 kept 8 files and 10,000,000 records, and `rewrite_data_files` reclaims all 96 alone (`removed_delete_files_count` 96) after `rewrite_position_delete_files` refuses on live DVs (`B-MOR-3`). **Read side, controlled:** the point probe reads at **0.64x** v2 (2,493 ms against 3,878 ms) on a cell where the copy-on-write control moved **1.00x** between the two runs, and after the cycle the table sits at **0.61x** that control where v2 sat at 2.02x — MW-7's F-MW7-1 residue is a v2 statement. **Write side, cross-run and uncontrolled (COW control 1.22x at identical knobs):** merges ran at 1.59x v2's mean and the run took 2:42:36 at peak RSS 4,792 MiB, on a different day and a box whose quiet was sampled only up to the start — a direction, not a coefficient. Evidence: [scale-v3-mw7-ledger.md](../../ledgers/archive/2026-09/2026-09-02-scale-v3-mw7-ledger.md) §3, whose tables carry every published number; the driver knob is `run_mw7.py --format-version 3` | the same measurement on a v3 table | SCALE-v3 |

### 3.1 The gate audit — every §3 row, 2026-09-03 (V1-GATE)

One row per §3 row, in §3's order. **Glyph** is that row's state today; **residual** names the
registry row carrying what that row's §3 **v1.0 requires** cell does not yet get, its class and
its date; **pin** is where the claim is held. The audit is scoped to those requires cells: a
residual inside one whose class is BACKLOG blocks the gate, and none is. Residuals that sit on a
surface §3 names but outside its requires cell are listed under the table — recorded, not
gating.

| # · §3 row | Glyph | Claim held today | Residual → registry row | Class · date | Pin |
|---|---|---|---|---|---|
| 1 · Read: data + deletion vectors | ✅ | DV reads Spark-exact, unpartitioned and partitioned (identity `part`) | — | — | `crates/repark-spark/src/tests/v3e3.rs`, `python/repark/tests/test_v3e3_fixtures.py` |
| 2 · Read: equality deletes + delete-file metadata | ✅ | Puffin DV beside an equality-delete file; `.delete_files` / `$delete_files` content 1 and 2 | — | — | `crates/repark-spark/src/tests/v3e3.rs`, `python/repark/tests/test_v3e3_fixtures.py` |
| 3 · Read: `_row_id` / `_last_updated_sequence_number` | ✅ | Spark-equal on single-table v3 reads, three doors (`V3-ROWID-1` FIXED) | joins / CTEs / subqueries / time travel refuse loud → `V3-ROWID-2` | DECLARED · 2026-08-31 (V3-4) — a registry **queue** entry under §7 "Surfaced, awaiting pins", not a §7 row; it carries pins and a date | `crates/repark-spark/src/tests/v3_lineage.rs`, `python/repark/tests/test_v3_lineage_columns.py` |
| 4 · Read/write: v3 types + default values | ✅ | `timestamp_ns` / `timestamptz_ns` CREATE, `write_default` / `initial_default`, `unknown` and binary `variant` refuse Spark-equal (V3-6) | `geometry` / `geography` → `V3-GEO-1`; shredded-Parquet `variant` → `V3-VARIANT-SHRED-1` | DECLARED · owner 2026-08-25 | `crates/repark-spark/src/tests/create_table.rs::v3_type_columns_geometry_geography_variant_refuse_naming_the_type`, `crates/repark-spark/src/tests/v3_types.rs` |
| 5 · Table encryption keys | ✅ | a stored `encryption.key-id` never changes a scan; nothing is applied | the whole feature → `ENC-1` | DECLARED exclusion · owner 2026-08-24 | `crates/repark-spark/src/tests/v3_cow.rs::v3_create_with_encryption_key_id_still_scans_without_a_kms` |
| 6 · Write: create v3 | ✅ | opt-in CREATE / CTAS behind `repark.sql.allowCreateFormatVersion3`; default stays v2 | — | — | `python/repark/tests/test_v3_create_opt_in.py`, `crates/repark-spark/src/tests/create_table.rs` |
| 7 · Upgrade: v2 → v3 in place | ✅ | ALTER upgrades metadata-only on three doors (`V3-UPGRADE-1`); a legacy position delete merges into the DV (`V3-UPGRADE-DV-1` FIXED) | `format-version = '4'` refuses where Spark takes it → `V3-UPGRADE-V4-1` | DECLARED · 2026-09-02 (V3-10) | `crates/repark-spark/src/tests/v3_upgrade.rs`, `python/repark/tests/test_v3_upgrade.py` |
| 8 · Write: append incl. row lineage | ✅ | lineage carried through a cross-engine round trip | — | — | `crates/repark-spark/src/tests/v3_lineage.rs` |
| 9 · Write: MoR DML via deletion vectors | ✅ | predicate, MERGE and subquery DML write file-scoped Puffin DVs on three doors; the container close is Spark-equal; same-commit file order is deterministic (`V3-MOR-1`, `V3-DV-1`, `V3-ROWID-3` FIXED) | file order vs Spark's hash-bucket order → `V3-FILEORDER-1`; plain-`WHERE` over a legacy delete → `V3-UPGRADE-DV-PLAIN-1`; a delete covering two data files → `V3-UPGRADE-DV-PART-1` | DECLARED · 2026-09-02 (V3-11, V3-12) | `crates/repark-spark/src/tests/v3_mor_dml.rs`, `v3_row_order.rs`, `v3_legacy_delete.rs` |
| 10 · Write: COW DML on an adopted v3 table | ✅ | every served copy-on-write shape keeps stored `_row_id` (`V3-COW-1` FIXED) | — | — | `crates/repark-spark/src/tests/v3_subquery_dml.rs`, `python/repark/tests/test_v3_cow_dml.py` |
| 11 · Write/maintain: partitioned v3 | ✅ | partitioned DV DELETE Spark-equal on three doors | — | — | `crates/repark-spark/src/tests/v3e3.rs` |
| 12 · Maintain: `rewrite_data_files` | ✅ | lineage preserved (`V3-LINEAGE-1`) and scoped DVs dropped with a true `removed_delete_files_count` (`V3-DANGLE-1`) | — | — | `crates/repark-spark/src/tests/call_v3.rs`, `crates/repark-spark/src/tests/call_v3_dv.rs` |
| 13 · Maintain: DV / delete-file maintenance | ✅ | DV compaction lands through `rewrite_data_files`; `rewrite_position_delete_files` refuses live DVs rather than answering Spark's four silent zeros | that refusal → `B-MOR-3` | DELIBERATE **by analogy** to OD-2 (applied in the MW-2 ledger, 2026-08-21; OD-2 of record is the orphan-files posture); registry §7, **no DECLARED class marker on the row**; re-measured 2026-09-02; **owner line pending** | `crates/repark-spark/src/tests/call_v3_dv.rs::call_rewrite_position_delete_files_still_refuses_engine_written_v3_dvs`, `python/repark/tests/test_v3_dv_compaction.py` |
| 14 · Maintain: expiry / orphans on v3 | ✅ | expire keeps the tag-reachable DV snapshot and drops the untagged one; the orphan 24 h floor refuses | — | — | `crates/repark-spark/src/tests/v3e4.rs` |
| 15 · Maintain: `rewrite_manifests` | ✅ | exercised on v3 by SCALE-v3 (2026-09-02): merge-on-read 59 → 1, copy-on-write 10 → 1 inside the 1e7 x 50 sequence; Spark-compared semantics are MANIFEST-1/2/3, measured on v2 | — | — | `crates/repark-spark/src/tests/call_manifests.rs`; the v3 exercise is [scale-v3-mw7-ledger.md](../../ledgers/archive/2026-09/2026-09-02-scale-v3-mw7-ledger.md) §3.4 |
| 16 · Refs + time travel on v3 | ✅ | BRANCH / TAG DDL, `VERSION AS OF` over DVs, `rollback_to_snapshot` | — | — | `crates/repark-spark/src/tests/v3e4.rs`, `python/repark/tests/test_v3e4_refs_time_travel.py` |
| 17 · Adopt: `register_table` | ✅ | Hadoop `vN.metadata.json` adopts, reads and writes `v(N+1)` (`V3-ADOPT-1` FIXED) | S3 Tables has no `registerTable` → `S3T-1` (fork R126 (c)) | DECLARED service gap · **undated on `S3T-1`**; dated 2026-08-27 on fork R126 (c) | `crates/repark-spark/src/tests/call_register.rs` |
| 18 · Live: Glue + S3 Tables v3 legs | ✅ | both legs green; Glue reproduced the local numbers exactly and S3 Tables accepts `format-version = 3` at CREATE (`S3T-V3-1` FIXED); re-run 2026-09-03 carried V3-11's exact `_row_id = 11` assertion | adopt on S3 Tables is row 17's `S3T-1` | — | `python/repark/tests/test_v3_acceptance_local.py`, `python/repark/tests/test_acceptance_v3_helpers.py` |
| 19 · Nightly oracle: v3 leg | ✅ | `v3-spark-part-dv` and `v3-spark-eq-dv` run as live `repark == Spark` triples; first green nightly 2026-09-02 | — | — | `python/repark/tests/test_v3_live_oracle.py` |
| 20 · Scale | ✅ | 1e7 x 50 re-measured on v3: 96 delete files against v2's 400, 496 data files against 1,696, and the maintenance sequence ends at zero delete files and zero delete records | — | — | [scale-v3-mw7-ledger.md](../../ledgers/archive/2026-09/2026-09-02-scale-v3-mw7-ledger.md) §3; driver `python/repark-parity/bench/mw7/run_mw7.py --format-version 3` |

**One v1.0 requirement has no §3 row (V1-GATE, 2026-09-03).** §2 pillar 4 asks for a *full
statement-coverage comparison against PySpark on v3 tables*. The matrix has never carried a row
for it, and no unit discharges it: the nightly v3 leg (V3E-5, green since 2026-09-02) is ten
cells over two fixtures — partitioned-DV reads, equality delete beside a DV, `delete_files`
kinds, UPDATE, matched-UPDATE MERGE on both write modes, and the two subquery-`WHERE` shapes —
a targeted live triple, not the statement matrix; SEM-1 is a function-semantics unit and touches
no v3 statement; and the tree carries no statement-coverage harness at any format version.
Recorded as owed, not claimed.

**Surface residuals outside the requires cells — recorded, not gating.** Each sits on a
surface §3 names; none is inside that row's v1.0 requires cell, so none is audited above.

| Row | Residual | Class | Why it is outside the requires cell |
|---|---|---|---|
| 12 · `rewrite_data_files` | `RDF-1` — a position-delete file spanning two or more data files is still not selected (F-16 residue 2, fork work); the row is FIXED 2026-09-02 for the single-referent shape and its dated prior states read "stays BACKLOG" | open residue on a FIXED row, fork-owned | the cell asks for lineage through rewrite, no stranded DVs and a true `removed_delete_files_count` — all three measured on v3 (`V3-LINEAGE-1`, `V3-DANGLE-1`) |
| 14 · expiry / orphans | `ORPHAN-1` (`older_than` required) and `ORPHAN-2` (dry-run default with Spark's result shape) | DECLARED, owner decision OD-2 (ruled 2026-08-21) | the cell asks that v3 expiry stays green with a live leg; both rows are deliberate strictness on every format version, not a v3 gap |
| 15 · `rewrite_manifests` | `MANIFEST-1` (data manifests only; Spark rewrites delete manifests too) and `MANIFEST-3` (`added_manifests_count` above the target size) | BACKLOG, both v2-measured; `MANIFEST-1` is fork work | the cell asks only that the procedure be exercised on v3, which SCALE-v3 did |

**Fork side, at the consumed pin `ff4764d3`.** Every 🟡 `GAP_MATRIX.md` row this gate leans on
carries a dated cell and a pin at that rev, so none is a blocker.

| Fork row | Glyph | What this gate leans on it for | Dated at the pin rev |
|---|---|---|---|
| R88 · V3 types: variant | 🟡 | binary `variant` refuses end to end; shredded Parquet is out of the fork's parity envelope | scope correction 2026-08-24, caveat 2026-08-25 (`V3-VARIANT-SHRED-1`) |
| R91 · V3 types: unknown | 🟡 | `unknown` CREATE and parquet write refuse loud | parquet-write refusal 2026-09-01, with its three fork pins named |
| R114 · Writer: deletion-vector (v3 Puffin DV) | 🟡 | the DV writer behind rows 9, 11 and 13 | F-18 container close 2026-09-02; PR-7 re-audit 2026-09-02 keeps residue U4 named |
| R126 · Catalogs (clause (c)) | 🟡 | S3 Tables `register_table` is a service gap, not a defect | F-9, 2026-08-27, "not a *yet*" |
| R167 · Hadoop `vN.metadata.json` names | 🟡 | Hadoop pointer adopt + write behind row 17 | 2026-08-28, engine pin named in the cell |

Two ❌ fork rows are the mirror of engine DECLARED rows, not blockers: R89 (geometry / geography,
DECLARED 2026-08-25) and R130 (encryption, DECLARED 2026-08-24). R136
(`RewritePositionDeleteFiles`) is ✅ at the pin rev, re-audited 2026-09-02.

**The gate.** v1.0 tags when every row above is ✅ or its residual is a dated DECLARED row —
registry ([docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)) on the
engine side, `GAP_MATRIX.md` on the fork side — each with a pin. An API review rides the same
gate (what hardens at v1.0 is the owner's call at that review). API review answered 2026-09-02
([packet](../../../docs/design/v1-0-api-review-2026-09-02.md), `R0 yes` at every
recommendation) and the freeze is registered — 888 names in
[v1-0-api-freeze.json](../../../docs/design/v1-0-api-freeze.json) behind
`python/repark-parity/tests/test_api_freeze.py`, versioning policy in
[docs/release.md](../../../docs/release.md) — so no gate item remains on the review.

**Audit result (V1-GATE, 2026-09-03).** §3.1 audits all twenty rows and the fork rows they
lean on: every row ✅ or dated DECLARED as of 2026-09-03. Two things are still owed and neither
is a §3 row. **Owner:** a one-line ruling confirming `B-MOR-3`'s DECLARED class for
`rewrite_position_delete_files` (§3.1 row 13 — the row is housed under the registry's §7 with no
class marker, and its OD-2 attachment is by analogy), and then the v1.0 tag. **Engineering:**
§2 pillar 4's **full v3 statement-coverage comparison against PySpark**, which V1-GATE searched
for and could not pin to any unit, run or fixture — it has no §3 row, so it is named here as the
remaining v1.0 item and queued on
[briefs/next-sequence.md](../../../briefs/next-sequence.md) as **V3-COV**.

## 4. The path, as two lanes

The delivery sequence lives in [docs/design/format-v3-track.md](../../../docs/design/format-v3-track.md)
§5. The critical path is: land the narrowed, guarded RP-2 increment → repair shared-Puffin DV
sibling closure in fork F-17 → take one fresh immutable RP-3 repin → V3-3 → V3-4 and V3-5 →
the production gate. V3-6 may run beside V3-3 or V3-4 once its fork type support is pinned.

**Engine lane.** RP-2 kept the DV-free first DELETE. RP-3 (2026-08-30) consumed F-17 and
measured the DV matrix: live-DV DELETE merge is green on three doors. RP-4 (2026-08-31) consumed
F-7 slice 1: `rewrite_data_files` lineage is Spark-equal (`V3-LINEAGE-1` FIXED). RP-6
(2026-09-01) lifted plain-`WHERE` UPDATE and sequential DELETE — F-rp3-c7 is a layout artefact,
not a defect. V3-7 (2026-09-02) lifted MERGE and V3-8 the subquery-`WHERE` COW rewrite
(`V3-COW-1` FIXED); the MoR subquery-`WHERE` cell is V3-9's.

**Fork lane.** F-17 — path-keyed removal of one blob from a shared Puffin must carry every
still-live sibling blob — landed as fork #237 on 2026-08-28, F-14 (Hadoop pointer writes) as
#235 the same day; details in the [fork handoff](../mid-term/iceberg-rust-handoff-2026-08-23.md).
F-15 provides the V3-6 type/default substrate. The engine consumes the landed batch as RP-3 at
the frozen fork SHA `d408da42` and never targets moving fork `main`; the closure is a call the
engine's own MOR path must make, so RP-3 wires it before it measures.

## 5. Owner actions and open dependencies

- **OD-3b** — **ruled 2026-08-25: the S3 Tables live legs are in v1.0.** The acceptance role
  needs table-data write + delete on the scratch namespace; the scoped statement lives in
  [docs/tier2-aws.md](../../../docs/tier2-aws.md) §2 (owner-executed IAM). Whether
  `DeleteObject` on table storage is authorized by `s3tables:PutTableData` is MW-10's first
  clause, measured on **format v2** (it proves the permission, not the v3 leg). Evidence:
  [mw-10-s3tables-mor-ledger.md](../../ledgers/archive/2026-08/2026-08-30-mw-10-s3tables-mor-ledger.md). A denial
  is a stop, not a design. **The IAM was applied on 2026-08-28.**
  Measured result (2026-08-30, first owner dispatch after the MW-10 merge — [run 33333274383](https://github.com/TRO-Wolf/repark/actions/runs/33333274383), green): **allow** — `expire_snapshots` removed the expired snapshot's files from table storage under the applied policy; no widen needed.
- **Sequencing vs the other campaigns.** This ruling makes v3 the spine to v1.0; FNP, perf,
  dbt, and the correctness backlog may use fork-wait windows as separately chartered units.
  They do not consume F-17 and do not gate the tag unless ruled into §3. A ready v3 unit takes
  priority over side-lane work.
- **AWS v3 support, as AWS documents it (checked 2026-08-23).** Glue (catalog, REST,
  maintenance) and S3 Tables (REST, maintenance) state v3 support, EMR ≥ 7.12 (AWS
  announcement 2025-11-26; the "Apache Iceberg on AWS" prescriptive-guidance table-spec-v3
  page). **Athena does not support v3 at all** — an input to any customer cutover story.
  Variant on AWS is S3 Tables-only and region-limited (announced 2026-07-28). Whether S3
  Tables' automatic compaction is deletion-vector-aware is not documented either way; the
  first evidence unit measures it before any v3 table lands there.
- **The v3 maintenance oracle is decided (V3E-2, 2026-08-24).** Live on this machine,
  **PySpark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0`** (zulu-17) **executes** v3
  `rewrite_data_files` and `expire_snapshots` — the `DataSourceV2Relation` break that
  blocked 4.1.2+1.10.0 (registry MOR-1) does **not** reproduce on 1.11.0. That pair is
  the v3 maintenance oracle and matches the nightly pin. CI constant
  `V3_MAINTENANCE_ORACLE`; verbatim transcript in the V3E-1/2 ledger. V3-0's
  PySpark 4.0.1 + Iceberg 1.10.0 remains a known-working control, not the named oracle.

## 6. What this charter does not decide

Unit scoping, estimates, and order within the lanes — each unit charters and measures per the
process contract before it builds. Shredded-Parquet `variant` is out of the v1.0 gate by
ruling (2026-08-25; §3's types row); binary variant is not. Incremental/changelog reads (the
row-lineage *consumer* surface — `create_changelog_view`, snapshot-range reads) are **out of the v1.0 gate by this
ruling (2026-08-23)**: lineage is preserved so other engines' consumers stay correct; serving
those reads ourselves is a post-v1.0 track unless the owner pulls it in at intake. Capability
status stays single-homed: the registry and the fork's `GAP_MATRIX.md` say what works;
[STATUS.md](../../../STATUS.md) says what is delivered; this file only defines the destination
and the gate.
