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
| Read/write: v3 types + default values | ⚠ V3-6 (2026-09-01): opt-in v3 CREATE takes `timestamp_ns` / `timestamptz_ns` (v2 refuses); append fills an omitted column from a schema-carried `write_default`, `initial_default` reads into pre-column files, DEFAULT DDL refuses Spark-equal; `unknown` CREATE refuse and parquet write refuse pinned (R91, RP-5). **Ruled 2026-08-25:** `geometry` / `geography` DECLARED out (registry `V3-GEO-1`); shredded-Parquet `variant` DECLARED out and binary `variant` measured refusing end to end (`V3-VARIANT-SHRED-1`, fork R88) — the residual is that dated DECLARED row | per-feature support or DECLARED | V3-6 (fork F-15) |
| Table encryption keys (v3, optional) | ❌ absent — **owner ruled 2026-08-24: DECLARED exclusion**; registry `ENC-1` (V3E-1) | the dated DECLARED registry row | ruled |
| Write: create v3 | ✅ opt-in CREATE/CTAS (`repark.sql.allowCreateFormatVersion3`, default false; V3-2) | stays opt-in until V3-3; default remains v2 | V3-2 |
| Upgrade: v2 → v3 in place (`ALTER … SET TBLPROPERTIES`, both doors) | 🚫 refuses, pinned (V3-2 kept ALTER refused; C-008) | **owner ruling 2026-08-25: build it, behind `repark.sql.allowCreateFormatVersion3`, after V3-3** | V3-3+ |
| Write: append incl. row lineage | ✅ Spark-verified (format-v3-track §2) | stays green + live leg | evidence (intake) |
| Write: MOR DML via deletion vectors | ⚠ V3-9 (2026-09-02): `V3-MOR-1` FIXED — predicate DML's V2-only delete-file gate is lifted, so `DELETE … WHERE` / `UPDATE … WHERE` on v3 write one file-scoped Puffin DV per touched data file on all three doors, created and adopted (`DELETE` `IN`/`EXISTS`/plain → `(1,0,1),(3,2,1)` next-row-id 3 added 0, 1-record DV; `NOT IN`/`NOT EXISTS` → `(2,1,1)`, 2-record DV; `UPDATE` `IN`/plain → `(1,0,1),(2,1,2),(3,2,1)` next-row-id 4 added 1, 2 data files; `write.delete.granularity` is inert on v3); V3-7 lifted MERGE matched-UPDATE/DELETE/INSERT/NMBS/mixed; RP-6 lifted plain-`WHERE` DELETE and UPDATE. v2 MoR still writes Parquet position deletes. **Residual `V3-DV-1` (BACKLOG, 2026-09-02):** closing a shared Puffin rewrites every sibling blob into one new container where Spark rewrites only the touched blob and leaves the sibling entry at its old container and offset (`removed-dvs 1` / `added-dvs 1`, two containers after) — rows, lineage, `referenced_data_file` and record counts agree; owner is fork ask **F-18** consumed by repin **RP-7** | full DML including UPDATE/MERGE, round-tripped | V3-9 |
| Write: COW DML on an adopted v3 table | ✅ V3-8 (2026-09-02): `V3-COW-1` FIXED — subquery-`WHERE` `DELETE … IN` / `NOT IN` / `EXISTS` / `NOT EXISTS` and `UPDATE … IN` keep stored `_row_id` on created and adopted v3 (`IN` delete `(1,0,1),(3,2,1)` next-row-id 5; `NOT IN` `(2,1,1)` next-row-id 4; `UPDATE` `(1,0,1),(2,1,2),(3,2,1)` next-row-id 6); V3-7 lifted MERGE, RP-6 first/second DELETE and UPDATE; F-rp3-c7 consumed as a two-file-seed artefact and F-v3-8-update-files as the one-vs-two-data-file artefact; owner ruling 2026-08-25 discharged | lineage carried per spec | V3-8 |
| Write/maintain: partitioned v3 | ✅ V3E-3 + RP-3 cells 3–6: partitioned DV DELETE Spark-equal on three doors | compaction proven on partitioned and spec-evolved tables | V3-5 |
| Maintain: `rewrite_data_files` | ✅ RP-4 lineage Spark-equal (`V3-LINEAGE-1` FIXED); V3-5 DV drop (`V3-DANGLE-1` FIXED, `removed_delete_files_count = 6`); F-3 option half taken | lineage through rewrite; strands no DVs; true `removed_delete_files_count` | done (V3-5) |
| Maintain: DV / delete-file maintenance | ⚠ V3-5: DV compact is `rewrite_data_files`; 🚫 `B-MOR-3` stays (R136 zeros on DV-only; CALL refuses so zeros cannot mean already-clean) | DV-aware compact via rewrite_data_files; position-delete rewrite still refuses live DVs | V3-5 done / B-MOR-3 stays |
| Maintain: expiry / orphans on v3 | ✅ V3E-4 (2026-08-25): expire with expirable snapshots (tag-reachable DV snapshot kept, untagged intermediate gone, MW-1 six-column schema); `remove_orphan_files` 24h floor still refuses and leaves a planted orphan. Engine-compared; live Spark triple is V3E-5 | stays + live leg | evidence (intake) |
| Maintain: `rewrite_manifests` | ✅ wired on v2 (MW-6, [#230](https://github.com/TRO-Wolf/repark/pull/230); rows MANIFEST-1/2/3) | exercised on v3 | evidence (intake) |
| Refs + time travel on v3 (rollback, branch/tag DDL, `AS OF` over DVs) | ✅ V3E-4 (2026-08-25): BRANCH/TAG on adopted partitioned-DV v3; `VERSION AS OF` / `FOR VERSION AS OF` over DVs matches V3E-3 Spark live set; `rollback_to_snapshot` restores it. Three doors (native DF N/A) | stays + live leg | evidence (intake) |
| Adopt: `register_table` | ✅ wired (#203); Hadoop `vN` writes FIXED RP-3 / F-14 (`V3-ADOPT-1`); S3 Tables is dated R126 (`S3T-1`, F-9) | stays | done |
| Live: Glue + S3 Tables v3 legs | ✅ LIVE-v3-M (2026-09-02): both legs green on `aws-acceptance` [run 33635288918](https://github.com/TRO-Wolf/repark/actions/runs/33635288918) (dispatched on merged `main` `8c4bc55`) — `test_v3_dv_dml_maintenance_against_glue` and `test_v3_dv_dml_maintenance_against_s3tables` over one shared body (opt-in v3 MoR CTAS partitioned by identity `part`, row-scoped `DELETE`, `MERGE`, a `_row_id` / `_last_updated_sequence_number` read, `rewrite_data_files`, `expire_snapshots`, and `register_table` on Glue only; S3 Tables is the dated gap `S3T-1` / R126). **S3 Tables accepts `format-version = 3` at CREATE** — the decision table's accepted branch, with only the service's own commit counts relaxed — and **Glue reproduced the local numbers exactly**; the local pin is `python/repark/tests/test_v3_acceptance_local.py` and the semantics are registry row `S3T-V3-1`. MW-10 is format v2 — it proves the S3 Tables expire permission, not this v3 leg. Evidence: [mw-10-s3tables-mor-ledger.md](../../ledgers/archive/2026-08/2026-08-30-mw-10-s3tables-mor-ledger.md); measured 2026-08-30, [run 33333274383](https://github.com/TRO-Wolf/repark/actions/runs/33333274383) answered PutTableData **allow**. | every green row re-proven live where the service supports it | evidence (intake) (+OD-3b / MW-10) |
| Nightly oracle: v3 leg | ✅ V3E-5 (2026-08-27) wired `v3-spark-part-dv` and `v3-spark-eq-dv` as a live triple `repark == Spark` (PySpark 4.1.2 + Iceberg 1.11.0); the CI leg was red from its first run (2026-08-28 → 2026-09-01, unqualified `CALL system.register_table`) until #300 fixed it; first green nightly 2026-09-02 (run 33575586119) | a v3 fixture leg in the nightly, green | evidence (intake) |
| Scale | ⚠ v2 measured at 1e7×50 (MW-7 — driver + census exist; ratios recorded) | the same measurement on a v3 table | evidence (intake) |

**The gate.** v1.0 tags when every row above is ✅ or its residual is a dated DECLARED row —
registry ([docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)) on the
engine side, `GAP_MATRIX.md` on the fork side — each with a pin. An API review rides the same
gate (what hardens at v1.0 is the owner's call at that review).

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
