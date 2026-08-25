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
| Read: `_row_id` / `_last_updated_sequence_number` | ❌ not plannable (registry V3-ROWID-1) | served as columns, Spark-equal | V3-4 |
| Read/write: v3 types + default values | ❌ absent — no engine surface reaches one. **Ruled 2026-08-25:** `geometry` / `geography` DECLARED out (registry `V3-GEO-1`); shredded-Parquet `variant` DECLARED out (queued `V3-VARIANT-SHRED-1`); binary variant, `timestamp_ns`, `unknown` and default values stay V3-6 | per-feature support or DECLARED | V3-6 (+H6/H7) ← fork F-15 |
| Table encryption keys (v3, optional) | ❌ absent — **owner ruled 2026-08-24: DECLARED exclusion**; registry `ENC-1` (V3E-1) | the dated DECLARED registry row | ruled |
| Write: create v3 | ✅ opt-in CREATE/CTAS (`repark.sql.allowCreateFormatVersion3`, default false; V3-2) | stays opt-in until V3-3; default remains v2 | V3-2 |
| Upgrade: v2 → v3 in place (`ALTER … SET TBLPROPERTIES`, both doors) | 🚫 refuses, pinned (V3-2 kept ALTER refused; C-008) | **owner ruling 2026-08-25: build it, behind `repark.sql.allowCreateFormatVersion3`, after V3-3** | V3-3+ |
| Write: append incl. row lineage | ✅ Spark-verified (format-v3-track §2) | stays green + live leg | evidence (intake) |
| Write: MOR DML via deletion vectors | 🚫 refuses (the R113 guard) | full DML, DV-writing, round-tripped | V3-3 ← fork F-13 |
| Write: COW DML on an adopted v3 table | 🚫 refuses (V3R-1 — **owner ruling 2026-08-25: guard**; registry `V3-COW-1`). V3E-1 measured `next_row_id` reassigning on DELETE / UPDATE / MERGE; with MOR refused too, a v3 table is append-only here | lineage carried per spec | V3-4 ← fork F-7 |
| Write/maintain: partitioned v3 | ❌ unmeasured (format-v3-track §7 — every fixture unpartitioned) | DV writes + compaction proven on partitioned and spec-evolved tables | V3-3 / V3-5 |
| Maintain: `rewrite_data_files` | 🚫 V3-LINEAGE-1 guard | lineage through rewrite; strands no DVs (V3-DANGLE-1); true `removed_delete_files_count` | V3-5 ← fork F-7 |
| Maintain: DV / delete-file maintenance | 🚫 B-MOR-3 refusal | DV-aware answer, Spark-compared | V3-5 ← fork F-7 |
| Maintain: expiry / orphans on v3 | ⚠ never exercised with real work (format-v3-track §7) | exercised + Spark-compared | V3-5 |
| Maintain: `rewrite_manifests` | ✅ wired on v2 (MW-6, [#230](https://github.com/TRO-Wolf/repark/pull/230); rows MANIFEST-1/2/3) | exercised on v3 | evidence (intake) |
| Refs + time travel on v3 (rollback, branch/tag DDL, `AS OF` over DVs) | ⚠ never exercised | exercised + Spark-compared | evidence (intake) |
| Adopt: `register_table` | ✅ wired (#203) | stays; Hadoop-pointer writes → fork F-14; S3 Tables → fork F-9 | done + residues |
| Live: Glue + S3 Tables v3 legs | ❌ nothing measured live (format-v3-track §7) | every green row re-proven live where the service supports it | evidence (intake) (+OD-3b) |
| Nightly oracle: v3 leg | ❌ none | a v3 fixture leg in the nightly, green | evidence (intake) |
| Scale | ⚠ v2 measured at 1e7×50 (MW-7 — driver + census exist; ratios recorded) | the same measurement on a v3 table | evidence (intake) |

**The gate.** v1.0 tags when every row above is ✅ or its residual is a dated DECLARED row —
registry ([docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)) on the
engine side, `GAP_MATRIX.md` on the fork side — each with a pin. An API review rides the same
gate (what hardens at v1.0 is the owner's call at that review).

## 4. The path, as two lanes

**Engine lane.** The unit slate already exists and is not restated here:
[docs/design/format-v3-track.md](../../../docs/design/format-v3-track.md) §5, plus the
cross-cutting evidence obligations in §3's matrix (unitized by the v3 intake). The design's
sequencing constraint — V3-2+ waits for MW to close — was met on 2026-08-23 (MW-5, #224):
**the lane is unblocked.** V3-3 (deletion-vector writes) is the largest engine unit; VARIANT
(H6, design ratified) is the largest single type item.

**Fork lane.** The hardest prerequisites are fork work, queued in the handoff:
[../mid-term/iceberg-rust-handoff-2026-08-23.md](../mid-term/iceberg-rust-handoff-2026-08-23.md)
F-7 (lineage through every row-rewrite — compaction and the COW DML path — plus DV-aware
maintenance and dangling-DV removal) and — added with this charter — F-13 (Puffin DV write
path), F-14 (`MetadataLocation` Hadoop pointer math), F-15 (v3 type system + default values).
The engine consumes each by rev-pin repin unit (handoff §5); the lanes run in parallel.

## 5. Owner actions and open dependencies

- **OD-3b** — **ruled 2026-08-25: the S3 Tables live legs are in v1.0.** The acceptance role
  needs table-data write + delete on the scratch namespace; the scoped statement lives in
  [docs/tier2-aws.md](../../../docs/tier2-aws.md) §2 (owner-executed IAM). Whether
  `DeleteObject` on table storage is authorized by `s3tables:PutTableData` is unverified —
  the first S3 Tables `expire_snapshots` measurement decides, and a denial is a stop, not a
  design.
- **Sequencing vs the other campaigns.** This ruling makes v3 the spine to v1.0; FNP, perf,
  dbt, and the correctness backlog interleave as owner-chartered units, they do not gate the
  tag unless ruled into §3.
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
