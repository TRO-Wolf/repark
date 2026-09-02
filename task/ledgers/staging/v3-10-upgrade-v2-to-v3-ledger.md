# Charter ledger — V3-10 · in-place v2 → v3 upgrade behind the create opt-in

**Date:** 2026-09-02 · **Branch:** `feat/v3-10-upgrade-v2-to-v3` · **Base:** `origin/main`
`ca9c007` · **Model:** claude-opus-5 (medium) · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) · **Path:** STANDARD.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The owner ruled on 2026-08-25: build the upgrade, behind
`repark.sql.allowCreateFormatVersion3`, after V3-3. V3-3 … V3-8 landed, so the engine mutates an
adopted v3 table Spark-equal and the precondition holds.

**Not in this unit:** merge-on-read *predicate* DML on v3 (V3-9 owns that gate); fork repin
(stays `fb0cacfa`); `.github/`; dependency files; format v4.

## PROPOSITION LEDGER — V3-10 — 2026-09-02

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Before the change, `ALTER TABLE … SET TBLPROPERTIES ('format-version' = '3')` refuses on all three doors with and without the opt-in, and the ANSI door's `format_version` key refuses with a TRIGGER note. | Red transcript at base `ca9c007`. | **PROVEN** | Base refusal was the fork's reserved-property error `DataInvalid => Table properties should not contain reserved properties, but got: [format-version]` (Spark door + facade, opt-in on and off); ANSI door refused with "`format_version` cannot be changed after creation … TRIGGER … a fork `UpgradeFormatVersion` action reachable through repark-iceberg". The two standing pins were `create_table.rs::or_replace_applies_requested_v3_and_alter_still_refuses_with_opt_in` and `alter/tests.rs::reserved_and_unchangeable_keys_refuse_loud`. |
| C-002 | Measure the live oracle (PySpark 4.1.2 + Iceberg 1.11.0, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, Hadoop catalog, `coalesce(1)` single-file layout) on the upgrade itself, downgrade, same-version, `'1'`/`'4'`/`'x'`/`''`, combined keys, MoR DML over a legacy parquet position delete, COW DML, and `rewrite_data_files` after the upgrade — recording format version, snapshot count, `next-row-id`, sequence numbers, lineage triples, delete-file kind and metadata-file counts. | Oracle transcript tables. | **PROVEN** | Transcript below (§ Oracle). Thirteen cells. |
| C-003 | With `repark.sql.allowCreateFormatVersion3 = true` the ALTER upgrades in place through the fork's `UpgradeFormatVersionAction` (no local metadata surgery) on the Spark door, the ANSI door and the facade, at the C-002 values; without the opt-in it refuses naming the conf; downgrade / unsupported versions refuse naming the key and both versions; after the upgrade append lineage, COW DML, MoR MERGE DV DML, `rewrite_data_files` and `register_table` are Spark-equal. | Three-door pins carrying absolute values; mutation N red of M. | **PROVEN** | Engine table below. Twelve Spark-door cells, three ANSI cells, three facade cells, one live triple, six resolver unit cells. Mutation (drop the upgrade action): 9 red of 12 Spark-door, 2 of 3 ANSI, 3 of 4 facade+live. Mutation (drop the opt-in check): the without-opt-in twin reds on each of the four surfaces (1 of 12, 1 of 3, 1 of 3, 1 of 6). Citation: `crates/repark-spark/src/tests/map.md`. |
| C-004 | Row-per-entry-point: every measured cell is pinned on the door it is reachable from, the incidental controls hold (CREATE opt-in unchanged, v2 stays the default, ALTER of another key alone unchanged, `OR REPLACE` keep-v3 green), and the live cell runs under `REPARK_PARITY_LIVE=1`. | `docs/testing.md` row-per-entry-point; live run. | **PROVEN** | `v3_upgrade.rs` (12), `v3/create.rs::alter_set_properties_*` (3), `test_v3_upgrade.py` (3), `test_v3_live_oracle.py::test_v3_upgrade_v2_to_v3_live_matches_spark` (live triple, green under `REPARK_PARITY_LIVE=1`), `format_version.rs` unit cells (6). Controls: `or_replace_applies_requested_v3_and_alter_upgrades_with_opt_in` (flipped, keep-v3 half untouched), `alter_of_another_key_alone_still_leaves_the_version_alone`, `the_engine_still_cannot_produce_a_v3_table` (unchanged, still green without the opt-in). Citation: `python/repark/tests/test_v3_upgrade.py`. |
| C-005 | Registry carries a FIXED row for the upgrade and a dated DECLARED row for each residual; north star §3 "Upgrade: v2 → v3 in place" is ✅ with the measured clause; `docs/design/format-v3-track.md` §5 Step 4 and the STATUS v3 bullet say what landed; maps in lockstep; this ledger `move`d to `completed/` last. | `make check-map-sync check-ledger-grammar check-ledgers check-docs-compaction`. | **PROVEN** | `V3-UPGRADE-1` FIXED; `V3-UPGRADE-V4-1` and `V3-UPGRADE-DV-1` DECLARED and dated 2026-09-02; north-star row ✅ keeping the 2026-08-25 ruling date; Step 4 one line; STATUS v3 bullet. Citation: `crates/repark-iceberg/src/write/map.md`. |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: v3-10-upgrade-v2-to-v3
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Upgrade, downgrade, same-version no-op, combined-key single commit, and every post-upgrade v3 path (append lineage, COW DELETE/UPDATE, MoR MERGE DV, rewrite_data_files, register_table) pinned at live-Spark values on the door each is reachable from.
      artifacts: [crates/repark-spark/src/tests/v3_upgrade.rs, crates/repark-sql/src/v3/create.rs, python/repark/tests/test_v3_upgrade.py, python/repark/tests/test_v3_live_oracle.py]
    - id: AT-2
      status: ATTACKED
      evidence: v1, v2 and v3 current versions; empty, non-numeric, out-of-range and negative requested values; opt-in on and off; upgrade alone and beside another key; a table with and without a legacy parquet position delete.
      artifacts: [crates/repark-functions/src/format_version.rs, crates/repark-spark/src/tests/v3_upgrade.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal leaves the table at its pre-ALTER version and rows; the legacy parquet-delete case refuses loudly and writes no Puffin file; the without-opt-in twin still cannot resolve _row_id.
      artifacts: [crates/repark-spark/src/tests/v3_upgrade.rs, python/repark/tests/test_v3_upgrade.py]
    - id: AT-4
      status: N/A
      justification: No new shared mutable state; the upgrade is one transaction on a loaded table.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM or secret handling. The reserved key is stripped before the property action, so it can never be persisted into the property map.
      artifacts: [crates/repark-iceberg/src/write/format_version.rs, crates/repark-spark/src/format_version.rs]
    - id: AT-6
      status: N/A
      justification: No Catalog trait change.
    - id: AT-7
      status: N/A
      justification: No new recursion or unbounded allocation; format_version_from_number returns an error rather than a silent fallback.
    - id: AT-8
      status: N/A
      justification: No dependency pin change; the fork stays fb0cacfa.
    - id: AT-9
      status: ATTACKED
      evidence: V3-UPGRADE-1 FIXED; V3-UPGRADE-V4-1 and V3-UPGRADE-DV-1 dated DECLARED with their TRIGGERs; north star, format-v3-track and STATUS truthed up.
      artifacts: [docs/spark-sql-iceberg-parity.md, task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md, STATUS.md]
    - id: AT-10
      status: ATTACKED
      evidence: Five clauses pinned; maps lockstep; two mutations run and restored; the alter.rs line ceiling ratcheted 1831 to 1830 in both the gate and its pin.
      artifacts: [scripts/check_rust_file_size.py, python/repark-parity/tests/test_cap_1_source_file_line_cap.py]
  complete: true
```

## Oracle transcript (C-002)

Live oracle: PySpark 4.1.2 + Iceberg 1.11.0, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, Hadoop
catalog, `coalesce(1)` single-file writes. Interpreter `<pyspark-4.1.2-oracle>`. Seed unless a
row says otherwise: v2 table `(1,a),(2,b),(3,c)` in one file, one append snapshot,
`last-sequence-number` 1.

| Cell | Spark result |
|---|---|
| (a) `ALTER … 'format-version'='3'` | format-version 2 → 3; snapshots 1 → 1 (**no snapshot**); `next-row-id` absent → 0; `last-sequence-number` 1; ONE new `*.metadata.json`; `properties` unchanged — the reserved key is **not** persisted; `DESCRIBE EXTENDED` gains `_row_id` / `_last_updated_sequence_number` and reads `format-version=3` |
| (a) rows right after the upgrade | `(1,NULL,NULL),(2,NULL,NULL),(3,NULL,NULL)` — pre-upgrade rows carry no lineage |
| (a) append `(4,d),(5,e)` in one file | `(1,2,1),(2,3,1),(3,4,1),(4,0,2),(5,1,2)`; `next-row-id` 5; snapshots 2; seq 2; files `first_row_id` 0 (new, 2 rows) and 2 (old, 3 rows) — the new manifest is assigned first |
| (b) downgrade `'2'` on v3 | `SparkException: Unsupported table change: Cannot downgrade v3 table to v2`; table unchanged |
| (c) `'3'` on a v3 table | succeeds as a **no-op** — no new metadata file, `next-row-id` and snapshots unchanged |
| (d) `'1'` on v2 | `Unsupported table change: Cannot downgrade v2 table to v1` |
| (d) `'4'` on v2 | **succeeds** — writes `"format-version": 4`, `next-row-id` 0, no snapshot; the v4 table then reads back; `CREATE … TBLPROPERTIES ('format-version'='4')` also succeeds |
| (d) `'x'` / `''` | `Unsupported table change: For input string: "x"` / `""` |
| (d) `'0'` on v4 | `Unsupported table change: Cannot downgrade v4 table to v0` |
| (e) `('format-version'='3', 'k'='v')` | both land in ONE metadata commit; `k=v` in `properties`, `format-version` absent, snapshots 1 |
| (f) v2 MoR, 4 rows, one **parquet** position delete, upgraded, then `DELETE`/`MERGE … DELETE` | the legacy parquet delete survives the upgrade; the v3 write emits ONE **PUFFIN** DV with `record_count = 2` (the legacy position merged in) and the parquet delete file leaves `.delete_files`; `next-row-id` 4; rows `(1,0,1),(4,3,1)`; one data file `first_row_id` 0 |
| (f′) same table with **no** legacy delete | MoR `MERGE … WHEN MATCHED THEN DELETE` on `id=2` → ONE PUFFIN DV `record_count = 1`; `next-row-id` 4; rows `(1,0,1),(3,2,1),(4,3,1)`; a following predicate `DELETE id=3` → DV `record_count = 2`, rows `(1,0,1),(4,3,1)`, `next-row-id` 4 |
| (f″) COW after the upgrade | `DELETE id=2` → `next-row-id` 3, rows `(1,0,2),(3,1,2),(4,2,2)`; then `UPDATE id=3` → `next-row-id` 6, rows `(1,0,2),(3,1,3),(4,2,2)` |
| (g) `rewrite_data_files` after the upgrade, six single-row files | one data file `first_row_id` 0; `next-row-id` 6; every row's `_last_updated_sequence_number` 7; the six `_row_id` are 0–5, but the id→row-id map follows Spark's rewrite task order (`1→3, 2→5, 3→2, 4→0, 5→1, 6→4` on the recorded run), so only the set and the sequence number are pinnable |

## Engine after the lift (C-003)

| Cell | Engine | Verdict |
|---|---|---|
| upgrade with the opt-in | v3, snapshots unchanged, `next-row-id` 0, reserved key not persisted | Spark-equal |
| rows right after the upgrade | `(1,NULL,NULL),(2,NULL,NULL),(3,NULL,NULL)` | Spark-equal |
| append after the upgrade | `(1,2,1),(2,3,1),(3,4,1),(4,0,2),(5,1,2)`, `next-row-id` 5 | Spark-equal |
| upgrade without the opt-in | refuses naming `repark.sql.allowCreateFormatVersion3` and `format-version`; table stays v2 | intended (opt-in) |
| downgrade `'2'` on v3 | refuses naming the key, `v3` and `v2` | Spark-equal class |
| same version | no-op; no metadata file written | Spark-equal |
| upgrade + another key | both land, ONE metadata commit | Spark-equal |
| `'1'` on v2 | refuses as a downgrade naming `v2` and `v1` | Spark-equal class |
| `'4'` on v2 | **refuses** naming the key, the value and "v1 through v3" | `V3-UPGRADE-V4-1` |
| `'x'` / `''` | refuses as unparsable naming the key | Spark-equal class |
| COW `DELETE id=2` then `UPDATE id=3` after the upgrade | `next-row-id` 3 then 6; `(1,0,2),(3,1,2),(4,2,2)` then `(1,0,2),(3,1,3),(4,2,2)` | Spark-equal |
| MoR `MERGE … DELETE` after the upgrade | ONE Puffin DV, `next-row-id` 4, `(1,0,1),(3,2,1),(4,3,1)` | Spark-equal |
| MoR *predicate* DELETE on v3 | still refused by `resolve_write_mode`'s V2-only gate | **consumed by V3-9** |
| MoR write over a legacy parquet position delete | refuses loudly at the fork's fresh-DV guard; no Puffin written; rows unchanged | `V3-UPGRADE-DV-1` |
| `rewrite_data_files` after the upgrade | one data file, `next-row-id` 6, six distinct ids 0–5, every seq 7 | Spark-equal on the pinnable invariants |
| `register_table` of the upgraded table on a fresh catalog | v3, `next-row-id` 5, same lineage triples | Spark-equal |

**Where the code lives.** `repark_functions::format_version::resolve_alter_format_version` is the
one resolver (it delegates the v3 opt-in refusal to `cardinality::resolve_create_format_version`,
so the CREATE and ALTER refusals cannot drift);
`repark_iceberg::write::format_version::set_properties_and_format_version` is the one transaction,
folding the upgrade action and the property action into a single commit;
`crates/repark-spark/src/format_version.rs` and `crates/repark-sql/src/alter.rs` are the two door
adapters. The upgrade also marks the ALTER dirty so the namespace is re-registered and the v3
metadata columns resolve on the next read.

**Residual `'4'`.** Iceberg 1.11.0 accepts format v4 on CREATE and ALTER. The owned fork's
`FormatVersion` stops at `V3`, so this engine refuses rather than writing metadata it cannot read
back. Filed as `V3-UPGRADE-V4-1`, dated 2026-09-02.

**Residual legacy parquet delete.** The fork's `row_delta_fresh_dv` guard refuses a DV that would
silently supersede a live parquet position delete; Spark merges it (`BaseDVFileWriter.loadPreviousDeletes`).
Filed as `V3-UPGRADE-DV-1`, dated 2026-09-02, with the wiring named as its TRIGGER.

**Pickup ritual.** `make ledger-archive` was run and its output **reverted**: the gates
(`check-map-sync`, `check-ledgers`) are green at `ca9c007` without it, and the concurrent V3-9
unit would collide on the same seventeen files. `make check-map-sync check-ledgers` green.

**Renamed pin.** `create_table.rs::or_replace_applies_requested_v3_and_alter_still_refuses_with_opt_in`
→ `…_and_alter_upgrades_with_opt_in`; the archived V3-2 and V3R-1 ledgers cite the old name and are
frozen. The without-opt-in twin lives in `v3_upgrade.rs::alter_upgrade_refuses_without_the_opt_in`.
