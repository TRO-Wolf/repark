# Independent re-audit: RePark format-v2 Iceberg PRODUCTION label (second full pass)

## 1. Header and audit boundaries

**Date:** 2026-10-29. **Auditor:** `claude-opus-5-5`, effort high, single agent (no sub-agents). **Mode:** findings only; read-only on both repositories; untracked probe files in scratch clones; local probes with a live Spark 4.1.2 oracle. **Elapsed wall time:** about 0.75 hours, 16:08:34–16:52 UTC (about 12:08–12:52 America/New_York). This is elapsed time, not summed compute.

| Repository | Revision read |
|---|---|
| TRO-Wolf/repark | `69b962d0ee4aad44ee606361ced5621e80086744` (main as cloned; one docs-only records commit, #1216, after `df43dca5`) |
| TRO-Wolf/iceberg-rust | `935cc55c8e2b20ee337c01eca45314ecb10e0c2f` (RePark's five `rev` pins and fork main as cloned; fork main was not re-fetched, see below) |
| Baseline used for "pre-existing or disturbed" | RePark `321c6095` (the first audit's sha), built separately |
| Oracle | PySpark 4.1.2 from `uv.lock` (`record` extra) with `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `SPARK_LOCAL_IP=127.0.0.1`, one JVM at a time, hadoop-type catalogs |

**Scratch root:** `the audit scratch directory/` with `audit2-repark/` (RePark clone), `audit2-fork/` (fork clone), `audit2-work/` (probes in `probes/`, logs, `repark-base/` clone at `321c6095`, `fork-target/`, `data/`). Push is disabled on all three clones. Nothing under `the owner's local checkout tree` was opened or modified. `git diff` is empty in both audited clones; the only additions are six untracked probe files (listed in section 9), none with a comment. No commit, push, pull request, AWS leg or credential use.

**Caps:** every build, test and probe ran under `systemd-run --user --scope -p MemoryMax=40G --same-dir --quiet taskset -c 0-25 env CARGO_BUILD_JOBS=8 RUST_TEST_THREADS=8 …`; the fork build additionally under the fork build lock file (`flock`) with `CARGO_INCREMENTAL=0` and its own target dir. The native module is a dev-profile `maturin develop` (uvx maturin 1.14.1) of the audited sha; it identifies by source sha, not by the 1.5.5 version string, and does not certify release wheels.

**Network:** only PyPI/crates.io as the locked builds needed. I did not fetch from GitHub, so fork main was not re-checked online, CI run histories were not read, and `cargo audit` was not re-run (its advisory database is fetched from GitHub). Those are gaps of this pass, stated, not inferred.

**Assumptions stated (the brief asked for none to be asked):**
- "Ids continue from AUD-006" is read as: the first new id is AUD-006.
- "Native door" (`crates/repark-sql`, the ANSI dialect) is inside the label's claim: STATUS.md says "Format-v2 reads and writes: PRODUCTION" with no door restriction.
- Severity uses the brief's scale. Where a write lands on an object other than the one Spark resolves, and Spark refuses the statement, I rate by the effect on persisted rows (deleted or overwritten rows rate as data loss).
- Two of my own probe files failed to compile on their first build (my code, not product code). I fixed the probe and ran it; no product test was re-run after a failure.

### Execution inventory

| Execution | Result |
|---|---|
| Locked env (`uv sync --locked --extra record --extra numpy --extra pandas --extra polars --extra ml-ext --no-install-package repark`) and `maturin develop` at `69b962d0` | Success; native build 4m34s |
| Same at baseline `321c6095` (separate clone and venv) | Success |
| First-audit probes A, B, C re-run unchanged (A, B byte-identical by SHA-256; C only its import path edited) | A: correct (ids 2,3); B: Parquet/Avro/ORC all read 0,4 after DELETE; C: gate now rejects the simulated `ConnectionError` (exit 1, the intended `AssertionError`) |
| First-audit fork probe D re-run unchanged (SHA-256 `3f94ab33…`) plus four new fork variants | 1 passed; 4 passed (filters, 20k rows across about 79 row groups, null key, re-added key with a new id) |
| Hint-walk go-around probe, RePark and Spark on the same files (18 cases) | **Silent stale read on a mixed-codec table (AUD-007)**; refusals where Spark answers (AUD-010); others Spark-equal |
| Delete-format cross-route matrix: 7 data/delete format pairs × 4 DML steps × 5 readers (RePark catalog, RePark path, Spark catalog, Spark path of each other's table) | 28/28 steps: all five readings identical |
| Spark-door post-commit fault matrix (15 statements × 2 fault modes) | 30/30: committed and reported `Ok` |
| Native-door post-commit fault matrix (5 statements × 2 modes, plus CTAS) | **CTAS, TRUNCATE, ALTER ADD COLUMN report an error after committing (AUD-011)** |
| DML target-spelling matrix vs Spark (25 steps), repeated on `321c6095` | **Temp-view shadowing writes to the Iceberg table (AUD-006)**; catalog/namespace collision (AUD-009); identical on the baseline |
| Volume: MoR DML with 3.46 GB of matched data-file path text (999,990 rows × ~3.46 KB paths), five routes | DELETE `>=`, `BETWEEN`, UPDATE: correct; subquery DELETE: documented refusal; MERGE: in-pool sort refusal, table unchanged |
| Two sessions on one shared catalog: stale-base writes (10 statements), 2×40 concurrent INSERT statements, 20 DELETE statements racing 40 INSERT statements | All correct; 2 DELETE statements failed loud on conflict validation; no lost or extra rows |
| RePark DML over a Parquet equality delete, key kept and key dropped (16 cases, Rust) | 16/16 correct, no resurrection |
| Struct drop probe vs Spark, with backtrace | **Library panic in the fork (AUD-008)** |
| Maintenance on Avro/ORC MoR tables vs Spark, Parquet control | Rows correct; `rewrite_position_delete_files` skips Avro/ORC (carded); `rewrite_data_files` leaves a delete file Spark removes (AUD-013) |
| WAP late-refusal, case-varied WAP, tables named `branch_x`/`tag_y`, text-leaf carve-out | Late refusal leaves a branch and a metadata file (matches its card); others Spark-equal or as carded |
| Existing tests: `test_acceptance_cross_read_local.py` (live Spark), `test_iceberg_load_path.py`, `test_ice_orc_avro_1.py`, `repark-parity/tests/test_cross_read_gate.py` with `REPARK_PARITY_LIVE=1` | **94 passed**, 0 skipped, 24.4 s |
| Records sweep: 1,241 added record lines since `321c6095`, every cited path and test name grepped | Stale ledger state and two date/card inconsistencies (AUD-012) |

## 2. Verdict on the label

**The label should be withdrawn (or, where the records treat it as re-issued, not stand) until AUD-006 and AUD-007 close, and AUD-008 is fixed or carries a dated row.** AUD-006 is a silent data loss reachable by ordinary Spark-door statements: with a temporary view `t` in scope, `DELETE FROM t`, `UPDATE t`, `MERGE INTO t`, `INSERT INTO t`, `INSERT OVERWRITE t` and `TRUNCATE TABLE t` all change the same-named Iceberg table in the current namespace, while `SELECT … FROM t` in the same session reads the view and Spark 4.1.2 refuses every one of those writes. AUD-007 is the AUD-001 failure class (a stale hint resurrects a deleted row) reached through Java's gzip metadata names, which the records list as out of scope but never say produce a wrong row; order section 1 says no carve-out covers a silent value.

Every first-audit finding is fixed on the shas read, and the fixes held under the go-arounds I tried (section 8). AUD-006 and the hint gap are pre-existing, not caused by the fixes: AUD-006 behaves identically on `321c6095`. The records currently state the label as re-dated PRODUCTION on 2026-10-28 (`9ab67d7d`), while AUD-002 to AUD-005 were open rows rather than signed carve-outs (AUD-012).

## 3. Findings, most severe first

### AUD-006 — A temporary view's name sends DELETE, UPDATE, MERGE, INSERT, INSERT OVERWRITE and TRUNCATE to the same-named Iceberg table

**Severity: Critical (silent data loss by ordinary statements). Repository: RePark, Spark door.** Pre-existing: identical on `321c6095` (`probe_view_shadow_base.log`), so the shared DML target resolver of #1206 neither caused nor fixed it.

**Where:** the Spark-door write routing resolves a one-part DML target by catalog and namespace qualification only. `crates/repark-spark/src/dml_target.rs:22-70` (`resolve_dml_target` → `qualify_table_parts`) never consults the session's temporary views. The INSERT, INSERT OVERWRITE and TRUNCATE routes resolve the same way: their outcome is identical, though they do not pass through that function.

**Reproduction:** `audit2-work/probes/probe_view_shadow.py` and `probe_view_shadow2.py`, each statement run on RePark (hadoop catalog `hd`) and on Spark 4.1.2 (its own warehouse):

```sql
CREATE NAMESPACE hd.ns;
CREATE TABLE hd.ns.t (id INT) USING iceberg TBLPROPERTIES ('format-version'='2');
INSERT INTO hd.ns.t VALUES (1),(2),(3);
USE hd.ns;
CREATE OR REPLACE TEMPORARY VIEW t AS SELECT 2 AS id;
SELECT id FROM t;                 -- RePark [2]   Spark [2]   (both read the view)
DELETE FROM t WHERE id = 2;       -- RePark ok    Spark error
SELECT id FROM hd.ns.t;           -- RePark [1,3] Spark [1,2,3]
INSERT INTO t VALUES (40);        -- RePark ok    Spark EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE
INSERT OVERWRITE t VALUES (50);   -- RePark ok    Spark EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE
SELECT id FROM hd.ns.t;           -- RePark [50]  Spark [1,2,3]
TRUNCATE TABLE t;                 -- RePark ok    Spark EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE
SELECT id FROM hd.ns.t;           -- RePark []    Spark [1,2,3]
```

`probe_view_shadow2.py` (merge-on-read table, view `t AS SELECT 99`): `UPDATE t SET id = 10 WHERE id = 1` makes the table `[2,3,10]` (Spark: `UNSUPPORTED_FEATURE.TABLE_OPERATION … does not support UPDATE TABLE`); `MERGE INTO t … WHEN MATCHED THEN DELETE` removes 2 (Spark: same class, MERGE INTO TABLE); `DELETE FROM t` empties the table, while `SELECT id FROM t` still returns `[99]` from the view.

**Expected:** Spark 4.1.2 (measured above): the one-part name resolves to the temporary view first, and every write to it refuses with the table unchanged. RePark's own `SELECT` agrees with Spark, so the same name resolves to two different objects within one session.

**Already named:** no. The registry's DML-2 row (`docs/spark-sql-iceberg-parity.md:1062-1063`) lists Spark's `EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE` for TRUNCATE, but no row or card records that RePark truncates the table behind the view. The CTE/temp-view card (`task/roadmap/mid-term/cte-temp-view-precedence-1-card-2026-10-01.md`) covers CTEs, not DML targets.

### AUD-007 — A stale hint still resurrects a deleted row when the newer metadata file has Java's gzip name

**Severity: Critical (silent wrong result), the same failure class and precondition as AUD-001, on a narrower trigger. Repository: RePark.** Pre-existing in kind: the walk added by #1205 does not cover compressed names.

**Where:** `crates/repark-core/src/iceberg_path.rs:125-146` (`newest_from_hint`) probes only `v{N+1}.metadata.json`. `metadata_file_version` (`:224-237`) rejects `v3.gz` stems, so the no-hint listing ignores gzip files too.

**Reproduction:** `audit2-work/probes/probe_hint_walk.py`, case `spark_mixed_codec_*`. Spark writes a hadoop table, `CREATE … ('format-version'='2')`, `INSERT (1),(2),(3)`, `ALTER TABLE … SET TBLPROPERTIES ('write.metadata.compression-codec'='gzip')`, `DELETE … WHERE id = 1`. The metadata dir then holds `v1.metadata.json, v2.metadata.json, v3.gz.metadata.json, v4.gz.metadata.json`, hint `4`. The probe restores the hint to `2`, as AUD-001's probe did.

```text
spark_mixed_codec_current_hint  repark: AnalysisException … version-hint.text names 'v4.metadata.json', which does not exist   spark: [[2],[3]]
spark_mixed_codec_stale_hint2   repark: [[1],[2],[3]]                                                                        spark: [[2],[3]]
```

**Expected:** ids 2,3. Spark 4.1.2 answers 2,3 on the same files with the same stale hint (Java `HadoopTableOperations` probes `v<N>.gz.metadata.json` and `v<N>.metadata.json.gz`). The production definition and order section 1 exclude a silent value from any carve-out.

**Already named:** partly. The ledger `task/ledgers/completed/ice-hadoop-hint-window-1-ledger.md` C-007 (OPEN, "the owner's question outside the label's gate") and the registry's FIXED paragraph (`docs/spark-sql-iceberg-parity.md:1264-1265`, "stay out of scope and OPEN") record the divergence shape, "Java answers v3.gz and RePark answers v2". Neither says the consequence is a deleted row returned, and neither STATUS nor the label order puts gzip-codec hadoop tables outside the claim.

### AUD-008 — Dropping the last field of a struct, then `SELECT *`, panics in the fork's reader

**Severity: High (library panic reachable by ordinary statements). Repository: fork, reached through RePark.** Pre-existing ("same on the base", fork ledger).

**Where:** fork `crates/iceberg/src/arrow/value.rs:1056` (`create_primitive_array_repeated`, the `(DataType::Struct(fields), None)` arm) calls `StructArray::new` with zero child fields. arrow-array 58.4.0 `struct_array.rs:91` unwraps `try_new`, which refuses an empty field list. Caller chain: `nested_projection::create_constant_column` (`nested_projection.rs:710`) → `RecordBatchTransformer::transform_columns` (`record_batch_transformer.rs:874`, `:960`) → `ArrowReader::process_parquet_file_scan_task` (`reader.rs:861`).

**Reproduction:** `audit2-work/probes/probe_struct_drop.py` (vs Spark) and `probe_struct_drop_bt.py` (`RUST_BACKTRACE=1`):

```sql
CREATE TABLE hd.ns.t (id INT, s STRUCT<k: INT>) USING iceberg TBLPROPERTIES ('format-version'='2');
INSERT INTO hd.ns.t SELECT 1, named_struct('k', 10);
ALTER TABLE hd.ns.t DROP COLUMN s.k;    -- both engines: ok
SELECT * FROM hd.ns.t;                  -- Spark: [(1, Row())]
                                        -- RePark: PySparkException: repark internal error in
                                        --   PyDataFrame.__arrow_c_stream__.next: called `Result::unwrap()` on an `Err` value:
                                        --   InvalidArgumentError("use StructArray::try_new_with_length or
                                        --   StructArray::new_empty_fields …") (a Rust panic was caught …)
SELECT id FROM hd.ns.t;                 -- both: [1]
```

Dropping both fields of a two-field struct, one at a time, gives the same panic (`hd.ns.u`). The panic is caught at the facade boundary, so the process survives. The arm would also produce a NULL struct where Spark answers a non-null empty struct.

**Expected:** Spark answers `(1, Row())`. The production definition lists "Panics in library code" as a disqualifier.

**Already named:** only on the fork. Fork `task/f-eqdel-dropped-key-1-ledger.md:219-220` and row F2-6 (`:232`) record it OPEN with "Carded on the RePark side". I found no RePark card, registry row or ledger row for it (grep for `StructArray::try_new`, "only child", "empty struct" and "panic" in `task/roadmap/` and the registry).

### AUD-010 — The path read refuses gzip-named hadoop tables and stale hints into cleaned-up versions, where Spark answers

**Severity: Medium (refusal of a read Spark answers; its message is misleading). Repository: RePark.**

**Reproduction:** `probe_hint_walk.py`:

```text
spark_gzip_current_hint (Spark table, codec gzip from create; files v1..v3.gz.metadata.json, hint 3)
   repark: AnalysisException: no Iceberg table found at '<loc>': version-hint.text names 'v3.metadata.json', which does not exist
   spark:  [[2],[3]]
spark_cleanup_stale_hint_to_deleted_v2 (Spark table with write.metadata.delete-after-commit.enabled=true,
   previous-versions-max=1; files v5, v6; hint restored to 2)
   repark: AnalysisException: … version-hint.text names 'v2.metadata.json', which does not exist
   spark:  [[1],[2],[3],[4],[5]]
```

**Expected:** Spark 4.1.2 answers both, as measured. The refusal names a missing file while the table's current metadata exists under its gzip name.

**Already named:** the gzip case only, as the out-of-scope C-007 above. The cleaned-up-version case is not named. The hint-file-missing fallback and a malformed hint are Spark-equal (`rp_table_hint_missing`, `rp_table_hint_garbage`).

### AUD-011 — Native door: CTAS, TRUNCATE and ALTER report failure after committing; a CTAS table stays invisible

**Severity: Medium (fail-loud outcome reporting, the AUD-002 class). Repository: RePark, native (ANSI) door.**

**Reproduction:** untracked `audit2-repark/crates/repark-sql/tests/audit2_native_postpublish.rs`, the repository's own `FaultCatalog` harness with the session built on `AnsiDialect`. The fault arms after a successful `update_table` or `publish_create_table`, then `list_tables` fails (mode `list`) or `load_table` fails (mode `load`):

```text
AUDIT2_NATIVE_CTAS armed=true exists=Ok(true) rows_after=Err("Error during planning: table 'fault.ns.t' not found") outcome=Err(Unexpected => table listing unavailable)
AUDIT2_PP case=truncate mode=list         verdict=ERR_BUT_COMMITTED snapshots=1->2 outcome=Err(Unexpected => table listing unavailable)
AUDIT2_PP case=alter_add_column mode=list verdict=ERR_BUT_COMMITTED snapshots=1->1 outcome=Err(Unexpected => table listing unavailable)
(insert, delete, update: OK_COMMITTED in both modes; all five OK_COMMITTED in mode load)
```

After CTAS, the next `SELECT` still fails "not found" with the fault cleared, because the name directory stays frozen.

**Expected:** the committed outcome is reported, as the Spark door now does (30/30 in my Spark-door matrix).

**Already named:** yes, card `task/roadmap/mid-term/native-door-post-publish-1-card-2026-10-28.md`, which says "Measured: none". This is that measurement, and it matches the card's site list.

### AUD-009 — A two-part name whose first part is also a catalog name resolves to a namespace; Spark resolves the catalog and refuses

**Severity: Low. Repository: RePark, Spark door.** Pre-existing (identical on `321c6095`).

**Reproduction:** `probe_catalog_ns_collision.py`, with catalogs `hd` and `ns` registered on both engines and current namespace `hd.ns`:

```text
SELECT id FROM ns.t          repark [[1],[2],[3]]   spark TABLE_OR_VIEW_NOT_FOUND `ns`.`t`
INSERT INTO ns.t VALUES (4)  repark ok              spark TABLE_OR_VIEW_NOT_FOUND
DELETE FROM ns.t WHERE id=1  repark ok              spark TABLE_OR_VIEW_NOT_FOUND
SELECT id FROM hd.ns.t       repark [[2],[3],[4]]   spark [[1],[2],[3]]
```

RePark is self-consistent here (its SELECT and DML agree), so I rate this Low, unlike AUD-006. **Expected:** Spark 4.1.2 refuses. **Already named:** no.

### AUD-012 — Records: the label ledger is stale, the label reads as re-issued, two dates and one card reference are wrong

**Severity: Low (records). Repository: RePark (one item on the fork).**

1. `task/ledgers/staging/ice-v2-label-1-ledger.md` at `69b962d0`: C-046 to C-050 (AUD-002 to AUD-005 and RUSTSEC-2026-0414) and C-056 are OPEN, and the last VERDICT line names them open, though #1206, #1209, #1210, #1211 and #1213 have merged. Measured: `git diff 321c6095..HEAD` of that ledger adds no closing rows after C-056.
2. `STATUS.md:45` says "Format-v2 reads and writes: PRODUCTION … Re-dated 2026-10-28 (`9ab67d7d`)". The order's Outcome (`task/wo/ice-v2-label-1/order.md`) re-dated the label on the merge of #1205 while AUD-002 to AUD-005 remained "dated rows with their units". These were OPEN defect rows, not owner-signed carve-outs. Order section 1: "A label moves when every row is green on main with a dated pin or carries a dated owner carve-out". The owner's re-audit brief describes the label as withdrawn pending re-issue, so the records and the brief disagree about the label's current state.
3. `task/ledgers/completed/ice-hadoop-hint-window-1-ledger.md:10` and `:18` say merged "2026-10-29". The squash `9ab67d7d` is `2026-10-28T09:58:30-04:00`, and label-ledger C-055 corrects the same date elsewhere.
4. Fork `task/f-eqdel-dropped-key-1-ledger.md` F2-6 says "Carded on the RePark side". No such RePark card exists (AUD-008).
5. Minor, not an error: registry POSDEL-BATCH-1 says "No live Spark run at the past-2-GiB size exists in this repository". Section 5 below records a RePark-only measurement past that size.

**Pins cited by PROVEN clauses:** I extracted every backticked path and test name from the 1,241 added record lines. All 61 cited repository paths exist; the one other path-shaped token, `docs/ice-v2-label-1`, is the label branch's name, not a file. The test names that no longer exist belong to the ICE-DELETE-FORMAT-GUARD-1 interim guard that #1209 reverted, and their rows are marked superseded or reverted: ice-delete-format-guard-1 C-007 dated row, the `data_format.rs` map row, and the RP-61 revert card. POSDEL-BATCH-1 names its renamed control pin with a note. I found no current PROVEN clause that cites a missing pin. Fork-sync rows RP-61 and RP-62 claim "the RePark lock moves only the six fork source lines": verified (6 removed and 6 added `source = …` lines in each of `6ec819ef` and `df43dca5`, nothing else).

### AUD-013 — `rewrite_data_files` on an Avro or ORC merge-on-read table leaves a delete file that Spark removes

**Severity: Low (maintenance divergence; rows correct). Repositories: fork maintenance, reached through RePark.**

**Reproduction:** `probe_maint_formats.py` (Avro and ORC, then a Parquet control). Each engine: MoR table, 7 rows in two INSERT statements, `DELETE … IN (2, 6)`, `UPDATE … WHERE id = 3`, `CALL rewrite_position_delete_files(…, map('rewrite-all','true'))`, `CALL rewrite_data_files(…, map('rewrite-all','true'))`, then `SELECT count(*) FROM <t>.delete_files`.

```text
                      rewrite_position_delete_files   rows after (both engines)       delete_files after rewrite_data_files
avro   repark/spark   0,0,0,0 / 3,3,2507,2507         1a 3z 4d 5e 7g (equal)          1 / 0
orc    repark/spark   0,0,0,0 / 1,1,1108,1108         equal                           1 / 0
parquet repark/spark  3,2,4530,3028 / 1,1,2005,2005   equal                           0 / 0
```

**Expected:** Spark 4.1.2 rewrites the Avro/ORC position deletes and leaves no delete file. The `rewrite_position_delete_files` skip is the carded `continue` (F-EQDEL-CONVERT-BATCH-1, second item, "Avro and ORC position deletes are left uncompacted; the data stays correct"), which this matches. The retained delete file after `rewrite_data_files` is not named. Since #1209, RePark-written Avro and ORC tables carry Avro and ORC deletes again, so both residues are reachable from ordinary RePark tables.

### AUD-014 — `write.metadata.delete-after-commit.enabled` is ignored on RePark's hadoop-type writes

**Severity: Low (a table property silently not honoured). Repository: RePark / fork memory catalog with hadoop naming.**

**Reproduction:** `probe_hint_walk.py` cases `rp_cleanup_setup` and `spark_cleanup_setup`, the same DDL (`'write.metadata.delete-after-commit.enabled'='true','write.metadata.previous-versions-max'='1'`) plus five INSERT statements on each engine. RePark keeps `v1…v6.metadata.json`; Spark keeps `v5, v6`. **Expected:** Spark/Java behaviour. **Already named:** no registry row mentions either property.

## 4. Leads (not reproduced; no severity)

| Lead | Evidence | What would prove or dismiss it |
|---|---|---|
| WAP-BRANCH-TARGET-SILENT-1 (P2 card: a case-varied DELETE under a session WAP branch writes to main) | On the facade's default config (`register_memory_catalog('ice')`), `DELETE/UPDATE/INSERT/MERGE` on `ICE.NS.T` and `ice.NS.t` under `spark.wap.branch` refuse before any write, and main is unchanged (`probe_wap_case.log`). Spark also refuses. | The card's exact session configuration (likely identifier normalisation on). A silent write to main under WAP would be a silent wrong-ref write. |
| A table created by another session on a shared catalog is "not found" until `refresh_catalog_provider` | In-process Rust probe (`repark-two-run1.log`): session B, registered before A's `CREATE TABLE`, fails `table 'ice.ns.t' not found` on SELECT and INSERT. After a refresh, all cross-session reads and writes are correct. | The same against a shared external catalog (Glue/REST) in two processes. Spark resolves by name per statement. |
| Retained memory on the position-delete path is outside the declared bound | Peak RSS 4.45–7.65 GiB for 3.46 GB of matched path text; no bound refusal on DELETE/UPDATE. The bound record lists only PP-01, PP-02 and PA-11 as out-of-pool holders. | Repeat at 10× matched path text, recording peak RSS and outcome. |
| `DELETE FROM v` where `v` is a temp view over an Iceberg table | Spark accepted it with no matching rows; RePark refuses `TableNotFound … name: "v"` | Repeat with a matching row to see whether Spark deletes through the view. If it does, RePark refuses a statement Spark answers. |
| F-EQDEL-DROPPED-KEY-2 (Avro/ORC data loud), F-EQDEL-PROMOTED-KEY-POS-1, REPLACE-WHERE-ORPHAN-1, REPLACE-WHERE-AVRO-PARTIAL-MATCH-1 | Not re-measured in this pass | Their cards' probes |
| Many small equality-delete files form a deep predicate tree (first-audit lead) | Not probed | As the first audit stated |

## 5. Gate dimensions (first audit's list): what changed since 2026-10-28, and what I measured

| Dimension | Covered by anything since 10-28? | This pass |
|---|---|---|
| **Volume** | RP-POSDEL-BATCH-2 pins exercise the batch boundary with a small `position-delete-batch-rows`, not real path-text volume | **Measured:** 1,000,000 rows, 16 data files at 3.46 KB path length, 999,990 matched (3.46 GB path text, above 2³¹). DELETE `>=` ok in 47.7 s, peak RSS 4.70 GiB; DELETE `BETWEEN` ok in 17.0 s, 7.65 GiB; UPDATE ok in 48.8 s, 4.45 GiB (survivors and snapshot counts exact); `IN (subquery)` refused by the documented subquery valve, table unchanged; MERGE refused by DataFusion's in-pool sort (`ResourcesExhausted`), table unchanged (1 snapshot). No panic. |
| **Concurrency** | Nothing new in the records | **Measured** (two sessions, one shared catalog, tokio interleaving): 80/80 concurrent INSERT statements present, none lost or extra. 20 DELETE statements racing 40 INSERT statements: 18 committed, 2 failed loud (`DataInvalid => Found conflicting files…`), 0 lost inserts, 0 rows kept that a committed delete targeted. |
| **Long sessions** | Nothing new | **Measured:** after B commits, A's next SELECT sees B's row, and A's DELETE, UPDATE, MERGE, INSERT OVERWRITE, TRUNCATE, INSERT and DELETE-all all act on B's row correctly (10/10). Table creation by another session is invisible until refresh (lead). |
| **Failure injection beyond kill** | AUD-002 pins (CTAS, RTAS, list failure) | **Measured:** a post-commit list or load failure on 15 Spark-door statements: 30/30 report the committed outcome. Native door: AUD-011. Not measured: I/O errors mid-commit, truncated metadata, clock skew. |
| **Carve-out blast radius** | None | Text-leaf (NESTED-TEXT-LEAF-STORE-1): under `America/New_York`, a string into a nested `STRUCT<ts>` and `ARRAY<TIMESTAMP>` leaf stores the same instant as an explicit `TIMESTAMP` literal (`1767243600` both); Spark refuses the string. It is a leniency, not a wrong value, in the cells I tried. ENC carve-outs not audited (per brief). |
| **Hints naming data-deleting actions** | None | Grep of every error string added since `321c6095` in RePark `crates/*/src` and fork `ea0b7741..935cc55c`: no hint names drop, truncate, expire, purge or remove. |
| **Partial commits** | Cards WAP-LATE-REFUSAL-BRANCH-1, REPLACE-WHERE-ORPHAN-1 | **Reproduced the WAP card:** under `spark.wap.branch`, a refused subquery DELETE creates branch `w1` and a metadata file (Spark executes the DELETE on `w1`). A refused `INSERT … SELECT FROM nosuchcat.ns.x` creates branch `w2` and a metadata file, where Spark refuses with no branch. Matches the card (P3). |

Add for the format-v3 gate: **name resolution against session state** (temp views, catalog vs namespace precedence) for every write door, compared with Spark; **Java-written metadata naming** (gzip codec, cleaned-up versions) on path reads; **schema evolution to empty structs**; **maintenance on non-Parquet deletes** with Spark's metadata outcome as the oracle.

## 6. What looks sound (checked this pass)

1. **AUD-001 fix:** the first audit's probe now returns ids 2,3. A hint stale by two versions walks to the newest; hint past newest, `0` and a missing hinted file refuse loud. Malformed, padded, negative or out-of-range hints fall back to listing and answer the newest. A gap at `v3` stops at `v2`, the same as Spark.
2. **AUD-003 fix, across routes:** 7 format pairs (Parquet/Avro/ORC data × inherited or explicit Avro/ORC/Parquet deletes) × DELETE `BETWEEN`, UPDATE, MERGE (delete, update, insert) and `IN (SELECT … FROM view)` DELETE. RePark's catalog read, RePark's path read of its own table and of Spark's table, and Spark's catalog and path reads of RePark's table all agree on all 28 states. Delete files carry the requested format on both engines.
3. **AUD-004 fix:** the first audit's fork probe passes. Dropped key with equality and range filters on `data`; 20k rows over about 79 row groups with page-index limits, before and after the drop (8,571/17,142 exact); null-key deletes; re-added key with a new id (field id 3) not deleted by the old delete, with the new id filterable. RePark DML over equality deletes, key kept and dropped (MoR and COW UPDATE, DELETE and MERGE; `rewrite_data_files`; `INSERT OVERWRITE … SELECT`): 16/16, no resurrection.
4. **AUD-002 fix:** see section 5. No disturbance found from the #1206 resolver: 25 target spellings behave identically on `321c6095` and `69b962d0`, including tables named `branch_x` and `tag_y`, branch-qualified DML, aliases and back-ticked names.
5. **AUD-005 fix:** the simulated `ConnectionError` is rejected. The live local cross-read with real Spark passes, so Spark's actual first line matches the narrowed condition `Cannot convert unsupported type to Spark: timestamp_ns` exactly.
6. **Repins:** each RePark lock move changes only the six fork `source` lines. Fork changes `ea0b7741..935cc55c` touch only the arrow reader and delete loaders, the ORC reader footer, the row-filter plan and the page-index evaluator, plus tests and ledgers. No workflow, writer or commit-path change. The new `plan_row_filter` refusals fire only for equality-delete ids absent from the table schema, so ordinary filters on added columns are untouched by construction.
7. **Existing pins** for the fixed items pass (94 passed, live tier on).

## 7. Fork section

**Verdict on the pin `935cc55c`:** it fixes AUD-003 and AUD-004 as RePark consumes them; every variant I tried holds. It carries one reproduced High: AUD-008, a reader panic on schema evolution to an empty struct, recorded on the fork as F2-6 and pointing at a RePark card that does not exist. It also carries one Low: AUD-013's maintenance residue on Avro/ORC deletes, partly carded. With AUD-008 open, the pin does not meet the definition's no-panic rule for the reads RePark serves.

**Findings:** AUD-008 (High, `crates/iceberg/src/arrow/value.rs:1056`); AUD-013 (Low, `crates/iceberg/src/maintenance/rewrite_position_delete_files.rs:565`, plus the delete file retained by `rewrite_data_files`).

**Leads:** retained memory on the position-delete path at 10× volume; deep predicate trees across many equality-delete files; F-EQDEL-DROPPED-KEY-2 and F-EQDEL-PROMOTED-KEY-POS-1 not re-measured.

**Interop workflow:** no `.github` change between `ea0b7741` and `935cc55c`, so the first audit's description stands (weekly, 65-suite discovery floor). I did not retrieve run results (no GitHub access in this pass). **Fork main:** `935cc55c` as cloned; not re-fetched.

**RustSec:** not re-run (the advisory DB needs GitHub). #1210's lock change is the two pulldown-cmark lines only (`0.13.4` to `0.13.5`, checksum), as its records say.

## 8. Fix-by-fix verdict (the changes table of the re-audit brief)

| Change | Verdict |
|---|---|
| #1203 `91671b02` RP-POSDEL-BATCH-2 | **Holds.** Past 2³¹ bytes of path text (3.46 GB), DELETE `>=`, DELETE `BETWEEN` and UPDATE complete with exact survivors; MERGE refuses loud and leaves the table unchanged (section 5). |
| #1204 `f7c6cda8` audit filed, label withdrawn | **Holds with a note:** the withdrawal was re-dated away the same day (AUD-012 item 2). |
| #1205 `9ab67d7d` hadoop hint walk | **Holds with a note:** plain names hold. **AUD-007 goes around it** with Java's gzip names (silent resurrection), and AUD-010 lists the refusals where Spark answers. |
| #1206 `3ce17f73` post-publish outcome, interim guard, resolver, branch ordering | **Holds** on the Spark door (30/30 post-commit faults). The resolver changed no spelling's outcome. The native door is not covered (AUD-011, carded). AUD-006 is pre-existing, not caused by it. |
| #1207 `36f9c732`, #1208 `3d24ad40` re-date records | **Holds with a note:** the re-date happened with AUD-002 to AUD-005 open as defect rows (AUD-012 item 2); one ledger still carries the wrong date (AUD-012 item 3). |
| #1209 `6ec819ef` repin to `56aa8913`, guard reverted | **Holds:** 28/28 cross-route states equal across five readers. Note AUD-013: maintenance on the Avro and ORC deletes this PR re-enables. |
| #1210 `e7135000` pulldown-cmark 0.13.5 | **Holds** as a lock change (2 lines); `cargo audit` not re-run (no advisory-DB access). |
| #1211 `7ef6bcf4` cross-read gate narrowed | **Holds:** the simulated failure is rejected, and the live local cross-read passes with real Spark's first line. |
| #1212 `7861a08c` records | **Holds** (records only; the cited card files exist). |
| #1213 `df43dca5` repin to `935cc55c`, eqdel door pins | **Holds:** the door read pins pass. My 16-case DML-over-equality-delete matrix (key kept and dropped) shows no resurrection. |
| Fork #375 `56aa8913` Avro/ORC delete loader | **Holds** (subsumed by #377 for equality deletes; position deletes verified across routes). |
| Fork #377 `935cc55c` dropped/nested/re-added keys by field id | **Holds:** the first audit's probe plus 4 variants pass. **Note:** its ledger's F2-6 (AUD-008) is open, and the "carded on the RePark side" claim is wrong. |
| Still-open cards | F-EQDEL-CONVERT-BATCH-1 item 2: reproduced, matches the card. WAP-LATE-REFUSAL-BRANCH-1: reproduced (two shapes), matches the card. SPARK-DOOR-TARGET-NORMALISATION-1: case-varied names refuse on both engines on hadoop catalogs (Spark-equal). WAP-BRANCH-TARGET-SILENT-1: not reproduced on the default config (lead). NATIVE-DOOR-POST-PUBLISH-1: now measured (AUD-011). The others were not re-measured. |

## 9. Reproduction commands and probe sources

Prefix `CAP` = `systemd-run --user --scope -p MemoryMax=40G --same-dir --quiet taskset -c 0-25 env CARGO_BUILD_JOBS=8 RUST_TEST_THREADS=8`; `SPARK` = `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1`; `S` = the scratch root; `PY` = `$S/audit2-repark/.venv/bin/python`. All Python probes run from `$S/audit2-work/probes/`.

```sh
CAP $S/audit2-work/build-native.sh                                  # uv sync --locked … ; uvx maturin@1.14.1 develop       exit 0
CAP $S/audit2-work/build-native-base.sh                             # same in repark-base @321c6095                          exit 0
CAP $PY probe_public_paths.py ; CAP $PY probe_delete_formats.py     # first-audit A, B                                       exit 0, 0
CAP PYTHONPATH=$S/audit2-repark/python/repark/tests $PY probe_cross_read_gate.py   # first-audit C; gate now rejects         exit 1
CAP SPARK $PY probe_hint_walk.py                                    # AUD-007, AUD-010, AUD-014                              exit 0
cd $S/audit2-fork && flock <fork-build-lock> CAP CARGO_INCREMENTAL=0 $S/audit2-work/fork-tests.sh
    # cargo test --locked -p iceberg --test audit_dropped_eq_key ; --test audit2_eqdel_probe (CARGO_TARGET_DIR=$S/audit2-work/fork-target)   exit 0, 0
CAP SPARK $PY probe_delete_routes.py                                # 28 cross-route states                                  exit 0
cd $S/audit2-repark && CAP cargo test --locked -p repark-spark --test audit2_postpublish_ops -- --nocapture       exit 0
cd $S/audit2-repark && CAP cargo test --locked -p repark-sql --test audit2_native_postpublish -- --nocapture --test-threads=1   # AUD-011   exit 0
CAP SPARK $PY probe_dml_targets.py ; same with $S/audit2-work/repark-base/.venv/bin/python   # AUD-009, baseline compare   exit 0, 0
CAP SPARK $PY probe_view_shadow.py ; same with the baseline venv ; CAP SPARK $PY probe_view_shadow2.py   # AUD-006      exit 0, 0, 0
CAP SPARK $PY probe_wap_case.py ; CAP SPARK $PY probe_wap_late_and_textleaf.py ; CAP SPARK $PY probe_textleaf.py         exit 0, 0, 0
CAP $S/audit2-work/volume-run.sh                                    # probe_volume_posdel.py ge|between|in_subquery|update|merge 1000000   exit 0 (each route 0)
cd $S/audit2-repark && CAP cargo test --locked -p repark-spark --test audit2_two_sessions -- --nocapture --test-threads=1
    # run 1: compile error in my probe (non-Send futures under tokio::spawn), exit 101; probe fixed
    # run 2: exit 0 (B not refreshed: the creation-visibility lead); run 3 with refresh: exit 0
    # run 4: audit2_concurrent_delete_and_insert with inserted values disjoint from the delete predicate: exit 0
CAP SPARK REPARK_PARITY_LIVE=1 PYTHONPATH=python/repark-parity/src $PY -m pytest python/repark/tests/parity/test_acceptance_cross_read_local.py \
    python/repark/tests/iceberg/test_iceberg_load_path.py python/repark/tests/writes/test_ice_orc_avro_1.py \
    python/repark-parity/tests/test_cross_read_gate.py -q -p no:cacheprovider          # 94 passed                         exit 0
CAP SPARK $PY probe_struct_drop.py ; CAP RUST_BACKTRACE=1 $PY probe_struct_drop_bt.py   # AUD-008                          exit 0, 0
CAP SPARK $PY probe_branch_named_tables.py ; CAP SPARK $PY probe_catalog_ns_collision.py                                   exit 0, 0
cd $S/audit2-repark && CAP cargo test --locked -p repark-sql --test audit2_eqdel_dml -- --nocapture
    # first build: compile error in my probe (a helper not copied), exit 101; probe fixed; run: exit 0
CAP SPARK $PY probe_maint_formats.py (avro, orc) ; same edited to ("parquet",)   # AUD-013                                   exit 0, 0
```

**Untracked probe files in the audited clones** (SHA-256 prefix): `audit2-repark/crates/repark-spark/tests/audit2_postpublish_ops.rs` (`234133bc…`), `audit2-repark/crates/repark-spark/tests/audit2_two_sessions.rs` (`c4009452…`), `audit2-repark/crates/repark-sql/tests/audit2_native_postpublish.rs` (`b4ae9c44…`), `audit2-repark/crates/repark-sql/tests/audit2_eqdel_dml.rs` (`6fec6d74…`), `audit2-fork/crates/iceberg/tests/audit_dropped_eq_key.rs` (`3f94ab33…`, the first audit's Appendix D unchanged), `audit2-fork/crates/iceberg/tests/audit2_eqdel_probe.rs` (`65cb0b5c…`). The Rust probes reuse the repository's own `FaultCatalog` and equality-delete helpers, copied verbatim, and contain no comments.

**Python probes** in `audit2-work/probes/` (SHA-256 prefix): `probe_view_shadow.py` `ab8ee8e7…`, `probe_view_shadow2.py` `e6a949c4…`, `probe_hint_walk.py` `3c570969…`, `probe_struct_drop.py` `83e69919…`, `probe_struct_drop_bt.py` `fde4f3e7…`, `probe_catalog_ns_collision.py` `6d9dc223…`, `probe_maint_formats.py` `6aecb052…` (as last run, Parquet), `probe_delete_routes.py` `e71dbe16…`, `probe_dml_targets.py` `0901c16c…`, `probe_volume_posdel.py` `81ab77fb…`, `probe_wap_case.py` `392d0d8a…`, `probe_wap_late_and_textleaf.py` `36637ac5…`, `probe_textleaf.py` `40af1421…`, `probe_branch_named_tables.py` `2ed4f12d…`, `spark_util.py` `5e7607ea…`; the first-audit probes `probe_public_paths.py` `44a696b3…` and `probe_delete_formats.py` `52a2cc29…` (both byte-identical to the first audit's Appendix A and B) and `probe_cross_read_gate.py` `1802a3f8…` (Appendix C with only its import path changed). Every log named above is beside them in `audit2-work/`.

**Minimal AUD-006 reproduction, self-contained:**

```python
import tempfile
from repark import ReparkSession
wh = tempfile.mkdtemp()
s = (ReparkSession.builder.appName("aud006").config("spark.sql.catalog.hd.type", "hadoop")
     .config("spark.sql.catalog.hd.warehouse", wh).getOrCreate())
for q in ["CREATE NAMESPACE hd.ns",
          "CREATE TABLE hd.ns.t (id INT) USING iceberg TBLPROPERTIES ('format-version'='2')",
          "INSERT INTO hd.ns.t VALUES (1),(2),(3)", "USE hd.ns",
          "CREATE OR REPLACE TEMPORARY VIEW t AS SELECT 99 AS id",
          "TRUNCATE TABLE t"]:
    s.sql(q).collect()
print(s.sql("SELECT id FROM t").to_arrow().to_pylist())         # [{'id': 99}]   (the view)
print(s.sql("SELECT id FROM hd.ns.t").to_arrow().to_pylist())   # []             (expected [1,2,3]; Spark refuses the TRUNCATE)
```

**Resources:** free space about 739 GiB at start and 716 GiB at the end (whole volume). This pass's work directory is about 12 GiB, plus the RePark clone's `target/`. Nothing outside the scratch root was deleted. No process I did not start was signalled.
