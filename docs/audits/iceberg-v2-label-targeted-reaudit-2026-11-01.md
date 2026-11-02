# Targeted re-audit: the three blocking fixes of the second audit, and regression of every earlier fix (format-v2 PRODUCTION label)

## 1. Header

**Date:** 2026-11-01. **Auditor:** `claude-opus-5-5`, effort high, single agent (no sub-agents). **Mode:** findings only; read-only on both repositories; untracked probe files in the scratch clones, removed at the end; local probes against a live Spark 4.1.2 oracle. **Elapsed wall time:** about 0.9 hours, 22:37:30 UTC to about 23:30 UTC on 2026-11-01 (17:37 to 18:30 America/New_York). This is elapsed time, not summed compute.

| Repository | Revision read |
|---|---|
| TRO-Wolf/repark | `d05620584965ecf22c6b10133bd430e1dee0d6f2` (main, the brief's sha; RP-64 merged) |
| TRO-Wolf/iceberg-rust | `56ff5545c10ddb7c5234f8d7fb513a72adc9ee80` (RePark's five `rev` pins; fork #380) |
| Baselines for "pre-existing" | RePark `321c6095` (the first audit's sha; the second audit's built clone `audit2-work/repark-base`, reused unchanged). The second audit's `69b962d0` clone no longer exists, so "pre-existing" below means "present at `321c6095`". |
| Oracle | PySpark 4.1.2 from `uv.lock` with `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64` (JDK 17), `SPARK_LOCAL_IP=127.0.0.1`, hadoop-type catalogs, one JVM at a time |

**Scratch root:** `<scratch>`. Clones: `<scratch>/audit3-repark`, `<scratch>/audit3-fork` (push disabled on both, detached at the shas above). Probes, logs and data: `<scratch>/audit3-work/` (`probes/`, `logs/`, `data/`, `fork-target/`). Nothing under the live checkout was opened. No commit, push, pull request, AWS leg or credential use. No process I did not start was signalled.

**Caps:** every build, test and probe ran under `systemd-run --user --scope -p MemoryMax=40G --same-dir --quiet taskset -c 0-25 env CARGO_BUILD_JOBS=8 RUST_TEST_THREADS=8 …`; fork builds additionally under `flock <lock>` with `CARGO_INCREMENTAL=0` and their own target dir. The native module is a dev-profile `uvx maturin@1.14.1 develop` of `d0562058`; it identifies by source sha, not by its version string, and does not certify release wheels.

**Assumptions stated (no questions were asked):**
- File name. My launcher's brief names `iceberg-v2-label-targeted-reaudit-2026-10-31.md`. The owner's brief, finalised 2026-11-01, names `iceberg-v2-label-targeted-reaudit-2026-11-01.md` in its section 5, and it says it is the task. I wrote one file under the owner's name.
- Finding ids start at AUD-015. Severity uses the first brief's scale. As in the second audit, a write that lands on an object other than the one Spark resolves, where Spark refuses, is rated by its effect on persisted rows.
- "Both doors" means the Spark door (the facade) and the native ANSI door (`repark-sql` with `AnsiDialect`, reached from Rust).
- "Fresh session" on a `type=hadoop` catalog means a new session that adopts the table with `register_table`. Measured: a `newSession()` on RePark's hadoop catalog does not see existing tables, which the registry declares (ICE-LOCAL-COMMIT-POINT-1: the local catalog keeps its pointer in process memory). A `CREATE TABLE` at the same location from that fresh session refuses with `CatalogCommitConflicts` and changes no existing file.
- I made three probe-side mistakes. Each failure was in my own code, not product code, and I fixed the probe and ran it again: a struct probe hit a JSON key error on map keys; a nested probe normalised Spark `Row` objects wrongly; and the native-door Rust probe failed to compile (a borrow of a temporary). I re-ran no product test after a failure. `probe_cross_read_gate_a3.py` first stopped at import setup (`ModuleNotFoundError: _acceptance`), which the first two audits also hit, and then ran with `PYTHONPATH` set to the tests directory.

### Execution inventory

| Execution | Result |
|---|---|
| `uv sync --locked …` + `maturin develop` at `d0562058` | Success (exit 0) |
| Second audit's AUD-006 probes, byte-identical (`probe_view_shadow.py`, `probe_view_shadow2.py`) | Every write through the view name refuses, and the table is unchanged |
| AUD-006 go-around matrix, RePark vs Spark (158 cases: 44 spellings, 22 frame-writer cases, case, non-ASCII, quoting, `USE`, view before or after the table, view over a frame or over the table) | 146 equal. The 12 that differ: **ALTER through a view name (AUD-015)**; the carded DELETE-through-view; the documented GLOBAL refusal; a Unicode-14 fold (AUD-019) |
| ALTER-through-view probe (16 ALTER kinds × with and without a view × hadoop and memory catalogs × Spark), plus the same on `321c6095` | **12 ALTER kinds change the table where Spark refuses (AUD-015)**; identical on `321c6095` |
| Temp-view surface probe (16 names, 8 scenarios, a fresh session) | No reading view lost; differences are carded or pre-existing |
| Second audit's hint probe, byte-identical (`probe_hint_walk.py`) | AUD-007 and AUD-010 (shape 1) closed |
| Hint go-around probe vs Spark (45 cases) + scale (5,000 versions, 22,000 unrelated files) | Spark-equal except **an unreadable hint (AUD-018)** and **a catalog-style name with no hint (AUD-017)**; scale reads in 0.03–0.15 s |
| `register_table` of gzip and legacy-gzip metadata; custom `write.metadata.path`; RePark writes with codec gzip | Correct rows; the custom path refuses as Java does |
| Second audit's AUD-008 probes, byte-identical | `(1, {})` where Spark gives `(1, Row())`; no panic |
| Emptied-struct go-around vs Spark (17 shapes × formats), time travel, map-key collapse, COW/MoR DML | Per-row values correct in every Parquet shape; Avro is loud (`DataInvalid`, carded S-10); ORC nested is loud; **`m[struct key]` panics (AUD-016)** |
| Native-door emptied-struct read, fresh session (Rust, `audit3_native_struct_drop.rs`) | Per row in 3 shapes, as on the Spark door |
| Id-less and name-mapping probe vs Spark (10 histories, 8 ALTER kinds' mapping JSON) | Spark-equal except the ruled struct-group divergence and RePark admitting a file Spark's `add_files` refuses |
| Ordinary nested reads, both writers, before and after evolution, row filters, column metrics | Data identical across RePark catalog, RePark path and Spark path; NaN counts equal |
| First-audit probes A, B, C, D, E (A, B, D, E byte-identical; C import path only) | All five fixes hold |
| Second-audit regression probes (12 Python, 4 Rust) and the 3.46 GB volume run | Identical to the second audit's outputs except the temp-view steps the fix changed |
| Fork: probe D, the 4 eqdel probes, the 7 new fork suites (103 tests), `cargo test -p iceberg --lib` (4,672 passed, 9 ignored) | All pass |
| Fork: `cargo test -p iceberg-datafusion` (the fork's DataFusion provider) | 43 test binaries: 802 passed, 0 failed, 15 ignored (11m40s) |
| RePark pins: 3 Rust temp-view suites (27 tests), eqdel door suites, `hint_walk` unit tests, and 10 facade files with `REPARK_PARITY_LIVE=1` | 716 facade tests passed; every Rust suite passed |
| Records sweep (registry, label ledger C-073 to C-078, fork-sync, STATUS, cards, 95 pin bindings) | All 95 pins exist; stale merge states and three inaccurate claims (AUD-020) |

## 2. Verdict

**No: the format-v2 PRODUCTION label should not be re-dated yet. Blocking: AUD-015 (Critical; the AUD-006 family through `ALTER TABLE`) and AUD-016 (High; a library panic on an ordinary SELECT over a format-v2 table, blocking under the AUD-008 precedent unless the owner scopes dependency expression panics out). The carded silent-value residue F-IDLESS-TOPLEVEL-POSITIONAL-1, re-measured here, also needs an owner ruling under order section 1 before the re-date.**

The three fixes under audit are CLOSED on every route and variant I measured (section 3). The 13 changes the second audit verified, and the first audit's five findings, all HOLD (section 4). The blocking problems are next to the fixes, not in them:
- AUD-015: the temp-view name still resolves to the same-named table for `ALTER TABLE`. `DROP COLUMN` and `REPLACE COLUMNS` remove a column's values from the current table where Spark refuses with `EXPECT_TABLE_NOT_VIEW`. This is the same silent-data-loss class that blocked the label under AUD-006, and it is present at `321c6095`.
- AUD-016: a map subscript with a struct key panics in DataFusion's `get_field`, on Iceberg tables and on literal maps alike.
- F-IDLESS-TOPLEVEL-POSITIONAL-1: the label ledger carries this card's silent wrong values as an OPEN residue (C-078). Order section 1 says no carve-out covers a silent value, and no record removes that route from the label's claim.

## 3. The three fixes

### AUD-006: temporary view name sends writes to the table. **CLOSED** (on its six SQL routes and on `writeTo`)

**The second audit's reproductions, unchanged** (`probe_view_shadow.py` `ab8ee8e7…`, `probe_view_shadow2.py` `e6a949c4…`). Every statement refuses on RePark, and `hd.ns.t` stays `[1,2,3]` after each one:

```text
DELETE FROM t WHERE id = 2      rp [EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'DELETE' expects a table but `t` is a view   sp INTERNAL_ERROR
INSERT INTO t VALUES (40)       rp EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE                                            sp same class
INSERT OVERWRITE t VALUES (50)  rp EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE                                            sp same class
TRUNCATE TABLE t                rp EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE 'TRUNCATE TABLE'                           sp same class
UPDATE t … / MERGE INTO t …     rp UNSUPPORTED_FEATURE.TABLE_OPERATION                                             sp same class
SELECT id FROM hd.ns.t          rp [1,2,3]   sp [1,2,3]          SELECT id FROM t   rp [99]   sp [99]
```

**Around it** (`p3_view_matrix.py` `23aafdee…`, 158 cases; each case runs in its own namespace on both engines and reads the table fully qualified afterwards). The following match Spark: the same answer, and the table unchanged where Spark refuses.
- Spellings: `AS x` and bare aliases; leading block and line comments; lower-case keywords; `IN (SELECT …)`; `INSERT INTO TABLE`; column lists; `INSERT … SELECT`; `BY NAME`; parenthesised queries; `INSERT OVERWRITE TABLE`; `MERGE WITH SCHEMA EVOLUTION`; copy-on-write and merge-on-read.
- Frame writers: `writeTo(...).append()`, `.append()` with an option, `overwritePartitions()` and `overwrite(lit(True))` all refuse `_LEGACY_ERROR_TEMP_1011`. `createOrReplace`, `replace`, `insertInto` (plain and overwrite), `saveAsTable` (append and overwrite) and `format("iceberg").save()` write the table on both engines. Each also ran with the name in upper case.
- Case: a view `T` with target `t` and the reverse, quoted and unquoted, under both settings of `spark.sql.caseSensitive`; created under one setting and used under the other. For example, created as `T` under `true` then used as `t` under `false`: both engines write the table. Used as `T` under `false`: both refuse `TABLE_OR_VIEW_NOT_FOUND`.
- Non-ASCII: `TÄ`/`tä`, `TΣ`/`tς` and `Tİ`/`ti̇`, under both settings and across a switch.
- Other shapes: two-part, three-part and quoted two-part names write the table on both engines. A view created before the table. A view over a frame (`createOrReplaceTempView`) and a view over the table itself. `USE` of another namespace that holds its own `t` between the view's creation and the write: both refuse, and both tables are unchanged.
- Refused on RePark only, in loud refusals: spellings RePark does not support at all (`IDENTIFIER('t')` targets, `WITH … INSERT`, `FROM … INSERT`, `EXECUTE IMMEDIATE`). Spark also refuses each of them through the view. No table changes.
- The fresh session (a new session plus `register_table`, which has no view in scope) writes the table, as Spark's `newSession()` does.

**Differences from Spark in the matrix:**
- `ALTER TABLE` through the view name is AUD-015 (new).
- `DELETE FROM v WHERE id = 2` through a view over the table: Spark deletes (`[1,3]`) and RePark refuses with the table unchanged. This is exactly card SPARK-DOOR-DELETE-THROUGH-VIEW-1.
- `CREATE GLOBAL TEMPORARY VIEW` and `createOrReplaceGlobalTempView` refuse on RePark as documented (`NotImplemented`), where Spark answers.
- A view named with a letter added in Unicode 14 (U+A7C0) is AUD-019 (new, Low).

The DDL-OVER-TEMP-VIEW-1 card says Spark's answer is unmeasured. Measured here: `DROP TABLE t` and `DROP TABLE IF EXISTS t` drop the view on both engines and leave the table. `CREATE OR REPLACE TABLE t AS SELECT 70` and `REPLACE TABLE t AS SELECT 70` replace the table on both engines, and `t` still reads the view (`[99]`). Both shapes are Spark-equal.

**Pins:** the three Rust suites passed (`temp_view_write_target` 10, `temp_view_name_case` 5, `temp_view_name_fold` 12), as did the three facade files (in the 716).

### AUD-007 (and AUD-010): a stale hint with gzip-named metadata. **CLOSED**

**The second audit's reproduction, unchanged** (`probe_hint_walk.py` `3c570969…`):

```text
spark_mixed_codec_current_hint   repark [[2],[3]]   spark [[2],[3]]
spark_mixed_codec_stale_hint2    repark [[2],[3]]   spark [[2],[3]]      (was repark [[1],[2],[3]])
spark_gzip_current_hint          repark [[2],[3]]   spark [[2],[3]]      (AUD-010 shape 1; was a refusal)
```

**Around it** (`p3_hint.py` `97d31d30…`, `p3_hint2.py` `68c12ac4…`; Spark-written mixed-codec table `v1, v2, v3.gz…v6.gz`, truth `[3],[4]`; every case on a copy, with Hadoop's `.version-hint.text.crc` removed when the hint is rewritten so that Spark really reads the hint):
- These hints match Spark: behind by many (`1`), behind by `2`, behind by `3`, behind by one (`5`), current (`6`), `"6\n"`, `"5\n"`, `"5\r\n"`, `" 5 "`, `"05"`, `"+5"`, empty, garbage, and no hint file. All-gzip table, hints `1` to `4`: equal.
- Past the newest (`9`, `7`): both refuse. RePark says `version-hint.text names 'v9.metadata.json', which does not exist`; Java says `Metadata file for version 9 is missing`.
- Both names present for one version (a plain `v6.metadata.json` that is a stale copy beside `v6.gz.metadata.json`): both engines prefer the plain name, with hint `6`, with hint `5` and with no hint.
- Java's legacy `vN.metadata.json.gz` names, with hint current, hint stale and no hint: equal.
- Unrelated files (`v7.metadata.json.tmp`, `v8.metadata.json.bak`, `notes.txt`, `vX.metadata.json`, a bare `<uuid>.metadata.json`): equal, with and without a hint. A catalog-style `NNNNN-<uuid>.metadata.json` with no hint is AUD-017.
- The second shape of AUD-010 (cleanup removed the hinted version): with the crc sidecar removed, both engines refuse. With the sidecar stale, Spark falls back to the listing. This confirms the records' "probe artefact" explanation (C-074).
- An unreadable hint (mode 000) is AUD-018: RePark refuses where Spark answers.
- After a failed commit: a refused `CREATE TABLE` at an existing location leaves a `00000-<uuid>.metadata.json` orphan, and the no-hint read still answers the newest (`[2],[3]`). A conflicting commit after `register_table` of an older version refuses with `CatalogCommitConflicts`, writes no metadata file, and the path read still answers the newest.
- `register_table` of `v6.gz.metadata.json` and of `v6.metadata.json.gz` reads `[3],[4]`. A write after it produces `v7.metadata.json` with hint `7`, and both engines' path reads answer `[3],[4],[5]`.
- RePark writes plain names even with `write.metadata.compression-codec=gzip`; both engines read the result. A custom `write.metadata.path` on a hadoop table refuses on both engines (`Hadoop path-based tables cannot relocate metadata`); RePark leaves one orphan `00000-<uuid>.metadata.json` in the custom directory.
- Scale (`p3_hint_scale.py` `a341db14…`): 5,000 gzip-named versions plus 20,000 unrelated files plus 2,000 `.tmp` names. RePark reads correctly in 0.04 s (hint current), 0.03 s (behind by one), 0.10 s (hint `1`, a 5,000-step walk) and 0.15 s (no hint); Spark takes 0.14–2.6 s.

**Pins:** `cargo test -p repark-core --lib hint_walk` passed; `test_iceberg_load_path.py` passed (in the 716).

### AUD-008: emptied struct panics in the fork reader. **CLOSED** (no panic in any shape; per-row values right against the data)

**The second audit's reproductions, unchanged** (`probe_struct_drop.py` `83e69919…`, `probe_struct_drop_bt.py`): `SELECT * FROM hd.ns.t` after `DROP COLUMN s.k` gives RePark `[[1, {}]]` and Spark `[[1, Row()]]`. The two-child shape gives the same. The backtrace probe prints `s: struct<>`, `is_valid: all not null`, with no panic.

**Around it** (`p3_struct.py` `e7f8a17d…`, `p3_struct2.py` `29a0a87d…`, `p3_struct_cow.py` `26d4d954…`, `audit3_native_struct_drop.rs` `25bb7c78…`). The read answers below were measured on both engines; where Spark answers differently, the difference is named.
- One-child struct with `(1,{k:10}), (2,NULL), (3,{k:NULL})` in one write: RePark `[{}, NULL, {}]`. Spark gives NULL on every row, the ruled per-write collapse (S-13). `s IS NULL` gives RePark `[2]` and Spark `[1,2,3]`, consistent with each engine's read.
- The same rows in three files: both engines `[{}, NULL, {}]`.
- Two children, both dropped: both `[{}, NULL]`. Two children, one dropped: both keep `j`.
- Nested `s.a.k` dropped: both `{a:{}, b:10}, {a:NULL, b:20}, NULL`. Three levels deep: equal.
- `ARRAY<STRUCT<k>>` after `DROP COLUMN arr.element.k`: RePark `[[{}, NULL, {}], NULL, []]`, which keeps the list length and each element's validity. Spark Parquet reads `[]` for the three-element row (Spark Avro reads `[{}, NULL, {}]`, as RePark does). RePark is the one that is right against the data.
- `MAP<STRING, STRUCT<k>>` after `DROP COLUMN m.value.k`: RePark `{a:{}, b:NULL}`; Spark fails the read (`IllegalArgumentException`).
- A child added after the drop: both `{j:NULL}` for old rows and `{j:40}` for new rows. The same name re-added: both `{k:NULL}`.
- A struct added later and then emptied: both read NULL where the file lacks it.
- 20,000 rows over pages and row groups (`page-row-limit=100`, 4 KiB row groups), every third row NULL: RePark has 6,667 NULL, 13,333 `{}`, 0 wrong. Spark gives all NULL (the per-write rule).
- Time travel to the last pre-drop snapshot and to the first snapshot: equal on both engines (`{k:10}`, `{k:NULL}` and NULL as written). `DESCRIBE` is equal.
- Writes into the emptied struct refuse on both engines.
- Merge-on-read DELETE and MERGE delete succeed on both. Copy-on-write DELETE, UPDATE and MERGE that must rewrite the file refuse on RePark (`Parquet does not support writing empty structs`), with 1 snapshot and the rows unchanged.
- Native door, fresh session (`register_table`), three shapes: per row, the same as the Spark door. `s IS NULL` gives `[2,4]` and `count(*)` gives 4.
- Avro data: loud `DataInvalid => Failed to build Arrow StructArray … try_new_with_length` (fork S-10), never a wrong row. ORC nested reads refuse loud (`FeatureUnsupported`), and Spark also fails reading ORC after the drop.
- Map key: `DROP COLUMN m.key.k` is accepted on RePark and refused on Spark (`Cannot alter map keys`), as F-MAP-KEY-DROP-ALTER-1 says. The card's "the read is correct" leaves out that two distinct keys `{k:1,j:2}` and `{k:3,j:2}` collapse into a map with two equal keys `{j:2}` (`cardinality` 2). See AUD-020, item 5.

**Pins:** `test_ice_struct_empty_child_1.py` passed (in the 716); the fork suite `struct_empty_child` passed (8).

## 4. Regression table (18 rows)

Prefixes: `CAP` = the cap prefix above; `SPARK` = `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1`; `PY` = `<scratch>/audit3-repark/.venv/bin/python`. Python probes ran from `<scratch>/audit3-work/probes/`. "Identical" means the output lines equal the second audit's logs (`<scratch>/audit2-work/*.log`) once temp paths and Py4J object ids are normalised.

| # | Change or finding | Verdict | Measured with |
|---|---|---|---|
| 1 | #1203 RP-POSDEL-BATCH-2 | **HOLDS** | `CAP <scratch>/audit3-work/volume-run.sh` (1,000,000 rows, 999,990 matched, 3.46 GB path text). DELETE `>=` ok in 49 s at 4.44 GiB; `BETWEEN` ok in 18 s at 7.68 GiB; UPDATE ok in 50 s at 4.48 GiB, survivors exact. The subquery DELETE hits the documented valve; MERGE refuses (`Not enough memory to continue external sort`) with 1 snapshot. Same as the second audit. |
| 2 | #1204 audit filed, label withdrawn | **HOLDS** | `docs/audits/` holds both audits; the order and STATUS say WITHDRAWN. |
| 3 | #1205 hadoop hint walk | **HOLDS** | `probe_public_paths.py` (ids 2,3, identical); `probe_hint_walk.py` `rp_table_*` cases are the same as the second audit. |
| 4 | #1206 post-publish outcome, resolver, branch ordering | **HOLDS** | `cargo test -p repark-spark --test audit2_postpublish_ops` (30/30 `OK_COMMITTED`); `probe_dml_targets.py` steps 0–10 and 14–24 identical (steps 11–13 now refuse, the AUD-006 fix); `probe_branch_named_tables.py` identical. |
| 5 | #1207/#1208 re-date records | **HOLDS with a note** | Superseded by the withdrawal. The hint-window ledger still says it moved "on the merge, 2026-10-29" (`task/ledgers/completed/ice-hadoop-hint-window-1-ledger.md:7`); the squash `9ab67d7d` is 2026-10-28 (AUD-012 item 3, unchanged). |
| 6 | #1209 repin to `56aa8913`, guard reverted | **HOLDS** | `probe_delete_routes.py`: 28/28 states equal across five readers, identical; `probe_delete_formats.py`: Parquet, Avro and ORC all read 0,4. |
| 7 | #1210 pulldown-cmark 0.13.5 | **HOLDS** | `Cargo.lock:4761-4762` reads 0.13.5. `cargo audit` was not run (the advisory database needs GitHub). |
| 8 | #1211 cross-read gate narrowed | **HOLDS** | `PYTHONPATH=…/tests $PY probe_cross_read_gate_a3.py` exits 1 with the intended `AssertionError` (`ConnectionError` rejected); `test_acceptance_cross_read_local.py` and `test_cross_read_gate.py` passed live (in the 716). |
| 9 | #1212 records | **HOLDS** | The cited card files exist (for example `f-ci-macos-cap-1-card-2026-10-29.md`). |
| 10 | #1213 repin to `935cc55c`, eqdel door pins | **HOLDS** | `cargo test -p repark-sql --test eqdel_dropped_key_door --test native_door_delete_format` passed; `--test audit2_eqdel_dml`: 16/16, `resurrected=false`. |
| 11 | Fork #375 Avro/ORC delete loader | **HOLDS** | Rows 6 and 10; `cargo test -p iceberg --lib` (4,672 passed). |
| 12 | Fork #377 dropped/nested/re-added keys | **HOLDS** | `cargo test -p iceberg --test audit_dropped_eq_key` (probe D, `3f94ab33…`) passed; `--test audit2_eqdel_probe`: 4 passed. |
| 13 | Still-open cards of the second audit | **HOLDS as carded** | `probe_maint_formats*.py`: Avro and ORC still leave 1 delete file where Spark leaves 0 (AUD-013), and `rewrite_position_delete_files` gives 0 on Avro and ORC; the rest is identical apart from byte sizes. `probe_wap_late_and_textleaf.py`, `probe_wap_case.py` and `probe_textleaf.py`: identical. |
| 14 | AUD-001 stale hint | **HOLDS** | `probe_public_paths.py` (`44a696b3…`, byte-identical): the stale-hint path read gives `[2],[3]`. |
| 15 | AUD-002 CTAS after publish | **HOLDS** | `cargo test -p repark-spark --test audit_postpublish` (Appendix E, `e9f043f3…`, byte-identical): `CTAS_RESULT=Ok(())`, 1 row, exit 0 (it was exit 101). |
| 16 | AUD-003 Avro/ORC MoR deletes | **HOLDS** | `probe_delete_formats.py` (`52a2cc29…`, byte-identical): all three formats read 0,4. |
| 17 | AUD-004 dropped equality-delete key | **HOLDS** | Probe D: `POST_DROP_RESULT=Ok(…)` gives `a,c,e`, exit 0. |
| 18 | AUD-005 cross-read gate | **HOLDS** | Row 8. |

**Carded items the owner's brief asks me to confirm are still only what their cards say.** Measured: AUD-009 (identical); AUD-011 (identical: native CTAS invisible, TRUNCATE and ADD COLUMN `ERR_BUT_COMMITTED`); AUD-013 (identical); AUD-014 (RePark keeps `v1…v6`); HADOOP-HINT-NEGATIVE-1 (hint `-1`: RePark answers, Spark refuses). F-IDLESS-TOPLEVEL-POSITIONAL-1 is re-measured: `(a INT, b STRING)` over a file `(b, a)`, after the UNSET, reads `[(None,'1'),(None,'2')]` and `WHERE a = 1` counts 0, where Spark refuses; `(a INT, b DATE)` reads `[(18263, 1970-01-02)]`; a same-type swap is Spark-equal. These are exactly the card's answers, and they are silent wrong values (see the verdict). Also measured: ADD-FILES-IDLESS-PROMOTION-1 (an incompatible nested file refuses at `add_files` with the internal statistics text); F-MAP-KEY-DROP-ALTER-1 (with the gap in AUD-020); fork S-10; SPARK-DOOR-DELETE-THROUGH-VIEW-1; TEMP-VIEW-NAME-VALIDATION-1 item 1 (`table('1t')` refuses); TEMP-VIEW-SURFACE-LEADS-1 leads 2 and 3. DDL-OVER-TEMP-VIEW-1 was measured Spark-equal (section 3). Not re-measured: F-NESTED-TYPE-MISMATCH-EMBEDDED-IDS-1, WRITER-PATH-TEMP-VIEW-TARGET-1's Rust-door bare names, MERGE-SCHEMA-EVOLUTION-SPELLING-1, the fork id-less ledger residue except S-17 (below), F-EQDEL-DROPPED-KEY-2, F-EQDEL-PROMOTED-KEY-POS-1, the REPLACE WHERE cards, and ENC-1.

**What the fixes may have disturbed: nothing found.** The checks behind that statement:
- #1219 (temp views): no view lost readability; `dropTempView` folds as Spark does; all 16 `createOrReplaceTempView` names are accepted or refused as Spark does, apart from the carded items. `catalog.listTables()` lists temp views where Spark (with a non-session current catalog) lists only tables; this is identical at `321c6095`, so pre-existing.
- #1218 (hint walk): no slowdown at 5,000 versions; unrelated files are ignored.
- Fork #378/#379/#380, ordinary nested reads (`p3_nested_regress.py` `beebd976…`). Spark-written and RePark-written tables with `struct`, `struct<struct>`, `array<struct>`, `map<string,struct>`, `array<array>` and `struct<array<map>>`, with NULLs at each level and NaN, read identically through RePark's catalog, RePark's path read and Spark's path read. That holds before and after a nested rename, `int→bigint` promotion, a nested `FIRST` reorder, an added nested field, and a list-element rename. Filters on `s.k > 5`, `s.a.b = 'x'`, `s.a IS NULL` and `isnan(s.a.c)` agree.
- NaN counts in `.files` agree between writers. RePark's `null_value_counts` and `value_counts` for a double nested under an optional struct count parent-null rows, where Spark's do not. RePark's reader answers `s.a.c IS NULL` and `s.a.c > 1.0` correctly on both writers' files. Spark's own reader misses rows 2 and 3, and the NaN row 4, on its own file (Spark-side, not a RePark finding).
- An ALTER on a table with no mapping emits no `schema.name-mapping.default` (both engines). The mapping JSON after ADD, RENAME, promote, move, DROP, re-add of a name, and SET TBLPROPERTIES is canonically equal to Spark's. That covers the measurable part of fork S-17. INSERT, MERGE and DELETE on an `add_files` table read the same as Spark.
- When an id-less file holds two columns that map to one id (`k` and `k2` after a rename), RePark admits the file and reads the last column, and filters agree with that. Spark's `add_files` refuses the same file (`Multiple entries…`), so the state is reachable only through RePark.
- The fork's own DataFusion provider: `cargo test -p iceberg-datafusion` gives 802 passed, 0 failed, 15 ignored across 43 binaries.

## 5. New findings

### AUD-015: `ALTER TABLE` through a temporary view's name changes the same-named Iceberg table, where Spark refuses

**Severity: Critical** (silent data loss by an ordinary statement: `DROP COLUMN`, `DROP COLUMNS` and `REPLACE COLUMNS` remove a column's values from every current read). This is the AUD-006 calibration, which counts deleted or overwritten values as data loss. **Repository: RePark, Spark door.** Pre-existing: identical at `321c6095` (`p3_view_alter_base321.log`).

**Reproduction:** `p3_view_alter.py` (`e9786ecb…`); RePark on hadoop catalog `hd` and memory catalog `mem`, Spark on its own warehouse:

```sql
CREATE NAMESPACE hd.a2v;
CREATE TABLE hd.a2v.t (id INT, v STRING) USING iceberg TBLPROPERTIES ('format-version'='2','k0'='x');
INSERT INTO hd.a2v.t VALUES (1,'a'),(2,'b'),(3,'c');
USE hd.a2v;
CREATE OR REPLACE TEMPORARY VIEW t AS SELECT 99 AS id;
ALTER TABLE t DROP COLUMN v;
--  RePark: ok; SELECT * FROM hd.a2v.t -> [[1],[2],[3]] (schema ['id']); SELECT * FROM t -> [[99]]
--  Spark : [EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'ALTER TABLE ... DROP COLUMNS' expects a table but `t` is a view;
--          SELECT * FROM hd.a2v.t -> [[1,'a'],[2,'b'],[3,'c']]
ALTER TABLE t REPLACE COLUMNS (id INT);     -- RePark: ok, table reads [[None],[None],[None]]; Spark: EXPECT_TABLE_NOT_VIEW
```

The same happens on both catalogs, while `SELECT * FROM t` keeps reading the view. Per ALTER kind, with view `t` in scope (RePark / Spark):
- `ADD COLUMN`, `ADD COLUMNS`, `RENAME COLUMN`, `ALTER COLUMN … TYPE BIGINT`, `ALTER COLUMN … COMMENT`: applied to the table / `EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE`.
- `SET TBLPROPERTIES` (for example `write.delete.mode`) and `UNSET TBLPROPERTIES`: applied / `EXPECT_TABLE_NOT_VIEW.USE_ALTER_VIEW`.
- `RENAME TO t_renamed`: on the memory catalog RePark renames the table, so `hd`'s `t` disappears. Spark renames the temp view and leaves the table. On the hadoop catalog RePark refuses with the catalog's `Cannot rename Hadoop tables`.
- Spark-equal: `ADD PARTITION FIELD` and `WRITE ORDERED BY` (both engines change the table); `SET IDENTIFIER FIELDS` (both refuse).

**Expected:** Spark 4.1.2's answers above: the refusal, with the table unchanged.

**Already named:** no. DDL-OVER-TEMP-VIEW-1 names `CREATE OR REPLACE TABLE` and `DROP TABLE` only, and both are Spark-equal (section 3). SPARK-DOOR-TEMP-VIEW-TARGET-1's refusal covers the six write heads (`temp_write.rs:12`, `WRITE_HEADS`).

### AUD-016: a map subscript with a struct key panics (`m[named_struct(...)]`)

**Severity: High** (library panic reachable by an ordinary SELECT; caught at the facade as an internal error). **Repository:** RePark, through DataFusion 54.1.0. Not Iceberg-specific. Pre-existing at `321c6095` (`p3_mapkey_panic_base321.log`).

**Reproduction:** `p3_mapkey_panic.py` (`68f6065d…`) and `p3_mapkey_bt.py` (`RUST_BACKTRACE=1`):

```sql
CREATE TABLE hd.ns.c (id INT, m MAP<STRUCT<k: INT, j: INT>, INT>) USING iceberg TBLPROPERTIES ('format-version'='2');
INSERT INTO hd.ns.c VALUES (1, map(named_struct('k', 1, 'j', 2), 7, named_struct('k', 3, 'j', 2), 8));
INSERT INTO hd.ns.c VALUES (2, map(named_struct('k', 1, 'j', 5), 9));
SELECT id, m[named_struct('k', 1, 'j', 2)] FROM hd.ns.c;
--  RePark: PySparkException: repark internal error in PyDataFrame.__arrow_c_stream__.next: the length + offset of the
--          sliced BooleanBuffer cannot exceed the existing length (a Rust panic was caught at the Python boundary; …)
--  Spark : [(1, 7), (2, None)]
SELECT id, m[named_struct('k', 3, 'j', 2)] FROM hd.ns.c WHERE id = 1;   -- RePark: "Join Error"; Spark [(1, 8)]
SELECT m[named_struct('k', 1, 'j', 2)] FROM (SELECT map(named_struct('k', 1, 'j', 2), 7) AS m);  -- RePark: same panic; Spark [7]
SELECT id, element_at(m, named_struct('k', 1, 'j', 2)) FROM hd.ns.c;   -- both [(1, 7), (2, None)]  (control)
SELECT id, m['a'] FROM <a MAP<STRING, INT> table>;                     -- both correct (control)
```

The backtrace runs `arrow-buffer-58.4.0/src/buffer/boolean.rs:516` ← `BooleanArray::slice` ← `datafusion-functions-54.1.0/src/core/getfield.rs:134` ← the expression simplifier (`expr_simplifier.rs:725`).

**Expected:** Spark's values. The production definition lists panics in library code as a disqualifier, and the order treated AUD-008 as one.

**Already named:** no. Grep for `BooleanBuffer`, struct map keys and subscripts in the registry: no row.

### AUD-017: with no readable hint, the path read prefers a catalog-style `NNNNN-<uuid>.metadata.json` above the newest `vN` and returns an older state

**Severity: Low** (silent wrong value, but only under a precondition that no ordinary RePark or Spark flow I found produces; it would rate Critical if one did). **Repository: RePark** (`crates/repark-core/src/iceberg_path.rs`, `metadata_file_order`, the `{leading}-{uuid}` arm). Pre-existing at `321c6095`.

**Reproduction:** `p3_catstyle_nohint.py` (`492faec6…`, RePark only, both builds) and `p3_hint.py` case `orphans_nohint_plus_catalog_style_00099` (with Spark):

```text
RePark table v1..v3, rows after DELETE: [2],[3]; remove version-hint.text
add 00000-<uuid>.metadata.json (copy of v2)   path read -> [[2],[3]]          (correct)
add 00009-<uuid>.metadata.json (copy of v2)   path read -> [[1],[2],[3]]      (deleted row returned)
Spark-written table v1..v6.gz, no hint, plus 00099-<uuid>.metadata.json (copy of v1):
                                              repark [] ; spark [[3],[4]]
```

**Expected:** Java's `HadoopTableOperations` considers only `v<N>` names on a hadoop table (Spark answers the newest above). **Already named:** the registry's DF-LOAD sentence records that the listing accepts both forms (registry diff line 7, 2026-10-29) without the consequence of mixing them.

### AUD-018: an unreadable `version-hint.text` makes the path read refuse, where Spark lists and answers

**Severity: Low** (a refusal of a read Spark answers; the precondition is an operational fault). **Repository: RePark** (`resolve_metadata_location`: an `exists` check, then an error from `read_version_hint` is propagated).

**Reproduction:** `p3_hint.py` case `mixed_hint_unreadable` (hint `2`, mode 000):

```text
repark: PySparkException: DataInvalid => Failed to read file …/version-hint.text: Permission denied (os error 13)
spark : [[3],[4]]
```

**Expected:** Java's `findVersion` catches the read failure and lists the directory. **Already named:** no.

### AUD-019: a view name with a letter newer than the JVM's Unicode folds on RePark and not on Spark

**Severity: Low** (loud; RePark refuses writes that Spark executes; the outcome depends on the JVM). **Repository: RePark** (`temp_view_key`, Rust `str::to_lowercase`).

**Reproduction:** `p3_view_matrix.py` cases `X.ci.newuni.*`. View `` `TꟀ` `` (U+A7C0, added in Unicode 14), table `` `tꟁ` ``, default case rule. Per statement (RePark / Spark):
- `DELETE FROM `` `tꟁ` `` WHERE id = 2`: RePark `EXPECT_TABLE_NOT_VIEW`, table `[1,2,3]` / Spark ok, table `[1,3]`.
- `INSERT`, `UPDATE`, `TRUNCATE`: the same pattern (RePark refuses; Spark writes).

The same shapes with `Ä`, final sigma and dotted `İ` are equal on both engines.

**Expected:** Spark on JDK 17, Unicode 13, which does not fold U+A7C0. On JDK 21 Spark would fold it, which I did not measure. **Already named:** no. The name rule in C-075 does not bound the Unicode version.

### AUD-020: records

**Severity: Low** (records). **Repository: RePark.**
1. **Stale merge state.** RP-64 merged as `d0562058` (2026-11-01 17:33 -05:00), but four records still describe it as pending:
   - `STATUS.md:45` says the id-less fix is "on branch `chore/rp-64-idless-struct-repin`, awaiting verification and merge". It names RP-64's first pin only (fork #379, not #380), and the section heading is still "Iceberg maturity (2026-10-27)".
   - Label ledger C-078 says "(PR #1228; commits …, not yet merged)", and the last VERDICT line names the branch.
   - The registry's ICE-NESTED-DDL-1 row dated 2026-11-01 says "verification and merge pending on branch `chore/rp-64-idless-struct-repin`, PR #1228".
   - C-077 says "Dispatched 2026-11-01 on the fork …, not merged", while C-078 closes it.
2. **A sentence in `docs/fork-sync.md:71` (RP-64 fix round 1)** says "a dropped field keeps its id with no name". Measured (`p3_idless.py`, `alter_kinds_with_mapping`): after `DROP COLUMN w` the mapping keeps `{field-id 5, names ["w"]}`, its id and its name, on both engines, as Java's "a delete removes nothing" does.
3. **The emptied-struct records cover structs only.** ICE-NESTED-DDL-1 (2026-10-31) and C-076 record no Spark measurement for structs inside lists or maps. Spark Parquet reads `[]` for a three-element `ARRAY<STRUCT<k>>` row after `DROP COLUMN arr.element.k`, and Spark fails reading `MAP<STRING, STRUCT<k>>` after `DROP COLUMN m.value.k`. RePark reads both per element, correctly. These are divergences in RePark's favour, unrecorded.
4. **DDL-OVER-TEMP-VIEW-1 says Spark's answer is unmeasured.** This pass measured both of its shapes Spark-equal (section 3). The card could retire on that measurement; AUD-015 is the part it does not cover.
5. **F-MAP-KEY-DROP-ALTER-1 calls the read after the drop "correct".** The drop can collapse distinct keys into equal ones. Measured: `{k:1,j:2}` and `{k:3,j:2}` both become `{j:2}`, in a map of `cardinality` 2. A map with two equal keys is not one Spark can hold, so it is not a correct read of the dropped schema.
6. **Unchanged from AUD-012 item 3:** `ice-hadoop-hint-window-1-ledger.md:7` gives the merge date as 2026-10-29; the squash is 2026-10-28.

What holds:
- All 95 distinct pin bindings in the five sidecars (`ice-v2-label-1`, `rp-63-struct-empty-repin-1`, `rp-64-idless-struct-repin-1`, `spark-door-temp-view-target-1`, `ice-hadoop-gzip-metadata-1`) name an existing file and function.
- The three I opened test what their clause says (C-004's write-options pin, C-001, and RP-64's C-022).
- The registry, the label ledger, the order and the fork's ledgers name every residue in section 2 of the brief.
- Each repin changes only the six fork `source` lines of `Cargo.lock` (and the five `rev` lines of `Cargo.toml`): `235f5740` and `d0562058` each show 6 removed and 6 added lines, and nothing else.
- C-073 to C-076 match my measurements, including C-074's crc explanation and C-075's route list.
- C-076's "pins red on `935cc55c`" and C-078's "red on `14a93602` / `ee592dcf`" were not re-measured (that needs builds at the old pins).

## 6. Leads (not reproduced; no severity)

| Lead | Evidence | What would prove or dismiss it |
|---|---|---|
| F-NESTED-TYPE-MISMATCH-EMBEDDED-IDS-1 is a carded silent NULL | Card text; not re-measured (it needs a foreign file with embedded ids and an unpromotable nested type) | A Spark-written file with `s<d INT>` ids registered under metadata that types `d` STRING; read on both engines. If reproduced, order section 1 applies as for the positional card. |
| A refused copy-on-write DML on an emptied struct leaves a data file behind | `p3_struct_cow.py`: `new_files: 1` under `data/` after each refused COW DELETE, UPDATE and MERGE; table unchanged, 1 snapshot | List the file, check that no manifest references it, check `remove_orphan_files`. An orphan is acceptable under the local rule if it is listed. |
| A refused `CREATE TABLE` (fresh session, or a custom `write.metadata.path`) leaves a `00000-<uuid>.metadata.json` orphan | `p3_fresh_create.py`, `p3_hint2.py` | The same orphan-listing check. Combined with a high-numbered orphan it is AUD-017's precondition. |
| REST-shaped commits of a schema update (one `SetProperties` beside `AddSchema` and `SetCurrentSchema`) | The fork unit tests cover the update list (`update_schema_mapping_tests.rs`); no REST catalog ran here | An ALTER against a REST catalog fixture, inspecting the commit request body. |
| SQL-surface gaps seen while probing nested reads (not Iceberg) | `arr[0].x` and `m['a'].p` refuse ("Dot access not supported for non-string expr"); `ll[0][1]` out of bounds answers NULL where Spark under ANSI raises `INVALID_ARRAY_INDEX` | Registry grep plus Spark measurement per expression. |
| Spark's own metrics prune misses `s.a.c IS NULL` rows on Spark-written files | `p3_nested_nullprune.py` (Spark side only) | Not a RePark defect. RePark answers correctly from both writers' files. |
| `writeTo` with options through a view on the Rust door alone; Rust-door bare names (WRITER-PATH-TEMP-VIEW-TARGET-1) | Not probed | The card's Rust-door probe. |

## 7. Commands run, with exit codes

`CAP`, `SPARK` and `PY` as in section 4; `S` = the scratch root; `W` = `<scratch>/audit3-work`; Python probes ran from `<scratch>/audit3-work/probes`.

```text
CAP <scratch>/audit3-work/build-native.sh   (uv sync --locked … ; uvx maturin@1.14.1 develop)                                 exit 0
CAP SPARK $PY probe_view_shadow.py ; CAP SPARK $PY probe_view_shadow2.py                                     exit 0, 0
CAP SPARK $PY p3_view_matrix.py A.del F.writeTo.append. C.cs.viewT.target_t.del X.ci.uml.del (smoke)        exit 0
CAP SPARK $PY p3_view_matrix.py                                                                             exit 0
CAP SPARK $PY p3_view_alter.py                                                                              exit 0
CAP <scratch>/audit2-repark/.venv/bin/python p3_view_alter.py --no-spark   (69b962d0 clone no longer exists)       exit 127
CAP <scratch>/audit2-work/repark-base/.venv/bin/python p3_view_alter.py --no-spark   (321c6095)                    exit 0
CAP SPARK $PY p3_view_surface.py                                                                            exit 0
CAP $PY p3_newsession.py ; CAP $PY p3_fresh_create.py                                                        exit 0, 0
CAP SPARK $PY probe_hint_walk.py                                                                            exit 0
CAP SPARK $PY p3_hint.py   (stopped at the custom write.metadata.path CREATE: Spark refuses it; the cases before it are recorded)   exit 1
CAP SPARK $PY p3_hint2.py                                                                                   exit 0
CAP SPARK $PY p3_hint_scale.py 5000 20000                                                                   exit 0
CAP SPARK $PY probe_struct_drop.py ; CAP RUST_BACKTRACE=1 $PY probe_struct_drop_bt.py                        exit 0, 0
CAP SPARK $PY p3_struct.py   (probe bug: JSON key on map keys)                                              exit 1
CAP SPARK $PY p3_struct.py   (probe fixed)                                                                  exit 0
CAP SPARK $PY p3_struct2.py                                                                                 exit 0
CAP SPARK $PY p3_mapkey_panic.py --spark ; CAP <scratch>/audit2-work/repark-base/.venv/bin/python p3_mapkey_panic.py   exit 0, 0
CAP RUST_BACKTRACE=1 $PY p3_mapkey_bt.py                                                                    exit 0
CAP <scratch>/audit3-work/run-regress-py.sh   (sequential, SPARK set):                                                         exit 0
    probe_public_paths 0, probe_delete_formats 0, probe_cross_read_gate_a3 1 (ModuleNotFoundError: _acceptance, probe setup),
    probe_delete_routes 0, probe_dml_targets 0, probe_catalog_ns_collision 0, probe_maint_formats 0,
    probe_maint_formats_avro_orc 0, probe_wap_case 0, probe_wap_late_and_textleaf 0, probe_textleaf 0, probe_branch_named_tables 0
CAP PYTHONPATH=<scratch>/audit3-repark/python/repark/tests $PY probe_cross_read_gate_a3.py   (intended AssertionError)   exit 1
cd <scratch>/audit3-repark && CAP <scratch>/audit3-work/repark-rust-tests.sh                                                          exit 0
    cargo test --locked -p repark-spark --test audit_postpublish -- --nocapture                    0
    cargo test --locked -p repark-spark --test audit2_postpublish_ops -- --nocapture               0
    cargo test --locked -p repark-spark --test temp_view_write_target --test temp_view_name_case --test temp_view_name_fold   0
    cargo test --locked -p repark-sql --test audit2_native_postpublish -- --nocapture --test-threads=1   0
    cargo test --locked -p repark-sql --test audit2_eqdel_dml -- --nocapture                       0
    cargo test --locked -p repark-sql --test eqdel_dropped_key_door --test native_door_delete_format   0
    cargo test --locked -p repark-core --lib hint_walk                                             0
cd <scratch>/audit3-fork && flock <lock> CAP CARGO_INCREMENTAL=0 <scratch>/audit3-work/fork-tests.sh                     exit 0
    cargo test --locked -p iceberg --test audit_dropped_eq_key -- --nocapture                      0
    cargo test --locked -p iceberg --test audit2_eqdel_probe -- --nocapture --test-threads=4       0
    cargo test --locked -p iceberg --test struct_empty_child --test struct_idless_child{,_2,_3,_4} --test name_mapping_schema_update{,_2}   0
    cargo test --locked -p iceberg --lib                                                           0 (4672 passed, 9 ignored)
CAP <scratch>/audit3-work/volume-run.sh   (ge 0, between 0, in_subquery 0, update 0, merge 0)                                  exit 0
CAP SPARK $PY p3_idless.py                                                                                  exit 0
CAP SPARK $PY p3_nested_regress.py ; same after a probe normalisation fix                                   exit 0, 0
CAP SPARK $PY p3_nested_nullprune.py                                                                        exit 0
CAP <scratch>/audit3-work/pytest-pins.sh   (SPARK REPARK_PARITY_LIVE=1 PYTHONPATH=python/repark-parity/src pytest of 10 files: the three RP-63/64
    facade pins, test_iceberg_load_path, the three temp-view facade pins, test_acceptance_cross_read_local,
    test_ice_orc_avro_1, test_cross_read_gate)   716 passed                                                 exit 0
cd <scratch>/audit3-repark && CAP cargo test --locked -p repark-sql --test audit3_native_struct_drop -- --nocapture   exit 101 (probe compile error, E0716)
    same after the probe fix                                                                                exit 0
CAP SPARK $PY p3_struct_cow.py                                                                              exit 0
CAP $PY p3_catstyle_nohint.py ; CAP <scratch>/audit2-work/repark-base/.venv/bin/python p3_catstyle_nohint.py       exit 0, 0
CAP SPARK $PY p3_positional.py                                                                              exit 0
CAP $PY p3_listtables.py ; CAP <scratch>/audit2-work/repark-base/.venv/bin/python p3_listtables.py                 exit 0, 0
cd <scratch>/audit3-fork && flock <lock> CAP CARGO_INCREMENTAL=0 <scratch>/audit3-work/fork-df-tests.sh  (cargo test --locked -p iceberg-datafusion: 802 passed, 0 failed, 15 ignored)   exit 0
```

**Probe files.** In the clones (untracked; removed after the run; SHA-256 prefixes):
- `audit3-repark/crates/repark-spark/tests/audit_postpublish.rs` `e9f043f3…` (first audit's Appendix E, byte-identical)
- `…/repark-spark/tests/audit2_postpublish_ops.rs` `234133bc…`
- `…/repark-sql/tests/audit2_native_postpublish.rs` `b4ae9c44…`
- `…/repark-sql/tests/audit2_eqdel_dml.rs` `6fec6d74…`
- `…/repark-sql/tests/audit3_native_struct_drop.rs` `25bb7c78…` (new)
- `audit3-fork/crates/iceberg/tests/audit_dropped_eq_key.rs` `3f94ab33…`
- `audit3-fork/crates/iceberg/tests/audit2_eqdel_probe.rs` `65cb0b5c…`

In `<scratch>/audit3-work/probes`:
- Second-audit probes, byte-identical: `probe_view_shadow.py` `ab8ee8e7…`, `probe_view_shadow2.py` `e6a949c4…`, `probe_hint_walk.py` `3c570969…`, `probe_struct_drop.py` `83e69919…`, `probe_struct_drop_bt.py` `fde4f3e7…`, `probe_public_paths.py` `44a696b3…`, `probe_delete_formats.py` `52a2cc29…`, `probe_delete_routes.py` `e71dbe16…`, `probe_dml_targets.py` `0901c16c…`, `probe_catalog_ns_collision.py` `6d9dc223…`, `probe_maint_formats.py` `6aecb052…`, `probe_wap_case.py` `392d0d8a…`, `probe_wap_late_and_textleaf.py` `36637ac5…`, `probe_textleaf.py` `40af1421…`, `probe_branch_named_tables.py` `2ed4f12d…`, `probe_volume_posdel.py` `81ab77fb…`, `spark_util.py` `5e7607ea…`.
- Edited copies: `probe_cross_read_gate_a3.py` `ea4cb262…` (import path only) and `probe_maint_formats_avro_orc.py` `e31c2b03…` (format tuple only).
- New: `util3.py` `34da3420…` and the `p3_*.py` files with the prefixes cited above.

None of these files contains a comment. Logs are in `<scratch>/audit3-work/logs/`.

## 8. Gate dimensions for the format-v3 label (what this pass adds)

- **Name resolution against session state for every statement kind, not only DML.** AUD-015 shows that the write-head list (`temp_write.rs`) is a closed list beside a resolver that does not consult temp views. A v3 gate row per statement family (DML, DDL, ALTER, procedures, frame writers) against Spark would have caught it.
- **Expression-level panics over Iceberg column types** (map with struct keys, emptied structs in every container): one SELECT per type × access form.
- **Foreign file naming in the metadata directory**: catalog-style names, orphans and unreadable hints, judged by what Spark ignores.
- **Carded silent values.** The label ledger should either hold none, or carry an owner ruling that removes each such route from the claim.

## 9. Fork section

**Verdict on the pin `56ff5545`:** it fixes AUD-008 and the id-less struct shape (C-077) as RePark consumes them, through both doors and in fresh sessions.
- My nested, time-travel, evolution, metrics and name-mapping probes found no regression.
- The fork's own suites pass: 4,672 lib tests, 103 tests in the 7 new suites, and the first and second audits' fork probes.
- The fork's id-less and emptied-struct residues are as carded. S-10 is loud on Avro. The no-mapping positional cast is a silent value, carded (see the verdict). F-MAP-KEY-DROP-ALTER-1 also produces duplicate map keys (AUD-020 item 5).
- The map-subscript panic (AUD-016) is in DataFusion, not the fork.

**`cargo test --locked -p iceberg-datafusion` (the fork's DataFusion provider):** 43 test binaries, 802 passed, 0 failed, 15 ignored, exit 0 (23:16:04 to 23:27:44 UTC).

**Clean-up:** all seven untracked probe files were removed from the two clones after the runs. `git status` is empty on both, HEAD is unchanged (`d0562058`, `56ff5545`), there is no stash, and the push URL is still disabled.
