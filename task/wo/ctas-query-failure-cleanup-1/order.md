# CTAS-QUERY-FAILURE-CLEANUP-1 (AUD-025): a CTAS whose query fails leaves no metadata behind

**Grade:** B (guided; the design below is fixed). **Builder:** Muse MAX (`muse-spark-1.3-contributor`, effort max). This
is the second builder that the owner's 2026-11-11 ruling allows.
**Review:** an Opus 5.5 (`claude-opus-5-5`) verifier and a fresh Muse MAX critic, in parallel. The R7 sentence applies:
every blocking finding must be resolved on the reviewed head. The cap is one build, one critique, one delta and one
delta re-check.
**Card:** [ctas-query-failure-cleanup-1-card-2026-11-05.md](../../roadmap/mid-term/ctas-query-failure-cleanup-1-card-2026-11-05.md).
**Ledger:** [ice-v2-label-1](../../ledgers/staging/ice-v2-label-1-ledger.md) C-101 (S2, blocks the format-v2 label).
**Release:** v1.5.8 under Q5 (2026-11-08), unless it is ready earlier.

## Rulings this order carries

- **Owner, 2026-11-11, verbatim: "Go with the defaults"**, adopting the label path-forward questions 1 to 13. The two
  that bind here:
  - **Q1:** "A second builder beside the lane … Yes, for those two units only: a Muse MAX builder with an Opus 5.5
    verifier, while the Opus lane keeps the design-heavy units." The orchestrator's condition, adopted with it: the
    second builder's builds pause while the main lane takes cost measurements. The mechanism is the measure lock in
    the brief.
  - **Q7:** "Not in the unit. Remove the leftover metadata file on both paths, which is what makes the leftover look
    like an empty table. Card the orphan data files; Spark leaves more."
- **Owner, 2026-11-05:** the three fix units, then the narrow re-audit. Every fix brief carries the audit's cells for
  the finding as its clause, plus the full facade gate. That ruling is recorded in ledger C-115.
- **Still binding:**
  - the table-completeness check and the named-seat rule (2026-11-10);
  - the insertion inventory (2026-11-09);
  - the one-round design (2026-11-03).

## Clause: the measured table

**Source.** The 2026-11-11 evaluation measured this table on the 1.5.6 wheel and Spark 4.1.2 with Iceberg 1.11.0,
hadoop catalog, ANSI on. Item 1 re-measures every row on this branch's build of main.

**Wrong on 1.5.6.** Each cell leaves `<table>/metadata/00000-<uuid>.metadata.json`. A path read then answers an empty
table with the CTAS schema, where Spark refuses.

| Row | Failure | Write path |
|---|---|---|
| F1 | The audit's repro, `CAST('q' AS INT)`, failing in the optimizer inside `create_physical_plan` | fast |
| F2 | Execution fails on the first row | fast |
| F3 | Execution fails on the last or a late row | fast |
| F4 | Partitioned table; execution fails on the last row | fast |
| F5 | The same failure with a table-property target file size | fast |
| F6 | The same failure with a write option or a session key such as `spark.sql.iceberg.compression-codec` (200k rows, 1 KB target) | options |

**Axes for each F row:**
- **Spellings** (each listed spelling reaches the same create):
  - SQL `CREATE TABLE … AS SELECT`;
  - `CREATE OR REPLACE TABLE … AS` on a new table;
  - `writeTo().create()`;
  - `writeTo().createOrReplace()` on a new table;
  - `saveAsTable`, default mode and append, on a new table;
  - `saveAsTable` overwrite on a new table.
- **Catalog kinds:** `type=hadoop` and the in-memory catalog (`register_memory_catalog`).
- **Format versions:** 2 and 3.
- **Doors:** the Spark facade, and the Rust Spark router (`crates/repark-spark` tests).
- **Reads per cell:**
  - the files left under `metadata/` and `data/`;
  - the same-session catalog;
  - the facade path read;
  - a fresh process's catalog read.
  - Spark's path read on three cells: F1, F3 and F6.

**Controls**, which must not change:

| Row | Statement | Expected |
|---|---|---|
| K1 | Analysis error (unresolved column) | Nothing written, before and after |
| K2 | Fast-path success | `v1.metadata.json` and the version hint, no `00000` file, the rows readable |
| K3 | Options-path success | Its metadata kept. The `00000` stray beside `v1` stays: it is STAGED-PUBLISH-OPTIONS-PATH-1's, not this unit's |
| K4 | Failing query over an **existing** table: `CREATE OR REPLACE`, `REPLACE`, `writeTo().replace()` and `createOrReplace()`, `saveAsTable` overwrite | The live table reads its 3 rows, and its current metadata file is untouched |
| K5 | A rejected publish and `CommitStateUnknown` | The STAGED-PUBLISH-CLEANUP-1 pins, unchanged |

### Table-completeness check

| Named seat or route | Disposition |
|---|---|
| The six spellings above | Carried |
| `PREPARE` of a CTAS, `EXPLAIN ANALYZE` of a CTAS | Item 1 measures one cell each. If either reaches `execute_ctas`, it joins the table; otherwise it is struck with the measured refusal |
| dbt `create_table_as` | Struck in writing: it emits the SQL spelling, which is carried |
| The native door (`repark.sql`, `crates/repark-sql/src/create_table.rs`) | **Out.** The label is scoped to the Spark door (ruling E, 2026-11-08). It gets a native gap-list row and the card NATIVE-CTAS-FAILURE-CLEANUP-1 |
| S3 Tables service-managed CTAS (create first, drop on failure) | **Out:** not measurable without AWS. Card S3T-CTAS-FAILED-DROP-1 for the case where a failed drop leaves a catalog entry |
| REST and Glue catalogs | Struck. The helper makes no catalog call, so it acts the same for every kind; one in-memory and one hadoop row per cell stand for them |
| Cancellation and panic during the query | **Out:** card CTAS-CANCEL-CLEANUP-1 (S2), naming the leftover. An async delete cannot run from `Drop`. Item 1 measures one cancel cell so the card states the measured leftover |

## Design (fixed; the builder makes no design choices)

1. **One new helper, catalog-free, in `crates/repark-iceberg/src/write/staged_publish.rs`, beside `discard_rejected`.**
   For example, `pub async fn discard_unpublished_create(staged: &StagedTableTransaction)`:
   - It acts only when `staged.mode() == StagedTableMode::Create`. For `Replace` it does nothing, because
     `begin_replace` writes no file and the live table's metadata must not be touched.
   - It deletes exactly one path: `staged.table().metadata_location()`, the begin-create `00000-<uuid>` file, through
     `staged.table().file_io()`.
   - It does not list directories, load from a catalog, or decide anything from a later read. STAGED-PUBLISH-CLEANUP-1
     lost two rounds to deletion decisions made from a later catalog read (its F1 and F5).
   - A failed delete logs a `tracing::warn!` in the style of `discard_rejected` and never replaces the caller's error.
2. **One call per write path, in `finish_ctas_staged_commit` (`crates/repark-spark/src/ctas.rs`):**
   - **Fast path:** if `write_ctas_query` returns `Err`, call the helper, then return the original error unchanged.
     That covers `create_physical_plan` (F1) and `write_data_files_from_plan` (F2 to F5).
   - **Options path:** any `Err` between the function's entry and the `publish_applied` call runs the helper, then
     returns the original error. That covers `resolve_with_session`, `execute_stream`, the two
     `stage_*_stream_with_overrides` calls, the transaction build and `apply_locally`.
   - Once `commit_staged` or `publish_applied` has been called, this helper never runs. Publish-time failures stay with
     `discard_rejected`.
3. **No directory removal.** An empty table directory already reads as "no Iceberg table found at …"; the evaluation
   measured this as S2 and S3 on 1.5.6.
4. **Data files are not this helper's job.** The fast path already deletes its attempt files
   (`partition_write.rs:260`). The options path's orphan data files are carded as CTAS-OPTIONS-ORPHAN-DATA-1 (Q7), and
   the F6 cells assert only that no `*.metadata.json` is left.
5. **Refusal text.** After the fix, a path read of the location refuses with RePark's existing text, "no Iceberg table
   found at …" (`iceberg_path.rs`).
   - The pins assert that text.
   - Spark's `TABLE_OR_VIEW_NOT_FOUND` differs in text only: a refusal-text card, CTAS-PATH-READ-TEXT-1, which each pin
     names in its id.
6. **Untouched:** the fork, `Cargo.toml`, `Cargo.lock`, the ENC-1 allowlist, any `impl Catalog`, the signed-zero seats,
   `case_bind.rs`, the native door. Parked PR #1071 touches the same CTAS seams, so it rebases onto this unit after the
   merge.

### Insertion inventory

| Where evidence is created | Where it can survive |
|---|---|
| `begin_staged_create` → fork `staged_table.rs` writes `00000-<uuid>.metadata.json` (`metadata_location.rs`) | **the gap this unit closes**: everything between the begin call and the commit call |
| Options-path `apply_locally` (`with_next_version`) | Kept on success (K3; the card's) |
| The fast path's attempt data files | Deleted on error by `partition_write.rs` |
| The options path's data files | Orphans, carded (Q7) |
| A rejected publish | `discard_rejected` (K5) |

## Items (commit after each)

1. **Re-measure the table on main.**
   - Use this clone's release build and the 2026-11-11 evaluation's scripts, copied into
     `evidence/step0/` with every absolute path scrubbed.
   - Measure every F row on every axis, and every K row, plus the PREPARE, EXPLAIN ANALYZE and cancel cells.
   - Re-run Spark once for F1, F3 and F6.
   - **Hand back** only if a row's class differs from the 1.5.6 table. Otherwise continue.
2. **The helper, the two call sites and every pin.** Tests go in the same commit as the code.
   - **Rust:** a new `crates/repark-spark/tests/ctas_query_failure_cleanup.rs`, table-driven across the F rows ×
     hadoop and memory × v2 and v3, plus K1, K2 and K4. Use `tempfile::TempDir`.
   - **Facade:** a new `python/repark/tests/iceberg/test_ctas_query_failure_cleanup_1.py`, covering the six spellings ×
     F1, F3 and F6 × v2 and v3, plus K4's five replace spellings. Use the `tmp_path` fixture.
   - **Each failing cell asserts all of these:**
     - the statement raises;
     - no `*.metadata.json` is left under `metadata/`;
     - on the fast path, no data file is left;
     - the same-session catalog refuses;
     - the facade path read refuses with RePark's text.
   - **Each control asserts its expected row** from the table above.
3. **The red check and the after-table.**
   - With the two call sites removed (`git checkout HEAD~1 -- crates/repark-spark/src/ctas.rs`, so the helper stays),
     the F cells go red. Record the count, then restore.
   - Re-run item 1's table on the branch head as `evidence/after-table.md`: every F cell moves to "nothing left" and
     every K cell is unchanged.
   - Also re-run STAGED-PUBLISH-CLEANUP-1's pins (`crates/repark-spark/tests/staged_publish_cleanup.rs`,
     `crates/repark-sql/tests/staged_publish_cleanup.rs`).
4. **Gates**, as in the brief, with the facade in the ci profile and in release. Then the `map.md` rows for every
   touched directory.

## Halts

- A K4 cell where the live table reads fewer than its 3 rows, or loses its current metadata file.
- The helper needs a catalog call, a listing, or a fork change.
- A STAGED-PUBLISH-CLEANUP-1 pin goes red.
- An item-1 row's class differs from the 1.5.6 table.
- A red test outside this unit's files.

## Cards this unit files (records clerk, at merge)

- CTAS-OPTIONS-ORPHAN-DATA-1 (S3; Q7);
- CTAS-PATH-READ-TEXT-1 (refusal text);
- CTAS-CANCEL-CLEANUP-1 (S2, with the measured leftover);
- NATIVE-CTAS-FAILURE-CLEANUP-1 (with a native gap-list row);
- S3T-CTAS-FAILED-DROP-1.

## Rulings after item 1 (orchestrator, as the order's author, 2026-11-11)

Item 1, at `a6178288`, re-measured the 228-cell table on main. Every class matches the 1.5.6 table.

1. **The 18 `saveAsTable` × v3 F-pin seats are struck, with the measured cause.**
   - `saveAsTable` has no public way to request format version 3 on main, measured three ways:
     - `.option` is ignored for creation;
     - the catalog `table-default.format-version` is refused as reserved;
     - there is no `tableProperty` API.
   - The facade pins keep `saveAsTable` × v2 for F1, F3 and F6, and SQL and `writeTo` on v2 and v3. K4's `saveAsTable`
     × v3 control stays: a v3 seed plus an overwrite keeps v3, measured intact.
   - No product change: adding a properties channel to `saveAsTable` is outside this unit. Card
     SAVEASTABLE-FORMAT-VERSION-1 (S3) at merge. Its first step is to measure whether Spark 4.1.2 offers a channel.
2. **The S3 Tables row of the completeness check changes from "out" to "measured live".**
   - This follows the owner's 2026-11-11 "Go" on the scoped label, ICE-V2-FLAT-AWS-LABEL-1, and its gate G5.
   - The cells are the G5 module of that unit's test-only PR, `python/repark/tests/parity/test_aws_acceptance_ctas_failure.py`.
     It runs from main on Glue and S3 Tables, before this unit merges (the baseline) and after it.
   - **The reading rule:**
     - a leftover that no catalog reader can see is a card;
     - a catalog entry that reads as an empty table blocks;
     - a catalog entry that lists the table but cannot be read also blocks (the orchestrator's reading).
   - **Coverage, READ:**
     - Glue takes `begin_staged_create` (`ctas.rs:272`), so this unit's helper covers it.
     - S3 Tables takes the service-managed create-first and drop-on-failure path (`ctas.rs:646-649`, `707-724`), which
       this unit does not change.
     - If the G5 run shows a blocking S3 Tables entry, the fix is a delta on this unit or its own unit, ruled then.
   - The builder runs nothing on AWS. The launcher has no AWS credentials, and the workflow runs only from main.
3. **The F6 orphan data files are smaller on main than the order said.**
   - On 1.5.6, OPT3 left `00000` plus 66 Parquet files. On main it leaves `00000` plus 0, with two nondeterministic
     single orphans across 28 F6 cells.
   - The metadata class is unchanged. The card CTAS-OPTIONS-ORPHAN-DATA-1 records the main numbers.
4. **Recorded for the records clerk, not this unit:**
   - `DROP TABLE` on the hadoop catalog leaves every file behind (the cancel rerun kept `v1` and 12 Parquet files). Spark's
     behaviour on the same drop is to be measured before any card says it differs.
   - The `saveAsTable` F5 seats run on the options path, because the target size arrives as a write option. They are
     recorded as options-path cells.
