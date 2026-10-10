# Card ENC-1-RESIDUE-1: what the keyed-table refusal does not cover

**Date:** 2026-10-09. **Filed by:** Claude (Opus 5.5), ENC-1 fold 2, from the Opus re-verify of PR #1019 head `78349722`.

**Status:** open. Not scheduled. Each item needs an owner call: accept, or close.

**Retires:** when every item below is ruled (accepted in words, or closed by a change with a pin).

## Why

The ENC-1 refusal holds: 862 keyed write attempts, 0 plaintext data, delete, manifest, puffin or stats files written into a keyed table, 0 orphans. The re-verify found five things that sit beside that claim. None writes plaintext into a keyed table through a product path. They are recorded here so they are decided, not forgotten. The ledger rows are in [enc-1-ledger.md](../../ledgers/completed/enc-1-ledger.md), section "Fold 2".

## Items

1. **A handle loaded before the key was added can stage one orphan Parquet.** The handle keeps its plain `FileIO`, so staging writes the file. The commit then reads the key from the catalog and refuses, so no snapshot is added and no pointer moves. The file stays under the table's `data/` directory, unreferenced. A fresh handle refuses at staging and writes nothing. On the SQL doors a second session that cached the table before another session added the key refuses with no file. So the orphan needs the key to arrive between a statement's table load and its staging, or a Rust caller that holds an old handle. Choice: accept the race, or sweep the staged files when the commit refuses.

2. **An unkeyed table placed under a keyed table's location writes plaintext there.** `CREATE TABLE new USING iceberg LOCATION '<keyed table directory>' AS SELECT …` runs, and so does an INSERT into a table registered from a metadata file written before the key. The files belong to the other table. The keyed table's snapshots, refs and rows do not change. Main does the same. Choice: accept (nothing is written into the keyed table), or refuse a create or register whose location lies under a keyed table.

3. **Ref-only commits publish rows that were staged before the key was added.** These run on a keyed table: the fast-forward `cherrypick_snapshot`, `set_current_snapshot`, `fast_forward`, and `CREATE OR REPLACE BRANCH main AS OF VERSION <staged>`. Each writes one metadata JSON and no data or manifest file. The rows were plaintext in the table's files before the key existed, like every other row written before the key. `publish_changes` refuses, and only by its entry check; without that check it would run like its twins. Choice: let `publish_changes` run when it is a pointer move, or name it as the one exception.

4. **Thirteen refused shapes are clean only because of their entry checks.** Plaintext safety does not depend on the entry checks: with all of them off, the catalog guard alone let no plaintext file through (700 Spark and 117 ANSI attempts). But a refused statement then leaves something behind. The five `spark.wap.branch` writes (INSERT, UPDATE, DELETE, MERGE, INSERT OVERWRITE) create the branch ref. MERGE WITH SCHEMA EVOLUTION commits the new column. `add_files` commits a property. CTAS and `writeTo().create()` with the key (two shapes) leave a `00000` metadata JSON for a table that does not exist. `publish_changes` and the three rewrite procedures with nothing to rewrite return success. With the checks on, as shipped, none of this happens. Choice: accept that unchanged state rests on the entry checks, or move each into the guard.

5. **The gate test is textual, and a catalog the caller builds is outside the guard.** `enc_1_gate.rs` matches spellings in source text. It now names the qualified `Table` spellings the verifier used, but a new alias can still pass. `Session::register_iceberg_catalog`, `CatalogRegistry::insert` and `ReparkCatalogProvider::try_new` are public and take any `Arc<dyn Catalog>`. Every product caller passes a catalog from the three builders, which is guarded. A Rust embedder that builds its own catalog gets no guard unless it calls `EncryptionGuardCatalog::install`. Choice: install the guard inside `register_iceberg_catalog`, or document the limit for embedders.

6. **A commit that moves a keyed table's metadata directory refuses on Glue.** The metadata-JSON exception is bound to the metadata directory of the loaded handle. Glue writes commit metadata through the base table's `FileIO`, so `SET LOCATION` or a change of `write.metadata.path` on a keyed Glue table refuses. Read from the fork source, not run against AWS. Choice: accept, or widen the exception to the directory the commit names.

## Interim rulings to confirm (orchestrator, 2026-10-09)

These refuse on a keyed table and are kept until the owner says otherwise: `CREATE BRANCH` on a table with no snapshot, `compute_table_stats`, `compute_partition_stats`, `rewrite_table_path`.
