# SESSION-CATALOG-TABLE-VISIBILITY-1 — a table one session created is not found by another session on the same catalog (diagnosed 2026-11-22, not live)

**Rating: proposed S2, to be confirmed by a diagnosis (the replay and the live run, 2026-11-21); the owner has not rated it.**

**Date:** 2026-11-21. **Filed by:** the records clerk (`claude-haiku-5-5`).
**Source:** two observations, one card, no audit finding id. Observation 1 is the replay's incidental note (an Opus 5.5 agent, main `dac2af61`, 2026-11-21). Observation 2 is the live acceptance run `44955277129` on main `5a3db151` (2026-11-21).

**Status:** filed 2026-11-21; diagnosed 2026-11-22 (read and measured locally, not live); not dispatched.

**Severity: S2 (proposed), to be confirmed by a diagnosis.** A session cannot see a table another session wrote. The diagnosis below names the root; it is not confirmed live.

## Finding

- **[M] Observation 1, the replay's incidental note.** A fresh RePark session, configured with the same Hadoop catalog (`lc`) and the same warehouse, could not find `lc.db.day_t`, which an earlier RePark session had written: "table 'lc.db.day_t' not found". The replay saw the same with two catalogs. The replay read through `register_table` instead. The verdict says this may be the handoff's open item on native Hadoop namespace durability across sessions, and that it is undiagnosed.
- **[M] Observation 2, the live run `44955277129`.** In `test_held_frame_external_writer_against_glue`, `test_held_frame_external_writer_against_s3tables`, `test_held_frame_refresh_against_glue` and `test_held_frame_refresh_against_s3tables`, `spark.newSession()` used as the second writer raised `AnalysisException: Error during planning: table '<catalog>.<namespace>.<table>' n…` (message cut in the log) for the table the first session had just created.
- **Reading recorded with the live run.** Spark's `newSession()` sees such a table. The four cells therefore did not measure the held-frame cache question they were written for.

## Why S2

The proposal is S2 pending a diagnosis. The sources give no impact measure beyond the two observations: a fresh session cannot find a table another session wrote, and four live cells did not measure their question.

## Scope

Two observations, one card. The diagnosis below finds two roots with one error text: the frozen table-name list (Glue and S3 Tables) and the per-session memory catalog (`type=hadoop`, card HADOOP-FACADE-FRESH-SESSION-1). The Glue and S3 Tables root is read and measured on a memory catalog, not confirmed live.

## Diagnosis (2026-11-22)

Read and measured locally, not live. The measurements are the diagnosis's own probe output (`probe_visibility.out`, kept outside the repository). Repository line numbers were checked against the base at `8357bcfb`. Tags: `[M]` measured, `[R]` read or reasoned, not run.

**Root.** Each session freezes its own per-namespace table-name list when it registers a catalog. A table resolves only if its name is in that list. A table another writer creates after the session was built is `not found` until the session relists.

- `crates/repark-iceberg/src/catalog/provider.rs:376-388`: `present_fork_schema` calls `freeze_fork_name_directory`, which probes `schema.table("__repark_snapshot_probe")` at :387. That makes the fork run one `list_tables` and keep the result. It is reached at registration from `ReparkCatalogProvider::try_new` (:35) and `snapshot_all_schemas` (:273).
- Fork `iceberg-rust` pin `b6e578d`: `crates/integrations/datafusion/src/schema.rs:146-149` (`resolve_table`: a name missing from the list returns `Ok(None)` with no catalog call) and `:204-227` (`fill_table_name_directory`: one `list_tables`, cached for good).
- A session relists a namespace only after its own DDL or commit doors, on `catalog.refreshTable` or `refresh_catalog_provider`, or when it registers a table itself. The metadata caches hold no negative or listing cache.

**A separate root from the `type=hadoop` symptom.** On `type=hadoop` the table is missing from the second session's own catalog. `crates/repark-core/src/catalog_kind.rs:28` maps `hadoop` to a memory catalog, and `crates/repark-core/src/session.rs:515-528` builds a new one for every session. The error text is identical in both cases, so the live log cannot tell them apart.

**Local measurements `[M]`.** A `type=hadoop` catalog `lc`; session A creates `lc.db.ta` with 2 rows, then `B = A.newSession()`.

- M1: B lists the catalog, but `SHOW NAMESPACES IN lc` is empty. B's read and insert of A's table both fail with `Error during planning: table 'lc.db.ta' not found`.
- M2: B creates and reads its own table (count 1). A cannot read B's table (`table 'lc.db.tb' not found`).
- M3: A registers a table with `register_table`. B reads it: `table 'lc.db.reg' not found`.
- M4: B registers the same metadata file itself, then reads it (count 2).
- M5: a fresh process with the same config lists no namespace, and `lc.db.ta` is not found.
- M6, the Glue and S3 Tables mechanism, on one session with one shared memory catalog `gc`, the table created through the catalog API: `listTables` returns `oob_t` and `tableExists` is `True`, yet `SELECT`, `INSERT INTO` and `spark.table` fail with `Error during planning: table 'gc.ns.oob_t' not found`. `refreshTable` followed by `SELECT` works (count 0).
- M6b: an own INSERT into a sibling table in the same namespace does not relist; a later out-of-band table is still not found.
- M6c: `refresh_catalog_provider("gc")` followed by `SELECT` works.

**Glue and S3 Tables prediction `[R]`.** The same `Error during planning: table '<catalog>.<namespace>.<table>' not found` for any table created after the session registered the catalog, by another session, process or job, until the session relists. The reach is wider than `newSession()`: any RePark session on Glue or S3 Tables stays blind to tables other jobs create after it starts. A `newSession()` built after the create would see the table. Spark is not blind here: `SparkCatalog.loadTable` resolves each reference by name, and `CachingCatalog` caches only hits, with no listing and no negative cache. Confidence: high, about 90%. The one open point is that the live log line is cut off. The verdict's section 3 gives the text of a live cell that would confirm or refute the prediction; it was not run.

**Trigger in the live cells.** `_assert_held_frame_external` (`python/repark/tests/parity/test_aws_acceptance.py:674-675`) creates the second session with `spark.newSession()` at :675. The held-frame cells create the table after that, in `HeldFrameCells.make` (`python/repark/tests/parity/_acceptance_held_frame.py:156-164`). The second writer's list was frozen without the table, so its INSERT fails at planning.

**Proposed fix shape (not dispatched) `[R]`.**

- In the fork's `resolve_table` (`schema.rs:146-149`), when the name is missing from the list, call `catalog.load_table(ident)` once. On success, insert the name and return the provider. On `TableNotFound` or `NoSuchNamespace`, return `Ok(None)`. On any other error, propagate it loudly, by name. This matches Spark's `loadTable` shape.
- Alternative on the RePark side: a wrapping `SchemaProvider` in `provider.rs` that, on a miss, refreshes that one namespace (`refresh_namespace`) and retries once. It needs no fork pin, but it pays a full `list_tables` per miss.
- Either shape keeps today's listing for `SHOW TABLES` and leaves the hadoop alias alone. Hadoop needs its own owner ruling (HADOOP-FACADE-FRESH-SESSION-1).
- The case-sensitivity pins (`crates/repark-spark/src/tests/casesens_true.rs:290-300`) must still refuse a case-mismatched name. The fallback must not case-fold.

**ENC-1 note.**

- What a load fetches: the fallback goes through the same guarded, `SessionTablesCatalog`-wrapped handle as a listed name's planning load. A miss costs one catalog fetch (a `GetTable` or metadata read, or a namespace `list_tables` in the alternative) where it costs none today. For a name that exists, that fetch is the table metadata a listed name already fetches.
- What it records: the load is recorded in `SessionTables` like any `load_table`, with the same epoch and race-guard rules.
- What it serves: nothing new. A table that resolves now is refused today.
- What it refuses: a keyed table refuses at the same guard, with the same text as a fresh guarded load. The fallback must never bypass `EncryptionGuardCatalog`.
- The ENC-1 allowlist (`crates/repark-iceberg/src/tests/enc_1_gate.rs`) is unchanged if the fallback lives in the fork schema provider or in a `SchemaProvider` wrapper, not in a new `impl Catalog`.

**Label path:** owner to decide (depends on the workload's reach).

Up: [map.md](map.md).
