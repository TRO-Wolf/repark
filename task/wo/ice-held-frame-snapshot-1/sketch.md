# ICE-HELD-FRAME-SNAPSHOT-1 — step 0: a measured design sketch (2026-11-20)

By Claude Opus 5.5 (`claude-opus-5-5`), effort xhigh. No product code, no build. Marks: **[M]** measured, **[R]**
read, **[Rec]** recommendation. The cells: [domain-table.md](domain-table.md). The sites:
[inventory.md](inventory.md). The probes, raw output and `javap` excerpts: [evidence/](evidence/map.md).

**The rulings carried.** The owner, 2026-11-20, about 00:22 UTC, verbatim: "Let's go with defaults across the board"
(the window's terms; the questions in §7 go to a Fable medium consult and the orchestrator acts on its guidance).
The cost rule (owner, 2026-11-19, about 14:00 UTC): C-012 a hard 1.05x; a short row over 1.05x is a flag,
attributed by swap or instruction counts; nothing over 1.15x passes without the owner. Standing: Rust-first
(2026-09-14) and the insertion-inventory rule (2026-11-09). The rating carried in: S1, label-blocking for
ICE-V2-FLAT-AWS-LABEL-1 (Fable's guidance under the owner's delegation, 2026-11-20); no card exists yet, and this
step 0 is its first record.

**The revision (2026-11-20, the same day).** The critic (Claude Opus 5.5) returned UNSOUND on one reading: Q6's
"a write of the held frame re-resolves the table by name" holds only for `writeTo` of a frame built from a name or
SQL text. Spark's rule (§1), seat A, the split's shape and the rating stand; its own re-measure agreed with every
mark. This revision measures the write rule on every hold and write path (domain table §8.1), adds the shapes the
first version missed (§8.2), adds `REFRESH TABLE` as an epoch source, weighs a fork-free variant against the fork
files (§4), restates the file count and splits unit 1 (§5), and rewrites Q6 and adds Q9 to Q11 (§7).

## 0. In short

1. **Spark's rule [R, M].** Analysis binds a table version; **execution re-binds it once per query**
   (`V2TableRefreshUtil`, Spark 4.1): every catalog-backed Iceberg relation without a time-travel spec is re-loaded
   from the session's `CachingCatalog` once per `(catalog, identifier)` per plan, then checked (a new table id or a
   changed column raises `INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS`). A Dataset's **own** plan (`collect`,
   `toLocalIterator`, `tail`, `explain`) is re-bound once and kept; `count`, `show`, `take`, children, writes and SQL
   over a view each re-bind. A same-session commit is visible to the next re-bind; an external one is not, until
   `REFRESH TABLE` (§1).
2. **The defect [M].** RePark binds the table when the name is resolved and never again: the fork's
   `from_planning_load` keeps that load's `Table` in the provider and every scan reads it (inventory B1 to B3). Of
   974 cells per build and mode, **785 are WRONG, all silent; 509 store old rows into an Iceberg table** through
   eight write shapes; 252 answer old rows where Spark raises; **7 RAISE where Spark answers** (`expire_snapshots`
   after an `INSERT OVERWRITE`, and a bare self-append after a dropped or renamed column). A deleted row comes back
   and is stored, `UPDATE`d and `MERGE`d rows keep their old values, an `INSERT OVERWRITE`, RTAS, `replace`,
   rollback or partition-spec change is invisible, and a held cast hides an ANSI error. Main and main before unit 1
   are equal in every cell; no cell moves with ANSI. Only the SQL temporary view is EQUAL (it re-plans its text).
3. **Fable's first option needs two corrections [R, M].** (a) RePark has no session copy of the current table that
   a commit updates: a commit moves the catalog's pointer and rebuilds the namespace's name directory, nothing else
   (inventory §4). Reading the current table at execution is a `load_table`, a catalog call on Glue and S3 Tables,
   unless the session keeps that state itself (seat A). (b) "At execution" must follow Spark's per-query rule, not
   every scan: the eleven own-plan sequences of domain table §5 and §8.2 (22 cells per build) are EQUAL today only
   because both engines keep the old snapshot there, and a re-bind on every action would turn them WRONG, a returned deleted row
   included. Unit 1's own-plan memo is the seat for that.
4. **Fable's pinned list needs one correction [M].** Spark pins only `VERSION AS OF` / `TIMESTAMP AS OF` (all 68
   such cells are EQUAL and must stay so). The `t.branch_b` and `t.tag_g` identifiers, the reader's `branch` option
   and the metadata tables are re-read at the action on Spark, and WRONG on main (16 cells).
5. **The seats [Rec].** **Seat A (recommended):** a session table state written by a decorator over the session's
   catalog handle on every same-session commit (the `CALL` procedures included) and by `REFRESH TABLE` /
   `refreshTable`, read by one synchronous optimizer rule that re-binds each catalog-backed Iceberg scan once per
   plan, through a fork-free wrapper over the fork's public `refreshed()` (Q9), with a new table slot in unit 1's
   own-plan memo, Spark's query and command checks, and Spark's write rule (a name- or text-built frame's `writeTo`
   re-planned by name). No catalog call is added beyond a refresh's one. **Seat B:** re-bind by `load_table`, a
   `block_on` in a synchronous rule or the same wrapper, one catalog call per held action on Glue and S3 Tables, and
   a held frame that sees another writer's commit before a refresh (Spark's does not).
6. **The split [Rec].** Seat A's true count is 13 product files and no fork file, so unit 1 splits: **1a**, the
   re-bind (11 files; 640 cells per build and mode; it can start before A1 merges), then **1b**, the write rule (2
   files; 118 cells). Unit 2 after A1: the 4 cached cells. The path read (27) by Q4; the Parquet inverse stays a named
   S3 divergence. Glue and S3 Tables are read, not measured: a gap for the owner-gated AWS acceptance leg, which
   should carry the "external commit, `REFRESH TABLE`, held read" cell.
7. **Cost: a gap.** No cost was measured: no exclusive sample could be taken in this step 0 (another lane held the
   measure lock). The build measures C-012 at the hard 1.05x and the held 2M-row rows under the lock; §6's estimates
   are readings, not measurements.
8. **Questions Q1 to Q11** (§7), each with options, a default and its cells.

## 1. Spark's rule (Spark 4.1.2 with `iceberg-spark-runtime-4.1_2.13-1.11.0`) [R, M]

Read from the bytecode ([evidence/jar/javap-refresh.txt](evidence/jar/javap-refresh.txt)) and checked by the `S`,
`P` and `X` cells ([domain-table.md](domain-table.md) §3 to §6).

1. **Analysis binds a table version.** `spark.table`, `spark.sql`, `read.table` and the path read build a
   `DataSourceV2Relation` over a `SparkTable`, loaded through `SparkCatalog.loadTable`. `SparkTable` captures its
   `schema` and `snapshot` when it is constructed (`determineLatestSnapshot`: the branch head, else
   `table.currentSnapshot()`), and its scan builder reads those fields. The `Table` underneath is the
   `CachingCatalog`'s object (`cache-enabled` defaults to true; Caffeine `expireAfterAccess`, the interval
   `cache.expiration-interval-ms`).
2. **Execution re-binds it, once per query.** Each `QueryExecution`, the first time it is planned, runs
   `V2TableRefreshUtil.refresh(session, plan, versionedOnly = true)` between `commandExecuted` and `normalized`
   (`lazyTableVersionsRefreshed`). Every relation with a catalog and an identifier, versioned (`SparkTable.version()`
   is `branch_<b>_snapshot_<id>`, never null) and with **no `timeTravelSpec`**, is replaced by a fresh one: per
   `(catalog, identifier)` once per plan (the `currentTables` map, so a self-join reads one snapshot), from the
   session's relation cache if it holds one (`lookupCachedRelation` over `SharedState.relationCache`), else
   `catalog.loadTable(ident)`. Then it
   checks the result against what analysis saw: a different `id()` (the table UUID) raises
   `INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.TABLE_ID_MISMATCH`; a dropped, renamed or retyped column raises
   `….COLUMNS_MISMATCH`; a new column is allowed for a query (`ALLOW_NEW_FIELDS`) and the old columns are read; a
   plan containing a `Command` uses `PROHIBIT_CHANGES`.
3. **A Dataset's own plan keeps its first binding.** `collect`, `collectToPython`, `toLocalIterator`, `tail` and
   `explain` run on the Dataset's own `queryExecution` field, refreshed once, the first time any of them plans it;
   later own-plan actions reuse that executed plan, so they read the first one's snapshot (even a row deleted since).
   `count` (`groupBy().count()`), `show`, `take`, a child frame, a write of the frame and SQL over a view of it each
   build a new `QueryExecution`, so each reads the table as of that action. `columns` and `schema` do not plan.
4. **A same-session commit is visible; an external one is not, until a refresh.** Spark's writes commit through the
   `SparkTable`'s `Table`, the `CachingCatalog`'s object, whose operations refresh on commit; the next `loadTable` is a
   cache hit with the new snapshot. A commit by another session (`newSession()` has its own `SparkCatalog` and cache)
   or another engine leaves the cached `Table` as it was until `REFRESH TABLE` / `catalog.refreshTable`
   (`SparkCatalog.invalidateTable` → `CachingCatalog.invalidateTable`), a same-session commit (its retry refreshes the
   base), or the idle expiry. That is ice-spark-table-1 C-005's stale read, and the reason Spark's held frame does not
   over-refresh.
5. **What stays pinned.** A `timeTravelSpec` (SQL `VERSION AS OF <id>`, `VERSION AS OF '<branch or tag>'`,
   `TIMESTAMP AS OF`, the reader's `versionAsOf`) is never refreshed, even a `TIMESTAMP AS OF` later than the last
   commit. The identifier forms `t.branch_b` and `t.tag_g`, the reader's `branch` option and the metadata tables
   (`t.snapshots`, `t.files`, `t.history`) **are** refreshed: they read the ref's head and the table's metadata as of
   the action.
6. **Caches and files.** A cached frame or `CACHE TABLE` is re-filled after a same-session write to its table (the
   cache answers the new rows on its next use). A Parquet path frame keeps the file listing it analysed (Spark 1,
   main 2; the inverse cell).
7. **Writes of a held frame** (revision, domain table §8.1). `writeTo(…)` of a frame built from a name or SQL text
   (`spark.table`, `read.table`, `spark.sql`) re-plans it from that name or text: the current table in the shape the
   text gives, with no identity check. `writeTo` of a frame derived through the DataFrame API, and `insertInto` /
   `saveAsTable` of any frame, are checked under `PROHIBIT_CHANGES` (`COLUMNS_MISMATCH` for any column change, an
   added one included; `TABLE_ID_MISMATCH` for a new table). A temp view re-resolves its tables by name with no
   identity check and raises `INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION` for a dropped or renamed
   column. The SQL `REFRESH TABLE` form refreshes like `catalog.refreshTable` (2, 2 then 3, 3), and the identifier
   selectors `snapshot_id_<n>` and `at_timestamp_<n>` are refreshed but resolve to the same snapshot, so they are
   pinned in effect (read; not measured).

## 2. RePark's mechanism [R, M]

A held RePark frame is a DataFusion `LogicalPlan` whose `TableScan` holds the provider built when its name was
resolved (inventory B1). The fork's `from_planning_load` stores that load's `Table` in `planning_table` (B2), and
`IcebergTableProvider::scan` reads `planning_table` on every scan (B3). Every action re-plans the same `LogicalPlan`,
so every action reads the resolution's snapshot. Writes load the current table (B4) and commit; the commit doors'
re-registration (inventory §4) rebuilds only the name directory and never reaches a held provider. A SQL temporary
view re-plans its text at every scan (V2), so it is EQUAL; a DataFrame view holds the frame's plan (V1), so it is
stale. Time travel, refs and the path read use static providers (T1 to T3, F4); metadata tables are bound at
resolution (T4). Main and main before unit 1 bind identically; unit 1 (#1373) touched none of these sites.

Measured: base equals main in all 3,896 verdict cells; the ANSI mode moves no verdict. The external writer on
`type=hadoop` is never visible to RePark, held or fresh (the pointer lives in the session; domain table §6), so the
local catalog cannot show an over-refresh; Glue and S3 Tables can (§4).

## 3. The domain [M]

On a scalar Iceberg v2 table on the local `type=hadoop` catalog, Spark 4.1.2, main and base, ANSI on and off, 974
verdict cells per build and mode and 6 external-writer cells ([domain-table.md](domain-table.md)):

- **First pass (465):** 10 holds (`spark.table`, `spark.sql`, `read.table`, the path read, `filter`,
  `groupBy().count()`, a DataFrame view, a SQL view, each side of a join) × 17 writes and a control × `collect`,
  `count`, `show`, `writeTo(copy).create()`, `INSERT … SELECT`, a self-join; 15 action sequences; the time-travel, ref
  and metadata-table frames; four cached frames; the Parquet inverse.
- **Revision (509):** 10 holds (the bare three, a SQL-text frame with a filter, `filter`, `select("n")`,
  `select("n", "c")`, `groupBy`, a join side, a DataFrame view) × 7 writes (`INSERT`, `DELETE`, `ALTER` add, drop,
  rename, drop and recreate, `DROP` then `writeTo().create()`) × 7 write paths (`writeTo` `create`,
  `createOrReplace`, `append` to another table, `insertInto`, `saveAsTable`, `INSERT … SELECT` from a view, the
  self-append), and 19 missed shapes (three `CALL`s, partition-spec and sort-order changes, a recreate in another
  namespace, a `MERGE` source view, two held frames, `overwrite(condition)` in a held join, ten rounds, a held cast,
  Fable's self-append, `first`/`head`, five `toPandas`/`toArrow` own-plan sequences, SQL `REFRESH TABLE`).

| per build and mode | WRONG (silent) | stored | Spark raises, RePark answers | RAISE | EQUAL |
|---|---|---|---|---|---|
| first pass | 327 | 62 | 44 | 0 | 138 |
| revision | 458 | 447 | 208 | 7 | 44 |
| **total** | **785** | **509** | **252** | **7** | **182** |

The wrong answers are always the resolution's rows. **Eight shapes store them:** `writeTo(copy).create()`,
`writeTo(new).createOrReplace()`, `writeTo(other).append()`, `write.insertInto`, `write.saveAsTable`, `INSERT …
SELECT` from a view of the held frame, the self-append into its own table (Fable §13 row 9), and a DataFrame view
used as a `MERGE` source; after a `DELETE` every one stores the deleted row. Fable's 13 shapes and the critic's 59
sampled cells all reproduce on main.

## 4. Seats (two, Rust-first) [Rec]

Both seats share one **re-binding point** and four rules taken from Spark; they differ only in where the current
table comes from. (Revised after the critic's round: the write rule, the refresh source, the fork-free variant and
the true file count are new.)

**The shared part.**

- **One synchronous optimizer rule** (new, `crates/repark-spark/src/view_ddl/iceberg_rebind.rs`, installed beside
  `ReaderParseRule`, `frame_settings.rs:318-327`; DataFusion's `OptimizerRule` is synchronous, as `ReaderParseRule` is
  at `frame_settings.rs:425`). Every action re-optimises its plan, so the rule runs once per action, and once per
  query over a DataFrame view. It walks the plan, subqueries included, and for each `TableScan` over a catalog-backed
  Iceberg provider (inventory B1), a metadata-table provider (T4) or a ref named by identifier (T3) decides **once
  per `(catalog, identifier)` per plan** (Spark's `currentTables`: a self-join reads one snapshot). The static
  time-travel providers (T1, T2, T5, `VERSION AS OF '<ref>'`) are not touched: the 68 pinned cells stay.
- **Spark's checks.** A query (`ALLOW_NEW_FIELDS`): another table UUID raises
  `INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.TABLE_ID_MISMATCH`, a missing, renamed or retyped field (by field id)
  raises `….COLUMNS_MISMATCH`, an added field is allowed and the frame keeps its columns. A write's input
  (`PROHIBIT_CHANGES`): any field change raises, an added one included. The write doors (inventory K5) enter a
  **command scope** the rule reads, so it knows which mode applies.
- **Spark's write rule for held frames** (domain table §8.1, measured on every hold, write verb and schema or identity
  change, both modes): only `writeTo(…)` (`create`, `createOrReplace`, `append`, the self-append) of a **bare** frame,
  one built from a name or SQL text (`spark.table`, `read.table`, `spark.sql`), is **re-planned by name**: it writes
  the current table in its current shape with no check. Every derived frame (`filter`, `select`, `groupBy`, a join)
  and **every** v1 write (`write.insertInto`, `write.saveAsTable`), bare frames included, is checked under
  `PROHIBIT_CHANGES`. A temp view made from the frame after the change re-resolves its tables by name with no
  identity check, raises `INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION` for a dropped or renamed column,
  and keeps its columns for an added one. **How a seat re-plans a bare frame:** the native frame carries a re-plan
  source set where it is built (`spark.table` and `read.table` through `session_table`, `catalog_surface.py:340-366`;
  `spark.sql` through `sql_built`, `crates/repark-python/src/session.rs:202`): the name or the SQL text. Any
  DataFrame-API operation makes a new frame without one. The `writeTo` entry (K5) re-plans a frame that carries a
  source from it (`SELECT * FROM <name>`, or the text) under the live session, so the write sees the current table
  and shape, and passes every other frame to the rule under the command scope. Spark's own mechanism is the same
  split: `DataFrameWriterV2` re-analyses a name-built Dataset's logical plan, every other plan is refreshed and checked.
- **Spark's own-plan rule** through unit 1's memo. #1373's `ReaderParseMemo` (`extension.rs:84-86`) stores only
  reader options (`Vec<(LiveOptions, ConfigOptions)>`); **seat A adds a slot** for `(catalog, identifier) → Table`
  decisions, taken in the synchronous rule, recorded on the first own-plan action of a frame object and reused by
  its later ones (`collect`, `toLocalIterator`, `tail`, `toArrow`, `toPandas`, `explain`, all measured, domain table
  §5 and §8.2). `explain` (`core.py:2485`) enters the memo.

### Seat A — a session table state, kept by the commits and the refreshes (recommended)

- **Where the current table comes from.** A per-session `SessionTables` map, `(catalog, identifier) → (epoch, Table
  or a drop marker)`, in a new `crates/repark-iceberg/src/catalog/session_tables.rs`. **Its writers:**
  1. a `Catalog` decorator over the session's registered handle (inventory R1), shaped like `EncryptionGuardCatalog`
     (R5), that records what `update_table`, `create_table`, `publish_create_table`, `publish_replace_table` and
     `register_table` return, a drop marker on `drop_table`, both names on `rename_table`. Every write door of
     inventory §4 and every `CALL` procedure (K4: `tx.commit(catalog)` through the passed handle) commits through it,
     so no commit door is edited;
  2. **`REFRESH TABLE` and `catalog.refreshTable`** (K2, K3): one deliberate `load_table`, recorded in the map. Spark's
     held frame answers an external commit after a refresh (3, 3; domain table §6), so on Glue and S3 Tables a held
     frame must too; without this writer it would stay old after a refresh. The same epoch moves A1's lazy cache
     table to refill, as Spark's `RefreshTableExec` re-caches;
  3. under Q2 (b), every planning load newer than the entry.
- The rule re-binds a scan when the map holds a newer state than the provider's planning load (by metadata location
  and `last-updated-ms`). **Spark's `CachingCatalog` shape:** the session's commits and refreshes are seen; another
  writer's are not, until a refresh.
- **Cells moved:** unit 1a's 640 and unit 1b's 118 per build and mode (domain table §7), the path read's 27 under
  Q4 (a), and the 4 cached cells in unit 2. **EQUAL lost: none known.** The external-writer control is kept on
  every catalog, refresh included.
- **`EXPLAIN`:** unchanged text; `EXPLAIN VERBOSE` prints the re-bound `snapshot_id` (fork `physical_plan/scan.rs:470-492`).
- **Cost:** no catalog call except the refresh's one, which Spark also makes. Per action, one plan walk and one map
  lookup per Iceberg scan; per commit, one map insert. A read estimate, not a measurement (§6).

### Seat A's re-bound provider: fork-free wrapper or two fork files (Q9)

| | (a) fork-free wrapper (the critic's variant) | (b) two fork constructors |
|---|---|---|
| mechanism | the rule wraps each catalog-backed Iceberg `TableScan` source in a product `TableProvider` (no I/O in the rule). Its **async** `scan` reads the decision (memo or map); if newer, it gets a provider from the fork's **public** `IcebergTableProvider::refreshed()` (fork `table/mod.rs:118-137`; it keeps `uuid_as_string`, the commit branch, the properties and the output spec) with the session's decorator answering that `load_table` from the map (a scoped mode, so no catalog call), or `SchemaProvider::table("t$snapshots")` the same way for a metadata table; it maps the held projection onto the new schema by field id and delegates | `IcebergTableProvider::rebound(table)` beside `from_planning_load` (fork `table/loaded.rs`, keeps the advertised schema, sets `planning_table`) and a rebuild accessor in `table/metadata_table.rs`; the rule swaps the provider directly; the fork's scan already reads a newer table under a fixed schema |
| files | one more product file (the wrapper), no fork file | two fork files, a fork PR, then a RePark re-pin of the whole `iceberg*` rev |
| risk | new projection-mapping code (field ids, the UUID-as-text advertised schema, filter push-down and limit delegation); every `downcast_ref::<IcebergTableProvider>` on an optimised plan must see through the wrapper (the builder lists them) | two repositories in one unit: the fork's protected main (its `Tests` default and doc jobs required) and the re-pin's Cargo.lock churn, so two gated rounds under a one-round cap |
| cost | one map lookup per scan; a schema conversion only when newer | the same |
| proof | the `alter_add` cells (old columns, all rows) and the UUID-text cells pin the mapping | the fork's own tests plus the same cells |

**Default (a)**, the fork-free wrapper: it keeps unit 1 to one PR in one repository under the one-round cap. If its
projection mapping fails the `alter_add` cells, (b) is the fallback.

### The true file count (seat A with (a))

Product files, by part (the earlier "9 plus 2 fork" omitted the write rule, the refresh writer and the memo's
wiring):

1. `crates/repark-iceberg/src/catalog/session_tables.rs` (new: the map, the decorator, the map-serving mode);
2. `crates/repark-iceberg/src/catalog/mod.rs` (exports);
3. the wrap at registration, `crates/repark-core/src/session.rs:458-482` (at its 1,000-line ceiling: line-neutral, or
   in the caller), and the map's owner on the session state;
4. `crates/repark-spark/src/view_ddl/iceberg_rebind.rs` (new: the rule and the checks);
5. `crates/repark-spark/src/view_ddl/held_iceberg_scan.rs` (new: the wrapper);
6. `crates/repark-spark/src/view_ddl/frame_settings.rs` (install);
7. `crates/repark-core/src/extension.rs` (the memo's table slot and the command scope);
8. `crates/repark-spark/src/time_travel.rs` and 9. `crates/repark-core/src/time_travel.rs` (the identifier-ref
   marker, T3);
10. `python/repark/src/repark/spark/dataframe/core.py` (`explain` under the memo; at its exact 3,461-line baseline,
    so line-neutral);
11. `crates/repark-python/src/session.rs` (the re-plan source at `sql_built` and `session_table`'s native path) and
    12. the native frame write entry the K5 doors call (the re-plan and the command scope);
13. the refresh writer: `crates/repark-spark/src/catalog_ops.rs` (`refresh_catalog_provider`'s native side) and the
    SQL `REFRESH TABLE` handler (`router.rs:600-612`).

**13 product files, no fork file** (14 with Q4 (a)'s `crates/repark-core/src/iceberg_path.rs`). That is over what one
round should carry, so §5 splits it.

### Seat B — re-bind by `load_table`

- **Where the current table comes from.** `catalog.load_table(identifier)`, once per table per plan. The rule is a
  synchronous `OptimizerRule`, so a `load_table` there needs a `block_on` inside the optimizer, the hazard the
  codebase avoids elsewhere (fork `schema.rs:411` `block_on_off_caller_runtime`); otherwise it needs the same async
  wrapper as seat A (a), at which point it differs from seat A only in skipping the map.
- **Cells moved:** the same on `type=hadoop`. **EQUAL lost on Glue and S3 Tables (read):** a held frame sees another
  writer's commit at its next action, before any refresh, where Spark's does not.
- **Cost:** a `load_table` on every action on a held Iceberg frame, with or without a write: a pointer read locally,
  one `GetTable` on Glue, one metadata-location call on S3 Tables (tens of milliseconds a call in region; read, not
  measured), so a short held read can pass 1.15x: an owner row.

**Recommendation: seat A, with the fork-free wrapper.**

## 5. The unit split under the one-round cap [Rec]

Seat A's true file count (13, §4) is more than one builder round should carry, so unit 1 splits along Spark's own
seam: the read side and the write rule.

- **Unit 1a — the re-bind (seat A with Q9 (a); Opus 5.5 xhigh builder, Opus 5.5 medium verifier, one round).** The
  map, the decorator and the refresh writer; the rule with the query checks and the view rules (no identity check
  inside a view, `INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION`); the wrapper; the memo's table slot and
  `explain`; refs and metadata tables (Q5); the `CALL` cells (Q11). Product files 1 to 10 and 13 of §4 (11). **Bar,
  per build and mode:** 640 WRONG or RAISE cells EQUAL (the first pass's 294, §8.1's 338, §8.2's 13); every
  EQUAL cell held, named: the 68 pinned and ref cells, the 28 SQL-view cells, the 11 own-plan rows (§5, §8.2), the
  controls; mutation proofs on the memo slot (drop it and the 11 rows turn WRONG) and on the identity check. Until
  unit 1b, a bare frame's `writeTo` after a schema or identity change raises the query check's condition where Spark
  re-plans by name (loud, 53 cells), which is Q6 (c)'s state. It can start before A1 merges. **Overlap with A1:**
  `extension.rs` and `core.py`; the second PR to merge rebases, `core.py` stays at its baseline.
- **Unit 1b — the write rule (Q6 (a)), after 1a.** The native re-plan source (`crates/repark-python/src/session.rs`
  and the frame's write entry, files 11 and 12 of §4) and the command scope (`extension.rs`, again). **Bar:** the
  118 cells of §8.1 that need it: a name- or text-built frame's `writeTo` after a schema or identity change
  (Spark re-plans), and an added column under a write (Spark raises `COLUMNS_MISMATCH` where a query allows it).
- **Unit 2 — the cached cells, after A1 merges.** A1's lazy cache table drops its fill when the map's epoch for a
  table it reads moves (commits and refreshes); C-009's token check reads the epoch. **Bar:** the 4 `C` cells per
  build and mode, with catalog-surface-1's C-009 pins and A1's bar held.
- **Not a unit:** the path read unless Q4 (a) (a separate S2 card by default); the Parquet inverse (S3, a named
  divergence); the `type=hadoop` catalog's blindness to another session or process (§6, and §8.2's
  `refresh_sql_external`), which is pre-existing and not this card's mechanism; Glue and S3 Tables live, the
  owner-gated AWS acceptance leg (§6).

## 6. Cost: a gap for the build [R]

**Cost is not measured in this step 0: the build measures C-012 (hard 1.05x) and the held rows below under the
exclusive lock, and nothing here substitutes for that.** No exclusive sample was taken. From about 05:42 UTC to the hand-back, the measure lock's shared side was held by
another lane's cargo build (cores 48-57); the first `flock -x -w 600` round waited its 600 s with no sample, and the
rule allows no shared-lock reading in its place (the same outcome as the cache step 0's revision series). The probe
and its five cases are committed for the build to run under the lock: [evidence/probes/cost.py](evidence/probes/cost.py)
(`held_nowrite`, `held_write`, `fresh_write`, `plan_100`, `held_filter_count`, over a 2M-row table). Design
estimates, read from the code and from measured records:

| row | seat A | seat B |
|---|---|---|
| a held 2M-row Iceberg read after a write | the new snapshot's scan: a fresh read's work without its planning `load_table`; today's number reads the wrong snapshot, so it is not a baseline to hold | the same scan plus one `load_table` |
| the same read with no write between | one plan walk and one map lookup per Iceberg scan: about 1.00x | one `load_table` per action: on `type=hadoop` a pointer read and a metadata-cache hit (catalog-surface-1 C-009 measured a planning `spark.table` at 0.221 ms a call); on Glue one `GetTable`, on S3 Tables one metadata-location call (tens of milliseconds in region; read, not measured) |
| a short held read (tens of ms) | about 1.00x | over 1.15x on Glue and S3 Tables: an owner row |
| C-012 (the like-for-like replay set) | about 1.00x: one lookup per Iceberg scan (a reading, not measured; the build measures it at the hard 1.05x) | about 1.00x locally; the replay does not run on Glue |

**Glue and S3 Tables (the label's catalogs), read only.** A held frame binds through the same fork schema provider
and planning fast path on all three catalogs (inventory B1 to B3, R6 to R8), so the defect is the same there; Fable's
13 shapes are catalog-agnostic by construction. The live cells and the per-call latencies are a gap for the
owner-gated AWS acceptance leg (`.github/workflows/aws-acceptance.yml`, `workflow_dispatch` with `glue` or
`s3tables`, environment `aws-acceptance`), whose modules (`test_aws_acceptance.py`, the dbt gold and cross-read
modules) hold no held-frame cell today: unit 1a's pins should add the `HW` `insert`, `delete`
and `copy` rows, the external-writer control, and the refresh cell **"hold `spark.table(t)`; an external commit; held
and fresh `count`; `REFRESH TABLE t`; held and fresh `count`"** (Spark: 2, 2 then 3, 3), the one cell that decides Q10
and Q2 on the label's catalogs.

## 7. Questions (for the Fable consult; each ruled from the tables)

- **Q1. The seat.** (a) seat A, the session table state kept by the commits and the refreshes; (b) seat B,
  `load_table` at the re-bind (a `block_on` in a synchronous rule, or seat A's async wrapper without the map). Both
  move the same cells on the local catalog (§4). **Default (a):** no catalog call on Glue or S3 Tables beyond a
  refresh, and a held frame does not see another writer's commit before a refresh, as on Spark (domain table §6).
- **Q2. What else the map records (seat A).** (a) commits and refreshes only; (b) also every planning load newer than
  the entry, so a held frame is never older than a read the session has already made. On `type=hadoop` the two are the
  same. On Glue and S3 Tables they differ only after another writer's commit and before a refresh: under (a) a held
  frame keeps its snapshot while a fresh read shows the commit; under (b) it follows the fresh read. Spark's held and
  fresh reads both answer its cache (2, 2 before the refresh). **Default (b).**
- **Q3. The own-plan rule.** (a) Spark's: an own-plan action keeps its frame object's first binding, through a new
  table slot in unit 1's memo; (b) re-bind on every action. Measured own-plan actions: `collect`, `toLocalIterator`,
  `tail`, `explain` (domain table §5) and **`toPandas`, `toArrow`** (§8.2: `collect` then `toPandas`, `toPandas`
  twice, `collect` then `toArrow`, `toArrow` twice, `toArrow` then `collect`; Spark keeps the first binding in all
  five, with pandas linked into the oracle). (b) turns these 11 EQUAL rows WRONG (22 cells per build). **Default
  (a).**
- **Q4. The path read** (27 WRONG per build and mode). (a) unit 1 re-binds it too, re-running the Hadoop discovery
  at each re-bind; (b) a separate card at S2. **Default (b).**
- **Q5. Refs and metadata tables** (16 WRONG). (a) follow Spark: re-bind the identifier refs, the `branch` option and
  the metadata tables, keep `VERSION AS OF` / `TIMESTAMP AS OF` pinned; (b) keep them pinned and name the 16 cells.
  **Default (a).**
- **Q6. Writes of a held frame after a schema or identity change (restated).** Spark's rule (domain table §8.1): a
  `writeTo(…)` of a frame built from a name or SQL text re-plans it by name into the current table and shape; a
  derived frame's `writeTo`, and every frame's `write.insertInto` and `write.saveAsTable`, raise
  `INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS` (`COLUMNS_MISMATCH` for any column change, an added one included;
  `TABLE_ID_MISMATCH` for a recreate); a view made after the change raises
  `INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION` for a dropped or renamed column and otherwise reads the
  current table. (a) Spark's split: the native frame carries its name or SQL text, `writeTo` re-plans a frame that has
  one, every other write enters the command scope (§4); (b) raise for both bare and derived (loud: the bare
  `writeTo` cells become RAISE where Spark answers); (c) defer the bare re-plan to its own card and ship (b) in unit
  1. Cells: domain table §8.1's schema-and-identity rows; the 118 cells per build and mode that need the
  write rule (§7: a name- or text-built frame's `writeTo`, and an added column under a write); (a) moves them, (b)
  turns 53 of them into RAISE, (c) leaves those 53 RAISE until its card. **Default (a)**, built as unit 1b (§5), with (c) as the fallback under the one-round cap.
- **Q7. The rating.** (a) S1, label-blocking; (b) S1, not blocking. **Default (a):** 785 silent WRONG cells
  per build and mode, 509 of them stored, through eight stored shapes, the path catalog-agnostic.
- **Q8. The order.** (a) unit 1a takes the next builder slot without waiting for A1, 1b after it, unit 2 after A1;
  (b) all wait for A1. **Default (a).**
- **Q9. The re-bound provider (new).** (a) the fork-free wrapper over the fork's public `refreshed()`, with the
  decorator serving the map; (b) two fork constructors (`rebound`, a metadata-table accessor). Same cells, same cost;
  (a) adds one product file and its projection mapping, (b) adds a fork PR through the fork's protected main and a
  re-pin (§4 table). **Default (a).** The `alter_add` cells of §1 and §8.1 decide it at build time.
- **Q10. `REFRESH TABLE` as an epoch source (new).** (a) `REFRESH TABLE` and `catalog.refreshTable` load the table
  once and write the map, and move A1's epoch; (b) leave refresh as today (a name-directory rebuild). Cells: domain
  table §6 (`refreshTable`: Spark 3, 3) and §8.2 `refresh_sql_external` (SQL `REFRESH TABLE`: Spark 2, 2 then 3, 3).
  On `type=hadoop` RePark cannot see the external commit at all, so the local cells stay WRONG under either option;
  the cell that decides it is the AWS leg's "external commit, refresh, held read". **Default (a).**
- **Q11. The `CALL` procedures in unit 1's bar (new).** (a) yes: `rollback_to_snapshot`, `rewrite_data_files` then a
  `DELETE`, and `INSERT OVERWRITE` then `expire_snapshots` (the RAISE) join unit 1's bar (they commit through the
  decorated handle, K4); (b) no, a follow-up card. Cells: §8.2, 3 per build and mode. **Default (a):** no extra file.

## 8. What the brief and the earlier records got wrong

1. **Fable §13: "a commit re-registers the namespace's providers and a held plan keeps the old one."** The
   re-registration rebuilds only the namespace's name directory; no provider is cached anywhere, and every planning
   round builds a fresh one. The pin is the fork's planning fast path: `from_planning_load` keeps the planning
   load's `Table` and every scan reads it (inventory §1). The conclusion stands (not catalog-specific).
2. **Fable's first option: "the scan reads the session catalog's current table at execution, which a same-session
   commit already updates, so no catalog call is added."** RePark has no such session state: on Glue and S3 Tables
   the current table is a catalog call away. Seat A builds that state; seat B pays the call. And "at execution" must
   be Spark's per-query rule with the own-plan memo, or 22 EQUAL cells per build turn WRONG (§0 point 3).
3. **The brief and Fable §13: "frames that must stay pinned: time travel, branch and tag reads, and metadata
   tables."** Spark pins only the time-travel forms. The identifier refs, the `branch` reader option and the metadata
   tables are refreshed on Spark and WRONG on main (16 cells per build and mode).
4. **The brief: "Spark answers the current one."** Per query, yes; per frame, no. A Dataset's own-plan actions keep
   their first binding, a deleted row included (domain table §5).
5. **The brief: "the external-writer control … a second session or a second engine writes."** On `type=hadoop` a
   RePark session cannot see a table another session or process created, so no RePark writer can reach the held
   table, and Spark's view of a RePark writer cannot be measured. The control measured is Spark with `newSession()`
   and RePark with Spark as the writer. No local REST catalog was available offline.
6. **Fable §13: "four of the 12 original cells are the cached shapes, and those need A1's lazy table to drop its fill on a
   write."**
   Confirmed: this table's four `C` cells per build and mode are those shapes, and nothing else in the WRONG set has a
   cache in it. catalog-surface-1 C-009's intent ("a held cache never serves rows older than the table") is the gap
   they show: its token is checked only by a new `spark.table` or `isCached`. All of Fable's 13 shapes reproduce on
   main.
7. **This step 0's first version (found by the critic, Claude Opus 5.5, 2026-11-20).**
   - **Q6 and domain table §2 generalised from the bare hold.** "A write of the held frame re-resolves the table by
     name" was measured only on `spark.table`. It holds for `writeTo` of frames built from a name or SQL text; a
     derived frame's `writeTo` and every `insertInto` / `saveAsTable` raise (§8.1). Q6 is restated.
   - **"Two shapes store"; "this table holds both".** Eight shapes store old rows (§8.1, §8.2): the six write
     paths to another table (`writeTo(copy).create()`, `createOrReplace`, `writeTo(other).append()`,
     `write.insertInto`, `write.saveAsTable`, `INSERT … SELECT` from a view), the self-append into its own table
     (Fable §13 row 9, which the first version did not hold), and a DataFrame view used as a `MERGE` source.
   - **"0 RAISE".** Main raises `Failed to load manifest list` where Spark answers after `INSERT OVERWRITE` then
     `expire_snapshots`, and refuses the bare self-append after a dropped or renamed column where Spark writes the new
     shape (§8).
   - **The inventory's `catalog_surface.py`** sat under `session/`; it is `python/repark/src/repark/spark/` (fixed), and
     T3 now cites `crates/repark-core/src/time_travel.rs:139`.
   - **"9 product files plus 2 fork files"** left out the write rule, the refresh writer and the memo's new slot; the
     true count is in §4.

Up: [map.md](map.md).
