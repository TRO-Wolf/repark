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
   465 cells per build and mode, **327 are WRONG, all silent; 62 store the old rows into an Iceberg table**; 44 answer
   old rows where Spark raises. A deleted row comes back, `UPDATE`d and `MERGE`d rows keep their old values, an
   `INSERT OVERWRITE`, RTAS or `replace` is invisible. Main and main before unit 1 are equal in every cell; no cell
   moves with ANSI. Only the SQL temporary view is EQUAL (it re-plans its text).
3. **Fable's first option needs two corrections [R, M].** (a) RePark has no session copy of the current table that
   a commit updates: a commit moves the catalog's pointer and rebuilds the namespace's name directory, nothing else
   (inventory §4). Reading the current table at execution is a `load_table`, a catalog call on Glue and S3 Tables,
   unless the session keeps that state itself (seat A). (b) "At execution" must follow Spark's per-query rule, not
   every scan: the six own-plan sequences of domain table §5 (12 cells per build) are EQUAL today only because both
   engines keep the old snapshot there, and a re-bind on every action would turn them WRONG, a returned deleted row
   included. Unit 1's own-plan memo is the seat for that.
4. **Fable's pinned list needs one correction [M].** Spark pins only `VERSION AS OF` / `TIMESTAMP AS OF` (all 68
   such cells are EQUAL and must stay so). The `t.branch_b` and `t.tag_g` identifiers, the reader's `branch` option
   and the metadata tables are re-read at the action on Spark, and WRONG on main (16 cells).
5. **The seats [Rec].** **Seat A (recommended):** a session table state that a decorator over the session's catalog
   handle updates on every same-session commit, read by one optimizer rule that re-binds each catalog-backed Iceberg
   scan once per plan, under unit 1's own-plan memo, with Spark's identity and column checks. No catalog call is added
   on any catalog. **Seat B:** the same rule and memo, re-binding by `load_table`: no commit-side code, one catalog
   call per held Iceberg table per action on Glue and S3 Tables, and a held frame that sees another writer's commit
   (Spark's does not).
6. **The split [Rec].** Unit 1, uncached (it can start before A1 merges): 294 of the 327 WRONG cells, the path read
   by Q4. Unit 2 after A1: the 4 cached cells. The Parquet inverse stays a named S3 divergence (Fable). Glue and S3
   Tables are read, not measured: a gap for the owner-gated AWS acceptance leg.
7. **Cost [M, R].** No exclusive sample could be taken (another lane held the measure lock); read estimates:
   seat A adds no catalog call and about 1.00x on every row; seat B adds one `load_table` per held action, a
   `GetTable` on Glue, over 1.15x on a short held read there (§6). C-012 is about 1.00x for both (a reading).
8. **Questions Q1 to Q8** (§7), each with a default.

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

## 2. RePark's mechanism [R, M]

A held RePark frame is a DataFusion `LogicalPlan` whose `TableScan` holds the provider built when its name was
resolved (inventory B1). The fork's `from_planning_load` stores that load's `Table` in `planning_table` (B2), and
`IcebergTableProvider::scan` reads `planning_table` on every scan (B3). Every action re-plans the same `LogicalPlan`,
so every action reads the resolution's snapshot. Writes load the current table (B4) and commit; the commit doors'
re-registration (inventory §4) rebuilds only the name directory and never reaches a held provider. A SQL temporary
view re-plans its text at every scan (V2), so it is EQUAL; a DataFrame view holds the frame's plan (V1), so it is
stale. Time travel, refs and the path read use static providers (T1 to T3, F4); metadata tables are bound at
resolution (T4). Main and main before unit 1 bind identically; unit 1 (#1373) touched none of these sites.

Measured: base equals main in all 1,860 verdict cells; the ANSI mode moves no verdict. The external writer on
`type=hadoop` is never visible to RePark, held or fresh (the pointer lives in the session; domain table §6), so the
local catalog cannot show an over-refresh; Glue and S3 Tables can (§4).

## 3. The domain [M]

On a scalar Iceberg v2 table on the local `type=hadoop` catalog, Spark 4.1.2, main and base, ANSI on and off, 465
verdict cells per build and mode and 6 external-writer cells ([domain-table.md](domain-table.md)):

- **Holds (10):** `spark.table`, `spark.sql`, `read.table`, the path read, `filter`, `groupBy().count()`, a DataFrame
  view, a SQL `CREATE TEMPORARY VIEW … AS SELECT`, and each side of a join with a fresh read.
- **Writes (17 and a control):** `INSERT`, `DELETE`, `UPDATE`, `MERGE`, `INSERT OVERWRITE` static and dynamic, the six
  `writeTo` verbs (`create` after a drop), RTAS, `ALTER` add, drop and rename a column, drop and recreate.
- **Actions (6):** `collect`, `count`, `show`, `writeTo(copy).create()`, `INSERT INTO u SELECT * FROM <view>`, a
  self-join; plus 15 action sequences (§5), the time-travel, ref and metadata-table frames (§4), four cached frames
  and the Parquet inverse.

| | WRONG (silent) | stored | Spark raises, RePark answers | EQUAL |
|---|---|---|---|---|
| per build and mode | 327 | 62 | 44 | 138 |

The wrong answers are always the resolution's rows: two rows after an `INSERT` of a third, the deleted row after a
`DELETE`, the old values after `UPDATE` and `MERGE`, the old table after `INSERT OVERWRITE`, RTAS, `replace`,
`createOrReplace` and a recreate, and the old rows with no error after a dropped or renamed column. The 62 stored
cells are `held.writeTo(copy).create()` and `INSERT … SELECT` from a view of the held frame, for every hold but the
SQL view. The step 0 that found it measured 12 count cells; Fable measured 13 shapes; this table holds both, and
Fable's 13 answers reproduce on main.

## 4. Seats (two, Rust-first) [Rec]

Both seats share one **re-binding point** and three rules taken from Spark; they differ only in where the current
table comes from.

**The shared part.**

- **One optimizer rule** (new, `crates/repark-spark/src/view_ddl/iceberg_rebind.rs`, installed beside
  `ReaderParseRule` in `frame_settings.rs:318-327`). Every action re-optimises its plan, so the rule runs once per
  action, and once per query over a DataFrame view (the view's plan is optimised when the query plans it). It walks
  the plan, subqueries included, and for each `TableScan` over a catalog-backed Iceberg provider (inventory B1), a
  metadata-table provider (T4) or a ref named by identifier (T3) decides **once per `(catalog, identifier)` per plan**
  (Spark's `currentTables`: a self-join reads one snapshot). The static time-travel providers (T1, T2, T5, the
  `VERSION AS OF '<ref>'` form) are not touched: the 68 pinned cells stay.
- **Spark's checks** on a re-bound table: another table UUID raises
  `INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.TABLE_ID_MISMATCH`; an advertised field missing, renamed or retyped (by
  field id) raises `….COLUMNS_MISMATCH`; added fields are allowed and the frame keeps its columns. The re-bound
  provider keeps the advertised schema, so the plan's column ordinals stay valid: a fork constructor,
  `IcebergTableProvider::rebound(table)` beside `from_planning_load` (fork `table/loaded.rs`), and an accessor that
  rebuilds a metadata-table provider over a newer table (fork `table/metadata_table.rs`). The fork's scan already
  reads a newer table under a fixed schema (the `try_new` path does it today).
- **Spark's own-plan rule**, through unit 1's memo (`ReaderParseMemo`, `extension.rs:84`, entered by `_own_plan`,
  `surface_a.py:406`): inside an own-plan scope the first decision per table is recorded on the frame object and
  reused by its later own-plan actions; every other action decides afresh. `explain` (`core.py:2485`) enters the memo,
  as Spark's `explain` plans the Dataset's own `queryExecution`. This keeps the six own-plan rows of domain table §5.

### Seat A — a session table state, kept by the commits (recommended)

- **Where the current table comes from.** A per-session `SessionTables` map, `(catalog, identifier) → (epoch, Table
  or a drop marker)`, in a new `crates/repark-iceberg/src/catalog/session_tables.rs`, filled by a `Catalog` decorator
  over the session's registered handle (inventory R1), shaped like the existing `EncryptionGuardCatalog` (R5): it
  passes every call through and records what `update_table`, `create_table`, `publish_create_table`,
  `publish_replace_table` and `register_table` return, a drop marker on `drop_table`, and both names on
  `rename_table`. Every write door of inventory §4 commits through that handle, so **no commit door is edited**. The
  rule re-binds a scan when the map holds a newer state than the provider's planning load (by metadata location and
  `last-updated-ms`; a provider planned after the commit already has it). Q2 decides whether planning loads also
  update the map.
- **This is Spark's `CachingCatalog` shape:** the session's own commits are seen at once; another writer's are not
  (on Glue and S3 Tables a fresh read still sees them, as it does today; Q2).
- **Product files: 9, plus 2 fork files.** `catalog/session_tables.rs` (new), `catalog/mod.rs` (224 lines), the
  wrap at registration (`crates/repark-core/src/session.rs:458-482`; the file is at its 1,000-line ceiling, so the
  wrap must be line-neutral there or sit in the caller), the owner of the map on the session state, the rule (new),
  `view_ddl/frame_settings.rs` (523), `crates/repark-core/src/extension.rs` (414), `crates/repark-spark/src/
  time_travel.rs` (879, the identifier-ref marker), `python/repark/src/repark/spark/dataframe/core.py` (`explain`
  under the memo; at its exact 3,461-line baseline, so line-neutral). Q4 (a) adds `crates/repark-core/src/
  iceberg_path.rs`.
- **Cells moved:** 294 per build and mode (242 catalog-table frames, 27 DataFrame views, 16 refs and metadata tables,
  9 sequences); 27 more for the path read under Q4 (a). **EQUAL lost: none known.** The external-writer control is
  unchanged on every catalog: a held frame never sees another writer's commit, like Spark's before a refresh.
- **`EXPLAIN`:** unchanged text. The physical `IcebergTableScan` line prints projection and predicate only
  (fork `physical_plan/scan.rs:470-492`); `EXPLAIN VERBOSE` prints the re-bound `snapshot_id`.
- **Cost:** no catalog call on any catalog. Per action, one plan walk and one map lookup per Iceberg scan; per
  commit, one map insert. A held frame after a write does the new snapshot's work, which is what Spark does. Glue and
  S3 Tables cost the same as the local catalog (§6).
- **A1:** the map's epoch is the token A1's lazy cache table needs to drop its fill after a same-session write
  (unit 2), and it can replace C-009's `DESCRIBE TABLE EXTENDED` read (0.292 ms a call) with a lookup.

### Seat B — re-bind by `load_table`

- **Where the current table comes from.** `catalog.load_table(identifier)` at the rule, once per table per plan. No
  map, no decorator.
- **Product files: 6, plus the same 2 fork files** (the rule, `frame_settings.rs`, `extension.rs`,
  `time_travel.rs`, `core.py`, the session wiring).
- **Cells moved:** the same 294 (and 27) on `type=hadoop`, where `load_table` reads the in-process pointer.
- **EQUAL lost, on Glue and S3 Tables (read, not measured):** a held frame sees another writer's commit at its next
  action; Spark's answers the cached table until `REFRESH TABLE` or the idle expiry. That is the over-refresh the
  brief warns of.
- **Cost:** every action on a held Iceberg frame pays a `load_table`, with or without a write, and a fresh
  `spark.table(t)` acted on later pays a second one. On `type=hadoop` it is a pointer read and a metadata-cache hit
  (§6 `plan`). On Glue it is one `GetTable` and on S3 Tables one metadata-location call per table per action (read
  estimate: tens of milliseconds a call in region; not measured), so a short held read can pass 1.15x: an owner
  row under the cost rule.

**Recommendation: seat A.** It moves the same cells, adds no catalog call, keeps the external-writer control on the
label's catalogs, and gives A1 its invalidation token. Its price is three more files (the map, the decorator, the
wiring) and a dependency on every commit going through the session's handle, which inventory §4 confirms for the Spark
door.

## 5. The unit split under the one-round cap [Rec]

- **Unit 1 — the uncached re-bind (seat A; Opus 5.5 xhigh builder, Opus 5.5 medium verifier, one round).** The
  map and decorator, the rule with Spark's checks, the memo binding, `explain` under the memo, refs and metadata tables
  (Q5), the view and write-of-held-frame rules (Q6). **Bar:** the 294 WRONG cells per build and mode EQUAL in both
  modes (588 per build); every EQUAL cell of the table held, named: the 68 pinned and ref cells, the 28 SQL-view
  cells, the six own-plan rows, the controls; a mutation proof on the memo (drop it and the six rows turn WRONG) and on
  the identity check. The path read joins the bar only under Q4 (a). It can start before A1 merges (Fable §13: the
  uncached half first). **Overlap with A1:** `extension.rs` and `core.py` may both be touched; the second PR to merge
  rebases, and `core.py` stays at its baseline either way.
- **Unit 2 — the cached cells, after A1 merges.** A1's lazy cache table drops its fill when the map's epoch for a
  table it reads moves; C-009's token check reads the epoch. **Bar:** the 4 `C` cells per build and mode (16), with
  catalog-surface-1's C-009 pins and A1's bar held.
- **Not a unit:** the Parquet inverse (S3, a named divergence, Fable §13); the `type=hadoop` catalog's blindness to
  another session or process (domain table §6), which is pre-existing and not this card's mechanism; Glue and S3
  Tables live, the owner-gated AWS acceptance leg (§6).
- **The one-round cap.** Unit 1 is one mechanism with one insertion point on the read side and one on the commit side;
  if its verifier fails on the Spark-shaped checks (Q6), the fallback is Q6 (b), raising the query path's condition,
  which keeps the round loud rather than silent.

## 6. Cost (design estimates) [M, R]

**No exclusive sample was taken.** From about 05:42 UTC to the hand-back, the measure lock's shared side was held by
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
modules) hold no held-frame cell today: unit 1's pins should add the `HW` `insert`, `delete` and `copy` rows and the
external-writer control there.

## 7. Questions (for the Fable consult; each ruled from the tables)

- **Q1. The seat.** (a) seat A, the session table state kept by the commits; (b) seat B, `load_table` at the
  re-bind. Both move the same 294 cells on the local catalog (§4). **Default (a):** no catalog call on Glue or S3
  Tables, and a held frame does not see another writer's commit, as on Spark (domain table §6).
- **Q2. What the session table state records (seat A).** (a) the session's commits only; (b) the commits and every
  planning load newer than the entry, so a held frame is never older than a read the session has already made.
  Spark's cache holds whatever the session last loaded or committed, so its held and fresh reads always agree (§6:
  2, 2 before the refresh, 3, 3 after). On `type=hadoop` the two are the same (a load reads the session's pointer).
  **Default (b).** It differs from (a) only on Glue and S3 Tables after another writer's commit.
- **Q3. The own-plan rule.** (a) Spark's: an own-plan action (`collect`, `toLocalIterator`, `toArrow`, `toPandas`,
  `tail`, `explain`) keeps the first own-plan binding of its frame object, through unit 1's memo; (b) re-bind on every
  action. (b) turns domain table §5's six EQUAL rows WRONG (12 cells per build, one of them a deleted row returned
  where Spark keeps it). **Default (a).**
- **Q4. The path read** (`read.format("iceberg").load(path)`; 27 WRONG per build and mode, 4 stored). Spark refreshes
  it like a catalog table. (a) unit 1 re-binds it too, re-running the Hadoop discovery (version hint, then the
  listing) at each re-bind: a file read per action locally, object-store reads on S3; (b) keep the static path read,
  file it as its own card at S2: a silent wrong read, but outside the label's catalog doors. **Default (b).**
- **Q5. Refs and metadata tables** (16 WRONG per build and mode). (a) follow Spark: re-bind the `t.branch_b` and
  `t.tag_g` identifiers, the reader's `branch` option and the metadata tables, keep every `VERSION AS OF` /
  `TIMESTAMP AS OF` pinned; (b) keep them pinned and name the 16 cells. **Default (a):** small, and the same rule.
- **Q6. Spark's re-resolution by name for writes and views.** After a schema or identity change, Spark's
  `held.writeTo(copy).create()` and SQL over a DataFrame view read the current table in its current shape
  (`(5,r)` after a recreate, `(1,) (2,) (9,)` after a dropped column), where its `count` raises; a DataFrame view
  raises `INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION` for a dropped or renamed column (domain table §1,
  §2; 13 cells per build and mode). (a) match Spark: commands and views re-resolve by name, queries check; (b) apply
  the query path's checks everywhere, so those cells raise where Spark answers (loud, RAISE). **Default (a)**, with
  (b) as the unit's fallback under the one-round cap.
- **Q7. The rating.** (a) S1, label-blocking for ICE-V2-FLAT-AWS-LABEL-1 (Fable); (b) S1, not label-blocking.
  **Default (a):** 327 silent WRONG cells per build and mode, 62 of them stored, on a scalar v2 table through the
  Spark door, and the path is catalog-agnostic (inventory §1).
- **Q8. The order.** (a) unit 1 takes the next builder slot without waiting for A1; unit 2 follows A1's merge
  (Fable §13); (b) both wait for A1. **Default (a).** Cells: unit 1's 294 have no cache in them.

## 8. What the brief and the earlier records got wrong

1. **Fable §13: "a commit re-registers the namespace's providers and a held plan keeps the old one."** The
   re-registration rebuilds only the namespace's name directory; no provider is cached anywhere, and every planning
   round builds a fresh one. The pin is the fork's planning fast path: `from_planning_load` keeps the planning
   load's `Table` and every scan reads it (inventory §1). The conclusion stands (not catalog-specific).
2. **Fable's first option: "the scan reads the session catalog's current table at execution, which a same-session
   commit already updates, so no catalog call is added."** RePark has no such session state: on Glue and S3 Tables
   the current table is a catalog call away. Seat A builds that state; seat B pays the call. And "at execution" must
   be Spark's per-query rule with the own-plan memo, or 12 EQUAL cells per build turn WRONG (§0 point 3).
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

Up: [map.md](map.md).
