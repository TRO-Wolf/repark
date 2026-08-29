# RePark roadmap — design plan by crate (work cards)

Companion to [release-roadmap-2026-08-29.md](release-roadmap-2026-08-29.md) (the *what* and *when*). This file is the *where* and
*how*: for every roadmap item, which crate is **NEW** or **UPDATED**, which files it touches,
which reference implementation to read, and the steps in order — written so a delegated
sub-agent (the delegated implementation tier executes a card; the narrow mechanical tier does
the checklist steps) can start without re-deriving the architecture. The tiers are roles, not
vendors: which model fills each one is a tool-adapter mechanic (AGENTS.md "Delegated work"),
never named in this plan. Drafted 2026-08-29; the six placement decisions in §6 were ruled the
same day (D-1, D-2, D-4, D-6 confirmed; D-5 overruled; D-3 amended once the DAG guard's
bindings rule was checked against the server edges), so this file is the plan of record.

---

## 0. How to read a work card

Every item below is one card with the same fields:

| Field | Meaning |
|---|---|
| **Home** | The crate(s) and directories. `NEW` = crate does not exist; `UPDATE` = it does. Tier and role per `scripts/check_crate_dag.py`. |
| **Edges** | Internal Cargo edges to declare in `ALLOWED_EDGES` (kind + one-line reason). Nothing else may be added. |
| **Reference** | Where to read a working implementation first. `LE/` = `~/CodeRepos/Research/LocalEngines/`, `FORK/` = the owned iceberg-rust fork checkout, `DF` = DataFusion. Read, never copy. |
| **Steps** | Ordered pseudocode. Each step names the file it lands in. |
| **Pins** | The tests that must go red first, then green (docs/testing.md). Oracle = Spark 4.1.2 on this box unless stated. |
| **Done when** | The acceptance line from the roadmap. |
| **Hand back when** | The decisions a sub-agent must *not* make alone — stop and report. |

**Standing rules a sub-agent inherits, by pointer only (do not restate, do not reinterpret):**
AGENTS.md (read path, precedence, verify gate) · docs/testing.md (pin-first) ·
`.agents/skills/engineering-method/SKILL.md` · **no comments in code files** (owner ruling
2026-08-26; markdown carries the prose; `pins:` citations live in the directory `map.md`) ·
`map.md` lockstep in the same commit · the pre-push forbidden-literal gate (no home paths, no
real bucket names, no session trailers) · never edit inside `LE/*` clones (they are pristine
upstream trees) · never point a reviewer at the live worktree.

### The crate map today (tiers from `scripts/check_crate_dag.py`)

```
tier 0 foundation     repark-common
tier 1 table service  repark-iceberg
tier 2 engine         repark-core          (ReparkSession, seams, spill, catalog_config, read_*)
tier 3 capability     repark-functions  repark-ta  repark-ml
tier 3 door           repark-spark  repark-sql
tier 4 bindings       repark-python        (nothing depends on it)
tier 4 server         repark-server  repark-server-{spark-connect,flight-sql,substrait}  repark-cli
                                           (planned; only other tier-4 server crates depend on them)
planned (manifest)    repark-exec  repark-io  repark-connect  + the five server-family crates above
                                           (paths must stay empty until chartered)
python                python/repark        (facade: repark/spark/*; native door today = repark.sql())
```

Rule of thumb for placing new code: a product edge may never point at a *strictly higher* tier.
Anything `repark-core` must call sits at tier ≤ 2. Anything that needs a door sits at tier ≥ 3.
A server that needs a door and the session is a tier-4 adapter like `repark-python`, but under
its own role `server` (D-3): the `bindings` rule forbids every inbound edge, and the protocol
crates and the CLI must depend on `repark-server`.

### Checklist — adding a NEW crate (mechanical-tier grade)

1. `Cargo.toml` (root): add to `[workspace] members`; add `[workspace.dependencies]` entry
   `repark-x = { path = "crates/repark-x", version = "0.0.0", default-features = false }`.
2. `scripts/check_crate_dag.py`: add to `TIERS`, `ROLES`, and every edge to `ALLOWED_EDGES`
   with kind + reason. A crate that introduces a role (the first `server` crate) also extends
   `ROLE_NAMES`, the role gloss, and `forbidden_reason` in the same change. `make check-crate-dag`
   must pass.
3. `repo-manifest.toml`: flip `status = "planned"` → `"delivered"` and add `layer = …`
   (or add a new `[components.repark-x]` block). `make check-manifest` must pass.
4. `crates/repark-x/src/lib.rs` thin (declarations + re-exports; `make check-lib-rs`), every
   module its own file, ≤ 1000 lines per file, comment ceiling **zero** for new files.
5. `crates/map.md` row + `crates/repark-x/map.md` (Purpose / Contents / I want to… / Pointers).
6. AGENTS.md change-location table: the row's Status column.
7. `make ci` green before the first commit.

---

## 1. Pre-1.0

### v0.6 — Track-B DML remainder

#### Card DML-A — `MERGE … WHEN NOT MATCHED BY SOURCE`

- **Home:** UPDATE `repark-iceberg/src/write/merge/` (the third arm: COW and MOR paths);
  UPDATE `repark-spark/src/merge.rs` and `repark-sql/src/merge/` (grammar + routing).
- **Edges:** none new.
- **Reference:** Spark `MergeIntoTable` semantics (oracle); existing two arms in
  `repark-iceberg/src/write/merge/` for the cardinality + store-assignment gates to reuse.
- **Steps:**
  1. `repark-iceberg/src/write/merge/`: extend the matched/not-matched plan with a
     `NotMatchedBySource { condition, action: Delete | Update(assignments) }` arm; reuse the
     existing cardinality gate; target rows with no source match feed the arm.
  2. COW: rows hit by the arm are rewritten (UPDATE) or dropped (DELETE) in the file-scoped
     rewrite; MOR: they become deletion-vector positions (v3) / position deletes (v2).
  3. Both doors: parse the arm; refuse if the dialect matrix (`matrix.rs`) says a form is
     out of scope; route to the same `MergeSpec`.
- **Pins:** entry-point-matrix rows for the arm × {COW, MOR} × {v2, v3}; oracle rows from
  Spark with an SCD-2 shaped fixture; row-lineage guard pin (v3 lineage survives the arm).
- **Done when:** the DML-3 boundary in the registry moves; all three arms in one statement.
- **Hand back when:** the arm interacts with `write.delete.granularity` (MW-9) — do not pick a
  default.

#### Card DML-B — `INSERT OVERWRITE … PARTITION (…)` static + dynamic

- **Home:** UPDATE `repark-iceberg/src/write/overwrite.rs`; UPDATE
  `repark-spark/src/insert_overwrite.rs`; UPDATE `repark-sql` equivalent route.
- **Edges:** none new (fork pin already carries F-5 / #217 — verified 2026-08-29).
- **Reference:** `FORK/crates/iceberg/src/transaction/overwrite_files.rs`
  (`overwrite_by_row_filter`, `validate_added_files_match_overwrite_filter`) and
  `…/replace_partitions.rs`; Spark's `OverwriteByExpression` vs `OverwritePartitionsDynamic`.
- **Steps:**
  1. Static `PARTITION (k=v, …)`: build the row filter `k=v AND …`; call
     `Transaction::overwrite_files().overwrite_by_row_filter(filter)` +
     `validate_added_files_match_overwrite_filter`; append the new files; commit.
  2. Dynamic / `writeTo().overwritePartitions()`: write files, then
     `Transaction::replace_partitions()` with `add_file` for each; commit.
  3. Engine-side guard: empty input to a dynamic overwrite refuses (Spark does this engine-side).
  4. Facade: `DataFrameWriterV2.overwritePartitions()` and `overwrite(condition)` leave the
     refuse list in `python/repark/src/repark/spark/dataframe/`.
- **Pins:** `insert_overwrite.rs::empty_insert_overwrite_partition_refuses_full_wipe` and
  `…_nonempty_refuses_whole_table_replace` flip from refuse-loud to partition-scoped with an
  oracle row each; a sibling-partition-untouched pin.
- **Done when:** both forms produce Spark's file set on the shared fixture.
- **Hand back when:** a multi-spec (partition-evolved) table is in the fixture — optional
  interop leg, owner-gated.

#### Card DML-C — `TRUNCATE TABLE`

- **Home:** UPDATE `repark-spark/src/router/` + `repark-sql/src/router/` (statement);
  UPDATE `repark-iceberg/src/write/overwrite.rs` (an empty overwrite made first-class).
- **Steps:** parse → `TruncateSpec { table }` → whole-table `overwrite_files` with an empty
  input → new snapshot with zero data files; metadata/history preserved.
- **Pins:** snapshot count +1, `table$files` empty, time travel to the prior snapshot works.
- **Hand back when:** never — this card is mechanical-tier grade once DML-B lands.

#### Card MAINT — `rewrite_data_files` `where` / `sort_order` / strategy

- **Home:** UPDATE `repark-spark/src/call.rs` (`execute_rewrite_data_files`) +
  `call_args.rs`; the sort strategy uses `FORK/…/transaction/sort_order.rs` +
  `rewrite_files.rs`.
- **Steps:** accept `where` (a row filter that narrows candidate files via
  `write/scan_prune.rs`), `sort_order` (write the rewritten files sorted; declared-sort metadata
  updated), `strategy = binpack | sort`; keep the v3 lineage guard
  (`refuse_v3_rewrite_that_would_lose_row_lineage`) in front.
- **Pins:** candidate-set pin per `where`; ordering pin on rewritten files; MW-7 numbers as
  the size defaults.

#### Card WAP — branch / tag writes + write-audit-publish

- **Home:** UPDATE `repark-iceberg/src/write/snapshot_refs.rs` (commit target = branch);
  both doors' `ref_ddl`; fork **F-6** (branch commit target) must land first.
- **Hand back when:** F-6 is not in the pin — file the fork handoff, do not work around it.

### v0.7 — Full example documentation

- **Home:** UPDATE `docs/` (new `docs/examples/` tree) + `python/repark-parity` fixtures the
  examples run against; every example is a test (`make py-test-facade` picks it up).
- **Steps:** one page per surface family (Iceberg DDL/DML/maintenance, both doors, TA, ML,
  config) with a runnable block and its recorded output; a `scripts/check_examples.py` gate that
  executes each block and diffs the output.
- **Done when:** the drift gate is in `make ci` and red on a stale output.

### v0.8 — Torture-test dataset suite

- **Home:** UPDATE `python/repark-parity/fixtures/torture/` (generated, seeded); UPDATE
  `repark-core/src/read_options.rs` for the opt-in secrets flag; later `repark-io` owns the
  reader when it exists.
- **Reference:** `LE/polars/crates/polars-io/src/csv/read/schema_inference.rs` (what a serious
  inferencer handles); `crates/repark-core/src/catalog_config.rs::prop_key_is_secret` +
  `python/repark/src/repark/spark/_secrets.py` (the needle set to reuse for column values).
- **Steps:**
  1. Generator script producing deep nesting, mixed encodings, ragged CSV, unicode headers,
     nulls-in-keys, 10k-column wide, and a "secrets-shaped" column set.
  2. Read-time flag `flag_secret_columns = off | warn | refuse` in `read_options.rs`; detection
     reuses the needle set, exact-name match only.
  3. Suite runs both doors over every fixture; the v1.2 `dynamicFlatten` work and the v0.9
     matrix consume these fixtures.
- **Hand back when:** a fixture needs a reader feature that does not exist — record the gap,
  do not build the reader here (that is v1.3 / `repark-io`).

### v0.9 — Never-OOM truth

- **Home:** UPDATE `repark-core/src/session/spill.rs` (already: fair spill pool, temp dir, the
  `SET` path); NEW measurement matrix under `python/repark-parity/tests/spill/`; DataFusion
  upstream for operators that cannot spill (W-3 window). `repark-exec` is **not** created for
  this — no new operator code arrives; extraction stays deferred per ADR-0005 §4.
- **Reference:** `LE/polars/crates/polars-ooc/src/` (`memory_manager.rs`, `spill_context`) for
  the shape of an honest OOC story; DF `FairSpillPool`, `DiskManager`.
- **Steps:**
  1. Matrix rows = operator × input-size/limit ratio (sort, hash agg, hash join, window,
     `dynamicFlatten`) at `memory_limit_bytes` fixed and input 2×, 4×, 8× the limit.
  2. Each cell records: spilled / completed / refused-loud; assert no silent OOM kill.
  3. Publish the matrix into `docs/` and the PROJECT.md goal line points at it (pointer only).
- **Done when:** every cell is one of the three states and the doc names each "cannot spill"
  cell with its upstream issue.
- **Hand back when:** a cell needs an operator change — that is W-3 (v1.2) or upstream.

### v0.10 — `repark.toml` and named sources

#### Card CFG-1 — the loader (Rust-owned)

- **Home:** UPDATE `repark-core`: new module family `src/config_file.rs` +
  `src/config_file/{discovery,profile,interpolate,redact,sources}.rs` (ADR-0005 §4 precedent:
  standalone policy gets its own module, `session.rs` does not grow). Builder hook in
  `session.rs` (`ReparkSessionBuilder::from_config_file(path?)`), precedence merged before
  `build()`. Deps: `serde`, `toml` (workspace-pinned).
- **Edges:** none new.
- **Reference:** `crates/repark-core/src/catalog_config.rs` (`CatalogSpec`, `CatalogKind`,
  `parse_catalog_specs`, `prop_key_is_secret`) — the config file is a second *source* of the same
  specs, not a second spec type.
- **Steps:**
  1. `discovery.rs`: `$REPARK_CONFIG` → `./repark.toml` → `~/.config/repark/repark.toml`;
     first hit wins; absent = empty config, never an error.
  2. `profile.rs`: parse `[default]` and `[<profile>]` tables; select by `REPARK_ENV`; deep
     merge profile over default; unknown top-level keys refuse-loud with the key path.
  3. `interpolate.rs`: `${ENV_VAR}` only (no nesting, no defaults syntax); missing var =
     refuse-loud naming the key path, never an empty string.
  4. `sources.rs`: `[<profile>.catalog.<name>]` → `CatalogSpec` (Java-class keys accepted 1:1;
     native `type = "glue" | "s3tables" | "memory" | "rest"`); `[<profile>.database.<kind>.<name>]`
     → `SourceSpec` (kinds `postgres | sqlserver | trino`). Names must be unique per profile
     across both families → refuse-loud on collision.
  5. `redact.rs`: `conf_dump()` masks any key `prop_key_is_secret` matches; values never
     leave Rust unmasked.
  6. Precedence: builder `.config()` > `REPARK_ENV` profile > `[default]`; recorded in the
     dump with a `source` column.
  7. `repark-python/src/session.rs`: `PyReparkSession::new(config_path: Option<str>)` and
     `sources()` (redacted); `python/repark/src/repark/config.py`: Pydantic v2 mirror for typed
     construction only (never the parser).
- **Pins:** discovery order; profile merge; `${VAR}` missing refuses; collision refuses; dump is
  redacted; precedence table as a parametrised test; Java-key ↔ native-key equivalence.
- **Done when:** a session built from a file registers the same catalogs the equivalent
  `.config()` calls would (existing `register_configured_catalogs` path, byte-identical specs).
- **Hand back when:** a key does not map onto an existing `CatalogSpec` field — do not invent a
  field.

#### Card CFG-2 — named-source registration seam

- **Home:** UPDATE `repark-core/src/catalog_state.rs` (the registry gains `SourceKind::Database`
  entries); the providers themselves land in `repark-connect` (v1.6). In v0.10 a database source
  is **parsed, validated, listed, and refuses-loud on use** ("connector arrives in v1.6").
- **Steps:** `register_configured_sources()` beside `register_configured_catalogs()`; lazy —
  nothing connects at build; `auto_register = false` opt-out honored; `ping()` refuses until
  v1.6.
- **Pins:** a configured Postgres source appears in `repark.sources()` and
  `SELECT … FROM company_db.public.t` refuses with the v1.6 message, not "table not found".

---

## 2. v1.0 — Iceberg format-v3

Governed by `task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md` and its acceptance matrix;
units are chartered there (RP-3 next, then V3-3+). No card here — do not fork the definition.

---

## 3. 1.x

### Card 1.1 — FNP + TA campaigns

- **Home:** UPDATE `repark-functions` (FNP-15/16 next, per `briefs/next-sequence.md`) and
  `repark-ta` (golden-safe perf: `crates/repark-ta/src/{overlap,momentum,…}.rs` and the
  `udf/` window layer). Existing campaigns; their ledgers govern.
- **Hand back when:** a fix changes a working query's result (that is SEM, held).

### Card 1.2 — Window performance + `dynamicFlatten`

- **Home:** UPDATE `repark-core/src/dynamic_flatten.rs` + `dynamic_flatten/`; UPDATE
  `repark-spark/src/window_range.rs`; window operator work is **upstream DataFusion** (W-0…W-2
  from the 2026-08-23 intake) — an engine-side workaround is not this card.
- **Reference:** `LE/polars/crates/polars-stream/src/nodes/` (morsel-driven window/group nodes,
  for the shape of a streaming window); DF `BoundedWindowAggExec`.
- **Steps:** baseline benchmark recorded first (`benches/`), then per-unit: flamegraph →
  allocation/plan-shape fix → same benchmark. `dynamicFlatten` runs over the v0.8 torture suite
  with a spill row in the v0.9 matrix.
- **Pins:** benchmark baseline files; torture-suite correctness pins; spill cell green.

### Card 1.3 — I/O parity with Polars → `repark-io` NEW

- **Home:** NEW `crates/repark-io` — **tier 1, role "table service"** (recommendation: it hands
  `TableProvider`s / batches to `repark-core`, so it must sit below tier 2; confirm with the
  orchestrating agent before the DAG edit). Modules: `csv/` (smart inference), `excel/` (read + write),
  `json/`, `ipc/`, `avro/`, `hive.rs` (partitioned-directory discovery over DF's listing
  table). Python native door: NEW `python/repark/src/repark/io/` (`scan_csv`, `scan_parquet`,
  `scan_ndjson`, `scan_ipc`, `read_excel`, `write_excel`, `sink_*`).
- **Edges:** `repark-core → repark-io` (normal; "read_* entry points delegate to the readers");
  `repark-io → repark-common` (normal; error seed). No door edge.
- **Reference:** `LE/polars/crates/polars-io/src/csv/read/{schema_inference,parser,options}.rs`,
  `…/hive.rs`, `…/ndjson`, `…/avro`, `…/ipc`; Excel: `LE/polars/py-polars/src/polars/io/spreadsheet/functions.py`
  (the option surface to match; engine in Rust = `calamine` read, `rust_xlsxwriter` write);
  the parity checklist = the `pl.read_*` / `pl.scan_*` / `sink_*` signatures at
  `LE/polars/py-polars/src/polars/io/`.
- **Steps:**
  1. Crate checklist (§0). Move the existing `read_csv`/`read_json` option plumbing from
     `repark-core/src/read_options.rs` behind the new crate, behavior-preserving, pins green
     before any new feature.
  2. `excel/`: retire the deferred refusals in `repark-python/src/session.rs`
     (`read_excel`, `excel_sheet_names`) — the withheld tests in
     `task/port/deferred-tests.md` are the pins.
  3. `csv/`: inference over the v0.8 torture fixtures; `_csv_smart.py` (facade) becomes a thin
     caller of the Rust inferencer.
  4. Hive-partitioned reads: DF listing table + partition-column typing; Polars' `hive.rs`
     option names.
  5. Per-format parity table in `python/repark-parity/` — one row per Polars option, state
     `parity | declared-difference | out-of-scope`.
- **Done when:** the parity table has no blank cell; Excel/CSV differentiators are named in the
  charter and demonstrated in `docs/examples/`.
- **Hand back when:** an option needs a Polars-only type (e.g. `Categorical` semantics) —
  declare the difference, do not emulate.

### Card 1.4 — Functions / expressions / transformations parity with Polars (the native lazy door)

- **Home:** NEW Python surface `python/repark/src/repark/{lazyframe,expr,functions}/` over
  DF's `DataFrame` + `Expr` through `repark-python` (UPDATE `dataframe.rs`, `column/`); Rust
  kernels that DF lacks land in `repark-functions` (a `polars` semantic family beside the Spark
  one, DataFusion-native, no `repark-core` dep). The Polars matrix leg lands in
  `python/repark-parity/` (ruled R-2).
- **Edges:** none new (`repark-python → repark-functions` exists).
- **Reference:** `LE/polars/crates/polars-plan/src/dsl/` (the `Expr` surface),
  `LE/polars/py-polars/src/polars/expr/`, `LE/polars/py-polars/src/polars/lazyframe/`;
  optimizer behaviours to *not* promise: `LE/polars/crates/polars-plan/src/plans/optimizer/`.
- **Steps:** generate the method census from `py-polars` (like the 2,509-pin PySpark cohort);
  bucket `direct DF | kernel needed | declared-difference`; ship by namespace (`str`, `dt`,
  `list`, `struct`, `arr`); each namespace = one unit with its matrix rows against the Polars
  oracle (a `pip install polars` on this box).
- **Hand back when:** a semantic requires changing a Spark-door result (never), or a new DF
  physical operator.

### Card 1.5 — PySpark functions/transformations parity

- **Home:** UPDATE `repark-functions`, `python/repark/src/repark/spark/functions_*.py`; the FNP
  campaign over the 2,509-pin cohort. Existing ledgers govern.

### Card 1.6 — Postgres / SQL Server / Trino → `repark-connect` NEW

- **Home:** NEW `crates/repark-connect` — **tier 1, role "table service"** (peer of
  `repark-iceberg`). Modules: `source.rs` (`SourceSpec` from CFG-1), `provider/{catalog,schema,
  table}.rs` (DF `CatalogProvider` / `SchemaProvider` / `TableProvider` per source),
  `pushdown.rs` (projection / filter / limit → remote SQL; Trino: whole-subtree),
  `read/{postgres,mssql,trino}.rs` (partitioned parallel reads → Arrow),
  `write/{postgres_copy,mssql_bulk,trino_insert,row}.rs`, `pool.rs` (per-session, lazy),
  `types.rs` (remote ↔ Arrow type map, one table per backend).
  UPDATE `repark-core/src/catalog_state.rs` (register providers under the source name),
  `repark-sql` + `repark-spark` (`INSERT INTO <source>.<schema>.<table> SELECT …` routing to
  the sink; `EXPLAIN` shows the pushdown boundary per source).
  Python: `read_database` / `write_database` conveniences only (`python/repark/src/repark/io/`);
  retire the deferred `read_postgres` refusal in `repark-python/src/session.rs`.
- **Edges:** `repark-core → repark-connect` (normal; "registers the providers");
  `repark-connect → repark-common` (normal). Deps: `tokio-postgres` (+`sqlx` only if needed),
  `tiberius`, `reqwest` for Trino's HTTP protocol. No JVM, no ODBC.
- **Reference:** DuckDB's ATTACH model — `LE/duckdb/src/include/duckdb/storage/storage_extension.hpp`
  (`attach_function_t` → a `Catalog`), `…/main/attached_database.hpp`, `…/catalog/catalog.hpp`
  (the shape: one attached catalog per source, transaction manager per source); Sail's catalog
  crates for a DataFusion-side registry shape: `LE/sail/crates/sail-catalog*/`; ConnectorX
  (partitioned reads on a partition column, Arrow destination) and ADBC as the external bars.
- **Steps:**
  1. Crate checklist (§0); `SourceSpec` consumed from CFG-1; CFG-2's refuse-loud stub retired.
  2. `TableProvider::scan` with `supports_filters_pushdown` → `Exact` for the predicate subset
     each backend can take; `TableProvider::insert_into` for writes; `EXPLAIN` prints the remote
     SQL per source (the pushdown boundary).
  3. Postgres read: `COPY (SELECT …) TO STDOUT (FORMAT BINARY)` decode → Arrow; parallel by
     partition column when given. Write: `COPY … FROM STDIN (FORMAT BINARY)` default, row
     `INSERT` fallback per the `bulk | row` flag.
  4. SQL Server: TDS via `tiberius`; read = paged/partitioned SELECT; write = bulk insert
     (`tiberius` bulk API), row fallback.
  5. Trino: HTTP statement protocol; read paged; whole-subtree pushdown (aggregates + joins
     that stay inside one Trino source) via a `SourceSubtreeRewrite` optimizer rule in
     `repark-core/src/pre_execute/`; write = batched `INSERT`.
  6. Benchmark: same query vs ConnectorX and pandas+SQLAlchemy; record the factor.
- **Pins:** per backend: type-map round-trip table; pushdown pin per predicate class; bulk vs
  row parity; the **federated statement** (Iceberg × Postgres × Trino join) with an `EXPLAIN`
  boundary pin; the benchmark within the stated factor.
- **Done when:** the roadmap's v1.6 acceptance line. Live DBs: the Docker Postgres/MSSQL on
  this box; Trino via its docker image.
- **Hand back when:** a type has no Arrow mapping (declare), or pushdown would change
  semantics (collation, NULL ordering) — declare, never approximate.

### Card 1.7 — dbt

- **Home:** sibling repo `dbt-repark`; engine-side only `config.py` (CFG-1 profiles readable as
  dbt targets). No engine card.

### Card 1.8 — Spark Connect server → `repark-server` NEW (+ ADR-0005 discharge)

- **Home:**
  - UPDATE `repark-core/src/session/` — **the ADR-0005 decomposition fires here** (trigger:
    server-protocol needs + cancellation): modules `runtime_factory.rs`, `catalog_manager.rs`,
    `object_store_registry.rs`, `temp_view_manager.rs`, `query_service.rs` (cancel, memory
    budget, admission), `semantic_profile.rs`. Behavior-preserving; public `ReparkSession`
    unchanged; discharge note appended to the ADR.
  - NEW `crates/repark-server` — **tier 4, role `server`** (D-3 as amended: a protocol adapter
    over the doors like `repark-python`, but one its protocol crates and the CLI must depend on,
    so it cannot carry the `bindings` role — that role's rule forbids every inbound edge). Modules:
    `session_manager.rs` (id → session, TTL, eviction), `cancel.rs`, `resource_policy.rs`,
    `protocol.rs` (the trait one protocol crate implements), `serve.rs` (tonic/tokio bootstrap).
  - NEW `crates/repark-server-spark-connect` — tier 4, role `server`: `proto/` (Spark Connect protobuf, vendored
    at a pinned Spark version), `service/{plan_analyzer,plan_executor,config,artifacts}.rs`,
    `plan_to_df.rs` (Connect relation → the Spark door's planner via `repark-spark`).
  - NEW binary `crates/repark-cli` (`repark serve`, `repark sql`): tier 4, role `server`.
- **Edges:** `repark-server → {repark-core}`; `repark-server-spark-connect → {repark-server,
  repark-spark, repark-functions}`; `repark-cli → {repark-server, repark-server-spark-connect}`.
  All normal. The `server` structural rule these edges need: nothing outside tier 4 may depend
  on a `server` crate; a `server` crate may depend on another `server` crate (`repark-cli →
  repark-server`) and on anything at tier ≤ 3; a `server` crate never depends on `repark-python`.
- **Reference:** `LE/sail/crates/sail-spark-connect/src/{server,session_manager,session,
  executor,streaming}.rs` and `service/{plan_analyzer,plan_executor,config_manager,
  artifact_manager}.rs` — the complete shape of a Rust Spark Connect server;
  `LE/sail/crates/sail-session/src/session_manager/` for session lifecycle; `LE/sail/crates/
  sail-execution/` is what we are **not** building (distributed job graph).
- **Steps:**
  1. Decomposition unit first (its own PR, pins = the existing session test suite unchanged).
  2. `repark-server` core with an in-process test protocol (no network) proving session
     lifecycle, cancellation mid-scan, memory budget refusal.
  3. Spark Connect: `AnalyzePlan` + `ExecutePlan` for the relation subset the facade already
     supports; `Config` service backed by the session's `SET` path (`spill.rs::maybe_apply_runtime_set`);
     artifacts refuse-loud (no JVM UDF jars).
  4. Acceptance: unmodified `pyspark` client, `SparkSession.builder.remote("sc://…")`, the
     facade suite's read-only subset passes over the wire.
- **Pins:** proto round-trip; session eviction; cancel; the facade subset over Connect.
- **Done when:** `pyspark` + `spark.remote(...)` with zero import changes.
- **Hand back when:** a Connect relation has no facade equivalent — list it; do not extend the
  facade from the server side.

### Card 1.9 — Multi-writer Iceberg + REST catalog

- **Home:** UPDATE `repark-iceberg/src/write/concurrency.rs` (the OCC retry policy: bounded
  retries, jitter, re-validate against the refreshed snapshot; serializable isolation checks
  per operation) + `write/mod.rs` (every commit path goes through it); UPDATE
  `catalog/builders.rs` (+ `CatalogKind::Rest` from CFG-1) over the fork's REST catalog crate.
- **Reference:** `FORK/crates/iceberg/src/transaction/{commit_status,snapshot}.rs` and the
  fork's catalog-rest crate; Java `BaseTransaction` retry semantics as the oracle
  (`commit.retry.num-retries`, `commit.retry.min-wait-ms`).
- **Pins:** two writers, N commits each, zero lost updates on Glue (live gate), S3 Tables
  (conflict-retry guidance), memory catalog (unit); serializable-isolation refusal pin.
- **Hand back when:** the fork's commit path needs a change — file a fork handoff (F-next).

---

## 4. 2.x

### Card 2.0-A — Arrow Flight SQL → `repark-server-flight-sql` NEW

- **Home:** NEW `crates/repark-server-flight-sql` — tier 4, role `server`: `service.rs` (`FlightSqlService`
  impl from `arrow-flight`), `handles.rs` (prepared statements, tickets), `catalog_meta.rs`
  (`GetTables` / `GetSchemas` over the federated registry), `auth_stub.rs` (accept-all until
  3.0, marked). `repark-cli serve --flight-sql`.
- **Edges:** `repark-server-flight-sql → {repark-server, repark-sql}` (the ANSI door is the
  Flight SQL dialect).
- **Reference:** `LE/sail/crates/sail-flight/src/{service,session,state}.rs`
  (`SailFlightSqlService` over `arrow-flight`); DF's `datafusion-examples` Flight SQL server.
- **Pins:** JDBC driver `SELECT` round trip (the Arrow Flight SQL JDBC jar in a test-only
  container — no JVM in the product); `GetTables` lists the federated namespace.
- **Done when:** a BI tool connects and lists `<source>.<schema>.<table>`.

### Card 2.0-B — Stable API + semver

- **Home:** UPDATE `python/repark/` (public surface frozen; divergence-registry shims retired),
  `docs/release.md` (deprecation policy), `crates/repark-core` (pub items audited: `cargo
  public-api` snapshot committed and gated).
- **Pins:** the public-API snapshot gate red on any unlisted pub change.

### Card 2.1 — Maintenance policy

- **Home:** UPDATE `repark-iceberg`: new `src/maintenance/{policy,plan,run,report}.rs` —
  `MaintenancePolicy` (from CFG-1 `[<profile>.maintenance]`), a planner that turns table state
  into an ordered list of the **existing** procedures (`expire_snapshots`, `rewrite_data_files`,
  `rewrite_position_delete_files`, `rewrite_manifests`, `remove_orphan_files`) with computed
  args, a runner, and a dry-run report. UPDATE `repark-spark/src/call.rs` +
  `repark-sql` for `CALL system.run_maintenance(table, dry_run)`. UPDATE `repark-server`
  `scheduler.rs` (cron-like, per profile).
- **Reference:** Java `RewriteDataFilesSparkAction` defaults + MW-7's measured numbers (the
  thresholds); DuckDB has no equivalent — none needed.
- **Steps:** policy schema → planner (pure function: `TableState → Vec<Action>`) → runner
  (each action = existing procedure; per-action report row) → `CALL` → scheduler.
- **Pins:** planner unit tests over synthetic table states (no I/O); dry-run emits and changes
  nothing (snapshot id unchanged); a CDC-shaped many-small-commits table converges.
- **Hand back when:** a threshold has no measured basis — record "unmeasured", do not guess.

### Card 2.2 — Incremental & change-data reads (RePark-side, fork-independent)

- **Owner ruling (2026-08-29):** the changelog is computed **in RePark**, not by the fork's
  `IncrementalChangelogScan` / `IncrementalAppendScan`. Rationale: a later roadmap item
  migrates the engine off the owned fork, so 2.2 must depend only on primitives that upstream
  iceberg-rust also ships — table metadata, the snapshot log, manifest lists, manifest entries
  and the Parquet reader — never on a fork-only scan type.
- **Home:** UPDATE `repark-iceberg/src/catalog/`: new `changes/` module family —
  `changes/diff.rs` (the snapshot walk: for every snapshot in `(A, B]`, load its manifest list,
  keep manifests whose `added_snapshot_id` is that snapshot, classify entries `ADDED` →
  inserted data file / new delete file, `DELETED` → removed data file), `changes/rows.rs`
  (turn the classification into rows: an added data file scans as `INSERT`; a removed data
  file scans as `DELETE`; a new position-delete or DV file scans its target data file and
  emits `DELETE` for the referenced positions; with v3 row lineage a `DELETE`+`INSERT` sharing
  `_row_id` collapses into `UPDATE_BEFORE`/`UPDATE_AFTER`), `changes/provider.rs` (a
  `TableProvider` for `table$changes` / `CHANGES BETWEEN` with the Spark columns
  `_change_type`, `_change_ordinal`, `_commit_snapshot_id` appended; append-only ranges take
  the cheap path — added data files only, no delete-file replay). UPDATE `repark-sql` +
  `repark-spark` grammar (`FROM t CHANGES BETWEEN snapshot A AND B`, Spark's `table_changes`
  / `$changes` form — the `$changes` suffix hooks the existing
  `repark-spark/src/metadata_tables.rs` resolver). Facade micro-batch subset: NEW
  `python/repark/src/repark/spark/streaming/{reader,writer,trigger}.py` (Iceberg source/sink
  only; `availableNow`, processing-time), state = last consumed snapshot id in a small
  checkpoint file.
- **Edges:** none new. The only `iceberg` items this card may import are ones that exist
  upstream (`TableMetadata`, `Snapshot`, `ManifestList`, `ManifestEntry`, `ManifestStatus`,
  `DataContentType`, `FileScanTask`, `ArrowReaderBuilder`); the manifest walk in
  `repark-spark/src/call.rs` (maintenance procedures) is the in-repo example of that surface.
- **Reference:** Spark's `_change_type`, `_commit_snapshot_id`, `_change_ordinal` output
  columns (oracle) and its `ChangelogIterator` pairing rule for updates; the fork's
  `FORK/crates/iceberg/src/scan/incremental.rs` is **read for the taxonomy only**
  (`ChangelogOperation`, how it orders overwrite snapshots) — nothing from it is called.
- **Pins:** changelog columns match Spark on the shared fixture across append / DV delete /
  overwrite; an append-only range yields no `DELETE` rows and reads no delete files (assert on
  the scanned-file list); `availableNow` drains exactly to the snapshot at start; a second run
  reads nothing; a compile-time pin greps the `changes/` module for the two fork scan type
  names and fails if either appears.
- **Done when:** the docs state "batch over snapshots, not a streaming engine", the oracle rows
  are green, and the module builds against the upstream `iceberg` API surface listed above.
- **Hand back when:** a v2 position-delete range needs equality-delete replay (out of scope —
  report, do not implement); a snapshot in the range was expired (the walk must error with the
  missing snapshot id, not skip it).

### Card 2.3 — CDC ingestion

- **Home:** UPDATE `repark-connect`: new `src/cdc/{postgres_pgoutput,mssql_cdc,decoder,
  sync}.rs` — Postgres logical replication (`tokio-postgres` replication connection, `pgoutput`
  decode → change batches), SQL Server CDC table polling (`cdc.fn_cdc_get_all_changes_*`),
  and `SyncSpec` from CFG-1 `[<profile>.sync.<name>]`. UPDATE `repark-iceberg/src/write/merge/`
  (a `MergeSpec` built from a change batch: upsert + delete by key, DV path on v3). Runner in
  `repark-server` (`sync_runner.rs`) or `repark sync --once` from the CLI.
- **Reference:** Debezium's pgoutput event shapes (oracle for decode); the 2.2 change columns
  for the *output* side.
- **Pins:** decode fixtures (captured WAL bytes → expected rows); end-to-end on the local
  Docker Postgres: insert/update/delete → Iceberg table equals the source after `--once`;
  restart resumes from the persisted LSN; 2.1 policy keeps file count bounded over 1,000
  small syncs.
- **Hand back when:** a source has no primary key — refuse-loud design is the owner's.

### Card 2.4 — Materialized views + result cache

- **Home:** UPDATE `repark-iceberg`: `src/mv/{define,refresh,lineage}.rs` — an MV is an Iceberg
  table plus a stored plan and the per-source snapshot ids; refresh = 2.2 changelog over each
  Iceberg source + full re-read of non-Iceberg sources, merged via `MergeSpec`. UPDATE
  `repark-core/src/catalog_state.rs` (MV entries), both doors (`CREATE MATERIALIZED VIEW … AS`,
  `REFRESH MATERIALIZED VIEW`). Result cache: `repark-core/src/pre_execute/result_cache.rs`
  keyed on `(plan fingerprint, snapshot ids)`.
- **Pins:** incremental refresh equals full recompute on the fixture; cache hit only when every
  snapshot id matches; a federated MV (Postgres × Iceberg) refreshes.
- **Hand back when:** a plan is not incrementally maintainable (non-distributive aggregate) —
  fall back to full refresh and report; do not implement differential rules ad hoc.

### Card 2.5 — Fleet-parallel

- **Home:** NEW `python/repark/src/repark/fleet/` (sweep / backtest API: N processes × one
  session each; the Iceberg commit protocol is the coordinator; results land as partitions of
  one table). Engine work ≈ zero; 1.9's OCC retry is the prerequisite.
- **Pins:** N workers, no lost partitions, deterministic result table.

### Card 2.6 — ML out-of-core off Iceberg

- **Home:** UPDATE `repark-ml` (streaming accumulators already: OLS/IRLS/KMeans — add
  epoch-able trainers with a `partial_fit(batch)` contract); UPDATE `repark-python/src/ml.rs`
  (a scan-driven loop: Iceberg scan → batches → trainer, no materialization); NEW
  `python/repark/src/repark/ml/iceberg.py` (`fit_stream(table, features, target)`).
- **Pins:** parameters equal the in-memory fit within tolerance on the fixture; peak RSS bounded
  by batch size (v0.9 matrix row).

### Card 2.7 — Observability

- **Home:** UPDATE `repark-core` (`tracing` is already wired: `repark-iceberg/src/test_tracing.rs`
  pins it, `repark-python` emits `py_entry` spans) — add `src/telemetry/{spans,metrics,history}.rs` (per-query span with source
  breakdown, spill/memory counters from `spill.rs`, a `query_history` in-memory table exposed
  as `system.query_history`); UPDATE `repark-server` (`otel.rs`: OTLP exporter, opt-in via
  CFG-1 `[<profile>.telemetry]`); UPDATE both doors' `EXPLAIN ANALYZE` to print per-source
  time/bytes from the 1.6 boundary.
- **Reference:** `LE/sail/crates/sail-telemetry/src/{metrics,execution,system_event}/`;
  `LE/polars/crates/polars-observer/` for the hook shape.
- **Pins:** span tree fixture per statement class; exporter off = zero overhead pin (bench).

### Card 2.8 — Cross-engine matrix, DuckDB leg

- **Home:** UPDATE `python/repark-parity/` (a DuckDB oracle beside Spark and Polars; `pip
  install duckdb`); no engine change. The DuckDB clone (`LE/duckdb/`) is for reading semantics
  (e.g. `extension/core_functions/`), not for building.

### Card 2.9 — Substrait ingress + Ibis backend

- **Home:** NEW `crates/repark-server-substrait` — tier 4, role `server`: `datafusion-substrait` consumer →
  DF plan → `QueryService`; protocol #3 on `repark-server`. NEW sibling package `ibis-repark`
  (an Ibis backend emitting Substrait or SQL to the Flight SQL endpoint — the owner picks the
  transport).
- **Pins:** a Substrait plan produced by DF's own producer round-trips; an Ibis expression over
  the federated namespace executes.
- **Hand back when:** an Ibis operation has no Substrait relation — declare, do not add SQL
  text fallbacks silently.

---

## 5. 3.0 — the trust promise

### Card 3.0-A — Authentication

- **Home:** UPDATE `repark-server`: `auth/{mtls,bearer,oidc}.rs` (tonic interceptors),
  `principal.rs` (the identity every session carries); CFG-1 gains `[<profile>.auth]` and the
  `${secret:<provider>/<name>}` interpolation (`interpolate.rs`: providers `aws-sm`, `vault`,
  `env`); the 2.0 `auth_stub.rs` is deleted.
- **Pins:** unauthenticated Flight SQL / Connect refused; principal present on every query span.

### Card 3.0-B — Authorization in the planner

- **Home:** UPDATE `repark-core/src/pre_execute/`: `policy/{grants,row_filter,column_mask}.rs`
  — an analyzer rule that rewrites the plan per principal (predicate injected under every scan of
  a governed table; masked columns projected through a mask function), so it holds for **every
  door and every federated source** (a Postgres scan gets the same injected predicate before
  pushdown). Grants stored in an Iceberg system table (`system.grants`); DDL `GRANT` / `REVOKE`
  in both doors.
- **Pins:** the same statement returns different rows per principal; `EXPLAIN` shows the
  injected predicate; a masked column never reaches the wire (Flight SQL byte-level pin).
- **Hand back when:** a policy needs cross-source joins to evaluate — owner design.

### Card 3.0-C — Isolation & quotas

- **Home:** UPDATE `repark-core/src/session/query_service.rs` (from 1.8): per-session memory /
  spill / time budgets, admission queue, cancellation on breach; `repark-server/resource_policy.rs`
  applies CFG-1 `[<profile>.quotas]`.
- **Pins:** two sessions, one over budget → refused with the budget named, the other completes.

### Card 3.0-D — Audit

- **Home:** UPDATE `repark-core/src/telemetry/history.rs` → durable sink: an Iceberg table
  `system.audit` (statement, principal, sources touched, snapshot ids read/written, outcome)
  via the append path; buffered writer in `repark-server`.
- **Pins:** the 3.0 acceptance statement — two principals, one refused at the column, neither
  sees the other's history, both in `system.audit`.

### Card 3.0-E — Deployment

- **Home:** `docker/` (signed image, `repark serve`), `docs/security.md` (response policy),
  CI release workflow.

---

## 6. Decisions this plan takes (ruled 2026-08-29)

| # | Decision | Alternative not taken |
|---|---|---|
| D-1 | `repark.toml` loader is a **module family in `repark-core`**, not a new crate. | `repark-config` at tier 0 — heavier DAG/manifest churn for no consumer that `repark-core` does not already serve. |
| D-2 | `repark-io` and `repark-connect` are **tier 1, role "table service"** peers of `repark-iceberg`. | A tier-3 capability — impossible, `repark-core` (tier 2) must call them. |
| D-3 | **Amended 2026-08-29:** the server family (`repark-server`, `-spark-connect`, `-flight-sql`, `-substrait`, `repark-cli`) is **tier 4, role `server`** — a new `ROLE_NAMES` entry whose `forbidden_reason` rule is "nothing outside tier 4 may depend on a `server` crate; a `server` crate never depends on `repark-python`". The five crates are reserved as `planned` in `repo-manifest.toml`. | Role `bindings` (the first draft) — checked against `forbidden_reason`, which refuses *every* inbound edge to a `bindings` target, so `repark-server-spark-connect → repark-server` and `repark-cli → repark-server` could never be declared. A tier 5 was not needed: the layering rule already lets same-tier edges through. |
| D-4 | v0.9 does **not** create `repark-exec`; extraction waits for real operator code (ADR-0005 §4). | Creating it for the matrix — forbidden by the "not ahead of its driver" rule. |
| D-5 | **Overruled by the owner (2026-08-29):** 2.2 is a **RePark-side snapshot diff** over upstream-compatible `iceberg` primitives; the fork's `IncrementalChangelogScan` is read for its taxonomy, never called. | Building on the fork's scan — cheaper today, but a later roadmap item migrates off the fork and the changelog must survive that. |
| D-6 | 3.0 authorization is a **planner rule**, not per-door checks. | Door-level checks — would miss the federated sources and the DataFrame door. |
