# Capability roadmap — engine, server, and shared operation

**Set 2026-08-29 · the owner's ruling is the merge of the change that added this file.**
**Class:** campaign. **Planning revision: 2026-09-07.** A capability section moves to
[../mid-term/](../mid-term/map.md) when an intake evaluates it into units. Its campaign closes
when its acceptance evidence is approved and its record is archived under
[docs/history/](../../../docs/history/map.md). Publishing a package version alone does not
close a capability. This file retires when its remaining capabilities are accepted or explicitly
superseded. The format-v3 definition's single home stays
[v1-0-iceberg-v3-northstar.md](v1-0-iceberg-v3-northstar.md).

**The direction.** The three capability groups are the **format foundation**, **server access
and native API maturity**, and **shared operation with explicit trust boundaries**. Frozen API
protection already applies under [the versioning policy](../../../docs/release.md#versioning-policy);
server delivery does not start that protection. The SQL door is the primary interface:
every configured source — Iceberg catalog,
database, remote engine — is one name in one namespace, and `repark.sql()` queries across them
with no per-source function. [PROJECT.md](../../../PROJECT.md) states intent and points here;
[STATUS.md](../../../STATUS.md) stays the home of current state.

Published versions and delivered subsets: [STATUS.md](../../../STATUS.md#release-state).

## Capability identifiers and legacy labels

**Owner-approved planning change, 2026-09-07:** plan by the capability IDs below. Assign package
versions when assembling a concrete release under
[release engineering](../../../docs/release.md#capability-milestones-and-release-assembly).
One capability may span several releases; a release may carry accepted slices from several
capabilities. Each slice still needs its own scope and evidence.

The existing numbered headings, table rows, and cross-references below are retained as
**legacy scope labels**, so existing briefs and design-card links still resolve. Prospective
numbers in those sections no longer reserve tags, imply delivery dates, or certify completion.
This interpretation supersedes the old heading-equals-tag rule and future tag assignments in
the dated Q&A. Actual published tags and their history do not change. The architectural order,
scope exclusions, and explicit dependencies remain; moving a capability is an intake decision.

| Capability ID | Legacy label | Scope home below |
|---|---|---|
| CAP-DML | 0.6 | Iceberg DML remainder |
| CAP-FORMAT-V3 | 1.0 | Format-v3 acceptance definition |
| CAP-EXAMPLES | 1.1 | Executable examples and coverage gate |
| CAP-DATASETS | 1.2 | Adversarial dataset suite |
| CAP-MEMORY | 1.3 | Spill and memory-failure evidence |
| CAP-CONFIG | 1.4 | Configuration and named sources |
| CAP-FUNCTION-TA | 1.5 | Spark-function and technical-analysis fixes |
| CAP-WINDOW-FLATTEN | 1.6 | Window and nested-data performance |
| CAP-IO | 1.7 | Native I/O parity |
| CAP-EXPRESSIONS | 1.8 | Native expressions and Polars oracle |
| CAP-SPARK-PARITY | 1.9 | PySpark function and transformation parity |
| CAP-CONNECTORS | 1.10 | Database access and federated statements |
| CAP-DBT | 1.11 | dbt integration and profile support |
| CAP-SPARK-CONNECT | 1.12 | Spark Connect and shared server core |
| CAP-MULTI-WRITER | 1.13 | Multi-writer Iceberg and REST catalogs |
| CAP-SERVER-ACCESS | Both 2.0 rows | Flight SQL and broader native API stabilization |
| CAP-MAINTENANCE | 2.1 | Maintenance policy |
| CAP-CHANGE-READS | 2.2 | Incremental reads and bounded micro-batch interface |
| CAP-CDC | 2.3 | Native database change ingestion |
| CAP-MATERIALIZED-VIEWS | 2.4 | Incremental materialized views and result cache |
| CAP-FLEET | 2.5 | Fleet-parallel workloads |
| CAP-ML | 2.6 | Out-of-core ML |
| CAP-OBSERVABILITY | 2.7 | Query diagnostics, traces, and metrics |
| CAP-DUCKDB-ORACLE | 2.8 | DuckDB function comparison leg |
| CAP-SUBSTRAIT | 2.9 | Substrait input and Ibis integration |
| CAP-SHARED-TRUST | 3.0 | Shared server authorization, isolation, audit, and deployment |
| CAP-FORMAT-V4 | Conditional 3.0 or later | Spec-dependent candidate; no release commitment |

Use these IDs in new intakes and worker briefs. Link the relevant scope section and current
evidence rather than copying the roadmap into each brief. Historical design-card numbers remain
lookup aids; an old version label is never evidence that work still needs implementation.

## Reconcile before scheduling

As of this planning review on 2026-09-07, the old slots no longer describe release contents.
Resolve each affected capability against its existing evidence home:

| Capability | Required pickup check | Evidence home |
|---|---|---|
| CAP-EXAMPLES | Distinguish the example gate and shipped examples from completion of the full backfill. | [STATUS: example campaign](../../../STATUS.md#active-workstreams) and [example inventory](../../../docs/examples/map.md) |
| CAP-MEMORY | Consume the existing spill matrix; scope residual failures separately from the measurement task. | [Spill baseline](../../../docs/perf/spill-matrix-baseline.md) |
| CAP-DBT | Distinguish the shipped in-repo adapter path from broader profile integration and separately scheduled acceptance. | [STATUS: release state and dbt workstream](../../../STATUS.md) |
| CAP-SERVER-ACCESS | Preserve the existing freeze; enumerate only the additional API and protocol acceptance obligations. | [Versioning policy](../../../docs/release.md#versioning-policy) |

The ordered execution queue remains [next-sequence.md](../../../briefs/next-sequence.md).
Check a queued unit against merged evidence before starting it. This roadmap is not a second
status tracker and does not mark an entire capability accepted because one subset shipped.

The [Rust unification brief](rust-unification-implementation-brief-2026-09-04.md) remains a
proposal for wider CDC and streaming contracts. Its old release references use the same mapping;
this planning change does not approve those implementation decisions or change agent routing.

---

## Pre-1.0 — close the gaps that would otherwise float

Historical baseline, recorded 2026-08-29 and renumbered 2026-09-03. The status words and fork
pins in this section describe that baseline, not current work. Current RePark state is in
[STATUS.md](../../../STATUS.md); fork capability status belongs only to the fork's
`docs/parity/GAP_MATRIX.md` and `docs/ENGINE_CONTRACT.md` at the consumed pin.

**Ruling 2026-09-03:** the owner cut v1.0.0 at the north-star gate ahead of the 0.x ladder.
v0.7 → 1.1, v0.8 → 1.2, v0.9 → 1.3, v0.10 → 1.4; former 1.1–1.9 become 1.5–1.13.
Each moved heading notes `(was v0.N)` or `(was 1.N)`.

### v0.6 — Iceberg DML remainder (Track B)

Everything a "production-grade Iceberg" claim needs that is not format-v3 work:

| Unit | Scope | Status |
|---|---|---|
| DML-A | `MERGE … WHEN NOT MATCHED BY SOURCE` (COW + MOR; reuse cardinality / store-assignment gates) | can start now |
| DML-B | `INSERT OVERWRITE … PARTITION (…)` static + dynamic, `writeTo().overwritePartitions()` | **unblocked — see below** |
| DML-C | `TRUNCATE` as a first-class statement (today: empty-overwrite substitute) | can start now |
| REF | branch / tag operations + write-audit-publish (WAP) | needs fork F-6 (branch commit target) |
| MAINT | `rewrite_data_files` `where` / `sort_order` / strategy | can start now |

**DML-B correction (2026-08-29, verified).** The intake said DML-B was blocked on fork handoff
F-5. That premise is stale: F-5 landed as fork PR #217 (`798a0c8ce`, 2026-08-23) and is an
ancestor of the engine's current pin `ce92a7bf` (`Cargo.toml:145`). The "static path" half of
the ask was void — Java routes static `PARTITION (k=v)` through `OverwriteFiles.overwriteByRowFilter`,
not `ReplacePartitions`. Build recipe is Spark's own:

- static `PARTITION (k=v, …)` → `Transaction::overwrite_files().overwrite_by_row_filter(k=v AND …)`
  plus `validate_added_files_match_overwrite_filter`
- dynamic / `overwritePartitions()` → `Transaction::replace_partitions()` with `add_file`
- engine guards "empty input dynamic overwrite" itself (Spark does this engine-side)
- acceptance pins `empty_insert_overwrite_partition_refuses_full_wipe` and
  `…_nonempty_refuses_whole_table_replace` flip against the current pin; no repin needed

Doc corrections, filed with this roadmap: the DML-B row in
[../mid-term/roadmap-intake-2026-08-23.md](../mid-term/roadmap-intake-2026-08-23.md), and the
F-5 Ask plus the closing ownership list in
[../mid-term/iceberg-rust-handoff-2026-08-23.md](../mid-term/iceberg-rust-handoff-2026-08-23.md)
(F-5 → "answered #217").

Fork housekeeping surfaced by the same report (not a 0.6 gate): fork QC #242 never reached fork
`main` — needs its own PR from `parity/unit3-partition-key-status`; stale remote
`parity/h7-p1-r114-dml-prune` is safe to delete; the R104 multi-spec interop leg is optional.

## v1.0 — Production-grade Iceberg format-v3 (the north star)

Owner ruling 2026-08-23; charter and acceptance gate in
`task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md`. That definition owns the acceptance gate
and declared exclusions. The other engine capabilities may proceed in parallel where their
dependencies permit; their legacy labels do not determine what tags next.

## 1.x — parity, connectors, and the first server

### v1.1 — Full example documentation (was v0.7)

Two deliverables, not one:

1. **Backfill** — an executable worked example for every public name in the accepted example roster:
   every `F.*` function, every DataFrame method, every TA kernel, every reader/writer.
   Ships as repo artifacts and runnable notebooks.
2. **The drift gate** — CI fails when a public name has no executable example.

The gate keeps coverage current as I/O, expression, and Spark-parity work extends the surface.
Each added surface ships its examples as part of its done gate.

**Additions (proposed 2026-08-31; the ruling is this change's merge).**

- **SQL-door examples.** This file names the SQL door the primary interface, yet the backfill
  list above enumerates only the Python surface. Add an example family for the SQL door: one
  runnable example per statement family and per `CALL system.*` procedure, roster hand-curated
  from the parity registry's §2 taxonomy — introspection cannot enumerate SQL statements, so
  the roster is a checked-in list held by the same drift gate.
- **Notebooks are generated, not written.** The notebook deliverable ships as a mechanical
  conversion of the example scripts, with a lockstep check; a notebook that can drift from its
  script would re-create the rot the gate exists to prevent.
- **FNP-15/16 landed 2026-08-30 (#271), inside the v1.1 window as required**: all 62
  declared-absent `F.*` names are exported and refuse loudly with registry §9 reasons
  (archived ledger `2026-08-30-fnp-15-16-ledger.md`), so "every public name" already means
  the intended surface. The remaining facade gap against live PySpark is a different
  cohort (~70 names: json, time/numeric, collate, aes/mask, bitmap aggs, window/stack
  families — FNP-9…14 territory, several deliberately deferred); registering those is
  post-v1.1 campaign work, not this window's.
- **The class surfaces are in** (owner ruling, 2026-08-31). "Every public name" includes
  `Column`, `Window` / `WindowSpec`, `Catalog`, `types`, `Row`, `ml`, and the other class
  surfaces the EX-0 inventory measured but excluded — roughly 120 further names. The
  inventory widens in a follow-up unit after EX-0 merges and before the backfill roster
  freezes, so per-family counts include them from the start.

### v1.2 — Torture-test dataset suite (was v0.8)

Every dataset ≥ 1M rows; generators checked in, data never committed as blobs.

- Nested reading + `dynamicFlatten`: deep struct/list nesting, mixed element types, lists of
  structs, capitalized field names, null-typed lists — the measurement bed for v1.6.
- Schema-inference conflict battery: type shifts mid-file (int32 → int64 at row 500k,
  string-vs-float halves, bool-looking ints, date-looking strings, …).
- Extreme types: high-precision decimals, UUIDs, paragraph-length strings, embedded HTML.
- smartCsv expansion: header normalization, blank cells, currency/decimal widths, bool spellings.
- **Opt-in secrets flagging** — the fixture (credential-shaped column names carrying fake
  plaintext) *and* the mechanism: disabled by default, one bool conf enables read-time
  flagging/refusal. This is about **data columns**; secrets in *configuration* are v1.4.

**Additions (proposed 2026-08-31; the ruling is this change's merge).**

- **Temporal-extremes family.** None of the five existing dataset families carries a temporal
  or interval class. The FNP-7 measured edges become a reusable battery: Duration values at
  the ±i64-microsecond bound (whole-day edge 106751991), MonthDayNano mixed and zero-unit
  shapes (the BL-14 class), `DATE` ± interval promotion boundaries, epoch extremes.
- **Aggregate-overflow decimals.** `decimal(38)` sums and averages that overflow at the
  aggregate boundary (the `try_avg` class beside BL-13) — a different claim from
  extreme-types' storage-side decimals, which all fit their declared width.
- **v3-DV-at-scale family.** Many-file format-v3 tables carrying live Puffin deletion
  vectors — the bed V3-5's true result counts and the v1.0 `10^7 x 50` gate reuse; today's
  DV fixtures are dozens of rows. Constraint stated up front: live DVs are Spark-written, so
  generation needs the local live oracle — CI keeps a small pre-generated fixture and the
  ≥1M tier generates only where Spark exists.

### v1.3 — Never-OOM truth (was v0.9)

The scope is a spill-coverage matrix: which operators spill, which do not, and how each fails
past the pool, including the W-3 window row. Consume the
[existing measurements](../../../docs/perf/spill-matrix-baseline.md) before planning new work.
CAP-WINDOW-FLATTEN uses that evidence. Measurement and pins do not establish a universal
process-memory guarantee; operator fixes require their own scoped units.

### v1.4 — `repark.toml`: one configuration file (was v0.10)

Split out of v0.8 on 2026-08-29 (now v1.2). It is product surface with precedence rules, and a dependency
of v1.10 (database connections) and v1.11 (dbt targets), so the schema wants to be right early.
Modeled loosely on `.pyiceberg.yaml`, but TOML with environment profiles.

**Design decisions (agreed 2026-08-29).**

| Topic | Decision |
|---|---|
| Owner | The loader lives in **Rust** (`serde` + `toml`), per ADR-0004 everything-through-Session, so the v1.12 Spark Connect server and v2.0 Flight SQL get it without Python. **Pydantic v2** models are the Python-facing validation / typed accessor — the same Rust-source / Python-mirror pattern already used for `prop_key_is_secret` (`python/repark/src/repark/spark/_secrets.py`). |
| Profiles | **Every** section is profile-scoped (`[default.…]`, `[prod.…]`), including databases. Load = merge `[default]` with the section named by `REPARK_ENV`. Unscoped sections are a parse error. |
| Catalog keys | Accept the facade's existing Java-class spellings 1:1 (`impl`, `catalog-impl`, … map to `spark.sql.catalog.<name>.<key>`) for migration fidelity, **and** a native short form `type = "glue" \| "s3tables" \| "memory" \| "rest"` (REST matters for v1.13). |
| Secrets | `${ENV_VAR}` interpolation works everywhere. Inline secret values are allowed but redacted in every conf dump through the existing `prop_key_is_secret`. |
| Precedence | explicit builder `.config()` **>** `REPARK_ENV` profile overlay **>** `[default]`. |
| Discovery | `$REPARK_CONFIG` **>** `./repark.toml` **>** `~/.config/repark/repark.toml`. First hit wins; no merging across files. |
| Naming | Trino is a remote source from RePark's point of view: `[<profile>.database.trino.<name>]`, symmetric with postgres / sqlserver. No `[engine.*]` table. |

**Sketch.**

```toml
# repark.toml — merge [default] with the profile named by REPARK_ENV (e.g. "prod")

[default]
"repark.write.max-concurrent-files" = "32"
"datafusion.execution.target_partitions" = "8"
"datafusion.execution.batch_size" = "98304"
"spark.sql.pyspark.inferNestedDictAsStruct.enabled" = "true"

[default.catalog.local]
type = "memory"                                   # native short form …
warehouse = "./warehouse"

[prod.catalog.glue_catalog]
impl = "org.apache.iceberg.spark.SparkCatalog"    # … or the facade's Java-class spellings, 1:1
catalog-impl = "org.apache.iceberg.aws.glue.GlueCatalog"
warehouse = "s3://my-iceberg-warehouse"

[prod.catalog.s3tables]
type = "s3tables"
warehouse = "${TABLE_BUCKET_ARN}"

[prod.database.postgres.company_db]
host = "db.internal"
port = 5432
database = "company"
schema = "public"
username = "${PG_USER}"
password = "${PG_PASSWORD}"                       # redacted in any conf dump
ssl = true
sslrootcert = "/etc/ssl/certs/company-root.pem"

[prod.database.sqlserver.company_db]
server = "sql.internal"
port = 1433
database = "company"
username = "${MSSQL_USER}"
password = "${MSSQL_PASSWORD}"
encrypt = true
trust_server_certificate = true

[prod.database.trino.analytics]
host = "trino.internal"
port = 443
http_scheme = "https"
auth = "basic"
username = "${TRINO_USER}"
password = "${TRINO_PASSWORD}"
```

**Named sources (ruled 2026-08-29, R-4).** Every `[<profile>.database.<kind>.<name>]` entry
auto-registers at session start as a **named connection**; all three doors refer to it by name
so no URI or secret ever appears at a call site.

**Governing rule — one namespace, one `sql()` (owner, 2026-08-29).** The SQL door is the
primary interface. Every configured source — Iceberg catalog *or* database *or* remote engine —
appears as `<source>.<schema>.<table>` in the same namespace, and `repark.sql()` queries and
joins across them in one statement with no per-source function to remember:

```sql
SELECT o.order_id, o.amount, c.segment, t.region_name
FROM   glue_catalog.sales.orders           o      -- Iceberg (Glue)
JOIN   company_db.public.customers         c USING (customer_id)   -- Postgres
JOIN   analytics.warehouse.dim_territory   t USING (territory_id)  -- Trino
WHERE  o.order_date >= DATE '2026-01-01'
```

The engine decides what runs where: projections, filters and limits push to every source;
for Trino (an engine, not a store) whole subtrees — aggregates and joins that are entirely
inside one Trino source — push down as a single remote query. Writes use the same names:
`INSERT INTO company_db.public.orders_stage SELECT … FROM glue_catalog.sales.orders`. The
`read_database` / `write_database` functions remain as the DataFrame-door spelling of the
same registration; they are conveniences, not the contract.

| Door | Shape |
|---|---|
| Native | `repark.read_database("company_db", query=…, partition_on="id", partitions=8)`; `df.write_database("company_db", table=…, mode="append", bulk=True)` |
| SQL (**primary**) | the entry registers as a DataFusion catalog provider in the same namespace as the Iceberg catalogs: `SELECT * FROM company_db.public.orders`, cross-source joins, `INSERT INTO <source>.<schema>.<table> SELECT …`; projection / filter / limit pushdown everywhere, subtree pushdown to Trino (the DuckDB `ATTACH … AS pg` / Trino-catalog model) |
| Spark facade | `spark.read.format("jdbc")` with a real URL stays for parity; `.option("source", "company_db")` is the shortcut |
| dbt (v1.11) | a `profiles.yml` target names the repark profile + source; credentials live in one place |

Semantics to pin in the charter:

- **Lazy** — registering opens nothing; connections open on first use, are pooled per session,
  and close on session stop.
- **Names are unique per profile across kinds** — `company_db` under both `postgres` and
  `sqlserver` in the same profile refuses loud at load (the earlier draft did exactly this).
- **Secrets never cross into Python** — Rust holds them; `repark.sources()` lists name / kind /
  host with values redacted; `repark.source("company_db").ping()` for health.
- `auto_register = false` (per entry or global) keeps an entry as data only.

---

| Legacy slot | Item | Notes and agreed scope |
|---|---|---|
| **1.5** | FNP fixes + TA fixes and optimizations | (was 1.1) Continues the FNP campaign (FNP-15/16 next) and the golden-safe TA perf campaign. |
| **1.6** | Window-function performance + measured `dynamicFlatten` correctness and optimization | (was 1.2) W-0…W-2 from the 2026-08-23 intake, measured against the nested suite and spill matrix. Intake enumerates semantic cases and workload targets; no universal bug-free or optimal-speed claim. |
| **1.7** | One-to-one **I/O parity with Polars** | (was 1.3) Excludes non-native Polars integrations (PyIceberg, Delta via `deltalake`), Hive metastore, Unity, HuggingFace. **Hive-partitioned directory reads are in** (DataFusion provides them). Absorbs two named differentiators — first-class Excel read/write and the smart-CSV / inference readers — name them explicitly in the charter. |
| **1.8** | One-to-one **functions, expressions, transformations parity with Polars** | (was 1.4) With 1.7, this is the **native lazy door maturing** far past today (`scan_*` / `sink_*`, expression API). State that scope in the charter — it is the real size of the item. **The Polars leg of the cross-engine function matrix lands here as the proof** (ruled R-2). |
| **1.9** | One-to-one **functions and transformations parity with PySpark** | (was 1.5) Mostly the FNP campaign continuing over the 2,509-pin cohort; closer than 1.8 and may ride alongside 1.5. |
| **1.10** | **Postgres, SQL Server, Trino** integration — reads *and* writes | (was 1.6) **Acceptance includes a federated statement**: one `repark.sql()` joining an Iceberg table, a Postgres table and a Trino table, with `EXPLAIN` showing the pushdown boundary per source. Pure Rust, no JVM: `tokio-postgres`/`sqlx`, `tiberius` (TDS), Trino HTTP. **Read bar** = ConnectorX's design: partitioned parallel reads on a partition column straight into Arrow; acceptance is a benchmark of the same query vs ConnectorX and pandas+SQLAlchemy, within a stated factor of ConnectorX. **Writes**: bulk path default (`COPY … BINARY`, TDS bulk insert, batched Trino `INSERT`) with a per-call `bulk` / `row` flag; row mode is the fallback for types bulk cannot carry. Reference ConnectorX and ADBC. Connections come from v1.4. |
| **1.11** | Full dbt support | (was 1.7) `dbt-repark` (sibling repo; M0–M2a merged, AWS gates owner-scheduled). Targets can read v1.4 profiles. |
| **1.12** | **Spark Connect server** | (was 1.8) Unmodified `pyspark` clients via `spark.remote(...)` — zero import changes. Built owned, not Sail. This is a gRPC daemon with sessions, cancellation and per-query resource policy, so it **fires the ADR-0005 session-decomposition trigger here, not at 2.0**: build the server core once (session manager, cancellation, resource policy), Spark Connect is protocol #1, Flight SQL at 2.0 is protocol #2 on the same core. PROJECT.md's "no daemon" becomes "no daemon *required*". |
| **1.13** | **Multi-writer Iceberg** + REST catalog first-class | (was 1.9) Lift the single-writer-per-table rule (OCC retry policy, serializable isolation done right); REST alongside Glue / S3 Tables. Review shared-writer and catalog assumptions before CAP-SERVER-ACCESS acceptance; existing frozen APIs remain protected throughout. |

---

## 2.x — the server release and the API promise

This group broadens native API stability and server access, then adds maintenance policy,
change reads, ingestion, and shared-operation prerequisites. Existing frozen APIs are already
protected by the release policy. The legacy labels below describe capability order, not a
requirement to wait for a particular package version.

| Legacy slot | Item | Notes |
|---|---|---|
| **2.0** | **Arrow Flight SQL endpoint** — the headline | Protocol #2 on the 1.12 server core. Unlocks JDBC/ODBC drivers, BI tools, non-Python clients, a standalone `repark` binary/shell. |
| **2.0** | **Broader native API stabilization** | Review remaining unfrozen surfaces and proposed shim retirements alongside Flight SQL. CAP-SERVER-ACCESS keeps these acceptance obligations together. Existing frozen rows and deprecation duties remain governed by `docs/release.md`; any breaking change follows that policy when a release is assembled. |
| **2.1** | **Maintenance policy** — "set it and forget it" | Declarative `[<profile>.maintenance]` in `repark.toml`: compaction targets, snapshot retention, orphan sweeps, DV / position-delete compaction thresholds. Executed by the server on a schedule or by `CALL run_maintenance()`, always with a dry-run report. The procedures already exist (`expire_snapshots`, `rewrite_data_files`, `rewrite_manifests`, `remove_orphan_files`, `rewrite_position_delete_files`); this is the policy layer over them. **Placed first in 2.x** (owner, 2026-08-29): 1.13 multi-writer and 2.3 CDC both generate many small commits, and tables must stay healthy before those arrive. |
| **2.2** | **Incremental & change-data reads** | `SELECT … FROM t CHANGES BETWEEN snapshot A AND B` (or `table$changes`), incremental append scans, and a micro-batch `readStream` / `writeStream` **subset** on the facade — Iceberg source and sink only, triggers `availableNow` and processing-time. v3 **row lineage** (1.0) makes change identification exact rather than diff-by-hash. Pure single-node. The biggest gap vs Spark for pipeline users after DML. **Not a streaming engine** — batch-over-snapshots, stated honestly in the docs. |
| **2.3** | **CDC ingestion from the connectors** | Postgres logical replication (`pgoutput`) and SQL Server CDC tables → Iceberg `MERGE` with deletion vectors, declared as a sync in `repark.toml`. Builds on 1.10 connectors (was 1.6 before the 2026-09-03 renumber) + RP-2/RP-3 DV merge + 2.2 change semantics. Replaces a Debezium + Kafka + Spark stack with one process — squarely the "no JVM, one box" thesis. |
| **2.4** | **Materialized views with incremental refresh** + result cache | Second consumer of row lineage: an MV over Iceberg refreshes from the snapshot delta, not a full recompute. Because of the federated namespace (v1.4), an MV can materialize a Postgres × Iceberg join into Iceberg. Result cache keyed on `(plan, snapshot ids)`. |
| **2.5** | Fleet-parallel as a product surface | Sweep / backtest API where the catalog commit protocol is the coordinator; near-zero engine work, high value for TA / futures workloads. |
| **2.6** | ML out-of-core off Iceberg | The `repark-ml` differentiator beyond the Arrow→DMatrix handoff: training loops streaming Iceberg scans, no extraction step. |
| **2.7** | **Observability** | OpenTelemetry traces + metrics from server and engine, a query-history table, `EXPLAIN ANALYZE` with per-source timings and bytes (extends the 1.10 pushdown-boundary `EXPLAIN`), spill / memory telemetry from v1.3. Prerequisite for 3.0 quotas and audit — you cannot govern what you cannot measure. |
| **2.8** | Cross-engine function matrix — DuckDB leg | PySpark + Polars + DuckDB oracles over one function matrix. The Polars leg ships inside v1.8 (ruled R-2, was 1.4); the DuckDB leg lives here. |
| **2.9** | **Substrait ingress + an Ibis backend** | Third protocol on the server core (after Spark Connect and Flight SQL): accept a Substrait plan so Ibis, DuckDB, Polars and other producers execute against RePark without a SQL round-trip. The Ibis backend makes RePark reachable from the largest Python dataframe-frontend ecosystem for near-zero engine work. The last protocol added before 3.0 freezes the protocol set. |

Explicitly **not** planned: distributed single-query execution (only if a query outgrows one
box — nothing says it has), any second table format, and a notebook / UI product (a BI tool over
Flight SQL *is* the UI).

---

## 3.0 — the trust promise

The target is a server safe to share between people and systems with different permissions.
Its acceptance covers authorization, resource isolation, and an audit trail. Earlier server
intakes must define their own deployment boundary and protection requirements; this later
capability is not permission to expose an unprotected endpoint. The proposed baseline security
and observability placement is discussed in the
[Rust unification brief](rust-unification-implementation-brief-2026-09-04.md#4-relationship-to-the-existing-roadmap).
Single node and single format remain the architectural scope.

| Area | What ships |
|---|---|
| **Authentication** | mTLS and bearer / OIDC on Flight SQL and Spark Connect. `repark.toml` secrets from a provider (`${secret:aws-sm/…}`, Vault) so nothing is inline even in prod files. |
| **Authorization** | Catalog / schema / table grants; row-filter and column-mask policies applied **in the planner**, so they hold across every door and every source in the federated namespace. |
| **Isolation & quotas** | Per-session memory, spill and time budgets with cancellation — the ADR-0005 decomposition *finished*, not just triggered. |
| **Audit** | Every statement: principal, sources touched, snapshot ids read and written — into an Iceberg table queryable with `repark.sql()`. |
| **Deployment** | Signed container image, `repark serve` as the supported daemon, a published security-response policy alongside the deprecation policy. |

**Acceptance, in one sentence:** two principals on one server each run a federated statement;
one is refused at the column; neither can see the other's query history; the audit table shows
both.

**Iceberg format-v4** remains a spec-dependent candidate. Intake must establish the applicable
specification and acceptance scope before any release allocation; no version is reserved.

---

## Decisions recorded (Q&A log)

These are dated decisions. The 2026-09-07 planning revision above supersedes prospective
package-number assignments; the remaining scope and architectural decisions still apply.

| Date | Question | Ruling |
|---|---|---|
| 2026-09-03 | Owner cut v1.0.0 at the north-star gate ahead of the 0.x ladder | **Renumber:** v0.7 → 1.1, v0.8 → 1.2, v0.9 → 1.3, v0.10 → 1.4; former 1.1–1.9 → 1.5–1.13. v0.6 stays the shipped DML remainder. |
| 2026-08-29 | Hive-partitioned directory reads in 1.3? | Keep whichever is easiest → **in** (DataFusion). Hive metastore, Unity, HuggingFace, Delta → out. |
| 2026-08-29 | 1.3 + 1.4 imply maturing the native lazy door | Understood; stated as explicit scope. |
| 2026-08-29 | 1.6 bulk vs row writes | Feature flag, bulk vs row. The "similar to ConnectorX" bar means read + memory performance vs the SQLAlchemy path, with integrations. |
| 2026-08-29 | 1.8 fires ADR-0005 at 1.8; "no daemon required" wording | Understood, acceptable. |
| 2026-08-29 | ML out-of-core has no home | Given v2.2. PROJECT.md's differentiator line gets a "2.x" pointer. |
| 2026-08-29 | DML-B blocked on fork F-5? | Premise stale — F-5 landed (#217), engine pin already carries it. DML-B stays in 0.6. |
| 2026-08-29 | Config file: own slot, Rust-owned loader, profile scoping, key forms, secrets, precedence, discovery, `[database.trino]` naming | Agreed as written under v0.10. |
| 2026-08-29 | R-1 — API freeze placement | **Share the 2.0.0 tag** with Flight SQL. |
| 2026-08-29 | R-2 — cross-engine matrix | **Split**: Polars leg inside v1.4, DuckDB leg at v2.3. |
| 2026-08-29 | R-3 — config-file numbering | **v0.10.** Semver / PEP 440 compare numerically (`0.10.0 > 0.9.0` for pip and Cargo); two-digit minors are routine (Polars 0.10–0.20, DataFusion, tokio 0.x). Folding into 0.9 mixes themes; renumbering churns agreed tags. |
| 2026-08-29 | R-4 — named sources | **Yes**, expounded under v0.10 "Named sources". |
| 2026-08-29 | Federated SQL | **The SQL door is primary.** One namespace `<source>.<schema>.<table>` across Iceberg catalogs, databases and remote engines; `repark.sql()` queries and joins across them with no per-source function. Functions are conveniences. Recorded as the governing rule under v0.10; v1.6 acceptance carries a federated join. |
| 2026-08-29 | 2.x continuation and 3.0 | Agreed: 2.2 incremental / change-data reads and 2.3 CDC ingestion are the priority of the 2.x line; **maintenance policy moves up to 2.1** ahead of them (multi-writer and CDC generate many small commits). Fleet-parallel, ML out-of-core and the DuckDB leg shift to 2.5 / 2.6 / 2.8. MVs at 2.4, observability at 2.7, Substrait + Ibis at 2.9. **3.0 = the trust promise** (auth, policies, quotas, audit); Iceberg v4 is spec-timed, not committed. |

## Open rulings

The original R-1..R-4 decisions are recorded above. Future release numbers, dates, capability
acceptance partitions, and worker assignments are resolved in their own intake or release
assembly. This roadmap does not authorize implementation merely by naming a capability.
