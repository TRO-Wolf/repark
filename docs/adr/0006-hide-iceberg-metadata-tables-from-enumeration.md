# ADR 0006 — Hide Iceberg `$`-metadata tables from enumeration, at the catalog layer

- **Status:** Accepted (2026-08-10)
- **Deciders:** project owner + Claude
- **Related:** [0001-own-iceberg-fork.md](0001-own-iceberg-fork.md) (the fork whose schema provider
  synthesizes the names), [0002-two-sql-doors.md](0002-two-sql-doors.md) (why this cannot be a door
  parser's decision), [../../STATUS.md](../../STATUS.md) "Known correctness issues" (the entry this
  ADR closes), [../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md) §2.1 (the metadata-
  table rows this is *not*), [../../task/h1c-ledger.md](../../task/h1c-ledger.md) (the unit that
  gathered the evidence), [../design/v2-engine-hardening.md](../design/v2-engine-hardening.md)
  decision D2 (the ruling that this be decided in the unit).

## Context

**The fact.** The fork's `IcebergSchemaProvider::table_names` (iceberg-datafusion `schema.rs`) does
not report what the catalog holds. For **every** base table it lists, it also *synthesizes* one name
per `MetadataTableType` — `snapshots`, `manifests`, `files`, `data_files`, `delete_files`, `entries`,
`all_files`, `all_data_files`, `all_delete_files`, `all_entries`, `history`, `refs`,
`metadata_log_entries`, `partitions`, `all_manifests`: fifteen today. A namespace of N tables
therefore enumerated as 16 N names in `SHOW TABLES` and every `information_schema` view.

Resolution is a **separate** path in the same provider: `table()` and `table_exist()` split a name on
`$` and build the metadata table on demand, entirely independently of what `table_names()` returned.
Nothing about addressability depends on the listing.

**What the engines we point at do.** Neither reference engine enumerates these names. The basis is
**documented** in both cases (the honesty note below says why it cannot be more than that here), so
the documents are named — a reader checks them instead of trusting this paragraph.

- *Apache Spark + Iceberg* — the docs present metadata tables purely as a naming convention over an
  existing table: "Metadata tables are identified by adding the metadata table name after the
  original table name. For example, history for `db.table` is read using `db.table.history`"
  ([Iceberg — Spark Queries, "Inspecting
  tables"](https://iceberg.apache.org/docs/latest/spark-queries/#inspecting-tables)). They are never
  presented as catalog entries, and the implementation agrees: `SparkCatalog.listTables` delegates
  straight to `icebergCatalog.listTables(namespace)` and maps the identifiers the catalog persists —
  it synthesizes nothing ([apache/iceberg
  `spark/v4.0/…/SparkCatalog.java`](https://github.com/apache/iceberg/blob/main/spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/SparkCatalog.java)),
  while the metadata-table suffix is recognized on the *load* path. Queryable, unlisted.
- *Trino* — "You can query each metadata table by appending the metadata table name to the table
  name: `SELECT * FROM "test_table$properties"`", and the same page's account of listings is about
  the metastore: "The `SHOW TABLES` statement, `information_schema.tables`, and `jdbc.tables` will
  all return all tables that exist in the underlying metastore" ([Trino — Iceberg connector,
  "Metadata tables"](https://trino.io/docs/current/connector/iceberg.html#metadata-tables)).
  `t$snapshots` is not a metastore table, so it is addressable and unlisted. The honest shape of
  this citation: the docs state the addressing rule and the listing rule and the conclusion follows
  from the two — there is no sentence in them reading "metadata tables are hidden from
  `SHOW TABLES`".

**Oracle basis: documented, and honestly so.** This repository's live oracle tier is plain PySpark
4.1.2 (`make parity-live` / `parity-live.yml`) with **no Iceberg runtime jar and no Iceberg catalog**
configured — it cannot create an Iceberg table, so it cannot observe Iceberg's `SHOW TABLES` at all.
The Spark half of the comparison above is documented behavior, not something re-derived here, and no
row in the divergence registry claims otherwise. That limitation is itself part of the decision (see
"Alternative considered").

**Cost, not only aesthetics.** DataFusion's `information_schema` builders call `table_type()` (which
defaults to `table()`) or `table()` **per enumerated name**, and resolving one synthesized name
costs the fork *two* `load_table` calls — `IcebergTableProvider::try_new` loads the base table to
capture its schema (`crates/integrations/datafusion/src/table/mod.rs`, `try_new`) and
`metadata_table()` then loads it again ("Load fresh table metadata for metadata table access", same
file). So on the old behavior every `SHOW TABLES` / `information_schema.tables` / `.columns` query
paid **thirty** extra `load_table` round-trips per base table. That number is measured, not read
off the code: a counting catalog wrapped under a one-base-table namespace recorded 31 `load_table`
calls for `SELECT count(*) FROM information_schema.tables` before the filter and 1 after. Against
Glue and S3 that is real latency and real request volume, and a single unloadable metadata table
would abort the whole introspection query.

**Where the decision belongs.** `SHOW TABLES` is rewritten by DataFusion into
`SELECT * FROM information_schema.tables`, and every `information_schema` view reads
`SchemaProvider::table_names`. One filter at that method therefore covers the ANSI door, the Spark
door, the PySpark facade and the bare `ReparkSession` at once. A door parser could only ever cover
one of the four, and would put a catalog fact in a grammar — which ADR-0002's no-blended-parser
posture rejects.

## Decision

1. **Metadata tables do not enumerate.** `MetadataProjectionSchemaProvider::table_names`
   (`crates/repark-iceberg/src/catalog/metadata_projection.rs`) filters out exactly the synthesized
   names whose base table contains no `$`. That decorator already wraps **every** schema provider
   the engine registers — full snapshot, single-namespace refresh, and `register_schema` — so there
   is no unwrapped path. (A base table with a `$` in its own name — `a$b` — still enumerates its
   fifteen synthesized names, because the predicate splits on the first `$` exactly as the fork's
   own resolution does; all sixteen of those names are unresolvable through the fork either way.
   Fork limitation, recorded under "Consequences" and pinned in the decorator's unit tests.)
2. **Hidden, never removed.** `table()` and `table_exist()` are unchanged. `t$snapshots` stays
   addressable by name through both doors and the facade, and the Spark door's `t.snapshots`
   spelling — which rewrites onto exactly that name — keeps working. This is the Trino shape.
3. **The filter is narrow, and derives its vocabulary from the fork.** A name is dropped only when
   it splits on `$` into a suffix that parses as a fork `MetadataTableType` **and** a base the
   wrapped provider actually knows. Both halves matter: reading the fork's own enum means a fork rev
   that adds a metadata table is covered without editing a list here, and requiring the base to
   exist means a real table named `q1$fy26` is never silently hidden. The set dropped is exactly the
   set the fork's own `table()` would resolve to a metadata table — so **no ordinary table is
   hidden**, and a name that is *not* addressable is left visible rather than quietly disappeared.
   ("Nothing is hidden" is never the claim: decision 2's whole point is that `t$snapshots` is both
   hidden from the listing and addressable. The claim is that nothing stops being addressable, and
   that nothing the wrapped provider would resolve as an ordinary table stops being listed.)
4. **This is a convergence, not a divergence — so it gets no registry row.** Both engines hide these
   names; repark now does too. Per
   [../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md) §6, a fixed issue is not a
   divergence: [../../STATUS.md](../../STATUS.md)'s entry is retired by this change and its
   semantics do not move to the registry. The registry's §2.1 note that pointed at the open question
   now points here.
5. **Both pre-existing pins were flipped in the same diff as the behavior, and neither was deleted.**
   `crates/repark-sql/tests/introspection.rs` (ANSI door) and
   `crates/repark-core/src/session/tests.rs` (bare core session) asserted the old behavior on
   purpose so this decision could not be made silently. Each now asserts the new behavior, states in
   its doc comment that it was flipped and why, and names its former test name so the diff reads as
   intent rather than as a regression.

## Alternative considered — keep the behavior and declare it (rejected)

The other admissible outcome was to leave enumeration as-is and record it as a declared divergence:
a row in [../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md) plus a live-tier
disclosure. Rejected on four grounds:

- **There is no engine this matches.** A declared divergence is a difference we *choose*, usually
  because matching would cost more than it is worth. Here both reference engines agree with each
  other, and repark agreed with neither; the behavior is an artifact of a fork convenience, not a
  design position anyone took.
- **The fix is smaller than the declaration.** It is a filter in a decorator that already exists and
  already wraps every registration path — no new layer, no new indirection, and no cost to
  resolvability. Declaring would have written more prose than the fix wrote code.
- **The declaration could not have been drift-detected.** The registry's live mirror is what makes a
  declared row honest: it re-asserts from a running Spark that the two engines *still* differ, so a
  silent convergence reds. The live tier has no Iceberg support, so this row's mirror could not
  exist — it would have been a documented-basis row with no detector, the weakest shape §1 of the
  registry admits, and admissible only where no fix is available. One was.
- **Keeping it has an ongoing cost.** Thirty extra `load_table` calls per base table on every
  introspection query (measured, above) is a bill paid forever by every user of a Glue catalog, in
  exchange for listing names Spark users have never seen listed.

## Consequences

- **Positive:** `SHOW TABLES` and `information_schema` show the catalog's tables; introspection stops
  paying a per-metadata-table metadata read; the behavior matches both engines RePark's two doors
  point at; the decision lives at one layer and reaches all four entry points from there.
- **Residue (fork-level, documented not engineered around):** a base table whose *own* name contains
  `$` — `a$b` — still enumerates its fifteen synthesized names. The predicate splits on the first
  `$`, so `a$b$snapshots` reads as base `a` / suffix `b$snapshots`, which is not a
  `MetadataTableType`. Splitting from the right would not help: the fork's own `table_exist("a$b")`
  splits on the first `$` too and answers false, so the base-existence guard can never confirm such
  a base — and `a$b` itself is unreachable through the fork today (its `table()` fails with
  `invalid metadata table type: b`). All sixteen names are therefore unresolvable either way, and
  the choice is between listing broken names and hiding them silently; this ADR lists them. Fixing
  it belongs in the fork's schema provider, not in this decorator; the decorator's unit test
  `the_filter_keeps_names_the_fork_did_not_synthesize` pins the 16-name listing so the residue
  cannot drift unnoticed, and `task/h1c-ledger.md` F-2 carries the fork-side follow-up.
- **Cost:** `information_schema.columns` no longer describes metadata tables, because it enumerates
  through the same method. That is the Trino/Spark shape and is what removes the round-trips; the
  columns are still reachable through `DESCRIBE ns."t$snapshots"`, which resolves by name.
- **Fork-repin duty.** The filter is coupled to a fork behavior and is re-verified at every repin,
  alongside the projection shim it shares a module with — the standing list lives in
  [../../crates/repark-iceberg/map.md](../../crates/repark-iceberg/map.md) "Known limitations". Its
  **removal criterion**: the filter goes when a fork rev stops synthesizing metadata names in
  `table_names` (at which point it becomes a no-op and the pins would still hold). Its **breakage
  criterion**: a fork rev that changes the synthesized *spelling* — anything other than
  `<base>$<MetadataTableType::as_str()>` — silently re-exposes the names, which is why the pins
  assert the emptiness of the listing and not merely the presence of the base table.
- **Guard:** a change that makes metadata tables enumerate again contradicts this ADR — write a
  superseding ADR. A change that filters them in a door parser, or that hides them by making
  `table()` / `table_exist()` refuse the `$` form, contradicts decisions 1–2 and breaks the pins in
  `crates/repark-spark/src/tests.rs` and `python/repark/tests/test_metadata_tables.py` that exist to
  catch exactly that.
