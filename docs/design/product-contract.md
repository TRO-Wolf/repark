# Product contract — introspection, statement boundaries, catalog visibility

**Settled 2026-08-11** · product-honesty statements drawn from the G3 engine-intake rows
**G3-E3**, **G3-E4**, and **G3-E7** · pure documentation of behavior already pinned in-tree ·
does **not** change engine code, the divergence registry
([../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md)), or AGENTS.md.

This document is the single home for three consumer-facing contracts a caller (dbt, a facade
job, a long-lived session) may rely on. Every claim below names a **real test or a real pinned
refusal** by path/`::` name. A sentence without a cite is not a product guarantee.

Related homes that this document does **not** restate:

| Concern | Home |
|---|---|
| How repark differs from Spark (row-level semantics) | [../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md) |
| Two SQL doors / dialect shape | [sql-doors.md](sql-doors.md), [../adr/0002-two-sql-doors.md](../adr/0002-two-sql-doors.md) |
| Session / server-prep (no global state) | [session-api.md](session-api.md), [../adr/0004-server-prep-disciplines.md](../adr/0004-server-prep-disciplines.md) |
| Metadata-table enumeration (ADR-0006) | [../adr/0006-hide-iceberg-metadata-tables-from-enumeration.md](../adr/0006-hide-iceberg-metadata-tables-from-enumeration.md) |

---

## 1. G3-E3 — Catalog-API-only introspection

### Contract

**The supported table-introspection path is the Catalog API**
(`spark.catalog.listTables` / `list_tables`, plus `tableExists` / `listDatabases` /
`listCatalogs`). Callers that need "what tables exist in namespace N" must use that path.

SQL `SHOW TABLES IN …` is **not** a supported substitute. Bare `SHOW TABLES` (no `IN` clause)
is a **conf-gated** DataFusion stock path, not the product listing surface.

### What is supported (Catalog API)

| Claim | Pin |
|---|---|
| `listTables(db)` returns MANAGED Iceberg tables + TEMPORARY views with the live PySpark field shape | `python/repark/tests/test_catalog_surface.py::test_list_tables_field_shape_and_temps` |
| `listTables` pattern filter (`*`, `\|`) matches Spark filterPattern | `python/repark/tests/test_catalog_surface.py::test_list_tables_filter_pattern` |
| Missing schema raises `SCHEMA_NOT_FOUND` | `python/repark/tests/test_catalog_surface.py::test_list_tables_missing_schema_raises` |
| No-arg `listTables()` uses `currentDatabase` | `python/repark/tests/test_catalog_surface.py::test_list_tables_no_arg_uses_current_database` |
| `listTables` is **list-on-access** (live Iceberg `Catalog::list_tables`, not the DF provider snapshot) | `python/repark/tests/test_catalog_staleness.py::test_list_tables_sees_oob_create`; engine half `crates/repark-iceberg/src/catalog/tests.rs::live_list_sees_oob_create_and_drop_while_provider_snapshot_stale` |
| `SHOW NAMESPACES IN <catalog>` is the implemented **SQL** sibling of `listDatabases` | `python/repark/tests/test_catalog_surface.py::test_show_namespaces_lists_registered_namespaces` |

Facade implementation note (not a second contract): `Catalog.list_tables` documents the
list-on-access path and the ST-1 refusal of `SHOW TABLES IN` at
`python/repark/src/repark/catalog.py` (module docstring + `list_tables`).

### What is refused or gated

#### `SHOW TABLES IN …` — pinned unimplemented (ST-1)

| Claim | Pin |
|---|---|
| `SHOW TABLES IN <catalog>.…` raises `UnsupportedOperationException` naming `SHOW TABLES` | `python/repark/tests/test_catalog_surface.py::test_show_tables_in_not_implemented_divergence` |
| Semantics live only in the divergence registry | [../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md) §2.4 **ST-1** |

Rationale (registry ST-1, not restated as a new decision): a partial SQL implementation that
listed the wrong set would be worse than a loud refusal. The Catalog facade is the supported
listing surface.

#### Bare `SHOW TABLES` — conf-gated (off by default)

Bare `SHOW TABLES` and `information_schema.*` are **stock DataFusion** surfaces. They require
the builder conf `datafusion.catalog.information_schema = true`. Without it they refuse, naming
`information_schema`. They are **not** the product listing contract for consumers; they exist
for Q8-style delegation and operator introspection once the conf is set.

| Claim | Pin |
|---|---|
| WITH conf: bare `SHOW TABLES` plans/executes and lists a door-created table (ANSI door) | `crates/repark-sql/tests/introspection.rs::show_tables_and_describe_delegate_through_the_ansi_door` |
| WITH conf: `information_schema.tables` enumerates a door-created Iceberg table (ANSI door) | `crates/repark-sql/tests/introspection.rs::information_schema_enumerates_an_iceberg_catalog_through_the_ansi_door` |
| WITH conf: same enumeration on the bare core session (no door) | `crates/repark-core/src/session/tests.rs::information_schema_enumerates_a_registered_iceberg_catalog_through_the_session` |
| WITHOUT conf: bare `SHOW TABLES` refuses naming `information_schema` (core) | `crates/repark-core/src/session/tests.rs::show_tables_still_refuses_without_the_information_schema_conf` |
| WITHOUT conf: same refusal through the ANSI door | `crates/repark-sql/tests/introspection.rs::introspection_still_refuses_without_the_information_schema_conf` |

Operator debug pointer: `crates/repark-core/src/map.md` ("SHOW TABLES / DESCRIBE refuses unless
information_schema is enabled") — set the conf on the builder; nothing else enables it.

### Consumer guidance (G3-E3)

- **Do:** `spark.catalog.listTables(namespace)` / `list_tables` for "what tables exist".
- **Do:** `spark.catalog.tableExists(…)` for existence probes (pins in
  `test_catalog_surface.py::test_table_exists_*`).
- **Do not** depend on `SHOW TABLES IN …` — it is a declared refusal (ST-1).
- **Do not** treat bare `SHOW TABLES` as always-on; enable
  `datafusion.catalog.information_schema` only if you intentionally want the DF stock path, and
  understand its snapshot residual (§3).

---

## 2. G3-E4 — Each `sql()` call is one eager commit boundary

### Contract

1. **One statement per `sql()` call.** Multi-statement scripts are refused loud
   (`PARSE_SYNTAX_ERROR` / "multiple SQL statements"). A trailing `;`, whitespace, or comment
   after a **single** statement is allowed.
2. **DML and DDL that the door routes as commands apply eagerly at `sql()` time** — even when
   the returned `DataFrame` is never collected (the F-BR-2 trap). A later `collect()` on that
   handle does **not** re-apply the write (exactly-once).
3. **No multi-statement atomicity.** There is no product transaction API that groups several
   `sql()` calls (or several statements inside one call) into one commit/rollback unit. A
   transaction API is **possible future work, not promised**.

### Multi-statement refuse (both doors)

| Claim | Pin |
|---|---|
| Spark door: genuine multi-statement refuses as parse class (`PARSE_SYNTAX_ERROR` / "multiple SQL statements") | `crates/repark-spark/src/tests/router.rs::bug010_multi_statement_refuses_parse_class` |
| Spark door: trailing `;` / whitespace / comments after one statement allowed | `crates/repark-spark/src/tests/router.rs::bug010_trailing_semicolon_whitespace_comments_allowed` |
| ANSI door: two statements refuse with `[PARSE_SYNTAX_ERROR]` and "multiple SQL statements" | `crates/repark-sql/src/guards/tests.rs::two_statements_refuse_with_parse_syntax_error_class` |
| ANSI door: trailing noise after one statement allowed | `crates/repark-sql/src/guards/tests.rs::single_statement_with_trailing_noise_is_allowed` |
| ANSI door: `;` inside literals/comments is not a separator | `crates/repark-sql/src/guards/tests.rs::semicolon_inside_literal_or_comment_is_not_multi_statement` |
| ANSI door: unparsable second statement still refuses (fail-closed) | `crates/repark-sql/src/guards/tests.rs::unparsable_second_statement_still_refuses` |
| ANSI door: multi-statement guard runs first (ordering) | `crates/repark-sql/src/guards/tests.rs::multi_statement_refuses_first_and_quote_aware` |

Implementation homes (for navigators, not additional guarantees): Spark
`crates/repark-spark/src/normalize.rs::refuse_multi_statement_sql`; ANSI
`crates/repark-sql/src/guards.rs::refuse_multi_statement`.

### Eager apply at `sql()` (no collect required)

| Claim | Pin |
|---|---|
| Spark door (Rust session): bare `INSERT` applies when the returned frame is dropped uncollected; follow-up SELECT sees the row | `crates/repark-spark/tests/dml_sessions.rs::session_sql_bare_dml_applies_eagerly` |
| Facade: bare `INSERT` applies without collect (value + Arrow type on `to_arrow`) | `python/repark/tests/test_sql_dml_eager.py::test_bare_sql_insert_applies_without_collect` |
| Facade: bare `DELETE` / `UPDATE` same | `test_bare_sql_delete_applies_without_collect`, `test_bare_sql_update_applies_without_collect` |
| Facade: collect of the returned frame does not double-apply | `python/repark/tests/test_sql_dml_eager.py::test_bare_sql_insert_applies_exactly_once_when_collected` |
| Facade: a failing DML raises at `sql()` time and commits nothing | `python/repark/tests/test_sql_dml_eager.py::test_bare_sql_failing_dml_raises_base_pyspark_exception_at_sql_time` |

"Commit boundary" here means: **one successful `sql()` call that routes a write has already
committed that write before the call returns** (Iceberg snapshot commit on the write path). It
does **not** mean multi-statement transactional isolation across calls.

### Explicitly not promised

- No `BEGIN` / `COMMIT` / `ROLLBACK` product API.
- No atomic multi-statement script execution (refused, see pins above).
- No cross-`sql()` rollback if call N+1 fails after call N committed.
- A future transaction surface, if ever designed, is a **new** design pass — not an implicit
  obligation of this contract.

### Consumer guidance (G3-E4)

- Issue **one statement per `sql()` call**; chain independent calls for multi-step jobs.
- Do not wrap several DML statements in one string hoping for atomicity — that path is refused.
- After a successful write `sql()`, assume the change is durable for subsequent calls on the
  same session (subject to §3 visibility rules for free-SQL residual).

---

## 3. G3-E7 — Catalog visibility after DDL

### Contract (what IS guaranteed today)

A consumer such as dbt that runs model N then model N+1 on the **same session** can rely on the
following **when both models use product surfaces** (Catalog API listing and/or product SQL DDL
through a door):

#### A. Catalog API listing is live (list-on-access)

| Claim | Pin |
|---|---|
| After an out-of-band create (no DF reregister), `listTables` includes the new name | `python/repark/tests/test_catalog_staleness.py::test_list_tables_sees_oob_create` |
| After an OOB drop, `listTables` omits the name (no phantom) | `python/repark/tests/test_catalog_staleness.py::test_list_tables_drop_oob_absent_not_phantom` |
| OOB drop of a **DF-known** name: `listTables` succeeds, omits victim, keeps siblings (no information_schema hard-fail) | `python/repark/tests/test_catalog_staleness.py::test_list_tables_oob_drop_df_known_absent_not_crash` |
| Temp views still list alongside live Iceberg names | `python/repark/tests/test_catalog_staleness.py::test_list_tables_temps_still_appended_with_live_iceberg` |
| Engine: live `list_table_names` sees OOB create/drop while the DF provider snapshot stays stale | `crates/repark-iceberg/src/catalog/tests.rs::live_list_sees_oob_create_and_drop_while_provider_snapshot_stale` |

#### B. Product SQL DDL + Catalog existence on the same session

| Claim | Pin |
|---|---|
| After product CTAS, `tableExists` is true; subsequent MERGE / SELECT see the table | `python/repark/tests/test_catalog_flow.py::test_silver_publish_flow` |
| Product CREATE TABLE via `sql()` is visible to a later `listTables` / field-shape listing (fixture creates then lists) | `python/repark/tests/test_catalog_surface.py` fixture + `test_list_tables_field_shape_and_temps` |
| WITH information_schema conf: a door-created table enumerates in `information_schema` / bare `SHOW TABLES` on the **same** session (product DDL path invalidates the DF name directory) | `crates/repark-sql/tests/introspection.rs::information_schema_enumerates_an_iceberg_catalog_through_the_ansi_door`, `show_tables_and_describe_delegate_through_the_ansi_door` |

Product DDL (CREATE/DROP TABLE, CTAS, schema DDL, …) invalidates the **touched namespace** on
the DataFusion provider (`invalidate_catalog_namespaces` / Spark
`catalog_ops::reregister`) so free SQL and conf-gated `SHOW TABLES` / `information_schema` see
product mutations without a manual full rebuild. Pins for the invalidate primitive:

| Claim | Pin |
|---|---|
| Invalidate adds a newly created namespace to the DF provider | `crates/repark-iceberg/src/catalog/tests.rs::invalidate_adds_new_namespace_to_df_provider` |
| Invalidate after live table drop removes the DF name | `crates/repark-iceberg/src/catalog/tests.rs::invalidate_after_live_table_drop_removes_df_name` |

#### C. End-to-end "model N then model N+1" shape (product path)

The silver publish flow is the standing acceptance kernel for "create if missing → write →
read back on the same session":

- Pin: `python/repark/tests/test_catalog_flow.py::test_silver_publish_flow`
  (`tableExists` false → CTAS → `tableExists` true → MERGE → SELECT).

That is the guarantee a sequential dbt-style consumer needs when it uses Catalog probes +
product SQL — **not** a claim about concurrent sessions or external writers.

### What is NOT guaranteed (honest residual)

| Residual | What is true | Pin / home |
|---|---|---|
| **Out-of-band (non-product) DDL vs free SQL** | Direct Catalog-API create/drop **without** product SQL does **not** refresh the DF provider snapshot. Free SQL / `information_schema` / bare `SHOW TABLES` can miss creates or phantom drops until an explicit refresh/rebuild. Live `listTables` / `list_table_names` still see truth. | `crates/repark-iceberg/src/catalog/tests.rs::live_list_sees_oob_create_and_drop_while_provider_snapshot_stale` (provider stays stale; live list does not); facade residual note in `test_catalog_staleness.py` module docstring |
| **OOB namespace drop phantoms on DF until full rebuild** | Live namespace list is clean; DF `schema_names` still phantoms until rebuild | `crates/repark-iceberg/src/catalog/tests.rs::oob_namespace_drop_phantoms_until_full_rebuild` |
| **Cross-process / other-session writers** | No pin in this tree that another process's Glue/S3 Tables mutation appears in free SQL without refresh. Catalog API list-on-access re-lists the Iceberg catalog handle for the **current** session's catalog; multi-writer cache coherence is **not** a stated product guarantee here. | *(nothing guaranteed — no cite invents one)* |
| **Bare `SHOW TABLES` without conf** | Still refused (§1). Enabling the conf does not remove the OOB residual above. | conf pins in §1 |

### Escape hatch

| Claim | Pin |
|---|---|
| `refresh_catalog_provider(catalog)` rebuilds the DF provider (SQL / information_schema residual recovery) | `python/repark/tests/test_catalog_staleness.py::test_refresh_catalog_provider_round_trip` |

ADR-0004 names explicit refresh as the free-SQL OOB recovery path; this contract does not
expand that ADR — it only records the consumer-visible obligation and the pin.

### Consumer guidance (G3-E7)

- Prefer **Catalog API** (`listTables` / `tableExists`) for "does model N's table exist before
  model N+1 runs" — list-on-access is the pin-backed path.
- Prefer **product SQL DDL** (door-routed `CREATE`/`DROP`/`CTAS`) over out-of-band Catalog
  mutations if free SQL must see the change without a manual refresh.
- If free SQL must see an OOB mutation, call `refresh_catalog_provider` (or accept the residual).
- Do not claim multi-session or multi-writer listing coherence without new pins.

---

## 4. Non-goals of this document

- No new SQL surface, no engine fix, no registry row edits.
- No promise of a transaction API (G3-E4).
- No reopening of settled absences (G3-E5 VIEW/INSERT_OVERWRITE rulings live in
  [sql-doors.md](sql-doors.md); G3-E1/E2/E6/E8 are engine gaps, not this contract).
- No restatement of ADR-0006 metadata-table hiding (pins live under that ADR and H-1c).

---

## 5. Provenance

| Intake row | Disposition in this doc |
|---|---|
| **G3-E3** | §1 Catalog-API-only introspection |
| **G3-E4** | §2 One `sql()` = one eager commit boundary; no multi-statement atomicity |
| **G3-E7** | §3 Catalog visibility after DDL — guarantees + honest residual |

Source intake (read-only; not edited by this change):
`planning/hardening/G3-engine-intake.md` § "Product statements" (workspace planning tree).

Changing a guarantee here requires a **new dated design pass** (or a code change that lands
new pins in the same PR) — not a silent prose edit that outruns the tests.
