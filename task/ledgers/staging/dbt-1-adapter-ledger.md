# Unit ledger — DBT-1 · a dbt path for RePark

**Retires:** this ledger moves to `../completed/` in the unit's last commit.

**Unit:** DBT-1 · **Date:** 2026-09-04 · **Model:** opus-5 ·
**Branch:** `feat/dbt-1` · **Base:** `origin/main` `55652ca`
**Driver:** [docs/cutover/inventory.md](../../../docs/cutover/inventory.md) §7 ruling 2 — gold
stays on Spark/Glue only until a dbt path lands; acceptance is cutover step C6.
**Registry:** [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) — the
statement shapes RePark refuses that dbt emits.

**Rubric:** STANDARD. `risk_tier: standard`.

## 1. Scope, as checkable propositions

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | The route is decided by measurement, not preference: every statement shape dbt emits for two `materialized='table'` `file_format='iceberg'` models and ten generic test blocks is run through `repark.sql()` on a memory-catalog Iceberg namespace, and each is recorded as served or refused with its exact message. | `python/dbt-repark/tests/test_statement_surface.py` (30 cases: 12 served, 16 refused, 2 probes) + §2, §3 | **PROVEN** |
| C-002 | `dbt-repark` exists as its own package under `python/dbt-repark/`: credentials (catalog, warehouse, `spark.sql.catalog.<name>.*` pass-through), a connection manager over one shared `ReparkSession` with per-thread cursors, a three-part relation, and an adapter whose `list_relations_without_caching` / `get_columns_in_relation` / `drop_relation` / `rename_relation` go through the surfaces RePark actually serves. | `python/dbt-repark/src/**`; `test_gold_models.py::test_docs_generate_reads_relations_and_columns`, `::test_relations_are_three_part`, `::test_a_missing_namespace_is_created`, `::test_a_second_profile_refuses_rather_than_reusing_the_session`, `::test_shared_session_serves_concurrent_statements`; all ten `test_cursor.py` cases; statement pins `S-RENAME` / `S-DROP` / `S-DROP-SCHEMA` | **PROVEN** |
| C-003 | `dbt run` builds the two gold models and `dbt test` runs ten test blocks green, against a memory catalog seeded from the S6 silver fixture, and the two tables answer the S6 measured rows. Red before the adapter, green after; the mutation table names which tests a broken CTAS macro reds. | `test_gold_models.py` + §5, §6 | **PROVEN** |
| C-004 | `incremental`, `snapshot` and `view` refuse with a message that names the reason, rather than emitting SQL RePark will fail on later — and so does every other configured clause RePark cannot serve. | `test_gold_models.py`, eight refusal cases: `view`, `incremental`, `snapshot`, `persist_docs.relation`, `persist_docs.columns`, `location_root`, `options`, `clustered_by` | **PROVEN** |
| C-005 | The Glue leg is written and skipped, gated on the same env variables as `python/repark/tests/test_aws_acceptance.py`, scoped to `testing_repark_acceptance`. Every refused statement shape has a registry row, and a gate runs the pins. Docs and maps are in lockstep. | `test_aws_acceptance_gold.py` (collected, skipped); registry §2.5 (five rows) + §7 (five rows); `make py-test-dbt` in `preflight` (§9); `docs/guide/dbt-on-repark.md`; five maps + AGENTS row | **PROVEN** |

`LOGIC_SCORE` = **5/5 `PROVEN`**.

## 2. Oracle and environment

| Half | Pin |
|---|---|
| RePark | `repark` 1.0.0 at base `55652ca`, native `_native.abi3.so` built from that tree (release). Memory catalog `ice`, namespace `gold`. No JVM, no Spark. |
| dbt | `dbt-core` 1.9.11, `dbt-adapters` 1.24.5, `dbt-common` 1.39.0, `dbt-spark` 1.9.3, `agate` 1.9.1, `Jinja2` 3.1.6, installed into the lane venv on 2026-09-04. |
| Gold SQL | **not re-typed here.** The two models and the silver fixture are the S6 program's, imported from `python/repark/tests/_sql_harden_cutover_run.py` (`_FCT_SQL`, `_AGG_SQL`, `_seed_gold_sql`). S6's row answers were measured against live Spark by SQL-HARDEN-1 and DATE-FN-1; this unit does not re-derive them. |

**Lane note (2026-09-04).** The lane venv's `repark.pth` pointed at a sibling checkout whose
native predated DATE-FN-1, so `DATE(...)` and `unix_timestamp(...)` — both load-bearing in the
gold fact model — were absent and every gold statement refused with `Invalid function 'date'`.
The `.pth` now points at this lane's own `python/repark/src`, with the base-commit native copied
in. No repository file changed; `*.so` is gitignored.

## 3. The measured statement surface (C-001)

What dbt emits, and what `repark.sql()` answers. Measured 2026-09-04 on the memory catalog. The
executable form is `python/dbt-repark/tests/test_statement_surface.py`; this table is its reading.

### 3.1 Served

| Shape | Emitting macro | Note |
|---|---|---|
| `create or replace table <cat>.<ns>.<t> using iceberg tblproperties (…) as <select>` | `spark__create_table_as` | the one statement the `table` materialization needs; re-running it replaces in place, so no drop is required |
| the same with `partitioned by (col)` | `spark__partition_cols` | the only placement clause served — see R13 |
| `drop table if exists <3-part>` (present or absent) | `spark__drop_relation` | |
| `alter table <3-part> rename to <3-part>` | `spark__rename_relation` | three-part only — see R-RENAME-TWO-PART |
| `create namespace if not exists <cat>.<ns>` | the `create_schema` override | |
| `show namespaces in <cat>` | the `list_schemas` override | |
| `alter table <3-part> set tblproperties (…)` | `apply_grants` neighbourhood | |
| `drop schema if exists <cat>.<ns> cascade` | `spark__drop_schema`, inherited | measured in round 2; the round-1 `repark__drop_schema` override was deleted as dead (§6 N5) |
| `select count(*) as failures, count(*) != 0 as should_warn, count(*) != 0 as should_error from ( … ) dbt_internal_test` | `default__get_test_sql` | all four generic-test bodies (`unique`, `not_null`, `accepted_values`, `relationships`) run inside it |
| `select * from ( … ) dbt_internal_test limit N` | `get_limit_subquery_sql` | |

### 3.2 Refused, with the exact message

| # | Shape | Emitting macro | RePark |
|---|---|---|---|
| R1 | `show databases` | `spark__list_schemas` | `AnalysisException: Error during planning: SHOW NAMESPACES requires an explicit catalog — SHOW NAMESPACES IN <catalog> (RePark has no current-catalog concept, so there is no default to resolve against)` |
| R2 | `create schema if not exists <ns>` | `spark__create_schema` | `AnalysisException: Error during planning: expected a two-part `catalog.namespace` name, got `gold`` |
| R3 | `show table extended in <ns> like '*'` | `spark__list_relations_without_caching` | `AnalysisException: Error during planning: SHOW [VARIABLE] is not supported unless information_schema is enabled` |
| R4 | `show tables in <ns> like '*'` | `list_relations_show_tables_without_caching` | `UnsupportedOperationException: This feature is not implemented: SHOW TABLES IN not supported` |
| R5 | `describe extended <ns>.<t>` | `describe_table_extended_without_caching` | `AnalysisException: Error during planning: table 'datafusion.gold.<t>' not found` — a two-part name in `DESCRIBE` resolves against the DataFusion default catalog, while a two-part name in `SELECT` resolves against the session's registered one |
| R6 | `describe extended <3-part>` | `spark__get_columns_in_relation_raw` | **runs**, but answers `column_name / data_type / is_nullable` with Arrow spellings (`Utf8`, `Int32`, `Date32`) and no `# Detailed Table Information` block, so `parse_describe_extended` yields no columns, no `Provider:` and no `Type:` |
| R7 | `show tblproperties <rel>` | `fetch_tbl_properties` | `AnalysisException: Error during planning: SHOW [VARIABLE] is not supported unless information_schema is enabled` |
| R8 | `create or replace view <rel> as <select>` | `spark__create_view_as` | `PySparkException: Unexpected => register_table does not support tables with data.` |
| R9 | `create or replace temporary view <rel> as <select>` | `spark__create_temporary_view` | `UnsupportedOperationException: This feature is not implemented: Temporary views not supported` |
| R10 | `alter table <ns>.<t> rename to <ns>.<t2>` | `spark__rename_relation` | `AnalysisException: Error during planning: ALTER TABLE expects a three-part `catalog.namespace.table` name, got `gold.<t>`` |
| R11 | `alter table <rel> alter column <c> comment '…'` | `spark__alter_column_comment` | `UnsupportedOperationException: ALTER COLUMN … COMMENT is not supported yet via SQL — column COMMENT is accepted on ADD COLUMN; UpdateColumnDoc is available on the write primitive (I6 stretch)` |
| R12 | `set <key> = <value>` | `server_side_parameters` | `PySparkException: datafusion engine error: Invalid or Unsupported Configuration: Could not find config namespace "spark"` — session config is a builder concern, not a statement |
| R13 | `create or replace table … location '…' as <select>` | `spark__location_clause` | `UnsupportedOperationException: This feature is not implemented: CREATE TABLE … LOCATION is not supported for Iceberg CTAS yet — table location is derived from the namespace warehouse (or service-managed catalog)` |
| R14 | `create or replace table … comment '…' as <select>` | `spark__comment_clause` | `UnsupportedOperationException: CREATE TABLE … COMMENT is not supported for Iceberg CTAS yet — use TBLPROPERTIES or ALTER TABLE when comment support lands` |
| R15 | `create or replace table … tblproperties (…) comment '…' as <select>` | `spark__comment_clause` | `SQL error: ParserError("Expected: end of statement, found: using at Line: 1, Column: 39")` — **the refusal names the wrong token.** The clause that failed is `comment`; the parser blames `using`, which is correct. This is the shape dbt actually emits, because `create_table_as` always puts `tblproperties` first |
| R16 | `create or replace table … options (k "v") as <select>` | `spark__options_clause` | the same misleading `ParserError` as R15 |
| R17 | `create or replace table … clustered by (c) into 4 buckets as <select>` | `spark__clustered_cols` | the same misleading `ParserError` as R15 |

R13–R17 were found late, while verifying a claim in the user guide rather than while building —
`persist_docs.relation` and the three placement clauses are configuration a real gold project can
carry, and each of them reaches the parser as part of the CTAS. R15 is the finding worth keeping:
the message is not merely unhelpful, it **points at a clause that is correct**.

Two root causes account for R1, R2, R5 and R10: **RePark has no current-catalog for free SQL**,
so `DESCRIBE`, `ALTER TABLE` and namespace DDL need the catalog part, which `dbt-spark`'s
relation cannot render (`SparkIncludePolicy.database = False`, and `SparkRelation.render()`
raises if both database and schema are included). R3, R4, R6 and R7 are one cause: **there is no
SQL listing or description surface**; the facade `Catalog` is the supported one (registry `ST-1`).
R8, R9 and R11 are three separate engine gaps.

## 4. The route (C-001)

### 4.1 Adapter, not Thrift

The brief's alternative was a Spark-Thrift-compatible endpoint so unmodified `dbt-spark`
connects over the wire. §3.2 settles it: **every refusal is in the statement surface, not the
transport.** A Thrift server would carry `show table extended`, `show tables in`, `describe
extended <2-part>`, `create or replace temporary view` and the rest to the same planner and
return the same errors, so it delivers a wire protocol and no working model. It would also be a
new long-lived network service on a single-node engine — the largest surface in the unit for the
smallest result. **Route: an in-process adapter.** The transport question can be revisited on its
own merits (a Flight SQL handler is already the named second thin adapter in
[docs/adr/0004-server-prep-disciplines.md](../../../docs/adr/0004-server-prep-disciplines.md));
it is not what gold needs.

### 4.2 Subclass `dbt-spark`, override what RePark refuses

The second decision is `SQLAdapter` from scratch versus `dbt-spark`'s `SparkAdapter` with
`dependencies=["spark"]`. Measured, not assumed:

- **What carries over unchanged.** `spark__create_table_as` for `file_format in ('delta',
  'iceberg')` emits `create or replace table <rel> using iceberg tblproperties (…) as <select>`
  — precisely the statement §3.1 shows RePark serves. Of the clause macros it calls, two are
  served and reused (`tblproperties_clause`, `partition_cols`) and four are refused and
  overridden (`options_clause`, `clustered_cols`, `location_clause`, `comment_clause`).
  `dbt-spark`'s `table` materialization body also carries: when `old_relation.is_iceberg` is
  true it *skips* the drop and goes straight to the replace, which is the shape RePark wants.
  `spark__drop_relation` and `spark__drop_schema` carry. `SparkColumn`, `SparkConfig` and the
  `convert_*_type` classmethods carry.
- **What does not.** `create_schema`, `list_schemas`, `list_relations_without_caching`,
  `get_columns_in_relation`, `rename_relation` (two-part), `create_view_as`,
  `create_temporary_view`, `alter_column_comment`, `make_temp_relation`, and the `incremental`
  and `snapshot` materializations. Also the `Relation` (cannot render three parts), the
  `Credentials` (`host` required, `database` forbidden) and the `ConnectionManager` (four wire
  transports, none of them ours).
- **The deciding argument is one reading of `file_format`, not the line count.**
  [docs/cutover/inventory.md](../../../docs/cutover/inventory.md) records the gold stage as two
  models at `materialized='table'` with `file_format='iceberg'` and ten test blocks. It records
  no other model config, so a claim that the project "already configures" `partition_by`,
  `location_root` or `options` would be unfounded — round 1 made that claim and it is withdrawn
  here (finding F-DBT-1-6). What survives the correction is the argument that matters:
  `file_format='iceberg'` is the key the whole materialization turns on, and `dbt-spark` already
  reads it in two places — `spark__create_table_as`'s branch and the `table` materialization's
  `is_iceberg` test. Re-authoring from `SQLAdapter` would put a second reading of that key
  beside dbt-spark's, and a silent disagreement between them is a wrong table, not an error.
  Reusing the macro that already produces the served statement removes that class entirely; the
  cutover also must not edit gold's `dbt_project.yml`, and it does not have to.
- **The cost is bounded.** `dbt-spark` 1.9.3's install requirements are `dbt-adapters`,
  `dbt-common`, `dbt-core`, `sqlparams`. `pyhive`, `thrift`, `pyodbc` and `pyspark` are all
  *extras*, so the dependency brings no driver and no JVM.

**Route: `dbt-repark` subclasses `SparkAdapter`, declares `dependencies=["spark"]`, and
overrides exactly the surfaces in the "what does not" list.** Materialization lookup follows the
plugin dependency chain (`Manifest._get_parent_adapter_types`), so a `repark`-prefixed
materialization wins over the inherited `spark` one at specificity 0 — which is how
`incremental` and `snapshot` become explicit refusals rather than inherited behaviour.

### 4.3 Consequences that become deliverables

| Refusal | What the adapter does instead |
|---|---|
| R1, R2, R5, R10 (no current catalog) | `ReparkRelation` renders `catalog.namespace.identifier`; `create_schema` and `list_schemas` are overridden to the catalog-qualified forms |
| R3, R4, R6, R7 (no SQL listing) | `list_relations_without_caching` uses `Catalog.list_tables`; `get_columns_in_relation` uses `session.table(...).schema`, which answers Spark spellings (`string`, `int`, `date`) |
| R8 (no views) | the `view` materialization refuses; registry row `DBT-VIEW-1` |
| R9 (no temporary views) | `incremental` and `snapshot` refuse; registry row `DBT-TEMPVIEW-1` |
| R11 (no column comment) | `persist_docs.columns` refuses; registry row `DBT-COLCOMMENT-1` |
| R12 (no `SET`) | session config is a credentials field, applied on the builder before `getOrCreate` |
| R14, R15 (no CTAS comment) | `persist_docs.relation` refuses; registry row `DBT-RELCOMMENT-1` |
| R13, R16, R17 (no other placement clause) | `location_root`, `options` and `clustered_by` refuse; registry row `DBT-CTASCLAUSE-1` |

## 5. Acceptance (C-003)

Local, memory catalog, no JVM. `make py-test-dbt` → **59 passed, 1 skipped**, 60 collected: 30
statement-surface cases, 10 cursor cases, 19 acceptance cases, 1 deferred Glue leg. Re-measured
in round 2 (2026-09-04) after the review; the round-1 counts in this section were wrong and are
corrected here rather than edited away.

| What | Result |
|---|---|
| `dbt run` | both models build; `gold_fct` then `gold_agg` through `ref()`, five silver tables through `source()` |
| gold fact rows | `(s1, 10, 15)`, `(s2, 20, 40)` — the S6 measured answer |
| gold aggregate rows | `(10, Thursday, 8.0, 9.0, 15.0, 1, 1)`, `(20, Friday, 6.0, 7.0, 40.0, 1, 1)` — the S6 measured answer |
| `dbt test` | 10 results, every one `pass`: two `unique`, five `not_null`, two `accepted_values`, one `relationships` |
| second `dbt run` | both tables replaced in place, rows unchanged |
| `dbt run --full-refresh` | accepted, rows unchanged |
| table format | both tables carry one `append` snapshot, `format-version` 2, and the project's `write.merge.mode` / `write.target-file-size-bytes` in the metadata |
| `dbt docs generate` | `catalog.json` carries both models with Spark type spellings (`string`, `int`, `date`, `timestamp`) and the five sources |
| a namespace that does not exist | created before the model builds; `show namespaces` then lists both |
| `partition_by` | builds, rows unchanged |
| refusals (eight) | `view`, `incremental`, `snapshot`, `persist_docs.relation`, `persist_docs.columns`, `location_root`, `options`, `clustered_by` — each fails at **compile** time with its registry row id in the message |
| cursor | `fetchall` / `fetchmany` / `fetchone` over three-row results; `description` over two columns; a zero-column DDL result answers `None`; bindings refused; two cursors independent |
| threads | one shared session answers 16 concurrent reads from 8 workers; a second profile with a different catalog is refused, not reused |

**Red first, re-measured.** With `python/dbt-repark/src` moved aside and the bytecode cache
cleared: **28 red of 60** — all 18 `test_gold_models.py` cases (17 fixture errors + 1 failure)
and all 10 `test_cursor.py` cases, every one `ModuleNotFoundError: No module named
'dbt.adapters.repark'`. The 30 `test_statement_surface.py` cases stay **green**, correctly: they
measure `repark.sql()` directly and need no adapter, which is why they are the design evidence
rather than acceptance. The Glue leg stays skipped. Round 1 recorded "7 of 7" for this check; it
was the count of a smaller, earlier module and is withdrawn (finding F-DBT-1-5).

**Order of work, stated honestly.** The statement-surface measurement and the design ledger
landed first, in their own commit, before any adapter existed. The acceptance module was written
*after* the adapter in the same working session, and the red-first check above was then run by
removing the adapter — a demonstration that the pins bind, not a claim that the test file
predates the source. §6 is the stronger evidence and is what the coverage attestation cites.

## 6. Mutation (C-003)

Re-measured in round 2 with a **control**, because the round-1 table was not sound: its CTAS
mutations replaced the whole macro and so deleted four refusal call sites at the same time,
making the readings compound rather than single-cause. Every row below changes **one** thing.

**Provenance.** The package ships no `repark__create_table_as`: the iceberg CTAS is inherited
from `dbt-spark`. M0–M2 and N1 were therefore applied by **appending a `repark__create_table_as`
override to the package's own
`src/dbt/include/repark/macros/adapters.sql`**, which shadows the inherited macro at dispatch
specificity 0. Nothing in `site-packages` was edited. M0 is the control: the appended macro is a
faithful copy of `spark__create_table_as`'s iceberg arm, including all seven clause calls, so a
zero-red control proves each later reading comes from the single line that changed. N2–N5 mutate
macros and methods the package **does** ship. Every mutation was applied to the delivered tree,
the whole suite run, and the tree restored.

| # | Mutation | Target | Red of 60 |
|---|---|---|---|
| M0 | control: faithful copy of the inherited iceberg arm | appended override | **0** — the base is faithful |
| M1 | `create table` instead of `create or replace table`, nothing else changed | appended override | 2 — `test_dbt_run_is_idempotent`, `test_full_refresh_rebuilds_both_models` |
| M2 | `{{ file_format_clause() }}` removed, nothing else changed | appended override | **0** — see below |
| N1 | `{{ tblproperties_clause() }}` removed, nothing else changed | appended override | 1 — `test_built_tables_carry_the_configured_iceberg_properties` |
| M3 | `list_relations_without_caching` returns `[]` | `impl.py` | 1 — `test_docs_generate_reads_relations_and_columns` |
| M4 | `get_columns_in_relation` returns `[]` | `impl.py` | 1 — `test_docs_generate_reads_relations_and_columns` |
| M5 | `ReparkIncludePolicy.database = False` (two-part relations) | `relation.py` | 1 — `test_relations_are_three_part` |
| N2 | `repark__create_schema` emits the one-part form | `adapters.sql` | 1 — `test_a_missing_namespace_is_created` |
| N3 | `repark__list_schemas` emits `show databases` | `adapters.sql` | 16 — the whole `dbt run` path |
| N4 | `ReparkCursor.fetchall` returns `rows[:1]` | `session.py` | 4 — the multi-row cursor cases |
| N5 | `repark__drop_schema` loses the catalog part | `adapters.sql` | **0** — the macro was deleted; see below |

**M2 reds nothing, and that is a true reading, not a gap.** A RePark catalog makes **every** table
it holds an Iceberg table: `create or replace table … as select` with no `using` clause produces
the same format-version-2 Iceberg table with the same metadata as the clause-bearing form
(measured directly, 2026-09-04). The clause is a no-op there, so no test can distinguish it and
none should claim to. Round 1 asserted that the table-property assertion "closed" M2; it does
not, it closes **N1**, which is the mutation that matters — dropping `tblproperties` silently
loses `write.merge.mode` and `write.target-file-size-bytes`, and that *is* observable.

**N5 reds nothing because the branch was dead, so it was deleted.** `repark__drop_schema` had no
caller on any dbt path this adapter serves. Measured 2026-09-04: `drop schema if exists
<catalog>.<namespace> cascade` — the form the **inherited** `spark__drop_schema` emits against a
three-part-rendering relation — is served by the SQL door and drops the namespace. The override
was therefore removing nothing and pinned by nothing, so it is gone; the inherited macro's
statement is pinned instead at `test_served_shapes_run[S-DROP-SCHEMA]`.

**M5 is honestly weak.** With two-part relations the gold project still builds, because RePark's
`SELECT` *does* resolve a two-part name against the session's catalog (registry `DBT-QUALIFY-1`).
The three-part policy is load-bearing for `DESCRIBE` and `ALTER TABLE`, which this project never
reaches, so its liveness rests on the direct relation pin plus the statement-surface rows
`R-DESCRIBE-TWO-PART` and `R-RENAME-TWO-PART` rather than on the acceptance path.

## 7. Findings

| ID | Severity | Category | Clause | Disposition | What |
|---|---|---|---|---|---|
| F-DBT-1-1 | S2 | AT-10 | C-003 | REMEDIATED | The suite could not see either overridden catalog method (M3/M4 reddened nothing). Closed by the `dbt docs generate` pin. |
| F-DBT-1-2 | S2 | AT-1 | C-004 | REMEDIATED | Four configured clauses a real gold project can carry — `persist_docs.relation`, `location_root`, `options`, `clustered_by` — reached the parser and failed at run time with a message naming the wrong token. Closed by four compile-time refusals and registry rows `DBT-RELCOMMENT-1` / `DBT-CTASCLAUSE-1`. |
| F-DBT-1-3 | S3 | AT-9 | C-001 | ACCEPTED_FLAGGED | RePark's parser reports the earliest unconsumed token, so a trailing clause failure is blamed on `using`. Engine-side; filed as the shared note on `DBT-RELCOMMENT-1` and `DBT-CTASCLAUSE-1`, not fixed here. |
| F-DBT-1-4 | S3 | AT-8 | C-002 | ACCEPTED_FLAGGED | A memory catalog lives in the session that registered it, so the `warehouse` profile field only works when one process both writes and reads. Documented in the guide and in `tests/map.md`; the durable path is `catalog_properties`. |
| F-DBT-1-5 | S2 | AT-10 | C-002, C-003 | REMEDIATED | Round 2. `ReparkCursor`'s multi-row path had **zero** coverage: `fetchall` truncated to one row survived the whole suite. Nothing on the `dbt run` path returns more than one row through the cursor. Closed by `test_cursor.py` (10 cases, N4 now 4 red). The same round corrected three false counts in §5 and §6. |
| F-DBT-1-6 | S2 | AT-1 | C-001 | REMEDIATED | Round 2. §4.2 and three maps claimed the subclass keeps clause macros "the production gold project already configures". The cutover inventory records only `materialized='table'`, `file_format='iceberg'` and ten test blocks; the adapter in fact **refuses** `location_root` and `options`. The argument is rewritten to the one key that is recorded and load-bearing. |
| F-DBT-1-7 | S2 | AT-10 | C-002 | REMEDIATED | Round 2. `repark__create_schema` and `repark__list_schemas` were unpinned: the fixture pre-created its namespace, so no test reached the create path. Closed by `test_a_missing_namespace_is_created` (N2 now 1 red, N3 16 red). |
| F-DBT-1-8 | S2 | AT-10 | C-005 | REMEDIATED | Round 2. Every registry pin named a file no gate ran. Closed by `make py-test-dbt`, wired into `make preflight`. It is **not** in `make ci`: see §9. |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: dbt-1-adapter
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause walked against the built package. C-001 is the 30-case measured table; C-003 is dbt run plus dbt test against the S6 answers; C-004 is eight compile-time refusals. Round 2 withdrew an unfounded claim about the production project's config (F-DBT-1-6).
      artifacts: [python/dbt-repark/tests/test_statement_surface.py, python/dbt-repark/tests/test_gold_models.py]
    - id: AT-2
      status: ATTACKED
      evidence: Absent relation on drop, absent namespace on list, missing namespace on run, empty and zero-column results through the cursor, multi-row paging to exhaustion, two-part versus three-part names, and every unsupported clause driven from a real dbt config rather than a hand-written statement.
      artifacts: [python/dbt-repark/tests/test_statement_surface.py, python/dbt-repark/tests/test_cursor.py]
    - id: AT-3
      status: ATTACKED
      evidence: A refusal reaches dbt as DbtRuntimeError with RePark's message intact; a failed open marks the connection FAIL rather than leaving a half-attached handle; a second dbt run replaces in place, so a re-run after a partial failure is idempotent. No transactions exist, which the guide states rather than implying.
      artifacts: [python/dbt-repark/src/dbt/adapters/repark/connections.py, python/dbt-repark/tests/test_gold_models.py]
    - id: AT-4
      status: ATTACKED
      evidence: One shared session under a lock; per-statement cursors on the calling thread, pinned independent of one another; 16 concurrent reads from 8 workers; a second profile with a different key is refused rather than silently attached to the live engine.
      artifacts: [python/dbt-repark/src/dbt/adapters/repark/session.py, python/dbt-repark/tests/test_cursor.py]
    - id: AT-5
      status: ATTACKED
      evidence: The Glue leg is skipped by default, writes only into testing_repark_acceptance, issues no DROP, and reads its ARN and warehouse from the environment. No credentials, no .github change, no Cargo change, no secret in any output. The cursor refuses bindings rather than interpolating them into SQL, which is pinned.
      artifacts: [python/dbt-repark/tests/test_aws_acceptance_gold.py, python/dbt-repark/tests/test_cursor.py]
    - id: AT-6
      status: ATTACKED
      evidence: The gold SQL is imported from the S6 program, never copied, so the two cannot drift. The built tables are asserted to be Iceberg at format-version 2 with the project's write properties - the assertion N1 reds - which is what a reader on Spark or Athena depends on.
      artifacts: [python/dbt-repark/tests/test_gold_models.py, python/repark/tests/_sql_harden_cutover_run.py]
    - id: AT-7
      status: N/A
      justification: No system-breaking resource behaviour. One session, one cursor per statement, results materialised once per cursor; the suite runs in about 35 seconds on the memory catalog.
    - id: AT-8
      status: ATTACKED
      evidence: dbt-spark 1.9.3 internals are relied on deliberately and named - create_table_as, the table materialization, drop_relation, drop_schema, SparkColumn, SparkConfig - with the version pinned in pyproject and in the Makefile target, and the dependency chain verified against dbt's own materialization lookup. Every inherited macro that RePark refuses is overridden; one override that shadowed a working inherited macro was deleted rather than kept (N5). F-DBT-1-4 records the memory-catalog assumption.
      artifacts: [python/dbt-repark/pyproject.toml, python/dbt-repark/src/dbt/include/repark/macros/map.md]
    - id: AT-9
      status: ATTACKED
      evidence: Every refusal names its registry row id in the message, so a user reading a dbt failure lands on the document that owns the fact. F-DBT-1-3 files the one diagnostic this unit cannot fix from the adapter.
      artifacts: [python/dbt-repark/src/dbt/include/repark/macros/materializations.sql, docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Eleven mutations including a zero-red control, each with its red set named, in the table above; the red-first removal of the whole source tree (28 red of 60); and findings F-DBT-1-1, F-DBT-1-5, F-DBT-1-7 and F-DBT-1-8, which are the record of mutations that reddened nothing until the suite was strengthened and of pins no gate ran. M2's and M5's weaknesses are stated rather than hidden.
      artifacts: [task/ledgers/staging/dbt-1-adapter-ledger.md, python/dbt-repark/tests/test_cursor.py]
  complete: true
```

## 8. What this unit did not do

- **No publish.** No wheel, no PyPI name, no uv workspace member, no `uv.lock` row. A member
  would put `dbt-core` into the repository lock for a package nothing in `make ci` imports.
- **No Thrift.** Rejected on measurement in §4.1, not deferred for cost.
- **No `incremental`, no `snapshot`.** Both need temporary views. They refuse.
- **The Glue leg is written, not run.** C-005 ships it skipped; the orchestrator runs it, and
  that run is cutover step C6.
- **`python/dbt-repark` is not in `check_python_conventions.py`'s scan roots**, which are
  `python/repark/src`, `python/repark-parity` and `scripts`. That is why `ReparkCredentials` can
  be a `@dataclass`: dbt's `Credentials` base is one, and the profile deserialiser requires it.
  Adding the scan root would need an owner ruling on that row, and is a decision of its own.
- **The suite is not in `make ci`.** §9 says why, and what CI wiring is still owed.

## 9. Where the suite runs (C-005)

`make py-test-dbt` runs `python/dbt-repark/tests` against the installed native module, provisioning
`dbt-core==1.9.11` and `dbt-spark==1.9.3`. It is wired into **`make preflight`**, immediately
after `py-test-facade`, and **not** into `make ci`. Three measured reasons:

1. `make ci` is native-build-free by design — the Makefile's own block above `test:` says so, and
   `verify` inherits it for inner-loop speed. This suite imports `repark`, so it needs the
   compiled module exactly as the facade suite does, and `py-test-facade` is likewise in
   `preflight` and not in `ci`.
2. `make ci` is **not what CI executes.** `.github/workflows/ci.yml` invokes each target and
   script individually (`make rust-clippy`, `./scripts/check_lib_py.sh`, `uvx ruff@…`, …). Adding
   a name to the `ci:` list therefore changes nothing in CI.
3. ci.yml's `Python` job does not build the native module — its own step comment says so, which is
   why example execution is skipped there and left to `wheels.yml` `smoke`. That job, which builds
   the wheel and runs the facade suite against it, is the only existing CI home where this suite
   could run.

Wiring it into CI is a `.github` change, which this unit may not make. The exact change is in the
hand-back for the orchestrator.

## 10. Round-2 review gaps

The Opus critic re-ran the unit on a fresh clone, confirmed the acceptance (independently derived
golden rows, tests binding on a duplicate row, idempotent runs, one snapshot per run, the Thrift
argument, the four compile-time refusals) and returned seven S2 and three S3 findings. All ten
are resolved here; none of the confirmed behaviour changed.

| # | Finding | Resolution |
|---|---|---|
| S2-1 | `ReparkCursor`'s multi-row path had zero coverage; `fetchall` → `rows[:1]` survived the suite | `tests/test_cursor.py`, 10 cases. Mutation N4 now reds 4. Filed as F-DBT-1-5. |
| S2-2 | Registry rows pinned to files no gate runs | `make py-test-dbt`, wired into `preflight`. **Not** into `ci` — §9 gives the three measured reasons and the CI change is in the hand-back. Filed as F-DBT-1-8. |
| S2-3 | §2.5 preamble said "nine served and ten refused" | Corrected to twelve served and sixteen refused, which is what the file collects. |
| S2-4 | Three false counts: red-first "7 of 7", C-004 "six" refusals then seven listed, M1 "1 red" | Re-measured. Red-first is 28 red of 60; C-004 lists eight refusals; M1 reds 2. §5 and §6 rewritten from measurement. |
| S2-5 | R2 and R3 had no registry row | `DBT-CREATENS-1` filed for the one-part namespace DDL. R3 folded into `DBT-TBLPROPS-1`, whose message it shares **verbatim** — one mechanism, one description (§6's rule); the row is retitled and carries both pins. |
| S2-6 | §6 claimed the mutations hit the delivered tree, but named a macro the package does not ship | Provenance stated: the CTAS mutations append a `repark__create_table_as` override to the package's own macro file, shadowing the inherited macro; nothing in `site-packages` was touched. A control (M0) and five mutations of macros the package **does** ship (N1–N5) were added. |
| S2-7 | The route argument overstated what the gold project configures | Rewritten in the ledger and three maps to the one key the inventory records and the materialization turns on. Filed as F-DBT-1-6. |
| S3-1 | R12 `SET` unpinned | Pinned at `[R-SET-CONF]`. The registry's queue entry `B-TZ-5` already owned the message and, by §6's rule, a queued candidate becomes a row in the change that pins it — so it is now a §7 row and the queue carries a dated departure note. |
| S3-2 | R13's `LOCATION` message truncated | Quoted whole, in §3.2, in `DBT-CTASCLAUSE-1`, and in the pin. |
| S3-3 | The `snapshot` refusal had no test | `test_snapshot_materialization_refuses`, driving a real snapshot block through `dbt snapshot`. |

Two things the round-2 work found on its own, beyond the ten: `repark__create_schema` /
`repark__list_schemas` were unpinned (F-DBT-1-7), and `repark__drop_schema` was a dead override
that the inherited macro already covers — measured and deleted (§6 N5).
