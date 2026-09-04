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
| C-001 | The route is decided by measurement, not preference: every statement shape dbt emits for two `materialized='table'` `file_format='iceberg'` models and ten generic test blocks is run through `repark.sql()` on a memory-catalog Iceberg namespace, and each is recorded as served or refused with its exact message. | `python/dbt-repark/tests/test_statement_surface.py` (27 cases) + §2, §3 | **PROVEN** |
| C-002 | `dbt-repark` exists as its own package under `python/dbt-repark/`: credentials (catalog, warehouse, `spark.sql.catalog.<name>.*` pass-through), a connection manager over one shared `ReparkSession` with per-thread cursors, a three-part relation, and an adapter whose `list_relations_without_caching` / `get_columns_in_relation` / `drop_relation` / `rename_relation` go through the surfaces RePark actually serves. | `python/dbt-repark/src/**`; `test_gold_models.py::test_docs_generate_reads_relations_and_columns`, `::test_relations_are_three_part`, `::test_a_second_profile_refuses_rather_than_reusing_the_session`, `::test_shared_session_serves_concurrent_statements`; statement pins `S-RENAME` / `S-DROP` | **PROVEN** |
| C-003 | `dbt run` builds the two gold models and `dbt test` runs ten test blocks green, against a memory catalog seeded from the S6 silver fixture, and the two tables answer the S6 measured rows. Red before the adapter, green after; the mutation table names which tests a broken CTAS macro reds. | `test_gold_models.py` + §5, §6 | **PROVEN** |
| C-004 | `incremental`, `snapshot` and `view` refuse with a message that names the reason, rather than emitting SQL RePark will fail on later — and so does every other configured clause RePark cannot serve. | `test_gold_models.py` six refusal cases (`view`, `incremental`, `persist_docs.relation`, `persist_docs.columns`, `location_root`, `options`, `clustered_by`) | **PROVEN** |
| C-005 | The Glue leg is written and skipped, gated on the same env variables as `python/repark/tests/test_aws_acceptance.py`, scoped to `testing_repark_acceptance`. Every refused statement shape has a registry row; docs and maps are in lockstep. | `test_aws_acceptance_gold.py` (collected, skipped); registry §2.5 + §7 (eight rows); `docs/guide/dbt-on-repark.md`; five maps + AGENTS row | **PROVEN** |

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
| R13 | `create or replace table … location '…' as <select>` | `spark__location_clause` | `UnsupportedOperationException: CREATE TABLE … LOCATION is not supported for Iceberg CTAS yet — table location is derived from the namespace warehouse` |
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
  — precisely the statement §3.1 shows RePark serves, including the `partition_by`,
  `location_root`, `options` and `tblproperties` clause macros the production gold project
  already configures. `dbt-spark`'s `table` materialization body also carries: when
  `old_relation.is_iceberg` is true it *skips* the drop and goes straight to the replace, which
  is the shape RePark wants. `spark__drop_relation` carries. `SparkColumn`, `SparkConfig` and
  the `convert_*_type` classmethods carry.
- **What does not.** `create_schema`, `list_schemas`, `list_relations_without_caching`,
  `get_columns_in_relation`, `rename_relation` (two-part), `create_view_as`,
  `create_temporary_view`, `alter_column_comment`, `make_temp_relation`, and the `incremental`
  and `snapshot` materializations. Also the `Relation` (cannot render three parts), the
  `Credentials` (`host` required, `database` forbidden) and the `ConnectionManager` (four wire
  transports, none of them ours).
- **The deciding argument is the production project, not the line count.** The cutover must not
  edit gold's `dbt_project.yml`. Re-authoring the clause macros would put RePark's reading of
  `file_format` / `tblproperties` / `partition_by` beside `dbt-spark`'s, where a silent
  disagreement is a wrong table, not an error. Reusing the macro that already produces the
  served statement removes that class entirely.
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

Local, memory catalog, no JVM. `.venv/bin/python -m pytest python/dbt-repark/tests -q` →
**44 passed, 1 skipped** (the skip is the Glue leg, C-005), 45 collected: 27 statement-surface
cases, 17 acceptance cases, 1 deferred.

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
| `partition_by` | builds, rows unchanged |
| refusals | `view`, `incremental`, `persist_docs.relation`, `persist_docs.columns`, `location_root`, `options`, `clustered_by` — each fails at **compile** time with its registry row id in the message |
| threads | one shared session answers 16 concurrent reads from 8 workers; a second profile with a different catalog is refused, not reused |

**Red first.** With `python/dbt-repark/src` moved aside and the bytecode cache cleared, the whole
acceptance module errors 7 of 7 at fixture setup with `ModuleNotFoundError: No module named
'dbt.adapters.repark'`. Restored, it is green. Measured 2026-09-04.

**Order of work, stated honestly.** The statement-surface measurement and the design ledger
landed first, in their own commit, before any adapter existed. The acceptance module was written
*after* the adapter in the same working session, and the red-first check above was then run by
removing the adapter — a demonstration that the pins bind, not a claim that the test file
predates the source. The mutation table in §6 is the stronger evidence and is what the coverage
attestation cites.

## 6. Mutation (C-003)

Every mutation was applied to the delivered tree, the whole suite run, and the tree restored.

| # | Mutation | Red of 45 |
|---|---|---|
| M1 | `repark__create_table_as` emits `create table` instead of `create or replace table` | 1 — `test_dbt_run_is_idempotent` |
| M2 | `repark__create_table_as` drops `using iceberg` **and** `tblproperties` | 1 — `test_built_tables_carry_the_configured_iceberg_properties` |
| M3 | `list_relations_without_caching` returns `[]` always | 1 — `test_docs_generate_reads_relations_and_columns` |
| M4 | `get_columns_in_relation` returns `[]` always | 1 — `test_docs_generate_reads_relations_and_columns` |
| M5 | `ReparkIncludePolicy.database = False` (two-part relations) | 1 — `test_relations_are_three_part` |

**M2 and M3 were originally invisible, and that is the useful part of this table.** The first M2
run reddened nothing: dropping `using iceberg` changes no observable behaviour, because a RePark
catalog makes **every** table it holds an Iceberg table — the clause is a no-op there, and a
suite that only checks rows cannot tell. The property assertion
(`test_built_tables_carry_the_configured_iceberg_properties`) was added to close that, and it
also closes the real risk, which was never the `using` keyword but the `tblproperties` beside it.
M3 and M4 were likewise invisible until `test_docs_generate_reads_relations_and_columns` was
added: nothing on the `dbt run` path reads either method's output, so `dbt docs generate` is the
only entry point where they are observable.

**M5 is honestly weak.** With two-part relations the gold project still builds, because RePark's
`SELECT` *does* resolve a two-part name against the session's catalog (registry `DBT-QUALIFY-1`).
The three-part policy is load-bearing for `DESCRIBE` and `ALTER TABLE`, which this project never
reaches, so its liveness rests on the direct relation pin plus the statement-surface rows
`R-DESCRIBE-TWO-PART` and `R-RENAME-TWO-PART` rather than on the acceptance path.

## 7. Findings

| ID | Severity | Category | Clause | Disposition | What |
|---|---|---|---|---|---|
| F-DBT-1-1 | S2 | AT-10 | C-003 | REMEDIATED | The suite could not tell an Iceberg CTAS from a plain one, nor see either catalog method. M2/M3/M4 reddened nothing. Closed by the table-property assertion and the `dbt docs generate` pin. |
| F-DBT-1-2 | S2 | AT-1 | C-004 | REMEDIATED | Four configured clauses a real gold project can carry — `persist_docs.relation`, `location_root`, `options`, `clustered_by` — reached the parser and failed at run time with a message naming the wrong token. Closed by four compile-time refusals and registry rows `DBT-RELCOMMENT-1` / `DBT-CTASCLAUSE-1`. |
| F-DBT-1-3 | S3 | AT-9 | C-001 | ACCEPTED_FLAGGED | RePark's parser reports the earliest unconsumed token, so a trailing clause failure is blamed on `using`. Engine-side; filed as the shared note on `DBT-RELCOMMENT-1` and `DBT-CTASCLAUSE-1`, not fixed here. |
| F-DBT-1-4 | S3 | AT-8 | C-002 | ACCEPTED_FLAGGED | A memory catalog lives in the session that registered it, so the `warehouse` profile field only works when one process both writes and reads. Documented in the guide and in `tests/map.md`; the durable path is `catalog_properties`. |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: dbt-1-adapter
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause walked against the built package. C-001 is the 27-case measured table; C-003 is dbt run plus dbt test against the S6 answers; C-004 is seven compile-time refusals, four of which the first pass missed (F-DBT-1-2).
      artifacts: [python/dbt-repark/tests/test_statement_surface.py, python/dbt-repark/tests/test_gold_models.py]
    - id: AT-2
      status: ATTACKED
      evidence: Absent relation on drop, absent namespace on list, empty result set through the cursor, two-part versus three-part names, and every unsupported clause driven from a real dbt config rather than a hand-written statement.
      artifacts: [python/dbt-repark/tests/test_statement_surface.py, python/dbt-repark/src/dbt/adapters/repark/impl.py]
    - id: AT-3
      status: ATTACKED
      evidence: A refusal reaches dbt as DbtRuntimeError with RePark's message intact; a failed open marks the connection FAIL rather than leaving a half-attached handle; a second dbt run replaces in place, so a re-run after a partial failure is idempotent. No transactions exist, which the guide states rather than implying.
      artifacts: [python/dbt-repark/src/dbt/adapters/repark/connections.py, python/dbt-repark/tests/test_gold_models.py]
    - id: AT-4
      status: ATTACKED
      evidence: One shared session under a lock; per-statement cursors on the calling thread; 16 concurrent reads from 8 workers pinned; a second profile with a different key is refused rather than silently attached to the live engine.
      artifacts: [python/dbt-repark/src/dbt/adapters/repark/session.py, python/dbt-repark/tests/test_gold_models.py]
    - id: AT-5
      status: ATTACKED
      evidence: The Glue leg is skipped by default, writes only into testing_repark_acceptance, issues no DROP, and reads its ARN and warehouse from the environment. No credentials, no .github change, no Cargo change, no secret in any output. The adapter interpolates no bindings into SQL - it refuses them.
      artifacts: [python/dbt-repark/tests/test_aws_acceptance_gold.py, python/dbt-repark/src/dbt/adapters/repark/session.py]
    - id: AT-6
      status: ATTACKED
      evidence: The gold SQL is imported from the S6 program, never copied, so the two cannot drift. The built tables are asserted to be Iceberg at format-version 2 with the project's write properties, which is what a reader on Spark or Athena depends on.
      artifacts: [python/dbt-repark/tests/test_gold_models.py, python/repark/tests/_sql_harden_cutover_run.py]
    - id: AT-7
      status: N/A
      justification: No system-breaking resource behaviour. One session, one cursor per statement, results materialised once per cursor; the suite runs in about 30 seconds on the memory catalog.
    - id: AT-8
      status: ATTACKED
      evidence: dbt-spark 1.9.3 internals are relied on deliberately and named - create_table_as, the table materialization, SparkColumn, SparkConfig - with the version pinned in pyproject and the dependency chain (dependencies=["spark"]) verified against dbt's own materialization lookup. Every inherited macro that RePark refuses is overridden, so nothing is presumed. F-DBT-1-4 records the memory-catalog assumption.
      artifacts: [python/dbt-repark/pyproject.toml, python/dbt-repark/src/dbt/include/repark/macros/map.md]
    - id: AT-9
      status: ATTACKED
      evidence: Every refusal names its registry row id in the message, so a user reading a dbt failure lands on the document that owns the fact. F-DBT-1-3 files the one diagnostic this unit cannot fix from the adapter.
      artifacts: [python/dbt-repark/src/dbt/include/repark/macros/materializations.sql, docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Five mutations, each red of 45, in the table above; the red-first removal of the whole source tree; and F-DBT-1-1, which is the record of two mutations that reddened nothing until the suite was strengthened. M5's weakness is stated rather than hidden.
      artifacts: [task/ledgers/staging/dbt-1-adapter-ledger.md, python/dbt-repark/tests/test_gold_models.py]
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
