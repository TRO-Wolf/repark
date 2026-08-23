# Unit ledger — Y-3: `spark.catalog.getDatabase` + G-6 live-leg

**Unit:** Y-3 · **Date:** 2026-08-12 · **Lane:** Y-3 ·
**Branch:** `grok/y3-getdatabase` · **Executor:** Grok (grok-4.5) ·
**Freeze base:** `a985edf7e22b68ea720cb2a8e08fca6cdd1a33b7`

Charter: `planning/grok/BRIEF-y3-getdatabase.md` (conductor overnight-4; A2 bound).

---

## 1. What landed

| Artifact | Role |
|---|---|
| [`python/repark/src/repark/catalog.py`](../python/repark/src/repark/catalog.py) | `get_database` / `getDatabase`: real `locationUri` + `description`; existence is DESCRIBE (no SHOW precheck) |
| [`python/repark/tests/test_catalog_surface.py`](../../../../python/repark/tests/test_catalog_surface.py) | value/shape, bare+qualified, SCHEMA_NOT_FOUND, FA-2 still None |
| [`python/repark/tests/_acceptance.py`](../../../../python/repark/tests/_acceptance.py) | G-6 Glue caller switched to `catalog.getDatabase` |
| [`python/repark/tests/test_acceptance_helpers.py`](../../../../python/repark/tests/test_acceptance_helpers.py) | stub + AST pin that the wrapper calls `getDatabase` |
| [`DEVELOPMENT.md`](../../../../DEVELOPMENT.md) | authorized ridealong: `preflight` matches AGENTS roster (pointer) |
| this ledger | linked from [`task/map.md`](../../../map.md) |

**No new engine method.** `getDatabase` follows the existing SQL catalog route
(`DESCRIBE NAMESPACE` — already `get_namespace` + `resolve_namespace_location`).
X-4 `NamespaceScopedCatalog.get_namespace` was already an explicit forward; iceberg
and `repark-python` needed no new surface. **FA-2 untouched** (`listDatabases`
still `locationUri=None`). Zero AWS.

---

## 2. §0 — locate before building

### 2.1 Catalog facade route

- `python/repark/src/repark/session/catalog.py` is a **re-export binding** marker
  (r27 T1). Methods live on `Catalog` in `python/repark/src/repark/catalog.py`.
- `listDatabases` / `databaseExists` → `SHOW NAMESPACES IN <catalog>` (SQL).
- `tableExists` → native `inner.table_exists`.
- `getDatabase` needs location, which SHOW does not carry. The existing sibling
  that does is `DESCRIBE NAMESPACE` (`execute_describe_namespace` →
  `catalog.get_namespace` + `resolve_namespace_location`). That is the engine
  route, not a new one.

### 2.2 G-6 location-guard + what "activation" means

G-6 (archived [`docs/history/hardening-h1/g6-chores-ledger.md`](../../../../docs/history/hardening-h1/g6-chores-ledger.md)
item 3) wired a **harness-local** DESCRIBE probe because `listDatabases` is FA-2
(`locationUri=None`). The helper
`probe_namespace_location_via_describe` docstring named `getDatabase` as its
**retirement condition**.

**Activation (concrete):**

1. `assert_glue_scratch_namespace_location` (the only production caller) now
   reads `spark.catalog.getDatabase(f"{SILVER_CATALOG}.{ACCEPTANCE_NAMESPACE}").locationUri`.
2. The env-gated Glue AWS test (`test_aws_acceptance.py`, skipped unless
   `REPARK_AWS_ACCEPTANCE=1`) therefore exercises the **public** API when it
   runs — that is the dormant live-leg. This unit does **not** turn the AWS
   gate on.
3. AWS-free unit tests pin the switch (stub `getDatabase` + AST "calls
   getDatabase, not sql/DESCRIBE").
4. The DESCRIBE helper stays as a unit-test extractor only.

### 2.3 Other catalog gaps (not built)

`getTable` / `getFunction` / `listFunctions` / `listColumns` / `createTable` /
`refreshTable` / `cacheTable` — still absent. Ledger only.

### 2.4 Live PySpark 4.1.2 oracle

Source (not memory): `org.apache.spark.sql.classic.Catalog.makeDatabase`
(v4.1.2) builds `Database(name=namespace.quoted, catalog=catalog.name,
description=metadata.get(PROP_COMMENT).orNull,
locationUri=metadata.get(PROP_LOCATION).orNull)`.
`getDatabase(dbName)` → `makeDatabase(None, dbName)` with
`resolveNamespace` (session-catalog hit → `spark_catalog`+name, else
`parseIdent` so `spark_catalog.default` works).

Error class from `error-conditions.json` in spark-common-utils 4.1.2:
`SCHEMA_NOT_FOUND` / SQLSTATE `42704` /
"The schema <schemaName> cannot be found. Verify the spelling and
correctness of the schema and catalog."

**Live probe transcript** (JVM lock, recorded below in §3).

---

## 3. Live probe transcript

Lock protocol: FIFO `/tmp/grok-jvm-record.lock`.

**Wait:** on arrival the lock was `MARKER=y6-g10-boundary` `pid=1528574`
`start=2026-08-12T18:55:47-04:00`. Holder pid died; a Y-6 `pyspark-shell`
(`appName=y6-followup`) ran then exited. **Did not steal.** Y-6 released
the file (~23:26Z). **No stale-lock removal by this lane.**

**Acquire:** 2026-08-12T23:26:43Z `MARKER=y3-getdatabase` `pid=1938134`
`lane=Y-3`. Standing `HiveThriftServer2` ignored. Java 17
(`/usr/lib/jvm/zulu-17-amd64`, Zulu 17.0.15). `pyspark==4.1.2`.
`local[2]`, ANSI on, `spark.sql.shuffle.partitions=2`, UI off,
`SPARK_LOCAL_IP=127.0.0.1`.

**Release:** marker-verified own lock, then `rm`. No leftover local
driver. Removed accidental `/tmp/grok-y3/spark-warehouse` (not committed).

### 3.1 Session catalog (verbatim)

```
pyspark 4.1.2
Database fields ('name', 'catalog', 'description', 'locationUri')

getDatabase('default')
  Database(name='default', catalog='spark_catalog',
           description='default database',
           locationUri='file:/tmp/grok-y3/spark-warehouse')

getDatabase('spark_catalog.default')  — identical to bare

getDatabase('y3_probe_db') after CREATE DATABASE … COMMENT 'y3 comment'
  Database(name='y3_probe_db', catalog='spark_catalog',
           description='y3 comment',
           locationUri='file:/tmp/grok-y3/spark-warehouse/y3_probe_db.db')

getDatabase('spark_catalog.y3_probe_db')  — identical to bare

getDatabase('y3_missing_ns') and getDatabase('spark_catalog.y3_missing_ns')
  type=AnalysisException
  getCondition()='SCHEMA_NOT_FOUND'
  getErrorClass()='SCHEMA_NOT_FOUND'
  getSqlState()='42704'
  getMessageParameters()={'schemaName': '`spark_catalog`.`y3_missing_ns`'}
  str=[SCHEMA_NOT_FOUND] The schema `spark_catalog`.`y3_missing_ns` cannot be found. Verify the spelling and correctness of the schema and catalog.
  (+ two Spark extra sentences: current_schema() hint + DROP SCHEMA IF EXISTS)
  SQLSTATE: 42704

getDatabase('no_such_catalog.y3_missing_ns')
  SCHEMA_NOT_FOUND
  schemaName='`spark_catalog`.`no_such_catalog`.`y3_missing_ns`'

getDatabase(None)
  Py4JJavaError / JVM NPE (String.toLowerCase) — Spark has no type guard.
  Repark raises PySparkTypeError(dbName) like the rest of this Catalog
  surface (deliberate, consistent; not a silent absorb).
```

### 3.2 Iceberg Hadoop catalog (GAV `iceberg-spark-runtime-4.1_2.13:1.11.0`)

`CREATE NAMESPACE … LOCATION` → Iceberg Hadoop
`UnsupportedOperationException: Cannot create namespace …: metadata is
not supported`. `DBPROPERTIES ('location'=…)` → Spark parse
`UNSUPPORTED_FEATURE.SET_NAMESPACE_PROPERTY` (reserved; use LOCATION).

Bare `CREATE NAMESPACE local.bare` (no LOCATION):

```
getDatabase('bare') == getDatabase('local.bare')
  Database(name='bare', catalog='local', description=None,
           locationUri='/tmp/y3-iceberg-wh-…/bare')   # synthesized warehouse/ns

missing bare/qualified
  SCHEMA_NOT_FOUND
  getMessageParameters()={'schemaName': '`y3_ice_missing`'}  # namespace only

DESCRIBE NAMESPACE local.bare
  ('Catalog Name', 'local'), ('Namespace Name', 'bare'),
  ('Location', '/tmp/y3-iceberg-wh-…/bare')

listDatabases() on Iceberg Hadoop fills locationUri (Spark). Repark
listDatabases stays FA-2 None — untouched.
```

**Contract used:** return type `Database(name, catalog, description,
locationUri)`; missing → `AnalysisException` + `SCHEMA_NOT_FOUND`;
bare and `catalog.db` agree; `locationUri` is the namespace location
when stored (repark memory `CREATE … LOCATION` / Glue). A
property-less repark memory namespace has `locationUri is None`
(Iceberg Hadoop synthesizes `warehouse/ns` — §6 residual, not FA-2).

---

## 4. Tests

| Test | Claim |
|---|---|
| `test_get_database_bare_and_qualified_shape` | field shape; bare == `catalog.db` == `spark_catalog.db` alias; no-location → None |
| `test_get_database_returns_location_and_comment` | COMMENT + LOCATION filled; **listDatabases still None (FA-2)** |
| `test_get_database_missing_raises_schema_not_found` | `AnalysisException` + `[SCHEMA_NOT_FOUND]` **equals DESCRIBE sibling** (bare + qualified) |
| `test_get_database_does_not_show_precheck` | AST: `get_database` calls DESCRIBE, not `_namespace_exists` |
| `test_get_database_location_uri_matches_describe_probe` | `getDatabase.locationUri` == `probe_namespace_location_via_describe` |
| public surface + non-str `dbName` | `getDatabase`/`get_database` in `dir(Catalog)`; `PySparkTypeError` |
| `test_assert_glue_scratch_namespace_location_composes_getdatabase_and_compare` | Glue wrapper reads `getDatabase.locationUri` |
| `test_glue_location_guard_calls_get_database` | AST: wrapper calls `getDatabase`, not DESCRIBE/`sql` |
| existing DESCRIBE helper stub | still extracts Location (helper retired as live path only) |
| `test_list_databases_location_uri_none_divergence` | FA-2 pin **untouched** |

Memory catalog only.

---

## 5. Gates

| Gate | Result | Log |
|---|---|---|
| `make verify` | **0** | `/tmp/y3-verify.log` (actor); `/tmp/y3-fix-verify.log` (cycle-1) |
| `make preflight` | **0** | `/tmp/y3-preflight.log` (actor; facade 2826/71); `/tmp/y3-fix-preflight.log` (cycle-1; facade **2828 passed**, 71 skipped) |

---

## 6. Registry-shaped findings (paste-true; do **not** land here)

None that change an existing row. FA-2 stays live (`listDatabases`
`description`/`locationUri` remain `None`). `getDatabase` is the public
read that FA-2's follow-on named.

Optional future row (not this unit): a LOCATION-less memory-catalog
namespace has `getDatabase.locationUri is None`. Spark's session catalog
`default` always has a `file:` warehouse URI; Iceberg Hadoop catalogs
often synthesize `warehouse/ns`. Repark memory `CREATE NAMESPACE` without
`LOCATION` stores no property (DESCRIBE omits Location). Honest; G-6's
Glue path always creates with `location=`.

---

## 7. Conductor A11 freeze

Base freeze `a985edf7`. No registry / `_live_parity` / lock / AWS /
`.github` / `Cargo.lock` / `uv.lock` edits. `DEVELOPMENT.md` one-line
ridealong is A11-whitelisted.

---

## 8. Cycle-1 ACC remediations (OPEN queue only)

| ID | Sev | Action |
|---|---|---|
| Q-001 | S1 | Deleted SHOW `_namespace_exists` precheck from `get_database`. Existence is DESCRIBE. Missing-ns pin equals DESCRIBE exception text; AST forbids `_namespace_exists` on `get_database`. |
| SEC-001 | S2 | Same swallow — fixed by deleting the precheck (catalog/IO errors now propagate from DESCRIBE). |
| Q-002 | S2 | `test_get_database_location_uri_matches_describe_probe`: `getDatabase.locationUri` == `probe_namespace_location_via_describe` on one memory-catalog session. |
| CL-002 | S3 | `test_catalog_surface.py` module docstring: not SHOW + `information_schema` only. |

Not in this queue (left OPEN): SEC-002 (E1 `getCondition`), SAF-001 (unmatched-quote `RuntimeError`), SEC-003 (keyword/`EXTENDED` quote-if-needed), CL-001 (workspace brief path). FA-2 `listDatabases` `locationUri=None` untouched. Zero AWS.
