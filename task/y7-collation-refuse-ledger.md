# Unit ledger — Y-7 / G15: collation refuses loudly

**Unit:** Y-7 · G15 · V2 Engine Hardening · **Date:** 2026-08-12 ·
**Lane:** repark · **Executor:** Grok (grok-4.5) ·
**Worktree:** `/tmp/grok-y7` · **Branch:** `grok/y7-g15-collation-refuse` ·
**Base (FROZEN):** `a985edf7e22b68ea720cb2a8e08fca6cdd1a33b7`
(`docs(l1): land overnight §6 handoffs so STATUS and the registry match main (#65)`)

**Charter:** `BRIEF-y7-g15-collation-refuse.md` (approved). Conductor:
`BRIEF-overnight-conductor-4.md`. **Owner ruling 2026-08-12: refuse loudly**
(implementation stays a future decision). Bound addenda A4 / A5 / A10.

This unit does **not** implement collation semantics. It closes the silently-wrong-count
window: every live collation entry point either refuses with an actionable message or is
proven absent.

---

## 0. Inventory (base `a985edf`, both doors)

Probed at the frozen SHA (code + census + sqlparser AST + Spark 4.1.2 sources in
`~/.cache/repark-pyspark-tests/v4.1.2`). Live-Spark transcripts land in §0b when the
JVM lock is acquired (MARKER=`y7-g15-collation`).

| Entry point | Door | Pre-Y-7 behaviour | Class | After Y-7 |
|---|---|---|---|---|
| `SELECT expr COLLATE NAME` | Spark SQL + ANSI SQL | DataFusion `Unsupported ast node in sqltorel: Collate {…}` (census `TypesTests.test_collated_string` = `FAIL-MISSING`) | already loud, non-actionable | G15 `NotImplemented` at the executing parse |
| `ORDER BY col COLLATE NAME` | Spark SQL + ANSI SQL | same unsupported-AST path (compare/order-changing) | already loud, non-actionable | G15 refuse |
| `CREATE TABLE t (s STRING COLLATE NAME)` | Spark SQL + ANSI SQL | Spark door: generic `CREATE TABLE column option \`COLLATE …\` is not supported yet` (`create_table.rs` `ColumnOption` residual). ANSI: type maps via `CAST(NULL AS …)` and drops the collation | generic refuse / silent type strip | G15 refuse (column option walk) |
| `CAST(x AS STRING COLLATE NAME)` / `col.cast(StringType("NAME"))` | facade Column | `_engine_type()` always returns `"string"` — collation stripped | silent ignore | refuse at first evaluation (`_engine_type_from_cast_arg`) |
| `createDataFrame(..., StringType("UNICODE_CI"))` | facade | `_data_type_to_sql_type` emits `STRING`; distinct('Alice','alice') = **2**. Spark 4.1.2 `test_create_df_with_collation` expects **1** | **silently wrong-count** (the G15 gap) | refuse at first evaluation |
| `createDataFrame(..., "name STRING COLLATE UNICODE_CI")` | facade | `fromDDL` builds `StringType("UNICODE_CI")` then same strip | silently wrong-count | refuse |
| `F.collate` / `F.collation` | facade functions | names are **not** on `repark.functions` (`AttributeError`) | proven absent (A5) | documented, **not stubbed** |
| `Column.collate` | facade column | method does not exist (`AttributeError`) | proven absent | documented, **not stubbed** |
| SQL `collate()` / `collation()` functions | both doors | `datafusion-spark` 54.1.0 registers neither; DataFusion `Invalid function` | proven absent | no stub |
| `spark.tvf.collations()` | facade | `tvf` surface not shipped | proven absent | no stub |
| `spark.conf.set("spark.sql.collation.*", …)` / builder `.config` | facade session | `RuntimeConfig.set` / `Builder._set_config_entry` store any unknown key | **silent ignore** | refuse (key contains `collation`) |
| `SET spark.sql.collation.…` | Spark SQL door | passthrough stores/ignores | silent ignore | G15 refuse on `Set::SingleAssignment` |
| `StringType("UTF8_LCASE")` construction + `simpleString` | types | `string collate UTF8_LCASE` display; equality includes collation | **keep** (A5; pinned by `test_types_simple_string.py::test_string_type_collation`) | untouched |
| `StringType()` / `UTF8_BINARY` createDataFrame + `ORDER BY` without COLLATE | all | binary compare / Spark null-placement | **keep** | untouched (default-path pins) |

Spark 4.1.2 would have done (from Apache tests + `SQLConf.scala` + `pyspark.sql.functions`):

- `F.collate(col, "UNICODE")` marks the column; `F.collation(…)` returns
  `SYSTEM.BUILTIN.UNICODE` (`FunctionsTests.test_collation`).
- `createDataFrame([("Alice",), ("alice",)], StringType("UNICODE_CI")).distinct().count() == 1`
  (`DataFrameTests.test_create_df_with_collation`) — **this is the silently-wrong-count**.
- SQL `expr COLLATE name` and `ORDER BY col COLLATE name` apply that collation to
  compare/order.
- Session keys: `spark.sql.collation.allowInMapKeys`,
  `spark.sql.collation.objectLevel.enabled`,
  `spark.sql.collation.schemaLevel.enabled`,
  `spark.sql.collation.trim.enabled`,
  `spark.sql.legacy.collationAwareHashFunctions`,
  `spark.sql.runCollationTypeCastsBeforeAliasAssignment.enabled`.

`dataframe/core.py` was **not** touched (Y-5). `filter("… COLLATE …")` is guarded in
`repark-python` `filter_sql` (binding), not in `core.py`. No morning deferral.

---

## 0b. Live Spark probe (verbatim)

**Lock:** acquired 2026-08-12T19:31:35-04:00 after marker-verify of a stale Y-6 lock
(`MARKER=y6-g10-boundary`, pid `1528574` dead, start `18:55:47`, age >30 min, no extra
`pyspark`/`SparkSubmit` drivers besides the standing `spark-thrift` container). Wrote
`MARKER=y7-g15-collation`. Removal of the Y-6 lock is recorded here (only that stale
file; not a live holder).

**Oracle env:** PySpark **4.1.2**, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`,
`SPARK_LOCAL_IP=127.0.0.1`, `master("local[2]")`, `spark.sql.ansi.enabled=true`,
`spark.sql.shuffle.partitions=2`, `spark.ui.enabled=false`. 2026-08-12.

```
SPARK 4.1.2
=== SQL expr COLLATE UTF8_LCASE ===
OK [('Alice',)]
=== SQL ORDER BY COLLATE UTF8_LCASE ===
OK [('A',), ('a',), ('b',)]
=== SQL ORDER BY default ===
OK [('A',), ('a',), ('b',)]
=== create_df UNICODE_CI distinct ===
OK 1
=== create_df default distinct ===
OK 2
=== F.collate + F.collation ===
OK [('SYSTEM.BUILTIN.UNICODE',)]
=== SQL collate() function ===
OK [('SYSTEM.BUILTIN.UNICODE',)]
=== SET spark.sql.collation.objectLevel.enabled ===
OK (None, 'true')
=== CAST AS STRING COLLATE UTF8_LCASE ===
OK [('Alice',)]
DONE
```

Spark applies every spelling. Repark's silent-wrong-count is the `create_df UNICODE_CI
distinct` cell (Spark **1**, pre-Y-7 repark **2**). SQL `COLLATE` / `collate()` /
`F.collate` all succeed on Spark; repark now refuses the compare/order-changing ones
and documents `F.collate` as absent.

---

## 1. Guard decisions

| ID | Decision | Rationale |
|---|---|---|
| D-1 | Refuse, do not implement | Owner ruling 2026-08-12. Locale/ICU stays a future decision. |
| D-2 | Parse altitude (G3-E8 lesson) | Guard the parse every route agrees on: Spark `execute_passthrough` + the router's successful parse (intercepted CREATE/ALTER never reach passthrough). ANSI: immediately after the stock parse, before match. |
| D-3 | Duplicate the detector; do not hoist | Door→door product edges are banned. Same message text in both doors (needles identical). Binding calls the Spark-door `pub` helper. |
| D-4 | Construction stays; first evaluation refuses | A5. `StringType("UTF8_LCASE")` + `simpleString` untouched. `createDataFrame` / `cast` / SQL COLLATE refuse. |
| D-5 | No refusing stubs for missing functions | A5 ABSENCE IS LOUD. `F.collate` / `F.collation` / `Column.collate` already `AttributeError`. |
| D-6 | Session keys: any key containing `collation` | Covers the Spark 4.1.2 `spark.sql.collation.*` family and `legacy.collationAwareHashFunctions` without a hand-maintained list that would rot. |
| D-7 | `NotImplemented` → `UnsupportedOperationException` | Documented scope gate, not a parse error. Spark would have *run* the query. |
| D-8 | No `core.py`, no `cross_door.rs` | Single-owner files tonight (Y-5 / Y-10). Filter-string path guarded in the binding. |

---

## 2. Pins

### Spark door (`crates/repark-spark/src/tests/collation.rs`)

| Test | Class | Needle |
|---|---|---|
| `select_collate_expression_refuses` | `NotImplemented` | `does not implement collation` + `UTF8_LCASE` + `binary/default` |
| `select_collate_unicode_ci_refuses` | `NotImplemented` | `UNICODE_CI` |
| `order_by_collate_refuses` | `NotImplemented` | `UTF8_LCASE` |
| `create_table_column_collate_refuses` | `NotImplemented` | `UTF8_LCASE` |
| `set_collation_session_key_is_detected` | `NotImplemented` | `spark.sql.collation.objectLevel.enabled` |
| `collate_inside_string_literal_is_not_refused` | (success) | literal is not a request |
| `default_order_by_without_collate_is_untouched` | (success) | default path |

### ANSI door (`crates/repark-sql/src/guards/tests.rs`)

| Test | Class | Needle |
|---|---|---|
| `collation_valve_fires_on_expression_collate` | `NotImplemented` | `UTF8_LCASE` |
| `collation_valve_fires_on_order_by_collate` | `NotImplemented` | `UNICODE_CI` |
| `collation_valve_fires_on_create_table_column_collate` | `NotImplemented` | `UTF8_LCASE` |
| `collation_valve_ignores_collate_inside_a_literal` | (success) | — |
| `collation_valve_refuses_end_to_end_and_default_select_is_untouched` | e2e refuse + `SELECT 1` | `does not implement collation` |

### Facade (`python/repark/tests/test_collation_refuse.py`)

2–3 refusal pins per live entry point + default + construction + absence. Arrow path on the
default createDataFrame pin (`to_arrow` values).

---

## 3. Message (byte-identical, both doors + facade)

```
repark does not implement collation: requested `{requested}`. Spark 4 would apply that
collation to comparisons and ORDER BY; repark refuses rather than silently ignore it.
Use binary/default ordering — omit COLLATE, keep StringType() / UTF8_BINARY, and do not
set a session collation.
```

---

## 4. Files

| Path | Role |
|---|---|
| `crates/repark-spark/src/collation.rs` | Spark-door detector + `pub` binding helper |
| `crates/repark-spark/src/spark_ast.rs` | executing-parse call |
| `crates/repark-spark/src/router.rs` | intercepted-parse call |
| `crates/repark-spark/src/lib.rs` | `pub use` |
| `crates/repark-spark/src/tests/collation.rs` | Spark-door pins |
| `crates/repark-sql/src/guards.rs` | ANSI-door detector (re-implemented) |
| `crates/repark-sql/src/router.rs` | post-parse call |
| `crates/repark-sql/src/guards/tests.rs` | ANSI-door pins |
| `crates/repark-python/src/column.rs` | `F.expr` / `Column.sql` |
| `crates/repark-python/src/dataframe.rs` | `filter_sql` |
| `python/repark/src/repark/types.py` | evaluation + session-key helpers |
| `python/repark/src/repark/column.py` | cast/try_cast first evaluation |
| `python/repark/src/repark/session/_funcs.py` | createDataFrame schema mapping |
| `python/repark/src/repark/session/builder_conf.py` | `conf.set` |
| `python/repark/src/repark/session/session_core.py` | builder `.config` |
| `python/repark/tests/test_collation_refuse.py` | facade pins |
| maps + this ledger | lockstep |

Not touched: `dataframe/core.py` (Y-5), `cross_door.rs` (Y-10), `functions.py` (absence is
loud), `Cargo.lock`, registry / `_live_parity.py`.

---

## 5. Deviations

None that change the ruling. `filter(str)` is guarded in the binding rather than
`core.py` (would have been a morning deferral if the parse lived only there).

---

## 6. Registry rows — READY TO PASTE, **not** landed

**Do not edit `docs/spark-sql-iceberg-parity.md` from this unit.** The orchestrator lands
these after the PR merges.

---

- **repark** — string collation is **refused** at parse / first evaluation. SQL `expr COLLATE
  name`, `ORDER BY col COLLATE name`, `CREATE TABLE … (col STRING COLLATE name)`,
  `SET spark.sql.collation.*`, `createDataFrame` with a non-`UTF8_BINARY` `StringType`, and
  `Column.cast`/`try_cast` to a collated string all raise `UnsupportedOperationException`
  (`NotImplemented` on the Rust doors) naming the requested collation and steering to
  binary/default ordering. `StringType(collation=…)` construction and `simpleString` display
  stay (schema metadata). `F.collate` / `F.collation` / `Column.collate` are not on the
  facade (AttributeError).
- **Apache Spark** — Spark 4.0+ applies the named collation to comparisons and `ORDER BY`.
  `createDataFrame([("Alice",), ("alice",)], StringType("UNICODE_CI")).distinct().count()`
  is **1**. `F.collate` / `F.collation` return a collated column / its name
  (`SYSTEM.BUILTIN.UNICODE`). *(oracle: Apache 4.1.2 tests `test_create_df_with_collation`,
  `test_collation`; SQLConf keys from `v4.1.2` `SQLConf.scala`. Live probe recorded in this
  ledger §0b when the JVM lock is held.)*
- **Pin** — `python/repark/tests/test_collation_refuse.py::test_create_dataframe_unicode_ci_refuses`,
  `…::test_sql_collate_expression_refuses`,
  `…::test_sql_order_by_collate_refuses`,
  `…::test_cast_collated_string_type_refuses`,
  `…::test_conf_set_collation_key_refuses`,
  `crates/repark-spark/src/tests/collation.rs::select_collate_expression_refuses`,
  `…::order_by_collate_refuses`,
  `…::create_table_column_collate_refuses`,
  `crates/repark-sql/src/guards/tests.rs::collation_valve_fires_on_expression_collate`,
  `…::collation_valve_refuses_end_to_end_and_default_select_is_untouched`.
- **Rationale** — DEFECT, refused pending a future implement-or-keep-absent decision.
  **History:** G15 (MEDIUM) — "collation is unimplemented and silently wrong-count." The
  census row `pyspark.sql.tests.test_dataframe.DataFrameTests.test_create_df_with_collation`
  was `FAIL-VALUE` / `2 != 1`: repark accepted `StringType("UNICODE_CI")`, stripped it to
  binary `STRING`, and counted Alice/alice as two values. SQL `COLLATE` was already a raw
  DataFusion unsupported-AST error (`test_collated_string` = `FAIL-MISSING`), not an
  actionable refusal. **Ruling provenance:** owner 2026-08-12, conductor-4 A5 (scope =
  compare/order-changing paths; constructor + simpleString stay; refuse at first
  evaluation; ABSENCE IS LOUD) and A10 (parse-altitude refuse on `spark_ast.rs` + repark-sql
  guard sites; G3-E8 lesson). Keep this row until collation is implemented or the product
  permanently documents absence without a silent path.

---
