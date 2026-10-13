# H2 probe: does SQL `TIMESTAMP` reach `SessionStateBuilder::with_type_planner`

Probe branch: `probe/type-planner-h2`, commit
`42ed8b556068e9bfa1b41f5d4b2b84368e41904b` (never merged, never pushed).
The probe registers a `TypePlanner` that maps every SQL `TIMESTAMP` spelling to
a marker field `probe_marker` of empty-`Struct` type, which no cast accepts, at
both builders named in sketch section 2. A release module built from the probe
branch ran each spelling below; the unit branch is unchanged.

## Probe diff

```diff
diff --git a/crates/repark-core/src/session/df_guards.rs b/crates/repark-core/src/session/df_guards.rs
index 3324cce4..b2638824 100644
--- a/crates/repark-core/src/session/df_guards.rs
+++ b/crates/repark-core/src/session/df_guards.rs
@@ -62,6 +62,29 @@ pub(super) fn apply_df_54_1_config_guards(config: &mut SessionConfig) {
         .enable_physical_uncorrelated_scalar_subquery = false;
 }

+#[derive(Debug)]
+pub(super) struct ProbeTimestampMarker;
+
+impl datafusion::logical_expr::planner::TypePlanner for ProbeTimestampMarker {
+    fn plan_type_field(
+        &self,
+        sql_type: &datafusion::sql::sqlparser::ast::DataType,
+    ) -> datafusion::common::Result<Option<datafusion::arrow::datatypes::FieldRef>> {
+        match sql_type {
+            datafusion::sql::sqlparser::ast::DataType::Timestamp(_, _) => {
+                Ok(Some(Arc::new(datafusion::arrow::datatypes::Field::new(
+                    "probe_marker",
+                    datafusion::arrow::datatypes::DataType::Struct(
+                        datafusion::arrow::datatypes::Fields::empty(),
+                    ),
+                    true,
+                ))))
+            }
+            _ => Ok(None),
+        }
+    }
+}
+
 /// The session context, built with DF 54.1 regression guard 2 of 2 installed.
 pub(super) fn context_with_df_54_1_rule_guards(
     config: SessionConfig,
@@ -71,6 +94,7 @@ pub(super) fn context_with_df_54_1_rule_guards(
     let analyzer_rules =
         extension.configure_analyzer_rules(analyzer_rules_with_df_54_1_rule_guards())?;
     let state = SessionStateBuilder::new()
+        .with_type_planner(Arc::new(ProbeTimestampMarker))
         .with_config(config)
         .with_runtime_env(runtime)
         .with_default_features()
diff --git a/crates/repark-python/src/column/expr_build.rs b/crates/repark-python/src/column/expr_build.rs
index cf598f66..91a71c83 100644
--- a/crates/repark-python/src/column/expr_build.rs
+++ b/crates/repark-python/src/column/expr_build.rs
@@ -272,6 +272,29 @@ pub(super) fn sql_context(
     Ok(context)
 }

+#[derive(Debug)]
+struct ProbeTimestampMarker;
+
+impl datafusion::logical_expr::planner::TypePlanner for ProbeTimestampMarker {
+    fn plan_type_field(
+        &self,
+        sql_type: &datafusion::sql::sqlparser::ast::DataType,
+    ) -> datafusion::common::Result<Option<datafusion::arrow::datatypes::FieldRef>> {
+        match sql_type {
+            datafusion::sql::sqlparser::ast::DataType::Timestamp(_, _) => {
+                Ok(Some(Arc::new(datafusion::arrow::datatypes::Field::new(
+                    "probe_marker",
+                    datafusion::arrow::datatypes::DataType::Struct(
+                        datafusion::arrow::datatypes::Fields::empty(),
+                    ),
+                    true,
+                ))))
+            }
+            _ => Ok(None),
+        }
+    }
+}
+
 fn build_expr_context() -> datafusion::error::Result<SessionContext> {
     let mut config = SessionConfig::new();
     config.options_mut().sql_parser.dialect = datafusion::config::Dialect::Databricks;
@@ -284,6 +307,7 @@ fn build_expr_context() -> datafusion::error::Result<SessionContext> {
     rules.push(std::sync::Arc::new(repark_spark::SparkProjectionDisplay));
     rules.extend(repark_spark::spark_literal_typing::spark_door_post_coercion_rules());
     let state = SessionStateBuilder::new()
+        .with_type_planner(Arc::new(ProbeTimestampMarker))
         .with_config(config)
         .with_default_features()
         .with_analyzer_rules(rules)
```

## Outcomes (release module from the probe branch, separate venv)

REACHES = the statement failed on the marker (`Unsupported CAST from … to
Struct()`); DOES NOT REACH = it answered or wrote a real timestamp type.

| # | Spelling | Outcome | Detail |
|---|---|---|---|
| 1 | `spark.sql("SELECT CAST(1 AS TIMESTAMP)")` | REACHES | `Unsupported CAST from Int32 to Struct()` |
| 2 | `spark.sql("SELECT TIMESTAMP '2024-01-01 00:00:00'")` | REACHES | `Unsupported CAST from Utf8 to Struct()` |
| 3 | `df.selectExpr("CAST(x AS TIMESTAMP)")` | REACHES | `Unsupported CAST from Int32 to Struct()` |
| 4 | `F.expr("CAST(x AS TIMESTAMP)")` | REACHES | `Unsupported CAST from Int32 to Struct()` |
| 5 | `F.col("x").cast("timestamp")` | DOES NOT REACH | answers `timestamp[us, tz=UTC]`; the DataFrame type-name map resolves it, never SQL planning |
| 6 | `CREATE TABLE mem.ns.h2 (c TIMESTAMP) USING iceberg` | DOES NOT REACH | table created; a follow-up read shows column `c` is `timestamp[us, tz=UTC]`, so DDL resolves the name through the catalog mapping, never the SQL planner |
| 7 | ANSI door `repark.sql("SELECT CAST(1 AS TIMESTAMP)")` | REACHES | `Unsupported CAST from Int64 to Struct()` |
| 8 | ANSI door `repark.sql("SELECT TIMESTAMP '2024-01-01 00:00:00'")` | REACHES | `Unsupported CAST from Utf8 to Struct()` |

## Reading for H2

Every SQL-expression spelling on both doors reaches the registered planner.
The DDL column-type spelling does not: it resolves through the catalog type
mapping. By the brief's letter H2 is MET by row 6; the expression path the
sketch's planner design needs (rows 1-4, 7-8) is proven present at both
builders. The orchestrator and the owner rule whether row 6 halts the unit:
DDL was never claimed to plan through `with_type_planner` (sketch section 2
names only the CAST and typed-literal paths).
