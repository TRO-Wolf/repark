//! WI-2 — the ANSI store-assignment gate on the **plain-INSERT** doors, as an `AnalyzerRule`.
//!
//! **What WI-1 could not reach.** #142 gave `INSERT OVERWRITE` and the public `append` entry
//! point the shared matrix in [`super::store_assign`], but the four plain-INSERT doors
//! (`INSERT INTO … SELECT`, `INSERT INTO … VALUES`, `writeTo().append()`,
//! `write.insertInto()`) all lower to the same statement, and DataFusion's `insert_to_plan`
//! (`datafusion-sql-54.1.0/src/statement.rs:2470-2480`) injects the conforming `CAST` itself at
//! **SQL-planning** time:
//!
//! ```text
//! Expr::Column(Column::from(source.schema().qualified_field(v)))
//!     .cast_to(target_field.data_type(), source.schema())?
//! ```
//!
//! By the time the fork's `IcebergTableProvider::insert_into` is called the input plan's schema is
//! already the TABLE schema — the `Date32` is gone and `18262` is baked in. No seam under
//! `crates/repark-iceberg/src/write/` is on that path (`write/map.md` has said so since v1), and
//! a `TableProvider` decorator is therefore the wrong seam however natural it looks
//! (`task/wi1-insert-store-gate-ledger.md` §4).
//!
//! **The seam.** One stage earlier: a `LogicalPlan::Dml(WriteOp::Insert(_))` node still carries
//! the synthesized `Projection`, and that projection's INPUT schema still carries the pre-cast
//! source types. This rule reads exactly that and runs the SAME matrix — imported, never
//! duplicated. It is door-agnostic within the Spark facade: every `INSERT` spelling converges on
//! this one node, so `VALUES`, `SELECT`, `writeTo().append()` and `write.insertInto()` are one
//! call site, not four.
//!
//! # Synthesized vs. explicit casts — the correctness constraint
//!
//! Spark's rule is that an **explicit** `CAST` written by the user is the user's stated intent and
//! is legal even where store assignment would refuse (`INSERT INTO t SELECT CAST(b AS INT)` with
//! `b BOOLEAN` is accepted; the bare `INSERT INTO t SELECT b` is not). So this rule must judge
//! ONLY the casts DataFusion synthesized.
//!
//! What makes that decidable: `insert_to_plan` builds the DML projection with
//! `project(source, exprs)` — a **new** `Projection` stacked on top of the fully planned source.
//! Its expressions are therefore always `Alias(Column | Cast(Column, target) | Cast(Literal, …))`
//! and never anything the user wrote; a user's `CAST` lives inside the source plan and reaches
//! this projection already conformed, at which point `cast_to` is a no-op and the expression is a
//! bare `Expr::Column`. Judging exactly `Alias(Cast(Column(c), target))` — where the pre-cast type
//! is READABLE off the projection's input schema — is therefore the provable subset:
//!
//! * a synthesized conform of a source column → gated;
//! * a user-written explicit `CAST` → arrives as a plain column, invisible to this rule.
//!
//! # The named residual: `INSERT INTO … VALUES` literal rows
//!
//! `insert_to_plan` hands the target schema to the VALUES planner
//! (`PlannerContext::set_table_schema`), and `LogicalPlanBuilder::infer_inner` then rewrites each
//! literal as `row[j].cast_to(field_type, schema)` **inside the `Values` node**. A user-written
//! `INSERT INTO t VALUES (CAST(x AS INT))` produces the byte-identical node, because the outer
//! `cast_to` is a no-op once the inner cast already yielded the target type. The two are
//! indistinguishable in the plan, so this rule does **not** judge `Cast(Literal, …)`: gating it
//! would refuse a legal explicit cast, which is the one failure mode worse than the gap.
//!
//! That residual is narrower than it looks. The `DATE ↔ INT` half of it — the pairs that carried a
//! silently-wrong VALUE rather than merely a laxer policy — is closed anyway by the CAST-legality
//! gate (`repark_functions::analyzer`'s G6-3 / G6-5 rows), which refuses the type pair wherever the
//! cast appears, `Values` node included. What stays open is a literal `VALUES` row whose pair is
//! cast-legal but not store-assignable (`VALUES (true)` into an `INT` column,
//! `VALUES (TIMESTAMP '…')` into a `BIGINT` column). Those write a defined, non-reinterpreted
//! value; they are a policy gap, not a corruption.

use datafusion::common::ExprSchema;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::Result;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, LogicalPlan, WriteOp};
use datafusion::optimizer::AnalyzerRule;

use super::store_assign::refuse_unless_write_store_assignable;

/// ===========================================================================================
/// The rule. Stateless and read-only — it rewrites nothing, it only refuses.
///
/// Register it **before** `repark_functions::analyzer_rules()` so a DML insert whose pair is on
/// BOTH matrices (`DATE → INT`) cites Spark's write class (`INCOMPATIBLE_DATA_FOR_TABLE`), which
/// is what Spark raises for that statement, rather than the CAST class.
/// ===========================================================================================
#[derive(Debug, Default, Clone, Copy)]
pub struct InsertStoreAssignment;

impl AnalyzerRule for InsertStoreAssignment {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.apply_with_subqueries(&mut |node: &LogicalPlan| {
            refuse_unassignable_insert(node)?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(plan)
    }

    #[allow(clippy::unnecessary_literal_bound)] // `AnalyzerRule::name` ties the lifetime to &self
    fn name(&self) -> &str {
        "repark_insert_store_assignment"
    }
}

/// The path label the refusal opens with — the statement the caller actually wrote.
const fn insert_label(op: InsertOp) -> &'static str {
    match op {
        InsertOp::Append => "INSERT INTO",
        InsertOp::Overwrite => "INSERT OVERWRITE",
        InsertOp::Replace => "REPLACE INTO",
    }
}

/// Check one plan node. Everything that is not an `INSERT` DML over a synthesized projection is
/// a no-op, so the rule is invisible to every other plan shape.
///
/// # Errors
/// [`datafusion::error::DataFusionError::Plan`] from
/// [`refuse_unless_write_store_assignable`] when a synthesized conform-cast's SOURCE type is not
/// ANSI-store-assignable to the target column's type.
fn refuse_unassignable_insert(plan: &LogicalPlan) -> Result<()> {
    let LogicalPlan::Dml(dml) = plan else {
        return Ok(());
    };
    let WriteOp::Insert(insert_op) = dml.op else {
        return Ok(());
    };
    // `insert_to_plan` always stacks its conform projection on top of the planned source. Any
    // other input shape is not a plan this rule can read pre-cast types out of, and is left alone.
    let LogicalPlan::Projection(projection) = dml.input.as_ref() else {
        return Ok(());
    };
    let label = insert_label(insert_op);
    let source_schema = projection.input.schema();
    for expr in &projection.expr {
        // The target column name is the alias `insert_to_plan` attached (`target_field.name()`).
        let (target_column, inner) = match expr {
            Expr::Alias(alias) => (alias.name.clone(), alias.expr.as_ref()),
            other => (other.schema_name().to_string(), other),
        };
        let Expr::Cast(cast) = inner else {
            continue;
        };
        // ONLY a cast over a bare source COLUMN is provably synthesized (module docs). A cast
        // over a literal is the `VALUES` residual; a cast over anything else is not a shape
        // `insert_to_plan` produces at all.
        let Expr::Column(column) = cast.expr.as_ref() else {
            continue;
        };
        let Ok(source_type) = source_schema.data_type(column) else {
            continue;
        };
        refuse_unless_write_store_assignable(
            label,
            &target_column,
            source_type,
            cast.field.data_type(),
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    use super::InsertStoreAssignment;

    /// A context with the rule installed and two empty tables: `t` (the target) and `s` (the
    /// source). Column names are `k` and `v` on both sides.
    fn ctx(target: DataType, source: DataType) -> SessionContext {
        let ctx = SessionContext::new();
        ctx.add_analyzer_rule(Arc::new(InsertStoreAssignment));
        for (name, value_type) in [("t", target), ("s", source)] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int32, true),
                Field::new("v", value_type, true),
            ]));
            let table = MemTable::try_new(Arc::clone(&schema), vec![vec![]]).unwrap();
            ctx.register_table(name, Arc::new(table)).unwrap();
        }
        ctx
    }

    /// Plan AND execute — the analyzer runs at physical planning, so a rule that only fires on
    /// `collect` and one that fires on `sql` are both caught here.
    async fn run(ctx: &SessionContext, sql: &str) -> Result<(), String> {
        match ctx.sql(sql).await {
            Err(error) => Err(error.to_string()),
            Ok(frame) => frame.collect().await.map(|_| ()).map_err(|e| e.to_string()),
        }
    }

    /// The WI-2 headline: the plain `INSERT INTO … SELECT` door that WI-1 could not reach.
    #[tokio::test]
    async fn insert_into_select_refuses_a_non_store_assignable_source_column() {
        let ctx = ctx(DataType::Int32, DataType::Date32);
        let message = run(&ctx, "INSERT INTO t SELECT k, v FROM s")
            .await
            .expect_err("date -> int must refuse");
        assert!(
            message.contains("INSERT INTO cannot store-assign column `v`"),
            "{message}"
        );
        assert!(message.contains("not ANSI-store-assignable"), "{message}");
        assert!(message.contains("Date32"), "{message}");
        assert!(message.contains("Int32"), "{message}");
        assert!(
            message.contains("INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST"),
            "{message}"
        );
    }

    /// The other measured store-assignment refusals reach the same node.
    #[tokio::test]
    async fn the_rest_of_the_matrix_refuses_on_the_plain_insert_door() {
        for (target, source) in [
            (DataType::Int32, DataType::Boolean),
            (
                DataType::Int64,
                DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None),
            ),
            (DataType::Int64, DataType::Utf8),
        ] {
            let ctx = ctx(target.clone(), source.clone());
            let message = run(&ctx, "INSERT INTO t SELECT k, v FROM s")
                .await
                .expect_err(&format!("{source:?} -> {target:?} must refuse"));
            assert!(
                message.contains("not ANSI-store-assignable"),
                "{source:?} -> {target:?}: {message}"
            );
        }
    }

    /// **The correctness constraint.** A user-written explicit `CAST` is the user's stated
    /// intent and is legal Spark even where store assignment refuses — and it must not be
    /// mistaken for the conform-cast DataFusion synthesizes.
    #[tokio::test]
    async fn an_explicit_user_cast_in_the_select_is_not_gated() {
        let ctx = ctx(DataType::Int32, DataType::Boolean);
        run(&ctx, "INSERT INTO t SELECT k, CAST(v AS INT) FROM s")
            .await
            .expect("an explicit CAST is the user's intent and must pass");
    }

    /// Positive controls: the gate must not narrow a legal write.
    #[tokio::test]
    async fn store_assignable_pairs_still_insert() {
        for (target, source) in [
            (DataType::Int64, DataType::Int32),   // widening
            (DataType::Int32, DataType::Int64),   // narrowing (runtime overflow, not analysis)
            (DataType::Utf8, DataType::Date32),   // atomic -> string
            (DataType::Date32, DataType::Date32), // identity
        ] {
            let ctx = ctx(target.clone(), source.clone());
            run(&ctx, "INSERT INTO t SELECT k, v FROM s")
                .await
                .unwrap_or_else(|error| panic!("{source:?} -> {target:?} must pass: {error}"));
        }
    }

    /// The NAMED residual, pinned so it cannot rot into a surprise: a literal `VALUES` row is
    /// conformed inside the `Values` node, where a synthesized cast and a user-written one are
    /// byte-identical. This rule deliberately does not judge it.
    #[tokio::test]
    async fn a_literal_values_row_is_the_named_residual_and_is_not_gated() {
        let ctx = ctx(DataType::Int32, DataType::Boolean);
        run(&ctx, "INSERT INTO t VALUES (1, true)")
            .await
            .expect("documented residual: Cast(Literal, ...) is not judged");
    }

    /// Nothing else in the plan tree is touched — a SELECT with the same pair still answers.
    #[tokio::test]
    async fn a_plain_select_is_invisible_to_the_rule() {
        let ctx = ctx(DataType::Int32, DataType::Date32);
        run(&ctx, "SELECT CAST(v AS INT) FROM s")
            .await
            .expect("the rule only reads Dml(Insert) nodes");
    }
}
