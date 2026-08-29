//! WI-2 ANSI store-assignment gate for plain `INSERT` plans.
//!
//! DataFusion inserts synthesized casts in the DML projection. This rule reads the projection's
//! input schema and applies the shared Spark ANSI matrix before the fork writer runs. All plain
//! `INSERT` spellings converge on this node.
//!
//! # Cast boundary
//!
//! DataFusion's DML projection identifies synthesized casts as `Alias(Cast(Column, target))`;
//! this rule judges them against Spark's ANSI matrix. Explicit user casts are already conformed
//! inside the source plan and remain legal. Literal `VALUES` casts are indistinguishable from
//! explicit casts, so `Cast(Literal, …)` remains the documented policy residual.

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
