use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::LogicalPlan;
use iceberg::TableIdent;
use repark_iceberg::write::insert_defaults::table_reference_target;
use repark_iceberg::write::sink_offsets::{in_body_scope, refuse_planned_sink_write};

use crate::catalog_state::CatalogRegistry;

fn executed_dml_targets(plan: &LogicalPlan) -> Result<Vec<(String, TableIdent)>> {
    let mut targets = Vec::new();
    plan.apply(|node| match node {
        LogicalPlan::Explain(_) => Ok(TreeNodeRecursion::Jump),
        LogicalPlan::Dml(dml) => {
            targets.extend(table_reference_target(&dml.table_name));
            Ok(TreeNodeRecursion::Continue)
        }
        _ => Ok(TreeNodeRecursion::Continue),
    })?;
    Ok(targets)
}

pub(crate) async fn refuse_unstamped_sink_dml(
    plan: &LogicalPlan,
    catalogs: &CatalogRegistry,
) -> Result<()> {
    if !in_body_scope() {
        return Ok(());
    }
    for (catalog, ident) in executed_dml_targets(plan)? {
        let Some(handle) = catalogs.get(&catalog) else {
            continue;
        };
        let Ok(table) = handle.load_table(&ident).await else {
            continue;
        };
        if let Some(refusal) = refuse_planned_sink_write(&table) {
            return Err(DataFusionError::External(Box::new(refusal)));
        }
    }
    Ok(())
}
