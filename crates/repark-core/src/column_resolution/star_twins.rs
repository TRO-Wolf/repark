use std::collections::{HashMap, HashSet};

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, LogicalPlan};

use super::Twins;

#[allow(clippy::missing_errors_doc)]
pub(super) fn refuse_star_twins(
    scope: &LogicalPlan,
    exprs: &[Expr],
    twins: &Twins<'_>,
) -> Result<()> {
    let keys = scan_twin_keys(scope);
    for expr in exprs {
        let Expr::Column(column) = expr else {
            continue;
        };
        let Some(relation) = column.relation.as_ref() else {
            continue;
        };
        if crate::frame_names::is_scratch_relation(relation.table()) {
            continue;
        }
        let Some(sharing) = twins.get(&column.name.to_ascii_lowercase()) else {
            continue;
        };
        if sharing.iter().any(|(candidate, name)| {
            candidate.is_some_and(|owner| *owner == *relation) && *name != column.name.as_str()
        }) && keys.contains(column.name.to_ascii_lowercase().as_str())
        {
            let name = column.name.as_str();
            return Err(DataFusionError::Plan(format!(
                "[COLUMN_ALREADY_EXISTS] The column `{name}` already exists. Choose another name or rename the existing column. SQLSTATE: 42711"
            )));
        }
    }
    Ok(())
}

fn scan_twin_keys(plan: &LogicalPlan) -> HashSet<String> {
    let mut keys = HashSet::new();
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && !crate::frame_names::is_scratch_relation(scan.table_name.table())
        {
            let mut seen: HashMap<String, &str> = HashMap::new();
            for (_, field) in scan.projected_schema.iter() {
                let name = field.name().as_str();
                let folded = name.to_ascii_lowercase();
                match seen.get(&folded) {
                    Some(first) if *first != name => {
                        keys.insert(folded);
                    }
                    None => {
                        seen.insert(folded, name);
                    }
                    _ => {}
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    keys
}
