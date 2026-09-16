use datafusion::catalog::default_table_source::source_as_provider;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{Expr, LogicalPlan, TableScan};

use super::METADATA_COLUMN_NAME;
use super::scan::{FileKind, FileMetadataScan};
use super::udf::{METADATA_UDF_NAME, METADATA_UDF_NAME_NO_ROW_INDEX};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileMetadataStatus {
    Absent,
    Available,
    Shadowed,
}

#[derive(Debug)]
pub(crate) enum WalkOutcome {
    DeadAttributeLoss,
    DeadOther,
    Found { scan: TableScan, kind: FileKind },
    Realized { with_row_index: bool },
}

pub(crate) fn marker_kind(scan: &TableScan) -> Option<FileKind> {
    let provider = source_as_provider(&scan.source).ok()?;
    let marker = provider.as_ref().downcast_ref::<FileMetadataScan>()?;
    Some(marker.kind().clone())
}

fn augmentation_fingerprint(plan: &LogicalPlan) -> Option<bool> {
    let mut found = None;
    let _ = plan.apply(|node| {
        if matches!(node, LogicalPlan::Join(_) | LogicalPlan::Aggregate(_)) {
            return Ok(TreeNodeRecursion::Stop);
        }
        for expr in node.expressions() {
            let _ = expr.apply(|leaf| {
                if let Expr::ScalarFunction(function) = leaf
                    && (function.name() == METADATA_UDF_NAME
                        || function.name() == METADATA_UDF_NAME_NO_ROW_INDEX)
                {
                    found = Some(function.args.len() > 6);
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            });
            if found.is_some() {
                break;
            }
        }
        if found.is_some() {
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    });
    found
}

pub(crate) fn walk_plan(plan: &LogicalPlan) -> WalkOutcome {
    if matches!(plan, LogicalPlan::Join(_) | LogicalPlan::Aggregate(_)) {
        return WalkOutcome::DeadAttributeLoss;
    }
    if let Some(with_row_index) = augmentation_fingerprint(plan) {
        return WalkOutcome::Realized { with_row_index };
    }
    match plan {
        LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Distinct(_) => {
            let inputs = plan.inputs();
            if inputs.len() == 1 {
                return walk_plan(inputs[0]);
            }
            let mut outcome = WalkOutcome::DeadOther;
            for input in &inputs {
                if let WalkOutcome::DeadAttributeLoss = walk_plan(input) {
                    outcome = WalkOutcome::DeadAttributeLoss;
                }
            }
            outcome
        }
        LogicalPlan::Join(_) | LogicalPlan::Aggregate(_) => WalkOutcome::DeadAttributeLoss,
        LogicalPlan::TableScan(scan) => match marker_kind(scan) {
            Some(kind) => WalkOutcome::Found {
                scan: scan.clone(),
                kind,
            },
            None => WalkOutcome::DeadOther,
        },
        _ => WalkOutcome::DeadOther,
    }
}

#[must_use]
pub fn file_metadata_status(plan: &LogicalPlan) -> FileMetadataStatus {
    match walk_plan(plan) {
        WalkOutcome::Found { scan, .. } => {
            let shadowed = marker_kind(&scan).is_some_and(|_| {
                scan.source
                    .schema()
                    .fields()
                    .iter()
                    .any(|field| field.name() == METADATA_COLUMN_NAME)
            });
            if shadowed {
                FileMetadataStatus::Shadowed
            } else {
                FileMetadataStatus::Available
            }
        }
        WalkOutcome::Realized { .. } => FileMetadataStatus::Available,
        WalkOutcome::DeadAttributeLoss | WalkOutcome::DeadOther => FileMetadataStatus::Absent,
    }
}
