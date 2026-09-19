use datafusion::error::{DataFusionError, Result};
use iceberg::expr::Predicate;
use iceberg::spec::Transform;
use iceberg::table::Table;

use crate::write::partition_overwrite::{
    PartitionEquality, PartitionFieldBinding, PartitionOverwriteRequest, StaticPartitionOverwrite,
    bind_partition_field, equality_predicate, non_partition_column, resolve_binding,
};

pub const OVERWRITE_MODE_OPTION: &str = "overwrite-mode";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OverwriteIntent {
    #[default]
    Session,
    Static,
    Dynamic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OverwriteMode {
    pub session_dynamic: bool,
    pub intent: OverwriteIntent,
    pub option_dynamic: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverwriteScope {
    WholeTable,
    RowFilter,
    ReplacePartitions,
}

#[derive(Debug, Clone)]
pub enum OverwritePlan {
    WholeTable,
    RowFilter(StaticPartitionOverwrite),
    ReplacePartitions(Vec<PartitionEquality>),
}

impl OverwriteMode {
    #[must_use]
    pub fn scope(self, has_static_values: bool) -> OverwriteScope {
        let dynamic = match self.intent {
            OverwriteIntent::Session => self.session_dynamic,
            OverwriteIntent::Static => false,
            OverwriteIntent::Dynamic => true,
        };
        if dynamic {
            OverwriteScope::ReplacePartitions
        } else if has_static_values {
            OverwriteScope::RowFilter
        } else if self.option_dynamic && self.intent == OverwriteIntent::Session {
            OverwriteScope::ReplacePartitions
        } else {
            OverwriteScope::WholeTable
        }
    }
}

impl OverwritePlan {
    #[must_use]
    pub fn equalities(&self) -> &[PartitionEquality] {
        match self {
            OverwritePlan::WholeTable => &[],
            OverwritePlan::RowFilter(spec) => &spec.equalities,
            OverwritePlan::ReplacePartitions(equalities) => equalities,
        }
    }
}

#[must_use]
pub fn overwrite_mode_option_is_dynamic(raw: &str) -> bool {
    raw.eq_ignore_ascii_case("dynamic")
}

#[allow(clippy::missing_errors_doc)]
pub fn plan_overwrite(
    table: &Table,
    request: &PartitionOverwriteRequest,
    mode: OverwriteMode,
) -> Result<OverwritePlan> {
    let bindings = validated_bindings(table, request)?;
    match mode.scope(!request.equalities.is_empty()) {
        OverwriteScope::WholeTable => Ok(OverwritePlan::WholeTable),
        OverwriteScope::ReplacePartitions => {
            Ok(OverwritePlan::ReplacePartitions(request.equalities.clone()))
        }
        OverwriteScope::RowFilter => {
            let mut predicates: Vec<Predicate> = Vec::with_capacity(request.equalities.len());
            for equality in &request.equalities {
                let binding = resolve_binding(&bindings, &equality.name)?;
                predicates.push(equality_predicate(binding, equality)?);
            }
            let predicate = predicates
                .into_iter()
                .reduce(Predicate::and)
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "INSERT OVERWRITE PARTITION row filter needs at least one k=v assignment"
                            .to_string(),
                    )
                })?;
            Ok(OverwritePlan::RowFilter(StaticPartitionOverwrite {
                predicate,
                equalities: request.equalities.clone(),
            }))
        }
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn validated_static_equalities(
    table: &Table,
    request: &PartitionOverwriteRequest,
) -> Result<Vec<PartitionEquality>> {
    validated_bindings(table, request)?;
    Ok(request.equalities.clone())
}

fn validated_bindings(
    table: &Table,
    request: &PartitionOverwriteRequest,
) -> Result<Vec<PartitionFieldBinding>> {
    let spec = table.metadata().default_partition_spec();
    if spec.is_unpartitioned() {
        return match request.names.first() {
            Some(name) => Err(non_partition_column(name)),
            None => Ok(Vec::new()),
        };
    }
    let schema = table.metadata().current_schema();
    let bindings = spec
        .fields()
        .iter()
        .map(|field| bind_partition_field(schema.as_ref(), field))
        .collect::<Result<Vec<_>>>()?;
    for equality in &request.equalities {
        let transform_source = bindings.iter().any(|binding| {
            binding.transform != Transform::Identity
                && binding
                    .source_column_name
                    .eq_ignore_ascii_case(&equality.name)
        });
        if transform_source && resolve_binding(&bindings, &equality.name).is_err() {
            return Err(DataFusionError::NotImplemented(format!(
                "INSERT OVERWRITE … PARTITION (…) static assignment `{}` is not an identity \
                 partition field of the target table",
                equality.name
            )));
        }
    }
    for name in &request.names {
        resolve_binding(&bindings, name)?;
    }
    for equality in &request.equalities {
        let binding = resolve_binding(&bindings, &equality.name)?;
        if binding.transform != Transform::Identity {
            return Err(DataFusionError::NotImplemented(format!(
                "static INSERT OVERWRITE PARTITION only supports identity partition fields; `{}` \
                 uses {}",
                binding.spec_field_name, binding.transform
            )));
        }
    }
    Ok(bindings)
}
