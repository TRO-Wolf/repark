//! Partition-transform parsing and Iceberg spec building (design §2 Q2).

use datafusion::error::{DataFusionError, Result};
use iceberg::spec::{Transform, UnboundPartitionSpec};

/// One partition transform, parsed from its Trino string spelling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PartitionTransform {
    /// `c` or `identity(c)`.
    Identity(String),
    /// `bucket(N, c)`.
    Bucket { column: String, buckets: u32 },
    /// `truncate(N, c)`.
    Truncate { column: String, width: u32 },
    /// `year(c)` / `month(c)` / `day(c)` / `hour(c)`.
    Temporal { column: String, unit: TemporalUnit },
}

/// The four Iceberg temporal transforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TemporalUnit {
    /// `year(c)`.
    Year,
    /// `month(c)`.
    Month,
    /// `day(c)`.
    Day,
    /// `hour(c)`.
    Hour,
}

impl PartitionTransform {
    /// The source column this transform reads.
    pub(crate) fn column(&self) -> &str {
        match self {
            PartitionTransform::Identity(column)
            | PartitionTransform::Bucket { column, .. }
            | PartitionTransform::Truncate { column, .. }
            | PartitionTransform::Temporal { column, .. } => column,
        }
    }

    /// The Iceberg transform.
    pub(crate) fn transform(&self) -> Transform {
        match self {
            PartitionTransform::Identity(_) => Transform::Identity,
            PartitionTransform::Bucket { buckets, .. } => Transform::Bucket(*buckets),
            PartitionTransform::Truncate { width, .. } => Transform::Truncate(*width),
            PartitionTransform::Temporal { unit, .. } => match unit {
                TemporalUnit::Year => Transform::Year,
                TemporalUnit::Month => Transform::Month,
                TemporalUnit::Day => Transform::Day,
                TemporalUnit::Hour => Transform::Hour,
            },
        }
    }

    /// Return the Java/Spark partition-field name; identity keeps the source column name.
    pub(crate) fn field_name(&self) -> String {
        match self {
            PartitionTransform::Identity(column) => column.clone(),
            PartitionTransform::Bucket { column, .. } => format!("{column}_bucket"),
            PartitionTransform::Truncate { column, .. } => format!("{column}_trunc"),
            PartitionTransform::Temporal { column, unit } => {
                let suffix = match unit {
                    TemporalUnit::Year => "year",
                    TemporalUnit::Month => "month",
                    TemporalUnit::Day => "day",
                    TemporalUnit::Hour => "hour",
                };
                format!("{column}_{suffix}")
            }
        }
    }
}

/// ===========================================================================================
/// Parse one transform spelling (`'ts'`, `'month(ts)'`, or `'bucket(16, id)'`).
/// ===========================================================================================
pub(crate) fn parse_transform(spelling: &str, form: &str) -> Result<PartitionTransform> {
    let trimmed = spelling.trim();
    if trimmed.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "{form} WITH: empty `partitioning` element"
        )));
    }
    // A bare column name is an identity partition (Trino spelling).
    let Some(open) = trimmed.find('(') else {
        return Ok(PartitionTransform::Identity(trimmed.to_string()));
    };
    if !trimmed.ends_with(')') {
        return Err(DataFusionError::Plan(format!(
            "{form} WITH: malformed `partitioning` element `{spelling}` — expected \
             `transform(args)` or a bare column name"
        )));
    }
    let name = trimmed[..open].trim().to_ascii_lowercase();
    let args: Vec<String> = trimmed[open + 1..trimmed.len() - 1]
        .split(',')
        .map(|arg| arg.trim().to_string())
        .filter(|arg| !arg.is_empty())
        .collect();

    let arity = |want: &str| {
        DataFusionError::Plan(format!(
            "{form} WITH: `partitioning` element `{spelling}` expects {want}, got {} argument(s)",
            args.len()
        ))
    };
    let width = |raw: &str, label: &str| -> Result<u32> {
        let parsed: i64 = raw.parse().map_err(|_| {
            DataFusionError::Plan(format!(
                "{form} WITH: `partitioning` element `{spelling}` {label} must be an integer, got \
                 `{raw}`"
            ))
        })?;
        if parsed <= 0 {
            return Err(DataFusionError::Plan(format!(
                "{form} WITH: `partitioning` element `{spelling}` {label} must be > 0, got \
                 `{parsed}`"
            )));
        }
        u32::try_from(parsed).map_err(|_| {
            DataFusionError::Plan(format!(
                "{form} WITH: `partitioning` element `{spelling}` {label} `{parsed}` is too large \
                 (max {})",
                u32::MAX
            ))
        })
    };

    match name.as_str() {
        "bucket" => {
            let [count, column] = args.as_slice() else {
                return Err(arity("(count, column)"));
            };
            Ok(PartitionTransform::Bucket {
                column: column.clone(),
                buckets: width(count, "bucket count")?,
            })
        }
        "truncate" => {
            let [size, column] = args.as_slice() else {
                return Err(arity("(width, column)"));
            };
            Ok(PartitionTransform::Truncate {
                column: column.clone(),
                width: width(size, "width")?,
            })
        }
        "identity" | "year" | "month" | "day" | "hour" => {
            let [column] = args.as_slice() else {
                return Err(arity("a single (column)"));
            };
            Ok(match name.as_str() {
                "year" => PartitionTransform::Temporal {
                    column: column.clone(),
                    unit: TemporalUnit::Year,
                },
                "month" => PartitionTransform::Temporal {
                    column: column.clone(),
                    unit: TemporalUnit::Month,
                },
                "day" => PartitionTransform::Temporal {
                    column: column.clone(),
                    unit: TemporalUnit::Day,
                },
                "hour" => PartitionTransform::Temporal {
                    column: column.clone(),
                    unit: TemporalUnit::Hour,
                },
                _ => PartitionTransform::Identity(column.clone()),
            })
        }
        other => Err(DataFusionError::NotImplemented(format!(
            "{form} WITH: `{other}` is not a supported partition transform (supported: identity, \
             bucket, truncate, year, month, day, hour)"
        ))),
    }
}

/// ===========================================================================================
/// Resolve declared transforms against the table's derived Iceberg schema into an Iceberg spec.
/// ===========================================================================================
pub(crate) fn build_partition_spec(
    schema: &iceberg::spec::Schema,
    transforms: &[PartitionTransform],
) -> Result<Option<UnboundPartitionSpec>> {
    if transforms.is_empty() {
        return Ok(None);
    }
    let mut builder = UnboundPartitionSpec::builder();
    for transform in transforms {
        let column = transform.column();
        let field = schema
            .as_struct()
            .fields()
            .iter()
            .find(|field| field.name == *column)
            .ok_or_else(|| {
                let available = schema
                    .as_struct()
                    .fields()
                    .iter()
                    .map(|field| field.name.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                DataFusionError::Plan(format!(
                    "cannot resolve partition column `{column}`: it is not a column of the table \
                     (names are exact-case; available: [{available}])"
                ))
            })?;
        builder = builder
            .add_partition_field(field.id, transform.field_name(), transform.transform())
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
    }
    Ok(Some(builder.build()))
}

#[cfg(test)]
mod tests;
