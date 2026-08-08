//! Partition-transform parsing and Iceberg spec building (design §2 Q2).
//!
//! `WITH (partitioning = ARRAY['month(ts)', 'bucket(16, id)'])` is the ONLY spelling this door
//! accepts. The transform strings are parsed by a **small pure function** here rather than by
//! sharing the Spark door's `PARTITIONED BY` validator: the design ruled explicitly against a
//! half-file move, and the reason holds up — the two doors read different grammars
//! (`PARTITIONED BY (bucket(16, id))` vs a string inside an ARRAY), so fusing their parsers would
//! couple two things that must stay free to differ. The anti-drift mechanism is the cross-door
//! differential row in PR-6, which pins identical ACCEPT/REJECT behavior without coupling the
//! code.
//!
//! Field NAMES follow Java/Spark (`col`, `col_bucket`, `col_trunc`, `col_year`, …). Matching
//! Java here is what lets a RePark-created table's partition spec read back identically to a
//! Spark-created one — a schema-equality pin, not cosmetics.

use datafusion::error::{DataFusionError, Result};
use iceberg::spec::{Transform, UnboundPartitionSpec};

/// One partition transform, parsed from its Trino string spelling.
///
/// Deliberately a re-implementation rather than a shared type with the Spark door (design §2 Q2:
/// "validation re-implemented ANSI-side as a small pure function, no half-file move"). The
/// anti-drift mechanism is the cross-door differential row in PR-6, not a shared parser: the two
/// doors read DIFFERENT syntax (`PARTITIONED BY (bucket(16, id))` vs `ARRAY['bucket(16, id)']`)
/// and coupling their parsers would fuse two grammars that must be free to differ.
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

    /// The partition-field NAME Java/Spark generate — identity keeps the column name, every other
    /// transform appends the Java suffix (`_bucket`, `_trunc`, `_year`, `_month`, `_day`,
    /// `_hour`; apache-iceberg `PartitionSpec.Builder`). Matching Java here is what lets a
    /// RePark-created table's spec read back identically to a Spark-created one.
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
/// Parse one transform spelling (`'ts'`, `'month(ts)'`, `'bucket(16, id)'`) — the small pure
/// function design §2 Q2 asks for.
/// ===========================================================================================
///
/// # Errors
/// An unknown transform name, the wrong argument count, a non-integer or non-positive width, or
/// a width that overflows `u32`.
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
/// Resolve declared transforms against the table's derived Iceberg schema into an
/// `UnboundPartitionSpec`. `None` when the table is unpartitioned.
/// ===========================================================================================
///
/// # Errors
/// A transform naming a column the table does not have (the message lists the available columns —
/// this is the error users actually hit, and a bare "not found" wastes a round trip).
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
