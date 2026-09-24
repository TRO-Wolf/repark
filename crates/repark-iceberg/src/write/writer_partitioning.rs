use datafusion::error::Result;
use iceberg::spec::Transform;
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};

use crate::write::illegal_argument::illegal_argument_error;

#[derive(Debug, Default, Clone)]
pub struct WriterLayout {
    pub partition_columns: Vec<String>,
    pub num_buckets: Option<i64>,
    pub bucket_columns: Vec<String>,
    pub sort_columns: Vec<String>,
}

fn quote_if_needed(part: &str) -> String {
    let plain = !part.is_empty()
        && part
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_');
    let numeric = part.chars().all(|character| character.is_ascii_digit());
    if plain && !numeric {
        part.to_string()
    } else {
        format!("`{}`", part.replace('`', "``"))
    }
}

fn describe_column(name: &str) -> String {
    name.split('.')
        .map(quote_if_needed)
        .collect::<Vec<_>>()
        .join(".")
}

fn describe_columns(names: &[String]) -> String {
    names
        .iter()
        .map(|name| describe_column(name))
        .collect::<Vec<_>>()
        .join(", ")
}

#[must_use]
pub fn provided_transforms(layout: &WriterLayout) -> Vec<String> {
    let mut transforms: Vec<String> = layout
        .partition_columns
        .iter()
        .map(|column| format!("identity({})", describe_column(column)))
        .collect();
    if let Some(num_buckets) = layout.num_buckets {
        let buckets = describe_columns(&layout.bucket_columns);
        if layout.sort_columns.is_empty() {
            transforms.push(format!("bucket({num_buckets}, {buckets})"));
        } else {
            let sorts = describe_columns(&layout.sort_columns);
            transforms.push(format!("sorted_bucket({buckets}, {num_buckets}, {sorts})"));
        }
    }
    transforms
}

#[must_use]
pub fn table_transforms(table: &Table) -> Vec<String> {
    let metadata = table.metadata();
    let schema = metadata.current_schema();
    metadata
        .default_partition_spec()
        .fields()
        .iter()
        .filter_map(|field| {
            let source = schema
                .name_by_field_id(field.source_id)
                .map_or_else(|| field.source_id.to_string(), describe_column);
            match field.transform {
                Transform::Void => None,
                Transform::Identity => Some(format!("identity({source})")),
                Transform::Bucket(count) => Some(format!("bucket({count}, {source})")),
                Transform::Truncate(width) => Some(format!("truncate({width}, {source})")),
                Transform::Year => Some(format!("years({source})")),
                Transform::Month => Some(format!("months({source})")),
                Transform::Day => Some(format!("days({source})")),
                Transform::Hour => Some(format!("hours({source})")),
                Transform::Unknown => Some(format!("unknown({source})")),
            }
        })
        .collect()
}

#[allow(clippy::missing_errors_doc)]
pub fn check_layout_matches_table(table: &Table, layout: &WriterLayout) -> Result<()> {
    let provided = provided_transforms(layout);
    if provided.is_empty() {
        return Ok(());
    }
    let current = table_transforms(table);
    if provided == current {
        return Ok(());
    }
    Err(illegal_argument_error(format!(
        "requirement failed: The provided partitioning or clustering columns do not match the \
         existing table's.\n - provided: {}\n - table: {}",
        provided.join(", "),
        current.join(", ")
    )))
}

#[allow(clippy::missing_errors_doc)]
pub async fn check_layout_matches_catalog_table(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    layout: &WriterLayout,
) -> Result<()> {
    if provided_transforms(layout).is_empty() {
        return Ok(());
    }
    let table = catalog
        .load_table(ident)
        .await
        .map_err(crate::catalog::iceberg_to_datafusion)?;
    check_layout_matches_table(&table, layout)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaveTarget {
    Create,
    Append,
    Overwrite,
    Skip,
}

impl SaveTarget {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Append => "append",
            Self::Overwrite => "overwrite",
            Self::Skip => "skip",
        }
    }
}

fn path_relation(path: &str) -> String {
    let segments: Vec<&str> = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    let Some((name, parent)) = segments.split_last() else {
        return quote_if_needed(path);
    };
    let namespace = if path.starts_with('/') {
        format!("/{}", parent.join("/"))
    } else {
        parent.join("/")
    };
    format!("{}.{}", quote_if_needed(&namespace), quote_if_needed(name))
}

#[allow(clippy::missing_errors_doc)]
pub fn decide_save_target(
    target: &str,
    relation_parts: &[String],
    exists: bool,
    mode: &str,
    explicit_format: bool,
) -> repark_common::Result<SaveTarget> {
    let normalized = if mode == "errorifexists" {
        "error"
    } else {
        mode
    };
    let writes_existing = matches!(normalized, "append" | "overwrite");
    let is_path = target.contains('/');
    if is_path && explicit_format && writes_existing {
        let relation = path_relation(target);
        return Err(repark_common::spark_error::analysis(
            repark_common::spark_error::TABLE_OR_VIEW_NOT_FOUND,
            &[("relationName", relation.as_str())],
        ));
    }
    if is_path || !explicit_format {
        return Err(repark_common::Error::Analysis(
            "DataFrameWriter.save(path) requires format('parquet'|'csv'|'json'|'text'); use \
             saveAsTable for Iceberg tables"
                .to_string(),
        ));
    }
    match (normalized, exists) {
        ("append", true) => Ok(SaveTarget::Append),
        ("overwrite", true) => Ok(SaveTarget::Overwrite),
        ("append" | "overwrite", false) => {
            let relation = relation_parts
                .iter()
                .map(|part| quote_if_needed(part))
                .collect::<Vec<_>>()
                .join(".");
            Err(repark_common::spark_error::analysis(
                repark_common::spark_error::TABLE_OR_VIEW_NOT_FOUND,
                &[("relationName", relation.as_str())],
            ))
        }
        ("ignore", true) => Ok(SaveTarget::Skip),
        ("error", true) => {
            let relation = relation_parts
                .iter()
                .map(|part| format!("`{}`", part.replace('`', "``")))
                .collect::<Vec<_>>()
                .join(".");
            Err(repark_common::spark_error::analysis(
                repark_common::spark_error::TABLE_OR_VIEW_ALREADY_EXISTS,
                &[("relationName", relation.as_str())],
            ))
        }
        _ => Ok(SaveTarget::Create),
    }
}
