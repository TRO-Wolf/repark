use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::spec::Schema as IcebergSchema;
use iceberg::{Catalog, ErrorKind, TableIdent};

use crate::catalog_ops::{iceberg_err, table_or_view_not_found};
use crate::describe_show::DescribeTable;
use crate::spark_type_names::spark_ddl_type_name;
use repark_common::spark_error;

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn describe_view_frame(
    ctx: &SessionContext,
    catalog: &dyn Catalog,
    describe: &DescribeTable,
    ident: &TableIdent,
) -> Result<DataFrame> {
    let view = match catalog.load_view(ident).await {
        Ok(view) => view,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::ViewNotFound | ErrorKind::FeatureUnsupported
            ) =>
        {
            return Err(table_or_view_not_found(
                &describe.catalog,
                &describe.namespace,
                &describe.table,
            ));
        }
        Err(error) => return Err(iceberg_err(error)),
    };
    if let Some(parts) = &describe.column {
        let column = parts
            .iter()
            .map(|part| format!("`{}`", part.replace('`', "``")))
            .collect::<Vec<_>>()
            .join(".");
        return Err(DataFusionError::Plan(spark_error::message(
            spark_error::UNRESOLVED_COLUMN_WITHOUT_SUGGESTION,
            &[("columnName", column.as_str())],
        )));
    }
    ctx.read_batch(describe_view_batch(view.metadata().current_schema())?)
}

#[allow(clippy::missing_errors_doc)]
pub(crate) fn describe_view_batch(schema: &IcebergSchema) -> Result<RecordBatch> {
    let rows = describe_view_rows(schema)?;
    let mut names = Vec::with_capacity(rows.len());
    let mut types = Vec::with_capacity(rows.len());
    let mut comments = Vec::with_capacity(rows.len());
    for (name, data_type, comment) in rows {
        names.push(name);
        types.push(data_type);
        comments.push(comment);
    }
    let batch_schema = Arc::new(Schema::new(vec![
        Field::new("col_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("comment", DataType::Utf8, true),
    ]));
    Ok(RecordBatch::try_new(
        batch_schema,
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
            Arc::new(StringArray::from(comments)),
        ],
    )?)
}

fn describe_view_rows(schema: &IcebergSchema) -> Result<Vec<(String, String, Option<String>)>> {
    let arrow_schema = iceberg::arrow::schema_to_arrow_schema(schema).map_err(iceberg_err)?;
    let mut rows = Vec::new();
    for (iceberg_field, arrow_field) in schema
        .as_struct()
        .fields()
        .iter()
        .zip(arrow_schema.fields())
    {
        rows.push((
            arrow_field.name().clone(),
            spark_ddl_type_name(arrow_field.data_type()),
            Some(iceberg_field.doc.clone().unwrap_or_default()),
        ));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;
    use iceberg::spec::{NestedField, PrimitiveType, Type};

    fn stored_schema() -> IcebergSchema {
        IcebergSchema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(
                    NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String))
                        .with_doc("the payload"),
                ),
            ])
            .build()
            .unwrap_or_else(|error| panic!("schema must build: {error}"))
    }

    #[test]
    fn view_rows_render_stored_schema_with_empty_comment_default() {
        let rows = describe_view_rows(&stored_schema())
            .unwrap_or_else(|error| panic!("rows must build: {error}"));
        assert_eq!(
            rows,
            vec![
                ("id".to_string(), "bigint".to_string(), Some(String::new())),
                (
                    "data".to_string(),
                    "string".to_string(),
                    Some("the payload".to_string())
                ),
            ]
        );
    }

    #[test]
    fn view_batch_is_column_rows_only_with_nullable_comment() {
        let batch = describe_view_batch(&stored_schema())
            .unwrap_or_else(|error| panic!("batch must build: {error}"));
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema().field(0).name(), "col_name");
        assert_eq!(batch.schema().field(1).name(), "data_type");
        assert_eq!(batch.schema().field(2).name(), "comment");
        assert!(!batch.schema().field(0).is_nullable());
        assert!(!batch.schema().field(1).is_nullable());
        assert!(batch.schema().field(2).is_nullable());
        let comments = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("comment column must be a string array"));
        assert!(comments.is_valid(0));
        assert_eq!(comments.value(0), "");
        assert_eq!(comments.value(1), "the payload");
    }
}
