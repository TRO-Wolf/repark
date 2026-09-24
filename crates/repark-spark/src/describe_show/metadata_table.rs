use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::TableReference;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};
use iceberg::{Catalog, ErrorKind, NamespaceIdent, TableIdent};

use crate::catalog_ops::{catalog_handle, iceberg_err, name_parts, table_or_view_not_found_parts};
use crate::describe_show::DescribeTable;
use crate::metadata_tables::{canonical_metadata_table_name, table_exists_parts};
use crate::namespace_ddl::consume_word;
use crate::spark_type_names::spark_ddl_type_name;
use repark_core::CatalogRegistry;

pub(crate) fn try_parse_describe_metadata_table(sql: &str) -> Option<DescribeTable> {
    let dialect = DatabricksDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    if !parser.parse_keyword(Keyword::DESCRIBE) && !parser.parse_keyword(Keyword::DESC) {
        return None;
    }
    if matches!(&parser.peek_token().token, Token::Word(word) if is_namespace_head(word)) {
        return None;
    }
    let _ = parser.parse_keyword(Keyword::TABLE);
    let extended =
        parser.parse_keyword(Keyword::EXTENDED) || consume_word(&mut parser, "FORMATTED");
    let name = parser.parse_object_name(false).ok()?;
    if !matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) {
        return None;
    }
    let parts = name_parts(&name);
    let [catalog, namespace, table, meta] = parts.as_slice() else {
        return None;
    };
    let suffix = canonical_metadata_table_name(meta)?;
    Some(DescribeTable {
        catalog: catalog.clone(),
        namespace: namespace.clone(),
        table: format!("{table}${suffix}"),
        extended,
        written_parts: parts,
        column: None,
        partition: false,
    })
}

pub(crate) fn rewrites_metadata_path(sql: &str) -> bool {
    crate::metadata_tables::sql_may_have_metadata_table_path(sql)
        && try_parse_describe_metadata_table(sql).is_none()
}

fn is_namespace_head(word: &Word) -> bool {
    word.value.eq_ignore_ascii_case("namespace")
        || word.value.eq_ignore_ascii_case("database")
        || word.value.eq_ignore_ascii_case("schema")
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn try_describe_metadata_table(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    describe: &DescribeTable,
) -> Option<Result<RecordBatch>> {
    if !describe.table.contains('$') {
        return None;
    }
    let handle = catalog_handle(catalogs, &describe.catalog).ok()?;
    dollar_metadata_table(ctx, catalogs, handle, describe).await
}

#[allow(clippy::missing_errors_doc)]
async fn dollar_metadata_table(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    handle: &Arc<dyn Catalog>,
    describe: &DescribeTable,
) -> Option<Result<RecordBatch>> {
    let (base, suffix) = describe.table.rsplit_once('$')?;
    if base.is_empty() {
        return None;
    }
    canonical_metadata_table_name(suffix)?;
    match table_exists_parts(catalogs, &describe.written_parts).await {
        Ok(true) => {
            return Some(Err(unsupported_compound_identifier(
                &describe.written_parts,
            )));
        }
        Ok(false) => {}
        Err(error) => return Some(Err(error)),
    }
    let reference = TableReference::full(
        describe.catalog.clone(),
        describe.namespace.clone(),
        describe.table.clone(),
    );
    match ctx.table_provider(reference).await {
        Ok(provider) => Some(metadata_table_describe_batch(&provider.schema())),
        Err(provider_error) => {
            let base_ident = TableIdent::new(
                NamespaceIdent::new(describe.namespace.clone()),
                base.to_string(),
            );
            match handle.load_table(&base_ident).await {
                Err(error)
                    if matches!(
                        error.kind(),
                        ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound
                    ) =>
                {
                    let parts: Vec<&str> = if describe.written_parts.is_empty() {
                        vec![
                            describe.catalog.as_str(),
                            describe.namespace.as_str(),
                            describe.table.as_str(),
                        ]
                    } else {
                        describe.written_parts.iter().map(String::as_str).collect()
                    };
                    Some(Err(table_or_view_not_found_parts(&parts)))
                }
                Err(error) => Some(Err(iceberg_err(error))),
                Ok(_) => Some(Err(provider_error)),
            }
        }
    }
}

fn unsupported_compound_identifier(parts: &[String]) -> DataFusionError {
    let name = parts
        .iter()
        .map(|part| format!("`{part}`"))
        .collect::<Vec<_>>()
        .join(".");
    DataFusionError::Plan(format!(
        "Unsupported compound identifier '{name}'. Expected 1, 2 or 3 parts, got {}",
        parts.len()
    ))
}

#[allow(clippy::missing_errors_doc)]
fn metadata_table_describe_batch(schema: &Schema) -> Result<RecordBatch> {
    let names: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let types: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| spark_ddl_type_name(field.data_type()))
        .collect();
    let comments: Vec<Option<String>> = vec![None; names.len()];
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Array;
    use iceberg::CatalogBuilder;

    use super::*;

    #[test]
    fn metadata_table_describe_batch_spells_column_rows() {
        let entries = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        );
        let schema = Schema::new(vec![
            Field::new("snapshot_id", DataType::Int64, false),
            Field::new("operation", DataType::Utf8, true),
            Field::new("summary", DataType::Map(Arc::new(entries), false), true),
        ]);
        let batch =
            metadata_table_describe_batch(&schema).expect("a three-field schema builds rows");
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["col_name", "data_type", "comment"]
        );
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.is_nullable())
                .collect::<Vec<_>>(),
            vec![false, false, true]
        );
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("col_name is utf8");
        let types = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("data_type is utf8");
        let comments = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("comment is utf8");
        assert_eq!(
            (0..3).map(|index| names.value(index)).collect::<Vec<_>>(),
            vec!["snapshot_id", "operation", "summary"]
        );
        assert_eq!(
            (0..3).map(|index| types.value(index)).collect::<Vec<_>>(),
            vec!["bigint", "string", "map<string,string>"]
        );
        assert!((0..3).all(|index| comments.is_null(index)));
    }

    #[tokio::test]
    async fn describe_metadata_table_real_table_at_written_path_wins() {
        let warehouse_dir = tempfile::TempDir::new().expect("warehouse tempdir");
        let warehouse = warehouse_dir
            .path()
            .to_str()
            .expect("utf8 warehouse")
            .to_string();
        let catalog: Arc<dyn Catalog> = Arc::new(
            iceberg::memory::MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(iceberg::io::LocalFsStorageFactory))
                .load(
                    "memory",
                    std::collections::HashMap::from([(
                        iceberg::memory::MEMORY_CATALOG_WAREHOUSE.to_string(),
                        warehouse.clone(),
                    )]),
                )
                .await
                .expect("memory catalog builds"),
        );
        catalog
            .create_namespace(
                &NamespaceIdent::new("ns".to_string()),
                std::collections::HashMap::from([(
                    "location".to_string(),
                    format!("{warehouse}/ns"),
                )]),
            )
            .await
            .expect("ns namespace");
        let nested = NamespaceIdent::from_vec(vec!["ns".to_string(), "t".to_string()])
            .expect("nested ident");
        catalog
            .create_namespace(
                &nested,
                std::collections::HashMap::from([(
                    "location".to_string(),
                    format!("{warehouse}/ns/t"),
                )]),
            )
            .await
            .expect("nested ns.t namespace");
        let base_location = format!("{warehouse}/ns/t_base");
        std::fs::create_dir_all(&base_location).expect("base dir");
        catalog
            .create_table(
                &NamespaceIdent::new("ns".to_string()),
                iceberg::TableCreation::builder()
                    .name("t".to_string())
                    .location(base_location)
                    .schema(one_int_schema())
                    .build(),
            )
            .await
            .expect("base table mt.ns.t");
        let colliding_location = format!("{warehouse}/ns/t/snapshots");
        std::fs::create_dir_all(&colliding_location).expect("colliding dir");
        catalog
            .create_table(
                &nested,
                iceberg::TableCreation::builder()
                    .name("snapshots".to_string())
                    .location(colliding_location)
                    .schema(one_int_schema())
                    .build(),
            )
            .await
            .expect("real table mt.ns.t.snapshots");
        let ctx = SessionContext::new();
        repark_iceberg::catalog::register_iceberg_catalog(&ctx, "mt", catalog.clone())
            .await
            .expect("df catalog registers");
        let catalogs = CatalogRegistry::from([("mt".to_string(), catalog)]);
        let error = crate::execute(&ctx, &catalogs, "DESCRIBE mt.ns.t.snapshots")
            .await
            .expect_err("a real table at the written path must not answer metadata rows");
        let message = error.to_string();
        assert!(
            message.contains("Unsupported compound identifier")
                && message.contains("`mt`.`ns`.`t`.`snapshots`"),
            "the colliding name answers what plain resolution answers: {message}"
        );
    }

    fn one_int_schema() -> iceberg::spec::Schema {
        iceberg::spec::Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![std::sync::Arc::new(
                iceberg::spec::NestedField::optional(
                    1,
                    "x",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                ),
            )])
            .build()
            .expect("schema builds")
    }
}
