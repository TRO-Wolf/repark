use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::TableReference;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer, Word};
use iceberg::{Catalog, ErrorKind, NamespaceIdent, TableIdent};

use crate::catalog_ops::{catalog_handle, iceberg_err, name_parts, table_or_view_not_found};
use crate::describe_show::DescribeTable;
use crate::metadata_tables::canonical_metadata_table_name;
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
    })
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
    let handle = catalog_handle(catalogs, &describe.catalog).ok()?;
    if describe.table.contains('$') {
        return dollar_metadata_table(ctx, handle, describe).await;
    }
    unqualified_metadata_table(ctx, catalogs, handle, describe).await
}

#[allow(clippy::missing_errors_doc)]
async fn dollar_metadata_table(
    ctx: &SessionContext,
    handle: &Arc<dyn Catalog>,
    describe: &DescribeTable,
) -> Option<Result<RecordBatch>> {
    let (base, suffix) = describe.table.split_once('$')?;
    if base.is_empty() {
        return None;
    }
    canonical_metadata_table_name(suffix)?;
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
                    Some(Err(table_or_view_not_found(
                        &describe.catalog,
                        &describe.namespace,
                        base,
                    )))
                }
                Err(error) => Some(Err(iceberg_err(error))),
                Ok(_) => Some(Err(provider_error)),
            }
        }
    }
}

#[allow(clippy::missing_errors_doc)]
async fn unqualified_metadata_table(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    handle: &Arc<dyn Catalog>,
    describe: &DescribeTable,
) -> Option<Result<RecordBatch>> {
    let suffix = canonical_metadata_table_name(&describe.table)?;
    if describe.namespace.is_empty() {
        return None;
    }
    let (default_catalog, default_namespace) = catalogs.current_defaults();
    if default_catalog != describe.catalog || default_namespace.is_empty() {
        return None;
    }
    let real = TableIdent::new(
        NamespaceIdent::new(describe.namespace.clone()),
        describe.table.clone(),
    );
    match handle.load_table(&real).await {
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound
            ) =>
        {
            let base = TableIdent::new(
                NamespaceIdent::new(default_namespace.clone()),
                describe.namespace.clone(),
            );
            if handle.load_table(&base).await.is_err() {
                return None;
            }
            let reference = TableReference::full(
                describe.catalog.clone(),
                default_namespace,
                format!("{}${suffix}", describe.namespace),
            );
            match ctx.table_provider(reference).await {
                Ok(provider) => Some(metadata_table_describe_batch(&provider.schema())),
                Err(provider_error) => Some(Err(provider_error)),
            }
        }
        Ok(_) | Err(_) => None,
    }
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
}
