use datafusion::error::Result;
use datafusion::prelude::{DataFrame, SessionContext};
use iceberg::spec::NestedFieldRef;
use iceberg::view::View;
use iceberg::{ErrorKind, NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;
use repark_iceberg::view::{VIEW_COMMENT_PROPERTY, view_read_spec};

use crate::catalog_ops::{catalog_handle, iceberg_err, table_or_view_not_found};
use crate::show_create::{
    ShowCreateStatement, render_tblproperties_clause, show_create_batch, spark_sql_string_literal,
};
use crate::view_ddl::execute::show_tblproperties_rows;

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn execute_show_create_view(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    statement: &ShowCreateStatement,
) -> Result<DataFrame> {
    let handle = catalog_handle(catalogs, &statement.catalog)?;
    let ident = TableIdent::new(
        NamespaceIdent::new(statement.namespace.clone()),
        statement.table.clone(),
    );
    let view = match handle.load_view(&ident).await {
        Ok(view) => view,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::ViewNotFound | ErrorKind::FeatureUnsupported
            ) =>
        {
            return Err(table_or_view_not_found(
                &statement.catalog,
                &statement.namespace,
                &statement.table,
            ));
        }
        Err(error) => return Err(iceberg_err(error)),
    };
    let text = render_show_create_view(statement, &view)?;
    ctx.read_batch(show_create_batch(text)?)
}

fn render_show_create_view(statement: &ShowCreateStatement, view: &View) -> Result<String> {
    let metadata = view.metadata();
    let properties = view_tblproperties(show_tblproperties_rows(
        view,
        None,
        &statement.catalog,
        &statement.namespace,
        &statement.table,
    ));
    Ok(render_create_view(
        &format!(
            "{}.{}.{}",
            statement.catalog, statement.namespace, statement.table
        ),
        metadata.current_schema().as_struct().fields(),
        metadata
            .properties()
            .get(VIEW_COMMENT_PROPERTY)
            .map(String::as_str),
        &properties,
        &view_read_spec(view)?.sql,
    ))
}

fn view_tblproperties(rows: Vec<(String, String)>) -> Vec<(String, String)> {
    let mut properties = rows
        .into_iter()
        .filter(|(key, _)| key != VIEW_COMMENT_PROPERTY)
        .collect::<Vec<_>>();
    properties.sort();
    properties
}

fn render_create_view(
    name: &str,
    fields: &[NestedFieldRef],
    comment: Option<&str>,
    properties: &[(String, String)],
    body: &str,
) -> String {
    let columns = fields
        .iter()
        .map(|field| match &field.doc {
            Some(doc) => format!("{} COMMENT {}", field.name, spark_sql_string_literal(doc)),
            None => field.name.clone(),
        })
        .collect::<Vec<_>>();
    let mut clauses = vec![format!(
        "CREATE VIEW {name} (\n  {})\n",
        columns.join(",\n  ")
    )];
    if let Some(comment) = comment {
        clauses.push(format!("COMMENT {}\n", spark_sql_string_literal(comment)));
    }
    clauses.push(render_tblproperties_clause(properties));
    clauses.push(format!("AS\n{body}\n"));
    clauses.concat()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::spec::{NestedField, PrimitiveType, Type};

    use super::*;

    fn pairs(items: &[(&str, &str)]) -> Vec<(String, String)> {
        items
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn fields(docs: &[(&str, Option<&str>)]) -> Vec<NestedFieldRef> {
        docs.iter()
            .zip(1..)
            .map(|((name, doc), id)| {
                let field = NestedField::optional(id, *name, Type::Primitive(PrimitiveType::Long));
                Arc::new(match doc {
                    Some(doc) => field.with_doc(*doc),
                    None => field,
                })
            })
            .collect()
    }

    #[test]
    fn view_tblproperties_drops_comment_and_sorts_by_key() {
        assert_eq!(
            view_tblproperties(pairs(&[
                ("location", "/wh/ns/v2"),
                ("provider", "iceberg"),
                ("format-version", "1"),
                ("comment", "view doc"),
                ("k", "v"),
            ])),
            pairs(&[
                ("format-version", "1"),
                ("k", "v"),
                ("location", "/wh/ns/v2"),
                ("provider", "iceberg"),
            ])
        );
    }

    #[test]
    fn render_create_view_without_docs_or_comment_matches_spark_v1() {
        assert_eq!(
            render_create_view(
                "sc.ns.v1",
                &fields(&[("id", None), ("data", None)]),
                None,
                &pairs(&[
                    ("format-version", "1"),
                    ("location", "/wh/ns/v1"),
                    ("provider", "iceberg"),
                ]),
                "SELECT id, data FROM sc.ns.t WHERE id > 0",
            ),
            "CREATE VIEW sc.ns.v1 (\n  id,\n  data)\nTBLPROPERTIES (\n  'format-version' = '1',\n  \
             'location' = '/wh/ns/v1',\n  'provider' = 'iceberg')\nAS\n\
             SELECT id, data FROM sc.ns.t WHERE id > 0\n"
        );
    }

    #[test]
    fn render_create_view_with_doc_and_comment_matches_spark_v2() {
        assert_eq!(
            render_create_view(
                "sc.ns.v2",
                &fields(&[("i", Some("the id")), ("d", None)]),
                Some("view doc"),
                &pairs(&[
                    ("format-version", "1"),
                    ("k", "v"),
                    ("location", "/wh/ns/v2"),
                    ("provider", "iceberg"),
                ]),
                "SELECT id, data FROM sc.ns.t",
            ),
            "CREATE VIEW sc.ns.v2 (\n  i COMMENT 'the id',\n  d)\nCOMMENT 'view doc'\n\
             TBLPROPERTIES (\n  'format-version' = '1',\n  'k' = 'v',\n  \
             'location' = '/wh/ns/v2',\n  'provider' = 'iceberg')\nAS\n\
             SELECT id, data FROM sc.ns.t\n"
        );
    }

    #[test]
    fn render_create_view_escapes_quotes_in_doc_and_comment() {
        assert_eq!(
            render_create_view(
                "sc.ns.q",
                &fields(&[("i", Some("it's"))]),
                Some("o'clock"),
                &pairs(&[("format-version", "1")]),
                "SELECT 1 AS i",
            ),
            "CREATE VIEW sc.ns.q (\n  i COMMENT 'it\\'s')\nCOMMENT 'o\\'clock'\n\
             TBLPROPERTIES (\n  'format-version' = '1')\nAS\nSELECT 1 AS i\n"
        );
    }
}
