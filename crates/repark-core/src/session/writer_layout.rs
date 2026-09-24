use iceberg::{NamespaceIdent, TableIdent};
use repark_common::Error;
use repark_iceberg::write::WriterRequest;
pub use repark_iceberg::write::{
    WriterAction, WriterLayout, WriterRefusal, WriterStatement, missing_column_message,
    save_target_names_table,
};

use crate::session::ReparkSession;

#[derive(Debug, Clone, Copy)]
pub struct TableWriteRequest<'a> {
    pub action: WriterAction,
    pub target: &'a str,
    pub qualified: Option<&'a str>,
    pub mode: &'a str,
    pub explicit_format: bool,
    pub layout: &'a WriterLayout,
    pub frame_columns: &'a [String],
    pub case_sensitive: bool,
}

impl ReparkSession {
    #[allow(clippy::missing_errors_doc)]
    pub async fn plan_table_write(
        &self,
        request: &TableWriteRequest<'_>,
    ) -> Result<WriterStatement, WriterRefusal> {
        let (parts, exists) = match request.qualified {
            Some(name) => {
                let parts =
                    crate::parse_table_identifier_segments(name).map_err(Error::DataFusion)?;
                (parts, self.table_exists(name).await?)
            }
            None => (Vec::new(), false),
        };
        let relation_parts = parts.get(1..).unwrap_or_default();
        let plan = repark_iceberg::write::plan_writer(&WriterRequest {
            action: request.action,
            target: request.target,
            relation_parts,
            exists,
            mode: request.mode,
            explicit_format: request.explicit_format,
            layout: request.layout,
            frame_columns: request.frame_columns,
            case_sensitive: request.case_sensitive,
        })?;
        if plan.check_layout {
            self.check_writer_layout(&parts, request.layout).await?;
        }
        Ok(plan.statement)
    }

    async fn check_writer_layout(
        &self,
        parts: &[String],
        layout: &WriterLayout,
    ) -> repark_common::Result<()> {
        let [catalog_name, namespace @ .., name] = parts else {
            return Err(Error::DataFusion(format!(
                "writer layout check needs a catalog-qualified table name, got {parts:?}"
            )));
        };
        let catalog = self.catalog_handle(catalog_name)?;
        let namespace = NamespaceIdent::from_vec(namespace.to_vec())
            .map_err(|error| Error::DataFusion(error.to_string()))?;
        repark_iceberg::write::check_layout_matches_catalog_table(
            catalog.as_ref(),
            &TableIdent::new(namespace, name.clone()),
            layout,
        )
        .await
        .map_err(crate::engine_err)
    }
}
