use iceberg::{NamespaceIdent, TableIdent};
use repark_common::{Error, Result};

use crate::session::ReparkSession;

impl ReparkSession {
    #[allow(clippy::missing_errors_doc)]
    pub async fn check_writer_layout(
        &self,
        parts: &[String],
        partition_columns: Vec<String>,
        num_buckets: Option<i64>,
        bucket_columns: Vec<String>,
        sort_columns: Vec<String>,
    ) -> Result<()> {
        let [catalog_name, namespace @ .., name] = parts else {
            return Err(Error::DataFusion(format!(
                "writer layout check needs a catalog-qualified table name, got {parts:?}"
            )));
        };
        let catalog = self.catalog_handle(catalog_name)?;
        let namespace = NamespaceIdent::from_vec(namespace.to_vec())
            .map_err(|error| Error::DataFusion(error.to_string()))?;
        let layout = repark_iceberg::write::WriterLayout {
            partition_columns,
            num_buckets,
            bucket_columns,
            sort_columns,
        };
        repark_iceberg::write::check_layout_matches_catalog_table(
            catalog.as_ref(),
            &TableIdent::new(namespace, name.clone()),
            &layout,
        )
        .await
        .map_err(crate::engine_err)
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn writer_save_target(
        &self,
        target: &str,
        qualified: Option<&str>,
        mode: &str,
        explicit_format: bool,
    ) -> Result<&'static str> {
        let (relation_parts, exists) = match qualified {
            Some(name) => {
                let parts =
                    crate::parse_table_identifier_segments(name).map_err(Error::DataFusion)?;
                let exists = self.table_exists(name).await?;
                (parts.into_iter().skip(1).collect::<Vec<_>>(), exists)
            }
            None => (Vec::new(), false),
        };
        repark_iceberg::write::decide_save_target(
            target,
            &relation_parts,
            exists,
            mode,
            explicit_format,
        )
        .map(repark_iceberg::write::SaveTarget::as_str)
    }
}
