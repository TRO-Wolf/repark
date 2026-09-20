use std::collections::HashMap;

use repark_common::Result;

use crate::catalog_config::{self, CatalogKind};
use crate::resolve_s3_region_override;
use crate::session::{AWS_ENABLE_CONFIG_KEY, ReparkSession};

impl ReparkSession {
    /// Register catalogs from a late configuration map onto the live session.
    /// # Errors
    /// Returns `Error::Config` if the `spark.sql.catalog.*` block is malformed.
    pub async fn register_late_configured_catalogs(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let specs = catalog_config::parse_catalog_specs(config)?;
        // Late configuration can introduce the first AWS signal for an offline session.
        let late_aws_signaled = specs
            .iter()
            .any(|spec| matches!(spec.kind, CatalogKind::Glue | CatalogKind::S3Tables))
            || resolve_s3_region_override(config)?.is_some()
            || config
                .get(AWS_ENABLE_CONFIG_KEY)
                .is_some_and(|value| value.trim().eq_ignore_ascii_case("true"));
        self.resolve_aws_sdk_config_if(self.aws_signaled || late_aws_signaled)
            .await;
        let mut added = Vec::new();
        let mut skipped = Vec::new();
        for spec in &specs {
            let already_iceberg = self.catalog_handle(&spec.name).is_ok();
            let already_postgres = self
                .postgres_catalog_names
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains(&spec.name);
            if already_iceberg || already_postgres {
                // Keep existing registrations; never replace them silently.
                skipped.push(spec.name.clone());
            } else {
                self.register_catalog_spec(spec).await?;
                added.push(spec.name.clone());
            }
        }
        added.sort();
        skipped.sort();
        Ok((added, skipped))
    }

    pub(super) fn note_catalog_table_props(&self, name: &str, props: &HashMap<String, String>) {
        self.catalogs
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .merge_table_props(name, props);
    }

    fn is_catalog_registered(&self, name: &str) -> bool {
        if self.catalog_handle(name).is_ok() {
            return true;
        }
        self.postgres_catalog_names
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(name)
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn register_late_catalog_block(
        &self,
        config: &HashMap<String, String>,
    ) -> Result<bool> {
        let Ok(specs) = catalog_config::parse_catalog_specs(config) else {
            return Ok(false);
        };
        let Some(spec) = specs.into_iter().next() else {
            return Ok(false);
        };
        if self.is_catalog_registered(&spec.name) {
            self.note_catalog_table_props(&spec.name, &spec.props);
            return Ok(false);
        }
        if matches!(spec.kind, CatalogKind::Glue | CatalogKind::S3Tables) {
            self.resolve_aws_sdk_config_if(true).await;
        }
        self.register_catalog_spec(&spec).await?;
        self.note_catalog_table_props(&spec.name, &spec.props);
        Ok(true)
    }
}
