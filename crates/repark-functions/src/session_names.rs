use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::ScalarValue;
use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::{SessionConfig, SessionContext};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionNameSource {
    Catalog,
    Namespace,
}

#[derive(Debug)]
struct SessionName {
    name: &'static str,
    source: SessionNameSource,
    signature: Signature,
}

impl SessionName {
    fn new(name: &'static str, source: SessionNameSource) -> Self {
        Self {
            name,
            source,
            signature: Signature::nullary(Volatility::Stable),
        }
    }

    fn value(&self, args: &ScalarFunctionArgs) -> String {
        let options = &args.config_options;
        let (catalog, namespace) = session_defaults_from_options(options).unwrap_or_else(|| {
            (
                options.catalog.default_catalog.clone(),
                options.catalog.default_schema.clone(),
            )
        });
        match self.source {
            SessionNameSource::Catalog => catalog,
            SessionNameSource::Namespace => namespace,
        }
    }
}

impl PartialEq for SessionName {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SessionName {}

impl Hash for SessionName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for SessionName {
    fn name(&self) -> &'static str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name, DataType::Utf8, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            self.value(&args),
        ))))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionDefaults {
    pub catalog: String,
    pub namespace: String,
}

impl Default for SessionDefaults {
    fn default() -> Self {
        Self {
            catalog: "spark_catalog".to_string(),
            namespace: "default".to_string(),
        }
    }
}

impl ConfigExtension for SessionDefaults {
    const PREFIX: &'static str = "repark.session-defaults";
}

impl ExtensionOptions for SessionDefaults {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, _value: &str) -> datafusion::common::Result<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not a settable option: the session default moves with `USE`",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

#[must_use]
pub fn with_session_defaults(config: SessionConfig) -> SessionConfig {
    config.with_option_extension(SessionDefaults::default())
}

#[must_use]
pub fn session_defaults_from_options(options: &ConfigOptions) -> Option<(String, String)> {
    options
        .extensions
        .get::<SessionDefaults>()
        .map(|extension| (extension.catalog.clone(), extension.namespace.clone()))
}

#[must_use]
pub fn current_catalog_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(SessionName::new(
        "current_catalog",
        SessionNameSource::Catalog,
    )))
}

#[must_use]
pub fn current_schema_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(SessionName::new(
        "current_schema",
        SessionNameSource::Namespace,
    )))
}

#[must_use]
pub fn current_database_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(SessionName::new(
        "current_database",
        SessionNameSource::Namespace,
    )))
}

pub fn register(ctx: &SessionContext) {
    ctx.register_udf(current_catalog_udf().as_ref().clone());
    ctx.register_udf(current_schema_udf().as_ref().clone());
    ctx.register_udf(current_database_udf().as_ref().clone());
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::StringArray;
    use datafusion::config::Dialect;
    use datafusion::prelude::SessionConfig;

    use super::*;

    fn test_context() -> SessionContext {
        let mut config = SessionConfig::new();
        config.options_mut().sql_parser.dialect = Dialect::Databricks;
        SessionContext::new_with_config(config)
    }

    async fn one_string(ctx: &SessionContext, sql: &str) -> (String, bool) {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut values = Vec::new();
        let mut nullable = false;
        for batch in &batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                values.push(column.value(row).to_string());
            }
            nullable = batch.schema().field(0).is_nullable();
        }
        assert_eq!(values.len(), 1);
        (values.remove(0), nullable)
    }

    #[tokio::test]
    async fn session_names_read_the_catalog_defaults() {
        let ctx = test_context();
        register(&ctx);
        ctx.sql("SET datafusion.catalog.default_catalog = 'sc'")
            .await
            .unwrap();
        ctx.sql("SET datafusion.catalog.default_schema = 'ns'")
            .await
            .unwrap();
        assert_eq!(
            one_string(&ctx, "SELECT current_catalog()").await,
            ("sc".to_string(), false)
        );
        assert_eq!(
            one_string(&ctx, "SELECT current_schema()").await,
            ("ns".to_string(), false)
        );
        assert_eq!(
            one_string(&ctx, "SELECT current_database()").await,
            ("ns".to_string(), false)
        );
    }

    #[tokio::test]
    async fn bare_session_name_does_not_resolve_as_call() {
        let ctx = test_context();
        register(&ctx);
        let error = ctx.sql("SELECT current_catalog AS v").await.unwrap_err();
        assert!(error.to_string().contains("No field named"), "got: {error}");
    }

    #[tokio::test]
    async fn session_names_track_a_later_set() {
        let ctx = test_context();
        register(&ctx);
        ctx.sql("SET datafusion.catalog.default_catalog = 'one'")
            .await
            .unwrap();
        assert_eq!(
            one_string(&ctx, "SELECT current_catalog()").await.0,
            "one".to_string()
        );
        ctx.sql("SET datafusion.catalog.default_catalog = 'two'")
            .await
            .unwrap();
        ctx.sql("SET datafusion.catalog.default_schema = ''")
            .await
            .unwrap();
        assert_eq!(
            one_string(&ctx, "SELECT current_catalog()").await.0,
            "two".to_string()
        );
        assert_eq!(
            one_string(&ctx, "SELECT current_database()").await.0,
            String::new()
        );
    }
}
