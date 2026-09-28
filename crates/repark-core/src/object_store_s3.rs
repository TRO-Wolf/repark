//! Register `s3://` and `s3a://` object stores for `read_parquet`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use object_store::CredentialProvider;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::aws::{AmazonS3Builder, AwsCredential};
use object_store::path::Path as ObjectPath;
use url::Url;

use repark_common::{Error, Result};

/// The URL schemes `RePark` treats as S3: `s3` (Iceberg warehouses) and `s3a` (Spark bronze reads).
pub(crate) const S3_SCHEMES: [&str; 2] = ["s3", "s3a"];

/// The Spark/Hadoop config key that overrides the S3 read region.
pub(crate) const S3A_REGION_CONFIG_KEY: &str = "spark.hadoop.fs.s3a.endpoint.region";

/// The repark-native spelling of the same override, accepted as a synonym.
pub(crate) const REPARK_S3A_REGION_CONFIG_KEY: &str = "repark.hadoop.fs.s3a.endpoint.region";

pub(crate) const S3A_ENDPOINT_CONFIG_KEY: &str = "spark.hadoop.fs.s3a.endpoint";

pub(crate) const REPARK_S3A_ENDPOINT_CONFIG_KEY: &str = "repark.hadoop.fs.s3a.endpoint";

pub(crate) const S3A_PATH_STYLE_CONFIG_KEY: &str = "spark.hadoop.fs.s3a.path.style.access";

pub(crate) const REPARK_S3A_PATH_STYLE_CONFIG_KEY: &str = "repark.hadoop.fs.s3a.path.style.access";

pub(crate) const S3A_SSL_ENABLED_CONFIG_KEY: &str = "spark.hadoop.fs.s3a.connection.ssl.enabled";

pub(crate) const REPARK_S3A_SSL_ENABLED_CONFIG_KEY: &str =
    "repark.hadoop.fs.s3a.connection.ssl.enabled";

#[derive(Debug)]
pub(crate) struct S3EndpointConfig {
    pub(crate) endpoint: Option<String>,
    pub(crate) path_style_access: Option<bool>,
    pub(crate) ssl_enabled: Option<bool>,
}

fn dual_key_raw(
    config: &HashMap<String, String>,
    repark_key: &str,
    spark_key: &str,
) -> Result<Option<String>> {
    let repark = config.get(repark_key);
    let spark = config.get(spark_key);
    match (repark, spark) {
        (Some(left), Some(right)) if left != right => Err(Error::Config(format!(
            "conflicting S3 config: `{repark_key}` and `{spark_key}` set different values"
        ))),
        (Some(value), _) | (_, Some(value)) => Ok(Some(value.clone())),
        (None, None) => Ok(None),
    }
}

fn parse_endpoint_bool(key: &str, raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "t" | "y" => Ok(true),
        "false" | "0" | "no" | "f" | "n" => Ok(false),
        _ => Err(Error::Config(format!(
            "S3 config `{key}` expects a boolean, got {raw:?}"
        ))),
    }
}

pub(crate) fn resolve_endpoint_config(
    config: &HashMap<String, String>,
) -> Result<Option<S3EndpointConfig>> {
    let endpoint = dual_key_raw(
        config,
        REPARK_S3A_ENDPOINT_CONFIG_KEY,
        S3A_ENDPOINT_CONFIG_KEY,
    )?;
    let path_style = dual_key_raw(
        config,
        REPARK_S3A_PATH_STYLE_CONFIG_KEY,
        S3A_PATH_STYLE_CONFIG_KEY,
    )?;
    let ssl = dual_key_raw(
        config,
        REPARK_S3A_SSL_ENABLED_CONFIG_KEY,
        S3A_SSL_ENABLED_CONFIG_KEY,
    )?;
    if endpoint.is_none() && path_style.is_none() && ssl.is_none() {
        return Ok(None);
    }
    let path_key = config
        .get(REPARK_S3A_PATH_STYLE_CONFIG_KEY)
        .map_or(S3A_PATH_STYLE_CONFIG_KEY, |_| {
            REPARK_S3A_PATH_STYLE_CONFIG_KEY
        });
    let ssl_key = config
        .get(REPARK_S3A_SSL_ENABLED_CONFIG_KEY)
        .map_or(S3A_SSL_ENABLED_CONFIG_KEY, |_| {
            REPARK_S3A_SSL_ENABLED_CONFIG_KEY
        });
    Ok(Some(S3EndpointConfig {
        endpoint,
        path_style_access: path_style
            .map(|raw| parse_endpoint_bool(path_key, &raw))
            .transpose()?,
        ssl_enabled: ssl
            .map(|raw| parse_endpoint_bool(ssl_key, &raw))
            .transpose()?,
    }))
}

pub(crate) fn resolve_endpoint_from_dump(
    rows: &[(String, String, String)],
) -> Result<Option<S3EndpointConfig>> {
    let config: HashMap<String, String> = rows
        .iter()
        .map(|row| (row.0.clone(), row.1.clone()))
        .collect();
    resolve_endpoint_config(&config)
}

pub(crate) fn has_s3_endpoint_keys(config: &HashMap<String, String>) -> bool {
    config.contains_key(S3A_ENDPOINT_CONFIG_KEY)
        || config.contains_key(REPARK_S3A_ENDPOINT_CONFIG_KEY)
        || config.contains_key(S3A_PATH_STYLE_CONFIG_KEY)
        || config.contains_key(REPARK_S3A_PATH_STYLE_CONFIG_KEY)
        || config.contains_key(S3A_SSL_ENABLED_CONFIG_KEY)
        || config.contains_key(REPARK_S3A_SSL_ENABLED_CONFIG_KEY)
}

/// Whether `scheme` is one `RePark` routes to an S3 object store.
pub(crate) fn is_s3_scheme(scheme: &str) -> bool {
    S3_SCHEMES.contains(&scheme)
}

/// The `(scheme, bucket)` of an `s3`/`s3a` URL, or `None` for any other path.
pub(crate) fn parse_s3_bucket(path: &str) -> Option<(String, String)> {
    let url = Url::parse(path).ok()?;
    let scheme = url.scheme();
    if !is_s3_scheme(scheme) {
        return None;
    }
    let bucket = url.host_str()?;
    if bucket.is_empty() {
        return None;
    }
    Some((scheme.to_string(), bucket.to_string()))
}

pub(crate) async fn resolve_s3_prefix_for_read(context: &SessionContext, path: &str) -> String {
    if path.ends_with('/') {
        return path.to_string();
    }
    let Some((_scheme, bucket)) = parse_s3_bucket(path) else {
        return path.to_string();
    };
    let Ok(url) = Url::parse(path) else {
        return path.to_string();
    };
    let prefix_text = url.path().trim_matches('/').to_string();
    if prefix_text.is_empty() {
        return path.to_string();
    }
    let Ok(prefix) = ObjectPath::from_url_path(&prefix_text) else {
        return path.to_string();
    };
    let Ok(store_url) = ObjectStoreUrl::parse(format!("s3://{bucket}")) else {
        return path.to_string();
    };
    let Ok(store) = context.runtime_env().object_store(&store_url) else {
        return path.to_string();
    };
    if store.head(&prefix).await.is_ok() {
        return path.to_string();
    }
    let mut listed = store.list(Some(&prefix));
    match listed.next().await {
        Some(Ok(_)) => format!("{path}/"),
        _ => path.to_string(),
    }
}

/// Bridges the resolved `aws-config` credential provider into `object_store`.
#[derive(Debug)]
pub(crate) struct AwsConfigCredentialProvider {
    inner: SharedCredentialsProvider,
}

impl AwsConfigCredentialProvider {
    /// Wrap an already-resolved SDK credentials provider.
    pub(crate) fn new(inner: SharedCredentialsProvider) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl CredentialProvider for AwsConfigCredentialProvider {
    type Credential = AwsCredential;

    /// Resolve the current AWS credentials from the wrapped chain and adapt them to `object_store`.
    /// # Errors
    /// Returns an `object_store` `Generic` error if the provider cannot supply credentials.
    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let credentials = self.inner.provide_credentials().await.map_err(|source| {
            object_store::Error::Generic {
                store: "S3",
                source: Box::new(source),
            }
        })?;
        Ok(Arc::new(AwsCredential {
            key_id: credentials.access_key_id().to_string(),
            secret_key: credentials.secret_access_key().to_string(),
            token: credentials.session_token().map(str::to_string),
        }))
    }
}

/// Build an authenticated Amazon S3 store from the SDK config and an optional region.
/// # Errors
/// Returns [`Error::DataFusion`] if region, credentials, or the store builder fail.
pub(crate) fn build_amazon_s3_store(
    bucket: &str,
    region_override: Option<&str>,
    endpoint: Option<&S3EndpointConfig>,
    sdk_config: &SdkConfig,
) -> Result<Arc<dyn ObjectStore>> {
    let region = region_override
        .map(str::to_string)
        .or_else(|| {
            sdk_config
                .region()
                .map(|region| region.as_ref().to_string())
        })
        .ok_or_else(|| {
            Error::DataFusion(format!(
                "no AWS region resolved for s3 bucket '{bucket}': set AWS_REGION, a \
                 shared-config region, or the '{REPARK_S3A_REGION_CONFIG_KEY}' (or \
                 '{S3A_REGION_CONFIG_KEY}') session config"
            ))
        })?;

    let credentials_provider = sdk_config.credentials_provider().ok_or_else(|| {
        Error::DataFusion(format!(
            "no AWS credentials provider resolved for s3 bucket '{bucket}' (the aws-config default \
             chain found no env vars, shared-credentials file, or instance role)"
        ))
    })?;
    let bridge = Arc::new(AwsConfigCredentialProvider::new(credentials_provider));

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_credentials(bridge);
    if let Some(config) = endpoint {
        if let Some(url) = &config.endpoint {
            builder = builder.with_endpoint(url);
        }
        if let Some(path_style) = config.path_style_access {
            builder = builder.with_virtual_hosted_style_request(!path_style);
        }
        if let Some(ssl_enabled) = config.ssl_enabled {
            builder = builder.with_allow_http(!ssl_enabled);
        }
    }
    let store = builder.build().map_err(|source| {
        Error::DataFusion(format!(
            "failed to build s3 store for bucket '{bucket}': {source}"
        ))
    })?;
    Ok(Arc::new(store))
}

/// Register one object store for `bucket` under both `s3://` and `s3a://` in the `RuntimeEnv`.
/// # Errors
/// Returns [`Error::DataFusion`] if a `scheme://bucket` URL cannot be built (bad bucket name).
pub(crate) fn register_bucket_store(
    context: &SessionContext,
    bucket: &str,
    store: &Arc<dyn ObjectStore>,
) -> Result<()> {
    let runtime = context.runtime_env();
    for scheme in S3_SCHEMES {
        let url = Url::parse(&format!("{scheme}://{bucket}")).map_err(|source| {
            Error::DataFusion(format!(
                "invalid s3 url for bucket '{bucket}' ({scheme}): {source}"
            ))
        })?;
        runtime.register_object_store(&url, store.clone());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_credential_types::Credentials;
    use object_store::PutPayload;
    use object_store::memory::InMemory;

    /// A static credentials provider for the adapter unit test.
    fn static_provider() -> SharedCredentialsProvider {
        SharedCredentialsProvider::new(Credentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            Some("session-token".to_string()),
            None,
            "static-test",
        ))
    }

    #[test]
    fn is_s3_scheme_matches_s3_and_s3a_only() {
        assert!(is_s3_scheme("s3"));
        assert!(is_s3_scheme("s3a"));
        assert!(!is_s3_scheme("file"));
        assert!(!is_s3_scheme("gs"));
        assert!(!is_s3_scheme("s3n")); // the legacy Hadoop scheme is intentionally NOT routed
    }

    #[test]
    fn parse_s3_bucket_extracts_scheme_and_host() {
        assert_eq!(
            parse_s3_bucket("s3://warehouse-bucket/a/b.parquet"),
            Some(("s3".to_string(), "warehouse-bucket".to_string()))
        );
        assert_eq!(
            parse_s3_bucket("s3a://example-team-bronze-bucket-v1/bronze/e/2026-07-09.parquet"),
            Some((
                "s3a".to_string(),
                "example-team-bronze-bucket-v1".to_string()
            ))
        );
    }

    #[test]
    fn parse_s3_bucket_ignores_non_s3_paths() {
        // Local + relative + other-scheme paths are pass-through (None), never treated as S3.
        assert_eq!(parse_s3_bucket("/tmp/local.parquet"), None);
        assert_eq!(parse_s3_bucket("data/local.parquet"), None);
        assert_eq!(parse_s3_bucket("file:///tmp/x.parquet"), None);
        assert_eq!(parse_s3_bucket("gs://bucket/x.parquet"), None);
        // A host-less s3 URL has no bucket to register.
        assert_eq!(parse_s3_bucket("s3:///x.parquet"), None);
    }

    #[test]
    fn endpoint_config_absent_without_keys() {
        let config = HashMap::new();
        assert!(resolve_endpoint_config(&config).unwrap().is_none());
        assert!(!has_s3_endpoint_keys(&config));
        assert!(resolve_endpoint_from_dump(&[]).unwrap().is_none());
    }

    #[test]
    fn endpoint_config_reads_spark_spellings() {
        let config = HashMap::from([
            (
                S3A_ENDPOINT_CONFIG_KEY.to_string(),
                "http://127.0.0.1:5599".to_string(),
            ),
            (S3A_PATH_STYLE_CONFIG_KEY.to_string(), "true".to_string()),
            (S3A_SSL_ENABLED_CONFIG_KEY.to_string(), "false".to_string()),
        ]);
        assert!(has_s3_endpoint_keys(&config));
        let resolved = resolve_endpoint_config(&config).unwrap().unwrap();
        assert_eq!(resolved.endpoint.as_deref(), Some("http://127.0.0.1:5599"));
        assert_eq!(resolved.path_style_access, Some(true));
        assert_eq!(resolved.ssl_enabled, Some(false));
    }

    #[test]
    fn endpoint_config_reads_repark_spellings() {
        let config = HashMap::from([
            (
                REPARK_S3A_ENDPOINT_CONFIG_KEY.to_string(),
                "http://minio:9000".to_string(),
            ),
            (
                REPARK_S3A_PATH_STYLE_CONFIG_KEY.to_string(),
                "TRUE".to_string(),
            ),
            (
                REPARK_S3A_SSL_ENABLED_CONFIG_KEY.to_string(),
                "False".to_string(),
            ),
        ]);
        let resolved = resolve_endpoint_config(&config).unwrap().unwrap();
        assert_eq!(resolved.endpoint.as_deref(), Some("http://minio:9000"));
        assert_eq!(resolved.path_style_access, Some(true));
        assert_eq!(resolved.ssl_enabled, Some(false));
    }

    #[test]
    fn endpoint_config_partial_keys_leave_rest_unset() {
        let config = HashMap::from([(
            S3A_ENDPOINT_CONFIG_KEY.to_string(),
            "https://s3.example.com".to_string(),
        )]);
        let resolved = resolve_endpoint_config(&config).unwrap().unwrap();
        assert_eq!(resolved.endpoint.as_deref(), Some("https://s3.example.com"));
        assert_eq!(resolved.path_style_access, None);
        assert_eq!(resolved.ssl_enabled, None);
    }

    #[test]
    fn endpoint_config_conflicting_dual_keys_refuse() {
        for (repark_key, spark_key) in [
            (REPARK_S3A_ENDPOINT_CONFIG_KEY, S3A_ENDPOINT_CONFIG_KEY),
            (REPARK_S3A_PATH_STYLE_CONFIG_KEY, S3A_PATH_STYLE_CONFIG_KEY),
            (
                REPARK_S3A_SSL_ENABLED_CONFIG_KEY,
                S3A_SSL_ENABLED_CONFIG_KEY,
            ),
        ] {
            let config = HashMap::from([
                (repark_key.to_string(), "left".to_string()),
                (spark_key.to_string(), "right".to_string()),
            ]);
            let error = resolve_endpoint_config(&config).unwrap_err();
            let message = error.to_string();
            assert!(message.contains(repark_key), "got: {message}");
            assert!(message.contains(spark_key), "got: {message}");
        }
    }

    #[test]
    fn endpoint_config_matching_dual_keys_agree() {
        let config = HashMap::from([
            (
                REPARK_S3A_PATH_STYLE_CONFIG_KEY.to_string(),
                "true".to_string(),
            ),
            (S3A_PATH_STYLE_CONFIG_KEY.to_string(), "true".to_string()),
        ]);
        let resolved = resolve_endpoint_config(&config).unwrap().unwrap();
        assert_eq!(resolved.path_style_access, Some(true));
    }

    #[test]
    fn endpoint_config_garbage_bool_refuses_naming_key() {
        let config = HashMap::from([(
            S3A_SSL_ENABLED_CONFIG_KEY.to_string(),
            "sometimes".to_string(),
        )]);
        let error = resolve_endpoint_config(&config).unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains(S3A_SSL_ENABLED_CONFIG_KEY),
            "got: {message}"
        );
        assert!(message.contains("sometimes"), "got: {message}");
    }

    #[test]
    fn endpoint_config_resolves_from_conf_dump_rows() {
        let rows = vec![
            (
                "spark.sql.session.timeZone".to_string(),
                "UTC".to_string(),
                "builder".to_string(),
            ),
            (
                REPARK_S3A_ENDPOINT_CONFIG_KEY.to_string(),
                "http://127.0.0.1:5599".to_string(),
                "builder".to_string(),
            ),
            (
                S3A_SSL_ENABLED_CONFIG_KEY.to_string(),
                "false".to_string(),
                "builder".to_string(),
            ),
        ];
        let resolved = resolve_endpoint_from_dump(&rows).unwrap().unwrap();
        assert_eq!(resolved.endpoint.as_deref(), Some("http://127.0.0.1:5599"));
        assert_eq!(resolved.path_style_access, None);
        assert_eq!(resolved.ssl_enabled, Some(false));
    }

    async fn prefix_context(keys: &[&str]) -> SessionContext {
        let context = SessionContext::new();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for key in keys {
            store
                .put(&ObjectPath::from(*key), PutPayload::from(vec![1u8]))
                .await
                .unwrap();
        }
        register_bucket_store(&context, "bucket", &store).unwrap();
        context
    }

    #[tokio::test]
    async fn slashless_prefix_with_children_reads_as_a_directory() {
        let context = prefix_context(&["cell/p/part-0.parquet"]).await;
        assert_eq!(
            resolve_s3_prefix_for_read(&context, "s3://bucket/cell/p").await,
            "s3://bucket/cell/p/"
        );
        assert_eq!(
            resolve_s3_prefix_for_read(&context, "s3a://bucket/cell/p").await,
            "s3a://bucket/cell/p/"
        );
    }

    #[tokio::test]
    async fn exact_object_reads_keep_their_spelling() {
        let context = prefix_context(&["cell/p/part-0.parquet", "cell/exact.csv"]).await;
        assert_eq!(
            resolve_s3_prefix_for_read(&context, "s3://bucket/cell/p/part-0.parquet").await,
            "s3://bucket/cell/p/part-0.parquet"
        );
        assert_eq!(
            resolve_s3_prefix_for_read(&context, "s3a://bucket/cell/exact.csv").await,
            "s3a://bucket/cell/exact.csv"
        );
    }

    #[tokio::test]
    async fn exact_object_wins_over_children_with_the_same_prefix() {
        let context = prefix_context(&["cell/p", "cell/p/part-0.parquet"]).await;
        assert_eq!(
            resolve_s3_prefix_for_read(&context, "s3://bucket/cell/p").await,
            "s3://bucket/cell/p"
        );
    }

    #[tokio::test]
    async fn sibling_prefixes_missing_keys_and_non_s3_paths_stay_untouched() {
        let context = prefix_context(&["cell/p2/part-0.parquet"]).await;
        assert_eq!(
            resolve_s3_prefix_for_read(&context, "s3://bucket/cell/p").await,
            "s3://bucket/cell/p"
        );
        assert_eq!(
            resolve_s3_prefix_for_read(&context, "s3://bucket/nowhere").await,
            "s3://bucket/nowhere"
        );
        assert_eq!(
            resolve_s3_prefix_for_read(&context, "s3://other/cell/p").await,
            "s3://other/cell/p"
        );
        assert_eq!(
            resolve_s3_prefix_for_read(&context, "s3://bucket/cell/p/").await,
            "s3://bucket/cell/p/"
        );
        assert_eq!(
            resolve_s3_prefix_for_read(&context, "s3://bucket").await,
            "s3://bucket"
        );
        assert_eq!(
            resolve_s3_prefix_for_read(&context, "/tmp/cell/p").await,
            "/tmp/cell/p"
        );
    }

    #[tokio::test]
    async fn credential_bridge_maps_static_credentials() {
        // The adapter must surface the wrapped provider's key/secret/token as an `AwsCredential`.
        let bridge = AwsConfigCredentialProvider::new(static_provider());
        let credential = bridge
            .get_credential()
            .await
            .expect("static provider yields credentials without a network call");
        assert_eq!(credential.key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(
            credential.secret_key,
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        );
        assert_eq!(credential.token.as_deref(), Some("session-token"));
    }
}
