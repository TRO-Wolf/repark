//! Register `s3://` and `s3a://` object stores for `read_parquet`.
//!
//! AWS credentials resolve once at session finalization through `aws-config`; the same store is
//! registered under both schemes so DataFusion preserves and resolves either URL unchanged.

use std::sync::Arc;

use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use datafusion::prelude::SessionContext;
use object_store::CredentialProvider;
use object_store::ObjectStore;
use object_store::aws::{AmazonS3Builder, AwsCredential};
use url::Url;

use repark_common::{Error, Result};

/// The URL schemes `RePark` treats as S3: `s3` (Iceberg warehouses) and `s3a` (Spark bronze reads).
/// A store built for a bucket is registered under both so either scheme resolves to it.
pub(crate) const S3_SCHEMES: [&str; 2] = ["s3", "s3a"];

/// The Spark/Hadoop config key that overrides the S3 read region (`spark.hadoop.` prefix + the
/// Hadoop `fs.s3a.endpoint.region` key). When absent, the region resolves from the aws-config
/// chain. Kept accepted verbatim — the near-drop-in contract.
pub(crate) const S3A_REGION_CONFIG_KEY: &str = "spark.hadoop.fs.s3a.endpoint.region";

/// The repark-native spelling of the same override, accepted as a synonym (2026-07-12 naming
/// decision). When both keys are set with different values, build fails loud naming both keys (identical values collapse).
pub(crate) const REPARK_S3A_REGION_CONFIG_KEY: &str = "repark.hadoop.fs.s3a.endpoint.region";

/// Whether `scheme` is one `RePark` routes to an S3 object store.
pub(crate) fn is_s3_scheme(scheme: &str) -> bool {
    S3_SCHEMES.contains(&scheme)
}

/// The `(scheme, bucket)` of an `s3`/`s3a` URL, or `None` for any other path (local, relative, a
/// non-S3 scheme). The bucket is the URL host — e.g. `s3a://my-bucket/a/b.parquet` → `("s3a",
/// "my-bucket")`. A non-parseable or host-less string is not an S3 path, so it returns `None` and
/// the caller passes the path through to DataFusion unchanged.
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

/// ===========================================================================================
/// Bridges the resolved `aws-config` credential provider into `object_store`.
/// ===========================================================================================
#[derive(Debug)]
pub(crate) struct AwsConfigCredentialProvider {
    inner: SharedCredentialsProvider,
}

impl AwsConfigCredentialProvider {
    /// Wrap an already-resolved SDK credentials provider (the aws-config default chain, or a static
    /// provider in tests).
    pub(crate) fn new(inner: SharedCredentialsProvider) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl CredentialProvider for AwsConfigCredentialProvider {
    type Credential = AwsCredential;

    /// Resolve the current AWS credentials from the wrapped chain and adapt them to `object_store`.
    ///
    /// # Errors
    /// Returns an `object_store` `Generic` error if the underlying provider fails to supply
    /// credentials (e.g. no profile resolved, IMDS unreachable) — surfaced with the S3 store label.
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

/// ===========================================================================================
/// Build an authenticated Amazon S3 store from the finalized SDK configuration and optional region
/// override. This function performs no AWS resolution itself.
///
/// # Errors
/// Returns [`Error::DataFusion`] if no region can be resolved, the resolved config carries no
/// credentials provider, or the `object_store` builder rejects the configuration.
/// ===========================================================================================
pub(crate) fn build_amazon_s3_store(
    bucket: &str,
    region_override: Option<&str>,
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

    let store = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_credentials(bridge)
        .build()
        .map_err(|source| {
            Error::DataFusion(format!(
                "failed to build s3 store for bucket '{bucket}': {source}"
            ))
        })?;
    Ok(Arc::new(store))
}

/// Register one object store for `bucket` under BOTH `s3://bucket` and `s3a://bucket` in the
/// session's `RuntimeEnv`, so `read_parquet` with either scheme routes to it.
///
/// The same `Arc` is registered under both schemes; an existing store for either URL is replaced.
///
/// # Errors
/// Returns [`Error::DataFusion`] if a `scheme://bucket` URL cannot be constructed (an invalid
/// bucket name).
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

    /// A static credentials provider (no network) for the adapter unit test — the aws-config chain
    /// resolves to one of these under the hood, so a static one exercises the same bridge code.
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

    #[tokio::test]
    async fn credential_bridge_maps_static_credentials() {
        // The adapter must surface the wrapped provider's key/secret/token as an `AwsCredential` —
        // the exact mapping `read_parquet` relies on to sign S3 requests. Static provider = no AWS.
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
