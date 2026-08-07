//! E-2 gate tests: conditional finalize-time AWS resolution (design §2).
//!
//! Both sides of the conditional guard, AWS-free by construction:
//! - an OFFLINE session (memory catalog only, no AWS signal) must NEVER resolve the AWS SDK
//!   chain at finalize — no IMDS probe for offline work;
//! - an S3-path read on a session that never resolved must fail LOUD, naming the finalize step
//!   and the opt-in conf — never a silent lazy chain resolution at query time (the v1 env-read
//!   this edit removes).

use std::sync::Arc;

use tempfile::TempDir;

use crate::ReparkSession;

/// GATE (offline side) — a session whose catalog config is a memory catalog only, with no
/// S3-region conf and no explicit opt-in, finalizes WITHOUT resolving the AWS SDK chain.
#[tokio::test]
async fn offline_session_finalize_never_resolves_aws_sdk_config() {
    let wh = TempDir::new().unwrap();
    let session = ReparkSession::builder()
        .config("spark.sql.catalog.ice.type", "memory")
        .config(
            "spark.sql.catalog.ice.warehouse",
            wh.path().to_string_lossy().to_string(),
        )
        .build()
        .unwrap();
    session
        .register_configured_catalogs()
        .await
        .expect("the memory catalog registers offline");
    assert!(
        !session.testing_aws_sdk_config_resolved(),
        "an offline session (no AWS-backed catalog spec, no region conf, no opt-in) must \
         never pay the AWS chain resolution / IMDS probe at finalize"
    );
    // A completely config-free session is equally offline.
    let bare = ReparkSession::new().unwrap();
    bare.register_configured_catalogs().await.unwrap();
    assert!(!bare.testing_aws_sdk_config_resolved());
}

/// GATE (read side) — an `s3://` read on a session that never resolved the SDK config fails
/// loud NAMING the missing step (`register_configured_catalogs`) and the explicit opt-in conf,
/// before any store build / network touch.
#[tokio::test]
async fn unfinalized_s3_read_fails_loud_naming_the_finalize_step() {
    let session = ReparkSession::new().unwrap();
    let error = session
        .read_parquet("s3://example-bucket/some/key.parquet")
        .await
        .expect_err("an S3 read without a resolved SDK config must refuse");
    let message = error.to_string();
    assert!(
        message.contains("register_configured_catalogs"),
        "the refusal must name the finalize step, got: {message}"
    );
    assert!(
        message.contains("repark.aws.enable"),
        "the refusal must name the explicit opt-in conf, got: {message}"
    );
    assert!(
        !session.testing_aws_sdk_config_resolved(),
        "the refused read must not have resolved the chain as a side effect"
    );
}

/// GATE (late signal side, AWS-free) — the LATE config map's S3-region conf belongs to the
/// late AWS-signal set exactly as it does at `build()`: a conflicting dual-spelling pair fails
/// loud (naming both keys) BEFORE any chain resolution, proving
/// `register_late_configured_catalogs` consults the region-conf signal class. The single-key
/// positive side is not driven end-to-end here for the same reason as the build-time tests —
/// a signaled finalize would touch the real credential chain.
#[tokio::test]
async fn late_config_region_conf_is_consulted_as_an_aws_signal() {
    let session = ReparkSession::new().unwrap();
    session.register_configured_catalogs().await.unwrap();
    let late = std::collections::HashMap::from([
        (
            "repark.hadoop.fs.s3a.endpoint.region".to_string(),
            "us-east-1".to_string(),
        ),
        (
            "spark.hadoop.fs.s3a.endpoint.region".to_string(),
            "us-west-2".to_string(),
        ),
    ]);
    let error = session
        .register_late_configured_catalogs(&late)
        .await
        .expect_err("a conflicting dual-spelling region pair must fail loud in the late path");
    assert!(
        error.to_string().contains("conflicting S3 region config"),
        "the refusal must be the dual-key conflict error, got: {error}"
    );
    assert!(
        !session.testing_aws_sdk_config_resolved(),
        "the conflict must refuse before any AWS SDK chain resolution"
    );
}

/// GATE (signal side, still AWS-free) — the explicit opt-in conf counts as an AWS signal: the
/// builder records it, and the UNFINALIZED read still refuses (resolution happens only at
/// finalize, never lazily at read time). The finalize itself is NOT run here — it would touch
/// the real credential chain.
#[tokio::test]
async fn opt_in_session_still_requires_finalize_before_s3_reads() {
    let session = ReparkSession::builder()
        .config("repark.aws.enable", "true")
        .build()
        .unwrap();
    let error = session
        .read_parquet("s3a://example-bucket/key.parquet")
        .await
        .expect_err("opt-in without finalize must still refuse (no lazy query-time resolution)");
    assert!(error.to_string().contains("register_configured_catalogs"));
    assert!(!session.testing_aws_sdk_config_resolved());
    // The in-memory test-store seam bypasses the gate exactly as v1's scheme-routing e2e does:
    // a pre-registered bucket store needs no SDK config.
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    session
        .register_s3_bucket_store_for_test("example-bucket", &store)
        .unwrap();
}
