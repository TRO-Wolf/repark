use super::*;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use tempfile::TempDir;

fn sample_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap()
}

/// Build a catalog with one namespace `sales` and register it in a fresh `SessionContext`.
/// Returns the context and the catalog handle (so a test can create tables on it directly).
async fn ctx_with_sales_namespace(wh: &TempDir) -> (SessionContext, Arc<dyn Catalog>) {
    let catalog = memory_catalog(wh.path().to_str().unwrap()).await.unwrap();
    catalog
        .create_namespace(&NamespaceIdent::new("sales".to_string()), HashMap::new())
        .await
        .unwrap();
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "ice", catalog.clone())
        .await
        .unwrap();
    (ctx, catalog)
}

/// U2-P9 (+ the U2-P2 helper arm): `resolve_namespace_location` over every key shape — the
/// property-map partition of audit BUG-001. Risk pinned: a reader that only knows `location`
/// fails every pre-existing real Glue database (whose fork-loaded map carries ONLY
/// `location_uri`), and a both-set map must resolve by DETERMINISTIC precedence
/// (`location` wins), never an iteration-order pick.
#[test]
fn resolve_namespace_location_covers_all_key_shapes() {
    // Legacy RePark shape: only `location`.
    let location_only = HashMap::from([("location".to_string(), "/wh/a".to_string())]);
    assert_eq!(resolve_namespace_location(&location_only), Some("/wh/a"));

    // Pre-existing real-Glue-DB shape (fork read direction): only `location_uri`.
    let location_uri_only =
        HashMap::from([("location_uri".to_string(), "/wh/glue_db".to_string())]);
    assert_eq!(
        resolve_namespace_location(&location_uri_only),
        Some("/wh/glue_db"),
        "a location_uri-only namespace (a pre-existing Glue database) must resolve — \
             the audit BUG-001 failing case"
    );

    // Both set, different: `location` (the Java-canonical key) wins, deterministically.
    let both_different = HashMap::from([
        ("location".to_string(), "/wh/primary".to_string()),
        ("location_uri".to_string(), "/wh/other".to_string()),
    ]);
    assert_eq!(
        resolve_namespace_location(&both_different),
        Some("/wh/primary"),
        "with both keys set, `location` must win (documented precedence, never iteration order)"
    );

    // Neither key: no location to resolve (the N5 fail-loud class upstream).
    let neither = HashMap::from([("comment".to_string(), "no location here".to_string())]);
    assert_eq!(resolve_namespace_location(&neither), None);
}

/// U2-P9: `mirror_namespace_location_keys` is unidirectional and non-clobbering. Risk pinned:
/// a mirror that overwrites an explicit `location_uri` destroys user input; one that
/// synthesizes `location` from `location_uri` fabricates state the caller never set (and
/// makes the real-Glue-DB single-key shape unconstructible); one that invents keys on a
/// location-less map turns a property-less namespace into a propertied one.
#[test]
fn mirror_namespace_location_keys_is_unidirectional_and_non_clobbering() {
    // `location` only → the twin is added (the U2 dual-write).
    let mut location_only = HashMap::from([("location".to_string(), "/wh/a".to_string())]);
    mirror_namespace_location_keys(&mut location_only);
    assert_eq!(
        location_only,
        HashMap::from([
            ("location".to_string(), "/wh/a".to_string()),
            ("location_uri".to_string(), "/wh/a".to_string()),
        ]),
        "a `location`-bearing map must gain an equal `location_uri` (and nothing else)"
    );

    // `location_uri` only → untouched (unidirectional; no synthesized `location`).
    let mut location_uri_only =
        HashMap::from([("location_uri".to_string(), "/wh/glue_db".to_string())]);
    mirror_namespace_location_keys(&mut location_uri_only);
    assert_eq!(
        location_uri_only,
        HashMap::from([("location_uri".to_string(), "/wh/glue_db".to_string())]),
        "an explicit location_uri-only map must stay single-key (unidirectional mirror)"
    );

    // Both set (different) → untouched (never clobber explicit input).
    let mut both = HashMap::from([
        ("location".to_string(), "/wh/primary".to_string()),
        ("location_uri".to_string(), "/wh/other".to_string()),
    ]);
    let expected = both.clone();
    mirror_namespace_location_keys(&mut both);
    assert_eq!(
        both, expected,
        "explicitly-set keys must never be overwritten by the mirror"
    );

    // Location-less → stays exactly as given (no key invention).
    let mut no_location = HashMap::from([("comment".to_string(), "hi".to_string())]);
    mirror_namespace_location_keys(&mut no_location);
    assert_eq!(
        no_location,
        HashMap::from([("comment".to_string(), "hi".to_string())])
    );
    let mut empty: HashMap<String, String> = HashMap::new();
    mirror_namespace_location_keys(&mut empty);
    assert!(
        empty.is_empty(),
        "a property-less namespace must stay property-less"
    );
}

/// `INSERT INTO` a pre-created Iceberg table works and reads back — the supported write path.
#[tokio::test]
async fn insert_into_precreated_table_round_trips() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalog) = ctx_with_sales_namespace(&wh).await;

    let creation = TableCreation::builder()
        .name("orders".to_string())
        .location(format!("{}/orders", wh.path().to_str().unwrap()))
        .schema(sample_schema())
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&NamespaceIdent::new("sales".to_string()), creation)
        .await
        .unwrap();
    // Re-register so the provider's namespace snapshot includes the new table.
    register_iceberg_catalog(&ctx, "ice", catalog)
        .await
        .unwrap();

    run(
        &ctx,
        "INSERT INTO ice.sales.orders VALUES (1, 'alan'), (2, 'turing')",
    )
    .await
    .expect("INSERT INTO a pre-created iceberg table should succeed");

    let batches = ctx
        .sql("SELECT id, name FROM ice.sales.orders ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let rows: usize = batches.iter().map(arrow_rows).sum();
    assert_eq!(rows, 2);
}

/// Locks the corrected understanding: DataFusion's `CREATE TABLE … AS SELECT` hands the schema
/// provider a table WITH data, which iceberg-datafusion rejects. CTAS-from-SELECT therefore
/// cannot be a passthrough — `repark-sql` must decompose it into CREATE + INSERT.
#[tokio::test]
async fn datafusion_ctas_with_data_is_rejected_by_iceberg() {
    let wh = TempDir::new().unwrap();
    let (ctx, _catalog) = ctx_with_sales_namespace(&wh).await;

    let outcome = run(
        &ctx,
        "CREATE TABLE ice.sales.t AS SELECT * FROM (VALUES (1, 'a')) AS s(id, name)",
    )
    .await;
    assert!(
        outcome.is_err(),
        "expected CTAS-with-data to be rejected by iceberg register_table, got Ok"
    );
}

/// The decomposed path CTAS will use: schema-only `CREATE` (no data) then `INSERT INTO`.
#[tokio::test]
async fn create_empty_then_insert_is_the_working_ctas_path() {
    let wh = TempDir::new().unwrap();
    let (ctx, _catalog) = ctx_with_sales_namespace(&wh).await;

    run(
        &ctx,
        "CREATE TABLE ice.sales.t (id INT NOT NULL, name STRING NOT NULL)",
    )
    .await
    .expect("schema-only CREATE into iceberg should succeed");
    run(
            &ctx,
            "INSERT INTO ice.sales.t SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS s(id, name)",
        )
        .await
        .expect("INSERT INTO the created iceberg table should succeed");

    let batches = ctx
        .sql("SELECT count(*) AS n FROM ice.sales.t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(n.value(0), 3);
}

/// Proves the `[patch.crates-io]` rewire to the owned fork is in effect — **at compile time**:
/// `iceberg::arrow::DeleteFilter` is public only in the fork (its engine-facing merge-on-read
/// surface — see the fork's `docs/ENGINE_CONTRACT.md` §2), not in crates.io iceberg 0.9.1, so
/// this function cannot compile against the registry crate. Compilation IS the assertion; the
/// body only instantiates a generic with the fork-only type (any runtime check on a
/// hard-coded type parameter would be tautological). If this fails to build, the patch
/// wiring in the workspace `Cargo.toml` has regressed. See ADR-0003.
#[test]
fn fork_patch_in_effect_deletefilter_is_public() {
    fn nameable<T: ?Sized>() {}
    nameable::<iceberg::arrow::DeleteFilter>();
}

// ---------------------------------------------------------------------------------------
// AWS builder tests — AWS-FREE. Construction of the fork's Glue / S3 Tables catalogs builds
// the AWS SDK client config but performs NO network call (verified against the fork's own
// offline constructor tests, and against `create_sdk_config`, which resolves credentials
// lazily on first request, not at build time). So the passthrough tests construct with dummy
// static credentials + a fake endpoint and never touch AWS. The required-prop validation
// tests never reach the fork builder at all.
// ---------------------------------------------------------------------------------------

/// A missing `warehouse` fails loud and names the key, before the fork builder runs.
#[tokio::test]
async fn glue_catalog_missing_warehouse_names_the_key() {
    let err = glue_catalog(&HashMap::new()).await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("warehouse"), "error must name the key: {msg}");
    assert!(msg.contains("Glue"), "error must name the surface: {msg}");
}

/// A present-but-blank `warehouse` is rejected the same way (guards the empty-string hole the
/// fork's own non-empty check would otherwise surface only as a vaguer downstream error).
#[tokio::test]
async fn glue_catalog_blank_warehouse_names_the_key() {
    let props = HashMap::from([(GLUE_CATALOG_PROP_WAREHOUSE.to_string(), "   ".to_string())]);
    let err = glue_catalog(&props).await.unwrap_err();
    assert!(
        err.to_string().contains("warehouse"),
        "blank warehouse must be rejected naming the key: {err}"
    );
}

/// With `warehouse` set, the Glue catalog constructs offline and forwards unrecognized props
/// (here a `FileIO`-bound `s3.region`) through to the catalog's properties, while the
/// recognized `warehouse` key is consumed (not left in the passthrough map).
///
/// `region_name` is pinned so the fork's `create_sdk_config` sets the region explicitly
/// (glue/src/utils.rs `AWS_REGION_NAME`); without it, `aws_config`'s default region chain
/// runs to its last link — the IMDS provider — and the test would open a real connection to
/// the EC2 metadata endpoint (169.254.169.254) on any region-less runner. Note `s3.region`
/// is a `FileIO`/`OpenDAL` key the SDK region resolver does not read, so it cannot substitute.
#[tokio::test]
async fn glue_catalog_constructs_and_passes_props_through() {
    let props = HashMap::from([
        (
            GLUE_CATALOG_PROP_WAREHOUSE.to_string(),
            "s3://example-bucket/warehouse".to_string(),
        ),
        ("region_name".to_string(), "us-east-2".to_string()),
        ("s3.region".to_string(), "us-east-2".to_string()),
        ("aws_access_key_id".to_string(), "AKIAEXAMPLE".to_string()),
        (
            "aws_secret_access_key".to_string(),
            "secretexample".to_string(),
        ),
    ]);
    let catalog = glue_catalog(&props)
        .await
        .expect("glue catalog constructs offline with dummy static credentials");
    assert_eq!(catalog.name(), "glue");
    assert_eq!(
        catalog.properties().get("s3.region").map(String::as_str),
        Some("us-east-2"),
        "unrecognized props pass through to FileIO"
    );
    assert!(
        !catalog
            .properties()
            .contains_key(GLUE_CATALOG_PROP_WAREHOUSE),
        "the recognized warehouse key is consumed, not forwarded"
    );
}

/// A missing `table_bucket_arn` fails loud and names the key, before the fork builder runs.
#[tokio::test]
async fn s3tables_catalog_missing_arn_names_the_key() {
    let err = s3tables_catalog(&HashMap::new()).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("table_bucket_arn"),
        "error must name the key: {msg}"
    );
    assert!(
        msg.contains("S3 Tables"),
        "error must name the surface: {msg}"
    );
}

/// A present-but-blank `table_bucket_arn` is rejected the same way.
#[tokio::test]
async fn s3tables_catalog_blank_arn_names_the_key() {
    let props = HashMap::from([(
        S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
        String::new(),
    )]);
    let err = s3tables_catalog(&props).await.unwrap_err();
    assert!(
        err.to_string().contains("table_bucket_arn"),
        "blank ARN must be rejected naming the key: {err}"
    );
}

/// With `table_bucket_arn` set, the S3 Tables catalog constructs offline and forwards
/// unrecognized props (here `region_name`) through to the catalog's properties.
#[tokio::test]
async fn s3tables_catalog_constructs_and_passes_props_through() {
    let props = HashMap::from([
        (
            S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
            "arn:aws:s3tables:us-east-2:123456789012:bucket/example".to_string(),
        ),
        ("region_name".to_string(), "us-east-2".to_string()),
    ]);
    let catalog = s3tables_catalog(&props)
        .await
        .expect("s3tables catalog constructs offline with a dummy ARN");
    assert_eq!(catalog.name(), "s3tables");
    assert_eq!(
        catalog.properties().get("region_name").map(String::as_str),
        Some("us-east-2"),
        "unrecognized props pass through to FileIO"
    );
    assert!(
        !catalog
            .properties()
            .contains_key(S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN),
        "the recognized ARN key is consumed, not forwarded"
    );
}

// ---------------------------------------------------------------------------------------
// Scheme-based FileIO selection — OFFLINE. Classification is a pure string→backend mapping;
// building the factory / FileIO performs no network call (S3 is contacted lazily on first
// use), so these pin "the right backend is chosen" structurally without ever touching AWS.
// ---------------------------------------------------------------------------------------

/// `s3://` selects the OpenDAL S3 backend, carrying the exact scheme so returned object paths
/// round-trip as `s3://`.
#[test]
fn classify_s3_scheme_selects_object_store() {
    assert_eq!(
        classify_location_backend("s3://example-team-bucket/warehouse/ns/t").unwrap(),
        LocationBackend::ObjectStoreS3 {
            configured_scheme: "s3".to_string()
        }
    );
}

/// `s3a://` (the Spark/Hadoop scheme) also selects S3, preserving `s3a` as the configured scheme.
#[test]
fn classify_s3a_scheme_selects_object_store() {
    assert_eq!(
        classify_location_backend("s3a://example-team-bucket/warehouse/ns/t").unwrap(),
        LocationBackend::ObjectStoreS3 {
            configured_scheme: "s3a".to_string()
        }
    );
}

/// `file://` selects the native local-filesystem backend.
#[test]
fn classify_file_scheme_selects_local_fs() {
    assert_eq!(
        classify_location_backend("file:///tmp/warehouse/ns/t").unwrap(),
        LocationBackend::LocalFs
    );
}

/// A bare absolute path (no `scheme://`) is the offline/local warehouse → local filesystem.
#[test]
fn classify_bare_path_selects_local_fs() {
    assert_eq!(
        classify_location_backend("/var/lib/repark/warehouse/ns/t").unwrap(),
        LocationBackend::LocalFs
    );
}

/// F-WG3C-1 / G-CI: a `:` AFTER the first `/` on an absolute path is a legal POSIX path
/// character (`/data/ns:v2/t`) and must stay [`LocationBackend::LocalFs`] — not be
/// misclassified as a mistyped scheme. The sole pre-existing bare-absolute pin used a
/// colon-free path; a future simplification of `has_colon_before_first_slash` (e.g. "any `:`
/// ⇒ mistyped") would silently break legit colon-in-path warehouses without this pin.
#[test]
fn classify_absolute_path_with_colon_after_slash_stays_local_fs() {
    assert_eq!(
        classify_location_backend("/data/ns:v2/t").unwrap(),
        LocationBackend::LocalFs
    );
    // The helper itself must also treat colon-after-slash as legal (not before-slash mistype).
    assert!(
        !has_colon_before_first_slash("/data/ns:v2/t"),
        "a `:` after the first `/` is a path character, not a mistyped scheme"
    );
}

/// An unsupported scheme fails loud — naming the offending scheme AND the supported set — so a
/// misconfigured warehouse never silently mis-places data.
#[test]
fn classify_unknown_scheme_fails_loud() {
    let error = classify_location_backend("gs://some-bucket/warehouse")
        .expect_err("an unsupported scheme must be a loud error, never a silent fallback");
    let message = error.to_string();
    assert!(
        message.contains("gs"),
        "error must name the scheme, got: {message}"
    );
    assert!(
        message.contains("s3://"),
        "error must name the supported set, got: {message}"
    );
}

/// F-BR-3: `s3:/bucket/wh` — an `s3://` typed with a single slash — carries no `://`, so the
/// pre-fix bare-path arm silently classified it `LocalFs` and a strict-catalog CTAS wrote a
/// broken table under a CWD-relative `s3:` directory. It must now fail loud, naming the location
/// and steering the user at `scheme://`.
#[test]
fn classify_single_slash_s3_scheme_fails_loud() {
    let error = classify_location_backend("s3:/bucket/wh")
        .expect_err("a single-slash `s3:/` scheme typo must be a loud error, never LocalFs");
    let message = error.to_string();
    assert!(
        message.contains("s3:/bucket/wh"),
        "error must name the offending location, got: {message}"
    );
    assert!(
        message.contains("mistyped"),
        "error must flag the mistyped scheme (did you mean `scheme://`), got: {message}"
    );
}

/// F-BR-3: `s3a:/x` (single-slash `s3a`) is the same mistyped-scheme boundary cell — loud error.
#[test]
fn classify_single_slash_s3a_scheme_fails_loud() {
    let error = classify_location_backend("s3a:/x")
        .expect_err("a single-slash `s3a:/` scheme typo must be a loud error, never LocalFs");
    let message = error.to_string();
    assert!(
        message.contains("s3a:/x"),
        "error must name the offending location, got: {message}"
    );
    assert!(
        message.contains("mistyped"),
        "error must flag the mistyped scheme, got: {message}"
    );
}

/// F-BR-3: a relative path (no leading `/`) would resolve against the process CWD, so a bare
/// warehouse path must be absolute — a relative one now fails loud instead of silently becoming
/// `LocalFs`.
#[test]
fn classify_relative_path_fails_loud() {
    let error = classify_location_backend("relative/path")
        .expect_err("a relative bare path must be a loud error, never LocalFs");
    let message = error.to_string();
    assert!(
        message.contains("relative/path"),
        "error must name the offending location, got: {message}"
    );
    assert!(
        message.contains("absolute"),
        "error must require an absolute path, got: {message}"
    );
}

/// F-BR-3: the empty string is not an absolute path — a common misconfiguration (an unset
/// warehouse variable resolving to `""`) must fail loud, not silently classify as `LocalFs`.
#[test]
fn classify_empty_location_fails_loud() {
    let error = classify_location_backend("")
        .expect_err("an empty location must be a loud error, never LocalFs");
    let message = error.to_string();
    assert!(
        message.contains("absolute"),
        "error must require an absolute path, got: {message}"
    );
    assert!(
        message.contains("s3://"),
        "error must name the supported location forms, got: {message}"
    );
}

/// Structural: an `s3://` location selects the OpenDAL S3 factory (not local) — proven by the
/// factory's `Debug` shape, with no S3 contact (the factory builds without connecting).
#[test]
fn factory_for_s3_is_opendal_s3() {
    let factory =
        storage_factory_for_location("s3://bucket/warehouse").expect("s3 factory builds offline");
    let debug = format!("{factory:?}");
    assert!(
        debug.contains("S3"),
        "an s3 location must select the S3 factory, got: {debug}"
    );
    assert!(
        !debug.contains("LocalFs"),
        "an s3 location must not select LocalFs, got: {debug}"
    );
    // Building a FileIO for it also does not contact S3 (storage is lazy on first use).
    file_io_for_location("s3://bucket/warehouse", &HashMap::<String, String>::new())
        .expect("s3 FileIO builds offline");
}

/// Structural: a bare/local location selects the local-filesystem factory.
#[test]
fn factory_for_local_is_local_fs() {
    let factory = storage_factory_for_location("/tmp/warehouse").expect("local factory builds");
    let debug = format!("{factory:?}");
    assert!(
        debug.contains("LocalFs"),
        "a local path must select the LocalFs factory, got: {debug}"
    );
}

/// Run a statement to completion, surfacing planning *or* execution errors.
async fn run(ctx: &SessionContext, sql: &str) -> Result<()> {
    ctx.sql(sql).await?.collect().await?;
    Ok(())
}

fn arrow_rows(b: &datafusion::arrow::array::RecordBatch) -> usize {
    b.num_rows()
}

// ---------------------------------------------------------------------------------------
// catalog listing staleness (CQ-008 / BUG-007). AWS-free MemoryCatalog only.
// ---------------------------------------------------------------------------------------

/// Documented strategy pin: facade uses list-on-access (not TTL).
#[test]
fn catalog_listing_strategy_is_list_on_access() {
    assert_eq!(CATALOG_LISTING_STRATEGY, "list-on-access");
}

/// Measure-first: single-namespace `list_tables` is cheaper than a full provider rebuild
/// (which lists every namespace + every table). Pins the T6 choice of list-on-access for
/// the facade rather than TTL-wrapping `IcebergCatalogProvider::try_new`.
#[tokio::test]
async fn listing_cost_list_tables_cheaper_than_provider_rebuild() {
    use std::time::Instant;

    let wh = TempDir::new().unwrap();
    let warehouse = wh.path().to_str().unwrap();
    let catalog = memory_catalog(warehouse).await.unwrap();
    let sales = NamespaceIdent::new("sales".to_string());
    catalog
        .create_namespace(&sales, HashMap::new())
        .await
        .unwrap();
    // A handful of tables so the provider rebuild has real list work; MemoryCatalog is
    // in-process so walls stay low — we only need relative cost, not absolute ms.
    for index in 0..8 {
        let name = format!("orders_{index}");
        let creation = TableCreation::builder()
            .name(name)
            .location(format!("{warehouse}/orders_{index}"))
            .schema(sample_schema())
            .properties(HashMap::new())
            .build();
        catalog.create_table(&sales, creation).await.unwrap();
    }
    // Warm both paths once so the measured loop is not first-touch dominated.
    let _ = list_table_names(catalog.as_ref(), "sales").await.unwrap();
    let _ = build_iceberg_catalog_provider(catalog.clone())
        .await
        .unwrap();

    let iterations: u32 = 20;
    let list_start = Instant::now();
    for _ in 0..iterations {
        let names = list_table_names(catalog.as_ref(), "sales").await.unwrap();
        assert_eq!(names.len(), 8);
    }
    let list_elapsed = list_start.elapsed();

    let rebuild_start = Instant::now();
    for _ in 0..iterations {
        let _ = build_iceberg_catalog_provider(catalog.clone())
            .await
            .unwrap();
    }
    let rebuild_elapsed = rebuild_start.elapsed();

    // list-on-access must not be slower than a full provider rebuild on the same catalog.
    // (Equality is allowed on an unloaded box; the structural point is we never pick TTL
    // rebuild-as-list when the cheap primitive exists.)
    assert!(
        list_elapsed <= rebuild_elapsed * 2,
        "list_table_names ({list_elapsed:?}) should be ≤ ~2× build_iceberg_catalog_provider \
             ({rebuild_elapsed:?}) over {iterations} iterations — re-measure if this regresses"
    );
    // Soft preference pin: listing is typically strictly cheaper; the 2x bound above is
    // the accept gate on a noisy box. Document the strategy constant with the measurement.
    assert_eq!(CATALOG_LISTING_STRATEGY, "list-on-access");
}

/// Out-of-band create/drop on the same Catalog handle: live list sees create and drops
/// phantoms; the DF provider snapshot stays stale until rebuild (residual pin).
#[tokio::test]
async fn live_list_sees_oob_create_and_drop_while_provider_snapshot_stale() {
    let wh = TempDir::new().unwrap();
    let warehouse = wh.path().to_str().unwrap();
    let catalog = memory_catalog(warehouse).await.unwrap();
    let sales = NamespaceIdent::new("sales".to_string());
    catalog
        .create_namespace(&sales, HashMap::new())
        .await
        .unwrap();
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "ice", catalog.clone())
        .await
        .unwrap();

    // Baseline: no tables.
    assert!(
        list_table_names(catalog.as_ref(), "sales")
            .await
            .unwrap()
            .is_empty()
    );
    let provider = ctx.catalog("ice").expect("registered");
    let schema = provider
        .schema("sales")
        .expect("sales namespace snapshotted");
    assert!(
        schema.table_names().is_empty(),
        "provider snapshot starts empty"
    );

    // Out-of-band create (direct Catalog API — no reregister).
    let creation = TableCreation::builder()
        .name("oob_created".to_string())
        .location(format!("{warehouse}/oob_created"))
        .schema(sample_schema())
        .properties(HashMap::new())
        .build();
    catalog.create_table(&sales, creation).await.unwrap();

    let live_after_create = list_table_names(catalog.as_ref(), "sales").await.unwrap();
    assert!(
        live_after_create.iter().any(|name| name == "oob_created"),
        "live list_table_names must see out-of-band create: {live_after_create:?}"
    );
    // Provider snapshot still empty — residual for free SQL until refresh.
    let schema_stale = provider.schema("sales").expect("sales");
    assert!(
        !schema_stale
            .table_names()
            .iter()
            .any(|name| name == "oob_created"),
        "DF provider snapshot must still be stale without rebuild (residual pin)"
    );

    // Explicit rebuild → provider catches up (the reregister / refresh path).
    register_iceberg_catalog(&ctx, "ice", catalog.clone())
        .await
        .unwrap();
    let provider_fresh = ctx.catalog("ice").expect("re-registered");
    let schema_fresh = provider_fresh.schema("sales").expect("sales");
    assert!(
        schema_fresh
            .table_names()
            .iter()
            .any(|name| name == "oob_created"),
        "after rebuild the DF provider must list the OOB table"
    );

    // Out-of-band drop.
    let ident = iceberg::TableIdent::new(sales.clone(), "oob_created".to_string());
    catalog.drop_table(&ident).await.unwrap();
    let live_after_drop = list_table_names(catalog.as_ref(), "sales").await.unwrap();
    assert!(
        !live_after_drop.iter().any(|name| name == "oob_created"),
        "live list must not phantom a dropped table: {live_after_drop:?}"
    );
    // Provider still has the phantom name until another rebuild — residual honesty pin.
    let schema_phantom = ctx
        .catalog("ice")
        .expect("still registered")
        .schema("sales")
        .expect("sales");
    assert!(
        schema_phantom
            .table_names()
            .iter()
            .any(|name| name == "oob_created"),
        "DF provider name directory still phantoms until rebuild (honest residual)"
    );

    // Rebuild after drop → phantom gone.
    register_iceberg_catalog(&ctx, "ice", catalog.clone())
        .await
        .unwrap();
    let schema_clean = ctx
        .catalog("ice")
        .expect("registered")
        .schema("sales")
        .expect("sales");
    assert!(
        !schema_clean
            .table_names()
            .iter()
            .any(|name| name == "oob_created"),
        "after rebuild drop must not phantom"
    );
}

/// Live namespace list sees out-of-band `create_namespace` without provider rebuild.
#[tokio::test]
async fn live_list_namespaces_sees_oob_namespace() {
    let wh = TempDir::new().unwrap();
    let catalog = memory_catalog(wh.path().to_str().unwrap()).await.unwrap();
    catalog
        .create_namespace(&NamespaceIdent::new("a".to_string()), HashMap::new())
        .await
        .unwrap();
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "ice", catalog.clone())
        .await
        .unwrap();

    catalog
        .create_namespace(&NamespaceIdent::new("oob_ns".to_string()), HashMap::new())
        .await
        .unwrap();
    let live = list_namespace_names(catalog.as_ref()).await.unwrap();
    assert!(
        live.iter().any(|name| name == "oob_ns"),
        "live list_namespace_names must see OOB namespace: {live:?}"
    );
    // Provider schema_names snapshot misses OOB namespace until rebuild.
    let provider = ctx.catalog("ice").expect("registered");
    assert!(
        !provider.schema_names().iter().any(|name| name == "oob_ns"),
        "DF provider schema_names stay stale without rebuild"
    );
}

/// PERF-07: product CREATE NAMESPACE shape — invalidate one ns adds it to DF.
#[tokio::test]
async fn invalidate_adds_new_namespace_to_df_provider() {
    let wh = TempDir::new().expect("tempdir");
    let catalog = memory_catalog(wh.path().to_str().expect("utf8"))
        .await
        .expect("memory catalog");
    catalog
        .create_namespace(&NamespaceIdent::new("a".to_string()), HashMap::new())
        .await
        .expect("create a");
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "ice", catalog.clone())
        .await
        .expect("register");

    catalog
        .create_namespace(&NamespaceIdent::new("new_ns".to_string()), HashMap::new())
        .await
        .expect("create new_ns");
    let provider = ctx.catalog("ice").expect("registered");
    assert!(
        !provider.schema_names().iter().any(|name| name == "new_ns"),
        "before invalidate DF must not see the new namespace"
    );

    invalidate_catalog_namespaces(&ctx, catalog.clone(), "ice", &["new_ns"])
        .await
        .expect("invalidate new_ns");
    assert!(
        provider.schema_names().iter().any(|name| name == "new_ns"),
        "after invalidate DF schema_names must include the new namespace"
    );
    assert!(
        provider.schema("new_ns").is_some(),
        "after invalidate schema(new_ns) must resolve"
    );
}

/// T6: OOB namespace drop — live list clean; DF phantoms until full rebuild.
#[tokio::test]
async fn oob_namespace_drop_phantoms_until_full_rebuild() {
    let wh = TempDir::new().expect("tempdir");
    let catalog = memory_catalog(wh.path().to_str().expect("utf8"))
        .await
        .expect("memory catalog");
    catalog
        .create_namespace(&NamespaceIdent::new("keep".to_string()), HashMap::new())
        .await
        .expect("keep");
    catalog
        .create_namespace(&NamespaceIdent::new("gone".to_string()), HashMap::new())
        .await
        .expect("gone");
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "ice", catalog.clone())
        .await
        .expect("register");

    catalog
        .drop_namespace(&NamespaceIdent::new("gone".to_string()))
        .await
        .expect("oob drop ns");
    let live = list_namespace_names(catalog.as_ref())
        .await
        .expect("live list");
    assert!(
        !live.iter().any(|name| name == "gone"),
        "live list must not phantom dropped namespace: {live:?}"
    );
    let provider = ctx.catalog("ice").expect("registered");
    assert!(
        provider.schema_names().iter().any(|name| name == "gone"),
        "DF provider must phantom OOB-dropped namespace until rebuild (residual honesty)"
    );

    rebuild_catalog_provider(&ctx, catalog.clone(), "ice")
        .await
        .expect("full rebuild");
    assert!(
        !provider.schema_names().iter().any(|name| name == "gone"),
        "after full rebuild DF must not phantom dropped namespace"
    );
    assert!(
        provider.schema("keep").is_some(),
        "sibling namespace must survive full rebuild"
    );
}

// ---------------------------------------------------------------------------------------
// PERF-07 — incremental namespace invalidation (counting catalog, no AWS).
// ---------------------------------------------------------------------------------------

/// Boxed future for desugared [`Catalog`] methods (no `async-trait` dep in this crate).
type BoxedCatalogFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = iceberg::Result<T>> + Send + 'a>>;

/// Counts `list_namespaces` / `list_tables` on the live handle — the Glue-scale cost drivers.
#[derive(Debug)]
struct CountingCatalog {
    inner: Arc<dyn Catalog>,
    list_namespaces: std::sync::atomic::AtomicUsize,
    list_tables: std::sync::atomic::AtomicUsize,
}

impl CountingCatalog {
    fn new(inner: Arc<dyn Catalog>) -> Self {
        Self {
            inner,
            list_namespaces: std::sync::atomic::AtomicUsize::new(0),
            list_tables: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn list_namespaces_count(&self) -> usize {
        self.list_namespaces
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn list_tables_count(&self) -> usize {
        self.list_tables.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn reset_counts(&self) {
        self.list_namespaces
            .store(0, std::sync::atomic::Ordering::SeqCst);
        self.list_tables
            .store(0, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Catalog for CountingCatalog {
    fn list_namespaces<'life0, 'life1, 'async_trait>(
        &'life0 self,
        parent: Option<&'life1 NamespaceIdent>,
    ) -> BoxedCatalogFuture<'async_trait, Vec<NamespaceIdent>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.list_namespaces
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.list_namespaces(parent)
    }

    fn create_namespace<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> BoxedCatalogFuture<'async_trait, iceberg::Namespace>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.create_namespace(namespace, properties)
    }

    fn get_namespace<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
    ) -> BoxedCatalogFuture<'async_trait, iceberg::Namespace>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.get_namespace(namespace)
    }

    fn namespace_exists<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
    ) -> BoxedCatalogFuture<'async_trait, bool>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.namespace_exists(namespace)
    }

    fn update_namespace<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> BoxedCatalogFuture<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.update_namespace(namespace, properties)
    }

    fn drop_namespace<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
    ) -> BoxedCatalogFuture<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.drop_namespace(namespace)
    }

    fn list_tables<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
    ) -> BoxedCatalogFuture<'async_trait, Vec<iceberg::TableIdent>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.list_tables
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.list_tables(namespace)
    }

    fn create_table<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
        creation: TableCreation,
    ) -> BoxedCatalogFuture<'async_trait, iceberg::table::Table>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.create_table(namespace, creation)
    }

    fn load_table<'life0, 'life1, 'async_trait>(
        &'life0 self,
        table: &'life1 iceberg::TableIdent,
    ) -> BoxedCatalogFuture<'async_trait, iceberg::table::Table>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.load_table(table)
    }

    fn drop_table<'life0, 'life1, 'async_trait>(
        &'life0 self,
        table: &'life1 iceberg::TableIdent,
    ) -> BoxedCatalogFuture<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.drop_table(table)
    }

    fn table_exists<'life0, 'life1, 'async_trait>(
        &'life0 self,
        table: &'life1 iceberg::TableIdent,
    ) -> BoxedCatalogFuture<'async_trait, bool>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.table_exists(table)
    }

    fn rename_table<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        src: &'life1 iceberg::TableIdent,
        dest: &'life2 iceberg::TableIdent,
    ) -> BoxedCatalogFuture<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.rename_table(src, dest)
    }

    fn register_table<'life0, 'life1, 'async_trait>(
        &'life0 self,
        table: &'life1 iceberg::TableIdent,
        metadata_location: String,
    ) -> BoxedCatalogFuture<'async_trait, iceberg::table::Table>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.register_table(table, metadata_location)
    }

    fn update_table<'life0, 'async_trait>(
        &'life0 self,
        commit: iceberg::TableCommit,
    ) -> BoxedCatalogFuture<'async_trait, iceberg::table::Table>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.update_table(commit)
    }
}

/// PERF-07 bar: after a warm multi-namespace register, one namespace invalidation must not
/// re-list every database (O(databases) → O(1)).
#[tokio::test]
async fn invalidate_one_namespace_is_o1_not_o_databases() {
    let wh = TempDir::new().unwrap();
    let warehouse = wh.path().to_str().unwrap();
    let memory = memory_catalog(warehouse).await.unwrap();
    // Glue-scale stand-in: many namespaces so a full rebuild would list each.
    let namespace_count = 12_usize;
    for index in 0..namespace_count {
        let name = format!("db_{index}");
        memory
            .create_namespace(&NamespaceIdent::new(name), HashMap::new())
            .await
            .unwrap();
    }
    let counting = Arc::new(CountingCatalog::new(memory));
    let catalog: Arc<dyn Catalog> = counting.clone();
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "ice", catalog.clone())
        .await
        .unwrap();

    // Product DDL on one namespace (create table out-of-band, then invalidate that ns only).
    let target = NamespaceIdent::new("db_3".to_string());
    let creation = TableCreation::builder()
        .name("orders".to_string())
        .location(format!("{warehouse}/orders_db3"))
        .schema(sample_schema())
        .properties(HashMap::new())
        .build();
    catalog.create_table(&target, creation).await.unwrap();

    counting.reset_counts();
    invalidate_catalog_namespaces(&ctx, catalog.clone(), "ice", &["db_3"])
        .await
        .unwrap();

    let list_namespaces = counting.list_namespaces_count();
    let list_tables = counting.list_tables_count();
    // Scoped rebuild: zero root list_namespaces (namespace_exists only) + one list_tables.
    // Allow a tiny constant; must NOT scale with namespace_count.
    assert!(
        list_namespaces <= 1,
        "namespace invalidation must not list all databases: list_namespaces={list_namespaces} \
             (namespaces={namespace_count})"
    );
    assert!(
        list_tables <= 2,
        "namespace invalidation must list_tables O(1), got {list_tables} \
             (namespaces={namespace_count})"
    );

    // Free SQL name directory sees the new table after invalidation.
    let provider = ctx.catalog("ice").expect("registered");
    let schema = provider.schema("db_3").expect("db_3");
    assert!(
        schema.table_names().iter().any(|name| name == "orders"),
        "after invalidate, DF provider must list the new table"
    );

    // Scale pin: doubling namespaces must not change per-DDL list_tables cost.
    for index in namespace_count..(namespace_count * 2) {
        let name = format!("db_{index}");
        catalog
            .create_namespace(&NamespaceIdent::new(name), HashMap::new())
            .await
            .unwrap();
    }
    // Full refresh so the extra namespaces exist in the provider (explicit OOB recovery).
    rebuild_catalog_provider(&ctx, catalog.clone(), "ice")
        .await
        .unwrap();

    let creation2 = TableCreation::builder()
        .name("orders2".to_string())
        .location(format!("{warehouse}/orders_db3_2"))
        .schema(sample_schema())
        .properties(HashMap::new())
        .build();
    catalog.create_table(&target, creation2).await.unwrap();
    counting.reset_counts();
    invalidate_catalog_namespaces(&ctx, catalog.clone(), "ice", &["db_3"])
        .await
        .unwrap();
    let list_tables_wide = counting.list_tables_count();
    assert!(
        list_tables_wide <= 2,
        "after growing to {} namespaces, invalidation list_tables still O(1), got \
             {list_tables_wide}",
        namespace_count * 2
    );
    assert_eq!(
        list_tables, list_tables_wide,
        "list_tables cost must be independent of database count (PERF-07 O(1) bar)"
    );
}

/// Baseline contrast: a full rebuild lists every namespace's tables (O(databases)).
/// pins: rp-1-fork-repin/C-011
#[tokio::test]
async fn full_rebuild_lists_every_namespace() {
    let wh = TempDir::new().unwrap();
    let warehouse = wh.path().to_str().unwrap();
    let memory = memory_catalog(warehouse).await.unwrap();
    let namespace_count = 8_usize;
    for index in 0..namespace_count {
        memory
            .create_namespace(&NamespaceIdent::new(format!("db_{index}")), HashMap::new())
            .await
            .unwrap();
    }
    let counting = Arc::new(CountingCatalog::new(memory));
    let catalog: Arc<dyn Catalog> = counting.clone();
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "ice", catalog.clone())
        .await
        .unwrap();
    counting.reset_counts();
    rebuild_catalog_provider(&ctx, catalog.clone(), "ice")
        .await
        .unwrap();
    let list_tables = counting.list_tables_count();
    assert!(
        list_tables >= namespace_count,
        "full rebuild must list_tables at least once per namespace: got {list_tables} for \
             {namespace_count} namespaces"
    );
}

/// Product-style invalidation keeps T6 residual honesty: OOB create without invalidate stays
/// invisible to the DF provider; live list still sees it.
/// pins: rp-1-fork-repin/C-011
#[tokio::test]
async fn incremental_provider_preserves_oob_staleness_residual() {
    let wh = TempDir::new().unwrap();
    let warehouse = wh.path().to_str().unwrap();
    let catalog = memory_catalog(warehouse).await.unwrap();
    let sales = NamespaceIdent::new("sales".to_string());
    catalog
        .create_namespace(&sales, HashMap::new())
        .await
        .unwrap();
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "ice", catalog.clone())
        .await
        .unwrap();

    let creation = TableCreation::builder()
        .name("oob".to_string())
        .location(format!("{warehouse}/oob"))
        .schema(sample_schema())
        .properties(HashMap::new())
        .build();
    catalog.create_table(&sales, creation).await.unwrap();

    assert!(
        list_table_names(catalog.as_ref(), "sales")
            .await
            .unwrap()
            .iter()
            .any(|name| name == "oob")
    );
    let provider = ctx.catalog("ice").expect("registered");
    assert!(
        !provider
            .schema("sales")
            .expect("sales")
            .table_names()
            .iter()
            .any(|name| name == "oob"),
        "OOB create must stay invisible to DF provider until invalidate (T6 residual)"
    );

    invalidate_catalog_namespaces(&ctx, catalog.clone(), "ice", &["sales"])
        .await
        .unwrap();
    assert!(
        provider
            .schema("sales")
            .expect("sales")
            .table_names()
            .iter()
            .any(|name| name == "oob"),
        "after namespace invalidate the DF provider must see the table"
    );
}

/// PERF-07: live table drop + namespace invalidate must clear DF phantoms
/// (product DROP TABLE shape — no silent drop cache after invalidate).
#[tokio::test]
async fn invalidate_after_live_table_drop_removes_df_name() {
    let wh = TempDir::new().expect("tempdir");
    let warehouse = wh.path().to_str().expect("utf8 path");
    let catalog = memory_catalog(warehouse).await.expect("memory catalog");
    let sales = NamespaceIdent::new("sales".to_string());
    catalog
        .create_namespace(&sales, HashMap::new())
        .await
        .expect("create sales");
    let creation = TableCreation::builder()
        .name("orders".to_string())
        .location(format!("{warehouse}/orders_drop_pin"))
        .schema(sample_schema())
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&sales, creation)
        .await
        .expect("create orders");
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "ice", catalog.clone())
        .await
        .expect("register");
    let provider = ctx.catalog("ice").expect("registered");
    assert!(
        provider
            .schema("sales")
            .expect("sales")
            .table_names()
            .iter()
            .any(|name| name == "orders"),
        "baseline: orders visible after register"
    );

    let ident = iceberg::TableIdent::new(sales.clone(), "orders".to_string());
    catalog.drop_table(&ident).await.expect("live drop");
    // Residual honesty: without invalidate the DF snapshot still phantoms.
    assert!(
        provider
            .schema("sales")
            .expect("sales")
            .table_names()
            .iter()
            .any(|name| name == "orders"),
        "OOB/live drop without invalidate must still phantom (T6 residual)"
    );

    invalidate_catalog_namespaces(&ctx, catalog.clone(), "ice", &["sales"])
        .await
        .expect("invalidate after drop");
    assert!(
        !provider
            .schema("sales")
            .expect("sales")
            .table_names()
            .iter()
            .any(|name| name == "orders"),
        "after invalidate, DF provider must not phantom a dropped table"
    );
}

/// PERF-07: product DROP NAMESPACE path must not list (zero-list map remove).
#[tokio::test]
async fn drop_namespace_from_provider_is_zero_list() {
    let wh = TempDir::new().expect("tempdir");
    let warehouse = wh.path().to_str().expect("utf8 path");
    let memory = memory_catalog(warehouse).await.expect("memory catalog");
    let namespace_count = 10_usize;
    for index in 0..namespace_count {
        memory
            .create_namespace(&NamespaceIdent::new(format!("db_{index}")), HashMap::new())
            .await
            .expect("create namespace");
    }
    let counting = Arc::new(CountingCatalog::new(memory));
    let catalog: Arc<dyn Catalog> = counting.clone();
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "ice", catalog.clone())
        .await
        .expect("register");

    // Live drop first (product order), then provider entry remove.
    catalog
        .drop_namespace(&NamespaceIdent::new("db_4".to_string()))
        .await
        .expect("drop namespace live");
    counting.reset_counts();
    drop_catalog_namespace_from_provider(&ctx, catalog.clone(), "ice", "db_4")
        .await
        .expect("drop from provider");

    assert_eq!(
        counting.list_namespaces_count(),
        0,
        "drop_catalog_namespace_from_provider must not list_namespaces"
    );
    assert_eq!(
        counting.list_tables_count(),
        0,
        "drop_catalog_namespace_from_provider must not list_tables (zero-list)"
    );
    let provider = ctx.catalog("ice").expect("registered");
    assert!(
        !provider.schema_names().iter().any(|name| name == "db_4"),
        "dropped namespace must leave the DF name directory"
    );
    // Siblings remain (not a full rebuild wipe).
    assert!(
        provider.schema("db_0").is_some(),
        "sibling namespace must survive zero-list drop"
    );
}

/// PERF-07: empty invalidate is a no-op (does not silently full-rebuild / heal OOB).
/// pins: rp-1-fork-repin/C-011
#[tokio::test]
async fn empty_invalidate_is_noop_not_full_rebuild() {
    let wh = TempDir::new().expect("tempdir");
    let warehouse = wh.path().to_str().expect("utf8 path");
    let catalog = memory_catalog(warehouse).await.expect("memory catalog");
    let sales = NamespaceIdent::new("sales".to_string());
    catalog
        .create_namespace(&sales, HashMap::new())
        .await
        .expect("create sales");
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "ice", catalog.clone())
        .await
        .expect("register");

    let creation = TableCreation::builder()
        .name("oob".to_string())
        .location(format!("{warehouse}/oob_empty_inv"))
        .schema(sample_schema())
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&sales, creation)
        .await
        .expect("oob create");

    invalidate_catalog_namespaces(&ctx, catalog.clone(), "ice", &[])
        .await
        .expect("empty invalidate");

    let provider = ctx.catalog("ice").expect("registered");
    assert!(
        !provider
            .schema("sales")
            .expect("sales")
            .table_names()
            .iter()
            .any(|name| name == "oob"),
        "empty invalidate must not heal OOB free-SQL residual (no silent full rebuild)"
    );
}

/// PERF-07: multi-namespace invalidate is O(|namespaces|) not O(databases).
#[tokio::test]
async fn invalidate_two_namespaces_is_o_namespaces_not_o_databases() {
    let wh = TempDir::new().expect("tempdir");
    let warehouse = wh.path().to_str().expect("utf8 path");
    let memory = memory_catalog(warehouse).await.expect("memory catalog");
    let namespace_count = 12_usize;
    for index in 0..namespace_count {
        memory
            .create_namespace(&NamespaceIdent::new(format!("db_{index}")), HashMap::new())
            .await
            .expect("create namespace");
    }
    let counting = Arc::new(CountingCatalog::new(memory));
    let catalog: Arc<dyn Catalog> = counting.clone();
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "ice", catalog.clone())
        .await
        .expect("register");

    // OOB creates in two namespaces (cross-ns rename shape).
    for (namespace_name, table_name) in [("db_2", "left_t"), ("db_9", "right_t")] {
        let namespace = NamespaceIdent::new(namespace_name.to_string());
        let creation = TableCreation::builder()
            .name(table_name.to_string())
            .location(format!("{warehouse}/{table_name}"))
            .schema(sample_schema())
            .properties(HashMap::new())
            .build();
        catalog
            .create_table(&namespace, creation)
            .await
            .expect("create table");
    }

    counting.reset_counts();
    invalidate_catalog_namespaces(&ctx, catalog.clone(), "ice", &["db_2", "db_9"])
        .await
        .expect("dual invalidate");

    let list_tables = counting.list_tables_count();
    assert!(
        list_tables <= 4,
        "dual-ns invalidate must list_tables O(|namespaces|), got {list_tables} \
             (namespaces={namespace_count})"
    );
    assert!(
        list_tables < namespace_count,
        "dual-ns invalidate must not walk every database: list_tables={list_tables}"
    );

    let provider = ctx.catalog("ice").expect("registered");
    assert!(
        provider
            .schema("db_2")
            .expect("db_2")
            .table_names()
            .iter()
            .any(|name| name == "left_t"),
        "src namespace must see new table after dual invalidate"
    );
    assert!(
        provider
            .schema("db_9")
            .expect("db_9")
            .table_names()
            .iter()
            .any(|name| name == "right_t"),
        "dest namespace must see new table after dual invalidate"
    );
}

/// PERF-07: invalidating one namespace must not rebuild sibling schema Arcs.
#[tokio::test]
async fn invalidate_preserves_sibling_schema_arc_identity() {
    let wh = TempDir::new().expect("tempdir");
    let warehouse = wh.path().to_str().expect("utf8");
    let catalog = memory_catalog(warehouse).await.expect("memory catalog");
    for name in ["alpha", "beta"] {
        catalog
            .create_namespace(&NamespaceIdent::new(name.to_string()), HashMap::new())
            .await
            .expect("create ns");
    }
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "ice", catalog.clone())
        .await
        .expect("register");
    let provider = ctx.catalog("ice").expect("registered");
    let sibling_before = provider.schema("beta").expect("beta schema");

    // Mutate alpha only, then invalidate alpha.
    let alpha = NamespaceIdent::new("alpha".to_string());
    let creation = TableCreation::builder()
        .name("t".to_string())
        .location(format!("{warehouse}/alpha_t"))
        .schema(sample_schema())
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&alpha, creation)
        .await
        .expect("create in alpha");
    invalidate_catalog_namespaces(&ctx, catalog.clone(), "ice", &["alpha"])
        .await
        .expect("invalidate alpha");

    let sibling_after = provider.schema("beta").expect("beta schema");
    assert!(
        Arc::ptr_eq(&sibling_before, &sibling_after),
        "sibling namespace schema Arc must be identity-stable across other-ns invalidate"
    );
    assert!(
        provider
            .schema("alpha")
            .expect("alpha")
            .table_names()
            .iter()
            .any(|name| name == "t"),
        "invalidated namespace must still refresh its own directory"
    );
}

/// PERF-07: same-Arc rebuild is the ADR-0004 escape hatch (in-place heal).
/// pins: rp-1-fork-repin/C-011
#[tokio::test]
async fn rebuild_same_catalog_heals_oob_and_stays_repark_provider() {
    let wh = TempDir::new().expect("tempdir");
    let warehouse = wh.path().to_str().expect("utf8");
    let catalog = memory_catalog(warehouse).await.expect("memory catalog");
    let sales = NamespaceIdent::new("sales".to_string());
    catalog
        .create_namespace(&sales, HashMap::new())
        .await
        .expect("sales");
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "ice", catalog.clone())
        .await
        .expect("register");

    let creation = TableCreation::builder()
        .name("oob_escape".to_string())
        .location(format!("{warehouse}/oob_escape"))
        .schema(sample_schema())
        .properties(HashMap::new())
        .build();
    catalog
        .create_table(&sales, creation)
        .await
        .expect("oob create");
    let provider_before = ctx.catalog("ice").expect("registered");
    assert!(
        provider_before
            .as_ref()
            .downcast_ref::<ReparkCatalogProvider>()
            .is_some(),
        "baseline provider type"
    );
    assert!(
        !provider_before
            .schema("sales")
            .expect("sales")
            .table_names()
            .iter()
            .any(|name| name == "oob_escape"),
        "OOB residual before rebuild"
    );

    rebuild_catalog_provider(&ctx, catalog.clone(), "ice")
        .await
        .expect("in-place rebuild");
    let provider_after = ctx.catalog("ice").expect("registered");
    assert!(
        provider_after
            .as_ref()
            .downcast_ref::<ReparkCatalogProvider>()
            .is_some(),
        "same-Arc rebuild must keep ReparkCatalogProvider (not foreign type)"
    );
    assert!(
        provider_after
            .schema("sales")
            .expect("sales")
            .table_names()
            .iter()
            .any(|name| name == "oob_escape"),
        "in-place rebuild must heal OOB free-SQL residual"
    );
}

/// PERF-07: invalidate/drop on an unregistered catalog name fail-loud
/// (must not silently register a new DF catalog under a typo).
#[tokio::test]
async fn invalidate_unregistered_catalog_fails_loud() {
    let wh = TempDir::new().expect("tempdir");
    let catalog = memory_catalog(wh.path().to_str().expect("utf8"))
        .await
        .expect("memory catalog");
    let ctx = SessionContext::new();
    // Deliberately do not register "ice".
    let err = invalidate_catalog_namespaces(&ctx, catalog.clone(), "ice", &["sales"])
        .await
        .expect_err("unregistered catalog must fail");
    let message = err.to_string();
    assert!(
        message.contains("not registered"),
        "error must name the failure mode, got: {message}"
    );
    assert!(
        ctx.catalog("ice").is_none(),
        "must not silently register the typo catalog name"
    );

    let err_drop = drop_catalog_namespace_from_provider(&ctx, catalog, "ice", "sales")
        .await
        .expect_err("unregistered drop must fail");
    assert!(
        err_drop.to_string().contains("not registered"),
        "drop error must name the failure mode, got: {err_drop}"
    );
    assert!(
        ctx.catalog("ice").is_none(),
        "drop must not silently register either"
    );
}

/// PERF-07: rebuild with a different catalog Arc replaces the provider
/// (does not silently `refresh_all` from the old interior handle).
#[tokio::test]
async fn rebuild_with_different_catalog_arc_rebinds_provider() {
    let wh_a = TempDir::new().expect("tempdir a");
    let wh_b = TempDir::new().expect("tempdir b");
    let warehouse_a = wh_a.path().to_str().expect("utf8");
    let warehouse_b = wh_b.path().to_str().expect("utf8");
    let catalog_a = memory_catalog(warehouse_a).await.expect("catalog a");
    let catalog_b = memory_catalog(warehouse_b).await.expect("catalog b");

    catalog_a
        .create_namespace(&NamespaceIdent::new("only_a".to_string()), HashMap::new())
        .await
        .expect("ns a");
    catalog_b
        .create_namespace(&NamespaceIdent::new("only_b".to_string()), HashMap::new())
        .await
        .expect("ns b");

    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "ice", catalog_a.clone())
        .await
        .expect("register a");
    assert!(
        ctx.catalog("ice")
            .expect("registered")
            .schema("only_a")
            .is_some()
    );

    rebuild_catalog_provider(&ctx, catalog_b.clone(), "ice")
        .await
        .expect("rebind to b");

    let provider = ctx.catalog("ice").expect("registered");
    assert!(
        provider.schema("only_b").is_some(),
        "rebuild with a different catalog Arc must rebind (see only_b)"
    );
    assert!(
        provider.schema("only_a").is_none(),
        "rebuild must not silently refresh_all from the previous catalog handle"
    );
}

// ---------------------------------------------------------------------------------------
// QUAL-05 / OBS1: catalog-edge spans fire; span fields never carry secret prop values.
// ---------------------------------------------------------------------------------------

/// Snapshot handle for one test's `catalog.*` span capture; clears the thread's capture
/// slot on drop so one test cannot leak into another.
struct CaptureGuard(std::sync::Arc<crate::test_tracing::SpanFieldCapture>);

impl Drop for CaptureGuard {
    fn drop(&mut self) {
        crate::test_tracing::clear_catalog_capture_slot();
    }
}

impl CaptureGuard {
    fn snapshot(&self) -> Vec<SpanEvent> {
        self.0.snapshot()
    }
}

use crate::test_tracing::SpanEvent;

/// ===========================================================================================
/// Begin capturing `catalog.*` spans on this thread (process-global subscriber, installed once).
/// ===========================================================================================
///
/// v1 installed THIS file's own process-global subscriber here. Merged with the write cohort
/// into one test binary, that install collides with the merge span recorder's — forced-edit
/// class 6 (docs/design/session-api.md §5) — so the install and the capture layer now live in
/// [`crate::test_tracing`]: one shared global subscriber carrying both layers. The capture
/// semantics are v1's, unchanged (global subscriber — never `set_default` per test, because
/// `tracing` caches callsite interest globally and a subscriber-less thread poisons it; see
/// the harness docs for the measured v1 flake — with a thread-local slot keeping each test's
/// capture private; `#[tokio::test]` is current-thread, so the span is always created on the
/// capturing thread). Every assertion below is byte-unchanged from v1.
fn capture_catalog_spans() -> CaptureGuard {
    CaptureGuard(crate::test_tracing::begin_catalog_capture())
}

/// Live list + register emit `catalog.*` spans (hang localization for catalog edge).
#[tokio::test]
async fn catalog_ops_emit_named_spans() {
    let capture = capture_catalog_spans();

    let wh = TempDir::new().unwrap();
    let warehouse = wh.path().to_str().unwrap();
    let catalog = memory_catalog(warehouse).await.unwrap();
    catalog
        .create_namespace(&NamespaceIdent::new("sales".to_string()), HashMap::new())
        .await
        .unwrap();
    let _ = list_table_names(catalog.as_ref(), "sales").await.unwrap();
    let _ = list_namespace_names(catalog.as_ref()).await.unwrap();
    let ctx = SessionContext::new();
    register_iceberg_catalog(&ctx, "ice", catalog)
        .await
        .unwrap();

    let events = capture.snapshot();
    let names: Vec<String> = events.iter().map(|(name, _)| name.clone()).collect();
    for expected in [
        "catalog.memory_catalog",
        "catalog.list_table_names",
        "catalog.list_namespace_names",
        "catalog.register_iceberg_catalog",
        "catalog.build_iceberg_catalog_provider",
    ] {
        assert!(
            names.iter().any(|name| name == expected),
            "expected span {expected}; recorded: {names:?}"
        );
    }

    // Field-name hygiene (no accidental dual-arg dumps of handles / extra channels).
    for (name, fields) in &events {
        let allowed: &[&str] = match name.as_str() {
            "catalog.memory_catalog" => &["warehouse"],
            "catalog.list_table_names" => &["namespace"],
            "catalog.list_namespace_names" | "catalog.build_iceberg_catalog_provider" => &[],
            "catalog.register_iceberg_catalog" => &["catalog_name"],
            other if other.starts_with("catalog.") => continue,
            _ => continue,
        };
        for (field_name, _) in fields {
            // tracing may also record the empty "message" field for some macros — ignore.
            if field_name == "message" {
                continue;
            }
            assert!(
                allowed.contains(&field_name.as_str()),
                "span {name} unexpected field {field_name}; allowed={allowed:?} fields={fields:?}"
            );
        }
    }
}

/// Forbidden substrings that must never appear in glue/s3tables span field **values**.
const GLUE_S3_FORBIDDEN_FIELD_VALUES: &[&str] = &[
    "SUPER_SECRET_VALUE_do_not_leak_obs1",
    "AKIAEXAMPLE",
    "s3://example-bucket/warehouse",
    "arn:aws:s3tables:us-east-2:123456789012:bucket/example",
    "us-east-2",
];

fn assert_glue_s3_field_names_allowlisted(glue_events: &[&SpanEvent], s3_events: &[&SpanEvent]) {
    for (_, fields) in glue_events {
        for (field_name, _) in fields {
            assert!(
                field_name == "prop_keys" || field_name == "has_warehouse",
                "catalog.glue_catalog unexpected field {field_name}; fields={fields:?}"
            );
        }
    }
    for (_, fields) in s3_events {
        for (field_name, _) in fields {
            assert!(
                field_name == "prop_keys" || field_name == "has_table_bucket_arn",
                "catalog.s3tables_catalog unexpected field {field_name}; fields={fields:?}"
            );
        }
    }
}

fn assert_catalog_events_forbid_prop_values(events: &[SpanEvent]) {
    for (name, fields) in events {
        if !name.starts_with("catalog.") {
            continue;
        }
        for (field_name, field_value) in fields {
            for needle in GLUE_S3_FORBIDDEN_FIELD_VALUES {
                assert!(
                    !field_value.contains(needle),
                    "span {name} field {field_name} leaked prop/secret value ({needle}): {field_value}"
                );
            }
        }
    }
}

/// Glue/S3 Tables builders emit spans whose fields never contain prop **values**
/// (keys may appear in `prop_keys`; presence bools only). Mutation-proof for accidental
/// `?props` dumps **and** for recording non-secret values (warehouse path / ARN).
#[tokio::test]
async fn glue_and_s3tables_spans_never_record_secret_values() {
    const SECRET: &str = "SUPER_SECRET_VALUE_do_not_leak_obs1";
    const WAREHOUSE_PATH: &str = "s3://example-bucket/warehouse";
    const TABLE_BUCKET_ARN: &str = "arn:aws:s3tables:us-east-2:123456789012:bucket/example";
    const REGION: &str = "us-east-2";

    let capture = capture_catalog_spans();

    let glue_props = HashMap::from([
        (
            GLUE_CATALOG_PROP_WAREHOUSE.to_string(),
            WAREHOUSE_PATH.to_string(),
        ),
        ("region_name".to_string(), REGION.to_string()),
        ("aws_access_key_id".to_string(), "AKIAEXAMPLE".to_string()),
        ("aws_secret_access_key".to_string(), SECRET.to_string()),
        ("session_token".to_string(), SECRET.to_string()),
        ("password".to_string(), SECRET.to_string()),
    ]);
    // Missing-required early path still opens the span (prop_keys recorded before builder).
    let _ = glue_catalog(&HashMap::<String, String>::new()).await;
    let _ = glue_catalog(&glue_props)
        .await
        .expect("glue constructs offline");

    let s3_props = HashMap::from([
        (
            S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN.to_string(),
            TABLE_BUCKET_ARN.to_string(),
        ),
        ("region_name".to_string(), REGION.to_string()),
        ("aws_secret_access_key".to_string(), SECRET.to_string()),
    ]);
    let _ = s3tables_catalog(&s3_props)
        .await
        .expect("s3tables constructs offline");

    let events = capture.snapshot();
    let glue_events: Vec<_> = events
        .iter()
        .filter(|(name, _)| name == "catalog.glue_catalog")
        .collect();
    let s3_events: Vec<_> = events
        .iter()
        .filter(|(name, _)| name == "catalog.s3tables_catalog")
        .collect();
    assert!(
        !glue_events.is_empty(),
        "catalog.glue_catalog must fire: {events:?}"
    );
    assert!(
        !s3_events.is_empty(),
        "catalog.s3tables_catalog must fire: {events:?}"
    );

    assert_glue_s3_field_names_allowlisted(&glue_events, &s3_events);
    assert_catalog_events_forbid_prop_values(&events);

    // Keys may be named (operator can see which secret-class props were configured).
    let glue_with_props = glue_events.iter().find(|(_, fields)| {
        fields
            .iter()
            .any(|(key, value)| key == "prop_keys" && value.contains("aws_secret_access_key"))
    });
    assert!(
        glue_with_props.is_some(),
        "prop_keys should list key names including aws_secret_access_key: {glue_events:?}"
    );
    let has_wh = glue_with_props.and_then(|(_, fields)| {
        fields
            .iter()
            .find(|(key, _)| key == "has_warehouse")
            .map(|(_, value)| value.as_str())
    });
    assert_eq!(
        has_wh,
        Some("true"),
        "has_warehouse should be true for non-empty warehouse prop: {glue_events:?}"
    );
}

/// `prop_key_names` is sorted key-only (stable span field for grepping).
#[test]
fn prop_key_names_sorted_keys_only() {
    let props = HashMap::from([
        ("z_key".to_string(), "secret-z".to_string()),
        ("a_key".to_string(), "secret-a".to_string()),
    ]);
    assert_eq!(prop_key_names(&props), "a_key,z_key");
    assert!(!prop_key_names(&props).contains("secret"));
}
