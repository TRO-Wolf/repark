use super::*;

fn hinted_table(versions: &[u64], hint: Option<&str>) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = dir.path().join("metadata");
    std::fs::create_dir_all(&metadata).expect("mkdir");
    for version in versions {
        std::fs::write(metadata.join(format!("v{version}.metadata.json")), b"{}").expect("write");
    }
    if let Some(hint) = hint {
        std::fs::write(metadata.join("version-hint.text"), hint.as_bytes()).expect("write");
    }
    let root = dir.path().to_string_lossy().to_string();
    (dir, root)
}

async fn resolve_table(root: &str) -> Result<String> {
    let file_io =
        repark_iceberg::catalog::file_io_for_location(root, &HashMap::<String, String>::new())
            .expect("file_io");
    resolve_metadata_location(&file_io, root, root).await
}

async fn resolved_name(versions: &[u64], hint: Option<&str>) -> String {
    let (_dir, root) = hinted_table(versions, hint);
    resolve_table(&root)
        .await
        .expect("resolve")
        .replace(&root, "<root>")
}

#[tokio::test]
async fn stale_hint_walks_to_the_next_metadata_file() {
    assert_eq!(
        resolved_name(&[1, 2, 3], Some("2\n")).await,
        "<root>/metadata/v3.metadata.json"
    );
}

#[tokio::test]
async fn stale_hint_walks_past_every_newer_metadata_file() {
    assert_eq!(
        resolved_name(&[1, 2, 3, 4], Some("2")).await,
        "<root>/metadata/v4.metadata.json"
    );
}

#[tokio::test]
async fn current_hint_selects_the_hinted_file() {
    assert_eq!(
        resolved_name(&[1, 2], Some("2")).await,
        "<root>/metadata/v2.metadata.json"
    );
}

#[tokio::test]
async fn hint_walk_stops_at_the_first_missing_version() {
    assert_eq!(
        resolved_name(&[1, 2, 4], Some("2")).await,
        "<root>/metadata/v2.metadata.json"
    );
}

#[tokio::test]
async fn hinted_missing_file_keeps_its_error_when_newer_files_exist() {
    let (_dir, root) = hinted_table(&[1, 3], Some("2"));
    let error = resolve_table(&root).await.expect_err("must refuse");
    assert!(matches!(error, Error::Analysis(_)));
    assert_eq!(
        error.to_string(),
        format!(
            "no Iceberg table found at '{root}': version-hint.text names \
             'v2.metadata.json', which does not exist"
        )
    );
}

#[tokio::test]
async fn no_hint_resolves_the_latest_v_form_by_listing() {
    assert_eq!(
        resolved_name(&[1, 2, 3], None).await,
        "<root>/metadata/v3.metadata.json"
    );
}

#[tokio::test]
async fn hint_walk_resolves_the_walk_limit_exactly() {
    let versions: Vec<u64> = (0..=HINT_WALK_LIMIT).collect();
    assert_eq!(
        resolved_name(&versions, Some("0")).await,
        format!("<root>/metadata/v{HINT_WALK_LIMIT}.metadata.json")
    );
}

#[tokio::test]
async fn hint_walk_beyond_the_limit_refuses_naming_it() {
    let versions: Vec<u64> = (0..=HINT_WALK_LIMIT + 1).collect();
    let (_dir, root) = hinted_table(&versions, Some("0"));
    let error = resolve_table(&root).await.expect_err("must refuse");
    assert!(matches!(error, Error::Analysis(_)));
    assert_eq!(
        error.to_string(),
        format!(
            "cannot resolve the Iceberg table at '{root}': more than 10000 \
             metadata files follow 'v0.metadata.json', the version \
             version-hint.text names"
        )
    );
}

#[tokio::test]
async fn hint_at_java_int_max_does_not_walk_past_it() {
    let (_dir, root) = hinted_table(&[2_147_483_647, 2_147_483_648], Some("2147483647"));
    assert_eq!(
        resolve_table(&root).await.expect("resolve"),
        format!("{root}/metadata/v2147483647.metadata.json")
    );
}

fn named_table(names: &[&str], hint: Option<&str>) -> (tempfile::TempDir, String) {
    let (dir, root) = hinted_table(&[], hint);
    for name in names {
        std::fs::write(dir.path().join("metadata").join(name), b"{}").expect("write");
    }
    (dir, root)
}

async fn resolved_named(names: &[&str], hint: Option<&str>) -> String {
    let (_dir, root) = named_table(names, hint);
    resolve_table(&root)
        .await
        .expect("resolve")
        .replace(&root, "<root>")
}

const MIXED_CODEC: [&str; 4] = [
    "v1.metadata.json",
    "v2.metadata.json",
    "v3.gz.metadata.json",
    "v4.gz.metadata.json",
];

#[tokio::test]
async fn mixed_codec_stale_hint_walks_to_the_newest_gzip_file() {
    assert_eq!(
        resolved_named(&MIXED_CODEC, Some("2")).await,
        "<root>/metadata/v4.gz.metadata.json"
    );
}

#[tokio::test]
async fn mixed_codec_current_hint_selects_the_gzip_file() {
    assert_eq!(
        resolved_named(&MIXED_CODEC, Some("4")).await,
        "<root>/metadata/v4.gz.metadata.json"
    );
}

#[tokio::test]
async fn gzip_table_current_hint_selects_the_gzip_file() {
    let names = [
        "v1.gz.metadata.json",
        "v2.gz.metadata.json",
        "v3.gz.metadata.json",
    ];
    assert_eq!(
        resolved_named(&names, Some("3")).await,
        "<root>/metadata/v3.gz.metadata.json"
    );
}

#[tokio::test]
async fn stale_hint_walks_to_a_legacy_gzip_name() {
    let names = ["v1.metadata.json", "v2.metadata.json.gz"];
    assert_eq!(
        resolved_named(&names, Some("1")).await,
        "<root>/metadata/v2.metadata.json.gz"
    );
}

#[tokio::test]
async fn current_hint_selects_a_legacy_gzip_name() {
    let names = ["v1.metadata.json", "v2.metadata.json.gz"];
    assert_eq!(
        resolved_named(&names, Some("2")).await,
        "<root>/metadata/v2.metadata.json.gz"
    );
}

#[tokio::test]
async fn walk_crosses_from_a_gzip_name_to_a_plain_name() {
    let names = ["v3.gz.metadata.json", "v4.metadata.json"];
    assert_eq!(
        resolved_named(&names, Some("3")).await,
        "<root>/metadata/v4.metadata.json"
    );
}

#[tokio::test]
async fn hinted_missing_version_refuses_when_only_newer_gzip_files_exist() {
    let (_dir, root) = named_table(&["v5.gz.metadata.json", "v6.gz.metadata.json"], Some("2"));
    let error = resolve_table(&root).await.expect_err("must refuse");
    assert!(matches!(error, Error::Analysis(_)));
    assert_eq!(
        error.to_string(),
        format!(
            "no Iceberg table found at '{root}': version-hint.text names \
             'v2.metadata.json', which does not exist"
        )
    );
}

#[tokio::test]
async fn no_hint_listing_picks_the_newest_across_plain_and_gzip_names() {
    assert_eq!(
        resolved_named(&MIXED_CODEC, None).await,
        "<root>/metadata/v4.gz.metadata.json"
    );
}

#[tokio::test]
async fn no_hint_listing_picks_a_legacy_gzip_newest() {
    let names = ["v1.metadata.json", "v2.metadata.json.gz"];
    assert_eq!(
        resolved_named(&names, None).await,
        "<root>/metadata/v2.metadata.json.gz"
    );
}

#[tokio::test]
async fn gzip_walk_stops_at_the_first_missing_version() {
    let names = [
        "v1.gz.metadata.json",
        "v2.gz.metadata.json",
        "v4.gz.metadata.json",
    ];
    assert_eq!(
        resolved_named(&names, Some("2")).await,
        "<root>/metadata/v2.gz.metadata.json"
    );
}

#[tokio::test]
async fn mixed_walk_stops_at_the_first_missing_version() {
    let names = [
        "v1.metadata.json",
        "v2.metadata.json",
        "v4.gz.metadata.json",
    ];
    assert_eq!(
        resolved_named(&names, Some("2")).await,
        "<root>/metadata/v2.metadata.json"
    );
}

#[tokio::test]
async fn walk_prefers_the_plain_name_of_a_version_under_two_names() {
    let names = [
        "v1.metadata.json",
        "v2.gz.metadata.json",
        "v2.metadata.json",
    ];
    assert_eq!(
        resolved_named(&names, Some("1")).await,
        "<root>/metadata/v2.metadata.json"
    );
}

#[tokio::test]
async fn hint_prefers_the_gz_infix_name_over_the_legacy_name() {
    let names = ["v2.metadata.json.gz", "v2.gz.metadata.json"];
    assert_eq!(
        resolved_named(&names, Some("2")).await,
        "<root>/metadata/v2.gz.metadata.json"
    );
}

#[test]
fn listing_prefers_plain_then_gz_infix_then_legacy_for_one_version() {
    let all = [
        "/w/t/metadata/v3.metadata.json.gz",
        "/w/t/metadata/v3.gz.metadata.json",
        "/w/t/metadata/v3.metadata.json",
    ];
    let compressed = [
        "/w/t/metadata/v3.gz.metadata.json",
        "/w/t/metadata/v3.metadata.json.gz",
    ];
    for names in [all.to_vec(), all.iter().rev().copied().collect()] {
        let files: Vec<FileInfo> = names
            .iter()
            .map(|name| FileInfo::new((*name).to_string(), 1, 0))
            .collect();
        assert_eq!(
            latest_metadata_location(&files),
            Some("/w/t/metadata/v3.metadata.json")
        );
    }
    for names in [
        compressed.to_vec(),
        compressed.iter().rev().copied().collect(),
    ] {
        let files: Vec<FileInfo> = names
            .iter()
            .map(|name| FileInfo::new((*name).to_string(), 1, 0))
            .collect();
        assert_eq!(
            latest_metadata_location(&files),
            Some("/w/t/metadata/v3.gz.metadata.json")
        );
    }
}

#[test]
fn compressed_names_parse_to_their_version() {
    assert_eq!(metadata_file_version("v3.gz.metadata.json"), Some(3));
    assert_eq!(metadata_file_version("v3.metadata.json.gz"), Some(3));
    assert_eq!(metadata_file_version("v3.gz.metadata.json.gz"), None);
    assert_eq!(metadata_file_version("v3.zstd.metadata.json"), None);
    assert_eq!(metadata_file_version(".v3.gz.metadata.json.crc"), None);
}

#[tokio::test]
async fn gzip_hint_walk_beyond_the_limit_refuses_naming_the_hinted_file() {
    let (dir, root) = hinted_table(&[], Some("0"));
    for version in 0..=HINT_WALK_LIMIT + 1 {
        std::fs::write(
            dir.path()
                .join("metadata")
                .join(format!("v{version}.gz.metadata.json")),
            b"{}",
        )
        .expect("write");
    }
    let error = resolve_table(&root).await.expect_err("must refuse");
    assert!(matches!(error, Error::Analysis(_)));
    assert_eq!(
        error.to_string(),
        format!(
            "cannot resolve the Iceberg table at '{root}': more than 10000 \
             metadata files follow 'v0.gz.metadata.json', the version \
             version-hint.text names"
        )
    );
}

async fn session_after_delete() -> (tempfile::TempDir, crate::ReparkSession, String) {
    use iceberg::TableCreation;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    let warehouse = tempfile::tempdir().expect("tempdir");
    let root = warehouse.path().to_string_lossy().to_string();
    let session = crate::ReparkSession::new().expect("ReparkSession");
    session
        .register_memory_catalog("ice", &root)
        .await
        .expect("catalog");
    let handle = session
        .catalogs_snapshot()
        .get("ice")
        .cloned()
        .expect("handle");
    let namespace = NamespaceIdent::new("db".to_string());
    handle
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .expect("schema");
    let table_root = format!("{root}/db/g");
    let creation = TableCreation::builder()
        .name("g".to_string())
        .location(table_root.clone())
        .schema(schema)
        .properties(HashMap::new())
        .build();
    handle
        .create_table(&namespace, creation)
        .await
        .expect("table");
    session
        .refresh_catalog_provider("ice")
        .await
        .expect("refresh");
    for statement in [
        "INSERT INTO ice.db.g VALUES (1), (2), (3)",
        "DELETE FROM ice.db.g WHERE id = 1",
    ] {
        session
            .sql(statement)
            .await
            .expect("statement")
            .collect()
            .await
            .expect("collect");
    }
    (warehouse, session, table_root)
}

fn rename_to_hadoop_names(table_root: &str, gzip_from: usize, hint: usize) {
    use std::io::Write;

    let metadata = std::path::Path::new(table_root).join("metadata");
    let mut names: Vec<String> = std::fs::read_dir(&metadata)
        .expect("read_dir")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .to_string()
        })
        .filter(|name| name.ends_with(".metadata.json"))
        .collect();
    names.sort();
    assert_eq!(names.len(), 3);
    for (index, name) in names.iter().enumerate() {
        let version = index + 1;
        let bytes = std::fs::read(metadata.join(name)).expect("read");
        if version >= gzip_from {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(&bytes).expect("gzip");
            let framed = encoder.finish().expect("gzip");
            std::fs::write(
                metadata.join(format!("v{version}.gz.metadata.json")),
                framed,
            )
            .expect("write");
        } else {
            std::fs::write(metadata.join(format!("v{version}.metadata.json")), bytes)
                .expect("write");
        }
        std::fs::remove_file(metadata.join(name)).expect("remove");
    }
    std::fs::write(metadata.join("version-hint.text"), hint.to_string()).expect("hint");
}

async fn read_ids(session: &crate::ReparkSession, argument: &str) -> Vec<i64> {
    let batches = session
        .read_iceberg_path(argument)
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    let mut ids: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .expect("Int64 id")
                .values()
                .to_vec()
        })
        .collect();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn explicit_gzip_metadata_path_reads_the_rows() {
    let (_dir, session, table_root) = session_after_delete().await;
    rename_to_hadoop_names(&table_root, 3, 3);
    assert_eq!(
        read_ids(
            &session,
            &format!("{table_root}/metadata/v3.gz.metadata.json")
        )
        .await,
        vec![2, 3]
    );
}

#[tokio::test]
async fn mixed_codec_stale_hint_after_delete_reads_the_survivors() {
    let (_dir, session, table_root) = session_after_delete().await;
    rename_to_hadoop_names(&table_root, 3, 2);
    assert_eq!(read_ids(&session, &table_root).await, vec![2, 3]);
}

#[tokio::test]
async fn gzip_current_hint_after_delete_reads_the_survivors() {
    let (_dir, session, table_root) = session_after_delete().await;
    rename_to_hadoop_names(&table_root, 1, 3);
    assert_eq!(read_ids(&session, &table_root).await, vec![2, 3]);
}
