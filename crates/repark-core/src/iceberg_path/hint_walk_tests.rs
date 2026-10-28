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
