use super::*;

const CATALOG_STYLE: &str = "00009-2f6c1c3e-9a7b-4d2e-8f10-5b3a7c9d0e11.metadata.json";

fn table(names: &[&str]) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = dir.path().join("metadata");
    std::fs::create_dir_all(&metadata).expect("mkdir");
    for name in names {
        std::fs::write(metadata.join(name), b"{}").expect("write");
    }
    let root = dir.path().to_string_lossy().to_string();
    (dir, root)
}

fn write_hint(root: &str, hint: &[u8]) {
    std::fs::write(format!("{root}/metadata/version-hint.text"), hint).expect("hint");
}

async fn resolve(root: &str) -> Result<String> {
    let file_io =
        repark_iceberg::catalog::file_io_for_location(root, &HashMap::<String, String>::new())
            .expect("file_io");
    resolve_metadata_location(&file_io, root, root).await
}

async fn resolved(root: &str) -> String {
    resolve(root)
        .await
        .expect("resolve")
        .replace(root, "<root>")
}

const GAP: [&str; 3] = ["v1.metadata.json", "v2.metadata.json", "v4.metadata.json"];

#[test]
fn hint_text_parses_like_java_read_line_then_parse_int() {
    assert_eq!(java_hint_version(b"5"), Some(5));
    assert_eq!(java_hint_version(b"5\n"), Some(5));
    assert_eq!(java_hint_version(b"5\r\n"), Some(5));
    assert_eq!(java_hint_version(b"5\r"), Some(5));
    assert_eq!(java_hint_version(b"5\nabc"), Some(5));
    assert_eq!(java_hint_version(b"5\n\xff"), Some(5));
    assert_eq!(java_hint_version(b"+5"), Some(5));
    assert_eq!(java_hint_version(b"-1"), Some(-1));
    assert_eq!(java_hint_version(b"0"), Some(0));
    assert_eq!(java_hint_version(b"2147483647"), Some(2_147_483_647));
    assert_eq!(java_hint_version(b""), None);
    assert_eq!(java_hint_version(b"\n5"), None);
    assert_eq!(java_hint_version(b" 5"), None);
    assert_eq!(java_hint_version(b"5 "), None);
    assert_eq!(java_hint_version(b"abc"), None);
    assert_eq!(java_hint_version(b"2147483648"), None);
    assert_eq!(java_hint_version(b"-2147483649"), None);
    assert_eq!(java_hint_version("\u{feff}5".as_bytes()), None);
    assert_eq!(java_hint_version(b"\xff5"), None);
}

#[tokio::test]
async fn leading_space_hint_falls_to_the_listing() {
    let (_dir, root) = table(&GAP);
    write_hint(&root, b" 1");
    assert_eq!(resolved(&root).await, "<root>/metadata/v4.metadata.json");
}

#[tokio::test]
async fn crlf_hint_parses_and_walks_from_the_hinted_version() {
    let (_dir, root) = table(&GAP);
    write_hint(&root, b"1\r\n");
    assert_eq!(resolved(&root).await, "<root>/metadata/v2.metadata.json");
}

#[tokio::test]
async fn only_the_first_hint_line_is_read() {
    let (_dir, root) = table(&GAP);
    write_hint(&root, b"1\n9");
    assert_eq!(resolved(&root).await, "<root>/metadata/v2.metadata.json");
}

#[tokio::test]
async fn hint_that_is_a_directory_falls_to_the_listing() {
    let (_dir, root) = table(&GAP);
    std::fs::create_dir(format!("{root}/metadata/version-hint.text")).expect("mkdir");
    assert_eq!(resolved(&root).await, "<root>/metadata/v4.metadata.json");
}

#[tokio::test]
async fn unreadable_hint_falls_to_the_listing() {
    use std::os::unix::fs::PermissionsExt;
    let (_dir, root) = table(&GAP);
    write_hint(&root, b"4");
    let hint = format!("{root}/metadata/version-hint.text");
    std::fs::set_permissions(&hint, std::fs::Permissions::from_mode(0o000)).expect("chmod");
    let answer = resolved(&root).await;
    std::fs::set_permissions(&hint, std::fs::Permissions::from_mode(0o644)).expect("chmod");
    assert_eq!(answer, "<root>/metadata/v4.metadata.json");
}

#[tokio::test]
async fn negative_hint_refuses_naming_the_missing_version() {
    let (_dir, root) = table(&GAP);
    write_hint(&root, b"-1");
    let error = resolve(&root).await.expect_err("must refuse");
    assert!(matches!(error, Error::Analysis(_)));
    assert_eq!(
        error.to_string(),
        format!("Metadata file for version -1 is missing under {root}/metadata")
    );
}

#[tokio::test]
async fn negative_hint_reads_a_negative_version_file_like_java() {
    let (_dir, root) = table(&["v-1.metadata.json", "v0.metadata.json"]);
    write_hint(&root, b"-1");
    assert_eq!(resolved(&root).await, "<root>/metadata/v0.metadata.json");
}

#[tokio::test]
async fn hint_past_the_newest_refuses_naming_the_metadata_dir() {
    let (_dir, root) = table(&GAP);
    write_hint(&root, b"9");
    let error = resolve(&root).await.expect_err("must refuse");
    assert_eq!(
        error.to_string(),
        format!("Metadata file for version 9 is missing under {root}/metadata")
    );
}

#[tokio::test]
async fn hint_into_a_removed_version_refuses() {
    let (_dir, root) = table(&GAP);
    write_hint(&root, b"3");
    let error = resolve(&root).await.expect_err("must refuse");
    assert_eq!(
        error.to_string(),
        format!("Metadata file for version 3 is missing under {root}/metadata")
    );
}

#[tokio::test]
async fn zero_hint_without_v0_is_a_missing_table() {
    let (_dir, root) = table(&GAP);
    write_hint(&root, b"0");
    let error = resolve(&root).await.expect_err("must refuse");
    assert_eq!(
        error.to_string(),
        format!(
            "no Iceberg table found at '{root}': expected metadata files under \
             '<location>/metadata' or a '<location>/metadata/version-hint.text'"
        )
    );
}

#[tokio::test]
async fn catalog_style_name_never_beats_a_v_name_without_a_hint() {
    let (_dir, root) = table(&["v1.metadata.json", "v2.metadata.json", CATALOG_STYLE]);
    assert_eq!(resolved(&root).await, "<root>/metadata/v2.metadata.json");
}

#[tokio::test]
async fn catalog_style_name_never_beats_a_v_name_behind_an_unparsable_hint() {
    for hint in [&b""[..], b"abc", b"2147483648", b" 2"] {
        let (_dir, root) = table(&["v1.metadata.json", "v2.metadata.json", CATALOG_STYLE]);
        write_hint(&root, hint);
        assert_eq!(resolved(&root).await, "<root>/metadata/v2.metadata.json");
    }
}

#[tokio::test]
async fn listing_skips_a_stem_whose_canonical_name_is_absent() {
    for name in [
        "v007.metadata.json",
        "v7.zstd.metadata.json",
        "v+7.metadata.json",
    ] {
        let (_dir, root) = table(&["v2.metadata.json", name]);
        assert_eq!(
            resolved(&root).await,
            "<root>/metadata/v2.metadata.json",
            "{name}"
        );
    }
}

#[tokio::test]
async fn listing_walks_from_the_newest_listed_version_only() {
    let (_dir, root) = table(&GAP);
    assert_eq!(resolved(&root).await, "<root>/metadata/v4.metadata.json");
}

#[test]
fn listing_stem_follows_the_java_version_pattern() {
    assert_eq!(hadoop_listing_stem("v5.metadata.json"), Some(5));
    assert_eq!(hadoop_listing_stem("v5.anything"), Some(5));
    assert_eq!(hadoop_listing_stem("v+5.metadata.json"), Some(5));
    assert_eq!(hadoop_listing_stem("v-5.metadata.json"), Some(-5));
    assert_eq!(hadoop_listing_stem("v.metadata.json"), None);
    assert_eq!(hadoop_listing_stem("v5"), None);
    assert_eq!(hadoop_listing_stem("V5.metadata.json"), None);
    assert_eq!(hadoop_listing_stem("v2147483648.metadata.json"), None);
    assert_eq!(hadoop_listing_stem(CATALOG_STYLE), None);
}
