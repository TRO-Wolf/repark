use super::*;

fn info(location: &str) -> FileInfo {
    FileInfo::new(location.to_string(), 1, 0)
}

fn local_file_io(root: &str) -> FileIO {
    repark_iceberg::catalog::file_io_for_location(root, &HashMap::<String, String>::new())
        .expect("file_io")
}

#[test]
fn numbered_metadata_files_pick_the_highest_version() {
    let files = vec![
        info("/w/t/metadata/00002-aaaa-0000.metadata.json"),
        info("/w/t/metadata/00010-bbbb-1111.metadata.json"),
        info("/w/t/metadata/00003-cccc-2222.metadata.json"),
    ];
    assert_eq!(
        latest_metadata_location(&files),
        Some("/w/t/metadata/00010-bbbb-1111.metadata.json")
    );
}

#[test]
fn v_form_metadata_files_pick_the_highest_version() {
    let files = vec![
        info("/w/t/metadata/v9.metadata.json"),
        info("/w/t/metadata/v10.metadata.json"),
    ];
    assert_eq!(
        latest_metadata_location(&files),
        Some("/w/t/metadata/v10.metadata.json")
    );
}

#[test]
fn names_outside_the_two_forms_are_ignored() {
    let files = vec![
        info("/w/t/metadata/5.metadata.json"),
        info("/w/t/metadata/junk.metadata.json.txt"),
        info("/w/t/metadata/abc.metadata.json"),
    ];
    assert_eq!(latest_metadata_location(&files), None);
}

#[tokio::test]
async fn version_hint_wins_over_directory_listing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = dir.path().join("metadata");
    std::fs::create_dir_all(&metadata).expect("mkdir");
    std::fs::write(metadata.join("00010-aaaa.metadata.json"), b"{}").expect("write");
    std::fs::write(metadata.join("v3.metadata.json"), b"{}").expect("write");
    std::fs::write(metadata.join("version-hint.text"), b"3\n").expect("write");
    let root = dir.path().to_string_lossy().to_string();
    let file_io = local_file_io(&root);
    let resolved = resolve_metadata_location(&file_io, &root)
        .await
        .expect("resolve");
    assert_eq!(resolved, format!("{root}/metadata/v3.metadata.json"));
}

#[tokio::test]
async fn listing_resolves_the_highest_numbered_metadata_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = dir.path().join("metadata");
    std::fs::create_dir_all(&metadata).expect("mkdir");
    std::fs::write(metadata.join("00002-aaaa.metadata.json"), b"{}").expect("write");
    std::fs::write(metadata.join("00010-bbbb.metadata.json"), b"{}").expect("write");
    let root = dir.path().to_string_lossy().to_string();
    let file_io = local_file_io(&root);
    let resolved = resolve_metadata_location(&file_io, &format!("{root}/"))
        .await
        .expect("resolve");
    assert_eq!(
        resolved,
        format!("{root}/metadata/00010-bbbb.metadata.json")
    );
}

#[tokio::test]
async fn hinted_metadata_file_must_exist() {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = dir.path().join("metadata");
    std::fs::create_dir_all(&metadata).expect("mkdir");
    std::fs::write(metadata.join("00002-aaaa.metadata.json"), b"{}").expect("write");
    std::fs::write(metadata.join("version-hint.text"), b"7\n").expect("write");
    let root = dir.path().to_string_lossy().to_string();
    let file_io = local_file_io(&root);
    let error = resolve_metadata_location(&file_io, &root)
        .await
        .expect_err("must refuse");
    assert!(matches!(error, Error::Analysis(_)));
    let message = error.to_string();
    assert!(message.contains(&root));
    assert!(message.contains("v7.metadata.json"));
}

#[tokio::test]
async fn unparsable_hint_falls_back_to_listing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = dir.path().join("metadata");
    std::fs::create_dir_all(&metadata).expect("mkdir");
    std::fs::write(metadata.join("00002-aaaa.metadata.json"), b"{}").expect("write");
    std::fs::write(metadata.join("version-hint.text"), b"abc\n").expect("write");
    let root = dir.path().to_string_lossy().to_string();
    let file_io = local_file_io(&root);
    let resolved = resolve_metadata_location(&file_io, &root)
        .await
        .expect("resolve");
    assert_eq!(
        resolved,
        format!("{root}/metadata/00002-aaaa.metadata.json")
    );
}

#[test]
fn v_form_requires_a_whole_integer_stem() {
    let files = vec![
        info("/w/t/metadata/v999junk.metadata.json"),
        info("/w/t/metadata/v10.metadata.json"),
    ];
    assert_eq!(
        latest_metadata_location(&files),
        Some("/w/t/metadata/v10.metadata.json")
    );
    let junk = vec![
        info("/w/t/metadata/v.metadata.json"),
        info("/w/t/metadata/v-1.metadata.json"),
        info("/w/t/metadata/v1.2.metadata.json"),
    ];
    assert_eq!(latest_metadata_location(&junk), None);
}

#[tokio::test]
async fn empty_metadata_dir_reports_the_supplied_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(dir.path().join("metadata")).expect("mkdir");
    let root = dir.path().to_string_lossy().to_string();
    let file_io = local_file_io(&root);
    let error = resolve_metadata_location(&file_io, &root)
        .await
        .expect_err("must refuse");
    assert!(matches!(error, Error::Analysis(_)));
    assert!(error.to_string().contains(&root));
}

#[tokio::test]
async fn missing_location_reports_the_supplied_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = format!("{}/no/such/dir", dir.path().to_string_lossy());
    let file_io = local_file_io(&root);
    let error = resolve_metadata_location(&file_io, &root)
        .await
        .expect_err("must refuse");
    assert!(error.to_string().contains(&root));
}
