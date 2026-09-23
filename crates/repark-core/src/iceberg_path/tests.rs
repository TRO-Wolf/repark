use super::*;

const UUID_A: &str = "8f449f4d-cfce-403f-a643-95d0a15c9634";
const UUID_B: &str = "37b0d3c0-deb7-47ee-b787-2f92e5b79515";
const UUID_C: &str = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";

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
        info(&format!("/w/t/metadata/00002-{UUID_A}.metadata.json")),
        info(&format!("/w/t/metadata/00010-{UUID_B}.metadata.json")),
        info(&format!("/w/t/metadata/00003-{UUID_C}.metadata.json")),
    ];
    assert_eq!(
        latest_metadata_location(&files),
        Some(format!("/w/t/metadata/00010-{UUID_B}.metadata.json").as_str())
    );
}

#[test]
fn numbered_metadata_requires_a_canonical_uuid() {
    let files = vec![
        info("/w/t/metadata/999-.metadata.json"),
        info(&format!("/w/t/metadata/00010-{UUID_A}.metadata.json")),
    ];
    assert_eq!(
        latest_metadata_location(&files),
        Some(format!("/w/t/metadata/00010-{UUID_A}.metadata.json").as_str())
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
        info("/w/t/metadata/999-.metadata.json"),
        info("/w/t/metadata/12-notauuid.metadata.json"),
        info(&format!("/w/t/metadata/+12-{UUID_A}.metadata.json")),
        info(&format!("/w/t/metadata/-{UUID_A}.metadata.json")),
        info("/w/t/metadata/v+3.metadata.json"),
        info(&format!("/w/t/metadata/12-{UUID_A}-extra.metadata.json")),
        info("/w/t/metadata/12-8f449f4d-cfce-403f-a643-95d0a15c963x.metadata.json"),
    ];
    assert_eq!(latest_metadata_location(&files), None);
}

#[test]
fn valid_forms_still_resolve() {
    let files = vec![
        info(&format!("/w/t/metadata/00001-{UUID_A}.metadata.json")),
        info("/w/t/metadata/v1.metadata.json"),
        info("/w/t/metadata/v007.metadata.json"),
        info("/w/t/metadata/00002-8F449F4D-CFCE-403F-A643-95D0A15C9634.metadata.json"),
    ];
    assert_eq!(
        latest_metadata_location(&files),
        Some("/w/t/metadata/v007.metadata.json")
    );
}

#[tokio::test]
async fn version_hint_wins_over_directory_listing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = dir.path().join("metadata");
    std::fs::create_dir_all(&metadata).expect("mkdir");
    std::fs::write(
        metadata.join(format!("00010-{UUID_A}.metadata.json")),
        b"{}",
    )
    .expect("write");
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
    std::fs::write(
        metadata.join(format!("00002-{UUID_A}.metadata.json")),
        b"{}",
    )
    .expect("write");
    std::fs::write(
        metadata.join(format!("00010-{UUID_B}.metadata.json")),
        b"{}",
    )
    .expect("write");
    let root = dir.path().to_string_lossy().to_string();
    let file_io = local_file_io(&root);
    let resolved = resolve_metadata_location(&file_io, &format!("{root}/"))
        .await
        .expect("resolve");
    assert_eq!(
        resolved,
        format!("{root}/metadata/00010-{UUID_B}.metadata.json")
    );
}

#[tokio::test]
async fn hinted_metadata_file_must_exist() {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = dir.path().join("metadata");
    std::fs::create_dir_all(&metadata).expect("mkdir");
    std::fs::write(
        metadata.join(format!("00002-{UUID_A}.metadata.json")),
        b"{}",
    )
    .expect("write");
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
    std::fs::write(
        metadata.join(format!("00002-{UUID_A}.metadata.json")),
        b"{}",
    )
    .expect("write");
    std::fs::write(metadata.join("version-hint.text"), b"abc\n").expect("write");
    let root = dir.path().to_string_lossy().to_string();
    let file_io = local_file_io(&root);
    let resolved = resolve_metadata_location(&file_io, &root)
        .await
        .expect("resolve");
    assert_eq!(
        resolved,
        format!("{root}/metadata/00002-{UUID_A}.metadata.json")
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
    assert!(matches!(error, Error::Analysis(_)));
    assert!(error.to_string().contains(&root));
}

#[test]
fn each_valid_form_parses_to_its_version() {
    let uuid = format!("00001-{UUID_A}.metadata.json");
    assert_eq!(metadata_file_version(&uuid), Some(1));
    assert_eq!(metadata_file_version("v1.metadata.json"), Some(1));
    assert_eq!(metadata_file_version("v007.metadata.json"), Some(7));
    let upper = "00004-8F449F4D-CFCE-403F-A643-95D0A15C9634.metadata.json";
    assert_eq!(metadata_file_version(upper), Some(4));
}

#[test]
fn uuid_dashes_at_wrong_positions_are_rejected() {
    for positions in [
        [7, 13, 18, 23],
        [8, 12, 18, 23],
        [8, 13, 19, 23],
        [8, 13, 18, 22],
    ] {
        let mut name = String::with_capacity(36);
        let mut hex = "0123456789abcdef0123456789abcdef".chars();
        for index in 0..36 {
            if positions.contains(&index) {
                name.push('-');
            } else {
                name.push(hex.next().expect("hex"));
            }
        }
        let file = format!("00099-{name}.metadata.json");
        assert_eq!(metadata_file_version(&file), None, "{file}");
        let files = vec![
            info(&format!("/w/t/{file}")),
            info(&format!("/w/t/00002-{UUID_A}.metadata.json")),
        ];
        assert_eq!(
            latest_metadata_location(&files),
            Some(format!("/w/t/00002-{UUID_A}.metadata.json").as_str())
        );
    }
}

#[tokio::test]
async fn nested_metadata_files_are_not_candidates() {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = dir.path().join("metadata");
    std::fs::create_dir_all(metadata.join("sub")).expect("mkdir");
    std::fs::write(metadata.join("v3.metadata.json"), b"{}").expect("write");
    std::fs::write(metadata.join("sub").join("v99.metadata.json"), b"{}").expect("write");
    let root = dir.path().to_string_lossy().to_string();
    let file_io = local_file_io(&root);
    let resolved = resolve_metadata_location(&file_io, &root)
        .await
        .expect("resolve");
    assert_eq!(resolved, format!("{root}/metadata/v3.metadata.json"));
}

#[tokio::test]
async fn nested_metadata_files_are_not_candidates_trailing_slash() {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = dir.path().join("metadata");
    std::fs::create_dir_all(metadata.join("sub")).expect("mkdir");
    std::fs::write(metadata.join("v3.metadata.json"), b"{}").expect("write");
    std::fs::write(metadata.join("sub").join("v99.metadata.json"), b"{}").expect("write");
    let root = dir.path().to_string_lossy().to_string();
    let file_io = local_file_io(&format!("{root}/"));
    let resolved = resolve_metadata_location(&file_io, &format!("{root}/"))
        .await
        .expect("resolve");
    assert_eq!(resolved, format!("{root}/metadata/v3.metadata.json"));
}

#[tokio::test]
async fn nested_metadata_files_are_not_candidates_file_scheme() {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = dir.path().join("metadata");
    std::fs::create_dir_all(metadata.join("sub")).expect("mkdir");
    std::fs::write(metadata.join("v3.metadata.json"), b"{}").expect("write");
    std::fs::write(metadata.join("sub").join("v99.metadata.json"), b"{}").expect("write");
    let root = dir.path().to_string_lossy().to_string();
    let file_io = local_file_io(&format!("file://{root}"));
    let resolved = resolve_metadata_location(&file_io, &format!("file://{root}"))
        .await
        .expect("resolve");
    assert_eq!(resolved, format!("{root}/metadata/v3.metadata.json"));
}

#[tokio::test]
async fn nested_numbered_metadata_is_not_a_candidate() {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = dir.path().join("metadata");
    std::fs::create_dir_all(metadata.join("metadata")).expect("mkdir");
    std::fs::write(
        metadata.join(format!("00002-{UUID_A}.metadata.json")),
        b"{}",
    )
    .expect("write");
    std::fs::write(
        metadata
            .join("metadata")
            .join(format!("00099-{UUID_B}.metadata.json")),
        b"{}",
    )
    .expect("write");
    let root = dir.path().to_string_lossy().to_string();
    let file_io = local_file_io(&root);
    let resolved = resolve_metadata_location(&file_io, &root)
        .await
        .expect("resolve");
    assert_eq!(
        resolved,
        format!("{root}/metadata/00002-{UUID_A}.metadata.json")
    );
}

#[tokio::test]
async fn only_nested_metadata_names_the_supplied_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = dir.path().join("metadata");
    std::fs::create_dir_all(metadata.join("sub")).expect("mkdir");
    std::fs::write(metadata.join("sub").join("v1.metadata.json"), b"{}").expect("write");
    let root = dir.path().to_string_lossy().to_string();
    let file_io = local_file_io(&root);
    let error = resolve_metadata_location(&file_io, &root)
        .await
        .expect_err("must refuse");
    assert!(matches!(error, Error::Analysis(_)));
    assert!(error.to_string().contains(&root));
}

#[tokio::test]
async fn non_utf8_hint_falls_back_to_listing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = dir.path().join("metadata");
    std::fs::create_dir_all(&metadata).expect("mkdir");
    std::fs::write(
        metadata.join(format!("00002-{UUID_A}.metadata.json")),
        b"{}",
    )
    .expect("write");
    std::fs::write(metadata.join("version-hint.text"), b"\xff\xfe3\n").expect("write");
    let root = dir.path().to_string_lossy().to_string();
    let file_io = local_file_io(&root);
    let resolved = resolve_metadata_location(&file_io, &root)
        .await
        .expect("resolve");
    assert_eq!(
        resolved,
        format!("{root}/metadata/00002-{UUID_A}.metadata.json")
    );
}

#[tokio::test]
async fn explicit_metadata_path_resolves_verbatim() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().to_string_lossy().to_string();
    let file_io = local_file_io(&root);
    let pinned = format!("{root}/metadata/v1.metadata.json");
    let resolved = resolve_metadata_location(&file_io, &pinned)
        .await
        .expect("resolve");
    assert_eq!(resolved, pinned);
}

async fn file_authority_refusal(argument: &str) -> String {
    let session = crate::ReparkSession::new().expect("ReparkSession");
    let error = session
        .read_iceberg_path(argument)
        .await
        .expect_err("must refuse");
    match error {
        Error::IllegalArgument(message) => message,
        other => panic!("expected IllegalArgument, got {other:?}"),
    }
}

fn authority_table_root() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = dir.path().join("metadata");
    std::fs::create_dir_all(&metadata).expect("mkdir");
    std::fs::write(metadata.join("v1.metadata.json"), b"{}").expect("write");
    let root = dir.path().to_string_lossy().to_string();
    (dir, root)
}

#[tokio::test]
async fn file_authority_location_refuses_wrong_fs() {
    let (_dir, root) = authority_table_root();
    let relative = root.trim_start_matches('/');
    let message = file_authority_refusal(&format!("file://{relative}")).await;
    assert_eq!(
        message,
        format!("Wrong FS: file://{relative}/metadata, expected: file:///")
    );
}

#[tokio::test]
async fn file_authority_location_trailing_slash_refuses_wrong_fs() {
    let (_dir, root) = authority_table_root();
    let relative = root.trim_start_matches('/');
    let message = file_authority_refusal(&format!("file://{relative}/")).await;
    assert_eq!(
        message,
        format!("Wrong FS: file://{relative}/metadata, expected: file:///")
    );
}

#[tokio::test]
async fn file_authority_metadata_file_refuses_wrong_fs() {
    let (_dir, root) = authority_table_root();
    let relative = root.trim_start_matches('/');
    let argument = format!("file://{relative}/metadata/v1.metadata.json");
    let message = file_authority_refusal(&argument).await;
    assert_eq!(message, format!("Wrong FS: {argument}, expected: file:///"));
}

#[tokio::test]
async fn file_localhost_location_refuses_wrong_fs() {
    let (_dir, root) = authority_table_root();
    let message = file_authority_refusal(&format!("file://localhost{root}")).await;
    assert_eq!(
        message,
        format!("Wrong FS: file://localhost{root}/metadata, expected: file:///")
    );
}

#[tokio::test]
async fn file_localhost_metadata_file_refuses_wrong_fs() {
    let (_dir, root) = authority_table_root();
    let argument = format!("file://localhost{root}/metadata/v1.metadata.json");
    let message = file_authority_refusal(&argument).await;
    assert_eq!(message, format!("Wrong FS: {argument}, expected: file:///"));
}
