use super::*;

const UUID_A: &str = "8f449f4d-cfce-403f-a643-95d0a15c9634";
const UUID_B: &str = "37b0d3c0-deb7-47ee-b787-2f92e5b79515";
const UUID_C: &str = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";

fn info(location: &str) -> FileInfo {
    FileInfo::new(location.to_string(), 1, 0)
}

async fn resolve_supplied(file_io: &FileIO, path: &str) -> Result<String> {
    resolve_metadata_location(file_io, path, path).await
}

fn no_metadata_message(path: &str) -> String {
    format!(
        "no Iceberg table found at '{path}': expected metadata files under \
         '<location>/metadata' or a '<location>/metadata/version-hint.text'"
    )
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
    let resolved = resolve_supplied(&file_io, &root).await.expect("resolve");
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
    let resolved = resolve_supplied(&file_io, &format!("{root}/"))
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
    let error = resolve_supplied(&file_io, &root)
        .await
        .expect_err("must refuse");
    assert!(matches!(error, Error::Analysis(_)));
    assert_eq!(
        error.to_string(),
        format!(
            "no Iceberg table found at '{root}': version-hint.text names \
             'v7.metadata.json', which does not exist"
        )
    );
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
    let resolved = resolve_supplied(&file_io, &root).await.expect("resolve");
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
    let error = resolve_supplied(&file_io, &root)
        .await
        .expect_err("must refuse");
    assert!(matches!(error, Error::Analysis(_)));
    assert_eq!(error.to_string(), no_metadata_message(&root));
}

#[tokio::test]
async fn missing_location_reports_the_supplied_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = format!("{}/no/such/dir", dir.path().to_string_lossy());
    let file_io = local_file_io(&root);
    let error = resolve_supplied(&file_io, &root)
        .await
        .expect_err("must refuse");
    assert!(matches!(error, Error::Analysis(_)));
    assert_eq!(error.to_string(), no_metadata_message(&root));
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
    let resolved = resolve_supplied(&file_io, &root).await.expect("resolve");
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
    let resolved = resolve_supplied(&file_io, &format!("{root}/"))
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
    let resolved = resolve_supplied(&file_io, &format!("file://{root}"))
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
    let resolved = resolve_supplied(&file_io, &root).await.expect("resolve");
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
    let error = resolve_supplied(&file_io, &root)
        .await
        .expect_err("must refuse");
    assert!(matches!(error, Error::Analysis(_)));
    assert_eq!(error.to_string(), no_metadata_message(&root));
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
    let resolved = resolve_supplied(&file_io, &root).await.expect("resolve");
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
    let resolved = resolve_supplied(&file_io, &pinned).await.expect("resolve");
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

async fn file_relative_refusal(argument: &str) -> String {
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

#[tokio::test]
async fn file_relative_location_refuses_uri_syntax() {
    let (_dir, root) = authority_table_root();
    let argument = format!("file:{}", root.trim_start_matches('/'));
    let message = file_relative_refusal(&argument).await;
    assert_eq!(
        message,
        format!("java.net.URISyntaxException: Relative path in absolute URI: {argument}")
    );
}

#[tokio::test]
async fn file_relative_location_trailing_slash_refuses_uri_syntax() {
    let (_dir, root) = authority_table_root();
    let argument = format!("file:{}", root.trim_start_matches('/'));
    let message = file_relative_refusal(&format!("{argument}/")).await;
    assert_eq!(
        message,
        format!("java.net.URISyntaxException: Relative path in absolute URI: {argument}")
    );
}

#[tokio::test]
async fn file_relative_metadata_file_refuses_uri_syntax() {
    let (_dir, root) = authority_table_root();
    let argument = format!(
        "file:{}/metadata/v1.metadata.json",
        root.trim_start_matches('/')
    );
    let message = file_relative_refusal(&argument).await;
    assert_eq!(
        message,
        format!("java.net.URISyntaxException: Relative path in absolute URI: {argument}")
    );
}

async fn session_with_rows() -> (tempfile::TempDir, crate::ReparkSession, String) {
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
    let table_root = format!("{root}/db/p");
    let creation = TableCreation::builder()
        .name("p".to_string())
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
    session
        .sql("INSERT INTO ice.db.p VALUES (1), (2)")
        .await
        .expect("insert")
        .collect()
        .await
        .expect("collect");
    (warehouse, session, table_root)
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

fn latest_metadata_file(table_root: &str) -> String {
    let mut names: Vec<String> = std::fs::read_dir(format!("{table_root}/metadata"))
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
    format!(
        "{table_root}/metadata/{}",
        names.last().expect("metadata file")
    )
}

#[tokio::test]
async fn file_single_slash_location_reads_rows() {
    let (_dir, session, table_root) = session_with_rows().await;
    assert_eq!(
        read_ids(&session, &format!("file:{table_root}")).await,
        vec![1, 2]
    );
}

#[tokio::test]
async fn file_upper_case_single_slash_location_reads_rows() {
    let (_dir, session, table_root) = session_with_rows().await;
    assert_eq!(
        read_ids(&session, &format!("FILE:{table_root}")).await,
        vec![1, 2]
    );
}

#[tokio::test]
async fn file_mixed_case_empty_authority_location_reads_rows() {
    let (_dir, session, table_root) = session_with_rows().await;
    assert_eq!(
        read_ids(&session, &format!("fIlE://{table_root}")).await,
        vec![1, 2]
    );
}

#[tokio::test]
async fn file_single_slash_latest_metadata_file_reads_rows() {
    let (_dir, session, table_root) = session_with_rows().await;
    let latest = latest_metadata_file(&table_root);
    assert_eq!(
        read_ids(&session, &format!("file:{latest}")).await,
        vec![1, 2]
    );
}

#[tokio::test]
async fn file_triple_slash_location_reads_rows() {
    let (_dir, session, table_root) = session_with_rows().await;
    assert_eq!(
        read_ids(&session, &format!("file://{table_root}")).await,
        vec![1, 2]
    );
}

#[tokio::test]
async fn file_single_slash_missing_location_names_the_supplied_argument() {
    let dir = tempfile::tempdir().expect("tempdir");
    let argument = format!("file:{}/no/such/dir", dir.path().to_string_lossy());
    let session = crate::ReparkSession::new().expect("ReparkSession");
    let error = session
        .read_iceberg_path(&argument)
        .await
        .expect_err("must refuse");
    assert!(matches!(error, Error::Analysis(_)), "{error:?}");
    assert_eq!(error.to_string(), no_metadata_message(&argument));
}

#[test]
fn v_form_versions_beyond_java_int_are_not_candidates() {
    assert_eq!(metadata_file_version("v2147483648.metadata.json"), None);
    assert_eq!(
        metadata_file_version("v2147483647.metadata.json"),
        Some(2_147_483_647)
    );
    let files = vec![
        info("/w/t/metadata/v1.metadata.json"),
        info("/w/t/metadata/v2.metadata.json"),
        info("/w/t/metadata/v2147483648.metadata.json"),
    ];
    assert_eq!(
        latest_metadata_location(&files),
        Some("/w/t/metadata/v2.metadata.json")
    );
}

#[test]
fn v_form_java_int_max_is_the_highest_candidate() {
    let files = vec![
        info("/w/t/metadata/v1.metadata.json"),
        info("/w/t/metadata/v2.metadata.json"),
        info("/w/t/metadata/v2147483647.metadata.json"),
        info("/w/t/metadata/v2147483648.metadata.json"),
    ];
    assert_eq!(
        latest_metadata_location(&files),
        Some("/w/t/metadata/v2147483647.metadata.json")
    );
}

#[test]
fn v_form_u64_overflow_stays_ignored() {
    assert_eq!(
        metadata_file_version("v99999999999999999999.metadata.json"),
        None
    );
    let files = vec![
        info("/w/t/metadata/v2.metadata.json"),
        info("/w/t/metadata/v99999999999999999999.metadata.json"),
    ];
    assert_eq!(
        latest_metadata_location(&files),
        Some("/w/t/metadata/v2.metadata.json")
    );
}

#[tokio::test]
async fn hint_beyond_java_int_falls_back_to_listing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = dir.path().join("metadata");
    std::fs::create_dir_all(&metadata).expect("mkdir");
    std::fs::write(metadata.join("v1.metadata.json"), b"{}").expect("write");
    std::fs::write(metadata.join("v2.metadata.json"), b"{}").expect("write");
    std::fs::write(metadata.join("version-hint.text"), b"2147483648").expect("write");
    let root = dir.path().to_string_lossy().to_string();
    let file_io = local_file_io(&root);
    let resolved = resolve_supplied(&file_io, &root).await.expect("resolve");
    assert_eq!(resolved, format!("{root}/metadata/v2.metadata.json"));
}

#[tokio::test]
async fn relative_file_path_keeps_one_of_two_trailing_slashes() {
    let (_dir, root) = authority_table_root();
    let argument = format!("file:{}", root.trim_start_matches('/'));
    let message = file_relative_refusal(&format!("{argument}//")).await;
    assert_eq!(
        message,
        format!("java.net.URISyntaxException: Relative path in absolute URI: {argument}/")
    );
}

#[tokio::test]
async fn non_candidate_metadata_names_report_the_supplied_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = dir.path().join("metadata");
    std::fs::create_dir_all(&metadata).expect("mkdir");
    std::fs::write(metadata.join("junk.metadata.json.txt"), b"{}").expect("write");
    std::fs::write(metadata.join("v2147483648.metadata.json"), b"{}").expect("write");
    std::fs::write(metadata.join("999-.metadata.json"), b"{}").expect("write");
    let root = dir.path().to_string_lossy().to_string();
    let file_io = local_file_io(&root);
    let error = resolve_supplied(&file_io, &root)
        .await
        .expect_err("must refuse");
    assert!(matches!(error, Error::Analysis(_)));
    assert_eq!(error.to_string(), no_metadata_message(&root));
}

#[test]
fn v_form_zero_is_the_lowest_candidate() {
    assert_eq!(metadata_file_version("v0.metadata.json"), Some(0));
    let files = vec![info("/w/t/metadata/v0.metadata.json")];
    assert_eq!(
        latest_metadata_location(&files),
        Some("/w/t/metadata/v0.metadata.json")
    );
}

#[tokio::test]
async fn hint_at_java_int_max_selects_that_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = dir.path().join("metadata");
    std::fs::create_dir_all(&metadata).expect("mkdir");
    std::fs::write(metadata.join("v2.metadata.json"), b"{}").expect("write");
    std::fs::write(metadata.join("v2147483647.metadata.json"), b"{}").expect("write");
    std::fs::write(metadata.join("version-hint.text"), b"2147483647").expect("write");
    let root = dir.path().to_string_lossy().to_string();
    let file_io = local_file_io(&root);
    let resolved = resolve_supplied(&file_io, &root).await.expect("resolve");
    assert_eq!(
        resolved,
        format!("{root}/metadata/v2147483647.metadata.json")
    );
}

#[test]
fn numbered_form_keeps_the_u64_range() {
    assert_eq!(
        metadata_file_version(&format!("4294967296-{UUID_A}.metadata.json")),
        Some(4_294_967_296)
    );
    assert_eq!(
        metadata_file_version(&format!("18446744073709551615-{UUID_A}.metadata.json")),
        Some(u64::MAX)
    );
    assert_eq!(
        metadata_file_version(&format!("18446744073709551616-{UUID_A}.metadata.json")),
        None
    );
    let files = vec![
        info(&format!("/w/t/metadata/00002-{UUID_A}.metadata.json")),
        info(&format!("/w/t/metadata/4294967296-{UUID_B}.metadata.json")),
    ];
    assert_eq!(
        latest_metadata_location(&files),
        Some(format!("/w/t/metadata/4294967296-{UUID_B}.metadata.json").as_str())
    );
}

#[tokio::test]
async fn file_mixed_case_single_slash_location_reads_rows() {
    let (_dir, session, table_root) = session_with_rows().await;
    assert_eq!(
        read_ids(&session, &format!("fIlE:{table_root}")).await,
        vec![1, 2]
    );
}

async fn resolve_with_hint(hint: &str) -> String {
    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = dir.path().join("metadata");
    std::fs::create_dir_all(&metadata).expect("mkdir");
    std::fs::write(metadata.join("v0.metadata.json"), b"{}").expect("write");
    std::fs::write(metadata.join("v2.metadata.json"), b"{}").expect("write");
    std::fs::write(metadata.join("version-hint.text"), hint.as_bytes()).expect("write");
    let root = dir.path().to_string_lossy().to_string();
    let file_io = local_file_io(&root);
    resolve_supplied(&file_io, &root)
        .await
        .expect("resolve")
        .replace(&root, "<root>")
}

#[tokio::test]
async fn hint_zero_selects_v0() {
    assert_eq!(
        resolve_with_hint("0").await,
        "<root>/metadata/v0.metadata.json"
    );
}

#[tokio::test]
async fn hint_below_java_int_range_falls_back_to_listing() {
    assert_eq!(
        resolve_with_hint("-1").await,
        "<root>/metadata/v2.metadata.json"
    );
}

#[tokio::test]
async fn hint_beyond_u64_falls_back_to_listing() {
    assert_eq!(
        resolve_with_hint("99999999999999999999").await,
        "<root>/metadata/v2.metadata.json"
    );
}

#[test]
fn numbered_form_zero_is_a_candidate() {
    assert_eq!(
        metadata_file_version(&format!("00000-{UUID_A}.metadata.json")),
        Some(0)
    );
}

#[test]
fn uuid_suffix_length_and_case_edges() {
    let short = &UUID_A[..35];
    assert_eq!(
        metadata_file_version(&format!("7-{short}.metadata.json")),
        None
    );
    assert_eq!(
        metadata_file_version(&format!("7-{UUID_A}0.metadata.json")),
        None
    );
    assert_eq!(
        metadata_file_version("7-8F449f4d-CFce-403F-a643-95D0a15c9634.metadata.json"),
        Some(7)
    );
}

#[test]
fn upper_case_v_prefix_is_not_a_candidate() {
    assert_eq!(metadata_file_version("V3.metadata.json"), None);
}

#[tokio::test]
async fn file_authority_two_trailing_slashes_keeps_one() {
    let (_dir, root) = authority_table_root();
    let relative = root.trim_start_matches('/');
    let message = file_authority_refusal(&format!("file://{relative}//")).await;
    assert_eq!(
        message,
        format!("Wrong FS: file://{relative}//metadata, expected: file:///")
    );
}

#[tokio::test]
async fn upper_case_file_authority_is_not_refused_wrong_fs() {
    let (_dir, root) = authority_table_root();
    let relative = root.trim_start_matches('/');
    let session = crate::ReparkSession::new().expect("ReparkSession");
    for argument in [
        format!("FILE://{relative}"),
        format!("File://localhost{root}"),
    ] {
        let error = session
            .read_iceberg_path(&argument)
            .await
            .expect_err("must refuse");
        assert!(matches!(error, Error::Analysis(_)), "{error:?}");
        assert_eq!(error.to_string(), no_metadata_message(&argument));
    }
}

#[tokio::test]
async fn upper_case_relative_file_path_is_not_refused_uri_syntax() {
    let (_dir, root) = authority_table_root();
    let relative = root.trim_start_matches('/');
    let session = crate::ReparkSession::new().expect("ReparkSession");
    for argument in [format!("File:{relative}"), format!("FILE:{relative}")] {
        let error = session
            .read_iceberg_path(&argument)
            .await
            .expect_err("must refuse");
        assert!(matches!(error, Error::Analysis(_)), "{error:?}");
        assert_eq!(
            error.to_string(),
            format!(
                "Error during planning: malformed storage location `{argument}`: a `:` before \
                 the first `/` looks like a mistyped URI scheme — did you mean `scheme://…`? \
                 RePark supports `s3://`, `s3a://`, `file://`, or a bare absolute filesystem path"
            )
        );
    }
}

#[tokio::test]
async fn file_upper_case_empty_authority_location_reads_rows() {
    let (_dir, session, table_root) = session_with_rows().await;
    assert_eq!(
        read_ids(&session, &format!("FILE://{table_root}")).await,
        vec![1, 2]
    );
}
