use std::collections::{BTreeMap, HashMap};

use tempfile::TempDir;

use super::super::wiring::{FileConfig, conf_dump_rows, load_file_config};
use super::{stub_environment, write_file};
use crate::session::ReparkSessionBuilder;

fn try_loaded_file(text: &str, variables: &[(&str, &str)]) -> crate::Result<FileConfig> {
    let directory = TempDir::new().expect("config fixture directory");
    let path = directory.path().join("repark.toml");
    write_file(&path, text);
    let environment = stub_environment(variables);
    load_file_config(Some(path), &environment, directory.path(), None)
}

fn staged_file(text: &str) -> (TempDir, std::path::PathBuf) {
    let directory = TempDir::new().expect("config fixture directory");
    let path = directory.path().join("repark.toml");
    write_file(&path, text);
    (directory, path)
}

#[test]
fn test_toml_display_table_sets_style() {
    let file = try_loaded_file(
        "[default.display]\nstyle = \"spark\"\nmax_rows = 20\nmax_cols = 8\nstr_len = 30\n",
        &[],
    )
    .expect("fixture loads");
    let pairs: HashMap<String, String> = file.pairs.into_iter().collect();
    assert_eq!(
        pairs.get("repark.display.style").map(String::as_str),
        Some("spark")
    );
    assert_eq!(
        pairs.get("repark.display.max_rows").map(String::as_str),
        Some("20")
    );
    assert_eq!(
        pairs.get("repark.display.max_cols").map(String::as_str),
        Some("8")
    );
    assert_eq!(
        pairs.get("repark.display.str_len").map(String::as_str),
        Some("30")
    );
    let error = try_loaded_file("[default.display]\nmax_rows = true\n", &[])
        .expect_err("non-string display value must refuse");
    assert!(
        error.to_string().contains("default.display.max_rows"),
        "{error}"
    );
}

#[test]
fn test_toml_session_table_sets_builder_knobs() {
    let (_directory, path) = staged_file(
        "[default.session]\nmemory_limit_gb = 2\nbatch_size = 4096\ntarget_partitions = 3\n",
    );
    let session = ReparkSessionBuilder::default()
        .from_config_file(Some(path))
        .build()
        .expect("file-built session");
    let config = session.context().copied_config();
    assert_eq!(config.batch_size(), 4096);
    assert_eq!(config.target_partitions(), 3);
    let file = try_loaded_file(
        "[default.session]\nmemory_limit_gb = 2\nbatch_size = 4096\ntarget_partitions = 3\n",
        &[],
    )
    .expect("fixture loads");
    assert_eq!(file.memory_limit_gb, Some(2));
    assert_eq!(file.batch_size, Some(4096));
    assert_eq!(file.target_partitions, Some(3));
    let error = try_loaded_file("[default.session]\nbatch_size = \"lots\"\n", &[])
        .expect_err("non-integer knob must refuse");
    assert!(
        error.to_string().contains("default.session.batch_size"),
        "{error}"
    );
}

#[test]
fn test_toml_conf_table_applies_in_order() {
    let file = try_loaded_file(
        "[default.conf]\nzebra = \"1\"\napple = \"2\"\nmango = \"3\"\n",
        &[],
    )
    .expect("fixture loads");
    let keys: Vec<&str> = file.pairs.iter().map(|(key, _)| key.as_str()).collect();
    let ranked: Vec<usize> = ["apple", "mango", "zebra"]
        .into_iter()
        .map(|key| {
            keys.iter()
                .position(|candidate| *candidate == key)
                .expect("conf key")
        })
        .collect();
    assert!(
        ranked[0] < ranked[1] && ranked[1] < ranked[2],
        "conf applies in sorted-key order, got {keys:?}"
    );
    let error = try_loaded_file("[default.conf]\nport = 5432.5\n", &[])
        .expect_err("non-string conf value must refuse");
    assert!(error.to_string().contains("default.conf.port"), "{error}");
}

#[test]
fn conf_nested_tables_flatten_with_dot_joins() {
    let file = try_loaded_file(
        "[default.conf]\nspark.sql.warehouse.dir = \"/nested/warehouse\"\n",
        &[],
    )
    .expect("nested conf loads");
    let pairs: HashMap<String, String> = file.pairs.into_iter().collect();
    assert_eq!(
        pairs.get("spark.sql.warehouse.dir").map(String::as_str),
        Some("/nested/warehouse")
    );
    let error = try_loaded_file(
        "[default.conf]\n\"a.b\" = \"1\"\n[default.conf.a]\nb = \"2\"\n",
        &[],
    )
    .expect_err("colliding conf spellings must refuse");
    assert!(error.to_string().contains("default.conf.a.b"), "{error}");
}

#[test]
fn file_builder_precedence_is_builder_then_profile_then_default() {
    let file = try_loaded_file(
        "[default.conf]\nshared = \"from-default\"\ndefault_only = \"d\"\n[prod.conf]\nshared = \"from-prod\"\nprod_only = \"p\"\n",
        &[("REPARK_ENV", "prod")],
    )
    .expect("fixture loads");
    let (path, profile) = file.provenance.clone().expect("forced file provenance");
    assert_eq!(profile, "prod");
    let file_source = format!("file:{}#prod", path.display());
    let cases: Vec<(Option<&str>, &str, String)> = vec![
        (Some("from-builder"), "from-builder", "builder".to_string()),
        (None, "from-prod", file_source.clone()),
    ];
    for (builder_value, expected_value, expected_source) in cases {
        let mut builder_config = HashMap::new();
        if let Some(value) = builder_value {
            builder_config.insert("shared".to_string(), value.to_string());
        }
        let dumped = conf_dump_rows(&file, &builder_config);
        let row = dumped
            .iter()
            .find(|(key, _, _)| key == "shared")
            .expect("shared row");
        assert_eq!(row.1, expected_value);
        assert_eq!(row.2, expected_source);
    }
    let dumped = conf_dump_rows(&file, &HashMap::new());
    let default_row = dumped
        .iter()
        .find(|(key, _, _)| key == "default_only")
        .expect("default row");
    assert_eq!(
        (default_row.1.as_str(), default_row.2.as_str()),
        ("d", "default")
    );
    let prod_row = dumped
        .iter()
        .find(|(key, _, _)| key == "prod_only")
        .expect("prod row");
    assert_eq!(
        (prod_row.1.as_str(), prod_row.2.as_str()),
        ("p", file_source.as_str())
    );
}

#[test]
fn file_dump_reports_origin_and_masks_secrets() {
    let file = try_loaded_file(
        "[default.conf]\npassword = \"s3cret\"\nnickname = \"plain\"\n[prod.conf]\nnickname = \"prod-name\"\n",
        &[("REPARK_ENV", "prod")],
    )
    .expect("fixture loads");
    let dumped = conf_dump_rows(&file, &HashMap::new());
    let password = dumped
        .iter()
        .find(|(key, _, _)| key == "password")
        .expect("password row");
    assert_eq!(password.1, "***");
    assert_eq!(password.2, "default");
    let nickname = dumped
        .iter()
        .find(|(key, _, _)| key == "nickname")
        .expect("nickname row");
    assert_eq!(nickname.1, "prod-name");
    assert!(
        nickname.2.starts_with("file:") && nickname.2.ends_with("#prod"),
        "{}",
        nickname.2
    );
    let mut builder_config = HashMap::new();
    builder_config.insert("nickname".to_string(), "builder-name".to_string());
    let over = conf_dump_rows(&file, &builder_config);
    let winner = over
        .iter()
        .find(|(key, _, _)| key == "nickname")
        .expect("nickname row");
    assert_eq!(
        (winner.1.as_str(), winner.2.as_str()),
        ("builder-name", "builder")
    );
}

#[tokio::test]
async fn file_built_session_registers_the_same_catalogs_as_config_calls() {
    let warehouse = TempDir::new().expect("warehouse fixture");
    let warehouse_text = warehouse.path().to_str().expect("utf8 warehouse");
    let (_directory, path) = staged_file(&format!(
        "[default.catalog.m]\ntype = \"memory\"\nwarehouse = \"{warehouse_text}\"\n"
    ));
    let from_file = ReparkSessionBuilder::default()
        .from_config_file(Some(path))
        .build()
        .expect("file build");
    from_file
        .register_configured_catalogs()
        .await
        .expect("file catalogs register");
    let from_calls = ReparkSessionBuilder::default()
        .config("repark.sql.catalog.m.type", "memory")
        .config("repark.sql.catalog.m.warehouse", warehouse_text)
        .build()
        .expect("config build");
    from_calls
        .register_configured_catalogs()
        .await
        .expect("config catalogs register");
    let paired = |dumped: Vec<(String, String, String)>| {
        dumped
            .into_iter()
            .map(|(key, value, _)| (key, value))
            .collect::<BTreeMap<String, String>>()
    };
    assert_eq!(
        paired(from_file.conf_dump()),
        paired(from_calls.conf_dump())
    );
    assert_eq!(
        from_file
            .table_exists("m.missing.missing")
            .await
            .expect("file probe"),
        from_calls
            .table_exists("m.missing.missing")
            .await
            .expect("config probe")
    );
}

#[test]
fn database_sources_refuse_until_named_registration_lands() {
    let error = try_loaded_file(
        "[default.database.postgres.company_db]\nurl = \"postgresql://localhost/db\"\n",
        &[],
    )
    .expect_err("database source must refuse");
    let message = error.to_string();
    assert!(
        message.contains("default.database.postgres.company_db"),
        "{message}"
    );
    assert!(message.contains("CFG-2"), "{message}");
}
