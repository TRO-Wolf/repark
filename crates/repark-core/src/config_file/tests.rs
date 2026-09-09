use std::collections::HashMap;
use std::path::Path;

use tempfile::TempDir;

use super::discovery::discover;
use super::interpolate::interpolate_table;
use super::profile::effective_table;
use super::{ConfigFile, load, parse};

fn stub_environment(values: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
    let values: HashMap<String, String> = values
        .iter()
        .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
        .collect();
    move |name| values.get(name).cloned()
}

fn write_file(path: &Path, text: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("fixture directories");
    }
    std::fs::write(path, text).expect("fixture file");
}

fn local_config_path(directory: &Path) -> std::path::PathBuf {
    directory.join("repark.toml")
}

fn home_config_path(home: &Path) -> std::path::PathBuf {
    home.join(".config").join("repark").join("repark.toml")
}

#[test]
fn load_without_a_file_is_the_empty_config() {
    assert_eq!(load().expect("empty config"), ConfigFile::default());
}

#[test]
fn an_empty_document_parses_to_the_empty_config() {
    assert_eq!(parse("").expect("empty document"), ConfigFile::default());
}

#[test]
fn an_unknown_top_level_key_refuses_as_a_config_error() {
    let error = parse("nonesuch = 1").expect_err("unknown key must refuse");
    assert!(error.to_string().contains("nonesuch"), "{error}");
}

#[test]
fn repark_config_beats_local_and_home_repark_toml() {
    let home = TempDir::new().expect("home fixture");
    let work = TempDir::new().expect("work fixture");
    let explicit = TempDir::new().expect("explicit fixture");
    write_file(&home_config_path(home.path()), "[default]\n");
    write_file(&local_config_path(work.path()), "[default]\n");
    let explicit_path = explicit.path().join("chosen.toml");
    write_file(&explicit_path, "[default]\n");
    let explicit_text = explicit_path.to_str().expect("utf8 path");
    let environment = stub_environment(&[("REPARK_CONFIG", explicit_text)]);
    let discovered = discover(&environment, work.path(), Some(home.path())).expect("discovery");
    assert_eq!(discovered.as_deref(), Some(explicit_path.as_path()));
}

#[test]
fn local_repark_toml_beats_the_home_copy() {
    let home = TempDir::new().expect("home fixture");
    let work = TempDir::new().expect("work fixture");
    write_file(&home_config_path(home.path()), "[default]\n");
    let local_path = local_config_path(work.path());
    write_file(&local_path, "[default]\n");
    let environment = stub_environment(&[]);
    let discovered = discover(&environment, work.path(), Some(home.path())).expect("discovery");
    assert_eq!(discovered.as_deref(), Some(local_path.as_path()));
}

#[test]
fn the_home_copy_is_found_when_no_local_file_exists() {
    let home = TempDir::new().expect("home fixture");
    let work = TempDir::new().expect("work fixture");
    let home_path = home_config_path(home.path());
    write_file(&home_path, "[default]\n");
    let environment = stub_environment(&[]);
    let discovered = discover(&environment, work.path(), Some(home.path())).expect("discovery");
    assert_eq!(discovered.as_deref(), Some(home_path.as_path()));
}

#[test]
fn no_file_anywhere_discovers_nothing() {
    let home = TempDir::new().expect("home fixture");
    let work = TempDir::new().expect("work fixture");
    let environment = stub_environment(&[]);
    let discovered = discover(&environment, work.path(), Some(home.path())).expect("discovery");
    assert_eq!(discovered, None);
}

#[test]
fn test_repark_config_empty_disables_discovery() {
    let work = TempDir::new().expect("work fixture");
    let local_path = local_config_path(work.path());
    write_file(&local_path, "[default]\n");
    let environment = stub_environment(&[("REPARK_CONFIG", "")]);
    let discovered = discover(&environment, work.path(), None).expect("discovery");
    assert_eq!(discovered, None);
}

#[test]
fn repark_config_naming_a_missing_path_refuses_naming_the_path() {
    let work = TempDir::new().expect("work fixture");
    let missing = work.path().join("absent").join("repark.toml");
    let missing_text = missing.to_str().expect("utf8 path");
    let environment = stub_environment(&[("REPARK_CONFIG", missing_text)]);
    let error = discover(&environment, work.path(), None).expect_err("missing path must refuse");
    assert!(error.to_string().contains(missing_text), "{error}");
}

fn merged_config() -> ConfigFile {
    parse(
        r#"
[default.conf]
"spark.sql.warehouse.dir" = "/default/warehouse"
[default.display]
style = "spark"
[prod.conf]
"spark.sql.warehouse.dir" = "/prod/warehouse"
[prod.display]
max_rows = 20
"#,
    )
    .expect("config fixture")
}

#[test]
fn profile_merge_deep_merges_the_profile_over_default() {
    let config = merged_config();
    let effective = effective_table(&config, Some("prod")).expect("effective table");
    let conf = effective
        .get("conf")
        .expect("conf table")
        .as_table()
        .expect("conf is a table");
    assert_eq!(
        conf.get("spark.sql.warehouse.dir"),
        Some(&toml::Value::from("/prod/warehouse"))
    );
    let display = effective
        .get("display")
        .expect("display table")
        .as_table()
        .expect("display is a table");
    assert_eq!(display.get("style"), Some(&toml::Value::from("spark")));
    assert_eq!(display.get("max_rows"), Some(&toml::Value::from(20)));
}

#[test]
fn without_repark_env_the_default_table_stands_alone() {
    let config = merged_config();
    let effective = effective_table(&config, None).expect("effective table");
    let display = effective
        .get("display")
        .expect("display table")
        .as_table()
        .expect("display is a table");
    assert_eq!(display.get("style"), Some(&toml::Value::from("spark")));
    assert_eq!(display.get("max_rows"), None);
    let conf = effective
        .get("conf")
        .expect("conf table")
        .as_table()
        .expect("conf is a table");
    assert_eq!(
        conf.get("spark.sql.warehouse.dir"),
        Some(&toml::Value::from("/default/warehouse"))
    );
}

#[test]
fn repark_env_naming_an_absent_profile_refuses_naming_the_profile() {
    let config = merged_config();
    let error = effective_table(&config, Some("staging")).expect_err("absent profile must refuse");
    assert!(error.to_string().contains("staging"), "{error}");
}

#[test]
fn an_unknown_key_inside_a_profile_refuses_with_the_key_path() {
    let error = parse("[default]\nnonesuch = 1").expect_err("unknown key must refuse");
    assert!(error.to_string().contains("default.nonesuch"), "{error}");
}

#[test]
fn an_unknown_display_key_refuses_with_the_key_path() {
    let error =
        parse("[default.display]\nnonesuch = 1").expect_err("unknown display key must refuse");
    assert!(
        error.to_string().contains("default.display.nonesuch"),
        "{error}"
    );
}

#[test]
fn an_unknown_session_key_refuses_with_the_key_path() {
    let error = parse("[prod.session]\nnonesuch = 1").expect_err("unknown session key must refuse");
    assert!(
        error.to_string().contains("prod.session.nonesuch"),
        "{error}"
    );
}

#[test]
fn a_non_table_display_or_session_refuses_naming_the_path() {
    let display_error = parse("[default]\ndisplay = 3").expect_err("non-table display must refuse");
    assert!(
        display_error.to_string().contains("default.display"),
        "{display_error}"
    );
    let session_error =
        parse("[prod]\nsession = \"x\"").expect_err("non-table session must refuse");
    assert!(
        session_error.to_string().contains("prod.session"),
        "{session_error}"
    );
}

fn interpolation_fixture() -> toml::Table {
    let mut conf = toml::Table::new();
    conf.insert(
        "spark.sql.warehouse.dir".to_string(),
        toml::Value::String("${WAREHOUSE}/tables".to_string()),
    );
    conf.insert(
        "spark.sql.shuffle.partitions".to_string(),
        toml::Value::Integer(50),
    );
    let mut table = toml::Table::new();
    table.insert("conf".to_string(), toml::Value::Table(conf));
    table
}

fn string_fixture(text: &str) -> toml::Table {
    let mut table = toml::Table::new();
    table.insert("value".to_string(), toml::Value::String(text.to_string()));
    table
}

fn string_of(table: &toml::Table) -> String {
    match table.get("value") {
        Some(toml::Value::String(text)) => text.clone(),
        other => panic!("expected a string value, got {other:?}"),
    }
}

#[test]
fn interpolation_expands_variables_in_string_values_only() {
    let table = interpolation_fixture();
    let environment = stub_environment(&[("WAREHOUSE", "/tmp/warehouse")]);
    let interpolated = interpolate_table(&table, &environment).expect("interpolation");
    let conf = interpolated
        .get("conf")
        .expect("conf table")
        .as_table()
        .expect("conf is a table");
    assert_eq!(
        conf.get("spark.sql.warehouse.dir"),
        Some(&toml::Value::from("/tmp/warehouse/tables"))
    );
    assert_eq!(
        conf.get("spark.sql.shuffle.partitions"),
        Some(&toml::Value::from(50))
    );
}

#[test]
fn a_missing_variable_refuses_naming_the_path_and_the_variable() {
    let table = interpolation_fixture();
    let environment = stub_environment(&[]);
    let error = interpolate_table(&table, &environment).expect_err("missing variable must refuse");
    let message = error.to_string();
    assert!(message.contains("WAREHOUSE"), "{message}");
    assert!(
        message.contains("conf.spark.sql.warehouse.dir"),
        "{message}"
    );
}

#[test]
fn a_dollar_without_a_brace_is_left_alone() {
    let table = string_fixture("cost is $5 and ${TOTAL}");
    let environment = stub_environment(&[("TOTAL", "42")]);
    let interpolated = interpolate_table(&table, &environment).expect("interpolation");
    assert_eq!(string_of(&interpolated), "cost is $5 and 42");
}

#[test]
fn an_unterminated_reference_refuses_naming_the_path() {
    let table = string_fixture("${TOTAL");
    let environment = stub_environment(&[("TOTAL", "42")]);
    let error = interpolate_table(&table, &environment).expect_err("unterminated must refuse");
    let message = error.to_string();
    assert!(message.contains("unterminated"), "{message}");
    assert!(message.contains("`value`"), "{message}");
}

#[test]
fn a_double_dollar_renders_the_reference_verbatim() {
    let table = string_fixture("$${TOTAL}");
    let environment = stub_environment(&[("TOTAL", "42")]);
    let interpolated = interpolate_table(&table, &environment).expect("interpolation");
    assert_eq!(string_of(&interpolated), "${TOTAL}");
}

#[test]
fn a_double_dollar_alone_renders_a_single_dollar() {
    let table = string_fixture("$$");
    let environment = stub_environment(&[]);
    let interpolated = interpolate_table(&table, &environment).expect("interpolation");
    assert_eq!(string_of(&interpolated), "$");
}

#[test]
fn an_empty_variable_name_refuses_naming_the_path() {
    let table = string_fixture("${}");
    let environment = stub_environment(&[]);
    let error = interpolate_table(&table, &environment).expect_err("empty variable must refuse");
    let message = error.to_string();
    assert!(message.contains("empty variable"), "{message}");
    assert!(message.contains("`value`"), "{message}");
}

#[test]
fn interpolation_reaches_array_elements() {
    let mut conf = toml::Table::new();
    conf.insert(
        "list".to_string(),
        toml::Value::Array(vec![
            toml::Value::String("${FIRST}".to_string()),
            toml::Value::String("plain".to_string()),
        ]),
    );
    let mut table = toml::Table::new();
    table.insert("conf".to_string(), toml::Value::Table(conf));
    let environment = stub_environment(&[("FIRST", "x")]);
    let interpolated = interpolate_table(&table, &environment).expect("interpolation");
    let conf = interpolated
        .get("conf")
        .expect("conf table")
        .as_table()
        .expect("conf is a table");
    assert_eq!(
        conf.get("list"),
        Some(&toml::Value::Array(vec![
            toml::Value::String("x".to_string()),
            toml::Value::String("plain".to_string()),
        ]))
    );
}

#[test]
fn a_missing_variable_in_an_unselected_profile_does_not_refuse() {
    let config = parse(
        r#"
[prod.conf]
"spark.sql.warehouse.dir" = "${MISSING_WAREHOUSE}/tables"
"#,
    )
    .expect("config fixture");
    let effective = effective_table(&config, None).expect("effective table");
    let environment = stub_environment(&[]);
    let interpolated =
        interpolate_table(&effective, &environment).expect("unselected profile must not refuse");
    assert!(interpolated.get("conf").is_none());
}
