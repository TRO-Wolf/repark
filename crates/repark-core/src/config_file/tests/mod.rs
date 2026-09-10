mod wiring;

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use tempfile::TempDir;

use crate::catalog_config::{CatalogKind, parse_catalog_specs};

use super::discovery::discover;
use super::interpolate::interpolate_table;
use super::maintenance::{MaintenancePolicy, parse_duration};
use super::profile::effective_table;
use super::redact::redact_config;
use super::sources::{SourceKind, SourceSpec, profile_sources};
use super::wiring::load_file_config;
use super::{ConfigFile, parse};

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
    let work = TempDir::new().expect("work fixture");
    let environment = stub_environment(&[]);
    let discovered = discover(&environment, work.path(), None).expect("discovery");
    assert_eq!(discovered, None);
    let file = load_file_config(None, &environment, work.path(), None).expect("empty config");
    assert!(file.provenance.is_none(), "{:?}", file.provenance);
    assert!(file.pairs.is_empty(), "{:?}", file.pairs);
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

fn single_profile(text: &str) -> super::Profile {
    let config = parse(text).expect("config fixture");
    let (_, profile) = config.profiles.iter().next().expect("one profile");
    profile.clone()
}

fn measured_glue_block() -> HashMap<String, String> {
    HashMap::from([
        (
            "spark.sql.catalog.glue_alt".to_string(),
            "org.apache.iceberg.spark.SparkCatalog".to_string(),
        ),
        (
            "spark.sql.catalog.glue_alt.catalog-impl".to_string(),
            "org.apache.iceberg.aws.glue.GlueCatalog".to_string(),
        ),
        (
            "spark.sql.catalog.glue_alt.warehouse".to_string(),
            "s3://example-team-spark-iceberg-glue-v1/".to_string(),
        ),
        (
            "spark.sql.catalog.glue_alt.io-impl".to_string(),
            "org.apache.iceberg.aws.s3.S3FileIO".to_string(),
        ),
    ])
}

#[test]
fn a_toml_catalog_block_matches_the_equivalent_config_calls_byte_for_byte() {
    let flat = parse_catalog_specs(&measured_glue_block()).expect("flat specs");
    let profile = single_profile(
        r#"
[default.catalog.glue_alt]
catalog-impl = "org.apache.iceberg.aws.glue.GlueCatalog"
warehouse = "s3://example-team-spark-iceberg-glue-v1/"
io-impl = "org.apache.iceberg.aws.s3.S3FileIO"
"#,
    );
    let sources = profile_sources("default", &profile).expect("profile sources");
    assert_eq!(sources.catalogs, flat);
    assert!(sources.sources.is_empty());
}

#[test]
fn java_class_and_native_type_spellings_produce_the_same_spec() {
    let arn = "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket";
    let glue_java = single_profile(
        r#"
[default.catalog.g]
catalog-impl = "org.apache.iceberg.aws.glue.GlueCatalog"
warehouse = "s3://bucket/wh"
"#,
    );
    let glue_native = single_profile(
        r#"
[default.catalog.g]
type = "glue"
warehouse = "s3://bucket/wh"
"#,
    );
    let s3tables_java = single_profile(&format!(
        r#"
[default.catalog.tb]
catalog-impl = "org.apache.iceberg.aws.s3tables.S3TablesCatalog"
warehouse = "{arn}"
"#
    ));
    let s3tables_native = single_profile(&format!(
        r#"
[default.catalog.tb]
type = "s3tables"
warehouse = "{arn}"
"#
    ));
    let glue_specs = (
        profile_sources("default", &glue_java).expect("glue java"),
        profile_sources("default", &glue_native).expect("glue native"),
    );
    let s3tables_specs = (
        profile_sources("default", &s3tables_java).expect("s3tables java"),
        profile_sources("default", &s3tables_native).expect("s3tables native"),
    );
    assert_eq!(glue_specs.0.catalogs, glue_specs.1.catalogs);
    assert_eq!(glue_specs.1.catalogs[0].kind, CatalogKind::Glue);
    assert_eq!(s3tables_specs.0.catalogs, s3tables_specs.1.catalogs);
    assert_eq!(s3tables_specs.1.catalogs[0].kind, CatalogKind::S3Tables);
    assert_eq!(
        s3tables_specs.1.catalogs[0]
            .props
            .get("table_bucket_arn")
            .map(String::as_str),
        Some(arn),
        "the warehouse ARN translation fires on both spellings"
    );
}

#[test]
fn native_type_catalog_blocks_match_the_flat_config_path() {
    let memory_flat = HashMap::from([
        ("spark.sql.catalog.m.type".to_string(), "memory".to_string()),
        (
            "spark.sql.catalog.m.warehouse".to_string(),
            "/tmp/wh".to_string(),
        ),
    ]);
    let memory_toml = single_profile(
        r#"
[default.catalog.m]
type = "memory"
warehouse = "/tmp/wh"
"#,
    );
    let postgres_flat = HashMap::from([
        (
            "spark.sql.catalog.pg.type".to_string(),
            "postgres".to_string(),
        ),
        (
            "spark.sql.catalog.pg.url".to_string(),
            "postgresql://localhost/db".to_string(),
        ),
        ("spark.sql.catalog.pg.user".to_string(), "u".to_string()),
        (
            "spark.sql.catalog.pg.password".to_string(),
            "s3cret".to_string(),
        ),
    ]);
    let postgres_toml = single_profile(
        r#"
[default.catalog.pg]
type = "postgres"
url = "postgresql://localhost/db"
user = "u"
password = "s3cret"
"#,
    );
    assert_eq!(
        profile_sources("default", &memory_toml)
            .expect("memory")
            .catalogs,
        parse_catalog_specs(&memory_flat).expect("flat memory")
    );
    assert_eq!(
        profile_sources("default", &postgres_toml)
            .expect("postgres")
            .catalogs,
        parse_catalog_specs(&postgres_flat).expect("flat postgres")
    );
}

#[test]
fn database_tables_parse_into_source_specs() {
    let profile = single_profile(
        r#"
[prod.database.postgres.company_db]
url = "postgresql://localhost:5432/company"
password = "s3cret"
[prod.database.sqlserver.warehouse_dw]
url = "sqlserver://localhost:1433;database=warehouse_dw"
[prod.database.trino.federated]
url = "http://trino:8080"
"#,
    );
    let sources = profile_sources("prod", &profile).expect("profile sources");
    assert!(sources.catalogs.is_empty());
    assert_eq!(sources.sources.len(), 3);
    let company = sources
        .sources
        .iter()
        .find(|source| source.name == "company_db")
        .expect("company_db");
    assert_eq!(company.kind, SourceKind::Postgres);
    assert_eq!(
        company.props.get("url").map(String::as_str),
        Some("postgresql://localhost:5432/company")
    );
    let warehouse = sources
        .sources
        .iter()
        .find(|source| source.name == "warehouse_dw")
        .expect("warehouse_dw");
    assert_eq!(warehouse.kind, SourceKind::SqlServer);
    let federated = sources
        .sources
        .iter()
        .find(|source| source.name == "federated")
        .expect("federated");
    assert_eq!(federated.kind, SourceKind::Trino);
}

#[test]
fn an_unknown_database_kind_refuses_naming_the_key_path() {
    let profile = single_profile(
        r#"
[prod.database.hive.warehouse]
url = "thrift://localhost:9083"
"#,
    );
    let error = profile_sources("prod", &profile).expect_err("unknown kind must refuse");
    let message = error.to_string();
    assert!(message.contains("prod.database.hive"), "{message}");
    assert!(
        message.contains("postgres, sqlserver, trino"),
        "names the known kinds: {message}"
    );
    let capitalized = single_profile(
        r#"
[prod.database.Postgres.company_db]
url = "postgresql://localhost/db"
"#,
    );
    let error = profile_sources("prod", &capitalized).expect_err("capitalized kind must refuse");
    assert!(
        error.to_string().contains("prod.database.Postgres"),
        "{error}"
    );
}

#[test]
fn a_name_collision_across_the_families_refuses_naming_both_keys() {
    let profile = single_profile(
        r#"
[default.catalog.acme]
type = "memory"
warehouse = "/tmp/wh"
[default.database.postgres.acme]
url = "postgresql://localhost/db"
"#,
    );
    let error = profile_sources("default", &profile).expect_err("collision must refuse");
    let message = error.to_string();
    assert!(message.contains("default.catalog.acme"), "{message}");
    assert!(
        message.contains("default.database.postgres.acme"),
        "{message}"
    );
}

#[test]
fn a_name_collision_inside_the_database_family_refuses_naming_both_keys() {
    let profile = single_profile(
        r#"
[prod.database.postgres.acme]
url = "postgresql://localhost/db"
[prod.database.trino.acme]
url = "http://trino:8080"
"#,
    );
    let error = profile_sources("prod", &profile).expect_err("collision must refuse");
    let message = error.to_string();
    assert!(message.contains("prod.database.postgres.acme"), "{message}");
    assert!(message.contains("prod.database.trino.acme"), "{message}");
}

#[test]
fn a_dump_masks_every_key_the_secret_predicate_matches() {
    let secret = "SUPER_SECRET_VALUE_do_not_leak";
    let secret_keys = [
        "aws_secret_access_key",
        "password",
        "session_token",
        "access_key_id",
        "token",
        "s3.access-key-id",
        "s3.secret-access-key",
        "credential",
        "accessKey",
        "apikey",
        "privateKey",
        "bearer",
        "basic.auth.user.info",
    ];
    let mut config = HashMap::new();
    config.insert(
        "spark.sql.catalog.pg.url".to_string(),
        "postgresql://localhost/db".to_string(),
    );
    for key in secret_keys {
        config.insert(key.to_string(), secret.to_string());
    }
    let dumped = redact_config(&config);
    assert_eq!(dumped.len(), config.len(), "redaction drops no key");
    assert!(
        !dumped.values().any(|value| value == secret),
        "a secret value leaked: {dumped:?}"
    );
    for key in secret_keys {
        assert_eq!(dumped.get(key).map(String::as_str), Some("***"), "{key}");
    }
    assert_eq!(
        dumped.get("spark.sql.catalog.pg.url").map(String::as_str),
        Some("postgresql://localhost/db"),
        "non-secret values stay verbatim"
    );
}

#[test]
fn a_source_spec_debug_masks_secret_props() {
    let secret = "SUPER_SECRET_VALUE_do_not_leak";
    let source = SourceSpec {
        name: "company_db".to_string(),
        kind: SourceKind::Postgres,
        props: BTreeMap::from([
            ("url".to_string(), "postgresql://localhost/db".to_string()),
            ("password".to_string(), secret.to_string()),
        ]),
    };
    let rendered = format!("{source:?}");
    assert!(!rendered.contains(secret), "leaked: {rendered}");
    assert!(rendered.contains("***"), "{rendered}");
    assert!(
        rendered.contains("postgresql://localhost/db"),
        "non-secret url stays visible: {rendered}"
    );
}

#[test]
fn a_non_string_catalog_prop_refuses_naming_the_key_path() {
    let profile = single_profile(
        r#"
[default.catalog.g]
type = "glue"
max_connections = 10
"#,
    );
    let error = profile_sources("default", &profile).expect_err("non-string must refuse");
    assert!(
        error
            .to_string()
            .contains("default.catalog.g.max_connections"),
        "{error}"
    );
}

#[test]
fn a_non_string_database_prop_refuses_naming_the_key_path() {
    let profile = single_profile(
        r"
[prod.database.postgres.company_db]
port = 5432
",
    );
    let error = profile_sources("prod", &profile).expect_err("non-string must refuse");
    assert!(
        error
            .to_string()
            .contains("prod.database.postgres.company_db.port"),
        "{error}"
    );
}

#[test]
fn a_non_table_catalog_slot_refuses_naming_the_key_path() {
    let profile = single_profile(
        r#"
[default.catalog]
m = "memory"
"#,
    );
    let error = profile_sources("default", &profile).expect_err("non-table must refuse");
    assert!(error.to_string().contains("default.catalog.m"), "{error}");
}

#[test]
fn a_non_table_database_name_slot_refuses_naming_the_key_path() {
    let profile = single_profile(
        r#"
[prod.database.postgres]
company_db = "postgresql://localhost/db"
"#,
    );
    let error = profile_sources("prod", &profile).expect_err("non-table must refuse");
    assert!(
        error
            .to_string()
            .contains("prod.database.postgres.company_db"),
        "{error}"
    );
}

#[test]
fn a_catalog_block_carrying_no_properties_refuses() {
    let profile = single_profile("[default.catalog.ghost]\n");
    let error = profile_sources("default", &profile).expect_err("empty block must refuse");
    let message = error.to_string();
    assert!(message.contains("default.catalog.ghost"), "{message}");
    assert!(message.contains("no properties"), "{message}");
}

fn maintenance_policy_fixture(text: &str) -> MaintenancePolicy {
    let config = parse(text).expect("config fixture");
    let (_, profile) = config.profiles.iter().next().expect("one profile");
    MaintenancePolicy::from_table(
        "default",
        profile.maintenance.as_ref().expect("maintenance table"),
    )
    .expect("policy fixture")
}

#[test]
fn maintenance_policy_parses_every_documented_key() {
    let policy = maintenance_policy_fixture(
        r#"
[default.maintenance]
target_file_size_bytes = 536870912
snapshot_retain_last = 5
snapshot_older_than = "7d"
orphan_older_than = "3d"
rewrite_manifests = true
position_delete_ratio = 0.3
"#,
    );
    assert_eq!(policy.target_file_size_bytes, Some(536_870_912));
    assert_eq!(policy.snapshot_retain_last, Some(5));
    assert_eq!(
        policy
            .snapshot_older_than
            .map(|duration| duration.as_secs()),
        Some(604_800)
    );
    assert_eq!(
        policy.orphan_older_than.map(|duration| duration.as_secs()),
        Some(259_200)
    );
    assert_eq!(policy.rewrite_manifests, Some(true));
    assert_eq!(policy.position_delete_ratio, Some(0.3));
    assert!(policy.tables.is_empty());
}

#[test]
fn an_unknown_maintenance_key_refuses_naming_the_key_path() {
    let error = parse("[default.maintenance]\nnonesuch = 1").expect_err("unknown key must refuse");
    assert!(
        error.to_string().contains("default.maintenance.nonesuch"),
        "{error}"
    );
}

#[test]
fn durations_parse_days_hours_and_minutes() {
    let path = "default.maintenance.snapshot_older_than";
    assert_eq!(parse_duration(path, "7d").expect("days").as_secs(), 604_800);
    assert_eq!(
        parse_duration(path, "12h").expect("hours").as_secs(),
        43_200
    );
    assert_eq!(
        parse_duration(path, "30m").expect("minutes").as_secs(),
        1_800
    );
}

#[test]
fn a_malformed_duration_refuses_naming_the_key() {
    let path = "default.maintenance.snapshot_older_than";
    for bad in ["7", "d", "7w", "1y", "", "seven days", "7 d", "-3d", "1.5h"] {
        let error = parse_duration(path, bad).expect_err("bad duration must refuse");
        assert!(error.to_string().contains(path), "{bad}: {error}");
    }
    let error = parse("[default.maintenance]\nsnapshot_older_than = 7")
        .expect_err("non-string duration must refuse");
    assert!(error.to_string().contains(path), "{error}");
}

#[test]
fn a_table_entry_overrides_the_profile_policy() {
    let policy = maintenance_policy_fixture(
        r#"
[default.maintenance]
target_file_size_bytes = 536870912
snapshot_retain_last = 5
[default.maintenance.tables."glue.silver.orders"]
target_file_size_bytes = 268435456
snapshot_retain_last = 20
[default.maintenance.tables."glue.silver.partial"]
target_file_size_bytes = 134217728
"#,
    );
    let resolved = policy.resolve("glue.silver.orders");
    assert_eq!(resolved.target_file_size_bytes, Some(268_435_456));
    assert_eq!(resolved.snapshot_retain_last, Some(20));
    let partial = policy.resolve("glue.silver.partial");
    assert_eq!(partial.target_file_size_bytes, Some(134_217_728));
    assert_eq!(partial.snapshot_retain_last, Some(5));
    let fallback = policy.resolve("glue.silver.other");
    assert_eq!(fallback.target_file_size_bytes, Some(536_870_912));
    assert_eq!(fallback.snapshot_retain_last, Some(5));
}

#[test]
fn the_adaptive_partitioning_key_refuses_as_not_yet_supported() {
    let error = parse("[default.maintenance]\nadaptive_partitioning = true")
        .expect_err("reserved key must refuse");
    let message = error.to_string();
    assert!(message.contains("adaptive_partitioning"), "{message}");
    assert!(message.contains("not yet supported"), "{message}");
    let error = parse("[default.maintenance.tables.orders]\nadaptive_partitioning = true")
        .expect_err("reserved key must refuse at table level");
    let message = error.to_string();
    assert!(message.contains("adaptive_partitioning"), "{message}");
    assert!(message.contains("not yet supported"), "{message}");
}

#[test]
fn a_catalog_name_with_a_dot_refuses_instead_of_resplitting() {
    let profile = single_profile(
        r#"
[default.catalog."a.b"]
type = "memory"
warehouse = "/tmp/wh"
"#,
    );
    let error = profile_sources("default", &profile).expect_err("dotted name must refuse");
    assert!(error.to_string().contains("default.catalog.a.b"), "{error}");
}

#[test]
fn a_parse_error_carries_position_but_never_the_source_line() {
    let error =
        parse("[default]\naws_access_key_id = AKIA_PROBE\n").expect_err("invalid toml must refuse");
    let message = error.to_string();
    assert!(!message.contains("AKIA_PROBE"), "{message}");
    assert!(
        !message.contains("aws_access_key_id = AKIA_PROBE"),
        "{message}"
    );
    assert!(message.contains("line 2"), "{message}");
}
