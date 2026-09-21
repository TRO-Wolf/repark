use datafusion::prelude::{SessionConfig, SessionContext};

use super::*;

#[test]
fn absent_key_is_false_like_spark() {
    let config: HashMap<String, String> = HashMap::new();
    assert!(!merge_schema_from_config_map(&config).expect("absent"));
}

#[test]
fn true_and_false_parse_case_insensitively() {
    assert!(parse_merge_schema_value("TRUE").expect("true"));
    assert!(parse_merge_schema_value(" true ").expect("padded"));
    assert!(!parse_merge_schema_value("False").expect("false"));
}

#[test]
fn a_non_boolean_refuses_with_sparks_conf_value_class() {
    let error = parse_merge_schema_value("yes").expect_err("yes");
    let message = error.to_string();
    assert!(
        message.contains("[INVALID_CONF_VALUE.TYPE_MISMATCH]"),
        "{message}"
    );
    assert!(
        message.contains(SPARK_SQL_ICEBERG_MERGE_SCHEMA_KEY),
        "{message}"
    );
    assert!(message.contains("SQLSTATE: 22022"), "{message}");
}

#[test]
fn the_carrier_round_trips_through_the_session_config() {
    let context =
        SessionContext::new_with_config(with_merge_schema_config(SessionConfig::new(), true));
    assert!(merge_schema_from_options(context.copied_config().options()));
    let bare = SessionContext::new();
    assert!(!merge_schema_from_options(bare.copied_config().options()));
}

#[test]
fn the_extension_refuses_a_direct_prefixed_set() {
    let mut carrier = MergeSchemaConfig::default();
    let error = carrier.set("enabled", "true").expect_err("prefixed set");
    assert!(error.to_string().contains("repark.merge-schema"), "{error}");
}

#[test]
fn only_the_iceberg_spelling_is_a_session_key() {
    assert!(is_merge_schema_session_key(
        SPARK_SQL_ICEBERG_MERGE_SCHEMA_KEY
    ));
    assert!(!is_merge_schema_session_key(
        "spark.sql.iceberg.mergeSchema"
    ));
    assert!(!is_merge_schema_session_key("mergeSchema"));
}
