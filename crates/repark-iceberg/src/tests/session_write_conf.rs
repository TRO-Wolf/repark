use std::collections::HashMap;

use datafusion::common::config::ConfigOptions;

use crate::write::session_write_conf::{
    SESSION_CODEC_KEY, SESSION_LEVEL_KEY, SESSION_SNAPSHOT_PREFIX, apply_session_extras,
    apply_session_write_key, resolve_empty_session_write, resolve_write_for_session,
    session_write_conf_from_options, unset_session_write_key, with_session_write_conf,
};
use crate::write::write_options::WriterStagingOverrides;
use crate::write::writer_props::parse_compression;
use datafusion::prelude::SessionConfig;

fn options_with(entries: &[(&str, &str)]) -> ConfigOptions {
    let mut options = ConfigOptions::new();
    for (key, value) in entries {
        assert!(apply_session_write_key(&mut options, key, value));
    }
    options
}

#[test]
fn unknown_keys_pass_through_unhandled() {
    let _: &str = "pins: ice-session-write-conf-1/C-033";
    let mut options = ConfigOptions::new();
    assert!(!apply_session_write_key(
        &mut options,
        "spark.sql.other",
        "v"
    ));
    assert!(!unset_session_write_key(&mut options, "spark.sql.other"));
    assert!(session_write_conf_from_options(&options).is_empty());
}

#[test]
fn codec_and_level_store_verbatim() {
    let _: &str = "pins: ice-session-write-conf-1/C-033";
    let options = options_with(&[(SESSION_CODEC_KEY, "GZIP"), (SESSION_LEVEL_KEY, "9")]);
    let view = session_write_conf_from_options(&options);
    assert!(!view.is_empty());
    let (_, staging) =
        resolve_write_for_session(&[], &WriterStagingOverrides::none(), &view).expect("resolve");
    assert_eq!(staging.codec.as_deref(), Some("GZIP"));
    assert_eq!(staging.level.as_deref(), Some("9"));
    assert!(parse_compression(staging.codec.as_deref(), staging.level.as_deref()).is_ok());
}

#[test]
fn snapshot_suffix_folds_case_and_last_write_wins() {
    let _: &str = "pins: ice-session-write-conf-1/C-033";
    let mut options = ConfigOptions::new();
    assert!(apply_session_write_key(
        &mut options,
        "spark.sql.iceberg.snapshot-property.Team",
        "a"
    ));
    assert!(apply_session_write_key(
        &mut options,
        "spark.sql.iceberg.snapshot-property.team",
        "b"
    ));
    let view = session_write_conf_from_options(&options);
    let merged = view.merged_snapshot_extra(&[]);
    assert_eq!(merged, vec![("team".to_string(), "b".to_string())]);
}

#[test]
fn statement_snapshot_wins_over_session() {
    let _: &str = "pins: ice-session-write-conf-1/C-033";
    let options = options_with(&[("spark.sql.iceberg.snapshot-property.team", "a")]);
    let view = session_write_conf_from_options(&options);
    let merged = view.merged_snapshot_extra(&[("team".to_string(), "opt".to_string())]);
    assert_eq!(merged, vec![("team".to_string(), "opt".to_string())]);
}

#[test]
fn statement_codec_and_level_win_over_session() {
    let _: &str = "pins: ice-session-write-conf-1/C-033";
    let options = options_with(&[(SESSION_CODEC_KEY, "gzip"), (SESSION_LEVEL_KEY, "9")]);
    let view = session_write_conf_from_options(&options);
    let statement = WriterStagingOverrides {
        codec: Some("snappy".to_string()),
        level: Some("3".to_string()),
        target_file_size_bytes: None,
    };
    let (_, staging) = resolve_write_for_session(&[], &statement, &view).expect("resolve");
    assert_eq!(staging.codec.as_deref(), Some("snappy"));
    assert_eq!(staging.level.as_deref(), Some("3"));
}

#[test]
fn empty_session_leaves_statement_untouched() {
    let _: &str = "pins: ice-session-write-conf-1/C-033";
    let options = SessionConfig::new().options().clone();
    let view = session_write_conf_from_options(&options);
    assert!(view.is_empty());
    let statement = WriterStagingOverrides {
        codec: Some("zstd".to_string()),
        level: None,
        target_file_size_bytes: Some(8),
    };
    let (snapshot, staging) = resolve_write_for_session(
        &[("team".to_string(), "opt".to_string())],
        &statement,
        &view,
    )
    .expect("resolve");
    assert_eq!(snapshot, vec![("team".to_string(), "opt".to_string())]);
    assert_eq!(staging.codec.as_deref(), Some("zstd"));
    assert_eq!(staging.target_file_size_bytes, Some(8));
}

#[test]
fn bogus_session_codec_refuses_naming_the_codec() {
    let _: &str = "pins: ice-session-write-conf-1/C-033";
    let options = options_with(&[(SESSION_CODEC_KEY, "bogus")]);
    let view = session_write_conf_from_options(&options);
    let error = resolve_write_for_session(&[], &WriterStagingOverrides::none(), &view)
        .expect_err("bogus codec refuses");
    assert!(error.to_string().contains("bogus"), "{error}");
}

#[test]
fn unset_clears_each_shape() {
    let _: &str = "pins: ice-session-write-conf-1/C-033";
    let mut options = ConfigOptions::new();
    assert!(apply_session_write_key(
        &mut options,
        SESSION_CODEC_KEY,
        "gzip"
    ));
    assert!(apply_session_write_key(
        &mut options,
        "spark.sql.iceberg.snapshot-property.team",
        "a"
    ));
    assert!(!session_write_conf_from_options(&options).is_empty());
    assert!(unset_session_write_key(&mut options, SESSION_CODEC_KEY));
    assert!(unset_session_write_key(
        &mut options,
        "spark.sql.iceberg.snapshot-property.team"
    ));
    assert!(session_write_conf_from_options(&options).is_empty());
}

#[test]
fn empty_value_survives_and_reserved_keys_skip() {
    let _: &str = "pins: ice-session-write-conf-1/C-033";
    let options = options_with(&[
        ("spark.sql.iceberg.snapshot-property.team", ""),
        ("spark.sql.iceberg.snapshot-property.operation", "fake"),
    ]);
    let view = session_write_conf_from_options(&options);
    let merged = view.merged_snapshot_extra(&[]);
    assert!(merged.contains(&("team".to_string(), String::new())));
    let mut summary = HashMap::from([("operation".to_string(), "append".to_string())]);
    apply_session_extras(&mut summary, &merged);
    assert_eq!(summary.get("operation").map(String::as_str), Some("append"));
    assert_eq!(summary.get("team").map(String::as_str), Some(""));
}

#[test]
fn empty_resolve_validates_and_returns_empties() {
    let _: &str = "pins: ice-session-write-conf-1/C-033";
    let ctx = datafusion::prelude::SessionContext::new();
    let (snapshot, staging) = resolve_empty_session_write(&ctx).expect("empty resolve");
    assert!(snapshot.is_empty());
    assert!(staging.codec.is_none());
}

#[test]
fn builder_map_installs_carrier() {
    let _: &str = "pins: ice-session-write-conf-1/C-033";
    let map = HashMap::from([
        (SESSION_CODEC_KEY.to_string(), "gzip".to_string()),
        (format!("{SESSION_SNAPSHOT_PREFIX}team"), "a".to_string()),
        ("unrelated".to_string(), "v".to_string()),
    ]);
    let conf = crate::write::session_write_conf::session_write_conf_from_config_map(&map);
    assert!(!conf.is_empty());
    let config = with_session_write_conf(SessionConfig::new(), conf);
    let view = session_write_conf_from_options(config.options());
    let (snapshot, staging) =
        resolve_write_for_session(&[], &WriterStagingOverrides::none(), &view).expect("resolve");
    assert_eq!(staging.codec.as_deref(), Some("gzip"));
    assert_eq!(snapshot, vec![("team".to_string(), "a".to_string())]);
}
