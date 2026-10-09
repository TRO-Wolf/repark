use std::collections::BTreeMap;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::*;
use crate::exceptions::IllegalArgumentException;

fn options(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

fn message(error: &PyErr, py: Python<'_>) -> String {
    error.value(py).to_string()
}

fn condition(error: &PyErr, py: Python<'_>) -> String {
    error
        .value(py)
        .getattr("_spark_error_class")
        .expect("condition attached")
        .extract::<String>()
        .expect("condition is str")
}

fn params(error: &PyErr, py: Python<'_>) -> BTreeMap<String, String> {
    error
        .value(py)
        .getattr("_spark_message_parameters")
        .expect("params attached")
        .extract::<Bound<'_, PyDict>>()
        .expect("params are a dict")
        .iter()
        .map(|(key, value)| {
            (
                key.extract::<String>().expect("param key"),
                value.extract::<String>().expect("param value"),
            )
        })
        .collect()
}

#[test]
fn output_mode_check_accepts_the_three_modes_in_any_case() {
    Python::attach(|py| {
        for mode in [
            "append", "Append", "APPEND", "complete", "COMPLETE", "Update", "UPDATE",
        ] {
            check_output_mode_value(py, mode).expect("a known mode passes");
        }
    });
}

#[test]
fn output_mode_check_refuses_an_unknown_mode_like_mb0c_o1() {
    Python::attach(|py| {
        let refused = check_output_mode_value(py, "bogus").expect_err("bogus refuses");
        assert!(refused.is_instance_of::<IllegalArgumentException>(py));
        assert_eq!(condition(&refused, py), "STREAMING_OUTPUT_MODE.INVALID");
        assert_eq!(
            params(&refused, py),
            BTreeMap::from([("outputMode".to_string(), "bogus".to_string())])
        );
        assert_eq!(
            message(&refused, py),
            "[STREAMING_OUTPUT_MODE.INVALID] Invalid streaming output mode: bogus. Accepted \
             output modes are 'Append', 'Complete', 'Update'. SQLSTATE: 42KDE"
        );
    });
}

#[allow(
    clippy::duration_suboptimal_units,
    reason = "from_hours and from_mins are unstable on this toolchain"
)]
#[test]
fn time_conf_parser_matches_sparks_measured_grammar() {
    for (text, expected) in [
        ("15s", Duration::from_secs(15)),
        ("100ms", Duration::from_millis(100)),
        ("15000", Duration::from_secs(15)),
        ("1h", Duration::from_secs(3_600)),
        ("0", Duration::ZERO),
        ("-5s", Duration::ZERO),
        ("1m", Duration::from_secs(60)),
        ("1min", Duration::from_secs(60)),
        ("5d", Duration::from_secs(432_000)),
        ("100us", Duration::from_micros(100)),
        ("15S", Duration::from_secs(15)),
        (" 15s ", Duration::from_secs(15)),
        ("1MS", Duration::from_millis(1)),
        ("2H", Duration::from_secs(7_200)),
        ("50", Duration::from_millis(50)),
    ] {
        assert_eq!(parse_time_conf(text), Some(expected), "{text}");
    }
    for text in [
        "abc",
        "1 minute",
        "",
        "1ns",
        "15 s",
        "+5s",
        "1.5s",
        "ms",
        "s",
        "m",
        "1w",
        "-",
        "99999999999999999999999999999999999999d",
    ] {
        assert_eq!(parse_time_conf(text), None, "{text}");
    }
}

#[test]
fn streaming_confs_parse_with_defaults_and_spark_mirror_refusals() {
    Python::attach(|py| {
        let parsed = parse_streaming_confs(py, &options(&[])).expect("empty confs pass");
        assert_eq!(parsed.stop_timeout, None);
        assert_eq!(parsed.polling_delay, DEFAULT_POLLING_DELAY);
        assert_eq!(parsed.recent_limit, DEFAULT_RECENT_PROGRESS);
        let parsed = parse_streaming_confs(
            py,
            &options(&[
                (CONF_STOP_TIMEOUT, "15s"),
                (CONF_POLLING_DELAY, "100ms"),
                (CONF_RECENT_LIMIT, "2"),
            ]),
        )
        .expect("set confs parse");
        assert_eq!(parsed.stop_timeout, Some(Duration::from_secs(15)));
        assert_eq!(parsed.polling_delay, Duration::from_millis(100));
        assert_eq!(parsed.recent_limit, 2);
        let parsed =
            parse_streaming_confs(py, &options(&[(CONF_RECENT_LIMIT, "0")])).expect("zero clamps");
        assert_eq!(parsed.recent_limit, 1);
        let parsed = parse_streaming_confs(py, &options(&[(CONF_RECENT_LIMIT, "-1")]))
            .expect("a negative clamps");
        assert_eq!(parsed.recent_limit, 1);
        for (key, conf_type) in [
            (CONF_STOP_TIMEOUT, "time in MILLISECONDS"),
            (CONF_POLLING_DELAY, "time in MILLISECONDS"),
            (CONF_RECENT_LIMIT, "int"),
        ] {
            let refused =
                parse_streaming_confs(py, &options(&[(key, "abc")])).expect_err("abc refuses");
            assert!(refused.is_instance_of::<IllegalArgumentException>(py));
            assert_eq!(condition(&refused, py), "INVALID_CONF_VALUE.TYPE_MISMATCH");
            assert_eq!(
                message(&refused, py),
                format!(
                    "[INVALID_CONF_VALUE.TYPE_MISMATCH] The value 'abc' in the config \"{key}\" \
                     is invalid. It should be a/an '{conf_type}' value. SQLSTATE: 22022"
                )
            );
            assert_eq!(
                params(&refused, py),
                BTreeMap::from([
                    ("confName".to_string(), key.to_string()),
                    ("confValue".to_string(), String::from("abc")),
                    ("confType".to_string(), conf_type.to_string()),
                ])
            );
        }
    });
}

#[test]
fn trigger_build_maps_kinds_and_refuses_loud_misuse() {
    Python::attach(|py| {
        assert!(matches!(
            build_trigger(py, "default", None).expect("default builds"),
            Trigger::ProcessingTime(zero) if zero.is_zero()
        ));
        assert!(matches!(
            build_trigger(py, "once", None).expect("once builds"),
            Trigger::Once
        ));
        assert!(matches!(
            build_trigger(py, "availableNow", None).expect("availableNow builds"),
            Trigger::AvailableNow
        ));
        assert!(matches!(
            build_trigger(py, "processingTime", Some("5 seconds")).expect("an interval builds"),
            Trigger::ProcessingTime(span) if span == Duration::from_secs(5)
        ));
        let refused = build_trigger(py, "processingTime", None).expect_err("no interval refuses");
        assert!(message(&refused, py).contains("needs an interval string"));
        let refused = build_trigger(py, "bogus", None).expect_err("a bogus kind refuses");
        assert!(message(&refused, py).contains("unknown trigger kind bogus"));
        let refused =
            build_trigger(py, "processingTime", Some("bogus")).expect_err("a bad string refuses");
        assert_eq!(
            refused
                .value(py)
                .getattr("_spark_error_class")
                .expect("condition attached")
                .extract::<String>()
                .expect("condition is str"),
            "INVALID_INTERVAL_FORMAT.UNRECOGNIZED_NUMBER"
        );
    });
}
