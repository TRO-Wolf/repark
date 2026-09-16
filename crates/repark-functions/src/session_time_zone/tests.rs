//! Pins for the session-zone CARRIER: it carries, and it is not a second way to set the zone.

use std::str::FromStr;

use datafusion::common::config::{ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::prelude::{SessionConfig, SessionContext};

use super::{
    DEFAULT_EXTRACTION_TIME_ZONE, SessionTimeZoneConfig, session_time_zone_from_options,
    with_session_time_zone,
};

#[test]
fn absent_carrier_falls_back_to_the_default_zone() {
    let options = ConfigOptions::default();
    assert_eq!(
        session_time_zone_from_options(&options),
        DEFAULT_EXTRACTION_TIME_ZONE,
        "a bare DataFusion context keeps today's stored-zone behavior, it does not fail"
    );
    assert_eq!(DEFAULT_EXTRACTION_TIME_ZONE, "UTC");
}

#[test]
fn installed_zone_is_readable_back_verbatim() {
    for zone in ["America/New_York", "Asia/Tokyo", "+05:30", "UTC"] {
        let config = with_session_time_zone(SessionConfig::new(), zone);
        assert_eq!(session_time_zone_from_options(config.options()), zone);
    }
}

#[test]
fn installing_twice_keeps_the_last_zone_rather_than_two_truths() {
    let config = with_session_time_zone(SessionConfig::new(), "America/New_York");
    let config = with_session_time_zone(config, "Asia/Tokyo");
    assert_eq!(
        session_time_zone_from_options(config.options()),
        "Asia/Tokyo"
    );
}

#[test]
fn the_carrier_refuses_to_be_set_and_names_the_one_authoritative_key() {
    let mut carrier = SessionTimeZoneConfig::default();
    for key in ["zone", "time_zone", "timeZone", "anything"] {
        let error = carrier
            .set(key, "Asia/Tokyo")
            .expect_err("the carrier must never be settable — that would be a second spelling");
        let message = error.to_string();
        assert!(
            message.contains("spark.sql.session.timeZone"),
            "the refusal must name the ONE key that does set the zone; got {message}"
        );
    }
    assert_eq!(
        carrier.zone(),
        DEFAULT_EXTRACTION_TIME_ZONE,
        "a refused set must not have moved the carrier"
    );
}

#[test]
fn set_zone_swaps_the_live_value_for_a_validated_runtime_set() {
    let mut carrier = SessionTimeZoneConfig::default();
    carrier.set_zone("Asia/Tokyo");
    assert_eq!(carrier.zone(), "Asia/Tokyo");
    assert_eq!(carrier.display(), "Asia/Tokyo");
    carrier.set_zone("UTC");
    assert_eq!(carrier.zone(), "UTC");
    assert_eq!(carrier.display(), "UTC");
}

#[test]
fn set_zone_keeps_the_raw_echo_and_canonicalizes_the_reader() {
    for (raw, canonical) in [
        ("+5", "+05:00"),
        ("+05", "+05:00"),
        ("+0530", "+05:30"),
        ("GMT+8", "+08:00"),
        ("gmt+8", "+08:00"),
        ("UT+3", "+03:00"),
        ("Z", "UTC"),
        ("z", "UTC"),
        ("UTC", "UTC"),
        ("America/New_York", "America/New_York"),
    ] {
        let mut carrier = SessionTimeZoneConfig::default();
        carrier.set_zone(raw);
        assert_eq!(carrier.display(), raw);
        assert_eq!(carrier.zone(), canonical);
        assert!(arrow::array::timezone::Tz::from_str(carrier.zone()).is_ok());
    }
}

#[test]
fn the_carrier_advertises_no_settable_entries() {
    assert!(
        SessionTimeZoneConfig::default().entries().is_empty(),
        "an entry here would surface in `information_schema` settings as a knob a user can turn"
    );
}

#[tokio::test]
async fn the_sql_set_door_cannot_reach_the_carrier() {
    // The acceptance gate is "exactly ONE authoritative spelling". DataFusion resolves an
    // extension namespace on the text before the FIRST `.`, so the two-segment PREFIX makes
    // every `SET` spelling of this carrier a loud miss rather than a working alias.
    let config = with_session_time_zone(SessionConfig::new(), "UTC");
    let ctx = SessionContext::new_with_config(config);
    for statement in [
        "SET repark.session.zone = 'Asia/Tokyo'",
        "SET repark.session.time_zone = 'Asia/Tokyo'",
        "SET spark.sql.session.timeZone = 'Asia/Tokyo'",
    ] {
        let outcome = ctx.sql(statement).await;
        assert!(
            outcome.is_err(),
            "`{statement}` must not be a second way to set the session zone"
        );
    }
    assert_eq!(
        session_time_zone_from_options(ctx.copied_config().options()),
        "UTC",
        "the session's zone is unchanged by every refused spelling"
    );
    assert_eq!(SessionTimeZoneConfig::PREFIX, "repark.session");
}

#[tokio::test]
async fn current_timezone_answers_the_carrier_zone_as_a_non_null_string() {
    let config = with_session_time_zone(SessionConfig::new(), "America/New_York");
    let ctx = SessionContext::new_with_config(config);
    ctx.register_udf(super::current_timezone_udf().as_ref().clone());
    let batches = ctx
        .sql("SELECT current_timezone() tz")
        .await
        .expect("the udf must resolve")
        .collect()
        .await
        .expect("the projection must run");
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    let schema = batch.schema();
    let field = schema.field_with_name("tz").expect("tz column");
    assert!(
        !field.is_nullable(),
        "Spark's current_timezone() is non-null"
    );
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("a utf8 column");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(values.value(0), "America/New_York");
}

#[tokio::test]
async fn current_timezone_defaults_to_utc_without_the_carrier() {
    let ctx = SessionContext::new();
    ctx.register_udf(super::current_timezone_udf().as_ref().clone());
    let batches = ctx
        .sql("SELECT current_timezone()")
        .await
        .expect("the udf must resolve")
        .collect()
        .await
        .expect("the projection must run");
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("a utf8 column");
    assert_eq!(values.value(0), DEFAULT_EXTRACTION_TIME_ZONE);
}
