//! Session-timezone conf pins (H-1a split A): parse/validate, the ONE spelling, and the value
//! actually reaching engine session state. Extraction semantics are NOT pinned here — they are
//! unchanged in this unit by design (split B owns them).

use std::collections::HashMap;

use super::*;
use crate::ReparkSession;

/// A builder conf map with one entry.
fn conf(key: &str, value: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    map.insert(key.to_string(), value.to_string());
    map
}

// === Parsing + validation ===================================================================

#[test]
fn absent_key_resolves_to_the_utc_default() {
    let resolved = resolve_session_time_zone(&HashMap::<String, String>::new()).unwrap();
    assert_eq!(resolved.id(), "UTC");
    assert_eq!(resolved, SessionTimeZone::default());
    assert_eq!(DEFAULT_SESSION_TIME_ZONE, "UTC");
}

#[test]
fn iana_zone_id_is_accepted_verbatim() {
    let resolved = resolve_session_time_zone(&conf(SESSION_TIME_ZONE_KEY, "America/New_York"))
        .expect("America/New_York is a real IANA zone");
    assert_eq!(resolved.id(), "America/New_York");
    assert_eq!(resolved.to_string(), "America/New_York");
}

#[test]
fn fixed_offset_is_accepted() {
    let resolved = resolve_session_time_zone(&conf(SESSION_TIME_ZONE_KEY, "+05:30"))
        .expect("a fixed offset is a legal session zone");
    assert_eq!(resolved.id(), "+05:30");
}

#[test]
fn padded_value_is_trimmed_not_treated_as_a_different_zone() {
    let resolved = resolve_session_time_zone(&conf(SESSION_TIME_ZONE_KEY, "  Asia/Tokyo \t"))
        .expect("surrounding whitespace is a typo, not a zone");
    assert_eq!(resolved.id(), "Asia/Tokyo");
}

#[test]
fn unknown_zone_fails_loud_naming_the_key() {
    let error = resolve_session_time_zone(&conf(SESSION_TIME_ZONE_KEY, "Mars/Olympus_Mons"))
        .expect_err("an unknown zone must not be silently accepted");
    let message = error.to_string();
    assert!(
        message.contains(SESSION_TIME_ZONE_KEY),
        "the refusal must name the conf key: {message}"
    );
    assert!(
        message.contains("Mars/Olympus_Mons"),
        "the refusal must quote the offending value: {message}"
    );
    assert!(
        matches!(error, repark_common::Error::Config(_)),
        "an invalid conf VALUE is a config error (-> IllegalArgumentException), got: {error:?}"
    );
}

#[test]
fn blank_value_fails_loud_rather_than_falling_back_to_the_default() {
    for blank in ["", "   ", "\t"] {
        let error = resolve_session_time_zone(&conf(SESSION_TIME_ZONE_KEY, blank))
            .expect_err("a blank zone is a typo, never a silent UTC");
        assert!(
            error.to_string().contains(SESSION_TIME_ZONE_KEY),
            "the refusal must name the conf key for value {blank:?}"
        );
    }
}

// === Exactly one spelling ===================================================================

/// The acceptance gate: one authoritative spelling. Lookalikes are unknown `.config` keys and
/// fall through to the default (PySpark's own tolerance) — they are NOT a second way to set the
/// session zone. If a future change adds an alias, this pin reds and the alias becomes a
/// deliberate, reviewed diff instead of an accident.
#[test]
fn lookalike_spellings_are_not_a_second_way_to_set_the_zone() {
    for lookalike in [
        "spark.sql.session.timezone",
        "spark.sql.session.time_zone",
        "spark.sql.sessionTimeZone",
        "repark.sql.session.timeZone",
        "repark.session.timeZone",
        " spark.sql.session.timeZone",
    ] {
        let resolved = resolve_session_time_zone(&conf(lookalike, "America/New_York"))
            .expect("an unknown config key is tolerated, as PySpark tolerates unknown keys");
        assert_eq!(
            resolved.id(),
            "UTC",
            "{lookalike:?} must not be a second spelling of {SESSION_TIME_ZONE_KEY}"
        );
    }
}

// === The value reaches engine session state =================================================

#[tokio::test]
async fn bare_session_carries_the_utc_default() {
    let session = ReparkSession::builder().build().unwrap();
    assert_eq!(session.session_time_zone().id(), "UTC");
}

#[tokio::test]
async fn builder_conf_reaches_the_built_session() {
    let session = ReparkSession::builder()
        .config(SESSION_TIME_ZONE_KEY, "America/New_York")
        .build()
        .unwrap();
    assert_eq!(session.session_time_zone().id(), "America/New_York");
}

/// Resolution happens ONCE, at construction: an invalid zone fails the BUILD, so no session ever
/// exists holding an unresolvable zone (and no query-time parse can surprise a running job).
#[tokio::test]
async fn invalid_zone_fails_the_build_not_a_later_query() {
    let error = ReparkSession::builder()
        .config(SESSION_TIME_ZONE_KEY, "Not/AZone")
        .build()
        .expect_err("an invalid session zone must fail at build");
    assert!(
        error.to_string().contains(SESSION_TIME_ZONE_KEY),
        "build refusal must name the conf key: {error}"
    );
}

/// A session clone shares the resolved zone (the session is cheap to clone by contract, and a
/// clone that re-resolved could disagree with its origin).
#[tokio::test]
async fn session_clone_shares_the_resolved_zone() {
    let session = ReparkSession::builder()
        .config(SESSION_TIME_ZONE_KEY, "Asia/Tokyo")
        .build()
        .unwrap();
    let cloned = session.clone();
    assert_eq!(cloned.session_time_zone(), session.session_time_zone());
    assert_eq!(cloned.session_time_zone().id(), "Asia/Tokyo");
}
