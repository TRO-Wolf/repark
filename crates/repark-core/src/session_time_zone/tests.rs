//! Session-timezone pins: validation, one spelling, and resolved engine state.

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

#[test]
fn runtime_values_accept_iana_and_java_offset_forms() {
    for zone in [
        "UTC",
        "GMT",
        "UT",
        "America/New_York",
        "Asia/Tokyo",
        "+05",
        "+5",
        "+0530",
        "+053000",
        "+05:30",
        "+05:30:00",
        "+08:00",
        "+18:00",
        "+18:00:00",
        "+180000",
        "GMT+8",
        "UTC+5",
        "UT+3",
        "Z",
    ] {
        let parsed = parse_runtime_session_zone_value(zone)
            .unwrap_or_else(|error| panic!("runtime must accept {zone:?}: {error}"));
        assert_eq!(parsed.id(), zone);
    }
}

#[test]
fn runtime_values_refuse_past_the_java_range_with_sparks_message() {
    for zone in [
        "+18:01",
        "+19:00",
        "+05:30:30",
        "+053025",
        "+05:30:01",
        "Not/AZone",
        "Mars/Olympus_Mons",
        "Invalid/Zone",
        "gmt+8",
        "z",
        "gmt",
        "",
        "   ",
        "  Asia/Tokyo  ",
        "'Asia/Tokyo'",
    ] {
        let error =
            parse_runtime_session_zone_value(zone).expect_err("runtime must refuse the zone");
        let message = error.to_string();
        assert!(
            message.contains("[INVALID_CONF_VALUE.TIME_ZONE]"),
            "refusal must carry Spark's class: {message}"
        );
        assert!(
            message.contains(SESSION_TIME_ZONE_KEY),
            "refusal must name the conf key: {message}"
        );
        assert!(
            message.contains(&format!("'{zone}'")),
            "refusal must echo the raw value: {message}"
        );
        assert!(
            message.contains("SQLSTATE: 22022"),
            "refusal must carry the SQLSTATE: {message}"
        );
        assert!(
            matches!(error, repark_common::Error::IllegalArgument(_)),
            "a refused runtime VALUE is IllegalArgument (-> IllegalArgumentException): {error:?}"
        );
    }
}

#[test]
fn canonical_zone_id_maps_java_forms_to_arrow_forms() {
    for (raw, canonical) in canonical_zone_table() {
        assert_eq!(canonical_session_zone_id(&raw), canonical, "raw {raw:?}");
    }
    assert_eq!(canonical_session_zone_id("+18:01"), "+18:01");
}

fn canonical_zone_table() -> Vec<(String, String)> {
    include_str!("canonical_zone_table.txt")
        .lines()
        .filter_map(|line| {
            let (raw, canonical) = line.split_once(" => ")?;
            Some((raw.to_string(), canonical.to_string()))
        })
        .collect()
}

#[tokio::test]
async fn stored_runtime_zone_is_live_on_the_session_and_its_clones() {
    let session = ReparkSession::builder().build().unwrap();
    assert_eq!(session.session_time_zone().id(), "UTC");
    session.set_runtime_zone(parse_runtime_session_zone_value("Asia/Tokyo").unwrap());
    assert_eq!(session.session_time_zone().id(), "Asia/Tokyo");
    assert_eq!(session.clone().session_time_zone().id(), "Asia/Tokyo");
}

#[test]
fn java_display_zone_id_matches_zone_id_get_id() {
    let table = [
        ("UTC", "UTC"),
        ("Asia/Kathmandu", "Asia/Kathmandu"),
        ("Africa/Monrovia", "Africa/Monrovia"),
        ("Europe/Amsterdam", "Europe/Amsterdam"),
        ("America/New_York", "America/New_York"),
        ("Europe/London", "Europe/London"),
        ("America/St_Johns", "America/St_Johns"),
        ("Asia/Kolkata", "Asia/Kolkata"),
        ("+05:30", "+05:30"),
        ("-08:00", "-08:00"),
        ("Asia/Calcutta", "Asia/Calcutta"),
        ("US/Eastern", "US/Eastern"),
        ("Etc/UTC", "Etc/UTC"),
        ("GMT", "GMT"),
        ("Z", "Z"),
        ("EST", "-05:00"),
        ("PST", "America/Los_Angeles"),
        ("GMT+05:30", "GMT+05:30"),
        ("UTC-3", "UTC-03:00"),
        ("Etc/GMT+5", "Etc/GMT+5"),
        ("+00:00", "Z"),
        ("+14:00", "+14:00"),
        ("Pacific/Apia", "Pacific/Apia"),
        ("MST", "-07:00"),
        ("HST", "-10:00"),
        ("CST", "America/Chicago"),
        ("IST", "Asia/Kolkata"),
        ("JST", "Asia/Tokyo"),
        ("CET", "CET"),
        ("EST5EDT", "EST5EDT"),
        ("CST6CDT", "CST6CDT"),
        ("MST7MDT", "MST7MDT"),
        ("PST8PDT", "PST8PDT"),
        ("UT", "UT"),
    ];
    for (raw, display) in table {
        assert_eq!(java_display_zone_id(raw), display, "raw {raw:?}");
    }
}

#[test]
fn time_parser_policy_parses_legacy_corrected_exception() {
    assert!(
        parse_time_parser_policy("LEGACY")
            .expect("parses")
            .is_legacy()
    );
    assert!(
        parse_time_parser_policy("legacy")
            .expect("parses")
            .is_legacy()
    );
    assert!(
        !parse_time_parser_policy("CORRECTED")
            .expect("parses")
            .is_legacy()
    );
    assert!(
        !parse_time_parser_policy("EXCEPTION")
            .expect("parses")
            .is_legacy()
    );
    assert!(!TimeParserPolicy::default().is_legacy());
    let error = parse_time_parser_policy("BOGUS").expect_err("refuses");
    let message = error.to_string();
    assert!(
        message.contains("[INVALID_CONF_VALUE.OUT_OF_RANGE_OF_OPTIONS]")
            && message.contains(TIME_PARSER_POLICY_KEY),
        "spark class and key surface: {message}"
    );
}

#[test]
fn time_parser_policy_setter_lazy_installs_the_carrier() {
    let session = ReparkSession::builder().build().unwrap();
    assert!(
        session
            .context()
            .copied_config()
            .options()
            .extensions
            .get::<TimeParserPolicyConfig>()
            .is_none()
    );
    session.set_time_parser_policy("LEGACY").expect("sets");
    let policy = session
        .context()
        .copied_config()
        .options()
        .extensions
        .get::<TimeParserPolicyConfig>()
        .expect("carrier installed")
        .policy;
    assert!(policy.is_legacy());
    session.set_time_parser_policy("CORRECTED").expect("sets");
    let policy = session
        .context()
        .copied_config()
        .options()
        .extensions
        .get::<TimeParserPolicyConfig>()
        .expect("carrier kept")
        .policy;
    assert!(!policy.is_legacy());
}

#[test]
fn conf_dump_legacy_detection_matches_builder_spelling() {
    let legacy = vec![(
        TIME_PARSER_POLICY_KEY.to_string(),
        "LEGACY".to_string(),
        "builder".to_string(),
    )];
    assert!(conf_dump_selects_legacy_policy(&legacy));
    let corrected = vec![(
        TIME_PARSER_POLICY_KEY.to_string(),
        "CORRECTED".to_string(),
        "builder".to_string(),
    )];
    assert!(!conf_dump_selects_legacy_policy(&corrected));
    assert!(!conf_dump_selects_legacy_policy(&[]));
    let session = ReparkSession::builder()
        .config(TIME_PARSER_POLICY_KEY, "LEGACY")
        .build()
        .unwrap();
    assert!(conf_dump_selects_legacy_policy(&session.conf_dump()));
}
