use std::collections::BTreeMap;

use repark_connect::{
    ConnectError, PostgresSettings, SettingsDoor, SpecRefusal, Spelling, UrlViolation,
};

const TOML: SettingsDoor = SettingsDoor::ReparkToml;
const SPARK: SettingsDoor = SettingsDoor::ReadPostgres;
const SECRET: &str = "S3CRET";

fn from_url(url: &str, door: SettingsDoor) -> Result<PostgresSettings, ConnectError> {
    let props = BTreeMap::from([("url".to_string(), url.to_string())]);
    PostgresSettings::from_props(&props, door)
}

fn endpoint(settings: &PostgresSettings) -> (&str, Option<&str>, &str, u16, &str, &str) {
    (
        settings.user.as_str(),
        settings.password.as_deref(),
        settings.host.as_str(),
        settings.port,
        settings.database.as_str(),
        settings.application_name.as_str(),
    )
}

fn echoes_nothing(error: &ConnectError, fragments: &[&str]) {
    let (display, debug) = (error.to_string(), format!("{error:?}"));
    for fragment in fragments {
        assert!(!display.contains(fragment), "{fragment} echoed: {display}");
        assert!(!debug.contains(fragment), "{fragment} echoed: {debug}");
    }
}

#[test]
fn the_userinfo_ends_at_the_last_at_before_the_first_slash() {
    let passwords = [
        ("S3CRET?leakedfragment=1", "S3CRET%3Fleakedfragment%3D1"),
        ("S3@CRET", "S3%40CRET"),
        ("S3CRET#pw", "S3CRET%23pw"),
        ("S3:CRET", "S3%3ACRET"),
        ("S3CRET?a=b&c", "S3CRET%3Fa%3Db%26c"),
    ];
    for (raw, encoded) in passwords {
        for written in [raw, encoded] {
            let url = format!("postgresql://u:{written}@h:6543/db?application_name=etl");
            let settings = from_url(&url, TOML).expect(written);
            assert_eq!(
                endpoint(&settings),
                ("u", Some(raw), "h", 6543, "db", "etl"),
                "{written}"
            );
            let bare = format!("postgresql://u:{written}@h");
            let settings = from_url(&bare, TOML).expect(written);
            assert_eq!(
                endpoint(&settings),
                ("u", Some(raw), "h", 5432, "u", "repark"),
                "{written}"
            );
        }
    }
    let slash = from_url("postgresql://u:S3CRET%2Fpw@h/db", TOML).expect("an encoded slash");
    assert_eq!(
        endpoint(&slash),
        ("u", Some("S3CRET/pw"), "h", 5432, "db", "repark")
    );
}

#[test]
fn no_userinfo_or_password_text_is_echoed_by_a_refusal() {
    let fragments = [SECRET, "leaked", "pw", "tail"];
    for (url, door) in [
        ("postgresql://u:S3CRET/pw@h/db", TOML),
        ("postgresql://u:S3CRET/leaked?pw=1@h/db", TOML),
        ("postgresql://u:S3CRET%zzpw@h/db", TOML),
        ("postgresql://u:S3CRETpw@h/db?password=S3CRETpw", TOML),
        ("postgresql://u:S3CRET@h/db?leaked=pw", TOML),
        (
            "jdbc:postgresql://h/db?user=u&password=S3CRET&leakedtail=x",
            SPARK,
        ),
        ("jdbc:postgresql://h/db?user=u&password=S3CRET&pw", SPARK),
        (
            "jdbc:postgresql://h/db?user=u&password=S3CRET%zz&pw=1",
            SPARK,
        ),
        ("postgresql://u@h/db?pw%zz=S3CRET", TOML),
    ] {
        let error = from_url(url, door).expect_err(url);
        echoes_nothing(&error, &fragments);
    }
    let error = from_url(
        "jdbc:postgresql://h/db?user=u&password=S3CRET&leakedtail=x",
        SPARK,
    )
    .expect_err("an unknown query key");
    assert_eq!(
        error,
        ConnectError::InvalidSpecification {
            key: Spelling::UrlPart("query key"),
            reason: SpecRefusal::UnknownKey { aliases: true },
        }
    );
    let settings = from_url("postgresql://u:S3CRET?leakedfragment=1@h/db", TOML)
        .expect("libpq takes the fragment as the password");
    assert_eq!(
        settings.password.as_deref(),
        Some("S3CRET?leakedfragment=1")
    );
    let debug = format!("{settings:?}");
    assert!(
        !debug.contains(SECRET) && !debug.contains("leaked"),
        "{debug}"
    );
}

#[test]
fn a_percent_escape_needs_two_hex_digits() {
    let malformed = SpecRefusal::Url(UrlViolation::PercentEncoding);
    for (url, part) in [
        (
            "postgresql://u:S3CRET%+Apw@h/db",
            Spelling::UrlPart("password"),
        ),
        (
            "postgresql://u:S3CRET%-1pw@h/db",
            Spelling::UrlPart("password"),
        ),
        (
            "postgresql://u:S3CRET% Apw@h/db",
            Spelling::UrlPart("password"),
        ),
        (
            "postgresql://u:S3CRET%A@h/db",
            Spelling::UrlPart("password"),
        ),
        ("postgresql://u%g0@h/db", Spelling::UrlPart("user")),
        (
            "postgresql://h/db?user=u&password=S3CRET%+A",
            Spelling::UrlPart("query"),
        ),
    ] {
        let error = from_url(url, TOML).expect_err(url);
        assert_eq!(
            error,
            ConnectError::InvalidSpecification {
                key: part,
                reason: malformed.clone(),
            },
            "{url}"
        );
        echoes_nothing(&error, &[SECRET, "pw"]);
    }
    let settings = from_url("postgresql://u:%2b%2F%aa%41@h/db", TOML);
    assert!(settings.is_err(), "%aa is not UTF-8 alone");
    let settings = from_url("postgresql://u:%2b%2F%41%c3%a9@h/db", TOML).expect("hex either case");
    assert_eq!(settings.password.as_deref(), Some("+/Aé"));
}
