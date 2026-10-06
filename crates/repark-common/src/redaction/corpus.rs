use std::collections::BTreeMap;
use std::panic::catch_unwind;

use super::mask_value_credentials;

const SHAPED_INPUTS: usize = 6_000;
const GARBAGE_INPUTS: usize = 3_000;

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    fn below(&mut self, bound: usize) -> usize {
        usize::try_from(self.next()).unwrap_or(0) % bound
    }

    fn pick<'a>(&mut self, items: &[&'a str]) -> &'a str {
        items[self.below(items.len())]
    }
}

const SOFT: &[&str] = &[
    "a", "b", "Z", "9", "_", "-", ".", "~", "!", "$", "'", "(", ")", "*", "+", ",", "%40", "%3A",
    "%2F", "é", "ß", "€", "😀", "\u{FF1A}", "\u{FF20}", "^", "|", "[", "]",
];
const HARD: &[&str] = &[
    "@", ":", "/", "?", "#", "&", ";", "=", " ", "\n", "\t", "\"", "'", "{", "}", "\\",
];
const SCHEMES: &[&str] = &[
    "postgresql",
    "POSTGRESQL",
    "jdbc:postgresql",
    "jdbc:mysql",
    "mysql",
    "mongodb+srv",
    "redis",
    "rediss",
    "https",
    "HTTP",
    "s3a",
    "thrift",
    "amqp",
    "sqlserver",
    "clickhouse",
    "snowflake",
];
const HOSTS: &[&str] = &[
    "db.example.com",
    "db.example.com:5432",
    "[::1]",
    "[::1]:5432",
    "10.0.0.1:1",
    "h1:5432,h2:5432",
    "xn--bcher-kva.example",
    "bücher.example",
];
const TAILS: &[&str] = &[
    "",
    "/db",
    "/db?sslmode=require",
    "?x=1",
    "#frag",
    "/a/b@c",
    "/x?y=a@b",
];
const QUERY_NAMES: &[&str] = &[
    "password",
    "PASSWORD",
    "Password",
    "sslpassword",
    "access_token",
    "X-Amz-Signature",
    "sig",
    "pwd",
    "passwd",
    "pass",
    "pw",
    "passcode",
    "AccountKey",
    "SharedAccessKey",
    "client_secret",
    "apiKey",
];

fn marker(rng: &mut Lcg, hard: bool, forbidden: &[&str]) -> String {
    let mut password = format!("QZX{}", rng.next() % 1_000_000);
    for _ in 0..rng.below(8) {
        let piece = if hard && rng.below(4) == 0 {
            rng.pick(HARD)
        } else {
            rng.pick(SOFT)
        };
        if !forbidden.contains(&piece) {
            password.push_str(piece);
        }
    }
    password.push_str("KQV");
    password
}

fn survives(output: &str, password: &str) -> bool {
    output.contains(password)
        || password
            .split(|character: char| HARD.iter().any(|hard| hard.contains(character)))
            .filter(|piece| piece.len() >= 6)
            .any(|piece| output.contains(piece))
}

fn quoted_for_libpq(password: &str) -> String {
    if password.contains([' ', '\n', '\t', '\'', '\\', ';', '&', '=']) {
        format!("'{}'", password.replace('\\', "\\\\").replace('\'', "\\'"))
    } else {
        password.to_string()
    }
}

fn escaped_for_json(password: &str) -> String {
    password
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\t', "\\t")
}

fn shaped(rng: &mut Lcg, hard: bool) -> (&'static str, String, String, String) {
    let user = rng.pick(&["u", "alice", "", "a%40b", "AKIAX"]);
    match rng.below(12) {
        0 => {
            let password = marker(rng, hard, &[]);
            let host = rng.pick(HOSTS);
            let input = format!(
                "{}://{user}:{password}@{host}{}",
                rng.pick(SCHEMES),
                rng.pick(TAILS)
            );
            ("url-userinfo", input, password, host.to_string())
        }
        1 => {
            let password = marker(rng, hard, &[]);
            let host = rng.pick(HOSTS);
            let input = format!(
                "{}://{host}{}?user=u&{}={password}&sslmode=require",
                rng.pick(SCHEMES),
                rng.pick(&["/db", ""]),
                rng.pick(QUERY_NAMES)
            );
            ("query-param", input, password, host.to_string())
        }
        2 => {
            let password = marker(rng, hard, &[]);
            let input = format!(
                "host=db.example.com port=5432 {}{}{} dbname=d",
                rng.pick(&["password", "PASSWORD", "Pwd", "PWD"]),
                rng.pick(&["=", " = ", "= ", " ="]),
                quoted_for_libpq(&password)
            );
            ("libpq-dsn", input, password, "db.example.com".to_string())
        }
        3 => {
            let password = marker(rng, hard, &[]);
            let input = format!(
                "Server=db.example.com;Database=d;Uid=u;{}={{{}}};Encrypt=yes",
                rng.pick(&["Pwd", "PWD", "Password"]),
                password.replace('}', "}}")
            );
            ("odbc-braced", input, password, "db.example.com".to_string())
        }
        4 => {
            let password = marker(rng, hard, &[":"]);
            let host = rng.pick(HOSTS);
            let input = format!(
                "{}://{password}@{host}{}",
                rng.pick(SCHEMES),
                rng.pick(TAILS)
            );
            ("lone-token", input, password, host.to_string())
        }
        5 => {
            let password = marker(rng, hard, &[]);
            let input = format!(
                "jdbc:sqlserver://db.example.com:1433;databaseName=d;user=u;{}={password};encrypt=true",
                rng.pick(&["password", "PASSWORD", "pwd"])
            );
            (
                "jdbc-sqlserver",
                input,
                password,
                "db.example.com:1433".to_string(),
            )
        }
        6 => {
            let password = marker(rng, hard, &[]);
            let input = format!(
                "primary={}://u:{password}@h1.example.com/db replica={}://v:{password}@h2.example.com/db",
                rng.pick(SCHEMES),
                rng.pick(SCHEMES)
            );
            ("multi-url", input, password, "h2.example.com".to_string())
        }
        7 => {
            let password = marker(rng, hard, &["\""]);
            let shown = if password.contains([' ', '\n', '\t', ';', ',', '(', ')', '\'', '=']) {
                format!("\"{password}\"")
            } else {
                password.clone()
            };
            let input = format!(
                "{}scott/{shown}@{}db.example.com:1521/ORCL",
                rng.pick(&["jdbc:oracle:thin:", ""]),
                rng.pick(&["", "//"])
            );
            (
                "oracle-ezconnect",
                input,
                password,
                "db.example.com:1521".to_string(),
            )
        }
        8 => {
            let password = marker(rng, hard, &[]);
            let input = format!(
                "{{\"host\":\"db.example.com\",\"user\":\"u\",\"{}\":\"{}\",\"port\":5432}}",
                rng.pick(&["password", "secret", "apiKey", "pwd", "client_secret"]),
                escaped_for_json(&password)
            );
            ("json", input, password, "db.example.com".to_string())
        }
        9 => {
            let password = marker(rng, hard, &[]);
            let shown = if password.contains(['\n', '\t', '#', '"', '\'', ':', '\\', '{', '}']) {
                format!("\"{}\"", escaped_for_json(&password))
            } else {
                password.clone()
            };
            let input = format!(
                "host: db.example.com\nuser: u\n{}: {shown}\nport: 5432",
                rng.pick(&["password", "secret", "token", "pwd"])
            );
            ("yaml", input, password, "db.example.com".to_string())
        }
        10 => {
            let password = marker(rng, hard, &[]);
            let input = format!(
                "jdbc:postgresql://db.example.com:5432/sales?user=u&password={password}&ssl=true"
            );
            (
                "query-at-port",
                input,
                password,
                "db.example.com:5432".to_string(),
            )
        }
        _ => {
            let password = marker(rng, hard, &[]);
            let host = rng.pick(HOSTS);
            let input = format!(
                "{}://{user}:{password}@{host}{}",
                rng.pick(SCHEMES),
                rng.pick(&["", "/db", "/db?sslmode=require"])
            );
            ("whitespace-userinfo", input, password, host.to_string())
        }
    }
}

fn garbage(rng: &mut Lcg) -> String {
    const ATOMS: &[&str] = &[
        "://",
        "@",
        ":",
        "/",
        "?",
        "#",
        "=",
        "&",
        ";",
        " ",
        "'",
        "\"",
        "{",
        "}",
        "}}",
        "\\",
        "%",
        "é",
        "😀",
        "\u{0301}",
        "\u{200B}",
        "\u{3000}",
        "\u{85}",
        "password",
        "pwd",
        "sig",
        "a",
        "Z",
        "9",
        "ß",
        "\u{10FFFF}",
        "\u{FEFF}",
        "\r\n",
        "scott/",
        "\"password\":",
        "pass: ",
    ];
    (0..rng.below(40)).map(|_| rng.pick(ATOMS)).collect()
}

#[test]
fn the_shaped_corpus_never_leaks_its_marker_and_keeps_its_host() {
    let mut rng = Lcg(0x5EC1_0000_0000_0001);
    let mut totals: BTreeMap<&str, usize> = BTreeMap::new();
    let mut failures = Vec::new();
    for case in 0..SHAPED_INPUTS {
        let (class, input, password, host) = shaped(&mut rng, case % 2 == 1);
        *totals.entry(class).or_default() += 1;
        match catch_unwind(|| mask_value_credentials(&input)) {
            Err(_) => failures.push(format!("panic {class}: {input:?}")),
            Ok(output) => {
                if survives(&output, &password) {
                    failures.push(format!("leak {class}: {input:?} -> {output:?}"));
                } else if !output.contains(&host) {
                    failures.push(format!("host {class}: {input:?} -> {output:?}"));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
    assert_eq!(totals.len(), 12, "{totals:?}");
    assert!(totals.values().all(|count| *count > 300), "{totals:?}");
}

#[test]
fn garbage_inputs_never_panic() {
    let mut rng = Lcg(0x5EC1_0000_0000_0002);
    for _ in 0..GARBAGE_INPUTS {
        let input = garbage(&mut rng);
        assert!(
            catch_unwind(|| mask_value_credentials(&input)).is_ok(),
            "{input:?}"
        );
    }
}
