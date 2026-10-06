use std::collections::BTreeMap;
use std::panic::catch_unwind;

use super::{mask_value_credentials, redact_value};

const SHAPED_INPUTS: usize = 9_000;
const GARBAGE_INPUTS: usize = 4_000;

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
const SECRET_WORDS: &[&str] = &[
    "Secret", "token", "pwd", "Password", "apikey", "pass", "pw", "sig",
];
const LOCATIONS: &[&str] = &[
    "s3://my-bucket/events/dt=2024-01-01/user@example.com/part-0.parquet",
    "s3a://data-lake/raw/emails/john@corp.com.json",
    "s3://bucket/2024@x/part.parquet",
    "hdfs://nn/warehouse/db.db/t/dt=2024@x",
    "s3://bucket/path?versionId=a@b",
    "gs://bucket/v1@2/obj",
    "abfss://container@account.dfs.core.windows.net/path/a@b.parquet",
    "wasbs://container@account.blob.core.windows.net/dir",
    "s3://warehouse/ns.db/t/data/email=a@b.co/part.parquet",
    "file:///home/u/a@b/c.csv",
    "s3://lake/teams/data@corp.example.com/ns",
    "s3://bucket@x/path",
    "s3://bucket/plain/path/part.parquet",
];
const FIXED_LEAKS: &[(&str, &str)] = &[
    (
        "jdbc:oracle:thin:scott/Tiger2026@db.example.com:1521/ORCL",
        "Tiger2026",
    ),
    (
        "jdbc:oracle:thin:scott/Tiger2026@//db.example.com:1521/ORCL",
        "Tiger2026",
    ),
    ("scott/Tiger2026@db.example.com:1521/ORCL", "Tiger2026"),
    (
        "jdbc:sqlserver://db:1433;user=sa;password=Spring@2026x;encrypt=true",
        "2026x",
    ),
    (
        "postgresql://db.example.com:5432/sales?user=u&password=Pa@ssw0rdQ",
        "ssw0rdQ",
    ),
    (
        "postgresql://alice:Two Words9@db.example.com:5432/sales",
        "Words9",
    ),
    ("mysql://h/db?user=u&passwd=MyPasswd7", "MyPasswd7"),
    (
        "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=QUJDREVGR0g9PQ==;EndpointSuffix=core.windows.net",
        "QUJDREVGR0g9PQ",
    ),
    ("{\"user\":\"u\",\"password\":\"JsonPw6\"}", "JsonPw6"),
    ("password: YamlPw8", "YamlPw8"),
    (
        "jdbc:oracle:thin:scott/OraDescPw1@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=db.example.com)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ORCL)))",
        "OraDescPw1",
    ),
    (
        "scott/OraDescPw3@(DESCRIPTION=(ADDRESS=(HOST=h)(PORT=1521)))",
        "OraDescPw3",
    ),
    (
        "postgresql://alice:MySecret=Value18@db.example.com/db",
        "MySecret",
    ),
    (
        "postgresql://alice:MyPwd=Value18@db.example.com/db",
        "MyPwd",
    ),
    (
        "jdbc:oracle:thin:scott/Ora,Pw12@db.example.com:1521/ORCL",
        "Pw12",
    ),
    (
        "jdbc:oracle:thin:scott/Ora(Pw15)@db.example.com:1521/ORCL",
        "Pw15",
    ),
    (
        "jdbc:oracle:thin:scott/Ora'Pw16@db.example.com:1521/ORCL",
        "Pw16",
    ),
    (
        "jdbc:oracle:thin:scott/Ora=Pw14@db.example.com:1521/ORCL",
        "Pw14",
    ),
    (
        "jdbc:oracle:thin:\"scott\"/OraPw11@db.example.com:1521/ORCL",
        "OraPw11",
    ),
    ("admin:QZX804085]-)*KQV@db.example.com", "QZX804085"),
    ("password:\n  YamlNext25", "YamlNext25"),
    ("client_secret: |\n  BlockX27", "BlockX27"),
    ("{\n  \"password\":\n    \"JsonNl26\"\n}", "JsonNl26"),
    ("token:\tLeak11", "Leak11"),
    (
        "jdbc:sqlserver://h;user=u@x.example.com;password={Pw@1;x}",
        "Pw@1",
    ),
    ("postgresql://u:p/x@h1 yLeak7@db.example.com/z", "yLeak7"),
    ("user=scott/OraKv1@db.example.com:1521/ORCL", "OraKv1"),
];

fn marker(rng: &mut Lcg, hard: bool) -> String {
    let mut password = format!("QZX{}", rng.next() % 1_000_000);
    for _ in 0..rng.below(8) {
        let piece = if hard && rng.below(4) == 0 {
            rng.pick(HARD)
        } else {
            rng.pick(SOFT)
        };
        password.push_str(piece);
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

struct Case {
    class: &'static str,
    input: String,
    password: String,
    host: Option<&'static str>,
}

fn case(class: &'static str, input: String, password: String, host: Option<&'static str>) -> Case {
    Case {
        class,
        input,
        password,
        host,
    }
}

fn shaped(rng: &mut Lcg, hard: bool) -> Case {
    let password = marker(rng, hard);
    let class = rng.below(17);
    if class < 6 {
        shaped_urls(rng, class, password)
    } else if class < 12 {
        shaped_logins_and_documents(rng, class, hard, password)
    } else {
        shaped_parameters(rng, class, password)
    }
}

fn shaped_urls(rng: &mut Lcg, class: usize, password: String) -> Case {
    let user = rng.pick(&["u", "alice", "", "a%40b", "AKIAX"]);
    let host = rng.pick(HOSTS);
    match class {
        0 => {
            let input = format!(
                "{}://{user}:{password}@{host}{}",
                rng.pick(SCHEMES),
                rng.pick(TAILS)
            );
            case("url-userinfo", input, password, Some(host))
        }
        1 => {
            let input = format!(
                "{}://{host}{}?user=u&{}={password}&sslmode=require",
                rng.pick(SCHEMES),
                rng.pick(&["/db", ""]),
                rng.pick(&[
                    "password",
                    "PASSWORD",
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
                ])
            );
            case("query-param", input, password, Some(host))
        }
        2 => {
            let password = password.replace(':', "");
            let input = format!(
                "{}://{password}@{host}{}",
                rng.pick(SCHEMES),
                rng.pick(TAILS)
            );
            case("lone-token", input, password, Some(host))
        }
        3 => {
            let input = format!(
                "primary={}://u:{password}@h1.example.com/db replica={}://v:{password}@h2.example.com/db",
                rng.pick(SCHEMES),
                rng.pick(SCHEMES)
            );
            case("multi-url", input, password, Some("h2.example.com"))
        }
        4 => {
            let word = rng.pick(SECRET_WORDS);
            let input = if rng.below(2) == 0 {
                format!(
                    "{}://{user}:{}{word}={password}@db.example.com/db",
                    rng.pick(SCHEMES),
                    rng.pick(&["", "ab", "x."])
                )
            } else {
                format!(
                    "{}://{user}:{password}{word}={}@db.example.com/db",
                    rng.pick(SCHEMES),
                    rng.pick(&["", "x", "Zz9=="])
                )
            };
            case("secret-word-in-userinfo", input, password, None)
        }
        _ => {
            let input = format!(
                "jdbc:sqlserver://db.example.com:1433;databaseName=d;user=u;{}={password};encrypt=true",
                rng.pick(&["password", "PASSWORD", "pwd"])
            );
            case(
                "jdbc-sqlserver",
                input,
                password,
                Some("db.example.com:1433"),
            )
        }
    }
}

fn shaped_logins_and_documents(rng: &mut Lcg, class: usize, hard: bool, password: String) -> Case {
    match class {
        6 => {
            let shown = if hard
                && password.contains([
                    '/', '@', ' ', ',', ';', '=', '(', ')', '"', '\'', '\n', '\t', ':', '?', '#',
                    '&', '{', '}', '\\',
                ]) {
                format!("\"{}\"", password.replace('"', ""))
            } else {
                password.clone()
            };
            let input = format!(
                "{}{}/{shown}@{}",
                rng.pick(&[
                    "jdbc:oracle:thin:",
                    "jdbc:oracle:oci:",
                    "",
                    "url=jdbc:oracle:thin:"
                ]),
                rng.pick(&["scott", "app_user", "SYS"]),
                rng.pick(&[
                    "db.example.com:1521/ORCL",
                    "//db.example.com:1521/ORCL",
                    "db.example.com:1521:ORCL",
                    "tcps://db.example.com:2484/ORCL",
                    "db_alias",
                ])
            );
            case("oracle-ezconnect", input, password.replace('"', ""), None)
        }
        7 => {
            let password =
                password.replace(['(', ')', ' ', '\n', '\t', '"', '\'', ',', ';', '='], "");
            let input = format!(
                "jdbc:oracle:thin:{}/{password}@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=db.example.com)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ORCL)))",
                rng.pick(&["scott", "app_user"])
            );
            case(
                "oracle-tns-descriptor",
                input,
                password,
                Some("HOST=db.example.com"),
            )
        }
        8 => {
            let input = format!(
                "{}:{password}@{}",
                rng.pick(&["u", "alice", "admin"]),
                rng.pick(&["db.example.com", "db.example.com:5432", "localhost:6379"])
            );
            case("bare-user-colon-pw", input, password, None)
        }
        9 => {
            let layout = rng.pick(&[
                "{\"user\":\"u\",\"KEY\":\"VAL\"}",
                "{\"KEY\": \"VAL\", \"host\": \"h\"}",
                "{\"a\":{\"KEY\" : \"VAL\"}}",
                "[{\"KEY\":\"VAL\"}]",
            ]);
            let key = rng.pick(&[
                "password",
                "Password",
                "client_secret",
                "pwd",
                "token",
                "apiKey",
            ]);
            let input = layout
                .replace("KEY", key)
                .replace("VAL", &escaped_for_json(&password));
            case("json-inline", input, password, None)
        }
        10 => {
            let key = rng.pick(&["password", "Password", "client_secret", "pwd", "token"]);
            let layout = rng.pick(&[
                "{\n  \"KEY\":\n    \"VAL\"\n}",
                "KEY:\n  VAL",
                "KEY: |\n  VAL",
                "KEY: >-\n  VAL",
            ]);
            let input = layout
                .replace("KEY", key)
                .replace("VAL", &escaped_for_json(&password));
            case("json-yaml-multiline", input, password, None)
        }
        _ => {
            let key = rng.pick(&[
                "password",
                "Password",
                "client_secret",
                "pwd",
                "token",
                "pass",
            ]);
            let value = if rng.below(2) == 0 {
                format!("\"{}\"", escaped_for_json(&password))
            } else {
                password.replace(['\n', '\r'], "")
            };
            let input = format!("{}{key}: {value}\nhost: h", rng.pick(&["", "  ", "- "]));
            case(
                "yaml-inline",
                input,
                password.replace(['\n', '\r'], ""),
                Some("host: h"),
            )
        }
    }
}

fn shaped_parameters(rng: &mut Lcg, class: usize, password: String) -> Case {
    match class {
        12 => {
            let input = format!(
                "host=db.example.com port=5432 {}{}{} dbname=d",
                rng.pick(&["password", "PASSWORD", "Pwd", "PWD"]),
                rng.pick(&["=", " = ", "= ", " ="]),
                quoted_for_libpq(&password)
            );
            case("libpq-dsn", input, password, Some("db.example.com"))
        }
        13 => {
            let input = format!(
                "Server=db.example.com;Database=d;Uid=u;{}={{{}}};Encrypt=yes",
                rng.pick(&["Pwd", "PWD", "Password"]),
                password.replace('}', "}}")
            );
            case("odbc-braced", input, password, Some("db.example.com"))
        }
        14 => {
            let input = format!(
                "{}{}={password}{}",
                rng.pick(&[
                    "",
                    "conn=postgresql://db.example.com/db ",
                    "url=jdbc:postgresql://db.example.com/db;",
                ]),
                rng.pick(&[
                    "password",
                    "pwd",
                    "client_secret",
                    "AccountKey",
                    "SharedAccessKey",
                    "passwd",
                    "pass",
                ]),
                rng.pick(&["", ";user=u", " user=u", "&user=u"])
            );
            case("kv-before-url", input, password, None)
        }
        15 => {
            let password = password.replace(['"', '\\'], "");
            let input = format!(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"{password}\";"
            );
            case("kafka-jaas", input, password, None)
        }
        _ => {
            let password = password.replace(';', "");
            let input = format!(
                "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey={password};EndpointSuffix=core.windows.net"
            );
            case("azure-connstr", input, password, Some("core.windows.net"))
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
        "(",
        ")",
        ",",
        "[",
        "]",
        "\n",
        "\t",
        ": ",
        "scott/",
        "@(",
        "\"password\":",
        "password: ",
        "\u{2028}",
        "\u{00A0}",
        "|\n  ",
        "s3://",
    ];
    (0..rng.below(48)).map(|_| rng.pick(ATOMS)).collect()
}

#[test]
fn the_shaped_corpus_never_leaks_its_marker_and_keeps_its_host() {
    let mut rng = Lcg(0x5EC1_0000_0000_0003);
    let mut totals: BTreeMap<&str, usize> = BTreeMap::new();
    let mut failures = Vec::new();
    for index in 0..SHAPED_INPUTS {
        let shaped = shaped(&mut rng, index % 2 == 1);
        *totals.entry(shaped.class).or_default() += 1;
        let through_key = rng.below(4) == 0;
        let outcome = catch_unwind(|| {
            if through_key {
                redact_value("spark.repark.conn.url", &shaped.input)
            } else {
                mask_value_credentials(&shaped.input)
            }
        });
        match outcome {
            Err(_) => failures.push(format!("panic {}: {:?}", shaped.class, shaped.input)),
            Ok(output) => {
                if survives(&output, &shaped.password) {
                    failures.push(format!(
                        "leak {}: {:?} -> {output:?}",
                        shaped.class, shaped.input
                    ));
                } else if shaped.host.is_some_and(|host| !output.contains(host)) {
                    failures.push(format!(
                        "host {}: {:?} -> {output:?}",
                        shaped.class, shaped.input
                    ));
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
    assert_eq!(totals.len(), 17, "{totals:?}");
    assert!(totals.values().all(|count| *count > 300), "{totals:?}");
}

#[test]
fn every_verdict_repro_is_masked() {
    for (raw, secret) in FIXED_LEAKS {
        let output = mask_value_credentials(raw);
        assert!(!output.contains(secret), "{raw:?} -> {output:?}");
    }
}

#[test]
fn credential_free_storage_locations_are_shown_as_is() {
    for location in LOCATIONS {
        assert_eq!(mask_value_credentials(location), *location);
    }
}

#[test]
fn garbage_inputs_never_panic() {
    let mut rng = Lcg(0x5EC1_0000_0000_0004);
    for _ in 0..GARBAGE_INPUTS {
        let input = garbage(&mut rng);
        assert!(
            catch_unwind(|| mask_value_credentials(&input)).is_ok(),
            "{input:?}"
        );
    }
}
