use super::{
    column_name_is_secret_shaped, mask_value_credentials, prop_key_is_secret, redact_value,
};

fn masked(value: &str) -> String {
    mask_value_credentials(value)
}

#[test]
fn url_userinfo_password_is_masked_keeping_user_host_and_database() {
    assert_eq!(
        masked("postgresql://alice:S3cretPw@db.example.com:5432/sales"),
        "postgresql://alice:***@db.example.com:5432/sales"
    );
}

#[test]
fn url_userinfo_is_masked_for_every_scheme() {
    let cases = [
        (
            "jdbc:postgresql://u:pw1@h:5432/db",
            "jdbc:postgresql://u:***@h:5432/db",
        ),
        ("postgres://u:pw2@h/db", "postgres://u:***@h/db"),
        ("mysql://u:pw3@h:3306/db", "mysql://u:***@h:3306/db"),
        ("sqlserver://u:pw4@h:1433", "sqlserver://u:***@h:1433"),
        ("redis://:pw5@h:6379/0", "redis://:***@h:6379/0"),
        ("http://u:pw6@h/x", "http://u:***@h/x"),
        ("https://u:pw7@h:8443/x?y=1", "https://u:***@h:8443/x?y=1"),
        ("s3://AKIAX:pw8@bucket/path", "s3://AKIAX:***@bucket/path"),
        (
            "thrift://u:pw9@metastore:9083",
            "thrift://u:***@metastore:9083",
        ),
        ("custom+tls://u:pw10@h", "custom+tls://u:***@h"),
        ("postgresql://u:p@ss@h/db", "postgresql://u:***@h/db"),
        (
            "postgresql://u:pw11@h1:5432,h2:5432/db",
            "postgresql://u:***@h1:5432,h2:5432/db",
        ),
    ];
    for (raw, expected) in cases {
        assert_eq!(masked(raw), expected, "{raw}");
    }
}

#[test]
fn lone_userinfo_is_masked_whole() {
    assert_eq!(
        masked("https://tok123@h.example.com/x"),
        "https://***@h.example.com/x"
    );
    assert_eq!(masked("git+ssh://deploy@h/repo"), "git+ssh://***@h/repo");
}

#[test]
fn userinfo_is_read_only_inside_the_authority() {
    let cases = [
        ("https://h:443/p?m=a@b.c", "https://h:443/p?m=a@b.c"),
        ("https://u:p1@h/x@y", "https://u:***@h/x@y"),
        (
            "https://h.example.com/p#frag:x@y",
            "https://h.example.com/p#frag:x@y",
        ),
        ("https://u:p2@h/p#a:b@c", "https://u:***@h/p#a:b@c"),
        ("https://h.example.com?u:p@x", "https://h.example.com?u:p@x"),
        ("http://[::1]:8080/x@y", "http://[::1]:8080/x@y"),
    ];
    for (raw, expected) in cases {
        assert_eq!(masked(raw), expected, "{raw}");
    }
}

#[test]
fn unclean_userinfo_inside_the_authority_fails_closed() {
    assert_eq!(masked("https://a@b:c@h/x"), "https://***@h/x");
    assert_eq!(
        masked("postgresql://u:p@ss@h/db"),
        "postgresql://u:***@h/db"
    );
}

#[test]
fn every_url_in_a_value_is_masked() {
    assert_eq!(
        masked("primary=postgresql://a:pw1@h1/db replica=postgresql://b:pw2@h2/db"),
        "primary=postgresql://a:***@h1/db replica=postgresql://b:***@h2/db"
    );
}

#[test]
fn secret_query_parameters_are_masked() {
    let cases = [
        (
            "postgresql://h/db?user=a&password=x1&sslmode=require",
            "postgresql://h/db?user=a&password=***&sslmode=require",
        ),
        (
            "postgresql://h/db?sslpassword=x2",
            "postgresql://h/db?sslpassword=***",
        ),
        (
            "https://h/api?access_token=x3&page=2",
            "https://h/api?access_token=***&page=2",
        ),
        (
            "https://b.s3.amazonaws.com/k?X-Amz-Credential=AKIA%2F1&X-Amz-Signature=x4&X-Amz-Expires=60",
            "https://b.s3.amazonaws.com/k?X-Amz-Credential=***&X-Amz-Signature=***&X-Amz-Expires=60",
        ),
        (
            "https://acct.blob.core.windows.net/c?sv=2021&sig=x5",
            "https://acct.blob.core.windows.net/c?sv=2021&sig=***",
        ),
        (
            "jdbc:sqlserver://h:1433;databaseName=d;user=u;password=x6;encrypt=true",
            "jdbc:sqlserver://h:1433;databaseName=d;user=u;password=***;encrypt=true",
        ),
    ];
    for (raw, expected) in cases {
        assert_eq!(masked(raw), expected, "{raw}");
    }
}

#[test]
fn keyword_dsn_secrets_are_masked() {
    let cases = [
        (
            "host=h port=5432 dbname=d user=u password=x1",
            "host=h port=5432 dbname=d user=u password=***",
        ),
        (
            "host=h password = x2 dbname=d",
            "host=h password = *** dbname=d",
        ),
        (
            "host=h password='a b\\'c' dbname=d",
            "host=h password=*** dbname=d",
        ),
        (
            "host=h password=pa&ss;word dbname=d",
            "host=h password=*** dbname=d",
        ),
        (
            "host=h password='unterminated dbname=d",
            "host=h password=***",
        ),
        (
            "Server=h;Database=d;Uid=u;Pwd=x3;",
            "Server=h;Database=d;Uid=u;Pwd=***;",
        ),
        (
            "Server=h; PWD={a;b}}c}; Database=d",
            "Server=h; PWD=***; Database=d",
        ),
        (
            "Driver={ODBC};Server=h;Password=\"q;w\";",
            "Driver={ODBC};Server=h;Password=***;",
        ),
    ];
    for (raw, expected) in cases {
        assert_eq!(masked(raw), expected, "{raw}");
    }
}

#[test]
fn non_secret_values_are_untouched() {
    let cases = [
        "https://db.example.com:5432/sales",
        "postgresql://db.example.com/sales?user=alice&sslmode=require",
        "s3://bucket/warehouse/path/part-0.parquet",
        "alice@example.com",
        "C:\\Users\\alice\\data\\file.csv",
        "host=h port=5432 dbname=d user=u",
        "org.apache.iceberg.aws.glue.GlueCatalog",
        "a=b=c",
        "",
        "password=",
    ];
    for raw in cases {
        assert_eq!(masked(raw), raw, "{raw}");
    }
}

#[test]
fn redact_value_keeps_the_key_rule_and_adds_the_value_rule() {
    assert_eq!(redact_value("password", "plain"), "***");
    assert_eq!(redact_value("s3.secret-access-key", "x"), "***");
    assert_eq!(
        redact_value(
            "url",
            "postgresql://alice:S3cretPw@db.example.com:5432/sales"
        ),
        "postgresql://alice:***@db.example.com:5432/sales"
    );
    assert_eq!(
        redact_value("uri", "jdbc:postgresql://u:p@h/db"),
        "jdbc:postgresql://u:***@h/db"
    );
    assert_eq!(redact_value("user", "alice"), "alice");
}

#[test]
fn prop_key_is_secret_is_unchanged_by_the_move() {
    for key in [
        "password",
        "s3.access-key-id",
        "client.secret",
        "token",
        "key",
        "my_key",
    ] {
        assert!(prop_key_is_secret(key), "{key}");
    }
    for key in ["url", "uri", "user", "host", "bucket_key", "kms_key_arn"] {
        assert!(!prop_key_is_secret(key), "{key}");
    }
}

fn assert_masks(cases: &[(&str, &str)]) {
    for (raw, expected) in cases {
        assert_eq!(masked(raw), *expected, "{raw}");
    }
}

#[test]
fn oracle_thin_and_ezconnect_passwords_are_masked() {
    assert_masks(&[
        (
            "jdbc:oracle:thin:scott/Tiger2026@db.example.com:1521/ORCL",
            "jdbc:oracle:thin:scott/***@db.example.com:1521/ORCL",
        ),
        (
            "jdbc:oracle:thin:scott/Tiger2026@//db.example.com:1521/ORCL",
            "jdbc:oracle:thin:scott/***@//db.example.com:1521/ORCL",
        ),
        (
            "scott/Tiger2026@db.example.com:1521/ORCL",
            "scott/***@db.example.com:1521/ORCL",
        ),
        (
            "scott/\"Ti ger;26\"@db.example.com:1521/ORCL",
            "scott/***@db.example.com:1521/ORCL",
        ),
        (
            "u:BareUserPass5@db.example.com:5432",
            "u:***@db.example.com:5432",
        ),
    ]);
}

#[test]
fn a_parameter_password_carrying_an_at_sign_keeps_the_host() {
    assert_masks(&[
        (
            "jdbc:sqlserver://db:1433;user=sa;password=Spring@2026x;encrypt=true",
            "jdbc:sqlserver://db:1433;user=sa;password=***;encrypt=true",
        ),
        (
            "postgresql://db.example.com:5432/sales?user=u&password=Pa@ssw0rdQ",
            "postgresql://db.example.com:5432/sales?user=u&password=***",
        ),
        (
            "jdbc:mysql://address=(host=h)(user=u)(password=Pw69)/db",
            "jdbc:mysql://address=(host=h)(user=u)(password=***)/db",
        ),
    ]);
}

#[test]
fn whitespace_inside_userinfo_fails_closed() {
    assert_masks(&[
        (
            "postgresql://alice:Two Words9@db.example.com:5432/sales",
            "postgresql://alice:***@db.example.com:5432/sales",
        ),
        (
            "postgresql://alice:Tab\tPw@db.example.com/sales",
            "postgresql://alice:***@db.example.com/sales",
        ),
        (
            "postgresql://alice:New\nLine@db.example.com/sales",
            "postgresql://alice:***@db.example.com/sales",
        ),
        (
            "https://h.example.com:8080 contact admin@x.com",
            "https://h.example.com:8080 contact admin@x.com",
        ),
    ]);
}

#[test]
fn an_unencoded_delimiter_inside_the_password_fails_closed() {
    assert_masks(&[
        (
            "postgresql://alice:pa/ss@h/db",
            "postgresql://alice:***@h/db",
        ),
        (
            "postgresql://alice:pa?ss@h/db",
            "postgresql://alice:***@h/db",
        ),
        (
            "postgresql://alice:pa#ss@h/db",
            "postgresql://alice:***@h/db",
        ),
        (
            "postgresql://alice:pa;ss@h/db",
            "postgresql://alice:***@h/db",
        ),
        (
            "s3a://AKIAX:wJalr/K7MDENG@bucket/warehouse",
            "s3a://AKIAX:***@bucket/warehouse",
        ),
        (
            "https://AbCd/EfGh+IjKl9@git.example.com/repo.git",
            "https://***@git.example.com/repo.git",
        ),
        ("https://h:443/p?m=a@b.c", "https://h:443/p?m=a@b.c"),
        (
            "https://db.example.com/u@x.example.com",
            "https://db.example.com/u@x.example.com",
        ),
        ("https://h/u@example.com", "https://***@example.com"),
        (
            "thrift://:QZX620999@Z#!9]KQV@db.example.com:5432",
            "thrift://:***@db.example.com:5432",
        ),
    ]);
}

#[test]
fn an_empty_userinfo_is_left_alone() {
    assert_masks(&[
        ("postgresql://@h/db", "postgresql://@h/db"),
        ("sqlite:///tmp/x.db", "sqlite:///tmp/x.db"),
    ]);
}

#[test]
fn every_whitespace_kind_ends_an_unquoted_keyword_value() {
    assert_masks(&[
        (
            "host=h password=x1\tdbname=d",
            "host=h password=***\tdbname=d",
        ),
        (
            "host=h password=x2\ndbname=d",
            "host=h password=***\ndbname=d",
        ),
    ]);
}

#[test]
fn the_wider_secret_parameter_names_are_masked() {
    assert_masks(&[
        (
            "mysql://h/db?user=u&passwd=MyPasswd7",
            "mysql://h/db?user=u&passwd=***",
        ),
        ("https://h/x?pass=Pass35", "https://h/x?pass=***"),
        ("https://h/x?pw=Pw36", "https://h/x?pw=***"),
        (
            "jdbc:snowflake://a/?user=u&passcode=Snow27",
            "jdbc:snowflake://a/?user=u&passcode=***",
        ),
        ("https://h/x?apiKey=Api38", "https://h/x?apiKey=***"),
        (
            "https://h/x?client_secret=Cs41",
            "https://h/x?client_secret=***",
        ),
        ("https://h/x?sas=Sas42", "https://h/x?sas=***"),
        (
            "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=QUJDREVGR0g9PQ==;EndpointSuffix=core.windows.net",
            "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=***;EndpointSuffix=core.windows.net",
        ),
        (
            "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=k=",
            "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=***",
        ),
    ]);
}

#[test]
fn json_and_yaml_secret_values_are_masked() {
    assert_masks(&[
        (
            "{\"user\":\"u\",\"password\":\"JsonPw6\"}",
            "{\"user\":\"u\",\"password\":***}",
        ),
        (
            "{\"password\": \"Js\\\"on7\", \"host\": \"h\"}",
            "{\"password\": ***, \"host\": \"h\"}",
        ),
        ("{'secret': 'Js8'}", "{'secret': ***}"),
        (
            "{\"pwd\":12345,\"host\":\"h\"}",
            "{\"pwd\":***,\"host\":\"h\"}",
        ),
        (
            "host: h\npassword: YamlPw8\nport: 5432",
            "host: h\npassword: ***\nport: 5432",
        ),
        (
            "Authorization: Basic dXNlcjpwYXNzNDM=",
            "Authorization: ***",
        ),
        ("host: h\nuser: u", "host: h\nuser: u"),
        ("jdbc:postgresql://h:5432/db", "jdbc:postgresql://h:5432/db"),
    ]);
}

#[test]
fn the_key_rule_covers_azure_account_keys_authorization_and_pats() {
    for key in [
        "spark.hadoop.fs.azure.account.key.acct.dfs.core.windows.net",
        "fs.azure.account.key",
        "spark.sql.catalog.c.header.Authorization",
        "spark.repark.github.pat",
        "pat",
    ] {
        assert!(prop_key_is_secret(key), "{key}");
        assert_eq!(redact_value(key, "QUJDREVGR0g9PQ=="), "***", "{key}");
    }
    for key in ["spark.repark.pattern", "path", "spark.sql.patternMatch"] {
        assert!(!prop_key_is_secret(key), "{key}");
    }
}

#[test]
fn the_column_predicate_keeps_the_pre_widening_rule() {
    for name in ["password", "api_key", "session_token", "my_key"] {
        assert!(column_name_is_secret_shaped(name), "{name}");
    }
    for name in [
        "authorization",
        "account_key_id_col",
        "github_pat",
        "pat",
        "fs_azure_account_key_acct",
    ] {
        assert_eq!(
            column_name_is_secret_shaped(name),
            name.ends_with("_key"),
            "{name}"
        );
    }
}
