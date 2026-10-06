pub const REDACTED: &str = "***";

#[must_use]
pub fn prop_key_is_secret(key: &str) -> bool {
    let lower = key.to_ascii_lowercase().replace(['-', '.'], "_");
    let compact = lower.replace('_', "");
    lower.contains("aws_secret")
        || lower.contains("secret")
        || lower.contains("password")
        || lower.contains("token")
        || lower.contains("credential")
        || lower.contains("connection_string")
        || lower.ends_with("access_key_id")
        || lower.ends_with("access_key")
        || compact.contains("accesskey")
        || compact.contains("apikey")
        || compact.contains("privatekey")
        || compact == "bearer"
        || compact.ends_with("bearer")
        || lower.contains("user_info")
        || compact.contains("userinfo")
        || lower == "key"
        || lower.ends_with("_key") && !lower.contains("bucket") && !lower.contains("arn")
}

#[must_use]
pub fn redact_value(key: &str, value: &str) -> String {
    if prop_key_is_secret(key) {
        REDACTED.to_string()
    } else {
        mask_value_credentials(value)
    }
}

#[must_use]
pub fn mask_value_credentials(value: &str) -> String {
    mask_secret_parameters(&mask_url_userinfo(value))
}

fn mask_url_userinfo(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(found) = rest.find("://") {
        let authority_start = found + "://".len();
        out.push_str(&rest[..authority_start]);
        let tail = &rest[authority_start..];
        let region_end = tail.find(char::is_whitespace).unwrap_or(tail.len());
        let region = &tail[..region_end];
        let clean_end = region.find(['/', '?', '#']).unwrap_or(region.len());
        let consumed = if let Some(at) = region[..clean_end].rfind('@') {
            push_masked_userinfo(&mut out, &region[..at]);
            at
        } else if let Some(at) = region.rfind('@')
            && region[..at].contains(':')
        {
            out.push_str(REDACTED);
            at
        } else {
            0
        };
        rest = &tail[consumed..];
    }
    out.push_str(rest);
    out
}

fn push_masked_userinfo(out: &mut String, userinfo: &str) {
    if userinfo.is_empty() {
        return;
    }
    if let Some((user, _)) = userinfo.split_once(':') {
        out.push_str(user);
        out.push(':');
    }
    out.push_str(REDACTED);
}

fn mask_secret_parameters(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut copied = 0;
    let mut index = 0;
    while let Some(offset) = value[index..].find('=') {
        let equals = index + offset;
        let name = parameter_name_before(&value[copied..equals]);
        index = equals + 1;
        if name.is_empty() || !parameter_name_is_secret(name) {
            continue;
        }
        let value_start = index + (value[index..].len() - value[index..].trim_start().len());
        let value_end = parameter_value_end(value, value_start);
        if value_end > value_start {
            out.push_str(&value[copied..value_start]);
            out.push_str(REDACTED);
            copied = value_end;
        }
        index = value_end.max(index);
    }
    out.push_str(&value[copied..]);
    out
}

fn parameter_name_before(prefix: &str) -> &str {
    let trimmed = prefix.trim_end();
    let start = trimmed
        .char_indices()
        .rev()
        .find(|(_, character)| !is_parameter_name_char(*character))
        .map_or(0, |(position, character)| position + character.len_utf8());
    &trimmed[start..]
}

fn is_parameter_name_char(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')
}

fn parameter_name_is_secret(name: &str) -> bool {
    if prop_key_is_secret(name) {
        return true;
    }
    let compact: String = name
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|character| character.to_ascii_lowercase())
        .collect();
    compact == "sig" || compact.ends_with("pwd") || compact.ends_with("signature")
}

fn parameter_value_end(value: &str, start: usize) -> usize {
    let rest = &value[start..];
    match rest.as_bytes().first() {
        Some(quote @ (b'\'' | b'"')) => {
            quoted_value_end(rest, *quote).map_or(value.len(), |end| start + end)
        }
        Some(b'{') => braced_value_end(rest).map_or(value.len(), |end| start + end),
        _ => rest
            .char_indices()
            .find(|(position, character)| {
                (matches!(character, '&' | ';') || character.is_whitespace())
                    && starts_next_parameter(&rest[position + character.len_utf8()..])
            })
            .map_or(value.len(), |(position, _)| start + position),
    }
}

fn quoted_value_end(rest: &str, quote: u8) -> Option<usize> {
    let bytes = rest.as_bytes();
    let mut position = 1;
    while position < bytes.len() {
        match bytes[position] {
            b'\\' => position += 2,
            byte if byte == quote => return Some(position + 1),
            _ => position += 1,
        }
    }
    None
}

fn braced_value_end(rest: &str) -> Option<usize> {
    let bytes = rest.as_bytes();
    let mut position = 1;
    while position < bytes.len() {
        if bytes[position] == b'}' {
            if bytes.get(position + 1) == Some(&b'}') {
                position += 2;
                continue;
            }
            return Some(position + 1);
        }
        position += 1;
    }
    None
}

fn starts_next_parameter(after: &str) -> bool {
    let after = after.trim_start_matches(|character: char| {
        character.is_whitespace() || matches!(character, '&' | ';')
    });
    if after.is_empty() {
        return true;
    }
    let name_end = after
        .find(|character: char| !is_parameter_name_char(character))
        .unwrap_or(after.len());
    name_end > 0 && after[name_end..].trim_start().starts_with('=')
}

#[cfg(test)]
mod tests {
    use super::{mask_value_credentials, prop_key_is_secret, redact_value};

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
    fn unclean_userinfo_fails_closed_to_the_whole_userinfo() {
        assert_eq!(
            masked("postgresql://alice:pa/ss@db.example.com:5432/sales"),
            "postgresql://***@db.example.com:5432/sales"
        );
        assert_eq!(masked("mysql://u:p?w#d@h/db"), "mysql://***@h/db");
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
}
