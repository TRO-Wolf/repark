//! Spark `parse_url` / `try_parse_url` over raw `java.net.URI`-style components.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use regex::Regex;

mod java_uri;

use java_uri::JavaUri;

/// The URL shims to register (after `datafusion-spark`'s defaults, so these names win).
#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![parse_url_udf(), try_parse_url_udf()]
}

/// Spark `parse_url` — raises `INVALID_URL` on an unparsable URL.
#[must_use]
pub fn parse_url_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkParseUrl {
        fail_on_error: true,
    }))
}

/// Spark `try_parse_url` — NULL on an unparsable URL.
#[must_use]
pub fn try_parse_url_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkParseUrl {
        fail_on_error: false,
    }))
}

/// `parse_url` / `try_parse_url`; the only difference is what an unparsable URL does.
#[derive(Debug)]
struct SparkParseUrl {
    fail_on_error: bool,
}

impl SparkParseUrl {
    fn signature() -> Signature {
        Signature::one_of(
            vec![TypeSignature::String(2), TypeSignature::String(3)],
            Volatility::Immutable,
        )
    }
}

impl PartialEq for SparkParseUrl {
    fn eq(&self, other: &Self) -> bool {
        self.fail_on_error == other.fail_on_error
    }
}

impl Eq for SparkParseUrl {}

impl Hash for SparkParseUrl {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

/// Spark `INVALID_URL` text matches the upstream kernel so existing pins keep matching.
fn invalid_url(value: &str) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(format!(
        "The url is invalid: {value}. Use `try_parse_url` to tolerate invalid URL and return \
         NULL instead. SQLSTATE: 22P02"
    ))
}

/// Spark `ParseUrl`'s query-key pattern: `REGEXPREFIX + key + REGEXSUBFIX`.
fn query_key_pattern(key: &str) -> std::result::Result<Regex, datafusion::error::DataFusionError> {
    Regex::new(&format!("(&|^){key}=([^&]*)")).map_err(|error| {
        datafusion::error::DataFusionError::Execution(format!(
            "parse_url: invalid QUERY key pattern '{key}': {error}"
        ))
    })
}

/// URL parse failures `try_parse_url` tolerates; key-pattern failures it does not.
enum ExtractError {
    /// Spark `INVALID_URL` — NULL under `try_parse_url`.
    InvalidUrl(datafusion::error::DataFusionError),
    /// Java `PatternSyntaxException` — raises under **both** UDFs.
    KeyPattern(datafusion::error::DataFusionError),
}

/// Extract one part.
fn extract(
    value: &str,
    part: &str,
    key: Option<&str>,
    patterns: &mut HashMap<String, Regex>,
) -> std::result::Result<Option<String>, ExtractError> {
    // Non-QUERY keys return NULL before URL parsing, matching Spark's three-argument evaluator.
    if key.is_some() && part != "QUERY" {
        return Ok(None);
    }
    let uri = JavaUri::parse(value).map_err(|_| ExtractError::InvalidUrl(invalid_url(value)))?;
    // HOST and PROTOCOL decode through Java getters; the other parts preserve raw escapes.
    let extracted = match part {
        "HOST" => uri.host().map(str::to_string),
        "PATH" => uri.raw_path().map(str::to_string),
        "QUERY" => match (uri.raw_query(), key) {
            (None, _) => None,
            (Some(query), None) => Some(query.to_string()),
            (Some(query), Some(key)) => {
                if !patterns.contains_key(key) {
                    // Compile after the part check so `try_parse_url` can NULL invalid URLs first.
                    let compiled = query_key_pattern(key).map_err(ExtractError::KeyPattern)?;
                    patterns.insert(key.to_string(), compiled);
                }
                patterns
                    .get(key)
                    .and_then(|pattern| pattern.captures(query))
                    .and_then(|captures| captures.get(2))
                    .map(|matched| matched.as_str().to_string())
            }
        },
        "REF" => uri.raw_fragment().map(str::to_string),
        "PROTOCOL" => uri.scheme().map(str::to_string),
        "FILE" => match (uri.raw_path(), uri.raw_query()) {
            (Some(path), Some(query)) => Some(format!("{path}?{query}")),
            (path, _) => path.map(str::to_string),
        },
        "AUTHORITY" => uri.raw_authority().map(str::to_string),
        "USERINFO" => uri.raw_user_info().map(str::to_string),
        _ => None,
    };
    Ok(extracted)
}

/// Cast a string-family argument to `Utf8` so one row loop serves every string layout.
fn as_utf8(array: &ArrayRef) -> Result<ArrayRef> {
    if array.data_type() == &DataType::Utf8 {
        return Ok(Arc::clone(array));
    }
    cast(array.as_ref(), &DataType::Utf8).map_err(Into::into)
}

impl ScalarUDFImpl for SparkParseUrl {
    fn name(&self) -> &str {
        if self.fail_on_error {
            "parse_url"
        } else {
            "try_parse_url"
        }
    }

    fn signature(&self) -> &Signature {
        static PARSE: std::sync::OnceLock<Signature> = std::sync::OnceLock::new();
        PARSE.get_or_init(SparkParseUrl::signature)
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types.first().cloned().unwrap_or(DataType::Utf8))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 2 || args.args.len() > 3 {
            return exec_err!(
                "`{}` expects 2 or 3 arguments, but got {}",
                self.name(),
                args.args.len()
            );
        }
        let return_type = args.return_field.data_type().clone();
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let url = as_utf8(&arrays[0])?;
        let part = as_utf8(&arrays[1])?;
        let key = match arrays.get(2) {
            Some(array) => Some(as_utf8(array)?),
            None => None,
        };
        let url = url.as_any().downcast_ref::<StringArray>();
        let part = part.as_any().downcast_ref::<StringArray>();
        let (Some(url), Some(part)) = (url, part) else {
            return exec_err!("`{}` expects string arguments", self.name());
        };
        let key_values = match key.as_ref() {
            Some(array) => match array.as_any().downcast_ref::<StringArray>() {
                Some(array) => Some(array.clone()),
                None => return exec_err!("`{}` expects string arguments", self.name()),
            },
            None => None,
        };

        let mut builder = StringBuilder::with_capacity(url.len(), url.len() * 16);
        let mut patterns: HashMap<String, Regex> = HashMap::new();
        for row in 0..url.len() {
            let key_is_null = key_values.as_ref().is_some_and(|array| array.is_null(row));
            if url.is_null(row) || part.is_null(row) || key_is_null {
                builder.append_null();
                continue;
            }
            let key = key_values.as_ref().map(|array| array.value(row));
            match extract(url.value(row), part.value(row), key, &mut patterns) {
                Ok(Some(value)) => builder.append_value(value),
                Err(ExtractError::KeyPattern(error)) => return Err(error),
                Err(ExtractError::InvalidUrl(error)) if self.fail_on_error => return Err(error),
                Ok(None) | Err(ExtractError::InvalidUrl(_)) => builder.append_null(),
            }
        }
        let result: ArrayRef = Arc::new(builder.finish());
        let result = if result.data_type() == &return_type {
            result
        } else {
            cast(result.as_ref(), &return_type)?
        };
        Ok(ColumnarValue::Array(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    fn one(sql: &str) -> Option<String> {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let batches = ctx
                .sql(sql)
                .await
                .expect("plan")
                .collect()
                .await
                .expect("collect");
            let column = batches[0].column(0);
            let strings = cast(column.as_ref(), &DataType::Utf8).expect("utf8");
            let strings = strings
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string array");
            if strings.is_null(0) {
                None
            } else {
                Some(strings.value(0).to_string())
            }
        })
    }

    /// X8 cases preserve Java URI components instead of a WHATWG-normalized URL.
    #[test]
    fn parse_url_matches_java_net_uri_not_the_whatwg_normalizer() {
        assert_eq!(
            one("SELECT parse_url('https://host:443/x', 'AUTHORITY')"),
            Some("host:443".to_string())
        );
        assert_eq!(
            one("SELECT parse_url('HTTPS://Example.COM/x', 'PROTOCOL')"),
            Some("HTTPS".to_string())
        );
        assert_eq!(
            one("SELECT parse_url('HTTPS://Example.COM/x', 'HOST')"),
            Some("Example.COM".to_string())
        );
        assert_eq!(
            one("SELECT parse_url('http://h/a/./b/../c', 'PATH')"),
            Some("/a/./b/../c".to_string())
        );
        assert_eq!(one("SELECT parse_url('http://例え.jp/x', 'HOST')"), None);
        assert_eq!(
            one("SELECT parse_url('http://@host/x', 'USERINFO')"),
            Some(String::new())
        );
        assert_eq!(one("SELECT parse_url('mailto:a@b.com', 'PATH')"), None);
        assert_eq!(
            one("SELECT parse_url('http://h/a/%2e%2e/b', 'PATH')"),
            Some("/a/%2e%2e/b".to_string())
        );
    }

    /// Java raw getters preserve percent escapes for every component except HOST and PROTOCOL.
    #[test]
    fn percent_escapes_are_not_decoded_by_any_part() {
        for (sql, expected) in [
            ("SELECT parse_url('http://h/a%20b', 'PATH')", "/a%20b"),
            ("SELECT parse_url('http://h/a%2Fb', 'PATH')", "/a%2Fb"),
            (
                "SELECT parse_url('http://us%65r@host/x', 'USERINFO')",
                "us%65r",
            ),
            (
                "SELECT parse_url('http://us%65r@host/x', 'AUTHORITY')",
                "us%65r@host",
            ),
            (
                "SELECT parse_url('http://h/p?a=1%26b=2', 'QUERY')",
                "a=1%26b=2",
            ),
            (
                "SELECT parse_url('http://h/p?a=1%26b=2', 'QUERY', 'a')",
                "1%26b=2",
            ),
            ("SELECT parse_url('http://h/p#f%20g', 'REF')", "f%20g"),
            (
                "SELECT parse_url('http://h/a%20b?q=1', 'FILE')",
                "/a%20b?q=1",
            ),
            ("SELECT parse_url('http://us%65r@host/x', 'HOST')", "host"),
        ] {
            assert_eq!(one(sql), Some(expected.to_string()), "{sql}");
        }
    }

    /// The `QUERY` key is a Java regex, not an exact key match.
    #[test]
    fn query_key_is_a_regex() {
        assert_eq!(
            one("SELECT parse_url('https://x/?foo=1', 'QUERY', 'f.o')"),
            Some("1".to_string())
        );
        assert_eq!(
            one("SELECT parse_url('https://x/?foo=1&bar=2', 'QUERY', 'bar')"),
            Some("2".to_string())
        );
        assert_eq!(
            one("SELECT parse_url('https://x/?foo=1', 'QUERY', 'nope')"),
            None
        );
    }

    /// `parse_url` raises where `try_parse_url` NULLs — Spark's `INVALID_URL` split.
    #[test]
    fn invalid_url_raises_on_parse_url_and_nulls_on_try() {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let failed = runtime.block_on(async {
            ctx.sql("SELECT parse_url('not a url', 'HOST')")
                .await
                .expect("plan")
                .collect()
                .await
        });
        let message = failed.expect_err("must raise INVALID_URL").to_string();
        assert!(message.contains("url is invalid"), "{message}");
        assert_eq!(one("SELECT try_parse_url('not a url', 'HOST')"), None);
    }

    /// An invalid query key pattern raises under both UDFs.
    #[test]
    fn uncompilable_query_key_raises_on_both_parse_url_and_try_parse_url() {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        for key in ["(", "[", "a{2,", "a)b"] {
            for udf in ["parse_url", "try_parse_url"] {
                let sql = format!("SELECT {udf}('https://a.b/c?x=1', 'QUERY', '{key}')");
                let failed = runtime
                    .block_on(async { ctx.sql(&sql).await.expect("plan").collect().await })
                    .err()
                    .unwrap_or_else(|| panic!("{udf} with key {key} must raise"));
                let message = failed.to_string();
                assert!(message.contains("invalid QUERY key pattern"), "{message}");
            }
        }
        assert_eq!(
            one("SELECT try_parse_url('not a url', 'QUERY', '(')"),
            None,
            "the URL fails first under try_parse_url, so the key is never compiled"
        );
        assert_eq!(
            one("SELECT parse_url('https://a.b/c?x=1', 'HOST', '(')"),
            None,
            "a 3-arg call with a non-QUERY part short-circuits to NULL before the key compiles"
        );
        assert_eq!(
            one("SELECT parse_url('https://a.b/c', 'QUERY', '(')"),
            None,
            "no raw query ⇒ NULL before the key is compiled"
        );
        assert_eq!(
            one(r"SELECT parse_url('https://a.b/c?a+b=1', 'QUERY', 'a\+b')"),
            Some("1".to_string())
        );
        assert_eq!(
            one(r"SELECT parse_url('https://a.b/c?axb=1', 'QUERY', 'a\+b')"),
            None
        );
    }

    /// A keyed non-QUERY part returns NULL before parsing the URL.
    #[test]
    fn a_key_with_a_non_query_part_is_null_and_never_parses_the_url() {
        assert_eq!(one("SELECT parse_url('https://a.b/c', 'HOST', 'k')"), None);
        assert_eq!(one("SELECT parse_url('https://a.b/c', 'PATH', 'k')"), None);
        assert_eq!(one("SELECT parse_url('not a url', 'HOST', 'k')"), None);
        assert_eq!(
            one("SELECT parse_url('https://a.b/c', 'HOST')"),
            Some("a.b".to_string())
        );
    }

    /// Pin the measured Java-regex versus Rust-regex residual in both directions.
    #[test]
    fn parse_url_query_key_regex_dialect_residual() {
        let url = "http://h/p?a=1&b=2&aa=3&xx=4";
        for (key, expected) in [
            ("(?i)A", Some("1")),
            ("a|b", None),
            ("x{2,3}", Some("4")),
            (r"\d", None),
            ("*", Some("1")),
            (r"\p{Alpha}", Some("1")),
            (r"\p{Lower}", Some("1")),
            (r"\P{Alpha}", None),
            ("a++", Some("1")),
            ("[a-z&&[^b]]", Some("1")),
            ("(?<n>a)", Some("a")),
            (r"\Aa", Some("1")),
        ] {
            let sql = format!("SELECT parse_url('{url}', 'QUERY', '{key}')");
            assert_eq!(one(&sql), expected.map(str::to_string), "{key}");
        }
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        for (key, java_answer) in [
            ("a(?=1)", "NULL"),
            ("(?<=&)b", "'2'"),
            (r"(a)\1", "'a'"),
            ("(?>a)", "'1'"),
            (r"\Qa\E", "'1'"),
        ] {
            for udf in ["parse_url", "try_parse_url"] {
                let sql = format!("SELECT {udf}('{url}', 'QUERY', '{key}')");
                let failed = runtime
                    .block_on(async { ctx.sql(&sql).await.expect("plan").collect().await })
                    .err()
                    .unwrap_or_else(|| {
                        panic!("{udf} key {key} must raise here (Spark answers {java_answer})")
                    });
                assert!(
                    failed.to_string().contains("invalid QUERY key pattern"),
                    "{failed}"
                );
            }
        }
    }

    #[test]
    fn null_inputs_stay_null() {
        assert_eq!(one("SELECT parse_url(CAST(NULL AS STRING), 'HOST')"), None);
        assert_eq!(
            one("SELECT parse_url('https://a.b/c', CAST(NULL AS STRING))"),
            None
        );
        assert_eq!(one("SELECT parse_url('https://a.b/c', 'NOSUCHPART')"), None);
    }

    #[test]
    fn ordinary_extraction_still_works() {
        assert_eq!(
            one("SELECT parse_url('https://spark.apache.org/path?a=1#r', 'HOST')"),
            Some("spark.apache.org".to_string())
        );
        assert_eq!(
            one("SELECT parse_url('https://spark.apache.org/path?a=1#r', 'FILE')"),
            Some("/path?a=1".to_string())
        );
        assert_eq!(
            one("SELECT parse_url('https://spark.apache.org/path?a=1#r', 'REF')"),
            Some("r".to_string())
        );
        assert_eq!(
            one("SELECT try_parse_url('https://spark.apache.org/path', 'HOST')"),
            Some("spark.apache.org".to_string())
        );
    }
}
