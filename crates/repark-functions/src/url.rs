//! Spark `parse_url` / `try_parse_url` on `java.net.URI`-shaped splitting (X8).
//!
//! `datafusion-spark` 54.1 extracts with `url::Url`, a WHATWG-URL **normalizer**. Spark uses
//! `java.net.URI`, a **splitter**. The two disagree on eleven measured recipes — an explicit
//! `AUTHORITY` port, scheme/host case, dot-segment resolution, an IDN host, empty-userinfo
//! punctuation, an opaque URL's `PATH`, `%2e` as a dot segment, a trailing-dash host label, an
//! underscore host label, an empty authority (`http:///p`), and a key beside a non-`QUERY` part
//! — because normalization rewrites the text before the getter ever runs. [`crate::java_uri`]
//! re-does the splitting; this module is the Spark surface over it.
//!
//! Part names are Spark's: `HOST`, `PATH`, `QUERY`, `REF`, `PROTOCOL`, `FILE`, `AUTHORITY`,
//! `USERINFO`. Anything else is NULL (Spark does not raise on an unknown part).
//!
//! **Which getter each part reads is not a guess — it is MEASURED-JAVAP.** `javap -p -c` over
//! `ParseUrlEvaluator$` from a local `spark-catalyst_2.13-4.1.2.jar` (the pyspark 4.1.2 sdist;
//! the jar is *not* vendored into this repo) gives the map in
//! `$anonfun$getExtractPartFunc$1..8`: `HOST` → `getHost` and `PROTOCOL` → `getScheme` are the
//! only two non-`Raw` getters; `PATH` → `getRawPath`, `QUERY` → `getRawQuery`,
//! `REF` → `getRawFragment`, `AUTHORITY` → `getRawAuthority`, `USERINFO` → `getRawUserInfo`.
//! So `parse_url` never percent-decodes: `PATH` on `http://h/a%20b` is `'/a%20b'`, and
//! `QUERY`-with-key on `?a=1%26b=2` is `'1%26b=2'` rather than a value truncated at a decoded `&`.
//!
//! `FILE` is `$anonfun$…$6`: `getRawQuery() != null ? getRawPath() + "?" + getRawQuery()
//! : getRawPath()` — Spark's own spelling, read off the bytecode.
//!
//! **The `QUERY` key is a Java regex.** Spark compiles `REGEXPREFIX + key + REGEXSUBFIX` and
//! returns group 2 **over the raw query**; the two constants are `"(&|^)"` and `"=([^&]*)"` in
//! the disassembled constant pool (MEASURED-JAVAP), and `extractValueFromQuery` is
//! `matcher.find() ? group(2) : null`. So `parse_url('https://x/?foo=1', 'QUERY', 'f.o')` is
//! `'1'` on Spark. The upstream kernel did exact key equality (NULL) — the FN-GT2 ledger
//! recorded that as a DF-owned residual; re-kernelling closes it. The `regex` crate is
//! linear-time, so a user-supplied key cannot `ReDoS` the engine.
//!
//! **The two failure modes are NOT symmetric, and that is measured.** `TryParseUrl`'s
//! `replacement` is `ParseUrl(params, failOnError = false)` — **not** `TryEval(ParseUrl)`
//! (MEASURED-JAVAP: `TryParseUrl.<init>` builds `ParseUrl.<init>:(Seq;Z)V` with `iconst_0`, and
//! there is no `TryEval` in its constant pool). Consequently:
//!
//! - An **unparsable URL** is the only thing `failOnError` guards. `getUrl`'s exception table
//!   catches `URISyntaxException` alone, so `parse_url` raises `INVALID_URL` and `try_parse_url`
//!   answers NULL.
//! - A key that does not *compile* (`'('`, `'a{2,'`) raises under **both**. `getPattern` calls
//!   `Pattern.compile` and has **no exception table at all**, so the `PatternSyntaxException`
//!   escapes `ParseUrl.eval` whatever `failOnError` says.
//!
//! **RESIDUAL — the key is a `java.util.regex` pattern on Spark and a `regex`-crate pattern
//! here, and the two dialects are not the same language.** This module *introduced* the regex
//! key path (upstream did exact key equality), so it introduced this residual; it is recorded
//! rather than papered over. MEASURED both ways this round (live `java.util.regex.Pattern` on
//! `OpenJDK` 11.0.31 vs `regex` 1.13.1 through both repark doors), raw query
//! `a=1&b=2&aa=3&xx=4`:
//!
//! | key | Java | repark |
//! |---|---|---|
//! | `a(?=1)` lookahead | NULL | **raises** `invalid QUERY key pattern` |
//! | `(?<=&)b` lookbehind | `'2'` | **raises** |
//! | `(a)\1` backreference | `'a'` | **raises** |
//! | `(?>a)` atomic group | `'1'` | **raises** |
//! | `\Qa\E` quoted literal | `'1'` | **raises** |
//!
//! The `regex` crate is a finite automaton, so lookaround, backreferences, atomic groups and
//! `\Q…\E` are not merely unimplemented — they are outside what it can express. A user key
//! using one gets an exception where Spark gets a value or NULL, under `parse_url` **and**
//! `try_parse_url`. Everything else measured agrees, including the constructs it would be
//! easy to assume diverge: `(?i)A`→`'1'`, `a|b`→NULL, `x{2,3}`→`'4'`, `\d`→NULL, `*`→`'1'`,
//! `[a`→raises on both, `\+`→literal `+` on both, `\p{Alpha}`/`\p{Lower}`→`'1'`,
//! `\P{Alpha}`→NULL, `a++` possessive→`'1'`, `[a-z&&[^b]]` class intersection→`'1'`,
//! `(?<n>a)` named group→`'a'`, `\Aa`→`'1'`. See `parse_url_query_key_regex_dialect_residual`.
//!
//! Note the shared quirk in the last two rows: Spark takes `group(2)` of
//! `(&|^)<key>=([^&]*)`, so a key that *itself* opens a capture group shifts the numbering and
//! the answer is the key's own capture rather than the value. Java and repark agree there.
//!
//! Order matters too, and it is the bytecode's: the 3-arg `evaluate` returns NULL immediately
//! when the part is not `QUERY`, then parses the URL, then returns NULL when there is no raw
//! query — the pattern is compiled **last**. So `try_parse_url('not a url','QUERY','(')` is NULL
//! (the URL fails first and the key is never compiled), while
//! `try_parse_url('https://a.b/c?x=1','QUERY','(')` raises.

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

#[path = "java_uri.rs"]
mod java_uri;

use java_uri::JavaUri;

/// ===========================================================================================
/// The URL shims to register (after `datafusion-spark`'s defaults, so these names win).
/// ===========================================================================================
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

/// ===========================================================================================
/// `parse_url` / `try_parse_url`; the only difference is what an unparsable URL does.
/// ===========================================================================================
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

/// Spark's `INVALID_URL` message (byte-identical to the upstream kernel's, so the existing
/// `url is invalid` pins keep matching).
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

/// Which failure this is — because `try_parse_url` tolerates exactly one of them.
///
/// MEASURED-JAVAP: `failOnError` is threaded only into `getUrl`, whose exception table catches
/// `URISyntaxException`; `getPattern` has no exception table, so a `PatternSyntaxException`
/// escapes both UDFs. Collapse these two arms into one and `try_parse_url` starts swallowing a
/// bad key that Spark raises on.
enum ExtractError {
    /// Spark `INVALID_URL` — NULL under `try_parse_url`.
    InvalidUrl(datafusion::error::DataFusionError),
    /// Java `PatternSyntaxException` — raises under **both** UDFs.
    KeyPattern(datafusion::error::DataFusionError),
}

/// Extract one part. `Ok(None)` is Spark's NULL.
fn extract(
    value: &str,
    part: &str,
    key: Option<&str>,
    patterns: &mut HashMap<String, Regex>,
) -> std::result::Result<Option<String>, ExtractError> {
    // MEASURED-JAVAP, and it is the FIRST thing the 3-arg `evaluate` does — before the URL is
    // ever parsed: a key with a part other than `QUERY` short-circuits to NULL. So
    // `parse_url('not a url', 'HOST', 'k')` is NULL on Spark, not `INVALID_URL`. Move this below
    // the parse and that row starts raising.
    if key.is_some() && part != "QUERY" {
        return Ok(None);
    }
    let uri = JavaUri::parse(value).map_err(|_| ExtractError::InvalidUrl(invalid_url(value)))?;
    // The getter per part is the disassembled `ParseUrlEvaluator$` map, not a guess: only HOST
    // (`getHost`) and PROTOCOL (`getScheme`) use a non-`Raw` getter; the other six read the
    // **raw** span, so percent-escapes are never decoded. See `java_uri.rs`'s module doc.
    let extracted = match part {
        "HOST" => uri.host().map(str::to_string),
        "PATH" => uri.raw_path().map(str::to_string),
        "QUERY" => match (uri.raw_query(), key) {
            (None, _) => None,
            (Some(query), None) => Some(query.to_string()),
            (Some(query), Some(key)) => {
                if !patterns.contains_key(key) {
                    // Compiled LAST, exactly like the 3-arg `evaluate`: after the part check and
                    // after the URL parse, so an unparsable URL under `try_parse_url` NULLs
                    // before a bad key can raise.
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
        // A `Signature` is cheap and immutable; a per-impl `OnceLock` keeps the borrow.
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
                // An uncompilable QUERY key escapes both UDFs (`getPattern` has no catch).
                Err(ExtractError::KeyPattern(error)) => return Err(error),
                Err(ExtractError::InvalidUrl(error)) if self.fail_on_error => return Err(error),
                // Absent component, or — `try_parse_url` only — an unparsable URL.
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

    /// The X8 dialect recipes, through the registered SQL name. Reverting the registration to
    /// the upstream kernel turns every one of these red with the normalized answer instead.
    /// Every expectation is MEASURED-JVM (`java.net.URI` on `OpenJDK` 11.0.31 through the
    /// MEASURED-JAVAP getter map).
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

    /// The getter dimension of X8: Spark reads `getRawPath` / `getRawQuery` / `getRawFragment` /
    /// `getRawAuthority` / `getRawUserInfo`, so **no** part but HOST/PROTOCOL is percent-decoded.
    /// Every expectation is MEASURED-JVM — `new java.net.URI(s)` on `OpenJDK` 11.0.31, driven
    /// through the MEASURED-JAVAP getter map. Bind these six parts back to decoding getters and
    /// every row here reds.
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
            // The key regex runs over the RAW query, so the value is not cut at a decoded `&`.
            (
                "SELECT parse_url('http://h/p?a=1%26b=2', 'QUERY', 'a')",
                "1%26b=2",
            ),
            ("SELECT parse_url('http://h/p#f%20g', 'REF')", "f%20g"),
            (
                "SELECT parse_url('http://h/a%20b?q=1', 'FILE')",
                "/a%20b?q=1",
            ),
            // HOST is `getHost` — not raw, but a host cannot hold an escape, so it is unchanged.
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

    /// A key that cannot compile raises under **both** UDFs — MEASURED-JAVAP: `getPattern` calls
    /// `Pattern.compile` with no exception table, and `TryParseUrl`'s replacement is
    /// `ParseUrl(params, failOnError = false)` rather than `TryEval(ParseUrl)`, so `failOnError`
    /// never reaches the compile. Fold `ExtractError::KeyPattern` back into
    /// `ExtractError::InvalidUrl` and the `try_parse_url` half of this reds with `None`.
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
        // Compile order is the bytecode's: part check, then URL, then pattern. So a bad key is
        // never even reached on these three, and they stay NULL rather than raising.
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
        // An escaped metacharacter compiles and is a literal on Java and rust alike.
        assert_eq!(
            one(r"SELECT parse_url('https://a.b/c?a+b=1', 'QUERY', 'a\+b')"),
            Some("1".to_string())
        );
        assert_eq!(
            one(r"SELECT parse_url('https://a.b/c?axb=1', 'QUERY', 'a\+b')"),
            None
        );
    }

    /// The 3-arg short-circuit, on its own. MEASURED-JAVAP: `evaluate(url, part, key)` compares
    /// `part` to `QUERY` and returns `null` before it touches the URL — so a non-QUERY part with
    /// a key is NULL, and an unparsable URL there does **not** raise. Delete the
    /// `key.is_some() && part != "QUERY"` guard in `extract` and the second row raises instead.
    #[test]
    fn a_key_with_a_non_query_part_is_null_and_never_parses_the_url() {
        assert_eq!(one("SELECT parse_url('https://a.b/c', 'HOST', 'k')"), None);
        assert_eq!(one("SELECT parse_url('https://a.b/c', 'PATH', 'k')"), None);
        assert_eq!(one("SELECT parse_url('not a url', 'HOST', 'k')"), None);
        // Two arguments, no key: the ordinary path is untouched.
        assert_eq!(
            one("SELECT parse_url('https://a.b/c', 'HOST')"),
            Some("a.b".to_string())
        );
    }

    /// The Java-regex vs `regex`-crate dialect **residual**, pinned in both directions.
    ///
    /// Spark compiles the key with `java.util.regex`; this kernel compiles it with the `regex`
    /// crate. Both halves are MEASURED (live `java.util.regex.Pattern` on `OpenJDK` 11.0.31 vs
    /// this kernel through both repark doors) over the raw query `a=1&b=2&aa=3&xx=4`.
    ///
    /// Pinning the *agreements* is the load-bearing half: they are what says the divergence is
    /// a narrow five-construct residual and not "the key is a different language". Pinning the
    /// raises records the residual so a future engine swap has to notice it.
    #[test]
    fn parse_url_query_key_regex_dialect_residual() {
        let url = "http://h/p?a=1&b=2&aa=3&xx=4";
        // AGREE — Java and the `regex` crate answer the same thing.
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
            // A key that opens its own group shifts Spark's `group(2)`: both engines answer
            // the key's capture, not the value.
            ("(?<n>a)", Some("a")),
            (r"\Aa", Some("1")),
        ] {
            let sql = format!("SELECT parse_url('{url}', 'QUERY', '{key}')");
            assert_eq!(one(&sql), expected.map(str::to_string), "{key}");
        }
        // DIVERGE — `java.util.regex` compiles these; the `regex` crate cannot express them, so
        // repark raises where Spark answers. The Java answer is named per row.
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
