//! Namespace describe and show handlers.
//!
//! Extracted MOVE-ONLY from `lib.rs` (r25 T0 DataFusion-style reorg). Zero behavior change.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::NamespaceIdent;
use regex::RegexBuilder;

use crate::catalog_ops::{catalog_handle, iceberg_err, name_parts, resolve_namespace};
use crate::namespace_ddl::consume_word;
use repark_core::CatalogRegistry;

/// A parsed Spark `DESCRIBE {NAMESPACE|DATABASE|SCHEMA} [EXTENDED] catalog.namespace`.
pub(crate) struct DescribeNamespace {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    /// `EXTENDED` was present — adds the `Properties` row (Z2).
    pub(crate) extended: bool,
}

/// The namespace property keys Spark's `SupportsNamespaces.RESERVED_PROPERTIES` excludes from the
/// `EXTENDED` `Properties` rendering because they already have their own row.
///
/// Live-oracle-derived (pyspark 4.0.0, a custom `DataSourceV2` catalog whose namespace metadata was
/// `{comment, location, owner, k1, k2, Amid}`): `DESCRIBE NAMESPACE EXTENDED` rendered
/// `Properties = ((Amid,vm), (k1,v1), (k2,v2))` — exactly the three reserved keys filtered, matched
/// case-sensitively.
///
/// `location_uri` is a **`RePark` divergence** (Group Z, disclosed): it is not a Spark reserved key,
/// it is `RePark`'s own U2 dual-write MIRROR of `location`
/// ([`repark_iceberg::catalog::mirror_namespace_location_keys`]). Rendering it would surface an internal
/// bookkeeping key that Spark-on-Iceberg has no equivalent of, and would duplicate the `Location`
/// row's value, so it is filtered alongside the three Spark keys.
pub(crate) const RESERVED_NAMESPACE_PROPERTIES: [&str; 4] =
    ["comment", "location", "owner", "location_uri"];

/// Spark's `REDACTION_REPLACEMENT_TEXT` default — the literal string a redacted property value is
/// replaced with (live oracle: `Properties = ((password,*********(redacted)))`).
pub(crate) const REDACTION_REPLACEMENT_TEXT: &str = "*********(redacted)";

/// ===========================================================================================
/// Recognise and parse `DESCRIBE|DESC {NAMESPACE|DATABASE|SCHEMA} [EXTENDED] catalog.namespace`.
///
/// sqlparser 0.59 has no namespace-describe statement at all, so — like
/// [`try_parse_create_namespace`] — we drive sqlparser's public `Parser` ourselves ahead of the
/// normal routing. `NAMESPACE` is not a sqlparser keyword (it tokenizes as a plain word) so it is
/// matched by value; `DATABASE` / `SCHEMA` / `EXTENDED` / `DESCRIBE` / `DESC` are keywords.
///
/// **The Z6 disambiguation rule.** `DESCRIBE` is overloaded in Spark: `namespace`, `database`, and
/// `schema` are all legal TABLE names, and Spark's grammar picks the namespace alternative only
/// when a full identifier follows the keyword. The live oracle (pyspark 4.0.0) confirms:
/// `DESCRIBE namespace` (nothing after) describes the TABLE `namespace`; `DESCRIBE namespace.tbl`
/// describes table `tbl` in database `namespace`; `DESCRIBE db1.namespace` describes the table
/// `namespace`. So this recogniser returns `None` — falling through to the normal routing and the
/// DataFusion relation-describe — whenever the keyword is NOT followed by a complete object name
/// that ends the statement. It commits (`Some`) only on the unambiguous namespace form.
///
/// `Some(Err(..))` means it IS a namespace describe but one `RePark` cannot resolve (a name that is
/// not two-part `catalog.namespace` — the same [`resolve_namespace`] contract `CREATE`/`DROP
/// NAMESPACE` enforce; Spark's single-part default-catalog and nested `a.b` forms are a disclosed
/// Group Z divergence).
/// ===========================================================================================
pub(crate) fn try_parse_describe_namespace(sql: &str) -> Option<Result<DescribeNamespace>> {
    let dialect = DatabricksDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    if !parser.parse_keyword(Keyword::DESCRIBE) && !parser.parse_keyword(Keyword::DESC) {
        return None;
    }
    let is_namespace = parser.parse_keyword(Keyword::DATABASE)
        || parser.parse_keyword(Keyword::SCHEMA)
        || consume_word(&mut parser, "NAMESPACE");
    if !is_namespace {
        return None;
    }
    let mut extended = parser.parse_keyword(Keyword::EXTENDED);
    // Spark's grammar needs a namespace name, and `EXTENDED` is only the flag when something
    // follows it: live oracle, `DESCRIBE NAMESPACE EXTENDED` (nothing after) resolves EXTENDED as
    // the NAMESPACE NAME and raises SCHEMA_NOT_FOUND. Give the token back so it parses as the name.
    if extended && matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) {
        parser.prev_token();
        extended = false;
    }
    // Z6: no parseable object name after the keyword → this is Spark's relation-describe of a table
    // literally named `namespace`/`database`/`schema`. Fall through, never error.
    let name = parser.parse_object_name(false).ok()?;
    if !matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) {
        return None;
    }
    Some(
        resolve_namespace(&name).map(|(catalog, namespace)| DescribeNamespace {
            catalog,
            namespace,
            extended,
        }),
    )
}

/// ===========================================================================================
/// `DESCRIBE {NAMESPACE|DATABASE|SCHEMA} [EXTENDED] catalog.namespace` → Spark's two-column
/// `info_name` / `info_value` metadata frame, read from the catalog's namespace properties.
///
/// The output shape is pinned to a LIVE pyspark 4.0.0 oracle run against a **`DataSourceV2`**
/// catalog (the class `RePark`'s Iceberg catalogs are), not the v1 session catalog, because the two
/// differ: the v1 `DescribeDatabaseCommand` always emits `Comment` / `Location` / `Owner` (empty
/// string when unset) while the v2 `DescribeNamespaceExec` emits each of those rows **only when the
/// namespace metadata carries the key**. `RePark` follows v2:
///
/// | row | emitted when |
/// |---|---|
/// | `Catalog Name` | always — the registered catalog name, emitted RAW (Spark uses `catalog.name()`) |
/// | `Namespace Name` | always — via [`quote_namespace_name_if_needed`], Spark's `NamespaceHelper.quoted` |
/// | `Comment` | the `comment` property is present |
/// | `Location` | [`repark_iceberg::catalog::resolve_namespace_location`] resolves (`location`, else the U2 `location_uri` mirror) |
/// | `Owner` | the `owner` property is present |
/// | `Properties` | `EXTENDED` only — always emitted then, `""` when there is nothing to show |
///
/// **Missing namespace** raises the oracle's class: the existence check fails with a
/// [`DataFusionError::Plan`], which `repark_session::engine_err` classifies `Analysis` →
/// `repark.errors.AnalysisException`, matching live pyspark's `AnalysisException` /
/// `SCHEMA_NOT_FOUND` (SQLSTATE 42704). The check is explicit rather than relying on the catalog
/// returning `ErrorKind::NamespaceNotFound`, so the class holds for every catalog implementation.
///
/// **Disclosed divergences (Group Z)** — the whole list, none silently dropped:
/// 1. **`Owner` is emitted only when the catalog stores an `owner` property.** `RePark` never
///    writes one, so in practice the row is absent. This is not an invented value: it is exactly
///    what the v2 oracle does for a namespace whose metadata lacks `owner`.
/// 2. **Single-part (`DESCRIBE NAMESPACE ns`) and nested (`cat.a.b`) names fail loud.** Spark
///    resolves the first against the current catalog and supports the second; `RePark`'s namespace
///    surface is two-part `catalog.namespace` throughout (`CREATE`/`DROP NAMESPACE` alike).
/// 3. **`location_uri` is filtered from `Properties`** — see [`RESERVED_NAMESPACE_PROPERTIES`].
/// 4. **The `SCHEMA_NOT_FOUND` text is carried inside DataFusion's "Error during planning: "
///    prefix**, so `str(exc)` is prefixed where Spark's is not. The exception CLASS matches.
/// 5. **Redaction is hard-wired to the DEFAULTS of both patterns** (`spark.redaction.regex` and
///    `spark.sql.redaction.options.regex`) — `RePark` has no config surface for either, so a caller
///    who re-tuned them in Spark sees a different redaction set here. The underscore/dash
///    `access_key` spellings are shown by BOTH engines — see [`property_is_redacted`].
/// 6. **`Location` falls back to the U2 `location_uri` mirror**
///    ([`repark_iceberg::catalog::resolve_namespace_location`]); Spark emits `Location` only from a
///    `location` metadata key. Latent today — `RePark`'s own `CREATE NAMESPACE … LOCATION` always
///    writes `location`, so the fallback is reachable only for a namespace created outside `RePark`
///    (a pre-existing Glue database), where surfacing the real path beats an absent row.
/// 7. **A lone trailing `EXTENDED`** (`DESCRIBE NAMESPACE EXTENDED`) binds as the namespace NAME in
///    both engines, but the messages differ: Spark raises `SCHEMA_NOT_FOUND` for a namespace called
///    `EXTENDED`, `RePark` raises its two-part-name error (divergence 2). Both are
///    `AnalysisException`.
///
/// ===========================================================================================
///
/// # Errors
/// Returns a plan error when the catalog is not registered or the namespace does not exist, and
/// propagates catalog-load failures.
pub(crate) async fn execute_describe_namespace(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    describe: DescribeNamespace,
) -> Result<DataFrame> {
    let handle = catalog_handle(catalogs, &describe.catalog)?;
    let ident = NamespaceIdent::new(describe.namespace.clone());
    if !handle.namespace_exists(&ident).await.map_err(iceberg_err)? {
        return Err(DataFusionError::Plan(format!(
            "[SCHEMA_NOT_FOUND] The schema `{}` cannot be found. Verify the spelling and \
             correctness of the schema and catalog.",
            describe.namespace
        )));
    }
    let namespace = handle.get_namespace(&ident).await.map_err(iceberg_err)?;
    ctx.read_batch(describe_namespace_batch(&describe, namespace.properties())?)
}

/// Build the `info_name` / `info_value` batch for one namespace (row set per
/// [`execute_describe_namespace`]).
///
/// The schema is the live oracle's verbatim: `info_name` **non-nullable** `Utf8`, `info_value`
/// nullable `Utf8`, each carrying Spark's field-level `comment` metadata.
pub(crate) fn describe_namespace_batch(
    describe: &DescribeNamespace,
    properties: &HashMap<String, String>,
) -> Result<RecordBatch> {
    let mut rows = vec![
        ("Catalog Name", describe.catalog.clone()),
        (
            "Namespace Name",
            quote_namespace_name_if_needed(&describe.namespace),
        ),
    ];
    if let Some(comment) = properties.get("comment") {
        rows.push(("Comment", comment.clone()));
    }
    if let Some(location) = repark_iceberg::catalog::resolve_namespace_location(properties) {
        rows.push(("Location", location.to_string()));
    }
    if let Some(owner) = properties.get("owner") {
        rows.push(("Owner", owner.clone()));
    }
    if describe.extended {
        rows.push(("Properties", render_namespace_properties(properties)));
    }

    let names: Vec<&str> = rows.iter().map(|(name, _)| *name).collect();
    let values: Vec<String> = rows.into_iter().map(|(_, value)| value).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("info_name", DataType::Utf8, false).with_metadata(HashMap::from([(
            "comment".to_string(),
            "name of the namespace info".to_string(),
        )])),
        Field::new("info_value", DataType::Utf8, true).with_metadata(HashMap::from([(
            "comment".to_string(),
            "value of the namespace info".to_string(),
        )])),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(values)),
        ],
    )?)
}

/// Render the `EXTENDED` `Properties` value in Spark's exact format.
///
/// Live oracle (pyspark 4.0.0, v2 catalog): the non-reserved keys, **sorted by key** (plain
/// byte/lexicographic order — `Amid` sorts before `k1`), each rendered `(key,value)` with no
/// escaping or quoting whatsoever, joined by `", "`, wrapped in one more pair of parentheses:
/// `((Amid,vm), (k1,v1), (k2,v2))`. Values containing commas, spaces, or parentheses pass through
/// raw (`{"a b": "c,d", "z": "(paren)", "empty": ""}` → `((a b,c,d), (empty,), (z,(paren)))`), and
/// an empty set renders as the EMPTY STRING, not `()`.
pub(crate) fn render_namespace_properties(properties: &HashMap<String, String>) -> String {
    let mut pairs: Vec<(&String, &String)> = properties
        .iter()
        .filter(|(key, _)| !RESERVED_NAMESPACE_PROPERTIES.contains(&key.as_str()))
        .collect();
    if pairs.is_empty() {
        return String::new();
    }
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    let rendered: Vec<String> = pairs
        .iter()
        .map(|(key, value)| {
            let shown = if property_is_redacted(key, value) {
                REDACTION_REPLACEMENT_TEXT
            } else {
                value.as_str()
            };
            format!("({key},{shown})")
        })
        .collect();
    format!("({})", rendered.join(", "))
}

/// Whether a namespace property's VALUE must be redacted in `DESCRIBE … EXTENDED` output.
///
/// The path in Spark is `DescribeNamespaceExec` → `SQLConf.redactOptions` → `Utils.redact`, and it
/// matters that it is **key OR value**: `Utils.redact` tests
/// `regex.findFirstIn(key).orElse(regex.findFirstIn(value))` and replaces the VALUE on either hit.
/// `redactOptions` folds **TWO** patterns over the pairs, so both apply here:
/// - `spark.redaction.regex`, default `(?i)secret|password|token|access[.]?key` — note `[.]?`, an
///   optional literal DOT only, NOT an underscore or a dash;
/// - `spark.sql.redaction.options.regex`, default `(?i)url`.
///
/// Each alternative is an unanchored substring match, case-insensitive over ASCII (Java's `(?i)`
/// without `UNICODE_CASE`), which is what `to_ascii_lowercase` + `contains` reproduces without a
/// regex dependency.
///
/// Live truth table (pyspark 4.0.0, v2 catalog, 2026-07-25) — every row reproduced by
/// `describe_namespace_extended_redaction_truth_table`:
/// `password`/`SeCrEt`/`my_token_2`/`accesskey`/`access.key` redact on the key;
/// `jdbc_url`/`urlish`/`valueurl` redact on the `url` key pattern; `innocent` =
/// `"my password is hunter2"` and `bare` = `"http://x/URL"` redact on the VALUE; while
/// `plain`, `access_key`, `ACCESS-KEY` and `dashaccess-key` are all SHOWN.
///
/// That last group is a **named, inherited Spark gap, not a `RePark` choice**: Spark's own default
/// pattern spells the separator `[.]?`, so the underscore and dash spellings — including
/// `AWS_ACCESS_KEY_ID` — are NOT redacted by Spark either. `RePark` matches the oracle exactly rather
/// than over-redacting, so the parity claim stays true; a caller who wants those covered must widen
/// the pattern in Spark too. (This predicate is therefore deliberately NARROWER than
/// `repark_session::catalog_config`'s redaction set, which guards a different surface — logged
/// catalog config — and answers to no oracle.)
pub(crate) fn property_is_redacted(key: &str, value: &str) -> bool {
    redaction_pattern_matches(key) || redaction_pattern_matches(value)
}

/// One side of [`property_is_redacted`]: does `text` match either default redaction pattern?
pub(crate) fn redaction_pattern_matches(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    [
        // `spark.redaction.regex` — `(?i)secret|password|token|access[.]?key`.
        "secret",
        "password",
        "token",
        "accesskey",
        "access.key",
        // `spark.sql.redaction.options.regex` — `(?i)url`.
        "url",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

/// Render one namespace-name part the way Spark's `NamespaceHelper.quoted` does.
///
/// Spark's `quoteIfNeeded` leaves a part bare only when it matches `[a-zA-Z0-9_]+` **and** is not
/// all digits; otherwise it wraps it in backticks, doubling any interior backtick. Live oracle
/// (pyspark 4.0.0, v2 catalog): `Mixed_Case9` → `Mixed_Case9`, `my ns` → `` `my ns` ``,
/// `weird.name` → `` `weird.name` ``, `dash-name` → `` `dash-name` ``, `123` → `` `123` ``,
/// ``has`tick`` → ``` `has``tick` ```.
///
/// Applies to the `Namespace Name` row only — Spark emits `Catalog Name` from `catalog.name()` raw.
pub(crate) fn quote_namespace_name_if_needed(part: &str) -> String {
    let bare = !part.is_empty()
        && part
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        && !part.bytes().all(|byte| byte.is_ascii_digit());
    if bare {
        return part.to_string();
    }
    format!("`{}`", part.replace('`', "``"))
}

/// A parsed Spark `SHOW {NAMESPACES|SCHEMAS|DATABASES} [{IN|FROM} catalog] [LIKE] ['pattern']`.
pub(crate) struct ShowNamespaces {
    /// The catalog named by `IN`/`FROM`. `RePark` has no current-catalog concept, so the clause is
    /// mandatory here (a disclosed Group AB divergence) — resolution happens in the parser.
    catalog: String,
    /// The `LIKE` pattern, unevaluated (Spark's `filterPattern` grammar — see
    /// [`filter_pattern_matches`]). `None` = show every namespace.
    pattern: Option<String>,
}

/// ===========================================================================================
/// Recognise and parse `SHOW {NAMESPACES|SCHEMAS|DATABASES} [{IN|FROM} catalog] [LIKE] ['pattern']`.
///
/// sqlparser 0.59 parses `SHOW NAMESPACES …` as an opaque `Statement::ShowVariable` (DataFusion
/// then refuses it with "SHOW [VARIABLE] is not supported unless `information_schema` is
/// enabled")
/// and `SHOW SCHEMAS` / `SHOW DATABASES` as `Statement::ShowSchemas` / `ShowDatabases`, which
/// DataFusion refuses with "Unsupported SQL statement". None of the three works today, so this
/// intercept shadows no working behaviour — measured, not assumed (AB6).
///
/// **Disambiguation (the Z6 question, answered differently).** `DESCRIBE` needed a fall-through
/// because `namespace`/`database`/`schema` are legal TABLE names and Spark routes
/// `DESCRIBE namespace` to the table describe. `SHOW` has no such overload: Spark's grammar has no
/// `SHOW <relation>` form at all, so `SHOW NAMESPACES|SCHEMAS|DATABASES` can only ever be this
/// statement, and a table named `namespaces` is reached through `SELECT`/`DESCRIBE`, never `SHOW`.
/// The head is therefore unambiguous and this recogniser COMMITS on it: a malformed tail becomes
/// `Some(Err(..))` naming the supported form, matching Spark (which raises `ParseException` /
/// `PARSE_SYNTAX_ERROR` for `SHOW NAMESPACES IN cat GARBAGE`) rather than falling through to
/// DataFusion's opaque `ShowVariable` refusal.
///
/// `NAMESPACES` is not a sqlparser keyword (it tokenizes as a plain word) so it is matched by
/// value; `SCHEMAS` / `DATABASES` / `IN` / `FROM` / `LIKE` are keywords. `FROM` is Spark's
/// documented synonym for `IN` (oracle-confirmed identical), and the `LIKE` keyword itself is
/// optional — the live oracle accepts a bare pattern literal (`SHOW NAMESPACES IN cat 'al*'`).
/// ===========================================================================================
pub(crate) fn try_parse_show_namespaces(sql: &str) -> Option<Result<ShowNamespaces>> {
    let dialect = DatabricksDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    let mut parser = Parser::new(&dialect).with_tokens(tokens);
    if !parser.parse_keyword(Keyword::SHOW) {
        return None;
    }
    let is_namespaces = parser.parse_keyword(Keyword::SCHEMAS)
        || parser.parse_keyword(Keyword::DATABASES)
        || consume_word(&mut parser, "NAMESPACES");
    if !is_namespaces {
        return None;
    }
    Some(parse_show_namespaces_tail(&mut parser))
}

/// The committed half of [`try_parse_show_namespaces`]: everything after the statement head.
pub(crate) fn parse_show_namespaces_tail(parser: &mut Parser) -> Result<ShowNamespaces> {
    let scope = if parser.parse_keyword(Keyword::IN) || parser.parse_keyword(Keyword::FROM) {
        Some(
            parser
                .parse_object_name(false)
                .map_err(show_namespaces_err)?,
        )
    } else {
        None
    };
    let had_like = parser.parse_keyword(Keyword::LIKE);
    let pattern = match &parser.peek_token().token {
        Token::SingleQuotedString(text) | Token::DoubleQuotedString(text) => {
            let text = text.clone();
            parser.next_token();
            Some(text)
        }
        _ => None,
    };
    if had_like && pattern.is_none() {
        return Err(DataFusionError::Plan(
            "SHOW NAMESPACES … LIKE needs a quoted pattern (e.g. SHOW NAMESPACES IN cat LIKE \
             'sales*')"
                .to_string(),
        ));
    }
    if !matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) {
        return Err(DataFusionError::Plan(format!(
            "could not parse `SHOW NAMESPACES` at `{}` — the supported form is \
             SHOW {{NAMESPACES|SCHEMAS|DATABASES}} {{IN|FROM}} <catalog> [LIKE] ['pattern']",
            parser.peek_token()
        )));
    }
    // Spark resolves a missing `IN` against the CURRENT catalog; `RePark` has none, so the clause
    // is required and its absence names the requirement instead of guessing a catalog (AB6).
    let Some(name) = scope else {
        return Err(DataFusionError::Plan(
            "SHOW NAMESPACES requires an explicit catalog — `SHOW NAMESPACES IN <catalog>` \
             (RePark has no current-catalog concept, so there is no default to resolve against)"
                .to_string(),
        ));
    };
    let parts = name_parts(&name);
    let [catalog] = parts.as_slice() else {
        return Err(DataFusionError::Plan(format!(
            "expected a one-part `IN <catalog>` name, got `{name}` — RePark namespaces are \
             single-level, so there are no nested namespaces to list under `{name}`"
        )));
    };
    Ok(ShowNamespaces {
        catalog: catalog.clone(),
        pattern,
    })
}

/// Fold a sqlparser error from the `SHOW NAMESPACES` tail into a plan-class [`DataFusionError`]
/// (classified `AnalysisException` by WG-3, the family Spark's `ParseException` also belongs to).
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn show_namespaces_err(err: ParserError) -> DataFusionError {
    DataFusionError::Plan(format!("could not parse SHOW NAMESPACES: {err}"))
}

/// ===========================================================================================
/// `SHOW {NAMESPACES|SCHEMAS|DATABASES} {IN|FROM} catalog [LIKE 'pattern']` → Spark's one-column
/// `namespace` frame, read from the catalog's real `Catalog::list_namespaces`.
///
/// Pinned to a LIVE pyspark 4.0.0 oracle run against a **`DataSourceV2`** catalog (the class
/// `RePark`'s Iceberg catalogs are — the Group Z rule), reproducing `ShowNamespacesExec`:
///
/// - **Schema:** one field, `namespace`, `Utf8`, **non-nullable**, with NO field metadata
///   (verbatim `{"name":"namespace","nullable":false,"type":"string","metadata":{}}`).
/// - **Row value:** Spark maps each namespace through `.quoted` — every PART rendered by
///   `quoteIfNeeded` ([`quote_namespace_name_if_needed`]) and joined with `.`. So a namespace
///   `my ns` shows as `` `my ns` `` and `123` as `` `123` ``.
/// - **Row order:** the catalog's own order, **unsorted** — Spark applies none (oracle: a catalog
///   returning `zeta, alpha, beta` shows exactly `zeta, alpha, beta`). `RePark` likewise passes
///   `Catalog::list_namespaces` order through untouched rather than inventing a sort.
/// - **`LIKE`:** Spark's `StringUtils.filterPattern`, applied to the RENDERED (quoted) row string,
///   not the raw name — see [`filter_pattern_matches`].
/// - **Synonyms:** `SHOW SCHEMAS` / `SHOW DATABASES` are byte-identical to `SHOW NAMESPACES`, and
///   `FROM` is identical to `IN` (all oracle-confirmed).
///
/// **Disclosed divergences from the live oracle (Group AB)** — the whole list, none dropped:
/// 1. **`IN`/`FROM` is MANDATORY.** Spark resolves a bare `SHOW NAMESPACES` against the current
///    catalog (`spark_catalog` by default, or whatever `USE cat` last set) and — oracle-confirmed —
///    ignores the current *namespace* entirely, always listing from the catalog ROOT. `RePark` has
///    no current-catalog concept anywhere, so the clause is required and its absence fails loud
///    naming the requirement rather than guessing. Same class as Group Z's divergence 2.
/// 2. **Only a ONE-part `IN <catalog>` is accepted; nested `IN cat.ns` fails loud.** Spark lists
///    the CHILDREN of `cat.ns` (oracle: `IN abcat.alpha` → `alpha.child1`, `alpha.child2`).
///    `RePark`'s namespace surface is single-level `catalog.namespace` throughout, so a nested
///    listing would always be empty; failing loud beats a silently-empty result that reads as
///    "no children exist". Closing it needs nested `NamespaceIdent` support across the whole
///    namespace surface — backlog, the same one Group Z named.
/// 3. **Unknown catalog:** Spark falls back to reading the name as a NAMESPACE of the current
///    catalog and raises `AnalysisException` / `SCHEMA_NOT_FOUND` (42704) for
///    `` `spark_catalog`.`nosuchcatalog` ``. `RePark` has no fallback catalog, so it raises the
///    registry's own "unknown catalog" error. The exception CLASS matches (`AnalysisException`,
///    via [`catalog_handle`]'s `DataFusionError::Plan` → WG-3 classification); the message and the
///    condition name do not.
/// 4. **The `LIKE` regex engine is Rust's `regex`, not Java's `java.util.regex`.** Every pattern in
///    the live truth table behaves identically, but the engines are not the same language: a
///    pattern using backreferences or lookaround compiles in Java and fails to compile in `regex`,
///    where `RePark` (like Spark on a `PatternSyntaxException`) silently drops that alternative;
///    case-insensitivity is Unicode-aware here versus Java's ASCII-only `(?i)`; and the
///    whole-pattern trim is `str::trim` (Unicode whitespace) versus Java `String.trim()`
///    (`<= U+0020` only). Named, not hidden — see [`filter_pattern_matches`].
/// 5. **A malformed tail is `AnalysisException`, not `ParseException`.** Spark raises
///    `ParseException` / `PARSE_SYNTAX_ERROR` (42601) for `SHOW NAMESPACES IN cat GARBAGE`.
///    `RePark`'s router reports it as a plan error; `repark.errors.ParseException` subclasses
///    `AnalysisException` (Group S), so `except AnalysisException` catches both — but the exact
///    leaf class differs. Same shape as the pre-existing create-namespace parse errors.
/// 6. **The condition name / SQLSTATE are not structured.** `RePark` has no `getCondition()` /
///    `getSqlState()` surface (the same backlog gap Groups S / X / Z disclosed).
///
/// ===========================================================================================
///
/// # Errors
/// Returns a plan error when the catalog is not registered, and propagates catalog-listing failures.
pub(crate) async fn execute_show_namespaces(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    show: ShowNamespaces,
) -> Result<DataFrame> {
    let handle = catalog_handle(catalogs, &show.catalog)?;
    let namespaces = handle.list_namespaces(None).await.map_err(iceberg_err)?;
    let rows = show_namespace_rows(&namespaces, show.pattern.as_deref());
    ctx.read_batch(show_namespaces_batch(rows)?)
}

/// Spark's `ShowNamespacesExec.run` body: render every namespace, then keep the ones the `LIKE`
/// pattern matches. Split out from [`execute_show_namespaces`] so the two oracle claims it carries
/// are testable without a catalog.
///
/// Two things are load-bearing and both are live-oracle-derived:
/// 1. **Render, THEN filter.** Spark maps `.quoted` before `filterPattern`, so the pattern sees the
///    QUOTED string — `LIKE 'dash-name'` matches nothing while `` LIKE '`dash-name`' `` matches.
/// 2. **No sorting.** Spark iterates the catalog's own order and emits matches in it (oracle: a
///    catalog returning `zeta, alpha, beta` shows `zeta, alpha, beta`, and `LIKE 'alpha|zeta'`
///    shows `zeta, alpha` — alternation order does NOT reorder the output). One namespace can match
///    several alternatives yet is emitted once, because the decision is per-namespace, not
///    per-alternative.
pub(crate) fn show_namespace_rows(
    namespaces: &[NamespaceIdent],
    pattern: Option<&str>,
) -> Vec<String> {
    namespaces
        .iter()
        .map(quoted_namespace)
        .filter(|rendered| pattern.is_none_or(|pattern| filter_pattern_matches(rendered, pattern)))
        .collect()
}

/// Build the one-column `namespace` batch (schema per [`execute_show_namespaces`]).
///
/// The schema is the live oracle's verbatim: a single `namespace` field, `Utf8`, **non-nullable**,
/// carrying no field metadata (unlike `DESCRIBE NAMESPACE`'s two fields, which do).
pub(crate) fn show_namespaces_batch(rows: Vec<String>) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "namespace",
        DataType::Utf8,
        false,
    )]));
    Ok(RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(rows))],
    )?)
}

/// Render a namespace the way Spark's `NamespaceHelper.quoted` does: every part through
/// `quoteIfNeeded` ([`quote_namespace_name_if_needed`]), joined with `.`.
///
/// Live oracle (pyspark 4.0.0, v2 catalog): a flat catalog shows `zeta` / `Mixed_Case9` /
/// `` `my ns` `` / `` `123` `` / `` `dash-name` `` / `` `weird.name` ``, and a nested listing shows
/// the FULL path from the root — `alpha.child1`, `alpha.child1.grand` — never the leaf alone.
pub(crate) fn quoted_namespace(namespace: &NamespaceIdent) -> String {
    namespace
        .iter()
        .map(|part| quote_namespace_name_if_needed(part))
        .collect::<Vec<String>>()
        .join(".")
}

/// Spark's `StringUtils.filterPattern`, the `SHOW … LIKE` matcher — NOT SQL `LIKE`.
///
/// Scala source (Spark 4.0.0, `o.a.s.sql.catalyst.util.StringUtils`), which the live oracle
/// confirms row for row:
/// ```text
/// pattern.trim().split("\\|").foreach { subPattern =>
///   try {
///     val regex = ("(?i)" + subPattern.replaceAll("\\*", ".*")).r
///     funcNames ++= names.filter { name => regex.matches(name) }
///   } catch { case _: PatternSyntaxException => }   // a bad alternative is SILENTLY DROPPED
/// }
/// ```
/// So, precisely: the WHOLE pattern is trimmed once (not each alternative — oracle:
/// `'alpha| beta'` matches only `alpha`, because ` beta` keeps its leading space), split on a
/// literal `|`, each alternative has every `*` replaced by `.*` and is then compiled as a
/// **case-insensitive Java regex** and **FULL-matched** (`Regex.matches`, not `find`) against the
/// name. It is a regex, not a glob: `?` is a quantifier, `.` matches any character, `%` and `_`
/// are literals, and a syntactically invalid alternative matches nothing instead of raising.
///
/// Live truth table (pyspark 4.0.0, v2 catalog, 2026-07-25) — every row reproduced by
/// `show_namespaces_like_truth_table`, matched against the RENDERED rows
/// `[zeta, alpha, beta, Mixed_Case9, `my ns`, `123`, `dash-name`, `weird.name`]`:
/// `alpha`/`ALPHA`/`AlPhA` → `alpha` (case-insensitive); `lph` → NOTHING (full match, not
/// substring); `*lph*` → `alpha`; `al*` → `alpha`; `*ta` → `zeta, beta`; `*et*` → `zeta, beta`;
/// `a?pha` → NOTHING (`?` is a quantifier); `al%` and `bet_` → NOTHING (no SQL-`LIKE` wildcards);
/// `dash-name` → NOTHING but `` `dash-name` `` and `*dash-name*` → the row (the pattern sees the
/// QUOTED string); `weird.name` → NOTHING (the row has backticks); `.*` → EVERYTHING (`.` is a
/// metachar); `[` → NOTHING (swallowed `PatternSyntaxException`) and `alpha|[` → `alpha` (only the
/// bad alternative is dropped); `  alpha  ` → `alpha` (trim); `alpha|zeta` → `zeta, alpha` in
/// CATALOG order; `al*|alpha` → `alpha` ONCE (no duplicate); `''` → NOTHING; `*` → EVERYTHING.
///
/// Reproduced with Rust's `regex` rather than transliterated by hand — the 2026-07-25 Group Z
/// DO-NOT. `\A`/`\z` (not `^`/`$`) give Java's `matches()` whole-input semantics exactly.
/// Divergence 4 of [`execute_show_namespaces`] names the two engines' remaining differences.
pub(crate) fn filter_pattern_matches(name: &str, pattern: &str) -> bool {
    pattern.trim().split('|').any(|alternative| {
        // NO wrapping group around the alternative: `\A(?:{alt})\z` would REBALANCE
        // shifted-but-balanced parens Java rejects — `alpha)(` becomes the valid
        // `\A(?:alpha)()\z` and MATCHES, where Spark's `Pattern.compile("alpha)(")` throws
        // `PatternSyntaxException` and the alternative is silently dropped (C-AB-S2; the
        // 64-pattern oracle diff is clean with the bare form, wrapper-artifact-free).
        RegexBuilder::new(&format!(r"\A{}\z", alternative.replace('*', ".*")))
            .case_insensitive(true)
            .build()
            .is_ok_and(|regex| regex.is_match(name))
    })
}
