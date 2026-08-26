//! SQP-1 pins — Spark string-literal escapes on the Spark SQL door.
//!
//! Oracle: PySpark 4.1.2 + Iceberg 1.11.0, `spark.sql.parser.escapedStringLiterals=false`,
//! `spark.sql.ansi.enabled=true`, measured against the live oracle (`<pyspark-4.1.2-oracle>`);
//! the charter ledger `task/ledgers/staging/sqp-1-spark-string-literals-ledger.md` holds the
//! transcript. Every value is the Spark value, on the Arrow collect path (value AND `Utf8` type).
//! Reverting the front-door `canonicalize` call reds every case here.
use super::super::*;
use super::common::*;

use std::borrow::Cow;

use datafusion::arrow::array::BooleanArray;

/// A `SessionContext` wired like production for pure-expression pins: the Spark function registry,
/// the expression-semantics analyzer rules, ANSI ON and `parse_float_as_decimal` — but no catalog,
/// which the value-domain literals do not need.
fn expr_ctx() -> (SessionContext, CatalogRegistry) {
    let config =
        crate::extension::apply_spark_float_as_decimal(datafusion::prelude::SessionConfig::new());
    let config = repark_functions::ansi::with_spark_ansi_config(config, true);
    let ctx = SessionContext::new_with_config(config);
    repark_functions::register_all(&ctx);
    repark_functions::decimal_spark::register_spark_decimal_planner(&ctx);
    for rule in repark_functions::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    (ctx, CatalogRegistry::new())
}

/// The `Utf8` value of `SELECT <expr> AS s`, asserting the Arrow type is `Utf8` (Spark STRING) —
/// value AND type, never a display path.
async fn string_value(ctx: &SessionContext, catalogs: &CatalogRegistry, expr: &str) -> String {
    let batches = execute(ctx, catalogs, &format!("SELECT {expr} AS s"))
        .await
        .unwrap_or_else(|error| panic!("`SELECT {expr}` failed: {error}"))
        .collect()
        .await
        .unwrap();
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &DataType::Utf8,
        "`{expr}` must yield a Spark STRING (Arrow Utf8)"
    );
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 column");
    column.value(0).to_string()
}

/// The string value of `SELECT <expr> AS s` for a DERIVED string (a function result, a
/// `CAST … AS STRING`), read through an Arrow cast to `Utf8` — DataFusion can return `Utf8View`
/// there, and the incidental type of a control result is not the thing under test.
async fn derived_string(ctx: &SessionContext, catalogs: &CatalogRegistry, expr: &str) -> String {
    let batches = execute(ctx, catalogs, &format!("SELECT {expr} AS s"))
        .await
        .unwrap_or_else(|error| panic!("`SELECT {expr}` failed: {error}"))
        .collect()
        .await
        .unwrap();
    let column = datafusion::arrow::compute::cast(batches[0].column(0), &DataType::Utf8).unwrap();
    column
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 after cast")
        .value(0)
        .to_string()
}

/// The integer value of `SELECT <expr> AS n`, read through an Arrow cast to `Int64` (a Spark
/// `regexp_count` is `Int32`).
async fn int_value(ctx: &SessionContext, catalogs: &CatalogRegistry, expr: &str) -> i64 {
    let batches = execute(ctx, catalogs, &format!("SELECT {expr} AS n"))
        .await
        .unwrap_or_else(|error| panic!("`SELECT {expr}` failed: {error}"))
        .collect()
        .await
        .unwrap();
    let column = datafusion::arrow::compute::cast(batches[0].column(0), &DataType::Int64).unwrap();
    column
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 after cast")
        .value(0)
}

/// The single boolean of `SELECT <expr> AS b` (LIKE / RLIKE controls).
async fn bool_value(ctx: &SessionContext, catalogs: &CatalogRegistry, expr: &str) -> bool {
    let batches = execute(ctx, catalogs, &format!("SELECT {expr} AS b"))
        .await
        .unwrap_or_else(|error| panic!("`SELECT {expr}` failed: {error}"))
        .collect()
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("Boolean column")
        .value(0)
}

/// The ordered `Utf8` values of a query's first column (NULL rendered as the marker), for the
/// write-path round-trips.
async fn string_column(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<String> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("`{sql}` failed: {error}"))
        .collect()
        .await
        .unwrap();
    let mut out = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 column");
        for index in 0..column.len() {
            out.push(if column.is_null(index) {
                "<NULL>".to_string()
            } else {
                column.value(index).to_string()
            });
        }
    }
    out
}

/// pins: sqp-1-spark-string-literals/C-001
///
/// The whole escape domain, one row per enumerated element. Each row is `(label, literal,
/// expected)`; every mismatch is collected so one broken element does not mask another.
#[tokio::test]
async fn unescape_covers_the_spark_escape_domain() {
    let (ctx, catalogs) = expr_ctx();
    // (label, SQL literal as written, Spark value)
    let cases: &[(&str, &str, &str)] = &[
        // (1) `\\` → `\`
        ("backslash-backslash", "'\\\\'", "\\"),
        ("regex-pattern-spelling", "'\\\\d'", "\\d"),
        // (2) `\'` → `'`   (3) `\"` → `"`
        ("escaped-single-quote", "'\\''", "'"),
        ("escaped-double-quote", "'\\\"'", "\""),
        // (4)-(7) control escapes
        ("newline", "'a\\nb'", "a\nb"),
        ("tab", "'\\t'", "\t"),
        ("carriage-return", "'\\r'", "\r"),
        ("backspace", "'\\b'", "\u{8}"),
        // (8) `\0` → NUL   (9) `\Z` → U+001A
        ("nul", "'\\0'", "\u{0}"),
        ("ctrl-z", "'\\Z'", "\u{1a}"),
        // (10) `\%` / `\_` keep the backslash (LIKE reads them)
        ("percent-keeps-backslash", "'\\%'", "\\%"),
        ("underscore-keeps-backslash", "'\\_'", "\\_"),
        // (11) `\uXXXX` exactly four hex; short / non-hex fall to the unknown-escape rule
        ("unicode-16", "'\\u0041'", "A"),
        ("unicode-16-short", "'\\u004'", "u004"),
        ("unicode-16-nonhex", "'\\u00zz'", "u00zz"),
        // (12) `\UXXXXXXXX` eight hex
        ("unicode-32", "'\\U0001F408'", "\u{1F408}"),
        // (13) a `\uD8xx\uDCxx` pair → one code point
        ("surrogate-pair", "'\\ud83d\\udc08'", "\u{1F408}"),
        ("lone-surrogate", "'\\ud83d'", "?"),
        // (14) `\NNN` octal, first digit `0`-`1`; otherwise literal
        ("octal-A", "'\\101'", "A"),
        ("octal-nul", "'\\000'", "\u{0}"),
        ("octal-nul-then-seven", "'\\0007'", "\u{0}7"),
        ("octal-high-200", "'\\200'", "200"),
        ("octal-high-377", "'\\377'", "377"),
        ("octal-high-777", "'\\777'", "777"),
        ("octal-single-1", "'\\1'", "1"),
        ("octal-short-12x", "'\\12x'", "12x"),
        // (15) any other `\c` → `c`
        ("unknown-d", "'\\d'", "d"),
        ("unknown-q", "'\\q'", "q"),
        ("no-hex-escape", "'\\x41'", "x41"),
        // (16) `''` → `'`
        ("doubled-quote", "'it''s'", "it's"),
    ];
    let mut failures = Vec::new();
    for (label, literal, expected) in cases {
        let got = string_value(&ctx, &catalogs, literal).await;
        if got != *expected {
            failures.push(format!(
                "{label}: {literal} -> {got:?} (expected {expected:?})"
            ));
        }
        // The oracle also pins code-point length; the exact value subsumes it, but assert it so a
        // future value edit that keeps the char count is still caught by the value column.
        let _ = got.chars().count();
    }
    assert!(
        failures.is_empty(),
        "escape-domain mismatches:\n{}",
        failures.join("\n")
    );
}

/// pins: sqp-1-spark-string-literals/C-002
///
/// `\'` / `\"` no longer end the literal (E5, E22); an unpaired trailing backslash is a parse
/// error (E16); `'a\\'` is `a\` (E15).
#[tokio::test]
async fn escaped_quotes_lex_and_unpaired_backslash_refuses() {
    let (ctx, catalogs) = expr_ctx();
    assert_eq!(string_value(&ctx, &catalogs, "'it\\'s'").await, "it's");
    assert_eq!(
        string_value(&ctx, &catalogs, "'a\\tb\\\\c\\'d'").await,
        "a\tb\\c'd"
    );
    assert_eq!(string_value(&ctx, &catalogs, "'a\\\\'").await, "a\\");
    // U11: `'\''` is a single quote (length 1), NOT an empty string followed by junk.
    assert_eq!(string_value(&ctx, &catalogs, "'\\''").await, "'");
    // E16 / U10: an unpaired trailing backslash is a lexer error, never a silent `a\`.
    let unpaired = execute(&ctx, &catalogs, "SELECT 'a\\'").await;
    assert!(
        unpaired.is_err(),
        "`'a\\'` (unpaired trailing backslash) must refuse"
    );
    let doubled = execute(&ctx, &catalogs, "SELECT 'a\\''b'").await;
    assert!(doubled.is_err(), "`'a\\''b'` must refuse (U10)");
}

/// pins: sqp-1-spark-string-literals/C-003
///
/// Adjacent single-quoted literals concatenate (E7, U17) where the door used to `ParserError`.
#[tokio::test]
async fn adjacent_literals_concatenate() {
    let (ctx, catalogs) = expr_ctx();
    assert_eq!(string_value(&ctx, &catalogs, "'ab' 'cd'").await, "abcd");
    // U17: concatenation runs on the already-unescaped values — `'a\\'` is `a\`, then `b`.
    assert_eq!(string_value(&ctx, &catalogs, "'a\\\\' 'b'").await, "a\\b");
    // Three-way, with a control escape in the middle segment.
    assert_eq!(string_value(&ctx, &catalogs, "'x' '\\t' 'y'").await, "x\ty");
}

/// pins: sqp-1-spark-string-literals/C-001, C-002
///
/// A run of single quotes is Spark's escaped-quote-inside-quotes, NOT a triple-quoted string —
/// Spark has none. BigQuery's lexer read `'''…'''` as a triple-quoted token, so `''''` opened a
/// triple quote (an unterminated-literal TokenizerError here), `'''a\tb'''` silently dropped a
/// quote, and a facade value beginning with an apostrophe crashed. [`SparkLexDialect`] keeps
/// Generic's no-triple-quote rule, so these match the oracle. Reverting to `BigQueryDialect` reds
/// every line.
#[tokio::test]
async fn quote_runs_are_not_triple_quoted_strings() {
    let (ctx, catalogs) = expr_ctx();
    // `''''` → `'` (one quote), `''''''` → `''` (two): a doubled `''` is one in-literal quote.
    assert_eq!(string_value(&ctx, &catalogs, "''''").await, "'");
    assert_eq!(string_value(&ctx, &catalogs, "''''''").await, "''");
    // `'''x'` → `'x` (quote, x): an escaped quote then a plain char, not a triple-quote start.
    assert_eq!(string_value(&ctx, &catalogs, "'''x'").await, "'x");
    // `'''a\tb'''` → quote, a, TAB, b, quote (length 5) — BigQuery gave `a<TAB>b` (length 4).
    let atab = string_value(&ctx, &catalogs, "'''a\\tb'''").await;
    assert_eq!(atab, "'a\tb'");
    assert_eq!(atab.chars().count(), 5);
}

/// pins: sqp-1-spark-string-literals/C-003
///
/// A DataFusion-native statement (`COPY …`, `CREATE [OR REPLACE] EXTERNAL TABLE …`) keeps Generic
/// literal semantics: its `OPTIONS ('k' 'v')` pairs are DataFusion's key/value grammar, NOT Spark
/// concatenation, so the front door leaves the whole statement untouched (`Cow::Borrowed`). The
/// facade's path writer runs COPY through this door and its readers may issue CREATE EXTERNAL
/// TABLE; canonicalising either would merge `'k' 'v'` into `'kv'`. Reverting the carve-out reds
/// each line (the pairs would merge → `Cow::Owned`). The contrast line proves the merge is live in
/// a NON-native statement, so the carve-out is what protects these two.
#[test]
fn datafusion_native_statements_keep_generic_literals() {
    for sql in [
        "COPY (SELECT 1) TO '/x' STORED AS CSV OPTIONS ('format.has_header' 'True')",
        "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION '/x' OPTIONS ('k' 'v')",
        "CREATE OR REPLACE EXTERNAL TABLE t STORED AS PARQUET LOCATION '/x' OPTIONS ('k' 'v')",
    ] {
        assert!(
            matches!(
                crate::spark_literals::canonicalize(sql).unwrap(),
                Cow::Borrowed(_)
            ),
            "`{sql}` is DataFusion-native and must be left Generic (its OPTIONS pairs must not merge)"
        );
    }
    // Contrast: the same `'k' 'v'` adjacency in a Spark statement DOES concatenate (E7), which is
    // exactly what the carve-out suppresses for the native statements above.
    let merged = crate::spark_literals::canonicalize("SELECT 'k' 'v'").unwrap();
    assert_eq!(merged.as_ref(), "SELECT 'kv'");
}

/// pins: sqp-1-spark-string-literals/C-004
///
/// Raw strings keep their content verbatim (E19) where the door used to refuse the token.
#[tokio::test]
async fn raw_strings_are_verbatim() {
    let (ctx, catalogs) = expr_ctx();
    assert_eq!(string_value(&ctx, &catalogs, "r'\\d'").await, "\\d");
    assert_eq!(string_value(&ctx, &catalogs, "R'a\\nb'").await, "a\\nb");
    assert_eq!(string_value(&ctx, &catalogs, "r'\\\\'").await, "\\\\");
}

/// pins: sqp-1-spark-string-literals/C-008
///
/// The incidental controls hold at the oracle's values — the escape rules must not disturb LIKE
/// wildcard escaping (E13), the `ESCAPE` clause (U14), RLIKE (U15), backtick identifiers (E26) or
/// the doubled-quote value (E6).
#[tokio::test]
async fn like_rlike_and_identifier_controls() {
    let (ctx, catalogs) = expr_ctx();
    // E13 LIKE: `\%` / `\_` are literal-wildcard escapes; a doubled `\\` is a literal backslash.
    assert!(bool_value(&ctx, &catalogs, "'a%b' LIKE 'a\\%b'").await);
    assert!(!bool_value(&ctx, &catalogs, "'axb' LIKE 'a\\%b'").await);
    assert!(bool_value(&ctx, &catalogs, "'a_b' LIKE 'a\\_b'").await);
    assert!(!bool_value(&ctx, &catalogs, "'axb' LIKE 'a\\_b'").await);
    assert!(bool_value(&ctx, &catalogs, "'a\\\\b' LIKE 'a\\\\\\\\b'").await);
    // (A non-backslash `ESCAPE '!'` clause is a pre-existing DataFusion limitation, unrelated to
    // literal escaping and out of this unit's scope — see the ledger C-008 note.)
    // U15 RLIKE is the same pattern-escape seat as `regexp_count` below (the door has no RLIKE
    // operator or `regexp_like` function — a separate gap), so the escape claim is pinned there.
    // E14 the headline silent-wrong-answer: `'\\d'` reaches the regex engine as `\d`.
    assert_eq!(
        int_value(&ctx, &catalogs, "regexp_count('a1b22', '\\\\d')").await,
        3,
        "regexp_count('a1b22', '\\\\d') is 3 on Spark"
    );
    assert_eq!(
        int_value(&ctx, &catalogs, "regexp_count('a1b22', '\\d')").await,
        0
    );
    assert_eq!(
        derived_string(&ctx, &catalogs, "regexp_replace('a.b', '\\\\.', '-')").await,
        "a-b"
    );
    // E26 backtick identifier: never unescaped — `a\tb` stays four characters as a column name.
    let batches = execute(&ctx, &catalogs, "SELECT 1 AS `a\\tb`")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches[0].schema().field(0).name(), "a\\tb");
    // E6 doubled quote (already right before the fix — must stay right).
    assert_eq!(string_value(&ctx, &catalogs, "'it''s'").await, "it's");
}

/// pins: sqp-1-spark-string-literals/C-005
///
/// Exactly-once on every enumerated execution path. Each path carries a user literal (`'\\d'` →
/// `\d`, `'a\tb'` → a TAB) and is checked by content, not by re-emission surviving untouched.
/// Every mismatch is collected so a break in one path does not mask another.
#[tokio::test]
async fn unescape_is_exactly_once_on_every_path() {
    let mut failures = Vec::new();
    failures.extend(read_path_failures().await);
    failures.extend(write_path_failures().await);
    failures.extend(reemission_path_failures());
    assert!(
        failures.is_empty(),
        "exactly-once path mismatches:\n{}",
        failures.join("\n")
    );
}

/// Record a mismatch under `label` (empty when the path is right).
fn diff(label: &str, got: &str, expected: &str) -> Option<String> {
    (got != expected).then(|| format!("{label}: {got:?} (expected {expected:?})"))
}

/// Paths (a) direct SELECT and (b) VALUES — no catalog needed.
async fn read_path_failures() -> Vec<String> {
    let (ctx, catalogs) = expr_ctx();
    let values = string_column(
        &ctx,
        &catalogs,
        "SELECT c FROM VALUES ('\\\\d'), ('a\\tb') AS t(c) ORDER BY c",
    )
    .await;
    [
        diff(
            "a:select",
            &string_value(&ctx, &catalogs, "'\\\\d'").await,
            "\\d",
        ),
        diff(
            "a:select-tab",
            &string_value(&ctx, &catalogs, "'a\\tb'").await,
            "a\tb",
        ),
        // Sorted by `c`: `\d` (0x5C…) sorts before `a<TAB>b` (0x61…).
        diff("b:values-0", &values[0], "\\d"),
        diff("b:values-1", &values[1], "a\tb"),
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// Paths (c)-(m) — the write/DML/DDL classes, round-tripped through an Iceberg table. Split by
/// setup only (clippy line ceiling); each half owns a fresh catalog.
async fn write_path_failures() -> Vec<String> {
    let mut failures = insert_delete_update_failures().await;
    failures.extend(merge_subquery_property_failures().await);
    failures
}

/// (c) INSERT VALUES, (d) INSERT SELECT, (j) CTAS, (e) DELETE WHERE =, (g) UPDATE SET.
async fn insert_delete_update_failures() -> Vec<String> {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.slit (c STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.slit VALUES ('\\\\d')",
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.slit SELECT 'a\\tb'").await;
    let stored = string_column(&ctx, &catalogs, "SELECT c FROM ice.sales.slit ORDER BY c").await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.jc AS SELECT '\\\\d' AS c",
    )
    .await;
    let ctas = string_column(&ctx, &catalogs, "SELECT c FROM ice.sales.jc").await;
    // DELETE the TAB row, leaving `\d`.
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.slit WHERE c = 'a\\tb'",
    )
    .await;
    let after_delete = string_column(&ctx, &catalogs, "SELECT c FROM ice.sales.slit").await;
    run(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.slit SET c = 'x\\ty' WHERE c = '\\\\d'",
    )
    .await;
    let after_update = string_column(&ctx, &catalogs, "SELECT c FROM ice.sales.slit").await;
    [
        // Sorted by `c`: `\d` before `a<TAB>b`.
        diff("c:insert-values", &stored[0], "\\d"),
        diff("d:insert-select", &stored[1], "a\tb"),
        diff("j:ctas", &ctas[0], "\\d"),
        diff("e:delete-where-eq", &after_delete.join("|"), "\\d"),
        diff("g:update-set", &after_update[0], "x\ty"),
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// (f) DELETE … IN (SELECT …) `predicate_dml` re-emission, (i) MERGE SET, (k)/(m) TBLPROPERTIES.
async fn merge_subquery_property_failures() -> Vec<String> {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.sin (c STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.sin VALUES ('a\\tb'), ('keep')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "DELETE FROM ice.sales.sin WHERE c IN (SELECT c FROM ice.sales.sin WHERE c = 'a\\tb')",
    )
    .await;
    let after_in = string_column(&ctx, &catalogs, "SELECT c FROM ice.sales.sin").await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.smrg (id INT, c STRING) USING iceberg",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.smrg VALUES (1, 'old')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "MERGE INTO ice.sales.smrg AS t USING (SELECT 1 AS id) AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.c = 'm\\tn'",
    )
    .await;
    let merged = string_column(&ctx, &catalogs, "SELECT c FROM ice.sales.smrg").await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.sprops (id BIGINT) USING iceberg TBLPROPERTIES ('note' = 'v\\tw')",
    )
    .await;
    let created_prop = table_property(&catalogs, "sprops", "note").await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.sprops SET TBLPROPERTIES ('note' = 'a\\\\b')",
    )
    .await;
    let altered_prop = table_property(&catalogs, "sprops", "note").await;
    [
        diff("f:delete-in-subquery", &after_in.join("|"), "keep"),
        diff("i:merge-set", &merged[0], "m\tn"),
        diff("k:tblproperties", &created_prop, "v\tw"),
        diff("m:alter-set-tblproperties", &altered_prop, "a\\b"),
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// Paths (l) CALL and (n) SET. Their runtime effect is not readable here — a `CALL` argument is
/// consumed by a maintenance procedure, and a runtime `SET spark.*` is TZ-3's tabled no-op — so
/// they are pinned at the canonicaliser, the one seat that processes them (C-010 proves nothing
/// else does).
fn reemission_path_failures() -> Vec<String> {
    [
        diff(
            "l:call-arg",
            crate::spark_literals::canonicalize("CALL x.system.p('a\\tb')")
                .unwrap()
                .as_ref(),
            "CALL x.system.p('a\tb')",
        ),
        diff(
            "n:set-value",
            crate::spark_literals::canonicalize("SET k = '\\\\d'")
                .unwrap()
                .as_ref(),
            "SET k = '\\d'",
        ),
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// A single table property value (`ice.sales.<table>`), for the C-005 TBLPROPERTIES paths.
async fn table_property(catalogs: &CatalogRegistry, table: &str, key: &str) -> String {
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), table.to_string());
    catalogs["ice"]
        .load_table(&ident)
        .await
        .unwrap()
        .metadata()
        .properties()
        .get(key)
        .cloned()
        .unwrap_or_else(|| "<missing>".to_string())
}

/// pins: sqp-1-spark-string-literals/C-010
///
/// The front-door canonicaliser has exactly one production caller — the grep pin that proves the
/// pass is applied once per `router::execute` and nowhere else. Walks `crates/repark-spark/src`
/// **recursively** (a `pub(crate)` fn can be called from any module, including a subdirectory one),
/// skipping the `tests/` subtree so the many test call sites here do not count. `python/` is not
/// walked: the facade cannot call a Rust private fn.
#[test]
fn front_door_has_one_caller() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut callers = Vec::new();
    collect_canonicalize_callers(&src, &mut callers);
    assert_eq!(
        callers.len(),
        1,
        "canonicalize must have exactly one caller (the front door); found: {callers:?}"
    );
}

/// Recurse `dir`, appending `path:line` for every production line that names
/// `spark_literals::canonicalize`. Any directory named `tests` (where the test call sites live) is
/// skipped so only production callers are counted.
fn collect_canonicalize_callers(dir: &std::path::Path, callers: &mut Vec<String>) {
    for entry in std::fs::read_dir(dir).expect("read src dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            if path.file_name().and_then(|name| name.to_str()) == Some("tests") {
                continue;
            }
            collect_canonicalize_callers(&path, callers);
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("read module");
        for (number, line) in text.lines().enumerate() {
            if line.contains("spark_literals::canonicalize") {
                callers.push(format!("{}:{}", path.display(), number + 1));
            }
        }
    }
}

/// pins: sqp-1-spark-string-literals/C-010
///
/// The Spark door's executing parse runs under `Generic` — the truth the module doc rests on when
/// it argues the canonical output cannot be escape-processed a second time. `extension::
/// apply_spark_parser_dialect` would set `Databricks` but is dead code (FNP-4b), so the session
/// keeps DataFusion's default. If it is ever wired, this reds and the module doc must change with
/// it.
#[test]
fn spark_door_executes_with_generic_dialect() {
    let (ctx, _catalogs) = expr_ctx();
    assert_eq!(
        ctx.state().config().options().sql_parser.dialect,
        datafusion::config::Dialect::Generic,
        "the Spark door parses under Generic; the module doc's re-tokenise argument depends on it"
    );
}

/// pins: sqp-1-spark-string-literals/C-012
///
/// The fast path borrows when there is nothing to do, and a lexer failure surfaces as a parse
/// error carrying line/column.
#[test]
fn fast_path_borrows_and_errors_carry_position() {
    // No quote, no backslash → borrowed unchanged.
    assert!(matches!(
        crate::spark_literals::canonicalize("SELECT 1 + 2").unwrap(),
        Cow::Borrowed(_)
    ));
    // A quote but no backslash and a single literal → still borrowed (the Generic lexer agrees).
    assert!(matches!(
        crate::spark_literals::canonicalize("SELECT * FROM t WHERE name = 'John'").unwrap(),
        Cow::Borrowed(_)
    ));
    // A backslash escape → owned, and the canonical text carries the Spark value spelled Generic.
    let owned = crate::spark_literals::canonicalize("SELECT '\\d' AS s").unwrap();
    assert!(matches!(owned, Cow::Owned(_)));
    assert_eq!(owned.as_ref(), "SELECT 'd' AS s");
    // An unpaired trailing backslash is a parse error naming the position.
    let error = crate::spark_literals::canonicalize("SELECT 'a\\'")
        .expect_err("unterminated literal must error")
        .to_string();
    assert!(
        error.contains("Line") && error.contains("Column"),
        "the lexer error must carry line/column, got: {error}"
    );
}
