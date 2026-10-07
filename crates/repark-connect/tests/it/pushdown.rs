use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::TimestampMicrosecondArray;
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::catalog::TableProvider;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr::{Cast, InList};
use datafusion::logical_expr::{
    Between, BinaryExpr, Expr, Operator, TableProviderFilterPushDown, col, lit,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use repark_common::{SourceIdentity, SourceKind};
use repark_connect::postgres::{PgTypeKind, TypeMod};
use repark_connect::{
    ColumnClass, ColumnCollation, MIN_POSTGRES_DAYS, PgIdent, PostgresScanExec, PostgresSource,
    PostgresTable, QualifiedRelation, ResolvedSource, ScanColumn, ScanSource, SettingsDoor,
    WallClockLocaliser,
};

#[derive(Debug)]
pub(crate) struct FixedZone {
    pub(crate) offset_micros: i64,
    pub(crate) label: &'static str,
}

impl WallClockLocaliser for FixedZone {
    fn localise(
        &self,
        wall: &TimestampMicrosecondArray,
    ) -> repark_connect::Result<TimestampMicrosecondArray> {
        Ok(wall.unary(|micros| micros - self.offset_micros))
    }

    fn zone_label(&self) -> Arc<str> {
        self.label.into()
    }
}

pub(crate) fn column(name: &str, typname: &str, typmod: i32, nullable: bool) -> ScanColumn {
    let kind = if typname == "enum" {
        PgTypeKind::Enum
    } else {
        PgTypeKind::Base
    };
    ScanColumn::resolve(
        PgIdent::new(name).expect("name"),
        typname,
        kind,
        TypeMod::new(typmod),
        nullable,
    )
    .expect("a mapped column")
}

pub(crate) fn orders() -> Vec<ScanColumn> {
    vec![
        column("id", "int8", -1, false),
        column("small", "int2", -1, true),
        column("qty", "int4", -1, true),
        column("amount", "numeric", TypeMod::numeric(10, 2).get(), true),
        column("ratio", "numeric", -1, true),
        column("paid", "bool", -1, true),
        column("day", "date", -1, true),
        column("at", "timestamptz", -1, true),
        column("wall", "timestamp", -1, true),
        column("note", "text", -1, true),
        column("code", "varchar", -1, true),
        column("padded", "bpchar", -1, true),
        column("score", "float8", -1, true),
        column("blob", "bytea", -1, true),
        column("token", "uuid", -1, true),
        column("doc", "jsonb", -1, true),
        column("span", "interval", -1, true),
        column("mood", "enum", -1, true),
    ]
}

pub(crate) fn resolved(columns: Vec<ScanColumn>, encoding: &str) -> ResolvedSource {
    let relation = QualifiedRelation::new(
        PgIdent::new("public").expect("schema"),
        PgIdent::new("orders").expect("table"),
    );
    ResolvedSource {
        source: ScanSource::Relation(relation),
        columns,
        server_version_num: 160_000,
        server_encoding: encoding.into(),
    }
}

pub(crate) fn source(extra: &[(&str, &str)]) -> Arc<PostgresSource> {
    let mut props = BTreeMap::from([
        ("host".to_string(), "db.internal.example".to_string()),
        ("port".to_string(), "6543".to_string()),
        ("database".to_string(), "warehouse".to_string()),
        ("user".to_string(), "reader".to_string()),
        ("password".to_string(), "S3cretPw".to_string()),
        ("sslmode".to_string(), "disable".to_string()),
    ]);
    for (key, value) in extra {
        props.insert((*key).to_string(), (*value).to_string());
    }
    let identity = SourceIdentity::unassigned("company_db".to_string(), SourceKind::Postgres);
    let zone = FixedZone {
        offset_micros: -5 * 3_600_000_000,
        label: "-05:00",
    };
    PostgresSource::new(&identity, props, SettingsDoor::ReparkToml, Arc::new(zone))
}

pub(crate) fn table(extra: &[(&str, &str)], encoding: &str) -> PostgresTable {
    source(extra)
        .table(resolved(orders(), encoding))
        .expect("an injected resolution")
}

pub(crate) fn context(extra: &[(&str, &str)], encoding: &str) -> SessionContext {
    let context = SessionContext::new();
    context
        .register_table("orders", Arc::new(table(extra, encoding)))
        .expect("register");
    context
}

pub(crate) fn find_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<&PostgresScanExec> {
    if let Some(scan) = plan.downcast_ref::<PostgresScanExec>() {
        return Some(scan);
    }
    plan.children().into_iter().find_map(find_scan)
}

pub(crate) async fn physical(context: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
    context
        .sql(sql)
        .await
        .expect(sql)
        .create_physical_plan()
        .await
        .expect(sql)
}

pub(crate) struct Split {
    pushed: Vec<String>,
    residual: Vec<String>,
    copy: String,
    values: Vec<String>,
    limit: Option<u64>,
}

fn split_of(scan: &PostgresScanExec) -> Split {
    let statement = scan.request().statement();
    Split {
        pushed: scan
            .pushed_filters()
            .iter()
            .map(ToString::to_string)
            .collect(),
        residual: scan
            .residual_filters()
            .iter()
            .map(ToString::to_string)
            .collect(),
        copy: statement.copy,
        values: statement
            .settings
            .into_iter()
            .map(|(_, value)| value)
            .collect(),
        limit: scan.pushed_limit(),
    }
}

async fn split(context: &SessionContext, filter: &str) -> Split {
    let sql = format!("SELECT id FROM orders WHERE {filter}");
    let plan = physical(context, &sql).await;
    split_of(find_scan(&plan).expect("one Postgres scan"))
}

async fn split_expr(context: &SessionContext, filter: Expr) -> Split {
    let plan = context
        .table("orders")
        .await
        .expect("orders")
        .filter(filter)
        .expect("filter")
        .create_physical_plan()
        .await
        .expect("plan");
    split_of(find_scan(&plan).expect("one Postgres scan"))
}

fn pushdown_of(extra: &[(&str, &str)]) -> repark_connect::Pushdown {
    table(extra, "UTF8").pushdown().clone()
}

fn rendered(extra: &[(&str, &str)], filter: &Expr) -> Option<(String, Vec<String>)> {
    pushdown_of(extra)
        .render(filter, 0)
        .map(|rendered| (rendered.sql, rendered.values))
}

fn slot(index: u16, cast: &str) -> String {
    format!("pg_catalog.current_setting('repark.p{index}')::pg_catalog.{cast}")
}

fn assert_residual(split: &Split, filter: &str) {
    assert!(
        split.pushed.is_empty(),
        "{filter}: pushed {:?}",
        split.pushed
    );
    assert_eq!(split.residual.len(), 1, "{filter}: {:?}", split.residual);
    assert!(!split.copy.contains(" WHERE "), "{filter}: {}", split.copy);
    assert!(split.values.is_empty(), "{filter}");
}

#[tokio::test]
async fn p01_null_tests_push() {
    let context = context(&[], "UTF8");
    for (filter, sql) in [
        ("note IS NULL", "(\"note\" IS NULL)"),
        ("blob IS NOT NULL", "(\"blob\" IS NOT NULL)"),
        ("score IS NULL", "(\"score\" IS NULL)"),
        ("mood IS NULL", "(\"mood\" IS NULL)"),
        ("token IS NOT NULL", "(\"token\" IS NOT NULL)"),
        ("wall IS NULL", "(\"wall\" IS NULL)"),
        ("padded IS NULL", "(\"padded\" IS NULL)"),
    ] {
        let split = split(&context, filter).await;
        assert_eq!(split.pushed, [filter], "{filter}");
        assert!(split.residual.is_empty(), "{filter}");
        assert!(
            split.copy.contains(&format!(" WHERE {sql}")),
            "{}",
            split.copy
        );
        assert!(split.values.is_empty());
    }
}

#[tokio::test]
async fn p02_boolean_tests_push() {
    let context = context(&[], "UTF8");
    for (filter, sql) in [
        ("paid", "\"paid\""),
        ("NOT paid", "(NOT \"paid\")"),
        ("paid IS TRUE", "(\"paid\" IS TRUE)"),
        ("paid IS FALSE", "(\"paid\" IS FALSE)"),
        ("paid IS NOT TRUE", "(\"paid\" IS NOT TRUE)"),
        ("paid IS NOT FALSE", "(\"paid\" IS NOT FALSE)"),
        ("paid IS UNKNOWN", "(\"paid\" IS UNKNOWN)"),
        ("paid IS NOT UNKNOWN", "(\"paid\" IS NOT UNKNOWN)"),
    ] {
        let split = split(&context, filter).await;
        assert_eq!(split.pushed.len(), 1, "{filter}: {:?}", split.residual);
        assert!(
            split.copy.contains(&format!(" WHERE {sql})")),
            "{}",
            split.copy
        );
    }
    let is_not_true = Expr::IsNotTrue(Box::new(col("paid")));
    assert_eq!(
        rendered(&[], &is_not_true),
        Some(("(\"paid\" IS NOT TRUE)".to_string(), Vec::new()))
    );
}

#[tokio::test]
async fn p03_integer_comparison_pushes_cross_width() {
    let context = context(&[], "UTF8");
    let wide = split(&context, "small = 100000").await;
    assert_eq!(wide.pushed, ["CAST(small AS Int64) = Int64(100000)"]);
    let compare = format!("(\"small\" OPERATOR(pg_catalog.=) {})", slot(0, "int8"));
    assert!(wide.copy.contains(&compare), "{}", wide.copy);
    assert_eq!(wide.values, ["100000"]);
    let narrow = split(&context, "small = 5").await;
    assert_eq!(narrow.pushed, ["small = Int16(5)"]);
    assert!(narrow.copy.contains(&slot(0, "int2")), "{}", narrow.copy);
    for (filter, op) in [
        ("qty <> 3", "<>"),
        ("qty < 3", "<"),
        ("qty <= 3", "<="),
        ("qty > 3", ">"),
        ("qty >= 3", ">="),
    ] {
        let split = split(&context, filter).await;
        let compare = format!("(\"qty\" OPERATOR(pg_catalog.{op}) {})", slot(0, "int4"));
        assert!(split.copy.contains(&compare), "{filter}: {}", split.copy);
    }
    let flipped = Expr::Literal(ScalarValue::Int32(Some(3)), None).lt(col("qty"));
    let (sql, values) = rendered(&[], &flipped).expect("a literal on the left pushes");
    assert_eq!(
        sql,
        format!("(\"qty\" OPERATOR(pg_catalog.>) {})", slot(0, "int4"))
    );
    assert_eq!(values, ["3"]);
    let distinct = split(&context, "qty IS DISTINCT FROM 3").await;
    let equal = format!("\"qty\" OPERATOR(pg_catalog.=) {}", slot(0, "int4"));
    let not_true = format!("(({equal}) IS NOT TRUE)");
    assert!(distinct.copy.contains(&not_true), "{}", distinct.copy);
    let same = split(&context, "qty IS NOT DISTINCT FROM 3").await;
    assert!(
        same.copy.contains(&format!("(({equal}) IS TRUE)")),
        "{}",
        same.copy
    );
    let distinct_null = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(col("qty")),
        Operator::IsDistinctFrom,
        Box::new(lit(ScalarValue::Int32(None))),
    ));
    assert_eq!(
        rendered(&[], &distinct_null),
        Some(("(\"qty\" IS NOT NULL)".to_string(), Vec::new()))
    );
}

#[tokio::test]
async fn p04_decimal_comparison_pushes() {
    let context = context(&[], "UTF8");
    let finer = split(&context, "amount >= CAST(100.005 AS DECIMAL(12,3))").await;
    assert_eq!(
        finer.pushed,
        ["CAST(amount AS Decimal128(12, 3)) >= Decimal128(Some(100005),12,3)"]
    );
    let compare = format!(
        "(\"amount\" OPERATOR(pg_catalog.>=) {})",
        slot(0, "numeric")
    );
    assert!(finer.copy.contains(&compare), "{}", finer.copy);
    assert_eq!(finer.values, ["100.005"]);
    let negative = split(&context, "amount < CAST(-0.5 AS DECIMAL(2,1))").await;
    assert_eq!(negative.values, ["-0.50"]);
    let unconstrained = split(
        &context,
        "ratio = CAST(0.000000000000000001 AS DECIMAL(38,18))",
    )
    .await;
    let rounded = format!(
        "(\"ratio\"::pg_catalog.numeric(38,18) OPERATOR(pg_catalog.=) {})",
        slot(0, "numeric")
    );
    assert!(
        unconstrained.copy.contains(&rounded),
        "{}",
        unconstrained.copy
    );
    assert_eq!(unconstrained.values, ["0.000000000000000001"]);
    let narrowing = Expr::Cast(Cast::new(
        Box::new(col("amount")),
        DataType::Decimal128(5, 1),
    ));
    let narrowed = narrowing.gt(lit(ScalarValue::Decimal128(Some(15), 5, 1)));
    assert_eq!(
        rendered(&[], &narrowed),
        None,
        "a narrowing cast stays residual"
    );
    let widening = Expr::Cast(Cast::new(
        Box::new(col("amount")),
        DataType::Decimal128(12, 3),
    ));
    let widened = widening.gt(lit(ScalarValue::Decimal128(Some(15), 12, 3)));
    assert!(
        rendered(&[], &widened).is_some(),
        "a lossless widening pushes"
    );
    assert_eq!(repark_connect::decimal_text(12_345_678, 3), "12345.678");
    assert_eq!(repark_connect::decimal_text(-5, 3), "-0.005");
    assert_eq!(repark_connect::decimal_text(12, -2), "1200");
}

#[tokio::test]
async fn p05_temporal_comparison_pushes_inside_range() {
    let ntz = context(&[("prefer_timestamp_ntz", "true")], "UTF8");
    let day = split(&ntz, "day >= DATE '2024-03-10'").await;
    assert!(day.copy.contains(&slot(0, "date")), "{}", day.copy);
    assert_eq!(day.values, ["2024-03-10"]);
    let at = split(&ntz, "at < TIMESTAMP '2024-03-10 12:00:00.5'").await;
    assert!(at.copy.contains(&slot(0, "timestamptz")), "{}", at.copy);
    assert_eq!(at.values, ["2024-03-10 12:00:00.500000+00"]);
    let wall = split(&ntz, "wall = TIMESTAMP '1970-01-01 00:00:00'").await;
    assert!(wall.copy.contains(&slot(0, "timestamp")), "{}", wall.copy);
    assert_eq!(wall.values, ["1970-01-01 00:00:00.000000"]);
    let first = ScalarValue::Date32(Some(i32::try_from(MIN_POSTGRES_DAYS).expect("days")));
    let earliest = split_expr(&ntz, col("day").gt_eq(lit(first))).await;
    assert_eq!(earliest.values, ["4714-11-24 BC"]);
    let before = ScalarValue::Date32(Some(i32::try_from(MIN_POSTGRES_DAYS - 1).expect("days")));
    let too_early = split_expr(&ntz, col("day").gt_eq(lit(before))).await;
    assert_residual(&too_early, "a date before 4714-11-24 BC");
    let micros = MIN_POSTGRES_DAYS * 86_400_000_000;
    let first_instant = ScalarValue::TimestampMicrosecond(Some(micros), Some("UTC".into()));
    let instant = split_expr(&ntz, col("at").gt_eq(lit(first_instant))).await;
    assert_eq!(instant.values, ["4714-11-24 00:00:00.000000+00 BC"]);
    let earlier = ScalarValue::TimestampMicrosecond(Some(micros - 1), Some("UTC".into()));
    let too_early = split_expr(&ntz, col("at").gt_eq(lit(earlier))).await;
    assert_residual(&too_early, "an instant before 4714-11-24 BC");
    let year_one = split(&ntz, "day = DATE '0001-01-01'").await;
    assert_eq!(year_one.values, ["0001-01-01"]);
    assert_eq!(repark_connect::date_text(-719_163), "0001-12-31 BC");
    assert_eq!(
        repark_connect::timestamp_text(-1, ""),
        Some("1969-12-31 23:59:59.999999".to_string())
    );
}

#[tokio::test]
async fn p06_text_comparison_is_code_point_order() {
    let context = context(&[], "UTF8");
    for (filter, op) in [("note < 'a'", "<"), ("code >= 'B'", ">=")] {
        let split = split(&context, filter).await;
        let column = if filter.starts_with("note") {
            "note"
        } else {
            "code"
        };
        let compare = format!(
            "(\"{column}\" COLLATE pg_catalog.\"C\" OPERATOR(pg_catalog.{op}) {})",
            slot(0, "text")
        );
        assert!(split.copy.contains(&compare), "{filter}: {}", split.copy);
    }
    let ascii = context_with_encoding("SQL_ASCII");
    assert_residual(
        &split(&ascii, "note < 'a'").await,
        "text on a non-UTF8 server",
    );
    assert_eq!(pushdown_of(&[]).class("note"), Some(ColumnClass::Text));
    assert_eq!(
        pushdown_of(&[]).class("padded"),
        Some(ColumnClass::NullTestOnly)
    );
}

fn context_with_encoding(encoding: &str) -> SessionContext {
    context(&[], encoding)
}

#[tokio::test]
async fn p06b_text_equality_ignores_nondeterministic_collation() {
    let mut columns = orders();
    if let Some(note) = columns
        .iter_mut()
        .find(|column| column.name.as_str() == "note")
    {
        note.collation = Some(ColumnCollation {
            name: PgIdent::new("und-ci").expect("collation"),
            deterministic: false,
        });
    }
    let table = source(&[]).table(resolved(columns, "UTF8")).expect("table");
    let context = SessionContext::new();
    context
        .register_table("orders", Arc::new(table))
        .expect("register");
    let split = split(&context, "note = 'abc'").await;
    let compare = format!(
        "(\"note\" COLLATE pg_catalog.\"C\" OPERATOR(pg_catalog.=) {})",
        slot(0, "text")
    );
    assert!(split.copy.contains(&compare), "{}", split.copy);
    assert_eq!(split.values, ["abc"]);
}

#[tokio::test]
async fn p06c_nul_literal_stays_residual() {
    let context = context(&[], "UTF8");
    let nul = split_expr(&context, col("note").eq(lit("a\0b"))).await;
    assert_residual(&nul, "a NUL literal");
    let like = split_expr(&context, col("note").like(lit("a\0%"))).await;
    assert_residual(&like, "a NUL pattern");
}

#[tokio::test]
async fn p07_in_list_three_valued() {
    let not_in = Expr::InList(InList::new(
        Box::new(col("qty")),
        vec![lit(1_i32), lit(ScalarValue::Int32(None))],
        true,
    ));
    let (sql, values) = rendered(&[], &not_in).expect("NOT IN with a NULL item pushes");
    let equal = "\"qty\" OPERATOR(pg_catalog.=)";
    let expected = format!(
        "(NOT ({equal} {} OR {equal} NULL::pg_catalog.int4))",
        slot(0, "int4")
    );
    assert_eq!(sql, expected);
    assert_eq!(values, ["1"]);
    let context = context(&[], "UTF8");
    let five = split(&context, "note IN ('a', 'b', 'c', 'd', 'e')").await;
    assert_eq!(five.values, ["a", "b", "c", "d", "e"]);
    assert!(five.copy.contains(" OR "), "{}", five.copy);
    let items = |count: usize| {
        (0..count)
            .map(|item| item.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    };
    let most = split(&context, &format!("qty IN ({})", items(256))).await;
    assert_eq!(most.values.len(), 256);
    let past = split(&context, &format!("qty IN ({})", items(257))).await;
    assert_residual(&past, "257 items");
}

#[tokio::test]
async fn p08_between_renders_inclusive() {
    let between = Expr::Between(Between::new(
        Box::new(col("qty")),
        false,
        Box::new(lit(2_i32)),
        Box::new(lit(4_i32)),
    ));
    let (sql, values) = rendered(&[], &between).expect("BETWEEN pushes");
    let range = format!(
        "((\"qty\" OPERATOR(pg_catalog.>=) {}) AND (\"qty\" OPERATOR(pg_catalog.<=) {}))",
        slot(0, "int4"),
        slot(1, "int4")
    );
    assert_eq!(sql, range);
    assert_eq!(values, ["2", "4"]);
    let negated = Expr::Between(Between::new(
        Box::new(col("qty")),
        true,
        Box::new(lit(2_i32)),
        Box::new(lit(4_i32)),
    ));
    assert_eq!(
        rendered(&[], &negated).map(|(sql, _)| sql),
        Some(format!("(NOT {range})"))
    );
    let context = context(&[], "UTF8");
    let split = split(&context, "qty BETWEEN 2 AND 4").await;
    assert_eq!(split.values, ["2", "4"]);
    assert!(split.residual.is_empty());
}

#[tokio::test]
async fn p09_like_pushes_well_formed_patterns() {
    let context = context(&[], "UTF8");
    for (filter, op, pattern) in [
        (r"note LIKE 'a\%b_'", "~~", r"a\%b_"),
        (r"note NOT LIKE '%\\%'", "!~~", r"%\\%"),
        ("code LIKE '_'", "~~", "_"),
    ] {
        let split = split(&context, filter).await;
        let column = if filter.starts_with("note") {
            "note"
        } else {
            "code"
        };
        let like = format!(
            "(\"{column}\" COLLATE pg_catalog.\"C\" OPERATOR(pg_catalog.{op}) {})",
            slot(0, "text")
        );
        assert!(split.copy.contains(&like), "{filter}: {}", split.copy);
        assert_eq!(split.values, [pattern], "{filter}");
    }
    for filter in [
        r"note LIKE 'ab\'",
        r"note LIKE 'a\b%'",
        "note ILIKE 'a%'",
        "padded LIKE 'a%'",
        "token LIKE 'a%'",
        "note SIMILAR TO 'a%'",
    ] {
        assert_residual(&split(&context, filter).await, filter);
    }
}

#[tokio::test]
async fn p10_logic_pushes_only_exact_children() {
    let context = context(&[], "UTF8");
    let half = split(&context, "qty = 1 OR lower(note) = 'x'").await;
    assert_residual(&half, "an OR with one inexact child");
    let both = split(&context, "id = 3 OR qty = 4").await;
    let either = format!(
        "((\"id\" OPERATOR(pg_catalog.=) {}) OR (\"qty\" OPERATOR(pg_catalog.=) {}))",
        slot(0, "int8"),
        slot(1, "int4")
    );
    assert!(both.copy.contains(&either), "{}", both.copy);
    let not = split(&context, "NOT (qty = 1 AND lower(note) = 'a')").await;
    assert_residual(&not, "a NOT over an inexact child");
    let negated = Expr::Not(Box::new(
        col("qty").eq(lit(1_i32)).and(col("note").eq(lit("a"))),
    ));
    let (sql, values) = rendered(&[], &negated).expect("NOT over exact children pushes");
    let both = format!(
        "(NOT ((\"qty\" OPERATOR(pg_catalog.=) {}) AND (\"note\" COLLATE pg_catalog.\"C\" \
         OPERATOR(pg_catalog.=) {})))",
        slot(0, "int4"),
        slot(1, "text")
    );
    assert_eq!(sql, both);
    assert_eq!(values, ["1", "a"]);
    let mixed = split(&context, "qty = 1 AND lower(note) = 'x'").await;
    assert_eq!(mixed.pushed, ["qty = Int32(1)"]);
    assert_eq!(mixed.residual, ["lower(note) = Utf8(\"x\")"]);
}

#[tokio::test]
async fn p11_limit_pushes_only_without_residual() {
    let context = context(&[], "UTF8");
    let exact = split(&context, "qty > 1 LIMIT 5").await;
    assert_eq!(exact.limit, Some(5));
    assert!(
        exact.copy.ends_with(" LIMIT 5) TO STDOUT (FORMAT BINARY)"),
        "{}",
        exact.copy
    );
    let residual = split(&context, "score > 1 LIMIT 5").await;
    assert_eq!(residual.limit, None);
    assert!(!residual.copy.contains("LIMIT"), "{}", residual.copy);
    let table = table(&[], "UTF8");
    let state = context.state();
    let filters = [col("score").gt(lit(1.0_f64)), col("qty").gt(lit(1_i32))];
    let plan = table
        .scan(&state, None, &filters, Some(5))
        .await
        .expect("scan");
    let scan = find_scan(&plan).expect("the scan");
    assert_eq!(
        scan.pushed_limit(),
        None,
        "the scan re-checks for a residual"
    );
    assert_eq!(split_of(scan).pushed, ["qty > Int32(1)"]);
}

#[tokio::test]
async fn a_limit_past_i64_max_never_pushes() {
    let context = context(&[], "UTF8");
    let past = physical(
        &context,
        "SELECT id FROM orders LIMIT 9223372036854775807 OFFSET 5",
    )
    .await;
    let scan = find_scan(&past).expect("the scan");
    assert_eq!(scan.pushed_limit(), None);
    assert!(!split_of(scan).copy.contains("LIMIT"));
    let at = physical(&context, "SELECT id FROM orders LIMIT 9223372036854775807").await;
    let scan = find_scan(&at).expect("the scan");
    assert_eq!(scan.pushed_limit(), Some(9_223_372_036_854_775_807));
    let table = table(&[], "UTF8");
    let state = context.state();
    for (limit, pushed) in [
        (usize::MAX, None),
        (1 << 63, None),
        ((1 << 63) - 1, Some(i64::MAX)),
    ] {
        let plan = table
            .scan(&state, None, &[], Some(limit))
            .await
            .expect("scan");
        let pushed = pushed.and_then(|limit| u64::try_from(limit).ok());
        assert_eq!(
            find_scan(&plan).expect("the scan").pushed_limit(),
            pushed,
            "{limit}"
        );
    }
}

#[tokio::test]
async fn r01_float_comparisons_stay_residual() {
    let context = context(&[], "UTF8");
    for filter in ["score > 1.0", "score = 0", "score IN (1.0, 2.0, 3.0, 4.0)"] {
        assert_residual(&split(&context, filter).await, filter);
    }
    assert_eq!(
        pushdown_of(&[]).class("score"),
        Some(ColumnClass::NullTestOnly)
    );
}

#[tokio::test]
async fn r02_bpchar_comparisons_stay_residual() {
    let context = context(&[], "UTF8");
    for filter in [
        "padded = 'a'",
        "padded < 'b'",
        "padded IN ('a', 'b', 'c', 'd')",
    ] {
        assert_residual(&split(&context, filter).await, filter);
    }
}

#[tokio::test]
async fn r03_ltz_timestamp_comparisons_stay_residual() {
    let ltz = table(&[], "UTF8");
    let field = ltz.schema().field_with_name("wall").expect("wall").clone();
    let zone = DataType::Timestamp(TimeUnit::Microsecond, Some("-05:00".into()));
    assert_eq!(field.data_type(), &zone);
    assert_eq!(
        ltz.pushdown().class("wall"),
        Some(ColumnClass::NullTestOnly)
    );
    let context = context(&[], "UTF8");
    let split = split(&context, "wall > TIMESTAMP '2024-11-03 06:00:00'").await;
    assert_residual(&split, "an LTZ timestamp");
    let ntz = table(&[("prefer_timestamp_ntz", "true")], "UTF8");
    let field = ntz.schema().field_with_name("wall").expect("wall").clone();
    assert_eq!(
        field.data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(ntz.pushdown().class("wall"), Some(ColumnClass::Timestamp));
}

#[tokio::test]
async fn r04_functions_arithmetic_and_casts_stay_residual() {
    let context = context(&[], "UTF8");
    for filter in [
        "id + 1 > 5",
        "lower(note) = 'x'",
        "substr(note, 1, 1) = 'x'",
        "CAST(note AS INT) = 1",
        "CASE WHEN qty > 1 THEN true ELSE false END",
        "COALESCE(qty, 0) = 1",
        "note ~ 'a.*'",
        "token = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
        "doc = '{}'",
        "span = '1 day'",
        "mood = 'happy'",
        "qty = id",
        "random() > 0.5",
    ] {
        let split = split(&context, filter).await;
        assert!(split.pushed.is_empty(), "{filter}: {:?}", split.pushed);
        assert!(!split.copy.contains(" WHERE "), "{filter}: {}", split.copy);
    }
}

#[tokio::test]
async fn r05_pushdown_predicate_false_pushes_nothing() {
    let off = context(&[("pushdown_predicate", "false")], "UTF8");
    for filter in ["qty = 1", "note IS NULL", "paid", "qty > 1 LIMIT 5"] {
        let split = split(&off, filter).await;
        assert!(split.pushed.is_empty(), "{filter}: {:?}", split.pushed);
        assert_eq!(split.limit, None, "{filter}");
        assert!(!split.copy.contains(" WHERE "), "{filter}: {}", split.copy);
    }
    let support = table(&[("pushdown_predicate", "false")], "UTF8")
        .supports_filters_pushdown(&[&col("qty").eq(lit(1_i32))])
        .expect("support");
    assert_eq!(support, [TableProviderFilterPushDown::Inexact]);
}

#[tokio::test]
async fn pushed_values_past_1024_fail_the_plan() {
    let context = context(&[], "UTF8");
    let list = |quote: &str| {
        (0..256)
            .map(|item| format!("{quote}{item}{quote}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let four = format!(
        "id IN ({}) AND qty IN ({}) AND small IN ({}) AND note IN ({})",
        list(""),
        list(""),
        list(""),
        list("'")
    );
    assert_eq!(split(&context, &four).await.values.len(), 1024);
    let five = format!("{four} AND code IN ({})", list("'"));
    let error = context
        .sql(&format!("SELECT id FROM orders WHERE {five}"))
        .await
        .expect("plans logically")
        .create_physical_plan()
        .await
        .expect_err("a 1025th value refuses");
    assert!(
        error.to_string().contains("more than 1024 values"),
        "{error}"
    );
}

#[tokio::test]
async fn a_filter_the_optimizer_would_still_rewrite_stays_inexact() {
    let pushdown = pushdown_of(&[]);
    let unsettled = col("qty").not_eq(lit(ScalarValue::Int32(None)));
    assert!(
        pushdown.render(&unsettled, 0).is_some(),
        "the shape itself renders"
    );
    assert_eq!(
        pushdown.support(&unsettled),
        TableProviderFilterPushDown::Inexact
    );
    let settled = col("qty").not_eq(lit(1_i32));
    assert_eq!(
        pushdown.support(&settled),
        TableProviderFilterPushDown::Exact
    );
    let qualified = Expr::Column(datafusion::common::Column::new(Some("orders"), "qty"));
    assert_eq!(
        pushdown.support(&qualified.gt(lit(1_i32))),
        TableProviderFilterPushDown::Exact
    );
    let context = context(&[], "UTF8");
    let plan = physical(&context, "SELECT id FROM orders WHERE qty NOT IN (1, NULL)").await;
    let text = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        find_scan(&plan).is_none() || text.contains("FilterExec"),
        "{text}"
    );
}
