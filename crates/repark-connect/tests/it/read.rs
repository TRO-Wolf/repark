use std::sync::Arc;

use repark_common::{Error, ErrorClass};
use repark_connect::postgres::{PgTypeKind, TypeMod};
use repark_connect::{
    CastType, CompareOp, ConnectError, DEFAULT_SCHEMA, IdentRefusal, MAX_PARAM_SLOTS,
    MIN_SERVER_VERSION_NUM, ParamSlot, PgIdent, Privilege, QualifiedRelation, ResolvedSource,
    SERVER_VERSION_ROW, ScanColumn, ScanRequest, ScanSource, check_server_version,
};

fn ident(name: &str) -> PgIdent {
    PgIdent::new(name).expect("identifier")
}

fn column(name: &str, typname: &str, kind: PgTypeKind, typmod: TypeMod) -> ScanColumn {
    ScanColumn::resolve(ident(name), typname, kind, typmod, true).expect("column resolves")
}

fn resolved(source: ScanSource) -> Arc<ResolvedSource> {
    Arc::new(ResolvedSource {
        source,
        columns: vec![
            column("id", "int4", PgTypeKind::Base, TypeMod::NONE),
            column(
                "amount",
                "numeric",
                PgTypeKind::Base,
                TypeMod::numeric(8, 3),
            ),
            column("iv", "interval", PgTypeKind::Base, TypeMod::NONE),
            column("doc", "jsonb", PgTypeKind::Base, TypeMod::NONE),
            column("mood", "mood", PgTypeKind::Enum, TypeMod::NONE),
            column("free", "numeric", PgTypeKind::Base, TypeMod::NONE),
            column(
                "We\"ird",
                "numeric",
                PgTypeKind::Base,
                TypeMod::numeric(5, -2),
            ),
        ],
        server_version_num: 160_004,
        server_encoding: "UTF8".into(),
    })
}

fn relation(schema: &str, table: &str) -> QualifiedRelation {
    QualifiedRelation::new(ident(schema), ident(table))
}

#[test]
fn statement_casts_every_column_and_server_text_to_text() {
    let request = ScanRequest::new(resolved(ScanSource::Relation(relation("s", "t"))));
    let statement = request.statement();
    assert_eq!(
        statement.copy,
        "COPY (SELECT \"id\"::pg_catalog.int4, \"amount\"::pg_catalog.numeric(8,3), \
         \"iv\"::pg_catalog.text, \"doc\"::pg_catalog.jsonb, \"mood\"::pg_catalog.text, \
         \"free\"::pg_catalog.numeric, \"We\"\"ird\"::pg_catalog.numeric(5,-2) FROM \"s\".\"t\") \
         TO STDOUT (FORMAT BINARY)"
    );
    assert!(statement.settings.is_empty());
    assert_eq!(statement.set_config_sql(), None);
    let casts: Vec<CastType> = resolved(ScanSource::query("SELECT 1"))
        .columns
        .iter()
        .map(|column| column.cast)
        .collect();
    assert_eq!(
        casts[2],
        CastType::Text,
        "interval reads as the server's text"
    );
    assert_eq!(casts[4], CastType::Text, "an enum reads as its label");
}

#[test]
fn pushed_values_ride_set_config_never_the_statement_text() {
    let hostile = "1 day'); DROP TABLE t; --";
    let request = ScanRequest::new(resolved(ScanSource::Relation(relation("s", "t"))))
        .project(&[0, 2])
        .expect("projection")
        .compare(1, CompareOp::Gt, "12.5".to_string())
        .expect("first value")
        .compare(2, CompareOp::Eq, hostile.to_string())
        .expect("second value")
        .limit(10);
    let statement = request.statement();
    assert_eq!(
        statement.copy,
        "COPY (SELECT \"id\"::pg_catalog.int4, \"iv\"::pg_catalog.text FROM \"s\".\"t\" WHERE \
         \"amount\" > pg_catalog.current_setting('repark.p0')::pg_catalog.numeric AND \
         \"iv\"::pg_catalog.text = pg_catalog.current_setting('repark.p1')::pg_catalog.text \
         LIMIT 10) TO STDOUT (FORMAT BINARY)"
    );
    assert!(!statement.copy.contains("12.5") && !statement.copy.contains("DROP"));
    assert_eq!(
        statement.settings,
        [
            ("repark.p0".to_string(), "12.5".to_string()),
            ("repark.p1".to_string(), hostile.to_string()),
        ]
    );
    assert_eq!(
        statement.set_config_sql().as_deref(),
        Some("SELECT pg_catalog.set_config($1, $2, true), pg_catalog.set_config($3, $4, true)")
    );
}

#[test]
fn param_slots_stop_at_1024_and_bad_indexes_refuse() {
    assert_eq!(MAX_PARAM_SLOTS, 1024);
    assert!(ParamSlot::new(1023).is_some());
    assert_eq!(ParamSlot::new(1024), None);
    let mut request = ScanRequest::new(resolved(ScanSource::query("SELECT 1")));
    for value in 0..1024 {
        request = request
            .compare(0, CompareOp::LtEq, value.to_string())
            .expect("a free slot");
    }
    assert_eq!(request.statement().settings.len(), 1024);
    assert!(
        request
            .clone()
            .compare(0, CompareOp::Lt, "0".into())
            .is_none()
    );
    assert!(
        request
            .clone()
            .compare(7, CompareOp::Lt, "0".into())
            .is_none()
    );
    assert!(request.project(&[0, 7]).is_none());
}

#[test]
fn query_mode_wraps_the_statement_and_an_empty_projection_selects_nothing() {
    let request = ScanRequest::new(resolved(ScanSource::query("SELECT 1 AS id")))
        .project(&[])
        .expect("empty projection");
    assert_eq!(
        request.statement().copy,
        "COPY (SELECT FROM (SELECT 1 AS id) AS repark_q) TO STDOUT (FORMAT BINARY)"
    );
    assert_eq!(request.relation(), None);
    assert_eq!(
        ScanSource::from_dbtable(" (SELECT 1) AS q").expect("subquery dbtable"),
        ScanSource::query("SELECT * FROM  (SELECT 1) AS q")
    );
}

#[test]
fn dbtable_parses_exact_qualified_and_quoted_parts() {
    let parse = |dbtable: &str| QualifiedRelation::parse(dbtable);
    assert_eq!(parse("sales.Orders"), Ok(relation("sales", "Orders")));
    assert_eq!(parse("Orders"), Ok(relation(DEFAULT_SCHEMA, "Orders")));
    assert_eq!(
        parse("\"My.Schema\".\"t\"\"x\""),
        Ok(relation("My.Schema", "t\"x"))
    );
    assert_eq!(
        parse("\"My.Schema\".\"t\"\"x\"").map(|parsed| parsed.to_string()),
        Ok("\"My.Schema\".\"t\"\"x\"".to_string())
    );
    let malformed = Err(ConnectError::InvalidIdentifier {
        reason: IdentRefusal::Qualification,
    });
    for dbtable in ["a.b.c", "\"open", "s.\"t\"x", "s\"t\""] {
        assert_eq!(parse(dbtable), malformed, "{dbtable}");
    }
    let empty = Err(ConnectError::InvalidIdentifier {
        reason: IdentRefusal::Empty,
    });
    for dbtable in ["", "s.", ".t", "\"\".t"] {
        assert_eq!(parse(dbtable), empty, "{dbtable}");
    }
    assert_eq!(
        ScanSource::from_dbtable("s.t"),
        Ok(ScanSource::Relation(relation("s", "t")))
    );
}

#[test]
fn servers_older_than_14_are_declared() {
    assert_eq!(MIN_SERVER_VERSION_NUM, 140_000);
    for supported in [140_000, 140_013, 160_004, 180_000] {
        assert_eq!(check_server_version(supported), Ok(()), "{supported}");
    }
    let refused = check_server_version(139_999).expect_err("13 is declared");
    assert_eq!(
        refused,
        ConnectError::DeclaredServerVersion {
            server_version_num: 139_999
        }
    );
    assert!(refused.to_string().contains(SERVER_VERSION_ROW));
    assert_eq!(SERVER_VERSION_ROW, "CONNECT-DECL-pg-server-version");
    assert_eq!(
        Error::from(refused).exception_class(),
        ErrorClass::Unsupported
    );
}

#[test]
fn read_errors_name_the_relation_and_fold_as_operational() {
    let denied = ConnectError::PermissionDenied {
        relation: relation("s", "t"),
        privilege: Privilege::Select,
    };
    assert_eq!(
        denied.to_string(),
        "the Postgres role lacks the SELECT privilege that reading \"s\".\"t\" needs"
    );
    let missing = ConnectError::RelationNotFound {
        relation: relation("s", "gone"),
    };
    assert!(missing.to_string().contains("\"s\".\"gone\""));
    for error in [denied, missing] {
        assert!(matches!(Error::from(error), Error::DataFusion(_)));
    }
}
