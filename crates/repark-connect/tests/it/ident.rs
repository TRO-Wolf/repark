use repark_common::{Error, ErrorClass};
use repark_connect::{ConnectError, IdentRefusal, MAX_IDENT_BYTES, PgIdent, QualifiedRelation};

fn ident(name: &str) -> PgIdent {
    PgIdent::new(name).expect("a valid identifier")
}

#[test]
fn identifiers_render_double_quoted_with_quotes_doubled() {
    assert_eq!(ident("orders").to_string(), "\"orders\"");
    assert_eq!(ident("MixedCase").to_string(), "\"MixedCase\"");
    assert_eq!(ident("a\"b").to_string(), "\"a\"\"b\"");
    assert_eq!(ident("\"").to_string(), "\"\"\"\"");
    assert_eq!(
        ident("x\"; DROP TABLE t; --").to_string(),
        "\"x\"\"; DROP TABLE t; --\""
    );
    assert_eq!(ident("a\"b").as_str(), "a\"b");
    let relation = QualifiedRelation::new(ident("Sales"), ident("q\"1"));
    assert_eq!(relation.to_string(), "\"Sales\".\"q\"\"1\"");
}

#[test]
fn identifiers_refuse_empty_nul_and_more_than_63_bytes() {
    assert_eq!(ident(&"a".repeat(MAX_IDENT_BYTES)).as_str().len(), 63);
    assert_eq!(ident(&"é".repeat(31)).as_str().len(), 62);
    for (name, reason) in [
        (String::new(), IdentRefusal::Empty),
        ("a\0b".to_string(), IdentRefusal::Nul),
        ("a".repeat(64), IdentRefusal::TooLong { bytes: 64 }),
        ("é".repeat(32), IdentRefusal::TooLong { bytes: 64 }),
    ] {
        let error = PgIdent::new(name).expect_err("the identifier refuses");
        assert_eq!(error, ConnectError::InvalidIdentifier { reason });
        assert!(
            error.to_string().starts_with("invalid specification"),
            "{error}"
        );
        assert_eq!(
            Error::from(error).exception_class(),
            ErrorClass::IllegalArgument
        );
    }
}
