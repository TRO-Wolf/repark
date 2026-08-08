//! Lowering pins for `MERGE INTO`. The executor is tier-1 and has its own battery; what is
//! pinned here is the ANSI→[`MergeSpec`] mapping, which is the half that could drift from the
//! Spark door's mapping of the same target type (design §6 R3).

use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use super::*;

fn parse(sql: &str) -> Merge {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("MERGE must parse");
    let Statement::Merge(merge) = statements.remove(0) else {
        panic!("expected a MERGE statement");
    };
    merge
}

fn lower_sql(sql: &str) -> (String, MergeSpec) {
    let merge = parse(sql);
    lower(&merge.table, &merge.source, &merge.on, &merge.clauses).expect("lowering must succeed")
}

fn lower_error(sql: &str) -> String {
    let merge = parse(sql);
    lower(&merge.table, &merge.source, &merge.on, &merge.clauses)
        .expect_err("lowering must refuse")
        .to_string()
}

/// The classic upsert round-trips: catalog split off the target, aliases kept, ON text preserved,
/// clauses in declaration order, expressions re-rendered verbatim.
#[test]
fn lowers_the_classic_upsert() {
    let (catalog, spec) = lower_sql(
        "MERGE INTO ice.sales.orders AS t USING staging AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET amount = s.amount \
         WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)",
    );
    assert_eq!(catalog, "ice");
    assert_eq!(spec.target.name, "orders");
    assert_eq!(spec.target.namespace().to_url_string(), "sales");
    assert_eq!(spec.target_alias, "t");
    assert_eq!(spec.source_from_sql, "staging");
    assert_eq!(spec.source_alias, "s");
    assert_eq!(spec.on_sql, "t.id = s.id");

    assert_eq!(spec.matched.len(), 1);
    assert!(spec.matched[0].predicate_sql.is_none());
    let MatchedAction::Update { assignments } = &spec.matched[0].action else {
        panic!("expected an UPDATE action");
    };
    assert_eq!(
        assignments,
        &vec![("amount".to_string(), "s.amount".to_string())]
    );

    assert_eq!(spec.not_matched.len(), 1);
    let InsertAction::Explicit {
        columns,
        values_sql,
    } = &spec.not_matched[0].action
    else {
        panic!("expected an explicit INSERT action");
    };
    assert_eq!(columns, &vec!["id".to_string(), "amount".to_string()]);
    assert_eq!(
        values_sql,
        &vec!["s.id".to_string(), "s.amount".to_string()]
    );
}

/// An UNALIASED relation is referenced by its bare name — the alias the executor uses must match
/// the one the user's own ON/SET text refers to, or every expression fails to resolve.
#[test]
fn unaliased_relations_take_their_bare_name_as_the_alias() {
    let (_, spec) = lower_sql(
        "MERGE INTO ice.sales.orders USING staging ON orders.id = staging.id \
         WHEN MATCHED THEN DELETE",
    );
    assert_eq!(spec.target_alias, "orders");
    assert_eq!(spec.source_alias, "staging");
}

/// Clause predicates, DELETE, and multiple WHEN clauses all survive in declaration order.
#[test]
fn clause_predicates_and_order_survive() {
    let (_, spec) = lower_sql(
        "MERGE INTO ice.sales.orders AS t USING staging AS s ON t.id = s.id \
         WHEN MATCHED AND s.deleted THEN DELETE \
         WHEN MATCHED THEN UPDATE SET amount = s.amount \
         WHEN NOT MATCHED AND s.amount > 0 THEN INSERT (id) VALUES (s.id)",
    );
    assert_eq!(spec.matched.len(), 2);
    assert_eq!(spec.matched[0].predicate_sql.as_deref(), Some("s.deleted"));
    assert!(matches!(spec.matched[0].action, MatchedAction::Delete));
    assert!(matches!(
        spec.matched[1].action,
        MatchedAction::Update { .. }
    ));
    assert_eq!(
        spec.not_matched[0].predicate_sql.as_deref(),
        Some("s.amount > 0")
    );
}

/// A subquery source is legal WITH an alias and refuses without one — an unaliased derived table
/// has no name the user's expressions could be resolving against.
#[test]
fn subquery_source_requires_an_alias() {
    let (_, spec) = lower_sql(
        "MERGE INTO ice.sales.orders AS t USING (SELECT 1 AS id) AS s ON t.id = s.id \
         WHEN MATCHED THEN DELETE",
    );
    assert_eq!(spec.source_from_sql, "(SELECT 1 AS id)");
    assert_eq!(spec.source_alias, "s");

    let err = lower_error(
        "MERGE INTO ice.sales.orders AS t USING (SELECT 1 AS id) ON t.id = 1 \
         WHEN MATCHED THEN DELETE",
    );
    assert!(err.contains("requires an alias"), "{err}");
}

/// A target that is not three-part refuses, naming the reason: this door resolves no default
/// catalog, so a short name has no meaning here.
#[test]
fn non_three_part_target_refuses() {
    let err = lower_error("MERGE INTO orders AS t USING s ON t.id = s.id WHEN MATCHED THEN DELETE");
    assert!(err.contains("three-part"), "{err}");
    assert!(err.contains("default catalog"), "{err}");
}

/// Clause/action pairings.
///
/// sqlparser enforces the two obvious ones itself (`INSERT` under `WHEN MATCHED`, `UPDATE` under
/// `WHEN NOT MATCHED`), so the lowering's arms for those are defensive rather than reachable from
/// SQL — pinned here as the parser property they actually are. The one that DOES parse and this
/// door does not implement is `WHEN NOT MATCHED BY SOURCE`, which refuses with its workaround.
#[test]
fn invalid_clause_action_pairings_refuse() {
    for sql in [
        "MERGE INTO ice.sales.orders AS t USING s ON t.id = s.id \
         WHEN MATCHED THEN INSERT (id) VALUES (s.id)",
        "MERGE INTO ice.sales.orders AS t USING s ON t.id = s.id \
         WHEN NOT MATCHED THEN UPDATE SET a = 1",
    ] {
        assert!(
            Parser::parse_sql(&GenericDialect {}, sql).is_err(),
            "the pairing is rejected at parse time: `{sql}`"
        );
    }

    let by_source = lower_error(
        "MERGE INTO ice.sales.orders AS t USING s ON t.id = s.id \
         WHEN NOT MATCHED BY SOURCE THEN DELETE",
    );
    assert!(by_source.contains("NOT MATCHED BY SOURCE"), "{by_source}");
    assert!(
        by_source.contains("separate DELETE"),
        "names the workaround: {by_source}"
    );
}

/// An UPDATE with no assignments and an INSERT with no column list are both statements that would
/// otherwise reach the executor as a silent no-op or an ambiguous positional insert.
#[test]
fn degenerate_update_and_insert_shapes_refuse() {
    let no_columns = lower_error(
        "MERGE INTO ice.sales.orders AS t USING s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT VALUES (s.id)",
    );
    assert!(no_columns.contains("explicit column list"), "{no_columns}");
}

/// The star forms are parse-level absent in this door (no sentinel machinery is duplicated) —
/// so they never reach the lowering at all, and the wrong-door sniff answers them instead.
#[test]
fn spark_star_forms_do_not_parse_in_this_door() {
    for sql in [
        "MERGE INTO ice.sales.orders AS t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET *",
        "MERGE INTO ice.sales.orders AS t USING s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *",
    ] {
        assert!(
            Parser::parse_sql(&GenericDialect {}, sql).is_err(),
            "`{sql}` must not parse — the star forms are Spark-door surface"
        );
    }
}
