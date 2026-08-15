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

/// M2 / r5 — Oracle-style `UPDATE SET … WHERE` is destructured and refused, not dropped.
#[test]
fn oracle_style_update_where_predicate_refuses() {
    let err = lower_error(
        "MERGE INTO ice.sales.orders AS t USING staging AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET amount = s.amount WHERE s.amount > 0",
    );
    assert!(
        err.contains("UPDATE SET … WHERE"),
        "must name the UPDATE WHERE construct: {err}"
    );
    assert!(
        err.contains("is not Spark MERGE grammar"),
        "must name the Spark form: {err}"
    );
    assert!(
        err.contains("WHEN MATCHED AND <cond>"),
        "must name the Spark rewrite: {err}"
    );
}

/// M2 — Oracle-style `UPDATE SET … DELETE WHERE` is destructured and refused.
#[test]
fn oracle_style_delete_where_predicate_refuses() {
    let err = lower_error(
        "MERGE INTO ice.sales.orders AS t USING staging AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET amount = s.amount DELETE WHERE s.amount > 0",
    );
    assert!(
        err.contains("DELETE WHERE"),
        "must name the DELETE WHERE construct: {err}"
    );
    assert!(
        err.contains("is not Spark MERGE grammar"),
        "must name the Spark form: {err}"
    );
}

/// M2 — Oracle-style `INSERT … WHERE` is destructured and refused.
#[test]
fn oracle_style_insert_where_predicate_refuses() {
    let err = lower_error(
        "MERGE INTO ice.sales.orders AS t USING staging AS s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount) WHERE s.amount > 0",
    );
    assert!(
        err.contains("INSERT … WHERE"),
        "must name the INSERT WHERE construct: {err}"
    );
    assert!(
        err.contains("is not Spark MERGE grammar"),
        "must name the Spark form: {err}"
    );
}

/// M3 / r7 — a source-qualified SET target is refused, naming the qualifier and target alias.
#[test]
fn source_qualified_set_target_refuses() {
    let err = lower_error(
        "MERGE INTO ice.sales.orders AS t USING staging AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET s.amount = 0",
    );
    assert!(
        err.contains("`s`"),
        "must name the received qualifier: {err}"
    );
    assert!(
        err.contains("target alias `t`"),
        "must name the target alias: {err}"
    );
}

/// M3 — three-or-more-part SET targets refuse as nested-field assignment.
#[test]
fn nested_field_set_target_refuses() {
    let err = lower_error(
        "MERGE INTO ice.sales.orders AS t USING staging AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.addr.city = s.amount",
    );
    assert!(
        err.contains("nested-field assignment is not supported"),
        "{err}"
    );
}

/// M3 positive — `t.amount` and bare `amount` both lower to the target column.
#[test]
fn target_qualified_and_bare_set_targets_lower() {
    let (_, qualified) = lower_sql(
        "MERGE INTO ice.sales.orders AS t USING staging AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET t.amount = s.amount",
    );
    let MatchedAction::Update { assignments } = &qualified.matched[0].action else {
        panic!("expected an UPDATE action");
    };
    assert_eq!(
        assignments,
        &vec![("amount".to_string(), "s.amount".to_string())]
    );

    let (_, bare) = lower_sql(
        "MERGE INTO ice.sales.orders AS t USING staging AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET amount = s.amount",
    );
    let MatchedAction::Update { assignments } = &bare.matched[0].action else {
        panic!("expected an UPDATE action");
    };
    assert_eq!(
        assignments,
        &vec![("amount".to_string(), "s.amount".to_string())]
    );

    let (_, folded) = lower_sql(
        "MERGE INTO ice.sales.orders AS t USING staging AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET T.amount = s.amount",
    );
    let MatchedAction::Update { assignments } = &folded.matched[0].action else {
        panic!("expected an UPDATE action");
    };
    assert_eq!(assignments[0].0, "amount");
}

/// M3 — quoted target alias + `"Tgt".amount` still resolves (unquote before compare).
#[test]
fn quoted_target_alias_set_target_lowers() {
    let (_, spec) = lower_sql(
        "MERGE INTO ice.sales.orders AS \"Tgt\" USING staging AS s ON \"Tgt\".id = s.id \
         WHEN MATCHED THEN UPDATE SET \"Tgt\".amount = s.amount",
    );
    assert_eq!(spec.target_alias, "\"Tgt\"");
    let MatchedAction::Update { assignments } = &spec.matched[0].action else {
        panic!("expected an UPDATE action");
    };
    assert_eq!(
        assignments,
        &vec![("amount".to_string(), "s.amount".to_string())]
    );
}

/// M3 — source-qualified INSERT columns refuse, naming qualifier and target alias.
#[test]
fn source_qualified_insert_column_refuses() {
    let err = lower_error(
        "MERGE INTO ice.sales.orders AS t USING staging AS s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (s.id, s.amount) VALUES (s.id, s.amount)",
    );
    assert!(
        err.contains("`s`"),
        "must name the received qualifier: {err}"
    );
    assert!(
        err.contains("target alias `t`"),
        "must name the target alias: {err}"
    );
}

/// M3 — three-or-more-part INSERT columns refuse as nested-field assignment.
#[test]
fn nested_field_insert_column_refuses() {
    let err = lower_error(
        "MERGE INTO ice.sales.orders AS t USING staging AS s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (t.addr.city) VALUES (s.amount)",
    );
    assert!(
        err.contains("nested-field assignment is not supported"),
        "{err}"
    );
}

/// M3 positive — target-qualified INSERT columns strip to the column names.
#[test]
fn target_qualified_insert_columns_lower() {
    let (_, spec) = lower_sql(
        "MERGE INTO ice.sales.orders AS t USING staging AS s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (t.id, t.amount) VALUES (s.id, s.amount)",
    );
    let InsertAction::Explicit { columns, .. } = &spec.not_matched[0].action else {
        panic!("expected an explicit INSERT action");
    };
    assert_eq!(columns, &vec!["id".to_string(), "amount".to_string()]);
}

/// M10 / r12 — an unconditioned MATCHED clause before another MATCHED clause refuses.
#[test]
fn non_last_unconditional_matched_clause_refuses() {
    let err = lower_error(
        "MERGE INTO ice.sales.orders AS t USING staging AS s ON t.id = s.id \
         WHEN MATCHED THEN DELETE \
         WHEN MATCHED AND s.amount > 0 THEN UPDATE SET amount = s.amount",
    );
    assert!(
        err.contains("NON_LAST_MATCHED_CLAUSE_OMIT_CONDITION"),
        "{err}"
    );
}

/// M10 — an unconditioned NOT MATCHED clause before another NOT MATCHED clause refuses.
#[test]
fn non_last_unconditional_not_matched_clause_refuses() {
    let err = lower_error(
        "MERGE INTO ice.sales.orders AS t USING staging AS s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount) \
         WHEN NOT MATCHED AND s.amount > 0 THEN INSERT (id) VALUES (s.id)",
    );
    assert!(
        err.contains("NON_LAST_NOT_MATCHED_CLAUSE_OMIT_CONDITION"),
        "{err}"
    );
}

/// M10 positive — an unconditioned LAST clause of its kind still lowers (first-match-wins).
#[test]
fn unconditional_last_matched_clause_still_lowers() {
    let (_, spec) = lower_sql(
        "MERGE INTO ice.sales.orders AS t USING staging AS s ON t.id = s.id \
         WHEN MATCHED AND s.deleted THEN DELETE \
         WHEN MATCHED THEN UPDATE SET amount = s.amount",
    );
    assert_eq!(spec.matched.len(), 2);
    assert!(spec.matched[0].predicate_sql.is_some());
    assert!(spec.matched[1].predicate_sql.is_none());
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
