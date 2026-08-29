/// Node-kind matrix for the empty-OW cast walk.
///
/// The end-to-end pins above cover `Aggregate`, `Window`, `Join.on`, `Filter` and
/// scalar-subquery hosts. The remaining hosts (`Values`, `DISTINCT ON`, `EXISTS`/`IN`
/// subqueries) are unreachable end-to-end — the optimizer const-folds a literal
/// `CAST('x' AS INT)` and the emptiness probe fails there first — so they are pinned here, on
/// the same analyzed plan the guard inspects.
///
/// The walk is position-AGNOSTIC (`LogicalPlan::apply_expressions`): "empty" is a runtime
/// property, so a fallible cast in a predicate is exactly as dangerous as one in a projection
/// — the probe reads zero rows and evaluates neither. What separates fire from no-fire is
/// [`cast_may_fail_at_runtime`] alone. Every host below flips the answer on that axis; none
/// of these branches is dead.
use super::super::*;
use super::common::*;

#[tokio::test]
async fn unsafe_cast_walk_fires_on_fallible_casts_in_every_position() {
    let ctx = cast_walk_ctx();

    // A fallible cast in ANY position — value-producing or predicate-only.
    for (host, source) in [
        (
            "Aggregate.aggr_expr",
            "SELECT max(CAST(a AS INT)) AS id, 'z' AS name FROM src2 WHERE false GROUP BY b",
        ),
        (
            "Aggregate.group_expr",
            "SELECT CAST(a AS INT) AS id, 'z' AS name FROM src2 WHERE false \
                 GROUP BY CAST(a AS INT)",
        ),
        (
            "Window.window_expr",
            "SELECT max(CAST(a AS INT)) OVER (PARTITION BY b) AS id, b AS name \
                 FROM src2 WHERE false",
        ),
        (
            "Values.values",
            "SELECT * FROM (VALUES (CAST('x' AS INT), 'z')) AS v(id, name) WHERE false",
        ),
        (
            "DistinctOn.select_expr",
            "SELECT DISTINCT ON (b) CAST(a AS INT) AS id, b AS name FROM src2 WHERE false",
        ),
        (
            "Projection.expr",
            "SELECT CAST(a AS INT) AS id, b AS name FROM src2 WHERE false",
        ),
        (
            "Expr::ScalarSubquery",
            "SELECT (SELECT CAST(a AS INT) FROM src2 LIMIT 1) AS id, name FROM src WHERE false",
        ),
        (
            "Expr::Exists",
            "SELECT id, CAST(EXISTS (SELECT CAST(a AS INT) FROM src2) AS STRING) AS name \
                 FROM src WHERE false",
        ),
        (
            "Expr::InSubquery",
            "SELECT id, CAST(id IN (SELECT CAST(a AS INT) FROM src2) AS STRING) AS name \
                 FROM src WHERE false",
        ),
        (
            "Filter.predicate (G1-C-002: a runtime-empty source evaluates no predicate)",
            "SELECT id, name FROM src WHERE CAST(name AS INT) = 1",
        ),
        (
            "Join.on (G1-C-001: an empty join side evaluates no key)",
            "SELECT s.id, s.name FROM src s JOIN src2 j ON CAST(j.a AS INT) = s.id \
                 WHERE false",
        ),
        (
            "Join.filter (non-equi join predicate)",
            "SELECT s.id, s.name FROM src s JOIN src2 j ON s.id > CAST(j.a AS INT) \
                 WHERE false",
        ),
        (
            "Sort.expr (ORDER BY is evaluated per row, so it raises on non-empty too; the \
                 LIMIT is what stops the planner from dropping the in-subquery Sort outright)",
            "SELECT b AS id, b AS name FROM src2 WHERE false ORDER BY CAST(a AS INT) LIMIT 5",
        ),
    ] {
        assert!(
            source_has_unsafe_cast(&ctx, source).await,
            "{host}: a fallible cast in ANY position must refuse the wipe"
        );
    }

    // Not a wipe hazard — the cast cannot raise, so empty and non-empty forms agree.
    for (why, source) in [
        (
            // DF54: `id > '99'` is Utf8→Int coercion (fallible) — moved to fallible list above if needed.
            // Keep a total comparison-coercion pin: same-type Utf8 comparison inserts no cast.
            "Filter predicate: same-type comparison is total (Utf8)",
            "SELECT id, name FROM src WHERE name > 'a'",
        ),
        (
            "NULL literal cast is total (NULL in, NULL out)",
            "SELECT id, CAST(NULL AS STRING) AS name FROM src WHERE false",
        ),
        (
            "Projection: analyzer concat coercion is total",
            "SELECT id, concat(name, id) AS name FROM src WHERE false",
        ),
        (
            "Projection: user-written stringify is total",
            "SELECT id, CAST(id AS STRING) AS name FROM src WHERE false",
        ),
        (
            "TRY_CAST is total (NULL, never an error)",
            "SELECT TRY_CAST(a AS INT) AS id, b AS name FROM src2 WHERE false",
        ),
        ("no cast at all", "SELECT id, name FROM src WHERE false"),
    ] {
        assert!(
            !source_has_unsafe_cast(&ctx, source).await,
            "{why}: must not block the wipe"
        );
    }
}

#[test]
fn reject_path_escape_ident_blocks_dotdot_and_separators() {
    assert!(reject_path_escape_ident("ok_table", "table").is_ok());
    assert!(reject_path_escape_ident("..", "table").is_err());
    assert!(reject_path_escape_ident("a/b", "table").is_err());
    assert!(reject_path_escape_ident("a\\b", "namespace").is_err());
    assert!(reject_path_escape_ident("foo..bar", "catalog").is_err());
}

// === Shared identifier probes ==========================================================
/// Shared probe table (`repark_iceberg::write::idents::probes`) drives CTAS path-escape refuse.
#[test]
fn qi1_path_escape_shared_probes_refuse() {
    for &(segment, kind_tag) in repark_iceberg::write::idents::probes::PATH_ESCAPE_PROBES {
        let err = reject_path_escape_ident(segment, "table").unwrap_err();
        let text = err.to_string();
        match kind_tag {
            "traversal" => assert!(
                text.contains("path traversal") || text.contains(".."),
                "segment {segment:?}: {text}"
            ),
            "separator" => assert!(
                text.contains("path separators") || text.contains('/') || text.contains('\\'),
                "segment {segment:?}: {text}"
            ),
            other => panic!("unknown kind tag {other}"),
        }
    }
    for safe in repark_iceberg::write::idents::probes::PATH_ESCAPE_SAFE {
        assert!(
            reject_path_escape_ident(safe, "table").is_ok(),
            "safe segment {safe:?}"
        );
    }
    // Empty remains sql-compose-only.
    assert!(reject_path_escape_ident("", "table").is_err());
}

/// G3-E8 detector: the valve reads the parsed `WHERE` expression and fires on **any** subquery,
/// at any depth, in any spelling — and on nothing else.
///
/// The detection rule is "a `Query` node under the predicate", not an enumeration of
/// subquery-bearing `Expr` variants, so this pin is what proves the rule reaches the shapes an
/// enumeration would have to list one by one (and would silently miss after a sqlparser bump).
#[test]
fn g3e8_subquery_detector_fires_on_every_spelling_and_no_other() {
    use datafusion::sql::sqlparser::ast::Expr;

    let parse = |sql: &str| -> Statement {
        Parser::parse_sql(&DatabricksDialect {}, sql)
            .unwrap_or_else(|error| panic!("{sql:?} must parse: {error}"))
            .remove(0)
    };
    let selection_of = |statement: &Statement| -> Option<Expr> {
        match statement {
            Statement::Delete(delete) => delete.selection.clone(),
            Statement::Update(update) => update.selection.clone(),
            other => panic!("not DML: {other}"),
        }
    };

    // Every WHERE spelling that carries a subquery must refuse at the *expression* valve …
    // (the product hole is statement-shaped only — see `g3e8_statement_valve`.)
    for sql in [
        "DELETE FROM t WHERE id IN (SELECT id FROM k)",
        "DELETE FROM t WHERE id NOT IN (SELECT id FROM k)",
        "DELETE FROM t WHERE NOT (id IN (SELECT id FROM k))",
        "DELETE FROM t WHERE EXISTS (SELECT 1 FROM k)",
        "DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM k)",
        "DELETE FROM t WHERE id > ANY (SELECT id FROM k)",
        "DELETE FROM t WHERE id > ALL (SELECT id FROM k)",
        "DELETE FROM t WHERE id = (SELECT max(id) FROM k)",
        "DELETE FROM t WHERE id = 1 OR id IN (SELECT id FROM k)",
        "DELETE FROM t WHERE id > 1 AND id IN (SELECT id FROM k)",
        // buried under a function argument and inside a CASE — positions an Expr-variant
        // enumeration would have to walk into anyway
        "DELETE FROM t WHERE abs(id - (SELECT max(id) FROM k)) > 1",
        "DELETE FROM t WHERE CASE WHEN id IN (SELECT id FROM k) THEN true ELSE false END",
        // subquery nested inside another subquery's FROM
        "DELETE FROM t WHERE id IN (SELECT id FROM (SELECT id FROM k) AS x)",
        // UPDATE IN stays refused at the statement valve (verb is Update). The expression-level
        // helper is verb-aware and would allow the same selection on DELETE; pin it with EXISTS.
        "UPDATE t SET name = 'z' WHERE id IN (SELECT id FROM k)",
        // These spellings cover the parser-bypass family (L1 M-4): none of them is
        // safe-because-uncorrelated — the safe/unsafe boundary is per-shape.
        "DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM k)",
        "DELETE FROM t WHERE EXISTS (SELECT 1 FROM k WHERE 1 = 0)",
        "DELETE FROM t WHERE id IN (SELECT max(id) FROM k)",
    ] {
        let statement = parse(sql);
        let selection = selection_of(&statement);
        assert!(
            refuse_dml_subquery_predicate(DmlSubqueryVerb::Delete, selection.as_ref(), "t")
                .is_err(),
            "detector must fire on {sql:?}"
        );
    }

    // … and every subquery-free predicate must pass, including the shapes that *look* like a
    // subquery (`IN` over a value list, a derived-table-free EXISTS-ish name).
    for sql in [
        "DELETE FROM t WHERE id = 2",
        "DELETE FROM t WHERE id IN (1, 2, 3)",
        "DELETE FROM t WHERE id BETWEEN 2 AND 3",
        "DELETE FROM t WHERE name LIKE 'b%' OR id = 3",
        "DELETE FROM t WHERE abs(id) > 1 AND name IS NOT NULL",
        "DELETE FROM t WHERE CASE WHEN id > 1 THEN true ELSE false END",
        "UPDATE t SET name = 'z' WHERE id = 2",
        // the assignment subquery is deliberately NOT the detector's business — only `selection`
        // is passed in, and this statement's WHERE is subquery-free
        "UPDATE t SET name = (SELECT max(name) FROM k) WHERE id = 2",
    ] {
        let statement = parse(sql);
        let selection = selection_of(&statement);
        assert!(
            refuse_dml_subquery_predicate(DmlSubqueryVerb::Delete, selection.as_ref(), "t").is_ok(),
            "detector must NOT fire on {sql:?}"
        );
    }

    // No WHERE clause at all is the provider's genuine match-all — never refused.
    let statement = parse("DELETE FROM t");
    assert!(
        refuse_dml_subquery_predicate(
            DmlSubqueryVerb::Delete,
            selection_of(&statement).as_ref(),
            "t"
        )
        .is_ok(),
        "a bare DELETE FROM t must not be refused"
    );
}

/// ===========================================================================================
/// The valve's AUTHORITATIVE entry point — the statement-shaped one the passthrough calls (F-A).
///
/// [`refuse_dml_subquery_predicate_in_statement`] is what runs at the EXECUTING parse, so it owns
/// two things the expression-level function does not: which statements it applies to (only
/// `DELETE`/`UPDATE` — everything else passes through untouched, which the passthrough relies on
/// for every SELECT in the engine), and the rendered target, which it reads off the parse tree so
/// that FROM-less and quoted spellings still name a usable table.
/// ===========================================================================================
#[test]
fn g3e8_statement_valve_covers_both_verbs_and_renders_the_parsed_target() {
    let parse = |sql: &str| -> Statement {
        Parser::parse_sql(&DatabricksDialect {}, sql)
            .unwrap_or_else(|error| panic!("{sql:?} must parse: {error}"))
            .remove(0)
    };

    // Fires, and names the parsed target — including the FROM-less spelling the router's own
    // parse rejects and the quoted form the text scan cannot read.
    for (sql, expected_target, verb) in [
        (
            "DELETE FROM ice.sales.t WHERE id = (SELECT max(id) FROM k)",
            "ice.sales.t",
            "DELETE",
        ),
        (
            "UPDATE ice.sales.t SET name = 'z' WHERE id = (SELECT max(id) FROM k)",
            "ice.sales.t",
            "UPDATE",
        ),
        (
            "DELETE FROM \"ice\".\"sales\".\"t\" WHERE id = (SELECT max(id) FROM k)",
            "\"ice\".\"sales\".\"t\"",
            "DELETE",
        ),
        (
            "DELETE FROM ice.sales.t WHERE id IN (SELECT id FROM (SELECT id FROM k) x)",
            "ice.sales.t",
            "DELETE",
        ),
    ] {
        let refusal = refuse_dml_subquery_predicate_in_statement(&parse(sql))
            .expect_err("the statement valve must fire")
            .to_string();
        assert!(
            refusal.contains(&format!("{verb} with a subquery")),
            "sql={sql:?}, got {refusal}"
        );
        assert!(
            refusal.contains(&format!("is refused on `{expected_target}`")),
            "the target must come from the parse tree, sql={sql:?}, got {refusal}"
        );
    }

    // Passes everything it must not gate: subquery-free DML, and every non-DML statement (a
    // SELECT with a subquery is not this valve's business — the passthrough plans it).
    for sql in [
        "DELETE FROM ice.sales.t WHERE id = 2",
        "DELETE FROM ice.sales.t WHERE id IN (SELECT id FROM k)",
        "DELETE FROM ice.sales.t WHERE id NOT IN (SELECT id FROM k)",
        "DELETE FROM ice.sales.t WHERE EXISTS (SELECT 1 FROM k)",
        "DELETE FROM ice.sales.t WHERE NOT EXISTS (SELECT 1 FROM k)",
        "DELETE FROM ice.sales.t WHERE EXISTS (SELECT 1 FROM k x WHERE x.id = ice.sales.t.id)",
        "UPDATE ice.sales.t SET name = 'z' WHERE id IN (1, 2)",
        "DELETE FROM ice.sales.t",
        "SELECT id FROM t WHERE id IN (SELECT id FROM k)",
        "INSERT INTO t SELECT id FROM k WHERE id IN (SELECT id FROM k2)",
        "MERGE INTO t USING (SELECT id FROM k) s ON t.id = s.id WHEN MATCHED THEN DELETE",
    ] {
        refuse_dml_subquery_predicate_in_statement(&parse(sql))
            .unwrap_or_else(|err| panic!("the statement valve must NOT fire on {sql:?}: {err}"));
    }
}
