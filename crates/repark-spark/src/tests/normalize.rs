/// P5C1-Q-001 (audit G1) — the node-kind matrix for the empty-OW cast walk.
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

// === r23 QI1: idents ===
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
