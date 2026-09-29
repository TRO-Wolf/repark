use datafusion::sql::sqlparser::ast::{CastKind, DataType, Expr, Query, SetExpr, TimezoneInfo};

use super::null_expr;

pub(super) fn mark_values_timestamp_casts(query: &mut Query) {
    mark_set_expr(query.body.as_mut());
}

pub(super) fn has_values_timestamp_cast(query: &Query) -> bool {
    set_expr_has_cast(query.body.as_ref())
}

fn mark_set_expr(body: &mut SetExpr) {
    match body {
        SetExpr::Values(values) => {
            for row in &mut values.rows {
                for cell in &mut row.content {
                    mark_cell(cell);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            mark_set_expr(left);
            mark_set_expr(right);
        }
        _ => {}
    }
}

fn set_expr_has_cast(body: &SetExpr) -> bool {
    match body {
        SetExpr::Values(values) => values
            .rows
            .iter()
            .any(|row| row.content.iter().any(|cell| needs_mark(peel(cell)))),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_has_cast(left) || set_expr_has_cast(right)
        }
        _ => false,
    }
}

fn peel(mut cell: &Expr) -> &Expr {
    while let Expr::Nested(inner) = cell {
        cell = inner;
    }
    cell
}

fn mark_cell(cell: &mut Expr) {
    if let Expr::Nested(inner) = cell {
        mark_cell(inner);
        return;
    }
    if !needs_mark(cell) {
        return;
    }
    let written = std::mem::replace(cell, null_expr());
    *cell = Expr::Cast {
        kind: CastKind::Cast,
        expr: Box::new(written),
        data_type: DataType::Timestamp(None, TimezoneInfo::None),
        array: false,
        format: None,
    };
}

fn needs_mark(cell: &Expr) -> bool {
    let Expr::Cast { expr, .. } = cell else {
        return false;
    };
    is_written_timestamp_cast(cell) && !is_written_timestamp_cast(peel(expr))
}

fn is_written_timestamp_cast(expr: &Expr) -> bool {
    let Expr::Cast {
        kind: CastKind::Cast | CastKind::DoubleColon,
        data_type,
        array: false,
        format: None,
        ..
    } = expr
    else {
        return false;
    };
    matches!(data_type, DataType::Timestamp(None, TimezoneInfo::None))
        || data_type.to_string().eq_ignore_ascii_case("timestamp_ltz")
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::ast::Statement;
    use datafusion::sql::sqlparser::dialect::DatabricksDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use crate::keyword_lower::{has_empty_map_or_timestamp_ns_cast, lower_spark_keywords};

    fn parse(sql: &str) -> Statement {
        Parser::parse_sql(&DatabricksDialect {}, sql)
            .unwrap()
            .remove(0)
    }

    fn lowered(sql: &str) -> String {
        let mut statement = parse(sql);
        lower_spark_keywords(&mut statement);
        statement.to_string()
    }

    #[test]
    fn a_written_timestamp_cast_in_values_is_marked_once() {
        let text = lowered(
            "SELECT * FROM VALUES (1, DATE '2024-03-10'), (2, CAST(d AS TIMESTAMP)), (3, \
             (x::timestamp)) AS s(id, v)",
        );
        assert!(
            text.contains("CAST(CAST(d AS TIMESTAMP) AS TIMESTAMP)")
                && text.contains("CAST(x::TIMESTAMP AS TIMESTAMP)")
                && text.contains("DATE '2024-03-10'"),
            "{text}"
        );
        let mut twice = parse("INSERT INTO t VALUES (1, CAST(d AS TIMESTAMP_LTZ))");
        lower_spark_keywords(&mut twice);
        lower_spark_keywords(&mut twice);
        assert!(
            twice
                .to_string()
                .contains("VALUES (1, CAST(CAST(d AS TIMESTAMP) AS TIMESTAMP))"),
            "{twice}"
        );
    }

    #[test]
    fn casts_outside_values_and_other_targets_are_untouched() {
        for sql in [
            "SELECT CAST(d AS TIMESTAMP) AS v",
            "SELECT * FROM VALUES (1, CAST(d AS DATE)), (2, TRY_CAST(d AS TIMESTAMP)) AS s(id, v)",
            "SELECT * FROM VALUES (1, CAST(d AS TIMESTAMP) + INTERVAL '1' DAY) AS s(id, v)",
        ] {
            assert_eq!(lowered(sql), parse(sql).to_string(), "{sql}");
        }
    }

    #[test]
    fn the_merge_probe_finds_a_written_timestamp_cast_in_values() {
        assert!(has_empty_map_or_timestamp_ns_cast(&parse(
            "MERGE INTO t USING (SELECT * FROM VALUES (1, CAST(d AS TIMESTAMP)) AS s(id, v)) s ON \
             t.id = s.id WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
        )));
        assert!(!has_empty_map_or_timestamp_ns_cast(&parse(
            "MERGE INTO t USING (SELECT * FROM VALUES (1, DATE '2024-03-10') AS s(id, v)) s ON \
             t.id = s.id WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
        )));
    }
}
