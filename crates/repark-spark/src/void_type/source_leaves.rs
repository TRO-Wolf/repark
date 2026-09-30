use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::sql::sqlparser::ast::{
    DataType as SqlDataType, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Query,
    SelectItem, SetExpr, TimezoneInfo,
};
use repark_functions::timestamp_ntz_cast::TIMESTAMP_NTZ_LITERAL_NAME;
use repark_iceberg::write::update_cast::incompatible_store_message;

use super::is_bare_null;
use super::ltz_values_store::literal_source_type;

#[must_use]
pub(crate) fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

#[must_use]
pub(crate) fn source_type_is_reliable(value: &Expr, target: &DataType) -> bool {
    let peeled = peel(value);
    if !is_branching(peeled) {
        return true;
    }
    let mut leaves = Vec::new();
    if !collect_leaves(peeled, &mut leaves) || leaves.is_empty() {
        return false;
    }
    leaves.iter().all(|leaf| {
        !is_string_type(leaf) && incompatible_store_message("", "", leaf, target).is_some()
    })
}

fn peel(value: &Expr) -> &Expr {
    let mut peeled = value;
    while let Expr::Nested(inner) = peeled {
        peeled = inner;
    }
    peeled
}

fn is_branching(value: &Expr) -> bool {
    match value {
        Expr::Case { .. } | Expr::Exists { .. } | Expr::InSubquery { .. } => true,
        Expr::Subquery(query) => !projects_one_column(query),
        Expr::Function(_) => branch_operands(value).is_some(),
        _ => false,
    }
}

fn projects_one_column(query: &Query) -> bool {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    let [item] = select.projection.as_slice() else {
        return false;
    };
    let projected = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => peel(expr),
        _ => return false,
    };
    matches!(projected, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
}

fn collect_leaves(value: &Expr, leaves: &mut Vec<DataType>) -> bool {
    let peeled = peel(value);
    if is_bare_null(peeled) {
        return true;
    }
    match peeled {
        Expr::Case {
            conditions,
            else_result,
            ..
        } => {
            conditions
                .iter()
                .all(|when| collect_leaves(&when.result, leaves))
                && else_result
                    .as_deref()
                    .is_none_or(|else_result| collect_leaves(else_result, leaves))
        }
        Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => false,
        _ => {
            if let Some(operands) = branch_operands(peeled) {
                return operands
                    .into_iter()
                    .all(|operand| collect_leaves(operand, leaves));
            }
            match leaf_type(peeled) {
                Some(data_type) => {
                    leaves.push(data_type);
                    true
                }
                None => false,
            }
        }
    }
}

fn branch_operands(value: &Expr) -> Option<Vec<&Expr>> {
    let Expr::Function(function) = value else {
        return None;
    };
    let FunctionArguments::List(list) = &function.args else {
        return None;
    };
    let mut operands = Vec::with_capacity(list.args.len());
    for argument in &list.args {
        let FunctionArg::Unnamed(FunctionArgExpr::Expr(operand)) = argument else {
            return None;
        };
        operands.push(operand);
    }
    let name = function.name.to_string();
    let named = |wanted: &str| name.eq_ignore_ascii_case(wanted);
    let count = operands.len();
    if named("coalesce") || named("greatest") || named("least") {
        (count >= 1).then_some(operands)
    } else if named("nvl") || named("ifnull") {
        (count == 2).then_some(operands)
    } else if named("nullif") {
        (count == 2).then(|| operands[..1].to_vec())
    } else if named("if") || named("nvl2") {
        (count == 3).then(|| operands[1..].to_vec())
    } else {
        None
    }
}

pub(crate) fn leaf_type(value: &Expr) -> Option<DataType> {
    match value {
        Expr::TypedString(typed) => {
            timestamp_type(&typed.data_type).or_else(|| literal_source_type(value))
        }
        Expr::Cast {
            data_type,
            array: false,
            ..
        } => timestamp_type(data_type).or_else(|| literal_source_type(value)),
        Expr::Function(function) => {
            let one_argument = matches!(
                &function.args,
                FunctionArguments::List(list) if list.args.len() == 1
            );
            (one_argument && function.name.to_string() == TIMESTAMP_NTZ_LITERAL_NAME)
                .then_some(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        _ => literal_source_type(value),
    }
}

fn timestamp_type(data_type: &SqlDataType) -> Option<DataType> {
    match data_type {
        SqlDataType::TimestampNtz(_) | SqlDataType::Timestamp(_, TimezoneInfo::WithoutTimeZone) => {
            Some(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        SqlDataType::Timestamp(_, _) => Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::ast::{SetExpr, Statement};
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use super::*;

    fn values_row(sql: &str) -> Vec<Expr> {
        let mut statements = Parser::parse_sql(&GenericDialect, sql).unwrap();
        let Statement::Insert(insert) = statements.swap_remove(0) else {
            panic!("want an INSERT statement");
        };
        let source = insert.source.unwrap();
        let SetExpr::Values(values) = source.body.as_ref() else {
            panic!("want a VALUES source");
        };
        values.rows[0].content.clone()
    }

    fn reliable(sql: &str, target: &DataType) -> Vec<bool> {
        values_row(sql)
            .iter()
            .map(|value| source_type_is_reliable(value, target))
            .collect()
    }

    #[test]
    fn plain_cells_keep_their_planned_type() {
        assert_eq!(
            reliable(
                "INSERT INTO t VALUES (TIMESTAMP '1970-01-01 00:00:01', CAST(1 AS TIMESTAMP), \
                 DATE '2020-01-01', abs(-1), '1', (TIMESTAMP '1970-01-01 00:00:01'), \
                 (SELECT tsc FROM src), (SELECT s.tsc AS c FROM src s))",
                &DataType::Int64,
            ),
            vec![true; 8]
        );
    }

    #[test]
    fn branches_of_timestamps_and_dates_are_reliable_into_numbers() {
        assert_eq!(
            reliable(
                "INSERT INTO t VALUES (CASE WHEN true THEN TIMESTAMP '1970-01-01 00:00:01' \
                 ELSE DATE '2020-01-01' END, nvl(CAST(1 AS TIMESTAMP), NULL), \
                 coalesce(__repark_timestamp_ntz__(1), CAST('x' AS TIMESTAMP_NTZ)), \
                 if(true, DATE '2020-01-01', NULL), nullif(TIMESTAMP '1970-01-01 00:00:01', 1))",
                &DataType::Int64,
            ),
            vec![true; 5]
        );
    }

    #[test]
    fn branches_with_a_string_or_storable_or_unknown_leaf_stay_silent() {
        assert_eq!(
            reliable(
                "INSERT INTO t VALUES (CASE WHEN true THEN '1' ELSE TIMESTAMP '2020-01-01 \
                 10:00:00' END, nvl('1', 2), CASE WHEN true THEN DATE '2020-01-01' ELSE 1 END, \
                 coalesce(abs(1), DATE '2020-01-01'), CASE WHEN true THEN NULL END, \
                 (SELECT 1), nvl(-NULL, DATE '2020-01-01'), (SELECT max(tsc) FROM src), \
                 nvl((SELECT tsc FROM src), NULL))",
                &DataType::Int64,
            ),
            vec![false; 9]
        );
    }

    #[test]
    fn a_timestamp_branch_into_date_stays_silent() {
        assert_eq!(
            reliable(
                "INSERT INTO t VALUES (CASE WHEN false THEN 'abc' ELSE TIMESTAMP \
                 '2020-01-01 10:00:00' END, nvl(TIMESTAMP '2020-01-01 10:00:00', NULL))",
                &DataType::Date32,
            ),
            vec![false, false]
        );
    }

    #[test]
    fn boolean_and_numeric_branches_follow_their_leaves() {
        assert_eq!(
            reliable(
                "INSERT INTO t VALUES (CASE WHEN true THEN true ELSE false END, \
                 CASE WHEN true THEN true ELSE 1 END)",
                &DataType::Int32,
            ),
            vec![true, false]
        );
        assert_eq!(
            reliable(
                "INSERT INTO t VALUES (CASE WHEN true THEN 1 ELSE 2.5 END)",
                &DataType::Boolean,
            ),
            vec![true]
        );
    }
}
