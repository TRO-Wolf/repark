use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, Result};
use datafusion::logical_expr::Expr;

pub(super) fn bind_case_insensitive(expr: Expr, frame_schema: &DFSchema) -> Result<Expr> {
    expr.transform(|node| {
        let bound = match &node {
            Expr::Column(column) => unique_case_match(column, frame_schema),
            _ => None,
        };
        Ok(match bound {
            Some(column) => Transformed::yes(Expr::Column(column)),
            None => Transformed::no(node),
        })
    })
    .map(|transformed| transformed.data)
}

fn unique_case_match(column: &Column, frame_schema: &DFSchema) -> Option<Column> {
    if column.relation.is_some() || frame_schema.has_column_with_unqualified_name(&column.name) {
        return None;
    }
    let mut hits = frame_schema
        .iter()
        .filter(|(_, field)| field.name().eq_ignore_ascii_case(&column.name))
        .map(|(qualifier, field)| Column::new(qualifier.cloned(), field.name()));
    let first = hits.next()?;
    if hits.any(|other| other != first) {
        return None;
    }
    Some(first)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::{Column, DFSchema};
    use datafusion::logical_expr::{Expr, col, lit};

    use super::bind_case_insensitive;

    fn frame(names: &[(&str, &str)]) -> DFSchema {
        let fields = names
            .iter()
            .map(|(qualifier, name)| {
                (
                    Some((*qualifier).into()),
                    Arc::new(Field::new(*name, DataType::Int64, true)),
                )
            })
            .collect::<Vec<_>>();
        DFSchema::new_with_metadata(fields, std::collections::HashMap::new()).unwrap()
    }

    #[test]
    fn folded_reference_binds_the_single_spelled_field() {
        let schema = frame(&[("t", "ID"), ("t", "data")]);
        let bound = bind_case_insensitive(col("ID").gt(lit(1i64)), &schema).unwrap();
        assert_eq!(
            bound,
            Expr::Column(Column::new(Some("t"), "ID")).gt(lit(1i64))
        );
    }

    #[test]
    fn exact_ambiguous_and_qualified_references_stay() {
        let schema = frame(&[("t", "id"), ("u", "ID")]);
        assert_eq!(
            bind_case_insensitive(col("id"), &schema).unwrap(),
            col("id")
        );
        let twins = frame(&[("t", "Id"), ("u", "ID")]);
        assert_eq!(bind_case_insensitive(col("id"), &twins).unwrap(), col("id"));
        let qualified = Expr::Column(Column::new(Some("t"), "id"));
        let single = frame(&[("t", "ID")]);
        assert_eq!(
            bind_case_insensitive(qualified.clone(), &single).unwrap(),
            qualified
        );
    }
}
