use datafusion::arrow::datatypes::{DataType, Fields};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Query;
use iceberg::spec::Type;
use repark_core::CatalogRegistry;

pub(super) async fn refuse_nested_mismatch(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &iceberg::table::Table,
    source: &Query,
    mapping: &[Option<usize>],
    table_display: &str,
    case_sensitive: bool,
) -> Result<()> {
    let schema = table.metadata().current_schema();
    let targets = schema.as_struct().fields();
    if !targets
        .iter()
        .any(|field| matches!(*field.field_type, Type::Struct(_)))
    {
        return Ok(());
    }
    let probe_sql = format!("SELECT * FROM ({source}) AS _repark_by_name_src LIMIT 0");
    let frame = crate::spark_ast::execute_passthrough(ctx, catalogs, &probe_sql).await?;
    let planned = frame.schema().as_arrow().fields().clone();
    check_columns(targets, mapping, &planned, table_display, case_sensitive)
}

pub(super) fn check_columns(
    targets: &[iceberg::spec::NestedFieldRef],
    mapping: &[Option<usize>],
    planned: &Fields,
    table_display: &str,
    case_sensitive: bool,
) -> Result<()> {
    for (target, slot) in targets.iter().zip(mapping) {
        let Some(source) = slot.and_then(|index| planned.get(index)) else {
            continue;
        };
        let path = vec![target.name.clone()];
        check_type(
            &target.field_type,
            source.data_type(),
            &path,
            table_display,
            case_sensitive,
        )?;
    }
    Ok(())
}

fn check_type(
    target: &Type,
    source: &DataType,
    path: &[String],
    table_display: &str,
    case_sensitive: bool,
) -> Result<()> {
    let (Type::Struct(expected), DataType::Struct(input)) = (target, source) else {
        return Ok(());
    };
    let mut matched = vec![false; input.len()];
    for field in expected.fields() {
        let mut child = path.to_vec();
        child.push(field.name.clone());
        let found = input
            .iter()
            .position(|candidate| super::same_name(candidate.name(), &field.name, case_sensitive));
        let Some(index) = found else {
            return Err(cannot_find_data(table_display, &child));
        };
        if let Some(flag) = matched.get_mut(index) {
            *flag = true;
        }
        if let Some(candidate) = input.get(index) {
            check_type(
                &field.field_type,
                candidate.data_type(),
                &child,
                table_display,
                case_sensitive,
            )?;
        }
    }
    let extra: Vec<String> = input
        .iter()
        .zip(&matched)
        .filter(|(_, hit)| !**hit)
        .map(|(field, _)| super::quote_name(field.name()))
        .collect();
    if extra.is_empty() {
        return Ok(());
    }
    Err(DataFusionError::Plan(format!(
        "[INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS] Cannot write incompatible data for \
         the table {table_display}: Cannot write extra fields {} to the struct {}. SQLSTATE: KD000",
        extra.join(", "),
        dotted(path)
    )))
}

fn dotted(path: &[String]) -> String {
    path.iter()
        .map(|part| super::quote_name(part))
        .collect::<Vec<_>>()
        .join(".")
}

fn cannot_find_data(table_display: &str, path: &[String]) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write incompatible data for the \
         table {table_display}: Cannot find data for the output column {}. SQLSTATE: KD000",
        dotted(path)
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Fields};
    use iceberg::spec::{NestedField, NestedFieldRef, PrimitiveType, StructType, Type};

    use super::check_columns;

    const TABLE: &str = "`sc`.`u7`.`t`";

    fn targets() -> Vec<NestedFieldRef> {
        let inner = StructType::new(vec![
            Arc::new(NestedField::optional(
                3,
                "a",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                4,
                "b",
                Type::Primitive(PrimitiveType::String),
            )),
        ]);
        vec![
            Arc::new(NestedField::optional(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(2, "s", Type::Struct(inner))),
        ]
    }

    fn frame(sub: Vec<Field>) -> Fields {
        Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("s", DataType::Struct(Fields::from(sub)), true),
        ])
    }

    fn run(sub: Vec<Field>, case_sensitive: bool) -> Result<(), String> {
        check_columns(
            &targets(),
            &[Some(0), Some(1)],
            &frame(sub),
            TABLE,
            case_sensitive,
        )
        .map_err(|error| error.to_string())
    }

    #[test]
    fn a_missing_sub_field_refuses_cannot_find_data_with_the_dotted_path() {
        let error = run(vec![Field::new("a", DataType::Int32, true)], false).expect_err("refuses");
        assert_eq!(
            error,
            "Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write \
             incompatible data for the table `sc`.`u7`.`t`: Cannot find data for the output \
             column `s`.`b`. SQLSTATE: KD000"
        );
    }

    #[test]
    fn an_extra_sub_field_refuses_extra_struct_fields() {
        let error = run(
            vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
                Field::new("c", DataType::Int32, true),
            ],
            false,
        )
        .expect_err("refuses");
        assert_eq!(
            error,
            "Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS] Cannot \
             write incompatible data for the table `sc`.`u7`.`t`: Cannot write extra fields `c` \
             to the struct `s`. SQLSTATE: KD000"
        );
    }

    #[test]
    fn reordered_and_case_folded_sub_fields_pass() {
        run(
            vec![
                Field::new("b", DataType::Utf8, true),
                Field::new("a", DataType::Int32, true),
            ],
            false,
        )
        .expect("reordered binds");
        run(
            vec![
                Field::new("A", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ],
            false,
        )
        .expect("case-insensitive binds");
        let error = run(
            vec![
                Field::new("A", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ],
            true,
        )
        .expect_err("case-sensitive refuses");
        assert!(error.contains("`s`.`a`"), "{error}");
    }
}
