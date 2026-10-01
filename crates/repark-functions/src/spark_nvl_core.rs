use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::type_coercion::functions::fields_with_udf;
use datafusion::logical_expr::{Expr, ReturnFieldArgs, ScalarUDF};

pub(crate) fn core_declared(core: &ScalarUDF, arg_types: &[DataType]) -> Option<DataType> {
    let fields = arg_fields(arg_types);
    let coerced = fields_with_udf(&fields, core).ok()?;
    let none: Vec<Option<&ScalarValue>> = vec![None; coerced.len()];
    let args = ReturnFieldArgs {
        arg_fields: &coerced,
        scalar_arguments: &none,
    };
    Some(core.return_field_from_args(args).ok()?.data_type().clone())
}

pub(crate) fn core_field(
    core: &ScalarUDF,
    name: &str,
    args: &ReturnFieldArgs<'_>,
) -> Option<Arc<Field>> {
    let coerced = fields_with_udf(args.arg_fields, core).ok()?;
    let forwarded = ReturnFieldArgs {
        arg_fields: &coerced,
        scalar_arguments: args.scalar_arguments,
    };
    let field = core.return_field_from_args(forwarded).ok()?;
    Some(Arc::new(Field::new(
        name,
        field.data_type().clone(),
        field.is_nullable(),
    )))
}

pub(crate) fn nvl_delegates(first: &DataType) -> bool {
    !matches!(
        first,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

pub(crate) fn literal_types(args: &[Expr]) -> Option<Vec<DataType>> {
    args.iter()
        .map(|arg| {
            let Expr::Literal(value, _) = arg else {
                return None;
            };
            Some(value.data_type())
        })
        .collect::<Option<Vec<DataType>>>()
}

pub(crate) fn validate_core_call(core: &ScalarUDF, arg_types: &[DataType]) -> Result<()> {
    let fields = arg_fields(arg_types);
    let coerced = fields_with_udf(&fields, core)?;
    let none: Vec<Option<&ScalarValue>> = vec![None; coerced.len()];
    let args = ReturnFieldArgs {
        arg_fields: &coerced,
        scalar_arguments: &none,
    };
    core.return_field_from_args(args)?;
    Ok(())
}

fn arg_fields(arg_types: &[DataType]) -> Vec<FieldRef> {
    arg_types
        .iter()
        .enumerate()
        .map(|(index, kind)| {
            Arc::new(Field::new(
                format!("arg{index}"),
                kind.clone(),
                *kind == DataType::Null,
            )) as FieldRef
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
    use datafusion::common::ScalarValue;

    use super::*;

    fn check_declared(core: &ScalarUDF, arg_types: &[DataType], want: Option<&DataType>) {
        assert_eq!(
            core_declared(core, arg_types).as_ref(),
            want,
            "{arg_types:?}"
        );
    }

    #[test]
    fn delegation_matches_base_bind_types() {
        let nvl = datafusion::functions::core::nvl();
        let string = DataType::Utf8;
        let int = DataType::Int64;
        check_declared(&nvl, &[string.clone(), int.clone()], Some(&string));
        check_declared(&nvl, &[int.clone(), string.clone()], Some(&string));
        let stamp = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        check_declared(&nvl, &[stamp.clone(), string.clone()], Some(&string));
        check_declared(&nvl, &[stamp.clone(), stamp.clone()], Some(&string));
        let nullif = datafusion::functions::core::nullif();
        let nano_utc = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
        check_declared(&nullif, &[string.clone(), stamp.clone()], Some(&nano_utc));
        check_declared(&nullif, &[string.clone(), DataType::Boolean], None);
        let nvl2 = datafusion::functions::core::nvl2();
        check_declared(
            &nvl2,
            &[int.clone(), string.clone(), string.clone()],
            Some(&string),
        );
        let nano = DataType::Timestamp(TimeUnit::Nanosecond, None);
        check_declared(&nvl2, &[int.clone(), nano, string.clone()], None);
    }

    #[test]
    fn delegation_keeps_core_field_shape() {
        let nvl = datafusion::functions::core::nvl();
        let fields: Vec<Arc<Field>> = vec![
            Arc::new(Field::new("a", DataType::Utf8, true)),
            Arc::new(Field::new("b", DataType::Int64, false)),
        ];
        let scalars: Vec<Option<&ScalarValue>> = vec![None, None];
        let args = ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: &scalars,
        };
        let field = core_field(&nvl, "ifnull", &args).expect("core field");
        assert_eq!(field.name(), "ifnull");
        assert_eq!(field.data_type(), &DataType::Utf8);
    }

    #[test]
    fn literal_types_rejects_non_literals() {
        use datafusion::logical_expr::{col, lit};
        assert_eq!(
            literal_types(&[lit("a"), lit(1)]),
            Some(vec![DataType::Utf8, DataType::Int32])
        );
        assert_eq!(literal_types(&[col("a"), lit(1)]), None);
    }

    #[test]
    fn validate_reports_base_bind_failure() {
        let nullif = datafusion::functions::core::nullif();
        let error = validate_core_call(&nullif, &[DataType::Utf8, DataType::Boolean])
            .expect_err("mismatch must fail");
        assert_eq!(
            error.to_string(),
            "Error during planning: For function 'nullif' Utf8 and Boolean is not comparable"
        );
        assert!(validate_core_call(&nullif, &[DataType::Utf8, DataType::Utf8]).is_ok());
        let nvl = datafusion::functions::core::nvl();
        assert!(validate_core_call(&nvl, &[DataType::Utf8, DataType::Int32]).is_ok());
    }
}
