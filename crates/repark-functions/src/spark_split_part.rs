//! Spark `split_part` — STRING `partNum` implicitly casts to INT (F-6c).
//!
//! DataFusion accepts an integer third argument, while Spark also casts STRING `partNum` to INT.
//! This overwrite widens coercion and keeps the upstream kernel.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};

/// ===========================================================================================
/// Spark `split_part` UDF (overwrites DataFusion's integer-only `partNum`).
/// ===========================================================================================
#[must_use]
pub fn split_part_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkSplitPart::new()))
}

#[derive(Debug)]
struct SparkSplitPart {
    signature: Signature,
}

impl SparkSplitPart {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkSplitPart {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkSplitPart {}

impl Hash for SparkSplitPart {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn is_utf8_family(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => true,
        DataType::Dictionary(_, value_type) => is_utf8_family(value_type),
        _ => false,
    }
}

impl ScalarUDFImpl for SparkSplitPart {
    crate::shim_udf_boilerplate!("split_part");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new("split_part", DataType::Utf8, nullable)))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 3 {
            return Err(DataFusionError::Plan(format!(
                "'split_part' expects (str, delimiter, partNum), got {} argument(s)",
                arg_types.len()
            )));
        }
        if !is_utf8_family(&arg_types[0]) {
            return Err(DataFusionError::Plan(format!(
                "'split_part' expects a string first argument, got {}",
                arg_types[0]
            )));
        }
        if !is_utf8_family(&arg_types[1]) {
            return Err(DataFusionError::Plan(format!(
                "'split_part' expects a string delimiter, got {}",
                arg_types[1]
            )));
        }
        let part_num = &arg_types[2];
        let part_ok =
            part_num.is_integer() || is_utf8_family(part_num) || *part_num == DataType::Null;
        if !part_ok {
            return Err(DataFusionError::Plan(format!(
                "'split_part' partNum must be an integer (Spark casts STRING), got {part_num}"
            )));
        }
        Ok(vec![DataType::Utf8, DataType::Utf8, DataType::Int64])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        reject_zero_part_num(&args)?;
        datafusion::functions::string::split_part().invoke_with_args(args)
    }
}

fn reject_zero_part_num(args: &ScalarFunctionArgs) -> Result<()> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let Some(part) = arrays.get(2) else {
        return Ok(());
    };
    let part = cast(part.as_ref(), &DataType::Int64)?;
    let part = part.as_primitive::<Int64Type>();
    for row in 0..part.len() {
        if !part.is_null(row) && part.value(row) == 0 {
            return exec_err!("split_part: the index 0 is invalid");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, StringArray};
    use datafusion::prelude::SessionContext;

    use super::*;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udf(split_part_udf().as_ref().clone());
        ctx
    }

    async fn one(ctx: &SessionContext, sql: &str) -> Option<String> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan `{sql}`: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute `{sql}`: {error}"));
        let column = batches[0].column(0);
        let strings = column
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("expected Utf8 for {sql}, got {:?}", column.data_type()));
        strings.is_valid(0).then(|| strings.value(0).to_string())
    }

    #[tokio::test]
    async fn string_part_num_casts_like_spark() {
        let ctx = ctx();
        assert_eq!(
            one(&ctx, "SELECT split_part('a.b.c', '.', '2')")
                .await
                .as_deref(),
            Some("b")
        );
        assert_eq!(
            one(&ctx, "SELECT split_part('a.b.c', '.', 2)")
                .await
                .as_deref(),
            Some("b")
        );
    }

    #[tokio::test]
    async fn part_num_zero_is_fail_loud() {
        let ctx = ctx();
        let planned = ctx
            .sql("SELECT split_part('a.b.c', '.', 0)")
            .await
            .expect("plan");
        assert!(planned.collect().await.is_err());
    }

    #[tokio::test]
    async fn dictionary_utf8_column_is_accepted() {
        use datafusion::arrow::array::{DictionaryArray, Int8Array};
        use datafusion::arrow::datatypes::{Field, Int8Type, Schema};
        use datafusion::arrow::record_batch::RecordBatch;

        let ctx = ctx();
        let values = StringArray::from(vec!["a.b.c", "x-y-z"]);
        let keys = Int8Array::from(vec![0_i8, 1]);
        let dict =
            DictionaryArray::<Int8Type>::try_new(keys, Arc::new(values)).expect("dictionary");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            dict.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(dict)]).expect("batch");
        ctx.register_batch("dict_parts", batch).expect("register");
        assert_eq!(
            one(&ctx, "SELECT split_part(s, '.', 2) FROM dict_parts")
                .await
                .as_deref(),
            Some("b")
        );
    }

    #[tokio::test]
    async fn register_all_overwrites_datafusion() {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        assert_eq!(
            one(&ctx, "SELECT split_part('a.b.c', '.', '2')")
                .await
                .as_deref(),
            Some("b")
        );
    }
}
