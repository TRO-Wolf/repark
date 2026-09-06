use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::Int32Array;
use datafusion::arrow::array::{Array, ArrayRef, MapArray, StructArray, new_empty_array};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::{concat, take};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

#[must_use]
pub fn create_map_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkCreateMap::new()))
}

#[derive(Debug)]
struct SparkCreateMap {
    signature: Signature,
}

impl SparkCreateMap {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkCreateMap {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkCreateMap {}

impl Hash for SparkCreateMap {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn widen(left: &DataType, right: &DataType) -> Result<DataType> {
    if left == right {
        return Ok(left.clone());
    }
    comparison_coercion(left, right).ok_or_else(|| {
        datafusion::error::DataFusionError::Plan(format!(
            "'create_map' cannot reconcile the argument types {left} and {right}"
        ))
    })
}

fn key_value_types(arg_types: &[DataType]) -> Result<(DataType, DataType)> {
    if !arg_types.len().is_multiple_of(2) {
        return exec_err!(
            "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] `create_map` requires an even number of \
             arguments, got {}",
            arg_types.len()
        );
    }
    let mut key = DataType::Utf8;
    let mut value = DataType::Utf8;
    let mut seen = false;
    for pair in arg_types.chunks(2) {
        if seen {
            key = widen(&key, &pair[0])?;
            value = widen(&value, &pair[1])?;
        } else {
            key = pair[0].clone();
            value = pair[1].clone();
            seen = true;
        }
    }
    Ok((key, value))
}

fn map_type(key: DataType, value: DataType) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", key, false),
                Field::new("values", value, true),
            ])),
            false,
        )),
        false,
    )
}

fn null_key(key: &ScalarValue) -> datafusion::error::DataFusionError {
    let _ = key;
    datafusion::error::DataFusionError::Execution(
        "[NULL_MAP_KEY] Cannot use null as map key. SQLSTATE: 2200E".to_string(),
    )
}

fn duplicate_key(key: &ScalarValue) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(format!(
        "Duplicate map key '{key}' was found, please check the input data. If you want to \
         remove the duplicated keys, you can set spark.sql.mapKeyDedupPolicy to \"LAST_WIN\" \
         so that the key inserted at last takes precedence."
    ))
}

fn row_slice(array: &ArrayRef, row: usize) -> Result<ArrayRef> {
    let indices = Int32Array::from(vec![i32::try_from(row).unwrap_or(0)]);
    Ok(take(array.as_ref(), &indices, None)?)
}

impl ScalarUDFImpl for SparkCreateMap {
    crate::shim_udf_boilerplate!("create_map");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let (key, value) = key_value_types(arg_types)?;
        Ok(map_type(key, value))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&types)?,
            false,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let (key, value) = key_value_types(arg_types)?;
        let mut coerced = Vec::with_capacity(arg_types.len());
        for _ in 0..arg_types.len() / 2 {
            coerced.push(key.clone());
            coerced.push(value.clone());
        }
        Ok(coerced)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let (key_type, value_type) = key_value_types(
            &args
                .arg_fields
                .iter()
                .map(|field| field.data_type().clone())
                .collect::<Vec<_>>(),
        )?;
        let entries_field = match map_type(key_type.clone(), value_type.clone()) {
            DataType::Map(field, _) => field,
            other => return exec_err!("'create_map' could not build its MAP type, got {other}"),
        };
        let DataType::Struct(pair) = entries_field.data_type() else {
            return exec_err!("'create_map' could not build its MAP entries");
        };
        let rows = args.number_rows;
        if args.args.is_empty() {
            let structs = StructArray::try_new(
                pair.clone(),
                vec![new_empty_array(&key_type), new_empty_array(&value_type)],
                None,
            )?;
            return Ok(ColumnarValue::Array(Arc::new(MapArray::try_new(
                Arc::clone(&entries_field),
                OffsetBuffer::new(vec![0_i32; rows + 1].into()),
                structs,
                None,
                false,
            )?)));
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let pairs = arrays.len() / 2;
        let mut keys: Vec<ArrayRef> = Vec::with_capacity(rows * pairs);
        let mut values: Vec<ArrayRef> = Vec::with_capacity(rows * pairs);
        let mut offsets: Vec<i32> = Vec::with_capacity(rows + 1);
        offsets.push(0);
        let mut length = 0_i32;
        for row in 0..rows {
            let mut seen: HashSet<ScalarValue> = HashSet::with_capacity(pairs);
            for index in 0..pairs {
                let key_array = &arrays[index * 2];
                if key_array.is_null(row) {
                    return Err(null_key(&ScalarValue::try_from_array(key_array, row)?));
                }
                let key = ScalarValue::try_from_array(key_array, row)?;
                if !seen.insert(key.clone()) {
                    return Err(duplicate_key(&key));
                }
                keys.push(row_slice(key_array, row)?);
                values.push(row_slice(&arrays[index * 2 + 1], row)?);
            }
            length += i32::try_from(pairs).unwrap_or(0);
            offsets.push(length);
        }
        let key_refs: Vec<&dyn Array> = keys.iter().map(AsRef::as_ref).collect();
        let value_refs: Vec<&dyn Array> = values.iter().map(AsRef::as_ref).collect();
        let key_column = if key_refs.is_empty() {
            new_empty_array(&key_type)
        } else {
            concat(&key_refs)?
        };
        let value_column = if value_refs.is_empty() {
            new_empty_array(&value_type)
        } else {
            concat(&value_refs)?
        };
        let structs = StructArray::try_new(pair.clone(), vec![key_column, value_column], None)?;
        Ok(ColumnarValue::Array(Arc::new(MapArray::try_new(
            entries_field,
            OffsetBuffer::new(offsets.into()),
            structs,
            None,
            false,
        )?)))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::RecordBatch;
    use datafusion::common::ScalarValue;
    use datafusion::prelude::SessionContext;

    fn run(sql: &str) -> datafusion::common::Result<Vec<RecordBatch>> {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        ctx.register_udf(super::create_map_udf().as_ref().clone());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async { ctx.sql(sql).await?.collect().await })
    }

    fn shown(sql: &str) -> String {
        let batches = run(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        ScalarValue::try_from_array(batches[0].column(0), 0)
            .expect("scalar")
            .to_string()
    }

    #[test]
    fn create_map_builds_a_non_null_map_from_alternating_arguments() {
        let batches = run("SELECT create_map('a', 1, 'b', 2)").expect("create_map");
        assert_eq!(shown("SELECT create_map('a', 1, 'b', 2)"), "[{a:1,b:2}]");
        assert!(!batches[0].schema().field(0).is_nullable());
        assert_eq!(shown("SELECT create_map()"), "[{}]");
    }

    #[test]
    fn create_map_refuses_a_null_key_and_a_duplicate_key() {
        let null_key =
            run("SELECT create_map(CAST(NULL AS STRING), 1)").expect_err("a null key must raise");
        assert!(null_key.to_string().contains("NULL_MAP_KEY"), "{null_key}");
        let duplicate =
            run("SELECT create_map('a', 1, 'a', 2)").expect_err("a duplicate key must raise");
        assert!(
            duplicate.to_string().contains("Duplicate map key"),
            "{duplicate}"
        );
    }
}
