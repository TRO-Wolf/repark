use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, MapArray, StructArray, new_empty_array};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute::interleave;
use datafusion::arrow::compute::kernels::cmp::eq;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

pub const SQL_MAP_NAME: &str = "__repark_create_map__";
const DATAFRAME_MAP_NAME: &str = "create_map";

#[must_use]
pub fn create_map_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkCreateMap::new(DATAFRAME_MAP_NAME)))
}

/// The same kernel under an internal name: the Spark SQL door lowers `map(...)` onto it.
#[must_use]
pub fn sql_map_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkCreateMap::new(SQL_MAP_NAME)))
}

#[derive(Debug)]
struct SparkCreateMap {
    signature: Signature,
    name: &'static str,
}

impl SparkCreateMap {
    fn new(name: &'static str) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            name,
        }
    }
}

impl PartialEq for SparkCreateMap {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
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

fn map_type(key: DataType, value: DataType, value_contains_null: bool) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", key, false),
                Field::new("values", value, value_contains_null),
            ])),
            false,
        )),
        false,
    )
}

fn value_contains_null(arg_fields: &[FieldRef]) -> bool {
    arg_fields
        .iter()
        .skip(1)
        .step_by(2)
        .any(|field| field.is_nullable())
}

fn null_key() -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(
        "[NULL_MAP_KEY] Cannot use null as map key. SQLSTATE: 2200E".to_string(),
    )
}

fn duplicate_key(key: &ScalarValue) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(format!(
        "[DUPLICATED_MAP_KEY] Duplicate map key {key} was found, please check the input data.\n\
         If you want to remove the duplicated keys, you can set \"spark.sql.mapKeyDedupPolicy\" \
         to \"LAST_WIN\" so that the key inserted at last takes precedence. SQLSTATE: 23505"
    ))
}

fn first_null_key(keys: &[&ArrayRef]) -> Option<(usize, usize)> {
    keys.iter()
        .enumerate()
        .filter_map(|(index, key)| {
            let nulls = key.logical_nulls()?;
            let row = nulls.iter().position(|valid| !valid)?;
            Some((row, index))
        })
        .min()
}

fn first_equal_row(left: &ArrayRef, right: &ArrayRef) -> Result<Option<usize>> {
    if !left.data_type().is_floating()
        && let Ok(equal) = eq(left, right)
    {
        return Ok(equal.iter().position(|flag| flag == Some(true)));
    }
    for row in 0..left.len() {
        if ScalarValue::try_from_array(left, row)? == ScalarValue::try_from_array(right, row)? {
            return Ok(Some(row));
        }
    }
    Ok(None)
}

fn first_duplicate_key(keys: &[&ArrayRef]) -> Result<Option<(usize, usize, usize)>> {
    let mut first: Option<(usize, usize, usize)> = None;
    for later in 1..keys.len() {
        for earlier in 0..later {
            let Some(row) = first_equal_row(keys[earlier], keys[later])? else {
                continue;
            };
            if first.is_none_or(|(best_row, best_later, _)| (row, later) < (best_row, best_later)) {
                first = Some((row, later, earlier));
            }
        }
    }
    Ok(first)
}

fn refuse_bad_keys(keys: &[&ArrayRef]) -> Result<()> {
    let null = first_null_key(keys);
    let duplicate = first_duplicate_key(keys)?;
    match (null, duplicate) {
        (Some((null_row, null_index)), Some((row, later, _)))
            if (row, later) < (null_row, null_index) =>
        {
            Err(duplicate_key(&ScalarValue::try_from_array(
                keys[later],
                row,
            )?))
        }
        (Some(_), _) => Err(null_key()),
        (None, Some((row, later, _))) => Err(duplicate_key(&ScalarValue::try_from_array(
            keys[later],
            row,
        )?)),
        (None, None) => Ok(()),
    }
}

fn flatten(columns: &[&ArrayRef], rows: usize) -> Result<ArrayRef> {
    if let [only] = columns {
        return Ok(Arc::clone(only));
    }
    let sources: Vec<&dyn Array> = columns.iter().map(AsRef::as_ref).collect();
    let indices: Vec<(usize, usize)> = (0..rows)
        .flat_map(|row| (0..columns.len()).map(move |column| (column, row)))
        .collect();
    Ok(interleave(&sources, &indices)?)
}

impl ScalarUDFImpl for SparkCreateMap {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let (key, value) = key_value_types(arg_types)?;
        Ok(map_type(key, value, true))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let (key, value) = key_value_types(&types)?;
        Ok(Arc::new(Field::new(
            self.name(),
            map_type(key, value, value_contains_null(args.arg_fields)),
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
        let entries_field = match map_type(
            key_type.clone(),
            value_type.clone(),
            value_contains_null(&args.arg_fields),
        ) {
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
        let arrays = args
            .args
            .iter()
            .map(|argument| argument.to_array(rows))
            .collect::<Result<Vec<ArrayRef>>>()?;
        let pairs = arrays.len() / 2;
        let keys: Vec<&ArrayRef> = arrays.iter().step_by(2).collect();
        let values: Vec<&ArrayRef> = arrays.iter().skip(1).step_by(2).collect();
        refuse_bad_keys(&keys)?;
        let structs = StructArray::try_new(
            pair.clone(),
            vec![flatten(&keys, rows)?, flatten(&values, rows)?],
            None,
        )?;
        Ok(ColumnarValue::Array(Arc::new(MapArray::try_new(
            entries_field,
            OffsetBuffer::from_lengths(std::iter::repeat_n(pairs, rows)),
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

    fn column_strings(sql: &str) -> Vec<String> {
        let batches = run(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        let mut cells = Vec::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                cells.push(
                    ScalarValue::try_from_array(batch.column(0), row)
                        .expect("scalar")
                        .to_string(),
                );
            }
        }
        cells
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

    #[test]
    fn create_map_builds_one_map_per_row_from_columns_and_scalars() {
        assert_eq!(
            column_strings(
                "SELECT create_map(k, v, 'z', w) FROM \
                 (VALUES ('a', 1, 10), ('b', 2, 20)) AS t(k, v, w)"
            ),
            ["[{a:1,z:10}]", "[{b:2,z:20}]"]
        );
        assert_eq!(
            column_strings("SELECT create_map('a', v) FROM (VALUES (1), (2), (3)) AS t(v)"),
            ["[{a:1}]", "[{a:2}]", "[{a:3}]"]
        );
        assert_eq!(
            column_strings("SELECT create_map(v, 'x') FROM (VALUES (1), (2)) AS t(v)"),
            ["[{1:x}]", "[{2:x}]"]
        );
    }

    #[test]
    fn create_map_keeps_nulls_and_nested_values() {
        assert_eq!(
            column_strings(
                "SELECT create_map('a', v, 'b', w) FROM \
                 (VALUES (CAST(NULL AS INT), 1), (2, 3)) AS t(v, w)"
            ),
            ["[{a:,b:1}]", "[{a:2,b:3}]"]
        );
        assert_eq!(
            column_strings(
                "SELECT create_map('a', named_struct('x', v)) FROM (VALUES (1), (5)) AS t(v)"
            ),
            ["[{a:{x: 1}}]", "[{a:{x: 5}}]"]
        );
        assert_eq!(
            column_strings(
                "SELECT create_map('a', make_array(v, v + 1)) FROM (VALUES (1), (5)) AS t(v)"
            ),
            ["[{a:[1, 2]}]", "[{a:[5, 6]}]"]
        );
    }

    #[test]
    fn create_map_reports_the_first_bad_row_in_row_order() {
        let duplicate_second = run(
            "SELECT create_map(x, 1, y, 2) FROM (VALUES ('a', 'b'), ('c', 'c'), ('d', 'd')) AS t(x, y)",
        )
        .expect_err("a duplicate key must raise");
        assert!(
            duplicate_second.to_string().contains("key c "),
            "{duplicate_second}"
        );
        let null_first = run("SELECT create_map(x, 1, y, 2) FROM \
             (VALUES (CAST(NULL AS STRING), 'b'), ('c', 'c')) AS t(x, y)")
        .expect_err("a null key must raise");
        assert!(
            null_first.to_string().contains("NULL_MAP_KEY"),
            "{null_first}"
        );
        let duplicate_first = run("SELECT create_map(x, 1, y, 2) FROM \
             (VALUES ('c', 'c'), (CAST(NULL AS STRING), 'b')) AS t(x, y)")
        .expect_err("a duplicate key must raise");
        assert!(
            duplicate_first.to_string().contains("DUPLICATED_MAP_KEY"),
            "{duplicate_first}"
        );
    }

    #[test]
    fn create_map_finds_duplicates_among_struct_and_nan_keys() {
        let structs = run("SELECT create_map(named_struct('a', 1), 1, named_struct('a', 1), 2)")
            .expect_err("equal struct keys must raise");
        assert!(
            structs.to_string().contains("DUPLICATED_MAP_KEY"),
            "{structs}"
        );
        let nan = run("SELECT create_map(CAST('NaN' AS DOUBLE), 1, CAST('NaN' AS DOUBLE), 2)")
            .expect_err("equal NaN keys must raise");
        assert!(nan.to_string().contains("DUPLICATED_MAP_KEY"), "{nan}");
        assert_eq!(
            column_strings("SELECT create_map(named_struct('a', 1), 1, named_struct('a', 2), 2)"),
            ["[{{a: 1}:1,{a: 2}:2}]"]
        );
    }

    #[test]
    fn the_sql_name_is_internal_and_create_map_stays_unregistered() {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let refused = runtime
            .block_on(async { ctx.sql("SELECT create_map('a', 1)").await?.collect().await })
            .expect_err("create_map is not a SQL function");
        assert!(
            refused.to_string().contains("Invalid function"),
            "{refused}"
        );
        let internal = runtime
            .block_on(async {
                ctx.sql(&format!("SELECT {}('a', 1)", super::SQL_MAP_NAME))
                    .await?
                    .collect()
                    .await
            })
            .expect("the internal name resolves");
        assert_eq!(internal[0].num_rows(), 1);
    }
}
