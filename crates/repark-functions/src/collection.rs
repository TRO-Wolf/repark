//! Spark collection shims for `element_at`, `[]`, `shuffle`, `str_to_map`, and map construction.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, Int64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::functions_nested::extract::array_element_udf;
use datafusion::functions_nested::map_extract::map_extract_udf;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// Regex `str_to_map` (Spark treats both delimiters as regular expressions).
mod str_to_map;
pub(crate) use str_to_map::bind_ascii_perl_classes;

/// NULL-guarded `shuffle` (X1 — the upstream kernel panics on an all-NULL list).
mod shuffle;

/// `map_from_entries` with Spark's `EXCEPTION` map-key dedup policy (X7).
mod map_from_entries;

/// The collection shims registered after DataFusion's defaults.
#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        element_at_udf(),
        str_to_map_udf(),
        shuffle_udf(),
        map_from_entries_udf(),
    ]
}

/// Spark `shuffle` UDF (NULL-guarded permutation; optional Spark 4.0 seed).
#[must_use]
pub fn shuffle_udf() -> Arc<ScalarUDF> {
    shuffle::shuffle_udf()
}

/// Spark `map_from_entries` UDF (duplicate keys raise `DUPLICATED_MAP_KEY`).
#[must_use]
pub fn map_from_entries_udf() -> Arc<ScalarUDF> {
    map_from_entries::map_from_entries_udf()
}

/// Spark `str_to_map` UDF (regex pair + key/value delimiters).
#[must_use]
pub fn str_to_map_udf() -> Arc<ScalarUDF> {
    str_to_map::str_to_map_udf()
}

/// Spark `element_at` UDF instance (1-based array / map-by-key).
#[must_use]
pub fn element_at_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkElementAt::new()))
}

/// Embedded `[]` UDF: 0-based access with NULL for invalid indices.
#[must_use]
pub fn spark_array_get_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkArrayGet::new()))
}

/// Embedded `Column[key]` UDF for 0-based arrays or map keys.
#[must_use]
pub fn spark_get_item_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkGetItem::new()))
}

/// `SparkArrayGet` — Spark `GetArrayItem` (`arr[i]`): 0-based; negative or out-of-range → NULL.
#[derive(Debug)]
struct SparkArrayGet {
    signature: Signature,
}

impl SparkArrayGet {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkArrayGet {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkArrayGet {}

impl Hash for SparkArrayGet {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkArrayGet {
    crate::shim_udf_boilerplate!("__repark_array_get__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        list_element_type(&arg_types[0]).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "'__repark_array_get__' expects an array first argument, got {}",
                arg_types[0]
            ))
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let indices = cast(arrays[1].as_ref(), &DataType::Int64)?;
        let indices = indices.as_primitive::<Int64Type>();
        let shifted: Int64Array = indices
            .iter()
            .map(|index| {
                index
                    .filter(|value| *value >= 0)
                    .map(|value| value.saturating_add(1))
            })
            .collect();
        delegate(
            &array_element_udf(),
            vec![
                ColumnarValue::Array(Arc::clone(&arrays[0])),
                ColumnarValue::Array(Arc::new(shifted)),
            ],
            args.arg_fields.clone(),
            &args,
            Arc::clone(&args.return_field),
        )
    }
}

/// `SparkGetItem` — Spark `[]` / `GetItem`: array 0-based **or** map-by-key.
#[derive(Debug)]
struct SparkGetItem {
    signature: Signature,
}

impl SparkGetItem {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkGetItem {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkGetItem {}

impl Hash for SparkGetItem {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkGetItem {
    crate::shim_udf_boilerplate!("__repark_get_item__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if let Some(element) = list_element_type(&arg_types[0]) {
            return Ok(element);
        }
        if let Some((_, value)) = map_key_value_types(&arg_types[0]) {
            return Ok(value);
        }
        Err(DataFusionError::Plan(format!(
            "'__repark_get_item__' expects an array or map first argument, got {}",
            arg_types[0]
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [container, key] = arg_types else {
            return Err(DataFusionError::Plan(format!(
                "'__repark_get_item__' expects (array, index) or (map, key), got {} argument(s)",
                arg_types.len()
            )));
        };
        if let Some(element) = list_element_type(container) {
            if !(key.is_integer() || *key == DataType::Null) {
                return Err(DataFusionError::Plan(format!(
                    "'__repark_get_item__' array index must be an integer, got {key}"
                )));
            }
            let container = match container {
                DataType::FixedSizeList(..) => {
                    DataType::List(Arc::new(Field::new_list_field(element, true)))
                }
                other => other.clone(),
            };
            return Ok(vec![container, DataType::Int64]);
        }
        if let Some((key_type, _)) = map_key_value_types(container) {
            return Ok(vec![container.clone(), key_type]);
        }
        Err(DataFusionError::Plan(format!(
            "'__repark_get_item__' expects an array or map first argument, got {container}"
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let container_type = args.arg_fields[0].data_type().clone();
        if list_element_type(&container_type).is_some() {
            return SparkArrayGet::new().invoke_with_args(args);
        }
        invoke_map(&args)
    }
}

/// `SparkElementAt` — Spark `element_at(array, index) | element_at(map, key)`.
#[derive(Debug)]
struct SparkElementAt {
    signature: Signature,
}

impl SparkElementAt {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkElementAt {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkElementAt {}

impl Hash for SparkElementAt {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

/// The key and value types of a `DataType::Map` (its entries struct is `[key, value]`).
fn map_key_value_types(data_type: &DataType) -> Option<(DataType, DataType)> {
    let DataType::Map(entries, _) = data_type else {
        return None;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return None;
    };
    let [key, value] = fields.iter().collect::<Vec<_>>()[..] else {
        return None;
    };
    Some((key.data_type().clone(), value.data_type().clone()))
}

/// The element type of a list-shaped `DataType`.
fn list_element_type(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Some(field.data_type().clone())
        }
        _ => None,
    }
}

impl ScalarUDFImpl for SparkElementAt {
    crate::shim_udf_boilerplate!("element_at");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if let Some(element) = list_element_type(&arg_types[0]) {
            return Ok(element);
        }
        if let Some((_, value)) = map_key_value_types(&arg_types[0]) {
            return Ok(value);
        }
        Err(DataFusionError::Plan(format!(
            "'element_at' expects an array or map first argument, got {}",
            arg_types[0]
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [container, key] = arg_types else {
            return Err(DataFusionError::Plan(format!(
                "'element_at' expects (array, index) or (map, key), got {} argument(s)",
                arg_types.len()
            )));
        };
        if let Some(element) = list_element_type(container) {
            if !(key.is_integer() || *key == DataType::Null) {
                return Err(DataFusionError::Plan(format!(
                    "'element_at' array index must be an integer, got {key}"
                )));
            }
            let container = match container {
                DataType::FixedSizeList(..) => {
                    DataType::List(Arc::new(Field::new_list_field(element, true)))
                }
                other => other.clone(),
            };
            return Ok(vec![container, DataType::Int64]);
        }
        if let Some((key_type, _)) = map_key_value_types(container) {
            return Ok(vec![container.clone(), key_type]);
        }
        Err(DataFusionError::Plan(format!(
            "'element_at' expects an array or map first argument, got {container}"
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let container_type = args.arg_fields[0].data_type().clone();
        if list_element_type(&container_type).is_some() {
            return invoke_array(&args);
        }
        invoke_map(&args)
    }
}

/// Run `udf` over already-coerced arguments — the delegation core both paths share.
fn delegate(
    udf: &ScalarUDF,
    args: Vec<ColumnarValue>,
    arg_fields: Vec<FieldRef>,
    template: &ScalarFunctionArgs,
    return_field: FieldRef,
) -> Result<ColumnarValue> {
    udf.invoke_with_args(ScalarFunctionArgs {
        args,
        arg_fields,
        number_rows: template.number_rows,
        return_field,
        config_options: Arc::clone(&template.config_options),
    })
}

/// Reject index 0, then run Spark `element_at` (1-based, negative-from-end, out-of-range NULL).
fn invoke_array(args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let indices = cast(arrays[1].as_ref(), &DataType::Int64)?;
    let indices = indices.as_primitive::<Int64Type>();
    for row in 0..indices.len() {
        if indices.is_valid(row) && indices.value(row) == 0 {
            return Err(DataFusionError::Execution(
                "element_at: SQL array indices start at 1; the index 0 is invalid (Spark \
                 INVALID_INDEX_OF_ZERO)"
                    .to_string(),
            ));
        }
    }
    let return_field = Arc::clone(&args.return_field);
    delegate(
        &array_element_udf(),
        args.args.clone(),
        args.arg_fields.clone(),
        args,
        return_field,
    )
}

fn invoke_map(args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    let value_type = args.return_field.data_type().clone();
    let list_type = DataType::List(Arc::new(Field::new_list_field(value_type.clone(), true)));
    let extracted = delegate(
        &map_extract_udf(),
        args.args.clone(),
        args.arg_fields.clone(),
        args,
        Arc::new(Field::new("map_extract", list_type.clone(), true)),
    )?;
    delegate(
        &array_element_udf(),
        vec![
            extracted,
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
        ],
        vec![
            Arc::new(Field::new("values", list_type, true)),
            Arc::new(Field::new("index", DataType::Int64, false)),
        ],
        args,
        Arc::clone(&args.return_field),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::{Array, Int64Array};
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        for udf in functions() {
            ctx.register_udf(udf.as_ref().clone());
        }
        ctx
    }

    async fn i64_values(ctx: &SessionContext, sql: &str) -> Vec<Option<i64>> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let column = batches[0].column(0);
        let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
        (0..values.len())
            .map(|row| values.is_valid(row).then(|| values.value(row)))
            .collect()
    }

    /// Spark array `element_at`: 1-based, negative counts from the end, out-of-range → NULL.
    #[tokio::test]
    async fn element_at_array_is_one_based() {
        let ctx = ctx();
        assert_eq!(
            i64_values(&ctx, "SELECT element_at([10, 20, 30], 1)").await,
            vec![Some(10)]
        );
        assert_eq!(
            i64_values(&ctx, "SELECT element_at([10, 20, 30], -1)").await,
            vec![Some(30)]
        );
        assert_eq!(
            i64_values(&ctx, "SELECT element_at([10, 20, 30], 4)").await,
            vec![None]
        );
    }

    /// Index 0 raises Spark's `INVALID_INDEX_OF_ZERO`.
    #[tokio::test]
    async fn element_at_array_zero_index_errors() {
        let ctx = ctx();
        let err = ctx
            .sql("SELECT element_at([10, 20, 30], 0)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("index 0"),
            "expected the zero-index reject, got: {err}"
        );
    }

    /// Spark map `element_at`: the plain value for the key, or NULL when absent.
    #[tokio::test]
    async fn element_at_map_returns_plain_value() {
        let ctx = ctx();
        assert_eq!(
            i64_values(&ctx, "SELECT element_at(map(['a', 'b'], [1, 2]), 'b')").await,
            vec![Some(2)]
        );
        assert_eq!(
            i64_values(&ctx, "SELECT element_at(map(['a', 'b'], [1, 2]), 'z')").await,
            vec![None]
        );
        assert_eq!(
            i64_values(
                &ctx,
                "SELECT element_at(map(['a', 'b'], [1, 2]), k) \
                 FROM (VALUES ('a'), ('z')) AS t(k) ORDER BY k"
            )
            .await,
            vec![Some(1), None]
        );
    }

    /// Polymorphic `GetItem` for 0-based arrays and map keys.
    #[tokio::test]
    async fn get_item_array_zero_based_and_map_by_key() {
        use datafusion::logical_expr::{Expr, col, lit};

        let ctx = SessionContext::new();
        ctx.register_udf(spark_get_item_udf().as_ref().clone());

        let array_df = ctx
            .sql("SELECT [10, 20, 30] AS arr")
            .await
            .expect("array frame")
            .select(vec![
                Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                    spark_get_item_udf(),
                    vec![col("arr"), lit(0i64)],
                ))
                .alias("v"),
            ])
            .expect("select get_item array");
        let batches = array_df.collect().await.expect("collect array get_item");
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64");
        assert_eq!(values.value(0), 10);

        let map_df = ctx
            .sql("SELECT map([0, 1], [100, 200]) AS m, 1 AS id")
            .await
            .expect("map frame")
            .select(vec![
                Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                    spark_get_item_udf(),
                    vec![col("m"), col("id")],
                ))
                .alias("v"),
            ])
            .expect("select get_item map");
        let batches = map_df.collect().await.expect("collect map get_item");
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64");
        assert_eq!(values.value(0), 200);
    }
}
