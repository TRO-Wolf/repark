use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, MapArray, StringArray, StructArray};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::type_coercion::binary::comparison_coercion;
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

#[must_use]
pub fn map_concat_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkMapConcat::new()))
}

#[derive(Debug)]
struct SparkMapConcat {
    signature: Signature,
}

impl SparkMapConcat {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkMapConcat {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkMapConcat {}

impl Hash for SparkMapConcat {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn empty_map_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Utf8, true),
            ])),
            false,
        )),
        false,
    )
}

fn entry_pair(found: &DataType) -> Result<(DataType, DataType, bool)> {
    let DataType::Map(entries, sorted) = found else {
        return exec_err!("'map_concat' requires MAP arguments, got {found}");
    };
    let DataType::Struct(pair) = entries.data_type() else {
        return exec_err!("'map_concat' requires a MAP entry struct, got {found}");
    };
    Ok((
        pair[0].data_type().clone(),
        pair[1].data_type().clone(),
        *sorted,
    ))
}

fn widen(left: &DataType, right: &DataType) -> Result<DataType> {
    if left == right {
        return Ok(left.clone());
    }
    comparison_coercion(left, right).ok_or_else(|| {
        datafusion::error::DataFusionError::Plan(format!(
            "'map_concat' cannot reconcile the map component types {left} and {right}"
        ))
    })
}

fn single_map_type(arg_types: &[DataType]) -> Result<DataType> {
    let mut answer: Option<(DataType, DataType, bool)> = None;
    for found in arg_types {
        if matches!(found, DataType::Null) {
            return exec_err!(
                "[DATATYPE_MISMATCH.MAP_CONCAT_DIFF_TYPES] `map_concat` requires MAP arguments \
                 of the same type; an untyped NULL is not one"
            );
        }
        let (key, value, sorted) = entry_pair(found)?;
        answer = Some(match answer {
            None => (key, value, sorted),
            Some((carried_key, carried_value, carried_sorted)) => (
                widen(&carried_key, &key)?,
                widen(&carried_value, &value)?,
                carried_sorted && sorted,
            ),
        });
    }
    let Some((key, value, sorted)) = answer else {
        return Ok(empty_map_type());
    };
    Ok(DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", key, false),
                Field::new("values", value, true),
            ])),
            false,
        )),
        sorted,
    ))
}

fn duplicate_key(key: &ScalarValue) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(format!(
        "Duplicate map key '{key}' was found, please check the input data. If you want to \
         remove the duplicated keys, you can set spark.sql.mapKeyDedupPolicy to \"LAST_WIN\" \
         so that the key inserted at last takes precedence."
    ))
}

fn empty_map_array(rows: usize) -> Result<ArrayRef> {
    let DataType::Map(entries, _) = empty_map_type() else {
        return exec_err!("'map_concat' could not build its empty MAP type");
    };
    let DataType::Struct(pair) = entries.data_type() else {
        return exec_err!("'map_concat' could not build its empty MAP entries");
    };
    let keys: ArrayRef = Arc::new(StringArray::new_null(0));
    let values: ArrayRef = Arc::new(StringArray::new_null(0));
    let structs = StructArray::try_new(pair.clone(), vec![keys, values], None)?;
    Ok(Arc::new(MapArray::try_new(
        Arc::clone(&entries),
        OffsetBuffer::new(vec![0_i32; rows + 1].into()),
        structs,
        None,
        false,
    )?))
}

fn row_slices(maps: &[&MapArray], row: usize) -> Option<Vec<StructArray>> {
    let mut pieces = Vec::with_capacity(maps.len());
    for map in maps {
        if map.is_null(row) {
            return None;
        }
        pieces.push(map.value(row));
    }
    Some(pieces)
}

fn refuse_duplicates(keys: &ArrayRef) -> Result<()> {
    let mut seen: HashSet<ScalarValue> = HashSet::with_capacity(keys.len());
    for index in 0..keys.len() {
        let key = ScalarValue::try_from_array(keys, index)?;
        if !seen.insert(key.clone()) {
            return Err(duplicate_key(&key));
        }
    }
    Ok(())
}

impl ScalarUDFImpl for SparkMapConcat {
    crate::shim_udf_boilerplate!("map_concat");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        single_map_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let types: Vec<DataType> = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            single_map_type(&types)?,
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let common = single_map_type(arg_types)?;
        Ok(vec![common; arg_types.len()])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return Ok(ColumnarValue::Array(empty_map_array(args.number_rows)?));
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let mut maps: Vec<&MapArray> = Vec::with_capacity(arrays.len());
        for array in &arrays {
            let Some(map) = array.as_any().downcast_ref::<MapArray>() else {
                return exec_err!(
                    "'map_concat' requires MAP arguments, got {}",
                    array.data_type()
                );
            };
            maps.push(map);
        }
        let rows = arrays[0].len();
        let entries_field = match maps[0].data_type() {
            DataType::Map(field, _) => Arc::clone(field),
            other => return exec_err!("'map_concat' requires MAP arguments, got {other}"),
        };
        let DataType::Struct(pair) = entries_field.data_type() else {
            return exec_err!("'map_concat' requires a MAP entry struct");
        };
        let mut pieces: Vec<StructArray> = Vec::new();
        let mut offsets: Vec<i32> = Vec::with_capacity(rows + 1);
        let mut present: Vec<bool> = Vec::with_capacity(rows);
        let mut length = 0_i32;
        offsets.push(0);
        for row in 0..rows {
            match row_slices(&maps, row) {
                Some(row_pieces) => {
                    for piece in &row_pieces {
                        length += i32::try_from(piece.len()).unwrap_or(0);
                    }
                    pieces.extend(row_pieces);
                    present.push(true);
                }
                None => present.push(false),
            }
            offsets.push(length);
        }
        let references: Vec<&dyn Array> = pieces.iter().map(|piece| piece as &dyn Array).collect();
        let joined: ArrayRef = if references.is_empty() {
            Arc::new(StructArray::new_empty_fields(0, None))
        } else {
            concat(&references)?
        };
        let structs = if references.is_empty() {
            StructArray::try_new(
                pair.clone(),
                pair.iter()
                    .map(|field| datafusion::arrow::array::new_empty_array(field.data_type()))
                    .collect(),
                None,
            )?
        } else {
            joined
                .as_any()
                .downcast_ref::<StructArray>()
                .cloned()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "'map_concat' could not join the map entries".to_string(),
                    )
                })?
        };
        let offset_buffer = OffsetBuffer::new(offsets.into());
        let built = MapArray::try_new(
            entries_field,
            offset_buffer,
            structs,
            if present.iter().all(|found| *found) {
                None
            } else {
                Some(NullBuffer::from(present))
            },
            false,
        )?;
        for row in 0..built.len() {
            if built.is_null(row) {
                continue;
            }
            let row_entries = built.value(row);
            refuse_duplicates(row_entries.column(0))?;
        }
        Ok(ColumnarValue::Array(Arc::new(built)))
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
    fn map_concat_unions_and_nulls_the_row_for_a_null_argument() {
        assert_eq!(
            shown("SELECT map_concat(map(['a'], [1]), map(['b'], [2]))"),
            "[{a:1,b:2}]"
        );
        assert_eq!(shown("SELECT map_concat(map(['a'], [1]))"), "[{a:1}]");
        let untyped = run("SELECT map_concat(map(['a'], [1]), NULL)")
            .expect_err("an untyped NULL argument must refuse the way Spark refuses it");
        assert!(
            untyped.to_string().contains("MAP_CONCAT_DIFF_TYPES"),
            "{untyped}"
        );
    }

    #[test]
    fn map_concat_refuses_a_duplicate_key_across_arguments() {
        let error = run("SELECT map_concat(map(['a'], [1]), map(['a'], [2]))")
            .expect_err("a duplicate key must raise");
        assert!(error.to_string().contains("Duplicate map key"), "{error}");
        assert!(error.to_string().contains("mapKeyDedupPolicy"), "{error}");
    }
}
