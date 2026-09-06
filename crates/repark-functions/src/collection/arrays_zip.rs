use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, ListArray, StructArray, new_null_array};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::concat;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

#[must_use]
pub fn arrays_zip_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkArraysZip::new()))
}

#[derive(Debug)]
struct SparkArraysZip {
    signature: Signature,
}

impl SparkArraysZip {
    fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkArraysZip {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkArraysZip {}

impl Hash for SparkArraysZip {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

fn element_type(data_type: &DataType, position: usize) -> Result<DataType> {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) => Ok(field.data_type().clone()),
        DataType::Null => Ok(DataType::Null),
        other => exec_err!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] 'arrays_zip' argument {position} must be \
             an ARRAY, got {other}"
        ),
    }
}

fn declared_fields(return_field: &FieldRef) -> Option<Fields> {
    let DataType::List(item) = return_field.data_type() else {
        return None;
    };
    match item.data_type() {
        DataType::Struct(fields) => Some(fields.clone()),
        _ => None,
    }
}

fn zip_fields(arg_fields: &[FieldRef]) -> Result<Fields> {
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(arg_fields.len());
    for (position, field) in arg_fields.iter().enumerate() {
        let element = element_type(field.data_type(), position + 1)?;
        fields.push(Arc::new(Field::new(position.to_string(), element, true)));
    }
    Ok(Fields::from(fields))
}

fn zip_type(fields: Fields) -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(fields),
        false,
    )))
}

fn padded_column(
    list: &ListArray,
    row: usize,
    width: usize,
    element: &DataType,
) -> Result<ArrayRef> {
    if list.is_null(row) {
        return Ok(new_null_array(element, width));
    }
    let values = list.value(row);
    if values.len() >= width {
        return Ok(values.slice(0, width));
    }
    let padding = new_null_array(element, width - values.len());
    let references: Vec<&dyn Array> = vec![values.as_ref(), padding.as_ref()];
    Ok(concat(&references)?)
}

impl ScalarUDFImpl for SparkArraysZip {
    crate::shim_udf_boilerplate!("arrays_zip");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let fields: Vec<Arc<Field>> = arg_types
            .iter()
            .enumerate()
            .map(|(position, data_type)| {
                Ok(Arc::new(Field::new(
                    position.to_string(),
                    element_type(data_type, position + 1)?,
                    true,
                )))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(zip_type(Fields::from(fields)))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let fields = zip_fields(args.arg_fields)?;
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            zip_type(fields),
            nullable,
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        for (position, data_type) in arg_types.iter().enumerate() {
            element_type(data_type, position + 1)?;
        }
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let fields = match declared_fields(&args.return_field) {
            Some(declared) => declared,
            None => zip_fields(&args.arg_fields)?,
        };
        if args.args.is_empty() {
            let structs = StructArray::new_empty_fields(0, None);
            return Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
                Arc::new(Field::new("item", DataType::Struct(fields), false)),
                OffsetBuffer::new(vec![0_i32; args.number_rows + 1].into()),
                Arc::new(structs),
                None,
            )?)));
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let mut lists: Vec<&ListArray> = Vec::with_capacity(arrays.len());
        for (position, array) in arrays.iter().enumerate() {
            let Some(list) = array.as_any().downcast_ref::<ListArray>() else {
                return exec_err!(
                    "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] 'arrays_zip' argument {} must be \
                     an ARRAY, got {}",
                    position + 1,
                    array.data_type()
                );
            };
            lists.push(list);
        }
        let rows = arrays[0].len();
        let mut per_argument: Vec<Vec<ArrayRef>> = vec![Vec::with_capacity(rows); lists.len()];
        let mut offsets: Vec<i32> = Vec::with_capacity(rows + 1);
        let mut present: Vec<bool> = Vec::with_capacity(rows);
        let mut length = 0_i32;
        offsets.push(0);
        for row in 0..rows {
            if lists.iter().any(|list| list.is_null(row)) {
                present.push(false);
                offsets.push(length);
                continue;
            }
            let width = lists
                .iter()
                .map(|list| list.value(row).len())
                .max()
                .unwrap_or(0);
            for (index, list) in lists.iter().enumerate() {
                let element = fields[index].data_type();
                per_argument[index].push(padded_column(list, row, width, element)?);
            }
            length += i32::try_from(width).unwrap_or(0);
            present.push(true);
            offsets.push(length);
        }
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(lists.len());
        for (index, pieces) in per_argument.iter().enumerate() {
            let references: Vec<&dyn Array> = pieces.iter().map(AsRef::as_ref).collect();
            columns.push(if references.is_empty() {
                new_null_array(fields[index].data_type(), 0)
            } else {
                concat(&references)?
            });
        }
        let structs = StructArray::try_new(fields.clone(), columns, None)?;
        let nulls = if present.iter().all(|found| *found) {
            None
        } else {
            Some(NullBuffer::from(present))
        };
        Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
            Arc::new(Field::new("item", DataType::Struct(fields), false)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(structs),
            nulls,
        )?)))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;

    fn run(sql: &str) -> datafusion::common::Result<Vec<datafusion::arrow::array::RecordBatch>> {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async { ctx.sql(sql).await?.collect().await })
    }

    #[test]
    fn zip_names_stay_stable_through_the_optimizer() {
        for sql in [
            "SELECT arrays_zip(array(1,2), array('a','b')) AS r",
            "SELECT arrays_zip(array(1)) AS r",
            "SELECT arrays_zip(array(1), CAST(NULL AS ARRAY<STRING>)) AS r",
            "SELECT arrays_zip(a, b) AS r FROM (SELECT array(1,2) AS a, array('x','y') AS b)",
        ] {
            let batches = run(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
            assert_eq!(batches[0].num_rows(), 1, "{sql}");
        }
    }
}
