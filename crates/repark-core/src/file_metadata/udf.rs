use std::collections::HashMap;
use std::sync::Arc;

use super::{FILE_SOURCE_METADATA_KEY, METADATA_COL_KEY, METADATA_COLUMN_NAME};
use arrow::array::StructArray;
use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::{exec_datafusion_err, exec_err, plan_err};
use datafusion::error::Result as DataFusionResult;

use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
pub(crate) const METADATA_FIELD_NAMES: [&str; 7] = [
    "file_path",
    "file_name",
    "file_size",
    "file_block_start",
    "file_block_length",
    "file_modification_time",
    "row_index",
];

pub(crate) const METADATA_UDF_NAME: &str = "__repark_file_metadata";

pub(crate) const METADATA_UDF_NAME_NO_ROW_INDEX: &str = "__repark_file_metadata_no_row_index";
fn metadata_field_type(name: &str) -> DataType {
    if name == "file_path" || name == "file_name" {
        DataType::Utf8
    } else if name == "file_modification_time" {
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    } else {
        DataType::Int64
    }
}

pub(crate) fn metadata_fields(with_row_index: bool) -> Vec<FieldRef> {
    METADATA_FIELD_NAMES
        .iter()
        .filter(|name| with_row_index || **name != "row_index")
        .map(|name| Arc::new(Field::new(*name, metadata_field_type(name), false)) as FieldRef)
        .collect()
}

pub(crate) fn metadata_outer_field(with_row_index: bool) -> FieldRef {
    let metadata = HashMap::from([
        (FILE_SOURCE_METADATA_KEY.to_string(), "true".to_string()),
        (
            METADATA_COL_KEY.to_string(),
            METADATA_COLUMN_NAME.to_string(),
        ),
    ]);
    Arc::new(
        Field::new(
            METADATA_COLUMN_NAME,
            DataType::Struct(metadata_fields(with_row_index).into()),
            false,
        )
        .with_metadata(metadata),
    )
}

#[derive(Debug)]
struct FileMetadataUdf {
    signature: Signature,
    with_row_index: bool,
}

impl FileMetadataUdf {
    fn udf(with_row_index: bool) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            signature: Signature::user_defined(Volatility::Immutable),
            with_row_index,
        }))
    }
}

impl PartialEq for FileMetadataUdf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for FileMetadataUdf {}

impl std::hash::Hash for FileMetadataUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for FileMetadataUdf {
    fn name(&self) -> &str {
        if self.with_row_index {
            METADATA_UDF_NAME
        } else {
            METADATA_UDF_NAME_NO_ROW_INDEX
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        exec_err!(
            "{} return_type is not used; return_field_from_args is",
            self.name()
        )
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DataFusionResult<FieldRef> {
        let expected = if self.with_row_index { 7 } else { 6 };
        if args.arg_fields.len() != expected {
            return plan_err!(
                "{} takes {expected} arguments, got {}",
                self.name(),
                args.arg_fields.len()
            );
        }
        Ok(metadata_outer_field(self.with_row_index))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let expected = if self.with_row_index { 7 } else { 6 };
        if args.args.len() != expected {
            return exec_err!(
                "{} takes {expected} arguments, got {}",
                self.name(),
                args.args.len()
            );
        }
        let fields = metadata_fields(self.with_row_index);
        let mut columns = Vec::with_capacity(expected);
        for (argument, field) in args.args.iter().zip(fields.iter()) {
            let array = argument.to_array(args.number_rows)?;
            let typed = arrow::compute::cast(array.as_ref(), field.data_type())
                .map_err(|error| exec_datafusion_err!("{error}"))?;
            columns.push(typed);
        }
        let result = StructArray::try_new(fields.into(), columns, None)
            .map_err(|error| exec_datafusion_err!("{error}"))?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

#[must_use]
pub(crate) fn file_metadata_call(args: Vec<Expr>, with_row_index: bool) -> Expr {
    FileMetadataUdf::udf(with_row_index).call(args)
}
