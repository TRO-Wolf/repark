use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int32Array, NullArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use iceberg::spec::Transform;
use iceberg::transform::create_transform_function;

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![bucket_udf(), truncate_udf()]
}

pub(crate) fn register(ctx: &datafusion::prelude::SessionContext) {
    for udf in functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
}

#[must_use]
pub fn bucket_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(IcebergSystemWidth {
        signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        kind: SystemKind::Bucket,
    }))
}

#[must_use]
pub fn truncate_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(IcebergSystemWidth {
        signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        kind: SystemKind::Truncate,
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SystemKind {
    Bucket,
    Truncate,
}

impl SystemKind {
    fn parameter_label(self) -> &'static str {
        match self {
            SystemKind::Bucket => "number of buckets",
            SystemKind::Truncate => "truncate width",
        }
    }

    fn build(self, width: u32) -> Transform {
        match self {
            SystemKind::Bucket => Transform::Bucket(width),
            SystemKind::Truncate => Transform::Truncate(width),
        }
    }

    fn null_output(self, length: usize) -> ArrayRef {
        match self {
            SystemKind::Bucket => Arc::new(Int32Array::new_null(length)),
            SystemKind::Truncate => Arc::new(NullArray::new(length)),
        }
    }
}

#[derive(Debug)]
struct IcebergSystemWidth {
    signature: Signature,
    kind: SystemKind,
}

impl PartialEq for IcebergSystemWidth {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

impl Eq for IcebergSystemWidth {}

impl Hash for IcebergSystemWidth {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
    }
}

fn width_argument(args: &[ColumnarValue], name: &str) -> Result<i64> {
    let Some(width) = args.first() else {
        return exec_err!("'{name}' expects a width and a value argument");
    };
    let ColumnarValue::Scalar(scalar) = width else {
        return exec_err!("'{name}' expects the width as an integer scalar literal, got a column");
    };
    match scalar {
        ScalarValue::Int8(Some(value)) => Ok(i64::from(*value)),
        ScalarValue::Int16(Some(value)) => Ok(i64::from(*value)),
        ScalarValue::Int32(Some(value)) => Ok(i64::from(*value)),
        ScalarValue::Int64(Some(value)) => Ok(*value),
        ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None) => {
            exec_err!("'{name}' expects the width as a non-null integer scalar literal")
        }
        other => exec_err!(
            "'{name}' expects the width as Int8, Int16, Int32 or Int64, got {}",
            other.data_type()
        ),
    }
}

fn transform_for_width(kind: SystemKind, width: i64) -> Result<Transform> {
    let label = kind.parameter_label();
    let candidate = u32::try_from(width).map_err(|_| {
        if width < 0 {
            DataFusionError::Execution(format!("Invalid {label}: {width} (must be > 0)"))
        } else {
            DataFusionError::Execution(format!(
                "Invalid {label}: {width} (must be <= {}, the Java int maximum)",
                i32::MAX
            ))
        }
    })?;
    Ok(kind.build(candidate))
}

impl ScalarUDFImpl for IcebergSystemWidth {
    fn name(&self) -> &'static str {
        match self.kind {
            SystemKind::Bucket => "__iceberg_system_bucket",
            SystemKind::Truncate => "__iceberg_system_truncate",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match self.kind {
            SystemKind::Bucket => Ok(DataType::Int32),
            SystemKind::Truncate => {
                let Some(value) = arg_types.get(1) else {
                    return exec_err!("'{}' expects a width and a value argument", self.name());
                };
                Ok(value.clone())
            }
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [width, value] = arg_types else {
            return exec_err!("'{}' expects a width and a value argument", self.name());
        };
        match width {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {}
            other => {
                return exec_err!(
                    "'{}' expects the width as Int8, Int16, Int32 or Int64, got {other}",
                    self.name()
                );
            }
        }
        Ok(vec![width.clone(), value.clone()])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let width = width_argument(&args.args, self.name())?;
        let Some(value) = args.args.get(1) else {
            return exec_err!("'{}' expects a width and a value argument", self.name());
        };
        let transform = transform_for_width(self.kind, width)?;
        let function = create_transform_function(&transform)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let input = match value {
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1)?,
            ColumnarValue::Array(array) => Arc::clone(array),
        };
        let output = if matches!(input.data_type(), DataType::Null) {
            self.kind.null_output(input.len())
        } else {
            function
                .transform(input)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?
        };
        match value {
            ColumnarValue::Scalar(_) => Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &output, 0,
            )?)),
            ColumnarValue::Array(_) => Ok(ColumnarValue::Array(output)),
        }
    }
}
