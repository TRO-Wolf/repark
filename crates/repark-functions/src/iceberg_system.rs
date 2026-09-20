use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, new_null_array};
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
    vec![
        bucket_udf(),
        truncate_udf(),
        years_udf(),
        months_udf(),
        days_udf(),
        hours_udf(),
        iceberg_version_udf(),
    ]
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

#[must_use]
pub fn years_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(IcebergSystemTemporal {
        signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        kind: SystemTemporalKind::Year,
    }))
}

#[must_use]
pub fn months_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(IcebergSystemTemporal {
        signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        kind: SystemTemporalKind::Month,
    }))
}

#[must_use]
pub fn days_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(IcebergSystemTemporal {
        signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        kind: SystemTemporalKind::Day,
    }))
}

#[must_use]
pub fn hours_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(IcebergSystemTemporal {
        signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        kind: SystemTemporalKind::Hour,
    }))
}

#[must_use]
pub fn iceberg_version_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(IcebergVersion {
        signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
    }))
}

pub const BUCKET_INTERNAL_NAME: &str = "__iceberg_system_bucket";
pub const TRUNCATE_INTERNAL_NAME: &str = "__iceberg_system_truncate";
pub const YEARS_INTERNAL_NAME: &str = "__iceberg_system_years";
pub const MONTHS_INTERNAL_NAME: &str = "__iceberg_system_months";
pub const DAYS_INTERNAL_NAME: &str = "__iceberg_system_days";
pub const HOURS_INTERNAL_NAME: &str = "__iceberg_system_hours";
pub const ICEBERG_VERSION_INTERNAL_NAME: &str = "__iceberg_system_iceberg_version";

pub const SYSTEM_FUNCTION_NAMES: [&str; 7] = [
    "bucket",
    "days",
    "hours",
    "iceberg_version",
    "months",
    "truncate",
    "years",
];

#[must_use]
pub fn internal_name(public: &str) -> Option<&'static str> {
    match public {
        "bucket" => Some(BUCKET_INTERNAL_NAME),
        "truncate" => Some(TRUNCATE_INTERNAL_NAME),
        "years" => Some(YEARS_INTERNAL_NAME),
        "months" => Some(MONTHS_INTERNAL_NAME),
        "days" => Some(DAYS_INTERNAL_NAME),
        "hours" => Some(HOURS_INTERNAL_NAME),
        "iceberg_version" => Some(ICEBERG_VERSION_INTERNAL_NAME),
        _ => None,
    }
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SystemTemporalKind {
    Year,
    Month,
    Day,
    Hour,
}

impl SystemTemporalKind {
    fn output_type(self) -> DataType {
        match self {
            SystemTemporalKind::Day => DataType::Date32,
            SystemTemporalKind::Year | SystemTemporalKind::Month | SystemTemporalKind::Hour => {
                DataType::Int32
            }
        }
    }

    fn build(self) -> Transform {
        match self {
            SystemTemporalKind::Year => Transform::Year,
            SystemTemporalKind::Month => Transform::Month,
            SystemTemporalKind::Day => Transform::Day,
            SystemTemporalKind::Hour => Transform::Hour,
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

#[derive(Debug)]
struct IcebergSystemTemporal {
    signature: Signature,
    kind: SystemTemporalKind,
}

impl PartialEq for IcebergSystemTemporal {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

impl Eq for IcebergSystemTemporal {}

impl Hash for IcebergSystemTemporal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
    }
}

#[derive(Debug)]
struct IcebergVersion {
    signature: Signature,
}

impl PartialEq for IcebergVersion {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for IcebergVersion {}

impl Hash for IcebergVersion {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
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

fn apply_transform(
    transform: Transform,
    value: &ColumnarValue,
    output: &DataType,
) -> Result<ColumnarValue> {
    let function = create_transform_function(&transform)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let input = match value {
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(1)?,
        ColumnarValue::Array(array) => Arc::clone(array),
    };
    let output = if matches!(input.data_type(), DataType::Null) {
        new_null_array(output, input.len())
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

impl ScalarUDFImpl for IcebergSystemWidth {
    fn name(&self) -> &'static str {
        match self.kind {
            SystemKind::Bucket => BUCKET_INTERNAL_NAME,
            SystemKind::Truncate => TRUNCATE_INTERNAL_NAME,
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
        let output = match self.kind {
            SystemKind::Bucket => DataType::Int32,
            SystemKind::Truncate => value.data_type(),
        };
        apply_transform(transform, value, &output)
    }
}

impl ScalarUDFImpl for IcebergSystemTemporal {
    fn name(&self) -> &'static str {
        match self.kind {
            SystemTemporalKind::Year => YEARS_INTERNAL_NAME,
            SystemTemporalKind::Month => MONTHS_INTERNAL_NAME,
            SystemTemporalKind::Day => DAYS_INTERNAL_NAME,
            SystemTemporalKind::Hour => HOURS_INTERNAL_NAME,
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.kind.output_type())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [value] = arg_types else {
            return exec_err!(
                "'{}' expects a single date or timestamp argument",
                self.name()
            );
        };
        if self.kind == SystemTemporalKind::Hour {
            match value {
                DataType::Timestamp(_, _) => {}
                other => {
                    return exec_err!(
                        "'{}' expects a timestamp argument, got {other}",
                        self.name()
                    );
                }
            }
        } else {
            match value {
                DataType::Date32 | DataType::Timestamp(_, _) => {}
                other => {
                    return exec_err!(
                        "'{}' expects a date or timestamp argument, got {other}",
                        self.name()
                    );
                }
            }
        }
        Ok(vec![value.clone()])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(value) = args.args.first() else {
            return exec_err!(
                "'{}' expects a single date or timestamp argument",
                self.name()
            );
        };
        let transform = self.kind.build();
        let output = self.kind.output_type();
        apply_transform(transform, value, &output)
    }
}

impl ScalarUDFImpl for IcebergVersion {
    fn name(&self) -> &'static str {
        ICEBERG_VERSION_INTERNAL_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            Ok(vec![])
        } else {
            exec_err!("'{}' expects no arguments", self.name())
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                env!("CARGO_PKG_VERSION").to_owned(),
            ))))
        } else {
            exec_err!("'{}' expects no arguments", self.name())
        }
    }
}
