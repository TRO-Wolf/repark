use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, internal_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::instant_ts::ltz_timestamp_type;
use crate::spark_string_timestamp::is_string_type;

pub use repark_common::udf_names::{TIMESTAMP_LTZ_CAST_NAME, TRY_TIMESTAMP_LTZ_CAST_NAME};

static TIMESTAMP_LTZ_CAST: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(SparkTimestampLtzCast::new(false))));
static TRY_TIMESTAMP_LTZ_CAST: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::from(SparkTimestampLtzCast::new(true))));

#[must_use]
pub fn timestamp_ltz_cast_udf(try_cast: bool) -> Arc<ScalarUDF> {
    if try_cast {
        Arc::clone(&TRY_TIMESTAMP_LTZ_CAST)
    } else {
        Arc::clone(&TIMESTAMP_LTZ_CAST)
    }
}

#[must_use]
pub(crate) fn unwrap_ltz_cast_marker(expr: &Expr) -> Option<(Expr, bool)> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    if function.args.len() != 1 {
        return None;
    }
    let try_cast = match function.func.name() {
        TIMESTAMP_LTZ_CAST_NAME => false,
        TRY_TIMESTAMP_LTZ_CAST_NAME => true,
        _ => return None,
    };
    Some((function.args[0].clone(), try_cast))
}

#[derive(Debug)]
struct SparkTimestampLtzCast {
    try_cast: bool,
    signature: Signature,
}

impl SparkTimestampLtzCast {
    fn new(try_cast: bool) -> Self {
        Self {
            try_cast,
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl PartialEq for SparkTimestampLtzCast {
    fn eq(&self, other: &Self) -> bool {
        self.try_cast == other.try_cast
    }
}

impl Eq for SparkTimestampLtzCast {}

impl Hash for SparkTimestampLtzCast {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkTimestampLtzCast {
    fn name(&self) -> &str {
        if self.try_cast {
            TRY_TIMESTAMP_LTZ_CAST_NAME
        } else {
            TIMESTAMP_LTZ_CAST_NAME
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(ltz_timestamp_type())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args.arg_fields.first().is_none_or(|field| {
            field.is_nullable() || self.try_cast || is_string_type(field.data_type())
        });
        Ok(Arc::new(Field::new(
            self.name(),
            ltz_timestamp_type(),
            nullable,
        )))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!(
            "'{}' must be rewritten by spark_ltz_timestamp_cast before execution",
            self.name()
        )
    }
}
