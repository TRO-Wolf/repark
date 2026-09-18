use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, Float32Array, Float64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;

#[derive(Debug, Eq)]
pub(crate) struct CanonicalFloatExpr {
    inner: Arc<dyn PhysicalExpr>,
}

impl CanonicalFloatExpr {
    pub(crate) fn wrap(inner: Arc<dyn PhysicalExpr>, key_type: &DataType) -> Arc<dyn PhysicalExpr> {
        if matches!(key_type, DataType::Float32 | DataType::Float64) {
            return Arc::new(Self { inner });
        }
        inner
    }
}

impl PartialEq for CanonicalFloatExpr {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Hash for CanonicalFloatExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl Display for CanonicalFloatExpr {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "canonical_nan({})", self.inner)
    }
}

impl PhysicalExpr for CanonicalFloatExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.inner.data_type(input_schema)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.inner.evaluate(batch)?.into_array(batch.num_rows())?;
        if let Some(floats) = array.as_any().downcast_ref::<Float32Array>() {
            let canonical = floats
                .iter()
                .map(|value| value.map(|float| if float.is_nan() { f32::NAN } else { float }))
                .collect::<Float32Array>();
            return Ok(ColumnarValue::Array(Arc::new(canonical)));
        }
        if let Some(floats) = array.as_any().downcast_ref::<Float64Array>() {
            let canonical = floats
                .iter()
                .map(|value| value.map(|float| if float.is_nan() { f64::NAN } else { float }))
                .collect::<Float64Array>();
            return Ok(ColumnarValue::Array(Arc::new(canonical)));
        }
        Ok(ColumnarValue::Array(array))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [inner] = <[Arc<dyn PhysicalExpr>; 1]>::try_from(children).map_err(|children| {
            DataFusionError::Internal(format!(
                "CanonicalFloatExpr expects exactly one child, got {}",
                children.len()
            ))
        })?;
        Ok(Arc::new(Self { inner }))
    }

    fn fmt_sql(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self}")
    }
}
