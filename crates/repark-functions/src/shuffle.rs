//! Spark `shuffle(array[, seed])` — NULL-guarded wrapper over the `datafusion-spark` kernel.
//!
//! **Why a wrapper (X1, S0).** `datafusion-spark` 54.1's `general_array_shuffle` writes a
//! placeholder slot for every NULL row with `mutable.extend(0, 0, 1)` — a read of source
//! range `0..1`. When the child values buffer is *empty* that read is out of bounds and
//! `arrow-data`'s primitive transform panics (`range end index 1 out of range for slice of
//! length 0`). A panic at the Python boundary is an S0: Spark returns NULL for `shuffle(NULL)`.
//!
//! **The trigger needs both halves** (measured on BASE `5f13647`, per batch): an empty values
//! buffer *and* at least one NULL row. `CAST(NULL AS ARRAY<INT>)` and a batch of `[[], NULL]`
//! panic; `CAST(array() AS ARRAY<INT>)` alone does **not** (empty buffer, no NULL row, so the
//! placeholder write never happens), and neither does `[[1, 2], NULL, [3]]` (a NULL row, but
//! the buffer is populated). The guard keys on the buffer alone, which is the wider of the two
//! conditions — deliberately, because it is the cheap one to test and the extra inputs it
//! intercepts are exactly those the kernel cannot change.
//!
//! The guard is exact, not defensive padding: when the values buffer is empty every row is
//! either NULL or the empty list, and a permutation of `[]` is `[]` — so the input *is* the
//! answer and the kernel has nothing to do. Any other input reaches the upstream kernel
//! untouched, so the permutation quality and the seed contract stay upstream's.
//!
//! **Seed (X2).** Upstream already accepts the Spark 4.0 `shuffle(array, seed)` overload
//! (`Int64` scalar). This wrapper keeps that arity so the Spark door and the facade
//! (`F.shuffle(col, seed)`) resolve the *same* UDF and produce the same permutation.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};

/// ===========================================================================================
/// The Spark `shuffle` UDF instance (NULL-guarded).
///
/// Registered after `datafusion-spark`'s defaults so it overwrites the panicking name on the
/// SQL doors, and embedded directly by [`crate::expr_fn::shuffle`] for the facade.
/// ===========================================================================================
#[must_use]
pub fn shuffle_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(ReparkShuffle::new()))
}

/// ===========================================================================================
/// NULL-guarded `shuffle`; every non-degenerate input delegates to `datafusion-spark`.
/// ===========================================================================================
#[derive(Debug)]
struct ReparkShuffle {
    inner: Arc<ScalarUDF>,
}

impl ReparkShuffle {
    fn new() -> Self {
        Self {
            inner: datafusion_spark::function::array::shuffle(),
        }
    }
}

impl PartialEq for ReparkShuffle {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for ReparkShuffle {}

impl std::hash::Hash for ReparkShuffle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

/// `true` when the kernel's NULL-slot placeholder read (`extend(0, 0, 1)`) would be out of
/// bounds — i.e. the list's child values buffer is empty. Every row is then NULL or `[]`.
fn values_buffer_is_empty(array: &ArrayRef) -> bool {
    match array.data_type() {
        DataType::List(_) => array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::ListArray>()
            .is_some_and(|list| list.values().is_empty()),
        DataType::LargeList(_) => array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::LargeListArray>()
            .is_some_and(|list| list.values().is_empty()),
        _ => false,
    }
}

impl ScalarUDFImpl for ReparkShuffle {
    fn name(&self) -> &'static str {
        "shuffle"
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.inner().return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.inner.inner().return_field_from_args(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args.first() {
            // A NULL list scalar (`CAST(NULL AS ARRAY<INT>)`) is its own shuffle. Returning it
            // unexpanded also keeps `shuffle(NULL)` foldable.
            Some(ColumnarValue::Scalar(scalar)) if scalar.is_null() => {
                return Ok(ColumnarValue::Scalar(scalar.clone()));
            }
            Some(ColumnarValue::Scalar(ScalarValue::List(list))) if list.values().is_empty() => {
                return Ok(ColumnarValue::Scalar(ScalarValue::List(Arc::clone(list))));
            }
            Some(ColumnarValue::Array(array)) if values_buffer_is_empty(array) => {
                return Ok(ColumnarValue::Array(Arc::clone(array)));
            }
            _ => {}
        }
        self.inner.inner().invoke_with_args(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, ListArray};
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::config::ConfigOptions;

    fn int_list_field() -> FieldRef {
        Arc::new(Field::new("item", DataType::Int32, true))
    }

    fn invoke(arg: ColumnarValue, rows: usize) -> Result<ColumnarValue> {
        let udf = ReparkShuffle::new();
        let field = Arc::new(Field::new("s", DataType::List(int_list_field()), true));
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![arg],
            arg_fields: vec![Arc::clone(&field)],
            number_rows: rows,
            return_field: field,
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    /// X1: the S0 panic input. A NULL list scalar must come back NULL, not abort the process.
    #[test]
    fn null_array_scalar_returns_null_instead_of_panicking() {
        let null_list = ScalarValue::List(Arc::new(ListArray::new_null(int_list_field(), 1)));
        let out = invoke(ColumnarValue::Scalar(null_list), 1).expect("shuffle(NULL) must not fail");
        match out {
            ColumnarValue::Scalar(scalar) => assert!(scalar.is_null()),
            ColumnarValue::Array(array) => assert_eq!(array.null_count(), 1),
        }
    }

    /// X1: the array form of the same degenerate shape (all rows NULL ⇒ empty values buffer).
    #[test]
    fn all_null_list_array_returns_all_nulls_instead_of_panicking() {
        let array: ArrayRef = Arc::new(ListArray::new_null(int_list_field(), 3));
        let out = invoke(ColumnarValue::Array(array), 3).expect("shuffle(NULL rows) must not fail");
        let ColumnarValue::Array(out) = out else {
            panic!("expected an array");
        };
        assert_eq!(out.len(), 3);
        assert_eq!(out.null_count(), 3);
    }

    /// X1, second panic shape: an empty list and a NULL in ONE batch. The values buffer is
    /// empty (so the placeholder read is out of bounds) but the rows are not all NULL, so the
    /// all-NULL test above does not cover it.
    #[test]
    fn empty_list_beside_a_null_row_returns_both_instead_of_panicking() {
        let values = Arc::new(Int32Array::from(Vec::<i32>::new()));
        let array: ArrayRef = Arc::new(ListArray::new(
            int_list_field(),
            OffsetBuffer::new(vec![0, 0, 0].into()),
            values,
            Some(vec![true, false].into()),
        ));
        let out = invoke(ColumnarValue::Array(array), 2).expect("must not panic");
        let ColumnarValue::Array(out) = out else {
            panic!("expected an array");
        };
        let list = out.as_any().downcast_ref::<ListArray>().expect("list");
        assert_eq!(list.len(), 2);
        assert!(!list.is_null(0), "row 0 is the empty list, not NULL");
        assert_eq!(list.value(0).len(), 0);
        assert!(list.is_null(1), "row 1 stays NULL");
    }

    /// The guard must not swallow real work: a populated array still reaches the kernel and
    /// keeps its multiset. (Drop the `values_buffer_is_empty` gate and this stays green — it is
    /// the *panic* tests above that kill the guard.)
    #[test]
    fn populated_array_still_shuffles_the_same_multiset() {
        let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let array: ArrayRef = Arc::new(ListArray::new(
            int_list_field(),
            OffsetBuffer::new(vec![0, 5].into()),
            values,
            None,
        ));
        let out = invoke(ColumnarValue::Array(array), 1).expect("shuffle must succeed");
        let ColumnarValue::Array(out) = out else {
            panic!("expected an array");
        };
        let list = out.as_any().downcast_ref::<ListArray>().expect("list");
        let mut got: Vec<i32> = list
            .value(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32")
            .values()
            .to_vec();
        got.sort_unstable();
        assert_eq!(got, vec![1, 2, 3, 4, 5]);
    }

    /// X2: the seeded overload resolves on the guarded UDF and is deterministic across calls.
    #[test]
    fn same_seed_gives_the_same_permutation() {
        use datafusion::prelude::SessionContext;

        let permute = || {
            let ctx = SessionContext::new();
            crate::register_all(&ctx);
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            runtime.block_on(async {
                let batches = ctx
                    .sql("SELECT shuffle(array(1, 2, 3, 4, 5, 6, 7, 8), 7) AS s")
                    .await
                    .expect("plan")
                    .collect()
                    .await
                    .expect("collect");
                let list = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("list")
                    .value(0);
                (0..list.len())
                    .map(|index| {
                        ScalarValue::try_from_array(&list, index)
                            .expect("scalar")
                            .to_string()
                    })
                    .collect::<Vec<_>>()
            })
        };
        assert_eq!(permute(), permute());
    }
}
