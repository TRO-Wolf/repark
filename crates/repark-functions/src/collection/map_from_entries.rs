//! Spark `map_from_entries` rejects duplicate keys with `DUPLICATED_MAP_KEY`.
//!
//! The guard runs before construction because the upstream kernel silently keeps the last value.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StructArray};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};

/// Spark `map_from_entries(array<struct<key, value>>)` with the `EXCEPTION` dedup policy.
#[must_use]
pub fn map_from_entries_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(ReparkMapFromEntries::new()))
}

#[derive(Debug)]
struct ReparkMapFromEntries {
    inner: Arc<ScalarUDF>,
}

impl ReparkMapFromEntries {
    fn new() -> Self {
        Self {
            inner: datafusion_spark::function::map::map_from_entries(),
        }
    }
}

impl PartialEq for ReparkMapFromEntries {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for ReparkMapFromEntries {}

impl Hash for ReparkMapFromEntries {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

/// Spark's `DUPLICATED_MAP_KEY` text, word-identical to the `str_to_map` shim's.
fn duplicate_key(key: &ScalarValue) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(format!(
        "Duplicate map key '{key}' was found, please check the input data. If you want to \
         remove the duplicated keys, you can set spark.sql.mapKeyDedupPolicy to \"LAST_WIN\" \
         so that the key inserted at last takes precedence."
    ))
}

/// Refuse the first repeated key in any row's entry list.
fn refuse_duplicate_keys(entries: &ArrayRef) -> Result<()> {
    let list = match entries.data_type() {
        DataType::List(_) | DataType::LargeList(_) => entries,
        _ => return Ok(()),
    };
    for row in 0..list.len() {
        if list.is_null(row) {
            continue;
        }
        let row_entries = match list.data_type() {
            DataType::List(_) => list
                .as_any()
                .downcast_ref::<datafusion::arrow::array::ListArray>()
                .map(|array| array.value(row)),
            _ => list
                .as_any()
                .downcast_ref::<datafusion::arrow::array::LargeListArray>()
                .map(|array| array.value(row)),
        };
        let Some(row_entries) = row_entries else {
            continue;
        };
        let Some(structs) = row_entries.as_any().downcast_ref::<StructArray>() else {
            continue;
        };
        if structs.num_columns() == 0 {
            continue;
        }
        let keys = structs.column(0);
        let mut seen: HashSet<ScalarValue> = HashSet::with_capacity(keys.len());
        for index in 0..keys.len() {
            let key = ScalarValue::try_from_array(keys, index)?;
            if key.is_null() {
                continue;
            }
            if !seen.insert(key.clone()) {
                return Err(duplicate_key(&key));
            }
        }
    }
    Ok(())
}

impl ScalarUDFImpl for ReparkMapFromEntries {
    fn name(&self) -> &'static str {
        "map_from_entries"
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

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.inner().coerce_types(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(first) = args.args.first() else {
            return exec_err!("`map_from_entries` expects 1 argument, but got 0");
        };
        let entries = first.to_array(args.number_rows)?;
        refuse_duplicate_keys(&entries)?;
        self.inner.inner().invoke_with_args(args)
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

    /// X7: the duplicate is refused, not silently collapsed to the last entry.
    #[test]
    fn duplicate_keys_raise_instead_of_last_win() {
        let error = run("SELECT map_from_entries(array(struct('a', '1'), struct('a', '2')))")
            .expect_err("duplicate map key must raise");
        let message = error.to_string();
        assert!(message.contains("Duplicate map key"), "{message}");
        assert!(message.contains("mapKeyDedupPolicy"), "{message}");
    }

    /// The guard must not refuse a legal map — distinct keys still build.
    #[test]
    fn distinct_keys_still_build_a_map() {
        let batches = run("SELECT map_from_entries(array(struct('a', '1'), struct('b', '2')))")
            .expect("distinct keys must succeed");
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].column(0).null_count(), 0);
    }

    #[test]
    fn null_entries_stay_null() {
        let batches =
            run("SELECT map_from_entries(CAST(NULL AS ARRAY<STRUCT<key: STRING, value: STRING>>))")
                .expect("NULL entries must not fail");
        assert_eq!(batches[0].column(0).null_count(), 1);
    }
}
