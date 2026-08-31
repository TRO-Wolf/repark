//! Spark `aggregate` / `reduce` — sequential fold with optional finish lambda.

use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, GenericListArray, OffsetSizeTrait, UInt32Array, UInt32Builder,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{Result, exec_err, plan_err, utils::adjust_offsets_for_slice};
use datafusion::logical_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDF, HigherOrderUDFImpl, LambdaParametersProgress, ValueOrLambda, Volatility,
};

use super::lambda_utils::list_element_field;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkAggregate {
    signature: HigherOrderSignature,
    aliases: Vec<String>,
}

impl Default for SparkAggregate {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkAggregate {
    pub fn new() -> Self {
        Self {
            signature: HigherOrderSignature::user_defined(Volatility::Immutable),
            aliases: vec![String::from("reduce")],
        }
    }
}

type ValueLambdaSlice<'a, V, L> = &'a [ValueOrLambda<V, L>];

fn array_and_initial<'a, V: std::fmt::Debug, L: std::fmt::Debug>(
    name: &str,
    args: ValueLambdaSlice<'a, V, L>,
) -> Result<(&'a V, &'a V, ValueLambdaSlice<'a, V, L>)> {
    if args.len() < 3 {
        return plan_err!("{name} expects an array, an initial value, and at least one lambda");
    }
    let ValueOrLambda::Value(array) = &args[0] else {
        return plan_err!("{name} expects a list as the first argument");
    };
    let ValueOrLambda::Value(initial) = &args[1] else {
        return plan_err!("{name} expects an initial value as the second argument");
    };
    Ok((array, initial, &args[2..]))
}

impl HigherOrderUDFImpl for SparkAggregate {
    fn name(&self) -> &'static str {
        "aggregate"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn coerce_value_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return plan_err!(
                "{} requires 2 value arguments, got {}",
                self.name(),
                arg_types.len()
            );
        }
        let list = match &arg_types[0] {
            DataType::List(_) | DataType::LargeList(_) => arg_types[0].clone(),
            DataType::ListView(field) | DataType::FixedSizeList(field, _) => {
                DataType::List(Arc::clone(field))
            }
            DataType::LargeListView(field) => DataType::LargeList(Arc::clone(field)),
            DataType::Null => DataType::new_list(DataType::Null, true),
            other => {
                return plan_err!(
                    "{} expected a list as first argument, got {other}",
                    self.name()
                );
            }
        };
        Ok(vec![list, arg_types[1].clone()])
    }

    fn lambda_parameters(
        &self,
        step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        let (array, initial, lambdas) = array_and_initial(self.name(), fields)?;
        let element = list_element_field(self.name(), array)?;
        if lambdas.is_empty() || lambdas.len() > 2 {
            return plan_err!(
                "{} expects a merge lambda and an optional finish lambda, got {}",
                self.name(),
                lambdas.len()
            );
        }
        let merge_output = match &lambdas[0] {
            ValueOrLambda::Lambda(output) => output,
            ValueOrLambda::Value(_) => {
                return plan_err!("{} expected a merge lambda", self.name());
            }
        };
        let acc = merge_output.as_ref().map_or_else(
            || {
                let mut field = initial.as_ref().clone();
                field = field.with_nullable(true);
                Arc::new(field)
            },
            |field| {
                let mut cloned = field.as_ref().clone();
                cloned = cloned.with_nullable(true);
                Arc::new(cloned)
            },
        );
        let merge_params = vec![Arc::clone(&acc), element];
        if lambdas.len() == 2 {
            match &lambdas[1] {
                ValueOrLambda::Lambda(_) => {}
                ValueOrLambda::Value(_) => {
                    return plan_err!("{} expected a finish lambda", self.name());
                }
            }
        }
        if step == 0 && merge_output.is_none() {
            let mut items = vec![Some(merge_params)];
            if lambdas.len() == 2 {
                items.push(None);
            }
            return Ok(LambdaParametersProgress::Partial(items));
        }
        if lambdas.len() == 1 {
            return Ok(LambdaParametersProgress::Complete(vec![merge_params]));
        }
        Ok(LambdaParametersProgress::Complete(vec![
            merge_params,
            vec![acc],
        ]))
    }

    fn coerce_values_for_lambdas(
        &self,
        fields: &[ValueOrLambda<DataType, DataType>],
    ) -> Result<Option<Vec<DataType>>> {
        let (array, initial, lambdas) = array_and_initial(self.name(), fields)?;
        let Some(ValueOrLambda::Lambda(merge_output)) = lambdas.first() else {
            return Ok(None);
        };
        if initial == merge_output {
            return Ok(None);
        }
        Ok(Some(vec![array.clone(), merge_output.clone()]))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<Arc<Field>> {
        let (_array, _initial, lambdas) = array_and_initial(self.name(), args.arg_fields)?;
        let output = if lambdas.len() == 2 {
            let ValueOrLambda::Lambda(finish) = &lambdas[1] else {
                return plan_err!("{} expected a finish lambda", self.name());
            };
            finish
        } else {
            let ValueOrLambda::Lambda(merge) = &lambdas[0] else {
                return plan_err!("{} expected a merge lambda", self.name());
            };
            merge
        };
        Ok(Arc::new(Field::new("", output.data_type().clone(), true)))
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        let (list, initial, lambdas) = array_and_initial(self.name(), &args.args)?;
        let list_array = list.to_array(args.number_rows)?;
        let mut acc = initial.to_array(args.number_rows)?;
        if list_array.null_count() == list_array.len() {
            return Ok(ColumnarValue::Array(
                datafusion::arrow::array::new_null_array(args.return_type(), list_array.len()),
            ));
        }
        let ValueOrLambda::Lambda(merge) = &lambdas[0] else {
            return exec_err!("{} expected a merge lambda", self.name());
        };
        let lengths = row_lengths(list_array.as_ref())?;
        let max_len = lengths.iter().copied().max().unwrap_or(0);
        for position in 0..max_len {
            let active: Vec<u32> = lengths
                .iter()
                .enumerate()
                .filter_map(|(row, length)| {
                    if list_array.is_valid(row) && *length > position {
                        u32::try_from(row).ok()
                    } else {
                        None
                    }
                })
                .collect();
            if active.is_empty() {
                continue;
            }
            let active_array = UInt32Array::from(active.clone());
            let acc_subset = take(acc.as_ref(), &active_array, None)?;
            let elem_subset = take_elements_at(list_array.as_ref(), &active, position)?;
            let merged_subset = merge
                .evaluate(
                    &[&|| Ok(Arc::clone(&acc_subset)), &|| {
                        Ok(Arc::clone(&elem_subset))
                    }],
                    |arrays| take_arrays_u32(arrays, &active_array),
                )?
                .into_array(active.len())?;
            acc = scatter_by_active(acc.as_ref(), &active, merged_subset.as_ref())?;
        }
        let folded = if lambdas.len() == 2 {
            let ValueOrLambda::Lambda(finish) = &lambdas[1] else {
                return exec_err!("{} expected a finish lambda", self.name());
            };
            let valid: Vec<u32> = (0..list_array.len())
                .filter(|&row| list_array.is_valid(row))
                .filter_map(|row| u32::try_from(row).ok())
                .collect();
            if valid.is_empty() {
                acc
            } else {
                let valid_array = UInt32Array::from(valid.clone());
                let acc_subset = take(acc.as_ref(), &valid_array, None)?;
                let finished = finish
                    .evaluate(&[&|| Ok(Arc::clone(&acc_subset))], |arrays| {
                        take_arrays_u32(arrays, &valid_array)
                    })?
                    .into_array(valid.len())?;
                scatter_by_active(acc.as_ref(), &valid, finished.as_ref())?
            }
        } else {
            acc
        };
        Ok(ColumnarValue::Array(mask_null_rows(
            folded.as_ref(),
            list_array.nulls(),
        )?))
    }
}

fn take_arrays_u32(arrays: &[ArrayRef], indices: &UInt32Array) -> Result<Vec<ArrayRef>> {
    arrays
        .iter()
        .map(|array| take(array.as_ref(), indices, None).map_err(Into::into))
        .collect()
}

fn row_lengths(list_array: &dyn Array) -> Result<Vec<usize>> {
    match list_array.data_type() {
        DataType::List(_) => Ok(lengths_of(list_array.as_list::<i32>())),
        DataType::LargeList(_) => Ok(lengths_of(list_array.as_list::<i64>())),
        other => exec_err!("expected list, got {other}"),
    }
}

fn lengths_of<O: OffsetSizeTrait>(list: &GenericListArray<O>) -> Vec<usize> {
    let offsets = adjust_offsets_for_slice(list);
    (0..list.len())
        .map(|row| {
            if list.is_null(row) {
                0
            } else {
                offsets[row + 1].as_usize() - offsets[row].as_usize()
            }
        })
        .collect()
}

fn take_elements_at(list_array: &dyn Array, active: &[u32], position: usize) -> Result<ArrayRef> {
    match list_array.data_type() {
        DataType::List(_) => take_at(list_array.as_list::<i32>(), active, position),
        DataType::LargeList(_) => take_at(list_array.as_list::<i64>(), active, position),
        other => exec_err!("expected list, got {other}"),
    }
}

fn take_at<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    active: &[u32],
    position: usize,
) -> Result<ArrayRef> {
    let offsets = adjust_offsets_for_slice(list);
    let values = list.values();
    let first = offsets.first().map_or(0, |offset| offset.as_usize());
    let mut indices = Vec::with_capacity(active.len());
    for row in active {
        let row = usize::try_from(*row).map_err(|_| {
            datafusion::error::DataFusionError::Execution(
                "row index does not fit in usize".to_string(),
            )
        })?;
        let start = offsets[row].as_usize() - first;
        let index = u32::try_from(start + position).map_err(|_| {
            datafusion::error::DataFusionError::Execution(
                "list value index does not fit in u32".to_string(),
            )
        })?;
        indices.push(index);
    }
    take(values.as_ref(), &UInt32Array::from(indices), None).map_err(Into::into)
}

fn scatter_by_active(base: &dyn Array, active: &[u32], updates: &dyn Array) -> Result<ArrayRef> {
    let mut builder = UInt32Builder::with_capacity(base.len());
    let mut update_index = 0usize;
    let mut next_active = 0usize;
    for row in 0..base.len() {
        let is_active =
            next_active < active.len() && usize::try_from(active[next_active]).ok() == Some(row);
        if is_active {
            let taken = u32::try_from(base.len() + update_index).map_err(|_| {
                datafusion::error::DataFusionError::Execution(
                    "scatter index does not fit in u32".to_string(),
                )
            })?;
            builder.append_value(taken);
            update_index += 1;
            next_active += 1;
        } else {
            let taken = u32::try_from(row).map_err(|_| {
                datafusion::error::DataFusionError::Execution(
                    "scatter row does not fit in u32".to_string(),
                )
            })?;
            builder.append_value(taken);
        }
    }
    let concatenated = datafusion::arrow::compute::concat(&[base, updates])?;
    take(concatenated.as_ref(), &builder.finish(), None).map_err(Into::into)
}

fn mask_null_rows(
    values: &dyn Array,
    nulls: Option<&datafusion::arrow::buffer::NullBuffer>,
) -> Result<ArrayRef> {
    let Some(nulls) = nulls else {
        return Ok(values.slice(0, values.len()));
    };
    if nulls.null_count() == 0 {
        return Ok(values.slice(0, values.len()));
    }
    let mut builder = UInt32Builder::with_capacity(values.len());
    for row in 0..values.len() {
        if nulls.is_valid(row) {
            let index = u32::try_from(row).map_err(|_| {
                datafusion::error::DataFusionError::Execution(
                    "row index does not fit in u32".to_string(),
                )
            })?;
            builder.append_value(index);
        } else {
            builder.append_null();
        }
    }
    take(values, &builder.finish(), None).map_err(Into::into)
}

pub fn aggregate_udf() -> Arc<HigherOrderUDF> {
    static INSTANCE: LazyLock<Arc<HigherOrderUDF>> =
        LazyLock::new(|| Arc::new(HigherOrderUDF::new_from_impl(SparkAggregate::new())));
    Arc::clone(&INSTANCE)
}
