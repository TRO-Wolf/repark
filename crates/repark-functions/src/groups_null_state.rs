use arrow::array::{Array, BooleanArray, BooleanBufferBuilder, PrimitiveArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::ArrowPrimitiveType;
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::logical_expr::EmitTo;

enum SeenValues {
    All { valid_groups: usize },
    Some { valid: BooleanBufferBuilder },
}

pub(crate) struct GroupNullState {
    seen: SeenValues,
}

impl GroupNullState {
    pub(crate) fn new() -> Self {
        Self {
            seen: SeenValues::All { valid_groups: 0 },
        }
    }

    pub(crate) fn size(&self) -> usize {
        match &self.seen {
            SeenValues::All { .. } => 0,
            SeenValues::Some { valid } => valid.capacity() / 8,
        }
    }

    fn take_builder(&mut self, total_num_groups: usize) -> BooleanBufferBuilder {
        let previous = std::mem::replace(&mut self.seen, SeenValues::All { valid_groups: 0 });
        match previous {
            SeenValues::All { valid_groups } => {
                let mut valid = BooleanBufferBuilder::new(total_num_groups);
                valid.append_n(valid_groups.min(total_num_groups), true);
                valid.append_n(total_num_groups.saturating_sub(valid_groups), false);
                valid
            }
            SeenValues::Some { mut valid } => {
                if valid.len() < total_num_groups {
                    valid.append_n(total_num_groups - valid.len(), false);
                }
                valid
            }
        }
    }

    pub(crate) fn accumulate<T, F>(
        &mut self,
        group_indices: &[usize],
        values: &PrimitiveArray<T>,
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
        mut value_fn: F,
    ) -> Result<()>
    where
        T: ArrowPrimitiveType + Send,
        F: FnMut(usize, T::Native) + Send,
    {
        if values.len() != group_indices.len() {
            return exec_err!("avg groups accumulate: values and group indices disagree");
        }
        if let Some(filter) = opt_filter
            && filter.len() != group_indices.len()
        {
            return exec_err!("avg groups accumulate: filter and group indices disagree");
        }
        if let SeenValues::All { valid_groups } = &mut self.seen
            && opt_filter.is_none()
            && values.null_count() == 0
        {
            for (row, group_index) in group_indices.iter().enumerate() {
                value_fn(*group_index, values.value(row));
            }
            *valid_groups = total_num_groups;
            return Ok(());
        }
        let mut seen = self.take_builder(total_num_groups);
        match (values.null_count() > 0, opt_filter) {
            (false, None) => {
                for (row, group_index) in group_indices.iter().enumerate() {
                    seen.set_bit(*group_index, true);
                    value_fn(*group_index, values.value(row));
                }
            }
            (true, None) => {
                let nulls = values.nulls().ok_or_else(|| {
                    DataFusionError::Internal(
                        "avg groups accumulate: null count without a null mask".to_string(),
                    )
                })?;
                for ((row, group_index), valid) in
                    group_indices.iter().enumerate().zip(nulls.iter())
                {
                    if valid {
                        seen.set_bit(*group_index, true);
                        value_fn(*group_index, values.value(row));
                    }
                }
            }
            (false, Some(filter)) => {
                for ((row, group_index), keep) in
                    group_indices.iter().enumerate().zip(filter.iter())
                {
                    if let Some(true) = keep {
                        seen.set_bit(*group_index, true);
                        value_fn(*group_index, values.value(row));
                    }
                }
            }
            (true, Some(filter)) => {
                for ((group_index, keep), value) in
                    group_indices.iter().zip(filter.iter()).zip(values.iter())
                {
                    if let Some(true) = keep
                        && let Some(value) = value
                    {
                        seen.set_bit(*group_index, true);
                        value_fn(*group_index, value);
                    }
                }
            }
        }
        self.seen = SeenValues::Some { valid: seen };
        Ok(())
    }

    pub(crate) fn build(&mut self, emit_to: EmitTo) -> Result<Option<NullBuffer>> {
        match emit_to {
            EmitTo::All => {
                let previous =
                    std::mem::replace(&mut self.seen, SeenValues::All { valid_groups: 0 });
                match previous {
                    SeenValues::All { .. } => Ok(None),
                    SeenValues::Some { mut valid } => Ok(Some(NullBuffer::new(valid.finish()))),
                }
            }
            EmitTo::First(count) => match &mut self.seen {
                SeenValues::All { valid_groups } => {
                    *valid_groups = valid_groups.saturating_sub(count);
                    Ok(None)
                }
                SeenValues::Some { valid } => {
                    let mut taken = std::mem::replace(valid, BooleanBufferBuilder::new(0));
                    let bits = taken.finish();
                    if count > bits.len() {
                        return exec_err!(
                            "avg groups emit: first-group count exceeds tracked groups"
                        );
                    }
                    let head = bits.slice(0, count);
                    let tail = bits.slice(count, bits.len() - count);
                    let mut rest = BooleanBufferBuilder::new(tail.len());
                    rest.append_buffer(&tail);
                    *valid = rest;
                    Ok(Some(NullBuffer::new(head)))
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;

    #[test]
    fn null_state_build_first_splits_mask() {
        let mut nulls = GroupNullState::new();
        let values = Float64Array::from(vec![Some(1.0), None, Some(3.0)]);
        nulls
            .accumulate(&[0, 1, 2], &values, None, 3, |_, _| {})
            .expect("accumulate");
        let head = nulls.build(EmitTo::First(1)).expect("head").expect("mask");
        assert_eq!(head.len(), 1);
        assert_eq!(head.null_count(), 0);
        let tail = nulls.build(EmitTo::All).expect("tail").expect("mask");
        assert_eq!(tail.len(), 2);
        assert_eq!(tail.null_count(), 1);
    }
}
