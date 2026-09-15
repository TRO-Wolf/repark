use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, GenericBinaryArray, OffsetSizeTrait};
use arrow::datatypes::{DataType, Int64Type};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{EmitTo, GroupsAccumulator};

use super::{
    BITMAP_BYTES, BitmapFold, fold_incoming, identity_byte, int64_positions,
    packed_bitmaps_to_array, parse_bigint_cell, position_at, set_bit, utf8_strings,
};

pub(crate) struct BitmapGroupsAccumulator {
    fold: BitmapFold,
    bits: Vec<u8>,
}

impl BitmapGroupsAccumulator {
    pub(crate) fn new(fold: BitmapFold) -> Self {
        Self {
            fold,
            bits: Vec::new(),
        }
    }

    fn ensure_groups(&mut self, total_num_groups: usize) {
        let want = total_num_groups.saturating_mul(BITMAP_BYTES);
        if self.bits.len() < want {
            self.bits.resize(want, identity_byte(self.fold));
        }
    }

    fn take_packed(&mut self, emit_to: EmitTo) -> Vec<u8> {
        match emit_to {
            EmitTo::All => std::mem::take(&mut self.bits),
            EmitTo::First(groups) => {
                let bytes = groups.saturating_mul(BITMAP_BYTES);
                let rest = self.bits.split_off(bytes);
                std::mem::replace(&mut self.bits, rest)
            }
        }
    }
}

fn row_is_active(opt_filter: Option<&BooleanArray>, is_valid: bool, row: usize) -> bool {
    if !is_valid {
        return false;
    }
    match opt_filter {
        Some(filter) => filter.is_valid(row) && filter.value(row),
        None => true,
    }
}

impl GroupsAccumulator for BitmapGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let Some(column) = values.first() else {
            return exec_err!("bitmap groups update: missing argument column");
        };
        if column.len() != group_indices.len() {
            return exec_err!("bitmap groups update: values and group indices disagree");
        }
        self.ensure_groups(total_num_groups);
        match self.fold {
            BitmapFold::Construct => self.update_construct(column, group_indices, opt_filter),
            BitmapFold::Or | BitmapFold::And => {
                self.update_bitmaps(column, group_indices, opt_filter)
            }
        }
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        packed_bitmaps_to_array(self.take_packed(emit_to))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![packed_bitmaps_to_array(self.take_packed(emit_to))?])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let Some(column) = values.first() else {
            return exec_err!("bitmap groups merge: missing state column");
        };
        self.ensure_groups(total_num_groups);
        self.update_bitmaps(column, group_indices, opt_filter)
    }

    fn size(&self) -> usize {
        self.bits.capacity()
    }
}

impl BitmapGroupsAccumulator {
    fn update_construct(
        &mut self,
        column: &ArrayRef,
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<()> {
        match column.data_type() {
            DataType::Null => Ok(()),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let strings = utf8_strings(column)?;
                let strings = strings.as_string::<i32>();
                for (row, group_index) in group_indices.iter().enumerate() {
                    if !row_is_active(opt_filter, strings.is_valid(row), row) {
                        continue;
                    }
                    let start = group_index.saturating_mul(BITMAP_BYTES);
                    set_bit(
                        &mut self.bits[start..start + BITMAP_BYTES],
                        parse_bigint_cell(strings.value(row))?,
                    )?;
                }
                Ok(())
            }
            _ => {
                let casted = int64_positions(column)?;
                let positions = casted.as_primitive::<Int64Type>();
                for (row, group_index) in group_indices.iter().enumerate() {
                    if !row_is_active(opt_filter, true, row) {
                        continue;
                    }
                    let Some(position) = position_at(column, positions, row)? else {
                        continue;
                    };
                    let start = group_index.saturating_mul(BITMAP_BYTES);
                    set_bit(&mut self.bits[start..start + BITMAP_BYTES], position)?;
                }
                Ok(())
            }
        }
    }

    fn update_bitmaps(
        &mut self,
        column: &ArrayRef,
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<()> {
        match column.data_type() {
            DataType::Binary => {
                self.fold_typed(column.as_binary::<i32>(), group_indices, opt_filter);
                Ok(())
            }
            DataType::LargeBinary => {
                self.fold_typed(column.as_binary::<i64>(), group_indices, opt_filter);
                Ok(())
            }
            DataType::BinaryView => {
                let array = column.as_binary_view();
                for (row, group_index) in group_indices.iter().enumerate() {
                    if !row_is_active(opt_filter, array.is_valid(row), row) {
                        continue;
                    }
                    self.fold_row(*group_index, array.value(row));
                }
                Ok(())
            }
            DataType::FixedSizeBinary(_) => {
                let array = column.as_fixed_size_binary();
                for (row, group_index) in group_indices.iter().enumerate() {
                    if !row_is_active(opt_filter, array.is_valid(row), row) {
                        continue;
                    }
                    self.fold_row(*group_index, array.value(row));
                }
                Ok(())
            }
            other => exec_err!("bitmap aggregate expected BINARY, got {other}"),
        }
    }

    fn fold_typed(
        &mut self,
        array: &GenericBinaryArray<impl OffsetSizeTrait>,
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
    ) {
        for (row, group_index) in group_indices.iter().enumerate() {
            if !row_is_active(opt_filter, array.is_valid(row), row) {
                continue;
            }
            self.fold_row(*group_index, array.value(row));
        }
    }

    fn fold_row(&mut self, group_index: usize, incoming: &[u8]) {
        let fold = self.fold;
        let start = group_index.saturating_mul(BITMAP_BYTES);
        fold_incoming(&mut self.bits[start..start + BITMAP_BYTES], incoming, fold);
    }
}
