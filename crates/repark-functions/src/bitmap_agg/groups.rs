use arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::{EmitTo, GroupsAccumulator};

use super::{
    BITMAP_BYTES, BitmapFold, coerce_bitmap_column, construct_positions, fold_incoming,
    identity_byte, packed_bitmaps_to_array, set_bit,
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
        let positions = construct_positions(column)?;
        for (row, group_index) in group_indices.iter().enumerate() {
            let Some(position) = positions[row] else {
                continue;
            };
            if !row_is_active(opt_filter, true, row) {
                continue;
            }
            let start = group_index.saturating_mul(BITMAP_BYTES);
            set_bit(&mut self.bits[start..start + BITMAP_BYTES], position)?;
        }
        Ok(())
    }

    fn update_bitmaps(
        &mut self,
        column: &ArrayRef,
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<()> {
        let fold = self.fold;
        let column = coerce_bitmap_column(column)?;
        let array = column.as_binary::<i32>();
        for (row, group_index) in group_indices.iter().enumerate() {
            if !row_is_active(opt_filter, array.is_valid(row), row) {
                continue;
            }
            let start = group_index.saturating_mul(BITMAP_BYTES);
            fold_incoming(
                &mut self.bits[start..start + BITMAP_BYTES],
                array.value(row),
                fold,
            );
        }
        Ok(())
    }
}
