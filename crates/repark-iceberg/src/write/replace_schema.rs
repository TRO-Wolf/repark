use std::cell::Cell;

use datafusion::error::Result;
use iceberg::spec::{Schema, TableMetadata, assign_fresh_ids_with_base};
use iceberg::{Error, ErrorKind};

#[allow(clippy::missing_errors_doc)]
pub fn replacement_schema(current: &TableMetadata, schema: &Schema) -> Result<Schema> {
    let counter = Cell::new(current.last_column_id());
    let mut next_id = || -> iceberg::Result<i32> {
        let next = counter.get().checked_add(1).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Field ID overflowed, cannot add more fields",
            )
        })?;
        counter.set(next);
        Ok(next)
    };
    assign_fresh_ids_with_base(schema, current.current_schema(), &mut next_id)
        .map_err(crate::catalog::iceberg_to_datafusion)
}
