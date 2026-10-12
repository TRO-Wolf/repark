# map — repark-functions/src/nested_ntz_cast

`__repark_cast_nested_ntz__` rewrites selected zoned timestamp leaves inside one
struct value. Struct validity, names, order, nullability and field metadata stay
on the value. Untouched children are the same arrays.

- `tests.rs` — one leaf in UTC, America/New_York and Asia/Kolkata against
  `__repark_cast_timestamp_ntz__`; a null struct, a null inner struct at depth 2
  and a null leaf; a 20-field struct; depth 3; a non-null member; `PARQUET:field_id`
  on the struct and its children; a scalar struct; a null scalar; an unresolvable
  path and a non-literal path are plan errors.
