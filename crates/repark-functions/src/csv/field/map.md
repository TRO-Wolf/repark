# map — repark-functions/src/csv/field

## Purpose

CSV-FIELD-KERNEL-1's tests for [`../field.rs`](../field.rs), against Spark 4.1.2's recorded answers.

## Contents

- `tests.rs` — every cell of `task/wo/csv-string-field-text-1/oracle.md` (74 texts × 13 types, 962
  cells); the 62 added texts × 16 types (a lone space, `.`, Fable's §10 texts, the `GMT`, year,
  exponent and boundary texts, and three more decimal types; 978 cells and 14 declared
  Unicode-digit cells); the four re-measured texts × three decimal types; the 84 zone cells
  (three zones, `TIMESTAMP_NTZ` also read under UTC, so F2's gap cells keep the wall clock); the
  misfit report; the bounded exponent; the refusals; and the registered UDF through SQL. The
  tables hold each answer in one canonical form: `N` NULL, integers, Python float reprs, decimal
  units, epoch days and epoch microseconds, `=` the text itself.

## Pointers

- Up: [../map.md](../map.md)
