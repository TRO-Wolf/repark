# map — repark-python/src/column/expr_build

## Purpose

Child modules of [`expr_build.rs`](../expr_build.rs), split out so it stays under the 1,000-line ceiling.

## Contents

- `py_functions.rs` — the module-level pyfunctions `register` adds: `grouping_id_column` (ATTR-ID-1 SJ-1b, moved
  verbatim) and `fragment_timestamp_ntz_sql`, the `TIMESTAMP_NTZ` respelling the `F.expr` facade asks for under a
  `TIMESTAMP_NTZ` session (FEXPR-TIMESTAMP-NTZ-1 narrow, 2026-11-27). `build_expr_context` registers
  `__repark_decimal_cast_nullable__` for the wrap the column-free fragment's string-literal NTZ cast takes, and a
  column-bearing fragment (`fragment_logical_expr`) keeps main's route until TIMESTAMP-NTZ-STRING-PARSE-1.

## Pointers

- Up: [../map.md](../map.md)
