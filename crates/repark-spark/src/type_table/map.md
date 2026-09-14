# map — repark-spark/src/type_table

## Purpose

Child modules of `type_table.rs` — the FACADE-4 step-1 shared conversion
table (Arrow ↔ `SparkDataType`/`SparkField` descriptor ↔ DDL/SQL text). The
parent file keeps the descriptor model, both Arrow directions, and every
writer surface; this directory holds what split out at the file-size
ceiling.

## Contents

- `parse.rs` — the text→descriptor half of the table: `parse_ddl` and
  `sql_type_from_token` (re-exported at `type_table::` so call sites are
  unchanged), field lists, `decimal(p,s)`, `char`/`varchar`, interval and
  `time(p)` spellings, Python-faithful trim/int semantics, and the
  process-local `OnceLock` regex cache (per-call `Regex::new` was the
  first-pass `fromDDL` regression; compiled once now). S2-21 remediation
  (P3-COLLATION): `SparkString.collation` produced here is `Cow<'static,
  str>` — `atomic_type_from_name` and the `sql_type_from_token` fallback
  borrow `DEFAULT_COLLATION`; only a parsed `string collate NAME` owns.
  pins: facade-4/C-010, C-012, C-019
