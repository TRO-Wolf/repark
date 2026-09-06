# map — repark-functions/src/json

## Purpose

The Spark JSON function family (FNP-10), declared by [`json.rs`](../json.rs) and registered from
`register_all`. Every kernel answers Spark 4.1.2, measured live on 2026-09-05 and recorded in
`task/ledgers/staging/fnp-9-collections-json-ledger.md`.

**No `serde_json`.** The unit's brief closes dependency and lockfile changes, and `serde_json` is a
workspace dependency this crate does not declare — adding it would rewrite `Cargo.lock`. The
reader here is also a better fit than a generic one: Spark keeps a JSON number's *token* for
integers and re-renders non-integers through Java's `Double.toString`, which a `serde_json::Value`
round-trip destroys.

## Contents

- `reader.rs` — the borrowed JSON reader (`JsonValue`), the compact re-serializer, and the Java
  number spellings. `json_number_text` keeps an integer token verbatim and re-renders anything
  with `.`/`e` through `java_double_text`; `java_double_text` reproduces `Double.toString` —
  plain decimal for `1e-3 <= |x| < 1e7`, `d.dddEn` outside it, always one digit after the point.
  pins: fnp-9-collections-json/C-002
- `path.rs` — the `get_json_object` path grammar and evaluator. Supported steps: `$`, `.name`,
  `['name']`, `[index]`, `[*]`. A wildcard collects the results of the remaining steps: none is
  SQL NULL; one at the top level is that result bare; otherwise a JSON array. A wildcard whose
  next step is itself a subscript flattens instead (Spark's Hive-inherited double-wildcard
  behaviour) and always wraps. `["name"]`, `$..name` and `.*` do not parse, which is Spark's
  answer of NULL rather than an error. pins: fnp-9-collections-json/C-002
- `scalars.rs` — `get_json_object`, `json_array_length`, `json_object_keys`. All nullable; a
  malformed document or a wrong-shaped value is NULL, never an error.
  pins: fnp-9-collections-json/C-002
- `schema_of.rs` — `schema_of_json`. Non-nullable STRING; struct fields sort alphabetically; a
  lone JSON null infers STRING while a null beside a typed sibling merges away; an integer wider
  than `i64` infers `DECIMAL(digits,0)`. A malformed document raises.
  pins: fnp-9-collections-json/C-003
- `to_json.rs` — `to_json` over STRUCT / ARRAY / MAP. A NULL **struct field** is omitted; a NULL
  **map value** is written as `null` — the asymmetry is Spark's, measured. Binary is base64,
  timestamps render in the session zone with three fraction digits, decimals keep their scale,
  and NaN/Infinity are JSON strings. pins: fnp-9-collections-json/C-004
- `ddl.rs` — the DDL schema parser `from_json` reads. Accepts a bare field list
  (`a INT, b STRING`) and a full type (`STRUCT<…>` / `ARRAY<…>` / `MAP<…>` / a primitive),
  case-insensitively, with backticked names. pins: fnp-9-collections-json/C-005
- `decode.rs` — JSON → Arrow for `from_json`, one pass per target field. A missing field, a JSON
  null, and a value of the wrong shape are all NULL (Spark's PERMISSIVE mode); an integer target
  refuses a fractional token. pins: fnp-9-collections-json/C-005
- `from_json.rs` — the `from_json` UDF: result type from the foldable schema argument, options
  read from a foldable MAP. Only `mode` (PERMISSIVE / FAILFAST) and `columnNameOfCorruptRecord`
  are honoured; every other option is REFUSED rather than silently ignored, because Spark honours
  around twenty of them and a silent ignore would be a wrong answer. Registry row
  `FNP10-JSON-OPTIONS-1`. pins: fnp-9-collections-json/C-005, C-008

Each kernel file carries its own `#[cfg(test)] mod tests` running the measured Spark cells
through a `SessionContext`.

## Pointers

- Up: [../map.md](../map.md) · Registry: [../../../../docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md)
