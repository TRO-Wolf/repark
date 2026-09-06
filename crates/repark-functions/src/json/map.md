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
  number spellings. **Round 2 (2026-09-06, findings F9/F14/F16/F17):** single-quoted strings and
  keys parse (Spark's factory allows them everywhere); a leading zero, a raw control character
  inside a string, and an unterminated escape are all malformed; `NaN` / `Infinity` /
  `-Infinity` literals parse to `JsonValue::NonFinite` and render as JSON strings; `-0` renders
  `0`; the `\uXXXX` escape is upper-case. `json_number_text` keeps an integer token verbatim and re-renders anything
  with `.`/`e` through `java_double_text`; `java_double_text` reproduces `Double.toString` —
  plain decimal for `1e-3 <= |x| < 1e7`, `d.dddEn` outside it, always one digit after the point.
  pins: fnp-9-collections-json/C-002
- `path.rs` — the `get_json_object` path grammar and evaluator. Supported steps: `$`, `.name`,
  `['name']`, `[index]`, `[*]`. **Round 2 (2026-09-06, critic finding F3):** the evaluator is
  Spark's three-style state machine, not a rule fitted to the cells that happened to be
  measured. `Raw` is the top-level style: a string leaf renders unquoted, a null leaf is SQL
  NULL, and a wildcard that collects exactly one result returns it bare. `New` is every style
  below a wildcard: strings quote, nulls render `null`, and a wildcard always wraps — which is
  why `$.a[0][*]` answers `[1]` where `$.a[*]` on the same array answers `1`, and why an index
  step immediately before a wildcard switches `Raw` to `New`. `Flatten` is the style a
  double wildcard (`[*][*]`) hands its children: an array at the end of the path flattens into
  the parent instead of nesting. `["name"]`, `$..name` and `.*` do not parse, which is Spark's
  answer of NULL rather than an error. pins: fnp-9-collections-json/C-002
- `scalars.rs` — `get_json_object`, `json_array_length`, `json_object_keys`. All nullable; a
  malformed document or a wrong-shaped value is NULL, never an error.
  pins: fnp-9-collections-json/C-002
- `schema_of.rs` — `schema_of_json`. Non-nullable STRING; struct fields sort alphabetically; a
  lone JSON null infers STRING while a null beside a typed sibling merges away; an integer wider
  than `i64` infers `DECIMAL(digits,0)`. A malformed document raises. **Round 2 (2026-09-06,
  findings F10/F15):** a field name that is not a plain identifier is backtick-quoted with an
  embedded backtick doubled, so the output round-trips through `ddl.rs`; an all-empty struct
  field is pruned the way Spark's `canonicalizeType` prunes it; a repeated key is KEPT rather
  than merged; an empty document infers STRING. pins: fnp-9-collections-json/C-003
- `to_json.rs` — `to_json` over STRUCT / ARRAY / MAP. A NULL **struct field** is omitted; a NULL
  **map value** is written as `null` — the asymmetry is Spark's, measured. Binary is base64,
  timestamps render in the session zone with three fraction digits, decimals keep their scale,
  and NaN/Infinity are JSON strings. pins: fnp-9-collections-json/C-004
- `ddl.rs` — the DDL schema parser `from_json` reads. Accepts a bare field list
  (`a INT, b STRING`) and a full type (`STRUCT<…>` / `ARRAY<…>` / `MAP<…>` / a primitive),
  case-insensitively, with backticked names — a doubled backtick inside one is the escape, which
  is what `schema_of_json` emits. `NOT NULL` and `COMMENT '…'` field modifiers parse and are
  ignored, as Spark ignores them. `INTERVAL` is refused loudly (§7 `FNP10-FROM-JSON-DDL-1`).
  pins: fnp-9-collections-json/C-005
- `decode.rs` — JSON → Arrow for `from_json`, one pass per target field, carrying a per-row
  **bad-record flag** beside the array. **Round 2 (2026-09-06, findings F1/F2/F6/F7/F8):** a
  value that cannot be decoded sets the flag, which is what `from_json` reads for FAILFAST and
  for `_corrupt_record` — Spark's "malformed record" is a bad VALUE, not only an unparsable
  document. A wrong-shaped array, struct or map NULLs the whole field and sets the flag, while a
  struct whose value IS an object keeps its other fields (Spark's partial results are
  struct-field-only). A repeated object key is last-wins. A number decoded into STRING takes
  `json_number_text`, so `1.50` becomes `1.5`. `decimal_units` scales the token TEXT, rounds
  HALF_UP at the declared scale and answers NULL when the result is wider than the precision.
  pins: fnp-9-collections-json/C-005
- `from_json.rs` — the `from_json` UDF: result type from the foldable schema argument, options
  read from a foldable MAP. **Round 2 (2026-09-06, findings F1/F4/F5/F6/F17):** FAILFAST and
  `_corrupt_record` both read `decode.rs`'s bad-record flag; an empty or whitespace document is
  a NULL row and never a corrupt record; a root object wraps for an `ARRAY<STRUCT>` schema; a
  scalar root schema, a non-STRING `_corrupt_record` and a non-STRING map key are refused by
  Spark's own condition names. Only `mode` (PERMISSIVE / FAILFAST) and `columnNameOfCorruptRecord`
  are honoured; every other option is REFUSED rather than silently ignored, because Spark honours
  around twenty of them and a silent ignore would be a wrong answer. The facade applies the same
  rule to `to_json` and `schema_of_json`, which implement no option at all. Registry row
  `FNP10-JSON-OPTIONS-1`. pins: fnp-9-collections-json/C-005, C-008

Each kernel file carries its own `#[cfg(test)] mod tests` running the measured Spark cells
through a `SessionContext`.

## Pointers

- Up: [../map.md](../map.md) · Registry: [../../../../docs/spark-sql-iceberg-parity.md](../../../../docs/spark-sql-iceberg-parity.md)
