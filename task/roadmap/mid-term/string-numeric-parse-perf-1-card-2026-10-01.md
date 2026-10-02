# STRING-NUMERIC-PARSE-PERF-1 — fast Spark-compatible string-to-number parse (owner's unit)

**Filed: 2026-10-01 from the overnight owner-items backlog** (source:
`/tmp/oc-worker/direct/wo/overnight-2026-09-29-owner-items.md`, line 71; new
unit, owner, 09-30 evening). **Severity:** not rated (performance unit).
**Target:** chartered unit (carried by #888 as VN7-5; not a #888 blocker).

## The brief, carried verbatim from the source line

Add a fast, Spark-compatible string→number parse in the shared `cast_map`
(`crates/repark-functions/src/cast_map/leaf.rs`, `string_to_fractional`, then
`java_double/parse_float.rs`). Today the Spark-correct per-row
string→DOUBLE/numeric parse makes #888's mixed string-number compares
(`nullif(double_col, string_col)`, `nullif(string_col, 5.5D)`) run 1.1–1.2x
base in debug. It affects every string→number cast. Target:
string→DOUBLE/INT/BIGINT/DECIMAL casts within 1.05x of DataFusion's native
cast, with byte-identical Spark semantics (trimming, ANSI errors,
Infinity/NaN spellings, exponent forms). #888 carries this as VN7-5, and it is
not a #888 blocker.

Paths at base `02c2f1be`, verified present 2026-10-01:
`crates/repark-functions/src/cast_map/leaf.rs` and
`crates/repark-functions/src/java_double/parse_float.rs`. (Neither name
`string_to_fractional` at this base; that symbol resolves against the #888
head.)

## Unit scope (from the source line)

- Fast path first in the shared `cast_map`, then the `java_double` float parse.
- Byte-identical Spark semantics: trimming, ANSI errors, Infinity/NaN spellings, exponent forms.
- Every string→number cast covered: DOUBLE, INT, BIGINT, DECIMAL.

## Clauses (draft)

- **C-001** The four casts land within 1.05x of DataFusion's native cast.
- **C-002** The mixed string-number compare cells answer Spark byte-identically.
- **C-003** VN7-5 closes; #888 need not wait.

## Pointers

- Up: [map.md](map.md)
