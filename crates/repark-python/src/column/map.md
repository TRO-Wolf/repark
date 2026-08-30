# map — repark-python/src/column

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

`PyColumn` is an immutable DataFusion expression wrapper. Its constructors and operators provide
the Python facade's Column surface while DataFrame methods bind expressions to input schemas.

## Modules

- [`mod.rs`](mod.rs) owns `PyColumn`, constructors, operators, aggregates, and window attachment.
- [`function_dispatch.rs`](function_dispatch.rs) owns scalar and aggregate function dispatch.
- [`expr_build.rs`](expr_build.rs) owns type parsing, alias handling, and expression inspection.
- [`window.rs`](window.rs) owns Spark frame conversion and unordered-window policy.
- [`door_parity_tests.rs`](door_parity_tests.rs) pins standalone facade UDF behavior against SQL.

## Contracts

- `literal` distinguishes Python `bool` from `int` and accepts only supported scalar types.
- `sql` analyzes standalone expressions before handoff; parse errors map to `ParseException` and
  unresolved names map to `AnalysisException`.
- Higher-order lambda variables are resolved against the consuming DataFrame schema.
- Nested higher-order functions refuse loudly rather than producing an invalid plan.
- `concat` propagates NULL and returns Spark-compatible UTF-8 output.
- Window frames use Spark-relative offsets. Count-like unsigned results are cast to signed types.
- Unknown scalar, aggregate, cast, or window names fail with typed Python exceptions.

## Change locations

Add a Column method in `mod.rs`, a scalar or aggregate dispatch arm in `function_dispatch.rs`, a
builder rule in `expr_build.rs`, or a frame rule in `window.rs`. Add the matching parity test.

## Verification

Run `cargo fmt --check`, `cargo test -p repark-python`, the exact-equivalence scanner, and map
sync after changes.

## Pointers

- Up: [src map](../map.md)
- Crate: [repark-python map](../../map.md)
