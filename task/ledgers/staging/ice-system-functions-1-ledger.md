# Charter ledger — ICE-SYSTEM-FUNCTIONS-1 · IPI-29 system-function UDFs, rounds 1-2 of 3

**Date:** 2026-09-20 · **Branch:** `fix/ipi-29-system-functions` · **Base:** `6d029ab8` (`origin/main`) · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Registry:** none this round; the parity row is filed when WO-3 delivers the catalog-qualified registration and SHOW.

**Retires:** WO-3 moves this ledger to `../completed/` in its last commit.

**Scope:** rounds 1-2 — `crates/repark-functions/src/iceberg_system.rs`
(`bucket_udf`, `truncate_udf` in round 1; `years_udf`, `months_udf`, `days_udf`,
`hours_udf`, `iceberg_version_udf` in round 2), two lines in
`repark-functions/src/lib.rs` (round 1; round 2 adds none — H-05 spent), the
external `iceberg` dependency in `repark-functions/Cargo.toml`,
`python/repark/tests/test_ice_system_functions_1.py`, three `map.md` files, this
ledger. No code comment added anywhere (`comment-ban hits=0`); Python docstrings
only where the presence gate needs them. Catalog-qualified registration and SHOW
are WO-3; `session.rs`, `router.rs`, `repark-spark`, `STATUS.md`, every other
`Cargo.toml` and every file-size EXCEPTIONS row are untouched.

## Measurements (decide-then-build evidence)

**M-1 — H-02 applied, not re-derived: option A.** The seven UDFs live in
`repark-functions` under reserved internal names
(`__iceberg_system_bucket`, `__iceberg_system_truncate` this round),
registered once from `register_all`; the `<cat>.system.<fn>` rewrite lands in
WO-3. The `iceberg` dependency is an external crate, invisible to
`check-crate-dag` (`crate-dag: 22 internal edges clean`, same count as the
closed ledger sample). No `repark-core` → `repark-functions` edge exists.

**M-2 — H-06 applied, not re-derived: width binds at invoke.** The width must
be a `ColumnarValue::Scalar` of Int8/Int16/Int32/Int64 in `1..=i32::MAX`; the
invoke builds `Transform::Bucket(n)` / `Transform::Truncate(n)` and calls the
fork's `create_transform_function`, then `transform(value_array)` on the value
array only. Width-first order only; an array width (the old column-first
order) refuses. Zero and in-`u32` over-wide widths refuse with the fork
`Bucket::new` / `Truncate::new` text; negatives and huge widths reuse the same
Java-shaped message. No hash, floor or truncation is re-implemented (T-1).

**M-3 — RePark reads BINARY columns as Arrow `large_binary`.** Probe on the
release module (2026-09-20): `SELECT b` over a one-column BINARY Iceberg table
answers Arrow `large_binary` with Spark type `BinaryType()`. The fork's
truncate preserves the input layout, so `truncate(1, b)` answers
`large_binary` / `BinaryType()` with values `[b'\x01', b'\xff', None]`. The
C-008 pin asserts that layout; it still distinguishes binary from string and
int, which is what H-04 needs.

**M-4 — refusal class and texts, measured.** Probe on the release module
(2026-09-20): `__iceberg_system_bucket(0, id)` raises
`repark.errors.PySparkException: Execution error: DataInvalid => Invalid
number of buckets: 0 (must be > 0)`; `__iceberg_system_bucket(id, 16)` raises
`PySparkException: Execution error: '__iceberg_system_bucket' expects the
width as an integer scalar literal, got a column`. The C-010 pins assert the
class and the fork-needle substrings.

**M-5 — UTC on both sides.** The inventory harness runs Spark with `TZ=UTC`
and `spark.sql.session.timeZone=UTC`; RePark's
`DEFAULT_SESSION_TIME_ZONE` is `UTC`. The fixture sets the zone explicitly
through the builder, so the `F-BUCKET-DATE` timestamp leg cannot drift with
the box zone.

**M-6 — H-01 applied, not re-derived: `days()` is DATE.** The UDF wraps
`create_transform_function(&Transform::Day)` and returns its `Date32` output
touched by nothing; `years` / `months` / `hours` return `Int32`. The fork
enum comment claiming Day returns int is stale — read at the pinned rev,
`Day.transform` builds a `Date32Array` for Timestamp(us), Timestamp(ns) and
Date32 inputs alike, matching Java `DaysFunction.resultType = DateType`.
`hours` takes timestamps only (the fork refuses `Date32` for Hour loudly);
the other three take date or timestamp.

**M-7 — H-07 applied: version text is the compiled crate version, unpinned.**
The fork exposes no version API at the pinned rev, so `iceberg_version()`
reports `env!("CARGO_PKG_VERSION")` (today `1.4.2`) — a stable non-null
string that is not Java's `1.11.0`. The UDF is a nullary UTF8 scalar
evaluated per row, refusing extra arguments; the C-016 pin asserts
non-nullness per row, stability across rows, and the string schema, never
the text.

**M-8 — the Day-recast mutation bites (H-01 self-check).** With `Day` output
temporarily recast to `Int32` in both `output_type` and invoke,
`test_days_pins_recorded_values_and_schema` fails on `int32 vs date32`
(values `[[19787,-1,null]]` — the right day counts, the wrong type) while
the NULL pin stays green; reverted before the gate, tree verified clean.

## PROPOSITION LEDGER — ICE-SYSTEM-FUNCTIONS-1 — 2026-09-20

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `__iceberg_system_bucket(16, id)` answers `[[4],[5],[null]]` as INT. | `test_bucket_long_pins_recorded_values` green offline and live. | **PROVEN** | Exact recorded `F-BUCKET-LONG` values on the Arrow path, Arrow type `int32`. pins: ice-system-functions-1/C-001 |
| C-002 | `__iceberg_system_bucket(16, data)` answers `[[5],[7],[null]]`. | `test_bucket_string_pins_recorded_values` green offline and live. | **PROVEN** | Exact recorded `F-BUCKET-STRING` murmur values, Arrow type `int32`. pins: ice-system-functions-1/C-002 |
| C-003 | `__iceberg_system_bucket(8, d), __iceberg_system_bucket(8, ts)` answers `[[0,2],[5,2],[null,null]]`. | `test_bucket_date_and_timestamp_pin_recorded_values` green offline and live. | **PROVEN** | Exact recorded `F-BUCKET-DATE` values under the M-5 UTC fixture, both `int32`. pins: ice-system-functions-1/C-003 |
| C-004 | `__iceberg_system_bucket(8, dec), __iceberg_system_bucket(8, b)` answers `[[0,6],[5,5],[null,null]]`. | `test_bucket_decimal_and_binary_pin_recorded_values` green offline and live. | **PROVEN** | Exact recorded `F-BUCKET-DECIMAL-BINARY` values, both `int32`. pins: ice-system-functions-1/C-004 |
| C-005 | `__iceberg_system_truncate(2, data)` answers `[["ab"],["z"],[null]]`, and a string shorter than the width returns whole. | `test_truncate_string_pins_recorded_values` and `test_truncate_string_shorter_than_width_returns_whole` green offline and live. | **PROVEN** | Exact recorded `F-TRUNCATE-STRING` values including the `"z"` whole-return, Arrow type `string`. pins: ice-system-functions-1/C-005 |
| C-006 | `__iceberg_system_truncate(10, id)` answers `[[-10],[0],[null]]`, and a negative floors down. | `test_truncate_long_pins_recorded_values` and `test_truncate_long_negative_floors_down` green offline and live. | **PROVEN** | Exact recorded `F-TRUNCATE-LONG` values including the `-10` floor, Arrow type `int64`. pins: ice-system-functions-1/C-006 |
| C-007 | `__iceberg_system_truncate(100, dec)` answers `[-6, 12]` as `decimal(10,2)`. | `test_truncate_decimal_pins_recorded_values_and_schema` green offline and live. | **PROVEN** | Exact recorded `F-TRUNCATE-DECIMAL` values, Arrow `decimal128(10,2)` and `df.schema` `DecimalType(10,2)`. pins: ice-system-functions-1/C-007 |
| C-008 | `__iceberg_system_truncate(1, b)` answers `[0x01, 0xff]` as binary. | `test_truncate_binary_pins_recorded_values_and_schema` green offline and live. | **PROVEN** | Exact recorded `F-TRUNCATE-BINARY` values, `df.schema` `BinaryType()`, Arrow `large_binary` per M-3. pins: ice-system-functions-1/C-008 |
| C-009 | NULL in gives NULL out for every value-taking function in round 1. | `test_every_function_returns_null_for_null_input` green offline and live. | **PROVEN** | One query over the all-NULL row asserts NULL from all ten bucket/truncate calls. pins: ice-system-functions-1/C-009 |
| C-010 | Zero and negative widths refuse with the fork text; the old column-first order refuses. | `test_bucket_width_must_be_a_positive_literal` and `test_bucket_old_argument_order_refuses` green offline and live. | **PROVEN** | `PySparkException` with the M-4 texts: the fork `Invalid number of buckets` / `Invalid truncate width` needles and the scalar-literal refusal. pins: ice-system-functions-1/C-010 |
| C-011 | The mechanics land as ruled: internal names registered once from `register_all`, `lib.rs` at 181 of the 182 ceiling, the external `iceberg` edge, H-06 width binding. | The diff; `check_lib_rs`, `check_crate_dag`, `check_rust_file_size` clean; the gate green. | **PROVEN** | `lib.rs` grows by exactly `pub mod iceberg_system;` + `iceberg_system::register(ctx);` (179 → 181 ≤ 182); `Cargo.toml` adds `iceberg.workspace = true`; H-02/H-06 applied per M-1/M-2. pins: ice-system-functions-1/C-011 |
| C-012 | `__iceberg_system_years(ts)` / `(d)` answers `[[-1,-1],[54,54],[null,null]]`, and a 1969 instant gives `-1`. | `test_years_pins_recorded_values` and `test_years_pre_epoch_is_negative` green offline and live. | **PROVEN** | Exact recorded `F-YEARS` values, both `int32`. pins: ice-system-functions-1/C-012 |
| C-013 | `__iceberg_system_months(ts)` / `(d)` answers `[[-1,-1],[650,650],[null,null]]`. | `test_months_pins_recorded_values` green offline and live. | **PROVEN** | Exact recorded `F-MONTHS` values, both `int32`. pins: ice-system-functions-1/C-013 |
| C-014 | `__iceberg_system_days(ts)` / `(d)` answers the recorded dates as DATE. | `test_days_pins_recorded_values_and_schema` green offline and live. | **PROVEN** | Exact recorded `F-DAYS` values, Arrow `date32` and `df.schema` `DateType()` on both legs per M-6; the M-8 recast mutation fails this pin. pins: ice-system-functions-1/C-014 |
| C-015 | `__iceberg_system_hours(ts)` answers `[[-1],[474898],[null]]`. | `test_hours_pins_recorded_values` green offline and live. | **PROVEN** | Exact recorded `F-HOURS` values, Arrow type `int32`. pins: ice-system-functions-1/C-015 |
| C-016 | `__iceberg_system_iceberg_version()` is a stable non-null string on every row. | `test_iceberg_version_is_non_null_per_row` green offline and live. | **PROVEN** | Three equal non-null strings, Arrow `string`, `df.schema` `StringType()`, `IS NOT NULL` true on all rows; the text itself unpinned per M-7. pins: ice-system-functions-1/C-016 |
| C-017 | NULL in gives NULL out for the four temporal functions. | `test_every_function_returns_null_for_null_input` green offline and live. | **PROVEN** | The NULL-row query grows by `years` / `months` / `days` (timestamp and date legs) / `hours` calls, all NULL; `iceberg_version` stays excluded as nullary per H-04. pins: ice-system-functions-1/C-017 |

## Gates

| Command | Result |
|---|---|
| lane gate `local-gate.sh xb-sysfn repark-functions python/repark/tests/test_ice_system_functions_1.py` (round 1) | `CB=0 R=0 T=0 U=0 L=0`, pytest `13 passed` offline and live |
| lane gate, same command (round 2) | `CB=0 R=0 T=0 U=0 L=0` |
| `cargo test -p repark-functions` (gate tier T, round 2) | `827 passed; 0 failed; 1 ignored` |
| pytest offline (gate tier U, round 2) | `19 passed in 0.95s` |
| pytest under `REPARK_PARITY_LIVE=1` (gate tier L, round 2) | `19 passed in 0.72s` |
| `cargo clippy -p repark-functions --all-targets -- -D warnings -A clippy::disallowed_methods` (Makefile `rust-clippy` flags) | clean |
| `cargo clippy -p repark-functions --lib -- -D warnings` (panic-ban shape) | clean |
| `cargo fmt -p repark-functions -- --check` | clean |
| `ruff check` + `ruff format --check` on the new test file | `All checks passed!`, `1 file already formatted` |
| `scripts/check_lib_rs.py` | `10 crate roots clean`, `lib.rs` at 181 of 182 |
| `scripts/check_rust_file_size.py` | `722 files clean`, `iceberg_system.rs` 409 of the 1000 default (round 2) |
| `scripts/check_crate_dag.sh` | `22 internal edges clean` |
| `comment_ban.py /tmp/xb-sysfn origin/main` | `hits=0` |
| `scripts/check_docstring_presence.py` / `scripts/check_python_conventions.py` | `295 files clean` / `363 files clean` |

## COVERAGE_ATTESTATION

```
COVERAGE_ATTESTATION:
  pr_unit: ice-system-functions-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause walked against a run — eight recorded-value pins on the Arrow path value AND type (C-001..C-008), the ten-call NULL row (C-009), the fork-text and old-order refusals (C-010), the mechanical diff (C-011); round 2 adds the four temporal value pins plus the date schema pin (C-012..C-015), the version stability pin (C-016) and the five-call temporal NULL extension (C-017).
      artifacts: [python/repark/tests/test_ice_system_functions_1.py, crates/repark-functions/src/iceberg_system.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries — width 0, width -1, negative truncate input, short string, NULL row, NULL-typed value array, scalar value input, over-i32 widths; round 2 adds the pre-epoch negative legs, the timestamp/date input pair per temporal, and the nullary version shape.
      artifacts: [python/repark/tests/test_ice_system_functions_1.py, crates/repark-functions/src/iceberg_system.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The maths stays in the fork by construction — one Transform build plus create_transform_function per invoke; a RePark re-implementation would have to diverge from the recorded murmur/floor values the pins assert exactly.
      artifacts: [crates/repark-functions/src/iceberg_system.rs, python/repark/tests/test_ice_system_functions_1.py]
    - id: AT-4
      status: N/A
      justification: Pure scalar kernels over one batch; no shared state, no lock, no ordering.
    - id: AT-5
      status: N/A
      justification: No privileged action, secret or injection surface; width binds from a typed scalar.
    - id: AT-6
      status: ATTACKED
      evidence: Registration runs once from register_all under reserved internal names; no bare bucket/truncate/years/months/days/hours/iceberg_version reaches the registry; round 2 adds zero lib.rs lines.
      artifacts: [crates/repark-functions/src/lib.rs, crates/repark-functions/src/iceberg_system.rs]
    - id: AT-7
      status: N/A
      justification: No hot-path change; one UDF invoke per call site, no added allocation beyond the fork transform output.
    - id: AT-8
      status: ATTACKED
      evidence: One external dependency added (iceberg, already pinned in the workspace lock); DAG, lib-rs and file-size gates re-run clean.
      artifacts: [crates/repark-functions/Cargo.toml]
    - id: AT-9
      status: ATTACKED
      evidence: Refusal texts pinned verbatim — the fork Invalid-needle messages and the scalar-literal message, all under PySparkException.
      artifacts: [python/repark/tests/test_ice_system_functions_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Every recorded value traces to the inventory dump /tmp/oc-worker/qe/cells/ipi-29-system-functions.txt and the cells_misc.py fixture, replayed on a UTC memory catalog.
      artifacts: [python/repark/tests/test_ice_system_functions_1.py]
```

Every clause above is PROVEN with a quoted command and its output; no clause is
OPEN. Touched files per clause: C-001..C-010, C-012..C-017
`python/repark/tests/test_ice_system_functions_1.py` +
`python/repark/tests/map.md` + `crates/repark-functions/src/iceberg_system.rs`;
C-011 `crates/repark-functions/src/lib.rs` + `crates/repark-functions/Cargo.toml`
+ `crates/repark-functions/map.md` + `crates/repark-functions/src/map.md` plus
this ledger's own entry in `task/ledgers/staging/map.md`.
`Cargo.lock` gains the one-line `repark-functions → iceberg` edge (committed
separately after the gate). `.github/` / `session.rs` / `router.rs` /
`repark-spark` / `STATUS.md`: untouched. Whole-workspace `cargo test`,
`make verify`, the facade suite and the parity harness not run per the brief's
machine rule; the lane gate is the proof.
