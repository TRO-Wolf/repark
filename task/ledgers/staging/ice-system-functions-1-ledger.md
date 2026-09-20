# Charter ledger — ICE-SYSTEM-FUNCTIONS-1 · IPI-29 bucket + truncate UDFs, round 1 of 3

**Date:** 2026-09-20 · **Branch:** `fix/ipi-29-system-functions` · **Base:** `6d029ab8` (`origin/main`) · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Registry:** none this round; the parity row is filed when WO-3 delivers the catalog-qualified registration and SHOW.

**Retires:** WO-3 moves this ledger to `../completed/` in its last commit; WO-2 amends it in place with the temporal/version clauses.

**Scope:** round 1 only — new `crates/repark-functions/src/iceberg_system.rs`
(`bucket_udf`, `truncate_udf`), two lines in `repark-functions/src/lib.rs`, the
external `iceberg` dependency in `repark-functions/Cargo.toml`, new
`python/repark/tests/test_ice_system_functions_1.py`, three `map.md` files, this
ledger. No code comment added anywhere (`comment-ban hits=0`); Python docstrings
only where the presence gate needs them. Temporal functions, `iceberg_version`,
catalog-qualified registration and SHOW are WO-2/WO-3; `session.rs`,
`router.rs`, `repark-spark`, `STATUS.md`, every other `Cargo.toml` and every
file-size EXCEPTIONS row are untouched.

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
| C-009 | NULL in gives NULL out for every value-taking function this round. | `test_every_function_returns_null_for_null_input` green offline and live. | **PROVEN** | One query over the all-NULL row asserts NULL from all ten bucket/truncate calls. pins: ice-system-functions-1/C-009 |
| C-010 | Zero and negative widths refuse with the fork text; the old column-first order refuses. | `test_bucket_width_must_be_a_positive_literal` and `test_bucket_old_argument_order_refuses` green offline and live. | **PROVEN** | `PySparkException` with the M-4 texts: the fork `Invalid number of buckets` / `Invalid truncate width` needles and the scalar-literal refusal. pins: ice-system-functions-1/C-010 |
| C-011 | The mechanics land as ruled: internal names registered once from `register_all`, `lib.rs` at 181 of the 182 ceiling, the external `iceberg` edge, H-06 width binding. | The diff; `check_lib_rs`, `check_crate_dag`, `check_rust_file_size` clean; the gate green. | **PROVEN** | `lib.rs` grows by exactly `pub mod iceberg_system;` + `iceberg_system::register(ctx);` (179 → 181 ≤ 182); `Cargo.toml` adds `iceberg.workspace = true`; H-02/H-06 applied per M-1/M-2. pins: ice-system-functions-1/C-011 |

## Gates

| Command | Result |
|---|---|
| lane gate `local-gate.sh xb-sysfn repark-functions python/repark/tests/test_ice_system_functions_1.py` | `CB=0 R=0 T=0 U=0 L=0` |
| `cargo test -p repark-functions` (gate tier T) | `827 passed; 0 failed; 1 ignored` |
| pytest offline (gate tier U) | `13 passed in 0.44s` |
| pytest under `REPARK_PARITY_LIVE=1` (gate tier L) | `13 passed in 0.57s` |
| `cargo clippy -p repark-functions --all-targets -- -D warnings -A clippy::disallowed_methods` (Makefile `rust-clippy` flags) | clean |
| `cargo clippy -p repark-functions --lib -- -D warnings` (panic-ban shape) | clean |
| `cargo fmt -p repark-functions -- --check` | clean |
| `ruff check` + `ruff format --check` on the new test file | `All checks passed!`, `1 file already formatted` |
| `scripts/check_lib_rs.py` | `10 crate roots clean`, `lib.rs` at 181 of 182 |
| `scripts/check_rust_file_size.py` | `722 files clean`, new file 198 of the 1000 default |
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
      evidence: Each clause walked against a run — eight recorded-value pins on the Arrow path value AND type (C-001..C-008), the ten-call NULL row (C-009), the fork-text and old-order refusals (C-010), the mechanical diff (C-011).
      artifacts: [python/repark/tests/test_ice_system_functions_1.py, crates/repark-functions/src/iceberg_system.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries — width 0, width -1, negative truncate input, short string, NULL row, NULL-typed value array, scalar value input, over-i32 widths.
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
      evidence: Registration runs once from register_all under reserved internal names; no bare bucket/truncate reaches the registry.
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
OPEN. Touched files per clause: C-001..C-010
`python/repark/tests/test_ice_system_functions_1.py` +
`python/repark/tests/map.md` + `crates/repark-functions/src/iceberg_system.rs`;
C-011 `crates/repark-functions/src/lib.rs` + `crates/repark-functions/Cargo.toml`
+ `crates/repark-functions/map.md` + `crates/repark-functions/src/map.md` plus
this ledger's own entry in `task/ledgers/staging/map.md`.
`Cargo.lock` / `.github/` / `session.rs` / `router.rs` / `repark-spark` /
`STATUS.md`: untouched. Whole-workspace `cargo test`, `make verify`, the facade
suite and the parity harness not run per the brief's machine rule; the lane gate
is the proof.
