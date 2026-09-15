# Unit ledger — FNP-6D · Spark bitmap aggregates

**Date:** 2026-09-15 · **Branch:** `feat/fnp-6d-bitmap-aggregates` · **Base:** `e98e899d`
**Model:** grok-4.6 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when FNP-6D merges, or when the owner closes the slate row.

**Why now.** Card FNP-6D (run 15c): Spark `bitmap_construct_agg` / `bitmap_or_agg` /
`bitmap_and_agg` are missing on RePark (`Invalid function`). Recorded oracle
`fixtures-batch3.json` cells `F6D-*` (live PySpark 4.1.2). A bitmap is BINARY of
exactly 4096 bytes (32768 bits, least-significant first). SQL door only; facade
names are run 15a after this merges.

**G-2 rulings.** D-1: UDAFs live in `crates/repark-functions/src/bitmap_agg.rs`,
registered through `aggregate::functions()` (the SQL-door path `count_if` uses;
`bitmap_count` itself is a datafusion-spark scalar). Sliding frames refuse
loudly (WIN-SLIDE). D-2: `[u8; 4096]` state, no new dependency. D-3: registry
§7 `FNP-6D` FIXED 2026-09-15 and the design-doc FNP-6d line to delivered.

**Not in this unit:** `STATUS.md`, `briefs/next-sequence.md`, `Cargo.toml`,
`Cargo.lock`, `pyproject.toml`, `uv.lock`, `.github/`, `functions*.py`,
`dataframe/**`, `column.py`, `session/**`. Facade names. Out-of-range position
and non-4096 bitmap length are not in the fixture: the kernel refuses rather
than panic; Spark's error class is unmeasured.

## PROPOSITION LEDGER — FNP-6D — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `bitmap_construct_agg(bitmap_bit_position(x))` over `VALUES (1),(2),(3),(32767),(NULL)` answers the F6D-construct bitmap: 4096-byte BINARY, first byte `0x07`, last byte `0x40`, count 4, NULL ignored. | Rust `construct_agg_sets_bits_zero_one_two_and_last_and_ignores_null`; Python `test_construct_agg_sets_bits_and_ignores_null`. | PROVEN | Red: `Invalid function 'bitmap_construct_agg'`. Green: rust 5/5, python 7/7 after the UDAF. |
| C-002 | Grouped construct then `bitmap_or_agg` / `bitmap_and_agg` answers F6D-or-and: OR popcount 3, AND popcount 1, both non-null bigint. | Rust `or_and_agg_fold_grouped_bitmaps`; Python `test_or_and_agg_fold_grouped_bitmaps`. | PROVEN | Same red/green as C-001. |
| C-003 | Empty input (F6D-empty): construct and or answer 4096 zero bytes; and answers 4096 `0xFF` bytes; all non-null BINARY. | Rust `empty_input_construct_and_or_are_zeros_and_is_ones`; Python `test_empty_input_identities_are_non_null_binary`. | PROVEN | Python uses Spark `CAST(NULL AS BINARY)` on the SQL door; rust kernel pin uses an empty Binary batch (DataFusion SQL has no `BINARY` type name). |
| C-004 | `length(bitmap_and_agg(b))` of one constructed bitmap is 4096, integer, non-null (F6D-and-empty-type). | Rust `and_agg_of_one_bitmap_has_length_4096`; Python `test_and_agg_length_is_4096_int`. | PROVEN | Green with C-001. |
| C-005 | Result type of each of the three aggregates is BINARY non-null. | The binary-cell assertions in the C-001 and C-003 pins. | PROVEN | `pa.binary()` / `DataType::Binary`, `nullable is False`. |
| C-006 | A sliding frame over each of the three names refuses loudly (WIN-SLIDE: message contains `retract_batch` or `sliding`). | Rust `sliding_frame_refuses_loudly`; Python `test_sliding_frame_refuses_loudly`. | PROVEN | `create_sliding_accumulator` returns DataFusion's retract_batch NotImplemented. |
| C-007 | The three UDAFs register through `aggregate::functions()` so the SQL door resolves them; `lib.rs` registration is untouched beyond `mod bitmap_agg`. | `aggregate.rs` extend + `bitmap_agg::functions()` returning the three names. | PROVEN | One `functions.extend(crate::bitmap_agg::functions())` in `aggregate.rs`; `lib.rs` gained only `mod bitmap_agg`. |
| C-008 | Registry §7 row `FNP-6D` is FIXED 2026-09-15; `docs/design/spark-function-parity.md` FNP-6d line is delivered. Sliding-window shape is DECLARED under that row. | The two doc edits. | PROVEN | `docs/spark-sql-iceberg-parity.md` §7 `FNP-6D`; design-doc FNP-6d line reads delivered 2026-09-15. |
| C-009 | Every new or moved file is listed in its directory `map.md` in the same commit. | `src/map.md`, crate `map.md`, `python/repark/tests/map.md`, `task/ledgers/staging/map.md`. | PROVEN | Step 1 listed the pin files; step 2/3 keep lockstep. |
| C-010 | No new dependency; accumulator state is `[u8; 4096]`. | Diff of `Cargo.toml` / `Cargo.lock` empty of this unit; the state field in `bitmap_agg.rs`. | PROVEN | `BitmapAccumulator.bits: [u8; 4096]`; no Cargo.toml/lock edit. |

## Evidence

### Red first (step 1, 2026-09-15)

Python `.venv/bin/python -m pytest python/repark/tests/test_fnp_6d_bitmap_aggregates.py -q --tb=line`
→ **7 failed in 0.10s**. Every construct/or/and pin:

```
repark.errors.AnalysisException: Error during planning: Invalid function 'bitmap_construct_agg'.
Did you mean 'bitmap_count'?
```

Sliding pins (the refusal message is not yet the WIN-SLIDE needle):

```
AssertionError: Regex pattern did not match.
  Expected regex: re.compile('(?i)retract_batch|sliding', re.IGNORECASE)
  Actual message: "Error during planning: Invalid function 'bitmap_or_agg'.\nDid you mean 'bit_or'?"
```

(and the construct/and siblings: `Invalid function 'bitmap_construct_agg'` / `'bitmap_and_agg'`.)

Rust `cargo test --release -p repark-functions --lib bitmap_agg`:
**0 passed; 5 failed**. Same planning diagnostic:

```
plan: ... Invalid function 'bitmap_construct_agg'.
Did you mean 'bitmap_count'?
```

`sliding_frame_refuses_loudly` panicked: `got Error during planning: Invalid function 'bitmap_construct_agg'. Did you mean 'string_agg'?`

## Disk

### Green (step 2, 2026-09-15)

`cargo test --release -p repark-functions --lib bitmap_agg`: **5 passed**.
`.venv/bin/python -m pytest python/repark/tests/test_fnp_6d_bitmap_aggregates.py -q`: **7 passed in 0.11s**.
`cargo clippy --locked -p repark-functions --all-targets -- -D warnings -A clippy::disallowed_methods`: clean.
`cargo clippy --locked -p repark-functions --lib` panic-ban set: clean.

Checked 2026-09-15: 426 GB free of 1.8 TB (76% used). `CARGO_TARGET_DIR=/tmp/pc-build/target`.
No worktree. No `cargo clean`.
