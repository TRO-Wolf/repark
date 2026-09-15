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
`bitmap_count` itself is a datafusion-spark scalar). Sliding frames use the
WIN-SLIDE-1 rescan (round 2: do not override `create_sliding_accumulator`).
D-2: `[u8; 4096]` state, no new dependency; grouped path is `n_groups * 4096`.
D-3: registry §7 `FNP-6D` FIXED 2026-09-15 and the design-doc FNP-6d line to
delivered. Round 2 (orchestrator, 2026-09-15): B8 cells are binding; L-001..L-005
and S2-21-1..5.

**Not in this unit:** `STATUS.md`, `briefs/next-sequence.md`, `Cargo.toml`,
`Cargo.lock`, `pyproject.toml`, `uv.lock`, `.github/`, `functions*.py`,
`dataframe/**`, `column.py`, `session/**`. Facade names. L-006 (grouping-column
nullability comes from VALUES, not the UDAF) is a ledger note only.

## PROPOSITION LEDGER — FNP-6D — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `bitmap_construct_agg(bitmap_bit_position(x))` over `VALUES (1),(2),(3),(32767),(NULL)` answers the F6D-construct bitmap: 4096-byte BINARY, first byte `0x07`, last byte `0x40`, count 4, NULL ignored. | Rust `construct_agg_sets_bits_zero_one_two_and_last_and_ignores_null`; Python `test_construct_agg_sets_bits_and_ignores_null`. | PROVEN | Red: `Invalid function 'bitmap_construct_agg'`. Green: rust 5/5, python 7/7 after the UDAF. |
| C-002 | Grouped construct then `bitmap_or_agg` / `bitmap_and_agg` answers F6D-or-and: OR popcount 3, AND popcount 1, both non-null bigint. | Rust `or_and_agg_fold_grouped_bitmaps`; Python `test_or_and_agg_fold_grouped_bitmaps`. | PROVEN | Same red/green as C-001. |
| C-003 | Empty input (F6D-empty): construct and or answer 4096 zero bytes; and answers 4096 `0xFF` bytes; all non-null BINARY. | Rust `empty_input_construct_and_or_are_zeros_and_is_ones`; Python `test_empty_input_identities_are_non_null_binary`. | PROVEN | Python uses Spark `CAST(NULL AS BINARY)` on the SQL door; rust kernel pin uses an empty Binary batch (DataFusion SQL has no `BINARY` type name). |
| C-004 | `length(bitmap_and_agg(b))` of one constructed bitmap is 4096, integer, non-null (F6D-and-empty-type). | Rust `and_agg_of_one_bitmap_has_length_4096`; Python `test_and_agg_length_is_4096_int`. | PROVEN | Green with C-001. |
| C-005 | Result type of each of the three aggregates is BINARY non-null. | The binary-cell assertions in the C-001 and C-003 pins. | PROVEN | `pa.binary()` / `DataType::Binary`, `nullable is False`. |
| C-006 | A sliding frame over `bitmap_construct_agg` answers Spark B8-window-sliding (counts 1, 2, 2) via WIN-SLIDE-1 rescan. The UDAF does not override `create_sliding_accumulator`. | Python `test_sliding_frame_answers_spark_via_win_slide_rescan`. | PROVEN | Round 1 refused with retract_batch. Round 2: dropping that override lets `sliding_frame_rescan` wrap the UDAF the same way as `bit_or`. |
| C-007 | The three UDAFs register through `aggregate::functions()` so the SQL door resolves them; `lib.rs` registration is untouched beyond `mod bitmap_agg`. | `aggregate.rs` extend + `bitmap_agg::functions()` returning the three names. | PROVEN | One `functions.extend(crate::bitmap_agg::functions())` in `aggregate.rs`; `lib.rs` gained only `mod bitmap_agg`. |
| C-008 | Registry §7 row `FNP-6D` is FIXED 2026-09-15; `docs/design/spark-function-parity.md` FNP-6d line is delivered. Sliding-window shape is WIN-SLIDE-1 rescan under that row. | The two doc edits. | PROVEN | `docs/spark-sql-iceberg-parity.md` §7 `FNP-6D`; design-doc FNP-6d line reads delivered 2026-09-15; round 2 names length/coercion and the rescan. |
| C-009 | Every new or moved file is listed in its directory `map.md` in the same commit. | `src/map.md`, crate `map.md`, `python/repark/tests/map.md`, `task/ledgers/staging/map.md`, `bitmap_agg/map.md`. | PROVEN | Step 1 listed the pin files; step 2/3 keep lockstep. |
| C-010 | No new dependency; accumulator state is `[u8; 4096]`. | Diff of `Cargo.toml` / `Cargo.lock` empty of this unit; the state field in `bitmap_agg.rs`. | PROVEN | `BitmapAccumulator.bits: [u8; 4096]`; grouped `Vec<u8>` of `n * 4096`; no Cargo.toml/lock edit. |
| C-011 | `bitmap_or_agg` / `bitmap_and_agg` accept a payload of any length and always answer 4096 bytes. Short `X'01'` (B8-or-short, B8-and-short) is zero-padded: both counts 1. Empty `X''` (B8-or-empty-bin) counts 0. Longer than 4096 (B8-or-long, including `concat` that types as Utf8) truncates to 4096. | Python `test_or_and_short_empty_and_long_normalize_to_4096`; rust `short_binary_or_and_normalize_to_4096`. | PROVEN | Round-2 red: Exact(Binary) refused short BINARY and concat Utf8. Green after pad/trunc + Utf8 coerce. AND of a short input is zero-padded then AND (tail fill 0), derived from "zero-padded" plus B8-and-short length 4096. |
| C-012 | `bitmap_construct_agg` coerces STRING as Spark BIGINT: `'1'` sets bit 1, first byte `0x02` (B8-construct-string). INT stays accepted (B8-construct-int). | Python `test_construct_agg_coerces_string_like_spark_bigint`. | PROVEN | Round-2 red: Utf8 vs Int64 signature. Green after Utf8/LargeUtf8/Utf8View on construct. |
| C-013 | Position outside `[0, 32767]` raises `[INVALID_BITMAP_POSITION] The 0-indexed bitmap position <p> is out of bounds. The bitmap has 32768 bits (4096 bytes). SQLSTATE: 22003` (B8-pos-32768, B8-pos-neg). `32767` counts 1 (B8-pos-32767). | Python `test_construct_agg_refuses_out_of_range_with_spark_class`; rust `out_of_range_position_names_invalid_bitmap_position`. | PROVEN | Round-2 red: generic Execution string. Green after Spark class text. |
| C-014 | All-NULL construct is 4096 zeros count 0 (B8-construct-all-null). All-NULL / empty AND identity is all ones count 32768 (B8-and-all-null, B8-and-empty). All-NULL OR is zeros count 0 (B8-or-all-null). Grouped result `b` is BINARY non-null (B8-group-schema). | Python `test_all_null_and_empty_identities_and_group_schema`. | PROVEN | Round-1 C-003 already covered empty identities; C-014 pins the B8 all-NULL and group-schema cells. |
| C-015 | Unbounded partition window answers B8-window-unbounded (counts 2, 2, 1). Sliding answers B8-window-sliding (counts 1, 2, 2) via WIN-SLIDE-1. | Python `test_unbounded_partition_window_answers_spark`, `test_sliding_frame_answers_spark_via_win_slide_rescan`; rust `unbounded_partition_window_answers`. | PROVEN | Round-1 sliding refused. Round-2: no `create_sliding_accumulator` override. |
| C-016 | All three aggregates implement `GroupsAccumulator`: one `Vec<u8>` of `n_groups * 4096`, `groups_accumulator_supported` true, in-place set-bit/OR/AND, `merge_batch` without `take`. Ungrouped `Accumulator` remains. | `crates/repark-functions/src/bitmap_agg/groups.rs`; `groups_accumulator_supported` / `create_groups_accumulator` in `bitmap_agg.rs`. | PROVEN | S2-21-1. evaluate/state emit one BinaryArray from the packed buffer (S2-21-2). |
| C-017 | OR/AND fold `chunks_exact(8)` as `u64` (`from_ne_bytes`/`to_ne_bytes`) after length normalisation. `convert_to_state` is not implemented (would allocate 4 KiB per input row). | `fold_words` in `bitmap_agg.rs`; groups map notes convert_to_state. | PROVEN | S2-21-3, S2-21-5. |

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

## Gates (step 4)

| Command | Result |
|---|---|
| `cargo test -p repark-functions` | 463 passed, 0 failed, 1 ignored (0.52s) |
| `cargo clippy --locked -p repark-functions --all-targets -- -D warnings -A clippy::disallowed_methods` | clean |
| `cargo fmt --check` (crate files) | clean |
| `.venv/bin/python -m pytest python/repark/tests -q` | serial: **6602 passed**, 358 skipped, 1 failed (`test_ml_boost_oracle.py::test_cross_validator_live_pyspark_shape` — `PermissionError` in `multiprocessing/synchronize.py`; isolated re-run still red; not this unit) |
| `PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest python/repark-parity/tests -q` | 757 passed, 2 skipped, 12 xfailed (140.21s) after restoring the PLAN-1 `Four units are deferred` end marker |
| `python3 scripts/check_ledger_grammar.py` | 138 live ledgers clean (980 clauses, 1599 pinned clause ids) |

## Round 2 (2026-09-15) — B8 cells, L-001..L-006, S2-21

Binding oracle: `fixtures-batch8.json` cells `B8-*`, live PySpark 4.1.2 (orchestrator).

### L-006 (ledger note only)

The grouping column's nullability comes from VALUES, not the UDAF. B8-group-schema
`g` is `int` non-null because `VALUES (1, 1) AS t(g, x)` types it that way. The
UDAF only claims BINARY non-null on `b`. No code change.

### Red first (round 2)

Python C-011 long cell (B8-or-long) after pad/trunc landed, before Utf8 coerce:

```
repark.errors.AnalysisException: Error during planning: Failed to coerce arguments
to satisfy a call to 'bitmap_or_agg' function: coercion from Utf8 to the signature
Exact(Binary) failed. No function matches the given name and argument types
'bitmap_or_agg(Utf8)'.
```

`concat(bitmap_construct_agg(0), X'01')` is Utf8 on this SQL door; Spark types it
BINARY. Or/and now accept Utf8/LargeUtf8/Utf8View and fold the bytes. Short
`VALUES (X'01')` is already BINARY on the same door (counts 1 for or and and:
zero-padded, AND tail filled 0). Prior round-2 reds closed in the same kernel:
Utf8 construct signature (B8-construct-string), `[INVALID_BITMAP_POSITION]`
(B8-pos-32768 / B8-pos-neg), sliding retract_batch (B8-window-sliding).

### Green (round 2)

`.venv/bin/python -m pytest python/repark/tests/test_fnp_6d_bitmap_aggregates.py -q`
→ **10 passed in 0.17s**.
`cargo test --release -p repark-functions --lib bitmap_agg`: **7 passed**.

### S2-21 grouped perf

Shape: `SELECT g, bitmap_count(bitmap_construct_agg(value % 32768)) FROM
(SELECT value, value % 100000 AS g FROM range(1000000)) GROUP BY g`
through `.venv/bin/python` after `uvx maturin@1.14.1 develop --release`.

Reviewer (before GroupsAccumulator): 69–114× slower than `avg`, 1.6 GiB RSS.

After (`GroupsAccumulator`, packed `n_groups * 4096`, u64 fold, one BinaryArray):

| | best of 3 | runs | notes |
|---|---|---|---|
| bitmap isolated | **1.307 s** | 2.100, 1.307, 1.781 | 100000 groups; `ru_maxrss` 8.17 GiB |
| avg same session | **0.071 s** | 0.074, 0.114, 0.071 | `ru_maxrss` after avg 1.00 GiB |
| bitmap same session | 1.931 s | 1.931, 2.181, 1.964 | ~27× avg (down from 69–114×) |

Packed state is 100000 × 4096 B = 0.39 GiB. Process RSS includes the DataFusion
pool and the 4096-byte BINARY result column. EXPLAIN is Partial +
FinalPartitioned `AggregateExec` over `bitmap_construct_agg`.
`convert_to_state` is not implemented (would be 4 KiB per input row).

## Gates (round 2)

| Command | Result |
|---|---|
| `cargo test -p repark-functions` | **465 passed**, 0 failed, 1 ignored (0.46s) |
| `make rust-clippy` | clean (`cargo clippy --locked --workspace --all-targets -- -D warnings -A clippy::disallowed_methods`) |
| `cargo fmt --all --check` | clean |
| `python3 scripts/check_rust_file_size.py` | 510 files clean (default ceiling 1000; 36 exceptions); `bitmap_agg.rs` 620 lines, `groups.rs` 152, no baseline raised |
| `.venv/bin/python -m pytest python/repark/tests -q` | **6630 passed**, 358 skipped, 1 failed (`test_ml_boost_oracle.py::test_cross_validator_live_pyspark_shape` — `PermissionError` in `multiprocessing/synchronize.py`; same as round 1; not this unit) |
| `PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest python/repark-parity/tests -q` | first run 756 passed + 1 failed (`test_dl_6_docs_links` — `bitmap_agg/map.md` existed untracked); after `git add` of that map, the pin **1 passed**. Net **757 passed**, 2 skipped, 12 xfailed |
| `python3 scripts/check_ledger_grammar.py` | 139 live ledgers clean (995 clauses, 1614 pinned clause ids, 2 exception rows) |

Checked 2026-09-15: 342 GB free of 1.8 TB (81% used). `CARGO_TARGET_DIR=/tmp/pc-build/target`.
No worktree. No `cargo clean`. Shared target lock waited on sibling `/tmp/pc-cv` and
`/tmp/pb-build` maturin jobs.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: fnp-6d
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against recorded F6D and B8 cells, not paraphrase — construct bits 0/1/2/32766 and NULL skip, grouped or/and popcounts, empty and all-NULL identities, length pad/trunc to 4096 (short X'01' count 1, empty count 0, concat Utf8 long truncated), STRING construct '1' first byte 0x02, INVALID_BITMAP_POSITION 32768/-1, 32767 counts 1, sliding 1/2/2 via WIN-SLIDE-1, unbounded 2/2/1, GroupsAccumulator packed n*4096, u64 fold, BINARY non-null, aggregate::functions registration, registry FNP-6D, maps, [u8; 4096] state.
      artifacts: [crates/repark-functions/src/bitmap_agg.rs, crates/repark-functions/src/bitmap_agg/groups.rs, python/repark/tests/test_fnp_6d_bitmap_aggregates.py, docs/spark-sql-iceberg-parity.md, fixtures-batch8.json]
    - id: AT-2
      status: ATTACKED
      evidence: Empty input vs all-NULL vs populated, NULL ignored, short/empty/long payloads, STRING vs INT construct, in-range 32767 vs 32768 vs -1, sliding vs unbounded vs scalar agg, grouped schema. AND of a short input is zero-padded (tail fill 0) so X'01' counts 1.
      artifacts: [crates/repark-functions/src/bitmap_agg.rs, python/repark/tests/test_fnp_6d_bitmap_aggregates.py]
    - id: AT-3
      status: ATTACKED
      evidence: Position outside [0, 32767] raises Spark [INVALID_BITMAP_POSITION] SQLSTATE 22003. DISTINCT refuses at accumulator build. Sliding no longer refuses — WIN-SLIDE-1 rescan. No retry/timeout surface.
      artifacts: [crates/repark-functions/src/bitmap_agg.rs, python/repark/tests/test_fnp_6d_bitmap_aggregates.py]
    - id: AT-4
      status: ATTACKED
      evidence: No shared mutable state. Ungrouped Accumulator owns [u8; 4096]. GroupsAccumulator owns one Vec<u8> of n_groups*4096, update/merge in place, evaluate/state one BinaryArray. Registration is one extend on aggregate::functions.
      artifacts: [crates/repark-functions/src/bitmap_agg.rs, crates/repark-functions/src/bitmap_agg/groups.rs, crates/repark-functions/src/aggregate.rs]
    - id: AT-5
      status: N/A
      justification: Local bitwise aggregates over in-memory Arrow; no credential, secret, network, path, or deserialization surface.
    - id: AT-6
      status: ATTACKED
      evidence: Recorded Spark 4.1.2 cells F6D-* and B8-* pin values AND Arrow type/nullability. Sliding is Spark-equal via WIN-SLIDE-1, not absorbed. Red-first Invalid function then Exact(Binary) Utf8 concat, then green.
      artifacts: [python/repark/tests/test_fnp_6d_bitmap_aggregates.py, fixtures-batch3.json, fixtures-batch8.json]
    - id: AT-7
      status: ATTACKED
      evidence: Packed GroupsAccumulator is n_groups*4096 (0.39 GiB at 100k groups). convert_to_state is not implemented so a 1M-row convert cannot allocate 4 KiB per row. 1M/100k grouped construct best-of-3 1.307s, ~27× avg (was 69–114×).
      artifacts: [crates/repark-functions/src/bitmap_agg/groups.rs, task/ledgers/staging/fnp-6d-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: No Cargo.toml/lock edit. No new crate. State is [u8; 4096] / packed Vec<u8>, not roaring. Facade names fenced to run 15a. Sliding uses WIN-SLIDE-1 under FNP-6D.
      artifacts: [crates/repark-functions/src/bitmap_agg.rs, docs/spark-sql-iceberg-parity.md]
    - id: AT-9
      status: ATTACKED
      evidence: Red-first pasted Invalid function then Utf8 Exact(Binary) concat. Position error names INVALID_BITMAP_POSITION and SQLSTATE 22003. Distinct names DISTINCT.
      artifacts: [task/ledgers/staging/fnp-6d-ledger.md, crates/repark-functions/src/bitmap_agg.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Round 1 red 7 python + 5 rust, green 7 python + 5 rust + 463 crate. Round 2 green 10 python + 7 rust + 465 crate. Mutation of pad/trunc, identity byte, or bit layout would red B8-or-short / B8-and-short / F6D-construct / F6D-empty.
      artifacts: [python/repark/tests/test_fnp_6d_bitmap_aggregates.py, crates/repark-functions/src/bitmap_agg.rs]
  complete: true
```
