# Charter ledger — FNP-BITMAP-FACADE-1 · the Spark-facade names for the bitmap aggregates

**Date:** 2026-09-15 · **Branch:** `feat/fnp-bitmap-facade-1` · **Base:** `main`
23ba2e53 · **Model:** zai/glm-5.3-flash (runs 1-15a) · muse-spark-1.3-contributor (run 16a remediation) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** FNP-16 bitmap rows: none filed against the facade names; FNP-6D (#609) is
FIXED on the SQL door at the base commit and this unit binds its three names through the
existing aggregate path.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** FNP-6D landed the three bitmap aggregates as Rust UDAFs
(`crates/repark-functions/src/bitmap_agg.rs`) with SQL-door pins at 23ba2e53. The Spark
facade has none of the three names (`F.bitmap_construct_agg` / `F.bitmap_or_agg` /
`F.bitmap_and_agg` are absent from every installer and from the split-identity inventory at
base — statically checked, and the red run below shows the absence at the attribute level).
The owner's shape rule (2026-09-14): a name is done when it is present on `repark.spark.functions`
AND answers PySpark 4.1.2 on BOTH doors, with pins. The standing instruction is Rust-first: the
UDAFs exist; this unit is the thin wrapper round — three `unary_aggregate_udaf` arms plus
three one-line facade wrappers over `column._inner.aggregate(kind, False)`.

**Not in this unit:** any kernel change (`bitmap_agg.rs` is untouched); `collect_list`-style
collection aggregates; the SQL door (already pinned by
`python/repark/tests/test_fnp_6d_bitmap_aggregates.py`, green at base); the
`dataframe/**`, `column.py`, `session/**`, `catalog.py`, `types.py` or SQL-parser files
(card decision D-2 fences them).

**Round shape (owner, run 15a):** five commits — (1) ledger + fixture + red pins,
(2) the Rust arms, (3) the Python wrappers, (4) HALT while the orchestrator copied in the
release native built from 72fed3f5 (`python/repark/src/repark/_native.abi3.so`, git-ignored;
probe `bitmap_count(bitmap_construct_agg(bitmap_bit_position(x)))` over `VALUES (1), (2)`
answers 2), (5) the real red re-run, green pins, census and gates on the native-carrying tree.

## PROPOSITION LEDGER — FNP-BITMAP-FACADE-1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `bitmap_construct_agg(col)` / `bitmap_or_agg(col)` / `bitmap_and_agg(col)` are present on `repark.spark.functions` (installed through `functions_bitwise.INSTALL_NAMES`), each with PySpark 4.1.2's one-parameter `col` signature, and the aggregate result type on the Python door is non-null `binary` of exactly 4096 bytes equal to the SQL door's. | `test_fnp_bitmap_facade_1.py::test_names_present_with_pyspark_signature`, `::test_construct_agg_global_matches_sql_door_and_fixture`, `::test_construct_agg_grouped_schema_and_bytes`. | **PROVEN** | Red first: on the base facade (commit-1 tree, native present, the wrappers replaced by their b2c2b69e state) all 9 pins failed in 0.34 s — first error `AssertionError: bitmap_construct_agg … callable(None)` and `AttributeError: module 'repark.spark.functions' has no attribute 'bitmap_construct_agg'`; the earlier no-native collection error was an environment gap, not a red. Green after: 10 passed in 0.20 s (the call_function pin added in the same unit). Result types match the fixture (`b: binary not null`, 4096 bytes) on both doors; the grouped key's nullability is an input-door property (both doors answer the same, pinned) while the aggregate itself is non-null. pins: fnp-bitmap-facade-1/C-001 |
| C-002 | The Python door answers Spark-equal bytes and counts for every card shape: global and grouped `bitmap_construct_agg`, grouped/global `bitmap_or_agg` / `bitmap_and_agg`, NULL rows skipped, empty and all-NULL identities (OR all-zero, AND all-one), a STRING argument coerced like BIGINT, `bitmap_count` over each result, one sliding-window cell through `Window.rowsBetween`, and `F.call_function` routing for all three names — each equal to the SQL door's answer and to the recorded fixture cell. | `test_fnp_bitmap_facade_1.py`, against `fnp_bitmap_facade_1_spark_oracle.json` (F6D-\*/B8-\* cells copied verbatim from `/tmp/oc-worker/pc-oracle/fixtures-batch3.json` / `fixtures-batch8.json`, Spark 4.1.2). | **PROVEN** | RED on the base facade, recorded 2026-09-15 with the orchestrator's native installed: `.venv/bin/python -m pytest python/repark/tests/test_fnp_bitmap_facade_1.py -q -p no:cacheprovider` → `9 failed in 0.34s`, first error `E AssertionError: bitmap_construct_agg / E assert False / E + where False = callable(None)` (module-level names absent). GREEN after the wrappers on the native tree: `10 passed in 0.20s`; the two grouped pins needed a pin-side fix (a single-row helper used on two-row tables; a group-key nullability assertion that belongs to the input door, not the aggregate — both doors agree and the pin now says so). Example `docs/examples/functions/bitmap_aggregates.py` executed green under `--require-execute`. pins: fnp-bitmap-facade-1/C-002 |
| C-003 | Out-of-range positions (`32768`, `-1`) raise Spark's `[INVALID_BITMAP_POSITION]` through the Python door with the SQL door's message body (the 0-indexed position, the 32768-bit/4096-byte bounds, `SQLSTATE: 22003`), and `32767` still answers count 1. | `test_fnp_bitmap_facade_1.py::test_out_of_range_positions_raise_invalid_bitmap_position` against fixture cells `B8-pos-32768` / `B8-pos-neg` / `B8-pos-32767`. | **PROVEN** | Red on the base facade (C-002's run), green on the landed tree: both positions raise with the fixture's message body and the boundary answers 1. pins: fnp-bitmap-facade-1/C-003 |
| C-004 | Census: the three names install through `install_into` so `test_functions_split_identity.py`'s alias tail carries them and its total-length constant moves 6 → 9 on the alias segment; the byname allowlist covers them as facade-only routines with `F.call_function` bytes equal to the SQL door; an example exists at `docs/examples/functions/bitmap_aggregates.py` in the ReparkSession idiom and executes green; the EX-0 example-coverage count moves 1010 → 1013 with the backlog baseline unchanged at 112; the FNP-6D registry row records the facade landing; every touched directory's `map.md` is in lockstep. | The split-identity pin, the byname pin (`test_fnp_misc_1_byname_allowlist_covers_facade`), the EX-0 gate, `scripts/check_example_coverage.py --require-execute`, the registry, the map gates. | **PROVEN** | Split identity green (alias segment 6 → 9). `FACADE_ONLY_ROUTINE_NAMES` gained the three names (owner ruling: PySpark 4.1.2 resolves `call_function("bitmap_construct_agg", …)` as a builtin, so they are facade-only routines); `test_fnp_misc_1.py` 116 passed; `F.call_function("bitmap_construct_agg", F.bitmap_bit_position(F.col("x")))` bytes equal the wrapper's and the SQL door's (pinned). EX-0 recount 1010 → 1013 with `inventory.txt` regenerated and the example covering all three (backlog 112). Registry FNP-6D Rationale flipped with date and unit id. Seven maps in lockstep (`tests`, `spark`, `column`, `repark-functions/src`, `staging`, parity `tests`, `docs/examples/functions`). `STATUS.md` and `briefs/next-sequence.md` untouched. pins: fnp-bitmap-facade-1/C-004 |

| C-005 | The facade inherits the kernel refusal (G-2 R-1): `F.bitmap_or_agg` / `F.bitmap_and_agg` over INT, FLOAT, BOOLEAN and STRING columns raise the fixture's `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` class with the BINARY requirement, the Spark type name and `SQLSTATE: 42K09`, and `F.bitmap_construct_agg` over a BOOLEAN column raises the BIGINT form — the facade coerces nothing before the kernel. | `test_fnp_bitmap_facade_1.py::test_or_and_agg_refuse_non_binary_columns` (8 shapes) + `::test_construct_agg_refuses_boolean_column`, against `fnp_6d_followup_1_spark_oracle.json` `FU-*` conditions. | **PROVEN** | Green at first run on the fixed kernel (the 8-shape parametrize plus the BOOLEAN construct pin, inside the 27-passed file run). Red lineage: the critic's pre-fix reproduction answered 4096-byte ASCII bitmaps for the same shapes (L-001), so these pins redden on any facade-or-kernel coercion. pins: fnp-bitmap-facade-1/C-005 |
| C-006 | `F.call_function` routing is pinned per valid Spark shape against the fixture (G-2 R-2): construct over `bitmap_bit_position` vs the `F6D-construct` hex and count; or/and over a BINARY column vs the `F6D-or-and` counts — each compared to the fixture cell, not only across doors. The old single pin over the invalid or/and-on-bitpos shape is removed. | `test_fnp_bitmap_facade_1.py::test_call_function_construct_over_bit_position_matches_fixture` + `::test_call_function_or_and_over_binary_match_fixture`. | **PROVEN** | Red first on the merge head with the fresh release native: the old pin failed with `[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] ... requires the "BINARY" type ... has the type "BIGINT". SQLSTATE: 42K09` (the fixed kernel refusing the invalid shape through the facade). Split pins green after. pins: fnp-bitmap-facade-1/C-006 |
| C-007 | `F.bitmap_construct_agg` over STRING `'abc'` raises `CAST_INVALID_INPUT` (ANSI default, the door default) with the fixture's malformed-cast body and `SQLSTATE: 22018`, and over FLOAT 1.7 answers position 1 (G-2 R-3). | `test_fnp_bitmap_facade_1.py::test_construct_agg_malformed_string_raises_cast_invalid_input` + `::test_construct_agg_float_fraction_answers_position_one`, against `FU-construct-abc` / `FU-construct-float-frac`. | **PROVEN** | Green at first run on the fixed kernel. Red lineage: the critic's pre-fix reproduction answered the all-zero identity for `'abc'` and dropped FLOAT 1.0 to zeros (L-003). pins: fnp-bitmap-facade-1/C-007 |
| C-008 | The two truncated BINARY cells carry full hex payloads and the unpinned shapes are pinned on the Python door with Arrow field types against the fixture schema cells (G-2 R-4): `F6D-empty` full bytes and `binary not null` for construct/or/and; `B8-or-short` / `B8-and-short` / `B8-or-empty-bin` / `B8-or-long` length 4096 with `int32` length fields on both doors (short-fold counts 1 on both doors); `B8-window-unbounded` rows `[[1, 2], [1, 2], [2, 1]]` with `int32`/`int64` fields; full-bytes compares for the grouped and STRING-`'1'` cells. | `test_fnp_bitmap_facade_1.py` R-4 pins + the restored `fnp_bitmap_facade_1_spark_oracle.json` cells. | **PROVEN** | Red first: both cells were 80-char unterminated fragments failing `ast.literal_eval` (`SyntaxError: unterminated string literal`), so any full-bytes pin errored on the pre-fix fixture. Restoration: byte 0 `0x02` from the recorded prefix, the zero tail from the bitmap format with only bit 1 set, corroborated byte-for-byte against the kernel answer for `construct('1')`. Grouped-key nullability stays door-vs-door (input-door property). pins: fnp-bitmap-facade-1/C-008 |
| C-009 | The Python door's default name matches the fixture schema name, and the SQL-door qualifier leak is a registry-recorded expected divergence (G-2 R-5): unaliased `bitmap_construct_agg(x)` selects `bitmap_construct_agg(x)` on the Python door; `spark.sql` / `selectExpr` render `bitmap_construct_agg(datafusion.public.<view>.x)` — pinned as today's shape, owned by the SQL door (run 16c). | `test_fnp_bitmap_facade_1.py::test_default_name_matches_fixture_schema` + `::test_sql_door_qualifier_leak_is_expected_divergence`; registry row under FNP-6D in `docs/spark-sql-iceberg-parity.md`. | **PROVEN** | Green at first run; the leak shape measured live (`datafusion.public.fnp_facade1_name_leak.x`, `__repark_selx_<hex>` for `selectExpr`) and recorded exactly. pins: fnp-bitmap-facade-1/C-009 |
| C-010 | `functions.__all__` carries no duplicates (owner Q-15a-5 via G-2 R-6). | `test_functions_split_identity.py::test_every_all_name_resolves` gains `assert len(set(F.__all__)) == len(F.__all__)`. | **PROVEN** | Green in the same run as the split-identity suite. pins: fnp-bitmap-facade-1/C-010 |

| C-011 | `degrees` / `radians` compute on a Rust kernel with thin facade binds (owner Q-15a-1 via G-2 R-7): new `call_scalar` dispatch arms reuse the engine's `degrees` / `radians` scalar UDFs; the facade wrappers are `_scalar` binds over a `double` cast keeping the `DEGREES(x)` / `RADIANS(x)` display names, the `toDegrees` / `toRadians` warnings, and every existing `fnp_alias_1_spark_oracle.json` pin green; `_rescaled` and the factor constants are deleted. | `test_fnp_alias_1.py` (55 pins, values/names/warnings/joins). | **PROVEN** | Reuse, not a new kernel: the engine UDF is `f64::to_degrees` / `f64::to_radians`, a single multiply by the correctly-rounded factor — the same operation the replaced Python multiply performed (D-7 of FNP-ALIAS-1). Evidence: all 55 alias pins green on the rebuilt native (Spark-recorded doubles, names, `FutureWarning` texts, semi/anti and ON-join behavior); a 4015-value sweep of kernel vs `x * (180.0/pi)` / `x * (pi/180.0)` answers 0 mismatches for both names. pins: fnp-bitmap-facade-1/C-011 |

VERDICT: 11 clauses, 11 PROVEN, 0 OPEN, 0 REJECTED.

## Orchestrator rulings G-2 (run 16a) — dispositions

| Ruling | Content | Disposition |
|---|---|---|
| R-1 (G-2) | The facade inherits the kernel refusal; pin or/and over INT/FLOAT/BOOLEAN/STRING and construct over BOOLEAN. | Applied in C-005; no facade coercion exists (wrappers pass the column straight to `aggregate`), pins green at first run. |
| R-2 (G-2) | Split the `call_function` pin per valid shape, each vs the fixture. | Applied in C-006; old pin removed after going red on the fixed kernel. |
| R-3 (G-2) | Pin construct over `'abc'` (`CAST_INVALID_INPUT`, ANSI default) and over FLOAT 1.7 (position 1). | Applied in C-007; pins green at first run. |
| R-4 (G-2) | Restore the two truncated BINARY cells; pin `F6D-empty`, short/empty/long folds, unbounded window, Arrow types vs fixture schema. | Applied in C-008. |
| R-5 (G-2) | Pin the Python-door default name; record the SQL-door qualifier leak as a registry residual with a divergence pin. | Applied in C-009. |
| R-6 (G-2, owner Q-15a-5) | Add `assert len(set(functions.__all__)) == len(functions.__all__)` to `test_functions_split_identity.py`. | Applied in C-010. |
| R-7 (G-2, owner Q-15a-1) | Move `degrees` / `radians` onto a Rust kernel (reuse the engine UDFs when bit-exact, else a Spark-exact kernel); add dispatch arms; keep display names, warnings and alias pins green; delete `_rescaled` and the constants. | Applied in C-011 as reuse: arms call `datafusion::functions::expr_fn::degrees` / `::radians`; facade binds keep names, warnings, pins; `_rescaled`, `_DEGREES_PER_RADIAN`, `_RADIANS_PER_DEGREE` deleted with the now-unused `math`, `lit`, `_thread_origin` imports. |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: fnp-bitmap-facade-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the brief. The deliverable is the ten-pin facade suite plus the byname routing pin, all run red on the base facade (9 of 9 original pins failed with the orchestrator's native installed and the wrappers reverted) and green after, each compared cell-by-cell to the SQL door and to live-PySpark-4.1.2 oracle cells copied verbatim.
      artifacts: [python/repark/tests/test_fnp_bitmap_facade_1.py, python/repark/tests/fnp_bitmap_facade_1_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Global and grouped construct; grouped and global or/and; NULL rows interleaved and all-NULL columns; empty frames; STRING coercion; INT argument; the 32767 boundary and both out-of-range positions; bitmap_count over every result; a sliding Window.rowsBetween cell; the call_function by-name route; result type and nullability on both doors; grouped-key nullability parity between doors.
      artifacts: [python/repark/tests/test_fnp_bitmap_facade_1.py]
    - id: AT-3
      status: N/A
      justification: No new Rust logic in this unit beyond three dispatch arms that bind existing, separately-tested UDAF constructors; the facade is one-line wrappers. No unwrap, expect, panic, async, lock or spawn introduced; the Rust diff was read against the neighbouring arms and the constructor names, and the native was built and probed by the orchestrator.
      artifacts: [crates/repark-python/src/column/function_dispatch.rs, crates/repark-functions/src/lib.rs]
    - id: AT-4
      status: N/A
      justification: The arms are pure lookups in an existing match table and the wrappers construct one Column each; no shared or mutable state beyond DataFusion's own UDAF singletons, no concurrency surface.
      artifacts: [crates/repark-python/src/column/function_dispatch.rs, python/repark/src/repark/spark/functions_bitwise.py]
    - id: AT-5
      status: N/A
      justification: No authn/authz, deserialization, path, credential or network surface; no dependency or manifest change; the oracle fixture is checked-in test data.
      artifacts: [python/repark/tests/fnp_bitmap_facade_1_spark_oracle.json]
    - id: AT-6
      status: ATTACKED
      evidence: This is the parity unit. Every facade answer is asserted equal to the SQL door's answer and to a recorded live-Spark cell (bytes, counts, types, nullability, error class and message body); nothing is compared to a hand-computed value except structural bit positions already pinned by FNP-6D.
      artifacts: [python/repark/tests/test_fnp_bitmap_facade_1.py, python/repark/tests/fnp_bitmap_facade_1_spark_oracle.json]
    - id: AT-7
      status: N/A
      justification: Not a perf unit; the wrappers add no compute (one aggregate binding each, the same path F.sum uses) and the dispatch arms add three string comparisons to an existing table walk.
      artifacts: [python/repark/src/repark/spark/functions_bitwise.py]
    - id: AT-8
      status: ATTACKED
      evidence: The aggregate path was read before editing (PyColumn.aggregate -> unary_aggregate_udaf, the unsigned-cast wrapper's no-op on Binary, the null-treatment branch for ignore_nulls=False), and the engine contracts (4096-byte BINARY, identity bitmaps, INVALID_BITMAP_POSITION, WIN-SLIDE-1 rescan for sliding frames) were taken from the FNP-6D SQL-door pins rather than assumed.
      artifacts: [crates/repark-python/src/column/mod.rs, python/repark/tests/test_fnp_6d_bitmap_aggregates.py]
    - id: AT-9
      status: ATTACKED
      evidence: The three names are pinned present on repark.spark.functions with their PySpark signatures (inspect), installed through the split-identity surface pin (alias tail and total length), through the byname call_function route, and through the example-coverage enumerator count.
      artifacts: [python/repark/tests/test_functions_split_identity.py, python/repark/tests/test_fnp_misc_1.py, python/repark-parity/tests/test_ex_0_example_coverage.py]
    - id: AT-10
      status: ATTACKED
      evidence: The red-first mutation is the base facade itself (wrappers reverted to b2c2b69e against the native tree) and all 9 original pins reded; two pin-side defects were caught by the engine on the first green run (a single-row helper over two-row grouped tables; a group-key nullability assertion that belongs to the input door) and fixed in the pins, not absorbed into the engine.
      artifacts: [python/repark/tests/test_fnp_bitmap_facade_1.py]
  complete: true
```

## What changed

| File | Change |
|---|---|
| `crates/repark-python/src/column/function_dispatch.rs` | Three `unary_aggregate_udaf` arms → `repark_functions::bitmap_agg::{bitmap_construct_agg_udaf, bitmap_or_agg_udaf, bitmap_and_agg_udaf}`; the four fully-qualified `binary_expr` arms condense onto the existing `datafusion::logical_expr` import (8 → 4 lines each) so the file moves 1000 → 991 lines with no baseline raised. |
| `crates/repark-functions/src/lib.rs` | `mod bitmap_agg;` → `pub mod bitmap_agg;` (line count unchanged, crate root holds its exact ceiling) so the cross-crate arm path compiles, the same shape as the `repark_functions::aggregate` arms. |
| `python/repark/src/repark/spark/functions_bitwise.py` | The three one-line wrappers over `_aggregate_argument(col)._inner.aggregate(kind, False)` in the `sum` shape; `INSTALL_NAMES` extended. |
| `python/repark/src/repark/spark/functions_byname.py` | `FACADE_ONLY_ROUTINE_NAMES` gains the three names (owner ruling in the run-15a follow-up: PySpark 4.1.2 resolves them as `call_function` builtins, so they are facade-only routines). |
| `python/repark/tests/test_fnp_bitmap_facade_1.py` | New. Ten pins: names/signature, global + grouped bytes vs SQL door and fixture, or/and folds, NULL and empty identities, STRING coercion, the error cell, the sliding `Window.rowsBetween` cell, and `call_function` routing. |
| `python/repark/tests/fnp_bitmap_facade_1_spark_oracle.json` | New. The 21 F6D-\*/B8-\* oracle cells, verbatim. |
| `python/repark/tests/test_functions_split_identity.py` | Alias-segment constant 6 → 9. |
| `python/repark-parity/tests/test_ex_0_example_coverage.py` | Enumerator count 1010 → 1013. |
| `docs/examples/functions/bitmap_aggregates.py` | New. The construct, fold, and identity arms, ReparkSession idiom; executes green under `--require-execute`. |
| `docs/examples/inventory.txt` | Regenerated (+3 rows). |
| `docs/spark-sql-iceberg-parity.md` | FNP-6D Rationale records the facade landing (date + unit id). |
| `.typos.toml` | `[type.fnpbitmapfacade1ledger]` `ba = "ba"` for this ledger's base SHA (the repo's existing short-hash precedent). |
| `map.md` × 7 | Lockstep: `tests`, `spark`, `column`, `repark-functions/src`, `staging`, parity `tests`, `docs/examples/functions`. |
| `STATUS.md`, `briefs/next-sequence.md` | Untouched. |
| `python/repark/tests/test_fnp_bitmap_facade_1.py` (run 16a) | R-1/R-2/R-3/R-5 pins added; the invalid-shape `call_function` pin split per valid shape vs the fixture; R-4 empty/fold/window/name pins added; grouped, or/and and STRING-`'1'` pins compare full bytes and actual Arrow types vs the fixture schema cells. 10 → 27 pins. |
| `python/repark/tests/fnp_bitmap_facade_1_spark_oracle.json` (run 16a) | R-4: the two truncated BINARY cells carry the full 4096-byte hex. |
| `python/repark/tests/test_functions_split_identity.py` (run 16a) | R-6: `__all__` uniqueness assert. |
| `python/repark/tests/map.md` (run 16a) | Facade pin-file and fixture entries record the R-1..R-5 remediation. |
| `docs/spark-sql-iceberg-parity.md` (run 16a) | R-5: FNP-6D residual row for the SQL-door qualifier leak with its divergence pin. |
| `crates/repark-python/src/column/function_dispatch.rs` (run 16a) | R-7: `degrees` / `radians` `call_scalar` arms onto the engine UDFs, beside the bitmap arms. |
| `python/repark/src/repark/spark/functions_math.py` (run 16a) | R-7: thin `_scalar` binds over a `double` cast; `_rescaled`, the factor constants and the orphaned imports deleted. |
| `crates/repark-python/src/column/map.md`, `python/repark/src/repark/spark/map.md`, `python/repark/tests/map.md` (run 16a) | R-7 lockstep: the dispatch-arm note, the `functions_math.py` implementation note, the `_rescaled` sentence. |

No public API change beyond the three additive names: no crate dependency, no `Cargo.lock`
edit, no `.github/` edit, no kernel change.

## Run 16a rebuild and rulings (orchestrator, 2026-09-15)

- The branch was rebuilt by cherry-picking this unit's eight commits onto main `bee2cde3`, the
  squash of FNP-6D-FOLLOWUP-1 (#623). The earlier merge commits of the pre-squash follow-up
  branch are gone. Conflicts resolved: map row tails kept from both sides; EX-0 1052 + 3 = 1055;
  `function_dispatch.rs` keeps the three bitmap arms beside main's
  `spark_result_types::approx_count_distinct_udaf()` arm; `test_functions_split_identity.py`
  takes main's FNP-11A tail with the install constant 6 → 9; the registry bitmap paragraph
  keeps the facade sentence once.
- Q1 of the remediation round (the FNP-6D-FOLLOWUP-1 ledger-grammar red seen by `make verify`):
  ruled "leave it to its own unit". #623 filed the COVERAGE_ATTESTATION and pins citations, and
  this commit carries that ledger's departure move to `completed/`, which #623 omitted.
- Owner ruling Q-15a-1 (DEGREES-RUST-1) is applied by rider R-7; owner ruling Q-15a-5 (the
  `len(set(__all__)) == len(__all__)` uniqueness pin) is applied by R-6.
- Orchestrator gate fixes on the rebuilt branch (release native from `d6753ec5`, 2026-09-15 11:46):
  `make verify` failed only on `ruff format` of `test_fnp_bitmap_facade_1.py` (whitespace from the
  cherry-pick resolution), and `test_fnp_misc_1_byname_allowlist_covers_facade` failed because
  R-7 moved `degrees` / `radians` onto engine UDFs, so the derived by-name allowlist no longer
  classifies them as facade-only. Both names left `FACADE_ONLY_ROUTINE_NAMES`; the four pin files
  re-ran green.
