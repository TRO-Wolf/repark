# Charter ledger — FNP-MATH-1 · math, formatting, masking and crypto functions

**Date:** 2026-09-15 · **Branch:** `feat/fnp-math-1` · **Base:** `origin/main`
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** EX-FN-5 (format_number), EX-FN-7 (hash), EX-FN-17 (sentences),
EX-FN-18 (split) stay BACKLOG until the build steps land; flip to FIXED with pins then.
One section-7 row per residual divergence measured, and per declared refusal.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The 1.5 Spark-parity campaign's FNP-MATH-1 card carries twelve
D-6 names plus the split facade half (D-8) and the BL-6 facade half (D-9) from
stubs and absences to PySpark 4.1.2 answers on both doors, pinned against recorded
oracle cells. Run 16a step 1 is the red-first foundation only: ledger, fixture,
pins, all verdicts OPEN. No product code, no cargo, no dependency edits.

**Not in this unit:** `to_char` / `to_varchar` / `to_number` / `to_binary`
(moved to FNP-11B, D-6); the SQL parser/dialect, `dataframe/**`, `column.py`,
`session/**`, `catalog.py`, `types.py` (runs 16b/16c fences); any dependency
change before its build step (AES crates approved under D-7, added later).

## Decisions (orchestrator rulings under G-2, copied from the card)

- D-1 Kernels in `crates/repark-functions/src/` (new modules listed in its
`map.md`), registered in `register_all`. Reuse `datafusion-spark` kernels where
they exist and answer the oracle. Facade wrappers destub `functions_expr.py`
and add names to the family modules; ADD arms only to
`crates/repark-python/src/column/function_dispatch.rs` (run 15c converges).
- D-2 Never edit `dataframe/**`, `column.py`, `session/**`, `catalog.py`,
`types.py`, the SQL parser/dialect, `Cargo.toml` or `Cargo.lock`.
- D-3 Registry: flip EX-FN-5, EX-FN-7, EX-FN-17, EX-FN-18 to FIXED with pins;
add a section-7 row per residual divergence measured (and per declared refusal).
- D-4 Pins: `python/repark/tests/test_fnp_math_1.py`, both doors, both ANSI
settings, over the oracle cells; red first on the base tree with the red summary
in the ledger; Rust unit tests beside each kernel.
- D-5 Commit per numbered step; if a name cannot reach the oracle within the
round, commit what is green, mark it OPEN with the measured blocker, continue.
- D-6 Scope for this PR: `bround`, `conv`, `mask`, `collate`, `collation`,
`sentences`, `hash`, `format_number`, `aes_encrypt`, `aes_decrypt`,
`try_aes_decrypt`, and the facade half of `split`. `to_char` / `to_varchar` /
`to_number` / `to_binary` moved to FNP-11B; `locate(pos)` and
`array_join(null_replacement)` stay here only if their pins are green in the
same round.
- D-7 Owner ruling Q-15c-1 (2026-09-15): AES dependencies APPROVED — the
RustCrypto crates `aes`, `aes-gcm`, `cbc`, `ecb`, added to the workspace
`[workspace.dependencies]` and `crates/repark-functions/Cargo.toml` only;
`Cargo.lock` changes limited to those crates and their new transitive deps;
`cargo deny check` and `scripts/check_crate_dag.py` stay green (HALT with output
if not). `mask` lands first with no dependency. Pin every mode (ECB, CBC, GCM
with and without AAD) and every key length (16/24/32 bytes and a bad length)
against the `F14-*` cells in `/tmp/oc-worker/pc-oracle/fixtures-batch3.json`
(card `/tmp/oc-worker/pc-cards/card-fnp14.md`), both doors.
- D-8 Run 16c P2 handoff (2026-09-15): `F.split` still raises
UnsupportedOperationException in functions_expr.py while the SQL door answers
`split(str, pattern[, limit])` on the Rust kernel `spark_split.rs`. Bind the
facade to that kernel through an ADD arm in function_dispatch.rs; pins against
`/tmp/oc-worker/qc-oracle/fixtures-batch12.json` cells Q12-41…Q12-55 (value,
type, containsNull=false, nullability). Run 16c's guard pin
`test_door_converge_2.py::test_facade_split_refusal_handoff_16a` flips when this
lands — update it in this PR if DOOR-CONVERGE-2 is on main.
- D-9 BL-6 facade half (run 15c P2): `F.bin` / `F.rint` refuse BOOLEAN before
the kernel with Spark's DATATYPE_MISMATCH class; `F.like` gains the 3-argument
escape form (`like(str, pattern, escapeChar)`); pins from the 15c batch fixtures
named in docs/spark-sql-iceberg-parity.md BL-6.

## PROPOSITION LEDGER — FNP-MATH-1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every D-6 name is present on `repark.spark.functions` with PySpark 4.1.2 parameter shapes. | `test_fnp_math_1.py::test_c001_*`, 17 pins, red on the base. | **OPEN** | Red 2026-09-15: 9 of 14 signature pins fail with `AttributeError` (`bround`, `conv`, `mask`, `collate`, `collation`, `sentences`, `aes_encrypt`, `aes_decrypt`, `try_aes_decrypt` absent). 5 pass on base (`hash`, `format_number`, `split`, `locate`, `array_join` stubs carry Spark's parameter names); `bin` / `rint` / `like` presence pins pass (no recorded signature cells — see needs-oracle list). pins: fnp-math-1/C-001 |
| C-002 | The D-6 names answer the recorded Python-door value cells on the cell's ANSI setting. | `test_fnp_math_1.py::test_c002_*`, 44 pins, red on the base. | **OPEN** | Red 2026-09-15: 44 of 44 fail. Absent names raise `AttributeError`; `hash` / `sentences` / `format_number` raise the stub `UnsupportedOperationException`; `locate(pos=…)` / `array_join(null_replacement=…)` raise the partial-shape refusals; the bare `locate(o, txt)` and 2-arg `array_join` cells answer values but render column names (`locate(o, txt)`, Spark `locate(o, txt, 1)`) the pins hold strictly. pins: fnp-math-1/C-002 |
| C-003 | The D-6 names answer the recorded SQL-door value cells on the cell's ANSI setting. | `test_fnp_math_1.py::test_c003_*`, 17 pins, red on the base. | **OPEN** | Red 2026-09-15: 17 of 17 fail. `Invalid function` at plan time for `conv`, `locate`, `split` (no `spark_split` kernel in this clone's source or native module — only `spark_split_part` exists); the G15 collation refusal for `collate`; `array_join` answers values under a divergent rendered name. pins: fnp-math-1/C-003 |
| C-004 | The recorded error cells raise Spark's error condition under both ANSI settings where the cell carries them. | `test_fnp_math_1.py::test_c004_*`, 15 pins, red on the base. | **OPEN** | Red 2026-09-15: 15 of 15 fail. `conv` ANSI-on must raise `[ARITHMETIC_OVERFLOW]`; unknown collation must raise `[COLLATION_INVALID_NAME]` (card prescription — the oracle cell itself is a Py4JJavaError wrap with no condition); bad AES key / bad ciphertext must raise `[INVALID_PARAMETER_VALUE.AES_KEY_LENGTH]` / `[AES_CRYPTO_ERROR]` on both doors under both ANSI settings. Base raises `Invalid function`, the stub refusal, or nothing. pins: fnp-math-1/C-004 |
| C-005 | The hash and AES cells match byte-exactly (Murmur3 ints incl. -0.0/0.0 equality, ECB exact hex, fixed-IV CBC/GCM exact hex, round-trips, every key length, try_ NULL). | `test_fnp_math_1.py::test_c005_*`, 60 pins, red on the base. | **OPEN** | Red 2026-09-15: 60 of 60 fail (28 o245 hash/AES cells on the recorded door/ANSI; 32 F14 value cells on both doors under both ANSI settings). Base: `Invalid function` on SQL, `AttributeError` / stub refusal on Python. pins: fnp-math-1/C-005 |
| C-006 | No regression: the harness frame and the neighbouring suites stay green. | Frame-control pin green on base; neighbouring suites re-run at step 7. | **OPEN** | `test_c006_frame_control_rows_answer` passes on base (frame seed rows answer, so every other red is a function gap, not a frame gap). Neighbouring suites (`test_functions_a/b/c`, census pins) untouched in step 1; re-run due at step 7. pins: fnp-math-1/C-006 |
| C-007 | Registry + maps: fixture blocks recorded with sources, ledger + map rows in lockstep. | `test_c007_fixture_blocks_present` green; this ledger; map rows. | **OPEN** | Fixture `fnp_math_1_spark_oracle.json` carries 137 cells in 4 named blocks (o245 106, F14 10, Q12 15, BL-6 6) with per-block source provenance; integrity pin green. Registry flips (D-3) and EX-FN census updates belong to the build steps. pins: fnp-math-1/C-007 |
| C-008 | The split facade half answers Q12-41…Q12-55 through `F.split` (value, type, containsNull=false, nullability) under both ANSI settings. | `test_fnp_math_1.py::test_c008_*`, 70 pins, red on the base. | **OPEN** | Red 2026-09-15: 70 of 70 fail. Facade raises `UnsupportedOperationException`; SQL door raises `Invalid function 'split'` (kernel absent here — see C-003). `test_door_converge_2.py` is not on this tree, so no guard pin flips yet. pins: fnp-math-1/C-008 |
| C-009 | The BL-6 facade half refuses BOOLEAN for `bin` / `rint` with `DATATYPE_MISMATCH` and answers 3-argument `like` escape, pinned from the recorded BL-6 cells. | `test_fnp_math_1.py::test_c009_*`, 18 pins; refusals + escape red on base. | **OPEN** | Red 2026-09-15: 12 of 18 fail (`F.bin(True)` / `F.rint(True)` answer `"1"` / `1.0` instead of refusing; `F.like` takes 2 args; SQL `bin(1)` renders `bin(Int64(1))` not `bin(1)`; `2.5D` literal does not parse; 3-arg SQL `like` unplanned). Green on base: SQL refusals `BL6-sql-0/1` (DOOR-CONVERGE-1) and the accepted `F.bin(1)` / `F.rint(2.5)` values. pins: fnp-math-1/C-009 |

VERDICT: 9 clauses, 0 PROVEN, 9 OPEN, 0 REJECTED.

## Red summary (base tree, 2026-09-15)

`PYTHONPATH=python/repark/src .venv/bin/python -m pytest python/repark/tests/test_fnp_math_1.py -q`:
**227 failed, 16 passed** (243 pins). The 16 greens are C-001 stub-signature
presence (8), the C-006 frame control, the C-007 fixture integrity pin, the
C-009 SQL-door refusals already FIXED by DOOR-CONVERGE-1 (4), and the accepted
`F.bin(1)` / `F.rint(2.5)` facade values (2). Every green is a control or a
prior unit's FIXED row, never a claim this unit makes.

Per-name red counts (Python door / SQL door, both ANSI settings): bround 8/2,
conv 6/1+error, hash 6/2 (C-005), format_number 4/2, mask 4/2, collate 2/2+2
errors, collation 2/0, sentences 4/2, split 10 Python (C-008) + 2 SQL (C-003) +
30/30 Q12 (C-008), locate 8/2, array_join 6/2, aes_encrypt 2/6 + 16 F14 value
+ 4 F14 error, aes_decrypt 2/6 + 4 F14 value + 4 F14 error, try_aes_decrypt 2/2
+ 4 F14, BL-6 6 facade + 6 SQL value red / 4 SQL refusal + 2 facade value green.

## Needs oracle cell (no recorded answer — never invented)

- Exact PySpark signatures for `bin` / `rint` / `like` (the o245 `signatures`
block has no entry; C-001 pins presence only).
- AES 24-byte and 32-byte key cells (F14 covers 16-byte keys and bad lengths
only: `short` 5 bytes, `shortkey` 8 bytes).
- AES CBC/GCM random-IV determinism beyond the recorded length/round-trip cells.
- `locate` / `array_join` stay in this PR only if their pins go green in the
same round (D-6); their cells ride in the o245 block either way.

## Out-of-scope observations (measured, not acted on)

- A `VALUES` list carrying a TIMESTAMP column fails to execute on the base tree
(`expected Timestamp(ns) but found Timestamp(µs, "UTC")`); the pin frame
therefore grafts `ts` per row with a `CASE` over scalar TIMESTAMP literals,
which answer. Timestamp-in-VALUES belongs to the types/session lanes.
- The `2.5D` DOUBLE-suffix literal does not parse on the SQL door
(`ParserError … found: D`); the BL6-sql-3 pin reproduces the oracle text
verbatim. Parser/dialect is run 16c's fence.
- Column-name renders diverge where values already answer: SQL `bin(1)` renders
`bin(Int64(1))`, Python `locate(o, txt)` renders without Spark's `, 1`, and
2-arg `array_join` renders typed literals. The pins hold Spark's recorded names
strictly; the build steps dispose each as a fix or a registry row.
- `test_door_converge_2.py` is not on this tree, so D-8's guard pin has nothing
to flip yet; the C-008 pins stand alone until it lands.
- `collation` answers for plain strings are `UTF8_BINARY` per the oracle cell;
the engine's G15 collation refusal text is confirmed live on the `collate` SQL
path and stays until the build step replaces it.

## Gates (step 1)

| gate | result |
|---|---|
| `pytest python/repark/tests/test_fnp_math_1.py -q` | 227 failed, 16 passed — RED as chartered |
| `uvx ruff@0.15.22 check` on the new test file | clean |
| `uvx ruff@0.15.22 format --check` on the new test file | clean |
| comment-ban grep over the diff | empty |
| typos gate | the fixture joins `fnp11a_r2_spark_oracle.json` in `.typos.toml` extend-exclude: recorded AES hex evidence trips the spell gate and is never hand-edited |
