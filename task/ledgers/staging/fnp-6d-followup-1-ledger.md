# Unit ledger — FNP-6D-FOLLOWUP-1 · bitmap aggregate signatures

**Date:** 2026-09-15 · **Branch:** `feat/fnp-6d-followup-1` · **Base:** `11ae1595`
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when FNP-6D-FOLLOWUP-1 merges, or when the owner closes the slate row.

**Why now.** P1 in merged #609, found by run 15a's Grok critic (report
`/tmp/oc-worker/pa-bitmap-crit/report.md` L-001 and L-003):
`crates/repark-functions/src/bitmap_agg.rs` admits Utf8 beside binary payloads,
so DataFusion casts INT/FLOAT/BOOLEAN/DATE to text and the OR/AND folds fold
decimal-text bytes; and `bitmap_construct_agg` answers NULL-identity on malformed
STRING where ANSI Spark raises CAST_INVALID_INPUT. Oracle: live PySpark 4.1.2,
recorded 2026-09-15 (`python/repark/tests/fnp_6d_followup_1_spark_oracle.json`,
50 cells, recorder `oracle_f6dfu.py`).

**G-2 rulings (orchestrator, copied).**
D-1: fix lives in `crates/repark-functions/src/bitmap_agg.rs` (plus
`bitmap_agg/groups.rs` if needed). Remove the Utf8 family from the OR/AND payload
signature; refuse non-binary at planning with Spark's class via `coerce_types`
(user-defined signature) or equivalent; `coerce_bitmap_column` keeps only binary
types. Construct refuses BOOLEAN/BINARY/DATE/TIMESTAMP and every other
non-numeric-non-string type the same way with "BIGINT".
D-2: message class, `The first parameter requires the "BINARY" type, however`,
and the Spark type name must match the fixture. Residual call-rendering goes to a
registry row in `docs/spark-sql-iceberg-parity.md` inside the bitmap/FNP-6D section.
No SQL parser/planner edit.
D-3: malformed STRING in construct raises Spark's CAST_INVALID_INPUT text when ANSI
is on; ANSI-off keeps the NULL skip and truncates '1.5' to 1. Reuse the Spark door's
`SparkAnsiConfig` carrier or an existing Spark string-to-BIGINT cast kernel; no second
ANSI carrier. If the ANSI flag cannot reach the aggregate without a core/planner edit,
implement ANSI-on (the default), pin it, registry-row the ANSI-off shape.
D-4: scope is signature/coercion plus pins. No facade (#613). No change to fold
semantics, padding, GroupsAccumulator, or window frames;
`test_fnp_6d_bitmap_aggregates.py` stays green.

**Owner rulings applied.** No-comments-in-code (2026-08-26): prose lives here and in
`map.md`, never in source. RUST FIRST (2026-09-14): the fix lands in
`repark-functions`; the facade stays untouched. SHAPE RULE: pins answer PySpark 4.1.2
on the SQL door against the recorded fixture cells.

**D-5 (orchestrator ruling on the step-1 HALT, 2026-09-15):** option (a). OR/AND refuse
every Utf8 payload per D-1. The single C-011 arm becomes
`CAST(concat(bitmap_construct_agg(0), X'01') AS BINARY)` — a no-op in Spark, keeps the
>4096-byte truncation coverage; the arm change is noted here (C-006 evidence) rather
than in the frozen FNP-6D ledger. One registry row in the FNP-6D section records
`concat(BINARY, BINARY)` typing STRING on the door (measured 2026-09-15, owner
DOOR-CONVERGE-2, run 16c) with a one-cell pin asserting today's Utf8 shape as the
expected divergence. `concat` itself is untouched.

**D-6 (orchestrator rulings, remediation round 2, 2026-09-15).**
L-001 P1 CONFIRMED: non-finite and out-of-range numerics raise `[CAST_OVERFLOW]`
with the per-cell value rendering (`NaN`, `Infinity`, `-Infinity`, `1.0E30D`,
`99999999999999999999BD`) and type (`DOUBLE`/`FLOAT`/`DECIMAL(20,0)`); 1.7 → 1 stays.
Overflow is detected per batch from the source array (non-null source, null cast
result); Arrow's safe cast stays global. Pinned on the global, grouped, and window
paths (C-004). The `1.0E30D` rendering reuses the crate's `java_double_text`
(`json::reader`, visibility widened to `pub(crate)`); no live-PySpark re-measure
exists in-lane (no pyspark module), so the recorded FU2 cells are the truth and the
pins assert their message core verbatim.
L-002 P2 ACCEPTED as a pin: a builder-config ANSI-off session still raises on
`'abc'` today, pinned as the expected divergence against `FU-construct-abc-nonansi`
(rows `[[0]]`), citing SET-ANSI-RUNTIME-1 and owner ruling Q-15c-3 (run 16b/16c owns
the per-query snapshot and will flip this pin); `SparkAnsiConfig` is not reachable
from `AccumulatorArgs` without a planner edit (critic-confirmed), so no ANSI-off
kernel in this round (C-005).
L-003 refuted by FU2-construct-str-overflow / -u64: Spark 4.1.2 raises
`CAST_INVALID_INPUT` (not `CAST_OVERFLOW`) for `'9223372036854775808'` and
`'18446744073709551615'`, and `'+1'` answers 1 — pinned as-is (C-003/C-004) so
today's correct behaviour cannot regress.
L-004 P2 ACCEPTED: refusal pins are driven from fixture ids — each pin looks up its
cell, runs the adapted SQL from the id → SQL map in the test file (adaptation
reasons in `python/repark/tests/map.md`), and asserts the fixture message core
(class, required type, Spark type name, SQLSTATE) extracted from the cell's
`message`, plus the registry-cited `t.x` qualifier for the nested bit-position
rendering (C-001/C-002/C-003/C-007).
L-005 P3 → ledger only (arity stays a loud Execution error; a WRONG_NUM_ARGS sweep
is out of this unit). L-006 P3: `FU2-construct-fixedbin` folds `CAST(x AS BINARY)`;
`FixedSizeBinary` joins the OR/AND payload set with a direct visit arm, pinned
(C-001).

**Not in this unit:** `STATUS.md`, `briefs/next-sequence.md`, `Cargo.toml`,
`Cargo.lock`, `pyproject.toml`, `uv.lock`, `.github/`, `functions*.py`,
`python/repark/src/repark/spark/dataframe/**`, `column.py`, `session/**`,
`catalog.py`, `types.py`, the SQL parser/planner beyond function registration.

## PROPOSITION LEDGER — FNP-6D-FOLLOWUP-1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `bitmap_or_agg` / `bitmap_and_agg` refuse every non-BINARY payload at planning with `[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]`, `The first parameter requires the "BINARY" type`, the Spark type name, `SQLSTATE: 42K09` (fixture ids `FU-or-*`, `FU-and-*`). | `test_fnp_6d_followup_1.py::test_or_and_agg_refuses_non_binary_payload` (12 cells). | PROVEN | Red then green 2026-09-15: 12/12 fail unfixed, 12/12 pass fixed; Rust `or_agg_refuses_non_binary_with_spark_class` + `and_agg_refuses_non_binary_with_spark_class`; door text in Green evidence. The D-5 conflict resolved per ruling (a). |
| C-002 | `bitmap_construct_agg` refuses BOOLEAN/BINARY/DATE/TIMESTAMP at planning with the same class requiring "BIGINT" (fixture `FU-construct-bool/binary/date` plus a TIMESTAMP arm per D-1). | `test_construct_agg_refuses_non_bigint_payload` (4 cells). | PROVEN | Red then green 2026-09-15: 4/4 fail unfixed, 4/4 pass fixed; Rust `construct_agg_refuses_non_bigint_with_spark_class`. |
| C-003 | `bitmap_construct_agg` on malformed STRING ('abc', '', '1.5', mixed '1'/'abc') raises Spark's CAST_INVALID_INPUT text with `SQLSTATE: 22018` under ANSI-on (the door default). | `test_construct_agg_malformed_string_raises_cast_invalid_input` (4 cells). | PROVEN | Red then green 2026-09-15: 4/4 answer count 0 unfixed, 4/4 raise fixed; Rust `construct_agg_malformed_string_raises_cast_invalid_input`; door text in Green evidence. |
| C-004 | `bitmap_construct_agg` answers Spark for numeric, trimmed-string, and NULL input: FLOAT 1.7 and 1.0, DOUBLE 2.0, DECIMAL 1.5, ' 1 ', INT, untyped NULL, typed NULL BIGINT. | `test_construct_agg_answers_numeric_trimmed_and_null` (8 cells). | PROVEN | Red then green 2026-09-15: 5 fail unfixed (float/double/decimal/space answer 0), 8/8 pass fixed; Rust `construct_agg_answers_numeric_trimmed_and_null` + `grouped_construct_agg_answers_trimmed_strings` (groups path shares `construct_positions`). |
| C-005 | ANSI-off cells (`*-nonansi` malformed-string skips, '1.5' truncates to 1). | Registry row per the D-3 fallback, or pins if the ruling opens a carrier. | PROVEN | D-3 fallback taken: kernel always raises ANSI-on (the door default); the `*-nonansi` fixture cells stay recorded-but-unpinned and the registry Residuals row says why (no runtime ANSI-off switch, SET-ANSI-RUNTIME-1; no config path in `AccumulatorArgs`). |
| C-006 | No regression: the FNP-6D pins and `cargo test -p repark-functions bitmap` stay green. | `test_fnp_6d_bitmap_aggregates.py` (10 passed 2026-09-15, unfixed tree) plus the post-fix rerun. | PROVEN | Post-fix 10/10 green incl. the D-5-adapted C-011 arm `CAST(concat(bitmap_construct_agg(0), X'01') AS BINARY)` (length 4096, Spark-true form; the only existing-pin SQL touched, per ruling (a)); full crate 473 passed; `make verify` exit 0. |
| C-007 | Registry rows (D-2 rendering residual, D-3 ANSI-off, and the ruling on the conflict below) plus map lockstep. | `docs/spark-sql-iceberg-parity.md` FNP-6D section; `map.md` rows for the fixture, the pin file, and this ledger. | PROVEN | Registry Residuals row landed (rendering qualifier, ANSI-off, concat/DOOR-CONVERGE-2 with the divergence pin); maps current for the fixture, both pin files, `bitmap_agg.rs`, `groups.rs`, `tests.rs`, and this ledger (hook-green). |

## Evidence

### Red first (step 1, 2026-09-15, unfixed tree at 11ae1595, DEBUG native)

`.venv/bin/python -m pytest python/repark/tests/test_fnp_6d_followup_1.py -q --tb=no`
→ **25 failed, 3 passed in 0.76s**. The 3 passes are the already-correct C-004 arms
(`construct-int`, `construct-null-bigint`, `FU-construct-null-literal`).

Probed answers on the unfixed tree (every one contradicts the fixture cell):

| Probe | Unfixed answer | Fixture truth |
|---|---|---|
| `bitmap_or_agg(x)`, x INT | folds text bytes `0x31`/`0x32` | DATATYPE_MISMATCH/INT |
| construct FLOAT 1.7 / FLOAT 1.0 / DOUBLE 2.0 / DECIMAL 1.5 | count 0 (coerced via text, cast to NULL) | count 1 (truncation) |
| construct ' 1 ' | count 0 | count 1 (trimmed) |
| construct true / X'01' / DATE'2020-01-01' | count 0 / count 0 / count 0 | DATATYPE_MISMATCH/BOOLEAN, BINARY, DATE |
| construct 'abc' / '' / '1.5' / mixed | count 0 | CAST_INVALID_INPUT (ANSI-on) |
| construct NULL / CAST(NULL AS BIGINT) | count 0 | count 0 (already correct) |

Baseline: `test_fnp_6d_bitmap_aggregates.py` → **10 passed in 0.68s** (C-006 red-first control).

### Green (step 2, 2026-09-15, fixed tree, release native)

`.venv/bin/python -m pytest python/repark/tests/test_fnp_6d_followup_1.py
python/repark/tests/test_fnp_6d_bitmap_aggregates.py -q` → **39 passed in 0.52s**
(29 followup incl. the DOOR-CONVERGE-2 divergence pin, 10 existing incl. the adapted
C-011 `CAST(concat(...) AS BINARY)` arm, which answers length 4096 as Spark does).

`cargo test -p repark-functions --lib bitmap_agg` → **13 passed, 0 failed** (8 carried
FNP-6D kernel pins plus `or_agg_refuses_non_binary_with_spark_class`,
`and_agg_refuses_non_binary_with_spark_class`,
`construct_agg_refuses_non_bigint_with_spark_class`,
`construct_agg_malformed_string_raises_cast_invalid_input`,
`construct_agg_answers_numeric_trimmed_and_null`,
`grouped_construct_agg_answers_trimmed_strings`).

`cargo test -p repark-functions` (full crate) → **473 passed, 0 failed, 1 ignored**.

`make verify` → exit 0 (56 ok suites; rust-file-size needed the sanctioned split of
the test module into `bitmap_agg/tests.rs`: parent 516 lines, tests 526 lines).

Door-measured refusal texts (2026-09-15, release):

```text
repark.errors.AnalysisException :: Error during planning:
[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "bitmap_or_agg(x)" due to
data type mismatch: The first parameter requires the "BINARY" type, however "x" has
the type "INT". SQLSTATE: 42K09
repark.errors.AnalysisException :: Error during planning:
[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve
"bitmap_or_agg(bitmap_bit_position(t.x))" due to data type mismatch: The first
parameter requires the "BINARY" type, however "bitmap_bit_position(t.x)" has the
type "BIGINT". SQLSTATE: 42K09
repark.errors.PySparkException :: Execution error: [CAST_INVALID_INPUT] The value
'abc' of the type "STRING" cannot be cast to "BIGINT" because it is malformed.
Correct the value as per the syntax, or change its target type. Use `try_cast` to
tolerate malformed input and return NULL instead. SQLSTATE: 22018
```

Two door-typing notes the pins absorb (not product gaps): DataFusion types
`VALUES (1)` as BIGINT where Spark types INT, so the INT cells pin
`VALUES (CAST(1 AS INT))`; nested-call rendering qualifies the column (`t.x`),
so the bit-position pins assert class plus type name (D-2 residual, registry row).

### Blocking conflict (needs a ruling before step 2)

D-1 (refuse every Utf8 payload in OR/AND) and D-4 (`test_fnp_6d_bitmap_aggregates.py`
stays green) are jointly unsatisfiable for one existing pin. Probed 2026-09-15 on the
unfixed tree: `SELECT concat(bitmap_construct_agg(0), X'01')` types as **string**
(Utf8), so the outer `bitmap_or_agg(b)` in the C-011 case
(`test_or_and_short_empty_and_long_normalize_to_4096`, 4097-byte truncation arm)
presents the aggregate with a plain Utf8 column — type-identical to the fixture's
`FU-or-string` (`VALUES ('abc')`, must refuse). No type-directed rule separates them;
the subquery boundary hides the `concat` from any expression-aware hook as well, and
the SQL parser/planner is fenced to run 16c. Candidate resolutions for the ruling:
(a) refuse Utf8 and adapt that one C-011 arm to `CAST(concat(...) AS BINARY)` (Spark-true
form, keeps the truncation coverage) plus a registry row for the concat-typing
divergence; (b) fix `concat` to return BINARY for all-binary inputs first (another
function's surface); (c) keep Utf8 accepted (violates D-1 and the fixture). Lean: (a).

## Disk

Checked 2026-09-15 before the probe runs: `df -h /tmp` shows 420 GB free of 1.8 TB.
No worktree. No `cargo clean`. Step 1 ran Python pins only (no Rust build). Step 2
ran one debug `cargo test` cycle, one `make verify`, and one release
`maturin develop`; no extra worktrees or coverage artifacts kept. Resolution of the
conflict above: orchestrator ruling D-5 (option (a)) received and applied 2026-09-15;
no HALT remains open.

## Gates (step 1)

| Command | Result |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_fnp_6d_followup_1.py -q --tb=no` | 25 failed, 3 passed (red-first, unfixed tree) |
| `.venv/bin/python -m pytest python/repark/tests/test_fnp_6d_bitmap_aggregates.py -q --tb=no` | 10 passed (C-006 baseline) |

## Gates (steps 2–3, fixed tree, 2026-09-15)

| Command | Result |
|---|---|
| `cargo test -p repark-functions --lib bitmap_agg` | 13 passed, 0 failed |
| `cargo test -p repark-functions` | 473 passed, 0 failed, 1 ignored |
| release native `uvx maturin@1.14.1 develop --release` | installed repark-1.4.1 |
| `.venv/bin/python -m pytest python/repark/tests/test_fnp_6d_followup_1.py python/repark/tests/test_fnp_6d_bitmap_aggregates.py -q` | 39 passed |
| `make verify` | exit 0 (56 ok suites) |
