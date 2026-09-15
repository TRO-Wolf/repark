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

**Not in this unit:** `STATUS.md`, `briefs/next-sequence.md`, `Cargo.toml`,
`Cargo.lock`, `pyproject.toml`, `uv.lock`, `.github/`, `functions*.py`,
`python/repark/src/repark/spark/dataframe/**`, `column.py`, `session/**`,
`catalog.py`, `types.py`, the SQL parser/planner beyond function registration.

## PROPOSITION LEDGER — FNP-6D-FOLLOWUP-1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `bitmap_or_agg` / `bitmap_and_agg` refuse every non-BINARY payload at planning with `[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]`, `The first parameter requires the "BINARY" type`, the Spark type name, `SQLSTATE: 42K09` (fixture ids `FU-or-*`, `FU-and-*`). | `test_fnp_6d_followup_1.py::test_or_and_agg_refuses_non_binary_payload` (12 cells). | OPEN | Red 2026-09-15: all 12 fail on the unfixed tree; the engine folds decimal-text bytes (see Red first). BLOCKED by the concat-Utf8 conflict below. |
| C-002 | `bitmap_construct_agg` refuses BOOLEAN/BINARY/DATE/TIMESTAMP at planning with the same class requiring "BIGINT" (fixture `FU-construct-bool/binary/date` plus a TIMESTAMP arm per D-1). | `test_construct_agg_refuses_non_bigint_payload` (4 cells). | OPEN | Red 2026-09-15: all 4 fail (BOOLEAN/DATE answer 0 today; BINARY errors with the wrong class). |
| C-003 | `bitmap_construct_agg` on malformed STRING ('abc', '', '1.5', mixed '1'/'abc') raises Spark's CAST_INVALID_INPUT text with `SQLSTATE: 22018` under ANSI-on (the door default). | `test_construct_agg_malformed_string_raises_cast_invalid_input` (4 cells). | OPEN | Red 2026-09-15: all 4 answer count 0 today. |
| C-004 | `bitmap_construct_agg` answers Spark for numeric, trimmed-string, and NULL input: FLOAT 1.7 and 1.0, DOUBLE 2.0, DECIMAL 1.5, ' 1 ', INT, untyped NULL, typed NULL BIGINT. | `test_construct_agg_answers_numeric_trimmed_and_null` (8 cells). | OPEN | Red 2026-09-15: 5 fail (float/double/decimal/space answer 0 today); INT and both NULL arms already pass. |
| C-005 | ANSI-off cells (`*-nonansi` malformed-string skips, '1.5' truncates to 1). | Registry row per the D-3 fallback, or pins if the ruling opens a carrier. | OPEN | The aggregate kernel has no config path: `AccumulatorArgs` carries exprs/schema only, and SQL `SET spark.sql.ansi.enabled` is stored-but-not-applied (SET-ANSI-RUNTIME-1), so ANSI-off is unreachable without a core/planner edit. D-3 fallback is ANSI-on plus a registry row. |
| C-006 | No regression: the FNP-6D pins and `cargo test -p repark-functions bitmap` stay green. | `test_fnp_6d_bitmap_aggregates.py` (10 passed 2026-09-15, unfixed tree) plus the post-fix rerun. | OPEN | Baseline green recorded; post-fix rerun pending the ruling. |
| C-007 | Registry rows (D-2 rendering residual, D-3 ANSI-off, and the ruling on the conflict below) plus map lockstep. | `docs/spark-sql-iceberg-parity.md` FNP-6D section; `map.md` rows for the fixture, the pin file, and this ledger. | OPEN | Maps for step 1 committed; registry rows pending the ruling. |

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
No worktree. No `cargo clean`. Step 1 ran Python pins only (no Rust build).

## Gates (step 1)

| Command | Result |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_fnp_6d_followup_1.py -q --tb=no` | 25 failed, 3 passed (red-first, unfixed tree) |
| `.venv/bin/python -m pytest python/repark/tests/test_fnp_6d_bitmap_aggregates.py -q --tb=no` | 10 passed (C-006 baseline) |
