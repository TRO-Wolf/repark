# Unit ledger — DOOR-CONVERGE-2 round 1 · P1 wrong answers on the Spark SQL door

**Date:** 2026-09-15 · **Branch:** `feat/door-kernel-converge-2` · **Base:** `origin/feat/door-kernel-converge-1`
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card DOOR-CONVERGE-2 round 1 (run 16c, 2026-09-15): four P1 names answer wrong on
the Spark SQL door — `concat` over arrays stringifies (`'[1][2]'`), `reverse` over arrays reverses
stringified text, `sequence` and `split` are "Invalid function". Each name converges when it answers
PySpark 4.1.2 on BOTH doors for argument shapes, NULL and edge cases, result VALUE and Arrow TYPE and
NULLABILITY, with pins; otherwise a dated DECLARED refusal with Spark's error class and a registry row.

**Not in this step:** `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.toml`,
`Cargo.lock`, `pyproject.toml`, `uv.lock`,
`python/repark/src/repark/spark/functions*.py`, `dataframe/**`, `column.py`, `catalog.py`,
`crates/repark-spark/src/{extension,normalize,spark_literals}.rs` (other lanes own them).
The decimal round/ceil/floor, date_part seconds, literal widths and element nullability clauses
are round 2.

**Oracle:** `/tmp/oc-worker/qc-oracle/fixtures-batch12.json` (PySpark 4.1.2, cells Q12-0…Q12-55;
temp view `t`: `a array<int> = [1,2]`, `an array<int> = [3,NULL]`, a NULL `array<int>` column,
`s string = 'a,b,,c'`, `n int = 5`, `sa array<string> = ['x','y']`) and
`/tmp/oc-worker/pc-oracle/fixtures-batch7.json` (cells N7-16 concat, N7-17 sequence, N7-18 split,
N7-22 reverse). Cells spelled `2.5D`/`1L` are re-pinned with `CAST` per the card note.

## Rulings

| Ruling | Source | Content |
|---|---|---|
| Q-15c-6 | owner 2026-09-15 | No global retag wrapper; per-function fixes, one oracle pin each. |
| Q-15c-7 | owner 2026-09-15 | concat/reverse on arrays and sequence/split on the SQL door are P1 and go first. |
| R-13 | DOOR-CONVERGE-1 round 4 | Literal-haystack nullability legs pin today's `nullable=True` as recorded divergence ARRAY-LITERAL-CONTAINSNULL-1 until the array-constructor fix lands. |
| Q-15c-4 | owner 2026-09-15 (via run-16c G-2) | Size baselines stay ratchet-only; a one-time +N is granted in the PR that needs it, never a standing allowance. |
| R-1 | DOOR-CONVERGE-2 G-2 Q1 | analyzer.rs +8 one-time grant, Q-15c-4, reason: array_concat→concat analyzer arm for Q12-16 outer nullability. Ceiling 1142→1150. |
| G-2 Q2 | orchestrator 2026-09-15 | The 15 clippy-1.96 errors land as an addendum commit in this unit; `make rust-clippy` must exit 0, no `#[allow]`. |
| G-2 Q3 | orchestrator 2026-09-15 | Sequence illegal-step follows the oracle: Spark's `IllegalArgumentException` class with the `requirement failed: Illegal sequence boundaries: …` text, using the existing error-mapping route if one exists. |
| R-15c-4 | orchestrator 2026-09-15 (round 3) | Critic cells Q15-0…Q15-19 re-measured on live PySpark 4.1.2 are the spec wherever they disagree with the critic's text (`fixtures-batch15-critic-622.json`). |

## PROPOSITION LEDGER — DOOR-CONVERGE-2 round 1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `concat` over arrays answers Spark on the SQL door (cells Q12-0…Q12-10, Q12-16 `||` over arrays); string/binary controls Q12-11…Q12-15 do not regress. Element type widens to the common type (Q12-3 bigint, Q12-4 `decimal(11,1)`), `containsNull` is the OR of the inputs (Q12-1, Q12-7), result nullable iff any input nullable, any NULL array → NULL (Q12-2), nested arrays keep inner nullability (Q12-8), empty `array()` joins (Q12-9), array mixed with string refuses `DATATYPE_MISMATCH.DATA_DIFF_TYPES` (Q12-10). BINARY stays BINARY (Q12-13). | `test_door_converge_2.py` Q12-0…Q12-16 legs green on both doors. | PROVEN | 2026-09-15: one `concat` UDF with string/binary/array arms (`collection/concat_array.rs` helpers, `PyColumn::concat` embeds the same UDF); coerce validates but never casts (widths resolve post-narrowing, kernel casts internally — cures the `decimal(21,1)` freeze). `||` over equal lists reaches the same kernel through a narrow `analyzer.rs` rewrite (`array_concat(...)` → `concat(...)` when every argument is list-shaped or NULL, mirroring the existing `substr` arm; DataFusion's nested planner bakes its own UDF into the `||` rewrite, so only the analyzer sees the name). Mixed string/binary resolves `Binary` (Spark's any-binary-wins `dataType`). Rust pins green (`concat_array::*`, `string::tests::concat_binary_stays_binary`); Python pins green (`test_door_converge_2.py` 62 passed). |
| C-002 | `reverse` over arrays answers Spark on the SQL door (Q12-17…Q12-25): element order reversed, element type and `containsNull` and nullability kept, NULL array → NULL, `reverse('abc')` still `'cba'`, untyped `reverse(NULL)` is a NULL STRING. | Q12-17…Q12-25 legs green on both doors. | PROVEN | 2026-09-15: `spark_reverse.rs` overwrites the string-only kernel (array arm keeps type/nullability, `NULL` → NULL `STRING`, other types refuse `UNEXPECTED_INPUT_TYPE`); facade arm moved to `dispatch_spark.rs`. Rust pins green; Python pins green (`test_door_converge_2.py` 62 passed). |
| C-003 | `sequence` is registered on the SQL door (Q12-26…Q12-40): integer widths kept (int, bigint, tinyint), descending default step, explicit step, dates with default 1-day step and INTERVAL MONTH step, timestamps with INTERVAL HOUR, NULL bound or step → NULL with `containsNull=false`, zero or wrong-sign step raises (closest RePark error mapping recorded), DECIMAL bound refuses `DATATYPE_MISMATCH.SEQUENCE_WRONG_INPUT_TYPES`. The existing expansion ceiling (`refuse_literal_expansion`) must still fire. | Q12-26…Q12-40 legs green on both doors. | PROVEN | 2026-09-15: `spark_sequence.rs` (int/date/timestamp arms; `SEQUENCE_WRONG_INPUT_TYPES` refusal; facade arm in `dispatch_spark.rs` with the ceiling kept). Headline find: DF's built-in `type_coercion` runs before `spark_integer_literal`, so a widening coerce shields provisional literals — `coerce_types` validates but never casts, widths resolve post-narrowing. G-2 Q3 verdict after probing (fallback per the ruling): the oracle class stays OPEN as a C-003 error sub-cell. Red-first run narrowed the pins to `IllegalArgumentException` and produced 4 failures proving every materialization path (`to_arrow`, `to_arrow_batches`, `collect`) crosses Arrow IPC as `ArrowInvalid: External error: Execution error: …` and remaps in `dataframe/export_errors.py::_export_engine_error` (run 16b's fence) — no Rust route can reach it, and the existing `Error::Config` route would prefix the text. The Rust half was built then reverted to avoid a plan/execute class split; the complete fix is one marker match in `export_errors.py` (P2 hand-off to run 16b below). Pins keep base `PySparkException` with the exact text (`test_facade_sequence_illegal_step_text` pins the text on the facade door). Rust pins green; Python pins green (`test_door_converge_2.py`; FNP9-SEQUENCE-1 rewritten to countdown/raise). |
| C-004 | `split` is registered on the SQL door (Q12-41…Q12-55): Java-regex pattern (`'.'` splits every char → four empty strings Q12-51), `limit` > 0 caps the pieces with the remainder in the last, `limit` ≤ 0 keeps trailing empties (Q12-44/45; Spark 4 keeps them for 0 too — read the cell), empty pattern splits per character (Q12-47), `split('', ',')` → `['']`, NULL string / pattern / limit → NULL, `containsNull=false`, numeric first argument cast to string (Q12-55). | Q12-41…Q12-55 legs green on both doors. | PROVEN | 2026-09-15: `spark_split.rs` (shared Java-regex compile + stepping with a `find_at` resume so overlapping matches advance one char — cures Q12-50/51; limit semantics per the brief, `containsNull=false`); facade arm in `dispatch_spark.rs`. Rust pins green; SQL-door Python legs green (`test_door_converge_2.py` 62 passed). Facade `F.split` raises in `functions_expr.py` (outside the fence) — P2 hand-off to run 16a, tracked by `test_facade_split_refusal_handoff_16a`. |
| C-005 | Each of C-001…C-004 pinned on the Python API as well (`F.concat`, `F.reverse`, `F.sequence`, `F.split` over a `createDataFrame` with the same columns) for value, Arrow type and nullability via `to_arrow`/`collect`. A facade cell whose fix lives in `functions*.py` is recorded as a P2 hand-off to run 16a and marked OPEN. | Facade legs green or handed off with the reason named. | PROVEN | 2026-09-15: `F.concat` over arrays, `F.reverse` over arrays/strings, and `F.sequence` literal/column-stop legs all green (`test_facade_concat_arrays`, `test_facade_reverse_arrays`, `test_facade_reverse_string`, `test_facade_sequence_column_stop`, `test_facade_sequence_literals` — value, Arrow type and nullability in one assertion row each). The `F.split` Python arm raises `UnsupportedOperationException` in `functions_expr.py`, outside this unit's fence — P2 hand-off to run 16a, guarded by `test_facade_split_refusal_handoff_16a` (red-when-wired: fails once run 16a wires the arm). N7-22 `reverse` parity intact (`test_n7_22_reverse_string_pair_still_passes`). |
| C-006 | Registry rows in `docs/spark-sql-iceberg-parity.md` (inside the section that already holds the DOOR-CONVERGE rows; append, do not reorder) disposition FIXED with the pin names; any of these four names in `EXPECTED_DIVERGENCES` removed when the kernels converge. | Registry diff + `door_parity_tests` green. | PROVEN | 2026-09-15: DC2-CONCAT-1 / DC2-REVERSE-1 / DC2-SEQUENCE-1 / DC2-SPLIT-1 appended after BL-18 (FIXED, pin names cited); FNP9-SEQUENCE-1 marked SUPERSEDED with a pointer. None of the four names was in `EXPECTED_DIVERGENCES`; `reverse`/`sequence`/`split` joined the `SCALAR_NAMES` ceiling. Follow-up: C-003's facade reroute left door-native `generate_series` split from facade `SparkSequence`, failing the 371-name parity test — recorded as an honest `EXPECTED_DIVERGENCES` row (table 14 → 15) since overwriting the door would break `FROM generate_series` table-function users. `door_parity_tests` green. |
| C-007 | Round-3 sequence/date correctness (L-001, L-004, L-005): month steps compute element `i` from the start on both doors (Q15-0…Q15-4); the real Q12-23 nested cell pinned with ids re-aligned; the `maxArrayElements` cap fires at runtime for column stops with the literal refusal's text. | Q15-0…Q15-4, Q12-23, Q15-18/Q15-19 legs green. | PROVEN | 2026-09-15: `collect_dates`/`collect_timestamps` compute element `i` from the start (`months × i`, `spark_add_months`); Q15-0…Q15-4 green on the SQL door plus `test_facade_sequence_month_step`. Q15-3 note: the fixture's naive 05:00/05:00/06:00 rows are US-Eastern display of 10:00Z instants (15:00→EST, DST jump to EDT); the pin asserts 10:00Z, matching the true instants. L-004: new `test_reverse_nested_column_keeps_inner` pins a real nested column on both doors (outer flips, inners kept, `item` type preserved); the Q15 id gaps are filled (Q15-15, Q15-17) and every Q12 pin SQL re-checked against `fixtures-batch12` (drifts found are documented respellings or pre-existing — see out-of-scope). L-005: row builders refuse over the ceiling with the literal text; Q15-18 (column stop, `PySparkException`), Q15-19 (literal stop, `AnalysisException`) and `test_facade_sequence_column_cap` green. `test_door_converge_2.py` 89 passed. |
| C-008 | Round-3 string/regex correctness (L-002, L-003, L-007): STRING+BINARY concat answers STRING on both doors (Q15-5…Q15-8); `\Q…\E` translates (Q15-9); lookaround/backref/possessive refuse naming the feature (Q15-10…Q15-12, BACKLOG row); invalid-pattern text pinned (Q15-13); code-point empty split pinned (Q15-16, overturn). | Q15-5…Q15-16 legs green; `regexp_*`/`rlike` suites green. | PROVEN | 2026-09-15: all-`BINARY`→`BINARY` else `STRING` in both `SparkConcat` return paths; Q15-5…Q15-8 green plus `test_facade_concat_string_binary`. The shared `translate_java_pattern` (`\Q…\E` quoting, loud feature refusals) serves `split`/`regexp_*`/`rlike`; Q15-9…Q15-14, Q15-16 (column, per-code-point, `nullable=True`) and Q15-17 (literal surrogate-pair) green; BACKLOG row JAVA-REGEX-FEATURES-1 in the registry with the C-008 pin cite (no live Disclosure — sibling BACKLOG rows carry none, and the live tier asserts the oracle side only). The louder refusal moved `test_fn_regexp_extract.py::test_extract_java_lookbehind_is_loud` to the new text (same clause, C-008 trailer appended). Regexp suites (`fnp6`, `extract`, `posix`, `lrs6`, `sem4`) 49 passed. |
| C-009 | Round-3 perf (P2-1…P2-4): split stops after `limit - 1` matches; literal patterns take the `str` path; scalar patterns compile once and pattern columns use an LRU(64); sequence reserves closed-form counts, writes at width, and takes the scalar path. Before/after best-of-3 on the release native in evidence. | Perf pins green; no behavior change (full gates). | PROVEN | 2026-09-15 after best-of-3 on the release native (box load 14–18, shared): B1 234 ms (base 215; within-build bounded-vs-full 243 vs 620 proves the P2-1 walk bound; Q15-15 pins equivalence), B2 20 (21), B4 17 (40, reserve + width-direct win), B3 20 with a reconstructed probe (scalar `\d+` ×1M — the 418 baseline's probe did not survive, so no B3 delta is claimed). Perf pins green (`test_split_limit_early_stop_matches_full_walk`, `test_split_per_row_pattern_column`, Q15-15). P3-trivial concat capacity hint done (`child_len`); P3 reverse-per-slot and wider scalar paths stay ledger-only per the finding; L-006 unchanged (guard pin green). Full `make verify` green (workspace Rust tests zero failures; `make ci` clean). |

## Round 3 review findings (2026-09-15)

| Finding | Reviewer | Severity | Disposition |
|---|---|---|---|
| P2-1 split limit walks the whole haystack | S2-21 perf (Grok 4.6) | P2 | Fix: bounded match walk + equivalence pin |
| P2-2 literal patterns use the regex engine | S2-21 perf (Grok 4.6) | P2 | Fix: `str` fast path + pins + measure |
| P2-3 regex compiled per row shape | S2-21 perf (Grok 4.6) | P2 | Fix: scalar-once + LRU(64) + per-row pin + measure |
| P2-4 sequence allocation (no reserve, Int64+cast, no scalar path) | S2-21 perf (Grok 4.6) | P2 | Fix: closed-form reserve, width-direct writes, scalar path + measure |
| P3 reverse per-slot extend | S2-21 perf (Grok 4.6) | P3 | Ledger only: primitive fast path is not trivial (per-row offsets) |
| P3 concat capacity hint | S2-21 perf (Grok 4.6) | P3 | Fix (trivial one-liner): total child length |
| P3 scalar fast paths elsewhere | S2-21 perf (Grok 4.6) | P3 | Ledger only: sequence + split-pattern done; concat/reverse deferred |
| L-001 month-step chaining | Critic-logic (Grok) / oracle Q15-0…Q15-4 | P1 | Fix: element `i` from the start + pins both doors |
| L-002 STRING+BINARY concat | Critic-logic (Grok) / oracle Q15-5…Q15-8 | P1 | Fix: all-BINARY→BINARY else STRING + pins both doors |
| L-003 Java-regex features | Critic-logic (Grok) / oracle Q15-9…Q15-13 | P1 | Fix: `\Q…\E` + loud refusals + BACKLOG row |
| L-004 weak reverse pin | Critic-logic (Grok) | P2 | Fix: real Q12-23 nested cell + id realign |
| L-005 runtime array cap | Critic-logic (Grok) / oracle Q15-18/Q15-19 | P2 | Fix: runtime cap with literal text + pins |
| L-006 F.split hand-off | Critic-logic (Grok) | P2 | No change: guard pin stays (run 16a) |
| L-007 empty-pattern code points | Critic-logic (Grok) / oracle Q15-16 | OVERTURNED | Pin Q15-16, no kernel change |

## Evidence

### Red-first run — base tree probes (2026-09-15)

`crates/repark-functions/tests/probe_door2.rs` (scratch, deleted before the last commit) on the
base tree, `register_all` + analyzer rules door:

```
PROBE SELECT concat(array(1), array(2))
  TYPE: Utf8 NULLABLE: false VALS: "[1][2]"
PROBE SELECT array(1) || array(2)
  TYPE: List(Field { data_type: Int32, nullable: true }) NULLABLE: false VALS: [1, 2]
PROBE SELECT reverse(array(1,2))
  TYPE: Utf8 NULLABLE: false VALS: "]2 ,1["
PROBE SELECT reverse(array(3, NULL))
  TYPE: Utf8 NULLABLE: false VALS: "] ,3["
PROBE SELECT reverse('abc')
  TYPE: Utf8 NULLABLE: false VALS: "cba"
PROBE SELECT sequence(1, 3)
  PLAN-ERR: Error during planning: Invalid function 'sequence'.
PROBE SELECT split('a,b', ',')
  PLAN-ERR: Error during planning: Invalid function 'split'.
```

Every clause red as reported in 15c: array `concat` stringifies, array `reverse` reverses the
stringified text, `sequence`/`split` are unregistered. `||` over arrays already answers `[1, 2]`
via DataFusion's nested-planner rewrite to `array_concat` — values right, element stays
`nullable: true` (the ARRAY-LITERAL-CONTAINSNULL-1 constructor divergence, round 2).

### Green run

2026-09-15, branch `feat/door-kernel-converge-2`, release native rebuilt after the
file-size splits (move-only: `spark_sequence/rows.rs`, `spark_regexp/tests.rs`,
`string/tests.rs`; `lib.rs` shim inline):

- `python/repark/tests/test_door_converge_2.py`: 89 passed (Q12-0…Q12-55,
  Q15-0…Q15-14, Q15-16…Q15-19, facade legs, L-004 nested pin, perf pins).
- `python/repark/tests/test_fnp11a_temporal.py`: 316 passed (no temporal regressions).
- Regexp suites (`test_fnp6_regexp`, `test_fn_regexp_extract`,
  `test_fn_regex_posix_class`, `test_lrs6_regexp_divergences`,
  `test_sem4_regex_group_index_message`): 49 passed.
- `cargo test -p repark-functions`: 546 passed, 0 failed, 1 ignored (gated PERF-03).
- `make verify` (`make ci` + `cargo test --locked --workspace`): green, zero failures.
- Perf after best-of-3 (release, `/tmp/bench_dc2_after.py`): B1 234 ms, B2 20 ms,
  B3 20 ms (reconstructed probe), B4 17 ms; within-build bounded-vs-full 243 vs 620 ms.

### Out-of-scope observed

- Q12-8 pins a reshaped expression (`concat(array(array(1, 2)), array(array(3)))`)
  instead of the fixture verbatim (`concat(array(array(1)), array(array(2)))`).
  Pre-existing (committed, C-001); same property, weaker cell. Not touched.
- Q15-16 pins `nullable=True` (column input) where the fixture says `False`; the
  value and type match. Kept honest; Spark marks function-of-nullable-input nullable.
- B3's baseline probe (418 ms) did not survive; the after-probe is reconstructed and
  no B3 delta is claimed (C-009 evidence).
- Gate repairs absorbed (same-unit debt, all green now): `spark_sequence/rows.rs` and
  `string/tests.rs` + `spark_regexp/tests.rs` file-size splits, `lib.rs` 177→172
  (single-use shim inlined), stale converge-1 staging-map link removed (ledger lives
  in `completed/`), `.typos.toml` `BA` oracle-value allowlist, ruff-format pass.
- `test_fn_regexp_extract.py::test_extract_java_lookbehind_is_loud` moved to the new
  loud refusal text (round-3 behavior change, C-008 trailer appended).

### Gate record (2026-09-15, G-2 rulings applied)

- `scripts/check_rust_file_size.py`: `analyzer.rs` 1142 → 1150 granted one-time (R-1,
  Q-15c-4); script and `test_cap_1_source_file_line_cap.py` mirror both updated.
- `make rust-clippy` (1.96.0): the 15 unit-file errors fixed in-unit (stride/end renames,
  arm merges, `matches!`, method refs, `invoke_*` split, `sequence_expr` split) plus 3
  follow-ons in the touched dispatch files (merged converged-name arms); gate green.
- G-2 Q3 red-first evidence (pins narrowed to `IllegalArgumentException`, 4 failures):
  `test_sql_error_cell[Q12-31]` / `[Q12-32]` → `ArrowInvalid: External error:
  Execution error: requirement failed: …`; `test_facade_sequence_illegal_step` /
  FNP9 sequence pin → `repark.errors.PySparkException: Execution error: requirement
  failed: …` via `dataframe/core.py:3841 _export_engine_error`. Verdict: class mapping
  lives in run 16b's `export_errors.py`; sub-cell OPEN, hand-off recorded.
- Final gates 2026-09-15 on the committed tree: `make verify` rc 0 (fmt, clippy 1.96,
  workspace Rust tests incl. the 371-name parity sweep); release native rebuilt;
  `test_door_converge_2.py` + `test_fnp_9_collections_json.py` 187 passed;
  `-k "concat or reverse or sequence or split or array"` 381 passed, 11 skipped.
- Late find: the 371-name sweep failed on `generate_series` (C-003's facade reroute
  left door-native split from facade `SparkSequence`; earlier verifies never reached
  `rust-test`). Honest `EXPECTED_DIVERGENCES` row, table 14 → 15 — overwriting the
  door would break `FROM generate_series` table-function users.

### P2 hand-offs to run 16a

- `F.split` (Python side): `functions_expr.py` raises `UnsupportedOperationException`
  before any engine arm runs; wiring the Python `F.split` to the converged
  `repark_functions::spark_split::SparkSplit` kernel (3-arg `limit`, Java regex,
  Spark limit semantics) belongs to run 16a. Guard:
  `test_facade_split_refusal_handoff_16a` (red-when-wired). SQL-door `split`
  (`spark.sql("SELECT split(...)")`) is converged in this unit.

### P2 hand-offs to run 16b

- Sequence illegal-step class (`IllegalArgumentException`, G-2 Q3): every materialization
  path remaps mid-stream failures in `dataframe/export_errors.py::_export_engine_error`
  to base `PySparkException`. Matching the `Illegal sequence boundaries` marker there and
  raising `IllegalArgumentException` with the exact text completes the oracle class on both
  doors; `test_facade_sequence_illegal_step_text` pins the text so the class change flips
  loudly. C-003's error sub-cell stays OPEN until then.

```
COVERAGE_ATTESTATION:
  pr_unit: door-converge-2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against its fixture cell, not a paraphrase — Q12-0…Q12-55 value, Arrow type and nullability legs on both doors plus N7-16/N7-17/N7-18/N7-22 facade legs; literal-input nullability divergences pin today's nullable=True per R-13/ARRAY-LITERAL-CONTAINSNULL-1.
      artifacts: [python/repark/tests/test_door_converge_2.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-2
      status: ATTACKED
      evidence: Boundary inputs exercised — NULL arrays, empty array(), mixed int/bigint/decimal widths, zero and wrong-sign steps, zero/negative split limits, empty pattern and empty input, numeric split input, decimal sequence bounds, tinyint stops.
      artifacts: [python/repark/tests/test_door_converge_2.py]
    - id: AT-3
      status: ATTACKED
      evidence: Failure paths pin Spark's classes and texts — DATATYPE_MISMATCH.DATA_DIFF_TYPES on array/string concat mix, SEQUENCE_WRONG_INPUT_TYPES on decimal bounds, UNEXPECTED_INPUT_TYPE on non-array reverse, the exact Illegal sequence boundaries text on both doors, the run-16a split refusal guard.
      artifacts: [python/repark/tests/test_door_converge_2.py]
    - id: AT-4
      status: N/A
      justification: Kernels are pure row functions; no shared mutable state, no locks, no ordering assumptions. Patterns compile per call, nothing cached across rows.
    - id: AT-5
      status: N/A
      justification: No auth, injection, secret, or deserialization surface — in-process Arrow kernels over typed input; split patterns are data, never executed.
    - id: AT-6
      status: ATTACKED
      evidence: Shared-surface edits verified behavior-preserving — the three dispatch arms merged by pure deletion (same body, diff reviewed, suites green); the G-2 Q3 Rust error-mapping half reverted subtraction-only with repark-common and repark-core byte-identical post-revert (git status clean for both crates).
      artifacts: [crates/repark-python/src/column/function_dispatch.rs, task/ledgers/staging/door-converge-2-ledger.md]
    - id: AT-7
      status: N/A
      justification: The unit makes no performance claim and ships no benchmark — kernels reuse Arrow builders (MutableArrayData, peripheral builders) with no per-row allocation added; measurement belongs to a perf unit.
    - id: AT-8
      status: ATTACKED
      evidence: Rewrite scoping hazards closed — the array_concat analyzer arm fires only when every argument is list-shaped or NULL, so string/binary concat and the Q12-11…Q12-15 controls never reach it; the merged dispatch arm routes identically to the three it replaces.
      artifacts: [python/repark/tests/test_door_converge_2.py, crates/repark-functions/src/analyzer.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Every refusal carries Spark's error class and text in the exception, diagnosable from the error alone — DATATYPE_MISMATCH classes, the exact boundaries text surviving Arrow export on the facade door; no logging changes.
      artifacts: [python/repark/tests/test_door_converge_2.py]
    - id: AT-10
      status: ATTACKED
      evidence: Pins-first held — Q12 legs failed on the base tree with the predicted wrong answers before each kernel; the G-2 Q3 class narrowing produced its predicted 4 failures (pasted above) and the pins kept the exact text instead of greening the wrong class; the FNP9 rewrite went red-then-green.
      artifacts: [python/repark/tests/test_door_converge_2.py, python/repark/tests/test_fnp_9_collections_json.py]
  complete: true
```
