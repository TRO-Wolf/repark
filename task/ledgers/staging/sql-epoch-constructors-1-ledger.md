# Unit ledger — SQL-EPOCH-CONSTRUCTORS-1 · `timestamp_seconds` / `timestamp_millis` / `timestamp_micros` on the Spark SQL door

**Date:** 2026-10-08 · **Branch:** `fix/sql-epoch-constructors-1` · **Base:** `40fc916f` (`main`)
**Model:** Muse Spark (muse-spark-1.3-contributor) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's product PR merges.

**Card:** [../../roadmap/mid-term/sql-epoch-constructors-1-card-2026-10-08.md](../../roadmap/mid-term/sql-epoch-constructors-1-card-2026-10-08.md).
**Registry:** the ZONE-HORIZON-RENDER-1 "do not exist" sentence, B-TZ-2 and the EX-7
`timestamp_seconds` schema half of
[../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md),
plus residue R-6 of the zone-horizon ledger.

**Why now.** Spark 4.1.2 carries all three epoch constructors as SQL built-ins; this
engine's Spark SQL door refused them as `UNRESOLVED_ROUTINE` while its DataFrame door
answered through DataFusion's `to_timestamp_*` kernels with the wrong types, no overflow,
truncated fractions and string parsing.

**Not in this unit:** `Cargo.toml`, `.github/`, `STATUS.md`, any Python product code.

## 1. What Spark answers (Step 0, live 4.1.2)

216 cells: 54 inputs × zones `UTC` / `America/New_York` × ANSI off / on, SQL door and
DataFrame door each with `unix_micros`, error class/SQLSTATE/first line and `df.schema`
type, plus `to_timestamp_*` probes. All four zone×ANSI groups are byte-identical.

- `timestamp_seconds` takes NUMERIC: integral scales exactly; fractional seconds keep
  their fraction (1.5 → 1500000 µs, 0.9 µs truncates to 0); double NaN/±Inf answer NULL;
  huge finite doubles saturate (±1E300 → ±max); decimals convert exactly or refuse.
- `timestamp_millis` / `timestamp_micros` take INTEGRAL only; doubles, floats, decimals,
  strings, booleans and timestamps refuse.
- Every type refusal is `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` 42K09, including a
  NULL of the wrong type. Every overflow is unclassified: integral `long overflow`;
  decimal `Overflow` only when the micros integer part passes 19 digits or exact
  micros leave the bigint range, else inexact micros refuse `Rounding necessary`
  even past the bound (fold 1, 2026-10-09, corrects "range wins over exactness").
  Both errors ignore ANSI.
- Spark refuses `to_timestamp_seconds` / `to_timestamp_millis` / `to_timestamp_micros`
  as `UNRESOLVED_ROUTINE`.

## 2. What the base answered

SQL door: `UNRESOLVED_ROUTINE` on all three names for every input. DataFrame door:
integral instants right but typed `timestamp[s]` / `[ms]` / `[us]` with no zone and
facade `timestamp_ntz` (the EX-7 `string` reading was already stale); no overflow;
fractions truncated (1.5 s → 1 s); timestamp-shaped strings parsed and answered;
timestamps passed through; decimal micros lost. 276 legs matched Spark as instants;
all hold after the change (§4).

## 3. The change

One parameterized UDF per spelling in `repark_functions::spark_epoch_ctor`, registered
from `register_all` and embedded by the facade through `dispatch_spark.rs` — one kernel
per name on both doors (the `expr_fn` file sits on its exact baseline, so no builder
was added there). Answers LTZ micros; the type gate refuses with Spark's class, state,
required kind and got-type; overflows carry Spark's core texts unclassified. The
non-Spark `to_timestamp_*` spellings keep their DataFusion kernels on both doors,
untouched. No Python product change: the facade schema follows the kernel's return
type. The native door keeps refusing the three names (declared, pinned).

## PROPOSITION LEDGER — SQL-EPOCH-CONSTRUCTORS-1 — 2026-10-08

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The 216-cell live Spark 4.1.2 oracle is recorded under `python/repark/tests` with values, error classes and types on both doors. | The recorder, the fixture and the banner. | PROVEN | `_record_sql_epoch_constructors_1.py` + `sql_epoch_constructors_1_spark_oracle.json`, banner 4.1.2, 54 inputs × 2 zones × ANSI off/on + `to_timestamp_*` probes. |
| C-002 | The Spark SQL door answers every value cell with Spark's micros, Arrow `timestamp[us, tz=UTC]` and facade `timestamp`. | One replay per group over the oracle. | PROVEN | `test_sql_door_matches_spark` (4 groups × 162 legs), green. |
| C-003 | The SQL door's refusals carry Spark's class and state: `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` 42K09 with the NUMERIC/INTEGRAL kind and Spark's got-type, and the overflow core text with no condition. | The same replay's refusal legs. | PROVEN | `test_sql_door_matches_spark` refusal legs, green. |
| C-004 | The DataFrame door answers every value cell with Spark's micros and schema. | One replay per group over the oracle. | PROVEN | `test_dataframe_door_matches_spark` (4 groups × 162 legs), green. |
| C-005 | The DataFrame door's refusals carry Spark's class, state and texts. | The same replay's refusal legs. | PROVEN | `test_dataframe_door_matches_spark` refusal legs, green. |
| C-006 | The native `repark.sql` door refuses all three names. | One refusal pin per name. | PROVEN | `test_native_door_refuses_the_spark_spellings`, green. |
| C-007 | Live Spark 4.1.2 re-derives every fixture cell. | The recorder's `--check` mode and the live pin. | PROVEN | `test_live_oracle_matches_committed_fixture` (runs under `REPARK_PARITY_LIVE=1`); the unit ran the recorder's `--check` against live Spark 4.1.2 with no drift. |
| C-008 | Each spelling resolves one kernel on both doors. | The door-parity guard over the converged arms. | PROVEN | `door_parity_tests.rs` `SCALAR_NAMES` gains the three spellings; `repark-python --lib` green. |
| C-009 | The 88-cell decimal-boundary extension (22 inputs × 4 groups) is recorded from live Spark 4.1.2 and replays equal on the SQL and DataFrame doors. | The recorder INPUTS, the 304-cell fixture, the both-doors replay. | PROVEN | `test_sql_door_matches_spark` + `test_dataframe_door_matches_spark` over 76 inputs (4 groups × 228 legs), green; the 216 older cells byte-identical. |
| C-010 | The kernel answers Spark's decimal order: inexact micros with at most 19 integer digits refuse `Rounding necessary` even past the bigint bound; `Overflow` only past 19 digits or for exact out-of-range micros. | The Rust boundary battery plus the order mutants. | PROVEN | `decimal_boundary_answers_spark_order` (17 refusals + 5 values), green; old-order restore and 1e19-drop mutants red. |

## 4. No worse than main

The 276 base legs that matched Spark as instants (DataFrame-door integral inputs and
numeric NULLs, all groups) answer the same instants at head; the branch-vs-head
comparison reports zero regressions. The `timestamp or datetime` facade subset moves
975 → 978 passed (the 3 new native-door pins), skips and xfails unchanged.
`test_fn_batch3.py` (which selects but never asserts the two columns) and
`test_functions_split_identity.py` pass unchanged.

## 5. Mutations

Four mutations against `spark_epoch_ctor.rs`, each reverted after: M1 scales seconds
by 1000 (killed), M2 gates seconds to INTEGRAL (3 tests killed), M3 wraps the
multiply and defaults overflow to 0 (the overflow test killed), M4 drops the UTC zone
(5 tests killed). All four killed by the module's 7 tests.

## 6. Out of scope, observed while measuring

- **O-1.** `to_timestamp_seconds` / `to_timestamp_millis` / `to_timestamp_micros`
  resolve on this engine's SQL door through DataFusion builtins where Spark 4.1.2
  refuses them; untouched by this unit.
- **O-2.** A `Decimal256` unscaled value past the `i128` range refuses `Overflow`
  without attempting the exact division; Spark has no 256-bit decimal (max precision
  38), so no Spark input reaches it.

## Fold 1 (2026-10-09) — the decimal boundary order

The verify verdict (FAIL, one S2, 8,514 cells, 0 regressions, 0 third answers) caught
`timestamp_seconds(decimal)`: inexact micros with at most 19 integer digits at or past
the bigint bound refused `Overflow` where Spark 4.1.2 refuses `Rounding necessary`
(120 cells). Cause: `decimal_seconds_to_micros` checked the `i64` range before the
remainder; Spark's `BigDecimal.longValueExact` answers `Overflow` first only past 19
integer digits. The §1 "range wins over exactness" line stated the wrong rule.

Re-measured on live Spark 4.1.2: the verdict's five values plus one each side of
every edge (MAX/MIN exact, ±half/±1/±1.5 micro, 19- vs 20-digit micros both signs,
exact vs inexact, a trailing-zero `(p,s)` twin) — 22 inputs, SQL literal, SQL
column, DataFrame column and parquet column, both doors, all agreeing (C-009). The
fix checks `|quotient| >= 10^19` first, then the remainder, then the `i64` range;
the Rust battery pins 17 refusals + 5 values and kills the old-order restore and
the 1e19-drop mutants (C-010). The harness now parses fractional literals as
decimals like the Spark door (`parse_float_as_decimal`); huge doubles spell
through string casts there since exponent literals never reach the kernel
(`spark_literals` rewrites them on the door).

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: sql-epoch-constructors-1
  complete: true
  reattested: []
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Clauses C-001..C-008 walked against behavior — the 216-cell live recording, the both-doors replay of every cell with value, Arrow type and facade type, the native-door pins, the live drift check and the door-parity guard.
      artifacts: [task/ledgers/staging/sql-epoch-constructors-1-ledger.md, python/repark/tests/test_sql_epoch_constructors_1.py, python/repark/tests/sql_epoch_constructors_1_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Boundary inputs run as oracle cells — the exact-fit and exact-overflow seconds/millis pairs, bigint extremes, 2100/1900/9999/0001 values, sub-micro doubles and decimals, NaN/±Inf, saturating doubles, huge+fractional decimals, wrongly-typed NULLs.
      artifacts: [python/repark/tests/sql_epoch_constructors_1_spark_oracle.json]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal shape is pinned on both doors — the DATATYPE_MISMATCH class, SQLSTATE, required kind and got-type, and the three unclassified overflow texts with no condition.
      artifacts: [python/repark/tests/test_sql_epoch_constructors_1.py]
    - id: AT-4
      status: N/A
      justification: A stateless scalar kernel — one pure function of its argument with no session read, no cache and no retained state.
    - id: AT-5
      status: N/A
      justification: No privileged action, no secret, no parsed path and no network or catalog touch; numeric input to timestamp output.
    - id: AT-6
      status: ATTACKED
      evidence: Every value cell compares micros, the Arrow type and the facade type against Spark; the 276 base Spark-equal legs re-compare at head with zero regressions.
      artifacts: [python/repark/tests/test_sql_epoch_constructors_1.py]
    - id: AT-7
      status: N/A
      justification: One row-wise pure kernel replacing one DataFusion kernel call per row; no added plan pass, no cache, no per-batch allocation beyond the output array.
    - id: AT-8
      status: ATTACKED
      evidence: DataFusion 54.1.0 `to_timestamp_*` read at its source (epoch scaling plus RFC3339 string parsing) and measured on the base DataFrame door; kept only for the non-Spark `to_timestamp_*` spellings, which Spark refuses and this unit does not touch.
      artifacts: [crates/repark-python/src/column/function_dispatch.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The new DATATYPE_MISMATCH text and the three overflow texts are pinned verbatim on both doors; the only intentional delta from Spark's sentence is the rendered argument (type, not expression).
      artifacts: [python/repark/tests/test_sql_epoch_constructors_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Four mutations run against the kernel (wrong scale, narrowed gate, wrapping overflow, dropped zone); all four killed by the module tests (§5).
      artifacts: [crates/repark-functions/src/spark_epoch_ctor.rs]
```
