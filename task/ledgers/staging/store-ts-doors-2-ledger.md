# Unit ledger — WO STORE-TS-DOORS-2 · a VALUES node inside INSERT … SELECT and the static-partition OVERWRITE refuse TIMESTAMP/DATE/`-NULL` sources like Spark

**Date:** 2026-09-29 · **Branch:** `fix/store-ts-doors-2` · **Base:** `57ebb57a` (`origin/main`, v1.5.1)
**Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Two silent pre-existing write doors from the v1.5.1 release differential,
both carried by v1.5.1: PE-18 — `INSERT INTO t SELECT a, b FROM VALUES (1,
TIMESTAMP'…') AS v(a, b)` into a BIGINT column stores epoch seconds where Spark
4.1.2 refuses `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`; PE-24 — `-NULL`
into a TIMESTAMP column through `INSERT OVERWRITE t PARTITION (p='a') VALUES (…,
-NULL)` stores NULL where Spark refuses. WO STORE-TS-TO-NUMERIC-1 (#885) closed
the same source classes on the other doors; this unit reuses #885's gate at the
two doors it missed.

**Step 0 (measured before any edit, 2026-09-29, zone UTC).** 78-statement matrix
plus 8 follow-up and 8 gap statements, RePark on `57ebb57a` against Spark 4.1.2
+ Iceberg 1.11.0 (`f0bb2e6a47d0ebda424ffd633fcea8644a597954`, session zone UTC
printed from a live session): the two repros; the VALUES-in-SELECT shapes through
a CTE (both spellings), a temp view, a join with a VALUES node (table side and
VALUES side), UNION of VALUES, a nested derived table with column aliases,
WHERE, and ORDER BY/LIMIT; the static-partition OVERWRITE with every #885 source
type (TIMESTAMP, `CAST(1 AS TIMESTAMP)`, TIMESTAMP column, DATE, `-NULL`, NULL,
INT, DECIMAL, STRING, BOOLEAN, `CAST(… AS TIMESTAMP_NTZ)`) into TIMESTAMP,
BIGINT, DATE and BOOLEAN targets; the SELECT, column-list, dynamic-partition and
BY NAME partition forms as controls.

Classes on base: 14 statements store where Spark refuses (the fix set: 9
VALUES-in-SELECT TIMESTAMP/`CAST(1 AS TIMESTAMP)` into BIGINT/DOUBLE, 3
`-NULL` through static-partition OVERWRITE VALUES, and 2 gap statements —
ORDER BY/LIMIT and SELECT-form `-NULL`); 4 both-refuse message differences the
fix retexts to Spark's exact refusal (DATE into INT via `DATATYPE_MISMATCH`
twice, VALUES⋈VALUES via `Inconsistent data length`, NTZ via
`DATATYPE_MISMATCH`); 4 both-refuse message differences kept as on base (view via
the WI-1 `repark_insert_store_assignment` text, CTE-before-INSERT via `not
implemented`, the arity probe bug, `unix_seconds`/`year` function-resolution
failures); 2 STRING stores owned by STORE-STRING-ASSIGN-1 kept as on base; every
other cell already matches Spark outcome and value (zone-clean readbacks via
`CAST AS STRING`: the first probe's 5-hour skew was driver-zone rendering in
`collect()`, not a product difference).

**Cause.** PE-18: the analyzer insert gate judges only a conform cast over a bare
source column, so a VALUES node inside a SELECT falls into its named residual
(`insert_gate.rs` `a_literal_values_row_is_the_named_residual_and_is_not_gated`),
and #885's VALUES gate reads only an INSERT whose body is a VALUES list.
PE-24: #885's INSERT `-NULL` gate maps the source against the full table width,
so a static-partition source (narrower than the table) bails on the width check,
and the partition-overwrite staging gate judges `Null` as storable anywhere.

**Fix.** Refusals only; nothing new is admitted. Both reuse #885's gate and its
`source_leaves` rule unchanged; no second copy.
- `void_type/select_values_arms.rs` (new): resolves an INSERT source into
  VALUES-cell arms — transparent projections (plain column refs, `*`) over
  derived tables, CTEs, UNION/INTERSECT/EXCEPT arms and joins, each output
  position mapped to the VALUES cells that flow into it, anything else (table
  and view columns, literals, CASTs, functions, ambiguous or unresolvable refs,
  unexpandable stars) left unmapped. `ltz_values_store.rs` projects each arm's
  rows and judges them through the unchanged `check_row`/`refuse_value`, so the
  STRING silence and the leaf-reliability rule apply exactly as on the VALUES
  door. The direct-VALUES path is untouched (its table load moved verbatim into
  `load_presented`).
- `void_type/insert_source_types.rs`: `refuse_partition_overwrite_sources` maps
  the filled partition-overwrite source onto the table columns minus the static
  partition columns (or by column list, bailing when a static column is listed)
  and runs the shared `refuse_negated_null_writes`. Called once from
  `execute_partition_overwrite`, so the static, dynamic, listed and BY NAME
  partition forms all pass it; the listed, dynamic and BY NAME forms already
  refused with the identical text through #885's earlier gate, so only the
  static-unlisted `-NULL` cells change.

**After the fix (same matrix, same recorded Spark answers).** All 14 fix-set
cells refuse with Spark's class, first line and SQLSTATE (`KD000`), byte-identical
to the recorded Spark text, and write nothing; the 4 retexted cells now also
carry Spark's exact text. 63 matrix cells are byte-identical to base (outcome,
text and stored value); zero cells where head refuses and base and Spark both
store; zero stored-value changes.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | TIMESTAMP, `CAST(1 AS TIMESTAMP)`, TIMESTAMP_NTZ and DATE sources through a VALUES node inside INSERT … SELECT into numeric columns refuse with Spark's recorded class, SQLSTATE and message body, and write nothing; the direct, star, CTE, UNION, alias, join, VALUES-join, WHERE and ORDER BY/LIMIT shapes are each pinned, and explicit `CAST(b AS BIGINT)`, INT and NULL still store. | `test_store_ts_doors_2.py` replays the recorded `vselect/*` refusal and store cells; the resolver classifier tests. | PROVEN | 15 refusal cells and 4 store cells equal `store_ts_doors_2_spark_oracle.json`; on `57ebb57a` 10 stored, 4 refused with a different text, and the WHERE shape stored (measured on a base rebuild). 8 resolver classifier tests pass. |
| C-002 | `-NULL` into TIMESTAMP, DATE and BOOLEAN columns through static-partition OVERWRITE VALUES and SELECT refuses with Spark's text naming `"DOUBLE"`, and writes nothing; `-NULL` into BIGINT still stores NULL; the listed, dynamic and BY NAME partition forms keep refusing with the identical text. | The replay test's `partovw/*` cells. | PROVEN | 6 refusal cells and 1 store cell equal the oracle; 4 refusals (VALUES into TIMESTAMP/DATE/BOOLEAN, SELECT into TIMESTAMP) stored NULL on `57ebb57a`. |
| C-003 | STRING into TIMESTAMP through the VALUES-in-SELECT door refuses with Spark's class (24 cells: the 6 c1 STRING sources × star, direct, CTE and UNION-STRING shapes); every other cell where base already matches Spark is unchanged — all other STRING-source stores, `CAST(ts AS BIGINT)`, the temp-view shape (WI-1 text), the CTE-before shape, and the #885 and WI-1 suites. | The Step 0 matrix re-run on head; the in-test view and STRING pins; the #885, WI-1 and NTZ suites green; the fold replay of c1/c3/c4/c5/c7 plus 31 fold cells. | PROVEN | The 24 STRING-into-TIMESTAMP cells equal Spark (base stored 8 and raised CAST_INVALID_INPUT on 16); the 2113-cell fold replay shows zero moves away from Spark; view and STRING pins pass; 165 tests green across the four files. |

## Mutation record (2026-09-29)

Each mutation rebuilt the native module (`make develop`), ran the replay test,
and was reverted; the final tree rebuilds green.

| # | Mutation | Replay cells red | Other files |
|---|---|---|---|
| M-PE18 | The SELECT-arm call site is skipped (`refuse_select_arms` unreachable) | 15: every `vselect/*` refusal; all stores and `partovw/*` stay green | view and STRING pins green |
| M-PE24 | The partition-overwrite call site is skipped (the helper call removed) | 4: `partovw/values/negnull/{ts,date,bool}`, `partovw/select/negnull/ts`; listed/dynamic stay green through the earlier gate | view and STRING pins green |
| M-VT1 | The fold skip is forced off (`is_datetime_type` returns false) | 12: every `p01`–`p12` store pin refuses again; `p13`–`p21` and all in-test pins stay green | the 26 original oracle cells green |
| M-VT1a | The R1 probe is forced off (`probed_type` returns None, arm plans on) | 6: every `q01`–`q06` function-cell pin refuses again; `q07`, `q08`–`q16` and all in-test pins stay green | all 47 pre-fold oracle cells green |
| M-VT1b | The R1 probe and the R2 arm plans are both forced off | 17: every `q01`–`q16` pin refuses again, plus `p09` (its NTZ sibling needs the probe); every other pre-fold cell stays green | the `values_current_ts` in-test pin refuses again |
| M-R2 | The R2 arm plans are forced off (`arm_plan` returns Failed, probes on) | 9: every `q08`–`q16` residue pin refuses again; `q01`–`q07` and all pre-fold cells stay green | the `current_ts_in_from`, `date_fn` and `join_unqual` in-test pins refuse again |

**Neighbours (2026-09-29).** The 78-statement Step 0 matrix plus 16 follow-up/gap
statements answer byte-identically on `57ebb57a` and on this unit except the 18
intended cells (14 store→refuse, 4 refuse-retext, each now byte-identical to the
recorded Spark answer). The `unix_seconds`/`year`-over-VALUES refusals, the view
WI-1 text, the CTE-before `not implemented` text and the STRING stores are
unchanged pre-existing divergences, each owned elsewhere (function resolution,
R-STN-1, the CTE-before door, STORE-STRING-ASSIGN-1).

**Existing pins changed (0).** No existing test file is touched; `git status`
shows only the four product files, the new resolver module, the new replay test
plus oracle, this ledger, and three `map.md` files.

## Verifier fold (2026-09-29, PR #894)

The verifier found one regression, VT-1 (S1): a STRING cell in a VALUES arm of
a UNION, UNION ALL, EXCEPT or INTERSECT refused into TIMESTAMP when a sibling
arm was TIMESTAMP, TIMESTAMP_NTZ or DATE. Spark widens the column and stores,
and base stored. The resolver judged each arm's cells on their own literal
types. The fold rulings (R1/R2) replace arm-local judgment for exactly that
cell: a mapped STRING cell (static source type STRING) in a set-operation arm
is skipped if and only if some sibling arm at the same position has static
type TIMESTAMP, TIMESTAMP_NTZ or DATE. A NULL, BIGINT, BOOLEAN, STRING or any
other sibling does not trigger the skip, and every non-STRING cell keeps being
judged exactly as before, on UNION and on EXCEPT/INTERSECT alike (right-operand
non-STRING cells keep refusing, so o5/o6 match Spark's refusal).

**Fix.** `select_values_arms.rs` records provenance beside the cell map: table
and column for a table-arm position, the literal expression for a
SELECT-no-FROM position, carried through derived tables, CTEs and set-operation
merges. `ltz_values_store.rs` types each sibling in `refuse_select_arms` —
mapped VALUES columns by the existing `leaf_type` (a column qualifies only when
every known leaf is datetime or NULL and at least one is datetime), table
provenance through the existing catalogs handle, SELECT-no-FROM expressions
through `leaf_type` then the existing probe — and substitutes the silent NULL
cell for a skipped STRING cell. No new type rule, no new cast table, no second
copy of #885's gate, no whole-source plan. A sibling that cannot be resolved
with those tools does not skip: `SELECT *` over a real table, a star over
mixed join factors, and non-column expressions over tables keep today's
judgement; no target cell needs them.

**Proof.** All 31 fold cells and the 13 `vt1/*` store cells equal Spark's
outcome, and its class wherever `51688c1c` already equalled it. Replaying
c1_string (432), c3_allow (633), c4_refuse (605), c5_extra (64) and c7_fn (348)
shows 0 cells that equalled Spark on `51688c1c` moving away from it; the only
changes are the 14 VT-1 stores (12 `vt1/*` plus the `widen/union_*` pair, rows
byte-identical to Spark and base) and the 7 fold flips (m4–m7/m11 refuse with
Spark's CAST_INVALID_INPUT, m8/n5 store). Pins: 21 oracle cells (`p01`–`p21`,
recorded on live Spark 4.1.2 with clean 8000-range ids) plus 3 in-test pins
covering `p22`–`p29`. Perf on the DEBUG build (`maturin develop`):
mapped-plus-judged vs unmapped control 1.18s/1.10s = 1.07x, and the sibling
scan itself 0.953s/0.946s = 1.007x against the no-scan refusal; the inline-
VALUES vs temp-view planning gap (3.6s vs 1.1s) reproduces with the gate
bypassed and is pre-existing.

**Carried (R4).** VT-2 (S3): mixed-type rows inside one VALUES node refuse with
CANNOT_SAFELY_CAST where Spark raises INVALID_INLINE_TABLE; the probe record
that marked it Spark-confirmed is corrected here. `vt1/union_date_ts_into_bigint`
still refuses CANNOT_SAFELY_CAST (Spark: INCOMPATIBLE_COLUMN_TYPE); o5/o6 keep
their CANNOT_SAFELY_CAST-vs-INCOMPATIBLE_COLUMN_TYPE class gap; m1 refuses with
the analyzer's `type_coercion` text exactly as on base. VT-4 (S2, pre-existing):
`array<timestamp>` into `array<bigint>` and struct/map timestamp fields store
where Spark refuses; out of scope. Noted: n5 stores `2020-01-01 10:00:00` where
Spark stores midnight (Spark widens STRING+DATE to DATE and truncates the
time); base agrees with head, so the value gap is pre-existing and the cell
meets its outcome-only target.

## Re-verify fold 2 (2026-09-30, PR #894)

The re-verifier found no false store but 19 refused stores plus two
measurement gaps (VT2-1..VT2-5). The fold rulings replace the skip's typing
only; the refusal path is untouched.

**Fix.** `sibling_types.rs` (new) holds `SiblingJudge`, moved from
`ltz_values_store.rs`. R1: a cell and every sibling cell are typed the way
`refuse_value` types them — `literal_source_type`, then `leaf_type` for the
TIMESTAMP/NTZ shapes the probe would only confirm, then the existing probe —
so the skip and the refusal never type a cell differently (VT2-1:
function-valued STRING cells and VALUES siblings of
`current_timestamp()`/`make_timestamp()`). R2: a position no cell or
provenance entry resolves falls back to planning that arm's SELECT alone
through the session and reading the column type (stars over tables,
expressions over table columns, constant expressions in a SELECT with FROM,
temp-view siblings); the whole set operation is never planned, and a plan
failure keeps the pre-fold judgment. Arms that reference CTE names, missing
objects, or anything else the session cannot plan keep the pre-fold judgment
too. VT2-2: a plan failure that is an ambiguity (typed
`AmbiguousReference`, else the bare word in the message) passes the STRING
cell through unjudged so the analyzer raises Spark's AMBIGUOUS_REFERENCE;
non-STRING cells beside an ambiguous arm keep being judged first (residual,
same as before). R3: the skip is one per-position map built once per
statement — probes cached by probe text, table schemas by table, arm plans
by arm — and reused for every cell; a cell whose static type is absent is
only probed when the map already skips its position. `select_values_arms.rs`
carries each arm's rendered SELECT and yields position-less arms for stars
over unresolvable factors instead of dropping them.

**Proof.** Pins `q01`–`q16` (recorded on live Spark 4.1.2, 8401–8551 id
range) plus four in-test pins: the 8 VT2-1 cells, the 11 residue cells and
the VT2-2 cell. The `cast_s_ts`, `to_timestamp_tbl` and `swapped_arms` pins
read the parseable `psrc` strings (the unit `src.s` values are unparsable,
unlike the attack setup). The `make_timestamp` and `values_current_ts`
pins are covered twice — the R1 probe and the R2 arm plan each skip them
alone (M-VT1a/M-R2). The `p09` NTZ sibling resolves through the probe (the
gate sees the lowered `__repark_cast_timestamp_ntz__` call) and is rescued
by the arm plan when probes are off. Replays of `verify-tsd/`,
`verify-tsd-fold/` and `reverify-tsd/`, the R3 perf table, and the named
suites run after the commit; the round hand-back records their numbers.

**Observed, not fixed (VT2-3).** A time-bearing STRING beside a DATE
sibling stores the time where Spark stores midnight (base agrees with
head). The fold reopens the base path for one more shape
(`test_fold_date_sibling_time_string_stores_with_midnight_divergence`
pins the stored rows); the orchestrator files the widening card.

**Numbers (2026-09-30, DEBUG build).** Replay on this head:
`verify-tsd/` (2,182 cells) plus `verify-tsd-fold/` (60 cells) show zero
strict differences against the `21a35daf` recordings, zero moves away
from Spark against `51688c1c` and base, and the same fixed sets as fold
1 (12 `vt1` stores, the widen pair, 7 fold flips). Attack replay (129
cells): the 20 intended changes only — the 19 stores now store with
Spark's rows (except the VT2-3 midnight gap on the DATE-sibling shape)
and the ambiguous join now raises AMBIGUOUS_REFERENCE; zero false
stores. Perf, medians of 3, mapped shape against the unmapped control in
the same DEBUG build: u10k 69.7s/70.2s = 0.99x, deep200 2.76s/1.85s =
1.49x, probe200 3.61s/2.71s = 1.33x, probe200first 3.44s/2.81s = 1.23x,
probe400 9.79s/7.43s = 1.32x; against the recorded base-release shape
ratios the gate-attributable cost is 1.01x, 1.13x, 1.09x, 1.15x and
1.05x (was 1.04x, 1.15x, 2.83x, 1.21x, 3.48x).

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: store-ts-doors-2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause is walked against recorded Spark answers — 21 must-change refusal cells across every shape and partition form, 5 store cells, and the 94-statement Step 0 matrix re-run confining every change to the intended cells.
      artifacts: [python/repark/tests/test_store_ts_doors_2.py, python/repark/tests/store_ts_doors_2_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Direct, star, CTE (both spellings), view, both join sides, UNION, nested aliases, WHERE, ORDER BY/LIMIT, VALUES/SELECT/listed/dynamic/BY NAME partition forms, and multi-row VALUES are exercised; views, table columns, literals, CASTs, functions and ambiguous refs fall through to base behaviour.
      artifacts: [crates/repark-spark/src/void_type/select_values_arms.rs, crates/repark-spark/src/void_type/insert_source_types.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal is a planning error raised before any file is staged; the replay test reads back the target after each refusal and matches Spark's unchanged rows, including the seeded rows and the empty overwritten partitions.
      artifacts: [python/repark/tests/test_store_ts_doors_2.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state; the resolver borrows the statement AST and the helper plans the statement's own source.
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling; the gates read table metadata and plan the statement's own source.
    - id: AT-6
      status: ATTACKED
      evidence: No stored-format change; the silent epoch-seconds and NULL writes are closed and the replay test asserts refused statements leave prior rows intact.
      artifacts: [python/repark/tests/test_store_ts_doors_2.py]
    - id: AT-7
      status: ATTACKED
      evidence: The SELECT-arm resolver runs only when the INSERT body is not a VALUES list and bails before any catalog load when no arm maps a VALUES cell; the partition helper loads the target schema and plans the filled source once per partition overwrite. No new per-row work.
      artifacts: [crates/repark-spark/src/void_type/ltz_values_store.rs, crates/repark-spark/src/void_type/insert_source_types.rs]
    - id: AT-8
      status: ATTACKED
      evidence: No new crate edge or dependency; the VALUES path keeps its exact behaviour through the extracted loader; insert_overwrite.rs holds at 998 lines with no ceiling change; views, BY NAME and the DataFrame doors are untouched.
      artifacts: [crates/repark-spark/src/void_type/ltz_values_store.rs, crates/repark-spark/src/insert_overwrite.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Every new refusal carries Spark's class, condition and SQLSTATE through the facade's stamped-message parse; the replay test compares all three with the recorded oracle.
      artifacts: [python/repark/tests/test_store_ts_doors_2.py]
    - id: AT-10
      status: ATTACKED
      evidence: Each new call site has a skip mutation that reds exactly its pins while the other family stays green; the resolver has classifier tests for every shape and every fall-through.
      artifacts: [crates/repark-spark/src/void_type/select_values_arms.rs, python/repark/tests/test_store_ts_doors_2.py]
  complete: true
```
