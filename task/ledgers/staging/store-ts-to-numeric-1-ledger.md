# Unit ledger — WO STORE-TS-TO-NUMERIC-1 · TIMESTAMP, DATE, STRING and `-NULL` sources follow Spark's store assignment on every write door

**Date:** 2026-09-28 · **Branch:** `fix/store-ts-to-numeric-1` · **Base:** `adc26586` (`origin/main`)
**Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Fold 2026-09-29:** verifier VT-1..VT-5 folded by Muse Spark
(`muse-spark-1.3-contributor`) on the same branch — see "Verifier fold" below.
**Narrowing fold 2026-09-29:** re-verify RT-1..RT-3 folded by Claude Opus 5.5
(`claude-opus-5-5`) under the orchestrator's narrowing ruling — see "Narrowing fold" below.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Verifier findings VG-5 (S1) and VG-7 (S2) on #882 and release differential
PE-9: `INSERT INTO sc.ns.b VALUES (0, TIMESTAMP'1970-01-01 00:00:01')` into a BIGINT
column stored `1` (epoch seconds), DOUBLE stored `1.0` and DECIMAL(10,2) `1.00`; a STRING
into a DATE column through VALUES stored; `-NULL` into TIMESTAMP stored NULL. Spark 4.1.2
refuses all of them with `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` under the default
`spark.sql.storeAssignmentPolicy=ANSI`. LTZ-STORE-INT-1 closed numeric into TIMESTAMP on
the VALUES door; this unit closes the other direction and the doors the Step 0 matrix
found open.

**Step 0 (measured before any edit, 2026-09-28, zone UTC).** One matrix, 3,645 cells:
15 sources (TIMESTAMP, TIMESTAMP_NTZ and DATE literals, `CAST(1 AS TIMESTAMP)`, a
TIMESTAMP table column, `'1'`, `'2024-01-01'`, `'abc'`, `-NULL`, `NULL`, `1`, `1.5`,
`1.5D`, `3000000000L`, `true`) × 10 targets (TINYINT, SMALLINT, INT, BIGINT, FLOAT,
DOUBLE, DECIMAL(10,2), DATE, BOOLEAN, TIMESTAMP) × 10 doors (VALUES, OVERWRITE VALUES,
UPDATE, INSERT … SELECT, INSERT OVERWRITE … SELECT, BY NAME, MERGE UPDATE, MERGE INSERT,
`writeTo().append()`, `write.insertInto()`) with the SELECT-shaped doors bare, through a
derived table and through a temp view, plus CTAS as a control. Spark 4.1.2 + Iceberg
1.11.0 through `jvm-lock.sh`; RePark on `adc26586`. Classes on base:

| Class | Cells |
|---|---|
| RePark stores, Spark refuses (must change) | 123 |
| Both refuse, same class and first line | 84 |
| Both refuse, same class, message differs | 6 |
| Both refuse, class differs | 2,146 |
| Both store, same value | 1,230 |
| RePark refuses, Spark stores (pre-existing, not touched) | 56 |

The 123 must-change cells: VALUES (single-row `INSERT INTO`) TIMESTAMP and
`CAST(1 AS TIMESTAMP)` into every numeric target (14), STRING into numeric and BOOLEAN
(8), STRING into DATE (1), BOOLEAN into numeric (6), numeric into BOOLEAN (3); STRING
into FLOAT/DOUBLE through INSERT … SELECT and both DataFrame doors, every wrap (18);
STRING into DECIMAL through UPDATE (1); `-NULL` into DATE, BOOLEAN and TIMESTAMP on every
door and wrap (72). Spark types `-NULL` as DOUBLE (`typeof(-NULL)` is `double`); RePark
types it `void`.

**Cause.** Four gaps around one shared matrix (`ansi_store_assignable`), which already
refuses every must-change pair: (1) the VALUES gate judged only TIMESTAMP (LTZ) targets;
(2) the Spark door's `spark_float_stringify` rule rewrites the synthesized STRING →
FLOAT/DOUBLE conform cast into `__repark_parse_java_double__` (or folds an aliased
literal to a float literal) before `InsertStoreAssignment` runs, so the analyzer gate
never sees the cast; (3) `spark_update_type_name` has no DECIMAL name, so the UPDATE gate
answered "no message" for a DECIMAL target; (4) a `-NULL` column plans as Arrow `Null`,
which the matrix stores anywhere.

**Fix.** Refusals only; nothing new is admitted.
- `void_type/ltz_values_store.rs`: the VALUES gate judges numeric, DATE and BOOLEAN
  targets as well as LTZ ones, through the shared matrix; the LTZ-only numeric fallback
  stays LTZ-only.
- `repark-iceberg` `update_cast.rs`: `incompatible_store_message` is the shared message
  with DECIMAL names (`DECIMAL(10,2)`); `incompatible_update_message` keeps its answers,
  so the native door and the VOID, NTZ and nested-MERGE gates are unchanged.
- `repark-iceberg` `negated_null_store.rs`: `refuse_negated_null_writes` walks a planned
  source's column lineage (projections, aliases, derived tables, joins, VALUES, UNION,
  DataFusion views, and Spark-door temp views through a session resolver) and judges a
  `Null` column that is a sign applied to NULL as DOUBLE. Callers: both MERGE gates, the
  Spark-door UPDATE gate, and the Spark-door INSERT gate below.
- `void_type/insert_source_types.rs`: one Spark-door INSERT gate, called from
  `router/insert_positional.rs` `prepare_positional_insert` (positional append, owned
  append, OVERWRITE, VALUES) and from `execute_insert_by_name`, plans the source once and
  refuses a negated NULL into a column DOUBLE cannot store and a STRING into FLOAT/DOUBLE,
  with Spark's text.
- `view_ddl/temp_view.rs`: `ReplanningTempView` keeps its creation-time plan;
  `definition_plan` answers it to the lineage walk (registered in `extension.rs`).

**After the fix (same matrix, same Spark answers).** All 123 must-change cells refuse with
Spark's class, first line and SQLSTATE (`KD000`), and write nothing. 132 cells that both
engines already refused now also carry Spark's exact text. No cell that stores on base
changed value or started refusing; the 56 RePark-only refusals and the 6 same-class
message differences are unchanged.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | On the VALUES door, TIMESTAMP (literal or `CAST`), TIMESTAMP_NTZ and DATE into a numeric or BOOLEAN column, BOOLEAN into a numeric column and a numeric into a BOOLEAN column refuse with Spark's recorded class, SQLSTATE and message body, and write nothing; a STRING-planned cell is not judged (C-006). | `test_store_ts_to_numeric_1.py` replays the recorded `values/*` refusal cells and their read-backs; the classifier unit test names the judged targets; the Step 0 matrix re-run on the narrowed head. | PROVEN | 5 VALUES refusal cells equal `store_ts_to_numeric_1_spark_oracle.json`; all 5 miss on `adc26586`. Narrowing fold: the 3,943-statement Step 0 matrix replay refuses all 960 cells where Spark refuses a TIMESTAMP, TIMESTAMP_NTZ, DATE, `CAST(1 AS TIMESTAMP)` or TIMESTAMP-column source into TINYINT…DECIMAL/BOOLEAN, on every door, with text byte-identical to `91e29424`. |
| C-002 | `-NULL` and `-(NULL)` into DATE, BOOLEAN, TIMESTAMP and TIMESTAMP_NTZ columns refuse with Spark's text naming `"DOUBLE"` on VALUES, OVERWRITE VALUES, UPDATE, INSERT … SELECT, INSERT OVERWRITE, BY NAME, MERGE UPDATE, MERGE INSERT and both DataFrame doors, bare, through a derived table and through a temp view; they still store NULL into numeric and STRING columns. | The replay test's `-NULL` refusal cells (every door, all three wraps, NTZ on VALUES, UPDATE and MERGE INSERT) and store cells (INT, STRING, DECIMAL through a view, DOUBLE through MERGE INSERT, BIGINT through UPDATE); the lineage unit tests. | PROVEN | 17 refusal cells and 5 store cells equal the oracle; the 17 refusals miss on `adc26586`; `cargo test -p repark-iceberg --lib negated_null`: 4 passed. |
| C-003 | STRING into FLOAT or DOUBLE refuses with Spark's text on INSERT … SELECT and both DataFrame doors, bare, through a derived table and through a temp view. | The replay test's `select`, `df_append` and `df_insertinto` STRING → FLOAT/DOUBLE cells. | REJECTED (withdrawn 2026-09-29 by the narrowing ruling: RePark's STRING typing does not follow Spark's widening, re-verify RT-1..RT-6; see C-006 and R-STN-10) | 4 refusal cells equal the oracle; all 4 stored on `adc26586`. |
| C-004 | STRING into DECIMAL(10,2) refuses on the UPDATE door with Spark's text naming `"DECIMAL(10,2)"`. | The replay test's `update/str_num/dec` cell. | REJECTED (withdrawn 2026-09-29 by the narrowing ruling; STRING into DECIMAL on UPDATE stores as on base; see C-006 and R-STN-10) | Equals the oracle; stored `1.00` on `adc26586`. |
| C-005 | Every must-not-change cell still stores with Spark's value: DATE → TIMESTAMP and TIMESTAMP → DATE (VALUES, SELECT, UPDATE, MERGE, BY NAME, OVERWRITE, `insertInto`), INT → BIGINT, INT → DOUBLE, DECIMAL → DOUBLE, in-range BIGINT → INT, an explicit `CAST(TIMESTAMP AS BIGINT)`, NULL into DATE and BOOLEAN, a TIMESTAMP column through a derived table into DATE, and the CTAS controls; LTZ-STORE-INT-1's INT → TIMESTAMP refusal stays. | The replay test's 22 non-`-NULL` store cells and the INT → TIMESTAMP refusal cell; `test_ltz_store_int_1.py`; the Step 0 matrix re-run (no stored value changed). | PROVEN | Store cells equal the oracle read-backs on head and on `adc26586`; LTZ pins green. |
| C-006 | Every refusal whose judged source type is STRING returns to base behaviour on every door: VALUES into numeric, DATE and BOOLEAN columns, INSERT … SELECT, OVERWRITE, BY NAME, column list, `writeTo().append()`, `writeTo().overwrite(condition)`, `write.insertInto()` and UPDATE into DECIMAL; the re-verify RT-1..RT-3 shapes store Spark's value. | The 7,034-statement re-verify corpus and its 10 direct probes replayed on the narrowed head against the recorded base and Spark answers; one replay pin per RT family. | PROVEN | Zero statements where head refuses and base and Spark both store; zero where head stores a value different from base; head's outcome differs from base only on the two `-NULL` controls (C-002). All 46 RT cells and the 10 direct probes answer as base; 6 replay cells (SQL temp view, `append`, `insertInto`, UNION, `max`, VALUES CASE into DATE) equal Spark's read-back. |
| C-007 | A new refusal trusts RePark's planned source type only when it is known to equal Spark's: a branching cell (CASE, `coalesce`/`greatest`/`least`/`nvl`/`ifnull`/`nullif`/`if`/`nvl2`, a scalar subquery other than a one-column projection, EXISTS, IN) refuses only when every leaf has a static non-STRING type that itself refuses into the target; otherwise the store behaves as on base. | `source_leaves.rs` classifier tests; the mutation that makes the leaf check accept any source type. | PROVEN | 5 classifier tests pass; the mutation reds the RT-2 VALUES CASE-into-DATE pin and the VT-1 VALUES CASE cells (see "Narrowing fold"). |

## Mutation record (2026-09-28)

Each mutation rebuilt the native module (`make develop`) on `55ba71bd`, ran the replay test,
`test_ltz_store_int_1.py` and `test_insert_store_assign.py`, and was reverted; `git status`
was clean before the next.

| # | Mutation | Replay cells red | Other files |
|---|---|---|---|
| M1 | The VALUES gate judges LTZ targets only (`is_judged_target` returns `is_microsecond_ltz`) | 8: every C-001 VALUES refusal | `test_a_literal_values_row_refuses_like_spark` red; LTZ pins green |
| M2 | The lineage walk never reports a negated NULL (`Negative` answers `Other`) | 17: every `-NULL` refusal, all doors and wraps | LTZ pins green |
| M3 | The INSERT gate skips STRING → FLOAT/DOUBLE | 4: every C-003 cell | LTZ pins green |
| M4 | `incompatible_store_message` stops naming DECIMAL | 2: `values/cast_ts/dec`, `update/str_num/dec` | LTZ pins green |
| MALL | M1 + M2 + M3 + M4 (the new direction off) | all 30 must-change cells; every store cell stays green | `test_a_literal_values_row_refuses_like_spark` red; LTZ pins green |

**Neighbours (2026-09-28).** 40 statements drawn from the diff-probe corpus
(`ins_probe.py`, `upd_probe.py`, `ovf_probe.py`, `intdiv_probe.py`: fractional and
overflowing quotients into BIGINT/INT through INSERT … SELECT, VALUES, OVERWRITE, UPDATE,
MERGE and a CTE) answer the same on `adc26586` and on this unit; the one textual
difference is the row order of an unordered `SELECT * FROM sc.ns.c` (same multiset).

**Existing pins changed (2).** `test_insert_store_assign.py`'s VALUES-residual pin
(`VALUES (true)` into INT stored `1`) becomes `test_a_literal_values_row_refuses_like_spark`;
`tests/update_cast.rs`'s `insert_string_into_bigint_keeps_the_insert_path` (a cast-kernel
error, explicitly not the store text) becomes `insert_string_into_bigint_refuses_like_spark`.
Both old pins recorded answers Spark does not give.

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: store-ts-to-numeric-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause is walked against recorded Spark answers — 30 must-change refusal cells across every door, source class and wrap, 27 store cells and the LTZ-STORE-INT-1 refusal control, and the 3,645-cell Step 0 matrix re-run. Narrowing fold: the 7,034-statement re-verify corpus, its 10 direct probes and the 3,943-statement Step 0 matrix replayed on the narrowed head against the recorded base and Spark answers.
      artifacts: [python/repark/tests/test_store_ts_to_numeric_1.py, python/repark/tests/store_ts_to_numeric_1_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Bare, derived-table, temp-view, column-list, BY NAME, multi-row VALUES, `-(NULL)`, UNION of `-NULL` and NULL, joins in the MERGE probes, and numeric, STRING and DECIMAL targets for `-NULL` are exercised; partitioned inserts, unmapped columns and unplannable sources fall through to base behaviour.
      artifacts: [crates/repark-spark/src/void_type/insert_source_types.rs, crates/repark-iceberg/src/write/negated_null_store.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal is a planning error raised before any file is staged; the replay test reads back the target after each refusal and matches Spark's unchanged rows, including the seeded row UPDATE and MERGE UPDATE would have changed and the OVERWRITE target.
      artifacts: [python/repark/tests/test_store_ts_to_numeric_1.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state; the view resolver is a plain function stored in the session config and the cached view plan is immutable.
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling; the gates read table metadata and plan the statement's own source.
    - id: AT-6
      status: ATTACKED
      evidence: No stored-format change; the silent epoch-seconds, STRING and NULL writes are closed and the replay test asserts refused statements leave prior rows intact.
      artifacts: [python/repark/tests/test_store_ts_to_numeric_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: The INSERT gate loads the target schema and plans the source once per INSERT, and only when the table has a DATE, BOOLEAN, TIMESTAMP, BINARY or floating-point column; the VALUES gate probes non-literal cells for numeric, DATE and BOOLEAN targets. Measured cost is recorded in residue R-STN-6.
      artifacts: [crates/repark-spark/src/void_type/insert_source_types.rs, crates/repark-spark/src/void_type/ltz_values_store.rs]
    - id: AT-8
      status: ATTACKED
      evidence: No new crate edge or dependency; `incompatible_update_message` keeps its answers so the native door, VOID, NTZ and nested-MERGE gates are unchanged; the temp-view plan is exposed through a session-config resolver, not through `get_logical_plan`, so DataFusion never inlines a replanning view.
      artifacts: [crates/repark-iceberg/src/write/update_cast.rs, crates/repark-spark/src/view_ddl/temp_view.rs, crates/repark-spark/src/extension.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Every new refusal carries Spark's class, condition and SQLSTATE through the facade's stamped-message parse; the replay test compares all three with the recorded oracle.
      artifacts: [python/repark/tests/test_store_ts_to_numeric_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Each mechanism has a mutation that reds the replay test while the LTZ-STORE-INT-1 pins stay green; the lineage walk has positive and negative unit tests; the judged-target classifier has a unit test.
      artifacts: [crates/repark-iceberg/src/write/negated_null_store.rs, crates/repark-spark/src/void_type/ltz_values_store.rs, python/repark/tests/test_store_ts_to_numeric_1.py]
  complete: true
```

## Verifier fold (2026-09-29)

The scoped verifier replayed 2,941 statements plus 36 controls on `575878bc`
against Spark 4.1.2 + Iceberg 1.11.0 and the `adc26586` base: all 391 head-vs-base
outcome changes run store → refuse, MALL restores all 391 to base with zero other
diffs, and the pins go red on exactly the must-change cells under MALL. Five
findings; this fold closes VT-1, VT-2 and VT-4 and records VT-3 and VT-5.

**VT-1 (S2, closed).** 23 stores Spark allows refused on head: mixed STRING and
numeric operands in `CASE`, `nvl` and `nullif` into numeric columns. Spark widens
those branches to a numeric type; RePark plans them STRING and both new gates
judged the planned type. The fix judges the widened (Spark-effective) branch
type instead of skipping every conditional or function: skipping all of them
would also admit `CASE WHEN true THEN '1' ELSE 2 END` and `nvl('1', 2)` into
BOOLEAN, which Spark refuses and the matrix must keep refusing, along with every
other function-typed source (`abs`, `length`, `concat`, `max(s)`). Concretely,
when a gate is about to refuse a STRING-planned source, `spark_widen.rs`
collects the branch operand types — CASE/nvl/ifnull/nullif/coalesce/if/nvl2/
greatest/least, `nullif` keeping its first argument — and skips the refusal only
when the widened type stores into the target through the shared matrix (a
widened numeric into an LTZ target still refuses; a still-refusing cell keeps
naming the planned STRING). The VALUES gate reads branches from the SQL AST; the
INSERT gate walks the planned source like the negated-NULL lineage, through
derived tables, views and joins, with `__repark_float_to_string__` transparent
(it only wraps FLOAT/DOUBLE — DataFrame temp views arrive inlined carrying that
analyzer artifact on the DOUBLE operand). Operands Spark would not widen
(BOOLEAN mixed with STRING, two disagreeing sides, `concat`, aggregates,
subqueries, UNION branches of mixed type) keep today's judgment, as do all
non-STRING planned sources. Proof: the verifier's full probe replays with
exactly the 23 flipping back to store (values verified against the recorded
Spark read-backs), every other outcome and all 36 controls unchanged; the 23
shapes are pinned as store cells in `test_store_ts_to_numeric_1.py`, and forcing
the widened judgment off reds exactly those 23. Evidence:
`/tmp/oc-worker/direct/wo/verify-tsn-opus-evidence/`.

**VT-2 (S3, closed).** `writeTo().overwrite(condition)` called no gate: STRING
into DOUBLE and `-NULL` into DATE/BOOLEAN stored where Spark refuses. One
call-site addition runs the shared INSERT gate in `execute_replace_where`
(positions are table-ordered there); the verifier's writer cells now refuse
with Spark's text and write nothing, and NULL, DOUBLE and CAST sources still
store through the same door.

**VT-4 (S3, closed).** The per-cell `SELECT <cell>` probe now fires only for
values without a declared type: DATE typed strings and plain CASTs (BOOLEAN,
DATE, integers, FLOAT/DOUBLE, STRING spellings, DECIMAL with precision and
scale) read by declared type exactly as the probe resolved them. TIMESTAMP
typed strings stay probed — their DataFusion unit/zone was not pinned, and a
wrong literal type would rename pinned messages. Measured numbers are in
R-STN-6.

**Premise correction (VT-5).** The brief premise that Spark stores
`CAST(-NULL AS DATE)` is false: Spark 4.1.2 refuses it (and `try_cast(-NULL AS
DATE)` and the UPDATE form) with `DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION`
because `-NULL` is DOUBLE. RePark stores these on base and head alike; the
residue is recorded as R-STN-9, not as a regression.

## Narrowing fold (2026-09-29)

**Why.** The scoped re-verify on `91e29424` replayed 7,034 statements (4,085 stores
and specials, 2,880 `typeof`/value selects, 32 controls) plus 10 direct probes against
Spark 4.1.2 and the `adc26586` base, and found 46 new false refusals (RT-1 view and
DataFrame-writer doors, RT-2 VALUES CASE into DATE, RT-3 UNION, GROUP BY, aggregate,
window, scalar-subquery and `concat` branches), every one judging a STRING-typed source
whose RePark type does not follow Spark's widening (RT-4..RT-7 are the same lattice).
The orchestrator ruled: ship only the refusals whose source type RePark derives
reliably. A new refusal fires only where RePark's source type is known to equal
Spark's; when in doubt base behaviour wins.

**Step 0 (replayed on `91e29424` before any edit).** The corpus replay equals the
re-verifier's recorded head answers on every statement: 46 RT cells refuse where base
and Spark store, 125 statements differ from base (46 + 79 correct new refusals), and
the 10 direct probes refuse exactly the eight cells the re-verify names. The original
3,943-statement Step 0 matrix (`store-ts-to-numeric-1-evidence/probe.py`) replayed on
`91e29424` refuses all 960 VG-5 cells (TIMESTAMP, TIMESTAMP_NTZ, DATE, `CAST(1 AS
TIMESTAMP)` and TIMESTAMP-column sources into TINYINT … DECIMAL and BOOLEAN where
Spark refuses).

**Change.**
- `void_type/ltz_values_store.rs`: for numeric, DATE and BOOLEAN targets the VALUES
  gate is silent when the planned source is STRING or when
  `source_leaves::source_type_is_reliable` rejects the cell; the VT-1 widening escape
  is removed, so LTZ targets answer exactly as on base.
- `void_type/source_leaves.rs` (new): the leaf rule of C-007.
- `void_type/insert_source_types.rs`: the INSERT gate judges `-NULL` only; the STRING
  → FLOAT/DOUBLE refusal, its plan walk and the `__repark_float_to_string__`
  transparency are gone.
- `void_type/spark_widen.rs`: deleted (the lattice RT-4, RT-5 and RT-7 fault).
- `update_cast.rs`: the DECIMAL-naming message refuses only where the base message
  already refuses, or where the source is not STRING and the SET value passes the
  leaf rule.
- The `writeTo().overwrite(condition)` call site (VT-2) is kept; through the narrowed
  gate it refuses only `-NULL` cells.

**After (same corpus, same recorded base and Spark answers, debug build).**

| Class | Statements |
|---|---|
| Head refuses where base and Spark both store | **0** (was 46) |
| Head stores a value different from base | **0** |
| Head stores where base refused | 0 |
| Head outcome differs from base | 2 (the two `-NULL` controls, C-002) |
| Returned to base versus `91e29424` (refuse → store) | 123: the 46 RT cells and 77 STRING-source refusals Spark also refuses |
| Both store, same value / value differs (pre-existing, base = head) | 1,189 / 89 |
| Both refuse | 1,374 |
| Pre-existing false refusals / misses (base = head) | 695 / 770 |
| Controls: Spark store = head store / both refuse / Spark refuses, head stores | 19 / 7 / 6 |

The six controls that now store are the STRING literal controls (`'1.5'` into DOUBLE
through VALUES and SELECT, `'1.5'` into FLOAT through SELECT, `'1'` into INT,
`'2020-01-01'` into DATE, `'true'` into BOOLEAN through VALUES): STRING-source
refusals the ruling removes, now answering as base. The 10 direct probes all answer as
base (8 store Spark's value; the DataFrame-view BY NAME cell keeps base's own
pre-existing refusal). The Step 0 matrix on the narrowed head: all 960 VG-5 cells
refuse, and every non-STRING-source cell answers byte-identically to `91e29424`
(outcome, refusal text and stored value); 126 STRING-source cells changed, each now
equal to base (28 store, 98 keep refusing with base's text).

**Pins.** Deleted from the replay oracle (STRING-source refusals): `values/str_num/int`,
`values/str_num/boolean`, `values/str_date/date`, `update/str_num/dec`,
`select/str_num/double`, `select/str_num/float/derived`, `df_append/str_num/double/view`,
`df_insertinto/str_num/float`. Added, measured on Spark 4.1.2 for this fold:
`view/case_mix/double` (RT-1 SQL temp view), `df_append/case_mix/double` (RT-1
`writeTo().append()`), `df_insertinto/case_mix/double` (RT-1 `insertInto`),
`union/str_double/double` (RT-3 UNION), `aggregate/max_case/double` (RT-3 aggregate),
`values/case_str_ts/date` (RT-2 CASE into DATE). Restored to its base text:
`tests/update_cast.rs` `insert_string_into_bigint_keeps_the_insert_path`. Removed with
the lattice: the seven `spark_widen.rs` tests and the two VALUES widening tests in
`ltz_values_store.rs`. Added: five `source_leaves.rs` classifier tests. The oracle is
79 cells, one per line (136 lines).

**Mutation record (narrowing fold).** Each mutation rebuilt the native module on
`98261b12`, replayed every oracle cell, ran `test_ltz_store_int_1.py` and
`test_insert_store_assign.py`, and was reverted with `git checkout`; `git status` was
clean after each.

| # | Mutation | Replay cells red | Other files |
|---|---|---|---|
| MN-A | The leaf check accepts any source type (`is_string_type` answers false, `source_type_is_reliable` answers true) | 4 of 79: `values/case_str_ts/date` (RT-2) and the three VT-1 VALUES CASE cells | 27 passed |
| MN-B | The INSERT gate refuses STRING into FLOAT/DOUBLE again (the `55ba71bd` rule) | 26 of 79: the five SELECT-door RT pins (`view/…`, `df_append/case_mix`, `df_insertinto/case_mix`, `union/…`, `aggregate/…`) and 21 VT-1 cells | 27 passed |

Every RT pin goes red under one of the two; the SELECT-door RT pins guard a rule that
no longer exists in the tree, so MN-B restores it to prove them.

## Residues

| # | Residue |
|---|---|
| R-STN-1 | Dated 2026-09-28 (pre-existing, out of scope): 2,014 matrix cells refuse on both engines with a different class — the SELECT-shaped doors (INSERT … SELECT, OVERWRITE, BY NAME, MERGE, DataFrame) still speak the WI-1 text `… cannot store-assign column …` for pairs their existing gates already refused. Moving those gates to Spark's text changes many existing pins and is its own unit. |
| R-STN-2 | Dated 2026-09-28 (pre-existing): 56 cells where RePark refuses and Spark stores — TIMESTAMP_NTZ literals into DATE and TIMESTAMP through the SELECT-shaped doors (`'__repark_timestamp_ntz__' expects an Int64 wall`), and CTAS of `-NULL` (`Invalid schema for v2`). |
| R-STN-3 | Dated 2026-09-28: `- -NULL` still stores into DATE through the SQL doors. The INSERT gate plans the rendered source, and `- -NULL` renders as `--NULL`, a comment (the VG-1/VG-3 stacked-sign rendering, owned by LTZ-STACKED-SIGN-1 #882). |
| R-STN-4 | Dated 2026-09-28: `+NULL` fails in RePark's planner (`Unary operator '+' only supports numeric…`) where Spark types it DOUBLE and refuses `CANNOT_SAFELY_CAST`; `abs(NULL)` names `"INT"` where Spark names `"DOUBLE"`. Both engines refuse; the NULL typing of functions is not store assignment. |
| R-STN-5 | Dated 2026-09-28 (same class as R-LTZ-2): a multi-row VALUES whose rows have incompatible types (`(15, '1'), (16, 2)` into INT) refuses `CANNOT_SAFELY_CAST`/KD000 per row where Spark answers `INVALID_INLINE_TABLE.INCOMPATIBLE_TYPES_IN_INLINE_TABLE`/42000. On base these rows stored. A scalar subquery in VALUES now refuses `CANNOT_SAFELY_CAST` where Spark answers `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.SCALAR_SUBQUERY_IN_VALUES`; base refused with a physical-plan error. |
| R-STN-6 | Dated 2026-09-28 (extends R-LTZ-4): every Iceberg INSERT into a table with a DATE, BOOLEAN, TIMESTAMP, BINARY or floating-point column plans its source once more, and every VALUES insert into a table with a numeric, DATE or BOOLEAN column loads the table schema and probes each non-literal cell. Measured on a debug build, median of 5: 2,000 `DATE'…'` rows into DATE 3.78 s → 4.66 s (+23%); 2,000 integer rows into BIGINT 1.12 s → 1.13 s; 2,000 strings into STRING 2.33 s → 2.32 s; `INSERT … SELECT` of 100,000 rows into DATE 0.116 s → 0.103 s; one DATE row 0.061 s both. Fold 2026-09-29 (VT-4): DATE typed strings and plain CASTs read by declared type, so those rows probe zero cells (unit-pinned); TIMESTAMP typed strings still probe. Debug medians of 3 post-fix, same box: 2,000 DATE rows 3.97 s (pre-fix 4.66 s cross-day, base 3.78 s); 10,000 `CAST(i AS DOUBLE)` rows 12.25 s; 2,000-row 7-column DATE/BOOLEAN/DOUBLE/DECIMAL/BIGINT/TIMESTAMP table 36.97 s; `INSERT … SELECT` 100k rows into DATE 0.10 s; one DATE row 0.066 s. Verifier release pre-fix (median of 5, measured twice): 2,000 DATE rows 0.293 s → 0.385 s (+32%); 10,000 DATE rows 2.67 s → 3.15 s (+18%); 10,000 `CAST(i AS DOUBLE)` rows 1.47 s → 1.98 s (+34%); 7-column table 2.00 s → 2.33 s (+16%). Release post-fix not re-measured in this fold. Narrowing fold 2026-09-29, release wheels (`CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16`) of `adc26586` and `98261b12` built on the same box, the verifier's `perf.py`, median of 5, measured twice, base → head: 2,000 DATE rows 0.292 s → 0.328 s (+12%); 10,000 DATE rows 2.68 s → 3.43 s (+28%); 10,000 `CAST(i AS DOUBLE)` rows 1.48 s → 1.50 s (+1%); 7-column table 2.01 s → 2.45 s (+22%); 10,000 integer and double-literal rows, `INSERT … SELECT` of 100k rows and one DATE row unchanged. 10,000-row STRING tables measured 1.83 s → 2.27 s (+24%) although neither gate reaches a STRING-only table; the debug builds of the same two commits answer 78.8 s and 79.3 s for that statement, so the release figure is build variance, not the gates. |
| R-STN-7 | Dated 2026-09-28: the three UPDATE cells `1` into DATE, BOOLEAN and TIMESTAMP refuse naming `"BIGINT"` where Spark names `"INT"` (the UPDATE probe types an integer literal BIGINT). Unchanged from base. |
| R-STN-8 | Dated 2026-09-29 (VT-3, pre-existing misses, out of scope): `-NULL` through `coalesce`, `nvl`, `if` or CASE, through GROUP BY, through a UNION or CASE with a typed DATE branch, and `-NULL`, `abs(NULL)`, `NULL + NULL` or `CAST(-NULL AS …)` into ARRAY, STRUCT or BINARY targets still store where Spark refuses. All missed on base too; R-STN-3 and R-STN-4 named only `- -NULL`, `+NULL` and `abs`. |
| R-STN-9 | Dated 2026-09-29 (VT-5, pre-existing, out of scope): Spark refuses `CAST(-NULL AS DATE)`, `try_cast(-NULL AS DATE)` and `UPDATE … SET dt = CAST(-NULL AS DATE)` with `DATATYPE_MISMATCH` (`-NULL` is DOUBLE); RePark stores them on base and head. The brief premise that Spark stores these is corrected in "Verifier fold" above. |
| R-STN-10 | Dated 2026-09-29 (narrowing ruling, owned by v1.5.2 card STORE-STRING-ASSIGN-1): STRING sources into numeric, DATE and BOOLEAN columns store as on base where Spark refuses — PE-9's STRING into DATE through VALUES, VG-7's STRING part (STRING into numeric and BOOLEAN through VALUES, into FLOAT/DOUBLE through INSERT … SELECT and the DataFrame writers, into DECIMAL through UPDATE), and the re-verify class RT-1..RT-7 (Spark's `findWiderTypeForString` widening STRING with an integral side to BIGINT and with a fractional side to DOUBLE, typed at the Spark door). 77 corpus statements and 28 Step 0 matrix cells are Spark refusals that now store as on base. |
