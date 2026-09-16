# Charter ledger — SPARK-SQL-GRAMMAR-1 · Spark operators, keywords and type names on the SQL door

**Date:** 2026-09-16 · **Branch:** `feat/spark-sql-grammar-1` · **Base:** `origin/main`
`0ef060af` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Hand-off note to run 17a (read first):** `python/repark/tests/test_fnp11a_temporal.py`
is 17a's file. This unit's only sanctioned edit there is removing the three skip
sets it closes: `BARE_UNIT_NAMES` handling (line 271), `D4_BLOCKED_SQL` (line 93),
`BARE_NULLARY_SQL` (line 97). The removal lands in this unit's C-008 / C-005 /
C-010 commits with the flipped cells as live pins. If a set stays skipped, this
unit did not close it and the file is otherwise untouched.

**Why now.** The SQL door parses Spark SQL with the Databricks dialect (FNP-4B
#611) but Spark-only operators (`>>>`, `div`), keywords (`RLIKE`, bare unit
names, bare nullary names), type names (`TIMESTAMP_LTZ`, `TIMESTAMP_NTZ`) and
planner seams (lateral alias, struct-dot) still refuse or mis-answer against
PySpark 4.1.2. This unit lowers each onto the registered kernels or refuses loud
with Spark's class, one pin per construct.

**Not in this unit:** the generator kernels (16a's FNP-GEN-1, C-009's
prerequisite); `functions*.py`, `dataframe/**`, `column.py`, `catalog.py`,
`session/**` (runs 16a/16b); any new kernel in `crates/repark-functions` except
the missing shift kernel the card names.

## Rulings recorded at open

- Q-15c-6 (owner): a narrow fix and a pin per construct. No registry-wide
  wrapper that re-binds every UDF's return field.
- Q-15c-4 (owner): size baselines move only downward. No one-time increase is
  taken in this round; a file that would grow past its ceiling is split instead.
- Batch-14 correction to C-010: bare `localtimestamp` refuses on Spark
  (`UNRESOLVED_COLUMN.WITH_SUGGESTION`, Q14-0), so registry EX-FN-25 (RePark
  resolving bare `localtimestamp` as a call) is RePark over-accepting. The fix
  is the refusal, and #606's `BARE_NULLARY_SQL` skip flips to an error pin.
- R-17c-3 (orchestrator): the docstring-presence gate is Python-only, so no Rust
  `///` is ever gate-required. The comment ban holds for Rust doc comments
  without exception. Their content lives in `map.md`.
- R-17c-6 (orchestrator): Spark's acceptance set is the specification in BOTH
  directions. RePark may not refuse a spelling Spark accepts, nor accept one
  Spark refuses. Bare nullary names that Spark refuses must refuse here; the
  ones it resolves must resolve. Cells outside every fixture are UNMEASURED,
  never guessed.
- R-17c-7 (orchestrator, G-2, 2026-09-16): C-009 is OPEN for the run because
  FNP-GEN-1 is not on main.
- Skill `audit-repark-parity` v1.0 was read before measuring. Step-1 deviation,
  recorded as a stop condition: no JVM may start on this lane, so the live
  PySpark oracle is NOT re-run. The recorded fixtures named in the step are the
  spec. Every RePark-side value below was observed live on a release native
  built from this branch's base (`maturin develop --release`, repark-1.4.2).

## PROPOSITION LEDGER — SPARK-SQL-GRAMMAR-1 — 2026-09-16

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `-8 >>> 1` answers `2147483644` int non-null on the SQL door. | `test_spark_sql_grammar_1.py` PG-ushift cell (value, Arrow type, nullability). | OPEN | RED on base: `ParserError("No infix parser for token ShiftRight")`. §1. |
| C-002 | `7 div 2` answers `3` bigint nullable on the SQL door. | PG-div cell in the same pin file. | OPEN | RED on base: `ParserError` on the `div` keyword. §1. |
| C-003 | `'abc' RLIKE '^a'` and `NOT RLIKE` lower onto `regexp_like`. | PG-rlike cells plus the negated form in the same pin file. | PROVEN | `keyword_lower.rs` lowers both signs onto the registered kernel; door pins + the retired 17a refusal pin (now an answer pin) green. Registry FN-RLIKE-KEYWORD-1 → FIXED. pins: spark-sql-grammar-1/C-003. §4. |
| C-004 | `CAST(x AS TIMESTAMP_LTZ)` answers `TIMESTAMP`. | PG-ltz-cast cell in the same pin file. | PROVEN | `keyword_lower.rs` rewrites the target to bare `TIMESTAMP` (TRY_CAST included); value+type exact, literal folds non-null like plain `CAST`, nullable frame stays nullable. pins: spark-sql-grammar-1/C-004. §4. |
| C-005 | `TIMESTAMP_NTZ '…'` / `CAST(x AS TIMESTAMP_NTZ)` parse onto the tz-naive Arrow path with Spark's value, or refuse loud naming TZ-6 with the TZ-6 row extended. `timestampdiff(DAY, ntz, TIMESTAMP_NTZ'…')` leaves `D4_BLOCKED_SQL`. | PG-ntz-lit / PG-ntz-cast cells, or the dated refusal plus the extended row. | PROVEN | No tz-naive CAST path exists (rewriting onto `TIMESTAMP WITHOUT TIME ZONE` plans tz-aware, measured), so the refusal stands, now as `[UNSUPPORTED_TIMESTAMP_NTZ]` naming TZ-6, with the TZ-6 dated extension; both PG cells pin the refusal. No contradiction with TZ-6's FIXED ruling (it covers the type level, this the SQL-door type name). `D4_BLOCKED_SQL` untouched. pins: spark-sql-grammar-1/C-005. §4. |
| C-006 | `named_struct('a', 1).a` answers `1` int non-null. | PG-struct-dot cell in the same pin file. | PROVEN | Was ALREADY-GREEN; pin filed unchanged. pins: spark-sql-grammar-1/C-006. §4. |
| C-007 | `SELECT 1 AS a, a + 1 AS b` answers `(1, 2)` both int non-null, or DECLARES with a registry row naming the planner seam. | PG-lateral-alias cell, or the dated refusal plus the new row. | OPEN | RED on base: `Schema error: No field named a`. Seam measured in step 2. §1. |
| C-008 | Bare unit spellings `timestampadd(DAY, 1, ts)`, `timestampdiff(HOUR, a, b)`, `dateadd(DAY, …)`, `datediff(HOUR, a, b)` rewrite onto the #606 kernels; quoted units refuse `INVALID_PARAMETER_VALUE.DATETIME_UNIT`; unknown units refuse `UNRESOLVED_ROUTINE`; 2-arg `datediff` stays `int`. Closes EX-FN-27. | Q14-22…40 cells plus PG-tsadd/tsdiff/dateadd/datediff-unit in the same pin file; `BARE_UNIT_NAMES` skip removed. | PROVEN | `crates/repark-spark/src/bare_unit.rs` rewrite + 7 in-module tests; `test_spark_sql_grammar_1.py` 50 passed (both ANSI); `test_fnp11a_temporal.py` 316 passed with the workaround retired. pins: spark-sql-grammar-1/C-008. §2. |
| C-009 | `LATERAL VIEW [OUTER] <generator>(…) <alias> AS <cols>` answers the run-15a oracle cells. | The lateral-15a cells in the same pin file. | OPEN | R-17c-7: FNP-GEN-1 is not on main (`git log origin/main --oneline | grep -i fnp-gen` empty, 2026-09-16). Generators are 16a's; this lane never implements them. |
| C-010 | Bare nullary keywords follow Spark both directions: `current_date`, `current_timestamp`, `current_user`, `user`, `session_user` resolve; `localtimestamp`, `current_catalog`, `current_database`, `current_schema`, `current_timezone`, `now` refuse `UNRESOLVED_COLUMN.WITH_SUGGESTION`; parenthesised forms resolve with Spark's type and nullability. | Q14-0…21 cells in the same pin file; `BARE_NULLARY_SQL` skip flipped to the error pin. | OPEN (refusal half + greens proven; session-names paren forms handed to 17a) | Refusal half PROVEN: demote + error map, Q14-0/8/10/12/18/20 framed+frameless pins, column-wins discriminator, `BARE_NULLARY_SQL` retired, EX-FN-25 FIXED. Greens pinned: current_date (nullable divergence recorded), current_timestamp, localtimestamp(), now(), current_timezone(). Paren `current_user`/`user`/`session_user`/`current_catalog`/`current_database`/`current_schema` still refuse on the SQL door (Python door answers; needs 17a's session plumbing) — divergence pins hold them loud. pins: spark-sql-grammar-1/C-010. §3. |

VERDICT: 10 clauses, 5 PROVEN, 5 OPEN, 0 REJECTED. No COVERAGE_ATTESTATION:
five clauses are still open, so attesting would be false.

## 1. Red-first record (base `0ef060af`, release native rebuilt 2026-09-16)

Probe `/tmp/remeasure_grammar1.py` (scratch, outside the repo) over `spark.sql`,
session zone UTC, ANSI off.
`PG-suffix` cells need FNP-4B literal suffixes and are not this card's clauses;
`PG-qualify`, `PG-nested-bt`, `PG-is-distinct`, `PG-values-alias`,
`PG-map-access` are likewise out of scope and were not run.

| Cell | Oracle (Spark 4.1.2) | RePark on base | State |
|---|---|---|---|
| PG-ushift | `a=2147483644 b=-4 c=8`, int non-null | `ParseException: No infix parser for token ShiftRight` | RED (C-001) |
| PG-div | `a=3` bigint nullable, `b=1` int | `ParseException` on `div` | RED (C-002) |
| PG-rlike | `True` boolean | `Unsupported ast node in sqltorel: RLike` (both signs) | RED (C-003) |
| PG-ltz-cast | `timestamp` nullable | `Unsupported SQL type TIMESTAMP_LTZ` | RED (C-004) |
| PG-ntz-lit / PG-ntz-cast | `timestamp_ntz` | `Unsupported SQL type TIMESTAMP_NTZ` | RED (C-005) |
| PG-struct-dot | `1` int non-null | `v:int32:nullable=False rows=[{'v': 1}]` | ALREADY-GREEN (C-006) |
| PG-lateral-alias | `(1, 2)` int non-null | `Schema error: No field named a` | RED (C-007) |
| PG-tsadd/tsdiff/dateadd/datediff-unit | timestamp / bigint answers | `No field named day/hour` | RED (C-008) |
| Q14-0 bare `localtimestamp` | refuses `UNRESOLVED_COLUMN.WITH_SUGGESTION` | answers `timestamp[us]` non-null | RED, over-accept (C-010, EX-FN-25) |
| Q14-2/3 `current_date` bare/paren | `date` non-null | `date32` nullable=True | RED on nullability (C-010) |
| Q14-4/5 `current_timestamp` bare/paren | `timestamp` non-null | `timestamp[us,tz=UTC]` non-null | GREEN value+type (C-010, pin to file) |
| Q14-6/14/16 bare `current_user`/`user`/`session_user` | `string` non-null | `No field named` | RED (C-010, needs kernels) |
| Q14-7/15/17 paren forms | `string` non-null | `Invalid function` | RED (C-010, needs kernels) |
| Q14-8/10/12 bare catalog/database/schema | refuse `UNRESOLVED_COLUMN` | `No field named` (wrong shape) | RED shape (C-010) |
| Q14-9/11/13 paren forms | `spark_catalog`/`default`/`default` | `Invalid function` | RED (C-010, needs kernels) |
| Q14-18 bare `current_timezone` | refuses `UNRESOLVED_COLUMN` | `No field named` (wrong shape) | RED shape (C-010) |
| Q14-19 `current_timezone()` | `string` non-null `'UTC'` | `string` non-null `'UTC'` | GREEN (C-010, pin to file) |
| Q14-20 bare `now` | refuses `UNRESOLVED_COLUMN` | `No field named` (wrong shape) | RED shape (C-010) |
| Q14-1 `localtimestamp()` / Q14-21 `now()` | `timestamp_ntz` / `timestamp` non-null | `timestamp[us]` non-null / `timestamp[us,tz=UTC]` non-null | Q14-21 GREEN; Q14-1 type question: RePark `localtimestamp()` is tz-naive `timestamp[us]`, Spark calls it `timestamp_ntz` (C-010, measure in step 2) |
| Q14-22…25/27…32/36…40 bare units | timestamp/bigint answers | `No field named <unit>` | RED (C-008) |
| Q14-26 quoted `'DAY'` | refuses `INVALID_PARAMETER_VALUE.DATETIME_UNIT` | answers `timestamp` | RED, over-accept (C-008) |
| Q14-33 `date_add(a, 1)` | `date` | not run cleanly (probe frame typed `ta` as timestamp; re-pin with a date frame in step 2) | UNMEASURED, re-pin |
| Q14-34 `datediff(b, a)` 2-arg | `int` | not run cleanly (same probe mistype; re-pin) | UNMEASURED, re-pin |
| Q14-35 `FORTNIGHT` | refuses `UNRESOLVED_ROUTINE` | `No field named fortnight` (wrong shape) | RED shape (C-008) |
| D4 `timestampdiff(DAY, ntz, TIMESTAMP_NTZ'…')` | blocked on the literal | `Unsupported SQL type TIMESTAMP_NTZ` | RED (C-005) |
| `SELECT nosuchfn(1)` | Spark `UNRESOLVED_ROUTINE` | `Invalid function 'nosuchfn'. Did you mean 'to_char'?` | Finding F-001: blanket unknown-function shape differs; existing pins assert `Invalid function` (`test_functions_gt2.py:318`, `test_column_parity_1.py:821`), so no blanket rewrite on this lane — Q14-35 is closed per-construct instead. |
| `typeof(1 + CAST(1 AS TINYINT))` | `int` | `bigint` | Finding F-002, filed, not fixed here: upstream literal typing owns it. No grammar pin encodes `bigint` as correct. |

Full probe output is pasted in the step-1 commit message body for the record.

## 2. C-008 implementation record (2026-09-16)

New module `crates/repark-spark/src/bare_unit.rs` (no new kernel: #606 already
routes 3-argument `dateadd` / `datediff` onto `timestampadd` / `timestampdiff`,
so the four names keep their spelling and only the unit argument is rewritten).
Runs pre-plan in `spark_ast::execute_passthrough` and, in lockstep, in the
range-frame restatement. Seven in-module tests.

Two measured consequences, both announced to run 17a here: the quoted-unit
refusal is Spark-correct (Q14-26) but retired five of 17a's pins that asserted
the old over-accepting answers (`timestampadd('DAY', …)` and siblings on the SQL
door). Those five now run the bare spelling with identical values
(`test_timestampadd_bare_unit_sql_matches_oracle`,
`test_timestampdiff_bare_unit_sql_matches_oracle`,
`test_bare_units_match_in_any_case`, `test_timestampdiff_ntz_pair_answers_days`,
`test_ntz_pair_ignores_session_zone`); the quoted SQL-door spelling has no
legitimate pin left. The `BARE_UNIT_NAMES` / `BARE_UNIT_CALL` workaround and the
`test_bare_timestampadd_unit_refuses` pin retired with it. `D4_BLOCKED_SQL` and
`BARE_NULLARY_SQL` are untouched.

Nullability notes: literal-only PG cells stay nullable on RePark where Spark
folds to non-null (the `test_folded_literals_stay_nullable` precedent, named in
the pin module docstring). Q14-34 needed a nullable frame leg to show Spark's
`int` nullable; nullability follows the inputs.

Gates for this slice: `cargo test -p repark-spark --lib bare_unit` 7 passed;
`test_spark_sql_grammar_1.py` 50 passed; `test_fnp11a_temporal.py` 316 passed
(366 jointly).

## 3. C-010 implementation record (2026-09-16)

New module `crates/repark-spark/src/bare_nullary.rs`, two halves. The parse
shapes (measured with a throwaway probe test, since removed): the Databricks
dialect parses bare `localtimestamp` as a no-paren `Function` call, while `now`,
`current_catalog`, `current_database`, `current_schema`, `current_timezone`
(and the resolving `current_user` / `user` / `session_user`) parse as plain
identifiers. `current_date`, `current_timestamp`, `current_time` also parse as
no-paren calls and already answer, so the demotion touches only the six refusing
names: a no-paren call becomes a column reference (a real column still wins —
Spark resolves bare names against columns first), and the missing-field error
maps to `[UNRESOLVED_COLUMN.WITH_SUGGESTION]` with the planner's candidates, or
`[WITHOUT_SUGGESTION]` frameless. Both shapes match the recorded oracle (FNP-11A
batch frameless, batch-14 framed). Eight in-module tests.

Two findings from the same measurement. First, the refusal map must sit around
the whole passthrough, not on `statement_to_plan`: the missing-field error for
`now` arrives wrapped (`Error during planning:` context), so a variant match
misses it and a text match catches it. Second, one silent build death: a
backgrounded `maturin develop` exited 0 with no output and no new artifact; the
foreground `touch` + rebuild compiled `repark-spark` (4m06s) and the behavior
appeared. Rebuilds on this lane run in the foreground with output checked.

Residual (P2 hand-off to run 17a, recorded here, not implemented): the SQL door
still refuses `current_user()` / `user()` / `session_user()` /
`current_catalog()` / `current_database()` / `current_schema()` with `Invalid
function`, where Spark answers `string` and the Python door answers its
foldable session strings (`functions_session.py`: identity `repark`, live
catalog/database names — another lane's server-prep design, ADR-0004). Wiring
the SQL door to the same session plumbing needs 16a's function/facade half and
a ruling on the catalog-name values (the card already flags the catalog name as
a registry question). The divergence pins hold the refusal loud meanwhile. The
`F.expr` / `filter`-string legs of both C-008 and C-010 need a binding call-site
for the repark-spark rewrites (`parse_sql_expr` bypasses the statement router);
same owner.

## 4. C-003 / C-004 / C-005 / C-006 record (2026-09-16)

New module `crates/repark-spark/src/keyword_lower.rs` (no new kernel: both
`regexp_like` and bare-`TIMESTAMP` CAST already answer on the door). `RLIKE`
lowers both signs; `TIMESTAMP_LTZ` CAST targets (TRY_CAST included) become bare
`TIMESTAMP`, inheriting the door's existing CAST folding exactly (measured:
literal folds non-null like plain `CAST`, nullable frame stays nullable with
`None` rows). `TIMESTAMP_NTZ` keeps refusing — now `[UNSUPPORTED_TIMESTAMP_NTZ]`
naming TZ-6 with the dated row extension — and `D4_BLOCKED_SQL` is untouched.
Six in-module tests.

17a's `test_sql_rlike_keyword_refuses` retired into `test_sql_rlike_keyword_answers`
(the `[[:alpha:]x]` over-accept question does not arise: the kernel is Java-
semantics via the shared translator, pinned by FN-REGEX-POSIX-1); the now-unused
`UnsupportedOperationException` import left with it.

Gates for this slice: `cargo test -p repark-spark --lib keyword_lower` 6 passed;
`test_spark_sql_grammar_1.py` + `test_fn_regex_posix_class.py` +
`test_fnp11a_temporal.py` 437 passed jointly.
