# Unit ledger — CAST-TS-STRING-1 · `CAST(<string> AS TIMESTAMP)` follows Spark 4.1.2's `stringToTimestamp`

**Date:** 2026-09-19 · **Branch:** `fix/cast-ts-string-1` · **Base:** `e36db95e` (`main`)
**Model:** claude-opus-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Owner direction 2026-09-18: 1:1 parity with Spark. The string → `TIMESTAMP` cast
was a silent wrong result on main: Arrow's `Timestamp(ns)` parser (strict grammar, 1677–2262)
stood in for Spark's `SparkDateTimeUtils.stringToTimestamp` (microseconds, its own grammar). 78
of 184 measured cells differed. ICE-TT-RESOLVE-1 pinned the time-travel consequences as strict
xfails citing this cast. Precedent: CAST-MAP-SPELL-1 put Spark's leaf rules for map casts in
`cast_map/leaf.rs`; scalar casts had none (Q-23b-F).

**Grammar source.** Spark 4.1 `sql/api/.../SparkDateTimeUtils.scala` (`parseTimestampString`,
`stringToTimestamp`, `getZoneId`) and `DataTypeErrorsBase.toSQLValue`, read at tag `v4.1.0`, then
confirmed cell by cell against the 604 recorded 4.1.2 answers.

**Not in this unit:** `STATUS.md`, `Cargo.toml`, `Cargo.lock`, `pyproject.toml`, `uv.lock`,
`.github/`, `TIMESTAMP_NTZ` (no measured cell), map-leaf timestamp casts, instant → string
rendering.

## PROPOSITION LEDGER — CAST-TS-STRING-1 — 2026-09-19

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A string `CAST(… AS TIMESTAMP)` on the facade `spark.sql` door (literal and temp-view column) answers the `unix_micros` Spark 4.1.2 answers for every recorded string, zone and ANSI mode, NULL exactly where Spark gives NULL with ANSI off. | `test_cast_ts_string_1.py` literal and view-column `CAST` pins, all 4 zone x ANSI groups. | PROVEN | Red on main (the 30 facade pins failed); green on `874883be`: 151 strings x 4 groups on each leg. |
| C-002 | With ANSI on, the cast raises `[CAST_INVALID_INPUT]` on exactly the strings Spark refuses, with Spark's message (`'…'` quoting with `\\` and `\'` escapes). | The ANSI-on groups compare the recorded message; Rust `the_raise_failure_mode_names_the_first_bad_value_in_spark_quoting`. | PROVEN | 82 recorded refusals per `CAST` and `to_timestamp` leg match; the escape rule is Spark's `toSQLValue`. |
| C-003 | Zone rules follow Spark: the session zone for a zone-less string; a zone in the string wins (offsets, `UTC` / `GMT` / `UT` prefixes, short ids, case-sensitive regions); a DST gap shifts forward, an overlap takes the earlier offset; far-future walls follow the final DST rule; local mean time before standard time; i64 overflow fails. | The New York groups; Rust zone, DST, proxy, LMT and range-edge tests. | PROVEN | `2026-03-08 02:30:00` NY → `1772955000000000`, `2999-07-01 12:00:00` NY → EDT, `-290308-12-21 19:59:05.224192` UTC → `i64::MIN`; chrono-tz's table end (2099) is pinned by `chrono_tz_tables_stop_after_the_last_tabulated_year`. |
| C-004 | A string `TRY_CAST(… AS TIMESTAMP)` and `Column.try_cast("timestamp")` answer `timestamp[us, tz=UTC]` with NULL wherever Spark's cast fails, under both ANSI modes; one-argument `try_to_timestamp` does the same. | Literal, view-column and `Column.try_cast` `TRY_CAST` pins against the recorded `try_cast` answers; Rust SQL tests. | PROVEN | Before: `timestamp[ns]` and Arrow's grammar. After: every recorded `try_cast` answer on every leg. |
| C-005 | One-argument `to_timestamp(<string>)` is Spark's cast (Spark plans it as `Cast`); `to_timestamp_ltz` follows. | Literal `to_timestamp` pins against the recorded `to_timestamp` answers; Rust `one_argument_to_timestamp_and_try_to_timestamp_follow_the_cast`. | PROVEN | 604 recorded answers match; the CAST rewrite targets this kernel. |
| C-006 | `Column.cast("timestamp")` and the SQL column path run the same kernel as a literal and produce `timestamp[us, tz=UTC]`. | `test_column_cast_matches_spark`, `test_sql_view_column_*`, `test_sql_literal_cast_is_microsecond_utc`. | PROVEN | Arrow type asserted on every column leg; values equal the literal leg. |
| C-007 | The oracle is recorded once from live PySpark 4.1.2 through the committed recorder, extends the 46 measured strings with Spark's grammar edges, and the live leg re-derives it. | `cast_ts_string_1_spark_oracle.json`, `_record_cast_ts_string_1.py`, `test_live_oracle_matches_committed_fixture` under `REPARK_PARITY_LIVE=1`. | PROVEN | 604 cells; the 184 cells measured first agree with the earlier measurement (the old "year N is out of range" cells were PySpark's conversion of the `t` column, so only `unix_micros` is recorded). |
| C-008 | The Rust kernel ports `parseTimestampString`, `getZoneId` and `stringToTimestamp` rule by rule, each rule pinned by a unit test. | `crates/repark-functions/src/tests/spark_string_timestamp.rs` (21 tests). | PROVEN | `cargo test -p repark-functions --lib tests::spark_string_timestamp` 27 passed (21 kernel + 6 SQL). |
| C-009 | ICE-TT-RESOLVE-1's cast-gap strict xfails become plain pins, and the alias-join cell uses the oracle's `2999-01-01`. | The flipped tests green in step 3. | PROVEN | `test_reader_tas_date_past_2262` (4), `test_facade_sql_tt2_short_and_year_only_casts` (2), `test_reader_tt2_timestamp_as_of_nosec` (2), alias join (2): `test_ice_tt_resolve_1*.py` 189 passed. |
| C-010 | `TIMESTAMP_NTZ` is untouched: `to_timestamp_ntz` keeps DataFusion's parse through `arrow_grammar_to_timestamp_udf`, and the `spark.sql.timestampType=TIMESTAMP_NTZ` rewrite is unchanged. | The existing NTZ tests stay green. | PROVEN | `timestamp_ltz_ntz::tests` and `instant_ts::tests::ntz_*` in the 820-test crate run; `to_timestamp_ntz('2020')` still refuses (a residue, see the registry row). |
| C-011 | `CAST-TS-STRING-1` reads FIXED 2026-09-19 in the registry's type-and-value section with before / after and the residues; ICE-TT-RESOLVE-1's residue note reads closed. | Registry diff; `check_docs_links.py`. | PROVEN | `docs/spark-sql-iceberg-parity.md` §4 row and the §2.1 closing note. |
| C-012 | The targeted gates are green on the release native built from this branch. | Summary lines in §Gates. | PROVEN | See §Gates. |
| C-013 | (verification critic, 2026-09-19) A dictionary-encoded string column runs the kernel on `CAST`, `TRY_CAST` and `try_to_timestamp`; the doubled-blank refusal is pinned in the kernel; the extractors' Arrow cast is a registered residue. | `is_string_type` unwraps `Dictionary(_, Utf8/LargeUtf8/Utf8View)` and serves `instant_ts::is_wall_clock_cast_source` and `try_to_timestamp`; `dictionary_encoded_string_columns_run_the_kernel`, `a_doubled_blank_between_date_and_time_refuses`. | PROVEN | Critic P1-DICT-BYPASS: dictionary columns took Arrow's parse (silent wrong instant). Reverting the dictionary arm turns the new pin red (orchestrator mutation). P2-MUTATION-D-DOUBLE-BLANK fixed; P2-EXTRACTOR-ARROW registered; P3-ZONE-PAD-ALLOC accepted (per-row allocation on zoned strings only). |

## Gates

Measured on the release native built from `45204211` (the step-4 tree):

- `test_cast_ts_string_1.py` offline `-n 4`: 30 passed, 1 skipped (the live leg).
- Live (`REPARK_PARITY_LIVE=1`, PySpark 4.1.2 with the shared oracle's Iceberg extensions):
  31 passed. The live leg re-derives the fixture through the recorder. Named parameters do not
  bind under the Iceberg SQL extension parser (`UNBOUND_SQL_PARAMETER`), so the recorder spells
  each string as a SQL literal. The re-derived cells equal the parameter-recorded fixture.
- Every file `rg -l "AS TIMESTAMP|cast\(.timestamp" python/repark/tests` finds (21 files)
  offline `-n 4`: 1007 passed, 16 skipped, 3 xfailed. The `to_timestamp` / `try_to_timestamp`
  / `to_timestamp_ltz` files (16): 800 passed, 12 skipped, 19 xfailed.
- `cargo test -p repark-functions --lib --tests`: 820 passed, 1 ignored;
  `… --lib tests::spark_string_timestamp`: 27 passed.
- `cargo clippy --locked -p repark-functions --all-targets -- -D warnings -A
  clippy::disallowed_methods` clean; the panic-ban flags (`--lib --bins -D
  clippy::disallowed_methods -D clippy::unwrap_used -D clippy::expect_used -D clippy::panic -D
  clippy::todo -D clippy::unimplemented -D clippy::unreachable`) clean.
- `cargo fmt --all --check` clean; `ruff check .` clean; `ruff format --check` on the changed
  Python clean.
- `check_rust_file_size.py` (694 files clean), `check_lib_rs.py` (10 roots clean),
  `check_lib_py.py` (830 files clean), `check_ledger_grammar.py`, `check_docs_links.py`,
  `sync_map_md.py --check`, `check_docstring_presence.py`, `check_python_conventions.py` clean.
- Comment ban: 0 hits.

## Residues

- `to_timestamp_ntz(<string>)` keeps DataFusion's parse (`'2020'` refuses; Spark answers).
- A string leaf in `CAST(… AS MAP<…, TIMESTAMP>)` keeps Arrow's parse (`cast_map/leaf.rs`).
- Rendering an LTZ instant after 2099 in a DST region zone uses standard time (chrono-tz's table
  ends in 2099; the kernel's proxy year is not used by the renderer).
- The native `repark.sql` door has no LTZ `TIMESTAMP`; its `TIMESTAMP` is the ANSI zoneless type.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: cast-ts-string-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every expectation reads from the committed Spark 4.1.2 oracle. Time-only
        cells compare the local date and time of day, because Spark resolves them against
        today's date. The live leg re-derives the fixture through the recorder.
      artifacts: [python/repark/tests/cast_ts_string_1_spark_oracle.json, python/repark/tests/_record_cast_ts_string_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: The 30 facade pins were red on main (step 1) and green on the fix. The
        three time-travel tests were strict xfails that failed on main.
      artifacts: [python/repark/tests/test_cast_ts_string_1.py, python/repark/tests/test_ice_tt_resolve_1_tt2.py]
    - id: AT-3
      status: ATTACKED
      evidence: Each grammar branch has a recorded string and a Rust test. This includes
        the byte-0 `T`, the sign-then-colon refusal, the zone after the fraction, the
        second dot, the trim set, the digit counts, the offset lengths and limits, the
        legacy padding, the prefixes, the short ids, the DST walls, the proxy years and
        both i64 edges.
      artifacts: [crates/repark-functions/src/tests/spark_string_timestamp.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The kernel is pure. It reads the clock once per batch for time-only
        strings, as Spark calls LocalDate.now per value. ANSI and the session zone are
        read per invocation from the session config. There is no shared state.
      artifacts: [crates/repark-functions/src/spark_string_timestamp.rs]
    - id: AT-5
      status: ATTACKED
      evidence: The parser works on bytes with bounded indexes and saturating digit
        accumulation. Zone ids resolve only through a whitelist pattern and chrono-tz. The
        error message escapes the value as Spark does. There is no unwrap or expect, and
        the panic-ban clippy flags are clean.
      artifacts: [crates/repark-functions/src/spark_string_timestamp/grammar.rs, crates/repark-functions/src/spark_string_timestamp/zone.rs]
    - id: AT-6
      status: ATTACKED
      evidence: TRY_CAST changes from timestamp[ns] to timestamp[us, UTC], and every
        column leg asserts the Arrow type. The residues (NTZ, map leaf, rendering after
        2099, native door) are named in the registry row.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/test_cast_ts_string_1.py]
    - id: AT-7
      status: N/A
      justification: No performance claim; the kernel replaces a per-row Arrow parse with a per-row byte scan.
    - id: AT-8
      status: ATTACKED
      evidence: No STATUS.md, Cargo.toml or Cargo.lock edit and no crate-DAG change. The
        crate root stays under its ceiling (179 of 182). instant_ts.rs stays under the
        default (973) because the string-cast helpers live in the new module.
      artifacts: [scripts/check_lib_rs.py, scripts/check_rust_file_size.py]
    - id: AT-9
      status: ATTACKED
      evidence: The registry row, the ICE-TT-RESOLVE-1 closing note and every touched
        map.md carry the change with pins.
      artifacts: [docs/spark-sql-iceberg-parity.md, crates/repark-functions/src/spark_string_timestamp/map.md, python/repark/tests/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: to_timestamp_ntz is isolated on the Arrow grammar. The existing
        to_timestamp, try_to_timestamp and time-travel files ran green (16 files, 796
        passed before the flip). Every file that spells a timestamp cast runs in step 5.
      artifacts: [crates/repark-functions/src/timestamp_ltz_ntz.rs, python/repark/tests/test_ice_tt_resolve_1.py]
  complete: true
```
