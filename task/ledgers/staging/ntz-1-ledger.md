# Unit ledger — WO NTZ-1 · TIMESTAMP_NTZ literals, casts and store assignment answer as Spark

**Date:** 2026-09-26 · **Branch:** `feat/ntz-1` · **Base:** `fbd97ef2`
(`origin/main`) · **Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Both scoreboard cells `TY-TIMESTAMP-NTZ` and `TY-TIMESTAMP-NTZ-V3`
fail at the first INSERT: the SQL door refuses the `TIMESTAMP_NTZ` literal and
every `CAST … AS TIMESTAMP_NTZ` with `UNSUPPORTED_TIMESTAMP_NTZ`, although the
type table, CREATE, the reader/writer and every partition transform are already
right. This unit adopts Spark's contract whole (R1): NTZ is a wall clock the
session zone never moves, in three slices — the literal and explicit casts
(slice 1), store assignment (slice 2), the storage surface and TZ-6 (slice 3).

**What Spark does (measured 2026-09-26; Spark probes `ntz-spark.json`,
`ntz2-spark.json`, `ntz4-spark.json`, `ntz7-spark.json`, `ntz7c-spark.json`,
RePark main `ntz-repark.json`, `ntz2-repark.json`, `ntz4-repark.json`).** A
`TIMESTAMP_NTZ '<s>'` literal is the naive wall: a zone suffix is dropped, a
date-only text is midnight, 1–9 fraction digits truncate to micros, and the
unaliased name is `TIMESTAMP_NTZ '<wall>'`; an unparsable text is
`ParseException` `INVALID_TYPED_LITERAL` (42604) with the `== SQL` window.
`CAST` / `::` / `TRY_CAST` to NTZ answer the wall for STRING, DATE, NULL,
TIMESTAMP (session-zone wall) and NTZ sources, `CAST_INVALID_INPUT` naming
`"TIMESTAMP_NTZ"` for a malformed string under ANSI, NULL under `TRY_CAST`,
and refuse numeric sources and targets with
`DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION` (42K09); `CAST(TIMESTAMP … AS
BIGINT)` stays epoch seconds. The DataFrame `cast("timestamp_ntz")` and
`cast(TimestampNTZType())` answer as the SQL cast. The literal and the cast
reach every DML door and CTAS; the one-row cells replay rows, cols,
`md.schema` and filter on v2 and v3. A nested NTZ cast target answers a value
on Spark and stays a refusal on RePark (the R4 text, C-005).

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `TIMESTAMP_NTZ '<s>'` on the Spark door is a naive microsecond wall, its zone suffix dropped and its fraction truncated to six digits, typed `timestamp_ntz`, named `TIMESTAMP_NTZ '<wall>'` unaliased; an unparsable `<s>` is Spark's `ParseException` `INVALID_TYPED_LITERAL` (42604). | Rust door pins plus the facade replay assert value, Arrow type, name and the refusal text. | PROVEN | `ntz_door.rs` `ntz_literal_is_a_naive_microsecond_wall`, `ntz_literal_drops_a_zone_suffix_and_truncates_to_micros`, `invalid_ntz_literal_is_a_parse_error`; `test_ntz_1.py` `lit*` steps; `test_pg_ntz_literal_answers_the_wall`; `test_typeof_ntz_literal_spells_timestamp_ntz`. |
| C-002 | `CAST` / `::` / `TRY_CAST` to `TIMESTAMP_NTZ` answer Spark's value for STRING, DATE, NULL, `TIMESTAMP` (session-zone wall) and `TIMESTAMP_NTZ` sources, `CAST_INVALID_INPUT` naming `"TIMESTAMP_NTZ"` for a malformed string under ANSI, NULL under `TRY_CAST`, and refuse numeric sources and numeric targets with `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION` (42K09); `CAST(TIMESTAMP … AS BIGINT)` stays epoch seconds. | Same doors; the NTZ→numeric refusal names the class and both types; the LTZ epoch-seconds path is guarded. | PROVEN | `ntz_door.rs` `ntz_cast_sources_answer_as_spark` (incl. the NTZ→NTZ identity leg), `try_cast_to_ntz_is_null_on_garbage`, `ntz_cast_refuses_numeric_sources_and_targets`, `ltz_numeric_cast_stays_epoch_seconds`; `test_ntz_1.py` cast/refusal steps; `test_pg_ntz_cast_answers_the_wall`. The malformed-string and rule-wrapped comparisons skip class/condition per R-3 (SQLSTATE and core text equal). |
| C-003 | The DataFrame door's `cast("timestamp_ntz")` and `cast(TimestampNTZType())` answer as the SQL cast. | One facade test through the same UDF, value and dtype. | PROVEN | `test_ntz_1.py` `test_dataframe_door_casts_answer_as_spark`. |
| C-004 | The literal and the cast reach INSERT VALUES, INSERT … SELECT, UPDATE, DELETE, MERGE and CTAS; cells `TY-TIMESTAMP-NTZ` and `TY-TIMESTAMP-NTZ-V3` replay EQUAL on every `obs` key. | One DML door test plus the harness replay of both cells (and three neighbours unchanged). | PROVEN | `ntz_door.rs` `ntz_literal_reaches_every_dml_door`; `test_ntz_1.py` v2/v3 cell tests; scoreboard `compare.py` 2026-09-27: `TY-TIMESTAMP-NTZ` EQUAL, `TY-TIMESTAMP-NTZ-V3` EQUAL, `TY-TIMESTAMP` EQUAL, `TY-TIMESTAMP-LTZ` EQUAL, `TY-PROMOTE-DATE-TS` SPARK-CANNOT both refuse (same ALTER COLUMN step, registered class). |
| C-005 | A nested cast target carrying `TIMESTAMP_NTZ` refuses with the R4 text (residue, dated, both texts). | The refusal text is pinned on two doors; the residue carries both texts. | PROVEN | `ntz_door.rs` `nested_ntz_cast_target_keeps_the_r4_refusal`; `test_ntz_1.py` `test_nested_cast_target_keeps_the_r4_refusal`; `keyword_lower.rs` `ntz_refusal_names_the_registry_row`; R-2. |
| C-006 | `TIMESTAMP` and `DATE` values store into a `TIMESTAMP_NTZ` column as the session-zone wall / midnight on VALUES, INSERT … SELECT, UPDATE, MERGE and the DataFrame append; `TIMESTAMP_NTZ` values store into a `TIMESTAMP` column as the session-zone instant. | Slice 2. | OPEN | Slice 2. Side effect measured 2026-09-27 (V-004, Slice 1 already): the LTZ-column INSERT of an NTZ literal stores the session-zone instant (unix 1704128400 under America/New_York, equal to Spark); the NTZ-column direction untouched. |
| C-007 | STRING, INT and BOOLEAN sources into a `TIMESTAMP_NTZ` column refuse with Spark's `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` text naming `"TIMESTAMP_NTZ"` on VALUES and INSERT … SELECT. | Slice 2. | OPEN | Slice 2. |
| C-008 | Partition transforms, `.partitions`, `.files` bounds, pruning, v3, CTAS, DDL presentation and a Spark-written NTZ table answer Spark's measured values. | Slice 3. | OPEN | Slice 3. |
| C-009 | TZ-6 states Spark's contract; the R2 residues are dated with both texts; no `UNSUPPORTED_TIMESTAMP_NTZ` text remains reachable for a scalar literal or cast (grep the tree). | Slice 3. | OPEN | Slice 3. |

## Mutation record (2026-09-26)

Each line was broken, the named tests ran red, and the file was restored byte-identical.

| # | Mutation | Red |
|---|---|---|
| M1 | Drop the zone-suffix strip (`parse_timestamp_ntz_wall` parses the raw text, localizing the suffix). | `ntz_literal_drops_a_zone_suffix_and_truncates_to_micros` reds (answers the instant, not the wall), restored green. |
| M2 | Unlocalize in UTC instead of the session zone (`ntz_single` binds `zone` to UTC). | `ntz_cast_sources_answer_as_spark` reds on the New York legs (answers the UTC wall), restored green. |
| M3 | Remove the S1-5 gate (the NTZ arm of `spark_refuses_cast`). | `ntz_cast_refuses_numeric_sources_and_targets` reds (TZ-5 leaks epoch seconds instead of refusing), restored green. The first attempt left the message-branch check in place and stayed green, so the gate was restructured to the single matrix predicate before this verification. |
| M4 | Wire the cast lowering only into `lower_spark_keywords` (drop the `TimestampNsCastLower::post_visit_expr` arm). | `ntz_literal_reaches_every_dml_door` reds on the MERGE row (`NotImplemented("Unsupported SQL type TIMESTAMP_NTZ")`), restored green. |
| M5 | Slice 2: remove the S2-1 arm → the VALUES / UPDATE New York pins red. | Pending slice 2. |
| M6 | Slice 2: remove the NTZ gate call → the INT VALUES pin reds. | Pending slice 2. |

## Tests rewritten

- `test_spark_sql_grammar_1.py::test_pg_ntz_literal_refuses` → `test_pg_ntz_literal_answers_the_wall`: asserts Spark's naive wall on the Arrow path (value, `timestamp[us]`, non-null — the wall literal folds like Spark, unlike the folded-literal precedent).
- `test_spark_sql_grammar_1.py::test_pg_ntz_cast_refuses` → `test_pg_ntz_cast_answers_the_wall`: same for `CAST('…' AS TIMESTAMP_NTZ)`.
- `test_fnp11b_typeof.py::test_typeof_ntz_literal_is_blocked_on_the_dialect_seam` → `test_typeof_ntz_literal_spells_timestamp_ntz`: asserts the recorded `timestamp_ntz` for TYPEOF-SQL-13.
- `test_fnp11a_temporal.py`: `D4_BLOCKED_SQL` loses `timestampdiff(DAY, ntz, TIMESTAMP_NTZ'2024-03-11 01:00:00')`; both cells (ANSI on/off) replay the recorded `[0, None]` bigint answer — no divergence, nothing stays skipped.
- `keyword_lower.rs::tests::ntz_refusal_names_the_registry_row`: asserts the R4 text.
- `keyword_lower.rs::tests::the_probe_finds_only_ns_casts_and_empty_map_calls` → `the_probe_finds_ns_and_ntz_casts_and_empty_map_calls`: gains the NTZ cast/typed-string probe legs and the nested-target negative leg.
- `cast_legality.rs::tests::the_deny_matrix_is_exactly_date_to_int_and_back` → `the_deny_matrix_is_date_int_and_ntz_numeric`: the NTZ→numeric pairs join the deny matrix.
- `cast_legality.rs::tests::adjacent_temporal_and_numeric_pairs_are_untouched`: the micros→Int non-refusal asserts give way to nanos/LTZ non-refusal asserts (the intended inversion — micros→Int now refuses), plus a micros→Boolean leg.
- `test_fnp11a_r2.py` `_ntz_call` literal rewrite stays (still passes); removable — a later round may route it through the real literal.
- `test_u9_types_1.py` oracle (`u9_types_1_spark_oracle.json`): residue R-2 retired — `ltz/v2|v3/side-insert-ntz` and `ltz/v2/cast-ntz` now answer Spark (`ok`, the four cast rows); `side-select` R-1 repark gains row `[4, "2024-04-01T00:00:00"]` (Spark's value; R-1 stands for the row-3 STRING write).

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ntz-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each Slice 1 clause is walked against Spark's measured answer on the same door; the new facade suite replays 25 oracle queries plus v2/v3 cells, and the Rust door suite pins values, types, names and refusal texts.
      artifacts: [python/repark/tests/test_ntz_1.py, python/repark/tests/ntz_1_spark_oracle.json, crates/repark-spark/src/tests/ntz_door.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Every named behavior is pinned per door (literal walls and truncation, string/date/NULL/instant/NTZ cast sources, try_cast NULL, the DML reach, CTAS type); the rewritten grammar/typeof/temporal pins hold their recorded answers.
      artifacts: [python/repark/tests/test_ntz_1.py, crates/repark-spark/src/tests/ntz_door.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal text is pinned (INVALID_TYPED_LITERAL with the SQL window, DATATYPE_MISMATCH both directions, CAST_INVALID_INPUT, R4 nested) with class, SQLSTATE and first line where the engine framing allows; the M1–M4 mutations break one mechanism each and the named tests red before restore.
      artifacts: [python/repark/tests/test_ntz_1.py, crates/repark-spark/src/tests/ntz_door.rs]
    - id: AT-4
      status: N/A
      justification: No commit, isolation or concurrency surface; each pin runs on a fresh warehouse in one session.
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling; SQL text and catalog writes on scratch warehouses only.
    - id: AT-6
      status: ATTACKED
      evidence: No stored-format change; scoreboard compare.py 2026-09-27 says TY-TIMESTAMP-NTZ EQUAL, TY-TIMESTAMP-NTZ-V3 EQUAL, TY-TIMESTAMP EQUAL, TY-TIMESTAMP-LTZ EQUAL, TY-PROMOTE-DATE-TS SPARK-CANNOT both refuse at the same ALTER COLUMN step.
      artifacts: [python/repark/tests/test_ntz_1.py, python/repark/tests/ntz_1_spark_oracle.json]
    - id: AT-7
      status: N/A
      justification: No performance claim; the literal UDF is Immutable so filters prune, pinned only by value.
    - id: AT-8
      status: ATTACKED
      evidence: No dependency, no Cargo.toml edit, no new crate edge (repark-spark and repark-python already depend on repark-functions); new code lives in the pre-named modules and every touched file holds its size gate.
      artifacts: [crates/repark-functions/src/timestamp_ntz_cast.rs, crates/repark-spark/src/spark_rewrites/timestamp_ntz_literal.rs, crates/repark-spark/src/keyword_lower/ntz_cast_lower.rs]
    - id: AT-9
      status: N/A
      justification: No registry change in slice 1; TZ-6 and the TY rows are slice 3 (C-009).
    - id: AT-10
      status: ATTACKED
      evidence: Each pin asserts exact values, types or texts; M1–M4 break one expectation each and the named tests red before restore.
      artifacts: [python/repark/tests/test_ntz_1.py, crates/repark-spark/src/tests/ntz_door.rs]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-1 | Dated 2026-09-26 (silent-wrong-data finding for its own unit, reported in the hand-back): `INSERT … VALUES (0, 1)` into a `TIMESTAMP` (LTZ) column — Spark refuses `[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the table `sc`.`ns`.`l`: Cannot safely cast `c` "INT" to "TIMESTAMP". SQLSTATE: KD000`, RePark main silently writes `1970-01-01 00:00:00.000001`. (U9 R-1 already holds the STRING half.) |
| R-2 | Dated 2026-09-26 (R2 naming class; unaliased casts keep RePark's existing cast naming per R6): Spark names `SELECT CAST('2024-01-01' AS TIMESTAMP_NTZ)` as `CAST(2024-01-01 AS TIMESTAMP_NTZ)` and `SELECT try_cast('x' AS TIMESTAMP_NTZ)` as `TRY_CAST(x AS TIMESTAMP_NTZ)` (measured live on Spark 4.1.2, `ntz7c-spark.json`; probe1's quoted spellings `CAST('…' …)` / `try_cast('x' …)` do not reproduce there); RePark renders the embedded-UDF call (`__repark_cast_timestamp_ntz__(Utf8("2024-01-01"))`). Values and dtypes are EQUAL; names are not compared on those legs. |
| R-3 | Dated 2026-09-26 (pre-existing engine framing, shared with the DATE pairs — not NTZ-specific): the malformed-string cast raises at execution time, so RePark reports `PySparkException` with condition `None` where Spark reports `DateTimeException` with condition `CAST_INVALID_INPUT` (SQLSTATE `22018` and the core text are EQUAL); the NTZ→numeric refusal crosses the `spark_expr_semantics` rule header, so its parsed condition is `None` (the `cannot cast "TIMESTAMP_NTZ" to "<T>"` clause and SQLSTATE `42K09` are EQUAL). |
| R-4 | Dated 2026-09-26 (MERGE error path bypasses the R4 mapper; out of the S1 fence, `merge.rs` is frozen): a nested NTZ cast target inside MERGE answers `UnsupportedOperationException: This feature is not implemented: Unsupported SQL type TIMESTAMP_NTZ` where the same construct on SELECT/UPDATE answers the R4 text `[UNSUPPORTED_TIMESTAMP_NTZ] TIMESTAMP_NTZ inside a nested cast target (ARRAY, STRUCT or MAP) is not supported yet; the scalar TIMESTAMP_NTZ literal and cast are. See TZ-6 (docs/spark-sql-iceberg-parity.md). SQLSTATE: 0A000`. |
| R-5 | Dated 2026-09-27 (V-003; no product change this round): Spark types `SELECT CAST('2024-01-01 00:00:00' AS TIMESTAMP_NTZ)` nullable (`ntz5-nullable-spark.json`: True under ANSI off and on); RePark folds the literal-only cell to non-null (False under both fixtures). |

## Fold r1 (2026-09-27)

Verifier r1 passed Slice 1 scopes 1–5 except V-001..V-006, folded here without
rewriting history.

- V-001: the invalid-literal caret line underlines the whole literal (byte
  length word-through-quote); the facade `lit_bad` step compares all four
  lines and a second length pins the rule.
- V-002: NTZ-to-numeric refusals render the literal UDF by its Spark display
  name (through the nullability wrapper on INT-family targets); both doors
  compare the full first line. R-3 stays: the condition is still None.
- V-003: the CAST nullability measured above; the divergence is R-5.
- V-004: the C-006 side-effect note above.
- V-005: the three Tests-rewritten lines above.
- V-006: the size row and the subject deviation below.

| File | Order budget | Landed |
|---|---|---|
| `crates/repark-spark/src/spark_rewrites/mod.rs` | +6 | +7: signature +3, NTZ extend +3, `mod` line +1 (the visibilities are net 0). The +1 stays: each line is load-bearing and the mechanical trims fail rustfmt (the one-line signature is 103 chars over the 100 ceiling; either one-line `extend` exceeds `fn_call_width` 60). |

Commit-subject deviation, recorded as-is: Slice 1 landed as `feat(ntz-1):
TIMESTAMP_NTZ literals and CAST ... AS TIMESTAMP_NTZ answer as Spark on both
doors — Slice 1` where the order named `feat(ntz-1): TIMESTAMP_NTZ literals
and CAST … AS TIMESTAMP_NTZ answer as Spark on both doors — TY-TIMESTAMP-NTZ
and TY-TIMESTAMP-NTZ-V3 EQUAL`.

## Fold r2 (2026-09-27)

Verifier r2 passed V-001..V-006 and filed V-007..V-010 with Spark 4.1.2
measured the same day (`target/verify/sp.py`, outputs copied verbatim to
`ntz-1-probes/ntz6-nested-spark.json`); folded here without rewriting
history.

- V-007/V-009: the invalid-literal position, 32-char window start, pad and
  caret width count CHARS, slicing only at char boundaries. Spark's
  measured answer for `SELECT '` + é×30 + `', TIMESTAMP_NTZ'x'` is
  INVALID_TYPED_LITERAL at position 42 with window `...` + é×29 +
  `', TIMESTAMP_NTZ'x'`, a 35-space pad and 16 carets — pinned byte for
  byte by `the_window_and_carets_count_chars_before_non_ascii_sql` and the
  facade `lit_bad_wide` oracle step; `TIMESTAMP_NTZ'é'` pins 16 carets in
  `a_non_ascii_literal_gets_sixteen_carets`. The V-001 pin text and the
  `test_ntz_1.py` docstring now say the CHAR length of the literal.
- V-008: a nested NTZ cast renders Spark's text in the numeric refusal —
  `literal_display_name` also maps the cast-to-NTZ UDFs to
  `CAST(<arg> AS TIMESTAMP_NTZ)` / `TRY_CAST(<arg> AS TIMESTAMP_NTZ)`,
  rendering a Utf8 literal argument unquoted and a column by its Spark
  name. Pinned byte-equal by
  `the_ntz_numeric_refusal_renders_a_nested_cast_like_spark`,
  `nested_ntz_cast_renders_like_spark_in_the_numeric_refusal` and the
  facade `cast_nested_ntz_bigint` step (`CAST(CAST(2024-01-01 AS
  TIMESTAMP_NTZ) AS BIGINT)`); the measured-EQUAL `TRY_CAST` and column
  shapes are pinned beside them (`ntz6-nested-spark.json`).
