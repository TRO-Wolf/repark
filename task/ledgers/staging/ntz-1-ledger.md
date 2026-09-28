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
| C-006 | `TIMESTAMP` and `DATE` values store into a `TIMESTAMP_NTZ` column as the session-zone wall / midnight on VALUES, plain INSERT … SELECT, UPDATE, MERGE and the DataFrame append — INSERT OVERWRITE, INSERT … BY NAME and STRUCT fields excluded (R-NTZ-S2-4, R-NTZ-S2-5, R-NTZ-S2-6); `TIMESTAMP_NTZ` values store into a `TIMESTAMP` column as the session-zone instant. | Slice 2. | PROVEN | Slice 2 (2026-09-27): the S2-1 cast retarget (`rewrite_cast` → `rewrite_ntz_target_cast`) converts VALUES, INSERT … SELECT and the DataFrame append (which lowers to one); the SQL-site wall-cast wrap converts the identity UPDATE projection (`update_projection_sql`), the MERGE UPDATE arms (`store_assignment_cast_sql`) and the MERGE INSERT arm (`insert_stream_checked` converting subquery). Rust pins `ntz_store.rs` (`ltz_values_store_their_session_zone_wall`, `update_and_merge_store_the_session_zone_wall` UTC+NY, `date_values_store_midnight`, `ntz_values_store_into_a_timestamp_column_as_session_instants`) and facade `test_ntz_1.py` (`test_store_values_and_select_answer_as_spark`, `test_update_and_merge_store_the_session_zone_wall` UTC+NY with rule-derived UTC legs, `test_dataframe_append_stores_the_session_zone_wall`, `test_ntz_values_store_into_timestamp_as_session_instants`) replay the probe SQL and Spark's recorded walls; probe6 repark `sel` byte-EQUAL to `ntz6-spark.json`, probe2 `sel`/`sel_after_dml` EQUAL. Zero TZ pins changed answer under S2-1 (40+23+571 before and after). V-004 side effect now pinned by `ntz_values_store_into_a_timestamp_column_as_session_instants`. Fold VN part 2 (2026-09-28): the retarget is a pre-pass over the store projection only (whole-expression casts of the DML projection and VALUES rows, bare values wrapped at NTZ targets); the `plan_contains_dml` gate is gone. Re-verify fold (2026-09-28): VALUES cells at naive-microsecond VALUES-schema fields wrap too (direct and pass-through nests), so the VALUES claim holds for zone-shift and truncation cells again; pins `test_ntz_9_verify.py` + `ntz_9_verify_spark_oracle.json` and Rust `ntz_store` VALUES pins. |
| C-007 | STRING, INT and BOOLEAN sources into a `TIMESTAMP_NTZ` column refuse with Spark's `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` text naming `"TIMESTAMP_NTZ"` on VALUES, INSERT … SELECT, UPDATE, MERGE and the DataFrame append — INSERT OVERWRITE and INSERT … BY NAME excluded (R-NTZ-S2-4, R-NTZ-S2-5). | Slice 2. | PROVEN | Slice 2 (2026-09-27): new `write/ntz_store.rs::refuse_ntz_writes` in the void-gate shape, called beside every `refuse_void_writes` (VALUES rows via a row probe, INSERT … SELECT, UPDATE SET, both MERGE arms, and so the DataFrame append); `spark_update_type_name` answers `TIMESTAMP_NTZ` for naive timestamps. The gate judges pre-wrap types on every path and fires before WI-2. Rust pin `ntz_refusals_name_timestamp_ntz` (VALUES/SELECT STRING+INT+BOOLEAN plus UPDATE and MERGE STRING rows) and facade `test_store_refusals_name_timestamp_ntz` (plus the STRING append) assert class, condition, SQLSTATE and Spark's first line. Fold VN (2026-09-28): `spark_update_type_name` answers `TIMESTAMP_NTZ` only for microsecond-naive timestamps, so an unlocalized `TIMESTAMP'…'` literal source names `TIMESTAMP` again (pin `update_refusal_names_a_timestamp_literal_source_as_timestamp`); the claim names exactly the proved doors. |
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
| M5a | Slice 2: remove the S2-1 arm → the VALUES / INSERT … SELECT New York pins red. | `ltz_values_store_their_session_zone_wall` reds, restored green (2026-09-27). |
| M5b | Slice 2: remove the SQL-site wrap (`ntz_wall_cast_sql` renders the raw expression) → the UPDATE and MERGE New York pins red. | `update_and_merge_store_the_session_zone_wall` reds (stores 12:00), restored green (2026-09-27). |
| M6 | Slice 2: remove the NTZ gate call → the INT VALUES pin reds. | `ntz_refusals_name_timestamp_ntz` reds (answers instead of refusing), restored green (2026-09-27). |

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
- Slice 2 (2026-09-27): `partitioned_merge.rs::merge_days_partitioned_upsert` and `::merge_days_partitioned_mor_delete_and_insert` gain `repark_functions::register_all(&ctx)` in setup — MERGE into the tests' naive-microsecond `ts` column now emits the wall-cast UDF call, which needs the production registration; assertions unchanged. The file sits at its exact size baseline, so two blank lines between statements in `merge_partitioned_mixed_upsert_stamps_partition_values` come out as the line-neutral offset (comments untouched).
- Slice 2 (2026-09-27): no other existing test was rewritten; the TZ pin set (571 pytest + 40 functions + 23 `session_timezone`) answers identically before and after S2-1.

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ntz-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each Slice 1 clause is walked against Spark's measured answer on the same door; the new facade suite replays 25 oracle queries plus v2/v3 cells, and the Rust door suite pins values, types, names and refusal texts. Slice 2 (2026-09-27): each store clause is walked against the probe SQL and Spark's recorded walls/refusals on the same door; probe6 sel is byte-EQUAL and probe2 value steps EQUAL.
      artifacts: [python/repark/tests/test_ntz_1.py, python/repark/tests/ntz_1_spark_oracle.json, crates/repark-spark/src/tests/ntz_door.rs, crates/repark-spark/src/tests/ntz_store.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Every named behavior is pinned per door (literal walls and truncation, string/date/NULL/instant/NTZ cast sources, try_cast NULL, the DML reach, CTAS type); the rewritten grammar/typeof/temporal pins hold their recorded answers. Slice 2 (2026-09-27): every store door is pinned per zone (VALUES/SELECT/UPDATE/MERGE/append walls, midnight, session instants) in UTC and New York; the TZ pin set answers identically before and after.
      artifacts: [python/repark/tests/test_ntz_1.py, crates/repark-spark/src/tests/ntz_door.rs, crates/repark-spark/src/tests/ntz_store.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal text is pinned (INVALID_TYPED_LITERAL with the SQL window, DATATYPE_MISMATCH both directions, CAST_INVALID_INPUT, R4 nested) with class, SQLSTATE and first line where the engine framing allows; the M1–M4 mutations break one mechanism each and the named tests red before restore. Slice 2 (2026-09-27): the CANNOT_SAFELY_CAST store refusals are pinned on VALUES/SELECT/UPDATE/MERGE plus the append with class, condition, SQLSTATE and first line; M5a/M5b/M6 red before restore.
      artifacts: [python/repark/tests/test_ntz_1.py, crates/repark-spark/src/tests/ntz_door.rs, crates/repark-spark/src/tests/ntz_store.rs]
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
      evidence: No dependency, no Cargo.toml edit, no new crate edge (repark-spark and repark-python already depend on repark-functions); new code lives in the pre-named modules and every touched file holds its size gate. Slice 2 (2026-09-27): still no dependency or Cargo.toml edit (the UDF-name const is mirrored, pinned equal); the Q2 INSERT-arm shape moved into insert_stream_checked to honor merge/mod.rs's exact baseline, and every touched file holds its size gate.
      artifacts: [crates/repark-functions/src/timestamp_ntz_cast.rs, crates/repark-spark/src/spark_rewrites/timestamp_ntz_literal.rs, crates/repark-spark/src/keyword_lower/ntz_cast_lower.rs, crates/repark-iceberg/src/write/ntz_store.rs]
    - id: AT-9
      status: N/A
      justification: No registry change in slice 1; TZ-6 and the TY rows are slice 3 (C-009).
    - id: AT-10
      status: ATTACKED
      evidence: Each pin asserts exact values, types or texts; M1–M4 break one expectation each and the named tests red before restore. Slice 2 (2026-09-27): each store pin asserts exact walls, instants or refusal texts; M5a/M5b/M6 break one mechanism each and the named tests red before restore.
      artifacts: [python/repark/tests/test_ntz_1.py, crates/repark-spark/src/tests/ntz_door.rs, crates/repark-spark/src/tests/ntz_store.rs]
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
| R-NTZ-S2-1 | Dated 2026-09-27 (orchestrator ruling Q3; S1 lowering-reach follow-up, no Slice 2 code change): `UPDATE … SET c = CAST(… AS TIMESTAMP_NTZ)` refuses with the R4 nested-target text because identity interception (`spark_ast.rs:109`) embeds the SET value before `lower_spark_keywords` (`:119`) runs, so the embedded SELECT carries unlowered door syntax; literals work via the earlier token layer. No probe uses an explicit cast in SET. |
| R-NTZ-S2-2 | Dated 2026-09-27 (for its own unit; no Slice 2 code change): `INSERT … VALUES (0, 1)` into a `TIMESTAMP` (LTZ) column — Spark refuses `CANNOT_SAFELY_CAST` INT → TIMESTAMP (`ntz4-spark.json` `ins_l_int`), RePark writes `1970-01-01 00:00:00.000001`. |
| R-NTZ-S2-3 | Dated 2026-09-27 (comparison semantics, out of the S2 store fence; no Slice 2 code change): `t.filter(F.col("c") == F.lit(datetime.datetime(2031, 1, 1)))` on an NTZ column answers `[Row(id=1)]` where Spark answers `[]` (`ntz2-spark.json` `dfapi.filter_lit_naive_dt`). The plan casts the NTZ side up to LTZ (`to_timestamp(c@1) = 1924992000000`) with no naive-target cast, so S2-1 is provably inert here and main's `[]` was vacuous (main could not load NTZ rows); the divergence predates the unit and needs a dedicated DataFrame-door literal/comparison probe. |
| R-NTZ-S2-4 | Dated 2026-09-28 (verifier VN-4, wrong on base, no fold code change): `INSERT OVERWRITE … SELECT TIMESTAMP'2024-01-01 12:00:00Z'` into a `TIMESTAMP_NTZ` column in a New York session — Spark stores `2024-01-01 07:00:00`, RePark stores `2024-01-01 12:00:00` (`ntz7` probe `v_ow_ltz_read`); a STRING source — Spark refuses `[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the table `hc`.`ns`.`vo`: Cannot safely cast `c` "STRING" to "TIMESTAMP_NTZ". SQLSTATE: KD000`, RePark refuses `INSERT OVERWRITE cannot store-assign column `c`: source type Utf8 is not ANSI-store-assignable to target type Timestamp(µs) (…)` (`v_ow_str`). |
| R-NTZ-S2-5 | Dated 2026-09-28 (verifier VN-4, wrong on base, no fold code change): `INSERT INTO … BY NAME SELECT … TIMESTAMP'2024-01-01 12:00:00Z' AS c` into a `TIMESTAMP_NTZ` column in a New York session — Spark stores `2024-01-01 07:00:00`, RePark stores `2024-01-01 12:00:00` (`ntz7` probe `v_byname_ltz_read`); a STRING source — Spark refuses `[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] … Cannot safely cast `c` "STRING" to "TIMESTAMP_NTZ". SQLSTATE: KD000`, RePark refuses `append cannot store-assign column `c`: source type Utf8 is not ANSI-store-assignable to target type Timestamp(µs) (…)` (`v_byname_str`). |
| R-NTZ-S2-6 | Dated 2026-09-28 (verifier VN-6a, wrong on base, no fold code change): `INSERT … VALUES (1, named_struct('t', TIMESTAMP'2024-01-01 12:00:00Z'))` into `(id INT, v STRUCT<t: TIMESTAMP_NTZ>)` in a New York session — Spark stores `v.t = 2024-01-01 07:00:00`, RePark stores `2024-01-01 12:00:00` (`ntz7` probe `v_struct_read`). |
| R-NTZ-S2-7 | Dated 2026-09-28 (verifier VN-6b, cited from `ntz4-*.json`, not re-run): `INSERT … VALUES (1, '2024-01-01 00:00:00')` into a `TIMESTAMP` (LTZ) column — Spark refuses `[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the table `sc`.`ns`.`l`: Cannot safely cast `c` "STRING" to "TIMESTAMP". SQLSTATE: KD000` (`ntz4-spark.json` `ins_l_str`), RePark writes the row (`ntz4-repark.json` `ins_l_str` counts 1). U9 R-1 already holds the STRING half; R-NTZ-S2-2 holds the INT half. |
| R-NTZ-S2-8 | Dated 2026-09-28 (verifier VN-5, no fold code change): VALUES inserts into NTZ tables run ~2.6× slower than base because every row plans `SELECT <row>` plus a full analyzer pass (`void_type.rs` `check_ntz_row`): 500 rows 531 ms → 1370 ms, 2000 rows 2086 ms → 5483 ms on a debug build; the `TIMESTAMP` (timestamptz) table is unchanged (616/2389 ms). |
| R-NTZ-S2-9 | Dated 2026-09-28 (re-verify RN-2, no fold code change): `UPDATE … SET id = from_utc_timestamp('2024-01-01 12:00:00','UTC') WHERE id = 1` into an INT column — Spark refuses `[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] … Cannot safely cast `id` "TIMESTAMP" to "INT" …`, RePark names the source `"TIMESTAMP_NTZ"` (`ntz9` probe `w_ref_from`). The name is judged from the pre-analyzer type alone (`spark_update_type_name`), where a zone-shift call over a string and a genuine NTZ source are both naive microseconds yet need different names, so no change local to `update_cast.rs:121` can fix it. Pinned as a divergence step in `test_ntz_9_verify.py`. |
| R-NTZ-S2-10 | Dated 2026-09-28 (re-verify, no fold code change): `UPDATE … SET id = CAST(c AS TIMESTAMP_NTZ) WHERE id = 1` into an INT column — Spark refuses `[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] … Cannot safely cast `id` "TIMESTAMP_NTZ" to "INT" …`, RePark refuses `[UNSUPPORTED_TIMESTAMP_NTZ] TIMESTAMP_NTZ inside a nested cast target …` (`ntz9` probe `w_ref_cast`). The text comes from the nested-target gate in `keyword_lower.rs`, not from `update_cast.rs:121`, so routing a top-level NTZ cast to the refusal path is out of the brief's locality bound. Pinned as a divergence step in `test_ntz_9_verify.py`. |
| R-NTZ-S2-11 | Dated 2026-09-28 (re-verify, no fold code change): `INSERT … VALUES (1, TIMESTAMP'2024-01-01 12:00:00'), (5, TIMESTAMP_NTZ'2024-03-10 02:30:00')` into an NTZ column in a New York session — Spark widens the mixed inline table to TIMESTAMP and gap-resolves the NTZ wall, storing `2024-03-10 03:30:00`; RePark converts each row independently and stores `2024-03-10 02:30:00` (`ntz9` probe `r_wide`). Widening needs cross-row VALUES coercion, not a one-arm fix. Pinned as a divergence step in `test_ntz_9_verify.py`. |

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
- V-010: the literal nullability leg is measured, not assumed —
  `ntz5_nullable_probe.py` now also records `SELECT TIMESTAMP_NTZ
  '2024-01-02 03:04:05' AS v` and `ntz5-nullable-spark.json` says
  nullable False under ANSI off and on (Spark 4.1.2, re-measured
  2026-09-27 through `jvm-lock.sh`); the PG-ntz-lit docstring cites it.

## Slice 2 (2026-09-27)

Branch `feat/ntz-1-s2`, base `origin/main`, model Muse Spark
(`muse-spark-1.3-contributor`). One commit; C-008 and C-009 stay OPEN.

- S2-1 as ordered: `instant_ts.rs` `rewrite_cast` calls
  `timestamp_ntz_cast::rewrite_ntz_target_cast` before its LTZ arms (+3 lines;
  file 972 → 975, inside the +8 unit cap). A `Cast` to exactly `Timestamp(µs, None)`
  from an instant, a non-microsecond naive timestamp, a date or a string becomes the
  embedded cast UDF; a microsecond-naive source keeps the cast and everything else
  falls to the existing arms. The whole TZ pin set ran before any edit
  (`target/ntz-s2/before.txt`) and again after: zero pins changed answer (40
  `repark-functions`, 23 `session_timezone`, 571 pytest).
- The mandated S2-3 re-measure showed S2-1 fixing VALUES, INSERT … SELECT, the
  DataFrame append (it lowers to SELECT, so `conform_batch` stays untouched) and the
  NTZ-into-TIMESTAMP direction — but the probe-shaped UPDATE (`WHERE id = 0`) and
  both MERGE arms still stored the UTC wall. Traces proved S2-1 structurally cannot
  reach them: the identity UPDATE path projects the raw SET value with no planned
  cast and its rewrite UNION coerces naive → LTZ with the batch cast reinterpreting
  downstream; MERGE UPDATE arms wrap values in `arrow_cast`, which simplifies to a
  reinterpret cast in the optimizer, after the analyzer ran; MERGE INSERT arms
  project plain LTZ into the same reinterpret. Only the fork-path UPDATE (no WHERE)
  was fixed by S2-1. First HALT with Q1–Q3; the orchestrator ruled shape (a) for the
  identity UPDATE and UDF emission at the SQL-generation sites for MERGE, with the
  const in the S2-2 module and the gate firing before the wrap on every path.
- The Q2 INSERT-arm shape deviates from the ruling's named site, recorded as-is: the
  ruling named `table_projection`, but threading a wrap flag through the projection
  plus a dual-SQL split costs +14 lines in `merge/mod.rs`, which sits at its exact
  size baseline (1622), and the tree law needs owner approval for growth. Instead
  `insert_stream_checked` gates the raw INSERT SQL (unchanged text, byte-identical
  plans for tables without NTZ columns) and only streams through a converting
  subquery that wraps NTZ target columns — the same const, the same gate-before-wrap
  order, zero lines in `merge/mod.rs`, zero test-signature churn. M5 splits into M5a
  (VALUES/SELECT) and M5b (UPDATE/MERGE) per the ruling; all three mutations ran red
  and restored green.
- S2-2 as ruled: new `write/ntz_store.rs` (gate in the void shape, the one home of
  `NTZ_WALL_CAST_UDF_NAME` — pinned equal to the registered UDF — and its renderer),
  called beside every `refuse_void_writes`; `spark_update_type_name` answers
  `TIMESTAMP_NTZ` for naive timestamps. MERGE matched-UPDATE validation only runs
  when files are affected, so the refusal pins seed a matching row (pre-existing
  executor behavior, not new).
- Probe re-runs after Slice 2: probe6 repark `sel` byte-EQUAL to `ntz6-spark.json`
  (all four 08:00 walls, append ok); probe2 32/53 byte-equal with every remaining
  DIFF classified — DDL/DML ack shapes and the R2 naming class (pre-existing), the
  S1 `lit_bad`/`cast_int`/`cast_ntz_bigint` framings (unchanged), the S2
  `ins_ansi_int` refusal template-equal — except `dfapi.filter_lit_naive_dt`,
  recorded as R-NTZ-S2-3. Scoreboard replay holds: `TY-TIMESTAMP-NTZ`,
  `TY-TIMESTAMP-NTZ-V3`, `TY-TIMESTAMP`, `TY-TIMESTAMP-LTZ` ok,
  `TY-PROMOTE-DATE-TS` errors as recorded. Done-gate deviation, recorded as-is: the
  order names `test_update_cast_parity.py` and `test_merge_parity.py`, which do not
  exist in the tree; the neighbouring `test_insert_store_assign.py`,
  `test_merge_store_assign.py`, `test_merge_into.py` and `test_ice_write_df_2_doors.py`
  ran instead (644 passed with the named eight).

## Fold VN (2026-09-28)

Branch `feat/ntz-1-s2`, model Muse Spark (`muse-spark-1.3-contributor`). One
commit; C-008 and C-009 stay OPEN. Folds the Opus verifier's VN-1..VN-7
(`verify-ntz-opus-handback.json`, NEEDS_REMEDIATION) without rewriting history.

- Step 0 measured first: `ntz-1-probes/ntz7_verify_probe.py` (New York
  session) ran on Spark 4.1.2 and on head `5d94594b`
  (`target/ntz-verify/ntz7-spark.json`, `ntz7-repark.json`). Every verifier
  premise held. VN-2: Spark answers `1704160800` / `2024-03-31 01:30:00` /
  `timestamp` for the `from_utc_timestamp` cells and `1704096000` /
  `2024-03-30 23:30:00` / `timestamp` for the `to_utc_timestamp` cells, where
  head answered `1704142800` / `2024-03-31 00:30:00` / `timestamp_ntz` and
  `1704078000` / `2024-03-31 00:30:00` / `timestamp_ntz`. VN-3: Spark's
  UPDATE refusal names `"TIMESTAMP"` → `"INT"`; head named `"TIMESTAMP_NTZ"`.
  VN-4/VN-6: Spark stores the session wall (`07:00`) and refuses STRING with
  `CANNOT_SAFELY_CAST`; head stored `12:00` and refused with the WI-2 text.
- VN-1: `AnsiDialect::on_session_built` registers
  `timestamp_ntz_cast_udf(false)`, so the UPDATE/MERGE wall-cast wrap resolves
  on the ANSI door. Pins `ansi_ntz_wall_cast.rs`
  (`ansi_update_into_a_naive_timestamp_column_stores_the_wall`,
  `ansi_merge_into_a_naive_timestamp_column_stores_the_walls`): UPDATE and
  MERGE into a naive `TIMESTAMP(6)` column store the walls and the column
  stays naive; both red without the registration (`UNRESOLVED_ROUTINE`).
- VN-2: the S2-1 retarget stays but fires only inside a DML plan
  (`plan_contains_dml` in `instant_ts.rs`, one predicate plus a threaded
  flag). Full removal was measured and rejected: `INSERT … SELECT *` and
  UNION sources store the correct session wall on head through the arm, and
  an AST-level wrap cannot see `*` positions or union branches, so removal
  would regress them; the DML gate preserves every store shape (VALUES,
  SELECT, REPLACE WHERE, the append, UPDATE/MERGE, overwrite/by-name) while
  plain SELECT statements revert to the base behavior. The seven cells pin to Spark's
  recorded answers through the facade (`test_ntz_7_verify.py` +
  `ntz_7_verify_spark_oracle.json`, rows and dtypes); all seven were red on
  head per the step-0 probe. Zero existing pins changed answer (740 pytest
  across the NTZ/TZ/grammar suites, 7/7 `ntz_store`, full lib sweeps green).
- VN-3: `spark_update_type_name` answers `TIMESTAMP_NTZ` only for
  microsecond-naive timestamps; the nanosecond zoneless form DataFusion uses
  for an unlocalized `TIMESTAMP'…'` literal names `TIMESTAMP` again on both
  doors (they share `incompatible_update_message`). Pin
  `update_refusal_names_a_timestamp_literal_source_as_timestamp` asserts
  Spark's `"TIMESTAMP"` → `"INT"` text; red without the fix.
- VN-4/VN-6: residues R-NTZ-S2-4…R-NTZ-S2-7 above, each with Spark's measured
  answer and RePark's; C-006/C-007 narrowed to the proved doors. VN-5:
  residue R-NTZ-S2-8 with the verifier's numbers. VN-7 (Slice 3 test hygiene:
  the fixed-path fixture lock, derived months/years bounds, pruning legs
  asserting rows only, unrecorded UPDATE/MERGE refusal table names): Slice 3
  follow-up, no edit here.
- No existing test was rewritten in this fold.

## Fold VN part 2 (2026-09-28)

Branch `feat/ntz-1-s2`, model Muse Spark (`muse-spark-1.3-contributor`). One
commit; C-008 and C-009 stay OPEN. Scopes the part-1 DML gate to the store
projection after the orchestrator ruled Q1 (land the structural fix; the
~50-line bound predates the measured shapes).

- Step 0 measured first: `ntz-1-probes/ntz8_verify_probe.py` (New York
  session) ran on Spark 4.1.2 and on head `0a61bc02`
  (`target/ntz-verify/ntz8-spark.json`, `ntz8-repark.json`). The 6 brief
  cells (from_utc/to_utc over fixed-offset strings on INSERT, UPDATE, MERGE,
  the NTZ store, and the CAST control) were already EQUAL at `0a61bc02`:
  wall-then-localize coincides with localize-then-shift when the zone offset
  is fixed. The 4 added DST cells (London, 2024-03-31) diverged on INSERT:
  Spark stores `1711863000`/`1711855800`, head stored `1711859400` for both
  (off by the DST hour); DST UPDATE/MERGE were already correct through the
  wrap path. No HALT on the premise: Spark's answers match the verifier's.
- Fix: `retarget_dml_store_casts` in `timestamp_ntz_cast.rs`, called from
  `instant_ts::analyze` in LTZ mode before the generic traversal; the generic
  arm is gone. It retargets only whole-expression casts (through Alias and
  the nullability wrapper) of the Projection directly under each Dml node
  and of VALUES rows, and wraps bare expressions at positional
  TIMESTAMP_NTZ targets (arity-guarded) so naive-at-plan-time function
  sources such as from_utc_timestamp still convert — without the wrap the
  missing store cast fails at the writer with the Arrow UTC-to-naive error.
  Cost: +89/−3 in `timestamp_ntz_cast.rs`, +7/−15 in `instant_ts.rs`
  (net +78; every piece forced by a pin or a measured failure). Both files
  stay under the 1000-line ceiling (451 and 977).
- Pins: `test_ntz_8_verify.py` + `ntz_8_verify_spark_oracle.json` replay the
  23-step recorded write sequence (10 reads assert rows and dtypes). Both
  DST INSERT reads were red at `0a61bc02` (test run plus probe); the other
  cells guard the mechanism. No existing test was rewritten in this fold.
- Preserved: all 26 ntz7 cells byte-identical (VN-2 EQUAL, residues
  R-NTZ-S2-4…R-NTZ-S2-8 untouched), SELECT * / UNION / REPLACE WHERE /
  reordered and partial column lists still store the session wall, 7/7
  `ntz_store`, full lib sweeps green, zero existing pins changed answer.

## Re-verify fold (2026-09-28)

Branch `feat/ntz-1-s2`, model Muse Spark (`muse-spark-1.3-contributor`). One
commit; C-008 and C-009 stay OPEN. Folds the Opus re-verify
(`reverify-ntz-opus-handback.json`): no S1, one S2 regression from part 2.

- Step 0 measured first: `ntz-1-probes/ntz9_verify_probe.py` (New York
  session) ran on Spark 4.1.2 and on head `bf534023`
  (`target/ntz-verify/ntz9-spark.json`, `ntz9-repark-prefix.json`). Spark
  stores the session wall for every VALUES cell: from_utc `21:00:00`,
  to_utc `03:00:00`, date_trunc `01:00:00`, DST rows
  `01:30/03:30/01:30/01:30`, column-list and mixed rows and the mixed
  NTZ+LTZ table likewise, `SELECT * FROM (VALUES …)` `21:00:00`; head
  errored on all 8 writes with the Arrow UTC-to-naive error. The mixed
  TIMESTAMP/TIMESTAMP_NTZ column stores `03:30:00` on Spark, `02:30:00`
  on RePark. The control SELECT read was EQUAL.
- RN-1 fix: the verifier's candidate (`retarget_rows` takes the VALUES
  schema and wraps cells at naive-microsecond fields) applied as-is, plus
  a small extension the probe forced: VALUES nested under pass-through
  SubqueryAlias/Projection chains retargets the same way, because the
  const-folded zone-shift call otherwise leaves a UTC literal in a naive
  VALUES schema and physical planning fails. RN-3: a Rust pin guards the
  bare-value wrap, and the wrap skips an already-wrapped expression.
- RN-2 and the CAST refusal text are residues R-NTZ-S2-9/R-NTZ-S2-10:
  both measured texts need names no change local to `update_cast.rs:121`
  can produce (the same naive-microsecond type must name `TIMESTAMP` for
  the zone-shift call and `TIMESTAMP_NTZ` for the cast; the CAST text
  comes from the `keyword_lower.rs` gate). The mixed widening cell is
  residue R-NTZ-S2-11. RN-4: the stale `plan_contains_dml` map note is
  gone. C-006's evidence names the part-2 pre-pass and this fold.
- Pins: `test_ntz_9_verify.py` + `ntz_9_verify_spark_oracle.json` replay
  the 39-step sequence (11 reads, 3 divergence steps); Rust `ntz_store`
  pins cover VALUES from_utc, the DST rows, the subquery nest, the
  nested-into-LTZ instant, and the RN-3 SELECT wrap, plus a wrap
  idempotence unit pin. Bite proofs: the 3 divergent Rust pins fail on
  the stashed implementation, the RN-3 pin fails with the wrap disabled,
  the facade writes errored pre-fix. No existing test was rewritten.
