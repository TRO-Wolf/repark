# Unit ledger — SUBSCRIPT-DOT-ACCESS-1 · a field access after a subscript, a call or a parenthesised expression answers as Spark does on the Spark door

**Date:** 2026-11-04 · **Base:** `3dc43ce1` (origin/main at the unit's start) · **Head:** `dd0d1c8e` (the builder's `f67bc205`, then the re-pin commit `dd0d1c8e`) · **Squash:** `3c9f300c` on main (PR #1259)
**Model:** builder `claude-opus-5-5` (both commits) · verifiers `claude-opus-5-5` and `muse-spark-1.3-contributor` (one round each) · records `claude-haiku-5-5`
**Policy:** [../../../AGENTS.md](../../../AGENTS.md) · **Path:** STANDARD.

**Status:** completed 2026-11-04 at `dd0d1c8e` (squashed to main as `3c9f300c`, PR #1259) after one verifier round each: the Opus 5.5 verdict PASS, and the Muse verdict PASS (94 neighbour probes on the base and head wheels). The records pass that writes this file closes the card SUBSCRIPT-DOT-ACCESS-1, files the four cards it names and records the leads. Clauses C-001 to C-006 are PROVEN; C-007 to C-019 are OPEN leads (L1 to L13).

**Why.** Nested rule doc D2 ([nested-rule-2026-11-03.md](../../../docs/corpus/nested-rule-2026-11-03.md)). On the Spark door a field access after a subscript, a call or a parenthesised expression refused with DataFusion's `Dot access not supported for non-string expr` where Spark 4.1.2 answers the field (`map('k', 1).k`, `element_at(m, 'k').x`, `(e).y`, `map_keys(mk)[0].a`). The gap was in the lowering, not the parser: sqlparser 0.62 already parses every chain as `Expr::CompoundFieldAccess`, and the DataFusion 54.1 lowering (`sql_compound_field_access_to_expr`) refuses `AccessExpr::Dot(Identifier)` after the first non-identifier access. Before the unit the D2 group matched Spark on 0 of 822 cells.

**Clause.** Spark's grammar (`SqlBaseParser.g4`, `#subscript` and `#dereference`: a subscript and a dereference over any `primaryExpression`) and Spark's `ExtractValue` dispatch (a struct field by name, a map value by key, an array element), on the cells the nested-cells table records as D2 ([nested-cells-2026-11-03.jsonl](../../../docs/corpus/nested-cells-2026-11-03.jsonl), 822 rows). The corpus-upstream checkout has no `sql/catalyst` expression sources, so `ExtractValue` is read from the grammar and the recorded oracle rows, not from its source. The pin binding is in [subscript-dot-access-1-pins.md](subscript-dot-access-1-pins.md).

Measured on Spark 4.1.2 (`q2.log`, 2026-11-04): `SELECT map('k', 1).k` = 1 (and `map('k', 1)['k']` = 1); `SELECT array(named_struct('a', 1), named_struct('a', 2)).a` = [1, 2]; `SELECT named_struct('a', 1).a` = 1.

## PROPOSITION LEDGER — SUBSCRIPT-DOT-ACCESS-1 — 2026-11-04

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | On the Spark door, a `.f` after a subscript, a call or a parenthesised root is respelled as a field-name access before planning (DataFusion's `get_field`), so each refused position answers the Spark field or refuses where Spark refuses; a leading column path (`t.m['k'].x`, `m.k.x`) keeps DataFusion's identifier resolution. | The rewrite's unit tests, the table replay with its skips counted by reason, and the red check with the call removed. | PROVEN | Site `crates/repark-core/src/column_resolution/dereference.rs` (`spell_dereferences_as_field_names`, 87 lines added), called first in `plan_statement_with_column_repair` (`crates/repark-core/src/column_resolution.rs`, +4), which the Spark door and its replans use. Pins: `dot_access_cells_answer_spark_on_the_spark_door` and `every_primary_takes_a_dereference_chain` in the Spark-door test file. The three inline unit tests in `dereference.rs` (`a_field_after_a_subscript_or_a_call_becomes_a_field_name`, `a_leading_column_path_stays_an_identifier_chain`, `explain_and_nested_queries_are_respelled`) are evidence here; the grammar gate binds test files and scripts only. Red: with the respelling call removed, all six new `subscript_dot_access` tests fail (builder; Opus and Muse each re-ran the removal). |
| C-002 | Each enumerated grammar neighbour answers Spark's value where Spark answers a value, or refuses where Spark refuses. The neighbours: quoted field names; a 0-based index after a subscript; `f(x).y`, `f(x)[0].y`, `(expr).y` and `(expr)[0]`; `named_struct(...).x`; `map(...)[k].x`; `array(...)[0].x`; `CAST(... AS STRUCT<...>).a`; CASE and nested subscripts; WHERE, GROUP BY, HAVING and ORDER BY chains; window frames and partitions; a lambda `e['k'].x`; a leading path. | The pinned neighbour rows, each Spark value recorded or derived from the grammar, and the verifiers' probes on both wheels. | PROVEN | Pins: `every_primary_takes_a_dereference_chain`, `card_statements_answer_spark_on_a_struct_keyed_map`, `chains_answer_in_where_group_by_and_order_by`, `leading_column_paths_keep_their_resolution`, `a_missing_field_or_a_non_struct_base_still_refuses`. Muse claim 2: 94 probes on the base and head wheels, 61 changed answers, each toward Spark or to a card-class refusal, 0 wrong values. Opus section 2: no wrong value where Spark answers a value; one answer class where Spark raises an error (duplicated or case-colliding struct field names) is lead L7 (C-013). Several neighbour values are derived from the grammar and the measured rows, not measured on a JVM; only the three forms under Why were measured. |
| C-003 | The table after the unit: the D2 group moves to 40 EQUAL, 152 to 153 D14 (the same values, other column names), 108 D15 (both refuse, other text), 100 ORC D3, 2 D12 and 6 to 7 other; the 413 native rows are unchanged; EQUAL stays 828 before and after, and every other group moves 0 cells. | The table comparison over the base and head wheels (the read, expression, merge and group scripts), and the replay test for the facade rows. | PROVEN | Pin: `dot_access_cells_answer_spark_on_the_spark_door` (275 of 408 facade-path-view rows, exact count asserted; skips by reason: native 408, ORC D3 100, Avro rows 26, Spark reader aborted D12 5, Spark collapse D12 2). The split: the builder's 40 / 152 / 108 / 100 / 2 / 7 and the Opus rerun's 40 / 153 / 108 / 100 / 2 / 6. The one-cell gap is an Avro `map_keys` order flip (lead L6, C-012), so the PR body's 152 and 7 are not exact. Other groups: 1152 of 1152 native A-read rows, 1488 of 1488 non-D2 A-read rows and 48 of 48 write fids unchanged (Opus and Muse). |
| C-004 | The re-pins: the FNP-GEN-1 critic test's four `from_csv` cells answer Spark 4.1.2's recorded rows, and the U9-TYPES-1 step `map/struct/field` asserts Spark's answer `[[0, 1], [1, null]]`; residue R-12 is dropped because RePark now answers that value. | The two tests run at the head; the expected rows are the recorded Spark rows. | PROVEN | Pins: `test_sql_door_from_csv_dot_access_cells` (the strict xfail is removed; the four cells are `CAST(from_csv('2024-01-01 10:00:00', 't TIMESTAMP').t AS BIGINT)`: [[1704103200]] twice in UTC and [[1704121200]] twice in the second zone) and `test_every_step_answers_as_spark_measured_or_as_its_dated_residue` (the step `SELECT id, c['k'].a FROM sc.ns.ms`). Muse: the re-pin selection exits 0. R-12 keeps its row text in the staging ledger, with a dated line. |
| C-005 | Boundary and gates: the diff is the 14 files of the claimed set (+493, -86), with no change to `.github`, Cargo, the native planning path or the fork; every gate exits 0 at the head, with the counts below. | The verifiers' runs at the head, and the boundary check on the file list. | PROVEN | none (records). Crate counts (builder; Opus section 3; Muse claim 3): repark-sql 660 passed, 4 ignored; repark-core 1601 passed, 2 ignored; repark-spark 3166 passed, 5 ignored; repark-functions 970 passed, 1 ignored; pytest `tests/sql` 1749 passed, 1 skipped; pytest `tests/iceberg` 2220 passed, 192 skipped, 12 xfailed. The full facade run (one process, `-n 8`) gave 23342 passed, 5 failed, 683 skipped, 150 xfailed; the 5 failures are the live-Spark io oracle tests, whose Spark worker lacked pandas because `PYSPARK_PYTHON` was unset, and with it set to the venv the affected files give 22 passed, 0 failed. `make rust-panic-ban` and `make rust-clippy` pass; `make check-map-size` reports 448 maps clean; five map baselines were lowered to their exact sizes and none raised: `crates/repark-core/src/column_resolution/map.md` (207, 15898), `crates/repark-core/src/map.md` (1571, 129022), `crates/repark-spark/src/tests/map.md` (2800, 237170), `python/repark/tests/functions/map.md` (145, 18709) and `python/repark/tests/timestamps/map.md` (221, 39701). |
| C-006 | Hygiene: no code comment in the added Rust, Python or JSON lines; each unit commit ends with one `Authored-By` trailer; no local path, session link, account id, bucket, region or ARN in the diff; the baselines are lowered, never raised. | Diff inspection by both verifiers and the builder's comment gate. | PROVEN | none (records). Builder: 0 added comment lines. Opus section 4: no comment in the added lines, no local path, session link, ARN or region string, and the trailer `Authored-By: Claude (claude-opus-5-5)` on each commit. Muse claim 4: the same, and no secret in the added lines. |
| C-007 | Lead L1 (the builder's question 1; the native door): the native door's planning (`PreExecute::plan`) is not wired. Wired, 52 native D2 rows would become EQUAL, but 105 native rows that refuse today would become wrong answers (`map_keys(mk)[0].x` and `ls[0].x` read the 1-based element, D5), and the native duplicate-field row would answer where Spark refuses (S2). The native door keeps refusing. | Closing question: fix the native one-based array subscript (D5) first, then wire the native planning path and pin its rows against Spark. | OPEN | Card [SUBSCRIPT-DOT-ACCESS-NATIVE-1](../../roadmap/mid-term/subscript-dot-access-native-1-card-2026-11-04.md). The native probe `SELECT map('a', named_struct('x', 1))['a'].x` still refuses with the dot text (Opus boundary check). Held by the orchestrator's ruling Q1 (b); see Scope notes. |
| C-008 | Lead L2 (the builder's question 4; D14): the respelled fields carry DataFusion-style output names (`t.m[k][x]`), where Spark writes `m[k1].x`, so 152 to 153 value-equal cells land in D14 and not in EQUAL. | Closing question: the output names of field results on the Spark door, or a D14 pin. | OPEN | No card by this pass. The nested rule doc's D14 row (card column `candidate`). |
| C-009 | Lead L3 (D15): the 108 refusals the unit makes match Spark where Spark refuses, but their texts lack Spark's error class (`Field zz not found in struct` for FIELD_NOT_FOUND; `... is not Struct, Map, or Null` for INVALID_EXTRACT_BASE_FIELD_TYPE). | Closing question: the Spark class and text for the two refusals, pinned on the Spark door. | OPEN | No card by this pass. The nested rule doc's D15 row (card column `candidate`). |
| C-010 | Lead L4 (Opus F4 and F5; Muse C1 and C2): three forms the rewrite does not reach. `array(named_struct('a', 1), named_struct('a', 2)).a` refuses `type List(Struct(...)) is not Struct, Map, or Null` where Spark answers [1, 2] (measured). `struct(1, 2).col1` refuses `Field col1 not found in struct` (the `struct()` fields are named c0 and c1, D13) where Spark answers 1 (reasoned from Spark's naming, not measured). `transform(map_values(m), e -> e.arr[0].g)` refuses UNRESOLVED_COLUMN on the lambda variable `e`, where Spark's answer [[6], [8]] is expected, not measured. | Closing question: the three forms, each measured on the Spark oracle, then pinned. | OPEN | Card [DEREFERENCE-FORMS-2](../../roadmap/mid-term/dereference-forms-2-card-2026-11-04.md). The table's B-expr row `arr_struct_field_all` stays refused. |
| C-011 | Lead L5 (Opus question 3, the null half): RePark's Avro round trip reads a null map or list column back as empty: `map_keys` answers `[]`, `size` answers 0, and `IS NULL` keeps no row. | Closing question: the null-collection round trip through the writer and the reader, then a pin of the Avro rows the replay skips (26 rows, for this cause and the order cause of L6) against Spark's values. | OPEN | Card [AVRO-NULL-COLLECTION-ROUNDTRIP-1](../../roadmap/mid-term/avro-null-collection-roundtrip-1-card-2026-11-04.md). Severity S2. |
| C-012 | Lead L6 (Opus F6; Opus question 3, the order half): RePark reads Spark-written Avro map entries in another order (`map_keys(m)` gives [k2, k1] where Spark has [k1, k2]), and the order changes from run to run on the same files. The element values agree with Spark. | Closing question: the entry order on read against Spark's writer order, then one pin on a fixed fixture. | OPEN | Card [AVRO-MAP-ENTRY-ORDER-1](../../roadmap/mid-term/avro-map-entry-order-1-card-2026-11-04.md). The replay skips the Avro rows, so the pin is not flaky; the table split moves by one cell between runs (C-003). Severity S3 (the verifier graded it S4 as a claim-precision item). |
| C-013 | Lead L7 (Opus F1, S2; Opus F2, S3; class D13): a field access after a subscript or a call reaches DataFusion's exact-name `get_field`. So (F1) a struct with a duplicated field name, or two names that differ only in case, answers where Spark raises AMBIGUOUS_REFERENCE_TO_FIELDS (`map('k', named_struct('a', 1, 'a', 2))['k'].a` answers 1; `map('k', named_struct('a', 1, 'A', 2))['k'].A` answers 2), and (F2) a differently cased name refuses `Field UP not found in struct` where Spark's case-insensitive lookup answers (`m['k'].UP` on a field `Up`, and `m['k'].F.G` on `f`; Spark answers 3 and 5). No wrong value where Spark answers a value. The bracket form and the leading path already behave so on main. | Closing question: refuse a case-insensitive collision after a subscript as Spark does, and match the case-insensitive field after a subscript. | OPEN | Recorded as dated lines under D13 of the nested rule doc (that row's card column is `candidate`; no card file by this pass). |
| C-014 | Lead L8 (Opus F3, S3): the DataFrame string-expression door (`df.filter("ls[0].a > 1")`) does not plan through the respelling and refuses `Dot access not supported for non-string expr`, where Spark answers [[2]]; `selectExpr` with the same chain answers. | Closing question: wire the respelling into the string-expression path. | OPEN | No card by this pass. |
| C-015 | Lead L9 (Muse C3, S3): a DELETE or UPDATE whose WHERE holds a chain (`DELETE FROM t WHERE m['k'].x = 5`) refuses with DataFusion's internal name `No field named __datafusion_extracted_1`, where the base refused with the dot text and Spark executes the statement. No partial write (the verifier checked the rows); plain and bare-subscript DML work on both wheels. | Closing question: the DML planner's handling of the respelled `get_field` projection. | OPEN | No card by this pass. |
| C-016 | Lead L10 (Muse out-of-scope observation; pre-existing): the MERGE planner does not take the respelling, so a MERGE with a chain refuses the dot text at the base and at the head. | Closing question: the MERGE planning path. | OPEN | No card by this pass. |
| C-017 | Lead L11 (Muse; pre-existing): LATERAL VIEW (explode, inline) is not implemented on the facade door, so a chain inside a lateral view cannot be tested there. | Closing question: LATERAL VIEW on the facade door. | OPEN | No card by this pass. |
| C-018 | Lead L12 (builder; D10): a negative or out-of-range array index after a subscript (`ls[-1].b`, `ls[5].b`) answers null where Spark ANSI raises INVALID_ARRAY_INDEX; the bare `ls[-1]` already does the same. | Closing question: D10's error on both forms. | OPEN | The nested rule doc's D10 row (card column `candidate`). |
| C-019 | Lead L13 (Opus F7, S4, AT-10): the `DfStatement::CopyTo` arm of `spell_dereferences_as_field_names` has no pin, and a local `COPY ... TO` refuses before planning on the Spark door, so no Spark-door input is known to change through that arm. | Closing question: a pin for the arm, or its removal. | OPEN | No card by this pass. |

## Evidence

**Verifier verdicts.** Opus 5.5 (`claude-opus-5-5`), one round: VERDICT PASS, with findings F1 to F7 and no re-check needed. Muse (`muse-spark-1.3-contributor`), one round: CONCLUDED, PASS, with claims 1 to 4 PROVEN, five cards (C1 to C5) and six out-of-scope observations. Both reports are untracked; their verdict lines are transcribed here.

**Builder.** Two commits: `f67bc205` (the Spark-door fix, the replay, the neighbour pins) and `dd0d1c8e` (the re-pins, which both verifiers checked at the head). The builder's hand-back put the native door, the 1-based subscript and the full-suite storm to the orchestrator; the rulings are under Scope notes.

**Mapping of the leads.** L1 = C-007 (builder Q1). L2 = C-008 (builder Q4; Muse C5). L3 = C-009 (the D15 class; Muse C4). L4 = C-010 (Opus F4 and F5; Muse C1 and C2). L5 = C-011 (Opus Q3, the null half). L6 = C-012 (Opus F6 and Q3, the order half). L7 = C-013 (Opus F1 and F2). L8 = C-014 (Opus F3). L9 = C-015 (Muse C3). L10 = C-016 and L11 = C-017 (Muse out-of-scope observations). L12 = C-018 (the builder's D10 residue). L13 = C-019 (Opus F7).

**Builder question 2 (design).** The measurement settled it: `map('k', 1).k` answers 1 on Spark 4.1.2, not a refusal or a struct read. The Spark door answers the same value, as the existing column path `m.k` does.

## Review findings

```yaml
FINDING:
  id: OPUS-F1
  severity: S2
  category: AT-3
  clause: C-013
  claim: A duplicated or case-colliding struct field name after a subscript answers where Spark raises AMBIGUOUS_REFERENCE_TO_FIELDS. The respelling reaches DataFusion's exact-name get_field; the bracket form and the leading path already do this on main.
  evidence: Opus verifier section 2 (F1), probes exact_dup_sub, case_dup_sub, case_dup_call and case_dup_leading_struct; the nested-cells table row ns_dup_field.
  disposition: OPEN (lead L7, C-013; dated line under D13 of the nested rule doc; no card file by this pass)
```

```yaml
FINDING:
  id: OPUS-F2
  severity: S3
  category: AT-8
  clause: C-013
  claim: A case-different field name after a subscript refuses where Spark's case-insensitive lookup answers (m['k'].UP on Up; m['k'].F.G on f). The same D13 behaviour shows on main's leading path.
  evidence: Opus verifier section 2 (F2), probe case_insensitive_field; the table row ns_case.
  disposition: OPEN (lead L7, C-013)
```

```yaml
FINDING:
  id: OPUS-F3
  severity: S3
  category: AT-8
  clause: C-014
  claim: The DataFrame string-expression door does not plan through the respelling; df.filter("ls[0].a > 1") refuses where Spark answers [[2]].
  evidence: Opus verifier section 2 (F3), probe df_expr_filter; selectExpr answers the same chain.
  disposition: OPEN (lead L8, C-014; no card by this pass)
```

```yaml
FINDING:
  id: OPUS-F4
  severity: S3
  category: AT-8
  clause: C-010
  claim: array(named_struct('a', 1), named_struct('a', 2)).a refuses where Spark answers [1, 2]; table row B-expr arr_struct_field_all.
  evidence: Opus verifier section 2 (F4); orc/q2.log (Spark answers [1, 2]).
  disposition: OPEN (lead L4, C-010; card DEREFERENCE-FORMS-2)
```

```yaml
FINDING:
  id: OPUS-F5
  severity: S3
  category: AT-8
  clause: C-010
  claim: A field on a lambda variable (transform(map_values(m), e -> e.arr[0].g)) refuses UNRESOLVED_COLUMN; the rewrite site does not see a CompoundIdentifier on a lambda variable.
  evidence: Opus verifier section 2 (F5), probe lambda; the verifier's Spark expectation [[6], [8]] is not measured.
  disposition: OPEN (lead L4, C-010; card DEREFERENCE-FORMS-2)
```

```yaml
FINDING:
  id: OPUS-F6
  severity: S3
  category: AT-6
  clause: C-012
  claim: RePark's Avro map_keys entry order on the Spark-written fixtures changes from run to run; the table split moves by one cell (152 and 7 in the PR body, 153 and 6 on the rerun).
  evidence: Opus verifier section 2 (F6); the two harness runs compared on the head cells file.
  disposition: OPEN (lead L6, C-012; card AVRO-MAP-ENTRY-ORDER-1; the PR numbers in C-003)
```

```yaml
FINDING:
  id: OPUS-F7
  severity: S3
  category: AT-10
  clause: C-019
  claim: The DfStatement::CopyTo arm of the respelling has no pin; no Spark-door input reaches it, because a local COPY refuses before planning. The verifier graded it S4; the records clerk files it at the grammar's S3 ceiling for a finding with no known wrong value.
  evidence: Opus verifier section 2 (F7) and its COPY probe.
  disposition: OPEN (lead L13, C-019; no card by this pass)
```

```yaml
FINDING:
  id: MUSE-C1
  severity: S3
  category: AT-8
  clause: C-010
  claim: SELECT array(named_struct('a', 1), named_struct('a', 2)).a refuses at execution with 'not Struct, Map, or Null'; Spark answers [[1, 2]].
  evidence: Muse hand-back card C1 (measured in orc/q2.log).
  disposition: OPEN (lead L4, C-010)
```

```yaml
FINDING:
  id: MUSE-C2
  severity: S3
  category: AT-8
  clause: C-010
  claim: SELECT struct(1, 2).col1 refuses 'Field col1 not found in struct'; struct() names its fields c0 and c1 (D13). Spark answers 1 (reasoned, not measured).
  evidence: Muse hand-back card C2.
  disposition: OPEN (lead L4, C-010)
```

```yaml
FINDING:
  id: MUSE-C3
  severity: S3
  category: AT-3
  clause: C-015
  claim: A DELETE or UPDATE with a chain in WHERE refuses with the internal name __datafusion_extracted_1, where Spark executes the statement; no partial write.
  evidence: Muse hand-back card C3 (rows p15 and p17 intact at the head).
  disposition: OPEN (lead L9, C-015; no card by this pass)
```

```yaml
FINDING:
  id: MUSE-C4
  severity: S3
  category: AT-8
  clause: C-009
  claim: The refusal texts for an invalid struct base and a missing field differ from Spark's error classes (INVALID_EXTRACT_BASE_FIELD_TYPE, FIELD_NOT_FOUND).
  evidence: Muse hand-back card C4 (the rows n01, n04, n16, n17 and n18).
  disposition: OPEN (lead L3, C-009; the nested rule doc's D15 row)
```

```yaml
FINDING:
  id: MUSE-C5
  severity: S3
  category: AT-8
  clause: C-008
  claim: The respelled fields carry DataFusion-style output names (t.m[k][x]) where Spark writes m[k1].x; 148 A-read and 5 B-expr cells are value-equal with other names.
  evidence: Muse hand-back card C5.
  disposition: OPEN (lead L2, C-008; the nested rule doc's D14 row)
```

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: subscript-dot-access-1
  complete: true
  reattested: [AT-1, AT-3, AT-6, AT-8, AT-10]
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Both verifiers walked the nested-cells table clause by clause. The Opus round walked 414 facade unit cells (408 A-read and 6 B-expr), 418 native unit cells and 2141 control cells against the grammar alternatives and the measured rows. The Muse round compared the whole table between the base and head wheels. ExtractValue's source is absent from the corpus-upstream checkout, so expectations come from the grammar, the measured rows and q2.log.
      artifacts: [crates/repark-core/src/column_resolution/dereference.rs, crates/repark-core/src/column_resolution.rs, crates/repark-spark/src/tests/subscript_dot_access.rs]
    - id: AT-2
      status: N/A
      justification: N/A — untouched by origin/main..dd0d1c8e, as the verifiers classified it. The neighbour probes did include negative and out-of-range indices and NULL maps and arrays; they are recorded under AT-1 and AT-3, and the classification stands unless the owner rules otherwise.
    - id: AT-3
      status: ATTACKED
      evidence: Missing field, non-struct base, NULL map or array, duplicated or case-colliding field (F1, L7) and case mismatch (F2, L7). Refusals are never silent. The DML chains refuse without a partial write (Muse C3, L9).
      artifacts: [crates/repark-spark/src/tests/subscript_dot_access.rs]
    - id: AT-4
      status: N/A
      justification: N/A — untouched by origin/main..dd0d1c8e; no concurrency, ordering or shared-state change in the diff. The full-suite OCC failure is OCC-DISJOINT-MERGE-FLAKE-1 (Scope notes).
    - id: AT-5
      status: N/A
      justification: N/A — untouched by origin/main..dd0d1c8e; no authentication, input-injection or secrets surface in the diff (the hygiene check is C-006).
    - id: AT-6
      status: ATTACKED
      evidence: CTAS, INSERT ... SELECT and temp-view round trips with chains. The view body is stored from the raw SQL slice, not from the respelled AST. Write statements cannot reach the rewrite: 173 A-ddl and 384 A-write statements are unchanged, and 48 of 48 write fids are identical. ORC and Avro exclusions were verified row by row. The Avro map-order flip (L6) is an order inconsistency, not a wrong value.
      artifacts: [crates/repark-core/src/column_resolution/dereference.rs, docs/corpus/nested-cells-2026-11-03.jsonl]
    - id: AT-7
      status: N/A
      justification: N/A — untouched by origin/main..dd0d1c8e; the rewrite is a single pass over the statement's expressions, with no unbounded growth or hot loop in the diff.
    - id: AT-8
      status: ATTACKED
      evidence: The DataFusion 54.1 planner contract was read. The respelling covers exactly the refusing positions, and a leading Dot(Identifier) run is left to compound resolution. The table's D15 rows differ from Spark's error classes (L3, C-009), and the native door is not wired (L1, C-007).
      artifacts: [crates/repark-core/src/column_resolution/dereference.rs, crates/repark-core/src/column_resolution.rs]
    - id: AT-9
      status: N/A
      justification: N/A — untouched by origin/main..dd0d1c8e; no logging, metrics or alarm surface in the diff.
    - id: AT-10
      status: ATTACKED
      evidence: With the respelling call removed, all six new subscript_dot_access tests fail (Opus; Muse exit 101). The replay asserts the pinned count (275) and the skip counts by reason. The gap named: the CopyTo arm has no pin (L13, C-019).
      artifacts: [crates/repark-spark/src/tests/subscript_dot_access.rs, crates/repark-core/src/column_resolution/dereference.rs]
```

## Scope notes

- The native door is held. The orchestrator's ruling Q1 (b) keeps it refusing; the owner's 2026-11-03 exclusion of native-door read semantics (the 1-based array subscript and `element_at` over a map answering a list) is recorded on the v2 label draft, family 3. L1 carries the measured reasons.
- The OCC failure in the full-suite run (2 of 8 parametrisations of the disjoint-MERGE test) is OCC-DISJOINT-MERGE-FLAKE-1 (card [occ-disjoint-merge-flake-1-card-2026-10-14.md](../../roadmap/mid-term/occ-disjoint-merge-flake-1-card-2026-10-14.md), first seen on CI 2026-10-07 on the DataFrame door), not this change. The respelling rewrites only a `.f` after a subscript, call or parenthesised root, and the eight MERGE statements and their setup have none, so each is unchanged. On the head, 6 of 6 repeats of all 8 parametrisations passed (48 of 48).
- The Muse harness's write input predates MAP-KEY-ALTER-1; the Opus rerun of the write side at the head differs only on mapkey ALTER rows with no access chain. No lead.
- A deep-expression pytest group took 27 minutes serially on the debug build, and the verifier's timeout cut three files; they pass without the timeout. A timing artifact, not a finding.
- Verifier environment: no JVM was used. The Spark column of the recorded table is the oracle, and the facade rerun needed `PYSPARK_PYTHON` set to the venv for the io oracle tests.
