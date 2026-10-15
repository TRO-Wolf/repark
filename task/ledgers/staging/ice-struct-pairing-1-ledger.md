# Unit ledger — ICE-STRUCT-PAIRING-1 · one rule pairs struct fields on every write door

**Date:** 2026-10-14 · **Branch:** `fix/ice-struct-pairing-1` · **Base:** `018d0efd`, stacked on
U-3 at `fb325e57` · **Model:** Claude (claude-opus-5-5), lane B; build rounds 1, 2 and 3 and the exception round ·
**Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** Target: the next bug release (1.5.6).

**Order:** [../../wo/ice-struct-pairing-1/order.md](../../wo/ice-struct-pairing-1/order.md)
(U-6, R-016) and its sketch, from registry row ICE-TSNS-SQL-1-R-016 (audit item PA-07), clause
C-037 and question Q8 of `ice-tsns-merge-wall-1-ledger.md`. **This ledger closes when the
unit's pull request merges.** Step 0 changed no product file
([step-0.md](../../wo/ice-struct-pairing-1/step-0.md)). Build round 1 halted on two rules of the
order's §6 (list L2 moved; the ANSI door's struct MERGE refused where main stored); the owner
answered questions 47 and 48 the same day and build round 2 finished the order. Verifier 1
returned FAIL on `e3fc32ab`; the owner's rulings 54 to 59 of the same night scoped build round 3
(§7). Round count for the class "a struct field paired by a rule other than Spark's": build
round 3, and there is no round 4. The owner's ruling 73 allowed one exception round after verifier 2's
FAIL (2026-10-15), widened by ruling 90 by one named change outside the class (§8); there is no
further round.

**Why.** Which source field fed which field of a struct column depended on the cast that
happened to run on a door's route, so RePark differed from Spark 4.1.2 in 279 of 512 measured
cells on each format version and stored a NULL nobody wrote in 48 of them. One function,
`pair_store_value`, now decides every pairing from the door's mode and the two types, and one
kernel applies it.

## PROPOSITION LEDGER — ICE-STRUCT-PAIRING-1 — 2026-10-14

The rule of the sketch's §4 is clauses C-001 to C-009, one per sentence; C-010 to C-015 are how
the doors reach it; C-016 to C-020 are the controls and the residues; C-021 to C-023 are the
proof and the records. Build round 3 adds C-024 to C-032, one per sentence of the owner's rulings
55(a) to 58(a). The exception round (2026-10-15, rulings 73, 74, 76 and 90) adds C-033 to C-037.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Two structs are paired by the door's mode; by position the i-th source field feeds the i-th target field. | The seam's table test and the fixture's by-position doors. | **PROVEN** | `struct_pairing/tests.rs` `the_rule_pairs_every_mode_shape_and_layout_and_keeps_every_null` (3 modes × 8 shapes × 10 layouts); `test_ice_struct_pairing_1.py::test_the_engine_answers_the_rule` on `insert`, `insert_values`, `insert_overwrite`, `replace_where`, `df_insert_into`, `df_insert_into_overwrite`; mutant 1 dies on both. pins: ice-struct-pairing-1/C-001 |
| C-002 | By position, a longer source refuses `EXTRA_STRUCT_FIELDS` and a shorter one `STRUCT_MISSING_FIELDS`. | The table test and the `extra` / `missing` cells of the by-position doors. | **PROVEN** | The same two tests; the Rust door's `every_door_pairs_struct_fields_by_spark_rule`; mutants 3 and 4 die on `insert/missing` and `insert/extra`. pins: ice-struct-pairing-1/C-002 |
| C-003 | By name, each target field takes the one source field of its name under `spark.sql.caseSensitive`. | The table test in both case modes; the fixture's `cs` group. | **PROVEN** | 80 `caseSensitive` cells green; mutant 2 (by name ignores the case rule) dies on the table test. pins: ice-struct-pairing-1/C-003 |
| C-004 | By name, a target field with no source field refuses `CANNOT_FIND_DATA` at its dotted path, and one with two refuses `AMBIGUOUS_COLUMN_NAME`. | The `renamed`, `renamed_first`, `missing` cells; the added cell `by_name_case_twins`. | **PROVEN** | `test_the_engine_answers_the_rule`; `test_the_engine_answers_the_rule_in_every_added_cell[by_name_case_twins]` answers Spark's `AMBIGUOUS_COLUMN_NAME` `` `st`.`a` `` (R-021 closes). pins: ice-struct-pairing-1/C-004 |
| C-005 | By name, a source field left over refuses `EXTRA_STRUCT_FIELDS`, under `merge-schema` too. | The `extra` cells; the added cell `merge_schema_extra`. | **PROVEN** | `test_the_engine_answers_the_rule_in_every_added_cell[merge_schema_extra]` answers Spark's `EXTRA_STRUCT_FIELDS` `c` to `st` (R-022 closes); mutant 4 dies. pins: ice-struct-pairing-1/C-005 |
| C-006 | A list pairs its element (path `element`), a map its key and value (`key`, `value`), with the same mode at every depth. | The table test's list, list-view, fixed-size-list and map layouts; the fixture's `nested`, `array` and `map` containers. | **PROVEN** | Mutants 5a (the walk stops at a list), 5b (at a map value), 5c (at a map key) die on the table test and on the store-assign and update-cast tests. pins: ice-struct-pairing-1/C-006 |
| C-007 | A dictionary or run-end-encoded value is paired as its values; anything else is a leaf, not looked at and not converted. | The table test's `Dictionary` and `RunEndEncoded` layouts; `the_leaf_pairs_name_their_paths_and_are_never_converted`. | **PROVEN** | Mutant 5d (the walk stops at a dictionary) dies on the table test. pins: ice-struct-pairing-1/C-007 |
| C-008 | The refusal texts are Spark's, with the table label the door prints: the INSERT doors and the DataFrame writers name the table, MERGE and `UPDATE` print an empty pair of backticks. | The fixture compares the full refusal text with the label the rule derives. | **PROVEN** | `test_the_engine_answers_the_rule` (`answers` holds the Spark text inside the engine's), and the Rust door test's `UNLABELLED` doors. pins: ice-struct-pairing-1/C-008 |
| C-009 | No arm fills a NULL and no arm drops a field. | Mutants that add each arm. | **PROVEN** | Mutant 3 (a missing field filled with NULL) dies on 25 Rust door cells and four `repark-iceberg` tests; mutant 4 (a surplus field dropped) on 15 Rust door cells and the table test. pins: ice-struct-pairing-1/C-009 |
| C-010 | Pairing returns the target's names in the target's order with the source's leaf types and every validity kept, and a paired value is the identity; an identity pairing adds nothing to the plan. | The table test's null and identity rows; the `EXPLAIN` byte-compare of H6. | **PROVEN** | `the_rule_pairs_every_mode_shape_and_layout_and_keeps_every_null`; mutant 9 (`is_identity` true for a reordered struct) dies on 14 Rust door cells; 20 `EXPLAIN` answers (plain and `VERBOSE`, ten writes with no struct or a struct in the table's order) are byte-identical on head and main, sha256 `9c2f0b9e…`. pins: ice-struct-pairing-1/C-010 |
| C-011 | The door states the mode: by position on `INSERT … VALUES` / `SELECT`, `INSERT OVERWRITE`, `REPLACE WHERE` and `insertInto` (with and without overwrite); by name on `INSERT … BY NAME`, every MERGE arm, `UPDATE`, `writeTo().append()`, `overwritePartitions()`, `saveAsTable` append and `overwrite(condition)`. | One door at a time handed the other mode. | **PROVEN** | §3: every one of the sixteen doors and `overwrite(condition)` dies under its own mutant, at the Rust door or in the facade fixture. pins: ice-struct-pairing-1/C-011 |
| C-012 | With no mode carried by the statement, `WriteOp::Insert` pairs by position and `WriteOp::Update` by name. | An analyzer-level test on a plain context. | **PROVEN** | `store_overflow/tests.rs` `an_insert_pairs_struct_fields_by_position_unless_its_statement_says_by_name`; mutant 8 (the default by name), which survived every other test, dies on it. pins: ice-struct-pairing-1/C-012 |
| C-013 | The three batch casts refuse a struct that reaches them unpaired, with hint H-1: S1 and S2 with the batch hint, S3 (a static `PARTITION` value) with the partition hint; the table keeps its rows. | One pin per cast. | **PROVEN** | `the_append_batch_cast_s1_refuses_…`, `the_positional_overwrite_cast_s2_refuses_…`, `struct_pairing_doors.rs` `the_partition_overwrite_route_refuses_…`; mutants 6a, 6b, 6c die on them. pins: ice-struct-pairing-1/C-013 |
| C-014 | One table, `BY_NAME_STORE_KERNELS`, holds every by-name store kernel and one function registers it; both doors call it at session build, and every name of the table and of `NS_WALL_KERNEL_NAMES` resolves on each door. | One session-build test per door reading both constants. | **PROVEN** | `repark-spark/tests/store_kernel_registry.rs`, `repark-sql/tests/ansi_store_kernel_registry.rs`; removing the ANSI door's call turns its test red on `__repark_store_int8__`. The ANSI door's MERGE `MATCHED UPDATE` and `NOT MATCHED INSERT` of a struct store `{a: 3, b: 4}` as main does. pins: ice-struct-pairing-1/C-014 |
| C-015 | A refusal raised inside `StoreOverflowCast` reaches Python with Spark's condition on its head. | The `overwrite(condition)` refusal cells. | **PROVEN** | `test_ice_write_df_2_overwrite.py::test_overwrite_condition_residues[oc_nested_*]` (five cells) read condition `None` before and Spark's condition after `spark_ast.rs` peels the rule's own context. pins: ice-struct-pairing-1/C-015 |
| C-016 | List L1 does not move: 28 cells per format version refuse before any store and write no file (registry row R-018). | `test_l1_refuses_and_writes_no_file`. | **PROVEN** | 56 tests green on main and on the head. pins: ice-struct-pairing-1/C-016 |
| C-017 | List L2 (`UPDATE` with no `WHERE`): the 40 cells Spark refuses give Spark's refusal, the 32 Spark stores answer as main, and four (v2 and v3, array, `case` and `case_reordered`) refuse with the raw `arguments need to have the same data type` (registry row R-014). | The three L2 tests (owner question 48). | **PROVEN** | `test_l2_refuses_as_spark_where_spark_refuses` (40), `test_l2_answers_as_main` (32), `test_l2_case_cells_keep_an_internal_refusal` (4). pins: ice-struct-pairing-1/C-017 |
| C-018 | The cells where main already answered as Spark answer as before. | `test_the_engine_answers_the_rule` over every ruled cell. | **PROVEN** | 2,458 fixture tests green on the head; on main 548 red, every one a cell the rule moves. pins: ice-struct-pairing-1/C-018 |
| C-019 | The dated residues that held main's struct pairing move to Spark's recorded answers: u8-write-sql R-18, u9-types-1 R-36, u7-write-df-2 R-15 and R-16. | Each test against its recording. | **PROVEN** | u8 nested oracle (2026-09-25), u9 oracle r8 (2026-09-26, `measure_r8.py`), u7 `measured` shapes (2026-09-25); `nested_assign_oracle.rs` replays 154 keys. pins: ice-struct-pairing-1/C-019 |
| C-020 | Every no-struct write costs within 1.05 of main. | Interleaved medians on the step 0 write set, the build lock held. | **OPEN** | §5: within 1.05 of main timed in the same passes (0.978, 1.026, 1.020), not of step 0's bar (1.057, 1.107, 0.975), which main itself now exceeds (1.080, 1.080, 0.956). Question Q1 of the hand-back: which bar the gate reads. |
| C-021 | Reverting the callers alone leaves the tree compiling and only the unit's own pins red; reverting the facade flag alone reds only the by-name DataFrame doors' cells. | The two revert-alone runs of the sketch's §9. | **PROVEN** | §4. pins: ice-struct-pairing-1/C-021 |
| C-022 | Registry rows: R-016, R-021 and R-022 closed with their pins; R-013, R-014 and R-018 carry this unit's names and cells; the static-partition route has its dated row. | The registry. | **PROVEN** | `docs/spark-sql-iceberg-parity.md`. pins: ice-struct-pairing-1/C-022 |
| C-023 | The 1.5.6 notes carry one line per stored-value change for a reader who will not open the registry. | The notes draft. | **PROVEN** | `task/roadmap/mid-term/v1-5-6-release-notes-draft.md`. pins: ice-struct-pairing-1/C-023 |
| C-024 | After pairing, by position or by name, every leaf pair passes the store-assignment judge flat columns use (`ansi_store_assignable`), at every depth and inside arrays and maps; the first leaf Spark refuses answers Spark's `CANNOT_SAFELY_CAST` with the leaf's path and Spark's type names, and nothing is written (ruling 56(a)). | The `r3_leaf` cells on the sixteen doors and both `PARTITION` spellings; the seam's unit test. | **PROVEN** | `test_ice_struct_pairing_1_round3.py::test_the_engine_answers_as_spark` over `r3_leaf` (118 Spark refusals answered with Spark's condition and detail, the table unchanged); `struct_pairing/tests.rs` `every_paired_leaf_is_judged_as_a_flat_column_is`. Verifier 1's F-01: its 4 regressions, 24 and 33 all refuse as Spark; its F-06 42 raw refusals carry Spark's text (§7). pins: ice-struct-pairing-1/C-024 |
| C-025 | A struct already in the table's field order is judged as a reordered one is, and a user's own `CAST` of a struct is judged on its output, not its operand. | The in-order leaf cases; five explicit-`CAST` cells. | **PROVEN** | `r3_leaf` `str5_to_bigint_inorder` and `bool_to_int_inorder` refuse on all eighteen doors as Spark; `r3_misc` `explicit_cast_values`, `_select`, `_overwrite`, `_by_name`, `_df_append` store Spark's `{a: 5, b: 'y'}`. pins: ice-struct-pairing-1/C-025 |
| C-026 | A struct that reaches a batch cast unpaired is judged by position before hint H-1: a surplus or missing field answers Spark's `EXTRA_STRUCT_FIELDS` / `STRUCT_MISSING_FIELDS`, a refused leaf `CANNOT_SAFELY_CAST`; only a struct Spark stores keeps H-1. | The `r3_part` shapes Spark refuses; the seam's unit test. | **PROVEN** | `test_the_engine_answers_as_spark` over `r3_part` (36 Spark refusals, Spark's condition and detail); `every_paired_leaf_is_judged_as_a_flat_column_is` (surplus field, INT into STRUCT on both entries). pins: ice-struct-pairing-1/C-026 |
| C-027 | `INSERT INTO t (cols) VALUES (…)` with a column list pairs by position, like every other INSERT door (ruling 57(a)). | Verifier 1's F-02 repros and three more. | **PROVEN** | `r3_misc` `values_collist_swapped`, `_swapped_renamed`, `_partial`, `_swapped_two_rows`, `_in_order`, `_swapped_leaf`, `overwrite_values_collist_swapped`: seven cells as Spark. pins: ice-struct-pairing-1/C-027 |
| C-028 | Under `spark.sql.caseSensitive=false`, by-name pairing compares names as Java's `equalsIgnoreCase`: same UTF-16 length, and per character equal, equal upper-cased, or equal lower-cased after upper-casing; not a full Unicode fold (ruling 55(a) F-03). | É/é, ẞ/ß and the non-matching ß/SS on live Spark; a unit test. | **PROVEN** | `r3_misc` `e_acute_*` (nine cells, every by-name door, MERGE and UPDATE included), `sharp_s_from_capital`, `capital_sharp_s_from_small` store as Spark; `sharp_s_from_ss` refuses `CANNOT_FIND_DATA` as Spark; `e_acute_case_sensitive` refuses as Spark; `case_insensitive_names_match_as_java_equals_ignore_case`. pins: ice-struct-pairing-1/C-028 |
| C-029 | The Spark door's `UPDATE` refusal pairs a struct SET value under the session's `spark.sql.caseSensitive` (ruling 55(a) F-05). | Verifier 1's eight cells. | **PROVEN** | `r3_misc` `cs_update_twins_v2/v3`, `cs_update_twins_plus_b_v2/v3`, `cs_update_no_where_twins_v2/v3`, `cs_update_no_where_twins_plus_b_v2/v3` answer Spark's `CANNOT_FIND_DATA` / `EXTRA_STRUCT_FIELDS`. pins: ice-struct-pairing-1/C-029 |
| C-030 | `UPDATE t SET st = (SELECT st FROM src) WHERE …` stores Spark's value with the source struct reordered and in order; it is never a success that writes nothing (ruling 55(a) F-04). Verifier 2 (V2-01) found this held for a scalar-comparison `WHERE` only; the other predicates refuse since 2026-10-15 (C-034). | Both shapes on live Spark. | **PROVEN** | `r3_misc` `update_subquery_reordered` and `update_subquery_in_order` store `{a: 1, b: 2}` as Spark (main refused the first raw and wrote nothing for the second). pins: ice-struct-pairing-1/C-030 |
| C-031 | Both `INSERT OVERWRITE … PARTITION` spellings refuse a struct Spark stores whose field names are not the table's, in order, with hint H-1 naming the workaround, as a carve-out dated 2026-10-14 and closed by the routing unit of question 34(b) (ruling 58(a)). | The `r3_part` cells Spark stores. | **PROVEN** | `test_the_partition_carve_out_refuses_with_the_workaround` (104 cells: 52 per route, 38 of them where main stored Spark's value); registry row ICE-STRUCT-PAIRING-1-STATIC-PARTITION. pins: ice-struct-pairing-1/C-031 |
| C-032 | Verifier 1's surviving mutants O1 (the ambiguity check off) and O5 (the passthrough drops `caseSensitive`) die on Rust tests (ruling 55(a) F-07). | Each mutant run against the Rust suites. | **PROVEN** | O1 dies on `case_twins_are_ambiguous_by_name_unless_the_session_is_case_sensitive`, O5 on `a_by_name_frame_door_pairs_under_the_session_case_rule` (§7). pins: ice-struct-pairing-1/C-032 |
| C-033 | When the rows of a multi-row `INSERT … VALUES` give a struct column field names that disagree (by position, at any depth, inside arrays and map entries, a different field count included, under `spark.sql.caseSensitive`), the statement refuses with Spark's `[INVALID_INLINE_TABLE.INCOMPATIBLE_TYPES_IN_INLINE_TABLE]` text naming the column `colN` by its position in the row, and writes nothing, on all three column-list spellings; rows that agree on the names, in the table's order or another, store by position as Spark does (verifier 2's V2-02, inside the class). | The `x_rows` cells on live Spark; a Rust door test. | **PROVEN** | `test_ice_struct_pairing_1_exception.py::test_multi_row_values_answer_as_spark` (22 cells: 12 refusals with Spark's text up to its SQLSTATE and the table unchanged, 10 stores as Spark, a NULL row among them) and `test_multi_row_values_outside_the_agreement_rule_still_refuse` (4 cells: a field count that differs, which list L1 refuses first, leaf types, `INSERT OVERWRITE`, `UNION ALL`); `struct_pairing_doors.rs` `multi_row_values_refuse_as_spark_when_rows_disagree_on_struct_names` (red with the check off). pins: ice-struct-pairing-1/C-033 |
| C-034 | An `UPDATE` whose `SET` value is a scalar subquery and that no identity path runs (`WHERE true`, an `IN` list, no `WHERE`, every other predicate that is not a scalar comparison; every `WHERE` on `repark.sql()`) refuses at `PreExecute::guard`, the one place every door's planned statement passes, with one text for every column type, before anything is written (owner rulings 74(a) and 90(a), the one named change outside the class). | The `x_update` cells on both doors; a pin per door. | **PROVEN** | `test_ruling_74_a_flat_int_update_from_a_subquery_refuses_before_any_write` (14 cells) and `test_ruling_90_a_struct_update_from_a_subquery_refuses_before_any_write` (28 cells); `update_subquery_refusal.rs`, `ansi_update_subquery_refusal.rs`, `pre_execute/tests.rs` `guard_refuses_an_update_whose_set_value_is_a_scalar_subquery`. pins: ice-struct-pairing-1/C-034 |
| C-035 | The identity path is untouched: `UPDATE … SET col = (SELECT col FROM src) WHERE id = 0` and the other scalar comparisons store Spark's value on the Spark SQL door as before, and a subquery inside a larger `SET` expression is not this refusal's shape. | The identity cells; verifier 2's 24 fixed cells re-run. | **PROVEN** | `test_the_identity_update_from_a_subquery_stores_spark_value` (6 cells); `update_subquery_refusal.rs` `the_identity_update_from_a_subquery_still_stores_on_the_spark_door`; `pre_execute/tests.rs` `guard_passes_an_update_whose_set_value_only_contains_a_subquery`. pins: ice-struct-pairing-1/C-035 |
| C-036 | The refusal names the unsupported shape and names no action that deletes data; the carve-out row's only workaround is "write the struct fields in table order" and both partition hints name no data-deleting action (ruling 76(a)). | The text and the two hints read. | **PROVEN** | The text is pinned verbatim in the three Rust pins and the fixture; registry row ICE-STRUCT-PAIRING-1-STATIC-PARTITION. pins: ice-struct-pairing-1/C-036 |
| C-037 | Verifier 2's surviving mutants `V2_L1` (the door-level leaf judge off), `V2_N3` (the column-list `VALUES` mapping positional), `V2_N4` (the UPDATE refusal's case rule fixed) and `V2_N5` (the identity path admitting no `SET` subquery) die on Rust tests. | Each mutant run with verifier 2's runner. | **PROVEN** | §8. pins: ice-struct-pairing-1/C-037 |

## 1. Build round 2 (2026-10-14)

Round 1 (`856b6263` to `6b59b5b2`) built the fixture, the seam, the callers and deletions, the
facade flag and the re-pins, and halted on two rules of the order's §6. Round 2, on the owner's
answers to questions 47 and 48:

- **One registration table (Q47, C-014).** `BY_NAME_STORE_KERNELS` and `NS_WALL_KERNEL_NAMES`
  in `store_cast.rs`; `register_store_cast_udfs` registers the table and nothing else; the ANSI
  door's `on_session_built` calls it beside the Spark door's `SparkExtension::register`. The
  wall-kernel names are spelled through `ntz_store`'s constants, since `repark-iceberg` has no
  edge to `repark-functions` and the order allows no `Cargo.toml` edit; each door's pin asserts
  they equal `repark-functions`' constants. Probe on the ANSI door: `MERGE … WHEN MATCHED THEN
  UPDATE SET st = s.st` and `… WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, s.st)`
  store `{a: 3, b: 4}` on both rows, as main does.
- **The error condition (C-015).** `spark_ast.rs` peels the `StoreOverflowCast` rule's own
  analyzer context, matched by the rule's `name()`, at both passthrough entries. Five cells
  (`oc_nested_missing_subfield`, `oc_nested_extra_subfield`, `oc_nested_missing_and_extra`,
  `oc_nested_missing_reordered`, `oc_nested_upper_subfield_case_sensitive`) read condition
  `None` with the `repark_store_overflow_cast caused by` prefix before, Spark's condition
  after. Round 1 counted six; the sixth, `test_nested_binding_residues_beside_spark`, was a
  stored-value residue (§ C-019), not a condition defect.
- **List L2 (Q48, C-017)** and **the residues (C-019).**
- **Pins added for the proof:** hint H-1 at S1, S2 and S3 (C-013) and the by-position default
  (C-012), each because a mutant of §2 survived without it.

## 2. Mutants (sketch §9)

Each mutant ran gated by an environment variable in one throwaway build, never committed; the
Rust binaries are `repark-iceberg`'s unit tests and `struct_pairing_doors`, the facade fixture
ran on a release module built from the same tree.

| Family | Mutant | First cell that died |
|---|---|---|
| 1 | by position pairs by name | `struct_pairing_doors` `insert/reordered` (17 door cells); the table test |
| 2 | by name ignores the case rule | the table test `the_rule_pairs_every_mode_shape_and_layout_and_keeps_every_null` |
| 3 | a missing field filled with NULL | `insert/missing` (25 door cells); four `repark-iceberg` tests |
| 4 | a surplus field dropped | `insert/extra` (15 door cells); the table test |
| 5 | the walk stops at a list / a map value / a map key / a dictionary | the table test each time; the list mutant also overflows the stack in `nested_pairs_are_excused_by_the_write_gate`, the map mutants fail `a_map_assigns_when_its_key_and_value_assign` |
| 6 | the guard of S1 / S2 / S3 removed | `the_append_batch_cast_s1_refuses_…` / `the_positional_overwrite_cast_s2_refuses_…` and the door's `PARTITION (p)` cell / the door's `PARTITION (p = 1)` cell |
| 7 | one door handed the other mode | §3 |
| 8 | the default of `WriteOp::Insert` by name | `an_insert_pairs_struct_fields_by_position_unless_its_statement_says_by_name` (added; it survived every test before) |
| 9 | `is_identity` true for a reordered struct | `insert/reordered` (14 door cells); the table test |

## 3. Family 7, door by door

| Door(s) | Site of the mode | Died on |
|---|---|---|
| `insert` (and `insert_values`, `df_insert_into` through the same plain route) | `spark_ast` statement pairing | `insert/reordered`, 8 door cells |
| `insert_values` alone | the `VALUES` arm of `transform_store_dml` | `insert_values/reordered` |
| `replace_where` | `replace_where.rs` source flag | `replace_where/reordered` |
| `insert_overwrite`, `df_insert_into_overwrite` | `insert_overwrite.rs` pairing | `insert_overwrite/reordered` |
| `df_overwrite_partitions` | the same site, by-name flag | `df_overwrite_partitions/reordered`; facade 49 tests |
| `df_append`, `df_save_append` (Rust) | the by-name passthrough of `router.rs` | `df_append/reordered`, `df_save_append/reordered` |
| `insert_by_name` | `insert_by_name.rs` | `insert_by_name/reordered` |
| `merge_insert`, `merge_insert_star` | the MERGE insert rewrite text | `merge_insert/reordered`, `merge_insert_star/reordered` |
| `merge_update` | `nested_assign.rs` assignment cast and refusal | `merge_update/reordered`, `merge_update/missing` |
| `merge_update_star` | `validate_update_store_assignment`; `store_assignment_cast_sql` | `merge_update_star/renamed`; `merge_update_star/reordered` |
| `update` | `nested_assign.rs`; `predicate_dml.rs` | `update/reordered`; facade `v2|update|array|reordered` (12) |
| `update_no_where` | `statement_pairing` for `WriteOp::Update` | facade `cs|update_no_where|*|case*` (4) |
| `df_insert_into`, `df_append`, `df_save_append`, `df_overwrite_partitions`, `overwrite(condition)` (facade flags) | `writer_layout.py` | 94, 45, 45, 49 and 2 fixture tests |

Three further sites survive and are equivalent: the `wrap_store_outputs` of
`insert_stream_checked` and `update_stream_checked` and the `validate_insert_store_assignment`
gate in `merge/insert.rs` see a value the rewrite text has already paired, so their mode cannot
change an answer.

## 4. Revert-alone (sketch §9, C-021)

- **The facade flag (`024bf845`) reverted alone** on `throwaway/revert-flag`, head module:
  141 failed, 19,304 passed, 488 skipped, 151 xfailed. All 141 are the three by-name DataFrame
  doors: 136 matrix cells (`df_append` 44, `df_overwrite_partitions` 48, `df_save_append` 44),
  three Parquet reads, and `test_nested_binding_answers_as_spark` for
  `append_nested_missing_subfield` and `op_nested_missing_subfield`.
- **The callers (`96325714`) reverted alone** on `throwaway/revert-callers`. The commit no
  longer reverts cleanly (round 2 builds on it); the four conflicts kept round 2's independent
  pieces (the registration table, the condition peel) and took the pre-callers code elsewhere.
  The tree compiles. `cargo test -p repark-iceberg -p repark-spark -p repark-sql`: 9 red, all
  this unit's pins (the door test's two tests, the S1 and S2 pins, the three round-1 re-pins of
  texts the deletions retired, the store-assign and update-cast re-pins, the R-18 replay).
  Facade: 573 failed, 18,872 passed, 488 skipped, 151 xfailed: the fixture's 566 (main's 548
  red cells and the 18 re-pinned L2 cells) and the seven residue tests of C-019. Nothing else.

## 5. Cost (C-020)

The step 0 write set (`probe_cell.py` over `synth_1000000.parquet`, 8 threads, 1 warm-up and
5 timed iterations), head and main's module interleaved in each of five passes, run twice with
the build lock held by the timing so no build ran beneath it (load 6.4 to 6.8, then 4.2 to
4.4). Medians of the ten pass medians:

| Cell | Head | Main, same passes | Head / main | Step 0 bar | Head / bar | Main / bar |
|---|---|---|---|---|---|---|
| `ctas` | 151.21 | 154.59 | 0.978 | 143.11 | 1.057 | 1.080 |
| `ctas_partitioned8` | 225.41 | 219.76 | 1.026 | 203.55 | 1.107 | 1.080 |
| `df_write_parquet_zstd` | 108.94 | 106.82 | 1.020 | 111.77 | 0.975 | 0.956 |

A first attempt ran while another lane held the lock and is discarded.

## 6. `EXPLAIN` (H6, C-010)

Ten writes, plain and `VERBOSE`: `INSERT … SELECT`, `VALUES`, from a view, with a column list,
narrowing, `BY NAME`, `INSERT OVERWRITE`, a partitioned `INSERT`, and two struct writes in the
table's field order. The 20 answers are byte-identical on head and main (171,674 bytes, one
sha256 `9c2f0b9eb799…`); `EXPLAIN INSERT … BY NAME` fails to parse identically on both.

## 7. Build round 3 (2026-10-14)

Verifier 1 (head `e3fc32ab`) returned FAIL on F-01, F-02 and F-03, with F-04 to F-09 beside
them. The owner's rulings 54 to 59 (order §1, scope change in §0) scoped this round:

- **Fixture first.** A recorder extension adds three groups to both recordings, never
  re-recording the matrix: `r3_leaf` (216 cells), `r3_part` (160) and `r3_misc` (34), live on
  Spark 4.1.2 and on main `293896ce`. No struct cell moves between `293896ce` and `37d60eaf`
  (U-3) in the 1,222 matrix cells or the 410 new ones. Red on the head before any fix: 166 of
  411, nine of them list L2's leaf cells, which that commit classed as Spark cells and the
  next classes as main's. Red on main: 566 of the round-1 fixture's 2,458 (the 548 of the PR body and
  the 18 L2 re-pins, verifier 1's F-09) and 294 of the round-3 file. Green on the head after
  the fixes: 2,869 of 2,869.
- **One commit per finding**, the fixture re-run after each: 56(a) (C-024 to C-026) leaves 25
  red, 57(a) (C-027) 20, F-03 (C-028) 10, F-05 (C-029) 2, F-04 (C-030) 0 after one fix forward
  (the Spark door's struct fold wraps the subquery, so the identity path's test reads every
  subquery in the value).
- **Mutants (C-032 and the sketch's §9 re-run).** O1 and O5 die on their new Rust pins. Family
  1 (by position pairs by name), 2 (case rule ignored), 3 (missing field NULL), 4 (surplus
  dropped, both modes), 5 (walk stops at a list, a map value, a map key, a nested struct, a
  dictionary), 6 (guard removed at S1, S2, S3), 7 (a door handed the other mode: the
  passthrough, `insert_by_name`, MERGE update, MERGE insert), 8 (`WriteOp::Insert` default by
  name) and 9 (`is_identity` true, and identity ignoring names) all die on named Rust tests
  and door cells. The `WriteOp::Update` arm of `statement_pairing` handed by position survives
  every test, as in verifier 1's run: equivalent, since an UPDATE's struct value is paired by
  the rewrite text before that arm is read (verifier 1's probe `C-M6d-probe.txt`).
- **Never worse than main.** Over the fixture and verifier 1's own probes re-run on the head
  (L, B, X, Y, R, D): the only cells where main answered as Spark and the head does not are the
  152 carve-out cells of C-031 (76 in the fixture, 76 in verifier 1's B probe, 38 per route
  each). 40 cells refuse where Spark stores outside the carve-out and one stores where Spark
  refuses; all 41 answer as main does (the required nested field that cannot be seeded or
  cast, the `WITH … INSERT` door, `UPDATE SET col = (SELECT literal)`, and the flat
  `VALUES ('5')` into a BIGINT column), none is in the unit's class, and verifier 1 named them
  out of scope.

## 8. The exception round (2026-10-15, rulings 73 to 76 and 90)

Verifier 2 returned FAIL on `5b97a250` with V2-01 (an `UPDATE` from a subquery that the identity
path does not run reports success and writes nothing) and V2-02 (multi-row `VALUES` whose rows
disagree on a struct's field names stores by position), and four surviving mutants (V2-03).
Step 0 (`ecf22e42`) showed V2-01 writes nothing for a flat INT column too, so ruling 90 widened
the round by that one change. Built on Opus 5.5 high under the owner's switch of 2026-10-15.

- **Fixture first** (`bf439f58`): `_record_ice_struct_pairing_1_exception.py` adds `x_update`
  (48 cells) and `x_rows` (26) to both recordings, live on Spark 4.1.2 and on main `293896ce`.
  Red on the head before any fix: 55 of 76 (13 `x_rows`, 42 `x_update`); on main 67 of 76.
  Green on the head after the fixes: 76 of 76; the round-1 file 2,458 and the round-3 file 411
  stay green, U-3's `test_ice_tsns_floor_doors_1.py` 246.
- **V2-02** (`261464de`, one fix forward `72f32945` for a NULL row, which the analyzer sees as a
  typed null literal of the table's struct): C-033.
- **The ruling 90 refusal** (`107f7c75`, one commit): `PreExecute::guard`, the one choke point
  every door's planned statement passes after its identity path declined; the admission side
  could not serve `repark.sql()`, which has no identity path, nor the identity path's
  schema-dependent declines. The guard sees through an alias, a cast, `arrow_cast` and RePark's
  `__repark_*` store kernels, because the Spark door's struct fold wraps a struct `SET` value
  in `__repark_store_pair__` and `arrow_cast`. Measured first on main's module and Spark over
  every shape it reaches (flat INT and struct, WHERE true, `IN` lists, no `WHERE`,
  `IS NOT NULL`, `BETWEEN`, a struct predicate, `IN (SELECT …)`, aggregates, `FROM`-less,
  correlated, aliased, multi-assignment, self-referencing subqueries): main never stores
  Spark's value through the provider on a product door. It writes nothing, writes the target's
  own column of the subquery column's name, or refuses `No field named …`. The one equality is
  `WHERE false`, where neither engine has a row to write (6 cells of verifier 2's Q probe).
  The Spark door's test harness context (no `SparkExtension`) stored an aggregate subquery
  through the provider; its negative `g3e8_update_set_subquery_without_where_subquery_still_executes`
  is replaced by the refusal pin, the product doors having refused that statement on main.
- **Mutants** (`ccb23364`, verifier 2's runner `v2_mutants.py` on a clone of the head): `V2_L1`
  dies on `a_paired_struct_leaf_spark_refuses_refuses_on_every_insert_spelling`, `V2_N3` on
  `a_column_list_values_row_maps_each_value_to_its_listed_column` (and two more), `V2_N4` on
  `the_update_refusal_pairs_under_the_session_case_rule`, `V2_N5` on
  `an_update_from_a_column_subquery_with_a_comparison_stores_spark_value` (after the ruling 90
  commit the mutant's declined `WHERE id = 0` reaches the guard and refuses, so the store pin
  still reds).
- **Never worse than main.** Over the fixture and verifier 2's probes re-run on the head
  module: every cell where the head refuses and Spark stores is the ruling 58 carve-out, the
  ruling 90 refusal, or a cell the round-3 head answered the same (lists L1, L2, the required
  nested field, `WITH … INSERT`, `UNION ALL` of disagreeing structs); every cell where the head
  stores a value Spark does not is equal to main or is ruling 75's JDK 17 note (8 cells). No
  verifier-2 probe cell outside V2-01 and V2-02 moved from the round-3 head.
- **Records:** ruling 75's dated note on R-016; ruling 76's carve-out workaround; card
  `task/roadmap/mid-term/update-subquery-store-1-card-2026-10-15.md`; registry row
  ICE-STRUCT-PAIRING-1-UPDATE-SUBQUERY; the 1.5.6 notes line.

```
COVERAGE_ATTESTATION:
  pr_unit: ice-struct-pairing-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the order (sections 0, 1, 4 and 6), the sketch (sections 4, 6 and 9), the owner's answers to questions 34, 35, 37, 47 and 48 and rulings 54 to 59 after verifier 1; C-020 stays OPEN on the bar question.
      artifacts: [task/ledgers/staging/ice-struct-pairing-1-ledger.md, task/wo/ice-struct-pairing-1/order.md]
    - id: AT-2
      status: ATTACKED
      evidence: The fixture enumerates seventeen doors, four containers, the field shapes and both case rules on v2 and v3 (1,216 matrix cells, five added); the seam's table test three modes by eight shapes by ten layouts.
      artifacts: [python/repark/tests/test_ice_struct_pairing_1.py, crates/repark-iceberg/src/write/struct_pairing/tests.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The seam and the kernel read for unwrap, panics and recursion depth; make rust-clippy and make rust-panic-ban run before hand-back.
      artifacts: [crates/repark-iceberg/src/write/struct_pairing.rs]
    - id: AT-4
      status: N/A
      justification: No shared mutable state is added; the mode rides on the statement's own state snapshot.
    - id: AT-5
      status: N/A
      justification: No credentials, network, paths or secret-bearing output are touched.
    - id: AT-6
      status: ATTACKED
      evidence: The fixture was red on main (566 tests, and 294 of the round-3 file); the nine mutant families of section 2 each die on a named cell, two of them only after the pins they exposed were added; in build round 3 verifier 1's survivors O1 and O5 die on new pins and the families die again on this head (section 7).
      artifacts: [python/repark/tests/test_ice_struct_pairing_1.py, crates/repark-spark/tests/struct_pairing_doors.rs]
    - id: AT-7
      status: ATTACKED
      evidence: Release modules built in the lock for the head, the mutants and both revert-alone trees; the gates of the hand-back run on the head.
      artifacts: [python/repark/tests/test_ice_struct_pairing_1.py]
    - id: AT-8
      status: ATTACKED
      evidence: Every expected answer is derived from the rule in the test and held to the live Spark 4.1.2 recording; the static-partition answers were recorded live on 2026-10-14.
      artifacts: [python/repark/tests/ice_struct_pairing_1_spark_oracle.json]
    - id: AT-9
      status: ATTACKED
      evidence: Registry rows R-016, R-021 and R-022 closed, R-013, R-014 and R-018 extended, the static-partition row dated; the 1.5.6 notes carry four lines.
      artifacts: [docs/spark-sql-iceberg-parity.md, task/roadmap/mid-term/v1-5-6-release-notes-draft.md]
    - id: AT-10
      status: ATTACKED
      evidence: The control cells (L1, L2, the cells main already answered as Spark) and the ANSI door's struct MERGE are held; both revert-alone runs red only the unit's own pins.
      artifacts: [python/repark/tests/test_ice_struct_pairing_1.py, crates/repark-sql/tests/ansi_store_kernel_registry.rs]
  complete: true
```
