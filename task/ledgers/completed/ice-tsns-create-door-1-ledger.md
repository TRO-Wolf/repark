# Unit ledger — ICE-TSNS-CREATE-DOOR-1 · a new nanosecond column needs an ask, at one door under every create seat

**Scope (2026-10-13):** Spark 4.1.2 with Iceberg 1.11 cannot open a table that holds
`timestamp_ns` or `timestamptz_ns`, and RePark created such a column silently from a query's
type. After this unit the column is created only when the user asked: DDL naming the type, a
written `CAST`, or the session setting `repark.sql.allowImplicitTimestampNanos`. Otherwise the
statement stops with an error that names both opt-ins. Nothing is narrowed silently.

**Date:** 2026-10-13 · **Branch:** `fix/ice-tsns-create-door-1` · **Base:** `f3f98823` ·
**Model:** Claude Opus 5.5 (executor) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Order:** the owner's ruling 1 of 2026-10-13, work order ICE-TSNS-CREATE-DOOR-1; design of
record: the ICE-TSNS-CREATE-DOOR-1 design sketch (sections 2, 3, 4, 7, 8) and census (three
passes). **This ledger closes when the unit's pull request merges.**

**Out of scope, untouched:** the frame door's floor; the Parquet nanosecond read door;
`write.parquet`; Arrow tables at `createDataFrame`; DESCRIBE display names;
`col.cast("timestamp_ns")`, `F.expr` and `TRY_CAST` support for the type;
`schema="c timestamp_ns"`; the naive-wall zone question; runtime `conf.set` of build-time
`repark.sql.*` keys; the ICE-TSNS-COERCION-1 module `timestamp_ns_coercion*` (not on the base;
nothing of it is touched).

## PROPOSITION LEDGER — ICE-TSNS-CREATE-DOOR-1 — 2026-10-13

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Main's answers are recorded by a committed recorder before any product edit: the 12-door matrix with no ask and with a written cast, the census pass 3 cells, 12 statements with no nanosecond output, and the v2 refusal texts, in UTC and America/New_York. | The fixture and its recorder. | **PROVEN** | `python/repark/tests/ice_tsns_create_door_1_main.json`, 200 cells recorded on `f3f98823`; §1. |
| C-002 | A cast kernel carries its origin: the registry's kernel is written, a kernel built by engine code is inserted; both share a name and are not equal. No behaviour changes. | Five pins in `crates/repark-functions/src/timestamp_ns_cast/tests.rs`; the three crates' suites with no expectation edited. | **PROVEN** | §2. |
| C-003 | `asked_nanosecond_columns` answers one flag per output column of a plan: true when the column has at least one nanosecond origin and every origin is a written kernel. | `crates/repark-functions/src/timestamp_ns_cast/asked_tests.rs`, 17 tests. | **PROVEN** | §3. Mutants m1 to m7 killed. |
| C-004 | `repark.sql.allowImplicitTimestampNanos` (alias `repark.sql.allow_implicit_timestamp_nanos`) is a session setting, default off, parsed as `allowCreateFormatVersion3` is; a bad value is a config error naming the key. | `implicit_timestamp_nanos_is_off_unless_either_key_turns_it_on` in `cardinality.rs`. | **PROVEN** | §3. Nothing reads it before slice 3. |
| C-005 | A query's Arrow schema becomes Iceberg columns only through `nanosecond_door`: the two old conversions are crate-private and all four outside callers pass a `NanosecondAsk`. A new nanosecond column that is not asked and not kept is refused there, by one function that builds the text. | `nanosecond_door_tests.rs` (9 tests); the crate no longer exports the old functions. | **PROVEN** | §5. Mutants m8, m9 killed. |
| C-006 | Every create and evolution door refuses an unasked nanosecond column with the exact text and leaves the table's files as they were; a written cast, and the plain column with the session setting on, create what main created; a column the replaced or evolved table already holds passes. | `test_ice_tsns_create_door_1.py` (246 tests), `crates/repark-spark/src/tests/nanosecond_create_door.rs` (6), two native-door tests in `crates/repark-sql/src/v3/create.rs`. | **PROVEN** | §6. 86 of the facade pins were red on main `f3f98823`. Mutants m10, m11 killed. |
| C-007 | A statement with no nanosecond output answers what main answered: 12 statements in two zones, cell for cell, and no other test of the four Rust crates or the facade suite changed its answer. | `test_statements_with_no_nanosecond_output_equal_main` (24); the suites. | **PROVEN** | §6, §7. One pre-existing test was re-seated (ruling Q2). |
| C-008 | A nanosecond type the statement asked for, kept or named, below format v3, is refused with one text that names the remedy, on both SQL doors; every other Iceberg error keeps its value. | `builders_tests.rs` (2), `test_a_nanosecond_type_below_format_v3` (38), one row per SQL door. | **PROVEN** | §9. |
| C-009 | The parity registry carries the dated rows and this ledger the release-note sentence. | Rows `ICE-TSNS-SQL-1-R-021` and `R-022` in `docs/spark-sql-iceberg-parity.md`; §11. | **PROVEN** | Both FIXED 2026-10-13; R-021 carries "Changed from main, owner ruling 2026-10-13" and the native door's move for the owner. |
| C-010 | The cost gate holds at 1.05 and the work order's gates pass. | Real ratios and exit codes. | **PROVEN** | §10, §13. Whole facade suite 17,022 passed, 0 failed; four Rust crates 5,325 passed, 0 failed. |
| C-011 | On the native door a refusal names only remedies that door can write, and the move from main on that door is recorded for the owner. | `native_ctas_of_an_unasked_nanosecond_column_is_refused_and_a_kept_one_passes`, `native_ctas_of_a_plain_timestamp_expression_at_v3_is_refused_and_six_digits_pass`, `the_text_names_only_the_remedies_the_seat_can_write`. | **PROVEN** | §4 Q3, §8. Owner to accept or flip. |
| C-012 | Fold 1: every remedy a refusal names works on the seat that raised it, by construction: each seat passes its remedies and its target, one function prints them, and a pin follows each named remedy on every seat for both refusals. | `test_every_remedy_a_refusal_names_works_when_followed` (50 cells, 13 Spark seats); `every_remedy_an_unasked_refusal_names_works_when_followed` and `every_remedy_a_named_type_below_v3_names_works_when_followed` (native); door unit pins. | **PROVEN** | §16.1. Findings F1, F3, F7, F8. |
| C-013 | Fold 1: an evolving seat checks only the columns it would add; a source column whose name the table holds under another type answers what main answered. | `test_an_evolving_seat_answers_a_same_name_column_of_another_type_as_main_did` (18 cells against main's recording); `an_evolving_seat_checks_only_the_columns_it_would_add`; a door unit pin. | **PROVEN** | §16.2. Finding F5. |
| C-014 | Fold 1: a cast written in a SQL temp view, and through EXCEPT, INTERSECT and a semi or anti join, is an ask; the shapes not followed are refused, pinned, and pass with the cast outermost. | `test_a_written_cast_is_followed_through_views_set_operations_and_semi_joins` (7), `test_a_written_cast_that_is_not_followed_is_refused_and_works_written_outermost` (5), the cache pins (3); walk unit pins. | **PROVEN** | §16.3. Findings F6 (closed), F9 (part built, part disclosed for the owner). |
| C-015 | Fold 1: a nanosecond field two and three levels down is refused, and the verify's surviving mutant (the nested walk stops at depth one) is killed. | `a_nanosecond_field_two_and_three_levels_down_is_checked` (Rust, 7 shapes); `test_a_nanosecond_field_two_and_three_levels_down_is_refused` (36 cells). | **PROVEN** | §16.4. |
| C-016 | Fold 1: a 200-column CTAS with no nanosecond column costs at most 1.05 of main, and the cost set's wide statement reaches the door. | Five interleaved sets. | **PROVEN** | §16.5: 0.993, from 1.181. Finding F10. |
| C-017 | Fold 1: a plain `TIMESTAMP` in a schema-evolving MERGE source is refused, the cells are recorded for the owner, and the hint on that seat names only spellings that store microseconds there. | `test_a_plain_timestamp_in_a_schema_evolving_merge_source_is_refused` (4); `a_plain_timestamp_in_a_merge_source_is_refused_and_the_named_spellings_work`. | **PROVEN** | §16.6. Finding F4, disclosed: owner to accept or flip. |
| C-018 | Fold 1: native `ALTER TABLE ADD COLUMN` and nested native DDL written as plain `TIMESTAMP` answer as main (a named exception the owner holds). | `plain_timestamp_in_native_add_column_and_nested_ddl_still_stores_nanoseconds_as_main`. | **PROVEN** | §16.7. Finding F2, disclosed: no behaviour change. |

## 1. Main's recorded answers (C-001)

`_record_ice_tsns_create_door_1_main.py` ran on the release module built from `f3f98823`
before any product file was edited. Every cell runs in a fresh session and warehouse, v3
opt-in on. The seed is `2026-01-02 03:04:05.123456789` and one NULL row, in a v3 table
`(id INT, c timestamp_ns, cz timestamptz_ns)`; no cell uses pandas or polars. Both zones answer
the same in every cell.

- **matrix, 96 cells** (12 doors, columns `c` and `cz`, no ask and a written cast, 2 zones).
  Eleven doors create the nanosecond column from the plain column and from the written cast
  alike, format version 3, all nine digits: SQL CTAS, `CREATE OR REPLACE … AS`,
  `REPLACE TABLE … AS`, `writeTo().create()`, `.createOrReplace()`, `.replace()`,
  `mode("overwrite").saveAsTable` with and without `overwriteSchema`, `mergeSchema`
  `saveAsTable` append and `writeTo().append()` on a `write.spark.accept-any-schema` table,
  `MERGE WITH SCHEMA EVOLUTION`. `saveAsTable` create has no v3 spelling and answers
  `Invalid schema for v2:` in all 8 of its cells.
- **pass3, 42 cells** (sources: the plain columns of the v3 table, a capsule-registered Arrow
  table as a temp view, a written cast of a string; 7 doors; 2 zones). All 42 create
  `c timestamp_ns, cz timestamptz_ns`: the census's 28 unasked cells and its 14 written-cast
  twins, reproduced.
- **plain, 24 cells** (12 statements over a microsecond table, 2 zones): CTAS,
  `CREATE OR REPLACE`, `writeTo().create()`, `saveAsTable` create, overwrite and append,
  `mergeSchema` append, `MERGE WITH SCHEMA EVOLUTION`, and four refusals
  (`TABLE_OR_VIEW_ALREADY_EXISTS`, `TABLE_OR_VIEW_NOT_FOUND`,
  `UNRESOLVED_COLUMN.WITH_SUGGESTION`, `INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS`).
- **v2, 38 cells** (19 doors, 2 zones): the census's 16 v2 refusal doors re-seated on native
  nanosecond sources (its `tempview` door seeded through pandas, which is a microsecond source
  since ICE-TSNS-FRAME-DOOR-1), plus column DDL naming the type and a `mergeSchema` append
  into a v2 `accept-any-schema` table, unasked and with a written cast. 13 answer
  `Invalid schema for v2:`; `like`, `cache`, `replace_missing`, `savepath` and the two arity
  refusals answer their own text.

Each cell holds the error (class, condition, text), whether the table's files are the same as
before the statement, and the table's format version, Iceberg types (metadata JSON), Arrow
types and int64 rows.

## 2. The mark (C-002)

`SparkTimestampNsCast` holds `origin: CastOrigin { Written, Inserted }`.
`timestamp_ns_cast_udf(zoned, origin)` takes it as a required argument; the registry sites
(`repark-functions` `instant_ts.rs::functions`, the ANSI door's `dialect.rs`) pass `Written`,
and `timestamp_ns_cast_expr(expr, zoned)` keeps its signature and builds `Inserted`. `name()`
is unchanged. `PartialEq` and `Hash` include the origin. `is_written_timestamp_ns_cast`
downcasts the function's implementation; no SQL text is read.

The work order writes the downcast as `func.inner().as_any()`. DataFusion 54.1's
`ScalarUDFImpl` has no `as_any`; the same downcast is `func.inner().downcast_ref::<T>()`, an
inherent method of `dyn ScalarUDFImpl`. It holds: `the_registered_kernel_is_written`.

Pins: `the_registered_kernel_is_written`, `a_kernel_built_by_engine_code_is_inserted`,
`written_and_inserted_kernels_share_a_name_and_are_not_equal`,
`a_written_cast_stays_written_through_two_analyses` (a `SELECT` planned through a session
holding the registry and every analyzer rule of the crate, `analyze_eagerly` twice),
`explain_text_is_unchanged_by_origin`. Two test sessions of `repark-spark` that register the
kernels by hand now pass `Written`, as the product registry does.

Gate: `cargo test --locked -p repark-functions -p repark-spark -p repark-sql` exit 0, no
expectation edited.

## 3. The walk and the setting (C-003, C-004)

`crates/repark-functions/src/timestamp_ns_cast/asked.rs`. The rule, the nodes followed and the
default are in that directory's `map.md`. Three points the work order does not spell out:

- **Every plan variant is named.** The plan `match` has no wildcard arm: the nodes that are
  not followed are listed and answer unasked, so a DataFusion upgrade that adds a node does
  not compile until the node is placed. The expression `match` keeps a default arm, which is
  the rule itself (the union of the children's origins, unasked when there is none).
- **A depth bound.** The walk answers unasked below 1,024 levels of plan and expression
  (`a_plan_deeper_than_the_walk_follows_is_not_asked`). A refusal with a remedy is the safe
  direction; an unbounded recursion on a user-shaped plan is not.
- **A column whose type holds no nanosecond timestamp is never walked**, so a statement with
  no nanosecond output pays one pass over its output types.

Tests: the fifteen the work order names, plus `nodes_that_keep_their_input_columns_keep_the_ask`
(`Filter`, `Sort`, `Limit`, `Distinct`, `Repartition`, `SubqueryAlias`, `Projection` in one
chain) and the depth bound.

Mutants, each run with `cargo test -p repark-functions --lib timestamp_ns_cast`:

| # | Mutant | Killed by |
|---|---|---|
| m1 | the registry registers `Inserted` | `the_registered_kernel_is_written`, `a_written_cast_stays_written_through_two_analyses` |
| m2 | `timestamp_ns_cast_expr` builds `Written` | `an_inserted_kernel_over_a_table_column_is_not_asked`, `an_inserted_kernel_over_a_microsecond_value_is_no_origin`, `a_kernel_built_by_engine_code_is_inserted` |
| m3 | `PartialEq` ignores `origin` | `written_and_inserted_kernels_share_a_name_and_are_not_equal` |
| m4 | the unfollowed plan arm answers asked | 9 tests, `a_table_column_is_not_asked` and `an_unknown_plan_node_is_not_asked` among them |
| m5 | asked wins over unasked when origins are combined | `a_union_with_one_table_column_branch_is_not_asked` and 3 more |
| m6 | an inserted kernel counts as an asked origin | `an_inserted_kernel_over_a_table_column_is_not_asked`, `an_inserted_kernel_over_a_microsecond_value_is_no_origin` |
| m7 | expressions with no nanosecond type are descended | `a_condition_over_nanoseconds_is_not_an_origin` and 2 more |

The setting is a fourth field of `ReparkSqlSettings` and `ReparkSqlConfig`. It accepts what
its sibling accepts (`true`, `false`, `1`, `0`, `yes`, `no`, any case), through the sibling's
parser, and its own error names its own key.

## 4. Rulings (orchestrator, provisional, 2026-10-13)

The executor halted before slice 3 with six questions. The orchestrator ruled; every ruling
below is provisional until the owner adopts it.

- **Q1.** `MergeSpec` gains a required field, and four test files outside the work order's
  list build a `MergeSpec` literal (`sink_offsets_isolation_tests.rs`,
  `merge/tests/helpers.rs`, `merge/tests/occ_scoped.rs`, `microbatch/crash_tests.rs`). Ruling:
  one `nanosecond_ask` line in each, no expectation changed.
- **Q2.** One pre-existing test moved:
  `crates/repark-spark/tests/timestamp_ns_nested_shapes.rs`
  `a_statement_that_does_not_supply_the_column_runs_and_carries_its_rows` seeded its table with
  `CREATE TABLE … AS SELECT id, named_struct('v', ns, 'n', 1) AS st, …`, an unasked nested
  nanosecond column, which the door refuses. Ruling: the seed writes the cast,
  `named_struct('v', CAST(ns AS timestamp_ns), 'n', 1)`; the assertions are unchanged. It is
  the only pre-existing test that moved in the four Rust crates (5,319 tests before the unit's
  own) and the facade suite.
- **Q3 (an owner question; this is the answer the build uses).** A hint never names a remedy
  that does not work on the door that raised it. The text is still built in one place; the
  seat says, as data, whether its dialect can write the nanosecond cast and how it spells
  microseconds. The Spark door's text is the work order's, unchanged. The native door's text
  drops the cast clause and names `TIMESTAMP(6)`. No native `CAST(… AS timestamp_ns)` spelling
  is added, and the kernel's internal name is not documented as an ask. §8 lists what moves on
  the native door for the owner to accept or flip.
- **Q4.** Refusal 2 reaches the native door: `repark-sql` `schema_ddl::iceberg_err` passes
  `repark_iceberg::catalog::iceberg_to_datafusion` (slice 4).
- **Q5.** The native seat compares a kept column's name exactly (`case_sensitive: true`).
- **Q6.** The work order counts 15 v2 doors; the census evidence file holds 16. Slice 4 pins
  19: those 16 on native sources, column DDL naming the type, and a `mergeSchema` append into a
  v2 `accept-any-schema` table, unasked and with a written cast.

## 5. The door (C-005)

`crates/repark-iceberg/src/write/nanosecond_door.rs`: `new_table_schema` and
`added_columns_schema` take a `NewColumns` request (the Arrow schema, a `NanosecondAsk`, the
schema of the table being replaced or evolved, the form word, the case rule).
`arrow_schema_to_iceberg_with_unknown` and `incoming_schema` are `pub(crate)`; the four outside
callers the work order names were the only ones, and each now goes through the door: the Spark
CTAS seat, the native create seat, the `mergeSchema` append seat and MERGE's
`union_source_schema`. The check, the kept rule and the text are in the directory's `map.md`.

- **Kept is compared by shape.** A nested column converted from Arrow gets fresh field ids, so
  equality of Iceberg types would never hold for a kept struct or list. Names and primitive
  types are compared at every depth; field ids, nullability and docs are not.
- **MERGE.** `MergeSpec::nanosecond_ask` is a function each SQL door sets; `repark-iceberg`
  does not name `repark-functions`.
- **The evolution seat** needs the flags before it builds the ask, because it drops void
  fields: the Spark adapter exposes `asked_columns` (the flags, `None` under the setting)
  beside `nanosecond_ask`.
- **The native seat** builds its request in the child module: `execute_create_table` was at
  clippy's line limit.

Mutants, `cargo test -p repark-iceberg --lib nanosecond_door` and
`-p repark-spark --lib nanosecond_create_door`:

| # | Mutant | Killed by |
|---|---|---|
| m8 | the kept test ignores the type | `a_kept_name_with_another_type_is_new`, `a_nested_nanosecond_field_is_checked` |
| m9 | the door does not look inside a list | `a_nested_nanosecond_field_is_checked` |
| m10 | the setting is read as always off; as always on | `a_written_cast_or_the_setting_creates_the_column_on_every_sql_door`; `an_unasked_nanosecond_column_is_refused_on_every_sql_door` and 2 more |
| m11 | the `mergeSchema` seat passes every; the MERGE seat passes every | `an_unasked_nanosecond_column_is_refused_on_every_sql_door`, both |

## 6. The cells (C-006, C-007)

Facade, `test_ice_tsns_create_door_1.py`, every cell in a fresh session and warehouse. Before
any product edit the file ran on main's module: 86 failed, 160 passed.

| Cells | Count | Answer |
|---|---|---|
| 12 doors, `c` and `cz`, 2 zones, no ask | 48 | refusal 1, exact text with the door's form; the table's files unchanged |
| the same with a written cast | 48 | 44 equal main's table, nine digits; the 4 `saveAsTable` create cells are refused for format v2 |
| the same, plain column, setting on | 48 | 44 equal the written cast's table; 4 refused for format v2 |
| census pass 3 | 42 | 28 unasked refuse naming `c`; 14 written-cast twins equal main |
| statements with no nanosecond output | 24 | equal main, cell for cell |
| a kept column, 9 doors, 2 zones | 18 | pass, all nine digits |
| a kept name with the other type; a new name | 2 | refusal 1 |
| DDL and `ADD COLUMN` naming the type | 2 | created |
| a cached frame; a pandas frame | 1 | refused; a microsecond table |
| `format-version` 3 alone | 1 | not an ask |
| asked shapes; unasked shapes | 5; 6 | created; refused |

The form word: `CTAS` for `CREATE TABLE … AS`, `writeTo().create()` and `saveAsTable` create;
`RTAS` for `CREATE OR REPLACE`, `REPLACE TABLE`, `createOrReplace`, `replace` and
`mode("overwrite").saveAsTable`; `mergeSchema append`; `MERGE WITH SCHEMA EVOLUTION`.

Rust, Spark door (`nanosecond_create_door.rs`): five SQL doors by two columns refuse with the
text, the stored schema and the warehouse's object count unchanged; the cast and the setting
create; kept passes case-insensitively.

## 7. Suites on the slice 3 source

`cargo test --locked --no-fail-fast -p repark-functions -p repark-iceberg -p repark-spark
-p repark-sql`: 5,321 passed, 0 failed, 10 ignored, with the Q2 re-seat. Before the re-seat it
was 5,318 passed and that one test failed. The whole facade suite on the slice 3 patch as
first built: 16,983 passed, 0 failed.

## 8. The native door: owner to accept or flip (Q3)

Measured through `repark-sql` on this unit's source, v3 opt-in on.

- The native parser cannot write the nanosecond cast: `CAST(x AS timestamp_ns)` answers
  `Unsupported SQL type timestamp_ns`. `CAST(x AS TIMESTAMP_NTZ)` answers
  `Unsupported SQL type TIMESTAMP_NTZ`. `CAST(x AS TIMESTAMP(6))` works and stores Iceberg
  `timestamp`. `CAST(x AS TIMESTAMP(6) WITH TIME ZONE)` parses and also stores a naive
  `timestamp`, so it is not offered as the zoned spelling: both kinds name `TIMESTAMP(6)`.
- On that door a plain `TIMESTAMP` expression is Arrow nanoseconds (DataFusion's default).
  So at `format_version = 3` these three statements, which created a `timestamp_ns` column on
  main, are now refused: `CREATE TABLE t WITH (format_version = 3) AS SELECT ts FROM
  <a timestamp_ns table>`, `… AS SELECT TIMESTAMP '2026-01-02 03:04:05' AS ts`,
  `… AS SELECT now() AS ts`. **Three measured native cells move**, and the class is every
  native v3 CTAS whose output holds a plain `TIMESTAMP` expression. At the default format
  version main already refused all three (`Invalid schema for v2:`); the text changes.
- The ask on the native door is the session setting. The microsecond form is
  `CAST(… AS TIMESTAMP(6))`. The refusal raised there says exactly that:
  ``[TIMESTAMP_NANOS_NOT_REQUESTED] CTAS would create column `ts` as timestamp_ns, a type Spark cannot read, and the statement does not ask for it. Set `repark.sql.allowImplicitTimestampNanos` = true on the session. For microseconds write CAST(<expression> AS TIMESTAMP(6)).``
- A kept column passes on a native RTAS; column DDL naming the type creates it.
- Open card, not filed here: a native spelling for the nanosecond cast, so the native door has
  a per-column ask.

## 9. Below format v3 (C-008)

One seat, `crates/repark-iceberg/src/catalog/builders.rs::iceberg_to_datafusion`: an
`iceberg::Error` of kind `DataInvalid` whose message starts `Invalid schema for v` and whose
every detail line names `timestamp_ns` or `timestamptz_ns` in the type position becomes
the text below. Every fork message measured for a
nanosecond type below v3 starts `Invalid schema for v2:`.

``[TIMESTAMP_NANOS_REQUIRES_FORMAT_V3] {detail lines joined by "; "}. Create the table with TBLPROPERTIES ('format-version'='3') (writeTo: tableProperty) and `repark.sql.allowCreateFormatVersion3` = true.``

The native door did not pass that seat (`schema_ddl::iceberg_err` built the error itself);
ruling Q4 routes it through. Proof that every other native error keeps its value: the four
crates' suites, 5,325 passed and 0 failed, no expectation edited.

The v2 doors, 19 in two zones (ruling Q6; the work order's "15" is 16 in the census evidence):

| Doors | Answer |
|---|---|
| `ctas_plain`, `cor`, `tempview`, `wtocreate`, `saveastable`, `wtoreplace`, `saveoverwrite`, `overwriteschema`, `evolving` (9) | refusal 1: the door runs before the fork's version check |
| `cast_ns`, `ddl`, `addcol_v2base`, `evolving_cast` (4) | refusal 2, exact text |
| `like`, `cache`, `replace_missing`, `savepath`, `mergeschema`, `wtoappendmerge` (6) | main's own text, byte for byte |

In every cell the table's files are unchanged and its state equals main's. Main answered
`DataInvalid => Invalid schema for v2:` in the first 13.

## 10. Cost (C-010)

`task/wo/ice-tsns-create-door-1/evidence/cost.md`: five interleaved sets, main's module then
head's, sixteen cores, the build lock held. Ratio of the medians, head over main: CTAS over a
1,000,000-row microsecond table 1.002; `mode("overwrite").saveAsTable` 1.019;
`MERGE WITH SCHEMA EVOLUTION` adding a microsecond column 1.005; `EXPLAIN` of a 200-column
query 0.966. The gate is 1.05. Single sets of the two 60 ms statements move by up to 8 percent
either way.

## 11. Registry and release note (C-009)

Rows `ICE-TSNS-SQL-1-R-021` (the create door; "Changed from main, owner ruling 2026-10-13";
the native door's move under "owner to accept or flip (question Q3)") and
`ICE-TSNS-SQL-1-R-022` (the below-v3 text).

**Release note (1.5.6):** RePark no longer creates a `timestamp_ns` / `timestamptz_ns` column
unless you ask: name the type in DDL, write `CAST(… AS timestamp_ns)` on the column, or set
`repark.sql.allowImplicitTimestampNanos`. Statements that relied on the old behaviour stop
with an error that names the options that work on the door that raised it; nothing is
narrowed silently.

**Not built.** Slice 5 of the work order (the session setting at the pandas door) is skipped:
owner question Q4 is not answered yes. The ICE-TSNS-COERCION-1 slice 1 module
(`timestamp_ns_coercion*`) is not on this base; nothing of it is touched, and its fixture
doors that create a nanosecond column from a rewritten read will need the setting or a
written cast when it lands on top of this unit.

## 12. Moved pre-existing tests

Three pre-existing tests moved; none was re-recorded.

| Test | On main | Now | Ruling |
|---|---|---|---|
| `crates/repark-spark/tests/timestamp_ns_nested_shapes.rs` `a_statement_that_does_not_supply_the_column_runs_and_carries_its_rows` | its seeding CTAS of `named_struct('v', ns, 'n', 1)` creates the table | the seed writes the cast, `named_struct('v', CAST(ns AS timestamp_ns), 'n', 1)`; assertions unchanged | Q2 |
| `python/repark/tests/test_ice_tsns_merge_wall_1.py` `test_control_targets_answer_what_main_answered[ntz_v2-update_column]` | sources `ns`, `tzns`: `PySparkException`, `- Invalid type for c: … is not supported until v3` | `AnalysisException`, `[TIMESTAMP_NANOS_REQUIRES_FORMAT_V3] …`, exact text | Q7 |
| the same, `[ltz_v2-update_column]` | the same | the same | Q7 |

The two facade tests failed on the slice 4 head and were handed back, not re-pinned. That
door creates its control table by DDL with a sibling column of the source type, so on a v2
target the DDL is refused, on main and now. **Ruling Q7 (orchestrator, provisional,
2026-10-13):** `ice_tsns_merge_wall_1_main.json` stays main's record, untouched; the test
asserts the fixture still holds main's text for those four cells and expects the new refusal.

**Ruling Q8 (orchestrator, provisional, 2026-10-13):** the cost gate is the ratio of the
five-set medians; every set is in `evidence/cost.json`.

**Registry numbers.** The two rows are `ICE-TSNS-SQL-1-R-021` and `R-022`; `R-020` is held by
another open unit.

## 13. Gates on the final head

| Gate | Result |
|---|---|
| `cargo fmt --check` | exit 0 |
| `make rust-clippy rust-panic-ban check-crate-dag check-lib-rs check-rust-file-size check-manifest` | each exit 0 |
| `cargo test --locked --no-fail-fast -p repark-functions -p repark-iceberg -p repark-spark -p repark-sql` | 5,325 passed, 0 failed |
| `make py-lint py-format-check check-lib-py check-python-conventions check-docstring-presence toml-check spell-check` | each exit 0 |
| `make check-ledgers check-ledger-grammar check-docs-links check-docs-compaction` | each exit 0 |
| `scripts/check_map_md.sh --base f3f98823`, `scripts/sync_map_md.py --check` | exit 0 |
| the facade pin file | 285 passed |
| the whole facade suite, `pytest -n 8 python/repark/tests` | 17,022 passed, 0 failed |
| cost, four ratios of medians | 1.002, 1.019, 1.005, 0.966 |
| mutants m1 to m11 | all killed |

## 14. Cells that move against main

- **Refused now, created a nanosecond column on main (owner ruling 1):** the plain column of
  a nanosecond source on a v3 target through 11 doors, 44 matrix cells and 28 census cells
  (§6); the same through a nested element, a union branch, a `coalesce` argument, a window,
  a join side, and a cached frame.
- **Text and class only, refused before and now:** below format v3, 9 unasked doors answer
  `TIMESTAMP_NANOS_NOT_REQUESTED` and 4 that ask or name the type answer
  `TIMESTAMP_NANOS_REQUIRES_FORMAT_V3`, where main answered `Invalid schema for v2:` (§9).
- **Owner to accept or flip (Q3), the native door:** three measured cells at
  `format_version = 3`, the plain column of a `timestamp_ns` table, a `TIMESTAMP '…'`
  literal and `now()`, are refused where main created `timestamp_ns`; the class is every
  native v3 CTAS whose output holds a plain `TIMESTAMP` expression (§8).
- **Owner to accept or flip, MERGE (fold 1, §16.6):** four measured cells, a plain
  `TIMESTAMP` literal or cast in a `MERGE WITH SCHEMA EVOLUTION` source that the evolution
  would add, are refused where main added a naive `timestamp_ns` column.
- **Owner to accept or flip, the native door (fold 1, §16.7):** the verify measured 20 native
  v3 cells of the class above; native `ADD COLUMN` and nested native DDL written as plain
  `TIMESTAMP` are a named exception that still equals main.
- **Back to main's answer in fold 1 (§16.2, §16.3):** a type change on an evolving seat; a
  cast written in a SQL temp view; EXCEPT, INTERSECT, semi and anti joins over cast columns.
- **Still refused where main created (fold 1, §16.3, for the owner):** a written cast under
  grouping sets, in a scalar subquery, in a lambda body, in a window ordered or partitioned
  by a plain nanosecond column, or before `cache` / `persist` / `localCheckpoint`.
- **Unchanged:** a written cast, the setting, a kept column, DDL naming the type, and every
  statement with no nanosecond output.

## 16. Fold 1 (2026-10-13), after an independent verify failed `53ced116`

The verify found ten findings and one surviving mutant. Each finding folded was reproduced
first: the fold's facade pin file ran on the module of the failed head and all of its 125
tests of that moment failed. The fixes are at the construction point. **Where a section above
quotes a refusal text or says the door does not read the format version, this section holds:**
both texts changed, and the door now takes its target's format version from the seat.
Rulings are the orchestrator's; the items marked for the owner are provisional.

| Finding | Status | Pin that proves it |
|---|---|---|
| F1 native below-v3 text named Spark spellings | CLOSED | `every_remedy_a_named_type_below_v3_names_works_when_followed` |
| F2 native `ADD COLUMN` / nested DDL plain `TIMESTAMP` store nanoseconds | DISCLOSED, unchanged, equal to main (owner holds the native door) | `plain_timestamp_in_native_add_column_and_nested_ddl_still_stores_nanoseconds_as_main` |
| F3 the setting remedy failed through `conf.set` / `SET` | CLOSED by wording; run-time set not built | `test_the_setting_is_read_when_the_session_is_built_and_the_hint_says_so` |
| F4 MERGE source: the zoned microsecond remedy failed; plain `TIMESTAMP` cells not listed | hint CLOSED; refusal DISCLOSED for the owner | `test_a_plain_timestamp_in_a_schema_evolving_merge_source_is_refused` |
| F5 evolving seats refused a type change as an unasked column | CLOSED | `test_an_evolving_seat_answers_a_same_name_column_of_another_type_as_main_did` |
| F6 a cast in a SQL temp view was refused | CLOSED | `test_a_written_cast_is_followed_through_views_set_operations_and_semi_joins` |
| F7 `saveAsTable` create named remedies that end in the next refusal | CLOSED | `test_every_remedy_a_refusal_names_works_when_followed[save_create-*]` |
| F8 an existing table was told to be created | CLOSED | the same test, replace and evolve seats |
| F9 written casts the walk does not follow | set operations and semi / anti joins CLOSED; the rest DISCLOSED for the owner | `test_a_written_cast_that_is_not_followed_is_refused_and_works_written_outermost` |
| F10 a 200-column CTAS cost 1.18 of main | CLOSED | §16.5 |
| mutant: the nested walk stops at depth one | KILLED | `a_nanosecond_field_two_and_three_levels_down_is_checked` |

### 16.1 Hints by construction (C-012; F1, F3, F7, F8)

A `NanosecondAsk` carries its seat's `Remedies`: whether the dialect can write the nanosecond
cast, the sentence that turns the setting on, a whole microsecond spelling for a naive and for
a zoned column (either may be absent), and that door's sentence for a v3 table on a create, on
a replace and on an existing table. `NewColumns::target` says whether the table is, or would
be, format v3, and if not which of the three it is. `nanosecond_door.rs` prints both refusals
from those and nothing else.

- **Unasked** (`TIMESTAMP_NANOS_NOT_REQUESTED`): the cast (Spark door only), the setting, the
  microsecond spelling. When the target is below v3 the text adds `A nanosecond column also
  needs a format version 3 table.` and the seat's way to one, because the cast or the setting
  alone would only reach the next refusal (F7).
- **Asked, below v3** (`TIMESTAMP_NANOS_REQUIRES_FORMAT_V3`): raised by the door, before the
  fork's check, with that same sentence. A create names SQL `TBLPROPERTIES` and
  `writeTo(...).tableProperty` and says `saveAsTable` cannot set the format version; a replace
  names those on the replace, or the `ALTER TABLE` upgrade; an evolving seat names the upgrade
  only. No existing table is told to be created (F8).
- **Seats outside the door** (column DDL on the Spark door, `ADD COLUMN` on both): the fork's
  error is folded by a function that has no seat, so it names no spelled statement, only that
  the table must be format version 3; each door's own `iceberg_err` adds its table-property
  spelling (`'format-version' = '3'`; `format_version = 3`), which is true for a new and for
  an existing table (F1, F8).
- **The setting** (F3): the text says `build the session with … = true
  (ReparkSession.builder.config; the key is read once, when the session is built)`. A run-time
  `spark.conf.set` or SQL `SET` of the key is accepted and ignored, pinned as it behaves. Main
  does the same for the sibling key `repark.sql.allowCreateFormatVersion3`. Making a run-time
  set of either key refuse loudly is a later card; no run-time `SET` is built here.
- **The native door** names `WITH (format_version = 3)`, `ALTER TABLE … SET PROPERTIES
  (format_version = 3)` and `CAST(<expression> AS TIMESTAMP(6))`, each followed in a pin.

The pin: 13 Spark seats by two columns by a v3 and a below-v3 target, 50 cells (`saveAsTable`
create has no v3 target). Each pins the exact text, then follows the microsecond spelling, the
cast and the session built with the setting; below v3 it pins the exact format-version refusal
for the cast and for the setting, then follows each way to a v3 table the text names, with the
cast and with the setting, and reads the stored Iceberg type.

### 16.2 Evolving seats (C-013; F5; ruling: yes)

`added_columns_schema` skips a field whose name the table already holds, whatever its type:
the evolution would not add it, and `evolve_schema` answers main's `Cannot change column
type`. `new_table_schema` still treats a kept name with another type as new, since a replace
does replace the column. 18 cells recorded on main's module (three doors, three held and
source pairs, two zones) equal the unit's, error text and table state.

### 16.3 What the walk follows (C-014; F6 closed, F9 provisional for the owner)

- **Views.** A scan whose source carries a plan is walked into it. The Spark door's SQL
  `CREATE TEMP VIEW` is its own re-planning provider, so the Spark seat gives the walk a
  resolver that answers the view's definition plan. A view of a view is followed; each view
  costs 32 of the walk's 1,024 levels.
- **Semi and anti joins**, in the `Join` arm, on the side whose rows they keep. `EXCEPT` and
  `INTERSECT` plan as such joins, so their left branch is followed and their right branch,
  which contributes no value, is not an origin.
- **Not built, each needs its own case (owner to flip):** `GROUP BY ROLLUP` / `CUBE` /
  `GROUPING SETS`, a scalar subquery's inner cast, a lambda body, a window function whose
  `ORDER BY` or `PARTITION BY` is a plain nanosecond column. Each is refused and pinned, and
  the same statement with the cast on the outermost select creates the column (pinned).
- **`cache`, `persist`, `localCheckpoint`** replace the plan with stored rows: a cast written
  before them is hidden, one written after them is an ask (pinned, in the registry row).

One limit of the resolver, recorded and not pinned: the definition plan is the view's text as
planned when the view was created. The view re-plans the same text at scan time, so a cast its
author wrote stays written; a temp view that reads another temp view later replaced by one
with a different plan is judged by the plan of the day it was defined.

### 16.4 The nested walk (C-015)

The verify's mutant made the door look one level into a list, struct or map and no further;
all 5,077 Rust tests passed on it. `a_nanosecond_field_two_and_three_levels_down_is_checked`
(array of struct, struct of struct, struct of array of struct, map value struct, map of map,
array of array, and a zoned leaf three levels down) fails on that mutant, run here. The facade
pins the same six shapes on a create and on both evolving seats, 36 cells.

### 16.5 Cost (C-016; F10)

`task/wo/ice-tsns-create-door-1/evidence/cost.md`. The seat ran a second eager analysis of the
plan on every create. `asked_nanosecond_columns_in` now answers all-false from the output
types when none holds a nanosecond timestamp, and analyzes only otherwise. Five interleaved
sets, ratios of medians: CTAS 0.989, `mode("overwrite").saveAsTable` 1.039,
`MERGE WITH SCHEMA EVOLUTION` 0.988, the 200-column CTAS **0.993** (the verify measured 1.181
on the failed head; per set 0.993, 1.001, 0.982, 0.989, 0.995). The cost set's fourth
statement is that CTAS; the `EXPLAIN` it replaces never reached the door.

### 16.6 Owner to accept or flip: MERGE sources (C-017; F4)

Inside a `MERGE WITH SCHEMA EVOLUTION` source a plain `TIMESTAMP` is Arrow nanoseconds: the
probe plans the source text without the Spark door's microsecond typing. That is main's
behaviour; main then silently added a naive `timestamp_ns` column. The door now refuses.
**Four measured cells move:** a source column written as `TIMESTAMP '2026-01-02 03:04:05'`,
`CAST('2026-01-02 03:04:05' AS TIMESTAMP)`, `CAST(c AS TIMESTAMP)`, `CAST(cz AS TIMESTAMP)`.
The same expressions through a DataFrame or a CTAS are microseconds and are not refused.
The hint on that seat names `CAST(<expression> AS TIMESTAMP_NTZ)` for a naive column and
`to_timestamp(<expression>)` for a zoned one; both are followed in pins and store
microseconds (`CAST(… AS TIMESTAMP)` does not, there). Future card, one line: type the MERGE
source through the Spark door so `TIMESTAMP` in it is microseconds.

### 16.7 Owner to accept or flip: the native door (C-018; F2, and §8)

- **Named exception, unchanged and equal to main:** at format version 3, native
  `ALTER TABLE … ADD COLUMN ts TIMESTAMP` stores `timestamp_ns`, and so do nested native
  column definitions (`a ARRAY<TIMESTAMP>`, `st STRUCT<v TIMESTAMP>`), with no ask. A
  top-level `ts TIMESTAMP` in native `CREATE TABLE` is refused by the older guard. Pinned as
  it is; the owner holds the native door.
- **Refused now, created on main (§8):** the verify measured 20 native cells at
  `format_version = 3` (a `TIMESTAMP` literal, `now()`, `current_timestamp`, a cast to
  `TIMESTAMP`, `to_timestamp`, `date_trunc`, the plain column of a nanosecond table, nested
  forms of them, and a `CREATE OR REPLACE` whose kept column differs in case or type).

### 16.8 The verify's probes, re-run on the fold

Its hint probe: every seat's named remedies are followed as before; the SQL temp view cells
create; no below-v3 text on an existing table says `Create the table`. Its three door and ask
probes, 439 cells: against its own run on the failed head, 15 cells change kind and no other.
Six create as on main (the SQL temp view, a view of a view, EXCEPT, INTERSECT, a semi and an
anti join); nine answer main's type-change error again. No cell that answers on main raises a
different error on the fold.

### 16.9 Gates on the fold

`cargo test --locked --no-fail-fast` over the four crates: 5,332 passed, 0 failed.
`make rust-clippy`, `make rust-panic-ban`, `cargo fmt --check`, the file-size, crate-root and
DAG checks: exit 0. The three pin files: 765 passed. The whole facade suite: 17,147
passed, 0 failed. Mutants: the verify's survivor killed; m1 to m11 unchanged in what they
test.

## 17. Coverage attestation

**The executor's own attestation**, filed from the runs recorded in this ledger. It is not an
independent verdict.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-tsns-create-door-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every behaviour clause has a pin that was red before its change. The facade pin file ran on main's module before any product edit, 86 failed and 160 passed, and is 285 passed on the head. Main's answers are a committed fixture recorded on f3f98823.
      artifacts: [python/repark/tests/test_ice_tsns_create_door_1.py, python/repark/tests/ice_tsns_create_door_1_main.json]
    - id: AT-2
      status: ATTACKED
      evidence: Naive and zoned columns, two session zones, NULL rows, nested list and struct fields, a kept nested column with other field ids, a missing flag, a plan deeper than the walk follows, grouping sets, semi joins, VALUES with an inserted NULL, a cached frame, a pandas frame, format v2 and v3 targets.
      artifacts: [crates/repark-functions/src/timestamp_ns_cast/asked_tests.rs, crates/repark-iceberg/src/write/nanosecond_door_tests.rs, python/repark/tests/test_ice_tsns_create_door_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: A refusal leaves the table as it was. The facade cells compare every file under the table directory before and after, the Rust cells the stored schema and the warehouse object count, on all twelve doors and below v3.
      artifacts: [python/repark/tests/test_ice_tsns_create_door_1.py, crates/repark-spark/src/tests/nanosecond_create_door.rs]
    - id: AT-4
      status: N/A
      justification: The door is a pure function of a schema, an ask and the existing schema; the setting is read from the session fixed at build. No shared mutable state is added.
    - id: AT-5
      status: N/A
      justification: No privileged action, secret, path or deserialization. The setting widens what a statement may create inside a table the user can already write.
    - id: AT-6
      status: ATTACKED
      evidence: The wrong-result risks are a silently created column and a refused asked column. Unasked cells refuse on every door, asked cells equal main's table to nine digits, 24 statements with no nanosecond output equal main cell for cell, and the walk's unsure answer is always a refusal.
      artifacts: [python/repark/tests/test_ice_tsns_create_door_1.py, crates/repark-functions/src/timestamp_ns_cast/asked.rs]
    - id: AT-7
      status: ATTACKED
      evidence: Five interleaved cost sets. Ratios of medians are 1.002, 1.019, 1.005 and 0.966 against a 1.05 gate. A column with no nanosecond type is never walked and the walk is bounded at 1,024 levels.
      artifacts: [task/wo/ice-tsns-create-door-1/evidence/cost.md, python/repark/tests/_time_ice_tsns_create_door_1.py]
    - id: AT-8
      status: ATTACKED
      evidence: The below-v3 mapping reads the owned fork's message and matches the type position only. Seven other errors pass unchanged in unit pins and the four crates pass with no expectation edited after the native door was routed through the seat. DataFusion 54.1's downcast is pinned. No manifest or lock change.
      artifacts: [crates/repark-iceberg/src/catalog/builders_tests.rs, crates/repark-functions/src/timestamp_ns_cast/tests.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Both refusal texts are pinned exactly on each SQL door, with class and condition. After fold 1 a pin follows every remedy a text names, on every seat of both doors and for both refusals, and reads the stored type. Refusals for other reasons keep main's text byte for byte.
      artifacts: [crates/repark-sql/src/v3/create.rs, crates/repark-spark/src/tests/nanosecond_create_door.rs, python/repark/tests/test_ice_tsns_create_door_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Eleven mutants, thirteen variants, all killed, and after fold 1 the independent verify's surviving mutant too. The whole facade suite and the four Rust crates ran on the fold with 0 failed. Three pre-existing tests moved and are listed with their rulings. The verify's ten findings are each closed or disclosed in section 16.
      artifacts: [task/ledgers/completed/ice-tsns-create-door-1-ledger.md, python/repark/tests/test_ice_tsns_merge_wall_1.py]
  reattested: [AT-2, AT-6, AT-7, AT-9, AT-10]
  complete: true
```
