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
| C-009 | The parity registry carries the dated rows and this ledger the release-note sentence. | Slice 6. | **OPEN** | |
| C-010 | The cost gate holds at 1.05 and the work order's gates pass. | Real ratios and exit codes. | **OPEN** | |
| C-011 | On the native door a refusal names only remedies that door can write, and the move from main on that door is recorded for the owner. | `native_ctas_of_an_unasked_nanosecond_column_is_refused_and_a_kept_one_passes`, `native_ctas_of_a_plain_timestamp_expression_at_v3_is_refused_and_six_digits_pass`, `the_text_names_only_the_remedies_the_seat_can_write`. | **PROVEN** | §4 Q3, §8. Owner to accept or flip. |

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
