# map — crates/repark-spark/tests/

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001). Wrapped-line fragments rewritten as complete sentences (D-002).

## Purpose

Integration tests of the assembled Spark door: a real `repark_core::ReparkSession` built with
`SparkExtension` + `SparkDialect`, exercised end-to-end.

## Contents

- [session_extension.rs](session_extension.rs) — temp view + `session.sql` reaches the
  Spark date shim (`year`, `weekofyear`) through the installed extension + dialect.
- [declared_sorted_tighten.rs](declared_sorted_tighten.rs) — Spark-door execution pins that
  `tightenNulls` elides `SortExec` for nullable `ORDER BY ts` keys, while hint mode keeps it.
  Tightened-source CTAS, derived expressions, EXISTS subqueries, cache remints, lazy `into_view`,
  and bare/two-part catalog names refuse without publishing the sink. All-nullable projection
  CREATE and INSERT remain allowed. EAGER-BUDGET-1 step 2 (2026-09-13): the
  cache-materialize call site passes `None` for the new `max_total_bytes` parameter; the
  pinned refusal is unchanged. pins: eager-budget-1/C-005
  **ICE-VIEWS-1 / V-001 (2026-09-22):** CREATE VIEW over a tightened source
  refuses before the catalog write — including the facade's
  `/* repark:bare-name */` marked spelling, which is a registered-catalog
  write, not a session view — while an untightened CREATE VIEW persists and
  reads back; a session `CREATE TEMPORARY VIEW` over the tightened source
  serves the source rows byte-for-byte (IPI-40 PR6a1,
  `session_scoped_temp_view_serves_and_select_into_stays_allowed`) and SELECT
  INTO stays allowed; `ALTER VIEW … AS` keeps its own refusal ahead of the tighten
  error.
- [ddl_sessions.rs](ddl_sessions.rs) — CTAS
  end-to-end, namespace-`location` on a strict catalog (ADV-1 / N5), the BUG-001 dual-key
  property pin, the `spark.catalog` metadata surface, and the config-driven memory catalog —
  all on memory/local catalogs (AWS-free).
  **CATALOG-1 (2026-09-26):** `config_driven_memory_catalog_registers_and_runs` declares the memory
  catalog with the `catalog-impl` long form — a bare `type=memory` now refuses at first use like Spark.
- [dml_sessions.rs](dml_sessions.rs) — `session_sql_bare_dml_applies_eagerly` — the F-BR-2 bare-
  `INSERT` eager-apply trap through
  `session.sql` (memory catalog, AWS-free).
- [session_timezone.rs](session_timezone.rs) — live-Spark value and Arrow-type pins for
  extractors, `date_trunc`, `date_format`, DST boundaries, pre-1970 instants, and the native
  DataFrame API across non-UTC and half-hour zones. DATE/TIME invariants, source instants,
  default-zone fallback, composed DATE/string inputs, date-valued shims, and TZ-7/TZ-8 registry
  divergences are covered. **FNP-11B step 4 (2026-09-15):**
  `time_arguments_never_move_with_the_session_zone` asserts the zone-independent
  `[UNSUPPORTED_TIME_TYPE]` refusal (hour/minute/second over TIME) instead of
  the old answering values. pins: fnp-11b/C-005
- [timestamp_cast_seconds.rs](timestamp_cast_seconds.rs) — the
  `CAST(TIMESTAMP AS <numeric>)` epoch-seconds class at the **Spark door** and the **native
  DataFrame API**, on real sessions, value AND Arrow type. Nine pins: whole instants either side
  of 1970; the **floor edge both signs** (Spark uses `Math.floorDiv`, so `-0.5 s → -1` and
  `-1.25 s → -2` where truncation toward zero says `0` and `-1` — the only inputs that separate
  the real fix from the plausible one); zone-independence across three zones (a cast reads the
  instant, never a wall clock); a real timestamp COLUMN with its null mask; narrower integer
  targets (`INT`/`SMALLINT`); float and decimal
  targets, which keep the fraction; reverse `CAST(<integer> AS TIMESTAMP)` reads seconds and
  round-trips; its Arrow type is `timestamp[us, tz=UTC]`. `CAST(ts AS DATE)` is TZ-8
  (session-zone Date32), and `CAST(ts AS STRING)` is `Utf8`. Ledger: `../../../task/tz5-cast-seconds-ledger.md`,
  `../../../task/v3-btz4-ledger.md`.
- [session_timestamp_type.rs](session_timestamp_type.rs) — **Q10:** Spark-door +
  native DataFrame pins for `spark.sql.timestampType`. Default LTZ type/value,
  NTZ opt-in literals/casts (naive µs, no localization), invalid-value refusal
  naming both tokens, DDL `TIMESTAMP` → Iceberg `timestamp` under NTZ /
  `timestamptz` under LTZ. `to_timestamp` stays LTZ.
- [timestamptz_ns_wall_doors.rs](timestamptz_ns_wall_doors.rs) — **ICE-TSTZNS-WALL-1
  (2026-10-10):** the zoned mirror of the file below.
  `every_door_stores_the_instant_insert_stores_in_a_zoned_column`: four walls (one before the
  epoch, New York's gap and overlap) from a `timestamp_ns`, a `TIMESTAMP_NTZ`, a `DATE` and a
  `TIMESTAMP` column go through ten doors (`BY NAME`, `OVERWRITE`, `REPLACE WHERE`, MERGE
  insert and `INSERT *`, MERGE update and `SET *`, `UPDATE` with and without a `WHERE`, a
  `CASE`), copy-on-write and merge-on-read, in UTC, America/New_York, Asia/Kolkata,
  Asia/Kathmandu and Australia/Lord_Howe. Each must store what `INSERT … SELECT` stores in
  the same session; INSERT itself is held to fixed instants in three zones.
  `a_value_past_the_range_answers_as_insert_does_on_every_door`: a `DATE`, a `TIMESTAMP_NTZ`
  and a `TIMESTAMP` past 2262 through the same doors, ANSI on (`[CAST_OVERFLOW]` naming
  `"TIMESTAMPTZ_NS"`) and off (NULL in a nullable column, the non-nullable refusal in a
  required one). On main both tests fail, the second by a panic in Arrow's cast.
  `a_dictionary_encoded_wall_stores_inserts_instant_on_the_overwrite_doors` (R-008 fold 1,
  2026-10-10): the same four walls Dictionary-encoded in a temp view go through `OVERWRITE`,
  `BY NAME`, `OVERWRITE … BY NAME` and dynamic partition `OVERWRITE` in America/New_York into
  a partitioned table; each must store what `INSERT … SELECT` of the same walls plain stores,
  and INSERT itself is held to the fixed New York instants (INSERT of the encoded source
  refuses, as the kernel refuses an encoded argument). Kills the mutant that stops seeing
  through `Dictionary` for the zoned target at the store.
  pins: ice-tstzns-wall-1/C-003, C-004, C-005, C-010
- [ice_tsns_type_planner_1_update_forms.rs](ice_tsns_type_planner_1_update_forms.rs) —
  **ICE-TSNS-TYPE-PLANNER-1, ruling 91 (2026-10-15):** in an America/New_York session, a
  `timestamp_ns` and a `timestamptz_ns` source into a `TIMESTAMP` column, copy-on-write and
  merge-on-read: `UPDATE … SET v = c`, the same with `WHERE id >= 0`, and `SET v = CAST(c
  AS TIMESTAMP) WHERE id >= 0` store one value (a pre-1970 value with digits below the
  microsecond among them). Red with the identity UPDATE seam reverted alone.
  pins: ice-tsns-type-planner-1/C-019
- [timestamp_ns_wall_doors.rs](timestamp_ns_wall_doors.rs) — **ICE-TSNS-MERGE-WALL-1
  (2026-10-09):** the readiness review's first probe test, ported unchanged in substance. In
  an America/New_York session `INSERT … VALUES` and `MERGE … INSERT *` of one `TIMESTAMP`
  literal into a `timestamp_ns` column must store one value. Red on main `40fc916f`
  (INSERT `1767323045123456000`, MERGE `1767341045123456000`).
  pins: ice-tsns-merge-wall-1/C-001
  **The Rust-door matrix (same unit):** six `timestamptz_ns` instants with non-zero digits
  below the microsecond (before the epoch, on each side of New York's gap, the two instants
  of one overlap wall) go through eight doors — `INSERT … SELECT`, `INSERT OVERWRITE`,
  `REPLACE WHERE`, MERGE insert, MERGE `INSERT *`, MERGE update, `UPDATE` with and without a
  `WHERE` — in UTC, America/New_York and Asia/Kolkata.
  `every_door_stores_a_nanosecond_instant_as_its_session_wall` asserts the Arrow type and the
  int64 walls of a `timestamp_ns` target;
  `every_door_keeps_a_nanosecond_instant_in_a_zoned_column` is the `timestamptz_ns` control.
  The expected walls are the facade matrix's `zoneinfo` values. Mutants M1, M2 and M3 of
  the ledger each red the wall test at the door they break.
  pins: ice-tsns-merge-wall-1/C-004, C-005, C-006, C-007, C-011
  **Fold 1 (2026-10-09), pins from the verify.**
  `a_nested_microsecond_ntz_field_keeps_the_split_main_has` holds the control: field
  assignment stores the session wall, the other four doors the UTC wall (parity row
  ICE-TSNS-SQL-1-R-011, OPEN).
  `an_overflowing_literal_stores_null_without_ansi_as_insert_does` is the 42 cells' shape:
  four far literals and each zone's edge instant through `UPDATE … WHERE` (both row-level
  modes) and `MERGE … INSERT VALUES`.
  `an_overflowing_wall_source_answers_as_insert_select_on_the_overwrite_doors` puts a
  `TIMESTAMP_NTZ` and a `DATE` past each end of the range through `INSERT OVERWRITE` and
  `INSERT … BY NAME`, ANSI on and off. `untouched_rows_carry_their_nanosecond_ticks` is the
  carry pin the facade module gave up: DELETE, sibling UPDATE and MERGE and the three
  maintenance rewrites, both row-level modes, `timestamp_ns` and `timestamptz_ns`.
  The two nested store pins of fold 1 left with the split; the nested pins are the next row.
  pins: ice-tsns-merge-wall-1/C-017, C-018, C-019, C-023, C-024
  **ICE-NTZ-NESTED-REFUSE-1 (2026-10-20):** the control above is rewritten into
  `a_nested_microsecond_ntz_field_refuses_an_instant_outside_utc`: in New York and Kolkata the
  six doors refuse the instant with R-011's text and the three seeded rows keep a NULL leaf; in
  UTC they store `1767323045123456`, the wall main stored. pins: ice-ntz-nested-refuse-1/C-002, C-003
- [write_door_record.rs](write_door_record.rs) — **WRITE-SPINE-1 step 3a round 2 (2026-10-24):**
  the runtime door record on a production-built Spark session over a memory catalog. One
  statement per Spark door, each followed by `last_write_doors`: `INSERT … SELECT` D1,
  `INSERT … BY NAME` D3, a positional `INSERT` into an `accept-any-schema` table D3, `INSERT
  OVERWRITE` D4, `REPLACE WHERE` D5, CTAS D6, MERGE insert and `INSERT *` D7, MERGE whole-struct
  update and `SET *` D8, MERGE field update and top-level leaf update D9, a two-arm MERGE
  `[D8, D7]`, `UPDATE` D10, `DELETE` and `SELECT` nothing, an `INSERT` with a write option D2;
  a second session with a session write conf records D2 for a plain `INSERT` and nothing for
  the next `SELECT`. **Round 3 (2026-10-24, ruling Q9):**
  `an_update_the_fork_provider_runs_records_the_update_door` runs an `UPDATE` with no `WHERE`,
  which the fork provider runs, and asserts D10 and the stored `k = 5`. **Round 4 (2026-10-24,
  verifier S1-1 and S2):** `a_merge_records_each_door_once_however_many_arms_take_it` (two field
  assignments and two insert arms record `[D9, D7]`; the dedupe mutant reds it) and
  `a_merge_whole_column_update_records_the_cast_path_it_takes` (whole `ARRAY<INT>`,
  `MAP<STRING, INT>` and `ARRAY<STRUCT<…>>` columns record D8, leaf `TIMESTAMP` and
  `TIMESTAMP_NTZ` columns D9). Risk: a branch that
  writes without recording, or a record that survives into the next statement.
  pins: write-spine-1/C-011
- [store_kernel_registry.rs](store_kernel_registry.rs) — **ICE-STRUCT-PAIRING-1 build round 2
  (2026-10-14), parity row R-013:** at session build the Spark door resolves every name of
  `store_cast::BY_NAME_STORE_KERNELS` and of `store_cast::NS_WALL_KERNEL_NAMES`; each table
  entry names its own kernel, and the wall names equal `repark-functions`' constants. The ANSI
  door's twin is `repark-sql/tests/ansi_store_kernel_registry.rs`. pins: ice-struct-pairing-1/C-014
  **WRITE-SPINE-1 step 1b (2026-10-24):** `missing_wall_kernel_refuses_instead_of_storing_utc`.
  In New York, a session that holds `__repark_cast_timestamp_ntz__` stores the instant
  `2026-01-02 03:04:05.123456+00:00` written `BY NAME` into a `TIMESTAMP_NTZ` column as the
  session wall `1767305045123456`. The same session with that kernel deregistered refuses the
  write with the internal error that names the kernel, and the table stays empty. Main stored
  the UTC wall. Risk: a session built without the kernel stores a wall shifted by the zone
  offset with no error. pins: write-spine-1/C-002
- [struct_pairing_doors.rs](struct_pairing_doors.rs) —
  **ICE-STRUCT-PAIRING-1 step 1 (2026-10-14), parity row R-016:** the sixteen write doors at
  the Rust door (`ReparkSession::sql`, and `sql_built_with_write_options` with the DataFrame
  writers' SQL, overwrite intent and `source_by_name`) × the shapes `reordered`, `renamed`,
  `extra`, `missing` of a top-level `STRUCT<a: INT, b: INT>`. The expected answer comes from
  Spark's rule in the test (by position on `INSERT`, `VALUES`, `INSERT OVERWRITE`, `REPLACE
  WHERE` and `insertInto`; by name elsewhere), with the refusal's full text and table label.
  `UPDATE` with no `WHERE` keeps main's raw refusal for the reordered shape (parity row
  R-014). Red on main: 34 of 64 cells. **Correction (2026-10-14):** `INSERT … VALUES` with a
  surplus or a missing field keeps main's raw planner refusal (list L1, parity row R-018), as
  the facade fixture holds it; red on main is 32 of 64 cells. Build round 2: the count of
  written rows is `i32::from`, for clippy's `bool_to_int_with_if`; and
  `the_partition_overwrite_route_refuses_an_unpaired_struct_with_hint_h1` pins hint H-1 on
  the partition overwrite route: `PARTITION (p = 1)` (S3, `strict_partition_store_cast`)
  refuses with the static-partition hint and `PARTITION (p)` (S2 through
  `write_overwrite_staged_files_from_stream`) with the batch hint, the seeded row kept; the
  same static statement with the struct in the table's field order stores.
  pins: ice-struct-pairing-1/C-002, C-008, C-009, C-011, C-013
  **Build round 3 (2026-10-14, ruling 55(a) F-07):**
  `a_by_name_frame_door_pairs_under_the_session_case_rule` drives the by-name DataFrame door
  (`source_by_name`) with a recased struct under each value of the session's case carrier:
  case-insensitive it stores `{a:1, b:2}`, case-sensitive it refuses with Spark's
  `CANNOT_FIND_DATA` for `` `st`.`a` `` and stores nothing (kills verifier 1's mutant O5, the
  passthrough dropping `caseSensitive`). pins: ice-struct-pairing-1/C-032
  **Exception round (2026-10-15, V2-02):**
  `multi_row_values_refuse_as_spark_when_rows_disagree_on_struct_names` drives a two-row
  `VALUES` through the three column-list spellings (none, `(id, st)`, `(st, id)`) with field
  names that disagree, rows that disagree only in order, and rows that differ only in case
  under each value of the session's case carrier: each disagreement refuses with Spark's
  `INVALID_INLINE_TABLE.INCOMPATIBLE_TYPES_IN_INLINE_TABLE` text for `col2` (`col1` for the
  swapped list) and writes neither row; rows that agree, in case-insensitive mode or in an order
  other than the table's, store by position as Spark does. pins: ice-struct-pairing-1/C-033
  **Exception round (2026-10-15, verifier 2's V2-03): the four mutant pins.**
  `a_paired_struct_leaf_spark_refuses_refuses_on_every_insert_spelling` (`INSERT … SELECT`,
  `VALUES`, column-list `VALUES` and `INSERT OVERWRITE` of `named_struct('b', '5', 'a', 1)`
  into `STRUCT<a: BIGINT, b: STRING>` refuse Spark's `CANNOT_SAFELY_CAST` for `` `st`.`a` ``,
  nothing written; kills `V2_L1`, the door-level leaf judge off);
  `a_column_list_values_row_maps_each_value_to_its_listed_column` (`(st, id)` and `(id, st)`
  lists store by position into their listed columns; kills `V2_N3`);
  `the_update_refusal_pairs_under_the_session_case_rule` (case twins under
  `caseSensitive=true`, with and without `WHERE`, refuse `CANNOT_FIND_DATA` for `` `st`.`b` ``;
  kills `V2_N4`); `an_update_from_a_column_subquery_with_a_comparison_stores_spark_value`
  (`WHERE id = 0` from a reordered and an in-order source stores `{a: 1, b: 2}`; kills
  `V2_N5`). pins: ice-struct-pairing-1/C-037
  **ICE-TSNS-FLOOR-DOORS-1 fold 1 (2026-10-14), the door pin the verify asked for.**
  `every_door_stores_a_nanosecond_source_as_insert_does_in_new_york` writes `-1 ns`,
  `-1,001 ns`, an exact microsecond and New York's gap wall (and a NULL row) through the MERGE
  insert arm (column list and `INSERT *`), `INSERT OVERWRITE` and `INSERT … BY NAME`, a
  `timestamp_ns` source into `TIMESTAMP` and a `timestamptz_ns` source into `TIMESTAMP_NTZ`,
  format versions 2 and 3, partitioned by a constant: 64 cells. The expected value is plain
  `INSERT`'s answer in the same America/New_York session, read back in the test, and that
  answer must differ from plain `INSERT` in a UTC session. The verify's mutants M2 (the MERGE
  insert arm reads the wall as UTC) and M10 (the wall is not localised) each red it.
  pins: ice-tsns-floor-doors-1/C-015
- [update_subquery_refusal.rs](update_subquery_refusal.rs) —
  **ICE-STRUCT-PAIRING-1 exception round (2026-10-15), owner rulings 74(a) and 90(a):** the
  Spark SQL door. `UPDATE t SET col = (SELECT col FROM t_src)` and the subquery with its own
  `WHERE`, under `WHERE true`, `WHERE id IN (0, 3)` and no `WHERE`, refuse with the ruling 90
  text and leave both rows as seeded: for a flat INT column
  (`ruling_74_a_flat_int_update_from_a_subquery_refuses_on_the_spark_door`) and a struct
  column fed reordered and in order
  (`ruling_90_a_struct_update_from_a_subquery_refuses_on_the_spark_door`).
  `the_identity_update_from_a_subquery_still_stores_on_the_spark_door` holds `WHERE id = 0` to
  Spark's value on the same three columns.
  `ruling_90_a_an_update_from_a_subquery_refuses_for_every_column_type` holds the refusal and
  the unchanged table for twelve column types (STRING, BIGINT, DOUBLE, DECIMAL, BOOLEAN, DATE,
  TIMESTAMP, TIMESTAMP_NTZ, ARRAY, MAP, a nested struct, an array of structs) under
  `WHERE true`. The ANSI door's twin is `repark-sql/tests/ansi_update_subquery_refusal.rs`.
  **Owner ruling 95 (2026-10-15):** every refused cell also reads, before and after the
  statement, the table's current snapshot id, its snapshot count, its metadata location and the
  file names under its `metadata/` directory, through the session's own catalog, and fails on
  any change.
  `ruling_95_an_update_from_a_subquery_that_matches_no_row_refuses_on_the_spark_door` holds the
  same refusal under `WHERE false` and `WHERE id BETWEEN 100 AND 200` (a filter the identity
  path declines that matches no row; `WHERE id > 100` is a scalar comparison the identity path
  runs) for the INT column and both struct sources: 12 cells.
  `ruling_95_the_whole_refused_family_answers_one_text_on_the_spark_door` collects the text of
  every refused cell of the file (42 cells) and holds them, the door's
  `Error during planning: ` prefix removed, to the one ruling 90 text;
  `ruling_95_the_refusal_names_no_data_deleting_action_on_the_spark_door` holds that no text
  names `DROP`, `DELETE`, `TRUNCATE`, `OVERWRITE`, `REPLACE`, `PURGE`, `expire` or
  `remove_orphan`, in any case.
  **UPDATE-SET-SUBQUERY-WALK-1 (2026-10-15, owner rulings 99 (c) and 110):**
  `ruling_99_c_a_scalar_subquery_inside_the_set_expression_refuses_on_the_spark_door` holds
  `SET v = 1 + (SELECT v FROM src)`, `COALESCE((SELECT v FROM src), 0)` and
  `CASE WHEN true THEN (SELECT v FROM src) ELSE 0 END`, each under `WHERE true` (the identity
  path does not run), to the ruling-90 text with both seeded rows unchanged.
  pins: ice-struct-pairing-1/C-034, C-035, C-036
- [timestamp_ns_nested_shapes.rs](timestamp_ns_nested_shapes.rs) —
  **ICE-TSNS-MERGE-WALL-1, the split (2026-10-09):** a nested `timestamp_ns` leaf is refused.
  `every_door_refuses_a_nested_nanosecond_leaf_and_writes_nothing` puts nine shapes (struct,
  struct in struct, array of struct, array, struct of array, map value, map key, map of
  struct, a struct with one field renamed) through seventeen doors in two zones: `INSERT`
  with `VALUES`, `SELECT`, a column list, `BY NAME` (named and star), `OVERWRITE` (both),
  `REPLACE WHERE`, the four MERGE arms, `UPDATE` with and without a `WHERE`, and the three
  routes the last verify leaked through that SQL can spell (an array element, a temporary
  view, a subquery). Each must take the named refusal with the target's leaf path, and per
  shape no file may appear under the warehouse and no snapshot be added.
  `a_field_assignment_into_the_leaf_is_refused_too` covers `SET st.v = …` through UPDATE and
  MERGE. `the_refusal_names_the_table_the_column_and_the_leaf` holds the whole text, on the
  door that used to name the source expression.
  `a_statement_that_does_not_supply_the_column_runs_and_carries_its_rows` seeds a table by
  CTAS (not gated) in both row-level modes and runs twelve statements that omit the column or
  give it a bare `NULL` (INSERT forms, sibling UPDATE and MERGE, DELETE), two maintenance
  calls and a time-travel read: all run and the seeded leaf reads back every nanosecond.
  `a_nested_zoned_or_microsecond_leaf_is_not_guarded` is the control.
  `an_update_with_no_where_refuses_a_value_narrowed_from_nanoseconds` is the top-level pin of
  `refuse_narrowed_update`: four spellings (the narrowing in the THEN branch, in the ELSE
  branch, in `if` and in an array element) from a `timestamp_ns` and a `timestamptz_ns`
  source are refused and store nothing; a plain column, a typed NULL, a zoned source, a
  literal, a microsecond literal beside an untyped NULL and a narrowing that sits only in a
  `CASE` condition store; `CAST(c AS TIMESTAMP)` and `date_trunc` with a `WHERE` still store.
  pins: ice-tsns-merge-wall-1/C-040, C-041, C-042, C-043, C-047
  **ICE-NTZ-NESTED-REFUSE-1 (2026-10-20):** the control's nested `TIMESTAMP_NTZ` row stored the
  UTC wall of a New York instant; it now asserts R-011's refusal (not R-015's text), dated in
  the test's doc comment. pins: ice-ntz-nested-refuse-1/C-008
  **ZONE-SEAM-1 fix round 3 (2026-10-23, owner ruling on verifier round 2 S1-3):**
  `a_partitioned_insert_refuses_a_nested_nanosecond_leaf_even_for_a_bare_null`: a
  format-version 3 table with `st STRUCT<v: timestamp_ns, n: INT>` partitioned by `k`, the
  partition column last, first and in the middle; a static and a dynamic `INSERT OVERWRITE` and
  `INSERT INTO`; a bare `NULL` and a value; UTC and New York: R-015's refusal and no new file in
  all 48 cells. Red on fix round 2's head (`b74f17e0`): the 24 bare-`NULL` cells stored.
  pins: zone-seam-1/C-023
- [ntz_nested_refuse.rs](ntz_nested_refuse.rs) — **ICE-NTZ-NESTED-REFUSE-1 (U-8, 2026-10-20),
  parity row R-011:** outside UTC a nested microsecond `TIMESTAMP_NTZ` leaf refuses an instant.
  `every_door_refuses_an_instant_outside_utc_and_writes_nothing`: five shapes (struct, struct in
  struct, array, array of struct, map value) through eighteen doors (`INSERT` with `VALUES`,
  `SELECT`, a column list, `BY NAME` named and star, `OVERWRITE` both, `REPLACE WHERE`, the
  MERGE insert arms built in the source and in the clause, `INSERT *`, MERGE update and
  `UPDATE SET *`, `UPDATE` with and without `WHERE`, a temporary view, a subquery, a scalar
  subquery in `VALUES`) in New York and Kolkata: the text with the leaf path, no file, no
  snapshot. `a_field_assignment_of_an_instant_refuses_too`: five field doors that stored the
  session wall on main. `the_refusal_names_the_table_the_column_and_the_leaf`: the whole text
  and no deleting word in it. `a_wall_or_the_hints_cast_stores_outside_utc_and_the_plain_update_refuses_the_cast`:
  sixteen doors store a wall and the hint's `CAST` stores the session wall, except the plain
  `UPDATE` above the leaf, which refuses the cast; the field assignment of the cast stores the
  session wall. `a_statement_that_does_not_supply_the_column_runs_and_carries_its_rows`: ten
  statements and two maintenance calls keep the stored wall.
  `a_zoned_leaf_and_a_top_level_ntz_column_are_not_guarded`: the controls, and R-015's text
  first on a struct holding both leaves.
  `the_type_read_follows_a_cte_skips_a_zoned_sibling_and_fails_closed`: a wall through
  `WITH … INSERT` passes the gate (main answers "not implemented"), an instant through it
  refuses, an instant into a zoned sibling field
  stores, and an unplannable value refuses. **Mutants (§7 of the ledger):** M1 the zone
  condition always on, M2 a failed plan read as naive, M3 the plain `UPDATE` sub-expression
  rule removed, M4 the struct arm of `holds_zoned_timestamp` removed, M5 the field-path pruning
  removed, M6 `PREPARE` decided by the zone, M7 the every-column fallback removed (M2b: a
  failed INSERT plan read as naive); each reds a named pin.
  pins: ice-ntz-nested-refuse-1/C-001, C-002, C-004, C-005, C-006, C-007, C-008, C-012, C-013,
  C-034
  (`the_type_read_follows_a_cte_skips_a_zoned_sibling_and_fails_closed` holds C-001 and C-004;
  `a_frame_through_a_repartition_or_an_unmodelled_node_refuses` holds C-034, described with
  `ntz_nested_refuse_casts.rs` below.)
- [ntz_into_ltz_nested.rs](ntz_into_ltz_nested.rs) — **ZONE-SEAM-1 step 3c (2026-10-22, ruling
  ZS-9):** a nested `TIMESTAMP_NTZ` value (a container cast of an instant, a `TIMESTAMP_NTZ`
  literal) into a `STRUCT<v: TIMESTAMP, n: INT>` column refuses outside UTC, with the whole text
  and no value written, on the store doors (`INSERT … BY NAME`, `INSERT OVERWRITE` from a query and
  from `VALUES`, MERGE `INSERT`, MERGE `UPDATE SET t.st =`, MERGE `UPDATE SET *`, plain `UPDATE`);
  stores the wall read in UTC on the same doors in a UTC session; and stores the wall read in New
  York on the doors that convert in the plan (positional `INSERT … SELECT` and `VALUES`, a MERGE
  assignment to the leaf, `BY NAME` of a leaf cast to `TIMESTAMP`). pins: zone-seam-1/C-015
  *Fix round 1 (2026-10-22, verifier S1-1):*
  `a_partitioned_overwrite_refuses_a_nested_wall_outside_utc_and_writes_no_file` holds six
  spellings of `INSERT OVERWRITE … PARTITION` on a table partitioned by `k` (a `SELECT` of a
  literal, a `SELECT` of a column of `ice.ns.src`, `VALUES`, a column list, the `TABLE` keyword, a
  dynamic `PARTITION (k)`), each with an instant struct cast to `TIMESTAMP_NTZ` and with a
  `TIMESTAMP_NTZ` value, in New York, Sydney and Kathmandu: the whole refusal text, the data file
  count unchanged from the seed's, no value stored. Red on `58321e94` (all 36 cells stored);
  `a_partitioned_overwrite_in_utc_stores_the_utc_read` is the UTC control on the same twelve
  writes. pins: zone-seam-1/C-016, C-017
  *Fix round 1 (2026-10-22, verifier S1-2):* `an_update_of_an_inner_struct_with_a_wall_refuses_outside_utc`
  (`UPDATE … SET st.i = named_struct('v', TIMESTAMP_NTZ …)` on `STRUCT<i: STRUCT<v: TIMESTAMP>>`
  refuses naming `st`.`i`.`v`, the seed kept; reds when `type_at` answers the column type for any
  path) and `a_plain_update_of_a_struct_with_a_cast_wall_text_stores_spark_instant` (the plain
  `UPDATE` exemption: `CAST('<wall>' AS TIMESTAMP_NTZ)` in the struct stores Spark's New York
  instant; reds when the exemption is removed). pins: zone-seam-1/C-018
  *Fix round 2 (2026-10-23, verifier round 2 S1-1):*
  `a_partition_column_before_the_struct_still_refuses_outside_utc_and_writes_no_file` puts the
  partition column FIRST (`(k, id, st)`) and in the MIDDLE (`(id, k, st)`) of the schema, for a
  static `PARTITION (k = 0)` and a dynamic `PARTITION (k)` `INSERT OVERWRITE`, each with an
  instant struct cast to `TIMESTAMP_NTZ` and with a `TIMESTAMP_NTZ` value, in New York and
  Sydney: the whole refusal text, the data file count unchanged from the seed's, no value stored
  (16 cells); `a_partition_column_before_the_struct_stores_the_utc_read_in_utc` is the UTC control
  on the same eight writes. The earlier partitioned pins keep the column last, where the struct
  column has one index with or without the partition skip. Red on the verifier's mutants Y2 (the
  router hands the gate an empty clause) and Y3 (a dynamic name skipped as static). pins:
  zone-seam-1/C-021
  *Fix round 3 (2026-10-23, item A):*
  `a_plain_update_with_an_unshifted_wall_beside_the_shifted_cast_refuses_outside_utc`: three
  values (an extract of a container cast beside the cast leaf; an extract leaf with the cast only
  inside an `INT` field's `CASE`; a `TIMESTAMP_NTZ` literal beside the cast leaf) through the
  plain `UPDATE` and the MERGE twin (the control), in New York and Sydney: the whole refusal, the
  data file count unchanged, no value stored. Red on `b74f17e0`: the six plain-`UPDATE` cells
  stored. `a_plain_update_with_two_walls_in_utc_stores_the_utc_read` is the UTC control;
  `a_plain_update_whose_only_wall_is_the_shifted_cast_stores_spark_instant` holds the exemption
  (the text cast stores the New York and Sydney read, the instant cast stores the instant).
  pins: zone-seam-1/C-024
  *Fold 4 (2026-10-23, verifier round 3 S1-2):*
  `a_plain_update_whose_only_naive_leaf_is_a_try_cast_stores_spark_instant` holds the `TRY_CAST`
  half of the exemption in New York: wall text stores the New York read, bad text stores `NULL`,
  an instant stores the instant, and the two-leaf `(CAST(instant), TRY_CAST(wall))` stores both;
  mutant Z6 (only the `CAST` name exempts) reds it. pins: zone-seam-1/C-028
- [ntz_nested_refuse_casts.rs](ntz_nested_refuse_casts.rs) — **ICE-NTZ-NESTED-REFUSE-1, fix
  round 1 (2026-10-20, after the verifier's FAIL on PR #1124):**
  `a_container_cast_of_an_instant_refuses_on_every_door_outside_utc`: a `CAST` or `TRY_CAST`
  of a whole struct or array holding an instant, through fourteen doors (`INSERT … SELECT`,
  `VALUES`, `TRY_CAST`, a cast of a zoned struct column, MERGE insert values, MERGE update set
  and `UPDATE SET *`, the plain `UPDATE`, a static `PARTITION`, a branch, a temporary view, a
  catalog view, `REPLACE WHERE` from a frame view, an array) in New York and Kolkata: the
  refusal, no file, no snapshot. `a_container_cast_of_an_instant_stores_the_wall_in_a_utc_session`:
  the same doors store the UTC wall in UTC. `the_leaf_cast_and_a_container_cast_of_a_wall_still_store_outside_utc`:
  the hint's leaf cast (struct, array, `transform`, MERGE) stores the session wall, a container
  cast of a wall or a string stores the wall.
  `a_prepared_write_with_a_parameter_that_supplies_the_column_refuses_in_every_zone`: six
  parameterised `PREPARE` forms refuse in UTC and New York; a `PREPARE` that does not supply
  the column, and two with no parameter, prepare and execute.
  `the_plain_update_refuses_a_timestamp_ntz_cast_in_a_whole_struct_outside_utc`: a string,
  `TRY_CAST`, a cast of a wall literal and an array refuse there; a literal, `to_timestamp_ntz`,
  the field assignment and MERGE store the wall; UTC stores the wall, a date cast midnight.
  `the_type_read_never_runs_the_source`: a counting function is called as often in New York as
  in UTC across an `INSERT`, an `UPDATE` and a `MERGE`; refused, it is never called, and no
  snapshot, file or view is left. A map value built by `map(<column>)` is not pinned: DataFusion's
  `map()` constructor over a column value panics (`list array`) on main as well, for any value
  type and in a plain `SELECT`, before any store (MAP-COLUMN-PANIC-1; corrected in fix round 2,
  round 1 blamed the container cast). The round's door matrix and named-shape probe
  (`task/wo/ice-ntz-nested-refuse-1/evidence/`) and its seven hand mutants (ledger §10: each
  one-line revert of a round-1 rule reds one of these pins) back the same rules.
  pins: ice-ntz-nested-refuse-1/C-015, C-016, C-017, C-018, C-019, C-020, C-021
  **Fix round 2 (2026-10-20, after the verifier's round-1 FAIL):**
  `the_plain_update_stores_a_wall_beside_a_zoned_sub_expression`: on the plain `UPDATE` of a
  whole struct, `to_timestamp_ntz(<instant>)` (also over `coalesce` of a zoned column) stores
  the session wall, and a wall beside a zoned column that feeds only an `INT` field or a
  condition stores the wall, in New York and Kolkata, as Spark and main do; round 1 refused all
  four (verifier S1-2). `the_cast_walk_reads_only_what_reaches_the_column`: a container cast
  of an instant in an `EXISTS` of a `WHERE`, in a `WHERE` and in a sibling `ARRAY<STRING>`
  column no longer refuses and the wall stores; through a derived table, a `UNION ALL` branch,
  a join side, `first_value … OVER`, a `GROUP BY`, a temporary view and a MERGE source it still
  refuses and adds no snapshot (verifier S1-3). With the walk disabled, each of the seven
  stores. `a_subquery_in_the_columns_expression_is_read_whole`: a scalar subquery returning a
  container cast of an instant (directly, through a derived table, as a MERGE update value)
  refuses, and so does an `IN` or `EXISTS` subquery holding one inside the column's own
  expression (read whole, fail closed); a scalar subquery of a wall or of the leaf cast stores
  (verifier S1-4: removing the subquery read stores the UTC wall).
  `a_view_the_write_reads_is_inlined_before_the_walk`: a catalog view and a frame view leave no
  scan whose source carries a logical plan, and a SQL temporary view leaves a
  `ReplanningTempView` scan; the premise on which round 1's frame-view branch was removed as
  dead (verifier S3-2).
  **Fix round 3 (2026-10-21, after the verifier's round-2 FAIL, ledger C-033):**
  `the_cast_walk_reads_only_what_reaches_the_column` adds `LATERAL (SELECT a.c AS s)` over a
  derived table that holds a container cast of an instant, on `INSERT` and as a MERGE source:
  each refuses and adds no snapshot (round 2 stored the UTC wall; the verifier's mutant L16, a
  `Subquery` node that stops the walk, stores it again). A lateral that returns only the wall
  sibling of that cast stores the wall, so the walk follows the one outer reference the column
  uses, not the whole left side.
  The same pin now holds every lineage arm that opens a hole when it stops (ledger C-034,
  verifier round-2 S1-3): `SELECT DISTINCT`, a `LIMIT` inside a derived table, a derived table
  read beside a window, `VALUES` whose second row holds the cast, a top-level `ORDER BY`, and a
  lateral whose outer relation shares its alias with an outer relation that holds a wall, and a
  lateral nested in a lateral's body under the same alias (the reference resolves in the
  nearest scope, and past it in the scopes outside it). `a_frame_through_a_repartition_or_an_unmodelled_node_refuses`
  (`ntz_nested_refuse.rs`) writes from a frame view through `repartition` and one through
  `distinct_on` (read whole); each refuses and adds no snapshot. Each pin fails under its
  one-line mutant (L5, L7, L8, L9, L13, L14, L18, R5, R6).
  pins: ice-ntz-nested-refuse-1/C-034
  The over-refusal classes the round lists with their counts (ledger C-035) are held by pins
  already here: the `PREPARE` pins and, in `src/tests/`, `only_a_utc_zone_lets_an_instant_through`.
  pins: ice-ntz-nested-refuse-1/C-035
  `a_view_the_write_reads_is_inlined_before_the_walk` plans through `ReparkSession::sql`, which
  reaches `router::execute_calibrated`, the function the type read's `planned()` calls, on the
  same text (ledger C-040, verifier round-2 S3-3). The verifier rounds' outcomes are C-036 to
  C-039.
  pins: ice-ntz-nested-refuse-1/C-036, C-037, C-038, C-040
  **Owner ruling 1 (2026-10-21, ledger C-041):** `a_subquery_in_the_columns_expression_is_read_whole`
  is renamed `a_scalar_subquery_is_read_by_its_output_and_a_boolean_subquery_is_skipped`. A
  scalar subquery that returns a container cast of an instant (directly, through a derived
  table, as a MERGE update value, over a temporary view, nested, or as an outer column followed
  back out) still refuses and adds no snapshot; a scalar subquery whose cast feeds only its own
  `WHERE`, or a column it does not return, a correlated one whose outer reference feeds only its
  `WHERE`, and an `EXISTS` in the value store the wall in New York and Kolkata. Each storing
  cell refuses on the round-2 walk.
  pins: ice-ntz-nested-refuse-1/C-041
  **Owner ruling 3 (2026-10-21, ledger C-042):** `only_a_utc_zone_lets_an_instant_through`
  (`src/tests/`) now stores in `Etc/UTC`, `Etc/GMT`, `Etc/Universal`, `Etc/Zulu`, `Zulu` and
  `GMT0` beside the spellings it held, and refuses in `Africa/Abidjan`, `Atlantic/Reykjavik`,
  `Europe/London` and the non-zero zones.
  pins: ice-ntz-nested-refuse-1/C-042
  After the rulings (ledger C-043): `a_frame_through_a_repartition_or_an_unmodelled_node_refuses`
  adds a `DISTINCT ON` frame over a SQL temporary view, read whole into the view (mutant M8a),
  and `router::nested_ns::tests::only_a_pure_utc_alias_counts_as_utc` writes its alias list out
  rather than reading the set it tests (mutant Z3).
  pins: ice-ntz-nested-refuse-1/C-043, C-044
  The `PREPARE` pins above also hold fix round 2's corrected statement: a UTC session refuses
  a `PREPARE` of an instant or of a parameter that supplies the column, and nothing else; the
  correct cells that costs are counted in the ledger (verifier S2-3, S2-4).
  The map residue named above is C-026. Not pinned, stated as a residue (CONTAINER-CAST-ZONE-1,
  C-027): a container cast or `from_json(to_json(…))` whose zoned leaf passes through `STRING`,
  `DATE` or `BIGINT`, and the six table-creating writers, store the UTC wall outside UTC on main
  as on this build. The plain `UPDATE` pins above hold C-028's corrected statement.
  The whole-text pin in `ntz_nested_refuse.rs` holds fix round 2's hint, which names
  `col.cast("timestamp_ntz")` for a DataFrame (C-029).
  The round's door matrix and named-shape probe (C-030), its 35 hand mutants, each killed by
  one of these pins or the router's unit test (C-031), and its gates (C-032) back the same
  rules.
  pins: ice-ntz-nested-refuse-1/C-022, C-023, C-024, C-025, C-026, C-027, C-028, C-029, C-030,
  C-031, C-032
  **Fix round 4 (2026-10-21, after the verifier's round-3 FAIL, ledger C-045, C-046):**
  `a_scalar_subquery_returning_the_container_cast_under_an_unnest_refuses` (`ntz_nested_refuse.rs`)
  plans a frame whose projection holds a scalar subquery that returns the container cast of an
  instant, unnests an array column above it (`DataFrame::unnest_columns`, the node a DataFrame
  `explode` builds, read whole), registers the frame as a temporary view and runs
  `INSERT INTO … SELECT … FROM <view>`: it refuses in New York and Kolkata with the hint and adds
  no snapshot. The control, the same frame whose subquery returns the wall, stores the UTC wall.
  The pin fails under mutant M10 (`plan_has` reads a whole-read node with `apply` in place of
  `apply_with_subqueries`), which survived the 144 earlier pins and stored the UTC wall in New
  York on a DataFrame `explode` written through `writeTo(…).append()`, `withColumn` and
  `append`, `write.insertInto` and an `INSERT … SELECT` from a temporary view; the kill is
  recorded in `task/wo/ice-ntz-nested-refuse-1/evidence/m10-kill-round-4.txt`. The earlier
  equivalence claim for M10 (C-043) is superseded by C-046.
  pins: ice-ntz-nested-refuse-1/C-045, C-046
  **Comment strip (fix round 4, ledger C-047):** the `///` lines the unit added to
  `ntz_nested_refuse.rs` (37), `ntz_nested_refuse_casts.rs` (63), `timestamp_ns_nested_shapes.rs`
  (5) and `timestamp_ns_wall_doors.rs` (7) are removed; the rows above hold every test's reason
  and its `pins:` citation. `timestamp_ns_wall_doors.rs`'s main-pin rewrite of 2026-10-20 (the
  six doors now refuse an instant outside UTC with one text and write nothing; UTC stores the
  walls it stored before) and `timestamp_ns_nested_shapes.rs`'s re-pin of the same day (the
  nested `TIMESTAMP_NTZ` row takes R-011's refusal, not the gate's text) are described in the
  paragraphs headed ICE-NTZ-NESTED-REFUSE-1 (2026-10-20) in their rows.
  **ZONE-SEAM-1 (2026-10-22, ruling ZS-4):** the seam rewrites a container cast of an instant
  before the router's gate reads the plan, so R-011's cast arm (`casts_a_zoned_value_to_naive`)
  no longer matches it and the write stores Spark's session wall. Five pins that held the
  refusal now hold the stored value outside UTC, per statement: in `ntz_nested_refuse_casts.rs`,
  `a_container_cast_of_an_instant_stores_the_session_wall_on_every_door_outside_utc` (the 14
  doors of `CONTAINER_CASTS`, a fresh catalog each), `the_cast_walk_reads_only_what_reaches_the_column`
  (each `ON_LINEAGE` statement into its own table: the session wall, beside `w`'s UTC wall for the
  `UNION ALL` and the `VALUES` rows) and
  `a_scalar_subquery_is_read_by_its_output_and_a_boolean_subquery_is_skipped` (each
  `IN_A_SUBQUERY` statement into its own table); in `ntz_nested_refuse.rs`,
  `a_frame_through_a_repartition_or_an_unmodelled_node_stores_the_session_wall` and
  `a_scalar_subquery_returning_the_container_cast_under_an_unnest_stores_the_session_wall`. Two
  statements (`(SELECT q.s)` over a derived table, and a `LATERAL` inside a `LATERAL`) fail on
  the head with the engine's own error; the pin holds that the same statement with the wall
  `named_struct('v', w, 'n', 1)` in place of the cast fails with the same text, so the failure is
  not the seam's. The plain `UPDATE` refusal (`shifts_in_an_update`) and every other refusal in
  both files are unchanged. pins: zone-seam-1/C-009
  pins: ice-ntz-nested-refuse-1/C-047
  **Count (fix round 4, ledger C-048):** `docs/spark-sql-iceberg-parity.md` R-011 counts ruling 2's
  over-refused correct cells as 16 in 8 shapes: the verifier's scalar-subquery shape, `INSERT INTO
  ice.ns.t SELECT id, (SELECT named_struct('v', w, 'n', size(CAST(array(x) AS
  ARRAY<TIMESTAMP_NTZ>))) FROM ice.ns.src LIMIT 1), 0 FROM ice.ns.src`, joins the sibling-field
  class (2 cells, New York and Kolkata; Spark and main store the wall, the build refuses). The
  count covers the shapes measured and the class is open-ended.
  pins: ice-ntz-nested-refuse-1/C-048
  **Gates (fix round 4, ledger C-049):** on the merged head the 3168-cell door matrix and the
  84-cell named-shape probe answer cell for cell as the committed head files (360 and 48 moved
  against main, every one to the refusal), and the unit's live module passes 67 of 67.
  pins: ice-ntz-nested-refuse-1/C-049
  **Owner ruling (2026-10-21, ledger C-050):** the New York nested-NTZ `INSERT` cell, which reads
  1.04–1.09x of main across four measurements (the type read's planning, about 3 ms per
  statement), is accepted as cost until GATE-RETIRE-1; the 1.05x gate does not move.
  pins: ice-ntz-nested-refuse-1/C-050
  **Cost ruling restated (2026-10-21, ledger C-051):** the ruled cell is an `INSERT` of a
  `named_struct` holding a `TIMESTAMP_NTZ` value in a non-UTC session zone (New York measured).
  It reads 1.00–1.15x of main across six measurements (builder 1.082 and 1.038, verifier 1.090
  and 1.050, fix round 4 1.147 and 1.004; main's 10-run rounds spread 24–33 ms, so the cell is
  bimodal), at or over the 1.05x gate on four of them; R-011 and the v1.5.6 notes draft state
  all six.
  pins: ice-ntz-nested-refuse-1/C-051
  **ICE-TSNS-TYPE-PLANNER-1 re-pin (R-017, ruling 100, 2026-10-15):** R-017's pin moves with
  R-017's fix: `an_update_with_no_where_stores_a_value_beside_a_null_as_its_source_stores`
  (was `an_update_with_no_where_refuses_a_value_narrowed_from_nanoseconds`) asserts that
  `UPDATE … SET v = CASE WHEN id > 0 THEN c ELSE NULL END` and its three sibling spellings
  (`NULL_SIBLING_UPDATES`) store what `SET v = c` stores, nine digits, for a `timestamp_ns`
  and a `timestamptz_ns` source. Oracle: Spark 4.1.2's microsecond twin of the four statements
  (Iceberg 1.11.0, New York, 2026-10-15) stores `c` unchanged; a NULL takes its sibling's
  type. pins: ice-tsns-type-planner-1/C-033
  **EXPLAIN-DML-WRITE-1 (2026-10-21, ruling 141):** the three prepared `INSERT` controls of
  `a_prepared_write_with_a_parameter_that_supplies_the_column_refuses_in_every_zone` now take the
  ruled `PREPARE` text (never the NTZ hint), their `EXECUTE`s fail and nothing is stored.
  pins: explain-dml-write-1/C-008
- [decimal_float_coercion.rs](decimal_float_coercion.rs) — WO-2 (xo-muse8 UNIT1
  fix-b): a decimal literal against a DOUBLE/FLOAT column widens the literal to
  DOUBLE (`d = CAST(0.0 AS DOUBLE)`, Spark's analyzed shape), never the column to
  decimal — so NaN rows answer instead of `Overflowing on NaN`. Forty-three
  live-Spark-4.1.2-measured row pins over a NaN/-0.0/null-bearing DOUBLE, FLOAT
  and DECIMAL(6,2) fixture: 26 match Spark (six comparison operators, IN/NOT IN,
  BETWEEN/NOT BETWEEN, negatives, reversed sides, FLOAT with the 0.1f32
  double-widening discriminator, decimal-vs-decimal/decimal-vs-integral
  near-miss controls) and 17 pin the separate pre-existing signed-zero kernel
  divergence (the float eq kernel keeps `-0.0` distinct from `0.0`, so zero-bound
  `=`/`<>`/`<`/`>=`/IN/BETWEEN cases differ from Spark by exactly the `-0.0`
  row), each with Spark's answer in the assertion message. One logical-plan
  shape pin proves no decimal cast remains.
- [ta_window.rs](ta_window.rs) — seven
  `sql_route_*` cases prove the TA window
  UDFs the composed `repark_ta::TaExtension` registers are `f64::to_bits`-identical to the
  `repark_ta` kernels on the crate's own 5000-row OHLC goldens (`../../repark-ta/tests/goldens/*.bin`
  — read, never re-recorded), across single/scalar-param/multi-series/parked-four families,
  `PARTITION BY` scoping, a 12k multi-batch partition, and the non-literal-period refuse.
  Three plan-shape pins cover named `OVER w` and inline same-spec
  `ta_*(…) OVER (PARTITION BY … ORDER BY …)` each plan one `WindowAggExec` (EXPLAIN +
  `create_physical_plan`); an intervening filter between two live windows stacks two
  (`sql_intervening_filter_between_windows_stacks_window_agg_exec`). Ledger:
  [../../../task/ta1-sql-fusion-ledger.md](../../../task/ledgers/archive/2026-08/2026-08-15-ta1-sql-fusion-ledger.md).

## I want to...

| ...do this | go to |
|---|---|
| See the door-installed session end-to-end pin | [session_extension.rs](session_extension.rs) |
| See test ownership notes | [map.md](map.md) |
| Read the extension under test | [../src/extension.rs](../src/extension.rs) |
| See the TA SQL route pinned bit-exact | [ta_window.rs](ta_window.rs) |
| See the session-timezone class pinned at the Spark door + DataFrame API | [session_timezone.rs](session_timezone.rs) |
| See the same class at the ANSI door | [../../repark-sql/tests/session_timezone_ansi_door.rs](../../repark-sql/tests/session_timezone_ansi_door.rs) |
| See the same class at the facade | [../../../python/repark/tests/timestamps/test_session_timezone_parity.py](../../../python/repark/tests/timestamps/test_session_timezone_parity.py) |
| See the timestamp-cast epoch-seconds class at the Spark door + DataFrame API | [timestamp_cast_seconds.rs](timestamp_cast_seconds.rs) |
| See that class at the ANSI door | [../../repark-sql/tests/timestamp_cast_ansi_door.rs](../../repark-sql/tests/timestamp_cast_ansi_door.rs) |
| See that class at the facade | [../../../python/repark/tests/timestamps/test_timestamp_cast_parity.py](../../../python/repark/tests/timestamps/test_timestamp_cast_parity.py) |

## Pointers

- Up: [../map.md](../map.md)
- Unit-level batteries live beside their modules under [../src/](../src/map.md); this
  directory is only for whole-session assemblies.
- `session_extension.rs` covers the session extension seam.

## Debug

- `cargo test -p repark-spark --test session_extension` (or `--test ddl_sessions`) runs one
  file. Never `--all-features` (AGENTS.md PyO3 note).
- `ddl_sessions.rs` failures usually mean a DDL handler regressed (ctas / namespace_ddl /
  catalog_ops), not the session seams — reproduce via the equivalent `session.sql` statement.
- `ta_window.rs` "missing fixture …" means the goldens moved: the path is
  `$CARGO_MANIFEST_DIR/../repark-ta/tests/goldens`, i.e. repark-ta's sibling position in the
  workspace. A *bit* mismatch is an engine/UDF regression, never a goldens edit — see
  [../../repark-ta/map.md#debug](../../repark-ta/map.md).
- `sql_*_window_agg_exec` RED: DataFusion's same-OVER fusion / intervening-filter stacking
  moved. The pin records measured truth (`task/ta1-sql-fusion-ledger.md`); do not "fix"
  the engine in this lane, and do not drop `ema5` from the stacked SELECT (DCE then
  collapses the count to 1).
- Week-53 assertion is ISO-week semantics (2021-01-01 → ISO week 53 of 2020) — a failure there
  is the date shim regressing, not the fixture.
