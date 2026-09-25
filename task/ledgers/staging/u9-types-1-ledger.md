# Unit ledger — WO U9-TYPES-1 · MAP, TIMESTAMP_LTZ, VOID (v3 unknown), UUID read

**Date:** 2026-09-25 · **Branch:** `feat/u9-types-1` · **Base:** `4ae73c2c`
(`origin/main`) **Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Four TYPES cells of the 2026-09-25 scoreboard run answered wrong on the Spark door:
`TY-TIMESTAMP-LTZ` and `TY-UNKNOWN-VOID` refused their column type at CREATE, `TY-UUID-READ`
refused `ALTER TABLE … ADD COLUMN u UUID`, and `TY-MAP` failed its INSERT on the empty map
literal `map()` (`Function 'map' expected at least one argument but received 0`).

**What Spark does (measured 2026-09-25; every measurement is committed as
`python/repark/tests/u9_types_1_spark_oracle.json`, generator
`target/probe-u9-types-1/build_oracle.py` + `merge_oracle.py` over `steps.py`; the first sweep
is `target/probe-u9-types-1/spark.json`, 204 probes).** A `TIMESTAMP_LTZ` column is Iceberg
`timestamptz` on v2 and v3, whatever `spark.sql.timestampType` says, and reads as Spark
`timestamp`; `TIMESTAMP_LTZ '…'` is a session-zone instant literal. `map()` is an empty
`map<void,void>` that coerces to the target map type. A `VOID` column is Iceberg `unknown` on
v3 only (v1/v2 refuse `Invalid schema for v<N>: - Invalid type for c: unknown is not supported
until v3`), reads NULL and describes as `void`. A `uuid` column (Spark cannot declare one in
SQL: `[UNSUPPORTED_DATATYPE] Unsupported data type "UUID"`) reads and writes as a canonical
lower-case `string`.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | A `TIMESTAMP_LTZ` column is Iceberg `timestamptz` on format v2 and v3: top level, a struct field, an array element and a map value, at CREATE and at `ADD COLUMN`, also under `spark.sql.timestampType=TIMESTAMP_NTZ`. It reads as Spark `timestamp` in `cols`, `DESCRIBE`, `dtypes` and `printSchema`, and `ALTER COLUMN c TYPE TIMESTAMP_LTZ` is a no-op commit. | Metadata schema and every schema surface on the facade; the Rust door schema. | PROVEN | Facade replay `ltz/v2/md`, `…/cols`, `…/describe`, `…/dtypes`, `…/printschema`, `ltz/v3/*`, `…/struct-md`, `…/addcol-md`, `…/alter-to-ltz`, `ltz/ntzconf/md` (the `c` field; `d` is R-5); Rust `a_timestamp_ltz_column_is_an_iceberg_timestamptz_on_v2_and_v3`. Cell `TY-TIMESTAMP-LTZ` replays EQUAL on every observation. |
| C-002 | `TIMESTAMP_LTZ '…'` (any case, with or without a space) is Spark's session-zone instant literal: type `timestamp`, the wall read in `spark.sql.session.timeZone` (UTC and America/New_York), usable in `VALUES`, comparisons and `CAST(… AS STRING)`. A `timestamp_ltz` identifier that is not followed by a string literal keeps its meaning, and `CAST(… AS TIMESTAMP_LTZ)` is unchanged. | Rows and types on the facade and the Rust door; token-planner unit pins. | PROVEN | Facade replay `ltz/v2/literal-cols`, `…/literal-rows`, `…/insert-ltz-literal`, `…/where-ltz-literal`, `ltz/ny/insert-ny-literal`, `ltz/ny/where-ltz`, `ltz/ntzconf/literal-cols`; Rust `a_timestamp_ltz_typed_literal_is_a_session_zone_instant`, `a_timestamp_ltz_word_that_is_not_a_typed_literal_keeps_its_meaning`, `spark_rewrites::timestamp_ltz_literal` tests. |
| C-003 | Reads and writes of a `TIMESTAMP_LTZ` column answer as Spark: rows, NULL, `IS NOT NULL`, range and string-literal comparisons, `ORDER BY` both ways, `CAST` to `STRING`, a `DATE` insert, a DataFrame `writeTo(...).append()`, the `.files` bounds and counts, `days` / `hours` / identity + `months` partitioning and the `.partitions` table, and the session-zone display under America/New_York. | Rows per shape on the facade; the Rust door write, filter and `days` spec. | PROVEN | Facade replay of the remaining `ltz/*` steps; Rust `a_timestamp_ltz_column_writes_reads_and_partitions_by_day`. |
| C-004 | The ANSI door keeps refusing the Spark spelling `TIMESTAMP_LTZ` in a column definition (`Unsupported SQL type TIMESTAMP_LTZ`) and creates no table. | Exact text on the ANSI door. | PROVEN | `crates/repark-sql/tests/ansi_door_u9_types.rs::the_spark_timestamp_ltz_spelling_refuses_on_the_ansi_door`. |
| C-005 | Every `ltz/*` step that does not answer as Spark carries its residue record (R-1..R-5) with both answers, and the replay holds RePark to that record. | The replay's residue arm; the residue-table cross-check. | PROVEN | `test_every_step_answers_as_spark_measured_or_as_its_dated_residue[ltz]`, `test_every_residue_names_a_ledger_residue_and_differs_from_spark`. |
| C-006 | `map()` (any case, no arguments, unqualified) is Spark's empty map: type `map<void,void>`, value `{}`, `size` 0, a Python `dict`; it writes into a `MAP<…>` column through `INSERT … VALUES` (beside `map('k', 1)` and `NULL`), `INSERT … SELECT`, `CAST(map() AS MAP<…>)`, a nested `MAP<STRING, MAP<…>>` value, `CASE`, `UNION ALL` and `map_concat`. A qualified `s.map()` and `map(k, v, …)` are unchanged. | Rows and types on the facade and the Rust door; AST unit pin. | PROVEN | Facade replay `map/v2/insert`, `map/v3/insert`, `map/literal/*` (EQUAL steps), `map/nested/insert`, `map/v2/insert-select-empty`, `…/insert-empty-typed`; Rust `an_empty_map_call_is_an_empty_map_of_void`, `a_map_column_takes_literals_the_empty_map_and_null`, `keyword_lower::an_empty_map_call_lowers_to_a_map_of_two_empty_arrays`. Cell `TY-MAP` replays EQUAL on every observation. |
| C-007 | A `MAP<STRING, INT>` column answers as Spark on v2 and v3: metadata `map<string,int>`, `cols` / `DESCRIBE` / `dtypes` / `printSchema`, rows, `IS NOT NULL`, `c['k']` in a filter and a projection, `element_at`, `map_keys` / `map_values`, `size`, `CAST` to `MAP<STRING, BIGINT>`, a DataFrame `writeTo(...).append()` of dicts, the `.files` counts, `ADD COLUMN` of a map, `ALTER COLUMN c.value TYPE BIGINT`, and a `STRUCT` or `MAP` value type. | Rows per shape on the facade. | PROVEN | Facade replay of the EQUAL `map/*` steps. |
| C-008 | The ANSI door keeps its own spellings: `MAP(ARRAY['k'], ARRAY[1])` and the empty `MAP(ARRAY[], ARRAY[])` write into a `MAP(VARCHAR, INTEGER)` column, and the Spark spelling `MAP()` refuses `Function 'map' expected at least one argument but received 0`. Every `map/*` step that does not answer as Spark carries its residue record (R-6..R-13). | Exact rows and text on the ANSI door; the replay's residue arm. | PROVEN | `crates/repark-sql/tests/ansi_door_u9_types.rs::the_ansi_empty_map_is_map_of_two_empty_arrays_and_the_spark_spelling_refuses`; `test_every_step_answers_as_spark_measured_or_as_its_dated_residue[map]`. |
| C-009 | A `VOID` column on a format-v3 table is Iceberg `unknown`: CREATE and `ADD COLUMN` commit, `INSERT … VALUES (0, NULL)` writes (no parquet column, as Java's `TypeToMessageType` emits none), the column reads NULL, describes as `void` (`cols`, `DESCRIBE`, `dtypes`), a non-NULL value refuses `CANNOT_SAFELY_CAST`, and v1 / v2 refuse `Invalid schema for v<N>: - Invalid type for c: unknown is not supported until v3` (cell `TY-UNKNOWN-VOID`). | The `void/*` steps replay EQUAL. | OPEN (question Q1 of the hand-back: the fork's parquet writer refuses `Writing the unknown column 'c' is not supported yet` on every INSERT, and its `readable_metrics` refuses `unknown`; measured with the DDL mapping enabled, `target/probe-u9-types-1/repark1.json` keys `void/*`) | Spark measured (group `void`, 21 steps); RePark keeps the loud CREATE / ADD COLUMN refusal, held by residue R-14 on every divergent step. |
| C-010 | A `uuid` column written through the Iceberg API reads and writes as Spark `string` (`cols`, `DESCRIBE`, a Python `str`): a canonical string literal writes, an upper-case one stores lower case, NULL writes, `u = '…'` filters on the canonical text, and a DataFrame append of a `STRING` column writes (cell `TY-UUID-READ`). | The `uuid/*` steps replay EQUAL. | OPEN (question Q2 of the hand-back: where the uuid ↔ string presentation lives, and whether the Spark door accepts the `UUID` type name that Spark itself refuses, as the scoreboard's RePark leg requires) | Spark measured (group `uuid`, 12 steps, and `target/probe-u9-types-1/spark.json` keys `uuid/*`); RePark keeps its loud refusal, held by residue R-15 on every divergent step. |

## Mutation record (2026-09-25)

Each line was broken, the named tests ran, and the file was restored.

| # | Mutation | Red |
|---|---|---|
| M1 | drop the `"timestamp_ltz"` arm of `iceberg_named_primitive` | Rust `a_timestamp_ltz_column_is_an_iceberg_timestamptz_on_v2_and_v3`, `a_timestamp_ltz_column_writes_reads_and_partitions_by_day`; facade `[ltz]` (CREATE refuses) |
| M2 | stop calling `plan_timestamp_ltz_literal_regions` from `plan_keyword_regions` | Rust `a_timestamp_ltz_typed_literal_is_a_session_zone_instant`, `a_timestamp_ltz_column_writes_reads_and_partitions_by_day`; facade `[ltz]` (`…/literal-cols`) |
| M3 | drop the `is_empty_map_call` arm of `lower_expression` | Rust `an_empty_map_call_is_an_empty_map_of_void`, `a_map_column_takes_literals_the_empty_map_and_null`, `an_empty_map_call_lowers_to_a_map_of_two_empty_arrays`; facade `[map]` |

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: u9-types-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause comes from the Spark measurement committed as the oracle; the TY cells replay through the scoreboard harness and each oracle step replays through the facade, EQUAL or held by its residue record.
      artifacts: [python/repark/tests/u9_types_1_spark_oracle.json, python/repark/tests/test_u9_types_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Each type is measured at CREATE (v1/v2/v3 where they differ), ADD COLUMN, nested positions, literals and NULLs, reads, filters, ordering, casts, DataFrame appends, partition transforms, ALTER COLUMN TYPE, the files table and a non-UTC session zone.
      artifacts: [target/probe-u9-types-1/steps.py, crates/repark-spark/src/tests/u9_timestamp_ltz.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Refused steps carry class, condition, SQLSTATE and text; a refused CREATE leaves no table (the replay reads the next step against the same catalog).
      artifacts: [python/repark/tests/test_u9_types_1.py, crates/repark-sql/tests/ansi_door_u9_types.rs]
    - id: AT-4
      status: N/A
      justification: No commit, isolation or concurrency path changes; the unit maps type names and rewrites literals before planning.
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling; the literal rewrite swaps one keyword token and re-parses through the door's parser.
    - id: AT-6
      status: ATTACKED
      evidence: The type mapper and canonicalize layer suites (create_table, keyword_lower, spark_rewrites, spark_literals) stay green; a timestamp_ltz identifier and a cast target keep their meaning.
      artifacts: [crates/repark-spark/src/spark_rewrites/timestamp_ltz_literal.rs, crates/repark-spark/src/tests/u9_timestamp_ltz.rs]
    - id: AT-7
      status: ATTACKED
      evidence: The literal planner is one linear pass over tokens already produced by canonicalize; statements without a quote keep the borrowed fast path.
      artifacts: [crates/repark-spark/src/spark_rewrites/timestamp_ltz_literal.rs]
    - id: AT-8
      status: ATTACKED
      evidence: No new dependency, crate edge or code comment; spark_literals.rs stays at 999 lines (one call renamed), router.rs untouched.
      artifacts: [crates/repark-spark/src/spark_literals.rs, crates/repark-spark/src/spark_rewrites/mod.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The registry gains one row per cell; residues cite the registry rows they belong to (TZ-6, TZ-7).
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Each load-bearing line was broken and its named tests ran red (mutation table).
      artifacts: [target/probe-u9-types-1/red-M1.txt, target/probe-u9-types-1/red-M2.txt, target/probe-u9-types-1/red-M3.txt]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-1 | Pre-existing (a bare `TIMESTAMP` column answers the same): a STRING literal in `INSERT … VALUES` into a `TIMESTAMP_LTZ` column writes it (`ltz/v2/side-insert-string`, `ltz/v3/side-insert-string`, and the rows of `…/side-select`), where Spark refuses `[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for the table … Cannot safely cast `c` "STRING" to "TIMESTAMP". SQLSTATE: KD000`. |
| R-2 | Declared (registry TZ-6, dated note 2026-09-16): the SQL door refuses the `TIMESTAMP_NTZ` type name, so `TIMESTAMP_NTZ '…'` into an LTZ column and `CAST(c AS TIMESTAMP_NTZ)` answer `[UNSUPPORTED_TIMESTAMP_NTZ] … SQLSTATE: 0A000` where Spark writes and casts (`ltz/v2/side-insert-ntz`, `ltz/v3/side-insert-ntz`, `ltz/v2/cast-ntz`). |
| R-3 | `ALTER COLUMN c TYPE TIMESTAMP_NTZ` / `STRING` refuse with Iceberg's `Cannot change column type: c: timestamptz -> timestamp` (`-> string`) as `PySparkException` `DataInvalid => …`, where Spark raises `Py4JJavaError` wrapping `SparkException: Unsupported table change: …` (`ltz/v2/alter-to-ntz`, `ltz/v2/alter-to-string`). |
| R-4 | Ruled (registry TZ-7, Q12): `collect()` of an LTZ column in a non-UTC session answers the session-zone wall (`2023-12-31T19:00:00` for `2024-01-01T00:00Z` under America/New_York), where Spark answers the instant in the Python host zone (`ltz/ny/select`; the host is UTC in the harness). |
| R-5 | Declared (registry TZ-6 residual, Q10): under `spark.sql.timestampType=TIMESTAMP_NTZ` a bare `TIMESTAMP` column is still `timestamptz`, where Spark stores `timestamp` (`ltz/ntzconf/md`, field `d`; the `TIMESTAMP_LTZ` field `c` answers as Spark). |
| R-6 | A map compares and sorts: `WHERE c = map('k', 1)` answers `[[0]]` and `ORDER BY c` answers rows, where Spark refuses `[DATATYPE_MISMATCH.INVALID_ORDERING_TYPE] … does not support ordering on type "MAP<STRING, INT>". SQLSTATE: 42K09` (`map/v2/where-eq`, `map/v2/order`). |
| R-7 | `CAST(<map> AS STRING)` refuses `Unsupported CAST from Map(…) to Utf8View`, where Spark renders `{k -> 1}`, `{a -> 2, b -> null}`, `{}` (`map/v2/cast-string`). The complex-to-string cast family is wider than maps: `CAST(array(1, NULL) AS STRING)` renders `[1, ]` for Spark's `[1, null]` and a struct refuses where Spark renders `{1, x}` (`target/probe-u9-types-1/spark-extra.json`). |
| R-8 | Refusals whose condition and SQLSTATE match but whose text does not: `CAST('{k -> 1}' AS MAP<…>)` quotes `Utf8("{k -> 1}")` for Spark's `"{k -> 1}"`, and both it and the `c.key` type change carry RePark's `Error during planning: ` prefix without Spark's position and plan tail (`map/v2/cast-from-string`, `map/v2/alter-key-type`). |
| R-9 | Invalid map values in `INSERT … VALUES` refuse with DataFusion's text as `PySparkException`: `map(1, 'a')` into `MAP<STRING, INT>` (`Cannot cast string 'a' to value of Int32 type`), `map(NULL, 1)` (`map key cannot be null`), `map('d', 1, 'd', 2)` (`map key must be unique, duplicate key found: d`), where Spark raises `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` / `KD000` and `INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION` / `42000` (`map/v2/insert-wrong-type`, `…/insert-null-key`, `…/insert-dup-key`). |
| R-10 | Partitioning by a map refuses with the fork's `DataInvalid => Cannot partition by non-primitive source field: 'map'.` where Spark raises `ValidationException: Cannot partition by non-primitive source field: map<string, int>` (`map/v2/partition`, `map/v2/create-partitioned`). |
| R-11 | `ALTER COLUMN c TYPE MAP<STRING, BIGINT>` refuses `ALTER COLUMN `c` TYPE to a non-primitive is not supported`, where Spark commits the value promotion (`map/v2/alter-map-type`; `ALTER COLUMN c.value TYPE BIGINT` answers as Spark). |
| R-12 | A field access after a subscript, `c['k'].a`, refuses `Dot access not supported for non-string expr`, where Spark answers the field (`map/struct/field`); `array(named_struct('a', 1))[0].a` refuses the same way where Spark answers `1` (`spark-extra.json`). |
| R-13 | `map()` does not unify with a typed map inside `coalesce`, `if`, `array(…)` or a `VALUES` table (DataFusion's coercion has no `Map<Null, Null>` arm), where Spark answers `map<string,int>` (`map/literal/coalesce`, `…/coalesce-cols`, `…/if`, `…/array-of-maps`, `…/values`). `CASE`, `UNION ALL`, `map_concat` and INSERT targets answer as Spark (C-006). |
| R-14 | Held for question Q1 (fork write of `unknown`): the Spark door keeps refusing `VOID` at CREATE and `ADD COLUMN` (`This feature is not implemented: column type `VOID` is not supported yet for Iceberg tables`) and `CAST(NULL AS VOID)` (`Unsupported SQL type VOID`), so every `void/*` step that follows answers from the missing table, where Spark answers as C-009 states (18 of 21 `void/*` steps). Measured with the DDL mapping enabled (2026-09-25, `repark1.json`): the fork refuses the INSERT, `DESCRIBE` renders `Null` for `void`, and v2 answers the fork's `DataInvalid => Invalid schema for v2: …` text as `PySparkException` where Spark raises `Py4JJavaError` wrapping `IllegalStateException`. |
| R-15 | Held for question Q2 (uuid as string): the Spark door keeps refusing `UUID` at CREATE and `ADD COLUMN` (`column type `UUID` is not supported yet for Iceberg tables`) and in `CAST` (`Unsupported SQL type UUID`), where Spark refuses the SQL spelling `[UNSUPPORTED_DATATYPE] Unsupported data type "UUID". SQLSTATE: 0A000` but reads and writes an API-created column as `string` (11 of 12 `uuid/*` steps). Mapping the type alone is not enough: the fork's `schema_to_arrow_schema` (`crates/iceberg/src/arrow/schema.rs` at pin `b2698e56`) gives `uuid` Arrow `FixedSizeBinary(16)`, and the provider advertises that schema, not `Utf8`. Also measured on Spark (`spark.json`): `'not-a-uuid'` and `7` refuse at the task with `IllegalArgumentException: Invalid UUID string: …`, `ORDER BY u` sorts by the canonical text's bytes, `bucket(4, u)` partitions, `ALTER COLUMN u TYPE STRING` is a no-op commit, and `.files` `readable_metrics.u` fails `ClassCastException: Cannot cast java.util.UUID to java.lang.CharSequence`. |
