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

## Mutation record (2026-09-25)

Each line was broken, the named tests ran, and the file was restored.

| # | Mutation | Red |
|---|---|---|
| M1 | drop the `"timestamp_ltz"` arm of `iceberg_named_primitive` | Rust `a_timestamp_ltz_column_is_an_iceberg_timestamptz_on_v2_and_v3`, `a_timestamp_ltz_column_writes_reads_and_partitions_by_day`; facade `[ltz]` (CREATE refuses) |
| M2 | stop calling `plan_timestamp_ltz_literal_regions` from `plan_keyword_regions` | Rust `a_timestamp_ltz_typed_literal_is_a_session_zone_instant`; facade `[ltz]` (`…/literal-cols`) |

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
      artifacts: [target/probe-u9-types-1/red-M1.txt, target/probe-u9-types-1/red-M2.txt]
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
