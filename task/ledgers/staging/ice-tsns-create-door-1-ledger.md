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
| C-003 | `asked_nanosecond_columns` answers one flag per output column by the origins rule. | Slice 2. | **OPEN** | |
| C-004 | `repark.sql.allowImplicitTimestampNanos` is a session setting, default off. | Slice 2. | **OPEN** | |
| C-005 | A query's Arrow schema becomes Iceberg columns only through `nanosecond_door`; a new nanosecond column that is not asked is refused there with one text. | Slice 3. | **OPEN** | |
| C-006 | Every create and evolution door refuses an unasked nanosecond column and leaves the table as it was; a written cast and the setting create what main created. | Slice 3. | **OPEN** | |
| C-007 | A statement with no nanosecond output answers what main answered. | Slice 3. | **OPEN** | |
| C-008 | A nanosecond type below format v3 is refused with one text that names the remedy. | Slice 4. | **OPEN** | |
| C-009 | The parity registry carries the dated rows and this ledger the release-note sentence. | Slice 6. | **OPEN** | |
| C-010 | The cost gate holds at 1.05 and the work order's gates pass. | Real ratios and exit codes. | **OPEN** | |

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
