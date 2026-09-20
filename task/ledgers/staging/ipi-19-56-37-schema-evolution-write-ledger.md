# Unit ledger — IPI-19 + IPI-56 + IPI-37 · schema evolution on write

**Date:** 2026-09-20 · **Branch:** `fix/ipi-19-56-37-schema-evo-write` · **Base:** `e4160a58`
(`origin/main`) **Model:** Claude Opus 5 (max) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Owner direction 2026-09-18 is 1:1 parity with Spark's Iceberg integration, and
2026-09-19 makes full Iceberg support the v1.5.0 gate. Twelve inventory cells answer in Spark and
refuse or silently mis-answer in RePark, all on one question: *what happens when the incoming
data's schema differs from the table's?* Three of them are silent-to-the-user refusals on the
DataFrame `mergeInto` builder that have nothing to do with schema evolution — Spark's own
condition form qualifies the target by its **short table name** and RePark answered
`No field named <table>.id`.

**What it is.** One decision seam in Rust: `write.spark.accept-any-schema` gates every evolving
write except `MERGE WITH SCHEMA EVOLUTION`; the incoming schema is merged into the table's with
the fork's `UpdateSchemaAction::union_by_name_with` (Java `unionByNameWith`); the schema update
and the data commit ride **one** `Transaction`, the schema action first, and the data files are
written against the **evolved** schema.

**Not in this unit:** positional `INSERT … VALUES` and `INSERT INTO t SELECT *` with extra
columns (IPI-17's `W-ACCEPT-ANY-INSERT-VALUES` / `W-INSERT-MERGE-SCHEMA-CONF`, awaiting their own
ruling); `write.spark.accept-any-schema` anywhere but as this gate (owner decision 8);
`STATUS.md`; `Cargo.toml` / `Cargo.lock`; the fork; any raised size ceiling.

**Writable paths:** `crates/repark-spark/src/`, `crates/repark-iceberg/src/write/`,
`crates/repark-python/src/session_runtime.rs`, `python/repark/src/repark/spark/`,
`python/repark/tests/`, `docs/spark-sql-iceberg-parity.md`, this ledger, touched `map.md` files.

## Measured

Spark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0` over an `InMemoryCatalog`, recorded by run
26e into `/tmp/oc-worker/qe/probe/p2.json` (keys `D.*`) and by the parity inventory into
`/tmp/oc-worker/nc-inventory/out/*.json` (the twelve cells). Every expected value in
`python/repark/tests/test_ice_merge_schema_1.py` is one of those recordings.

| # | Fact | Evidence |
|---|---|---|
| M-1 | `mergeSchema=true` REQUIRES `write.spark.accept-any-schema='true'`; without it the write fails with `[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS]` / `21S01` and the table is unchanged | `D.mergeSchema.ms_plain` |
| M-2 | With the property the new column lands **last, optional**; existing rows read NULL | `D.mergeSchema` (the accept-any-schema row), `.rows` |
| M-3 | `writeTo(t).option("mergeSchema","true").append()` behaves identically, and fails identically without the property | `D.writeTo.mergeSchema` |
| M-4 | A frame **missing** a table column succeeds and writes NULL; the schema is unchanged — `mergeSchema` is *union by name*, not *replace* | `D.mergeSchema.missing` |
| M-5 | Type widening does NOT narrow the table: an INT source into a BIGINT column leaves `LongType` | `D.mergeSchema.widen` |
| M-6 | `MERGE WITH SCHEMA EVOLUTION` does NOT need the property; it adds the column with the **source's** type, unwidened (`IntegerType`), optional, last | `D.mse` |
| M-7 | `MERGE WITH SCHEMA EVOLUTION` with no new column is an ordinary upsert; the schema is unchanged | `D.mse_nonew` |
| M-8 | A plain `MERGE INTO` whose source carries an extra column **succeeds**; the extra column is ignored | `D.mse_plain_no_evo` |
| M-9 | `spark.sql.iceberg.merge-schema` only matters when the table carries the property. (no property, conf false) and (no property, conf true) → the arity error; (property, conf false) → `IllegalArgumentException: Field extra not found in source schema`; (property, conf true) → ok, `extra` `IntegerType` last | `D.sql_by_name.*` and its accept-any-schema twin |

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The DataFrame write option `mergeSchema` (and its Iceberg spelling `merge-schema`) adds the frame's extra columns to the table in **one** snapshot: optional, last, carrying the frame's own type, with existing rows reading NULL — on `saveAsTable(mode="append")` and on `writeTo(t).append()` alike. | The three DataFrame cells, schema and rows and snapshot count asserted. | OPEN | Cells `W-DF-OPT-MERGE-SCHEMA-NAMED`, `W-DF-OPT-MERGE-SCHEMA-ICEBERG-NAMED`, `W-DF-V2-MERGE-SCHEMA`. |
| C-002 | Without `write.spark.accept-any-schema='true'` on the table an extra column refuses with Spark's exact `[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS]` text and SQLSTATE `21S01`, whether `mergeSchema` is set or not, and the table is left unchanged. | M-1's pin plus the no-option control. | OPEN | `D.mergeSchema.ms_plain`. |
| C-003 | `mergeSchema` is union-by-name, not replace: a column missing from the frame stays in the schema and is written NULL, and a source column narrower than the table's does not narrow it. | M-4 and M-5's pins. | OPEN | `D.mergeSchema.missing`, `D.mergeSchema.widen`. |
| C-004 | Precedence is per-write option > session conf `spark.sql.iceberg.merge-schema` > false: an explicit `option("mergeSchema","false")` refuses even with the conf true, and the conf alone evolves. | The two precedence pins. | OPEN | Java `SparkWriteConf.mergeSchema()` reads the option then the conf. |
| C-005 | `MERGE WITH SCHEMA EVOLUTION` parses, and adds the source's new column with the **source's** type unwidened, optional, last; with no new column it is an ordinary upsert leaving the schema untouched. Both commit exactly one new snapshot. | The two cells plus a whitespace/case variant. | OPEN | Cells `W-MERGE-SCHEMA-EVOLUTION-BIGINT`, `-NO-NEW-COL`; `D.mse`, `D.mse_nonew`. |
| C-006 | `MERGE WITH SCHEMA EVOLUTION` needs no table property — the asymmetry with C-002 is Spark's. | The no-property pin. | OPEN | `D.mse` on a plain table. |
| C-007 | A plain `MERGE INTO` is untouched by the clause sniffer: an extra source column is still ignored rather than refused, and a row whose data is literally `evolution` is not eaten. | M-8's pin and the token-boundary pin. | OPEN | `D.mse_plain_no_evo`. |
| C-008 | `DataFrame.mergeInto` binds Spark's own condition form — the target by its **short table name**, the source by the frame's alias — on all five non-evolution cells. | The five cells. | OPEN | Cells `W-DFMERGE-UPSERT`, `-DELETE`, `-UPDATE-COLS`, `-NOT-MATCHED-BY-SOURCE`, `-CONDITIONAL`. |
| C-009 | `MergeIntoWriter.withSchemaEvolution()` evolves instead of refusing, and needs no table property (it lowers to `MERGE WITH SCHEMA EVOLUTION`). | The cell. | OPEN | Cell `W-DFMERGE-SCHEMA-EVOLUTION`. Owner decision 9: the no-property half is a ruling, unmeasured. |
| C-010 | RePark keeps answering the qualifiers Spark rejects: `target.` / `source.` and the bare-key sugar still resolve (owner decision 22 — narrow `EX-DF-9`, do not retire it). | The two legacy pins plus the shipped `test_examples_dataframe_b.py` batch. | OPEN | `docs/spark-sql-iceberg-parity.md` `EX-DF-9`. |
| C-011 | `INSERT … BY NAME` with an extra column answers M-9's four combinations with their three distinct outcomes and **two** error classes, and the `EXTRA_COLUMNS` arm still fires for a misnamed column at or below target arity. | The matrix pin and the `EXTRA_COLUMNS` control. | OPEN | `D.sql_by_name*`. |
| C-012 | The session conf reaches the write decision through both spellings — `spark.conf.set` and SQL `SET spark.sql.iceberg.merge-schema = true` — and `unset` clears it. | The SQL `SET` pin. | OPEN | Cell `SC-MERGE-SCHEMA-SQL` applies the conf through `conf.set`. |
| C-013 | Positional `INSERT … VALUES` consults neither the conf nor the property: it neither starts adding `colN` nor starts refusing with `Field col1 not found in source schema` (IPI-17 stays where it is). | The IPI-17 boundary pin. | OPEN | Critic ruling A-10; IPI-17 awaits its own ruling. |
