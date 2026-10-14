# WRITE-BODY-CARRIERS-1 step 0: where the refusal would go

Reading only; nothing here was run. Line numbers are the step-0 tree. Two
product paths reach a CTAS body: the Spark door (measured in `main-cells.md`)
and the ANSI door (not measured). Each is named below with its earliest point.

## Path 1: the Spark door

`python/repark/src/repark/spark/session/session_core.py:149`
`ReparkSession.sql` hands the text to
`python/repark/src/repark/spark/session/sql_run.py:17` `run_sql`, which calls
the native `sql` after facade rewrites. `crates/repark-python/src/session.rs:169`
`PyReparkSession::sql` plans through `plan_session_sql_inner` at :93 into
`crates/repark-core/src/session.rs:418` `ReparkSession::sql`,
`crates/repark-core/src/session/write_options.rs:99`
`sql_with_write_options_inner`, and the session dialect:
`crates/repark-spark/src/dialect.rs:39` `SparkDialect::execute_with_write_options`
calls `crates/repark-spark/src/router.rs:85` `execute_in_session`, then :130
`execute_calibrated`, :205 `execute_time_travelled`, and :281 `execute_inner`.

In `execute_inner`, `crates/repark-spark/src/normalize.rs:187`
`parse_single_normalized` returns at router.rs:313. sqlparser 0.62 parses a
write after `AS` as the CTAS query: `Statement::CreateTable` with
`query.body` one of `SetExpr::Insert`, `Update`, `Delete`, `Merge` (the
`src/ast/query.rs` enum). The arm at router.rs:326,
`Statement::CreateTable(create) if create.query.is_some()`, calls
`crates/repark-spark/src/ctas.rs:52` `build_ctas`, which re-renders the body
to SQL at ctas.rs:146 (`query_sql: query.to_string()`), then
`crates/repark-spark/src/ctas.rs:158` `execute_ctas`. `execute_ctas` checks
target existence at :171-185 and resolves locations at :194-203, then runs the
body at :206 through `spark_ast::execute_passthrough`. The passthrough treats
a DML plan as an eager command (`crates/repark-spark/src/spark_ast.rs:222`)
and collects it at :247-251, which commits the inner write; schema derivation
afterwards fails on the write's UInt64 count.

Convergence: `CREATE OR REPLACE TABLE` parses natively to the same arm;
`REPLACE TABLE` is rewritten to it by token prepend
(`crates/repark-spark/src/normalize/replace_table.rs:11`); the service-managed
split at ctas.rs:247 takes the already-executed frame; router.rs:327 is the
only product caller of `execute_ctas`. The DataFrame writers carry no SQL body.

Candidate point: the arm at router.rs:326. The body's kind is known as soon as
the parse returns at :313, and the arm runs before `build_ctas`, before every
catalog read, and before any file is written. Refusing there when
`create.query.body` is a write covers both CTAS spellings and `REPLACE TABLE`
at one site.

## Path 2: the ANSI door

`crates/repark-sql/src/dialect.rs:31` `AnsiDialect::execute` calls
`crates/repark-sql/src/router.rs:23` `execute`, whose match arm at :128 routes
`Statement::CreateTable` to `crates/repark-sql/src/create_table.rs:65`
`execute_create_table`. The body is planned and guarded at :28-38
`derive_ctas_query` (`crates/repark-core/src/pre_execute.rs:35` `plan`,
`:42` `guard`, `:51` `execute`), the frame's schema is read at :105, and the
Iceberg conversion at :127 fails on a write's UInt64 count.

DataFusion 54.1.0 `execute_logical_plan` returns DML plans lazily (the
fallthrough arm builds a `DataFrame`; the write runs on collect), and nothing
between :38 and :127 collects the frame, so on this reading the ANSI door
refuses before any file is written. Not measured: step 0 ran no ANSI cell.
The card's refusal is a Spark-parity refusal, so whether the ANSI door takes
the same named refusal is a fix-time decision, recorded as a ledger question.
