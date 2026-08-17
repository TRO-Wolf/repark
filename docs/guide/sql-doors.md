# The two SQL doors

repark has **two** SQL surfaces, and it does not blend them. Which one you use decides which
dialect parses your string and which semantics evaluate it. Getting this wrong is the single
easiest way to be surprised by an answer, so this guide is mostly about telling them apart.

## The rule

> Two doors, each declaring its dialect. **No blended parser.** A string enters through exactly
> one door and is parsed by that door's dialect, full stop.

That is [ADR-0002](../adr/0002-two-sql-doors.md), and it is a hard rule rather than a convention.
The alternative — one parser that "accepts both" — has to guess which dialect a string meant, and
dialect guessing produces silent wrong answers on exactly the strings where the dialects disagree.
Beneath both doors the Iceberg machinery (commit semantics, MERGE, snapshots, evolution) is
shared, so a fix under the doors fixes both.

| | Spark door | Native door |
|---|---|---|
| Entry point | `spark.sql(...)` on a `ReparkSession` | `repark.sql(...)`, a module-level callable |
| Dialect | Spark SQL | stock DataFusion / ANSI |
| Function library | Spark shims (`array`, `date_add`, …) | DataFusion's own |
| Expression semantics | Spark's (ANSI on by default) | the engine's |
| Session | your `ReparkSession` — its temp views, catalogs and conf | a separate process-wide engine session |
| Use it for | migrating Spark SQL; anything the facade touches | standard SQL against the native engine |

## The Spark door — `spark.sql`

This is the near-drop-in one. It parses the **Spark dialect** and evaluates with Spark expression
semantics, so existing production SQL runs unchanged, and it is the door everything else in the
facade shares — temp views, catalogs, session conf.

```python
spark.createDataFrame([(1, "a")], ["id", "name"]).createOrReplaceTempView("v")
spark.sql("SELECT * FROM v").collect()
```

```text
[Row(id=1, name='a')]
```

Spark semantics, not the engine's defaults:

```python
spark.sql("SELECT 5 / 2 AS q").collect()        # [Row(q=2.5)] — Spark's `/` is real division
spark.sql("SELECT 1.5 AS v").to_arrow().schema  # v: decimal128(2, 1) not null — literals are DECIMAL
spark.sql("SELECT array(10, 20, 30)[0] AS v").collect()   # [Row(v=10)] — 0-based subscript
```

ANSI mode is **on** by default here (Spark 4's default), so arithmetic faults raise:

```python
spark.sql("SELECT 1 / 0 AS q").collect()
```

```text
PySparkException: Execution error: [DIVIDE_BY_ZERO] Division by zero. Use try_divide to tolerate
divisor being 0 and return NULL instead. If necessary set "spark.sql.ansi.enabled" to "false" to
bypass this error. (ArithmeticException)
```

Set `spark.sql.ansi.enabled` on the builder to change that — see
[session-and-conf.md](session-and-conf.md).

**What belongs to this door:** anything that has to see your session. Temp views, catalog tables,
Iceberg DDL and DML, and every statement whose text came out of an existing Spark job. If you are
migrating, this is your door and you should not have to think about the other one.

## The native door — `repark.sql`

`repark.sql` is a **callable**, not a package (`import repark.sql` fails on purpose — the
pyspark-alias package moved to `repark.spark.sql` so a mechanical `pyspark` → `repark.spark` swap
still works).

```python
import repark

repark.sql("SELECT 5 / 2 AS q").collect()
```

```text
[Row(q=2)]
```

Same string, different answer, and that is the point: this door has **no Spark extension**. `/`
between integers truncates, a bare `1.5` is a `DOUBLE` rather than a `DECIMAL`, and the Spark
function shims are simply not registered:

```python
repark.sql("SELECT 1.5 AS v").to_arrow().schema      # v: double not null
repark.sql("SELECT array(10, 20, 30)[0] AS v")
```

```text
AnalysisException: Error during planning: Invalid function 'array'.
```

Division by zero surfaces as the engine's own error rather than Spark's error class:

```python
repark.sql("SELECT 1 / 0 AS q").collect()
```

```text
PySparkException: Arrow error: Divide by zero error
```

It runs on a **process-wide engine session of its own**, distinct from anything
`ReparkSession.builder.getOrCreate()` hands you. That session is persistent across calls, so state
you create through this door is visible to later calls through the same door:

```python
repark.sql("CREATE TABLE t (id BIGINT) AS VALUES (1), (2)").collect()
repark.sql("SELECT count(*) AS n FROM t").collect()
```

```text
[Row(n=2)]
```

…and **not** to the facade session, in either direction:

```python
spark.createDataFrame([(1,), (2,)], ["id"]).createOrReplaceTempView("v")
repark.sql("SELECT count(*) FROM v")
```

```text
AnalysisException: Error during planning: table 'datafusion.public.v' not found
```

```python
spark.sql("SELECT * FROM t")   # `t` was created through repark.sql above
```

```text
AnalysisException: Error during planning: table 'spark_catalog.default.t' not found
```

The result is an ordinary `DataFrame`, so `collect()` / `to_arrow()` / `show()` all work as they do
anywhere else. The argument must be a `str` — a `bytes` or `Column` is refused rather than coerced.

**Why two sessions and not one.** Extensions in this engine are *session*-scoped, not
dialect-scoped: a Spark-extended session has Spark expression semantics through every door, so
running "ANSI" SQL on a facade session could not give honest ANSI answers for anything the
analyzer or function layer touches. An honest second door therefore needs a second engine session,
which is what `repark.sql` is.

**Honest scope, today.** What Python reaches through `repark.sql` is the *native, non-Spark*
session described above. The full ANSI/Trino-style door — the one with the curated Iceberg DDL
vocabulary (`WITH (…)` table properties, `FOR VERSION AS OF` / `FOR TIMESTAMP AS OF` time travel,
maintenance as callable operations), the guard set, and the wrong-door sniff below — lives in
`crates/repark-sql` and is reachable from Rust. Its relocation onto the Python callable is tracked
in [STATUS.md](../../STATUS.md); do not assume a refusal or a spelling described for that door is
what you will get from Python until it says so.

## Wrong-door ergonomics

Someone arriving from Spark will eventually paste Spark SQL into the native door. The engine's
ANSI door (`crates/repark-sql`, the Rust-reachable one) answers that with a **wrong-door sniff**.
Its design is worth knowing because it explains what such a sniff will and will not do for you —
and because it is what the Python callable will inherit when the re-home named above lands:

- It runs on the **error path only**. Nothing is scanned unless a parse or plan has already
  failed, so a statement that worked is never second-guessed and the happy path costs nothing.
- When it fires it upgrades the error to name three things: the **token** that gave it away, the
  **native equivalent**, and the fact that a **Spark door** exists.
- It scans text with string literals and comments blanked out, so `SELECT 'USING'` and a `-- USING`
  comment are invisible to it.
- Tokens that are also ordinary ANSI SQL (`USING` is a join clause; `tag`, `branch`, `namespace`
  and `database` are legal column names) only fire under a leading keyword they could belong to. A
  `SELECT tag FROM t` that failed because `t` does not exist is not answered with "this looks like
  Spark SQL".

Spark-isms it recognizes include `USING`, `TBLPROPERTIES`, `PARTITIONED BY`, bare
`VERSION`/`TIMESTAMP AS OF`, `SYSTEM_*`, `INSERT OVERWRITE`, `CALL …system…`, backticks,
`NAMESPACE`/`DATABASE`, `LATERAL VIEW`, and a top-level `CREATE BRANCH`. Separately, four statement
shapes are **deliberately absent** from that door and refuse with a steer rather than a parse
error: `INSERT OVERWRITE` (express it as `MERGE INTO`, `DELETE` + `INSERT`, or
`CREATE OR REPLACE TABLE … AS SELECT`), `CALL c.system.<proc>(…)` and `ALTER TABLE … EXECUTE`
(maintenance runs as a callable operation on the session), and `TRUNCATE TABLE` (Iceberg has no
truncate primitive, and the two things it could mean commit differently).

That machinery is the Rust door's. Through the Python callable today you get the raw parser error
instead — which is exactly the "technically correct, practically useless" message the sniff exists
to replace:

```python
repark.sql("CREATE TABLE t (id BIGINT) USING iceberg TBLPROPERTIES ('a'='b')")
```

```text
ParseException: SQL error: ParserError("Expected: end of statement, found: USING at Line: 1,
Column: 28")
```

When you see that, you are on the native door with Spark syntax. Move the string to `spark.sql`,
or write the native spelling.

## Identifier case

The native door plans on stock DataFusion, which follows the ANSI rule: an **unquoted** identifier
folds to lower case, a `"Quoted"` one is taken literally. So `SELECT "Id" FROM t` finds a column
only if it really is `Id`.

repark also resolves *quoted* identifiers case-sensitively on the **Spark door**, where Spark would
resolve `` `ID` `` against a column named `id`:

```python
spark.createDataFrame([(1,)], ["id"]).createOrReplaceTempView("q")
spark.sql("SELECT `ID` FROM q")
```

```text
AnalysisException: Schema error: No field named "ID". Valid fields are q.id.
```

Unquoted references agree with Spark. This is registry row
[ID-1](../spark-sql-iceberg-parity.md#id-1--a-quoted-identifier-resolves-case-sensitively) — a
declared, engine-wide property of the resolution layer, not a per-door quirk, and the two doors do
not disagree with each other about it.

## Choosing a door

- Migrating an existing Spark job, or touching anything in your session (temp views, catalog
  tables, Iceberg) → **`spark.sql`**. This is the default answer.
- Writing new standard SQL against the native engine, deliberately without Spark's semantics →
  **`repark.sql`**, remembering it has its own session.
- Wanting one string to work in both → it will not, in general, and that is the design. Write the
  string for the door you are using; the registry records where the dialects disagree.

## See also

- [../adr/0002-two-sql-doors.md](../adr/0002-two-sql-doors.md) — the decision and its consequences.
- [../design/sql-doors.md](../design/sql-doors.md) — the settled design of the native door's
  Iceberg DDL surface (Q1–Q15).
- [session-and-conf.md](session-and-conf.md) — `spark.sql.ansi.enabled` and the rest of the conf
  that shapes the Spark door.
- [../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md) — the divergence registry,
  including the statement forms each door refuses.
