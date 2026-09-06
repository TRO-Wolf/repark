# Session and configuration

The `ReparkSession` builder, the conf keys you will actually set, and the one rule that explains
most surprises: **engine knobs are resolved once, at session build.**

If you have not built a session yet, start with [getting-started.md](getting-started.md).

## The builder

```python
from repark import ReparkSession

spark = (
    ReparkSession.builder.appName("etl")
    .config("spark.sql.session.timeZone", "America/New_York")
    .config("spark.sql.shuffle.partitions", "8")
    .config("repark.memory.limit.gb", "4")
    .getOrCreate()
)
```

`appName` / `config` / `getOrCreate` behave as in PySpark. `config` also takes `map=` and `conf=`
forms. Unknown keys are stored and ignored rather than refused, so an existing script's
`.config(...)` chain does not have to be pruned before it runs.

`.master(url)` is accepted for source compatibility and **has no effect** — repark is single-node.
The first call per process warns so a cluster URL is never silently downgraded:

```python
ReparkSession.builder.master("local[4]").appName("etl").getOrCreate()
```

```text
UserWarning: Spark master URL is accepted for source compatibility but ignored; repark runs
single-node (distribution is deferred, OTH-010).
```

A second call is silent, on purpose: a script that sets it in a loop should not drown its own logs.
That row, and the rest of the accepted-but-not-reproduced surface, is the divergence registry's
[drop-in disclosure table](../spark-sql-iceberg-parity.md#8-drop-in-disclosure-rationale).

**`getOrCreate` returns the live session if there is one.** Builder options that name engine knobs
are *not* re-applied to it — they are warned about instead:

```python
first = ReparkSession.builder.config("spark.sql.shuffle.partitions", "4").getOrCreate()
second = ReparkSession.builder.config("spark.sql.shuffle.partitions", "9").getOrCreate()
```

```text
UserWarning: Using an existing ReparkSession; some configuration may not apply (engine knobs are
fixed at session build; unapplied keys: ['spark.sql.shuffle.partitions']).
```

`second is first`, and the value stays `4`. PySpark *does* apply the option to the live session
here, so a notebook that rebuilds the builder cell expecting a new partition count gets a different
answer than it would under Spark. Call `spark.stop()` first if you want a fresh engine.

## How `conf.get` / `conf.set` behave

`conf` is a string-keyed map on the live session.

```python
spark.conf.get("spark.app.name")            # 'etl'
spark.conf.get("nope.nope", "fallback")     # 'fallback'
spark.conf.get("nope.nope")                 # raises
```

```text
Exception: Configuration property nope.nope is not set.
```

That last line matters more than it looks: **a key you never set is not readable back, even when
the engine has a default for it.** Only the keys in the session's defaults table (below) answer
`conf.get` without a prior `set`. `spark.sql.ansi.enabled` is the one that catches people — see
its section.

`conf.isModifiable(key)` reports Spark's static/dynamic split (`spark.sql.warehouse.dir` is
`False`; `spark.sql.shuffle.partitions` is `True`). `conf.unset(key)` removes a stored value.

Three tiers of key, and knowing which tier a key is in explains every "why did my `set` do
nothing":

| Tier | Where it takes effect | Examples |
|---|---|---|
| **Build-time engine knob** | resolved inside `getOrCreate()`; a later `conf.set` does not move it | `spark.sql.session.timeZone`, `spark.sql.ansi.enabled`, `spark.sql.shuffle.partitions`, `repark.memory.limit.gb`, `spark.sql.timestampType` |
| **Live DataFusion config** | forwarded to the running engine as `SET <key> = <value>` | anything under the `datafusion.` prefix |
| **Facade-local** | stored on the session, read by the facade | `spark.app.name`, `repark.display.style`, and any key repark does not claim |

The `datafusion.` tier refuses loud on a key or value the engine rejects, so a typo is never a
silent store-only twin:

```python
spark.conf.set("datafusion.execution.batch_size", "4096")   # forwarded, readable back
spark.conf.set("datafusion.nope.nope", "1")                 # raises
```

```text
IllegalArgumentException: [INVALID_CONF_VALUE.REQUIREMENT] The value '1' in the config
'datafusion.nope.nope' is invalid. datafusion engine error: Invalid or Unsupported Configuration:
Config value "nope" not found on ConfigOptions
```

## Where the defaults live

The facade's default conf table is `_SQLCONF_DEFAULTS` in
`python/repark/src/repark/spark/session/_funcs.py` — that is the single home for every key repark
answers `conf.get` for on a session that never set it. Today:

```python
for key in (
    "spark.sql.session.timeZone",
    "spark.sql.pyspark.inferNestedDictAsStruct.enabled",
    "spark.sql.sources.partitionOverwriteMode",
    "spark.sql.timestampType",
):
    print(key, "=", spark.conf.get(key))
```

```text
spark.sql.session.timeZone = UTC
spark.sql.pyspark.inferNestedDictAsStruct.enabled = true
spark.sql.sources.partitionOverwriteMode = STATIC
spark.sql.timestampType = TIMESTAMP_LTZ
```

(`spark.app.name` is in the same table, defaulting to `repark`.)

## `spark.sql.pyspark.inferNestedDictAsStruct.enabled` — default `true`

**This is a deliberate divergence from PySpark**, registry row
[FA-4](../spark-sql-iceberg-parity.md#fa-4--infernesteddictasstruct-defaults-to-true). A
dict-valued *cell* in `createDataFrame` python-object ingestion infers as a `StructType` here;
PySpark's conf (SPARK-35929) defaults to `false` and infers `MapType`.

```python
nested = [{"id": 1, "payload": {"x": 1, "y": "left"}}]

spark.createDataFrame(nested).schema.simpleString()
# 'struct<id:bigint,payload:struct<x:bigint,y:string>>'

spark.conf.set("spark.sql.pyspark.inferNestedDictAsStruct.enabled", "false")
spark.createDataFrame(nested).schema.simpleString()
# 'struct<id:bigint,payload:map<string,string>>'
```

Setting the key to `"false"` restores **byte-identical PySpark inference** — both directions stay
under test. Row-level dicts (a dict *is* the row) are unaffected either way, and an explicit
`schema=` wins over both.

Why the flip: the dominant facade ingestion shape is nested dict rows headed for
`dynamicFlatten` or struct addressing, where map inference is a silent no-op surprise. This is one
of the few keys whose `set` takes effect immediately — inference happens in Python at
`createDataFrame` time, not at session build.

## `spark.sql.session.timeZone` — default `UTC`

repark's default is the fixed constant `UTC`. Spark defaults this key to the **JVM's local zone**,
so the same job gives different wall-clock answers on two hosts; a reproducible default was chosen
instead, and reading the host zone would be an environment read the engine's design forbids.
Registry row [TZ-2](../spark-sql-iceberg-parity.md#tz-2--the-session-timezone-default-is-utc).

Set it **on the builder**, where it reaches the engine:

```python
tz = ReparkSession.builder.config("spark.sql.session.timeZone", "America/New_York").getOrCreate()
tz.sql("SELECT hour(TIMESTAMP '2026-03-01 05:30:00Z') AS h").collect()
```

```text
[Row(h=0)]
```

```python
utc = ReparkSession.builder.getOrCreate()          # after tz.stop()
utc.sql("SELECT hour(TIMESTAMP '2026-03-01 05:30:00Z') AS h").collect()
```

```text
[Row(h=5)]
```

A **runtime** `conf.set` of this key is a different story. It is accepted (so a drop-in script, and
PySpark's own `sql_conf` helper, still run), it warns **once per process**, and it is neither
validated nor stored — `conf.get` keeps reporting the zone the engine really has:

```python
spark.conf.set("spark.sql.session.timeZone", "Europe/Paris")   # warns once, does nothing
spark.conf.get("spark.sql.session.timeZone")                   # still the build-time zone
```

repark is knowingly laxer than PySpark on this one key (PySpark raises on an invalid zone here);
the warning says so, and the whole shape is registry row
[TZ-3](../spark-sql-iceberg-parity.md#tz-3--a-runtime-confset-of-the-session-zone-is-accepted-neither-validated-nor-applied).
The zone is validated exactly once, in the engine, at build.

What the zone reaches — and what it does not — is documented in the module that owns the key,
`python/repark/src/repark/spark/session/session_time_zone.py`. Timestamp behavior in general has
its own family of registry rows (`TZ-*` in §4 and §7); read them before you port a timestamp-heavy
job.

## `spark.sql.ansi.enabled` — default `true`

repark's Spark door defaults to **ANSI on**, matching Spark 4. Division and modulo by zero raise
rather than returning `NULL`:

```python
spark.sql("SELECT 1 / 0 AS q").collect()
```

```text
PySparkException: Execution error: [DIVIDE_BY_ZERO] Division by zero. Use try_divide to tolerate
divisor being 0 and return NULL instead. If necessary set "spark.sql.ansi.enabled" to "false" to
bypass this error. (ArithmeticException)
```

Two honest caveats on that message. `try_divide` is Spark's own suggestion text and is **not**
implemented here yet (`Invalid function 'try_divide'`). Integer `+` / `-` / `*` overflow **does**
raise under ANSI (`ARITHMETIC_OVERFLOW`; F-Y10-1, 2026-08-30) for INT/BIGINT;
`ansi=false` wraps at the source type. SMALLINT/Int16 still wraps (residue 2026-08-30).
Decimal overflow is DEC-6 (FIXED). Do not read "ANSI on" as "every arithmetic fault
raises" — float `/ 0` on the ANSI door is still IEEE Inf (F-Y10-2).

It is a **build-time carrier**: set it on the builder, not at runtime.

```python
lax = ReparkSession.builder.config("spark.sql.ansi.enabled", "false").getOrCreate()
lax.sql("SELECT 1 / 0 AS q").collect()
```

```text
[Row(q=None)]
```

The key is **not** in the defaults table, so on a session that never set it `conf.get` raises
`Configuration property spark.sql.ansi.enabled is not set.` even though the engine's answer is
`true`. Use `spark.conf.get("spark.sql.ansi.enabled", "true")` if you need a value in a log line,
and read the engine as the source of truth (`crates/repark-functions/src/ansi.rs`).

This flag belongs to the **Spark door only**. The native `repark.sql()` door has its own semantics
— see [sql-doors.md](sql-doors.md).

## Partitions, batches and memory

| Key | Alias | Meaning |
|---|---|---|
| `spark.sql.shuffle.partitions` | `repark.target.partitions` | engine target partition count (DataFusion `target_partitions`) |
| `spark.sql.execution.arrow.maxRecordsPerBatch` | `repark.batch.size` | Arrow batch size |
| `repark.memory.limit.gb` | — | build-time size of the bounded memory pool |

All three are build-time. Out-of-range values follow **Spark's own per-key rule**, checked inside
`getOrCreate()`:

```python
ReparkSession.builder.config("spark.sql.shuffle.partitions", "0").getOrCreate()
```

```text
IllegalArgumentException: [INVALID_CONF_VALUE.REQUIREMENT] The value '0' in the config
"spark.sql.shuffle.partitions" is invalid. The value of spark.sql.shuffle.partitions must be
positive
```

`0` or negative for the batch key is Spark's documented "no limit" sentinel, so it is *accepted*
and warns once — repark cannot emit unbounded Arrow batches, so the engine default is used and
values are unaffected:

```text
UserWarning: config 'spark.sql.execution.arrow.maxRecordsPerBatch' = 0 means 'no limit' in Spark;
repark accepts it but cannot honor it (DataFusion always emits bounded Arrow batches), so the
engine default batch size is used instead. Set a positive value to control batching (SAF-006).
```

Note the **timing** divergence on the whole family: repark validates eagerly inside `getOrCreate()`
so a misconfigured session is never handed out, while a fresh-process PySpark only raises at the
first `sessionState` touch. If your script wraps `try`/`except` around its first query rather than
around `getOrCreate()`, move it.

**Memory has exactly one truth, reachable two ways.** `repark.memory.limit.gb` sizes the pool at
build (RAM-relative default, capped at 8 GiB; `0` opts out). To re-size a *live* session use the
DataFusion key — setting the build-time knob at runtime refuses loud and says so:

```python
spark.conf.set("repark.memory.limit.gb", "2")
```

```text
IllegalArgumentException: [INVALID_CONF_VALUE.REQUIREMENT] config 'repark.memory.limit.gb' is
build-time only (FairSpillPool size at getOrCreate; RAM-relative default, cap 8 GiB; 0 =
unbounded). To re-size the live pool use spark.conf.set('datafusion.runtime.memory_limit', 'NG')
or SQL SET datafusion.runtime.memory_limit = 'NG' — same pool, one truth.
```

Setting **both** on the same builder refuses too: same pool, ambiguous initial size.

## Iceberg catalog caches

| Key | Alias | Meaning |
|---|---|---|
| `repark.iceberg.metadataCache` | `repark.iceberg.metadata_cache` | session-scoped metadata-document cache (default `true`) |
| `repark.iceberg.metadataCacheEntries` | `repark.iceberg.metadata_cache_entries` | retained-location bound, cleared at the statement door (default `512`) |
| `repark.iceberg.manifestCacheBytes` | `repark.iceberg.manifest_cache_bytes` | shared manifest-cache byte budget per memory catalog (default `33554432` = on; `0` disables) |

All three are build-time and memory-catalog-only. A bad value fails loud inside
`getOrCreate()`, naming both the key set and the canonical spelling. The manifest
budget sizes the fork's shared `ObjectCache`: on a default session a repeated read opens
no manifest-list and no manifest at all. The default is on since PERF-ICE-CATALOG-IO-3
(2026-09-05): RP-13 landed the fork key fix first (`F-CATIO-KEY` — the cache stores the
context-free parse and applies each caller's lineage per read), so upgrade-boundary
tables serve assigned lineage with the cache on. To turn the cache off, set the key to
`"0"`. The 32 MiB is the fork's estimated manifest weight per memory catalog (one shared
cache per catalog handle; the fork enforces it with moka `max_capacity`), not a
resident-bytes ceiling — a cached small manifest+list measures ~7.5 KB resident
against ~1 KiB charged. Size the budget to the working set: a budget far under it
churns, so a second pass over 2,000 tables at 128 KiB costs what explicit `"0"` costs
(8.1 s vs 8.2 s, against 5.6 s cached); below ~1 MiB prefer `"0"`. Numbers and the
commit-side scope live in
[../perf/iceberg-catalog-io-baseline.md](../perf/iceberg-catalog-io-baseline.md) §6.

## `repark.display.style` — a repark extra

`df.show()` defaults to a PySpark-shaped ASCII grid (`spark` style). Two other renderers are
available for interactive work, via the conf key or `session.display_style`:

```python
spark.conf.set("repark.display.style", "duckdb")
spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]).show()
```

```text
┌───────┬─────────┐
│   id  │   name  │
│ int64 │ varchar │
├───────┼─────────┤
│     1 │ a       │
│     2 │ b       │
├───────┴─────────┤
│      2 rows     │
└─────────────────┘
```

```python
spark.conf.set("repark.display.style", "polars")
```

```text
shape: (2, 2)
┌─────┬──────┐
│ id  ┆ name │
│ --- ┆ ---  │
│ i64 ┆ str  │
╞═════╪══════╡
│ 1   ┆ a    │
│ 2   ┆ b    │
└─────┴──────┘
```

Both head-and-tail styles call `count()` for their shape line — an extra full scan. `spark` is the
default and does not.

## Stopping

`spark.stop()` ends the session. Every handle taken from it — DataFrames, `sparkContext`, `conf` —
refuses afterwards rather than working against a dead engine:

```text
RuntimeError: Cannot call methods on a stopped ReparkSession
```

The next `getOrCreate()` builds a fresh one, which is how you change a build-time knob.

## See also

- [dataframe-guide.md](dataframe-guide.md) — what the session hands you.
- [sql-doors.md](sql-doors.md) — the two SQL surfaces and which conf belongs to which.
- [../spark-sql-iceberg-parity.md](../spark-sql-iceberg-parity.md) — every declared difference,
  with its pin.
