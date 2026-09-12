# `repark.toml` — file-based session configuration

One file holds the session configuration that `.config(...)` calls would otherwise repeat on
every builder chain: catalog blocks, free-form conf pairs, the display style, and the three
engine knobs. If you have not built a session yet, start with
[getting-started.md](getting-started.md); the builder semantics underneath are in
[session-and-conf.md](session-and-conf.md).

## The complete file

This is the whole surface in one file — four profiles, one catalog, one database source, and
the display and session tables. It was rendered by `repark.config` (see below) and parsed back
before it landed here:

```toml
[default.display]
style = "polars"
max_rows = 10
[default.session]
memory_limit_gb = 2
batch_size = 4096
target_partitions = 4
[default.conf]
spark.sql.shuffle.partitions = "4"
example.app.tag = "cfg-1"
[default.catalog.local]
type = "memory"
warehouse = "/tmp/repark-wh"
[prod.conf]
example.app.tag = "cfg-1-prod"
[prod.catalog.events]
type = "memory"
warehouse = "/tmp/repark-wh-events"
[read.display]
style = "spark"
max_rows = 50
[read.conf]
example.app.tag = "cfg-1-read"
[write.session]
target_partitions = 16
[write.conf]
example.app.tag = "cfg-1-write"
[write.database.postgres.company_db]
dbname = "analytics"
host = "db.example.com"
```

Two constraints apply to this file today, stated plainly:

1. **A profile carrying a non-empty `[<profile>.database]` table refuses at load.** Named
   sources arrive with the CFG-2 card, so the `[write]` profile above cannot open a session
   yet. The refusal names the source and the card:

   ```text
   IllegalArgumentException: repark config error: database sources
   (default.database.postgres.company_db) are parsed but named-source registration arrives
   with CFG-2 — drop the `[<profile>.database]` tables until that card lands
   ```

   Until CFG-2 lands, drop the database tables and the file loads. Everything below was run
   against that loadable variant.

2. **`[<profile>.conf]` keys apply in sorted-key order, not file order.** The TOML table the
   loader reads does not retain file order, so the application order is the deterministic
   sorted-key order. A file with `zebra`, `apple`, `mango` in that file order still loads all
   three values; only the application order differs from the file order.

## Building a session from the file

Force a file with `Builder.config_file` (camelCase `configFile` is the same call), or do
nothing and let `getOrCreate` discover one (see below). The `repark.config` module
(`ReparkConfig`, `ProfileConfig`, `DisplayConfig`, `SessionConfig`, `CatalogBlock`,
`DatabaseSource`) builds and validates the same file in Python and renders the text a user
could write. It is typed construction only: discovery, the profile merge, `${VAR}`
interpolation, and the source translation live in the engine and stay there.

```python
from pathlib import Path
from repark import ReparkSession
from repark.config import (
    CatalogBlock,
    DisplayConfig,
    ProfileConfig,
    ReparkConfig,
    SessionConfig,
)

built = ReparkConfig(
    profiles={
        "default": ProfileConfig(
            display=DisplayConfig(style="polars", max_rows=10),
            session=SessionConfig(memory_limit_gb=2, batch_size=4096),
            conf={"example.app.tag": "cfg-1"},
            catalog={"local": CatalogBlock(type="memory", warehouse="/tmp/repark-wh")},
        )
    }
)
config_path = Path("/tmp/repark-wh/repark.toml")
config_path.parent.mkdir(parents=True, exist_ok=True)
built.save(config_path)
spark = ReparkSession.builder.configFile(str(config_path)).getOrCreate()
print(spark.conf.get("example.app.tag"))
print(spark.display_style)
spark.stop()
```

```text
cfg-1
polars
```

Unknown keys refuse at construction: `DisplayConfig(style="spark", nonesuch="x")` raises
`pydantic.ValidationError`, as does any key outside `memory_limit_gb` / `batch_size` /
`target_partitions` in the session table, any database kind outside `postgres` /
`sqlserver` / `trino`, a dotted catalog name, an empty catalog block, and a name carried by
both a catalog and a database source.

## Discovery and profiles

The loader looks for a file in this order and takes the first hit: the path named by
`REPARK_CONFIG`, then `./repark.toml` in the current directory, then
`~/.config/repark/repark.toml`. No file anywhere is the empty configuration, never an
error. `REPARK_ENV` selects the profile; without it the `[default]` table stands alone.
With `REPARK_CONFIG` pointing at a file holding `probe.key = "from-env"` and a local
`repark.toml` holding `probe.key = "from-local"`, the session answers:

```text
probe: from-env
```

and with no `REPARK_CONFIG` the same process answers `probe: from-local`.
`REPARK_CONFIG=""` (set but empty) disables discovery for one process: the local file is
ignored and the key is unset:

```text
Exception: Configuration property probe.key is not set.
```

An unknown non-empty `REPARK_ENV` refuses naming the profile and the profiles the file
carries:

```text
IllegalArgumentException: repark config error: unknown profile `staging` named by
REPARK_ENV; the config file carries profiles: default, other
```

`Builder.config_file(path)` forces a file — a forced path that does not exist refuses
naming it — while plain `getOrCreate()` runs the automatic discovery above.

## Interpolation

String values expand `${VAR}` from the process environment. A missing variable refuses loud,
naming the variable and the key path it sat under:

```text
IllegalArgumentException: repark config error: missing environment variable
`NO_SUCH_VAR_DEFINED_ANYWHERE` for key `conf.key`
```

`$$` escapes a literal dollar: with `TOTAL=42` in the environment, `$${TOTAL}` answers the
literal `${TOTAL}` with no lookup. An unterminated `${` refuses naming the key path:

```text
IllegalArgumentException: repark config error: unterminated `${` reference at key
`conf.key`
```

A `$` without a brace is left alone. Interpolation runs on the merged profile, so a missing
variable in a profile `REPARK_ENV` did not select never refuses.

## Precedence and the redacted dump

One chain decides every key: an explicit builder `.config()` beats the `REPARK_ENV` profile,
which beats `[default]`. With `shared.key` set in all three places and `default.only` only
in `[default]`, the builder-first session answers `shared: from-builder`; without the
builder call the same key answers `shared: from-prod`, and `default.only` answers `d`.

The engine keeps the origin of every merged key beside its value: `builder` for a key the
builder map carried, `file:<path>#<profile>` for a key that survived from the selected
profile, `default` for a key that survived from `[default]`. Secrets stay masked in that
view — `conf.getAll()` answers `***` for a secret key while the value itself stays plain:

```python
snapshot = spark.conf.getAll()
print(snapshot.get("example.token.password"))
print(snapshot.get("example.nickname"))
print(spark.conf.get("example.token.password"))
```

```text
***
plain
s3cr3t
```

An explicit `conf.get` of the named secret key answers the raw value; only the bulk read
masks. Which keys count as secret is the catalog predicate the redaction shares with
`CatalogSpec` (the `password` / `token` / `secret` family).

## The tables

Each profile carries six optional tables. `[<profile>.display]` takes `style`, `max_rows`,
`max_cols`, `str_len` — the four `repark.display.*` keys from
[session-and-conf.md](session-and-conf.md) — with string values verbatim and integers
stringified. `[<profile>.session]` takes `memory_limit_gb`, `batch_size`,
`target_partitions` as integers or integer strings; anything else refuses naming the key
path. `[<profile>.conf]` takes any key with a string or integer value; nested tables
flatten with dot joins, so the natural `spark.sql.x = "v"` spelling works, and a
quoted-plus-nested collision refuses. `[<profile>.catalog.<name>]` blocks carry `type`
(`memory`, `glue`, `s3tables`, or a `catalog-impl` class name) plus string properties, and
parse into the same spec the equivalent `.config()` keys produce — the catalog keys
themselves are in [iceberg-guide.md](iceberg-guide.md). `[<profile>.maintenance]` takes
`target_file_size_bytes`, `snapshot_retain_last`, `snapshot_older_than`,
`orphan_older_than`, `rewrite_manifests`, `position_delete_ratio`, plus a
`tables."<catalog>.<db>.<table>"` entry per override — every key optional, durations
as `"<n>d"`, `"<n>h"`, or `"<n>m"`, unknown keys refusing loud with the key path.
The full shape, the step order it drives, and the `CALL run_maintenance()` door are
in [maintenance-policy.md](maintenance-policy.md). Unknown keys inside `display` and
`session` refuse loud; `conf` accepts any key.

## The measured `read` and `write` profiles

PROFILES-1 swept nineteen knobs over a ten-cell bed — five read shapes over the
futures parquet and TPC-H SF10, three write shapes over a 200-file Iceberg table —
three repetitions, medians, release build
([../perf/config-profiles-2026-09-12.md](../perf/config-profiles-2026-09-12.md)
carries every row and the method; the committed CSVs beside it recompute each
number). A knob earns a place in a profile only when one of its values measured
at least 5 % better than the default on a cell the knob can reach in the
profile's own workload class (read: scan+filter, group-by, joins, window; write:
append, `INSERT OVERWRITE`, `MERGE`) — and it keeps the place only when no
sibling cell in that class paid the win back. The profiles are per-knob argmaxes:
values were measured one knob at a time, never in combination (DYNCFG-1 owns
interactions), so read them as "each value won alone on this box", not as a tuned
bundle.

### `read` — analytics scans, joins, aggregations

| conf key | value | measured ratio | measured on |
|---|---|---|---|
| `repark.batch.size` | `16384` | 0.3323 | tpch `group_by` |
| `repark.target.partitions` | `32` | 0.7800 | tpch `group_by` |
| `datafusion.optimizer.repartition_joins` | `"false"` | 0.7517 | tpch `sort_merge_join` |

Committed as [../examples/config/read.toml](../examples/config/read.toml):

```toml
[read.session]
batch_size = 16384
target_partitions = 32

[read.conf]
datafusion.optimizer.repartition_joins = "false"
```

`REPARK_ENV=read` selects it. The `[read.session]` keys emit the same conf keys
the table names: `session.batch_size` is `repark.batch.size`, and
`session.target_partitions` is `repark.target.partitions`. Two spellings drive
the one engine batch-size option — `repark.batch.size` (equally
`spark.sql.execution.arrow.maxRecordsPerBatch`) and
`datafusion.execution.batch_size` were swept separately and agree (0.3323 /
0.3148 on tpch `group_by`); the profile names the repark spelling, and the
`datafusion.*` spellings alias `target_partitions` the same way
(`spark.sql.shuffle.partitions` is the PySpark spelling). Each row's ratio is the
measured median ratio to `@default` on the cited cell; `repartition_joins` also
measured 0.9426 on tpch `hash_join` and 0.9647 on `merge_updates`, and
`target_partitions = 32` measured 0.8178 on tpch `scan_filter`.

### `write` — bulk appends, `INSERT OVERWRITE`, `MERGE`

No knob earned a place. Every swept value either stayed within 5 % of the
default on the write cells or bought one write shape at the price of a sibling:
`write.distribution-mode = 'none'` measured 0.9333 on `INSERT OVERWRITE` but
1.2217 on `MERGE` (and it is an Iceberg table property regardless — it would sit
unread in this file; it is set with `ALTER TABLE … SET TBLPROPERTIES`), and
`datafusion.execution.batch_size = 262144` measured 0.9391 on append in one sweep
while its `MERGE` cost (1.0543) replicated across both batch-size spellings — the
twin spelling's append read 0.9854, so the win did not reproduce. The `write`
profile is therefore the engine defaults, and the committed file
([../examples/config/write.toml](../examples/config/write.toml)) carries the
bare `[write]` table as the honest record: `REPARK_ENV=write` builds a stock
session.

### No effect measured

Five more knobs were swept and moved no reachable cell by more than 5 % — the
maximum deviation |ratio − 1| over each knob's affected cells, per the step-2
method:

| knob | max deviation on affected cells |
|---|---|---|
| `repark.scan.concurrency_limit` | 0.0345 |
| `datafusion.execution.parquet.max_row_group_size` | 0.0399 |
| `datafusion.execution.parquet.bloom_filter_on_write` | 0.0153 |
| `datafusion.execution.parquet.write_batch_size` | 0.0438 |
| `write.target-file-size-bytes` | 0.0417 |

The remaining ten swept knobs measured an effect but never a qualifying win —
each stays at the default because its non-default cells were flat-to-worse, not
untried (ratios are the argmax regression each knob produced on a cell it can
reach):

| knob | non-default result | cell |
|---|---|---|
| `datafusion.optimizer.prefer_hash_join` | `false` 10.2064 | tpch `hash_join` |
| `datafusion.optimizer.repartition_aggregations` | `false` 4.2430 | tpch `hash_join` |
| `datafusion.optimizer.repartition_file_scans` | `false` 6.7024 | tpch `scan_filter` |
| `datafusion.execution.parquet.pushdown_filters` | `true` 1.7493 | tpch `hash_join` |
| `datafusion.execution.parquet.enable_page_index` | `false` 1.0839 | tpch `sort_merge_join` |
| `datafusion.execution.parquet.bloom_filter_on_read` | `false` 1.2182 | futures `window` |
| `datafusion.execution.parquet.compression` | `snappy` 1.0870 | iceberg `append_files` |
| `write.distribution-mode` | `none` 1.2217 | iceberg `merge_updates` |
| `repark.merge.file_scoped_rewrite` | `false` 1.8483 | iceberg `merge_updates` |
| `repark.merge.scan_pruning` | `false` 1.4638 | iceberg `merge_updates` |

`repartition_file_scans = "false"` also produced the sweep's largest single
improvement (0.3331 on futures `scan_filter`) — on the bed's noisiest cell, while
the same shape on TPC-H regressed 6.7×, so it is not a win. The same applies to
every argmax that landed on a futures read cell: those cells carried the run's
desktop noise (the step-2 method records load ~15 of 64 threads), and no profile
entry rests on one. `datafusion.execution.coalesce_batches` is not swept at all —
it refuses loud at `.config()` (R-16).
