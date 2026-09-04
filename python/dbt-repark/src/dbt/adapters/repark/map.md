# map — python/dbt-repark/src/dbt/adapters/repark

## Purpose

The adapter. `dbt.adapters` is a namespace package (`pkgutil.extend_path`), so this directory
becomes `dbt.adapters.repark` as soon as `python/dbt-repark/src` is on `sys.path` — no install
step, which is why the tests can run an unpublished package.

`ReparkAdapter` **subclasses `dbt-spark`'s `SparkAdapter`** and the plugin declares
`dependencies=["spark"]`. What that buys and what it costs is argued from the measured statement
surface in [../../../../map.md](../../../../map.md) "The route". In one line: `dbt-spark`'s
`create_table_as` already emits the one CTAS shape RePark serves, and re-authoring it would put a
second reading of `file_format` beside dbt-spark's.

## Contents

- `__init__.py` — the `AdapterPlugin`, and the two session functions callers need
  (`acquire_session`, `release_session`).
- `__version__.py` — dbt reads `version` at adapter registration; without it registration fails.
- `connections.py` — `ReparkCredentials` (profile fields; `catalog` is the profile spelling of
  dbt's `database`) and `ReparkConnectionManager`. The transaction methods are no-ops: RePark has
  no transactions, and every Iceberg commit is its own snapshot. `exception_handler` re-raises
  RePark's refusal as `DbtRuntimeError` with the message intact, because that message is what the
  divergence registry rows quote.
- `session.py` — the process-wide session, and the pyodbc-shaped cursor dbt drives it through.
  RePark's `getOrCreate` answers **one session per process**, so a second profile with a
  different catalog or warehouse would silently run against the first one's engine. That is
  refused, not reused; `release_session` is how a caller that owns the process starts over.
- `relation.py` — `ReparkRelation`. It subclasses `BaseRelation`, **not** `SparkRelation`, for two
  reasons a subclass could not work around: `SparkRelation.__post_init__` raises when `database`
  is set, and `SparkRelation.render()` raises when database and schema are both included. RePark
  needs all three parts.
- `impl.py` — `ReparkAdapter`. Only the surfaces RePark refuses are overridden:
  `list_relations_without_caching` (no `SHOW TABLES IN`, registry `ST-1`),
  `get_columns_in_relation` and `parse_columns_from_information` (`DESCRIBE EXTENDED` answers
  Arrow spellings, registry `DBT-DESC-1`), `_get_columns_for_catalog` (same reason, for
  `dbt docs`), and `get_relation`, which un-does dbt-spark's nulling of the database.
  pins: dbt-1-adapter/C-002

## I want to...

| ...do this | go to |
|---|---|
| Add a profile field | `ReparkCredentials` in `connections.py`, then the profile table in [../../../../../../docs/guide/dbt-on-repark.md](../../../../../../docs/guide/dbt-on-repark.md) |
| Change how a statement reaches the engine | `ReparkCursor.execute` in `session.py` |
| Change what dbt sees as a relation | `impl.py` `list_relations_without_caching` |
| Change a macro instead | [../../include/repark/map.md](../../include/repark/map.md) |

## Pointers

- Up: [../../../../map.md](../../../../map.md)
- The macros: [../../include/repark/map.md](../../include/repark/map.md)
- The engine surface: [../../../../../repark/src/repark/map.md](../../../../../repark/src/repark/map.md)

## Debug

| Symptom | First check |
|---|---|
| `No module named 'dbt.adapters.repark.__version__'` | `__version__.py` is missing; dbt reads it at registration |
| `a RePark session is already live for a different profile` | one process, one session — call `release_session()` between profiles |
| a model builds against the wrong catalog | the profile's `catalog`, and whether a previous session was released |
| `Cannot set database in spark!` | something built a `SparkRelation`; `ReparkRelation` is the three-part one |
