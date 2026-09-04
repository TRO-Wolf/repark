# map — docs/examples/

## Purpose

Executable worked examples of the public surface. The v0.7 drift gate
(`scripts/check_example_coverage.py`, `make check-example-coverage`) walks the
facade, reads each script's `COVERS` list, and fails when a public name is
neither covered nor listed in the backlog ratchet or the cloud exceptions file.

Examples run against local filesystem and a memory catalog only. They are not a
substitute for pins in `python/repark/tests/` — they teach the public name.
Each `COVERS` name must appear as a real use in that script: a module-door name
(`F.*`, `ta.*`, `types.*`, `ml.*`) on its own door, `repark.sql` on the module
alias, class-surface names on a repark-rooted local (assignment dataflow), and
the class-root surfaces (`SparkSession.builder`, `SparkSession.Builder.*`,
`Window.*`) on the class name. The session class root binds under both
spellings — `ReparkSession`, the real class, and `SparkSession`, its exported
alias. Examples construct the session as `repark = ReparkSession.builder…`
(owner ruling, 2026-09-01); `SparkSession.*` stays the inventory spelling.
`exceptions.txt` is an exact count ratchet
(`EXCEPTIONS_BASELINE`); a new row is a visible baseline bump.

EX-1 (2026-08-31) widened the closed set with the class surfaces the owner ruled
into v0.7 — Column, Window, WindowSpec, Catalog, the `types` module surface,
`ml`, and Row: 150 names, 763 → 913, all of them backlog rows.

**Backfill hazard — write the example for the class you named.** The use check
matches a `COVERS` entry on its last component plus the receiver's *kind*, not
the owning class. Two owners that share a kind are therefore interchangeable to
the gate, and four leaf groups conflate a new surface with an old one, all on a
repark-rooted local: `{Column, DataFrame}.alias`, `{Column, DataFrame}.transform`,
`{DataFrame, WindowSpec}.orderBy` / `.order_by`, and
`{DataFrameWriter, WindowSpec}.partitionBy` / `.partition_by`. A `Column.alias`
row is satisfied by a `DataFrame.alias` call, so review — not the gate — holds
that an example demonstrates the name it claims. What does **not** conflate:
`Column.*` versus the `F.*` twins (`asc`, `like`, `round`, `when`, …), split by
door kind, and `Window.*` versus `WindowSpec.*`, split by the class root versus a
local. Both splits are pinned.

This file closes when the v0.7 example backfill is complete and the backlog
file is empty.

## Contents

- [inventory.txt](inventory.txt) — checked-in snapshot of the enumerator
  (`family<TAB>name`). Must match the AST walk.
- [backlog.txt](backlog.txt) — uncovered public names. Count ratchets down only
  (`BACKLOG_BASELINE` in the gate script).
- [exceptions.txt](exceptions.txt) — public names whose only honest example
  needs a cloud service, each with a one-line reason.
- [functions/](functions/map.md) — `F.*` examples.
- [dataframe/](dataframe/map.md) — DataFrame / GroupedData / na / stat examples.
- [column/](column/map.md) — `Column.*` examples (EX-17: 34 names; the engine-plumbing
  rows `for_select`, `join_sql_part`, `spark_display_part`, `spark_wrap_display_part`,
  `sql_expr_part`, `sql_expr_without_alias` stay on the backlog as non-Spark surface;
  the repark-extension namespaces `str`/`dt` are documented with their PySpark-spelled
  twins).
- [catalog/](catalog/map.md) — `Catalog.*` examples (EX-20: the first 18 roster names minus the
  three measured divergences `getDatabase`/`get_database`, `listDatabases`, and the
  `functionExists(name, dbName)` arm — EX-CAT-1..3; EX-21: the setter, exists,
  register, and list_tables remainder; `list_databases` stays on the backlog with §7
  `EX-CAT-2`, whose function object it shares).
- [ta/](ta/map.md) — TA kernel examples.
- [io/](io/map.md) — reader / writer examples.
- [session/](session/map.md) — `repark.sql`, `ReparkSession` construction, and the
  session-level surface (EX-21: builder snake spellings, active-session trio, conf,
  catalog, frame builders, file readers, memory catalog, temp-view listing, name
  resolution, display style, and the two loud refusals; `registerTempTable` and
  `pandas_api` do not exist on live PySpark 4.1.2 and are taught as the refusals they
  are).
- [window/](window/map.md) — `Window` / `WindowSpec` examples (EX-20: all 22 names;
  snake_case spellings are repark extensions covered beside their camelCase twins; the
  tied-key ordered default frame stays a §7 divergence, EX-WIN-1).
- [types/](types/map.md) — `types.*` examples (EX-22: all 28 names; construction and display
  arms measured Spark-equal on live PySpark 4.1.2; `repark_type_to_arrow` /
  `struct_type_from_arrow` are repark extensions taught beside the mapped types).
- `ml/` — the remaining EX-1 family. It is an
  inventory family with no example yet; the backfill creates the directory (and its
  `map.md`) with the first example it lands there.

## I want to...

| I want to... | go to |
|---|---|
| See which public names still need an example | [backlog.txt](backlog.txt) |
| Add an example for a name | a new `*.py` under the family dir with `COVERS`, then drop the name from the backlog and ratchet `BACKLOG_BASELINE` |
| Record a cloud-only name | [exceptions.txt](exceptions.txt) |
| Run the gate | `make check-example-coverage` |

## Pointers

- Up: [../map.md](../map.md)
- Gate: [../../scripts/check_example_coverage.py](../../scripts/check_example_coverage.py)
- Ruling: [../../task/roadmap/epic-term/release-roadmap-2026-08-29.md](../../task/roadmap/epic-term/release-roadmap-2026-08-29.md)
- Ledger: [../../task/ledgers/archive/2026-09/2026-09-02-ex-0-example-drift-gate-ledger.md](../../task/ledgers/archive/2026-09/2026-09-02-ex-0-example-drift-gate-ledger.md)
- Ledger: [../../task/ledgers/archive/2026-09/2026-09-02-ex-1-class-surfaces-ledger.md](../../task/ledgers/archive/2026-09/2026-09-02-ex-1-class-surfaces-ledger.md)

## Debug

| Symptom | First check |
|---|---|
| `public name … has no example` | Add `COVERS` or a backlog row (new names cannot join the backlog) |
| `backlog still lists` | Remove the name from `backlog.txt` and lower `BACKLOG_BASELINE` |
| `skipping example execution` | Native module is not importable — `make develop` |
