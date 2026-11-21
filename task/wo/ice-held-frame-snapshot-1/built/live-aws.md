# ICE-HELD-FRAME-SNAPSHOT-1 — the held-frame cells for the live Glue and S3 Tables run (2026-11-21)

By Claude Opus 5.5 (`claude-opus-5-5`), effort xhigh. Tests and records only: no product code and no workflow change.
The owner, 2026-11-20: "The live Glue and S3 Tables run comes after 1a merges and before 1b builds." 1a merged on
2026-11-21 (#1390, `347eff83`). The owner, 2026-11-21, also asked for `held_filter_count`'s twin row in this run.
The cells are the ones [sketch.md](../sketch.md) §6 names. `N` is the session scratchpad, which is outside the
repository.

## Where the cells live

- **The body:** [`_acceptance_held_frame.py`](../../../../python/repark/tests/parity/_acceptance_held_frame.py), a
  non-collected module beside `_acceptance_replace.py`. It imports nothing outside the standard library, so the same
  code runs on RePark (live and local) and on Spark (the measurement below). Spark's answers are its constants
  `SPARK_ROWS` and `SPARK_EXTERNAL`. `spark_answer` applies Spark's cache interval to them.
- **The live tests:** eight tests at the end of
  [`test_aws_acceptance.py`](../../../../python/repark/tests/parity/test_aws_acceptance.py), one Glue and one S3
  Tables test per family: `test_held_frame_{rows,external_writer,refresh,cost}_against_{glue,s3tables}`. They use the
  module's skip gate, the scratch namespace `testing_repark_acceptance`, `run_table_prefix()` names,
  `assert_real_buckets_configured`, the Glue location guard, and the S3 Tables skip when `TABLE_BUCKET_ARN` is unset.
  The acceptance step already runs this module with `-q -rA`, so no workflow change is needed.
- **The local twin:**
  [`test_ice_held_frame_snapshot_1_live_twin.py`](../../../../python/repark/tests/iceberg/held_frame/test_ice_held_frame_snapshot_1_live_twin.py)
  runs the same body on a `type=hadoop` catalog through the facade, in both ANSI modes.

**Tables.** Each cell makes its own table, `<run prefix>held_<cell>_<8 hex>`, with the module's
`ICEBERG_TABLE_PROPERTIES` (format 2, copy-on-write), seeded `(1, 'a'), (2, 'b')`. The copy cell adds `_c` and `_f`.
That is 9 tables per catalog per run. None is dropped or deleted. The leg's cleanup removes them by the run prefix,
and the cross-read step reads them first. The row `DELETE` statements live in the body module, as
`_acceptance_v3.py`'s do, so the harness's no-`DELETE FROM` guard over `test_aws_acceptance.py` and `_acceptance.py`
still holds.

## The cells

Spark 4.1.2 with `iceberg-spark-runtime-4.1_2.13-1.11.0`, `local[1]`, a `type=hadoop` catalog, zone UTC. Both ANSI
modes give the same answer in every cell. "main" is main's package `pkg-a1m-bb8ae8fc` (its product tree equals
`0cc65e9a`'s) on the twin's `type=hadoop` catalog, both modes. Rows are sorted `repr`s; `(1,a)` is `(1, 'a')`.

| family | cell | steps (one table each) | Spark | main, local | live tests |
|---|---|---|---|---|---|
| `HW` | `insert` | hold `spark.table(t)`; `INSERT (9, 'z')`; the held rows; a fresh frame's rows | `(1,a) (2,b) (9,z)`; the same | equal | `test_held_frame_rows_against_*` |
| `HW` | `delete` | hold; `DELETE … WHERE n = 1`; the held `count`; a fresh `count` | `1`; `1` | equal | the same test |
| `HW` | `copy` | hold; `DELETE … WHERE n = 1`; `held.writeTo(<t>_c).create()`, a fresh frame's `writeTo(<t>_f).create()`; the rows each stores | `(2,b)`; `(2,b)` | equal | the same test |
| `X` | `control` | hold; held `count`; an external `INSERT (9, 'z')`; held `count`, held rows, fresh `count`; a same-session `INSERT (8, 'q')`; held and fresh `count`; `catalog.refreshTable(t)`; held and fresh `count` | `2`; `2`, `(1,a) (2,b)`, `2`; `4`, `4`; `4`, `4` | `2`, then the writer refuses: `table 'lc.db.held_control_…' not found` | `test_held_frame_external_writer_against_*` |
| refresh | `refresh` | hold; an external `INSERT (9, 'z')`; held and fresh `count`; `REFRESH TABLE t`; held and fresh `count` | `2`, `2`; `3`, `3` | the writer refuses: `table 'lc.db.held_refresh_…' not found` | `test_held_frame_refresh_against_*` |
| cost | `held_read` | below | not asserted | printed | `test_held_frame_cost_against_*` |
| cost | `held_filter_count` | below | not asserted | printed | the same test |

The `HW` rows are the pin file's `insert_collect`, `delete_count` and `delete_copy_stored`, each with its fresh half.
The refresh cell is the one that decides Q10 and Q2 on the label's catalogs (sketch §6, §7).

**The external writer.** Live, it is `spark.newSession()` of the reader's session. RePark builds a new engine handle
from the same builder config, so the writer has its own catalog instance and its own session table state
(`SessionTables` is a session-config extension; see the
[session_tables map](../../../../crates/repark-iceberg/src/catalog/session_tables/map.md)). Its commit reaches the
reader only through the catalog's table pointer: Glue's `GetTable`, or the S3 Tables metadata location. The writer is
stopped when the cell ends.

**Why Spark's answer transfers.** Spark's answers were measured with two writers: Spark's `newSession()` (its own
`SparkCatalog` and `CachingCatalog`) and a second Spark process over the same warehouse. The answers are the same.
Spark's reader therefore answers from its own catalog instance's cache, whoever commits outside it. A second RePark
session commits outside it in the same way. The catalog under the cache does not change the rule. `SparkCatalog.initialize`
wraps whatever catalog it builds, of any `type` or `catalog-impl`, in `CachingCatalog.wrap` when `cache-enabled` is
true (the default) and `cache.expiration-interval-ms` is not 0. That is read from the runtime jar's bytecode (`javap -c`,
`N/ice-live-out/jar/`). Spark on Glue or on the S3 Tables REST endpoint was not measured.

**The local twin cannot show an external writer.** On `type=hadoop` a second RePark session cannot see the table, so the
two external cells are strict xfails there. The reason is the registry's ICE-CATALOG-SESSION-HADOOP-1 alias (domain
table §6; card HADOOP-FACADE-FRESH-SESSION-1). The refusal itself is pinned
(`test_the_local_catalog_refuses_the_second_writer_by_name`), so the twin fails if that changes.

## Spark's cache interval (measured)

`CachingCatalog` keeps a table for `cache.expiration-interval-ms`, 30 s by default, **after its last access**
(`expireAfterAccess`, with soft values; jar bytecode). Each reader access (`spark.table`, an action that re-binds) is
a cache read. The probe `N/ice-live-out/probes/spark_cells.py` runs the committed body on Spark with a pause before a
chosen reader access. `idle` is the reader's idle seconds before each access, as the body records it. Every row is the
same in both ANSI modes. Raw output: `N/ice-live-out/spark/cells-{true,false}.json`.

| cell | writer, catalog | pause | idle before each access (s) | Spark's answer |
|---|---|---|---|---|
| refresh | session, cached | none | 0.13, 0.00 | `2, 2, 3, 3` |
| refresh | process, cached | none (the writer's JVM took 17.9 s) | 17.89, 0.00 | `2, 2, 3, 3` |
| refresh | session, cached | 35 s before the held `count` | 35.11, 0.00 | `3, 3, 3, 3` |
| refresh | session, cached | 35 s before the fresh `count` | 0.12, 35.00 | `2, 3, 3, 3` |
| refresh | session, cached | 20 s before each (40 s after the hold) | 20.12, 20.00 | `2, 2, 3, 3` |
| refresh | session, `cache-enabled=false` | none | 0.12, 0.00 | `3, 3, 3, 3` |
| control | session, cached | none | 0.00, 0.15, 0.00, 0.00 | `2, 2, (1,a) (2,b), 2, 4, 4, 4, 4` |
| control | process, cached | none (17.9 s) | 0.00, 17.96, 0.00, 0.00 | `2, 2, (1,a) (2,b), 2, 4, 4, 4, 4` |
| control | session, cached | 35 s before the held `count` | 0.00, 35.10, 0.00, 0.00 | `2, 3, (1,a) (2,b) (9,z), 3, 4, 4, 4, 4` |
| control | session, cached | 35 s before the held rows | 0.00, 0.12, 35.00, 0.00 | `2, 2, (1,a) (2,b) (9,z), 3, 4, 4, 4, 4` |
| control | session, cached | 35 s before the fresh `count` | 0.00, 0.12, 0.00, 35.00 | `2, 2, (1,a) (2,b), 3, 4, 4, 4, 4` |
| control | session, cached | 20 s before the held `count` and the rows | 0.00, 20.12, 20.00, 0.00 | `2, 2, (1,a) (2,b), 2, 4, 4, 4, 4` |
| control | session, `cache-enabled=false` | none | 0.00, 0.12, 0.00, 0.00 | `2, 3, (1,a) (2,b) (9,z), 3, 4, 4, 4, 4` |

**The finding.**

1. **Yes, the answers change once the interval has passed.** The first reader access after an idle gap over 30 s
   loads the table again and sees the external commit, and every access after it does too. Before that, Spark
   answers its cached table.
2. **The interval measures idle time, not age.** Two 20 s gaps, 40 s after the hold, keep the cached answer.
3. **A refresh or a same-session commit does not depend on the interval.** After `REFRESH TABLE` the answers are
   `3, 3`. After a same-session commit they are `4, 4`, because the commit's retry refreshes the cached table.
4. **With the cache off, Spark answers the expired values at once.** So the `2, 2` before a refresh is the cache's
   answer, not a rule of the held frame.

**What the live cells assert.** Each external cell prints its `idle_s`. It asserts `spark_answer(label, idle_s)`: the
cached answer while no reader gap passed 30 s, and the measured expired answer from the first gap that did. The twin
pins that rule on every measured row above
(`test_the_pinned_spark_answer_follows_the_measured_cache_interval`). The rule has two limits. It uses the reader's idle
time as RePark spends it on the live run, so it gives Spark's answer for the same idle pattern, not Spark's own timing.
And a gap within about a second of 30 s could go either way on Spark.

## RePark on Glue and S3 Tables: expected answers (read from the code, not measured)

1a's seat A records every `load_table` (Q2 (b)). A fresh `spark.table(t)` on Glue or S3 Tables is a planning load (one
`GetTable`, or one metadata-location call; inventory R7, R8), so it sees an external commit at once and moves the
session's entry. A held frame re-binds from that entry and makes no catalog call.

| cell | read prediction | Spark |
|---|---|---|
| `HW` rows | Spark's answers (the same mechanism as the local pins) | the table above |
| `control` | `2, 2, (1,a) (2,b), 3, 4, 4, 4, 4` | the fresh `count` is `2` |
| `refresh` | `2, 3, 3, 3` | the fresh `count` is `2` |

If the prediction holds, the two external cells are red on both catalogs at one value each: the fresh read after
another writer's commit, before a refresh, where Spark answers its cached table. The brief asks for exactly that
measurement. The held frame agrees with Spark in both cells.

## Local-only

The module never drops or deletes a Glue table, namespace or S3 Tables table. So these cells are not live cells, and
they stay on the local catalog:

- The pin file's cells that need `DROP TABLE` or a recreate: `recreate_count`, `dropped_table_count`,
  `dfview_recreate`, `recreate_explain` and `dropped_table_explain`, each in both ANSI modes.
- D1's shape, a frame built over a DataFrame view, then a recreate (1a delta 2's `dfview_frame_recreate_*`,
  `dfview_frame_dropped_table_*`, `dfview_written_frame_recreate_count` and `dfview_filter_frame_recreate_count`).
- The domain table's writes `wt_create` (create after a `DROP`) and `recreate`, in every family (`HW`, `WA`, `HA`,
  `B`), and §8.2's "hold `db.t`; `DROP`; create `db2.t`".
- Any table `RENAME TO`: Glue has no rename, so Iceberg's Glue catalog deletes the old entry.

The reverse case also holds: the two external cells run only live, since `type=hadoop` cannot show them (above).

## How to read a run log

The acceptance step's `-rA` log carries the captured output of each passed test in its PASSES section, and a failed
test's in its failure. `grep -E '^ICE-HELD-(CELL|COST) '` finds the lines.

- `ICE-HELD-CELL label=<cell> catalog=<glue|s3tables> {"answer": …, "spark": …}`: one line per cell, printed before
  its assertion. For the external cells, `answer` holds the answer and `idle_s`.
- `ICE-HELD-COST label=<row> catalog=<glue|s3tables> n=<calls> held_median_ms=<ms> fresh_median_ms=<ms> ratio=<held over fresh>`:
  one line per cost row, never asserted. `parse_cost_line` in the body reads one. The fields are in this order and
  separated by spaces, each a `key=value` pair.
  - `held_read`: no write. Per call, a frame is held before the timer and then its `collect()` is timed. It is set
    against `spark.table(t).collect()`, whose resolution is inside the timer. Ten calls each, interleaved, after one
    warm read.
  - `held_filter_count` (the twin of 1a's flagged row): per call, a frame `spark.table(t).filter("n >= 0")` is held,
    one row the filter drops is inserted, and then the held `count()` is timed. It is set against one insert and a
    fresh frame's `filter("n >= 0").count()`. Ten calls each, interleaved.
  - A ratio below 1 means that a held frame costs less than a fresh one. On Glue and S3 Tables the difference is
    expected to be the planning load the fresh frame makes and the held frame does not (read). These are readings
    from a live run, not a C-012 sample.
- Main's local lines, from two twin runs under the shared measure lock (so not a cost sample): `held_read` 1.033
  and 0.791 ms held against 1.728 and 1.333 ms fresh (0.598 and 0.594); `held_filter_count` 3.818 and 3.841 ms
  against 5.502 and 5.147 ms (0.694 and 0.746).

## Gates

On the tests commit `5056f936`, with main's package `pkg-a1m-bb8ae8fc` and its native library printed in every run.
Each command ran under `measure.sh shared` on cores 32 to 47 and exited 0.

| gate | result |
|---|---|
| `python/repark/tests/iceberg/held_frame/` (the twin and the pin file) | 96 passed, 4 xfailed (the twin's strict xfails) |
| `python/repark/tests/parity/` collected, `REPARK_AWS_ACCEPTANCE` unset | 858 collected, no error; the 8 new live tests skip |
| `test_aws_acceptance.py` and the module's source pins (`test_acceptance_helpers.py`, `_ephemeral_runs`, `_v3_helpers`, `_replace_offline`) | 126 passed, 19 skipped |
| `test_aws_acceptance_job_split.py`, `test_live_v3_docs.py`, `test_v3r_1_rulings.py` | 40 passed |
| the package facade at `-n 6` over `python/repark/tests/iceberg/` | 2,570 passed, 299 skipped, 17 xfailed |
| `make check-docs-links` (offline), `sync_map_md.py --check`, `check_map_size.py` | clean |
| `typos` 1.47.2, `ruff` 0.15.22 `check .` and `format --check .` | clean |
| the comment gate against `0cc65e9a` | 0 |

Up: [map.md](map.md).
