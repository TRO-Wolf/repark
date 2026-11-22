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
  Tables test per family: `test_held_frame_{rows,shapes,cost,spark_shapes}_against_{glue,s3tables}` (the external
  writer and refresh families were replaced on 2026-11-22; see the LIVE-SECOND-WRITER-1 section). They use the
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

**Superseded on 2026-11-22.** The `X` and refresh rows above, and the text below from "The external writer" to
the end of "RePark on Glue and S3 Tables: expected answers", describe the `spark.newSession()` cells. Live run
`44955277129` showed that the second RePark session could not find the table, so those cells measured nothing
(card [SESSION-CATALOG-TABLE-VISIBILITY-1](../../../roadmap/mid-term/cards-2026-11-21/session-catalog-table-visibility-1-card-2026-11-21.md)).
They were removed. The text stays as the record of that measurement. Their replacement is the LIVE-SECOND-WRITER-1
section.

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

## LIVE-SECOND-WRITER-1: a second Spark process as the other writer (2026-11-22)

By Claude Opus 5.5 (`claude-opus-5-5`), effort xhigh. Tests and records only: no product code and no workflow change.
The source is the orchestrator's brief and its addendum from the Fable adviser the owner named. The owner adopted
the rule these cells grade on 2026-11-22. Fable measured it on Spark 4.1.2 with Iceberg 1.11.0, with a second Spark
process as the other writer:

- **Table-level cache** (`spark.table(t).cache()`, the same with `.alias("a")`, `CACHE TABLE t`): every read of `t` in
  the session, fresh reads included, answers from the cache until `REFRESH TABLE` or a same-session data write. A
  same-session `SET TBLPROPERTIES` does not unpin it.
- **Query-level cache** (anything built above the table): answers what the session's latest read of the table answers,
  and is never staler than a fresh read taken at the same moment.

`S` below is this lane's scratchpad, which is outside the repository.

### What changed

- Removed: `test_held_frame_external_writer_against_*`, `test_held_frame_refresh_against_*`, the body's `control` and
  `refresh` cells, `SPARK_EXTERNAL`, `spark_answer` and `session_writer`, and the twin's strict xfails and interval pins
  for them. Kept as they were: the `HW` row cells and the cost rows. Their tests pass no writer now.
- Added: `test_held_frame_shapes_against_{glue,s3tables}`, which is the RePark reader leg, and
  `test_held_frame_spark_shapes_against_{glue,s3tables}`, which is the Spark reader leg. Both use the same writer process.

### The other writer

[`_acceptance_held_frame_writer.py`](../../../../python/repark/tests/parity/_acceptance_held_frame_writer.py) is a
separate OS process running pyspark 4.1.2 with the Iceberg runtime. Each test starts one writer, which stays up for
that test. `SparkProcessWriter` in the body sends the writer its Spark conf as the first stdin line, never in argv.
Then it sends one JSON line per write. For each write the writer inserts `(9, 'z')`, reads its own `count(*)` back,
and answers with one `ICE-HELD-WRITER {…}` line.

- **Why one long-lived process, not one script per write.** A fresh writer JVM took 17.9 s in the earlier probe
  (the table above), and on CI it also resolves the packages again. Seven of those per test would put the Spark leg's
  first read after each write at or past Spark's 30 s catalog-cache interval, and it would land differently from cell
  to cell. With one process, the first write's gap was 3.3 s locally and each later one about 0.3 s, so each read lands
  in the same window. The writer writes each table once and never reads it before that write, so its own
  `CachingCatalog` never serves it a stale table.
- **The conf** (`spark_conf`). It has the keys of the cross-read module's `_spark_session_for_catalog`: ANSI on, UTC,
  `local[2]`, two shuffle partitions, the UI off, the Iceberg extensions, and `spark.jars.packages` with the runtime and
  the AWS bundle. It uses the same catalog blocks: `glue_catalog_config` and `s3tables_spark_catalog_config`. It adds
  `spark.driver.bindAddress` and `spark.driver.host` set to `127.0.0.1`. The workflow sets `SPARK_LOCAL_IP` to that
  address for its other Spark steps, but not for the acceptance step. The Python is `resolve_live_spark_python()`: on CI
  that is the acceptance venv, which has pyspark from the `record` extra. `setup-java` provides Java 17. No workflow
  change is needed.
- **Isolation.** The writer shares neither the reader's JVM (it is its own process) nor its RePark engine (it never
  imports repark).
- **The log.** The writer's stdout and stderr go to the test's output line by line as `ICE-HELD-WRITER-LOG <line>`.
  ARNs and 12-digit ids are scrubbed by the `scrub` that `condition()` uses.
- **Timeouts.** The first reply has 900 s, because CI resolves the packages then. Each write has 300 s. A missed reply
  kills the process, and the cells after it fail at once.

### The shapes

There is one table per shape, per catalog, per leg: `<run prefix>held_<shape>_<8 hex>`, with
`ICEBERG_TABLE_PROPERTIES`, seeded `(1, 'a'), (2, 'b')`. That makes 14 tables per catalog (7 shapes, 2 legs). With the 7 the row and cost cells
make, it is 21 per catalog and 42 per run; the removed control and refresh cells made none. None is dropped. The leg's cleanup removes them, and the cross-read step reads them first: 14 more per catalog in its walk.

| shape | kind | held frame | new frame of the same shape |
|---|---|---|---|
| `bare` | table | `spark.table(t).cache()` | `spark.table(t)` |
| `alias` | table | `spark.table(t).alias("a").cache()` | the same build |
| `cache_table` | table | `CACHE TABLE t`, then `spark.table(t)` | `spark.table(t)` |
| `sql_star` | query | `spark.sql("SELECT * FROM t").cache()` | the same build |
| `filter` | query | `spark.table(t).filter("n > 0").cache()` | the same build |
| `agg` | query | `spark.table(t).groupBy("c").count().cache()`; its `count` is the number of groups | the same build |
| `df_view` | query | `spark.table(t)` as a temp view, read by name, cached | the view registered again and read (Fable's `view_of`) |

Steps:

1. Cache, then `before`: the held `count`, which must be 2.
2. The writer's insert and its read-back.
3. Four reads: `held` (the held `count`), `fresh` (`SELECT count(*)`), `new` (a new frame's `count`), and `held_again`.
4. `ALTER TABLE t SET TBLPROPERTIES ('k' = 'v')`, then the four reads.
5. `REFRESH TABLE t`, then the four reads.
6. `catalog.clearCache()`.

`idle_s` holds the reader's idle seconds before each of the 13 reads (12 gaps), as the body recorded them before.

### The grading

**Validity, on both legs and for every shape.** The cell fails on any of these: an error in a step (making the table,
caching, the write, the `ALTER`, the `REFRESH`); `before` other than 2; a writer read-back other than `{"count": 3}`;
or a fresh read after `REFRESH TABLE` other than 3. So a run whose writer did not land, or whose reader never sees it,
cannot pass. The twin shows this with a writer that the `type=hadoop` reader cannot see.

**The RePark leg** (`grade(shape, found, "repark")`). Each step lists the held, fresh, new and held-again reads:

| shapes | after the writer | after the property commit | after `REFRESH TABLE` |
|---|---|---|---|
| `bare`, `alias` | 2; open; open; 2 | 2; open; open; 2 | 3; 3; 3; 3 |
| `cache_table` | all four open | all four open | all four open (the fresh read is also the validity read) |
| `sql_star`, `filter`, `agg`, `df_view` | 2 or 3; recorded; the fresh read; the fresh read | 3; 3; 3; 3 | 3; 3; 3; 3 |

"Open" is recorded under `open: fresh-read-while-table-cached` and is never asserted. The owner has not ruled whether
RePark must match Spark's 2 there in #1394. Every read of `cache_table` on this leg is open (addendum 1). Any read in
`PENDING_1394` is recorded under `pending: #1394` and is not asserted (addendum 2); #1394 delta 2 empties it (below).

**The Spark leg** (`grade(shape, found, "spark")`). It asserts only the window-independent rows of Fable's table: for
`bare`, `alias` and `cache_table`, every read is 2 until `REFRESH TABLE` and 3 after it, fresh reads included; for the
query-level shapes, every read after `REFRESH TABLE` is 3. The rest is recorded. Fable's three measured windows (cache
off, inside 30 s, after 35 s) pass both legs' grading in the twin, the RePark leg with no pending exemption.

**Errors in reads that are not asserted.** An error in an asserted read fails that read, since it is not the wanted
value. An error in an open, pending or recorded read is recorded and does not fail the cell. That follows the
addendum ("recorded only, not asserted") over the brief's "any error fails the cell". It keeps main green where the
rule is not asserted. Errors in the steps still fail the cell. The orchestrator should confirm this choice.

### Main's predicted answers (written before any live run)

Two predictions. Each step lists held, fresh, new and held again.

- **A, the adviser's.** Held frames answer 2 everywhere, after `REFRESH TABLE` included. Fresh reads (the `SELECT` and
  the new frame) answer 3.
- **B, read from main's code** (`python/repark/src/repark/spark/catalog_surface.py`). `spark.table(name)` calls
  `_drop_stale_identity_frames`. That unpersists a cached frame that `spark.table` returned once the frame's baseline
  no longer matches. The baseline is the table's `current-snapshot-id` at `cache()`, read by `DESCRIBE TABLE EXTENDED`,
  or for a view the view's registration object. `refresh_table` unpersists every such frame. So `bare` and `df_view`
  answer 3 from the first `spark.table` call after the commit, which is the new-frame read. `alias`, `filter`, `agg`
  and the SQL frame are not such frames and answer 2 everywhere. The SQL door does not consult the cache overlay, so
  the fresh `SELECT` answers 3. `ALTER TABLE … SET TBLPROPERTIES` loads the table through the catalog before it commits
  (`set_table_properties`), so it is predicted to commit on Glue and S3 Tables. The local `CatalogCommitConflicts` in
  Fable's RePark run comes from the in-process catalog never seeing the other process's commit.

| shape | after the writer | after the property commit | after `REFRESH TABLE` |
|---|---|---|---|
| `bare`, `df_view` | A: 2, 3, 3, 2. B: 2, 3, 3, 3 | A: 2, 3, 3, 2. B: 3, 3, 3, 3 | A: 2, 3, 3, 2. B: 3, 3, 3, 3 |
| `alias`, `sql_star`, `filter`, `agg` | 2, 3, 3, 2 | 2, 3, 3, 2 | 2, 3, 3, 2 |
| `cache_table` | held 2, fresh 3, new 3 (the entry drops at that `spark.table(t)`); held again not read to the end | as after the writer | as after the writer; all open |

**`PENDING_1394`** is the union of the reads where A or B breaks the rule. The twin pins it as exactly that union. It
has 27 entries: `bare` and the four query-level shapes, each with held again after the writer, and held and held
again after the property commit and after `REFRESH TABLE`; `alias`, with held and held again after `REFRESH TABLE`.
B alone adds three: `bare`'s held again after the writer, and its held and held again after the commit. To promote
the list once #1394 lands, empty it. The twin's Spark-window cells already show that Spark's answers pass with no
exemption. **#1394 delta 2 (2026-11-22) empties it**: every one of the 27 reads is one the delta makes RePark answer
by the rule ([unit-2.md](unit-2.md) §10), so after the merge they are live assertions. The twin now pins main's two
predictions breaking the rule at exactly those 27 reads, and the head's predicted answers passing with no exemption.

On main, the RePark leg therefore asserts the validity reads and these:

- `bare` and `alias`: the first held read after the writer answers 2.
- `alias`: held again after the writer, and held and held again after the commit, answer 2.
- Every shape but `cache_table`: the fresh and new reads after `REFRESH TABLE` answer 3.
- The query-level shapes: the first held read answers 2 or 3, the new frame after the writer equals the fresh read,
  and the fresh and new reads after the commit answer 3.

Both predictions pass all of these.

### The local Spark run

The reader is Spark (`spark_session` in the body) and the writer is the committed writer script as a subprocess, on
one `type=hadoop` warehouse, Spark 4.1.2 with `iceberg-spark-runtime-4.1_2.13-1.11.0`. It ran through `spark_slot.sh`
and `measure.sh shared` on cores 32 to 47. The probe is `S/live2-out/spark_local.py`, with raw output in
`S/live2-out/spark-local/{window,nocache,expired}.{json,log}`. Each run exited 0. In every cell, all four reads of a
step gave one number:

| run | table-level shapes (after the writer, after the commit, after `REFRESH`) | query-level shapes | Fable's table | match |
|---|---|---|---|---|
| reader cache on, no pause | 2, 2, 3 | 2, 3, 3 | "inside 30 s" | 7 of 7 shapes, every step |
| reader `cache-enabled=false` | 2, 2, 3 | 3, 3, 3 | "catalog cache off" | 7 of 7 |
| reader cache on, 35 s before the first read after the writer | 2, 2, 3 | 3, 3, 3 | "after 35 s" | 7 of 7 |

In all three runs both legs' grading passed every shape, the RePark leg with no pending exemption. `before` was 2 and
the writer's read-back was 3 in every cell.

### What stays open

- `open: fresh-read-while-table-cached`. The owner has not ruled on it. The RePark leg records it, and the Spark leg
  asserts Spark's 2.
- `pending: #1394`: empty since #1394 delta 2; the 27 reads are asserted once it merges. The revert trigger is in
  [unit-2.md](unit-2.md) §10.
- RePark's own answer is measured only live. Locally, `type=hadoop` cannot show another process's commit to RePark,
  so the twin covers RePark's run of the body and the grading, not RePark's answers.
- Not measured before this run: Spark's `ALTER` and `REFRESH` on S3 Tables through the REST block (the flat-matrix and
  CTAS-failure modules already run Spark creates there). A refusal fails that leg's cells at the step, and the record shows it through `condition()`.
- Run time. A guess: about 4 to 6 minutes per RePark-leg test and 5 to 8 per Spark-leg test, inside the job's 90
  minutes and each test's 1,200 s. The cross-read walk also gains 28 tables.

### How to read the new lines

`grep -E '^ICE-HELD-(CELL|COST|WRITER-LOG) '`.

- `ICE-HELD-CELL label=shape_<shape> leg=<repark|spark> catalog=<glue|s3tables> {…}`: one line per shape per test,
  printed before the test's assertion. Its keys:
  - `answer`: the cell, with `idle_s`.
  - `failed`: read and reason.
  - `open: fresh-read-while-table-cached`, `pending: #1394`, `recorded`: each maps a read to its answer.
- `ICE-HELD-WRITER-LOG <line>`: the writer process's output, scrubbed.

### Gates

Each command ran under `measure.sh shared` on cores 32 to 47 and exited 0. The facade runs used main's package
`pkg-main-dac2af61`, with its native library printed.

| gate | result |
|---|---|
| the twin, `test_ice_held_frame_snapshot_1_live_twin.py` | 68 passed |
| `python/repark/tests/iceberg/held_frame/` (the twin and the pin file) | 163 passed |
| `test_aws_acceptance.py`, `REPARK_AWS_ACCEPTANCE` unset | 19 skipped, no error (8 held-frame tests, as before) |
| `test_aws_acceptance.py` and its source pins (`test_acceptance_helpers.py` with the no-`DELETE FROM` guard, `_ephemeral_runs` with the literal scan, `_v3_helpers`, `_replace_offline`) | 126 passed, 19 skipped |
| `test_aws_acceptance_job_split.py`, `test_live_v3_docs.py`, `test_v3r_1_rulings.py` | 40 passed |
| `python/repark/tests/parity/` collected | 858 collected, no error |
| the local Spark run, three windows | exit 0 each; 21 of 21 shape rows match Fable's table |
| `make py-lint`, `make py-format-check` (ruff 0.15.22) | clean |
| `sync_map_md.py --check`, `check_map_size.py`, `typos` 1.47.2 | clean |
| `make check-docs-links check-ledgers check-ledger-grammar` (offline) | clean |
| comments, docstrings and `noqa` added outside Markdown | 0 |

Up: [map.md](map.md).
