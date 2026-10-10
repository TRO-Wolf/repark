# `repark.spark.streaming`

## Purpose

The structured-streaming facade: `DataStreamReader`, `DataStreamWriter`, `StreamingQuery`
and `StreamingQueryManager` against the stub binding. Builders, option plumbing, trigger
parsing and every §4 refusal that needs no driver are real; a spec that passes every
check stops at the stub terminal and the wire-up round owns everything past it.

- `readers.py` — **MB-4 surface half (2026-10-08):** `DataStreamReader(session)` and
  `DataStreamWriter(frame)` per the sketch's §3.7 signatures. Argument checks mirror the
  pinned PySpark 4.1.2 source (`VALUE_NOT_NON_EMPTY_STR` on load/outputMode/queryName,
  `NOT_STR` on table, single-trigger/`VALUE_NOT_TRUE` on trigger, no check on
  format/option/foreachBatch/toTable-name/start-path). Options store via the `to_str`
  mirror (bool to lowercase, `None` held) with case-insensitive last-write-wins and
  secret registration; `None` values drop at the door call. `load` falls back to the
  `path` option when the argument is omitted. `start`/`toTable` merge kwargs in PySpark
  order through the validated setters. MBE-6/MBE-7 refuse Python-side with
  `NOT_IMPLEMENTED` (`outputMode(complete|update)`, `readStream.format(<x>)`,
  `trigger(continuous)`, and the symmetric `writeStream.format(<x>)`); the native doors
  raise MBE-3/MBE-17/MBE-4/MBE-10. **MB-4 round 2 (2026-10-08):** `trigger()` parses
  `processingTime` and `continuous` through the native `check_trigger_interval`
  (MB-0c T1/T1B/T1C/T4 grammar) before storing the stripped string; the parsed
  duration threads into the start spec in a later round.
  pins: mb-4/C-020. A missing format refuses as the PySpark default
  `parquet`; `partitionBy` and `path` pass through opaquely. Omitted PySpark members
  (`csv`, `json`, `orc`, `parquet`, `schema`, `text`, `xml`, `clusterBy`, `foreach`,
  `partitionBy`, `resetTerminated`, `processAllAvailable`, `load`'s `format`/`schema`
  kwargs) are wire-up-owned.
  pins: mb-4/C-004, C-005, C-006
  **MB-4 round 2b (2026-10-08):** the reader wire-up: `load`/`table` wrap the native
  streaming frame in a facade `DataFrame` and return it; the format check folds case and
  leaves `table`, `toTable` and the `foreachBatch` start alone; a sourceless `load` is
  `IllegalArgumentException` with Spark's text. pins: mb-4/C-023, C-011, C-013
  **MB-4 round 3 (2026-10-08):** `outputMode` validates through the native check
  (MB-0c O1 at the setter); `start`/`toTable` pass the conf map and the output mode
  and return `StreamingQuery` over the native handle. The MBE-6 refusal moved to Rust
  (`check_output_mode` at the doors). pins: mb-4/C-025
  **MB-4 round 3b (2026-10-08):** `start` passes the session alive token so the
  native foreach adapter can build the batch `DataFrame` against a live session.
  pins: mb-4/C-026
  **MB-4 fold 1 (2026-10-09):** the format refusal moves to the native
  `check_stream_format` (same doors, same feature shapes, values masked);
  `format` checks `NOT_STR` eagerly, `None` still meaning unset.
  pins: mb-4/C-037
  **MB-4 fold 1 (2026-10-09):** `start` and `toTable` refuse MBE-18
  (`NOT_IMPLEMENTED`, Python UDF over a streaming DataFrame) after the session
  check, before the native call. pins: mb-4/C-039
  **MB-4 fold 1b (2026-10-09):** the continuous-trigger refusal, the
  `path`-option lookup and the option de-duplication move to Rust:
  `_start_checks` is deleted (`build_trigger` is the single home, so a
  continuous trigger now refuses after path, checkpoint, unknown options and
  output mode), and `_store_option` / the `load` fallback delegate to the
  native `store_stream_option` / `stream_option_path` over the facade dict.
  pins: mb-4/C-040
  **MB-4 fold 1b (2026-10-09):** `toTable` refuses a non-str table name
  `NOT_STR` at the native-call boundary, after the kwargs, alive and UDF
  checks. pins: mb-4/C-041
  **MB-4 fold 1b (2026-10-09):** `partitionBy(*cols)` is declared
  (SES-DECL): the frame alive check, then `NOT_IMPLEMENTED` naming the
  member. pins: mb-4/C-043
  **MB-4 fold 1b (2026-10-09):** the record-only residue (the 31
  divergent doors, the declared members, the fence record) files under
  STREAM-SURFACE-RESIDUE-1. pins: mb-4/C-044
- `query.py` — **MB-4 surface half (2026-10-08):** `StreamingQuery` carries the §3.7
  surface with the driver-owned bodies behind the stub terminal; only the
  `awaitTermination` timeout check (`VALUE_NOT_POSITIVE`, `<= 0`) is real.
  `StreamingQueryManager(session)` answers `active == []` and `get() is None`
  for real and validates the `awaitAnyTermination` timeout (`< 0`) before the
  terminal. All three manager methods check the session is alive.
  pins: mb-4/C-006
  **MB-4 round 3 (2026-10-08):** `StreamingQuery(handle)` binds the native
  `PyStreamingQuery`; every member delegates to it (`status`/`lastProgress`/
  `recentProgress` parse the native JSON). The manager keeps its stub terminal
  until item 8. pins: mb-4/C-024
  **MB-4 round 3c (2026-10-08):** the manager goes live: `active`/`get`/
  `awaitAnyTermination` delegate to the native `streams_*` doors,
  `resetTerminated` joins the surface, and the stub terminal is gone.
  pins: mb-4/C-028
  **MB-4 fold 1b (2026-10-09):** `get` refuses a non-str id `NOT_STR`
  after the alive check, at the native-call boundary. pins: mb-4/C-041
  **MB-4 fold 1b (2026-10-09):** `processAllAvailable`, `explain`,
  `addListener` and `removeListener` are declared (SES-DECL):
  `NOT_IMPLEMENTED` naming the member, after the alive check where the
  receiver has one. pins: mb-4/C-043
- `__init__.py` — **MB-4 surface half (2026-10-08):** re-exports the four public names.
  pins: mb-4/C-004

## Boundary rules

Python builds options and hands the `foreachBatch` callable; Python never touches rows
and never decides commits. Refusals use the sketch's §4 classes only; anything without
a §4 row passes to the stub terminal and the wire-up round classifies it.

## The `foreachBatch` contract (owner ruling D4, 2026-10-10)

The contract a user reads is the `Notes:` section of `DataStreamWriter.foreachBatch` in
`readers.py`; the registry rows `MB-4-FOREACH-EO-1`, `MB-4-FOREACH-SIDE-EFFECTS-1` and
`MB-4-FOREACH-SINK-SHAPES-1` hold the measurements behind it.

- **The declared sink** is the table named by `.option("repark.cdc.sink", ...)`. The
  callable's one write to it commits the batch, and its rows land exactly once.
- **Effects before the sink commit are at-least-once.** A batch that fails before the commit
  runs again under the same batch id, and so does everything the callable did before it.
- **Effects after the sink commit are at-most-once on a failure.** The batch is durable once
  the sink commit lands. If the callable raises or the process dies after it, the callable
  does not run again for that batch, and what it had not yet done is not done.
- **The recipe:** write the sink last, or key the side effect on the batch id. Keying on the
  batch id is the recipe Spark documents for `foreachBatch`. Where the two engines differ is
  the second half: Spark replays the whole callable after a failure (cell MB0-W6), so a body
  ported from Spark that relied on that must put its sink write last.
- **The sink is exclusive for rows.** While a query name lives, a snapshot on the sink's
  main branch that carries no batch stamp ends the query `RecoveryRequiredException`, and
  every later start refuses with no callable run until it is resolved (fold 4, 2026-10-10:
  loud on every start, where fold 3 was loud once for a snapshot under the batch's own
  stamp). The driver looks above the query's newest stamp and between its two newest stamps
  (down to the head the query started on, which the first stamp records as
  `repark.cdc.starting-head`), at every start and before every callable. Since fold 5 the
  stretch under the newest stamp is reported whether or not its lower end is still in the
  table (an expiry may have removed it), and a `toTable` start under a name that has run
  through `foreachBatch` (its mark, or any of its `foreachBatch` stamps, is still on the
  sink) is refused the same way. A healthy sink is not refused after an expiry.
- **A stamp is not committed over a stray (fold 5).** If an unstamped snapshot lands on the
  sink after a batch began, the callable's own sink write is refused before it lands; the
  batch ends `RecoveryRequiredException` with the stray above the newest stamp, where every
  start finds it and no expiry can remove it.
- **A limit.** A stray that an earlier build of this branch left under a stamp, followed by
  an expiry that keeps only the newest snapshot, cannot be seen: the stray's snapshot is
  gone with every bound, and the next start runs on. This build no longer produces that
  state. Until card MB-SINK-MAINTENANCE-PATH-1 lands, run expiry on a sink with a live
  `foreachBatch` query so that it retains the newest two stamps and everything between.
- **The error says what resolves it, and each recipe is exact when followed.** Above the
  newest stamp: roll the sink back to the named snapshot and start again, or keep the rows
  and start under a new name with the printed `repark.cdc.start-after-snapshot-id`. Under a
  stamp: a rollback to the newest stamp does not remove it, so the text names the snapshot
  below it and the new name's start position; the old name stays refused. Where the driver
  cannot prove a recipe it does not print it: no rollback on a sink that started empty or
  where another query's batches share the stretch, no new name when the batch ends inside a
  source snapshot. A new name without the printed position delivers every stamped batch
  again; the text says so.
- **What the check does not look at.** Between batches, a commit that adds no snapshot to
  the main branch (a table property, a schema change, a branch, a tag, a staged or branch
  write, statistics) passes; inside a batch such a change ends that batch
  `RecoveryRequiredException` once and the next start runs on. Another streaming query's
  stamped batches pass. A batch write cannot pass by forging a stamp: a `repark.cdc.*`
  snapshot property from writer options or the session conf is refused by name. There is
  no maintenance path against a live sink yet: card
  `task/roadmap/mid-term/mb-sink-maintenance-path-1-card-2026-10-10.md`.
- **Session settings.** A streaming plan runs under the session settings of the moment its
  batch starts (time zone, ANSI mode, the batch's own `current_timestamp()`), so both doors
  store what the same statement stores as a batch write. `current_timestamp()` is one value
  for the whole micro-batch on both doors: the frame a callable receives carries it as a
  constant, as Spark's does (fold 5). The `toTable` door refuses a sink
  with a nested `timestamp_ns` leaf with the batch doors' text, and the `foreachBatch` door
  refuses a keyed sink (ENC-1) before it writes anything.
- **`checkpointLocation` holds no state.** It is required and recorded. The state is the sink
  plus the query name; clearing the checkpoint resets nothing.

Python holds none of this: the callable is handed to the driver, which owns every rule.
pins: mb-4-foreach-eo/C-031, C-033, C-040, C-041, C-044

## Known limitations

`spark.readStream` and `spark.streams` answer since the wire-up round (on main both
refuse); the session example `docs/examples/session/streaming_entry_points.py` covers the
two names, and the example that listed them among the refusals no longer does (fold 4,
2026-10-10: the example-coverage CI job was red on the branch). pins: mb-4-foreach-eo/C-038
The valid-spec terminal, writer unknown options, the missing-format default, the
trigger interval grammar, and a sourceless `load()` are halt-pending; see the MB-4
ledger's OPEN clauses. The IPI-47 cells answer through this surface per the ledger's
C-007.
pins: mb-4/C-007
