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
- `__init__.py` — **MB-4 surface half (2026-10-08):** re-exports the four public names.
  pins: mb-4/C-004

## Boundary rules

Python builds options and hands the `foreachBatch` callable; Python never touches rows
and never decides commits. Refusals use the sketch's §4 classes only; anything without
a §4 row passes to the stub terminal and the wire-up round classifies it.

## Known limitations

The builder surface is reachable module-direct only; `spark.readStream` and
`spark.streams` keep refusing exactly as on main until the wire-up round flips them.
The valid-spec terminal, writer unknown options, the missing-format default, the
trigger interval grammar, and a sourceless `load()` are halt-pending; see the MB-4
ledger's OPEN clauses. The IPI-47 cells answer through this surface per the ledger's
C-007.
pins: mb-4/C-007
