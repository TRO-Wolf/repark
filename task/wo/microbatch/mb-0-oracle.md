# MB-0 — the streaming oracle on Spark 4.1.2 + Iceberg 1.11.0                       grade: C   engine band: clerk (Muse at max)   release: 1.7

> **North Star briefing (owner, 2026-10-05).** Every executor and verifier on this order reads [the CDC and micro-batch North Star](../../roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md) first: NS-1…NS-19, the authority order in §2 (Flink governs guarantees, Spark the surface, Iceberg's own sinks the commits, else refuse with a dated row), the Rust placement in §4, and the production-grade lists in §5. The North Star sits below every ruled O/D/CC/ES/CL row and above the agent's judgment. For a question no ruled row answers, write the four lines (the question, Flink's answer, Spark's answer, the NS default), act on the default, and record the four lines as a dated ledger row. Halt only for the three §8 cases.


## 0. Why and what is out of scope

The v1.6.0 card's step 0 stands as slice MB-0: every design question the
sketch closes is answered from recorded Spark plus Iceberg cells, not from
documentation. This order records those cells on Spark 4.1.2 + Iceberg
1.11.0 with local catalogs, commits them verbatim with a SHA-256, and stops.
Out of scope, named: any RePark code, any design ruling, any fork change, any
Kafka or continuous-mode cell.

This order runs only after the orchestrator's bisect timing run hands back.

## 1. Rulings already made

- O-1…O-10 plus O-9a as ruled in [packet.md](packet.md) §2; nothing is
  decided inside this round.
- The cell list below is exhaustive for MB-0: 24 cells, MB0-R1…R13 (read),
  MB0-W1…W8 (write), MB0-T1…T3 (triggers and progress).
- A cell that Spark answers with an error records the error class and the
  full text verbatim; a cell that Spark answers with rows records the rows
  and their types.

## 2. Files

| path | kind |
|---|---|
| `python/repark-parity/tests/live_spark/mb0_streaming_oracle.py` | new: the recorder, one function per cell, each printing one JSON object |
| `python/repark-parity/tests/live_spark/mb0_streaming_oracle.json` | new: the 24 cells, committed verbatim |
| `python/repark-parity/tests/live_spark/mb0_streaming_oracle.sha256` | new: the SHA-256 of the JSON file |
| `python/repark-parity/tests/live_spark/map.md` | edited: one row per new file |

No Rust file is touched. No ceiling applies to a new Python file under the
default ceiling of 1000 (`scripts/check_lib_py.py`).

## 3. Skeletons

The recorder exposes `record_all(spark) -> dict`, returning one entry per
cell id. Each entry has exactly these fields:

```
{"cell": "MB0-R1", "statement": "<the Spark code, verbatim>",
 "kind": "rows|error|summary|progress|checkpoint",
 "answer": <rows with types | {"class": ..., "text": ...} | {key: value}>,
 "field": "<the JSON field name from §4>"}
```

The JSON file is the list of the 24 entries in cell order, pretty-printed
with sorted keys. The `.sha256` file holds `<sha256>  <filename>`.

## 4. Steps

Step 1. Provision one Spark 4.1.2 + Iceberg 1.11.0 session over a local
catalog (Hadoop or in-memory; record which in the JSON preamble).

Step 2. Run the recorder through the managed interpreter only:

```
systemd-run --user --scope --slice=repark.slice env JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 /tmp/sparkenv/bin/python python/repark-parity/tests/live_spark/mb0_streaming_oracle.py
```

Step 3. Record the 24 cells. For every cell: the Spark statement, the
expected kind of answer, and the JSON field it lands in.

Read side (a streaming read over a table the recorder appends to, overwrites,
deletes from, and rewrites between triggers):

| cell | statement | kind | field |
|---|---|---|---|
| MB0-R1 | `readStream.format("iceberg")` over appends only, `availableNow`, `collect` per batch | rows | `read.appends.rows` |
| MB0-R2 | an overwrite snapshot lands in the window, no skip options | error class + text | `read.overwrite_unskipped.error` |
| MB0-R3 | an overwrite snapshot lands, `streaming-skip-overwrite-snapshots=true` | rows (the skip) | `read.overwrite_skipped.rows` |
| MB0-R4 | an overwrite snapshot lands, only `streaming-skip-delete-snapshots=true` | error class + text (kind isolation) | `read.overwrite_wrong_skip.error` |
| MB0-R5 | a delete snapshot (row-level delete) lands, no skip options | error class + text | `read.delete_unskipped.error` |
| MB0-R6 | a delete snapshot lands, `streaming-skip-delete-snapshots=true` | rows (the skip) | `read.delete_skipped.rows` |
| MB0-R7 | a delete snapshot lands, only `streaming-skip-overwrite-snapshots=true` | error class + text (kind isolation) | `read.delete_wrong_skip.error` |
| MB0-R8 | a `replace` snapshot (`rewrite_data_files` compaction) lands, no skip options | rows or error, verbatim (decides packet Q4 and the F-APPEND `replace` pin) | `read.replace_unskipped.answer` |
| MB0-R9 | a `replace` snapshot lands, `streaming-skip-overwrite-snapshots=true` | rows or error, verbatim | `read.replace_skip_overwrite.answer` |
| MB0-R10 | `stream-from-timestamp` set to a mid-history timestamp | rows (which snapshots stream) | `read.from_timestamp.rows` |
| MB0-R11 | `streaming-max-files-per-micro-batch=1` over a multi-file snapshot | rows per batch + batch count | `read.max_files.batches` |
| MB0-R12 | `streaming-max-rows-per-micro-batch=1` over a multi-row snapshot | rows per batch + batch count | `read.max_rows.batches` |
| MB0-R13 | read the checkpoint directory after R11: the stored offset | checkpoint JSON (snapshot id + position), verbatim | `read.checkpoint.offset` |
| MB0b-R14 | `stream-from-timestamp` = head timestamp + 1 h, then an append below it; `availableNow` before and after the append (same and fresh checkpoint), plus a later snapshot at or after T | rows per batch + `progress_batches` + offsets | `read.from_timestamp_past_head.rows` |
| MB0b-R15 | `streaming-max-rows-per-micro-batch` = 3, 4, 5 over three 2-row one-file appends, and over one 3-file snapshot | rows per batch + end offsets | `read.max_rows_crossing.batches` |
| MB0b-R16 | the table's first snapshot is an `INSERT OVERWRITE` on the empty table, then an append; stream from earliest | error class + text, verbatim | `read.first_snapshot_overwrite.answer` |

Write side (a streaming write into an Iceberg sink; the restart cells kill
the query between batches):

| cell | statement | kind | field |
|---|---|---|---|
| MB0-W1 | `writeStream.format("iceberg")`, `append` mode, `fanout-enabled=false`, one batch | summary keys | `write.append_no_fanout.summary` |
| MB0-W2 | `writeStream.format("iceberg")`, `append` mode, `fanout-enabled=true`, one batch | summary keys | `write.append_fanout.summary` |
| MB0-W3 | `writeStream.format("iceberg")`, `complete` mode | rows/summary or error, verbatim (decides packet Q3) | `write.complete.answer` |
| MB0-W4 | `foreachBatch` appending each batch frame into an Iceberg table via a batch write | snapshots + summary keys per batch | `write.foreach_batch.snapshots` |
| MB0-W5 | restart from the checkpoint after a committed batch | rows (no duplicates) | `write.restart_committed.rows` |
| MB0-W6 | kill mid-batch (uncommitted), restart from the checkpoint | rows (replay, no duplicates) | `write.restart_uncommitted.rows` |
| MB0-W7 | the snapshot summary of one micro-batch commit: every key and value | summary keys | `write.summary_keys.summary` |
| MB0-W8 | omit `checkpointLocation` on an Iceberg sink | error class + text, or rows if Spark allows it (decides packet Q1) | `write.no_checkpoint.answer` |

Triggers and progress:

| cell | statement | kind | field |
|---|---|---|---|
| MB0-T1 | `trigger.once()` on an Iceberg stream | rows or error, verbatim (decides packet Q2) | `trigger.once.answer` |
| MB0-T2 | `trigger.availableNow()` over two pending batches | rows per batch + stopped flag | `trigger.available_now.batches` |
| MB0-T3 | `lastProgress` / `status` after one batch; `stop` then `awaitTermination` | progress JSON verbatim + call outcomes | `trigger.progress.json` |

**MB-0b (2026-10-07, MB-1 fold 1).** The three MB0b rows above were added after
the verifier's FAIL on PR #979 to measure its S1/S2 bytecode readings before
implementing them. Recorded with `MB0_CELLS=MB0b-R14,MB0b-R15,MB0b-R16`; the 24
MB-0 entries and the preamble stayed byte-identical. Measured answers:

| cell | answer |
|---|---|
| MB0b-R14 | Nothing streams while no snapshot has a timestamp at or after T: each run writes `START_OFFSET` (snapshot `-1`, position `-1`) and an empty batch, before the append, resumed after it, and fresh. Once a snapshot at or after T lands, only that snapshot streams (rows `[12]`, end `(ordinal 2, position 1)`); the earlier append below T never does. Agrees with the verifier. |
| MB0b-R15 | A file is added, then the batch stops once its rows reach the cap (`>=`); the crossing file stays in. Max 3 and max 4 give `[4][2]`, max 5 gives `[6]`, the same over three snapshots and over one. End offsets for three snapshots: max 3/4 `(1,1)` then `(2,1)`; max 5 `(2,1)`. Agrees with the verifier; the sketch's §3.3 "until the next would exceed" rule is not Spark's. |
| MB0b-R16 | `STREAM_FAILED` / `IllegalStateException`: `Cannot process overwrite snapshot: <first id>, to ignore overwrites, set streaming-skip-overwrite-snapshots=true`. No batch runs. The overwrite on the empty table added two data files. Agrees with the verifier. |

Step 4. Write the JSON and its SHA-256:

```
sha256sum python/repark-parity/tests/live_spark/mb0_streaming_oracle.json > python/repark-parity/tests/live_spark/mb0_streaming_oracle.sha256
```

Step 5. Commit the three files plus the `map.md` row in one commit.

## 5. Gates and their expected output

- `python -m json.tool python/repark-parity/tests/live_spark/mb0_streaming_oracle.json > /dev/null` — exit 0.
- `sha256sum -c python/repark-parity/tests/live_spark/mb0_streaming_oracle.sha256` — `: OK`.
- `bash scripts/check_map_md.sh --base origin/main` — exit 0.
- `uvx typos@1.47.2 python/repark-parity/tests/live_spark/mb0_streaming_oracle.py` — exit 0.
- The recorder re-run is byte-identical apart from timestamps, snapshot ids
  and query ids; the hand-back lists which fields vary.

## 6. Halt rules

1. Fewer than 24 cells record: halt with the missing ids.
2. A cell Spark answers differently on re-run (beyond ids and timestamps):
   halt with both answers, verbatim.
3. The Spark or Iceberg version under `/tmp/sparkenv` is not 4.1.2 / 1.11.0:
   halt with the measured versions.
4. Any ambiguity in a statement above: halt rather than choosing a reading.

## 7. Hand-back

`{"status":"DONE|HALT","cells":24,"sha256":"<hex>","volatile":["<json paths that vary per run>"],"questions":[]}`
plus the commit sha. The JSON file is the deliverable; the hand-back carries
no cell content beyond the hash.
