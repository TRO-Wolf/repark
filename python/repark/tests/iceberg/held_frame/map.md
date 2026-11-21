# map — python/repark/tests/iceberg/held_frame

## Purpose

ICE-HELD-FRAME-SNAPSHOT-1 unit 1a (2026-11-20): a held Iceberg frame, or a DataFrame view over one, read or written
after a write to its own table in the same session answers the table as of the action, as Spark 4.1.2 re-binds at
execution (`V2TableRefreshUtil`), while a frame's own-plan actions keep their first binding and the time-travel forms
stay pinned. The parent [../map.md](../map.md) is at its size ceiling, so the pins live in this directory.

## Contents

- `test_ice_held_frame_snapshot_1.py` — 60 cells (30 shapes in both ANSI modes) against the recorded Spark answers:
  re-bound reads, deleted rows stored by `writeTo().create()` and `insertInto`, the self-append, own-plan `collect`,
  `explain` and `localCheckpoint`, `TABLE_ID_MISMATCH`, `COLUMNS_MISMATCH` (a dropped or widened column, and an added
  one under a derived frame's write), a DataFrame view after a recreate and after a dropped column, a dropped and
  re-added column read by name, a reordered column, `rollback_to_snapshot`, a pinned `VERSION AS OF`, a join with a
  fresh read, `TRUNCATE`, and a dropped table. 38 are red on main `9a48932f`; the 8 own-plan, checkpoint and
  time-travel cells are EQUAL on both (fix round 1 recount; round 0 wrote 40 and 6). pins: ice-held-frame-snapshot-1/1a.
  Fix round 1 (ENC-1, owner 2026-11-20), RePark only, since Spark has no table encryption gate: 4 cells where a held
  frame read after a refused write to a keyed table answers as before the write (and as a fresh read), and 9 pairs
  that read and write through a held frame, a DataFrame view over one, or a frame held across a same-session commit
  that adds or removes `encryption.key-id`, each against the same action on a fresh frame. Every pair is identical,
  refusal text included, and is pinned. pins: ice-held-frame-snapshot-1/1a-enc-1.
  Delta 1 (2026-11-20): 14 of the 60 Spark cells (7 shapes) pin `explain` after a change. `explain()`, and a child's,
  raise `COLUMNS_MISMATCH` after a dropped column, `TABLE_ID_MISMATCH` after a recreate and `TABLE_OR_VIEW_NOT_FOUND`
  after a drop; `explain(True)` prints the condition; SQL `EXPLAIN` over a DataFrame view returns rows that name the
  view's condition; `explain` after an insert prints a plan. 12 are red on main and 10 on the fix round 1 head. The 4
  refused-write cells read the held frame by `count` and by a child, since `collect` keeps its first binding.
  pins: ice-held-frame-snapshot-1/1a-delta-1.
- `test_ice_held_frame_snapshot_1_live_twin.py` — the live Glue and S3 Tables cells' local twin (2026-11-21): the
  helper `../../parity/_acceptance_held_frame.py` on a `type=hadoop` catalog in both ANSI modes. The `insert`, `delete`
  and `copy` rows answer Spark's held and fresh answers; the external-writer and refresh cells are strict xfails
  (ICE-CATALOG-SESSION-HADOOP-1: a second session cannot see the table, pinned by its refusal); the cache-interval
  rule of Spark's pinned answers is pinned on the measured pauses; the cost rows print parseable lines.
  pins: ice-held-frame-snapshot-1/live-aws.
- `_ice_held_frame_snapshot_1_cells.py` — the cells, engine-agnostic, shared by the test and the recorder.
- `_ice_held_frame_snapshot_1_enc_1_cells.py` — the ENC-1 cells (fix round 1), RePark only.
- `_record_ice_held_frame_snapshot_1.py` — re-records the oracle on live PySpark 4.1.2 with Iceberg 1.11.0:
  `python _record_ice_held_frame_snapshot_1.py <runtime jar> <empty warehouse dir>`.
- `ice_held_frame_snapshot_1_spark_oracle.json` — the recorded Spark answers, keyed by ANSI mode.

Up: [../map.md](../map.md).
