# map — python/repark/tests/iceberg/held_frame

## Purpose

ICE-HELD-FRAME-SNAPSHOT-1 unit 1a (2026-11-20): a held Iceberg frame, or a DataFrame view over one, read or written
after a write to its own table in the same session answers the table as of the action, as Spark 4.1.2 re-binds at
execution (`V2TableRefreshUtil`), while a frame's own-plan actions keep their first binding and the time-travel forms
stay pinned. The parent [../map.md](../map.md) is at its size ceiling, so the pins live in this directory.

## Contents

- `test_ice_held_frame_snapshot_1.py` — 46 cells (23 shapes in both ANSI modes) against the recorded Spark answers:
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
- `_ice_held_frame_snapshot_1_cells.py` — the cells, engine-agnostic, shared by the test and the recorder.
- `_ice_held_frame_snapshot_1_enc_1_cells.py` — the ENC-1 cells (fix round 1), RePark only.
- `_record_ice_held_frame_snapshot_1.py` — re-records the oracle on live PySpark 4.1.2 with Iceberg 1.11.0:
  `python _record_ice_held_frame_snapshot_1.py <runtime jar> <empty warehouse dir>`.
- `ice_held_frame_snapshot_1_spark_oracle.json` — the recorded Spark answers, keyed by ANSI mode.

Up: [../map.md](../map.md).
