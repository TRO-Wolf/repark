# ICE-HELD-FRAME-SNAPSHOT-1 unit 1a — residual cards for the orchestrator to file

Fix round 1 (2026-11-20), F3. One line each; the orchestrator files them. The cells are in [bar.md](bar.md), "The cells
round 0 left open".

- **ICE-EXPIRED-FILE-READ-ERROR-1 (S3, loud on both engines, not label-blocking).** An own-plan `collect` of a held
  frame after `INSERT OVERWRITE` and `expire_snapshots` (cell `D|table|own_collect_ow_expire_collect`, both ANSI modes)
  raises on both engines because the data file is gone, with different errors: Spark 4.1.2 raises `SparkException`
  ("Job aborted due to stage failure", a missing-file cause), RePark raises `External error: DataInvalid => Failed to
  open file <path>`. The binding agrees (the `count` after it answers 1 on both); only the error class and text differ,
  and they differ the same way on main `9a48932f`.

Two of round 0's open cells already have cards, so they are not carded again here:
[STRING-CAST-LENIENT-1](../../../roadmap/mid-term/string-cast-lenient-1-card-2026-11-16.md) (`M|held_cast`) and
[LOCAL-CHECKPOINT-NEW-FRAME-1](../../../roadmap/mid-term/local-checkpoint-new-frame-1-card-2026-10-03.md)
(`local_checkpoint_after_collect`).

Up: [map.md](map.md).
