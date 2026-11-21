# Card ICE-EXPIRED-FILE-READ-ERROR-1: an own-plan collect of a held frame raises on both engines after INSERT OVERWRITE and expire_snapshots, with different errors

**Date:** 2026-11-21. **Filed by:** the records clerk (`claude-haiku-5-5`). **Source:** the residual card line of ICE-HELD-FRAME-SNAPSHOT-1 unit 1a (fix round 1, F3, dated 2026-11-20), in [cards.md](../../../wo/ice-held-frame-snapshot-1/built/cards.md).

**Rating: S3, not label-blocking.**

**Status:** filed 2026-11-21; not dispatched.

- An own-plan `collect` of a held frame after `INSERT OVERWRITE` and `expire_snapshots` (cell `D|table|own_collect_ow_expire_collect`, both ANSI modes) raises on both engines because the data file is gone, with different errors. Spark 4.1.2 raises `SparkException` ("Job aborted due to stage failure", a missing-file cause). RePark raises `External error: DataInvalid => Failed to open file <path>`.
- The binding agrees: the `count` after it answers 1 on both. Only the error class and text differ, and they differ the same way on main `9a48932f`.

Up: [map.md](map.md).
