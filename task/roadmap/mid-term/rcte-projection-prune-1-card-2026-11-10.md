# Card RCTE-PROJECTION-PRUNE-1: a recursive CTE that selects only some of its columns fails on both doors

**Date:** 2026-11-10. **Filed by:** the records clerk (`claude-haiku-5-5`), from the scoped review of STRUCT-SETOP-TYPE-FAMILY-1 (PR #1290, finding F-7).

**Status:** filed 2026-11-10, not dispatched; S3. Valid recursive CTEs fail. Pre-existing: `b8ad7a9c` and #1290's head behave the same.

**Why:** the measured forms, on the native door and on facade SQL:
1. `WITH RECURSIVE r AS (SELECT 1 AS n, 'x' AS s UNION ALL SELECT n + 1 AS n, 'y' AS s FROM r WHERE n < 2) SELECT s FROM r` fails with a Join Error. The cause is a caught panic, "primitive array", at `arrow-array-58.4.1/src/cast.rs:849`. The process survives.
2. The `s`-first projection fails with `Arrow error: Schema error: project index 1 out of bounds`.
3. On facade SQL, `SELECT s, n FROM r` fails with `field data type differs (Int64 vs Int32)`.
Spark 4.1.2 answers all three forms.

**Open question for the owner:** the review did not test whether the caught panic can abort under `panic = "abort"` or another runtime. That decides whether the card is a release blocker.

**Close:** the three forms answer as Spark does, with a pin on each door.
