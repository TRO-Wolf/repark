# ICE-NTZ-NESTED-WALL-1 progress

**Step:** 1 committed. `zone_store_frame` calls `nested_micro_ntz_wall_sql`.

**Walk commit:** `e926fbe89e3eec9e9eaaff6284b948a5c1f37d25`

**Red pin:** `evidence/pin-red-step1.txt`. On the parent, America/New_York and
Asia/Kolkata store `1767323045123456`. On the commit the three zones store the
session wall.

**Step 2, merge insert:** `1aa27854810ecaa157717faf718f8fa0c3c62963` calls the same walk from
`zone_wrapping_stream_sql`. Red on the parent: the column-list door stores
`1767323045123456` in America/New_York and Asia/Kolkata
(`evidence/pin-red-step2.txt`). Green on the commit.

**Step 2, store assignment:** `c3a63d17ab7df8f718583c36c97b34f047abf8b3` calls
the same walk from `store_assignment_cast_sql`. A caller that has a source
type passes it. `store_assignment_then_sql` passes `None` because it has the
SQL text and the target type only. The Spark fold passes the probed type for
a top-level struct and `None` when the probe does not plan. A path longer
than one step stays on `nested_leaf_cast_sql`. Red on the parent: MERGE
UPDATE from a column stores `1767323045123456` in America/New_York and
Asia/Kolkata (`evidence/pin-red-step2-store.txt`). Green on the commit: that
door and UPDATE from a sibling `TIMESTAMP` column store the session wall. A
wall literal stays `1767323045123456`. A field assignment stays the session
wall. A nested `TIMESTAMP` leaf stays the instant.

**Next:** `update_projection_sql`, then `insert_timestamp_ns::after_analysis`.
