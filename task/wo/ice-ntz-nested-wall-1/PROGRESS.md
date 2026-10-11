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

**Next:** `store_assignment_cast_sql`, then `update_projection_sql`, then
`insert_timestamp_ns::after_analysis`.
