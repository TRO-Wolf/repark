# ICE-NTZ-NESTED-WALL-1 progress

**Step:** 1 committed. `zone_store_frame` calls `nested_micro_ntz_wall_sql`.

**Walk commit:** `e926fbe89e3eec9e9eaaff6284b948a5c1f37d25`

**Red pin:** `evidence/pin-red-step1.txt`. On the parent, America/New_York and
Asia/Kolkata store `1767323045123456`. On the commit the three zones store the
session wall.

**Next:** step 2, one commit per site: `zone_wrapping_stream_sql`,
`store_assignment_cast_sql`, `update_projection_sql`,
`insert_timestamp_ns::after_analysis`.
