# Pin 6 halt analysis — ICE-TSNS-COERCION-1 slice 1 round 2 (2026-10-12)

Full pin-6 sweep on this head: 9,600 statement measures + 50 reads, compared
against the committed twin fixture (error texts denoised of session-local
table counters only). 8,967 measures and 45 reads equal the twin. The 633
measure and 5 read mismatches fall into six classes. No cell shows head
wrong where main was right; every class is root-caused below with a specimen.

## M1 — fallback on engine-inserted DML store casts: 258 cells, head == main

Doors that write into a `TIMESTAMP` (ltz) target insert `CAST(assignment AS
TIMESTAMP)` before analysis (proven: bare `SET v = c` falls back on ltz and
not on ts_ns; fallback delta 2 on the failing cells). The round-2 fallback
rule trips on that cast, so the statement stays main's — usually main's
error (`No field named __common_expr_1`, the pre-existing UPDATE+CSE bug)
or main's double-shift bug — while the twin succeeds. The cast is
indistinguishable at plan level from a user-written `SET v = CAST(..)`.

Specimen: `UTC|ltz|ns|coalesce|untyped_null|update_nowhere` — head and main
error identically; twin stores `[[1, 1767323045123456], [2, -1]]`.

## M2 — error text quotes the spelling: 80 cells, head == main

`update_scalar_subquery` on ns source (60): sites inside the scalar
subquery cannot resolve columns against merged plan inputs, so the walk
skips them (fallback delta 0, verified) and the statement keeps main's
decorrelation error, which quotes `NULL`. `explain_analyze_update` on the
union form (20): the MERGE rewrite is refused before analysis. The twin
error quotes the kernel, so no statement-side text can ever match it.

Specimen: `UTC|ltz|ns|array|untyped_null|update_scalar_subquery` — all
three error; head and main quote `array(i.ns,NULL)`, twin quotes the kernel.

## M3 — head exceeds the twin: 220 cells, head ns-exact

`union_null_insert`, `df_union_null_append` (all five forms) and
`df_functions_append` (array) on ns targets: head preserves full ns
precision (all 44 UTC cells store the exact seed ticks
`1767323045123456789`/`-1`); main and twin truncate to microseconds
because the door's own union NULL branch stays untyped on main, and the
df twin spelling errors outright. Spark keeps ns here, so head is right
and the twin is wrong or broken on these doors.

Specimen: `UTC|ts_ns|ns|array|untyped_null|union_null_insert` — head
`[[2, -1]]`, main and twin `[[2, -1000]]`.

## M4 — stale fixture error text: 75 cells, head == live main

All `tzns_utc` error cells on `update_scalar_subquery` (60) and
`explain_analyze_update` (15): the recorded text quotes `i.tzns`, but the
live statement on both this head and the pre-slice baseline build quotes
`i.tzns_utc`. Stored values agree; only the recorded column name is stale.

## R1 — union read field-id metadata: 5 reads, values identical

The `tzns_utc|union` reads in all zones: rows, type and nullability equal
the twin; `PARQUET:field_id` is 4 on head, 3 on main and twin. Field 4 is
the `tzns_utc` source column (correct lineage); 3 is main's union-coercion
quirk, also present in main's own statement read.

## Pin 8b

The fallback counter moves across the pin-6 statements (M1 cells fall
back), so the zero-fallback pin fails for the same reason as M1.

## What was already narrowed in this round

The fallback cast check first descended into container targets and caught
the engine-inserted `CAST(array AS List(Timestamp(ns)))`, blocking the
array form everywhere. It now matches the brief's letter (top-level
`Timestamp` target only), with a Rust pin
(`container_timestamp_cast_does_not_fall_back`) and twin-verified
justification (the array/ltz live twin reproduces the recorded twin).
DML store casts are top-level and still trip it — that is M1.
