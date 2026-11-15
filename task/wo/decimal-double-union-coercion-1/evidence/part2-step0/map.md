# task/wo/decimal-double-union-coercion-1/evidence/part2-step0/ — step 0 of part 2, on main `a06c1088`

Step 0 of DECIMAL-DOUBLE-UNION-COERCION-1 part 2 (seat V, the tripwire, the `IN` rewrite; step 4 of the owner's split
plan). No product code. Part 1's evidence is [../step0b/](../step0b/map.md) and [../fix/](../fix/map.md). The
inventory answers are [../../inventory.md](../../inventory.md) section J.

- [table.md](table.md) is the measured table: the 83 seat-V cells per mode, the new cells by family, the `IN` plan
  shapes and the `IN` cost rows.
- [probes/](probes/map.md) holds the runners.
- [cells/](cells/map.md) holds the joined cells.
- [raw/](raw/map.md) holds the raw sets.

## Provenance

- **Builds:**
  - main is the scratchpad package `pkg-ddu1-7b21bde3`. Its `BUILT_FROM` reads `7b21bde3`, but its product tree
    equals main `a06c1088`'s: `git diff --name-only 7b21bde3 a06c1088 -- crates python/repark/src Cargo.toml
    Cargo.lock` lists only two `map.md` files;
  - dc6b is `pkg-main-dc6b4c94` (main before part 1), for the moved-since column and the cost baseline;
  - each raw file's `provenance` row names the loaded `_native.abi3.so` relative to the scratchpad.
- **Oracle:** PySpark 4.1.2 on Zulu JDK 17, `local[1]`, UI off, session zone UTC, shuffle partitions 1, writes
  through `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` into a private hadoop catalog per run.
- **When and where:** 2026-11-15, about 13:05 to 13:35 UTC, lane 1, cores 32 to 47. Probes held the shared measure
  lock; Spark JVMs ran through the shared Spark slot; the cost rows held the exclusive measure lock and every build
  lock, with nothing else of the lane queued.
- **Commands:**
  1. The sketch's runners and step 0's `dml.py` / `fexpr_extra.py`, unchanged, on main in both ANSI modes, joined
     with step 0′'s Spark raw sets by `../step0/probes/analyze_step0.py` and tagged by
     `../step0b/probes/analyze_step0b.py` against [../step0b/cells/](../step0b/cells/map.md).
  2. `probes/p2_cells.py <spark|repark> <true|false> <warehouse parent> <iceberg jar> <out>` on Spark, main and
     dc6b, both modes; joined into [cells/p2-joined.jsonl](cells/p2-joined.jsonl).
  3. `probes/in_cost.py <label> <cost data> <out> <case,…>` on main and dc6b, three alternating runs; the cost data
     is part 1's `cost_data.py` output (100M rows).

Up: [../map.md](../map.md).
