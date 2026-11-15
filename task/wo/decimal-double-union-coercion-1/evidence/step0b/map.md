# task/wo/decimal-double-union-coercion-1/evidence/step0b/ — step 0′ of the build, on main

Step 0′ of DECIMAL-DOUBLE-UNION-COERCION-1 under the owner's split plan (2026-11-14, "Merge and go with the split
plan"). This is part 1 (seat E, K2, K1 on written casts, Spark door only), re-measured on main `dc6b4c94` after #1335,
#1336 and #1337 merged. No product code. Step 0 on the stack base `ff34baf5` stays in [../step0/](../step0/map.md)
as history. The inventory answers are [../../inventory.md](../../inventory.md) section I.

- [table.md](table.md) is the rendered table. It covers:
  - the joined cells per section and their parts;
  - what moved since `6f9f2aaa`;
  - the signed-zero shapes;
  - the emulated head on the fuzz, the structured set and the DML cells;
  - the kernels;
  - Spark's own answers for the widened forms.
- [probes/](probes/map.md) holds the step-0′ runners and the analysis.
- [cells/](cells/map.md) holds the joined cells and the counts.
- [raw/](raw/map.md) holds the small raw sets: DML, `F.expr`, the zero fold and the rule lists.

## Provenance

- **Builds:**
  - main is `pkg-main-dc6b4c94` in the session scratchpad (`BUILT_FROM` `dc6b4c94`, a fresh release build);
  - the recorded `ff34baf5` and `6f9f2aaa` answers are step 0's raw sets, re-joined, not re-run;
  - each raw file's `provenance` row names the loaded `_native.abi3.so` relative to the scratchpad.
- **Oracle:**
  - PySpark 4.1.2 on Zulu JDK 17, run fresh for this step;
  - `local[1]`, or `local[2]` for the fuzz, structured and kernel sets;
  - UI off, session zone UTC;
  - writes go through `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` into a private hadoop catalog per run.
  - Spark's fresh run equals its 2026-11-14 recording except for one whole-shape overflow message (the table's
    section 9).
- **When and where:** 2026-11-15, about 09:40 to 10:20 UTC, lane 1. Every run held the shared measure lock on cores
  32 to 47, and every Spark JVM ran through the shared Spark slot.
- **Commands:**
  1. The sketch's runners and step 0's runners, unchanged, on main and on Spark, in both ANSI modes ([../step0/map.md](../step0/map.md)).
  2. From the `probes/` directory:
     - `coerce_head.py <spark|repark> <true|false> <out> <seat_e|k1|k1k2>` (Spark's forms, and main's `seat_e`);
     - `coerce_head_k1.py <true|false> <out> <k1|k1k2> <kernel out>` (main's `k1` and `k1k2`);
     - `kernels.py <spark|repark> <true|false> <out>`;
     - `family_head.py <spark|repark> <true|false> <out>`;
     - `dml_head.py <spark|repark|native> <true|false> <warehouse parent> <iceberg jar> <out>`;
     - `zero_fold.py <out>` (on `6f9f2aaa` and on `dc6b4c94`).
  3. The analysis:
     - `../step0/probes/analyze_step0.py <spark raw> <main raw> <ff34 raw> <joined>`;
     - `analyze_step0b.py <joined> ../step0/cells <cells>`;
     - `analyze_head.py <spark raw> <main raw> <ff34 raw> <ff34 widened raw> <head raw> <cells>`;
     - `render_step0b.py <cells>`, whose output is the table.

Up: [../map.md](../map.md).
