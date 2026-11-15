# task/wo/decimal-double-union-coercion-1/evidence/step0/ — step 0 of the build, on the stack base

Step 0 of DECIMAL-DOUBLE-UNION-COERCION-1, stacked on DOUBLE-DECIMAL-STORE-ROUND-1's head `ff34baf5` (owner,
2026-11-14, "Go with recommendation"). No product code. Every cell has Spark's answer, main's and the stack
base's; the inventory answers are [../../inventory.md](../../inventory.md) section H.

- [table.md](table.md) — the rendered table: counts per section, ANSI mode and build; the fuzz and family
  sets; the cells that move between the builds; the wrong writes; the signed-zero shapes; the emulated head.
- [rule-lists.md](rule-lists.md) — the live analyzer lists on both doors and both builds.
- [probes/](probes/map.md) — the step-0 runners and the analysis.
- [cells/](cells/map.md) — the joined cells and the counts.
- [raw/](raw/map.md) — the small raw sets (DML and `F.expr`).

## Provenance

- **Builds.** Main: the package built from `6f9f2aaa` (`dd-finish/pkg-base` in the session scratchpad). Main is
  now `340aba98`, whose only product change (#1331) is a native-door write refusal for views, database sources
  and in-memory overwrites; no cell here writes through the native door's DataFrame writer, so `6f9f2aaa`
  stands for main. The stack base: a package built from `ff34baf5` for this step (`ddu-work/pkg-ff34`). Every
  raw file's `provenance` row names the loaded `_native.abi3.so` relative to the scratchpad.
- **Oracle.** PySpark 4.1.2 on Zulu JDK 17, `local[1]` (`local[2]` for the fuzz and family sets), UI off,
  session zone UTC, shuffle partitions 1; writes through `iceberg-spark-runtime-4.1_2.13-1.11.0.jar` into a
  private hadoop catalog per run.
- **When and where.** 2026-11-14, about 06:00–07:00 local time, lane 1: every run under the shared measure lock
  on cores 32–47, every Spark JVM through the shared Spark slot.
- **Commands.** The sketch's runners, unchanged ([../map.md](../map.md) lists them), for the domain table on
  both builds and on Spark; then, from `probes/`: `dml.py <spark|repark|native> <true|false> <warehouse-parent>
  <iceberg-jar> <out> [widened]`; `coerce.py` and `coerce_widened.py <spark|repark> <true|false> <out>`;
  `family.py <spark|repark> <true|false> <out> [widened]`; `fexpr_extra.py <spark|repark> <true|false> <out>
  <the sketch's probes directory>`; `dec_to_double.py <spark|repark> true <out>`; then
  `analyze_step0.py <spark raw> <main raw> <ff34 raw> <cells>`, `analyze_widened.py <spark raw> <main raw>
  <ff34 raw> <widened raw> <cells>` and `render_step0.py <cells>`, whose output is [table.md](table.md).
- **The Spark re-measure** of the sketch's cells agrees with the sketch's own Spark recording; main's re-measure
  differs from the sketch's `04d84ab9` recording only in the signed-zero shapes (#1326) and the `-0.0` union
  controls.

Up: [../map.md](../map.md).
