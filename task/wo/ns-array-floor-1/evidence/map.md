# task/wo/ns-array-floor-1/evidence/ — measured cells

- [input-cells.md](input-cells.md) + [input-cells.json](input-cells.json) —
  D1: what each pandas and Arrow door receives for a timestamp_ns column, a
  timestamptz_ns column, the two U-1 widened shapes, and a microsecond
  control, at top level and inside an array, on main, the U-1 head, and live
  Spark 4.1.2 (control only).
- [output-cells.md](output-cells.md) + [output-cells.json](output-cells.json) —
  D2: what each door stores for a returned datetime64[ns] array or column
  under array and top-level timestamp declarations, on main and live Spark
  4.1.2, frame ticks and read-back ticks.

Up: [../map.md](../map.md).
