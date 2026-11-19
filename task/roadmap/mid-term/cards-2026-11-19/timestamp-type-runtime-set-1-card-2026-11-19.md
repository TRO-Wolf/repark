# Card TIMESTAMP-TYPE-RUNTIME-SET-1: a runtime `SET spark.sql.timestampType` is echoed by `conf.get` and silently ignored by the engine

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under Fable's guidance of 2026-11-19 (the owner's delegation, about 01:28 UTC). **Source:** the Fable reply of 2026-11-19 in the agents' lessons folder (file `Fable reply — CSV schema timestamp zone unit 0 2026-11-19.md`), §2, kept outside the repository; the named exception in the [unit 0 bar](../../../wo/csv-schema-timestamp-zone-1/bar.md).

**Status:** filed 2026-11-19; not dispatched. It is the named exception in the CSV-SCHEMA-TIMESTAMP-ZONE-1 bar for the 14 cells, which keep the builder's answers and not main's.

**Severity: S2, not label-blocking** (Fable's guidance under the owner's delegation of 2026-11-19, about 01:28 UTC; Fable's §2 rating: "S2, not label-blocking. It is silent, but it needs a non-default key set at runtime, and the documented opt-in works.").

## Finding

**[R]** `python/repark/src/repark/spark/session/timestamp_type.py` says the engine resolves the key once at build, that a runtime set does not re-resolve the DDL mapping, and that the opt-in is `builder.config`. The defect is not in the DDL parse. Fable named it in place of the working name `DDL-TIMESTAMP-TYPE-CONF-1`, which the tree does not hold.

**[M]** One row `2024-01-01 03:00:00`, zone Los Angeles, `schema("n INT, ts TIMESTAMP")`:

| key set by | build | reader | type | `unix_timestamp` | `hour` | string |
|---|---|---|---|---|---|---|
| runtime `SET` | main | CSV | `timestamp` | 1704078000 | 19 | `2023-12-31 19:00:00` |
| runtime `SET` | main | JSON | `timestamp` | 1704106800 | 3 | `2024-01-01 03:00:00` |
| runtime `SET` | head | CSV | `timestamp` | 1704106800 | 3 | `2024-01-01 03:00:00` |
| runtime `SET` | head | JSON | `timestamp` | 1704106800 | 3 | `2024-01-01 03:00:00` |
| builder | main | CSV | `timestamp` | 1704078000 | 19 | `2023-12-31 19:00:00` |
| builder | main | JSON | `timestamp_ntz` | 1704078000 | 3 | `2024-01-01 03:00:00` |
| builder | head | CSV | `timestamp_ntz` | 1704078000 | 3 | `2024-01-01 03:00:00` |
| builder | head | JSON | `timestamp_ntz` | 1704078000 | 3 | `2024-01-01 03:00:00` |

- **[M]** Under the builder opt-in the head answers Spark in all three columns, and main does not. Spark's values are the builder's recording (`1704078000`, `3`, the wall-clock string).
- **[M]** Under a runtime `SET` the head's CSV equals JSON on both builds. The JSON cell was already WRONG on main in `unix_timestamp`. Unit 0 moved CSV onto the answer the rest of the engine already gives.
- **Main:** under a runtime `SET`, the CSV read answers `unix_timestamp` 1704078000, `hour` 19 and the string `2023-12-31 19:00:00`.
- **Spark:** not run in the consult. Its values in the table are the builder's recording (builder rows) or Fable's reading.

**First step** (Fable's §2): measure `SET` then CTAS into an Iceberg v2 table. If the column type differs from Spark's, the rating goes to the owner. Then choose between honouring the key and refusing the `SET` loud.

Up: [map.md](map.md).
