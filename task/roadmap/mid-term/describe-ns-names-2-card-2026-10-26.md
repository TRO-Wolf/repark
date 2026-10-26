# Card DESCRIBE-NS-NAMES-2: the remaining DESCRIBE surfaces show the Iceberg spec names for nanosecond columns, through one renderer seam

**Date:** 2026-10-26. **Filed by:** Claude Haiku 5.5 (`claude-haiku-5-5`), clerk, from the NS-TRUTHUP-1 verifier round at head `d28b04f2` (order [task/wo/ns-truthup-1/order.md](../../wo/ns-truthup-1/order.md), section 8).

**Status:** filed, not scheduled. Grade: clerk. It is the residue of registry row `ICE-TSNS-SQL-1-R-001` ([docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)). The DataFrame and DDL surfaces named under Out of scope are an owner question, not this card's work.

## Why

NS-TRUTHUP-1 (owner decision D3) made `DESCRIBE TABLE` and `DESCRIBE TABLE EXTENDED` show `timestamp_ns` and `timestamptz_ns` for top-level nanosecond columns. The change is one private function, `describe_ddl_type_name` in `crates/repark-spark/src/describe_show.rs`, which delegates every other type to `spark_ddl_type_name`. The other DESCRIBE surfaces call `spark_ddl_type_name` directly, so they still show the microsecond names:

- `DESCRIBE <table> <column>`: `crates/repark-spark/src/describe_column.rs` (the column batch and the partition path).
- The DESCRIBE partition section: `describe_column.rs`, and the partition struct type in `describe_show.rs`, which is a struct and so delegates.
- Nested leaves inside `DESCRIBE TABLE`: the seam takes only a top-level type. Reproduced by the orchestrator on the head module, 2026-10-26, on a v3 table `(ts timestamp_ns, st STRUCT<v: timestamp_ns>, arr ARRAY<timestamp_ns>)`: `ts timestamp_ns`, but `st struct<v:timestamp_ntz>` and `arr array<timestamp_ntz>`.
- `DESCRIBE TABLE` on a view: `crates/repark-spark/src/view_ddl/describe.rs` and `view_ddl/temp_ddl.rs` call `spark_ddl_type_name`. On a temporary view over that table, `DESCRIBE TABLE vw` shows `timestamp_ntz` for all three columns (orchestrator reproduction, 2026-10-26).

Each of these predates NS-TRUTHUP-1 and is not a regression.

## What the card asks

1. One renderer seam: the nanosecond-name rule of `describe_ddl_type_name` recurses into struct fields, array elements and map keys and values, so a nested nanosecond leaf shows its spec name.
2. The same seam serves `DESCRIBE <table> <column>`, the DESCRIBE partition section, and `DESCRIBE TABLE` on a view (temporary and persisted), so the surfaces cannot drift apart.
3. Controls: microsecond names stay on v2 tables and on `timestamp` and `timestamp_ntz` columns, top-level and nested, on every surface.

## Done when

- Each surface above shows the spec names for nanosecond leaves, pinned in Rust and on the facade where the surface has a facade door.
- The control cells pass unchanged.
- The registry row R-001 residue line and the order's section 8 residue bullet are updated, in the same commit, to name what remains or to say it is empty.

## Out of scope

`SHOW CREATE TABLE`, `df.schema`, `dtypes` and `printSchema`. They carry the Arrow type and stay unchanged by design; whether they should show the spec names is an owner decision, and this card does not change them.

## Retires

When the R-001 residue line is empty.
