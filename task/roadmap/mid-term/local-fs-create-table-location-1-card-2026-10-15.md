# Card LOCAL-FS-CREATE-TABLE-LOCATION-1: `CREATE TABLE … LOCATION` outside writes outside

**Date:** 2026-10-15. **Filed by:** Muse Spark, clerk, under owner ruling 105 (a).

**Status:** open. The same class as PA-02: the next U-5-kind unit on that lane after the PA-LOCAL-FS-POLICY-1 re-verifier; the 1.5.6 local-file policy line does not read clean until it lands.

**Retires:** when the location is held to the same policy as `COPY`, with door pins.

**Registry row:** [LOCAL-FS-CREATE-TABLE-LOCATION-1](../../../docs/cutover/production-audit-2026-10-11.md) (dated row 2026-10-15, owner ruling 105 (a)).

## Risk

A Spark-door `CREATE TABLE … LOCATION '<outside>'` writes data and metadata outside every registered warehouse with the opt-in off.

## Sequence

On the Spark SQL door, `CREATE TABLE sp1 (a INT) USING parquet LOCATION '<outside>'` followed by `INSERT`, and `CREATE TABLE … USING parquet LOCATION '<outside>' AS SELECT`, write Iceberg data and metadata outside every registered warehouse with `repark.sql.allowLocalFilesystemDDL` off, on main and on the PR #1075 head. The statement is not a `Copy` or `CreateExternalTable` plan node, so the local-file guard never sees it.

## Note

Measured by the U-5 verifier on main and PR #1075's head.
