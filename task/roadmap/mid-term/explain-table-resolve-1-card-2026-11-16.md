# Card EXPLAIN-TABLE-RESOLVE-1: `EXPLAIN SELECT ... FROM <table>` on the Spark door cannot find a table that the same `SELECT` reads

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from items 1 and 7 of section 6 of the step 0 sketch of CAST-LITERAL-UNWRAP-1, [sketch.md](../../wo/cast-literal-unwrap-1/sketch.md). The owner ruled on 2026-11-16, about 16:25, verbatim: "adopt Fable's, run the string-cast sizing", which adopted the four side cards. **Evidence:** [explain-side-repark.txt](../../wo/cast-literal-unwrap-1/evidence/raw/explain-side-repark.txt) and [explain-side-spark.txt](../../wo/cast-literal-unwrap-1/evidence/raw/explain-side-spark.txt).

**Status:** filed 2026-11-16, not dispatched. **Severity: S3.**

## Measured

Step 0, on the Spark door, with Spark 4.1.2 as the oracle. After `CREATE TABLE p USING parquet AS ...`:

- `SELECT ... FROM p` answers.
- `EXPLAIN SELECT c FROM p` raises `table 'datafusion.public.p' not found`.
- `EXPLAIN SELECT c FROM default.p` raises `'datafusion.default.p' not found`.
- Only `spark_catalog.default.p` explains.
- `EXPLAIN EXTENDED` does not parse: `Expected: an SQL statement, found: EXTENDED`.
- Spark explains all five forms.

Seen in passing and not chased: the table is scanned by an `IcebergTableScan` on RePark and by a Parquet `FileScan` on Spark.

## Second instance

Item 7 of section 6: the critic's s26, on its `pq` table. It is the same `datafusion.public` resolution as the first instance. An `EXPLAIN` over a `read.parquet` temp view explains.

## Relation to other cards

The same resolution family is [BARE-READ-DEFAULT-NAMESPACE-1](bare-read-default-namespace-1-card-2026-11-02.md): a bare read the facade cannot qualify resolves against `datafusion.public`. The step 0 sketch considered folding the second instance into that card. This card keeps both instances and is filed separately at S3, per the sketch's default.

Up: [map.md](map.md).
