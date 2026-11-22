# OUTPUT-SORT-ORDER-ID-VALIDATE-1 — an unknown output sort-order id is not refused on RePark

**Rating: proposed S3 (the replay, 2026-11-21); the owner has not rated it.**

**Date:** 2026-11-21. **Filed by:** the records clerk (`claude-haiku-5-5`).
**Source:** an outside gap-map audit of 2026-11-21 (finding id `NEW-spark-1`), replayed on main `dac2af61` by an Opus 5.5 agent on 2026-11-21.

**Status:** filed 2026-11-21; not dispatched.

**Severity: S3 (proposed).** Missing validation; rows and metadata are correct.

## Finding

Setup for the `[M]` cells: Spark 4.1.2 with the Iceberg 1.11.0 jar, Java 17, session zone UTC, ANSI on; RePark built from main `dac2af61`.

- **[M] Spark.** `output-sort-order-id=99` is refused: "Cannot use output sort order id 99…". The table stays empty.
- **[M] RePark.** The write commits row 1. Its data file records sort_order_id 1 (read from Spark's `.files`), so no bad id was written.
- **[M] Controls.** A valid id 1, and no option, both commit on both engines.

## Why S3

Missing validation. In this replay the rows and the recorded metadata are correct.

## Scope

The `output-sort-order-id` option on write. RePark commits the write where Spark refuses the id 99.

**Label path:** owner to decide (depends on the workload's reach).

Up: [map.md](map.md).
