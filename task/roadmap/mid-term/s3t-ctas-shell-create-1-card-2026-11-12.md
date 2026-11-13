# Card S3T-CTAS-SHELL-CREATE-1 — an S3 Tables CTAS creates its entry with no first version

**Date:** 2026-11-12 · **Filed by:** Claude Opus 5.5 (`claude-opus-5-5`), the orchestrator ·
**Source:** owner ruling of the same day, verbatim "Go with the default C’". Today's behavior
stays inside ICE-V2-FLAT-AWS-LABEL-1 as a stated caveat
([parity doc S3T-CTAS-KILL-1](../../../docs/spark-sql-iceberg-parity.md)), and this card is
the fix after the label. No product code in this filing. **Status:** filed, not dispatched.

## Why

On S3 Tables, CTAS goes create-first (`execute_ctas_service_managed` in
`crates/repark-spark/src/ctas.rs`). The fork's `create_table` (`crates/catalog/s3tables/src/catalog.rs`)
calls the S3 Tables `CreateTable` API, writes an empty first metadata file, and sets the
table's metadata location, so the table is readable as empty from its creation. A definite
failure drops it. A killed process cannot, and the table stays listed with zero rows.

The AWS client jar (`software.amazon.s3tables.iceberg.S3TablesCatalog`) calls `CreateTable`
too, but sets no metadata location until the commit. Its leftover entry is listed and fails on
read. `CREATE TABLE IF NOT EXISTS` treats it as absent, and the next create adopts it. The
adoption is read from the jar's source and not measured.

| Cell (live, `ctas-failure` leg) | Run | Result |
|---|---|---|
| K, S3 Tables, plain CTAS killed mid-write | 43126006620, 43220069749 | killed (`-9`); listed; Spark reads 0 rows, no error |
| K, Glue | 43123707194 | killed (`-9`); not listed (the staged create commits at the end) |
| S1, S3 Tables, an entry created by the API and never committed | 43220069749 | listed; no metadata location; Spark over REST: `The specified metadata location is not valid`; repark: not found |
| R1, S3 Tables, silver `CREATE TABLE IF NOT EXISTS … AS SELECT` killed, then the retry with `MERGE` | 43220069749 | killed (`-9`); 0 rows between; the CTAS skips (0.1 s); `MERGE` writes 100,000 rows, equal to the expected set |

The gap this card closes is the bare `CREATE TABLE IF NOT EXISTS … AS SELECT` retry with no
follow-up write: after a kill it skips and the table stays empty with no error.

## Decisions (proposed)

| id | decision |
|---|---|
| D1 | The service-managed create calls `CreateTable` and holds the version token. The first metadata file, carrying the snapshot, is written and its location set once, at publish. The fork needs a create that stops before the first metadata location (a `stage_create` on the s3tables catalog). |
| D2 | An entry with no metadata location reads as absent: `table_exists` answers false (it answers true today), so `IF NOT EXISTS` proceeds, and the create adopts the entry, as the jar does. |
| D3 | A definite failure still drops the entry, and an ambiguous publish still raises `CommitStateUnknownException` with no drop (ICE-COMMIT-UNKNOWN-1). |
| D4 | The native door's `create_first_service_managed` takes the same shape. |

## Done when

- Live K on S3 Tables: the killed CTAS leaves an entry that every reader refuses, or nothing.
- A bare `IF NOT EXISTS` retry after the kill writes every row.
- R1, S1 and C1–C6 are unchanged.
- The parity doc's S3T-CTAS-KILL-1 row and its G5 waiver retire.
