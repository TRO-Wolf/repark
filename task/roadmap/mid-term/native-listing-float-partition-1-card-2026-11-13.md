# Card NATIVE-LISTING-FLOAT-PARTITION-1: a native listing table loses the `0.0`/`-0.0` partition for `WHERE p = q` on DOUBLE partition columns

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** SIGNED-ZERO-NORMALIZE-1 part B
part 1 verifier 2, 2026-11-13, finding F4.

**Status:** filed 2026-11-13, not dispatched. Severity **S3**: a silent wrong read on a native-door source that no
other RePark path builds today.

## Measured

- **Shape:** native `CREATE EXTERNAL TABLE … PARTITIONED BY (p, q)`, with `p` and `q` typed DOUBLE.
- `WHERE p = q` and `WHERE p = q + 0.0` lose the partition where one side is `0.0` and the other `-0.0`. Spark's
  semantics keep it.
- **Builds:** part B's head `b84adbc7` and its base, main `276dc503`, identical.
- **Spark door:** not affected, because its `read.parquet` does not discover partitions (pre-existing).

## Mechanism (from the source)

- `ListingTable::supports_filters_pushdown` answers `Exact` for a filter that uses only partition columns
  (datafusion-catalog-listing 54.1.0 `src/table.rs:601-623`).
- So in the first optimizer pass `push_down_filter` moves the bare comparison into `full_filters`, and part B's key
  seat never sees it.
- That provider would also answer `Exact` for a wrapped filter (`helpers.rs:94-96` accepts any `Immutable` scalar
  function). The verifier found no plan that delivers one to that scan today.
- **Pin gap:** `no_provider_in_use_takes_a_wrapped_filter_as_exact` (part B's `key_tests.rs`) covers only an
  unpartitioned listing table.

## Open point

Part B verifier 1 recorded a native local `CREATE EXTERNAL TABLE` as refused, while verifier 2 built this table. Which
locations the native guard admits for this shape is to be measured first.

## Fix direction

None given by the source.

## Close

The reach is measured; then the partition-only comparison keeps Spark's rows on every reachable listing source, or a
recorded ruling states the limit, with a partitioned listing cell pinned either way.
