# S3-PATH-WRITE-1 — plain Parquet, CSV and JSON path writes to `s3://`, in the v1.5.0 target

**Filed:** 2026-09-22 by the orchestrating session on the owner's ruling of the same day ("add it as a
v1.5.1 card"), after the owner asked whether RePark can write standard Parquet objects to S3 today. It
cannot; this card says exactly why, what closes it, and what it must not disturb. Nothing here is measured
against Spark yet; step 0 is the measurement.

**Owner, 2026-09-23:** "add the S3 writer ability to the 1.5 target." This card is now unit **U12** of the
v1.5.0 remainder ([v1-5-0-remainder-spec-2026-09-23.md](v1-5-0-remainder-spec-2026-09-23.md)); the
`W-PATH-S3-*` cells recorded in step 0 join the gate inventory, and the gate waits on them.

## What is true on main today (read from the tree, 2026-09-22)

- **Reads reach S3.** `Session::read_parquet`, `read_csv` and `read_json` call
  `ensure_s3_bucket_registered` before the scan (`crates/repark-core/src/session.rs:774-806`, the helper at
  `:942`), which builds one `object_store` S3 store from the aws-config chain plus the
  `repark.hadoop.fs.s3a.endpoint.region` override and registers it under both `s3://bucket` and `s3a://bucket`
  (`crates/repark-core/src/object_store_s3.rs`). The tier-2 acceptance run proves a bronze `s3a://` Parquet read.
- **Path writes do not.** `DataFrameWriter.parquet` / `csv` / `json` all end in `_apply_path_write`
  (`python/repark/src/repark/spark/dataframe/writer_readwriter.py:401`), which is a local-filesystem
  protocol: `Path(path)` and `destination.exists()` decide the save mode (`:413`), the data goes through
  `COPY (SELECT …) TO '<sibling repark-staging-<uuid>>'`, and the commit is `staging.rename(destination)`
  (`:472`) with `shutil.rmtree` for `overwrite`. An `s3://` destination fails on the first filesystem call,
  before the `COPY` runs, and nothing on the write side registers the bucket store.
- **Iceberg tables on S3 already write.** `saveAsTable`, `INSERT`, `MERGE` and the maintenance procedures
  against a Glue or S3 Tables catalog with an `s3://` warehouse are the live-AWS legs in
  [../../../docs/tier2-aws.md](../../../docs/tier2-aws.md) §6. So the gap is loose objects, not S3.

## Step 0 — the oracle (before any design is ruled)

Record on Spark 4.1.2 with the `s3a` connector, against the scratch prefix the tier-2 role already grants:
`df.write.mode(m).parquet("s3a://…/p")` for each of `error`, `ignore`, `overwrite`, `append`; the same for
`csv` and `json`; `partitionBy` on one and two columns; an empty DataFrame; a destination that already holds
a `_SUCCESS` marker and one that holds foreign objects; `s3://` versus `s3a://` spelling; and what objects
Spark leaves behind (`_SUCCESS`, `part-*` names, `_temporary/` cleaned or not). The cells go into the parity
inventory as `W-PATH-S3-*` and every design question below is answered from them.

## Design questions to rule, in order

1. **Where the protocol lives.** Rust first ([../../../AGENTS.md](../../../AGENTS.md), the 2026-09-14
   ruling): a `Session::write_path(df, url, format, mode, options)` seam in `repark-core` that owns the save
   mode, the staging and the commit for every scheme; the Python writer forwards and stops calling
   `Path`, `exists`, `rename` and `rmtree` itself. The local scheme keeps today's staging-and-rename
   behaviour bit for bit (`ex-0-example-drift-gate` and the `test_write_*` pins stay green).
2. **The S3 commit.** Object stores have no rename. The candidates the oracle decides between: write
   `part-*` objects straight under the destination prefix and finish with `_SUCCESS` (what Spark's
   `FileOutputCommitter` v2 does on S3), or stage under a sibling prefix and copy-then-delete (v1's
   shape, twice the PUTs). Failed-write cleanup: what a partial write leaves and whether RePark deletes it.
3. **Save modes without a filesystem.** `error` / `ignore` need a prefix existence probe (one `LIST` with
   `max_keys=1`); `overwrite` needs list-and-delete of the prefix before the write; `append` is a plain
   write with fresh object names. The `_SUCCESS`-only and foreign-object cells rule what "exists" means.
4. **Credentials and region.** Reuse `ensure_s3_bucket_registered`; the write side must resolve the same
   chain and the same region override, and `note_local_write_root` must not be called for a URL (it feeds
   the local-warehouse guard).
5. **IAM surface.** The tier-2 role grants `PutObject` / `DeleteObject` only under the warehouse scratch
   prefix (OD-3) and bronze is read-only. The acceptance leg for this card writes under that scratch
   prefix; a bronze-style target prefix is a separate owner grant, not something this card assumes.
6. **Not in scope.** Iceberg writes (already served), ORC path writes (the ORC default ruling covers
   Iceberg ORC, not loose files), Hadoop-style `_temporary/` emulation, multipart tuning beyond
   `object_store` defaults, and any change to `read_*`.

## Done condition

Every `W-PATH-S3-*` cell EQUAL on a recorded oracle with one live leg in the tier-2 workflow; the local
path-write pins unchanged; the Python writer free of filesystem calls; one ledger; a verification critic
before the product-Rust merge. Size: one day lane, one PR, no fork work, no Cargo.toml change (the
`object_store` S3 feature is already on for reads).

## Rules that bind it

The comment ban on every actor, measure before ruling, recorded-oracle pins with a live leg, the 3 GB
table-size flag and the $25 AWS spend cap on the live leg, and the map lockstep for every directory touched.
