# map — task/ledgers/staging/u12-probes/

## Purpose

Step-0 oracle evidence for S3-PATH-WRITE-1 (U12): the probes that recorded
the `W-PATH-S3-*` cells and the JSON files they produced, plus the verifier
fold's `VU-*` cells. The ledger that reads this evidence is
[s3-path-write-1-ledger.md](../s3-path-write-1-ledger.md). No product code
lives here; the product rounds pin against these files.

## Contents

- [u12_probe.py](u12_probe.py) — the probe. Usage
  `u12_probe.py <spark|repark> <out.json>` with `AWS_ENDPOINT_URL` pointing at
  a local moto server and `U12_MOTOPY` naming a Python with `boto3`. One JSON
  record per cell: write outcome, S3 listing before and after (UUIDs
  normalised, sizes as `0` or `>0`), and same-engine read-back. The repark
  leg configures the moto endpoint through the `repark.hadoop.fs.s3a.*` keys.
  Ruff-clean; no comments by the project ban.
- [u12_probe2.py](u12_probe2.py) — the verifier-fold probe (2026-09-28).
  Usage `u12_probe2.py <spark|repark> <out.json>` with the same environment.
  Ten cells: extension-named destinations with overwrite+append (parquet, csv,
  json), an exact-key object under error/ignore/overwrite, read-transform
  self-overwrite, `#` and `?` keys beside a `data/` dataset, and a text
  write. The repark leg configures the moto endpoint as above. Ruff-clean;
  no comments by the project ban.
- [u12-spark.json](u12-spark.json) — the Spark leg: 38 cells recorded on Spark
  4.1.2 (`hadoop-aws` 3.4.2, UTC) against `moto[server]` 5.2.3 on
  `http://127.0.0.1:5599`, bucket `u12spark`.
- [u12-spark-2.json](u12-spark-2.json) — the verifier-fold Spark leg
  (2026-09-28): 10 `VU-*` cells on Spark 4.1.2 (`hadoop-aws` 3.4.2, UTC)
  against `moto[server]`, bucket `u12spark2`.
- [u12-repark.json](u12-repark.json) — the repark leg: the same 38 cells
  against repark 1.5.0 (UTC) with the moto endpoint configured, bucket
  `u12repark`. Re-recorded 2026-09-28 after the round-2 build: 33 writes ok,
  5 `PATH_ALREADY_EXISTS` refusals on error-if-exists over a seeded prefix.
