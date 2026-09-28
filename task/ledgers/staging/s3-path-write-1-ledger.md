# Unit ledger — S3-PATH-WRITE-1 step 0 · the s3a path-write oracle and the design answers

**Date:** 2026-09-28 · **Branch:** `feat/s3-path-write-1` · **Base:** `b1ee89fc`
(`origin/main`) **Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** [S3-PATH-WRITE-1](../../roadmap/mid-term/s3-path-write-1-5-0.md) (U12,
v1.5.1) orders measurement before design: record what Spark 4.1.2 does when a
`df.write` lands on S3, then rule design questions 1–5 from the cells. This round
is that step 0 plus the answers. No product code changes here; the evidence is
[u12-probes/](u12-probes/map.md) and the orchestrator rules before any product round.

**Safety.** No real AWS was touched. Every S3 byte went to a local
`moto[server]` 5.2.3 emulator on `http://127.0.0.1:5599` under the brief's
`testing` credentials; the server was stopped after the runs. The repark leg ran
with no `AWS_*` variables in its environment, so its S3 failures are local
refusals, never outbound calls.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The oracle ran on Spark 4.1.2 with `hadoop-aws` 3.4.2, session zone UTC, against moto 5.2.3, and recorded 38 `W-PATH-S3-*` cells with a quoted banner. | Banner from the live session plus the emulator version. | PROVEN | [u12-spark.json](u12-probes/u12-spark.json) `banner`; Oracle setup below. |
| C-002 | Save modes behave identically across parquet, csv and json: `error` writes on an empty prefix and refuses `PATH_ALREADY_EXISTS` on a seeded one, `ignore` writes on empty and no-ops on seeded, `overwrite` replaces, `append` adds a fresh part file. | The 24 mode cells with listings and read-backs. | PROVEN | `W-PATH-S3-{parquet,csv,json}-{error,ignore,overwrite,append}-{empty,existing}` in [u12-spark.json](u12-probes/u12-spark.json). |
| C-003 | "Exists" means any object under the prefix: a `_SUCCESS`-only and a `foreign.txt`-only prefix both refuse `error` and no-op `ignore`; `overwrite` deletes every object under the prefix including `foreign.txt`; `append` preserves every object. | The 8 `_SUCCESS`-only and foreign-object cells. | PROVEN | `W-PATH-S3-parquet-{success-only,foreign}-{error,ignore,overwrite,append}` in [u12-spark.json](u12-probes/u12-spark.json). |
| C-004 | Layout: flat `part-00000-<UUID>-c000.<ext>` files, `key=value/` hive dirs under `partitionBy`, a trailing 0-byte `_SUCCESS` on every successful write, no `_temporary/` residue in any cell, and a part file even for empty frames. | Listings of all 38 cells. | PROVEN | `list_after` of every cell in [u12-spark.json](u12-probes/u12-spark.json). |
| C-005 | The `s3://` and `s3a://` spellings write identical objects and read back identical rows. | The two scheme cells. | PROVEN | `W-PATH-S3-parquet-scheme-{s3,s3a}` in [u12-spark.json](u12-probes/u12-spark.json). |
| C-006 | RePark 1.5.0 writes no S3 object: every path write fails with `No suitable object store`, every S3 read-back refuses locally for lack of SDK config, and the read side honours no endpoint override. | The 38 repark cells plus the store builder source. | PROVEN | [u12-repark.json](u12-probes/u12-repark.json); [object_store_s3.rs](../../../crates/repark-core/src/object_store_s3.rs). |
| C-007 | Design question 1 rules that `Session::write_path` in `repark-core` owns the protocol for every scheme and the Python writer forwards to it. | Orchestrator ruling on the recommendation in Design answers. | OPEN | Recommendation under question 1; the oracle cells it must satisfy. |
| C-008 | Design question 2 rules the S3 commit shape (direct part files plus trailing `_SUCCESS`) and failed-write cleanup. | Orchestrator ruling on the recommendation in Design answers. | OPEN | Recommendation under question 2; C-004. |
| C-009 | Design question 3 rules the save-mode probes (existence LIST, overwrite list-and-delete, append plain write). | Orchestrator ruling on the recommendation in Design answers. | OPEN | Recommendation under question 3; C-002, C-003. |
| C-010 | Design question 4 rules credential and region resolution (reuse the read chain unchanged, no endpoint option in the product round). | Orchestrator ruling on the recommendation in Design answers. | OPEN | Recommendation under question 4; C-006. |
| C-011 | Design question 5 rules the live leg under the tier-2 scratch prefix on the existing grants. | Orchestrator ruling on the recommendation in Design answers. | OPEN | Recommendation under question 5; Live-leg plan. |

## Oracle setup

Live banner from the probe session: `version 4.1.2, spark.sql.session.timeZone UTC`.
Spark ran as `local[1]` with 2 g driver memory and the UI off, through the box JVM
lock. The S3A connector is `org.apache.hadoop:hadoop-aws:3.4.2` with
`software.amazon.awssdk:bundle:2.23.19`, resolved through `spark.jars.packages`;
the first attempt with the cached `hadoop-aws` 3.4.0 wrote fine but parquet
read-back died in `S3AInputStream.readVectored` with
`NoSuchMethodError: VectoredReadUtils.validateNonOverlappingAndReturnSortedRanges`,
a client-version skew, so attempt 2 (3.4.2) is the recorded configuration. Java is
Zulu 17; the default Java 11 cannot launch the Spark 4.1.2 gateway. S3A options:
endpoint `http://127.0.0.1:5599`, path-style access on, SSL off,
`SimpleAWSCredentialsProvider` with the `testing` keys, and `fs.s3.impl` mapped to
the S3A filesystem so the `s3://` spelling works. Buckets `u12spark` (Spark) and
`u12repark` (repark) on the emulator. The written frame is `(id, grp)` with rows
`(1,a)`, `(2,b)`, `(3,a)`; seeds are `(0,seed)`; the two-column partition cell uses
`(id, grp, val)` rows `(1,a,x)`, `(2,b,y)`, `(3,a,z)`. Every cell uses a fresh
prefix named for its key; UUIDs and job ids in listings are normalised, sizes are
`0` or `>0`.

## Measured Spark behaviour per cell

One row per cell. `1x part` is one `part-00000-<UUID>-c000.<ext>` object; `new`
is the 3 written rows, `seed` the 1 seed row. Types: parquet reads back
`id bigint, grp string` (Spark infers `LongType` for Python ints); csv reads back
`_c0 string, _c1 string` with no header; json reads back `grp string, id bigint`.

| Cell | Write | Objects after write | Read-back |
|---|---|---|---|
| W-PATH-S3-parquet-error-empty | ok | `_SUCCESS` (0), 1x part parquet (>0) | ok, 3 rows (new) |
| W-PATH-S3-parquet-error-existing | `PATH_ALREADY_EXISTS` (AnalysisException, 42K04), prefix untouched | `_SUCCESS` (0), 1x part parquet (>0, seed) | ok, 1 row (seed) |
| W-PATH-S3-parquet-ignore-empty | ok | `_SUCCESS` (0), 1x part parquet (>0) | ok, 3 rows (new) |
| W-PATH-S3-parquet-ignore-existing | ok, no-op | `_SUCCESS` (0), 1x part parquet (>0, seed) | ok, 1 row (seed) |
| W-PATH-S3-parquet-overwrite-empty | ok | `_SUCCESS` (0), 1x part parquet (>0) | ok, 3 rows (new) |
| W-PATH-S3-parquet-overwrite-existing | ok, seed part deleted | `<DIR-MARKER>` (0), `_SUCCESS` (0), 1x part parquet (>0) | ok, 3 rows (new) |
| W-PATH-S3-parquet-append-empty | ok | `_SUCCESS` (0), 1x part parquet (>0) | ok, 3 rows (new) |
| W-PATH-S3-parquet-append-existing | ok | `_SUCCESS` (0), 2x part parquet (>0, fresh UUID) | ok, 4 rows (seed+new) |
| W-PATH-S3-csv-error-empty | ok | `_SUCCESS` (0), 1x part csv (>0) | ok, 3 rows (new) |
| W-PATH-S3-csv-error-existing | `PATH_ALREADY_EXISTS`, prefix untouched | `_SUCCESS` (0), 1x part csv (>0, seed) | ok, 1 row (seed) |
| W-PATH-S3-csv-ignore-empty | ok | `_SUCCESS` (0), 1x part csv (>0) | ok, 3 rows (new) |
| W-PATH-S3-csv-ignore-existing | ok, no-op | `_SUCCESS` (0), 1x part csv (>0, seed) | ok, 1 row (seed) |
| W-PATH-S3-csv-overwrite-empty | ok | `_SUCCESS` (0), 1x part csv (>0) | ok, 3 rows (new) |
| W-PATH-S3-csv-overwrite-existing | ok, seed part deleted | `<DIR-MARKER>` (0), `_SUCCESS` (0), 1x part csv (>0) | ok, 3 rows (new) |
| W-PATH-S3-csv-append-empty | ok | `_SUCCESS` (0), 1x part csv (>0) | ok, 3 rows (new) |
| W-PATH-S3-csv-append-existing | ok | `_SUCCESS` (0), 2x part csv (>0, fresh UUID) | ok, 4 rows (seed+new) |
| W-PATH-S3-json-error-empty | ok | `_SUCCESS` (0), 1x part json (>0) | ok, 3 rows (new) |
| W-PATH-S3-json-error-existing | `PATH_ALREADY_EXISTS`, prefix untouched | `_SUCCESS` (0), 1x part json (>0, seed) | ok, 1 row (seed) |
| W-PATH-S3-json-ignore-empty | ok | `_SUCCESS` (0), 1x part json (>0) | ok, 3 rows (new) |
| W-PATH-S3-json-ignore-existing | ok, no-op | `_SUCCESS` (0), 1x part json (>0, seed) | ok, 1 row (seed) |
| W-PATH-S3-json-overwrite-empty | ok | `_SUCCESS` (0), 1x part json (>0) | ok, 3 rows (new) |
| W-PATH-S3-json-overwrite-existing | ok, seed part deleted | `<DIR-MARKER>` (0), `_SUCCESS` (0), 1x part json (>0) | ok, 3 rows (new) |
| W-PATH-S3-json-append-empty | ok | `_SUCCESS` (0), 1x part json (>0) | ok, 3 rows (new) |
| W-PATH-S3-json-append-existing | ok | `_SUCCESS` (0), 2x part json (>0, fresh UUID) | ok, 4 rows (seed+new) |
| W-PATH-S3-parquet-partitionby-1col | ok | `_SUCCESS` (0), `grp=a/part-...` (>0), `grp=b/part-...` (>0) | ok, 3 rows (new) |
| W-PATH-S3-parquet-partitionby-2col | ok | `_SUCCESS` (0), 3x `grp=X/id=N/part-...` (>0) | ok, 3 rows (new), cols `val, grp, id` |
| W-PATH-S3-parquet-empty-df | ok | `_SUCCESS` (0), 1x part parquet (>0, footer only) | ok, 0 rows |
| W-PATH-S3-csv-empty-df | ok | `_SUCCESS` (0), 1x part csv (0 bytes) | ok, 0 rows, no columns |
| W-PATH-S3-parquet-success-only-error | `PATH_ALREADY_EXISTS`, prefix untouched | `_SUCCESS` (0) only | `UNABLE_TO_INFER_SCHEMA` (42KD9, no part files) |
| W-PATH-S3-parquet-success-only-ignore | ok, no-op | `_SUCCESS` (0) only | `UNABLE_TO_INFER_SCHEMA` (42KD9, no part files) |
| W-PATH-S3-parquet-success-only-overwrite | ok | `<DIR-MARKER>` (0), `_SUCCESS` (0), 1x part parquet (>0) | ok, 3 rows (new) |
| W-PATH-S3-parquet-success-only-append | ok | `_SUCCESS` (0), 1x part parquet (>0) | ok, 3 rows (new) |
| W-PATH-S3-parquet-foreign-error | `PATH_ALREADY_EXISTS`, prefix untouched | `foreign.txt` (>0) only | SparkException (foreign.txt unreadable as parquet) |
| W-PATH-S3-parquet-foreign-ignore | ok, no-op | `foreign.txt` (>0) only | SparkException (foreign.txt unreadable as parquet) |
| W-PATH-S3-parquet-foreign-overwrite | ok, `foreign.txt` deleted | `<DIR-MARKER>` (0), `_SUCCESS` (0), 1x part parquet (>0) | ok, 3 rows (new) |
| W-PATH-S3-parquet-foreign-append | ok, `foreign.txt` kept | `foreign.txt` (>0), `_SUCCESS` (0), 1x part parquet (>0) | SparkException (foreign.txt unreadable as parquet) |
| W-PATH-S3-parquet-scheme-s3 | ok | `_SUCCESS` (0), 1x part parquet (>0) | ok, 3 rows (new) |
| W-PATH-S3-parquet-scheme-s3a | ok | `_SUCCESS` (0), 1x part parquet (>0) | ok, 3 rows (new) |

Layout notes, all read from the listings. Flat part names are
`part-00000-<UUID>-c000.snappy.parquet`, `part-00000-<UUID>-c000.csv` and
`part-00000-<UUID>-c000.json`; under `partitionBy` the shape is
`part-00000-<UUID>.c000.snappy.parquet` (dots, not dashes) inside `key=value/`
dirs. `_SUCCESS` is 0 bytes after every successful write. No `_temporary/`
object survives in any of the 38 cells. The `<DIR-MARKER>` row is the S3A
parent-directory marker, visible only after the five overwrites of a
pre-existing prefix; it is client-side S3A emulation, not data, and RePark
need not reproduce it. The `error` refusal, verbatim:

`[PATH_ALREADY_EXISTS] Path s3a://u12spark/W-PATH-S3-parquet-error-existing/p already exists. Set mode as "overwrite" to overwrite the existing path. SQLSTATE: 42K04`

The `_SUCCESS`-only read-back refusal, verbatim:

`[UNABLE_TO_INFER_SCHEMA] Unable to infer schema for Parquet. It must be specified manually. SQLSTATE: 42KD9`

## RePark on main today

The repark leg ([u12-repark.json](u12-probes/u12-repark.json), banner
`repark-1.5.0`, zone UTC) ran the same 38 cells with no `AWS_*` variables in
its environment. Every write failed with the same local error (37x `s3a`, 1x
`s3` for the scheme cell):

`datafusion engine error: Internal error: No suitable object store found for s3a:%2F. See RuntimeEnv::register_object_store.`

The card's "fails on the first filesystem call" is refined: `Path.exists()`
returns false and the `COPY` runs, but DataFusion reads the `s3a://…` staging
path as a URL with no registered store and refuses. Every read-back failed
with the never-resolved-SDK-config refusal. No cell created any S3 object and
no local residue survived (the contained working directory was empty after the
run). The 12 `existing`-prefix seeds failed the same way, so their prefixes
stayed empty.

Endpoint finding: the read side honours no custom endpoint.
[object_store_s3.rs](../../../crates/repark-core/src/object_store_s3.rs)
`build_amazon_s3_store` sets bucket, region and credentials on
`AmazonS3Builder` and never calls `with_endpoint`; no file under `crates/`
reads `AWS_ENDPOINT_URL` or any `repark.hadoop.fs.s3a.endpoint` option. Only
the region override (`repark.hadoop.fs.s3a.endpoint.region` /
`spark.hadoop.fs.s3a.endpoint.region`, consumed by
[ensure_s3_bucket_registered](../../../crates/repark-core/src/session.rs))
exists. A moto-backed product test would need a new session key; see question 4.

## Design answers

**1. Where the protocol lives.** Rust first: a `Session::write_path` seam in
`repark-core` owns the save mode, the staging and the commit for every scheme,
and the Python writer in
[writer_readwriter.py](../../../python/repark/src/repark/spark/dataframe/writer_readwriter.py)
forwards and stops calling `Path`, `exists`, `rename` and `rmtree`. The local
scheme keeps today's staging-and-rename behaviour bit for bit; the pins under
Rust seam shape stay green. Cells: all 38 define the S3 target the seam must hit.
RECOMMENDATION: adopt the seam as drawn; no Python filesystem calls survive on
the S3 path.

**2. The S3 commit.** The oracle shows the v2 shape: part files land directly
under the destination prefix and a 0-byte `_SUCCESS` finishes the write; no
staging prefix, no copy-then-delete, and no `_temporary/` residue in any cell
(C-004). Failed-write cleanup is unmeasured — every Spark write here succeeded —
so the product round probes it (abort mid-write, list the residue) before ruling
whether RePark deletes its own partial part files. RECOMMENDATION: direct-write
part files plus trailing `_SUCCESS`; best-effort deletion of own partial files
on failure, confirmed by a fault-injection probe in the product round.

**3. Save modes without a filesystem.** `error` and `ignore` need one prefix
existence probe (`LIST` with `max_keys=1`): any object counts, including a lone
`_SUCCESS` or a lone `foreign.txt` (C-003). `overwrite` lists and deletes the
whole prefix first, foreign objects included
(`W-PATH-S3-parquet-foreign-overwrite` deletes `foreign.txt`). `append` is a
plain write with fresh UUID part names and preserves everything
(`W-PATH-S3-parquet-foreign-append` keeps `foreign.txt`). The refusal text is
Spark's `PATH_ALREADY_EXISTS` with SQLSTATE 42K04, which RePark's local door
already spells the same way. RECOMMENDATION: implement exactly the measured
semantics; no `_SUCCESS`-content sniffing, no foreign-object filtering.

**4. Credentials and region.** The write side reuses
`ensure_s3_bucket_registered`: the same aws-config chain, the same region
override, and no `note_local_write_root` call for a URL. The endpoint finding
above (no override exists) stays as is: the product round adds no endpoint
option, and moto-backed tests are not part of it — the live leg is the
acceptance. If the orchestrator later wants emulator-backed tests, the
disciplined shape is a `repark.hadoop.fs.s3a.endpoint` session key (never an
env read at query time). RECOMMENDATION: reuse the chain unchanged; no new
config key in the product round.

**5. IAM surface.** The acceptance leg writes under the warehouse scratch
prefix, where the tier-2 role already grants `PutObject`, `GetObject`,
`ListBucket` and `DeleteObject` (OD-3); `overwrite`'s list-and-delete and
`append`'s plain writes need nothing more. A bronze-style target prefix stays a
separate owner grant. RECOMMENDATION: per-run uuid stem under the scratch
namespace, zero IAM change.

## Rust seam shape

Home: `crates/repark-core`, beside `read_parquet` /
`ensure_s3_bucket_registered` in
[session.rs](../../../crates/repark-core/src/session.rs), reached from the
facade through the PyO3 binding as a thin adapter. Sketch, mirroring the
`read_*` shapes (`&self`, plain `&str` path, options map):

```rust
pub async fn write_path(
    &self,
    frame: &DataFrame,
    url: &str,
    format: PathWriteFormat,
    mode: SaveMode,
    options: &HashMap<String, String>,
) -> Result<PathWriteOutcome>
```

`PathWriteFormat` (Parquet, Csv, Json) and `SaveMode` (Error, Ignore, Overwrite,
Append) are new small enums; no `SaveMode` type exists today. The seam parses
the scheme (`object_store_s3::parse_s3_bucket` for S3, local path otherwise),
applies the mode from C-002/C-003, executes the write (local keeps
staging-and-rename; S3 writes direct part files plus `_SUCCESS`), and returns
what the writer needs for its output listing. The local-path pins that guard it:

- [test_r2_read_formats2.py](../../../python/repark/tests/test_r2_read_formats2.py) — path modes error/ignore/append/overwrite, partitionBy layouts, csv/json/parquet write options
- [test_e2_readwriter.py](../../../python/repark/tests/test_e2_readwriter.py) — parquet/csv/json save/load round trips
- [test_r1_read_formats.py](../../../python/repark/tests/test_r1_read_formats.py) — read side plus the `PATH_ALREADY_EXISTS` pin
- [test_examples_io_session.py](../../../python/repark/tests/test_examples_io_session.py) — writer output listing (`_SUCCESS`, part files)
- [test_io_text_1.py](../../../python/repark/tests/test_io_text_1.py) — text-door path modes (adjacent door, stays local-only)
- [test_ex_0_example_coverage.py](../../../python/repark-parity/tests/test_ex_0_example_coverage.py) — the example-drift gate over [docs/examples/io](../../../docs/examples/io/map.md) (`parquet_roundtrip`, `writer_csv`, `writer_json`, `writer_partition`, `text_read_write`)

## Round estimate

Two rounds after the design ruling. Round 1: the Rust seam, the Python
forwarding, the local pins green, the fault-injection probe for failed-write
cleanup, and the verification critic the card orders before the product-Rust
merge. Round 2: the tier-2 live leg confirmation (owner-run) plus residue
fix-ups. No fork work, no `Cargo.toml` change (`object_store` S3 is already on
for reads).

## Live-leg plan

The live leg is a new test in
[test_aws_acceptance.py](../../../python/repark/tests/test_aws_acceptance.py)
(the module section 6 of [tier2-aws.md](../../../docs/tier2-aws.md) rows), run
by the owner-tier `aws-acceptance.yml` workflow on the `REPARK_AWS_ACCEPTANCE=1`
gate — no workflow change in this round. It writes parquet, csv and json under
a per-run `testing_<uuid>` stem in the scratch namespace off
`REPARK_ACCEPT_WAREHOUSE`, covers the four save modes plus `partitionBy` and an
empty frame, asserts the object layout from C-004 and the row read-backs, and
needs no new grant: the scratch-prefix `PutObject`/`DeleteObject` pair (OD-3)
already covers it. Bronze stays untouched.
