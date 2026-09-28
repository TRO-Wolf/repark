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
| C-007 | Design question 1 rules that `Session::write_path` in `repark-core` owns the protocol for every scheme and the Python writer forwards to it. | Orchestrator ruling on the recommendation in Design answers. | PROVEN (RULED 2026-09-28) | Q1 ADOPTED in Orchestrator rulings; local stays in Python this round. |
| C-008 | Design question 2 rules the S3 commit shape (direct part files plus trailing `_SUCCESS`) and failed-write cleanup. | Orchestrator ruling on the recommendation in Design answers. | PROVEN (RULED 2026-09-28) | Q2 ADOPTED in Orchestrator rulings; Fault record. |
| C-009 | Design question 3 rules the save-mode probes (existence LIST, overwrite list-and-delete, append plain write). | Orchestrator ruling on the recommendation in Design answers. | PROVEN (RULED 2026-09-28) | Q3 ADOPTED in Orchestrator rulings; C-002, C-003. |
| C-010 | Design question 4 rules credential and region resolution (reuse the read chain unchanged, no endpoint option in the product round). | Orchestrator ruling on the recommendation in Design answers. | PROVEN (RULED 2026-09-28) | Q4 OVERRIDDEN in Orchestrator rulings: the endpoint keys ship; C-012. |
| C-011 | Design question 5 rules the live leg under the tier-2 scratch prefix on the existing grants. | Orchestrator ruling on the recommendation in Design answers. | PROVEN (RULED 2026-09-28) | Q5 ADOPTED in Orchestrator rulings; C-016, C-017. |
| C-012 | The `s3a` endpoint keys (`fs.s3a.endpoint`, `fs.s3a.path.style.access`, `fs.s3a.connection.ssl.enabled`, both spellings) are honoured on the read and write side; absent keys leave behaviour byte-identical. | Unit tests for parsing plus moto reads and writes through a custom endpoint. | PROVEN | `endpoint_config_*` in [object_store_s3.rs](../../../crates/repark-core/src/object_store_s3.rs); Round 1 verification. |
| C-013 | Every one of the 38 `W-PATH-S3-*` cells replays on moto: write outcome, normalised listing and read-back rows are EQUAL, or a dated residue below records both answers. | The moto pins plus the residue entries. | PROVEN | [test_s3_path_write_1.py](../../../python/repark/tests/test_s3_path_write_1.py); Residues. |
| C-014 | The local path is bit-for-bit unchanged: the local pins stay green and no existing pin changes. | The writer suites in the round gate. | PROVEN | Round 1 verification; the suites listed there. |
| C-015 | Failed-write cleanup is best effort and the residue of each failure shape is recorded. | The fault-injection probe output below. | PROVEN | Fault record F1-F5. |
| C-016 | The live leg is written in the module the plan named, skipped unless the tier-2 marker is present, with no workflow change. | The skipped test in the acceptance module. | PROVEN | `test_u12_s3_path_write_against_scratch_prefix` in [test_aws_acceptance.py](../../../python/repark/tests/test_aws_acceptance.py). |
| C-017 | The live leg runs green on real AWS under the tier-2 scratch prefix. | The owner runs it; moto cannot prove real-AWS reachability. | OPEN | C-016; the owner run is pending. |

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

## Orchestrator rulings (2026-09-28)

- **Q1 ADOPTED.** `Session::write_path(df, url, format, mode, options)` in
  `repark-core` owns save mode, staging and commit; the Python writer forwards
  to it and makes no filesystem calls for a URL. The local scheme keeps
  today's staging-and-rename behaviour bit for bit. Taken up: the local path
  stays in Python this round and only `s3://` / `s3a://` route to Rust.
- **Q2 ADOPTED.** Part objects go directly under the destination and
  `_SUCCESS` is written last. Failed-write cleanup is best effort; the Fault
  record below probes it and records what is left.
- **Q3 ADOPTED.** "Exists" means any object under the prefix, `_SUCCESS`-only
  and foreign objects included. `overwrite` lists and deletes the whole
  prefix. `append` preserves.
- **Q4 OVERRIDDEN — the endpoint keys ship.** Honoured on both the read and
  the write side in `object_store_s3.rs`: `spark.hadoop.fs.s3a.endpoint` /
  `repark.hadoop.fs.s3a.endpoint`, `…fs.s3a.path.style.access`, and
  `…fs.s3a.connection.ssl.enabled`. Absent keys leave today's behaviour
  byte-identical. Nothing is read from the environment beyond what the
  aws-config chain already reads. See C-012.
- **Q5 ADOPTED.** The live leg runs under the tier-2 scratch prefix on the
  existing grants, in the module the plan named, skipped unless the tier-2
  environment marker is present. The owner runs it. No `.github/` change. See
  C-016 and C-017.

## Round 1 implementation (2026-09-28)

Rust owns the S3 protocol in `crates/repark-core/src/session/path_write.rs`:
`ReparkSession::write_path` parses format and mode, validates `partitionBy`
and writer options (mirroring the Python writer message for message),
runs the Q3 save-mode protocol over the registered S3 store, executes one
`COPY` with a per-call temp view, materialises an empty part when `COPY`
writes none (parquet via the `datafusion::parquet` re-export, so no new
dependency), and puts a 0-byte `_SUCCESS` last. It returns the part count
present after the write. `COPY` names are per-statement random
(`{id}_{n}.parquet`), so `append` writes directly with no staging and no
collision window. Append validation reads destination part *files* (prefix
reads refuse without a trailing slash, residue `R-S3-SLASH-READ`) and treats
the string/binary view family as one logical type, since the engine reads
parquet strings back as `Utf8View`. Overwrite at a bucket root refuses loud
rather than deleting a whole bucket.

The endpoint keys resolve from the session config dump at store-build time on
both doors, so late `SET` keys neither apply nor signal (consistent with the
late region override, which is signal-only). A present key always applies;
an absent key leaves the builder call out, which keeps the object_store
defaults (path-style URLs) exactly as before.

Python is a thin forward: `writer_s3.py` detects the scheme and calls the
`session_write_path` binding with the writer state. The local branch is
untouched; `note_local_write_root` is never called for a URL, and a pin
asserts S3 writes leave the local directory empty. The partition-clause
helper moved from `writer_readwriter.py` to `writer_layout.py` unchanged so
the S3 branch fits the file-size ceiling; the move retired that file's
exception row (996 lines).

Cell verdicts: 22 EQUAL; 16 residues in 5 families below. The pins read back
through trailing-slash URLs (residue `R-S3-SLASH-READ` pins the slashless
refusal separately) and normalise part names (note `R-S3-PART-NAME`).

## Residues (2026-09-28)

- **R-S3-CSV-HEADER** — 9 cells (`W-PATH-S3-csv-*`). Repark writes csv with a
  header row by default; Spark writes none. Oracle bytes for
  `csv-error-empty` are one headerless part plus `_SUCCESS`, read back as 3
  rows under `_c0`, `_c1`. Repark bytes are one headed part plus `_SUCCESS`,
  read back as 3 rows under `id`, `grp` with `header=true`, or 4 rows under
  `_c0`, `_c1` with the default headerless read (each part contributes its
  header line as a data row). The empty-frame csv part carries the header
  line (`>0` bytes) where Spark writes 0 bytes. Root cause is the pre-existing
  local write default, pinned locally and out of scope for this round.
- **R-S3-READBACK-NOFILES** — 4 cells (`success-only-error`,
  `success-only-ignore`, `foreign-error`, `foreign-ignore`). Reading a prefix
  with no part files refuses on both engines, with different text. Oracle:
  `AnalysisException` `UNABLE_TO_INFER_SCHEMA`, "Unable to infer schema for
  Parquet. It must be specified manually." Repark: `AnalysisException` "No
  files found at ... Cannot infer schema from an empty location; either add
  data files or declare an explicit schema for the table." (no Spark error
  class). Same refusal reason, read-side wording.
- **R-S3-READBACK-FOREIGN** — 1 cell (`parquet-foreign-append`). Oracle errors
  the read-back (`SparkException`, Spark tries to open `foreign.txt` as
  parquet). Repark lists only `*.parquet` parts, skips the foreign object,
  and reads back the 3 appended rows. Robustness divergence, read-side.
- **R-S3-HIVE-READ** — 2 cells (`parquet-partitionby-1col`,
  `parquet-partitionby-2col`). Writes and listings are EQUAL (hive dirs,
  parts, `_SUCCESS`); the read-back drops the partition columns (`[id]`
  instead of `[id, grp]`). The local door drops them identically (measured
  2026-09-28), so this is a pre-existing engine-wide read gap, not S3
  behaviour. Candidate follow-up card (2026-09-28): "hive partition discovery
  on path reads".
- **R-S3-SLASH-READ** — read method for all 38 cells, pinned separately.
  Oracle reads slashless prefixes (`s3a://bucket/p`). Repark refused them in
  round 1: `PySparkException` "File path ... does not match the expected
  extension '.parquet'" (DataFusion treats a slashless S3 path as a single
  file). **Round 2 (2026-09-28, RETIRED by orchestrator ruling):** the card's
  "no change to read_*" is overridden for this one gap. A slashless `s3://` /
  `s3a://` path that names no exact object but has objects under `<path>/`
  reads as a directory prefix; exact keys keep single-file reads. All 38 pins
  read slashless; `test_trailing_slash_prefix_read_still_works` keeps the
  trailing-slash spelling green.
- **R-S3-PART-NAME** — normalisation note, all cells. Spark names parts
  `part-00000-<UUID>-c000.snappy.parquet`; DataFusion names them
  `{write_id}_{n}.parquet` with a per-statement random id. The pins normalise
  both to `part.<ext>` and compare counts, hive dirs, `_SUCCESS`, sizes, and
  the `_SUCCESS`-last ordering.
- **R-S3-OVERWRITE-WINDOW** — failure path, no oracle cell. Reworded
  2026-09-28: the earlier "Spark keeps the old objects when the write fails"
  was unmeasured — no oracle cell fails an overwrite mid-write — and is
  withdrawn; Spark's failed-overwrite residue is unmeasured. The ruled
  delete-then-write overwrite deletes the prefix first: probe F4 below shows
  a seeded prefix wiped to empty when the `COPY` fails. Accepted by the Q2
  ruling (direct parts plus best-effort cleanup); recorded here so a later
  round can stage overwrites if the window matters.
- **R-S3-APPEND-EXACT** — no oracle cell (re-verify RU-3, 2026-09-28).
  Appending onto a destination that is an exact object refuses loud
  (`AnalysisException` "cannot append to path ..."), because the exact
  object wins the read and appended rows would land invisibly beside it.
  `u12-spark-2.json` covers exact-key error/ignore/overwrite only; Spark's
  append answer is unmeasured (S3A `mkdirs` under a file is expected to
  fail). A Spark cell decides whether the refusal stands.

## Fault record (2026-09-28, moto)

- **F1** — `error`-mode write onto a seeded prefix refuses
  `[PATH_ALREADY_EXISTS]`; the listing afterwards is byte-identical (part
  plus `_SUCCESS`, sizes unchanged).
- **F2** — append with a mismatched schema refuses `column sets differ`; the
  listing afterwards is identical.
- **F3** — a frame that fails before the sink runs (`CAST('x' AS INT)`,
  rejected while optimising) in `error` mode on an empty prefix leaves the
  prefix empty with no `_SUCCESS`.
- **F4** — the same failing frame in `overwrite` mode on a seeded prefix
  deletes the seed and writes nothing: the residue is the empty prefix. See
  `R-S3-OVERWRITE-WINDOW`.
- **F5** — SIGKILL between the first completed part PUT and the `_SUCCESS`
  PUT leaves 4 complete part files (432608, 397717, 216761 and 194749 bytes)
  and no `_SUCCESS`. Parts are complete-or-absent; the marker is last.

## Round 1 verification (2026-09-28)

- Rust: `cargo test -p repark-core --lib` full (845 passed), `cargo test -p
  repark-core --lib -- session::tests::path_write` (15 passed),
  `object_store_s3` (12 passed); clippy `-p repark-core --all-targets` and
  `-p repark-python --all-targets` with `-D warnings
  -A clippy::disallowed_methods`; `cargo fmt -- --check`.
- Python: `test_s3_path_write_1.py` (40 passed: 38 cells plus the slashless
  and no-local-IO pins) on `moto_server` from `target/u12/moto-venv`;
  `test_e2_readwriter.py`, `test_r2_read_formats2.py`,
  `test_r1_read_formats.py`, `test_examples_io_session.py`,
  `test_io_text_1.py`, `test_io_text_2.py`, `test_df_plan_introspect_1.py`,
  `test_df_surface_a_1.py`, `test_df_surface_b_1.py`,
  `test_io_bucket_cluster_1.py`, `test_logical_width_1.py`,
  `test_mapinarrow.py`, `test_nullability_2.py`, `test_time_travel.py`,
  `test_writer_v2.py` all green with zero pin changes;
  `test_production_file_size.py`,
  `test_ex_0_example_coverage.py`,
  `test_cap_1_source_file_line_cap.py` green; `test_aws_acceptance.py`
  collects 11 skipped without the tier-2 marker.
- Gates: `comment_ban.py`, `check_lib_rs`, `check_rust_file_size.py`,
  `check_lib_py.py`, `check_ledger_grammar.py`, `check_docs_links.py`,
  `check_map_md.sh --base origin/main`, ruff check and format checks. The
  `writer_readwriter.py` line-count exception retired (996 lines).

## Out of scope observed (2026-09-28)

- The card's cell arithmetic reads "24 + 3 + 4 + 4 + 2 + 2" but claims 38
  cells; the oracle holds 38 cells with 2 empty-frame cells (no
  `json-empty-df`). The card carries a dated correction; the product covers
  the empty-json shape anyway (Rust protocol test).
- The csv/json refuse-loud messages point at `task/r2-read-formats2-ledger.md`,
  a path that no longer exists (ledgers live under `task/ledgers/`). The Rust
  mirror repeats the pointer verbatim so both doors refuse identically; the
  pointer itself is left for its owner.
- Late `SET` of the endpoint keys (after session build) neither applies nor
  signals, matching the late region override. Only builder/file config feeds
  the S3 store.
- Appending a parquet-read frame (`Utf8View`) onto a dataset written from
  `createDataFrame` (`Utf8`), or the reverse, leaves the dataset unreadable
  ("Fail to merge schema ... Utf8 vs Utf8View"). The local door does the same
  (measured 2026-09-28 on the re-verify wheel), and the S3 fold never touched
  the `COPY` data path. Separate card.

## Round 2 implementation (2026-09-28)

The orchestrator overrode the card's "no change to read_*" for
`R-S3-SLASH-READ` alone: without it no S3 write/read round trip holds.
`object_store_s3.rs` gains `resolve_s3_prefix_for_read`: a trailing-slash
path, a bucket root, and any non-S3 path pass through untouched; otherwise
the registered store is probed — an exact object keeps the spelling
(single-file read), objects under `<path>/` append one `/` (directory read,
the Spark and local-door rule), and anything else (missing keys, sibling
prefixes, unregistered buckets, lookup failures) keeps the original spelling
so downstream errors are byte-identical. The probe rides the store DataFusion
lists through, so the memory, moto and real-S3 list semantics agree (the S3
list prefixes `<path>/`; the memory list is delimiter-aware and excludes the
exact key). `read_parquet` (inside `read_parquet_nullable`), `read_csv`
(inside `read_csv_path`) and `read_json` resolve once per call, so the
double-read shapes probe once.

The 38 moto pins read slashless, as Spark does; no cell verdict moves (every
cell already passed through the trailing-slash spelling). The refusal pin is
replaced by a slashless round-trip pin, a trailing-slash pin, and an
exact-key pin per format. `R-S3-HIVE-READ` stays a residue and is named above
as the candidate follow-up card "hive partition discovery on path reads";
`R-S3-CSV-HEADER` stays (owner decision); `R-S3-OVERWRITE-WINDOW` is reworded
— Spark's failed-overwrite residue is unmeasured.

## Round 2 verification (2026-09-28)

- Rust: `cargo test -p repark-core --lib` full (852 passed, 1 ignored: the
  845 round-1 tests plus the 4 `object_store_s3` prefix-resolution pins and
  the 3 `session::tests::s3_prefix_read` round-trip pins); `cargo clippy -p
  repark-core --all-targets` with `-D warnings -A clippy::disallowed_methods`
  and workspace `make rust-panic-ban` green; `cargo fmt -- --check` green.
- Python: `test_s3_path_write_1.py` (44 passed: 38 slashless cells plus the
  slashless, trailing-slash, 3 exact-key and no-local-IO pins) on
  `moto_server` from `target/u12/moto-venv`; every read-path file from
  `grep -l "read.parquet\|read.csv\|read.json"` (35 files, `-n 8`): 847
  passed, 30 skipped, zero pin changes.
- Gates: `comment_ban.py` over `origin/main...HEAD`, `check_lib_rs`,
  `check_rust_file_size.py` (948 files; `session.rs` 997 under the 1000
  ceiling), `check_lib_py.py`, `check_ledger_grammar.py`,
  `check_docs_links.py`, `check_map_md.sh --base origin/main`, ruff check
  and format checks on the touched test file.

## Verifier fold VU-1..VU-9 (2026-09-28)

The Opus verifier reproduced four silent data-loss bugs on moto at aa471249
plus five smaller findings. Spark decides each fix: `u12_probe2.py` records
10 `VU-*` cells on Spark 4.1.2 (`hadoop-aws` 3.4.2, UTC) against
`moto[server]`, bucket `u12spark2`, saved as `u12-spark-2.json`; the stale
`u12-repark.json` (round-1 capture, every read-back failing locally) is
replaced by a current capture with the moto endpoint configured (33 writes
ok, 5 `PATH_ALREADY_EXISTS` refusals).

Spark's answers: an extension-named destination is a directory — overwrite
3 rows then append 2 rows yields 5 rows under `t.parquet/` (parquet, csv,
json alike). An object at the exact key counts as existing: `error` raises
`PATH_ALREADY_EXISTS`, `ignore` no-ops, `overwrite` deletes the exact
object and writes parts beneath. `#` and `?` stay literal in keys
(`data#v2/`, `data?v=2/`) beside an untouched `data/`. Text writes parts
under the destination with a `value: string` read-back. One answer
contradicts the verifier: the read-transform-overwrite shape does NOT
refuse upfront with `UNSUPPORTED_OVERWRITE.PATH` on Spark 4.1.2 — the job
deletes the source mid-write and fails loud with
`FAILED_READ_FILE.FILE_NOT_EXIST`, leaving only directory markers. The
fold's refusal is therefore stricter than Spark: it refuses before deleting
anything, using Spark's error class for the same condition. The message
wording stays reviewable.

Dispositions. VU-1: `COPY` always targets the directory spelling
(`<url>/`, extension left in place) for every format; appends add parts.
VU-2: the error, ignore and overwrite probes `HEAD` the exact key as well
as listing `<prefix>/`; overwrite deletes the exact object. VU-3: an
overwrite whose destination prefix is read by the frame's own plan —
`ListingTable` scan URLs under the same bucket and prefix in either
direction — refuses with `[UNSUPPORTED_OVERWRITE.PATH]` before any
delete. VU-4: the destination key keeps `#`, `?` and `%` literally (the
Hadoop rule); the `COPY` and read URLs carry the `%`-encoded form so the
list, delete, exact-key and DataFusion keys agree. VU-5: a bare
`host:port` endpoint takes `http://` or `https://` from
`connection.ssl.enabled` (default `https`) and validates with `Url::parse`,
so it never panics. VU-6: an explicit `http://`/`https://` endpoint
decides `allow_http` regardless of the ssl key; the boolean keys accept
only `true`/`false` (case-insensitive); a failed prefix list now fails the
read with the store error instead of the masked extension message. VU-7:
`df.write.text` to `s3://`/`s3a://` refuses loud — routing text through
the S3 door needs an engine text format, a larger change. VU-8: `is_s3_url`
is a `s3://`/`s3a://` prefix match, so `s3:foo` and leading-whitespace
spellings stay local. VU-9: a `p2/` sibling pin (overwrite, error and
ignore on `p` leave `p2/` intact) and column name/type/order comparison in
the oracle comparator.

No new residues. `R-S3-OVERWRITE-WINDOW` (the failure-path window between
delete and commit) is unchanged by the VU-3 upfront refusal, which closes
the success-path loss only.

## Verifier-fold verification (2026-09-28)

- Rust: `cargo test -p repark-core --lib` full green (862 passed, 1
  ignored), including 4 new `session::tests::path_write` pins (extension
  directories, exact-key save modes, self-overwrite refusal, `#` keys)
  and 6 new `object_store_s3` pins (raw split, directory target, bare-host
  normalisation, scheme-led `allow_http`, strict booleans, `#` resolve);
  `cargo clippy -p repark-core -p repark-python --all-targets` with `-D
  warnings -A clippy::disallowed_methods` and workspace `make
  rust-panic-ban` green; `cargo fmt --check` green.
- Python: `test_s3_path_write_1.py` (71 passed: 38 oracle cells with the
  strengthened comparator plus the VU-1..VU-9 pins, every S1 red at
  aa471249) on `moto_server` from `target/u12/moto-venv`; every facade
  file that reads or writes parquet/csv/json/text plus
  `test_dfcore_1_exports.py` (43 files, `-n 8`): 1374 passed, 27 skipped,
  zero local-path pin changes.
- Gates: `comment_ban.py` over `origin/main...HEAD`, `check_lib_rs`,
  `check_rust_file_size.py`, `check_lib_py.py`, `check_ledger_grammar.py`,
  `check_docs_links.py`, `check_map_md.sh --base origin/main`, ruff check
  and format checks on the touched Python files.

## Re-verify fold RU-1..RU-4 (2026-09-28)

The Opus re-verify at b04ee90e confirmed VU-1, VU-2 and VU-4..VU-9 fixed and
charged four findings: two S1 silent data-loss shapes in the VU-3
self-overwrite refusal, and two S3 loud-but-wrong shapes. All four land here.

Dispositions. RU-1: `plan_reads_s3_prefix` walks with
`apply_with_subqueries`, so IN, EXISTS and scalar subqueries (nested
included) count as reads; each shape pins a moto refusal with the listing
unchanged. RU-2: `scan_url_hits_prefix` compares the decoded scan key
(`ListingTableUrl::prefix`) and the `object_store` bucket against the
destination instead of re-splitting the percent-encoded `as_str`, so `a b`,
`a%20b`, `d#v2` and `données` self-overwrites refuse; the `src2`→`src`,
`srcx`→`src`, other-bucket and local-source pass-throughs stay green. RU-3:
the append branch `HEAD`s the exact key and refuses loud when an object sits
at the destination (`R-S3-APPEND-EXACT`; Spark unmeasured). RU-4: the
trailing-slash and exact-object read branches encode through
`split_s3_url_raw` + `encode_s3_key_for_url` like the slashless branch, so
`data#v2/` and its `?`/`%` siblings read the rows.

## Re-verify-fold verification (2026-09-28)

- Rust: `cargo test -p repark-core --lib` full green (864 passed, 1
  ignored), including the 2 new `object_store_s3` pins (trailing-slash and
  exact-object reads encode `#`/`?`/`%`); `cargo clippy -p repark-core -p
  repark-python --all-targets` with `-D warnings -A
  clippy::disallowed_methods` and workspace `make rust-panic-ban` green;
  `cargo fmt --check` green.
- Python: `test_s3_path_write_1.py` (84 passed: the 71 verifier-fold pins
  plus 3 RU-1 subquery, 6 RU-2 encoded-key and pass-through, 1 RU-3
  exact-append and 3 RU-4 trailing-slash pins, every S1 red at b04ee90e) on
  `moto_server` from `target/u12/moto-venv`; every facade file that reads or
  writes parquet/csv/json/text plus `test_dfcore_1_exports.py` (43 files,
  `-n 8`): all green, zero local-path pin changes.
- Gates: `comment_ban.py` over `origin/main...HEAD`, `check_lib_rs`,
  `check_rust_file_size.py`, `check_lib_py.py`, `check_ledger_grammar.py`,
  `check_docs_links.py`, `check_map_md.sh --base origin/main`, ruff check
  and format checks on the touched Python files.
