# Sketch ICE-INTEGRITY-PINS-1: three integrity pins (kill mid-write, unknown commit outcome live, cross-release)

**Date:** 2026-10-14. **Author:** Claude (Opus 5.5), lane B of the Iceberg closeout, unit U-9.
**Status:** sketch, docs only; no product code, no test, no workflow change in this round. The
build waits on the owner's answers to §8. **Plan row:** U-9 of
[the closeout plan](../../../docs/artifacts/iceberg-closeout-plan-2026-10-13.html) ("No
kill-mid-write pin, ICE-COMMIT-UNKNOWN-1 never live, no cross-release pin"). **Records it
closes:** row K3 of
[the production status](../../../docs/cutover/production-iceberg-status-2026-09-14.md) ("PROVEN
offline; no live drill"), the BL-5 named residual ("an injection test for that path") and the
live half of [ICE-COMMIT-UNKNOWN-1](../../../task/ledgers/completed/ice-commit-unknown-1-ledger.md).
**Base:** main `37d60eaf`; fork rev `076d5f98` (`Cargo.lock`). Every `path:line` below is on
those two revs. A fork path carries the prefix `fork:`; it was checked in the cargo checkout of
that rev. **Target:** 1.5.6, with U-11 after it (§0).

## 0. Why, and what is out of scope

The v2 and v3 labels need "integrity under failure" (closeout plan §1): a writer killed between
its data files and its commit leaves the table as it was and its orphans counted; an unknown
commit outcome is drilled on the real catalogs and resolved on reopen, never guessed; a table
written by the last release reads equal on the head, and the reverse. Today none of the three is
pinned on a live catalog, and the kill is pinned nowhere. U-11 (the resource bound) waits on the
kill pin, so that a refusal above the bound and a crash have two different, pinned outcomes.

Out of scope, by name: any change to the commit path, the fork, the error taxonomy or the
workflow file (the workflow change is the owner's, §6.5); a durable commit pointer for the local
catalog (§3.1, question Q5); stamping an operation id on the plain `INSERT` error (question Q7);
the streaming sink's own recovery (`resolve_unknown_outcome`, pinned by the micro-batch units);
multi-writer safety (row K4); orphan deletion (only the count is in scope).

## 1. The three pins in one paragraph each

**K, kill mid-write.** Kill the writing process at the point where every data file, manifest and
manifest list of the statement is durable and the catalog has not been asked to commit. Reopen
from a fresh process. The table reads exactly as before, and the files the attempt wrote are
counted and reported. Local: one Rust pin per write door. Live: one acceptance leg on Glue and on
S3 Tables.

**U, unknown outcome.** Let the catalog commit request leave the process, then cut the network
in four ways (the request lands or not; the follow-up reads pass or not). The statement either
succeeds through the fork's reconciliation or raises `CommitStateUnknownException`. A fresh
session then resolves the outcome by the operation id: committed or not committed, and rows are
present exactly once or not at all. Live on Glue and S3 Tables; an offline Rust twin of the same
cells runs on every pull request.

**X, cross-release.** The v1.5.4 wheel and the head, in two interpreters of one job, write and
read the same scratch tables on both catalogs and both format versions. Every row each reads is
equal, value and type, to what the writer read back from its own table.

## 2. Measured on 2026-10-14 (local, no AWS)

Probes ran in the foreground under `ulimit -v 67108864`, from a scratch directory outside the
tree. "Main" is the release module of `293896ce` (v1.5.5) in a sibling scratch clone; nothing
was built. The probe scripts stay in the lane's scratch directory, uncommitted.

**M1, the local kill.** A memory catalog (`type=hadoop` maps to it,
`crates/repark-core/src/catalog_kind.rs:28`) over a warehouse directory. Setup writes 3 rows. A
child process adopts the table (`CALL register_table` on the newest metadata file) and runs
`INSERT INTO ice.sales.t SELECT … FROM range(2000000)`. The parent watches the table directory
and sends `SIGKILL` at the first sight of a trigger file. A fresh process then adopts the newest
metadata file and reads.

| Trigger | Format | Killed after | Rows on reopen | Snapshots | Data files on disk | Orphan data files |
|---|---|---|---|---|---|---|
| first new data file | v2 | 0.020 s | 3 | 1 | 2 | 1 |
| first new data file | v3 | 0.017 s | 3 | 1 | 2 | 1 |
| first new manifest (`*-m0.avro`) | v2 | 0.285 s | 3 | 1 | 27 | 26 |
| first new manifest (`*-m0.avro`) | v3 | 0.320 s | 3 | 1 | 27 | 26 |
| new metadata file (`00002-….metadata.json`) | v2 | 0.249 s | **2,000,003** | 2 | 27 | 0 |
| new metadata file (`00002-….metadata.json`) | v3 | 0.302 s | **2,000,003** | 2 | 27 | 0 |
| no kill (control) | v2 (200,000 rows) | exit 0 | 200,003 | 2 | 5 | 0 |
| no kill (control) | v3 | exit 0 | 2,000,003 | 2 | 27 | 0 |

Read: the local catalog has no durable pointer, so the newest metadata file is the commit. A kill
before that file leaves the table unchanged with every written file orphaned; a kill after it
reads back as committed. The kill pin must therefore name its point inside the commit call, not
by watching files (§3.2). The trigger-by-file runs are races and are not a pin.

**M2, the orphan report through RePark.** On the two "first new manifest" warehouses, after the
kill:

```
CALL ice.system.remove_orphan_files(table => 'sales.t', dry_run => true, older_than => <now>)
  -> refused: "CALL remove_orphan_files refuses an `older_than` less than 24 hours in the past."
same call, older_than => now - 25 h, fresh file times      -> 0 rows
same call, older_than => now - 25 h, file times set 2 days back -> 28 rows: 26 .parquet, 2 .avro, 0 .json
rows after                                                  -> 3
```

v2 and v3 answer the same. So RePark reports the orphans only when they are older than the
24-hour floor (parity with Spark, row ORPHAN-1), which a fresh kill never is.

**M3, the catalog endpoint.** Inside an unprivileged network namespace (loopback only; an
outbound connect fails with `ENETUNREACH`), with dummy keys and no profile, a RePark session
configured `spark.sql.catalog.c.uri=http://127.0.0.1:<port>` (Glue) or
`spark.sql.catalog.c.endpoint_url=http://127.0.0.1:<port>` (S3 Tables) sends its catalog calls to
a local listener:

| Catalog | First request seen | `Host` signed | Statement on a listener that answers | Statement on a listener that reads the request and closes |
|---|---|---|---|---|
| Glue | `POST /`, `X-Amz-Target: AWSGlue.GetDatabases` | `127.0.0.1:<port>` | `SHOW NAMESPACES` ok | 3 requests in 0.8 s, then `DispatchFailure … TransientError … IncompleteMessage` |
| S3 Tables | `GET /namespaces/<bucket ARN>` | `127.0.0.1:<port>` | `SHOW NAMESPACES` ok | 3 requests in 0.28 s, then the same error |

Read: the catalog properties pass through RePark untouched (`crates/repark-core/src/catalog_config.rs:92`,
`crates/repark-iceberg/src/catalog/builders.rs:123` and `:162`), so a local gate in front of the
catalog needs no product change. The operation is visible in clear text on both catalogs. The
signature covers the local host, so a gate that forwards to AWS must sign again (§4.2). The AWS
SDK sends a request whose response was lost three times: the SDK's retry classifier treats every
timeout and IO error as transient for every operation (crate `aws-smithy-runtime` 1.12.1 from crates.io,
`TransientErrorClassifier::classify_retry` in its client retry classifiers module), and the fork
configures no retry policy for either catalog client. That holds for the commit call too, which
§4.4 cell U-5 drills (unmeasured here: no local Glue or S3 emulator exists on this box).

**M4, resolution by id.** On the M1 warehouse killed after the metadata file:

```
SELECT snapshot_id, operation, summary['engine.operation-id'], summary['added-records']
FROM ice.sales.t.snapshots ORDER BY committed_at
  (3994156687832690410, 'append', '73ada5ba-…', '3')
  (4395867129689212565, 'append', '1527cc35-…', '2000000')
```

The `snapshots` table exposes `summary` as `map<string,string>`, so "did the statement with id
X commit" is one query on a fresh session.

**M5, two releases in two interpreters.** Stand-ins, not the release wheels: the v1.5.5 release
module (`293896ce`) and a build that still reports `1.5.4` (`c36caefc`, after the `v1.5.4` tag
`f4e424c1`; no checkout at the tag carries a built module on this box). One warehouse, ten
columns (`BIGINT`, `STRING`, `DECIMAL(10,2)`, `TIMESTAMP`, `TIMESTAMP_NTZ`, `DATE`, `BOOLEAN`,
`ARRAY<INT>`, `STRUCT<a INT, b STRING>`, `MAP<STRING, INT>`), 1,000 rows. Writer A creates and
reads back to Arrow IPC; reader B adopts the newest metadata file and reads; B appends 3 rows;
A reads again.

| Format | Direction | Reader equals writer (values, schema) | Appender equals re-reader | Rows |
|---|---|---|---|---|
| v2 | 1.5.4 build writes, 1.5.5 reads | True, True | True | 1,003 |
| v2 | 1.5.5 writes, 1.5.4 build reads | True, True | True | 1,003 |
| v3 | 1.5.4 build writes, 1.5.5 reads | True, True | True | 1,003 |
| v3 | 1.5.5 writes, 1.5.4 build reads | True, True | True | 1,003 |

Read: the mechanism works (two interpreters, one warehouse, adopt by metadata file, Arrow
equality). It proves nothing about the real v1.5.4 wheel or about delete files; §5 carries those.

## 3. Pin K: kill between the data files and the commit

### 3.1 Where it sits today

Every write door ends in one fork call, `Transaction::commit`
(`fork:crates/iceberg/src/transaction/mod.rs:379`), whose `do_commit` (`:501`) asks the catalog
to commit at `:582` (`catalog.update_table`). Before that call the statement's data files, delete
files, manifests and manifest list are written and nothing references them.

| Door | Entry (main) | Commit function | Operation id on the error |
|---|---|---|---|
| `INSERT … SELECT` / `VALUES`, no write option (and `writeTo().append()`, which reaches the router as `INSERT`) | `execute_insert_routed`, `crates/repark-spark/src/router.rs:414`, to `passthrough_after_p11` at `:470` (`crates/repark-spark/src/catalog_ops.rs:85`), then DataFusion's `insert_into` (`fork:crates/integrations/datafusion/src/table/mod.rs:244`) | `IcebergCommitExec` (`fork:crates/integrations/datafusion/src/physical_plan/commit.rs:84`), which mints `engine.operation-id` at `:372` and commits at `:402` | `None`: the fork mints the id and does not hand it to RePark |
| `INSERT` with a write option, a partition clause the router owns, or a session write key | `router.rs:451`, `:459` to `execute_append_with_options`, `crates/repark-spark/src/append_with_options.rs:10` | `commit_append_with_summary`, `crates/repark-iceberg/src/write/write_options.rs:425` (from `append_with_options.rs:90`); `append_with_statement_options`, `write_options.rs:104` (from `:99`) | minted (`commit_err`, `crates/repark-iceberg/src/write/commit_error.rs:40`) |
| Rust `append` API | `crates/repark-iceberg/src/write/append.rs:33` | `commit_append`, `append.rs:153` | minted |
| `INSERT OVERWRITE`, `TRUNCATE` | the router's overwrite arm, `router.rs:447` | `commit_overwrite_replace_all_to`, `crates/repark-iceberg/src/write/overwrite_commit.rs:15` | minted |
| `MERGE`, RePark `DELETE` and `UPDATE` | `crates/repark-iceberg/src/write/merge/snapshot_commit.rs` | `commit_overwrite` (`:143`), `commit_row_delta_kind` (`:319`) | minted |

Where the catalog call puts the commit:

| Catalog | `update_table` | Metadata file written | The commit point |
|---|---|---|---|
| local (memory; `type=hadoop` maps to it) | `fork:crates/iceberg/src/catalog/memory/catalog.rs:548` | `:584`, inside the call | the metadata file: the pointer lives in process memory and dies with it (M1) |
| Glue | `fork:crates/catalog/glue/src/catalog.rs:928` | `:943`, before the request | Glue's `UpdateTable` (sent at `:955`) |
| S3 Tables | `fork:crates/catalog/s3tables/src/catalog.rs:860` | `:872`, before the request | `UpdateTableMetadataLocation` (`fork:crates/catalog/s3tables/src/commit_transport.rs:101`) |

What exists, and what does not:

- **Cleanup in process.** A definite commit failure deletes the attempt's files best-effort, and
  an unknown outcome skips that (`crates/repark-iceberg/src/write/merge/abort.rs:15`, the skip at
  `:20`; registry BL-5). The partitioned CTAS write takes a census of the data root and sweeps
  what appeared on a failed write (`crates/repark-iceberg/src/write/partition_write.rs:301`, pin
  at `:696`). A killed process runs neither.
- **Orphan reporting.** Only `remove_orphan_files` (rows ORPHAN-1 to ORPHAN-4), which refuses an
  `older_than` under 24 hours (M2) and refuses on S3 Tables (ORPHAN-S3TABLES-1). The stream
  path already leaves rolled files on a failed write (WRITE-ABORT-INSERT-1).
- **Recovery on reopen.** None for a batch write: a reopen is a catalog load. The micro-batch sink
  has its own walk, `resolve_unknown_outcome` (`crates/repark-iceberg/src/write/sink_offsets.rs:484`).
  The local catalog reopens by adopting the newest metadata file through `CALL register_table`
  (M1), or by a path read that follows `version-hint.text` (`crates/repark-core/src/iceberg_path.rs:99`).
- **No crash pin.** No test kills a writer. The micro-batch "crash" pins are in-process fault
  catalogs (`crates/repark-iceberg/src/microbatch/crash_tests.rs:50`, `UnknownAfterLanding` and
  `UnknownWithoutLanding`). Two tests re-run their own binary as a child, which is the mechanism
  this pin reuses: `crates/repark-python/src/dataframe/tests.rs:345` (a panic fence) and
  `crates/repark-connect/tests/it/copy_accounting.rs:84` (an allocation limit).

### 3.2 The failure to inject, and how

The point: the entry of the catalog's commit call. Every data file, delete file, manifest and
manifest list is durable; the metadata file is not yet written on the local catalog, and the
commit request has not reached Glue or S3 Tables.

| Option | Mechanism | Precision | Product hook | Cost | Verdict |
|---|---|---|---|---|---|
| **K-A** test catalog decorator and a re-run child | The Rust test re-runs its own binary as a child (an environment key selects the child arm). The child wraps the memory catalog in `HoldCatalog`, a test-only `Catalog` that forwards every call; its `update_table` writes a marker file holding the attempted snapshot ids and then waits forever. The child registers it as a session catalog (`register_iceberg_catalog`, the helper at `crates/repark-spark/src/tests/describe_view_routing.rs:255`) and runs the statement through the Spark door. The parent waits for the marker, kills the child (`SIGKILL`), and reopens from a fresh catalog. | exact | none (test code only) | one test file of about 250 lines; 18 children (§3.3), estimated 20 to 40 s (unmeasured) | **chosen for the Rust pin** |
| **K-B** the catalog endpoint gate holds the commit request | The child's session points its catalog at the gate of §4.2. The gate holds `UpdateTable` (Glue) or `UpdateTableMetadataLocation` (S3 Tables) unanswered and unforwarded, and tells the parent. The parent kills the child; the gate then closes the connection without forwarding. | exact: the request is held, the metadata file is written | none | shares the gate with pin U | **chosen for the live leg** |
| K-C kill on file observation | M1: poll the table directory, kill at a trigger file | a race of milliseconds between the manifest list and the metadata file; M1 reached both sides | none | none | rejected as a pin |
| K-D stop at a system call | `strace --inject` or an `LD_PRELOAD` shim stops the child at the first rename onto `*.metadata.json` | exact on a local file system only | none, but a C shim or `strace` on the runner | medium; Linux only; breaks if the storage layer stops renaming | rejected |
| K-E a fault point in product code | a `fail`-style point or a session key that pauses before `update_table` | exact on every catalog, through the released wheel | **yes** | small code, but a test seam inside the commit path, which AGENTS.md "Change discipline" names a sensitive path | rejected unless the owner wants a kill without the gate (Q1) |

### 3.3 The cells

**Local Rust pin**, each door on format version 2 and on 3. Doors: K-1 `INSERT … SELECT` (the
fork's commit), K-2 `INSERT … SELECT` with a write option (`commit_append_with_summary`), K-3
`INSERT OVERWRITE`, K-4 `MERGE` copy-on-write, K-5 `MERGE` merge-on-read (delete files), K-6
`DELETE … WHERE` copy-on-write, K-7 `CREATE TABLE … AS SELECT`. Each cell asserts:

1. **Reached and killed.** The marker exists and names the attempted snapshot id; the child died
   of signal 9.
2. **Unchanged.** On reopen the newest metadata file, the current snapshot id, the set of
   snapshot ids and `SELECT * … ORDER BY` (values and Arrow types) equal the pre-state; the
   attempted snapshot id is absent.
3. **Orphans counted and reported.** Files under the table location minus the files any snapshot
   reaches, split by kind; the test prints one line,
   `K-n v<f> orphans data=<n> deletes=<n> manifests=<n> lists=<n> metadata=<n>`, and asserts
   `data >= 1` (K-1 to K-5, K-7), `deletes >= 1` (K-5), `manifests >= 1`, `lists = 1`,
   `metadata = 0`.
4. **The report agrees with RePark.** After setting every file time two days back,
   `CALL … remove_orphan_files(dry_run => true, older_than => now - 25 h)` lists exactly the set
   of (3) (M2: 28 = 26 data files + 2 avro files).
5. **Writable.** One more `INSERT` on the reopened table commits and reads back the pre-state
   plus its rows.

K-7 has no pre-state. Its expected answer (no table, or an empty table the create-first path left)
is measured in step 0 and written into the cell before the pin; the cell then asserts it and (3).

**Controls.** K-C0: the hook forwards at once; the statement commits, orphans are 0. K-C1: the
hook forwards and stops after `update_table` returns; after the kill the reopen reads the new
rows (M1's metadata-file row, declared as the local catalog's answer, Q5). Both on v2 and v3.
Total: 7 × 2 + 2 × 2 = 18 children.

**Live leg**, on Glue and S3 Tables, v2 and v3, doors `INSERT … SELECT`, `MERGE`,
`INSERT OVERWRITE`: 12 cells (K-G-n, K-S-n). Each asserts (1') the gate held the commit request
and closed it unforwarded after the kill; (2') on reopen the attempted snapshot id (read from the
metadata file the held request names) is absent and the rows equal the pre-state; (3') the
orphans are counted: the held request's metadata file is adopted into a local memory-catalog
session (`register_table`, no AWS catalog write), its snapshot's files minus the pre-state's are
listed, and each is confirmed present by a `HEAD` (`pyarrow.fs.S3FileSystem`); the leg prints the
same report line with `metadata=1`; on Glue the count is checked again by listing the table
prefix (S3 Tables refuses a listing); (4') writable. "Unchanged" is not "the same snapshot set":
S3 Tables maintenance may commit in between (§7).

### 3.4 Where the pins live

- `crates/repark-spark/src/tests/kill_mid_write.rs` (new): `HoldCatalog`, the child arm, the 18
  cells; `#[cfg(unix)]`. It joins the existing library test binary beside
  `describe_view_routing.rs`, whose `register_catalog` it reuses, so it adds no test binary.
  If T-1 has moved `crates/repark-spark/src/tests/` into one `tests/it` binary by then, it
  moves with it.
- `python/repark/tests/test_aws_acceptance_integrity_kill.py` (new): the live leg.
- `python/repark/tests/_acceptance_commit_gate.py` (new, not collected): the gate of §4.2.
- `python/repark/tests/_acceptance_integrity_child.py` (new, not collected): the child that runs
  one statement through a session pointed at the gate.
