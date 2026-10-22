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

Fork lines are cited at `076d5f98`; re-read them at the current pin `d80a2884` before building.

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
in four ways (the request lands or not; the follow-up reads pass or not), and once more letting
the SDK's own resend through (U-5). The statement either
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

**Built 2026-10-22 (pin K, local).** `crates/repark-spark/src/tests/kill_mid_write.rs` and
`kill_mid_write_hold.rs`: the 18 cells of §3.3 green, 20 of 20 runs, four mutations red
([the unit ledger](../../ledgers/staging/ice-integrity-pins-1-ledger.md)). Premises corrected on
main `18626942` and fork `d80a2884`: the fork lines moved (`Transaction::commit` `:373`,
`do_commit` `:495`, `update_table` call `:574`, reconcile `:424`); the `Catalog` trait gained
`publish_create_table` and `publish_replace_table`, so the decorator forwards every method and
follows `EncryptionGuardCatalog`; K-7 holds `publish_create_table`, the second hook point §7
anticipated, and reads back as the created table, because the staged metadata file is the local
commit (orchestrator ruling K-1, the local catalog only); `type=hadoop` is the memory catalog
with Hadoop file names. Flagged for pin U, not acted on: the fork's `do_commit` already checks
`rejected_attempt_landed` before re-basing (`mod.rs:497`), the first half of ruling 84's card.
The live leg (§3.3, 12 cells) and pins U and X await #1070.

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

## 4. Pin U: the unknown commit outcome, live

**Built 2026-10-22 (pin U, legs pending #1070).**
- The offline twin is `crates/repark-spark/src/tests/commit_unknown_doors.rs` with
  `commit_unknown_gate.rs`: the 24 cells of §4.5, green, and red under five mutants.
- The live leg is `python/repark/tests/parity/test_aws_acceptance_commit_unknown.py`, with its
  gate in `_acceptance_commit_gate.py`, its signer in `_acceptance_sigv4.py` and its resolver in
  `_acceptance_commit_resolver.py`. The gate's and the resolver's AWS-free tests run on every pull
  request.
- One premise moved. Fork #368, carried by the pin `d80a2884`, turned the SDK retry off on both
  commit calls, so the SDK re-send that §4.4's U-5 drilled no longer happens. Under the
  orchestrator's ruling of 2026-10-22, U-5 asserts one commit request, and U-5r drills the fork's
  own re-send after a throttle reply. That makes 96 statements.
- Paths follow #1113 to `python/repark/tests/parity/`.
- The [unit ledger](../../ledgers/staging/ice-integrity-pins-1-ledger.md) holds C-030 to C-041.

### 4.1 Where it sits today

- **The fork decides the kind.** Glue: `map_glue_commit_sdk_error`
  (`fork:crates/catalog/glue/src/commit_transport.rs:314`) maps an SDK error by whether the
  request may have been sent: maybe sent → `CommitStateUnknown`; never sent → `Unexpected`; a
  response → `map_update_table_service_error` (`:343`), where `ConcurrentModificationException`
  is a retryable `CatalogCommitConflicts` and an internal error or operation timeout is unknown.
  S3 Tables does the same (`fork:crates/catalog/s3tables/src/commit_transport.rs:299`, `:331`).
- **The fork never retries the unknown kind and reconciles it.** `Transaction::commit`
  (`fork:crates/iceberg/src/transaction/mod.rs:379`) retries only retryable kinds other than
  unknown; on unknown it calls `reconcile_unknown_commit_outcome` (`:430`), which reloads the
  table under the `commit.status-check.*` table properties
  (`fork:crates/iceberg/src/transaction/commit_status.rs:83`, `:90`, `:97`, `:104`; defaults
  3 retries, 1 s, 60 s, **30 minutes** total) and searches the snapshot set for the attempted
  snapshot ids: found → `Ok` with no re-apply; absent after a good refresh → the original error;
  unreadable → the original error.
- **The fork's own drills sit below RePark and below the SDK's resend.** Its transport seams are
  `#[cfg(test)]` (`ScriptedGlueCommitTransport`, `DiscardingGlueCommitTransport` in
  `fork:crates/catalog/glue/src/commit_transport.rs`). Its credentialed accepted-then-lost smoke
  (`fork:crates/catalog/glue/src/commit_outcome_tests.rs:573`, gated by `ICEBERG_PR5A_CREDENTIALED`;
  the S3 Tables twin keys the same variable at
  `fork:crates/catalog/s3tables/src/commit_outcome_tests.rs:40`) turns a success into a lost
  response after the SDK returned, so the SDK never resends. Row R157 still lists those smokes
  as pending (`fork:docs/parity/GAP_MATRIX.md:159`).
- **RePark wraps it.** `commit_err` (`crates/repark-iceberg/src/write/commit_error.rs:40`) carries
  the id minted by `operation_id_and_summary` (`:58`); `is_commit_state_unknown` (`:65`) keeps a
  possibly landed CTAS table; `error_map` downcasts it (`crates/repark-core/src/error_map.rs:171`;
  the plain kind through `classify_iceberg_error`, `:274`); Python raises
  `repark.errors.CommitStateUnknownException` with `operation_id`
  (`python/repark/src/repark/errors.py:343`). The MERGE abort keeps the files on unknown
  (`crates/repark-iceberg/src/write/merge/abort.rs:20`). Offline pins:
  `crates/repark-iceberg/src/write/merge/tests/commit_unknown.rs:44` and the rest listed in the
  registry row ICE-COMMIT-UNKNOWN-1.
- **Resolution on reopen.** No product call. The registry row gives Airflow its guidance (alert on
  the class, retry only `MERGE` and `CTAS IF NOT EXISTS`). M4 shows the one query that resolves a
  stamped statement. The micro-batch sink resolves its own (`sink_offsets.rs:467`, `:484`).
- **Live.** Nothing. Row K3: "No live drill on a real catalog is recorded."

### 4.2 How the network is cut

| Option | Mechanism | What it exercises | Product or fork change | Verdict |
|---|---|---|---|---|
| **U-A** catalog endpoint gate | A Python gate in the test process. The writer session's `uri` (Glue) or `endpoint_url` (S3 Tables) names the gate (M3). The gate reads each request, signs it again for the regional endpoint with the job's credentials (SigV4; M3 shows the signature covers the local host), forwards it over HTTPS, and for the commit call obeys the cell's script: forward or not, answer or reset. It finds the commit by `X-Amz-Target: AWSGlue.UpdateTable`, or by the S3 Tables `UpdateTableMetadataLocation` path (the exact path is a step-0 check). It refuses any request naming a namespace other than the scratch one. A cut is a reset (the connection closes), never a silent hold: the SDK sets no read timeout and would wait forever. | the real SDK with its own resend (M3), the fork's classification, retry gate and reconciliation, RePark's wrapping, the Python class, through the built module | none | **chosen for the legs** |
| U-B the fork's transport seam | expose the fork's `#[cfg(test)]` transports behind a cargo feature; a credentialed Rust test | the fork's classifier and reconciliation; not the SDK's resend, not the wheel | fork change | rejected for U-9; the fork lane may run its own credentialed smoke (Q2) |
| U-C a RePark-side catalog decorator | a test `Catalog` around the real catalog: forward then answer unknown; answer unknown without forwarding; fail reads for a window | the fork's reconciliation and RePark's wrapping; not the SDK, not the fork's classification | none, but a credentialed cargo step | rejected for the legs; **chosen as the offline twin** on the memory catalog (§4.5) |
| U-D a host-level cut | toggle a firewall rule or namespace when the commit is seen | the whole stack | root on the runner | rejected: the request leaves before the toggle; a race |
| U-E a short SDK timeout | a timeout below the commit's latency | the stack | a key RePark does not expose | rejected: sent or not sent is chance |

### 4.3 What RePark reads on reopen, and what it answers

The resolver is a fresh session on the real endpoint, never on the gate.

- The statement raised with `operation_id = X`:
  `SELECT count(*) FROM <t>.snapshots WHERE summary['engine.operation-id'] = 'X'`; 1 is
  committed, 0 is not committed, more than 1 fails the cell (a duplicate commit).
- The statement raised with `operation_id = None` (the plain `INSERT` door, §3.1): the gate kept
  the commit request, which names the attempt's metadata file; the harness adopts that file into
  a local memory-catalog session, reads the attempted snapshot id, and asks
  `SELECT count(*) FROM <t>.snapshots WHERE snapshot_id = <id>`. A user holding `None` has only
  the snapshot-set difference, sound only under the single-writer rule (inventory §2): Q7.
- Never guessed. The resolver answers only after the gate has closed every connection that held
  the commit (in U-3 and U-4 the request never left the gate, so nothing in flight can land) and
  after two reads of the current metadata location, 2 s apart, agree. A failed read makes the
  cell fail as "unresolved"; the resolver never picks an answer.
- Rows: `SELECT * … ORDER BY` equals the pre-state plus the statement's rows once (committed), or
  the pre-state (not committed).

### 4.4 The cells

Each cell runs on Glue and S3 Tables, v2 and v3, for three doors: `INSERT … SELECT` (plain),
`INSERT … SELECT` with a write option, `MERGE`. Drill tables carry
`commit.status-check.num-retries = 1`, `min-wait-ms = 100`, `max-wait-ms = 200`,
`total-timeout-ms = 5000`, so a cell whose reads are cut ends in seconds.

| Cell | Gate script for the commit call | Expected answer of the statement | Expected resolution |
|---|---|---|---|
| U-1 landed, reads open | forward; reset its response; reset any resend unforwarded; forward reads | succeeds: the fork sees unknown, reloads, finds the snapshot | committed; rows once; one snapshot with the id |
| U-2 landed, reads cut | forward; reset its response and every later request until the statement returns | `CommitStateUnknownException`; `operation_id` a string on the option and `MERGE` doors, `None` on the plain door | committed; rows once |
| U-3 absent, reads cut | read it, do not forward, reset; reset every later request | `CommitStateUnknownException` | not committed; rows equal the pre-state; orphans counted as in K (the attempt's metadata file included) |
| U-4 absent, reads open | read it, do not forward, reset; forward reads | `CommitStateUnknownException`: the refresh shows the snapshot absent and the fork surfaces the original error | not committed; rows equal the pre-state |
| U-5 landed, the SDK's resend forwarded | forward; reset its response; forward the SDK's resends and every read | succeeds or raises the class; either way rows exactly once | rows exactly once; at most one snapshot with the id |

**2026-10-22, U-3's orphan check narrowed live (fix round 1, verifier S1-3).** The built leg U
does not count U-3's orphans as pin K does. Its live check is `metadata=1 lists=1`: the held
metadata file and the attempt's manifest list exist on storage. The data files and manifests
are not counted, and ruling 82's ceiling (at most the files the crashed commit wrote) is not
asserted live. The reasons: pyarrow cannot read the Avro manifest list, `register_table` is
refused on S3 Tables (R126), and pin K counts the files and asserts the ceiling on the local
catalog. Ledger C-044.

**U-5's prediction, unmeasured.** The SDK resends the commit (M3). The resend carries the base
version (Glue `VersionId`, the S3 Tables version token), so the service answers a conflict, which
the fork maps to a retryable `CatalogCommitConflicts`. `Transaction::commit` then re-applies the
append on the refreshed base, which already holds the first attempt's snapshot: the rows land
twice. If U-5 answers that, it is a silent duplicate and a halt (§7, H-U5), not a pin to record.

**Controls.** U-C0: the gate forwards everything; the statement succeeds, rows once. U-C1: the
same statement with no gate; the same answer as U-C0, so the gate adds nothing. U-C2, never sent:
a gate cannot produce it (the request has left the SDK when the gate sees it); the fork's offline
pins (PR-5A) cover it, named here and not run. Count: (5 + 2) × 3 doors × 2 formats × 2 catalogs
= 84 statements.

### 4.5 Where the pins live

- `python/repark/tests/test_aws_acceptance_commit_unknown.py` (new): the live leg, with the gate
  of §3.4.
- `crates/repark-spark/src/tests/commit_unknown_doors.rs` (new): the offline twin. A test
  `GateCatalog` on the memory catalog forwards and answers unknown, or answers unknown without
  forwarding, and fails `load_table` for the status-check window or not: U-1 to U-4, three doors,
  v2 and v3, 24 cells, asserting the same answers (the class through `engine_err`, the id present
  or `None`) and the same resolution. U-5 has no offline twin: it is the SDK's behaviour.

## 5. Pin X: the previous release and the head read each other's tables

**Built 2026-10-22 (pin X, live leg pending #1070).**
- The previous release is **1.5.5** (the orchestrator's ruling, by ruling 81's checklist line),
  hash-pinned in `python/repark/tests/parity/previous_release_requirements.txt`.
- The runner is `_acceptance_cross_release_runner.py` and the driver is `_acceptance_cross_release.py`.
- The live leg is `test_aws_acceptance_cross_release.py`, 40 cells.
- The local control is `test_cross_release_local.py`: X-L reads 20 of 20 cells equal in both
  directions, X-NS prints equal, and X-C0 and X-C1 run on every pull request.
- The head still reports 1.5.5, so a side's identity is its version plus its install origin.
- X-D's `total-records` counts data-file records under merge-on-read (1,050 against 900 rows
  read), so §5.3 item 3 is asserted as the two sides agreeing.

### 5.1 Where it sits today

- **No pin.** No test runs two RePark releases against one table. The acceptance job builds only
  the head (`maturin develop`, `.github/workflows/aws-acceptance.yml:77`).
- **The wheels.** Every tag builds one manylinux wheel, checks that its version matches the tag,
  and publishes it to PyPI through the `release` environment (`.github/workflows/release.yml`).
  `v1.5.4` is tagged at `f4e424c1` (`STATUS.md`, release state). Whether PyPI serves
  `repark==1.5.4` is unmeasured: this round fetched nothing, and the local package cache holds
  PyPI wheels up to 1.5.3 only. The plan names 1.5.4 because it was written before 1.5.5 shipped
  (Q3).
- **What can be reused.** U-10's comparison of two engines' reads, `compare_schemas` and
  `compare_row_sets` (`python/repark/tests/_acceptance_cross_read.py:166`, `:193`); the newest
  metadata file, `current_metadata_location` (`python/repark/tests/_acceptance_v3.py:264`); delete
  files, `delete_file_rows` (`_acceptance_v3.py:185`); the S3 Tables v3 create classifier,
  `classify_v3_create_outcome` (`_acceptance_v3.py:288`).

### 5.2 How the two releases run in one job

- **Install, before credentials.** In the build step, a second interpreter:
  `uv venv --python 3.12 .venv-previous`, then `uv pip install --only-binary :all:
  --require-hashes -r python/repark/tests/previous_release_requirements.txt` into it. The
  requirements file (new, written in the build round by `uv pip compile --generate-hashes` from
  `repark==1.5.4`) pins the wheel and its dependencies by hash, so a re-published or squatted file
  cannot enter. The step ends with an import check of the version. The wheel tag is
  `cp312-abi3`, so Python 3.12 serves both releases.
- **Run.** The leg runs in the head's interpreter and starts the previous release as a child,
  `$REPARK_PREVIOUS_RELEASE_PYTHON _acceptance_cross_release_runner.py <json>`. The runner (new,
  not collected) is one script both releases execute; it uses only calls 1.5.4 has (the builder,
  `sql`, `to_arrow`), runs one operation (create, insert, delete, merge, append or read), writes
  the table it reads to an Arrow IPC file, and prints its `repark.__version__`. The head side runs
  the same runner in the head's interpreter, so both sides take the same path.
- **Tables.** Fixed names under the scratch namespace, `testing_xrel_<kind>_v<f>_<writer>`. The
  writer starts each run with `CREATE OR REPLACE TABLE … AS SELECT`, so the run's writer is the
  table's creator and nothing accumulates but snapshots.

| Kind | Shape | What it proves |
|---|---|---|
| X-P plain | the ten columns of M5, 1,000 rows | data files, manifests, stats |
| X-T partitioned | X-P `PARTITIONED BY (days(ts), bucket(4, id))` | partition specs and manifest partition summaries |
| X-D deletes | X-P with merge-on-read modes; the writer runs `DELETE … WHERE id % 10 = 0` and a `MERGE` that updates 50 rows | v2 position delete files; v3 deletion vectors |
| X-E evolved | X-P, then `ALTER TABLE … ADD COLUMN`, `RENAME COLUMN`, and one more insert | field ids across schema versions |

Nanosecond columns are out: R-008 and R-019 moved nanosecond writes in 1.5.5 (`STATUS.md`,
release state), so the two releases do not write them alike. Step 0 records one nanosecond table as a cell that is printed,
not asserted (X-NS), so the gap is visible.

### 5.3 The cells

Every kind on Glue and S3 Tables, v2 and v3, in both directions: the previous release writes and
the head reads; the head writes and the previous release reads (4 × 2 × 2 × 2 = 32 cells). Each
asserts:

1. **The right releases ran.** The writer and the reader each printed the version their role
   names (`1.5.4` and the head's); a cell where both print the same version fails.
2. **Equal.** The reader's Arrow table equals the writer's own read-back: `compare_schemas` and
   `compare_row_sets` find nothing (values and types).
3. **Counted.** The current snapshot's `total-records` equals the rows each side read.
4. **Deletes seen alike** (X-D). Both sides list the same delete files.

**Mixed-writer cells** (M5's second half), on X-P, both directions, both formats, both catalogs
(8 cells): the reader appends 3 rows, and the writer reads again and equals the appender's read.

**Controls.** X-C0: head writes, head reads, through the runner (the runner adds nothing). X-C1:
the runner refuses when its version is not the one its role names (a mutant of 1 must go red).
S3 Tables v3: if the service refuses `format-version` 3 at create, the cell records the
refusal through `classify_v3_create_outcome`, as the acceptance legs already do.

## 6. The legs

### 6.1 Files and markers

| File (all new) | Kind | Runs |
|---|---|---|
| `python/repark/tests/test_aws_acceptance_integrity_kill.py` | leg K, 12 cells | AWS workflow only |
| `python/repark/tests/test_aws_acceptance_commit_unknown.py` | leg U, 84 statements | AWS workflow only |
| `python/repark/tests/test_aws_acceptance_cross_release.py` | leg X, 40 cells | AWS workflow only |
| `python/repark/tests/_acceptance_commit_gate.py` | the gate (§4.2) | helper, not collected |
| `python/repark/tests/_acceptance_integrity_child.py` | the writer child of leg K | helper, not collected |
| `python/repark/tests/_acceptance_cross_release_runner.py` | the runner of leg X | helper, not collected |
| `python/repark/tests/previous_release_requirements.txt` | hash-pinned 1.5.4 | read by the workflow |
| `python/repark/tests/test_acceptance_commit_gate.py` | the gate against a loopback fake endpoint with dummy keys (M3's shape): commit detection on both catalogs, the scratch-namespace refusal, every script (forward, reset, hold), SigV4 against fixed test vectors | every pull request, no AWS |
| `crates/repark-spark/src/tests/kill_mid_write.rs` | pin K, 18 cells (§3.4) | every pull request |
| `crates/repark-spark/src/tests/commit_unknown_doors.rs` | U's offline twin, 24 cells (§4.5) | every pull request |

Markers: each leg module carries the same module-level
`pytestmark = pytest.mark.skipif(os.environ.get("REPARK_AWS_ACCEPTANCE") != "1", …)` as
`python/repark/tests/test_aws_acceptance.py:84`. The S3 Tables tests skip when `TABLE_BUCKET_ARN`
is absent, as `python/repark/tests/test_aws_acceptance_cross_read.py` does. Leg X fails, never
skips, when `REPARK_PREVIOUS_RELEASE_PYTHON` is unset with the gate on: a skip would hide a
missing install. Every new `.py` and `.rs` file updates its directory's `map.md` in its commit.

### 6.2 The scratch-only rule

- Namespace `testing_repark_acceptance`, tables prefixed `testing_`; nothing is dropped; the
  placeholder-bucket refusal runs first (`assert_real_buckets_configured`,
  `python/repark/tests/_acceptance.py:42`); account ids are masked in every message.
- The gate refuses a request whose Glue `DatabaseName` or S3 Tables namespace is not the scratch
  one, and forwards only to the Glue and S3 Tables endpoints of the job's region. It never logs a
  header (they carry credentials).
- The orphans legs K and U-3 leave sit under the warehouse scratch prefix, which the bucket's
  lifecycle policy already cleans (the workflow's header comment). No deletion is added.

### 6.3 Order in the job

After the dbt gold step and before U-10's cross-read step if the owner has wired it, so every
table exists before Spark's discovery and Spark also reads the integrity tables. The legs need
the same environment as the acceptance step plus `REPARK_PREVIOUS_RELEASE_PYTHON`.

### 6.4 Run time added (estimates, unmeasured)

| Leg | Work | Estimate |
|---|---|---|
| K | 12 kills, each a child start, a write, a reopen and the orphan listing | 3 to 5 min |
| U | 84 statements (cut-read cells bounded by the 5 s status-check budget), each with a reopen | 12 to 15 min |
| X | 40 cells, each one or two child runs | 6 to 8 min |
| install of 1.5.4 | before credentials | under 1 min |
| local Rust pins (CI, not the AWS job) | 18 kills + 24 offline cells | under 1 min |

The `live-aws` job's `timeout-minutes` is 90 today (`.github/workflows/aws-acceptance.yml:49`);
about 25 minutes more suggests 120. Step 4 replaces these estimates with the first run's
numbers.

### 6.5 The workflow change the owner would make (a proposal; the file is untouched)

Against `.github/workflows/aws-acceptance.yml` on `37d60eaf`. It applies with or without U-10's
proposed step; with it, the new step goes before the cross-read step.

```diff
--- a/.github/workflows/aws-acceptance.yml
+++ b/.github/workflows/aws-acceptance.yml
@@ -46,7 +46,7 @@
     name: tier-2 live AWS acceptance (Glue + S3 Tables, scratch-only)
     if: github.event_name == 'schedule' || inputs.leg == 'acceptance'
     runs-on: ubuntu-latest
-    timeout-minutes: 90
+    timeout-minutes: 120
     environment: aws-acceptance
     permissions:
       id-token: write   # OIDC exchange — scoped to this job only
@@ -81,6 +81,11 @@
           uv sync --locked
           (cd python/repark && uvx maturin@1.14.1 develop)
           VIRTUAL_ENV="$PWD/.venv" uv pip install --quiet dbt-core==1.9.11 dbt-spark==1.9.3
+          uv venv --python 3.12 .venv-previous
+          VIRTUAL_ENV="$PWD/.venv-previous" uv pip install --only-binary :all: --require-hashes \
+            -r python/repark/tests/previous_release_requirements.txt
+          .venv-previous/bin/python -c \
+            "import repark; assert repark.__version__ == '1.5.4', repark.__version__"
       # Credentials mint HERE, last, immediately before the only step that needs them.
       - uses: aws-actions/configure-aws-credentials@e6de054238d6b7531b4efff3b6587d9aade6a06c  # v6.2.3
         with:
@@ -115,6 +120,21 @@
         run: >
           VIRTUAL_ENV="$PWD/.venv"
           uv run --no-project python -m pytest python/dbt-repark/tests/test_aws_acceptance_gold.py -q -rA
+      - name: integrity legs (REPARK_AWS_ACCEPTANCE=1, ICE-INTEGRITY-PINS-1)
+        if: ${{ !cancelled() }}
+        env:
+          REPARK_AWS_ACCEPTANCE: "1"
+          REPARK_ACCEPT_BRONZE_BUCKET: ${{ vars.REPARK_ACCEPT_BRONZE_BUCKET }}
+          REPARK_ACCEPT_WAREHOUSE: ${{ vars.REPARK_ACCEPT_WAREHOUSE }}
+          TABLE_BUCKET_ARN: ${{ secrets.TABLE_BUCKET_ARN }}
+          REPARK_PREVIOUS_RELEASE_PYTHON: ${{ github.workspace }}/.venv-previous/bin/python
+        run: >
+          VIRTUAL_ENV="$PWD/.venv"
+          uv run --no-project python -m pytest
+          python/repark/tests/test_aws_acceptance_integrity_kill.py
+          python/repark/tests/test_aws_acceptance_commit_unknown.py
+          python/repark/tests/test_aws_acceptance_cross_release.py
+          -q -rA
 
   ice-read-perf-bench:
     name: ICE-READ-PERF bench (S3 Tables + Glue, dispatch-only, scratch-only)
```

No IAM change is expected: the legs call `UpdateTable`, `GetTable`, the S3 Tables table calls
and object reads the acceptance role already uses (unmeasured until the first run; a denial is
reported through `format_denial_failure` as today). The version literal in the import check moves
with Q3's answer.

## 7. Risks, how each is held deterministic, and the halts

| Risk | Held by |
|---|---|
| The kill lands at a different point each run (M1's races) | The local child cannot pass the hook: `update_table` waits forever, so the kill always finds it there. The parent waits for the marker under a 60 s deadline; a missed deadline fails the cell, never skips it. The live child is held by the gate the same way. |
| A held request hangs a session forever | The SDK has no default read timeout. Only leg K holds, and its parent kills the child under a deadline. Leg U never holds: a cut is a reset. |
| The fork reconciles for up to 30 minutes | Every drill table sets the four `commit.status-check.*` properties (§4.4); the leg asserts they are present before the statement and fails fast if not. |
| The SDK resends a commit whose response was lost (M3) | U-1 to U-4 reset the resend unforwarded, so they drill the fork alone. U-5 forwards it on purpose (H-U5). |
| S3 Tables answers an old metadata location after a commit (consistency unmeasured) | The resolver answers only after the gate has closed every connection that held the commit and two reads 2 s apart agree, up to a 60 s deadline; otherwise the cell fails as "unresolved". |
| S3 Tables maintenance commits between the pre-state and the reopen | "Unchanged" is "the attempted snapshot id is absent and the rows equal the pre-state", not "the same snapshot set". The fork's reconciliation searches the snapshot set, so a buried commit is still found. |
| The gate's second signature is refused (clock skew, a header it drops) | The gate's AWS-free unit test checks SigV4 against fixed vectors; the first credentialed run proves AWS accepts it (H-U1). |
| The previous release refuses or misreads a head-written feature (v3 defaults, row lineage, deletion vectors) | That is a finding, not flakiness: the cell fails with both versions' answers printed (H-X1). |
| Two nightly runs overlap | The workflow's concurrency group serializes runs; the bench job uses other table names. |
| T-1 moves `crates/repark-spark/src/tests/` | The two Rust pin files move with the binary; their names stay. |

**A second seam would be** any of: a door whose commit does not pass `Catalog::update_table` (the
CTAS staged path may commit through `create_table`; then the decorator holds that call too, and
the sketch names it as a second hook point in step 0); a commit the gate cannot recognise on one
catalog; a door that needs product code to be held. Each is a halt, not a design change inside
the round.

**Halts for the order.**

- H-K1: a door's commit passes neither `update_table` nor `create_table` of the session catalog.
- H-K2: a kill cell's reopen reads anything but the pre-state. A real defect; the owner hears first.
- H-U1: AWS refuses the gate's re-signed requests on the first credentialed run.
- H-U5: U-5 lands the rows twice, or leaves two snapshots carrying the id. A silent duplicate;
  the owner hears first, and the fix is a fork card (Q6).
- H-U2: a cell answers "unresolved" on two runs in a row.
- H-X1: a cross-release cell is not equal.
- H-X2: PyPI does not serve the pinned 1.5.4 wheel with the recorded hashes.
- H-6: any change to a product file, the fork, `Cargo.toml` or a workflow. The unit adds tests and
  records only; the workflow is the owner's (§6.5).

## 8. Questions

Each question has its premise and a lean. RULING: the orchestrator can rule. OWNER: the owner's.

- **Q1 (OWNER), a product hook for the kill point?** Premise: §3.2 reaches the exact point with
  no product code (a test catalog decorator locally, the gate live); a product fault point (K-E)
  is the only way to kill at that point through the released wheel without a gate, and it puts a
  test seam in the commit path. Lean: no product hook.
- **Q2 (OWNER), a transport seam for the unknown-outcome drill?** Premise: the gate (U-A) cuts the
  real network after the request leaves the SDK, with no product or fork change, and it is the
  only option that exercises the SDK's own resend; the fork's seams are `#[cfg(test)]` and its
  credentialed smoke never lets the SDK resend. Lean: no seam; the gate for the legs, the RePark
  test decorator for the offline twin. The fork lane may still run its own credentialed smoke to
  close row R157's residue; that is not U-9.
- **Q3 (OWNER), where the previous wheel comes from, and which version.** Premise: release
  wheels go to PyPI (`release.yml`); workflow artifacts expire; the plan names 1.5.4, written
  before 1.5.5 shipped on 2026-10-13. Lean: PyPI, installed by exact version with
  `--require-hashes` from a compiled requirements file, before credentials. Keep 1.5.4 for the
  1.5.6 label as the plan says, and add one line to the release runbook (publish-pypi) that moves
  the pin to the newest previous release at each tag.
- **Q4 (RULING), where the orphan count is reported.** Premise: RePark has no report surface;
  `remove_orphan_files` cannot list files younger than 24 hours (M2) and refuses on S3 Tables.
  Lean: the pins compute the count and print it as one line per cell (§3.3); locally it is checked
  against a dry run after the file times are set back (M2 measured the same 28), on Glue against a
  listing of the table prefix, on S3 Tables against the attempt's own file list. No product
  surface in U-9.
- **Q5 (RULING), the local catalog's commit point.** Premise: the memory catalog keeps its
  pointer in memory, so after a process death the newest metadata file is the commit: a kill
  after that file is written reads back as committed (M1, 2,000,003 rows). This matches the Hadoop
  catalog's rule. Lean: accept it as the local catalog's declared answer, pin it as control K-C1,
  and record it in the registry beside ICE-COMMIT-UNKNOWN-1.
- **Q6 (OWNER), if U-5 duplicates rows.** Premise: M3 measured three sends of a call whose
  response was lost, the SDK's retry classifier is operation-agnostic, and the fork configures no
  retry policy; the predicted path (a conflict on the resend, mapped retryable, re-applied) would
  append twice. Lean: a halt of the live leg and a fork card that sets the SDK's retry to one
  attempt on the two commit calls (`UpdateTable`, `UpdateTableMetadataLocation`), so the fork's
  classifier owns the outcome; the v2 label waits on it, since a duplicate append is a silent
  value. Ruling 84 widens this: the drill runs the sequence lost reply, SDK re-send, stale rejection, fork re-base (see the card ice-commit-rebase-dup-1).
- **Q7 (RULING), the plain `INSERT` raises with `operation_id = None`.** Premise: the fork mints
  and stamps an id (`fork:crates/integrations/datafusion/src/physical_plan/commit.rs:372`) but
  does not hand it to RePark; a user can then resolve only by the snapshot-set difference, sound
  only under the single-writer rule. Lean: U-9 pins the door as it is (U-2 asserts `None`) and
  the harness resolves by the gate's captured snapshot id; a separate card threads the fork's id
  to the error.
- **Q8 (OWNER), the workflow change and the first run.** Premise: tier-2 never runs unmerged code
  (AGENTS.md), so the legs merge skipped-by-default and their first live run is after the owner
  wires §6.5. Lean: wire §6.5 as written; the first dispatched run is the legs' proof, and a red
  is a fold pull request.
- **Q9 (RULING), S3 Tables maintenance on the drill tables.** Premise: the bench disables
  compaction with an AWS CLI step and a maintenance-configuration grant; the drill can instead
  define "unchanged" so a service commit does not matter (§7). Lean: the robust definition; no
  IAM change.
- **Q10 (RULING), how the gate signs again.** Premise: botocore is not in `uv.lock`; SigV4 is
  about 80 lines of the standard library (`hmac`, `hashlib`). Lean: the standard library, held by
  the fixed-vector unit test; no new dependency.

## Owner rulings (2026-10-15)

Questions 79 to 81, 84 and 86 were the owner's; 82, 83, 85, 87 and 88 were the orchestrator's provisional rulings, which the owner confirmed with the additions quoted below.

```
79. No. Nothing in the product exists for a test.
80. No. Same rule; the offline twin's hook stays test-only.
81. Yes. PyPI, exact version, hash-checked, installed before any credential
    is in the environment. Keep 1.5.4. One release-checklist line moves the
    pin at each tag.
84. Yes, and the card is wider than turning the SDK retry off. The fork
    already re-bases and re-appends after a CAS rejection
    (append.rs:1685). A lost reply, an SDK re-send, a stale rejection, and
    that re-base appends the same rows twice. The drill must run that
    exact sequence. The fork card: after any CAS rejection, refresh and
    check whether the table already holds our snapshot id before
    re-basing; then switch the SDK retry off on the two commit calls. If
    the drill shows a double append, the live leg stops and v2 waits.
86. Yes. Wire it as the plan writes it; I merge the .github change. The
    first run is the proof. One clerk PR carries U-9's step and U-10's AWS
    leg together.
82. Confirmed, with a ceiling: the count is asserted at most the files the
    crashed commit wrote, and the table's visible rows equal pre-crash.
83. Confirmed as the local catalog's rule only. The pin names it so; Glue
    and REST follow the catalog pointer, and the AWS drill pins that.
85. Confirmed. The error text still tells the user what to read back
    (the table's snapshot list). The operation id card is dated.
87. Confirmed. The changed check compares snapshot ids we wrote, never the
    metadata location, so a maintenance snapshot is tolerated. If
    maintenance fires mid-drill, retry once and log it.
88. Confirmed. Signer is test-only; the published AWS test vectors sit
    beside it and run in the offline twin. The moto venv's boto3 is not a
    dependency, so the hand-back cross-checks one signed request against
    it once and records the match.
```

- 79 answers Q1: §3.2 keeps K-E rejected, so the kill point needs no product hook.
- 80 answers Q2: §4.5 keeps the offline twin's hook test-only; the legs keep the gate.
- 81 answers Q3: §5.2 and §6.5 keep the PyPI exact-version hash-checked install before credentials, and keep 1.5.4.
- 84 answers Q6: §4.4 cell U-5, §7 H-U5 and Q6 now drill the full lost-reply sequence, and the fork card checks for our snapshot id before re-basing.
- 86 answers Q8: §6.5 is wired as written, and one clerk pull request carries U-9's step with U-10's AWS leg.
- 82 touches Q4: §3.3 item 3 asserts the orphan count at most the crashed commit's files, with visible rows equal to pre-crash.
- 83 touches Q5: §3.1 and the §3.3 controls keep the newest-metadata-file rule as the local catalog's only.
- 85 touches Q7: §4.3 keeps the snapshot-list read-back in the error text, and the operation id card is dated.
- 87 touches Q9: §4.1 and §4.3 compare snapshot ids the drill wrote, never the metadata location, and retry once on mid-drill maintenance.
- 88 answers Q10: §4.2 and §6.1 keep the signer test-only with the published vectors beside it, and the moto cross-check happens once in the hand-back.

## 9. The order's outline (grade A; the runs as a grade-C card)

**Grade:** A for the build, to an Opus 5.5 high executor: the order carries the cells, the gates
and the halts, and the decisions are the answers to §8. The live runs are a grade-C run card for
Muse xhigh (dispatch the workflow, paste the outputs, change nothing). One fresh Opus 5.5 high
verifier, scoped to the cells, the mutants, the gate's scratch refusal and the first run's output.
**Release:** 1.5.6. Steps, in the shape of the U-6 order's §4:

0. **Measure, no test code** (Muse xhigh, local only). (a) For each door of §3.3, which session
   catalog call commits it, with a throwaway decorator (H-K1), and K-7's state after a kill;
   (b) the 1.5.4 wheel's hashes from PyPI through `uv pip compile --generate-hashes` (a read) and
   M5 again with the real wheel, X-NS recorded; (c) the gate's SigV4 against the published AWS
   test vectors; (d) the Rust child's start time, as the bar for §6.4. Commit `step-0.md` beside
   the order. Any halt ends the round here.
1. **The Rust pins** (Opus 5.5 high). `kill_mid_write.rs` and `commit_unknown_doors.rs`. Main is
   expected to answer every cell as §3.3 and §4.4 say, so the red proof is the mutants of step 4,
   run before this commit and pasted into it. Commit.
2. **The gate, the child, the runner** (Opus 5.5 high). The three helpers, their AWS-free unit
   test, the requirements file. Commit.
3. **The three legs** (Opus 5.5 high), skipped without `REPARK_AWS_ACCEPTANCE=1`. Commit.
4. **Proof** (Opus 5.5 high). Mutants, each turning a named cell red: the hook forwards before it
   holds (K cells read committed rows); the orphan count skips manifests (K (3) and (4)); the gate
   forwards in U-3 (U-3 resolves committed); the resolver answers "not committed" on a failed read
   (the unresolved check); the status-check properties dropped (the cut-read cells miss their
   deadline); the runner runs the head as the previous release (X-C1); the comparison ignores
   types (X cells on `DECIMAL` and `TIMESTAMP_NTZ`). Local run times. CI green. Then the pull
   request. After merge and the owner's wiring (Q8), the run card dispatches the workflow; a red
   cell is a fold.
5. **Records** (Opus 5.5 high). Registry row ICE-COMMIT-UNKNOWN-1 gains the live pins and the run
   id; production status row K3 moves to "drilled live" with the run id; a registry note for the
   local catalog's commit point (Q5) and, if ruled, for the plain `INSERT`'s `None` (Q7);
   `STATUS.md`; the unit ledger `task/ledgers/staging/ice-integrity-pins-1-ledger.md` with one
   clause per cell family; every map row. Commit.

Gates of the order: `cargo test -p repark-spark kill_mid_write` and `commit_unknown_doors`;
`make verify`; `make preflight` (the gate's unit test in the facade suite);
`python3 scripts/check_rust_file_size.py`; `make check-docs-links check-docs-compaction
check-ledger-grammar`; `make spell-check`; `python3 scripts/sync_map_md.py --check`;
`bash scripts/check_map_md.sh --base origin/main`. One cargo build at a time,
`CARGO_BUILD_JOBS=6`; probes under `ulimit -v 67108864`, in the foreground.

## 10. Not measured in this round

The real 1.5.4 wheel (PyPI was not read); AWS accepting a re-signed request; the S3 Tables
commit path as the gate sees it; the SDK's resend of a commit call (only reads were measured, M3)
and U-5's outcome; S3 Tables consistency right after a commit; the IAM coverage of the legs;
every run time in §6.4; the Rust child's start time; K-7's expected state; whether every door
commits through `update_table` on the memory catalog (H-K1); delete files, deletion vectors and
schema evolution across releases; the X-NS cell; the Glue prefix listing through
`pyarrow.fs`; adopting an S3 Tables metadata file into a local memory-catalog session to count
the orphans.
