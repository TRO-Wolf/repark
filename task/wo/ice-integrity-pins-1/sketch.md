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
