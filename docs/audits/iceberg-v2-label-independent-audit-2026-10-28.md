# Independent audit: RePark format-v2 Iceberg PRODUCTION label

## 1. Header and audit boundaries

**Date:** 2026-10-28. **Mode:** findings only; fresh scratch clones, local probes, source review and read-only CI evidence retrieval. **Auditor:** Codex, assisted by two `gpt-6.1-sol` investigators. The primary runtime's exact model identifier is not exposed. **Elapsed time:** 0.34 hours wall time (20.2 minutes), 10:36:14–10:56:28 UTC; approximately 06:36–06:56 America/New_York. This is elapsed audit time, not summed agent billing.

| Repository | Audited revision |
|---|---|
| [TRO-Wolf/repark](https://github.com/TRO-Wolf/repark) | `321c6095ec92391fe71f3c23ce2f60ece0daed2c`, `main` when cloned |
| [TRO-Wolf/iceberg-rust](https://github.com/TRO-Wolf/iceberg-rust) | `ea0b7741a57eb81ff25ee9dc18487d641b435a1b`, both RePark's five dependency pins and fork `main` when cloned |
| Label's recorded matrix revision | `105d4625883fa10639c938a99cf411b9e1d26647` |
| RustSec database used locally | `40f5be6fb12ed93fc591bf4ab23b7d793b3f2239` |

Scratch root: `the audit scratch directory`. No checkout under `the owner's local checkout tree` was opened or modified. No tracked source, manifest, lockfile, workflow or policy was edited. Two untracked Rust probes were added to scratch clones; Python probes and reports live beside them. No commit, push, PR, AWS leg or credential-bearing operation was performed.

I read the production definition, maturity statement, label order and relevant registry sections, and recorded independent hypotheses **before** reading unit ledgers. The hypotheses concerned commit outcomes, evolving delete semantics, retained memory, reader freshness, failure-state visibility and the scope of exemptions. The fork investigator independently followed the same ordering. The other investigator provided source leads but stopped before a final report; those leads were not accepted as findings without coordinator reproduction.

The native module was built from the audited RePark revision with its unchanged lockfile, using Rust 1.96.0 and `maturin develop --locked` in the dev profile. Its package version reports 1.5.5; the **source SHA**, not that version string, identifies this build. These results do not independently certify the distributed release wheels.

Every build/test used this prefix on the owner's machine:

```sh
systemd-run --user --scope -p MemoryMax=40G --same-dir --quiet \
  taskset -c 0-25 env CARGO_BUILD_JOBS=8 RUST_TEST_THREADS=8
```

No Spark executable or PySpark installation was present in the inspected execution environment. No Spark JVM or live AWS leg ran. Expectations below come from the production definition, explicit Iceberg requirements, Java Iceberg 1.11.0 source, and controls; none is represented as a fresh Spark 4.1.2 measurement.

### Execution inventory

| Execution | Result |
|---|---|
| Locked Python environment setup; exact-head native build | Success; native build 4m40s |
| Public Hadoop stale-hint probe | Deleted row returned by path reader; catalog/latest-metadata controls correct |
| Public Parquet/Avro/ORC MoR DELETE probe | Parquet succeeds end to end; Avro and ORC commit, then fail on read |
| Fork equality-delete/schema-drop probe | **Failed**, exit 101; one test, product returns `Unexpected => field not found` |
| RePark post-publication refresh probe | **Failed**, exit 101; CTAS returns an ordinary error although its table has one committed row |
| Cross-read classification probe | A simulated connection failure is accepted for a nanosecond-bearing table; non-nanosecond control rejected |
| Existing path-read and resource-bound tests | **72 passed, 18 deselected**, 79.34s; scale cells deliberately excluded |
| Fork interop driver sabotage self-test | Passed all driver-mechanics checks; no Java suites executed locally |
| RePark configured `cargo audit --json` | **Failed**, exit 1, on newly observed `RUSTSEC-2026-0414` |
| Same lockfile audited outside repository configuration | **Failed**, exit 1; exposes the three ignored zstd advisories as well as pre-existing quick-xml entries and 0414 |

The first standalone cross-read probe stopped at import setup (`ModuleNotFoundError: _acceptance`), before exercising the classifier. Its log is preserved. Supplying the repository's test-helper directory through `PYTHONPATH` allowed the actual probe to execute. No product-test failure was rerun to obtain a green result. Small probe programs print observations and exit zero when observation succeeds; that exit code does not mean the product behavior was correct.

## 2. Verdict on the label

**The label should be withdrawn until the stale-Hadoop-hint gap closes (AUD-001), and the additional supported-path failures below are fixed or explicitly excluded through an enforceable operating envelope.** AUD-001 reproduces a silent wrong result after a committed DELETE, which conflicts with the label order's own rule that a silent value disqualifies and cannot be covered by a carve-out. The existing declaration establishes that the gap was known; it does not make the returned rows correct.

This is a recommendation about the present broad **format-v2 reads and writes: PRODUCTION** claim, not an assertion that every V2 workload is unsafe. A smaller qualified claim would require a precise supported workload list, entry points and enforced exclusions; adding more exception prose to the current claim is insufficient. I have not changed the label.

## 3. Reproduced findings

### AUD-001 — A stale Hadoop hint silently returns a row already deleted

**Severity: Critical — silent wrong result. Repository: RePark.** This is logical row resurrection in a read, not physical restoration of data files.

**Source:** [`crates/repark-core/src/iceberg_path.rs:99–106`](https://github.com/TRO-Wolf/repark/blob/321c6095ec92391fe71f3c23ce2f60ece0daed2c/crates/repark-core/src/iceberg_path.rs#L99). When the hinted file exists, resolution returns it immediately without checking the next committed Hadoop metadata version.

**Ordinary entry:** `session.read.format("iceberg").load(table_location).to_arrow()`. The precondition is a stale hint after publication of the next complete metadata file. The probe models that documented failure window by restoring the previous hint after a successful DELETE; it does **not** claim to have killed a process at that window.

**Reproduction:** Appendix A, `probe_public_paths.py`. Create a V2 Hadoop table, insert ids 1,2,3, commit `DELETE FROM hd.ns.t WHERE id = 1`, then restore hint `2` while complete `v3.metadata.json` remains. Execute the path read, a catalog read and a direct read of v3 metadata.

```text
old_hint=2; new_hint=3
current catalog rows:       [{"id":2},{"id":3}]
direct latest metadata:     [{"id":2},{"id":3}]
path read with stale hint:  [{"id":1},{"id":2},{"id":3}]
catalog after same fault:   [{"id":2},{"id":3}]
metadata files: v1.metadata.json, v2.metadata.json, v3.metadata.json
```

**Expected:** the table-location read discovers the later committed version and returns ids 2,3; it must not quietly serve the earlier state as current. Java Iceberg 1.11.0 [`HadoopTableOperations.refresh`, lines 95–116](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/hadoop/HadoopTableOperations.java#L95) walks successive versions beyond the initial hint. The local production definition requires correct documented behavior and protection during failures.

**Already named:** yes. Registry `ICE-LOCAL-COMMIT-POINT-1`, dated Hadoop-window paragraph; label ledger C-028; card `ICE-HADOOP-HINT-WINDOW-1`. The independent result establishes the concrete consequence: a deleted row is returned without an error. This finding is not a new discovery of the window and is not evidence against catalog reads or explicit historical metadata reads.

### AUD-002 — CTAS reports failure after successfully publishing its table

**Severity: Medium — fail-loud outcome-reporting gap. Repository: RePark.** This is an outcome-reporting/failed-write guarantee violation. The table was fully published before the injected listing error. Under the brief’s strict rubric this is Medium: no partial snapshot, library panic, row resurrection or data loss was demonstrated.

**Source:** [`crates/repark-spark/src/ctas.rs:312–324`](https://github.com/TRO-Wolf/repark/blob/321c6095ec92391fe71f3c23ce2f60ece0daed2c/crates/repark-spark/src/ctas.rs#L312) finishes the staged commit, then propagates a failure from `reregister` as the statement error. The refresh path in [`crates/repark-iceberg/src/catalog/provider.rs:241–254`](https://github.com/TRO-Wolf/repark/blob/321c6095ec92391fe71f3c23ce2f60ece0daed2c/crates/repark-iceberg/src/catalog/provider.rs#L241) performs fallible catalog operations after publication.

**Ordinary statement through the public Rust session:**

```sql
CREATE TABLE fault.ns.t USING iceberg
TBLPROPERTIES ('format-version'='2')
AS SELECT CAST(7 AS INT) AS id;
```

**Failure schedule:** a local `Catalog` wrapper forwards every operation to the real fork MemoryCatalog. Immediately after `publish_create_table` succeeds, it makes `list_tables` return a deterministic `Unexpected` error. No storage method, transaction logic or product source is patched. A separate call through the underlying catalog then loads and scans the published table.

**Reproduction:** Appendix E, untracked `crates/repark-spark/tests/audit_postpublish.rs`. First and only execution, exit **101**:

```text
CTAS_RESULT=Err("Unexpected => AUDIT: catalog listing unavailable after publication")
PUBLISHED_ROWS=1; CURRENT_SNAPSHOT=true
CTAS returned failure despite a committed table:
Err("Unexpected => AUDIT: catalog listing unavailable after publication"); rows=1
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured;
0 filtered out; finished in 0.05s
```

The product error does not identify a committed outcome or an unresolved commit state. The test verifies publication, a current snapshot and one readable row; it does not assert the returned cell's value. The panic shown by the Rust test runner is the probe assertion, **not** a library panic.

**Expected:** a failed write must not be reported as though its effects are absent when publication already succeeded. The API must communicate the committed outcome accurately, including any refresh problem; the production definition's data-integrity rule requires the last committed state to remain intact after a failed write. A subsequent caller retry must not depend on knowing this hidden post-publication stage. The source sequence means an ordinary fallible catalog refresh is sufficient to trigger the issue; this audit simulated it locally rather than causing a cloud outage.

**Already named:** the label records unknown-commit transport drills and local kill points, but no explicit post-publication provider-refresh exception was found in its order, ledger or reviewed registry entries. This is distinct from the declared unpinned replace kill doors. The same source path handles replace operations, but **only CTAS was reproduced** here.



### AUD-003 — V2 MoR DELETE writes Avro/ORC deletes that the next read cannot consume

**Severity: Medium — fail-loud gap on a documented surface. Repositories: fork, reached through RePark.** No silent corruption or physical data loss was observed.

**Source:** fork [`crates/iceberg/src/arrow/caching_delete_file_loader.rs:265–271`](https://github.com/TRO-Wolf/iceberg-rust/blob/ea0b7741a57eb81ff25ee9dc18487d641b435a1b/crates/iceberg/src/arrow/caching_delete_file_loader.rs#L265) opens non-Puffin position deletes with the Parquet-only loader. The writer at [`crates/integrations/datafusion/src/physical_plan/delete_position_deletes.rs:328`](https://github.com/TRO-Wolf/iceberg-rust/blob/ea0b7741a57eb81ff25ee9dc18487d641b435a1b/crates/integrations/datafusion/src/physical_plan/delete_position_deletes.rs#L328) selects the requested file format.

**Ordinary statements:** for each `FORMAT` in `parquet`, `avro`, `orc`, on a fresh local table:

```sql
CREATE TABLE mem.ns.t (id INT) USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.format.default'='FORMAT',
               'write.delete.mode'='merge-on-read');
INSERT INTO mem.ns.t VALUES (0),(1),(2),(3),(4);
SELECT id FROM mem.ns.t ORDER BY id;
DELETE FROM mem.ns.t WHERE id BETWEEN 1 AND 3;
SELECT id FROM mem.ns.t ORDER BY id;
```

**Reproduction:** Appendix B, `probe_delete_formats.py`, executed against the freshly built Python facade.

| Format | Read before DELETE | DELETE | Read after DELETE |
|---|---|---|---|
| Parquet | 0,1,2,3,4 | success | 0,4 |
| Avro | 0,1,2,3,4 | success | Parquet footer error |
| ORC | 0,1,2,3,4 | success | Parquet footer error |

Exact failure on both non-Parquet cases:

```text
PySparkException: External error: Unexpected => Failed to load Parquet metadata,
source: Parquet error: Invalid Parquet file. Corrupt footer
```

**Expected:** a supported successful DELETE leaves a readable table containing ids 0,4. If this delete format is unsupported, the statement must refuse before publication. The error text does not establish that the Avro/ORC file itself is corrupt: the reader is applying the wrong decoder.

**Already named:** no matching delete-reader-format exception found in the reviewed records. RePark's registry `ICE-WRITE-OPTIONS-ORC-AVRO` says SERVED. Fork GAP_MATRIX R118 explicitly says ORC/Avro writing landed on the V2 position-delete door; its listed residues concern other interop/type/codec work. Existing fork `crates/integrations/datafusion/tests/mor_delete_format.rs:364–442` checks suffix/magic after writing the deletes, but not a subsequent row read. R117's Parquet evidence does not close that combination.

### AUD-004 — Dropping an equality-delete key makes a valid V2 table unreadable

**Severity: Medium — fail-loud schema-evolution gap. Repository: fork.** The failure was reproduced through the public Rust APIs; the equivalent RePark SQL sequence was not separately executed.

**Source:** [`crates/iceberg/src/arrow/caching_delete_file_loader.rs:305–314`](https://github.com/TRO-Wolf/iceberg-rust/blob/ea0b7741a57eb81ff25ee9dc18487d641b435a1b/crates/iceberg/src/arrow/caching_delete_file_loader.rs#L305) evolves equality-delete keys against the current schema; [`arrow/delete_file_loader.rs:176–186`](https://github.com/TRO-Wolf/iceberg-rust/blob/ea0b7741a57eb81ff25ee9dc18487d641b435a1b/crates/iceberg/src/arrow/delete_file_loader.rs#L176) passes that schema and the historical equality ids to `RecordBatchTransformer`.

**Calls and data:** create V2 `{id: long, data: string}`; append `(10,a),(20,b),(30,c),(40,d),(50,e)`; commit an equality delete for ids 20 and 40. A baseline scan successfully returns three rows. Commit `Transaction::update_schema().delete_column("id")`, then scan the remaining `data` column.

**Reproduction:** Appendix D, the untracked `crates/iceberg/tests/audit_dropped_eq_key.rs`, using production data/equality writers, transactions and Arrow scanner. First and only run:

```text
POST_DROP_RESULT=Err(Unexpected => field not found
)
dropping equality-delete key must preserve readable surviving data:
Unexpected => field not found
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured;
0 filtered out; finished in 0.05s
exit: 101
```

The displayed panic is the probe's `expect` assertion. **The library returned an error; this is not a demonstrated library panic.**

**Expected:** scan returns `a,c,e`, using the historical delete-key field to apply deletes even though it is absent from the current projection/schema. The [Iceberg specification, Equality Delete Files](https://iceberg.apache.org/spec/#equality-delete-files), explicitly requires continued use of dropped equality-delete columns. No fresh Spark oracle is needed to infer that explicit format requirement.

**Already named:** no matching dropped-key exception located in the reviewed fork/RePark registry entries. General schema-evolution and equality-delete coverage do not test this sequence.

### AUD-005 — Cross-read gate accepts a connection failure as an expected nanosecond limitation

**Severity: Low under the brief's scale — reproduced gate defect, not a reproduced product wrong result. Repository: RePark.** It affects shared cross-read assurance, particularly the experimental/V3 nanosecond surface; it is not proof that ordinary V2 cross-read runs failed unnoticed.

**Source:** [`python/repark/tests/parity/_acceptance_cross_read.py:298–314`](https://github.com/TRO-Wolf/repark/blob/321c6095ec92391fe71f3c23ce2f60ece0daed2c/python/repark/tests/parity/_acceptance_cross_read.py#L298) classifies every Spark exception as `SPARK-CANNOT-READ`; [`:376–384`](https://github.com/TRO-Wolf/repark/blob/321c6095ec92391fe71f3c23ce2f60ece0daed2c/python/repark/tests/parity/_acceptance_cross_read.py#L376) accepts that outcome whenever `has_ns_columns` is true, without checking the exception against a known unsupported-type condition.

**Reproduction:** Appendix C calls the actual helper with an in-memory Arrow nanosecond table and a local stand-in that raises `ConnectionError`. No real connection is attempted.

```json
{"gate":"accepted","status":"SPARK-CANNOT-READ","error_class":"ConnectionError","error":"simulated oracle connection unavailable","has_ns_columns":true,"spark_row_count":null}
{"control":"non_ns_connection_failure_rejected"}
```

**Expected:** connection, authorization, catalog initialization and malformed-file failures must fail verification independently of timestamp precision. Only a specifically established unsupported-type condition may be accepted. This follows the helper's own “expected nanosecond” contract, the label ledger C-010's catalog-init exclusion, and the production definition's evidence requirements.

**Already named:** the intended nanosecond exception is named; this broad acceptance condition is not equivalent to that exception. No claim is made that a cited successful CI run actually encountered the simulated failure.

## 4. Leads not reproduced

These have **no severity rating** and do not become findings by resemblance to past incidents.

| Lead | Evidence inspected | What would prove or dismiss it |
|---|---|---|
| Remaining position-delete overflow routes | Registry POSDEL-BATCH-1 describes the unbatched RePark BinaryOp writer and subquery route; RP-POSDEL-BATCH-2 and equality conversion are known pending work | Actual matched path-text total beyond Arrow's 32-bit limit through each SQL route, with pre/post snapshots and rows. A small batch-property test is not a reproduction of the volume panic. |
| Batching leaves retained path memory unbounded | Fork `delete_position_deletes.rs:168–219` clones/groups collected pairs before Arrow chunking | Measure peak RSS, pool usage and outcomes over increasing row count and path length. Confirm whether the stated envelope refuses before exhausting process memory. |
| Many small equality-delete files form a deep predicate tree | Fork `arrow/delete_filter.rs:577–595` folds file predicates; balancing within one file does not establish balancing across files | Grow file count at constant total rows; scan with normal stack settings. Record any stack failure once. |
| Slice-aware accounting can understate retained allocation | RePark `crates/repark-iceberg/src/write/resource_bound.rs:221–228` counts logical slice bytes; sliced arrays may retain larger parent buffers | Produce such slices through a public write source, measure retained allocations/RSS and the actual refusal threshold. Do not equate logical-size arithmetic alone with a demonstrated OOM. |
| Concurrent statement/partition amplification | The bound record describes per-statement allowances beside the shared pool, not a global reservation for all retained input | Barrier-synchronized writes at controlled concurrency, total process memory and exact committed rows; distinguish supported single-writer scope from multiple writers to one table. |
| Unmeasured error-after-commit paths beyond the tested create | CTAS/RTAS and other mutation paths refresh providers after committing | Repeat the failure schedule per operation/catalog and assert the caller's outcome classification, all snapshot rows and safe retry behavior. |
| Definite failure orphans and partial metadata writes | Fork commit path conservatively retains staged artifacts on errors; local recovery uses metadata files as authority | Inject write/close/rename/list failures and repeat definite failures; measure file growth and reopened table state. Retaining files on an unknown commit is required for safety, not itself a defect. |
| Broader carve-out blast radius | Text-to-nested-timestamp leniency and keyed-handle/replace exceptions are recorded | Execute the full type/container/entry-point census, including map-key collisions, nulls and changed properties. This audit did not rerun the 102-cell text census or race encryption updates. |
| Long-lived session/cache behavior | Existing source and cache tests distinguish metadata handles and schema providers | Compare new statements, retained plans, fresh sessions and external commits under the documented snapshot contract; a deliberately pinned historical plan is not automatically stale-read misconduct. |

No fresh exploit was demonstrated for the dependency advisories. Their version, feature and call-site evidence is separated below from product findings.

## 5. Gate dimensions absent or insufficient for the next label

For the format-v3 label with date of record **2026-11-17**, apply these dimensions to a declared finite workload set. A row needs its actual result and exact revision, not only a checked assertion or successful upstream run.

| Dimension | Required discriminating evidence |
|---|---|
| Public routing | Equivalent predicates (`BETWEEN`, comparisons, `IN`, subquery), native SQL, Spark SQL and writer APIs; verify which writer actually executes. |
| File format composition | Parquet/Avro/ORC data **and delete** combinations; read exact surviving values after DML and reopen, not just suffix/magic. |
| Evolution history | Equality deletes followed by drop/rename/promotion/re-add with a new id; partition evolution; V2→V3 upgrade with old deletes retained. |
| Volume boundaries | Row count, total UTF-8 path bytes, maximum value width, data/delete/manifest count, manifests per snapshot and metadata history length. Cross the real boundary, not only a smaller configured row limit. |
| Resource ownership | Pool bytes versus total RSS, disk, open handles, live encoders and retained Arrow buffers; multiplied partitions and simultaneous statements. |
| Commit outcome | Definite failure, proved success and unresolved outcome; also failure **after** catalog publication but before returning to the caller. |
| Failure schedule | Data writes, writer close, manifest/list/metadata creation, publication, hint write, provider refresh and reopen; truncated files and failed reads as distinct cases. |
| Concurrency | Two writers, maintenance under a writer, readers across commits, schema/partition changes while a write is planned; exact outcomes and no lost acknowledged rows. |
| Reader authority | Catalog pointer, Hadoop current-version walk, explicit historical metadata, cached plans and fresh sessions; no accidental mixing of those contracts. |
| Retention and recovery | Reachability of every retained snapshot/ref after failure and cleanup; no deletion of possibly committed files; bounded or operationally managed orphan growth. |
| Independent oracle | Required row/schema comparisons must execute. Infrastructure failures cannot become an allowed unsupported-feature result. |
| Exception boundaries | For each carve-out, a minimal trigger, affected entry points and proof that neighboring supported cases remain safe. Silent wrong results cannot be excused under a definition that forbids them. |
| Refusal guidance | Error class and actionable hint; no blind destructive remediation; assertions on persisted state after a refusal. |
| Runtime/platform/artifact | Packaged-wheel behavior, supported platforms and memory detection, cross-release reads/writes, and fault behavior after a real process restart. |
| Evidence freshness | Bind suite selection, dependency resolution and results to the exact candidate revision; distinguish weekly interop, nightly product acceptance and manual-only scale runs. |

The present 842-cell statement matrix does not establish these products of dimensions. It remains valuable statement-level evidence. Its **705/132/5/0** tally was read from the signed records, not independently recomputed in this audit. The resource scale workflow is `workflow_dispatch` only at this revision; it is not a scheduled regression guarantee.

## 6. What looks sound

1. **Existing tested behavior still works.** The exact-head `test_iceberg_load_path.py` and non-scale `test_ice_resource_bound_1.py` selection passed 72 tests. This checks the implemented normal path-read behavior and bounded-path controls/refusals; it does not override AUD-001's missing stale-hint case.
2. **The new fork chunking is structurally careful.** Source inspection found whole `(path, position)` pairs retained across chunk boundaries, no empty chunk for empty input, and an oversized-single-path guard before Arrow construction. This is source evidence, not a locally rerun past-2-GiB benchmark.
3. **Unknown commits are treated conservatively in the reviewed transaction path.** Fork `transaction/mod.rs` excludes `CommitStateUnknown` from retry and retains uncertainty when readback cannot prove a landing. Retaining staged artifacts in this state avoids deleting potentially live data. This audit did not execute live transport ambiguity.
4. **The independent format probe has a meaningful control.** The identical Parquet DELETE returns the correct survivors, while both Avro and ORC read correctly before the mutation. The failure is not explained by a generally broken local catalog or initial data reader.
5. **The fork's interop driver fails closed on its mechanics.** Local `--selftest` passed missing-prerequisite, failing-suite, empty discovery/run and subset-reporting checks. The retrieved 2026-10-24 run reports **65 passed, 0 failed, 65 run, 65 discovered**.
6. **Recent recorded CI success is real at the run-summary level.** Read-only GitHub checks found eight consecutive successful parity-live runs, October 21–28, and successful AWS acceptance October 26–28. The latest two workflows ran on `57e37a724707aea9fcf93ca322dafc5b6831918f`, not this audit's exact head. I did not replay their cloud assertions or equate success with coverage of the new findings.

Useful run links: [parity-live October 28](https://github.com/TRO-Wolf/repark/actions/runs/40649661343), [AWS acceptance October 28](https://github.com/TRO-Wolf/repark/actions/runs/40658998480), [label-cited acceptance October 26](https://github.com/TRO-Wolf/repark/actions/runs/40295982749), [fork interop October 24](https://github.com/TRO-Wolf/iceberg-rust/actions/runs/40072218692).

## 7. Fork pin assessment

### Verdict on the pin

**The pin is not sufficient evidence for the unrestricted V2 production label.** AUD-003 and AUD-004 reproduce supported sequences that the pinned fork cannot read after their successful mutations. Neither finding implies all Parquet-only workloads fail, and neither demonstrates silent corruption.

The pin in RePark and fork main matched exactly when cloned. The locally built RePark module consumed that unchanged revision. A dependency pin identifies source; it does not identify a shared resolved dependency graph: the two lockfiles differ.

### Findings and leads

The fork's reproduced findings are **AUD-003** (position-delete file format dispatch) and **AUD-004** (historical equality key after schema drop), with reproductions above and complete probe sources below. The fork's unrated leads are retained path memory, depth across many equality-delete files, error artifacts, and additional evolution/concurrency/failure schedules in section 4. No unsafe cleanup of possibly committed files was found in the reviewed transaction retry/reconciliation path; that is a bounded source-review result.

### What the interop workflow proves

Despite its historical filename/name, [`nightly_interop.yml`](https://github.com/TRO-Wolf/iceberg-rust/blob/ea0b7741a57eb81ff25ee9dc18487d641b435a1b/.github/workflows/nightly_interop.yml) is **weekly**, Saturdays at 06:43 UTC, plus manual dispatch. It invokes the full discovered `run-interop-*.sh` set, with a discovery floor of 65; it does not silently select a smaller subset. `run.sh` and `run-inspection-manifests.sh` are explicitly outside that discovery pattern.

The latest successful retrieved run, October 24, tested `d80a2884316e7a6fb457c29799be923bbedf977c`, preceding the current pin. Its 65 passing suites are real compatibility evidence for their fixtures. They do not establish arbitrary scale, dropped equality keys, all data/delete file format pairings, RePark's routing, or current-pin production qualification. No full Java interop run was attempted locally.

### RustSec: graph presence is different from vulnerable-call reachability

The [October 28 fork audit](https://github.com/TRO-Wolf/iceberg-rust/actions/runs/40609014133) failed on all four IDs in the brief, at the audited fork SHA. RePark's configured policy explicitly ignores the first three; that explains why its earlier green audit is not evidence that those affected package versions are absent.

| Advisory | Fork lock | RePark lock and resolved engine graph | Reachability conclusion from this audit |
|---|---|---|---|
| [RUSTSEC-2026-0391](https://rustsec.org/advisories/RUSTSEC-2026-0391.html) | `zstd 0.13.3` | Same version, reachable dependency; ignored by policy | Vulnerable API involves abandoned streaming contexts reused through `with_context`. Reviewed direct sites use fresh contexts or bulk APIs; no affected end-to-end call reproduced. Broader absence is not certified. |
| [RUSTSEC-2026-0398](https://rustsec.org/advisories/RUSTSEC-2026-0398.html) | `zstd-safe 7.2.4` | Same version, reachable dependency; ignored by policy | Resolved RePark features do **not** enable `seekable`, the advisory's required feature. No applicable call shown in this build. |
| [RUSTSEC-2026-0403](https://rustsec.org/advisories/RUSTSEC-2026-0403.html) | `zstd-safe 7.2.4` | Same version; `legacy` enabled; ignored by policy | Requires the affected failed-stream/output-buffer behavior. Reviewed Parquet/Arrow IPC use bulk APIs; fork compression uses `decode_all`, which the advisory distinguishes as unaffected. Other streaming integration requires continued analysis; no exploit reproduced. |
| [RUSTSEC-2026-0410](https://rustsec.org/advisories/RUSTSEC-2026-0410.html) | `zstd-sys 2.0.16+zstd.1.5.7` | `zstd-sys 2.1.2+zstd.1.5.7` | RePark resolves the patched version. Advisory is additionally wasm32-specific; this audit built Linux x86_64. |

`cargo metadata --locked --format-version 1` showed zstd under the actual `repark-python` normal/build dependency closure, through components including Parquet, Arrow IPC, DataFusion, Apache Avro, ORC and the fork. Resolved zstd-safe features were `arrays`, `legacy`, `std`, `zdict_builder`; zstd-sys features were `legacy`, `std`, `zdict_builder`. This establishes package/feature presence, not exploitability.

**Fresh local audit change:** configured `cargo audit` no longer passed at audit time. It reported **RUSTSEC-2026-0414, pulldown-cmark 0.13.4**. The lockfile names it through `prost-build`; it was absent from the inspected active native dependency closure. Its advisory concerns `ENABLE_WIKILINKS`; this audit did not establish that affected feature/API in RePark. Treat this as a current dependency-gate result, not a newly proved Iceberg runtime defect. The earlier green CI run predates the advisory database revision used here.

Policy sources: RePark `.cargo/audit.toml:21–33` and `deny.toml:35–56`. Lockfile SHA-256 values: RePark `1312b188c1d1d2bd3998dedcf7c51b52bb85c96b377f060ad134a9556f86a63e`; fork `461efca98f8a11dfb000c9ecb4b60beaf85e87fe1e4d43f84b95591bbe077b1c`.

## Reproduction commands and artifact retention

All paths below belong to this audit. Probe sources are embedded in the appendices so this report remains useful if the scratch directory is later removed.

```sh
cd the audit scratch directory
systemd-run --user --scope -p MemoryMax=40G --same-dir --quiet taskset -c 0-25 env CARGO_BUILD_JOBS=8 RUST_TEST_THREADS=8 repark/.venv/bin/python probe_public_paths.py
systemd-run --user --scope -p MemoryMax=40G --same-dir --quiet taskset -c 0-25 env CARGO_BUILD_JOBS=8 RUST_TEST_THREADS=8 repark/.venv/bin/python probe_delete_formats.py
systemd-run --user --scope -p MemoryMax=40G --same-dir --quiet taskset -c 0-25 env CARGO_BUILD_JOBS=8 RUST_TEST_THREADS=8 PYTHONPATH=the audit scratch directory/repark/python/repark/tests repark/.venv/bin/python probe_cross_read_gate.py

cd the audit scratch directory/fork
systemd-run --user --scope -p MemoryMax=40G --same-dir --quiet taskset -c 0-25 env CARGO_BUILD_JOBS=8 RUST_TEST_THREADS=8 CARGO_TARGET_DIR=the audit scratch directory/repark/target cargo test --locked -p iceberg --test audit_dropped_eq_key -- --nocapture

cd the audit scratch directory/repark
systemd-run --user --scope -p MemoryMax=40G --same-dir --quiet taskset -c 0-25 env CARGO_BUILD_JOBS=8 RUST_TEST_THREADS=8 cargo test --locked -p repark-spark --test audit_postpublish -- --nocapture
systemd-run --user --scope -p MemoryMax=40G --same-dir --quiet taskset -c 0-25 env CARGO_BUILD_JOBS=8 RUST_TEST_THREADS=8 PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest python/repark/tests/iceberg/test_iceberg_load_path.py python/repark/tests/writes/test_ice_resource_bound_1.py -q -k 'not scale'
```

The Python environment was provisioned with `uv sync --locked --extra numpy --extra pandas --extra polars --no-install-package repark`. From `repark/python/repark`, the capped build command invoked the scratch `.venv/bin/maturin develop --locked` with `VIRTUAL_ENV` set to that same scratch virtual environment.

Evidence files beside the clones: `native-build.log`, `public-paths.log`, `delete-formats.log`, `fork-dropped-eq-probe.log`, `postpublish-probe.log`, both cross-read probe logs, `scoped-existing-tests.log`, `interop-selftest.log`, audit JSON outputs, resolved metadata and retrieved CI logs. CI log contents are not reproduced wholesale because the report must exclude cloud identifiers and deployment details.

**Resources:** Free space checked before cloning/builds and again during and after validation: about 735 GiB initially, 717 GiB after the fork probe, and 680 GiB at handoff. These are whole-volume readings, not a claim that this audit consumed the entire difference. This audit’s scratch directory is about 15 GiB, including a shared build cache measured at about 13 GiB. The scratch clones, environment, shared compilation cache and probes are retained to let the owner reproduce findings without rebuilding from zero. No other task's artifacts were deleted. `git diff` remained empty in both clones; the only source additions were the two untracked probes. No full workspace suite, full scale matrix, live cloud acceptance, process-kill suite, live Spark comparison, security exploit, platform matrix or release-wheel compatibility run was performed by this audit.


## Appendix A. Public stale-hint reproduction

Exact executed source: `probe_public_paths.py`. SHA-256: `44a696b3105f761e57c2142a5c6c9d92c575cafd72bbdbe2df26e093a205f190`.

```python
import json
import tempfile
from pathlib import Path
from repark import ReparkSession

root = Path(tempfile.mkdtemp(prefix='audit-hadoop-', dir=Path(__file__).parent))
session = (ReparkSession.builder.appName('independent-hadoop-hint')
    .config('spark.sql.catalog.hd.type', 'hadoop')
    .config('spark.sql.catalog.hd.warehouse', str(root)).getOrCreate())
session.sql('CREATE NAMESPACE hd.ns').collect()
session.sql("CREATE TABLE hd.ns.t (id INT) USING iceberg TBLPROPERTIES ('format-version'='2')").collect()
session.sql('INSERT INTO hd.ns.t VALUES (1), (2), (3)').collect()
location = root / 'ns' / 't'
hint = location / 'metadata' / 'version-hint.text'
old_hint = hint.read_text()
session.sql('DELETE FROM hd.ns.t WHERE id = 1').collect()
new_hint = hint.read_text()
current = session.sql('SELECT id FROM hd.ns.t ORDER BY id').to_arrow().to_pylist()
direct = session.read.format('iceberg').load(str(location / 'metadata' / f'v{new_hint.strip()}.metadata.json')).to_arrow().to_pylist()
hint.write_text(old_hint)
stale = session.read.format('iceberg').load(str(location)).to_arrow().to_pylist()
catalog = session.sql('SELECT id FROM hd.ns.t ORDER BY id').to_arrow().to_pylist()
print(json.dumps(dict(case='stale_hint_after_delete', old_hint=old_hint.strip(), new_hint=new_hint.strip(), current=current, direct_latest=direct, stale_hint_path=stale, catalog=catalog, metadata_files=sorted(p.name for p in hint.parent.glob('*.metadata.json')))), flush=True)
session.stop()
```


## Appendix B. Public delete-format reproduction

Exact executed source: `probe_delete_formats.py`. SHA-256: `52a2cc2925b8398020cbd426c6b4e9bf43492de4cf33d951672f67ad9db72681`.

```python
import json
import tempfile
from pathlib import Path
from repark import ReparkSession

for file_format in ['parquet', 'avro', 'orc']:
    root = Path(tempfile.mkdtemp(prefix=f'audit-{file_format}-', dir=Path(__file__).parent))
    session = ReparkSession.builder.appName(f'independent-{file_format}').getOrCreate()
    session.register_memory_catalog('mem', str(root))
    session.sql('CREATE NAMESPACE mem.ns').collect()
    session.sql(f"CREATE TABLE mem.ns.t (id INT) USING iceberg TBLPROPERTIES ('format-version'='2', 'write.format.default'='{file_format}', 'write.delete.mode'='merge-on-read')").collect()
    session.sql('INSERT INTO mem.ns.t VALUES (0),(1),(2),(3),(4)').collect()
    before = session.sql('SELECT id FROM mem.ns.t ORDER BY id').to_arrow().to_pylist()
    try:
        session.sql('DELETE FROM mem.ns.t WHERE id BETWEEN 1 AND 3').collect()
        delete = 'success'
    except Exception as error:
        delete = f'{type(error).__name__}: {error}'
    try:
        after = session.sql('SELECT id FROM mem.ns.t ORDER BY id').to_arrow().to_pylist()
    except Exception as error:
        after = f'{type(error).__name__}: {error}'
    print(json.dumps(dict(format=file_format, before=before, delete=delete, after=after, files=sorted(p.suffix for p in root.rglob('*') if p.is_file()))), flush=True)
    session.stop()
```


## Appendix C. Cross-read classifier reproduction

Exact executed source: `probe_cross_read_gate.py`. SHA-256: `220aef6232c6c255f41852e3869370225d139ca2cffc644e7f82aaab8b659975`.

```python
import json
import sys
from pathlib import Path
import pyarrow as pa

sys.path.insert(0, str(Path(__file__).parent / 'repark/python/repark/tests/parity'))
from _acceptance_cross_read import cross_read_table_pair, assert_outcomes_expected

class LocalResult:
    def to_arrow(self):
        return pa.table({'event_time': pa.array([1], type=pa.timestamp('ns'))})

class LocalEngine:
    def sql(self, statement):
        return LocalResult()

class UnavailableOracle:
    def sql(self, statement):
        raise ConnectionError('simulated oracle connection unavailable')

outcome = cross_read_table_pair(LocalEngine(), 'audit.ns.t', UnavailableOracle(), 'audit.ns.t')
assert_outcomes_expected([outcome])
print(json.dumps(dict(gate='accepted', status=outcome.status, error_class=outcome.spark_error_class, error=outcome.spark_error_first_line, has_ns_columns=outcome.has_ns_columns, spark_row_count=outcome.spark_row_count)), flush=True)
control = outcome._replace(has_ns_columns=False)
try:
    assert_outcomes_expected([control])
except AssertionError:
    print(json.dumps(dict(control='non_ns_connection_failure_rejected')), flush=True)
else:
    raise AssertionError('non-nanosecond control should be rejected')
```


## Appendix D. Fork equality-delete/schema-drop reproduction

Exact executed source: `fork/crates/iceberg/tests/audit_dropped_eq_key.rs`. SHA-256: `3f94ab3383230138f62de554112be76c57b062d2cd9467ec32192f0201bc24ef`.

```rust
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use iceberg::arrow::DeleteFilter;
use iceberg::io::{FileIO, LocalFsStorageFactory};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFile, FormatVersion, Literal, NestedField, PartitionKey, PartitionSpec,
    PrimitiveType, Schema, SchemaRef, SortOrder, Struct, TableMetadata, Transform, Type,
    UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::base_writer::position_delete_writer::{
    PositionDeleteFileWriterBuilder, PositionDeleteWriterConfig,
};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Deserialize;

fn gen_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build the {id long, data string} schema")
}
async fn create_rust_table(catalog: &impl Catalog, table_location: &str) -> Table {
    let namespace = NamespaceIdent::new("interop".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");

    let creation = TableCreation::builder()
        .name("rust_table".to_string())
        .location(table_location.to_string())
        .schema(gen_schema())
        .partition_spec(UnboundPartitionSpec::builder().build())
        .sort_order(SortOrder::unsorted_order())
        .format_version(FormatVersion::V2)
        .build();

    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create rust_table")
}
async fn write_gen_data_file(table: &Table) -> DataFile {
    use iceberg::arrow::schema_to_arrow_schema;

    let schema = table.metadata().current_schema();
    let arrow_schema = Arc::new(schema_to_arrow_schema(schema).expect("iceberg schema → arrow"));

    let ids = Int64Array::from(vec![10_i64, 20, 30, 40, 50]);
    let data = StringArray::from(vec!["a", "b", "c", "d", "e"]);
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(ids) as ArrayRef,
        Arc::new(data) as ArrayRef,
    ])
    .expect("build the 5-row data batch");

    let file_path = format!(
        "{}/data/00000-rust-data.parquet",
        table.metadata().location()
    );
    let output = table
        .file_io()
        .new_output(file_path)
        .expect("new parquet output");
    let parquet_builder = ParquetWriterBuilder::new(
        parquet::file::properties::WriterProperties::builder().build(),
        schema.clone(),
    );
    let mut writer = parquet_builder
        .build(output)
        .await
        .expect("build parquet writer");
    writer.write(&batch).await.expect("write data batch");
    let data_file_builders = writer.close().await.expect("close parquet writer");

    let mut builder = data_file_builders
        .into_iter()
        .next()
        .expect("one data file builder");
    builder
        .content(DataContentType::Data)
        .partition_spec_id(0)
        .partition(Struct::empty())
        .build()
        .expect("build unpartitioned data file")
}
async fn write_equality_delete_for_ids(table: &Table, ids: &[i64]) -> DataFile {
    use iceberg::arrow::schema_to_arrow_schema;

    let schema = table.metadata().current_schema();
    let config = EqualityDeleteWriterConfig::new(vec![1], schema.clone())
        .expect("equality-delete writer config (equality_ids = [1])");

    let location_gen = DefaultLocationGenerator::new(table.metadata()).expect("location generator");
    let file_name_gen = DefaultFileNameGenerator::new(
        "eq-del".to_string(),
        Some(uuid::Uuid::now_v7().to_string()),
        iceberg::spec::DataFileFormat::Parquet,
    );
    let projected_iceberg_schema = Arc::new(
        iceberg::arrow::arrow_schema_to_schema(config.projected_arrow_schema_ref())
            .expect("projected arrow schema → iceberg schema"),
    );
    let parquet_builder = ParquetWriterBuilder::new(
        parquet::file::properties::WriterProperties::builder().build(),
        projected_iceberg_schema,
    );
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        table.file_io().clone(),
        location_gen,
        file_name_gen,
    );

    let mut writer = EqualityDeleteFileWriterBuilder::new(rolling, config)
        .unpartitioned()
        .build(None)
        .await
        .expect("build equality-delete writer");

    let arrow_schema = Arc::new(schema_to_arrow_schema(schema).expect("iceberg schema → arrow"));
    let data: Vec<&str> = std::iter::repeat_n("x", ids.len()).collect();
    let batch = RecordBatch::try_new(arrow_schema, vec![
        Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
        Arc::new(StringArray::from(data)) as ArrayRef,
    ])
    .expect("build the equality-delete key batch");
    writer
        .write(batch)
        .await
        .expect("write equality-delete batch");
    writer
        .close()
        .await
        .expect("close equality-delete writer")
        .into_iter()
        .next()
        .expect("one equality-delete file")
}
async fn append_5row_table(catalog: &impl Catalog, table_location: &str) -> (Table, String) {
    let table = create_rust_table(catalog, table_location).await;
    let data_file = write_gen_data_file(&table).await;
    let data_file_path = data_file.file_path().to_string();
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .add_data_files(vec![data_file])
        .apply(tx)
        .expect("apply fast append");
    let table = tx.commit(catalog).await.expect("commit fast append");
    (table, data_file_path)
}
#[tokio::test]
async fn equality_delete_survives_drop_of_key_column() {
    let tmp = tempfile::TempDir::new().expect("temp dir");
    let warehouse = tmp.path().to_string_lossy().to_string();
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("audit", HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]))
        .await.expect("catalog");
    let (table, _) = append_5row_table(&catalog, &format!("{warehouse}/table")).await;
    let file = write_equality_delete_for_ids(&table, &[20, 40]).await;
    let tx = Transaction::new(&table);
    let tx = tx.row_delta().add_deletes(vec![file]).apply(tx).expect("apply deletes");
    let table = tx.commit(&catalog).await.expect("commit deletes");
    let baseline = table.scan().build().expect("scan").to_arrow().await.expect("baseline stream")
        .try_collect::<Vec<_>>().await.expect("baseline collect");
    assert_eq!(baseline.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    let tx = Transaction::new(&table);
    let tx = tx.update_schema().delete_column("id").apply(tx).expect("apply drop id");
    let table = tx.commit(&catalog).await.expect("commit drop id");
    let stream = table.scan().build().expect("post-drop scan").to_arrow().await.expect("post-drop stream");
    let after = stream.try_collect::<Vec<_>>().await;
    println!("POST_DROP_RESULT={after:?}");
    let batches = after.expect("dropping equality-delete key must preserve readable surviving data");
    let values: Vec<String> = batches.iter().flat_map(|b| b.column(0).as_any().downcast_ref::<StringArray>().expect("data strings").iter().map(|s| s.expect("non-null").to_owned())).collect();
    assert_eq!(values, vec!["a", "c", "e"]);
}
```


## Appendix E. RePark post-publication refresh reproduction

Exact executed source: `repark/crates/repark-spark/tests/audit_postpublish.rs`. SHA-256: `e9f043f3c3afeee416675198780ff1088b79adac6450db4692bf1cb98760f446`.

```rust
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use iceberg::{Catalog, CatalogBuilder, Namespace, NamespaceIdent, TableIdent, TableCreation, TableCommit, ViewCreation};
use iceberg::table::Table;
use iceberg::view::{View, ViewCommit};
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use futures::TryStreamExt;

#[derive(Debug)]
struct RefreshFailureCatalog {
    inner: Arc<dyn Catalog>,
    fail_listing: AtomicBool,
}

#[async_trait::async_trait]
impl Catalog for RefreshFailureCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn update_namespace_properties(
        &self,
        namespace: &NamespaceIdent,
        removals: HashSet<String>,
        updates: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner
            .update_namespace_properties(namespace, removals, updates)
            .await
    }

    async fn set_namespace_properties(
        &self,
        namespace: &NamespaceIdent,
        updates: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner
            .set_namespace_properties(namespace, updates)
            .await
    }

    async fn remove_namespace_properties(
        &self,
        namespace: &NamespaceIdent,
        removals: HashSet<String>,
    ) -> iceberg::Result<()> {
        self.inner
            .remove_namespace_properties(namespace, removals)
            .await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        if self.fail_listing.load(Ordering::SeqCst) {
            return Err(iceberg::Error::new(iceberg::ErrorKind::Unexpected, "AUDIT: catalog listing unavailable after publication"));
        }
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        self.inner.update_table(commit).await
    }

    async fn publish_create_table(&self, table: Table) -> iceberg::Result<Table> {
        let committed = self.inner.publish_create_table(table).await?;
        self.fail_listing.store(true, Ordering::SeqCst);
        Ok(committed)
    }

    async fn publish_replace_table(
        &self,
        table: Table,
        expected_base_metadata_location: Option<String>,
    ) -> iceberg::Result<Table> {
        self.inner.publish_replace_table(table, expected_base_metadata_location).await
    }

    async fn list_views(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_views(namespace).await
    }

    async fn create_view(
        &self,
        namespace: &NamespaceIdent,
        creation: ViewCreation,
    ) -> iceberg::Result<View> {
        self.inner.create_view(namespace, creation).await
    }

    async fn load_view(&self, view: &TableIdent) -> iceberg::Result<View> {
        self.inner.load_view(view).await
    }

    async fn drop_view(&self, view: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_view(view).await
    }

    async fn view_exists(&self, view: &TableIdent) -> iceberg::Result<bool> {
        self.inner.view_exists(view).await
    }

    async fn rename_view(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_view(src, dest).await
    }

    async fn update_view(&self, commit: ViewCommit) -> iceberg::Result<View> {
        self.inner.update_view(commit).await
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn properties(&self) -> &HashMap<String, String> {
        self.inner.properties()
    }

    async fn invalidate_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.invalidate_table(table).await
    }

    async fn invalidate_view(&self, view: &TableIdent) -> iceberg::Result<()> {
        self.inner.invalidate_view(view).await
    }
}

#[tokio::test]
async fn failed_ctas_must_not_hide_a_committed_table() {
    let directory = tempfile::tempdir().expect("local warehouse");
    let inner: Arc<dyn Catalog> = Arc::new(MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("memory", HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), directory.path().display().to_string())]))
        .await.expect("catalog"));
    inner.create_namespace(&NamespaceIdent::new("ns".to_string()),
        HashMap::from([("location".to_string(), directory.path().join("ns").display().to_string())]))
        .await.expect("namespace");
    let catalog = Arc::new(RefreshFailureCatalog { inner: inner.clone(), fail_listing: AtomicBool::new(false) });
    let session = ReparkSession::builder().with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension)).build().expect("session");
    session.register_iceberg_catalog("fault", catalog.clone()).await.expect("register");
    let result = match session.sql("CREATE TABLE fault.ns.t USING iceberg TBLPROPERTIES ('format-version'='2') AS SELECT CAST(7 AS INT) AS id").await {
        Ok(frame) => frame.collect().await.map(|_| ()).map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    };
    println!("CTAS_RESULT={result:?}");
    let ident = TableIdent::from_strs(["ns", "t"]).expect("identifier");
    let published = inner.load_table(&ident).await.expect("published table remains");
    let batches: Vec<_> = published.scan().build().expect("scan").to_arrow().await.expect("read").try_collect().await.expect("rows");
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    println!("PUBLISHED_ROWS={rows}; CURRENT_SNAPSHOT={}", published.metadata().current_snapshot().is_some());
    assert!(result.is_ok(), "CTAS returned failure despite a committed table: {result:?}; rows={rows}");
}
```
