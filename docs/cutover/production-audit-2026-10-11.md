# Production readiness audit — 2026-10-11

Class: campaign. This assessment freezes when the owner dispositions its findings into remediation units or a qualified production scope. Archive it when that disposition campaign closes. Later measurements amend it with a date; they do not erase the original evidence.

## Assessment and revision

**Current main is not ready for an unrestricted production-grade claim.** The review reproduced a credential disclosure, two bypasses of the local-file write policy, a caught Rust panic, and existing wrong-value write paths. The strongest risks are in surfaces that already exist, rather than in missing roadmap features. A tightly qualified workload may still be usable, but its qualification must exclude or close the applicable findings below.

The review targets RePark main `07be1dc930711bb927204f3df99d08526c9aaef6`, fetched on 2026-10-11, version 1.5.4 in Cargo, and owned Iceberg fork `076d5f982d1d2ef352c3bb5d76d02cff27bc164a`. This is a source assessment of that main revision, not a claim about the exact published 1.5.4 wheel. Some fixes on main postdate its release tag. The branch is `docs/production-bugs-audit-2026-10-11`; runtime code is unchanged.

The fresh editable native module reports 1.5.4 and lives in this worktree. The module previously installed in the original checkout reported 1.0.1 and was deliberately excluded from engine evidence. Local runtime results below use the fresh build. It is a development build: its functional results are useful; its timings are not release performance baselines.

The subsequent requested Sol 6.1 High performance review is recorded separately in [the performance assessment](../perf/production-performance-audit-2026-10-11.md). Raw reproduction scripts and selected outputs are in [the evidence appendix](production-audit-evidence-2026-10-11.md). The unit record is [the audit ledger](../../task/ledgers/completed/production-audit-2026-10-11-ledger.md).

## Evidence and priority conventions

**Executed** means this review exercised the pinned code. **Source-confirmed** means the control flow is present but its operational consequence was not end-to-end tested. **Existing measured finding** means an open repository finding was checked against current source; a fresh reproduction is named when available. **Qualification gap** means the necessary evidence is unavailable; it is not an assertion that the feature fails. **Conditional candidate** requires the stated premise to be established before filing it as a confirmed defect.

S1 blocks production use of the affected surface because it can disclose secrets, defeat an explicit safety policy, or silently store wrong data. S2 needs resolution or a documented exclusion before broad production qualification. Severity follows the stated trigger; it is not a claim that every RePark workload is affected. No unconditional catastrophic S0 defect was established. These severities are recommendations, not owner dispositions.

| ID | Priority | Finding | Evidence | Applicability |
|---|---|---|---|---|
| PA-01 | S1 | dbt profile mismatch exposes both profiles' raw configuration values | Executed source module | dbt process opening differing profiles |
| PA-02 | S1 | Executing plan wrappers bypass the default local-file DDL policy | Executed public Spark SQL | `EXPLAIN ANALYZE COPY` outside warehouse |
| PA-03 | S1 | Missing parent directories permit a warehouse-symlink escape | Executed public Spark SQL | Local warehouse with a symlink to another directory |
| PA-04 | S2 | Non-ASCII local paths panic in both policy helpers | Executed Spark entry point; ANSI source-confirmed | Ordinary Unicode filenames |
| PA-05 | S1 | Write carriers bypass timestamp store conformance | Fresh reproduction of an existing measured finding | Zoned/nanosecond write through executing wrappers |
| PA-06 | S1 | Nested microsecond NTZ values depend on the write route | Fresh reproduction of existing R-011 | Nested instant-to-wall writes in non-UTC sessions |
| PA-07 | S1 | Struct assignment may silently map fields differently or fill NULL | Existing measured R-016 | Reordered, renamed, recased or missing struct fields |
| PA-08 | S1 | Untyped NULL expressions narrow nanoseconds before storage | Fresh expression reproduction of existing R-017 | Nanosecond expressions with bare NULL |
| PA-09 | S2 | Negative nanoseconds narrow differently between write routes | Existing measured R-009 | Pre-epoch values with sub-microsecond digits |
| PA-10 | S2 | Default memory detection misses nested cgroups and non-Linux RAM | Source-confirmed | Restricted deployment without an explicit pool budget |
| PA-11 | S2 | Changelog view materializes the entire source outside pool accounting | Source-confirmed | Large changelog queries, including small result limits |
| PA-12 | S2 | Latest live-parity gate is red; at least one pin is stale | Remote run plus fresh live-oracle classification | Release/compatibility evidence |
| PA-13 | S2 | Current cloud acceptance and some oracle fixtures are unqualified | Remote run and source-confirmed paths | AWS production sign-off and fixture reproducibility |
| PA-14 | S2 | Current-state and execution documentation contradict the tree | Source-confirmed | Operator and contributor decisions |
| PA-15 | S2 | Two-session microbatch completion test fails its bound | Full-suite observation plus isolated failure | Current-base Rust readiness gate; root cause unclassified |
| PC-01 | Conditional S1 | Optional cluster constructor can expose an unauthenticated scheduler | Source-confirmed configuration; no remote exercise | Opt-in `cluster` with a non-loopback bind |
| PC-02 | Conditional S2 | Snapshot ancestry traversal has no cycle bound | Source-confirmed loop; malformed-metadata admission unestablished | Microbatch source metadata with a parent cycle |

## PA-01 — dbt profile mismatch discloses configuration secrets

In [dbt session management](../../python/dbt-repark/src/dbt/adapters/repark/session.py), `session_key` includes the full sorted `catalog_properties` and `session_properties`. `acquire_session` embeds both the live and requested keys with `repr` in its mismatch exception, lines 20–27 and 72–77. A changed database name alone is sufficient to take this branch even if both profiles carry the same secret. These exceptions can reach terminal output, job logs and support bundles.

A direct execution of the current source module used two synthetic secret sentinels and a pre-seeded live session. Both sentinels appeared in the `DbtRuntimeError`. This requires no actual credential, cloud request or native engine behavior. The guard against reusing a mismatched profile is appropriate; its diagnostic serialization is unsafe.

The related [credentials class](../../python/dbt-repark/src/dbt/adapters/repark/connections.py), line 59, includes `catalog_properties` in `_connection_keys`. Review the dbt debug display path as part of the same unit. That secondary display risk was source-inspected, not reproduced through the full dbt CLI here.

**Required result:** compare full configuration internally, but emit only safe identifiers and changed key names. Test the real dbt error and debug paths with sentinel secrets in both property dictionaries, including a source URL containing credentials. Verify that mismatch refusal still works. This boundary should consume the project's redaction policy; a Python exception must not bypass it.

**Amendment (2026-10-16, PA-DBT-MISMATCH-REDACT-1):** on main `60134633` the refusal carried all three sentinels, and the debug display was reproduced through `dbt debug`: it printed and logged the `catalog_properties` secret and URL password. The refusal now names the live and requested `database` and `warehouse` (through `redact_property_value`) and the sorted changed keys, prefixed by their dictionary, and no value. dbt's connection report renders `catalog_properties` redacted. Pins: [`test_profile_mismatch_redact.py`](../../python/dbt-repark/tests/test_profile_mismatch_redact.py), 9 cases, 8 red on main; ledger [pa-dbt-mismatch-redact-1](../../task/ledgers/staging/pa-dbt-mismatch-redact-1-ledger.md).

## PA-02 — an executing wrapper bypasses the local-file policy

[Spark's guard](../../crates/repark-spark/src/local_fs_ddl.rs), lines 24–33, examines only a root `CreateExternalTable` or `Copy` plan. [ANSI's guard](../../crates/repark-sql/src/guards.rs), beginning at line 310, has the same shallow shape. Executing wrappers contain the write below the root, so the guard returns success without checking its destination.

With default configuration and a registered local warehouse, bare `COPY (SELECT 1 AS a) TO '<outside>/direct' STORED AS PARQUET` refused and wrote no file. `EXPLAIN ANALYZE COPY ... TO '<outside>/wrapped'` succeeded and created a Parquet file in the same outside directory. The permitted warehouse control also succeeded. This is an end-to-end public Spark SQL reproduction.

This defeats `repark.sql.allowLocalFilesystemDDL=false`, an explicit engine policy. It does not confer permissions beyond those of the host process, and that setting should not be described as a complete operating-system sandbox. The direct/wrapped discrepancy is nevertheless a defect in the promised restriction.

**Required result:** check each write that an executing carrier can run, including `Analyze`, prepared statements at execution, and any supported nested write carrier. Preserve plain `EXPLAIN` behavior. Prove refusal before side effects through both SQL doors with default and explicit opt-in controls. The duplicate ANSI implementation is implicated by source, but was not independently exercised here. The public Python `repark.sql()` callable builds the default `DataFusionDialect`, not `repark_sql::AnsiDialect`; its shared `PreExecute` belt does not call this door-specific local-file guard. Follow-up qualification must trace the actual native entry point and decide its policy explicitly, rather than assuming the ANSI crate guard automatically protects that callable. This routing observation is source-confirmed, not another executed bypass in this audit.

**Amendment (2026-10-15, PA-LOCAL-FS-POLICY-1):** one policy, `repark_core::local_fs_policy`, now serves both doors and `repark.sql()`. It walks every `Copy` and `CreateExternalTable` node at any depth before execution: `Analyze` and a `Prepare` input are included, and nothing below a plain `Explain` is checked. `PREPARE … AS COPY` does not parse on either door. By the owner's ruling of 2026-10-15, `repark.sql()` gets the guard through `LocalFsGuardedDialect`; that entry has no opt-in. The ANSI door's opt-in never opened on main: its reader missed the real extension's unprefixed key. It opens now. Pins: `python/repark/tests/session/test_pa_local_fs_policy_1.py` (`test_pa02_*`, `test_repark_sql_*`), `crates/repark-spark/src/tests/pa_local_fs_policy.rs` and `crates/repark-sql/tests/pa_local_fs_policy.rs` (`pa02_*`), and `crates/repark-core/src/local_fs_policy.rs` (`the_walk_checks_a_write_below_every_carrier_but_plain_explain`, `the_native_door_*`). Ledger: [pa-local-fs-policy-1](../../task/ledgers/staging/pa-local-fs-policy-1-ledger.md).

## PA-03 — a missing destination parent defeats warehouse containment

`canonicalize_best_effort` in [the Spark guard](../../crates/repark-spark/src/local_fs_ddl.rs), line 125, and [the ANSI guard](../../crates/repark-sql/src/guards.rs), line 418, canonicalizes the target or its immediate parent. If both do not exist, it falls back to lexical normalization. It does not resolve the nearest existing ancestor.

The task fixture created `<warehouse>/link -> <outside>`. A direct COPY to `<warehouse>/link/new/sub/export`, with the intermediate directories absent, passed the default policy and wrote a file at `<outside>/new/sub/export`. No race was needed. This is a fresh public Spark SQL reproduction in task-owned temporary directories.

**Required result:** the destination containment check must account for symlinks in existing ancestors. Decide the concurrent symlink-change threat model explicitly; a pathname-only precheck can still have a check/use race when another actor controls the directory. At minimum, add tests for missing one and multiple parent directories, symlinks inside and outside the warehouse, and both SQL doors. Refuse unsafe or indeterminate destinations rather than treating lexical ancestry as proof of physical containment.

**Amendment (2026-10-15, PA-LOCAL-FS-POLICY-1):** containment now resolves the nearest existing ancestor with `canonicalize` and joins the missing tail. A `..` in that tail, a dangling symlink, an ancestor whose metadata read fails other than not-found, or an empty location is indeterminate and refuses. By the owner's ruling of 2026-10-15, the concurrent symlink-change race between the check and the write stays out of scope as a dated row in the unit ledger. Pins: the `pa03_*` tests of both doors' `pa_local_fs_policy.rs` and of `test_pa_local_fs_policy_1.py`, and the resolver tests of `crates/repark-core/src/local_fs_policy.rs`.

**Amendment (2026-10-15, PA-LOCAL-FS-POLICY-1 fix round, owner ruling 103):** the guard now reads
a location the way the write reads it, through DataFusion's own `ListingTableUrl::parse`, so a
symlink followed by `..`, a percent-encoded dot segment (`%2e%2e`, `%2E%2E`, `.%2e`) and a
relative `file:` URL, each of which wrote outside every warehouse on the first head of this unit,
refuse on both doors and write nothing. A `..` that steps back over a symlink under the
warehouse refuses, and so does any location whose as-typed reading, with symlinks followed,
resolves elsewhere than the written one; both checks use the warehouse root and the target
resolved the same way, so a warehouse registered through a symlinked path still accepts writes
inside it. Pins: the `v01_*`, `v02_*` and `v03_*` tests of both doors' `pa_local_fs_policy.rs`
and of `test_pa_local_fs_policy_1.py`, and `crates/repark-core/src/local_fs_policy/tests.rs`.

**Amendment (2026-10-16, PA-LOCAL-FS-POLICY-1 fix round 2, owner ruling 114 (b)):** the guard
judges where the bytes land. The written reading of the location (DataFusion's
`ListingTableUrl::parse`, which removes every `..` the write removes) is resolved by its nearest
existing ancestor plus the remaining components and compared against the warehouse root, read as
a plain path or `file:` URL with no glob split and resolved the same way. The as-typed reading
and its whole-path comparison of the previous amendment are withdrawn: a `..` the write removes,
over a symlink or after a missing directory, is judged after it is removed and writes where it
lands inside the warehouse; a `[`, `?` or `*` in a warehouse path no longer widens the root to its
parent. The V-01, V-02 and V-03 pins still refuse. The check-then-write race stays the dated row.
Pins: the `rv01_*`, `rv02_*`, `rv03_*` and 114-cell tests of both doors' `pa_local_fs_policy.rs`
and of `crates/repark-core/src/local_fs_policy/tests.rs`.

## PA-04 — Unicode location classification panics

`strip_ascii_prefix_ci` in [Spark](../../crates/repark-spark/src/local_fs_ddl.rs), line 53, slices `value[..prefix_len]` without checking a UTF-8 character boundary. `strip_prefix_ci` in [ANSI](../../crates/repark-sql/src/guards.rs), line 403, repeats it. The prefix is ASCII; the candidate path is not necessarily ASCII.

COPY to the relative location `aaaaaaé` produced a caught Rust panic at byte 7 inside `é`. The Python binding raised `PySparkException` and printed the panic hook on stderr. The review did **not** observe Python process termination. A Rust caller without the Python fence still reaches a panic rather than a normal planning error. An extracted exact helper also reproduced the boundary failure independently.

**Required result:** use a checked prefix slice or ASCII-byte comparison that returns a normal classification for every valid UTF-8 path. Test non-ASCII characters spanning byte boundaries 5, 7 and 9, ordinary local filenames, and `file://localhost` forms. Verify the named policy error and the explicit opt-in success path instead of merely asserting that a panic was caught.

**Amendment (2026-10-15, PA-LOCAL-FS-POLICY-1):** the one `strip_ascii_prefix_ci` compares through `str::get`, so `aaaaé`, `aaaaaaé` and `file://aaaaaaaaé` classify as local paths and refuse with the named policy error on both doors and `repark.sql()`. The `file://localhost` forms and ordinary non-ASCII names refuse outside, write under the warehouse, and write under the opt-in. Pins: the `pa04_*` tests of both doors' `pa_local_fs_policy.rs` and of `test_pa_local_fs_policy_1.py`, and the classification tests of `crates/repark-core/src/local_fs_policy.rs`.

## PA-05 — executing carriers still store the wrong timestamp instant

This is an existing gap already named in [the parity registry](../spark-sql-iceberg-parity.md), R-008's carrier note and `WRITE-BODY-CARRIERS-1`. The ordinary top-level `timestamptz_ns` fix on current main is real; this report does not reopen its covered bare write routes.

The fresh build was exercised with the same `timestamp_ns` source column into three v3 `timestamptz_ns` tables in `America/New_York`. Bare INSERT stored `1767341045123456789`. `EXPLAIN ANALYZE INSERT` and `PREPARE ... AS INSERT` followed by `EXECUTE` each stored `1767323045123456789`. The source row and target schemas were the same. Values were read as integer ticks from the actual Parquet files. The carrier paths stored an instant five hours away from the bare-write result.

**Required result:** all statements that execute a write must apply the same target-store conformance and refusals as the bare statement. Cover timestamp walls, zoned sources, nested refusals, invalid inputs and out-of-range values. Assert stored Arrow type and ticks, no extra publication, and no files or snapshot for refusals. Until then, exclude executing write carriers from qualified production workflows or refuse them by name. A green ordinary-write suite does not qualify its wrappers.

## PA-06 — nested microsecond NTZ writes disagree

Existing [R-011](../spark-sql-iceberg-parity.md) concerns `STRUCT<v: TIMESTAMP_NTZ, n: INT>`. Field assignment stores an instant's session wall; whole-struct INSERT and other routes store its UTC wall. It is not covered by the refusal of nested **nanosecond** leaves.

A fresh public SQL reproduction inserted the instant `2026-01-02 03:04:05.123456+00:00` into one nested row and assigned it to another row with `UPDATE ... SET st.v`. In `America/New_York`, the stored microsecond ticks differed by five hours. [The existing Rust pin](../../crates/repark-spark/tests/timestamp_ns_wall_doors.rs), `a_nested_microsecond_ntz_field_keeps_the_split_main_has`, deliberately holds the inconsistent baseline across six nested routes and three zones. Passing this pin means the known discrepancy persists; it is not a correctness certificate.

**Required result:** choose and implement the nested store contract across whole-struct and leaf assignments, or narrowly refuse the unsafe forms. Re-measure nested Spark routes with the pinned oracle before making a Spark-parity claim; the registry explicitly says that its earlier unit did not run those nested Spark routes. Test both row-level modes, literals and columns, NULLs, non-UTC zones, and read the stored type and value.

## PA-07 — struct field mapping can change values silently

Existing [R-016](../spark-sql-iceberg-parity.md) records name-versus-position differences between INSERT, overwrite, BY NAME, MERGE and DataFrame routes. This is wider than nanosecond timestamps. Some routes fill a target field with NULL when the source field was renamed; other routes map by position or refuse. The same input therefore may be accepted with different field values according to its route.

The registry reports an 80-cell live Spark measurement on 2026-10-09 and names the discrepancies and absent pins. This review source-checked that it remains open. It did not repeat those 80 cells. Treat the linked measurement as existing evidence, rather than a new independently verified matrix.

**Required result:** pin the complete field-alignment matrix and implement Spark's route-specific contract at the shared write boundary. Use distinguishable values in each field, renamed and swapped names, case variants, missing/extra fields and nullable targets. A row-count or schema-only assertion will miss the wrong-field value. Exclude affected struct writes until that contract is qualified.

## PA-08 — bare NULL narrows nanosecond expressions

Existing [R-017](../spark-sql-iceberg-parity.md) describes nanosecond expressions that are coerced to microseconds when paired with an untyped NULL. The store sees an already narrowed input and cannot recover the lost digits.

The fresh build returned `timestamp[us, tz=UTC]` with microsecond ticks for `coalesce(CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns), NULL)`. Replacing the bare NULL with `CAST(NULL AS timestamp_ns)` returned `timestamp[ns]` and retained the original nine digits. The wider existing measurement covers stores and more expression forms. The unfiltered UPDATE refusal addresses one route; the other answering routes are not thereby fixed.

**Required result:** preserve the sibling timestamp type during NULL coercion while preserving intentional user-written narrowing. Cover `coalesce`, `nvl`, CASE, `if` and array forms, naive/zoned values, ANSI modes and relevant write routes. Assert the expression schema before the store and stored ticks afterwards. Typed NULL is a local workaround, not broad qualification of implicit expressions.

## PA-09 — pre-epoch narrowing has inconsistent rounding

Existing [R-009](../spark-sql-iceberg-parity.md) records `-1 ns` becoming `0 µs` on several overwrite/MERGE/DataFrame routes while INSERT floors to `-1 µs`. Some wall-to-instant routes also differ by the session zone. The recorded fixture and its baseline control are the evidence; this audit did not independently replay the complete microsecond-target matrix.

**Required result:** enforce the owner-ruled floor semantics in every narrowing route. Include negative values immediately around the epoch, timezone-sensitive naive sources and deliberate explicit casts. Historical datasets and nanosecond ingest must exclude these forms until closed. Keep this distinct from the fixed top-level nanosecond-target store kernel.

## PA-10 — deployment memory detection is incomplete

[The default pool detector](../../crates/repark-core/src/session/spill.rs), lines 69–78, reads only `/sys/fs/cgroup/memory.max`, then host `/proc/meminfo`, then falls back to 8 GiB. It does not locate the process's cgroup, inspect ancestor limits, handle cgroup v1, or detect memory on macOS/Windows. On the audit host the root `memory.max` file was absent and the process belonged to a nested systemd cgroup; no smaller limit was imposed to deliberately induce an OOM.

In a nested cgroup restricted below the host-derived budget, pool admission can exceed the actual process allowance. On non-Linux machines the fallback can exceed available RAM. Further, the pool is per session and is not a cap on process RSS or all Arrow/Python allocations. The existing spill work should not be represented as a process-wide memory guarantee.

**Required result:** derive the effective memory constraint for supported deployment platforms, or require an explicit budget when detection cannot be trusted. Test process membership and ancestor limits using fixtures, plus a disposable constrained process integration test. Document the remaining headroom for decoded batches, Arrow results, caches, writer buffers and multiple sessions. The performance review can quantify these costs separately.

**Amendment (2026-10-26, ICE-RESOURCE-BOUND-1):** closed for cgroup v2. The default pool detector (`detect_memory_bytes` in [spill.rs](../../crates/repark-core/src/session/spill.rs)) reads the unified `0::` line of `/proc/self/cgroup`, walks `memory.max` from the process's cgroup to the root, takes the minimum finite value and caps it by the host `MemTotal`. With no finite cgroup value it uses `MemTotal`, as before. Owner ruling Q3 closes it this way. Pins: the seven fixture cells in `spill_unit_tests`, each asserting the clamped pool. cgroup v1, macOS and Windows are **declared**, not closed: the detector keeps its declared path there, the deployment must set an explicit pool budget (`repark.memory.limit.gb` or `datafusion.runtime.memory_limit`), and the build does not refuse (O-11). Not in this unit: the disposable constrained-process integration test that this row names (acceptance only, O-13), and the headroom document for decoded batches, caches, writer buffers and several sessions. Both are carded: [PA-10-CONSTRAINED-PROCESS-TEST-1](../../task/roadmap/mid-term/pa-10-constrained-process-test-1-card-2026-10-26.md) and [PA-10-HEADROOM-DOC-1](../../task/roadmap/mid-term/pa-10-headroom-doc-1-card-2026-10-26.md). Ledger: [ice-resource-bound-1](../../task/ledgers/staging/ice-resource-bound-1-ledger.md), C-009. Bound: [resource-bound-2026-10-26](../perf/resource-bound-2026-10-26.md).

## PA-11 — changelog query materialization bypasses pool budgeting

[`ChangelogViewProvider`](../../crates/repark-iceberg/src/catalog/changelog_view.rs), lines 53–57 and 110–118, asks its source for every column with no filter or limit, collects all source batches, concatenates them, transforms the full result, and projects only afterwards. There is no reservation around the retained and concatenated batches. The outer execution limit does not make the source stream incremental.

This can allocate the entire changelog plus its concatenated copy even for a small result limit. It is source-confirmed; this audit did not intentionally exhaust host memory. Stateful changelog transformations can require cross-batch context, so simply forwarding LIMIT would be incorrect for some transforms.

**Required result:** provide a budgeted transformation strategy with bounded state or a spill path, and a clear refusal for unsupported sizes. Measure large histories with narrow projection, LIMIT and full materialization under a small pool budget. Preserve cross-batch changelog ordering and correctness. The memory limit must either account for these buffers or explicitly state the exclusion.

**Amendment (2026-10-26, ICE-RESOURCE-BOUND-1):** bounded; the statement refuses above the bound. `ChangelogViewPartition::execute` drains its source window through `drain_changelog_source` and refuses when the window's Arrow bytes exceed `repark.changelog.max-source-fraction` (default 0.0625) times the live pool: 512 MiB at the 8 GiB default pool. The refusal keeps the pool's class and names the changelog read and the key; it names no data-deleting action. Nothing is staged, because the read writes no file. The bound covers the source window, not the result: a `LIMIT` and a narrow projection do not reduce the charge. This bounds the statement; it does not reduce the memory it uses. Below the bound the window is still held whole, and the step-0 measurement put the outside-pool growth at 7.4 to 7.8 times the source bytes. Pins: the PA-11 cells of `crates/repark-spark/src/tests/resource_bound.rs` (format v2 and v3) and the changelog cells of `python/repark/tests/writes/test_ice_resource_bound_1.py`. Ledger C-003.

## PA-12 — live-parity failure needs classification before release qualification

The latest inspected [live-parity run](https://github.com/TRO-Wolf/repark/actions/runs/38121782754) targeted `2c33f289c177edc439de6c58a6beef45c53e7ba1`, the code parent preceding the current docs-only main update. It finished with **10 failed, 16,934 passed, 23 skipped and 153 xfailed**. A successful fast CI run on current main does not replace this failed live gate.

Fresh classification of the indexed-transform failure is unusually useful: the live banner was **Spark 4.1.2, UTC**. On ANSI on and off, the no-NULL, mixed-NULL, empty-array and NULL-array cases matched RePark's SQL and Column routes in values, integer element width and nullability. The mixed-NULL Column case in [the test](../../python/repark/tests/parity/test_parity_live_fnp8.py) still expects int64. The observed answer is int32 on both engines. **This failure is a stale claim, not evidence that RePark returns the wrong type in that cell.** The pin should be repaired after its complete controls are re-measured, using the parity-audit discipline.

The same run reports pinned Apache `minute` and `second` tests failing on `UNSUPPORTED_TIME_TYPE`. Those are loud refusals, not silent value corruption. Whether they are disposed divergences or unsupported additions to the pinned Apache tests requires a fresh affected live-oracle measurement and registry decision. The subprocess summary reports three failures but retains details for only two in its tail; this audit does not invent the third cause.

**Required result:** classify every red independently, restore a full green live run at the chosen release revision, and distinguish stale pins from product bugs and declared refusals. Do not weaken the gate or change runtime defaults to erase evidence. This branch reports classifications and outstanding registry work; it changes no pins.

## PA-13 — AWS acceptance and fixture reproduction remain unqualified

Six failures in the same parity run are fixture subprocesses looking for `/tmp/sparkenv/bin/python`, which the runner did not have: `test_ice_avro_name_1`, `test_ice_overwrite_mode_1_transform`, `test_ice_replace_columns_1`, `test_ice_rtas_byname_1`, `test_ice_session_write_conf_1` and `test_ice_wap_branch_1` (each its `test_live_oracle_fixture_reproduces` case). This is a reproducibility defect in the test path. The local audit machine has that path, but a clean runner does not inherit it. A passing recorded golden is insufficient if its live reproduction cannot start.

The `test_live_spark_race_scan_forwards` fixture comparison also returned a Spark `AnalysisException` where its fixture expected `committed`. This audit did not replay the catalog-race oracle. It remains unclassified; the failed assertion does not establish a RePark race defect.

The latest inspected [AWS acceptance run](https://github.com/TRO-Wolf/repark/actions/runs/38126286100), at the same parent revision, was **cancelled** after the acceptance step had run about 74 minutes. Build and credential setup completed; the dbt gold step was skipped. Cancellation is missing qualification evidence, not proof that an assertion failed. No cloud acceptance run or cloud mutation was dispatched by this audit.

**Required result:** provision the pinned oracle through a portable configured executable, replay the affected fixtures, and obtain a completed AWS acceptance run including repeated publication and dbt gold at the production revision. Record the run, catalog, revision and actual acceptance outcomes. Review why the cancelled run made no completed evidence; do not guess its cause from duration alone.

## PA-14 — documentation can cause incorrect operational assumptions

These are current-tree discrepancies and operational wording gaps as of 2026-10-11:

| Claim location | Current statement | Evidence and consequence |
|---|---|---|
| [README](../../README.md), line 11 | Leads with v1.0.0 on PyPI | Cargo and release state are 1.5.4. The dated milestone is historical, but its front-door presentation does not identify the current version. |
| [STATUS](../../STATUS.md), lines 10, 66 | Last updated 2026-09-12; nine delivered crates | The file contains October release state. Cargo has eleven workspace crates, including delivered connect and distributed. |
| [ARCHITECTURE](../../ARCHITECTURE.md), lines 12–16 | Ten crates; connect described among planned extractions | The workspace has eleven and connect has substantial implemented code. A reviewer may miss its security and runtime surface. |
| ARCHITECTURE, line 144 | Nothing executes until a terminal action | Bare COPY applies eagerly. [The existing test](../../crates/repark-spark/src/tests/local_fs_ddl.rs) explicitly requires that behavior. DDL/writes need separate descriptions from lazy reads. |
| ARCHITECTURE, line 174 | On failure, no partial snapshot is published | Atomic publication can still hold when a failed call committed a complete snapshot. A definite failure differs from `CommitStateUnknown`, where publication may have succeeded. The wording needs that distinction; the inspected fork already avoids blind retry correctly. |
| [DEVELOPMENT](../../DEVELOPMENT.md), testing section | Wheel workflow runs the facade suite; dbt has no CI job | [Smoke CI](../../.github/workflows/smoke.yml) runs the facade and dbt paths. The described gate ownership is stale. |

**Required result:** a separate documentation truth-up against Cargo, workflow definitions and execution tests. Keep facts in their designated homes. State a qualified production scope with exclusions and evidence, rather than deriving production readiness from a release number, passing baseline-preservation pins or the historical matrix count. The documentation discrepancies are not repaired as passengers in this findings branch.

## PA-15 — current-base microbatch completion fails its test bound

The assembled-branch `make verify` run passed static checks and matrix liveness, then the current core suite reported `microbatch::lifecycle_tests::two_sessions_on_one_sink_wait_for_the_scope` as FAILED. The test runs two sessions against one sink through both `foreachBatch` and `toTable`, then requires termination and durable epoch 5. Its `ended` helper wraps termination in a ten-second timeout.

A direct isolated execution of the exact freshly built test binary also failed: zero passed, one failed, 1,420 filtered out, 14.28 seconds. Stderr names `lifecycle_tests.rs:131` and `the query ends within the bound: Elapsed(())`. This independent process was run while the rest of workspace verification and another host build were active. It reproduces a missed completion bound on this environment; it does not by itself prove a deadlock, corrupt publication or lost records. After the broad test run finished, the same isolated binary passed in 2.19 seconds. The failure is intermittent under the observed conditions; scheduling sensitivity is plausible, but the cause remains unclassified. All three outcomes are recorded in the evidence appendix.

**Required result:** determine whether the bound fails because of scheduling/test assumptions, expensive per-batch work, or a lifecycle/serialization defect. Use barriers and observable progress for the concurrency claim, record which door/epoch is stalled, and preserve finite timeout coverage. Do not simply raise the bound or serialize all tests to hide the behavior. Obtain a full green default workspace run at the chosen revision after a justified resolution. Until classified, the current default readiness gate is unqualified on this host.

**2026-10-12 publication check:** the complete default Rust workspace run passed: 7,433 passed, zero failed and 68 ignored, including the previously failing microbatch test. This supersedes the lack of a green full rerun above; the cause of the earlier intermittent failures remains unclassified.

## Dated rows filed after the audit

### REPARK-SQL-LOCAL-DDL-1 — DATED ROW 2026-10-15 (owner ruling 104 (a))

Once PA-LOCAL-FS-POLICY-1 lands, the public Python `repark.sql()` refuses every local `COPY` and `CREATE EXTERNAL TABLE` outside a registered warehouse, including read-only registration of an existing local file, and that entry offers no opt-in. On main the same statement still registers the file and `SELECT` reads it, so the refusal is new with the policy unit, while the missing opt-in holds on both. Card: [REPARK-SQL-LOCAL-DDL-1](../../task/roadmap/mid-term/repark-sql-local-ddl-1-card-2026-10-15.md) (open, not scheduled).

### LOCAL-FS-CREATE-TABLE-LOCATION-1 — DATED ROW 2026-10-15 (owner ruling 105 (a))

A Spark-door `CREATE TABLE … USING parquet LOCATION '<outside>'` writes Iceberg data and metadata outside every registered warehouse with `repark.sql.allowLocalFilesystemDDL` off, whether followed by `INSERT` or in `… AS SELECT` form, because it is not a `Copy` or `CreateExternalTable` plan node and the local-file guard never sees it. Main behaves the same, and the PR #1075 head does too. Card: [LOCAL-FS-CREATE-TABLE-LOCATION-1](../../task/roadmap/mid-term/local-fs-create-table-location-1-card-2026-10-15.md) (open; the same class as PA-02).

### LOCAL-FS-WRITE-ROOT-1 — DATED ROW 2026-10-15 (owner ruling 105 (a))

One DataFrame write to a path makes later SQL writes to that path allowed: `ReparkSession::note_local_write_root` registers every DataFrame write path as a local warehouse root. Read in the code on main, not run against Spark. Card: [LOCAL-FS-WRITE-ROOT-1](../../task/roadmap/mid-term/local-fs-write-root-1-card-2026-10-15.md) (open, not scheduled).

### PA-10 — DATED ROW 2026-10-26 (ICE-RESOURCE-BOUND-1 step 0)

Measured 2026-10-26 (ICE-RESOURCE-BOUND-1 step 0): in a nested cgroup v2 scope the root `memory.max` is absent and the leaf scope holds the cap (40 GiB). The detector reads the machine total from `/proc/meminfo` (125 GiB), so it sets the 8 GiB pool and does not read the leaf cap. By reading, not run, a nested 4 GiB cap would also give 8 GiB. Status unchanged; the owner rules the close-or-declare question ([order](../../task/wo/ice-resource-bound-1/order.md), step 0, question 3).

### PA-11 — DATED ROW 2026-10-26 (ICE-RESOURCE-BOUND-1 step 0)

Measured 2026-10-26 (ICE-RESOURCE-BOUND-1 step 0): on the ci profile with debug assertions on, under a 40 GiB scope, the changelog view's statement RSS growth is 7.38 to 7.83 times the source bytes on all 12 cells (1M, 4M and 16M rows; v2 and v3; `SELECT id ... LIMIT 10` and `SELECT count(id)`). A `LIMIT 10` does not reduce it (16M rows: up to 6,732 MiB), and a 32 MiB pool never refused. Table: [step-0 bound table](../../task/wo/ice-resource-bound-1/evidence/step-0-bound-table-2026-10-26.md). Status unchanged; the owner rules the bound.

## Conditional candidates requiring a focused follow-up

### PC-01 — optional cluster scheduler exposure

[`ReparkClusterExecutor::new`](../../crates/repark-distributed/src/cluster.rs), lines 405–460, accepts an arbitrary `SocketAddr`, starts the scheduler with default gRPC configuration, and connects through `http://`. The inspected constructor has no authentication/TLS policy or rejection of non-loopback binds. The `cluster` feature is opt-in and this is not an assertion that the default wheel exposes a network service. No remote access was attempted.

For production use of this constructor, verify the dependency server defaults and restrict it to loopback unless a reviewed authenticated transport is supplied. Test rejected wildcard/public binds or authenticated callers, message limits, session isolation, cancellation and shutdown. Until that work is done, document the feature as trusted local experimental execution. Promote this candidate to a confirmed exposure only after the effective server configuration is established.

### PC-02 — microbatch snapshot-parent cycles

[`WindowPlanner::ancestry`](../../crates/repark-iceberg/src/microbatch/window.rs), line 354, repeatedly follows parent IDs into a growing vector without a visited set or metadata-count bound. The sink's [lineage traversal](../../crates/repark-iceberg/src/write/sink_offsets.rs) already uses a metadata-sized bound. If a source table admits a self-parent or multi-node cycle, source planning can loop and retain snapshot references without limit.

This audit did not establish that the pinned fork admits such malformed metadata, and did not construct an exhausting loop. First test metadata admission with a finite corrupt fixture. If admitted, use a bounded traversal that reports a named catalog-integrity error; test self-cycle, two-node cycle, missing parent and valid deep history. If the fork rejects cycles before this boundary, record that invariant and close the candidate with the admission test. Missing proof is not a confirmed denial-of-service defect.

## Reviewed protections and scope limits

The safety review also checked positive controls, to avoid filing already solved problems:

- The Postgres pool has bounded checkout/connect waits, retires unsuccessful resets, and cancels/aborts dropped leases. TLS uses certificate and hostname verification unless `sslmode=disable` is explicitly selected. Parallel lanes export/import a repeatable-read snapshot before streaming. These are source checks, not a newly completed live Postgres qualification.
- Source listing uses the shared property redactor. The dbt mismatch exception is a separate Python path and is the confirmed hole.
- Merge abort cleanup explicitly preserves staged files on `CommitStateUnknown`. The pinned fork excludes unknown-outcome errors from retry even if marked retryable, then performs read-only reconciliation. A blind retry or deletion of possibly committed files was not found in those inspected paths.
- Microbatch `foreachBatch` side effects are explicitly at-least-once; a sink offset stamp is a separate contract. This report does not reclassify that declared behavior as exactly-once. Source replacement has a UUID check at the window boundary. Resume, stop timeout and recovery tests were inspected.
- Nested naive nanosecond writes have named refusals; the ordinary top-level nanosecond and zoned nanosecond store fixes on main are present. Missing feature support and declared refusals are not themselves silent-corruption findings.
- Unsafe code is confined to the binding boundary by the workspace policy. Arrow capsules remain a trusted native ABI boundary; no arbitrary-pointer exploit was established.
- Dependency advisory/license/source checks and Python dependency auditing completed successfully on 2026-10-11. That result applies to scanner coverage and databases at that time, not to application security.

This was a deep, risk-focused review with executed boundary probes, not an exhaustive proof of every function. It reviewed representative engine semantics and write boundaries, local filesystem/catalog policy, credential handling, Postgres connection lifecycle, microbatch publication/recovery, resource accounting, optional cluster exposure, packaging/workflows and current-state claims. It did not run every function against Spark, execute real AWS writes, validate every cloud-auth mode, complete a clean live Postgres matrix, perform process-kill/power-loss recovery across every catalog, or complete a prolonged production workload soak. Those cells remain unqualified.

## Proposed remediation order and production gate

1. Close PA-01–04 in narrow security and boundary units. They are small, independently reproducible defects; include both doors where implicated.
2. Close or refuse the unsafe stored-value forms PA-05–09. Reuse the existing cards and registry rows rather than creating competing truth sources. Data already written through inconsistent routes may need a separate assessment; a fix cannot recover lost precision automatically.
3. Restore reproducible, classified live evidence for PA-12–13. A stable qualified workload needs complete job comparisons, schema/value checks and repeated publication/recovery evidence, not just unit pass counts.
4. Qualify memory and service deployment boundaries in PA-10–11 and PC-01–02. Measure RSS outside the pool, concurrent-session headroom, cancellation, and recovery. The Sol performance report can prioritize measured speed/RAM improvements without changing this correctness gate.
5. Truth up current-state documentation and record the explicit deployment scope, supported catalogs/write forms, exclusions, operational limits, rollback and recovery procedure.

For an unrestricted production claim, the affected S1s need remediation or enforced named refusals. A narrower canary qualification must name exactly which findings are excluded by its input/operation contract, then measure that complete workload on the intended build and catalog. Existing owner-deferred findings remain deferred until the owner changes their disposition; this assessment explains their production implications and does not silently overrule them.

## Validation record

The [evidence appendix](production-audit-evidence-2026-10-11.md) records commands, build identity, functional controls, live-oracle classification and final gate outcomes. The targeted fresh-build Python suite passed 526 tests. The static `make ci` portion of assembled `make verify` passed; the default Rust run stopped at PA-15 and made `verify` exit 2. The eight crates not reached by that stopping command passed separately: 5,710 tests passed, zero failed and six ignored across 66 test/doctest groups. A final isolated PA-15 retry passed after the broad run, without resolving the earlier two failures or producing a green full workspace run. Document, map, ledger and spelling checks passed at handoff. The audit phase made no commit, push, PR or cloud mutation. On 2026-10-12 the owner authorized publication of this documentation branch; publication checks are recorded in the evidence appendix.
