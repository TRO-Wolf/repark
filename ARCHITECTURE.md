# ARCHITECTURE.md — component boundaries + runtime flows

How repark is put together: the crate boundaries and dependency direction, then the three runtime
flows a change most often touches (session construction, query execution per door, write/commit).
It is **hand-written and descriptive** — for current *state* (what is delivered, what is deferred)
read [STATUS.md](STATUS.md); for the per-crate contracts (owns / does-not-own / inputs / outputs /
failure model) read each crate's `map.md` "Component contract" section; for the rules that govern a
change read [AGENTS.md](AGENTS.md).

## Component boundaries + dependency direction

repark is one Cargo workspace of nine crates layered on Apache DataFusion + Arrow + the owned
`iceberg-rust` fork. Dependencies point **one way, down the tiers** — no cycles, no door-to-door
edge, bindings reach inward only.

```
 tier 4  bindings            repark-python  (PyO3 cdylib `_native`; the only crate allowed `unsafe`)
                                  │  (reaches down; never depended upon)
 tier 3  doors +             repark-spark ──┐   repark-sql        repark-functions  repark-ta  repark-ml
         semantic profiles   (Spark door)   │   (ANSI/Trino door)  (Spark fns)      (TA UDFs) (ML kernels)
         + capability leaves       │        │        │
 tier 2  engine / session          └────► repark-core ◄───────────┘
                                          (ReparkSession, ExecutionBackend, SqlDialect/SessionExtension seams)
                                                  │
 tier 1  table service                      repark-iceberg
                                          (Glue + S3 Tables catalogs; Spark-semantics write adapter over the fork)
                                                  │
 tier 0  foundation                          repark-common
                                          (Error/Result seed + dialect-neutral SQL surface registry)
```

Rules that hold at every tier (the enforced ones point at their SSOT — prose never restates it):

- **No `repark-*` crate depends on a strictly higher tier; same-tier edges are allowed.** The tier
  map and allowed edges are enforced by `scripts/check_crate_dag.py` (`make check-crate-dag`, in
  `make ci` + the pre-commit hook). That script is the SSOT — read it for the exact edges; when
  you add a crate or an edge, change the script, not this file.
- **No door ↔ door edge, ever.** `repark-spark` and `repark-sql` never depend on each other; they
  share machinery only through tiers 0–1. That is what lets each door keep its own grammar. (Their
  test binaries may cross for cross-door equivalence — a dev-only edge, invisible to the product
  DAG.)
- **Bindings depend inward only.** `repark-python` reaches down to `repark-core` /
  `repark-functions` / `repark-ta` / `repark-spark` / `repark-ml`; nothing depends on it. It
  deliberately does **not** edge to `repark-sql` (no ANSI surface from Python) or `repark-iceberg`
  (Iceberg is reached only through `ReparkSession` and SQL text).
- **`repark-common` depends on nothing internal** — it is the bottom that keeps the DAG acyclic.
- **The table-format engine lives in the owned fork, not here.** `repark-iceberg` is a thin adapter:
  catalog wiring + a Spark-semantics write layer (MERGE INTO, append, overwrite, ALTER) over the
  fork; DELETE/UPDATE/INSERT are planned onto the fork's `iceberg-datafusion` `TableProvider`.

Crate-by-crate responsibilities: [crates/map.md](crates/map.md) and each crate's `map.md`.

## Runtime flow 1 — session construction

`ReparkSession` is built in **two phases**: a synchronous `build()` that does **no network I/O**,
then an async `register_configured_catalogs()` finalize that does the I/O. This split is what lets
an offline session never pay an AWS probe, and lets the PyO3 constructor `block_on` exactly one
async step.

```
ReparkSession::builder()
  .config(...)/.memory_limit_gb(...)/.batch_size(...)/.target_partitions(...)
  .with_sql_dialect(<door dialect>)          ← optional seam (default: stock DataFusion)
  .with_extension(<door extension>)          ← optional seam (default: no-op)
  .build()                                    ── SYNC, no I/O ──────────────────────────┐
      1. validate knobs (batch_size ≥ 1, target_partitions ≥ 1, memory ≥ 1 MiB or 0)   │
      2. parse catalog specs from `*.sql.catalog.<name>.*` (parse only; records         │
         `aws_signaled` — does NOT resolve credentials)                                 │
      3. assemble SessionConfig: DF-version regression guards, write/merge knobs,       │
         batch/partition setters, `datafusion.*` passthrough                            │
      4. extension hook 1 — configure(): install engine knobs as ConfigExtensions       │
      5. build RuntimeEnv: FairSpillPool (default 8 GiB; 0 = unbounded),                │
         object-list cache disabled (limit 0)                                           │
      6. create the DataFusion SessionContext                                           │
      7. extension hook 2 — register(): Spark function registry + analyzer rules +      │
         TA window UDFs (door-supplied; a bare session is stock DataFusion)             │
      8. wrap the context in SingleNodeBackend; pin the session-default SqlDialect;     │
         start an empty CatalogRegistry                                                 │
  .register_configured_catalogs()             ── ASYNC finalize ────────────────────────┘
      • resolve the AWS SDK credential chain ONCE, only if `aws_signaled` (else skipped)
      • register each configured catalog (memory / glue / s3tables) as a DataFusion
        CatalogProvider AND in the session registry (so the write path reaches the handle)
```

**Immutable after `build()`:** the `SessionConfig`, the `RuntimeEnv` (memory pool, caches), the
installed extension registrations, and the session-default dialect. **Still mutable at runtime:**
the `CatalogRegistry` — catalogs and namespaces can be registered after build (it is behind an
`RwLock`, snapshotted per query so no lock is held across an `.await`).

## Runtime flow 2 — query execution, per door

`session.sql(text)` routes through the session-default `SqlDialect`; a caller can also pass an
explicit dialect (`sql_with`) to drive two doors from one session. Either way the session builds a
per-call `EngineContext` snapshot (the DataFusion context + a catalog-registry snapshot + the
read-only-catalog set) and hands it to the door. Errors fold to the shared `repark_common::Error`
taxonomy at the session boundary, so every door reports the same error classes.

```
session.sql(text)
   └─► SqlDialect::execute(EngineContext, text)
         ┌──────────────── native door (repark-sql, AnsiDialect) ────────────────┐
         │ text guards (multi-statement FIRST) → merge-on-read valve              │
         │ → pre-parse rewrites (ALTER…EXECUTE refuse, branch/tag DDL,            │
         │   SET PROPERTIES, FOR…AS OF time travel)                              │
         │ → parse (stock DataFusion `Generic` dialect)                          │
         │ → match: intercept Iceberg catalog DDL (CREATE/DROP TABLE,            │
         │   CREATE/DROP SCHEMA, ALTER TABLE), lower MERGE, refuse set           │
         │   (INSERT OVERWRITE / CALL / TRUNCATE)                                │
         │ → else DELEGATE to DataFusion (SEC-02 local-fs guard between plan     │
         │   and execute); DELETE/UPDATE/INSERT ride the fork TableProvider      │
         └───────────────────────────────────────────────────────────────────────┘
         ┌──────────────── Spark door (repark-spark, SparkDialect) ──────────────┐
         │ write-to-branch sniff → metadata-table rewrite (t.snapshots →         │
         │   t$snapshots) → time-travel rewrite → multi-statement refuse         │
         │ → pre-parse intercepts (partition-field DDL, CREATE/DESCRIBE/SHOW      │
         │   NAMESPACE, branch/tag DDL)                                          │
         │ → parse (Databricks dialect + Spark-ism normalizers)                  │
         │ → match: CTAS, column-def CREATE TABLE, DROP TABLE, DROP NAMESPACE,   │
         │   ALTER, MERGE, INSERT OVERWRITE, CALL, TRUNCATE refuse               │
         │ → INSERT/DELETE/UPDATE PASSTHROUGH behind the P11 read-only-catalog    │
         │   guard + the merge-on-read valve                                     │
         └───────────────────────────────────────────────────────────────────────┘
   └─► both return a DataFusion `DataFrame`; nothing executes until a terminal
       action (collect / to_arrow / show / count) pulls batches.
```

On a parse **or** plan failure — and only then — the native door upgrades the error through a
wrong-door "sniff" that recognises a Spark-ism and steers the user to the other door; the original
error stays the first line. The two doors share **no** code path above tier 1: they meet only at the
Iceberg machinery (`repark-iceberg`) and the session (`repark-core`).

## Runtime flow 3 — write / commit

Two write families reach Iceberg, and they take different paths:

- **DataFusion-native DML** (`DELETE` / `UPDATE` / non-overwrite `INSERT`) is planned onto the
  fork's `iceberg-datafusion` `TableProvider` (ADR-0001). repark adds no write adapter here — it
  only guards it (read-only-catalog refusal, the merge-on-read multi-spec valve).
- **RePark-owned writes** (`MERGE INTO`, bulk `append`, `INSERT OVERWRITE`, `CTAS`) go through
  `repark-iceberg::write`, a thin Spark-semantics adapter over the owned fork:

```
plan + schema/type validation            (reject before any file is written)
        │
write data files                          (staged parquet; not yet visible)
        │
optimistic commit against the fork        (isolation → validation → commit action;
        │                                  MERGE is copy-on-write OR merge-on-read,
        │                                  per the fork ENGINE_CONTRACT §6)
        │
snapshot publish                          (ONE catalog publish; CTAS is create-or-replace —
        │                                  no drop-then-insert window)
        └── on failure: no partial snapshot is published; staged files are abandoned
            (INSERT OVERWRITE uses stage-then-swap so a failure leaves the prior data intact)
```

The heavy table-format engine (write actions, schema/partition evolution, snapshot management,
maintenance) lives **in the fork**, reached through its `ENGINE_CONTRACT`; `repark-iceberg` supplies
only the Spark-semantics surface and the RePark-owned MERGE executor. Catalog credentials are
resolved by the fork's Glue / S3 Tables builders at registration, per session.

## `ExecutionBackend` — what the seam is, honestly

`repark-core` routes execution through an `ExecutionBackend` trait. Today it has exactly one
implementation, `SingleNodeBackend`, and its whole surface is a single method that hands back a
**concrete DataFusion `SessionContext`**. It is best read as a **local execution-context holder and
a deliberately-minimal future extension point** — the *trait boundary* is the load-bearing part
(keeping the session behind it means a future distributed coordinator can slot in without reworking
the write path), **not** its current surface.

It is **not** evidence that distribution needs no wider change. Because the method returns a
`SessionContext` by reference, callers today can and do use single-node DataFusion facilities
directly; a real distributed backend would require widening this surface (and revisiting those
call sites), not merely adding a second `impl`. Distribution is deferred by decision (ADR-0004);
single-node DataFusion is the v1 target and handles the intended workload. The honest-doc status of
this seam is tracked in [STATUS.md](STATUS.md) "Architectural risks".

## Onward

- Current state (release, delivered/deferred, known issues): [STATUS.md](STATUS.md).
- Per-crate component contracts: each crate's `map.md` "Component contract" section, indexed from
  [crates/map.md](crates/map.md).
- The rules governing a change: [AGENTS.md](AGENTS.md). Load-bearing decisions:
  [docs/adr/](docs/adr/).
- Commands to build, test, and verify: [DEVELOPMENT.md](DEVELOPMENT.md).
