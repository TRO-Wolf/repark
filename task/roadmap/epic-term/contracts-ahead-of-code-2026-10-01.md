# Contracts ahead of code — the crate contracts, the enterprise seams, and Delta Lake (ruled 2026-10-01)

**Date:** 2026-10-01 · **Ruled by:** the owner ("let's go ahead and get it added to the plan"), in discussion with a Claude session (claude-fable-5-1) · **Measured on:** `origin/main` at `daf9bbaa` (the third external review's base) and the `docs/roadmap-adjustments-2026-10-01` branch at `3fc0ba92` · **Builds on:** [crate-layout-1-8-2026-10-01.md](crate-layout-1-8-2026-10-01.md) (the layout, CL-1…CL-8), [unified-database-query-cdc-silver-plan-2026-09-13.md](unified-database-query-cdc-silver-plan-2026-09-13.md) (the three lifecycles, the capture checkpoint), [deterministic-silver-layer-compiler-2026-09-12.md](deterministic-silver-layer-compiler-2026-09-12.md), [release-roadmap-2026-08-29.md](release-roadmap-2026-08-29.md) (rows 1.6, 1.7, 2.x, 3.0).

The five arriving crates are pre-declared in the DAG gate and the manifest (CL-8) but have no code. This file rules the **contracts between them before the first unit opens**, so that each arrives onto decided boundaries rather than deciding them in its first pull request. Three external reviews shaped it: the Sail engineering review (plan quality, observability, complete workloads), the Supabase ETL review (recovery contracts for capture), and the eight-item review of the first draft of these contracts, checked against `daf9bbaa`, whose revisions to items 2, 4 and 7 are adopted below. Part B applies the same method to what an enterprise buyer asks for: plan the seam where it is cheap now, reserve the name, write the refusal, and let demand open the card.

## Part A — the crate contracts

### CC-1 · Core consumes the services; Session owns embedded capture

`repark-core` reaches down to `repark-connect`, `repark-cdc` and `repark-io` as each arrives, through the seams that already exist: `SessionExtension` (`crates/repark-core/src/extension.rs`: `configure`, `configure_analyzer_rules`, `register`) mounts providers and rules at build time; `ExecutionBackend` and `SqlDialect` stay the only other extension points. `SessionExtension` is a registration hook, not a lifecycle: a long-running capture task needs a handle the Session owns. In embedded mode that handle obeys four rules, decided here and implemented at 1.7:

1. Registering a source does not start capture.
2. Explicit asynchronous shutdown waits for owned work and reports the durable checkpoint.
3. A shutdown timeout, or an ambiguous publication, produces an explicit recovery-required outcome, never a silent success.
4. Dropping a Python object is never the only shutdown mechanism.

The checkpoint outlives the Session; service mode (1.12) owns the same capture implementation later. **Pushdown is rendered as what the statement actually pushed**, per source, including residual filters kept engine-side, not as what the connector supports in general. That rendering is the `EXPLAIN` boundary the 1.6 acceptance line names.

### CC-2 · Shared identity, the narrow T-6

Measured on `daf9bbaa`: the loader's `SourceKind` / `SourceSpec` / `ProfileSources` are `pub(crate)` in `crates/repark-core/src/config_file/sources.rs`, distinct from the public handles `SourceRow` / `NamedSource` in `named_sources.rs`. They do not move together. The split:

| crate | owns |
|---|---|
| `repark-common` | the minimal source identity (configured name, kind, **a source generation**) and genuinely shared value types |
| `repark-connect` | connection settings and database-specific type conversion |
| `repark-core` | profile loading, precedence, registration, the user-facing handles |
| `repark-cdc` | replication lineage, offsets, capture state |

**A configured name is not a durable CDC identity.** Re-pointing `orders` at a different database must not reuse the previous database's checkpoint; the generation field in the identity type is where that is detected, which is why it moves to `repark-common` with the name rather than later. A narrow `cdc → connect` same-tier edge is allowed only if connection code is genuinely shared; replication connections stay separate from query pools (unified plan line 56). T-6 is scheduled **with the first connector consumer** (C-1 of §A.9), not ahead of it: the boundary is ruled now, the move waits for its caller.

### CC-3 · Configuration: four reserved prefixes, no keys

`repark.connect.*`, `repark.cdc.*`, `repark.crawler.*` and `repark.io.*` are reserved beside the families on `main` (`repark.sql.*`, iceberg, text, memory, write, hadoop, display, merge, aws, scan). No key is defined until its unit lands. The **owner crate interprets and validates** its settings; `repark-core` loads, merges and transports them and never interprets a value it does not own. The named-source format from CFG-2 (`[<profile>.database.<name>.*]`) stays the one source format. A `repark.connect.*` session default and a source entry are the same key family, **and the source entry wins**, so the prefixes do not become a second configuration system.

### CC-4 · Errors: operational meaning first, Spark wording where measured

Connector and capture errors carry structured operational meaning: authentication failure, retryable disconnect, lost replication history, invalid specification, ambiguous destination commit. A Spark condition is rendered only where the semantics match and the facade contract has been measured; otherwise the condition is RePark-owned, on the precedent of `CommitStateUnknown` (already in `repark-common/src/lib.rs`, the PyO3 exceptions and the Python taxonomy). Checkpoint loss is never forced into an approximately similar Spark class. There is no blanket carve-out: **each RePark-owned condition gets a dated registry row**, as a declared refusal does, so the extension boundary stays enumerable and cannot excuse an unrelated parity failure.

### CC-5 · Features: Cargo modularity is not wheel availability

"In the standard wheel" does not require "no Cargo feature". Postgres query and capture ship in the standard wheel when delivered, behind features that stay available to Rust consumers (the `repark-distributed` precedent: `default = ["local"]`, `cluster` opt-in). **SQL Server ships through pip as an opt-in** (owner, 2026-10-01: "ship it as an option in pip"). A pip extra cannot switch compiled code inside one wheel, so the C-0 measurement (build time, wheel size, linking, platforms) decides the mechanism: compiled into the standard wheel with the extra carrying only Python dependencies, or a sibling `repark-mssql` wheel. A compiled capability omitted from the wheel cannot be enabled by a runtime setting, which is the reason the default leans inclusive.

### CC-6 · Postgres tests: a disposable container, and the five failure scenarios

Live cells run against a disposable local Postgres container, never an Airflow-managed database without established ownership and isolation. Private schemas are not enough for capture tests: **publications and replication slots get unique names and explicit cleanup**. The failure harness covers destination writes, checkpoint persistence and reconnects, plus five scenarios that are the pin list of `repark-cdc`'s S0 and exist before any capture code:

1. Crash after the destination commit, before checkpoint persistence.
2. Snapshot-to-WAL handover under concurrent writes.
3. Replay and duplicate delivery.
4. Schema changes and partial update images.
5. Lost slots or unavailable WAL.

Postgres supplies the source behaviour; RePark's assertions establish separately that acknowledged progress stays recoverable. The live-cell rules (guard the shared session, single-file seed, private catalog, no environment pop, prove co-collected) apply unchanged.

### CC-7 · Metrics: crate-owned emission, entry-point initialization

Each crate owns its metric meanings and bounded labels and emits through the `metrics` facade. **Exporter initialization belongs to the embedding application or service entry point**, with an explicit Python entry point beside the env-gated, process-wide `try_init_repark_tracing` (`crates/repark-python/src/lib.rs`), which is a precedent to inspect, not the model. The per-query report reads query-scoped measurements from the plan's `MetricsSet` directly: subtracting global counters fails under overlapping queries, and query ids as labels are unbounded cardinality. No `repark-telemetry` crate through 1.8; a module in `repark-common` until shared exporter or lifecycle code earns one.

### CC-8 · Denied edges, in the gate, with kinds

Explicit denials go in `scripts/check_crate_dag.py` beside `ALLOWED_EDGES`, each naming the dependency kinds it covers, and only where they protect a decision no role or tier rule already implies. Four rows, all four kinds (normal, dev, build, optional):

| denied | protects |
|---|---|
| `repark-python → repark-sql` | the bindings reach the doors through `repark-core`'s dialect seam only |
| `repark-python → repark-iceberg` | the bindings never reach a table service directly |
| `repark-functions → repark-spark-dialect` | the function leaf never learns the grammar (layout ruling §3.3); a dev edge would let a test smuggle it in |
| `repark-crawler → repark-cdc` | discovery never publishes and never reads capture state (the three lifecycles) |

`repark-cdc → repark-core` was proposed and dropped: the layering rule already forbids a tier-1 crate from reaching tier 2. The table lands as a small chore on the gate script in the tidy window, not in this docs change.

### CC-9 · Capture shutdown and checkpoint advancement: the medallion's first decision

The most consequential open contract across all three reviews is the same one seen from two sides: the Supabase review's accepted-versus-durable distinction and the eight-item review's shutdown rules. It is filed once, as the first D-decision of the 1.7 charter, with these clauses: an event is *accepted* when the producer has it and *durable* when its Bronze outcome is committed; recovery advances on durable only; the four shutdown rules of CC-1; and **a checkpoint written by one producer generation is never advanced by another**. Diffing RePark's Bronze stream against Supabase ETL's output on the same source is a measurement, never a dependency.

### CC-10 · Delta Lake: the seam, not the feature

An enterprise buyer is often a Databricks shop whose data is in Delta. The ask is "read what we have and help us land it in Iceberg", not "be our Delta writer". Delta write support would be a second table service with its own log, checkpoints, deletion vectors and `MERGE`, doubling an Iceberg-only parity matrix of 842 cells, against an oracle Databricks defines. Ruled:

- **`repark-delta` is a reserved name**, a tier-1 table service peer of `repark-iceberg`, listed in the layout ruling's §4 as *not before 2.x, demand-triggered*. It is **not** pre-declared: CL-8 pre-declares crates with a release, and this one has none.
- **A dated refusal row for `format("delta")`** on both doors, with Spark's error class, so the registry says what RePark does today.
- **A 2.x card, demand-triggered, read-only**: `delta-rs` exposes a DataFusion `TableProvider`, so a scan is cheap in code; the real cost is version lockstep, since `delta-rs` pins its own DataFusion and the workspace pins 54.1, the problem the Ballista audit measured. Writes stay out of the card.
- **Bronze and Silver stay Iceberg-only publication targets.** The `SilverPlan` compiles over a pinned Iceberg snapshot; a Delta source enters only through a read scan or capture into Bronze.
- **The Databricks route needs no Delta code**: Unity Catalog serves UniForm-enabled Delta tables over the Iceberg REST protocol, and the fork's `iceberg-catalog-rest` is complete. The REST slot was ruled the same day: **1.7**, inside the medallion release (owner: "let's get the REST catalog moved to 1.7"). Delta-to-Iceberg metadata migration in the manner of Apache XTable, Iceberg metadata written over the same Parquet files, is the enterprise lever that pulls customers toward the format RePark owns.

The release roadmap's "any second table format" in its explicitly-not-planned list now reads "as a publication target"; read-only interop is this card.

## Part B — the enterprise seams

Same method: where a buyer will ask and the seam is cheap now, reserve it; where it is not, write the refusal or the demand-triggered row so it can be said in a sales call. Everything below was checked against the 2.x and 3.0 rows so that nothing duplicates what is already planned (authentication, grants with row filters and column masks in the planner, quotas, the audit table, signed images, secrets providers, observability, the maintenance policy, multi-writer, Flight SQL, Substrait).

| id | seam | what the buyer asks | ruled now | the card |
|---|---|---|---|---|
| ES-1 | **Connector auth methods** | password auth to SQL Server or RDS is rarely allowed | an auth-method field on `repark-connect`'s connection settings, reserved in C-1; password at 1.6; IAM token (RDS) and Kerberos / Active Directory as **declared refusals** with a dated row | 1.6 (field), demand-triggered (methods) |
| ES-2 | **Azure and GCS object stores** | multi-cloud warehouses | the workspace builds `object_store` 0.13 with only the `aws` feature; ADLS Gen2 and GCS are features of the same crate behind the same `CredentialProvider` bridge; `repark.azure.*` and `repark.gcs.*` reserved beside `repark.aws.*`, no keys | demand-triggered; the cost is a live-cell tier per cloud like `docs/tier2-aws.md`, not code |
| ES-3 | **Iceberg table encryption** | keys applied, not stored | ENC-1 in the parity registry records that `encryption.key-id` is stored and never applied; the feature is a fork item filed with 3.0 authentication; **ruled 2026-10-01: refuse** — the first write to a table carrying `encryption.key-id` refuses, as Spark does without a KMS, and `CREATE` keeps succeeding as in Spark; a product card, release the owner's, flipping the ENC-1 pin on purpose | the refusal next; encryption 3.0 |
| ES-4 | **Lineage emission** | OpenLineage into Collibra, Atlan, DataHub | the 3.0 audit row already records principal, sources and snapshot ids, which is the OpenLineage event shape; an emitter is one exporter over the CC-7 facade | 2.7 sub-item |
| ES-5 | **Compliance erasure and legal hold** | right-to-erasure with proof; records that may not expire | erasure = delete, rewrite, expire, orphan sweep and an audit record as one procedure; legal hold = an Iceberg tag the maintenance policy refuses to expire; both over procedures that exist | 2.1 sub-items |
| ES-6 | **More connectors** | Oracle, MySQL, Snowflake, BigQuery | behind `repark-connect`'s provider trait; the `format("jdbc")` URL dispatch row (2026-09-15) refuses by scheme; cloud warehouses through ADBC | demand-triggered rows, no card |
| ES-7 | **Airflow provider** | schedule RePark from the orchestrator they run | a sibling repository on the `dbt-repark` model, engine side unchanged | demand-triggered |
| ES-8 | **Unity Catalog, Hive metastore** | read the catalog they have | Unity through its Iceberg REST endpoint (CC-10, the REST campaign); Hive metastore stays out (ruled 2026-08-29) | 1.7 (the REST campaign, ruled 2026-10-01) |
| ES-9 | **Supply chain** | an SBOM and signed artifacts on every security questionnaire | CycloneDX output beside the `cargo-deny` job already in CI; signed wheels join the signed image | 3.0 deployment row |
| ES-10 | **Declared out, in writing** | high availability, cross-region replication, FIPS-validated crypto | HA and replication: single node is the thesis and table replication is a catalog concern; FIPS: RustCrypto is not a validated module | the explicitly-not-planned list |

## Part C — decisions recorded

| id | decision | status |
|---|---|---|
| CC-1 | core consumes the services through the existing seams; Session owns embedded capture under the four shutdown rules; `EXPLAIN` renders what was pushed, with residuals, per source | **ruled 2026-10-01** |
| CC-2 | the narrow T-6: identity with a generation field to common, connection settings and conversions to connect, loading and handles stay in core, lineage and offsets in cdc; scheduled with C-1 | **ruled 2026-10-01** |
| CC-3 | four reserved prefixes, no keys; owner crate validates, core loads and merges; the source entry beats the session default | **ruled 2026-10-01** |
| CC-4 | structured operational meaning first; Spark wording where measured; RePark-owned otherwise with a dated registry row each | **ruled 2026-10-01** |
| CC-5 | Postgres query and capture in the standard wheel behind retained features; SQL Server a pip opt-in, the mechanism decided by the C-0 measurement | **ruled 2026-10-01** (owner: "ship it as an option in pip") |
| CC-6 | disposable container, unique publication and slot names with cleanup, the five scenarios as cdc S0 pins | **ruled 2026-10-01** |
| CC-7 | `metrics` facade, crate-owned emission, entry-point initialization with an explicit Python entry, plan-scoped per-query report, no telemetry crate through 1.8 | **ruled 2026-10-01** |
| CC-8 | four denied edges with kinds in the gate script; `cdc → core` dropped as a duplicate; lands as a tidy-window chore | **ruled 2026-10-01**; the chore is open |
| CC-9 | capture shutdown and checkpoint advancement filed as the 1.7 charter's first D-decision, with the generation clause | **ruled 2026-10-01** |
| CC-10 | `repark-delta` reserved, not pre-declared; `format("delta")` refusal row; a read-only 2.x card on demand; Bronze and Silver Iceberg-only; the Databricks route is UniForm over Iceberg REST | **ruled 2026-10-01** |
| ES-1…ES-9 | the enterprise seams as tabled | **ruled 2026-10-01** ("get it added to the plan"); ES-3 ruled **refuse**, ES-8's REST slot ruled **1.7** the same day |
| ES-10 | HA, replication and FIPS declared out | **ruled 2026-10-01** |

## Part D — what this change touches elsewhere

- [crate-layout-1-8-2026-10-01.md](crate-layout-1-8-2026-10-01.md): the role word is the gate's `capability`, not "tool"; the header cites design-plan cards 1.3 and 1.6 and the unified plan for the 1.7 crates (card 1.7 is dbt); `silver/` is on `main` already (S-0 and S-1, five modules), so the arrival reads "S-2 onward at 1.7"; §4 gains the `repark-delta` row.
- [roadmap-design-plan-2026-08-29.md](roadmap-design-plan-2026-08-29.md) card 1.6: Trino struck (1.10 since 2026-09-13), `SourceSpec` from CFG-2, the CC-2 module split, the ES-1 auth-method field.
- [release-roadmap-2026-08-29.md](release-roadmap-2026-08-29.md): the 1.6 row's "1.8's CDC" is 1.7's; the explicitly-not-planned list carries the CC-10 and ES-10 wording; the 2.1, 2.7 and 3.0 rows carry ES-5, ES-4, ES-3 and ES-9; a Q&A row for this ruling.
- `ARCHITECTURE.md`: one sentence naming the five pre-declared arrivals, since the gate declares fifteen crates while the map draws ten.
- `docs/artifacts/contracts-ahead-of-code-2026-10-01.html`: the artifact copy.

## Leaves this directory when

C-1 has landed with the CC-2 move, the CC-8 chore has merged, and the 1.7 charter carries CC-9 as its first decision; then this file is the dated record and the cards name the contracts.
