# repark-core Session API — the phase-1 design (settled 2026-08-06)

The deliberate design pass the port plan requires before phase-1 code lands. Provenance: three
independent designs (port-fidelity-first, server-first, surface-minimal) were produced against a
six-report recon corpus and scored by a three-judge panel (port-risk, future-proofing,
simplicity). **Port-fidelity anchors the synthesis** (anchored by 2 of 3 judges; the simplicity
judge's own recommendation held it as the contingency), with six grafts from the other two
attempts, recorded below where they land.

Governing facts from recon that make this design cheap: v1's phase-1 crate cone contains **zero
product-code global mutable state and zero product-code query-time env reads** except one (E-2,
fixed here). ADR-0004's disciplines force exactly two product-code changes; the phase cut (the
phase-2 crates not existing yet) forces two seam inversions and three type hoists. Everything
else re-homes byte-faithful. **Genuine rewrites: zero.**

## 1. Crate layout — three crates

```
crates/
  repark-common    (tier 0)  v1 repark-core, verbatim: Error / ErrorClass / Result seed.
  repark-iceberg   (tier 1)  v1 repark-catalog + v1 repark-write, merged as two independent
                             module trees (catalog/, write/); fork pin + ADR-0001 proof test.
  repark-core      (tier 2)  v1 repark-session re-homed = the Session-centric engine API,
                             plus CatalogRegistry/LocationPolicy/TimeTravelSpec + read_table_at
                             + reregister_catalog_provider hoisted from v1 repark-sql.
```

DAG: `repark-core → {repark-iceberg, repark-common}`, `repark-iceberg → repark-common`.

**Why repark-common is forced:** V2's repark-core (the Session) must call repark-iceberg
(builders, providers, knob installers), but v1's write code types its public surface on the v1
error seed. Seed inside repark-core ⇒ dependency cycle. The seed stays a leaf crate below
everything — v1's actual shape, renamed because V2 reserves `repark-core` for the Session crate.
repark-core re-exports it so bindings and doors import one crate.

**Deliberately not created:** no repark-exec, no repark-io (no v1 code exists for either;
execution config is ~40 lines inside `build()`; the AGENTS.md target-map rows get a dated
correction — orchestrator-scoped). No new frame-handle type. No plan IR. No dialect registry.

**Error boundary ported as-is, not unified** (accepted debt, omissions ledger O-1): the catalog
half keeps `datafusion::error::Result` (v1's documented no-core-dep choice); the write half keeps
its v1 mix (`repark_common::Error` for merge/append/overwrite, `iceberg::Result` for
alter/snapshot_refs); the fold stays in repark-core's `error_map.rs`. Retyping ~2k lines + 24+
tests during the port would confound the census gate.

## 2. The Session type

Names port verbatim: **`ReparkSession` / `ReparkSessionBuilder`** (courtesy alias
`pub use ReparkSession as Session`). All state `Arc`-shared, `Clone` cheap, manual non-leaking
`Debug` — exactly v1.

**Builder:** v1's knob surface unchanged (`config`, `configs`, `memory_limit_gb/_bytes` with the
1 MiB floor and 8 GiB default, `batch_size` fail-loud-on-0, `target_partitions`) plus the two
phase-cut seam slots: `with_extension(Arc<dyn SessionExtension>)`,
`with_sql_dialect(Arc<dyn SqlDialect>)`.

**`build()` keeps v1's exact order**, with the two inline phase-2 call sites replaced by
extension hooks at the same positions: validate knobs → `parse_catalog_specs` fail-loud →
`resolve_s3_region_override` (conf only) → write knobs from the config map → `SessionConfig` +
the DF-pin regression flag → knobs installed as ConfigExtensions via `repark_iceberg::write::*`
(the server-ready conf transport, verbatim) → **`ext.configure(conf, config)` hook** →
RuntimeEnv (FairSpillPool 8 GiB default; object-list cache disabled — both load-bearing defaults
port with their comments) → `SessionContext::new_with_config_rt` → **`ext.register(ctx)` hook**
→ assemble.

**Two-phase lifecycle kept and promoted to the documented contract:** sync, env-free `build()` +
async `register_configured_catalogs()` finalize (v1 method names, including
`register_late_configured_catalogs` for the getOrCreate reuse path). PyO3 block_ons finalize
today; a Flight SQL handler awaits it at session open. Fully-async construction was judged an
unforced reshaping of every ported construction call site.

**Credentials + FileIO (E-2, the one ADR-forced product edit — with the conditional graft):**
catalog credentials stay exactly v1 (resolved inside the fork's builders at registration, held
per-session; RePark never touches credential values; per-location FileIO from the owning
catalog's props). The fix: v1 resolved the AWS SDK chain lazily at first S3 *path-read* — a
query-time env read. V2 resolves it in `register_configured_catalogs()`, stored on the session
(`aws_sdk_config: Arc<OnceLock<SdkConfig>>`), **and only when the session signals AWS use** (an
AWS-backed catalog spec, an S3-region/`fs.s3a.`-style conf key, or an explicit opt-in) — so
offline sessions never pay an IMDS probe. An S3-path read on a session that never resolved fails
loud, naming the missing step. Gate tests cover both sides. *(Graft: surface-minimal's
conditional guard, endorsed by two judges, discharging the anchor's own R-3 risk.)*

**Runtime ownership — the engine owns none:** every engine entry point is async; repark-core
never creates a runtime, never block_ons. The embedding supplies the runtime (PyO3's shared
runtime in phase 3; a server brings its own), preserving v1's no-nested-block_on streaming
property. An `EngineRuntime` type was considered (server-first) and **deferred to phase 3** —
it becomes engine API the day the binding ports, additively; building it in phase 1 is unused
surface (omissions ledger O-5, named slot in the server landing map).

**Catalog registration:** verbatim dual registration (DataFusion side via
`register_iceberg_catalog` with the incremental provider; engine side into `CatalogRegistry`
with `LocationPolicy`). `CatalogKind::Postgres` still parses (test fidelity) but registration
fails loud `NotImplemented` until repark-connect exists.

**E-4 fixed now (graft, my ruling):** `LocationPolicy::TempFallbackAllowed` becomes
`TempFallbackAllowed { root: PathBuf }`, resolved once at `register_memory_catalog` time —
removing the phase-2 CTAS-time `std::env::temp_dir()` env read *before* the type becomes public
phase-1 API. Phase-2's re-home consumes `root` instead of calling temp_dir (a one-line edit it
owed anyway). Bounded edit: the hoisted registry tests adjust one variant construction.

## 3. The internal engine API

One API, exported by repark-core. Bindings import **repark-core only** (v1's binding already
proves this shape); doors import repark-core + repark-iceberg.

**Frame handle = DataFusion `DataFrame`, re-exported. No wrapper.** v1's binding wraps it thin
(handle + runtime + schema memo) and streams via `__arrow_c_stream__` per batch. A newtype now
would rewrite every ported test and the phase-3 binding for an abstraction v1 never needed.
Revisit deadline: the first tagged release, when the API-forever clock starts (ledger O-6).

**SQL entry + dialect seam** (phase-cut inversion; **EngineContext graft** from server-first —
struct-extensible instead of positional args, so seam growth is non-breaking):

```rust
#[non_exhaustive]
pub struct EngineContext<'a> {
    pub ctx: &'a SessionContext,
    pub catalogs: &'a CatalogRegistry,     // per-query snapshot
    pub read_only: &'a HashSet<String>,
}
#[async_trait]
pub trait SqlDialect: Send + Sync {
    // Field set mirrors v1 execute_with_read_only(ctx, catalogs, query, read_only) exactly.
    // FROZEN 2026-08-08 (phase-2 PR-6) — see "Seam freeze" below.
    async fn execute(&self, cx: EngineContext<'_>, query: &str)
        -> datafusion::error::Result<DataFrame>;
}
pub struct DataFusionDialect;   // phase-1 default: plain ctx.sql(); reads/temp views work,
                                // and DELETE/UPDATE/INSERT already ride the fork TableProvider.

impl ReparkSession {
    pub async fn sql(&self, query: &str) -> Result<DataFrame>;              // session dialect
    pub async fn sql_with(&self, d: &Arc<dyn SqlDialect>, q: &str) -> Result<DataFrame>;
}   // sql_with = two doors sharing one session (ADR-0002 "one test row per door")
```

**Registration seam:** `SessionExtension { fn configure(&self, SessionBuildConf<'_>,
SessionConfig) -> DFResult<SessionConfig>; fn register(&self, ctx) -> DFResult<()> }` — both
defaulted (the trait-wrapping both-sides audit applies). *`configure`'s first argument was the bare
conf map as shipped in phase 1; it became `SessionBuildConf` (the map PLUS the values `build()` has
already resolved) on 2026-08-10 — see the superseding note linked from the seam freeze below.* Phase-2 repark-spark ships one extension holding
exactly what v1 inlined (function registry + analyzer rules + TA UDFs + cardinality config).
Consequence, stated plainly: the phase-1 native core has DataFusion semantics — Spark semantics
are the Spark door's extension by definition.

### Seam freeze (2026-08-08, phase-2 PR-6)

`SqlDialect::execute(EngineContext<'_>, &str)` and `SessionExtension` are **FROZEN** as shipped
in phase 1 — the status flips from UNSTABLE here, per
[sql-doors.md](sql-doors.md) §3. Both phase-2 doors (`repark-spark`, `repark-sql`) implement them
unchanged; there are no core-side pre-execution hooks, and guards are door-called. `EngineContext`
is `#[non_exhaustive]`, so adding a field stays non-breaking; changing or removing a method or an
existing field now requires a superseding design note.

**SUPERSEDED IN PART, 2026-08-10 —
[session-extension-conf-seam.md](session-extension-conf-seam.md).** `SessionExtension::configure`
now takes a `SessionBuildConf<'_>` (the builder conf map PLUS the values `build()` already resolved
from it, today the session timezone) instead of the bare map. That note is the amendment the
sentence above requires: it prices the break at three in-tree implementors and no external ones,
and records the two rejected alternatives. `SqlDialect::execute`, `SessionExtension::register` and
the session-scoped-not-dialect-scoped rule below are **unchanged and still frozen**.

**Extensions are session-scoped, not dialect-scoped.** A Spark-extended session has Spark
expression semantics through **every** door, including the ANSI one — the extension's function
registry and analyzer rules are installed on the `SessionContext`, which every dialect receives.
Three consequences, all load-bearing:

1. Cross-door equivalence evidence needs **two sessions** (design §2 Q13 / graft G5) — a native,
   extension-less session driven through `AnsiDialect` and a Spark-extended session driven
   through `SparkDialect` — compared on the Arrow path, value AND type. `sql_with` on one session
   is legal only for surfaces the analyzer/UDF layer cannot touch (pure DDL/catalog ops).
2. A door's own matrix row may not claim evidence gathered on a session carrying another door's
   extension; both doors' matrices enforce this in a test.
3. Door-neutral extensions compose the same way: `repark_ta::TaExtension` on a NATIVE session
   makes the `ta_*` window UDFs callable through the ANSI door (design §2 Q11).

Pinned by `crates/repark-sql/tests/cross_door.rs::extensions_are_session_scoped_not_dialect_scoped`.

The R2 config-plumbing fix that PR-6 landed in `repark-core` (the builder's `datafusion.*` keys
reaching `SessionConfig`, so `information_schema` is enable-able) is **not** a seam change: it
touches neither trait, and both doors reach it only as ordinary session configuration.

**The rest of the surface ports verbatim** (bodies byte-faithful, call paths re-prefixed):
lifecycle (`builder()`, `new()`, finalize pair, `context()` escape hatch — kept public, the
phase-2 doors receive `&SessionContext` exactly as v1); catalog ops (`register_iceberg_catalog`,
`register_memory_catalog`, `create_namespace` with the location/location_uri mirror,
`table_exists`, the three listing families, `refresh_catalog_provider`,
`note_local_write_root`); readers (`read_parquet/csv/json` — S3 paths use the session-held
SdkConfig; `read_iceberg_table` + `TimeTravelOpts` → hoisted `read_table_at`); the temp-view
family (all six methods incl. the Arrow-IPC path); the `testing_`-seams (gaining `#[doc(hidden)]`
only); the error surface (`Error`/`ErrorClass`/`Result` re-export + `engine_err`).

**Write/DML entry:** bindings write through `sql()` only (v1 parity). Doors consume the
repark-iceberg public surface — pinned name-stable in phase 1 as a deliberate API commitment
(merge/append/overwrite/alter/snapshot-ref entrypoints, `file_io_for_location`, provider
rebuild/reregister family) so the phase-2 port is import-path rewriting. MERGE stays
RePark-owned; DELETE/UPDATE/INSERT delegate to the fork; `engine.operation-id` stamping and the
ENGINE_CONTRACT §5 validation recipes port untouched. An `AppendReport` commit receipt
(operation-id + snapshot-id) is a named phase-2+ question (ledger O-4), not phase-1 code.

## 4. ExecutionBackend — the exact boundary

Ports verbatim: `trait ExecutionBackend { fn session_context(&self) -> &SessionContext; }` +
`SingleNodeBackend`. The backend owns *where planning and compute run* — nothing else. Catalogs,
credentials, FileIO, commit, session config, temp-view policy never cross the seam; Iceberg
write/commit is never serialized through it (a future distributed backend distributes scan/
compute only; commit stays coordinator-side). The trait grows only when a second backend exists —
no speculative `execute_stream` widening (rejected from server-first as an ADR-0004 §3
contradiction).

## 5. The complete forced-edit ledger

Product-code deltas from v1, in full — everything else is re-home:

1. **E-2**: AWS chain resolution moves from lazy path-read time to conditional finalize (§2).
2. **Dialect inversion**: `sql()`'s one call into v1 repark-sql becomes `dialect.execute(…)`
   with a mirrored signature; `DataFusionDialect` is the phase-1 impl.
3. **Extension inversion**: v1's inline functions/TA/analyzer/cardinality registration in
   `build()` becomes the two `SessionExtension` hooks at the same positions.
4. **E-4**: `TempFallbackAllowed` gains `{ root: PathBuf }`, resolved at registration (§2).
5. Mechanical renames: `repark_catalog::` → `repark_iceberg::catalog::`, `repark_write::` →
   `repark_iceberg::write::`, `repark_core::` (v1) → `repark_common::`, `repark_session::` →
   `repark_core::` — the four prefix rules that also generate the census rename map.
6. **Test-harness merge (added 2026-08-06, PR-B assembly STOP):** merging two v1 crates into one
   test binary breaks the per-binary global-tracing-subscriber invariant both v1 harnesses
   relied on. Fix: one shared `cfg(test)` tracing harness installing a single global subscriber
   carrying BOTH layers (catalog span capture + merge span recorder); exactly the two v1 install
   call sites edited to use it; all test assertions byte-unchanged. Same-class hazard applies to
   the PR-C session tests — audit for global installs before assembly.
   Procedural companion ruling: where a prefix rewrite pushes a v1 line past 100 columns, run
   the fidelity check pre-fmt (recorded), then `cargo fmt`; enumerate every reflow site in the
   unit ledger.

Deferred with their crates (v1 stays authoritative): `read_excel`/`read_postgres` + their error
folds, postgres catalog registration body, SQL-text time-travel rewriting, the whole v1
repark-sql/functions/ta/ml/postgres/excel/python set.

## 6. Door-readiness and the server landing map

**Phase-2 Spark door:** implements `SqlDialect` with v1's `execute_with_read_only` body (the
seam mirrors its signature), ships the `SessionExtension` with v1's registration code, consumes
repark-iceberg under v1 names. **Phase-2 native door:** a second `SqlDialect` impl (its own
design pass, per the port plan). **Phase-3 binding:** import-path rewriting + the facade — type
names, method names, lifecycle, and frame handle are all preserved for exactly this reason.

**Server landing map** — named additive slots, deliberately NOT built in phase 1 (ADR-0004
defers them; building them now would be unfalsifiable surface): `SessionId` + session registry;
`close()`/lifecycle; per-session credential vending (the ADR's deferred problem #1);
`EngineRuntime` (phase 3, with the binding); cancellation/`ActiveQueries` (server milestone,
needs a real server to test against). Each lands additively on this API — that is the ADR-0004
success criterion this design was checked against.

## 7. Census accounting and the port procedure

- Phase-1 ported tier-1 tests: repark-common 2 + repark-iceberg 241 (catalog 50, write 191;
  corrected 2026-08-06 from grep-based recon counts by the generated `--list` at the pin) +
  the hoisted read_table_at/registry tests + the untangled session subset (audit below).
- The crate merges ship as **declared-rename units** with a **mechanically generated** old→new
  test-name map: `cargo test --workspace -- --list` at the pinned v1 SHA → apply the four prefix
  rules → diff against V2's list must be empty. Never hand-written.
- **Session-test audit (the biggest census risk):** the 49 session tests + 26 catalog-config + 4
  object-store tests split mechanically into port-now vs deferred; 24 call `.sql()` and an
  unknown subset needs phase-2 interception. The audit decides per test — some pass under
  `DataFusionDialect` (plain reads/temp views); none get `#[ignore]`.
- **Deferred-test manifest (graft):** a checked-in manifest lists every v1 phase-1-cone test NOT
  ported, with its target phase — so (ported ∪ deferred) = v1-total is auditable at every phase
  boundary, not just at the phase-3 census.

## 8. Omissions ledger — resisted improvements, with revisit triggers

| # | Resisted | Revisit trigger |
|---|---|---|
| O-1 | Error-boundary unification (write half's `iceberg::Result` mix) | dedicated post-census unit |
| O-2 | CatalogRegistry concern-split (SEC-02 roots / P11 postgres names ride along) | phase-2 doors that consume them |
| O-3 | `OPERATION_ID_PROP` hoist out of `merge/mod.rs` | a maintenance/`CALL` backend actually landing |
| O-4 | `AppendReport` commit receipt | phase-2 write-path port |
| O-5 | `EngineRuntime` type | phase-3 binding port (replaces the process-wide runtime singleton) |
| O-6 | Frame-handle newtype over DataFusion `DataFrame` | first tagged release (API-forever clock) |
| O-7 | Fully-async one-phase `build()` | server milestone, only if session-open latency demands it |

## 9. Open items carried into the execution brief ([phase-1-engine-core.md](../history/port-v2/phase-1-engine-core.md), archived)

- **Port-source pin (R-1):** v1 main vs the r27 branch differ in the phase-1 cone only by the
  move-only merge-module split; the design assumes the split shape. The brief pins one SHA before
  the literal-copy commit — operator decision.
- **Fork-shim staleness (R-4):** before copying, re-check at the pinned fork rev whether the
  metadata-projection shim is still needed, and re-run the trait-wrapping both-sides audit on the
  namespace-scoped catalog wrapper.
- **Governance corrections (orchestrator-scoped):** AGENTS.md exec/io target-map rows; the
  todo.md check_lib_py phase mislabel; CARGO_EMPTY deletions and workflow arming are `.github/`-
  and governance-touching and stay out of delegated hands per the standing rules.
