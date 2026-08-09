# Phase-1 PR-C ledger — repark-core (Session)

> **ARCHIVED 2026-08-09** (Front-Door FD-4) — a historical record of the v1 → v2 port, kept for
> provenance and **not a source of live rules**: every rule still in force was promoted to a
> current document first ([promotion-ledger.md](promotion-ledger.md)). Relative links were
> repaired for this location on the same date; nothing else changed. Current state:
> [STATUS.md](../../../STATUS.md).

Status: MERGED 2026-08-07 (PR #6, `c05bc31`; #5 auto-closed at #4's branch deletion; archived 2026-08-09).
Port-Source: `fc3f48102e437e2843ded460bc161edb434dac93` (v1 main, #141 squash).
Depends on PR-A (workspace + repark-common) and PR-B (repark-iceberg, branch base `ffee1d0`).

## Scope

Re-home v1 `crates/repark-session` as V2 `crates/repark-core` (the Session-centric engine API,
tier 2) per `docs/design/session-api.md` §2–§5: module split (v1 lib.rs body → `src/session.rs`
+ the six support modules), three hoists from the v1 SQL crate (`catalog_state.rs`,
`time_travel.rs` + `read_table_at` + tests, `TimeTravelSpec`), the two phase-cut seams
(`dialect.rs`, `extension.rs`) with new-seam tests, the four forced edits (E-2, dialect
inversion, extension hooks, E-4), and the session-test audit (port-now 68 / deferred 18).
Out of scope: read_excel/read_postgres + their folds, postgres catalog registration body,
SQL-text time-travel rewriting, all phase-2 crates.

## Commit series (as landed)

Ledger-plan commits 1+2 landed FOLDED, and the wiring is staged-then-wired — the copied v1
session.rs body references the phase-2 crates (`repark_sql`/`repark_functions`/`repark_ta`/
`repark_postgres`) until ALL four forced edits apply, so no tree with a *declared* session
module compiles before then. To keep the hard rule "every commit compiles + `make ci` green",
the staged files are carried unwired (present, not declared in lib.rs) and wire up in the final
commit. Recorded as the sanctioned fold per brief §2 PR-C rule.

1. `7a62a8b` — copy + module-split re-home (ledger 1+2 folded): v1 sources at the pin under the
   split shape, prefix rules + deferred excisions applied; lib.rs wires only backend +
   catalog_config; root manifest gains the member + chrono + repark-core dep; clippy.toml
   doc-valid-idents gains `PostgreSQL` (v1 list entry).
2. `a08f9a0` — hoists: catalog_state.rs + time_travel.rs (+ file-backed tests, 2), MOVE-ONLY
   except E-4's type side (`TempFallbackAllowed { root: PathBuf }`, baked into the hoisted
   file; enum loses `Copy`, `location_policy()` clones). Cargo.toml + iceberg-datafusion +
   chrono.
3. `71f3d41` — seams: dialect.rs wired with its 2 new-seam tests; extension.rs + its 2 tests
   staged-unwired (the tests drive `ReparkSession`).
4. `ac8dfa8` — forced edit E-2 (patch 01): conditional finalize-time AWS resolution +
   `session/aws_gate_tests.rs` (3 gate tests, AWS-free).
5. `a0d31e9` — forced edit dialect inversion (patch 02): `sql()`/`sql_with()` →
   `dialect.execute(EngineContext, query)`; `with_sql_dialect`; manual builder `Debug`.
6. `e6c01c7` — forced edit extension hooks (patch 03): `with_extension`;
   `ext.configure`/`ext.register` at v1's inline positions.
7. `cc79daf` — residual phase-2 sweep + E-4 call site (patch 04): crate-path imports,
   `CatalogKind::Postgres` fail-loud `NotImplemented`, `time_travel::read_table_at`, E-2
   async→sync store build, E-4 session-side `root: std::env::temp_dir()` at
   `register_memory_catalog`, rustfmt reflow of prefix-rewrite sites. **Assembly-note-1 swap
   recorded:** both staged `rebuild_catalog_provider` call sites
   (`refresh_catalog_provider`, `testing_create_ref`) landed as
   `repark_iceberg::catalog::reregister_catalog_provider` — v1's exact call name via the PR-B
   catalog_ops hoist.
8. (this commit) — wire-up + test audit: full lib.rs manifest; extension module wired;
   `session/tests.rs` = the 38 port-now tests (v1 order, 11 deferred spans excised whole);
   v1's two `#[cfg(test)] pub(crate) use` companions re-homed into `session.rs` (the split
   made it the test cohort's parent — crate-root placement would not resolve for
   `use super::*;`); `tests/ta_window.rs` NOT ported (deferred whole); deferred-test manifest
   + this ledger + task/todo.md + map.md lockstep.

## Forced-edit classes exercised (design §5)

1. E-2 — commits 4 + 7 (resolution at finalize, gated on the AWS signal; sync store build).
2. Dialect inversion — commit 5.
3. Extension inversion — commit 6.
4. E-4 — commits 2 (type side, hoist) + 7 (registration-time root resolution).
5. Mechanical prefix renames — commits 1 (product code) and 8 (test bodies:
   `repark_catalog::memory_catalog` ×2, `repark_write::idents::probes` ×3 — recorded; the
   staged assembly note scoped only the probes sites, the memory_catalog sites are the same
   R-5 class).
6. Test-harness class-6: **not needed** — audited; the v1 session test sources install no
   global tracing subscriber (grep for `tracing_subscriber`/`set_global_default`/`try_init` =
   zero hits), re-confirmed by the merged binary passing 77/77.

## Fidelity + reflow record

`ws1/fidelity_check.sh` (inverts the three prefix rewrites; pin-side deletions = the declared
deferred spans) run pre-fmt against the staging copies with `V1_REPO` at the pin: **all files
OK, exit 0** (backend, catalog_config, read_options, idents, object_store_s3, error_map
[spans 103–139], session [spans 1156–1251, 1419–1431], raw tests.rs, raw ta_window.rs
byte-identical). Rustfmt reflow sites (prefix rewrite pushed v1 lines past 100 cols; patch 04):
`session.rs` — `scan_pruning`/`file_scoped_rewrite`/`scan_concurrency` binding splits,
`with_merge_session_knobs(...)` multi-line call, `testing_create_ref(...)` multi-line call,
`sql_with` signature compaction. (Correction, verification pass: the
`mirror_namespace_location_keys` doc comment was previously listed here as a rustfmt wrap —
rustfmt does not wrap comments, so the prefix rewrite left that doc line >100 cols; it was
hand-wrapped in the verification-fix commit.)

## Census

- Full-workspace `cargo test --locked --workspace -- --list` = **322** (321 at the audit
  commit; +1 verification-fix gate test):
  - PR-B baseline 244 (repark-common 2 + repark-iceberg catalog 50 + write 191 + fork-pin 1)
  - + 68 ported session tier (session::tests 38 + catalog_config 26 + object_store_s3 4)
  - + 2 hoisted time_travel parser/resolution pins
  - + 8 NEW additive seam/gate tests (dialect 2, extension 2, session::aws_gate_tests 4 —
    the 4th is the verification-fix late-config region-signal pin).
- Name-by-name: {v1 session-tier names at the pin − 18 deferred} under the prefix rules diffs
  **EMPTY** against this repo's `--list` (generated, never hand-written).
- Deferred: 18 names with target phases in `task/port/deferred-tests.md`; (68 ∪ 18) = 86 =
  the v1 session tier at the pin. Zero `#[ignore]`, zero skipped-in-CI.
- Test run: 322/322 pass (`make test`); class-6 span tests (repark-iceberg tracing harness)
  pass in the full workspace run.

## Gates

- `make ci` green on every commit (pre-commit hook: map-guard, crate-DAG, lib-rs, fmt, taplo,
  typos); `make preflight` green at head (verify + cargo-audit + cargo-deny + workflow lint).
- `cargo test --locked --workspace` (never `--all-features`); `--locked` everywhere.
- Forbidden-literal sweep clean over the tree AND `git log -p phase-1/pr-b..HEAD` (the
  synthetic example-account fixture ARN sanctioned).
- crate-DAG: `repark-core → {repark-iceberg, repark-common}` only; lib.rs manifest under the
  150-line ceiling; no inline test modules.

## Deviations (recorded)

- Ledger commits 1+2 folded; staged-then-wired carry (rationale above).
- E-4's type side landed with the hoist commit (the staged hoist file bakes it in) instead of
  a separate E-4 commit; the session-side call site landed with patch 04 as planned.
- extension.rs + tests staged at the seams commit but wired at the final commit (its tests
  drive `ReparkSession`).
- The two v1 `#[cfg(test)] pub(crate) use` lines re-homed into `session.rs`, not lib.rs (the
  assembly note's wording) — the module split moved the test cohort's parent.
- clippy.toml `doc-valid-idents` gained `PostgreSQL` (restores the v1 allowlist entry the V2
  seed list had trimmed; needed by ported doc comments).
- Test-body prefix renames extended to the two `repark_catalog::memory_catalog` sites (same
  mechanical class as the staged probes note; without them the ported test cannot compile).
- Verification-fix commit (post-selfcheck deltas in `session.rs`, all defect-driven): the four
  ported `testing_` seams gained the `#[doc(hidden)]` design §3 owed them; the late-catalog
  AWS signal gained the missing S3-region conf class (`resolve_s3_region_override`, matching
  `build()`'s three-class set) + a 4th AWS-free gate test; the
  `mirror_namespace_location_keys` doc line hand-wrapped to ≤100 cols (see the reflow-record
  correction above).
