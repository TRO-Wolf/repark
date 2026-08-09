# Unit ledger — P2E: repark-ta (the PR-4 port + `TaExtension`)

> **ARCHIVED 2026-08-09** (Front-Door FD-4) — a historical record of the v1 → v2 port, kept for
> provenance and **not a source of live rules**: every rule still in force was promoted to a
> current document first ([promotion-ledger.md](promotion-ledger.md)). Relative links were
> repaired for this location on the same date; nothing else changed. Current state:
> [STATUS.md](../../../STATUS.md).

**Unit:** phase-2 PR-4 · **Brief:**
[phase-2-sql-doors.md](phase-2-sql-doors.md) §1 "PR-4" · **Design:**
[docs/design/sql-doors.md](../../design/sql-doors.md) Q11 · **Port-Source:** v1 `main` @
`fc3f48102` · **Status:** MERGED 2026-08-08 (PR #12; archived 2026-08-09) · **Stacked on:** phase-2 PR-3b
([p2d-spark-dml-ledger.md](p2d-spark-dml-ledger.md))

## Scope

Land the TA function set as its own tier-3 crate, owned by **neither** SQL door (design Q11),
and re-attach it to the Spark door at v1's registration position:

- Port `crates/repark-ta` from the pin **VERBATIM** — all `src/`, `tests/` (`contract.rs`,
  `goldens.rs`, `p1c_microbench.rs`), the 148 `.bin` goldens + `manifest.json`, `NOTICE`, and
  the four `map.md` files. `diff -r` against the pin reports **only** the declared deltas of
  classes 2, 3 and 5 below — `Cargo.toml`, two `map.md` dead-link fixes, the new
  `src/extension.rs` + `src/extension/`, and the 4-line `mod extension` wiring in `src/lib.rs`.
  No kernel byte and no test byte was touched; all 148 goldens are md5-identical.
- NEW (the only non-ported code): `src/extension.rs` — `TaExtension`, a thin
  `repark_core::SessionExtension` whose `register` forwards to `udf::register_all` and whose
  `configure` stays at the trait default. Feature-gated behind `datafusion` alongside `udf`.
- Restore the p2b TA-omission rider: `repark-spark`'s `SparkExtension.register` composes
  `TaExtension` at v1 `build()`'s exact position — function registry → analyzer rules → TA UDFs
  (verified against `v1-pin/crates/repark-session/src/lib.rs:320-329`). `repark-spark` gains the
  `repark-ta` dep (same-tier 3 → 3 edge, DAG-legal).
- Port deferred rows #8–#14: v1 `repark-session/tests/ta_window.rs` →
  `repark-spark/tests/ta_window.rs`, file shape kept, all 7 `sql_route_*` passing. The manifest
  drops to exactly 4 rows (the post-milestone-one postgres/excel bucket).
- TA census generated at the pin and re-generated here; empty diff.

Out of scope: the ANSI door's TA smoke row (rider #3 below — `repark-sql` does not exist yet),
`repark-postgres` / `repark-excel` (post-milestone-one), carve-out files
(`.github/`, `AGENTS.md`, `CLAUDE.md`, `Makefile`), any AWS-touching test (E-2: every test here
runs on in-memory `SessionContext`s / local fixtures; acceptance env vars NEVER set).

## Edit classes (declared, bounded — p2b classes 1, 2, 6 inherited)

1. **Verbatim crate copy** — `cp -r` from the pin, `diff -r` proof. No in-file edits.
2. **Manifest alignment** (`crates/repark-ta/Cargo.toml`) — the sanctioned exception to class 1:
   (a) `repark-core = { workspace = true, optional = true }` added and folded into the
   `datafusion` feature (`datafusion = ["dep:datafusion", "dep:repark-core"]`) so `TaExtension`
   can implement the seam without the kernel core ever pulling the engine in; (b) `tokio` added
   as a dev-dep — the `TaExtension` test runs a real SQL window query, so it needs a runtime
   (the kernel batteries stay sync); (c) the banner's stale `repark-session` consumer sentence
   re-pointed at the Spark door. Root `Cargo.toml`: workspace member + internal-dep entry +
   `serde_json = "1"` hoisted to `[workspace.dependencies]` (line carried from the v1 root
   manifest at the pin — repark-ta's `serde_json.workspace = true` dev-dep needs it).
3. **NEW door-native code** — `src/extension.rs` + `src/extension/tests.rs`, banner style, with
   its tests in the same commit; plus the minimal 4-line `mod extension` / `pub use` wiring in
   `src/lib.rs` that the new module requires (the only edit to a ported source file).
4. **Deferred-test session adaptation** (`ta_window.rs`, exactly as p2c class 4 declares) — v1
   `ReparkSession::new()` → the door-installed builder (`with_extension(SparkExtension)` +
   `with_sql_dialect(SparkDialect)`), three construction sites; plus the class-2 prefix map
   `repark_session::` → `repark_core::` and `arrow::` → `datafusion::arrow::` (the door crate
   has no direct `arrow` dev-dep; the re-export is the same types). Test bodies otherwise
   v1-faithful. **The goldens path needed no fix** — see rider #4.
5. **Guard-table entry** — `scripts/check_lib_rs.py` `EXCEPTIONS` gains `repark-ta` (measured
   249, ceiling 260) with a reason + ratchet note: the crate root is the `TaError` contract plus
   the flat kernel re-export surface, and splitting either would break the port's identity diff.
   Sanctioned out (2) of the guard's own error message.

No other edit class is authorized; anything else is a STOP.

## Census — repark-ta

Method: `cargo test -p repark-ta -- --list`, `grep ': test$'`, strip suffix, `sort -u`. Never
hand-written (docs/testing.md). Full proof: `pr4-census-proof.txt` (workstream scratch).

The crate carries an **optional `datafusion` feature** (v1 shape, preserved), so the default
census does not see `udf::tests::*`. Both surfaces are pinned so neither can drift silently.

| Pass | v1 @ `fc3f48102` | this repo | Sorted diff |
|---|---|---|---|
| `cargo test -p repark-ta -- --list` (default features) | **146** | **146** | **EMPTY** |
| `… --features datafusion -- --list` | **178** | **180** | +2 / −0 (both NEW, below) |

Binaries listed on both sides: `unittests src/lib.rs`, `tests/contract.rs`, `tests/goldens.rs`,
`tests/p1c_microbench.rs` (+ 0 doc-tests). `tests/ta_window.rs` is **not** a repark-ta binary in
either repo — in v1 it lived in `repark-session/tests/`, here in `repark-spark/tests/`.

The v1 census was **independently regenerated at the pin** and is byte-identical to the stored
file — the ground truth is reproducible, not inherited.

NEW names, additive and outside the ported census (door-native, feature-on pass only):

| New test | What it pins |
|---|---|
| `extension::tests::ta_extension_register_installs_the_whole_ta_udf_set_bit_exact` | `register` makes `ta_ema(close, 3) OVER (ORDER BY ts)` callable on a bare context and `f64::to_bits`-identical to `repark_ta::ema`; plus every `window_udfs()` name is registered (whole registry, not one function) |
| `extension::tests::ta_extension_configure_is_the_trait_default_pass_through` | the trait-wrapping **both-sides** audit: TA installs no `ConfigExtension`, so `configure` returns the `SessionConfig` untouched |

Ported deferred rows #8–#14 (names unchanged, in `repark-spark/tests/ta_window.rs`, 7/7 PASS):
`sql_route_single_series_kernels_match_the_kernel`,
`sql_route_scalar_param_kernels_match_the_kernel`,
`sql_route_multi_series_kernels_match_the_kernel`, `sql_route_parked_four_match_the_kernel`,
`sql_route_partition_by_scopes_the_series`,
`sql_route_multi_batch_partition_matches_the_kernel`,
`sql_route_rejects_a_non_literal_period`.

One further NEW name lands in `repark-spark` (outside the 334-name PR-3b census, additive):
`extension::tests::register_composes_the_ta_extension_window_udfs` — the rider restoration
pinned from the *door* side, bit-exact.

## Restoration checklist vs p2b declared riders

| p2b declaration | PR-4 action | Status |
|---|---|---|
| Rider #1 — TA registration OMITTED from `SparkExtension.register` | `TaExtension` composed at v1's exact position (after `register_all` + `analyzer_rules`); module doc's "TEMPORARY OMISSION" paragraph replaced by a "Composed, not re-implemented" paragraph; pinned by `register_composes_the_ta_extension_window_udfs` | **DISCHARGED** |
| Rider #2 — write-knob split | untouched (conformance note, not an omission) | n/a |
| Rider #3 — `EngineContext::new` seam constructor | untouched | n/a |
| Rider #4 — PR-1 doc-comment re-home | untouched (discharged at PR-2/3b per p2c) | n/a |

## Verification — §7-checklist substitution for the four-lens panel

Brief §2 prescribes staged delegated workstreams followed by a four-lens verification panel
(port-fidelity/census, design-conformance, testing-discipline, public-hygiene). This unit was
executed **single-agent** (CLAUDE.md `<subagent_policy>` default), so there was no independent
panel to convene. Declared substitution: the four lenses are discharged as an explicit
self-audit checklist against **session-api.md §7** ("Census accounting and the port procedure"),
which is the SSOT the port-fidelity lens actually reads from. Recorded verdicts:

| Lens | §7 clause | Verdict |
|---|---|---|
| Port fidelity | "declared-rename units with a **mechanically generated** old→new map; `--list` at the pinned SHA; diff must be empty; never hand-written" | PASS — no rename map needed (crate name and every test path unchanged); both `--list` passes generated, default-feature diff EMPTY |
| Census accounting | "(ported ∪ deferred) = v1-total, auditable at every phase boundary" | PASS — 146 ported ∪ 0 deferred = 146; the 7 `ta_window` rows move from *deferred* to *ported* (in repark-spark), manifest remainder exactly 4 |
| Design conformance | sql-doors.md Q11: "ports with a thin `TaExtension` (register-only), owned by neither door; Spark extension composes it; native sessions opt in" | PASS — register-only (configure defaulted); the crate owns the extension, the door composes it; `TaExtension` is `pub` for native opt-in |
| Testing discipline | docs/testing.md hard rule 1 (tests in the same commit) + test-per-change | PASS — commit 1 carries the ported batteries, commit 2 carries all three new tests + the 7 ported rows; zero `#[ignore]`, zero `--skip`, zero commented-out tests |
| Public hygiene | forbidden-literal sweep over the diff **and** the commit messages | PASS — 0 hits on both passes (gate table below) |

This substitution is a **deviation from brief §2** and is recorded as such in Deviations below.

## Gate results

| Gate | Result | Evidence |
|---|---|---|
| `make ci` per commit | PASS | commit 1 verified green in a detached throwaway worktree (`make ci` exit 0); commit 2 verified there too (`cargo fmt --all --check` exit 0, repark-ta 130 + repark-spark 345/5/1/1/7 all green); commit 3 is docs-only |
| `make ci` (PR head) | PASS | exit 0 |
| `make preflight` (PR head) | PASS | exit 0 |
| `cargo test --workspace` (never `--all-features`) | PASS | exit 0 |
| `cargo test -p repark-ta --features datafusion` | PASS | 130 lib + 10 contract + 39 goldens + 1 microbench, 0 failed / 0 ignored |
| `cargo test -p repark-spark --test ta_window` | PASS | 7/7 |
| clippy `--workspace --all-targets -D warnings` | PASS | three findings in the NEW code were fixed *before* commit 2 landed (two `usize as i64` casts → `i64::try_from(_).expect(…)`, the v1 idiom; one `doc_lazy_continuation` in the reworded `SparkExtension` module doc) |
| TA census empty sorted diff | PASS | table above; `pr4-census-proof.txt` |
| deferred rows #8–#14 closed; remainder = 4 | PASS | `port/deferred-tests.md` rows struck LANDED + dated row-close note |
| forbidden-literal sweep (diff + commit messages) | PASS | 0 hits, both passes |
| map.md lockstep (`check_map_md.sh`) | PASS | exit 0 |
| `check_crate_dag.py` / `check_lib_rs.py` | PASS | "8 internal edges clean across 6 of 7 mapped crates"; "6 crate roots clean" — tier 3 → 2 (feature-tied) + 3 → 3 edges legal, ceiling exception declared |
| `typos` / `taplo` (pre-commit + CI) | PASS | `.typos.toml` gained `TEMA`/`CMO`/`tema`, carried from the pin's own config |

## Riders (declared temporary omissions / deviations)

1. **ANSI TA smoke row NOT landed** — design Q11 owes the ANSI door "one smoke row
   (`f64::to_bits` vs golden) + the non-literal-period refuse row". `repark-sql` does not exist
   until PR-5, so the toll cannot be paid here. **Lands PR-6** with the ANSI matrix; the two
   rows are surfaces on the ANSI side of the Q13 matrix, not TA-crate work.
2. **`repark-ta`'s `datafusion` feature stays non-default** — making it default would have been
   the easy way to put `TaExtension`'s tests in the default census, but it changes the ported
   crate's shape and would pull `udf::tests::*` (32 names) into a census the pin does not have
   them in. Kept as v1 had it; the second census pass is the price, and it is a *better* pin
   (it covers 34 names the single default pass never sees).
3. **`TaExtension` ships TWO tests, not one** — the brief's PR-4 line implies "with its own
   test" (singular). The trait-wrapping both-sides audit (AGENTS.md) requires the *defaulted*
   half be pinned as well as the overridden half, so `configure`'s pass-through gets its own
   case. Additive; declared rather than silently absorbed.
4. **Goldens path NOT rewritten** (a declared class that turned out to be a no-op) — the port
   plan anticipated fixing `ta_window.rs`'s
   `$CARGO_MANIFEST_DIR/../repark-ta/tests/goldens`. It resolves unchanged: repark-ta sits at
   the same sibling position relative to the door crate here as it did to `repark-session` in
   v1. The line is byte-identical to the pin. Recorded because "we fixed the paths" would have
   been a false claim.

## Deviations / STOPs

- **Four-lens panel → §7 self-audit checklist** (see the Verification section): single-agent
  execution, no independent panel. Declared, not silent.
- **`serde_json` hoisted to `[workspace.dependencies]`** — repark-ta is the first member to need
  it; the pin's own root manifest carries the identical `serde_json = "1"` line, so this is a
  carry-over, not a new dependency decision.
- **`scripts/check_lib_rs.py` EXCEPTIONS grew a row** — the guard's ceilings "ratchet DOWN
  only"; this is a new crate's first entry (not a raise), with the measured count and a ratchet
  trigger recorded, exactly as the guard's error message sanctions.

## Retrospective

*(filled at unit close, per SEPMO)*
