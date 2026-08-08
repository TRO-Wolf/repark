# Phase 3 — the Python binding, the facade, and the census (the milestone-one design)

Status: **SETTLED 2026-08-08.** The deliberate design pass for phase 3 of the port
([../port/PLAN.md](../port/PLAN.md)), analogous to [session-api.md](session-api.md) (phase 1) and
[sql-doors.md](sql-doors.md) (phase 2). Synthesized from a three-design competition judged by three
independent critics: the census-first architecture won both doctrine lenses; the delivery mechanics
(slate ordering, the census stability run, the additive cohort flag, the tier-2 IAM posture) are
grafted from the delivery-risk design; four rulings (the `EngineRuntime` split, the error
type-identity guard, the facade-cohort environment clauses, the re-home carve-out record) are
grafted from the target-architecture design; every judge-confirmed defect is fixed in this text.
Port source: the private v1 engine repository at the frozen
port pin `fc3f48102` — verified byte-identical to that repository's `main` across the whole tree,
zero commits of drift. Base: public `main` at the phase-2 close.

Companion execution brief: `briefs/phase-3-python-facade.md`. Census procedure and recorded
baselines: `docs/port/census.md` + `task/census/`. Deferred obligations:
[../../task/port/deferred-tests.md](../../task/port/deferred-tests.md).

---

## 0. Verdict shape

**Phase 3 is a fidelity claim, not a redesign opportunity.** Milestone one is defined mechanically:
v1's suite green here, and the pyspark-compat census byte-flat multiset-compared across the two
repositories. Every design choice below is subordinated to that claim. The architecture is therefore
a **census-first verbatim port**:

- The Rust binding crate and the whole Python tree are copied at the pin and re-homed with a
  **closed, enumerated set of mechanical edit classes** (§3). Anything not on that list is a defect,
  not a judgement call. A divergence that is *defensible* but avoidable is still a divergence: it
  costs census cells, and census cells are the acceptance gate.
- The Python package layout ports **exactly as it is at the pin**: `python/repark` is the PySpark
  facade, `repark.sql` is the pyspark-alias shim (import-identity asserted), and the r26 region
  splits under `session/` and `dataframe/` keep their `_wire()` loops and frozen import paths. The
  target layout recorded in `AGENTS.md` / `PROJECT.md` (`python/repark` = native + ANSI,
  `python/repark/spark` = facade) is **real but later**: it becomes its own designed phase after
  milestone one and before 1.0 (§4, Q1).
- **No ANSI door is exposed from Python in phase 3 at all** (§4, Q2). The frozen seam rule —
  extensions are session-scoped, not dialect-scoped — means an honest two-door Python surface needs
  two engine sessions. That is a design pass, and it is not this one.
- Three engine surfaces are deliberately not in the wheel: the Excel reader, the Postgres reader,
  and nothing else. `repark-ml` **is** in scope and is scheduled here for the first time (§4, Q3):
  without it the ML facade package and its ~138 facade tests cannot port, and "v1's suite green" is
  false by construction.
- The deferral boundary is drawn **at the Rust binding, never in the Python facade**. Deferred
  surfaces keep their pymethod name and arity and raise a loud, named refusal (edit class EC-3), so
  every Python source file ports byte-identical **except the enumerated public-hygiene scrub
  (EC-9)** — this repository is public and a handful of ported test constants and docstrings name
  private infrastructure; they are sanitized as declared, outcome-neutral content edits. Only the
  tests that exercise a deferred surface are withheld, by node id, in a checked-in ledger.

Two things phase 3 must *build* rather than copy, because v1 has neither: a **mechanical report
comparator** (two census reports in, a pass/fail multiset diff out — the acceptance gate has no
implementation today) and a **tier-2 live-AWS workflow** (no template exists). Both get the full
verification panel; everything else gets the slim tier.

---

## 1. What lands, and what it is made of

| Artifact | Shape at the pin | Edit classes applied |
|---|---|---|
| `crates/repark-ml` | 1,703 lines, five modules, one third-party dep (`thiserror`), no internal deps | EC-7 (crate `map.md` only) — every `.rs` and `Cargo.toml` verbatim |
| `crates/repark-python` | ~6,653 lines Rust, flat six-module `src/` + `tests/bindings.rs`; 3 pyclasses, 5 exceptions, 3 ML pyfunctions | EC-1, EC-2, EC-3, EC-5, EC-6, EC-10 |
| `python/repark` (the wheel) | 53 modules / ~46 KLOC source + 127 test files | EC-4 (tests only), EC-7 (map.md), EC-9 (hygiene) |
| `python/repark-parity` | 85-line comparison core + 64 generated unit-test names (static `def test_` is 53; parametrization brings the collected count to 64 — corrected at PR-4 from the recon's static 58) + `compat/` census machinery + `bench/` | none (verbatim) + NEW comparator |
| uv workspace | virtual root, two members, one `uv.lock`, four load-bearing ruff per-file-ignore blocks | none (verbatim) |
| Mechanical gates | `check_lib_py` returns; crate-DAG rows; panic-ban carve-out | none (port) |
| CI | wheels, pip-audit, parity-live ported; rust job split; live-AWS net-new | §7 |

Sizes are orientation. Every count that gates anything is **generated** at PR time — `cargo test
--workspace -- --list` for Rust, `pytest --collect-only -q` for Python — never hand-written. That
rule already caught two miscounts in phases 1 and 2.

---

## 2. Crate and package layout, DAG tiers, dependency edges

### 2.1 `scripts/check_crate_dag.py` gains two rows and one tier name

| crate | tier | internal deps | role |
|---|---|---|---|
| `repark-ml` | 3 | *(none)* | native estimator kernels (Cholesky / streaming OLS / IRLS / Lloyd); params-only models |
| `repark-python` | **4 (new tier: bindings)** | `repark-core`, `repark-functions`, `repark-ta` (feature `datafusion`), `repark-spark`, `repark-ml` | the PyO3 cdylib — a thin adapter, per ADR-0004 |

`repark-ml` sits at tier 3 beside `repark-functions` / `repark-ta`: it is a capability leaf with zero
internal edges, and tier 0 must keep meaning "the shared vocabulary every crate may depend on". The
tier map is the SSOT; `TIER_NAMES` gains `4: "bindings"` and tier 3's label widens from "spark
surface" to "surface crates". Both rows are **pre-declared** before the crates land, matching the
phase-2 pattern.

`repark-python` is the only crate above tier 3, and nothing may ever depend on it. It is also the
only crate that opts out of `[lints] workspace = true` (local `unsafe_code = "allow"`, clippy
`all`+`pedantic` at priority −1) — the workspace root already reserves that carve-out.

### 2.2 The binding's dependency edges, and the two that disappear

v1's binding named six repark crates. The ported one names five, and the mapping is not
one-to-one:

- `repark-session` + `repark-core` (v1's error seed) **collapse into a single `repark-core` dep**.
  This is not cosmetic: v1's `repark_core::Error` / `ErrorClass` lines compile unchanged here because
  `repark-core` re-exports them from `repark-common` — the same identifiers now mean a different
  crate. Leaving both dep entries in `Cargo.toml` makes the names collide. Collapse is mandatory.
- `repark-spark` is **added** — v1's binding named no SQL crate at all (§5, F3).
- `repark-excel` and the transitive Postgres reader are **dropped**; their pymethods survive as
  refuse-arms (EC-3).
- `repark-ml`, `repark-functions`, `repark-ta` (feature `datafusion`, spelled explicitly — never
  `--all-features`) carry over unchanged.

`repark-iceberg` is not a binding dep in v1 and must not become one: the binding reaches Iceberg
only through `ReparkSession` and through SQL text. That is the bindings-as-thin-adapter discipline,
and the crate-DAG guard does not enforce it — review does.

### 2.3 The Python tree

```
python/
  repark/                  maturin backend; manifest-path ../../crates/repark-python/Cargo.toml,
    pyproject.toml         module-name repark._native, python-source src, features ["extension-module"]
    src/repark/            53 modules, ported verbatim, including:
      __init__.py            ReparkSession == ReParkSession == SparkSession (one object)
      sql/                   the pyspark.sql alias package (identity re-exports + loud absent list)
      session/  dataframe/   the r26 region splits — _wire() loops and frozen import paths, verbatim
      ml/                    15 modules, 28 transformers, lazy ext backends
    tests/                 127 files, ported minus the declared deferral list
  repark-parity/           hatchling; comparison core + compat census machinery + bench
```

The wheel's runtime dependency stays exactly one (`pyarrow>=25`); `numpy`, `pandas`, `polars` and
`ml-ext` stay extras, lazy-imported. The uv workspace root returns as a **virtual** root with both
members, the dev group, `known-first-party`, and — load-bearing — the four ruff per-file-ignore
blocks (`**/tests/**`, `ml/**`, `session/**`, `dataframe/**`). Those ignores are not style
preferences: `import *`, `F821`, and `E402` are how the region splits preserve pre-split import
paths. Dropping them does not merely lint-fail, it invites a "cleanup" that breaks
`test_t0_df_regions_import_freeze`. `uv.lock` is checked in from this phase on.

`python/repark/map.md` is **stale at the pin** (it documents a flat `session.py` / `dataframe.py`
layout and omits eight modules). It is regenerated against the real tree, not ported stale (EC-7).
map.md files carry no tests and no names, so this costs no census cell.

---

## 3. The mandatory edit classes

Ten classes. A diff line that fits none of them is a finding.

**EC-1 — import re-home.** One live prefix rule plus the Cargo collapse. Derived from the actual
call sites, not from the recon's prefix table: `repark_catalog::`, `repark_write::`, and v1's Spark
`repark_sql::` occur **zero** times in the phase-3 port scope (verified by grep at the pin), so the
only live rewrite is `repark_session::` → `repark_core::`; `repark_functions::` and `repark_ta::`
are unchanged. The **deliberate non-edge list** is recorded as a decision: the binding gains no
`repark-sql` and no `repark-iceberg` edge, so a missed re-home is a loud compile error, never a
silent bind to the wrong crate. `repark_core::Error` / `ErrorClass` lines are **not** edited but
**are** re-pointed by the re-export — reviewers must read them as changed even though they are
textually identical, and the trap is mechanized: a compile-time type-identity test asserts
`repark_core::Error` and `repark_common::Error` are the same type, so a future re-split cannot
quietly re-point the text-stable lines.

**EC-2 — door-installed builder.** Every construction site of `ReparkSession` in the binding becomes
`ReparkSession::builder()…​.with_sql_dialect(Arc::new(SparkDialect)).with_extension(Arc::new(SparkExtension)).build()`.
A bare builder yields stock DataFusion. This is the standing edit class already recorded from phases
1 and 2; phase 3 is where it finally bites at the user-visible boundary (§5, F3).
**Explicit non-sites:** the column module's four throwaway `SessionContext` provisionings — the
standalone-Column analysis contexts that call `register_all` + `analyzer_rules` directly (one
production site + three test sites; the count was verified against the pin at PR-3, correcting an
earlier draft's "five") — plus the dataframe module's three plain test fixtures, all port
**verbatim** and are NOT door-install sites: the door-neutral function registry retains all three
symbols, and mechanically applying EC-2 there would change standalone-Column semantics. A reviewer
applying "every bare construction becomes a door-installed builder" must stop at session
construction sites.

**EC-3 — refuse-arms.** For every deferred engine surface, the pymethod **stays** with its v1 name,
arity, and defaults, and returns
`UnsupportedOperationException` whose message names the surface, the reason ("scheduled
post-milestone-one"), and the tracking issue. Applies to `read_excel`, `excel_sheet_names`, and
`read_postgres`. Consequence, and the point: `python/repark/src/**` ports byte-identical, the facade
`DataFrameReader.excel()/jdbc()` methods keep working up to the loud refusal, and the deferral is
visible in exactly one file. Each refuse-arm ships with a Rust test asserting the exception type and
that the message names the surface — a refusal is a behavior.

**EC-4 — deferred-by-name test excision.** A test that exercises a deferred surface is **not
ported** and **not skipped**; it is listed by pytest node id in
`task/port/deferred-python-tests.txt` (machine-readable) and summarized in
`task/port/deferred-tests.md` under the reconciliation rule. **The list is generated empirically,
never transcribed by file**: the candidate files are run against a built wheel with the refuse-arms
in place, and a test defers only if its failure traces to a deferred surface — the criterion is
*where the exception is raised*, hand-adjudicated per node. The competition's judges verified both
failure directions of a by-file list: most of the offline JDBC-options pins raise their
`IllegalArgumentException` from the **facade** before any native reader is reached (they port and
PASS), while the Postgres *catalog-config* registration tests — which received a real session at
the pin — hit the engine's `CatalogKind::Postgres` `NotImplemented` registration here and **do**
defer. Over-deferral and under-deferral are both gate failures: the reconciliation identity only
catches tests missing from both sides, so the comparator's allowlist must match reality, and a
harness test asserts the checked-in ledger and the comparator's machine-readable allowlist are
byte-identical — a ledger that can drift from the gate it feeds is not a ledger.

**EC-5 — `EngineRuntime`.** The phase-1 omissions ledger names an `EngineRuntime` type as a phase-3
deliverable that "becomes engine API the day the binding ports, additively." Honoring that recorded
resolution: the **type** lands in `repark-core` (additive, tier-legal, no behavior change), and the
**instance** — v1's process-wide `OnceLock<Runtime>` — stays in `repark-python`, same lifetime, same
behavior, same pin test name (`sequential_sessions_share_one_tokio_runtime`). Rationale in §4, Q7.

**EC-6 — doc-drift rider.** `crates/repark-spark/src/dialect.rs` tells readers to install the dialect
with `with_dialect`; the method is `with_sql_dialect`. Fixed in the PR that first wires the door.
**Second rider, declared by PR-2 and discharged in PR-3:** `crates/repark-ml` carries four verbatim
references to `docs/ml-design.md`, a v1-only path with no counterpart here — `Cargo.toml:6`,
`src/lib.rs:3`, `src/logistic_regression.rs:199` (comments), and `src/error.rs:52`, which is inside
an `#[error(...)]` format string and is therefore **user-visible at runtime**. PR-2 left all four
byte-identical to keep the crate's verbatim/identity claim intact and to keep the fix out of a slim
port; they become reachable from Python only when PR-3 wires the binding, so PR-3 repoints them at
the in-repo ML authority (`docs/design/python-facade.md` §4 Q3) or drops the pointer, and pins the
`Singular` message with a test. Neither the rider nor its deferral may go unrecorded: it is entered
in `task/p3b-ml-ledger.md` and must be closed in `task/p3c-*` before phase close.

**EC-7 — map.md regeneration.** Stale v1 `map.md` files are rewritten to the true tree rather than
ported stale. Every new directory gets one in the same change.

**EC-8 — census invocation pinning.** Cohort module lists are passed explicitly on the command line;
the `--stretch` flag is never used for the classic cohort. On the v2 side only, the harness
additionally gains an **additive** `CLASSIC_MODULES` constant and `--classic` flag (new code, new
harness tests; the existing `--stretch` flag is left byte-identical and gains a test that **pins its
blending behavior** so the trap is documented rather than merely avoided). See §5, F1.

**EC-9 — public-hygiene scrub.** This repository is public; a handful of ported test files name
private infrastructure in constants and docstrings (a real warehouse bucket, a production namespace,
a fully-qualified production table name in one non-gated test, and references to a private
orchestration script). Each such literal is replaced by a synthetic equivalent
(`s3://example-warehouse/…`, `acceptance_scratch`, fixture account values already sanctioned in
phases 1–2), enumerated line-by-line in the PR body's hygiene ledger, and verified by the standing
forbidden-content greps. These are **declared, outcome-neutral content edits**: node ids do not
change, and the affected gated tests still skip identically. An un-enumerated hygiene edit is a
finding; an un-scrubbed private literal is a phase-blocking incident.

**EC-10 — `check_lib_rs` exception row.** The binding's crate root is 217 lines against the guard's
150-line default ceiling (it is a manifest — module decls, exception taxonomy, error fold, pymodule
registration — and already uses the sanctioned file-backed test module). The guard's EXCEPTIONS
table gains a `repark-python` row in the same PR that lands the crate; without it every slate reds
on arrival.

---

## 4. Rulings, Q1–Q10

### Q1 — Python door exposure and the `repark.sql` collision → **port v1's layout verbatim; re-home after milestone one as its own phase.**

`python/repark` is the PySpark facade. `repark.sql` is the pyspark-alias package, with its
import-time identity assert and its loud `_PYSPARK_SQL_ABSENT` list. `from repark import
ReparkSession` remains the one changed import line.

The rationale is not conservatism for its own sake. The acceptance gate quantifies over test names
and over the pyspark-redirect census, and the redirect seam repoints `pyspark.sql.*` at this package
tree. Re-homing 46 KLOC under `repark.spark` in the same phase would (a) rename every facade test
path, forcing the entire suite through declared-rename discipline, (b) change the redirect map that
the census bootstrap installs, and (c) make any census movement uninterpretable — you could no
longer tell a port defect from a layout consequence. The gate would survive as a ritual and die as
evidence.

**The migration is designed now, executed later.** Recorded target, to be settled in its own design
pass immediately after milestone one and before the first tag (pre-1.0, breaking changes allowed —
the API-forever clock starts at the first tagged release, not at public):

- `repark.spark` becomes the facade package; `repark.spark.sql` becomes the alias package, so the
  mechanical swap for multi-import scripts becomes `pyspark` → `repark.spark` and the one-line-import
  promise survives as `from repark.spark import ReparkSession`.
- Top-level `repark` becomes the native lazy API, with `repark.sql()` as the ANSI door **function**
  — which is why the alias *package* cannot stay at `repark.sql`: a module and a callable cannot
  occupy one name honestly.
- The re-home ships as a declared-rename unit with a generated old→new node-id map, plus a
  deprecation shim at `repark` re-exporting the facade names for one minor series.

`AGENTS.md` and `PROJECT.md` target maps gain a dated note that `python/repark` is the facade until
that phase (orchestrator carve-out edit). Three additional mechanisms make the deferral honest
rather than hopeful:

- **The testing contract is amended in this phase**: the entry-point matrix's row-2 spelling
  (`native repark.sql()`) is annotated to state that the spelling is the *target*, occupied by the
  alias package until the post-milestone re-home. Phase 3 must not merge with the central testing
  structure naming a spelling the shipped tree contradicts.
- **A release-prep gate converts the window from a promise into a mechanism**: the first-tag
  checklist in `docs/release.md` gains a hard item that **fails the tag while `repark.sql` is still
  a module**. One slip would otherwise make the alias package an API-forever commitment.
- **The structural carve-outs of the future re-home are recorded now**, so the later design pass
  starts from facts: `repark._native` cannot move (one extension module, fixed by the maturin
  module-name), `repark.errors` cannot move (the taxonomy is defined in Rust and re-exported by
  identity, and facade tests pin it), and `__version__` stays top-level (one distribution). The
  re-home phase also inherits the judges' verified finding that the facade corpus is **not**
  node-id-invariant under a re-home (parametrized ids embed module-path strings), so it ships as a
  declared-rename unit with a generated old→new map — never inside a fidelity phase.

### Q2 — ANSI door from Python in phase 3 → **nothing. Zero ANSI surface in the wheel.**

The seam freeze establishes that extensions are session-scoped, not dialect-scoped: a
Spark-extended session has Spark expression semantics through every door, so `sql_with` on a facade
session cannot produce honest ANSI results for anything the analyzer or UDF layer touches. An honest
two-door Python surface needs **two engine sessions**, which needs a design pass covering session
lifecycle, catalog sharing, temp-view visibility, and which door owns `getOrCreate`. That is real
work with real ambiguity and no census obligation attached to it — precisely the thing that must not
be smuggled into a fidelity phase.

Cost of waiting: none that binds. The dbt adapter is a separate package that may target the Spark
door at milestone one (it is the verbatim v1 surface, so a v1-built adapter swaps cleanly), and the
ANSI door is fully reachable from Rust today. Trigger to open the design: the first consumer that
needs ANSI-from-Python — most likely the dbt adapter choosing the native door.

### Q3 — `repark-ml` → **in scope, phase 3, first PR.**

1,703 lines, zero internal dependencies, one third-party dep. Without it: `ml.rs` (630 lines) and its
three pyfunctions cannot port, `repark.ml` (15 modules, 28 transformers) cannot import, ~138 facade
ML tests plus the ML oracle files cannot port, and the full-extras facade cohort is missing a
double-digit percentage of its rows. "v1's full suite green" would then be a claim about a suite we
chose not to run. It has no scheduling row anywhere in the repo today; this design creates one, and
`AGENTS.md`'s target map row moves from "later" to "phase 3".

`repark-excel` and the Postgres reader stay post-milestone-one by the phase-2 decision. They get
refuse-arms (EC-3) and deferred-by-name rows (EC-4), and the four existing Rust deferred rows gain
their Python siblings under the same reconciliation rule.

### Q4 — census mechanics → **explicit cohort lists, a fresh dual-repo baseline, a defined facade cohort, a new comparator, and no CI wiring.** Full procedure in §6.

Summary of the four rulings: (a) cohorts are named by explicit module list on the command line, never
by `--stretch`; (b) the baseline is re-recorded by running the pinned procedure against the v1 pin
itself, and `PLAN.md`'s numeric table is replaced by the recorded run; (c) the full-extras facade
cohort is defined here and recorded at the same freeze point; (d) `make census` stays local +
slate-run, not CI-wired — it needs a 20-minute-per-module wall, a runtime sparse clone of the Apache
Spark test tree, and a scratch interpreter, and the facade signal that *does* belong in CI is the
packaged-wheel facade suite (§7).

### Q5 — CI delta → §7. Every job add, rename, or split updates branch protection in the same change.

### Q6 — packaging → **settle four, defer one, and change nothing in phase 3.**

- **abi3-py312: confirmed.** One `cp312-abi3` wheel per platform, no interpreter matrix. Already true
  at the pin; the open item closes as *recorded*, not as *changed*.
- **Python floor: 3.12.** Follows from abi3-py312.
- **Wheel matrix at the first tag: manylinux x86_64 only.** macOS arm64 / Windows / musllinux have no
  prior art, no local proof path, and no user. Adding a platform is non-breaking; shipping a broken
  one is not.
- **sdist: yes, from the first tag.** `maturin build --sdist` is one artifact and one line. Without
  it PyPI holds no source distribution at all, which forecloses source installs and leaves no
  archival copy of what a release was built from. Document that building it requires a Rust
  toolchain.
- **Version SSOT: the workspace `Cargo.toml`.** At the first release the wheel's pyproject declares
  `dynamic = ["version"]` so maturin takes the crate version (a bare deletion of the field is a
  metadata error), the tag must match, and a wheel-path test pins `repark.__version__` against it.
  **This edit does not land in phase 3** — the pyproject ports verbatim at `0.0.0`, and the change
  rides the release PR, where an actual build can prove it. The shape is recorded here so the
  release PR executes a decision instead of making one.

Registry configuration (PyPI trusted publisher, per-crate crates.io trusted publishing, the one-time
classic token for each first-ever crate name, then revocation) is maintainer-side and unchanged.

### Q7 — `EngineRuntime` home → **the type in `repark-core`, the instance in `repark-python`.**

The engine deliberately owns no runtime: `repark-core` never blocks on anything, every entry point
is async, and the embedding supplies the executor. But the phase-1 omissions ledger already resolved
where the *name* lives: the `EngineRuntime` type "becomes engine API the day the binding ports,
additively" — and a settled design does not quietly reverse a recorded resolution. So the type lands
in `repark-core` as a thin, additive wrapper (tier-legal, publicly documented as
"the embedding's executor handle; core never constructs one"), while the process-wide
`OnceLock<Runtime>` **instance** stays in `repark-python` with the same lifetime, the same behavior,
and its pin test (`sequential_sessions_share_one_tokio_runtime`). Core still never blocks; the
binding still owns process-wide state; a second embedding (a Flight SQL handler is the anticipated
one) gets a named type to hold rather than a convention to rediscover.

### Q8 — the Spark-door time-travel temp-view leak → **do not touch it in phase 3.**

The Spark door inherits v1's leak: pinned time-travel views are registered and never deregistered.
The facade's `list_temp_view_names` and catalog-surface tests will observe leaked views — and so do
v1's, because it is the same bug. Bug-for-bug parity holds, and the census is byte-flat *because* it
holds.

Fixing it during phase 3 would move facade cells and census cells for a reason unrelated to the port,
which is exactly the movement the gate exists to detect. So: the tracked-debt row stays open, gains a
divergence-with-issue note, and the fix lands **immediately after milestone-one acceptance**, paired
with the matching v1 bugfix (legal: v1 is bugfix-only from that moment) and with the facade rows
updated in the same change. Never silently, and never inside the fidelity window.

Measured stakes, recorded so the ruling rests on evidence: the judges verified the blast radius is
small in both directions — the facade's catalog listing filters the pinned-view prefix (a hygiene
test asserts the *filtered* list is clean), while one ported time-travel test asserts the raw
registration list is **non-empty**, i.e. the ported suite pins the leak's *presence*. That second
fact is also why the fix cannot land before the port: it would red a ported fidelity test on
arrival.

### Q9 — the two recorded parity bases → **port both verbatim; record the tension; re-record later.**

The live scenario registry records its goldens under ANSI-on (the Spark 4 default); the SQL
passthrough corpus carries hand-computed non-ANSI goldens, authored when live recording was not
available. Both port unchanged, including the two guard tests that hard-assert the scenario count
(27) and the disclosure name set — those must only ever change deliberately.

Re-recording the passthrough corpus against live Spark 4.1.2 under one basis is the right end state
and is *not* phase-3 work: it changes goldens, which changes results, which is census movement. It
is scheduled post-milestone-one, gated on the live oracle tier having run green at least once on
merged code, and it is a declared-rename-free but golden-changing unit that ships alone. So the
tension stays legible, `docs/port/census.md` records a machine-checkable `basis:` designation per
golden corpus (`live-recorded/ansi-on` vs `hand-computed/non-ansi`) with the hard rule that the
live oracle tier may only re-derive goldens whose basis is live-recorded.

### Q10 — cutover sequencing → **recorded as a constraint, not decided here.**

Which production workloads move when, and the rollback story, is an operations decision owned by the
operator. The design records what engineering guarantees it: during the parallel-run window a given
Iceberg table is written by v1 or by this engine, never both — single-writer-per-table, enforced by
schedule and by convention, not by the engine (no cross-process write lock exists, and inventing one
is not milestone-one scope). Phase-3 *acceptance* does not depend on the cutover decision; the
*milestone-one declaration* does, and the phase-close checklist names it as a user-side item.

---

## 5. The three hard findings

### F1 — the classic census cohort is mis-scripted at the pin

The census shell script runs the classic cohort with `--stretch`. At the pin, the stretch list is
eight modules (`test_column`, `test_readwriter`, and the six modules that also constitute the C3
expand cohort), which the resolver appends to the night-1 trio — an eleven-module run. But the /345
denominator is the five-module set: `test_functions` (137) + `test_dataframe` (60) + `test_types`
(104) + `test_column` (28) + `test_readwriter` (16). Running the script as-is blends C3 into the
classic denominator, contradicting both the runner's own denominator-isolation doctrine and
`PLAN.md`'s "cohort denominators are never blended".

**Ruling.** The runner ports **byte-verbatim first** — it carries 49 harness unit tests that must
empty-diff, and hiding the fix inside the ported constants would put it inside the thing being
validated. The fix is two-layered (EC-8):

1. **Invocation pinning, both sides.** The acceptance procedure (§6) invokes `python -m
   compat.runner` directly with an explicit five-module list and no `--stretch`, with identical
   argument vectors on both sides — the v1 pin needs no edit at all and stays read-only. A v1-side
   script bugfix is optional and is the operator's call; it is not a phase-3 dependency.
2. **An additive v2 fix, after the verbatim port empty-diffs.** The harness gains a
   `CLASSIC_MODULES` constant and a `--classic` flag as declared new code with new harness tests,
   and the ported `scripts/run_census.sh` uses it. The existing `--stretch` flag is left
   byte-identical and gains a test that pins its append-blending behavior, so the trap is
   documented in executable form rather than merely dodged.

### F2 — the recorded baselines are stale, and one cohort has no baseline at all

`PLAN.md` cites classic 135/345, expand 42/171, expand2 41/167. The 135 matches one dated report; the
pin's newest reports say 142/345 and 44/171; the newest expand2 says 87/167, and 41/167 matches no
report found. Report filenames do not track their generated timestamps. The fourth row — the
"full-extras facade cohort" — appears nowhere in the source repository under that name and has no
recorded count.

**Ruling.** The gate is byte-flat *across repositories*, not agreement with a historical number, so
the real acceptance is a fresh run of the same procedure on both sides. But a gate with a stale
baseline table invites someone to compare against the table. Therefore:

1. `PLAN.md`'s numeric table is **replaced** by a pointer: "baseline = the freeze-point run recorded
   in `task/census/baseline-<pin>.md`", with the four cohort numbers reproduced there and dated.
2. The freeze-point run is generated from the pin with the pinned venv recipe (§6) before any v2
   census is run, and its full interpreter manifest is recorded alongside it. A baseline whose
   environment is not recorded is not a baseline.
3. **The full-extras facade cohort is defined here** (§6.3) and its count recorded in the same run.

### F3 — the door-wiring inversion

In v1 the session crate depended on the Spark SQL crate and inlined the Spark function registry, the
analyzer rules, the TA kernels, and the cardinality configuration into `build()`, and `sql()` called
the Spark router directly. The binding got Spark semantics for free and named no SQL crate. Here,
those are exactly the two seams phase 1 inverted: a bare builder produces stock DataFusion.

A verbatim port of `PyReparkSession::__new__` therefore compiles, runs, and silently produces a
non-Spark session — and the failure mode is the worst kind: hundreds of facade tests failing in
confusing, semantics-shaped ways rather than one loud error.

**Ruling.** EC-2 is mandatory and is the *first* thing written in the binding's session module, with
its own test before any facade exists: a ported-session test asserting that a session built by
`PyReparkSession::__new__` resolves a Spark-only function and routes a Spark-only statement. Second
half of the finding: the Cargo dep list collapses `repark-core` + `repark-session` into one
`repark-core` (EC-1), and reviewers treat unchanged `repark_core::Error` lines as changed lines.

---

## 6. Census and acceptance, end to end

### 6.1 The environment is part of the pin

The census classifies real test outcomes, so the interpreter environment is an input to the result.
Both sides run the identical recipe:

- Python 3.12; `pyspark==4.1.2`; JDK Temurin 17 where a JVM is needed (it is not needed for the
  cohorts themselves — the redirect replaces pyspark's SQL layer — but the pin must be stated);
  `pyarrow>=25`; **`pandas>=2.1,<3`**; `maturin` 1.14.1; `uv` 0.9.5; Rust 1.96.0.
- The pandas major is load-bearing and non-negotiable: 55 rows in the always-green pin list were
  excluded because Apache's own test helpers import a pandas internal removed in pandas 3. A census
  run under pandas 3 is a different measurement.
- The Apache test tree is fetched at run time by sparse clone at the tag matching the installed
  pyspark. **Nothing from that tree is ever committed**, in either repository.
- The full `pip freeze` of the scratch interpreter is recorded verbatim in the run's report. Two runs
  whose freezes differ are not comparable, and the comparator says so rather than diffing anyway.

### 6.2 Generating the freeze-point baseline (v1 side, read-only)

In a read-only worktree of the private v1 engine repository at the pin — never a push, never a
fetch, never a write:

1. Create the scratch interpreter, install the facade editable plus the pinned oracle stack, then
   `maturin develop`. Record `pip freeze`.
2. **Stability run, first and mandatory.** Run the classic cohort **twice** and diff the two JSON
   outputs against each other before anything is compared across repositories. The Apache suite
   touches the filesystem, the clock, and a network-fetched source tree; a row that is not stable
   against itself cannot be evidence about a port. Any self-diff row is quarantined **by name** in
   the baseline file as known-unstable, excluded from the gate, and counted separately. A gate whose
   flake floor is unmeasured cannot distinguish a port defect from suite noise.
3. Classic cohort, explicit module list, no `--stretch`:
   `python -m compat.runner --modules test_functions,test_dataframe,test_types,test_column,test_readwriter --output classic.json --markdown classic.md`
4. Expand cohort: the same command with `--c3-expand` (which returns only its own six modules).
5. Expand2 cohort: `--c4-expand` (nine modules).
6. Full-extras facade cohort: §6.3.

The four JSON reports, the four markdown reports, and the freeze manifest are committed here under
`task/census/baseline-<pin>/`. They are evidence, not source: they are never edited by hand, and a
re-run replaces the whole directory in one commit.

### 6.3 The full-extras facade cohort — definition

The fourth acceptance row has never been defined. It is defined here as:

> **The entire facade test suite, executed against an installed wheel, with every optional extra
> present and every gate variable unset.**

Concretely: build the wheel; create a bare interpreter *outside* the workspace; install
`repark[pandas,polars,numpy,ml-ext]` plus `pytest` plus the parity package explicitly (the bare
interpreter is outside the uv workspace, so the parity package will not be resolved implicitly);
then run two invocations whose outputs are **both** recorded artifacts:

```
python -m pytest python/repark/tests --collect-only -q  > collected.txt
python -m pytest python/repark/tests -q --junitxml=facade.xml
```

The environment clauses are part of the definition, each enumerated in the recorded manifest:
every gate variable **unset, by name** (the full `REPARK_*` gate list plus the acceptance
variables — the manifest lists each one it verified absent); **no JVM on `PATH`** (pyspark-gated
tests must skip for the recorded reason, not accidentally run); **pyspark ABSENT** from the
interpreter (ten `importorskip` sites would otherwise silently change outcome class); **duckdb
ABSENT** (three `importorskip` sites, and duckdb is a dev-group dep rather than an extra, so an
unstated venv choice would silently vary the cohort); the four extras present **by name**
(`numpy`, `pandas`, `polars`, `ml-ext`).

The recorded quantity is a **pair of multisets, not a number**: the collected-name multiset (the
relocation-discipline artifact — names identical across repos modulo the declared deferral list) and
the `(node id → outcome)` multiset from the JUnit XML, where outcome ∈ {passed, failed, skipped,
xfailed, error}. Skips are first-class outcomes: a test that silently stops skipping is exactly as
interesting as one that stops passing. The headline count (passed / total) is derived and reproduced
in the report for human use. JUnit XML keeps the procedure dependency-free (no pytest plugin) and
node-id-keyed, which is what makes the comparison a multiset comparison rather than a score
comparison.

Why "full-extras": the ML, polars, pandas, and numpy paths are a large fraction of the facade and
are exactly the paths that a partial install silently skips. A cohort that lets an install decision
change its denominator is not a gate.

### 6.4 The comparator (new code)

v1 emits reports; nothing in either repository turns two reports into a verdict. Phase 3 builds
`python/repark-parity/compat/compare_reports.py`:

- Inputs: two census JSON reports (or two JUnit XMLs), plus the deferred node-id list and the
  quarantined-unstable list.
- It first compares the environment manifests and **fails loudly on any difference** rather than
  proceeding.
- It builds `{test_id → class}` on each side, subtracts the deferred list from the v1 side only,
  excludes quarantined rows on both sides (reporting them separately), and asserts: identical key
  sets, and identical class per key — a sorted-rendering byte comparison, no fuzzy matching, no
  aggregate-only comparison, no per-class tolerance.
- It re-asserts the two required denominators per cohort — `pass / all_collected` and
  `pass / engine_relevant` — and fails if either differs at all.
- **The checked-in ledger is the ONLY subtraction input.** There is no flag, environment variable,
  or config path by which a row can be excluded without appearing in the ledger — an undeclared
  subtraction is structurally impossible, and a unit test provokes one to prove the property.
- Output: a delta grouped by direction (pass→fail, fail→pass, class-change, appeared, vanished) with
  both classifications per cell, the deferred subtraction echoed so the reconciliation identity is
  visible, and a non-zero exit on any difference. Empty diff is the only pass.
- **Attribution rule.** Zero movement is the bar. Where a cell does move, acceptance requires it to
  be *attributable*: the moving cell must map to a deferred-by-name surface (an Apache row that
  exercises a JDBC or Excel read is the anticipated case, most plausibly inside `test_readwriter`).
  Every such cell is enumerated by name in the phase-close ledger with the surface it depends on and
  the post-milestone-one row that will close it. **Unattributed movement fails the phase.** This is
  the honest reading of "any movement is a finding to resolve, not noise to wave through": findings
  get resolved by naming, not by tolerance.

The comparator is new behavior, so it lands with its own unit tests (identical reports → empty diff
and exit 0; one moved cell → exit 1 naming it; a deferred cell present on one side only → passes;
mismatched environment manifests → loud failure) and the full verification panel.

### 6.5 Reconciliation

`(ported ∪ deferred) = the v1 pin totals` runs at the phase boundary, over three populations:

| population | ported side | deferred side |
|---|---|---|
| Rust — `repark-ml` | `cargo test -- --list`, identity map (no rename) | none expected |
| Rust — `repark-python` | `cargo test -- --list`, identity map (crate name and module paths unchanged) | none expected |
| Python — facade suite | `pytest --collect-only -q` | `task/port/deferred-python-tests.txt` |

Both Rust populations port under an **identity** map: the crate names and module paths are unchanged,
so relocation discipline's move-only shape applies and the sorted `--list` diff must be empty with no
declared renames. The Python population is move-only in the same sense — every ported test keeps its
node id — with a single declared deferral list. Any test that appears on both sides, or on neither,
is a failure of the phase, not a rounding error.

### 6.6 What the phase-close PR must show

1. The four baseline reports plus the freeze manifest, committed — including the stability-run
   self-diff and any quarantined rows, named.
2. The four v2 reports plus their manifest, committed.
3. Comparator output for all four cohorts: empty diff, matching denominators, exit 0 — or an
   attributed-movement table where every row names a deferred surface.
4. `--list` empty sorted diffs for `repark-ml` and `repark-python`; `--collect-only` empty sorted
   diff for the facade suite after applying the deferral list.
5. The deferred manifest reconciled and appended to its reconciliation log, now carrying both the
   four Rust rows and the Python node-id list, all pointing at the post-milestone-one bucket.
6. `PLAN.md`'s baseline table replaced by the recorded-run pointer (F2).
7. `make preflight` green; every `map.md` in lockstep; zero `#[ignore]`, zero skipped-in-CI, zero
   `--skip`.
8. The retrospective, and the named user-side items still open (§11).

---

## 7. The CI delta

### 7.1 Ordering constraint

The rust job must be fixed **before** the binding lands, not after: the current combined lint+test
job restores two cache prefixes onto one runner disk — the exact configuration v1 had to split after
three disk-exhaustion and linker-signal incidents — and it has no Python setup step, so
`cargo test --workspace` will fail to link the cdylib the moment `repark-python` arrives.

### 7.2 Job-by-job

| Change | Lands in | Required check? |
|---|---|---|
| Split `rust` → `rust-lint` + `rust-test`; add setup-python 3.12 to both (libpython); add the free-disk step and the debug/incremental env; align the cache keys with `cache-warm.yml` (prefix-key kept, `shared-key` per family added on both sides — without it rust-cache mixes the job id into the key and warm saves are never restored) | PR-1 | yes — replaces one context with two, pushed to branch protection in the same change |
| Panic-ban carve-out: workspace invocation excludes the binding; a second invocation runs it `--lib` with the FULL deny list — the five exception-macro sites carry a module-scoped `#![expect(clippy::disallowed_methods)]` (per-call-site cannot reach a macro expansion), keeping the spawn ban live for the binding | PR-3 (with the crate) | covered by `rust-lint` |
| `ci.yml` `python` job extended: `uv lock --locked`, the parity-harness pytest; renamed from "Python (ruff)" | PR-4 | yes — rename updates protection in the same change |
| `pip-audit.yml` ported as-is (weekly cron + path-filtered) | PR-4 | no — path-filtered checks must never be required |
| `wheels.yml`: job `smoke` = debug host build (no manylinux container), venv import smoke, **and the full facade suite against the packaged wheel**; always-run on `pull_request` with **no paths filter** (a path-filtered required check deadlocks PRs — twice, historically). Lands **with the wheel package, never before it** — a required check whose working directory does not exist reds `main` on merge | PR-5 | **yes** |
| `wheels.yml`: job `release-wheels`, tag-only, `manylinux: auto`, `--release`, shared-cache explicitly disabled, artifact upload | PR-5 | no |
| **No rust-cache step anywhere in `wheels.yml`** — the workflow is tag-triggered, so any cache restore is a cache-poisoning finding; the lookup-only variant is not a safe substitute | PR-5 | — |
| `ci.yml` `python` job: `check_lib_py` step added | PR-5 | covered by `python` |
| `parity-live.yml` ported and **armed**: Temurin 17 + setup-python 3.12 + Rust + uv, `uv sync --extra record`, `maturin develop`, live gate set to exactly `"1"`; triggers = nightly cron + `workflow_dispatch` **only**. v1's `pull_request` trigger is deliberately dropped: the live tier runs on merged code only | PR-6 | no |
| `aws-acceptance.yml` — net-new tier 2 (§7.4) | PR-6 | no |
| `release.yml` | not in phase 3 | — |

### 7.3 The facade job: a deliberate reversal

Recon recommends un-pausing v1's separate `facade` job, on the grounds that its stated pause reason
("until the repo goes public") has expired. **This design declines**, and says so plainly rather than
quietly.

The facade job runs the same suite as the wheels smoke job, from a `maturin develop` build. Under
the real-artifact rule, a develop build proves strictly less: producer and consumer compile
together, so boundary layout, symbol, and lifecycle mismatches are structurally invisible — the exact
failure class the rule exists to catch. So the packaged-wheel run is the one that must be required,
and running both on every PR buys a weaker duplicate of a signal we already block on. `make
py-test-facade` remains the fast local loop.

**Reversal trigger, pre-committed:** if the wheels `smoke` job's wall clock exceeds roughly fifteen
minutes, split it — import smoke stays required, and the facade suite moves to a develop-based job
that also becomes required, with the packaged-wheel facade run moving to push-on-main and tags.

Required checks after phase 3: `rust-lint`, `rust-test`, `guards`, `python`, `cargo-deny`, `taplo`,
`typos`, `zizmor`, and the wheels `smoke` job — nine, all always-run on `pull_request`.

### 7.4 Tier-2 live AWS — net-new design

There is no template: v1 never had a live-AWS workflow, only a locally-gated test module. The
contract is fixed by the project rules — nightly on the default branch plus manual dispatch, OIDC
only, merged code only, no self-hosted runners.

- **Triggers:** `schedule` (nightly, off-peak, minute-offset so it does not collide with the parity
  cron) and `workflow_dispatch`. **No `pull_request` trigger of any kind**, so a fork PR can never
  reach it.
- **Merged-code-only enforcement, mechanically:** the first step fails the job unless the ref is the
  default branch. Dispatch from a topic branch is refused rather than trusted, because `contents:
  read` on a dispatch does not by itself constrain the ref.
- **Credentials:** `permissions: { id-token: write, contents: read }` scoped to the job. Role
  assumption via the SHA-pinned AWS credentials action with a repository **variable** holding the
  role ARN and region, and a short session name. No long-lived keys, ever, anywhere.
- **Environment gate:** the job runs in a GitHub **environment** (`aws-acceptance`) carrying
  deployment protection — a manual dispatch still requires a human approval step before credentials
  can be minted.
- **Trust policy (user-side):** the IAM role trusts the GitHub OIDC provider with the subject
  constrained to **both** the default branch of this repository **and** the `aws-acceptance`
  environment — not the whole repo, and not a wildcard; a compromised workflow file on a topic
  branch cannot assume the role. Its permission policy is least-privilege over one scratch Glue
  database, one scratch S3 prefix, and one S3 Tables bucket.
- **Never-teardown as a permissions fact, not a convention:** the role's policy grants **no
  table-delete and no object-delete permission of any kind**. The harness's documented posture
  (create-only, scratch namespace, scratch prefix, no teardown path) is thereby enforced by IAM
  rather than promised by a docstring — a compromised or buggy job *cannot* delete, regardless of
  what it runs.
- **Scope:** the job runs the acceptance test module only — never the full facade suite. Blast
  radius and runner minutes both.
- **Inputs:** the acceptance gate variable set to exactly `"1"`; the three entity/date/id-column
  values as repository variables; the S3 Tables bucket ARN as a repository **secret**, because it is
  account-identifying and this repository is public. Absent secret ⇒ that leg skips, exactly as the
  ported test module already behaves.
- **Scratch accumulation:** a nightly create-only job accumulates scratch tables. Handled outside
  the workflow entirely: an S3 lifecycle expiry on the scratch prefix, configured once by a human —
  neither a delete path in CI nor a human remembering to reap. Glue scratch-database entries are
  reviewed manually at a documented cadence in the runbook.
- **Hygiene:** `timeout-minutes` set (this workflow, unlike the ported ones, gets an explicit wall);
  concurrency group with cancel-in-progress; all actions SHA-pinned; no secret ever echoed.

### 7.5 Makefile

Eight targets return, each dual-wired with the CI step it mirrors and landing in the PR that lands
its code: `check-lib-py`, `py-test`, `py-test-facade`, `py-lock-check`, `py-audit`, `build-wheel`,
`census`, `parity-live` (plus `develop`). The Makefile is an orchestrator carve-out; the targets land
as orchestrator commits on the owning PR's branch, as in phases 1 and 2.

---

## 8. Testing discipline for phase 3

- **The real-artifact rule arms here.** Any change to the PyO3 seams, Arrow C-stream export, IPC
  ingest, or the abi3/wheel surface needs a test crossing the built artifact. The wheels `smoke` job
  is that test's standing home; `maturin develop` runs never satisfy it. When it is unclear whether a
  change is boundary-class, it is.
- **Carry-forward traps are pins, not trivia**, and each ports with its test: the no-detach
  thread-local plus its Drop guard (abi3 cannot ask whether the GIL is held, so the flag *is* the
  contract, and losing it turns a nested generator into a process abort); the ingest drain holding
  the GIL for its whole duration (releasing it deadlocks a Python-iterator-backed stream); the
  stream-poll fence (the C callback is outside PyO3's trampoline, so an escaping panic aborts);
  export declaring the analyzed logical schema and ignoring `requested_schema`; the PyCapsule
  protocol rather than the pyarrow feature (a second interpreter-binding pin will not link).
- **Do not "clean up" on the way past.** The capsule-name constant is duplicated in two modules at
  the pin. It stays duplicated. Hoisting it is a defensible refactor and an indefensible phase-3
  diff; it is a one-line post-milestone-one change with nothing riding on it.
- **Test-only backdoors ship.** The `testing_*` session methods and the panic probe are exposed on
  the released module at the pin, undocumented and hidden on the Rust side. They port as-is:
  feature-gating them changes the cdylib's build shape and interacts with the `--all-features` ban.
  Recorded as an accepted risk with a post-milestone-one review trigger.
- **`cargo test --workspace`, never the all-features flag.** The `extension-module` feature is off by
  default precisely so the test binary links libpython; turning it on tells the binding not to.
- **Env-gated tiers stay opt-out by default**, each armed only by an exact `"1"`, each skipping with
  a visible reason otherwise. Delegated agents never set any of them and never touch AWS.
- **Gate provocation proofs** for `check_lib_py` and for the crate-DAG rows: the violating change is
  introduced, the failing run captured verbatim, reverted, and the clean run captured. Provocations
  are never committed.

---

## 9. PR slate (summary; the brief is authoritative)

Seven PRs. Verification tier follows phase-2 precedent: **slim** (builder plus one adversarial
verifier) for mechanical ports whose acceptance is an empty diff; **full four-lens panel**
(port-fidelity/census, design-conformance, testing-discipline, public-hygiene) for new or design
code. Two ordering rules are load-bearing and judge-verified: **the parity package lands before the
facade** (nine facade test files import it — the facade suite cannot even collect without it, and
the wheels smoke step installs it explicitly), and **the wheels workflow lands with the wheel
package, never before it** (a required check whose working directory does not exist reds `main`).

1. **Arming** — this design + the brief in-repo; the two crate-DAG tier rows + tier-4 name,
   pre-declared with provocation proofs; the rust job split (lint/test, setup-python on both,
   free-disk, cache re-key) with the protection-context swap; the testing-contract row-2 annotation
   (Q1); the dialect-module doc-drift rider (EC-6). No new code surface. *Slim.*
2. **`repark-ml`** — verbatim, identity census, empty sorted `--list` diff. The `diff -r` oracle
   against the port pin is **empty excluding `crates/repark-ml/map.md`**, which is regenerated
   under EC-7 (§1, §3) because five of its v1 links name paths that do not exist in this
   repository; `src/map.md` and every `.rs` / `Cargo.toml` byte are unchanged. *Slim.*
3. **`crates/repark-python`** — the whole crate in one PR (it cannot compile in halves): EC-1/2/3
   with the door-wiring pin test written first, EC-5 (the `EngineRuntime` type in `repark-core`,
   additive, + the instance here), EC-10 (`check_lib_rs` exception row), the error type-identity
   test, the panic-ban carve-out invocation, refuse-arm tests. No wheel is buildable yet and none is
   claimed. *Full panel.*
4. **`python/repark-parity` + census foundation** — comparison core + its 64-name generated test census + `compat/` +
   `bench/` verbatim; the additive `--classic` fix + the `--stretch` blending pin (EC-8); the new
   report comparator + its unit tests; `docs/port/census.md`; the uv workspace root **declaring the
   parity member only** (the facade member joins in PR-5 — declaring a missing member fails the
   lock); the extended + renamed python CI job; `pip-audit.yml`; **the recorded v1 freeze-point
   baseline, including the stability self-diff** — landed here, two PRs before the facade, so a
   baseline surprise (an Apache row moved by a refuse-armed surface) surfaces with runway to act.
   *Slim on the verbatim port; full design lens on the comparator, the flag, and the baseline.*
5. **`python/repark` (facade + suite) + the wheel path** — 53 modules verbatim; tests minus the
   empirically-generated deferral ledger (EC-4); the hygiene scrub with its ledger (EC-9); uv
   member extension + `uv.lock`; `check_lib_py` + Makefile wiring; map.md regeneration (EC-7);
   `wheels.yml` (smoke job → required, no paths filter; release-wheels tag-only). The real-artifact
   rule is discharged here for the first time: wheel import smoke + the packaged-wheel facade run.
   The PR body presents the diff as "literal copy plus the enumerated edit classes" and the panel
   reviews the enumeration. *Declared stall fallback:* split **within the PR** into a package
   commit and an immediately following test commit — never across a merge boundary. *Full panel on
   the edit classes; slim on the copy; a dedicated census lens whose evidence is the collect-only
   identity diff.*
6. **Tier-2 CI** — `parity-live.yml` ported and armed (nightly + dispatch, no PR trigger);
   `aws-acceptance.yml` net-new per §7.4. Depends on PR-5 (a workflow dispatching against a missing
   suite is a broken gate). *Full panel, with a mandatory security lens on the OIDC trust
   conditions, the permission scope, and the absence of any pull-request trigger.*
7. **Phase close** — the v2 census run, comparator outputs for all four cohorts, the reconciliation
   append, `PLAN.md` re-baseline, target-map notes, retrospective, and the linked operator cutover
   note (Q10). *Full panel — this PR is the acceptance claim.*

Order: **1 → 2 → (3 ∥ 4) → 5 → 6 → 7.** PR-3 and PR-4 have disjoint code footprints (crate tree vs
Python-harness tree) and may run in parallel; both carry orchestrator carve-out edits (`ci.yml`,
Makefile), so whichever merges second takes the union-merge-plus-full-re-gate recipe from phase 2.
The spine is otherwise strictly ordered because each stage is the previous stage's only proof.

Stall behavior (judge-verified): after every PR, `main` is green and shippable, advertises no
capability it cannot deliver, and has left no tests behind; the first point at which a wheel could
honestly be tagged is after PR-5.

---

## 10. Risks accepted

- **R1 — one large binding PR.** Roughly 6.6 KLOC of Rust cannot land in runnable halves. Mitigated
  by verbatim-copy provenance (the diff is reviewable as "copy plus the enumerated edit classes"),
  by landing the door-wiring test first, and by the full panel.
- **R2 — attributed census movement.** Deferring the Excel and Postgres readers may move Apache-suite
  cells — the read/write module is in the classic cohort and the JDBC formats are reachable from it.
  Mitigated by the attribution rule (§6.4), by the baseline landing two PRs before the facade (PR-4)
  so the discovery has runway, and by a pre-committed **escalation clause**: if the pinned baseline
  shows any Apache-cohort row whose classification depends on a refuse-armed surface, the connector
  crate is pulled forward into phase 3 — **the gate is never relaxed.**
- **R3 — the leaked temp views stay leaked through milestone one.** Deliberate (Q8). It is a real
  defect visible to users of time travel through the facade; it is recorded, issue-tracked, and
  scheduled immediately post-acceptance with its v1 pair.
- **R4 — test-only backdoors in a public wheel.** Accepted for parity (§8), reviewed
  post-milestone-one.
- **R5 — census reproducibility depends on a runtime clone of an external test tree.** If the
  upstream tag or repository moves, a re-run is not comparable. Mitigated by recording the resolved
  tag and the full environment manifest in every report, and by the comparator refusing to diff
  across differing manifests.
- **R6 — CI wall clock.** The required wheels job now builds a wheel and runs the full facade suite
  on every PR. Accepted for one phase, with the pre-committed split trigger in §7.3.
- **R7 — the Python re-home is deferred, so the public target layout and the shipped layout disagree
  until after milestone one.** Mitigated by dated notes in the target maps and by the migration shape
  being written down here rather than left to discovery.
- **R8 — the live-AWS job accumulates scratch tables.** Accepted in preference to putting a delete
  path in CI; manual reaping documented.

---

## 11. What stays user-side

Not delegable, and none of it blocks the engineering slate except where noted:

1. Branch-protection context updates after each job split or rename (an engineering PR proposes them;
   only a maintainer can apply them).
2. The AWS role, its OIDC trust policy scoped to the default branch **and** the `aws-acceptance`
   environment, its least-privilege no-delete policy, the S3 lifecycle expiry on the scratch prefix,
   and the repository variables and secret the tier-2 workflow reads — **blocks PR-6's first green
   run**, not its merge.
3. The first `workflow_dispatch` acceptance run of the live oracle and live AWS workflows: runner
   behavior cannot be proven locally, and the honest local proof is the make target plus the workflow
   linter.
4. PyPI and crates.io trusted publishers, the one-time classic token per first-ever crate name, and
   its revocation — first release only.
5. The **cutover sequencing** decision under single-writer-per-table (Q10) — a milestone-one blocker
   that is an operations call.
6. Declaring the v1 engine bugfix-only at acceptance, and the optional v1-side census-script bugfix
   (F1), neither of which any agent may perform: that repository is read-only at the pin.

