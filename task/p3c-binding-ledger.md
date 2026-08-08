# Unit ledger — P3C: `crates/repark-python` (the PyO3 binding port)

**Unit:** phase-3 PR-3 · **Brief:**
[../briefs/phase-3-python-facade.md](../briefs/phase-3-python-facade.md) §1 "PR-3" · **Design:**
[../docs/design/python-facade.md](../docs/design/python-facade.md) §1 (edit classes
`EC-1, EC-2, EC-3, EC-5, EC-6, EC-10`), §2.1 (tier-4 row), §2.2 (dep edges + the two deliberate
non-edges), §3 (the edit-class definitions), §4 Q7 (`EngineRuntime` home), §5 F3 (the door-wiring
inversion), §9 PR-3 · **Port-Source:** v1 `main` @ `fc3f48102` · **Status:** IN FLIGHT ·
**Stacked on:** phase-3 PR-2 ([p3b-ml-ledger.md](p3b-ml-ledger.md))

## Scope

Land the whole PyO3 binding crate in one PR — it cannot compile in halves (design R1) — as a
**literal copy plus a closed, enumerated set of edit classes**. Anything in the diff that fits no
class below is a defect, not a judgement call.

`cp -a` of `v1-pin/crates/repark-python` → `crates/repark-python`: 10 files (`Cargo.toml`,
three `map.md`, five `src/*.rs`, `tests/bindings.rs`), 6,853 lines. Then, and only then, the edit
classes.

Out of scope, and none of it is claimed: the wheel (`python/repark`, PR-5 — **no wheel is
buildable from this PR**), the parity/census machinery (PR-4), any ANSI-from-Python surface
(design §4 Q2 — zero), the Spark-door time-travel temp-view leak (§4 Q8 — deliberately untouched),
and every carve-out file (`.github/`, `AGENTS.md`, `CLAUDE.md`, `Makefile`, branch protection),
which the orchestrator commits on this branch. **No AWS**: no `REPARK_*`, `TABLE_BUCKET_ARN`,
`REPARK_PG_DSN` or any gate/credential variable was set at any point, and no test in this PR
reaches a network.

## Edit classes, applied

### EC-1 — import re-home + the Cargo collapse

**Files:** `crates/repark-python/Cargo.toml`, `src/lib.rs`, `src/session.rs`, `src/map.md`,
`map.md`; root `Cargo.toml`.

The design's "one live prefix" rule is confirmed empirically at the pin, not taken on faith:

```
$ grep -rn 'repark_catalog::\|repark_write::\|repark_sql::' <v1-pin>/crates/repark-python/{src,tests}
$ echo $?
1                       # zero occurrences of all three — nothing else to re-home
$ grep -rn 'repark_session::' <v1-pin>/crates/repark-python/src | wc -l
7
```

All seven rewritten to `repark_core::` (3 doc-comment mentions, `use repark_session::ReparkSession`,
`repark_session::engine_err`, `repark_session::TimeTravelOpts`, and
`repark_session::PostgresReadOptions` — the last one deleted with its refuse-arm, EC-3).
`repark_functions::` (41 sites) and `repark_ta::` (2 sites) are unchanged, as the design states.

**The Cargo collapse (mandatory, not cosmetic).** v1 named six repark crates; this one names five:

| v1 dep | here | why |
|---|---|---|
| `repark-core` + `repark-session` | **one** `repark-core` | `repark-core` re-exports `Error`/`ErrorClass`/`Result` from `repark-common`; naming both entries makes the identifiers collide |
| — | **`repark-spark`** ADDED | the door the constructor now installs (EC-2 / §5 F3); v1's binding named no SQL crate at all |
| `repark-excel` | **DROPPED** | its two pymethods survive as EC-3 refuse-arms |
| `repark-functions`, `repark-ta` (feature `datafusion`), `repark-ml` | unchanged | — |

**Deliberate non-edges, recorded as a decision:** no `repark-sql` and no `repark-iceberg`. The
crate-DAG guard cannot enforce this (a tier-4 crate may legally name anything below it), so it is
enforced by review and stated in `crates/repark-python/map.md` + `crates/map.md`.

**The type-identity mechanization.** Every `repark_core::Error` / `ErrorClass` line in this crate
is *textually identical* to the pin yet now resolves through a different crate. A rule that depends
on a reviewer remembering that is not a rule, so `src/tests.rs` gains four `const _` coercions
(both directions, `Error` and `ErrorClass`) plus the runtime companion
`repark_core_error_is_the_repark_common_error_type`. A future re-split of the taxonomy stops the
crate compiling at the seam instead of silently rebinding the exhaustive fold.

Naming `repark_common` requires an edge; it is a **dev-dependency**, so the product dep list stays
exactly the five crates §2.2 names and the crate-DAG guard (normal edges only) is unaffected.

### EC-2 — the door-installed builder (written FIRST, per §5 F3)

**Files:** `src/session.rs` (the single construction site), `Cargo.toml` (`repark-spark`).

There is exactly **one** `ReparkSession` construction site in the crate (`src/session.rs:109` at
the pin, inside `PyReparkSession::__new__`). It becomes:

```rust
let session = builder
    .with_sql_dialect(Arc::new(repark_spark::SparkDialect))
    .with_extension(Arc::new(repark_spark::SparkExtension))
    .build()
    .map_err(to_py_err)?;
```

**Explicit NON-sites, ported verbatim as the design requires:** `src/column.rs`'s throwaway
`SessionContext` provisionings (`SessionContext::new()` + `repark_functions::register_all` +
`analyzer_rules()` — line 357 in the production `expr`/`sql` path plus the sibling consumer
contexts in its `#[cfg(test)]` battery at 2007 / 2045 / 2103), and `src/dataframe.rs`'s three
`SessionContext::new_with_config(...)` test fixtures. None is a *session* construction site;
mechanically applying EC-2 there would change standalone-Column semantics. Zero bytes changed in
either file.

**The pin test, written before the rest of the crate was touched:**
`spark_doored_session_resolves_spark_function_and_routes_spark_statement` drives a session built by
`PyReparkSession::__new__` and asserts both halves —

- *registry*: `SELECT weekofyear(DATE '2021-01-01')` resolves **and evaluates to 53** on the Arrow
  path (`collect`, value AND type — an `Int32Array`, not `show` text). `weekofyear` is a Spark-only
  name; stock DataFusion spells the concept `date_part('week', …)`.
- *router*: `TRUNCATE TABLE any_table` returns the Spark router's own C4-L-001 refusal (naming
  `INSERT OVERWRITE`), folded to `UnsupportedOperationException`. Stock DataFusion cannot produce
  that message.

Mutation observables are stated in the test's doc block: drop `.with_extension(...)` → part 1 reds
with "Invalid function 'weekofyear'"; drop `.with_sql_dialect(...)` → part 2 reds because
DataFusion's generic unsupported-statement error answers instead. This is the class of failure the
test exists for: a verbatim port of `__new__` *compiles and runs* and silently produces a non-Spark
session.

### EC-3 — refuse-arms

**Files:** `src/session.rs` (three pymethods + one shared helper; the `excel_options_from_map` /
`parse_excel_bool` option parsers deleted with the reader), `Cargo.toml` (`repark-excel` dropped).

`read_excel`, `excel_sheet_names` and `read_postgres` keep their **exact** pin name, arity,
`#[pyo3(signature = …)]` defaults and `#[allow]`s — including `read_postgres`'s nine arguments —
and route through one helper, `deferred_reader_error(surface)`, raising
`UnsupportedOperationException` whose message names (a) the surface, (b) "scheduled
post-milestone-one", (c) the tracking row: `task/todo.md` → "Post-milestone-one (BACKLOG)" →
`repark-postgres` + `repark-excel`.

The deferral boundary is drawn **at the Rust binding and nowhere else**, which is what lets
`python/repark/src/**` port byte-identical in PR-5 and keeps the deferral visible in one file.

`read_postgres` binds and drops every argument **without formatting any of them**: `url` and
`properties` can carry a password or a DSN, and a refusal must not become the thing that leaks one.
The test asserts that negatively (no `postgresql://`, no sentinel secret in the message).

Three tests, one per arm — a refusal is a behavior:
`read_excel_refuses_with_named_unsupported_operation`,
`excel_sheet_names_refuses_with_named_unsupported_operation`,
`read_postgres_refuses_with_named_unsupported_operation`.

### EC-5 — `EngineRuntime` (type → `repark-core`, instance → `repark-python`)

**Files (new):** `crates/repark-core/src/runtime.rs`, `crates/repark-core/src/runtime/tests.rs`,
`crates/repark-core/src/runtime/map.md`. **Files (edited):** `crates/repark-core/src/lib.rs`,
`crates/repark-core/Cargo.toml`, `crates/repark-core/map.md`, `crates/repark-core/src/map.md`,
`crates/repark-python/src/session.rs`.

Per §4 Q7 (which honors, rather than reverses, the phase-1 omissions ledger): the **type** lands in
`repark-core` — additive, tier-legal, publicly documented as "the embedding's executor handle; core
never constructs one" — and the process-wide `OnceLock` **instance** stays in the binding.
`runtime.rs` has no `Runtime::new`, no `Default`, and one constructor that takes an `Arc<Runtime>`
the embedder already owns; core still never blocks on its own behalf. `tokio` becomes a normal dep
of `repark-core` purely to *name* the type — **no new package resolves**, since DataFusion already
pulls tokio into the lock.

In the binding, `static SHARED_RUNTIME: OnceLock<Arc<Runtime>>` becomes
`OnceLock<EngineRuntime>`; `shared_runtime()` keeps its exact signature and race handling. Same
lifetime, same behavior, and the pin test keeps its name:
`sequential_sessions_share_one_tokio_runtime` (unchanged bytes, still green).

### EC-6 — the PR-2 `docs/ml-design.md` rider, DISCHARGED

**Files:** `crates/repark-ml/Cargo.toml:6`, `crates/repark-ml/src/lib.rs:3`,
`crates/repark-ml/src/logistic_regression.rs:199`, `crates/repark-ml/src/error.rs` (the `Singular`
`#[error(...)]` string) + a new `#[cfg(test)] mod tests` in `error.rs`;
`crates/repark-ml/map.md`, `crates/repark-ml/src/map.md`.

**This is a DECLARED edit to a crate PR-2 landed as verbatim**, recorded here with its citation:
design §3 **EC-6, second rider**, and `p3b-ml-ledger.md` "F-2", which assigned discharge to PR-3
precisely because the pointer becomes reachable from a user surface only when the binding wires it.
All four sites now name the in-repo authority, `docs/design/python-facade.md` §4 Q3.

`error.rs:52` was the one that mattered: it is inside a `thiserror` format string, so the dead
pointer was **emitted to end users at runtime**. Its new text is pinned by
`error::tests::singular_message_points_at_the_in_repo_ml_authority`, which asserts the new pointer
is present, `ml-design.md` is absent, and the surrounding diagnostic (pivot, detail, the
"refuses pseudoinverse / silent regularization" clause) is undisturbed — a repoint, not a rewrite.

**`repark-ml` census impact: test ADDITIONS only.** No existing test name changed, moved or
vanished (34 → 35). Verbatim status of the crate is now: every `.rs` byte identical to the pin
**except** the four repointed comment/string lines and the appended test module — enumerated above,
nowhere else.

### EC-10 — the `check_lib_rs` EXCEPTIONS row

**Files:** `scripts/check_lib_rs.py`, `scripts/map.md`.

`crates/repark-python/src/lib.rs` is 217 lines against the guard's 150-line default. It is a
**manifest** — ~25 doc lines, five `mod` decls, three `pub use`, the five-member `create_exception!`
taxonomy (each with its own PySpark-parity docstring), the two error folds, the env-gated tracing
init, and the `#[pymodule]` registration — and it already uses the sanctioned file-backed test
module (`#[cfg(test)] mod tests;` → `src/tests.rs`). Row added at ceiling **230** (measured 217)
with the reason and a stated ratchet ("if the exception taxonomy moves to its own module").
Without the row every slate reds on the crate's arrival.

### Workspace wiring (mechanical, not an edit class)

- Root `Cargo.toml`: `crates/repark-python` added to `[workspace] members`; `pyo3 = { version =
  "0.29", features = ["abi3-py312"] }` added to `[workspace.dependencies]` at the v1 root's exact
  spec, comment included (the 0.28 RUSTSEC rationale). `tokio`, `futures`, `tracing`, `arrow` were
  already present at v1's specs; `tracing-subscriber` is spelled inline in the crate exactly as at
  the pin (feature-lean `fmt`/`env-filter`/`std`/`ansi`), so no workspace entry was added for it.
  **No `[workspace.dependencies] repark-python` entry** — the v1 root has none either, and nothing
  may ever depend on the binding.
- The crate keeps its **LOCAL** `[lints]` block (`unsafe_code = "allow"` + clippy all/pedantic at
  priority −1); it does not inherit `[lints] workspace = true`, and the root already reserved the
  carve-out. The stale future-tense wording there ("will NOT inherit") is corrected to present
  tense — comment-only.
- `.cargo/config.toml` already carries `PYO3_PYTHON = "python3"`, byte-identical to the v1 pin's.
  **No edit was needed** (verified by diff, not assumed).
- `Cargo.lock` committed.

### map.md lockstep (EC-7 in spirit — declared, PR-2 precedent)

The crate's three `map.md` files port with their v1-only claims rewritten **truthfully**, never
ported stale (design §3 EC-7; CLAUDE.md's map.md-accuracy rule is hard). Rewrites, each because the
v1 text is FALSE here:

- `../repark-session/map.md` → `../repark-core/map.md`; the `repark-session` dep line → the
  five-crate list with the collapse and the two non-edges; `repark-excel` → the EC-3 refuse-arms.
- `python/repark/**` references (facade modules, `errors.py`, three facade test files) labelled
  "**lands phase-3 PR-5**; not in the tree yet" rather than linking into empty space.
- `task/pg2-pg-runtime-ledger.md` (no counterpart here) → the `StreamingBatchReader` rustdoc,
  which carries the same rationale.
- EC-2 / EC-3 / EC-5 / EC-10 facts added, plus three new `## Debug` rows (the two refusal symptoms
  and "a Spark-only function fails on a `PyReparkSession`" → the dropped door install).

Also updated in the same commit: `crates/map.md` (the tier-4 row, the DAG sentence with the
non-edges, two Debug rows), root `map.md` (root `Cargo.toml` changed), `crates/repark-core/map.md`
+ `src/map.md` (the `tokio` dep and the `runtime` module), the new
`crates/repark-core/src/runtime/map.md`, `crates/repark-ml/map.md` + `src/map.md` (rider
discharged), `scripts/map.md` (EC-10), and `task/map.md` (this ledger).

## Census obligation — identity map + declared additions (REQUIRED, DISCHARGED)

Design §6.5: both Rust populations port under an **identity** map — crate names and module paths
unchanged, so the sorted `--list` diff must be empty **except** tests this PR declares as new. The
counts are generated, never hand-written.

```
# v1 side (READ-ONLY port source, built only to enumerate)
(cd <v1-pin> && cargo test -p <crate> -- --list 2>/dev/null | grep ': test$' | sort)
# v2 side (this worktree), same command
```

### `repark-python` — 49 → 54 (5 declared additions, 0 removals, 0 renames)

```
$ diff py-census-v1.txt py-census-v2.txt
39a40
> session::tests::excel_sheet_names_refuses_with_named_unsupported_operation: test
40a42,43
> session::tests::read_excel_refuses_with_named_unsupported_operation: test
> session::tests::read_postgres_refuses_with_named_unsupported_operation: test
41a45
> session::tests::spark_doored_session_resolves_spark_function_and_routes_spark_statement: test
45a50
> tests::repark_core_error_is_the_repark_common_error_type: test
```

Every hunk is an `>`-only addition; there is not one `<` line and not one changed line. The five:

| test | edit class it pins |
|---|---|
| `session::tests::spark_doored_session_resolves_spark_function_and_routes_spark_statement` | EC-2 (both halves of the door) |
| `session::tests::read_excel_refuses_with_named_unsupported_operation` | EC-3 |
| `session::tests::excel_sheet_names_refuses_with_named_unsupported_operation` | EC-3 |
| `session::tests::read_postgres_refuses_with_named_unsupported_operation` | EC-3 (+ the no-credential-echo property) |
| `tests::repark_core_error_is_the_repark_common_error_type` | EC-1 (companion to the four `const _` coercions) |

### `repark-ml` — 34 → 35 (1 declared addition, 0 removals, 0 renames)

```
$ diff ml-census-v1.txt ml-census-v2.txt
8a9
> error::tests::singular_message_points_at_the_in_repo_ml_authority: test
```

PR-2's 34 are byte-for-byte the same names. The one addition is the EC-6 rider's message pin.

### `repark-core` — +1 (declared, not a ported population)

`runtime::tests::engine_runtime_clone_shares_one_executor_and_drives_futures`, plus one doc-test on
`EngineRuntime`. `repark-core` is not one of design §6.5's two identity populations (it is a
phase-1 crate with its own recorded census), but EC-5 adds engine API here, so the addition is
declared rather than left to be discovered: 87 → 88 unit tests, 0 → 1 doc-tests.

## Gate results

Run in this worktree. `--workspace`, **never** `--all-features`, **never** `--no-verify`.
`make ci` is not run whole in this PR: its `rust-panic-ban` target still carries the pre-carve-out
workspace invocation, and the carve-out is an orchestrator edit that lands after this commit
(design §7.2). Both halves of the ban are therefore run here by raw `cargo`, mirroring exactly what
the carved-out target will do.

| gate | command | result |
|---|---|---|
| format | `cargo fmt --all -- --check` | exit 0, no diff |
| clippy (general) | `cargo clippy --locked --workspace --all-targets -- -D warnings -A clippy::disallowed_methods` | `Finished` — zero warnings |
| panic ban (a) | `cargo clippy --locked --workspace --exclude repark-python --lib --bins -- -D clippy::disallowed_methods -D clippy::unwrap_used -D clippy::expect_used -D clippy::panic -D clippy::todo -D clippy::unimplemented -D clippy::unreachable` | `Finished` — clean |
| panic ban (b) | `cargo clippy --locked -p repark-python --lib -- -D clippy::unwrap_used -D clippy::expect_used -D clippy::panic -D clippy::todo -D clippy::unimplemented -D clippy::unreachable` | `Finished` — exit 0. Emits 5 **warnings** (not errors) for `create_exception!`'s `Result::expect`, which is precisely why this invocation omits `-D clippy::disallowed_methods`, per clippy.toml's recorded carve-out |
| check | `cargo check --locked --workspace` | `Finished` |
| tests | `cargo test --locked --workspace` | all green — see below |
| crate DAG | `./scripts/check_crate_dag.sh` | `crate-dag: 16 internal edges clean across 9 of 9 mapped crates` |
| lib.rs | `./scripts/check_lib_rs.sh` | `lib-rs: 9 crate roots clean (no inline test modules; ceilings held)` — green **with** the EC-10 row |
| taplo / typos / ruff | `make toml-check`, `make spell-check`, `make py-lint`, `make py-format-check` | clean |
| map.md lockstep | pre-commit hook (`scripts/check_map_md.sh`) | passed on the single commit |
| hygiene | both mandated forbidden-pattern passes (staged diff vs `main`; commit-metadata log) | **0** matches |

> **This table proves green-on-a-clean-tree only, which docs/testing.md says proves nothing about
> detection.** The detection evidence for the two gates this PR ADDS is in "Provocation proofs"
> below (added by the 2026-08-08 fix pass); read that section, not this table, when asking whether
> a gate works.

`cargo test --locked --workspace`: `repark-python` **33** unit + **21** `tests/bindings.rs` = 54,
matching the `--list` census; `repark-ml` **35**; `repark-core` **88** + 1 doc-test; every other
crate unchanged from PR-2's recorded run. Every reported line `0 failed; 0 ignored`. The
`bindings.rs` suite boots a real embedded CPython 3.12 through the `auto-initialize` dev-dep — no
wheel involved, and none is claimed by this PR.

## Provocation proofs

Added 2026-08-08 by the fix pass (verify-panel F-5). Required by docs/testing.md "Gate provocation
proofs": *"A new mechanical gate … is not 'done' because it runs green — green on a clean tree
proves nothing about detection… A gate with no recorded provocation is treated as unproven — same
standing as an untested behavior."* The "Gate results" table above records only green-on-clean-tree
runs, which is exactly what that rule says proves nothing. **Every provocation below was reverted;
no provocation identifier (`_provocation`, `inline_probe`, `ceiling provocation`) exists in the
tree** — confirm with `grep -rn '_provocation\|inline_probe\|ceiling provocation' crates/ scripts/`
(exit 1).

This PR adds two mechanical gates: **(G1)** the new `cargo clippy -p repark-python --lib`
invocation in `make rust-panic-ban` (orchestrator commit `344cde2`, mirrored by ci.yml's
`rust-lint` job), and **(G2)** the new `repark-python` row in `scripts/check_lib_rs.py`'s
`EXCEPTIONS` table (ceiling 230).

### G1 — the `repark-python` panic-ban invocation

| # | provocation | command | result |
|---|---|---|---|
| **P-1 (must-FAIL, control)** | `let _provocation: i32 = "1".parse::<i32>().unwrap();` inserted into `deferred_reader_error` (`src/session.rs`, a production `--lib` path) | `cargo clippy --locked -p repark-python --lib -- -D clippy::unwrap_used -D clippy::expect_used -D clippy::panic -D clippy::todo -D clippy::unimplemented -D clippy::unreachable` | **exit 101** — ``error: used `unwrap()` on a `Result` value`` → ``error: could not compile `repark-python` (lib) due to 1 previous error``. The panic half of the ban detects. |
| **P-2 (must-FAIL, and it does NOT)** | `fn _provocation_spawn() { let _ = tokio::spawn(async {}); let _ = tokio::task::spawn_blocking(\|\| 1); }` inserted into `src/session.rs` | same command as P-1 | **exit 0** — ``warning: `repark-python` (lib) generated 9 warnings`` / ``Finished `dev` profile``. **The async cancel-safety half of the ban does not detect.** This is the recorded cost of the carve-out; see F-2 below. |
| **P-3 (must-PASS)** | none — clean tree | same command as P-1 | exit 0, 5 `create_exception!` warnings (the false-fire the carve-out exists for) |

### G1 remedy — what was tried, and what actually works

The panel proposed the narrow fix as *"keep `-D clippy::disallowed_methods` and put
`#[expect(clippy::disallowed_methods, reason=…)]` on the five `create_exception!` sites"*. That was
tried and **disproved**:

| # | form | command | result |
|---|---|---|---|
| **P-4** | outer `#[expect(clippy::disallowed_methods, reason = …)]` on each of the 5 `pyo3::create_exception!(` invocations in `src/lib.rs` | `cargo clippy --locked -p repark-python --lib -- -D clippy::disallowed_methods …` | **exit 101**, all five still error: ``error: use of a disallowed method `core::result::Result::expect` --> crates/repark-python/src/lib.rs:119:1`` … ``note: this error originates in the macro `$crate::create_exception_type_object```. A lint level on a macro *invocation* is not carried into the expansion — **the per-call-site form does not work here.** |
| **P-5** | `mod exceptions { #![expect(clippy::disallowed_methods, reason = …)] … }` wrapping exactly the five macro sites (inner attribute), + `pub use exceptions::{…};` | same command, with P-2's spawns still present | **exit 101 with only the two spawn errors left** — ``error: use of a disallowed method `tokio::task::spawn_blocking` --> crates/repark-python/src/session.rs:55:13`` and its `tokio::spawn` sibling; **zero** `create_exception!` errors. **The module-scoped inner attribute is the form that works**, and it restores the spawn ban. |

P-5 was **not landed** by this fix pass: it only takes effect paired with restoring
`-D clippy::disallowed_methods` on the second `make rust-panic-ban` invocation, and the Makefile is
an orchestrator-only carve-out file (brief §"Carve-outs"). Landing the `lib.rs` half alone would be
a non-verbatim structural edit to ported bytes with **zero** observable effect. Both halves are
handed to the orchestrator as one change — see F-1/F-2.

### G2 — the `check_lib_rs.py` EXCEPTIONS row (`repark-python`, ceiling 230)

| # | provocation | command | result |
|---|---|---|---|
| **P-6 (must-FAIL, ceiling rule)** | 20 comment lines appended to `crates/repark-python/src/lib.rs` (218 → 238) | `./scripts/check_lib_rs.sh` | **exit 1** — `ERROR: repark-python src/lib.rs is 238 lines (ceiling 230). Reason on file: … Sanctioned outs: (1) move production code into a named module with pub use re-exports, or (2) edit EXCEPTIONS in scripts/check_lib_rs.py with a reason (ceilings ratchet down only).` / `lib-rs: FAIL — 1 violation(s) across 9 crate roots`. **The row raises the ceiling, it does not disable the rule.** |
| **P-7 (must-FAIL, inline-test rule)** | `#[cfg(test)] mod inline_probe { #[test] fn t() {} }` appended to the same root | `./scripts/check_lib_rs.sh` | **exit 1** — `ERROR: repark-python src/lib.rs:220: inline #[cfg(test)] mod inline_probe { … } is forbidden — move the body to src/inline_probe.rs and leave `#[cfg(test)] mod inline_probe;` (file-backed only).` **The EXCEPTIONS row buys ceiling slack only; rule 1 still bites the exempted crate.** |
| **P-8 (must-PASS)** | none — clean tree | `./scripts/check_lib_rs.sh` | exit 0 — `lib-rs: 9 crate roots clean (no inline test modules; ceilings held)` |

P-6 also corrected a stale number: the row's `# measured 217` comment (and `scripts/map.md`'s
"217-line PyO3 crate root") were off by one against the landed file. Measured is **218**; both are
now 218. The ceiling is unchanged at 230 and still ratchets down only.

## Findings from the verify panel (2026-08-08)

Nine MED findings; dispositions below. **F-1, F-2, F-8 and the AGENTS.md half of F-9 are
ORCHESTRATOR ACTIONS** — they live in `AGENTS.md` / `CLAUDE.md` / `Makefile`, which the brief
(§"Carve-outs") makes orchestrator-only and which the fix pass is forbidden to touch. They are
itemized here, with the evidence already gathered, so the orchestrator can land them in one commit
on this branch.

### FIXED in this pass

**F-3 — `impl From<Arc<Runtime>> for EngineRuntime` was untested public engine API.**
*Reproduced:* `grep -rn 'EngineRuntime' crates/ --include=*.rs` → the impl at `runtime.rs:72` has
**zero** callers; no `.into()` / `From::from` construction exists anywhere; `runtime/tests.rs`
exercises only `new` / `clone` / `runtime()` / `block_on`. *Fixed:* the impl is **deleted**. EC-5
requires the type, not the conversion; design §8 forbids defensible-but-avoidable additions in a
fidelity phase; docs/testing.md requires a test per behavior. The module doc now states the surface
is exactly one constructor + one accessor + `block_on` and why, and
`crates/repark-core/src/map.md` records the removal so it is not re-added by reflex. *Proved:*
`cargo test --locked -p repark-core` green; the ledger's EC-5 paragraph ("one constructor that
takes an `Arc<Runtime>`") and `runtime/map.md`'s Debug row ("the only constructor takes an
`Arc<Runtime>`") — both previously **wrong** — are now literally true.

**F-4 — `read_postgres`'s no-credential-echo pin covered only `url`.**
*Reproduced:* the claim (code comment `session.rs:339`, the EC-3 ledger text, and
`src/map.md:293`) names **two** vectors, `url` AND `properties`; the test passed
`properties=None`. Mutation B — replace the refusal with
`UnsupportedOperationException::new_err(format!("… props={}", format!("{properties:?}")))` — was
**GREEN**: `test session::tests::read_postgres_refuses_with_named_unsupported_operation ... ok`. A
properties-only leak shipped undetected. *Fixed:* the pin now passes
`Some(HashMap::from([("password".to_owned(), "sentinel-property-secret".to_owned())]))` and adds a
second negative assertion. Two **distinct** sentinels (rather than reusing `sentinel-secret`) so a
leak of either vector is pinned independently rather than by accident of a shared substring.
*Proved:* clean tree GREEN; Mutation B re-applied → **RED** with
`a refusal must never echo the connection PROPERTIES — they may carry credentials: … props=Some({"password": "sentinel-property-secret"})`; mutation reverted. Product code was already
safe (`let _ = (…)` binds and drops without formatting) — it was the pin that was short.

**F-5 — two mechanical gates landed with no provocation proofs.**
*Reproduced:* `grep -in "provocation\|provoke" task/p3c-binding-ledger.md` → **exit 1, zero hits**,
against `task/p3a-arming-ledger.md:62` and `task/p1a-workspace-arming-ledger.md:52` which both
carry the section. *Fixed:* the "Provocation proofs" section above, with eight recorded
provocations (P-1…P-8) covering both gates in both directions. *Proved:* P-2 is the concrete cost
F-5 predicted — an unprovoked gate hid a real hole for a full review cycle.

**F-6 — real-artifact deferral recorded nowhere in the testing contract.**
*Reproduced:* `grep -n "real-artifact\|wheel\|boundary" task/port/deferred-tests.md` → no matching
obligation row; `docs/testing.md:114` carries no PR-3 carve-out. The design waives it (§9 PR-5) but
CLAUDE.md's precedence chain puts docs/testing.md **above** a design document, so a design waiver
does not discharge a contract obligation. *Fixed:* new section **"Deferred testing-contract
obligations (NOT v1 test names)"** in `task/port/deferred-tests.md`, deliberately outside the
`(ported ∪ deferred)` arithmetic (it is an obligation, not a v1 test name), with owner (PR-3),
creditor (`docs/testing.md:114`), discharger (PR-5) and the blocking clause: *PR-5's acceptance is
blocked on this row.* *Proved:* the row is now greppable from the manifest a future agent actually
reads, and the deferral has a named owner instead of living only in a design §.

**F-7 — `clippy.toml:40-42` described the carve-out in future tense AND misstated it.**
*Reproduced:* `sed -n '40,42p' clippy.toml` → "When `repark-python` … **lands**, it gets a
carve-out … that package **is gated with unwrap_used/expect_used only**", while the landed second
invocation denies **six** lints. `clippy.toml` is not on the carve-out list (ledger line 24), so it
was always the builder's to fix. *Fixed:* the block now states the crate LANDED in phase-3 PR-3,
enumerates all six denied lints, names the omitted one (`disallowed_methods`), and records the
**known cost** (the spawn ban is off for this crate — P-2) plus the proven remedy and the disproof
of the per-call-site form (P-4/P-5). *Proved:* every claim in the new text is one of the recorded
provocations above; `cargo clippy` behavior unchanged (comment-only edit).

**F-9 (PROJECT.md half) — `PROJECT.md:64` still called `repark-python` "the future crate".**
*Reproduced:* `grep -n 'repark-python' PROJECT.md` → `64:- **\`unsafe_code = "forbid"\`**
workspace-wide EXCEPT the future \`crates/repark-python\`.` The crate is in the tree and already
sets the allow (`crates/repark-python/Cargo.toml:19-20`). `PROJECT.md` is **not** on the carve-out
list, so this half is the builder's. *Fixed:* the line now reads "EXCEPT `crates/repark-python`
(landed phase-3 PR-3; the crate sets a local `unsafe_code = "allow"` …)". *Proved:*
`grep -n 'future .crates/repark-python' PROJECT.md` → exit 1. The `AGENTS.md:102` /
`CLAUDE.md:122` halves remain — see F-9-ORCH.

### ORCHESTRATOR ACTIONS — not fixable from this branch's builder surface

**F-1 / F-2 (one change, two findings) — the panic-ban carve-out is a crate-wide escape, and it
disarms the spawn ban.** `AGENTS.md`'s hard rule reads *"Escape = per-call-site
`#[expect(clippy::disallowed_methods, reason = ...)]` stating the lifecycle; never a file/crate-wide
allow"*, and `clippy.toml`'s own text says the same. The landed gate does exactly that at gate
level. Evidence and remedy are fully worked out above: **P-2** proves the hole (exit 0 with a
detached `tokio::spawn` + an unbounded `spawn_blocking` in the crate that owns the process-wide
runtime and does GIL-releasing `block_on`); **P-4** disproves the panel's suggested per-call-site
form; **P-5** proves the working one. The single orchestrator change is:

1. `crates/repark-python/src/lib.rs` — wrap exactly the five `pyo3::create_exception!` invocations
   in `mod exceptions { #![expect(clippy::disallowed_methods, reason = "…")] … }` + `pub use`
   (declared EC-1-adjacent deviation from verbatim; cite design §7.2).
2. `Makefile` — drop `--exclude repark-python` from the workspace invocation, or restore
   `-D clippy::disallowed_methods` on the `-p repark-python --lib` one.
3. `.github/workflows/ci.yml` — mirror, per the dual-wiring rule.
4. `AGENTS.md` — either amend the hard rule or (preferred, and what P-5 enables) leave it intact
   because the escape is no longer crate-wide.

Re-run P-2 afterwards: it must flip from exit 0 to exit 101. Until then `clippy.toml` carries the
cost in writing, which is the most this branch can do.

**F-8 — `AGENTS.md` / `PROJECT.md` still describe `repark-python` as future and `repark-ml` as
"later".** The `PROJECT.md` half is FIXED above (F-9 disposition). Remaining, orchestrator-only:
`AGENTS.md:102` (the "future `crates/repark-python`" invariant) and the crate table row
`| ML: … | crates/repark-ml | later |` (`AGENTS.md` ~36-44), which design §4 Q3 requires to read
"phase 3" now that PR-2 landed the crate.

**F-9-ORCH — `AGENTS.md:102` and `CLAUDE.md:122` call `repark-python` "the future crate" that
"will set" a local unsafe allow.** Both are false as of this PR
(`crates/repark-python/Cargo.toml:19-20` sets `unsafe_code = "allow"` today), and `CLAUDE.md:122`
sits inside the `<non_negotiable_invariants>` load-bearing region. Both files are orchestrator-only
by the brief; the builder correctly did not touch them (it did fix the analogous line it owned —
`git diff main...HEAD -- Cargo.toml` shows "will NOT inherit these" → "does NOT inherit these").

## Notes for the verify panel

1. **The one judgement call worth arguing** is the door pin's *statement* half. `TRUNCATE TABLE`
   was chosen because it is the only Spark-router-owned outcome reachable with **no catalog, no
   table and no network**, and its message is unmistakably the router's. The alternative
   (registering a memory catalog and running `SHOW NAMESPACES`) would prove the same thing with far
   more machinery in a fidelity PR. If the panel prefers a positive-result statement pin, say so —
   the cost is a temp dir and a memory catalog, not a design change.
2. **`.err().expect()` → let-else.** Three refuse-arm tests originally used `expect_err`, which
   needs `Debug` on the Ok type; `PyDataFrame` is not `Debug` at the pin and **must not be made
   `Debug` for a test's convenience**. `.err().expect()` then tripped `clippy::err_expect`. The
   landed form is `let Err(e) = … else { panic!(…) }` — `#[cfg(test)]`-only, and `--lib` (both
   panic-ban invocations) does not compile it.
3. **`tokio` is now a normal dep of `repark-core`.** That is a real, if small, widening of a tier-2
   crate's manifest, taken deliberately so `EngineRuntime` can *name* the handle. Worth a look: no
   new package resolves, no `Runtime` is constructed, and no core entry point blocks.
4. **The `repark-common` dev-dep on the binding** is the one edge not in design §2.2's list. It is
   test-only, exists solely so the EC-1 identity guard can name both paths, and is invisible to the
   crate-DAG guard (normal edges only) by that guard's stated design.
5. **`map.md` rewrites are declared, not silent** — same posture and rationale as PR-2's F-1
   disposition. Every rewritten line is one whose v1 text names a path that does not exist here.
6. **Deliberately NOT done**, each because the design says so: the duplicated
   `ARROW_STREAM_CAPSULE_NAME` constant stays duplicated (§8 "do not clean up on the way past");
   the `testing_*` backdoors and the `panic_probe` ship as-is (§8, accepted risk R4); the Spark-door
   time-travel leak is untouched (§4 Q8) — a ported test pins its presence.
7. Building in the v1 pin worktree created only its gitignored `target/`; no tracked file there was
   modified, and nothing was fetched or pushed.
