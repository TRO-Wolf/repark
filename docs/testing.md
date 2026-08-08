# Testing contract (repark)

This is the mandatory testing-discipline contract for this repo. Read it before any code change.
It is ported from the private v1 repository and applies from commit one of the port: code arriving
in phases 1–3 lands under these rules, and the rules themselves are phase-0 deliverables so the
gate exists before the first crate does.

## The two hard rules

1. **Tests land in the same commit/PR as the code they test.** No "I'll add tests later" — not for
   "tiny", "prototype", or "obvious" code. A PR that adds or changes behavior without adding or
   changing the corresponding tests is reverted, not patched. The only exempt changes are ones with
   no testable surface: pure docs, comment-only edits, renames, lockfile-only bumps, and config/stub
   scaffolding with no behavior (e.g. phase 0 of the port).
2. **No coverage-percentage target; enforce test-per-change.** Every behavior gets a test; every
   spec invariant (the settled decisions in the plan) gets ≥1 test exercising the contract.

## The entry-point matrix — the central testing structure

RePark exposes every behavior through up to three user entry points:

| Row | Entry point | Spelling |
|---|---|---|
| 1 | **Native DataFrame API** | `repark` lazy DataFrame ops |
| 2 | **ANSI SQL door** | native `repark.sql()` — ANSI/Trino-style dialect |
| 3 | **Spark facade** | facade `.sql()` (Spark dialect) + PySpark-shaped DataFrame API |

> **Row-2 spelling note (phase 3, 2026-08-08).** `repark.sql()` names the *target* spelling. Until
> the post-milestone-one re-home, the shipped tree occupies `repark.sql` with the ported
> pyspark-alias *package*, and the ANSI door is reachable from Rust only
> (docs/design/python-facade.md Q1/Q2). `docs/release.md` "Hard blockers" fails the first tag
> while `repark.sql` is still a module, so the target spelling cannot silently become an
> API-forever promise.

The matrix rule: **every behavior and every divergence class is a row per entry point it is
reachable from.** New SQL surface lands with both SQL spellings and at least one test row per door
(the two-doors rule — see `docs/adr/0002-two-sql-doors.md`). A claim tested through one entry
point says nothing about the others; the matrix is the standing detector for that gap. This
structure exists from day one — it is not discovered after divergence bugs, it is where divergence
pins live.

## What "calibration-sensitive" means here

For an Iceberg/Spark engine the precision surface varies by domain. Use the right tool per surface:

| Surface | Test idiom |
|---|---|
| `DECIMAL(p,s)` arithmetic | **decimal128 bit-exact** fixture regression |
| Null ordering, sort, window-frame semantics | **row-order fixtures** (ordered assertions) |
| Float aggregation across partitions (order-dependent accumulation) | **`f64::to_bits`** bit-exact |
| Schema evolution (add/rename/drop, type promote) | **schema-equality** (field IDs + types) |
| Facade behavior vs Spark | **Spark-parity differential** (see below) |
| Spark-vs-DataFusion **silent-divergence claims** | **per-class pins on the Arrow path** (below) |

Do not reflexively reach for `f64::to_bits`; pick the idiom that matches the failure mode.

## The Spark-parity differential harness (mandatory; ports in phase 3)

"The facade maps one-to-one with PySpark" is only credible if checked. The parity harness
(`python/repark-parity`, ported in phase 3) compares repark output against a reference and fails
on any divergence (null-aware, order-insensitive by default — Spark result sets are unordered
unless `ORDER BY` pins them).

- **check mode** (routine CI, no JVM): compare repark output against recorded Spark **golden**
  fixtures stored in the corpus. This is what runs on every PR.
- **record mode** (occasional, needs `pyspark` + a JVM, gated behind the `record` extra): run the
  same operation on real Spark and refresh the goldens. Keeps the no-JVM promise for everyone else.

**Every new facade DataFrame op or Spark function lands with a parity case in the same commit.**
The comparison core has its own unit tests.

### Divergence-class claims

A claim that repark matches Spark where DataFusion silently diverges ("Spark parity",
"fixed \<finding\>", "handled") quantifies over **divergence classes × user entry points** — and
the claim is only as true as its weakest untested cell. Three rules, learned the hard way in v1
(an expression bit-reinterpreted at the Arrow boundary while the one pinned case was green):

1. **Pin every class the claim names, per entry point.** One representative case is not the
   claim; a disjunctive acceptance criterion ("division *or* substr") is satisfied by its
   cheapest disjunct and is banned in charters. The standing detector is the **divergence corpus
   × entry-point matrix** (above) — new expression entry points must join the matrix, new
   divergence classes get a corpus row.
2. **Pin the path users migrate on — `collect`/`to_arrow`, value AND Arrow type.** Display paths
   (`show`) prove nothing about export paths.
3. **A pin is valid only if reverting the fix turns it red**, and every branch a fix adds needs a
   nameable input where it changes the output — a dead branch is a defect, not a belt-and-brace.

### The live oracle tier (drift detector)

Record-then-pin has a blind spot: goldens are recorded from live Spark **once**, at authoring time,
and pinned; routine CI is JVM-free and never re-checks them. Two failure classes then hide until a
human re-runs the oracle by hand — **golden drift** (a stale or hand-edited pin) and **oracle
drift** (a Spark bump that changes semantics under a still-green pin). Record-then-pin remains the
law for *authoring* a golden (routine CI must stay no-JVM for everyone); the live tier is the
*standing detector* that closes the blind spot.

- **What it is.** A shared scenario registry runs one engine-agnostic recipe on both engines; the
  live tier re-derives every mandated golden from live Spark and asserts
  **repark == pinned golden == live Spark** (value AND Arrow-path type/nullability). Load-bearing
  **disclosures** (recorded divergences) are re-asserted the other way: the live tier proves the
  recorded Spark behaviour *still differs* from repark, so a silent convergence goes RED and
  forces the disclosure to be revisited (never laundered into "parity").
- **Gate + cadence.** Armed only by an explicit env var set to exactly `"1"`; unset → every live
  test SKIPs with a visible reason, never a silent pass. It runs in tier-2 CI (nightly +
  on-dispatch, merged code only — never against unmerged code). `make ci` / `make verify` stay
  JVM-free and unchanged. The harness and its workflow port in phase 3.
- **Triage a live-tier RED.** A scenario that reds on the *golden* leg while repark and live Spark
  still agree ⇒ **golden drift** (fix the pin). A scenario where repark == old pin but live Spark
  now diverges ⇒ **oracle drift** (a Spark bump moved the semantics; re-record and reconcile). A
  *disclosure* that reds ⇒ the two engines **converged** — update the disclosure, do not silently
  re-label it parity.

## Boundary changes need a real-artifact test (applies from phase 3)

In-process tests cannot catch boundary bugs: when producer and consumer compile together
(`maturin develop`, same build), both sides share one definition of the boundary, so layout,
symbol, and lifecycle mismatches are structurally invisible.

Any change to boundary code — the PyO3 seams in `repark-python`, Arrow C-stream export, IPC
ingest, abi3/wheel surface — needs at least one test that crosses the REAL artifact: the built
wheel import smoke at minimum, a behavior test through the installed wheel when the change is
behavioral. `maturin develop` facade tests alone do not count for this class. When unsure whether
a change is boundary-class, it is — the cost is one wheel-path test.

## Unit testing the engine

Rust unit tests use iceberg-rust's in-process `MemoryCatalog` + `FileIO("memory")` — no S3, fully
deterministic. Real-AWS integration tests (Glue / S3 Tables) are `#[cfg(feature = "integration")]`
and gated by an env var; they never run in the default unit-test CI (this is what makes tier-1 CI
safe on untrusted code).

## Gate provocation proofs

A new mechanical gate (lint entry, guard script, CI step, pre-commit hook) is not "done" because
it runs green — green on a clean tree proves nothing about detection. Every new gate ships with
**provocation proofs in the unit ledger**: the violating change is temporarily introduced, the
failing run is captured verbatim (command + output + exit code), the change is reverted, and the
clean run is captured too. Both directions where the rule has two sides (a must-FAIL and a
must-PASS case). Provocations are **never committed** — the final-gate audit greps the tree for
provocation identifiers. Where a gate matches compiled paths rather than source text (e.g. clippy
`disallowed-methods` matching DefIds), provoke with the form the code actually uses, not the
canonical spelling. A gate with no recorded provocation is treated as unproven — same standing as
an untested behavior.

## Relocation discipline

The port depends on this section: phases 1–3 move tests between repositories, and the acceptance
gate (`docs/port/PLAN.md`) is defined in terms of it. Moving tests is allowed only under one of
two declared shapes — never mixed in one diff:

1. **Move-only relocation.** Test code moves; every test NAME stays byte-identical. The gate is
   mechanical: `cargo test --workspace -- --list` (Rust) / `pytest --collect-only -q` (Python)
   captured at base and tip, sorted — **the diff must be empty**. Inline `#[cfg(test)] mod tests {…}`
   → file-backed `#[cfg(test)] mod tests;` qualifies (module path unchanged); moving tests into a
   different module, or into the `tests/` integration directory, does not.
2. **Declared-rename unit.** Anything that changes test paths ships **alone**, with an explicit
   old-name → new-name map in its ledger; the identity gate then reads "empty after applying the
   declared map". A rename smuggled into a move-only diff is a slate-failing violation — a pin's
   name is part of the pin.

## Forbidden patterns

Banned without an explicit linked tracking issue + deadline: `#[ignore]`, commented-out tests,
`// TODO: add test`, `assert!(result.is_ok())` as the entire test body, `cargo test -- --skip` in CI,
skipping commit hooks.

## Per-phase test expectations (the port)

- **Phase 0 (bootstrap):** no testable code surface; the gates themselves are the deliverable, and
  each new mechanical gate carries provocation proofs per the section above.
- **Phase 1 (engine core):** the Rust unit-test tier ports with `repark-core` / `repark-iceberg`
  under relocation discipline (test names byte-identical); MemoryCatalog-based, no AWS.
- **Phase 2 (the two SQL doors):** every ported/added SQL behavior gets its entry-point-matrix
  rows — both doors, one test row per door; divergence-class pins follow the rules above.
- **Phase 3 (Python facade + parity):** the parity harness, the census machinery, and the live
  oracle tier port; the acceptance gate is the census multiset, byte-flat across repos
  (`docs/port/PLAN.md`); boundary changes take the real-artifact rule from here on.
