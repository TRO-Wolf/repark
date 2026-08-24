---
name: rust-code-quality
version: "1.0"
description: >-
  Run a focused Rust quality review over changed crates/ code when one is
  requested, when reviewing a Rust PR or commit, or when another review
  workflow delegates the Rust-specific checks. Do not auto-load it for every
  implementation edit, and do not use it to re-check what the armed gates
  already hold (`make rust-clippy`, `make rust-panic-ban`, the file-size
  ratchet) — it exists for the findings no linter can reach: escape-hatch
  review, Spark-visible behavior in the diff, ANSI dual-door coverage, float
  semantics, hot-path allocation, and the error contract.
---

# Rust Code Quality Gate

A review procedure for a Rust diff in this workspace. It records a proven
review *sequence*; every rule it leans on is a pointer into the spine
([AGENTS.md](../../../AGENTS.md), [docs/testing.md](../../../docs/testing.md))
or the engineering method ([../engineering-method/SKILL.md](../engineering-method/SKILL.md)
"Rust") — on any conflict, those win. The Python counterpart is
[../code-quality/SKILL.md](../code-quality/SKILL.md); its "how a rule is held"
tags (linter / purpose-built gate / review) and its §13 arming ratchet apply
here unchanged.

## What the gates already hold — never re-review it

Machine-held, so a reviewer's finding on these is a duplicate, not a catch:

- **Panics**: `make rust-panic-ban` denies `unwrap`/`expect`/`panic!`/`todo!`/
  `unimplemented!`/`unreachable!` in production code, plus the
  `clippy.toml` `disallowed-methods` list (unwrap/expect and
  `tokio::spawn`/`spawn_blocking`). See AGENTS.md "no panics in prod".
- **Everything warn-level**: `make rust-clippy` runs `clippy::all` +
  `clippy::pedantic` with `-D warnings` — including
  `cast_possible_truncation`/`cast_sign_loss`.
- **`unsafe`**: `unsafe_code = "forbid"` workspace-wide except
  `crates/repark-python` (PyO3 macros). See AGENTS.md.
- **File size**: `scripts/check_rust_file_size.py` — ceilings ratchet down
  only.
- **Crate edges and licenses**: `scripts/check_crate_dag.py`, `deny.toml`.

What survives for review is exactly what those gates let through: the escape
hatches, and the semantics no linter understands.

## Quick start

1. Identify the changed `.rs` files (`git diff --name-only` on the range under
   review).
2. Run the candidate scans below on those files.
3. Walk the manual checklist against the diff.
4. Resolve or rebut every finding with evidence; P0/P1 findings cannot be
   deferred.

## Candidate scans

These find *candidates*, not findings. Inspect the syntax, the `#[cfg(test)]`
scope, and the changed hunk before reporting — text filters do not reliably
distinguish production code from tests, and test code legitimately unwraps
(`clippy.toml` allows it there).

```bash
# 1. New escape hatches — every one is a review item
rg -n '#\[(expect|allow)\(' <changed-files>

# 2. Value-path `as` casts (judge: value semantics, or index/length arithmetic?)
rg -n ' as (i8|i16|i32|i64|i128|u8|u16|u32|u64|usize|f32|f64)\b' <changed-files>

# 3. Relaxed atomics — verify each ordering is argued, not defaulted
rg -n 'Ordering::Relaxed' <changed-files>

# 4. Stringly-typed errors — no new ones
rg -n 'Result<[^,>]+,\s*String>' <changed-files>

# 5. Output macros — held by review until armed (see "Arming candidates")
rg -n 'println!|eprintln!|dbg!' <changed-files>
```

## Manual review checklist

### Parity and refuse-loud (the P0 axis)

- [ ] Any Spark-visible behavior change in the diff — an output value, a type,
      a default, an error class, error message text — carries oracle
      measurement and a pin or a registered divergence in the same PR
      ([docs/testing.md](../../../docs/testing.md); divergence-registry rows
      are recorded orchestrator-side).
- [ ] No silent fallback or silent coercion where the contract refuses loud. A
      wrong answer outranks a crash here; an error outranks a silent guess.
- [ ] A changed error message is a behavior change: the exception-class fold
      (`crates/repark-core/src/error_map.rs`, and the exhaustive
      `exception_class()` match in `crates/repark-common`) stays exhaustive
      and PySpark-shaped.
- [ ] No golden edited by hand. A golden that moves is a finding to triage
      (docs/testing.md: golden drift vs oracle drift), never a file to update
      in passing.

### ANSI dual-door

- [ ] Any kernel or rewrite with ANSI dispatch exercises **both** doors in its
      tests — the raise branch under ANSI and the NULL/permissive branch
      without ([crates/repark-functions/src/decimal_spark.rs](../../../crates/repark-functions/src/decimal_spark.rs)
      tests are the reference shape).

### Float semantics

- [ ] A change touching sort, group-by, join keys, or comparisons over floats
      states which layer supplies Spark's float contract (NaN largest and
      self-equal, `-0.0 == 0.0` for grouping) and shows a measurement — the
      workspace has no float-ordering module of its own, so DataFusion/Arrow
      defaults are being *relied on*, not verified, unless the test proves it.
      Write `-0.0` into test values deliberately; it never appears by accident.

### Escape hatches and casts

- [ ] Every new `#[expect(...)]` is per-call-site with a `reason` stating the
      invariant (AGENTS.md: never a file- or crate-wide allow).
- [ ] Any file-level allow of cast lints carries a banner stating the parity
      justification (`crates/repark-functions/src/random.rs` is the precedent:
      bit-exact Java/Scala casts). A new value-path `as` cast in a kernel
      without one is a finding; `try_into()` with a typed error is the default.

### Error handling

- [ ] New error paths classify through the `error_map.rs` helpers
      (`engine_err`/`iceberg_err`) at the boundary — not ad-hoc strings, not a
      new `From` impl that bypasses classification.
- [ ] No new `Result<_, String>`; internal helpers return real error types
      (Opus.md "Error Handling"). Existing ones are debt, not precedent.
- [ ] Error text is actionable and stable — it is part of the parity surface.

### Concurrency

- [ ] No `std::sync` lock guard bound across an `.await` (guards are acquired
      and dropped in one expression chain, the house pattern in
      `repark-core/src/session.rs`).
- [ ] Each `Ordering::Relaxed` is argued at the site; when in doubt, the
      stronger ordering with a comment beats the weaker one without.
- [ ] The `tokio::spawn` cancel-safety ban is not smuggled around via
      `JoinSet`, `FuturesUnordered`-with-detach, or a helper crate.

### Recursion

- [ ] Any recursion over user-influenced input (SQL text, nested types, parsed
      JSON) has a depth bound or an explicit stack — Opus.md "Function Length
      & Recursion" treats unbounded recursion over external data as a
      denial-of-service vector. House precedents: `CONST_FOLD_MAX_DEPTH`,
      `MAX_ERROR_PEEL_DEPTH`, `dynamic_flatten`'s loud `max_depth` refusal.

### Hot path

- [ ] No per-row allocation inside a kernel loop unless it is the necessary,
      pre-sized output — the reference idiom is the two-pass sizing in
      `crates/repark-functions/src/string.rs` (`SparkSubstr`, PERF-03).
      Report allocation findings with the concrete per-row cost, not on
      principle.

### Reuse and necessity

- [ ] Before a hand-rolled kernel or helper: checked `datafusion-spark`,
      DataFusion itself, and `repark-common`/`repark-core` first. A deliberate
      small copy states so in a comment (the `DecimalAverager` copy in
      `aggregate.rs` is the shape).
- [ ] Every branch a change adds has a nameable input where it changes the
      output — a dead branch is a defect, not a belt-and-brace
      (docs/testing.md).
- [ ] Comments carry the non-obvious invariant, not narration (AGENTS.md
      "Change discipline").

### Ratchets, tests, maps

- [ ] No file-size ceiling raised without a stated reason in the diff
      (`scripts/check_rust_file_size.py` ratchets down only).
- [ ] Tests follow [docs/testing.md](../../../docs/testing.md) — same-commit,
      revert-red, entry-point matrix; that contract is the authority, this
      checklist does not restate it. One addition it does not carry: no
      near-duplicate test pinning the same code path and the same poison-value
      class as an existing test — but boundary companions (`n == max` vs
      `max + 1`, empty vs absent) are never near-duplicates.
- [ ] Touched directories' `map.md` updated in the same change (AGENTS.md).

## Severity

For a query engine the cardinal sin is a silently wrong answer, so the scale
inverts the crash-first ordering a service repo would use:

- **P0 (block)**: silently wrong query results — an unregistered divergence
  from Spark, a silent fallback where the contract refuses loud; data loss;
  undefined behavior.
- **P1 (must fix)**: a panic reachable from SQL input; a correctness or parity
  bug with a pin; a golden moved without triage; a hand-edited expectation.
- **P2 (should fix)**: avoidable duplication with a concrete simpler
  replacement; unjustified escape hatch; missing dual-door coverage on a
  touched kernel.
- **P3 (nice to fix)**: local clarity issue with no behavioral risk.

## Output template

```
## Rust Code Quality Report

### Candidate scans
- escape-hatch candidates inspected: N
- value-path cast candidates inspected: N
- relaxed-ordering candidates inspected: N
- stringly-error candidates inspected: N
- output-macro candidates inspected: N

### Findings
- [P1] `path:line` — one-sentence defect
  - Evidence: what was measured or read that proves it
  - Fix: ...

### Verdict
PASS / BLOCKED (list the P0/P1 findings)
```

## Arming candidates

`println!`/`eprintln!`/`dbg!` are mechanically decidable and production code is
currently clean of them — the right end state is three more `-D` flags
(`clippy::print_stdout`, `clippy::print_stderr`, `clippy::dbg_macro`) on
`make rust-panic-ban`, after which scan 5 and its checklist weight disappear.
Arming is its own change with provocation proofs, per
[../code-quality/SKILL.md](../code-quality/SKILL.md) §13 and docs/testing.md
"gate provocation".
