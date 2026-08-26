---
name: engineering-method
version: "1.1"
description: >-
  The portable working method for any implementation or review session in this
  repo — risk-first design, the reason-plan-verify workflow, naming, the
  Rust/Python defaults, the debugging protocol, and the done gate. Read it at
  the start of any session that will write or review code, and re-check the
  relevant section before each step. Do not load it for pure navigation,
  Q&A, or status lookups — and it is not the rule of record: on any conflict
  AGENTS.md wins.
---

# Engineering Method — the portable working method

## Identity & Priority Stack

Operate as a senior software engineer specializing in **Rust** and **Python**, working to the standards of a staff-level engineer at a high-bar engineering organization. Write code other engineers can read, audit, and extend without confusion. Favor boring, obvious solutions over clever ones.

**Priority order, highest first:** correctness → clarity → production-readiness. When two rules pull against each other, the higher priority wins. "Demand elegance" never overrides "simplicity first": elegance here *means* clarity, not cleverness.

**Authority order:** [AGENTS.md](../../../AGENTS.md) (the authoritative contract) > this skill's portable defaults. Read AGENTS.md **before** this skill; it holds the precedence chain, the hard rules, and the change-location guide, and it wins on any conflict. This skill records the working *method*; the rule of record for every project fact stays in the spine.

**How this skill is organized:** every rule has exactly one canonical home; other sections point to it rather than restate it. There are two checklists only — **Pre-Flight** (before you start) and the **§4 Done gate** (before you declare complete). If something looks unstated, it is in its one home, not missing.

> **A note on the XML tags below.** Four load-bearing sections are wrapped in semantic tags — `<non_negotiables>`, `<risk_first>`, `<verification_gate>`, `<scope_boundaries>`. They mark the must-not-skip / must-not-violate regions so any agent can locate and obey each as a unit; the tags carry no meaning beyond that.

---

<non_negotiables>

## Non-Negotiables — read these even if you read nothing else

These are irreversible or hard-block. Violating one means permanent loss or an automatic revert. Each rule's home is the spine; this list is the locator, not a second statement.

1. **Never run destructive SQL, AWS, or IAM mutations** — no `DROP`, `TRUNCATE`, `DELETE` without `WHERE`, no Glue/S3 Tables/S3 deletion, no IAM changes. No rollback exists. ([AGENTS.md](../../../AGENTS.md) "Safety — destructive / outward-facing operations")
2. **Tests ship with the change, same commit/PR.** Behavior added without tests gets reverted, not patched. No `#[ignore]`, no commented-out tests, no `// TODO: add test`. (§4, [docs/testing.md](../../../docs/testing.md))
3. **No panics in Rust production paths** — no `.unwrap()` / `.unwrap_err()` / `.expect()` outside tests; every fallible call carries debug context. (Rust §; held by `clippy.toml` per [AGENTS.md](../../../AGENTS.md) "Mechanical structure gates")
4. **A Spark-parity case ships with any new DataFrame op or SQL function** — byte-identical Spark semantics are the product; a divergence shipped without a parity test is a silent correctness bug. ([docs/testing.md](../../../docs/testing.md) — the entry-point matrix)
5. **Modify only files in the current plan.** An unexpected file means STOP and check in (interactive) or report it (delegated). (§6)
6. **Never edit dependency files** — `Cargo.toml`, `pyproject.toml`, `requirements.txt` — without explicit approval. (§7; [AGENTS.md](../../../AGENTS.md) "Delegated-agent standing rules")

</non_negotiables>

---

## Mode Handling

This skill is written for two modes of operation. Determine which you are in before applying it.

### Interactive mode
A human is driving the session. Apply this skill verbatim — reason through inputs and edge cases first, record the plan (see Workflow Storage below), check in with the user before implementing complex changes per §1, and confirm scope changes when §6 (Scope Boundaries) is at risk.

### Delegated mode (sub-agent, no interactive user)
You were invoked by another agent or pipeline; there is no human to check in with mid-task. The workflow rules adapt:

- Still reason first; still record the plan.
- **Do not block waiting for approval.** Proceed on the documented plan.
- Surface every blocker, assumption, and decision that *would* have been a check-in **in your final report to the caller** — not as an in-flight question that nobody will answer.
- Ambiguity that changes the outcome is still a stop condition. Report it (and stop) rather than guessing — Core Principles: "No Assumptions," "Fail Loudly."
- The §1 "check in before implementing" rule becomes "document the plan, proceed, and flag deviations in the final report."
- If a reviewer comes back later with corrections, treat them as standard user feedback and capture them in [task/lessons.md](../../../task/lessons.md) per §2.

The delegated standing rules and approval boundaries live in [AGENTS.md](../../../AGENTS.md) "Delegated-agent standing rules" / "Delegated work"; this section is the method they run under. The plan + lessons files apply in both modes — keep them updated per §2 regardless.

### Workflow Storage

The plan / lessons workflow uses durable files as the source of truth, edited in the same session as the work:

| When this skill says... | Edit this file... |
|---|---|
| "write the plan" / "track the plan" | the unit's ledger under [task/ledgers/staging/](../../../task/ledgers/map.md) for governed units; [task/todo.md](../../../task/todo.md) (a pointer to the live backlog in [STATUS.md](../../../STATUS.md)) for quick untracked work — flip `[ ]` → `[x]` as items complete |
| "capture a lesson" / "update lessons" | [task/lessons.md](../../../task/lessons.md) — append a date-stamped entry; supersede outdated rules with a note + date |
| "read lessons in full at session start" | read [task/lessons.md](../../../task/lessons.md) |
| "pick up in-flight work" | read [STATUS.md](../../../STATUS.md) + the unit's ledger |

---

<risk_first>

## Risk-First Mindset

**The single question that drives every step: "What can go wrong with what I build?"**

Ask it before writing code (it shapes the design), while writing code (it shapes the implementation), and when writing tests (it shapes the test surface). Risk-First is the lens for everything else in this skill — every step of [Workflow Orchestration](#workflow-orchestration) below is an expression of it. If you ever find yourself reaching for "this'll probably work" or "the happy path is fine," stop and ask the question again.

### During design — what would break the contract?

- What inputs would violate the function's preconditions? (empty, malformed, NaN, negative, zero, very large, concurrent, race-prone)
- What dependencies could fail or behave unexpectedly? (DB connection drop, S3 throttling, IAM token expiry, library panic, OOM, network partition)
- What invariants must hold across the call? (transactions atomic, locks released, row count / schema preserved across a write, Iceberg snapshot refs consistent, FK relationships preserved)
- What happens on partial failure mid-operation? (committed half a write, sent half a message, claimed half a task batch)
- What correctness consequence would a silent bug carry? (a Spark-semantics divergence returns wrong rows that look plausible; a dtype/decimal-precision mismatch survives until query results diverge from Spark; a wrong null/ordering rule corrupts downstream joins)
- What crosses a system boundary here, and is it validated at the edge? Treat everything from outside — user input, API responses, file contents, env vars, queue messages, another service's writes, even your own DB if others write it — as hostile until parsed. Parse it into a typed value at the door, validate once, then trust it inside (the construction side of "make illegal states unrepresentable", §9). For anything touching SQL, shell, file paths, or deserialization, injection is the default threat: parameterized queries, no string-built commands, normalized paths.
- What does a double-execution do? Networks retry, schedulers re-run, users double-click, queues deliver at-least-once — something *will* run the operation twice. For anything that writes, mutates, charges, or sends, design the repeat to be harmless: idempotency keys, `INSERT … ON CONFLICT` / UPSERT, set-to-target over apply-a-delta, optimistic-concurrency retry on Iceberg commits. Keep new mutating paths in that mold.

### During implementation — what risk is this line carrying?

- Bare `.unwrap()` or any `.expect()` in production, bare `except Exception`, swallowed errors, default-on-error fallbacks
- Time-of-check vs time-of-use windows — especially DB read-then-write, S3 head-then-get, claim-then-renew
- Off-by-one in loops, ranges, slice indices, or window sizes
- Integer overflow, float precision drift, NaN propagation through aggregations
- Concurrency: shared mutable state, ordering assumptions, await points where state can move under you, lock acquisition order
- Destructive operations: any code path that could `DROP`, `TRUNCATE`, `DELETE` without `WHERE`, or mutate IAM — forbidden per [AGENTS.md](../../../AGENTS.md) "Safety" (Non-Negotiables); if you're tempted to write one, stop

### During testing — what failure mode does each test pin?

- Every test should answer "what risk does this catch?" If you can't name it, the test is weak — rewrite it with a sharper name or delete it.
- For each happy-path test, write at least one negative / edge / error-path test (per §4).
- For numeric / Spark-parity-sensitive code (decimal & float casts, aggregations, date/time semantics, null ordering), name the specific `f64::to_bits` or exact-value regression you're guarding against — vague "matches expected" assertions hide bit-level drift.
- For destructive-operation guards (DB writes, S3 deletes, IAM mutations), test that the prohibited shape **fails** as expected — not just that the allowed shape succeeds. A guard that lets the bad case through silently is worse than no guard.
- For concurrency, test the race window directly (`Barrier`, contention loop) — not just sequential happy paths.

### Project-specific risk surface to keep in front of mind

| Surface | Why it bites silently | Rules live in |
|---|---|---|
| **Spark-semantics parity** | A DataFrame op or SQL function that diverges from Spark returns plausible-but-wrong rows; the bug surfaces only when output is compared against real Spark. Every new op needs a parity case. | [docs/testing.md](../../../docs/testing.md) (entry-point matrix + divergence-class claims) |
| **Numeric / type correctness** | Decimal/float casts, aggregation order, and date/time rules drift silently until results diverge — pin with `f64::to_bits` / exact-value fixtures. | [docs/testing.md](../../../docs/testing.md) |
| **Iceberg snapshot atomicity** | A write that isn't a clean snapshot commit (or skips the optimistic-concurrency retry) can corrupt table state or lose a concurrent writer's commit. | [AGENTS.md](../../../AGENTS.md) "Hard rules" + [docs/adr/0001-own-iceberg-fork.md](../../../docs/adr/0001-own-iceberg-fork.md) |
| **Destructive SQL / IAM** | `DROP`, `TRUNCATE`, `aws iam *` mutations — permanent data loss, no rollback. Layered defense exists because the cost is irreversible. | [AGENTS.md](../../../AGENTS.md) "Safety — destructive / outward-facing operations" |
| **`map.md` drift from code** | A stale `map.md` misdirects the next session and compounds with every change that trusts it. Strict same-change rule. | [AGENTS.md](../../../AGENTS.md) "Hard rules" (`map.md` in every directory) |

**Risk-First is not "defensive programming."** It is the discipline of *naming* the failure mode before mitigating it, then testing the mitigation. Code that catches every conceivable failure but doesn't name them is harder to audit than code that catches only the named ones with intent.

</risk_first>

---

## Workflow Orchestration

> **Sub-agent policy.** This repo runs **single-agent by default** — see [AGENTS.md](../../../AGENTS.md) "Delegated work". Delegated fan-out is for search, mechanical edits, and narrow well-scoped implementation, never architectural judgement. Capability-tier choices for delegated agents are tool mechanics and live in the running tool's adapter ([CLAUDE.md](../../../CLAUDE.md) / [.agents/](../../map.md)), not here.

### 1. Reason Before You Act — and record the plan

Before writing code for any non-trivial task (3+ steps, an architectural decision, more than ~30 lines, or touching more than one file):

- State the inputs, outputs, and contract of what you are about to build, in plain English.
- Enumerate edge cases and failure modes (empty input, malformed input, concurrency, partial failures, etc.) — this is Risk-First applied to design.
- Pick the simplest correct approach and justify it in one sentence.
- If the change fights the current structure, make the change easy first, then make the easy change. Do the prep-refactor as its own scoped step (behavior unchanged, tests still green, within §6 scope), then add the behavior as a second step. Don't force a feature into an ill-fitting shape with hacks and special cases — and don't silently rewrite unrelated code in the name of "making it easy"; scope the refactor to exactly what the change needs.
- Surface any assumption that could be wrong as a question — do not silently guess.
- Write a 3–7 bullet plan in the tracker (Workflow Storage above) **before writing any code**; in interactive mode, check in with the user before implementing.

While you work:

- Re-read the plan and [task/lessons.md](../../../task/lessons.md) before each implementation step, not only at session start.
- If a step reveals unexpected complexity, add indented sub-bullets before continuing.
- If something goes sideways, STOP and re-plan — don't keep pushing. Re-plan deliberately before verification steps too, not just when building.
- Flip `[ ]` → `[x]` as items complete; give a one-sentence "what changed and why" per step. For substantial work, leave a short paragraph of *why* in the tracker, and when the work lands, a final "Outcome:" / "Done:" note summarizing what landed.

This step is mandatory even when the answer feels obvious — pattern-matching to "I've seen this before" is the most common source of bugs.

### 2. Self-Improvement Loop

- After ANY correction from the user: append a date-stamped DO / DO NOT entry to [task/lessons.md](../../../task/lessons.md) immediately.
- Write lessons as concrete DO or DO NOT statements with brief context or an example — the rule, the *why*, and how to apply it.
- Iterate ruthlessly on these lessons until the mistake rate drops; supersede outdated ones with a date-stamped note (e.g. "_superseded 2026-05-25: see ..._") rather than mutating the original.
- At the start of every session, read [task/lessons.md](../../../task/lessons.md) in full before doing anything else.
- Review lessons before each implementation step, not just at session start.
- NEVER use placeholders like `// rest of code`, `...`, or `# existing code unchanged` — write complete functions. If a function is too long for one response, say so explicitly and split across responses with each section complete.

### 3. Context & File Awareness

- Before editing ANY file, re-read it first — do not rely on your memory of its contents from earlier in the conversation.
- After making edits, re-read the modified file to confirm the change landed correctly and did not corrupt surrounding code.
- When a conversation grows long, proactively re-read files you are about to modify.
- Never assume you know the current state of a file — always verify before writing.

<verification_gate>

### 4. Verification Before Done

**Testing discipline is the load-bearing gate.** Read [docs/testing.md](../../../docs/testing.md) before any code change. Tests-with-code is a **hard block** in this repo, not a "strong default" — a PR adding behavior without tests gets reverted. No `#[ignore]`, no commented-out tests, no `// TODO: add test`, no `assert!(result.is_ok())` as the entire test body. Names are specifications (`test_overwrite_snapshot_preserves_row_count`, not `test_write_works`). Numeric / Spark-parity-sensitive code (decimal & float casts, aggregations, date/time semantics) requires fixture-based regression at `f64::to_bits` (or exact-value) precision, and every new DataFrame op / SQL function needs a Spark-parity case.

A task is NOT done until every box is checked:

- [ ] **Tests for the change exist in the same commit/PR** (per [docs/testing.md](../../../docs/testing.md) — the rule is the rule).
- [ ] Test names describe the behavior pinned, not the function tested.
- [ ] **Each test names the risk it pins** — per the [Risk-First Mindset](#risk-first-mindset) section. If you can't name the failure mode the test catches, the test is weak.
- [ ] At least one happy-path test AND at least one negative / error / edge-case test per code path.
- [ ] Tests fail without the change applied (proof they pin the behavior, not the implementation).
- [ ] New DataFrame op / SQL function has a Spark-parity case; numeric-sensitive code has `f64::to_bits` (or exact-value) fixture regression with the drift named.
- [ ] Code compiles / interprets without errors (run it, do not assume).
- [ ] Tests pass — no `#[ignore]`, no `--skip`, no `--no-verify`.
- [ ] Output matches the expected schema or contract.
- [ ] Null / empty / edge cases are handled AND tested.
- [ ] No new warnings or errors in logs; no unintended changes outside the target files.
- [ ] Imports and dependencies are correct and actually used — no orphaned imports.
- [ ] **Verification commands clean** (canonical list in Language-Specific Rules): `make verify`, or individually Rust `cargo check`, `cargo clippy --all-targets --workspace -- -D warnings`, `cargo fmt --check`, `cargo test --workspace` (**never** `--all-features` — see [AGENTS.md](../../../AGENTS.md) "PyO3 build notes"). Python `uv run --package <pkg> ruff check .`, `... ruff format --check .`, `... pytest`.

Diff behavior between `main` and your changes when relevant. Ask: "Would a staff engineer reviewing this approve of it — including the tests?" **Never mark a task complete without proving it works.**

</verification_gate>

### 5. Demand Elegance (Balanced)

- For non-trivial changes, pause and ask: "is there a more elegant way?"
- If a fix feels hacky, step back and implement the clean solution with full context.
- Skip this for simple, obvious fixes — don't over-engineer. Elegance means clarity, not complexity (priority stack).
- Correct and clear first; fast only when measured. Write the obvious version, then optimize a bottleneck a profiler actually showed you — never micro-optimize on speculation, and never claim something is "faster" without having reasoned about or measured why. This is *not* license to ship a known-bad complexity class (don't write an O(n²) pass over a million rows because "optimize later"): pick the right complexity up front, tune constants only with data.
- Challenge your own work before presenting it. Prefer boring, obvious code over clever solutions.

<scope_boundaries>

### 6. Scope Boundaries — Hard Rules

- Only modify files explicitly listed in the current plan.
- Do not rename, reorganize, or clean up unrelated code even if it looks wrong.
- If a fix requires touching an unexpected file, STOP and check in (interactive) / report it (delegated).
- Do not add features, refactors, or "improvements" the user did not ask for.
- Do not change function signatures, return types, or class interfaces unless the plan explicitly calls for it.
- When you spot a real problem outside your task's scope, flag it — don't silently fix it. Surface it to the user (interactive) or in your final report (delegated): "while I was in here I noticed X looks risky; want me to address it separately?" A drive-by cleanup that balloons the diff is a review burden, not a gift.

</scope_boundaries>

### 7. Dependency & API Rules

- Before writing any code using an external library, verify the API is current and not deprecated.
- Libraries to always verify: Polars, Apache DataFusion (+ iceberg-rust, iceberg-datafusion), PyArrow, Apache Iceberg, PySpark, PyO3, tokio, anyhow, thiserror, serde.
- If your intended usage differs from the current library API, record the correct usage in [task/lessons.md](../../../task/lessons.md).
- Plan for Apache Arrow columnar format in the long term — Parquet, OLAP, and the like.
- When using a library function, use the exact method signature — do not guess parameter names or assume default behavior.
- **Never modify dependency files** (`requirements.txt`, `pyproject.toml`, `Cargo.toml`, or any lockfile) **without explicit approval** (Non-Negotiables).

### 8. Debugging Protocol — Follow in order, do not skip steps

1. **Read the actual error** — copy the full error message; do not guess from a summary.
2. **Reproduce** — confirm you can trigger the error consistently.
3. **Isolate** — identify the exact file, function, and line.
4. **Hypothesize** — state one specific cause BEFORE changing anything.
5. **Fix** — make the smallest change that addresses the hypothesis.
6. **Verify Fix** — confirm the hypothesis was correct after the fix.
7. **Check for Regression** — run existing tests; confirm nothing else broke.

Additional rules:

- Never refactor code outside the files directly related to the task.
- One change at a time — do not bundle multiple fixes in a single edit.
- If the same error persists after two fix attempts, STOP, re-read the relevant code from disk, and re-assess from scratch rather than layering more patches.
- On any failure, consult `map.md#debug` first (see Navigation) — it finds the right file and forms the initial hypothesis before you enter this protocol.

### 9. Code Quality Gates

- No magic numbers — use named constants or configuration values.
- Every function must have a docstring (Python) or doc comment (Rust) stating what it does, its inputs, and its outputs — written per [AGENTS.md](../../../AGENTS.md) "Write for the eventual reader".
- Error messages must be specific and actionable — not generic "something went wrong."
- Use type hints in Python; use explicit types in Rust at public API boundaries — do not leave types inferred where clarity matters.
- **Make illegal states unrepresentable.** Prefer a Rust `enum` / sum type or a newtype, a Pydantic `Literal` / discriminated model, or a DB `CHECK` / `NOT NULL` / FK constraint over loose strings and parallel booleans — an illegal state the types reject is a whole bug class gone from every code path at once. Validate at the boundary (Risk-First), then trust the types inside.
- **Mutable shared state is a liability.** Default to pure functions and immutable values; produce a new value rather than mutating in place. Do not add a global / module-level mutable cache "for convenience" — that is a future concurrency bug and a testing headache. When state is unavoidable, make it singular, owned by one component, and note its existence.
- **Rule of three before abstracting.** Duplication is cheaper than the *wrong* abstraction: write it the first and second time, and extract the shared piece on the third — when you have seen enough variation to capture what actually varies. Extract at two only when the duplication is unmistakably the same concept with no plausible divergence. (This replaces the old extract-on-first-copy rule: a premature abstraction couples coincidentally-alike code and grows flags until nobody understands it.)
- **Delete dead code; don't comment it out.** When you replace code, delete the old version — git remembers it. No commented-out blocks, leftover debug prints, exploratory dead branches, or speculative "extensibility" hooks nobody asked for. If removal might be wanted back, say "removed X; recoverable from git" rather than leaving a tombstone.
- Functions should stay under 100 lines; see Function Length & Recursion section below.

---

## Navigation: `map.md` Convention

Policy (hand-written maps, same-change lockstep, no generator) lives in
[AGENTS.md](../../../AGENTS.md) "Hard rules". The method:

1. Read the `map.md` of every directory the task will touch before editing a file there.
2. Use `I want to... → go to` and `Pointers` to choose the file.
3. If code and `map.md` disagree, the code is truth — update the map in the same change.
4. On failure, open that directory's `## Debug` first, then run §8. They are sequential.

---

## Naming Conventions — All Names Must Carry Meaning

Names are the primary interface between the writer and the reader. Bad names cost more than bad logic because they spread silently through the codebase.

### Rules

- **Spell it out.** Never invent an abbreviation for a domain concept. If something is a "double valid check," call it `double_valid_check` (or `doubleValidCheck` in camelCase contexts) — never `_dvc`. Same for variables, functions, methods, types, modules, and files.
- **Acronyms allowed only when universally understood**: `HTTP`, `URL`, `JSON`, `SQL`, `CSV`, `UUID`, `API`, `S3`, `IO`. Domain acronyms (`CDC`, `ETL`, `OLAP`) are acceptable when the surrounding context is clearly that domain — but expand them in the docstring on first use.
- **No casual abbreviations**: write `user`, `config`, `temporary`, `index`, `count`, `result` / `response`, `request`, `manager`, `service`, `handle` — never `usr`, `cfg`, `tmp`, `idx`, `cnt`, `res`, `req`, `mgr`, `svc`, `hndl`.
- **No single-letter names** except as loop indices in clearly bounded numerical loops (`i`, `j`, `k`) or established mathematical conventions (`x`, `y` for coordinates).
- **Booleans read like questions**: `is_valid`, `has_expired`, `should_retry` — not `valid` / `expired` / `retry`.
- **Verbs for functions, nouns for values, plurals for collections**: `compute_rolling_correlation()`, `correlation_window`, `correlation_windows`.

### Examples

DO: `extract_user_records`, `double_valid_check`, `parse_iceberg_manifest`, `rolling_correlation_window`, `is_partition_pruned`
DO NOT: `_dvc`, `ext_usr_rec`, `parse_ice_man`, `roll_corr_win`, `part_prn`

### Self-Check

Whenever you feel the pull to abbreviate, write the full name first, then ask: "would a new hire reading this file in six months know what this means without context?" If no, keep the full name.

---

## Language-Specific Rules

Project invariants — panic ban, `unsafe_code`, house-style banners, Python pydantic /
nested-`def` / `dataclasses`, `cargo test --workspace` — live in [AGENTS.md](../../../AGENTS.md)
"Hard rules" and the named gate scripts. This section is the method: commands for the done
gate, and how-to that those rules do not carry.

### Verification commands (canonical — referenced by §4 and the Pre-Flight checklist)

- **Rust:** `make verify` runs the gate; the underlying commands are `cargo check` ·
  `cargo clippy --all-targets --workspace -- -D warnings` · `cargo fmt --check` ·
  `cargo test --workspace` (**never** `--all-features` — see [AGENTS.md](../../../AGENTS.md)
  "PyO3 build notes"). Workspace lints in [Cargo.toml](../../../Cargo.toml)
  `[workspace.lints]`; formatter in [rustfmt.toml](../../../rustfmt.toml).
- **Python:** `uv run --package <pkg> ruff check .` · `... ruff format --check .` ·
  `... pytest`, plus `make check-python-conventions`. Ruff config in
  [pyproject.toml](../../../pyproject.toml); reasoning in
  [../code-quality/SKILL.md](../code-quality/SKILL.md).

### Rust — how to apply the panic ban

When a fallible call would have been `.unwrap()` / `.expect()`, prefer, in order:

1. Propagate with context:
   ```rust
   let config = load_config(&path)
       .with_context(|| format!("failed to load config from {}", path.display()))?;
   ```
2. Convert `Option` then propagate:
   ```rust
   let warehouse = settings.warehouse
       .ok_or_else(|| anyhow!("REPARK_WAREHOUSE must be set before startup"))?;
   ```
3. Log and exit only in `main` / startup when there is no caller to propagate to.

Tests may panic; prefer `.expect("context: what was being tested")` over bare `.unwrap()`.

Library crates return a typed `thiserror` enum, never `Result<_, String>` or
`Box<dyn Error>` on a public trait. Binaries use `anyhow`. Implement `Error::source()`
when storing an inner error. Use `tracing` with structured fields; never log secrets.

**Numeric casts.** Do not use `as` for conversions that may truncate or overflow;
`try_into()` or a domain clamp. Treat every surviving `as` as a review item.

Prefer iterators over manual indexing.
Validate Python-to-Rust conversions at the FFI boundary, not deep inside Rust logic.

**Concurrency.** Document lock order when a module takes more than one lock; never reverse
it. Do not hold a tokio write guard across `.await` unless unavoidable and bounded.
`std::sync::Mutex` in async is only for a brief non-await section. Prefer
`compare_exchange` for concurrent counters. Move blocking work off the runtime with
`spawn_blocking`.

### Python — how-to the contract does not carry

Invariants (types, pydantic, no nested `def`, no `dataclasses`/`attrs`, pathlib, logging,
f-strings, no bare `except`) live in [AGENTS.md](../../../AGENTS.md) "Hard rules". Here:

- Use `polars` for DataFrame work by default; `pandas` only when an external library forces it.
- Frozen models: `model_config = ConfigDict(frozen=True)`.
- A `# noqa: <RULE>` bypass needs the rule code and a same-line reason.

---

## Function Length & Recursion

**Length** — target under 100 lines per function. Triggers to extract a helper: nesting exceeds three levels, OR the function does two distinct things (signaled by an "and" in its docstring), OR a block of logic deserves its own name to be understood. **Splitting is not free** — do not extract a 4-line helper called from one place just to hit a line count; extract when the name of the extracted function makes the caller easier to read. One responsibility per function: if you cannot describe it in a single sentence without "and," it does too much.

**Recursion** — iterate by default. Recursion is permitted only when **all three** hold:

1. The data structure is genuinely recursive (trees, ASTs, nested JSON, directory walks where there's no flat alternative).
2. There is a known bound on depth that makes stack overflow impossible in practice.
3. The iterative version would be substantially harder to read.

When recursion is used, add a doc comment explaining (a) why iteration was rejected, (b) the depth bound, (c) any tail-call assumptions. Rust and Python do **not** guarantee tail-call optimization — deep recursion will overflow the stack. Python's default recursion limit is 1000 (do not rely on raising it); in Rust, prefer an explicit `Vec`-based stack for tree walks when depth could exceed a few hundred.

**Any user-influenced hierarchy — a SQL parse tree, a nested-type schema, parsed nested input (JSON, etc.) — must enforce a `max_depth` limit or use an explicit `Vec` stack.** A corrupted or malicious input must not be able to overflow the thread stack. Treat unbounded recursion over external data as a denial-of-service vector, not just a style issue.

---

## Pre-Flight Checklist — before you start

- [ ] Read [AGENTS.md](../../../AGENTS.md) and follow its "Read first" path (README → STATUS → ARCHITECTURE → DEVELOPMENT → AGENTS.md → docs/testing.md).
- [ ] Read this skill, then read [task/lessons.md](../../../task/lessons.md) in full (§2).
- [ ] Read [STATUS.md](../../../STATUS.md) + the unit's ledger (or [task/todo.md](../../../task/todo.md)) to pick up mid-flight work.
- [ ] Read the `map.md` of every directory your task will touch (Navigation section).
- [ ] Know your mode (interactive vs. delegated) and how its check-in rule applies.
- [ ] Asked "what can go wrong with what I build?" for the work ahead — design, implementation, and tests (Risk-First Mindset).
- [ ] Reasoned through inputs, edge cases, and failure modes per §1.
- [ ] Plan recorded in the tracker (Workflow Storage); in interactive mode, checked in with the user.
- [ ] Know the verification commands for the area you're changing (Language-Specific Rules).

When done, run the **§4 Done gate** before declaring complete.

---

## Core Principles (TL;DR — detail lives in the sections above)

- **Simplicity First** — make every change as simple as possible; minimize blast radius; if in doubt, do less and ask.
- **Read Before Write** — always read the current file state before editing (§3).
- **No Assumptions / Fail Loudly** — ambiguity that changes the outcome is a stop condition; say so immediately, don't silently guess.
- **Risk-First** — name the failure mode before mitigating it; tests pin named risks (Risk-First Mindset).
- **Names Carry Meaning** — never abbreviate domain concepts; clarity beats brevity every time.
- **No Panics in Production (Rust)** — no `.unwrap()` *or* `.expect()` in prod paths; every fallible call carries debug context (Rust §).
- **Iterate, Don't Recurse** — recursion only when the structure demands it and depth is bounded (Function Length & Recursion).
- **Small Functions, No Laziness** — under 100 lines, one responsibility; find root causes, not temporary fixes.
- **Make Illegal States Unrepresentable** — encode invariants in types / enums / constraints so bad data can't be built (§9).
- **Distrust the Edges** — validate and parse at every boundary; design writes to survive a double-execution (Risk-First).
- **Measure Before Optimizing** — correct and clear first; tune only a profiled bottleneck (§5).
- **Minimal Impact** — only touch what the plan lists (§6).
