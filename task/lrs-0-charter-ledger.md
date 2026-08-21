# Charter ledger — LRS-0 · the low-risk sweep

**Date:** 2026-08-20 · **Branch:** `fix/low-risk-sweep` · **Base:** `feat/spark-function-parity`
@ `8a28057` · **Design:** [../docs/design/low-risk-sweep.md](../docs/design/low-risk-sweep.md) ·
**Slate:** [../briefs/low-risk-sweep.md](../briefs/low-risk-sweep.md)

**Rebased 2026-08-21:** `feat/spark-function-parity` squash-merged as [#190](https://github.com/TRO-Wolf/repark/pull/190) / `65bacdf`, whose tree is byte-identical to `8a28057`, so this branch was replayed onto `main` with zero conflicts and a byte-identical result tree. The base commit named above is the one the work was actually done on; it is unreachable from `main` post-squash, which is this repo's normal squash-merge outcome.

Opens the campaign. Every clause below is **PROVEN** on this tree before any unit runs; a clause that
could not be proven would hold the gate rather than ship as an assumption.

## Propositions

| # | Clause | State | Evidence |
|---|---|---|---|
| C-001 | **No unit in this campaign changes a computed answer.** Amended twice on 2026-08-20 and back to its original form: the oracle first made LRS-6 look decidable, so an exception was written in; implementing it then showed the fix needs the collector to run in UTF-16 space (a mid-surrogate offset is not a byte boundary, so there is no `regex::Match` to build), which is a restructure of a hot path rather than an edge-case patch. LRS-6 shipped as measurement and registry rows instead, and the exception is withdrawn. | PROVEN | By enumeration of the change classes: LRS-1 replaces an engine internal error with a facade refusal on paths that fail today (all three measured failing — `unresolved LambdaVariable x_0`, `SanityCheckPlan`, `ParserError`); LRS-2 changes one rejected argument shape and one error type; LRS-3 registers a divergence and adds an alias for a name the SQL door does not currently resolve at all; LRS-4 widens a test's domain; LRS-5 moves files. No arm computes a value that a working query reaches today. |
| C-002 | **Every defect this campaign fixes reproduces on this tree.** | PROVEN | Measured 2026-08-20 against the wheel built from `8a28057`: value-argument nesting → `AnalysisException: unresolved LambdaVariable x_0`; higher-order window ordering key → `AnalysisException: SanityCheckPlan … BoundedWindowAggExec`; `cube` and `rollup` over a higher-order column → `ParseException: ParserError`; `F.xxhash64()` → `ValueError: call_scalar(xxhash64) expects at least 1 args, got 0`; keyword-only lambda parameter → raw `TypeError: <lambda>() takes 0 positional arguments`; non-callable → raw `TypeError: 'not callable' is not a callable object`; SQL door `approx_count_distinct` → `Invalid function 'approx_count_distinct'`. Six `#[path]` sites counted in-tree. |
| C-003 | **The roster covers the whole round-2 forward table.** | PROVEN | All nine forwarded rows are placed: F-R3-3/FNP-R3-4, FNP-R3-3 and F-R3-6 → LRS-1; F-R3-10 and FNP-R3-6 → LRS-2; F-R3-9 → LRS-3; FNP-R3-7 → LRS-4; F-R3-4 and the group-by naming row → the excluded table with reasons. No row is unplaced. |
| C-004 | **Nothing is excluded for effort — only for blast radius.** | PROVEN | The excluded table (design §5) carries a mechanism for each row: analyzer idempotence and `Aggregate`/`Projection` field naming; output-schema change on every unaliased expression group key; a matching-semantics change needing a Java `Matcher` oracle; a change to what a working `orderBy` returns. LRS-4 is the largest unit in the campaign by effort and is **included**, which is the check that the tier is about radius. |
| C-005 | **The base is green, so anything red here was caused here.** | PROVEN | At `8a28057`: `cargo test --workspace` 45 binaries / 1,990 passed / 0 failed; `make ci` exit 0; `make preflight` exit 0; facade 3,548 passed / 70 skipped / 0 failed. Each captured alone with its own `$?`. |
| C-006 | **No unit touches a forbidden surface.** | PROVEN | By construction of the roster: no AWS credential or environment, no `Cargo.toml [patch]`, no `.github/` change, no lockfile change, no secret in any output. LRS-5 moves files inside two crates and changes no dependency edge. |
| C-007 | **LRS-5 cannot move a crate-root ceiling.** | PROVEN | Rust 2018 permits `foo.rs` beside `foo/`, so every move is file-to-subdirectory with the `#[path]` attribute deleted — no file is renamed to `mod.rs` and no code moves between files. `check_lib_rs.py` and `check_rust_file_size.py` are the mechanical proof and run on every commit. |
| C-008 | **LRS-4 cannot quietly widen the sanctioned-out table.** | PROVEN | The table's length is now an assertion inside the test (`EXPECTED_DIVERGENCES.len() == 20`, added in the round-2 remediation), so adding a row fails the suite until the number is changed in the same commit — which is where the reason has to be written. |
| C-010 | **The campaign has an independent oracle, and used it before writing any unit.** | PROVEN | A live PySpark 4.1.2 on a JVM runs on this machine (design §7). It refuted **three** of the Critic round's suggested fixes — `xxhash64()` raises rather than accepting; `lambda x, /:` works rather than being rejected; a non-callable already matches Spark — so following the review verbatim would have shipped new divergences. Every answer is transcribed into the unit ledger that used it. |
| C-009 | **Every claim this campaign makes will be checkable by a reader who does not trust it.** | PROVEN | Each unit ships a ledger with its measurement and a regression pin that is red before its fix. The campaign-level claim (C-001) is checked by the base's own suites, which cover the computed answers: 1,990 Rust assertions and 3,548 facade assertions must all still pass. |

**OPEN clauses: none.**

## APPROVAL_GATE

**PASSED** on the owner's standing instruction, 2026-08-20:

> "when all of the work is complete for the current branch you are working on, go ahead and plan to
> checkout a new branch off that branch, the new checkout branch will be your overnight work. Plan
> to tackle all of the low hanging fruit, anything that is a low risk tier item and plan […] that
> to be on the new checkout branch. Follow the SEPMO architecture as per our usual and continue to
> run the disk size checks as usual."

Read as: the low tier ships, the campaign runs under SEPMO with the S1 severity floor, and disk
checks run at phase boundaries. The tiering itself (design §2) is the part that needed a decision
and is recorded above as C-004 so it can be disputed on evidence rather than taste.

Delivery is **manual PR** per standing process: the orchestrator prepares and reviews, the owner
merges.

## Disk baseline

`df -h .` at open: **446G free of 1.8T (75% used)**; `target/` at **63G**. Re-checked at every unit
boundary and before every broad validation. Floor for stopping and reclaiming: ~100G free.
