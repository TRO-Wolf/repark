# Unit ledger — FNP-6a · regexp_extract_all + regexp_substr

**Unit:** FNP-6a · **Date:** 2026-08-20 · **Executor:** Claude (Opus 5) ·
**Branch:** `feat/spark-function-parity` · **Base:** `5dc71ac` (FNP-5) ·
**Charter:** [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) clauses **C-007**, **C-012** ·
**Design:** [../docs/design/spark-function-parity.md §7](../docs/design/spark-function-parity.md).
**SEPMO:** STANDARD. Floor S1.

**Writable:** `crates/repark-functions/src/{spark_regexp.rs,string.rs,expr_fn.rs}`,
`crates/repark-python/src/column/function_dispatch.rs`,
`python/repark/src/repark/spark/{functions.py,functions_expr.py}`, the facade tests, this ledger,
`task/map.md`, the touched `map.md` files.

## The campaign changes character here

FNP-1/2/3/5 moved **34 names** from refusing-or-absent to working **without writing a single new
kernel** — the engine already had them and the facade could not reach them. That seam is now
harvested. Every remaining unit is new Rust: FNP-6 is ten kernels, FNP-7 is eleven (probing the
live registry shows only `try_sum` exists), FNP-4c is eight.

These two are the campaign's first new kernels, and they are the cheapest of the ten because the
hard part was paid for by an earlier campaign.

## What was reused, and the one thing that could not be

`spark_regexp.rs` already implements Java's `Matcher.find()` stepping for `regexp_count` /
`regexp_instr`: an empty match is reported where a previous non-empty match ended, and empty
matches advance — behaviour the `regex` crate's `find_iter` suppresses (`[0-9]*` on `2026-08-19`
is 3 there, 6 in Spark). It also binds Java's ASCII `\d`/`\w`/`\s` against the crate's Unicode
ones. Both kernels reuse that walk through a new `collect_matches`, and a pin asserts
`regexp_count` and `size(regexp_extract_all(...))` agree on an empty-matching pattern, so the two
cannot drift.

**One deliberate difference, documented at the function.** The counting walk probes for matches at
mid-surrogate UTF-16 indices, which Java can reach and Rust's `&str` cannot address — such a
position is not a byte boundary, so there is no range to extract. `collect_matches` therefore steps
over supplementary-plane characters whole. The consequence is that on astral text with an
empty-matching pattern, `regexp_count` can exceed `size(regexp_extract_all(...))`: a divergence
between two of our own functions, recorded rather than hidden.

## Two conventions Spark keeps apart

| | no match | NULL input |
|---|---|---|
| `regexp_extract_all` | **empty array** | NULL |
| `regexp_substr` | **NULL** | NULL |
| `regexp_extract` (existing) | empty string | NULL |

`regexp_substr` returning NULL rather than `''` is the whole reason Spark has it beside
`regexp_extract`, whose convention cannot distinguish "matched empty" from "did not match". Pinned
so it cannot be tidied away.

## Findings

| ID | Sev | Finding | Disposition |
|---|---|---|---|
| F-FNP6A-1 | **S1** | **I wrote the `lit_indices` defect this campaign exists to eliminate.** My first wrappers declared `regexp` a literal (`lit_indices={1, 2}`). PySpark's signature is `regexp: ColumnOrName` — a bare `str` is a **column name** — which repark's own `regexp_count` already documents in its docstring. | `REMEDIATED` — signatures match `regexp_count` exactly; the tests pass `F.lit(...)` for patterns |
| F-FNP6A-2 | S2 | `collect_matches` cannot reproduce the counting walk's mid-surrogate probe | `ACCEPTED_FLAGGED` — documented at the function; registry row handed to FNP-Z |
| F-FNP6A-3 | S3 | The cross-door pin needed `[0-9]+` rather than `\d+` | `ACCEPTED_FLAGGED` — SQL-literal backslash handling is a separate open residual in STATUS "Known correctness issues"; this row is about door agreement, and says so |

## The finding worth keeping

F-FNP6A-1 is the campaign's own thesis landing on its author. GT1 found **38** wrappers with the
wrong literal/column decision; design §4.5 records that `lit_indices` stays in Python under the
owner's chosen scope, and that 27 exported names carry one today. Writing two new wrappers, I
produced a 39th — and the only reason it surfaced in minutes rather than in a user's query is that
the test called the function the way a user would, against an oracle that disagreed.

It is direct evidence for the recommendation in §4.5: this failure class is not solved by care.

## Oracles

Python's `re`, not repark's own output — the patterns chosen are ones where Python and Java agree,
so `re` is a fair judge. The Java-specific behaviour (empty-match stepping, ASCII classes) stays
pinned by `regexp_count`'s existing tests, and the new agreement pin ties the two together.

## Gates

| Gate | Result |
|---|---|
| `cargo test --workspace --no-fail-fast` | **45 binaries, 1,990 passed, 0 failed**, cargo exit 0 |
| `make ci` | exit **0**. Two reds on the way: `rust-fmt-check`, then a clippy `empty line after doc comment` — my `collect_matches` insertion had landed between `count_non_overlapping`'s doc block and its `fn`, orphaning the comment. Fixing it needed care: the first attempt deleted the block as a duplicate when it was the only copy. Restored, and it now cross-references `collect_matches` so the divergence is documented from both sides. |
| facade pytest (full) | **3,498 passed, 70 skipped, 0 failed** |
| `regexp_count` / `regexp_instr` existing tests | 9 passed — the generalization is behaviour-preserving for the counting walk |

## Campaign accounting at this unit

| | |
|---|---|
| `__all__` | 333 → **355** |
| Names moved from refusing-or-absent to working | **36** |
| New kernels written to get there | **2** — both in this unit |

The first six units wrote none. That is the finding worth carrying forward: the engine was
consistently more capable than the facade could reach, and the cheap seam is now exhausted.
