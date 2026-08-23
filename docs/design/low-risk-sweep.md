# Design — the low-risk sweep (LRS)

**Opened:** 2026-08-20 · **Branch:** `fix/low-risk-sweep` · **Base:**
`feat/spark-function-parity` @ `8a28057` · **Charter ledger:**
[../../task/lrs-0-charter-ledger.md](../../task/ledgers/archive/2026-08/2026-08-21-lrs-0-charter-ledger.md) ·
**Slate:** [../../briefs/low-risk-sweep.md](../../briefs/low-risk-sweep.md)

**Rebased 2026-08-21:** `feat/spark-function-parity` squash-merged as [#190](https://github.com/TRO-Wolf/repark/pull/190) / `65bacdf`, whose tree is byte-identical to `8a28057`, so this branch was replayed onto `main` with zero conflicts and a byte-identical result tree. The base commit named above is the one the work was actually done on; it is unreachable from `main` post-squash, which is this repo's normal squash-merge outcome.

## 1. What this is, and what makes it one campaign

The two Critic rounds over the function-parity branch left **17 findings below the S1 floor**. They
were not fixed there for a good reason — a remediation commit is not the place for work that is not
blocking — but they are not noise either. Eight of them share a shape: **the engine's internal
error reaches a user who wrote ordinary PySpark**, or **a deliberate decision is not written down
anywhere a reader would find it**.

That shape is what makes this a campaign rather than a list. None of it changes a computed answer.
Every unit either turns an internal error into a stated refusal, corrects an argument contract to
match PySpark's, registers a decision that was already made, or moves a file to where Rust expects
it. The measure of success is that **no query that works today returns a different value
tomorrow.**

## 2. Risk tiering — how the roster was chosen

Every candidate was placed in one of three tiers, and only the first ships here.

**Low — changes a failure into a better failure, or changes nothing observable.** A refusal where an
internal error leaked; an argument the facade should accept or should reject by name; a divergence
written into the registry; a file moved into the canonical module tree. The compiler or an existing
test proves the move; nothing computes differently.

**Medium — changes what a working query returns or how it is named.** Excluded, with the reason
attached (§5).

**High — engine semantics, or a shared analyzer/plan path.** Excluded (§5).

The tier is about **blast radius, not effort**. LRS-4 is the most work in this campaign and is still
low: it can only widen a test's domain, and if that domain turns up new divergences the unit's
deliverable becomes the measurement, not a fix.

## 3. The units

### LRS-1 — a refusal, not a plan dump *(3 findings, all S2)*

Three facade paths hand the user an engine internal when the honest answer is "this shape is not
supported, here is what to do instead". All three were found by round 2; all three reproduce.

| Path | What the user sees today |
|---|---|
| a higher-order call in a **value argument** of another (`exists(array(exists(...)), ...)`) | `AnalysisException: Error during planning: unresolved LambdaVariable x_0` |
| a higher-order column as a **`Window` ordering key** | `AnalysisException: SanityCheckPlan … BoundedWindowAggExec: wdw=[count(` |
| a higher-order column under **`cube` / `rollup`** | `ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery …")` |

The first is a hole in a guard that already exists: `refuse_nested_higher_order` walks lambda
**bodies** only, so nesting in a value argument slips past it and produces the exact internal error
the guard was added to abolish. The fix is to run the same walk over the value arguments — or,
better, over the assembled `HigherOrderFunction`, so both positions are covered by construction.

The second and third are a different mechanism with the same symptom: those paths lower a `Column`
to **SQL text**, which the Spark facade's dialect cannot read back. The F-CSP-4 remediation
disclosed this for joins only; the disclosure has to name every path that lowers to text, and each
should refuse in the shape `refuse_nested_higher_order` already uses — name the function, name the
limit, name the workaround (project the higher-order result into a column first, then group or
order by that column, which does work).

**All three shapes work in Spark** (oracle, §7): body nesting, value-argument nesting, a
higher-order column as a window ordering key, and `cube` / `rollup` over one all return answers.
These are therefore real gaps, and each refusal must say so — a message that implies Spark does not
support the shape either would be false.

**Not in scope:** making any of these three shapes *work*. Two of them wait on FNP-4b (the
Spark-door dialect), which is deferred and blocked on a write-path change.

### LRS-2 — argument contracts that match PySpark *(2 findings, S3)*

**Scoped by the oracle (§7), which refuted two of the three suggested fixes.**

- `F.xxhash64()` is rejected with `call_scalar(xxhash64) expects at least 1 args, got 0` — the
  *dispatcher's* message, naming an internal function the user never called. Round 2 suggested
  accepting the zero-argument form and emitting `lit(42)`. **Spark raises**:
  `AnalysisException: [WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The 'xxhash64' requires > 0 parameters but
  the actual number is 0`, through the facade and through SQL alike. So the fix is to refuse by the
  function's own name, not to accept.
- `_lambda_arity` rejects only `*args` / `**kwargs`, so a keyword-only parameter passes the gate and
  fails later as a raw Python `TypeError: <lambda>() takes 0 positional arguments but 1 was given`.
  Spark raises `PySparkValueError [UNSUPPORTED_PARAM_TYPE_FOR_HIGHER_ORDER_FUNCTION]` — "should use
  only POSITIONAL or POSITIONAL OR KEYWORD arguments". Round 2 suggested rejecting everything that
  is not `POSITIONAL_OR_KEYWORD`; **that would be wrong**, because `lambda x, /: x > 2` works in
  Spark. The predicate is: reject `KEYWORD_ONLY`, `VAR_POSITIONAL`, `VAR_KEYWORD`.
- **No change** for a non-callable second argument. Round 2 suggested guarding it with a
  `PySparkValueError`; Spark raises a plain `TypeError: 'nope' is not a callable object`, which is
  byte-for-byte what repark already raises. Fixing it would have *created* a divergence.

### LRS-3 — write down what was already decided *(1 finding S3, plus one gap this campaign found)*

- The 1,000,000-character `randstr` cap is a **deliberate safety limit**, not a Spark parity claim,
  and it appears in no registry. Confirmed a divergence by the oracle: Spark returns a 5,000,000-
  character string for `randstr(5000000, 1)` without complaint. Register it. Round 2 also showed the cap is **per row**: a legal
  length times a large batch still overflows arrow-rs's i32 string offsets. It is caught at the
  boundary rather than aborting, so this is a message quality issue, not a safety one — bound
  `length × batch_size` so the refusal is stated rather than discovered.
- The SQL door does not know the name **`approx_count_distinct`** at all, only DataFusion's
  `approx_distinct`. Spark SQL has it. This is one `with_aliases` line in `register_all`, exactly
  like the `percentile_approx` / `approx_percentile` aliases already there.

### LRS-4 — give the C-012 guard a real domain *(1 finding, S3)*

`door_parity_tests.rs` checks a **hand-maintained** `SCALAR_NAMES` list, so the kernels this branch
added sit outside the guard entirely and "makes the policy mechanical" claims more than it enforces.
Derive the domain from the dispatch table instead, so a new arm joins the checked set the day it is
written, with `EXPECTED_DIVERGENCES` as the only sanctioned way out.

**This unit measures before it fixes.** Widening the domain will find divergences that were never
checked. Each one is either closed or registered with a reason — it does **not** get quietly added
to the sanctioned-out table to make the test pass. If the count is large, the unit ships the
measurement plus the widened guard behind the rows it can justify, and hands the rest forward.

### LRS-5 — the canonical Rust module layout *(the standing rule, six sites)*

AGENTS.md now requires Rust's default module file layout and forbids `#[path = "…"]` for module
inclusion. Six sites remain, and none is one of the sanctioned exceptions:

| Site | Move to |
|---|---|
| `repark-functions/src/url.rs` → `java_uri` | `src/url/java_uri.rs` |
| `repark-functions/src/collection.rs` → `str_to_map`, `shuffle`, `map_from_entries` | `src/collection/…` |
| `repark-iceberg/src/write/predicate_dml.rs` → two `#[cfg(test)]` modules | `src/write/predicate_dml/…` |

Rust 2018 allows `foo.rs` beside a `foo/` directory, so no file needs renaming to `mod.rs` and the
crate-root ceilings are untouched. The compiler proves the move; there is nothing to measure.

## 4. Order

LRS-5 first — it is pure motion, it touches no logic, and doing it first means every later unit
edits files already at their final path. Then LRS-1 (the largest user-visible improvement), LRS-2,
LRS-3, and LRS-4 last because it is the only one whose scope is not fully known until it runs.

## 5. Excluded, with the reason

Not "later" as a way of saying no — each of these is a real item with a real reason it is not
low-risk.

| Item | Tier | Why not here |
|---|---|---|
| the unsigned-count cast moved into the shared analyzer, so the SQL door matches the facade | High | the rewrite must be idempotent across re-analysis, and must not rename an `Aggregate` node's output field that a parent `Projection` refers to by name. Engine semantics. |
| `groupBy` naming an expression key from the facade's projection name instead of the engine's spelling | Medium | changes the output schema of every unaliased expression group key, not just higher-order ones. A sensitive shared path. |
| the empty-pattern collector agreeing with `regexp_count` on astral text | **High**, measured | the oracle settled what the right answer is (5, not 4) but not how to reach it: a mid-surrogate offset is not a byte boundary, so Rust's `&str` cannot address one and there is no `regex::Match` to build there. Closing it means running the collector in UTF-16 space and mapping back — a restructure of a hot path. **LRS-6** shipped the measurement and registry rows `RE-1` / `RE-2` instead. |
| `regexp_extract_all(str, regexp)` returning group 0 where Spark returns group 1 | **Medium**, found here | a silently wrong answer on ordinary input (`['a1','b2']` vs Spark's `['a','b']`), on both doors. A one-line default change, but it changes what every two-argument caller gets back — a deliberate decision, not a sweep item. Registered as `RE-1`, pinned to today's behavior. |
| `_sort_specs` adopting PySpark's truncating `zip` and its list-of-columns form | Medium | changes what a working `orderBy` returns. The current strictness is louder than PySpark and was a deliberate call. |
| FNP-15/16 (62 names), FNP-4c (8 lambda kernels), FNP-8 (repatriation of 55) | — | campaign units with their own charters, not sweep items. Sized in the parity design §7.1. |

## 6. Done criteria

1. Every unit has a ledger with its findings, its evidence, and its disposition.
2. Every fix has a regression pin that is **red before it and green after**, measured, not assumed.
3. No test that passes on the base fails here, and **no query returns a different value** — the one
   claim this whole campaign rests on.
4. `make preflight` exit 0, captured alone, its own `$?` read immediately.
5. Anything a unit declines to fix is registered where the next reader will find it, with its
   reason — never dropped, and never left as a promise without an artifact.


## 7. The oracle

A live **PySpark 4.1.2** on a JVM is installed on this machine, outside the repo, in a virtualenv
this document calls `<pyspark-4.1.2-oracle>` — run its `bin/python` with `JAVA_HOME` pointed at a
JDK 17 or newer (Spark 4 refuses JDK 11). It runs. That makes it an **independent oracle** in the sense [../testing.md](../testing.md) requires — real
Spark answering, not repark agreeing with itself, and not a signature read off a docstring.

It was used to scope this campaign before any unit was written, and it changed three answers:

| Question | Round 2 assumed | Spark actually |
|---|---|---|
| `F.xxhash64()` | accepted, returns the seed | **raises** `WRONG_NUM_ARGS.WITHOUT_SUGGESTION` |
| `lambda x, /: …` as a higher-order body | should be rejected | **works** |
| a non-callable passed as `f` | should raise `PySparkValueError` | raises plain `TypeError` — already what repark does |
| `regexp_extract_all('🎉ab', '', 0)` | undecided, "needs a Java oracle" | `['','','','','']` — **5**, matching `regexp_count` |
| `randstr(5000000, 1)` | cap assumed to be a parity question | returns 5,000,000 characters; Spark has no cap |
| `groupBy(F.exists(...))` column name | PySpark shows `x` | shows `namedlambdavariable()` — Spark's own name is opaque too |

Three suggested fixes from the Critic round were **wrong**, and following them would have shipped
new divergences. That is the argument for reaching for the oracle rather than the docstring, and it
is now written down so the next campaign does not re-derive it.

The oracle is *outside* this repository and is not a build dependency; nothing in CI can reach it.
It is a measurement tool for the author, and every answer it gives is transcribed into the unit
ledger that used it, so a reader who does not have it can still check the reasoning.
