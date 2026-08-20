# Unit ledger — FNP-4a · the higher-order seam

**Unit:** FNP-4a · **Date:** 2026-08-20 · **Executor:** Claude (Opus 5) ·
**Branch:** `feat/spark-function-parity` · **Base:** `947204c` (FNP-3) ·
**Charter:** [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) clause **C-003** ·
**Design:** [../docs/design/spark-function-parity.md §3](../docs/design/spark-function-parity.md).
**SEPMO:** STANDARD. Floor S1.

**Writable:** `crates/repark-functions/src/{higher_order.rs,lib.rs}`,
`crates/repark-python/src/{column/mod.rs,dataframe.rs}`,
`crates/repark-spark/src/extension.rs`, `python/repark/src/repark/spark/functions*.py`, the facade
tests, this ledger, `task/map.md`, the touched `map.md` files.

## Unit split, taken mid-unit on measured evidence

The design had FNP-4 as "the seam + the eleven, dialect included". It became three units when the
dialect turned out to reach further than the design assumed. **Owner ruling, 2026-08-20:** split.

| Unit | Scope | State |
|---|---|---|
| **FNP-4a** (this) | The seam: registry, binding entry point, lambda-variable resolution, and `exists` — the one Spark higher-order function needing no new kernel | delivered |
| **FNP-4b** | The Spark-door dialect, and making the engine's own generated SQL dialect-independent | planned, evidence below |
| **FNP-4c** | The eight new `HigherOrderUDFImpl` kernels plus `forall` and `reduce` | planned |

## What was actually wrong — two independent causes

DataFusion 54.1 ships the whole machinery: `Expr::HigherOrderFunction`, `Expr::Lambda`,
`Expr::LambdaVariable`, the `HigherOrderUDFImpl` trait, a registry separate from scalar UDFs, and
three working kernels. RePark could reach none of it.

1. **The SQL door** parses `x -> y` as PostgreSQL's JSON arrow, because
   `Dialect::supports_lambda_functions()` is `false` and DataFusion parses with `Generic`. → FNP-4b.
2. **The facade** builds expressions through `call_scalar(name, args)`, and a lambda is not an
   argument — it is a *body* that must be built against a synthetic parameter that does not exist
   until something mints it. → this unit.

Neither is a missing engine capability, and neither is caused by the other.

## The seam

`repark_functions::higher_order` is one table read by two callers: `register` installs it on the
session for the SQL door, `by_name` serves the facade, whose `PyColumn` is standalone and has no
session to resolve against. Reading one table is what keeps C-012 true here by construction.

`PyColumn::lambda_variable` mints a placeholder; `PyColumn::call_higher_order(name, values,
lambdas)` assembles the call. Value arguments first, then one lambda per `(params, body)` — that
is every Spark higher-order signature's actual shape, not a convention this layer invents.

Variables built through the expression API are **unresolved** (`LambdaVariable::field` is `None`);
DataFusion's SQL planner resolves them as it plans, the expression API does not, and an unresolved
variable fails when the plan asks it for a type. `PyDataFrame::bound` runs
`resolve_lambda_variables` against the frame's schema, and every site that hands a column to
DataFusion goes through it.

Python decides nothing semantic: it binds names to positions, which is the callable's own
signature. Parameter names are `x`/`y`/`z` regardless of what the caller wrote, matching PySpark —
they travel into the plan, so a user-chosen name could collide with a column.

**Aliasing is not free.** `exists` ships as an alias of `array_any_match` because that kernel is
bit-for-bit Spark under the default three-valued logic — measured, not assumed: a NULL among
otherwise-false elements yields NULL, empty array yields false, null array yields null.
`transform` and `filter` are **not** aliased onto `array_transform` / `array_filter`: both declare
a single lambda parameter and Spark's `(element, index)` form is a hard plan error against them.
A pin asserts those two spellings do **not** resolve, so a later well-meaning alias reds the build.

## Known limitation, recorded rather than implied

`join_on` resolves lambda variables against the **left** frame's schema, so a lambda over a
right-side column is not covered. The other four consumption sites — `select`, `filter`,
`with_column`, `sort` — are pinned. Naming this beats a test called "every entry point" that
covers four of five.

## FNP-4b's evidence, gathered here

The dialect change was implemented, measured, and unwired; `apply_spark_parser_dialect` stays in
`extension.rs` under `#[expect(dead_code)]` with the measurement attached, so FNP-4b starts from
evidence rather than from scratch.

**It works.** With the Spark door's session on `Dialect::Databricks`, all three DataFusion kernels
became reachable through SQL:

```
SELECT exists(a, x -> x > 2)          => [True]
SELECT array_transform(a, x -> x + 1) => [[2, 3, 4]]
SELECT array_filter(a, x -> x > 1)    => [[2, 3]]
```

**And it costs 5 cross-door DML tests** (`cargo test --workspace`: 1,985 passed, 5 failed — down
from 8 for a session-wide flip, so the per-door scoping does work; the two `repark-core`
struct-literal failures are gone and the native door is untouched).

Root cause, measured: RePark's **own internally-generated SQL** quotes identifiers with ANSI
double quotes (`write/idents.rs::quote_ident_spark`, 35 call sites). A Spark dialect reads double
quotes as **string literals**, so `SELECT "_file", "_pos" FROM …` selects two strings and the
position-delete path fails with ``identity SELECT `_pos` column is not Int64``.

That is registry row **BL-2**'s family, and wider than BL-2 states: not only user-supplied filter
strings, but SQL the engine writes for itself.

The right fix is almost certainly that engine-internal SQL should not depend on the session
dialect at all — there are 9 non-test `ctx.sql(...)` sites in the write path — but that is a
change to the Iceberg write/commit path, and AGENTS.md "Change discipline" forbids a
semantic-adjacent rewrite of a sensitive path riding along with a feature. FNP-4b judges it on its
own evidence.

## Findings

| ID | Sev | Finding | Disposition |
|---|---|---|---|
| F-FNP4A-1 | S1 | No Spark higher-order function was reachable from the facade at all | `REMEDIATED` — `exists` ships; the seam serves the other ten |
| F-FNP4A-2 | S1 | The Spark door cannot parse a lambda | `ACCEPTED_FLAGGED` → FNP-4b, with the fix measured and the blocker identified |
| F-FNP4A-3 | S2 | Engine-internal SQL is coupled to the session dialect through ANSI identifier quoting | `ACCEPTED_FLAGGED` → FNP-4b; adjacent to registry row BL-2, which understates the scope |
| F-FNP4A-4 | S3 | `join_on` resolves against the left schema only | `ACCEPTED_FLAGGED` — recorded above and in the test's docstring |

## Gates

| Gate | Result |
|---|---|
| `cargo test --workspace --no-fail-fast` | **45 binaries, 1,990 passed, 0 failed**, cargo exit 0 (1,987 + this unit's 3 registry tests) |
| `make ci` | exit **0**. Four reds on the way, all lint: `rust-fmt-check`, two `clippy::doc_markdown` rounds (prose "RePark"/"DuckDB" want backticks), and ruff import ordering. |
| facade pytest (full) | **3,475 passed, 70 skipped, 0 failed** |
| dialect-on measurement (unwired) | 1,985 passed, **5 failed**, all `cross_door.rs` — recorded above for FNP-4b |

## Fresh execution (binding manifest `s0_fresh_execution`)

Novel input through the public surface, absent from any committed test, chosen to attack the one
claim whose failure class is silently-wrong-results — *is the lambda actually evaluated by the
engine, or is Python in the row loop?*

```
python lambda invocations after building the Column : 1
python lambda invocations after select()            : 1
python lambda invocations after executing on data   : 1
result                                              : [True]

logical_plan   Projection: Boolean(true) AS r
physical_plan  ProjectionExec: expr=[true as r]
```

The callable runs **once**, at expression-build time, and never again — and DataFusion
constant-folded the whole `exists(array, x -> x > 2)` to `true` at plan time. The optimizer could
only do that by reasoning about the expression natively; an opaque Python callback is not
foldable. That is the strongest available evidence that the lambda is an ordinary Rust expression
tree rather than a callout, and it is why C-002 ("Python never touches rows") still holds with
higher-order functions on the surface.
