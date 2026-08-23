# Unit ledger — LRS-1 · a refusal, not a plan dump

**Date:** 2026-08-20 · **Branch:** `fix/low-risk-sweep` · **Charter:**
[lrs-0-charter-ledger.md](2026-08-21-lrs-0-charter-ledger.md) · **Design:**
[../docs/design/low-risk-sweep.md](../../../../docs/design/low-risk-sweep.md) §3 LRS-1 ·
**Source findings:** round 2 F-R3-3 / FNP-R3-4, FNP-R3-3, F-R3-6

## Measured before anything was edited

| Shape | repark, at `19fb0c8` | Spark 4.1.2 (oracle) |
|---|---|---|
| higher-order call in a **value argument** | `AnalysisException: Error during planning: unresolved LambdaVariable x_0` | works |
| higher-order column as a **`Window.orderBy`** key | `AnalysisException: SanityCheckPlan … BoundedWindowAggExec` | works |
| higher-order column as a **`Window.partitionBy`** key | `PySparkException: Internal error: ORDER BY column cannot be empty. This issue was likely caused by a bug in DataFusion` | works |
| higher-order column under **`cube`** / **`rollup`** | `ParseException: ParserError("Expected: SELECT, VALUES, or a subquery … at Line: 1, Column: 593")` | works |

`partitionBy` is not in any of the three findings — it was found here, by asking whether the
`orderBy` defect's sibling position had the same problem. It did, with a *different* internal error.
The unit would have shipped a half-fix without that check.

**Every one of these works in Spark**, so none of them is an unsupported shape. Each refusal says
that plainly. A message implying Spark rejects it too would have been false, and it is the kind of
false statement a user cannot check without installing Spark.

## What changed

**One guard, both argument positions.** `refuse_nested_higher_order` walked lambda bodies only.
`PyColumn::call_higher_order` now runs the same walk over each **value argument** before pushing it,
with the position named in the message. The walk itself is factored out as
`expr_build::contains_higher_order` so the guard and the facade ask the same question of the
expression — not of its rendered SQL text, which would be answering something else.

**A facade predicate, not a string check.** `PyColumn::contains_higher_order` exposes that walk, and
`Column._reject_higher_order(operation)` raises `UnsupportedOperationException` naming the operation,
the column, the fact that Spark supports it, and the workaround.

**Two call sites, both at the narrowest shared point.** `window._normalize_window_column` is the one
normalizer both `partitionBy` and `orderBy` pass through; `DataFrame._grouping_sets_grouped` is the
one entry for `cube` / `rollup` / `groupingSets`. Each is a single line, in a loop that already
rejects partition transforms and nested generators — the refusal reads like its neighbours because
it is the same kind of thing.

The `cube` / `rollup` refusal fires **before** the SQL text is built. Refusing after the parser has
already failed would have reformatted the same error rather than replaced it.

## The workaround is pinned, because a message that names one has to mean it

`df.select(exists(...).alias("e")).cube("e").count()` returns `(True, 2)` and `(NULL, 2)` — which is
what the oracle returns for the refused spelling. The refusal costs the user one `select`, and the
pin proves it.

## Evidence

- 9 pins in `python/repark/tests/test_lrs1_higher_order_refusals.py`. Run against the base with the
  fix stashed: **5 failed, 4 passed** — the five refusals red, and the workaround and
  over-firing checks green on both sides, which is what those two are for.
- `cargo test --workspace` — 45 binaries, **1,990 passed, 0 failed**.
- facade — **3,557 passed, 70 skipped, 0 failed** (3,548 on the base, +9).
- `make ci` — exit 0. Each captured alone.

Two pins exist only to bound the change: ordinary columns still pass every one of these paths, and
higher-order columns still work everywhere they worked before (`select`, `alias`, `groupBy`, and a
lambda body capturing an outer column). **A refusal that over-fires is a worse regression than the
internal error it replaced**, and nothing in the fix's own logic would catch that.

## Found here, not fixed here

`Window.partitionBy("k")` with no `orderBy` and no explicit frame fails for **any** column, not just
a higher-order one — `Internal error: ORDER BY column cannot be empty`. Spark answers `[2, 2, 1]`;
the frame-carrying spelling `partitionBy("k").rowsBetween(unboundedPreceding, unboundedFollowing)`
already answers correctly here. That is a real defect on ordinary PySpark, it is **pre-existing**
(measured on the base), and it is out of this unit's scope. Carried as **LRS-7**.

## Disposition

**DELIVERED.** Charter C-001 held: every shape this unit touches was failing before it.
