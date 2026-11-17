# Card DATAFUSION-REWRITE-LEADS-1: six agent-only leads from the simplifier audit, measured by an agent and not re-measured

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from section 5 of Fable's simplifier audit of 2026-11-16. Fable did not re-measure these, and this card does not either.

**Status:** ruled 2026-11-16, not dispatched; not label-blocking. Measured by an agent, not re-measured.

**Severity:** S3, ruled 2026-11-16 (the proposed severity stands). A placeholder: each lead is unmeasured and may move once step 0 measures it.

Ruled 2026-11-16 ("Merge those and defaults."): the proposed S3 stands; not label-blocking; the six leads stay unmeasured until step 0.

## Leads

Each lead is the audit agent's claim, with its inventory rule. Measure each against Spark 4.1.2 in both ANSI modes before any severity is set.

1. **Planning raise on a constant.** A failing cast of a constant in a dead `CASE` branch raises at planning, and `-(INT_MIN)` raises at planning. Rules SIM-CONST-DEAD (`ES:545-575`) and SIM-CONST-ARITH (`ES:705-759`).
2. **Folded negation.** `-(-a)` folds to `a`, which hides the ANSI overflow of the smallest INT. Rule SIM-NEG-NEG (`ES:1427-1429`).
3. **Subquery list.** `x IN ((subquery))` answers where Spark raises for a subquery of several rows. Rule SIM-INLIST-SUBQ (`ES:1766-1781`).
4. **Decimal division.** `a / 1` over a DECIMAL takes DataFusion's result type. Rule SIM-DIV-ONE (`ES:1147-1154`).
5. **Regular expressions.** `$` before a final newline differs from Java's (rule REGEX-LOWER, `opt/simplify_expressions/regex.rs:84-95`). REGEX-DIALECT raises at planning for a lookahead or backreference even in a dead branch. The refusal of lookahead and backreference itself is carded in JAVA-REGEX-FEATURES-1.
6. **Float sum.** A float `sum` whose rounding order differs from Spark's. Rule FN-SUM-LINEAR (`opt/simplify_expressions/linear_aggregates.rs:55-128`).

## Covered elsewhere

The audit's `array_contains` lead with `-0.0` is carded in COLLECTION-FLOAT-EQ-1 (`array_contains(array(-0.0D), 0.0D)` is true on Spark and false on main). Lookahead and backreference refusals are carded in JAVA-REGEX-FEATURES-1.

## Not yet known

Every seat, because none of the six is measured. Reproduction: none yet; step 0 runs one probe per lead on both engines and both modes.

Up: [map.md](map.md).
