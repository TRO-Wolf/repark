# Card SUM-EXACT-TOTAL-DIVERGENCE-1: `sum` and `try_sum` answer the exact total past 38 digits, where Spark's running total comes back as NULL or raises (a kept divergence)

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`). **Source:** the SUM-DEC-I128WRAP-1 bar, [bar.md](../../../wo/sum-dec-i128wrap-1/evidence/built/bar.md), the section "The cards" of delta 2 (the paragraph that starts **SUM-EXACT-TOTAL-DIVERGENCE-1**); and the SUM-DEC-I128WRAP-1 entry of the [parity registry](../../../../docs/spark-sql-iceberg-parity.md), its delta 3 paragraph, which names this card and the kept divergence.

**Status:** filed 2026-11-19; a kept divergence, not a defect to fix. It supersedes SUM-GROUPED-MERGE-TOTAL-1, the delta 1 card proposed in the same bar.

**Rating:** owner-accepted by decision I, 2026-11-18, about 20:05 UTC. Owner, verbatim: "Go with Fable's recommendations on all seven, re-run 1362" ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-18.md), section 8).

## Finding

Decimal `sum` and `try_sum` answer the exact total whenever it fits the result precision. Where Spark's own running total passes 38 digits and comes back (per row, per split or in its merge order), Spark raises `ARITHMETIC_OVERFLOW` (ANSI on) or answers NULL, and RePark answers the exact total on every run, layout and machine. That includes ANSI-on Iceberg writes Spark refuses and RePark commits, and ANSI-off writes that store the exact total where Spark stores NULL.

- **Cells:** the kept divergence (133 / 161 against `local[1]`, 30 per mode against `local[4]`), the pin file's `kept-divergence:` cells and `t4` group 21.
- **Condition class (23 cells):** a non-window total past 38 digits raises `ARITHMETIC_OVERFLOW` where Spark raises `NUMERIC_VALUE_OUT_OF_RANGE` after a merge (ANSI on; the pin file's `condition-class:` cells).
- **Label:** the label wording is the orchestrator's to file. The owner's ruling of 2026-11-18 says the label text names `sum`'s kept divergence; its list is in [kept-divergences.md](../../../wo/ice-v2-flat-aws-label-1/design/kept-divergences.md).

**Main:** PR #1360 merged as `77faf69e`, so main answers the exact total.

Up: [map.md](map.md).
