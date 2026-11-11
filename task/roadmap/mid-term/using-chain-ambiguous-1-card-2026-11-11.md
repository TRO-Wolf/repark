# Card USING-CHAIN-AMBIGUOUS-1: a USING chain sharing one key list refuses as ambiguous from four tables on

**Date:** 2026-11-11. **Filed by:** the records clerk (`claude-haiku-5-5`), from the Muse MAX round 1 observations on PR #1295 and the builder's notes (`handback-notes.md`, the "Moves, cost, cards" section).

**Status:** filed 2026-11-11, not dispatched. Severity **S3**. Pre-existing: main and head refuse the same way.

**Measured:** a `USING` chain over four tables that share one key list refuses with `AMBIGUOUS_REFERENCE`. Spark answers. The cell ids are not recorded in the round 1 verdicts; the repro is the four-table chain.

**Close:** the four-table `USING` chain answers as Spark does, with a pin that names its cells.
