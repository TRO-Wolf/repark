# task/wo/decimal-divide-literal-1/evidence/built/ — the bar, the cost, the raw data (2026-11-18)

Built by Claude Opus 5.5 (`claude-opus-5-5`) on branch `fix/decimal-divide-literal-1`, product commit `a30a8d60`,
package `pkg-ddl-a30a8d60` (built from a clean worktree of that commit; its `BUILT_FROM` names it). Main's package is
`pkg-avg-27d6149f`; main `cd3d9ac3`'s product tree equals it. **Delta 1** (the same day): product commit `8efe8e69`
(a decimal `mod()` takes the `%` path), package `pkg-ddl-8efe8e69`; it added `bar.md` §0, replaced the cost series and
restructured the scripts to pass `ruff`. **Delta 2** (the same day): product commit `5b34022f` (a decimal `pmod()`
on Spark's formula over `%`, the runtime `SET` of `pickMinimumPrecision`, `F.pmod`), package `pkg-ddl-5b34022f`;
it added `bar.md` §D2 and a new cost series in `cost.md`, under a cap lifted from 5 to 8 on Fable's guidance.

- [bar.md](bar.md) — delta 1's Spark measurement of `mod`/`pmod`/`%` by zero and with negatives, the "NULL → RAISE,
  Spark's answer" class cell for cell, the session-view CTAS cell and the proposed cards (§0); step 0's whole domain
  table re-run on the head (7,258 comparisons, 0 EQUAL → non-EQUAL, the native door, `avg`/`mean`/`sum`, `try_*`,
  typed and folded divisors unchanged, 1,088 cells became EQUAL; unchanged on the delta's head), the named classes
  left, the pins (red on main, green on the head).
- [cost.md](cost.md) — the held rows (SQL division and remainder, DataFrame division, non-decimal controls, C-012),
  delta 1's series on `pkg-ddl-8efe8e69`: five alternating runs, ten for a row over the bar.
- [raw/](raw/map.md) — the head's compacted cells, the comparisons, the pin runs and the cost samples.
- [scripts/](scripts/map.md) — the oracle recorder, the comparison and class scripts, the cost runner.

Up: [../map.md](../map.md).
