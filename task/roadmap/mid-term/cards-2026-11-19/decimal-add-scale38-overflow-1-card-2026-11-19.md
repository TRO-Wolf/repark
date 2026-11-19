# Card DECIMAL-ADD-SCALE38-OVERFLOW-1: `x + 1` on a `decimal(38,38)` column value raises Arrow's arithmetic overflow where Spark answers

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's rulings of the 2026-11-18 window (item 10; [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-18.md)). **Source:** the sum bar at commit `8325bfb0` (`task/wo/sum-dec-i128wrap-1/evidence/built/bar.md`, the expression-card paragraph, `git show`), and the sum verdict's C3 (`S/verify-sum/verdict.md`).

**Status:** filed 2026-11-19, not dispatched. Not the sum unit's seat: the expression layer.

**Severity: S3, not label-blocking** (owner, 2026-11-19, about 01:28 UTC, verbatim "Go with your recommendations and tonight you don’t even need to wait on me even if fable says that it’s my call. Merge what you need, follow the goal and good luck"; the rulings record's item 10 names it not blocking).

## Finding

`x + 1` on a `decimal(38,38)` value of `0.99999999999999999999999999999999999999` (a column, not a literal) raises `Arrow error: Arithmetic overflow` in RePark's decimal addition, in both ANSI modes. Spark 4.1.2 answers `2.000000000000000000000000000000000000` (`decimal(38,36)`) in both (measured; a literal `0.5` answers `1.5` on both engines).

- The sum bar's `sum/plus1/g11/c38_38` (ANSI off) shows it now because the total is `M` at scale 38 instead of NULL. With ANSI on it changes the error of `sum/beside/g11` and `sum/plus1/g11`.
- The sum verdict's C3 confirms the loud expression cell `sum/plus1/g11/c38_38` (ANSI off) is REPARK_RAISES on main too.

**Main:** raises the same way, `Arrow error: Arithmetic overflow`, on main `6286ba19` and on the head alike, in both ANSI modes.

Up: [map.md](map.md).
