# Card AVG-DECIMAL-GROUPED-RUN-VARIANCE-1: a grouped decimal `avg` answers differently from run to run, and with stealing off follows the core count

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's rulings of the 2026-11-18 window (item 8; [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-18.md)). **Source:** the grouped-`avg` section of the sum sizing, `N/sum-sizing-out/sizing.md` (lines 394 to 408, measured on main `pkg-view1-c54f5fc7`); the sum bar at commit `8325bfb0` (`task/wo/sum-dec-i128wrap-1/evidence/built/bar.md`, lines 317 to 330, read with `git show`). Related: [AVG-GROUPED-SUM-STICKY-1](../consult-2026-11-17/avg-grouped-sum-sticky-1-card-2026-11-17.md).

**Status:** filed 2026-11-19, not dispatched. Built next on lane B, after SUM-DEC-I128WRAP-1, as its own unit.

**Severity: S2, not label-blocking** (owner, 2026-11-18, about 20:05 UTC, verbatim "Go with Fable's recommendations on all seven, re-run 1362"; item 8 names the grouped decimal `avg` card S2, not blocking, built next on lane B after `sum`).

## Finding

Grouped `avg` on the `big` table, ANSI off, stealing on: 4 answers at 4 cores (583, 398, 618 or 645 NULL groups of 3,000), 3 at 8 cores, 3 at `SET target_partitions` 4, 4 at `SET` 8, and 1 at 2 and 16 cores (2 cores: 0 NULL groups). The exact average is `0.0167` in every group.

- Stealing off: steady, but the answer follows the core count: 3,000 NULL groups at 2 cores, 398 at 4, 437 at 8, 0 at 16. ANSI on: raises at 2, 4 and 8 cores, answers at 16.
- On the `t4` table, stealing on, at 2 cores or `SET 2`: g23 and g25 vary between `1.6667` and NULL (ANSI off), and between `1.6667` and `ARITHMETIC_OVERFLOW` (ANSI on). g21 with ANSI on alternates between `NUMERIC_VALUE_OUT_OF_RANGE` (18 or 11 times) and `ARITHMETIC_OVERFLOW` (2 or 9 times).
- Spark 4.1.2 on `big`: every group NULL or `ARITHMETIC_OVERFLOW`, one answer, at `local[1]` and `local[4]`.
- In the sum bar, across its 24 configurations, only grouped `avg` varies (up to 4 answers; `t4x` 2 at 2 cores and `SET 2`).
- The grouped partial ships one column with a sentinel, so a partial whose own total leaves 38 digits marks the group. AVG-GROUPED-SUM-STICKY-1 ("checks once") is therefore not stable run to run.

**Main:** the varying answers above are main's own, measured on `pkg-view1-c54f5fc7` (the sizing). Main's grouped `avg` varies run to run with stealing on.

Up: [map.md](map.md).
