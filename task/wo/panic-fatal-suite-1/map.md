# task/wo/panic-fatal-suite-1/ — PANIC-FATAL-SUITE-1

PANIC-FATAL-SUITE-1 (2026-10-21, owner ruling): the two follow-ups that MAP-COLUMN-PANIC-1 (PR #1131) left on
`REPARK_PANIC_FATAL`. The switch read the environment on the caught-panic arm, which ADR 0004 bans at query time, and
no suite set it. The owner ruled to keep the switch, read it once when the Python module initialises, and have the
facade suite set it, so that a caught panic in any facade test is a hard failure. The unit built on branch
`fix/panic-fatal-suite-1` from main `f0918cb5` (builder Claude Sonnet 5.5, orchestrator review, PR #1134). It retires
when the pull request merges; the card's status paragraph and the v1.5.6 release-notes draft record it in the same
change.

The work ran in this order, one commit each. First, `fence.rs` reads the variable once into a `OnceLock<bool>` through
`initialise_panic_policy`, `panics_are_fatal` reads the static only and answers `false` when it was never set, and
`_native` calls the initialiser first and publishes `_native.__panic_fatal__` as test surface. The child-process fence
tests keep their assertions because the probe children call the initialiser; one new probe never calls it and shows
that the environment alone no longer turns the flag on. Second, `conftest.py` sets the flag with
`os.environ.setdefault` before the first `repark` import, and `session/test_panic_fatal_suite_1.py` proves the order
through `_native.__panic_fatal__` (red when the line moves below the import) and on both workers of `-n 2 --dist
each`. Third, there was nothing to move into a child interpreter: no facade test provokes a fenced panic on purpose,
so no commit; the pin's own cleared-flag legs are the first. Fourth, the pin runs the native `repark.sql()` door's
`map('a', <column>)` defect in a child that inherits the suite environment and in one with the flag removed, through
both fences. Fifth, ADR 0004 records the allowed class of environment read.

What the measurements say: the armed facade suite finished twice with no failure caused by a panic, and with the
skipped and xfailed sets identical to main's. One failure on main and one in the second armed run are wall-clock tests
that pass alone under load; the first armed run stalled at 99 % for a cause that is not identified. The numbers, the
two mutation checks, the child output and the Rust runs are in [evidence/](evidence/map.md).

Up: [../map.md](../map.md).
