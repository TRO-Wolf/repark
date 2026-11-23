# Card ARROW-ERROR-NUL-ABORT-1: a cast error over text holding a NUL byte aborts the Python process

**Date:** 2026-11-23. **Filed by:** the orchestrator (`claude-opus-5-5`), from the STRING-CAST-LENIENT-1 INT-slice
builder's report (PR #1413) and a re-measurement here. **Severity:** S2 proposed (owner to rate): loud, but the
whole interpreter dies, so a scheduler task loses every result and its cleanup. **Status:** filed, not dispatched.

## Measured

RePark main `5caea2a5` (the 1.5.7 product tree), facade `spark.sql`, a temporary view over
`createDataFrame([("\x00x",), ("1",)], "s string")`, `SELECT CAST(s AS <type>) FROM t`:

| Target | ANSI on | ANSI off |
|---|---|---|
| INT | process aborts (exit 134) | process aborts (exit 134) |
| DATE | process aborts | process aborts |
| DECIMAL(5,2) | process aborts | process aborts |
| BOOLEAN | process aborts | process aborts |
| DOUBLE | process aborts | answers `NULL, 1.0` |

The same casts over a literal (`CAST(concat(chr(0), 'x') AS INT)`) fail while the plan is simplified, and raise a
`PySparkException` without aborting. Spark 4.1.2 answers `CAST_INVALID_INPUT` with ANSI on and NULL with ANSI off.

## Cause (from the builder's reading, not re-derived here)

The Arrow cast error's text carries the NUL byte into Arrow's C stream error message, and building that message
panics inside a function that cannot unwind, so the process aborts. PR #1413 renders a NUL as `\u0000` on the
STRING-to-integer path only; every other Arrow error that can quote a NUL is unchanged.

## First step

Find the seat where an engine error becomes the stream's error message, and escape NUL there once for every error,
rather than per cast. Pin each target above in both ANSI modes in a subprocess, so an abort fails the test instead of
the run.

Up: [map.md](map.md).
