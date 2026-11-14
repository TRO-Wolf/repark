# task/wo/signed-zero-const-compare-1/evidence/step0/raw/ — raw cells

One JSON object per line, as `../probe_const.py` wrote them: a `leg` header (engine, version, ANSI mode, part), one
`cell` row per cell (door, operator, type, left and right form keys, the expression, the outcome and the rows or
the error), and an `end` row with the run's seconds. Files are `<engine>-<part>-ansi-<true|false>.jsonl`, where
`<engine>` is `spark` (PySpark 4.1.2) or `repark-main` (the release build of `d843d37e`), and `<part>` is `select`,
`nan`, `context` or `dml`. The native door runs only in the RePark `ansi-true` files, because it has no ANSI mode.
Every file is gzip (`gzip -9 -n`). `cells.jsonl.gz` is the comparer's joined output, one row per RePark cell with its verdict and both answers.

Up: [../map.md](../map.md).
