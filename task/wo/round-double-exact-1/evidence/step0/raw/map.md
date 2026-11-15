# task/wo/round-double-exact-1/evidence/step0/raw/ — the committed measurement

The full per-cell output (about 70 MB of JSON lines: Spark and RePark × both ANSI modes, plus the misc
and kernel-check files) stays in the lane's scratch directory; these files hold what the tables cite.

- `values.json` — the 519 DOUBLE and 421 FLOAT values: SQL text, bits, class.
- `jdk-scan-hits.json` — 280,151 doubles scanned on Spark; the 383 whose JDK 17 text is not the shortest
  and rounds differently from the shortest text at some scale from -3 to 30.
- `summary.json` — the verdict counts for every context, door, source, function and mode, the model check
  and the kernel check (both empty: no differences), the type differences and the RePark error texts.
- `sel-diff.tsv` — every SELECT cell where any door differs from Spark: source, function, scale, value id,
  Spark's bits as `0x` hex, then the bits from `sql`, `fcol`, `fexpr` and `native` (`=` where a door equals Spark).
  Identical in both ANSI modes.

Up: [../map.md](../map.md).
