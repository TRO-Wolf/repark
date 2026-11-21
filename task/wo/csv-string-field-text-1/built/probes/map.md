# task/wo/csv-string-field-text-1/built/probes/ — the wiring unit's probes (comment-free Python)

- `pins.py` — the pin file's cells, run on Spark 4.1.2 (recorded) and on each package, both ANSI modes: the card's
  reads, N1, misfits, pairing and width, the late misfit, header names, the local scan topology, the refusals and
  the two Iceberg v2 writes.
- `compare.py` — joins a package's `pins.py` or `s3.py` records with Spark's: EQUAL, WRONG, RAISE, BOTHERR.
- `s3.py` — the S3 cells on a local moto server (`uv run --offline --with moto[server]`; no external call): one file,
  a prefix of two files, a glob, two files with headers in different orders, `gzip`, a late misfit. Its `spark`
  engine reads byte-identical local files on Spark.
- `shapes.py` — empty and header-only files, a directory whose first file is empty, `multiLine`, duplicate header
  names, a short row and the field types Spark's CSV source refuses.
- `fix1.py` — the fix round's cells, each under an explicit `spark.sql.caseSensitive`: case-differing and
  colliding header names with and without a schema, and the corrupt-record column first, in the middle, last,
  typed `INT`, named by the option or by `spark.sql.columnNameOfCorruptRecord`, and in another case.
- `rerun.py` — step 0's domain, revision and kernel cells on the base and the head against Spark's recorded cells:
  verdicts per set and build, the moves, and the EQUAL cells lost.
- `cost_table.py` — the cost table from `cost.py`'s samples and `perf stat` files: medians, ratios, instruction and
  cycle counts per query.
- `cost.py` — the cost cases (`k0_count`, `k0_hour`, `is_count`, `is_sum`, `is_none`, `is_infer`): a warm-up, then
  ten timed queries inside a `perf stat --control` window.

- `WordClassTable.java` — delta 2's generator of `java_case.rs`'s `WORD_CLASS_RUNS`: each code point's class in the
  legacy word `BreakIterator` rules and `ConditionalSpecialCasing.isCased`, from Zulu 17's `Character.getType`.
- `JavaLower.java` — Zulu 17's `String.toLowerCase()` over a file of `\u{…}`-escaped strings (the final-sigma
  test table's answers).
- `JavaSweep.java` — Zulu 17's sigma forms for every code point in 22 contexts around a U+03A3; the same contexts
  through `string_lowered` gave 0 differences in 24,465,408 strings.
- `JavaFuzz.java` — seeded random strings (1 to 9 code points from one alphabet) with Zulu 17's lowered output; three runs, 9,000,000
  strings, 0 differences against `string_lowered`.
- `delta2_callers.py` — one Spark cell family per caller of `java_case`'s lowering (the CSV header rule, the
  `withColumns` duplicate check, the `fillna`/`dropna`/free-Column `drop` fold, and the `equalsIgnoreCase` fold's
  `drop`, `dropDuplicates` and map struct keys), each under an explicit `spark.sql.caseSensitive`;
  `delta2_compare.py` tabulates the verdicts.

Up: [../map.md](../map.md).
