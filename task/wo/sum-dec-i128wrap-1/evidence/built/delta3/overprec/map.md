# task/wo/sum-dec-i128wrap-1/evidence/built/delta3/overprec/ — the verifier's F1 files, measured on three builds

The verifier's over-precision reproduction (`S/verify-sum/overprec/`), re-made byte for byte and widened: Parquet
footers that declare `decimal(19,0)` or `decimal(20,0)` over 16-byte values near 10^38, one file and two (the second
file one row of `0`, so the ungrouped plan has a partial step), `target_partitions` 1, 2, 4 and 8, both ANSI modes.
Spark 4.1.2 refuses to read every file (the verifier's `spark-l1.txt`). Comment-free Python.

- `craft.py` — writes the files: `wrap5` (`[A, A, A, R]`, `A = 10^38 − 1`, total `2^128 + 5`), `neg` (its negation),
  `fit` (`[A, A, −A, 7]`, total `10^38 + 6`), `filler` (`[0]`), and the two-file folders.
- `probe.py` — reads each file through RePark and runs `sum`, `try_sum` and `avg`, ungrouped and grouped, plus the
  ungrouped plan's aggregate mode; one JSON line per answer.
- `expect.py` — checks every `sum` and `try_sum` answer against delta 3's expectation (NULL, `ARITHMETIC_OVERFLOW`,
  or at precision 19 a multi-phase partial's "past its declared precision" error) and tallies the rest.
- `table.py` — prints one line per query, file and mode across the four partition counts.
- `pkg-sum-845766fd.jsonl`, `pkg-sum-fcb0c888.jsonl`, `pkg-main-4c365814.jsonl` — the answers on delta 3's head,
  delta 2's head and main now (672 lines each).
- `expect-pkg-sum-845766fd.txt` (384 of 384 as expected), `expect-pkg-sum-fcb0c888.txt` (128 values: `5` and `-5`
  from the ungrouped path), `expect-pkg-main-4c365814.txt` (192 values and 36 NULLs where ANSI-on `sum` raises).

Up: [../map.md](../map.md).
