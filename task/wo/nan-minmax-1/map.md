# task/wo/nan-minmax-1/ — NAN-MINMAX-1

NAN-MINMAX-1 part 1, the float statistics seat (owner, 2026-11-13: "Go with default", before the scoped label; the
sketch's Q3, Q4, Q5 and Q10 ruled "defaults"). DataFusion trusted Parquet's NaN-free float min and max as exact, so a
plain-Parquet file whose other float values are one value read every NaN as that value, and `max`/`min` answered from
the footer. Card [NAN-MINMAX-1](../../roadmap/mid-term/nan-minmax-1-card-2026-11-13.md); design in
[nan-compare-spark-1/](../nan-compare-spark-1/map.md) §4.2(a).

- [evidence/](evidence/map.md) — step 0 on main `631d283c` against Spark 4.1.2 (the constant-file table and every read
  path), then the head; the cost rows; the delta round.

Up: [../map.md](../map.md).
