# python/repark/tests/parity/data/ — committed Spark values for the flat matrix

The Class A expected values of the flat live matrix (ICE-V2-FLAT-AWS-LABEL-1): 420 cells, one per (committed Spark door and seat, layout, predicate) combination, copied from the Spark 4.1.2 legs of the write-path envelope matrix. Each entry holds the committed outcome, the answer ids, the post-statement ids, the snapshot commit delta and the error class.

- [flat_matrix_expected.json](flat_matrix_expected.json) — the values the live module reads; it reads nothing else.
- [build_flat_matrix_expected.py](build_flat_matrix_expected.py) — rebuilds that file from `task/wo/write-path-envelope-matrix-1/evidence/raw/`; the counts are asserted (420 cells).

Up: [../map.md](../map.md).
