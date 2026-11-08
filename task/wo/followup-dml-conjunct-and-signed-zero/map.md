# task/wo/followup-dml-conjunct-and-signed-zero/ — DML conjunct drop and signed zero (envelope follow-up)

Two domain tables carried over from the write-path envelope matrix, measured on the Spark 4.1.2 oracle and on the
release wheel built from `fdc61f26`. Table A asks whether a DML predicate that the Spark SQL door refuses can still
drop a conjunct and widen a write. Table B asks where `-0.0` against `0.0` reaches reads and writes. Measurement
only; no product code; a records-only round with no frontier critic.

- [evidence/](evidence/map.md) — the probe, the raw leg files, the two tables and the provenance.

Up: [../map.md](../map.md).
