# map — task/wo/signed-zero-normalize-1/evidence/item2

## Purpose

Build item 2 of SIGNED-ZERO-NORMALIZE-1 part A: the generator of the facade pin data.

## Contents

- [pin_cells.py](pin_cells.py) — reads item 1's base RePark legs (`item1/raw/repark-68b09d18-reads.jsonl`,
  `-add.jsonl`) for each cell's executed text and the Spark legs (`raw/spark-signed-zero-reads.jsonl`,
  `item1/raw/spark-add.jsonl`) for its truth (the `truth_layout` answer, as `item1/labels.py` reads it). It keeps
  groups D2–D6, D13 and A1–A3 on the test layout (A2 on the native door only: the Spark door's `PREPARE` is
  `SPARK-DOOR-PREPARE-1`), and writes `python/repark/tests/sql/data/signed_zero_normalize_1_cells.jsonl`. D6 rows
  carry the base names; D6-2 on `spark-sql` refused at base, so it carries the head's measured name, which spells
  the user's comparison. `RESIDUES` marks the 8 Spark-door cells whose computed decimal operand still refuses
  (`DECIMAL-EXPR-FLOAT-COMPARE-1`). The delta appends `DELTA_PINNED` (groups V-B1 and V-A3) from
  `item3/raw/delta-spark.jsonl` (truth and names) and `delta-repark-68b09d18.jsonl` (text and base label).
