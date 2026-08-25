# map — repark-spark/src/tests/fixtures

## Purpose

Checked-in on-disk Iceberg tables the Spark-door unit battery adopts. They exist because
some pins need a table this engine cannot write (format-v3, Puffin deletion vectors).

## Contents

- [v3-spark-mor/](v3-spark-mor/map.md) — Spark 4.0.1 + Iceberg 1.10.0 format-v3 table with
  three live Puffin deletion vectors. V3-1 `CALL system.register_table` + `B-MOR-3`.
- [v3-spark-part-dv/](v3-spark-part-dv/map.md) — **V3E-3:** Spark 4.1.2 + Iceberg 1.11.0
  partitioned format-v3 table with live Puffin deletion vectors (identity `part`).
- [v3-spark-eq-dv/](v3-spark-eq-dv/map.md) — **V3E-3:** Spark 4.1.2 + Iceberg 1.11.0
  partitioned format-v3 table with a Puffin DV **and** an equality-delete file.

## Pointers

- Up: [../map.md](../map.md)
