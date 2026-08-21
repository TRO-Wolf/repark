# map — repark-spark/src/tests/fixtures

## Purpose

Checked-in on-disk Iceberg tables the Spark-door unit battery adopts. They exist because
some pins need a table this engine cannot write (format-v3, Puffin deletion vectors).

## Contents

- [v3-spark-mor/](v3-spark-mor/map.md) — Spark 4.0.1 + Iceberg 1.10.0 format-v3 table with
  three live Puffin deletion vectors. V3-1 `CALL system.register_table` + `B-MOR-3`.

## Pointers

- Up: [../map.md](../map.md)
