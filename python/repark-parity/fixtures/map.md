# map — python/repark-parity/fixtures

## Purpose

Generated-test fixture sources for the parity harness. Data is never committed — the
generators write into a temp dir at test time or under `/tmp/torture/` on the full tier,
and refuse repository-internal output directories — with three ruled exceptions under
`fixtures/torture/data/`: `v3_dv/` (TORTURE-1 D-5, the ≤ 1 MB Spark-written DV table),
`ice_spark_table_1/` (ICE-SPARK-TABLE-1, the 67.8 KB Spark-written v2 CoW table), and
`ice_v3_write_default_1/` (ICE-V3-WRITE-DEFAULT-1, the 225.1 KB Spark-written v3
column-defaults tables plus the 22-cell oracle).

## Contents

- [torture/](torture/map.md) — the TORTURE-1 generator package. It lives outside the hatch
  package and is importable as `repark_parity.torture` through a checkout-only `__path__`
  graft in [../src/repark_parity/__init__.py](../src/repark_parity/__init__.py): the
  package `__path__` gains this `fixtures/` directory when it exists beside `src/`, which
  keeps the card's home directory, module path, and `python -m repark_parity.torture`
  CLI spelling all true without a `pyproject.toml` edit. A wheel never ships `fixtures/`,
  so the graft is a no-op there.

## Pointers

- Up: [../map.md](../map.md)
