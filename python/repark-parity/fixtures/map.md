# map — python/repark-parity/fixtures

## Purpose

Generated-test fixture sources for the parity harness. Data is never committed — the
generators write into a temp dir at test time or under `/tmp/torture/` on the full tier,
and refuse repository-internal output directories — with a single ruled exception:
`fixtures/torture/data/v3_dv/` (TORTURE-1 D-5, the ≤ 1 MB Spark-written DV table).

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
