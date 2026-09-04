# map — python/dbt-repark/src/dbt/include/repark

## Purpose

The macro package dbt loads for the `repark` adapter. `dbt.include` is a namespace package, so
this directory becomes `dbt.include.repark` from `sys.path` alone, and `PACKAGE_PATH` is what the
`AdapterPlugin` passes as its `include_path`.

Because the plugin declares `dependencies=["spark"]`, dbt loads `dbt_spark`'s macros too and
dispatches in the order `repark__` → `spark__` → `default__`. Everything here is therefore an
**override of something `dbt-spark` gets wrong for RePark** — nothing here re-implements a macro
that already works.

## Contents

- `__init__.py` — `PACKAGE_PATH`.
- `dbt_project.yml` — the include project (`name: dbt_repark`, `macro-paths: ["macros"]`).
- `macros/` — the overrides and the refusals. See [macros/map.md](macros/map.md).

## Pointers

- Up: [../../../../map.md](../../../../map.md)
- The adapter: [../../adapters/repark/map.md](../../adapters/repark/map.md)
