# map — repark-core/src/local_fs_policy/native_ctas

## Purpose

The seat tests for `../native_ctas.rs`, NATIVE-CTAS-ICEBERG-REFUSE-1's native-door CTAS refusal.

## Contents

- [`tests.rs`](tests.rs) — through a real guarded session (`with_local_fs_guard`) with a memory
  catalog `ice` and a `type=hadoop` catalog `hd`:
  `every_ctas_into_an_iceberg_catalog_refuses_before_any_create` (eight spellings, six query shapes,
  both catalogs, a mixed-case target: `Error::NotImplemented` with the exact text, no table after);
  `the_refusal_names_a_quoted_target_so_its_workaround_reaches_that_table` (`"MixQ"` on both
  catalogs: the text names it quoted, and the workaround followed literally fills that table);
  `an_unqualified_ctas_under_an_iceberg_default_catalog_refuses`;
  `a_refused_replace_leaves_the_existing_table_and_its_rows` (`OR REPLACE` with a failing and a
  succeeding query, `IF NOT EXISTS`, plain CTAS over an existing table);
  `the_controls_keep_answering` (in-memory CTAS bare and qualified, plain `CREATE TABLE`,
  `INSERT … SELECT`, `EXPLAIN INSERT`, an in-memory `CREATE VIEW`);
  `a_ctas_into_a_memory_schema_under_an_iceberg_catalog_still_stores_rows`; and
  `the_walk_names_the_resolved_target_and_only_iceberg_schemas` (the predicate and the walk).

## Pointers

- Up: [local_fs_policy map](../map.md)
- The facade pin: `python/repark/tests/sql/test_native_ctas_iceberg_refuse_1.py`
