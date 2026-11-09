# task/wo/write-path-envelope-matrix-1/ — WRITE-PATH-ENVELOPE-MATRIX-1, Step 3.1 pulled forward: one predicate set across every seat and door

One fixed set of 36 predicates and one fixed 8-row seed table, crossed with six statement seats
(`select`, `delete`, `update`, `insert`, `ctas`, `merge`), the Spark-SQL door, the native door and the
two DataFrame doors, and three Iceberg table kinds (v2 copy-on-write, v2 merge-on-read, v3), measured on
the main release wheel built from `fdc61f26` and on the Spark 4.1.2 oracle. The invariant: the rows
`SELECT * FROM t WHERE P` returns must be exactly the rows each write seat touches. Measurement only;
no product code; a records-only round with no frontier critic.

- [evidence/](evidence/map.md) — the probe, the raw leg files, the cell table, the summary, the finding lines and the provenance.

Up: [../map.md](../map.md).
