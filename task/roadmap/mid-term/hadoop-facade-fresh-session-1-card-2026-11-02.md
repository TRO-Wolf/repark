# Card HADOOP-FACADE-FRESH-SESSION-1: a fresh session on a `type=hadoop` catalog lists no table that Spark's HadoopCatalog finds in the same warehouse directory

**Date:** 2026-11-02. **Filed by:** `claude-haiku-5-5` for the orchestrator, from the MAP-STRUCT-KEY-SUBSCRIPT-1 hand-back (the residue R4 of its records) and the orchestrator's measurement of the same sequence.

**Status:** recorded design, measured 2026-11-02 by the orchestrator; grade C, P3; no fix proposed; not dispatched. Retires if the owner rules that a `type=hadoop` catalog must discover namespaces and tables from its warehouse directory as Spark does.

**Why:** A fresh RePark session with a `type=hadoop` catalog over the warehouse that an earlier, stopped session wrote lists no namespace (`SHOW NAMESPACES` is empty), and a read answers `SCHEMA_NOT_FOUND` and `table not found`. Spark's HadoopCatalog over the same directory lists the table and reads its rows. This is the design that the parity registry records: [ICE-CATALOG-SESSION-HADOOP-1](../../../docs/spark-sql-iceberg-parity.md) aliases `type=hadoop` to the in-process memory catalog, and [ICE-LOCAL-COMMIT-POINT-1](../../../docs/spark-sql-iceberg-parity.md) says that after a process ends the reopen is `CALL <catalog>.system.register_table` on the newest metadata file. The MAP-STRUCT-KEY-SUBSCRIPT-1 facade cells use that reopen.

**Site:** the registry's alias of `type=hadoop` to the in-process memory catalog (ICE-CATALOG-SESSION-HADOOP-1). No code site is located, and none is proposed.

**Fix (proposed):** none. The card records a divergence from Spark. Any change to discovery from the warehouse directory is an owner ruling, because it reverses ICE-CATALOG-SESSION-HADOOP-1's alias.

**Measured (2026-11-02, the orchestrator):** a fresh RePark session with a `type=hadoop` catalog over the warehouse written by an earlier, stopped session: `SHOW NAMESPACES` is empty, and reads answer `SCHEMA_NOT_FOUND` and `table not found`. Spark 4.1.2's HadoopCatalog over the same directory lists the table and reads its rows. The builder's facade cells reopen with the memory catalog and `register_table` on the newest metadata file, and they read the table (C-020 of the unit ledger).

**Retires:** the card when the owner rules that a `type=hadoop` catalog discovers namespaces and tables from its warehouse directory as Spark's HadoopCatalog does, and the rule is pinned; or when the owner rules the reopen-by-`register_table` shape as the kept design, and the registry row records that ruling.

**Relation:** [MAP-STRUCT-KEY-SUBSCRIPT-1](../../ledgers/staging/map-struct-key-subscript-1-ledger.md) (residue R4; the facade cells in `python/repark/tests/functions/test_map_struct_key_subscript_1.py` use the reopen step); `python/repark/tests/iceberg/test_ice_idless_struct_1.py` (the same reopen, pre-existing); ICE-CATALOG-SESSION-HADOOP-1 and ICE-LOCAL-COMMIT-POINT-1 in the parity registry.

**Append (2026-11-13):** seen again by NAN-WRITE-CANONICAL-1 verifier 1, 2026-11-13 (#1325, finding C4): a RePark
`type=hadoop` catalog opened in a new session over an existing warehouse reports `SCHEMA_NOT_FOUND` / `table … not
found` for tables another RePark process wrote there, while Spark finds them. Verifier 2 (C2) saw the same. Both
verifiers, like the builder, reached the tables with `register_table`. Unchanged: still not ruled, not dispatched.

Up: [map.md](map.md).
