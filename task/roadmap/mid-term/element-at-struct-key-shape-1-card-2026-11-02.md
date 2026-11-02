# Card ELEMENT-AT-STRUCT-KEY-SHAPE-1: `element_at` with a struct key of another field order or name casts by name, where Spark refuses

**Date:** 2026-11-02. **Filed by:** `claude-haiku-5-5` for the orchestrator, from the MAP-STRUCT-KEY-SUBSCRIPT-1 hand-back (the residue R2 of its records).

**Status:** filed; grade C, P3; not dispatched. RePark's answer is unmeasured: the first unit measures it before any fix.

**Why:** On a map `MAP<STRUCT<k: INT, j: INT>, INT>`, Spark 4.1.2 refuses `element_at(m, named_struct('j', 2, 'k', 1))` with `DATATYPE_MISMATCH.MAP_FUNCTION_DIFF_TYPES`, because the key's struct type differs from the map's key type. RePark's `element_at` coercion casts the key by field name, so it may answer a value where Spark refuses. The subscript form `m[named_struct('j', 2, 'k', 1)]` is not affected: MAP-STRUCT-KEY-SUBSCRIPT-1 casts that by position, as Spark does (Spark answers `[[1, None], [2, None]]` there).

**Site:** the argument coercion of `element_at` in RePark, which casts a struct key by name. The exact site is not located yet; the first unit locates it.

**Fix (proposed):** the first unit measures RePark's answer on the shape above. A struct key whose field names or order differ from the map's key type refuses as Spark does, with the same error class; the pin is the Spark refusal text's class and the required type.

**Measured (Spark 4.1.2, copied from the MAP-STRUCT-KEY-SUBSCRIPT-1 builder's notes, measured once on a hadoop catalog):**

```text
SELECT id, element_at(m, named_struct('j', 2, 'k', 1)) FROM c
AnalysisException [DATATYPE_MISMATCH.MAP_FUNCTION_DIFF_TYPES] Input to `element_at` should have been "MAP" followed by a value with same key type, but it's ["MAP<STRUCT<k: INT, j: INT>, INT>", "STRUCT<j: INT NOT NULL, k: INT NOT NULL>"]. SQLSTATE: 42K09
```

RePark on this shape: unmeasured.

**Retires:** the card when `element_at` with a struct key of another field order or name refuses as Spark does, pinned, or when the owner rules the cast-by-name answer as a kept divergence.

**Relation:** [MAP-STRUCT-KEY-SUBSCRIPT-1](../../ledgers/staging/map-struct-key-subscript-1-ledger.md) (residue R2; its clause C-015 keeps the shape refusal of the subscript form); the registry section [ELEMENT-AT-ALIAS-1](../../../docs/spark-sql-iceberg-parity.md) is a different defect (the alias binding) and is not retired by this card.

Up: [map.md](map.md).
