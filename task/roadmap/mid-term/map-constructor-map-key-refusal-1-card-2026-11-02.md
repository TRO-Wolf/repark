# Card MAP-CONSTRUCTOR-MAP-KEY-REFUSAL-1: `map()` accepts a map-typed key, where Spark refuses it with `INVALID_MAP_KEY_TYPE`

**Date:** 2026-11-02. **Filed by:** `claude-haiku-5-5` for the orchestrator, from the round-1 Opus 5.5 verifier's finding F6 on MAP-STRUCT-KEY-SUBSCRIPT-1 (S2, pre-existing: head equals base).

**Status:** filed; grade C, P3; not dispatched.

**Why:** `map(map(1, 2), 3)` takes a map as its key, and the subscript `map(map(1, 2), 3)[map(1, 2)]` answers `3` on the Spark and ANSI doors. Spark 4.1.2 refuses the constructor with `INVALID_MAP_KEY_TYPE`, because a map key cannot be or contain a map. The fault is the map constructor's missing key-type check, not the subscript, which only looks the key up.

**Site:** not located; the first unit locates the map constructor's key-type check.

**Fix (proposed):** refuse a map key whose type is, or contains, a map, as Spark does, with `DATATYPE_MISMATCH.INVALID_MAP_KEY_TYPE`; pin the Spark class and the type text on the Spark door and the ANSI door.

**Measured (Spark 4.1.2, round-1 verifier):**

```text
SELECT map(map(1, 2), 3)[map(1, 2)]
AnalysisException [DATATYPE_MISMATCH.INVALID_MAP_KEY_TYPE] Cannot resolve "map(map(1, 2), 3)" due to data type mismatch: The key of map cannot be/contain "MAP<INT, INT>". SQLSTATE: 42K09; line 1 pos 7
```

RePark: `[[3]]` on the Spark and ANSI doors, on head and base.

**Retires:** the card when the constructor refuses a map key as Spark does, pinned on both doors.

**Relation:** [MAP-STRUCT-KEY-SUBSCRIPT-1](../../ledgers/staging/map-struct-key-subscript-1-ledger.md) (round-1 residue F6).

Up: [map.md](map.md).
