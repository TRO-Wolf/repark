# Card MAP-CONSTRUCTOR-FLOAT-DUPLICATE-KEY-1: `map()` builds struct and array keys that differ only by float sign or NaN bits, where Spark raises DUPLICATED_MAP_KEY

**Date:** 2026-11-02. **Filed by:** `claude-haiku-5-5` (records clerk) for the orchestrator, from the MAP-STRUCT-KEY-SUBSCRIPT-1 round-4 verifier verdict (its F3).

**Status:** filed; grade C, P3; not dispatched. Pre-existing: head equals base for the constructor (the base panicked on the subscript of these maps before the subscript fix).

**Why:** Spark 4.1.2 compares map keys after normalising floating leaves: `-0.0` and `0.0` are one key, and every NaN bit pattern is one key. A literal map whose two keys are equal under that rule raises `DUPLICATED_MAP_KEY` under the default policy. RePark's constructor duplicate check compares the keys without that normalisation, so it builds both entries, and a subscript then answers the first matching entry. An exact duplicate struct key does refuse in RePark; only the float-differing forms are missed. Over stored data holding both keys, both engines give the first match, so the subscript agrees with Spark there; the gap is the literal constructor.

**Measured (Spark 4.1.2 against RePark, copied from the round-4 verifier):**

```text
map(named_struct('k', -0.0D), 1, named_struct('k', 0.0D), 2)[named_struct('k', 0.0D)]
    Spark: [DUPLICATED_MAP_KEY] Duplicate map key [0.0] was found   (LAST_WIN gives 2)    RePark: 1
map(named_struct('k', CAST('NaN' AS DOUBLE)), 1, named_struct('k', sqrt(-1D)), 2)[named_struct('k', CAST('NaN' AS DOUBLE))]
    Spark: DUPLICATED_MAP_KEY [NaN]                                                       RePark: 1
map(array(-0.0D), 1, array(0.0D), 2)[array(0.0D)]
    Spark: DUPLICATED_MAP_KEY [0.0]                                                       RePark: 1
size(map_from_arrays(array(named_struct('k', -0.0D), named_struct('k', 0.0D)), array(1, 2)))
    Spark: refuses (DUPLICATED_MAP_KEY)                                                   RePark: 2
```

The scalar form `map(-0.0D, 1, 0.0D, 2)[0.0D]` refuses in Spark and answers 2 in RePark; it is a scalar key and belongs to [SCALAR-FLOAT-KEY-SIGNED-ZERO-NAN-1](scalar-float-key-signed-zero-nan-1-card-2026-11-02.md), not this card.

**Fix (proposed):** the constructor's duplicate check normalises floating leaves the way Spark's float normalisation does before it compares keys, so a struct, array or map key that Spark treats as a duplicate refuses with `DUPLICATED_MAP_KEY` under the default policy. The site is the constructor, not the subscript; the subscript needs no change. The cells above are the pins.

**Retires:** the card when the three constructor rows and the `map_from_arrays` row refuse as Spark does, pinned.

**Relation:** [MAP-STRUCT-KEY-SUBSCRIPT-1](../../ledgers/staging/map-struct-key-subscript-1-ledger.md) clause C-052. The constructor's refusal of map-typed keys is [MAP-CONSTRUCTOR-MAP-KEY-REFUSAL-1](map-constructor-map-key-refusal-1-card-2026-11-02.md), the same site; the scalar float form is [SCALAR-FLOAT-KEY-SIGNED-ZERO-NAN-1](scalar-float-key-signed-zero-nan-1-card-2026-11-02.md) (C-046).

Up: [map.md](map.md).
