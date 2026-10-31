# Card F-IDLESS-STRUCT-CHILD-REMAP-1: an id-less Parquet data file with an absent struct child reads a dropped child's value, or a batch of the wrong type (fork card)

**Date:** 2026-10-31. **Filed by:** `claude-haiku-5-5` for the orchestrator, from the RP-63 records round, from the fork ledger of F-STRUCT-EMPTY-CHILD-PANIC-1 (clauses S-11 and S-16) and the orchestrator's probe.

**Status:** filed, not dispatched. Grade B, P2. This is the owner's fork work; RePark takes it by an ordinary pin bump afterwards. The orchestrator holds the probe (two failing Rust tests) and attaches it when the unit is chartered; no file path is named here.

**Why:** On a Parquet data file without embedded field ids (the fallback-id path, with or without a name mapping), a struct whose task-schema children are all absent from the file reads wrong values. The fork's name mapping and fallback-id stamping cover top-level fields only, so a nested struct gets no ids. Two shapes are wrong, both before RP-63:

- (i) a two-leaf file `s<d, e>` with rows `[{d: 1, e: 2}, NULL]`; then add `s.k` and drop `s.d` and `s.e`. The full scan reads `s` validity `[true, false]` and `k = [1, NULL]`: the dropped child's value reads under the new child `k`, which was added after the file was written and must be NULL on every row.
- (ii) a one-leaf file `s<d>`; then add `s.k` and drop `s.d`. The scan returns a record batch whose column is `Struct<d>` under a schema field `Struct<k>`, so `concat_batches` fails with `column types must match schema types`.

RePark writes field ids into every Parquet data file it produces, so these shapes reach RePark only on tables whose data files another writer produced without ids (a registered or added external file).

**Site:** the fork's `nested_projection::build_struct`, whose id-less `PlanNode::Cast` path reads the whole top-level group through `unmapped_group_leaf_indices`; and the top-level-only stamping of `apply_name_mapping_to_arrow_schema` and `add_fallback_field_ids_to_arrow_schema`.

**Fix (proposed):** the id-less struct path projects each child the way the id path does, a child with no leaf in the file reads NULL per row, the struct's validity comes from the file per row (the S-15 rule), and the fallback ids reach nested structs as well as top-level fields.

**Measured (fork ledger S-16, reproduced by the orchestrator at `935cc55c`, `a47b9e01`, `90114dda` and the merge `14a93602`):** shape (i) and shape (ii) above, both with the batch type and values stated. The unit's pins are both shapes end to end on a file written without ids, with and without a name mapping, reading NULL per row with the batch type equal to the schema type.

**Retires:** the card when both shapes read NULL per row with the batch type equal to the schema type, with the pins green; the fork ledger's S-11 and S-16 then close by citation.

**Relation:** [F-STRUCT-EMPTY-CHILD-PANIC-1](f-struct-empty-child-panic-1-card-2026-10-29.md) (the parent fork card; its clauses S-11 and S-16); RP-63 (the pins of `python/repark/tests/iceberg/test_ice_struct_empty_child_1.py` cover embedded-id files only); the registry line ICE-NESTED-DDL-1 dated 2026-10-31.

Up: [map.md](map.md).
