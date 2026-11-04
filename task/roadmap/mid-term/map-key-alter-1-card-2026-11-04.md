# Card MAP-KEY-ALTER-1: every ALTER and schema-evolution union that would change a field inside a map key refuses as Spark does

**Date:** 2026-11-04. **Filed by:** the records clerk (`claude-haiku-5-5`) on the owner's ruling of 2026-11-03 (D1 and D16 of the nested rule doc fixed before the v2 label as one unit), from the CORPUS-3-NESTED pilot.

**Status:** closed 2026-11-04 at `a6233fea` (PR #1256, squashed to main as `3dc43ce1`) after two verifier rounds (Opus 5.5, and `muse-spark-1.3-contributor`). Ledger: [map-key-alter-1-ledger.md](../../ledgers/completed/map-key-alter-1-ledger.md). Clauses C-001 to C-008 PROVEN; C-009 to C-018 OPEN leads. The leads: L1 (C-009) card [DF-APPEND-MAP-KEY-UNION-1](df-append-map-key-union-1-card-2026-11-04.md); L3 (C-011) card [ICE-CREATE-MAP-KEY-FIELD-IDS-1](ice-create-map-key-field-ids-1-card-2026-11-04.md); L2 (C-010) no card; L4 to L10 (C-012 to C-018) card [MAP-KEY-REFUSAL-TEXTS-1](map-key-refusal-texts-1-card-2026-11-04.md).

**Clause:** the `A-ddl` rows of the [nested cells table](../../../docs/corpus/nested-cells-2026-11-03.jsonl) for the map-key groups (D1 and D16 of the [nested rule doc](../../../docs/corpus/nested-rule-2026-11-03.md)), the schema-evolution callers as they enumerate, and the controls unmoved.

**Why:** On both doors an ALTER that changed a field inside a map key was applied where Spark refuses with `Cannot alter map keys` (D1: 28 cells, 5 DDL and 23 Spark reads of RePark's table). A move of a direct key child was reordered where Spark's ALTER succeeds and leaves the key struct unchanged (D16: 9 cells). A schema-evolution union could add or widen a field inside a key and commit it.

**Fix shape:** one shared rule in `crates/repark-iceberg/src/write/map_key_change.rs` (`map_key_steps_refusal`, `column_path_changes_map_key_refusal`, `union_map_key_refusal`), called by both DDL doors before the change applies and by every schema-evolution caller before the write. A move of a direct key child is a no-op with nothing committed; a move deeper under a key, or after the field itself, refuses.

**Measured:** the Spark door's mapkey `A-ddl` rows match Spark 24 of 33 before and 33 of 33 after; the post-DDL schema equals Spark's schema1 on 8 of 8 fixtures (3 of 8 before). The native door's ten mapkey DDL rows act or refuse as Spark does, with text equal on nine. The three reproduced union commits (BY NAME deep add, BY NAME deep widen, MERGE deep add) refuse with Java's text and no write.

**Retires:** closed by citation: the ledger's clauses C-001 to C-008 (PROVEN) and the pins in [map-key-alter-1-pins.md](../../ledgers/completed/map-key-alter-1-pins.md).

**Relation:** [F-MAP-KEY-DROP-ALTER-1](f-map-key-drop-alter-1-card-2026-10-31.md) (the fork's own drop, still open on the fork side); the [v2 label draft](../../../docs/corpus/v2-label-supported-behaviour-draft-2026-11-03.md), family 3.

Up: [map.md](map.md).
