# Card ICE-CREATE-MAP-KEY-FIELD-IDS-1: CREATE numbers a map key's struct children before the value id, so a map-key refusal text carries different field ids from Spark's

**Date:** 2026-11-04. **Filed by:** the records clerk (`claude-haiku-5-5`) from ledger lead L3 (clause C-011) of [MAP-KEY-ALTER-1](../../ledgers/completed/map-key-alter-1-ledger.md), from the PR #1256 body ("Not changed").

**Status:** filed, not dispatched. Verifier class S3 (refusal text only).

**Why:** RePark's CREATE assigns the field ids of a map key's struct children before the id of the value. The map-key refusal texts name the key struct with its ids (`Cannot alter map keys: map<struct<...>, ...>`), so the ids in the text differ from Spark's. The MAP-KEY-ALTER-1 table compares refusal texts with the ids masked, so the rows match Spark and the difference does not show in the pin. Nothing is written by the refused statement.

**Fix shape:** align the CREATE id numbering of a map key's struct children with Java's order, then pin the unmasked text of one refusal.

**Measured:** only the masked comparison (the table). The unmasked text differs (PR #1256 body).

**Retires:** when a CREATE with a map key assigns Spark's ids and one unmasked refusal text is pinned.

**Relation:** [MAP-KEY-ALTER-1](map-key-alter-1-card-2026-11-04.md) (lead L3, clause C-011).

Up: [map.md](map.md).
