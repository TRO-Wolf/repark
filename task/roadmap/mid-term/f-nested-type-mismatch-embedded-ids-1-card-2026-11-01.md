# Card F-NESTED-TYPE-MISMATCH-EMBEDDED-IDS-1: an incompatible nested type on a Parquet data file with embedded field ids reads a silent NULL struct (fork card)

**Date:** 2026-11-01. **Filed by:** `claude-haiku-5-5` for the orchestrator, from the RP-64 records round, from the fork ledger of F-IDLESS-STRUCT-CHILD-REMAP-1 (clause S-23).

**Status:** filed; grade B, P2; not dispatched. This is the owner's fork work; the fork is not edited from RePark.

**Why:** On a Parquet data file WITH embedded field ids, an incompatible nested type collapses the struct's validity. Table `s<d: string>`, file `s<d: int>` with field ids 1, 2 and 3 and values `[1, NULL]`: the struct's validity reads `[false, false]` with a NULL child, a silent NULL. The id-less route refuses the same shape loud with `DataInvalid` (`nested field ... cannot be read as the table type`). The refusal shipped in fork #379 is gated on missing field ids, so this shape keeps its old answer. Pre-existing at `14a93602`, confirmed by both round-2 verifiers of fork #379 (fork S-23).

**Reachability:** through RePark only on a table whose data file another writer produced with embedded field ids, and with a type the table schema does not promote.

**Site:** the fork's embedded-id read route, which the #379 type refusal does not reach because that refusal is gated on missing field ids.

**Fix (proposed):** the embedded-id route refuses the same incompatible nested type with `DataInvalid`, as the id-less route now does, so no silent NULL is returned for a type the table cannot read.

**Measured:** fork ledger S-23, the probe of both round-2 verifiers of fork #379 at `14a93602`.

**Retires:** the card when the embedded-id route refuses the same way, with fork S-23 closing by citation.

**Relation:** [F-IDLESS-STRUCT-CHILD-REMAP-1](f-idless-struct-child-remap-1-card-2026-10-31.md) (closed 2026-11-01 by fork #379 and RP-64; the id-less route's refusal is the model for this fix); [F-IDLESS-TOPLEVEL-POSITIONAL-1](f-idless-toplevel-positional-1-card-2026-11-01.md) (the other carded residue); RP-64 (the records are in its unit ledger, clause C-018).

Up: [map.md](map.md).
