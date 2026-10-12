# Card C-4-WRITE-ONLY-ROLE-1: a role with INSERT but not SELECT cannot write

**Date:** 2026-10-10. **Filed by:** Muse Spark (muse-spark-1.3-contributor), C-4 measure lane, from the orchestrator's brief. **Source:** the C-4 measure round (Frontier D13 ruling, 2026-10-10) and the ledger's §7 open item.

**Status:** filed, not scheduled. The ruling is a card, not a fix.

## Why

**The block.** Every Postgres write resolves its target with C-2's `discover`, which reads the table's column metadata and refuses a role without `SELECT` (`PermissionDenied { Select }`). A role with `INSERT` but not `SELECT` therefore never reaches the write core. Both doors pin this live: the write answers the missing `SELECT` privilege.

**Why it is a limitation, not a bug.** Spark's JDBC writer also reads table metadata before writing, so a write-only role fails there too. Lifting it here needs a second discovery mode that resolves a relation for writing without `SELECT`, which changes C-2's read surface for a role shape neither engine serves.

## The ask

None. The card records the limitation so no unit re-litigates it. If the owner ever wants write-only roles, the work is the second discovery mode of the ledger's §7, as its own unit with its own Spark grid.

## Gates

- no code gate: the card records a limitation and asks for no fix;
- any other gate: Not measured.

## Pointers

- The C-4 ledger's §7 open item (a write-only role cannot be resolved).
- The step-2 owner-question pins (a write-only role names the missing `SELECT` privilege, both doors).
