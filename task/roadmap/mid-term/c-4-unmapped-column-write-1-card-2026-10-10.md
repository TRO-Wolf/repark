# Card C-4-UNMAPPED-COLUMN-WRITE-1: resolve past unmapped columns, refuse only when one is named

**Date:** 2026-10-10. **Filed by:** Muse Spark (muse-spark-1.3-contributor), C-4 measure lane, from the orchestrator's brief. **Source:** the C-4 measure round (Frontier D13 ruling, 2026-10-10) and the ledger's §7 open item.

**Status:** filed, not scheduled. The ruling is a follow-up card, not in C-4.

## Why

**The block.** A relation with an unmapped column (`time`, arrays, ranges) does not resolve at all, even when the write does not name that column: C-2's `discover` refuses the whole relation under the column's registry row. Both doors pin this live: the write names the row even for an unnamed column.

**The lean.** Resolve the relation with its unmapped columns marked unavailable, and refuse only when one is named: a write that omits them stores, a write that names one refuses with the column's registry row. Reads keep today's whole-relation refusal unless the follow-up unit rules otherwise.

## The ask

Take the lean as its own follow-up unit: the marked-unavailable resolution, the write that omits an unmapped column storing, the write that names one refusing, and the read door's behavior ruled, each with live pins on both doors.

## Gates

- the follow-up unit's own gate: omitted unmapped columns store, named ones refuse, reads ruled;
- any other gate: Not measured.

## Pointers

- The C-4 ledger's §7 open item (a relation with an unmapped column does not resolve).
- The step-2 owner-question pins (an unmapped column names its row even when unnamed, both doors).
