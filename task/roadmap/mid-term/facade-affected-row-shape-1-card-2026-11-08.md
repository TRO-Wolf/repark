# Card FACADE-AFFECTED-ROW-SHAPE-1: the facade's affected-row column depends on the route

**Date:** 2026-11-08. **Filed by:** the records clerk (`claude-haiku-5-5`), from the Muse MAX verdict of EMPTY-DML-SHARED-GUARD-1 (ledger [empty-dml-shared-guard-1-ledger.md](../../ledgers/completed/empty-dml-shared-guard-1-ledger.md), clause C-016, Muse MAX F1).

**Status:** filed 2026-11-08, severity S3, not dispatched; awaiting the owner's charter. The Muse MAX verdict graded it as not blocking, with no action required. It predates the unit.

**Why:** A guarded empty DML on the facade answers `[{count: 0}]`, the one-row `Values` node in the DML's own output schema. A provider-path write answers `[{}]`, one row with no columns, as a selective DELETE does. The row count is 1 on every route. The shape follows the route: `false` and `NULL` fold to an EmptyRelation and reach the guard, `1 = 0` and `prim_col = NULL` take the facade's plain route, and `id IS NULL` on a required column takes the metadata-delete door. The wheel table test reads `count` only where that column exists, so it does not pin the shape.

**Proposed fix:** record the route-dependent shape as the facade's contract with a pin, or align the guarded answer with the provider-path one, which is a shape change with its own pin and release note.

**Close:** the shape is pinned or declined.
