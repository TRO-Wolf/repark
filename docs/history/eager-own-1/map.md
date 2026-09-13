# map — docs/history/eager-own-1/

## Purpose
The EAGER-OWN-1 record (2026-09-13): the owner's source review of eager-materialization
retention, archived when the refcounted `CacheViewHandle` fix and its lifecycle tests landed.
Current state is [STATUS.md](../../../STATUS.md); the unit ledger — held in staging while review
findings may still reopen it — is
[task/ledgers/staging/eager-own-1-ledger.md](../../../task/ledgers/staging/eager-own-1-ledger.md);
the card stays at
[task/roadmap/mid-term/eager-own-1-card-2026-09-13.md](../../../task/roadmap/mid-term/eager-own-1-card-2026-09-13.md).

## Contents
- [eager-materialization-retention-review-2026-09-13.md](eager-materialization-retention-review-2026-09-13.md)
  — the owner's source review, verbatim: bare `eager()` calls each registered an unowned
  `__repark_cache_*` MemTable; the findings table, usage guidance, the three proposed
  corrections and the validation table the unit's pins followed. Closed 2026-09-13.

## Pointers
- Up: [../map.md](../map.md)
