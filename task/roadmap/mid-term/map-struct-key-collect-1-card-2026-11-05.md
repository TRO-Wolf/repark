# Card MAP-STRUCT-KEY-COLLECT-1: a map column with struct keys cannot be collected into Python on either door

**Date:** 2026-11-05. **Filed by:** the records clerk (`claude-haiku-5-5`), from the format-v2 label re-audit ([report](../../../docs/audits/iceberg-v2-label-corpus-audit-2026-11-05.md), finding AUD-027).

**Status:** filed, awaiting the owner's decision; severity S3 (a loud error); does not block the format-v2 label. Ledger [ice-v2-label-1-ledger.md](../../ledgers/staging/ice-v2-label-1-ledger.md) C-103.

**Why:** `SELECT *` over a map whose keys are structs raises `TypeError: unhashable type: 'dict'` on the Python side on both doors, where Spark answers. The subscript itself answers as Spark does, so [MAP-STRUCT-KEY-SUBSCRIPT-1](map-struct-key-subscript-1-card-2026-11-01.md) holds. These columns (the `mapkey` shapes of the nested corpus) are outside the 280-cell full-row claim of the label.

**Proposed fix:** choose the Python collection form for a struct map key (a hashable row form that carries the same values as Spark's collected answer), then pin it on both doors.

**Close:** a pin that collects a struct-keyed map on both doors to the shape Spark collects.
