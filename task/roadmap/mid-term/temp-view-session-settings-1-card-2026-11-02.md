# Card TEMP-VIEW-SESSION-SETTINGS-1: a temp view is analysed under the SQL settings of the session that reads it, where Spark keeps the creating session's

**Date:** 2026-11-02. **Filed by:** `claude-haiku-5-5` (records clerk) for the orchestrator, from the observations of the MAP-STRUCT-KEY-SUBSCRIPT-1 round-5 Opus verdict. The observations are not counted against that unit.

**Status:** filed; grade not set (the orchestrator grades it); not dispatched. Head equals base for the cells below.

**Why:** Spark stores the creating session's SQL configs with a temp view. RePark re-analyses the view under the setting of the session that reads it. The verdict finds that the subscript follows the door's other ANSI-gated rules here, so the gap belongs to the views. The card VIEW-ANSI-CAPTURE-1 names the same re-analysis for `sequence` and CAST; the cells below add a struct-key subscript and the arithmetic cells.

**Measured (Spark 4.1.2 against RePark on the head, the verdict's observations):**

```text
created under ANSI on, read under ANSI off:
  CREATE OR REPLACE TEMP VIEW v_on AS SELECT id, m[named_struct('j', 1, 'k', 2)] AS x FROM c
                                                       Spark [[1, 7], [2, None]]    RePark refuses (renamed struct key)
created under ANSI off, read under ANSI on:
  SELECT 1/0                                           Spark NULL                   RePark DIVIDE_BY_ZERO
  SELECT CAST(2147483647 AS INT) + 1                   Spark -2147483648            RePark ARITHMETIC_OVERFLOW
created under ANSI on, read under ANSI off:
  SELECT 1/0                                           Spark DIVIDE_BY_ZERO         RePark NULL
```

**Fix (for the orchestrator to grade):** analyse a temp view under the SQL settings it was created under, as Spark does.

**Retires:** the card when the view cells above answer as Spark does, pinned.

**Relation:** [VIEW-ANSI-CAPTURE-1](view-ansi-capture-1-card-2026-10-01.md) (the same re-analysis for `sequence` and CAST; whether the two are one gap is for the orchestrator to judge); [MAP-STRUCT-KEY-SUBSCRIPT-1](../../ledgers/staging/map-struct-key-subscript-1-ledger.md) (observations only). Up: [map.md](map.md).
