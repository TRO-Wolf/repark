# Card IF-BINARY-NULL-COERCION-1: IF with a BINARY branch and a NULL branch fails RePark's coercion, where Spark answers

**Date:** 2026-11-02. **Filed by:** `claude-haiku-5-5` (records clerk) for the orchestrator, from the observations of the MAP-STRUCT-KEY-SUBSCRIPT-1 round-5 Opus verdict. The observations are not counted against that unit.

**Status:** filed; grade not set (the orchestrator grades it); not dispatched. Head equals base; outside the subscript.

**Why:** RePark's `if` coercion fails on a BINARY branch against a NULL branch. Spark 4.1.2 answers the statement; the verdict records the answer without copying its value.

**Measured (the verdict's observation):**

```text
SELECT IF(1 = 1, X'61', NULL)
  Spark 4.1.2: answers
  RePark:      refuses with "Expect to get struct but got Binary"
```

**Also measured in the same round (the `conf.set` whitespace note):** `conf.set('spark.sql.ansi.enabled', ' false ')`: Spark trims the value and accepts it, so ANSI is off; RePark raises `INVALID_CONF_VALUE.TYPE_MISMATCH` and keeps ANSI on (head equals base). RePark's builder accepts `0` as false, while its `conf.set('0')` raises; Spark's builder with `0` was not measured. This is a separate observation of the same round, recorded on this card.

**Retires:** the card when `IF(1 = 1, X'61', NULL)` answers as Spark does, pinned. The whitespace note retires when `' false '` is accepted as Spark accepts it, pinned.

**Relation:** [MAP-STRUCT-KEY-SUBSCRIPT-1](../../ledgers/staging/map-struct-key-subscript-1-ledger.md) (observations only). Up: [map.md](map.md).
