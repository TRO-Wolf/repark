# Card ANSI-SUBSCRIPT-AND-AMBIGUOUS-FIELD-1: under ANSI, an out-of-range array subscript and a duplicate-field lookup write rows where Spark raises

**Date:** 2026-11-05. **Filed by:** the records clerk (`claude-haiku-5-5`), from the format-v2 label re-audit ([report](../../../docs/audits/iceberg-v2-label-corpus-audit-2026-11-05.md), findings AUD-022 and AUD-023).

**Status:** dispatched 2026-11-05 (owner), Opus 5.5 lanes in that order, lane 2 of 3. AUD-022 is S1 and AUD-023 is S2; both block the format-v2 label (ledger [ice-v2-label-1-ledger.md](../../ledgers/staging/ice-v2-label-1-ledger.md) C-099); it closes on its merge and the narrow second re-audit.

**Why:** Owner ruling 1 (2026-11-05) makes the expression answers cards for SELECT: an out-of-range subscript answers null where Spark raises `INVALID_ARRAY_INDEX` under ANSI (D10), and a duplicated field answers where Spark raises `AMBIGUOUS_REFERENCE_TO_FIELDS` (D13). Inside a statement that writes, the same expressions write. `DELETE FROM t WHERE l[10] IS NULL` deletes every row, where Spark raises and changes nothing. `UPDATE t SET id = id + 100 WHERE l[5] IS NULL` rewrites every row. `INSERT INTO t SELECT id + 10, array(l[7]), m FROM t` stores `[null]` arrays. `INSERT INTO t SELECT 8, named_struct('a', named_struct('a', 1, 'a', 2).a, 'b', 'd')` stores a row with `a = 1`, where Spark refuses. The ruling's limit does not reach DML, so these are blockers.

**Proposed fix:** one unit in the facade's expression lowering. Under ANSI, an out-of-range element raises Spark's condition, and a duplicate-field reference raises `AMBIGUOUS_REFERENCE_TO_FIELDS`, on every route that can reach a write. The SELECT answers stay cards under ruling 1, see [NESTED-EXPRESSION-ANSWERS-1](nested-expression-answers-1-card-2026-11-05.md).

**Close:** pins on DELETE, UPDATE, INSERT with SELECT and the struct-source INSERT, each matching Spark's refusal and unchanged table, then a targeted re-audit of AUD-022 and AUD-023.
