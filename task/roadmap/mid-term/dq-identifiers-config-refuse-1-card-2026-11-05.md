# Card DQ-IDENTIFIERS-CONFIG-REFUSE-1: `SET spark.sql.ansi.doubleQuotedIdentifiers=true` is accepted and not honoured, so DML reads a double-quoted column as a string literal

**Date:** 2026-11-05. **Filed by:** the records clerk (`claude-haiku-5-5`), from the format-v2 label re-audit ([report](../../../docs/audits/iceberg-v2-label-corpus-audit-2026-11-05.md), finding AUD-021).

**Status:** closed 2026-11-05 at `6cc7fd79` (PR #1266, squashed to main as `6a8e4c33`) after the two verifiers; lane 1 of 3. Severity S1, a blocker of the format-v2 label (ledger [ice-v2-label-1-ledger.md](../../ledgers/staging/ice-v2-label-1-ledger.md) C-098). The narrow second re-audit of AUD-021 has not run, so the label stays unsigned. Ledger [dq-identifiers-config-refuse-1-ledger.md](../../ledgers/completed/dq-identifiers-config-refuse-1-ledger.md); the open leads are card [DQ-IDENTIFIERS-CONFIG-RESIDUE-1](dq-identifiers-config-residue-1-card-2026-11-05.md).

**Why:** With the configuration on, Spark 4.1.2 reads `"s"` as the column `s`. RePark reads it as the string literal `'s'`. On a three-row probe table, `DELETE FROM t WHERE "s" = 's'` deletes every row where Spark deletes one; `UPDATE t SET id = 10 WHERE "s" = 'a'` changes nothing where Spark updates one row; `INSERT INTO t VALUES (4, "x")` stores `'x'` where Spark refuses `UNRESOLVED_COLUMN`. The names cells for this configuration touched no table data, which is why the 2026-11-03 names ruling filed them as a card. A DML probe writes, so the finding is a wrong write.

**Proposed fix:** refuse the `SET` of this configuration loudly as unsupported, with a message that names the configuration. Honouring the configuration stays with IDENT-DIALECT-2.

**Close:** the refusal lands with a pin on each DML head the audit measured (DELETE, UPDATE, INSERT with SELECT, INSERT with VALUES), each refusing or matching Spark on both doors, and then a targeted re-audit of AUD-021.
