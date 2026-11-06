# Card DQ-IDENTIFIERS-CONFIG-RESIDUE-1: ten open leads from the double-quoted-identifiers refusal and its two verifiers

**Date:** 2026-11-05. **Filed by:** the records clerk (`claude-haiku-5-5`), from the closure of [DQ-IDENTIFIERS-CONFIG-REFUSE-1](dq-identifiers-config-refuse-1-card-2026-11-05.md) (ledger [dq-identifiers-config-refuse-1-ledger.md](../../ledgers/completed/dq-identifiers-config-refuse-1-ledger.md), leads C-009 to C-018).

**Status:** filed 2026-11-05, severity S3, not dispatched; awaiting the owner's charter. Neither verifier graded any lead as blocking. Lead L4 changes an answer after a `RESET` on a path that main already had; its grade is the owner's.

**Why:** The two verifiers of the refusal (both PASS, zero blocking findings) and the builder's questions left ten items open. Each is a loud refusal or a message difference with no wrong write, except L4. They are gathered here as cells, each with its closing question.

## Cells

- **L1, `spark.conf.get` with no default (C-009).** RePark raises "not set" where Spark answers `false`. Close: add the key to the SQL defaults with its pin (this moves `getAll` and the symbol-hash pin), or decline.
- **L2, the native config map (C-010).** `PyReparkSession.native(config={key: "true"})` is accepted, because that door installs no Spark extension. Close: refuse it with a pin, or record why it stays accepted.
- **L3, the fresh-build prefix (C-011).** The fresh-build refusal keeps DataFusion's `This feature is not implemented:` prefix; the runtime doors do not. Close: one message shape, or accept the prefix.
- **L4, the reuse fold (C-012).** A builder that folds `spark.sql.ansi.enabled=false` and then refuses the key leaves the first key folded: the message says the configuration is unchanged while the store holds `false`, and after `RESET` the engine answers `1/0` as NULL. The fold mechanics are pre-existing. Close: fold atomically or drop the message clause; grade the `RESET` answer on its own.
- **L5, `SET -v` (C-013).** The listing omits the key and other documented keys that Spark lists with their defaults; identical on main. Close: list them as `SET <key>` answers.
- **L6, comments in set values (C-014).** A trailing comment is stripped from a SET value, so `=true -- c` refuses where Spark answers TYPE_MISMATCH, and `=false -- c` is accepted where Spark refuses. Pre-existing. Close: match Spark's value text, or decide the stripping is kept.
- **L7, multi-statement SET text (C-015).** Both sides refuse with the configuration unchanged; the text differs (`PARSE_SYNTAX_ERROR` against `INVALID_SET_SYNTAX`). Close: match the text, or record it as the same class.
- **L8, the AUD-021 cell test (C-016).** The cell test compares with RePark's own baseline, not with Spark's configuration-off answer for the eleven facade heads. Close: record Spark's answers in the cells file.
- **L9, `df.where` integer cast (C-017).** A loud error whose class differs from Spark's; no wrong answer. Close: pin the class in an error-contract unit, or record it.
- **L10, MERGE with a qualified double-quoted reference (C-018).** Both sides refuse with the table unchanged; the error class differs. Close: as L9.

**Close:** each cell closes on its own pin or a dated ruling. The card leaves when the owner charters it or declines it, and each cell is recorded under its C-number in the unit ledger.
