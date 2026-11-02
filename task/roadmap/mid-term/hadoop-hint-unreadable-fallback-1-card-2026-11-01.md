# Card HADOOP-HINT-UNREADABLE-FALLBACK-1: an unreadable version-hint.text makes the hadoop path read refuse, where Spark lists the directory and answers

**Date:** 2026-11-01. **Filed by:** the third-audit records round (`claude-haiku-5-5`), from the targeted re-audit of the format-v2 label, finding AUD-018 ([the report](../../../docs/audits/iceberg-v2-label-targeted-reaudit-2026-11-01.md), section 5).

**Status:** open. Grade C (clerk). P3. Not dispatched; no build is owed until the owner charters it. Not a label blocker (ledger clause C-082).

**Why:** When `version-hint.text` exists but cannot be read (the audit's case is mode 000, permission denied), `resolve_metadata_location` checks that the file exists and then propagates the read error, a `DataInvalid` that names the hint file. Spark's `findVersion` catches that failure and lists the directory, and Spark answers the newest state where RePark refuses. The precondition is an operational fault, not a state of the table, so the refusal is loud and nothing on disk is damaged. An unparsable or out-of-range hint already falls back to the listing ([ICE-HADOOP-HINT-WINDOW-1](ice-hadoop-hint-window-1-card-2026-10-22.md)); an unreadable one does not.

**Site:** `resolve_metadata_location` in `crates/repark-core/src/iceberg_path.rs`: the `exists` check, then the `read_version_hint` error is propagated.

**Fix:** an unreadable hint falls back to the listing as an unparsable hint does, as Java does, so the read answers the newest state. The pin is the audit's case: the mixed-codec table of the hint probe, hint `2`, file mode 000, read against Spark's answer.

**Measured (the third audit, section 5; copied):**

```text
repark: PySparkException: DataInvalid => Failed to read file …/version-hint.text: Permission denied (os error 13)
spark : [[3],[4]]
```

**Retires:** the card when an unreadable hint answers as Spark does, pinned. The label's AUD-018 row (ledger clause C-082) then closes by citation.

**Relation:** [ICE-HADOOP-HINT-WINDOW-1](ice-hadoop-hint-window-1-card-2026-10-22.md), whose fallback to the listing covers unparsable and out-of-range hints; [HADOOP-HINT-CATALOG-STYLE-NAME-1](hadoop-hint-catalog-style-name-1-card-2026-11-01.md), the other Low hint card of the same audit.
