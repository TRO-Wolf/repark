# Card HADOOP-HINT-CATALOG-STYLE-NAME-1: with no readable hint, the hadoop path read prefers a catalog-style metadata name numbered above the newest version

**Date:** 2026-11-01. **Filed by:** the third-audit records round (`claude-haiku-5-5`), from the targeted re-audit of the format-v2 label, finding AUD-017 ([the report](../../../docs/audits/iceberg-v2-label-targeted-reaudit-2026-11-01.md), section 5).

**Status:** folded into HADOOP-DISCOVERY-RULE-1 (owner 2026-11-03). The fix shape and the pins are that card's; this card stays as the record of the audit's probe. Not a label blocker (ledger clause C-081).

**Why:** On a `type=hadoop` table with no readable hint, the path read orders the metadata files by their leading number. A catalog-style `NNNNN-<uuid>.metadata.json` numbered above the newest `vN` wins, so the read returns an older state and a deleted row comes back. Spark ignores such names. No ordinary RePark or Spark flow the audit found creates the layout, which is why it is rated Low; the audit says it would rate Critical if one did. The registry's DF-LOAD sentence records that the listing accepts both name forms, without the consequence of mixing them.

**Site:** `crates/repark-core/src/iceberg_path.rs`, `metadata_file_order`, the `{leading}-{uuid}` arm.

**Fix:** with no readable hint, the path read considers only the `v<N>` names Java reads (the plain and gzip forms, as [ICE-HADOOP-GZIP-METADATA-1](ice-hadoop-gzip-metadata-1-card-2026-10-29.md) pins them), as Java's `HadoopTableOperations` does, so a catalog-style name never takes part in the ordering. The pins are the audit's two no-hint cases, with a catalog-style file numbered above the newest version, read against Spark's answer.

**Measured (the third audit, section 5; copied):**

```text
table v1..v3, rows after a DELETE: [2],[3]; version-hint.text removed
+ 00000-<uuid>.metadata.json (copy of v2)   path read: [[2],[3]]        (correct)
+ 00009-<uuid>.metadata.json (copy of v2)   path read: [[1],[2],[3]]    (the deleted row returns)
Spark-written table v1..v6.gz, no hint, plus 00099-<uuid>.metadata.json (copy of v1):
    RePark [], Spark [[3],[4]]
```

**Retires:** the card when a no-hint read ignores catalog-style names as Spark does, pinned on both cases. The label's AUD-017 row (ledger clause C-081) then closes by citation.

**Relation:** [ICE-HADOOP-HINT-WINDOW-1](ice-hadoop-hint-window-1-card-2026-10-22.md), the hint walk and the listing this read shares; [HADOOP-HINT-UNREADABLE-FALLBACK-1](hadoop-hint-unreadable-fallback-1-card-2026-11-01.md), the other Low hint card of the same audit.
