# Card HADOOP-METADATA-DIR-UNLISTABLE-1: a metadata directory that cannot be listed, with no readable hint, is refused where Java may read version 0

**Date:** 2026-11-03. **Filed by:** the records clerk (`claude-haiku-5-5`) on the owner's ruling of 2026-11-03, from the Opus 5.5 verifier's finding F3 and the Muse verifier's probe P16 on HADOOP-DISCOVERY-RULE-1 (PR #1251; ledger clause C-013 of [its ledger](../../ledgers/completed/hadoop-discovery-rule-1-ledger.md)).

**Status:** open. Grade C (clerk). S3, refusal class: no wrong read was measured. Not dispatched; no build is owed until the owner charters it. Not a label blocker under the label gate, which reads a refusal as a card.

**Why:** With no readable hint, RePark lists `metadata/` to find the newest file. When the directory cannot be listed (the verifier's shape: execute-only, mode 0100, with `v0` through `v5` present), RePark surfaces the directory error. The verifiers read Java differently. The Opus source reading is that `findVersion` catches the listing failure and takes version 0, and `refresh` then reads `v0` and walks forward; so Java answers where a `v0` file exists and refuses where none does. The Muse run reads the same shape as a `RuntimeIOException` that refuses. Neither verifier ran Hadoop on this shape, so the disagreement stands.

**Site:** `crates/repark-core/src/iceberg_path.rs`, the listing used by `resolve_metadata_location`, and its error arm.

**Measured (copied from the verifiers' probes; not in the cells table):** Opus probe `listing_io_error_exec_only`: `Failed to read directory .../metadata`. Muse probes P16 and P16b: a permission error. Both refuse; no read changed on either verifier's run.

**Fix shape:** after a Hadoop run confirms Java's behaviour on this shape: (a) if Java reads `v0`, catch the listing failure to version 0 as Java does, pinned beside the hint pins; (b) if Java refuses, keep the refusal and carry its text difference as a refusal-text card.

**Retires:** the card when the Hadoop run settles the shape and (a) or (b) is ruled and pinned.

**Relation:** [HADOOP-DISCOVERY-RULE-1](hadoop-discovery-rule-1-card-2026-11-03.md), the unit whose listing this shape reaches; [HADOOP-HINT-UNICODE-DIGITS-1](hadoop-hint-unicode-digits-1-card-2026-11-03.md), the sibling lead of the same verification.
