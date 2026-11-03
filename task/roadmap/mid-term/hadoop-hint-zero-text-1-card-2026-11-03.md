# Card HADOOP-HINT-ZERO-TEXT-1: a zero version hint refuses on the path read with RePark's own text, where Spark raises TABLE_OR_VIEW_NOT_FOUND

**Date:** 2026-11-03. **Filed by:** the records clerk (`claude-haiku-5-5`) on the owner's ruling of 2026-11-03, from the CORPUS-1 pilot (defect D5 of the [remaining-work estimate](../../../docs/corpus/remaining-work-estimate-2026-11-03.md)).

**Status:** open. Grade C (clerk). S3, refusal text: not a label blocker under the label gate. Not dispatched; no build is owed until the owner charters it.

**Why:** A `version-hint.text` that holds `0` makes both engines refuse a path read of a `type=hadoop` table. Java's rule (R4 of the [rule doc](../../../docs/corpus/hadoop-discovery-rule-2026-11-03.md)) treats version 0 on a first load as a table that does not exist, so Spark raises `TABLE_OR_VIEW_NOT_FOUND`. RePark refuses with its own message, which names `v0.metadata.json`. The class is the same, a refusal with nothing read and nothing written; the words differ.

**Site:** `crates/repark-core/src/iceberg_path.rs`, the message at line 195 (`version-hint.text names '{file_name}', which does not exist`).

**Fix shape:** The charter rules between two shapes. (a) Match Spark: a zero hint refuses with `TABLE_OR_VIEW_NOT_FOUND` on the path read, pinned beside the existing hint pins. (b) Keep RePark's text: register the difference as an accepted text divergence in the parity registry, with a pin and the reason.

**Measured (pilot cells, S3; count from the estimate's D5):** 84 cells, class REFUSE_TEXT. Spark: `[TABLE_OR_VIEW_NOT_FOUND] The table or view ... cannot be found`. RePark: `no Iceberg table found at '<table>': version-hint.text names 'v0.metadata.json', which does not exist`. Evidence: [cells](../../../docs/corpus/hadoop-discovery-cells-2026-11-03.jsonl).

**Retires:** when the zero-hint refusal follows the charter's ruling and is pinned on the path read.

**Relation:** [HADOOP-DISCOVERY-RULE-1](hadoop-discovery-rule-1-card-2026-11-03.md), whose R4 this card's text follows; [HADOOP-HINT-MISSING-VERSION-TEXT-1](hadoop-hint-missing-version-text-1-card-2026-11-03.md), the sibling refusal text for a hinted version with no file.
