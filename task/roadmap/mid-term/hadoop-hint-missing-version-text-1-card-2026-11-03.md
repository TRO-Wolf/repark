# Card HADOOP-HINT-MISSING-VERSION-TEXT-1: a hinted version with no metadata file refuses with RePark's own text, where Java's text names the missing version

**Date:** 2026-11-03. **Filed by:** the records clerk (`claude-haiku-5-5`) on the owner's ruling of 2026-11-03, from the CORPUS-1 pilot (defect D6 of the [remaining-work estimate](../../../docs/corpus/remaining-work-estimate-2026-11-03.md)).

**Status:** open. Grade C (clerk). S3, refusal text: not a label blocker under the label gate. Not dispatched; no build is owed until the owner charters it.

**Why:** A `version-hint.text` that names a version with no metadata file, for example a hint past the newest file, makes both engines refuse a path read of a `type=hadoop` table. Java's `refresh` (R4 of the [rule doc](../../../docs/corpus/hadoop-discovery-rule-2026-11-03.md)) raises `ValidationException("Metadata file for version %d is missing under %s")`, which names the version. RePark refuses with its own message, which names the hinted file `v<N>.metadata.json`. The class is the same; the words differ.

**Site:** `crates/repark-core/src/iceberg_path.rs`, the same message at line 195 as HADOOP-HINT-ZERO-TEXT-1.

**Fix shape:** The same two shapes as HADOOP-HINT-ZERO-TEXT-1. (a) Match Java's text for a missing hinted version, naming the version and the table root, pinned. (b) Register the text difference as an accepted divergence with a pin and the reason.

**Measured (pilot cells, S3; count from the estimate's D6):** 148 cells, class REFUSE_TEXT. Spark: `ValidationException: Metadata file for version 10 is missing under <root-path>` (a hint of 10 past the newest file). RePark: `no Iceberg table found at '<table>': version-hint.text names 'v10.metadata.json', which does not exist`. Evidence: [cells](../../../docs/corpus/hadoop-discovery-cells-2026-11-03.jsonl).

**Retires:** when the missing-version refusal follows the charter's ruling and is pinned on the path read.

**Relation:** [HADOOP-DISCOVERY-RULE-1](hadoop-discovery-rule-1-card-2026-11-03.md), R4's second bullet; [HADOOP-HINT-ZERO-TEXT-1](hadoop-hint-zero-text-1-card-2026-11-03.md), the sibling text card for a zero hint.
