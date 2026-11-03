# Card ICE-HADOOP-GZIP-CODEC-PROPERTY-1: a RePark write under write.metadata.compression-codec=gzip keeps plain metadata file names, where Java writes the gzip name

**Date:** 2026-11-03. **Filed by:** the records clerk (`claude-haiku-5-5`) on the owner's ruling of 2026-11-03, from the CORPUS-1 pilot (a seed observation of the [remaining-work estimate](../../../docs/corpus/remaining-work-estimate-2026-11-03.md)).

**Status:** open. Grade C (clerk). S3: Java reads the plain names, so the read stays equal. Not a label blocker. Not dispatched; no build is owed until the owner charters it.

**Why:** Under `write.metadata.compression-codec=gzip`, RePark's writer keeps the new version's file as plain `v<N>.metadata.json`. Java's writer (R5 of the [rule doc](../../../docs/corpus/hadoop-discovery-rule-2026-11-03.md)) writes `v<N>.gz.metadata.json` under the same property. Java's reader probes the plain name first (R1), so Spark reads the RePark table and the rows stay equal. The difference is the file name on disk, not the rows read.

**Measured:** not a cell. A seed observation of the pilot, recorded in the estimate's note on seed observations; it is not in the cells table ([cells](../../../docs/corpus/hadoop-discovery-cells-2026-11-03.jsonl)).

**Fix shape:** The charter rules between two shapes. (a) RePark writes Java's gzip name under the gzip codec, with the reads pinned on both engines. (b) The plain name stays, and the difference is registered as an accepted divergence with a pin on the Spark read.

**Retires:** when the name on disk follows the charter's ruling, pinned.

**Relation:** [ICE-HADOOP-GZIP-METADATA-1](ice-hadoop-gzip-metadata-1-card-2026-10-29.md), the merged read side of the gzip names; [HADOOP-DISCOVERY-RULE-1](hadoop-discovery-rule-1-card-2026-11-03.md), which does not port R1's codec-by-name, so the writer's name is a separate question.
