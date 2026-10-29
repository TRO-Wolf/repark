# Card ICE-HADOOP-METADATA-CLEANUP-1: the metadata cleanup properties are ignored on RePark's hadoop-type writes

**Date:** 2026-10-29. **Filed by:** the second-audit records round (`claude-haiku-5-5`), from the second independent audit of the format-v2 label, finding AUD-014 ([the report](../../../docs/audits/iceberg-v2-label-independent-reaudit-2026-10-29.md), section 3).

**Status:** open. Grade C (clerk). Not dispatched; no build is owed until the owner charters it.

**Why:** A table that sets `write.metadata.delete-after-commit.enabled` with `write.metadata.previous-versions-max` expects Java's cleanup of old metadata files. On RePark's hadoop-type writes the property is ignored, so every metadata version is kept, while Spark keeps only the versions the properties allow. The divergence leaves files that Spark removes, and it is a property silently not honoured, not a wrong row. No registry row mentions either property before this round.

**Site:** the hadoop-type metadata commit of RePark's writers over the fork's hadoop naming. The audit's probe located the behaviour and did not name the function; the charter names the commit path from the code.

**Fix:** The hadoop-type commit honours the delete-after-commit and previous-versions-max properties as Java does, so that after the same sequence of writes the metadata directory keeps the same versions on both engines. The unit adds one pin: the same table definition and the same five writes after the create, with the set of metadata file names compared with Spark's. It shares its fixture with the cleaned-up-version shape of ICE-HADOOP-GZIP-METADATA-1, so the two can be built in one pass.

**Measured (copied from the second audit's probe, 2026-10-29):** the same table definition on both engines, with the delete-after-commit property set to true and the previous-versions-max property set to one, then five inserts. RePark keeps the metadata files for versions one to six. Spark keeps versions five and six.

**Retires:** the card when RePark's hadoop-type writes keep the same metadata versions as Spark after the probe sequence, and the pin is green. The label ledger's AUD-014 row (`ice-v2-label-1` C-071) then closes by citation.

**Relation:** [ICE-HADOOP-GZIP-METADATA-1](ice-hadoop-gzip-metadata-1-card-2026-10-29.md), which shares the cleaned-up-version fixture on the read side; the parity registry's ICE-LOCAL-COMMIT-POINT-1 dated line of 2026-10-29, which carries this finding.
