# Card HADOOP-DISCOVERY-RULE-1: the hadoop path read follows Java's metadata discovery, fixed before the v2 label

**Date:** 2026-11-03. **Filed by:** the records clerk (`claude-haiku-5-5`) on the owner's rulings of 2026-11-03 ("proceed with your recommendations"), from the CORPUS-1 pilot.

**Status:** dispatched 2026-11-03, Opus 5.5 lane. Owner ruling 2026-11-03: D1 and D3 are fixed before the v2 label as one unit; D2 rides along as the same mechanism; the codec-by-name (R1) is not ported.

**Clause:** the cells table, the rows of groups D1, D3 and D2 of the [remaining-work estimate](../../../docs/corpus/remaining-work-estimate-2026-11-03.md), re-measured on the path read against Spark. The append rows (`path_insert`) of those hints stay under the D7 exclusion, not this unit.

**Why:** On a `type=hadoop` table read by path, RePark's version-hint handling departs from Java's `HadoopTableOperations` in three places. A catalog-style `NNNNN-<uuid>.metadata.json` wins the listing when no hint is readable, and a deleted row comes back (D1, S2). A negative hint is answered where Java refuses (D3, S2). An unreadable or directory-shaped hint refuses where Java lists (D2, S3). The ported rule (R2 to R4 of the [rule doc](../../../docs/corpus/hadoop-discovery-rule-2026-11-03.md)) predicts Spark's answer on 1910 of 1910 path-read fixtures.

**Fix shape:** Port R2 to R4 into RePark's path read in `crates/repark-core/src/iceberg_path.rs` (`read_version_hint`, `java_int_version`, `resolve_metadata_location`, `metadata_file_order`).

- R2: the hint is read as Java reads it. Any read failure, an empty file, a non-numeric or out-of-range value falls to R3 when `metadata/` exists. A parsed value is taken as it is, a negative one included, and R4 refuses it.
- R3: the listing considers only the `v<N>` names (`v([^\.]*)\..*`), so a catalog-style name never takes part.
- R4: a hinted version with no file refuses with the missing-version error. Otherwise the walk goes forward to the first missing version.
- R1's codec-by-name is not ported. A file whose name lies about its payload (D4) stays an exclusion of the v2 label.

**Measured (pilot, 2026-11-03; counts and classes from the estimate's defect table; answers from the cells file):**

- D1, 84 cells, ANSWER_DIFFERS, S2: a catalog-style file present, no readable hint. Spark `rows [3, 4]`; RePark `rows [1, 2, 3]`, so the deleted row returns.
- D3, 126 cells, REPARK_ACTS_SPARK_REFUSES_NOOP, S2: hint `-1`. Spark `ValidationException: Metadata file for version -1 is missing`; RePark `rows [3, 4]`. 21 of the 126 answer a deleted row (the D1 mechanism).
- D2, 252 cells (168 REPARK_REFUSES_SPARK_ACTS, 84 REFUSE_TEXT), S3: a hint that is unreadable (mode 000) or a directory. Spark lists and answers; RePark `DataInvalid => Failed to read file .../version-hint.text`.

Rule references (line numbers in `HadoopTableOperations.java` at `apache-iceberg-1.11.0`, as the rule doc cites them): R2 `findVersion` lines 313 to 350; R3 the stem parse lines 274 to 285 and `VERSION_PATTERN` line 59; R4 `refresh` lines 102 to 127. Evidence: [cells](../../../docs/corpus/hadoop-discovery-cells-2026-11-03.jsonl).

**Retires:** when the D1, D3 and D2 rows of the cells table read as Spark does, the rule check passes on the path read, and the Rust pins cover the three shapes. The cards [HADOOP-HINT-NEGATIVE-1](hadoop-hint-negative-1-card-2026-10-29.md), [HADOOP-HINT-CATALOG-STYLE-NAME-1](hadoop-hint-catalog-style-name-1-card-2026-11-01.md) and [HADOOP-HINT-UNREADABLE-FALLBACK-1](hadoop-hint-unreadable-fallback-1-card-2026-11-01.md) then close by citation.

**Relation:** [ICE-HADOOP-HINT-WINDOW-1](ice-hadoop-hint-window-1-card-2026-10-22.md), the hint walk this rule extends; [ICE-HADOOP-GZIP-METADATA-1](ice-hadoop-gzip-metadata-1-card-2026-10-29.md), the name forms; the [v2 label draft](../../../docs/corpus/v2-label-supported-behaviour-draft-2026-11-03.md), family 1 and its exclusions D4, D7 and D8.
