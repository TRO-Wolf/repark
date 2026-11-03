# docs/corpus — CORPUS-1 reference corpus

The reference inventory for the five finding families of the v2-label audits, the measured pilot
for Hadoop metadata discovery, and the remaining-work estimate. Upstream sources are Apache Iceberg
`apache-iceberg-1.11.0` and Apache Spark `v4.1.2`; each file names its pinned paths and lines.
A dated file is not edited after filing; a later measurement is a new file.

| File | What it is |
|---|---|
| [discovery-inventory-2026-11-03.md](discovery-inventory-2026-11-03.md) | Family 1, Hadoop metadata discovery: the upstream tests, their RePark coverage and gaps, the axes. |
| [names-inventory-2026-11-03.md](names-inventory-2026-11-03.md) | Family 2, name resolution at the SQL door, beside the AUD-015 table. |
| [nested-inventory-2026-11-03.md](nested-inventory-2026-11-03.md) | Family 3, nested-type reader and expression shapes. |
| [deletes-inventory-2026-11-03.md](deletes-inventory-2026-11-03.md) | Family 4, delete files by format and evolution (Avro and ORC delete files excluded). |
| [commit-inventory-2026-11-03.md](commit-inventory-2026-11-03.md) | Family 5, a commit that succeeds while the door reports failure. |
| [hadoop-discovery-rule-2026-11-03.md](hadoop-discovery-rule-2026-11-03.md) | The pilot's rule: Java's hint read, listing fallback, name forms and cleanup, with line citations. |
| [hadoop-discovery-cells-2026-11-03.jsonl](hadoop-discovery-cells-2026-11-03.jsonl) | The pilot's measured cells in the AUD-015 table schema, Spark 4.1.2 against RePark. |
| [names-rule-2026-11-03.md](names-rule-2026-11-03.md) | Family 2's rule: temp-view precedence, `IDENTIFIER()`, the double-quoted config, listing heads, with citations and the cell summary. |
| [names-cells-2026-11-03.jsonl](names-cells-2026-11-03.jsonl) | Family 2's measured cells from the Spark golden files, facade and native door, Spark 4.1.2 against RePark. |
| [remaining-work-estimate-2026-11-03.md](remaining-work-estimate-2026-11-03.md) | Cells to build per family, hours from the pilot, defects found, a date range. |
| [v2-label-supported-behaviour-draft-2026-11-03.md](v2-label-supported-behaviour-draft-2026-11-03.md) | Draft of the v2 label's supported behaviours, evidence and exclusions. |
