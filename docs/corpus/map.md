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
