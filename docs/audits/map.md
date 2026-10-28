# docs/audits — independent audit reports

Standalone copies of audit reports filed by an independent reviewer and never edited after filing:
a later audit is a new file. Each report's findings are reproduced by the orchestrator before a unit
is dispatched on them; the report is the record of what was found, and the registry, the ledgers and
the roadmap cards own what is fixed and what is declared.

| File | What it is |
|---|---|
| [iceberg-v2-label-independent-audit-2026-10-28.md](iceberg-v2-label-independent-audit-2026-10-28.md) | The independent audit of the format-v2 PRODUCTION label, run on 2026-10-28 against main `321c6095` and the fork pin `ea0b7741`: five reproduced findings (AUD-001 Critical, a stale `type=hadoop` version hint returns a deleted row; AUD-002 to AUD-004 Medium, CTAS outcome reporting, Avro and ORC delete files the reader cannot consume, a dropped equality-delete key; AUD-005 Low, the cross-read gate accepts a connection failure), the RustSec advisory assessment, the gate dimensions for the format-v3 label, and the probe sources. The label is withdrawn pending ICE-HADOOP-HINT-WINDOW-1 on AUD-001. |
