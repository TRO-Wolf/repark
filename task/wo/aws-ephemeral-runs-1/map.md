# task/wo/aws-ephemeral-runs-1/ — the evidence for AWS-EPHEMERAL-RUNS-1

AWS-EPHEMERAL-RUNS-1 (owner rulings 2026-10-24): every aws-acceptance run creates its own tables under a run
prefix, cross-reads them and removes them. The card is
[../../roadmap/mid-term/aws-ephemeral-runs-1-card-2026-10-24.md](../../roadmap/mid-term/aws-ephemeral-runs-1-card-2026-10-24.md).

- [step0-census.md](step0-census.md) — step 0 (2026-10-24, read-only): every table-creating and table-reading site per leg, the bench table exclusion, every place `testing_` is assumed, and finding F-1 (one shared run prefix lets a leg's cleanup and cross-read reach other legs' tables).

Up: [../map.md](../map.md).
