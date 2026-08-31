# docs/artifacts — published report pages

Self-contained HTML pages the orchestrating session publishes as Claude artifacts and files here
so the repository carries the same page the owner reads. One file per report, dated in the
filename; each is a complete standalone document (open it in a browser) and is never edited
after filing — a later report is a new file. Every fact on a page cites its source documents;
the page is a rendering, and the sources stay authoritative.

| File | What it is |
|---|---|
| [road-to-v1-2026-08-30.html](road-to-v1-2026-08-30.html) | The master plan from 2026-08-30 to the v1.0 tag: the 0.x ladder (v0.6 DML remainder through v0.10 repark.toml) and the v3 spine (fork F-7, V3-4/5/6, live legs, gate review) as parallel streams; the PR-parking operating model; worker tiers (Grok maxed, GLM fan-outs, no Opus); the owner's per-release SQM + publish-pypi walkthrough. Sources: the release roadmap, the v1.0 gate, the intake, release.md. |
| [next-two-prs-2026-08-30.html](next-two-prs-2026-08-30.html) | The plan for the next two large PRs after MW-10 closed: V3-3 (v3 UPDATE/MERGE, priority lane, owner gate) and FNP-15/16 (register the 62 missing names, Grok template + GLM fan-out), run as parallel worker lanes. Sources: the slate, the V3-3 charter ledger, spark-function-parity §7/§7.1, STATUS.md, live PR/run state. |
| [roadmap-status-2026-08-30.html](roadmap-status-2026-08-30.html) | Roadmap status after PR #262: release state, the version ladder v0.5→3.0, the v1.0 north-star gate row by row, the ordered next steps, campaigns, risks. Sources: STATUS.md, the 2026-08-29 release roadmap and design plan, format-v3-track §5, the v1.0 gate, the slate, the live ledgers. |
