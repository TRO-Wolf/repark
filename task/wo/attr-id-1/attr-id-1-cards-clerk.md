# ATTR-ID-1 OD-3: file four pre-existing findings as mid-term cards (Muse clerk, medium; lane /tmp/xcards, branch docs/attr-id-1-cards at origin/main db3a1f37; docs only)

Read first:
- AGENTS.md, from this clone;
- `/tmp/oc-worker/direct/wo/attr-id-1-design.md`, §3.8 and §7a (the owner adopted OD-3 on 2026-09-30: "Go with your recommendations on the document");
- one existing card for the format: `task/roadmap/mid-term/tz-offset-seconds-1-card-2026-09-16.md`;
- `task/roadmap/mid-term/map.md`.

## Write four cards under `task/roadmap/mid-term/`, one file each
Take each card's facts from the verifier hand-back named here. Keep the repro verbatim, give the severity, and write "Filed: 2026-09-30 under owner ruling OD-3 (ATTR-ID-1 work order)". Include the measured base and Spark behaviour, the suspected cause and the suggested fix direction, marked as the verifier's suggestion rather than a decision.

1. **`plan-depth-1-card-2026-09-30.md`:** RC5-5, from `/tmp/oc-worker/direct/wo/reverify4-cs2-opus-handback.json`. The process crashes with SIGSEGV on `count()` over 200 or more chained `DataFrame.filter` calls; the script is `/tmp/oc-worker/direct/wo/reverify4-cs2/segv.py`.
   - **Orchestrator measurement, 2026-09-30:** the PR #892 head (cb8c8b8d, DEEP-FILTER-CHAIN-CRASH-1) answers 50 at both 200 and 1000 chained string filters. Base v1.5.1 (adc26586) dumps core on both.
   - The card therefore says it is **covered by PR #892** and closes when #892 merges. Its remaining work is to re-run the repro, with string and Column filters under both `caseSensitive` settings, on main after that merge.
2. **`sql-lambda-scope-1-card-2026-09-30.md`:** RC5-6, from the same hand-back. It gives silent wrong rows on the `spark.sql` path.
3. **`sort-parent-column-1-card-2026-09-30.md`:** RC5-7, from the same hand-back. `orderBy(parent Column)` on a case-twin frame returns rows in the wrong order under `caseSensitive=true`.
4. **`unicode-case-version-1-card-2026-09-30.md`:** RC3-5 / R-CS2-17, from `/tmp/oc-worker/direct/wo/reverify2-cs2-opus-handback.json`. Case folding uses Rust's Unicode 16 tables, while JDK 17 Spark uses Unicode 13. Search `/tmp/xs-cs1/task/ledgers/staging/` for the R-CS2-17 residue text as well; the #881 lane is read-only for you.

Add one row per card to `task/roadmap/mid-term/map.md`, in the existing row style. Run `test -e` on every path you cite in a card; paths under `/tmp` are evidence locations and may be cited as such.

## Gate
- `bash scripts/check_map_md.sh --base origin/main`
- `python3 scripts/check_docs_links.py`
- `python3 scripts/check_ledger_grammar.py`
- `uvx typos` over the new files, if the repo's typos config is present.

## Commit (one commit)
- Commit: `docs(attr-id-1-cards): file RC5-5, RC5-6, RC5-7 and RC3-5 as mid-term cards under owner ruling OD-3`.
- Identity: `git -c user.name="TRO-Wolf" -c user.email=64240326+TRO-Wolf@users.noreply.github.com`.
- Exactly one trailer: `Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>`.
- Hand-back: `/tmp/xcards/handback.json`.

## Rules
- Work only in `/tmp/xcards`; read other paths without changing them.
- No code files and no product edits.
- Never touch `~/CodeRepos`. Never use AWS credentials. Do not push.
