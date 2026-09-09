# Unit ledger — DOCS-LINKS-1 · `make check-docs-links`, the gate LEDGER-READING-1 assumed

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when DOCS-LINKS-1 merges.

**Unit:** DOCS-LINKS-1 step 1 · **Date:** 2026-09-09 · **Model:** glm-5.3-flash (GLM 5.3 Flash,
zai/glm-5.3-flash) · **Branch:** `feat/docs-links-1`
**Path:** STANDARD.
**Home:** [scripts/check_docs_links.py](../../../scripts/check_docs_links.py),
[scripts/docs_links_allowlist.txt](../../../scripts/docs_links_allowlist.txt),
[test_dl_6_docs_links.py](../../../python/repark-parity/tests/test_dl_6_docs_links.py)

## Clause table

| Clause | Decision | Verdict | Evidence (red-first output, baseline, gates) |
|---|---|---|---|
| C-001 | D-1 scope: over every tracked `*.md` (`git ls-files`), each inline relative link (`[t](p)`, `[t](p#anchor)`) resolves to a tracked file relative to the linking file (a directory by anything tracked beneath it — AGENTS.md, STATUS.md and 6 more files carry directory links the card's Why paragraph calls healthy), a `#anchor` on an `.md` target matches the GitHub-style heading slug (lower-case, spaces to `-`, punctuation dropped, duplicate headings suffixed `-1` per GitHub's renderer), and `docs:` evidence cells under `task/ledgers/**` follow the same rule with repo-root-relative paths; `http(s)`/`mailto` links, bare `#fragments`, absolute targets and anchors into non-markdown targets are out of scope, code spans and fenced blocks hold documentation, not links | PROVEN | Red first: `VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests/test_dl_6_docs_links.py -q` on the base tree (no script) → `8 failed in 0.22s`. Green after the script: same command → `8 passed in 1.22s`. The fixture tree pins each scope half: one good link and one good anchor count toward the exit-0 line, externals, bare fragments, code spans and fenced links are skipped, a missing target, an existing-but-untracked target, a bad anchor and a bad `docs:` cell each red with the standing message, a subdirectory file's `../` link resolves relative to its file, and the real tree is green under the seeded allowlist. |
| C-002 | D-2 output: one line per broken link `path:line: <link> -> <reason>` and exit 1 on any; exit 0 prints the counts of files and links checked | PROVEN | Failure direction on the unseeded tree: `docs-links: FAIL — 10 broken link(s)` (exit 1) after ten `path:line: <link> -> <reason>` lines on stderr. Clean direction: `docs-links: 692 files, 4478 links checked — clean` (exit 0). Wiring: the `check-docs-links` target is defined in the Makefile and joins `make ci` immediately after `check-docs-compaction` — the card's "added to `preflight` beside `check-docs-compaction`" is satisfied mechanically, because `preflight` runs `ci` through `verify`, and that is exactly where `check-docs-compaction` itself is wired (the card's premise that `check-docs-compaction` is a direct preflight prerequisite is not the tree's fact). Measured: `make check-docs-links` exit 0, `make check-docs-compaction` exit 0, `make check-ledgers` exit 0. |
| C-003 | D-3 baseline: run the script over the tree first, seed `scripts/docs_links_allowlist.txt` with the broken links (one `path:line:link` entry per line, entries only shrink), file the count as residue, fix nothing | PROVEN | Baseline run (before seeding): exit 1 with exactly 10 broken links — 6 missing targets (5 in immutable `task/ledgers/archive/` ledgers, 1 an ellipsis character in a completed ledger) and 4 stale anchors into `docs/spark-sql-iceberg-parity.md` (the hand-written anchors predate the registry headings' FIXED suffixes and drop the `--` a `/` renders as). The allowlist carries those 10 entries one `path:line:link` per line under a `#` header the script tolerates; a malformed entry fails closed (exit 2, pinned); an entry drops exactly its own finding and a stranger link still reds (pinned). Nothing fixed, nothing repaired. Runtime measured 0.82 s whole-tree (0.83/0.81 s repeats; 692 files, 4478 links) — inside the 10 s budget. Audit round 1 turned "only shrinks" into an enforced ratchet: an allowlist entry that matched no finding this run is a gate failure (`scripts/docs_links_allowlist.txt: stale entry <path:line:link> — the link is no longer broken; remove this row`, exit 1, `docs-links: FAIL — 0 broken link(s), 1 stale allowlist entry`), a fully-matched allowlist still exits 0 with the counts line, and the reseeded real tree measured all 10 entries matched (`docs-links: 693 files, 4482 links checked — clean`) — no row removed, none stale. Red first for the new pin: `VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests/test_dl_6_docs_links.py -q` on the pre-ratchet script → `1 failed, 8 passed in 1.31s` (`test_stale_allowlist_entry_fails` red: the stale entry exited 0); green after: same command → `9 passed in 1.28s`. |
| C-004 | D-8 (orchestrator ruling, 2026-09-09): the allowlist ratchets down and a stale entry is a gate failure — an entry that matched no broken link in the run is itself a finding, so a fixed link takes its row away under gate pressure, not by memory | PROVEN | The ratchet is enforced in `run()`: entries are consumed by matching findings, and `allowlist - matched` prints one stale-entry line per survivor before the FAIL summary, exit 1. Pinned red-first by `test_stale_allowlist_entry_fails` (all-good fixture links, one allowlist entry → exit 1 with the stale entry's text and the `0 broken link(s), 1 stale allowlist entry` summary), and the zero-stale half is pinned by the existing allowlist tests plus the real-tree run, which exits 0 with all 10 seeded entries matched. |

## Residue

| Count | Where recorded | Disposition |
|---|---|---|
| 10 broken links measured 2026-09-09 (4 stale anchors into the divergence registry, 5 dead targets in immutable archive ledgers, 1 ellipsis target in a completed ledger) | `scripts/docs_links_allowlist.txt`, one `path:line:link` entry per line | Deferred by card D-3 — seeded, not fixed; the allowlist only shrinks as each entry's link is repaired in a unit that owns the file. |

## Notes

Audit round 1 (2026-09-09, one finding): the allowlist was an exemption list, not a ratchet —
nothing noticed an entry whose link had been fixed or whose line had moved. Remediated as C-003
and C-004 record: matching consumes an entry, and a survivor is a gate failure. The real tree
re-ran clean with all 10 seeded entries still matching, so no row was removed.

Plain `python3 -m pytest` cannot import pytest in this clone, so the gate command used the
ruling's fallback, `VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest …`; the clone's
`.venv` was provisioned minimally (pytest only, no project sync, no lockfile touched). The test
home is `python/repark-parity/tests/test_dl_6_docs_links.py` per ruling D-4 — no `scripts/tests/`
tree exists or was created — and the tests carry no inline pins, so the unit's clauses are cited
from the tests map row.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: docs-links-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each decision walked as an executable case on a scratch tree — the clean fixture counts files and links, the missing and untracked targets red, the bad anchor and bad `docs:` cell red with the standing message, the allowlist drops only its own entry — plus the real-tree green run under the seeded allowlist.
      artifacts: [python/repark-parity/tests/test_dl_6_docs_links.py, scripts/check_docs_links.py, scripts/docs_links_allowlist.txt]
    - id: AT-2
      status: ATTACKED
      evidence: The numeric outputs are pinned by exact-string asserts (the fixture's file and link counts, the exit codes) and the real-tree counts and runtime were measured and pasted into C-002 and C-003 rather than estimated.
      artifacts: [python/repark-parity/tests/test_dl_6_docs_links.py]
    - id: AT-3
      status: N/A
      justification: No failure path beyond the gate's own reporting; the failure lines and exit codes are the subject and are asserted verbatim in both directions.
    - id: AT-4
      status: N/A
      justification: No state, ordering or concurrency; the gate is a single read-only pass with one anchor cache.
    - id: AT-5
      status: N/A
      justification: No privileged action, secret, deserialization or network; the gate reads tracked files and one allowlist file.
    - id: AT-6
      status: ATTACKED
      evidence: The holes that would fake a green gate were attacked — a target that exists on disk but is untracked still reds, a stranger broken link stays red beside an allowlisted one, and a malformed allowlist entry fails closed (exit 2) instead of silently passing.
      artifacts: [python/repark-parity/tests/test_dl_6_docs_links.py]
    - id: AT-7
      status: N/A
      justification: No execution path or resource behavior beyond reading markdown; the whole-tree run measured 0.82 s against the 10 s budget.
    - id: AT-8
      status: N/A
      justification: No API, dependency or upstream contract touched; no Cargo, pyproject, uv.lock or .github file edited, and the script is stdlib-only.
    - id: AT-9
      status: N/A
      justification: Nothing new to diagnose; every failure line carries path, line, the link as written and the reason.
    - id: AT-10
      status: ATTACKED
      evidence: Red-first — the eight pins ran against the base tree where the script does not exist (8 failed in 0.22s), so the base tree is the mutant for the whole gate, and each behavior case then ran green against the landed script.
      artifacts: [python/repark-parity/tests/test_dl_6_docs_links.py, python/repark-parity/tests/map.md]
  complete: true
```
