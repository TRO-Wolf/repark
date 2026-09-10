# Unit ledger — REVIEW-FIX-12 · the docs-links gate measures what it claims

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when REVIEW-FIX-12 merges, or when the owner closes the slate row.

**Unit:** REVIEW-FIX-12 · **Date:** 2026-09-10 · **Executor:** Muse Spark (muse-spark-1.3-contributor), Actor ·
**Branch:** `fix/review-fix-12` · **Base:** `8b2673fd`
**Model:** muse-spark-1.3-contributor
**Card:** card REVIEW-FIX-12 in `task/roadmap/mid-term/review-1-findings-2026-09-10.md` (Q-35, Q-36, Q-37, Q-38, Q-46, Q-47)
**risk_tier:** standard.

Every pin below went red first against the base tree, then green after the fix. All
pasted gate output is inside code spans so this ledger's own table rows stay clean.

## Proposition ledger

| ID | Clause | Evidence | Verdict |
|---|---|---|---|
| C-001 | D-1 Anchors are slugged from the rendered heading text, and duplicate suffixing counts occurrences of the final slug. | Red: `test_link_heading_slugs_the_rendered_text` failed on base with `README.md:11: LINKHEAD.md#usage -> anchor #usage does not match a heading`, and the converse pin showed the base gate accepting the raw-markdown anchor; `test_duplicate_slugs_count_the_final_slug` failed with `DUPS.md#foo-1-1 -> anchor #foo-1-1 does not match a heading`. Green after: all three pins pass; headings that are links slug to `usage`, and `Foo` / `Foo` / `Foo-1` anchor `foo` / `foo-1` / `foo-1-1`. | **PROVEN** |
| C-002 | D-2 The `docs:` evidence-cell pattern applies to table cells under `task/ledgers/**`, not to prose. | Red: `test_docs_token_in_prose_is_not_an_evidence_cell` failed on base with `task/ledgers/staging/u1-ledger.md:7: add -> does not exist` for a prose line carrying `docs: add changelog`. Green after: the prose line is ignored and the pin passes; the table-cell bad-anchor pin still reds. | **PROVEN** |
| C-003 | D-3 An unbalanced fence at end of file is a finding, not a silent skip. | Red: `test_unclosed_fence_is_a_finding` failed on base with returncode 0 and `docs-links: 4 files, 6 links checked — clean` for a file with an unclosed fence and a broken link after it. Green after: the gate reports `unclosed fenced code block` at the opener line and exits 1. | **PROVEN** |
| C-004 | D-4 An absolute target is skipped as out of scope, like `http(s)` and `mailto`, and `scripts/map.md` says so once. | Red: `test_absolute_target_is_out_of_scope` failed on base with `README.md:11: /abs/path -> resolves outside the repository`. Green after: the link is skipped and the pin passes; the scope sentence lives in the `check_docs_links.py` bullet of `scripts/map.md` and nowhere else. | **PROVEN** |
| C-005 | D-5 A same-file `#anchor` is checked against that file's headings, and the stale `V3-COV-8` fragment at `docs/spark-sql-iceberg-parity.md:3053` is fixed in the same commit. | Red: `test_same_file_anchor_is_checked` failed on base with returncode 0 and `docs-links: 4 files, 6 links checked — clean` for a same-file anchor that resolves nowhere; the corrected gate then flagged the tree with `docs/spark-sql-iceberg-parity.md:3053: #v3-cov-8--ctas-derives-a-wider-required-iceberg-column-where-spark-derives-the-literals-narrower-optional-one -> anchor #v3-cov-8--ctas-derives-a-wider-required-iceberg-column-where-spark-derives-the-literals-narrower-optional-one does not match a heading`. The `:3018` heading had gained a `FIXED 2026-09-05, TYPES-1` suffix, so its slug ends `--fixed-2026-09-05-types-1`; the link now carries that suffix and the tree is clean. | **PROVEN** |
| C-006 | D-6 An indented fence opener is recognised: at most three leading spaces open a fence. | Red: `test_four_space_indented_fence_is_not_a_fence` failed on base with returncode 0 and `docs-links: 4 files, 6 links checked — clean`. Green after: the four-space fence hides nothing and the gate reports `MISSING.md -> does not exist`; the guard `test_indented_fence_opener_still_hides_links` passes before and after, so a two-space opener still fences. | **PROVEN** |

## Fix

| Name | Layer |
|---|---|
| `heading_anchors` | `scripts/check_docs_links.py`: inline-link syntax reduced to its text before slugging; duplicates suffixed by occurrence of the final slug with collision re-check |
| `scan_file` | `scripts/check_docs_links.py`: same-file `#anchor` checked against the linking file's own headings; absolute `/…` targets skipped; `docs:` cells gated to table rows; the unclosed fence reported at its opener line at end of file |
| `FENCE_PATTERN` | `scripts/check_docs_links.py`: `^\s*` narrowed to `^ {0,3}`, shared by scanning and anchor collection |
| stale fragment | `docs/spark-sql-iceberg-parity.md:3053` repointed at the measured `--fixed-2026-09-05-types-1` anchor |

## Red-first record

| Pins | Red on base | Green after |
|---|---|---|
| 8 pins in `python/repark-parity/tests/test_dl_6_docs_links.py` | 8 failed, 11 passed | 19 passed |
| whole-tree `python3 scripts/check_docs_links.py` | 1 broken link (the stale V3-COV-8 fragment), 0 stale allowlist entries | `docs-links: 736 files, 4772 links checked — clean`, exit 0 |

No allowlist row was added or retired: all nine seeded entries still match findings, so the
ratchet list is untouched.

## Delivery

| Item | Path |
|---|---|
| Gate | `scripts/check_docs_links.py` (D-1 through D-6) |
| Pins | `python/repark-parity/tests/test_dl_6_docs_links.py` (eight red-first pins plus the two-space-opener guard) |
| Tree repair | `docs/spark-sql-iceberg-parity.md:3053` |
| Scope record | `scripts/map.md` (`check_docs_links.py` bullet) and `python/repark-parity/tests/map.md` (`test_dl_6_docs_links.py` bullet, carrying the clause citations) |
| Ledger | `task/ledgers/staging/review-fix-12-ledger.md` (this file) |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: review-fix-12
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Eight red-first pins fail on the base tree in the critic's exact classes and pass after the fix.
      artifacts: [python/repark-parity/tests/test_dl_6_docs_links.py]
    - id: AT-2
      status: ATTACKED
      evidence: Boundary arms pinned both ways — raw-markdown anchor rejected, two-space opener still fences, table-cell docs cells still checked.
      artifacts: [python/repark-parity/tests/test_dl_6_docs_links.py]
    - id: AT-3
      status: ATTACKED
      evidence: Malformed allowlist still fails closed and stale entries still fail; both pins untouched and green.
      artifacts: [python/repark-parity/tests/test_dl_6_docs_links.py]
    - id: AT-4
      status: N/A
      justification: Single-threaded batch script over git-tracked text; no shared mutable state, no concurrency.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, .github, or dependency-file change; no push, no gh.
      artifacts: [scripts/check_docs_links.py]
    - id: AT-6
      status: ATTACKED
      evidence: No public API change; the gate's stdout contract gains one reason string for one new finding class.
      artifacts: [scripts/check_docs_links.py]
    - id: AT-7
      status: N/A
      justification: Docs gate with scratch-tree pins; no live oracle exists for GitHub slug rendering.
    - id: AT-8
      status: ATTACKED
      evidence: No Python size-gate row covers scripts; ruff line-length holds; no comments added to code.
      artifacts: [scripts/check_docs_links.py, python/repark-parity/tests/test_dl_6_docs_links.py]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface; gate stdout unchanged except the new finding line.
    - id: AT-10
      status: ATTACKED
      evidence: Clauses cited from python/repark-parity/tests/map.md; maps in lockstep in the same commit.
      artifacts: [task/ledgers/staging/review-fix-12-ledger.md, python/repark-parity/tests/map.md]
```
