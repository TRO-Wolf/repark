# JAVA-REGEX-FEATURES-1 — split and regexp_* answer Java lookaround, backreferences and possessive quantifiers

**Filed: 2026-10-02, re-cut from the closed draft PR #658.** The PR body
and close comment were not re-read in this lane (`gh` is banned for
worker lanes); the card is re-cut from
[overnight-report-2026-09-16-18c.md](overnight-report-2026-09-16-18c.md)
§1c (verified in-repo). Kept branch: `feat/java-regex-features-1` at
`4c36c6f6df37c8afdefe8bca4beae524d687ec6e` (12 commits per 18c; remote
head read 2026-10-02 matches). **Status:** draft, verification-clean
(Grok verification critic: L-001…L-007 and PERF-001/002/003/004/007 all
CLOSED, no new P1; actor gates verify 0, clippy 0, facade 9100 passed,
parity 757 passed), never queued — CI and the merge queue could not
finish inside run 18c's window. **Target:** mid-term.

## The gap it records

Spark's `split` / `regexp_*` / `rlike` answer lookahead, lookbehind,
backreferences and possessive quantifiers; the `regex` crate supports
none of them. The branch carries `fancy-regex 0.11` as the fallback
engine, used only for patterns that need one of those features, with a
backtrack limit (owner ruling Q-16c-1). The oracle probe differs only on
the three declared residue cells below.

## Repro and measured answers

Spark 4.1.2 answers, recorded 2026-09-16 in `/tmp/oc-worker/sc/oracle/`
(`rx3-oracle.json` batch `sc18-rx3`, `b3-oracle.json`; verified present
2026-10-02):

| Cell | Expr | Spark |
|---|---|---|
| RX3-SQL-20 | `SELECT regexp_replace('😀','(?=)','X')` | `X?X?X` |
| RX3-SQL-21 | `SELECT split('😀','(?=)')` | `['?', '?', '']` |
| RX2-SQL-12 | `SELECT rlike('aA', '(?i)(a)\1')` | `true` |

RX3-SQL-20/21 are Java's zero-width matches splitting a surrogate pair
(DECLARED row R2); RX2-SQL-12 is the case-insensitive backreference
(DECLARED row R1). The branch's answers on these three cells are not
recorded in the re-cut sources — not yet measured. Repro to run on the
day: the three cells on the rebased branch against live Spark.

## Scope

Rebase onto current main, re-run the full gate (verify, clippy, whole
facade, whole parity), then un-draft and merge once CI is green. Carry
the DECLARED rows JAVA-REGEX-BACKTRACK-1 (a runaway pattern refuses
loudly; Spark's JVM dies with StackOverflowError), R1 and R2.

## Out of scope

- The R1/R2 residues themselves (declared divergences, not merge
  blockers).
- The date-boundary pin flake Q-18c-6
  (`test_q14_current_date_bare_and_paren` red on main 20:00–24:00 EDT;
  unrelated to this unit).

## Clauses (draft)

- **C-001** The branch rebases with the verification-clean verdict
  intact (re-run the critic if the rebase touches product code).
- **C-002** The full gate and CI are green on the rebased head.
- **C-003** The three residue cells carry their DECLARED pins.

## Pointers

- Up: [map.md](map.md) · State at 21:15 (in-repo):
  [overnight-report-2026-09-16-18c.md](overnight-report-2026-09-16-18c.md)
  §1c · Oracle batches (evidence location):
  `/tmp/oc-worker/sc/oracle/rx3-oracle.json`,
  `/tmp/oc-worker/sc/oracle/b3-oracle.json`
