# Charter ledger — DL-3 · the archive month map reads like an index, not a book

**Date:** 2026-08-23 · **Branch:** `feat/dl-3-archive-map-compaction` · **Base:** `3e9a0d1`
(`main`, post-#224) · **Policy:** [../../../AGENTS.md](../../../../AGENTS.md) "Markdown document
lifecycle" · **Changes:** `scripts/ledger_lifecycle.py` (a DL-1 surface) + one migration

**Retires:** this ledger moves to `../completed/` in the unit's last commit; the first pickup after
the merge archives it.

An agent reported `task/ledgers/archive/2026-08/map.md` at ~54 kB / ~13k tokens; measured true
(53,713 B at `d01c3b6`; **55,502 B / 125 rows** after this unit's own pickup archived MW-0 and
MW-5). The DL-1 backfill moved every `task/map.md` row into the month map with its description
intact. No gate penalizes the file and only `../map.md` links to it — the cost is paid solely by
an agent that opens it — and it does not compound (future months hold a handful of ledgers). The
owner nevertheless ruled to compact.

## 1. Ruling (owner, 2026-08-23)

**Compact.** Archive month maps carry **one line per ledger** — the link plus the first sentence
of the description. The full prose survives in two places that outrank a navigation row: the
ledger itself (the record) and git history (the rows as backfilled). The lifecycle rule "truth
moves, it is never deleted" is read as being about the *record*, not the navigation copy — this
reading is the ruling's substance and is recorded here so it is not re-litigated.

## 2. Design

- **`_condense_row()`** in `scripts/ledger_lifecycle.py`: join a row's wrapped lines into one,
  keep the link, cut the description at the first sentence boundary (`. ` after the em-dash;
  a description with no boundary stays whole). Applied by the paste path **only when the
  destination map is an archive month map** — `staging/` and `completed/` rows keep travelling
  whole, because those maps are the live read path where the description earns its bytes.
- **Purpose note**, in the generator and the tree: archive maps say they are off the normal read
  path — grep the directory for a unit; do not read the file whole.
- **Migration**: the 125 existing rows of `archive/2026-08/map.md` condensed once by the same
  function (a scratch driver importing the module — no second implementation), committed after a
  dry-run diff on a clone. Expected ≈55 kB → ≈15 kB.
- Out of scope: row format changes anywhere else; `archive/map.md` (already one row per month);
  any ledger's content.

## 3. Proposition ledger — DL-3

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Archiving a ledger whose map row wraps over several lines and sentences yields a one-line month-map row ending at the first sentence, link intact. | Provocation test on a scratch tree. | PROVEN | `test_an_archive_month_map_row_is_one_line_first_sentence` |
| C-002 | A `move` to `staging/` or `completed/` still carries the whole row — wrapped text and sub-lists. | The whole-row rule gets its own live-bin pin; the two old archive-destination pins are retargeted (a declared behaviour change, in commit 3's message). | PROVEN | `test_a_move_to_a_live_bin_still_carries_the_whole_row` |
| C-003 | The migrated `2026-08/map.md` is one line per row, every link still resolves, and the size drop is measured and recorded. | The migration commit's `check-ledgers` + `map-sync` runs and the byte counts. | PROVEN | `578a704`: 126 rows, 55,502 → **29,277 B**; 143 maps clean; 522 ledger links resolve. Mechanism pin: the C-001 test |
| C-004 | Archive maps state they are off the read path, in the generator and in the tree, and a regenerated `archive/map.md` does not drift from the tree copy. | Grep both after the migration; `ledger-archive` idempotence run. | PROVEN | `test_an_archive_month_map_row_is_one_line_first_sentence` (the note is asserted); dry-run: `nothing to archive`, regeneration changes only the Purpose lines |

Verdicts were `OPEN` at charter time and flipped at departure, with the attestation below.
VERDICT: PASS (OPEN=0, REJECTED=0).

## 4. Execution record (2026-08-23, same day)

Five commits: `b0afb91` pickup (MW-0 + MW-5 ledgers archived by `make ledger-archive`; the
grammar gate's mw-0 `EXCEPTIONS` row went red by its own stale-row rule and was deleted) →
`2fc727f` this charter → `26d2c20` `_condense_row` + Purpose notes + tests (24 green; the two
DL-1 archive-destination pins retargeted as a declared behaviour change) → `578a704` the
migration (dry-run on a clone first, diff inspected, idempotence checked; **measured 29,277 B**,
not the charter's ≈15 kB guess — filenames appear twice per row; the expectation was wrong, the
measurement stands) → this departure.

**The Critic's pass** (procedural break): the S0 class is *silent loss of a row's meaning*; the
fresh input was the multi-sentence + `+ `-continuation row in the C-001 test, chosen to hit the
sentence cut and the join in one row, absent from the Actor-phase tests.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: DL-3
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        C-001..C-004 walked against the script and the migrated tree; each clause's test cites it.
      artifacts: [python/repark-parity/tests/test_dl_1_ledger_lifecycle.py]
    - id: AT-2
      status: ATTACKED
      evidence: >
        A description with no sentence boundary (stays whole), bold-only descriptions, a row whose
        continuation starts with `+ `, a nested sub-list after the first sentence (dropped), links
        inside the dropped tail (fewer links is legal; resolution re-checked by the plan verifier).
      artifacts: [test_an_archive_month_map_row_is_one_line_first_sentence, test_a_plus_continuation_is_joined_not_split]
    - id: AT-3
      status: ATTACKED
      evidence: >
        Idempotence after migration (`archive` = nothing to archive; regeneration drifts nothing
        but the Purpose wording, which the migration aligned).
      artifacts: ["dry-run record in 578a704's message"]
    - id: AT-4
      status: N/A
      justification: stateless one-pass text transform inside the existing plan/apply machinery.
    - id: AT-5
      status: N/A
      justification: no new inputs, no subprocess, writes staged as before.
    - id: AT-6
      status: ATTACKED
      evidence: >
        The compatibility surface is the 126 real rows: migrated by the same function the tests
        pin, on a clone first, links verified before write (refuse-on-dangle unchanged).
      artifacts: ["578a704", "test run: 24 passed"]
    - id: AT-7
      status: N/A
      justification: strictly fewer bytes; the read-cost reduction is the unit's purpose.
    - id: AT-8
      status: ATTACKED
      evidence: >
        The DL-1 contract change is declared, not smuggled: the old archive-destination pins were
        retargeted in their own commit with the reason, and scripts/map.md states the split rule.
      artifacts: ["26d2c20", scripts/map.md]
    - id: AT-9
      status: ATTACKED
      evidence: >
        The month maps now say what they are and how to use them (grep, not read); the summary
        lines carry the counts.
      artifacts: [task/ledgers/archive/2026-08/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: >
        Every clause carries a pins: citation from a named test; the behaviour change replaced its
        old pins rather than deleting coverage.
      artifacts: [test_a_move_to_a_live_bin_still_carries_the_whole_row]
  reattested: []
  complete: true
```
