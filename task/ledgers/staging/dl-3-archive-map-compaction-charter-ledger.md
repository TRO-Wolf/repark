# Charter ledger — DL-3 · the archive month map reads like an index, not a book

**Date:** 2026-08-23 · **Branch:** `feat/dl-3-archive-map-compaction` · **Base:** `3e9a0d1`
(`main`, post-#224) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md) "Markdown document
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
| C-001 | Archiving a ledger whose map row wraps over several lines and sentences yields a one-line month-map row ending at the first sentence, link intact. | Provocation test on a scratch tree. | OPEN | commit 3 — `test_dl_1_ledger_lifecycle.py` |
| C-002 | A `move` to `staging/` or `completed/` still carries the whole row — wrapped text and sub-lists. | The existing DL-1/DL-2 row-travel tests stay green unchanged. | OPEN | commit 3 test run |
| C-003 | The migrated `2026-08/map.md` is one line per row, every link still resolves, and the size drop is measured and recorded. | The migration commit's `check-ledgers` + `map-sync` runs and the byte counts. | OPEN | commit 4, §4 |
| C-004 | Archive maps state they are off the read path, in the generator and in the tree, and a regenerated `archive/map.md` does not drift from the tree copy. | Grep both after the migration; `ledger-archive` idempotence run. | OPEN | commit 4, §4 |

Verdicts flip at departure with the attestation, per the DL-2 gate.

## 4. Execution record

*(appended at departure)*
