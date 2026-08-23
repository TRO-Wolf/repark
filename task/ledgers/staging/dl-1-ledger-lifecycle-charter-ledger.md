# Charter ledger — DL-1 · the ledger lifecycle, run by a script

**Date:** 2026-08-23 · **Branch:** `feat/dl-1-ledger-lifecycle` · **Base:** `b13b22c` (`main`,
post-#220) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md) "Markdown document lifecycle" ·
**Executor today:** [../../../.agents/skills/compact-context-docs/SKILL.md](../../../.agents/skills/compact-context-docs/SKILL.md)

**Retires:** this ledger moves to `task/ledgers/completed/` in the unit's last commit — it is the
first ledger to use the mechanism it charters — and the first pickup after the merge archives it.

The owner asked for a data-lifecycle policy for the per-unit ledgers: a deterministic script that
archives what is finished, so the pickup ritual spends no tokens on filing, and a home for the
roadmap by horizon. Four rulings were taken on 2026-08-23 and are recorded in §2. This ledger is
the scope audit; it proposes nothing that the rulings did not already decide, and it names every
number it relies on.

## 1. The problem, measured (2026-08-23, at `b13b22c`)

| Measure | Value | What it means |
|---|---|---|
| `task/` on disk | 6.6 MB | — |
| of which `task/census/` | 4.5 MB (68 %) | two recorded acceptance runs, milestone-one evidence, regenerable by `make census` |
| unit ledgers (`task/*-ledger.md`) | **125 files, 1.45 MB**, all born 2026-08 | the count is the problem, not the bytes |
| `task/map.md` | 76 KB, 137 entries | read at every pickup — the real token cost |
| git pack, whole repository | 4.85 MB | `task/` history alone: 13.2 MB raw → 4.0 MB packed |
| files outside `task/` that link to a ledger | **182**, to 110 distinct ledgers | `python/repark/tests/map.md` 41, each `docs/history/*/promotion-ledger.md` ≈ 20 |
| ledger links the gates check today | only those inside `map.md` files | `scripts/sync_map_md.py`; links from STATUS, briefs, the registry and test maps are unchecked |
| ledgers carrying a machine-readable "done" | 7 of 125 | the merge is the only reliable signal |

Two conclusions fall out. **Moving a ledger is a repository-wide link rewrite**, so the move and
the rewrite must be one deterministic operation, and the repository needs the link check it does
not have. **Bytes are not the lever**: the archive's job is provenance (AGENTS.md "truth moves,
it is never deleted"), git already compresses and delta-stores the text, and a binary
consolidation (parquet was asked about) would be un-diffable, un-greppable and un-linkable inside
the repository for no size gain that matters. A *derived* dataset built from the archive outside
the tree is a separate, later idea (§7).

## 2. Owner rulings (2026-08-23)

1. **Three bins, with an explicit `completed/`.** `staging/` → `completed/` is the agent's move,
   in the unit's last commit; `completed/` → `archive/` is the script's, at pickup.
2. **Two roadmap bins, not three.** `briefs/next-sequence.md` is already the short-term home
   (the ordered slate) and STATUS the active/deferred home; a `short-term/` directory would be a
   third home for the same queue. `mid-term/` and `epic-term/` only.
3. **Backfill in one migration PR.** Every finished ledger on `main` moves in one mechanical
   change, links rewritten.
4. **Evict `task/census/`.** The working tree and the maps stop carrying the 4.5 MB; git history
   keeps the evidence at a named SHA.

## 3. The design

### 3.1 Bins — the directory *is* the status

```
task/ledgers/
  staging/                       born on the unit's branch; may sit on main across PRs (charters)
  completed/                     moved here by the agent in the unit's last commit; frozen
  archive/yyyy-mm/yyyy-mm-dd-<name>.md   moved here by the script at pickup; immutable
```

No frontmatter, no status field, nothing to parse: a ledger's state is its path. The ledger
class row in AGENTS.md changes from "frozen at merge; archived with its campaign" to these three
states; a campaign's folder under `docs/history/` links to its ledgers in the monthly archive
instead of containing them (the two existing campaign folders keep theirs — archived material is
not moved twice).

A ledger in `staging/` on `main` is legitimate exactly when the retirement event it names at
birth has not happened — charters that span PRs (`mw-0-charter`, …). That is the AGENTS.md
"names the event that retires it at birth" rule doing the work; no allowlist.

### 3.2 The script — `scripts/ledger_lifecycle.py`, zero tokens

Python, stdlib only, the same shape as `scripts/sync_map_md.py`. Three subcommands:

- **`archive`** — for each file in `task/ledgers/completed/`: the date is the author date of the
  commit on `main` that first placed it in `completed/` (`git log --diff-filter=A --format=%as
  -1 --first-parent main -- <path>`), so two agents on two machines produce the same name; `git
  mv` to `archive/yyyy-mm/yyyy-mm-dd-<name>.md`; rewrite every relative link that resolved to
  the old path in every `*.md` under the repository (resolution is path-based, not text-based:
  a link is rewritten only if it *resolved* to the moved file, so a same-named file elsewhere is
  untouched); create `archive/yyyy-mm/map.md` if missing and add the row; remove the
  `completed/map.md` row; regenerate `archive/map.md` (one row per month, newest first). Exit 0
  with a summary line, or non-zero having changed nothing if any link would fail to resolve
  after the rewrite.
- **`move <ledger> <bin>`** — the agent's `staging/` → `completed/` step and the roadmap
  promotion (§3.4), with the same link rewrite and map-row maintenance. The only way ledgers
  should move.
- **`check`** — the gate (§3.3).

Determinism rules the script obeys: no wall clock (dates come from git); no network; stable
ordering (sorted paths); idempotent (`archive` on an empty `completed/` is a no-op exit 0);
never rewrites inside `docs/history/` except the link repair the archive rules already allow.

### 3.3 The gate — `make check-ledgers`, wired into `make ci`

`ledger_lifecycle.py check` fails on any of: a `*-ledger.md` outside the three bins; an archive
file whose `yyyy-mm-dd-` prefix does not match its `yyyy-mm/` directory; a relative link
anywhere in the repository's markdown (not only maps) whose target is a `-ledger.md` that does
not exist; a `completed/` or `archive/` ledger modified in the diff under check other than a
link repair (the "frozen" and "immutable" rules, enforced for the first time). Each failure is
a provocation proof in the unit's tests ([../../../docs/testing.md](../../../docs/testing.md)
"the gate goes red on a planted violation, then green").

### 3.4 The roadmap bins

```
task/roadmap/mid-term/    evaluated intakes awaiting charter — the two 2026-08 intakes and the fork handoff move here
task/roadmap/epic-term/   north-star tracks shaped like PROJECT.md roadmap items
```

Promotion is `ledger_lifecycle.py move`: epic → mid when an intake evaluates it, mid → a brief
under `briefs/` when the owner charters it (the brief is a new file; the intake is retired by
the rule it already states). `briefs/` stays the short-term home, STATUS the state home — the
bins add no third home for sequence.

### 3.5 Census eviction

`git rm -r task/census/` in its own commit. The pointer that replaces it — in
`docs/port/census.md` (the procedure's home) and the one STATUS line — names the last `main`
SHA that carries the directory, so the milestone-one evidence stays reachable by `git show
<sha>:task/census/…` and the comparator's "regenerate a comparable run" path is unchanged.
`.typos.toml`, the root `map.md`, `docs/history/map.md` and one ledger lose their links. Stated
plainly: this shrinks the checkout and the map surface, **not the pack** — history keeps the
bytes, and that is the point.

### 3.6 The skill, split

`compact-context-docs` keeps the judgement half (true up STATUS, replace restatements with
pointers, sweep stale lifecycle claims — scoped to the just-merged delta) and gains a mechanical
step 0: `make ledger-archive` (the script's `archive`), run before anything is read. Its
"Archive what closed" step points at the monthly archive for ledgers and keeps `docs/history/`
for campaign briefs and designs. The skill states that a mid-tier model is sufficient for the
scoped pickup ritual provided the orchestrator reviews the docs-only first commit — tier naming
stays in the adapters, per AGENTS.md "Delegated work".

## 4. The backfill (one PR, mechanical)

At `b13b22c`: 125 ledgers. Classification at execution time, recorded in the migration commit:

- **archive** — every ledger whose unit merged and whose retirement event (where it names one)
  has occurred. Expected ≈ 118. Archive date = the author date of the squash-merge commit that
  first placed the file on `main` (`--first-parent`, so the date is the merge, not the branch
  work). All land under `archive/2026-08/`.
- **staging** — charters whose campaign STATUS "Active workstreams" still lists as open
  (`mw-0-charter` until MW-5; `sem-0-charter` while HELD; the V3 / FNP / LRS charters per
  STATUS at execution). Each gets a dated `**Retires:**` line at its top if it lacks one — the
  one edit a frozen ledger may take, and it is the lifecycle rule's own requirement.
- **links** — 182 files rewritten by the script, none by hand; `task/map.md` drops from 137
  entries to the live ledgers plus one row per bin; the `docs/history/*/promotion-ledger.md`
  links are repaired (the archive rules allow link repair).

The migration is the script's first real run and its acceptance: `check` green before, the
planted-violation proofs red, `check` green after, `make ci` green, and `python3
scripts/sync_map_md.py --check` clean.

## 5. Commits, in order

1. This charter + the `task/ledgers/` maps (docs-only; this commit).
2. `scripts/ledger_lifecycle.py` + its tests (provocation proofs) + `make check-ledgers` /
   `make ledger-archive`, `check-ledgers` into `make ci`.
3. AGENTS.md ledger-class row + `docs/history/map.md` "archive a campaign" row — the contract
   edit, deliberate and alone in its commit.
4. The skill split (§3.6) + `.agents/skills/map.md` row.
5. Census eviction (§3.5).
6. The backfill (§4) — the script's run, plus the classification list in the commit message.
7. Roadmap bins (§3.4): the three 2026-08 task documents moved by `move`, pointers repaired.
8. Departure edit: this ledger `move`d to `completed/`, `briefs/next-sequence.md` and STATUS
   trued up for DL-1 only.

Size **S/M**: one script (~300 lines), one gate, six doc edits, one mechanical migration.

## 6. Out of scope

- Parquet or any binary consolidation inside the repository (§1).
- Archiving `briefs/` or `docs/design/` documents — they keep the `docs/history/` campaign
  path; only ledgers get the monthly archive.
- `task/fnp-0-census/` (292 KB) and `task/port/` (40 KB): campaign working records, not runs;
  they archive with their campaigns, not here.
- Any change to what a ledger *says* — format, sections, the SEPMO proposition ledger.

## 7. Later, if wanted

A derived dataset *outside* the tree — ledger path, unit, dates, PR, the `task/metrics.md`
row — built from the archive by a script into parquet, queryable with this engine. Derived, never
the source; it earns its own intake when the metrics questions are written down.
