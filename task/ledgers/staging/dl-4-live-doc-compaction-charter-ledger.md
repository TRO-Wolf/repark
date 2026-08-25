# Charter ledger — DL-4 · the live documents carry only live state

**Date:** 2026-08-25 · **Branch:** `docs/dl-4-charter` (this charter) then `feat/dl-4-live-doc-compaction` (the
unit) · **Base:** `8083be5` (`main`, #236) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md)
"Markdown document lifecycle" · **Changes:** `scripts/ledger_lifecycle.py` (a DL-1 surface) + one
new gate + one migration + the lifecycle rule text · **SEPMO path:** LIGHT eligible (docs +
one script; no engine code) — the orchestrator reads the rubric at pickup · **Size:** M

**Retires:** this ledger moves to `../completed/` in the unit's last commit; the first pickup after
the merge archives it. **Sequencing:** ahead of V3E-4 — every agent V3E-4 spawns onboards on the
files this unit shrinks; the owner's merge of the change that added this file is the ruling.

## 0. The measurement (2026-08-25, at `8083be5`, tokens ≈ bytes / 4)

An outside agent walked the faithful `/sepmo-core` read sequence for a fresh work group and
found ~97k tokens before a ledger could be written, ~35k of it live signal. Re-measured here:

| File | Bytes | The live part | The rest |
|---|---|---|---|
| `STATUS.md` | 65,183 | Release, delivered, deferred, blockers, the open tracks (~20 kB) | "Active workstreams" is 35,808 B under **one H2 with no sub-headings**; six of its ten campaign bullets are closed (DL-1..3's diary, PYC, LRS, MW "closed by MW-5" then MW-6..9, H-1's residue, three 2026-08-15 increment H2s). "Known correctness issues" (10,799 B) restates the divergence registry — a single-home violation. |
| `briefs/next-sequence.md` | 25,842 | Standing rules (1,327 B) + the queue table + Lane A (~5 kB) | 20+ "*X merged as #N and left this file*" paragraphs (the unit left; its obituary stayed) + 9.6 kB of PYC / MW-5 / A13 appendices whose ledgers are archived. |

The cause is the pickup ritual as practised: [compact-context-docs](../../../.agents/skills/compact-context-docs/SKILL.md)
says "true up STATUS.md first," but scoped mode is bounded to the just-merged delta, so every
pickup *appended* a departure line and none removed the closed material — a whole-file
compaction is never anyone's unit. DL-3 did exactly this for the archive month maps
([record](../archive/2026-08/2026-08-23-dl-3-archive-map-compaction-charter-ledger.md)); this
unit does it for the two live files and makes the mechanism run at pickup so the files cannot
regrow unnoticed.

**Why this is a per-agent cost, not a per-session one.** AGENTS.md "Read first" + the engineering
method are paid by every Actor and Critic a SEPMO unit spawns, not once per session. A 40 kB
saving on the shared path is worth 40 kB × (orchestrator + every subagent).

## 1. Ruling (owner, by the merge of this file)

1. **Live documents carry live state.** A merged unit's record is its archived ledger and its
   PR; `STATUS.md` and the slate carry **no obituary**. A closed campaign's diary moves to
   `docs/history/<campaign>/` — the lifecycle rule "truth moves, it is never deleted" applies
   to the *record* (DL-3's reading), and the move is mechanical.
2. **Closure is declared, never inferred.** "Every ledger with this prefix is archived" does
   not mean a campaign is closed — MW was ruled closed by MW-5 and then ran MW-6..9. A block
   closes when its marker says `state=closed`, set by the departure edit under an owner
   ruling; the script enforces the consequence, it does not decide it.
3. **Markers are HTML comments, not XML elements.** `<!-- … -->` renders as nothing on
   GitHub, is legal markdown everywhere, already appears in the tree's generated `map.md`
   files, and is exactly as parseable. DL-2 measured XML as a ledger carrier and declined it;
   the same reasoning holds here.
4. **The byte ratchet is the load-bearing guard.** Markers make compaction mechanical; the
   ratchet is what makes regrowth visible regardless of marker discipline.

## 2. Design

**Block grammar** — one marker line opens a block, one closes it; no nesting.

```markdown
<!-- ws id=mw ledgers=mw- state=closed closed=2026-08-23 by=#224 history=docs/history/iceberg-maintenance-wave -->
- **Iceberg maintenance wave (MW)** …
<!-- /ws -->

<!-- unit id=v3e-4 ledger=v3e-4-refs-time-travel -->
| 1 | **V3E-4** — … |
…the "why" paragraph for V3E-4…
<!-- /unit -->
```

`ws` blocks wrap every top-level bullet under `STATUS.md` "Active workstreams"; `unit` blocks
wrap each queue row and its reasoning in `briefs/next-sequence.md`. Keys are `key=value`,
space-separated; `ledgers=` is a comma list of ledger-filename prefixes; `state` ∈ {`open`,
`held`, `closed`}; `closed`/`by`/`history` are required iff `state=closed`.

**`scripts/ledger_lifecycle.py compact [--base SHA]`** — a new subcommand on the existing
script, in the existing plan → verify-links → apply shape:

| Step | Behaviour | Reuses |
|---|---|---|
| Parse | read both files' blocks; refuse unbalanced, nested, or unknown markers and any Active-workstreams bullet outside a block | new (~40 lines) |
| Unit departure | for every ledger that entered `archive/` in the delta (`--base`, default: the archive move just planned), delete its `unit` block whole — row, prose, no residue | ledger id from the filename, as `archive` derives it |
| Workstream closure | for every `ws` with `state=closed` still in STATUS: cut the block, paste it under `docs/history/<campaign>/status-record.md` (create the bin + its `map.md` from the template if absent), rewrite every relative link, refuse on a dangling one, leave **one line** in a "Closed campaigns" list — name, dates, closing PR, history link | `cut_row` / `paste_row` / `rewrite_links` / `dangling` / `map_template` generalised from map rows to blocks |
| Lockstep | the touched directories' `map.md` rows | `sync_map_md` |

The git diff is the **scope** (`--base` bounds the delta, as `check` already does) and never
the **signal**: no regex over diff content infers state from prose.

**`scripts/check_docs_compaction.py` / `make check-docs-compaction`** — in `make ci` and, if
its measured time allows, the pre-commit hook: (a) no `state=closed` block remains in STATUS;
(b) no `unit` block whose ledger sits in `completed/` or `archive/`; (c) coverage — every
top-level Active-workstreams bullet is inside a `ws` block; (d) a **byte ceiling** on
`STATUS.md` and `briefs/next-sequence.md`, seeded from the post-migration measurement, raised
only by an explicit edit in the PR that needs it (the PYC-6 ratchet pattern).

**`make ledger-archive`** becomes archive → compact → check, still "pickup step 0, zero tokens".

**The migration (one-time, on a clone first, diff inspected):** mark every block; declare
closed — PYC (last unit #216), LRS (delivered), MW (ruled closed by MW-5, #224; MW-6..9 recorded
in its history record), the three 2026-08-15 increment H2s (→ `hardening-h1/`); new history
bins `pyc/`, `lrs/`; DL, SEM (held), Format-v3, perf, FNP, H-2, dbt stay `open`/`held` and are
trimmed by hand to their live state inside the same two sections and nowhere else; "Known
correctness issues" becomes a pointer to the registry plus the existing "Release blockers" H2;
the slate's obituary paragraphs and the PYC / MW-5 / A13 appendices are deleted (their ledgers
are archived; `briefs/map.md` keeps one sentence pointing at the archive). Byte counts before
and after are **measured and recorded**, not promised — DL-3's ≈15 kB guess was wrong and the
measurement stood.

**Rule text** — the smallest edits that make the behaviour a rule: `compact-context-docs`
pickup step 2 (archive → compact → check) and a "delete, don't narrate" gotcha; the slate's
"Rolling slate" line and standing rule 7; AGENTS.md "Markdown document lifecycle" (one
sentence) and the gate roster row; `skills/sepmo/binding-manifest.md` "Unit pickup /
departure"; `scripts/map.md`.

**Out of scope.** Generating "Known correctness issues" from the registry (declined: a pointer
is enough and a generator is a second home); any ledger's content; `AGENTS.md` /
`engineering-method` overlap (its own small unit — it touches the engineering contract); the
outside agent's own skill wrapper (not in this tree).

## 3. Risk, first

- **S0 — silent loss of a deferred-work record** (the compact-context-docs gotcha). Closed
  blocks *move*; the hand-trims of open blocks are diff-reviewed on a clone against the
  rule "every deferred item still has a home"; C-006 carries the check.
- **S1 — a marker edits a ledger.** The frozen rule (`_frozen`, `check-ledgers`) stays on the
  path; C-004.
- **S2 — marker discipline decays.** Coverage check (c) and the ratchet (d) are gates, not
  conventions; C-005.
- **Retired at charter time:** "HTML comments trip the hooks" — the tree already carries them
  in generated maps under `typos`, the map guards and the forbidden-pattern hook.

## 4. Proposition ledger — DL-4

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The parser accepts the grammar in §2 and refuses an unbalanced block, a nested block, an unknown marker, a `closed` block missing `closed`/`by`/`history`, and an Active-workstreams bullet outside any block — each with the file and line. | Provocation tests, one per refusal. | OPEN | *what closes it:* the named tests red-then-green |
| C-002 | Archiving a ledger removes its `unit` block from the slate whole — row and prose — and leaves no residue; a second run changes nothing. | Scratch-tree test around `archive`. | OPEN | *what closes it:* the test, and the idempotence assertion in it |
| C-003 | A `ws` block with `state=closed` is cut from STATUS, lands in `docs/history/<campaign>/status-record.md` (bin and `map.md` created if absent), every relative link still resolves, and STATUS keeps exactly one line for it. | Scratch-tree test; `check-map-sync` on the result. | OPEN | *what closes it:* the test plus the map-sync run |
| C-004 | `compact` never modifies a file under `task/ledgers/` or `docs/history/` other than the `status-record.md` it appends to and the bin's `map.md`. | Test asserting the touched-path set; `check-ledgers` frozen findings clean. | OPEN | *what closes it:* the touched-path test |
| C-005 | `check-docs-compaction` fails on each of (a)–(d) in §2 and passes on the migrated tree; its runtime is measured and recorded, and it is wired into `make ci` (and pre-commit iff ≤ 0.2 s median, n=5). | Four red provocations, one green run, the timing. | OPEN | *what closes it:* the provocations and the recorded time |
| C-006 | The migrated tree: every closed campaign's diary is in its history bin; every deferred item in the old text has a home in the new tree (listed); "Known correctness issues" is a pointer; the slate carries no obituary; all links resolve; before/after bytes are recorded for both files. | Dry run on a clone, diff inspected, the deferred-item list, the byte table. | OPEN | *what closes it:* the migration commit's message carrying the table and the list |
| C-007 | `make ledger-archive` on the migrated tree is idempotent: archive → compact → check reports nothing to do and changes no file. | Two consecutive runs, `git status` clean between. | OPEN | *what closes it:* the dry-run record |
| C-008 | The rule text in §2 is in place — each named document states the behaviour once, no document restates another — and the maps are in lockstep. | Grep for the sentences; `check-map-sync`; the binding-manifest row. | OPEN | *what closes it:* the grep transcript in the departure commit |

Verdicts flip at departure with the attestation, per the grammar (`make check-ledger-grammar`).

## 5. Execution shape (the unit, not this charter)

Commits, in order: pickup (archives the V3E-3 ledger; the delta compaction) → markers on both
files, no content change → `compact` + tests → the gate + ratchet seed (a placeholder ceiling
until the migration measures) → the migration on a clone, then the tree → rule text + maps →
departure. Each commit passes `make ci`; the migration commit carries C-006's table.
