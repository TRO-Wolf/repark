> **Errata 2026-08-25 (departure day):** §6 records `ci.yml` as unwired, an owner action. The owner
> granted a one-time scoped edit the same evening and the `check_docs_compaction` step is in the
> `guards` job on this branch. Nothing else in this record changes.

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

**`scripts/ledger_lifecycle.py compact`** — a new subcommand on the existing script, in the existing plan → verify-links → apply shape:

| Step | Behaviour | Reuses |
|---|---|---|
| Parse | read both files' blocks; refuse unbalanced, nested, or unknown markers and any Active-workstreams bullet outside a block | new (~40 lines) |
| Unit departure | for every ledger that entered `archive/` in the delta (`--base`, default: the archive move just planned), delete its `unit` block whole — row, prose, no residue | ledger id from the filename, as `archive` derives it |
| Workstream closure | for every `ws` with `state=closed` still in STATUS: cut the block, paste it under `docs/history/<campaign>/status-record.md` (create the bin + its `map.md` from the template if absent), rewrite every relative link, refuse on a dangling one, leave **one line** in a "Closed campaigns" list — name, dates, closing PR, history link | `cut_row` / `paste_row` / `rewrite_links` / `dangling` / `map_template` generalised from map rows to blocks |
| Lockstep | the touched directories' `map.md` rows | `sync_map_md` |

The git diff is never the **signal**: no regex over diff content infers state from prose.
*(Amended in flight, 2026-08-25: the charter's `--base` flag was not built — the tree's own state
is the scope. A unit has left when its ledger sits in `completed/` or the archive; a campaign has
left when its marker says so. That is simpler, idempotent, and needs no diff. Recorded, not
erased.)*

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
sentence) and the gate roster row; `.agents/skills/sepmo/binding-manifest.md` "Unit pickup /
departure" (the path after #238, which merges first); `scripts/map.md`.

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
| C-001 | The parser accepts the grammar in §2 and refuses an unbalanced block, a nested block, an unknown marker, a `closed` block missing `closed`/`by`/`history`, and an Active-workstreams bullet outside any block — each with the file and line. | Provocation tests, one per refusal. | PROVEN | `test_the_parser_refuses_a_malformed_document` (8 refusals, each `STATUS.md:<line>`), `test_a_bullet_outside_any_block_is_found_by_line`, `test_a_marker_inside_code_is_prose_not_a_marker` |
| C-002 | Archiving a ledger removes its `unit` block from the slate whole — row and prose — and leaves no residue; a second run changes nothing. | Scratch-tree test around `archive`. | PROVEN | `test_archive_makes_a_merged_unit_leave_the_slate_whole` (row + prose gone, `#` renumbered, second run changes nothing) |
| C-003 | A `ws` block with `state=closed` is cut from STATUS, lands in `docs/history/<campaign>/status-record.md` (bin and `map.md` created if absent), every relative link still resolves, and STATUS keeps exactly one line for it. | Scratch-tree test; `check-map-sync` on the result. | PROVEN | `test_a_closed_campaign_leaves_status_for_its_history_bin` (record, bin map, history row, links `../../`, one STATUS line after the wrapped row; `sync_map_md --check` clean) + the two Critic remediations `test_compact_refuses_cleanly_without_the_closed_campaigns_marker`, `test_two_closed_campaigns_sharing_a_bin_get_one_map_row` |
| C-004 | `compact` never modifies a file under `task/ledgers/` or `docs/history/` other than the `status-record.md` it appends to and the bin's `map.md`. | Test asserting the touched-path set; `check-ledgers` frozen findings clean. | PROVEN | `test_compact_touches_only_the_two_live_files_and_the_campaign_bin` (staged set exact; ledger and other history bytes unchanged); `check-ledgers` frozen rule clean at every commit |
| C-005 | `check-docs-compaction` fails on each of (a)–(d) in §2 and passes on the migrated tree; its runtime is measured and recorded, and it is wired into `make ci` (and pre-commit iff ≤ 0.2 s median, n=5). | Four red provocations, one green run, the timing. | PROVEN | `test_the_gate_is_red_on_each_class_and_green_on_the_compacted_tree` ((a)–(d) red, green after `archive`); n=5 timing 0.05 / 0.05 / 0.05 / 0.05 / 0.05 s → `make ci`, `make install-hooks`, `.pre-commit-config.yaml` (`58cbf9a`) |
| C-006 | The migrated tree: every closed campaign's diary is in its history bin; every deferred item in the old text has a home in the new tree (listed); "Known correctness issues" is a pointer; the slate carries no obituary; all links resolve; before/after bytes are recorded for both files. | Dry run on a clone, diff inspected, the deferred-item list, the byte table. | PROVEN | `test_the_tree_is_migrated`; `deab046` carries the table and every deferred item's home. Measured: STATUS.md 65,890 → 30,055 B (Active workstreams 35,808 → 9,042; Known correctness issues 10,799 → 5,465; the three increments 3,027 → 0, moved); `briefs/next-sequence.md` 26,731 → 5,107 B at migration, 5,594 B with the rule text |
| C-007 | `make ledger-archive` on the migrated tree is idempotent: archive → compact → check reports nothing to do and changes no file. | Two consecutive runs, `git status` clean between. | PROVEN | the idempotence assertion in the C-002 test (`pins:` cites C-007); on the tree: `make ledger-archive` twice → `nothing to archive`, `docs-compaction: clean`, `git status --porcelain` empty (§6) |
| C-008 | The rule text in §2 is in place — each named document states the behaviour once, no document restates another — and the maps are in lockstep. | Grep for the sentences; `check-map-sync`; the binding-manifest row. | PROVEN | `test_the_rule_text_is_in_place`; grep `compact` per file: AGENTS.md 3, slate 4, compact-context-docs 9, binding-manifest 1, skills map 3, scripts map 5 — each states its own sentence, none restates another (§6) |

Verdicts were `OPEN` at charter time and flipped at departure with the attestation below.
VERDICT: PASS (OPEN=0, REJECTED=0).

## 5. Execution shape (the unit, not this charter)

Commits, in order: pickup (archives the V3E-3 ledger; the delta compaction) → markers on both
files, no content change → `compact` + tests → the gate + ratchet seed (a placeholder ceiling
until the migration measures) → the migration on a clone, then the tree → rule text + maps →
departure. Each commit passes `make ci`; the migration commit carries C-006's table.
*(As run: the `compact` and gate commits landed as one, `58cbf9a` — see §6.)*

## 6. Execution record (2026-08-25, same day)

Commits on `feat/dl-4-live-doc-compaction`, base `c533d6a` (#239): `d498e7a` pickup (V3E-3
ledger archived; the slate still said "V3E-3 ships in this PR" — removed) → `a679352` markers,
no content change (10 `ws` blocks, 3 inline + 2 block `unit` markers) → `58cbf9a` compact + gate
+ tests → `b9b5b4a` map rows append in place (not the archive sort) → `6570b9c` + `b22a600` a
wrapped closed-campaigns row is one row, in the writer and in the coverage check (both found on
the migration's dry runs) → `deab046` the migration → `3121cd8` the rule text → the
ceilings re-seeded from the unit's final sizes (the next commit) → `c53fef0` a marker inside a code span is prose
(the slate's own rule text tripped the parser; the gate went red on its author, as designed) →
`4adea62` the Critic's three findings remediated → this departure.

**Dry run.** The migration is one script; it ran on a fresh clone twice (the first exposed
`6570b9c`/`b22a600`), diff inspected, `check-map-sync` / `check-ledgers` / `typos` clean,
idempotence checked on the clone (`archive` → nothing; gate clean; porcelain 0), then the same
script ran on the tree. Every deferred item in the cut text has a home: the three
`status-record.md` files (bullet text verbatim, links rewritten), `hardening-h1/increments-2026-08-15.md`,
the closed-campaigns list, the archive (20 obituaries #203..#235, the PYC / MW-5 / A13 appendices),
the registry (the disposed Known-issues families, one line each here), the trimmed bullets.

**Deviations, dated.** `--base` not built (§2 note). §5's commits 3 and 4 folded. `ci.yml` is not
wired — `.github/` is outside this unit's grant; the gate is in `make ci`, the hook and the
pre-commit config, and the CI dual-wire is an **owner action** exactly as #223 was for DL-1/DL-2.
One out-of-scope fix rode the rule-text commit because it sat in the just-merged delta: #239's
`#convergence-labels` anchor → `#convergence-labels-hard`.

**Not in scope, noted for the next pickup:** the four pre-existing registry anchor mismatches
(`G4-3`, `ST-1`, `NS-1` headings linked from two `map.md` files).

## 7. PRE_EXECUTION_REVIEW

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-PER-DL-4
  agent: Orchestrator
  action: execute PR-carved charter DL-4 (one PR unit) on feat/dl-4-live-doc-compaction
  charter_trace: C-001..C-008
  preconditions:
    - origin/main at c533d6a (#239) with #237 (this charter) and #238 merged: SATISFIED (gh pr view; git log)
    - pickup ritual run first (archive, drift checks, delta compaction as a docs-only commit): SATISFIED (d498e7a)
    - proportionality rubric recorded: SATISFIED (LIGHT fails criterion 3 — the migration moves ~40 kB across 13 files — so STANDARD; the bound engine CCC runs the Critic stage under the manifest's procedural in-session break)
    - binding manifest resolves every binding (green commands, critic_engine=CCC, severity_floor S1): SATISFIED (binding-manifest.md rows)
    - the owner's go-ahead: SATISFIED ("Go, start the DL-4 pickup and run it", 2026-08-25)
  success_condition: every C-001..C-008 PROVEN with a cited pin; make ci green; STATUS.md and the slate under their seeded ceilings; the frozen rule clean; no departure line for any unit anywhere
  step_risks:
    - silent loss of a deferred-work record (S0 class): HANDLED(closed blocks move verbatim; hand-trims diff-reviewed on a clone; C-006 lists every home)
    - a marker edits a ledger: HANDLED(frozen rule stays on the path; C-004 asserts the touched set)
    - marker discipline decays: HANDLED(coverage check + byte ceiling are gates, not conventions)
    - the migration script differs between clone and tree: HANDLED(one script file, run by path on both)
  contingencies:
    - a red gate after the migration: EXECUTABLE(additive — a further commit; no destructive step exists)
    - the migration proves wrong after merge: EXECUTABLE(additive — every cut is in git history and in its record; a revert commit restores STATUS)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## 8. The Critic pass — CCC under the procedural break

*Context break executed; attacking artifacts, not memory.* Inputs: the charter clauses, the diff
at `c53fef0`, the test run, the attack taxonomies — not the Actor's narrative. Risk tier
**standard** (behaviour-affecting scripts; no `crates/` touched, so the CRATE contract is N/A).
`claims_critic=true`. Five inputs freshly executed through the public entry point
(`ledger_lifecycle.py compact` on scratch repositories, none in the committed tests at the time):

| Probe | Input | Observed → expected | Result |
|---|---|---|---|
| P1 | `history=../probe-escape` | refused, but only by the dangling-link check; nothing written outside | **SEC-001** |
| P2 | STATUS with no `<!-- closed-campaigns -->` | `ValueError` traceback, exit 1 → should be a named refusal | **Q-001** |
| P3 | two closed blocks, one bin | two records ✓, **two** `status-record.md` rows in the bin map → one | **L-001** |
| P4 | `history=docs/history` (the bin root) | a record written at the root, a row on the root map → refuse | folded into SEC-001 |
| P5 | unit ids `x1` and `x10`, ledger `x10-thing` | `x1` kept, `x10` gone ✓ | null report |

**Critic-1 (quality / tests):** verdict NEEDS_REMEDIATION → CLEAN. Q-001 filed. Null reports:
plain classes not models (the hook runs the system interpreter — a recorded reason, not a
shortcut); every public name documented (`check-docstring-presence` clean); the mutation-proof
check — each regression test was observed red by execution (the probes) before its fix.
**Critic-2 (security / safety):** NEEDS_REMEDIATION → CLEAN. SEC-001 filed. Null reports: no
subprocess takes marker text; nothing is written unless every link resolves (plan → verify →
apply, inherited from DL-1); refusals leave the tree untouched (asserted).
**Critic-3 (logic):** NEEDS_REMEDIATION → CLEAN. L-001 filed. Null reports: prefix matching
(P5); renumbering only `#`-headed tables, non-digit first cells left alone; `_collapse` never
leaves a double blank; the wrapped-row handling in writer and coverage check alike (`b22a600`).
**Critic-4 (claims / record):** CLEAN. The byte counts in this ledger and in `deab046` re-measured
with `wc -c` (30,055 / 5,107 / 5,594); the obituary count (20) re-counted from the pre-migration
slate; every `pins:` citation resolves (`check-ledger-grammar`, 125 pinned ids); identity at
`%ae` across the branch equal to the repository's configured email — **attacked**; the charter's
`--base` prose vs the tree — filed as the dated §2 amendment rather than left to drift.

```yaml
FINDING:
  id: F-DL4-1
  severity: S2
  category: AT-5
  clause: C-003
  disposition: REMEDIATED
  claim: a closed block's history= could name a directory outside docs/history/ or the bin root; refusal was incidental (dangling link), not by rule
  evidence: 4adea62 — HISTORY_DIR grammar (docs/history/<name>); test_the_parser_refuses_a_malformed_document cases `../out` and `docs/history`; probe P1/P4 now REFUSED by rule, nothing written

FINDING:
  id: F-DL4-2
  severity: S2
  category: AT-3
  clause: C-003
  disposition: REMEDIATED
  claim: STATUS without a closed-campaigns marker → uncaught ValueError traceback instead of a refusal
  evidence: 4adea62 — compact_plan returns the cause as a finding; test_compact_refuses_cleanly_without_the_closed_campaigns_marker (no Traceback, porcelain empty)

FINDING:
  id: F-DL4-3
  severity: S3
  category: AT-6
  clause: C-003
  disposition: REMEDIATED
  claim: two closed blocks sharing a history bin appended the bin map's status-record row twice
  evidence: 4adea62 — row appended only when absent; test_two_closed_campaigns_sharing_a_bin_get_one_map_row

FINDING:
  id: F-DL4-4
  severity: S3
  category: AT-3
  clause: C-002
  disposition: ACCEPTED_FLAGGED
  claim: `archive` applies the ledger move and only then runs compact; a compact refusal leaves the archive staged with the slate un-compacted — a partial, visible state (the gate is red) rather than an atomic refusal
  evidence: by construction (run_archive → run_compact); below the floor; the gate makes it visible at once; flagged in the PR
```

```yaml
COVERAGE_ATTESTATION:
  pr_unit: DL-4
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        C-001..C-008 walked clause by clause against the tests and the tree; each clause's
        evidence cell names the test that cites it.
      artifacts: [python/repark-parity/tests/test_dl_4_live_doc_compaction.py]
    - id: AT-2
      status: ATTACKED
      evidence: >
        Eight parser refusals; a marker inside a code span and inside a fence; a wrapped list row;
        a bullet outside any block; the traversal, root-bin, missing-marker and shared-bin probes;
        the x1/x10 prefix probe; an empty-Contents map; a closed block last before the list.
      artifacts: [test_the_parser_refuses_a_malformed_document, test_a_marker_inside_code_is_prose_not_a_marker, "critic probes P1-P5 (§8)"]
    - id: AT-3
      status: ATTACKED
      evidence: >
        Every refusal path writes nothing (plan → verify links → apply; asserted by porcelain);
        the missing-marker path is a named refusal (F-DL4-2); the archive-then-compact partial
        state is F-DL4-4, flagged.
      artifacts: [test_compact_refuses_cleanly_without_the_closed_campaigns_marker, "F-DL4-4"]
    - id: AT-4
      status: N/A
      justification: a single-process, stateless text transform; writes are staged in one pass; no concurrency surface.
    - id: AT-5
      status: ATTACKED
      evidence: >
        Path traversal through history= (F-DL4-1) remediated by grammar; no marker text reaches
        a subprocess; git is the only subprocess and takes fixed arguments plus tracked paths.
      artifacts: ["4adea62", test_the_parser_refuses_a_malformed_document]
    - id: AT-6
      status: ATTACKED
      evidence: >
        Truth moves, it is never deleted: cut content lands verbatim in its record (asserted),
        links rewritten and verified before any write; the frozen rule held at every commit;
        C-006 lists each deferred item's home; L-001 (duplicate map row) remediated.
      artifacts: [test_a_closed_campaign_leaves_status_for_its_history_bin, test_compact_touches_only_the_two_live_files_and_the_campaign_bin, "deab046"]
    - id: AT-7
      status: N/A
      justification: the gate runs in 0.05 s and the unit strictly reduces bytes on the read path; nothing system-breaking is reachable.
    - id: AT-8
      status: ATTACKED
      evidence: >
        The lifecycle contract change is declared, not smuggled — archive and a move to
        completed/ now end in compact — in the script docstring, scripts/map.md, the ritual,
        the slate's rule 7 and the manifest row; the DL-1 and DL-2 suites still pass (24 + 11).
      artifacts: [scripts/map.md, "python/repark-parity/tests/test_dl_1_ledger_lifecycle.py: 24 passed"]
    - id: AT-9
      status: ATTACKED
      evidence: >
        Every parser finding names file and line; every gate finding names the file, the block or
        the byte count and the command that fixes it; compact logs what left and where it went.
      artifacts: [test_the_gate_is_red_on_each_class_and_green_on_the_compacted_tree]
    - id: AT-10
      status: ATTACKED
      evidence: >
        Every PROVEN clause is cited by at least one test (check-ledger-grammar); the three
        Critic findings were red by execution before their fix and are pinned; the tree pins
        C-006 and C-008 read the real files, not fixtures.
      artifacts: [python/repark-parity/tests/test_dl_4_live_doc_compaction.py, "make check-ledger-grammar: 125 pinned clause ids"]
  reattested: [AT-3, AT-5, AT-6]
  complete: true
```
