# Card MAP-CURRENCY — maps checked like a build target (2026-09-20)

**Status:** intake, written at the owner's request on 2026-09-20. **Step 1 is chartered** (the owner
asked for its work order, §5). Steps 2 and 3 wait for an owner ruling on §6.

**Why now:** the campaign is moving orchestration and execution to cheaper models. The map rule is
one of the places where every agent, of every tier, spends tokens on work a script can do — and
where parallel lanes collide.

## 1. What the maps cost today — measured on main, 2026-09-20

| Measure | Value |
|---|---|
| tracked `map.md` files | 310 |
| their total size | 3.06 MB (about 765k tokens) |
| the largest, `python/repark/tests/map.md` | 589 KB (about 150k tokens) |
| maps between 79 KB and 107 KB | seven, among them `crates/repark-spark/src`, `crates/repark-core/src`, `crates/repark-functions/src` |
| map touches in the last 150 commits on main | 864 (about six per commit) |
| map lines among all changed lines in those commits | 12,910 of 373,407 (3 %) |

Four findings follow from the numbers and from the campaign's run reports.

1. **Reading is the cost, not writing.** An agent adds a row by opening the map first. Opening the
   tests map costs about 150k tokens; the row it adds is a few dozen. Writing is 3 % of changed lines.
2. **The rule fires per commit.** [scripts/check_map_md.sh](../../../scripts/check_map_md.sh) runs in
   the pre-commit hook over *staged* files, so every work-in-progress commit needs a staged map
   edit. One open unit carries 52 commits. Many map edits exist only to pass the hook.
3. **The rule is not checked at pull-request level at all.** The `map.md guard` step in
   [ci.yml](../../../.github/workflows/ci.yml) runs the same script, and in CI nothing is staged, so
   the step passes on every tree. A clone without the hook installed can merge code with no map change.
4. **Maps are a standing source of merge conflicts.** Every lane that works in one directory edits
   one map. Orchestrators wrote five separate one-off "union" scripts that keep both sides of an
   append-style conflict on rebase — maps, ledgers and the parity registry are what they were run
   on — and ready pull requests have waited for hours while main moved under them. How many of those
   conflicts were maps was not counted; step 1's measurement (§4) counts it.

A fifth observation, from reading the largest maps: "touch the map in every commit" invites a
paragraph about *what this commit did*. That is history — git and the ledgers already hold it — and
it is why a test directory's map reached 589 KB.

## 2. The target: "current for these inputs", not "touched"

A map has two parts with different owners.

| Part | Owner | Content |
|---|---|---|
| **navigation** | a script | one row per file and per child directory, sorted, each with the short blob id of the file the row was last reviewed against |
| **explanation** | an agent | the directory's purpose, ownership, invariants, failure behaviour, where a change belongs |

The comment ban makes the explanation permanent: code carries no prose, so the map is the only home
for it. Only the navigation can be compiled.

**Staleness is tracked per row.** A row is *stale* when its file's blob id differs from the id in
the row; *missing* when a file has no row; *dead* when a row has no file. A script lists all three
without reading any prose. Two pull requests then collide in a map only when they changed the same
source file — and those collide anyway. A single receipts file, or one fingerprint per directory,
would recreate today's hotspot and is rejected for that reason.

**The implementing agent writes nothing in the map.** The one fact a script cannot infer — *did an
invariant or an ownership rule change?* — is one field in the hand-back file every executor round
already writes: `invariant_change: none`, or one sentence of at most twenty words. That keeps the
owner's checkpoint ("stop and say what you changed") at the price of a few tokens.

**A clerk-tier agent writes the prose**, from a packet a script assembles: the stale rows, the
diffs of exactly those files, the hand-back field, and the explanation section only when
`invariant_change` is not `none`. It never opens the rest of the map. It may answer `UNCHANGED` for
a row; the script then refreshes the blob id and nothing else.

**CI verifies currency for the branch diff**: no stale, missing or dead row in any directory the
branch touched, no duplicate row, every link resolves, and the size gate (§4) holds.

## 3. Structured data leaves the prose

`pins: <unit>/C-NNN` citations live in maps today because the ledger-grammar gate reads every
tracked file under `crates/`, `python/` and `scripts/`. They are structured data. Step 2 moves them
to a sorted per-directory sidecar (`pins.toml`) that the same gate reads: deterministic, mergeable,
and a large block out of the biggest maps. The gate's reading rule is the only thing that changes;
no pin is lost.

## 4. Steps

| Step | Content | Migration | Risk |
|---|---|---|---|
| **1** (chartered, §5) | the lockstep rule moves from per-commit to per-pull-request and becomes real in CI; the hook warns; `merge=union` for local rebases; a duplicate-row check | none | low |
| **2** | the two-part format with per-row blob ids; the generator for navigation; `pins.toml`; a size gate (a map over the ceiling fails; ceilings seeded from the tree and only ratchet down); history paragraphs move to the ledgers | all 310 maps, one clerk job per top-level directory, in a window with few open pull requests | medium — it conflicts with every open branch |
| **3** | `make docs-sync` as the standard last step before a pull request: list stale rows → clerk patches → verify | none | low |

Measure before step 2 is chartered: map touches per merged pull request and clerk-rebase rounds per
pull request, over the week after step 1 lands, against the §1 baseline.

## 5. Step 1 — work order MAP-PR-GATE-1

**Executor tier:** Devin or Muse. **Critic:** Grok. **One pull request.** No Rust, no build.

**Goal.** A directory's map must change in the same *pull request* as its code, and CI must really
check it. The pre-commit hook stops blocking work-in-progress commits.

**Files to touch**

1. `scripts/check_map_md.sh` — add a branch mode: `check_map_md.sh --base <ref>` reads
   `git diff --name-only --diff-filter=d <ref>...HEAD` for the code list and
   `git diff --name-only <ref>...HEAD` for the map list, and applies the existing rule unchanged
   (same suffix list, same root-level `map.md` case, same "directory has no map.md" error). With no
   argument it keeps reading the staged set but **exits 0** and prefixes each finding with
   `WARNING:` plus one line naming the CI check that will hold it.
2. `.github/workflows/ci.yml` — the `map.md guard` step runs
   `bash scripts/check_map_md.sh --base "origin/${{ github.base_ref }}"` on pull requests (the
   checkout needs enough history for the merge base: fetch the base ref explicitly, do not switch
   the whole job to a full clone without measuring the time). On a push to main the step is skipped.
3. `Makefile` — `check-map-md` gains `BASE ?= origin/main` and calls the branch mode; the
   `install-hooks` recipe and `.pre-commit-config.yaml` keep calling the staged mode (now a
   warning). The two hook paths stay identical to each other.
4. `.gitattributes` (new) — `map.md merge=union` and `**/map.md merge=union`.
5. `scripts/sync_map_md.py` — a third unconditional rule: within one map, two list rows whose
   *first* link has the same target are a finding (`duplicate row`). `--fix` never resolves it.
   This is the guard for what a union merge leaves behind when two branches edited one row.
6. `AGENTS.md` — the bullet "`map.md` in every directory, updated in the same change" names the
   pull request as the unit of "same change", names CI as the enforcement and the hook as a warning;
   the lifecycle table row for **navigation** says "in the same pull request". `DEVELOPMENT.md`,
   `CONTRIBUTING.md` and the root `map.md` rows that describe the guard follow. Do not restate the
   rule anywhere new.
7. Tests beside the existing script tests (find them through `scripts/map.md`): the branch mode
   passes when code and map both changed, fails when only code changed, fails on a new directory
   with no map, ignores a deletion-only change, and handles a root-level file; the staged mode
   exits 0 and prints the warning; the duplicate-row rule fires on a two-row fixture and stays quiet
   on a row that merely mentions the same file twice.

**Design rulings already made**

- "Same change" means the pull request. Main is squash-merged, so one pull request is one commit
  on main, and the contract's wording survives.
- The hook warns; it never blocks. The reason is measured in §1 (finding 2).
- `merge=union` helps **local** merges and rebases only. GitHub's server-side mergeability check is
  expected to ignore it — **measure this** with a throwaway pair of branches in a scratch clone and
  record the answer in the ledger; the campaign rebases locally before every merge, which is where
  the conflicts cost time.
- No map format change, no sorting, no generator in this step. The repo has 310 hand-written maps;
  step 2 owns their migration.

**Must be true at the end**

- `make check-map-md` fails on a scratch branch that changes a `.rs` file without its map, and
  passes once the map changes — in a later commit of the same branch.
- The CI step fails on such a pull request (prove it on a draft pull request, then close it).
- `python3 scripts/sync_map_md.py --check` is clean on main's tree with the new rule armed; if
  main already holds duplicate rows, list them in the ledger and fix them in this pull request.
- Every guard the repo runs on documentation stays green: map, map-sync, docs-links,
  docs-compaction, manifest, typos.

**Do not touch** any map's content beyond the rows this change itself requires; the ledger-grammar
gate; `check_manifest.py`; any Rust or Python package source.

**House rules.** No code comments in any source file, new or moved — shell, Python, YAML and the
`.gitattributes` file included; the explanation goes in `scripts/map.md`. Commit after every step.
Write the hand-back file last, with `invariant_change` filled in (this unit changes one: the unit
of the lockstep rule).

## 6. Owner rulings needed before step 2

1. **The contract change.** AGENTS.md says maps are hand-written and that there is no generator.
   Step 2 introduces one for the navigation part. *Recommendation: yes — the explanation stays
   agent-written, and that is where the value is.*
2. **History leaves the maps.** Paragraphs that describe what a unit did move to that unit's
   ledger; the map keeps what the directory *is*. *Recommendation: yes, with the size gate as the
   ratchet that keeps it so.*
3. **`pins.toml` sidecars** replace pin citations in map prose. *Recommendation: yes; the
   ledger-grammar gate changes its reading rule in the same pull request, so no pin is ever unread.*
4. **Who writes map prose.** *Recommendation: the clerk tier, never an orchestrator and never an
   Opus executor; the implementing agent reviews the clerk's diff only when `invariant_change` is
   not `none`.*
5. **When to migrate.** The migration conflicts with every open branch. *Recommendation: a window
   with at most two open non-draft pull requests, one clerk job per top-level directory, merged in
   one day.*
