# Architecture-scoped maps

**Class:** contract (AGENTS.md "Markdown document lifecycle"); amended deliberately. **Adopted:** 2026-10-29, by the owner's word on the map policy evaluation. **Built by:** MAP-GOVERNANCE-1 (card [task/roadmap/mid-term/map-governance-1-card-2026-10-29.md](../task/roadmap/mid-term/map-governance-1-card-2026-10-29.md); ledger [task/ledgers/staging/map-governance-1-ledger.md](../task/ledgers/staging/map-governance-1-ledger.md)).

## Purpose

A `map.md` routes a reader to the document or code that owns a fact. It is a navigation aid, not a record. Before this policy every unit that pinned a ledger clause edited a map, and every executor read the maps it touched first. The maps held 5,405 pin citations at `aa52b585`, and a single unit's mandatory map reads cost about 105k tokens. Pins now live in ledger sidecars (below), and maps route.

## Where a map lives

- A `map.md` sits at a repository root, a crate, a package, or a complex subsystem boundary where it routes a task, names an owner or a boundary, or names the verification.
- A new directory does not need a map, and neither does a leaf of obvious or generated files. A new directory with a map is still allowed.
- Hand-written: no generator writes map prose. The one script that writes map files is `scripts/ledger_lifecycle.py`, which creates and edits the bin, archive and history maps (see "Ownership").

## What a map holds

- Routes, owners, boundaries and the verification for its directory.
- Not history. History lives in ledgers, PRs or `docs/history/`, never in a map.
- Transition: a map that still holds dated history is not wrong until its ratchet row is lowered. Lowering the row is the moment the history must leave.
- Not pins. A clause's pin is a sidecar line, never a `pins:` citation in a map. The migration removed 2,650 citation lines from 231 maps and wrote no map prose.

## Ownership

- `scripts/ledger_lifecycle.py` creates and edits the bin maps (`task/ledgers/staging/map.md`, `completed/map.md`), the archive maps (`archive/map.md` and the month maps), and `docs/history/<dir>/map.md` in `compact`. It moves rows between the staging and completed bin maps, and moves each excepted size row when it rewrites links.
- A unit's sidecar moves with its ledger on `move` and `archive`. The archive sidecar takes its ledger's `yyyy-mm-dd-` prefix. A sidecar in `completed/` or the archive is not frozen; a ledger row is.

## Verification

- Lockstep warns. `scripts/check_map_md.sh` prints `WARNING:` when a touched directory's map is not touched. It exits 0 in both modes, staged and branch.
- `scripts/sync_map_md.py --check` (`make check-map-sync`) gates link validity and duplicate rows. Coverage is reported only under `--strict`.
- `scripts/check_map_size.py` (`make check-map-size`) gates size. It runs in `make ci` and in the ci.yml guards step on pull requests.

## The size ratchet

- A tracked `map.md` may hold 200 lines and 16 KiB (16,384 bytes). Crossing either fails.
- `EXCEPTIONS` in `scripts/check_map_size.py` records the exact `(lines, bytes)` of each map that was over a ceiling when the ratchet armed. An excepted map fails when a measure exceeds both its row and its size at `git merge-base HEAD origin/main` (the row alone with no merge base, or when the map is absent there), so growth that arrived on `main` is not the branch's. Shrinkage below the row fails until the row is lowered.
- `--update-baselines` lowers a shrunk row, deletes a row once its map is within both ceilings, and raises a row to the tree's size where `main` grew the map, never above the merge-base size. It refuses a new violator and growth past both measures, and writes nothing then. The way out of a violation is a split or a condensed map.
- **Push to `main`.** On a push to `main` HEAD is `origin/main`, so the merge base is HEAD and the rule cannot red there; the pull-request run holds it (ci.yml's guards checkout is `fetch-depth: 0`).
- `EXEMPT` and `EXEMPT_PATTERNS` list the lifecycle inventories, with a reason each: the three bin maps, and the archive month maps (`task/ledgers/archive/*/map.md`). The lifecycle script rewrites them on every `move` and `archive`.

## Pin sidecars

- A ledger's clause pins live in `task/ledgers/<bin>/<unit>-pins.md`, beside the ledger. The bin is `staging`, `completed` or `archive`.
- One line per clause: `C-NNN: <path>[::<name>], ...` or `C-NNN: none (records)`.
- `none (records)` means no map citation and no row-named test binds it; the gate's `--report row-named-records` lists any such line whose row names a defined test. The gate counts and lists it. A rerun never replaces an on-disk records line except by the row-named pass: a row that backticks a test which exactly one bindable file defines binds that file and the name, and the grammar gate fails when a PROVEN clause's row names such a test and no binding names its file and the name, so a hand records line over such a row fails the gate.
- A sidecar counts only beside its own ledger. A unit in two bins is refused by `ledger_lifecycle.py check`.

## How the grammar gate reads sidecars

`scripts/check_ledger_grammar.py`, rule B, reads each sidecar line:

- A path binds only if it is a tracked `.rs`, `.py` or `.sh` test or script. A map, a ledger, a sidecar, a licence or readme never binds. A fixture binds the test that reads it, else the clause records. A test-placed helper with no test never binds. A `_record*` file never binds.
- A `::name` binds only if the bound file defines it: Python by `ast`; Rust with comments, strings and character literals blanked; shell by function definition. A definition inside a string, a heredoc or a comment binds nothing.
- The line must name a clause of the ledger beside it. A name the file does not define, or a path that is not tracked, goes red.
- Pins in test and script sources (`pins:` citations) also count. A `pins:` citation in a map or in production source binds nothing.
- **Intent.** A `PROVEN` clause whose own row backticks a test that exactly one bindable file defines must bind that test by name: `<file>::<name>` on its sidecar line, or a `pins:` citation in that file. A path-only line does not satisfy it, so renaming that test reds the gate. Reading units (`**Path:**` valued `READING`) are exempt.

## Migration

`scripts/migrate_pins_to_sidecars.py` moved the citations once. It is idempotent: a rerun writes nothing, `--check` writes nothing and exits 1 when a rewrite is pending, and `--report hand-candidates` lists the disclaiming sentences that name no clause id, for a hand `none (records)` line. A disclaiming sentence unbinds only the clause ids it names explicitly. A "proves … only" sentence unbinds nothing and is reported. A row-named pass then binds each PROVEN clause to a test its own row backticks when exactly one bindable file defines that name, overriding a records line; a name that two or more files define is listed as an ambiguous row-named test for a hand line. A citation in an indented dated paragraph or a nested sub-bullet of a test file's item binds that item's file per clause only when the clause's paragraph or row backticks a name that file defines.

## Open

- The owner-ruling adjustment names the pin sidecar as the binding's home. The owner amended it on 2026-10-30, and the orchestrator committed the amendment in `AGENTS.md`, `CLAUDE.md` and `scripts/check_owner_ruling.py` together as `279bddb6` (ledger C-011).
- Citation text left in map prose is inert (ledger C-012). The gate no longer reads it; each such citation binds through its sidecar.
