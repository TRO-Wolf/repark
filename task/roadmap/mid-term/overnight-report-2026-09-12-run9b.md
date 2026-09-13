# Overnight report — run 9b (orchestrator B), 2026-09-12

**Outcome:** list exhausted at 02:35 local, ahead of the 06:30 stop. **3 product/docs PRs merged** (#547, #551, #556), **0 lanes parked**, one owner-question set (DYNCFG-1 DQ-1..DQ-7) and one parked key (CFG-2 D-5). Worker cost: Devin SWE-2 free tier (5 rounds, 400 agent steps); Grok S2-21 reviewers $0.95 (2 rounds). No Muse, no GLM, no live Spark, no JVM.

**Orchestrator:** Claude Opus 5 (medium), beside run 9 · **Grants:** G-1 yes, G-2 yes, G-3 06:30 local or list exhausted, G-4 Devin (swe-2-high) actors, Grok for S2-21 reviewers only, G-5 yes · **Base:** `origin/main` `0233d96f` · **Scope:** CFG-2 named sources (one PR per step), then the DYNCFG-1 intake (docs PR).

## Lanes

| Lane | Unit / step | Tier | Rounds | PR | Result |
|---|---|---|---|---|---|
| b-cfg2 | CFG-2 step 1 — Rust seam, load refusal retired | Devin I | 3 on one session (121 + 68 + 100 steps) + 1 Grok reviewer ($0.40) | #551 | **merged** `25707c46`, tree-equal |
| b-cfg2s2 | CFG-2 step 2 — Python door, config mirror, ledger departs | Devin I | 1 (79 steps, 44 min) + 1 Grok reviewer ($0.55) | #556 | **merged** `8f710b6a`, tree-equal (fourth update-branch; one CI red fixed: the inventory pin 928 → 930) |
| b-dyncfg | DYNCFG-1 intake — card set from PROFILES-1 | Devin reading | 1 (32 steps, 11 min) | #547 | **merged** `fd51dd34`, tree-equal |

## Decisions taken under §6 (G-2)

- CFG-2 rounds: step 1 Rust + guide constraint 1; step 2 Python door + ledger departure (a step order inside one card).
- CFG-2 D-1: the refusal names roadmap **1.10**, not the card's v1.6 — the release roadmap renumbered database connections 1.6 → 1.10 on 2026-09-03.
- CFG-2 D-2..D-6 recorded on the ruled card (roadmap-design-plan) on the step-1 branch.

- CFG-2 step-1 audit findings sent back (§4): F-1 three `/// # Errors` doc comments → `#[allow(clippy::missing_errors_doc)]`, the repo's 32-site precedent; F-2 `CREATE TABLE` / `DROP TABLE` under a source name must answer the D-1 message, not DataFusion's generic refusal; F-3 verify every catalog-registration path refuses a source name.
- S2-21 for CFG-2 step 1: one Rust reviewer only — the Python diff is a single pin flip ("beyond pins" not met).
- CFG-2 F-2 accepted outside Home: `crates/repark-core/src/pre_execute.rs` gains one call to `named_sources::refuse_source_ddl`. DataFusion's `drop_table` swallows `deregister_table` errors into "doesn't exist", so the shared guard is the only place the D-1 message can surface; the F-3 sweep also found SQL `CREATE CATALOG` silently replacing a source provider, now refused.
- S2-21 P2-1 remedy revised to `HashMap<String, Arc<SourceSpec>>` (not a name set): after F-2 the DDL guard reads the stored spec, so the reviewer's premise that the values go unread no longer holds; the `Arc` keeps the per-query snapshot to refcount bumps.
- CFG-2 step 2 decisions D-7..D-12 (orchestrator, G-2): `sources()` returns `SourceMetadata` namedtuples beside `CatalogMetadata` (the PySpark `listCatalogs` idiom); `source(name)` returns a `NamedSource` handle whose `ping()` raises the mapped `UnsupportedOperationException`; the pyo3 door lives in its own module; `auto_register` is a typed `StrictBool` on the mirror; `listCatalogs()` measured unchanged (only `spark_catalog`); the guide gains a subsection. The public shape of `SourceMetadata` / `NamedSource` is the owner's to overturn.
- CFG-2 step 2 accepted outside Home: `_promote_active` and `_builder_config_get_master` moved byte-for-byte into `session_state.py` / `session_configuration.py` (through `_funcs.py`) to hold `session_core.py` at its 2 304-line baseline — split, not a raised ceiling. My suspicion that the moved `_promote_active` broke `getActiveSession()` was measured false: the active session switches after `sql()` and `createDataFrame()` on the branch exactly as on the step-1 tree.

## Parked for the owner

- DYNCFG-1 DQ-1..DQ-7 (write-back target, `[<profile>.autotune]` key names, writes allowed, sample definition, regression-only knobs, live session vs file, overrun semantics): premises and leans in `task/roadmap/mid-term/dyncfg-1-cards-2026-09-12.md` §2. DYNCFG-1-BED (M×2) needs no ruling and can start.

- CFG-2 D-5: a profile-level (global) `auto_register` key — the release roadmap text says "per entry or global", the key's location is an unruled public name; only the per-entry key ships.
  **Owner ruling (2026-09-13):** the step-2 public shape stands (`SourceMetadata` rows beside `CatalogMetadata`, the `NamedSource` handle with `ping()`); the global `auto_register` key is a later card, not a change to #556.

## Timeline

- 20:30 local: session start; runbook, slate 2 S2-19..S2-29, CFG-2 card, DYNCFG-1 epic read. Run 9 busy (cargo/rustc/maturin live). Lanes b-cfg2, b-dyncfg cloned at `0233d96f`.
- 20:41: both Devin rounds launched.
- 20:53: DYNCFG-1 CONCLUDED (one commit, 3 markdown files). Audit: figures spot-checked, CSV totals recomputed, three wording fixes; docs gates green → #547 (`make preflight` skipped: docs-only diff).
- 21:36: #547 merged `fd51dd34` (tree-equal); one Slack note. The `/tmp/b-dyncfg` clone stays on disk (the sandbox refused the `rm` of a registered working directory; docs only, no build).
- 21:41: CFG-2 step 1 CONCLUDED (`d0a27dea`, 60 min); audit and gate re-run started.
- 21:46: my gate re-run on `d0a27dea` green (config_file, named_sources, session, `make verify`, `make develop`, test_config_mirror). Follow-up round launched (resume `unleashed-tsunami`).
- 22:05: S2-21 Grok Rust perf reviewer on `d0a27dea` CONCLUDED (18 turns, $0.40): no P1; P2-1 the registry stores unread `SourceSpec`s and `sql_with` clones them per query (~296 ns for one source, ~1 ms at 1024) → back to the actor; P3-1..3 plus the memory-catalog first check → ledger (the one-line check fixed). Report `/tmp/oc-worker/b-rev-cfg2/report.md`.
- 22:12: follow-up 1 CONCLUDED `9c15cacf` (68 steps): F-1 fixed, F-2 pinned red-first (C-011), F-3 swept every `register_catalog` path and closed the `CREATE CATALOG` hole. Audit clean. Follow-up 2 (P2-1, memory-catalog check, P3 ledger rows) queued behind builders.
- 22:31: follow-up 2 CONCLUDED `a7fcf121` (100 steps): P2-1 `Arc<SourceSpec>` in the registry and session, the memory-catalog first check through `is_registered`, P3-1..3 in the ledger's S2-21 section (C-012). Audit clean. My gate re-run plus `make preflight` started on HEAD.
- 22:51: my gates on `a7fcf121` green: config_file, named_sources, session, `make verify`, `make develop`, test_config_mirror, and `make preflight` (facade 5 959 passed / 369 skipped). The step-2 brief is staged at `/tmp/oc-worker/b-cfg2s2/brief-1.md` (D-7..D-12: `SourceMetadata` namedtuple beside `CatalogMetadata`, a `NamedSource` handle, the pyo3 door in its own module, `auto_register` typed in the mirror, `listCatalogs` measured not changed, guide subsection). It launches after step 1 merges.
- 23:10: step-1 branch merged with `origin/main` (3 commits, run 9's `silver` module; `lib.rs` and maps auto-merged, no conflicts); `make verify` green on the merged tree. Pushing the PR.
- 23:12: #551 open (head `a9031fb7` confirmed on GitHub).
- 23:38: #551 green (9 pass / 2 skipping), but `main` moved twice while CI ran (run 9's merges, last #550 PERF-CAST-1 s2): update-branch → `5d8bdb5b`, then again → `64a5ebbd`; CI re-running.
- 23:59: #551 behind again (#552 platform-1 CI change) while the `build + import smoke` job (~17–20 min per head) ran; third update-branch. Observation for the owner: with two orchestrators merging, the smoke job's length makes update-branch a race — run 9 merges roughly every 20 min.
- 00:15: #551 merged `25707c46` (tree-equal) after three update-branch rounds; one Slack note. `/tmp/b-cfg2` stays on disk (the session's working directory; the sandbox refuses its `rm`). Step-2 lane `/tmp/b-cfg2s2` cloned off `25707c46`; Devin launch queued behind builders.
- 00:59: CFG-2 step 2 CONCLUDED (`e196464d` + `c03eeb8b` ledger departure, C-013..C-019). Audit clean (scope, comment grep, trailers, the `pins:` docstring citation is the examples convention in 191 of 216 files). My gates: develop, the two pin files, named_sources, example coverage, `make verify` green; preflight running.
- 01:10: S2-21 Grok Rust+Python reviewer on `c03eeb8b` CONCLUDED (21 turns, $0.55): no P1, no P2 — `sources()` is one native crossing (128 µs at one source, debug), no session-build cost when no source is declared; three P3 notes.
- 01:18: step-2 preflight green (facade 5 969 passed / 369 skipped). Orchestrator commit `02d06e01`: the reviewer's P3 rows in the departed ledger (the frozen rule is clean), `NamedSource` docstring trimmed to one line, map lockstep. Merged `origin/main` (1 commit, maps auto-merged); `make verify` running.
- 01:30: `make verify` green on the merged step-2 tree `9820679f`; pushing the step-2 PR.
- 01:31: #556 open (head `9820679f` confirmed on GitHub).
- 01:40: #556 CI red on `Python / parity-harness tests`: `test_ex_0_enumerator_emits_five_families_and_repark_sql` pins the example inventory at 928 rows, and step 2's two public names make 930. Orchestrator miss: I did not run the whole parity suite before the PR (runbook §5, the run-5 lesson; preflight does not include it). Fixing on the branch.
- 01:51: fix `39a54f64` (the inventory pin 928 → 930 plus the parity map line, the PERF-UNPIVOT-1 precedent), merged `origin/main` → `059b1aca`; `test_ex_0_example_coverage.py` 26 passed, example coverage 923 names / 809 covered / 112 backlog (unchanged); `make verify` running. The local whole-parity run took 8 min before its first stop; the full run continues.
- 01:54: `make verify` green on `059b1aca`; pushed to #556, CI re-running.
- 02:12: #556 green except the smoke job, but `main` moved again while it ran; update-branch (the smoke job restarts, ~20 min).
- 02:13: the drift was run 9's #555 (FACADE-3 step 1: perf docs and goldens, no file shared with CFG-2); #556 head `eee4b7f0` confirmed on GitHub, CI re-running.
- 02:35: #556 merged `8f710b6a` (tree-equal) after update-branch round four (run 9's #555). CFG-2 is closed: the ledger is in `completed/`, and `docs/guide/repark-toml.md` constraint 1 now states the lazy-register, refuse-on-use truth. The list is exhausted; the session stops opening lanes.

## Lessons for the next orchestrator

- **Run the whole parity suite before every product PR.** `make preflight` does not include it, and the example-inventory count pin (`test_ex_0_example_coverage.py`) reds CI whenever a public name is added. The local run takes more than 8 minutes, so detach it.
- **Two orchestrators merging means repeated update-branch rounds.** Each one restarts the ~20-minute `build + import smoke`; #551 needed three rounds and #556 four. A queue or a merge slot would save roughly an hour per PR on a busy night.
- **The sandbox refuses `rm` on a registered working directory.** `/tmp/b-cfg2`, `/tmp/b-cfg2s2` and `/tmp/b-dyncfg` stay on disk; the owner may remove them. `/tmp/b-rev-cfg2`, `/tmp/b-rev-cfg2s2` and `/tmp/b-report` are also left.
