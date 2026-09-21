# xo-glm lane report — unit IPI-32 (catalog and session commands, 11 cells)

Lane `xb-cat`, branch `fix/ipi-32-catalog-session`, PR **repark#758** (OPEN). RePark only.
Written 2026-09-20 21:18 EDT (tick 99); refreshed as PRs close.

## Unit
IPI-32 catalog and session commands, 11 inventory cells (CAT-* / USE / SHOW / DESCRIBE / REFRESH family).
Packet `/tmp/oc-worker/plan-packets/ipi-32-catalog-session.md`; late critic report folded in from the start
(H-01..H-10 from `/tmp/grok-worker/re-rv-32/20260920T092917Z/out.json`; H-06 and H-04 declined with evidence).

## Rounds spent (executor tier → count)
- Muse (max, worker role): r1 131914Z, r2 151718Z (ended at step cap, no handback), r3 172618Z (S1–S9b),
  r4 203257Z (six critic findings, test/docs only), r5 210108Z (scratch-home pinning), r6 222313Z
  (planner-defaults revert + registry box + V-001..V-003) — **6 rounds**.
- Muse-clerk (medium): r7 011751Z (rebase-as-union onto e0b91bca + V-002 class sweep + ruled
  SHOW TABLES mirror fix) — **1 round, running**.
- Grok critic via xreview (xr-xb-cat): 192403Z VOID, 193835Z VOID, 195815Z NEEDS_REMEDIATION (fixed by r4+r5),
  214139Z NEEDS_REMEDIATION (V-001..V-003, fixed by r6) — **4 stamps, 2 remediations, 0 PASS yet**.
- GLM Flash / Devin: 0 rounds (no clerk need before r7; kept for narrow mechanical work if it arises).
- Two orchestrator-direct fix commits (tick 76, eb7975f5: clippy too_many_lines extraction, ruff PTH rewrites,
  docs guide rewrite, map/allowlist rows) — small mechanical repair after a red CI, done between rounds.

## Critic rejections and their classes
- 195815Z: six findings, test/docs-only (missing pins, stale ledger rows, docs drift). Class per finding folded
  into r4; no second finding of any of those classes since.
- 214139Z: V-001 hollow REFRESH pin (assertion-only, no observable rebuild), V-002 single-backtick
  missing-table error shape (class swept in r7 over the merged tree), V-003 tautological cell-obs assert.
  All three fixed in r6; V-002 class sweep extended over main's new code in r7.

## Gate / CI failures encountered (all root-caused by local measurement)
1. CI red ×3 on b5a26799 (clippy too_many_lines, ruff PTH, docs-links) → fixed in eb7975f5.
2. Wheels smoke f15a6bf5: 322 failures, root cause = S9 dialect.rs planner-default flip made bare scratch
   registrations land in the Iceberg-backed spark_catalog.default → r5 pinned every internal scratch/temp
   registration to explicit datafusion.public.
3. Wheels smoke 77ee7064: 27 failures in five measured clusters (EXPLAIN prefixes missing from Python
   expansion; USE DATABASE reading engine defaults vs facade state; truncate-view bare register_table;
   one-part CREATE VIEW routing; message-shape/aliasing in AMBIGUOUS candidates, orc door, metadata tables,
   write-options ordering) → root cause the same H-01 flip → r6 reverted the flip entirely and moved Spark
   session defaults into an Arc-shared box on CatalogRegistry (current_defaults/set_defaults) with a
   native set_session_catalog carrier; planner options stay datafusion/public in every session.
   r6's own gates: 279+38+250+248 python tests green, full Rust suites green, clippy/fmt/size/comment-ban
   clean — except one provably contradictory pin (below).

## Questions that needed a ruling
- Zero claims-file QUESTION lines. One executor question (r6 Q1, HALT): bare SHOW TABLES shape pins contradict
  (test_metadata_tables.py:500 reads `table_name`; test_catalog_surface.py:443 and Rust pin
  show_tables_empty_ambient_scope_is_empty mandate (namespace, tableName, isTemporary)). Settled by me from
  code + my tick-61 measurement (that test was already named stale) + the S9 precedent for its Rust twin:
  1-identifier mirror fix `table_name` → `tableName`, test-only. Recorded in claims 21:14 line.
- One earlier executor deviation accepted with proof: r6 shipped a native box-only catalog setter instead of
  the briefed `USE <name>` sync (SQL USE provably breaks the CAT-CURRENT-CATALOG oracle pin and clears the
  facade database) — verified against the gates before acceptance.

## Cell counts
- Before the unit: **0/11 EQUAL**.
- After r6 (pre-merge, local gates): the 11 cells' pins are green locally (ice_catalog_session_1 +
  catalog_surface + show_namespaces + auto_memory_catalog + spark_sql_grammar_1 sets), but the v1.5.0 count
  is only claimable from the post-merge inventory replay:
  `cd /tmp/oc-worker/nc-inventory && ./run-engine.sh repark catsession-after cells_ddl.py` (then cells_props.py,
  cells_proc.py) + `python3 compare.py`. Target 11/11. Not yet run — merge pending.

## Current state (21:18 EDT)
- PR #758 OPEN, remote head 77ee7064 (pre-r6). r6 commits 64fa3153 + 412cbb0a accepted locally.
- Main moved 3dd7b754 → e0b91bca (IPI-51 stamps, IPI-19 merge schema-evolution, fork pin 886b94c1);
  merge-tree conflicts in alter.rs/router.rs/describe_show.rs/session_runtime.rs/guards → muse-clerk r7
  (round 20260921T011751Z, service muse-xb-cat-011751) is rebasing as a union + sweeping the V-002 class +
  applying the ruled mirror fix.
- After r7: comment-ban, my own diff read, clippy + panic-ban, local gate chain (xgate), push
  (force-with-lease recipe), CI incl. the ~21-min wheels smoke, fresh Grok critic on the new HEAD, then the
  merge queue (PASS + all-zero gate + green CI + comment-ban 0), then the cell replay.

## What I would do next
1. Land r7, re-gate, push, critic, queue #758 for merge.
2. Replay the 11 cells post-merge; any non-EQUAL cell becomes a numbered residue with its cell name.
3. If the critic or CI surfaces a new class: name the class, sweep the whole PR for it before the next critic.
4. No pin bumps, no fork PRs, no STATUS.md edits; keep one local gate and at most two rounds per lane
   (box rule) overnight.

## Update 22:06 (tick 101)
- r7 muse-clerk round 20260921T011751Z CONCLUDED: rebase-as-union onto main e0b91bca (22 commits, 6 conflict stops, both sides survive), V-002 class sweep zero sites, Q1 tableName fix (test-only), stale staging ledger row removed. All clerk gates green (comment-ban 0, 4 py files, cargo test -p repark-spark, EXPLAIN repro, repro_del, ruff/fmt/map-sync/checkers, cap baselines).
- Orchestrator acceptance passed: comment-ban 0, clone clean, no skip-worktree, Cargo diff vs main empty, carrier at session_core.py:1343, partition-mgmt refusals intact, Muse trailers present.
- LINT BEFORE PUSH in flight: make rust-clippy RC=0 (green); make rust-panic-ban relaunched detached after first attempt was killed by tool timeout (log /tmp/oc-worker/xb-cat-panicban.log).
- Fresh local gate launched 22:05 (stale r5 .done deleted). Push + fresh critic + CI next.
