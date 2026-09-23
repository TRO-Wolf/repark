# Report — xo-glmflash2 (IPI-26/27 parser + DDL, 34 cells)

Production lane; report kept current as PRs close. Last updated: 2026-09-21 17:10 EDT (tick 154).

## Cells

Target: 34 D-family cells per units.py regexes (packet /tmp/oc-worker/plan-packets/ipi-26-27-parser-ddl.md).
**Before: 0 of 34 → tick 88 replay: 9 of 34 → tick 104 (post-#773-merge): 10 of 34 →
tick 154 (post-#774-merge replay on main fa8f9b4d): 14 of 34 closed.**
The +4 over 10: D-CREATE-PART-DATE-ALIAS, D-CREATE-PART-DATEHOUR-ALIAS (both full-EQUAL,
obs-identical), D-ADD-PART-FIELD-OLD-SYNTAX-BUCKET, D-ADD-PART-FIELD-OLD-SYNTAX-TRUNC (argument
order), D-REPLACE-PART-FIELD — all WO3/#774 family, and DATEHOUR closed beyond the packet's
expected 12. Verified directly from the controlled replay JSONs (out/repark-zz-ddl-after2.json +
repark-zz-deep-after2.json, rc=0 both legs) and re-verified in the shared matrix.json (14/34).

MEASUREMENT-CLASS FINDING (tick 154, swept): compare.py's "newest-wins" is FILENAME sort, not
mtime — other lanes' partial runs `repark-zzcat-ddl.json` (11:03, pre-#774 repark results) and
`repark-zzzz-pr2a-*.json` sorted AFTER my `zz-` outputs and poisoned my D-cells in matrix.json
(they read REFUSED-UNREGISTERED while my fresh run said EQUAL). Fix of record: replay outputs
copied to `repark-zzzzz-{ddl,deep}-after2.json` (sort last), compare.py re-run, matrix now correct.
Rule: any lane replaying must name its outputs to sort after every existing `repark-z*` file, or
classify its cells directly from its own JSONs.

Replay method: `/tmp/nc-build/.venv` is GONE box-wide (run-engine.sh repark leg broken); replayed in
the lane with `/tmp/xb-ddl/.venv/bin/python harness.py --engine repark --cells cells_ddl.py` +
`cells_deep.py` (cwd `/tmp/oc-worker/nc-inventory`), outputs `out/repark-zz-ddl-after.json`
(190 rows, rc=0) and `out/repark-zz-deep-after.json` (42 rows, rc=0), then `compare.py`.
Validity: lane tree `402bda5d` == merged main `6cff128f` tree, verified directly (git rev-parse of
both trees); zz- output names sort last so compare.py's newest-wins picks these runs.

The 14 closed: D-COMMENT-ON, D-CREATE-COMMENT, D-CREATE-COL-COMMENT, D-CREATE-LOCATION,
D-CTAS-LOCATION, D-CTAS-COMMENT, D-DESCRIBE (free, unblocked by column COMMENT), D-X-CHANGE-COLUMN-TYPE,
D-X-CLUSTERED-BY (#759/#770 family), D-CREATE-PART-DATE-ALIAS, D-CREATE-PART-DATEHOUR-ALIAS,
D-ADD-PART-FIELD-OLD-SYNTAX-BUCKET, D-ADD-PART-FIELD-OLD-SYNTAX-TRUNC, D-REPLACE-PART-FIELD (#774).

Note: D-X-CHANGE-COLUMN-RENAME (#773's extra cell, outside this packet's 34) closes as refusal
parity (compare.py SPARK-CANNOT, why "both refuse"): spark leg `ParseException
_LEGACY_ERROR_TEMP_0034`, repark leg `PARSE_SYNTAX_ERROR`/42601 — exactly the INDEX.md
decision-16 interim mapping; pre-#773 the cell read DIFFERENT.

NEW CLASS FINDING (apply-layer closure gap, carded as WO-APPLY-MAP, not yet started):
- D-ADD-COL-STRUCT and D-X-ADD-COL-MAP-KEY-STRUCT both moved from the parse refusal (before) to the
  SAME apply-layer failure: `PySparkException: unexpected target column type Map("key_value":
  non-null Struct("key": … "value": Int32 …))`. The parser fixes held; the native write layer
  rejects a map whose key/target is a struct. r1's parse-layer closures of these two cells were
  premature — cell-EQUAL is the standard.

Remaining 20 not-EQUAL (34−14): OPTIONS ×2 (D-5, in flight on xb-opt), SET-LOCATION, SET/DROP
IDENTIFIER ×2 (D-6), NS-ALTER-PROPS, UNSET-PROPS-IF-EXISTS, ALTER-TYPE nested/array/map ×3,
CREATE-V1 + TP-FORMAT-V1-DELETE (D-8), REF-BRANCH-ON-EMPTY (A-5), SHOW-CREATE ×2 (D-7),
DESCRIBE-COLUMN ×2, D-X-PARTITIONED-COLDEF (D-9/A-11), apply-layer map ×2 (WO-APPLY-MAP above).

Residues (not cells): docs-parity; registry.md IO-ORC-SQL-1 stale pin-shape text;
docs/spark-sql-iceberg-parity.md ~3113 stale-pin prose.

## PRs

- repark#759 — r1 "splitter angle depth, ALTER MAP dialect, CLUSTERED BY rewrite": MERGED
  (squash into main). Post-merge replay: D-X-CLUSTERED-BY EQUAL confirmed; D-ADD-COL-STRUCT and
  D-X-ADD-COL-MAP-KEY-STRUCT hold at the apply layer (see class finding above).
- repark#770 — r2 "COMMENT on columns/tables, CREATE/CTAS LOCATION, dbt comment+location retirement":
  **MERGED 2026-09-21T08:05:13Z as 6cff128f** (drive RESULT=TREE-EQUAL; 11 squash commits; critic
  PASS @7844cf85 = verdict of record; CI 9 pass + 2 skip; local gate 5×0; comment gate 0).
- repark#773 — CCR "refuse Hive-style CHANGE COLUMN that renames at parse time" (1 cell:
  D-X-CHANGE-COLUMN-RENAME): OPENED tick 98 @48dc8d95 (4 commits on fix/ipi-26-27-change-rename,
  rebased on 7748a459; lint 3-target 0; local gate 5×0; comment gate 0). **Critic PASS @48dc8d95
  (xr-xb-ddl-clerk 20260921T103659Z) + CI green + QUEUED tick 100** (run16 merge-queue,
  queue line normalized to bare `773` by xo-grok2 tick 104 note — drive-merge.sh compares awk $1).
  **MERGED 2026-09-21T11:45:17Z as 5c3226bb** (main 7748a459 → 5c3226bb). Post-merge replay of
  D-X-CHANGE-COLUMN-RENAME launched tick 104 (replay-ccr-0749.service, lane .venv workaround,
  `repark-zz-{ddl,deep}-after2.json`); formal count → 10/34 once banked.
   Note of record: refusal is Plan-based [PARSE_SYNTAX_ERROR], surfaces as AnalysisException / 42601
   (Q1 ruling, claims 06:18); ParseException reclass = separate future unit.
- repark#774 — WO3 "partition transform aliases, bucket/truncate argument order, REPLACE PARTITION
  FIELD transform LHS" (2+ cells + near-miss pins): critic r1 NEEDS_REMEDIATION (quoted-string
  sniff hole, wrong REPLACE pin, transform-source-col match) → WO3-R remediation → final critic v5
  20260921T201010Z PASS, findings EMPTY, @213bd8cb = CURRENT head. Local gate 5×0 @213bd8cb (16:34),
  lint PASS, comment gate 0. **MERGED 2026-09-21T20:42:23Z as squash fa8f9b4d** (tree diff vs
  213bd8cb EMPTY, verified). Post-merge replay tick 154 → 14/34 banked (see Cells). DONE.

## Executor rounds

Closed: muse xb-ddl 20260920T203553Z (WO2); devin xb-ddl-clerk 20260920T205910Z / 211646Z / 222435Z
(WO1b fail / retry / WO1c); muse-clerk xb-ddl-clerk 20260921T015744Z (WO1d); muse-clerk xb-ddl
20260921T020119Z (WO2b rebase); glmflash xb-ddl 20260921T032616Z (WO4 dispatch fn); muse-clerk
xb-ddl-clerk 20260921T035342Z (WO5 cap-ratchet); muse-clerk xb-ddl 20260921T044434Z (WO6 r2 rebase);
muse-clerk xb-ddl 20260921T053306Z (WO7); muse-clerk xb-ddl 20260921T063352Z (WO8 rebase); glmflash
xb-ddl 20260921T072750Z (WO9); glmflash xb-ddl 20260921T083342Z (WO3, concluded); glmflash
xb-ddl-clerk 20260921T093509Z (CCR, concluded); glmflash xb-ddl 20260921T104840Z (#774 ruff-format
clerk, CONCLUDED tick 99, commit 81cad671, pushed tick 99).
Tiers: muse-clerk 7 rounds, devin 3, muse 1, glmflash 6.

## Critic verdicts

- r1 full critic 20260921T011408Z PASS @5f0b9cc2; delta 20260921T013805Z NEEDS_REMEDIATION (V-001)
  → fixed (WO5); delta2 20260921T042641Z PASS @9021fa19.
- r2 critics: 20260921T060502Z PASS @4d3ed3d5 and 20260921T064448Z PASS @d6142bac both VOID
  (superseded by rebase); **20260921T074057Z PASS @7844cf85 = verdict of record** (CI + queue
  satisfied; merged).
- #774/#773 critics concluded tick 100 (both launched tick 98): **#773 PASS @48dc8d95 = verdict of
  record** (queued). #774 NEEDS_REMEDIATION @5f5a516a (head moved to 81cad671 for an unrelated
  style commit) → WO3-R remediation launched tick 100; fresh critic owed after it lands.

## Questions asked

One, ruled: Q1 (CCR, tick 97) — a Plan-based [PARSE_SYNTAX_ERROR] surfaces as AnalysisException;
probed, ruling filed claims 06:18. ParseException reclass carded as separate future unit.

## What I would do next / in flight

1. ~~#773 / #774~~ MERGED and replayed (#773 → 10/34 era; #774 → 14/34, tick 154). DONE.
2. D-5 (xb-opt, in flight): glmflash round 20260921T192918Z running on fix/ipi-26-27-options;
   on handback: comment gate → diff read → rebase onto post-#774 main → my own gate → lint →
   xpr → critic → queue (cells D-CREATE-OPTIONS, D-CTAS-OPTIONS).
3. WO-APPLY-MAP (new): native write layer rejects map-with-struct target (2 cells, same error).
4. Later WOs: SET LOCATION (decision 18), identifier fields (D-6/A-3), ALTER TYPE nested ×3,
   format-v1 (D-8/A-4), CREATE BRANCH empty (A-5), SHOW CREATE (D-7/A-8), DESCRIBE-COLUMN,
   PARTITIONED-COLDEF (D-9/A-11), NS-ALTER-PROPS, UNSET-PROPS-IF-EXISTS.
5. Residues this lane (numbered, not closable here): D-ADD-COL-STRUCT +
   D-X-ADD-COL-MAP-KEY-STRUCT — fork Map/List arm + later bump (claims 14:27).
