# report-xo-opus61 — lane D (U3 PD-BATTERY, U4 DDL-SHOW-DESCRIBE) — run 28, 2026-09-23 (HAND-OFF at the 18:27 stopping point)

## Unit 1 — U3 PD-BATTERY — CLOSED, NO CODE (06:4x; accepted by the scoreboard owner 06:5x)
- Recorded legs on main 88b6f59: all 41 predicates equal in all 9 PD cells; the one diff is p01 `cat IN ('a','z')` in PD-IDENT/PD-MULTI
  where Spark raises INTERNAL_ERROR (SPARK-CANNOT sub-predicate, ledger note on both cells).
- `startswith()` answered since 09-22; ICE-NAN-DECIMAL-LITERAL-1 FIXED (09-23 leg answers). The REFUSED-UNREGISTERED verdicts were stale
  overrides.json entries carried by stage.sh since 09-20.
- Before/after (matrix on 88b6f59f, recomputed by the scoreboard owner): EQUAL 607 -> 616, REFUSED-UNREGISTERED 34 -> 25.
  Evidence: /tmp/xo-xo-opus61/u3-overrides-corrected.json, /tmp/xo-xo-opus61/replay-u3/matrix.json. Lane xd-pd never built; removed.

## Unit 2 — U4 DDL-SHOW-DESCRIBE (10 cells) — NOT MERGED; HAND-OFF (18:27 EDT)
Stack: C (#810) <- A (#816, contains C@b2721248) <- B (#813, contains A@a7c24106). All three GitHub-CONFLICTING (stack rebased locally
past the pushed heads). main = fd43f192 (#806, touched DESCRIBE metadata tables — B may conflict). Cells: C = D-SHOW-CREATE, -PLAIN;
A = D-SHOW-TBLPROPERTIES, -KEY, D-SHOW-TABLE-EXTENDED; B = D-DESCRIBE-COLUMN, -PLAIN, -VERSION, -EXTENDED, D-CREATE-DEFAULT-PROPS. No replay counts yet (nothing merged).
Measurements: wo/spark-4.1.2-u4-measured.json, measure/u4a_live.json, /tmp/xd-create-scratch/m2..m16 (.py/.json). Work orders + critic briefs: /tmp/xo-xo-opus61/wo/.

### PR C — repark#810 "feat(show-create): SHOW CREATE TABLE answers Spark 4.1.2's text for Iceberg tables" (base main)
- Pushed head ea9acc75 (CI green, stale). Clone /tmp/xd-create, branch xd/show-create, HEAD 0349971c (WO-C10 step 1 commit on b2721248), 6 dirty files.
- ROUND STILL RUNNING at 18:27: WO-C10 (terra) /tmp/oc-worker/codex-worker/xd-create/20260923T220935Z/, unit codex-xd-create-220935.service.
  Do not touch the clone until `exit` + handback.json exist.
- Last critic: r7 (Sol, at b2721248, valid) = NEEDS_REMEDIATION. V-001 P1: `SHOW CREATE /* c TABLE …` pinned TokenizerError; Spark (m12, m15)
  = UNCLOSED_BRACKETED_COMMENT/42601 for every unclosed `/*`. V-002 P2: absence-only router refusal test. Class: TOKENIZER-FAILURE PINS.
  Ruling (claims 17:59): front-door rule, first line of router execute_with_statement_options, new normalize/statement_guard.rs (refuse_multi_statement_sql
  + helpers move there), SHOW CREATE's own unclosed arm deleted. Near misses (m15/m16): nested closed, '/* x' literal, `-- /* x` -> rows;
  unclosed `/*+` hint and `SELECT 'unterminated; SELECT 2` -> PARSE_SYNTAX_ERROR caret (IPI-51, declared divergence).
- Gate: none at the new head (the b2721248 gate/lint was stopped).
- NEXT: on WO-C10 hand-back: comment gate, author TRO-Wolf, trailer terra only, read questions[], read the diff (scanner handles quotes, --, nesting, /*+;
  router call first; no loosened pins). Merge-tree vs origin/main; /tmp/xo-xo-opus61/lint-xd-create.sh + xgate xd-create repark-core,repark-spark +
  test_show_create_table.py test_e1_errorclass.py test_ice_error_conditions_1.py test_describe_table.py. All 0 -> xpr.sh xd-create (same title)
  /tmp/xo-xo-opus61/wo/prC-body.md; gh pr edit 810 --body-file with the front-door rule. Critic r8: wo/critic-C.md "Round 8 note" is written;
  `sed -i 's/<C10HEAD>/<sha>/'`, add WO-C10 question rulings, xreview.sh xd-create. PASS at current head + CI green -> merge queue + drive-merge.sh 810.
  After merge: replay D-SHOW-CREATE, -PLAIN; rm -rf /tmp/xd-create /tmp/xr-xd-create.

### PR A — repark#816 "feat(spark): SHOW TABLE EXTENDED answers Spark 4.1.2 rows (U4 PR A)" (base main; stacked on C)
- Pushed head a7c24106 (CI green, stale). Clone /tmp/xd-props, branch xd/show-tblprops, HEAD c6b05bf2 (contains C@b2721248). No round running.
- WO-A7 (luna, ;;-measurement pins) ended exit 0 status BLOCKED, 0 commits: work left in the tree (staged tests/show_table_extended.rs +28:
  `; ;` equality pin + exact `;;x` multi-statement refusal pin; unstaged completed/show-table-extended-1-ledger.md m14 note). The commit hook failed on a
  dead link: task/ledgers/staging/map.md still lists wo-c3-ledger.md, which lives in completed/ (a stale staging row a rebase re-added).
  Fix: delete that staging map row, then commit both slices (luna trailer the WO names).
- Last critic: r2 (Sol, at a7c24106) = NEEDS_REMEDIATION, 5 findings (parser-message change effects unpinned, partial SHOW TABLE EXTENDED row pins,
  map citations). WO-A4..A7 addressed them; critic r3 not yet run. Local gate all 0 at 16:41 (older head; re-gate needed).
- NEXT: commit WO-A7 as above; restack onto C's new head (`git -C /tmp/xd-props rebase --onto <newC> b2721248`); grep staging+completed map.md for dup/stale
  rows; lint-xd-props.sh + xgate xd-props (repark-core,repark-spark + test_catalog_surface.py test_fnp_4b_literals.py test_e1_errorclass.py
  test_ice_error_conditions_1.py test_ice_branch_ops_1.py python/dbt-repark/tests/test_statement_surface.py); push #816; critic r3 from wo/critic-A.md
  (base 037ad168 -> <C head>; add WO-A5..A7, NEAR MISS, FNP-4B residue, m14 `;;` ruling = parity; m15 front-door rule is C's).
- A2 (SHOW TBLPROPERTIES table branch + retire DBT-TBLPROPS-1, dbt pin R-SHOW-TBLPROPERTIES) NOT STARTED: waits on xo-opus59 PR4 (parser owner).

### PR B — repark#813 "feat(describe): DESCRIBE column, identity partitions and owner stamp answer Spark 4.1.2" (base main; stacked on A)
- Pushed head 2cc8b252 (CI red: Python, build + import smoke — stale head). Clone /tmp/xd-show, branch xd/describe, HEAD 1e4d8e29 (contains A@a7c24106).
  No round running. Local gate at 16:50: CB=0 R=0 T=1 U=1 L=1 (6+1 known failures to fold).
- No critic round yet on B.
- NEXT: restack onto A's new head (`rebase --onto <NEWA> a7c24106`; merge-tree check against fd43f192 first); WO-B-owner (wo/WO-B-owner-ste-owner-line.md)
  + fold the 6+1 failures + NEAR MISS and TOKENIZER-FAILURE PINS sweeps; re-gate; fill prB-additions-draft.md <FILL>, prB-body.md; xpr.sh xd-show
  (absolute body path) + gh pr edit 813 --body-file; critic from wo/critic-B.md; add xr-xd-show to LANES.

### Clones on disk
/tmp/xd-create (xd/show-create, round running), /tmp/xd-props (xd/show-tblprops, 2 dirty), /tmp/xd-show (xd/describe, clean),
/tmp/xr-xd-create (b2721248), /tmp/xr-xd-props (a7c24106). Keep all until their PRs merge.

### Not started
A2 (above). Everything else in the unit list is in a PR above.

### Lessons (evidence in state-tick93.bak / state-tick102.bak)
- A pin of raw fallback text was written next to a measurement of the same input that contradicted it; check every fallback pin against the measurement file.
- A parse error that looked statement-specific was lexer-level in Spark (m15); measure one statement outside the unit before scoping a fix.
- `;;` looked like a defect; a 1-minute live run (m14) showed parity. Measure before ruling.
- Repo-wide: `stmt;;x` gives RePark's multi-statement text where Spark gives `extra input 'x'` (claims NOTE 17:55).
- Near misses apply to every string recognizer, error-message parsers included (critic C r6).
- A read-only critic sandbox turns a finding round into BLOCKED; briefs now ask for reasoned mutations.
- GitHub can report CONFLICTING where local merge-tree is clean; rebase before the critic, not after. Rebases re-add staging-map rows silently
  (this is what blocked WO-A7's commit); lint after every rebase.
