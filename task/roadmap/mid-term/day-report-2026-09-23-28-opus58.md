# Report — xo-opus58 (Opus 5.5 high orchestrator, lane A READ, 2026-09-23) — HAND-OFF (run stopped by owner 18:27; written tick 146, 18:30)

## HAND-OFF (this section supersedes the Unit 1-3 summaries below; the tick log after them is history)

### Outcome
| unit | cells | result |
|---|---|---|
| 1 REG-1 | R-TT-TIMESTAMP-AS-OF-EPOCH | no code regression: a harness timing race (probe/reg1-finding.md). No PR. Residue R-1: cell stays flaky until cells_read.py tt() gets a deterministic gap (proposed to the scoreboard owner in claims). |
| 2 U0 | R-DF-LOAD-META, R-DF-LOAD-META-FILES-VERSIONASOF | #805 MERGED 16:01 (0002a7f2): NOT-PARSED -> EQUAL, REFUSED-REGISTERED -> EQUAL |
| 2 U0 | R-MT-DESCRIBE | #806 MERGED 17:48 (main fd43f192): REFUSED-UNREGISTERED -> EQUAL |
| 3 U10 | R-INPUT-FILE-NAME | #811 MERGED 16:44 (main b7a3c905): REFUSED-UNREGISTERED -> EQUAL |
| 3 U10 | R-DF-LOAD-PATH, R-DF-LOAD-METADATA-JSON | OPEN, no PR yet (lane xo58-dfl, below) |
| 3 U10 | R-MC-DELETED (+ R-MC-STAR-EXCLUDES, R-MC-POS-MOR) | OPEN, no PR yet (lane xo58-mcd, below) |
Net: +4 EQUAL banked (3 cells via #805/#806, 1 via #811). Main at hand-off: fd43f192.

### Open work 1 — DFLOAD (R-DF-LOAD-PATH + R-DF-LOAD-METADATA-JSON), no PR opened
- Clone /tmp/xo58-dfl, branch fix/u10-df-load-path, base fd43f192 (= main, no rebase needed). Committed head d09b6691 (r4 red pins);
  working tree DIRTY (iceberg_path.rs, iceberg_path/tests.rs, reader_iceberg_path.py) — devin is mid-round doing mutation checks.
- ROUND STILL RUNNING at 18:30: devin r4, service devin-xo58-dfl-222039, round dir /tmp/devin-worker/xo58-dfl/20260923T222039Z,
  work order /tmp/xo-xo-opus58/wo/dfload-r4.md, session outrageous-wednesday. Do not touch the clone until the service exits.
- Last critic: Sol r3 at 62869c40 (/tmp/oc-worker/codex-worker/xr-xo58-dfl/20260923T221003Z, 54 tools, valid) = NEEDS_REMEDIATION.
  V-001 P1: the resolver uses FileIO local list, which is RECURSIVE, so nested metadata/sub/v99 outranks the direct v3 (verified
  local_fs.rs:268-298 + iceberg_path.rs:51-57). V-002..V-004: ledger claims exceed pins (valid names per form, misplaced-dash UUID,
  non-UTF-8 hint). Classes: C "resolver trusts listing scope", B "ledger claims > pins" (3rd hit), A near-miss gap. r4 fixes all four.
  Equivalent mutant (NOT a finding): dropping `!rest.is_empty()`.
- Gate: none valid (the 18:09 gate was stopped; head changes in r4). CI: none (no PR).
- NEXT STEP when r4 hands back: comment gate (comment_ban.py /tmp/xo58-dfl origin/main HEAD); `ls-files -v | grep '^S'` empty; read the
  iceberg_path.rs diff (direct-child filter, private, no new dep); check nested pins + mutation reds + V-002..4 pins + class tables.
  Then gate+lint: systemd-run --user --slice=repark.slice --collect --unit=gatelint-xo58-dfl-<ts> bash /tmp/xo-xo-opus58/wo/dfl-gate-lint.sh
  (5 codes in /tmp/oc-worker/xo58-dfl-localgate.done + CLIPPY PANIC FILESIZE in /tmp/xo-xo-opus58/wo/dfl-lint.done). Critic r4 Sol:
  copy wo/critic-dfl-r3.md -> wo/critic-dfl-r4.md with the r3 delta; xreview.sh xo58-dfl wo/critic-dfl-r4.md. All 0 -> xpr.sh xo58-dfl
  "U10: format('iceberg').load(<path>) through a static table" /tmp/xo-xo-opus58/wo/pr-dfload-body.md. PASS at head + CI green -> queue.
  If r4 exits without handback.json: read its out.txt + wo/dfload-r4.launch.log; if steps ran out, split the order.
- After merge: replay both cells (before: refused "invalid table identifier"; Spark [[2,"b","y"],[3,"c","x"]]; recipe: copy
  /tmp/xo-xo-opus58/replay811/, sed run.sh for the cells); rm -rf /tmp/xo58-dfl /tmp/xr-xo58-dfl.
- Fork: none needed (measured: pin 604edca StaticTable already serves it; probe/dfload-finding.md).

### Open work 2 — MCDEL (R-MC-DELETED), no PR opened
- Clone /tmp/xo58-mcd, branch fix/u10-mc-deleted, base fd43f192 (= main). Committed head 98482613 (r2 row-level + full-message pins);
  working tree DIRTY (metadata_columns.rs, 1 line) — devin is mid-round doing mutation checks.
- ROUND STILL RUNNING at 18:30: devin r2, service devin-xo58-mcd-221513, round dir /tmp/devin-worker/xo58-mcd/20260923T221513Z,
  work order /tmp/xo-xo-opus58/wo/mcdel-r2.md (rulings R1-R3), session great-basilisk. Do not touch the clone until it exits.
- Last critic: Sol r1 at c6d4c237 (/tmp/oc-worker/codex-worker/xr-xo58-mcd/20260923T215741Z, 47 tools, valid) = NEEDS_REMEDIATION V-001..V-003,
  class "pin asserts shape, not rows or the full message". Live Spark measured (probe/mcdq1-spark.log, mcdq2-spark.log): WHERE _deleted -> [1];
  ORDER BY _deleted,id -> [2,3,4,1]; count WHERE _deleted IS NOT NULL -> 3; self-join WHERE b._deleted -> []; quoted `_DELETED` resolves.
  KNOWN DIVERGENCE kept refused + ledgered: Spark serves _file/_deleted over VERSION AS OF (pre-existing for all metadata columns, no cell).
- Gate: none at this head. CI: none (no PR). Fork: RePark-only at pin 604edca; /tmp/xo58-fork is a read-only reference clone.
- NEXT STEP when r2 hands back: comment gate; skip-worktree empty; diff only metadata_columns files/tests/ledger (any R1 production fix
  red-first; R3: did quoted `_FILE` resolve?); sweep table + mutations; S1-S14 pinned as rows. Then gate+lint:
  systemd-run ... --unit=gatelint-xo58-mcd-<ts> bash /tmp/xo-xo-opus58/wo/mcd-gate-lint.sh. Critic r2 Sol: wo/critic-mcd-r1.md -> r2 with delta.
  Replay: systemd-run --user --slice=repark.slice --collect --unit=replay-mcd-<ts> bash -c "bash /tmp/xo-xo-opus58/replaymcd/run.sh >
  /tmp/xo-xo-opus58/replaymcd/replay.out 2>&1" (before REFUSED-UNREGISTERED; expect [[1,true],[2,false],[3,false],[4,false]]).
  xpr.sh xo58-mcd "U10: serve the _deleted metadata column" /tmp/xo-xo-opus58/wo/pr-mcdel-body.md (add the KNOWN DIVERGENCE lines).
  If xo-opus60's pin bump 604edca -> fork main lands first, replay R-MC-DELETED on both pins.
- After merge: rm -rf /tmp/xo58-mcd /tmp/xr-xo58-mcd /tmp/xo58-fork.

### Clones at hand-off
| path | branch | head | round running? |
|---|---|---|---|
| /tmp/xo58-dfl | fix/u10-df-load-path | d09b6691 + dirty | YES devin r4 (222039Z) |
| /tmp/xr-xo58-dfl | fix/u10-df-load-path | 62869c40 | no (critic clone) |
| /tmp/xo58-mcd | fix/u10-mc-deleted | 98482613 + dirty | YES devin r2 (221513Z) |
| /tmp/xr-xo58-mcd | fix/u10-mc-deleted | c6d4c237 | no (critic clone) |
| /tmp/xo58-fork | fork reference | — | no |
| /tmp/xo55-rid | inherited from xo-opus55 (file-order relocation for RP-48) | — | no; NOT mine, never started, RP-48 unclaimed |

### Unit-list items not started
- R-MC-ROW-ID-V3 / L-INSERT-OVERWRITE / R-MT-FILES / RP-48 pin bump (optional for this lane, waiting on xo55-rid): not started, not claimed.
- Residue from #811 (wo/ifn-r1.md fall-through list): count(DISTINCT), self join, VALUES, metadata table, UNION ALL outer SELECT, still refused as before.
- Residue R-1 (Unit 1 harness race).


## Unit 1 — REG-1 R-TT-TIMESTAMP-AS-OF-EPOCH
Finding (tick 1, 06:3x): NOT a code regression; a harness timing race. SEC1 = floor(t1)+1 lands after the DELETE commit whenever
that commit shares insert 2's wall-clock second; RePark then correctly answers S2 (2 rows). MEASURED on main 88b6f59f, 5 trials,
resolver correct both ways (evidence /tmp/xo-xo-opus58/probe/reg1-finding.md). Spark side UNMEASURED (source: SnapshotUtil <=).
No bisect builds, no PR. Proposal to the scoreboard owner (claims): make the gap deterministic in cells_read.py tt().
Residue R-1: the cell stays flaky until the harness change lands.

## Unit 2 — U0 #805 / #806
| PR | rounds (tier) | critic | cells |
|---|---|---|---|
| repark#805 | rd-r5fix (devin) launched t1 | Sol r2 NR at 825d9558 (V-001..V-004 test-side) | R-DF-LOAD-META, R-DF-LOAD-META-FILES-VERSIONASOF |
| repark#806 | md-r2fix (muse resume) launched t1 | Grok r1 NR at 2d1e18d0 (V-001/V-002) | R-MT-DESCRIBE |

## Unit 3 — U10 READ-REST: prep only.
- R-INPUT-FILE-NAME: wo/ifn-r1.md ready (trailer fixed to Devin); clone xo55-ifn fast-forwarded to f411192d.
- R-DF-LOAD-PATH / R-DF-LOAD-METADATA-JSON: MEASURED (probe/dfload-finding.md) — fork pin 604edca already serves it
  (StaticTable::from_metadata_file, FileIO::list, IcebergStaticTableProvider::try_new_from_table); RePark-only, no fork PR, no bump.
  Spark rows [[2,"b","y"],[3,"c","x"]]; RePark raises "invalid table identifier". Draft wo/dfload-r1.md.
  07:03 lane xo58-dfl claimed and set up in the background (branch fix/u10-df-load-path); no executor round yet.

## Counters
Rounds: devin 1, muse 1. Critic rejections: 0. Gate failures: 0. Questions: 0. Spark measurements: 1 (reader types, spark-types.log). Fork-API measurements: 1 (dfload).

## tick 8 (07:4x) — #806 md-r2fix HALT settled by measurement
- md-r2fix (Muse) HALTed with Q1 (V-001 cannot be fixed at the engine: the Python facade expands `t.snapshots` to `mt.t.snapshots`,
  byte-identical to the explicit form) and Q2 (lib.rs 153 > ceiling 152 after main's view_dispatch/view_ddl; hook blocked every commit).
- MEASURED live Spark 4.1.2 / Iceberg 1.11 InMemoryCatalog (probe/mdq1*-spark.log): two-part `USE sc.db; DESCRIBE t.snapshots`
  is TABLE_OR_VIEW_NOT_FOUND (md-r1 §4.4 two-part recovery was never Spark parity); `USE sc.db; DESCRIBE db.t.snapshots` gives rows;
  four-part not-founds name the FULL name as written (`sc`.`db`.`missing`.`snapshots`), not the base; `t$snapshots` is not found.
- Rulings (claims 07:4x): drop the recovery (option a); name the full four-part name; lib.rs: move the module under describe_show/,
  no ceiling raise. Known divergences left in the ledger: the name in the two-part not-found, the unknown-suffix error, quoted `t$snapshots`.
- md-r3fix launched on Muse (resume 01a0cb5f), WO /tmp/xo-xo-opus58/wo/md-r3fix.md.

## tick 9 (07:4x)
- #805: Devin rd-r5fix re-pinned the live leg itself (75671c3f: `delete`, `[[1],[1]]` on hadoop/local[2]) + map.md (489845e6); its xgate at 489845e6 CB=R=T=U=L=0; clippy+panic-ban green per its log. Hand-back pending.
- #806: md-r3fix running (muse-xo55-md-113839). Critic brief wo/critic-md-r2.md rewritten to the tick-8 rulings (r3 delta).

## tick 10 (07:41)
- #805 rd-r5fix (Devin) handed back CONCLUDED: V-001..V-004 closed test-side; live leg pinned on its own engine
  (hadoop/local[2]: ops [append,append,delete], files [[1],[1]]; reader==SQL equality also asserted). Comment gate 0.
  Rebased onto f411192d cleanly (new head 3e76a4a2), skip-worktree empty, xgate queued 07:41.
- R-INPUT-FILE-NAME: ifn-r1 launched on Devin (lane xo55-ifn); claims line written.

## tick 15 (08:20)
- #805 critic r3 (Sol, 65 tools, valid) NEEDS_REMEDIATION at 3e76a4a2: V-001 live leg pins a Spark-file-split value ([[1],[1]] / delete),
  V-002 C-018/C-019 relative-only, V-003 tests/map.md entry mangled. Class: "parity pin on a layout-dependent value or relative-only".
  Measured (probe/rd-c18-finding.md): all_* column lists Spark == RePark; Spark REFUSES `"t$snapshots"` on both doors (reader
  IllegalArgumentException "Cannot parse identifier", SQL PARSE_SYNTAX_ERROR 42601) while RePark serves it — a pre-existing divergence,
  declared in the ledger, follow-up. WO wo/rd-r6fix.md launched on devin; critic brief wo/critic-rd-r4.md ready.
- #806 md-r3fix (muse) CONCLUDED at 7a4b5381, comment gate 0; base still 88b6f59f -> WO wo/md-r4clerk.md (rebase + two stale doc blurbs)
  waits for a slot.

- 10:50 #811 critic r2 NEEDS_REMEDIATION (V-001 alias-scope leak: a CTE aliased like the Iceberg table got its input_file_name() rewritten). Fix WO ifn-r3fix queued. Follow-up gap: main's wildcard expansion shares the alias fallback in sole_rewritten_relation.

## tick 55 11:45 — #811 critic r3
Sol r3 (153457Z, 30 tools) = NEEDS_REMEDIATION at e88ebfc4: V-001 P1 a CTE named like the table, after `USE ice.ns`, is collected as the
physical table (shared collector; also a latent main `_file` leak); V-002 P2 prefix-only scalar-argument pin. Orchestrator's miss: the r3
work order listed the CTE-named-like-the-table near miss without setting the session defaults, so the pin passed by accident (same
scope-leak family as r2 V-001). Fix WO wo/ifn-r4fix.md (devin) queued; sweeps now include temp views (measure+report only).

## tick 61 12:20
- #806 md-r6fix concluded (critic r4 V-001..V-003 fixed; NamespaceNotFound -> 42P01; real-table-wins on the four-part DESCRIBE intercept; D-4 nested-namespace DESCRIBE gap recorded). Rebased onto 854ac2b6 -> c49ba03c, pushed, critic r5 fired, gate+lint running. ifn-r4fix launched.

## tick 62 12:25
- #806 critic r5 (Sol 162003Z, 22 tools) NEEDS_REMEDIATION at c49ba03c: V-001 uppercase-suffix not-found named in canonical case
  (new class: canonical-vs-written identifier); V-002 ledger still claims the deleted registry-defaults fallback and a stale pin count.
  V-002 is a REPEAT of the class swept in r6fix (prose-overstates-status) — orchestrator miss: the r6fix sweep order listed the ledger's
  status/evidence rows but not its history paragraphs, and my own PR body line 5 carried the same stale claim (fixed by me this tick).
  WO md-r7fix orders a sentence-by-sentence sweep with grep/count evidence. ifn-r4fix launch (not yet started) was yielded to the U0 fix.
## tick 65 12:37 — #805 critic r7
- Sol r7 (163146Z, 36 tools) NEEDS_REMEDIATION at bf9a147a, two P2: V-001 ledger rows C-001/C-004/C-008 still carry pre-r5 literals;
  V-002 docstring/ledger say "every refusal" compares with SQL (C-011/C-012 narrower). V-001 is a REPEAT of r5 V-002 — orchestrator miss:
  my rd-r8fix WO said "ledger append-only; never edit table rows", so the follow-up paragraph recorded the fix while the rows stayed stale.
  Ruling now: rows edited in place. Same classes found in my own wo/pr805-body.md (r5/r6 paras) and corrected by me.
- WO rd-r10fix (devin, docs/ledger only). The pending ifn-r4fix launch was stopped again for U0 priority.

## tick 73 13:14 — #805 critic r8
- Sol r8 (170833Z, 39 tools) NEEDS_REMEDIATION at d86e7f35: V-001 P1 "every answering test compares reader and SQL" (C-010, C-013 narrower),
  V-002 P1 parity doc "reader refusals carry the SQL door's texts verbatim" (C-011, C-012 narrower), V-003 P2 C-014 "backticked" unpinned.
- ORCHESTRATOR MISS: class "prose overstates the assertions" found a THIRD time. The r10 sweep order listed only "every/all/each/always" and
  did not name the parity doc; the r11 order enumerates every quantifier/strength word across docstrings, ledger, map and parity doc.
- Ruling: narrow the prose, add no assertion. WO wo/rd-r11fix.md (devin). ifn-r4fix launch stopped again for U0 priority.

## tick 82 13:52 — #806 critic r6
- NEEDS_REMEDIATION at 2162a675 (sol 174525Z, 44 tools). V-001 P1: my rebase resolution (NamespaceNotFound -> 42P01) broke the unchanged main
  test test_ice_views_2_describe.py missing-namespace pin (`No such namespace`); I had added that file to the re-gate, which would have
  caught it, but the critic got there first. Orchestrator miss: the rebase ruling did not grep main's tests for the old behaviour.
  Class "branch change vs unchanged test elsewhere" swept in md-r8fix. V-002 P2: C-008 refusal pin substring-only = REPEAT of the
  refusal-pin class swept in r6fix -> on me (my r6fix sweep scoped to "wrong class", not full message/condition/SQLSTATE).
- FYI claims line to xo-opus59 (IPI-40 owner): the view test is re-pinned to 42P01 in #806.

## tick 83 13:57 — #805 critic r9
- Sol r9 (174934Z, 45 tools) NEEDS_REMEDIATION at 5f738b4a: V-001 C-008 compares rows only while prose says rows+columns (prose-overstates, FOURTH
  finding — orchestrator failure: my r11 sweep order was sentence-first and omitted the ledger AT-* entries); V-002 C-011 ledger literal missing
  `Error during planning: ` (ledger-literal class, second); V-003 DataInvalid-on-multi-level-namespace probe reads as not-found.
- Rulings: V-001 add the column assertion (stop narrowing prose); V-003 no Rust change — the arm copies main's SQL-door rule
  (metadata_tables.rs:246-251, #219 Glue), AT-9 narrowed. Follow-up gap: a non-Glue DataInvalid probe is read as not-found on BOTH doors.
- Lesson: sweep orders for prose classes must be test-first (assertion matrix per test) and name every prose container incl. AT-* entries.
- Slot: ifn-r4fix devin round 175536Z had started seconds before I launched rd-r12fix; stopped it (2-round cap, U0 first); clone unchanged.

## tick 88 14:09
- ORCHESTRATING SESSION ruled: #805 prose findings no longer go through worker rounds; I fix prose myself, critic r10 once, hold on a prose re-find. Acked. #806 CI red = r6 V-001 (being fixed by md-r8fix).

## tick 92 (14:40)
- #805: rd-r12fix closed critic r9 (C-008 now asserts column names on both reads; C-011 ledger quotes the exact tested literal; AT-9 scoped to the #219 probe rule). Following the session ruling, I narrowed three leftover ledger sentences myself (2d37e883) instead of running another worker round. Pushed; critic r10 fired once at 2d37e883; gate re-running at that head.
- #811: ifn-r4fix (devin) launched in the freed slot.

## tick 93 14:46
- #805 critic r10 (sol 183942Z, 37 tools) = NEEDS_REMEDIATION, one P1 prose finding (provenance-overstated: type/nullability attributed to recorded cells; the live Spark leg pins them). r9 findings closed, code/tests clean per critic. Per the session ruling (claims 14:1x) no worker round: #805 HELD for the session's ruling; fix ready in wo/rd-r10-hold.md; claims 14:46. Orchestrator lesson: append-only per-critic paragraphs keep stale claims alive; the fifth prose-class finding is on me.

## tick 103 (15:1x)
- #805: orchestrating session ruled (b). I applied the r10 prose fix myself (e9a9ab61: docstring, map.md r2 para, ledger AT-10 now name the live Spark leg as the source of type/nullability pins; recorded cells = rows + column names). Comment/map/grammar/ruff gates 0; no re-gate (prose only; gate at 2d37e883 covers code). Scoped critic r11 fired. Root cause (ledger paragraphs appended per round) written to lessons.md B15.
- Residue slot: if r11 returns only a NEW prose finding, #805 queues anyway and the finding is logged here as a numbered residue for the session.

## tick 104 15:12
- #806 critic r7 (sol 190521Z, 36 tools, valid) = NEEDS_REMEDIATION at e12038af: V-001 P1 DESCRIBE `$` split first-vs-last disagrees with the
  fork's SELECT (rsplit_once, fork 604edca schema.rs:176-184) for `a$b$snapshots`; V-002 P3 branch deleted IPI-30 C-009 line in task/ledgers/staging/map.md.
  New classes: "`$` split disagrees with fork", "branch deletes an existing shared-ledger line". WO md-r9fix (devin).
- Follow-up gap (main, not this PR): time_travel.rs:169 split_once('$'); metadata_tables.rs:235 comment says the fork uses split_once (stale vs 604edca).

## tick 105 15:18
- #805: critic r11 (Sol 190814Z, 25 tools, valid) = PASS at e9a9ab61 (r10 V-001 closed; no code/test finding; no prose residue). Waiting only on CI (Build+import smoke, Rust test pending) before the queue.

## tick 108 15:38
- #805 QUEUED 15:38 at e9a9ab61 (critic r11 PASS = head, CI green 9/2 skipped, gate .done at 2d37e883 all 0 + docs-only since). drive-merge unit merge-805-153852. After merge: replay R-DF-LOAD-META, R-DF-LOAD-META-FILES-VERSIONASOF; rm clones.

## tick 109 15:46
- #811 ifn-r4fix (devin 184951Z) CONCLUDED at 5371be4f: CollectTables skips one-part names equal to any CTE name (planner fold); red-first + mutation at the collector seam; full-value scalar pin. Pushed; gate + critic r4 running.
- Executor Q1 accepted (lean). Follow-up gaps: (a) `USE <cat>.<ns>` does not update DataFusion planner default_catalog/default_schema; (b) prepare_metadata_column_sql registers __repark_mc_N temp views under live planner defaults (fails 'register_table does not support tables with data' when defaults point at a populated iceberg schema; lineage_columns.rs registers under datafusion.public). Statement-wide CTE skip is conservative: an outer one-part real table shadowed by an inner-scope CTE name falls through to today's error (refusal, not a wrong answer).
- dfload-r1 launched (xo58-dfl, devin).

## tick 110 15:49
- #811: critic r4 (Sol 194549Z, 26 tools, valid) = PASS at 5371be4f, no findings; the hand-back was text only because the sandbox blocked the file write. The critic's cargo legs could not run (read-only target), so the executor's green run and the local gate carry the tests. Queue waits on gate-xo55-ifn-154533 and CI at 5371be4.
- #805: queued 15:38, drive-merge still running (PR OPEN at e9a9ab61).
- tick 115 16:04: #805 MERGED 16:01 as 0002a7f2 (drive-merge TREE-EQUAL; queue-line format fix 16:00). Replay of R-DF-LOAD-META / R-DF-LOAD-META-FILES-VERSIONASOF running on the merged tree (before 09-23: NOT-PARSED / REFUSED-REGISTERED). /tmp/xr-xo55-rd removed; /tmp/xo55-rd goes after the replay.
- tick 115 16:05: #805 replay on merged tree: R-DF-LOAD-META NOT-PARSED -> EQUAL; R-DF-LOAD-META-FILES-VERSIONASOF REFUSED-REGISTERED -> EQUAL (+2 EQUAL). /tmp/xo55-rd removed.

## tick 118 16:11
- #806: md-r9fix (devin 192152Z) closed critic r7 V-001 (`a$b$snapshots` DESCRIBE split at last `$`, red-first pin C-020 + near misses C-021/C-022, mutation re-reds) and V-002 (C-009 map line restored). Rebased conflict-free onto 0002a7f2 -> 07927248; pushed; gate+lint and critic r8 running.
- Follow-up gaps (executor-observed, not edited): time_travel.rs:169 `split_once('$')` on the time-travel `$` name (same first-vs-last question); metadata_tables.rs:235 `contains('$')` refusal cites the fork's old split_once.
- Orchestrator slip: critic r8 was first fired with an unfilled brief (sed delimiter clash); stopped within a minute and refired filled.

## tick 123 (16:27)
- #811 (R-INPUT-FILE-NAME) QUEUED 16:19 at 5371be4f (critic r4 PASS, gate 0, CI green); drive-merge-811 running.
- #806 (R-MT-DESCRIBE): critic r8 (sol) at 07927248 = NR with prose findings only (pin count 22 -> 25; staging map.md pin citation). Fixed by the orchestrator in 5c569d13 (docs only); scoped critic r9 fired at 5c569d13.
- R-MC-DELETED: measured that fork pin 604edca already serves the `_deleted` scan mode (reader.rs:555 -> arrow/pos_apply.rs include_deleted). RePark-only unit, no fork PR and no RP-49 bump. Scoreboard before: REFUSED-UNREGISTERED (`[ICE-MC-1] metadata column _deleted is not yet served`). Lane xo58-mcd + devin WO wo/mcdel-r1.md launched.
- tick 128 16:44: #811 MERGED (head cf3692f4, main b7a3c90). Replay R-INPUT-FILE-NAME at the PR tree 5371be4f (replay811/): REFUSED-UNREGISTERED (09-23, UNRESOLVED_ROUTINE) -> EQUAL (Spark and RePark both [[2,true],[3,true]]; +1 EQUAL). /tmp/xo55-ifn and /tmp/xr-xo55-ifn removed. Residues from wo/ifn-r1.md fall-through list stay (count(DISTINCT), self join, VALUES, metadata table, UNION ALL outer SELECT).
- tick 140 17:48: #806 MERGED (main fd43f192). Replay R-MT-DESCRIBE at the PR tree 5c569d13 (replay806/): REFUSED-UNREGISTERED (09-23, TABLE_OR_VIEW_NOT_FOUND `t$snapshots`) -> EQUAL (Spark and RePark both 6 rows committed_at timestamp / snapshot_id bigint / parent_id bigint / operation string / manifest_list string / summary map<string,string>; +1 EQUAL). /tmp/xo55-md and /tmp/xr-xo55-md removed. Unit 2 (U0: #805 + #806) complete: R-DF-LOAD-META, R-DF-LOAD-META-FILES-VERSIONASOF, R-MT-DESCRIBE all EQUAL.
