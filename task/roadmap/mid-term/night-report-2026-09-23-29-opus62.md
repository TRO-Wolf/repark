# report-xo-opus62 — run 29 close-out, U10 lane A (FINAL; refreshed 2026-09-24 07:08)

## FINAL SUMMARY (07:08) — every item merged or residued
| item | cells | PR | before | after (after-merge replay on main) |
|---|---|---|---|---|
| 1 DFLOAD | R-DF-LOAD-PATH, R-DF-LOAD-METADATA-JSON | repark#819 MERGED 04:28Z (main 3cf263da) | 0/2 EQUAL (both refused "invalid table identifier") | 2/2 EQUAL `[[2,"b","y"],[3,"c","x"]]` (replaydfl/replay-after.out) |
| 2 MCDEL | R-MC-DELETED, R-MC-POS-MOR, R-MC-STAR-EXCLUDES | repark#822 MERGED ~11:05Z (main bf90513a, tree == PASS head ac7e5d6e) | R-MC-DELETED refused `[ICE-MC-1] metadata column _deleted is not yet served`; POS-MOR, STAR-EXCLUDES guard cells | 3/3 EQUAL: POS-MOR `[[2,0],[3,0],[4,1]]`, DELETED `[[1,true],[2,false],[3,false],[4,false]]`, STAR-EXCLUDES `[[2,"b","y"],[3,"c","x"],[4,"d","x"]]` (/tmp/xo-xo-opus62/replaymcd-main/replay.out, 07:06, head bf90513a) |

Numbered residues (cell name):
1. R-1 `R-TT-TIMESTAMP-AS-OF-EPOCH` — harness race, inherited; not reopened.
2. `R-DF-LOAD-SPELLINGS` — ledgered in #819.
3. `R-MC-RESERVED-NAME-SCAN` (ledger C-015) — unrouted shapes answer on a reserved-name user column where Spark refuses; ledgered in #822.
4. `R-MC-RESERVED-NAME-CLASS` (N13) — same text, ValidationException vs AnalysisException class gap; ledgered in #822.
5. `R-MC-IFN-COMMA-JOIN` (I1) — comma-join `input_file_name()` [UNRESOLVED_ROUTINE]; ledgered in #822.
Known divergences kept refused and ledgered in #822: C-008 (metadata columns over VERSION AS OF), C-013 (quoted `_DELETED`).

Housekeeping: /tmp/xo55-rid removed 18:43; /tmp/xo58-dfl + /tmp/xr-xo58-dfl removed 00:37; /tmp/xo58-mcd, /tmp/xr-xo58-mcd, /tmp/xo58-fork removed 07:08. Small probe dirs /tmp/xo58-mcd-r*probe (<1 MB each) kept as evidence.


## Item 1 DFLOAD (R-DF-LOAD-PATH, R-DF-LOAD-METADATA-JSON) — lane xo58-dfl, branch fix/u10-df-load-path
- devin r4 (round 20260923T222039Z) CONCLUDED at c2f719aa: is_direct_child filters the recursive local FileIO listing (r3 V-001 P1);
  V-002..V-004 pins; class B/C sweep tables in the hand-back. Comment gate 0; no skip-worktree entries.
- 18:52 gate+lint+replay chain queued (gatelint-xo58-dfl-185206); replay dir /tmp/xo-xo-opus62/replaydfl (Spark answers from scoreboard 2026-09-23).
- 18:55 Sol critic r4 launched on c2f719aa (brief /tmp/xo-xo-opus62/wo/critic-dfl-r4.md).
- **DONE: repark#819 MERGED 2026-09-24 04:28Z (main 3cf263da).** Cells before (main pre-#819): R-DF-LOAD-PATH and
  R-DF-LOAD-METADATA-JSON both refused "invalid table identifier" (0/2 EQUAL). After (after-merge replay on main tree,
  /tmp/xo-xo-opus62/replaydfl/replay-after.out, 00:30): both [[2,"b","y"],[3,"c","x"]] = Spark 4.1.2 (2/2 EQUAL).
  Residue ledgered in #819: R-DF-LOAD-SPELLINGS. Clones /tmp/xo58-dfl and /tmp/xr-xo58-dfl removed 00:37.

## Item 2 MCDEL (R-MC-DELETED + R-MC-STAR-EXCLUDES, R-MC-POS-MOR) — lane xo58-mcd, branch fix/u10-mc-deleted
- Sol critic r2 at 311c97fa = NEEDS_REMEDIATION (V-001 P1 combined _spec_id/_deleted pin counts only; V-002 P2 self-join
  pin non-discriminating; V-003 P2 ORDER BY pin sorted). Class: ledger clause claims more than its pin proves.
- Live Spark probe mcdq3 (zulu-17, /tmp/xo-xo-opus62/probe/mcdq3-spark.log): discriminating join a.id=b.id+1 WHERE b._deleted -> [[2,1]];
  MoR unfiltered projection keeps row 1 with _deleted=true. Table in /tmp/xo-xo-opus62/wo/mcdel-r3.md.
- Opus fix round r3 launches when the gate chain at 311c97fa releases the clone.
- KNOWN DIVERGENCE (VERSION AS OF) stays refused and ledgered (C-008).

## Residues
- R-1 R-TT-TIMESTAMP-AS-OF-EPOCH harness race — inherited residue, not reopened.

## Housekeeping
- /tmp/xo55-rid removed 18:43 (salvage bundle + patch in /tmp/xo-xo-opus62/salvage/).
- 18:58 tick 6: mcd gate at 311c97fa all 0 (CB=0 R=0 T=0 U=0 L=0); lint running. MCDEL PR body drafted /tmp/xo-xo-opus62/wo/pr-mcdel-body.md (KNOWN DIVERGENCE C-008 VERSION AS OF + C-013 quoted _DELETED). dfl chain + Sol r4 still running.

## Tick 7 (19:03)
- MCDEL: gate+lint+replay at 311c97fa all 0 (CB=0 R=0 T=0 U=0 L=0; CLIPPY=0 PANIC=0 FILESIZE=0); replay R-MC-POS-MOR [[2,0],[3,0],[4,1]], R-MC-DELETED [[1,true],[2,false],[3,false],[4,false]], R-MC-STAR-EXCLUDES [[2,"b","y"],[3,"c","x"],[4,"d","x"]]. Opus r3 (WO mcdel-r3.md) launched for Sol r2 NR V-001..V-003.
- DFLOAD: Sol r4 on c2f719aa = NEEDS_REMEDIATION (46 tools, valid): V-001 `file://tmp/...` falsely reports no metadata; V-002 C-007 pin checks one name only.
  Measured Spark (probe/dflfs-spark.out): every `file://<authority>/...` load refuses IllegalArgumentException `Wrong FS: <arg>[/metadata], expected: file:///`. Ruling: refuse identically (critic's normalise lean rejected, claims line). Opus r5 WO dfload-r5.md launched; stale gate chain at c2f719aa stopped.

## Tick 9 (19:25)
- MCDEL opus r3 CONCLUDED (3 commits; test + paperwork only; RePark matched live Spark P1-P8 on the first run; mutations a/b/c each failed one test). Rebased onto main 3383a54 (docs-only, no overlap) -> head 0ace83da. Comment gate 0, skip-worktree none. Diff read: helpers unsorted; ORDER BY pins assert order; sorted(..) only on the unordered legs; join-right P3/P4/P6 exact.
- Launched gate+lint+replay chain gatelint-xo58-mcd-192507 and Sol critic r3 (brief /tmp/xo-xo-opus62/wo/critic-mcd-r3.md) at 0ace83da.
- DFLOAD opus r5 still running (started 19:09).

## Tick 10 (19:37)
- MCDEL Sol critic r3 (round 20260923T232511Z, 41 tools, valid) = NEEDS_REMEDIATION at 0ace83da:
  V-001 MoR type/nullability not asserted (class B, ledger over-claim); V-002 parity doc ICE-MC-FILEPOS-1 + ice-metadata-cols-1 C-023
  still say `_deleted` refuses (class F, stale live record, new); V-003 user column named `_deleted` unpinned (class E, new).
- V-003 measured both engines (/tmp/xo-xo-opus62/probe/mcdcol-{spark,repark}.log): Spark refuses every scan with
  "Table column names conflict with names reserved for Iceberg metadata columns: [_deleted]. ..."; RePark leaks
  `__repark_mc_N._deleted` duplicate-field error. Ruling in run29 claims 19:48 (refuse references with exact Spark text;
  SELECT */DELETE served + WHERE-shape message = KNOWN DIVERGENCE, residue R-MC-RESERVED-NAME-SCAN).
- Stopped the r3 gate chain (obsolete). opus r4 launched (resume session d9fb89ee) with WO /tmp/xo-xo-opus62/wo/mcdel-r4.md.
- DFLOAD opus r5 still running.

## Tick 11 (19:41)
- DFLOAD opus r5 CONCLUDED (4 commits): file://<authority> refuses with Spark's exact Wrong FS text (5 Rust + 2 Python pins, red on c2f719aa, refusal-drop mutation red); C-007 now a before/after registration-inventory pin (temp-view mutation red); C-008 added; class B table narrowed unassertable claims. Rebased onto 3383a54 clean -> head 60770d43; comment gate 0; skip-worktree none; diff read OK.
- r5 Q1 (file:/// refused on "Spark 3.5") re-measured: /tmp/sparkm is Spark 4.1.2 and the old probe used an in-memory table without v<N>.metadata.json. On a hadoop table (probe/dflfs{2,3,4}-spark.out) Spark READS file:///abs, file:/abs, File:/abs, FILE:/abs, fIlE:///abs, file:////abs, file:/abs/.../vN.metadata.json; refuses file:<relative> with IllegalArgumentException `java.net.URISyntaxException: Relative path in absolute URI: <arg>`; non-lower-case scheme with authority and //abs raise an uncategorized Java RuntimeIOException. RePark r5 refuses file:/abs and mixed-case spellings.
- Ruling claims 19:40 (R1 normalise, R2 refuse relative, R3 keep Wrong FS, R4 KNOWN DIVERGENCE residue R-DF-LOAD-SPELLINGS). opus r6 launched (WO wo/dfload-r6.md, resume session 9b3cee17) under unit launch-xo58-dfl-r6-194026.
- MCDEL opus r4 still running.

## tick 14 (20:08)
- DFLOAD r6 (opus) CONCLUDED at af49f4c0: file: spellings per ruling 19:40 (R1 read, R2 URISyntaxException refusal), C-009 + residue R-DF-LOAD-SPELLINGS ledgered; 35 Rust / 23 Python pins green, three mutations red. Comment gate 0. Gate chain and Sol critic r5 launched.
- MCDEL r4 (opus) still running.

## Tick 15 (20:16)
- DFLOAD Sol r5 (af49f4c0) = NEEDS_REMEDIATION: V-001 P1 metadata-version range (u64 vs Java int), V-002 C-007 inventory pin only current catalog,
  V-003 two-slash refusal unpinned, V-004 docstring overclaim, V-005 map.md clause range. Ruling claims 20:15 (measure on Spark first).
  Stale gate chain stopped; opus r7 fix round launched (WO /tmp/xo-xo-opus62/wo/dfload-r7.md). MCDEL opus r4 still running.

## Tick 17 (20:38)
- MCDEL opus r4 exit 0 at 458fa042 (6 commits): V-001 MoR type/nullability asserts; V-002 parity doc + C-023 superseded; V-003 collision refusal with Spark text + provider no longer duplicates a user-carried reserved field. Red fab6745c (3 fails) -> green 28/28 metadata_columns, 1767 repark-spark lib. Mutations a/b1/b2 red. Comment gate 0.
- Launched: gate+lint+replay chain, Sol critic r4, Spark probe mcdjoin (join-level collision, multi-name order).
- DFLOAD opus r7 still running.

### Tick 18 (20:46)
- MCDEL: Sol critic r4 NEEDS_REMEDIATION (valid, 36 tools) — V-001 two-name bracket order claimed as Spark's without a measurement (class M), V-002 unreferenced-collision near miss pinned for `_file` only (class B). The Spark join/order probe had crashed on an IndentationError. Fixed it, added cases for the per-name and order questions, and relaunched it on both engines. Fix WO mcdel-r5 is drafted and waits for the measurement.
- DFLOAD: opus r7 still running.

## Tick 19 (20:46)
- MCDEL: probes mcdjoin + mcdorder landed (Spark 4.1.2 vs RePark 458fa042). The conflict bracket lists the colliding names the query references, in the table's declaration order, on both engines (6 queries match). Unreferenced reserved-name scans answer on both engines (all 5 names match). Join J1 (`p._deleted` joined to a table with a user `_deleted` column): Spark answers and RePark refuses, as main already did. It becomes residue candidate R-MC-RESERVED-NAME-JOIN. Ledger C-015's "Spark refuses every scan" is contradicted by the probes, so r5 narrows it. The opus r5 fix round has been launched (WO /tmp/xo-xo-opus62/wo/mcdel-r5.md). The mcdfile probe is still running.
- DFLOAD: the opus r7 round is still running.

## tick 20 (20:47)
- MCDEL mcdfile probe landed: user-_file table SELECT id,_deleted/_pos/_partition and WHERE _spec_id EQUAL; SELECT * Spark refuses (reserved-name [_file]), RePark answers -> R-MC-RESERVED-NAME-SCAN. user-_deleted table id/count/_file EQUAL. Detail: /tmp/xo-xo-opus62/probe/mcdfile-summary.md

## tick 22 (21:04) — DFLOAD r7 in
- opus r7 (round /tmp/opus-worker/xo58-dfl/20260924T002500Z) CONCLUDED, 5 commits: V-001 Java-int version range per Spark 4.1.2 measurement (dflver-spark.out: v2147483648 not a candidate, v2147483647 is, oversized hint falls back to the listing) via java_int_version on the v-stem and hint (numbered form keeps u64); V-002 every-catalog inventory pin (mutation shown); V-003 two-slash pin; V-004/V-005 prose/maps. Worker gates: 40 Rust, 25 pytest, clippy/panic-ban/fmt 0, replay both cells [[2,"b","y"],[3,"c","x"]].
- Rebased onto main 458718b5 -> head 9cb3b1ae (clean); comment gate hits=0; no skip-worktree.
- Gate chain gatelint-xo58-dfl-210346 and Sol critic r6 (brief /tmp/xo-xo-opus62/wo/critic-dfl-r6.md) running.
- New UNRULED residue recorded in the ledger by r7: sign-accepting parseInt / canonical v<int> read (v007) — not measured, left as ledger note.

## tick 23 (21:10) — DFLOAD Sol r6 = NEEDS_REMEDIATION (round xr-xo58-dfl/20260924T010414Z, 51 tools, head 9cb3b1ae)
- V-001 P2 class P (refusal pins use unanchored `match=re.escape`), V-002 P2 class B (inventory omits currentCatalog; restore unpinned), V-003 P3 class F (python/repark/tests/map.md and crates/repark-core/src/iceberg_path/map.md stop at C-007).
- ORCHESTRATOR FAILURE, recorded: V-002 and V-003 repeat classes B and F from r5. My r7 WO scoped class F to docstrings/ledger only (not every map.md) and asked for the catalog restore without asking for a pin on it. The r8 WO sweeps every map.md in every changed directory and every helper side effect.
- Gate chain gatelint-xo58-dfl-210346 stopped (head will change). opus r8 launched 21:10 (unit launch-xo58-dfl-r8-211003, WO /tmp/xo-xo-opus62/wo/dfload-r8.md, resume 9b3cee17). Test/map-only fix; product code unchanged.

## tick 25 (21:24)
- MCDEL opus r5 CONCLUDED (round /tmp/opus-worker/xo58-mcd/20260924T005510Z): closes Sol r4 V-001 (class M, bracket order now measured: declaration order on both engines, pinned O1-O3/O6-O9) and V-002 (class B, near-miss loop over every served reserved name). No production change; 29 metadata_columns tests green. New residue candidate R-MC-RESERVED-NAME-JOIN (ledger C-016, J1: Spark answers, RePark refuses; main refused too).
- Rebased onto 458718b5 -> dae592de. Gate chain gatelint-xo58-mcd-212411 + Sol critic r5 running.

## tick 26 (21:31) — MCDEL Sol r5 = NEEDS_REMEDIATION (head dae592de)
- V-001 class F: C-012 claims full-row pins everywhere; the `_spec_id` near-miss `_file` leg asserts id, count and `.parquet` suffix only.
- V-002 class N: the reserved-name collision check matches SQL words statement-wide, so an output alias (`SELECT id AS _deleted` on a user-`_deleted` table) refuses. The r5 work order's near-miss list did not name alias positions (output alias, table alias, CTE name, derived-table alias): an orchestrator gap in the near-miss list, same family as r4 V-002 (class B).
- Measurement first: probe mcdalias (A1-A10) on Spark and RePark; the r6 opus work order is cut on its rows.

## tick 27 (21:43)
- DFLOAD: opus r8 CONCLUDED (round /tmp/opus-worker/xo58-dfl/20260924T012005Z, 4 commits, tests/maps/ledger only; full-string refusal pins, current-catalog inventory pin, maps C-001..C-010; mutations red). Orchestrator found one more class-F miss (test module `pins:` line stopped at C-008) and fixed it in one line, 51b2f955 — third class-F hit on this lane, counted as orchestrator failure. Gate chain gatelint-xo58-dfl-214114 + Sol critic r7 (codex round xr-xo58-dfl/20260924T014137Z, brief wo/critic-dfl-r7.md) on 51b2f955.
- MCDEL: alias probe mcdalias A1-A10 landed. Spark answers every alias/table-alias/CTE/derived-alias shape and refuses only when the scan reads the user reserved column (A9); RePark refused A1,A2,A4,A5,A6,A10 (6 DIVERGENT, 4 EQUAL). Ruling (claims 21:46): collision check moves to the provider scan's projected user columns (Spark's validateMetadataColumnReferences rule); expected to also close R-MC-RESERVED-NAME-JOIN (J1). Opus r6 launched (launch-xo58-mcd-r6-214300, WO wo/mcdel-r6.md, resume d9fb89ee).

## tick 28 (21:47)
- DFLOAD Sol r7 = NEEDS_REMEDIATION (round xr-xo58-dfl/20260924T014137Z, 44 tools, head 51b2f955; critic sandbox was read-only, so it ran no tests). V-001 P2, class B-SET: C-004 covers ten option keys, the pins cover two. **Orchestrator failure:** this is the third class-B finding on this PR. The r8 class-B sweep ran one mutation per clause but never listed every member of a quantified set. The r9 sweep lists every member of every set that each clause quantifies over.
- Gate chain gatelint-xo58-dfl-214114 stopped and its .done files removed. opus r9 launched (unit launch-xo58-dfl-r9-214636, WO /tmp/xo-xo-opus62/wo/dfload-r9.md, resume 9b3cee17). Changes are limited to tests and the ledger.
- MCDEL opus r6 launcher (launch-xo58-mcd-r6-214300) still running.

## tick 31 (22:14)
- DFLOAD: opus r9 CONCLUDED (round /tmp/opus-worker/xo58-dfl/20260924T015639Z): r7 V-001 class B-SET fixed — every member of both refused option sets pinned full-string; B-SET sweep table over C-001..C-010 closed four more unpinned members (before-resolution order, non-candidate-only dir, C-006 selectors/options, C-010 range endpoints). Ruling r9-Q1: docstring stays (open sets, structural + representative pins). Rebased onto c7879a91 -> 43241e6c. Gate chain + Sol critic r8 running.
- MCDEL: opus r6 still running.

## tick 32 (22:22)
- DFLOAD: Sol r8 on 43241e6c = NEEDS_REMEDIATION, 3 findings (the critic's filesystem was read-only, so it ran no tests; it read the code only). V-001 C-010 numbered-form u64 range has no pin above u32::MAX; V-002 C-009 "any scheme case" has no mixed-case single-slash pin (both class B-EDGE); V-003 stale test name `test_load_path_with_time_travel_option_refuses` at parity doc:264 and ledger:129 (class F-NAME). The gate chain on 43241e6c was stopped. opus r10 (WO wo/dfload-r10.md) was launched with exhaustive B-EDGE and F-NAME sweeps; the F-NAME sweep script is in the WO.
- MCDEL: opus r6 still running.
- 22:47 tick 34: MCDEL opus r6 CONCLUDED (scan-level collision check, A1-A10 EQUAL, J1 answers, C-016 PROVEN, C-017 B2/B7 divergences). Rebased -> 6079369e, CB 0. Gate chain + Sol critic r6 started. DFLOAD opus r10 still running.

## Tick 35 (22:53)
- DFLOAD: opus r10 CONCLUDED (b0bc85a9, 5 commits, tests/docs/ledger/maps only). Gate chain + Sol critic r9 started.
- MCDEL: Sol r6 on 6079369e NEEDS_REMEDIATION — V-001/V-002 class F (legs asserting less than C-012/C-017 claim), V-003 class N (JOIN USING position unmeasured). Class N second hit: my r6 sweep brief listed positions from memory; r7 enumerates them from the grammar and sweeps F per assert leg. opus r7 launched (mcdel-r7.md).
- 22:58 tick 36: DFL Sol r9 on b0bc85a9 = NR (V-001 C-002 scope, V-002 C-007 time, V-003 C-004 count; class F-CLAIM). Chain gatelint-xo58-dfl-225210 stopped, .done removed. opus r11 launched (WO wo/dfload-r11.md, text only, unit launch-xo58-dfl-r11-225747).

## tick 37 (23:06)
- DFL: opus r11 (/tmp/opus-worker/xo58-dfl/20260924T025808Z) CONCLUDED, head e287f3a9, text only (r9 V-001..V-003 narrowed; F-CLAIM sweep table in hand-back). Comment gate 0, no ^S, main unchanged. Gate chain gatelint-xo58-dfl-230546 + Sol critic r10 (20260924T030548Z) running.
- MCD: opus r7 still running (opus-xo58-mcd-025801).

### tick 38 (23:11)
- DFLOAD: Sol r10 on e287f3a9 = NEEDS_REMEDIATION, one P3 (V-001, pre-existing `load` docstring reader.py:412-415 says iceberg load takes a table name only; false since the `/` route). Class F-STALE (5th F hit; r11 sweep covered only added text). Stale gate chain stopped. opus r12 launched (WO /tmp/xo-xo-opus62/wo/dfload-r12.md, resume 9b3cee17), docstring + sweep of pre-existing text; no product change.
- MCDEL: opus r7 still running.
- 23:25 tick 40: DFLOAD opus r12 CONCLUDED (docstring/md F-STALE sweep, head ce66821f); ruling r11 (reader.py:457 no-arg error string out of scope); gate chain gatelint-xo58-dfl-232412 + Sol critic r11 launched. MCDEL opus r7 still running.

## Tick 43 (23:38)
- DFLOAD: PR repark#819 OPENED at ce66821f (on main c7879a91). Evidence: Sol critic r11 PASS for ce66821f (66 tools, valid); local gate CB=0 R=0 T=0 U=0 L=0; CLIPPY=0 PANIC=0 FILESIZE=0; comment gate 0; skip-worktree none.
  Pre-queue replay (replaydfl/replay.out): R-DF-LOAD-PATH and R-DF-LOAD-METADATA-JSON — RePark [[2,"b","y"],[3,"c","x"]] = Spark 4.1.2 [[2,"b","y"],[3,"c","x"]] -> EQUAL (before: both refused "invalid table identifier").
  Residue R-DF-LOAD-SPELLINGS ledgered in the PR. Waiting on CI; then queue + post-merge replay + rm clones.
- MCDEL: opus r7 (unit opus-xo58-mcd-025801) still running.

## Tick 47 (00:00)
- MCDEL opus r7 HALT (exit 0, 6 commits 6079369e..ee655264, tests/docs only, all gates 0; V-001..V-003 closed; F per-leg sweep 85 legs; N grammar sweep N1-N19).
  Q1 raised: R-MC-QUALIFIED-WILDCARD (C-018): `x.*` under a non-table alias + any metadata column expands every provider field, projecting `_deleted` resurrects deleted MoR rows (X3 RePark [1,2,3,4] vs Spark [2,3,4]).
- RULING (claims 00:0x): fix in this PR (COMMON29 rule 4, silent wrong answers first). Opus r8 launched (WO /tmp/xo-xo-opus62/wo/mcdel-r8.md, resume d9fb89ee) — core rewriter qualifier-by-FROM-alias fix, X1-X3/N13/Q1/Q2 flipped to Spark, near-miss legs.
- DFLOAD #819: CI pending 1 at ce66821f.
- 00:28 tick 51: #819 CI green at ce66821f (rerun cleared the unpivot perf flake). Sol PASS r11 for this head + localgate 0 + replay EQUAL -> queued 819 DFLOAD 00:28, drive-merge merge-819-002822. MCD r8 running (3 commits: red pins, alias fix, paperwork).
- 00:31 tick 52: **DFLOAD repark#819 MERGED** 04:28Z (main 3cf263da; tree identical to the PASS head ce66821f). Cells: before R-DF-LOAD-PATH / R-DF-LOAD-METADATA-JSON both refused ("invalid table identifier"); pre-queue replay EQUAL [[2,"b","y"],[3,"c","x"]]; after-merge replay running (replay-dfl-after-003023, output /tmp/xo-xo-opus62/replaydfl/replay-after.out). Residue ledgered in #819: R-DF-LOAD-SPELLINGS.
- MCDEL opus r8 CONCLUDED (3 commits): C-018 fixed in repark-core metadata_columns.rs (`qualified_rewrite` resolves `x.*` via this SELECT's aliased FROM relations, `sole_relation_alias` qualifies bare `*`); X1-X3, B1/B2, K1, Q1/Q2, P1/P2, N13 now EQUAL; near misses W1-W5, D1, C1, P3 still EQUAL. Orchestrator read the product diff: OK. Rebased onto 3cf263da -> 66f7b647, CB 0, no skip-worktree. Gate chain gatelint-xo58-mcd-003023 + Sol critic r7 (brief wo/critic-mcd-r7.md) running.
- 00:37 tick 53: DFLOAD after-merge replay EQUAL 2/2 -> clones removed. MCDEL Sol critic r7 on 66f7b647 = NEEDS_REMEDIATION (41 tools, valid; read-only sandbox, no tests run). V-001 P1: wildcard expansion falls back to the statement-global `by_alias`, so an unrelated relation (plain table/view/CTE/derived) sharing the Iceberg table's alias in another SELECT scope gets expanded with the Iceberg columns (silent column drop), in `sole_rewritten_relation` and `qualified_rewrite`. Orchestrator confirmed from the code (metadata_columns.rs:296,420,486,501). Class R: statement-global lookup used for a per-SELECT decision. Gate chain on 66f7b647 stopped (obsolete). Opus r9 launched (launch-xo58-mcd-r9-003526, WO wo/mcdel-r9.md, resume d9fb89ee): per-SELECT scoping, S1-S5 Spark-measured pins, class R sweep over every lookup, mutations.
- 01:12 tick 55: MCD r9 still running (3 commits to 268c18ca, no hand-back yet). #819 re-verified MERGED. Chain script extended with the r9 scope probe.

- 01:18 MCD r9 (opus, round 20260924T044528Z) CONCLUDED: V-001 (class R) fixed — by_alias removed, rewrite_for_relation structural; S1-S8 EQUAL (S1/S4B/S3B/S6 changed), r8 rows unchanged, M2 text divergence (both refuse). Mutations a/b/c red. Rebased onto a6e8bcda -> head 4003c964, CB 0. Gate+lint+replay chain and Sol critic r8 launched.
- 01:23 tick 57: MCDEL Sol critic r8 on 4003c964 = NEEDS_REMEDIATION (44 tools, valid; read-only). V-001 P2: `rewrite_for_relation` also matches the `__repark_mc_N` replacement name, but pre_visit_select runs before pre_visit_table_factor renames, so the arm looks dead, and C-019 claims it. Class T (a code arm or ledger sentence no test reaches); it is a new class, not a repeat of R. Gate chain on 4003c964 stopped. Opus r10 launched (unit launch-xo58-mcd-r10-012254, WO /tmp/xo-xo-opus62/wo/mcdel-r10.md, resume d9fb89ee): run the mutation first, then remove the arm or pin it, plus a class-T sweep over every r8/r9 arm.
- 02:33 MCD r10 (opus, round 20260924T052756Z) CONCLUDED at 179a72df: replacement-name arm removed (dead), two more dead arms removed, C-019 narrowed, M-22 added. Comment gate 0.
- 02:41 tick 62: MCDEL Sol critic r9 on 179a72df = NEEDS_REMEDIATION (34 tools, valid; read-only, no builds). V-001 P2: M-22 claims every live arm was mutated and each reds a test, but the saved files hold only T16/T17/N3/N6 (T1-T15 results overwritten, `rewrite_input_file_names` never mutated). Class F (ledger stronger than its saved evidence). **This is a repeat of class F on this PR = orchestrator failure**: r10's WO asked for a sweep table but did not require saved per-mutation results, and the critic's sandbox is read-only, so it can only check saved files. Gate chain gatelint-xo58-mcd-023153 stopped. Opus r11 launched (unit launch-xo58-mcd-r11-023716, WO /tmp/xo-xo-opus62/wo/mcdel-r11.md, resume d9fb89ee): one script re-runs every arm mutation at current code, saves JSON + logs under /tmp/xo58-mcd-r11probe/, and M-22 cites only those files; class-F sweep over C-016..C-019 and M-20..M-22.
- 04:25 tick 67: MCD r11 (opus, round 20260924T064718Z) CONCLUDED: every mutation saved in /tmp/xo58-mcd-r11probe/mutations-r11.json (M01-M24 live arms incl. rewrite_input_file_names, R1-R5 regressions, A1-A3 dead-arm re-adds). Class-T gap M22 (comma-join guard) closed by new core leg `input_file_name_over_a_comma_join_is_left_unresolved`; the comma-join residue ([UNRESOLVED_ROUTINE] vs Spark, mcdifncomma I1) is ledgered. No pinned answer changed; metadata_columns.rs 991/1000 lines. Rebased onto origin/main 4b1688f2 (fork repin) -> head f3dd7b7a; ledger SHAs mapped in /tmp/xo-xo-opus62/wo/mcd-sha-map-r11rebase.txt. Gate chain (gatelint-xo58-mcd-042500) + Sol critic r10 (critic-mcd-r10.md) launched.
- 04:30 tick 68: MCDEL Sol critic r10 on f3dd7b7a = NEEDS_REMEDIATION (34 tools, valid; read-only). V-001 P1: M-20 lists N13 as EQUAL, but Spark raises ValidationException (via Py4JJavaError) and RePark raises AnalysisException with the same text (the IPI-51 class gap), and the pins compare text only. V-002 P2: M-22 says the core leg pins the comma-join `input_file_name()` [UNRESOLVED_ROUTINE] residue, but the leg asserts only the rewritten SQL. **Class F repeat (4th on this PR) = orchestrator failure, recorded:** the r11 class-F sweep checked that every cited file exists. It did not compare error CLASS on ERR rows (COMMON.md replay rule), and it did not check that a "pins X" test actually asserts X. The r12 WO sweeps both (F-CLASS, F-PIN) over the whole PR. Gate chain on f3dd7b7a stopped. opus r12 launched (launch-xo58-mcd-r12-043003, WO /tmp/xo-xo-opus62/wo/mcdel-r12.md, resume d9fb89ee). New residues to be ledgered: R-MC-RESERVED-NAME-CLASS (N13), R-MC-IFN-COMMA-JOIN (I1).

- 2026-09-24 04:59 (tick 71): owner override received — finish extended to 09:30 EDT; critic cap 6 per PR from 04:59. MCDEL opus r12 still running; PR not yet opened.

### 05:05 tick 73 — U10-mcdel r12 in
- opus r12 (round 20260924T084005Z) closed critic r10 V-001/V-002 with test pins and ledger changes only; residues R-MC-RESERVED-NAME-CLASS (N13 and the reserved-name refusal cells) and R-MC-IFN-COMMA-JOIN (I1) added to the numbered residue list.
- Rebased onto main 970ac11a -> ec5b4f2d; stripped the banned trailers trailers from the two r12 commits; SHA map /tmp/xo-xo-opus62/wo/mcd-sha-map-r12rebase.txt.
- Gate+lint+replay chain and Sol critic r11 launched 05:04 (critic round 1 of 6 under the 04:59 cap).
- 05:08 MCDEL: Sol critic r11 on ec5b4f2d (29 tools, valid) NEEDS_REMEDIATION — V-001 P2 class F (sub-class F-META): U10 ledger header "Registry: no registry row touched" while the diff changes ICE-MC-FILEPOS-1 (and ICE-MC-1 / MC-DELETED-1 ids). Repeat of class F = orchestrator gap: the r12 sweep covered pin claims only, not ledger header metadata. opus r13 (mcdel-r13.md, markdown only + F-META sweep of both ledgers' headers) queued behind the running gate chain.
- 05:17 tick 75: ack 05:14 ruling; scoped critic r12 brief ready (wo/critic-mcd-r12-filled.md, placeholders <HEAD13> <HB13>); r13 waits on chain gatelint-xo58-mcd-050441 (still running, head ec5b4f2d).
- 05:44 tick 77: MCDEL chain gatelint-xo58-mcd-050441 on ec5b4f2d ALL GREEN: local gate CB=0 R=0 T=0 U=0 L=0; CLIPPY=0 PANIC=0 FILESIZE=0; replay R-MC-POS-MOR [[2,0],[3,0],[4,1]], R-MC-DELETED [[1,true],[2,false],[3,false],[4,false]], R-MC-STAR-EXCLUDES [[2,"b","y"],[3,"c","x"],[4,"d","x"]] (unchanged, EQUAL); probes mcdalias-r8=0, mcdscope-r9=0; no skip-worktree files. Opus r13 (markdown-only F-META fix, WO wo/mcdel-r13.md, resume d9fb89ee) launched as unit opus-xo58-mcd-054434. Per ruling 05:14: no re-gate if diff is .md-only; one scoped critic r12 next.

- 05:59 tick 79: mcdel r13 CONCLUDED (d33dc832, markdown only: ledger Registry names ICE-MC-FILEPOS-1, F-META header sweep). Comment gate 0; no re-gate per #805(b). Scoped critic r12 launched.
- 06:05 tick 80: MCDEL Sol critic r12 (round 20260924T095914Z, 12 tools, valid; scoped per #805(b)) = PASS at d33dc832 (header audit 4/4 TRUE, r13 delta 3 .md files, 0 code files). **repark#822 opened** at d33dc832 (body wo/pr-mcdel-body.md: cells, residues 1-3, SHA maps, evidence). Waiting for CI; then queue + drive-merge + after-merge replay + rm clones.
- 06:30 tick 85: #822 went CONFLICTING (GitHub) after main moved 970ac11a -> d4caca39 (DESCRIBE column feature; overlaps tests/mod.rs, map.md files, parity doc). Local merge-tree clean. Rebased -> head ac7e5d6e (range-diff 43 '=', patch 16 docs/map.md context only; CB 0; no ^S; SHA map /tmp/xo-xo-opus62/wo/mcd-sha-map-r14rebase.txt; pre-rebase head kept on backup/mcd-pre-r14rebase). Pushed via xpr.sh; PR MERGEABLE. Re-gate chain gatelint-xo58-mcd-062951 + rebase-scoped Sol critic r13 (brief wo/critic-mcd-r13-rebase.md; critic count 3/6 since 04:59) running.
- 06:33 tick 86: critic r13 (20260924T103007Z) VOID on format only (summary lacked the verdict word; no defect). Refired as r15 with an evidence-first brief.
- 06:35 tick 88: Sol critic r15 (20260924T103247Z, 19 tools, valid) = PASS at ac7e5d6e (range-diff 43 '=', patch 16 map.md only; both test-module sets kept; DESCRIBE parsers do not intercept SELECT metadata routing). Still waiting on the gate chain gatelint-xo58-mcd-062951 and CI #822.
- 06:47 tick 91: gate chain gatelint-xo58-mcd-062951 ALL GREEN at ac7e5d6e (local gate CB=0 R=0 T=0 U=0 L=0; CLIPPY/PANIC/FILESIZE 0; replay R-MC-POS-MOR, R-MC-DELETED, R-MC-STAR-EXCLUDES all ok, same rows as before; probes r8/r9 0). Only CI #822 build + import smoke still running.
- 07:00 tick 92: #822 CI green at ac7e5d6e (9 SUCCESS, 2 SKIPPED), mergeState CLEAN, not behind main d4caca3. QUEUED '822 U10-mcdel 07:00'; drive-merge unit merge-822-070028. After merge: replay the 3 cells on main (before/after), rm -rf /tmp/xo58-mcd /tmp/xr-xo58-mcd /tmp/xo58-fork, final report, DONE.
- 07:07 tick 93: **MCDEL repark#822 MERGED** (main bf90513a; tree identical to the PASS head ac7e5d6e). After-merge replay of R-MC-POS-MOR, R-MC-DELETED, R-MC-STAR-EXCLUDES running on main (unit replay-mcd-main-070555, output /tmp/xo-xo-opus62/replaymcd-main/replay.out); clones removed once it is banked.
- 07:08 tick 93: after-merge replay on main bf90513a 3/3 EQUAL; clones /tmp/xo58-mcd /tmp/xr-xo58-mcd /tmp/xo58-fork removed. All items merged or residued -> DONE.
