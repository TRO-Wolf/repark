# Report xo-muse7 — NON-Iceberg PySpark functions parity (run27, FINAL)

Lane: xo-muse7 (FULL-MUSE: Muse max executors, GLM 5.3 Flash mechanical, Devin clerk-only, Grok critic).
Shift: day + NIGHT SHIFT extension to 06:00 2026-09-22; run ended by finish-time order at tick 91.
Outcome: 0 merges. Both active units end one rebase short of the gate with all content
green: agg rebase follow-up 13f2 CONCLUDED exit=0 (banked, not yet own-verified at
cutoff); math follow-up 9f2 still RUNNING healthy at cutoff. Nothing was pushed in a
red or unverified state; every accept in this lane was comment-gate-first with own-run
gates. 15 main moves endured, 12 local gates 5x0, 0 gate failures, 0 formal questions.

## Units

UNIT 1 — FNP-AGG-1 (rescue of #625, 11 aggregates). Slice-(d)-first plan (grouping_id +
foundation, tick 16) held all run. Slice built, accepted, rebased 13x across 15 main
moves; head @c7dfd93a (rebase-13 Base 2c782a99) PARTIAL-accepted tick 88 after rb13 died
in the box-wide provider-network incident with the tree complete (own full verification
green; only the worker LINT-trio rows died unbanked). Follow-up 13f2 (verify + trio,
ZERO commits) CONCLUDED exit=0 handback=yes at cutoff — banked, own-verify owed. No PR
opened yet (open-early owed right after the next gate). Remaining slices (c),(b),(a)
lean TBD after (d) lands.
UNIT 2 — FNP-MATH-1 (rescue of #628, 8 names). PR repark#779 open; critic r1 remediated
(all 6 findings + V-007), rebased 9x; pushed @9a306e15 (tick 68, MERGEABLE, fresh CI);
CI Python red (freeze 890-vs-889, fixed @5ed5d44b) + smoke red (tuning Cholesky, cause
PROVEN = hash-destub-flips-consumer-path, fixed @7bff4570 + ml-row @5b31a301) both fixed
in unpushed heads; head @f2f2bdae (rebase-9 Base 2c782a99) PARTIAL-accepted tick 88
(same incident, tree complete, own full verification green). Follow-up 9f2 (verify +
trio + example-coverage) RUNNING healthy at cutoff (~11 min, 488KB @06:07). Push +
fresh critic owed after the gate. Steps 8-10 unbuilt (separate follow-up).
UNIT 3 — aes x3 / collate / collation. NOT STARTED. Owns the 94 strict-xfail backlog
WO-4 fenced ("flips when the follow-up lands"). AES crates pre-approved; collate needs
design note + QUESTION first.
UNIT 4 — #662 metadataColumn. NOT STARTED.
Residue (out of lane, UDTF helpers): AnalyzeArgument, AnalyzeResult, ArrowUDFType,
OrderingColumn, PartitioningColumn, SelectedColumn, SkipRestOfInputTableException.

## PRs

- repark#625 OPEN CONFLICTING @4a3379fe (old head, untouched; closes with pointer once slices land).
- repark#628 OPEN CONFLICTING @10f18d40 (old head, untouched; closes with pointer after #779 lands).
- repark#779 OPEN CONFLICTING @9a306e15, CI 2 fail / 7 pass / 2 skip (world status 06:07).
  Opened + updated twice by this lane (xpr.sh exit 0 x3). The 2 reds on the pushed head
  are both fixed in unpushed heads: (1) Python job test_freeze_inventory (890-vs-889
  FROZEN-PIN-DRIFT from native ArithmeticException, fixed by WO-7 regen @5ed5d44b);
  (2) smoke example-coverage on deterministic tuning.py (native Cholesky refusal via
  the newly-answering hash primary path, fixed by WO-8 fold-degeneracy fallback
  @7bff4570 + ml map row @5b31a301). CONFLICTING bit flipped by the 14th/15th moves;
  the remediation-head push resolves it.

## Rounds per executor tier (all mine)

Muse family (muse-spark-1.3-contributor): 36 rounds — 31 accepted, 1 concluded-banked
(13f2, own-verify owed), 4 infra-deaths, 1 running.
- Max-effort content (exit 0, handback yes): agg 105756Z (hb=no, superseded), 120147Z,
  131853Z, MEASURE 133231Z, SLICE-(d) 141455Z (202/400 steps); math 134621Z, WO-2
  135848Z, WO-3 141500Z, 6b-r3 214554Z (~110 min), WO-8 055511Z (HALT-accepted, lib-py
  fence tripwire worked as designed).
- Max-effort infra deaths (WO healthy, split rule NOT triggered): math 6b-r1 195824Z
  exit 1 (81/400 tools, provider transport x5); math 6b-r2 211004Z exit 1 (80/400,
  box-wide network incident; banked 2 sound commits via partial accept).
- Clerk (medium/150): agg rb1 152130Z, rb2 163020Z, rb3 195824Z, rb4 211004Z, rb5
  230651Z, rb6 003848Z, rb7 015809Z, rb8 025253Z (HALT-accepted, pin-shape tripwire),
  8f2 032025Z, rb9 035650Z, rb10 055508Z, rb11 064041Z, rb12 080415Z (all exit 0 +
  accepted); rb13 090943Z DIED exit 1 hb=no (44/150, box-wide incident 05:33-05:35,
  tree complete -> PARTIAL-accept @c7dfd93a); 13f2 095607Z CONCLUDED exit=0 hb=yes
  (banked, own-verify owed). Math: rb2 163018Z, rb3 235852Z (orchestrator identity
  reset-author, tree-identical proof), rb4 004451Z, rb5 015938Z, rb6 031143Z
  (2 same-spot conflicts resolved keep-both), rb7 034949Z, rb8 080415Z (HALT-accepted,
  straddled 14th move, proceed path); rb9 092209Z DIED exit 1 hb=no (22/150, same
  incident, tree complete -> PARTIAL-accept @f2f2bdae); 9f2 095607Z RUNNING at cutoff.
- At DONE: agg 13f2 concluded (status CONCLUDED, questions [], summary claims trio
  banked green — NOT own-verified, never trust a handback); math 9f2 healthy ~11 min.
  NEVER re-issue either (prove death first).

GLM 5.3 Flash (opencode, zai/glm-5.3-flash): 8 rounds, all exit 0, handback yes.
- math WO-4 fence 145521Z (68 steps, $0.1796); math 6a-r1 175607Z (51, $0.1122,
  HALT-accepted, sweep tripwire worked as designed); agg WO-6 r1 182659Z (22, $0.0450,
  HALT-accepted, -w false positive -> OP9); agg 6f2 190130Z (9, $0.0282);
  math 6a-followup 184316Z (83, $0.3418); math WO-7 freeze 045658Z (cost unrecorded);
  math 8f2 063820Z (40 steps, $0.0490, HALT-accepted, ml-lockstep tripwire);
  math 8f3 071611Z (14 steps, $0.0267). Metered total: $0.7825 over 7 costed rounds
  (WO-7 cost not recorded in state), 287 costed steps.

Devin (free): 2 rounds. Math WO-5 172544Z exit 0 (ruff fix; last successful Devin
round box-wide). Agg WO-6 181045Z exit 1, no handback, zero commits (tier outage
`Unknown model: swe-2-high`; Devin EXACTLY-ONCE for WO-6 consumed, GLM covered it).

Critic (Grok via xreview): 2 rounds. Math r1 @45ca788b NEEDS_REMEDIATION (4 P1 + 2 P2;
all premises independently verified; remediation landed @d63b59f2; xr clone removed
after banking). Math r2 @9a306e15 VOID (1-turn fabrication: 13s, num_turns=1,
gates=[] commits=[], verdict discarded, xr clone removed; 4th such fabrication that
night, independently confirmed). Agg: no critic yet (owed after first PR). Genuine
critic rejections of lane content: 1 (r1, fully remediated).

## Questions asked

Formal QUESTION lines filed: 0. All design questions were settled by measurement or
in-repo precedent (never invented). Internal rulings: fence strict=True; WO-6-Q1
(AST-equality supersedes `diff -w`); 6a-Q1 (EXTEND V-001 to 5 rewrites + V-007 joins);
6b-V-006-DEVIATION (one collision pin, own both-ANSI probe); RB8-Q1 (SKIP pin ratchet,
OP15 PIN-RATCHET-NEEDS-SLACK); WO-8-Q1 ((c) new-file self-service, OP18); 8f2-Q1
(extend ml row now); rb8-Q1/Q2/Q3 (exact-6-file battery, accept-path, bare builder
command, OP19 FULL-PATHS-IN-WOs). Lessons OP6-OP19 banked in the state file.

## Before/after cell counts

Cells close only on merge + replay; 0 merged, so 0 closed. Suite-level before/after:
UNIT 1: before = #625 unverified, never gated. MEASURE: 12/12 names bind, Python door
12/12, SQL 9/12 (3 refuse UNRESOLVED_ROUTINE exactly like live Spark 4.1.2); suites
204 passed / 17 xfailed / 0 failed; 11 kernel modules green (89 tests); 11 value cells
+ 4 SQL refusals + byte-exact sumDistinct warning confirmed live; EX-0 delta = 12
names. After = grouping slice @c7dfd93a: 24-pass suite green on 13 successive rebased
heads, whole-crate cargo test 852 passed / 0 failed (845 + 7 base startswith tokio
tests), EX-0 1082->1083, 6 xgates 5x0. Remaining 10 names still only in #625.
UNIT 2: before = #628 unreviewed; WO-3 fresh-binary measure 141 passed / 8 xfailed /
94 failed (ALL 94 = unfinished-name params). After = @f2f2bdae: 143 passed /
101 xfailed / 0 failed / 0 xpassed (own-run EXACT), FULL-file battery 189 passed,
coverage script rc 0 (1078/966/111/1/249), EX-0 1082->1085 (+3 facade), BOTH ruff
green, cap-1 DUAL-PIN drift fixed (49 green + DL-1 13), freeze file 22 green, builder
check green, tuning.py rc 0, 6 xgates 5x0. Net vs WO-3: +2 passed, +93 xfailed fenced
(94 backlog now strict), 0 red.
UNIT 3/4: 0 cells (not started).

## Gate and CI record

Local gates (mine): 12 concluded, 12 GREEN 5x0, 0 failures (agg xgate-1, xgate-3
@60d86671, xgate-4 @413ef1b1, xgate-6 @7608deb7, xgate-7 @c917c32c, xgate-8 @f8dbf686;
math xgate-1 @37acc398, xgate-2 @45ca788b, xgate-3 @ba0f9307 warm-cache <60s every leg
header-verified, xgate-5 @1a35b6f1, xgate-6 @418aff18, xgate-7 @9a306e15 — the last on
CURRENT main, first such in the lane). Agg xgate-5 STOPPED pre-slot on a just-moved
base (OP14, zero waste — not a failure). Stale receipts always banked + deleted before
relaunch. LINT BEFORE PUSH satisfied at every pushed/accepted head. Comment gate:
own-run 0 hits before EVERY accept, all 91 ticks. Main moves endured: 15 (#777, #778,
#782, #774, #781-PR2a, #784, #788, #783, #780, #790, #789, #785, #794 incl RP-46 pin
bump 97f9b8a3->311b9fa4, #793, #792) — each measured zero-FNP-overlap; 22 rebase
accepts (agg 13 + math 9), all subjects-identical + redelta set-exact + key-files
zero-delta. CI: only pushed head is #779 @9a306e15 (2 known reds, both fixed
unpushed). No other heads pushed.

## Cost

Metered: GLM $0.7825 (7 costed rounds; WO-7 cost unrecorded). Muse: 31 accepted
rounds + 4 lost to provider/box-network incidents (~227 wasted steps, no WO fault:
6b-r1 81, 6b-r2 80, rb13 44, rb9 22) + 1 concluded-banked + 1 running at cutoff.
Devin $0. Grok: 2 critic rounds (r1 38 turns genuine; r2 1-turn void). Disk: 622G
avail at close, 150G floor held all run; no clones removed (nothing merged — DISK
rule had no trigger). Dominant cost was the 15-move rebase chase: 22 clerk rebases
to hold two branches mergeable with zero merges to show for it.

## What I would do next (handoff order)

1. Agg: 13f2 handback -> own-verify (comment gate FIRST + trio rows banked green +
   redelta/subjects re-verify) -> on exit=0 FULL-accept @c7dfd93a -> cut rebase-14
   onto 8de204f6 (redelta = #792-exact-10, banked tick 89; shared surface = py tests
   map tail only; rust-size +2 NEW .rs) -> clerk -> fresh xgate -> on 5x0 pre-push
   audit -> xpr.sh NEW PR (smallest slice first, open-early owed) -> FRESH xreview
   (add xr-fnp-agg to LANES; grok-4.7 default; brief carries the evidence rule +
   explicit xr-clone path guard + hollow-critic arrays-required rule + critic-venv-path
   pin) -> CLASS-SWEEP any finding -> queue.
2. Math: 9f2 handback -> own-verify (comment gate FIRST + trio + example-coverage
   rows banked?) -> on exit=0 FULL-accept @f2f2bdae -> cut rebase-10 onto 8de204f6
   (same #792-exact-10 redelta) -> clerk -> fresh xgate -> pre-push audit -> xpr.sh
   update #779 (resolves CONFLICTING) -> FRESH xreview (re-add xr-fnp-math; same
   brief guards) -> on PASS + CI green + gate 5x0 + comment clean -> queue #779.
   Then close #628 with pointer. Steps 8-10 = separate follow-up.
3. Agg slices (c),(b),(a) after (d) lands; close #625 with pointer once landed.
4. Unit 3: live-Spark AES measure first (jvm-lock, JAVA_HOME=zulu-17, one JVM) ->
   Rust WO (approved crates only) -> collate design note -> QUESTION -> build per
   ruling. Unit 3 owns the 94 strict xfails.
5. Unit 4 (#662) if time, same rescue order.
6. Watch: queue EMPTY at close; main 8de204f6. Both lanes CLERK-RESERVED until
   handbacks resolve — do not touch /tmp/fnp-agg or /tmp/fnp-math. Any new merge ->
   rebase owed before gate/push (REBASE BEFORE YOU GATE).
7. Open risks: (a) #779 stays CI-red until the remediation push; (b) agg has no PR
   yet — open it at the first 5x0, do not batch; (c) the rebase chase was the
   dominant cost — accept+push inside queue-drain windows where possible.

Evidence: /tmp/oc-worker/run27/ticks-xo-muse7/016..091/ (accepts, diffs, gates,
critic out.json, step-1 probes, WO copies with md5s); claims lines xo-muse7 t37-t91;
runs.tsv rows. The state file carries the full per-tick ledger.
