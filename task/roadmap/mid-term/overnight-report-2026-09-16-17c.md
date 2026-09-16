# Overnight report — run 17c, the night of 2026-09-15 → 16

**Slice:** the SQL door, the config carriers and the registry backlog (lane prefix `rc-`, build clones
`/tmp/pc-cv2` and `/tmp/qc-conv2`). **Campaign:** 1.5 Spark parity. **Sibling runs:** 17a (functions),
17b (facade / dataframe). **Base at start:** `0355ef5e` (FNP-4B #611); v1.4.2 had been tagged that evening
and is an ancestor — nothing tonight changed that tag.

## 1. What landed

| PR | Unit | Actor tier | Rounds | Merged | Reviewers |
|---|---|---|---|---|---|
| **#633** | JAVA-DOUBLE-FD-1 (inherited from run 16c) | **Devin SWE-2** | 1 | `bae1d587` | critic-logic: 0 P1 / 0 P2 / 2 P3 |
| **#639** | SET-ANSI-RUNTIME-1 (Q-15c-3) | **Muse contributor** | 4 | `6f74897f` | logic + 2× S2-21 perf: **1 shared P1**; 2 verification passes |
| **#641** | BL-11 numeric → BINARY under ANSI off | **Muse contributor** | 2 | `0ef060af` | logic: 0 P1 / 2 P2; Rust perf: 0 P1 / 1 P2 |
| **#642** | registry rows BL-19 / BL-20 (docs) | orchestrator | 1 | `(merged)` | — |
| **#644** | SPARK-SQL-GRAMMAR-1 | **Muse contributor** | 1 | **merged** | — (cut off by the stop time) |
| **#643** | DOOR-CONVERGE-2b | **Devin SWE-2** | 1 | **parked, draft** | — (never gated) |

### 1a. The two units the clock caught

**SPARK-SQL-GRAMMAR-1 (#644)** — 5 of 10 clauses PROVEN in a single round: C-003 (`RLIKE` / `NOT RLIKE`
onto `regexp_like`), C-004 (`TIMESTAMP_LTZ` → `TIMESTAMP`, `TRY_CAST` included), C-005 (the `TIMESTAMP_NTZ`
seam now refuses with Spark's `[UNSUPPORTED_TIMESTAMP_NTZ]`), C-006 (struct dot-access, already green),
C-008 (bare datetime-unit keywords onto #606's kernels) and C-010 (bare nullary keywords following Spark
**in both directions** — the ones Spark resolves resolve, the six it refuses refuse). Five stay OPEN with
the seam named in each: C-001/C-002 need kernels the fence does not cover (`>>>`, `div`), C-007 is the
lateral-column-alias planner seam, C-009 waits on FNP-GEN-1 (#629, still not on main, ruling R-17c-7).
The round was stopped at the cutoff; the orchestrator committed its last two files (below) and gated.

**DOOR-CONVERGE-2b (#643)** — **parked as a draft, never gated.** Five hours on one Devin round produced
seven new Rust sources, the pins file and the ledger, and no commit; the orchestrator committed the tree
with Devin's attribution, stripped eight comment-ban violations from the pins file, pushed it and opened
a draft so run 18 inherits it. Every clause should be treated as OPEN until re-measured. Snapshots of the
pre-commit tree are under `/tmp/oc-worker/qc-conv2/snapshots/`.

## 2. Census slice — the divergence registry, before and after

Counted by disposition over `docs/spark-sql-iceberg-parity.md`:

| | rows | BACKLOG | FIXED | DECLARED |
|---|---|---|---|---|
| before run 17c | 335 | 158 | 88 | 57 |
| after | 339 | **154** | **94** | **58** (+1 dated) |

Six rows moved BACKLOG → FIXED (TZ-3, SET-ANSI-RUNTIME-1, BL-11 and the rows they discharge); four new
rows were opened rather than left implicit — three narrow residues and one dated declared divergence.
**Net: four fewer backlog rows and four more honest ones.**

## 3. The finding of the night: three reviewers agreed, and they were all still short

On #639, the critic-logic pass and both S2-21 perf reads independently landed on the same P1 — a runtime
`SET` of a Java `ZoneId` spelling (`+5`, `GMT+8`, `Z`) **succeeded**, and then every value-bearing query
and `createDataFrame` raised, because the new gate stored the raw text while the consumers parse with
Arrow `Tz` / Python `ZoneInfo`. That was fixed under ruling R-17c-4.

All three reports also carried the same note: **Spark's own acceptance set was UNMEASURED** — no JVM had
been started for it. So the orchestrator measured it, on live PySpark 4.1.2
(`fixtures-batch17-zone-spellings.json`, 15 cells). **Five of fifteen cells diverged, in both directions:**

| | Spark | repark after the "fix" |
|---|---|---|
| `+05:30:30`, `+18:00:00` | accepts | **refused** |
| `gmt+8`, `z` | refuses (`ZoneId` matching is case-sensitive) | **accepted** |
| `"  Asia/Tokyo  "` | refuses (whitespace is not trimmed) | **accepted, trimmed** |

Ruling **R-17c-6** corrected **my own earlier R-17c-4**, which had said "a value that validates but cannot
be canonicalised is refused at the SET". I had written that without the oracle, and it was wrong in
exactly the direction that makes a parity engine quietly narrower than Spark. Four of the five closed;
the fifth ships as a dated declared divergence (below).

**The lesson, for the runbook:** three independent expert reviewers agreeing is not a measurement. When a
report says UNMEASURED, that is the next step, not a footnote — and an orchestrator ruling written ahead
of the oracle is exactly as unreliable as a worker's guess.

## 4. Declared refusals and residues opened tonight (all dated, all with pins)

- **SET-ANSI-RUNTIME-4** — sub-minute fixed offsets (`+05:30:30`): **Spark accepts, repark refuses.**
  Proof recorded in the ledger: Arrow's `Tz` has no seconds-offset representation, and the three parse
  sites are outside the unit's fence. `+18:00:00` *is* implemented, so the line is drawn at sub-minute,
  not at seconds-syntax. Audited by the verification critic.
- **SET-ANSI-RUNTIME-2** — `current_timezone()` on a stale frame answers the build zone.
- **SET-ANSI-RUNTIME-3** — a runtime ANSI-off does not reach `CAST('x' AS INT)`. The first draft of this
  row under-drew the gap; a critic measured the true breadth and the row was rewritten, then a later pass
  found `CAST('x' AS FLOAT)` missing from it too.

## 5. Two divergences found but not caused, filed for the registry

1. **`INT + TINYINT` promotes to BIGINT on the SQL door; Spark says INT.** Measured: Spark's
   `typeof(1 + CAST(1 AS TINYINT))` is `int` and the cast is 4 bytes on both its doors; repark's SQL door
   gives 8 bytes while its **Python door gives 4 — the two repark doors disagree.** Pre-existing in the
   SQL door's integral literal typing; BL-11 merely made it visible, being the first cast whose result
   *width* is decided by the static integral type.
2. **`typeof` is missing on the SQL door and the unknown-function refusal is the wrong shape:**
   `SELECT typeof(1)` gives `Error during planning: Invalid function 'typeof'.` where Spark raises
   `UNRESOLVED_ROUTINE`. The blanket refusal contract is the more important half — every unknown function
   on the SQL door currently refuses in the wrong shape.

## 6. Devin SWE-2 versus Muse contributor — the comparison the owner asked for

| | Devin SWE-2 | Muse Spark 1.3 contributor |
|---|---|---|
| units | 2 (FD-1 fix round; DOOR-CONVERGE-2b) | 2 (SET-ANSI-RUNTIME-1, BL-11) |
| rounds | 1 completed + 1 long-running | 6 |
| rounds lost to rate limits | **0** (last night lost two) | 0 |
| a completed round | FD-1 fix: 18 turns, 17 edits, ~32 min, 1.92 M tokens | SET-ANSI r1: 245 steps, ~75 min |
| diff quality on hand-back | clean: fence held, trailer byte-exact, **no comment-ban violation** | round 1 added **33 Rust `///` doc comments** and had to be sent back |
| ledger honesty | accurate | BL-11 claimed `make verify` rc 0 when it was **rc 2** |
| hand-back JSON | written every time | **never written**, in any of 6 rounds |
| commit cadence | batches: 3h+ with zero commits on the big card, despite an explicit "commit as you go" | commits per clause group, as briefed |

**Read:** Devin is the more careful *reader* — its FD-1 round diagnosed a subtle analyzer-ordering bug
(`__repark_suffix_literal__` beating `FoldSparkNumericCasts`, with the schema pinned at analysis) faster
and more accurately than the brief's own hypothesis, and its one diff needed no correction. But it batches
work: on DOOR-CONVERGE-2b it ran 3.5 h and produced five new source files with **no commit at all**, which
is an outage away from losing everything (the orchestrator snapshotted it manually). Muse commits properly
and moves fast, but needed a correction round on three of four units — the comment ban, an overstated
gate line, and an under-drawn residue row. **Recommendation:** Devin for the diagnosis-heavy cards with an
explicit "commit within the first 30 minutes" instruction the launcher enforces; Muse for the broad ones,
with the comment-ban grep and a gate re-run treated as mandatory audit steps, never as trust.

Reviewer tier (Grok 4.6, read-only): 8 rounds, **$4.16 total**, 169 turns. Value delivered: one P1 that
would have shipped a session-breaking bug, one under-drawn residue row, one drift risk closed, and two
clean "no P1" confirmations. Never on Opus, per the standing ruling.

## 7. Rust-first roll-call — Python-only logic left behind, with reasons

- **`session_time_zone.py`'s `tzinfo` bridge** — `collect()` and naive `createDataFrame` must hand Python
  a `datetime.tzinfo`. Inherently Python; the value and the validation are Rust. Confirmed by the
  verification critic: "no Python-only SET gate".
- **`builder_conf.py` / `sql_set_statements.py`** — storage and forwarding only; both files are
  net-negative in #639 because their validators moved into Rust.
- **Two lines in `session_configuration.py`** — a `_SQLCONF_DEFAULTS` entry, the one coordination point,
  announced in the ledger.
- **BL-11: none.** The unit changed no Python source at all.
- **The duplicated canonicaliser** (`repark-core` and `repark-functions`) is Rust-on-both-sides but is
  duplicated because `repark-functions` cannot depend on `repark-core`. Now pinned by one shared table
  asserted from both crates. The right long-term home is a shared leaf crate — see the questions below.

## 8. Rulings taken under G-2 (all recorded in the unit ledgers with their numbers)

- **R-17c-1** — SET-ANSI-RUNTIME-1 keeps the lane name `pc-cv2`; no clone rename (the release native and
  the parked work lived there).
- **R-17c-2** — the FD-1 rebase regression ships inside #633 rather than as a follow-up unit, because
  #633 is the commit that introduces it.
- **R-17c-3** — the docstring-presence gate is **Python-only** (`SCAN_ROOTS`), so no Rust `///` is ever
  gate-required; the comment ban applies to Rust doc comments without exception, and their content lives
  in `map.md`.
- **R-17c-4** — the runtime zone snapshot carries the Spark-visible text plus a canonical companion;
  canonicalisation happens once, in Rust, at the gate. *(Superseded in part by R-17c-6.)*
- **R-17c-5** — DOOR-CONVERGE-2b runs on Devin in the repurposed `/tmp/qc-conv2`; every measurement comes
  from a release native built from that branch.
- **R-17c-6** — **Spark's acceptance set is the specification in both directions.** repark may not refuse
  what Spark accepts, nor accept what Spark refuses. Case-sensitive `ZoneId` matching, no whitespace
  trimming, seconds-precision offsets accepted. Corrects R-17c-4.
- **R-17c-7** — SPARK-SQL-GRAMMAR-1's C-009 (LATERAL VIEW) stays OPEN for the run: its prerequisite,
  17a's generator kernels (FNP-GEN-1, #629), never reached main.
- **R-17c-8** — two edits to run 17a's test files ship inside #644 rather than as a hand-off: dropping
  `test_fnp11a_r2.py`'s `BARE_UNIT` translation regex (C-008 makes the bare units run natively, so the
  rewrite would hide the behaviour under test) and flipping `test_fnp11b_typeof.py` to Spark's
  `[UNSUPPORTED_TIMESTAMP_NTZ]` class (C-005 now carries it). Both are *forced* by this unit's behaviour
  change and in-kind with the skip-set removal the card already sanctions; leaving either would put main
  red. Announced to 17a in the ledger and the PR.

## 9. Owner questions, with recommendations

1. **Q-17c-1 — sub-minute fixed offsets.** `+05:30:30` is a dated declared divergence because Arrow's
   `Tz` cannot express it. *Recommendation:* accept the declaration for 1.5 and open a 1.6 card to carry
   fixed offsets as **seconds east of UTC** in the carrier rather than as a `Tz` string, which also
   removes the Java/Arrow grammar mismatch at its root. Low urgency — it is a spelling almost nobody uses.
2. **Q-17c-2 — the SQL door's integral literal typing** (§5.1) makes the two repark doors disagree on a
   measured Spark answer. *Recommendation:* a 1.5 card, and a high one: a door disagreement is the exact
   failure the shape rule exists to prevent, and literal typing is upstream of a great many casts.
3. **Q-17c-3 — the unknown-function refusal shape** (§5.2). *Recommendation:* a 1.5 card, ahead of adding
   any more individual function names: `UNRESOLVED_ROUTINE` is a blanket contract, so one fix corrects
   every missing name at once instead of once per name.
4. **Q-17c-4 — a shared leaf crate for code both `repark-core` and `repark-functions` need.** The zone
   canonicaliser is the second thing to be duplicated across that boundary. *Recommendation:* defer until
   a third case appears; the shared-table pin is adequate for two.
5. **Q-17c-5 — the BL-11 encoder's ~1.5× per-row cost** (Rust-perf P2: 26.81 ms vs 18.35 ms over 1e6
   cached rows). *Recommendation:* a follow-up perf card, not a 1.5 blocker — the absolute cost is about
   one millisecond per million rows per cast.
6. **Q-17c-7 — a Rust panic reaches the Python boundary under memory pressure.** On the CI job for a
   **docs-only** PR, `test_h3_spill_nlj_1_a_tight_pool_refuses_a_nested_loop_join_with_the_typed_exception`
   failed with `inner future panicked during poll (a rust panic was caught at the python boundary; this is
   a bug — please report it)` instead of the Never-OOM typed refusal. It passed locally in `make preflight`
   on three branches tonight and failed once on a loaded runner, so it is pressure-dependent — which is
   when the contract matters most. *Recommendation:* a real card, not a re-run. I re-ran the job to unblock
   a docs merge, but the panic is a defect in the guard the Never-OOM work exists to provide.
7. **Q-17c-6 — `r7-launch.sh` waits for `pgrep -x cargo` to be empty across the whole box.** With three
   orchestrators that state is rare; it silently stalled two of my launches (one for ~35 minutes) with no
   output. *Recommendation:* make the wait count *this orchestrator's* builds against the two-build cap
   rather than requiring a globally idle box, or give it a timeout that logs and proceeds.

## 10. Mechanics worth carrying forward

- **The shared merge queue is read with a literal `head -1`.** `qc-merge.sh` and `qc-requeue.sh` both do
  `head -1 … | cut -d' ' -f1`, so a **leading comment line stalls every merge in the queue** — it held
  #633 for 13 minutes with no diagnostic. Entries now go above the notes; the file says so.
- **`pkill -f '<pattern>'` kills the orchestrator's own shell** (the runbook warns; it still cost two
  turns). Use the bracket form or kill by PID.
- Branch protection is `strict: true`, so every time main moves a PR must re-merge and re-run ~25 minutes
  of CI. With three runs merging all night this is the single biggest source of wall-clock loss.
- A build clone can be **repurposed** between units (`git checkout -B <new> origin/main`) — it keeps the
  warm `target/` and the `.venv`, which is far cheaper than a new clone, but the installed native is then
  from the wrong tree and the next round's brief must say "rebuild first".

