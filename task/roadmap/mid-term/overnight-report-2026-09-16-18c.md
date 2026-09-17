# Day report — run 18c, 2026-09-16 (09:00 → 21:15 EDT)

**Slice:** the SQL door, the planner contracts and the registry backlog (lane prefix `sc-`, build clones `/tmp/sc-build` and
`/tmp/sc-build2`). **Campaign:** 1.5 Spark parity. **Sibling runs:** 18a (functions), 18b (facade / dataframe). **Base at start:**
`33c87cbf` (#629 FNP-GEN-1 steps 1–2). **Actor tier:** Muse Spark 1.3 contributor for every unit (owner, morning of 2026-09-16).
**Reviewer tier:** Grok 4.6, read-only. **Owner rulings applied:** every run-17 recommendation, as ruled that morning.

## 1. What landed

| PR | Unit | Muse rounds | Merged | Review (Grok 4.6) | Grok cost |
|---|---|---|---|---|---|
| **#651** | Cards TZ-OFFSET-SECONDS-1 (1.6, Q-17c-1) + BL11-ENCODER-PERF-1 (Q-17c-5) — no code | orchestrator | `a92a68db` | — | — |
| **#654** | **BL-19 UNRESOLVED-ROUTINE-1** (Q-17c-3) | 2 | `50f03f64` | logic **4 P1** / 1 P2 / 1 P3 (refuted by measurement); Rust perf 0/0/2; Py perf 0/0/3; verification: all CLOSED, 1 P3 | $2.25 |
| **#656** | **BL-20 SQL-LITERAL-TYPING-1** (Q-17c-2) | 3 | `b8e79fca` | logic **1 P1** / 1 P2 / 1 P3 (→ P2 on measurement); Rust perf 0/2/3; Py perf 0/0/2; verification: all CLOSED + 1 new P2, fixed | $3.10 |
| **#643** | DOOR-CONVERGE-2b (inherited draft) | 1 (+ Devin's round 1 from 17c) | **parked, draft** | none yet | — |
| **#658** | JAVA-REGEX-FEATURES-1 (Q-16c-1) | 3 (2 resumed, one after an outage) | **draft, see §1c** | logic **5 P1** / 2 P2; Rust perf 0/3/4; Py perf 0/1/0; verification: all CLOSED, no new P1 | $1.86 |

Grok total for the run: **$7.21** over 12 rounds (plus one verification round aborted and relaunched after a rebase conflict, §6).
All three merges were squash merges through the shared queue, with the tree-equality check.

### 1a. What the two merged units changed

- **BL-19 (#654).** Every unknown function now refuses on both doors with Spark's
  `[UNRESOLVED_ROUTINE] Cannot resolve routine `<name>` on search path [...]. SQLSTATE: 42883; line L pos P`, instead of
  DataFusion's `Invalid function`. The doors are `spark.sql`, `F.expr`, `selectExpr`, string `filter`/`where` and
  `F.call_function`. It is one Rust mapper (`repark-core/src/unknown_routine.rs`), not a list of names.
  - **Spelling and position** come from sqlparser's tokens, so a name inside a string or comment never wins. Quoted multi-part
    names render one backtick pair per part, and `system.builtin.x` answers `REQUIRES_SINGLE_PART_NAMESPACE`.
  - **Oracle:** 88 cells.
  - **Residues:** BL-19-POS-FILTER and BL-19-POS-SELX (fragment positions; see §7), and BL-19-LATERAL-1.
- **BL-20 (#656).** One Rust analyzer rule, `SparkIntegralLiteral`, seated before DataFusion's `type_coercion`, types unsuffixed
  integral literals as Spark does:
  - INT when the value fits, BIGINT when it needs 64 bits;
  - DECIMAL(p,0) past that, with `DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION` past 38 digits.

  `1 + CAST(1 AS TINYINT)` is now int on `spark.sql`, `selectExpr` and `F.expr`, matching the Python door and Spark. The late
  duplicate narrowing pass and a higher-order-path fold were removed, so one rule remains. `-(2147483648)` stays bigint while
  `-2147483648` is int, as measured.
  - **Oracle:** 77 cells.
  - **Residues:** BL-20-OVF (narrow-integral overflow wraps where Spark raises) and BL-20-NATIVE (the native ANSI door keeps Int64).

### 1b. The units the clock caught

- **JAVA-REGEX-FEATURES-1** — `fancy-regex 0.11` is the fallback engine, used only for lookaround, backreferences, possessive
  quantifiers and atomic groups. It is the one dependency change in the slice.
  - **Critic round:** five P1s. A 10 000-byte "looping haystack" cap refused linear patterns Spark answers. A textual
    lookbehind rewrite was wrong for multi-character atoms, `{n,}`, `{n,m}` and concatenated bodies. A numbered backreference
    to a named group was refused.
  - **Measurement first:** the orchestrator measured every shape on Spark (`rx3-oracle.json`, 27 cells) before sending them
    back. Ruling R-18c-O8 replaced the rewrite with a semantic anchored-suffix check.
  - **Round 2:** a provider outage cut it (§5). It was resumed on the same session and landed the routing fix, the semantic
    lookbehind and the surrogate-count agreement.
  - **State at the stop time:** in §1c.
- **DOOR-CONVERGE-2b (#643).**
  - **Round 2:** re-measured every clause on a release native and reports C-001…C-007 PROVEN. It fixed four round-1 kernel
    defects the facade suite exposed, and merged its narrowing rule into BL-20's.
  - **Rebase:** the branch was stacked on BL-20's head, then rebased onto main after #656; `cargo check` is clean.
  - **Why parked:** the actor's own whole-facade run records **25 failures**, which it traces to two seams outside its fence:
    21 in ML `array_element` lowering and 4 in `fnp_gen_1` union nested-unification. `crates/repark-functions/src/expr_fn.rs`
    is 1003 lines after the rebase. Nobody has reviewed it yet. It stays a draft for run 19; the PR body lists what is owed.

### 1c. State of JAVA-REGEX-FEATURES-1 at 21:15

**#658, draft.** The branch was rebased onto main and pushed at 20:30 (head `4c36c6f6`, 12 commits, each with the Muse trailer).

**Verification critic (Grok 4.6):** every claimed fix is CLOSED: L-001…L-007 and PERF-001/002/003/004/007. It confirmed that
lookbehind is a semantic anchored-suffix check, accepted the actor's argument for not taking PERF-005, and found no new P1. The
oracle probe differs only on the three declared residue cells (RX3-SQL-20/21, RX2-SQL-12).

**Gates:**
- The actor reports verify 0, clippy 0, facade 9100 passed and parity 757 passed.
- The orchestrator's re-run was launched at 20:30: verify 0, clippy 0, parity 757 passed, facade 9361 passed and 2 failed. Both failures are `test_spark_sql_grammar_1.py::test_q14_current_date_bare_and_paren`, a date-boundary defect in a run-17c pin: after 20:00 EDT the session's UTC date is tomorrow, while the test compares local `date.today()`. It is unrelated to this unit and red on main in the same window (Q-18c-6).

**Not queued:** CI and the merge queue cannot finish inside run 18c's window. It is ready for the owner or run 19 to un-draft
and merge once CI is green.

## 2. Census slice — the divergence registry, before and after

Counted by the `Rationale` disposition over `docs/spark-sql-iceberg-parity.md` (the same script at launch and at the end; the whole
registry, all three runs):

| | rows | BACKLOG | FIXED | DECLARED |
|---|---|---|---|---|
| main at launch (`33c87cbf`) | 390 | 163 | 111 | 80 |
| main at 20:10 (`b8e79fca`) | 398 | 162 | 119 | 80 |

**This run's rows on main:**
- **Moved to FIXED:** BL-19 and BL-20.
- **Opened as dated residues** rather than left implicit: BL-19-POS-SELX, BL-19-POS-FILTER, BL-19-LATERAL-1, BL-20-OVF and
  BL-20-NATIVE.

**On the regex branch, not yet on main:**
- **Moves to FIXED:** JAVA-REGEX-FEATURES-1.
- **Opened as DECLARED:** JAVA-REGEX-BACKTRACK-1 (a runaway pattern refuses loudly; Spark has no class, its JVM dies with
  StackOverflowError), JAVA-REGEX-FEATURES-1-R1 (case-insensitive backreference) and R2 (Java's zero-width matches split a
  surrogate pair, which UTF-8 cannot carry).

The function-name census from `pyspark-surface.json` is run 18a's slice. This slice's contract rows change how *every* missing name
refuses (BL-19) and how every integral literal types (BL-20), rather than adding names.

## 3. Oracle batches recorded today (live PySpark 4.1.2, `/tmp/oc-worker/sc/oracle/`)

| Batch | Cells | Used by |
|---|---|---|
| `bl20-oracle.json` (LIT-*) | 64 | BL-20 |
| `lit2-oracle.json` (LIT2-*) | 13 | BL-20 remediation (the critic's conjectures) |
| `b2-oracle.json` (UR-*, UC-*, RX-*) | 65 | BL-19, ERR-UNRESOLVED-COL-1, regex |
| `ur3-oracle.json` (UR3-*) | 20 | BL-19 remediation |
| `b3-oracle.json` (RX2-*, UC2-*) | 63 | regex, ERR-UNRESOLVED-COL-1 |
| `rx3-oracle.json` (RX3-*) | 27 | regex remediation |
| `c13a-oracle.json` (C13A-*) | 39 | FNP-13a collation (carded, §8) |
| `seam-oracle.json` (SEAM-*) | 22 | SQL-DOOR-SEAMS-1 (carded, §8) |

The measurements changed several reviewer claims:
- **Refuted:** the claim that `pos` counts UTF-16 units. `'😀'` answers pos 12, which counts code points (L-006 on BL-19).
- **Raised:** a P3 on BL-20 became a P2 once measured: `-(2147483648)` is bigint in Spark.
- **Confirmed:** every regex P1, including that Java 17 accepts unbounded `(?<=a+b)`.
- **Ranking rule fitted:** the `UNRESOLVED_COLUMN` suggestion ranking was fitted by script on 20 of 20 unqualified cells. The
  candidates are backtick-quoted, sorted lexicographically, then stable-sorted by Levenshtein distance from the unquoted base
  name, and the top five are shown.

## 4. Per-PR reviewer verdicts (detail)

- **#654 BL-19:**
  - **Critic-logic, head `63f40f67`:**
    - L-001 took its spelling from the first textual match.
    - L-002 took its position from a decoy inside a string or comment.
    - L-003 split quoted multi-part names on DataFusion's Display string.
    - L-004 had a retired pin asserting UNRESOLVED_ROUTINE for `schema_of_csv`, a name Spark has.
    - L-005 computed string-filter positions on the quoted rewrite.
  - **Remediation:** the token-based matcher closed L-001 through L-004. L-005 was accepted as the residue rows under R-18c-O1.
  - **Verification critic, head `05c94c2f`:** everything claimed is CLOSED. One P3 remains (V-001: the error is formatted twice).
- **#656 BL-20:**
  - **Critic-logic, head `996094c8`:**
    - L-001: `F.expr` built its own analyzer context without the rule.
    - L-002: `date_add`/`date_sub`/`factorial` refused an unsuffixed literal.
    - L-003: `-(2147483648)`.
    - Its attack on re-analysis narrowing an explicit BIGINT came back clean.
  - **Rust perf:** PERF-001 (a full walk with no pre-check) and PERF-002 (`Values` cloned every cell).
  - **Verification critic, head `a51dc153`:** all CLOSED. It also surveyed the `Exact(Int32)` family and ten late-rule paths.
    One new P2 (V-001, the fold in the higher-order preparation path) was fixed in round 3.
- **Regex branch, head `9cc09218`:** see §1b. PYPERF-001 found that `F.split` raises in Python (`functions_expr.py`, run 18a's
  file); it is a hand-off.

## 5. Muse discipline

| | BL-19 | BL-20 | Regex | #643 |
|---|---|---|---|---|
| rounds (resumed) | 2 (1) | 3 (2) | 3 (2, one after an outage) | 1 |
| steps | 276 + 128 | 427 + 317 + 40 | 366 + 99 + resumed | 461 |
| gate claims re-run by the orchestrator | 2 | 2 | 1 (verify, clippy) | 0 (parked) |
| gate claims overturned by the re-run | 0 | 0 | 0 | — |
| comment-ban rejections | 0 | 0 | 0 | 0 |
| attribution defects | 0 | **8 commits** | 0 | 0 |
| ledger rows written ahead of the work | 0 | 0 | **4** | 0 |

The findings in that table:
- **Attribution.** BL-20's two resumed rounds wrote a co-author trailer, in the form the owner's pre-push hook forbids, instead
  of `Authored-By`, on eight commits. The first rounds of the same session did not. The orchestrator rewrote those messages to
  the required trailer (`filter-branch --msg-filter`, tree unchanged) before the push.
- **Ledger ahead of work.** After the outage, the regex round's ledger claimed four fixes "in the step-3 commit" that did not
  exist. The resume brief named that finding, and the rows were corrected as the fixes landed.
- **Gate claims and comments.** No gate claim was overstated today, and no round added a comment. The two Muse faults of runs 16
  and 17 did not recur.
- **Outage.** One provider outage (`transport_stream_error: body-truncated`, 12-minute ceiling) cut regex round 2 after two
  commits. The session was resumed, never restarted, and lost no work.

## 6. Mechanics worth carrying forward

- **A rebase conflict, then a launch on a half-rebased tree.** On BL-19, the orchestrator rebased, the rebase stopped on a
  conflict in `repark-core/src/lib.rs` (from #653), and the gates and verification critic were launched before the exit status
  was read. Both were stopped, the conflict was resolved (keep both re-exports), and both relaunched. From then on every rebase
  was followed by `test -d .git/rebase-merge` before anything else.
- **Load-dependent wall-clock tests failed on this run's gates**, each passing alone:
  - `repark-iceberg` `listing_cost_list_tables_cheaper_than_provider_rebuild` (6.5 ms vs 1.7 ms), twice in `make verify`;
  - `test_h3_spill_matrix.py::…[window_unbounded-64M]` once in the facade suite.

  Both were accepted as load flakes (R-18c-O6, R-18c-O7) on the evidence that they pass alone and touch nothing the unit
  changed. See Q-18c-3.
- **A stacked branch.** #643 was based on BL-20's head while BL-20 waited in the queue, then moved with
  `rebase --onto origin/main <bl20-head>` after the squash merge. Only map files conflicted.
- **The two-build-clone cap.** With about 1.1 T free after midday, a fully carded and oracled unit (SQL-DOOR-SEAMS-1) waited
  three hours for a lane and was not opened. See Q-18c-4.
- **Merge queue.** It worked. #656 sat behind 18a's #657 for about 20 minutes, then needed an update-branch and a CI re-run.

## 7. Rust-first roll-call (Q-17a-2) — logic still in Python, with reasons

- **`functions_byname.py` (run 18a) and `catalog_surface.py` (run 18b)** still build an `UNRESOLVED_ROUTINE` message in Python for
  `F.call_function` / `catalog.getFunction`. The Rust mapper now makes the same decision, so these are duplicates that can drift.
  They are hand-offs: those files belong to other runs today.
- **String `filter`/`where` and `selectExpr` fragment positions (BL-19-POS-FILTER, BL-19-POS-SELX).** `dataframe/core.py`
  (run 18b) quotes identifiers before handing the predicate to Rust, so Rust never sees the user's original text. The class and
  message are Spark's; only `pos` differs. The fix is to pass the original fragment into the Rust call, which is a one-argument
  change in an 18b file.
- **`udtf.py`** parses unsuffixed integer tokens to Python `int` before `F.lit`, so a 19-digit token refuses instead of typing
  `decimal(19,0)`. **`functions.py`** still forces `CAST(… AS INT)` on foldable day counts, a leftover from Int64 literals. Both are
  run 18a's files: hand-offs.
- **`functions_expr.py` `F.split`** raises in Python, so the facade cannot reach the lookaround `split` kernel that the SQL door now
  answers. It is run 18a's file: a hand-off.
- **None of today's product code in this slice is Python.**

## 8. Cards filed for run 19 (on disk, oracles recorded)

- **SQL-DOOR-SEAMS-1** — `/tmp/oc-worker/sc/cards/card-seams.md`. It covers generator multi-alias `AS (x, y)` with
  `UDTF_ALIAS_NUMBER_MISMATCH` / `MULTI_ALIAS_WITHOUT_GENERATOR`, and struct dot access on a call result with `FIELD_NOT_FOUND`.
  It flips run 18a's four strict-xfail pins "owned by run 18c". Oracle: `seam-oracle.json`.
- **ERR-UNRESOLVED-COL-1** — `/tmp/oc-worker/sc/cards/card-uc.md`. It gives both doors Spark's ranked suggestion list (fitted
  rule above) and retires 17a's seven DataFusion `Schema error` pins. Oracle: 17a's 10 cells plus 18c's 55 (`UC-*`, `UC2-*`).
- **FNP-13a collation** — `/tmp/oc-worker/sc/cards/card-c13a.md`. It covers the carrier, UTF8_LCASE, RTRIM and precedence on the
  SQL door, with no new dependency. Oracle: batch 18 plus `c13a-oracle.json` (39 cells, including `COLLATION_INVALID_NAME`,
  `INDETERMINATE_COLLATION_IN_EXPRESSION`, representative-value grouping). 13b (string functions) follows it.
- **DOOR-CONVERGE-2b (#643)**, parked draft; §1b lists what is owed.

## 9. Rulings taken under G-2 (each in its unit ledger or here)

- **R-18c-O1:** BL-19's string `filter`/`selectExpr` positions ship as dated residue rows. The original text lives in an 18b file,
  and the verification critic judged the hand-off honest.
- **R-18c-O2:** JAVA-REGEX-FEATURES-1 opened before #643, because #643 had to sit on BL-20, which was still in remediation.
- **R-18c-O3:** a runaway regex refuses loudly with a named execution error and a DATED DECLARED row. Spark has no error class
  (measured: `java.lang.StackOverflowError`).
- **R-18c-O4:** BL-20 excludes the DataFrame schema-string widths (LOGICAL-WIDTH-1, run 18b, merged as `778fa9b1`) and the `div` /
  unary `~` grammar residues.
- **R-18c-O5:** the `-(2147483648)` finding was raised from P3 to P2 on measurement (bigint in Spark).
- **R-18c-O6 and R-18c-O7:** two load-dependent timing tests were accepted as flakes on pass-alone evidence (§6).
- **R-18c-O8:** regex lookbehind uses a semantic anchored-suffix check, not a textual rewrite of the quantifier.
- **R-18c-O9:** SQL-DOOR-SEAMS-1 was carded ahead of ERR-UNRESOLVED-COL-1, being smaller and unblocking a sibling run's pins.
- **R-18c-O10:**
  - #643 was parked as a draft: 25 facade reds outside its fence and a 1003-line file after the rebase.
  - SQL-DOOR-SEAMS-1 was not opened at 20:05. A round opened then could not finish by 21:15 and would have been left running
    with no orchestrator.

## 10. Owner questions, with recommendations

1. **Q-18c-1 — Muse's resumed rounds wrote a forbidden co-author trailer.** It happened on eight BL-20 commits, and the pre-push
   hook would have refused the push. *Recommendation:* the lane setup (`r8-lane.sh`) installs a `commit-msg` hook that rejects
   that form at commit time, so the worker sees the refusal inside its own turn. It should also add the trailer when it is
   missing. This is cheaper than an orchestrator rewrite after every resumed round.
2. **Q-18c-2 — #643's 25 facade reds sit in ML `array_element` lowering and a generator union path.** *Recommendation:* in
   run 19, open a small unit that fixes the ML `array_element` lowering (21 of the 25) first, then gate and review #643 on top of
   it. Do not loosen the 25 pins to fit.
3. **Q-18c-3 — wall-clock ratio assertions flake when three orchestrators share the box.** This covers `repark-iceberg`
   `listing_cost_…` and the spill-matrix pool refusal. *Recommendation:* move wall-clock ratio assertions out of `make verify`
   into the perf lane that runs alone, or give them a retry-once harness. Owner decides which: both keep the signal, and a
   red `verify` that an orchestrator has to argue away teaches the wrong habit.
4. **Q-18c-4 — the two-build-clone cap with more than 1 T free.** A carded, oracled unit waited three hours for a lane.
   *Recommendation:* allow a third build clone per orchestrator while `df -h /` shows more than 600 G free, with the same
   per-lane target rule.
5. **Q-18c-5 — the regex declarations.** A runaway pattern refuses with repark's own named error (Spark's JVM dies with
   StackOverflowError), and Java's lone-surrogate zero-width results cannot be represented in UTF-8. *Recommendation:* accept
   both as dated declarations. Neither is a shape a real query depends on.

6. **Q-18c-6 — `test_spark_sql_grammar_1.py::test_q14_current_date_bare_and_paren` fails after 20:00 EDT.** It compares
   `current_date` (the session zone, UTC by default) with Python's local `date.today()`, so every evening gate on this box is red
   for four hours. *Recommendation:* a two-line test fix, comparing against the session zone's date. Run 18c recommends it rather
   than taking it, because the file is 17c's merged pin, and it goes into run 19's first docs/test PR.

## Pointers

- Up: [map.md](map.md) · Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md)
- Previous slice report: [overnight-report-2026-09-16-17c.md](overnight-report-2026-09-16-17c.md)
