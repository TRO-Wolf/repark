# Overnight report — run 15c (2026-09-15): the SQL door and the registry BACKLOG

**Unit** overnight-15c · **Orchestrator** Claude (claude-opus-5) · **Window** 2026-09-14 20:45 → 2026-09-15 07:15 EDT · **Lane prefix** `pc-` · **Charter** the 1.5 Spark-parity campaign (PR #591), run 15c slice: FNP-4b expression strings, BACKLOG rows BL-6/7/11/16/17/18 + LOG-1 + B-TZ-5, the SQL-door divergences, FNP-13/14, FNP-6d, the SQL-door halves of 15a's temporal names.

This report closes when its PR merges. Every measured claim below is dated 2026-09-15 unless stated.

## 1. Census slice — before / after

Two units merged tonight (#601, #609). Three are left as PRs for the owner (DOOR-CONVERGE-1, #612 draft, #611 draft). Runs 15a and 15b each held a four-to-six deep merge queue; every PR landed in order. The states below are read at the report's commit.

| Item (run 15c slice) | Before (main acbb6a8e) | After (main at report time) | Unit / PR |
|---|---|---|---|
| B-TZ-5 `SET` / `RESET` / `SET TIME ZONE` through `spark.sql` | BACKLOG, every shape refused | **FIXED** (residues TZ-3, SET-ANSI-RUNTIME-1, SET-TZ-LOCAL-1 recorded) | SQL-SET-DOOR-1 #601 merged 462c1eaf |
| `current_timezone()` on the SQL door | "Invalid function" | **FIXED** | #601 |
| FNP-6d `bitmap_construct_agg` / `bitmap_or_agg` / `bitmap_and_agg` (SQL door) | absent | **FIXED** (SQL door; facade names by run 15a) | FNP-6D #609 merged |
| BL-16 `hypot` rescale + SQL-door registration | BACKLOG | FIXED on the DOOR-CONVERGE-1 branch (PR open, not merged at 07:15) | DOOR-CONVERGE-1 |
| BL-17 `base64` padding + MIME chunking, `unbase64` BINARY + Java decoder errors | BACKLOG | FIXED on the DOOR-CONVERGE-1 branch (PR open, not merged at 07:15) | DOOR-CONVERGE-1 |
| BL-18 `approx_count_distinct` / `regr_count` non-null | BACKLOG | FIXED on the DOOR-CONVERGE-1 branch (PR open, not merged at 07:15) | DOOR-CONVERGE-1 |
| BL-6 `bin` / `rint` BOOLEAN refusal | BACKLOG | SQL door FIXED on the DOOR-CONVERGE-1 branch (PR open); facade half is a P2 for run 15a | DOOR-CONVERGE-1 |
| `EXPECTED_DIVERGENCES` (facade vs SQL door kernel identity) | 22 names | 22 → 14 on the DOOR-CONVERGE-1 branch (PR open; lands in 15b's queue after 15a's #606) | DOOR-CONVERGE-1 |
| BL-7 DOUBLE / FLOAT stringify as Java | BACKLOG | FIXED on draft #612 (round 2, worker-gated); JDK-17 non-shortest cells BACKLOG JAVA-DOUBLE-FD-1 | JAVA-DOUBLE-STR-1 |
| FNP-4b Spark-door dialect: BL-2 backticks, BL-9 double-quoted strings, BL-10 escapedStringLiterals (builder), BL-12 `\U` artifact, numeric suffixes, exponent literals DOUBLE, F.expr column binding | deferred since 2026-08-20 | draft #611 (head e41e4f98, gated); critic fixes in progress on `wip/fnp-4b-round4` (33 facade failures) | FNP-4B |
| LOG-1 | already FIXED 2026-08-31 (SEM-1) — truthed, no change | unchanged | — |
| BL-11 numeric → BINARY under ANSI off | BACKLOG | unchanged — needs a runtime ANSI carrier (SET-ANSI-RUNTIME-1) first; oracle cells BL11-* recorded | — |
| FNP-14 `mask`, `aes_*` | absent | not started — card written (`/tmp/oc-worker/pc-cards/card-fnp14.md`), oracle cells F14-* recorded; dependency ruling R-15c-6 | — |
| FNP-13 collation | DECLARED (G15) | not started — card not written; design note required first (FNP-13 row) | — |
| SPARK-SQL-GRAMMAR-1 (TIMESTAMP_NTZ/LTZ type names, `>>>`, `div`, RLIKE keyword, chained struct dots, lateral column alias, bare-unit `timestampadd` family, LATERAL VIEW) | divergent | not started — card written, oracle cells PG-* + 15a's LATERAL VIEW cells | — |
| DOOR-CONVERGE-2 (decimal-scale round/ceil/floor, `date_part` seconds, array literal widths, element nullability, promise_retag, `concat`/`reverse` on arrays, `sequence`/`split` on the SQL door) | divergent | not started — card written, oracle batches 2/3/7 | — |

## 2. Per-PR table

| PR | Unit | State at 07:15 | Actor rounds (worker, steps / turns, cost) | Reviews (verdict, cost) | Orchestrator gates |
|---|---|---|---|---|---|
| #601 | SQL-SET-DOOR-1 (B-TZ-5, `current_timezone()`) | **merged 462c1eaf**, TREE-EQUAL, 02:31 | Devin 40 + 53 steps (free); Devin round 3 rate-limited (0); Grok round 3, 50 turns, $1.48 | Critic-logic NEEDS_REMEDIATION ($1.11; 2 of its claims overturned by oracle batch 5); S2-21 perf P1 full-query scan ($0.67) | make verify rc 0, facade 6639, parity 757, dbt 30 |
| #609 | FNP-6D (`bitmap_construct_agg` / `bitmap_or_agg` / `bitmap_and_agg`) | **merged**, tree-equal, 05:06 | Grok 94 turns $6.91 + 96 turns $6.84 | Critic-logic NEEDS_REMEDIATION ($0.51); S2-21 perf P1 no GroupsAccumulator ($0.60) | make verify rc 0, facade green, parity 757, dbt 30 |
| `feat/door-kernel-converge-1` (PR opening at report time) | DOOR-CONVERGE-1 (BL-16/17/18, BL-6 SQL door, 22 → 14 divergences) | **open**, orchestrator-gated on a private target; queue slot after 15a's #606 (owner merges after one rebase) | Devin 61 + 166 + 94 + 129 steps + round 5 (free); one round rate-limited (0) | Critic-logic NEEDS_REMEDIATION, 5 P1 confirmed by batch 6 ($0.78); S2-21 perf 2 P1 ($0.86) | private-target make verify rc 0; facade / parity in the PR body |
| #612 | JAVA-DOUBLE-STR-1 (BL-7) | **draft**, head = round 1 rebased and gated; round 2 committed 41fb4a79 and pushed (worker-gated: make verify rc 0, facade 6799, parity 757) | Muse 171 steps (round 1); round 2 35 steps lost to the 04:17 network failure, resumed | Critic-logic NEEDS_REMEDIATION, all confirmed by batch 10 ($0.61); S2-21 perf no P1 ($0.54) | make verify rc 0, facade 6791, parity 757, dbt 30 (round 1) |
| #611 | FNP-4B (Spark-door dialect, BL-2/9/10/12, typed literals, exponent DOUBLE) | **draft**; head e41e4f98 unchanged; round 4 salvaged to `wip/fnp-4b-round4` f44021f6 (make verify rc 0, parity 757, facade 33 failed: 14 test_explode_rewrite, 4 test_dml_b_partition_overwrite, 3 test_fnp_4b_literals, …) | Muse 898 + 374 steps; orchestrator remediation e41e4f98; Grok round 4, 300 turns (max), $24.85, uncommitted, then salvaged by the orchestrator | Critic-logic NEEDS_REMEDIATION ($1.37; 2 of its claims reversed by batch 9); S2-21 perf no P1 ($0.85) | e41e4f98: make verify rc 0, facade 6178 / 0 failed, parity 757 |

**Spend:** Grok $47.98 over 14 runs (5 actor rounds, 9 reviews). Devin free (8 rounds, 2 lost to its free-tier rate limit). Muse 4 rounds (1 lost to the 04:17 box-wide network failure). Oracle: 454 live PySpark 4.1.2 cells in 12 fixture files.

## 3. Oracle record

Ten live PySpark 4.1.2 batches, recorded by the orchestrator before any implementation or to adjudicate a reviewer claim, kept at `/tmp/oc-worker/pc-oracle/fixtures-batch{1..10}.json` (411 cells). Two reviewer claims were overturned by a live cell (SET keys are case-sensitive on Spark; `1e3L` and `0x1D` are identifiers on Spark, not numbers) and one of my own fixture labels was wrong (`FNP4B-where-escape`).

## 4. Rulings taken under G-2

| ID | Ruling | Where recorded |
|---|---|---|
| R-15c-1 | Extra Rust-capable lanes are `git clone --shared` copies of the build clone with `CARGO_TARGET_DIR` on the build clone's target: one target dir, cargo's lock serialises builds, about 4 G per lane. A git worktree was tried first and refused by the Devin launcher. | this report |
| R-15c-2 | FNP-4B decisions D-1..D-8: Databricks dialect for every Spark-door statement; backtick quoting in Rust `quote_ident_spark` and Python `_idents.quote_ident`; `F.expr` and filter strings on the same lexer; numeric suffixes; the `escapedStringLiterals` builder carrier; the `\U` artifact. Follow-ups A-1 (BL-2 hunk in `dataframe/core.py`, granted by run 15b), A-2 (exponent literals are DOUBLE), A-3 (CAP-1 mirror ratchet). | fnp-4b ledger |
| R-15c-3 | SQL-SET-DOOR-1 lives in `session/sql_set_statements.py` under run 15b's grant; the `session_core.py` hook is a net-zero 14-line block because the file sits on an exact 2304-line baseline. | sql-set-door-1 ledger R-1 |
| R-15c-4 | A reviewer claim without a fixture cell is re-measured on live Spark before it reaches an actor. Batch 5 overturned two SET-door claims; batch 9 overturned two FNP-4B claims. | ledgers |
| R-15c-5 | The Devin free tier rate-limits two concurrent lanes: one Devin lane at a time from 23:18, and a rate-limited round falls back to Grok (runbook S2-15), never to Muse. | this report |
| R-15c-6 | FNP-14 cipher dependency: RustCrypto `aes` / `aes-gcm` / `cbc` / `ecb`, landing only with cargo-deny, audit and crate-DAG green. The card is written; the unit is not started. | card |
| R-15c-7 | BL-6's facade half (`F.bin` / `F.rint` cast first) lives in `functions*.py` and is a P2 for run 15a. | §6 |
| R-15c-8 | FNP-4B Q1: the Spark-facing display fields stay byte-identical and only internal-SQL golden fields may move to backticks. Q2: backtick spans are protected in the filter quoter. Q3: the cache/eager view leaks are FNP-4B regressions (53 passed on a main-state native). | fnp-4b ledger |
| R-15c-9 | The orchestrator applied the FNP-4B mechanical remediation itself: strip the actor's 127 added comment lines, restore the one pinned module-doc line, widen the rebind leaf regexes (cleared by 15b), and set size ceilings to real counts in both tables. | commit e41e4f98 |
| R-15c-10 | DOOR-CONVERGE-1 drops its registry-wide `promise_retag` wrapper and the `make_array` containsNull change (AGENTS "Fixes stay narrow"); both move to DOOR-CONVERGE-2 against oracle batch 7. | door-converge-1 ledger |
| R-15c-11 | JAVA-DOUBLE-STR-1: the JDK-17 `FloatingDecimal` digit generation is ported only if it fits the round; otherwise a dated BACKLOG row `JAVA-DOUBLE-FD-1` with codifying pins. | java-double-str-1 ledger |
| R-15c-12 | No second critic round on a remediation round whose every fix is pinned to a measured oracle cell (SQL-SET-DOOR-1 round 3, FNP-6D round 2). A semantic widening (DOOR-CONVERGE-1's retag) instead goes out of the unit. | this report |

## 5. Owner questions (with recommendations)

| # | Question | Recommendation |
|---|---|---|
| Q-15c-1 | FNP-14 needs a cipher dependency for `aes_encrypt` / `aes_decrypt` / `try_aes_decrypt`. Approve the RustCrypto `aes`, `aes-gcm`, `cbc`, `ecb` crates? | **Yes.** They are pure Rust, RustSec-audited and support 128/192/256-bit keys. The oracle cells F14-* are recorded. `mask` needs no dependency and can land first. |
| Q-15c-2 | Spark 4.1.2 runs on JDK 17, whose `Double.toString` is not shortest round-trip for some values (`8.41E21` → `8.409999999999999E21`, `1.0E23` → `9.999999999999999E22`). Should RePark port JDK 17 `FloatingDecimal` for byte-exact text, or accept the shortest form for those values? | **Port it** (about 300 deterministic lines). The 1.5 charter is byte-parity, and a CAST to STRING feeding a hash or a join key diverges silently otherwise. Until then the divergence stays a dated BACKLOG row, `JAVA-DOUBLE-FD-1` or recorded in the JAVA-DOUBLE-STR-1 ledger. |
| Q-15c-3 | `spark.conf.set` and SQL `SET` of `spark.sql.ansi.enabled` and `spark.sql.session.timeZone` are stored but not applied to a live session (SET-ANSI-RUNTIME-1, TZ-3). Spark applies both immediately. Build a per-query config snapshot through Session? | **Yes, as a 1.5 unit owned jointly by run 15b** (`RuntimeConfig`) and the Rust carriers (`repark_functions::ansi`, `session_time_zone`). It unblocks BL-11 (ANSI-off numeric → BINARY) and `escapedStringLiterals` via SET. |
| Q-15c-4 | Exact size baselines (`session_core.py` 2304) make any pure insertion a baseline raise that needs owner approval. Keep ratchet-only? | **Keep ratchet-only.** Tonight's net-zero edits worked. If a hook line is ever unavoidable, grant a one-time +N in the PR, not a standing allowance. |
| Q-15c-5 | FNP-13 (collation, G15 retirement) needs its own design note before a unit opens. When? | **After FNP-4B lands.** Collation changes comparison, ordering and hashing on both doors; it should start from the Databricks-dialect parser. |
| Q-15c-6 | DOOR-CONVERGE-1 round 2 proposed a registry-wide `promise_retag` wrapper that re-binds every scalar UDF's return field. It was taken out of the unit. Is a global wrapper an acceptable mechanism for DOOR-CONVERGE-2's element-nullability work? | **No global wrapper.** Fix per function with a batch-7 oracle pin each. A global wrapper moved one cell the wrong way (`array_append` nullability) and its claimed fix did not reach the SQL door. |
| Q-15c-7 | Pre-existing wrong answers found by oracle batch 7: `concat(array, array)` returns a STRING `'[1][2]'`, `reverse(array)` reverses the stringified text, and `sequence` / `split` are "Invalid function" on the SQL door. Priority? | **P1, top of DOOR-CONVERGE-2.** These are silent wrong values on the Spark door, not missing names. |
| Q-15c-8 | FNP-4B (the Spark-door dialect) queues after run 15b's #608 because both carry `dataframe/core.py` baseline rows. If it is not merged by 07:15, merge it in the morning after one rebase? | **Yes.** Rebase onto main, recompute the `core.py` baselines to the real count, re-run the gates, then merge. The PR body will list the rebase steps. |

## 6. P2 findings for the other runs

- **Run 15a:** `F.bin` / `F.rint` still cast BOOLEAN before the kernel (BL-6 facade half); `F.like` has no 3-argument escape form; `F.bround` absent (oracle `DIV-api-round`).
- **Run 15b:** `RuntimeConfig.set` of `spark.sql.ansi.enabled` and `spark.sql.session.timeZone` is stored but not applied (SET-ANSI-RUNTIME-1, TZ-3); `conf.unset` hides a builder value where Spark restores it (`S5-builder-after-reset`); `RuntimeConfig.set` silently stores `spark.wap.*` keys.

- **Run 15c's own P1 follow-up, FNP-6D-FOLLOWUP-1 (found at 06:33 by run 15a's critic on its facade PR #613):** in merged #609, `bitmap_or_agg` / `bitmap_and_agg` accept a Utf8 payload, so an INT / FLOAT / BOOLEAN argument is implicitly cast to text and folded as bytes where Spark refuses non-BINARY. `bitmap_construct_agg('abc')` answers the zero identity through the NULL skip where Spark fails the BIGINT cast. The oracle had cells for a coercible STRING (`'1'`) and for BINARY lengths, but none for a numeric payload or a malformed STRING. Recommendation: the next run's first unit, a signature-only fix plus two pins (report: run 15a `pa-bitmap-crit`).

## 7. Lessons (mechanics)

- **Oracle first, and oracle the reviewers too.** Ten batches (411 cells) settled every behaviour question. Four reviewer claims were wrong against a live cell, and one of my own fixture labels was wrong. Recording the answer before briefing cost about a minute per batch.
- **A cell spelled with a syntax another unit adds is not a test.** Batch 4 wrote doubles as `1.0E7D`, which parse only after FNP-4B; JAVA-DOUBLE-STR-1's probe read 24/44 until rewritten with `CAST('…' AS DOUBLE)` (21/21).
- **The comment ban needs an orchestrator grep on every hand-back.** Muse added 127 comment lines in one round. Stripping them blindly also removed a `//!` line that a parity test pins, and a `pins:` citation the ledger-grammar gate needs.
- **`make rust-clippy`, never a bare `cargo clippy -D warnings`.** The bare form reds on test-code `unwrap` by design.
- **The map lockstep hook wants every touched directory's `map.md` in the same commit**, including when the orchestrator only edits a regex or a ceiling.
- **Exact size baselines turn a pure insertion into a baseline raise.** The SET-door hook had to be net-zero, and the owner's approval is the only way to grow a file.
- **Free Devin tolerates one lane.** Two concurrent Devin rounds both died on "Reached free model rate limit".
- **A box-wide network failure (04:17) kills every Muse stream at once.** `--resume` on the same session recovers; nothing was lost.
- **Merge coordination between three runs works through cross-session messages.** Holds were announced with a PR number and an ETA, a four-deep queue was agreed, and each landing was pinged to the next owner. #597 had missed its squash three times before the hold.
- **A worker round that widens scope (DOOR-CONVERGE-1's registry-wide retag) looks green on its own gates.** It still needs a main-state probe to show which divergences it moved: 20 of 22 were pre-existing, 1 regressed, and 1 was fixed.
- **A shared `CARGO_TARGET_DIR` across clones is a race, not a cache.** Sibling lanes rebuilding the same workspace crates from different trees produced phantom `E0425 cannot find function` errors in `make verify` for DOOR-CONVERGE-1 and for the JAVA-DOUBLE-STR-1 actor, who also ran `cargo clean` on the shared cache (6.6 GiB) while diagnosing it. Lane-private targets fixed both. Budget about 30 G per lane, and remove it when the lane closes.
- **A 300-turn cap lost a whole round.** FNP-4B round 4 cost $24.85 and ended uncommitted mid-gate. Briefs for large rounds should demand a commit per clause group, not per round, so a cap or a crash leaves committed progress.
- **A comparison native built from a newer main is not a baseline.** My DOOR-CONVERGE-1 audit blamed a nullability change on the branch when main had moved (44ca3aea). Build the comparison from the branch's own merge-base.
- **Draft PRs from the last gated commit protect a night's work.** The lanes live in `/tmp`, which a reboot clears. #611 and #612 were pushed as drafts while their rounds were still running.
