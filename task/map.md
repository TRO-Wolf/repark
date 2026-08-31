# map — task/

## Purpose

Working state for **current** work: the rules in force, the unit ledgers by state, the roadmap by
horizon, and the acceptance inputs that gates still read. Finished **phases and campaigns** do not accumulate here —
they are archived under [../docs/history/](../docs/history/map.md) once their rules have been
promoted to a current document (mid-campaign phase promotions are allowed; see hardening-h1).

Current state (release, delivered surface, what happens next) is **[../STATUS.md](../STATUS.md)**,
not this directory.

## Contents
- [ledgers/](ledgers/map.md) — **the ledger bins (DL-1, 2026-08-23):** `staging/` →
  `completed/` → `archive/yyyy-mm/`; the directory is the status. Every unit ledger lives there
  (the 2026-08 backfill moved 122 to the archive and left four open charters in `staging/`).
- [roadmap/](roadmap/map.md) — **the roadmap by horizon (DL-1, 2026-08-23):** `mid-term/`
  (evaluated intakes awaiting a charter — the two 2026-08 intakes and the fork handoff) and
  `epic-term/` (north-star tracks). Short-term stays [../briefs/next-sequence.md](../briefs/next-sequence.md).

- [lrs-z-retrospective.md](lrs-z-retrospective.md) — **LRS close-out (2026-08-20):** seven units,
  the invariant held, and the two findings that matter most were both found while doing something
  else — `RE-1` and `LOG-1`, silently wrong answers on common functions. Also: the live Spark
  oracle refuted three of the Critic round's suggested fixes and one of mine.
- [fnp-0-census/](fnp-0-census/map.md) — the measured evidence that gate rests on: the facade
  classification of all 345 functions, the PySpark 4.1.2 gap partition, the higher-order-function
  spec, and the kernel ownership map.
- [window-bench-report-2026-08-31.md](window-bench-report-2026-08-31.md) — **W-0 (2026-08-31):**
  dated window-shape measurements (sliding / constant / 1e7 unpartitioned / Iceberg
  lead-lag / over `memory_limit`) plus the thirteen sliding-frame refusals. Ledger:
  [ledgers/staging/w-0-window-bench-ledger.md](ledgers/staging/w-0-window-bench-ledger.md).
- [todo.md](todo.md) — a **pointer only**: the live backlog is [../STATUS.md](../STATUS.md), and a
  unit's working plan is its own ledger. The file keeps its name because live code, docs and one
  runtime error message cite this path.
- [lessons.md](lessons.md) — DO / DO-NOT rules in force (append date-stamped; supersede, don't
  delete). Seeded 2026-08-06 from the private v1 repository. PR-245 adds original-source mapping
  for rewritten SQL locations, AST-based enumerable syntax guards, and lifecycle-aware ledger
  pins; PR-247 adds completed-ledger citation discipline; the 2026-08-27 correction preserves
  model provenance while removing code-quality grades.
- [metrics.md](metrics.md) — the **process metrics ledger**: one section per retrospective, the
  eight-metric set the SEPMO retrospective contract fixes (findings per cycle, cycles to
  convergence, noise ratio, coverage misses, escaped defects by origin, LIGHT-path escapes, flags
  shipped, environment drift events). Append a section per campaign; never rewrite an earlier one.
  Created 2026-08-10 with the Front-Door campaign's numbers.
- [census/](census/map.md) — what a gate still reads of the recorded census runs: the
  baseline's facade cohort (`collected.txt`, `facade.xml`), pinned by `test_deferred_ledger.py`.
  The rest was evicted 2026-08-23 and lives at `b13b22c` ([../docs/port/census.md](../docs/port/census.md) §7).
- [mw-6-critic-evidence/](mw-6-critic-evidence/map.md) — the Critic-round evidence the archived
  **MW-6** ledger cites by path, given a durable home (PROC-1, 2026-08-25) so a scratch-directory
  `rm -rf` cannot strand a committed ledger's citations. Verbatim, never hand-edited — excluded
  from `ruff`/`typos` like `census/`.
- [port/](port/map.md) — **live acceptance inputs**: the deferred-test manifest and its
  reconciliation rule ([port/deferred-tests.md](port/deferred-tests.md)), the machine-readable
  deferral allowlist ([port/deferred-python-tests.txt](port/deferred-python-tests.txt)) and its
  mirror additions ledger ([port/added-python-tests.txt](port/added-python-tests.txt)). The census
  comparator still subtracts these, so they are not history.



## Where the closed campaigns' ledgers went

The seventeen `p1*` / `p2*` / `p3*` unit ledgers, the four phase briefs and the port's `todo.md`
execution log moved to [../docs/history/port-v2/](../docs/history/port-v2/map.md) on **2026-08-09**
(Front-Door FD-4), keeping their basenames. A citation of `task/p3e-facade-ledger.md` — a few
survive in Rust doc comments — means
[../docs/history/port-v2/p3e-facade-ledger.md](../docs/history/port-v2/p3e-facade-ledger.md), and so
on. Nothing was lost in the move; the audit is
[../docs/history/port-v2/promotion-ledger.md](../docs/history/port-v2/promotion-ledger.md).

`fd3-ledger.md` left the same way on **2026-08-10**, at the Front-Door campaign's close-out: it is
[../docs/history/frontdoor/fd3-ledger.md](../docs/history/frontdoor/fd3-ledger.md), alongside that
campaign's design, slate and retrospective. Its audit is the retrospective's "Promotion check"
section, and the one rule it stranded — set a repo-local git identity before the first commit — was
promoted into [lessons.md](lessons.md) (2026-08-10) **before** the move.

**H-1 phase ledgers** (and the parallel G/N corpus units delivered through the H-1 close gate,
repark #35–#46) moved on **2026-08-11** by **G-9** — a **mid-campaign** phase promotion, not a
campaign close-out. Basenames kept under
[../docs/history/hardening-h1/](../docs/history/hardening-h1/map.md):

| Former `task/` path | Now |
|---|---|
| `task/h1d-ledger.md` | [../docs/history/hardening-h1/h1d-ledger.md](../docs/history/hardening-h1/h1d-ledger.md) |
| `task/h1a-ledger.md` | [../docs/history/hardening-h1/h1a-ledger.md](../docs/history/hardening-h1/h1a-ledger.md) |
| `task/h1c-ledger.md` | [../docs/history/hardening-h1/h1c-ledger.md](../docs/history/hardening-h1/h1c-ledger.md) |
| `task/h1b-ledger.md` | [../docs/history/hardening-h1/h1b-ledger.md](../docs/history/hardening-h1/h1b-ledger.md) |
| `task/g4-tests-split-ledger.md` | [../docs/history/hardening-h1/g4-tests-split-ledger.md](../docs/history/hardening-h1/g4-tests-split-ledger.md) |
| `task/g4-artifacts/` | [../docs/history/hardening-h1/g4-artifacts/](../docs/history/hardening-h1/g4-artifacts/map.md) |
| `task/g5-sweep-ledger.md` | [../docs/history/hardening-h1/g5-sweep-ledger.md](../docs/history/hardening-h1/g5-sweep-ledger.md) |
| `task/g6-chores-ledger.md` | [../docs/history/hardening-h1/g6-chores-ledger.md](../docs/history/hardening-h1/g6-chores-ledger.md) |
| `task/g7-decimal-ledger.md` | [../docs/history/hardening-h1/g7-decimal-ledger.md](../docs/history/hardening-h1/g7-decimal-ledger.md) |
| `task/n2-merge-ledger.md` | [../docs/history/hardening-h1/n2-merge-ledger.md](../docs/history/hardening-h1/n2-merge-ledger.md) |
| `task/g8-file-size-ledger.md` | [../docs/history/hardening-h1/g8-file-size-ledger.md](../docs/history/hardening-h1/g8-file-size-ledger.md) |

A citation of `task/h1d-ledger.md` (or any row above) means the matching file under
`docs/history/hardening-h1/`. The audit is
[../docs/history/hardening-h1/promotion-ledger.md](../docs/history/hardening-h1/promotion-ledger.md).

## Live unit ledgers

| Ledger | Unit |
|---|---|
| [df1-rust-flatten-ledger.md](ledgers/archive/2026-08/2026-08-20-df1-rust-flatten-ledger.md) | **DF1** native `dynamic_flatten` plan rewrite + thin facade |
| [rsix-rsi-sma-iter-ledger.md](ledgers/archive/2026-08/2026-08-15-rsix-rsi-sma-iter-ledger.md) | **T5** conductor-15 iterator-form `rsi`/`sma` (bit-exact) |
| [s1-spill-truth-ledger.md](ledgers/archive/2026-08/2026-08-15-s1-spill-truth-ledger.md) | **S-1** spill truth and reach (FairSpillPool SET / temp_directory / RAM default) |
| [c19-al1a-mimalloc-ledger.md](ledgers/archive/2026-08/2026-08-16-c19-al1a-mimalloc-ledger.md) | **AL-1a / conductor-19** feature-gated mimalloc (default off) |
| [c19-bh1-default-conf-ledger.md](ledgers/archive/2026-08/2026-08-16-c19-bh1-default-conf-ledger.md) | **BH-1 / conductor-19** TA bench default-conf primary (measure-only) |
| [p1-ta-kernel-benches-ledger.md](ledgers/archive/2026-08/2026-08-15-p1-ta-kernel-benches-ledger.md) | **P-1** criterion TA kernel baseline (measure-only) |
| [m14-abort-cleanup-ledger.md](ledgers/archive/2026-08/2026-08-15-m14-abort-cleanup-ledger.md) | **M14** rejected MERGE commit abort-deletes written files |
| [bl4-update-store-assign-ledger.md](ledgers/archive/2026-08/2026-08-15-bl4-update-store-assign-ledger.md) | **BL-4** UPDATE SET ANSI store-assignment gate (shared INSERT matrix) |
| [wi1-insert-store-gate-ledger.md](ledgers/archive/2026-08/2026-08-15-wi1-insert-store-gate-ledger.md) | **WI-1** ANSI store-assignment gate on the non-MERGE write paths (matrix hoisted to `write/store_assign.rs`; plain `INSERT INTO` named unclosed) |
| [wi2-g6-cast-integrity-ledger.md](ledgers/archive/2026-08/2026-08-16-wi2-g6-cast-integrity-ledger.md) | **G6-3 / G6-5 + WI-2** — Spark-parity `CAST`/`TRY_CAST` DATE↔INT refusals in `analyzer/cast_legality.rs`, and the plain-INSERT store gate as an `AnalyzerRule` over `Dml(Insert)` (`write/insert_gate.rs`). Names the `VALUES`-literal residual |
| [m16-posdelete-specid-ledger.md](ledgers/archive/2026-08/2026-08-15-m16-posdelete-specid-ledger.md) | **M16** evolved unpartitioned position-delete `spec_id` |
| [fn-d-datetime-ledger.md](ledgers/archive/2026-08/2026-08-15-fn-d-datetime-ledger.md) | **FN-D** datetime aliases/shims — 11 shipped, rest honest-cut |
| [fn-e-collections-ledger.md](ledgers/archive/2026-08/2026-08-15-fn-e-collections-ledger.md) | **FN-E** collections / higher-order alias batch |
| [ta3-volume-goldens-ledger.md](ledgers/archive/2026-08/2026-08-15-ta3-volume-goldens-ledger.md) | **TA-3** volume-family goldens (`ad`/`adosc`/`obv`/`mfi`) — recorder + C recon, no kernels |
| [fn-f-try-bitwise-ledger.md](ledgers/archive/2026-08/2026-08-15-fn-f-try-bitwise-ledger.md) | **FN-F** try / session / bitwise — 10 shipped, rest deferred |
| [mg1-scanprune-hardening-ledger.md](ledgers/archive/2026-08/2026-08-15-mg1-scanprune-hardening-ledger.md) | **MG-1** scan-prune hardening — M1/M5/M6/M7 |
| [r3-g8-absences-ledger.md](ledgers/archive/2026-08/2026-08-14-r3-g8-absences-ledger.md) | **R-3 / G8** four pin-absences → Tested (JOIN both doors, ANSI WINDOW + FLOAT) |
| [r4-tz8-ledger.md](ledgers/archive/2026-08/2026-08-14-r4-tz8-ledger.md) | **R-4 / TZ-8** CAST(ts AS DATE) / to_date session-zone dates; datediff residual |
| [r2-dec-close-ledger.md](ledgers/archive/2026-08/2026-08-14-r2-dec-close-ledger.md) | **R-2** DEC close — U4b `/` + DEC-8 `ExprPlanner` + DEC-6 exec-raise; TY-3 DECLARED |
| [s5-v-landing-ledger.md](ledgers/archive/2026-08/2026-08-13-s5-v-landing-ledger.md) | **S-5** V-wave §6 landing increment — registry + one STATUS dated note |
| [s2-g8-ledger.md](ledgers/archive/2026-08/2026-08-13-s2-g8-ledger.md) | **S-2 / G8** capability value-semantics matrix + test-name liveness gate |
| [s1-ansi-knob-u5-ledger.md](ledgers/archive/2026-08/2026-08-13-s1-ansi-knob-u5-ledger.md) | **S-1 / U5** ANSI knob default TRUE + DEC-7 `/0`/`% 0`; DEC-6 DECLARE; DEC-9 residue |
| [v5-w-landing-ledger.md](ledgers/archive/2026-08/2026-08-13-v5-w-landing-ledger.md) | **V-5** W-wave §6 landing increment — registry + one STATUS dated note |
| [v4-partition-values-ledger.md](ledgers/archive/2026-08/2026-08-13-v4-partition-values-ledger.md) | **V-4** write-path partition-key VALUE audit — carry-check + load-bearing + TZ-8 |
| [v2-dec-u3u4-ledger.md](ledgers/archive/2026-08/2026-08-13-v2-dec-u3u4-ledger.md) | **V-2** DEC U3+U4a — integer-literal min-precision + add/sub/mul 38-clamp |
| [w5-z-landing-ledger.md](ledgers/archive/2026-08/2026-08-13-w5-z-landing-ledger.md) | **W-5** Z-wave §6 landing increment — registry + one STATUS dated note |
| [z5-landing-increment-ledger.md](ledgers/archive/2026-08/2026-08-13-z5-landing-increment-ledger.md) | **Z-5** Y-wave §6 landing increment — registry + one STATUS dated note |
| [l1-landing-truth-ledger.md](ledgers/archive/2026-08/2026-08-12-l1-landing-truth-ledger.md) | **L-1** landing-truth — STATUS + registry + live-mirror both-halves + G14 |
| [y4-rename-ledger.md](ledgers/archive/2026-08/2026-08-13-y4-rename-ledger.md) | **Y-4 / G4b-R1** declared rename (G4b flipped rows + TZ-5 nullability row) |
| [n2b-merge-followup-ledger.md](ledgers/archive/2026-08/2026-08-11-n2b-merge-followup-ledger.md) | **N-2b / W-2** MERGE follow-up — items 1+4 in PR #50; items 2+3 (lifecycle live + 13 tz live scenarios) in the second PR. Full N-2b closed only when **both** PRs land. |
| [x5-nested-comparator-ledger.md](ledgers/archive/2026-08/2026-08-12-x5-nested-comparator-ledger.md) | **X-5 / G18** nested comparator + nested-container corpus (Part 1+2) |
| [x4-catalog-forwards-ledger.md](ledgers/archive/2026-08/2026-08-12-x4-catalog-forwards-ledger.md) | **X-4 / G17** catalog wrapper explicit forwards (HIGH `publish_replace_table`) |
| [y10-ansi-door-ledger.md](ledgers/archive/2026-08/2026-08-13-y10-ansi-door-ledger.md) | **Y-10 / G11** ANSI door — correctness, not parity |
| [y3-getdatabase-ledger.md](ledgers/archive/2026-08/2026-08-13-y3-getdatabase-ledger.md) | **Y-3** `getDatabase` + G-6 live-leg |
| [y5-origin-map-ledger.md](ledgers/archive/2026-08/2026-08-13-y5-origin-map-ledger.md) | **Y-5 / G4b-R2** semi/anti origin-map join-type awareness |
| [g5br-range-residuals-ledger.md](ledgers/archive/2026-08/2026-08-13-g5br-range-residuals-ledger.md) | **Y-1 / G5b-R** window-RANGE residuals — R3 empty-frame fix (Half-B: kind-or-magnitude invert, no YEAR pair), R2 DAY TO SECOND, R1/R4/R5 deferred; ANSI wrap residual |

## I want to...

| ...do this | go to |
|---|---|
| See the live backlog / what happens next | [../STATUS.md](../STATUS.md) |
| Read the DF1 native `dynamic_flatten` port | [df1-rust-flatten-ledger.md](ledgers/archive/2026-08/2026-08-20-df1-rust-flatten-ledger.md) |
| Read the U-DF-1 explode mixed-case bind | [c17-explode-case-ledger.md](ledgers/archive/2026-08/2026-08-16-c17-explode-case-ledger.md) |
| Read the T5 rsi/sma iterator-form rewrite | [rsix-rsi-sma-iter-ledger.md](ledgers/archive/2026-08/2026-08-15-rsix-rsi-sma-iter-ledger.md) |
| Read the AL-1a feature-gated mimalloc spike | [c19-al1a-mimalloc-ledger.md](ledgers/archive/2026-08/2026-08-16-c19-al1a-mimalloc-ledger.md) |
| Read the BH-1 default-conf bench-harness fix | [c19-bh1-default-conf-ledger.md](ledgers/archive/2026-08/2026-08-16-c19-bh1-default-conf-ledger.md) |
| Read the P-1 criterion TA kernel baseline | [p1-ta-kernel-benches-ledger.md](ledgers/archive/2026-08/2026-08-15-p1-ta-kernel-benches-ledger.md) |
| Read the M14 rejected-commit abort cleanup | [m14-abort-cleanup-ledger.md](ledgers/archive/2026-08/2026-08-15-m14-abort-cleanup-ledger.md) |
| Read the BL-4 UPDATE SET store-assignment gate | [bl4-update-store-assign-ledger.md](ledgers/archive/2026-08/2026-08-15-bl4-update-store-assign-ledger.md) |
| Read the M16 evolved-spec position-delete stamp | [m16-posdelete-specid-ledger.md](ledgers/archive/2026-08/2026-08-15-m16-posdelete-specid-ledger.md) |
| Read the G8 value-semantics matrix + liveness gate | [s2-g8-ledger.md](ledgers/archive/2026-08/2026-08-13-s2-g8-ledger.md) |
| Read the R-3 flip of the four G8 pin-absences | [r3-g8-absences-ledger.md](ledgers/archive/2026-08/2026-08-14-r3-g8-absences-ledger.md) |
| Read the FN-D datetime function batch | [fn-d-datetime-ledger.md](ledgers/archive/2026-08/2026-08-15-fn-d-datetime-ledger.md) |
| Check a rule before acting | [lessons.md](lessons.md) |
| See how a data-loss defect is localized, valved and oracled before it is fixed | [g3e8-guard-ledger.md](ledgers/archive/2026-08/2026-08-11-g3e8-guard-ledger.md) |
| Find out why a `DELETE`/`UPDATE` with a subquery `WHERE` is refused | [g3e8-guard-ledger.md](ledgers/archive/2026-08/2026-08-11-g3e8-guard-ledger.md) §2 (the matrix) + §3 (D-3, the deliberate over-refusal) |
| See which G3-E8 spelling now executes (IN-DELETE) | [z1-g3e8-pr1-ledger.md](ledgers/archive/2026-08/2026-08-13-z1-g3e8-pr1-ledger.md) |
| See which G3-E8 spelling now executes (NOT IN + NULL trap) | [w3-g3e8-pr2-ledger.md](ledgers/archive/2026-08/2026-08-13-w3-g3e8-pr2-ledger.md) |
| Start a new unit's ledger | copy the shape of the archived [h1d-ledger.md](../docs/history/hardening-h1/h1d-ledger.md) (or [fd3-ledger.md](../docs/history/frontdoor/fd3-ledger.md)); link it from this map in the same commit |
| See how a §6 handoff is classified and landed (or superseded) | [l1-landing-truth-ledger.md](ledgers/archive/2026-08/2026-08-12-l1-landing-truth-ledger.md) (W/X wave) · [z5-landing-increment-ledger.md](ledgers/archive/2026-08/2026-08-13-z5-landing-increment-ledger.md) (Y wave) · [w5-z-landing-ledger.md](ledgers/archive/2026-08/2026-08-13-w5-z-landing-ledger.md) (Z wave) · [v5-w-landing-ledger.md](ledgers/archive/2026-08/2026-08-13-v5-w-landing-ledger.md) (W wave) · [s5-v-landing-ledger.md](ledgers/archive/2026-08/2026-08-13-s5-v-landing-ledger.md) (V wave) |
| See how a divergence gets declared, pinned and mirrored | [../docs/history/hardening-h1/h1d-ledger.md](../docs/history/hardening-h1/h1d-ledger.md), then [../docs/spark-sql-iceberg-parity.md](../docs/spark-sql-iceberg-parity.md) §6 |
| Read why the session timezone is a build-time knob with one spelling | [../docs/history/hardening-h1/h1a-ledger.md](../docs/history/hardening-h1/h1a-ledger.md) |
| Read how timestamp extraction came to honor it, and what the fix deliberately did NOT close | [../docs/history/hardening-h1/h1a-ledger.md](../docs/history/hardening-h1/h1a-ledger.md) "§ Split B" |
| See how an open question gets FIXED instead of declared (and why a fixed defect gets no row) | [../docs/history/hardening-h1/h1c-ledger.md](../docs/history/hardening-h1/h1c-ledger.md) + [../docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md](../docs/adr/0006-hide-iceberg-metadata-tables-from-enumeration.md) |
| Find out why a `__repark_tt_*` name is on a session, and which of its three producers put it there | [../docs/history/hardening-h1/h1b-ledger.md](../docs/history/hardening-h1/h1b-ledger.md), then [../crates/repark-spark/src/map.md](../crates/repark-spark/src/map.md) `## Debug` |
| See what a two-mutation acceptance looks like (and why the second mutation is the one that matters) | [../docs/history/hardening-h1/h1b-ledger.md](../docs/history/hardening-h1/h1b-ledger.md) §7c/§7d |
| Read the MERGE INTO differential corpus (gap G3) ledger + registry paste rows | [../docs/history/hardening-h1/n2-merge-ledger.md](../docs/history/hardening-h1/n2-merge-ledger.md) |
| Read the decimal128 differential corpus ledger (Python half) | [../docs/history/hardening-h1/g7-decimal-ledger.md](../docs/history/hardening-h1/g7-decimal-ledger.md) |
| Read the decimal128 Rust half (G-7b pins + cross-door) | [g7b-decimal-rust-ledger.md](ledgers/archive/2026-08/2026-08-11-g7b-decimal-rust-ledger.md) |
| Read the window-function differential corpus (gap G5) ledger | [w4-windows-ledger.md](ledgers/archive/2026-08/2026-08-11-w4-windows-ledger.md) |
| Read the G5b-R window-RANGE residual dispositions (Y-1) | [g5br-range-residuals-ledger.md](ledgers/archive/2026-08/2026-08-13-g5br-range-residuals-ledger.md) |
| Read the V-4 write-path partition-value audit | [v4-partition-values-ledger.md](ledgers/archive/2026-08/2026-08-13-v4-partition-values-ledger.md) |
| Read the R-4 TZ-8 CAST/to_date session-zone fix | [r4-tz8-ledger.md](ledgers/archive/2026-08/2026-08-14-r4-tz8-ledger.md) |
| Read the W-4 Z-wave residual close (R1/R5/Q-002) | [w4-z-residuals-ledger.md](ledgers/archive/2026-08/2026-08-13-w4-z-residuals-ledger.md) |
| Read the nested comparator + nested-container corpus (gap G18) ledger | [x5-nested-comparator-ledger.md](ledgers/archive/2026-08/2026-08-12-x5-nested-comparator-ledger.md) |
| Read the G17 catalog-wrapper forwards ledger | [x4-catalog-forwards-ledger.md](ledgers/archive/2026-08/2026-08-12-x4-catalog-forwards-ledger.md) |
| Read the G11 ANSI-door correctness-not-parity ledger | [y10-ansi-door-ledger.md](ledgers/archive/2026-08/2026-08-13-y10-ansi-door-ledger.md) |
| Read the getDatabase / G-6 live-leg ledger | [y3-getdatabase-ledger.md](ledgers/archive/2026-08/2026-08-13-y3-getdatabase-ledger.md) |
| Read the three-valued-logic differential corpus (gap G12) ledger | [x2-tvl-ledger.md](ledgers/archive/2026-08/2026-08-12-x2-tvl-ledger.md) |
| See how a corpus refuse-split gets FIXED and flipped (and why the row keeps its name) | [g4b-join-widening-ledger.md](ledgers/archive/2026-08/2026-08-12-g4b-join-widening-ledger.md) §2 D2 / §4 |
| See the declared-rename map that retired those kept names (and the TZ-5 flip-row name) | [y4-rename-ledger.md](ledgers/archive/2026-08/2026-08-13-y4-rename-ledger.md) |
| See why `select(right["k"])` after a semi join must raise `MISSING_ATTRIBUTES` | [y5-origin-map-ledger.md](ledgers/archive/2026-08/2026-08-13-y5-origin-map-ledger.md) |
| See why a dependency edge or a manifest field is gated, and the proofs it fires | [../docs/history/frontdoor/fd3-ledger.md](../docs/history/frontdoor/fd3-ledger.md) |
| File a retrospective's metrics | [metrics.md](metrics.md) — append a section, never rewrite one |
| See which v1 tests are deferred, and why | [port/deferred-tests.md](port/deferred-tests.md) |
| Feed the census comparator its allowlists | [port/map.md](port/map.md) |
| Run or compare a census | [../docs/port/census.md](../docs/port/census.md) |
| Read the port's record (briefs, unit ledgers, retrospectives) | [../docs/history/port-v2/README.md](../docs/history/port-v2/README.md) |
| Read the H-1 phase archive (mid-campaign) | [../docs/history/hardening-h1/README.md](../docs/history/hardening-h1/README.md) |
| Read the port plan the phases executed | [../docs/port/PLAN.md](../docs/port/PLAN.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../AGENTS.md](../AGENTS.md) (the durable contract) and [../STATUS.md](../STATUS.md)
  (current state); this directory holds the moving parts of work in flight.
- Unit ledgers: one `<unit>-ledger.md` per delivered unit, with gate evidence and provocation proofs
  per [../docs/testing.md](../docs/testing.md), linked from this map in the same commit. When a
  **phase or campaign** closes (or is deliberately mid-campaign-promoted), its ledgers are archived
  under [../docs/history/](../docs/history/map.md) after a promotion audit — never deleted.

## Debug

- `pg-integration-report.md` may appear here untracked: `python/repark/tests/test_pg_acceptance.py`
  writes it (CWD-relative) on every facade run. It is gitignored on purpose — a run output, not a
  record. Do not `git add` it.
- If work and trackers disagree, the code is truth — update the tracker.
- A link into `task/p*-ledger.md` or `task/fd3-ledger.md` fails: see "Where the closed campaigns'
  ledgers went" above — same basename, under [../docs/history/](../docs/history/map.md).
- A link into `task/h1*-ledger.md`, `task/g4-*-ledger.md`, `task/g5-sweep-ledger.md`,
  `task/g6-chores-ledger.md`, `task/g7-decimal-ledger.md`, `task/n2-merge-ledger.md`,
  `task/g8-file-size-ledger.md`, or `task/g4-artifacts/` fails the same way: those moved to
  [../docs/history/hardening-h1/](../docs/history/hardening-h1/map.md) on **2026-08-11** (G-9).
- **H-1 phase ledgers were promoted mid-campaign** to
  [../docs/history/hardening-h1/](../docs/history/hardening-h1/map.md) (2026-08-11). **H-2+ unit
  ledgers re-accumulate here** until the next promotion. Empty-of-ledgers is again a valid steady
  state between units (the campaign continues; only the closed H-1 phase record left).
- Looking for a backlog item that is not in [../STATUS.md](../STATUS.md)? Check
  [../docs/history/port-v2/promotion-ledger.md](../docs/history/port-v2/promotion-ledger.md) or
  [../docs/history/hardening-h1/promotion-ledger.md](../docs/history/hardening-h1/promotion-ledger.md)
  — if it was live at archival, that table says where it went.
