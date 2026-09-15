# Overnight report — run 14b of 2026-09-14 (day run: ARRAY-NULL-1, FACADE-4 step 1)

**Session:** one Opus orchestrator (overnight-14b), 2026-09-14 12:58 → 23:30 local, beside run 14 (overnight-14: the
Iceberg cutover slate, i- and fork lanes). **Grants:** G-1 yes, G-2 yes, G-3 stop when the list is exhausted or by 23:30
local, G-4 Devin actor + Grok critic/reviewers (no Muse, no GLM), G-5 yes. **Procedure:**
[overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md). STATUS.md, tags and the
release pipeline untouched; no run-14-owned file edited by either unit.

## 1. Outcome

| # | Unit | Result | PR | Merged | Rounds |
|---|---|---|---|---|---|
| 1 | ARRAY-NULL-1 (steps 0+1 + coercion) | **ready for review, not merged** — CI green on `087074ce`, then main moved; parked at the stop (R14b-D-14) | #581 | — (owner merges when CI is green) | Devin 4 follow-ups on `painted-magnesium` ($0); Grok critic-logic ×2 ($1.22 + $1.22), S2-21 ×2 ($1.05 + $0.80) |
| 2 | FACADE-4 step 1 | **merged** | #582 | `7693ef23` (tree-equal) | Devin 4 follow-ups on `coherent-sheet` ($0); Grok critic-logic ×3 ($0.99 + $1.01 + $1.00), S2-21 Rust + Python re-checks ($0.94 + $1.67) |
| 3 | ANSI-DOOR-1 step 0 | not started — the list's first two units took the window | — | — | — |

Grok spend this run: $11.87 over nine rounds. Devin: eight follow-up rounds, $0.

## 2. ARRAY-NULL-1

Draft #581 arrived with the null-preserving `ScalarUDF` and two pre-existing coercion findings (L-1 `array<string>` + int
recast the array; L-2 decimal/double cells unmeasured).

**Spark measured first (orchestrator, PySpark 4.1.2, `local[1]`, zulu-17), three passes, 100+ cells:** the element and
the array element resolve to Spark's recursive tightest common type — numeric precedence byte < short < int < long <
float < double; string vs non-string and any decimal mismatch refuse with
`DATATYPE_MISMATCH.ARRAY_FUNCTION_DIFF_TYPES`; arrays, maps and structs widen recursively (structs by position,
case-insensitive names); date + timestamp → timestamp at midnight in the session zone; timestamp_ntz + timestamp →
timestamp; µs precision, 0001-01-01 and 9999-12-31 survive. Oracle tables are in the ledger.

| Reading (release natives) | Before (`main`) | After (#581) |
|---|---|---|
| `array<string>` + int | array recast to `list<int>`; letters fail at collect | refuses at planning, Spark's class in the message, both doors |
| `array<float>` + 16777217 | `list<double>` 16777217 | `list<float>` 16777216.0 (Spark) |
| `array<float16>` + 100000 | `Inf` | `float` 100000.0 |
| `array<date>` + timestamp, LA session | UTC midnight; SQL door wraps 0001-01-01 → 1754 (`timestamp[ns]`) | LA midnight, µs, 0001/9999 exact (oracle unix micros) |
| `array<array<int>>` + `array<bigint>` | refuses | `array<array<bigint>>` (Spark) |
| depth-40 chain RSS / collect | worker dies at level 15 (CASE explosion) | ~1.7–2.0 MB / ~57 ms; pin bound max(2×flat, 2×depth-4) |
| 100 % NULL batch, 1e6 rows (facade) | CASE 3.09 ms | 0.77 ms |
| date→LTZ widening, 1e6 × 8 values | wrong answer (UTC) | 185.9 ms (round-3 review measured 6.7 s before the day-span cache and window slicing) |
| `array<timestamp[ns]>` + timestamp | — | 35.4 ms (1.39 s before the unit-only rescale) |
| identical-type 0 % / 10 % NULL | DataFusion kernel floor | within 2–3 % of the floor, no CAST |

**Gates (orchestrator)** on `50628e13`, rebased on main `84992add`: `make preflight` rc 0 (facade suite 6267 passed / 373
skipped), whole parity suite 757 passed / 2 skipped / 12 xfailed, `make verify` rc 0, release native + every
`array_append`/`array_prepend` pin file 200 passed. Shipped as `593a23d3` after a `map.md`-only rebase onto FACADE-4's
merge; CI is the gate of record on the combined tree (R14b-D-13).

**Orchestrator verification of the S2-21 round-3 bars** (release native of `50628e13` on a consistent bed, the reviewer's
own harness, 3 repeats × medians of 5):

| cell | bar | measured |
|---|---:|---:|
| 100 % NULL identical-type, facade | ≤ 0.90 ms | 0.78–0.86 ms |
| `array<timestamp[ns]>` + timestamp | ≤ 55 ms | 25.6–26.5 ms |
| date→LTZ, facade, LA session | ≤ 620 ms | 179.8–188.2 ms (base 204.5–213.4 ms, wrong zone at ns) |
| NTZ→LTZ, facade | — | 171.0–173.4 ms |
| int→bigint widening | — | 22.8–23.7 ms |
| `array<timestamp[us]>` identical-type | — | 18.1–19.2 ms |

**Review rounds.**

| round | verdict | outcome |
|---|---|---|
| critic-logic round 2 (after the first coercion fix) | L-1/L-2 closed on 66 cells; P1 L-5 ns wrap, P1 L-6 session zone, P2 L-7 float+int, L-8 nested, L-9 NTZ/LTZ, L-10 substring pin; P3 L-11 | all measured on the oracle first; L-9 followed the oracle (widen), not the critic's "refuse"; fixed in follow-up 4 |
| S2-21 round 2 | no P1/P2; round-1 P2-1 pin floor and P3-1 all-null fixed | P3-A CAST residue (later removed) |
| critic-logic round 3 | L-5..L-11 closed; round-2 L-8 withdrawn (the actor's evidence held); P2 L-12 float16 Inf; P3 L-13..L-15 | L-12 fixed (float16 as FLOAT), L-13 pinned as the product rule, L-14/L-15 residue |
| S2-21 round 3 | P1 all-null short-circuit after conversion (+28.5 %); P2 localization 6.7 s; P2 ns→µs 1.39 s | fixed in follow-up 6 with numeric bars; root cause: each batch converted the whole shared child buffer |
| orchestrator verification (no fourth round, R14b-D-12) | multi-batch sliced-input differential 0 mismatches over 51,206 rows; bar cells re-measured on the rebased release native (§2 verification table) | — |

**Residue (ledger):** DataFusion-only alias spellings (`list_append`, `array_push_*`) keep the null-dropping kernel; the
Spark error class travels in the message text (a dedicated Python class needs `error_map.rs`, owned by run 14);
struct field matching ignores `spark.sql.caseSensitive=true`; the SQL door cannot spell a `TIMESTAMP_NTZ'…'` literal;
~O(d²) analyzer planning on deep 1-row chains.

## 3. FACADE-4 step 1

One Rust conversion table under every type-conversion surface, with every public answer, refusal class and message kept
byte for byte. No census D-row is unified; the Q-R13-1..10 rulings sit in the ledger as the targets of the unification
slices.

| Reading (release natives, ABAB) | Before (`main`) | After (#582) |
|---|---|---|
| end-to-end cells (a)–(d), 1e5 rows | — | every cell within ±5 % (worst `collect` +1.8 %) |
| `dtypes` wide50 | 60.5 µs | first pass 124.7 µs (P1); after the fixes −3.5 % (actor), −20 % (orchestrator spot-check, loaded box) |
| `df.schema` / `printSchema` / DESCRIBE | — | within the bar or faster (nested3 −30 to −50 %) |
| `fromDDL` wide50 | 125.7 µs | 69–87 µs |
| `struct_type_from_arrow` wide50 | 100 µs | 67–69 µs |
| `repark_type_to_arrow` wide50 | 60 µs | 117–202 µs (under 1 ms; P2 residue) |
| `toDDL` wide50 | 25 µs | 95–193 µs (under 1 ms; P2 residue) |
| two-parent type classes (600 ordered pairs) | — | 600/600 byte-identical (critic's probe, re-run by the orchestrator: DIFF_COUNT=0) |
| `types.py` exact baseline | 1834 | 1792 |

**Gates (orchestrator)** on `2d6a7735`, rebased on main `4d6b1ab0`: `make preflight` rc 0 (facade suite 6256 passed / 373
skipped), whole parity suite 757 passed / 2 skipped / 12 xfailed, `make verify` rc 0, release native + FACADE-4 pins 299
passed / 7 skipped. Orchestrator ABAB spot-check on release natives (three passes, load 12–18): `dtypes` flat7 10.70 →
9.52 µs, `df.schema` flat7 8.11 → 7.55, `dtypes` wide50 59.45 → 47.83, `df.schema` wide50 44.08 → 40.86. CI green after
update-branch over run 14's report; squashed as `7693ef23`, tree-equal.

**Review rounds.**

| round | verdict | outcome |
|---|---|---|
| critic-logic round 1 (never ran in run 13) | P1 L-001 i64 saturation, P1 L-002 subclass `simpleString`, P2 L-003..L-006 | fixed and pinned |
| actor HALT | `dtypes` +13 / +15.5 % against the bar | ruled (R14b-D-2): per-class literal methods; fixed |
| S2-21 Rust re-check | no P1; every surface and 1-row e2e cell within ±5 %; P2-DICT-OUT open | residue (Q-R13-14, R14b-D-5) |
| S2-21 Python re-check | no P1; every e2e and surface cell within +5 % (worst csv infer +3.2 %); nested `simpleString` and outbound dict P2 | residue (Q-R13-14, R14b-D-5) |
| critic-logic round 2 | P1 L-007 StructField override, L-008 interval `toDDL` leaf, L-009 multiple inheritance | fixed and pinned |
| critic-logic round 3 | 580/600 pairs identical; P1 L-010 (15 pairs), L-011 (5 pairs) | fixed; verified by the critic's own probe (R14b-D-11) |

**Residue (ledger):** outbound descriptor dict tree and nested `simpleString` (P2, next round); DDL parse has no depth
cap (card DDL-DEPTH-1); spaced refusals parse twice; Arrow depth-64 fallback cost; JSON round-trip, foreign subtypes and
wide decimals on Python paths.

## 4. Rulings applied (run 14b)

Owner standing instruction: follow each report's recommendation, with two limits.

| Ruling | Applied |
|---|---|
| Q-13b-5 | ARRAY-NULL-1 matches Spark on element coercion, measured first — applied |
| Q-13b-6 | order ARRAY-NULL-1 → gates → merge → ANSI-DOOR-1 — applied (ANSI-DOOR-1 not reached) |
| Q-13b-7 | critics get oracle answers as fixtures — applied (three oracle passes by the orchestrator) |
| Q-13b-8 | Grok reviewer units at 64 G with in-process `RLIMIT_AS` — applied to every review round |
| Q-R13-1..10 | recorded in the FACADE-4 ledger as the unification targets; binary checked on the oracle (`binary` on schema, dtypes, DESCRIBE); uint32 → bigint |
| Q-R13-11 | already applied by run 13 |
| Q-R13-12 | FACADE-5 step 1 (not this run) |
| Q-R13-13 | no new refusal; card DDL-DEPTH-1 filed with this report |
| Q-R13-14 | accepted only while every end-to-end cell stays within ±5 % of main on a release build — it holds; numbers in the FACADE-4 ledger and §3 |

## 5. Decisions under §6 (G-2)

- R14b-D-1 FACADE-4 step 1 stays the byte-identical consolidation (R13-D-5); Q-R13-1..10 recorded in the ledger as binding targets for the unification slices after step 1 (rulings block /tmp/oc-worker/r14b/f4-rulings.md). Reason: the unit's reviewed scope; Q-R13-7 itself asks for its own slice; the owed list for #582 names no unification.
- R14b-D-2 (G-2) FACADE-4 Q1 → option (a): per-class literal token methods in types.py (base's shape); types.py exact baseline may rise from the branch's 1610 to the new count because it stays below main's 1834 ("ceilings only move down" is measured against main). Python-only round beside ARRAY's build lane (R13b-D-6 precedent).
- R14b-D-3 ARRAY follow-up 4 queued behind FACADE-4's build round (one build lane); launched automatically 16:44.
- R14b-D-4 FACADE-4 re-checks run sequentially in the single review lane on bed /tmp/f-rev4s1r (no build: the lane's .so matches d82c5cc6's Rust): S2-21 Rust (17:29) → S2-21 Python → critic-logic re-check.
- R14b-D-5 P2-DICT-OUT is not sent back before merge: owner ruling Q-R13-14 (applied as recommended) says the reviewers' remaining P2s go into the next round rather than blocking, with the ±5 % limit, which holds. Filed as a ledger residue row and a follow-up item in the report.
- R14b-D-6 ARRAY needs a third critic-logic and S2-21 round (execution-time conversion is new per-batch product code). Bed /tmp/g-critarr3; the lane's .so predated the final clippy refactor, so the bed native is rebuilt from f28c7dee after the FACADE-4 gates release the build lane; the ARRAY review queue starts only after that rebuild and the FACADE-4 review queue.
- R14b-D-7 L-007..L-009 go back to the actor before merge (P1). Python-only follow-up 4 beside the ARRAY bed rebuild; the dtypes bar must still hold; the gates re-run after it.
- R14b-D-8 L-12 → Float16 participates as Spark FLOAT (float32) on the ladder (float16+float16 unchanged); L-13 pinned as the product rule (always case-insensitive, the oracle default) with a residue row; L-14 residue. ARRAY follow-up 5 launched ~19:26 (small Rust change; the running S2-21 round 3 on the bed waits for idle builders).
- R14b-D-9 ARRAY perf P1-1/P2-1/P2-2 back to the actor (follow-up 6) with numeric bars: all-null ≤ 0.90 ms and no widening copy; localization within 3× of base's date CAST with exact DST-day answers pinned; ns→µs within 3× of the µs kernel.
- R14b-D-10 No third S2-21 round on FACADE-4: the follow-up-4 change is correctness-only Python and the actor's two release-native ABAB passes keep every surface cell inside the bar; the orchestrator spot-checks dtypes/df.schema ABAB at gate time. Critic-logic round 3 on bed /tmp/f-crit4s1c (097306e9) launched ~19:57.
- R14b-D-11 L-010/L-011 back to the actor (follow-up 5, Python only). To bound the review loop before the 23:30 stop, no fourth critic round: the fix is verified by re-running the critic's own probe_r3/compare_r3 against base (0 differing keys required), by the actor and re-run by the orchestrator.
- R14b-D-12 No fourth ARRAY review round: the follow-up-6 change is perf-only with numeric bars; the orchestrator re-measures the bar cells on the rebased release native and runs its own multi-batch sliced-input differential on the window-slicing code.
- R14b-D-13 ARRAY ships right after its gates: a map.md-only rebase onto main 7693ef23 (FACADE-4's merge), push, undraft and the merge chain, with CI as the gate of record on the combined tree (a second local gate pass does not fit before 23:30); the orchestrator's perf verification runs during CI and the merge unit is stopped if a bar fails.
- R14b-D-14 ARRAY-NULL-1 parks at the G-3 stop as a ready PR (not draft): local gates green on 50628e13, the orchestrator's bar verification and multi-batch differential done, CI running on the updated head; the merge chain was stopped at 23:21 so no automatic squash lands after 23:30. The owner merges when CI is green.

## 6. Incidents and harness lessons

- INC-R14b-1 `make preflight` runs `make develop` (a DEBUG build) over a lane's release native. FACADE-4's actor found a
  663 MB debug `.so` at round start and restored the release build by copy. Rule: after any preflight, rebuild or restore
  the release native before a measurement; gate scripts now run the release build after preflight.
- INC-R14b-2 `run_baseline.py`'s idle wait uses `pgrep -e`, which errors on this box and reads as idle. Reviewers and the
  actor used `pgrep -x`. Fix card-worthy (one line).
- INC-R14b-3 #581 and #582 went `DIRTY` three times as run 14 merged (#587 fork repin + v1.4.1, #589, #590, a roadmap
  doc). Every rebase needed `map.md` unions; #590 also touched `lib.rs`, the Rust size ratchet and the CAP-1 mirror, which
  merged textually and were checked by hand. Each rebase over Rust changes forced a full gate re-run.
- INC-R14b-4 A round-4 body-hash baseline in `test_production_file_size.py` froze a mid-edit value; the local focused
  gates passed on a later edit, CI's wheels job caught it on `097306e9`, and follow-up 5 corrected it.
- Grok critics found real byte-identity breaks in three successive rounds on FACADE-4, each narrower than the last
  (subclass overrides → multiple inheritance → 20 of 600 pairs). Re-running the critic's own probe after the last fix
  bounded the loop without another round.
- A foreground wait that sleeps before its loop overruns the tool timeout; keep the whole call under the limit.

## 7. Owner questions — recommendations

- **Q-R14b-1 (FACADE-4 outbound conversions):** `repark_type_to_arrow` and `toDDL` still build a Python dict per node
  and re-parse it in Rust (2–7× base, all under 1 ms; no bar breach). **Recommend** a small follow-up slice that emits
  the same tagged tuple outbound as inbound, measured against the ±5 % bar, before the unification slices.
- **Q-R14b-2 (FACADE-4 unification order):** **Recommend** Q-R13-9 (nested nullability) and Q-R13-2/3 (binary, float32
  per surface) first — silent loss and a known Spark divergence — then Q-R13-4/5/6, and Q-R13-7 (the CSV lattice) last as
  its own slice.
- **Q-R14b-3 (ARRAY-NULL-1 error class):** the Spark class travels in the message text because a typed
  `AnalysisException` subclass needs `crates/repark-core/src/error_map.rs` (run 14's file tonight). **Recommend** a
  one-row follow-up after run 14 releases the file.
- **Q-R14b-4 (`spark.sql.caseSensitive`):** struct field matching in `array_append` ignores the conf. **Recommend**
  keeping it pinned as the product rule until a caseSensitive-wide card exists.
- **Q-R14b-5 (review-loop bound):** three critic rounds on one PR were needed tonight. **Recommend** a standing rule:
  after the second remediation round, the orchestrator re-runs the last critic's own probes instead of a new round, and
  a new round only when product code outside the findings' files changed.
- **Q-R14b-7 (#581, parked at the stop):** local gates are green on `50628e13`, every S2-21 bar was re-measured by the
  orchestrator on a consistent release native, CI went green on `087074ce`, and main moved during the final CI run.
  **Recommend** `gh pr update-branch 581`, `gh pr checks 581 --watch`, then the squash with `--match-head-commit` and the
  tree check; no code change is outstanding.
- **Q-R14b-6 (ANSI-DOOR-1):** not started; the brief and lane are staged. **Recommend** it opens the next expressions run.

## Pointers

- Up: [map.md](map.md) · Previous: [overnight-report-2026-09-14-run13b.md](overnight-report-2026-09-14-run13b.md),
  [overnight-report-2026-09-14-run13.md](overnight-report-2026-09-14-run13.md)
- Cards: [array-null-1-card-2026-09-13.md](array-null-1-card-2026-09-13.md),
  [ansi-door-1-card-2026-09-13.md](ansi-door-1-card-2026-09-13.md),
  [ddl-depth-1-card-2026-09-14.md](ddl-depth-1-card-2026-09-14.md)
