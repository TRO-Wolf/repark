# Unit ledger — R-3 / G8 absence pins

**Unit:** R-wave **R-3** — flip the four G8 `DeliberatelyAbsent` cells to `Tested` ·
**Date:** 2026-08-14 · **Lane:** R-3 · **Branch:** `grok/r3-g8-absences` ·
**Worktree:** `/tmp/grok-r3` · **Base (frozen):** `fddf1bc4840ade68274ca5c55993dda0fb182a61`
(`#94`) · **SEPMO:** STANDARD acc + C4 · **max_cycles:** 2 · **floor:** S1.

Charter: `BRIEF-r3-g8-absences.md` + conductor-9 A1–A11 +
`task/s2-g8-ledger.md` §6 (the four declared pin-absences ARE the charter).
Design: G11 (ANSI = correctness, not Spark parity). Surface vocabulary stays at 50 —
no new `SurfaceId`. `cross_door.rs` is R-1's (untouched). Registry / STATUS closed
(A4 — §6 paste-true only).

---

## 0. §0 recon (homes + liveness + lock)

### 0.1 Test homes (existing, no new crates)

| Cell | Door | Home | Why this home |
|---|---|---|---|
| `SEMANTICS_JOIN_NULL_KEYS` | Spark | **NEW** `crates/repark-spark/src/tests/join_null_keys.rs` | G-4 production-aligned leaf next to `float_agg.rs` / `window_temporal_range.rs`; wired from `tests/mod.rs`. Spark-parity goldens, live-reverified under the JVM lock. |
| `SEMANTICS_JOIN_NULL_KEYS` | ANSI | **NEW** `crates/repark-sql/tests/ansi_door_join_null_keys.rs` | Cargo integration-test sibling of `ansi_door_values.rs` (G11 mold). Native `AnsiDialect`, no extension. |
| `SEMANTICS_WINDOW_FRAMES` | ANSI | **NEW** `crates/repark-sql/tests/ansi_door_window_frames.rs` | Same G11 integration-test home. Native-profile ROWS/RANGE frame-value pins. Spark-door G5b twins stay untouched. |
| `SEMANTICS_FLOAT_DETERMINISM` | ANSI | **NEW** `crates/repark-sql/tests/ansi_door_float_agg.rs` | G7 rust twins (`crates/repark-spark/src/tests/float_agg.rs`) are the mold — ANSI-door twins, not a Spark rewrite. |

Not used: `crates/repark-sql/src/tests.rs` (end-to-end Iceberg DDL battery — wrong altitude
for value-semantics pins). Not touched: `crates/repark-sql/tests/cross_door.rs` (R-1).
No Python `repark.spark` imports. No G4 `test_join_parity.py` rewrite.

### 0.2 Probe (2026-08-14, native ANSI session, deleted `_r3_probe.rs`)

| Surface | Measured |
|---|---|
| INNER NULL keys | one row `(1, a, 1, x)` |
| LEFT NULL keys | `(1, a, 1, x)` + `(NULL, n, NULL, NULL)` |
| LEFT SEMI vs NULL-only right | empty |
| LEFT ANTI vs NULL-only right | both left rows kept |
| EXISTS / NOT EXISTS | equivalent to SEMI / ANTI |
| default / RANGE unbounded | `[70, 70, 100, 70, 150]` |
| ROWS unbounded | `[10, 30, 100, 70, 150]` |
| RANGE 1 PRECEDING numeric | `[70, 70, 100, 70, 80]` |
| ROWS sliding ±1 | `[30, 60, 90, 120, 90]` |
| DATE unit-less RANGE 1 PRECEDING | `[10, 30, 60]` (DF **months**) |
| DATE `INTERVAL '1' DAY` | `[10, 30, 30]` (portable days) |
| float sum/avg p=1,2,8 | G7 bits exactly (`3.75` / `2.25` / `0.46875` / `0.28125`) |

### 0.3 JVM lock

FIFO after R-2. At §0 open: `/tmp/grok-jvm-record.lock` held by
`MARKER=r1-record pid=2727182` then refreshed to `pid=2788473
ISO=2026-08-13T21:29:21-04:00 lane=r1-g3e8-pr4 step=probe-anyall-select`.
ANSI-door work proceeded without the lock. Spark-door JOIN live-record waits.
See §5 for acquire / rm events.

### 0.4 Freeze check

`git rev-parse HEAD` at start = `fddf1bc4840ade68274ca5c55993dda0fb182a61`. No fetch. No rebase.

---

## 1. What landed

| Artifact | Path | Role |
|---|---|---|
| Spark-door JOIN pin | `crates/repark-spark/src/tests/join_null_keys.rs` | INNER / LEFT / SEMI / ANTI NULL-key values |
| Spark module wire | `crates/repark-spark/src/tests/mod.rs` | `mod join_null_keys;` |
| ANSI JOIN pin | `crates/repark-sql/tests/ansi_door_join_null_keys.rs` | Native 3VL, same four join kinds |
| ANSI WINDOW pin | `crates/repark-sql/tests/ansi_door_window_frames.rs` | ROWS/RANGE + DATE unit-less months |
| ANSI FLOAT twins | `crates/repark-sql/tests/ansi_door_float_agg.rs` | G7 `f64::to_bits` × partitions 1/2/8 |
| Spark matrix flip | `crates/repark-spark/src/matrix.rs` | JOIN cell → Tested; absences 4→3; shipped 46→47 |
| ANSI matrix flip | `crates/repark-sql/src/matrix.rs` | three cells → Tested; absences 7→4; shipped 43→46 |
| Maps | spark `src/map.md` + `src/tests/map.md`; sql `map.md` + `src/map.md` + `tests/map.md`; `task/map.md` | lockstep |
| This ledger | `task/r3-g8-absences-ledger.md` | unit record |

---

## 2. Completeness — the four flipped cells

Cites are live `cargo test --locked --workspace --lib --tests --bins -- --list`
names (verify liveness: **93 Tested cites live**, `--list` 1555 names; was 89 / 1529).

| ID | Spark door | ANSI door |
|---|---|---|
| `SEMANTICS_JOIN_NULL_KEYS` | **Tested** `tests::join_null_keys::spark_door_null_keys_never_match_inner_left_semi_anti` (SparkExtended) | **Tested** `ansi_door_null_keys_never_match_inner_left_semi_anti` (Native) |
| `SEMANTICS_WINDOW_FRAMES` | Tested (unchanged) `tests::window_temporal_range::temporal_range_interval_bounds_still_match_spark` | **Tested** `ansi_door_rows_and_range_frame_values` (Native) |
| `SEMANTICS_FLOAT_DETERMINISM` | Tested (unchanged) `tests::float_agg::pin_sum_f64_bits_at_target_partitions_1` | **Tested** `ansi_door_sum_f64_bits_at_target_partitions_1` (Native) |

Absence-count pins:

| Door | Before (S-2) | After (R-3) |
|---|---|---|
| Spark Tested / Absent | 46 / 4 | **47 / 3** |
| ANSI Tested / Absent | 43 / 7 | **46 / 4** |

The three remaining Spark absences are the PR-6 structural set (sort order, unknown-key
refuse, wrong-door sniff). The four remaining ANSI absences are the standing rulings
(Q3 partitioning, Q9 overwrite, TRUNCATE, Q7 CALL). Vocabulary stays at 50 IDs.

### G11 honesty (ANSI)

- JOIN: standard-SQL 3VL. Spark 4.1.2 **agrees** (G4); documented in the test comment,
  not a parity claim.
- WINDOW: numeric ROWS/RANGE agree with G5; **unit-less** `RANGE 1 PRECEDING` over
  DATE is DF-native **months** (`[10, 30, 60]`). Spark 4.1.2 reads that spelling as
  **days** (`[10, 30, 30]`). Pinned as DF-native; not "fixed". `INTERVAL '1' DAY` is
  the portable spelling (`[10, 30, 30]`).
- FLOAT: same bits as G7 Spark-door twins on this fixture (stock DataFusion
  aggregation; no Spark analyzer). Cross-count spread p=8 ≠ p=1 disclosed, not fudged.

---

## 3. ACC

Actor → Critic-1 (quality) → Critic-2 (security). `max_cycles=2`, floor S1.
Claims via C4-equivalent honesty: every matrix cite is a live `--list` name.

### Actor

Implemented the four cells, flipped the matrices, updated maps, ran `make verify`.

### Critic-1 (quality)

| ID | Finding | Disposition |
|---|---|---|
| C1-1 | Four new Tested cites exist on `--list` (liveness 93 / 1555) | **HOLD** |
| C1-2 | Absence-count pins moved with the rows (47/3 and 46/4) | **HOLD** |
| C1-3 | ANSI WINDOW pins DF-native DATE months, documents Spark days | **HOLD** |
| C1-4 | String physical type is `Utf8View` on inline UNION ALL; tests accept the Utf8 family (value pin is 3VL, not string layout) | **HOLD** — named; not a silent type skip |
| C1-5 | Spark JOIN goldens copied from G4 and re-verified live under the lock | **HOLD** — live 4.1.2 21:52:44: inner `(1,a,1,x)`; left + orphan; semi empty; anti both left rows |

### Critic-2 (security)

| ID | Finding | Disposition |
|---|---|---|
| C2-1 | No AWS / IAM / Glue / S3 mutation | **HOLD** |
| C2-2 | No lockfile / `[patch]` / `.github` / STATUS / registry edit | **HOLD** |
| C2-3 | No `repark.spark` import; no new Python corpus | **HOLD** |
| C2-4 | JVM lock FIFO + RELEASE-ON-EXIT; no steal of a live r1/r2 marker | **HOLD** — events in §5 |
| C2-5 | Hygiene greps count 0 on the diff + log | **HOLD** after commit |

Cycle 1 → early_stop (zero open quality/security defects after live-record).
**ACC-CONVERGED.**

---

## 4. Gates

| Gate | EC | Note |
|---|---|---|
| `make check-matrix-test-liveness` (inside verify) | **0** | `93 Tested cites live (cargo --list 1555 names)` |
| `make verify` | **0** | cd-fused `/tmp/grok-r3` |
| `make preflight` | **0** | facade `3053 passed, 71 skipped`; audit + zizmor clean |
| Two-pass hygiene | *(after commit)* | |
| `%ae` | *(after commit)* | |
| JVM lock | §5 | |

---

## 5. Lock events

| Time (local) | Event |
|---|---|
| §0 open | lock present `MARKER=r1-record pid=2727182` then `pid=2788473 ISO=2026-08-13T21:29:21-04:00 lane=r1-g3e8-pr4 step=probe-anyall-select` |
| 21:34–21:46 | polled; pid **DEAD**; age 5→17 min; **no stale-rm** (age < ~30 min; R-1 worktree still dirty) |
| 21:49:12 | lock **FREE** (no r1/r2 live marker) |
| 21:49:27 | **ACQUIRED** `MARKER=r3-record pid=3025906 ISO=2026-08-13T21:49:27-04:00 lane=r3-g8-absences` |
| 21:52:33 | first acquire killed mid `uv run` (tried to build repark); leftover own marker, pid dead |
| 21:52:41 | **STALE-RM** own marker `pid=3025906`; **re-ACQUIRED** `pid=3065906` |
| 21:52:44 | live Spark 4.1.2 record of four JOIN SQLs — matches rust goldens; EC 0 |
| 21:52:44+ | **RELEASED** `rm /tmp/grok-jvm-record.lock` (own pid=3065906, trap EXIT) |

---

## 6. Handoff (registry / STATUS — paste-true; do not land)

Four G8 pin-absences are now **Tested** (this unit). Vocabulary stays at 50 IDs.
Do **not** pre-claim H-2 close beyond "G8 pin-absences filled." Residual from S-2
§6 item 5 (G6 cast-failure rust twin) is untouched.

| ID | Spark door | ANSI door |
|---|---|---|
| `SEMANTICS_JOIN_NULL_KEYS` | **Tested** `tests::join_null_keys::spark_door_null_keys_never_match_inner_left_semi_anti` | **Tested** `ansi_door_null_keys_never_match_inner_left_semi_anti` |
| `SEMANTICS_WINDOW_FRAMES` | Tested (unchanged; G5b) | **Tested** `ansi_door_rows_and_range_frame_values` |
| `SEMANTICS_FLOAT_DETERMINISM` | Tested (unchanged; G7) | **Tested** `ansi_door_sum_f64_bits_at_target_partitions_1` |
