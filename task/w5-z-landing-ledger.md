# Unit ledger — W-5 Z-wave §6 landing increment

**Unit:** W-5 · **Date:** 2026-08-13 · **Lane:** repark · **Executor:** Grok ·
**Worktree:** `/tmp/grok-w5` · **Branch:** `grok/w5-z-landing` ·
**Base (frozen):** `c7e6589088111ded62848751a30a45adfea0973a`
(`fix(tz4): LTZ instant producers emit µs+UTC; TIMESTAMP → timestamptz (#79)`)

**Charter:** workspace brief `BRIEF-w5-z-landing.md` + conductor-6 Addendum A1–A8
(not in this repo). SEPMO STANDARD, acc + `claims_critic=true`. Prime directive:
verify-before-paste. Z-5 mold, one wave later.

**One-time grant (expires at wave end):** `docs/spark-sql-iceberg-parity.md` + ONE
dated `STATUS.md` note (Z wave landed ×5 + W wave in flight) and `_Last updated`.
**A1 carve-out:** TZ-6 and TZ-7 sections are W-1's — not touched (no in-flight
footnotes, no invented `live-mirror` tokens). `_live_parity.py` and
`test_parity_live.py` are CLOSED (W-1).

---

## A. §6 sweep — classification table (completeness proof)

Every named Z-wave §6 handoff enumerated. Disposition is one of **LANDED** /
**ALREADY-LANDED** / **SUPERSEDED** / **DEFERRED**. Re-verified on THIS base
(`c7e6589` / `#79` tip), not on the Z-wave freeze (`9b2dce3`).

| Ledger | Handoff item | Class | Action / cite |
|---|---|---|---|
| `z1-g3e8-pr1` | G3-E8 progress footnote (uncorrelated IN both doors; family **not** fixed; 16 spellings stay refused) | **LANDED** | live BACKLOG row updated; `delete_in_subquery` is content; residual family named; row kept |
| `z1-g3e8-pr1` | G3-E8-NULL keep | **ALREADY-LANDED** | live row kept; `NOT IN` + NULL still refused |
| `z2-tz4-pr1` | TZ-4 progress row, **not** retired | **LANDED** | dated progress note + repark half; residues named; heading/anchor kept |
| `z2-tz4-pr1` | A11 ANSI `timestamp_ns` reject | **LANDED** | cited as TZ-4 residue; pin `session_wiring.rs::ansi_column_def_timestamp_still_rejects_ns_on_v2` |
| `z2-tz4-pr1` | TZ-6 / TZ-7 retire | **DEFERRED** | A1: W-1 owns those two sections; byte-identical (sha256 match); no footnotes; no invented live-mirrors |
| `z2-tz4-pr1` | B-TZ-4 until PR-3 | **DEFERRED** | still queued, not a row |
| `z2-tz4-pr1` | `tz_aware_to_naive_round_trip` flip to equality | **SUPERSEDED** | Z-2 §0b claimed a type flip; on this base the row is still a disclosure (`timestamp[ns]`, zoneless CAST-str + B-TZ-4). Not pasted as equality |
| `z3-dec-u1u2` | DEC-4 / campaign DEC-5 `avg` → FIXED | **LANDED** | dated FIXED note; live BACKLOG row retired; corpus equality (`repark is None`); name kept |
| `z3-dec-u1u2` | DEC-1 still BACKLOG | **LANDED** | live row kept; dated still-OPEN note (U2 deferred). W-2 in flight lives in STATUS, not the registry rationale |
| `z3-dec-u1u2` | TY-3 still DECLARED | **LANDED** | dated 2026-08-13 note: U2 did not land; revisit rides with the U2 unit |
| `z4-residuals` | G4b D6 residual SAF-001 `F.abs` | **LANDED** | dated extension of the G4b D6 FIXED note (#77); no new live row |
| `z4-residuals` | G5b-R1 still OPEN | **LANDED** | live row kept; rationale = Z-4 recon (`spark_ast.rs` pre-plan quote). Still OPEN; W-4 in STATUS |
| `z4-residuals` | G5b-R4 still OPEN | **LANDED** | live row kept; rationale = Z-4 recon (DF 54.1.0 120 vs Spark 90). Still OPEN; W-4 in STATUS |
| `z4-residuals` | G5b-R5 still OPEN + Spark-half recon | **LANDED** | Spark half amended to Z-4 numeric-`n` RANGE recon (read-only on this base). Still OPEN. JVM lock **never taken** |
| `z4-residuals` | G13 / F-Y10-1 / G5b-R3-ANSI text | **ALREADY-LANDED** | Z-5 already routed F-Y10-1; R3-ANSI already named inside the G5b-R3 FIXED note |
| `z5` (prior increment) | G5b-R5 Spark half "self only" | **SUPERSEDED** | contradicted by Z-4 live recon now in the corpus comment on this base |
| `w5-z-landing` | this table | **LANDED** | this file |

**Counts:** LANDED 11 · ALREADY-LANDED 2 · SUPERSEDED 2 · DEFERRED 2 · table rows 17.

**Live-mirror both-halves:** **none.** No §6 handoff demanded a new `live-mirror:`
token. TZ-6 / TZ-7 carry none; A1 forbids inventing any. Exact-set size stays **14**.

### Verify-before-paste notes

- **Base moved.** Z-wave handoffs were written on `9b2dce3` (#72). This tree is
  `c7e6589` (#79 tip, `#75`–`#79` + `#73` on `main`). Pins were re-read here.
- **G3-E8 is a footnote, not a family close.** `delete_in_subquery` is
  `kind="content"`; `g3e8_delete_in_subquery_*` execute; ANSI
  `dml_subquery_in_delete_executes_and_deletes_exactly_the_match` executes; ROW 9 is
  restated over NOT IN / EXISTS / UPDATE IN. Residual refuse pins still hold.
- **TZ-4 is progress, not retirement.** Instant producers + Spark-door
  `timestamptz` match the pins. `tz_aware_to_naive_round_trip` is **still** a
  disclosure on this base (Z-2 §0b over-claimed). A11 ns-reject pin is present.
- **DEC-4 ≠ campaign DEC-5 naming.** Registry DEC-4 is `avg`; campaign DEC-5 is
  `avg`. Registry DEC-5 (`INT * DECIMAL`) stays BACKLOG. Pasted the FIXED note on
  registry DEC-4.
- **DEC-1 / TY-3.** Bare `1.23` is still `float64`. TY-3 pin still asserts double /
  nullable vs Spark `(11,1)` non-null. U2 did not land.
- **G5b-R5 resolved read-only.** Corpus comment +
  `temporal_range_interval_bound_over_int_key_still_arrow_cast` on this base already
  carry Z-4's live recon (numeric `n` RANGE, unit ignored; unique-key seed still
  `[10,20,30,40,50]`). W-5 did **not** take `/tmp/grok-jvm-record.lock` (held by
  `MARKER=w1-blast` / W-1; left untouched).
- **In-flight lane names stay out of the registry rationale** (Z-5 C1 Q-003).
  W-1 / W-2 / W-3 / W-4 are named in STATUS + this ledger only.
- **`_live_parity.py` / `test_parity_live.py` untouched.** W-1 owns them tonight.

---

## B. Other landings

- **STATUS.md** `_Last updated: 2026-08-13` (already current) + ONE dated note
  (Z wave landed ×5 + W wave in flight) under Current milestone. Known-issues
  restated for what Z-wave actually closed: G3-E8 IN-DELETE is no longer "all
  spellings refused"; DEC-4 avg is no longer "photographed, not fixed"; TZ-4
  PR-1 progress. **Not** claimed closed: TZ-6 / TZ-7 (W-1); R1 / R4 / R5 (W-4).
  The 2026-08-12 H-2 seed+tail snapshot and the 2026-08-13 Y-wave note are left
  as history (Z-5 mold).
- **No** `_live_parity.py`, **no** `test_parity_live.py`, **no** engine / corpus
  edits, **no** `briefs/`, **no** lockfiles, **no** W-1 TZ-6/TZ-7 section edits.

---

## C. Gate evidence

Recorded as `cmd > /tmp/w5-<gate>.log 2>&1; echo $?`.

| Gate | Log | Exit |
|---|---|---|
| `make verify` | `/tmp/w5-verify.log` | **0** |
| `make preflight` | `/tmp/w5-preflight.log` | **0** (facade **2922 passed**, 71 skipped; cargo deny / pip-audit / zizmor "No findings to report") |
| JVM lock | n/a | **never taken** (W-1 holds `MARKER=w1-blast`; not ours; not removed) |

---

## D. Authorship

Per-command `git -c user.name=TRO-Wolf -c user.email=64240326+TRO-Wolf@users.noreply.github.com`.
Trailer `Authored-By: Grok (grok-4.5) <noreply@x.ai>`. After every commit:
`git log -1 --format='%ae'` must equal that email byte-exact.

---

## Critic remediations (cycle 1)

| ID | Sev | Disposition |
|---|---|---|
| C1 / C4 in-flight lane names in registry | S2 | **REMEDIATED pre-gate** — W-1/W-2/W-3/W-4 live only in STATUS + this ledger (Z-5 Q-003 mold) |
| C4 TZ-6/TZ-7 | S1 | **CLEAN** — sha256 of both sections matches the pre-edit snapshot; 16 registry hunks, none overlap original :701–:762 |
| C4 G5b-R5 | S1 | **CLEAN** — Spark half follows Z-4 recon on this base; lock never taken |
| C4 family-fixed / TZ retire / R-closed | S1 | **CLEAN** — none of those claims made |
| C2 | — | **CLEAN** (docs-only; no secrets; JVM lock never taken) |

## E. Out of scope (honored)

W-1 TZ-6/TZ-7 sections; `_live_parity.py`; `test_parity_live.py`; live-tier
both-halves; `briefs/`; engine code; new corpus rows; lockfiles; `.github/`;
`docs/design/`; AWS; merges; JVM lock.
