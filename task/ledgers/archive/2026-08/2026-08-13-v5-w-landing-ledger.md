# Unit ledger — V-5 W-wave §6 landing increment

**Unit:** V-5 · **Date:** 2026-08-13 · **Lane:** repark · **Executor:** Grok ·
**Worktree:** `/tmp/grok-v5` · **Branch:** `grok/v5-w-landing` ·
**Base (frozen):** `8d325d4f47f46154bd954dc515d717434517fca5`
(`fix(tz4): localize zoneless LTZ inputs; distinguish TIMESTAMP_NTZ (#85)`)

**Charter:** workspace brief `BRIEF-v5-w-landing.md` + conductor-7 Addendum A1–A8
(not in this repo). SEPMO STANDARD, acc + `claims_critic=true`. Prime directive:
verify-before-paste. W-5 mold, one wave later.

**One-time grant (expires at wave end):** `docs/spark-sql-iceberg-parity.md` + ONE
dated `STATUS.md` note (W wave landed ×5 + V wave in flight + 48-hour push context)
and `_Last updated`. Sole registry writer tonight. `_live_parity.py` and
`test_parity_live.py` are CLOSED (V-3).

---

## A. §6 sweep — classification table (completeness proof)

Every named W-wave §6 / §5b handoff enumerated, plus stale in-flight wordings whose
PRs merged today (`#82`–`#85`). Disposition is one of **LANDED** /
**ALREADY-LANDED** / **SUPERSEDED** / **DEFERRED**. Re-verified on THIS base
(`8d325d4` / `#85` tip), not on the W-wave freeze (`c7e6589`).

| Ledger | Handoff item | Class | Action / cite |
|---|---|---|---|
| `w1-tz4-pr2` | TZ-4 progress row → PR-2 landed | **LANDED** | dated progress note `#79`+`#85`; heading kept; residues B-TZ-4 + A11 |
| `w1-tz4-pr2` | TZ-6 FIXED note | **ALREADY-LANDED** | in-file from `#85`; not duplicated; pins `timestamp_ntz_is_indistinguishable_from_timestamp` (equality) + `a_naive_ntz_timestamp_is_not_shifted_by_the_session_zone` |
| `w1-tz4-pr2` | TZ-7 FIXED note | **ALREADY-LANDED** | in-file from `#85`; not duplicated; pins zoneless / naive-column equalities + `a_zoneless_timestamp_input_localizes_in_the_session_zone` |
| `w1-tz4-pr2` | A11 ANSI `timestamp_ns` reject | **LANDED** | cited as TZ-4 residue; pin `session_wiring.rs::ansi_column_def_timestamp_still_rejects_ns_on_v2` still reds ns+v3 |
| `w1-tz4-pr2` | B-TZ-4 string-cast | **DEFERRED** | still queued, not a row; V-3 |
| `w1-tz4-pr2` | `tz_aware_to_naive_round_trip` flip to equality | **LANDED** | on this base the row is a content equality (`timestamp[us, tz=UTC]`); W-5's SUPERSEDED ("still `timestamp[ns]`") is stale |
| `w2-dec-u2` | DEC-1 → dated FIXED | **LANDED** | live BACKLOG row retired; corpus equalities (`repark is None`); names kept |
| `w2-dec-u2` | TY-3 stays DECLARED | **LANDED** | dated 2026-08-13: U2 landed; repark half updated to `(21,1)` nullable vs Spark `(11,1)` non-null; U3 **not** pre-claimed |
| `w2-dec-u2` | DEC-6 wrap-not-residue | **LANDED** | live BACKLOG kept; repark half updated (`10^38` at `(38,0)`); raise is U5 |
| `w3-g3e8-pr2` §5b | G3-E8 progress (NOT IN + NULL 3VL both doors) | **LANDED** | live BACKLOG row updated, **not** deleted; IN + NOT IN execute; EXISTS / UPDATE stay refused |
| `w3-g3e8-pr2` §5b | G3-E8-NULL keep + flip DELETE half | **LANDED** | DELETE half "matches Spark"; UPDATE half still refused; row kept |
| `w3-g3e8-pr2` §5b | EXISTS / dbt gate | **DEFERRED** | V-1; gate NOT met (IN + NOT IN execute; EXISTS still refused) |
| `w4-z-residuals` | G5b-R1 → FIXED | **LANDED** | dated FIXED note; live BACKLOG row retired; unquoted interval equality `[10,30,60,90,90]` |
| `w4-z-residuals` | G5b-R5 → FIXED | **LANDED** | dated FIXED note with numeric-`n` wording (not Y-1 self-group); live row retired |
| `w4-z-residuals` | G5b-R4 recon stays OPEN | **LANDED** | live BACKLOG kept; W-4 re-verify (DF 54.1 still 120) |
| `w4-z-residuals` | Q-002 aggregate-origin FIXED | **LANDED** | dated extension of the G4b D6 FIXED note (`#82`); no new live row |
| `w4-z-residuals` | G5b-R3-ANSI named residual | **ALREADY-LANDED** | already named inside the G5b-R3 FIXED note |
| `w5-z-landing` | TZ-4 "TZ-6/TZ-7 retire only with PR-2" / "flip not true" | **SUPERSEDED** | PR-2 landed; flip is true on this base |
| `w5-z-landing` | DEC-1 "Still OPEN / Do not mark FIXED" | **SUPERSEDED** | `#84` closed DEC-1 |
| `w5-z-landing` | TY-3 "U2 did not land" | **SUPERSEDED** | U2 landed; declaration kept on the new photograph |
| `w5-z-landing` | G5b header "R1 / R4 / R5 stay OPEN" | **SUPERSEDED** | R1/R5 closed in `#82`; R4 stays OPEN |
| `w5-z-landing` | G3-E8 "NOT IN remains refused / next FIX cut" | **SUPERSEDED** | `#83` shipped NOT IN + NULL 3VL |
| `w5-z-landing` | G4b D6 "aggregate builders named residual Q-002" | **SUPERSEDED** | `#82` closed Q-002 |
| `w5` increment note | TZ-6/TZ-7 "not touched" history | **ALREADY-LANDED** | left as history; V-5 increment note appended |
| STATUS / `task/map.md` | W-wave "in flight" + W-2/W-4 in-flight map lines | **LANDED** | one new STATUS dated note + known-issues restated; map lockstep |
| `v5-w-landing` | this table | **LANDED** | this file |

**Counts:** LANDED 14 · ALREADY-LANDED 4 · SUPERSEDED 6 · DEFERRED 2 · table rows 26.

**Live-mirror both-halves:** **none.** No §6 handoff demanded a new `live-mirror:`
token. Exact-set size stays **14**. `_live_parity.py` untouched.

### Verify-before-paste notes

- **Base moved.** W-wave handoffs were written on `c7e6589` (`#79`). This tree is
  `8d325d4` (`#85` tip, `#81`–`#85` + `#80` on `main`). Pins were re-read here and
  re-run (Rust) — see §C.
- **TZ-6 / TZ-7 already FIXED in-file.** `#85` wrote the dated notes. V-5 verified
  the headings and the equality pins; it did **not** duplicate the notes.
- **`tz_aware_to_naive_round_trip` is equality on this base.** W-5 correctly called
  Z-2's flip SUPERSEDED on `c7e6589`. `#85` flipped the row; the pin list now
  includes it as a progress equality.
- **A11 is still a residue.** `ansi_column_def_timestamp_still_rejects_ns_on_v2`
  still asserts `timestamp_ns` + `v3`. `create_table.rs` was CLOSED to W-1.
- **DEC-1 ≠ TY-3.** Bare `SELECT 1.23` is `decimal128(3,2)` (FIXED). Inline-VALUES
  union is still `(21,1)` nullable vs Spark `(11,1)` non-null (DECLARED). U3 is
  **not** claimed.
- **DEC-6 photograph changed.** U2 removed the float-residue; the live row now
  pins wrap-to-`10^38` at `(38,0)`. Still BACKLOG (raise is U5).
- **G3-E8 is a footnote, not a family close.** `delete_not_in_subquery` and
  `delete_not_in_subquery_with_null_key` are `kind="content"`; EXISTS / UPDATE
  stay `kind="split"`. Row kept. dbt gate **not** met.
- **G5b-R5 wording is numeric-`n`, not Y-1 self-group.** Unique-key
  `[10,20,30,40,50]` is `n=1` on gaps of 10; magnitude pin distinguishes.
- **In-flight lane names stay out of the registry rationale** (Z-5 / W-5 C1
  Q-003). V-1 / V-2 / V-3 live only in STATUS + this ledger.
- **`_live_parity.py` / `test_parity_live.py` untouched.** V-3 owns them tonight.
- **JVM lock never taken.** V-5 is forbidden to take `/tmp/grok-jvm-record.lock`.

---

## B. Other landings

- **STATUS.md** `_Last updated: 2026-08-13` (already current) + ONE dated note
  (W wave landed ×5 + V wave in flight + 48-hour push) under Current milestone.
  Known-issues restated for what W-wave actually closed: TZ-6 / TZ-7 FIXED;
  DEC-1 FIXED; G5b-R1 / R5 FIXED; G3-E8 NOT IN + NULL 3VL execute; Q-002
  closed. **Not** claimed closed: B-TZ-4; A11 `timestamp_ns`; EXISTS / dbt
  gate; TY-3 (still DECLARED); G5b-R4; DEC-2/3/5–9. The 2026-08-13 Y-wave and
  Z-wave notes are left as history (W-5 mold).
- **No** `_live_parity.py`, **no** `test_parity_live.py`, **no** engine / corpus
  edits, **no** `briefs/`, **no** lockfiles, **no** TZ-6/TZ-7 FIXED-note rewrite.

---

## C. Gate evidence

Recorded as `cmd > /tmp/v5-<gate>.log 2>&1; echo $?`.

| Gate | Log | Exit |
|---|---|---|
| targeted rust (DEC-1 / DEC-6 / R1 / R5 / R4 / NOT IN / TZ-6 / TZ-7 / A11 / ROW 9) | this ledger §C.1 | **0** |
| `make verify` | `/tmp/v5-verify.log` | **0** |
| `make preflight` | `/tmp/v5-preflight.log` | **0** (facade **2980 passed**, 71 skipped; cargo deny / pip-audit / zizmor "No findings to report") |
| JVM lock | n/a | **never taken** |

### C.1 Targeted re-verify on `8d325d4` (before paste)

All `cargo test` invocations cd-fused in `/tmp/grok-v5`. Real exit 0.

- `repark-spark --lib`: `configure_defaults_parse_float_as_decimal`,
  `configure_makes_bare_1_23_decimal128_3_2`,
  `pin_literal_1_23_infers_decimal128_3_2_i128`,
  `pin_overflow_max_decimal38_plus_one_wrong_value_i128`,
  `temporal_range_unquoted_interval_literal_matches_quoted`,
  `temporal_range_interval_bound_over_int_key_is_numeric_n`,
  `temporal_range_following_to_following_still_includes_current_row`,
  `g3e8_delete_not_in_subquery_deletes_non_matching_rows`,
  `g3e8_delete_not_in_subquery_with_null_key_deletes_nothing`,
  `g3e8_delete_not_in_empty_subquery_deletes_every_row` — **10 passed**.
- `repark-spark --test session_timezone`:
  `a_zoneless_timestamp_input_localizes_in_the_session_zone`,
  `a_naive_ntz_timestamp_is_not_shifted_by_the_session_zone` — **2 passed**.
- `repark-sql --test session_wiring` `ansi_column_def_timestamp_still_rejects_ns_on_v2`;
  `--test cross_door` `cross_door_g3e8_refusals_render_identically` +
  `cross_door_g3e8_not_in_delete_executes_identically`; `--lib`
  `dml_subquery_not_in_delete_executes_and_honors_three_valued_logic` — **4 passed**.
- Corpus source (no JVM): DEC-1 three literal rows `repark is None`; TY-3 pin
  asserts `decimal128(21,1)` nullable vs Spark `(11,1)` non-null; R1/R5 window
  rows equality; R4 disclosure `120` vs Spark `90`; `delete_not_in_*` `kind="content"`;
  `delete_exists_correlated` still `kind="split"`; `tz_aware_to_naive_round_trip`
  `repark is None` at `timestamp[us, tz=UTC]`.
- `live-mirror:` exact-set size **14** (no tokens added or removed).

---

## D. Authorship

Per-command `git -c user.name=TRO-Wolf -c user.email=64240326+TRO-Wolf@users.noreply.github.com`.
Trailer `Authored-By: Grok (grok-4.5) <noreply@x.ai>`. After every commit:
`git log -1 --format='%ae'` must equal that email byte-exact.

---

## Critic remediations (cycle 1)

Sequential hats (spawn unavailable; independence weaker than separate agents).
**Context break executed; attacking artifacts, not memory.**

| ID | Sev | Disposition |
|---|---|---|
| C1 / C4 in-flight lane names in registry | S2 | **REMEDIATED pre-gate** — V-1/V-2/V-3 live only in STATUS + this ledger (Z-5 / W-5 Q-003 mold) |
| C4 TZ-6/TZ-7 duplication | S1 | **CLEAN** — FIXED notes left byte-as-#85; V-5 did not rewrite those headings |
| C4 family-fixed / TZ retire / R4-closed / dbt-gate / U3 | S1 | **CLEAN** — none of those claims made |
| C4 DEC-1 `1.0` trailing-zero claim | S2 | **REMEDIATED** — dropped; no pin on this base |
| C4 table arithmetic | S2 | **REMEDIATED** — 14 / 4 / 6 / 2 of 26 |
| C4 live-mirror invent | S1 | **CLEAN** — exact-set stays 14 |
| C2 | — | **CLEAN** (docs-only; no secrets; JVM lock never taken) |

**Convergence:** `ACC-CONVERGED` (C1+C2+C4; verify 0, preflight 0). CL-IDENTITY checked after commit.

## E. Out of scope (honored)

`_live_parity.py`; `test_parity_live.py`; live-tier both-halves; `briefs/`;
engine code; new corpus rows; lockfiles; `.github/`; `docs/design/`; AWS;
merges; JVM lock; TZ-6/TZ-7 FIXED-note rewrite; U3 / EXISTS / B-TZ-4 claims.
