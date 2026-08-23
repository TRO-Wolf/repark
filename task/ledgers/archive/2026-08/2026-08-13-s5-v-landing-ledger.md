# Unit ledger — S-5 V-wave §6 landing increment

**Unit:** S-5 · **Date:** 2026-08-14 · **Lane:** repark · **Executor:** Grok ·
**Worktree:** `/tmp/grok-s5` · **Branch:** `grok/s5-v-landing` ·
**Base (frozen):** `d9a739123be8b00bc1fc1e6d4bbad875ba6caa76`
(`feat(decimal): U3 fromLiteral min-precision + U4a SparkDecimalPrecision clamp (#91)`)

**Charter:** workspace brief `BRIEF-s5-v-landing.md` + conductor-8 +
`S-WAVE-KICKOFF.md`. SEPMO STANDARD, acc + `claims_critic=true`. Prime
directive: verify-before-paste. V-5 mold, one wave later.

**One-time grant (expires at wave end):** `docs/spark-sql-iceberg-parity.md` + ONE
dated `STATUS.md` note (V wave landed ×5 + S wave in flight + one sentence of
48-hour-push context) and `_Last updated`. Sole registry writer tonight.
`_live_parity.py` and `test_parity_live.py` are CLOSED.

---

## A. §6 sweep — classification table (completeness proof)

Every named V-wave §6 handoff from the four engine ledgers, plus stale
in-flight wordings whose PRs merged today (`#88`–`#91`; `#87` is the prior
landing increment). Disposition is one of **LANDED** / **ALREADY-LANDED** /
**SUPERSEDED** / **DEFERRED**. Re-verified on THIS base (`d9a7391` / `#91`
tip), not on the V-wave freeze (`8d325d4`).

| Ledger | Handoff item | Class | Action / cite |
|---|---|---|---|
| `v1-g3e8-pr3` | IN-DELETE (uncorrelated) | **ALREADY-LANDED** | PR-1 / `#78`; already in the G3-E8 footnote from V-5 |
| `v1-g3e8-pr3` | NOT IN-DELETE + NULL 3VL trap | **ALREADY-LANDED** | PR-2 / `#83`; already in the G3-E8 footnote from V-5 |
| `v1-g3e8-pr3` | EXISTS / NOT EXISTS ± correlation | **LANDED** | content rows + both-door execute pins; `#89` |
| `v1-g3e8-pr3` | UPDATE IN / NOT IN / EXISTS stay refused | **LANDED** | restated in the G3-E8 footnote; `g3e8_update_subquery_family_all_refuse` |
| `v1-g3e8-pr3` | correlated IN / ANY/ALL / scalars / rest of §5 | **LANDED** | restated; ROW 9 over correlated IN / UPDATE IN / nested / scalar |
| `v1-g3e8-pr3` | Registry G3-E8 kept BACKLOG (footnote) | **LANDED** | IN + NOT IN + `[NOT] EXISTS` ± correlation execute both doors; family **not** "fixed" |
| `v1-g3e8-pr3` | Registry G3-E8-NULL keep | **ALREADY-LANDED** | V-5 already flipped the DELETE half; UPDATE half still split |
| `v1-g3e8-pr3` | dbt-upgrade gate MET | **LANDED** | the gate line is true on this tree; family still not closed |
| `v2-dec-u3u4` | DEC-2 `/` still BACKLOG; U4b declared | **LANDED** | live BACKLOG kept; CAST-after wrongs the value |
| `v2-dec-u3u4` | DEC-3 add/sub/mul clamp → FIXED (U4a) | **LANDED** | dated FIXED note; live wrong-width row retired; names kept |
| `v2-dec-u3u4` | DEC-4 already FIXED | **ALREADY-LANDED** | Z-3 / `#76`; not rewritten |
| `v2-dec-u3u4` | DEC-5 width FIXED via U3 (campaign DEC-8); nullability BACKLOG | **LANDED** | both halves `(12,2)`; Spark nullable / repark non-null (DEC-9) |
| `v2-dec-u3u4` | Registry DEC-8 `(38,20)*(38,20)` still BACKLOG | **LANDED** | plan-refuse before any AnalyzerRule; ExprPlanner, not DEC-3 |
| `v2-dec-u3u4` | TY-3 stays DECLARED | **LANDED** | dated 2026-08-14: UNION `forType(INT)` recon; still `(21,1)` nullable |
| `v3-btz4` | B-TZ-4 string-cast → dated FIXED | **LANDED** | awaiting-pins bullet retired; 12 STRING equalities + Rust pins |
| `v3-btz4` | TZ-4 progress: string-cast landed | **LANDED** | heading kept; residue A11 `timestamp_ns` |
| `v3-btz4` | TZ-8 NY `2024-06-14` handoff | **LANDED** | existing TZ-8 row; **not** FIXED; V-3 evidence cited |
| `v4-partition-values` | F-V4-1 timestamptz meta projection | **LANDED** | DECLARED; fork-wave-routed; two pins |
| `v4-partition-values` | F-V4-2 `+00:00` vs `UTC` annotation | **LANDED** | DECLARED; fork-wave-routed; type rider |
| `v4-partition-values` | TZ-8 partition dates | **LANDED** | CROSS-CITE v3's TZ-8 row — one row, two citations, never two rows |
| brief / `#85` | TZ-6 FIXED note | **ALREADY-LANDED** | in-file from `#85`; not duplicated |
| brief / `#85` | TZ-7 FIXED note | **ALREADY-LANDED** | in-file from `#85`; not duplicated; stale "PR-3" pointer closed |
| `v5-w-landing` | increment note | **ALREADY-LANDED** | left as history; S-5 increment note appended |
| `v5-w-landing` | G3-E8 "EXISTS stay refused / dbt gate not met" | **SUPERSEDED** | `#89` shipped `[NOT] EXISTS` ± correlation; gate MET |
| `v5-w-landing` | TZ-4 "B-TZ-4 stays queued until PR-3" | **SUPERSEDED** | `#90` closed B-TZ-4 |
| `v5-w-landing` | DEC-3 live BACKLOG (`(38,20)` / `(38,18)`) | **SUPERSEDED** | `#91` U4a clamp |
| `v5-w-landing` | DEC-5 width `(31,2)` | **SUPERSEDED** | `#91` U3 → `(12,2)` |
| `v5-w-landing` | TY-3 "U3 not pre-claimed / residual campaign DEC-8" | **SUPERSEDED** | U3 landed; residual is UNION `forType(INT)` |
| `v5-w-landing` | "DEC-2/3/5–9 remain photographed, not fixed" | **SUPERSEDED** | DEC-3 FIXED; DEC-5 width FIXED; DEC-2/6/7/9 + registry DEC-8 stay |
| STATUS / `task/map.md` | V-wave "in flight" + this increment | **LANDED** | one new STATUS dated note + known-issues restated; map lockstep |
| `s5-v-landing` | this table | **LANDED** | this file |
| `v2-dec-u3u4` | U4b `/` Spark-formula implementation | **DEFERRED** | BACKLOG row landed; UDF not this increment |
| `v2-dec-u3u4` | Registry DEC-8 ExprPlanner close | **DEFERRED** | BACKLOG row landed; fix not this increment |
| residue / S-1 | DEC-6/7/9 + ANSI knob / U5 | **DEFERRED** | not pre-claimed |
| residue / S-wave | G8 / `repark.sql` re-home / dbt-repark upgrade | **DEFERRED** | not pre-claimed |

**Counts:** LANDED 18 · ALREADY-LANDED 7 · SUPERSEDED 6 · DEFERRED 4 · table rows 35.

**Live-mirror both-halves:** **none.** No §6 handoff demanded a new `live-mirror:`
token. Exact-set size stays **14**. `_live_parity.py` untouched.

### Binding paste truths (do not over-claim)

- **G3-E8 family footnote.** IN + NOT IN + `[NOT] EXISTS` ± correlation all
  EXECUTE both doors — that is the dbt-upgrade gate line. The family is still
  **not** "fixed" while UPDATE IN + correlated IN / ANY / ALL stay valved.
- **DEC-8 naming.** Campaign DEC-8 (U3 integer-literal `fromLiteral`) closed
  registry **DEC-5 width**. Registry **DEC-8** is `(38,20)*(38,20)` plan-refuse
  and stays BACKLOG. Do not write "DEC-8 FIXED" without saying which one.
- **DEC-2/3/4.** DEC-3 FIXED via U4a clamp. DEC-2 `/` EXCEPTED — U4b declared.
  DEC-4 already FIXED (`#76`); not rewritten.
- **TY-3** stays DECLARED with the UNION `forType(INT)` recon.
- **B-TZ-4** dated FIXED. **TZ-8** date-cast handoff with captured NY
  `2024-06-14` evidence is landed, **not** fixed. Registry class stays
  **BACKLOG** (intent to FIX). The brief's "DECLARED, not fixed" is the
  increment disposition (disclose the handoff; do not claim FIXED), not a
  flip of the row to permanent-DECLARED.
- **V-4** 4 diverge rows: 2× F-V4-1 + 2× TZ-8. F-V4-1/2 land as DECLARED
  fork-wave-routed findings. TZ-8 partition dates CROSS-CITE v3's row.
- **TZ-6 / TZ-7** already FIXED on main via `#85` / V-5 — not duplicated.
- **S-wave work** (ANSI knob, G8, re-home, dbt upgrade) is **not** pre-claimed.

### Verify-before-paste notes

- **Base moved.** V-wave handoffs were written on `8d325d4` (`#85`). This tree
  is `d9a7391` (`#91` tip, `#87`–`#91` + `#86` on `main`). Pins were re-read
  here and re-run (Rust) — see §C.
- **TZ-6 / TZ-7 already FIXED in-file.** `#85` wrote the dated notes. S-5
  verified the headings; it did **not** duplicate them. The one TZ-7 residual
  sentence that still said "B-TZ-4 … is PR-3" was closed to `#90` (a stale
  pointer, not a rewrite of the FIXED note).
- **EXISTS is content on this base.** `delete_exists_correlated` /
  `delete_not_exists_correlated` and the uncorrelated / none / all / empty /
  NULL-key / duplicate siblings are `kind="content"`. Residual splits:
  `delete_correlated_in_subquery`, `update_in_subquery`,
  `update_not_in_subquery_with_null_key`.
- **dbt gate MET ≠ family fixed.** Both doors execute IN + NOT IN (incl. NULL
  trap) + `[NOT] EXISTS` ± correlation. UPDATE IN + correlated IN / ANY / ALL
  stay valved. The dbt-repark **upgrade** is S-4 and is not claimed here.
- **DEC-3 ≠ registry DEC-8.** Clamp rows are equalities (`repark is None`) at
  `(38,6)` / `(38,17)` / `(38,9)`. `(38,20)*(38,20)` still refuses at
  `BinaryExpr::get_type` before any `AnalyzerRule`.
- **Campaign DEC-8 ≠ registry DEC-8.** U3 `fromLiteral` made `5 * DECIMAL(10,2)`
  `decimal128(12,2)` (width). Nullability still diverges (DEC-9).
- **TY-3 is still `(21,1)` nullable.** U3 does not apply to UNION set-op
  widening. Spark uses `forType(INT)=(10,0)` → `(11,1)` non-null. Applying
  `fromLiteral` would yield `(3,1)` — neither today nor Spark.
- **B-TZ-4 equalities.** NY `2024-06-15 08:00:00` space-separated `Utf8`;
  fraction `.123400` → `.1234`; NTZ stays 12:00 under NY.
- **TZ-8 is one row.** V-3 captured NY `CAST(to_timestamp('2024-06-15T03:00:00Z')
  AS DATE)` → Spark `2024-06-14` / repark `2024-06-15`. V-4 adds identity
  partition dates under NY: Spark `2023-12-31` / repark `2024-01-01`. Same
  class. Not FIXED. Registry class stays BACKLOG (intent to FIX) — see binding
  paste truths.
- **F-V4-1 / F-V4-2 are findings, not a repark fix.** Fork CLOSED. DECLARED
  so they are not silent skips.
- **In-flight lane names stay out of the registry rationale** (Z-5 / W-5 / V-5
  Q-003). S-1 / S-2 / S-3 / S-4 live only in STATUS + this ledger.
- **`_live_parity.py` / `test_parity_live.py` untouched.**
- **JVM lock never taken.** S-5 is forbidden to take `/tmp/grok-jvm-record.lock`.
  Observed absent at start of unit; left untouched.

---

## B. Other landings

- **STATUS.md** `_Last updated: 2026-08-14` + ONE dated note (V wave landed ×5
  + S wave in flight + 48-hour push) under Current milestone. Known-issues
  restated for what V-wave actually closed: `[NOT] EXISTS` ± correlation
  execute (dbt gate MET, family not fixed); B-TZ-4 FIXED; DEC-3 FIXED; DEC-5
  width FIXED; TY-3 still DECLARED with `forType(INT)`. **Not** claimed closed:
  UPDATE IN + correlated IN/ANY/ALL; TZ-8; DEC-2 `/` (U4b); registry DEC-8;
  DEC-6/7/9; F-V4-1/2 fork-wave; G5b-R4; G8; `repark.sql` re-home. The
  2026-08-13 Y / Z / W increment notes are left as history (V-5 mold).
- **No** `_live_parity.py`, **no** `test_parity_live.py`, **no** engine / corpus
  edits, **no** `briefs/`, **no** lockfiles, **no** TZ-6/TZ-7 FIXED-note rewrite.

---

## C. Gate evidence

Recorded as `cmd > /tmp/s5-<gate>.log 2>&1; echo $?`.

| Gate | Log | Exit |
|---|---|---|
| targeted rust (U3 / U4a / DEC-8 / B-TZ-4 / EXISTS / UPDATE refuse / ROW 9 / TZ-8 / identity) | this ledger §C.1 | **0** |
| `make verify` | `/tmp/s5-verify.log` | **0** |
| `make preflight` | `/tmp/s5-preflight.log` | **0** (facade **3045 passed**, 71 skipped; cargo deny / pip-audit / zizmor "No findings to report") |
| JVM lock | n/a | **never taken** |

### C.1 Targeted re-verify on `d9a7391` (before paste)

All `cargo test` invocations cd-fused in `/tmp/grok-s5`. Real exit 0.

- `repark-spark --lib`: `pin_int_times_decimal_is_12_2_i128`,
  `pin_mul_38_10_clamps_to_38_6_i128`, `pin_add_38_18_clamps_to_38_17_i128`,
  `pin_mul_38_20_still_refuses_at_plan`,
  `g3e8_delete_exists_uncorrelated_and_correlated_execute`,
  `g3e8_update_subquery_family_all_refuse`,
  `g3e8_delete_not_in_subquery_with_null_key_deletes_nothing` — **7 passed**.
- `repark-functions --lib`: `spark_timestamp_string_trims_trailing_fraction_zeros`,
  `ltz_renders_in_the_session_zone_and_ntz_does_not`,
  `timestamp_cast_to_string_is_spark_utf8`,
  `mul_38_20_still_refuses_before_any_analyzer_rule` — **4 passed**.
- `repark-sql --test cross_door`: `cross_door_g3e8_exists_delete_executes_identically`,
  `cross_door_g3e8_refusals_render_identically` — **2 passed**.
- `repark-sql --lib`: `dml_subquery_exists_delete_executes_uncorrelated_and_correlated` — **1 passed**.
- `repark-spark --test session_timezone`:
  `timestamp_to_date_paths_outside_this_crate_still_read_the_stored_zone` — **1 passed**
  (repark `2024-06-15`; comment records Spark `2024-06-14`).
- `repark-iceberg --lib`: `identity_select_exists_matches_spark_412_row_sets` — **1 passed**.
- Corpus source (no JVM): DEC-3 three clamp rows `repark is None` at Spark
  `(38,6)` / `(38,17)` / `(38,9)`; DEC-5 both halves `(12,2)` (nullability
  diverges); DEC-2 four div rows still disclosed; DEC-8 `repark_raises`;
  TY-3 pin asserts `decimal128(21,1)` nullable vs Spark `(11,1)` non-null
  with `forType(INT)` docstring; EXISTS family `kind="content"`; correlated
  IN + UPDATE `kind="split"`; B-TZ-4 NY string `2024-06-15 08:00:00`
  (`repark is None`); V-4 `tz8_*` + `carry_identity_timestamp_*` pins present.
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
| C4 TZ-8 class flip BACKLOG→DECLARED | S1 | **CLEAN** — row stays BACKLOG (intent to FIX); brief "DECLARED, not fixed" is increment disposition, recorded in binding paste truths |
| C4 family-fixed / dbt-upgrade-landed / U4b-shipped / DEC-8-FIXED (unqualified) / TZ-8-FIXED | S1 | **CLEAN** — none of those claims made |
| C4 TZ-6/TZ-7 duplication | S1 | **CLEAN** — FIXED notes left as `#85`; only a stale TZ-7 "PR-3" pointer closed |
| C4 live-mirror invent | S1 | **CLEAN** — exact-set stays 14 |
| C4 table arithmetic | S2 | **REMEDIATED** — 18 / 7 / 6 / 4 of 35 |
| C4 campaign DEC-8 vs registry DEC-8 | S1 | **CLEAN** — name-collision note on DEC-5; DEC-8 rationale says not campaign DEC-8 |
| C1 F-V4-1 in §2.1 (not a statement refuse) | S2 | **REMEDIATED** — preamble: metadata-projection gap; write succeeds |
| C2 | — | **CLEAN** (docs-only; no secrets; JVM lock never taken) |

**Convergence:** `ACC-CONVERGED` (C1+C2+C4; verify 0, preflight 0). CL-IDENTITY checked after commit.

## E. Out of scope (honored)

`_live_parity.py`; `test_parity_live.py`; live-tier both-halves; `briefs/`;
engine code; new corpus rows; lockfiles; `.github/`; `docs/design/`; AWS;
merges; JVM lock; TZ-6/TZ-7 FIXED-note rewrite; S-wave ANSI / G8 / re-home /
dbt-upgrade claims; "family fixed" / "DEC-8 FIXED" (unqualified) / "TZ-8 FIXED".
