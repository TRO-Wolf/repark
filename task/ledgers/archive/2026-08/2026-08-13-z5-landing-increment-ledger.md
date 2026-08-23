# Unit ledger — Z-5 Y-wave §6 landing increment

**Unit:** Z-5 · **Date:** 2026-08-13 · **Lane:** repark · **Executor:** Grok ·
**Worktree:** `/tmp/grok-z5` · **Branch:** `grok/z5-landing-increment` ·
**Base (frozen):** `9b2dce3c73af402e8705923135d7de014da5501f`
(`fix(g5b-r): window RANGE residuals — R3 no longer wraps (#72)`)

**Charter:** workspace brief `BRIEF-z5-landing-increment.md` (conductor-5; not in this
repo). SEPMO STANDARD, acc + `claims_critic=true`. Prime directive: verify-before-paste.
L-1 mold at one-tenth the size.

**One-time grant (expires at wave end):** `docs/spark-sql-iceberg-parity.md` + ONE dated
`STATUS.md` note (Y wave landed + Z wave in flight) and `_Last updated`. `_live_parity.py`
is Z-2's tonight.

---

## A. §6 sweep — classification table (completeness proof)

Every named Y-wave §6 handoff enumerated. Disposition is one of **LANDED** /
**ALREADY-LANDED** / **SUPERSEDED** / **DEFERRED**. Re-verified on THIS base
(`9b2dce3`), not on the Y-wave freeze (`a985edf`).

| Ledger | Handoff item | Class | Action / cite |
|---|---|---|---|
| `g5br-range-residuals` | G5b-R2 `DAY TO SECOND` | **LANDED** | FIXED dated note; live BACKLOG row retired (corpus equality `[10,30,60,90,90]`; Rust `temporal_range_day_to_second_literal_matches_spark`) |
| `g5br-range-residuals` | G5b-R3 negative-offset wrap | **LANDED** | FIXED dated note on Spark door / facade `.sql()` (`count` `[0,0,0,0,0]`, `sum` NULL; Rust empty-frame + invert-refuse pins). **Not** claimed closed on the ANSI door |
| `g5br-range-residuals` | G5b-R3-ANSI named residual | **LANDED** | named inside the R3 FIXED note only; no dedicated pin → no G5b-R3-ANSI row |
| `g5br-range-residuals` | G5b-R1 unquoted `INTERVAL` | **ALREADY-LANDED** | live BACKLOG row kept; still OPEN (Z-4 in flight — not this PR) |
| `g5br-range-residuals` | G5b-R4 FOLLOWING-to-FOLLOWING | **ALREADY-LANDED** | live BACKLOG row kept (120 vs 90); still OPEN (Z-4 in flight) |
| `g5br-range-residuals` | G5b-R5 interval over numeric key | **LANDED** | still OPEN; Spark half **amended** (verify-before-paste: Spark returns the recorded self-group table `[10,20,30,40,50]`; L-1's "raises a Spark error class" was stale vs the Y-1 corpus) |
| `y7-collation-refuse` | G15 collation refuse disclosure | **LANDED** | new BACKLOG row G15; pins re-checked present (Python + both Rust doors) |
| `y4-rename` | G6-4 pin line | **LANDED** | citation now `timestamp_to_int_nullability`; `live-mirror: cast_timestamp_to_int_nullability` unchanged |
| `y4-rename` | REG-G4-1/2 FIXED-note test ids | **LANDED** | `df_left_semi_on_name` / `df_left_anti_on_name` (old `*_unsupported` ids gone from collect) |
| `y4-rename` | `_live_parity.py` Disclosure note citation | **DEFERRED** | Z-2 owns `_live_parity.py` tonight; live-mirror **token** already correct; corpus node-id in the note is still the pre-rename spelling |
| `y10-ansi-door` | G11 closed-ruling row | **LANDED** | dated ruling note after ID-1; six `cross_door.rs` + six `ansi_door_values.rs` tests present |
| `y10-ansi-door` | F-Y10-1 integer wrap | **LANDED** | routed to DEC campaign U5 / G13 (addendum Q11 = A); **not** a new DEC row; **not** a fix |
| `y10-ansi-door` | F-Y10-2 ANSI float `/ 0` Inf | **LANDED** | residual note; door-vs-door Inf-vs-NULL already INTENDED in `cross_door_float_div_by_zero_is_infinity_on_ansi_null_on_spark`; not DEC-7 |
| `y10-ansi-door` | dated slate-amendment in `briefs/` | **DEFERRED** | never-touch `briefs/` |
| `y3-getdatabase` | FA-2 `getDatabase` `locationUri` note | **LANDED** | FA-2 rationale dated note; `listDatabases` half unchanged (`test_list_databases_location_uri_none_divergence` still pins `None`) |
| `y3-getdatabase` | optional LOCATION-less memory-ns row | **DEFERRED** | Y-3 said not this unit; no new pin-as-divergence |
| `y5-origin-map` | G4b D6 origin-map FIXED | **LANDED** | FIXED dated note; no live row; no `live-mirror`; pins present in `test_g4b_semi_join.py` |
| `y5-origin-map` | `F.abs` after semi binds left | **DEFERRED** | Z-4 in flight; this increment does not land Z-4 work |
| `y6-boundary-shapes` | G10-1 map `toPandas` dict vs list-of-pairs | **LANDED** | BACKLOG; no `live-mirror` (handoff had none; Z-2 owns `_live_parity.py`) |
| `y6-boundary-shapes` | G10-2 struct Long `float` vs `int` | **LANDED** | same |
| `y6-boundary-shapes` | G10-3 pandas-ingest `item` vs `element` | **LANDED** | same |
| `y6-boundary-shapes` | G10-4 inbound `datetime64[us]` vs `[ns]` | **LANDED** | same |
| `z5-landing-increment` | this table | **LANDED** | this file |

**Counts:** LANDED 17 · ALREADY-LANDED 2 · SUPERSEDED 0 · DEFERRED 4 · table rows 23.

**Live-mirror both-halves:** **none.** No §6 handoff demanded a new `live-mirror:` token.
Y-4 6.3 is a citation refresh inside an existing Disclosure note (deferred, Z-2). Y-6 G10
rows are registry-only (same class as L-1's W-4 ranking TYPE rows). Expected by the
charter.

### Verify-before-paste notes

- **Base moved.** Y-wave handoffs were written on `a985edf` (#65). This tree is
  `9b2dce3` (#72 tip). Pins were re-read here; R2/R3 corpus rows are equalities; R1/R4/R5
  still disclose / raise as Y-1 recorded.
- **G5b-R5 Spark half amended.** L-1 wrote "Apache Spark — raises a Spark error class."
  The live corpus on this base (`temporal_range_interval_bound_over_int_key`) pins Spark
  as the self-group table `[10,20,30,40,50]` and repark as `repark_raises="PySparkException"`
  with the Arrow-cast needle. Pasted the corpus, not L-1.
- **R1 / R4 / R5 are not Z-4.** Z-4 is in flight on those residuals. This increment tags
  them still OPEN and does not flip them.
- **F-Y10-1/2 follow the 2026-08-13 DEC addendum** (Q11 = A overflow on U5 / G13; native
  ANSI door does not get Spark DecimalPrecision). No invented DEC-10 / integer-overflow
  row and no invented float-div0 DEC row.
- **G4b D6 is a FIXED note, not a live row** (Y-5 D5). Conditionless semi/anti stays G4-3.
- **`_live_parity.py` untouched.** Z-2 owns it tonight.

---

## B. Other landings

- **STATUS.md** `_Last updated: 2026-08-13` + ONE dated note (Y wave landed + Z wave in
  flight) under Current milestone. Critic cycle-1 also updated the known-issues G5b-R3
  HIGH bullet to FIXED-on-Spark-door (STATUS house rule: the fixing landing deletes or
  restates the open defect; leaving HIGH wrap as live was CL-STALE). The 2026-08-12 H-2
  seed+tail "Still open: G11/G15 / G5b-R" sentence is left as the L-1 snapshot; the dated
  note is the increment.
- **No** `_live_parity.py`, **no** `test_parity_live.py`, **no** engine / corpus edits,
  **no** `briefs/`, **no** Z-1/Z-2/Z-3/Z-4 in-flight work.

---

## C. Gate evidence

Recorded as `cmd > /tmp/z5-<gate>.log 2>&1; echo $?`.

| Gate | Log | Exit |
|---|---|---|
| `make verify` | `/tmp/z5-verify.log` | **0** |
| `make preflight` | `/tmp/z5-preflight.log` | **0** (facade **2901 passed**, 71 skipped; cargo deny / pip-audit / zizmor "No findings to report") |
| JVM lock | n/a | **never taken** |

---

## D. Authorship

Per-command `git -c user.name=TRO-Wolf -c user.email=64240326+TRO-Wolf@users.noreply.github.com`.
Trailer `Authored-By: Grok (grok-4.5) <noreply@x.ai>`. After every commit:
`git log -1 --format='%ae'` must equal that email byte-exact.

---

## Critic remediations (cycle 1)

| ID | Sev | Disposition |
|---|---|---|
| C1 Q-001 / C4 CL-STALE | S1 | **REMEDIATED** — STATUS known-issues G5b-R3 restated as FIXED on Spark door; R1/R4/R5 + ANSI wrap named |
| C1 Q-002 | S2 | **ACCEPTED_FLAGGED** — 2026-08-12 H-2 "Still open: G11/G15 / G5b-R" left as L-1 snapshot; 2026-08-13 dated note is the increment |
| C1 Q-003 | S2 | **REMEDIATED** — registry R1/R4/R5 rationale no longer names the in-flight Z-4 lane |
| C2 | — | **CLEAN** (null reports; docs-only; JVM lock never taken) |

## E. Out of scope (honored)

Z-1/Z-2/Z-3/Z-4 implementation; `_live_parity.py`; live-tier both-halves; `briefs/`;
engine code; new corpus rows; lockfiles; `.github/`; `docs/design/`; AWS; merges.
