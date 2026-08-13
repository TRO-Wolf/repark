# W-1 TZ-4 PR-2 ledger — zoneless localization + NTZ distinction

**Date:** 2026-08-13 · **Lane:** W-1 · **Branch:** `grok/w1-tz4-pr2` · **Base (frozen):**
`c7e6589088111ded62848751a30a45adfea0973a` (`#79` tip)
**Charter:** `BRIEF-w1-tz4-pr2.md` + conductor-6 A1–A8 + `TZ4-DESIGN.md` §5.2 + Y-8
addendum (Q3–Q12 = memo leans; Q12 = session-zone `toInternal`/`fromInternal`).
**SEPMO:** HIGH — octo + C4, sequential hat-switch (no isolation worktree).

---

## 0. Blast-radius sweep (mandatory before code)

Enumerated zoneless LTZ input paths and NTZ sites on this tree:

| Path | Home | Disposition |
|---|---|---|
| `TIMESTAMP '…'` literal | `instant_ts.rs` wrap → `to_timestamp` | **localize at invoke** |
| zoneless `to_timestamp(str)` | `SparkToTimestamp` | **localize if no zone suffix** |
| `CAST(str AS TIMESTAMP)` | analyzer rewrite → `to_timestamp` | **localize** |
| `CAST(date AS TIMESTAMP)` | same rewrite | **localize midnight** |
| `CAST(µs-naive AS TIMESTAMP)` | NTZ promote | **localize** |
| leftover `Timestamp(ns, None)` | DF `now()` / folded instants | **type-wrap only** (not NTZ) |
| zone-suffixed `to_timestamp('…Z')` | same UDF | **do not localize** |
| naive `datetime` `createDataFrame` | `_funcs.py` infer LTZ + localize | **localize** |
| explicit `TimestampType` schema | `_data_type_to_sql_type` + arrow | **µs+UTC** |
| explicit `TimestampNTZType` schema | `TIMESTAMP_NTZ` → naive µs | **stay naive** |
| `TimestampType.toInternal`/`fromInternal` | `types.py` + `session_time_zone.py` | **session zone (Q12)** |
| `cast(TimestampType())` | `column.rs` `parse_data_type` | **µs+UTC, no strip** |
| extractors | `datetime.rs` coerce | **type-driven** (zoned=LTZ, naive=NTZ) |
| `_arrow_type_to_repark` / `simpleString` | `types.py` / `dataframe.rs` | **distinguish tz vs naive** |
| `CAST(ts AS DATE)` / `to_date` / `datediff` | TZ-8 | **out of scope** |
| `CAST(ts AS STRING)` render | B-TZ-4 | **out of scope** |
| `spark.sql.timestampType` / `conf.set` tz | Q10/Q11 | **out of scope** |
| `functions.py` `F.lit(aware)` | W-4 CLOSED | **named residual** |
| ANSI CREATE ns-reject | `create_table.rs` CLOSED | **untouched** |

Test families:

| Family | Disposition |
|---|---|
| tz equality rows (ints) | stay |
| `tz_aware_to_naive_round_trip` | **flip to equality** (string was zone-capable) |
| TZ-7 two spellings + naive column | **flip to equality** |
| TZ-7 `TIMESTAMP` literal row | **value-converged**; extractor **nullability** residual (Spark non-null) |
| TZ-6 NTZ vs LTZ | **flip to equality** |
| `test_a3_cast_vocab` TimestampType | **µs+UTC** |
| dogfood `cast(TimestampType())` | **keeps UTC** |
| interchange Z-2 loose pins | **tightened** to `us`+`UTC` where type parity is claimed |
| live SCENARIOS | count stays 42; no recipe flip expected |
| `_live_parity.py` | Y-4 citation only (`[timestamp_to_int_nullability]`) |
| TZ-5 numeric-cast corpus | stay (nullability of `to_timestamp` kept True) |

---

## 1. Decisions

1. **Type-driven extraction (Q8=A).** `Timestamp(_, Some(_))` is an instant (session zone).
   `Timestamp(_, None)` is NTZ (no session zone). Coerce arms are fixed points.
2. **NTZ is µs-naive only.** Leftover `Timestamp(ns, None)` is an un-annotated instant
   (PR-1 residue / DF `now()`), not NTZ — type-wrap, do not localize.
3. **Localize at invoke, not as a folded literal.** Analyze-time µs+UTC literals let
   `date_format` const-eval with a tz-stripped `ScalarValue`. `TIMESTAMP '…'` rewrites to
   `to_timestamp('<wall>')`; `to_timestamp` / `date_format` are `Volatile` so the carrier
   is present. Planner-inserted `CAST(to_timestamp AS Timestamp(µs))` (naive) is peeled.
4. **Python boundary.** `TimestampType` → `timestamp[us, tz=UTC]`; `TimestampNTZType` →
   `timestamp[us]`. Q12: `toInternal`/`fromInternal` use `spark.sql.session.timeZone`.
5. **Retirement.** TZ-6 and TZ-7 headings get dated FIXED notes. Value class closed.
   Residual named: extractor nullability on `TIMESTAMP` literals; `F.lit(aware)` under
   non-UTC (`functions.py` CLOSED); B-TZ-4; TZ-8.

---

## 2. Lock events

| Event | Detail |
|---|---|
| acquire | `/tmp/grok-jvm-record.lock` `MARKER=w1-blast` pid=2051068 iso=2026-08-13T12:54:30-04:00 |
| conductor stale-rm | 2026-08-13T13:25:18 — acquire-shell pid died; conductor wrote `/tmp/grok-w1-first-released` and opened FIFO |
| accidental overwrite | W-1 wrote `MARKER=w1-record` over live `MARKER=w2-dec` pid=2598238 — **restored immediately** (W-2 SparkSubmit still running) |
| re-acquire | `MARKER=w1-parity-live` iso=2026-08-13T13:53:51 then refreshed 13:59:10 after W-2 released |
| release | marker-verified rm after parity-live (collation-only expected red) |
| leftover | **no** |

---

## 3. Actor / Critic (sequential C4, early_stop)

- Cycle 1 Actor: type-driven coerce + localize + Python boundary + pin flips.
- Cycle 1 Critic (procedural break): CAST(ns-naive) was being treated as NTZ (S1) —
  **REMEDIATED** (only µs-naive is NTZ). Analyze-time literal localization broke
  `date_format` (S1) — **REMEDIATED** (rewrite to `to_timestamp` + peel naive CAST).
  `to_timestamp` return field must stay nullable (S1, TZ-5 corpus) — **REMEDIATED**.
- Early stop after cycle 1 Half A CLEAN ≥ S1.

---

## 4. Pins (tests with code)

- Rust: `instant_ts::tests::zoneless_inputs_localize_in_the_session_zone`,
  `zone_suffixed_to_timestamp_is_not_localized_again`;
  `session_timezone.rs::a_zoneless_timestamp_input_localizes_in_the_session_zone`,
  `a_naive_ntz_timestamp_is_not_shifted_by_the_session_zone`;
  coerce pin `only_timestamp_arguments_are_coerced_to_instants` (naive → not instant).
- Facade: TZ-6/TZ-7/round-trip rows; `test_a3_cast_vocab` timestamp; dogfood
  `cast(TimestampType())` keeps UTC; interchange type pins tightened.

---

## 5. Out of grant / named residuals

- B-TZ-4 (`CAST(ts AS STRING)` render) — PR-3.
- TZ-8 date-cast shim.
- `functions.py` `F.lit` (W-4 CLOSED).
- `column.py` CLOSED — NTZ cast token not added to the Python allowlist.
- **Forced overflow (representation flip):**
  `python/repark-parity/bench/fuzz/compare.py` (UTC-aware == naive wall vs DuckDB);
  `test_fuzz_smoke.py` pin; `dataframe/core.py` collect → naive session-zone wall
  (Apache `test_datetime_at_epoch`). Pandas-inferred naive timestamps stay naive
  (G10 unit-family pins). Live `cast_timestamp_to_int_nullability` kept as
  nullability disclosure (string-literal `TIMESTAMP` → non-null LTZ literal).
- `spark.sql.timestampType`, runtime `conf.set` timezone.
- Extractor nullability on `TIMESTAMP '…'` (Spark non-null).

---

## 6. Registry handoff (W-5 owns the TZ-4 progress row — TEXT ONLY)

Paste-true for W-5's TZ-4 progress row (do **not** land here):

> **TZ-4 progress (2026-08-13, TZ-4 PR-2 / W-1).** Instant producers were µs+UTC in PR-1
> (`#79`). PR-2 localizes zoneless LTZ inputs in `spark.sql.session.timeZone` and
> distinguishes `TIMESTAMP` (µs+UTC / Iceberg `timestamptz`) from `TIMESTAMP_NTZ`
> (naive µs / Iceberg `timestamp`). Extraction is type-driven. **TZ-6 and TZ-7 are
> FIXED** (their registry sections, W-1 grant). Residual: extractor nullability on
> `TIMESTAMP` literals; B-TZ-4 string-cast; TZ-8 date-cast. `spark.sql.timestampType`
> is not implemented (Q10).

---

## 7. Identity / hygiene

- Commits: `git -c user.name=TRO-Wolf -c user.email=64240326+TRO-Wolf@users.noreply.github.com`
- Trailer: `Authored-By: Grok (grok-4.5) <noreply@x.ai>`
- Two-pass hygiene before push (count 0).
- `planning/grok/*` not committed.
