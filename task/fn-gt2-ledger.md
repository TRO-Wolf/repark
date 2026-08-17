# Unit ledger — FN-GT2 leftover thin-wires + SQM rework

**Unit:** FN-GT2 · conductor-20 · **Date:** 2026-08-17 ·
**Executor:** Grok (grok-4.6) ·
**Worktree:** the conductor-20 worktree · **Branch:** `grok/c20-fngt2-thin-wire` ·
**PR:** #174 (in-place rework; stays DRAFT) ·
**Base:** `b628b0f` (origin/main, B4 #175; rebased from `2cfcba9`).

**Oracle:** pinned pyspark==4.1.2, `JAVA_HOME` = the OpenJDK 21 path named
in the orchestrator unstick, `SPARK_LOCAL_IP=127.0.0.1`.

## SQM verdict items

| Item | Fix |
|---|---|
| W1 `element_at` | `lit_indices={1}` unless `extraction` is a `Column`. `'b'` is a literal map key. Pin: `map(a→1,b→2)` + column `b='a'` → lit key 2.0, col key 1.0. |
| W2 `make_interval` / `make_dt_interval` | `str` excluded from `lit_indices` (`make_date` mold). `F.make_interval("y")` with `y=2` → 24 months. Spark display `'2 years'`; repark display `'24 mons'` (same span). |
| W3 `unix_micros` | `.cast("timestamp")` first (`unix_date` mold). String input no longer raises. |
| W4 `str_to_map` | Owned regex UDF (`str_to_map.rs`) overwrites DF literal `str.split`. Discriminating pin: `'[,c]'` → `{a:1, b:2, '':3}`. |
| W5 non-UTC | `America/Los_Angeles` pins: unix_micros epoch-string is 8 hours in micros; 2015-07-22 10:00:00 is the PDT instant. Date/interval names hold. |
| P1 | `make_interval(days=1)` → MonthDayNano(0,1,0) + `month_day_nano_interval`; `make_dt_interval(1,0,0,0)` → `timedelta(days=1)` + `duration[us]`. Vacuous `assert X or str(...)` removed. |
| P2 | `bitmap_bit_position(1)=0`, `(123)=122`; `bitmap_bucket_number(1)=1`, `(123)=1`. All int64. |
| P3 | unix_micros non-epoch non-UTC: the 2015-07-22 10:00:00 LA instant. |
| P4 | `is_map` on map_from_entries / str_to_map; string type on try_parse_url / url_decode / try_url_decode. |
| P5 | NULL-input rows for the GT2 names (shuffle residual; `parse_url` NULL now pinned). |
| R1 | Parameters/Returns/Examples on all 18 names. Examples executed in `test_gt2_docstring_examples_execute`. `bitmap_*` moved to `functions_bitwise.py`. |

## get() payload (not this PR)

PySpark 4.1.2 `get(col, index)` is **array-only** (maps refuse). FN-E shipped
`getitem` and `test_get_map_by_key` still pins map lookup. Docstring now states
both facts (Spark refuse + repark `getitem` still serves maps). Impl unchanged.

## Residuals

- `shuffle(NULL array)` panics inside `datafusion-spark`'s kernel (`arrow-data` primitive transform, slice length 0). Not pinned. DF-owned.
- `make_interval` CAST AS STRING is `'24 mons'` / `'1 days'`, not Spark's `'2 years'` / `'1 days'` (days match; years display differs).
- `shuffle(seed=)` (PySpark 4.0+) is an honest cut.
- Calendar-interval `collect()` stays `PySparkNotImplementedError` (existing `test_f1_errorclass`); GT2 pins `to_arrow()`.
- Q-010: `parse_url` / `try_parse_url` / `str_to_map` bare-`str` part/key/delim
  is a convenience lit; Spark 4.1.2 `ColumnOrName` binds a column of that name.
  Pass `F.col(...)` for the Spark column-name path. ACCEPTED_FLAGGED S2.
- `parse_url` QUERY 3rd arg: Spark compiles an unquoted Java `Pattern`; DF is
  exact key equality. Pin: `'f.o'` on `?foo=1` → NULL (Spark `'1'`). DF-owned.
- `parse_url('not a url','HOST')`: Spark raises `INVALID_URL`; DF HOST is NULL.
  `'inva lid://host'` raises on both. Mixed kernel, both sides pinned.
- Spark `unix_micros` refuses DATE (`DATATYPE_MISMATCH`); facade `.cast("timestamp")`
  accepts DATE and session-localizes (LA `make_date(1970,1,1)` → `28_800_000_000`).

## Mutation-proof pins

| If this is dropped… | this test reds |
|---|---|
| `element_at` wraps a str key as a literal | `test_element_at_string_is_literal_map_key` |
| interval str parts forced to literals | `test_make_interval_str_is_column_name` |
| unix_micros no timestamp cast | `test_unix_micros_and_date_diff` (string input) |
| unix_micros ignores session zone | `test_unix_micros_non_utc_non_epoch` |
| str_to_map literal split | `test_str_to_map_delimiters_are_regex` |
| vacuous interval assert | `test_make_interval_and_dt` |
| bitmap not-None | `test_bitmap_scalars` |
| QUERY key treated as Java regex | `test_parse_url_and_try` (`'f.o'` → NULL) |
| interval `"y"` not read as column years | `test_make_interval_str_is_column_name` MonthDayNano (24,0,0) |

## Files

- `crates/repark-functions/src/str_to_map.rs` (new; `#[path]` from `collection.rs`)
- `crates/repark-functions/src/collection.rs` / `expr_fn.rs` / `Cargo.toml` / `map.md`
- `crates/repark-python/src/column/map.md`
- `python/repark/src/repark/spark/functions_{collections,datetime,url,bitwise,}.py`
- `python/repark/tests/test_functions_gt2.py` / `tests/map.md` / `spark/map.md`
- `task/fn-gt2-ledger.md` / `task/map.md`

## Gates

| Gate | Result |
|---|---|
| `make verify` | exit **0** (post-rebase `b628b0f`; re-run after honesty pins) |
| `make preflight` | exit **0**; facade **3343 passed / 70 skipped** (pre-honesty; re-run after) |
| two-pass hygiene | PASS1 added-line needles **0**; PASS2 new files **0** |

## Pre-PR critic report (/repark-harden)

Engine: ACC `review-only` risk_tier=high (expr_fn.rs / dispatch / owned UDF) —
Critic-1 + Critic-2 spawned as explore subagents. Finder-battery 5 dimensions
spawned, 3-vote on S1 candidates, then a second 5-finder quiet round (also
spawned). Not a write path — two write-path quiet rounds N/A.

Critic-1 (quality/parity): attacked signatures, W1–W5, pins, R1, maps.
Q-001 S1 (doc claimed DF always NULL on invalid URL) **WITHDRAWN** after
mixed-kernel honesty: Spark raises on `'not a url'`; DF HOST NULL;
`'inva lid://host'` raises on both. Q-002 = Q-010 ACCEPTED_FLAGGED S2.
Re-spot CLEAN.

Critic-2 (security/safety): ReDoS (rust `regex` 1.13 linear), panics,
injection (identifier dispatch), TZ parse-at-build, no write path.
Verdict **CLEAN** at S1. S2: LAST_WIN dead copy; pattern echo in errors.

Signature table: 18 GT2 names vs live 4.1.2. Remaining mismatch: Q-010
convenience lit. `create_map` / `try_url_encode` not shipped (latter
absent from 4.1.2).

Oracle probes (JAVA 21, pyspark 4.1.2): element_at `'b'` vs `col('b')`;
`make_interval("y")`; unix_micros UTC 0 + LA 28.8e9 / 2015-07-22 PDT;
`str_to_map` `[,c]`; bitmap 0/−1/1/123/32769 match Spark; `parse_url`
schemeless RAISE vs NULL; QUERY `'f.o'` Spark `'1'` / repark NULL;
unix_micros(DATE) Spark DATATYPE_MISMATCH, repark LA 28.8e9.

Pin audit: W/P items name the impl they kill. Post-battery tighten:
W2 MonthDayNano (24,0,0); `date_diff` exact `int32`; bitmap 0/−1;
unix_micros LA column; QUERY `'f.o'` honest NULL.

Convergence: **ACC-CONVERGED** at S1 floor.

## Finder-battery report

Target: `origin/main...HEAD` + unstaged honesty | dimensions: 5
(wiring, pins, fence, removed-behavior, cross-file/TZ/regex) | finders
and verifiers spawned as explore subagents.

Round-1 raw → deduped S1 candidates (3-vote):
- F1 ColumnOrName convenience lit — CONFIRMED **S2** (Q-010 already flagged)
- F2 `date_diff` TZ-8 13-vs-14 — **REFUTED** (confounded `F.to_timestamp` NTZ;
  SQL `datediff` of LTZ already 13)
- F3 `parse_url` QUERY Java Pattern vs DF exact — CONFIRMED; remediating as
  documented DF-owned residual + `'f.o'` pin (not a new owned UDF)
- F4 `get()` docstring vs `test_get_map_by_key` — CONFIRMED doc clash;
  remediating (impl unchanged)
- F5 stale FN-D/FN-E Deferred — REFUTED as contract lie (historical);
  annotated “GT2 shipped”
- F8 bitmap ≤0 formula — **REFUTED** (DF already matches Spark; 0/−1 pinned)
- F10 unix_micros column epoch — **REFUTED** (cast-first still reds Utf8;
  LA column now pinned)

Quiet round (5 finders spawned):
- `unix_micros(DATE)` UTC-midnight S1 — **REFUTED live**: Spark refuses DATE;
  repark LA `make_date(1970,1,1)` → `28_800_000_000`
- `unix_seconds`/`unix_millis` missing cast-first — **out of scope** (FN-D,
  not a GT2 name)
- `date_diff` `int32|int64` disjunction + W2 display-only pin — remediating
  (`int32`; MonthDayNano (24,0,0))
- fence / removed-behavior: no new S0/S1

Verdict: no open S0/S1. Quiet-round survivors either REFUTED, out of scope,
or remediating as pin honesty. Do not stamp CLEAN for an unrun loop — this
second round **did run**.
