# Unit ledger — FN-GT2 leftover thin-wires + SQM rework

**Unit:** FN-GT2 · conductor-20 · **Date:** 2026-08-17 ·
**Executor:** Grok (grok-4.6) ·
**Worktree:** the conductor-20 worktree · **Branch:** `grok/c20-fngt2-thin-wire` ·
**PR:** #174 (in-place rework; stays the one PR) ·
**Base:** `2cfcba9` (origin/main, FN-GT1 #172).

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

PySpark 4.1.2 `get(col, index)` is **array-only** (`ColumnOrName | int`; a `str` index is a column name). It refuses maps (`DATATYPE_MISMATCH` — first param requires ARRAY). FN-E shipped `get`; it is not a GT2 name. No change here.

## Residuals

- `shuffle(NULL array)` panics inside `datafusion-spark`'s kernel (`arrow-data` primitive transform, slice length 0). Not pinned. DF-owned.
- `make_interval` CAST AS STRING is `'24 mons'` / `'1 days'`, not Spark's `'2 years'` / `'1 days'` (days match; years display differs).
- `shuffle(seed=)` (PySpark 4.0+) is an honest cut.
- Calendar-interval `collect()` stays `PySparkNotImplementedError` (existing `test_f1_errorclass`); GT2 pins `to_arrow()`.

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
| `make verify` | exit **0** |
| `make preflight` | exit **0**; facade **3330 passed / 70 skipped** (critic pins added after; GT2 file 18 passed) |
| two-pass hygiene | PASS1 added-line needles **0**; PASS2 new files **0** |

## Pre-PR critic report (/repark-harden)

Engine: ACC `review-only` risk_tier=high (expr_fn.rs / dispatch / owned UDF) —
two explore critics spawned for real, then finder-battery 5 dimensions spawned
for real. Second quiet finder loop **NOT-RUN** (not a write path).

Critic-1 (quality/parity): attacked signatures, W1–W5 wiring, pins, R1, maps.
Filed Q-001..Q-011. S1s remediating this turn: Spark camelCase kwargs
(`pairDelim` / `keyValueDelim` / `partToExtract`); empty-pair keep; kv-regex
+ SQL-door pins; `url_decode` raise pin; honest `parse_url` NULL kernel;
`make_dt_interval("d")`; 3-arg QUERY pin; crate-root map row. Q-010
(ColumnOrName vs convenience lit on delimiter/`partToExtract` `str`) is
ACCEPTED_FLAGGED S2 — defaults must be literals; Column form is the Spark
column-name path.

Critic-2 (security/safety): attacked ReDoS, panic, error leak, coerce,
unix_micros TZ, facade injection, hygiene. Verdict CLEAN at S1 floor.
SAF-001 S2 remediating: defensive Utf8 cast before downcast.

Signature table: 18 GT2 names vs live 4.1.2. Mismatches found/fixed:
element_at extraction lit; interval str=column; unix_micros cast-first;
str_to_map regex; camelCase kwargs. Remaining: `parse_url`/`str_to_map`
bare-`str` convenience lit (Q-010).

Oracle probes: element_at `'b'` vs `col('b')`; make_interval(`"y"`);
unix_micros UTC 0 + LA 28.8e9 / 2015-07-22 PDT instant; str_to_map
`[,c]` / `[x]` / empty pair; bitmap 1/123/32769; url_decode `%ZZ` raise.

Pin audit: each W/P item names the impl it kills (see mutation table).
Finder S1s remediating: wrap-bucket 32769; raw-str delimiter; parse_url
NULL; make_date column names; unix_micros column name.

Convergence: **ACC-CONVERGED** at S1 floor after one remediation cycle.
Finder-battery: see below.

## Finder-battery report

Target: worktree vs `2cfcba9` + uncommitted rework | dimensions: 5
(wiring, pins, fence, removed-behavior, cross-file/TZ/regex) | finders
spawned as explore subagents.

Round-1 raw findings remediating (S1): wrap-around bitmap bucket;
`parse_url` NULL input; `make_date`/`unix_micros` column-name direction;
raw-str `str_to_map` delim; exact-type tightening where cheap; `get()`
doc honesty; rustdoc throw-vs-NULL.

Refuted: unescaped `|` as pair delim — live 4.1.2 also raises
DUPLICATED_MAP_KEY (not a rust-only miss).

Residuals (S2, not blocking): facade `str` convenience lit vs Spark
ColumnOrName for delimiters/`partToExtract`; rust regex ≉ Java Pattern
(Unicode classes / lookaround); SQL-door `unix_micros` TZ is the
engine CAST path (W5 pins the facade mold); `mapKeyDedupPolicy` text
is inherited DF dead copy; shuffle(NULL) DF kernel panic.

Second quiet loop: **NOT-RUN**.

Verdict: S1s from the spawned round remediating; do not stamp CLEAN
for a second unrun loop.
