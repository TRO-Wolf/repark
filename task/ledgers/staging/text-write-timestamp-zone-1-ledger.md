# Unit ledger — WO TEXT-WRITE-TIMESTAMP-ZONE-1 · CSV and JSON writes format TIMESTAMP in the session zone

**Date:** 2026-09-29 · **Branch:** `fix/text-write-timestamp-zone-1` · **Base:** `adc26586` (`origin/main`)
**Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** In `America/New_York`, `SELECT 1 AS id, TIMESTAMP '2024-03-10 02:30:00' AS t`
written with `.write.csv(p)` held `2024-03-10 07:30:00` (the UTC wall, no offset) and with
`.write.json(p)` held `2024-03-10T07:30:00Z`. Spark 4.1.2 writes
`2024-03-10T03:30:00.000-04:00` in both: the session-zone wall under Spark's default
`timestampFormat` (`yyyy-MM-dd'T'HH:mm:ss.SSSXXX`). A reader in another tool saw a different
wall from what Spark wrote. Present on `main` (release-diff `x3/csv_ts_local_r`,
`x3/json_ts_local_r`).

**Diagnosis, measured.** The CSV/JSON writers handed TIMESTAMP columns to the Arrow sink
unformatted (UTC wall, no offset, no millis), and refused the three temporal options
(`dateFormat`, `timestampFormat`, `timestampNTZFormat`) loud. The step-0 oracle (8 probe
rounds, 616 cells over UTC, America/New_York, Asia/Kolkata, Australia/Lord_Howe, a fixed
`+05:30` zone) locked Spark's bytes: session-zone wall with offset for LTZ, zone-free
`yyyy-MM-dd'T'HH:mm:ss[.SSS]` for NTZ, `yyyy-MM-dd` for DATE, user patterns honored per
kind, and per-kind refusal classes for bad patterns.

**Fix.** One shared Rust formatter at the writer's formatting step, no sink change. CSV and
JSON `COPY` statements (local and s3a) build their inner `SELECT` in
`text_write_format::select`, which wraps every temporal column in the volatile
`repark_write_format_text` UDF carrying the three user patterns plus the canonical session
zone id; the UDF renders each value (`render.rs`) and the sink writes plain strings.
Non-temporal frames keep byte-identical `SELECT *`; partition columns stay unwrapped;
parquet/ORC/Iceberg and every read path are untouched. Bad patterns refuse eagerly with
Spark's class (`INVALID_DATETIME_PATTERN.*`, NTZ downgrades, `DATETIME_*_RECOGNITION` /
week-based); kind-missing fields fail lazily at execution with Spark's message, as Spark
fails the write task. About 1640 product lines (formatter, UDF, SELECT builder, both
bindings) plus 43 Rust unit pins and 182 facade pins.

**What Spark does (Spark 4.1.2, `local[1]`, probes `probe0.py`–`probe7.py`, outputs
`w0-spark.json`–`w7-spark.json`; the replayed subset is committed as
`python/repark/tests/text_write_timestamp_zone_1_fixture.json`).** Session zone
America/New_York unless noted:

| Cell | Spark |
|---|---|
| DST-gap `TIMESTAMP '2024-03-10 02:30:00'`, csv + json | `2024-03-10T03:30:00.000-04:00` |
| DST-overlap `TIMESTAMP '2024-11-03 01:30:00'` ×2 instants | `-04:00` then `-05:00` walls |
| frac 0 / 3 / 6 (`…56`, `…56.789`, `…56.789456`) | `.000` / `.789` / `.789` (millis truncation) |
| `TIMESTAMP_NTZ`, csv + json | `2024-06-15T12:34:56.789`, no zone, every session zone |
| `DATE`, csv + json | `2024-06-15` |
| pre-1900 Kolkata (`TIMESTAMP '1899-12-31 23:59:59'`) | `1899-12-31T23:59:59.000+05:21:10` (LMT seconds kept) |
| user `timestampFormat yyyy/MM/dd HH:mm` | `2024/06/15 12:34` |
| user `timestampNTZFormat yyyy/MM/dd HH:mm:ss` | `2024/06/15 12:34:56` |
| user `dateFormat dd/MM/yyyy` | `15/06/2024` |
| `timestampFormat` on a DATE column / `dateFormat` on a TIMESTAMP column | ignored (that kind's default renders) |
| `timestampFormat ''` / `[]` / `[xxx` (date) | empty string |
| `timestampFormat QQQQQ`, `dateFormat qqqqq` | `INVALID_DATETIME_PATTERN.LENGTH`, `IllegalArgumentException` |
| `timestampFormat ddd`, `BOGUS`, `]`, `vv`, `OOO`, `V`, `yyyy V` | `DATETIME_PATTERN_RECOGNITION`, `ILLEGAL_CHARACTER` (names `B`), `RECOGNITION`, `WITH_SUGGESTION` ×4 |
| `timestampNTZFormat` of the same bad shapes | `WITH_SUGGESTION` (downgraded) |
| `dateFormat` of the same bad shapes | same classes, except structural faults fail lazily (`Pattern invalid…`, `Unknown pattern letter…`, `Pattern letter count…`, `Pattern ends with an incomplete string literal…` with `y`→`u`) |
| zone letters on NTZ/DATE (`XXX`, `VV`, `z`, `HH`) | lazy `Unsupported field: OffsetSeconds/HourOfDay…` / `Unable to extract ZoneId from temporal …` |
| zone letters on LTZ (`z`, `zz`, `zzzz`, `v`) | rendered from JRE locale data (`EDT`, `Eastern Daylight Time`, `ET`) — RePark refuses loud (divergence, see Residues) |
| NULL | empty field / absent JSON key |

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | CSV and JSON LTZ bytes equal Spark's default `timestampFormat` in the session zone (gap, overlap, frac 0/3/6, pre-1900 LMT seconds, every probe zone); NTZ and DATE render zone-free defaults; struct/array nesting formats in JSON; a same-zone read-back returns the original values. | Facade bytes cells + read-back legs in `test_text_write_timestamp_zone_1.py` over the committed fixture. | PROVEN | 182 passed (179 fixture cells incl. 4-zone matrix, CSV/JSON read-back, s3a). Full 616-cell oracle replay: 562 byte-or-class exact, 54 classified with zero flips. |
| C-002 | User `timestampFormat` / `timestampNTZFormat` / `dateFormat` are honored exactly as Spark honors them (per-kind semantics, cross-kind ignored), on CSV and JSON, local and s3a. | Facade user-format cells + the three flipped R2 honored pins + the Rust honored pin. | PROVEN | Same 182 green; `test_r2_read_formats2.py` honored trio green; `temporal_write_options_are_honored_on_csv_path_write` green. |
| C-003 | Unsupported patterns refuse with Spark's error class, never silently: eager `INVALID_DATETIME_PATTERN.*` / `DATETIME_*` classes per kind, lazy `Unsupported field` / `Unable to extract ZoneId` / structural messages at execution. | Facade error-token cells + Rust validator unit pins. | PROVEN | 25 error-token cells green; Rust validator suite green (every class per kind). |
| C-004 | The Rust compiler, validator, renderer and SELECT builder match the oracle cell by cell, including the offset-letter matrix with LMT seconds, the `''` quote escape, section skip, and the star fast path / partition skip / eager rejection of the builder. | `session::tests::text_write_format` unit pins. | PROVEN | `cargo test -p repark-core --lib session::tests::text_write_format`: 43 passed. |
| C-005 | The s3a route formats identically (moto, fake creds only) and read-back round-trips the instant. | The moto s3a leg + the CSV/JSON read-back legs. | PROVEN | `test_s3a_csv_write_matches_spark` green; CSV read-back returns the instant, JSON keeps the bytes and `CAST` recovers it. |
| C-006 | Nothing else changes: parquet/ORC/Iceberg writes, non-temporal CSV/JSON bytes, the CSV header default, and every read path are untouched; the 25-statement neighbour diff reports no unintended change; the only pin flips are the four mandated refusal-to-honored flips. | Neighbour base-vs-head diff + the flip census + the untouched-path suites. | PROVEN | 25-statement base-vs-head diff: 1 change, the intended `n22` zone wall; multi-part order and random names normalized. Flip census: 3 R2 + 1 Rust refusal pins flipped (all mandated); 0 UTC-wall asserts; 0 unexpected. Temporal + csv/json suites green (237, 459, 801 passed). |

## Mutation record (2026-09-29)

| # | Mutation | Red |
|---|---|---|
| M1 | Force the SELECT builder zone to UTC (`zone = "UTC"`) | 71 facade pins red: every non-UTC LTZ bytes cell plus the CSV/JSON read-back and s3a legs; pure NTZ (`ntz0/3/6`) and DATE (`date`) cells stay green; `userdate`/`userntz` red only via their `t` column. Reverted; `git status` clean; 182 green. |

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: text-write-timestamp-zone-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every must-change oracle cell replays byte-or-class exact across 4 zones plus a fixed-offset zone; DST gap/overlap, frac 0/3/6, NTZ, DATE, NULL, pre-1900 LMT seconds, user formats per kind, header option, and nested JSON all pin Spark's bytes.
      artifacts: [python/repark/tests/test_text_write_timestamp_zone_1.py, python/repark/tests/text_write_timestamp_zone_1_fixture.json]
    - id: AT-2
      status: ATTACKED
      evidence: CSV and JSON doors, local and s3a routes, LTZ/NTZ/DATE value kinds, struct/list/map recursion, partition-column exclusion, duplicate/case-folded names left unwrapped; parquet/ORC/Iceberg and reads verified untouched by the neighbour diff.
      artifacts: [crates/repark-core/src/session/text_write_format.rs, crates/repark-core/src/session/text_write_format/select.rs, crates/repark-core/src/session/text_write_format/udf.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal class per pattern kind pins Spark's class and message (eager INVALID_DATETIME_PATTERN/INCONSISTENT_BEHAVIOR, lazy Unsupported-field/ZoneId/structural); zone-name patterns refuse loudly and pin the divergence instead of guessing bytes.
      artifacts: [crates/repark-core/src/session/text_write_format.rs, python/repark/tests/test_text_write_timestamp_zone_1.py]
    - id: AT-4
      status: N/A
      justification: Pure function of (value, pattern, zone); no shared or mutable state; the UDF holds no cache.
    - id: AT-5
      status: ATTACKED
      evidence: The s3a leg runs against moto with fake credentials only; no AWS access, no secret in output; zone ids and patterns are quoted as literals/identifiers in generated SQL (backticks parse on both doors).
      artifacts: [python/repark/tests/test_text_write_timestamp_zone_1.py]
    - id: AT-6
      status: ATTACKED
      evidence: Written bytes round-trip the instant on same-zone read-back (CSV infers timestamp; JSON keeps the string and CAST recovers it); the s3a object bytes equal the local bytes.
      artifacts: [python/repark/tests/test_text_write_timestamp_zone_1.py]
    - id: AT-7
      status: N/A
      justification: No performance claim; formatting is one pass over temporal columns only, and non-temporal frames keep byte-identical SELECT *.
    - id: AT-8
      status: ATTACKED
      evidence: No new crate edge (module lives in repark-core under session, UDF self-registers at session build); no Cargo.toml change; the binding adds one thin pyfunction beside the existing two.
      artifacts: [crates/repark-core/src/session.rs, crates/repark-python/src/session_write_options.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Refusals compare Spark's class plus the full message head on eager paths and the lazy message on execution paths; the four flipped pins now assert honored bytes with their Spark keys.
      artifacts: [python/repark/tests/test_text_write_timestamp_zone_1.py, python/repark/tests/test_r2_read_formats2.py]
    - id: AT-10
      status: ATTACKED
      evidence: M1 forces the builder zone to UTC and 71 zone pins red while pure NTZ/DATE pins stay green, proving the zone legs are load-bearing and the zone-free legs are independent; revert is clean and green.
      artifacts: [crates/repark-core/src/session/text_write_format/select.rs, python/repark/tests/test_text_write_timestamp_zone_1.py]
  complete: true
```

## Residues

- R-1 (tzdata, read-consistent): post-2100 DST labels follow the engine-wide tzdata
  (`chrono-tz 0.10.4` via Arrow), which disagrees with Java for far-future rules
  (NY 2101-06 renders `-05:00`, Lord_Howe `+11:00`). Reads (`CAST`, `date_format`,
  `HOUR`) render the same wall, and the explicit offset keeps the instant
  round-tripping. Fixing needs a dependency bump, not a formatter change. Cells:
  `w0 NY|csv|post2100`, `NY|json|post2100`, Lord_Howe post2100.
- R-2 (divergence, loud): zone-name patterns (`v`, `z` counts 1–4) refuse with
  `WITH_SUGGESTION` where Spark renders JRE locale names. Names need CLDR data a
  pure-Rust engine cannot reproduce byte-exactly; refusing beats guessing.
  Pinned as `_divergence` legs. Cells: `w1 csv-ts-13/43`, `json-ts-13/43`,
  `w2 utc-ts-09`, `ny-ts-31/63/64`, `kolkata-old-08`.
- R-3 (sink, pre-existing): empty strings stay unquoted in CSV (Arrow writer);
  timestamp map keys fail JSON writes (Arrow utf8-keys-only); struct columns
  stay rejected in CSV (Arrow). All identical at base, including non-temporal
  frames. Cells: `w3 emptystr/ts-17/date-04`, `w4 misc-04`, `w2 ny-map-*`,
  `w0 csv|nested`.
- R-4 (untouched paths): csv-only options on JSON writes, hive-partitioned
  writes, and `INTERVAL id HOURS` parsing keep their base behavior (refusals /
  parse error). Cells: `w0 json|noheader/nullvalue/quoteall`, `partts`, `multi`.
- R-5 (JSON inference, pre-existing): a JSON read-back infers the written
  timestamp as STRING (Spark infers timestamp); `CAST` recovers the instant.
  Pinned as string-equality plus cast.

## Neighbours

25 CSV/JSON read/write statements (`/tmp/neighbours.py`), run at base
(`5fb38051`) and head in `America/New_York`, compared with random part names
normalized and multi-part rows compared as sets (part order is
nondeterministic at both). One real change: `n22_json_ts_default`,
`2024-06-15T16:34:56.789Z` → `2024-06-15T12:34:56.789-04:00` — the fix itself.
Unchanged: non-temporal CSV/JSON bytes (header, no-header, nullValue,
quoteAll, doubles, booleans, decimals, special chars, empty frame, modes,
multi-row), DATE/NTZ defaults, nested JSON, parquet temporal round-trip,
CSV/JSON reads (including the read-`timestampFormat` refusal), partitioned
CSV layout, and text writes. A first run caught one genuine flip —
`n17_parquet_temporal` stored strings, because the Python SELECT wrapper ran
for every format; the wrapper is now CSV/JSON-only (Rust already branched)
and `test_parquet_temporal_round_trip_keeps_types` pins it.

## Evidence

- Oracle probes and outputs (lane-local, outside the repo): `/tmp/xcsvts-step0/probe0.py`
  through `probe7.py`, `w0-spark.json` through `w7-spark.json` plus `w0-repark.json`.
- End-to-end replay harness (lane-local): `/tmp/replay_tw.py`; last full run
  `/tmp/replay_out.txt`: 562 of 616 cells byte-or-class exact, 54 classified
  (8 pre-existing parse gaps, 12 untouched option refusals, 4 pre-existing sink
  rejections, 8 pre-existing partition refusals, 4 R-1, 9 R-2, 7 R-3 quoting,
  2 R-3 map keys), zero flips.
- Neighbour base-vs-head harness (lane-local): `/tmp/neighbours.py`.
