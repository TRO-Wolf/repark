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
`repark_write_format_text` UDF carrying the three user patterns plus the raw session
zone id; the UDF canonicalizes the zone for offsets, resolves the Java display id for
`VV`, and renders each value (`render.rs`) while the sink writes plain strings.
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
| C-001 | CSV and JSON LTZ bytes equal Spark's default `timestampFormat` in the session zone (gap, overlap, frac 0/3/6, pre-1900 LMT seconds, every probe zone); NTZ and DATE render zone-free defaults; struct/array nesting formats in JSON; a same-zone read-back returns the original values. | Facade bytes cells + read-back legs in `test_text_write_timestamp_zone_1.py` over the committed fixture. | PROVEN | 207 passed (199 fixture cells incl. 4-zone matrix, verifier-fold cells, CSV/JSON read-back, LEGACY legs, s3a). Full 616-cell oracle replay: 562 byte-or-class exact, 54 classified with zero flips. |
| C-002 | User `timestampFormat` / `timestampNTZFormat` / `dateFormat` are honored exactly as Spark honors them (per-kind semantics, cross-kind ignored), on CSV and JSON, local and s3a. | Facade user-format cells + the three flipped R2 honored pins + the Rust honored pin. | PROVEN | Same 182 green; `test_r2_read_formats2.py` honored trio green; `temporal_write_options_are_honored_on_csv_path_write` green. |
| C-003 | Unsupported patterns refuse with Spark's error class, never silently: eager `INVALID_DATETIME_PATTERN.*` / `DATETIME_*` classes per kind, lazy `Unsupported field` / `Unable to extract ZoneId` / structural messages at execution. | Facade error-token cells + Rust validator unit pins. | PROVEN | 25 error-token cells green; Rust validator suite green (every class per kind). |
| C-004 | The Rust compiler, validator, renderer and SELECT builder match the oracle cell by cell, including the offset-letter matrix with LMT seconds, the `''` quote escape, section skip, and the star fast path / partition skip / eager rejection of the builder. | `session::tests::text_write_format` unit pins. | PROVEN | `cargo test -p repark-core --lib session::tests::text_write_format`: 52 passed (verifier fold adds quote runs, year width/sign, trailing-`]` class, backslash escape, case twins). |
| C-005 | The s3a route formats identically (moto, fake creds only) and read-back round-trips the instant. | The moto s3a leg + the CSV/JSON read-back legs. | PROVEN | `test_s3a_csv_write_matches_spark` green; CSV read-back returns the instant, JSON keeps the bytes and `CAST` recovers it. |
| C-006 | Nothing else changes: parquet/ORC/Iceberg writes, non-temporal CSV/JSON bytes, the CSV header default, and every read path are untouched; the 25-statement neighbour diff reports no unintended change; the only pin flips are the four mandated refusal-to-honored flips. | Neighbour base-vs-head diff + the flip census + the untouched-path suites. | PROVEN | 25-statement base-vs-head diff: 1 change, the intended `n22` zone wall; multi-part order and random names normalized. Flip census: 3 R2 + 1 Rust refusal pins flipped (all mandated); 0 UTC-wall asserts; 0 unexpected. Temporal + csv/json suites green (237, 459, 801 passed). |

## Mutation record (2026-09-29)

| # | Mutation | Red |
|---|---|---|
| M1 | Force the SELECT builder zone to UTC (`zone = "UTC"`) | 71 facade pins red: every non-UTC LTZ bytes cell plus the CSV/JSON read-back and s3a legs; pure NTZ (`ntz0/3/6`) and DATE (`date`) cells stay green; `userdate`/`userntz` red only via their `t` column. Reverted; `git status` clean; 182 green. |
| M2 | Restore the undoubled splice (`quote_literal` doubles quotes only) | The 3 facade VC-6 pins (`vc6-bsquote` ×2, `vc6-bspair`) plus the Rust `select_escapes_backslash_quote_in_pattern_literal` pin red (quote-adjacent backslash breaks the `COPY` literal again). Reverted; `git status` clean; 207 green. |

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
      evidence: CSV and JSON doors, local and s3a routes, LTZ/NTZ/DATE value kinds, struct/list/map recursion, partition-column exclusion, case-duplicate temporal columns all wrapped (quoted identifiers resolve case-sensitively; verifier fold VC-8); parquet/ORC/Iceberg and reads verified untouched by the neighbour diff.
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
      evidence: The s3a leg runs against moto with fake credentials only; no AWS access, no secret in output; zone ids ride quoted identifiers and patterns ride string literals that double every backslash before doubling quotes (the COPY tokenizer passes backslash pairs verbatim and treats backslash-quote as an escape, so undoubled input would break the literal; the UDF halves the pairs back — ordinal-probed round-trip, verifier fold VC-6).
      artifacts: [python/repark/tests/test_text_write_timestamp_zone_1.py]
    - id: AT-6
      status: ATTACKED
      evidence: Written bytes round-trip the instant on same-zone read-back (CSV infers timestamp; JSON keeps the string and CAST recovers it); the s3a object bytes equal the local bytes. Explicit-schema CSV read-back of LMT offset-seconds refuses loud (Arrow parser gap, residue R-6, card CSV-READ-OFFSET-SECONDS-1; verifier fold VC-7).
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
- R-6 (CSV read, carded CSV-READ-OFFSET-SECONDS-1): an explicit-schema CSV
  read-back of a written LMT offset with seconds (`-04:56:02`) refuses loud
  (the Arrow timestamp parser rejects `±HH:MM:SS`); infer-schema and JSON
  schema reads work. The writer's bytes equal Spark's; only the schema-read
  leg cannot reingest them. Accepting seconds needs a read-path cast the fold
  scoped out (>60 lines). Pinned as the loud refusal.
- R-7 (LEGACY policy, carded TEXT-WRITE-LEGACY-POLICY-1): CSV/JSON writes
  carrying temporal columns or temporal options refuse loud under
  `spark.sql.legacy.timeParserPolicy=LEGACY` (legacy `SimpleDateFormat` /
  hybrid-calendar rendering is not implemented); non-temporal writes proceed.
  Pinned as the refusal plus the inert leg.

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

## DIFF-PROBE fold (2026-09-29, Muse worker lane `/tmp/xcsvts`)

**P1M-JSON (performance): closes with no product change.** Re-measured on
release builds per the fold brief (`maturin develop --release` with
`CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16` in `/tmp/xrel` at `adc26586` and at
head `784f3667`; debug `.so` files backed up and restored after; 3 fresh
processes per side, each running the probe's `do_perf` shape for CSV, JSON
and parquet in order; 1M rows, `America/New_York`). Release bytes and first
shas match the debug report exactly, so the harness replays the same writes.
`write_ms` per sample:

| Side | CSV | JSON | Parquet |
|---|---|---|---|
| Base release | 114.4 / 127.1 / 115.3 | 104.2 / 77.9 / 103.4 | 77.1 / 65.3 / 62.6 |
| Head release | 81.3 / 93.6 / 92.8 | 101.4 / 99.4 / 108.0 | 60.5 / 74.9 / 56.0 |
| Ratio head/base (means) | 0.75x | 1.08x | 0.93x |
| Ratio head/base (medians) | 0.80x | 0.98x | 0.93x |

JSON is 1.08x on means and 0.98x on medians, under the 1.2x line; the
1.32x/1.34x debug ratio was a debug-build artifact. CSV head is faster than
base (pre-formatted strings skip the writer's timestamp path), and the
parquet control sits at 0.93x, bounding harness noise near +/-10%. No
formatter change was made; the UDF keeps per-value rendering. Lane-local
harness: `/tmp/perfrel.py`, outputs under `/tmp/perfrel/`.

**W180 (empty `timestampFormat` writes unquoted empty): HALT, needs a ruling.**
Spark (NP07, live 4.1.2) writes `id,t\n1,""\n`; head writes `id,t\n1,\n`;
base refused the option. The audit found no targeted fix at the formatting
layer:

- The UDF renders every non-null value under the empty pattern as a non-null
  empty string; the Arrow sink (`csv-core` `QuoteStyle::Necessary`) never
  quotes an empty field (`needs_quotes` is false on zero bytes), and the only
  other styles quote the wrong cells (`Always` would quote `1`; `NonNumeric`
  would quote every plain string, flipping NP02). There is no per-value
  quoting knob; the sink maps `{NULL -> "", "" -> ""}`.
- Byte post-processing cannot recover the distinction: NULL and `""` leave
  identical bytes (proven: `SELECT '' AS s` and `SELECT CAST(NULL AS
  TIMESTAMP)` both write `,1`; MIX3 under the empty pattern writes three
  identical `N,` rows where Spark wants `1,""`, `2,`, `3,""` per NP07+NQ01).
  A correct rewrite would need row values aligned to part files across the
  local and s3a routes, compression, partitions and CSV options.
- Blast radius of any sink change: plain empty strings (NQ02: Spark `""`,
  both sides unquoted) share the residue R-3; JSON already matches Spark
  (`{"id":1,"t":""}`); RePark reads `1,""` and `1,` back identically
  (`t=None`), so the divergence is write-bytes only.

Recommended disposition: dispose as a divergence — pin the current bytes with
the NP07 Spark key beside the R-3 residue (the test module docstring already
records "empty strings stay unquoted in CSV") — or card a sink-level fix (a
CSV writer that quotes empty-but-not-null). The fold lane committed no
product change; the W180 pin and the full DIFF-PROBE replay ride with the
ruling lane.

**W180 ruling (orchestrator, 2026-09-29):** W180 is disposed as part of the
pre-existing residue R-3 (the CSV sink leaves empty strings unquoted). No pin
asserts the wrong bytes. The sink-level fix, a CSV writer that quotes empty
but non-null fields on the local and s3a routes, is carded as
CSV-EMPTY-QUOTE-1 for v1.5.2. It covers NQ02 and NP07 together.

## Opus verifier fold (2026-09-29, Muse worker lane `/tmp/xcsvts`)

Folded findings VC-1..VC-9 from `verify-csvts-opus-handback.json` (Opus
verifier, ~1470-cell harness). Oracle-first: saved `out/*-spark.json` cells
plus live Spark 4.1.2 probes for the gaps (trailing-`]` classes, backslash
runs) and jshell probes against `DateTimeFormatter`, `SimpleDateFormat`, and
`ZoneId.of(id, SHORT_IDS)`.

- VC-1 (quotes): the scan and tokenizer now port Java's literal scan (on `'`,
  scan to the closing quote treating `''` as escape; empty content yields one
  quote). The finding's `8 quotes -> 2` example is a typo: the oracle says 3
  across all three kinds (`p2*39`), and the port renders 3. Pins: 4/6/8-run
  and mixed cells.
- VC-2 (`y` width): runs past 6 refuse (RECOGNITION for LTZ/DATE,
  WITH_SUGGESTION for NTZ); 6 stays accepted. Pins per kind.
- VC-3 (years): `y` renders the signed proleptic year (`-0001`, `00`,
  `+10000`) unless an unquoted `G` selects year-of-era (`BC 0001`).
  Pins from the `ext` oracle rows.
- VC-4 (`VV`): the UDF takes the raw zone and resolves
  `ZoneId.of(raw, SHORT_IDS).getId()` for `VV` (offsets/walls stay canonical).
  The full `SHORT_IDS` table is jshell-verified, including the tzdb-region
  overrides (`EST5EDT` stays verbatim). PST-as-session-zone still refuses at
  `conf.set` (pre-existing, identical at base; not a VC finding).
- VC-5 (LEGACY): `spark.sql.legacy.timeParserPolicy` is a real session knob
  (lazily installed carrier + runtime set/unset + facade forward, default
  `EXCEPTION`; builder-seeded values are read back from the conf dump, since a
  builder install has no room under the `lib.rs`/`session.rs` ceilings);
  temporal-bearing CSV/JSON writes refuse loud under `LEGACY`. The LEGACY
  formatter is carded as TEXT-WRITE-LEGACY-POLICY-1 (residue R-7).
- VC-6 (backslash splice): literals double every backslash before doubling
  quotes and the UDF halves the pairs back. The COPY tokenizer passes `\\`
  through verbatim and treats `\'` as an escaped quote (ordinal-probed), so
  pure quote-doubling broke quote-adjacent backslashes; the pair round-trips
  every shape 1:1. The finding's Expr suggestion was weighed and declined:
  `COPY` executes as one SQL string on both routes, so an Expr projection
  would replace the commit machinery for no byte gain. Pins: quote-backslash
  and backslash-pair cells on CSV and JSON.
- VC-7 (offset-seconds read): scoped out as residue R-6
  (card CSV-READ-OFFSET-SECONDS-1); the round-trip leg pins the loud refusal.
- VC-8 (case twins): the case-folded uniqueness skip is gone; every temporal
  column wraps. Pinned `T,t` bytes cell.
- VC-9 (trailing `]`): LTZ/DATE take RECOGNITION when every letter is a
  legacy `SimpleDateFormat` letter (jshell-measured set; `xxx]` stays
  WITH_SUGGESTION / lazy-`]`), NTZ downgrades (live-probed). Pins per kind.

AT-2, AT-5, AT-6 evidence updated for the fold; C-001 counts 207 facade pins
and C-004 counts 52 Rust pins. The splice-restore mutation is recorded as M2
above: the VC-6 pins red.

## Re-verify fold VC2-1..VC2-7 (2026-09-29, Muse worker lane `/tmp/xcsvts`)

Folded re-verify findings VC2-1..VC2-7 from
`reverify-csvts-opus-handback.json` (live Spark 4.1.2 oracle,
4700+ cells). Commit 4f6d8216.

- VC2-1 (`VV` zero offsets): `java_display_zone_id` returns the bare
  prefix when the normalized offset is `+00:00` or `-00:00`, matching
  `ZoneId.of` with-prefix display. Pinned all five measured spellings
  (`UTC+0`, `UTC+00:00`, `UTC-00:00`, `GMT+0`, `GMT-0`) as Rust table
  rows plus `VV` and full-pattern facade cells.
- VC2-2 (`g` padding): Modified Julian Day renders sign plus
  zero-padded absolute value at minimum width = letter count
  (`SignStyle.NORMAL`), with no truncation past the width. Pinned
  `g` through `gggggg` on MJD 3, -7, 7715, 60374 in Rust and as
  24 facade cells.
- VC2-3 (formatter speed): measured release on varied data first
  (5.5-11.7x), then applied R1: an offset-interval cache (6-day
  probe steps under the measured 597600s minimum transition gap of
  the bundled chrono-tz 0.10.4 tables; transitions span 1844-2099),
  a reused buffer with no per-value `String`, and a table-driven
  default renderer (`fast.rs`) with day cache, precomputed offset
  suffix, and small-delta carry. Byte-identity pinned by a
  scalar-vs-fast differential test (7 zones, transitions, era/year
  boundaries, extremes, both directions) plus unchanged checksums.
- VC2-4 (policy default): the facade default and the Rust default
  both read `CORRECTED`, as Spark 4.1.2 reports. No refusal or write
  changes: only `is_legacy` is ever read. The `_SQLCONF_DEFAULTS`
  symbol hash is re-baselined. Pinned get/unset/write.
- VC2-5 (EST/MST/HST tzdb rules): already at base; carried.
- VC2-6 (pattern message): the RECOGNITION text keeps its error
  class but drops the LEGACY clause; the Rust message helper and
  pins follow.
- VC2-7 (LEGACY scope): under LEGACY the write refuses only when
  the frame carries temporal columns; inert options on a
  non-temporal frame write. Pinned.

Release perf on varied 1M timestamps (median of 15, both release,
verifier harness; raw JSON under
`/tmp/oc-worker/direct/wo/reverify-csvts-fold/`):

| shape | head (s) | base (s) | ratio |
|---|---|---|---|
| ctl_parquet_many | 0.150 | 0.150 | 1.00x |
| ctl_csv_plain | 0.053 | 0.053 | 0.99x |
| ctl_json_plain | 0.066 | 0.065 | 1.01x |
| csv_one_default | 0.154 | 0.119 | 1.28x |
| csv_many_default | 0.402 | 0.218 | 1.85x |
| json_one_default | 0.121 | 0.091 | 1.33x |
| json_many_default | 0.404 | 0.263 | 1.53x |

Every default shape stays over the 1.2x bar (pattern shapes have no
base: base refuses the options). UDF-level profile: 21ns loop+append,
19ns resolve-hit, scalar render 250ns, fast render 24-58ns per
column shape. The read plus string-write floor leaves csv_many about
14ns/value of UDF budget against a 25ns append+civil floor, so the
bar needs a structural change (no string materialization), not more
UDF tuning. Handed back HALT with the numbers.
