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
| C-007 | A failed s3a CSV/JSON write leaves the destination key set unchanged: the commit deletes exactly the output paths the write's own sink recorded (plus a materialized empty part) on failure, over empty, appended and partitioned prefixes; the original error is preserved and a cleanup failure is appended to it. | `session::tests::path_write` rollback pins (400k-row zone-letters-on-NTZ render failure) + the rollback-helper unit pins. | PROVEN | `failed_render_overwrite_into_empty_leaves_no_objects`, `failed_render_append_preserves_existing_objects`, `failed_render_partitioned_overwrite_leaves_no_objects` green; recorded-delete/note unit pins green; cleanup-removal mutation reds the three pins. |
| C-008 | A failed local CSV/JSON write leaves the destination unchanged: the staged COPY is removed and overwrite leaves no destination while append keeps every destination byte, including partitioned layouts. | Facade destination-snapshot legs over the 400k-row zone-letters-on-NTZ render failure. | PROVEN | `test_failed_local_overwrite_leaves_no_destination`, `test_failed_local_append_preserves_destination`, `test_failed_local_partitioned_overwrite_leaves_no_destination` green. |
| C-009 | Objects a concurrent writer commits during a failed s3a write's window survive: keys PUT under the same prefix, a sibling prefix and an unrelated prefix while a 400k-row JSON append fails keep their bytes, and a failing append at the bucket root keeps a foreign prefix intact while the write's own parts are gone. | In-memory S3 concurrent pins beside the C-007 rollback pins. | PROVEN | `failed_render_append_leaves_concurrent_objects_alone`, `failed_render_root_append_leaves_foreign_prefixes_alone` green; the M10 listing-delete mutation reds both. |

## Mutation record (2026-09-29)

| # | Mutation | Red |
|---|---|---|
| M1 | Force the SELECT builder zone to UTC (`zone = "UTC"`) | 71 facade pins red: every non-UTC LTZ bytes cell plus the CSV/JSON read-back and s3a legs; pure NTZ (`ntz0/3/6`) and DATE (`date`) cells stay green; `userdate`/`userntz` red only via their `t` column. Reverted; `git status` clean; 182 green. |
| M2 | Restore the undoubled splice (`quote_literal` doubles quotes only) | The 3 facade VC-6 pins (`vc6-bsquote` ×2, `vc6-bspair`) plus the Rust `select_escapes_backslash_quote_in_pattern_literal` pin red (quote-adjacent backslash breaks the `COPY` literal again). Reverted; `git status` clean; 207 green. |
| M3 | VC2-1: return the prefixed offset (`UTC+00:00`) instead of the bare prefix on normalized zero offsets | The Rust `java_display_zone_id_matches_zone_id_get_id` table pin reds (18 pass, 1 fails). Reverted; `git status` clean. |
| M4 | VC2-2: render `g` as a bare day count with no sign/zero padding | The Rust `modified_julian_day_pads_to_letter_count` pin reds (55 pass, 1 fails). Reverted; `git status` clean. |
| M5 | VC2-4: flip the facade `_SQLCONF_DEFAULTS` policy back to `LEGACY` | The facade `test_time_parser_policy_default_is_corrected` pin reds. Reverted; pin green. |
| M6 | VC2-6: replace the RECOGNITION guide clause with a `LEGACY` stub | 7 Rust recognition pins red (49 pass, 7 fail). Reverted; `git status` clean. |
| M7 | VC2-7: refuse LEGACY writes even with no temporal column (`temporal_columns \|\| true`) | Blind at the Rust level (56 pass: no Rust pin covers the non-temporal LEGACY leg); the 2 facade non-temporal LEGACY pins red after a venv rebuild. Reverted and rebuilt; pins green; `git status` clean. |
| M8 | VC3-1/VC3-2: restore the exact 6-day step search (`probe`/`bound_above`/`bound_below`, 12288 budget) | The all-zones `cached_offsets_match_direct_lookups_in_every_zone` pin reds on the first zone (Africa/Abidjan): the offset asserts stay green, the 15-minute window pin fails. Reverted; `git status` clean; 3/3 cache pins green. |
| M9 | VC4-1: replace the rollback call in `commit_s3_write` with a no-op | All three C-007 s3a pins red (partial objects survive the failed write). Reverted; 6/6 rollback pins green. |
| M10 | VC5-1: replace the recorded-paths deletion in `commit_s3_write_text` with a list-and-delete-everything rollback | Both C-009 concurrent pins red (concurrent foreign keys deleted) plus the C-007 append pin. Reverted; 5/5 failed-render pins green. |

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

Replay against the reverify-csvts Spark oracle (new-head outputs under
`/tmp/rerun/out/`, conf legs re-captured under `/tmp/conf-rerun/` and
byte-identical to the first rerun): 5054 cells (4968 cellset, 29
doors, 8 doors2, 49 conf) — zero cells moved away from Spark. Fixed
toward Spark: 10 zones2 `VV` zero-offset cells, 3 letters `g` cells,
and 8 conf cells (6 runtime get-cells plus `builder_then_unset_get`
on both builder legs). The verifier `out/` tree is untouched: conf
and doors data dirs were re-verified byte-identical to their backups
after the reruns. Mutations M3-M7 (table above) each red the pins of
their fix and revert clean. The temporary release probes left the
tree (commit e75232ab); lane gate green; lane `.venv` restored to a
debug build; base worktree removed.

## Re-verify 2 fold VC3-1..VC3-2 (2026-09-30, Muse worker lane `/tmp/xcsvts`)

Folded re-verify-2 findings VC3-1 (S1) and VC3-2 (S3) from
`reverify2-csvts-opus-handback.json`. Commits 301dd4e6 (product, tests,
maps) and c058b900 (adjacency follow-up).

- VC3-1 (shuffled cliff): the offset cache no longer walks in steps. A
  miss resolves the instant directly through chrono-tz, once, and caches
  a 15-minute window around it only after proving both ends hold the
  same offset; an unproven window caches nothing. The proof itself runs
  only when the value lands within one window of the previous value
  (c058b900): scattered misses on shuffled data skip a proof they cannot
  amortize, which moved the release 20k shape from 2.0x to 1.4x base.
  UTC and fixed-offset session zones resolve once per call with no zone
  lookup; other zones share one cache per session zone, held on the UDF
  struct and parked between calls. Default columns build in one
  offsets-plus-values buffer validated once per batch; the pattern path
  computes `epoch_day` once per value and `has_era` once per pattern.
- VC3-2 (step assumption): the 6-day search is deleted with its map.md
  guard prose. The new `text_write_format_cache` suite runs all 597
  bundled zones over every transition found by a 3-day scan of
  1840-2100 (exact instants plus neighbours, each queried after warming
  days away on both sides) plus sampled years 1-9999: cached offsets
  equal direct lookups every time, and every cached window holds at or
  under 15 minutes.
- Carried: VC3-3 (S3, also on base — dictionary-typed timestamp columns
  skip the formatter; the orchestrator files the card) and R-1 (years at
  or past 2100 in DST zones).

Shuffled data, debug (verifier harness; base-debug numbers recorded by
the round-2 verifier on adc26586, one test-only commit before the
5fb38051 branch point; base ignores the session zone, so the NY base leg
stands in where the verifier recorded NY only):

| shape | head | base | ratio | old head |
|---|---|---|---|---|
| vc31 20k NY | 0.11 s | 0.09 s | 1.2x | 2.44 s |
| wide 2k UTC / NY | 0.029 / 0.040 s | 0.021 / 0.030 s | 1.4x / 1.3x | 11.2 / 16.3 s |
| wide 20k UTC / NY | 0.098 / 0.117 s | 0.096 / 0.096 s | 1.0x / 1.2x | 112 / 164 s |
| birth/wide/modern 200k NY | 0.41 / 0.37 / 0.33 s | 0.32 / 0.28 / 0.33 s | 1.3x / 1.3x / 1.0x | 8.2 s / timeout / 0.33 s |
| birth/wide/modern 200k UTC | 0.28 / 0.27 / 0.31 s | 0.32 / 0.28 / 0.34 s | 0.9x / 1.0x / 0.9x | timeout / timeout / 0.31 s |

Shuffled data, release (both release,
`CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16`; base worktree
`/tmp/xcsvts-base` at 5fb38051, removed after):

| shape | head | base | ratio |
|---|---|---|---|
| vc31 20k NY / UTC | 0.011 / 0.009 s | 0.008 / 0.008 s | 1.4x / 1.1x |
| wide 2k/20k UTC | 0.004 / 0.008 s | 0.004 / 0.010 s | 1.0x / 0.8x |
| wide 2k/20k NY | 0.004 / 0.010 s | 0.004 / 0.009 s | 1.0x / 1.1x |
| birth/wide/modern 200k NY | 0.052 / 0.046 / 0.040 s | 0.042 / 0.041 / 0.036 s | 1.2x / 1.1x / 1.1x |
| birth/wide/modern 200k UTC | 0.038 / 0.036 / 0.035 s | 0.039 / 0.043 / 0.045 s | 1.0x / 0.9x / 0.8x |

Every shuffled shape sits at or under 1.4x base in both profiles; the old
head ran 26x to 1700x on the same shapes.

Default shapes, release, median of 15 (verifier 1M harness):

| shape | head (s) | base (s) | ratio | last fold |
|---|---|---|---|---|
| csv_one_default | 0.161 | 0.119 | 1.35x | 1.28x |
| csv_many_default | 0.424 | 0.238 | 1.78x | 1.85x |
| json_one_default | 0.127 | 0.101 | 1.26x | 1.33x |
| json_many_default | 0.422 | 0.242 | 1.74x | 1.53x |

Ratios match the last fold within run-to-run noise (base itself moves
about 15% across days under shared-machine load; an interleaved re-run
put head absolutes under the last fold's head absolutes on both many
shapes). Time-ordered debug shapes match aa5f563b exactly (modern NY
0.329 vs 0.33 s, modern UTC 0.311 vs 0.31 s).

Replay of `reverify2-csvts/` (which replays every `reverify-csvts/`
cell: probe and canon identical, cell defs purely additive) on the debug
lane build: 5194 cells (4968 replay + 226 fast) plus conf (3 modes),
doors, doors2, g and zchg probes — zero cells moved away from Spark,
zero changed at all. Three harness artifacts, each confirmed not a
behavior change: the `rt-spark-*` readbacks need the verifier's
`*-spark` symlinks (mirrored; the verifier hit the same 4 false moves);
`surf/json_basic` shows the documented nondeterministic JSON-infer
column-order flake (abf7bb42 agrees with the new head; aa5f563b flipped
without any read-path change); `surf/csv_basic` and `doors` embed the
harness out-dir path in readback errors (identical modulo the path).
`fastrand` (21.5 min on the old head) finishes in seconds.

Pins: the all-zones Rust suite plus a `perf`-marked 20k shuffled pin
with a 1.0 s debug budget (measured 0.11 s; the old head needed 2.44 s).
Mutation M8 (table above) restores the exact 6-day step search: the
all-zones suite reds on the first zone (behavioral asserts stay green —
6-day steps are correct on the bundled tables; the 15-minute window pin
is the structural killer). Reverted; `git status` clean. Lane gate
green; lane `.venv` restored to a debug build; base worktree removed.

Sink-format round (2026-09-30, lane /tmp/xcsvts, branch
fix/text-write-timestamp-zone-1): temporal formatting moves from the serial
UDF projection into the sink's parallel per-batch serializers, per the
orchestrator sketch (Q1 option 1, Q2 no interim). Route (A): two factories
(`repark_text_csv`, `repark_text_json`) registered on the session; the two
COPY builders emit the custom `STORED AS` plus the spec as `repark.text.*`
OPTIONS; the inner SELECT is plain `SELECT *`. Non-temporal frames keep
plain `STORED AS CSV`/`JSON`, so their path is untouched by construction.

Step-0 spike (committed first): five Rust pins prove in DataFusion 54.1.0
that a custom `STORED AS` resolves, custom OPTIONS reach `create` verbatim,
part files keep `.csv`/`.json` (the extension comes from the format's
`get_ext`, not the factory's), a serializer error surfaces intact, and
unstripped custom keys fail the inner factory (stripping is load-bearing).

Two-door tokenizer finding: the Spark door processes backslash escapes in
string literals while the core door passes them verbatim, so no single
literal quoting carries backslash patterns through both doors. User
patterns ride the OPTIONS as hex (`repark.text.*_format_hex`); only the
validated zone id rides as a plain literal. The vc6-bsquote facade cells
pin the round trip. Bad hex refuses loud at plan time.

Product: `spec.rs` (TextWriteSpec, option keys, hex encode/decode),
`serializer.rs` (wrapping BatchSerializer over the unchanged `udf.rs`
engine, one fresh offset cache per call, partition columns skipped by
lowercased name), `sink.rs` (mirrors CsvSink/JsonSink), `file_format.rs`
(delegating format plus factory plus session registration), `select.rs`
rebuilt around `text_write_copy_parts` (same eager validation and LEGACY
refusal, same refusal order). The `repark_write_format_text` UDF shell,
its registration and the SELECT wrapper are removed: grep found no other
caller. One-line `bytes.workspace` addition to repark-core (workspace pin,
already in the lock; the BatchSerializer signature needs the type).

Correctness proof: 10 new Rust sink pins (zone bytes csv/json, user
patterns, raw partition directory names, lazy/eager error identity, empty
and all-null frames, keep-partition native pass-through against plain
CSV), the rewritten COPY-parts pins, all 245 facade pins green, plus the
head-vs-new byte differential, the reverify replays, the skip-a-column
mutation and the release perf table below.

Byte differential: 50/50 head-vs-new cells byte-identical, `diff -r -x
manifest.txt` RC=0. Reverify replays: rv1 1726 cells plus rv2 5199 cells,
0 real moves (2 documented output-root path artifacts in parser column
numbers). Skip-a-column mutation went red and was reverted. No
sink-surfaced error text changed, so no per-cell error listing is owed.

Release perf, CODEGEN_UNITS=16, interleaved, median of 15, seconds:

| shape | base | new | ratio |
|---|---|---|---|
| csv_one_default | 0.118 | 0.112 | 0.95x |
| csv_many_default | 0.204 | 0.193 | 0.95x |
| json_one_default | 0.095 | 0.080 | 0.84x |
| json_many_default | 0.235 | 0.207 | 0.88x |
| ctl_csv_plain | 0.055 | 0.052 | 0.96x |
| ctl_json_plain | 0.067 | 0.067 | 1.00x |
| ctl_parquet_many | 0.147 | 0.143 | 0.98x |

User-pattern shapes against head (median of 15, seconds):

| shape | head | new | ratio |
|---|---|---|---|
| csv_one_pattern | 0.426 | 0.141 | 0.33x |
| csv_many_pattern | 1.633 | 0.291 | 0.18x |
| json_one_pattern | 0.402 | 0.108 | 0.27x |
| json_many_pattern | 1.660 | 0.291 | 0.18x |

Shuffled-zone shapes against base (median of 7, ratio only): UTC 0.89x to
1.09x; non-UTC 1.04x to 1.32x, worst wide_20k_NY 1.32x and vc31_20k_NY
1.31x, all at or under the 1.4x bar.

Thread course: new build shows 16-thread CPU in the parallel serialize
region (0.10 to 0.15s, DONE 0.197s). Head shows a serial UDF phase then
the 16-thread sink (DONE 0.382s). The temporal format work moved into
the sink tasks.

`scripts/verify-repark-tokenizer.py` and `scripts/repark-python-session.py`
do not exist in this lane, so no script mapping names a stale helper.
`gate.sh` reports GATE GREEN.

Re-verify 3 fold (2026-09-30, lane /tmp/xcsvts, branch
fix/text-write-timestamp-zone-1): re-verify 3 found no output-byte change on
any successful write but filed VC4-1 (a failed s3a text write commits partial
part objects, up to whole batches plus empty parts, beside the destination)
and VC4-2 (a lazy render error quotes whichever failing batch finished
first). R1 (VC4-1): `commit_s3_write` snapshots the destination keys before
the COPY and, on any failure, deletes every key missing from the snapshot,
over every mode, partitioned layouts and empty parts; existing append objects
stay untouched. A snapshot-listing failure refuses before the COPY; a cleanup
failure is appended to the original error text with the variant preserved.
The code lives in the new `session/path_write/rollback.rs`; the
append-validation cohort moved verbatim to `session/path_write/append.rs`
under the file-size gate. VC4-2 is accepted and documented: the serialize
tasks run in parallel, so the quoted failing value is whichever batch loses
the race; 3d4f2030 was already nondeterministic here (29/30 first-row in the
verifier's 30-run probe).

Pins: three Rust s3a pins over the in-memory S3 route (400k-row JSON write,
zone letters on NTZ, one non-null NTZ at row 99999): overwrite-into-empty and
partitioned overwrite leave no objects, append preserves keys and bytes
(C-007); three local-route pins snapshot the destination tree around the same
failing write (C-008). Mutation M9 (the cleanup call replaced with a no-op):
all three s3a pins red. Reverted; pins green.

Proof continued: the last round's byte differential (50 cells: defaults,
patterns, nested, partitioned, empty, all-null, year-1, options, error cells,
six 1M-row shapes, one moto s3a cell) re-ran against `/tmp/xcsvts-head`
(3d4f2030): `diff -r -x manifest.txt` RC=0; the unexcluded diff shows only the
harness `build=` label. Overwrite order, measured over moto: on 3d4f2030 a
failing 400k-row overwrite over a seeded prefix leaves zero objects (both
seed keys gone, no partials — the projection error fires before any writer
exists); on the new build the same probe leaves zero objects too (seed gone,
partials rolled back). The old-data loss on a failed overwrite is therefore
the pre-existing delete-before-COPY order, unchanged by this fold. Replay
outputs live in `/tmp/oc-worker/direct/wo/reverify3-csvts-fold/`.

Re-verify 4 fold (2026-09-30, lane /tmp/xcsvts, branch
fix/text-write-timestamp-zone-1): re-verify 4 found the VC4-1 rollback clean
on the s3a route (24/24 failing writes leave keys and ETags unchanged, s3a
success cells identical) but filed VC5-1 (S2, new since 68125572): the
snapshot-diff rollback deletes every key missing from the snapshot, so
objects a concurrent writer committed during the write window are deleted
too (30/39 same-prefix PUTs, 29/41 bucket-wide at the bucket root). R1: a
failed CSV/JSON write deletes exactly the output paths its own sink
recorded, plus a materialized empty part, and nothing else. `ReparkTextSink`
owns a per-write `Arc<Mutex<Vec<ObjectPath>>>` collector on the sink
instance of one COPY; the `spawn_writer_tasks_and_join` override records
each demux path and forwards the stream into DataFusion's orchestration
unchanged, so success bytes and keys are identical. The s3a text commit
builds the COPY's physical plan explicitly (`task_ctx` plus
`create_physical_plan`, the same two steps `DataFrame::collect` runs) so it
can reach the sink's collector through the `DataSinkExec` node, then
executes that plan; on failure it deletes exactly the recorded paths plus
the materialized empty part. No listing-based deletion remains.
`_SUCCESS` is never deleted on failure: its PUT is the last commit step,
so a failed PUT created nothing and a surviving one belongs to another
writer. A cleanup failure appends the bare cleanup message to the original
error once, without repeating the variant prefix. Parquet is restored to
exactly 68125572's commit with no rollback, a known gap: a parquet-side
rollback needs its own sink, and parquet has no render-time failures. A
second known gap: text writes without temporal columns use DataFusion's
plain sinks rather than `ReparkTextSink`, so an IO-mid-COPY failure there
leaves partial output as at 68125572.

Pins (C-009): two Rust pins over the in-memory S3 route reuse the 400k-row
zone-letters-on-NTZ failing write: an append with keys PUT under the same
prefix, a sibling prefix and an unrelated prefix while it fails keeps every
foreign key and byte with no other new key, and a failing append at the
bucket root keeps a foreign prefix intact with no other new key. Mutation
M10 (the rollback deletes by listing again): both pins red. Reverted; pins
green. Proof outputs live in
`/tmp/oc-worker/direct/wo/reverify4-csvts-fold/`.
