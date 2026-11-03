# Unit ledger — HADOOP-DISCOVERY-RULE-1 · the hadoop path read follows Java's metadata discovery (hint read, listing, refresh walk, refusals)

**Date:** 2026-11-03 · **Branch:** `fix/hadoop-discovery-rule-1` · **Base:** `05d0437d` (origin/main when the verifiers ran) · **Head:** `5ca218f8` · **Squash:** `d2f3313e` on main (PR #1251)
**Model:** builder `claude-opus-5-5` · verifiers `claude-opus-5-5` and `muse-spark-1.3-contributor` (round 1) · records `claude-haiku-5-5`
**Policy:** [../../../AGENTS.md](../../../AGENTS.md) · **Path:** STANDARD.

**Status:** completed 2026-11-03 at `5ca218f8` (squashed to main as `d2f3313e`, PR #1251) after both verifiers. The Opus 5.5 round-1 verdict was FAIL on F1 alone; the owner ruled F1 an accepted leniency on 2026-11-03 (C-012), so the verdict counts as PASS. The Muse round-1 verdict is PASS with R1F1 (S3, test adequacy). Clauses C-001 to C-011 are PROVEN; C-012 to C-015 are OPEN leads (L1 to L4). The records pass that writes this file closes the card HADOOP-DISCOVERY-RULE-1 and the three hint cards it folds.

**Why.** On a `type=hadoop` table read by path, RePark's version-hint handling departed from Java's `HadoopTableOperations` in three places. A catalog-style `NNNNN-<uuid>.metadata.json` won the listing when no hint was readable, so a deleted row came back (D1). A negative hint was answered where Java refuses (D3). An unreadable or directory-shaped hint refused where Java lists (D2). The rule (R2 to R4 of [the rule doc](../../../docs/corpus/hadoop-discovery-rule-2026-11-03.md)) is ported into the path read in `crates/repark-core/src/iceberg_path.rs`.

**Clause.** The measured cells table ([hadoop-discovery-cells-2026-11-03.jsonl](../../../docs/corpus/hadoop-discovery-cells-2026-11-03.jsonl)): groups D1, D3 and D2 re-measured on the path read against the Spark 4.1.2 column, with the controls D4, D5, D7, D8, the checksum controls and the 818 already-EQUAL cells unmoved; the rule R2 to R4; and the case table in the Evidence section. The pin binding is in [hadoop-discovery-rule-1-pins.md](hadoop-discovery-rule-1-pins.md).

## PROPOSITION LEDGER — HADOOP-DISCOVERY-RULE-1 — 2026-11-03

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The listing considers only the `v<N>` names that Java's `VERSION_PATTERN` admits, and a candidate needs its canonical name listed and N above zero, so a catalog-style `NNNNN-<uuid>.metadata.json` never takes part in the no-hint read (D1: 84 cells, 84 of 84 EQUAL). | Table-driven pin over the path-read rows, and the listing units. | PROVEN | Pins: `path_read_cells_match_the_spark_column`, `listing_stem_follows_the_java_version_pattern`, `listing_skips_a_stem_whose_canonical_name_is_absent`, `listing_walks_from_the_newest_listed_version_only`, `catalog_style_name_never_beats_a_v_name_without_a_hint`, `catalog_style_name_never_beats_a_v_name_behind_an_unparsable_hint`. Both verifiers re-ran the 3904-cell table at the head; Opus: D1 84 of 84 byte-equal. Mutation (Opus AT-10): the listing filter in `listed_hadoop_version` set to `>= i32::MAX` turns the table pin red with 147 mismatches. |
| C-002 | A negative hint, a hinted version past the newest file, and a hinted version whose file was cleaned up each refuse, naming the missing version under the metadata directory with Java's text (D3: 126 cells; D6: 148 cells; all 274 refuse on both sides, and the version N and the body match Spark's). | Unit per shape, the table pin, and the Python pin on the hinted-missing text. | PROVEN | Pins: `negative_hint_refuses_naming_the_missing_version`, `negative_hint_reads_a_negative_version_file_like_java`, `hint_past_the_newest_refuses_naming_the_metadata_dir`, `hint_into_a_removed_version_refuses`, `hinted_missing_file_keeps_its_error_when_newer_files_exist`, `hinted_missing_version_refuses_when_only_newer_gzip_files_exist`, `hinted_metadata_file_must_exist`, and `test_load_hinted_missing_metadata_names_the_path` in the Python load-path file. The table scrubs the Spark root path to a placeholder, so the path half of the body is not compared from the table (Opus caveat). The exception class differs; see C-011. |
| C-003 | A hint that cannot be read, is a directory, is empty, is non-numeric, has a leading space, or is beyond Java's `int` falls to the listing; the hint's first line is read as Java's `readLine` reads it, so a trailing LF, CRLF or lone CR ends the number (D2: 168 of 252 EQUAL; the other 84 are name-lying fixtures under the D4 exclusion, C-009). | Unit per shape, and the table pin. | PROVEN | Pins: `unreadable_hint_falls_to_the_listing`, `hint_that_is_a_directory_falls_to_the_listing`, `hint_text_parses_like_java_read_line_then_parse_int`, `leading_space_hint_falls_to_the_listing`, `only_the_first_hint_line_is_read`, `crlf_hint_parses_and_walks_from_the_hinted_version`, `unparsable_hint_falls_back_to_listing`, `non_utf8_hint_falls_back_to_listing`, `hint_beyond_java_int_falls_back_to_listing`, `hint_below_java_int_range_falls_back_to_listing`, `hint_beyond_u64_falls_back_to_listing`, and the table pin. Muse re-ran the table with the same class buckets. |
| C-004 | Each enumerated case of the builder's table has a pin or a measured cell, except the three carried as leads or exclusions: the Unicode-digit hint (C-012), the listing I/O error (C-013) and the codec-by-name case (C-009). | The case table in the Evidence section, each case matched to its pin or cell. | PROVEN | Pins: `zero_hint_without_v0_is_a_missing_table`, `hint_zero_selects_v0`, `missing_location_reports_the_supplied_path`, `hint_walk_stops_at_the_first_missing_version`, `listing_resolves_the_highest_numbered_metadata_file`, `listing_skips_a_stem_whose_canonical_name_is_absent`, `catalog_style_name_never_beats_a_v_name_without_a_hint`, `listing_stem_follows_the_java_version_pattern`, `hint_text_parses_like_java_read_line_then_parse_int`, `leading_space_hint_falls_to_the_listing`, `negative_hint_refuses_naming_the_missing_version`, `hint_past_the_newest_refuses_naming_the_metadata_dir`, `hint_into_a_removed_version_refuses`, and the table pin. |
| C-005 | The controls are unmoved: D4 (504 cells), D5 (84), D7 (1764), D8 (84), the 40 checksum controls (15 EQUAL before and after) and the 818 already-EQUAL cells keep their class and their `repark_changed` flag on main's column; no `path_insert` or catalog cell moves. | Both verifiers re-run the pilot runner on the head wheel against the stored Spark column, and compare with main's column. | PROVEN | Pins: `path_read_cells_match_the_spark_column` covers the path-read rows. Opus: zero mismatches between the head and main's column on every control cell; Muse: identical class buckets. The append and catalog controls are measured by the runner, not pinned. |
| C-006 | The suites and gates are green at the head at the builder's counts: repark-core 1598 passed, repark-spark 3143, repark-sql 654; facade iceberg 2215 passed (192 skipped, 12 xfailed) and facade sql 1749 passed; `make rust-panic-ban` exit 0; `check_map_size.py` exit 0 with 448 maps clean. | The verifiers' runs. | PROVEN | Re-run by both verifiers (Opus section 3; Muse gate list, every exit 0). Records only; the logs are untracked. |
| C-007 | Hygiene: no code comment in the Rust or Python diff; one `Authored-By` trailer per commit; no home or scratch path, session link, account id, bucket, region or ARN in the diff; the new test files have rows in `crates/repark-core/src/iceberg_path/map.md`. | Diff inspection by both verifiers. | PROVEN | Opus section 4; Muse claim 4. Records only. |
| C-008 | Ruling Q1 (owner, 2026-11-03), widened by Opus F2: a metadata directory with no canonical `v<N>` candidate keeps the leading-integer listing, so a name the listing admits is read (a catalog-style name only, or a `v007` name with no `v7` file), where Java says the table does not exist. This is a registered accepted leniency. | The catalog-only shape is pinned; the widened shape is measured by probe. | PROVEN | Pin: `listing_resolves_the_highest_numbered_metadata_file` (two catalog-style names, the newer one read), with the dfload-1 Python pins. The widened `v007` shape reads `v007` with no hint (Opus probe `v007_alone`; Muse P6, twice); it is unpinned and carried under the ruling as Opus F2. |
| C-009 | Ruling Q2 (owner, 2026-11-03): the 84 D2 cells over a name that lies about the payload (42 gzip-named plain payloads and 42 plain-named gzip payloads, under an unreadable or directory hint) are not EQUAL and count under the D4 exclusion. RePark reads the payload and answers where Spark refuses on the codec. | The count by name form. | PROVEN | Opus `analyze.py` run: the 84 are exactly the two name forms, 42 and 42. The 472 codec-by-name path-read rows are skipped by the pin under the D4 exclusion. Records only. |
| C-010 | Ruling Q3 (owner, 2026-11-03): a CRLF hint parses as its number. `BufferedReader.readLine` strips `\r\n` and a lone `\r` (JDK 21 probes), so the rule-doc sentence that CRLF leaves `\r` was wrong. | Unit pin and the JDK probe. | PROVEN | Pin: `crlf_hint_parses_and_walks_from_the_hinted_version`. Opus probes `crlf_hint_gap` and `cr_only_hint_gap` give rows [2, 3] at the head, as Java does. The rule doc carries a dated correction under R2 (records commit). |
| C-011 | Ruling Q4 (owner, 2026-11-03): D3 and D6 both refuse with Java's message body. The compare script's 30-character class prefix keeps them REFUSE_TEXT, because Spark prints `org.apache.iceberg.exceptions.ValidationException` and RePark an `AnalysisException`; the exception class name is not a parity claim, so EQUAL_REFUSE on the pin's both-refuse reading is accepted. D5 (a zero hint) keeps RePark's not-found text and is carded. | The ruling; the body equality is C-002's pin and the table count. | PROVEN | Body equality: 126 of 126 (D3) and 148 of 148 (D6) by both verifiers. D5 card: HADOOP-HINT-ZERO-TEXT-1. Records only. |
| C-012 | Lead L1 (Opus F1; accepted leniency, owner 2026-11-03): Java's `Integer.parseInt` accepts Unicode decimal digits, so a hint written in a non-ASCII digit form parses in Java and falls to the listing in RePark, which can then read a different file where the hinted version has a gap after it. No Java or RePark writer produces such a hint. | Closing question: port the radix-10 digit classes, or keep the leniency registered in the v2 label draft's exclusions. | OPEN | Card HADOOP-HINT-UNICODE-DIGITS-1. Opus probe `unicode_digit_hint_gap` (metadata v1, v2, v3 and v5; hint of the Arabic-Indic two): RePark rows [3, 4]; Java reads v2 and walks to v3, rows [2, 3] (JDK 21 probe). Not in the cells table. |
| C-013 | Lead L2 (Opus F3; Muse P16): with no readable hint and a `metadata/` directory that cannot be listed, RePark surfaces the directory error. The verifiers disagree on Java: the Opus source reading takes version 0 and reads `v0` when it exists, while the Muse run reads a `RuntimeIOException` that refuses. Neither is a wrong read. | Closing question: a Hadoop run on the same shape; then port the version-0 catch, or carry the text as a refusal card. | OPEN | Card HADOOP-METADATA-DIR-UNLISTABLE-1. Opus probe `listing_io_error_exec_only` gives "Failed to read directory"; Muse P16 and P16b give a permission error. Neither verifier ran Hadoop. |
| C-014 | Lead L3 (Muse R1F1, S3, test adequacy): the version-0 arm of the listing filter in `listed_hadoop_version` has no pin. With the filter changed from `>= 0` to `> 0`, the path suite stays green (103 passed); the head reads `v0` correctly. | Closing question: a pin on a no-hint `metadata/` holding `v0.metadata.json` and a catalog-style file with a different payload, at the next touch of `iceberg_path.rs`. | OPEN | Muse mutant run (untracked log). No card: a test-adequacy lead for the next touch of `iceberg_path.rs`. |
| C-015 | Lead L4 (Opus note b): the walk-limit refusal text, which names `version-hint.text`, is now reachable after a listed version as well as after a hint. A wording point inside an unmeasured 10,000-file bound. | Closing question: whether the text should name the listing when no hint exists. | OPEN | Opus note on `hint_walk_limit_error`. No card. |

## Evidence

**Verifier verdicts.** Opus 5.5 (`claude-opus-5-5`), round 1: VERDICT FAIL on F1 alone, with F2 and F3 as findings and a note. The owner ruled F1 an accepted leniency on 2026-11-03 (C-012), so the verdict counts as PASS. Muse (`muse-spark-1.3-contributor`), round 1: CONCLUDED, PASS, with R1F1. Both reports are untracked.

**Gates, as the verifiers ran them at `5ca218f8`.** `cargo test --locked -p repark-core` 1598 passed, 0 failed, 2 ignored; `-p repark-spark` 3143 passed, 0 failed, 5 ignored; `-p repark-sql` 654 passed, 0 failed, 4 ignored; `pytest python/repark/tests/iceberg` 2215 passed, 192 skipped, 12 xfailed; `pytest python/repark/tests/sql` 1749 passed, 1 skipped. `make rust-panic-ban`, `make rust-clippy`, `make check-map-size`, the map sync check and `check_rust_file_size` exit 0. The table re-run: 3904 cells at the head, none missing; the class buckets equal the builder's.

**Case table** (Java: `HadoopTableOperations` in apache-iceberg-1.11.0, lines 59, 102 to 127, 237 to 255, 274 to 285 and 313 to 350; RePark at the head).

| Case | Java | RePark at the head | Pin or measure |
|---|---|---|---|
| Hint missing | open throws, listing | listing | corpus cells, hint missing rows |
| Hint empty | readLine null, caught, listing | listing | `hint_text_parses_like_java_read_line_then_parse_int` |
| Hint non-numeric | parseInt throws, listing | listing | corpus cells; `catalog_style_name_never_beats_a_v_name_behind_an_unparsable_hint` |
| Hint leading space | NumberFormatException, listing | listing (no trim) | `leading_space_hint_falls_to_the_listing` |
| Hint CRLF or lone CR | readLine strips, parses | parses | `crlf_hint_parses_and_walks_from_the_hinted_version` (C-010) |
| Hint with more lines | first line only | first line only | `only_the_first_hint_line_is_read` |
| Hint beyond int; the int minimum | NFE beyond; minimum parses | listing beyond; minimum parses | `hint_beyond_java_int_falls_back_to_listing`, `hint_below_java_int_range_falls_back_to_listing` |
| Hint unreadable (mode 000) | open throws, listing | listing (D2) | `unreadable_hint_falls_to_the_listing` |
| Hint is a directory | read throws, listing | listing (D2) | `hint_that_is_a_directory_falls_to_the_listing` |
| Hint zero, no v0 | table not found | RePark not-found text (D5, both refuse) | `zero_hint_without_v0_is_a_missing_table` |
| Hint zero with v0 | reads v0, walks | same | `hint_zero_selects_v0` |
| Hint negative | ValidationException, version -1 missing | refuses with Java's text (D3) | `negative_hint_refuses_naming_the_missing_version` |
| Hint past newest, or into a removed version | missing-version refusal | same body (D6) | `hint_past_the_newest_refuses_naming_the_metadata_dir`, `hint_into_a_removed_version_refuses` |
| Listing pattern, canonical name listed | max N above zero | `listed_hadoop_version` | `listing_stem_follows_the_java_version_pattern` |
| Non-parsing stem beside a canonical name | skipped | skipped | `listing_skips_a_stem_whose_canonical_name_is_absent` |
| Catalog-style name beside v names | never a candidate | never while a Java candidate exists (D1) | `catalog_style_name_never_beats_a_v_name_without_a_hint` |
| Forward walk | to the first gap | to the first gap, checked i32 | `hint_walk_stops_at_the_first_missing_version` |
| `metadata/` absent | version 0, not found | not-found text | `missing_location_reports_the_supplied_path` |
| No Java candidate, names only leading-integer | table not found | leading-integer listing (C-008) | `listing_resolves_the_highest_numbered_metadata_file` |
| Listing I/O error | caught, version 0 (source reading) | surfaces the directory error | none; lead C-013 |
| Unicode-digit hint | parses | falls to the listing | none; lead C-012 |
| Codec by name | reads by name | reads by payload | excluded, C-009 |
| `v007` alone, no hint | not a candidate | read (widened C-008) | probe only; C-008 |

**Gate runs, this records pass.** Recorded in the commit's hand-back, not tracked here.

## Review findings

```yaml
FINDING:
  id: OPUS-F1
  severity: S3
  category: AT-1
  clause: C-012
  claim: Java parses a hint written in Unicode decimal digits and RePark falls to the listing, so a hinted version with a gap after it reads a different file.
  evidence: Opus verifier F1, probe unicode_digit_hint_gap on the head wheel and a JDK 21 probe; the same behaviour is on main.
  disposition: ACCEPTED_FLAGGED (owner ruling 2026-11-03: accepted leniency; card HADOOP-HINT-UNICODE-DIGITS-1)
```

```yaml
FINDING:
  id: OPUS-F2
  severity: S3
  category: AT-1
  clause: C-008
  claim: With no Java candidate, the leading-integer fallback also admits a non-canonical v name such as v007 alone, where Java finds no candidate.
  evidence: Opus verifier F2, probe v007_alone; the same behaviour is on main.
  disposition: ACCEPTED_FLAGGED (owner ruling 2026-11-03: Q1 widened to any name the leading-integer listing admits when no canonical candidate exists)
```

```yaml
FINDING:
  id: OPUS-F3
  severity: S3
  category: AT-3
  clause: C-013
  claim: A metadata directory that cannot be listed, with no readable hint, is refused by RePark; the Opus source reading says Java takes version 0 and reads v0 when it exists.
  evidence: Opus verifier F3, probe listing_io_error_exec_only; the Java behaviour is from source and was not run on Hadoop. The Muse run read the same shape as a RuntimeIOException.
  disposition: OPEN (card HADOOP-METADATA-DIR-UNLISTABLE-1; closing question is a Hadoop run)
```

```yaml
FINDING:
  id: MUSE-R1F1
  severity: S3
  category: AT-10
  clause: C-014
  claim: The version-0 arm of the listing filter has no pin; a mutant from the filter test to a strict greater-than survives the full path suite.
  evidence: Muse verifier R1F1, mutant run (103 passed, 0 failed); the head reads v0 correctly.
  disposition: OPEN (test-adequacy lead for the next touch of iceberg_path.rs)
```

```yaml
FINDING:
  id: OPUS-N1
  severity: S3
  category: AT-1
  clause: C-010
  claim: The rule doc's R2 sentence said a CRLF hint leaves a carriage return and falls to the listing; readLine strips CRLF and a lone CR, so the hint parses.
  evidence: Opus verifier Q3 and note (a); Muse note on a lone CR; JDK 21 probes.
  disposition: REMEDIATED (records commit: dated correction under R2 of the rule doc, the earlier sentence kept with a pointer)
```

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: hadoop-discovery-rule-1
  complete: true
  reattested: []
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Both verifiers walked the enumerated cases against the HadoopTableOperations source and JDK 21 behaviour, and the full 3904-cell table against the Spark column. Findings OPUS-F1, OPUS-F2 and OPUS-N1.
      artifacts: [crates/repark-core/src/iceberg_path.rs, crates/repark-core/src/iceberg_path/discovery_rule_tests.rs, crates/repark-core/src/iceberg_path/corpus_cells_tests.rs, crates/repark-core/src/iceberg_path/hint_walk_tests.rs, crates/repark-core/src/iceberg_path/tests.rs]
    - id: AT-2
      status: N/A
      justification: N/A — untouched by origin/main..5ca218f8 beyond what AT-1 covers; the hint boundary values are walked under AT-1 and AT-10.
    - id: AT-3
      status: ATTACKED
      evidence: Unreadable hint, directory hint, listing I/O error on an execute-only metadata directory, missing metadata directory, the i32 bounds and the walk bound. Finding OPUS-F3.
      artifacts: [crates/repark-core/src/iceberg_path.rs, crates/repark-core/src/iceberg_path/discovery_rule_tests.rs, crates/repark-core/src/iceberg_path/tests.rs]
    - id: AT-4
      status: N/A
      justification: N/A — untouched by origin/main..5ca218f8
    - id: AT-5
      status: N/A
      justification: N/A — untouched by origin/main..5ca218f8
    - id: AT-6
      status: ATTACKED
      evidence: Wrong-file reads checked on every cell group; D1 fixed 84 of 84; the widened and Unicode-digit shapes read a different file where Java does not, carried as C-008 and C-012. No state change on any read; insert and catalog cells unchanged.
      artifacts: [crates/repark-core/src/iceberg_path.rs, docs/corpus/hadoop-discovery-cells-2026-11-03.jsonl]
    - id: AT-7
      status: N/A
      justification: N/A — untouched by origin/main..5ca218f8
    - id: AT-8
      status: ATTACKED
      evidence: Error texts for D3 and D6 match Java's body; the Error analysis kind is kept; the fork is untouched; codec order equals Java's codec values; the Python load-path text pin moved to Java's wording.
      artifacts: [crates/repark-core/src/iceberg_path.rs, python/repark/tests/iceberg/test_iceberg_load_path.py]
    - id: AT-9
      status: N/A
      justification: N/A — untouched by origin/main..5ca218f8
    - id: AT-10
      status: ATTACKED
      evidence: The table pin is table-driven over the path-read rows; the mutation on the listing filter turns it red with 147 mismatches; unit tests cover CRLF, a plus sign, BOM, non-UTF-8, v-1, v007, v+7, v7.zstd and the walk. Unicode digits and the listing I/O error have no pin (C-012, C-013); the version-0 arm has no pin (MUSE-R1F1, C-014).
      artifacts: [crates/repark-core/src/iceberg_path/corpus_cells_tests.rs, crates/repark-core/src/iceberg_path/discovery_rule_tests.rs, crates/repark-core/src/iceberg_path/tests.rs]
```

## Scope notes

- Opus marked AT-2 N/A. The hint boundary values are this unit's core, and their pins sit under AT-1 and AT-10; the record keeps the verifier's category as written.
- The Muse and Opus reports disagree on Java's behaviour in the listing I/O error (C-013). Neither verifier ran Hadoop; the disagreement is the lead, not a settled reading.
- The builder's `crates/repark-core/src/map.md` baseline on main was lowered by the unit to its exact size, from (1576, 129321) to (1573, 129108), in `scripts/check_map_size.py`.
- Advisory lockstep warnings from the unit's map check named `python/repark/tests/iceberg/map.md` and `scripts/map.md`; their rows were still accurate and were not edited by the unit.
