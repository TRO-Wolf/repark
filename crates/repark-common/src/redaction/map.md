# map — repark-common/src/redaction

## Purpose

The unit pins of [../redaction.rs](../redaction.rs), the shared property redactor
(SOURCE-URL-REDACT-1). `../redaction.rs` declares both files as `#[cfg(test)]` modules.
See [../map.md](../map.md).

## Contents

- `tests.rs` — the hand-written pins, one test per shape or rule. **SOURCE-URL-REDACT-1-FN fold 3 (2026-10-06):** `mask_url_userinfo_masks_a_url_password_and_leaves_other_text` pins a URL password inside a sentence, the Iceberg key-collision sentences, and `s3://bucket@x/path`. pins: source-url-redact-1/C-056. Round 1 and fold 1: URL
  userinfo for every scheme, the lone token, every URL in a value, the authority scoping,
  query parameters, libpq and ODBC keywords, untouched non-secrets, the key rule.
  pins: source-url-redact-1/C-001, C-002, C-003, C-004, C-005, C-006, C-015
  **Fold 2 (2026-10-06), from the verifier's FAIL on `c7ac129c`:** Oracle thin / EZConnect and
  bare `user:pass@host` (C-021); a `;`-property or query password carrying an `@`, and MySQL's
  parenthesized pairs (C-022, C-023); whitespace inside a userinfo (C-024); the wider
  parameter names and the `…Name` exclusion (C-025); JSON and YAML values (C-026); the widened
  key rule and the frozen column rule (C-027); an unencoded `/ ? # ;` inside a password, the
  host-likeness rules and both sides of the fail-closed leg (C-034); the `\t` / `\n` value end
  and the empty userinfo that killed the verifier's surviving mutants M-13 and M-18 (C-028).
  pins: source-url-redact-1/C-021, C-022, C-023, C-024, C-025, C-026, C-027, C-028, C-034
  **SOURCE-URL-REDACT-2 (2026-10-06):** three registry pins run under one test lock (the store is process-wide): every shape masks after registering while a plain value stays out, unregistered credential-free text is byte-identical, and the 257th value evicts the first. pins: source-url-redact-1/C-057
  **SOURCE-URL-REDACT-2 fold 1 (2026-10-07):** four registry pins under the same lock: longest-first replacement (the verifier's `password=S3` / DSN pair shows no `cr3tPw` tail, plus a nested pair where the short value sits at a token boundary so the insertion-order mutant turns red), LRU refresh (the `evict.py` shape: 255 fillers, a DSN touch, one new value, the DSN still masked), whole-token replacement (`pw=a` leaves `cpw=abc` byte-identical while a quoted `'pw=a'` masks), and the `{:?}` inner-form branch (a backslash value appearing only in Debug form; deleting the branch turns the pin red). pins: source-url-redact-2/R-A1, R-A2, R-A3, R-A4
  **SOURCE-URL-REDACT-2 fold 2 (2026-10-07):** two pins under the same lock: the eight re-verify edge shapes (`url=`, trailing `.`, `,`, JSON quotes, `;`, `conn_`, `[...]`) all mask — restoring the round-A boundary set turns the `url=`/`conn_` cells red — and letter-or-digit neighbours on either side keep the text byte-identical, which turns red when the trailing check is forced true.
  **SOURCE-URL-REDACT-2 fold 3 round B (2026-10-07):** `registered_values_mask_when_their_own_edge_is_punctuation` pins the re-verify's ODBC shape (`;` then `Encrypt=yes`, then a digit) and its mirror images (a value led by `;` after a letter, a digit and an ANSI SGR escape); all five cells turn red on the neighbour-only rule. pins: source-url-redact-1/C-074
  **SOURCE-URL-REDACT-2 fold 3 round B2 (2026-10-07):** `registered_values_mask_after_an_ansi_csi_sequence` pins the re-verify's ANSI cell (`\x1b[31m` before a DSN) and a two-parameter SGR (`\x1b[1;31m`), both masking, plus two guards that stay byte-identical: a bare `m` prefix and `[31m` without ESC. Dropping the CSI check turns the `\x1b[31m` cell red. pins: source-url-redact-1/C-075
- `corpus.rs` — the permanent fixed-seed corpus: a hand-rolled LCG generates 6,000 shaped
  inputs over 12 classes (half with the hard characters `@ : / ? # & ; = space \n \t " ' { } \`),
  each with a marker password, and 3,000 garbage inputs; no marker survives, `catch_unwind`
  sees no panic, every host is kept. A lone token never carries `:` (it would be a user and a
  password) and an Oracle password never carries `"`. Runs in about 0.15 s in debug. The
  verifier's own 40,000-case harness, rerun against this code, leaks no password.
  pins: source-url-redact-1/C-037, C-038, C-039
  **SOURCE-URL-REDACT-1 fold 3 (2026-10-06):** the corpus mirrors the re-verify's 17 classes (9,000 shaped inputs, a quarter
  through `redact_value`), carries every repro of both verdicts as fixed cases and a
  storage-location class that must stay unchanged, and 4,000 garbage inputs; `tests.rs` adds
  the storage, TNS, userinfo-span, fail-closed login, multi-line and mutant-killing pins. The
  36-mutant run (33 red, N7 over-mask only, N19 and F14 equivalent) and the re-verify probe
  (0 survivals outside the R-9 known limit and the R-2 storage lone tokens) are in the ledger;
  the known limits are named in `CONNECT-DIV-url-userinfo`.
  pins: source-url-redact-1/C-041, C-043, C-044, C-045, C-046, C-047, C-048, C-049, C-050, C-051
  **SOURCE-URL-REDACT-1 fold 4 (2026-10-06):** `tests.rs` pins a URL embedded in JSON,
  parentheses, quotes, angle and square brackets, braces and comma lists, a bare login whose
  password holds `@x(`, and the verifier's N19 pin; `corpus.rs` adds the `embedded-url` class
  (18 classes).
  pins: source-url-redact-1/C-052, C-053, C-054
