# map — repark-common/src/redaction

## Purpose

The unit pins of [../redaction.rs](../redaction.rs), the shared property redactor
(SOURCE-URL-REDACT-1). `../redaction.rs` declares both files as `#[cfg(test)]` modules.
See [../map.md](../map.md).

## Contents

- `tests.rs` — the hand-written pins, one test per shape or rule. Round 1 and fold 1: URL
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
- `corpus.rs` — the permanent fixed-seed corpus: a hand-rolled LCG generates 6,000 shaped
  inputs over 12 classes (half with the hard characters `@ : / ? # & ; = space \n \t " ' { } \`),
  each with a marker password, and 3,000 garbage inputs; no marker survives, `catch_unwind`
  sees no panic, every host is kept. A lone token never carries `:` (it would be a user and a
  password) and an Oracle password never carries `"`. Runs in about 0.15 s in debug. The
  verifier's own 40,000-case harness, rerun against this code, leaks no password.
  pins: source-url-redact-1/C-037, C-038, C-039
