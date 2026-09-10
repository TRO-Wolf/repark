# Unit ledger — REVIEW-FIX-5 step 1 · DESCRIBE metadata-name intercept, Owner, short names, redaction

**Unit:** REVIEW-FIX-5 step 1 · **Date:** 2026-09-10 · **Branch:** `fix/review-fix-5` · **Base:** `origin/main`
**Model:** Muse Spark (muse-spark-1.3-contributor)
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** The review-1 sweep confirmed two DESCRIBE defects (Q-14: a real table named
`files` never reaches the describe path; Q-15: `Owner` reads `USER`/`USERNAME` on the query
path against ADR-0004) and ruled two more into this card (RF-2: short names complete from
session defaults; RF-6: Table Properties redacts through `prop_key_is_secret`).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Not in this step:** production-session wiring of the Owner snapshot (the real
`ReparkSession` builder in `repark-core` and the PyO3 session build install no Owner
extension yet, so outside the Rust test session `Owner` reads `unknown`; recorded as
residue below, not as a clause), any dependency file, `STATUS.md`,
`briefs/next-sequence.md`, `gh`, pushes.

## PROPOSITION LEDGER — REVIEW-FIX-5 step 1 — 2026-09-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | D-1: the metadata-table check applies to the suffix position of a four-or-more-part name, never to the table segment of a three-part name; `DESCRIBE TABLE EXTENDED ice.sales.files` returns Spark's rows for the real `ice.sales.files` table. | `describe_table_extended_describes_a_real_table_named_files` | **PROVEN** | Red 2026-09-10 (`cargo test -p repark-spark describe_table`, behavior code untouched): `DESCRIBE TABLE EXTENDED ice.sales.files` falls through to DataFusion and dies with `Expected: end of statement, found: EXTENDED at Line: 1, Column: 16`. Green after removing the table-segment metadata check from `try_parse_describe_table`: first row `("id", "bigint", None)`, a `Name = ice.sales.files` row, and a non-empty `Owner` row. The four-part `DESCRIBE ice.sales.t1.snapshots` still stays out, pinned by the updated leaves-alone test. |
| C-002 | D-2: `Owner` is resolved once when the session is built and never from `std::env::var` on the query path; the pin asserts the resolved value with the environment changed after the session is built. | `describe_table_owner_is_the_session_resolved_user` | **PROVEN** | Red 2026-09-10 (same run; only the inert `DescribeOwnerConfig` extension plus `setup_with_owner` existed, the query path still read the environment): two sessions built with distinct owners both reported the ambient process user — `left: "john", right: "review_fix_5_first_session_user"`. The card's literal env-mutation choreography is replaced by this two-session variant because `std::env::set_var` is `unsafe` on this toolchain and `unsafe_code = "forbid"` bans it; a live query-path read would give both sessions the same process value, so distinct per-session owners prove the same proposition race-free. Green after the query path reads the `DescribeOwnerConfig` extension installed at session build: session one reports `review_fix_5_first_session_user`, session two its own. `grep std::env describe_show.rs` now matches only the build-time `session_owner_snapshot`. |
| C-003 | D-3: one- and two-part names in `DESCRIBE [TABLE] [EXTENDED\|FORMATTED]` complete from the session current catalog and namespace exactly as `SELECT` does; both spellings return the same rows as the fully qualified three-part name. | `describe_table_short_names_complete_from_session_defaults` | **PROVEN** | Red 2026-09-10 (same run): `DESCRIBE TABLE desc_demo` falls through to DataFusion and dies with `Expected: end of statement, found: desc_demo at Line: 1, Column: 16`, reproducing the reTest-bed measurement of RF-2. Green after the parser takes one- and two-part names and the router completes them from `datafusion.catalog.default_catalog` / `default_schema` (the `resolve_table_ident` rule `SELECT` resolves by) before the registered-catalog check: `DESCRIBE TABLE desc_demo`, `DESCRIBE TABLE sales.desc_demo`, and both `EXTENDED` spellings return exactly the three-part rows. |
| C-004 | D-4: the table-property rows `DESCRIBE TABLE EXTENDED` prints go through `prop_key_is_secret`, the same predicate the config redaction uses, with no second predicate; a secret-shaped property renders redacted, a non-secret property renders in the clear; the Spark delta (`s3.access-key-id` prints in the clear under Spark) is recorded as a deliberate divergence. | `describe_table_extended_redacts_through_prop_key_is_secret` | **PROVEN** | Red 2026-09-10 (same run): Table Properties rendered `[current-snapshot-id=none,k=v,s3.access-key-id=AKIAEXAMPLE]` — the access key in the clear, reproducing Q-30. Note on the card wording: `DESCRIBE NAMESPACE EXTENDED` in this tree uses Spark's `Utils.redact` predicate (its truth table is pinned byte-for-byte to live Spark, e.g. `ACCESS-KEY` and `access_key` print in the clear), so "the SAME predicate NAMESPACE already uses" is read as the repo-shared secret predicate `prop_key_is_secret` (repark-core, also mirrored by the facade `_secrets.py`), which RF-6 names. Green after `render_table_properties` calls the shared predicate (`pub` widened one token in `repark-core`, re-exported at its root; no new edge): `s3.access-key-id=AKIAEXAMPLE` renders `*********(redacted)` with the plaintext absent, `k=v` stays in the clear. The pre-existing `secret_token` pin still greens under the new predicate. |

| C-005 | D-5: `catalog_config.rs` carries no `//` comments; the seventeen moved reasons live on the `catalog_config.rs` row of `crates/repark-core/src/map.md`, and the size baseline ratchets down to the new exact count with no other row touched. | `check-rust-file-size` green at 1028 plus `grep -n "^\s*//"` showing only `///`/`//!` doc lines | **PROVEN** | `make verify` exit 0 (2026-09-10); `catalog_config.rs` is 1028 lines against the ratcheted 1028 baseline, no other EXCEPTIONS row touched; the `//` sweep shows only required doc comments. |
| C-006 | R5-C: a production session built through `ReparkSessionBuilder` (not the test helper) reports the resolved owner, not `unknown`, in the `Owner` row of `DESCRIBE TABLE EXTENDED`; the struct keeps its name and prefix in `repark-core`, re-exported at the root, with no new crate edge. | `describe_table_owner_resolves_in_a_production_built_session` | **PROVEN** | Red 2026-09-10 against the install-free tree: `assertion left == right failed: a production-built session resolves the owner at build, never unknown / left: "unknown" / right: "john"`. Green with the `ReparkSessionBuilder` install: the production session's `Owner` row equals `session_owner_snapshot()`, and the untouched two-session pin still proves the no-env-on-the-query-path half. The `#[non_exhaustive]` extension forced one design point: cross-crate construction goes through the `with_session_owner` installer mirroring `with_repark_sql_config`. `make verify` exit 0 (2026-09-10, second round): spark lib 920, core lib 294, zero failures. |

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

## Green

`cargo test -p repark-spark describe` 2026-09-10: `37 passed; 0 failed`
(lib target; the four new pins plus the updated leaves-alone test and the new
short-name parser pin among them). Full-crate runs in the same tree:
`cargo test -p repark-spark` lib `919 passed; 0 failed` with every integration
target green; `cargo test -p repark-core` `294 + 37 + 8 passed; 0 failed`.
`make verify` counts are in Gates below (run before commit).

## Red first

## Residue

- `R-DESCRIBE-TWO-PART` stays in the registry: RF-2 retires it "when it lands", and it has
  not landed for the dbt session. Completion reads the engine
  `datafusion.catalog.default_catalog`, and facade current-catalog is facade-only by
  `R-CURCAT-FACADE` (`session_core.py`: "catalog state is facade-only, never engine USE
  state"), so the dbt session still completes to the unregistered `datafusion` catalog,
  falls through to DataFusion, and refuses with the pinned
  `table 'datafusion.gold.…' not found`. The row leaves when a session unifies facade
  current-catalog with the engine default (or issues the equivalent `SET`), at which point
  its pin reds on purpose. Untouched here: the dbt suite is Python-home and unrunnable in
  this Rust-only round.
- Two DESCRIBE surfaces now use two different predicates by decision (R5-B):
  `DESCRIBE NAMESPACE EXTENDED` keeps Spark's `Utils.redact` regexes
  (`property_is_redacted`, byte-pinned to live Spark) while `DESCRIBE TABLE EXTENDED` uses
  `prop_key_is_secret`. RF-6's parenthetical descriptor was wrong, its named predicate
  wins: Spark's own `access[.]?key` regex does not match hyphenated `access-key`, while
  `prop_key_is_secret` normalises hyphens and dots to underscores and does — so only the
  named predicate delivers the stated reason (Spark prints `s3.access-key-id` in the clear,
  RePark deliberately does not).

## Gates

- `cargo test -p repark-spark describe` 2026-09-10: `37 passed; 0 failed` (lib target).
- `cargo test -p repark-spark --lib` 2026-09-10: `919 passed; 0 failed`; all integration
  targets green in the same invocation.
- `cargo test -p repark-core --lib` 2026-09-10: `294 passed; 0 failed`.
- `make verify` 2026-09-10: first run RED at one gate only — `check-rust-file-size`:
  `crates/repark-core/src/catalog_config.rs` grew to 1045 lines against the exact baseline
  1044. The +1 was the D-4 widening (`pub(crate)` to `pub`) plus the `#[must_use]` that
  clippy pedantic mandates on the newly public function. Per ruling R5-A the line was
  funded by D-5 (seventeen `//` lines moved to the core map) instead of an upward
  amendment: the file now stands at 1028 with the baseline ratcheted down to match.
  Re-run after D-5: exit 0 — 54 `test result: ok` lines, zero failures; spark lib
  919, core lib 294, describe filter 37, all green.
- Comment fence 2026-09-10: zero added `//` lines in Rust, zero added `#` lines in
  py/toml/sh/yml. The brief's literal grep additionally matches `#[tokio::test]` /
  `#[must_use]` attribute lines; those are attributes, not comments, and the same lines
  already open every existing test in the file.
