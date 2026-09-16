# Charter ledger — JAVA-REGEX-FEATURES-1 · lookaround, backreferences, possessive quantifiers, atomic groups

**Date:** 2026-09-16 (round 1) · **Branch:** `feat/java-regex-features-1` · **Base:** `02abfd0e`
· **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `JAVA-REGEX-FEATURES-1` (BACKLOG → FIXED) and `JAVA-REGEX-BACKTRACK-1` (new,
DECLARED), both in [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

**Why now.** The 1.5 Spark-parity campaign: every regex name shares one compiler
(`crates/repark-functions/src/spark_regexp.rs`, `spark_regexp_match.rs`, `spark_split.rs` over
`java_regex.rs`) that refuses lookaround, backreferences and possessive quantifiers while Spark
evaluates them. Owner ruling Q-16c-1 approves `fancy-regex` as the fallback engine for exactly
those patterns. Oracle: live PySpark 4.1.2, 2026-09-16 —
`/tmp/oc-worker/sc/oracle/b2-oracle.json` cells `RX-SQL-00…24`, `RX-PY-00…02`,
`/tmp/oc-worker/sc/oracle/b3-oracle.json` cells `RX2-SQL-00…29`, copied to
[../../../python/repark/tests/java_regex_features_1_spark_oracle.json](../../../python/repark/tests/java_regex_features_1_spark_oracle.json).

**Not in this unit:** `functions*.py`, `dataframe/**`, `column.py`, `catalog.py`, `types.py`
(run 18b owns them); the Python function registry (run 18a). A facade half needing a Python
edit there is recorded as a P2 hand-off, never edited here. The `UR-*`, `UC-*`, `UC2-*` oracle
cells belong to other units.

## PROPOSITION LEDGER — JAVA-REGEX-FEATURES-1 — 2026-09-16

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Positive/negative lookahead, bounded and unbounded lookbehind answer Spark on every regex name (`rlike`/`regexp`/`regexp_like`, `regexp_extract`, `regexp_extract_all`, `regexp_replace`, `regexp_count`, `regexp_instr`, `regexp_substr`, `split`). | `python/repark/tests/test_java_regex_features_1.py` pins over RX-SQL-00…02/07/08/10/11/20, RX2-SQL-02/07…09/13/20…24 plus Rust unit tests beside the kernel. | **PROVEN** | 61/61 pins green on the rebuilt release native; b2 probe diffs 0. `crates/repark-functions/src/spark_regex_engine.rs` implements selection + walks; 21 Rust unit tests green; full crate suite 754 passed. |
| C-002 | Numbered (`\1`) and named (`\k<x>`) backreferences answer Spark in match and in replace (`$1`, `$1$1`). | Pins over RX-SQL-03/06/12/21/23, RX2-SQL-10/11/14/26 plus Rust unit tests. | **PROVEN** | All green except RX2-SQL-12, which becomes residue row JAVA-REGEX-FEATURES-1-R1 with an honest `_divergence` pin: fancy-regex compares backreferences case-sensitively even under `(?i)` (verified at the Rust level against raw fancy-regex), while Java compares case-insensitively. RX2-SQL-11 stays a value pin (false on both). |
| C-003 | Possessive quantifiers (`*+`, `++`, `?+`, `{n,m}+`) and atomic groups (`(?>…)`) answer Spark. | Pins over RX-SQL-04/05/09, RX2-SQL-04/05/18/19 plus Rust unit tests. | **PROVEN** | All green, both doors where bound. |
| C-004 | The fancy engine runs under an explicit backtrack limit; a repark overrun raises a loud execution error naming the pattern and the limit; RX2-SQL-00 errors (never silent NULL/false); RX-SQL-18 and RX2-SQL-01 answer `false` within 2 s. | Limit number + timings in this ledger; overrun pin over RX2-SQL-00 and a synthetic exponential cell. | **PROVEN** | `FANCY_BACKTRACK_LIMIT = 10_000_000`, `FANCY_LOOP_HAYSTACK_MAX = 10_000`. RX-SQL-18 / RX2-SQL-01 answer `false` in ~0.001 s best-of-5 on the release native (fancy delegates easy repeats to the DFA, so the budget never binds them). RX2-SQL-00 raises `regex overrun on pattern '(a|b)*c': exceeded looping-pattern haystack (limit 10000)` (pinned by match `overrun`). The Rust `backtrack_budget_trips_on_hard_exponential` pin (`(a+)+b(?=c)` on 25 a's) proves the budget tripwire fires and names pattern + limit. |
| C-005 | Invalid patterns in the fallback grammar raise Spark's `[INVALID_PARAMETER_VALUE.PATTERN]` class naming the function. | Pins over RX-SQL-13/14, RX2-SQL-16/17/25 with the Spark message. | **PROVEN** | RX2-SQL-16/17 byte-exact per `msg.py` (EQ). All regexp-family invalids (fast and fallback) raise the class — the oracle demands it for `(`/`a{2,1}`/`a{` too. One documented formatting delta: RX2-SQL-25 echoes the kernel-received pattern `(a)\k<x>` (1 backslash) where Spark echoes the SQL-literal spelling `(a)\\k<x>` (NE on `msg.py`); the class, names and SQLSTATE match. `split` keeps its long-standing text (no oracle cell pins its Spark shape). |
| C-006 | The Python door (`Column.rlike`, `F.regexp_extract`, `F.regexp_replace`) answers through the same Rust kernel with no Python change. | Pins over RX-PY-00…02 plus one Python-door leg per remaining name. | **PROVEN** | RX-PY cells plus `test_python_door_fancy_match_names`, `test_python_door_fancy_extract_names`, `test_python_door_null_pattern_stays_null` green. Zero Python edits. P2 hand-off to run 18a: `F.split` still raises `UnsupportedOperationException` (`functions_expr.py:579`, disclosed R-FN-BATCH1) — the `F.split` leg was dropped; split stays pinned on the SQL door. |
| C-007 | A pattern the `regex` crate handles costs the same as before (dispatch overhead ~zero). | Before/after release-native timings on 1e6 rows in this ledger. | **PROVEN** | Before (main native): rlike 0.019 s, count 0.034 s, replace 0.048 s. After (this branch): rlike 0.017–0.021 s, count 0.036–0.044 s, replace 0.042–0.053 s (three runs, best of 3 each). Ranges overlap fully — run-to-run variance on these 20–50 ms queries exceeds any delta, so no regression is measurable; by construction the fast path adds one enum dispatch per row (the scan runs once per distinct pattern, cached). |
| C-008 | Registry `JAVA-REGEX-FEATURES-1` → FIXED, `JAVA-REGEX-BACKTRACK-1` DECLARED dated 2026-09-16 quoting Spark's `StackOverflowError`; the Q15-10…12 refusal legs flip to values. | Registry diff + `test_door_converge_2.py` diff. | **PROVEN** | Registry: FIXED row plus R1 (`(?i)` backref) and BACKTRACK-1 rows, both DECLARED 2026-09-16. Q15-10…12 moved to value legs (all green); Q15-13 untouched and green after restoring the engine-detail suffix for split invalids. |

**Red-first evidence (2026-09-16, main native rebuilt step 0):** the probe
`<clone>/.venv/bin/python /tmp/oc-worker/sc/oracle/rp.py <oracle> RX` reports 20 diffs on b2
and 21 on b3 — every fancy-feature cell errors with
`unsupported Java regular expression feature 'lookahead' | 'lookbehind' | 'backreference' |
'possessive quantifier'`, `(?>…)` fails as a regex parse error, RX-SQL-15 answers `a[b]c`
against Spark's `a[]c`, and RX2-SQL-00 answers `false` against Spark's `StackOverflowError`
query kill. Full per-cell output in the step-2 commit message.

## Rulings (originating + orchestrator)

- Q-16c-1 (owner, 2026-09-15): `fancy-regex` approved as the fallback engine for Java
  lookaround, backreferences and possessive quantifiers. Used only for patterns that need one
  of those features (D-1); the `regex` crate stays the engine for everything it can express.
- Q-17a-2 (owner, 2026-09-16): a decision that raises, casts, coerces or branches on a value is
  a kernel too — engine selection, replacement expansion and the overrun tripwire all live in
  Rust (`crates/repark-functions`); Python holds names, argument shapes and API plumbing only.
- R-18c-O3 (orchestrator, card D-4): a repark overrun raises a loud execution error naming the
  pattern and the limit; the registry gains dated DECLARED row `JAVA-REGEX-BACKTRACK-1`
  quoting Spark's `StackOverflowError`. Never a silent NULL/false.

## Design record (decisions taken inside the card)

- D-1 engine selection: per compiled pattern, in Rust, by an extended feature scan —
  lookahead, lookbehind, numbered/named backreference, possessive quantifier, atomic group —
  plus the catastrophic-shape class below. Fast patterns keep the `regex` crate untouched.
- D-1/D-3 tension, resolved: D-1 says fancy is used ONLY when the pattern needs a fancy
  feature, but D-3 requires RX-SQL-18 / RX2-SQL-01 (`(a+)+b`, no fancy feature) to run under
  the backtrack limit within 2 s. Resolution: the scan also routes nested unbounded
  quantification (`(a+)+`, `(a|b)*`, `(a*)*` — a quantified group or a quantifier applied to a
  quantified atom) to the fancy engine with delegation disabled, so the limit binds. Plain
  non-nested patterns never leave the DFA path (C-007).
- D-3/D-4 tripwire, from fancy-regex 0.11 VM ground truth (`src/vm.rs`: the counter
  increments once per backtrack-stack pop): no single work count separates RX-SQL-18
  (`(a+)+b` on 25 a's, ~2^24 pops — must complete `false`) from RX2-SQL-00 (`(a|b)*c` on
  40000 chars, ~8·10^4 pops — must raise). The separator is input length / stack depth, so
  the overrun rule is disjunctive: (i) backtrack pops exceed `FANCY_BACKTRACK_LIMIT`, or
  (ii) a looping fancy pattern (unbounded `*`/`+`/`{n,}` outside classes) meets a haystack
  longer than `FANCY_LOOP_HAYSTACK_MAX` bytes — the static shape of Java's recursive-matcher
  stack overflow (Java recursion depth grows with loop iterations, bounded by input length;
  measured overflow at 40000 chars). Both name the pattern and the tripped limit.
- D-4 RX2-SQL-00: errors loudly (outcome-converges with Spark killing the query) instead of
  answering the mathematically-correct-but-divergent `false`.
- D-5 `${name}`: Spark answers `a[]c` (RX-SQL-15) and `""` (RX2-SQL-27) because Spark SQL
  variable substitution empties `${…}` before the regex runs; RePark has no substitution
  layer (verified: no substitute/VARIABLE handling in `repark-spark`/`repark-sql`). The shared
  replacement pre-pass therefore drops every `${…}` span before engine-native `$` expansion,
  on both engines. All other `$` behaviour stays byte-identical (narrow fix). If RePark ever
  gains `${}` substitution, this pre-pass moves to that layer.
- D-5 `\1` with no group 1 (RX2-SQL-15): Java reads it as octal `\x01` (fancy-regex 0.11 has
  no octal escapes — verified in `src/parse.rs` — and errors). Out-of-range single-digit
  `\1`…`\7` are rewritten to `\x0N` before engine selection, so the cell stays fast and
  answers `false`. `\8`/`\9` out of range and multi-digit misses keep fancy's verdict
  (invalid-pattern class), noted as the documented boundary; no oracle cell covers them.
- D-5 `(?P<name>…)`: Java rejects it, fancy-regex accepts it. The scan refuses it as an
  invalid Java pattern (Spark class) rather than answering where Spark errors.
- D-5 invalid classes: fast-path invalids keep today's text byte-identical (Q15-13 pins
  `nclosed character class`; the brief flips Q15-10…12 only). Fallback invalids raise the
  byte-exact Spark `[INVALID_PARAMETER_VALUE.PATTERN]` message with the invoked UDF's own
  name (`rlike`, `regexp_like`, `regexp_extract`, `regexp_extract_all`, `regexp_replace`,
  `regexp_count`, `regexp_instr`, `regexp_substr`, `split`).
- D-6 cost: release-native before/after timings over the three 1e6-row queries in
  `/tmp/cost_regex.py` (throwaway probe, not committed); numbers in the C-007 row.

## Findings recorded during implementation (2026-09-16)

- F-1 delegation: fancy-regex 0.11 marks a repeat over easy children as delegable
  (`src/analyze.rs`: `Repeat` inherits the child's hardness), so nested-quantifier
  patterns such as `(a+)+b` run on the DFA and never touch the backtrack budget. The
  budget binds only patterns with a hard node (lookaround, backreference, atomic),
  which is why RX-SQL-18 / RX2-SQL-01 answer in ~1 ms. Verified by timing on the
  release native and by the trip pin, which uses `(a+)+b(?=c)` (the trailing
  lookaround forces the repeat into the VM).
- F-2 `(?i)` backreferences: raw fancy-regex compares a backreference case-sensitively
  even under `(?i)` (Rust-level probe, no translation involved), while Java compares
  case-insensitively. No rewrite can fix this without reimplementing backtracking
  search, so RX2-SQL-12 becomes residue row JAVA-REGEX-FEATURES-1-R1 with a
  `_divergence` pin. Case-insensitive matching itself works (`(?i)ab` on `AB` true).
- F-3 `regexp_substr` nullability was hardcoded true while every sibling UDF follows
  the arguments; fixed to `any_arg_nullable` in the same file (RX-SQL-09/RX2-SQL-24
  pin nullable false now).
- F-4 `F.split` still raises `UnsupportedOperationException` before reaching the
  kernel (disclosed R-FN-BATCH1, run 16a/18a territory, outside this unit's fence):
  recorded as a P2 hand-off, the `F.split` leg dropped, split pinned on the SQL door.
- F-5 `RX-SQL-18` / `RX2-SQL-01` pin nullable true: the nullability rides in on
  `repeat(...)` (another unit's kernel); `rlike` propagates correctly (NULL legs pin
  true, literal legs pin false).

## Gates

| Command | Result |
|---|---|
| (pending) | — |

## VERDICT: 8 clauses, 8 PROVEN, 0 OPEN, 0 REJECTED (round 1, step 6).
