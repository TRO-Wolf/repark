# Unit ledger — STRING-LITERAL-ESCAPE-1 · SQL string literals unescape as Spark does

**Date:** 2026-09-29 · **Branch:** `fix/string-literal-escape-1` · **Base:** `5fb38051` (`origin/main`)
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** PE-10 (S1, silent, pre-existing): `SELECT "x\\""y"` answers `x\""y`
on RePark where Spark 4.1.2 answers `x\"y` — a doubled `""` inside a
double-quoted literal is not collapsed, and the wrong value is stored or
returned without any error. The Step-0 matrix (C-000) measures 229 literals on
five doors under both `escapedStringLiterals` settings against live PySpark
4.1.2; this unit makes the shared Rust value engine quote-aware, answers
`r"…"` literals, splits raw doublings into head plus quoted tail, and keeps
verbatim values exact — on `spark.sql`, `F.expr`, `selectExpr`, `filter`, and
the write path.

**Not in this step:** `r`/`R` followed by three or more quotes (sqlparser's
triple path, which Spark's pair-lexing does not share — 7 literals stay
error-vs-value, unchanged from base); `F.expr` under `escapedStringLiterals=true`
(the Column plans on a sessionless context, so the flag cannot reach it —
pre-existing, previously scoped to session doors by FNP-4B);
`spark.sql.ansi.doubleQuotedIdentifiers` (no carrier; measured only); `STATUS.md`,
`briefs/`, `.github/`, `Cargo.toml`, `Cargo.lock`.

## PROPOSITION LEDGER — STRING-LITERAL-ESCAPE-1 — 2026-09-29

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-000 | The Step-0 matrix is measured before any edit: 229 literals (every Spark escape in both quote types, doublings before/after/between escapes, adjacency, raw forms, 54 controls) on five doors under both `escapedStringLiterals` settings, on RePark and live PySpark 4.1.2; the premise holds (`"x\\""y"` → `x\""y` vs Spark `x\"y`); 22 default-mode and 69 verbatim sql-door diffs isolate to the unescape layer. | The four probe JSONs agree cell by cell; the comparison table is in the hand-back. | PROVEN | Measured 2026-09-29, PySpark 4.1.2 `local[1]` via `jvm-lock.sh`. Spark values baked into the pins verbatim. |
| C-001 | Default-mode values are quote-aware: `""` collapses only in double-quoted literals and `''` only in single-quoted (cross-type doublings pass through); `r"…"`/`R"…"` answer; every default diff cell flips to Spark on all five doors and the write read-back. | The `double_quoted_*` Rust pins plus `DEFAULT_CASES` facade pins green; post-fix probe shows only the `r''''` triple-raw edge. | PROVEN | Post-fix default: 5/1145 cells differ, all `r''''` on five doors; write read-back differs on that row only. |
| C-002 | A raw `''`/`""` ends the raw token: the head stays verbatim and the tail unescapes with the same quote's rules (`r'a''\n'` → `a`+LF, `r'a''b''c'` → `ab'c`); in verbatim mode the raw value is the opening quote plus the raw text with the first doubling removed (`r'a'` → `'a`). | The `raw_doubled_quote_splits_like_spark` Rust pins plus the raw facade rows green. | PROVEN | Split rule confirmed by discriminator cells on Spark (`r'a''\n'`, `r'\n''\t'`, `r''''` → empty); uppercase `R` splits identically. |
| C-003 | Verbatim mode keeps the raw text exactly (backslashes AND doublings; `d-dq`-family downstream errors become values); every verbatim session-door cell matches Spark; the `F.expr`-ignores-verbatim gap is measured and disclosed, not absorbed. | The `verbatim_keeps_*` Rust pins plus `VERBATIM_CASES` facade pins green; verbatim non-expr non-`r''''` diffs are zero. | PROVEN | Post-fix verbatim: 0 non-expr diffs outside `r''''`; ~150 expr-door diffs are the pre-existing sessionless-planning gap. |
| C-004 | Nothing else moves: every Step-0 match stays a match (zero match-to-diff transitions); the native door answers byte-identically; NTZ/LTZ and escape neighbours green; the full `repark-spark` suite green; byte-frozen files keep their hashes. | The before/after transition check, the native rerun, neighbour suites, and the freeze record green. | PROVEN | 2472 passed, 0 failed; `test_pr_245_revalidation_record.py` 11 passed; native before==after. |

## Evidence

### C-000 diff taxonomy (all measured, Spark 4.1.2)

Default mode, 22 literals: nine `""`-doubling cells (`d-defect`, `d-bs-doubled`,
`d-doubled-esc`, `d-esc-doubled`, `d-escq-doubled`, `d-doubled-escq`, `d-between`,
`j-its-d`, `c-dsay`, `x-esc-cross2`), five double-raw refusals (`r-d-bs`,
`R-d-bs`, `r-d-nl`, `r-d-doubled`, `r-d-empty`), six single-raw splits
(`r-doubled`, `x-raw-a`, `x-raw-b`, `x-raw-d`, `x-raw-e`, `x-draw-a`,
`x-draw-c`). Verbatim sql door, 69 literals: all doublings kept by Spark
(`'it''s'` → `it''s`), verbatim raw marked (`r'a'` → `'a`), and three
borrowed-double downstream errors fixed by rewriting. Error-vs-error cells
(`e-*` family, `r'\''`, unterminated) match in kind and are untouched.

### C-001/C-002/C-003 files

Product: `crates/repark-spark/src/spark_literals/unescape.rs` (child module;
the 999-line parent forced the split — no ceiling raised, `lib.rs`
untouched), `spark_literals.rs` (four-arm value dispatch, verbatim-aware
rewrite rule), `spark_rewrites/create_options.rs` (option keys unescape with
their own quote type). Pins: `crates/repark-spark/src/tests/string_literal_escape_1.rs`
(own leaf — the SQP-1 leaf is byte-frozen),
`python/repark/tests/test_string_literal_escape_1.py` (40 default + 11
verbatim cases on five doors plus an iceberg write round-trip).

### C-004 guards and mutation

Neighbours: SQP-1/FNP-4B/LIKE-escape suites 93 passed + 1 pre-existing xfail;
NTZ/LTZ suites 48 passed (`test_ltz_stacked_sign_1.py` absent — PR #882
unmerged; equivalents from the re-verify corpus covered as controls). Native
door: 12/12 byte-identical. Mutation (old unescape restored, signatures
kept): 5/6 new Rust pins red (the borrow pin correctly stays green) and 6/6
facade tests red; fix restored byte-exact (`diff` clean) and green again.

### C-004 known edges (measured, unchanged from base)

| Edge | Spark value | RePark | Reason |
|---|---|---|---|
| `r''''`, `r'''x'''`, `r'''''x'''''`, `r''''''`, `R"""x"""`, `r""""`, `r""""""` (+ verbatim twins) | ``, `x'`, `'x''`, `'`, `x"`, ``, `"` | TokenizerError / ParserError | `r`+3-quote runs take sqlparser's triple path; no fork edit allowed |
| `F.expr` under `escapedStringLiterals=true` | verbatim values | default values | Column plans on a sessionless context; needs session-aware planning |
| `"…"` under ANSI + `doubleQuotedIdentifiers=true` | identifier / `UNRESOLVED_COLUMN` | string literal | no carrier (measure-only per brief) |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: string-literal-escape-1
  complete: true
  reattested: [AT-1, AT-2, AT-3, AT-6, AT-8, AT-10]
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Clauses C-000..C-004 walked against behavior — the 229-literal Step-0 matrix on five doors under both modes against live PySpark 4.1.2, the post-fix rerun to zero unexplained diffs, the residuals ledgered with Spark values, the guards re-run to zero match-to-diff transitions, the mutation run red-first at both levels, and the gate run as written; every clause is PROVEN and cited from the maps.
      artifacts: [task/ledgers/staging/string-literal-escape-1-ledger.md, python/repark/tests/test_string_literal_escape_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries actually exercised — empty literals both quotes, quote runs of 2/3/4/6, lone and trailing escapes, NUL/control bytes, astral and lone surrogates, out-of-range \U, malformed \u, unterminated and unpaired-backslash refusals, 10k-char control; Arrow value AND type pinned per facade cell.
      artifacts: [python/repark/tests/test_string_literal_escape_1.py, crates/repark-spark/src/tests/string_literal_escape_1.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Failure modes preserved in kind — unterminated literals, unpaired trailing backslashes, and raw backslash-quote forms refuse on both engines before and after; error-to-value flips land only where Spark answers (double-raw, verbatim borrowed doubles); no new failure mode and no retry/cleanup surface (pure value function).
      artifacts: [crates/repark-spark/src/tests/string_literal_escape_1.rs, task/ledgers/staging/string-literal-escape-1-ledger.md]
    - id: AT-4
      status: N/A
      justification: No shared or mutable state — the value engine is a pure function of (raw text, quote, verbatim flag); the flag arrives as a bool from session config and no ordering or reentrancy assumption exists.
    - id: AT-5
      status: N/A
      justification: No privileged action, no secret, no injection or deserialization surface — literal values already inside the engine, requoted through the existing exact round-trip.
    - id: AT-6
      status: ATTACKED
      evidence: Stored values move only toward Spark — the write read-back differs solely on the documented r'''' row; every value that matched Spark is byte-identical after (zero match-to-diff transitions, native door included); no migration or schema surface.
      artifacts: [python/repark/tests/test_string_literal_escape_1.py]
    - id: AT-7
      status: N/A
      justification: No resource or performance envelope change — the same single token pass with one value allocation per literal as before; nothing unbounded, nothing hot.
    - id: AT-8
      status: ATTACKED
      evidence: Dependency and size contracts honored — no sqlparser/fork or Cargo change; the downstream Generic behaviors relied upon (backslash-kept, own-quote doubling collapse) are measured, not presumed; the 1,000-line ceiling holds via a child-module split with lib.rs untouched and no ceiling raised; the byte-frozen SQP-1 leaf keeps its hash.
      artifacts: [crates/repark-spark/src/spark_literals/unescape.rs, crates/repark-spark/src/tests/string_literal_escape_1.rs]
    - id: AT-9
      status: N/A
      justification: No new failure path and no diagnostic change — remaining refusals keep their texts and locations, and error-to-value flips only remove errors Spark does not raise.
    - id: AT-10
      status: ATTACKED
      evidence: Red-first held — old behavior fails 5/6 new Rust pins and 6/6 facade tests, and passes again on the byte-exact restore; every new branch names a flipping input (quote arms, raw head/tail/verbatim splits, verbatim rewrite rule); no dead branch ships.
      artifacts: [crates/repark-spark/src/tests/string_literal_escape_1.rs, python/repark/tests/test_string_literal_escape_1.py]
```
