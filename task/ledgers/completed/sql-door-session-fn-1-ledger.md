# Unit ledger — SQL-DOOR-SESSION-FN-1 · session functions on the Spark SQL door

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when SQL-DOOR-SESSION-FN-1 merges, or when the owner closes the slate row.

**Unit:** SQL-DOOR-SESSION-FN-1 · **Date:** 2026-09-06 · **Executor:** Muse Spark (muse-spark-1.3), Actor ·
**Branch:** `fix/sql-door-session-fn-1` · **Base:** `origin/main` `e100f72d` (v1.1.1)
**Model:** muse-spark-1.3
**risk_tier:** standard.

Spark is the oracle. Live PySpark 4.1.2 (uv.lock pin), zulu-17, `local[2]`, ANSI on, 2026-09-06.
Banner proof is the measured `SELECT version()` cell below. Session-timezone banner omitted:
every cell here is a timezone-free string identity function. One JVM at a time; zero linger.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Four cells (`user()`, `current_user()`, `session_user()`, `version()`) plus bare forms, expression/WHERE/FROM-`t` forms, shadowing and arity probes measured on both repark doors before the fix and on live Spark. | Oracle tables below. | **PROVEN** |
| C-002 | Spark-door UDFs answer the facade strings: the three user spellings answer `repark` (equal to `F.current_user()`), `version()` answers `repark-<workspace>` (equal to `F.version()`); Utf8 non-null; native door untouched. | Kernels + registration + pins. | **PROVEN** |
| C-003 | The Spark door accepts the parenthesised user calls (top level, inside expressions, WHERE, FROM-`t`) via a Databricks retry gated on a token sniff; bare forms keep today's column-or-error behavior; a retry that also fails returns the original Generic error. | Router/passthrough + pins. | **PROVEN** |
| C-004 | Pins per form (Rust parse+evaluate, facade Spark-door cells+expressions, live shape pin) are red before the fix and green after; unregister mutation reds the suite. | Pin files + mutation table + pytest summaries. | **PROVEN** |
| C-005 | Registry rows `SQL-SESSION-FN-1` and `SQL-VERSION-1` filed with per-door cells and the bare-form/native residues; maps lockstep; attestation with pytest counts. | Registry + maps + attestation. | **PROVEN** |

## Oracle (live PySpark 4.1.2, 2026-09-06, JDK 17, `local[2]`, ANSI on)

| Cell | Spark | repark Spark door before | repark Spark door after |
|---|---|---|---|
| `SELECT user()` | `john` string col `user()` | `ParseException` col 12 | `repark` Utf8 non-null |
| `SELECT current_user()` | `john` col `current_user()` | `ParseException` col 20 | `repark` Utf8 non-null |
| `SELECT session_user()` | `john` col `session_user()` | `ParseException` col 20 | `repark` Utf8 non-null |
| `SELECT version()` | `4.1.2 f0bb2e6a…597954` col `version()` | `Apache DataFusion 54.1.0, x86_64 on linux` | `repark-1.1.1` Utf8 non-null |
| `SELECT user` / `current_user` / `session_user` | `john` (col `current_user()`) | `No field named <name>` | unchanged (fence) |
| `SELECT concat(user(), 'x')` | `johnx` | `ParseException` col 19 | `reparkx` |
| `SELECT concat(version(), 'x')` | `4.1.2 …954x` | `Apache DataFusion …xx` | `repark-1.1.1x` |
| `SELECT current_user() = 'nosuchuser_xyz'` | `False` boolean | `ParseException` | `False` boolean |
| `SELECT current_user() AS c, id FROM t` | 3 rows `(john, id)` | `ParseException` | 3 rows `(repark, id)` |
| `SELECT id FROM t WHERE current_user() = '…'` | 0 rows | `ParseException` col 60 | 0 rows |
| `SELECT user FROM shadow` (shadowed) | `colval` (column wins) | `colval` | unchanged (fence) |
| `SELECT version FROM shadow` (shadowed) | `vval` | `vval` | unchanged (fence) |
| `SELECT user(1)` | `WRONG_NUM_ARGS` | `ParseException` | loud plan error naming `user` |
| Native door `SELECT user()` | n/a (Spark has one door) | `ParseException` | unchanged (fence) |
| Native door `SELECT version()` | n/a | `Apache DataFusion …` | unchanged (fence) |

The repark login value is the product default `repark`, not the OS login (ADR-0004 forbids
an OS-user read); only the shape is pinned against live Spark.

## Design

Root cause, measured on sqlparser 0.62.0: the executing parse runs under the Generic dialect,
whose `parse_expr_prefix_by_reserved_word` reads `user`/`current_user`/`session_user` as
paren-less special functions and rejects the `(` before the function router runs. The router's
own Databricks parse accepts the calls. The pinned parser cannot change, so the fix sits in
repark's layer: `spark_ast` retries a failed Generic parse with the Databricks dialect when the
`normalize` token sniff sees an unquoted user word followed by `(`, and returns the original
error when the retry also fails. Four nullary Immutable Utf8 UDFs (`session_functions.rs`,
registered from `register_all`, Spark door only) answer the facade strings. A second AST rewrite
restores bare no-paren user-function nodes to (compound) identifiers, because DataFusion's
planner gives UDFs precedence over columns — without it, registering `user` would flip
`SELECT user FROM t` from the column to the session string (Spark answers the column).

## Kernels

| Name | Layer |
|---|---|
| `user`, `current_user`, `session_user` | `session_functions.rs` `SessionUser`, nullary Immutable, Utf8 non-null, Scalar `repark` |
| `version` | `session_functions.rs` `SessionVersion`, nullary Immutable, Utf8 non-null, Scalar `repark-<CARGO_PKG_VERSION>` |
| parse retry | `spark_ast.rs` `parse_with_session_user_fallback`, sniff-gated, original error preserved |
| bare preserve | `spark_ast.rs` `restore_bare_session_user_columns`, Function-None to (compound) identifier |

## Mutation

| Knob | Red |
|---|---|
| sniff returns `false` | `tests::session_functions` suite red + both sniff unit tests red |
| drop both `restore_bare_session_user_columns` calls | `spark_door_bare_user_keeps_column_or_error` red (shadow answers `repark`); bare-version pin stays green, proving `version` needs no rewrite |
| unregister `session_functions` (registration line out) | 7 red of 11 (parse succeeds via retry, plan fails `Invalid function 'user'`); 3 fences + arity stay green |

## Pins

- `crates/repark-spark/src/tests/session_functions.rs` — 11 door tests. Red run:
  `8 failed, 3 passed` (fences green by design); green run: `11 passed`.
- `crates/repark-functions/src/session_functions.rs` `tests` — 4 kernel tests, green.
- `crates/repark-spark/src/tests/normalize.rs` — 2 sniff tests, green; bite-proven by
  the sniff-false mutation (no green-before run: the helper did not exist).
- `python/repark/tests/test_sql_door_session_fn_1.py` — 6 JVM-free tests + 1 live test.
  Red run: `4 failed, 2 passed, 1 skipped`; green run:
  `8 passed, 1 skipped` (with `test_examples_functions_b.py`); live leg
  `REPARK_PARITY_LIVE=1`: `7 passed`. The live pin's repark leg shares the exact door
  path proven red pre-fix, so no separate live red run was needed.

## Attestation

Actor (muse-spark-1.3) attests, no separate Critic on this lane: every C-001..C-005 clause
carries a live-Spark-measured pin; every red classified in §Pins with zero unexplained reds;
per-branch mutations (§Mutation) each red their named subset; both SQL doors carry pins per
fixed row (Spark-door fix pins, native-door fence pins). `LOGIC_SCORE` 5/5.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: sql-door-session-fn-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each C-001..C-005 clause walked against its pins; all four cells assert the facade string on repark-only, live-shape, and Rust-door legs.
      artifacts: [python/repark/tests/test_sql_door_session_fn_1.py, crates/repark-spark/src/tests/session_functions.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Expression/WHERE/FROM-t cells, case-insensitive spellings, quoted/lookalike/bare sniff negatives, comment/whitespace between name and paren, broken-call original-error fence, and inline/partitioned/named/nested/quoted window refusals through both executing parse paths.
      artifacts: [crates/repark-spark/src/tests/session_functions.rs, crates/repark-spark/src/tests/normalize.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Arity refuses loud naming the function; zero-argument window calls carry UNSUPPORTED_EXPR_FOR_WINDOW, the function expression, and SQLSTATE 42P20; argument-bearing and DISTINCT forms retain their prior planner errors; bare uses and the native door stay fenced.
      artifacts: [python/repark/tests/test_sql_door_session_fn_1.py, crates/repark-spark/src/tests/session_functions.rs]
    - id: AT-4
      status: N/A
      justification: Scalar UDFs are pure functions of zero inputs and the AST rewrites are pure tree walks; no shared mutable state, no ordering assumption, no concurrency surface.
    - id: AT-5
      status: N/A
      justification: No auth, secret, deserialization, or path handling; the identity is a constant and the version is the build stamp.
    - id: AT-6
      status: ATTACKED
      evidence: Native-door fences, including version() OVER (), guard the non-Spark door against the Spark-only registration; registry rows SQL-SESSION-FN-1/SQL-VERSION-1 retain the per-door cells and dated residues.
      artifacts: [python/repark/tests/test_sql_door_session_fn_1.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: N/A
      justification: Nullary scalar evaluation and a single token scan plus one AST walk per statement; no recursion over user input beyond the parser's own, no new hot loop.
    - id: AT-8
      status: ATTACKED
      evidence: Live Spark 4.1.2 is the contract on the identity, version, and four window-refusal cells; the 2026-09-08 finalization run passed 22 tests against Spark 4.1.2 under UTC. Original Generic errors still survive a failed retry; zero dependency or manifest changes.
      artifacts: [python/repark/tests/test_sql_door_session_fn_1.py]
    - id: AT-9
      status: N/A
      justification: Synchronous library with no ops surface; every failure reaches the caller as a typed error, so there is no log/metric/alarm path to diagnose.
    - id: AT-10
      status: ATTACKED
      evidence: Original red-first battery plus the repaired candidate's focused Rust 14/14 and facade 17/17 JVM-free runs; the direct Databricks lambda parse and Generic fallback both exercise the window visitor. Every added branch has a nameable flipping input.
      artifacts: [crates/repark-spark/src/tests/session_functions.rs, crates/repark-spark/src/tests/normalize.rs, python/repark/tests/test_sql_door_session_fn_1.py]
  reattested: [AT-1, AT-2, AT-3, AT-6, AT-8, AT-10]
  complete: true
```

## Out of scope observed

- `F.expr("user()")` / `filter_sql` (`Column::sql`, repark-python) parses fragments with the
  Generic dialect on a throwaway context and still rejects the parenthesised user calls.
  Same defect family, different entry point; left for its own unit.
- Shared Spark registration makes `F.expr("version()")` answer `repark-1.1.1`. Main's lambda
  dialect also makes a lambda fragment containing `user()` answer `repark`. These Spark-closer
  passenger paths are measured S3 scope, not part of the plain `F.expr("user()")` claim.

## Resume repair plan (2026-09-07)

Live Spark 4.1.2 UTC rejects `user()`, `current_user()`, `session_user()`, and `version()` when
each has `OVER`; the exact measured errors are retained in
`/tmp/codex-resume-411-evidence/oracle-results.json`. RePark accepted the same calls and returned
the facade strings because DataFusion dropped or ignored the window form. The repair visits the
executing Spark-door AST after the bare-name restoration and refuses unqualified calls to those
four names with an `OVER` clause, including quoted function names. It preserves ordinary calls,
real window functions, quoted shadow columns, and the native SQL door. The test battery covers
inline, partitioned, named-window, nested, and quoted-call forms, plus real-window and
quoted-shadow controls. Argument-bearing window forms preserve planner validation: Spark reports
wrong arity for `(1)` and unresolved `DISTINCT`; RePark's planner wording is a declared residual,
but these forms must refuse without the window-expression class.

## Resume verification (2026-09-07)

The six public window-refusal pins fail against the freshly built original PR head. The repaired
native module passes the dedicated facade file: 17 passed with the live tier disabled, then
22 passed with live Spark 4.1.2 under UTC. The live file now mirrors the four window refusals.
`make verify` completed with exit 0 after restoring the scratch clone's missing `origin/main`
reference to the verified base `c81ba043b007514ef0dd7f82f7b8b4121f101b50`.
The independent resumed review found no remaining repair findings; its explicit coverage
attestation is `/tmp/codex-resume-411-evidence/final-repair-attestation.md`.
No commit, push, or delivery action had occurred at this verification point. The owner authorized
the ledger departure move on 2026-09-08 after final readiness review.

## Main reconciliation and remediation disposition (2026-09-08)

The finalization candidate is based on GitHub `main` `f00ed9ea31f6708f50fdfdda26b5facd4b53d529`.
PR head `f46b4fb6ebab75f5528141a5529749b2597efe38` and the window repair were applied as a
working-tree delta. The union keeps main's FNP-8 lambda-dialect selection and DFCORE registry
record. The session retry remains the original narrow token-gated path. The AST visitor sees
zero-argument calls as `FunctionArguments::List` with empty arguments, no duplicate treatment,
and no clauses. The focused test now reaches that representation through both the Generic retry
and a direct Databricks lambda parse.

| Finding | Disposition | Evidence |
|---|---|---|
| GLM-F1 (session scalar `OVER` accepted) | **REMEDIATED** | `spark_door_session_scalars_refuse_window_use` and `test_spark_door_session_scalars_refuse_window_use` were red on the original PR head and are green on this candidate. They pin the class, expression text, and SQLSTATE. |
| GLM-F2 (whole-statement Databricks retry) | **ACCEPTED_FLAGGED (S3)** | The final critic sustained the mechanism below the S1 floor. The retry runs only after the configured executing dialect fails and the quote-aware token sniff matches. `spark_door_broken_user_call_keeps_original_error` proves a failed retry returns the original error. |
| GLM-F3 (compound bare-user restore arm) | **ACCEPTED_FLAGGED** | The branch has no demonstrated wrong-result input in the delivered surface. Removing it is cleanup outside this repair and remains below the S1 floor. |
| GLM-F4 (arity validation timing and wording) | **ACCEPTED_FLAGGED** | All four argument-bearing window calls and `user(DISTINCT)` refuse without the window class. The registry already declares the DataFusion wording residue. The handed scope excludes changing arity semantics. |
| GLM-F5 (kernel `is_err` assertions) | **ACCEPTED_FLAGGED** | Door-level Rust and Python tests name the function and exercise the public error path. The kernel assertions remain supplemental and the finding is below the S1 floor. |
| GLM-F6 (unmeasured contexts) | **PARTIALLY WITHDRAWN; ACCEPTED_FLAGGED remainder** | The final critic measured CTE, subquery, GROUP BY, HAVING, and ORDER BY contexts. Output-column naming remains unpinned below the S1 floor. |
| GLM-F7 (Python evidence gap) | **REMEDIATED** | The candidate-built native module loaded from this checkout. The dedicated file passed 17 tests with five live-only skips, then 22 tests against live Spark 4.1.2 under UTC. |
| GLM-F8 (main integration) | **WITHDRAWN** | The final candidate and review use current `main` `f00ed9ea`; the FNP-8 lambda route remains intact. |
| F-411-R1 (`F.expr` passengers) | **ACCEPTED_FLAGGED (S3)** | Plain `F.expr("user()")` still refuses, while `F.expr("version()")` and lambda-contained `user()` now resolve through shared registration and main's lambda dialect. |
| F-411-R2 (mixed-case window text) | **ACCEPTED_FLAGGED (S3)** | Mixed-case unquoted names retain source case in the quoted error expression. Lowercase class, expression, and SQLSTATE match the live pins. |

Current candidate evidence: `cargo test -p repark-functions --lib session_functions --locked
--offline` passed 4/4; `cargo test -p repark-spark --lib session_functions --locked --offline`
passed 14/14; `cargo test -p repark-spark --lib session_user_sniff --locked --offline` passed
2/2; the dedicated facade file passed 17 with five live-only skips and 22/22 with live Spark
4.1.2. `make verify` passed with exit 0 after the root manifest stayed at its 175-line ceiling.
No dependency, lock, workflow, AWS, commit, push, or PR mutation occurred.

## Final independent review and departure (2026-09-08)

The independent Grok review used an isolated clone and the exact 15-file candidate patch,
SHA-256 `12d59fb0325047ce764a0756cc0fa149fcae7febfa55c7a235f625e12cbc2d66`.
It matched every load-bearing source, test, map, registry, and ledger byte against the actor
candidate. The review reran 20 focused Rust tests and the JVM-free facade file (17 passed,
5 live-only skipped), ran fresh public-entry probes, and issued **CLEAN** at the S1 floor.
The actor's byte-identical candidate had already passed the 22-test live Spark 4.1.2 leg,
`make verify`, and `make preflight`.

No finding at or above S1 remains. The final review sustained the S3 fallback, dead-branch,
arity-wording, broad kernel-assertion, and output-name residues recorded above. It also filed
F-411-R1 and F-411-R2. The registry pin prose now names the window tests, closing its S3
documentation lag. These disclosures remain visible for delivery review; this ledger does not
claim a separate acceptance decision for each flag.

The owner authorized finalization without authorizing a merge. The sanctioned lifecycle command
moved this ledger from `staging/` to `completed/`; the next post-merge pickup files it in the
dated archive.
