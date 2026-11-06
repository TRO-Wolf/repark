# Unit ledger — DQ-IDENTIFIERS-CONFIG-REFUSE-1 · a double-quoted-identifiers configuration is refused loudly, not accepted and ignored

**Date:** 2026-11-05 · **Base:** `77d8f2e5` (the first parent of the squash) · **Head:** `6cc7fd79` · **Squash:** `6a8e4c33` on main (PR #1266)
**Model:** builder `claude-opus-5-5` (the four unit commits) · verifiers `claude-opus-5-5` and `muse-spark-1.3-contributor` (one round each) · records `claude-haiku-5-5`
**Policy:** [../../../AGENTS.md](../../../AGENTS.md) · **Path:** STANDARD.

**Status:** completed 2026-11-05 at `6cc7fd79` (squashed to main as `6a8e4c33`, PR #1266) after the two verifiers. Both verdicts were PASS in their one round, with zero blocking findings. Clauses C-001 to C-008 are PROVEN or records; C-009 to C-018 are OPEN leads, gathered as the cells of card [DQ-IDENTIFIERS-CONFIG-RESIDUE-1](../../roadmap/mid-term/dq-identifiers-config-residue-1-card-2026-11-05.md). The narrow second re-audit of AUD-021 has not run, so C-098 of the format-v2 label is fixed, not re-audited.

**Why.** On Spark 4.1.2 with `spark.sql.ansi.doubleQuotedIdentifiers=true`, `"s"` reads as the column `s`. RePark read it as the string literal `'s'` and accepted the setting without effect. On a three-row probe table, `DELETE FROM t WHERE "s" = 's'` deleted every row where Spark deletes one, `UPDATE t SET id = 10 WHERE "s" = 'a'` changed no row where Spark changes one, and `INSERT INTO t VALUES (4, "x")` stored `'x'` where Spark refuses `UNRESOLVED_COLUMN`. The finding is AUD-021 (S1) of the format-v2 label re-audit. The names ruling of 2026-11-03 had filed the names cells for this configuration as a card, because they touched no table data; a DML probe writes, so the finding is a wrong write.

**Clause.** The fix, as the card proposes it: refuse the `SET` of this configuration loudly as unsupported, with a message that names the configuration; honouring it stays with IDENT-DIALECT-2. Sources: the audit's AUD-021 cells in [reaudit-cells-2026-11-05.jsonl](../../../docs/corpus/reaudit-cells-2026-11-05.jsonl); the names corpus group D2 in [names-rule-2026-11-03.md](../../../docs/corpus/names-rule-2026-11-03.md); the DML-reach rule of 2026-11-05 (each DML head that reaches a double-quoted name answers Spark's configuration-off answer after a refused set). Spark's answers were measured on Spark 4.1.2 with Iceberg 1.11.0 and are recorded in [dq_identifiers_config_refuse_1_cells.jsonl](../../../python/repark/tests/sql/data/dq_identifiers_config_refuse_1_cells.jsonl).

## PROPOSITION LEDGER — DQ-IDENTIFIERS-CONFIG-REFUSE-1 — 2026-11-05

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every path that sets `spark.sql.ansi.doubleQuotedIdentifiers` to true refuses with `UnsupportedOperationException` naming the configuration: SQL `SET` in each spelling, `conf.set`, and the builder both on a live session (reuse) and on a fresh build. | The facade pins on the three set paths, and the Rust refusal test in the Spark installer. Red: both Rust refusal sites turned to `Ok` make 51 of 51 facade tests fail. | PROVEN | Sites: `crates/repark-functions/src/ansi.rs` (`SPARK_SQL_ANSI_DOUBLE_QUOTED_IDENTIFIERS_KEY`, `parse_double_quoted_identifiers`, `double_quoted_identifiers_refusal_message`); `crates/repark-python/src/session_runtime.rs` (the runtime setter); `crates/repark-spark/src/extension.rs` (`refuse_double_quoted_identifiers` in `install_spark_session_conf`); the facade `builder_conf.py`, `session_core.py` and `sql_set_statements.py`. Both verifiers measured 11 of 11 set paths refused at head. The two Rust unit tests in `ansi.rs` are evidence only: the gate's pin rule does not bind a test in a `src/` file. |
| C-002 | A refused set leaves the configuration unchanged: `SET <key>` reads `false`, `conf.get` with a default answers `false`, `RESET <key>` and bare `RESET` answer as Spark does with the configuration off, and a builder `.config(key, "false")` builds and reads `false`. | The conf arm tests (each refusal reads back the default), the builder reuse and fresh-build test, and the builder-false and RESET test. | PROVEN | After each of the eleven refusals `SET <key>` reads `false`, and `getAll` and `get` hold no value (Opus claim 2, Muse AT-8). A refused set holds on a live clone: a `newSession` reads `false` and refuses on its own; the parent is unaffected (Muse). |
| C-003 | A value that is not a Spark boolean (`'true'` quoted, `maybe`, `1`, `yes`, `on`, `t`, empty) answers Spark's `INVALID_CONF_VALUE.TYPE_MISMATCH` text on the SQL and the conf paths, and `false` in any letter case is accepted as a no-op that reads back the raw value. | The SQL arm test (arms `set_quoted_true`, `set_bad_value`, `set_false`, `set_upper_false`) and the conf arm test (`conf_set_bad`, `conf_set_true_bool`). | PROVEN | Opus claim 2 and Muse claim 2: nine of nine non-boolean spellings give Spark's text exactly, on both paths. The Rust test `double_quoted_identifiers_parse_like_a_spark_boolean` in `ansi.rs` is evidence only (not bindable). |
| C-004 | The six DML-reach statements, run after a refused set, answer Spark's configuration-off answers. `DELETE FROM t WHERE "s" = 's'` deletes every row; `UPDATE ... WHERE "s" = 'a'` changes no row; `INSERT ... SELECT` and `INSERT ... VALUES` store the quoted text as a string; `SELECT "id"` answers `'id'`; `SELECT ... WHERE "s" = 'a'` matches no row. | The DML-reach pin on the config-on mode, which compares table state for DML and rows for SELECT with the Spark readback in the cells file, and asserts each answer differs from the config-on answer. | PROVEN | Both verifiers measured 6 of 6 matched. Muse also measured MERGE, CTAS, `insertInto`, `saveAsTable`, overwrite, a view, nested and non-ASCII names, and `df.filter` and `selectExpr`: each equals Spark's configuration-off answer. The pin covers the facade door only; the native door never reaches this set (lead C-010). |
| C-005 | The ANSI-mode ruling: the refusal holds under `spark.sql.ansi.enabled=false` as well as true. This is a deliberate over-refusal of `true` with ANSI off, where Spark accepts it as a no-op, because a later `SET spark.sql.ansi.enabled=true` would switch the configuration on in Spark. | The DML-reach pin in its `ansi_off_config_on` mode: ANSI set false, the key refused, then the six statements answer the configuration-off answers. | PROVEN | The orchestrator confirmed builder question 1 (refuse under both ANSI modes), as the builder's hand-back records it. The ANSI toggle probe (`ansi=false; dq=true; ansi=true`) leaves the configuration off on head. Muse recorded the padded value (`' true '`) as the same over-refusal: Spark accepts it as true, RePark refuses it and stays off. |
| C-006 | Controls: the names corpus group D2 moves no statement answer. Its facade door (the two double-quoted files, 152 cells over memory and hadoop, no view and view) answers the same, except that the enabled file's setup line `SET doubleQuotedIdentifiers=true` is now refused, and its 32 D2 cells and 2 D2c cascade rows answer the same under a refused set. The AUD-015 table is unchanged and its test passes. | none (records) | PROVEN (the builder's measurement; neither verifier re-ran the 152 names cells) | Builder control: no other names file sets or names the key (grep), so their rows cannot move, and they were not re-run. AUD-015 cells `ident_dialect_1_cells.jsonl` unchanged, sha256 `0507e12c…73377`, confirmed by Opus. |
| C-007 | Boundary and gates: the squash changes the thirteen files named in the evidence and no fork, Cargo, `.github`, reader, parser or `repark-sql` file. The `session_core.py` ratchet is lowered from 2277 to 2274 in `scripts/check_lib_py.py` and mirrored in `python/repark-parity/tests/test_cap_1_source_file_line_cap.py`, and the gates pass at head. | The CAP-1 test that equals the mirror, the gate's table and the live measured debt. Records: the exit codes in the evidence. | PROVEN | Opus claim 3 and Muse claim 3 matched the builder's counts (see Evidence). |
| C-008 | Hygiene: no code comment in any added Rust, Python, TOML or shell line (the added `#[test]` attributes are not comments); each unit commit ends in one `Authored-By` trailer; no local path, session link, account id, bucket, region or ARN in an added line. | none (records) | PROVEN | Both verifiers' diff scans: no added comment or docstring (Opus: the only added `#[...]` lines are attributes); four commits, each with one trailer; `make rust-panic-ban` exit 0 and `make check-map-size` exit 0 at head. |
| C-009 | Lead L1 (builder question 2): `spark.conf.get(key)` with no default raises "not set" in RePark, where Spark answers `false`. | Closing question: add the key to `_SQLCONF_DEFAULTS` with its pin (which moves `getAll` and the symbol-hash pin in `test_production_file_size.py`), or the owner's ruling that it is out of scope. | OPEN | Measured by the builder; carded, not changed. Card DQ-IDENTIFIERS-CONFIG-RESIDUE-1, cell L1. |
| C-010 | Lead L2 (builder question 3): `PyReparkSession.native(config={key: "true"})` is accepted, because that door installs no Spark extension; its SQL `SET` already refuses. | Closing question: whether the native config map refuses the key, then a pin. | OPEN | Measured by the builder; both verifiers observed it as carded, not as a finding. Card cell L2. |
| C-011 | Lead L3 (builder question 4): the fresh-build refusal keeps DataFusion's `This feature is not implemented:` prefix, and the runtime doors do not. | Closing question: strip the prefix so every door shares one message shape, or accept the prefix. | OPEN | Carded by the orchestrator's ruling (Q2 to Q4 carded, no change). Card cell L3. |
| C-012 | Lead L4 (Opus F1, S3 by the verifier): on a live session, `.config("spark.sql.ansi.enabled", "false").config(key, "true")` folds the first key into the store and the builder before the second key refuses. The refusal says the configuration is unchanged, yet `SET spark.sql.ansi.enabled` reads `false`, the engine stays ANSI on (`1/0` still raises), and after `RESET spark.sql.ansi.enabled` the planted `false` reaches the engine and `1/0` answers NULL. The fold is pre-existing: main accepts the same call and stores the same value, and the engine is untouched. | Closing question: fold atomically (validate every key before the first fold) or drop "unchanged" from the reuse-door text; grade the RESET wrong answer on its own. | OPEN | Reproduced by the verifier on the head wheel; the probe is not committed. Card cell L4. |
| C-013 | Lead L5 (Opus F2; a Muse observation): `SET -v` and bare `SET` omit the configuration key and the other documented keys that Spark lists with their defaults. The listing path is untouched by the unit and identical on main. | Closing question: list the key with `false` in `SET -v`, as `_SET_READ_DEFAULTS` answers `SET <key>`, and decide the other documented keys. | OPEN | Opus F2 (S3, a card); Muse: identical on main (`dd9298b0`) and head. Card cell L5. |
| C-014 | Lead L6 (Opus F3): on the SQL path a trailing comment is stripped from a set value, so `set spark.sql.ansi.doubleQuotedIdentifiers=true -- c` refuses with the UNSUPPORTED_FEATURE text where Spark answers `INVALID_CONF_VALUE.TYPE_MISMATCH` for the value `'true -- c'`, and `=false -- c` is accepted where Spark refuses. The comment stripping is pre-existing; nothing switches the configuration on and no name is read wrongly. | Closing question: keep the stripping (a Spark-visible change to every set) or match Spark's value text, then a pin. | OPEN | Opus F3 (S3, a card). Card cell L6. |
| C-015 | Lead L7 (Opus F4; a Muse observation): a multi-statement string (`SET k=true; SET k=false`, or `SET k=true; SELECT 1`) refuses on both sides with the configuration unchanged, but the text differs: RePark answers `PARSE_SYNTAX_ERROR` ("multiple SQL statements"), Spark answers `INVALID_SET_SYNTAX`. The text is pre-existing on main. | Closing question: match Spark's text on this path, or record it as the same class of loud refusal. | OPEN | Opus F4 (S3, a card); Muse: both loud-refuse, text differs. Card cell L7. |
| C-016 | Lead L8 (Opus F5, test adequacy): the AUD-021 cell test `test_aud021_cell_set_paths_refuse_and_the_statement_runs_under_the_default` compares RePark's answer after the refusal with RePark's own baseline, not with a recorded Spark configuration-off answer. The six DML-reach tests do compare with Spark's readback, and the verifier measured MERGE, `DELETE ... IN`, `UPDATE ... SET`, CTAS and `insertInto` against Spark's configuration-off answer (all equal). | Closing question: record Spark's configuration-off answer for the eleven AUD-021 facade heads, a test-adequacy change with no behaviour change. | OPEN | Opus F5 (test strength; the verifier gave no S-grade, filed at S3). Card cell L8. |
| C-017 | Lead L9 (Muse observation): a `df.where(F.expr(...))` with an integer cast raises a loud error whose class differs from Spark's. No wrong answer, and the table is unchanged. | Closing question: the error class, with a pin, in an error-contract unit, or a record that it is the same class as the cast unit's. | OPEN | Muse, pre-existing on main and head. Card cell L9. |
| C-018 | Lead L10 (Muse observation): a `MERGE` with a qualified double-quoted reference refuses on both sides with the table unchanged, but the error class differs from Spark's. | Closing question: as C-017. | OPEN | Muse, pre-existing on main and head. Card cell L10. |

## Evidence

**Verifier verdicts.** Opus 5.5 (`claude-opus-5-5`), one round: PASS, zero blocking findings, five notes (F1 to F5) and one count note (F6). Muse (`muse-spark-1.3-contributor`), one round: PASS, zero findings, four pre-existing observations (the `SET -v` listing, the multi-statement text, the `df.where` integer-cast error class, the MERGE qualified-reference error class). Opus ran read-attack only, on a detached clone at `6cc7fd79`, and left no product file changed. The two reports are untracked; their verdict lines and closure tables are transcribed here.

**Counts.** The facade file holds 51 tests with the records file present (33 AUD-021 cells across three set paths, plus 18). Without it, 18 pass and 3 skip with the reason `records-file-absent`; the builder's "33" counts cells, not skips (Opus F6, recorded at C-001). The full facade suite, `pytest python/repark/tests -n 8`: 23365 passed, 686 skipped, 150 xfailed without the records file (Opus, the builder's exact counts), and 23398 passed, 683 skipped, 150 xfailed, 0 failed with it (Muse). Rust: `repark-functions` 972 passed and 1 ignored; `repark-python` 196 passed; `repark-spark` 3177 passed and 5 ignored.

**Mutation.** Both Rust refusal sites turned to `Ok` and rebuilt: 51 of 51 facade tests red (49 with `DID NOT RAISE UnsupportedOperationException`, 2 on arm assertions). Restored green.

**Neighbour arms.** Muse ran about 120 neighbour arms against a live Spark 4.1.2 in one JVM: each matched Spark exactly or refused louder, with no wrong write and no double-quoted name read as a column. Both verifiers compared main with head on the sibling knobs (`spark.sql.ansi.enabled`, `spark.sql.caseSensitive`), the reuse fold and `SET -v`. Muse: 20 of 20 identical, and the 7 differing arms all touch the target key; Opus: the sibling knobs identical.

**Commits.** `42ad5e2b` (the Rust refusal), `baea9ea7` (the facade routing and the AUD-021 pins), `a3d44bdf` (the reuse-fold routing moved into `RuntimeConfig` to hold the `session_core` ratchet), `6cc7fd79` (the CAP-1 table mirror). Squashed to main as `6a8e4c33` (PR #1266).

**Builder.** Hand-back status DONE at `6cc7fd79`. Its four questions: question 1 confirmed (C-005); questions 2 to 4 carded (C-009 to C-011), no change.

**Baseline rows.** `scripts/check_lib_py.py`: the `session_core.py` row lowered from 2277 to 2274 in the code commit `a3d44bdf`, mirrored in `test_cap_1_source_file_line_cap.py` at `6cc7fd79` (exact; the guard refuses a shrink below baseline). No map row was lowered by the code commits.

## Scope notes

- The narrow second re-audit of AUD-021 has not run. C-098 of the format-v2 label is fixed by this unit, and the label stays unsigned.
- The label section of `docs/spark-sql-iceberg-parity.md` has no AUD-021 open item, and this records pass adds no line there. Its exclusions row for the double-quoted configuration still reads as a card (the pre-audit reading); the label text edit is the owner's order.
- The native door is unchanged (C-010). Honouring the configuration stays with IDENT-DIALECT-2.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: dq-identifiers-config-refuse-1
  complete: true
  reattested: [AT-1, AT-3, AT-6, AT-8, AT-10]
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Both verifiers checked the claims against measured results: the table at head, the counts, the mutation and the probe rerun (Muse, 67 of 67 identical to the builder's output; Opus, identical output on the head wheel).
      artifacts: [python/repark/tests/sql/test_dq_identifiers_config_refuse_1.py, python/repark/tests/sql/data/dq_identifiers_config_refuse_1_cells.jsonl, docs/corpus/reaudit-cells-2026-11-05.jsonl]
    - id: AT-2
      status: N/A
      justification: N/A — untouched by origin/main..6cc7fd79, as both verifiers classified it.
    - id: AT-3
      status: ATTACKED
      evidence: Both verifiers attacked the doors and their neighbours: SQL spellings, multi-statement strings, non-boolean values, padded and case-variant keys, the conf object, the config file, newSession, isModifiable, the DataFrame doors, and the reuse fold. Findings Opus F1 to F4 are leads C-012 to C-015, each graded S3 or a card.
      artifacts: [crates/repark-functions/src/ansi.rs, crates/repark-python/src/session_runtime.rs, crates/repark-spark/src/extension.rs, python/repark/src/repark/spark/session/builder_conf.py, python/repark/src/repark/spark/session/session_core.py, python/repark/src/repark/spark/session/sql_set_statements.py]
    - id: AT-4
      status: N/A
      justification: N/A — untouched by origin/main..6cc7fd79; the unit adds no concurrent-writer path.
    - id: AT-5
      status: N/A
      justification: N/A — untouched by origin/main..6cc7fd79; no authentication, input-injection or secrets surface in the diff.
    - id: AT-6
      status: ATTACKED
      evidence: The six DML-reach statements, measured against Spark's configuration-off answers, plus MERGE, CTAS, insertInto, saveAsTable, overwrite, views, nested and non-ASCII names; zero wrong writes found.
      artifacts: [python/repark/tests/sql/test_dq_identifiers_config_refuse_1.py, python/repark/tests/sql/data/dq_identifiers_config_refuse_1_cells.jsonl]
    - id: AT-7
      status: N/A
      justification: N/A — untouched by origin/main..6cc7fd79; the refusal adds no cost on a read or write path.
    - id: AT-8
      status: ATTACKED
      evidence: Refusals hold state: after each refusal the set reads false, getAll and get hold no value, a live clone stays at false, and RESET is stable. The siblings spark.sql.ansi.enabled and spark.sql.caseSensitive are identical on main and head.
      artifacts: [python/repark/src/repark/spark/session/session_core.py, python/repark/src/repark/spark/session/sql_set_statements.py, python/repark/tests/sql/test_dq_identifiers_config_refuse_1.py]
    - id: AT-9
      status: N/A
      justification: N/A — untouched by origin/main..6cc7fd79; no logging, metrics or alarm surface in the diff.
    - id: AT-10
      status: ATTACKED
      evidence: The mutation that turns both Rust refusal sites to Ok (51 of 51 red), the boundary check (thirteen files, no fork or parser change), the gate counts, the CAP-1 mirror and the hygiene scan.
      artifacts: [crates/repark-spark/src/tests/spark_dialect.rs, python/repark/tests/sql/test_dq_identifiers_config_refuse_1.py, python/repark-parity/tests/test_cap_1_source_file_line_cap.py]
```

## Findings

```yaml
FINDING:
  id: OPUS-F1
  severity: S3
  category: AT-3
  clause: C-012
  claim: On a live session, a builder that folds spark.sql.ansi.enabled=false and then refuses the key leaves the first key folded, so the refusal text says the configuration is unchanged while the store and the builder hold false and a later RESET plants false into the engine (1/0 answers NULL).
  evidence: Opus verdict F1 with the verifier's fold probe (arm ansi_then_key); main accepts the same call and stores the same value.
  disposition: OPEN (lead L4, C-012; card DQ-IDENTIFIERS-CONFIG-RESIDUE-1, cell L4)
```

```yaml
FINDING:
  id: OPUS-F2
  severity: S3
  category: AT-3
  clause: C-013
  claim: SET -v and bare SET omit the configuration key, while Spark lists it with its default false and its doc.
  evidence: Opus verdict F2 (probe arm set_v_fresh); the listing is identical on main.
  disposition: OPEN (lead L5, C-013; card DQ-IDENTIFIERS-CONFIG-RESIDUE-1, cell L5)
```

```yaml
FINDING:
  id: OPUS-F3
  severity: S3
  category: AT-3
  clause: C-014
  claim: A trailing comment is stripped from a SET value on the SQL path, so the refusal fires where Spark answers TYPE_MISMATCH and the false form is accepted where Spark refuses; no state is read wrongly.
  evidence: Opus verdict F3 (probe arm multi, statement 4); the stripping is pre-existing.
  disposition: OPEN (lead L6, C-014; card DQ-IDENTIFIERS-CONFIG-RESIDUE-1, cell L6)
```

```yaml
FINDING:
  id: OPUS-F4
  severity: S3
  category: AT-3
  clause: C-015
  claim: A multi-statement SET string refuses on both sides with the configuration unchanged, but the error text differs from Spark's INVALID_SET_SYNTAX.
  evidence: Opus verdict F4 (probe arm multi, statement 6); identical on main.
  disposition: OPEN (lead L7, C-015; card DQ-IDENTIFIERS-CONFIG-RESIDUE-1, cell L7)
```

```yaml
FINDING:
  id: OPUS-F5
  severity: S3
  category: AT-1
  clause: C-016
  claim: The AUD-021 cell test compares RePark's answer after the refusal with RePark's own baseline, not with a recorded Spark configuration-off answer, so it does not pin Spark's answer by itself.
  evidence: Opus verdict F5 (test file lines 120 to 140); the six DML-reach tests do pin Spark's answers, and the verifier measured five further statements against Spark.
  disposition: OPEN (lead L8, C-016; card DQ-IDENTIFIERS-CONFIG-RESIDUE-1, cell L8)
```

```yaml
FINDING:
  id: OPUS-F6
  severity: S3
  category: AT-1
  clause: C-001
  claim: The count of 33 for the AUD-021 cells is a count of cells, not of skips; without the records file the facade file gives 18 passing tests and 3 skipping tests.
  evidence: Opus verdict F6; the counts in the Counts paragraph above.
  disposition: REMEDIATED (the ledger states 51 tests with the records file present, and 18 passing with 3 skips without it)
```

The unit's residue has no blocking finding. Opus F1 to F5 and the Muse observations are the leads C-012 to C-018 and the cells of [DQ-IDENTIFIERS-CONFIG-RESIDUE-1](../../roadmap/mid-term/dq-identifiers-config-residue-1-card-2026-11-05.md).
