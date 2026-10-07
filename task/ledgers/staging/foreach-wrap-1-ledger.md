# Unit ledger — FOREACH-WRAP-1 · `foreach` / `foreachPartition` errors masked, `transform` as Spark raises it

**Date:** 2026-10-07 · **Branch:** `fix/foreach-wrap-1` · **Base:** `7a8fcf1a` (`main`, v1.5.3)
· **Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: high** (security).

**Order:** the orchestrator's brief FOREACH-WRAP-1, the follow-up the v1.5.3 notes name as a known
limit of SOURCE-URL-REDACT-2 ([its ledger](source-url-redact-1-ledger.md), fold 4). Round 2
carries the ruling on round 1's Q1: answer (A).

**The defect.** SOURCE-URL-REDACT-2 masks every user-callback door except `foreach`,
`foreachPartition` and `transform`. Those let the user's own exception through unchanged, so a
credential in its message stays visible. The re-verify-4 probe
`reverify4/probes/doors4.py` showed 1 leak for each door in both `plain` and `surrogate` modes.

**Step 0, measured on live Spark 4.1.2 (commit `61b4f5f2`).** The recorder
`python/repark-parity/tests/live_spark/fw1_callback_oracle.py` records three cells, and a re-run
is byte-identical. `FW1-foreach` and `FW1-foreachPartition` raise `py4j.protocol.Py4JJavaError`,
with no `getErrorClass`, no Python cause and `__context__` None. The JVM chain is
`SparkException` over `api.python.PythonException`, and the user's message, credential
included, is inside `str()` (`secret_in_str` true). `FW1-transform` raises the user's own
`ValueError`, unchanged (`is_original` true).

**Ruling on Q1 (orchestrator, round 2): (A).** RePark has no `Py4JJavaError`.
`foreach` and `foreachPartition` keep the user's class, as `DF-FOREACH-1` declares, but raise
`scrub_exception`'s masked copy after the handler. This is stricter than Spark, by the security
ruling "no secret on any door". `transform` keeps the passthrough, matching Spark, under the
dated registry row `DF-TRANSFORM-1`.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `foreach` and `foreachPartition` (`dataframe/surface_b.py`) catch an `Exception` from the loop, keep `failure = scrub_exception(error)` and raise it after the handler. A user exception carrying a credential leaves as a copy of the same class (`DF-FOREACH-1`), with `str`, `repr` and the formatted traceback masked (`http://u:***@…`), and with `__context__` and `__cause__` None. A lone surrogate in the message masks the same way. | `test_foreach_door_raises_a_masked_copy_of_the_user_class` (`foreach` and `foreachPartition` × `plain` and `surrogate`). | PROVEN | 4 cells red on `61b4f5f2` (the raised object is the user's own) and green on the head. Mutation M-1 (`failure = error` at the `foreach` door) recorded red: 2 cells, `foreach-plain` and `foreach-surrogate`. Mutation M-2 (the `foreach` raise moved back inside its `except`, even `from None`) recorded red: the same 2 cells, `__context__` is the raw `ValueError`. Both reverted; the file is byte-identical to the head. `doors4.py plain` and `doors4.py surrogate` each print `TOTAL 1`: `foreach` and `foreachPartition` show `type=ValueError str_leak=False repr_leak=False fmt_leak_lines=0 ctx=NoneType`, and the 1 is `transform` (C-003). |
| C-002 | A credential-free exception from `f` leaves `foreach` and `foreachPartition` as the user's own object, its text and `args` byte for byte, with `__context__` None. The existing `DF-FOREACH-1` pin (the class is kept) holds unchanged. | `test_foreach_door_keeps_a_credential_free_error` (both doors); `test_df_surface_b_1.py::test_foreach_propagates_user_exception`. | PROVEN | Green on `61b4f5f2` and on the head: they guard identity, and they stay green under M-1 and M-2. |
| C-003 | `transform` keeps the passthrough: an exception `func` raises propagates as that same object, unmasked, as Spark 4.1.2 raises it (`FW1-transform`: `builtins.ValueError`, `is_original` true). The registry row `DF-TRANSFORM-1` is DECLARED 2026-10-07: the user's own exception from `transform` is not masked, because it never crosses an engine boundary, which is exactly what Spark does. | `test_transform_passes_the_user_exception_through`; the `DF-TRANSFORM-1` row in `docs/spark-sql-iceberg-parity.md`. | PROVEN | Green on both. `transform` (`dataframe/core.py`) is unchanged, and `doors4.py` shows it as the one remaining line in both modes. |
| C-004 | Spark 4.1.2's behaviour for the three doors is recorded live in the repo's recorder pattern, and the registry rows cite it: `DF-FOREACH-1` notes that Spark shows the credential inside `Py4JJavaError`'s `str()` (`FW1-foreach`, `FW1-foreachPartition`), and `DF-TRANSFORM-1` cites `FW1-transform`. | `fw1_callback_oracle.json` with its `.sha256`; the pins read the cells. | PROVEN | Commit `61b4f5f2`; `sha256sum -c fw1_callback_oracle.sha256` OK. The C-001 and C-003 pins assert `secret_in_str`, `has_get_error_class`, `is_original` and the class from the recording. |

## Also updated

- `docs/spark-sql-iceberg-parity.md`: `DF-FOREACH-1` (repark half, Spark half with the FW1
  cells, pins, rationale) and the new `DF-TRANSFORM-1`.
- [The v1.5.3 card](../../roadmap/mid-term/v1-5-3-card-2026-10-04.md), SOURCE-URL-REDACT-2 row:
  the known limit, closed after v1.5.3.
- [source-url-redact-1-ledger.md](source-url-redact-1-ledger.md), fold 4's out-of-scope
  paragraph: closed by this unit. The v1.5.3 release notes stay as shipped; they describe that
  release.

## Gates (2026-10-07, debug wheel `make develop` on the head)

- The five redaction test files plus `test_foreach_wrap_1.py`, `-n 8`: 253 passed.
- `pytest python/repark/tests -n 8 -k "foreach or partition or transform"`: 747 passed,
  58 skipped, 1 xfailed.
- `test_production_file_size.py`: 11 passed. Ruff check and format: clean.
- `check_lib_py`, `check_python_conventions`, `check_docstring_presence`, `sync_map_md --check`,
  `check_ledger_grammar`: clean.
- `comment_ban.py /tmp/xfw1 origin/main HEAD`: 0 hits.

```
COVERAGE_ATTESTATION:
  pr_unit: foreach-wrap-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each ruling item maps to a clause. The masked copy raised below the handler is C-001; the credential-free identity is C-002; the transform passthrough with its dated DECL row is C-003; the FW1 citation in DF-FOREACH-1 is C-004.
      artifacts: [python/repark/src/repark/spark/dataframe/surface_b.py, python/repark/tests/test_foreach_wrap_1.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-2
      status: ATTACKED
      evidence: Both doors are exercised with a plain message, a lone-surrogate message and a credential-free message; foreachPartition raises from inside the row iterator.
      artifacts: [python/repark/tests/test_foreach_wrap_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The failure path is the subject. The raise sits after the handler, so __context__ is None (M-2 red), and an engine error from toLocalIterator or to_arrow_batches inside the try takes the same scrub, keeping its identity when nothing masks.
      artifacts: [python/repark/src/repark/spark/dataframe/surface_b.py]
    - id: AT-4
      status: N/A
      justification: No shared state is added; failure is a local, and the generator left suspended by the raise is closed by the runtime as before.
    - id: AT-5
      status: ATTACKED
      evidence: str, repr and format_exception carry no userinfo on both doors, in both doors4.py modes; transform is the one ruled exception, matching Spark.
      artifacts: [python/repark/tests/test_foreach_wrap_1.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-6
      status: N/A
      justification: No data path changes; the rows each callback sees are unchanged, and the DF-FOREACH-1 visit pins pass.
    - id: AT-7
      status: N/A
      justification: One try block around the existing loop; no new allocation on the success path.
    - id: AT-8
      status: ATTACKED
      evidence: The error contract is the registry's. DF-FOREACH-1 keeps the user's class, and its pin test_foreach_propagates_user_exception is unchanged and green; DF-TRANSFORM-1 records the passthrough against the live FW1-transform cell.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/test_df_surface_b_1.py]
    - id: AT-9
      status: ATTACKED
      evidence: The masked copy keeps the user's traceback frames and the class, so the failure stays diagnosable with only the credential masked.
      artifacts: [python/repark/tests/test_foreach_wrap_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: 4 cells are red on the base. M-1 (scrub dropped) and M-2 (raise inside the handler) are each red on 2 cells. Every branch the diff adds has an input that changes its output: a secret flips the copy, and no error leaves failure None.
      artifacts: [python/repark/tests/test_foreach_wrap_1.py]
  complete: true
```
