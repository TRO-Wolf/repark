# Unit ledger — WO UUID-CAST-WINDOW-1 · the UUID refusal's SQL window counts as Spark counts

**Date:** 2026-09-27 · **Branch:** `fix/uuid-cast-window-1` · **Base:** `c4a45139`
(`origin/main`) **Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** `uuid_cast_message` counts the `== SQL (line, position) ==` window in
bytes: the position is a byte offset and the window slices the line at a byte index,
which panics inside a multi-byte character and misplaces the caret on any non-ASCII
SQL before the `UUID` token. The NTZ-1 fold assumed Spark counts characters everywhere;
this unit measures Spark 4.1.2's actual UUID-refusal window and matches it exactly.

**What Spark does (measured 2026-09-27; Spark probes `probe-out.json` and
`probe2-out.json` under `/tmp/oc-worker/direct/wo/uuid-cast-window-1-probes/`, two JVM
runs, session timezone UTC).** The `position` counts Unicode scalar values, 1-based:
forty `é` before the token reads 69, thirty-four emoji reads 63 (UTF-16 code units
would read 97), and forty decomposed `e`+U+0301 reads 109 (scalar count, not
grapheme). The window cuts apply those char-derived counts as UTF-16 code-unit
indices into the line: the left cut sits 32 before the token's char index (case 4
skips 30 units and shows 23 emoji, not 12) and the right cut 36 past it
(`...UU...`), with `...` on each cut side; the caret pad is 35 when the left cut
applies, else position minus one, and always four carets. Twelve boundary probes pin
the rule: no-cut lines (24, 73 units), the 55/68-unit fragment caps, the left+right
combo at 157 units, the exact-100/101-unit windowed lines, and the newline leg (line
2, position 13). A cut that splits a surrogate pair renders `?` through the PySpark
door (measured on a UTF-8 box, R-1). The ASCII control is byte-identical to RePark
main. The oracle
[uuid_cast_window_1_spark_oracle.json](../../../python/repark/tests/uuid_cast_window_1_spark_oracle.json)
keeps the four SQL texts and Spark's full messages, dropping only PySpark's leading
and trailing newline.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The UUID refusal's window and position count as Spark counts — the position in Unicode scalar values, the window cuts at UTF-16 code-unit offsets (32 left, 36 past the token start, `...` on each cut side), the caret pad 35 or position minus one with four carets — and never panic on non-ASCII SQL. | Rust pins plus the facade replay byte-compare all four oracle messages; a sweep covers 2/3/4-byte cells across alignments. | PROVEN | [uuid_cast.rs](../../../crates/repark-spark/src/uuid_cast.rs) `uuid_window_counts_characters`, `uuid_window_does_not_panic_on_non_ascii`; [test_uuid_cast_window_1.py](../../../python/repark/tests/test_uuid_cast_window_1.py) all four cases `==`, 4 passed. |

## Mutation record (2026-09-27)

Each line was broken, the named tests ran, and the file was restored.

| # | Mutation | Red |
|---|---|---|
| M1 | Revert to byte slicing (`column = offset - line_start + 1`, `&line[window_start..]`). | `uuid_window_counts_characters` reds (answers position 109 with 6 `é`, wants 69 with 12); `uuid_window_does_not_panic_on_non_ascii` reds (the byte slice panics on the odd-head sweep leg); restored green. The facade pins red on the same byte code (main red probe: case 2 position 109, case 4 position 165). |

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: uuid-cast-window-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The clause is walked against Spark's measured answer on the same statements; the Rust pins and the facade replay byte-compare every recorded message.
      artifacts: [crates/repark-spark/src/uuid_cast.rs, python/repark/tests/test_uuid_cast_window_1.py, python/repark/tests/uuid_cast_window_1_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Every named shape is pinned (the ASCII control, the 2-byte run, the newline split, the astral run with its right truncation), plus a 2/3/4-byte cell sweep over sixty counts and two head widths.
      artifacts: [crates/repark-spark/src/uuid_cast.rs, python/repark/tests/test_uuid_cast_window_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The byte-slice panic is replaced by unit-safe slicing; the sweep holds the position header on every alignment without panicking.
      artifacts: [crates/repark-spark/src/uuid_cast.rs]
    - id: AT-4
      status: N/A
      justification: No commit, isolation or concurrency surface; the window is a pure function of the SQL text.
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling; SQL text only.
    - id: AT-6
      status: N/A
      justification: No stored format changes; the oracle JSON is a new test fixture, not a product format.
    - id: AT-7
      status: N/A
      justification: No performance claim; the window walks the line once.
    - id: AT-8
      status: ATTACKED
      evidence: No dependency, no crate edge, no NTZ touch; Spark's 32/36-unit window contract is pinned from measurement rather than presumed from the NTZ shape.
      artifacts: [python/repark/tests/uuid_cast_window_1_spark_oracle.json, python/repark/tests/test_uuid_cast_window_1.py]
    - id: AT-9
      status: ATTACKED
      evidence: The byte-slice panic and the misplaced window both fail their pins red-then-green under M1; a wrong window fails with expected versus observed text.
      artifacts: [crates/repark-spark/src/uuid_cast.rs]
    - id: AT-10
      status: ATTACKED
      evidence: M1 reverts to byte slicing and both Rust pins red before restore; the facade pins red on the same byte code.
      artifacts: [crates/repark-spark/src/uuid_cast.rs, python/repark/tests/test_uuid_cast_window_1.py]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-1 | Dated 2026-09-27 (measured, unpinned): a window cut that splits a surrogate pair renders `?` through the PySpark door on a UTF-8 box (probe2 `midsurr`); the transfer step that replaces the lone surrogate is unidentified, so the oracle holds no such case and the implementation renders `?` for a split pair without a pin. |
| R-2 | Dated 2026-09-27 (unmeasured, not fixed here): NTZ-1's invalid-literal window counts the skip in characters and never right-truncates; if Spark renders `INVALID_TYPED_LITERAL` through the same 32/36-unit machinery, astral-plane SQL diverges there too. A follow-up measures it; this fix stays narrow. |
| R-3 | Dated 2026-09-27 (unmeasured edge): when the AST carries a UUID cast but the text scan finds no token, the message falls back to line 1, position 1 and now right-truncates long lines through the same uniform rule; no pin covers the fallback. |
