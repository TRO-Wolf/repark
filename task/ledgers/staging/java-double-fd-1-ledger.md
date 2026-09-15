# Unit ledger — JAVA-DOUBLE-FD-1 round 2 (run 16c) · reviewer cells Q19

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands
(the orchestrator's departure move). This file closes when JAVA-DOUBLE-FD-1 round 2
merges, or when the owner closes the slate row.

**Unit:** JAVA-DOUBLE-FD-1 round 2 · **Date:** 2026-09-15 · **Model:**
muse-spark-1.3-contributor (muse-worker 16c) · **Branch:** `feat/java-double-fd-1`
**Spec:** `/tmp/oc-worker/qc-oracle/fixtures-batch19-critic-633.json`, cells `Q19-*`
(live PySpark 4.1.2 on JDK 17).

## Scope

Answer the round-2 reviewer findings in slice order: L-004/L-001, then L-002, then
L-003, then P2-1/P2-4 (P2-2 only with time left, never P2-3). Every finding lands
with oracle pins on both doors where the facade reaches, registry rows, and a
before/after measure for the perf slices. P2-2 ran out of time and stays residue.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| L-004 | `%F` is not a Java conversion; the shim refuses it at invoke with `Conversion = 'F'` (Q19-fmt-4/5). | `test_q19_fmt4_upper_f_refused`, `test_q19_fmt5_upper_f_width_refused` read `Q19-fmt-4`/`Q19-fmt-5` verbatim on the SQL door plus facade legs. | **PROVEN** |
| L-001 | Sign, space and paren flags never prefix NaN; infinity keeps sign handling (Q19-fmt-0..3, controls 6/7). | `test_q19_fmt0_nan_plain` through `test_q19_fmt7_inf_paren_space` read `Q19-fmt-0`..`Q19-fmt-7` verbatim on the SQL door plus facade legs. | **PROVEN** |
| L-002 | `#` forces the decimal point at precision 0 (Q19-fmt-8..11, controls 12/13). | `test_q19_fmt8_alt_plain` through `test_q19_fmt13_noalt_zero` read `Q19-fmt-8`..`Q19-fmt-13` verbatim on the SQL door plus facade legs. | **PROVEN** |
| L-003 | Java-suffixed casts apply to every STRING value, not just literals (eight Q19 column cells). | `test_spark_door_cast_suffix_sql_ok` plus `test_q19_suffix_col_ok` read the eight Q19 column cells on the SQL door and over real `Column.cast` frames, pinning value AND Arrow type AND nullability on both ANSI settings; registry `docs/spark-sql-iceberg-parity.md` carries the Q19 rows. | **PROVEN** |
| P2-1 | The `%f` shim renders through reused batch buffers instead of one `Vec<Option<String>>` per batch. | `format_float_value_into` with two batch scratches plus `StringBuilder` appends; 1M-row `%.2f` release-native best-of-3 0.023s before, 0.016s after; all Q19 pins still green. | **PROVEN** |
| P2-4 | The row hot paths reuse the caller buffer instead of one `String` per value. | `to_json` float rows and reader `NonFinite` write through `with_java_double_text` into the existing buffer; 1M-row `to_json` best-of-3 0.036s before, 0.034s after; `test_fnp_9_collections_json.py` green. | **PROVEN** |
| P2-2 | Heapless `%f` digit path. | No time left in the box; the quadratic left-pad loop is already gone with the P2-1 rewrite. Follow-up: one scratch `Vec<u8>` through `half_up_fixed_into`. | **OPEN** |
