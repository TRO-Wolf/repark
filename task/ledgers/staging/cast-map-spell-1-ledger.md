# Unit ledger — CAST-MAP-SPELL-1 · `CAST(… AS MAP<…>)` and `.cast(MapType)` answer Spark 4.1.2

**Date:** 2026-09-19 · **Branch:** `fix/cast-map-spell-1` · **Base:** `e6ec5531` (`main`)
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Owner direction 2026-09-18 20:58: 1:1 parity with Spark's Iceberg
integration — a refusal is no longer an acceptable end state for a cell Spark answers.
Registry row `CAST-MAP-SPELL-1` (BACKLOG) and the typed-map-cast half of `FNP8-PARSING`
are this unit. Stock sqlparser (Databricks/Generic dialects) has no `MAP<…>` angle-bracket
type and DataFusion 54 plans `SQLDataType::Map` as unsupported, so serving the cast is a
cast-UDF plus token-rewrite feature on both SQL doors plus complex-type support in the
DataFrame `.cast` path.

**Step 2.** Fixture plus red pins only, no product code: the 21-cell oracle verbatim
under `python/repark/tests/cast_map_spell_1/`, the repo-idiomatic re-deriver
`python/repark/tests/_record_cast_map_spell_1.py`, and
`python/repark/tests/test_cast_map_spell_1.py` with one pin per cell per door. Red on
the base tree for the recorded reasons (`ParserError("Expected: ), found: <")` on the
facade door, `unknown cast type 'map<string,long>'` on the DataFrame door).

**Not in this unit:** `STATUS.md`, `Cargo.toml`, `Cargo.lock`, `pyproject.toml`, `uv.lock`,
`.github/`, AWS credentials or envs.

## PROPOSITION LEDGER — CAST-MAP-SPELL-1 — 2026-09-19

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The 21-cell oracle is recorded from live PySpark 4.1.2 (one short JVM, ANSI on except the legacy cell) and committed verbatim with provenance, SHA-256 and a re-deriver that exits non-zero on drift. | Fixture file plus map.md plus `_record_cast_map_spell_1.py --check`. | OPEN | Step 2 lands the files; drift check runs in step 5. |
| C-002 | The facade SQL door answers every SQL cell: schema string, per-field nullability and rows equal the oracle, and the two refusal cells raise with Spark's error-class token. | `test_cast_map_spell_1.py` facade pins, green after step 3. | OPEN | Red on the base tree per the step-2 commit. |
| C-003 | The native ANSI door answers every SQL cell it can spell, with the same value, type and refusal pins. | `test_cast_map_spell_1.py` native pins, green after step 3. | OPEN | Native-spellable set measured in step 2. |
| C-004 | The DataFrame door answers the three `.cast` cells (MapType object, DDL string, nested DDL string). | `test_cast_map_spell_1.py` DataFrame pins, green after step 3. | OPEN | Red on the base tree per the step-2 commit. |
| C-005 | `MAP<K, V>` parses as a CAST/TRY_CAST target on both SQL doors at any nesting, any case, with or without spaces. | Rust unit tests under the crate's `src/tests/` plus the step-2 pins. | OPEN | Fix lands in step 3. |
| C-006 | Element casts run key-wise and value-wise with Spark semantics: ANSI raises `CAST_INVALID_INPUT`, legacy yields per-element NULL, `try_cast` yields per-element NULL. | Oracle invalid/legacy/try_cast cells on both doors. | OPEN | Fix lands in step 3. |
| C-007 | A non-map source refuses with `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`. | Oracle `int_to_map_refuses` cell on both doors. | OPEN | Fix lands in step 3. |
| C-008 | The old refusal pins flip: the `test_nullability_2.py` map-spelling refusal pin, the 8 strict xfails in `test_ice_array_insert_1.py` (verbatim statements become plain pins; substitute twins stay as the alternate-source coverage), and any FNP8-PARSING typed-map-cast pins. | Flipped pins green in step 3. | OPEN | Substitutes stay: they pin the alternate source, not the cast. |
| C-009 | `CAST-MAP-SPELL-1` reads FIXED 2026-09-19 with before/after and pins; `FNP8-PARSING`'s typed-map-cast sentence is true or names exactly what remains; ICE-ARRAY-INSERT-1's map_list sentence is true. | Registry diff plus this ledger. | OPEN | Lands in step 4. |
| C-010 | The targeted gates are green: the new test file plus `test_nullability_2.py` plus `test_ice_array_insert_1.py` offline and live, every MAP-cast test file, per-crate Rust tests, `cargo fmt --check`, per-crate clippy, whole-tree `ruff check`, `ruff format --check` on changed Python. | Pasted summary lines in step 5. | OPEN | Runs in step 5. |
