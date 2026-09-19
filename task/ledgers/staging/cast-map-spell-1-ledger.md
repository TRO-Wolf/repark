# Unit ledger — CAST-MAP-SPELL-1 · `CAST(… AS MAP<…>)` and `.cast(MapType)` answer Spark 4.1.2

**Date:** 2026-09-19 · **Branch:** `fix/cast-map-spell-1` · **Base:** `e6ec5531` (`main`)
**Model:** muse-spark-1.3-contributor (steps 1–2), claude-opus-5 (steps 3–5) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
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
| C-001 | The 21-cell oracle is recorded from live PySpark 4.1.2 (one short JVM, ANSI on except the legacy cell) and committed verbatim with provenance, SHA-256 and a re-deriver that exits non-zero on drift. | Fixture file plus map.md plus `_record_cast_map_spell_1.py --check`. | PROVEN | SHA-256 `9ac3edde…` equals the orchestrator's recording; `--check` on live Spark 4.1.2 prints `cast_map_spell_1 oracle matches the committed fixture` (rc 0). |
| C-002 | The facade SQL door answers every SQL cell: schema string, per-field nullability and rows equal the oracle, and the two refusal cells raise with Spark's error-class token. | `test_cast_map_spell_1.py` facade pins, green after step 3. | PROVEN | 15 value cells, the legacy twin and 2 refusals pass; rows compare as a multiset (the `UNION ALL` cell promises no order, measured NULL row first). |
| C-003 | The native ANSI door answers every SQL cell it can spell, with the same value, type and refusal pins. | `test_cast_map_spell_1.py` native pins, green after step 3. | PROVEN | 11 value cells and 2 refusals pass on `repark.sql`; the rewrite runs in `session_runtime::prepare_session_sql` (that door is core `DataFusionDialect`) and in the `repark-sql` router. |
| C-004 | The DataFrame door answers the three `.cast` cells (MapType object, DDL string, nested DDL string). | `test_cast_map_spell_1.py` DataFrame pins, green after step 3. | PROVEN | `Column.cast` forwards unknown names to `PyColumnParts.cast_type_token`; `expr_build::cast_to` builds the UDF expr; column.py 1532 → 1529. |
| C-005 | `MAP<K, V>` parses as a CAST/TRY_CAST target on both SQL doors at any nesting, any case, with or without spaces. | Rust unit tests plus the step-2 pins. | PROVEN | `cast_map::tests::map_target_parses_any_case_spacing_and_nesting`, `rewrite_*` (9 passed); the lowercase, nested, array-of-map and struct-with-map cells on both doors. |
| C-006 | Element casts run key-wise and value-wise with Spark semantics: ANSI raises `CAST_INVALID_INPUT`, legacy yields per-element NULL, `try_cast` yields per-element NULL. | Oracle invalid/legacy/try_cast cells on both doors. | PROVEN | `invalid_value_raises_under_ansi_and_nulls_otherwise`; the three oracle cells pass on both doors. Residue named in the registry row: non-string leaf failures keep Arrow's message. |
| C-007 | A non-map source refuses with `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`. | Oracle `int_to_map_refuses` cell on both doors. | PROVEN | `non_map_source_refuses_at_planning`; the cell passes on both doors (plan-time refusal from `return_field_from_args`). |
| C-008 | The old refusal pins flip: the `test_nullability_2.py` map-spelling pin, the 8 strict xfails in `test_ice_array_insert_1.py` (verbatim statements become plain pins), and any FNP8-PARSING typed-map-cast pins. | Flipped pins green in step 3. | PROVEN | `test_cast_to_map_type_spelling_answers_per_cast_map_spell_1`; 30 plain `test_cell_matches_spark` ids (substitute twins dropped, ruling Q-23b-CM-3); 24 FNP8 dispositions re-derived, each equal to Spark's rows and Arrow schema; ICE-LIST-NULL-1's `map_int` seed runs verbatim (225 passed, 32 xfailed as before). |
| C-009 | `CAST-MAP-SPELL-1` reads FIXED 2026-09-19 with before/after and pins; `FNP8-PARSING`'s typed-map-cast sentence is true or names exactly what remains; ICE-ARRAY-INSERT-1's map_list sentence is true. | Registry diff plus this ledger. | PROVEN | `docs/spark-sql-iceberg-parity.md`: the row reads FIXED with before/after and residues; FNP8-PARSING keeps only the struct-field lambda and bare `map()`; ICE-ARRAY-INSERT-1 and ICE-LIST-NULL-1 read verbatim. |
| C-010 | The targeted gates are green: the new test file plus `test_nullability_2.py` plus `test_ice_array_insert_1.py` offline and live, every MAP-cast test file, per-crate Rust tests, `cargo fmt --check`, per-crate clippy, whole-tree `ruff check`, `ruff format --check` on changed Python. | Pasted summary lines in step 5. | PROVEN | See §Gates. |
| C-011 | Map key legality follows Spark 4.1.2: under ANSI the key pair needs ANSI castability; in legacy mode and under `try_cast` (either mode) it needs legacy castability and a key cast that cannot produce NULL (`forceNullable` false); an illegal pair refuses with `[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION]`. | Round-3 cells `dup_keys_legacy`, `try_null_key_*`, `try_widen_key_*` on both doors; the Spark matrix measured 2026-09-19 (8 source types x 9 key targets x 3 modes). | OPEN | Red on the round-2 tree. |
| C-012 | Colliding keys after a legal key cast are stored as Spark stores them — no dedup, entry order kept (`map_keys` `[2, 1, 2]`) — so a `collect()` shows the later value, as Spark's does. | Round-3 cells `dup_keys_ansi`, `dup_keys_order_ansi`. | OPEN | The orchestrator's `{1: 'b'}` reading is Spark's Python dict view; `map_keys` is the storage. |
| C-013 | Leaf element casts follow Spark: integral and fractional narrowing raise `[CAST_OVERFLOW]` under ANSI and wrap in legacy mode; string leaves trim whitespace, and legacy string-to-integral accepts a fractional part. | Round-3 cells `overflow_leaf_*`, `bigint_overflow_leaf_*`, `whitespace_leaf_*`, `fraction_text_leaf_*`. | OPEN | Red on the round-2 tree. |
| C-014 | A `/*! … */` comment hint is an ordinary comment to the map-cast rewrite. | Round-3 cells `comment_hint_*`; Rust `comment_hint_is_an_ordinary_comment`. | OPEN | Red on the round-2 tree. |

## Gates

Measured on the release native built from the step-3 tree:

- Offline `-n 4`: `test_cast_map_spell_1.py` + `test_nullability_2.py` + `test_ice_array_insert_1.py`
  82 passed, 13 skipped; `test_fnp8_oracle_matrix.py` 358 passed, 93 skipped;
  `test_ice_list_null_1.py` 225 passed, 4 skipped, 32 xfailed.
- Live (`REPARK_PARITY_LIVE=1`, PySpark 4.1.2): the three unit files 95 passed.
- `cargo test -p repark-functions --lib cast_map` 9 passed; `cargo test -p repark-sql --lib` 356 passed.
- `cargo fmt --all --check` clean; `cargo clippy -p repark-functions -p repark-spark -p repark-sql
  -p repark-python --all-targets -- -D warnings -A clippy::disallowed_methods` clean.
- `ruff check .` clean; `ruff format --check` on the changed Python clean.
- Comment ban: 0 hits.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: cast-map-spell-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every expectation reads from the committed Spark 4.1.2 oracle, and
        the live drift check re-derives it (rc 0). The 24 FNP8 dispositions were
        re-measured and each equals Spark's recorded rows and Arrow schema, so no
        RePark-only answer was written into a pin.
      artifacts: [python/repark/tests/cast_map_spell_1/cast_map_spell_1_spark_oracle.json, python/repark/tests/fnp8_repark_dispositions.json]
    - id: AT-2
      status: ATTACKED
      evidence: Red on the base tree (34 failed, recorded in step 2); the flipped
        pins asserted the refusal before. After the fix, the native door stayed red
        until the binding session door carried the rewrite, which shows the pins
        discriminate by door.
      artifacts: [python/repark/tests/test_cast_map_spell_1.py, python/repark/tests/test_nullability_2.py]
    - id: AT-3
      status: ATTACKED
      evidence: All 21 cells are pinned on every door that can spell them. The Rust
        suite covers the rewrite's no-op cases (string literal, comment, non-map
        cast), nested and try_cast splices, the empty-map operand and multibyte text
        after the splice.
      artifacts: [crates/repark-functions/src/cast_map/tests.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The UDFs are stateless LazyLock singletons; ANSI is read per
        invocation from the session config, never cached.
      artifacts: [crates/repark-functions/src/cast_map.rs]
    - id: AT-5
      status: ATTACKED
      evidence: The rewrite never echoes user text into the emitted literal. The
        target is re-rendered from the parsed Arrow type with single quotes
        doubled. The operand text is spliced unchanged, and unparsable targets
        leave the statement untouched for the parser to refuse. The DataFrame token
        is rendered from the parsed type. Type and cast recursion are bounded
        (depth 32 and 64); the kernel has no unwrap or indexing panic.
      artifacts: [crates/repark-functions/src/cast_map/rewrite.rs, crates/repark-functions/src/cast_map.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Nullability follows Spark (source or try_cast; empty map non-null),
        and the planned field is pinned in Rust and in the facade cells. Residues are
        named in the registry row rather than hidden (non-string leaf messages,
        whitespace trimming, operand display).
      artifacts: [docs/spark-sql-iceberg-parity.md, crates/repark-functions/src/cast_map/tests.rs]
    - id: AT-7
      status: N/A
      justification: No wall-clock or performance claim; the rewrite tokenizes only statements containing both `map` and `cast`.
    - id: AT-8
      status: ATTACKED
      evidence: No STATUS.md, Cargo.toml or Cargo.lock edit, and no crate-DAG change.
        The native door reuses repark-functions because repark-sql may not depend on
        repark-spark. Both size ceilings moved down (column.py 1532 -> 1529,
        session.rs 1127 -> 1126).
      artifacts: [scripts/check_lib_py.py, scripts/check_rust_file_size.py]
    - id: AT-9
      status: ATTACKED
      evidence: The registry rows CAST-MAP-SPELL-1, FNP8-PARSING, ICE-ARRAY-INSERT-1
        and ICE-LIST-NULL-1 read true. Every touched directory's map.md carries the
        change with pins, and check_docs_links, sync_map_md and check_ledger_grammar
        are clean.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/map.md, crates/repark-functions/src/cast_map/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: Every other file that casts to MAP ran green. FNP8 re-derived; the
        ICE-LIST-NULL-1 seed runs verbatim with 225 passed and the same 32 strict
        xfails. The repark-sql lib suite passes (356).
      artifacts: [python/repark/tests/test_fnp8_oracle_matrix.py, python/repark/tests/test_ice_list_null_1.py]
  complete: true
```
