# Charter ledger — IO-DECLARED-1 · the declared orc/xml/jdbc IO refusals and `na.replace`

**Date:** 2026-09-14 · **Branch:** `feat/io-declared-1` · **Base:** `b1d342b6` · **Model:**
zai/glm-5.3-flash · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `IO-ORC-1` and `IO-XML-1` (DECLARED per R-1; BACKLOG 2026-09-14 — reachable in
Rust, needs an ORC / XML crate, owner question Q-15B-1) and `IO-JDBC-1` (DECLARED per R-2
"until the 1.6 native connectors (Postgres, SQL Server); the JVM JDBC driver path is
unreachable") filed at §5 end of
[../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

**Why now.** The 1.5 PySpark-parity campaign: every public name of PySpark 4.1.2's reader /
writer / `na` surfaces answers Spark or carries a dated declared refusal with Spark's own
error class. `orc`, `xml`, and `jdbc` had either a silent generic refusal (the writer's
`DATA_SOURCE_NOT_FOUND` text mentioning orc) or a repark-extension implementation wearing a
Spark name (`read.jdbc` over the postgres connector, snake_case signature). This unit files
the refusals with Spark's classes, keeps Spark's own pre-refusal checks (`rowTag`, save
mode) byte-exact, and adds the one missing `na` delegation.

**Not in this unit:** the `text` reader/writer (IO-TEXT-1, build clone); any ORC/XML/JDBC
engine work; the 1.6 native connectors; `format("jdbc").load()`'s postgres-connector alias
(R-2 covers the `jdbc()` methods; the alias is unchanged and noted in
out_of_scope_observed).

## PROPOSITION LEDGER — IO-DECLARED-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every ORC reader/writer name — `DataFrameReader.orc` (Spark's full signature), `DataFrameWriter.orc(path, mode, partitionBy, compression)`, and `format("orc")` on either side — raises `PySparkNotImplementedError` `NOT_IMPLEMENTED` `{"feature": "orc"}`, str `[NOT_IMPLEMENTED] orc is not implemented.`, at the call (at `load()`/`save()` for the `format` spellings), and the writer `save()` fallback's orc arm is rewritten to this class from its old `DATA_SOURCE_NOT_FOUND` text. | `python/repark/tests/test_io_declared_1.py` orc pins; the rewritten pins. | **PROVEN** | Red first: the whole new pin module ran on the base tree — 20 failed (all orc/xml/jdbc/na ids, `AttributeError: ... no attribute 'orc'` among them), then 13 orc/xml/jdbc pins green after the module landed. Pins assert class, condition, params, and str byte-exact. The old orc pins were rewritten: `test_e2_readwriter.py::test_save_orc_declared_not_implemented` + `test_load_orc_declared_not_implemented`, `test_r1_read_formats.py::test_load_orc_declared_not_implemented` + `test_write_orc_declared_not_implemented`; `test_writer_v2.py`'s DATA_SOURCE_NOT_FOUND claim swaps its probe to `avro`, which keeps the residual answer. Registry row `IO-ORC-1`. pins: io-declared-1/C-001 |
| C-002 | Every XML reader/writer name — `DataFrameReader.xml(path, rowTag, schema, **options)`, `DataFrameWriter.xml(path, rowTag, mode, **options)`, and `format("xml")` on either side — first reproduces Spark's own `rowTag` check and refuses `AnalysisException` `XML_ROW_TAG_MISSING` (SQLSTATE 42KDF, message and `{"rowTag": "`rowTag`"}` params byte-exact, cell `xml_read_default_rowtag` / `xml_no_rowtag_write`) when no `rowTag` argument or `rowTag` option is set, then refuses `NOT_IMPLEMENTED` `{"feature": "xml"}` with a `rowTag` present. | The five xml pins in the new module, driven from the fixture cells. | **PROVEN** | Red first (same 20-red run). The rowTag check reads the argument or the option map case-insensitively (`reader._options` / `writer._options`), so `format("xml").option("rowTag","row").load(...)` refuses `NOT_IMPLEMENTED` while bare `format("xml").load(...)` answers `XML_ROW_TAG_MISSING`; `pytest.raises` cells assert `getSqlState() == "42KDF"`. A provided-but-empty `rowTag` counts as provided (the declared refusal fires) — Spark's separate empty-string answer was not recorded by the oracle and is not invented here. Registry row `IO-XML-1`. pins: io-declared-1/C-002 |
| C-003 | `DataFrameReader.jdbc` (Spark's camelCase signature `url, table, column, lowerBound, upperBound, numPartitions, predicates, properties`) and `DataFrameWriter.jdbc(url, table, mode, properties)` refuse: the writer first raises `AnalysisException` `INVALID_SAVE_MODE` (SQLSTATE 42000, the `writer_jdbc_mode_bad` cell message byte-exact, params `{"mode": "\"<mode>\""}`) for a mode outside Spark's valid set, then every call refuses `NOT_IMPLEMENTED` `{"feature": "jdbc"}`; reads keep the native alternative (`format('postgres')` / `session.read_postgres`). | The four jdbc pins; the rewritten `test_pg_jdbc_options.py` pins; the two env-gated live call sites. | **PROVEN** | Red first (same 20-red run; the old snake-case signature also lacked `lowerBound`-style kwargs). Mode check first: `mode="bogus"` answers the cell byte-exact before any refusal; `mode="append"`/absent answers `NOT_IMPLEMENTED`. The valid set is Spark's own six from the `INVALID_SAVE_MODE` message (`append`, `overwrite`, `ignore`, `error`, `errorifexists`, `default`), case-sensitive like the writer's own `mode()`. The reader's postgres-connector body is replaced by the refusal — R-2's row names `read_postgres` as today's alternative, and `test_pg_jdbc_options.py`'s three `spark.read.jdbc` pins were rewritten to the declared shape (the dbtable-forwarding contract moved onto the `format('postgres')` spelling); the two env-gated live call sites (`test_pg_acceptance.py`, `test_pg_jdbc_oracle.py`) moved to `format('postgres')`. Registry row `IO-JDBC-1`. pins: io-declared-1/C-003 |
| C-004 | `DataFrameNaFunctions.replace(to_replace, value=<no value>, subset=None)` is exactly `DataFrame.replace(to_replace, value, subset)` through the shared no-value sentinel: scalar/list/dict/subset/None cells answer Spark byte-exact from `facade_reader_writer_oracle.json` (`na_replace_basic`, `na_replace_subset`, `na_replace_list`, `na_replace_none_value`, `na_replace_dict` → `MIXED_TYPE_REPLACEMENT`, `na_replace_novalue_nondict` → `ARGUMENT_REQUIRED`), and the `na_replace_identity` cell holds (`na.replace(...)` collect equals `df.replace(...)` collect). | The seven na.replace pins in the new module, driven from the fixture cells. | **PROVEN** | Red first (same 20-red run, `AttributeError: 'DataFrameNaFunctions' object has no attribute 'replace'`). The sentinel `replace_expr._NO_VALUE` now defaults both signatures; a non-dict `to_replace` with the sentinel raises `ARGUMENT_REQUIRED` (Spark's confusing "when `to_replace` is dict" condition is Spark's own 4.1.2 message, reproduced byte-exact with the cell's params) while an explicit `None` value still null-replaces — the two shapes the old `value=None` default could not distinguish. The dict+sentinel path no longer trips the "value will be ignored" warning, and the `MIXED_TYPE_REPLACEMENT` raise now carries Spark's full message text (the cell's str shape; the class and empty params were already pinned). `core.py` stays at its 4035 exact baseline (the early import swap is line-neutral). Registry coverage inside `IO-DECLARED-1`. pins: io-declared-1/C-004 |
| C-005 | The unit's rows land: registry `IO-ORC-1` / `IO-XML-1` / `IO-JDBC-1` at §5 end with oracle cell citations and dates; the fixture copy is byte-identical and listed in `python/repark/tests/map.md`; every new public name is enumerated and covered by a `docs/examples/io/` example with `COVERS`; the example, inventory, and exception counts re-measure green. | The registry diff; the maps; the gates. | **PROVEN** | Fixture copied byte-identical (25064 bytes, `cmp` clean) from run 15b and listed in `python/repark/tests/map.md`. Example `docs/examples/io/io_declared_refusals.py` covers all seven names and runs green standalone and under the gate's execution step. `--write-inventory` added exactly 6 rows (5 io + `DataFrameNaFunctions.replace`); `DataFrameReader.jdbc` left `exceptions.txt` (an example now covers it) and `EXCEPTIONS_BASELINE` ratcheted 2 → 1 with its pin; raw walk 955 → 961 pinned. API freeze (`build_api_freeze.py`) still matches — the freeze registers the packet rows, not the post-freeze additive names. pins: io-declared-1/C-005 |
| C-006 | No regression: the touched surfaces' suites and the inventory gates stay green — `test_writer*.py`, `test_e2_readwriter.py`, `test_r1_read_formats.py`, `test_replace*.py`, `test_pg_*` offline pins, the CAP-1 size ratchet, the API freeze, and the full facade + parity suites on the final tree. | The gates table. | **PROVEN** | `test_writer.py` + `test_replace_linear_1.py` + `test_writer_v2.py` 70 passed; the five rewritten/adjacent files with the new module: 130 passed; `test_cap_1_source_file_line_cap.py` 23 passed after the ratchets (writer_readwriter 1111 → 1110 mirrored, the reader.py exception retires at 954); `build_api_freeze.py` exit 0; full facade suite and `python/repark-parity/tests` counts in §Gates. `STATUS.md` and `briefs/next-sequence.md` untouched. pins: io-declared-1/C-006 |

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

## Per-name decision table

| name | decision | one line of reason |
|---|---|---|
| `DataFrameReader.orc` | declared | A Rust ORC reader needs a new crate — a dependency decision reserved to the owner (Q-15B-1); the refusal is the parity answer (IO-ORC-1). |
| `DataFrameWriter.orc` | declared | Same crate gap as the reader side; the old `DATA_SOURCE_NOT_FOUND` orc arm became Spark's class. |
| `DataFrameReader.xml` | declared | Needs an XML crate (Q-15B-1); Spark's own `rowTag` check is reproduced exactly before the refusal. |
| `DataFrameWriter.xml` | declared | Same crate gap; `rowTag` check first on the write side (cell `xml_no_rowtag_write`). |
| `DataFrameReader.jdbc` | declared (R-2) | The JVM JDBC driver path is unreachable until the 1.6 native connectors; reads keep `format('postgres')` / `session.read_postgres`. |
| `DataFrameWriter.jdbc` | declared (R-2) | Same JDBC gap on the write path; the mode check is pure API plumbing (Python-correct), then the refusal. |
| `DataFrameNaFunctions.replace` | implemented (delegation) | Pure API plumbing: it delegates to the existing `replace` plan builder — no engine capability is missing, so Rust-first is satisfied by `replace_expr` staying the body. |

Python is correct for every row under the Rust-first standing instruction: this unit adds
refusals and one delegation, no Python compute.

## Settled decisions (recorded, not guessed)

- **`reader.jdbc` refuses whole.** R-2 lists the reader's Spark signature inside the declared
  treatment and names `read_postgres` as today's alternative, so the shipped
  postgres-connector behavior of the `jdbc` name ends with this unit. The alternative names
  (`format('postgres')` / `format('postgresql')` / `session.read_postgres`) are untouched;
  `format("jdbc")`'s alias arm is also untouched (R-2 covers the `jdbc()` methods) and is
  noted for the orchestrator.
- **Writer `jdbc` mode set.** The six modes Spark's own `INVALID_SAVE_MODE` message names,
  case-sensitive like `DataFrameWriter.mode()`; the message text comes from the
  `writer_jdbc_mode_bad` cell with the mode substituted and quoted as the cell's params do.
- **`rowTag` "provided"** means the argument is not `None` or any `rowTag`-spelled key exists
  in the option map (case-insensitive); an empty string counts as provided.
- **Pins rewritten** (all recorded above): the four orc pins in `test_e2_readwriter.py` /
  `test_r1_read_formats.py` / (probe swap in) `test_writer_v2.py`, and the three
  `spark.read.jdbc` pins in `test_pg_jdbc_options.py`; `test_pg_acceptance.py` /
  `test_pg_jdbc_oracle.py` live call sites moved to the alternative.

## Gates

| gate | result |
|---|---|
| Red first — `.venv/bin/python -m pytest python/repark/tests/test_io_declared_1.py -q` on the base tree | **20 failed** (`test_reader_orc_refuses_at_the_call`, `test_reader_format_orc_refuses_at_load`, `test_reader_xml_without_row_tag_refuses`, `test_reader_xml_with_row_tag_refuses`, `test_reader_format_xml_row_tag_from_options`, `test_writer_orc_refuses_at_the_call`, `test_writer_format_orc_refuses_at_save`, `test_writer_xml_without_row_tag_refuses`, `test_writer_xml_with_row_tag_refuses`, `test_writer_jdbc_bad_mode_is_invalid_save_mode`, `test_writer_jdbc_refuses_after_the_mode_check`, `test_reader_jdbc_refuses_at_the_call`, `test_reader_jdbc_props_refuses_at_the_call`, `test_na_replace_basic`, `test_na_replace_subset`, `test_na_replace_list`, `test_na_replace_none_value`, `test_na_replace_dict_mixed_types_raise`, `test_na_replace_novalue_nondict_raise`, `test_na_replace_identity`) — reasons: missing `orc`/`xml` writer methods, missing reader `orc`/`xml`, old `jdbc` signature and postgres-connector behavior, missing `na.replace`, `AttributeError`/wrong error class. |
| New module + touched pins | 20 passed (`test_io_declared_1.py`); 130 passed across `test_io_declared_1.py`, `test_pg_jdbc_options.py`, `test_e2_readwriter.py`, `test_r1_read_formats.py`, `test_writer_v2.py`. |
| Size gates | `python3 scripts/check_lib_py.py` — 700 files clean, 31 exceptions (writer_readwriter ratcheted 1111 → 1110, reader.py exception retired at 954); CAP-1 test 23 passed. |
| Example coverage | `check_example_coverage.py` exit 0 with execution (954 names, 841 covered, 112 backlog, 1 exceptions, 223 examples); `test_ex_0_example_coverage.py` 26 passed. |
| API freeze | `build_api_freeze.py` exit 0 (frozen surface matches the tree). |
| Full suites, ruff, typos, ledger grammar, fence | §Gates of the handback; run on the final tree. |
