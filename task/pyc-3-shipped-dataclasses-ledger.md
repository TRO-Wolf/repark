# PYC-3 — the two shipped `dataclass` containers

**Unit:** PYC-3
**Branch:** `feat/pyc-3-shipped-dataclasses`
**Base:** `origin/main` @ `8ce8d13` (#207 PYC-2)
**Date:** 2026-08-22
**Path:** HIGH (user `/sepmo-octo`; new wheel dependency; MERGE builder is a public API;
  smartCsv diagnostics are a public dict surface)
**Critic engine:** `octo` (cycles=4, early_stop=true, claims_critic=true, floor S1)
**Approval:** user "that is SQM, proceed with the next group of work and continue our /sepmo-octo"

The ordered queue is [briefs/next-sequence.md](../briefs/next-sequence.md). PYC-3 converts
`spark/merge.py` and `spark/_csv_smart.py` from `dataclasses` to Pydantic v2 `BaseModel`.
Campaign invariant: **no call that worked before returns a different value.**

---

## PROPOSITION LEDGER — PYC-3 — 2026-08-22

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|--------|--------------------------|-------------------|---------|----------|
| C-001 | `_Clause` in `spark/merge.py` is a Pydantic v2 `BaseModel`, not a `dataclass` | File has no `dataclasses` import; class subclasses `BaseModel` | PROVEN | `merge.py` `_Clause`; `test_pyc_3_merge_clause_is_basemodel` |
| C-002 | `ColumnIngestReport`, `IngestReport`, `ColumnResolution`, `PreparedCsv` in `spark/_csv_smart.py` are Pydantic v2 `BaseModel`s, not dataclasses | File has no `dataclasses` import; four classes subclass `BaseModel` | PROVEN | `_csv_smart.py`; `test_pyc_3_csv_containers_are_basemodels` |
| C-003 | The accepted-input set of those containers is pinned: every constructor shape production uses still constructs | Enumerate production construction sites; a test constructs each shape | PROVEN | Domain table below; `test_pyc_3_clause_accepted_shapes`; `test_pyc_3_csv_accepted_construction` |
| C-004 | `MergeIntoWriter` public builder inputs are not narrowed | Existing `test_merge_into.py` type-error and happy-path pins stay green | PROVEN | `test_merge_into.py` (type errors, upsert, partial update/insert, no-clause) |
| C-005 | `describe_ingest()` dict identity is unchanged (`to_dict` key set, optional decimal keys omitted when None) | Pin `to_dict` on a representative report; existing smartCsv describe pins stay green | PROVEN | `test_pyc_3_ingest_report_to_dict_identity`; `test_t4_csv_smart.py` |
| C-006 | `DATACLASS_EXCEPTIONS` rows for the two files are deleted, not zeroed | AST of `check_python_conventions.py` has neither key | PROVEN | `test_pyc_3_exception_rows_deleted_not_zeroed` |
| C-007 | This unit does not convert `scripts/check_parity_live_dual_wire.py` or any `python/repark-parity` dataclass file | Those keys remain in `DATACLASS_EXCEPTIONS` | PROVEN | remaining 21 rows; PYC-4 owns them |
| C-008 | Extra kwargs on the new models are refused (`extra="forbid"`), not pydantic's default ignore | Construction with an unknown field raises `ValidationError` (internal constructors; public MERGE/smartCsv still raise PySpark type errors) | PROVEN | `test_pyc_3_extra_fields_refused` |
| C-009 | `IngestReport` stays mutable after init — `prepare_messy_csv` / `load_smart_csv` assign fields post-construction | Frozen config is not set on `IngestReport`; mutation pin | PROVEN | `test_pyc_3_ingest_report_post_init_mutation` |
| C-010 | pydantic v2 is a declared hard dependency of the `repark` wheel (`>=2.10,<3`) | `python/repark/pyproject.toml` `dependencies`; `uv.lock` records it | PROVEN | pyproject + lock; map.md truth-up |
| C-011 | Nested-def EXCEPTIONS table is unchanged by this unit | 9 nested-def rows remain | PROVEN | `make check-python-conventions` (9 nested-def, 21 dataclass) |
| C-012 | `map.md` / STATUS / sequence / ledger land in the same change | `check_map_md` green; STATUS names PYC-3 as this change | PROVEN | this change (same tree as the conversion) |
| C-013 | Remaining dataclass debt after this unit is the 20 parity files plus `scripts/check_parity_live_dual_wire.py` (21 rows) | Gate summary line | PROVEN | conventions gate after conversion |

VERDICT: PASS (OPEN=0, REJECTED=0). LOGIC_SCORE = 13/13.

KILLED_ASSUMPTIONS:
- "Pydantic is already a wheel dependency" — REMOVED (`uv.lock` had zero `pydantic` hits; pyproject listed only `pyarrow>=25.0.0`). PYC-3 adds it (C-010).
- "`frozen=True` on every model" — REMOVED. `IngestReport` is assigned after init in production (C-009). `_Clause` and the other three CSV models are frozen.
- "The public MERGE API constructs `_Clause` from user kwargs" — REMOVED. Users call `MergeIntoWriter` methods; `_Clause` is private. The validation hazard is still real if a conversion rejected a shape those methods already produce (C-003/C-004).

RISK_HEATMAP:
- pydantic coercion would silently change values dataclass stored as-is. Mitigation: `strict=True` + `extra="forbid"`.
- Second hard wheel dep contradicts the phase-3 freeze "exactly one (`pyarrow>=25`)". Mitigation: dated truth-up on live maps; the PYC owner charter requires BaseModel.

---

## Domain (accepted-input set)

### `merge._Clause` — 8 unique `(kind, action)` shapes from 8 builder sites

| kind | action | extra fields production sets |
|---|---|---|
| `matched` | `update_all` | `predicate_sql` optional |
| `matched` | `update` | `predicate_sql` optional, `assignments` non-empty `dict[str, str]` |
| `matched` | `delete` | `predicate_sql` optional |
| `not_matched` | `insert_all` | `predicate_sql` optional |
| `not_matched` | `insert` | `predicate_sql` optional, `assignments` non-empty |
| `not_matched_by_source` | `update_all` | `predicate_sql` optional |
| `not_matched_by_source` | `update` | `predicate_sql` optional, `assignments` non-empty |
| `not_matched_by_source` | `delete` | `predicate_sql` optional |

Defaults: `predicate_sql=None`, `assignments={}`. Default factory is not shared across instances.

### `_csv_smart.py`

| Class | Production construction | Post-init mutation? |
|---|---|---|
| `ColumnIngestReport` | `infer_schema_from_rows` | no |
| `IngestReport` | `prepare_messy_csv` kwargs; `load_smart_csv` then assigns sampling + `columns` | **yes** |
| `ColumnResolution` | `resolve_column_type` (two return sites) | no |
| `PreparedCsv` | `prepare_messy_csv` (empty and non-empty) | no |

`to_dict` is the public `describe_ingest` payload: decimal keys appear only when not None.

---

## Octo (SEPMO-octo)

`critic_engine: octo` · cycles=4 · early_stop=true · claims_critic=true · floor S1.

| Cycle | Half A OPEN ≥floor | Half B | Notes |
|---|---|---|---|
| 1 | C1-Q-001, C1-Q-003, C1-CL-001..004 (C1-Q-002 withdrawn) | remediations | Critic-2/3 CLEAN |
| 2 | C2-Q-001, C2-Q-002 | remediations | Critic-2/3/4 CLEAN |
| 3 | C3-Q-001 | remediations + S2/S3 dual-home | Critic-2/3/4 CLEAN |
| 4 | C4-CL-001 (tests/map.md withSchemaEvolution) | remediations | Critic-1/2/3 CLEAN |

Scratch: `/tmp/critic-octo-repark-pyc-3-2026-08-22/`

## Pins

- `python/repark/tests/test_pyc_3_dataclasses.py` — accepted shapes, extra-field refuse, mutation, `to_dict` identity, EXCEPTIONS keys gone, both files import-free of `dataclasses`.
- Behaviour stays on existing `test_merge_into.py` and `test_t4_csv_smart.py`.

## Bait

`from dataclasses import dataclass` restored at the top of `merge.py` with the EXCEPTIONS row gone → `check_python_conventions.py` exit 1 (`imports dataclasses`). Removed; gate green.

## Ceilings

Neither file is on a `check_lib_py` EXCEPTIONS row (default 2500). No ceiling move.

## Gate

`make verify` exit 0.
`make preflight` exit 0 immediately before the PR: **3678 passed, 70 skipped,
0 failed** facade. python-conventions: 162 files, 9 nested-def rows, 21 dataclass
rows. lib-py clean. audit + workflows-lint clean. pip-audit: no known
vulnerabilities (including the new pydantic pin).

## Wheel dependency

Phase-3 freeze said the wheel's only hard runtime dep is `pyarrow>=25`. PYC-3 adds `pydantic>=2.10,<3` because `BaseModel` is a class body, not a lazy import. Live maps (`python/repark/map.md`, `docs/port/census.md`) are truthed up. The historical freeze sentence in `docs/design/python-facade.md` gets a dated PYC-3 footnote rather than an in-place rewrite of the original decision.
