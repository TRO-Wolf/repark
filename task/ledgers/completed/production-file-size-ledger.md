# Production file-size refactor ledger

This ledger closes when the production file-size refactor merges. It stays in `staging/` through
the sequential Critic pass.

## Frozen charter

Starting census at `323ce9ab1ef1600383bf58ab1b9a1252f532dfd7`: `_funcs.py` has 8,390 physical
lines, 115 top-level functions, 50 top-level assigned names, and six direct importing modules in
the tree. The chosen seam keeps `_funcs.py` as the compatibility router and extracts configuration,
catalog resolution, session state, reader support, createDataFrame conversion stages, SQL-UDF
stages, and SQL relation parsing into responsibility-named sibling modules.

| Clause | Proposition | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | The starting symbol and consumer census and the chosen ownership seam are reproducible. | AST census, tree import scan, and dependency graph in this ledger. | PROVEN |
| C-002 | Every new or changed Python source file has at most 1,000 physical lines. | `test_split_files_stay_within_default_source_ceiling`; `check_lib_py.py` exit 0. | PROVEN |
| C-003 | Every name importable from `_funcs` at the parent remains importable from `_funcs`. | `test_all_parent_symbols_remain_on_compatibility_namespace`. | PROVEN |
| C-004 | Moved top-level definitions retain behavior and source-body AST identity. | `test_moved_symbol_bodies_match_the_frozen_parent`; 165 hashes. | PROVEN |
| C-005 | The extracted import graph is cycle-safe and all internal edges are deliberate. | `test_session_package_imports_without_a_cycle`; router binding table. | PROVEN |
| C-006 | `_funcs.py` leaves `check_lib_py.py` `EXCEPTIONS` and the CAP-1 regression baseline. | `test_cap_1_funcs_source_size_exception_is_retired`; guard exit 0. | PROVEN |
| C-007 | Session and test maps describe each new ownership boundary without duplicating implementation detail. | Session, tests, scripts, and staging maps updated. | PROVEN |
| C-008 | Focused session, createDataFrame, SQL-UDF, SQL parser, and import tests pass. | Focused seven-file run: 324 passed, exit 0. | PROVEN |
| C-009 | `make py-test`, Python source/convention/docstring/Ruff gates, `make ci`, `make verify`, and `make preflight` pass. | All named gates exit 0; facade 3,728 passed / 71 skipped. | PROVEN |
| C-010 | The sequential Critic files full coverage attestation and closes all blocking findings. | Critic attestation and findings ledger. | PROVEN |

## Plan

- [x] Record the parent symbol inventory, consumers, owner assignments, and cross-module edges.
- [x] Extract responsibility modules and reduce `_funcs.py` to a compatibility router.
- [x] Add namespace, move-identity, line-ceiling, exception-retirement, and import-cycle pins.
- [x] Update session, tests, scripts, and staging-ledger maps in lockstep.
- [x] Run focused tests and Python mechanical gates; repair only refactor defects.
- [x] Re-check disk, stage the intended tree, and run the three broad gates.
- [x] Hand the staged Actor tree to the sequential Critic with C-010 still OPEN.

## Actor evidence

Actor runtime: GPT-5.6 Sol, medium effort. Disk before work: 625 GiB free on the shared 1.8 TiB
filesystem. No build artifacts existed specifically for this unit at pickup.

### Dependency and move evidence

The parent has 165 moved bindings: 115 functions and 50 assigned names. Six modules import
`_funcs` directly. Tree consumers spell two private attributes directly; the package initializer
re-exports the complete namespace through `dir(_funcs)`.

The router imports 16 owner modules and records 74 cross-owner symbol bindings. The
createDataFrame cycle is values ↔ schema/inference plus rows/Arrow/tuples. The SQL-UDF cycle is
rewrite ↔ discovery/materialization plus parser/residual/relation parsing. The router binds those
edges only after every owner imports. A fresh-process import test holds that order. The package
initializer wires the pre-existing facade class globals into every owner. Mutable active-session
and warning state routes through `session_state` for the legacy router and package attributes.

Final physical lines: `_funcs.py` 489; configuration 270; catalog 300; session state 198; reader
support 330; createDataFrame values/schema/rows/inference/Arrow/tuples 594/710/885/747/406/633;
SQL-UDF parsing/rewrite/discovery/residual/materialization 494/869/268/598/314; SQL relations 780;
package initializer 101; compatibility regression 940. Every changed Python source is at most
1,000 lines.

### Gate evidence

- Focused session/createDataFrame/SQL parser/SQL-UDF/import cohort: 324 passed, exit 0.
- Compatibility and move-identity suite: 10 passed, exit 0.
- `make py-test`: 420 passed, exit 0.
- Ruff lint/format, source-size, conventions, docstrings, maps, ledgers, ledger grammar, and owner
  ruling: exit 0.
- `make ci`: exit 0.
- `make verify`: exit 0.
- `make preflight`: exit 0; facade 3,730 passed / 71 skipped; Rust and Python audits and workflow
  lint passed.

No task-owned artifact needs cleanup. The shared incremental `target/` and `.venv` remain because
the broad gates use them and this unit did not create isolated copies.

## Critic findings and disposition

| Finding | Severity | Disposition |
|---|---|---|
| CCC-001 — The ledger understated the parent census as 135 moved bindings. | S1 | REMEDIATED — the independent census and ledger now record 115 functions plus 50 assigned names. |
| CCC-002 — The compatibility suite did not attack misowned constants or redirected cross-owner bindings. | S1 | REMEDIATED — AST-derived ownership and bytecode-derived dependency pins fail under both mutations. |
| CCC-003 — The compatibility router leaked the new public star-import name `sys`. | S1 | REMEDIATED — the router and package use the private `_sys` alias; the public star surface equals the parent. |
| CCC-004 — Configuration owned unrelated reader and SQL-relation constants. | S1 | REMEDIATED — definitions moved without AST changes into `reader_support` and `sql_relations`; owner pins and maps follow. |
| CCC-005 — The lazy array-typecode cache became stale on the router and package after owner initialization. | S2 | REMEDIATED — the compatibility proxy routes reads and writes to the cache owner; the full-suite order failure is pinned. |

The sequential Critic found no remaining blocking defect. Direct attribute state reads and writes,
two-thread write churn, owner and router reloads, fresh-process imports, and reset hooks stayed
coherent. `vars()` and `module.__dict__` retain the imported snapshot for proxied private state;
the compatibility contract covers direct imports and attribute access, and no tree consumer uses
the reflective snapshot. Parent and current isolated processes produced identical values and error
text for 21 helper cases and representative createDataFrame, SQL-UDF, JSON-reader, configuration,
and relation-parser paths.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: production-file-size
  cycle: final
  risk_tier: high
  critic_engine: ccc
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every parent binding, direct importer, package export, owner, and cross-owner global was independently derived and checked.
      artifacts: [test_production_file_size.py, _funcs.py]
    - id: AT-2
      status: ATTACKED
      evidence: Missing, misowned, mutated, and redirected bindings fail in fresh subprocess mutation proofs.
      artifacts: [test_symbols_keep_their_responsibility_owner_and_router_identity, test_cross_owner_globals_resolve_to_their_canonical_binding]
    - id: AT-3
      status: ATTACKED
      evidence: Focused semantic tests and isolated parent-current cases cover configuration, readers, createDataFrame, SQL-UDF, and relation parsing.
      artifacts: [test_session.py, test_e2_readwriter.py, test_udf.py, test_f1_sql_expander.py]
    - id: AT-4
      status: ATTACKED
      evidence: Mutable state, lazy cache, concurrent writes, reset, reload, dir, and direct package and router access were attacked.
      artifacts: [session_state.py, test_lazy_mutable_cache_stays_coherent_through_compatibility_modules]
    - id: AT-5
      status: ATTACKED
      evidence: Environment, paths, regex globals, logger identity, exception text, lazy imports, facade wiring, and Python row-compute boundaries were inspected.
      artifacts: [session package staged diff, focused 323-test cohort]
    - id: AT-6
      status: ATTACKED
      evidence: Parent public imports, callable identity, module ownership, source AST, CAP retirement, maps, and owner ruling were remeasured.
      artifacts: [test_production_file_size.py, test_cap_1_source_file_line_cap.py, check_lib_py.py]
    - id: AT-7
      status: N/A
      justification: The change moves existing Python definitions and adds constant-time module attribute routing; it adds no new row computation or unbounded work.
    - id: AT-8
      status: ATTACKED
      evidence: Source-size enforcement and its mirrored parity baseline retire exactly the former monolith exception.
      artifacts: [scripts/check_lib_py.py, test_cap_1_source_file_line_cap.py]
    - id: AT-9
      status: ATTACKED
      evidence: Public errors and warnings remain byte-identical in isolated parent-current cases and focused tests.
      artifacts: [test_session.py, test_session_config_knobs.py, test_udf.py]
    - id: AT-10
      status: ATTACKED
      evidence: Mutation pins, focused tests, and every final broad gate run against the converged tree.
      artifacts: [test_production_file_size.py, make py-test, make ci, make verify, make preflight]
```
