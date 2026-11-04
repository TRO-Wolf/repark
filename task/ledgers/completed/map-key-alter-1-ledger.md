# Unit ledger — MAP-KEY-ALTER-1 · every ALTER and schema-evolution union that would change a field inside a map key refuses as Spark does

**Date:** 2026-11-04 · **Branch:** `fix/map-key-alter-1` · **Base:** `7be4ebc5` (origin/main, merged into the branch at `f6ca0090`) · **Head:** `a6233fea` · **Squash:** `3dc43ce1` on main (PR #1256)
**Model:** builder `claude-opus-5-5` (round one and the delta) · verifiers `claude-opus-5-5` and `muse-spark-1.3-contributor` (two rounds each) · records `claude-haiku-5-5`
**Policy:** [../../../AGENTS.md](../../../AGENTS.md) · **Path:** STANDARD.

**Status:** completed 2026-11-04 at `a6233fea` (squashed to main as `3dc43ce1`, PR #1256) after two verifier rounds. Round one: the Opus verdict was FAIL on F1 (a case-variant rename of a map-key field committed) and F2 (a self-move no-op); the Muse verdict was FAIL on R1F1 to R1F3 (schema-evolution unions added or widened a field inside a map key). The delta at `a6233fea` closed all five. The Opus re-check and the Muse re-check both PASS. Clauses C-001 to C-008 are PROVEN; C-009 to C-018 are OPEN leads (L1 to L10). The records pass that writes this file closes the card MAP-KEY-ALTER-1 and files the three S3 cards it names.

**Why.** Nested rule doc D1 and D16 ([nested-rule-2026-11-03.md](../../../docs/corpus/nested-rule-2026-11-03.md)). On both doors an ALTER that changed a field inside a map key (`map<struct<...>, V>`) was applied where Spark refuses with `Cannot alter map keys`; the mapkey `A-ddl` rows matched Spark on 24 of 33. A move of a direct key child was reordered where Spark's ALTER succeeds and leaves the key struct unchanged. Schema-evolution unions (INSERT BY NAME under merge-schema, MERGE WITH SCHEMA EVOLUTION) could add or widen a field inside a key and commit it. The rule (Java `SchemaUpdate.java` at `apache-iceberg-1.11.0`, `ApplyChanges.map`, the throw at line 724; `UnionByNameVisitor`) is ported into one shared file, `crates/repark-iceberg/src/write/map_key_change.rs`, which both DDL doors and every schema-evolution caller call.

**Clause.** The `A-ddl` rows of the measured cells table ([nested-cells-2026-11-03.jsonl](../../../docs/corpus/nested-cells-2026-11-03.jsonl)): 173 rows, 172 pinned (one row has no insert statement and is skipped), with the controls unmoved; the union arm as the schema-evolution callers enumerate it; the native door's DDL rows. The pin binding is in [map-key-alter-1-pins.md](map-key-alter-1-pins.md).

## PROPOSITION LEDGER — MAP-KEY-ALTER-1 — 2026-11-04

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | On the Spark door, every ALTER that would change a field inside a map key (drop, rename, type change, COMMENT, add under the key) refuses with Spark's text and commits nothing; the mapkey `A-ddl` rows match Spark on 33 of 33 (24 before). | Table-driven pin over the `A-ddl` rows: act or refuse, the refusal text with field ids masked, and the metadata location and schema compared on refusal. | PROVEN | Pins: `nested_ddl_cells_answer_spark_on_map_keys_and_keep_every_control`, `every_change_to_a_map_key_field_refuses_with_spark_text`, `adding_to_a_map_key_refuses_with_spark_text`, `a_map_inside_a_map_key_refuses_with_the_outer_map`, `a_primitive_map_key_keeps_its_own_refusals`. Red on the base door code: 7 of 9 tests fail (builder, Opus round one); green at head. Opus round one replayed the 33 unit rows on the facade: 33 of 33 match, with nothing committed on each refusal. The post-DDL schema equals Spark's schema1 on 8 of 8 mapkey fixtures (3 of 8 before). |
| C-002 | On the native (ANSI) door the same refusals hold: a drop, rename or add under a map key refuses with nothing committed, and the metadata file count and the Arrow schema are unchanged on refusal. | Rust pins that compare the metadata file count and the schema before and after each refusal. | PROVEN | Pins: `native_door_refuses_a_drop_rename_or_add_under_a_map_key`, `native_door_refuses_changes_deeper_under_a_map_key`. Red on base with the two native source files stashed: 3 of 4 tests fail, the controls pass (builder). With the delta stashed, both new native refusal tests fail (Opus round two). Opus round one replayed the ten mapkey DDL rows on the native door: each acts or refuses as Spark does, text equal on nine; dotted TYPE refuses at parse, lead L8 (C-016). |
| C-003 | A move of a direct key-struct child (FIRST, or AFTER a sibling) commits nothing and leaves the key struct unchanged, as Spark's ALTER does; a move under a deeper key path refuses with the map-key text; a move AFTER the field itself refuses with Java's `Cannot move ... after itself` before the no-op applies. | Pins over the move rows on both doors, each with the no-commit check. | PROVEN | Pins: `moving_a_map_key_field_succeeds_and_keeps_the_key_struct`, `a_struct_below_a_map_key_refuses_every_change_and_its_move`, `moving_a_map_key_field_after_itself_refuses_before_the_no_op`, `native_door_moves_a_map_key_field_as_a_no_op`, `native_door_refuses_a_map_key_field_moved_after_itself`. The unit's brief said a move refuses; the table (the move ddl1 row: Spark OK, key order a,b), the rule doc D16 and the Java source say the move is never applied to the key, so the no-op won. Opus round one F2 (`mk.key.b AFTER b` acted with nothing committed) closed in the delta: Opus round two has it refusing on both doors. Observation, not a finding: Spark commits a metadata file for the unapplied move and RePark commits none (Opus round one O1). |
| C-004 | A schema-evolution union that would add, widen or update a field inside a map key refuses before any write, on every enumerated caller: INSERT BY NAME under merge-schema, a positional INSERT routed by name, a DataFrame append with an extra top-level column, and MERGE WITH SCHEMA EVOLUTION; a union that leaves the key as it is still writes. | Pins per caller with a no-write proof (metadata location, schema, file count, zero rows in the target); facade pins for the SQL and DataFrame routes. | PROVEN | Spark door pins: `a_union_that_adds_or_widens_below_a_map_key_refuses_on_every_evolving_write`, `a_union_that_adds_a_field_to_a_map_key_struct_refuses_with_the_add_text`, `a_union_that_widens_a_primitive_map_key_refuses_with_the_update_text`, `a_dataframe_append_with_an_extra_column_and_a_key_change_adds_nothing`, `a_union_that_leaves_the_map_key_as_it_is_still_writes`. Facade pins: `test_an_evolving_statement_refuses_a_map_key_change`, `test_a_dataframe_append_with_an_extra_column_refuses_a_map_key_change`. With the delta stashed, 12 of 15 new tests fail (Spark 6 of 8, native 2 of 2, facade 4 of 5); the three greens are controls. Round one reproduced three silent commits (BY NAME deep add, BY NAME deep widen, MERGE deep add; Muse R1F1 to R1F3); round two reproduces each as a refusal with the schema, the file counts and the rows unchanged. The native door has no union path (its MERGE sets schema evolution off). A DataFrame append that differs only inside the key skips the union: lead L1 (C-009). |
| C-005 | Every rename of a field inside a map key refuses, whatever the target name, with nothing committed: the map-key text for a case variant or a free name; the fork's duplicate-name text for a rename onto an exact-case sibling (lead L5, C-013). A plain struct's rename is unchanged, and Java allows it. | Pin over the six target spellings on the Spark door and the native spellings, with the table state compared before and after. | PROVEN | Pins: `renaming_a_map_key_field_refuses_whatever_the_new_name` (TO A, TO B, mk.key.A TO B, mk.key.b TO A, TO a, TO b), `native_door_refuses_every_rename_of_a_map_key_field`. Opus round one F1 (S1): `RENAME COLUMN mk.key.a TO A` and three other case variants committed on both doors, leaving a case-colliding key. Closed in the delta: Opus round two has every case variant refusing with the map-key text on both doors. With caseSensitive on, the facade refuses the same statements (two with `Cannot rename missing column`); the native door has no case mode, so that check is facade-only. |
| C-006 | The controls are unmoved: the non-mapkey groups of the `A-ddl` table keep their before and after counts (struct 32 of 33, map 32 of 33, list 33 of 33, structlist 32 of 33, empty 0 of 8); the 139 control rows keep their recorded RePark outcome; map-value and list-element fields still evolve through DDL and through unions. | The control rows of the table pin; the value-side and list-element pins. | PROVEN | Pins: `nested_ddl_cells_answer_spark_on_map_keys_and_keep_every_control`, `map_value_and_list_element_fields_still_evolve`, `map_value_and_list_element_unions_still_evolve`, `native_door_still_evolves_map_value_and_list_element_fields`, `test_a_map_value_union_still_evolves`. Opus round one and round two replayed the 139 control rows on the facade: 139 of 139 match. Muse round two: the union controls evolve. |
| C-007 | Boundary and gates: the diff is the unit's files plus one `pub mod` line in `crates/repark-iceberg/src/write/mod.rs`; no fork, reader, writer data path or `.github` change. At the head the suites are repark-spark 3160, repark-sql 660, repark-core 1598 and repark-iceberg 1061 passed, facade iceberg 2220 passed (192 skipped, 12 xfailed), facade sql 1749 passed (1 skipped); `make rust-panic-ban` and `make check-map-size` exit 0 (448 maps clean); four baselines in `scripts/check_map_size.py` lowered to exact sizes, none raised. | The verifiers' runs at the head, and the boundary check on the name list. | PROVEN | none (records). Opus round one section 4 and round two section 2; Muse round two claim 3. Round one's Muse run had one unrelated repark-core timing failure under load (two_sessions_on_one_sink_wait_for_the_scope, microbatch lifecycle, untouched files); the round-two run of repark-core passed. |
| C-008 | Hygiene: no code comment in the Rust or Python diff; each unit commit carries one `Authored-By` trailer on its last line; no home or scratch path, session link, account id, bucket, region or ARN in added lines; the baselines are lowered, never raised. | Diff inspection by both verifiers and the builder's comment gate. | PROVEN | none (records). Round one: 0 added comment lines, one trailer on each unit commit (`ba0ebe88`, `ec02bf63` claude-opus-5-5), no secrets in the added lines (Muse claim 4). Round two: `a6233fea` carries one trailer (claude-opus-5-5). Builder gate: 0 added comment lines. |
| C-009 | Lead L1 (builder question 1; Opus round one C6 and C7; Opus round two C6): a DataFrame `writeTo` or `saveAsTable` append whose frame differs only inside the map-key column is lowered to an explicit-column INSERT. It down-casts a wider key field, or the planner refuses it with a cast error, and it never reaches the union, so the union rule cannot run there. | Closing question, a scope decision for the owner: lower every merge-schema DataFrame append to BY NAME (this changes every such append, value-side nested evolution included), or keep the path and state its texts as an exclusion. | OPEN | Card [DF-APPEND-MAP-KEY-UNION-1](../../roadmap/mid-term/df-append-map-key-union-1-card-2026-11-04.md). |
| C-010 | Lead L2 (Opus round two F3, S3, latent): the union walk does not model doc updates. The fork's `union_update_existing` calls `update_column_doc` when the incoming doc differs, so a doc on a key field in the incoming schema would commit under a key where Java refuses. Not reachable today: a measured source whose key field carries a doc leaves no doc on either side. | Closing question: an Alter step in the union walk when the incoming doc differs from the current one, or a pin that records the walk as unreachable. | OPEN | Opus round two probe `docprobe2` (a doc on the source key field, then an INSERT BY NAME with an extra column). No card: the unit's card list names none. |
| C-011 | Lead L3 (PR #1256 body, "Not changed"): RePark's CREATE numbers a map key's struct children before the value id, so the field ids in a map-key refusal text differ from Spark's. The texts match once the ids are masked, which is how the table compares them. | Closing question: align the CREATE id numbering of key children with Java's, then pin the unmasked text of one refusal. | OPEN | Card [ICE-CREATE-MAP-KEY-FIELD-IDS-1](../../roadmap/mid-term/ice-create-map-key-field-ids-1-card-2026-11-04.md). |
| C-012 | Lead L4 (builder question 4; PR #1256 body): a COMMENT on a map-key field that repeats the current doc is refused with the map-key text. Java's source reads an equal doc as a no-op. Spark's answer is not measured. | Closing question: Spark's answer to an equal-doc COMMENT on a key field, measured; then match it. | OPEN | Card [MAP-KEY-REFUSAL-TEXTS-1](../../roadmap/mid-term/map-key-refusal-texts-1-card-2026-11-04.md). |
| C-013 | Lead L5 (Muse R1F4, S3, pre-existing; Opus round one C2): a rename of a key-struct field onto an exact-case sibling (`mk.key.a TO b`) refuses with the fork's duplicate-name text `Invalid schema: multiple fields for name mk.key.b`, where Spark answers FIELD_ALREADY_EXISTS. Plain structs behave the same way. | Closing question: a rename FIELD_ALREADY_EXISTS check before the apply, with Spark's text, measured. | OPEN | Card [MAP-KEY-REFUSAL-TEXTS-1](../../roadmap/mid-term/map-key-refusal-texts-1-card-2026-11-04.md). |
| C-014 | Lead L6 (Muse R1F6, S3, pre-existing): `DROP COLUMN mk.key` and `RENAME COLUMN mk.key TO k2` answer the fork's terse texts `Cannot delete map keys: map` and `Cannot update map keys: map`, where Java renders the full map type. Nothing is written. | Closing question: the full-map rendering, checked against Spark's text. | OPEN | Card [MAP-KEY-REFUSAL-TEXTS-1](../../roadmap/mid-term/map-key-refusal-texts-1-card-2026-11-04.md). |
| C-015 | Lead L7 (Opus round one C3, S3, pre-existing): dotted `ALTER COLUMN` with DROP NOT NULL, SET NOT NULL, SET DEFAULT, DROP DEFAULT and CHANGE COLUMN refuses at parse on both doors, with nothing committed. For an optional key field, DROP NOT NULL is a Java no-op that Spark accepts. | Closing question: parse the dotted nullability and default changes on both doors, then measure Spark's answers. | OPEN | Card [MAP-KEY-REFUSAL-TEXTS-1](../../roadmap/mid-term/map-key-refusal-texts-1-card-2026-11-04.md). |
| C-016 | Lead L8 (Opus round one C4 and C5, S3, pre-existing): on the native door a dotted `ALTER COLUMN mk.key.a TYPE BIGINT` (and SET DATA TYPE) refuses with a parse error, not Spark's `Cannot alter map keys`; every mapkey INSERT with `CAST(NULL AS MAP<STRUCT<...>, ...>)` refuses at parse. Nothing is committed. | Closing question: dotted TYPE and the MAP-of-STRUCT cast on the ANSI door. | OPEN | Card [MAP-KEY-REFUSAL-TEXTS-1](../../roadmap/mid-term/map-key-refusal-texts-1-card-2026-11-04.md). |
| C-017 | Lead L9 (Opus round two F4, S3, pre-existing): under `spark.sql.caseSensitive=true`, `ALTER COLUMN mk.key.B AFTER b` refuses with `Cannot move mk.key.B after itself`, where Spark reports an unresolved column. Move names resolve case-insensitively whatever the setting. Nothing is committed. | Closing question: the move's column resolution under the case setting, measured. | OPEN | Card [MAP-KEY-REFUSAL-TEXTS-1](../../roadmap/mid-term/map-key-refusal-texts-1-card-2026-11-04.md). |
| C-018 | Lead L10 (Opus round one C1, S3; Opus round two: now refused): `RENAME COLUMN mk.key.a TO a` (a self-rename) refuses with the map-key text, where Spark answers FIELD_ALREADY_EXISTS (derived from Spark's analyzer, not measured). Round one had it acting with nothing committed. | Closing question: Spark's text for the self-rename, measured. | OPEN | Card [MAP-KEY-REFUSAL-TEXTS-1](../../roadmap/mid-term/map-key-refusal-texts-1-card-2026-11-04.md). |

## Evidence

**Verifier verdicts.** Opus 5.5 (`claude-opus-5-5`), round one: VERDICT FAIL on F1 (S1, the rename arm), with F2 and cards C1 to C7. Opus round two: VERDICT PASS, with F3 and F4 as S3 cards and the closure of all earlier findings. Muse (`muse-spark-1.3-contributor`), round one: CONCLUDED, FAIL on R1F1 to R1F3 (S1), with R1F4 to R1F6 as S3 cards. Muse round two: PASS, all five first-round findings closed with no-write proofs and about 50 neighbour probes clean. All four reports are untracked; their verdict lines are transcribed here.

**Round-one builder counts and round-two counts.** The round-one suites at `ec02bf63` were repark-spark 3152, repark-sql 658, repark-core 1598, repark-iceberg 1061, facade iceberg 2215 and facade sql 1749. The delta's counts at `a6233fea` are the final ones in C-007.

**Mapping of the leads.** L1 = C-009 (Opus C6 and C7), L2 = C-010 (Opus round two F3), L3 = C-011 (PR #1256 "Not changed"), L4 = C-012 (builder question 4), L5 = C-013 (Muse R1F4, Opus C2), L6 = C-014 (Muse R1F6), L7 = C-015 (Opus C3), L8 = C-016 (Opus C4 and C5), L9 = C-017 (Opus round two F4), L10 = C-018 (Opus C1).

**Builder question 2 (design).** The union step list is re-derived in RePark because the fork's `TransactionAction` is `pub(crate)`. The alternative, a dry-run catalog that captures the AddSchema update, needs about 17 Catalog methods; the walk was chosen. The fork still builds the evolved schema; the walk only decides whether a map key would change.

## Review findings

```yaml
FINDING:
  id: OPUS-F1
  severity: S1
  category: AT-1
  clause: C-005
  claim: A case-variant rename of a map-key struct field committed on both doors (mk.key.a TO A, mk.key.A TO B, mk.key.a TO B, mk.key.b TO A); Java refuses alter map keys and Spark refuses with FIELD_ALREADY_EXISTS.
  evidence: Opus verifier round one, F1, probes P01 to P03 on the facade and the native probe; Opus round two section 1 closes it.
  disposition: REMEDIATED (a6233fea: every rename under a map key refuses whatever the target name)
```

```yaml
FINDING:
  id: OPUS-F2
  severity: S3
  category: AT-1
  clause: C-003
  claim: ALTER COLUMN mk.key.b AFTER b acted as a no-op; Java refuses Cannot move mk.key.b after itself, and the same self-move refused before the unit.
  evidence: Opus verifier round one, F2; Opus round two section 1 (refuses on both doors, nothing committed).
  disposition: REMEDIATED (a6233fea: the after-itself check runs before the no-op short-circuit)
```

```yaml
FINDING:
  id: MUSE-R1F1
  severity: S1
  category: AT-6
  clause: C-004
  claim: INSERT BY NAME under merge-schema added a field deep inside a map-key struct; Java refuses Cannot alter map keys and the schema is unchanged.
  evidence: Muse verifier round one, R1F1, reproduced by the orchestrator on the facade; Muse round two closes it with a no-write proof.
  disposition: REMEDIATED (a6233fea: the union rule runs before the write on the BY NAME route)
```

```yaml
FINDING:
  id: MUSE-R1F2
  severity: S1
  category: AT-6
  clause: C-004
  claim: INSERT BY NAME under merge-schema widened a primitive field deep inside a map-key struct; Java refuses Cannot alter map keys.
  evidence: Muse verifier round one, R1F2; orchestrator reproduction; Muse round two closes it.
  disposition: REMEDIATED (a6233fea: the union rule refuses the widening before the write)
```

```yaml
FINDING:
  id: MUSE-R1F3
  severity: S1
  category: AT-6
  clause: C-004
  claim: MERGE WITH SCHEMA EVOLUTION added a field deep inside a map-key struct; Java refuses Cannot alter map keys.
  evidence: Muse verifier round one, R1F3; orchestrator reproduction; Muse round two closes it.
  disposition: REMEDIATED (a6233fea: evolve_merge_schema runs the union rule before the per-column update)
```

```yaml
FINDING:
  id: MUSE-R1F4
  severity: S3
  category: AT-8
  clause: C-013
  claim: A rename onto an exact-case sibling under a key refuses with the fork's duplicate-name text, not Spark's FIELD_ALREADY_EXISTS; the same on plain structs (pre-existing).
  evidence: Muse verifier round one, R1F4 (card); orchestrator reproduction.
  disposition: OPEN (card MAP-KEY-REFUSAL-TEXTS-1, lead L5)
```

```yaml
FINDING:
  id: MUSE-R1F5
  severity: S3
  category: AT-8
  clause: C-004
  claim: The union direct-add refusal used the fork's terse text without the Unsupported prefix or the full map rendering.
  evidence: Muse verifier round one, R1F5; Opus round two section 1 (the full text now, aligned).
  disposition: REMEDIATED (a6233fea: the full Unsupported table change text with the map rendering)
```

```yaml
FINDING:
  id: MUSE-R1F6
  severity: S3
  category: AT-8
  clause: C-014
  claim: DROP and RENAME of the map key itself answer the fork's terse texts (pre-existing, untouched code).
  evidence: Muse verifier round one, R1F6 (card); orchestrator reproduction.
  disposition: OPEN (card MAP-KEY-REFUSAL-TEXTS-1, lead L6)
```

```yaml
FINDING:
  id: OPUS-F3
  severity: S3
  category: AT-1
  clause: C-010
  claim: The union walk does not model doc updates; a doc on a key field in the incoming schema would commit a key change where Java refuses. Latent: not reachable today.
  evidence: Opus verifier round two, F3, probe docprobe2.
  disposition: OPEN (lead L2; no card)
```

```yaml
FINDING:
  id: OPUS-F4
  severity: S3
  category: AT-8
  clause: C-017
  claim: Under caseSensitive=true, a move of a key child named with the other case refuses with the after-itself text, where Spark reports an unresolved column; nothing is committed.
  evidence: Opus verifier round two, F4, facade probe with caseSensitive on.
  disposition: OPEN (card MAP-KEY-REFUSAL-TEXTS-1, lead L9)
```

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: map-key-alter-1
  complete: true
  reattested: [AT-1, AT-3, AT-6, AT-8, AT-10]
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Both Opus rounds walked the enumerated DDL, rename, move and union arms against the Java source (SchemaUpdate.java, ApplyChanges.map, the rename and nullability checks, UnionByNameVisitor) and the fork's union (rev 66901f0). The round-one and round-two probes ran about 35 and 37 facade neighbours and 15 native probes. Findings OPUS-F1 to OPUS-F4 and MUSE-R1F4 to MUSE-R1F6 are AT-1 or AT-8 items; the Muse rounds used claim numbers, classified here by the records clerk.
      artifacts: [crates/repark-iceberg/src/write/map_key_change.rs, crates/repark-spark/src/nested_column_ddl.rs, crates/repark-spark/src/column_move.rs, crates/repark-spark/src/tests/map_key_alter.rs, crates/repark-spark/src/tests/map_key_union.rs, crates/repark-sql/src/alter/nested.rs, crates/repark-sql/tests/alter_map_key.rs]
    - id: AT-2
      status: N/A
      justification: N/A — untouched by origin/main..a6233fea beyond the deeper-path and two-key shapes, which the table's rows walk under AT-1 and AT-10. The record keeps the verifiers' category as written.
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal was checked for nothing committed: the metadata location, the schema, the metadata and data file counts and the target rows, on both doors and on each union caller. The round-one red checks and the round-two closure proofs cover the failure path of each arm. No finding of the failure kind beyond the union commits (AT-6).
      artifacts: [crates/repark-iceberg/src/write/map_key_change.rs, crates/repark-sql/tests/alter_map_key.rs, crates/repark-spark/src/tests/map_key_union.rs, python/repark/tests/iceberg/test_map_key_union_1.py]
    - id: AT-4
      status: N/A
      justification: N/A — untouched by origin/main..a6233fea; no concurrency, ordering or shared-state change in the diff.
    - id: AT-5
      status: N/A
      justification: N/A — untouched by origin/main..a6233fea; no authentication, input-injection or secrets surface in the diff (the hygiene check is C-008).
    - id: AT-6
      status: ATTACKED
      evidence: The union arm committed silent schema drift inside a map key on three paths (MUSE-R1F1 to R1F3, S1), closed in the delta with no-write proofs. The DataFrame append that differs only inside the key is lead L1 (C-009, a write path that skips the rule; no wrong read). The union walk ignores doc updates (OPUS-F3, latent). No wrong read was found on any cell group.
      artifacts: [crates/repark-iceberg/src/write/map_key_change.rs, crates/repark-spark/src/tests/map_key_union.rs, python/repark/tests/iceberg/test_map_key_union_1.py]
    - id: AT-7
      status: N/A
      justification: N/A — untouched by origin/main..a6233fea; the diff adds a rule walk over the key's fields, with no unbounded growth or hot loop.
    - id: AT-8
      status: ATTACKED
      evidence: The error contracts were checked against Spark's texts with field ids masked: every DDL refusal arm and the union add and update texts match. The fork's terse texts (MUSE-R1F4, R1F6), the parse-level refusals (C-015, C-016) and the caseSensitive move text (OPUS-F4) differ from Spark and are carded as MAP-KEY-REFUSAL-TEXTS-1.
      artifacts: [crates/repark-iceberg/src/write/map_key_change.rs, crates/repark-spark/src/nested_column_ddl.rs, crates/repark-spark/src/column_move.rs, crates/repark-sql/src/alter/nested.rs]
    - id: AT-9
      status: N/A
      justification: N/A — untouched by origin/main..a6233fea; no logging, metrics or alarm surface in the diff.
    - id: AT-10
      status: ATTACKED
      evidence: Red on base: the Spark pin 7 of 9 fails (round one) and the native pin 3 of 4 (round one); with the delta stashed, 12 of 15 new tests fail (round two: Spark 6 of 8, native 2 of 2, facade 4 of 5). The three greens are controls by design. The table pin replays all 172 rows; with the fix stashed its failure lists the 9 mapkey mismatches. Gaps named as leads: the Unicode and unmasked-id texts (C-011), the caseSensitive move (C-017).
      artifacts: [crates/repark-spark/src/tests/map_key_alter.rs, crates/repark-spark/src/tests/map_key_union.rs, crates/repark-sql/tests/alter_map_key.rs, python/repark/tests/iceberg/test_map_key_union_1.py]
```

## Scope notes

- The builder's round-one claim that a rename onto a same-named sibling keeps Spark's precedence was wrong: RePark had no rename FIELD_ALREADY_EXISTS check, and the exact-case sibling was caught only by the fork's duplicate-name validation. The delta removed the exception (C-005).
- The unit's brief said a move of a direct key child refuses. The table, D16 and the Java source said no-op; the table won (C-003).
- Round one's Muse run recorded one repark-core timing failure under load (two_sessions_on_one_sink_wait_for_the_scope, microbatch lifecycle, files this unit does not touch); the round-two run passed.
- Opus round one: the verifier's reproductions were on the facade and the native door; the Spark JVM was not run (no live oracle), so Spark's texts for F1, C1 and F2 are derived from the analyzer and Java and are marked as such in the leads.
