Rendered by `probe/render_step0.py` from `raw/main-dd77603a.jsonl`; module `_native.abi3.so`.

Path: `identity` = the batches reached `collect_identity_update_rows`; `fork` = no identity event (the statement went to the fork's `TableProvider::update`). Batches: new-value batches in arrival order, each shown as its set of nullable fields. Predicted: refusals that `MemTable::try_new`'s `contains` check implies from the printed nullability (copy-on-write only).

| Door | Statement | Layout | Kind | Attempts | Outcomes | Path | Batches per attempt | Order (count, outcome) | Predicted / refused |
|---|---|---|---|---|---|---|---|---|---|
| native | float-literal | mixed-nulls | v2-cow | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| native | int-both | mixed-nulls | v2-cow | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| native | int-both | no-nulls | v2-cow | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| native | int-col | mixed-nulls | v2-cow | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| native | int-col | no-nulls | v2-cow | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| native | int-one | mixed-nulls | v2-cow | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| native | int-one | no-nulls | v2-cow | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| native | required-set-null | required | v2-cow | 3 | refused-other 3 | fork 3 | 0: 3 | `none` (3, refused-other) | 0 / 0 |
| native | string | mixed-nulls | v2-cow | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| native | string | no-nulls | v2-cow | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| spark | float-literal | mixed-nulls | v2-cow | 10 | answered 4, refused-mismatch 6 | identity 10 | 2: 10 | `{e} then {i,e}` (6, refused-mismatch); `{i,e} then {e}` (4, answered) | 6 / 6 |
| spark | float-literal | mixed-nulls | v2-mor | 10 | answered 10 | identity 10 | 2: 10 | `{e} then {i,e}` (8, answered); `{i,e} then {e}` (2, answered) | 0 / 0 |
| spark | float-literal | mixed-nulls | v3 | 10 | answered 2, refused-mismatch 8 | identity 10 | 2: 10 | `{e,_last_updated_sequence_number} then {i,e,_last_updated_sequence_number}` (8, refused-mismatch); `{i,e,_last_updated_sequence_number} then {e,_last_updated_sequence_number}` (2, answered) | 8 / 8 |
| spark | int-both | mixed-nulls | v2-cow | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| spark | int-both | mixed-nulls | v2-mor | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| spark | int-both | mixed-nulls | v3 | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| spark | int-both | no-nulls | v2-cow | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| spark | int-both | no-nulls | v2-mor | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| spark | int-both | no-nulls | v3 | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| spark | int-col | mixed-nulls | v2-cow | 10 | answered 5, refused-mismatch 5 | identity 10 | 2: 10 | `{e} then {i,e}` (5, refused-mismatch); `{i,e} then {e}` (5, answered) | 5 / 5 |
| spark | int-col | mixed-nulls | v2-mor | 10 | answered 10 | identity 10 | 2: 10 | `{e} then {i,e}` (5, answered); `{i,e} then {e}` (5, answered) | 0 / 0 |
| spark | int-col | mixed-nulls | v3 | 10 | answered 7, refused-mismatch 3 | identity 10 | 2: 10 | `{e,_last_updated_sequence_number} then {i,e,_last_updated_sequence_number}` (3, refused-mismatch); `{i,e,_last_updated_sequence_number} then {e,_last_updated_sequence_number}` (7, answered) | 3 / 3 |
| spark | int-col | no-nulls | v2-cow | 10 | answered 10 | identity 10 | 2: 10 | `{} then {}` (10, answered) | 0 / 0 |
| spark | int-col | no-nulls | v2-mor | 10 | answered 10 | identity 10 | 2: 10 | `{} then {}` (10, answered) | 0 / 0 |
| spark | int-col | no-nulls | v3 | 10 | answered 10 | identity 10 | 2: 10 | `{_last_updated_sequence_number} then {_last_updated_sequence_number}` (10, answered) | 0 / 0 |
| spark | int-one | mixed-nulls | v2-cow | 10 | answered 10 | identity 10 | 1: 10 | `{i,e}` (10, answered) | 0 / 0 |
| spark | int-one | mixed-nulls | v2-mor | 10 | answered 10 | identity 10 | 1: 10 | `{i,e}` (10, answered) | 0 / 0 |
| spark | int-one | mixed-nulls | v3 | 10 | answered 10 | identity 10 | 1: 10 | `{i,e,_last_updated_sequence_number}` (10, answered) | 0 / 0 |
| spark | int-one | no-nulls | v2-cow | 10 | answered 10 | identity 10 | 1: 10 | `{}` (10, answered) | 0 / 0 |
| spark | int-one | no-nulls | v2-mor | 10 | answered 10 | identity 10 | 1: 10 | `{}` (10, answered) | 0 / 0 |
| spark | int-one | no-nulls | v3 | 10 | answered 10 | identity 10 | 1: 10 | `{_last_updated_sequence_number}` (10, answered) | 0 / 0 |
| spark | required-set-null | required | v2-cow | 3 | refused-mismatch 1, refused-other 2 | identity 3 | 2: 3 | `{i,r} then {r}` (2, refused-other); `{r} then {i,r}` (1, refused-mismatch) | 1 / 1 |
| spark | required-set-null | required | v2-mor | 3 | refused-other 3 | identity 3 | 2: 3 | `{i,r} then {r}` (1, refused-other); `{r} then {i,r}` (2, refused-other) | 0 / 0 |
| spark | required-set-null | required | v3 | 3 | refused-mismatch 2, refused-other 1 | identity 3 | 2: 3 | `{i,r,_last_updated_sequence_number} then {r,_last_updated_sequence_number}` (1, refused-other); `{r,_last_updated_sequence_number} then {i,r,_last_updated_sequence_number}` (2, refused-mismatch) | 2 / 2 |
| spark | string | mixed-nulls | v2-cow | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| spark | string | mixed-nulls | v2-mor | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| spark | string | mixed-nulls | v3 | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| spark | string | no-nulls | v2-cow | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| spark | string | no-nulls | v2-mor | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |
| spark | string | no-nulls | v3 | 10 | answered 10 | fork 10 | 0: 10 | `none` (10, answered) | 0 / 0 |

Totals: 372 attempts, 25 refused with the mismatch text, 25 predicted from the printed batches, 0 attempts where the prediction and the outcome disagree.

## Refusal texts

| Statement | Outcome | Text (first 200 characters) | Count |
|---|---|---|---|
| float-literal | refused-mismatch | `Error during planning: Mismatch between schema and batches` | 14 |
| int-col | refused-mismatch | `Error during planning: Mismatch between schema and batches` | 8 |
| required-set-null | refused-mismatch | `Error during planning: Mismatch between schema and batches` | 3 |
| required-set-null | refused-other | `Error during planning: UPDATE cannot assign NULL to required column 'r'` | 3 |
| required-set-null | refused-other | `datafusion engine error: Arrow error: Invalid argument error: Column 'r' is declared as non-nullable but contains null values` | 6 |

## Tables the door could not create

| Cell | Text (first 200 characters) | Count |
|---|---|---|
| native|float-literal|mixed-nulls|v2-mor | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 97")` | 2 |
| native|float-literal|mixed-nulls|v2-mor | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 98")` | 8 |
| native|float-literal|mixed-nulls|v3 | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 97")` | 2 |
| native|float-literal|mixed-nulls|v3 | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 98")` | 8 |
| native|int-both|mixed-nulls|v2-mor | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 97")` | 2 |
| native|int-both|mixed-nulls|v2-mor | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 98")` | 8 |
| native|int-both|mixed-nulls|v3 | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 97")` | 2 |
| native|int-both|mixed-nulls|v3 | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 98")` | 8 |
| native|int-both|no-nulls|v2-mor | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 97")` | 2 |
| native|int-both|no-nulls|v2-mor | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 98")` | 8 |
| native|int-both|no-nulls|v3 | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 97")` | 2 |
| native|int-both|no-nulls|v3 | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 98")` | 8 |
| native|int-col|mixed-nulls|v2-mor | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 97")` | 2 |
| native|int-col|mixed-nulls|v2-mor | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 98")` | 8 |
| native|int-col|mixed-nulls|v3 | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 97")` | 2 |
| native|int-col|mixed-nulls|v3 | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 98")` | 8 |
| native|int-col|no-nulls|v2-mor | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 97")` | 2 |
| native|int-col|no-nulls|v2-mor | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 98")` | 8 |
| native|int-col|no-nulls|v3 | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 97")` | 2 |
| native|int-col|no-nulls|v3 | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 98")` | 8 |
| native|int-one|mixed-nulls|v2-mor | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 97")` | 1 |
| native|int-one|mixed-nulls|v2-mor | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 98")` | 9 |
| native|int-one|mixed-nulls|v3 | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 97")` | 1 |
| native|int-one|mixed-nulls|v3 | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 98")` | 9 |
| native|int-one|no-nulls|v2-mor | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 97")` | 1 |
| native|int-one|no-nulls|v2-mor | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 98")` | 9 |
| native|int-one|no-nulls|v3 | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 97")` | 1 |
| native|int-one|no-nulls|v3 | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 98")` | 9 |
| native|required-set-null|required|v2-mor | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 74")` | 3 |
| native|required-set-null|required|v3 | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 74")` | 3 |
| native|string|mixed-nulls|v2-mor | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 97")` | 2 |
| native|string|mixed-nulls|v2-mor | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 98")` | 8 |
| native|string|mixed-nulls|v3 | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 97")` | 2 |
| native|string|mixed-nulls|v3 | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 98")` | 8 |
| native|string|no-nulls|v2-mor | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 97")` | 1 |
| native|string|no-nulls|v2-mor | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 98")` | 9 |
| native|string|no-nulls|v3 | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 97")` | 1 |
| native|string|no-nulls|v3 | `SQL error: ParserError("Expected: end of statement, found: USING at Line: 1, Column: 98")` | 9 |

## One physical plan per door and statement (identity path only)

### spark|float-literal

```text
ProjectionExec: expr=[_file@6 as _file, _pos@7 as _pos, id@0 as id, i@1 as i, d@2 as d, tf@3 as tf, e@4 as e, s@5 as s, upd as flag]
  FilterExec: __repark_spark_nonnull__(d@2 IS NOT NULL AND d@2 >= -0 AND d@2 <= 0)
    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
      StreamingTableExec: partition_sizes=1, projection=[id, i, d, tf, e, s, _file, _pos]
```

### spark|int-col

```text
ProjectionExec: expr=[_file@6 as _file, _pos@7 as _pos, id@0 as id, i@1 as i, d@2 as d, tf@3 as tf, e@4 as e, s@5 as s, upd as flag]
  FilterExec: i@1 IS NULL OR i@1 <= 7
    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
      StreamingTableExec: partition_sizes=1, projection=[id, i, d, tf, e, s, _file, _pos]
```

### spark|int-one

```text
ProjectionExec: expr=[_file@6 as _file, _pos@7 as _pos, id@0 as id, i@1 as i, d@2 as d, tf@3 as tf, e@4 as e, s@5 as s, upd as flag]
  FilterExec: id@0 = 4
    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
      StreamingTableExec: partition_sizes=1, projection=[id, i, d, tf, e, s, _file, _pos]
```

### spark|required-set-null

```text
ProjectionExec: expr=[_file@3 as _file, _pos@4 as _pos, id@0 as id, i@1 as i, NULL as r, flag@2 as flag]
  FilterExec: i@1 IS NULL OR i@1 <= 7
    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
      StreamingTableExec: partition_sizes=1, projection=[id, i, flag, _file, _pos]
```
