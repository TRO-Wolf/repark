# Native-door write cells, measured against Spark (2026-11-08)

Measured by the Claude Haiku 5.5 clerk under WO native-door-ansi-route-1 (ruling F). The reference is Spark 4.1.2 with Iceberg 1.11.0 (the oracle). The native door's own reference is Trino, which is unmeasured, so Spark stands in for it here.

Door ids: `base/native` is `repark.sql()` on base (stock DataFusion); `base/facade` is `ReparkSession.sql()` on base; `proto/native` is `repark.sql()` routed through AnsiDialect; `proto/facade` is the facade on proto; `spark` is PySpark with Iceberg.

`files` is the data-file count before and after, counting every non-hidden file under `data/` except `.crc` files, so Iceberg delete files are included. `snapshot changed` compares the current snapshot id. Group W and group E rows are the persisted rows sorted by id on table `ice.ns.t`. Spark rows carry the label `reference`.

## Cells

| group | cell | leg/door | kind | outcome | rows after (id:v) | snapshot changed | files | label |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| W | P-U1-in | base/native | v2-cow | refused AnalysisException: Error during planning: UPDATE with a subquery in its WHERE clause is refused on `ice.ns.t`: subquery | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | REFUSED-WHERE-SPARK-ACCEPTED |
| W | P-U1-in | base/native | v2-mor | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| W | P-U1-in | base/native | v3-cow | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| W | P-D1-in | base/native | v2-cow | refused AnalysisException: Error during planning: DELETE with a subquery in its WHERE clause is refused on `ice.ns.t`: subquery | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | REFUSED-WHERE-SPARK-ACCEPTED |
| W | P-D1-in | base/native | v2-mor | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| W | P-D1-in | base/native | v3-cow | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| W | P-D3-exists_correlated | base/native | v2-cow | refused AnalysisException: Error during planning: DELETE with a subquery in its WHERE clause is refused on `ice.ns.t`: subquery | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | REFUSED-WHERE-SPARK-ACCEPTED |
| W | P-D3-exists_correlated | base/native | v2-mor | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| W | P-D3-exists_correlated | base/native | v3-cow | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| W | P-D5-not_in | base/native | v2-cow | refused AnalysisException: Error during planning: DELETE with a subquery in its WHERE clause is refused on `ice.ns.t`: subquery | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | REFUSED-WHERE-SPARK-ACCEPTED |
| W | P-D5-not_in | base/native | v2-mor | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| W | P-D5-not_in | base/native | v3-cow | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| W | P-D6-exists_no_match | base/native | v2-cow | refused AnalysisException: Error during planning: DELETE with a subquery in its WHERE clause is refused on `ice.ns.t`: subquery | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | REFUSED-WHERE-SPARK-ACCEPTED |
| W | P-D6-exists_no_match | base/native | v2-mor | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| W | P-D6-exists_no_match | base/native | v3-cow | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| W | P-C1-delete_in_self | base/native | v2-cow | refused AnalysisException: Error during planning: DELETE with a subquery in its WHERE clause is refused on `ice.ns.t`: subquery | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | REFUSED-WHERE-SPARK-ACCEPTED |
| W | P-C1-delete_in_self | base/native | v2-mor | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| W | P-C1-delete_in_self | base/native | v3-cow | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| E | E-u-required-is-null | base/native | v2-cow | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | EQUAL |
| E | E-u-required-is-null | base/native | v2-mor | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| E | E-u-required-is-null | base/native | v3-cow | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| E | E-a-false | base/native | v2-cow | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | EQUAL |
| E | E-a-false | base/native | v2-mor | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| E | E-a-false | base/native | v3-cow | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| E | E-c-one-eq-zero | base/native | v2-cow | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | EQUAL |
| E | E-c-one-eq-zero | base/native | v2-mor | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| E | E-c-one-eq-zero | base/native | v3-cow | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| E | E-selective-id-minus-1 | base/native | v2-cow | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | MISSING-COMMIT |
| E | E-selective-id-minus-1 | base/native | v2-mor | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| E | E-selective-id-minus-1 | base/native | v3-cow | not attempted (base native creates v2 copy-on-write only) | - | - | - | SETUP-MISSING |
| W | P-U1-in | base/facade | v2-cow | answered | 1:10 2:5 3:5 4:40 | yes | 1 -> 2 | EQUAL |
| W | P-U1-in | base/facade | v2-mor | answered | 1:10 2:5 3:5 4:40 | yes | 1 -> 3 | EQUAL |
| W | P-U1-in | base/facade | v3-cow | answered | 1:10 2:5 3:5 4:40 | yes | 1 -> 2 | EQUAL |
| W | P-D1-in | base/facade | v2-cow | answered | 1:10 4:40 | yes | 1 -> 2 | EQUAL |
| W | P-D1-in | base/facade | v2-mor | answered | 1:10 4:40 | yes | 1 -> 2 | EQUAL |
| W | P-D1-in | base/facade | v3-cow | answered | 1:10 4:40 | yes | 1 -> 2 | EQUAL |
| W | P-D3-exists_correlated | base/facade | v2-cow | answered | 1:10 4:40 | yes | 1 -> 2 | EQUAL |
| W | P-D3-exists_correlated | base/facade | v2-mor | answered | 1:10 4:40 | yes | 1 -> 2 | EQUAL |
| W | P-D3-exists_correlated | base/facade | v3-cow | answered | 1:10 4:40 | yes | 1 -> 2 | EQUAL |
| W | P-D5-not_in | base/facade | v2-cow | answered | 2:20 3:30 | yes | 1 -> 2 | EQUAL |
| W | P-D5-not_in | base/facade | v2-mor | answered | 2:20 3:30 | yes | 1 -> 2 | EQUAL |
| W | P-D5-not_in | base/facade | v3-cow | answered | 2:20 3:30 | yes | 1 -> 2 | EQUAL |
| W | P-D6-exists_no_match | base/facade | v2-cow | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | MISSING-COMMIT |
| W | P-D6-exists_no_match | base/facade | v2-mor | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | MISSING-COMMIT |
| W | P-D6-exists_no_match | base/facade | v3-cow | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | MISSING-COMMIT |
| W | P-C1-delete_in_self | base/facade | v2-cow | answered | (none) | yes | 1 -> 1 | EQUAL |
| W | P-C1-delete_in_self | base/facade | v2-mor | answered | (none) | yes | 1 -> 2 | EQUAL |
| W | P-C1-delete_in_self | base/facade | v3-cow | answered | (none) | yes | 1 -> 1 | EQUAL |
| E | E-u-required-is-null | base/facade | v2-cow | answered | 1:10 2:20 3:30 4:40 | yes | 1 -> 1 | EXTRA-COMMIT |
| E | E-u-required-is-null | base/facade | v2-mor | answered | 1:10 2:20 3:30 4:40 | yes | 1 -> 1 | EXTRA-COMMIT |
| E | E-u-required-is-null | base/facade | v3-cow | answered | 1:10 2:20 3:30 4:40 | yes | 1 -> 1 | EXTRA-COMMIT |
| E | E-a-false | base/facade | v2-cow | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | EQUAL |
| E | E-a-false | base/facade | v2-mor | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | EQUAL |
| E | E-a-false | base/facade | v3-cow | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | EQUAL |
| E | E-c-one-eq-zero | base/facade | v2-cow | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | EQUAL |
| E | E-c-one-eq-zero | base/facade | v2-mor | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | EQUAL |
| E | E-c-one-eq-zero | base/facade | v3-cow | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | EQUAL |
| E | E-selective-id-minus-1 | base/facade | v2-cow | answered | 1:10 2:20 3:30 4:40 | yes | 1 -> 1 | EQUAL |
| E | E-selective-id-minus-1 | base/facade | v2-mor | answered | 1:10 2:20 3:30 4:40 | yes | 1 -> 1 | EQUAL |
| E | E-selective-id-minus-1 | base/facade | v3-cow | answered | 1:10 2:20 3:30 4:40 | yes | 1 -> 1 | EQUAL |
| R | R1-ctas-unqualified | base/native | - | answered | [{"n": 5000}] | - | - | ACCEPTED-WHERE-SPARK-REFUSED |
| R | R2-bracketed-delete | base/native | - | refused AnalysisException: Error during planning: DELETE with a subquery in its WHERE clause is refused on `[ice].[ns].[t]`: su | 1:10 2:20 3:30 4:40 | - | - | BOTH-REFUSED |
| R | R3-kernel-by-name | base/native | - | refused AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `__repark_cast_timestamp_ns__` on search path [`system`. | - | - | - | BOTH-REFUSED |
| W | P-U1-in | proto/native | v2-cow | answered | 1:10 2:5 3:5 4:40 | yes | 1 -> 2 | EQUAL |
| W | P-U1-in | proto/native | v2-mor | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT, v INT) TB | - | - | - | SETUP-MISSING |
| W | P-U1-in | proto/native | v3-cow | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT, v INT) TB | - | - | - | SETUP-MISSING |
| W | P-D1-in | proto/native | v2-cow | answered | 1:10 4:40 | yes | 1 -> 2 | EQUAL |
| W | P-D1-in | proto/native | v2-mor | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT, v INT) TB | - | - | - | SETUP-MISSING |
| W | P-D1-in | proto/native | v3-cow | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT, v INT) TB | - | - | - | SETUP-MISSING |
| W | P-D3-exists_correlated | proto/native | v2-cow | answered | 1:10 4:40 | yes | 1 -> 2 | EQUAL |
| W | P-D3-exists_correlated | proto/native | v2-mor | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT, v INT) TB | - | - | - | SETUP-MISSING |
| W | P-D3-exists_correlated | proto/native | v3-cow | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT, v INT) TB | - | - | - | SETUP-MISSING |
| W | P-D5-not_in | proto/native | v2-cow | answered | 2:20 3:30 | yes | 1 -> 2 | EQUAL |
| W | P-D5-not_in | proto/native | v2-mor | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT, v INT) TB | - | - | - | SETUP-MISSING |
| W | P-D5-not_in | proto/native | v3-cow | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT, v INT) TB | - | - | - | SETUP-MISSING |
| W | P-D6-exists_no_match | proto/native | v2-cow | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | MISSING-COMMIT |
| W | P-D6-exists_no_match | proto/native | v2-mor | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT, v INT) TB | - | - | - | SETUP-MISSING |
| W | P-D6-exists_no_match | proto/native | v3-cow | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT, v INT) TB | - | - | - | SETUP-MISSING |
| W | P-C1-delete_in_self | proto/native | v2-cow | answered | (none) | yes | 1 -> 1 | EQUAL |
| W | P-C1-delete_in_self | proto/native | v2-mor | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT, v INT) TB | - | - | - | SETUP-MISSING |
| W | P-C1-delete_in_self | proto/native | v3-cow | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT, v INT) TB | - | - | - | SETUP-MISSING |
| E | E-u-required-is-null | proto/native | v2-cow | answered | 1:10 2:20 3:30 4:40 | yes | 1 -> 1 | EXTRA-COMMIT |
| E | E-u-required-is-null | proto/native | v2-mor | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT NOT NULL,  | - | - | - | SETUP-MISSING |
| E | E-u-required-is-null | proto/native | v3-cow | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT NOT NULL,  | - | - | - | SETUP-MISSING |
| E | E-a-false | proto/native | v2-cow | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | EQUAL |
| E | E-a-false | proto/native | v2-mor | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT NOT NULL,  | - | - | - | SETUP-MISSING |
| E | E-a-false | proto/native | v3-cow | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT NOT NULL,  | - | - | - | SETUP-MISSING |
| E | E-c-one-eq-zero | proto/native | v2-cow | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | EQUAL |
| E | E-c-one-eq-zero | proto/native | v2-mor | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT NOT NULL,  | - | - | - | SETUP-MISSING |
| E | E-c-one-eq-zero | proto/native | v3-cow | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT NOT NULL,  | - | - | - | SETUP-MISSING |
| E | E-selective-id-minus-1 | proto/native | v2-cow | answered | 1:10 2:20 3:30 4:40 | yes | 1 -> 1 | EQUAL |
| E | E-selective-id-minus-1 | proto/native | v2-mor | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT NOT NULL,  | - | - | - | SETUP-MISSING |
| E | E-selective-id-minus-1 | proto/native | v3-cow | setup failed: This feature is not implemented: Unsupported SQL statement: CREATE TABLE ice.ns.t (id INT NOT NULL,  | - | - | - | SETUP-MISSING |
| W | P-U1-in | proto/facade | v2-cow | answered | 1:10 2:5 3:5 4:40 | yes | 1 -> 2 | EQUAL |
| W | P-U1-in | proto/facade | v2-mor | answered | 1:10 2:5 3:5 4:40 | yes | 1 -> 3 | EQUAL |
| W | P-U1-in | proto/facade | v3-cow | answered | 1:10 2:5 3:5 4:40 | yes | 1 -> 2 | EQUAL |
| W | P-D1-in | proto/facade | v2-cow | answered | 1:10 4:40 | yes | 1 -> 2 | EQUAL |
| W | P-D1-in | proto/facade | v2-mor | answered | 1:10 4:40 | yes | 1 -> 2 | EQUAL |
| W | P-D1-in | proto/facade | v3-cow | answered | 1:10 4:40 | yes | 1 -> 2 | EQUAL |
| W | P-D3-exists_correlated | proto/facade | v2-cow | answered | 1:10 4:40 | yes | 1 -> 2 | EQUAL |
| W | P-D3-exists_correlated | proto/facade | v2-mor | answered | 1:10 4:40 | yes | 1 -> 2 | EQUAL |
| W | P-D3-exists_correlated | proto/facade | v3-cow | answered | 1:10 4:40 | yes | 1 -> 2 | EQUAL |
| W | P-D5-not_in | proto/facade | v2-cow | answered | 2:20 3:30 | yes | 1 -> 2 | EQUAL |
| W | P-D5-not_in | proto/facade | v2-mor | answered | 2:20 3:30 | yes | 1 -> 2 | EQUAL |
| W | P-D5-not_in | proto/facade | v3-cow | answered | 2:20 3:30 | yes | 1 -> 2 | EQUAL |
| W | P-D6-exists_no_match | proto/facade | v2-cow | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | MISSING-COMMIT |
| W | P-D6-exists_no_match | proto/facade | v2-mor | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | MISSING-COMMIT |
| W | P-D6-exists_no_match | proto/facade | v3-cow | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | MISSING-COMMIT |
| W | P-C1-delete_in_self | proto/facade | v2-cow | answered | (none) | yes | 1 -> 1 | EQUAL |
| W | P-C1-delete_in_self | proto/facade | v2-mor | answered | (none) | yes | 1 -> 2 | EQUAL |
| W | P-C1-delete_in_self | proto/facade | v3-cow | answered | (none) | yes | 1 -> 1 | EQUAL |
| E | E-u-required-is-null | proto/facade | v2-cow | answered | 1:10 2:20 3:30 4:40 | yes | 1 -> 1 | EXTRA-COMMIT |
| E | E-u-required-is-null | proto/facade | v2-mor | answered | 1:10 2:20 3:30 4:40 | yes | 1 -> 1 | EXTRA-COMMIT |
| E | E-u-required-is-null | proto/facade | v3-cow | answered | 1:10 2:20 3:30 4:40 | yes | 1 -> 1 | EXTRA-COMMIT |
| E | E-a-false | proto/facade | v2-cow | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | EQUAL |
| E | E-a-false | proto/facade | v2-mor | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | EQUAL |
| E | E-a-false | proto/facade | v3-cow | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | EQUAL |
| E | E-c-one-eq-zero | proto/facade | v2-cow | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | EQUAL |
| E | E-c-one-eq-zero | proto/facade | v2-mor | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | EQUAL |
| E | E-c-one-eq-zero | proto/facade | v3-cow | answered | 1:10 2:20 3:30 4:40 | no | 1 -> 1 | EQUAL |
| E | E-selective-id-minus-1 | proto/facade | v2-cow | answered | 1:10 2:20 3:30 4:40 | yes | 1 -> 1 | EQUAL |
| E | E-selective-id-minus-1 | proto/facade | v2-mor | answered | 1:10 2:20 3:30 4:40 | yes | 1 -> 1 | EQUAL |
| E | E-selective-id-minus-1 | proto/facade | v3-cow | answered | 1:10 2:20 3:30 4:40 | yes | 1 -> 1 | EQUAL |
| R | R1-ctas-unqualified | proto/native | - | refused AnalysisException: Error during planning: CREATE TABLE AS SELECT target `offset_nested_sort_1_native` is not a qualifie | unread: Error during planning: table 'datafusion.public.offset_nested_sort_1_nat | - | - | BOTH-REFUSED |
| R | R2-bracketed-delete | proto/native | - | refused ParseException: SQL error: ParserError("Expected: identifier, found: [ at Line: 1, Column: 13") | 1:10 2:20 3:30 4:40 | - | - | BOTH-REFUSED |
| R | R3-kernel-by-name | proto/native | - | answered | [{"__repark_cast_timestamp_ns__(Utf8(\"2026-01-02 03:04:05\"))": "2026-01-02 03:04:05"}] | - | - | ACCEPTED-WHERE-SPARK-REFUSED |
| W | P-U1-in | spark | v2-cow | answered | 1:10 2:5 3:5 4:40 | yes | 2 -> 3 | reference |
| W | P-U1-in | spark | v2-mor | answered | 1:10 2:5 3:5 4:40 | yes | 2 -> 5 | reference |
| W | P-U1-in | spark | v3-cow | answered | 1:10 2:5 3:5 4:40 | yes | 2 -> 3 | reference |
| W | P-D1-in | spark | v2-cow | answered | 1:10 4:40 | yes | 2 -> 3 | reference |
| W | P-D1-in | spark | v2-mor | answered | 1:10 4:40 | yes | 2 -> 4 | reference |
| W | P-D1-in | spark | v3-cow | answered | 1:10 4:40 | yes | 2 -> 3 | reference |
| W | P-D3-exists_correlated | spark | v2-cow | answered | 1:10 4:40 | yes | 2 -> 3 | reference |
| W | P-D3-exists_correlated | spark | v2-mor | answered | 1:10 4:40 | yes | 2 -> 4 | reference |
| W | P-D3-exists_correlated | spark | v3-cow | answered | 1:10 4:40 | yes | 2 -> 3 | reference |
| W | P-D5-not_in | spark | v2-cow | answered | 2:20 3:30 | yes | 2 -> 3 | reference |
| W | P-D5-not_in | spark | v2-mor | answered | 2:20 3:30 | yes | 2 -> 4 | reference |
| W | P-D5-not_in | spark | v3-cow | answered | 2:20 3:30 | yes | 2 -> 3 | reference |
| W | P-D6-exists_no_match | spark | v2-cow | answered | 1:10 2:20 3:30 4:40 | yes | 2 -> 2 | reference |
| W | P-D6-exists_no_match | spark | v2-mor | answered | 1:10 2:20 3:30 4:40 | yes | 2 -> 2 | reference |
| W | P-D6-exists_no_match | spark | v3-cow | answered | 1:10 2:20 3:30 4:40 | yes | 2 -> 2 | reference |
| W | P-C1-delete_in_self | spark | v2-cow | answered | (none) | yes | 2 -> 2 | reference |
| W | P-C1-delete_in_self | spark | v2-mor | answered | (none) | yes | 2 -> 4 | reference |
| W | P-C1-delete_in_self | spark | v3-cow | answered | (none) | yes | 2 -> 2 | reference |
| E | E-u-required-is-null | spark | v2-cow | answered | 1:10 2:20 3:30 4:40 | no | 2 -> 2 | reference |
| E | E-u-required-is-null | spark | v2-mor | answered | 1:10 2:20 3:30 4:40 | no | 2 -> 2 | reference |
| E | E-u-required-is-null | spark | v3-cow | answered | 1:10 2:20 3:30 4:40 | no | 2 -> 2 | reference |
| E | E-a-false | spark | v2-cow | answered | 1:10 2:20 3:30 4:40 | no | 2 -> 2 | reference |
| E | E-a-false | spark | v2-mor | answered | 1:10 2:20 3:30 4:40 | no | 2 -> 2 | reference |
| E | E-a-false | spark | v3-cow | answered | 1:10 2:20 3:30 4:40 | no | 2 -> 2 | reference |
| E | E-c-one-eq-zero | spark | v2-cow | answered | 1:10 2:20 3:30 4:40 | no | 2 -> 2 | reference |
| E | E-c-one-eq-zero | spark | v2-mor | answered | 1:10 2:20 3:30 4:40 | no | 2 -> 2 | reference |
| E | E-c-one-eq-zero | spark | v3-cow | answered | 1:10 2:20 3:30 4:40 | no | 2 -> 2 | reference |
| E | E-selective-id-minus-1 | spark | v2-cow | answered | 1:10 2:20 3:30 4:40 | yes | 2 -> 2 | reference |
| E | E-selective-id-minus-1 | spark | v2-mor | answered | 1:10 2:20 3:30 4:40 | yes | 2 -> 2 | reference |
| E | E-selective-id-minus-1 | spark | v3-cow | answered | 1:10 2:20 3:30 4:40 | yes | 2 -> 2 | reference |
| R | R1-ctas-unqualified | spark | - | refused AnalysisException: [UNRESOLVABLE_TABLE_VALUED_FUNCTION] Could not resolve `generate_series` to a table-valued function. | unread: [TABLE_OR_VIEW_NOT_FOUND] The table or view `offset_nested_sort_1_native | - | - | reference |
| R | R2-bracketed-delete | spark | - | refused ParseException: [PARSE_SYNTAX_ERROR] Syntax error at or near '['. SQLSTATE: 42601 (line 1, pos 12) == SQL == DELETE  | 1:10 2:20 3:30 4:40 | - | - | reference |
| R | R3-kernel-by-name | spark | - | refused AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve routine `__repark_cast_timestamp_ns__` on search path [`system`. | - | - | - | reference |

## Counts by label, group and leg/door

### Group W

| label | base/facade | base/native | proto/facade | proto/native |
| --- | --- | --- | --- | --- |
| EQUAL | 15 | 0 | 15 | 5 |
| ROWS-DIFFER | 0 | 0 | 0 | 0 |
| EXTRA-COMMIT | 0 | 0 | 0 | 0 |
| MISSING-COMMIT | 3 | 0 | 3 | 1 |
| BOTH-REFUSED | 0 | 0 | 0 | 0 |
| REFUSED-WHERE-SPARK-ACCEPTED | 0 | 6 | 0 | 0 |
| ACCEPTED-WHERE-SPARK-REFUSED | 0 | 0 | 0 | 0 |
| SETUP-MISSING | 0 | 12 | 0 | 12 |
| ORACLE-MISSING | 0 | 0 | 0 | 0 |

### Group E

| label | base/facade | base/native | proto/facade | proto/native |
| --- | --- | --- | --- | --- |
| EQUAL | 9 | 3 | 9 | 3 |
| ROWS-DIFFER | 0 | 0 | 0 | 0 |
| EXTRA-COMMIT | 3 | 0 | 3 | 1 |
| MISSING-COMMIT | 0 | 1 | 0 | 0 |
| BOTH-REFUSED | 0 | 0 | 0 | 0 |
| REFUSED-WHERE-SPARK-ACCEPTED | 0 | 0 | 0 | 0 |
| ACCEPTED-WHERE-SPARK-REFUSED | 0 | 0 | 0 | 0 |
| SETUP-MISSING | 0 | 8 | 0 | 8 |
| ORACLE-MISSING | 0 | 0 | 0 | 0 |

### Group R

| label | base/native | proto/native |
| --- | --- | --- |
| EQUAL | 0 | 0 |
| ROWS-DIFFER | 0 | 0 |
| EXTRA-COMMIT | 0 | 0 |
| MISSING-COMMIT | 0 | 0 |
| BOTH-REFUSED | 2 | 2 |
| REFUSED-WHERE-SPARK-ACCEPTED | 0 | 0 |
| ACCEPTED-WHERE-SPARK-REFUSED | 1 | 1 |
| SETUP-MISSING | 0 | 0 |
| ORACLE-MISSING | 0 | 0 |

