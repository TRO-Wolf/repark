# NATIVE-CTAS-ICEBERG-REFUSE-1 step 0 on main a3f8c7d9

## `native-door.jsonl` (380 cells)

| group | target | form | shape | outcome | second statement | before | after |
|---|---|---|---|---|---|---|---|
| iceberg-ctas | ice | ctas | literal | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | ctas | plain | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | ctas | union | answered |  | - | rows [], 0 snap, 00000-1dfd4c |
| iceberg-ctas | ice | ctas | union_all | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | ctas | distinct | answered |  | - | rows [], 0 snap, 00000-d3e4b7 |
| iceberg-ctas | ice | ctas | intersect | answered |  | - | rows [], 0 snap, 00000-5aa9e1 |
| iceberg-ctas | ice | ctas | except | answered |  | - | rows [], 0 snap, 00000-791c83 |
| iceberg-ctas | ice | ctas | values | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | ctas | aggregate | answered |  | - | rows [], 0 snap, 00000-056fdf |
| iceberg-ctas | ice | ctas | join | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | ctas_lower | literal | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | ctas_lower | plain | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | ctas_lower | union | answered |  | - | rows [], 0 snap, 00000-715b31 |
| iceberg-ctas | ice | ctas_lower | union_all | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | ctas_lower | distinct | answered |  | - | rows [], 0 snap, 00000-6dbfa8 |
| iceberg-ctas | ice | ctas_lower | intersect | answered |  | - | rows [], 0 snap, 00000-55ab89 |
| iceberg-ctas | ice | ctas_lower | except | answered |  | - | rows [], 0 snap, 00000-14b7a7 |
| iceberg-ctas | ice | ctas_lower | values | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | ctas_lower | aggregate | answered |  | - | rows [], 0 snap, 00000-919071 |
| iceberg-ctas | ice | ctas_lower | join | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | ctas_mixed | literal | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | ctas_mixed | plain | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | ctas_mixed | union | answered |  | - | rows [], 0 snap, 00000-1eb618 |
| iceberg-ctas | ice | ctas_mixed | union_all | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | ctas_mixed | distinct | answered |  | - | rows [], 0 snap, 00000-602f87 |
| iceberg-ctas | ice | ctas_mixed | intersect | answered |  | - | rows [], 0 snap, 00000-7c8152 |
| iceberg-ctas | ice | ctas_mixed | except | answered |  | - | rows [], 0 snap, 00000-5065e4 |
| iceberg-ctas | ice | ctas_mixed | values | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | ctas_mixed | aggregate | answered |  | - | rows [], 0 snap, 00000-414452 |
| iceberg-ctas | ice | ctas_mixed | join | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | if_not_exists | literal | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | if_not_exists | plain | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | if_not_exists | union | answered |  | - | rows [], 0 snap, 00000-6c112e |
| iceberg-ctas | ice | if_not_exists | union_all | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | if_not_exists | distinct | answered |  | - | rows [], 0 snap, 00000-656cc1 |
| iceberg-ctas | ice | if_not_exists | intersect | answered |  | - | rows [], 0 snap, 00000-fb3ac9 |
| iceberg-ctas | ice | if_not_exists | except | answered |  | - | rows [], 0 snap, 00000-67fd8b |
| iceberg-ctas | ice | if_not_exists | values | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | if_not_exists | aggregate | answered |  | - | rows [], 0 snap, 00000-fe5279 |
| iceberg-ctas | ice | if_not_exists | join | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | or_replace | literal | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | or_replace | plain | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | or_replace | union | answered |  | - | rows [], 0 snap, 00000-4b47de |
| iceberg-ctas | ice | or_replace | union_all | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | or_replace | distinct | answered |  | - | rows [], 0 snap, 00000-0213ea |
| iceberg-ctas | ice | or_replace | intersect | answered |  | - | rows [], 0 snap, 00000-9a585a |
| iceberg-ctas | ice | or_replace | except | answered |  | - | rows [], 0 snap, 00000-a3105b |
| iceberg-ctas | ice | or_replace | values | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | or_replace | aggregate | answered |  | - | rows [], 0 snap, 00000-16baa5 |
| iceberg-ctas | ice | or_replace | join | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | columns | literal | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | columns | plain | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | columns | union | answered |  | - | rows [], 0 snap, 00000-f6eb11 |
| iceberg-ctas | ice | columns | union_all | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | columns | distinct | answered |  | - | rows [], 0 snap, 00000-44d9fa |
| iceberg-ctas | ice | columns | intersect | answered |  | - | rows [], 0 snap, 00000-bae8d1 |
| iceberg-ctas | ice | columns | except | answered |  | - | rows [], 0 snap, 00000-51c19d |
| iceberg-ctas | ice | columns | values | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | columns | aggregate | answered |  | - | rows [], 0 snap, 00000-6df672 |
| iceberg-ctas | ice | columns | join | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | replace | literal | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | ice | replace | plain | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | ice | replace | union | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | ice | replace | union_all | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | ice | replace | distinct | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | ice | replace | intersect | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | ice | replace | except | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | ice | replace | values | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | ice | replace | aggregate | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | ice | replace | join | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | ice | temporary | literal | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | ice | temporary | plain | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | ice | temporary | union | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | ice | temporary | union_all | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | ice | temporary | distinct | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | ice | temporary | intersect | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | ice | temporary | except | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | ice | temporary | values | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | ice | temporary | aggregate | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | ice | temporary | join | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | ice | multi_statement | literal | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | ice | multi_statement | plain | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | ice | multi_statement | union | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | ice | multi_statement | union_all | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | ice | multi_statement | distinct | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | ice | multi_statement | intersect | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | ice | multi_statement | except | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | ice | multi_statement | values | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | ice | multi_statement | aggregate | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | ice | multi_statement | join | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | ice | explain | literal | answered |  | - | no table |
| iceberg-ctas | ice | explain | plain | answered |  | - | no table |
| iceberg-ctas | ice | explain | union | answered |  | - | no table |
| iceberg-ctas | ice | explain | union_all | answered |  | - | no table |
| iceberg-ctas | ice | explain | distinct | answered |  | - | no table |
| iceberg-ctas | ice | explain | intersect | answered |  | - | no table |
| iceberg-ctas | ice | explain | except | answered |  | - | no table |
| iceberg-ctas | ice | explain | values | answered |  | - | no table |
| iceberg-ctas | ice | explain | aggregate | answered |  | - | no table |
| iceberg-ctas | ice | explain | join | answered |  | - | no table |
| iceberg-ctas | ice | explain_analyze | literal | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | ice | explain_analyze | plain | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | ice | explain_analyze | union | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | ice | explain_analyze | union_all | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | ice | explain_analyze | distinct | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | ice | explain_analyze | intersect | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | ice | explain_analyze | except | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | ice | explain_analyze | values | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | ice | explain_analyze | aggregate | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | ice | explain_analyze | join | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | ice | prepare | literal | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | ice | prepare | plain | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | ice | prepare | union | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | ice | prepare | union_all | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | ice | prepare | distinct | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | ice | prepare | intersect | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | ice | prepare | except | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | ice | prepare | values | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | ice | prepare | aggregate | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | ice | prepare | join | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | hd | ctas | literal | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | ctas | plain | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | ctas | union | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | union_all | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | ctas | distinct | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | intersect | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | except | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | values | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | ctas | aggregate | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | join | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | ctas_lower | literal | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | ctas_lower | plain | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | ctas_lower | union | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | union_all | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | ctas_lower | distinct | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | intersect | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | except | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | values | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | ctas_lower | aggregate | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | join | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | ctas_mixed | literal | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | ctas_mixed | plain | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | ctas_mixed | union | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_mixed | union_all | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | ctas_mixed | distinct | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_mixed | intersect | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_mixed | except | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_mixed | values | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | ctas_mixed | aggregate | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_mixed | join | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | if_not_exists | literal | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | if_not_exists | plain | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | if_not_exists | union | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | union_all | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | if_not_exists | distinct | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | intersect | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | except | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | values | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | if_not_exists | aggregate | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | join | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | or_replace | literal | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | or_replace | plain | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | or_replace | union | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | union_all | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | or_replace | distinct | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | intersect | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | except | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | values | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | or_replace | aggregate | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | join | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | columns | literal | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | columns | plain | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | columns | union | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | columns | union_all | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | columns | distinct | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | columns | intersect | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | columns | except | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | columns | values | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | columns | aggregate | answered |  | - | rows [], 0 snap, v1.metadata. |
| iceberg-ctas | hd | columns | join | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | replace | literal | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | hd | replace | plain | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | hd | replace | union | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | hd | replace | union_all | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | hd | replace | distinct | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | hd | replace | intersect | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | hd | replace | except | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | hd | replace | values | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | hd | replace | aggregate | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | hd | replace | join | refused ParseException: SQL error: ParserError("Expected: SELECT, VALUES, or a subquery in the query body, found: AS at Line: 1, … |  | - | no table |
| iceberg-ctas | hd | temporary | literal | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | hd | temporary | plain | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | hd | temporary | union | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | hd | temporary | union_all | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | hd | temporary | distinct | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | hd | temporary | intersect | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | hd | temporary | except | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | hd | temporary | values | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | hd | temporary | aggregate | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | hd | temporary | join | refused UnsupportedOperationException: This feature is not implemented: Temporary tables not supported |  | - | no table |
| iceberg-ctas | hd | multi_statement | literal | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | hd | multi_statement | plain | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | hd | multi_statement | union | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | hd | multi_statement | union_all | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | hd | multi_statement | distinct | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | hd | multi_statement | intersect | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | hd | multi_statement | except | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | hd | multi_statement | values | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | hd | multi_statement | aggregate | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | hd | multi_statement | join | refused UnsupportedOperationException: This feature is not implemented: The context currently only supports a single SQL statement |  | - | no table |
| iceberg-ctas | hd | explain | literal | answered |  | - | no table |
| iceberg-ctas | hd | explain | plain | answered |  | - | no table |
| iceberg-ctas | hd | explain | union | answered |  | - | no table |
| iceberg-ctas | hd | explain | union_all | answered |  | - | no table |
| iceberg-ctas | hd | explain | distinct | answered |  | - | no table |
| iceberg-ctas | hd | explain | intersect | answered |  | - | no table |
| iceberg-ctas | hd | explain | except | answered |  | - | no table |
| iceberg-ctas | hd | explain | values | answered |  | - | no table |
| iceberg-ctas | hd | explain | aggregate | answered |  | - | no table |
| iceberg-ctas | hd | explain | join | answered |  | - | no table |
| iceberg-ctas | hd | explain_analyze | literal | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | hd | explain_analyze | plain | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | hd | explain_analyze | union | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | hd | explain_analyze | union_all | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | hd | explain_analyze | distinct | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | hd | explain_analyze | intersect | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | hd | explain_analyze | except | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | hd | explain_analyze | values | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | hd | explain_analyze | aggregate | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | hd | explain_analyze | join | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable |  | - | no table |
| iceberg-ctas | hd | prepare | literal | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | hd | prepare | plain | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | hd | prepare | union | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | hd | prepare | union_all | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | hd | prepare | distinct | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | hd | prepare | intersect | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | hd | prepare | except | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | hd | prepare | values | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | hd | prepare | aggregate | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | hd | prepare | join | answered | refused UnsupportedOperationException: This feature is not implemented: Unsupported logical plan: CreateMemoryTable | - | no table |
| iceberg-ctas | ice | mixed_case_target | literal | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | mixed_case_target | distinct | answered |  | - | rows [], 0 snap, 00000-50dff5 |
| iceberg-ctas | ice | external | - | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | external | - | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | default_catalog_unqualified | literal | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | default_catalog_unqualified | distinct | answered |  | - | rows [], 0 snap, 00000-be66bc |
| workaround | ice | create_then_insert | literal | answered | answered | - | rows [[1]], 1 snap, 00001-4040be |
| workaround | ice | create_then_insert | plain | answered | answered | - | rows [[1], [1], [2], [3]], 1 snap, 00001-a06f9d |
| workaround | ice | create_then_insert | union | answered | answered | - | rows [[1], [2], [3], [4]], 1 snap, 00001-8a03ef |
| workaround | ice | create_then_insert | union_all | answered | answered | - | rows [[1], [1], [2], [2], [3], [3], [4]], 1 snap, 00001-d61148 |
| workaround | ice | create_then_insert | distinct | answered | answered | - | rows [[1], [2], [3]], 1 snap, 00001-baf66e |
| workaround | ice | create_then_insert | intersect | answered | answered | - | rows [[2], [3]], 1 snap, 00001-58b3ff |
| workaround | ice | create_then_insert | except | answered | answered | - | rows [[1]], 1 snap, 00001-adb837 |
| workaround | ice | create_then_insert | values | answered | answered | - | rows [[1], [2]], 1 snap, 00001-de43d2 |
| workaround | ice | create_then_insert | aggregate | answered | answered | - | rows [[1, 2], [2, 1], [3, 1]], 1 snap, 00001-1ffa51 |
| workaround | ice | create_then_insert | join | answered | answered | - | rows [[2], [3]], 1 snap, 00001-5624cf |
| workaround | hd | create_then_insert | literal | answered | answered | - | rows [[1]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | plain | answered | answered | - | rows [[1], [1], [2], [3]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | union | answered | answered | - | rows [[1], [2], [3], [4]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | union_all | answered | answered | - | rows [[1], [1], [2], [2], [3], [3], [4]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | distinct | answered | answered | - | rows [[1], [2], [3]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | intersect | answered | answered | - | rows [[2], [3]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | except | answered | answered | - | rows [[1]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | values | answered | answered | - | rows [[1], [2]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | aggregate | answered | answered | - | rows [[1, 2], [2, 1], [3, 1]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | join | answered | answered | - | rows [[2], [3]], 1 snap, v2.metadata. |
| existing | ice | or_replace_failing | - | refused PySparkException: datafusion engine error: Arrow error: Divide by zero error |  | rows [[7], [8]], 1 snap, 00001-81b298 | unreadable, 1 metadata file(s) |
| existing | ice | or_replace_succeeding | - | refused PySparkException: Unexpected => register_table does not support tables with data. |  | rows [[7], [8]], 1 snap, 00001-45d29e | unreadable, 1 metadata file(s) |
| existing | ice | if_not_exists_existing | - | answered |  | rows [[7], [8]], 1 snap, 00001-09b52a | rows [[7], [8]], 1 snap, 00001-09b52a |
| existing | ice | ctas_existing | - | refused PySparkException: datafusion engine error: Execution error: Table 'ice.ns.e270' already exists |  | rows [[7], [8]], 1 snap, 00001-e64b72 | rows [[7], [8]], 1 snap, 00001-e64b72 |
| existing | hd | or_replace_failing | - | refused PySparkException: datafusion engine error: Arrow error: Divide by zero error |  | rows [[7], [8]], 1 snap, v2.metadata. | no table |
| existing | hd | or_replace_succeeding | - | refused PySparkException: Unexpected => register_table does not support tables with data. |  | rows [[7], [8]], 1 snap, v2.metadata. | no table |
| existing | hd | if_not_exists_existing | - | answered |  | rows [[7], [8]], 1 snap, v2.metadata. | rows [[7], [8]], 1 snap, v2.metadata. |
| existing | hd | ctas_existing | - | refused PySparkException: datafusion engine error: Execution error: Table 'hd.ns.e274' already exists |  | rows [[7], [8]], 1 snap, v2.metadata. | rows [[7], [8]], 1 snap, v2.metadata. |
| control | ice | create_no_query | - | answered |  | - | rows [], 0 snap, 00000-5cda84 |
| control | ice | insert_select | union | answered |  | rows [], 0 snap, 00000-3a22eb | rows [[1], [2], [3], [4]], 1 snap, 00001-03dc15 |
| control | ice | create_view | distinct | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| control | hd | create_no_query | - | answered |  | - | rows [], 0 snap, v1.metadata. |
| control | hd | insert_select | union | answered |  | rows [], 0 snap, v1.metadata. | rows [[1], [2], [3], [4]], 1 snap, v2.metadata. |
| control | memory-bare | create_view | distinct | answered |  | - | rows [[1], [2], [3]] |
| memory-ctas | memory-bare | ctas | literal | answered |  | - | rows [[1]] |
| memory-ctas | memory-bare | ctas | plain | answered |  | - | rows [[1], [1], [2], [3]] |
| memory-ctas | memory-bare | ctas | union | answered |  | - | rows [[1], [2], [3], [4]] |
| memory-ctas | memory-bare | ctas | union_all | answered |  | - | rows [[1], [1], [2], [2], [3], [3], [4]] |
| memory-ctas | memory-bare | ctas | distinct | answered |  | - | rows [[1], [2], [3]] |
| memory-ctas | memory-bare | ctas | intersect | answered |  | - | rows [[2], [3]] |
| memory-ctas | memory-bare | ctas | except | answered |  | - | rows [[1]] |
| memory-ctas | memory-bare | ctas | values | answered |  | - | rows [[1], [2]] |
| memory-ctas | memory-bare | ctas | aggregate | answered |  | - | rows [[1, 2], [2, 1], [3, 1]] |
| memory-ctas | memory-bare | ctas | join | answered |  | - | rows [[2], [3]] |
| memory-ctas | memory-bare | if_not_exists | literal | answered |  | - | rows [[1]] |
| memory-ctas | memory-bare | if_not_exists | plain | answered |  | - | rows [[1], [1], [2], [3]] |
| memory-ctas | memory-bare | if_not_exists | union | answered |  | - | rows [[1], [2], [3], [4]] |
| memory-ctas | memory-bare | if_not_exists | union_all | answered |  | - | rows [[1], [1], [2], [2], [3], [3], [4]] |
| memory-ctas | memory-bare | if_not_exists | distinct | answered |  | - | rows [[1], [2], [3]] |
| memory-ctas | memory-bare | if_not_exists | intersect | answered |  | - | rows [[2], [3]] |
| memory-ctas | memory-bare | if_not_exists | except | answered |  | - | rows [[1]] |
| memory-ctas | memory-bare | if_not_exists | values | answered |  | - | rows [[1], [2]] |
| memory-ctas | memory-bare | if_not_exists | aggregate | answered |  | - | rows [[1, 2], [2, 1], [3, 1]] |
| memory-ctas | memory-bare | if_not_exists | join | answered |  | - | rows [[2], [3]] |
| memory-ctas | memory-bare | or_replace | literal | answered |  | - | rows [[1]] |
| memory-ctas | memory-bare | or_replace | plain | answered |  | - | rows [[1], [1], [2], [3]] |
| memory-ctas | memory-bare | or_replace | union | answered |  | - | rows [[1], [2], [3], [4]] |
| memory-ctas | memory-bare | or_replace | union_all | answered |  | - | rows [[1], [1], [2], [2], [3], [3], [4]] |
| memory-ctas | memory-bare | or_replace | distinct | answered |  | - | rows [[1], [2], [3]] |
| memory-ctas | memory-bare | or_replace | intersect | answered |  | - | rows [[2], [3]] |
| memory-ctas | memory-bare | or_replace | except | answered |  | - | rows [[1]] |
| memory-ctas | memory-bare | or_replace | values | answered |  | - | rows [[1], [2]] |
| memory-ctas | memory-bare | or_replace | aggregate | answered |  | - | rows [[1, 2], [2, 1], [3, 1]] |
| memory-ctas | memory-bare | or_replace | join | answered |  | - | rows [[2], [3]] |
| memory-ctas | memory-bare | columns | literal | answered |  | - | rows [[1]] |
| memory-ctas | memory-bare | columns | plain | answered |  | - | rows [[1], [1], [2], [3]] |
| memory-ctas | memory-bare | columns | union | answered |  | - | rows [[1], [2], [3], [4]] |
| memory-ctas | memory-bare | columns | union_all | answered |  | - | rows [[1], [1], [2], [2], [3], [3], [4]] |
| memory-ctas | memory-bare | columns | distinct | answered |  | - | rows [[1], [2], [3]] |
| memory-ctas | memory-bare | columns | intersect | answered |  | - | rows [[2], [3]] |
| memory-ctas | memory-bare | columns | except | answered |  | - | rows [[1]] |
| memory-ctas | memory-bare | columns | values | answered |  | - | rows [[1], [2]] |
| memory-ctas | memory-bare | columns | aggregate | answered |  | - | rows [[1, 2], [2, 1], [3, 1]] |
| memory-ctas | memory-bare | columns | join | answered |  | - | rows [[2], [3]] |
| memory-ctas | memory-bare | ctas_lower | literal | answered |  | - | rows [[1]] |
| memory-ctas | memory-bare | ctas_lower | plain | answered |  | - | rows [[1], [1], [2], [3]] |
| memory-ctas | memory-bare | ctas_lower | union | answered |  | - | rows [[1], [2], [3], [4]] |
| memory-ctas | memory-bare | ctas_lower | union_all | answered |  | - | rows [[1], [1], [2], [2], [3], [3], [4]] |
| memory-ctas | memory-bare | ctas_lower | distinct | answered |  | - | rows [[1], [2], [3]] |
| memory-ctas | memory-bare | ctas_lower | intersect | answered |  | - | rows [[2], [3]] |
| memory-ctas | memory-bare | ctas_lower | except | answered |  | - | rows [[1]] |
| memory-ctas | memory-bare | ctas_lower | values | answered |  | - | rows [[1], [2]] |
| memory-ctas | memory-bare | ctas_lower | aggregate | answered |  | - | rows [[1, 2], [2, 1], [3, 1]] |
| memory-ctas | memory-bare | ctas_lower | join | answered |  | - | rows [[2], [3]] |
| memory-ctas | memory-qualified | ctas | literal | answered |  | - | rows [[1]] |
| memory-ctas | memory-qualified | ctas | plain | answered |  | - | rows [[1], [1], [2], [3]] |
| memory-ctas | memory-qualified | ctas | union | answered |  | - | rows [[1], [2], [3], [4]] |
| memory-ctas | memory-qualified | ctas | union_all | answered |  | - | rows [[1], [1], [2], [2], [3], [3], [4]] |
| memory-ctas | memory-qualified | ctas | distinct | answered |  | - | rows [[1], [2], [3]] |
| memory-ctas | memory-qualified | ctas | intersect | answered |  | - | rows [[2], [3]] |
| memory-ctas | memory-qualified | ctas | except | answered |  | - | rows [[1]] |
| memory-ctas | memory-qualified | ctas | values | answered |  | - | rows [[1], [2]] |
| memory-ctas | memory-qualified | ctas | aggregate | answered |  | - | rows [[1, 2], [2, 1], [3, 1]] |
| memory-ctas | memory-qualified | ctas | join | answered |  | - | rows [[2], [3]] |
| memory-ctas | memory-qualified | if_not_exists | literal | answered |  | - | rows [[1]] |
| memory-ctas | memory-qualified | if_not_exists | plain | answered |  | - | rows [[1], [1], [2], [3]] |
| memory-ctas | memory-qualified | if_not_exists | union | answered |  | - | rows [[1], [2], [3], [4]] |
| memory-ctas | memory-qualified | if_not_exists | union_all | answered |  | - | rows [[1], [1], [2], [2], [3], [3], [4]] |
| memory-ctas | memory-qualified | if_not_exists | distinct | answered |  | - | rows [[1], [2], [3]] |
| memory-ctas | memory-qualified | if_not_exists | intersect | answered |  | - | rows [[2], [3]] |
| memory-ctas | memory-qualified | if_not_exists | except | answered |  | - | rows [[1]] |
| memory-ctas | memory-qualified | if_not_exists | values | answered |  | - | rows [[1], [2]] |
| memory-ctas | memory-qualified | if_not_exists | aggregate | answered |  | - | rows [[1, 2], [2, 1], [3, 1]] |
| memory-ctas | memory-qualified | if_not_exists | join | answered |  | - | rows [[2], [3]] |
| memory-ctas | memory-qualified | or_replace | literal | answered |  | - | rows [[1]] |
| memory-ctas | memory-qualified | or_replace | plain | answered |  | - | rows [[1], [1], [2], [3]] |
| memory-ctas | memory-qualified | or_replace | union | answered |  | - | rows [[1], [2], [3], [4]] |
| memory-ctas | memory-qualified | or_replace | union_all | answered |  | - | rows [[1], [1], [2], [2], [3], [3], [4]] |
| memory-ctas | memory-qualified | or_replace | distinct | answered |  | - | rows [[1], [2], [3]] |
| memory-ctas | memory-qualified | or_replace | intersect | answered |  | - | rows [[2], [3]] |
| memory-ctas | memory-qualified | or_replace | except | answered |  | - | rows [[1]] |
| memory-ctas | memory-qualified | or_replace | values | answered |  | - | rows [[1], [2]] |
| memory-ctas | memory-qualified | or_replace | aggregate | answered |  | - | rows [[1, 2], [2, 1], [3, 1]] |
| memory-ctas | memory-qualified | or_replace | join | answered |  | - | rows [[2], [3]] |
| memory-ctas | memory-qualified | columns | literal | answered |  | - | rows [[1]] |
| memory-ctas | memory-qualified | columns | plain | answered |  | - | rows [[1], [1], [2], [3]] |
| memory-ctas | memory-qualified | columns | union | answered |  | - | rows [[1], [2], [3], [4]] |
| memory-ctas | memory-qualified | columns | union_all | answered |  | - | rows [[1], [1], [2], [2], [3], [3], [4]] |
| memory-ctas | memory-qualified | columns | distinct | answered |  | - | rows [[1], [2], [3]] |
| memory-ctas | memory-qualified | columns | intersect | answered |  | - | rows [[2], [3]] |
| memory-ctas | memory-qualified | columns | except | answered |  | - | rows [[1]] |
| memory-ctas | memory-qualified | columns | values | answered |  | - | rows [[1], [2]] |
| memory-ctas | memory-qualified | columns | aggregate | answered |  | - | rows [[1, 2], [2, 1], [3, 1]] |
| memory-ctas | memory-qualified | columns | join | answered |  | - | rows [[2], [3]] |
| memory-ctas | memory-qualified | ctas_lower | literal | answered |  | - | rows [[1]] |
| memory-ctas | memory-qualified | ctas_lower | plain | answered |  | - | rows [[1], [1], [2], [3]] |
| memory-ctas | memory-qualified | ctas_lower | union | answered |  | - | rows [[1], [2], [3], [4]] |
| memory-ctas | memory-qualified | ctas_lower | union_all | answered |  | - | rows [[1], [1], [2], [2], [3], [3], [4]] |
| memory-ctas | memory-qualified | ctas_lower | distinct | answered |  | - | rows [[1], [2], [3]] |
| memory-ctas | memory-qualified | ctas_lower | intersect | answered |  | - | rows [[2], [3]] |
| memory-ctas | memory-qualified | ctas_lower | except | answered |  | - | rows [[1]] |
| memory-ctas | memory-qualified | ctas_lower | values | answered |  | - | rows [[1], [2]] |
| memory-ctas | memory-qualified | ctas_lower | aggregate | answered |  | - | rows [[1, 2], [2, 1], [3, 1]] |
| memory-ctas | memory-qualified | ctas_lower | join | answered |  | - | rows [[2], [3]] |

## `spark-door.jsonl` (66 cells)

| group | target | form | shape | outcome | second statement | before | after |
|---|---|---|---|---|---|---|---|
| iceberg-ctas | hd | ctas | literal | answered |  | - | rows [[1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | plain | answered |  | - | rows [[1], [1], [2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | union | answered |  | - | rows [[1], [2], [3], [4]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | union_all | answered |  | - | rows [[1], [1], [2], [2], [3], [3], [4]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | distinct | answered |  | - | rows [[1], [2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | intersect | answered |  | - | rows [[2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | except | answered |  | - | rows [[1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | values | answered |  | - | rows [[1], [2]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | aggregate | answered |  | - | rows [[1, 2], [2, 1], [3, 1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | join | answered |  | - | rows [[2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | literal | answered |  | - | rows [[1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | plain | answered |  | - | rows [[1], [1], [2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | union | answered |  | - | rows [[1], [2], [3], [4]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | union_all | answered |  | - | rows [[1], [1], [2], [2], [3], [3], [4]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | distinct | answered |  | - | rows [[1], [2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | intersect | answered |  | - | rows [[2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | except | answered |  | - | rows [[1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | values | answered |  | - | rows [[1], [2]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | aggregate | answered |  | - | rows [[1, 2], [2, 1], [3, 1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | join | answered |  | - | rows [[2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | literal | answered |  | - | rows [[1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | plain | answered |  | - | rows [[1], [1], [2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | union | answered |  | - | rows [[1], [2], [3], [4]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | union_all | answered |  | - | rows [[1], [1], [2], [2], [3], [3], [4]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | distinct | answered |  | - | rows [[1], [2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | intersect | answered |  | - | rows [[2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | except | answered |  | - | rows [[1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | values | answered |  | - | rows [[1], [2]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | aggregate | answered |  | - | rows [[1, 2], [2, 1], [3, 1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | join | answered |  | - | rows [[2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | literal | answered |  | - | rows [[1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | plain | answered |  | - | rows [[1], [1], [2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | union | answered |  | - | rows [[1], [2], [3], [4]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | union_all | answered |  | - | rows [[1], [1], [2], [2], [3], [3], [4]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | distinct | answered |  | - | rows [[1], [2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | intersect | answered |  | - | rows [[2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | except | answered |  | - | rows [[1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | values | answered |  | - | rows [[1], [2]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | aggregate | answered |  | - | rows [[1, 2], [2, 1], [3, 1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | join | answered |  | - | rows [[2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | replace | literal | refused AnalysisException: Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `hd`.`ns`.`t41` cannot be found. Verify … |  | - | no table |
| iceberg-ctas | hd | replace | plain | refused AnalysisException: Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `hd`.`ns`.`t42` cannot be found. Verify … |  | - | no table |
| iceberg-ctas | hd | replace | union | refused AnalysisException: Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `hd`.`ns`.`t43` cannot be found. Verify … |  | - | no table |
| iceberg-ctas | hd | replace | union_all | refused AnalysisException: Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `hd`.`ns`.`t44` cannot be found. Verify … |  | - | no table |
| iceberg-ctas | hd | replace | distinct | refused AnalysisException: Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `hd`.`ns`.`t45` cannot be found. Verify … |  | - | no table |
| iceberg-ctas | hd | replace | intersect | refused AnalysisException: Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `hd`.`ns`.`t46` cannot be found. Verify … |  | - | no table |
| iceberg-ctas | hd | replace | except | refused AnalysisException: Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `hd`.`ns`.`t47` cannot be found. Verify … |  | - | no table |
| iceberg-ctas | hd | replace | values | refused AnalysisException: Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `hd`.`ns`.`t48` cannot be found. Verify … |  | - | no table |
| iceberg-ctas | hd | replace | aggregate | refused AnalysisException: Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `hd`.`ns`.`t49` cannot be found. Verify … |  | - | no table |
| iceberg-ctas | hd | replace | join | refused AnalysisException: Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `hd`.`ns`.`t50` cannot be found. Verify … |  | - | no table |
| workaround | hd | create_then_insert | literal | answered | answered | - | rows [[1]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | plain | answered | answered | - | rows [[1], [1], [2], [3]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | union | answered | answered | - | rows [[1], [2], [3], [4]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | union_all | answered | answered | - | rows [[1], [1], [2], [2], [3], [3], [4]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | distinct | answered | answered | - | rows [[1], [2], [3]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | intersect | answered | answered | - | rows [[2], [3]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | except | answered | answered | - | rows [[1]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | values | answered | answered | - | rows [[1], [2]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | aggregate | answered | answered | - | rows [[1, 2], [2, 1], [3, 1]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | join | answered | answered | - | rows [[2], [3]], 1 snap, v2.metadata. |
| existing | hd | or_replace_failing | - | refused PySparkException: datafusion engine error: Execution error: [DIVIDE_BY_ZERO] Division by zero. Use try_divide to tolerate … |  | rows [[7], [8]], 1 snap, v2.metadata. | rows [[7], [8]], 1 snap, v2.metadata. |
| existing | hd | or_replace_succeeding | - | answered |  | rows [[7], [8]], 1 snap, v2.metadata. | rows [[1], [1], [2], [3]], 2 snap, v3.metadata. |
| existing | hd | if_not_exists_existing | - | answered |  | rows [[7], [8]], 1 snap, v2.metadata. | rows [[7], [8]], 1 snap, v2.metadata. |
| existing | hd | ctas_existing | - | refused AnalysisException: Error during planning: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `hd`.`ns`.`e64` because it … |  | rows [[7], [8]], 1 snap, v2.metadata. | rows [[7], [8]], 1 snap, v2.metadata. |
| control | hd | create_no_query | - | answered |  | - | rows [], 0 snap, v1.metadata. |
| control | hd | insert_select | union | answered |  | rows [], 0 snap, v1.metadata. | rows [[1], [2], [3], [4]], 1 snap, v2.metadata. |

## `spark.jsonl` (66 cells)

| group | target | form | shape | outcome | second statement | before | after |
|---|---|---|---|---|---|---|---|
| iceberg-ctas | hd | ctas | literal | answered |  | - | rows [[1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | plain | answered |  | - | rows [[1], [1], [2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | union | answered |  | - | rows [[1], [2], [3], [4]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | union_all | answered |  | - | rows [[1], [1], [2], [2], [3], [3], [4]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | distinct | answered |  | - | rows [[1], [2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | intersect | answered |  | - | rows [[2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | except | answered |  | - | rows [[1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | values | answered |  | - | rows [[1], [2]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | aggregate | answered |  | - | rows [[1, 2], [2, 1], [3, 1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas | join | answered |  | - | rows [[2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | literal | answered |  | - | rows [[1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | plain | answered |  | - | rows [[1], [1], [2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | union | answered |  | - | rows [[1], [2], [3], [4]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | union_all | answered |  | - | rows [[1], [1], [2], [2], [3], [3], [4]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | distinct | answered |  | - | rows [[1], [2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | intersect | answered |  | - | rows [[2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | except | answered |  | - | rows [[1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | values | answered |  | - | rows [[1], [2]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | aggregate | answered |  | - | rows [[1, 2], [2, 1], [3, 1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | ctas_lower | join | answered |  | - | rows [[2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | literal | answered |  | - | rows [[1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | plain | answered |  | - | rows [[1], [1], [2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | union | answered |  | - | rows [[1], [2], [3], [4]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | union_all | answered |  | - | rows [[1], [1], [2], [2], [3], [3], [4]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | distinct | answered |  | - | rows [[1], [2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | intersect | answered |  | - | rows [[2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | except | answered |  | - | rows [[1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | values | answered |  | - | rows [[1], [2]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | aggregate | answered |  | - | rows [[1, 2], [2, 1], [3, 1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | if_not_exists | join | answered |  | - | rows [[2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | literal | answered |  | - | rows [[1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | plain | answered |  | - | rows [[1], [1], [2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | union | answered |  | - | rows [[1], [2], [3], [4]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | union_all | answered |  | - | rows [[1], [1], [2], [2], [3], [3], [4]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | distinct | answered |  | - | rows [[1], [2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | intersect | answered |  | - | rows [[2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | except | answered |  | - | rows [[1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | values | answered |  | - | rows [[1], [2]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | aggregate | answered |  | - | rows [[1, 2], [2, 1], [3, 1]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | or_replace | join | answered |  | - | rows [[2], [3]], 1 snap, v1.metadata. |
| iceberg-ctas | hd | replace | literal | refused AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `ns`.`t41` cannot be found. Verify the spelling and correctness … |  | - | no table |
| iceberg-ctas | hd | replace | plain | refused AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `ns`.`t42` cannot be found. Verify the spelling and correctness … |  | - | no table |
| iceberg-ctas | hd | replace | union | refused AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `ns`.`t43` cannot be found. Verify the spelling and correctness … |  | - | no table |
| iceberg-ctas | hd | replace | union_all | refused AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `ns`.`t44` cannot be found. Verify the spelling and correctness … |  | - | no table |
| iceberg-ctas | hd | replace | distinct | refused AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `ns`.`t45` cannot be found. Verify the spelling and correctness … |  | - | no table |
| iceberg-ctas | hd | replace | intersect | refused AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `ns`.`t46` cannot be found. Verify the spelling and correctness … |  | - | no table |
| iceberg-ctas | hd | replace | except | refused AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `ns`.`t47` cannot be found. Verify the spelling and correctness … |  | - | no table |
| iceberg-ctas | hd | replace | values | refused AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `ns`.`t48` cannot be found. Verify the spelling and correctness … |  | - | no table |
| iceberg-ctas | hd | replace | aggregate | refused AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `ns`.`t49` cannot be found. Verify the spelling and correctness … |  | - | no table |
| iceberg-ctas | hd | replace | join | refused AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `ns`.`t50` cannot be found. Verify the spelling and correctness … |  | - | no table |
| workaround | hd | create_then_insert | literal | answered | answered | - | rows [[1]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | plain | answered | answered | - | rows [[1], [1], [2], [3]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | union | answered | answered | - | rows [[1], [2], [3], [4]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | union_all | answered | answered | - | rows [[1], [1], [2], [2], [3], [3], [4]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | distinct | answered | answered | - | rows [[1], [2], [3]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | intersect | answered | answered | - | rows [[2], [3]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | except | answered | answered | - | rows [[1]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | values | answered | answered | - | rows [[1], [2]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | aggregate | answered | answered | - | rows [[1, 2], [2, 1], [3, 1]], 1 snap, v2.metadata. |
| workaround | hd | create_then_insert | join | answered | answered | - | rows [[2], [3]], 1 snap, v2.metadata. |
| existing | hd | or_replace_failing | - | refused ArithmeticException: [DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead. If … |  | rows [[7], [8]], 1 snap, v2.metadata. | rows [[7], [8]], 1 snap, v2.metadata. |
| existing | hd | or_replace_succeeding | - | answered |  | rows [[7], [8]], 1 snap, v2.metadata. | rows [[1], [1], [2], [3]], 2 snap, v3.metadata. |
| existing | hd | if_not_exists_existing | - | answered |  | rows [[7], [8]], 1 snap, v2.metadata. | rows [[7], [8]], 1 snap, v2.metadata. |
| existing | hd | ctas_existing | - | refused AnalysisException: [TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `ns`.`e64` because it already exists. Choose a … |  | rows [[7], [8]], 1 snap, v2.metadata. | rows [[7], [8]], 1 snap, v2.metadata. |
| control | hd | create_no_query | - | answered |  | - | rows [], 0 snap, v1.metadata. |
| control | hd | insert_select | union | answered |  | rows [], 0 snap, v1.metadata. | rows [[1], [2], [3], [4]], 1 snap, v2.metadata. |

