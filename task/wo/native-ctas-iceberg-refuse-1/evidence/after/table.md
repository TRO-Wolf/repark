# NATIVE-CTAS-ICEBERG-REFUSE-1 after, head of fix/native-ctas-iceberg-refuse-1

## `native-door.jsonl` (380 cells)

| group | target | form | shape | outcome | second statement | before | after |
|---|---|---|---|---|---|---|---|
| iceberg-ctas | ice | ctas | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t1: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas | plain | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t2: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas | union | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t3: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas | union_all | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t4: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t5: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas | intersect | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t6: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas | except | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t7: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas | values | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t8: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas | aggregate | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t9: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas | join | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t10: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_lower | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t11: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_lower | plain | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t12: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_lower | union | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t13: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_lower | union_all | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t14: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_lower | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t15: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_lower | intersect | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t16: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_lower | except | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t17: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_lower | values | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t18: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_lower | aggregate | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t19: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_lower | join | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t20: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_mixed | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t21: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_mixed | plain | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t22: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_mixed | union | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t23: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_mixed | union_all | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t24: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_mixed | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t25: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_mixed | intersect | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t26: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_mixed | except | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t27: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_mixed | values | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t28: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_mixed | aggregate | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t29: create the table … |  | - | no table |
| iceberg-ctas | ice | ctas_mixed | join | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t30: create the table … |  | - | no table |
| iceberg-ctas | ice | if_not_exists | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t31: create the table … |  | - | no table |
| iceberg-ctas | ice | if_not_exists | plain | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t32: create the table … |  | - | no table |
| iceberg-ctas | ice | if_not_exists | union | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t33: create the table … |  | - | no table |
| iceberg-ctas | ice | if_not_exists | union_all | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t34: create the table … |  | - | no table |
| iceberg-ctas | ice | if_not_exists | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t35: create the table … |  | - | no table |
| iceberg-ctas | ice | if_not_exists | intersect | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t36: create the table … |  | - | no table |
| iceberg-ctas | ice | if_not_exists | except | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t37: create the table … |  | - | no table |
| iceberg-ctas | ice | if_not_exists | values | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t38: create the table … |  | - | no table |
| iceberg-ctas | ice | if_not_exists | aggregate | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t39: create the table … |  | - | no table |
| iceberg-ctas | ice | if_not_exists | join | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t40: create the table … |  | - | no table |
| iceberg-ctas | ice | or_replace | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t41: create the table … |  | - | no table |
| iceberg-ctas | ice | or_replace | plain | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t42: create the table … |  | - | no table |
| iceberg-ctas | ice | or_replace | union | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t43: create the table … |  | - | no table |
| iceberg-ctas | ice | or_replace | union_all | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t44: create the table … |  | - | no table |
| iceberg-ctas | ice | or_replace | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t45: create the table … |  | - | no table |
| iceberg-ctas | ice | or_replace | intersect | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t46: create the table … |  | - | no table |
| iceberg-ctas | ice | or_replace | except | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t47: create the table … |  | - | no table |
| iceberg-ctas | ice | or_replace | values | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t48: create the table … |  | - | no table |
| iceberg-ctas | ice | or_replace | aggregate | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t49: create the table … |  | - | no table |
| iceberg-ctas | ice | or_replace | join | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t50: create the table … |  | - | no table |
| iceberg-ctas | ice | columns | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t51: create the table … |  | - | no table |
| iceberg-ctas | ice | columns | plain | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t52: create the table … |  | - | no table |
| iceberg-ctas | ice | columns | union | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t53: create the table … |  | - | no table |
| iceberg-ctas | ice | columns | union_all | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t54: create the table … |  | - | no table |
| iceberg-ctas | ice | columns | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t55: create the table … |  | - | no table |
| iceberg-ctas | ice | columns | intersect | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t56: create the table … |  | - | no table |
| iceberg-ctas | ice | columns | except | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t57: create the table … |  | - | no table |
| iceberg-ctas | ice | columns | values | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t58: create the table … |  | - | no table |
| iceberg-ctas | ice | columns | aggregate | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t59: create the table … |  | - | no table |
| iceberg-ctas | ice | columns | join | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t60: create the table … |  | - | no table |
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
| iceberg-ctas | ice | explain | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t91: create the table … |  | - | no table |
| iceberg-ctas | ice | explain | plain | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t92: create the table … |  | - | no table |
| iceberg-ctas | ice | explain | union | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t93: create the table … |  | - | no table |
| iceberg-ctas | ice | explain | union_all | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t94: create the table … |  | - | no table |
| iceberg-ctas | ice | explain | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t95: create the table … |  | - | no table |
| iceberg-ctas | ice | explain | intersect | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t96: create the table … |  | - | no table |
| iceberg-ctas | ice | explain | except | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t97: create the table … |  | - | no table |
| iceberg-ctas | ice | explain | values | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t98: create the table … |  | - | no table |
| iceberg-ctas | ice | explain | aggregate | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t99: create the table … |  | - | no table |
| iceberg-ctas | ice | explain | join | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t100: create the table … |  | - | no table |
| iceberg-ctas | ice | explain_analyze | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t101: create the table … |  | - | no table |
| iceberg-ctas | ice | explain_analyze | plain | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t102: create the table … |  | - | no table |
| iceberg-ctas | ice | explain_analyze | union | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t103: create the table … |  | - | no table |
| iceberg-ctas | ice | explain_analyze | union_all | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t104: create the table … |  | - | no table |
| iceberg-ctas | ice | explain_analyze | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t105: create the table … |  | - | no table |
| iceberg-ctas | ice | explain_analyze | intersect | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t106: create the table … |  | - | no table |
| iceberg-ctas | ice | explain_analyze | except | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t107: create the table … |  | - | no table |
| iceberg-ctas | ice | explain_analyze | values | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t108: create the table … |  | - | no table |
| iceberg-ctas | ice | explain_analyze | aggregate | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t109: create the table … |  | - | no table |
| iceberg-ctas | ice | explain_analyze | join | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t110: create the table … |  | - | no table |
| iceberg-ctas | ice | prepare | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t111: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t111' does not exist | - | no table |
| iceberg-ctas | ice | prepare | plain | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t112: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t112' does not exist | - | no table |
| iceberg-ctas | ice | prepare | union | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t113: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t113' does not exist | - | no table |
| iceberg-ctas | ice | prepare | union_all | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t114: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t114' does not exist | - | no table |
| iceberg-ctas | ice | prepare | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t115: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t115' does not exist | - | no table |
| iceberg-ctas | ice | prepare | intersect | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t116: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t116' does not exist | - | no table |
| iceberg-ctas | ice | prepare | except | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t117: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t117' does not exist | - | no table |
| iceberg-ctas | ice | prepare | values | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t118: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t118' does not exist | - | no table |
| iceberg-ctas | ice | prepare | aggregate | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t119: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t119' does not exist | - | no table |
| iceberg-ctas | ice | prepare | join | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t120: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t120' does not exist | - | no table |
| iceberg-ctas | hd | ctas | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t121: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas | plain | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t122: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas | union | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t123: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas | union_all | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t124: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t125: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas | intersect | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t126: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas | except | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t127: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas | values | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t128: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas | aggregate | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t129: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas | join | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t130: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_lower | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t131: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_lower | plain | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t132: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_lower | union | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t133: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_lower | union_all | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t134: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_lower | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t135: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_lower | intersect | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t136: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_lower | except | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t137: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_lower | values | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t138: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_lower | aggregate | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t139: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_lower | join | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t140: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_mixed | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t141: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_mixed | plain | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t142: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_mixed | union | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t143: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_mixed | union_all | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t144: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_mixed | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t145: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_mixed | intersect | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t146: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_mixed | except | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t147: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_mixed | values | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t148: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_mixed | aggregate | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t149: create the table … |  | - | no table |
| iceberg-ctas | hd | ctas_mixed | join | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t150: create the table … |  | - | no table |
| iceberg-ctas | hd | if_not_exists | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t151: create the table … |  | - | no table |
| iceberg-ctas | hd | if_not_exists | plain | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t152: create the table … |  | - | no table |
| iceberg-ctas | hd | if_not_exists | union | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t153: create the table … |  | - | no table |
| iceberg-ctas | hd | if_not_exists | union_all | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t154: create the table … |  | - | no table |
| iceberg-ctas | hd | if_not_exists | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t155: create the table … |  | - | no table |
| iceberg-ctas | hd | if_not_exists | intersect | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t156: create the table … |  | - | no table |
| iceberg-ctas | hd | if_not_exists | except | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t157: create the table … |  | - | no table |
| iceberg-ctas | hd | if_not_exists | values | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t158: create the table … |  | - | no table |
| iceberg-ctas | hd | if_not_exists | aggregate | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t159: create the table … |  | - | no table |
| iceberg-ctas | hd | if_not_exists | join | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t160: create the table … |  | - | no table |
| iceberg-ctas | hd | or_replace | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t161: create the table … |  | - | no table |
| iceberg-ctas | hd | or_replace | plain | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t162: create the table … |  | - | no table |
| iceberg-ctas | hd | or_replace | union | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t163: create the table … |  | - | no table |
| iceberg-ctas | hd | or_replace | union_all | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t164: create the table … |  | - | no table |
| iceberg-ctas | hd | or_replace | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t165: create the table … |  | - | no table |
| iceberg-ctas | hd | or_replace | intersect | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t166: create the table … |  | - | no table |
| iceberg-ctas | hd | or_replace | except | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t167: create the table … |  | - | no table |
| iceberg-ctas | hd | or_replace | values | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t168: create the table … |  | - | no table |
| iceberg-ctas | hd | or_replace | aggregate | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t169: create the table … |  | - | no table |
| iceberg-ctas | hd | or_replace | join | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t170: create the table … |  | - | no table |
| iceberg-ctas | hd | columns | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t171: create the table … |  | - | no table |
| iceberg-ctas | hd | columns | plain | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t172: create the table … |  | - | no table |
| iceberg-ctas | hd | columns | union | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t173: create the table … |  | - | no table |
| iceberg-ctas | hd | columns | union_all | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t174: create the table … |  | - | no table |
| iceberg-ctas | hd | columns | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t175: create the table … |  | - | no table |
| iceberg-ctas | hd | columns | intersect | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t176: create the table … |  | - | no table |
| iceberg-ctas | hd | columns | except | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t177: create the table … |  | - | no table |
| iceberg-ctas | hd | columns | values | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t178: create the table … |  | - | no table |
| iceberg-ctas | hd | columns | aggregate | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t179: create the table … |  | - | no table |
| iceberg-ctas | hd | columns | join | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t180: create the table … |  | - | no table |
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
| iceberg-ctas | hd | explain | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t211: create the table … |  | - | no table |
| iceberg-ctas | hd | explain | plain | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t212: create the table … |  | - | no table |
| iceberg-ctas | hd | explain | union | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t213: create the table … |  | - | no table |
| iceberg-ctas | hd | explain | union_all | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t214: create the table … |  | - | no table |
| iceberg-ctas | hd | explain | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t215: create the table … |  | - | no table |
| iceberg-ctas | hd | explain | intersect | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t216: create the table … |  | - | no table |
| iceberg-ctas | hd | explain | except | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t217: create the table … |  | - | no table |
| iceberg-ctas | hd | explain | values | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t218: create the table … |  | - | no table |
| iceberg-ctas | hd | explain | aggregate | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t219: create the table … |  | - | no table |
| iceberg-ctas | hd | explain | join | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t220: create the table … |  | - | no table |
| iceberg-ctas | hd | explain_analyze | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t221: create the table … |  | - | no table |
| iceberg-ctas | hd | explain_analyze | plain | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t222: create the table … |  | - | no table |
| iceberg-ctas | hd | explain_analyze | union | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t223: create the table … |  | - | no table |
| iceberg-ctas | hd | explain_analyze | union_all | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t224: create the table … |  | - | no table |
| iceberg-ctas | hd | explain_analyze | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t225: create the table … |  | - | no table |
| iceberg-ctas | hd | explain_analyze | intersect | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t226: create the table … |  | - | no table |
| iceberg-ctas | hd | explain_analyze | except | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t227: create the table … |  | - | no table |
| iceberg-ctas | hd | explain_analyze | values | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t228: create the table … |  | - | no table |
| iceberg-ctas | hd | explain_analyze | aggregate | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t229: create the table … |  | - | no table |
| iceberg-ctas | hd | explain_analyze | join | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t230: create the table … |  | - | no table |
| iceberg-ctas | hd | prepare | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t231: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t231' does not exist | - | no table |
| iceberg-ctas | hd | prepare | plain | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t232: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t232' does not exist | - | no table |
| iceberg-ctas | hd | prepare | union | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t233: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t233' does not exist | - | no table |
| iceberg-ctas | hd | prepare | union_all | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t234: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t234' does not exist | - | no table |
| iceberg-ctas | hd | prepare | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t235: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t235' does not exist | - | no table |
| iceberg-ctas | hd | prepare | intersect | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t236: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t236' does not exist | - | no table |
| iceberg-ctas | hd | prepare | except | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t237: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t237' does not exist | - | no table |
| iceberg-ctas | hd | prepare | values | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t238: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t238' does not exist | - | no table |
| iceberg-ctas | hd | prepare | aggregate | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t239: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t239' does not exist | - | no table |
| iceberg-ctas | hd | prepare | join | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.t240: create the table … | refused PySparkException: datafusion engine error: Execution error: Prepared statement 'p_t240' does not exist | - | no table |
| iceberg-ctas | ice | mixed_case_target | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t241: create the table … |  | - | no table |
| iceberg-ctas | ice | mixed_case_target | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.t242: create the table … |  | - | no table |
| iceberg-ctas | ice | external | - | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | hd | external | - | refused PySparkException: Unexpected => register_table does not support tables with data. |  | - | no table |
| iceberg-ctas | ice | default_catalog_unqualified | literal | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.d245: create the table … |  | - | no table |
| iceberg-ctas | ice | default_catalog_unqualified | distinct | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.d246: create the table … |  | - | no table |
| workaround | ice | create_then_insert | literal | answered | answered | - | rows [[1]], 1 snap, 00001-cb9dfa |
| workaround | ice | create_then_insert | plain | answered | answered | - | rows [[1], [1], [2], [3]], 1 snap, 00001-98465c |
| workaround | ice | create_then_insert | union | answered | answered | - | rows [[1], [2], [3], [4]], 1 snap, 00001-336bf9 |
| workaround | ice | create_then_insert | union_all | answered | answered | - | rows [[1], [1], [2], [2], [3], [3], [4]], 1 snap, 00001-cc4402 |
| workaround | ice | create_then_insert | distinct | answered | answered | - | rows [[1], [2], [3]], 1 snap, 00001-fbbc04 |
| workaround | ice | create_then_insert | intersect | answered | answered | - | rows [[2], [3]], 1 snap, 00001-b00a33 |
| workaround | ice | create_then_insert | except | answered | answered | - | rows [[1]], 1 snap, 00001-a519df |
| workaround | ice | create_then_insert | values | answered | answered | - | rows [[1], [2]], 1 snap, 00001-4d8092 |
| workaround | ice | create_then_insert | aggregate | answered | answered | - | rows [[1, 2], [2, 1], [3, 1]], 1 snap, 00001-e5bed8 |
| workaround | ice | create_then_insert | join | answered | answered | - | rows [[2], [3]], 1 snap, 00001-20b62a |
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
| existing | ice | or_replace_failing | - | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.e267: create the table … |  | rows [[7], [8]], 1 snap, 00001-a95a3e | rows [[7], [8]], 1 snap, 00001-a95a3e |
| existing | ice | or_replace_succeeding | - | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.e268: create the table … |  | rows [[7], [8]], 1 snap, 00001-e7c018 | rows [[7], [8]], 1 snap, 00001-e7c018 |
| existing | ice | if_not_exists_existing | - | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.e269: create the table … |  | rows [[7], [8]], 1 snap, 00001-ef9e59 | rows [[7], [8]], 1 snap, 00001-ef9e59 |
| existing | ice | ctas_existing | - | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table ice.ns.e270: create the table … |  | rows [[7], [8]], 1 snap, 00001-0bbb62 | rows [[7], [8]], 1 snap, 00001-0bbb62 |
| existing | hd | or_replace_failing | - | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.e271: create the table … |  | rows [[7], [8]], 1 snap, v2.metadata. | rows [[7], [8]], 1 snap, v2.metadata. |
| existing | hd | or_replace_succeeding | - | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.e272: create the table … |  | rows [[7], [8]], 1 snap, v2.metadata. | rows [[7], [8]], 1 snap, v2.metadata. |
| existing | hd | if_not_exists_existing | - | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.e273: create the table … |  | rows [[7], [8]], 1 snap, v2.metadata. | rows [[7], [8]], 1 snap, v2.metadata. |
| existing | hd | ctas_existing | - | refused UnsupportedOperationException: repark.sql() does not support CREATE TABLE ... AS SELECT into the Iceberg table hd.ns.e274: create the table … |  | rows [[7], [8]], 1 snap, v2.metadata. | rows [[7], [8]], 1 snap, v2.metadata. |
| control | ice | create_no_query | - | answered |  | - | rows [], 0 snap, 00000-241638 |
| control | ice | insert_select | union | answered |  | rows [], 0 snap, 00000-0bcc7d | rows [[1], [2], [3], [4]], 1 snap, 00001-c08146 |
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

