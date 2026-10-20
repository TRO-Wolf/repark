# NS-ARRAY-FLOOR-1 Lane A echo cells (F2)

The function returns its input frame unchanged and the declared return
schema is the input frame's own schema. Sources are a plain timestamp_ns
column, coalesce(ns, NULL), and a microsecond control; on live Spark the
microsecond control plus a pandas frame built with datetime64[ns] values.
Recorder `python/repark/tests/timestamps/_record_ns_array_floor_1_lane_a.py`. Zone UTC.
Live banner: spark 4.1.2 zone UTC, pandas 3.0.6, default safe setting
(true in this environment). Ticks are read back as integers through an
Arrow cast. The live ns_pandas ticks compose a floor to microseconds with a
naive-to-JVM-zone localization (January dates take EST, plus 18000000000).

## Run main

## NS-ARRAY-FLOOR-1 Lane A echo cells

Backend main base 293896ce zone UTC live False safe n/a.

| door | source | schema | outcome | ticks_or_error |
|---|---|---|---|---|
| grouped_applyin | ns_plain | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkException None applyInPandas failed converting pandas output to declared schema: ('Casting from timestamp[ns] to timestamp[us] would lose data: -1', 'Conversion failed for column v with type datetime64[ns]') |
| cogroup_applyin | ns_plain | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkException None applyInPandas failed converting pandas output to declared schema: ('Casting from timestamp[ns] to timestamp[us] would lose data: -1', 'Conversion failed for column v with type datetime64[ns]') |
| grouped_applyinarrow | ns_plain | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected timestamp[us], actual timestamp[ns]). |
| cogroup_applyinarrow | ns_plain | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected timestamp[us], actual timestamp[ns]). |
| mapinpan | ns_plain | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type timestamp[us] (timestamp_ntz), got timestamp[ns] |
| mapinarrow | ns_plain | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type timestamp[us] (timestamp_ntz), got timestamp[ns] |
| scalar_pandas_udf | ns_plain | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkException None pandas_udf 'func' failed converting result to declared type timestamp[us] (timestamp_ntz): Casting from timestamp[ns] to timestamp[us] would lose data: 1767323045123456789 |
| scalar_arrow_udf | ns_plain | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkException None arrow_udf 'func' failed converting result to declared type timestamp[us]: Casting from timestamp[ns] to timestamp[us] would lose data: 1767323045123456789 |
| grouped_applyin | ns_coal | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [None, -1, 1767323045123456, 1767323045000000, -2] |
| cogroup_applyin | ns_coal | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [None, -1, 1767323045123456, 1767323045000000, -2] |
| grouped_applyinarrow | ns_coal | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [None, -1, 1767323045123456, 1767323045000000, -2] |
| cogroup_applyinarrow | ns_coal | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [None, -1, 1767323045123456, 1767323045000000, -2] |
| mapinpan | ns_coal | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [1767323045123456, None, 1767323045000000, -1, -2] |
| mapinarrow | ns_coal | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [1767323045123456, None, 1767323045000000, -1, -2] |
| scalar_pandas_udf | ns_coal | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [1767323045123456, None, 1767323045000000, -1, -2] |
| scalar_arrow_udf | ns_coal | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [1767323045123456, None, 1767323045000000, -1, -2] |
| grouped_applyin | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [None, -1, 1767323045123456, 1767323045000000, -2] |
| cogroup_applyin | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [None, -1, 1767323045123456, 1767323045000000, -2] |
| grouped_applyinarrow | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [None, -1, 1767323045123456, 1767323045000000, -2] |
| cogroup_applyinarrow | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [None, -1, 1767323045123456, 1767323045000000, -2] |
| mapinpan | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [1767323045123456, None, 1767323045000000, -1, -2] |
| mapinarrow | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [1767323045123456, None, 1767323045000000, -1, -2] |
| scalar_pandas_udf | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [1767323045123456, None, 1767323045000000, -1, -2] |
| scalar_arrow_udf | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [1767323045123456, None, 1767323045000000, -1, -2] |

## Run U-1 head

## NS-ARRAY-FLOOR-1 Lane A echo cells

Backend u1 base 07cbe563 zone UTC live False safe n/a.

| door | source | schema | outcome | ticks_or_error |
|---|---|---|---|---|
| grouped_applyin | ns_plain | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkException None applyInPandas failed converting pandas output to declared schema: ('Casting from timestamp[ns] to timestamp[us] would lose data: -1', 'Conversion failed for column v with type datetime64[ns]') |
| cogroup_applyin | ns_plain | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkException None applyInPandas failed converting pandas output to declared schema: ('Casting from timestamp[ns] to timestamp[us] would lose data: -1', 'Conversion failed for column v with type datetime64[ns]') |
| grouped_applyinarrow | ns_plain | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected timestamp[us], actual timestamp[ns]). |
| cogroup_applyinarrow | ns_plain | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected timestamp[us], actual timestamp[ns]). |
| mapinpan | ns_plain | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type timestamp[us] (timestamp_ntz), got timestamp[ns] |
| mapinarrow | ns_plain | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type timestamp[us] (timestamp_ntz), got timestamp[ns] |
| scalar_pandas_udf | ns_plain | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkException None pandas_udf 'func' failed converting result to declared type timestamp[us] (timestamp_ntz): Casting from timestamp[ns] to timestamp[us] would lose data: 1767323045123456789 |
| scalar_arrow_udf | ns_plain | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkException None arrow_udf 'func' failed converting result to declared type timestamp[us]: Casting from timestamp[ns] to timestamp[us] would lose data: 1767323045123456789 |
| grouped_applyin | ns_coal | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkException None applyInPandas failed converting pandas output to declared schema: ('Casting from timestamp[ns] to timestamp[us] would lose data: -1', 'Conversion failed for column v with type datetime64[ns]') |
| cogroup_applyin | ns_coal | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkException None applyInPandas failed converting pandas output to declared schema: ('Casting from timestamp[ns] to timestamp[us] would lose data: -1', 'Conversion failed for column v with type datetime64[ns]') |
| grouped_applyinarrow | ns_coal | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected timestamp[us], actual timestamp[ns]). |
| cogroup_applyinarrow | ns_coal | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected timestamp[us], actual timestamp[ns]). |
| mapinpan | ns_coal | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type timestamp[us] (timestamp_ntz), got timestamp[ns] |
| mapinarrow | ns_coal | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type timestamp[us] (timestamp_ntz), got timestamp[ns] |
| scalar_pandas_udf | ns_coal | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkException None pandas_udf 'func' failed converting result to declared type timestamp[us] (timestamp_ntz): Casting from timestamp[ns] to timestamp[us] would lose data: 1767323045123456789 |
| scalar_arrow_udf | ns_coal | [['k', 'int'], ['v', 'timestamp_ntz']] | error | PySparkException None arrow_udf 'func' failed converting result to declared type timestamp[us]: Casting from timestamp[ns] to timestamp[us] would lose data: 1767323045123456789 |
| grouped_applyin | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [None, -1, 1767323045123456, 1767323045000000, -2] |
| cogroup_applyin | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [None, -1, 1767323045123456, 1767323045000000, -2] |
| grouped_applyinarrow | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [None, -1, 1767323045123456, 1767323045000000, -2] |
| cogroup_applyinarrow | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [None, -1, 1767323045123456, 1767323045000000, -2] |
| mapinpan | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [1767323045123456, None, 1767323045000000, -1, -2] |
| mapinarrow | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [1767323045123456, None, 1767323045000000, -1, -2] |
| scalar_pandas_udf | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [1767323045123456, None, 1767323045000000, -1, -2] |
| scalar_arrow_udf | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [1767323045123456, None, 1767323045000000, -1, -2] |

## Run live Spark 4.1.2

## NS-ARRAY-FLOOR-1 Lane A echo cells

Backend spark412 base none zone UTC live True safe true.

Banner spark 4.1.2 zone UTC

| door | source | schema | outcome | ticks_or_error |
|---|---|---|---|---|
| grouped_applyin | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [None, 17999999999, 1767341045123456, 1767341045000000, 17999999998] |
| cogroup_applyin | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [None, 17999999999, 1767341045123456, 1767341045000000, 17999999998] |
| grouped_applyinarrow | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [None, 17999999999, 1767341045123456, 1767341045000000, 17999999998] |
| cogroup_applyinarrow | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [None, 17999999999, 1767341045123456, 1767341045000000, 17999999998] |
| mapinpan | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [1767341045123456, None, 1767341045000000, 17999999999, 17999999998] |
| mapinarrow | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [1767341045123456, None, 1767341045000000, 17999999999, 17999999998] |
| scalar_pandas_udf | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [1767341045123456, None, 1767341045000000, 17999999999, 17999999998] |
| scalar_arrow_udf | us | [['k', 'int'], ['v', 'timestamp']] | answer | [['k', 'int'], ['v', 'timestamp']] ticks [1767341045123456, None, 1767341045000000, 17999999999, 17999999998] |
| grouped_applyin | ns_pandas | [['k', 'bigint'], ['v', 'timestamp']] | answer | [['k', 'bigint'], ['v', 'timestamp']] ticks [None, 17999999999, 1767341045123456, 1767341045000000, 17999999998] |
| cogroup_applyin | ns_pandas | [['k', 'bigint'], ['v', 'timestamp']] | answer | [['k', 'bigint'], ['v', 'timestamp']] ticks [None, 17999999999, 1767341045123456, 1767341045000000, 17999999998] |
| grouped_applyinarrow | ns_pandas | [['k', 'bigint'], ['v', 'timestamp']] | answer | [['k', 'bigint'], ['v', 'timestamp']] ticks [None, 17999999999, 1767341045123456, 1767341045000000, 17999999998] |
| cogroup_applyinarrow | ns_pandas | [['k', 'bigint'], ['v', 'timestamp']] | answer | [['k', 'bigint'], ['v', 'timestamp']] ticks [None, 17999999999, 1767341045123456, 1767341045000000, 17999999998] |
| mapinpan | ns_pandas | [['k', 'bigint'], ['v', 'timestamp']] | answer | [['k', 'bigint'], ['v', 'timestamp']] ticks [1767341045123456, None, 1767341045000000, 17999999999, 17999999998] |
| mapinarrow | ns_pandas | [['k', 'bigint'], ['v', 'timestamp']] | answer | [['k', 'bigint'], ['v', 'timestamp']] ticks [1767341045123456, None, 1767341045000000, 17999999999, 17999999998] |
| scalar_pandas_udf | ns_pandas | [['k', 'bigint'], ['v', 'timestamp']] | answer | [['k', 'bigint'], ['v', 'timestamp']] ticks [1767341045123456, None, 1767341045000000, 17999999999, 17999999998] |
| scalar_arrow_udf | ns_pandas | [['k', 'bigint'], ['v', 'timestamp']] | answer | [['k', 'bigint'], ['v', 'timestamp']] ticks [1767341045123456, None, 1767341045000000, 17999999999, 17999999998] |

