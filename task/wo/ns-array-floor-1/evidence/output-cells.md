# NS-ARRAY-FLOOR-1 output cells (D2)

Nine doors return datetime64[ns] values under array<timestamp>,
array<timestamp_ntz>, timestamp and timestamp_ntz, on main (module 293896ce)
and live Spark 4.1.2. Recorder
`python/repark/tests/_record_ns_array_floor_1_output.py`. Zone UTC.
Live banner: spark 4.1.2 zone UTC. Floor reference ticks: array
[1767323045123456, 1767323045000000] and [1767323045000000], top
[1767323045123456, None, 1767323045000000].

## Run main

## NS-ARRAY-FLOOR-1 output cells

Backend main base 293896ce zone UTC live False.

| door | kind | declared | outcome | ticks_or_error |
|---|---|---|---|---|
| grouped_applyin | array | tz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000], [1767323045123456, 1767323045000000, 1767323045000000]] stored [[1767323045123456, 1767323045000000, 1767323045000000], [1767323045123456, 1767323045000000, 1767323045000000]] |
| grouped_applyin | array | ntz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000], [1767323045123456, 1767323045000000, 1767323045000000]] stored [[1767323045123456, 1767323045000000, 1767323045000000], [1767323045123456, 1767323045000000, 1767323045000000]] |
| grouped_applyin | top | tz | error | PySparkException None applyInPandas failed converting pandas output to declared schema: ('Casting from timestamp[ns] to timestamp[us, tz=UTC] would lose data: 1767323045123456789', 'Conversion failed for column v with type |
| grouped_applyin | top | ntz | error | PySparkException None applyInPandas failed converting pandas output to declared schema: ('Casting from timestamp[ns] to timestamp[us] would lose data: 1767323045123456789', 'Conversion failed for column v with type datetim |
| cogroup_applyin | array | tz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000], [1767323045123456, 1767323045000000, 1767323045000000]] stored [[1767323045123456, 1767323045000000, 1767323045000000], [1767323045123456, 1767323045000000, 1767323045000000]] |
| cogroup_applyin | array | ntz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000], [1767323045123456, 1767323045000000, 1767323045000000]] stored [[1767323045123456, 1767323045000000, 1767323045000000], [1767323045123456, 1767323045000000, 1767323045000000]] |
| cogroup_applyin | top | tz | error | PySparkException None applyInPandas failed converting pandas output to declared schema: ('Casting from timestamp[ns] to timestamp[us, tz=UTC] would lose data: 1767323045123456789', 'Conversion failed for column v with type |
| cogroup_applyin | top | ntz | error | PySparkException None applyInPandas failed converting pandas output to declared schema: ('Casting from timestamp[ns] to timestamp[us] would lose data: 1767323045123456789', 'Conversion failed for column v with type datetim |
| grouped_applyinarrow | array | tz | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected list<item: timestamp[us, tz=UTC]>, actual list<item: timestamp[ns]>). |
| grouped_applyinarrow | array | ntz | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected list<item: timestamp[us]>, actual list<item: timestamp[ns]>). |
| grouped_applyinarrow | top | tz | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected timestamp[us, tz=UTC], actual timestamp[ns]). |
| grouped_applyinarrow | top | ntz | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected timestamp[us], actual timestamp[ns]). |
| cogroup_applyinarrow | array | tz | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected list<item: timestamp[us, tz=UTC]>, actual list<item: timestamp[ns]>). |
| cogroup_applyinarrow | array | ntz | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected list<item: timestamp[us]>, actual list<item: timestamp[ns]>). |
| cogroup_applyinarrow | top | tz | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected timestamp[us, tz=UTC], actual timestamp[ns]). |
| cogroup_applyinarrow | top | ntz | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected timestamp[us], actual timestamp[ns]). |
| mapinpan | array | tz | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type list<item: timestamp[us, tz=UTC]> (array<timestamp>), got list<item: timestamp[us]> |
| mapinpan | array | ntz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000]] stored [[1767323045123456, 1767323045000000, 1767323045000000]] |
| mapinpan | top | tz | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type timestamp[us, tz=UTC] (timestamp), got timestamp[ns] |
| mapinpan | top | ntz | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type timestamp[us] (timestamp_ntz), got timestamp[ns] |
| mapinarrow | array | tz | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type list<item: timestamp[us, tz=UTC]> (array<timestamp>), got list<item: timestamp[ns]> |
| mapinarrow | array | ntz | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type list<item: timestamp[us]> (array<timestamp_ntz>), got list<item: timestamp[ns]> |
| mapinarrow | top | tz | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type timestamp[us, tz=UTC] (timestamp), got timestamp[ns] |
| mapinarrow | top | ntz | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type timestamp[us] (timestamp_ntz), got timestamp[ns] |
| scalar_pandas_udf | array | tz | answer | frame [[1767323045123456, None], [1767323045123456, None], [1767323045123456, None]] stored [[1767323045123456, None], [1767323045123456, None], [1767323045123456, None]] |
| scalar_pandas_udf | array | ntz | answer | frame [[1767323045123456, None], [1767323045123456, None], [1767323045123456, None]] stored [[1767323045123456, None], [1767323045123456, None], [1767323045123456, None]] |
| scalar_pandas_udf | top | tz | error | PySparkException None pandas_udf 'func' failed converting result to declared type timestamp[us, tz=UTC] (timestamp): Casting from timestamp[ns] to timestamp[us, tz=UTC] would lose data: 1767323045123456789 |
| scalar_pandas_udf | top | ntz | error | PySparkException None pandas_udf 'func' failed converting result to declared type timestamp[us] (timestamp_ntz): Casting from timestamp[ns] to timestamp[us] would lose data: 1767323045123456789 |
| groupedagg_pandas_udf | array | tz | answer | frame [[1767323045123456, None], [1767323045123456, None]] stored [[1767323045123456, None], [1767323045123456, None]] |
| groupedagg_pandas_udf | array | ntz | answer | frame [[1767323045123456, None], [1767323045123456, None]] stored [[1767323045123456, None], [1767323045123456, None]] |
| groupedagg_pandas_udf | top | tz | error | PySparkException None applyInPandas failed converting pandas output to declared schema: ('Casting from timestamp[ns] to timestamp[us, tz=UTC] would lose data: 1767323045123456789', 'Conversion failed for column v with type |
| groupedagg_pandas_udf | top | ntz | error | PySparkException None applyInPandas failed converting pandas output to declared schema: ('Casting from timestamp[ns] to timestamp[us] would lose data: 1767323045123456789', 'Conversion failed for column v with type datetim |
| scalar_arrow_udf | array | tz | error | PySparkException None arrow_udf 'func' failed converting result to declared type list<item: timestamp[us, tz=UTC]>: Casting from timestamp[ns] to timestamp[us, tz=UTC] would lose data: 1767323045123456789 |
| scalar_arrow_udf | array | ntz | error | PySparkException None arrow_udf 'func' failed converting result to declared type list<item: timestamp[us]>: Casting from timestamp[ns] to timestamp[us] would lose data: 1767323045123456789 |
| scalar_arrow_udf | top | tz | error | PySparkException None arrow_udf 'func' failed converting result to declared type timestamp[us, tz=UTC]: Casting from timestamp[ns] to timestamp[us, tz=UTC] would lose data: 1767323045123456789 |
| scalar_arrow_udf | top | ntz | error | PySparkException None arrow_udf 'func' failed converting result to declared type timestamp[us]: Casting from timestamp[ns] to timestamp[us] would lose data: 1767323045123456789 |

## Run live

## NS-ARRAY-FLOOR-1 output cells

Backend spark412 base none zone UTC live True.

Banner spark 4.1.2 zone UTC

| door | kind | declared | outcome | ticks_or_error |
|---|---|---|---|---|
| grouped_applyin | array | tz | error | ModuleNotFoundError  No module named 'pandas' |
| grouped_applyin | array | ntz | error | ModuleNotFoundError  No module named 'pandas' |
| grouped_applyin | top | tz | error | ModuleNotFoundError  No module named 'pandas' |
| grouped_applyin | top | ntz | error | ModuleNotFoundError  No module named 'pandas' |
| cogroup_applyin | array | tz | error | ModuleNotFoundError  No module named 'pandas' |
| cogroup_applyin | array | ntz | error | ModuleNotFoundError  No module named 'pandas' |
| cogroup_applyin | top | tz | error | ModuleNotFoundError  No module named 'pandas' |
| cogroup_applyin | top | ntz | error | ModuleNotFoundError  No module named 'pandas' |
| grouped_applyinarrow | array | tz | error | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| grouped_applyinarrow | array | ntz | error | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| grouped_applyinarrow | top | tz | error | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| grouped_applyinarrow | top | ntz | error | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| cogroup_applyinarrow | array | tz | error | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| cogroup_applyinarrow | array | ntz | error | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| cogroup_applyinarrow | top | tz | error | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| cogroup_applyinarrow | top | ntz | error | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| mapinpan | array | tz | error | ModuleNotFoundError  No module named 'pandas' |
| mapinpan | array | ntz | error | ModuleNotFoundError  No module named 'pandas' |
| mapinpan | top | tz | error | ModuleNotFoundError  No module named 'pandas' |
| mapinpan | top | ntz | error | ModuleNotFoundError  No module named 'pandas' |
| mapinarrow | array | tz | error | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| mapinarrow | array | ntz | error | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| mapinarrow | top | tz | error | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| mapinarrow | top | ntz | error | PySparkImportError PACKAGE_NOT_INSTALLED [PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed; however, it was not found. |
| scalar_pandas_udf | array | tz | error | ModuleNotFoundError  No module named 'pandas' |
| scalar_pandas_udf | array | ntz | error | ModuleNotFoundError  No module named 'pandas' |
| scalar_pandas_udf | top | tz | error | ModuleNotFoundError  No module named 'pandas' |
| scalar_pandas_udf | top | ntz | error | ModuleNotFoundError  No module named 'pandas' |
| groupedagg_pandas_udf | array | tz | error | ModuleNotFoundError  No module named 'pandas' |
| groupedagg_pandas_udf | array | ntz | error | ModuleNotFoundError  No module named 'pandas' |
| groupedagg_pandas_udf | top | tz | error | ModuleNotFoundError  No module named 'pandas' |
| groupedagg_pandas_udf | top | ntz | error | ModuleNotFoundError  No module named 'pandas' |
| scalar_arrow_udf | array | tz | error | PySparkNotImplementedError UNSUPPORTED_SIGNATURE [UNSUPPORTED_SIGNATURE] Unsupported signature: (array: 'Any') -> 'pa.Array'. |
| scalar_arrow_udf | array | ntz | error | PySparkNotImplementedError UNSUPPORTED_SIGNATURE [UNSUPPORTED_SIGNATURE] Unsupported signature: (array: 'Any') -> 'pa.Array'. |
| scalar_arrow_udf | top | tz | error | PySparkNotImplementedError UNSUPPORTED_SIGNATURE [UNSUPPORTED_SIGNATURE] Unsupported signature: (array: 'Any') -> 'pa.Array'. |
| scalar_arrow_udf | top | ntz | error | PySparkNotImplementedError UNSUPPORTED_SIGNATURE [UNSUPPORTED_SIGNATURE] Unsupported signature: (array: 'Any') -> 'pa.Array'. |

