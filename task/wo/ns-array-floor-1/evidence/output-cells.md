# NS-ARRAY-FLOOR-1 output cells (D2, F1)

Nine doors return datetime64[ns] values under array<timestamp>,
array<timestamp_ntz>, timestamp and timestamp_ntz, on main (module 293896ce)
and live Spark 4.1.2 (both safe settings). Recorder
`python/repark/tests/_record_ns_array_floor_1_output.py`. Zone UTC. Live
banner: spark 4.1.2 zone UTC, pandas 3.0.6. Ticks are read back as integers
through an Arrow cast, never through Python datetime. Floor reference:
array [1767323045123456, 1767323045000000, 1767323045000000, -1, -2] and
[1767323045000000], top [1767323045123456, None, 1767323045000000, -1, -2];
truncation reference: top [1767323045123456, None, 1767323045000000, 0, -1].

## Run main

## NS-ARRAY-FLOOR-1 output cells

Backend main base 293896ce zone UTC live False safe n/a.

| door | kind | declared | outcome | ticks_or_error |
|---|---|---|---|---|
| grouped_applyin | array | tz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2], [1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] stored [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2], [1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] |
| grouped_applyin | array | ntz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2], [1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] stored [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2], [1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] |
| grouped_applyin | top | tz | error | PySparkException None applyInPandas failed converting pandas output to declared schema: ('Casting from timestamp[ns] to timestamp[us, tz=UTC] would lose data: 1767323045123456789', 'Conversion failed for column v with type /  |
| grouped_applyin | top | ntz | error | PySparkException None applyInPandas failed converting pandas output to declared schema: ('Casting from timestamp[ns] to timestamp[us] would lose data: 1767323045123456789', 'Conversion failed for column v with type datetim /  |
| cogroup_applyin | array | tz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2], [1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] stored [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2], [1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] |
| cogroup_applyin | array | ntz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2], [1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] stored [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2], [1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] |
| cogroup_applyin | top | tz | error | PySparkException None applyInPandas failed converting pandas output to declared schema: ('Casting from timestamp[ns] to timestamp[us, tz=UTC] would lose data: 1767323045123456789', 'Conversion failed for column v with type /  |
| cogroup_applyin | top | ntz | error | PySparkException None applyInPandas failed converting pandas output to declared schema: ('Casting from timestamp[ns] to timestamp[us] would lose data: 1767323045123456789', 'Conversion failed for column v with type datetim /  |
| grouped_applyinarrow | array | tz | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected list<item: timestamp[us, tz=UTC]>, actual list<item: timestamp[ns]>). /  |
| grouped_applyinarrow | array | ntz | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected list<item: timestamp[us]>, actual list<item: timestamp[ns]>). /  |
| grouped_applyinarrow | top | tz | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected timestamp[us, tz=UTC], actual timestamp[ns]). /  |
| grouped_applyinarrow | top | ntz | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected timestamp[us], actual timestamp[ns]). /  |
| cogroup_applyinarrow | array | tz | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected list<item: timestamp[us, tz=UTC]>, actual list<item: timestamp[ns]>). /  |
| cogroup_applyinarrow | array | ntz | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected list<item: timestamp[us]>, actual list<item: timestamp[ns]>). /  |
| cogroup_applyinarrow | top | tz | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected timestamp[us, tz=UTC], actual timestamp[ns]). /  |
| cogroup_applyinarrow | top | ntz | error | PySparkRuntimeError RESULT_COLUMN_TYPES_MISMATCH [RESULT_COLUMN_TYPES_MISMATCH] Column types of the returned data do not match specified schema. Mismatch: column 'v' (expected timestamp[us], actual timestamp[ns]). /  |
| mapinpan | array | tz | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type list<item: timestamp[us, tz=UTC]> (array<timestamp>), got list<item: timestamp[us]> /  |
| mapinpan | array | ntz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] stored [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] |
| mapinpan | top | tz | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type timestamp[us, tz=UTC] (timestamp), got timestamp[ns] /  |
| mapinpan | top | ntz | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type timestamp[us] (timestamp_ntz), got timestamp[ns] /  |
| mapinarrow | array | tz | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type list<item: timestamp[us, tz=UTC]> (array<timestamp>), got list<item: timestamp[ns]> /  |
| mapinarrow | array | ntz | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type list<item: timestamp[us]> (array<timestamp_ntz>), got list<item: timestamp[ns]> /  |
| mapinarrow | top | tz | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type timestamp[us, tz=UTC] (timestamp), got timestamp[ns] /  |
| mapinarrow | top | ntz | error | PySparkException None mapInArrow schema mismatch on field 'v': expected type timestamp[us] (timestamp_ntz), got timestamp[ns] /  |
| scalar_pandas_udf | array | tz | answer | frame [[1767323045123456, None], [1767323045123456, None], [1767323045123456, None], [1767323045123456, None], [1767323045123456, None]] stored [[1767323045123456, None], [1767323045123456, None], [1767323045123456, None], [1767323045123456, None], [1767323045123456, None]] |
| scalar_pandas_udf | array | ntz | answer | frame [[1767323045123456, None], [1767323045123456, None], [1767323045123456, None], [1767323045123456, None], [1767323045123456, None]] stored [[1767323045123456, None], [1767323045123456, None], [1767323045123456, None], [1767323045123456, None], [1767323045123456, None]] |
| scalar_pandas_udf | top | tz | error | PySparkException None pandas_udf 'func' failed converting result to declared type timestamp[us, tz=UTC] (timestamp): Casting from timestamp[ns] to timestamp[us, tz=UTC] would lose data: 1767323045123456789 /  |
| scalar_pandas_udf | top | ntz | error | PySparkException None pandas_udf 'func' failed converting result to declared type timestamp[us] (timestamp_ntz): Casting from timestamp[ns] to timestamp[us] would lose data: 1767323045123456789 /  |
| groupedagg_pandas_udf | array | tz | answer | frame [[1767323045123456, None], [1767323045123456, None]] stored [[1767323045123456, None], [1767323045123456, None]] |
| groupedagg_pandas_udf | array | ntz | answer | frame [[1767323045123456, None], [1767323045123456, None]] stored [[1767323045123456, None], [1767323045123456, None]] |
| groupedagg_pandas_udf | top | tz | error | PySparkException None applyInPandas failed converting pandas output to declared schema: ('Casting from timestamp[ns] to timestamp[us, tz=UTC] would lose data: 1767323045123456789', 'Conversion failed for column v with type /  |
| groupedagg_pandas_udf | top | ntz | error | PySparkException None applyInPandas failed converting pandas output to declared schema: ('Casting from timestamp[ns] to timestamp[us] would lose data: 1767323045123456789', 'Conversion failed for column v with type datetim /  |
| scalar_arrow_udf | array | tz | error | PySparkException None arrow_udf 'func' failed converting result to declared type list<item: timestamp[us, tz=UTC]>: Casting from timestamp[ns] to timestamp[us, tz=UTC] would lose data: 1767323045123456789 /  |
| scalar_arrow_udf | array | ntz | error | PySparkException None arrow_udf 'func' failed converting result to declared type list<item: timestamp[us]>: Casting from timestamp[ns] to timestamp[us] would lose data: 1767323045123456789 /  |
| scalar_arrow_udf | top | tz | error | PySparkException None arrow_udf 'func' failed converting result to declared type timestamp[us, tz=UTC]: Casting from timestamp[ns] to timestamp[us, tz=UTC] would lose data: 1767323045123456789 /  |
| scalar_arrow_udf | top | ntz | error | PySparkException None arrow_udf 'func' failed converting result to declared type timestamp[us]: Casting from timestamp[ns] to timestamp[us] would lose data: 1767323045123456789 /  |

## Run live Spark 4.1.2, safe=false

## NS-ARRAY-FLOOR-1 output cells

Backend spark412 base none zone UTC live True safe false.

Banner spark 4.1.2 zone UTC

| door | kind | declared | outcome | ticks_or_error |
|---|---|---|---|---|
| grouped_applyin | array | tz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2], [1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] |
| grouped_applyin | array | ntz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2], [1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] |
| grouped_applyin | top | tz | answer | frame [1767323045123456, None, 1767323045000000, 0, -1, 1767323045123456, None, 1767323045000000, 0, -1] |
| grouped_applyin | top | ntz | answer | frame [1767323045123456, None, 1767323045000000, 0, -1, 1767323045123456, None, 1767323045000000, 0, -1] |
| cogroup_applyin | array | tz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2], [1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] |
| cogroup_applyin | array | ntz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2], [1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] |
| cogroup_applyin | top | tz | answer | frame [1767323045123456, None, 1767323045000000, 0, -1, 1767323045123456, None, 1767323045000000, 0, -1] |
| cogroup_applyin | top | ntz | answer | frame [1767323045123456, None, 1767323045000000, 0, -1, 1767323045123456, None, 1767323045000000, 0, -1] |
| grouped_applyinarrow | array | tz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 3386, in main process() File <scratch> line 3377, in process se / pyspark.errors.exceptions.base.PySparkRuntimeError: [RES |
| grouped_applyinarrow | array | ntz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 3386, in main process() File <scratch> line 3377, in process se / pyspark.errors.exceptions.base.PySparkRuntimeError: [RES |
| grouped_applyinarrow | top | tz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 3386, in main process() File <scratch> line 3377, in process se / pyspark.errors.exceptions.base.PySparkRuntimeError: [RES |
| grouped_applyinarrow | top | ntz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 3386, in main process() File <scratch> line 3377, in process se / pyspark.errors.exceptions.base.PySparkRuntimeError: [RES |
| cogroup_applyinarrow | array | tz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 3386, in main process() File <scratch> line 3377, in process se / pyspark.errors.exceptions.base.PySparkRuntimeError: [RES |
| cogroup_applyinarrow | array | ntz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 3386, in main process() File <scratch> line 3377, in process se / pyspark.errors.exceptions.base.PySparkRuntimeError: [RES |
| cogroup_applyinarrow | top | tz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 3386, in main process() File <scratch> line 3377, in process se / pyspark.errors.exceptions.base.PySparkRuntimeError: [RES |
| cogroup_applyinarrow | top | ntz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 3386, in main process() File <scratch> line 3377, in process se / pyspark.errors.exceptions.base.PySparkRuntimeError: [RES |
| mapinpan | array | tz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] |
| mapinpan | array | ntz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] |
| mapinpan | top | tz | answer | frame [1767323045123456, None, 1767323045000000, 0, -1] |
| mapinpan | top | ntz | answer | frame [1767323045123456, None, 1767323045000000, 0, -1] |
| mapinarrow | array | tz | error | UnsupportedOperationException UNSUPPORTED_ARROWTYPE [UNSUPPORTED_ARROWTYPE] Unsupported arrow type Timestamp(NANOSECOND, null). SQLSTATE: 0A000 /  |
| mapinarrow | array | ntz | error | UnsupportedOperationException UNSUPPORTED_ARROWTYPE [UNSUPPORTED_ARROWTYPE] Unsupported arrow type Timestamp(NANOSECOND, null). SQLSTATE: 0A000 /  |
| mapinarrow | top | tz | error | UnsupportedOperationException UNSUPPORTED_ARROWTYPE [UNSUPPORTED_ARROWTYPE] Unsupported arrow type Timestamp(NANOSECOND, null). SQLSTATE: 0A000 /  |
| mapinarrow | top | ntz | error | UnsupportedOperationException UNSUPPORTED_ARROWTYPE [UNSUPPORTED_ARROWTYPE] Unsupported arrow type Timestamp(NANOSECOND, null). SQLSTATE: 0A000 /  |
| scalar_pandas_udf | array | tz | answer | frame [[1767323045123456, None], [1767323045123456, None], [1767323045123456, None], [1767323045123456, None], [1767323045123456, None]] |
| scalar_pandas_udf | array | ntz | answer | frame [[1767323045123456, None], [1767323045123456, None], [1767323045123456, None], [1767323045123456, None], [1767323045123456, None]] |
| scalar_pandas_udf | top | tz | answer | frame [1767323045123456, None, 1767323045000000, 0, -1] |
| scalar_pandas_udf | top | ntz | answer | frame [1767323045123456, None, 1767323045000000, 0, -1] |
| groupedagg_pandas_udf | array | tz | answer | frame [[1767323045123456, None], [1767323045123456, None]] |
| groupedagg_pandas_udf | array | ntz | answer | frame [[1767323045123456, None], [1767323045123456, None]] |
| groupedagg_pandas_udf | top | tz | answer | frame [1767323045123456, 1767323045123456] |
| groupedagg_pandas_udf | top | ntz | answer | frame [1767323045123456, 1767323045123456] |
| scalar_arrow_udf | array | tz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File "pyarrow/array.pxi", line 1203, in pyarrow.lib.Array.cast File <scratch> line  /  |
| scalar_arrow_udf | array | ntz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File "pyarrow/array.pxi", line 1203, in pyarrow.lib.Array.cast File <scratch> line  /  |
| scalar_arrow_udf | top | tz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File "pyarrow/array.pxi", line 1203, in pyarrow.lib.Array.cast File <scratch> line  /  |
| scalar_arrow_udf | top | ntz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File "pyarrow/array.pxi", line 1203, in pyarrow.lib.Array.cast File <scratch> line  /  |

## Run live Spark 4.1.2, safe=true

## NS-ARRAY-FLOOR-1 output cells

Backend spark412 base none zone UTC live True safe true.

Banner spark 4.1.2 zone UTC

| door | kind | declared | outcome | ticks_or_error |
|---|---|---|---|---|
| grouped_applyin | array | tz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2], [1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] |
| grouped_applyin | array | ntz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2], [1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] |
| grouped_applyin | top | tz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 473, in _create_array return pa.Array.from_pandas( ^^^^^^^^^^^^ / pyspark.errors.exceptions.base.PySparkValueError: Failed |
| grouped_applyin | top | ntz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 473, in _create_array return pa.Array.from_pandas( ^^^^^^^^^^^^ / pyspark.errors.exceptions.base.PySparkValueError: Failed |
| cogroup_applyin | array | tz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2], [1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] |
| cogroup_applyin | array | ntz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2], [1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] |
| cogroup_applyin | top | tz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 473, in _create_array return pa.Array.from_pandas( ^^^^^^^^^^^^ / pyspark.errors.exceptions.base.PySparkValueError: Failed |
| cogroup_applyin | top | ntz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 473, in _create_array return pa.Array.from_pandas( ^^^^^^^^^^^^ / pyspark.errors.exceptions.base.PySparkValueError: Failed |
| grouped_applyinarrow | array | tz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 3386, in main process() File <scratch> line 3377, in process se / pyspark.errors.exceptions.base.PySparkRuntimeError: [RES |
| grouped_applyinarrow | array | ntz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 3386, in main process() File <scratch> line 3377, in process se / pyspark.errors.exceptions.base.PySparkRuntimeError: [RES |
| grouped_applyinarrow | top | tz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 3386, in main process() File <scratch> line 3377, in process se / pyspark.errors.exceptions.base.PySparkRuntimeError: [RES |
| grouped_applyinarrow | top | ntz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 3386, in main process() File <scratch> line 3377, in process se / pyspark.errors.exceptions.base.PySparkRuntimeError: [RES |
| cogroup_applyinarrow | array | tz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 3386, in main process() File <scratch> line 3377, in process se / pyspark.errors.exceptions.base.PySparkRuntimeError: [RES |
| cogroup_applyinarrow | array | ntz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 3386, in main process() File <scratch> line 3377, in process se / pyspark.errors.exceptions.base.PySparkRuntimeError: [RES |
| cogroup_applyinarrow | top | tz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 3386, in main process() File <scratch> line 3377, in process se / pyspark.errors.exceptions.base.PySparkRuntimeError: [RES |
| cogroup_applyinarrow | top | ntz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 3386, in main process() File <scratch> line 3377, in process se / pyspark.errors.exceptions.base.PySparkRuntimeError: [RES |
| mapinpan | array | tz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] |
| mapinpan | array | ntz | answer | frame [[1767323045123456, 1767323045000000, 1767323045000000, -1, -2]] |
| mapinpan | top | tz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 473, in _create_array return pa.Array.from_pandas( ^^^^^^^^^^^^ / pyspark.errors.exceptions.base.PySparkValueError: Failed |
| mapinpan | top | ntz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 473, in _create_array return pa.Array.from_pandas( ^^^^^^^^^^^^ / pyspark.errors.exceptions.base.PySparkValueError: Failed |
| mapinarrow | array | tz | error | UnsupportedOperationException UNSUPPORTED_ARROWTYPE [UNSUPPORTED_ARROWTYPE] Unsupported arrow type Timestamp(NANOSECOND, null). SQLSTATE: 0A000 /  |
| mapinarrow | array | ntz | error | UnsupportedOperationException UNSUPPORTED_ARROWTYPE [UNSUPPORTED_ARROWTYPE] Unsupported arrow type Timestamp(NANOSECOND, null). SQLSTATE: 0A000 /  |
| mapinarrow | top | tz | error | UnsupportedOperationException UNSUPPORTED_ARROWTYPE [UNSUPPORTED_ARROWTYPE] Unsupported arrow type Timestamp(NANOSECOND, null). SQLSTATE: 0A000 /  |
| mapinarrow | top | ntz | error | UnsupportedOperationException UNSUPPORTED_ARROWTYPE [UNSUPPORTED_ARROWTYPE] Unsupported arrow type Timestamp(NANOSECOND, null). SQLSTATE: 0A000 /  |
| scalar_pandas_udf | array | tz | answer | frame [[1767323045123456, None], [1767323045123456, None], [1767323045123456, None], [1767323045123456, None], [1767323045123456, None]] |
| scalar_pandas_udf | array | ntz | answer | frame [[1767323045123456, None], [1767323045123456, None], [1767323045123456, None], [1767323045123456, None], [1767323045123456, None]] |
| scalar_pandas_udf | top | tz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 473, in _create_array return pa.Array.from_pandas( ^^^^^^^^^^^^ / pyspark.errors.exceptions.base.PySparkValueError: Failed |
| scalar_pandas_udf | top | ntz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 473, in _create_array return pa.Array.from_pandas( ^^^^^^^^^^^^ / pyspark.errors.exceptions.base.PySparkValueError: Failed |
| groupedagg_pandas_udf | array | tz | answer | frame [[1767323045123456, None], [1767323045123456, None]] |
| groupedagg_pandas_udf | array | ntz | answer | frame [[1767323045123456, None], [1767323045123456, None]] |
| groupedagg_pandas_udf | top | tz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 473, in _create_array return pa.Array.from_pandas( ^^^^^^^^^^^^ / pyspark.errors.exceptions.base.PySparkValueError: Failed |
| groupedagg_pandas_udf | top | ntz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File <scratch> line 473, in _create_array return pa.Array.from_pandas( ^^^^^^^^^^^^ / pyspark.errors.exceptions.base.PySparkValueError: Failed |
| scalar_arrow_udf | array | tz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File "pyarrow/array.pxi", line 1203, in pyarrow.lib.Array.cast File <scratch> line  /  |
| scalar_arrow_udf | array | ntz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File "pyarrow/array.pxi", line 1203, in pyarrow.lib.Array.cast File <scratch> line  /  |
| scalar_arrow_udf | top | tz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File "pyarrow/array.pxi", line 1203, in pyarrow.lib.Array.cast File <scratch> line  /  |
| scalar_arrow_udf | top | ntz | error | PythonException None An exception was thrown from the Python worker. Please see the stack trace below. Traceback (most recent call last): File "pyarrow/array.pxi", line 1203, in pyarrow.lib.Array.cast File <scratch> line  /  |

