from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, NoReturn

from repark import _native
from repark.errors import (
    IllegalArgumentException,
    PySparkNotImplementedError,
    PySparkTypeError,
    PySparkValueError,
)
from repark.spark._secrets import register_config_value
from repark.spark.streaming.query import StreamingQuery

if TYPE_CHECKING:
    from repark.spark.dataframe import DataFrame
    from repark.spark.session.session_core import ReparkSession

_STREAMING_CONF_KEYS = (
    "spark.sql.streaming.checkpointLocation",
    "spark.sql.streaming.stopTimeout",
    "spark.sql.streaming.pollingDelay",
    "spark.sql.streaming.numRecentProgressUpdates",
)

_DEFAULT_TRIGGER = "default"

_PROCESSING_TIME_TRIGGER = "processingTime"
_ONCE_TRIGGER = "once"
_CONTINUOUS_TRIGGER = "continuous"
_AVAILABLE_NOW_TRIGGER = "availableNow"


def _to_option_text(value: object) -> str | None:
    if isinstance(value, bool):
        return str(value).lower()
    if value is None:
        return None
    return str(value)


def _store_option(options: dict[str, str | None], key: object, value: object) -> None:
    text = _to_option_text(value)
    if text is not None:
        register_config_value(text)
    key_text = str(key)
    for existing in list(options):
        if existing.lower() == key_text.lower():
            del options[existing]
    options[key_text] = text


def _without_none_values(options: dict[str, str | None]) -> dict[str, str]:
    return {key: value for key, value in options.items() if value is not None}


def _not_implemented(feature: str) -> NoReturn:
    raise PySparkNotImplementedError(
        f"[NOT_IMPLEMENTED] {feature} is not implemented.",
        errorClass="NOT_IMPLEMENTED",
        messageParameters={"feature": feature},
    )


def _refuse_format(door: str, source: str | None) -> None:
    if source is None:
        _not_implemented(f"{door}.format(parquet)")
    elif source.lower() != "iceberg":
        _not_implemented(f"{door}.format({source[:64]})")


def _option_path(options: dict[str, str | None]) -> str | None:
    for key, value in options.items():
        if key.lower() == "path" and value:
            return value
    return None


def _streaming_confs(session: ReparkSession) -> dict[str, str]:
    found: dict[str, str] = {}
    for key in _STREAMING_CONF_KEYS:
        value = session.conf.get(key, None)
        if value is not None:
            found[key] = value
    return found


class DataStreamReader:
    """Interface used to load a streaming DataFrame from external storage systems.

    Use ``SparkSession.readStream`` to access this.
    """

    def __init__(self, session: ReparkSession) -> None:
        """Bind to the owning session; each ``spark.readStream`` access returns a fresh reader."""
        self._session = session
        self._format: str | None = None
        self._options: dict[str, str | None] = {}

    def format(self, source: str) -> DataStreamReader:
        """Specifies the input data source format.

        Parameters
        ----------
        source : str
            name of the data source, e.g. 'iceberg'.
        """
        self._format = source
        return self

    def option(self, key: str, value: object) -> DataStreamReader:
        """Adds an input option for the underlying data source."""
        _store_option(self._options, key, value)
        return self

    def options(self, **options: object) -> DataStreamReader:
        """Adds input options for the underlying data source."""
        for key, value in options.items():
            self.option(key, value)
        return self

    def load(self, path: str | None = None) -> DataFrame:
        """Loads a data stream from a data source and returns it as a DataFrame.

        Parameters
        ----------
        path : str, optional
            table identifier for table-backed data sources.
        """
        if path is not None and (type(path) is not str or len(path.strip()) == 0):
            raise PySparkValueError(
                errorClass="VALUE_NOT_NON_EMPTY_STR",
                messageParameters={"arg_name": "path", "arg_value": str(path)},
            )
        _refuse_format("readStream", self._format)
        effective = path if path is not None else _option_path(self._options)
        if effective is None:
            raise IllegalArgumentException("Cannot open table: path is not set")
        inner = self._session._ensure_alive()
        native = _native.load_stream(inner, effective, _without_none_values(self._options))
        from repark.spark.dataframe import DataFrame

        return DataFrame(native, inner, self._session._alive_token)

    def table(self, tableName: str) -> DataFrame:  # noqa: N803 — PySpark kwarg name
        """Define a Streaming DataFrame on a Table.

        Parameters
        ----------
        tableName : str
            name of the table.
        """
        if not isinstance(tableName, str):
            raise PySparkTypeError(
                errorClass="NOT_STR",
                messageParameters={
                    "arg_name": "tableName",
                    "arg_type": type(tableName).__name__,
                },
            )
        inner = self._session._ensure_alive()
        native = _native.load_stream(inner, tableName, _without_none_values(self._options))
        from repark.spark.dataframe import DataFrame

        return DataFrame(native, inner, self._session._alive_token)


class DataStreamWriter:
    """Interface used to write a streaming DataFrame to external storage systems.

    Use ``DataFrame.writeStream`` to access this.
    """

    def __init__(self, frame: DataFrame) -> None:
        """Bind to the streaming DataFrame; each access returns a fresh writer."""
        self._frame = frame
        self._format: str | None = None
        self._output_mode: str | None = None
        self._options: dict[str, str | None] = {}
        self._query_name: str | None = None
        self._trigger_kind: str | None = None
        self._trigger_interval: str | None = None
        self._foreach: Callable[[DataFrame, int], None] | None = None
        self._partition_by: list[str] = []

    def format(self, source: str) -> DataStreamWriter:
        """Specifies the underlying output data source."""
        self._format = source
        return self

    def outputMode(  # noqa: N802 — PySpark method name
        self,
        outputMode: str,  # noqa: N803 — PySpark arg name
    ) -> DataStreamWriter:
        """Specifies how data of a streaming DataFrame/Dataset is written to a streaming sink.

        Options include:

        * `append`: Only the new rows in the streaming DataFrame/Dataset will be written to
           the sink
        * `complete`: All the rows in the streaming DataFrame/Dataset will be written to the sink
           every time these are some updates
        * `update`: only the rows that were updated in the streaming DataFrame/Dataset will be
           written to the sink every time there are some updates. If the query doesn't contain
           aggregations, it will be equivalent to `append` mode.
        """
        if not outputMode or type(outputMode) is not str or len(outputMode.strip()) == 0:
            raise PySparkValueError(
                errorClass="VALUE_NOT_NON_EMPTY_STR",
                messageParameters={
                    "arg_name": "outputMode",
                    "arg_value": str(outputMode),
                },
            )
        _native.check_output_mode(outputMode)
        self._output_mode = outputMode
        return self

    def option(self, key: str, value: object) -> DataStreamWriter:
        """Adds an output option for the underlying data source."""
        _store_option(self._options, key, value)
        return self

    def options(self, **options: object) -> DataStreamWriter:
        """Adds output options for the underlying data source."""
        for key, value in options.items():
            self.option(key, value)
        return self

    def queryName(  # noqa: N802 — PySpark method name
        self,
        queryName: str,  # noqa: N803 — PySpark arg name
    ) -> DataStreamWriter:
        """Specifies the name of the StreamingQuery that can be started with start.

        Parameters
        ----------
        queryName : str
            unique name for the query
        """
        if not queryName or type(queryName) is not str or len(queryName.strip()) == 0:
            raise PySparkValueError(
                errorClass="VALUE_NOT_NON_EMPTY_STR",
                messageParameters={
                    "arg_name": "queryName",
                    "arg_value": str(queryName),
                },
            )
        self._query_name = queryName
        return self

    def trigger(
        self,
        *,
        processingTime: str | None = None,  # noqa: N803 — PySpark kwarg name
        once: bool | None = None,
        continuous: str | None = None,
        availableNow: bool | None = None,  # noqa: N803 — PySpark kwarg name
    ) -> DataStreamWriter:
        """Set the trigger for the stream query.

        If this is not set it will run the query as fast as possible, which is equivalent
        to setting the trigger to ``processingTime='0 seconds'``.

        Parameters
        ----------
        processingTime : str, optional
            a processing time interval as a string, e.g. '5 seconds', '1 minute'.
            Set a trigger that runs a microbatch query periodically based on the
            processing time. Only one trigger can be set.
        once : bool, optional
            if set to True, set a trigger that processes only one batch of data in a
            streaming query then terminates the query. Only one trigger can be set.
        continuous : str, optional
            a time interval as a string, e.g. '5 seconds', '1 minute'.
            Set a trigger that runs a continuous query with a given checkpoint
            interval. Only one trigger can be set.
        availableNow : bool, optional
            if set to True, set a trigger that processes all available data in multiple
            batches then terminates the query. Only one trigger can be set.
        """
        params = [processingTime, once, continuous, availableNow]
        if params.count(None) != 3:
            raise PySparkValueError(
                errorClass="ONLY_ALLOW_SINGLE_TRIGGER",
                messageParameters={},
            )
        if processingTime is not None:
            if type(processingTime) is not str or len(processingTime.strip()) == 0:
                raise PySparkValueError(
                    errorClass="VALUE_NOT_NON_EMPTY_STR",
                    messageParameters={
                        "arg_name": "processingTime",
                        "arg_value": str(processingTime),
                    },
                )
            _native.check_trigger_interval(processingTime.strip())
            self._trigger_kind = _PROCESSING_TIME_TRIGGER
            self._trigger_interval = processingTime.strip()
        elif once is not None:
            if once is not True:
                raise PySparkValueError(
                    errorClass="VALUE_NOT_TRUE",
                    messageParameters={"arg_name": "once", "arg_value": str(once)},
                )
            self._trigger_kind = _ONCE_TRIGGER
            self._trigger_interval = None
        elif continuous is not None:
            if type(continuous) is not str or len(continuous.strip()) == 0:
                raise PySparkValueError(
                    errorClass="VALUE_NOT_NON_EMPTY_STR",
                    messageParameters={
                        "arg_name": "continuous",
                        "arg_value": str(continuous),
                    },
                )
            _native.check_trigger_interval(continuous.strip())
            self._trigger_kind = _CONTINUOUS_TRIGGER
            self._trigger_interval = continuous.strip()
        else:
            if availableNow is not True:
                raise PySparkValueError(
                    errorClass="VALUE_NOT_TRUE",
                    messageParameters={
                        "arg_name": "availableNow",
                        "arg_value": str(availableNow),
                    },
                )
            self._trigger_kind = _AVAILABLE_NOW_TRIGGER
            self._trigger_interval = None
        return self

    def foreachBatch(  # noqa: N802 — PySpark method name
        self, func: Callable[[DataFrame, int], None]
    ) -> DataStreamWriter:
        """Sets the output of the streaming query to be processed using the provided function.

        In every micro-batch, the provided function will be called with (i) the output rows
        as a DataFrame and (ii) the batch identifier.
        """
        self._foreach = func
        return self

    def _apply_start_kwargs(
        self,
        outputMode: str | None,  # noqa: N803 — PySpark kwarg name
        partitionBy: str | list[str] | None,  # noqa: N803 — PySpark kwarg name
        format: str | None,
        queryName: str | None,  # noqa: N803 — PySpark kwarg name
        options: dict[str, Any],
    ) -> None:
        self.options(**options)
        if outputMode is not None:
            self.outputMode(outputMode)
        if partitionBy is not None:
            if isinstance(partitionBy, (list, tuple)):
                self._partition_by = list(partitionBy)
            else:
                self._partition_by = [partitionBy]
        if format is not None:
            self.format(format)
        if queryName is not None:
            self.queryName(queryName)

    def _start_checks(self) -> None:
        if self._trigger_kind == _CONTINUOUS_TRIGGER:
            _not_implemented("trigger(continuous)")

    def start(
        self,
        path: str | None = None,
        format: str | None = None,
        outputMode: str | None = None,  # noqa: N803 — PySpark kwarg name
        partitionBy: str | list[str] | None = None,  # noqa: N803 — PySpark kwarg name
        queryName: str | None = None,  # noqa: N803 — PySpark kwarg name
        **options: object,
    ) -> StreamingQuery:
        """Streams the contents of the DataFrame to a data source.

        The data source is specified by the ``format`` and a set of ``options``.
        If ``format`` is not specified, the default data source configured by
        ``spark.sql.sources.default`` will be used.

        Parameters
        ----------
        path : str, optional
            the path in a Hadoop supported file system
        format : str, optional
            the format used to save
        outputMode : str, optional
            specifies how data of a streaming DataFrame/Dataset is written to a
            streaming sink.
        partitionBy : str or list, optional
            names of partitioning columns
        queryName : str, optional
            unique name for the query
        **options : dict
            All other string options. You may want to provide a `checkpointLocation`
            for most streams, however it is not required for a `memory` stream.
        """
        self._apply_start_kwargs(outputMode, partitionBy, format, queryName, options)
        if self._foreach is None:
            _refuse_format("writeStream", self._format)
        self._start_checks()
        self._frame._ensure_alive()
        session = self._frame.sparkSession
        inner_session = session._ensure_alive()
        handle = _native.start_stream(
            inner_session,
            self._frame._inner,
            self._trigger_kind or _DEFAULT_TRIGGER,
            self._trigger_interval,
            _without_none_values(self._options),
            _streaming_confs(session),
            self._query_name,
            self._foreach,
            path,
            self._partition_by,
            self._output_mode,
        )
        return StreamingQuery(handle)

    def toTable(  # noqa: N802 — PySpark method name
        self,
        tableName: str,  # noqa: N803 — PySpark kwarg name
        format: str | None = None,
        outputMode: str | None = None,  # noqa: N803 — PySpark kwarg name
        partitionBy: str | list[str] | None = None,  # noqa: N803 — PySpark kwarg name
        queryName: str | None = None,  # noqa: N803 — PySpark kwarg name
        **options: object,
    ) -> StreamingQuery:
        """Starts the execution of the streaming query to the given table.

        Parameters
        ----------
        tableName : str
            string, for the name of the table.
        format : str, optional
            the format used to save.
        outputMode : str, optional
            specifies how data of a streaming DataFrame/Dataset is written to a
            streaming sink.
        partitionBy : str or list, optional
            names of partitioning columns
        queryName : str, optional
            unique name for the query
        **options : dict
            All other string options. You may want to provide a `checkpointLocation`.
        """
        self._apply_start_kwargs(outputMode, partitionBy, format, queryName, options)
        self._start_checks()
        self._frame._ensure_alive()
        session = self._frame.sparkSession
        inner_session = session._ensure_alive()
        handle = _native.to_table_stream(
            inner_session,
            self._frame._inner,
            tableName,
            self._trigger_kind or _DEFAULT_TRIGGER,
            self._trigger_interval,
            _without_none_values(self._options),
            _streaming_confs(session),
            self._query_name,
            self._partition_by,
            self._output_mode,
        )
        return StreamingQuery(handle)
