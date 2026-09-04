"""SQL-HARDEN-1 runners - execute one cutover program on a duck-typed session.

pins: sql-harden-1-cutover-shapes/C-001, C-002
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, NamedTuple

import pyarrow as pa
from _sql_harden_cutover_programs import _NAMESPACE, _Program, mor_properties

_STAGING = "staging_view"
_EXCERPT = 76

_ROW_PROBE = (
    "SELECT id, CAST(amount AS STRING) AS amount, units, note, part FROM {t} ORDER BY id, part"
)
_SNAP_PROBE = "SELECT operation FROM {t}.snapshots ORDER BY committed_at"
_DEL_PROBE = "SELECT content, file_format FROM {t}.delete_files ORDER BY 1, 2"


_FCT_SQL = (
    "SELECT /*+ BROADCAST(Provider, DD) */ "
    "DD.calendar_date, DD.year_month, DD.year, DD.year_quarter, "
    "Surveys.survey_id, Surveys.patient_visit_id, "
    "Surveys.gene_prissy_score, Surveys.experience_score, "
    "CONCAT(Provider.provider_first_name, ' ', Provider.provider_last_name) AS provider_name, "
    "Appointment.appointment_datetime, Appointment.appointment_clinic_id AS clinic_id, "
    "PatientVisit.provider_seen_time, PatientVisit.checkin_time, "
    "CAST((unix_timestamp(PatientVisit.provider_seen_time) "
    "- unix_timestamp(PatientVisit.checkin_time)) / 60 AS INT) AS wait_time_minutes "
    "FROM {survey} Surveys "
    "INNER JOIN {visit} PatientVisit "
    "ON PatientVisit.patient_visit_id = Surveys.patient_visit_id "
    "INNER JOIN {provider} Provider "
    "ON Provider.provider_id = PatientVisit.pv_provider_id "
    "INNER JOIN {appointment} Appointment "
    "ON Appointment.appointment_id = PatientVisit.pv_appointment_id "
    "INNER JOIN {dates} DD "
    "ON DD.calendar_date = DATE(Appointment.appointment_datetime)"
)

_AGG_SQL = (
    "SELECT /*+ BROADCAST(DD) */ "
    "GFS.clinic_id, GFS.calendar_date, DD.day_name AS day_of_week, "
    "ROUND(AVG(GFS.gene_prissy_score), 2) AS avg_gene_prissy_score, "
    "ROUND(AVG(GFS.experience_score), 2) AS avg_experience_score, "
    "ROUND(AVG(GFS.wait_time_minutes), 2) AS avg_wait_time_minutes, "
    "COUNT(*) AS num_surveys, "
    "COUNT(DISTINCT GFS.patient_visit_id) AS num_patient_visits "
    "FROM {fct} GFS INNER JOIN {dates} DD ON DD.calendar_date = GFS.calendar_date "
    "GROUP BY GFS.clinic_id, GFS.calendar_date, DD.day_name"
)


class _Ctx(NamedTuple):
    """Duck-typed session plus the names and warehouse one program interpolates."""

    session: Any
    functions: Any
    types: Any
    window: Any
    names: dict[str, str]
    warehouse: Path
    parquet: Path
    stem: str
    format_version: int
    props: str


def sql_arrow(session: Any, text: str) -> pa.Table:
    """Run SQL on Spark or repark and return an Arrow table."""
    result = session.sql(text)
    if hasattr(result, "to_arrow"):
        return result.to_arrow()
    return result.toArrow()


def _cell(value: Any) -> Any:
    """One Arrow value rendered as a comparable Python scalar."""
    if isinstance(value, bool | int | float | str) or value is None:
        return value
    return str(value)


def _rows(table: pa.Table) -> list[list[Any]]:
    """Every row of an Arrow result as a sorted list of comparable scalars."""
    names = table.column_names
    rows = [[_cell(row[name]) for name in names] for row in table.to_pylist()]
    rows.sort(key=repr)
    return rows


def _schema_cell(table: pa.Table) -> list[list[Any]]:
    """Arrow schema as ``[name, type, nullable]`` triples."""
    return [[field.name, str(field.type), field.nullable] for field in table.schema]


def _excerpt(error: BaseException) -> str:
    """The first line of an engine error, trimmed to a length one source line holds."""
    return str(error).strip().splitlines()[0][:_EXCERPT]


def _try_sql(ctx: _Ctx, text: str, *, capture: bool = False) -> list[Any]:
    """Run one statement and return ``[OK, rows_or_None]`` or ``[ERROR, excerpt]``."""
    try:
        table = sql_arrow(ctx.session, text)
    except Exception as error:
        return ["ERROR", _excerpt(error)]
    if capture:
        return ["OK", _rows(table)]
    return ["OK", None]


def _try_probe(ctx: _Ctx, text: str) -> list[Any]:
    """Run one probe and return ``[OK, rows]`` or ``[ERROR, excerpt]``."""
    try:
        return ["OK", _rows(sql_arrow(ctx.session, text))]
    except Exception as error:
        return ["ERROR", _excerpt(error)]


def _schema_probe(ctx: _Ctx, table: str, columns: str) -> list[Any]:
    """Collect ``columns`` from ``table`` and return the Arrow schema cell."""
    try:
        got = sql_arrow(ctx.session, f"SELECT {columns} FROM {table} LIMIT 1")
    except Exception as error:
        return ["ERROR", _excerpt(error)]
    return ["OK", _schema_cell(got)]


def _latest_metadata(warehouse: Path, stem: str) -> str:
    """The newest metadata pointer of one table under a warehouse."""
    candidates = sorted(
        warehouse.glob(f"*/{stem}/metadata/*.metadata.json"),
        key=lambda path: (path.stat().st_mtime, path.name),
    )
    return str(candidates[-1]) if candidates else ""


def _metadata_facts(warehouse: Path, stem: str) -> Any:
    """Engine-comparable facts from the table metadata JSON."""
    pointer = _latest_metadata(warehouse, stem)
    if not pointer:
        return ["ABSENT"]
    document = json.loads(Path(pointer).read_text(encoding="utf-8"))
    schema = next(
        (
            entry
            for entry in document["schemas"]
            if entry["schema-id"] == document["current-schema-id"]
        ),
        {"fields": []},
    )
    write_properties = sorted(
        (key, value)
        for key, value in document.get("properties", {}).items()
        if key.startswith("write.") or key == "format-version"
    )
    schema_fields = []
    for field in schema.get("fields", []):
        field_type = field["type"]
        if isinstance(field_type, str):
            field_type = field_type.replace(" ", "")
        schema_fields.append([field["name"], field_type, field["required"]])
    return [
        ["format-version", document.get("format-version")],
        ["schema", schema_fields],
        ["write-properties", write_properties],
        ["next-row-id", document.get("next-row-id")],
    ]


def apply_dedup(frame: Any, functions: Any, types: Any, window_mod: Any) -> Any:
    """Keep the newest row per id, then coalesce+cast amount/units/note."""
    window = window_mod.partitionBy("id").orderBy(functions.col("ingestion_timestamp").desc())
    return (
        frame.withColumn("row_num", functions.row_number().over(window))
        .filter(functions.col("row_num") == 1)
        .drop("row_num")
        .withColumn(
            "units",
            functions.coalesce(functions.col("units"), functions.lit(0)).cast(types.IntegerType()),
        )
        .withColumn(
            "amount",
            functions.coalesce(functions.col("amount"), functions.lit(0.0)).cast(
                types.DecimalType(10, 4)
            ),
        )
        .withColumn(
            "note",
            functions.coalesce(functions.col("note"), functions.lit("unknown")).cast(
                types.StringType()
            ),
        )
    )


def _load_staging(ctx: _Ctx) -> None:
    """Read the bronze parquet into ``staging_view``."""
    frame = ctx.session.read.format("parquet").load(str(ctx.parquet))
    frame.createOrReplaceTempView(_STAGING)


def _load_staging_deduped(ctx: _Ctx) -> None:
    """Read bronze, apply the S3 transform, and register ``staging_view``."""
    frame = ctx.session.read.format("parquet").load(str(ctx.parquet))
    apply_dedup(frame, ctx.functions, ctx.types, ctx.window).createOrReplaceTempView(_STAGING)


def _ctas_sql(ctx: _Ctx) -> str:
    """The pipeline CTAS-if-fresh statement."""
    return (
        f"CREATE TABLE IF NOT EXISTS {ctx.names['t']} USING iceberg "
        f"TBLPROPERTIES ({ctx.props}) AS SELECT * FROM {_STAGING}"
    )


def _merge_sql(ctx: _Ctx) -> str:
    """The pipeline MERGE UPDATE SET * / INSERT * statement."""
    target = ctx.names["t"]
    return (
        f"MERGE INTO {target} AS Target USING {_STAGING} AS Source "
        f"ON Target.id = Source.id "
        "WHEN MATCHED THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    )


def _table_probes(ctx: _Ctx) -> list[list[Any]]:
    """Row set, schema, snapshots, delete-file kinds, files, metadata facts."""
    target = ctx.names["t"]
    probes = [
        _try_probe(ctx, _ROW_PROBE.format(t=target)),
        _schema_probe(ctx, target, "id, amount, units, note, part"),
        _try_probe(ctx, _SNAP_PROBE.format(t=target)),
        _try_probe(ctx, _DEL_PROBE.format(t=target)),
        ["META", _metadata_facts(ctx.warehouse, ctx.stem)],
    ]
    return probes


def run_ctas(ctx: _Ctx) -> dict[str, Any]:
    """S1 / S7: parquet temp-view CTAS IF NOT EXISTS with pipeline TBLPROPERTIES."""
    _load_staging(ctx)
    statements = [_try_sql(ctx, _ctas_sql(ctx))]
    return {"statements": statements, "probes": _table_probes(ctx)}


def run_merge(ctx: _Ctx) -> dict[str, Any]:
    """S2 / S7: deduped staging, CTAS, then MERGE twice; the second pass must keep the row set."""
    _load_staging_deduped(ctx)
    statements = [
        _try_sql(ctx, _ctas_sql(ctx)),
        _try_sql(ctx, _merge_sql(ctx)),
    ]
    after_first = _try_probe(ctx, _ROW_PROBE.format(t=ctx.names["t"]))
    statements.append(_try_sql(ctx, _merge_sql(ctx)))
    after_second = _try_probe(ctx, _ROW_PROBE.format(t=ctx.names["t"]))
    probes = _table_probes(ctx)
    probes.append(["IDEM", [after_first, after_second, after_first == after_second]])
    return {"statements": statements, "probes": probes}


def run_dedup(ctx: _Ctx) -> dict[str, Any]:
    """S3: row_number dedup then coalesce+cast including Decimal(10,4) and IntegerType."""
    frame = ctx.session.read.format("parquet").load(str(ctx.parquet))
    try:
        deduped = apply_dedup(frame, ctx.functions, ctx.types, ctx.window)
        if hasattr(deduped, "to_arrow"):
            table = deduped.select("id", "amount", "units", "note", "part").to_arrow()
        else:
            table = deduped.select("id", "amount", "units", "note", "part").toArrow()
    except Exception as error:
        return {
            "statements": [["ERROR", _excerpt(error)]],
            "probes": [["ERROR", _excerpt(error)]],
        }
    rows = _rows(table)
    return {
        "statements": [["OK", rows]],
        "probes": [["OK", rows], ["OK", _schema_cell(table)]],
    }


def run_overwrite(ctx: _Ctx) -> dict[str, Any]:
    """S4 / S7: partitioned table then writeTo.overwritePartitions on one partition."""
    target = ctx.names["t"]
    create = (
        f"CREATE TABLE {target} (id STRING, note STRING, part INT) USING iceberg "
        f"PARTITIONED BY (part) TBLPROPERTIES ({ctx.props})"
    )
    seed = f"INSERT INTO {target} VALUES ('A', 'x', 10), ('B', 'y', 20)"
    statements = [_try_sql(ctx, create), _try_sql(ctx, seed)]
    try:
        source = ctx.session.sql("SELECT * FROM (VALUES ('C', 'z', 10)) AS s(id, note, part)")
        source.writeTo(target).overwritePartitions()
        statements.append(["OK", None])
    except Exception as error:
        statements.append(["ERROR", _excerpt(error)])
    row_sql = f"SELECT id, note, part FROM {target} ORDER BY id"
    probes = [
        _try_probe(ctx, row_sql),
        _schema_probe(ctx, target, "id, note, part"),
        _try_probe(ctx, _SNAP_PROBE.format(t=target)),
        ["META", _metadata_facts(ctx.warehouse, ctx.stem)],
    ]
    return {"statements": statements, "probes": probes}


def run_maint(ctx: _Ctx) -> dict[str, Any]:
    """S5: pipeline CALL argument shapes after a CTAS and two extra appends."""
    _load_staging_deduped(ctx)
    catalog = ctx.names["c"]
    ident = ctx.names["q"]
    extra_c = (
        f"INSERT INTO {ctx.names['t']} SELECT 'C' AS id, ingestion_timestamp, amount, "
        f"units, note, 30 AS part FROM {_STAGING} WHERE id = 'B'"
    )
    extra_d = extra_c.replace("'C'", "'D'").replace("30 AS part", "40 AS part")
    statements = [
        _try_sql(ctx, _ctas_sql(ctx)),
        _try_sql(ctx, extra_c),
        _try_sql(ctx, extra_d),
        _try_sql(
            ctx,
            f"CALL {catalog}.system.expire_snapshots("
            f"table => '{ident}', older_than => TIMESTAMP '2999-01-01 00:00:00', "
            "retain_last => 3)",
            capture=True,
        ),
        _try_sql(
            ctx,
            f"CALL {catalog}.system.rewrite_data_files(table => '{ident}', strategy => 'binpack')",
            capture=True,
        ),
        _try_sql(
            ctx,
            f"CALL {catalog}.system.remove_orphan_files("
            f"table => '{ident}', older_than => TIMESTAMP '2020-01-01 00:00:00')",
            capture=True,
        ),
        _try_sql(
            ctx,
            f"CALL {catalog}.system.rewrite_position_delete_files(table => '{ident}')",
            capture=True,
        ),
    ]
    return {"statements": statements, "probes": _table_probes(ctx)}


GOLD_CREATED_BEFORE_FCT = ("survey", "visit", "provider", "appointment", "dates")
GOLD_CREATED_AT_FCT = ("fct", "agg")


def _seed_gold_sql(names: dict[str, str], props: str) -> list[str]:
    """DDL + VALUES for the five silver-shaped tables the gold models join."""
    survey = names["survey"]
    visit = names["visit"]
    provider = names["provider"]
    appointment = names["appointment"]
    dates = names["dates"]
    return [
        (
            f"CREATE TABLE {survey} (survey_id STRING, patient_visit_id STRING, "
            f"gene_prissy_score INT, experience_score INT) USING iceberg "
            f"TBLPROPERTIES ({props})"
        ),
        f"INSERT INTO {survey} VALUES ('s1', 'v1', 8, 9), ('s2', 'v2', 6, 7)",
        (
            f"CREATE TABLE {visit} (patient_visit_id STRING, pv_appointment_id STRING, "
            f"pv_provider_id INT, checkin_time TIMESTAMP, provider_seen_time TIMESTAMP) "
            f"USING iceberg TBLPROPERTIES ({props})"
        ),
        (
            f"INSERT INTO {visit} VALUES "
            f"('v1', 'a1', 1, TIMESTAMP '2026-01-01 10:00:00', "
            f"TIMESTAMP '2026-01-01 10:15:00'), "
            f"('v2', 'a2', 1, TIMESTAMP '2026-01-02 10:00:00', "
            f"TIMESTAMP '2026-01-02 10:40:00')"
        ),
        (
            f"CREATE TABLE {provider} (provider_id INT, provider_first_name STRING, "
            f"provider_last_name STRING) USING iceberg TBLPROPERTIES ({props})"
        ),
        f"INSERT INTO {provider} VALUES (1, 'Jamie', 'Smith')",
        (
            f"CREATE TABLE {appointment} (appointment_id STRING, appointment_datetime TIMESTAMP, "
            f"appointment_clinic_id INT) USING iceberg TBLPROPERTIES ({props})"
        ),
        (
            f"INSERT INTO {appointment} VALUES "
            f"('a1', TIMESTAMP '2026-01-01 09:00:00', 10), "
            f"('a2', TIMESTAMP '2026-01-02 09:00:00', 20)"
        ),
        (
            f"CREATE TABLE {dates} (calendar_date DATE, year_month INT, year INT, "
            f"year_quarter STRING, day_name STRING) USING iceberg TBLPROPERTIES ({props})"
        ),
        (
            f"INSERT INTO {dates} VALUES "
            f"(DATE '2026-01-01', 202601, 2026, '2026Q1', 'Thursday'), "
            f"(DATE '2026-01-02', 202601, 2026, '2026Q1', 'Friday')"
        ),
    ]


def run_gold(ctx: _Ctx) -> dict[str, Any]:
    """S6: gold join + aggregate Iceberg tables, then a second full-refresh overwrite."""
    statements = [_try_sql(ctx, text) for text in _seed_gold_sql(ctx.names, ctx.props)]
    fct_select = _FCT_SQL.format(
        survey=ctx.names["survey"],
        visit=ctx.names["visit"],
        provider=ctx.names["provider"],
        appointment=ctx.names["appointment"],
        dates=ctx.names["dates"],
    )
    fct = ctx.names["fct"]
    agg = ctx.names["agg"]
    statements.append(
        _try_sql(
            ctx,
            f"CREATE TABLE {fct} USING iceberg TBLPROPERTIES ({ctx.props}) AS {fct_select}",
        )
    )
    agg_select = _AGG_SQL.format(fct=fct, dates=ctx.names["dates"])
    statements.append(
        _try_sql(
            ctx,
            f"CREATE TABLE {agg} USING iceberg TBLPROPERTIES ({ctx.props}) AS {agg_select}",
        )
    )
    statements.append(
        _try_sql(ctx, f"INSERT INTO {ctx.names['survey']} VALUES ('s3', 'v1', 10, 10)")
    )
    statements.append(_try_sql(ctx, f"INSERT OVERWRITE {fct} {fct_select}"))
    fct_rows = "SELECT survey_id, clinic_id, wait_time_minutes FROM {t} ORDER BY survey_id"
    agg_rows = (
        "SELECT clinic_id, calendar_date, day_of_week, num_surveys, num_patient_visits "
        "FROM {t} ORDER BY clinic_id"
    )
    probes = [
        _try_probe(ctx, fct_rows.format(t=fct)),
        _schema_probe(ctx, fct, "survey_id, clinic_id, wait_time_minutes, provider_name"),
        _try_probe(ctx, agg_rows.format(t=agg)),
        _schema_probe(ctx, agg, "clinic_id, calendar_date, day_of_week, num_surveys"),
        ["META", _metadata_facts(ctx.warehouse, ctx.stem + "_fct")],
    ]
    return {"statements": statements, "probes": probes}


_RUNNERS = {
    "ctas": run_ctas,
    "merge": run_merge,
    "dedup": run_dedup,
    "overwrite": run_overwrite,
    "maint": run_maint,
    "gold": run_gold,
}


def make_names(
    catalog: str, stem: str, qualified_call: bool, namespace: str = _NAMESPACE
) -> dict[str, str]:
    """Catalog/namespace names one program interpolates."""
    target = f"{catalog}.{namespace}.{stem}"
    ident = target if qualified_call else f"{namespace}.{stem}"
    return {
        "t": target,
        "c": catalog,
        "q": ident,
        "survey": f"{catalog}.{namespace}.{stem}_survey",
        "visit": f"{catalog}.{namespace}.{stem}_visit",
        "provider": f"{catalog}.{namespace}.{stem}_provider",
        "appointment": f"{catalog}.{namespace}.{stem}_appointment",
        "dates": f"{catalog}.{namespace}.{stem}_dates",
        "fct": f"{catalog}.{namespace}.{stem}_fct",
        "agg": f"{catalog}.{namespace}.{stem}_agg",
    }


def program_sql_texts(program: _Program, names: dict[str, str], props: str) -> list[str]:
    """Every SQL statement one program interpolates, for the namespace pin."""
    target = names["t"]
    catalog = names["c"]
    ident = names["q"]
    texts: list[str] = []
    if program.runner in {"ctas", "merge", "maint"}:
        texts.append(
            f"CREATE TABLE IF NOT EXISTS {target} USING iceberg "
            f"TBLPROPERTIES ({props}) AS SELECT * FROM {_STAGING}"
        )
    if program.runner == "merge":
        texts.append(
            f"MERGE INTO {target} AS Target USING {_STAGING} AS Source "
            f"ON Target.id = Source.id "
            "WHEN MATCHED THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT *"
        )
    if program.runner == "overwrite":
        texts.append(
            f"CREATE TABLE {target} (id STRING, note STRING, part INT) USING iceberg "
            f"PARTITIONED BY (part) TBLPROPERTIES ({props})"
        )
        texts.append(f"INSERT INTO {target} VALUES ('A', 'x', 10), ('B', 'y', 20)")
    if program.runner == "maint":
        texts.append(
            f"INSERT INTO {target} SELECT 'C' AS id, ingestion_timestamp, amount, "
            f"units, note, 30 AS part FROM {_STAGING} WHERE id = 'B'"
        )
        texts.append(
            f"CALL {catalog}.system.expire_snapshots("
            f"table => '{ident}', older_than => TIMESTAMP '2999-01-01 00:00:00', "
            "retain_last => 3)"
        )
        texts.append(
            f"CALL {catalog}.system.rewrite_data_files(table => '{ident}', strategy => 'binpack')"
        )
        texts.append(
            f"CALL {catalog}.system.remove_orphan_files("
            f"table => '{ident}', older_than => TIMESTAMP '2020-01-01 00:00:00')"
        )
        texts.append(f"CALL {catalog}.system.rewrite_position_delete_files(table => '{ident}')")
    if program.runner == "gold":
        texts.extend(_seed_gold_sql(names, props))
        fct_select = _FCT_SQL.format(
            survey=names["survey"],
            visit=names["visit"],
            provider=names["provider"],
            appointment=names["appointment"],
            dates=names["dates"],
        )
        texts.append(
            f"CREATE TABLE {names['fct']} USING iceberg TBLPROPERTIES ({props}) AS {fct_select}"
        )
        agg_select = _AGG_SQL.format(fct=names["fct"], dates=names["dates"])
        texts.append(
            f"CREATE TABLE {names['agg']} USING iceberg TBLPROPERTIES ({props}) AS {agg_select}"
        )
        texts.append(f"INSERT INTO {names['survey']} VALUES ('s3', 'v1', 10, 10)")
        texts.append(f"INSERT OVERWRITE {names['fct']} {fct_select}")
    return texts


def run_program(
    program: _Program,
    session: Any,
    warehouse: Path,
    *,
    catalog: str,
    functions: Any,
    types: Any,
    window: Any,
    qualified_call: bool,
    parquet: Path,
    stem: str | None = None,
    namespace: str = _NAMESPACE,
) -> dict[str, Any]:
    """Execute one inventory program and return its statement and probe cells."""
    stem = stem if stem is not None else program.name.replace("-", "_")
    ctx = _Ctx(
        session=session,
        functions=functions,
        types=types,
        window=window,
        names=make_names(catalog, stem, qualified_call, namespace),
        warehouse=warehouse,
        parquet=parquet,
        stem=stem,
        format_version=program.format_version,
        props=mor_properties(program.format_version),
    )
    return _RUNNERS[program.runner](ctx)


def as_golden(outcome: dict[str, Any]) -> dict[str, Any]:
    """One outcome in the golden's shape: tuples flattened to lists."""
    return json.loads(json.dumps(outcome))


def without_meta(outcome: dict[str, Any]) -> dict[str, Any]:
    """Drop the warehouse-local META probe so a Glue/S3 Tables run can compare."""
    return {
        "statements": outcome["statements"],
        "probes": [cell for cell in outcome["probes"] if cell[0] != "META"],
    }
