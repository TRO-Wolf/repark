from __future__ import annotations

import argparse
import contextlib
import datetime
import decimal
import io
import json
import os
import re
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any


def _encode_value(value: Any, engine: str, zone_name: str) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return ["bool", value]
    if isinstance(value, int):
        return ["int", value]
    if isinstance(value, float):
        return ["float", repr(value)]
    if isinstance(value, decimal.Decimal):
        return ["decimal", str(value)]
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None or engine == "spark":
            utc = value.astimezone(datetime.UTC)
        else:
            import zoneinfo

            utc = value.replace(tzinfo=zoneinfo.ZoneInfo(zone_name)).astimezone(datetime.UTC)
        epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)
        delta = utc - epoch
        micros = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
        return ["ts_us", micros]
    if isinstance(value, datetime.date):
        return ["date", value.isoformat()]
    if isinstance(value, (bytes, bytearray)):
        return ["bytes", value.hex()]
    if isinstance(value, str):
        return ["str", value]
    return ["other", type(value).__name__, str(value)]


def _encode_rows(rows: list[Any], engine: str, zone_name: str) -> list[list[Any]]:
    encoded = [[_encode_value(cell, engine, zone_name) for cell in row] for row in rows]
    encoded.sort(key=lambda item: json.dumps(item, sort_keys=True))
    return encoded


def _encode_error(exc: BaseException) -> dict[str, Any]:
    text = str(exc)
    first = text.splitlines()[0] if text else ""
    cond_match = re.search(r"\[([A-Z0-9_.]+)\]", text)
    cond = cond_match.group(1) if cond_match else None
    return {
        "status": "err",
        "type": type(exc).__name__,
        "condition": cond,
        "message": first[:240],
    }


def _explain_text(df: Any) -> str:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(True)
    return buf.getvalue()


def _count_files_scanned_spark(df: Any, sf: Any) -> dict[str, Any]:
    input_files = sorted(df.inputFiles())
    explained = _explain_text(df)
    part_filters = re.findall(r"PartitionFilters:\s*\[[^\]]*\]", explained)
    try:
        read_files = sorted(
            Path(r[0]).parent.name + "/" + Path(r[0]).name
            for r in df.select(sf.input_file_name()).distinct().collect()
        )
    except Exception:
        read_files = []
    return {
        "input_files_count": len(input_files),
        "input_files_basenames": [Path(p).parent.name + "/" + Path(p).name for p in input_files],
        "distinct_read_files": read_files,
        "partition_filters": part_filters[:2],
    }


def _count_files_scanned_repark(df: Any) -> dict[str, Any]:
    explained = _explain_text(df)
    matches = re.findall(r"file_groups=\{[^}]+\}", explained)
    part_files = re.findall(r"[^/,\s{}]+/part-[^,\s{}\]]+", explained)
    return {
        "file_groups": matches[:2],
        "scanned_parts": sorted(set(part_files)),
        "scanned_parts_count": len(set(part_files)),
    }


def _read_path(
    spark: Any,
    reader_kind: str,
    path: Any,
    options: dict[str, str] | None = None,
) -> Any:
    rdr = spark.read
    if options:
        for k, v in options.items():
            rdr = rdr.option(k, v)
    if reader_kind == "parquet":
        return rdr.parquet(path)
    if reader_kind == "parquet_load":
        return rdr.format("parquet").load(path)
    if reader_kind == "csv_header":
        return rdr.option("header", "true").csv(path)
    if reader_kind == "csv_noheader":
        return rdr.option("header", "false").csv(path)
    if reader_kind == "json":
        return rdr.json(path)
    if reader_kind == "orc":
        return rdr.orc(path)
    if reader_kind == "text":
        return rdr.text(path)
    raise ValueError(reader_kind)


def _fmt_dir_for_reader(reader_kind: str) -> str:
    if reader_kind in {"parquet", "parquet_load"}:
        return "parquet"
    return reader_kind


def _ext_for_reader(reader_kind: str) -> str:
    mapping = {
        "parquet": ".parquet",
        "parquet_load": ".parquet",
        "csv_header": ".csv",
        "csv_noheader": ".csv",
        "json": ".json",
        "orc": ".orc",
        "text": ".txt",
    }
    return mapping[reader_kind]


def _capture_read(
    spark: Any,
    engine: str,
    reader_kind: str,
    path: Any,
    zone_name: str,
    options: dict[str, str] | None = None,
) -> dict[str, Any]:
    try:
        df = _read_path(spark, reader_kind, path, options)
        schema_str = df.schema.simpleString()
        dtypes = [[name, dtype] for name, dtype in df.dtypes]
        rows = _encode_rows(df.collect(), engine, zone_name)
        return {
            "status": "ok",
            "schema": schema_str,
            "dtypes": dtypes,
            "rows": rows,
        }
    except Exception as exc:
        return _encode_error(exc)


def _build_session(engine: str, warehouse: Path) -> tuple[Any, Any]:
    if engine == "spark":
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as sf

        jar = os.environ["SPARK_ICEBERG_JAR"]
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("partdisc-probe")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", "true")
            .config("spark.jars", jar)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.ice.type", "hadoop")
            .config("spark.sql.catalog.ice.warehouse", str(warehouse))
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        return spark, sf
    from repark.spark import functions as sf
    from repark.spark.session import ReparkSession

    spark = (
        ReparkSession.builder.appName("partdisc-probe")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(warehouse))
        .config("repark.local_fs.allow_ddl", "true")
        .getOrCreate()
    )
    return spark, sf


def _set_conf(
    spark: Any,
    *,
    zone: str = "UTC",
    ansi: str = "true",
    case_sensitive: str = "false",
    type_inference: str = "true",
) -> None:
    spark.conf.set("spark.sql.session.timeZone", zone)
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    spark.conf.set("spark.sql.caseSensitive", case_sensitive)
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", type_inference)


def _run_queries(
    spark: Any,
    sf: Any,
    engine: str,
    reader_kind: str,
    path: Path,
    zone_name: str,
    ansi: str,
    scratch: Path,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    _set_conf(spark, zone=zone_name, ansi=ansi)

    def _run(name: str, fn: Callable[[], dict[str, Any]]) -> None:
        try:
            out[name] = fn()
        except Exception as exc:
            out[name] = _encode_error(exc)

    _run(
        "schema_dtypes_collect",
        lambda: _capture_read(spark, engine, reader_kind, str(path), zone_name),
    )

    def _where_eq_int() -> dict[str, Any]:
        df = _read_path(spark, reader_kind, str(path)).where("p = 2")
        rows = _encode_rows(df.collect(), engine, zone_name)
        scan_info = (
            _count_files_scanned_spark(df, sf)
            if engine == "spark"
            else _count_files_scanned_repark(df)
        )
        return {
            "status": "ok",
            "schema": df.schema.simpleString(),
            "rows": rows,
            "scan": scan_info,
        }

    _run("where_p_eq_2", _where_eq_int)

    def _where_eq_str() -> dict[str, Any]:
        df = _read_path(spark, reader_kind, str(path)).where("p = '2'")
        return {
            "status": "ok",
            "schema": df.schema.simpleString(),
            "rows": _encode_rows(df.collect(), engine, zone_name),
        }

    _run("where_p_eq_str2", _where_eq_str)

    def _select_p() -> dict[str, Any]:
        df = _read_path(spark, reader_kind, str(path)).select("p")
        return {
            "status": "ok",
            "schema": df.schema.simpleString(),
            "rows": _encode_rows(df.collect(), engine, zone_name),
        }

    _run("select_p", _select_p)

    def _count() -> dict[str, Any]:
        df = _read_path(spark, reader_kind, str(path))
        return {"status": "ok", "count": df.count()}

    _run("count", _count)

    def _groupby_p() -> dict[str, Any]:
        df = _read_path(spark, reader_kind, str(path)).groupBy("p").count()
        return {
            "status": "ok",
            "schema": df.schema.simpleString(),
            "rows": _encode_rows(df.collect(), engine, zone_name),
        }

    _run("groupBy_p_count", _groupby_p)

    def _temp_view_sql() -> dict[str, Any]:
        df = _read_path(spark, reader_kind, str(path))
        view_name = f"v_{reader_kind}_{ansi}"
        df.createOrReplaceTempView(view_name)
        all_rows = _encode_rows(
            spark.sql(f"SELECT * FROM {view_name}").collect(), engine, zone_name
        )
        filt_rows = _encode_rows(
            spark.sql(f"SELECT * FROM {view_name} WHERE p = 2").collect(), engine, zone_name
        )
        grp_rows = _encode_rows(
            spark.sql(f"SELECT p, COUNT(*) AS c FROM {view_name} GROUP BY p").collect(),
            engine,
            zone_name,
        )
        return {
            "status": "ok",
            "all": all_rows,
            "where_2": filt_rows,
            "group": grp_rows,
        }

    _run("temp_view_sql", _temp_view_sql)

    def _input_file_name_df() -> dict[str, Any]:
        df = _read_path(spark, reader_kind, str(path)).select(sf.input_file_name().alias("fn"))
        rows = df.collect()
        rels = sorted(Path(r[0]).parent.name + "/" + Path(r[0]).name for r in rows)
        return {"status": "ok", "files": rels}

    _run("input_file_name_df", _input_file_name_df)

    def _input_file_name_sql() -> dict[str, Any]:
        df = _read_path(spark, reader_kind, str(path))
        view_name = f"v_ifn_{reader_kind}_{ansi}"
        df.createOrReplaceTempView(view_name)
        rows = spark.sql(f"SELECT input_file_name() AS fn FROM {view_name}").collect()
        rels = sorted(Path(r[0]).parent.name + "/" + Path(r[0]).name for r in rows)
        return {"status": "ok", "files": rels}

    _run("input_file_name_sql", _input_file_name_sql)

    def _copy_roundtrip() -> dict[str, Any]:
        df = _read_path(spark, reader_kind, str(path))
        copy_dir = scratch / f"copy_{reader_kind}_{ansi}"
        df.write.mode("overwrite").parquet(str(copy_dir))
        back = spark.read.parquet(str(copy_dir))
        return {
            "status": "ok",
            "schema": back.schema.simpleString(),
            "rows": _encode_rows(back.collect(), engine, zone_name),
        }

    _run("read_then_write_copy_readback", _copy_roundtrip)

    if reader_kind in {"parquet", "parquet_load"}:

        def _copy_part_roundtrip() -> dict[str, Any]:
            df = _read_path(spark, reader_kind, str(path))
            copy_dir = scratch / f"copy_part_{reader_kind}_{ansi}"
            df.write.mode("overwrite").partitionBy("p").parquet(str(copy_dir))
            back = spark.read.parquet(str(copy_dir))
            return {
                "status": "ok",
                "schema": back.schema.simpleString(),
                "rows": _encode_rows(back.collect(), engine, zone_name),
            }

        _run("read_then_write_partitioned_copy_readback", _copy_part_roundtrip)

    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=["spark", "repark"], required=True)
    parser.add_argument("--fixtures", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    fixtures = Path(args.fixtures)
    spark_root = fixtures / "spark"
    repark_root = fixtures / "repark"
    warehouse = fixtures / "iceberg_wh"

    scratch = Path(tempfile.mkdtemp(prefix=f"partdisc-probe-{args.engine}-"))
    session_warehouse = warehouse if args.engine == "spark" else (scratch / "repark_ice_wh")
    session_warehouse.mkdir(parents=True, exist_ok=True)
    spark, sf = _build_session(args.engine, session_warehouse)
    results: dict[str, Any] = {}
    if args.engine == "repark":
        spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.db")
        spark.sql(
            "CREATE TABLE ice.db.part_ice (id BIGINT, s STRING, p INT) "
            "USING iceberg PARTITIONED BY (p)"
        )
        spark.sql(
            "INSERT INTO ice.db.part_ice VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', 2), (4, 'd', 2)"
        )

    readers = [
        "parquet",
        "parquet_load",
        "csv_header",
        "csv_noheader",
        "json",
        "orc",
        "text",
    ]

    try:
        for rdr in readers:
            fdir = spark_root / _fmt_dir_for_reader(rdr)
            ext = _ext_for_reader(rdr)

            for ansi in ["true", "false"]:
                qres = _run_queries(
                    spark, sf, args.engine, rdr, fdir / "one_level", "UTC", ansi, scratch
                )
                for qname, val in qres.items():
                    results[f"Q/{rdr}/ansi={ansi}/{qname}"] = val

            _set_conf(spark, zone="UTC", ansi="true")
            results[f"L/{rdr}/two_level"] = _capture_read(
                spark, args.engine, rdr, str(fdir / "two_level"), "UTC"
            )

            repark_one = repark_root / _fmt_dir_for_reader(rdr) / "one_level"
            if repark_one.exists():
                results[f"L/{rdr}/repark_written_one_level"] = _capture_read(
                    spark, args.engine, rdr, str(repark_one), "UTC"
                )
            repark_two = repark_root / _fmt_dir_for_reader(rdr) / "two_level"
            if repark_two.exists():
                results[f"L/{rdr}/repark_written_two_level"] = _capture_read(
                    spark, args.engine, rdr, str(repark_two), "UTC"
                )

            basic_layouts = [
                "type_int",
                "type_bigint",
                "type_double",
                "type_decimal_looking",
                "type_decimal_huge",
                "type_decimal_only_huge",
                "type_string",
                "type_bool",
                "type_leading_zero",
                "type_leading_zero_mixed",
                "type_int_string_mix",
                "type_int_double_mix",
                "special_hive_default",
                "special_hive_default_only",
                "special_empty_val",
                "special_empty_val_only",
                "special_url_escaped",
                "special_url_spark_written",
                "conflict_names",
                "mixed_depth",
                "root_and_part",
                "noise_hidden",
                "root_with_equals_flat/tag=foo",
                "root_with_equals_part/tag=foo",
            ]
            for layout in basic_layouts:
                _set_conf(spark, zone="UTC", ansi="true")
                results[f"L/{rdr}/{layout}"] = _capture_read(
                    spark, args.engine, rdr, str(fdir / layout), "UTC"
                )

            if rdr != "text":
                for layout in ["col_in_file_and_dir_same", "col_in_file_and_dir_diff_type"]:
                    _set_conf(spark, zone="UTC", ansi="true")
                    results[f"L/{rdr}/{layout}"] = _capture_read(
                        spark, args.engine, rdr, str(fdir / layout), "UTC"
                    )

            for zone in ["UTC", "America/New_York"]:
                for ansi in ["true", "false"]:
                    _set_conf(spark, zone=zone, ansi=ansi)
                    for layout in [
                        "type_date",
                        "type_timestamp_spark",
                        "type_timestamp_raw",
                        "type_timestamp_iso",
                        "type_timestamp_frac",
                        "type_timestamp_frac2",
                    ]:
                        results[f"TZ/{rdr}/{layout}/tz={zone}/ansi={ansi}"] = _capture_read(
                            spark, args.engine, rdr, str(fdir / layout), zone
                        )

            for layout in [
                "one_level",
                "type_int",
                "type_leading_zero",
                "type_date",
                "type_timestamp_raw",
                "special_hive_default_only",
            ]:
                _set_conf(spark, zone="UTC", ansi="true", type_inference="false")
                results[f"INFER_OFF/{rdr}/{layout}"] = _capture_read(
                    spark, args.engine, rdr, str(fdir / layout), "UTC"
                )
            _set_conf(spark, zone="UTC", ansi="true", type_inference="true")

            one = fdir / "one_level"
            _set_conf(spark, zone="UTC", ansi="true")
            results[f"OPT/{rdr}/leaf_read"] = _capture_read(
                spark, args.engine, rdr, str(one / "p=1"), "UTC"
            )
            results[f"OPT/{rdr}/basePath_leaf"] = _capture_read(
                spark, args.engine, rdr, str(one / "p=1"), "UTC", options={"basePath": str(one)}
            )
            results[f"OPT/{rdr}/basePath_glob"] = _capture_read(
                spark, args.engine, rdr, str(one / "p=*"), "UTC", options={"basePath": str(one)}
            )
            results[f"OPT/{rdr}/glob_dir"] = _capture_read(
                spark, args.engine, rdr, str(one / "p=*"), "UTC"
            )
            results[f"OPT/{rdr}/glob_files"] = _capture_read(
                spark, args.engine, rdr, str(one / f"p=*/*{ext}"), "UTC"
            )
            results[f"OPT/{rdr}/path_list_dirs"] = _capture_read(
                spark, args.engine, rdr, [str(one / "p=1"), str(one / "p=2")], "UTC"
            )
            if rdr == "parquet":
                try:
                    df_var = spark.read.parquet(str(one / "p=1"), str(one / "p=2"))
                    results["OPT/parquet/varargs_dirs"] = {
                        "status": "ok",
                        "schema": df_var.schema.simpleString(),
                        "dtypes": [[n, t] for n, t in df_var.dtypes],
                        "rows": _encode_rows(df_var.collect(), args.engine, "UTC"),
                    }
                except Exception as exc:
                    results["OPT/parquet/varargs_dirs"] = _encode_error(exc)
            results[f"OPT/{rdr}/recursive_true"] = _capture_read(
                spark, args.engine, rdr, str(one), "UTC", options={"recursiveFileLookup": "true"}
            )
            results[f"OPT/{rdr}/recursive_false"] = _capture_read(
                spark, args.engine, rdr, str(one), "UTC", options={"recursiveFileLookup": "false"}
            )
            results[f"OPT/{rdr}/mergeSchema_true_one"] = _capture_read(
                spark, args.engine, rdr, str(one), "UTC", options={"mergeSchema": "true"}
            )
            results[f"OPT/{rdr}/mergeSchema_false_one"] = _capture_read(
                spark, args.engine, rdr, str(one), "UTC", options={"mergeSchema": "false"}
            )
            if rdr != "text":
                evo = fdir / "evolved"
                results[f"OPT/{rdr}/mergeSchema_true_evolved"] = _capture_read(
                    spark, args.engine, rdr, str(evo), "UTC", options={"mergeSchema": "true"}
                )
                results[f"OPT/{rdr}/mergeSchema_false_evolved"] = _capture_read(
                    spark, args.engine, rdr, str(evo), "UTC", options={"mergeSchema": "false"}
                )

            for cs in ["false", "true"]:
                _set_conf(spark, zone="UTC", ansi="true", case_sensitive=cs)
                if rdr != "text":
                    results[f"CASE/{rdr}/dir_upper_data_lower/cs={cs}"] = _capture_read(
                        spark, args.engine, rdr, str(fdir / "case_dir_upper_data_lower"), "UTC"
                    )
                results[f"CASE/{rdr}/dir_upper_no_data/cs={cs}"] = _capture_read(
                    spark, args.engine, rdr, str(fdir / "case_dir_upper_no_data"), "UTC"
                )
                try:
                    df_u = _read_path(spark, rdr, str(fdir / "case_dir_upper_no_data")).where(
                        "p = 2"
                    )
                    results[f"CASE/{rdr}/dir_upper_where_lower_p/cs={cs}"] = {
                        "status": "ok",
                        "schema": df_u.schema.simpleString(),
                        "rows": _encode_rows(df_u.collect(), args.engine, "UTC"),
                    }
                except Exception as exc:
                    results[f"CASE/{rdr}/dir_upper_where_lower_p/cs={cs}"] = _encode_error(exc)
                try:
                    df_u2 = _read_path(spark, rdr, str(fdir / "case_dir_upper_no_data")).where(
                        "P = 2"
                    )
                    results[f"CASE/{rdr}/dir_upper_where_upper_P/cs={cs}"] = {
                        "status": "ok",
                        "schema": df_u2.schema.simpleString(),
                        "rows": _encode_rows(df_u2.collect(), args.engine, "UTC"),
                    }
                except Exception as exc:
                    results[f"CASE/{rdr}/dir_upper_where_upper_P/cs={cs}"] = _encode_error(exc)
                results[f"CASE/{rdr}/dir_mixed_case/cs={cs}"] = _capture_read(
                    spark, args.engine, rdr, str(fdir / "case_dir_mixed"), "UTC"
                )

        _set_conf(spark, zone="UTC", ansi="true", case_sensitive="false", type_inference="true")
        for ice_door, loader in [
            ("read_table", lambda: spark.read.table("ice.db.part_ice")),
            ("spark_table", lambda: spark.table("ice.db.part_ice")),
            ("spark_sql", lambda: spark.sql("SELECT * FROM ice.db.part_ice")),
            (
                "iceberg_load_path",
                lambda: spark.read.format("iceberg").load(str(warehouse / "db" / "part_ice")),
            ),
        ]:
            try:
                idf = loader()
                results[f"ICE/{ice_door}/read"] = {
                    "status": "ok",
                    "schema": idf.schema.simpleString(),
                    "dtypes": [[n, t] for n, t in idf.dtypes],
                    "rows": _encode_rows(idf.collect(), args.engine, "UTC"),
                }
                results[f"ICE/{ice_door}/where_p_2"] = {
                    "status": "ok",
                    "rows": _encode_rows(idf.where("p = 2").collect(), args.engine, "UTC"),
                }
                results[f"ICE/{ice_door}/select_p"] = {
                    "status": "ok",
                    "rows": _encode_rows(idf.select("p").collect(), args.engine, "UTC"),
                }
                results[f"ICE/{ice_door}/count"] = {"status": "ok", "count": idf.count()}
                results[f"ICE/{ice_door}/groupBy_p"] = {
                    "status": "ok",
                    "rows": _encode_rows(idf.groupBy("p").count().collect(), args.engine, "UTC"),
                }
            except Exception as exc:
                results[f"ICE/{ice_door}/read"] = _encode_error(exc)
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
        spark.stop()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2, sort_keys=True), encoding="utf-8")
    print(f"PROBE-DONE engine={args.engine} cells={len(results)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
