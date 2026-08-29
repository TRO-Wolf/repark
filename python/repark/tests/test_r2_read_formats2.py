"""R2 — writer option matrix / path modes / partitionBy honesty pins (Arrow path).

Charter: ``.r22/CHARTER.md`` (TRACK R2). Ledger: ``task/r2-read-formats2-ledger.md``.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark.session import _reset_active_session_for_tests


@pytest.fixture
def spark(tmp_path: Path) -> ReparkSession:
    """Isolated session (no AWS)."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-r2-read-formats2").getOrCreate()
    yield session
    session.stop()
    _reset_active_session_for_tests()


def _sorted_rows(frame: object) -> list[dict[str, object]]:
    """Collect Arrow rows ordered by first column for stable comparison."""
    table = frame.to_arrow()  # type: ignore[attr-defined]
    rows = table.to_pylist()
    if not rows:
        return rows
    key0 = next(iter(rows[0]))
    return sorted(rows, key=lambda row: (row[key0] is None, row[key0]))


# Writer option matrix honesty


def test_write_csv_quote_all_always(spark: ReparkSession, tmp_path: Path) -> None:
    """quoteAll=true → every field quoted (DF quote_style Always)."""
    path = tmp_path / "qa"
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").option(
        "quoteAll", "true"
    ).csv(str(path), header=True)
    text = "\n".join(part.read_text(encoding="utf-8") for part in path.rglob("*.csv"))
    assert '"id"' in text and '"name"' in text
    assert '"1"' in text and '"a"' in text


def test_write_csv_quote_all_kwarg(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "qa_kw"
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").csv(
        str(path), header=True, quoteAll=True
    )
    text = "\n".join(part.read_text(encoding="utf-8") for part in path.rglob("*.csv"))
    assert '"1"' in text


def test_write_csv_escape_quotes_double(spark: ReparkSession, tmp_path: Path) -> None:
    """escapeQuotes=true → doubled internal quotes (RFC style)."""
    path = tmp_path / "eq"
    spark.createDataFrame([(1, 'c"d')], ["id", "name"]).write.mode("overwrite").option(
        "escapeQuotes", "true"
    ).csv(str(path), header=True)
    text = "\n".join(part.read_text(encoding="utf-8") for part in path.rglob("*.csv"))
    assert '""' in text or 'c""d' in text


def test_write_csv_null_value_and_header_round_trip(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "nv"
    spark.createDataFrame([(1, None), (2, "b")], ["id", "name"]).write.mode("overwrite").option(
        "header", "true"
    ).option("nullValue", "NA").csv(str(path))
    text = "\n".join(part.read_text(encoding="utf-8") for part in path.rglob("*.csv"))
    assert "NA" in text
    loaded = spark.read.csv(str(path), header=True, nullValue="NA", inferSchema=True)
    rows = _sorted_rows(loaded)
    assert rows[0]["name"] is None
    assert rows[1]["name"] == "b"


def test_write_csv_date_format_refuse_loud(spark: ReparkSession, tmp_path: Path) -> None:
    """dateFormat refuse-loud — SimpleDateFormat vs strftime mismatch (no silent mis-format)."""
    path = tmp_path / "dfmt"
    with pytest.raises(AnalysisException, match=r"dateFormat|not supported|strftime"):
        spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").option(
            "dateFormat", "yyyy-MM-dd"
        ).csv(str(path), header=True)


def test_write_csv_timestamp_format_refuse_loud(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "tfmt"
    with pytest.raises(AnalysisException, match=r"timestampFormat|not supported|strftime"):
        spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").csv(
            str(path), header=True, timestampFormat="yyyy-MM-dd HH:mm:ss"
        )


def test_write_json_date_format_refuse_loud(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "jdf"
    with pytest.raises(AnalysisException, match=r"dateFormat|not supported"):
        spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").option(
            "dateFormat", "yyyy-MM-dd"
        ).json(str(path))


def test_write_parquet_compression_snappy(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "pq_snappy"
    spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]).write.mode("overwrite").option(
        "compression", "snappy"
    ).parquet(str(path))
    assert list(path.rglob("*.parquet"))
    loaded = spark.read.parquet(str(path))
    assert _sorted_rows(loaded) == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    table = loaded.orderBy("id").to_arrow()
    assert table.schema.field("id").type in (pa.int64(), pa.int32())


def test_write_parquet_compression_gzip_kwarg(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "pq_gzip"
    spark.createDataFrame([(3, "c")], ["id", "name"]).write.mode("overwrite").parquet(
        str(path), compression="gzip"
    )
    assert list(path.rglob("*.parquet"))
    assert spark.read.parquet(str(path)).to_arrow().to_pylist() == [{"id": 3, "name": "c"}]


def test_write_csv_unknown_option_loud(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "unk"
    with pytest.raises(AnalysisException, match=r"not supported"):
        spark.createDataFrame([(1,)], ["id"]).write.mode("overwrite").option(
            "maxRecordsPerFile", "1"
        ).csv(str(path), header=True)


# Path write modes (oracle: Spark overwrite / append / error / ignore)


def test_path_mode_error_on_existing(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "exists"
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").parquet(str(path))
    with pytest.raises(AnalysisException, match=r"PATH_ALREADY_EXISTS|already exists"):
        spark.createDataFrame([(2, "b")], ["id", "name"]).write.mode("error").parquet(str(path))


def test_path_mode_ignore_preserves_existing(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "ign"
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").csv(
        str(path), header=True
    )
    spark.createDataFrame([(99, "z")], ["id", "name"]).write.mode("ignore").csv(
        str(path), header=True
    )
    loaded = spark.read.csv(str(path), header=True, inferSchema=True)
    assert _sorted_rows(loaded) == [{"id": 1, "name": "a"}]


def test_path_mode_ignore_writes_when_absent(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "ign_new"
    spark.createDataFrame([(5, "e")], ["id", "name"]).write.mode("ignore").json(str(path))
    assert path.exists()
    assert spark.read.json(str(path)).to_arrow().to_pylist() == [{"id": 5, "name": "e"}]


def test_path_mode_append_merges_rows(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "app"
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").parquet(str(path))
    spark.createDataFrame([(2, "b")], ["id", "name"]).write.mode("append").parquet(str(path))
    rows = _sorted_rows(spark.read.parquet(str(path)))
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_path_mode_append_csv_round_trip(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "app_csv"
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").csv(
        str(path), header=True
    )
    spark.createDataFrame([(2, "b")], ["id", "name"]).write.mode("append").csv(
        str(path), header=True
    )
    # Multiple part files; read directory.
    loaded = spark.read.csv(str(path), header=True, inferSchema=True)
    assert _sorted_rows(loaded) == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def test_path_mode_overwrite_replaces(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "ow"
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").json(str(path))
    spark.createDataFrame([(9, "z")], ["id", "name"]).write.mode("overwrite").json(str(path))
    assert spark.read.json(str(path)).to_arrow().to_pylist() == [{"id": 9, "name": "z"}]


# partitionBy (hour-0 DF COPY PARTITIONED BY wire)


def test_partition_by_parquet_hive_layout(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "pb_pq"
    spark.createDataFrame([(1, "a"), (1, "b"), (2, "c")], ["id", "name"]).write.mode(
        "overwrite"
    ).partitionBy("id").parquet(str(path))
    assert (path / "id=1").is_dir()
    assert (path / "id=2").is_dir()
    assert list((path / "id=1").rglob("*.parquet"))
    # Data files omit partition column (Spark shape); reader hive-discovery residual.
    part_rows = spark.read.parquet(str(path / "id=1")).to_arrow().to_pylist()
    assert all(set(row.keys()) == {"name"} for row in part_rows)
    assert sorted(row["name"] for row in part_rows) == ["a", "b"]


def test_partition_by_csv_multi_column(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "pb_multi"
    spark.createDataFrame([(1, "x", "a"), (1, "y", "b")], ["id", "cat", "name"]).write.mode(
        "overwrite"
    ).partitionBy("id", "cat").csv(str(path), header=True)
    assert (path / "id=1" / "cat=x").is_dir()
    assert (path / "id=1" / "cat=y").is_dir()


def test_partition_by_unknown_column_loud(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "pb_bad"
    with pytest.raises(AnalysisException, match=r"partitionBy column|not in the DataFrame"):
        spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").partitionBy(
            "missing"
        ).json(str(path))


def test_partition_by_save_kwarg(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "pb_save"
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").format(
        "parquet"
    ).save(str(path), partitionBy="id")
    assert (path / "id=1").is_dir()


def test_partition_by_append_merges_partition_dirs(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "pb_app"
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").partitionBy(
        "id"
    ).parquet(str(path))
    spark.createDataFrame([(1, "b"), (2, "c")], ["id", "name"]).write.mode("append").partitionBy(
        "id"
    ).parquet(str(path))
    assert (path / "id=1").is_dir() and (path / "id=2").is_dir()
    names = [row["name"] for row in spark.read.parquet(str(path / "id=1")).to_arrow().to_pylist()]
    assert sorted(names) == ["a", "b"]


def test_partition_by_parquet_root_read_no_null_partition_keys(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """No empty full-schema root part; root read not null-filled.

    Root schema pollution makes ``read.parquet(root)`` merge schemas so every data row gets
    partition keys as ``None`` — silent wrong. Partition keys must be either omitted
    (hive-discovery residual) or present with real values, never all-null fabricated columns.
    """
    path = tmp_path / "pb_root"
    spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]).write.mode("overwrite").partitionBy(
        "id"
    ).parquet(str(path))
    # No root-level empty full-schema part file.
    root_parquet = list(path.glob("*.parquet"))
    assert root_parquet == [], f"unexpected root parquet pollution: {root_parquet}"
    assert (path / "id=1").is_dir() and (path / "id=2").is_dir()
    rows = spark.read.parquet(str(path)).to_arrow().to_pylist()
    assert len(rows) == 2
    # Critical honesty: never fabricate null partition keys for every data row.
    if any("id" in row for row in rows):
        assert any(row.get("id") is not None for row in rows), (
            "partition key present only as null-fill — silent wrong (C3-001 regression)"
        )
        assert all(row.get("id") is not None for row in rows)
    else:
        # Hive discovery residual: data files omit partition col; names only is honest.
        assert all(set(row.keys()) == {"name"} for row in rows)
        assert sorted(row["name"] for row in rows) == ["a", "b"]


def test_partition_by_duplicate_column_loud(spark: ReparkSession, tmp_path: Path) -> None:
    """Duplicate partitionBy names refuse (no nested id=1/id=1/)."""
    path = tmp_path / "pb_dup"
    with pytest.raises(AnalysisException, match=r"duplicate partitionBy"):
        spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").partitionBy(
            "id", "id"
        ).parquet(str(path))


def test_path_append_schema_column_set_loud(spark: ReparkSession, tmp_path: Path) -> None:
    """Append with fewer columns refuses (no silent null-fill)."""
    path = tmp_path / "app_schema"
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").parquet(str(path))
    with pytest.raises(AnalysisException, match=r"column sets differ|cannot append"):
        spark.createDataFrame([(2,)], ["id"]).write.mode("append").parquet(str(path))
    # Prior data preserved.
    assert _sorted_rows(spark.read.parquet(str(path))) == [{"id": 1, "name": "a"}]


def test_path_append_type_mismatch_loud(spark: ReparkSession, tmp_path: Path) -> None:
    """Type-incompatible append fails at write (not only at read)."""
    path = tmp_path / "app_type"
    spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").parquet(str(path))
    with pytest.raises(AnalysisException, match=r"type mismatch|cannot append"):
        spark.createDataFrame([(2.5, "b")], ["id", "name"]).write.mode("append").parquet(str(path))
    assert _sorted_rows(spark.read.parquet(str(path))) == [{"id": 1, "name": "a"}]


def test_path_append_onto_plain_file_loud(spark: ReparkSession, tmp_path: Path) -> None:
    """Append onto a plain file → AnalysisException, not raw FileExistsError."""
    path = tmp_path / "plain.parquet"
    path.write_bytes(b"not-a-dir")
    with pytest.raises(AnalysisException, match=r"PATH_ALREADY_EXISTS|is a file"):
        spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("append").parquet(str(path))


def test_path_overwrite_symlink_dir_loud(spark: ReparkSession, tmp_path: Path) -> None:
    """Overwrite of symlink directory → AnalysisException, not raw OSError."""
    real = tmp_path / "real_dest"
    real.mkdir()
    (real / "marker.txt").write_text("keep", encoding="utf-8")
    link = tmp_path / "link_dest"
    link.symlink_to(real, target_is_directory=True)
    with pytest.raises(AnalysisException, match=r"symbolic link|symlink|cannot overwrite"):
        spark.createDataFrame([(1, "a")], ["id", "name"]).write.mode("overwrite").parquet(str(link))
    # Symlink target content not destroyed by a failed rmtree attempt.
    assert (real / "marker.txt").read_text(encoding="utf-8") == "keep"


# Residual read options (pins for R1 residuals; divergences.md is D3 sole-writer)


def test_read_csv_encoding_non_utf8_loud(spark: ReparkSession, tmp_path: Path) -> None:
    """Residual read option: non-UTF-8 encoding remains refuse-loud (R1 residual)."""
    path = tmp_path / "enc.csv"
    path.write_text("id\n1\n", encoding="utf-8")
    with pytest.raises(AnalysisException, match=r"encoding|not supported"):
        spark.read.option("encoding", "ISO-8859-1").csv(str(path), header=True)


def test_read_csv_timestamp_format_loud(spark: ReparkSession, tmp_path: Path) -> None:
    path = tmp_path / "tf.csv"
    path.write_text("ts\n2020-01-01 00:00:00\n", encoding="utf-8")
    with pytest.raises(AnalysisException, match=r"timestampFormat|not supported"):
        spark.read.option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(str(path), header=True)
