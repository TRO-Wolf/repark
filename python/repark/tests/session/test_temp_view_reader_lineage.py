from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from test_temp_view_session_settings_1 import build

from repark.spark.session import _reset_active_session_for_tests

pytestmark = pytest.mark.xdist_group("temp_view_reader_lineage")
LA = "America/Los_Angeles"
TK = "Asia/Tokyo"
UTC_PARSE = [(1, 1704078000), (2, 1704110400)]
LA_PARSE = [(1, 1704106800), (2, 1704139200)]
SPARK = {
    "json/direct/read_utc": UTC_PARSE,
    "json/frame_utc/direct_read_la": LA_PARSE,
    "json/frame_utc_view_tk/read_la": LA_PARSE,
    "json/sqlview_utc/read_utc": UTC_PARSE,
    "json/derived_filter/read_utc": UTC_PARSE,
    "json/derived_expr/read_utc": [(2, 1704078000), (3, 1704110400)],
    "csv_infer/direct/read_utc": UTC_PARSE,
    "csv_infer/derived_filter/read_utc": UTC_PARSE,
}


def conf(session: Any, zone: str, ansi: str) -> None:
    session.sql(f"SET spark.sql.session.timeZone={zone}").collect()
    session.sql(f"SET spark.sql.ansi.enabled={ansi}").collect()


def rows(session: Any, query: str) -> list[tuple]:
    return sorted(tuple(row) for row in session.sql(query).collect())


def measure(session: Any, root: Path, ansi: str) -> dict[str, list[tuple]]:
    (root / "json").mkdir()
    (root / "json" / "a.json").write_text(
        '{"n": 1, "ts": "2024-01-01 03:00:00"}\n{"n": 2, "ts": "2024-01-01 12:00:00"}\n'
    )
    (root / "csv").mkdir()
    (root / "csv" / "a.csv").write_text("n,ts\n1,2024-01-01 03:00:00\n2,2024-01-01 12:00:00\n")
    read = session.read.schema("n INT, ts TIMESTAMP")
    conf(session, LA, ansi)
    made_la = read.json(str(root / "json"))
    made_la.createOrReplaceTempView("jf")
    made_la.filter("n > 0").createOrReplaceTempView("jd_filter")
    made_la.selectExpr("n + 1 AS n1", "ts").createOrReplaceTempView("jd_expr")
    inferred = session.read.option("header", "true").option("inferSchema", "true")
    csv = inferred.csv(str(root / "csv"))
    csv.createOrReplaceTempView("cf")
    csv.filter("n > 0").createOrReplaceTempView("cd_filter")
    conf(session, "UTC", ansi)
    session.sql(
        "CREATE OR REPLACE TEMP VIEW sj AS SELECT n, unix_timestamp(ts) AS u FROM jf"
    ).collect()
    read.json(str(root / "json")).createOrReplaceTempView("jfu")
    conf(session, TK, ansi)
    session.sql(
        "CREATE OR REPLACE TEMP VIEW sju AS SELECT n, unix_timestamp(ts) AS u FROM jfu"
    ).collect()
    found = {}
    conf(session, "UTC", ansi)
    found["json/direct/read_utc"] = rows(session, "SELECT n, unix_timestamp(ts) FROM jf")
    found["json/sqlview_utc/read_utc"] = rows(session, "SELECT * FROM sj")
    found["json/derived_filter/read_utc"] = rows(
        session, "SELECT n, unix_timestamp(ts) FROM jd_filter"
    )
    found["json/derived_expr/read_utc"] = rows(
        session, "SELECT n1, unix_timestamp(ts) FROM jd_expr"
    )
    found["csv_infer/direct/read_utc"] = rows(session, "SELECT n, unix_timestamp(ts) FROM cf")
    found["csv_infer/derived_filter/read_utc"] = rows(
        session, "SELECT n, unix_timestamp(ts) FROM cd_filter"
    )
    conf(session, LA, ansi)
    found["json/frame_utc/direct_read_la"] = rows(session, "SELECT n, unix_timestamp(ts) FROM jfu")
    found["json/frame_utc_view_tk/read_la"] = rows(session, "SELECT * FROM sju")
    return found


@pytest.fixture(scope="module", params=["true", "false"])
def measured(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[dict[str, list[tuple]]]:
    root = tmp_path_factory.mktemp(f"tvrl-{request.param}")
    session = build(root, request.param)
    found = measure(session, root, request.param)
    session.stop()
    _reset_active_session_for_tests()
    yield found


@pytest.mark.parametrize("key", sorted(SPARK))
def test_a_reader_built_view_parses_under_the_reader_zone(
    measured: dict[str, list[tuple]], key: str
) -> None:
    assert measured[key] == SPARK[key], key
