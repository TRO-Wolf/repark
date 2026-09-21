"""M8-STARTSWITH-1: SQL startswith answers the PD p17 predicate with Spark semantics."""

from repark.spark import ReparkSession
from repark.spark import functions as F  # noqa: N812


def test_fn_startswith_sql_where_matches_spark() -> None:
    """M8-STARTSWITH-1: WHERE startswith(s, 'ap') answers [1, 2]; NULL and '' excluded."""
    repark = ReparkSession.builder.appName("fn-startswith").master("local[1]").getOrCreate()
    try:
        rows = repark.sql(
            "SELECT id FROM (VALUES (1, 'apple'), (2, 'apricot'), (3, 'banana'), "
            "(4, CAST(NULL AS STRING)), (6, ''), (7, 'Zeta')) AS t(id, s) "
            "WHERE startswith(s, 'ap') ORDER BY id"
        ).collect()
        assert [row[0] for row in rows] == [1, 2]
    finally:
        repark.stop()


def test_fn_startswith_df_door_matches_sql_door() -> None:
    """M8-STARTSWITH-1: F.startswith over a literal agrees with Column.startswith."""
    repark = ReparkSession.builder.appName("fn-startswith-df").master("local[1]").getOrCreate()
    try:
        frame = repark.createDataFrame(
            [
                (1, "apple"),
                (2, "apricot"),
                (3, "banana"),
                (4, None),
                (6, ""),
            ],
            ["id", "s"],
        )
        expected = [1, 2]
        via_function = [
            row[0]
            for row in frame.filter(F.startswith(F.col("s"), F.lit("ap"))).orderBy("id").collect()
        ]
        via_method = [
            row[0] for row in frame.filter(F.col("s").startswith("ap")).orderBy("id").collect()
        ]
        assert via_function == expected
        assert via_method == expected
    finally:
        repark.stop()
