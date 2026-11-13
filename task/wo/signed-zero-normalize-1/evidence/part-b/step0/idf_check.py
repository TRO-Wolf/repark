from repark import ReparkSession

session = ReparkSession.builder.appName("idf").getOrCreate()
session.sql(
    "CREATE OR REPLACE TEMPORARY VIEW v AS SELECT * FROM VALUES "
    "(1, CAST('0.0' AS DOUBLE), CAST('-0.0' AS DOUBLE)) AS x(id, d, e)"
)
for query in (
    "SELECT id FROM v WHERE d IS DISTINCT FROM e",
    "SELECT id FROM v WHERE NOT (d <=> e)",
    "SELECT id FROM v WHERE d IS DISTINCT FROM 1.5",
):
    try:
        print(query, session.sql(query).collect())
    except Exception as error:
        print(query, "REFUSED", str(error)[:200])
