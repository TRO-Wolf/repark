M38 = "99999999999999999999999999999999999999"

FIXTURES = {
    "fx": "SELECT CAST(2147483647 AS INT) AS a, CAST(0 AS INT) AS b, 'abc' AS s, true AS t",
    "fpw": "SELECT CAST(v AS DECIMAL(38,0)) AS x FROM (VALUES ('"
    + M38
    + "'), ('"
    + M38
    + "')) t(v)",
    "tzt": "SELECT TIMESTAMP '2024-01-01 02:30:00' AS ts, '2024-01-01 02:30:00' AS s, DATE "
    "'2024-01-01' AS dt",
    "fmap": "SELECT map('a', 1) AS mp, array(1, 2) AS ar",
    "fmk": "SELECT map(named_struct('i', 1, 'j', 2), 7) AS mk",
    "cs": "SELECT CAST(1 AS INT) AS a",
    "fdec": (
        "SELECT CAST(k AS INT) AS k, CAST(x0 AS DECIMAL(38,0)) AS x0, CAST(x10 AS "
        "DECIMAL(38,10)) AS x10, "
        "CAST(xd AS DECIMAL(10,2)) AS xd FROM (VALUES (0, '1', NULL, NULL), (1, NULL, '1.5', "
        "'2.25')) t(k, x0, x10, xd)"
    ),
}
CASE_TWIN_FIXTURE = ("cs2", "SELECT CAST(1 AS INT) AS a, CAST(2 AS INT) AS A")
CATALOG_TABLES = {"fx": "mc.ns.fx", "tzt": "mc.ns.tzt"}
NAMESPACE_TABLES = {"lc.ns1.t1": 1, "lc.ns2.t1": 2}

ANSI_BODIES = {
    "div": "SELECT 10 / b AS m FROM fx",
    "mod": "SELECT 10 % b AS m FROM fx",
    "add_ovf": "SELECT a + 1 AS m FROM fx",
    "avg": "SELECT avg(x) AS m FROM fpw",
    "cast_ovf": "SELECT CAST(a AS SMALLINT) AS m FROM fx",
    "cast_date": "SELECT CAST(s AS DATE) AS m FROM fx",
    "cast_dbl": "SELECT CAST(s AS DOUBLE) AS m FROM fx",
    "cast_int": "SELECT CAST(s AS INT) AS m FROM fx",
    "map_sub": "SELECT mp['zz'] AS m FROM fmap",
    "map_elt": "SELECT element_at(mp, 'zz') AS m FROM fmap",
    "arr_elt": "SELECT element_at(ar, 5) AS m FROM fmap",
    "arr_sub": "SELECT ar[5] AS m FROM fmap",
    "seq": "SELECT sequence(1, '3') AS m FROM fx",
    "map_struct": "SELECT mk[named_struct('j', 1, 'k', 2)] AS m FROM fmk",
    "int_bin": "SELECT hex(CAST(a AS BINARY)) AS m FROM fx",
    "bool_dec": "SELECT CAST(t AS DECIMAL(1,1)) AS m FROM fx",
    "dbl_lit": "SELECT CAST('abc' AS DOUBLE) AS m",
}
TZ_BODIES = {
    "hour": "SELECT hour(ts) AS m FROM tzt",
    "str": "SELECT CAST(ts AS STRING) AS m FROM tzt",
    "date": "SELECT CAST(ts AS DATE) AS m FROM tzt",
    "fmt": "SELECT date_format(ts, 'yyyy-MM-dd HH') AS m FROM tzt",
    "parse": "SELECT unix_timestamp(CAST(s AS TIMESTAMP)) AS m FROM tzt",
    "lit": "SELECT unix_timestamp(TIMESTAMP '2024-01-01 02:30:00') AS m",
    "curtz": "SELECT current_timezone() AS m",
    "fromunix": "SELECT from_unixtime(0) AS m",
    "date_ts": "SELECT unix_timestamp(CAST(dt AS TIMESTAMP)) AS m FROM tzt",
    "trunc": "SELECT CAST(date_trunc('DAY', ts) AS STRING) AS m FROM tzt",
}
CS_BODIES = {
    "ci_upper": "SELECT A AS m FROM cs",
    "cs_twin": "SELECT A AS m FROM cs2",
    "cs_lower": "SELECT a AS m FROM cs2",
}
TPP_BODIES = {
    "tpp_parse": "SELECT unix_timestamp('2024-1-1', 'yyyy-MM-dd') AS m",
    "tpp_try": "SELECT try_to_timestamp('2024-1-1', 'yyyy-MM-dd') IS NULL AS m",
}
DEC_BODIES = {
    "dec_ct": "SELECT CASE WHEN k = 0 THEN x0 ELSE x10 END AS m FROM fdec",
    "dec_dbl": "SELECT coalesce(xd, CAST(1.5 AS DOUBLE)) AS m FROM fdec",
}
TST_BODIES = {
    "tst_lit": "SELECT typeof(TIMESTAMP '2024-01-01 02:30:00') AS m",
    "tst_cast": "SELECT typeof(CAST(s AS TIMESTAMP)) AS m FROM tzt",
}
NS_BODIES = {"ns_read": "SELECT x AS m FROM t1"}

FULL_DOORS = [
    "sql",
    "df",
    "held",
    "tsql",
    "tdf",
    "vss",
    "vsd",
    "vds",
    "ren",
    "heldf",
    "tsqlf",
    "tdff",
    "dff",
    "vdsf",
]
SMALL_DOORS = ["sql", "df", "held", "tsql", "heldf", "tsqlf", "dff"]
FRESH = {"heldf", "tsqlf", "tdff", "dff", "vdsf"}
DOOR_ORDER = [
    "sql",
    "df",
    "held",
    "tsql",
    "tdf",
    "vss",
    "vsd",
    "vds",
    "ren",
    "heldf",
    "tsqlf",
    "tdff",
    "dff",
    "vdsf",
    "cat",
]
POSITIONS = {
    "plain": "SELECT m FROM {V}",
    "cte": "WITH c AS (SELECT m FROM {V}) SELECT m FROM c",
    "join": "SELECT v.m FROM {V} v CROSS JOIN (SELECT 1 AS j) q",
    "subq": "SELECT (SELECT max(m) FROM {V}) AS m",
}

FAMILIES = {
    "ansi": {
        "key": "spark.sql.ansi.enabled",
        "values": [("on", "true"), ("off", "false")],
        "bodies": ANSI_BODIES,
        "doors": FULL_DOORS,
        "positions": True,
        "catalog": ["div", "add_ovf", "cast_ovf", "cast_date"],
        "writes": {
            "div": "DOUBLE",
            "add_ovf": "INT",
            "cast_ovf": "SMALLINT",
            "avg": "DECIMAL(38,4)",
        },
    },
    "tz": {
        "key": "spark.sql.session.timeZone",
        "values": [("utc", "UTC"), ("la", "America/Los_Angeles")],
        "bodies": TZ_BODIES,
        "doors": FULL_DOORS,
        "positions": True,
        "catalog": ["hour", "str", "lit", "curtz"],
        "writes": {"hour": "INT", "str": "STRING", "date": "DATE", "lit": "BIGINT"},
    },
    "cs": {
        "key": "spark.sql.caseSensitive",
        "values": [("ci", "false"), ("cs", "true")],
        "bodies": CS_BODIES,
        "doors": SMALL_DOORS,
        "positions": False,
        "catalog": [],
        "writes": {},
    },
    "tpp": {
        "key": "spark.sql.legacy.timeParserPolicy",
        "values": [("cor", "CORRECTED"), ("leg", "LEGACY")],
        "bodies": TPP_BODIES,
        "doors": SMALL_DOORS,
        "positions": False,
        "catalog": [],
        "writes": {},
    },
    "dec": {
        "key": "spark.sql.legacy.decimal.retainFractionDigitsOnTruncate",
        "values": [("dflt", "false"), ("leg", "true")],
        "bodies": DEC_BODIES,
        "doors": SMALL_DOORS,
        "positions": False,
        "catalog": [],
        "writes": {},
    },
    "tst": {
        "key": "spark.sql.timestampType",
        "values": [("ltz", "TIMESTAMP_LTZ"), ("ntz", "TIMESTAMP_NTZ")],
        "bodies": TST_BODIES,
        "doors": SMALL_DOORS,
        "positions": False,
        "catalog": [],
        "writes": {},
    },
    "ns": {
        "key": "USE",
        "values": [("n1", "lc.ns1"), ("n2", "lc.ns2")],
        "bodies": NS_BODIES,
        "doors": SMALL_DOORS,
        "positions": False,
        "catalog": [],
        "writes": {},
    },
}

NEST = {
    "ansi": ("SELECT b, 10 / b AS m FROM fx", "SELECT m, 10 / b AS m2 FROM {I}"),
    "tz": ("SELECT ts, hour(ts) AS m FROM tzt", "SELECT m, hour(ts) AS m2 FROM {I}"),
}
NEST_DOORS = [("sql", "sql"), ("df", "sql"), ("sql", "df"), ("df", "df")]

READER = {
    "ansi": {
        "body": "SELECT a, b FROM fx",
        "forms": {
            "filter": "SELECT b AS m FROM {V} WHERE 10 / b IS NULL",
            "proj_div": "SELECT 10 / b AS m FROM {V}",
            "proj_add": "SELECT a + 1 AS m FROM {V}",
        },
    },
    "tz": {
        "body": "SELECT ts, hour(ts) AS h FROM tzt",
        "forms": {
            "filter": "SELECT h AS m, hour(ts) AS rh FROM {V} WHERE hour(ts) = {H}",
            "proj": "SELECT h AS m, hour(ts) AS rh, CAST(ts AS STRING) AS rs FROM {V}",
        },
    },
}
READER_HOUR = {"utc": 2, "la": 18}

NOW_BODY = "SELECT current_timezone() AS tz, current_timestamp() AS ct"
NOW_READ = "SELECT tz, current_timezone() AS rtz, ct = current_timestamp() AS same_now FROM {V}"

UNSET_BODIES = {k: ANSI_BODIES[k] for k in ("div", "add_ovf", "cast_ovf")}


def catalog_body(body: str) -> str:
    out = body
    for local, table in CATALOG_TABLES.items():
        out = out.replace(" FROM " + local, " FROM " + table)
    return out
