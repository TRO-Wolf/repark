import argparse
import hashlib
import json
import os
import sys
from collections import Counter
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from typing import Any

OUT = Path()
RAW = OUT / "raw"
ICEBERG_JAR = os.environ.get("ICEBERG_JAR", "")

ELEM_SQL = {"I64": "Int64", "UTF8": "Utf8", "STRUCT": 'Struct("x": Int64, "y": Utf8)'}
ELEM_DDL = {"I64": "bigint", "UTF8": "string", "STRUCT": "struct<x:bigint,y:string>"}

KINDS = {
    "LV_I64": {"container": "ListView", "elem": "I64", "twin": "L_I64"},
    "LLV_I64": {"container": "LargeListView", "elem": "I64", "twin": "L_I64"},
    "LV_UTF8": {"container": "ListView", "elem": "UTF8", "twin": "L_UTF8"},
    "LLV_UTF8": {"container": "LargeListView", "elem": "UTF8", "twin": "L_UTF8"},
    "LV_STRUCT": {"container": "ListView", "elem": "STRUCT", "twin": "L_STRUCT"},
    "LLV_STRUCT": {"container": "LargeListView", "elem": "STRUCT", "twin": "L_STRUCT"},
    "L_I64": {"container": "List", "elem": "I64", "twin": None},
    "LL_I64": {"container": "LargeList", "elem": "I64", "twin": None},
    "FSL2_I64": {"container": "FixedSizeList", "elem": "I64", "twin": None},
    "L_UTF8": {"container": "List", "elem": "UTF8", "twin": None},
    "L_STRUCT": {"container": "List", "elem": "STRUCT", "twin": None},
    "NEST_LV": {"nest": "ListView", "elem": "I64", "twin": "NEST_L"},
    "NEST_L": {"nest": "List", "elem": "I64", "twin": None},
}

LOGICAL = {"A": [1, 2], "B": [2, 1], "C": [], "D": [1, None], "E": None, "F": [7, 7], "G": [2]}
FSL2_LOGICAL = {
    "A": [1, 2],
    "B": [2, 1],
    "C": [3, 3],
    "D": [1, None],
    "E": None,
    "F": [7, 7],
    "G": [2, 2],
}

BASE_LEFT = [
    (1, "A", 1, 1),
    (2, "B", 2, 2),
    (3, "A", 3, 1),
    (4, "C", 4, 2),
    (5, "E", 5, 1),
    (6, "D", 6, 2),
    (7, "F", 7, 1),
    (8, "A", 0, 1),
]
BASE_RIGHT = [
    (10, "A", 1, "p"),
    (11, "A", 2, "q"),
    (12, "B", 2, "r"),
    (13, "C", 3, "s"),
    (14, "E", 1, "t"),
    (15, "D", 2, "u"),
    (16, "G", 1, "v"),
]
ONE_LEFT = [(1, "A", 1, 1)]
ONE_RIGHT = [(10, "B", 1, "p")]
CYCLE = ["A", "B", "C", "E", "D", "F", "G"]
BIG_LEFT = [(i + 1, CYCLE[i % 7], i + 1, i % 2 + 1) for i in range(1000)]
BIG_RIGHT = [(10001 + i, CYCLE[(i + 3) % 7], i % 2 + 1, f"r{i}") for i in range(1000)]
DATASETS = {
    "base": (BASE_LEFT, BASE_RIGHT),
    "one": (ONE_LEFT, ONE_RIGHT),
    "big": (BIG_LEFT, BIG_RIGHT),
}
SIZES = {"base": (8, 7), "one": (1, 1), "big": (1000, 1000)}

DF_JT = {
    "inner": "inner",
    "left": "left",
    "right": "right",
    "full": "full",
    "semi": "left_semi",
    "anti": "left_anti",
}
SQL_JT = {
    "inner": "JOIN",
    "left": "LEFT JOIN",
    "right": "RIGHT JOIN",
    "full": "FULL OUTER JOIN",
    "semi": "LEFT SEMI JOIN",
    "anti": "LEFT ANTI JOIN",
}
NATIVE_TO_SQL_FORM = {"native_using": "sql_using", "native_on": "sql_on"}
FULL_FORMS = ["df_str", "df_list", "df_expr", "df_expr_and", "sql_using", "sql_on"]
MORE_JT = ["left", "right", "full", "semi", "anti"]


def elements(spec: Any, letter: Any) -> Any:
    table = FSL2_LOGICAL if spec.get("container") == "FixedSizeList" else LOGICAL
    return table[letter]


def sql_type(container: Any, elem: Any) -> Any:
    if container == "FixedSizeList":
        return f"FixedSizeList(2, {ELEM_SQL[elem]})"
    return f"{container}({ELEM_SQL[elem]})"


def spark_ddl(spec: Any) -> Any:
    if "nest" in spec:
        return "struct<a:array<bigint>>"
    return f"array<{ELEM_DDL[spec['elem']]}>"


def spark_elem(elem: Any, v: Any) -> Any:
    if elem == "I64":
        return v
    if elem == "UTF8":
        return None if v is None else chr(96 + v)
    return (None, None) if v is None else (v, chr(96 + v))


def spark_key(spec: Any, letter: Any) -> Any:
    vals = elements(spec, letter)
    if "nest" in spec:
        inner = None if vals is None else [spark_elem(spec["elem"], v) for v in vals]
        return (inner,)
    if vals is None:
        return None
    return [spark_elem(spec["elem"], v) for v in vals]


def sql_elem(elem: Any, v: Any) -> Any:
    if elem == "I64":
        return f"CAST({v} AS BIGINT)" if v is not None else "CAST(NULL AS BIGINT)"
    if elem == "UTF8":
        return f"'{chr(96 + v)}'" if v is not None else "CAST(NULL AS VARCHAR)"
    if v is None:
        return "named_struct('x', CAST(NULL AS BIGINT), 'y', CAST(NULL AS VARCHAR))"
    return f"named_struct('x', CAST({v} AS BIGINT), 'y', '{chr(96 + v)}')"


def sql_list(container: Any, elem: Any, vals: Any) -> Any:
    t = sql_type(container, elem)
    if vals is None:
        return f"arrow_cast(NULL, '{t}')"
    if not vals:
        return f"arrow_cast(make_array(), '{t}')"
    items = ", ".join(sql_elem(elem, v) for v in vals)
    return f"arrow_cast(array({items}), '{t}')"


def sql_key(spec: Any, letter: Any) -> Any:
    vals = elements(spec, letter)
    if "nest" in spec:
        return f"named_struct('a', {sql_list(spec['nest'], 'I64', vals)})"
    return sql_list(spec["container"], spec["elem"], vals)


def build_views(leg: Any, s: Any, spec: Any, ds: Any) -> Any:
    left, right = DATASETS[ds]
    if leg == "spark":
        ddl_k = spark_ddl(spec)
        ldf = s.createDataFrame(
            [(lid, spark_key(spec, letter), idv, j) for lid, letter, idv, j in left],
            f"lid bigint, k {ddl_k}, id bigint, j int",
        )
        rdf = s.createDataFrame(
            [(rid, spark_key(spec, letter), j, rv) for rid, letter, j, rv in right],
            f"rid bigint, k {ddl_k}, j int, rv string",
        )
        ldf.createOrReplaceTempView("lv_l")
        rdf.createOrReplaceTempView("lv_r")
        return
    lsql = " UNION ALL ".join(
        f"SELECT CAST({lid} AS BIGINT) AS lid, {sql_key(spec, letter)} AS k, "
        f"CAST({idv} AS BIGINT) AS id, CAST({j} AS INT) AS j"
        for lid, letter, idv, j in left
    )
    rsql = " UNION ALL ".join(
        f"SELECT CAST({rid} AS BIGINT) AS rid, {sql_key(spec, letter)} AS k, "
        f"CAST({j} AS INT) AS j, '{rv}' AS rv"
        for rid, letter, j, rv in right
    )
    s.sql(f"CREATE OR REPLACE TEMPORARY VIEW lv_l AS {lsql}").collect()
    s.sql(f"CREATE OR REPLACE TEMPORARY VIEW lv_r AS {rsql}").collect()


def reached_types(leg: Any, s: Any) -> Any:
    out = {}
    for name in ("lv_l", "lv_r"):
        df = s.table(name)
        out[f"{name}_schema"] = df.schema["k"].dataType.simpleString()
        try:
            if leg == "main":
                out[name] = str(df.toArrow().schema.field("k").type)
            else:
                out[name] = df.schema["k"].dataType.simpleString()
        except Exception as error:
            out[name] = (
                f"ERR {type(error).__name__}: "
                f"{str(error).splitlines()[0][:120] if str(error) else ''}"
            )
    return out


def norm(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "asDict"):
        value = value.asDict()
    if isinstance(value, dict):
        return [norm(value[key]) for key in sorted(value)]
    if isinstance(value, (list, tuple)):
        return [norm(item) for item in value]
    return value


def record(collect: Any) -> Any:
    try:
        rows = collect()
    except Exception as error:
        message = str(error).splitlines()[0][:200] if str(error) else ""
        return {"status": "error", "error_class": type(error).__name__, "error_msg": message}
    lines = sorted(json.dumps(norm(row), sort_keys=True, default=str) for row in rows)
    digest = hashlib.sha256("\n".join(lines).encode()).hexdigest()[:16]
    return {"status": "ok", "n": len(lines), "sha": digest, "sample": lines[:3]}


def join_df(leg: Any, s: Any, fn: Any, native_sql: Any, form: Any, jt: Any) -> Any:
    how = DF_JT[jt]
    cols = ["lid"] if jt in ("semi", "anti") else ["lid", "rid"]
    sql_cols = ", ".join(cols)
    if form in ("sql_using", "sql_on", "native_using", "native_on"):
        clause = "USING (k)" if form.endswith("using") else "ON l.k = r.k"
        alias = "" if form.endswith("using") else " l"
        ralias = "" if form.endswith("using") else " r"
        query = f"SELECT {sql_cols} FROM lv_l{alias} {SQL_JT[jt]} lv_r{ralias} {clause}"
        if form.startswith("native"):
            if native_sql is None:
                raise RuntimeError("repark.sql is not exposed")
            return native_sql(query)
        return s.sql(query)
    left = s.table("lv_l")
    right = s.table("lv_r")
    if form == "df_str":
        df = left.join(right, "k", how)
    elif form == "df_list":
        df = left.join(right, ["k"], how)
    elif form == "df_expr":
        df = left.alias("l").join(right.alias("r"), fn.col("l.k") == fn.col("r.k"), how)
    elif form == "df_expr_and":
        cond = (fn.col("l.k") == fn.col("r.k")) & (fn.col("l.id") > 0)
        df = left.alias("l").join(right.alias("r"), cond, how)
    elif form == "df_two":
        df = left.join(right, ["k", "j"], how)
    else:
        raise ValueError(form)
    return df.select(*cols)


def explain_text(df: Any) -> Any:
    buffer = StringIO()
    with redirect_stdout(buffer):
        df.explain(True)
    lines = buffer.getvalue().splitlines()[:60]
    return "\n".join(line[:220] for line in lines)


def join_cell(
    leg: Any, s: Any, fn: Any, native_sql: Any, kind: Any, form: Any, jt: Any, ds: Any
) -> Any:
    rec = {
        "cell_id": f"join|{kind}|{form}|{jt}|{ds}",
        "leg": leg,
        "kind": kind,
        "form": form,
        "jt": jt,
        "ds": ds,
    }
    rec["outcome"] = record(lambda: join_df(leg, s, fn, native_sql, form, jt).collect())
    if leg == "main" and form in ("df_str", "sql_on") and jt == "inner" and ds == "base":
        try:
            rec["explain"] = explain_text(join_df(leg, s, fn, native_sql, form, jt))
        except Exception as error:
            rec["explain"] = (
                f"ERR {type(error).__name__}: "
                f"{str(error).splitlines()[0][:200] if str(error) else ''}"
            )
    return rec


def dml_cell(leg: Any, s: Any, op: Any, kind: Any) -> Any:
    table = f"ice.db.dml_{op}_{kind.lower()}"
    rec = {"cell_id": f"dml|{op}|{kind}", "leg": leg, "kind": kind, "op": op, "ds": "base"}
    try:
        s.sql("CREATE NAMESPACE IF NOT EXISTS ice.db").collect()
        s.sql(f"DROP TABLE IF EXISTS {table}").collect()
        s.sql(
            f"CREATE TABLE {table} (lid BIGINT, rid BIGINT) USING iceberg TBLPROPERTIES "
            "('format-version'='2','write.delete.mode'='copy-on-write','write.update.mode'='copy-on-write','write.merge.mode'='copy-on-write')"
        ).collect()
        if op == "ins":
            s.sql(
                f"INSERT INTO {table} SELECT l.lid, r.rid FROM lv_l l JOIN lv_r r ON l.k = r.k"
            ).collect()
        else:
            s.table("lv_l").join(s.table("lv_r"), "k", "inner").select("lid", "rid").writeTo(
                table
            ).append()
    except Exception as error:
        rec["outcome"] = {
            "status": "error",
            "error_class": type(error).__name__,
            "error_msg": str(error).splitlines()[0][:200] if str(error) else "",
            "stage": "write",
        }
        return rec
    rec["outcome"] = record(lambda: s.sql(f"SELECT lid, rid FROM {table}").collect())
    return rec


def agg_cell(leg: Any, s: Any, fn: Any, kind: Any) -> Any:
    rec = {"cell_id": f"agg|{kind}", "leg": leg, "kind": kind, "op": "agg", "ds": "base"}
    rec["outcome"] = record(
        lambda: (
            s.table("lv_l")
            .join(s.table("lv_r"), "k", "inner")
            .agg(fn.count(fn.lit(1)).alias("c"), fn.sum("lid").alias("s"))
            .collect()
        )
    )
    return rec


def plan_join(leg: Any) -> Any:
    cells = []
    for kind in KINDS:
        for form in FULL_FORMS:
            cells.append((kind, form, "inner", "base"))
        for form in ("df_str", "sql_on"):
            for jt in MORE_JT:
                cells.append((kind, form, jt, "base"))
        cells.append((kind, "df_two", "inner", "base"))
        for form in ("df_str", "sql_on"):
            for ds in ("one", "big"):
                cells.append((kind, form, "inner", ds))
        if leg == "main":
            for form in ("native_using", "native_on"):
                cells.append((kind, form, "inner", "base"))
    return cells


def dml_ops(kind: Any) -> Any:
    ops = [("agg", kind)]
    if KINDS[kind]["twin"] is not None:
        ops = [("ins", kind), ("wt", kind), *ops]
    return ops


def make_spark() -> Any:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as fn

    s = (
        SparkSession.builder.master("local[4]")
        .appName("lv-step0-spark")
        .config("spark.jars", ICEBERG_JAR)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", f"file://{OUT / 'warehouse-spark'}")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    return s, fn, None


def make_main() -> Any:
    import repark
    from repark import ReparkSession
    from repark.spark import functions as fn

    s = (
        ReparkSession.builder.appName("lv-step0-main")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", f"file://{OUT / 'warehouse-main'}")
        .getOrCreate()
    )
    return s, fn, getattr(repark, "sql", None)


def meta(leg: Any, s: Any) -> Any:
    import pyarrow

    info = {"leg": leg, "python": sys.version.split()[0], "pyarrow": pyarrow.__version__}
    try:
        if leg == "spark":
            import pyspark

            info["pyspark"] = pyspark.__version__
            info["spark_version"] = s.version
        else:
            import repark
            import repark._native as native

            info["repark_file"] = repark.__file__
            info["debug_assertions"] = native.__debug_assertions__
        info["ansi_enabled"] = s.conf.get("spark.sql.ansi.enabled", None)
    except Exception as error:
        info["meta_error"] = (
            f"{type(error).__name__}: {str(error).splitlines()[0][:160] if str(error) else ''}"
        )
    return info


def run_leg(leg: Any, kinds: Any, dss: Any) -> None:
    s, fn, native_sql = make_spark() if leg == "spark" else make_main()
    RAW.mkdir(parents=True, exist_ok=True)
    path = RAW / f"{leg}.jsonl"
    count = 0
    with path.open("w") as handle:
        handle.write(json.dumps({"meta": meta(leg, s)}) + "\n")
        handle.flush()
        for kind in kinds:
            for ds in dss:
                cells = [cell for cell in plan_join(leg) if cell[0] == kind and cell[3] == ds]
                if not cells:
                    continue
                try:
                    build_views(leg, s, KINDS[kind], ds)
                    reached = reached_types(leg, s)
                except Exception as error:
                    message = str(error).splitlines()[0][:200] if str(error) else ""
                    reached = {"build_error": f"{type(error).__name__}: {message}"}
                    for cell in cells:
                        rec = {
                            "cell_id": f"join|{cell[0]}|{cell[1]}|{cell[2]}|{cell[3]}",
                            "leg": leg,
                            "kind": cell[0],
                            "form": cell[1],
                            "jt": cell[2],
                            "ds": cell[3],
                            "reached": reached,
                            "outcome": {
                                "status": "error",
                                "error_class": "BuildError",
                                "error_msg": message,
                            },
                        }
                        handle.write(json.dumps(rec) + "\n")
                        count += 1
                    continue
                for kind_, form, jt, ds_ in cells:
                    rec = join_cell(leg, s, fn, native_sql, kind_, form, jt, ds_)
                    rec["reached"] = reached
                    handle.write(json.dumps(rec) + "\n")
                    handle.flush()
                    count += 1
                if ds == "base":
                    for op, op_kind in dml_ops(kind):
                        if op == "agg":
                            rec = agg_cell(leg, s, fn, op_kind)
                        else:
                            rec = dml_cell(leg, s, op, op_kind)
                        rec["reached"] = reached
                        handle.write(json.dumps(rec) + "\n")
                        handle.flush()
                        count += 1
                print(f"{leg} {kind} {ds} done, cells so far {count}", flush=True)
    s.stop()
    print(f"{leg} finished, {count} records in {path}", flush=True)


def load(path: Any) -> Any:
    records = {}
    if not path.exists():
        return records, {}
    meta_info = {}
    for line in path.read_text().splitlines():
        row = json.loads(line)
        if "meta" in row:
            meta_info = row["meta"]
            continue
        records[row["cell_id"]] = row
    return records, meta_info


def label_of(rec: Any, ref: Any) -> Any:
    kind_type = rec["cell_id"].split("|")[0]
    outcome = rec["outcome"]
    if ref is None:
        return "NO-REFERENCE"
    oracle = ref["outcome"]
    if outcome["status"] != "ok" and oracle["status"] != "ok":
        return "BOTH-REFUSED"
    if outcome["status"] != "ok":
        return "REFUSED-WHERE-SPARK-ANSWERED"
    if oracle["status"] != "ok":
        return "ACCEPTED-WHERE-SPARK-REFUSED"
    if outcome["sha"] == oracle["sha"] and outcome["n"] == oracle["n"]:
        return "EQUAL"
    if kind_type == "dml":
        return "WRONG-WRITE"
    if kind_type == "join":
        ds = rec["ds"]
        if outcome["n"] == SIZES[ds][0] * SIZES[ds][1]:
            return "CROSS-PRODUCT"
    return "WRONG-ANSWER"


def same_outcome(a: Any, b: Any) -> Any:
    if a["status"] != b["status"]:
        return False
    if a["status"] == "ok":
        return a["sha"] == b["sha"] and a["n"] == b["n"]
    return a["error_class"] == b["error_class"]


def short(outcome: Any) -> Any:
    if outcome["status"] == "ok":
        return f"ok n={outcome['n']} sha={outcome['sha']}"
    return f"{outcome['error_class']}: {outcome.get('error_msg', '')[:90]}"


def ref_id(cell_id: Any) -> Any:
    parts = cell_id.split("|")
    if parts[0] == "join":
        parts[2] = NATIVE_TO_SQL_FORM.get(parts[2], parts[2])
    return "|".join(parts)


def twin_id(cell_id: Any, twin: Any) -> Any:
    parts = cell_id.split("|")
    if parts[0] != "join":
        return None
    parts[1] = twin
    return "|".join(parts)


def render() -> None:
    spark, spark_meta = load(RAW / "spark.jsonl")
    main, main_meta = load(RAW / "main.jsonl")
    rows = []
    labelled = {}
    for cell_id, rec in main.items():
        ref = spark.get(ref_id(cell_id))
        label = label_of(rec, ref)
        invariance = "-"
        if rec["cell_id"].startswith("join|"):
            twin = KINDS[rec["kind"]]["twin"]
            if twin is None:
                invariance = "NO-TWIN"
            else:
                twin_rec = main.get(twin_id(cell_id, twin))
                if twin_rec is None:
                    invariance = "NO-TWIN-RUN"
                else:
                    invariance = (
                        "INVARIANT"
                        if same_outcome(rec["outcome"], twin_rec["outcome"])
                        else "VARIANT-DIFFERS"
                    )
        labelled[cell_id] = (label, invariance)
        rows.append((rec, label, invariance, ref))
    spark_rows = []
    for rec in spark.values():
        label = "ORACLE" if rec["outcome"]["status"] == "ok" else "ORACLE-REFUSED"
        spark_rows.append((rec, label))

    lines = []
    lines.append("# LISTVIEW-INNER-JOIN-KEY-1 step-0 table")
    lines.append("")
    lines.append(
        f"Cells: spark {len(spark)}, main {len(main)}. "
        f"Spark meta: `{json.dumps(spark_meta)}`. Main meta: `{json.dumps(main_meta)}`."
    )
    lines.append("")
    lines.append(
        "Labels are against the Spark leg for the same kind, form, join type and dataset. "
        "Native forms are compared with the Spark `sql_using` and `sql_on` cells. "
        "DML labels are on the persisted rows."
    )
    lines.append("")

    def pivot(title: Any, dim_index: Any) -> None:
        counts = Counter()
        keys = set()
        for rec, label, _, _ in rows:
            parts = rec["cell_id"].split("|")
            if parts[0] != "join":
                continue
            key = parts[dim_index]
            counts[(key, label)] += 1
            keys.add(key)
        labels = sorted({label for _, label, _, _ in rows})
        lines.append(f"## Counts: label by {title}")
        lines.append("")
        lines.append("| " + title + " | " + " | ".join(labels) + " |")
        lines.append("|" + "---|" * (len(labels) + 1))
        for key in sorted(keys):
            lines.append(
                f"| {key} | " + " | ".join(str(counts[(key, label)]) for label in labels) + " |"
            )
        lines.append("")

    pivot("key type", 1)
    pivot("join form", 2)
    pivot("join type", 3)

    combo = Counter()
    for rec, label, _, _ in rows:
        parts = rec["cell_id"].split("|")
        if parts[0] == "join":
            combo[(label, parts[1], parts[2], parts[3])] += 1
    lines.append("## Counts: label by key type, join form and join type (datasets summed)")
    lines.append("")
    lines.append("| label | key type | join form | join type | cells |")
    lines.append("|---|---|---|---|---|")
    for (label, kind, form, jt), count in sorted(combo.items()):
        lines.append(f"| {label} | {kind} | {form} | {jt} | {count} |")
    lines.append("")

    lines.append("## Wrong list (WRONG-ANSWER or WRONG-WRITE, excluding cross products)")
    lines.append("")
    for rec, label, _invariance, ref in rows:
        if label in ("WRONG-ANSWER", "WRONG-WRITE"):
            lines.append(
                f"- `{rec['cell_id']}` {label} main: {short(rec['outcome'])} / "
                f"spark: {short(ref['outcome']) if ref else 'none'}"
            )
            lines.append(
                f"  - main sample: `{rec['outcome'].get('sample', [''])[:1]}` "
                f"spark sample: `{ref['outcome'].get('sample', [''])[:1] if ref else ''}`"
            )
    lines.append("")

    lines.append("## Cross-product list (row count equals left x right)")
    lines.append("")
    for rec, label, _invariance, ref in rows:
        if label == "CROSS-PRODUCT":
            lines.append(
                f"- `{rec['cell_id']}` main: {short(rec['outcome'])} / "
                f"spark: {short(ref['outcome']) if ref else 'none'}"
            )
            lines.append(f"  - main sample: `{rec['outcome'].get('sample', [''])[:1]}`")
    lines.append("")

    lines.append("## Cell table (main leg, labelled)")
    lines.append("")
    lines.append(
        "| cell | key left | key right | main outcome | spark outcome | label | kind invariance |"
    )
    lines.append("|---|---|---|---|---|---|---|")
    for rec, label, invariance, ref in rows:
        reached = rec.get("reached", {})
        lines.append(
            f"| {rec['cell_id']} | {reached.get('lv_l', '')} | {reached.get('lv_r', '')} | "
            f"{short(rec['outcome'])} | {short(ref['outcome']) if ref else 'none'} | "
            f"{label} | {invariance} |"
        )
    lines.append("")
    lines.append("## Cell table (spark oracle leg)")
    lines.append("")
    lines.append("| cell | key left | key right | spark outcome | label |")
    lines.append("|---|---|---|---|---|")
    for rec, label in spark_rows:
        reached = rec.get("reached", {})
        lines.append(
            f"| {rec['cell_id']} | {reached.get('lv_l', '')} | {reached.get('lv_r', '')} | "
            f"{short(rec['outcome'])} | {label} |"
        )
    (OUT / "table.md").write_text("\n".join(lines) + "\n")

    explain_lines = []
    for rec, label, _invariance, _ref in rows:
        if label != "EQUAL" and rec.get("explain"):
            explain_lines.append(f"### {rec['cell_id']} ({label})")
            explain_lines.append("```")
            explain_lines.append(rec["explain"])
            explain_lines.append("```")
            explain_lines.append("")
    (OUT / "explain_failing.md").write_text("\n".join(explain_lines) + "\n")
    print(f"rendered table.md with {len(rows)} main cells and {len(spark_rows)} spark cells")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leg", choices=["spark", "main"])
    parser.add_argument("--render", action="store_true")
    parser.add_argument("--kinds", default=",".join(KINDS))
    parser.add_argument("--dss", default="base,one,big")
    parser.add_argument("--out", type=Path, default=Path())
    args = parser.parse_args()
    global OUT, RAW
    OUT = args.out
    RAW = OUT / "raw"
    if args.render:
        render()
        return
    run_leg(args.leg, args.kinds.split(","), args.dss.split(","))


if __name__ == "__main__":
    main()
