from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import shutil
import sys
from collections import Counter
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
RAW = HERE / "raw"
TABLE = HERE / "delta2-table.md"
NS = "ns"
ANSWERED = "ANSWERED"
REFUSED = "REFUSED"
PANIC = "PANIC"
LEGS = ("spark", "head", "base")
ICEBERG_TAIL = (
    " USING iceberg TBLPROPERTIES ('format-version' = '2', "
    "'write.delete.mode' = 'copy-on-write', 'write.update.mode' = 'copy-on-write')"
)
NATIVE_CONFIG = {"repark.sql.allowCreateFormatVersion3": "true"}
L = "named_struct('a', 1, 'b', 2)"
R = "named_struct('b', 3, 'a', 4)"
RS = "named_struct('a', 3, 'b', 4)"
T_LR = "s STRUCT<a: INT, b: INT>"
T_X = "s STRUCT<x: STRUCT<a: INT, b: INT>>"
N0 = ("S", "SELECT NULL AS s")
NL = ("S", f"SELECT {L} AS s")
NR = ("S", f"SELECT {R} AS s")
NRS = ("S", f"SELECT {RS} AS s")
LR_UNION = f"SELECT {L} AS s UNION ALL SELECT {R} AS s"
LRS_UNION = f"SELECT {L} AS s UNION ALL SELECT {RS} AS s"
ANCHOR = "named_struct('a', 1, 'b', 10)"
REC_REORDERED = "named_struct('b', r.s.b + 1, 'a', r.s.a + 11)"
REC_SAME = "named_struct('a', r.s.a + 11, 'b', r.s.b + 1)"
EXPRS = {
    "F1": f"(SELECT max(s.a) FROM (SELECT {L} AS s UNION ALL SELECT {R} AS s))",
    "int": "(SELECT max(x) FROM (SELECT 3 AS x UNION ALL SELECT 2 AS x))",
    "same": (
        f"(SELECT max(s.a) FROM (SELECT {L} AS s UNION ALL "
        "SELECT named_struct('a', 4, 'b', 5) AS s))"
    ),
}


def legs_of(name: str) -> tuple[str, ...]:
    return {"shared": ("spark", "head", "base"), "repark": ("head", "base"), "spark": ("spark",)}[
        name
    ]


def make_cells() -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []

    def add(
        row: Any,
        variant: str,
        door: str,
        legs: str,
        kind: str,
        payload: Any,
        ddl: str,
        forms: tuple[str, ...] = ("read", "insert"),
        twin: str | None = None,
    ) -> None:
        for form in forms:
            cid = f"{row}.{variant}.{door}.{form}"
            if twin == "self":
                twin_id = cid
            elif twin:
                twin_id = twin.format(form=form)
            else:
                twin_id = None
            cells.append(
                {
                    "id": cid,
                    "row": str(row),
                    "variant": variant,
                    "door": door,
                    "form": form,
                    "legs": legs_of(legs),
                    "kind": kind,
                    "payload": payload,
                    "ddl": ddl,
                    "twin": twin_id,
                }
            )

    def sql_and_df(row: Any, variant: str, text: str, tree: Any, ddl: str) -> None:
        add(row, variant, "sql", "shared", "query", text, ddl, twin="self")
        add(row, variant, "df", "shared", "tree", tree, ddl, twin="self")
        add(row, variant, "native", "repark", "query", text, ddl)

    union_all = f"SELECT NULL AS s UNION ALL SELECT {L} AS s UNION ALL SELECT {R} AS s"
    sql_and_df(1, "union-all", union_all, ("U", ("U", N0, NL), NR), T_LR)
    sql_and_df(
        1, "union", union_all.replace("UNION ALL", "UNION"), ("UD", ("UD", N0, NL), NR), T_LR
    )

    x0 = "SELECT named_struct('x', NULL) AS s"
    x1 = f"SELECT named_struct('x', {L}) AS s"
    x2 = f"SELECT named_struct('x', {R}) AS s"
    sql_and_df(
        2,
        "union-all",
        f"{x0} UNION ALL {x1} UNION ALL {x2}",
        ("U", ("U", ("S", x0), ("S", x1)), ("S", x2)),
        T_X,
    )

    c0 = "SELECT CASE WHEN true THEN NULL END AS s"
    sql_and_df(
        3,
        "union-all",
        f"{c0} UNION ALL SELECT {L} AS s UNION ALL SELECT {R} AS s",
        ("U", ("U", ("S", c0), NL), NR),
        T_LR,
    )

    left4 = f"(SELECT NULL AS s UNION ALL SELECT {L} AS s) "
    sql_and_df(4, "intersect", f"{left4}INTERSECT SELECT {R} AS s", ("I", ("U", N0, NL), NR), T_LR)
    sql_and_df(4, "except", f"{left4}EXCEPT SELECT {R} AS s", ("E", ("U", N0, NL), NR), T_LR)

    k0 = "SELECT CAST(NULL AS STRUCT<a: INT, b: INT>) AS s"
    sql_and_df(
        5,
        "union-all",
        f"{k0} UNION ALL SELECT {L} AS s UNION ALL SELECT {R} AS s",
        ("U", ("U", ("S", k0), NL), NR),
        T_LR,
    )

    sql_and_df(
        6,
        "ctl-null-middle",
        f"SELECT {L} AS s UNION ALL SELECT NULL AS s UNION ALL SELECT {R} AS s",
        ("U", ("U", NL, N0), NR),
        T_LR,
    )
    sql_and_df(
        6,
        "ctl-null-nested",
        f"SELECT NULL AS s UNION ALL (SELECT {L} AS s UNION ALL SELECT {R} AS s)",
        ("U", N0, ("U", NL, NR)),
        T_LR,
    )
    sql_and_df(
        6,
        "ctl-same-names",
        f"SELECT NULL AS s UNION ALL SELECT {L} AS s UNION ALL SELECT {RS} AS s",
        ("U", ("U", N0, NL), NRS),
        T_LR,
    )

    cast_r = f"SELECT CAST({R} AS STRUCT<a: INT, b: INT>) AS s"
    sql_and_df(
        12, "cast-reordered", f"{cast_r} UNION ALL SELECT {L} AS s", ("U", ("S", cast_r), NL), T_LR
    )

    for name, expr in EXPRS.items():
        for shape in ("select", "withColumn", "filter"):
            add(
                10,
                f"{name}-{shape}",
                "df",
                "shared",
                "tree",
                frame_builder(expr, shape),
                "v INT",
                twin="self",
            )

    add(11, "create-view", "sql", "repark", "view", ("CREATE VIEW", LR_UNION, "V"), T_LR)
    add(
        11,
        "create-temporary-view",
        "sql",
        "repark",
        "view",
        ("CREATE TEMPORARY VIEW", LR_UNION, "TV"),
        T_LR,
    )
    add(11, "dataframe-temp-view", "df", "repark", "dfview", ("U", NL, NR), T_LR)

    for variant, verb, body, twin in [
        ("view-create", "CREATE VIEW", LR_UNION, "ref.lr.sql.{form}"),
        ("view-replace", "CREATE OR REPLACE VIEW", LR_UNION, "ref.lr.sql.{form}"),
        ("view-intersect", "CREATE VIEW", f"SELECT {L} AS s INTERSECT SELECT {R} AS s", None),
        ("view-except", "CREATE VIEW", f"SELECT {L} AS s EXCEPT SELECT {R} AS s", None),
        ("view-scalar", "CREATE VIEW", f"SELECT (SELECT s FROM ({LR_UNION}) q LIMIT 1) AS s", None),
        ("view-cte", "CREATE VIEW", f"WITH u AS ({LR_UNION}) SELECT s FROM u", None),
    ]:
        add(7, variant, "native", "repark", "view", (verb, body, "V"), T_LR, twin=twin)

    add(
        8,
        "prepare-union",
        "native",
        "repark",
        "prepare",
        (LR_UNION, "", ""),
        T_LR,
        twin="ref.lr.sql.{form}",
    )
    add(
        8,
        "prepare-param",
        "native",
        "repark",
        "prepare",
        (f"SELECT {L} AS s UNION ALL SELECT named_struct('b', $1, 'a', 4) AS s", "(INT)", "(3)"),
        T_LR,
    )

    def rec(body: str) -> str:
        return (
            f"WITH RECURSIVE r(s) AS (SELECT {ANCHOR} AS s UNION ALL SELECT {body} AS s FROM r "
            "WHERE r.s.b < 11) SELECT s FROM r"
        )

    add(
        9,
        "rec-reordered",
        "native",
        "repark",
        "query",
        rec(REC_REORDERED),
        T_LR,
        twin="9.rec-reordered.sql.{form}",
    )
    add(9, "rec-reordered", "sql", "spark", "query", rec(REC_REORDERED), T_LR)
    add(
        9,
        "rec-same",
        "native",
        "repark",
        "query",
        rec(REC_SAME),
        T_LR,
        twin="9.rec-same.sql.{form}",
    )
    add(9, "rec-same", "sql", "spark", "query", rec(REC_SAME), T_LR)

    add(
        13,
        "view-same-name",
        "native",
        "repark",
        "view",
        ("CREATE VIEW", LRS_UNION, "V"),
        T_LR,
        twin="ref.lrs.sql.{form}",
    )
    add(
        13,
        "prepare-same-name",
        "native",
        "repark",
        "prepare",
        (LRS_UNION, "", ""),
        T_LR,
        twin="ref.lrs.sql.{form}",
    )

    add("ref", "lr", "sql", "spark", "query", LR_UNION, T_LR)
    add("ref", "lrs", "sql", "spark", "query", LRS_UNION, T_LR)
    return cells


def frame_builder(expr: str, shape: str) -> Any:
    def build_frame(door: dict[str, Any], names: dict[str, str]) -> Any:
        fx = door["functions"]
        base = door["sql"]("SELECT 1 AS one")
        if shape == "select":
            return base.select(fx.expr(expr).alias("v"))
        if shape == "withColumn":
            return base.withColumn("v", fx.expr(expr)).select("v")
        return base.filter(fx.expr(f"{expr} IS NOT NULL")).select(fx.expr(expr).alias("v"))

    return build_frame


def build(door: dict[str, Any], names: dict[str, str], node: Any) -> Any:
    if callable(node):
        return node(door, names)
    tag = node[0]
    if tag == "S":
        return door["sql"](node[1])
    left = build(door, names, node[1])
    right = build(door, names, node[2])
    if tag == "U":
        return left.union(right)
    if tag == "UD":
        return left.union(right).distinct()
    if tag == "I":
        return left.intersect(right)
    return left.subtract(right)


def sql_action(template: str) -> Any:
    return lambda door, names: door["sql"](template.format(**names))


def insert_action(body: str) -> Any:
    return lambda door, names: door["sql"](("INSERT INTO {T} " + body).format(**names))


def frame_action(node: Any) -> Any:
    return lambda door, names: build(door, names, node)


def frame_insert(node: Any) -> Any:
    return lambda door, names: build(door, names, node).write.insertInto(names["T"])


def dfview_action(node: Any) -> Any:
    return lambda door, names: build(door, names, node).createOrReplaceTempView(names["DV"])


def setup_action(door: dict[str, Any], names: dict[str, str]) -> Any:
    return door["setup"](names)


def steps_for(cell: dict[str, Any], door: dict[str, Any]) -> list[tuple[str, Any, bool]]:
    form = cell["form"]
    kind = cell["kind"]
    payload = cell["payload"]
    ddl = cell["ddl"]
    head = [("create", setup_action, False)]
    create_table = [("create", sql_action(f"CREATE TABLE {{T}} ({ddl}){door['tail']}"), False)]
    persisted = ("read", sql_action("SELECT * FROM {T}"), True)
    if kind == "prepare":
        body, decl, args = payload
        if form == "read":
            return [
                *head,
                ("prepare", sql_action(f"PREPARE {{P}}{decl} AS {body}"), False),
                ("execute", sql_action(f"EXECUTE {{P}}{args}"), True),
            ]
        return (
            head
            + create_table
            + [
                ("prepare", sql_action(f"PREPARE {{P}}{decl} AS INSERT INTO {{T}} {body}"), False),
                ("insert", sql_action(f"EXECUTE {{P}}{args}"), False),
                persisted,
            ]
        )
    if kind == "query":
        if form == "read":
            return [*head, ("read", sql_action(payload), True)]
        return head + create_table + [("insert", insert_action(payload), False), persisted]
    if kind == "view":
        verb, body, key = payload
        create_view = ("create", sql_action(f"{verb} {{{key}}} AS {body}"), False)
        source = f"SELECT * FROM {{{key}}}"
        if form == "read":
            return [*head, create_view, ("read", sql_action(source), True)]
        return [
            *head,
            create_view,
            *create_table,
            ("insert", insert_action(source), False),
            persisted,
        ]
    if kind == "tree":
        if form == "read":
            return [*head, ("read", frame_action(payload), True)]
        return head + create_table + [("insert", frame_insert(payload), False), persisted]
    create_view = ("create", dfview_action(payload), False)
    source = "SELECT * FROM {DV}"
    if form == "read":
        return [*head, create_view, ("read", sql_action(source), True)]
    return [
        *head,
        create_view,
        *create_table,
        ("insert", insert_action(source), False),
        persisted,
    ]


def canonical(frame: Any) -> list[str]:
    if type(frame).__module__.startswith("pyspark"):
        rows = [row.asDict(recursive=True) for row in frame.collect()]
    else:
        table = frame.toArrow() if hasattr(frame, "toArrow") else frame.to_arrow()
        rows = table.to_pylist()
    return sorted(json.dumps(row, sort_keys=True, default=str) for row in rows)


def attempt(call: Any) -> tuple[Any, Any]:
    try:
        return call(), None
    except KeyboardInterrupt:
        raise
    except BaseException as error:
        return None, error


def error_record(error: BaseException, base: Path, stage: str) -> dict[str, Any]:
    text = str(error).replace("\n", " ").replace(str(base), "<wh>")
    panic = type(error).__name__ == "PanicException" or "panicked at" in text
    return {"class": type(error).__name__, "stage": stage, "text": text[:200], "panic": panic}


def observe(root: Path) -> dict[str, int]:
    if not root.exists():
        return {"metadata": 0, "data": 0}
    return {
        "metadata": sum(1 for _ in root.rglob("*.metadata.json")),
        "data": sum(1 for _ in root.rglob("*.parquet")),
    }


def execute(
    door: Any,
    names: dict[str, str],
    steps: list[tuple[str, Any, bool]],
    record: dict[str, Any],
    root: Path,
) -> dict[str, Any]:
    result = None
    for stage, action, keep in steps:
        before = observe(root) if stage == "insert" else None
        value, error = attempt(lambda action=action: action(door, names))
        if error is None and value is not None:
            rows, error = attempt(lambda value=value: canonical(value))
            if keep and error is None:
                result = rows
        if stage == "insert":
            after = observe(root)
            record.update(snapshot_changed=after != before, files_before=before, files_after=after)
        if error is not None:
            record.update(
                outcome=PANIC if error_record(error, root, stage)["panic"] else REFUSED,
                stage=stage,
                error=error_record(error, root, stage),
                result=None,
            )
            return record
    record.update(outcome=ANSWERED, stage=None, error=None, result=result)
    return record


def names_for(index: int) -> dict[str, str]:
    cat = f"c{index:03d}"
    return {
        "cat": cat,
        "T": f"{cat}.{NS}.t",
        "V": f"v{index:03d}",
        "P": f"p{index:03d}",
        "TV": f"tv{index:03d}",
        "DV": f"dv{index:03d}",
    }


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def spark_context(
    arguments: argparse.Namespace, base: Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as spark_functions

    session = (
        SparkSession.builder.master("local[1]")
        .appName("probe-delta2")
        .config("spark.ui.enabled", "false")
        .config("spark.local.dir", str(base / "spark-local"))
        .config("spark.jars", arguments.ivy_jar)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")

    def setup(names: dict[str, str]) -> None:
        cat = names["cat"]
        session.conf.set(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
        session.conf.set(f"spark.sql.catalog.{cat}.type", "hadoop")
        session.conf.set(f"spark.sql.catalog.{cat}.warehouse", str(base / cat))
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.{NS}").toArrow()

    door = {"sql": session.sql, "functions": spark_functions, "tail": ICEBERG_TAIL, "setup": setup}
    jar = Path(arguments.ivy_jar)
    header = {
        "record": "leg",
        "leg": "spark",
        "spark_version": session.version,
        "jdk_version": session.sparkContext._jvm.System.getProperty("java.version"),
        "iceberg_jar": jar.name,
        "iceberg_jar_sha256": sha256_of(jar),
        "ansi_enabled": session.conf.get("spark.sql.ansi.enabled"),
        "case_sensitive": session.conf.get("spark.sql.caseSensitive"),
        "python": sys.version.split()[0],
        "venv": sys.prefix,
    }
    return {"sql": door, "df": door, "native": None, "close": session.stop}, header


def repark_context(
    arguments: argparse.Namespace, base: Path
) -> tuple[dict[str, Any], dict[str, Any]]:
    import pyarrow

    import repark
    from repark import ReparkSession
    from repark import functions as repark_functions
    from repark._native import PyReparkSession

    native = PyReparkSession.native(config=NATIVE_CONFIG)
    repark._ANSI_NATIVE = native
    facade = (
        ReparkSession.builder.appName("probe-delta2")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )

    def native_setup(names: dict[str, str]) -> None:
        cat = names["cat"]
        root = base / cat
        native.register_memory_catalog(cat, str(root))
        native.create_namespace(cat, NS, str(root / NS))

    def facade_setup(names: dict[str, str]) -> None:
        cat = names["cat"]
        root = base / cat
        facade.register_memory_catalog(cat, str(root))
        facade.sql(f"CREATE NAMESPACE {cat}.{NS} LOCATION '{root / NS}'").toArrow()

    facade_door = {
        "sql": facade.sql,
        "functions": repark_functions,
        "tail": ICEBERG_TAIL,
        "setup": facade_setup,
    }
    native_door = {
        "sql": repark.sql,
        "functions": repark_functions,
        "tail": "",
        "setup": native_setup,
    }
    header = {
        "record": "leg",
        "leg": arguments.leg,
        "repark_version": importlib.metadata.version("repark"),
        "repark_file": repark.__file__,
        "pyarrow_version": pyarrow.__version__,
        "native_config": NATIVE_CONFIG,
        "python": sys.version.split()[0],
        "venv": sys.prefix,
    }
    return {
        "sql": facade_door,
        "df": facade_door,
        "native": native_door,
        "close": lambda: None,
    }, header


def base_record(leg: str, cell: dict[str, Any]) -> dict[str, Any]:
    return {
        "record": "cell",
        "leg": leg,
        "id": cell["id"],
        "row": cell["row"],
        "variant": cell["variant"],
        "door": cell["door"],
        "form": cell["form"],
    }


def write_record(handle: Any, record: dict[str, Any], base: Path) -> None:
    text = json.dumps(record, sort_keys=True, default=str).replace(str(base), "<wh>")
    handle.write(text + "\n")


def parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Unit C delta-2 set-operation rows (measurement only)."
    )
    parser.add_argument("--leg", choices=LEGS)
    parser.add_argument("--ivy-jar")
    parser.add_argument("--render", action="store_true")
    parser.add_argument("--plan-only", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    arguments = parse_arguments(argv)
    if arguments.render:
        return render()
    plan = make_cells()
    if arguments.plan_only:
        print(f"cells in plan: {len(plan)}")
        return 0
    if arguments.leg is None:
        print("--leg is required", file=sys.stderr)
        return 2
    selected = [(index, cell) for index, cell in enumerate(plan) if arguments.leg in cell["legs"]]
    base = HERE / "work" / arguments.leg
    if base.exists():
        shutil.rmtree(base)
    base.mkdir(parents=True)
    RAW.mkdir(parents=True, exist_ok=True)
    if arguments.leg == "spark":
        if arguments.ivy_jar is None:
            print("--ivy-jar is required for the spark leg", file=sys.stderr)
            return 2
        context, header = spark_context(arguments, base)
    else:
        context, header = repark_context(arguments, base)
    outcomes: Counter[str] = Counter()
    with (RAW / f"{arguments.leg}.jsonl").open("w", encoding="utf-8") as handle:
        write_record(handle, header, base)
        for index, cell in selected:
            door = context[cell["door"]]
            names = names_for(index)
            steps = steps_for(cell, door)
            record = execute(
                door, names, steps, base_record(arguments.leg, cell), base / names["cat"]
            )
            outcomes[record["outcome"]] += 1
            write_record(handle, record, base)
            handle.flush()
    context["close"]()
    print(f"leg {arguments.leg}: cells {len(selected)} outcomes {dict(sorted(outcomes.items()))}")
    return 0


def load(leg: str) -> dict[str, dict[str, Any]]:
    path = RAW / f"{leg}.jsonl"
    records: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return records
    for line in path.read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        if record.get("record") == "cell":
            records[record["id"]] = record
    return records


def compare(cell: dict[str, Any], record: dict[str, Any], reference: dict[str, Any] | None) -> str:
    if reference is None:
        return "ORACLE-MISSING"
    repark_ok = record["outcome"] == ANSWERED
    spark_ok = reference["outcome"] == ANSWERED
    if repark_ok and spark_ok:
        if record["result"] == reference["result"]:
            return "EQUAL"
        return "RESULT-ROWS-DIFFER" if cell["form"] == "read" else "ROWS-DIFFER"
    if not repark_ok and not spark_ok:
        return "BOTH-REFUSED"
    if not repark_ok:
        return "REFUSED-WHERE-SPARK-ACCEPTED"
    return "ACCEPTED-WHERE-SPARK-REFUSED"


def detail(record: dict[str, Any]) -> str:
    if record["outcome"] == ANSWERED:
        text = " ; ".join(record["result"]) if record["result"] else "(no rows)"
    else:
        error = record["error"]
        text = f"{error['class']}: {error['text']}"
    return text.replace("|", "\\|")


def render() -> int:
    plan = make_cells()
    by_leg = {leg: load(leg) for leg in LEGS}
    spark = by_leg["spark"]
    lines = [
        "# Unit C delta-2 acceptance table (measured, not committed)",
        "",
        "Legs: spark = Spark 4.1.2 oracle; head = unit C head b8a703c3; "
        "base = RePark 1.5.6 from PyPI.",
        "Label is against the Spark twin. Head and base cells without a twin are ORACLE-MISSING.",
        "",
        "| id | leg | outcome | stage | label | result or refusal (first 200 chars) |",
        "|---|---|---|---|---|---|",
    ]
    labelled: list[tuple[dict[str, Any], str, dict[str, Any], str]] = []
    for cell in plan:
        for leg in LEGS:
            if leg not in cell["legs"]:
                continue
            record = by_leg[leg].get(cell["id"])
            if record is None:
                continue
            if leg == "spark":
                label = "(reference)" if cell["row"] == "ref" else "(oracle)"
            else:
                reference = spark.get(cell["twin"]) if cell["twin"] else None
                label = compare(cell, record, reference)
            labelled.append((cell, leg, record, label))
            stage = record["stage"] or "-"
            lines.append(
                f"| {cell['id']} | {leg} | {record['outcome']} | {stage} | {label} "
                f"| {detail(record)} |"
            )
    counts: Counter[tuple[str, str, str]] = Counter()
    for cell, leg, _record, label in labelled:
        if leg != "spark":
            counts[(cell["row"], leg, label)] += 1
    lines += [
        "",
        "## Counts by label, row and leg",
        "",
        "| row | leg | label | count |",
        "|---|---|---|---|",
    ]
    for (row, leg, label), count in sorted(
        counts.items(), key=lambda item: (item[0][0], item[0][1], item[0][2])
    ):
        lines.append(f"| {row} | {leg} | {label} | {count} |")
    lines += ["", "## Per row and door (spark / base / head)", ""]
    rows = []
    for cell in plan:
        if cell["row"] not in rows:
            rows.append(cell["row"])
    for row in rows:
        for door in ("sql", "df", "native"):
            group = [
                (c, leg, r, lab)
                for c, leg, r, lab in labelled
                if c["row"] == row and c["door"] == door
            ]
            if not group:
                continue
            parts = []
            for leg in LEGS:
                entries = [r for c, lg, r, _ in group if lg == leg]
                if not entries:
                    continue
                tags = Counter()
                for r in entries:
                    tag = r["outcome"] + (f"@{r['stage']}" if r["stage"] else "")
                    tags[tag] += 1
                labels = Counter(lab for c, lg, _, lab in group if lg == leg and lg != "spark")
                extra = f" labels {dict(sorted(labels.items()))}" if labels else ""
                parts.append(f"{leg} {dict(sorted(tags.items()))}{extra}")
            lines.append(f"- row {row} {door}: " + " | ".join(parts))
    lines += ["", "## Head answers (every cell, one line each)", ""]
    for cell, leg, record, label in labelled:
        if leg == "head" and record["outcome"] == ANSWERED:
            reference = spark.get(cell["twin"]) if cell["twin"] else None
            spark_text = (
                f"spark {reference['outcome']} {' ; '.join(reference['result'] or [])}"
                if reference and reference["outcome"] == ANSWERED
                else (f"spark {reference['outcome']}" if reference else "no spark twin")
            )
            lines.append(
                f"- {cell['id']}: head {' ; '.join(record['result'] or []) or '(no rows)'} "
                f"| {spark_text} | {label}"
            )
    TABLE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {TABLE} with {len(labelled)} labelled cells")
    return 0


if __name__ == "__main__":
    sys.exit(main())
