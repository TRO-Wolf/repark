from __future__ import annotations

import argparse
import importlib.util
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
RAW = HERE / "raw"
SPARK_OUT = RAW / "controls-spark.jsonl"
MAIN_OUT = RAW / "controls-repark-main-fdc61f26.jsonl"
PRIOR_SPARK = RAW / "spark.jsonl"
PRIOR_MAIN = RAW / "repark-main-fdc61f26.jsonl"
TABLE_OUT = HERE / "controls-table.md"
MATRIX_CONTROL = "control"
MATRIX_DISTINCT = "union-distinct"
MATRIX_COMPARISON = "comparison"
UNION = "UNION"
UNION_DISTINCT = "UNION DISTINCT"
PREDICTION_OPERATION = {UNION_DISTINCT: UNION}
COMPARED_OK = (["EQUAL"], ["ORACLE-MISSING"])
SEED_ROWS = "(1, 'a'), (2, 'b')"
COMPARISON_COLUMNS = "id BIGINT, v STRING"
COMPARISON_OPERATION = "INSERT ... SELECT * FROM t WHERE"
ROUTES = ("sql", "filter-str", "f-expr", "typed")
COMPARISONS = (
    ("c1", "named_struct('a', id, 'b', 2) = named_struct('b', 2, 'a', 1)"),
    ("c2", "named_struct('a', id, 'b', CAST(NULL AS INT)) = named_struct('a', 1, 'c', 2)"),
    ("c3", "named_struct('a', id, 'a', 2) = named_struct('a', CAST(1 AS BIGINT), 'a', 1)"),
)
TYPED_TEXT = {
    "c1": (
        "F.struct(F.col('id').alias('a'), F.lit(2).alias('b')) == "
        "F.struct(F.lit(2).alias('b'), F.lit(1).alias('a'))"
    ),
    "c2": (
        "F.struct(F.col('id').alias('a'), F.lit(None).cast('int').alias('b')) == "
        "F.struct(F.lit(1).alias('a'), F.lit(2).alias('c'))"
    ),
    "c3": (
        "F.struct(F.col('id').alias('a'), F.lit(2).alias('a')) == "
        "F.struct(F.lit(1).cast('bigint').alias('a'), F.lit(1).alias('a'))"
    ),
}


def load_base() -> Any:
    spec = importlib.util.spec_from_file_location("probe_setop", HERE / "probe_setop.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


base = load_base()


def identical(node: Any) -> Any:
    if isinstance(node, int):
        return node + 2
    if isinstance(node, str):
        return "y"
    tag, body = node
    if tag == "s":
        return ("s", [(name, identical(child)) for name, child in body])
    if tag == "a":
        return ("a", [identical(body[0])])
    key, child = body[0]
    return ("m", [(key, identical(child))])


def control_shapes() -> list[tuple[str, Any, Any]]:
    return [(f"ctl-{shape}", left, identical(left)) for shape, left, _, _ in base.SHAPES]


def make_spec(
    shape: str, left: Any, right: Any, op: str, form: str, door: str, matrix: str
) -> dict[str, Any]:
    spec = base.make_spec(shape, left, right, op, form, door)
    spec["prediction"] = base.make_prediction(PREDICTION_OPERATION.get(op, op), left, right)
    spec["matrix"] = matrix
    return spec


def control_specs() -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    for shape, left, right in control_shapes():
        for door in ("sql", "native"):
            for op in base.SQL_OPS:
                for form in base.FORMS:
                    specs.append(make_spec(shape, left, right, op, form, door, MATRIX_CONTROL))
        for op in base.DF_OPS:
            for form in base.FORMS:
                specs.append(make_spec(shape, left, right, op, form, "df", MATRIX_CONTROL))
    return specs


def distinct_specs() -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    for shape, left, right, _ in base.SHAPES:
        for door in ("sql", "native"):
            for form in base.FORMS:
                specs.append(
                    make_spec(shape, left, right, UNION_DISTINCT, form, door, MATRIX_DISTINCT)
                )
    return specs


def comparison_specs() -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    for cell_id, predicate in COMPARISONS:
        for route in ROUTES:
            specs.append(
                {
                    "matrix": MATRIX_COMPARISON,
                    "cell_id": cell_id,
                    "predicate": predicate,
                    "route": route,
                    "join": f"{cell_id}|{route}|insert",
                    "table": base.sanitize(f"t_cmp_{cell_id}_{route}"),
                }
            )
    return specs


def plan() -> list[dict[str, Any]]:
    return control_specs() + distinct_specs() + comparison_specs()


def route_argument(spec: dict[str, Any], functions: Any) -> Any:
    if spec["route"] == "filter-str":
        return spec["predicate"]
    if spec["route"] == "f-expr":
        return functions.expr(spec["predicate"])
    return TYPED_BUILDERS[spec["cell_id"]](functions)


def filter_text(spec: dict[str, Any]) -> str:
    if spec["route"] == "filter-str":
        return f'"{spec["predicate"]}"'
    if spec["route"] == "f-expr":
        return f'F.expr("{spec["predicate"]}")'
    return TYPED_TEXT[spec["cell_id"]]


def typed_c1(functions: Any) -> Any:
    left = functions.struct(functions.col("id").alias("a"), functions.lit(2).alias("b"))
    right = functions.struct(functions.lit(2).alias("b"), functions.lit(1).alias("a"))
    return left == right


def typed_c2(functions: Any) -> Any:
    left = functions.struct(
        functions.col("id").alias("a"), functions.lit(None).cast("int").alias("b")
    )
    right = functions.struct(functions.lit(1).alias("a"), functions.lit(2).alias("c"))
    return left == right


def typed_c3(functions: Any) -> Any:
    left = functions.struct(functions.col("id").alias("a"), functions.lit(2).alias("a"))
    right = functions.struct(
        functions.lit(1).cast("bigint").alias("a"), functions.lit(1).alias("a")
    )
    return left == right


TYPED_BUILDERS = {"c1": typed_c1, "c2": typed_c2, "c3": typed_c3}


def comparison_record(ctx: dict[str, Any], spec: dict[str, Any], qualified: str) -> dict[str, Any]:
    return {
        "record": "cell",
        "engine": ctx["engine"],
        "leg": ctx["label"],
        "matrix": MATRIX_COMPARISON,
        "shape": spec["cell_id"],
        "operation": COMPARISON_OPERATION,
        "form": "insert",
        "door": spec["route"],
        "join": spec["join"],
        "table": qualified,
        "predicate": spec["predicate"],
        "column_type": COMPARISON_COLUMNS,
        "ddl": None,
        "seed": None,
        "statement": None,
        "outcome": None,
        "error": None,
        "result": None,
        "persisted_before": None,
        "persisted_after": None,
        "snapshot_before": None,
        "snapshot_after": None,
        "files_before": None,
        "files_after": None,
        "facts": None,
        "prediction": None,
    }


def run_comparison_statement(
    ctx: dict[str, Any], door: dict[str, Any], spec: dict[str, Any], qualified: str
) -> tuple[str, dict[str, Any] | None]:
    if spec["route"] == "sql":
        statement = f"INSERT INTO {qualified} SELECT * FROM {qualified} WHERE {spec['predicate']}"
        _, error = base.attempt(ctx, lambda: base.collect(door["sql"](statement)), "sql")
        return statement, error
    statement = f"table({qualified}).filter({filter_text(spec)}).write.insertInto({qualified})"
    frame, error = base.attempt(
        ctx,
        lambda: door["session"].table(qualified).filter(route_argument(spec, door["functions"])),
        "filter",
    )
    if error is None:
        _, error = base.attempt(ctx, lambda: frame.write.insertInto(qualified), "write")
    return statement, error


def run_comparison(ctx: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    door = ctx["doors"]["sql"]
    qualified = f"{door['catalog']}.{base.NAMESPACE}.{spec['table']}"
    record = comparison_record(ctx, spec, qualified)
    ddl = base.create_sql(door, qualified, COMPARISON_COLUMNS)
    record["ddl"] = ddl
    _, error = base.attempt(ctx, lambda: base.collect(door["sql"](ddl)), "create")
    if error is None:
        seed = f"INSERT INTO {qualified} VALUES {SEED_ROWS}"
        record["seed"] = seed
        _, error = base.attempt(ctx, lambda: base.collect(door["sql"](seed)), "seed")
    if error is not None:
        return base.finish_setup(record, error)
    table_dir = door["root"] / base.NAMESPACE / spec["table"]
    select_sql = f"SELECT * FROM {qualified}"
    before = base.observe(ctx, door, table_dir, select_sql)
    statement, error = run_comparison_statement(ctx, door, spec, qualified)
    after = base.observe(ctx, door, table_dir, select_sql)
    record.update(
        statement=statement,
        outcome=base.outcome_of(error),
        error=error,
        persisted_before=before["persisted"],
        persisted_after=after["persisted"],
        snapshot_before={
            "count": before["snapshot_count"],
            "current": before["current_snapshot_id"],
        },
        snapshot_after={"count": after["snapshot_count"], "current": after["current_snapshot_id"]},
        files_before=before["files"],
        files_after=after["files"],
        facts=base.facts_of(before, after),
    )
    return record


def run_record(ctx: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    if spec["matrix"] == MATRIX_COMPARISON:
        return run_comparison(ctx, spec)
    record = base.run_spec(ctx, spec)
    record["matrix"] = spec["matrix"]
    return record


def twin_key(record: dict[str, Any] | None) -> str | None:
    if record is None:
        return None
    if record["outcome"] != base.ANSWERED:
        return record["outcome"]
    rows = base.canon(base.answered_rows(record))
    if record["form"] == "insert":
        return f"{rows} changed={record['facts']['snapshot_changed']}"
    return f"{rows} {record['result']['signature']}"


def same_text(first: dict[str, Any] | None, second: dict[str, Any] | None) -> str:
    if first is None or second is None:
        return "-"
    return "same" if twin_key(first) == twin_key(second) else "differs"


def is_non_equal(labels: list[str]) -> bool:
    return labels not in COMPARED_OK


def control_block(
    spark: dict[tuple[str, str], dict[str, Any]], main_cells: list[dict[str, Any]]
) -> tuple[list[list[str]], list[tuple[str, list[str]]], list[str]]:
    rows: list[list[str]] = []
    entries: list[tuple[str, list[str]]] = []
    non_equal: list[str] = []
    for cell in main_cells:
        if cell["matrix"] != MATRIX_CONTROL:
            continue
        counterpart = spark.get((cell["join"], cell["door"]))
        labels = base.cell_labels(counterpart, cell)
        entries.append((cell["door"], labels))
        rows.append(
            [
                cell["shape"],
                cell["operation"],
                cell["door"],
                cell["form"],
                cell["prediction"]["text"],
                base.measured_text(counterpart),
                base.measured_text(cell),
                base.pairing_of(counterpart),
                base.pairing_of(cell),
                ", ".join(labels),
            ]
        )
        if is_non_equal(labels):
            non_equal.append(
                f"control | {cell['shape']} | {cell['operation']} | {cell['door']} | "
                f"{cell['form']} | {', '.join(labels)}"
            )
    return rows, entries, non_equal


def distinct_block(
    spark: dict[tuple[str, str], dict[str, Any]],
    main_cells: list[dict[str, Any]],
    prior_spark: dict[tuple[str, str], dict[str, Any]],
    prior_main: dict[tuple[str, str], dict[str, Any]],
) -> tuple[list[list[str]], list[tuple[str, list[str]]], list[str]]:
    rows: list[list[str]] = []
    entries: list[tuple[str, list[str]]] = []
    non_equal: list[str] = []
    for cell in main_cells:
        if cell["matrix"] != MATRIX_DISTINCT:
            continue
        union_join = cell["join"].replace(f"|{UNION_DISTINCT}|", f"|{UNION}|")
        union_spark = prior_spark.get((union_join, cell["door"]))
        union_main = prior_main.get((union_join, cell["door"]))
        counterpart = spark.get((cell["join"], cell["door"]))
        labels = base.cell_labels(counterpart, cell)
        entries.append((cell["door"], labels))
        rows.append(
            [
                cell["shape"],
                cell["door"],
                cell["form"],
                cell["prediction"]["text"],
                base.measured_text(union_spark),
                base.measured_text(union_main),
                base.measured_text(counterpart),
                base.measured_text(cell),
                same_text(union_main, cell),
                ", ".join(labels),
            ]
        )
        if is_non_equal(labels):
            non_equal.append(
                f"union-distinct | {cell['shape']} | {cell['operation']} | {cell['door']} | "
                f"{cell['form']} | {', '.join(labels)}"
            )
    return rows, entries, non_equal


def comparison_block(
    spark: dict[tuple[str, str], dict[str, Any]], main_cells: list[dict[str, Any]]
) -> tuple[list[list[str]], list[tuple[str, list[str]]], list[str]]:
    rows: list[list[str]] = []
    entries: list[tuple[str, list[str]]] = []
    non_equal: list[str] = []
    for cell in main_cells:
        if cell["matrix"] != MATRIX_COMPARISON:
            continue
        counterpart = spark.get((cell["join"], cell["door"]))
        labels = base.cell_labels(counterpart, cell)
        entries.append((cell["door"], labels))
        rows.append(
            [
                cell["shape"],
                cell["predicate"],
                cell["door"],
                base.measured_text(counterpart),
                base.measured_text(cell),
                ", ".join(labels),
            ]
        )
        if is_non_equal(labels):
            non_equal.append(
                f"comparison | {cell['shape']} | {cell['predicate']} | {cell['door']} | "
                f"{', '.join(labels)}"
            )
    return rows, entries, non_equal


def plain_table(header: list[str], rows: list[list[str]]) -> list[str]:
    lines = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join("---" for _ in header) + " |",
    ]
    lines += ["| " + " | ".join(base.markdown_cell(cell) for cell in row) + " |" for row in rows]
    return lines


def count_table(entries: list[tuple[str, list[str]]], groups: tuple[str, ...]) -> list[str]:
    total: Counter[str] = Counter()
    per_group: dict[str, Counter[str]] = defaultdict(Counter)
    for group, labels in entries:
        total.update(labels)
        per_group[group].update(labels)
    lines = [
        "| label | all cells | " + " | ".join(groups) + " |",
        "| " + " | ".join("---" for _ in range(len(groups) + 2)) + " |",
    ]
    for label in sorted(total):
        counts = [str(total[label])] + [str(per_group[group][label]) for group in groups]
        lines.append(f"| {label} | " + " | ".join(counts) + " |")
    return lines


CONTROL_HEADER = [
    "shape",
    "operation",
    "door",
    "form",
    "predicted (left field order)",
    "Spark measured",
    "RePark measured",
    "pairing Spark",
    "pairing RePark",
    "labels for RePark",
]
DISTINCT_HEADER = [
    "shape",
    "door",
    "form",
    "predicted (left field order)",
    "Spark UNION (existing table)",
    "RePark UNION (existing table)",
    "Spark UNION DISTINCT",
    "RePark UNION DISTINCT",
    "RePark UNION DISTINCT same as RePark UNION",
    "labels for RePark",
]
COMPARISON_HEADER = [
    "comparison",
    "predicate",
    "route",
    "Spark measured",
    "RePark measured",
    "labels for RePark",
]


def render() -> int:
    spark = base.index_cells(base.load_cells(str(SPARK_OUT)))
    main_cells = base.load_cells(str(MAIN_OUT))
    prior_spark = base.index_cells(base.load_cells(str(PRIOR_SPARK)))
    prior_main = base.index_cells(base.load_cells(str(PRIOR_MAIN)))
    control_rows, control_entries, control_non_equal = control_block(spark, main_cells)
    distinct_rows, distinct_entries, distinct_non_equal = distinct_block(
        spark, main_cells, prior_spark, prior_main
    )
    comparison_rows, comparison_entries, comparison_non_equal = comparison_block(spark, main_cells)
    lines = [
        "# Identical-struct controls, UNION DISTINCT and the shared-helper comparison cells "
        "(measurement only)",
        "",
        "Generated by `probe_setop_controls.py --render` from `raw/controls-spark.jsonl` and "
        "`raw/controls-repark-main-fdc61f26.jsonl`; the existing UNION cells are read from "
        "`raw/spark.jsonl` and `raw/repark-main-fdc61f26.jsonl`. Not edited by hand. "
        "Measurement only.",
        "",
        "## 1. Identical-struct controls",
        "",
        "The right operand of each control has the left operand's field names, order and "
        "types, with different values (each integer plus two, each string `y`). Both readings "
        "predict the same rows, so the pairing columns read `n/a`. Native-door cells have no "
        "Spark reference and read ORACLE-MISSING; they are not compared.",
        "",
        *plain_table(CONTROL_HEADER, control_rows),
        "",
        "### Label counts (controls)",
        "",
        *count_table(control_entries, ("sql", "native", "df")),
        "",
        "### Non-EQUAL control cells (compared with Spark)",
        "",
        *(control_non_equal or ["None."]),
        "",
        "## 2. Explicit UNION DISTINCT beside UNION",
        "",
        "Measured on the ten existing shapes with their existing right operands, on the sql "
        "and native doors, read and insert. The UNION columns are the existing table's cells. "
        "The last measured column compares the RePark UNION DISTINCT cell with the RePark UNION "
        "cell on outcome, rows and schema (read) or snapshot change (insert).",
        "",
        *plain_table(DISTINCT_HEADER, distinct_rows),
        "",
        "### Label counts (UNION DISTINCT)",
        "",
        *count_table(distinct_entries, ("sql", "native")),
        "",
        "### Non-EQUAL UNION DISTINCT cells",
        "",
        *(distinct_non_equal or ["None."]),
        "",
        "## 3. Shared-helper comparison cells",
        "",
        "Each cell runs `INSERT INTO t SELECT * FROM t WHERE <predicate>` on a fresh v2 "
        "copy-on-write table seeded with `(1, 'a'), (2, 'b')`, through four routes: `sql` (the "
        'SQL statement), `filter-str` (`filter("<predicate>")`), `f-expr` '
        '(`filter(F.expr("<predicate>"))`) and `typed` (`F.struct` with aliased fields, the '
        "typed spelling of `named_struct`). The release wheel is measured; a debug assertion "
        "would be unmeasured.",
        "",
        *plain_table(COMPARISON_HEADER, comparison_rows),
        "",
        "### Label counts (comparison cells)",
        "",
        *count_table(comparison_entries, ROUTES),
        "",
        "### Non-EQUAL comparison cells",
        "",
        *(comparison_non_equal or ["None."]),
        "",
    ]
    TABLE_OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"controls: {len(control_rows)}")
    print(f"union-distinct: {len(distinct_rows)}")
    print(f"comparison: {len(comparison_rows)}")
    print("control labels: " + label_summary(control_entries))
    print("union-distinct labels: " + label_summary(distinct_entries))
    print("comparison labels: " + label_summary(comparison_entries))
    for line in control_non_equal + distinct_non_equal + comparison_non_equal:
        print(line)
    return 0


def label_summary(entries: list[tuple[str, list[str]]]) -> str:
    total: Counter[str] = Counter()
    for _, labels in entries:
        total.update(labels)
    return ", ".join(f"{label}={count}" for label, count in sorted(total.items()))


def parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Struct controls, UNION DISTINCT and comparison cells (measurement only)."
    )
    parser.add_argument("--render", action="store_true")
    parser.add_argument("--engine", choices=("spark", "repark"))
    parser.add_argument("--label")
    parser.add_argument("--warehouse")
    parser.add_argument("--out")
    parser.add_argument("--ivy-jar")
    parser.add_argument("--wheel")
    parser.add_argument("--plan-only", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    arguments = parse_arguments(argv)
    if arguments.render:
        return render()
    specs = plan()
    if arguments.plan_only:
        print(f"cells planned: {len(specs)}")
        return 0
    missing = [
        name for name in ("engine", "label", "warehouse", "out") if getattr(arguments, name) is None
    ]
    if arguments.engine == "spark" and arguments.ivy_jar is None:
        missing.append("ivy-jar")
    if arguments.engine == "repark" and arguments.wheel is None:
        missing.append("wheel")
    if missing:
        print(f"missing arguments: {', '.join(missing)}", file=sys.stderr)
        return 2
    warehouse = Path(arguments.warehouse)
    if warehouse.exists() and any(warehouse.iterdir()):
        print("warehouse must be empty or absent", file=sys.stderr)
        return 2
    warehouse.mkdir(parents=True)
    context = base.build_context(arguments, warehouse)
    out = Path(arguments.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    outcomes: Counter[str] = Counter()
    with out.open("w", encoding="utf-8") as handle:
        base.write_line(handle, context["header"], context["scrub"])
        for spec in specs:
            record = run_record(context, spec)
            outcomes[record["outcome"]] += 1
            base.write_line(handle, record, context["scrub"])
            handle.flush()
    context["close"]()
    print(f"cells: {len(specs)} outcomes: {dict(sorted(outcomes.items()))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
