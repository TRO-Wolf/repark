import argparse
import json
import shutil
import sys
from collections import Counter
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import probe_family as pf

ADD_LEGS = ["spark", "head"]
ADD_FIXTURES = [
    (
        "lmk",
        ["lid", "s"],
        [["1", "map('k', 1)"], ["2", "map('k', 2)"], ["3", pf.null_of("MAP<STRING, INT>")]],
    ),
    (
        "rmk",
        ["rid", "s"],
        [["10", "map('k', 1)"], ["30", "map('k', 7)"], ["40", pf.null_of("MAP<STRING, INT>")]],
    ),
    (
        "lms",
        ["lid", "s"],
        [
            ["1", f"map({pf.struct_of('ab', 1, 2)}, 1)"],
            ["2", f"map({pf.struct_of('ab', 2, 1)}, 1)"],
            ["3", pf.null_of("MAP<STRUCT<a: INT, b: INT>, INT>")],
        ],
    ),
    (
        "rms",
        ["rid", "s"],
        [
            ["10", f"map({pf.struct_of('ba', 2, 1)}, 1)"],
            ["30", f"map({pf.struct_of('ba', 7, 7)}, 1)"],
            ["40", pf.null_of("MAP<STRUCT<b: INT, a: INT>, INT>")],
        ],
    ),
]
MAP_KEY_SHAPES = [("mapkey_plain", "lmk", "rmk"), ("mapkey_struct", "lms", "rms")]
DML_PICKS = [
    ("D-PRED", "delete", "between", "r"),
    ("D-PRED", "delete", "nullif", "r"),
    ("D-PRED", "delete", "case", "r"),
    ("D-PRED", "delete", "eq", "r"),
    ("D-INSERT", "insert-using-join", None, None),
    ("D-MERGE", "merge-using-join", None, None),
]


def natural_exist_cells(shape: str, lview: str, rview: str, case: str) -> list[dict]:
    out = []
    for jt, word in pf.JOIN_WORDS[4:]:
        for proj, cols in [("pair", "lid"), ("star", "*")]:
            out.append(
                pf.make(
                    group="J-NATURAL",
                    seat="key-list join",
                    door="sql",
                    detail="natural",
                    shape=shape,
                    case=case,
                    jt=jt,
                    proj=proj,
                    twin=f"natural|{jt}|{proj}" if proj == "star" else None,
                    op={
                        "kind": "sql",
                        "text": f"SELECT {cols} FROM {lview} NATURAL {word} {rview}",
                    },
                )
            )
    return out


def not_between_str_cells(
    shape: str, lview: str, rview: str | None, case: str, rhs: str
) -> list[dict]:
    out = []
    for door, detail, wrap in pf.BOOL_DF:
        out.append(
            pf.make(
                group="F-NOT_BETWEEN",
                seat="NOT BETWEEN",
                door=door,
                detail=detail,
                shape=shape,
                case=case,
                op={
                    "kind": "cmp",
                    "form": "not_between",
                    "door": door,
                    "detail": detail,
                    "wrap": wrap,
                    "L": lview,
                    "R": rview,
                    "rhs": rhs,
                },
            )
        )
    return out


def map_key_cells(shape: str, lview: str, rview: str) -> list[dict]:
    out = []
    for jt, word in pf.JOIN_WORDS:
        pairs = ["lid"] if jt in pf.SEMI else ["lid", "rid"]
        text = f"SELECT {', '.join(pairs)} FROM {lview} {word} {rview} USING (s)"
        out.append(
            pf.make(
                group="J-USING",
                seat="key-list join",
                door="sql",
                detail="using",
                shape=shape,
                jt=jt,
                proj="pair",
                op={"kind": "sql", "text": text},
            )
        )
        out.append(
            pf.make(
                group="J-DF",
                seat="key-list join",
                door="df_typed",
                detail="join_str",
                shape=shape,
                jt=jt,
                proj="pair",
                op={
                    "kind": "df_join",
                    "L": lview,
                    "R": rview,
                    "key": "s",
                    "how": pf.DF_HOW[jt],
                    "form": "str",
                    "proj": "pair",
                    "pairs": pairs,
                },
            )
        )
    out.append(
        pf.make(
            group="J-NATURAL",
            seat="key-list join",
            door="sql",
            detail="natural",
            shape=shape,
            jt="inner",
            proj="pair",
            op={"kind": "sql", "text": f"SELECT lid, rid FROM {lview} NATURAL JOIN {rview}"},
        )
    )
    out.append(
        pf.make(
            group="C-ON",
            seat="ON control",
            door="sql",
            detail="on",
            shape=shape,
            jt="inner",
            proj="pair",
            op={
                "kind": "sql",
                "text": f"SELECT l.lid, r.rid FROM {lview} l JOIN {rview} r ON l.s = r.s",
            },
        )
    )
    return out


def prepared_cell(group: str, seat: str, shape: str, detail: str, text: str) -> dict:
    return pf.make(
        group="P-PREPARE",
        seat="PREPARE/EXECUTE",
        door="sql",
        detail=f"{group}:{detail}",
        shape=shape,
        jt="inner" if "JOIN" in text else None,
        proj="pair",
        prepare=True,
        op={"kind": "sql", "text": text, "of_seat": seat},
    )


def prepared_cells() -> list[dict]:
    return [
        prepared_cell(
            "J-NATURAL",
            "key-list join",
            "rev",
            "semi",
            "SELECT lid FROM lk NATURAL LEFT SEMI JOIN rrev",
        ),
        prepared_cell(
            "J-NATURAL",
            "key-list join",
            "rev",
            "anti",
            "SELECT lid FROM lk NATURAL LEFT ANTI JOIN rrev",
        ),
        prepared_cell(
            "J-USING",
            "key-list join",
            "map",
            "map",
            "SELECT lid, rid FROM lmap JOIN rmap USING (s)",
        ),
        prepared_cell(
            "J-USING",
            "key-list join",
            "mapkey_struct",
            "mapkey",
            "SELECT lid, rid FROM lms JOIN rms USING (s)",
        ),
        prepared_cell(
            "K-KIND",
            "list kinds",
            "kind",
            "plain-plain",
            "SELECT lid, rid FROM kL_plain JOIN kR_plain_rev USING (s)",
        ),
        prepared_cell(
            "K-KIND",
            "list kinds",
            "kind",
            "large-plain",
            "SELECT lid, rid FROM kL_large JOIN kR_plain_rev USING (s)",
        ),
        prepared_cell(
            "S-SETOP",
            "set-op control",
            "rev",
            "intersect",
            "SELECT s FROM lk INTERSECT SELECT s FROM rrev",
        ),
        prepared_cell(
            "S-SETOP",
            "set-op control",
            "rev",
            "except",
            "SELECT s FROM lk EXCEPT SELECT s FROM rrev",
        ),
        prepared_cell(
            "S-SETOP",
            "set-op control",
            "rev",
            "union_all",
            "SELECT s FROM lk UNION ALL SELECT s FROM rrev",
        ),
    ]


def gen_add_cells() -> list[dict]:
    cells = []
    for shape, lview, rview in pf.SHAPES:
        for case in pf.cases_for(shape):
            cells += natural_exist_cells(shape, lview, rview, case)
    cells += natural_exist_cells("INT", "ik", "irk", "insensitive")
    for shape, lview, rview in pf.SHAPES:
        for case in pf.cases_for(shape):
            cells += not_between_str_cells(shape, lview, rview, case, "r.s")
    for name, rhs in [("B", pf.B), ("A", pf.A), ("C", pf.C)]:
        cells += not_between_str_cells(f"lit-{name}", "lk", None, "insensitive", rhs)
    for shape, lview, rview in MAP_KEY_SHAPES:
        cells += map_key_cells(shape, lview, rview)
    cells += prepared_cells()
    for index, cell in enumerate(cells, 1):
        cell["id"] = f"X{index:04d}"
    return cells


def picked(cell: dict, group: str, detail: str, form: str | None, bound: str | None) -> bool:
    if cell["group"] != group or cell["fmt"] != "v2cow" or cell["shape"] != "rev":
        return False
    if group == "D-PRED":
        return cell["detail"] == detail and cell["pred_form"] == form and cell["bound"] == bound
    return cell["detail"] == detail


def gen_add_dml_cells() -> list[dict]:
    source = pf.gen_dml_cells()
    cells = []
    for group, detail, form, bound in DML_PICKS:
        match = [cell for cell in source if picked(cell, group, detail, form, bound)]
        cell = dict(match[0])
        index = len(cells) + 1
        table = f"hd.vp.x{index}"
        old = cell["table"]
        cell["setup"] = [text.replace(old, table) for text in cell["setup"]]
        cell["stmt"] = cell["stmt"].replace(old, table)
        if cell.get("select"):
            cell["select"] = cell["select"].replace(old, table)
        cell.update(table=table, of=cell["id"], id=f"Y{index:04d}", group="P-DML", prepare=True)
        cells.append(cell)
    return cells


def run_dml_prepared(spark: Any, cell: dict, leg: str) -> dict:
    table = cell["table"]
    out = {"id": cell["id"]}
    for statement in cell["setup"]:
        result = pf.try_call(lambda text=statement: spark.sql(text).collect())
        if result["status"] != "ok":
            out["setup"] = result
            return out
    out["pre"] = pf.attempt(lambda: spark.table(table))
    out["snap_before"] = pf.attempt(
        lambda: spark.sql(f"SELECT count(*) AS n FROM {table}.snapshots")
    )
    if cell.get("select"):
        out["selected"] = pf.attempt(lambda: spark.sql(cell["select"]))
    name = "p_" + cell["id"].lower()
    if leg == "spark":
        out["dml"] = pf.try_call(lambda: spark.sql(cell["stmt"]).collect())
    else:
        out["prepare"] = pf.try_call(lambda: spark.sql(f"PREPARE {name} AS {cell['stmt']}"))
        if out["prepare"]["status"] == "ok":
            out["dml"] = pf.try_call(lambda: spark.sql(f"EXECUTE {name}").collect())
        else:
            out["dml"] = dict(out["prepare"])
    if out["dml"]["status"] == "ok":
        out["post"] = pf.attempt(lambda: spark.table(table))
        out["snap_after"] = pf.attempt(
            lambda: spark.sql(f"SELECT count(*) AS n FROM {table}.snapshots")
        )
    return out


def setup_views(spark: Any, leg: str) -> list[dict]:
    errors = []
    statements = pf.view_statements(leg) + [
        (name, pf.view_sql(name, columns, rows)) for name, columns, rows in ADD_FIXTURES
    ]
    for name, statement in statements:
        try:
            spark.sql(statement).collect()
        except BaseException as error:
            errors.append({"view": name, "error": " ".join(str(error).split())[:300]})
    return errors


def run_add(leg: str, tag: str) -> None:
    wh = pf.WORK / f"{leg}-{tag}" / "wh"
    shutil.rmtree(wh, ignore_errors=True)
    wh.mkdir(parents=True, exist_ok=True)
    pf.RAW.mkdir(parents=True, exist_ok=True)
    spark = pf.session(leg, str(wh))
    (pf.RAW / f"{leg}-{tag}.identity.json").write_text(json.dumps(pf.identity(leg), indent=1))
    functions = pf.frame_functions(leg)
    errors = setup_views(spark, leg)
    (pf.RAW / f"{leg}-{tag}.setup.json").write_text(json.dumps(errors, indent=1))
    with (pf.RAW / f"{leg}-{tag}.jsonl").open("w") as handle:
        for cell in gen_add_cells():
            case = "true" if cell["case"] == "sensitive" else "false"
            spark.conf.set("spark.sql.caseSensitive", case)
            result = pf.run_query(spark, functions, cell, leg)
            result["id"] = cell["id"]
            handle.write(json.dumps(result, default=str) + "\n")
            handle.flush()
        spark.conf.set("spark.sql.caseSensitive", "false")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS hd.vp").collect()
        for cell in gen_add_dml_cells():
            result = run_dml_prepared(spark, cell, leg)
            handle.write(json.dumps(result, default=str) + "\n")
            handle.flush()
    spark.stop()


def load_add(leg: str, tag: str) -> dict:
    path = pf.RAW / f"{leg}-{tag}.jsonl"
    if not path.exists():
        return {}
    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    return {rec["id"]: rec for rec in records}


def prep_status(rec: dict | None) -> str:
    if not rec or "prepare" not in rec:
        return "-"
    prep = rec["prepare"]
    return prep["status"] if prep["status"] == "ok" else f"err {prep.get('class')}"


def add_section(tag: str, title: str) -> list[str]:
    spark, head = load_add("spark", tag), load_add("head", "add")
    if not spark or not head:
        return []
    legs = {"spark": spark, "head": head}
    cells = gen_add_cells()
    dml_cells = gen_add_dml_cells()
    int_map = {c["twin"]: c for c in cells if c["shape"] == "INT" and c["twin"]}
    rows = []
    for c in cells:
        s, h = legs["spark"].get(c["id"]), legs["head"].get(c["id"])
        lab = pf.qlabel(s, h)
        extra = "-"
        if c["twin"] and c["shape"] != "INT" and c["twin"] in int_map:
            twin = int_map[c["twin"]]
            twin_lab = pf.qlabel(spark.get(twin["id"]), head.get(twin["id"]))
            extra = "INT-TWIN-SAME" if twin_lab == lab else "INT-TWIN-DIFFERS"
        schema = "-"
        if pf.ok(s) and pf.ok(h):
            schema = "same" if s["schema"] == h["schema"] else "differs"
        rows.append({"cell": c, "lab": lab, "extra": extra, "schema": schema, "s": s, "h": h})
    dml_rows = []
    for c in dml_cells:
        s, h = legs["spark"].get(c["id"]), legs["head"].get(c["id"])
        dml_rows.append({"cell": c, "lab": pf.dml_label(s, h, c), "s": s, "h": h})
    lines = ["", f"## {title}", ""]
    lines.append(f"Query cells: {len(rows)}. DML cells: {len(dml_rows)} (per leg).")
    lines.append("Legs: `spark` and `head` (this unit's base, the clone's release build).")
    lines.append("")
    lines.append("| seat | group | head label vs Spark | count |")
    lines.append("|---|---|---|---|")
    counts = Counter((r["cell"]["seat"], r["cell"]["group"], r["lab"]) for r in rows)
    counts.update(("DML EXECUTE", r["cell"]["group"], r["lab"]) for r in dml_rows)
    for (seat, group, lab), count in sorted(counts.items()):
        lines.append(f"| {seat} | {group} | {lab} | {count} |")
    lines.append("")
    lines.append(
        "| id | group | seat | door | detail | join | shape | proj | case | statement "
        "| label vs Spark | int twin | schema | spark | head | head PREPARE |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        c = r["cell"]
        statement = pf.describe(c).replace("|", "/")[:220]
        lines.append(
            f"| {c['id']} | {c['group']} | {c['seat']} | {c['door']} | {c['detail']} "
            f"| {c['jt'] or '-'} | {c['shape']} | {c['proj'] or '-'} | {c['case']} "
            f"| {statement} | {r['lab']} | {r['extra']} | {r['schema']} "
            f"| {pf.short(r['s']).replace('|', '/')} | {pf.short(r['h']).replace('|', '/')} "
            f"| {prep_status(r['h'])} |"
        )
    lines.append("")
    lines.append("| id | of | fmt | group | detail | predicate or source | label | head PREPARE |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for r in dml_rows:
        c = r["cell"]
        source = (c.get("pred") or c["stmt"])[:120].replace("|", "/")
        lines.append(
            f"| {c['id']} | {c['of']} | {c['fmt']} | {c['group']} | {c['detail']} "
            f"| {source} | {r['lab']} | {prep_status(r['h'])} |"
        )
    return lines


def render_add() -> None:
    pf.render()
    path = pf.OUT / "table.md"
    lines = add_section("add", "Completeness additions (build item 1)")
    if lines:
        path.write_text(path.read_text() + "\n".join(lines[1:]) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leg", choices=ADD_LEGS)
    parser.add_argument("--tag", default="add")
    parser.add_argument("--render", action="store_true")
    parser.add_argument("--plan", action="store_true")
    args = parser.parse_args()
    if args.plan:
        query, dml = gen_add_cells(), gen_add_dml_cells()
        for key, count in sorted(Counter((c["group"], c["seat"]) for c in query).items()):
            print(key, count)
        print("query", len(query), "dml", len(dml))
        return
    if args.render:
        render_add()
        return
    run_add(args.leg, args.tag)


if __name__ == "__main__":
    main()
