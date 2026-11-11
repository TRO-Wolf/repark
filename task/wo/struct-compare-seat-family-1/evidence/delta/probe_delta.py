import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "step0"))
import probe_family as pf

HERE = Path(__file__).resolve().parent
RAW = HERE / "raw"
WORK = HERE / "work"
LEGS = ["spark", "pr", "head"]
FAMILY_VIEWS = ("lk", "rrev", "ldup")
DUP_TEXT = "nullif(l.s, r.s) IS NULL"
FIXTURES = [
    (
        "dk_lms",
        ["lid", "s"],
        [
            ["1", "named_struct('a', 1, 'm', map('x', 1))"],
            ["2", "named_struct('a', 2, 'm', map('y', 2))"],
        ],
    ),
    (
        "dk_rms",
        ["rid", "s"],
        [
            ["10", "named_struct('a', 1, 'm', map('x', 1))"],
            ["20", "named_struct('a', 3, 'm', map('z', 3))"],
        ],
    ),
    (
        "dk_rms2",
        ["rid", "s"],
        [
            ["10", "named_struct('b', 1, 'n', map('x', 1))"],
            ["20", "named_struct('b', 3, 'n', map('z', 3))"],
        ],
    ),
    (
        "dk_lam",
        ["lid", "a"],
        [["1", "array(map('x', 1))"], ["2", "array(map('y', 2))"]],
    ),
    (
        "dk_ram",
        ["rid", "a"],
        [["10", "array(map('x', 1))"], ["20", "array(map('z', 3))"]],
    ),
    (
        "dk_lnm",
        ["lid", "s"],
        [["1", "named_struct('p', named_struct('m', map('x', 1)))"]],
    ),
    (
        "dk_rnm",
        ["rid", "s"],
        [["10", "named_struct('q', named_struct('n', map('x', 1)))"]],
    ),
    (
        "dk_lmx",
        ["lid", "s"],
        [
            ["1", "named_struct('m', map('a', 1), 'b', 2)"],
            ["2", "named_struct('m', map('b', 2), 'b', 1)"],
        ],
    ),
    (
        "dk_rmx",
        ["rid", "s"],
        [
            ["10", "named_struct('x', map('a', 1), 'y', 2)"],
            ["30", "named_struct('x', map('c', 3), 'y', 9)"],
        ],
    ),
    (
        "dk_rbm",
        ["rid", "s"],
        [
            ["10", "named_struct('b', 2, 'm', map('a', 1))"],
            ["30", "named_struct('b', 9, 'm', map('c', 3))"],
        ],
    ),
    (
        "dk_lda",
        ["lid", "s"],
        [
            ["1", "array(named_struct('a', 1, 'm', map('x', 1)))"],
            ["2", "array(named_struct('a', 2, 'm', map('y', 2)))"],
        ],
    ),
    (
        "dk_rda",
        ["rid", "s"],
        [
            ["10", "array(named_struct('b', 1, 'n', map('x', 1)))"],
            ["20", "array(named_struct('b', 3, 'n', map('z', 3)))"],
        ],
    ),
    (
        "dk_lpa",
        ["lid", "s"],
        [["1", "named_struct('p', array(map('x', 1)))"]],
    ),
    (
        "dk_rpa",
        ["rid", "s"],
        [["10", "named_struct('q', array(map('x', 1)))"]],
    ),
    (
        "dk_lmk",
        ["lid", "s"],
        [["1", "map('a', 1)"], ["2", "map('b', 2)"]],
    ),
    (
        "dk_rmk",
        ["rid", "s"],
        [["10", "map('a', 1)"], ["30", "map('c', 3)"]],
    ),
    (
        "dk_lint",
        ["lid", "s"],
        [["1", "1"], ["2", "2"]],
    ),
    (
        "dk_rint",
        ["rid", "s"],
        [["10", "1"], ["30", "3"]],
    ),
]


def sql_cell(cell_id: str, finding: str, item: str, text: str) -> dict:
    return pf.make(
        id=cell_id,
        finding=finding,
        item=item,
        group="DELTA",
        seat="key-list join",
        door="sql",
        detail="using",
        shape="delta",
        jt="inner",
        proj="pair",
        op={"kind": "sql", "text": text},
    )


def join_cell(finding: str, item: str, cell_id: str, sides: tuple[str, str, str]) -> dict:
    left, right, key = sides
    return pf.make(
        id=cell_id,
        finding=finding,
        item=item,
        group="DELTA",
        seat="key-list join",
        door="df_typed",
        detail="join_str",
        shape="delta",
        jt="inner",
        proj="pair",
        op={
            "kind": "df_join",
            "L": left,
            "R": right,
            "key": key,
            "how": "inner",
            "form": "str",
            "proj": "pair",
            "pairs": ["lid", "rid"],
        },
    )


CELLS = [
    sql_cell("DK01", "J21", "1", "SELECT lid, rid FROM dk_lms JOIN dk_rms USING (s)"),
    sql_cell("DK02", "J22", "1", "SELECT lid, rid FROM dk_lms JOIN dk_rms2 USING (s)"),
    sql_cell("DK03", "J22 full", "1", "SELECT lid, rid FROM dk_lms FULL JOIN dk_rms2 USING (s)"),
    sql_cell("DK04", "M04", "1", "SELECT lid, rid FROM dk_lam JOIN dk_ram USING (a)"),
    sql_cell("DK05", "M05", "1", "SELECT lid, rid FROM dk_lnm JOIN dk_rnm USING (s)"),
    sql_cell("DK06", "Muse J33d", "1", "SELECT lid, rid FROM dk_lmx JOIN dk_rmx USING (s)"),
    sql_cell("DK07", "deeper", "1", "SELECT lid, rid FROM dk_lda JOIN dk_rda USING (s)"),
    sql_cell("DK08", "deeper", "1", "SELECT lid, rid FROM dk_lpa JOIN dk_rpa USING (s)"),
    sql_cell("DK09", "J22 natural", "1", "SELECT lid, rid FROM dk_lms NATURAL JOIN dk_rms2"),
    join_cell("J22 DataFrame", "1", "DK10", ("dk_lms", "dk_rms2", "s")),
    join_cell("M04 DataFrame", "1", "DK11", ("dk_lam", "dk_ram", "a")),
    sql_cell(
        "DK12",
        "Muse K05",
        "6",
        "SELECT l.lid, r.lid AS rlid FROM dk_lmk l JOIN dk_lint r USING (s)",
    ),
    sql_cell("DK13", "Muse K09", "6", "SELECT l.lid, r.rid FROM dk_lmk l JOIN rrev r USING (s)"),
    sql_cell("DK14", "Muse K10", "6", "SELECT l.lid, r.rid FROM lk l JOIN dk_rmk r USING (s)"),
    sql_cell("DK15", "Muse K06", "6", "SELECT l.lid, r.rid FROM dk_lint l JOIN dk_rmk r USING (s)"),
    sql_cell("DK16", "Muse J36", "6", "SELECT lid, rid FROM dk_lint JOIN dk_rmk USING (s)"),
    join_cell("Muse K06 DataFrame", "6", "DK17", ("dk_lint", "dk_rmk", "s")),
    join_cell("Muse K05 DataFrame", "6", "DK18", ("dk_lmk", "dk_rint", "s")),
    sql_cell("DK19", "control J33", "control", "SELECT lid, rid FROM dk_lmx JOIN dk_rbm USING (s)"),
    sql_cell(
        "DK20", "control J37", "control", "SELECT l.lid, r.rid FROM dk_lint l JOIN rrev r USING (s)"
    ),
    sql_cell("DK21", "control J20", "control", "SELECT lid, rid FROM dk_lmk JOIN dk_rmk USING (s)"),
    sql_cell(
        "DK23",
        "F6 SQL twin",
        "4",
        f"SELECT l.lid, r.rid, {DUP_TEXT} AS x FROM ldup l CROSS JOIN rrev r",
    ),
]
DUP_ID = "DK22"


def view_statements(leg: str) -> list[tuple[str, str]]:
    family = [(name, sql) for name, sql in pf.view_statements(leg) if name in FAMILY_VIEWS]
    return family + own_views()


def own_views() -> list[tuple[str, str]]:
    return [(name, pf.view_sql(name, columns, rows)) for name, columns, rows in FIXTURES]


def dup_frame(spark: Any, sf: Any) -> Any:
    frame = spark.table("ldup").alias("l").crossJoin(spark.table("rrev").alias("r"))
    return frame.select("l.lid", "r.rid", sf.expr(DUP_TEXT).alias("x"))


def run_leg(leg: str) -> None:
    wh = WORK / leg / "wh"
    shutil.rmtree(wh, ignore_errors=True)
    wh.mkdir(parents=True, exist_ok=True)
    RAW.mkdir(parents=True, exist_ok=True)
    spark = pf.session(leg, str(wh))
    (RAW / f"{leg}.identity.json").write_text(json.dumps(pf.identity(leg), indent=1))
    sf = pf.frame_functions(leg)
    for _name, statement in view_statements(leg):
        spark.sql(statement).collect()
    with (RAW / f"{leg}.jsonl").open("w") as handle:
        for cell in CELLS:
            result = pf.run_query(spark, sf, cell, leg)
            result["id"] = cell["id"]
            handle.write(json.dumps(result, default=str) + "\n")
            handle.flush()
        result = pf.attempt(lambda: dup_frame(spark, sf))
        result["id"] = DUP_ID
        handle.write(json.dumps(result, default=str) + "\n")
    spark.stop()


def load(leg: str) -> dict:
    path = RAW / f"{leg}.jsonl"
    return {json.loads(line)["id"]: json.loads(line) for line in path.read_text().splitlines()}


def outcome(record: dict) -> str:
    if record["status"] == "ok":
        return json.dumps(record["rows"])
    return "ERR " + record["class"]


def render() -> None:
    legs = {leg: load(leg) for leg in LEGS}
    print("| id | finding | item | spark | pr | head | head = spark |")
    print("|---|---|---|---|---|---|---|")
    rows = [(cell["id"], cell["finding"], cell["item"]) for cell in CELLS]
    for cell_id, finding, item in [*rows, (DUP_ID, "F6", "4")]:
        shown = [outcome(legs[leg][cell_id]) for leg in LEGS]
        same = "yes" if shown[0] == shown[2] else "no"
        print(f"| {cell_id} | {finding} | {item} | " + " | ".join(shown) + f" | {same} |")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--leg", choices=LEGS)
    parser.add_argument("--render", action="store_true")
    args = parser.parse_args()
    if args.render:
        render()
        return
    run_leg(args.leg)


if __name__ == "__main__":
    main()
