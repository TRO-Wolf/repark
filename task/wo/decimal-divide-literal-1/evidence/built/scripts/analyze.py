import collections
import json
import statistics
import sys
from pathlib import Path

BAR = 1.05
NAMES = {
    "s1_sql_d10_div3": "1 SQL `d10 / 3` (`DECIMAL(10,2)`)",
    "s1_sql_d38_div3": "1 SQL `d38 / 3` (`DECIMAL(38,2)`)",
    "s1_sql_d10_mod3": "1 SQL `mod(d10, 3)`",
    "s1_sql_d10_pmod3": "1 SQL `pmod(d10, 3)`",
    "s1_sql_d10_pct3": "1 SQL `d10 % 3` (a base for the SQL `pmod()` row)",
    "s2_df_d10_div3": "2 DataFrame `col(d10) / 3`",
    "s2_df_d10_mod3": "2 DataFrame `col(d10) % 3`",
    "s2_df_d10_pmod": "2 DataFrame `F.pmod(col(d10), F.lit(3))`",
    "s2_df_d10_expr_mod": '2 DataFrame `F.expr("mod(d10, 3)")`',
    "s2_df_d10_expr_div3": '2 DataFrame `F.expr("d10 / 3")` (a base for row 2 `/`)',
    "s3_sql_int_div_int": "3 SQL `i1 / i2` (`INT`)",
    "s3_sql_double_div_double": "3 SQL `x1 / x2` (`DOUBLE`)",
    "s3_df_int_div_int": "3 DataFrame `col(i1) / col(i2)`",
    "s3_df_double_div_double": "3 DataFrame `col(x1) / col(x2)`",
    "C-012 c012p6": "4 C-012 `c012p6`",
    "C-012 c012rest": "4 C-012 `c012rest`",
}
OWNER_ROW = "s2_df_d10_div3"
RATIO_BASES = {
    "s2_df_d10_div3": "main's DataFrame `col(d10) / 3` (a `double` division)",
    "s1_sql_d10_div3": "main's SQL `d10 / 3`",
    "s2_df_d10_expr_div3": 'main\'s `F.expr("d10 / 3")` on the DataFrame door',
}

PMOD_ROW = "s1_sql_d10_pmod3"
PMOD_BASES = {
    "s1_sql_d10_pmod3": "main's SQL `pmod(d10, 3)` (`decimal(12,2)`, not Spark's type)",
    "s1_sql_d10_pct3": "main's SQL `d10 % 3`",
}


def load(cost_dir: Path) -> tuple[dict, dict]:
    per: dict = collections.defaultdict(lambda: collections.defaultdict(list))
    answers: dict = collections.defaultdict(dict)
    raw = cost_dir / "raw-ddl.jsonl"
    if raw.exists():
        for line in raw.open():
            r = json.loads(line)
            per[r["case"]][r["label"]].append(r["seconds"] / r.get("reps", 1))
            answers[r["case"]][r["label"]] = r.get("answer")
    for line in (cost_dir / "samples.jsonl").open():
        r = json.loads(line)
        if r["group"] == "c012" and r["rc"] == 0:
            per["C-012 " + r["case"]][r["build"]].append(r["wall"])
    return per, answers


def med(xs: list) -> float:
    return statistics.median(xs) if xs else float("nan")


def band(xs: list, ys: list) -> tuple[float, float] | None:
    n = min(len(xs), len(ys))
    rs = [x / y for x, y in zip(xs[:n], ys[:n], strict=True) if y]
    return (min(rs), max(rs)) if rs else None


def verdict(ratio: float, control: tuple[float, float] | None, case: str) -> str:
    if ratio <= BAR:
        return "under"
    if case == OWNER_ROW:
        return "**over: the owner's row**"
    if control and control[1] >= ratio:
        return "over, inside its band"
    return "**over, outside its band**"


def table(cost_dir: Path) -> list[str]:
    per, answers = load(cost_dir)
    out = [
        "| row | main (s) | head (s) | head/main | control band (main-aa/main) | bar | runs"
        " | answers equal | at the bar |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for case in sorted(per, key=lambda c: NAMES.get(c, c)):
        d = per[case]
        m, h, a = d.get("main", []), d.get("head", []), d.get("main-aa", [])
        ratio = med(h) / med(m) if m and h else float("nan")
        control = band(a, m)
        shown = f"{control[0]:.3f} to {control[1]:.3f}" if control else ""
        same = len({str(v) for v in answers[case].values()}) <= 1
        name, runs = NAMES.get(case, case), min(len(m), len(h))
        out.append(
            f"| {name} | {med(m):.4f} | {med(h):.4f} | {ratio:.3f} | {shown} | {BAR}"
            f" | {runs} | {same} | {verdict(ratio, control, case)} |"
        )
    head = med(per[OWNER_ROW].get("head", []))
    out += ["", "The owner's row, `col(d10) / 3` on the head, against three bases on main:", ""]
    out += ["| base on main | main (s) | head `col(d10) / 3` (s) | ratio |", "|---|---|---|---|"]
    for case, text in RATIO_BASES.items():
        base = med(per[case].get("main", []))
        ratio = head / base
        out.append(f"| {text} | {base:.4f} | {head:.4f} | {ratio:.3f} |")
    head = med(per[PMOD_ROW].get("head", []))
    out += ["", "The SQL `pmod(d10, 3)` row on the head, against two bases on main:", ""]
    out += ["| base on main | main (s) | head `pmod(d10, 3)` (s) | ratio |", "|---|---|---|---|"]
    for case, text in PMOD_BASES.items():
        base = med(per[case].get("main", []))
        out.append(f"| {text} | {base:.4f} | {head:.4f} | {head / base:.3f} |")
    return out


print("\n".join(table(Path(sys.argv[1]))))
