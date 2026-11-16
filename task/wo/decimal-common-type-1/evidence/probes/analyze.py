import json
import re
import sys
from collections import Counter, defaultdict
from collections.abc import Callable
from pathlib import Path

raw, out = Path(sys.argv[1]), Path(sys.argv[2])
NUM = re.compile(r"-?\d+\.\d+")
WRITE_GROUPS = {"iceberg", "icedml"}
CONFOUNDED = {
    "nvl",
    "nullif",
    "array_union",
    "lag",
    "lead",
    "join_full_using",
}
CONTROL_KEYS = ("_control", "_same_type")
SINGLE = {
    "agg/sum_type": "avg-round",
    "values/v_int_c": "int-literal-type",
    "literal/u_arith": "arithmetic-control",
}


def cause(key: str, group: str, pos: str) -> str:
    if key.startswith("legacy/"):
        return "legacy"
    if key.startswith("rev/"):
        if key.startswith("rev/m2/using_") and "aliased" not in key:
            return "using-unaliased-refusal"
        if key.endswith(CONTROL_KEYS):
            return "control:revision"
        return "common-type"
    for prefix, name in SINGLE.items():
        if key.startswith(prefix):
            return name
    if key.endswith("/update_set_coalesce"):
        return "update-cse"
    if key.startswith("special/w_range"):
        return "range-offset"
    if key.startswith("special/pivot"):
        return "pivot-unsupported"
    if key.startswith("special/gsets_ac"):
        return "grouping-control"
    if group and "control" in group:
        return "control:" + (pos if pos in CONFOUNDED else "core")
    if key.startswith("pair/") and pos in CONFOUNDED:
        return "confounded:" + pos
    return "common-type"


def load(name: str) -> dict:
    path = raw / name
    if not path.exists():
        return {}
    rows = [json.loads(line) for line in path.open()]
    return {r["key"]: r for r in rows}


def trim(m: re.Match) -> str:
    text = m.group(0).rstrip("0").rstrip(".")
    return "0" if text in ("-0", "") else text


def numeric(res: dict) -> list:
    return sorted(NUM.sub(trim, row) for row in res["rows"])


def is_err(res: object) -> bool:
    return isinstance(res, str)


def is_write(rec: dict) -> bool:
    if rec.get("group") in WRITE_GROUPS:
        return True
    return rec.get("group") == "icepart" and not rec["key"].startswith("icepart/read/")


def verdict(rp: dict, sp: dict) -> str:
    r, s = rp.get("result"), sp.get("result")
    rst, sst = rp.get("statement", "ok"), sp.get("statement", "ok")
    write = is_write(rp)
    if write:
        r_fail, s_fail = rst != "ok" or is_err(r), sst != "ok" or is_err(s)
        if r_fail and s_fail:
            return "both_raise"
        if r_fail:
            return "over_raise"
        if s_fail:
            return "missed_raise"
        if r == s or (r["types"] == s["types"] and numeric(r) == numeric(s)):
            return "agree"
        if numeric(r) == numeric(s):
            return "wrong_type"
        return "wrong_write"
    if is_err(r) and is_err(s):
        return "both_raise"
    if is_err(r):
        return "over_raise"
    if is_err(s):
        return "missed_raise"
    if r == s or (r["types"] == s["types"] and numeric(r) == numeric(s)):
        return "agree"
    if numeric(r) == numeric(s):
        return "wrong_type"
    return "wrong_answer"


def clip(res: object) -> str:
    return json.dumps(res)[:200].rsplit(" ", 1)[0]


def short(res: object) -> object:
    if isinstance(res, str):
        return res[:160].rsplit(" ", 1)[0]
    return res


def main() -> None:
    verdicts = []
    for ansi in ("false", "true"):
        sources = (
            ("repark", f"repark-{ansi}.jsonl", f"spark-{ansi}.jsonl"),
            ("native", "native.jsonl", f"spark-{ansi}.jsonl"),
            ("repark", f"revision-repark-{ansi}.jsonl", f"revision-spark-{ansi}.jsonl"),
        )
        for engine, name, spark_name in sources:
            rp = load(name)
            sp = load(spark_name)
            for key, rec in rp.items():
                if "result" not in rec or key.startswith(("provenance", "data/", "elapsed")):
                    continue
                skey = rec.get("spark_key", key)
                if skey not in sp:
                    continue
                if key.startswith("legacy/") and key in (
                    "legacy/set",
                    "legacy/reset",
                    "legacy/after_reset_conf",
                ):
                    continue
                v = verdict(rec, sp[skey])
                verdicts.append(
                    {
                        "key": key,
                        "engine": engine,
                        "ansi": ansi,
                        "group": rec.get("group"),
                        "door": rec.get("door", "native" if engine == "native" else "sql"),
                        "pos": rec.get("pos"),
                        "pair": rec.get("pair"),
                        "verdict": v,
                        "cause": cause(key, rec.get("group", ""), rec.get("pos", "")),
                        "repark": short(rec.get("result")),
                        "repark_statement": rec.get("statement"),
                        "spark": short(sp[skey].get("result")),
                        "spark_statement": sp[skey].get("statement"),
                    }
                )
    with (out / "verdicts.jsonl").open("w") as fh:
        for v in verdicts:
            fh.write(json.dumps(v, sort_keys=True) + "\n")
    lines = ["# Summary (written by analyze.py)", ""]
    bad = {"wrong_answer", "wrong_type", "wrong_write", "over_raise", "missed_raise"}

    def table(title: str, keyf: Callable[[dict], str]) -> None:
        agg: dict = defaultdict(Counter)
        for v in verdicts:
            agg[keyf(v)][v["verdict"]] += 1
        cols = [
            "agree",
            "both_raise",
            "wrong_answer",
            "wrong_type",
            "wrong_write",
            "over_raise",
            "missed_raise",
        ]
        lines.extend(
            [
                f"## {title}",
                "",
                "| key | cells | " + " | ".join(cols) + " |",
                "|---|---|" + "---|" * len(cols),
            ]
        )
        for k in sorted(agg, key=str):
            c = agg[k]
            lines.append(
                f"| {k} | {sum(c.values())} | " + " | ".join(str(c[x]) for x in cols) + " |"
            )
        lines.append("")

    table("By engine and ANSI", lambda v: f"{v['engine']} ansi={v['ansi']}")
    table(
        "By cause (RePark Spark door)",
        lambda v: f"{v['cause']} ansi={v['ansi']}" if v["engine"] == "repark" else "(native)",
    )
    table(
        "By cause (native door)",
        lambda v: f"{v['cause']} ansi={v['ansi']}" if v["engine"] == "native" else "(repark)",
    )
    table(
        "Common-type cells by group (RePark Spark door)",
        lambda v: (
            f"{v['group']} ansi={v['ansi']}"
            if v["engine"] == "repark" and v["cause"] == "common-type"
            else "(other)"
        ),
    )
    table("By group (both ANSI modes, both doors)", lambda v: f"{v['engine']} {v['group']}")
    table("By door", lambda v: f"{v['engine']} {v['door']}")
    table(
        "By pair (pair cells, Spark-door SQL)",
        lambda v: (
            v["pair"] if v["key"].startswith("pair/") and v["engine"] == "repark" else "(other)"
        ),
    )
    table(
        "By position (pair cells, Spark-door SQL)",
        lambda v: (
            v["pos"] if v["key"].startswith("pair/") and v["engine"] == "repark" else "(other)"
        ),
    )
    table(
        "By position (pair cells, native door)",
        lambda v: (
            v["pos"] if v["key"].startswith("pair/") and v["engine"] == "native" else "(other)"
        ),
    )
    lines.extend(["## Every non-agreeing cell outside the pair grid", ""])
    for v in verdicts:
        if (
            v["verdict"] in bad
            and not v["key"].startswith("pair/")
            and v["engine"] == "repark"
            and v["ansi"] == "false"
        ):
            lines.append(
                f"- `{v['engine']}` ansi={v['ansi']} `{v['key']}`: **{v['verdict']}**; "
                f"RePark `{clip(v['repark'])}`; Spark `{clip(v['spark'])}`"
            )
    lines.append("")
    (out / "summary.md").write_text("\n".join(lines) + "\n")
    print(Counter((v["engine"], v["ansi"], v["verdict"]) for v in verdicts))


main()
