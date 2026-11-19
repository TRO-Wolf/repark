import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from cells import CONFIGS, FUNCS, SHAPES

HMS = ["hour", "minute", "second"]
FORECAST = {
    "hour": "cast_hour",
    "minute": "cast_minute",
    "second": "cast_second",
    "extract_HOUR": "cast_hour",
    "date_part_HOUR": "cast_hour",
    "extract_MINUTE": "cast_minute",
    "date_part_MINUTE": "cast_minute",
    "extract_H": "cast_hour",
    "extract_HRS": "cast_hour",
    "date_part_min": "cast_minute",
}
HMS_CELLS = {
    "hour",
    "minute",
    "second",
    "cast_hour",
    "cast_minute",
    "cast_second",
    "extract_HOUR",
    "extract_MINUTE",
    "extract_SECOND",
    "date_part_HOUR",
    "date_part_MINUTE",
    "date_part_SECOND",
}
Key = tuple[str, str, str, str, str]


def family(fid: str) -> str:
    if fid in HMS:
        return "hms"
    if fid.startswith("cast_"):
        return "control"
    if fid.startswith(("extract_", "date_part_")):
        return fid.split("_")[0] + ("_part" if fid.startswith("date_part") else "")
    if fid.startswith("date_trunc"):
        return "date_trunc"
    if fid.startswith("date_format") or fid == "unix_timestamp":
        return "format"
    return "date_field"


def load(path: Path) -> dict[Key, tuple[str, str]]:
    out: dict[Key, tuple[str, str]] = {}
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        r = json.loads(line)
        if "fn" not in r:
            continue
        key = (r["cfg"], r["in"], r["sh"], r["door"], r["fn"])
        out[key] = ("err", r["err"]) if "err" in r else ("v", r.get("v", "?"))
    return out


def verdict(spark: tuple[str, str], main: tuple[str, str]) -> str:
    if spark[0] == "v" and main[0] == "v":
        return "EQUAL" if spark[1] == main[1] else "WRONG"
    if spark[0] == "v":
        return "RAISE"
    if main[0] == "v":
        return "WRONG"
    return "EQUAL" if spark[1] == main[1] else "RAISE"


def table(title: str, rows: dict[str, Counter[str]], lines: list[str]) -> None:
    lines.append("## " + title)
    lines.append("| key | EQUAL | WRONG | RAISE | total |")
    lines.append("|---|---|---|---|---|")
    for key in sorted(rows):
        c = rows[key]
        total = sum(c.values())
        lines.append(
            "| " + key + " | " + " | ".join(str(c[v]) for v in ("EQUAL", "WRONG", "RAISE"))
        )
        lines[-1] += " | " + str(total) + " |"
    lines.append("")


def short(answer: tuple[str, str] | None) -> str:
    if answer is None:
        return "-"
    if answer[0] == "v":
        return answer[1]
    text = answer[1]
    return "!" + (text if text.isupper() and " " not in text else text[:24])


def extra(
    spark: dict[Key, tuple[str, str]],
    main_: dict[Key, tuple[str, str]],
    base: dict[Key, str],
    lines: list[str],
) -> None:
    lines.append("## hms not-EQUAL cells by shape and configuration (each of 16 = 4 in x 4 doors)")
    cfgs = [c for c in CONFIGS if not c.endswith("ntz")]
    lines.append("| shape | " + " | ".join(cfgs) + " |")
    lines.append("|---|" + "---|" * len(cfgs))
    for sh in SHAPES:
        row = []
        for cfg in cfgs:
            c = Counter(
                v[0]
                for (k_cfg, _, k_sh, _, fn), v in base.items()
                if k_cfg == cfg and k_sh == sh and fn in HMS and v != "EQUAL"
            )
            row.append(" ".join(k + str(n) for k, n in sorted(c.items())) or ".")
        lines.append("| " + sh + " | " + " | ".join(row) + " |")
    lines.append("")
    lines.append(
        "## Literal SQL answers, Spark / main (hour, minute, second, extract_SECOND, year)"
    )
    shown = ["hour", "minute", "second", "extract_SECOND", "year", "cast_hour"]
    for cfg in CONFIGS:
        lines.append("### " + cfg)
        lines.append("| shape | " + " | ".join(shown) + " |")
        lines.append("|---|" + "---|" * len(shown))
        for sh in SHAPES:
            cells = []
            for fn in shown:
                key = (cfg, "L", sh, "sql", fn)
                cells.append(short(spark.get(key)) + " / " + short(main_.get(key)))
            lines.append("| " + sh + " | " + " | ".join(cells) + " |")
        lines.append("")
    lines.append("## G doors by function and door (each of 12 = 4 cfg x 3 in)")
    g: dict[str, Counter[str]] = defaultdict(Counter)
    for (_, _, sh, door, fn), v in base.items():
        if sh == "g":
            g[fn + " " + door][v] += 1
    table("G doors", g, lines)
    lines.append(
        "## Door spread: (cfg, in, shape, fn) groups whose main answers differ across doors"
    )
    groups: dict[tuple[str, str, str, str], set[str]] = defaultdict(set)
    for (cfg, inp, sh, door, fn), ans in main_.items():
        if sh != "g" and not cfg.endswith("ntz"):
            groups[(cfg, inp, sh, fn)].add(door + "=" + short(ans)[:20])
    spread = Counter()
    examples: dict[str, str] = {}
    for (cfg, inp, sh, fn), answers in groups.items():
        values = {a.split("=", 1)[1] for a in answers}
        if len(values) > 1:
            spread[fn] += 1
            examples.setdefault(fn, cfg + " " + inp + " " + sh + " " + " ".join(sorted(answers)))
    for fn, n in sorted(spread.items()):
        lines.append("- " + fn + ": " + str(n) + "; e.g. " + examples[fn])
    lines.append("")


def main() -> None:
    raw, out_dir = Path(sys.argv[1]), Path(sys.argv[2])
    spark: dict[Key, tuple[str, str]] = {}
    main_: dict[Key, tuple[str, str]] = {}
    for cfg in CONFIGS:
        spark.update(load(raw / ("spark-" + cfg + ".jsonl")))
        for part in sorted(raw.glob("spark-" + cfg + "--*.jsonl")):
            spark.update(load(part))
        main_.update(load(raw / ("main-" + cfg + ".jsonl")))
    verdicts: dict[Key, str] = {}
    full = Path(sys.argv[3]) if len(sys.argv) > 3 else out_dir / "verdicts.jsonl"
    with full.open("w") as handle:
        for key in sorted(spark):
            if key not in main_:
                continue
            v = verdict(spark[key], main_[key])
            verdicts[key] = v
            cfg, inp, sh, door, fn = key
            record: dict[str, Any] = {"cfg": cfg, "in": inp, "sh": sh, "door": door, "fn": fn}
            record.update({"spark": list(spark[key]), "main": list(main_[key]), "verdict": v})
            handle.write(json.dumps(record) + "\n")
    with (out_dir / "hms-cells.tsv").open("w") as handle:
        handle.write("cfg\tin\tshape\tdoor\tfn\tspark\tmain\tverdict\n")
        for key, v in sorted(verdicts.items()):
            if key[4] in HMS_CELLS:
                answers = [short(spark[key]), short(main_[key])]
                handle.write("\t".join([*key, *answers, v]) + "\n")
    lines: list[str] = []
    base = {k: v for k, v in verdicts.items() if not k[0].endswith("ntz")}
    ntz = {k: v for k, v in verdicts.items() if k[0].endswith("ntz")}
    lines.append("cells: " + str(len(base)) + " (LTZ configurations), " + str(len(ntz)) + " (NTZ)")
    lines.append("missing in main: " + str(len([k for k in spark if k not in main_])))
    lines.append("")
    total: dict[str, Counter[str]] = defaultdict(Counter)
    by_cfg: dict[str, Counter[str]] = defaultdict(Counter)
    by_family: dict[str, Counter[str]] = defaultdict(Counter)
    by_door: dict[str, Counter[str]] = defaultdict(Counter)
    by_input: dict[str, Counter[str]] = defaultdict(Counter)
    for key, v in base.items():
        cfg, inp, sh, door, fn = key
        total["all"][v] += 1
        by_cfg[cfg][v] += 1
        by_family[family(fn)][v] += 1
        if sh != "g":
            by_door[door][v] += 1
            by_input[inp][v] += 1
        else:
            by_door["g:" + door][v] += 1
    table("All LTZ cells", total, lines)
    table("By configuration", by_cfg, lines)
    table("By family", by_family, lines)
    table("By door", by_door, lines)
    table("By input (projection doors)", by_input, lines)
    by_fn: dict[str, Counter[str]] = defaultdict(Counter)
    for (_, _, sh, _, fn), v in base.items():
        if sh != "g":
            by_fn[fn][v] += 1
    table("By function (projection doors)", by_fn, lines)
    lines.append(
        "## Not-EQUAL projection cells by function and shape (each of 64 = 4 cfg x 4 in x 4 doors)"
    )
    shapes = list(SHAPES)
    lines.append("| function | " + " | ".join(shapes) + " |")
    lines.append("|---|" + "---|" * len(shapes))
    grid: dict[str, Counter[str]] = defaultdict(Counter)
    for (_, _, sh, _, fn), v in base.items():
        if sh != "g" and v != "EQUAL":
            grid[fn][sh + v[0]] += 1
    for fid, _, _ in FUNCS:
        cells = []
        for sh in shapes:
            w, r = grid[fid][sh + "W"], grid[fid][sh + "R"]
            cells.append(("W" + str(w) if w else "") + ("R" + str(r) if r else "") or ".")
        lines.append("| " + fid + " | " + " | ".join(cells) + " |")
    lines.append("")
    lines.append("## Spark: implicit cast equals the explicit cast (hour/minute/second vs cast_*)")
    same: Counter[str] = Counter()
    for (cfg, inp, sh, door, fn), ans in spark.items():
        if fn in HMS and sh != "g":
            other = spark.get((cfg, inp, sh, door, "cast_" + fn))
            same[("ntz:" if cfg.endswith("ntz") else "ltz:") + str(other == ans)] += 1
    lines.append(json.dumps(dict(sorted(same.items()))))
    lines.append("")
    lines.append("## Seat forecast: main's cast_* answer against Spark's answer, per cell")
    fc: dict[str, Counter[str]] = defaultdict(Counter)
    residue: list[str] = []
    for key, sp in spark.items():
        cfg, inp, sh, door, fn = key
        if fn not in FORECAST or sh == "g":
            continue
        proxy = main_.get((cfg, inp, sh, door, FORECAST[fn]))
        if proxy is None:
            continue
        v = verdict(sp, proxy)
        fc[fn + (" ntz" if cfg.endswith("ntz") else "")][v] += 1
        if v != "EQUAL" and fn in HMS:
            residue.append(" ".join(key) + " spark=" + str(sp) + " proxy=" + str(proxy))
    table("forecast by function", fc, lines)
    lines.append("residue (hms): " + str(len(residue)))
    lines.extend(sorted(residue)[:120])
    lines.append("")
    lines.append("## NTZ configurations (spark.sql.timestampType=TIMESTAMP_NTZ, ANSI on, L and I)")
    ntz_fn: dict[str, Counter[str]] = defaultdict(Counter)
    for (_, _, _, _, fn), v in ntz.items():
        ntz_fn[fn][v] += 1
    table("NTZ by function", ntz_fn, lines)
    extra(spark, main_, base, lines)
    (out_dir / "summary.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines[:40]))


main()
