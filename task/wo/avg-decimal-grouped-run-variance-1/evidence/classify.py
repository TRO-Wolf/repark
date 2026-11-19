import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

from model import Model

ORDER = ["2c on", "4c on", "8c on", "10c on", "2c off", "4c off", "8c off", "10c off"]
ORDER += ["SET 2", "SET 4", "SET 8"]


def config(row: dict[str, object]) -> str:
    if row["tp"]:
        return f"SET {row['tp']}"
    return f"{row['cpus']}c {row['steal']}"


def norm(label: str) -> str:
    got = json.loads(label)
    if isinstance(got, dict) and "statement" in got and got["statement"] != "ok":
        return json.dumps({"statement": got["statement"], "rows": "ERR"})
    return label


def is_err(label: str) -> bool:
    got = json.loads(label)
    if isinstance(got, dict) and "statement" in got:
        return got["statement"] != "ok"
    return isinstance(got, str) and got.startswith("ERR")


def short(label: str) -> str:
    got = json.loads(label)
    if isinstance(got, str):
        return got
    if isinstance(got, dict) and "sha" in got:
        return f"rows={got['rows']} nulls={got['nulls']}"
    if isinstance(got, dict) and "statement" in got:
        return f"stmt {got['statement']}; " + short(json.dumps(got["rows"]))
    if isinstance(got, list) and len(got) == 1:
        return "|".join("NULL" if v is None else str(v) for v in got[0])
    text = json.dumps(got)
    return text if len(text) <= 70 else text[:67] + "..."


def verdict(label: str, spark: str | None, model: str) -> str:
    if spark is None:
        return "-"
    if label == spark:
        return "E"
    if is_err(label) and is_err(spark):
        return "C"
    if is_err(label) or is_err(spark):
        return "R"
    return "W" if label == model else "X"


def exact(label: str, model: str) -> bool:
    return label == model or (is_err(label) and is_err(model))


def load_spark(path: Path) -> dict[tuple[str, str, str], tuple[str, int]]:
    out = {}
    for line in path.read_text().splitlines():
        row = json.loads(line)
        seen = Counter({norm(k): v for k, v in row["answers"].items()})
        out[(row["master"], row["ansi"], row["query"])] = (seen.most_common(1)[0][0], len(seen))
    return out


def main() -> None:
    data, spark_path, out_dir = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    builds = [Path(p) for p in sys.argv[4:]]
    model = Model(data)
    spark = load_spark(spark_path)
    out_dir.mkdir(parents=True, exist_ok=True)
    for path in builds:
        cells: dict[tuple[str, str], dict[str, tuple[str, int, Counter[str]]]] = defaultdict(dict)
        for line in path.read_text().splitlines():
            row = json.loads(line)
            seen = Counter({norm(k): v for k, v in row["answers"].items()})
            label = seen.most_common(1)[0][0]
            cells[(row["query"], row["ansi"])][config(row)] = (label, len(seen), seen)
        lines = ["query\tansi\tconfigs\tvs_local1\tvs_local4\texact\tanswers"]
        for (query, ansi), per in sorted(cells.items()):
            want = json.dumps(model.answer(query, ansi))
            s1 = spark.get(("local[1]", ansi, query), (None, 0))[0]
            s4 = spark.get(("local[4]", ansi, query), (None, 0))[0]
            names = [c for c in ORDER if c in per]
            v1, v4, ex, answers = [], [], [], Counter()
            for name in names:
                label, distinct, seen = per[name]
                answers.update(seen)
                if distinct > 1:
                    v1.append(f"V{distinct}")
                    v4.append(f"V{distinct}")
                    ex.append("v")
                    continue
                v1.append(verdict(label, s1, want))
                v4.append(verdict(label, s4, want))
                ex.append("y" if exact(label, want) else "n")
            listed = "; ".join(f"{short(k)} x{v}" for k, v in answers.most_common(4))
            row = [query, ansi, ",".join(names), " ".join(v1), " ".join(v4), "".join(ex), listed]
            lines.append("\t".join(row))
        (out_dir / f"verdicts-{path.stem}.tsv").write_text("\n".join(lines) + "\n")
        print(path.stem, len(lines) - 1, "rows", flush=True)
    rows = [
        "query\tansi\tlocal1\tlocal4\tspark_runs_distinct\tmodel\tmodel_vs_local1\tmodel_vs_local4"
    ]
    for query, ansi in sorted({(q, a) for _, a, q in spark}):
        want = json.dumps(model.answer(query, ansi))
        s1, d1 = spark.get(("local[1]", ansi, query), (None, 0))
        s4, d4 = spark.get(("local[4]", ansi, query), (None, 0))
        m1 = verdict(want, s1, want)
        m4 = verdict(want, s4, want)
        rows.append(
            "\t".join(
                [
                    query,
                    ansi,
                    short(s1) if s1 else "-",
                    short(s4) if s4 else "-",
                    f"{d1}/{d4}",
                    short(want),
                    m1,
                    m4,
                ]
            )
        )
    (out_dir / "spark-vs-model.tsv").write_text("\n".join(rows) + "\n")
    print("spark", len(rows) - 1, "rows", flush=True)


if __name__ == "__main__":
    main()
