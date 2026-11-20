import gzip
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import model

DROP = {"rows", "names", "types", "error", "ms", "plan", "surf"}
TIE_ROWS = {
    "T": (
        [(1, "a", 1, 10), (2, "a", 1, 20)],
        [
            (1, "a", 1, 10, "x1"),
            (2, "a", 1, 10, "x2"),
            (3, "a", 1, 10, "x3"),
            (4, "a", 1, 18, "y18"),
            (5, "a", 1, 22, "z22"),
        ],
    ),
}
NUMERIC = ("INT", "BIGINT", "DOUBLE", "DECIMAL(10,2)")
P_ON = {
    "none": "none",
    "by_k1": "k1",
    "by_k1k2": "k1k2",
    "left_right_by": "k1",
    "left_on_right_on": "k1",
    "timestamp_timedelta": "k1",
}


def load(raw: Path) -> dict[str, dict[tuple[str, str], dict[str, Any]]]:
    out: dict[str, dict[tuple[str, str], dict[str, Any]]] = {"spark": {}, "main": {}}
    for path in sorted(raw.glob("*.jsonl*")):
        if not path.name.startswith(("spark-", "repark-")):
            continue
        engine = "spark" if path.name.startswith("spark") else "main"
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt") as handle:
            for line in handle:
                rec = json.loads(line)
                if rec["g"] == "X":
                    continue
                key = json.dumps({k: v for k, v in rec.items() if k not in DROP}, sort_keys=True)
                out[engine][(key, rec["surf"])] = rec
    return out


def pairs(answer: dict[str, Any], rid_at: int) -> dict[str, str] | None:
    found: dict[str, str] = {}
    for text in answer["rows"]:
        row = json.loads(text)
        if row[0] in found:
            return None
        found[row[0]] = row[rid_at]
    return found


def tie_sets(rec: dict[str, Any]) -> dict[str, list[str] | None] | None:
    if rec["g"] == "G":
        return model.ties(rec)
    if rec["g"] == "M" and all(x in NUMERIC for x in rec["typ"].split(">")):
        return model.ties(rec)
    if rec["g"] == "T":
        lrows, rrows = TIE_ROWS["T"]
        return model.ties(rec, lrows, rrows)
    if rec["g"] == "P" and rec["case"] in P_ON:
        return model.ties({**rec, "on": P_ON[rec["case"]], "how": "left"})
    return None


def valid(found: dict[str, str] | None, sets: dict[str, list[str] | None]) -> bool:
    if found is None or set(found) != set(sets):
        return False
    for lid, rid in found.items():
        allowed = sets[lid]
        if allowed is None and rid != "None":
            return False
        if allowed is not None and rid not in allowed:
            return False
    return True


def rid_index(answer: dict[str, Any]) -> int:
    names = answer.get("names", [])
    return names.index("rid") if "rid" in names else 5


def verdict(
    oracle: dict[str, Any], got: dict[str, Any], surf: str, sets: dict[str, list[str] | None] | None
) -> str:
    gerr = got.get("error", "")
    if surf == "D" and gerr.startswith("PySparkAttributeError"):
        return "MISSING"
    if surf == "PD" and "rows" in oracle and "rows" in got:
        po, pg = pairs(oracle, rid_index(oracle)), pairs(got, rid_index(got))
        if po == pg:
            return "EQUAL"
        if sets is not None and valid(po, sets) and valid(pg, sets):
            return "TIE"
        return "WRONG"
    if "error" in oracle and "error" in got:
        return "EQUAL"
    if "error" in oracle:
        return "WRONG"
    if "error" in got:
        return "RAISE"
    if oracle["rows"] == got["rows"]:
        if oracle["names"] != got["names"]:
            return "WRONG-NAMES"
        if oracle["types"] != got["types"]:
            return "WRONG-TYPE"
        return "EQUAL"
    if sets is not None:
        po, pg = pairs(oracle, rid_index(oracle)), pairs(got, rid_index(got))
        if valid(po, sets) and valid(pg, sets):
            return "TIE"
    return "WRONG"


def main() -> None:
    raw, out = Path(sys.argv[1]), Path(sys.argv[2])
    data = load(raw)
    spark, main_ = data["spark"], data["main"]
    counts: Counter[tuple[str, ...]] = Counter()
    with out.open("w") as handle:
        for (key, surf), rec in sorted(spark.items()):
            sets = tie_sets(rec)
            if surf == "D" and sets is not None and "error" not in rec:
                ok = valid(pairs(rec, rid_index(rec)), sets)
                counts[
                    ("model", rec["g"], rec["ansi"], "spark-D", "AGREE" if ok else "DISAGREE")
                ] += 1
            if surf in ("W1", "W2", "PD"):
                oracle = spark.get((key, "D" if surf != "PD" else "P"))
                if oracle is not None:
                    v = verdict(oracle, rec, surf, sets)
                    counts[("fidelity", rec["g"], rec["ansi"], "spark-" + surf, v)] += 1
                    handle.write(
                        json.dumps({"key": json.loads(key), "pair": "spark-" + surf, "v": v}) + "\n"
                    )
            if surf in ("D", "P", "SQL", "F.expr", "F", "DataFrame"):
                for msurf in ("D", "W1", "W2") if surf == "D" else (surf,):
                    got = main_.get((key, msurf))
                    if surf == "P":
                        v = "MISSING"
                    elif got is None:
                        continue
                    else:
                        v = verdict(rec, got, msurf, sets)
                    counts[("domain", rec["g"], rec["ansi"], "main-" + msurf, v)] += 1
                    handle.write(
                        json.dumps(
                            {
                                "key": json.loads(key),
                                "pair": "main-" + msurf,
                                "v": v,
                                "spark": rec.get("error", "rows"),
                                "main": "" if got is None else got.get("error", "rows"),
                            }
                        )
                        + "\n"
                    )
                    if msurf in ("W1", "W2"):
                        sw = spark.get((key, msurf))
                        if sw is not None and got is not None:
                            pv = verdict(sw, got, msurf, sets)
                            counts[("engine", rec["g"], rec["ansi"], msurf, pv)] += 1
    for k, n in sorted(counts.items()):
        print("\t".join(k), n, sep="\t")


if __name__ == "__main__":
    main()
