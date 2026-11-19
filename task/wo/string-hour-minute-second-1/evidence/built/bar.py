import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

HMS = ("hour", "minute", "second")
CFGS = ("a0-utc", "a0-la", "a1-utc", "a1-la", "a1-utc-ntz", "a1-la-ntz")
Key = tuple[str, str, str, str, str]


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


def load_all(raw: Path, engine: str) -> dict[Key, tuple[str, str]]:
    out: dict[Key, tuple[str, str]] = {}
    for cfg in CFGS:
        out.update(load(raw / f"{engine}-{cfg}.jsonl"))
        for part in sorted(raw.glob(f"{engine}-{cfg}--*.jsonl")):
            out.update(load(part))
    return out


def verdict(spark: tuple[str, str], got: tuple[str, str] | None) -> str:
    if got is None:
        return "MISSING"
    if spark[0] == "v" and got[0] == "v":
        return "EQUAL" if spark[1] == got[1] else "WRONG"
    if spark[0] == "v":
        return "RAISE"
    if got[0] == "v":
        return "WRONG"
    return "EQUAL" if spark[1] == got[1] else "RAISE"


def scope(key: Key) -> str:
    cfg, _, sh, _, fn = key
    if fn not in HMS:
        return "other"
    if sh == "g":
        return "multi-row"
    return "ntz" if cfg.endswith("ntz") else "ltz"


def short(answer: tuple[str, str] | None) -> str:
    if answer is None:
        return "-"
    return answer[1] if answer[0] == "v" else "!" + answer[1][:40]


def main() -> None:
    spark = load_all(Path(sys.argv[1]), "spark")
    base = load_all(Path(sys.argv[2]), "main")
    head = load_all(Path(sys.argv[3]), "main")
    out = Path(sys.argv[4])
    out.mkdir(parents=True, exist_ok=True)
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    lost, gained, moved, off_hms = [], [], [], []
    for key in sorted(spark):
        vb, vh = verdict(spark[key], base.get(key)), verdict(spark[key], head.get(key))
        sc = scope(key)
        label = (
            sc + " " + key[4]
            if sc != "other"
            else ("ntz other" if key[0].endswith("ntz") else "ltz other")
        )
        counts["base " + label][vb] += 1
        counts["head " + label][vh] += 1
        counts["base all"][vb] += 1
        counts["head all"][vh] += 1
        row = "\t".join(
            [*key, short(spark[key]), short(base.get(key)), short(head.get(key)), vb, vh]
        )
        if vb == "EQUAL" and vh != "EQUAL":
            lost.append(row)
        if vb != "EQUAL" and vh == "EQUAL":
            gained.append(key)
        if base.get(key) != head.get(key):
            moved.append(row)
        if sc != "other" and vh != "EQUAL":
            off_hms.append(row)
    lines = [f"cells: spark {len(spark)}, base {len(base)}, head {len(head)}", ""]
    lines.append("| scope | build | EQUAL | WRONG | RAISE | MISSING | total |")
    lines.append("|---|---|---|---|---|---|---|")
    for label in sorted({k.split(" ", 1)[1] for k in counts}):
        for build in ("base", "head"):
            c = counts[build + " " + label]
            cells = [str(c[v]) for v in ("EQUAL", "WRONG", "RAISE", "MISSING")]
            lines.append(f"| {label} | {build} | " + " | ".join(cells) + f" | {sum(c.values())} |")
    moved_fns = Counter(r.split("\t")[4] for r in moved)
    gained_fns = Counter(k[4] for k in gained)
    lines += [
        "",
        f"moved cells (head answer differs from base): {len(moved)}",
        json.dumps(dict(moved_fns)),
    ]
    lines += [f"gained EQUAL: {len(gained)}", json.dumps(dict(gained_fns))]
    lines += [f"lost EQUAL: {len(lost)}", *lost]
    lines += ["", f"hms cells not EQUAL on head: {len(off_hms)}", *off_hms]
    (out / "bar-summary.md").write_text("\n".join(lines) + "\n")
    header = "cfg\tin\tshape\tdoor\tfn\tspark\tbase\thead\tbase verdict\thead verdict\n"
    (out / "moved.tsv").write_text(header + "\n".join(moved) + "\n")
    print("\n".join(lines[:80]))


main()
