import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

CASES = ["k0_count", "k0_hour", "is_count", "is_sum", "is_none", "is_infer"]


def main() -> None:
    samples: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    answers: dict[tuple[str, str], set[str]] = defaultdict(set)
    for line in Path(sys.argv[1]).read_text().splitlines():
        record = json.loads(line)
        build = record["label"].split("-")[0]
        samples[(record["case"], build, record["variant"])].append(record["seconds"])
        answers[(record["case"], record["variant"])].add(record["answer"])
    head = "| case | main base | main seat | unit 1 base | unit 1 seat | seat / base, unit 1 |"
    lines = [head + " unit 1 / main base | rounds |", "|---" * 8 + "|"]
    for case in CASES:
        med = {
            key: statistics.median(values) * 1000
            for key, values in samples.items()
            if key[0] == case
        }
        main_base = med[(case, "main", "base")]
        unit_base = med[(case, "unit1", "base")]
        main_seat = med.get((case, "main", "seat"))
        unit_seat = med.get((case, "unit1", "seat"))
        ratio = f"**{unit_seat / unit_base:.3f}x**" if unit_seat else "—"
        rounds = len(samples[(case, "unit1", "base")])
        cells = [
            case,
            f"{main_base:.2f}",
            f"{main_seat:.2f}" if main_seat else "—",
            f"{unit_base:.2f}",
            f"{unit_seat:.2f}" if unit_seat else "—",
            ratio,
            f"{unit_base / main_base:.3f}x",
            str(rounds),
        ]
        lines.append("| " + " | ".join(cells) + " |")
    lines.append("")
    for key, values in sorted(answers.items()):
        lines.append(f"- answers {key[0]} {key[1]}: {len(values)} distinct")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
