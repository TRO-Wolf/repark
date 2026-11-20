import sys
from pathlib import Path

from common import Rec, session
from seat_emu import emulate

PAIRS: dict[str, list[str]] = {
    "INT": ["-007", "1.0", "0x10", "1_000", " 7", "7 "],
    "BIGINT": ["-007", "9223372036854775807", "9223372036854775808"],
    "BOOLEAN": ["t", "1", "0", " true", "FALSE", "y", "false "],
    "DOUBLE": ["NaN", "Inf", "-Inf", "Infinity", "1d", "1,5", " 1e3", "+1.5"],
    "DECIMAL(10,2)": ["1,000.5", "1.005", "123456789.1", "-0.5", "+5", "1e400", ".5"],
    "DATE": ["2024-1-5", "2024-01-15T10:30:00", "20240115", "2024-01-15 10:30:00", " 2024-01-15"],
    "TIMESTAMP": [
        "2024-01-15T10:30:00Z",
        "2024-01-15 10:30:00+02:00",
        "2024-01-15",
        "1705314600",
        "2024-01-15 10:30",
    ],
    "TIMESTAMP_NTZ": ["2024-01-15T10:30:00", "2024-01-15 10:30:00+02:00", "2024-01-15"],
}


def gen(data: Path) -> None:
    data.mkdir(parents=True, exist_ok=True)
    for kind, texts in PAIRS.items():
        for index, text in enumerate(texts):
            quoted = f'"{text}"' if "," in text else text
            name = f"kx{kind.split('(')[0]}{index}-alone-off.csv"
            (data / name).write_text(f"{quoted},k\n")


def main() -> None:
    engine, parent, data_dir, out, ansi = sys.argv[1:6]
    data = Path(data_dir)
    if engine == "gen":
        gen(data)
        return
    s, _ = session(engine, parent)
    r = Rec(s, out)
    r.conf(zone="UTC", ansi=ansi)
    variants = ("spark",) if engine == "spark" else ("base", "text_try")
    for kind, texts in PAIRS.items():
        for index in range(len(texts)):
            path = str(data / f"kx{kind.split('(')[0]}{index}-alone-off.csv")
            schema = f"x {kind}, k STRING"
            for variant in variants:
                tag = f"K/{ansi}/{variant}/{kind}/{index}"
                if variant == "text_try":
                    r.run(tag, lambda p=path, sc=schema: emulate(s, "text_try", p, "off", sc, "S"))
                else:
                    r.run(tag, lambda p=path, sc=schema: s.read.schema(sc).csv(p))
    s.stop()


if __name__ == "__main__":
    main()
