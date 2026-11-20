import sys

from common import Rec, session

TEXTS = ["007", "+5", "1.50", "1e3", "5.", " 7 ", "1.5", "TRUE", "yes", "2147483648", "abc"]
TYPES = ["INT", "BIGINT", "DECIMAL(10,2)", "DOUBLE", "BOOLEAN"]


def main() -> None:
    engine, parent, out = sys.argv[1:4]
    s, _ = session(engine, parent)
    r = Rec(s, out)
    for ansi in ("true", "false"):
        r.conf(zone="UTC", ansi=ansi)
        for kind in TYPES:
            for fn in ("CAST", "TRY_CAST"):
                for index, text in enumerate(TEXTS):
                    query = f"SELECT {fn}(t AS {kind}) AS v FROM VALUES ('{text}') AS w(t)"
                    r.sql(f"C/{ansi}/{fn}/{kind}/{index}", query)
    s.stop()


if __name__ == "__main__":
    main()
