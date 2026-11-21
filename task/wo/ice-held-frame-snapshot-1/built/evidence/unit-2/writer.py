import sys

from common import condition, session

engine, wh, statement = sys.argv[1:4]
s, _, _ = session(engine, wh, "true", wh)
try:
    s.sql(statement).collect()
    print("WROTE", statement, flush=True)
except Exception as error:
    print("WRITE_ERROR", condition(error), flush=True)
    sys.exit(3)
