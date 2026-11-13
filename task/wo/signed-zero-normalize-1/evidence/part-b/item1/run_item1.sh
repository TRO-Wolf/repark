set -eu
O="$(cd "$(dirname "$0")" && pwd)"
C="${SZB_CLONE:?set SZB_CLONE to the RePark tree}"
W="${SZB_WORK:?set SZB_WORK to a scratch working directory}"
R="${SZB_RAW:-$O/raw}"
ENGINE="$1"
PART="$2"
OUT="$3"
shift 3
mkdir -p "$R" "$W"
cd "$W"
export SZB_CLONE="$C"
if [ "$ENGINE" = "spark" ]; then
  export JAVA_HOME="${JAVA_HOME:?set JAVA_HOME to a JDK 17}"
  export PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell"
  exec "${SZB_SPARK_PYTHON:?set SZB_SPARK_PYTHON to the Spark 4.1.2 oracle interpreter}" -I "$O/item1_probe.py" --engine spark --part "$PART" --warehouse "$W/wh-item1-spark-$PART" --out "$R/$OUT" --ivy-jar "${SZB_ICEBERG_JAR:?set SZB_ICEBERG_JAR to iceberg-spark-runtime-4.1_2.13-1.11.0.jar}" "$@"
fi
exec "$C/.venv/bin/python" -I "$O/item1_probe.py" --engine repark --part "$PART" --warehouse "$W/wh-item1-repark-$PART" --out "$R/$OUT" "$@"
