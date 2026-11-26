"""Capstone step 7 (T5): prove the lakehouse.

  python -m masar.proof counts       row counts per layer + PII / leakage checks
  python -m masar.proof timetravel   silver/trips history, version 0 vs latest count
  python -m masar.proof              both
"""
import sys
from pathlib import Path

from delta.tables import DeltaTable

from masar.config import BRONZE, GOLD, SILVER
from masar.quality.pii import PII_COLUMNS
from masar.spark import get_spark

LAYERS = [
    ("bronze", "trips",              f"{BRONZE}/trips"),
    ("bronze", "drivers",            f"{BRONZE}/drivers"),
    ("bronze", "gps_pings",          f"{BRONZE}/gps_pings"),
    ("silver", "trips",              SILVER),
    ("gold",   "zone_hourly_demand", f"{GOLD}/zone_hourly_demand"),
    ("gold",   "trip_features",      f"{GOLD}/trip_features"),
    ("gold",   "bi_trips_safe",      f"{GOLD}/bi_trips_safe"),
    ("gold",   "training_set",       f"{GOLD}/training_set"),
]
# columns that are computed from the trip itself or from the same hour: they leak the label
LEAKY = {"fare_per_km", "demand_same_hour", "ended_at", "duration_min"}


def counts(spark) -> None:
    print(f"{'layer':7} {'table':20} {'rows':>9}")
    for layer, name, path in LAYERS:
        if (Path(path) / "_delta_log").exists():
            n = f"{spark.read.format('delta').load(path).count():,}"
        else:
            n = "missing"
        print(f"{layer:7} {name:20} {n:>9}")

    bi = f"{GOLD}/bi_trips_safe"
    if (Path(bi) / "_delta_log").exists():
        pii = set(PII_COLUMNS["trips"]) & set(spark.read.format("delta").load(bi).columns)
        print(f"PII check bi_trips_safe: {'PASS' if not pii else 'FAIL ' + str(sorted(pii))}")
    ts = f"{GOLD}/training_set"
    if (Path(ts) / "_delta_log").exists():
        leaks = LEAKY & set(spark.read.format("delta").load(ts).columns)
        print(f"Leakage check training_set: {'PASS' if not leaks else 'FAIL ' + str(sorted(leaks))}"
              "  (features use the prior hour only; label = duration_min)")


def timetravel(spark) -> None:
    dt = DeltaTable.forPath(spark, SILVER)
    dt.history().select("version", "timestamp", "operation").orderBy("version").show(truncate=False)
    latest = dt.history(1).select("version").first()[0]
    v0 = spark.read.format("delta").option("versionAsOf", 0).load(SILVER).count()
    now = spark.read.format("delta").load(SILVER).count()
    print(f"silver/trips versions: {latest + 1}")
    print(f"version 0 count: {v0:,}   latest (v{latest}) count: {now:,}   differs: {v0 != now}")
    print(f"SQL: SELECT COUNT(*) FROM delta.`{SILVER}` VERSION AS OF 0")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    spark = get_spark("proof")
    if mode in ("counts", "all"):
        counts(spark)
    if mode in ("timetravel", "all"):
        timetravel(spark)
