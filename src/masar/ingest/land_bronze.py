from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F

from masar.spark import get_spark

BRONZE = "./lakehouse/bronze"
def land_riders(source_glob: str) -> None:
    spark = get_spark("land-riders")
    raw = spark.read.parquet(source_glob)   # Parquet carries its own schema
    _append_new_files(spark, raw, f"{BRONZE}/riders")

def _lineage(df: DataFrame) -> DataFrame:
    return (df.withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.input_file_name()))


def _append_new_files(spark, raw: DataFrame, target: str) -> None:
    """Idempotent append: skip rows whose source file is already in the target table."""
    df = _lineage(raw)
    cols = df.columns
    if DeltaTable.isDeltaTable(spark, target):
        loaded = (spark.read.format("delta").load(target)
                  .select("_source_file").distinct())
        df = df.join(loaded, "_source_file", "left_anti").select(*cols)
    df.write.format("delta").mode("append").save(target)


def land_trips(source_glob: str) -> None:
    spark = get_spark("land-trips")
    raw = (spark.read
           .option("header", True)
           .option("mode", "PERMISSIVE")   # keep malformed rows, do not drop
           .csv(source_glob))
    _append_new_files(spark, raw, f"{BRONZE}/trips")


def land_gps(source_glob: str) -> None:
    spark = get_spark("land-gps")
    raw = spark.read.json(source_glob)   # schema-on-read for nested JSON
    _append_new_files(spark, raw, f"{BRONZE}/gps_pings")


def land_drivers(source_glob: str) -> None:
    spark = get_spark("land-drivers")
    raw = (spark.read
           .option("header", True)
           .option("mode", "PERMISSIVE")   # keep malformed rows, do not drop
           .csv(source_glob))
    _append_new_files(spark, raw, f"{BRONZE}/drivers")


if __name__ == "__main__":
    land_trips("data/raw/trips_2026-*.csv")
    land_gps("data/raw/gps/*.ndjson")
    land_drivers("data/raw/drivers.csv")
    land_riders("data/raw/riders/*.parquet")
    print("landed")
