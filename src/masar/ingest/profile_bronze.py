from delta.tables import DeltaTable
from pyspark.sql import functions as F

from masar.spark import get_spark

BRONZE = "./lakehouse/bronze"


def profile(table_path: str, key: str | None = None) -> None:
    spark = get_spark("profile")
    df = spark.read.format("delta").load(table_path)
    n = df.count()
    print(f"\n=== {table_path} ===")
    print(f"rows={n:,}  columns={len(df.columns)}")
    df.printSchema()
    df.select([
        (F.sum(F.col(c).isNull().cast("int")) / F.lit(n)).alias(c)
        for c in df.columns
    ]).show(truncate=False, vertical=True)
    if key and key in df.columns:
        d = df.groupBy(key).count().filter("count > 1").count()
        print(f"duplicate {key} groups: {d:,}")


def check_orphans() -> None:
    """rider_id values in trips that have no row in the riders table."""
    spark = get_spark("orphans")
    trips = spark.read.format("delta").load(f"{BRONZE}/trips")
    riders = spark.read.format("delta").load(f"{BRONZE}/riders")
    o = (trips.join(riders, "rider_id", "left_anti")
              .select("rider_id").distinct().count())
    print(f"\norphan rider_id in trips: {o:,}")


if __name__ == "__main__":
    spark = get_spark("profile")
    profile(f"{BRONZE}/trips", key="trip_id")
    if DeltaTable.isDeltaTable(spark, f"{BRONZE}/riders"):
        profile(f"{BRONZE}/riders", key="rider_id")
        check_orphans()
    else:
        print("\nriders table not landed yet -> run land_bronze first")