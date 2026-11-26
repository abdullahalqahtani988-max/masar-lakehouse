"""Capstone step 3 (T4): one Delta CHECK constraint + one safe schema change on silver/trips.

Run:  python -m masar.delta.capstone_controls
Safe to re-run: the constraint is dropped and re-added; the column is only added once.
"""
from pyspark.sql import functions as F

from masar.config import SILVER
from masar.spark import get_spark

TABLE = f"delta.`{SILVER}`"          # path-based: works without the metastore
CONSTRAINT = ("positive_fare", "fare_sar > 0")
NEW_COLUMN = ("promo_code", "STRING")


def main() -> None:
    spark = get_spark("capstone-controls")
    before = spark.read.format("delta").load(SILVER).count()

    # 1) constraint: Delta checks existing rows first, then every future write
    name, rule = CONSTRAINT
    spark.sql(f"ALTER TABLE {TABLE} DROP CONSTRAINT IF EXISTS {name}")
    spark.sql(f"ALTER TABLE {TABLE} ADD CONSTRAINT {name} CHECK ({rule})")
    print(f"constraint added: {name} CHECK ({rule})")

    # 2) prove it: try to append one trip with a negative fare
    bad = (spark.read.format("delta").load(SILVER).limit(1)
           .withColumn("trip_id", F.lit("BAD_FARE_TEST"))
           .withColumn("fare_sar", F.lit(-5.0)))
    try:
        bad.write.format("delta").mode("append").save(SILVER)
        print("UNEXPECTED: bad insert was accepted")
    except Exception as e:
        msg = str(e).splitlines()[0][:160]
        print(f"bad insert REJECTED as expected: {type(e).__name__}: {msg}")
    after_bad = spark.read.format("delta").load(SILVER).count()
    print(f"rows before={before:,} after bad insert={after_bad:,} (unchanged: {before == after_bad})")

    # 3) safe schema change: additive, nullable column; old rows read it as NULL
    col, typ = NEW_COLUMN
    if col not in spark.read.format("delta").load(SILVER).columns:
        spark.sql(f"ALTER TABLE {TABLE} ADD COLUMNS ({col} {typ} "
                  f"COMMENT 'promotion applied to the trip; NULL for older trips')")
        print(f"schema change: added nullable column {col} {typ}")
    df = spark.read.format("delta").load(SILVER)
    print(f"new column visible: {col in df.columns}; "
          f"non-null values: {df.where(F.col(col).isNotNull()).count()}")
    spark.sql(f"DESCRIBE DETAIL {TABLE}").select("numFiles", "properties").show(truncate=False)


if __name__ == "__main__":
    main()
