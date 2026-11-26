from pyspark.sql import functions as F
from masar.spark import get_spark

spark = get_spark("make-riders")
trips = spark.read.option("header", True).csv("data/raw/trips_2026-06-01.csv")

riders = (trips.select("rider_id", F.col("city").alias("home_city"))
          .dropDuplicates(["rider_id"])
          .withColumn("signup_date",
                      F.date_sub(F.lit("2026-06-01").cast("date"), (F.rand(42) * 900).cast("int")))
          .withColumn("is_premium", F.rand(7) < 0.15))

riders.coalesce(1).write.mode("overwrite").parquet("data/raw/riders")