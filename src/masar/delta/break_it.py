import pandas as pd
from pyspark.sql import functions as F
from masar.config import SILVER
from masar.spark import get_spark

spark = get_spark("break-it")


def count():
    return spark.read.format("delta").load(SILVER).count()


print("before:", count())

# ---------- المحاولة 1: أسعار غلط (المتوقع: REJECTED) ----------
bad = spark.createDataFrame(pd.read_parquet("data/fixtures/dirty_batch_bad_fares.parquet"))
for f in spark.read.format("delta").load(SILVER).schema.fields:
    if f.name in bad.columns:
        bad = bad.withColumn(f.name, F.col(f.name).cast(f.dataType))

try:
    bad.write.format("delta").mode("append").save(SILVER)
    print("UNEXPECTED: write succeeded")
except Exception as e:
    print("REJECTED:", type(e).__name__, str(e)[:300])

print("after bad fares:", count())

# ---------- المحاولة 2: صف صحيح (المتوقع: ACCEPTED) ----------
good = spark.createDataFrame(
    [("T-GOOD-1", "Riyadh", 25.0, "completed", "2026-06-01 10:00:00", "2026-06-01 10:20:00")],
    ["trip_id", "city", "fare_sar", "status", "started_at", "ended_at"],
)
good = good.withColumn("started_at", F.to_timestamp("started_at")) \
           .withColumn("ended_at", F.to_timestamp("ended_at"))
for f in spark.read.format("delta").load(SILVER).schema.fields:
    if f.name in good.columns:
        good = good.withColumn(f.name, F.col(f.name).cast(f.dataType))

try:
    good.write.format("delta").mode("append").save(SILVER)
    print("ACCEPTED: valid row written")
except Exception as e:
    print("REJECTED:", type(e).__name__, str(e)[:300])

print("after good row:", count())

# ---------- تنظيف: نحذف الصف التجريبي عشان silver يرجع لحالته ----------
spark.sql("DELETE FROM silver.trips WHERE trip_id = 'T-GOOD-1'")
print("after cleanup:", count())