from masar.spark import get_spark

CONSTRAINTS = {
    "positive_fare":  "fare_sar > 0",
    "valid_duration": "ended_at > started_at",
    "valid_status":   "status = 'completed'",
    "valid_city":     "city IN ('Riyadh', 'Jeddah', 'Dammam', 'Mecca', 'Medina')",
}


def add_constraints() -> None:
    spark = get_spark("enforce-silver")
    for name, rule in CONSTRAINTS.items():
        spark.sql(f"ALTER TABLE silver.trips DROP CONSTRAINT IF EXISTS {name}")
        spark.sql(f"ALTER TABLE silver.trips ADD CONSTRAINT {name} CHECK ({rule})")
        print(f"added {name}: {rule}")
    spark.sql("SHOW TBLPROPERTIES silver.trips").show(truncate=False)


if __name__ == "__main__":
    add_constraints()