# Masar capstone: submission

Name: Abdullah Mohammed Al-Qahtani
Branch: main
Link: <https://github.com/abdullahalqahtani988-max/masar-lakehouse/tree/main>

How to submit: `git add -A && git commit -m "Capstone" && git push`. You push to your own
fork (GitHub creates it on your first push). Send the link above to your trainer. If you
never pushed, send this file and the output of `python -m masar.proof` instead.

## What I built (one line per step)
1. Bronze: `masar.ingest.land_bronze` lands the raw CSV / NDJSON / Parquet files as Delta tables with no cleaning, and adds `_ingested_at` and `_source_file`. It only appends files that are not already in the table, so running it twice does not duplicate rows.
2. Silver (dbt): `dbt build` runs the staging models (casting and trimming) and the incremental `silver_trips` model (MERGE on `trip_id`, completed trips only, joined with drivers). It also runs 9 tests (unique, not_null, accepted range, accepted values, relationships), and all 9 pass. Then `masar.quality.silver_gate` runs the Great Expectations checks; if a check fails it exits with code 1 and the pipeline stops.
3. Delta control: `masar.delta.capstone_controls` adds `CHECK (fare_sar > 0)` as `positive_fare`. A test row with `fare_sar = -5.0` is rejected with `DELTA_VIOLATE_CONSTRAINT_WITH_VALUES`, and the row count stays at 44,371. Schema change: `ALTER TABLE ... ADD COLUMNS (promo_code STRING)`, which adds a nullable column. Old rows read it as NULL, and nothing is rewritten.
4. Gold: `zone_hourly_demand` has one row per (city, date, hour), giving 120 rows, partitioned by date. `trip_features` has one row per trip (44,371 rows) and uses only the previous hour of demand (point-in-time).
5. BI-safe table and training set: `bi_trips_safe` replaces `rider_id` and `driver_id` with salted SHA-256 pseudonyms and cuts `dropoff_geohash` down to 4 characters. `training_set` joins `trip_features` to the label `duration_min`. PII check: PASS. Leakage check: PASS (it has no `fare_per_km`, no same-hour demand, and no `ended_at`).
6. Pipeline (`make pipeline`): `src/masar/pipeline.py` runs 8 steps in order: land bronze, register, dbt build, quality gate, gold, BI-safe, training set, counts. Each step runs as its own process, and the first failure stops the run (exit 1). From a clean state (`make clean-lakehouse`) it finished with `PIPELINE OK` in about 3.5 minutes.
7. Proof (counts and time travel): `python -m masar.proof` prints the row count of every layer, runs the PII and leakage checks, shows the silver history, and compares version 0 with the latest version.

## Counts per layer
Taken after `make clean-lakehouse` and `make pipeline`, then step 3, then the Day 2 erasure (`python -m masar.delta.merge_ops erase`):

```
layer   table                     rows
bronze  trips                   48,200
bronze  drivers                  1,850
bronze  gps_pings              120,000
silver  trips                   44,364   (44,371 right after the pipeline; 7 rows of rider_00042 erased)
gold    zone_hourly_demand         120
gold    trip_features           44,371
gold    bi_trips_safe           44,371
gold    training_set            44,371
PII check bi_trips_safe: PASS
Leakage check training_set: PASS
```

Quality gate (silver, 44,371 rows): trip_id_not_null, trip_id_unique, fare_is_double,
fare_positive, city_known, duration_sane and fare_mean_sane all passed with bad_rows=0.

## Time travel
```
version | operation
0       | CREATE OR REPLACE TABLE AS SELECT   (dbt build)
1       | DROP CONSTRAINT                     (capstone_controls is safe to re-run)
2       | ADD CONSTRAINT  positive_fare
3       | ADD COLUMNS     promo_code
4       | DELETE          erase rider_00042 (right to be forgotten)

SELECT COUNT(*) FROM delta.`lakehouse/silver/trips` VERSION AS OF 0   -> 44,371
SELECT COUNT(*) FROM delta.`lakehouse/silver/trips`                   -> 44,364 (v4)
```
The older version's count is different from the latest, so time travel works. Note that the erased rows can still be read in v0 until VACUUM deletes the old files.

## Steps the trainer handed over (if any)
None.

## Layers in my own words
- Bronze is a raw copy of the source data plus lineage columns. If something goes wrong later, I can rebuild everything from here.
- Silver holds clean, typed data with one row per trip. Tests and a quality gate protect it, so other tables can trust it.
- Gold holds tables built for one use each: a demand mart for dashboards, features for ML, a BI-safe view without PII, and a training set without leakage.

## One design choice
Each pipeline step runs in its own Python process instead of running every step in one script. Spark's local Derby metastore allows only one Spark session at a time, so separate processes avoid lock errors. It also means a failing step returns a non-zero exit code and the whole run stops (fail-fast), so gold is never built on top of a silver table that failed the quality gate.

## One thing I would improve
The pipeline builds bronze again by merging, and that brings back the trips I erased. The quarantine write also adds duplicate rows each time it runs (not idempotent). I would keep a list of erased riders and filter them out in the silver model, and make the quarantine write a MERGE on `trip_id`. I would also add the constraint and the schema change to the pipeline itself, so a clean run includes them.
