"""Create a dirty trips file for a new day with KNOWN injected problems."""
import csv

SRC = "data/raw/trips_2026-06-01.csv"
OUT = "data/raw/trips_2026-06-02.csv"

with open(SRC, newline="") as f:
    reader = csv.DictReader(f)
    cols = reader.fieldnames
    rows = [dict(r) for _, r in zip(range(1000), reader)]

for i, r in enumerate(rows):
    r["trip_id"] = f"T-1{i:05d}"                  # new ids for the new file
    if i < 30:
        r["rider_id"] = ""                         # 30 missing rider_id
    elif i < 50:
        r["fare_sar"] = str(-abs(float(r["fare_sar"])))   # 20 negative fares
    elif i < 60:
        r["fare_sar"] = "abc"                      # 10 non-numeric fares
    elif i < 70:
        r["fare_sar"] = "99999"                    # 10 extreme fares
    elif i < 90:
        r["start_ts"], r["end_ts"] = r["end_ts"], r["start_ts"]   # 20 trips ending before they start
    elif i < 105:
        r["rider_id"] = f"rider_9{i:05d}"          # 15 riders not in riders table
    elif i < 115:
        r["surge_multiplier"] = "0.5"              # 10 invalid surge (< 1)
    elif i < 125:
        r["city"] = " riyadh"                      # 10 dirty city text

dups = [dict(r) for r in rows[200:225]]            # 25 duplicated trip_ids

with open(OUT, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=cols)
    w.writeheader()
    w.writerows(rows + dups)
    for k in range(5):
        f.write(f"T-BROKEN-{k},D-0001,rider_00001\n")   # 5 malformed lines (missing columns)

print(f"wrote {len(rows) + len(dups) + 5} rows to {OUT}")