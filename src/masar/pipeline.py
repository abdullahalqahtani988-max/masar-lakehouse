"""Capstone step 6: run the whole pipeline with `make pipeline`.

Each step runs in its own Python process, so only ONE Spark session (and one
lock on the local Derby metastore) is alive at a time. The first failing step
stops the pipeline with exit code 1 (fail-fast), so nothing downstream is built
on bad data.
"""
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PY = sys.executable
# use the dbt that lives next to this Python (.venv/bin/dbt), else the one on PATH
DBT = str(Path(PY).parent / "dbt") if (Path(PY).parent / "dbt").exists() else "dbt"
DBT_DIR = str(ROOT / "dbt" / "masar")

STEPS = [
    # (name, command)
    ("1 land bronze",          [PY, "-m", "masar.ingest.land_bronze"]),
    ("2 register bronze",      [PY, "-m", "masar.register"]),
    ("3 dbt build silver",     [DBT, "build", "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR]),
    ("4 quality gate",         [PY, "-m", "masar.quality.silver_gate"]),
    ("5 gold marts",           [PY, "-c", "from masar.transform.build_gold import "
                                          "build_zone_hourly_demand, build_trip_features; "
                                          "build_zone_hourly_demand(); build_trip_features()"]),
    ("6a consumer: BI-safe",   [PY, "-m", "masar.serve.bi_view"]),
    ("6b consumer: training",  [PY, "-m", "masar.serve.training_join"]),
    ("7 counts per layer",     [PY, "-m", "masar.proof", "counts"]),
]


def run(name, cmd):
    print(f"\n=== [{name}] " + " ".join(Path(c).name if i == 0 else c for i, c in enumerate(cmd)), flush=True)
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src"), "MASAR_HOME": str(ROOT)}
    start = time.time()
    if subprocess.run(cmd, cwd=ROOT, env=env).returncode != 0:
        print(f"=== [{name}] FAILED: pipeline halted (fail-fast)")
        sys.exit(1)
    print(f"=== [{name}] OK ({time.time() - start:.0f}s)")


def main():
    for name, cmd in STEPS:
        run(name, cmd)
    print("\nPIPELINE OK")


if __name__ == "__main__":
    main()
