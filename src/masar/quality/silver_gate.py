"""Quality gate on silver/trips (T7). Exit 1 if any check fails, so the pipeline stops.

Run:  python -m masar.quality.silver_gate
"""
import sys

from masar.quality.contract import INTEGRITY, load, validate


def main() -> int:
    df = load("silver")
    print(f"silver/trips rows: {df.count():,}")
    results = validate(df)
    for name, r in results.items():
        print(f"  {name:18} passed={r['passed']!s:5} bad_rows={r['bad_rows']}")
    broken = [n for n in INTEGRITY if not results[n]["passed"]]
    if broken:
        print(f"FAIL-FAST: integrity checks failed {broken}")
        return 1
    failed = [n for n, r in results.items() if not r["passed"]]
    if failed:
        print(f"quality checks failed: {failed}")
        return 1
    print("quality gate: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
