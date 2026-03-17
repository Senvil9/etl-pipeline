"""
generate_csv.py
---------------
Generates a fake users CSV that mirrors the reqres.in /users API shape.
Uses the Faker library so you can generate as many unique rows as you want.

Output: data/users.csv  (default)

Usage:
    python scripts/generate_csv.py                  # 100 rows
    python scripts/generate_csv.py --rows 500       # 500 rows
    python scripts/generate_csv.py --rows 50 --out data/users.csv
"""
from __future__ import annotations

import argparse
import csv
import random
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

AVATAR_BASE = "https://reqres.in/img/faces/{id}-image.jpg"


def generate_rows(n: int) -> list[dict]:
    rows = []
    used_ids: set[int] = set()

    for _ in range(n):
        # Generate a unique user_id
        uid = random.randint(1, 99999)
        while uid in used_ids:
            uid = random.randint(1, 99999)
        used_ids.add(uid)

        first = fake.first_name()
        last = fake.last_name()

        rows.append(
            {
                "user_id":    uid,
                "email":      fake.unique.email(),
                "first_name": first,
                "last_name":  last,
                "avatar":     AVATAR_BASE.format(id=uid),
            }
        )

    rows.sort(key=lambda r: r["user_id"])
    return rows


def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["user_id", "email", "first_name", "last_name", "avatar"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"✓ Wrote {len(rows)} rows to {path}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate fake users CSV")
    ap.add_argument("--rows", type=int, default=100,
                    help="Number of rows to generate (default: 100)")
    ap.add_argument("--out", type=str, default="data/users.csv",
                    help="Output file path (default: data/users.csv)")
    args = ap.parse_args()

    rows = generate_rows(args.rows)
    write_csv(rows, Path(args.out))


if __name__ == "__main__":
    main()