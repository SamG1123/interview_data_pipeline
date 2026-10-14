
import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "data/raw/questions1_utf8.csv"
OUTPUT = ROOT / "data/sample/questions_sample.csv"

with SOURCE.open("r", encoding="utf-8-sig", newline="") as source:
    reader = csv.DictReader(source)
    rows = list(reader)
    fieldnames = reader.fieldnames

sample = rows[:100]

with OUTPUT.open("w", encoding="utf-8", newline="") as target:
    writer = csv.DictWriter(target, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(sample)

print(f"Source rows: {len(rows)}")
print(f"Sample rows: {len(sample)}")
print(f"Saved to: {OUTPUT}")
