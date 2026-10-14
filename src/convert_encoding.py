
from pathlib import Path

source = Path("data/raw/questions1.csv")
destination = Path("data/raw/questions1_utf8.csv")

text = source.read_text(encoding="cp1252")
destination.write_text(text, encoding="utf-8", newline="")

print(f"Converted: {source} -> {destination}")
print(f"Output size: {destination.stat().st_size:,} bytes")
