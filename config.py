from pathlib import Path

# Project directories
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
SAMPLE_DIR = DATA_DIR / "sample"
OUTPUT_DIR = BASE_DIR / "out"

# AWS configuration — placeholders for now
BUCKET = "replace-with-your-unique-bucket-name"
REGION = "ap-south-1"
