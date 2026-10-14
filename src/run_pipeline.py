
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from transform import run


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "local"

    if mode != "local":
        sys.exit("Usage: python src/run_pipeline.py local")

    input_path = ROOT / "data/sample/questions_sample.csv"
    output_dir = ROOT / "out/local/curated"
    rejected_dir = ROOT / "out/local/rejected"

    if not input_path.exists():
        sys.exit(f"Sample dataset not found: {input_path}")

    print("Starting local interview-question pipeline...")
    print(f"Input: {input_path}")

    summary = run(
        str(input_path),
        str(output_dir),
        str(rejected_dir),
    )

    print("\nLocal pipeline completed successfully.")
    print(f"Curated output: {output_dir}")
    print(f"Rejected output: {rejected_dir}")
    print(f"Input rows: {summary['rows_in']}")
    print(f"Rejected rows: {summary['rows_rejected']}")
    print(f"Rows after deduplication: {summary['rows_after_dedupe']}")


if __name__ == "__main__":
    main()
