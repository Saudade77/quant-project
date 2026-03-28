"""Backtest entrypoint for the strategy workflow."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.run_pipeline import run_pipeline


def main() -> None:
    """Run the backtest portion of the full research pipeline."""
    result = run_pipeline()

    print("Backtest executed successfully.")
    print(f"Input data source: {result['input_data_source']}")
    print(f"Backtest rows: {len(result['backtest_result'])}")
    print(result["performance"])
    print(f"Report: {result['report_path']}")


if __name__ == "__main__":
    main()
