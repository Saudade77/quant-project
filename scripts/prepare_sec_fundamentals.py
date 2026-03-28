"""Build project-standard fundamentals from downloaded SEC source files."""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data.sec_fundamentals import prepare_sec_fundamentals_data


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare standard fundamentals from SEC raw source files.")
    parser.add_argument(
        "--mapping-path",
        default="data/raw/sec_source/company_tickers.json",
        help="Path to SEC ticker-to-CIK JSON.",
    )
    parser.add_argument(
        "--submissions-zip",
        default="data/raw/sec_source/submissions.zip",
        help="Path to SEC submissions ZIP.",
    )
    parser.add_argument(
        "--companyfacts-zip",
        default="data/raw/sec_source/companyfacts.zip",
        help="Path to SEC companyfacts ZIP.",
    )
    return parser


def main() -> None:
    """Build the project's standard fundamentals parquet from SEC data."""
    parser = _build_parser()
    args = parser.parse_args()

    written_paths = prepare_sec_fundamentals_data(
        mapping_path=args.mapping_path,
        submissions_zip_path=args.submissions_zip,
        companyfacts_zip_path=args.companyfacts_zip,
    )

    print("Prepared SEC fundamentals.")
    for key, value in written_paths.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
