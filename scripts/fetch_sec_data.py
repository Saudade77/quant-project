"""Download free SEC raw source files used for fundamentals preparation."""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data.sec import (
    SEC_COMPANYFACTS_BULK_URL,
    SEC_SUBMISSIONS_BULK_URL,
    download_sec_bulk_archive,
    download_sec_company_tickers,
)


def fetch_sec_source_data(
    output_dir: str | Path = "data/raw/sec_source",
    user_agent: str | None = None,
    project_root: Path | None = None,
) -> dict[str, Path]:
    """Download the SEC free raw source files into the project workspace."""
    project_root = project_root or PROJECT_ROOT
    base_dir = project_root / output_dir
    base_dir.mkdir(parents=True, exist_ok=True)

    kwargs = {"user_agent": user_agent} if user_agent else {}
    tickers_path = download_sec_company_tickers(base_dir / "company_tickers.json", **kwargs)
    submissions_path = download_sec_bulk_archive(SEC_SUBMISSIONS_BULK_URL, base_dir / "submissions.zip", **kwargs)
    companyfacts_path = download_sec_bulk_archive(SEC_COMPANYFACTS_BULK_URL, base_dir / "companyfacts.zip", **kwargs)

    return {
        "company_tickers": tickers_path,
        "submissions": submissions_path,
        "companyfacts": companyfacts_path,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch free SEC EDGAR source files.")
    parser.add_argument(
        "--output-dir",
        default="data/raw/sec_source",
        help="Directory where SEC raw source files will be stored.",
    )
    parser.add_argument(
        "--user-agent",
        default=None,
        help="Optional SEC-compliant user-agent string, e.g. 'your-name your@email.com'.",
    )
    return parser


def main() -> None:
    """Download SEC raw source files."""
    parser = _build_parser()
    args = parser.parse_args()

    written_paths = fetch_sec_source_data(output_dir=args.output_dir, user_agent=args.user_agent)

    print("Fetched SEC source data.")
    for key, value in written_paths.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
