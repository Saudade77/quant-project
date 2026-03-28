"""Orchestrate free-source real data preparation for the research pipeline."""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import yaml

from scripts.fetch_sec_data import fetch_sec_source_data
from scripts.prepare_sec_fundamentals import prepare_sec_fundamentals_data
from scripts.prepare_yahoo_prices import prepare_yahoo_price_data
from src.data.yahoo import read_tickers_file


def _load_default_config(project_root: Path) -> dict:
    config_path = project_root / "config" / "default.yaml"
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def prepare_real_data(
    tickers: list[str],
    start_date: str,
    end_date: str,
    benchmark_ticker: str = "SPY",
    sec_user_agent: str | None = None,
    sec_output_dir: str = "data/raw/sec_source",
    project_root: Path | None = None,
) -> dict[str, Path]:
    """Prepare project-standard real data tables from free Yahoo and SEC sources."""
    project_root = project_root or PROJECT_ROOT

    sec_paths = fetch_sec_source_data(
        output_dir=sec_output_dir,
        user_agent=sec_user_agent,
        project_root=project_root,
    )
    price_paths = prepare_yahoo_price_data(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        benchmark_ticker=benchmark_ticker,
        project_root=project_root,
    )
    fundamentals_paths = prepare_sec_fundamentals_data(
        project_root=project_root,
        mapping_path=str(sec_paths["company_tickers"].relative_to(project_root)),
        submissions_zip_path=str(sec_paths["submissions"].relative_to(project_root)),
        companyfacts_zip_path=str(sec_paths["companyfacts"].relative_to(project_root)),
    )

    return {
        **price_paths,
        **fundamentals_paths,
        **sec_paths,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare free-source real data for the research pipeline.")
    parser.add_argument("--tickers", nargs="*", default=None, help="Ticker symbols to download.")
    parser.add_argument("--tickers-file", default=None, help="Optional text file with one ticker per line.")
    parser.add_argument("--start-date", default=None, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end-date", default=None, help="End date in YYYY-MM-DD format.")
    parser.add_argument("--benchmark-ticker", default="SPY", help="Benchmark ticker.")
    parser.add_argument(
        "--sec-user-agent",
        default=None,
        help="Optional SEC-compliant user-agent string, e.g. 'your-name your@email.com'.",
    )
    parser.add_argument(
        "--sec-output-dir",
        default="data/raw/sec_source",
        help="Directory where SEC raw source files will be stored.",
    )
    return parser


def main() -> None:
    """Prepare free-source real data into the project's standard tables."""
    parser = _build_parser()
    args = parser.parse_args()
    config = _load_default_config(PROJECT_ROOT)

    tickers: list[str] = []
    if args.tickers:
        tickers.extend(args.tickers)
    if args.tickers_file:
        tickers.extend(read_tickers_file(args.tickers_file))
    if not tickers:
        parser.error("Provide at least one ticker via --tickers or --tickers-file.")

    written_paths = prepare_real_data(
        tickers=tickers,
        start_date=args.start_date or config["start_date"],
        end_date=args.end_date or config["end_date"],
        benchmark_ticker=args.benchmark_ticker,
        sec_user_agent=args.sec_user_agent,
        sec_output_dir=args.sec_output_dir,
    )

    print("Prepared real project data.")
    for key, value in written_paths.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
