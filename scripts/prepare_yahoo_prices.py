"""Download Yahoo Finance prices and write project-standard price tables."""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import yaml

from src.data.yahoo import build_benchmark_from_prices, download_yahoo_prices, read_tickers_file


def _load_paths_config(project_root: Path) -> dict:
    config_path = project_root / "config" / "paths.yaml"
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)["paths"]


def _load_default_config(project_root: Path) -> dict:
    config_path = project_root / "config" / "default.yaml"
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def prepare_yahoo_price_data(
    tickers: list[str],
    start_date: str,
    end_date: str,
    benchmark_ticker: str = "SPY",
    project_root: Path | None = None,
) -> dict[str, Path]:
    """Download Yahoo prices and write project-standard prices and benchmark tables."""
    project_root = project_root or PROJECT_ROOT
    paths_config = _load_paths_config(project_root)

    market_data_path = project_root / paths_config["market_data"]
    benchmark_data_path = project_root / paths_config["benchmark_data"]

    for path in [market_data_path, benchmark_data_path]:
        path.parent.mkdir(parents=True, exist_ok=True)

    requested_tickers = sorted({str(ticker).upper().strip() for ticker in tickers if str(ticker).strip()} | {benchmark_ticker.upper()})
    prices = download_yahoo_prices(requested_tickers, start_date=start_date, end_date=end_date)
    benchmark = build_benchmark_from_prices(prices, ticker=benchmark_ticker)

    prices.to_parquet(market_data_path, index=False)
    benchmark.to_parquet(benchmark_data_path, index=False)

    return {
        "market_data": market_data_path,
        "benchmark_data": benchmark_data_path,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare project price data from Yahoo Finance.")
    parser.add_argument("--tickers", nargs="*", default=None, help="Ticker symbols to download.")
    parser.add_argument("--tickers-file", default=None, help="Optional text file with one ticker per line.")
    parser.add_argument("--start-date", default=None, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end-date", default=None, help="End date in YYYY-MM-DD format.")
    parser.add_argument("--benchmark-ticker", default="SPY", help="Benchmark ticker to download and extract.")
    return parser


def main() -> None:
    """Download Yahoo Finance prices into the project's standard tables."""
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

    written_paths = prepare_yahoo_price_data(
        tickers=tickers,
        start_date=args.start_date or config["start_date"],
        end_date=args.end_date or config["end_date"],
        benchmark_ticker=args.benchmark_ticker,
    )

    print("Prepared Yahoo price data.")
    for key, value in written_paths.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
