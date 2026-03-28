"""Generate deterministic sample US equity data for local development."""

from pathlib import Path

import numpy as np
import pandas as pd
import yaml


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_paths_config(project_root: Path) -> dict:
    config_path = project_root / "config" / "paths.yaml"
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)["paths"]


def generate_sample_data(project_root: Path | None = None) -> dict[str, Path]:
    """Generate sample US price, fundamentals, and benchmark data."""
    project_root = project_root or _project_root()
    paths_config = _load_paths_config(project_root)

    market_data_path = project_root / paths_config["market_data"]
    fundamentals_data_path = project_root / paths_config["fundamentals_data"]
    benchmark_data_path = project_root / paths_config["benchmark_data"]

    for path in [market_data_path, fundamentals_data_path, benchmark_data_path]:
        path.parent.mkdir(parents=True, exist_ok=True)

    tickers = ["AAPL", "MSFT", "NVDA", "AMZN"]
    dates = pd.bdate_range("2024-01-02", "2024-06-28")

    price_rows: list[dict[str, float | int | pd.Timestamp | str]] = []
    fundamentals_rows: list[dict[str, float | pd.Timestamp | str]] = []
    benchmark_rows: list[dict[str, float | pd.Timestamp | str]] = []

    base_prices = {"AAPL": 180.0, "MSFT": 380.0, "NVDA": 500.0, "AMZN": 150.0}
    daily_drifts = {"AAPL": 0.0010, "MSFT": 0.0012, "NVDA": 0.0018, "AMZN": 0.0009}
    price_phases = {"AAPL": 0.1, "MSFT": 0.7, "NVDA": 1.3, "AMZN": 2.0}
    pe_bases = {"AAPL": 28.0, "MSFT": 32.0, "NVDA": 45.0, "AMZN": 55.0}
    pb_bases = {"AAPL": 35.0, "MSFT": 12.0, "NVDA": 30.0, "AMZN": 8.0}
    roe_bases = {"AAPL": 0.28, "MSFT": 0.32, "NVDA": 0.35, "AMZN": 0.18}

    for index, date in enumerate(dates):
        benchmark_close = 470.0 * (1.0 + 0.0007) ** index * (1.0 + 0.01 * np.sin(index / 7))
        benchmark_rows.append({"date": date, "ticker": "SPY", "close": round(float(benchmark_close), 4)})

        for ticker in tickers:
            phase = price_phases[ticker]
            close = base_prices[ticker] * (1.0 + daily_drifts[ticker]) ** index * (1.0 + 0.015 * np.sin(index / 6 + phase))
            open_price = close * (1.0 + 0.002 * np.cos(index / 8 + phase))
            high = max(open_price, close) * 1.004
            low = min(open_price, close) * 0.996
            volume = int(1_000_000 + 80_000 * index + 50_000 * np.sin(index / 5 + phase))

            price_rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "open": round(float(open_price), 4),
                    "high": round(float(high), 4),
                    "low": round(float(low), 4),
                    "close": round(float(close), 4),
                    "volume": volume,
                }
            )

            fundamentals_rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "pe": round(float(pe_bases[ticker] + 1.5 * np.sin(index / 18 + phase)), 4),
                    "pb": round(float(pb_bases[ticker] + 0.8 * np.cos(index / 16 + phase)), 4),
                    "roe": round(float(roe_bases[ticker] + 0.01 * np.sin(index / 20 + phase)), 4),
                }
            )

    pd.DataFrame(price_rows).to_parquet(market_data_path, index=False)
    pd.DataFrame(fundamentals_rows).to_parquet(fundamentals_data_path, index=False)
    pd.DataFrame(benchmark_rows).to_parquet(benchmark_data_path, index=False)

    return {
        "market_data": market_data_path,
        "fundamentals_data": fundamentals_data_path,
        "benchmark_data": benchmark_data_path,
    }


def main() -> None:
    """Generate sample input files that allow the whole pipeline to run."""
    generated_paths = generate_sample_data()
    print("Sample data generated.")
    for key, value in generated_paths.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
