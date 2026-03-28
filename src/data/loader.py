"""Load market, fundamentals, and benchmark data into standard tables."""

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import yaml

from src.data.cleaner import (
    BENCHMARK_REQUIRED_COLUMNS,
    FUNDAMENTALS_REQUIRED_COLUMNS,
    PRICE_REQUIRED_COLUMNS,
    clean_benchmark_data,
    clean_fundamentals_data,
    clean_price_data,
)


@dataclass
class DataBundle:
    """Container for the major datasets used in research and backtesting."""

    prices: pd.DataFrame = field(default_factory=pd.DataFrame)
    fundamentals: pd.DataFrame = field(default_factory=pd.DataFrame)
    benchmark: pd.DataFrame = field(default_factory=pd.DataFrame)


def _empty_table(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _read_yaml(path: Path) -> dict:
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as handle:
        content = yaml.safe_load(handle) or {}

    if not isinstance(content, dict):
        raise ValueError(f"Expected a mapping in config file: {path}")

    return content


def _resolve_path(project_root: Path, configured_path: str | None) -> Path | None:
    if not configured_path:
        return None

    path = Path(configured_path)
    if path.is_absolute():
        return path

    return project_root / path


def _read_table(path: Path | None, empty_columns: list[str]) -> pd.DataFrame:
    if path is None or not path.exists():
        return _empty_table(empty_columns)

    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)

    if path.suffix.lower() in {".parquet", ".pq"}:
        return pd.read_parquet(path)

    raise ValueError(f"Unsupported data file format: {path}")


def _filter_by_date_range(data: pd.DataFrame, start_date: str | None, end_date: str | None) -> pd.DataFrame:
    if data.empty or "date" not in data.columns:
        return data

    filtered = data.copy()
    if start_date:
        filtered = filtered[filtered["date"] >= pd.Timestamp(start_date)]
    if end_date:
        filtered = filtered[filtered["date"] <= pd.Timestamp(end_date)]

    return filtered.reset_index(drop=True)


def load_market_data(
    config_path: str = "config/default.yaml",
    paths_path: str = "config/paths.yaml",
) -> DataBundle:
    """Load local US market data.

    The loader reads local CSV/Parquet files defined in config, applies
    lightweight schema normalization, and clips datasets to the configured
    date range. Missing files are tolerated and result in empty tables so
    that early development can proceed without a full dataset.
    """
    project_root = Path(__file__).resolve().parents[2]
    config = _read_yaml(project_root / config_path)
    path_config = _read_yaml(project_root / paths_path).get("paths", {})

    start_date = config.get("start_date")
    end_date = config.get("end_date")

    prices_raw = _read_table(
        _resolve_path(project_root, path_config.get("market_data")),
        PRICE_REQUIRED_COLUMNS,
    )
    fundamentals_raw = _read_table(
        _resolve_path(project_root, path_config.get("fundamentals_data")),
        FUNDAMENTALS_REQUIRED_COLUMNS,
    )
    benchmark_raw = _read_table(
        _resolve_path(project_root, path_config.get("benchmark_data")),
        BENCHMARK_REQUIRED_COLUMNS,
    )

    prices = _filter_by_date_range(clean_price_data(prices_raw), start_date, end_date)
    fundamentals = _filter_by_date_range(clean_fundamentals_data(fundamentals_raw), start_date, end_date)
    benchmark = _filter_by_date_range(clean_benchmark_data(benchmark_raw), start_date, end_date)

    return DataBundle(
        prices=prices,
        fundamentals=fundamentals,
        benchmark=benchmark,
    )
