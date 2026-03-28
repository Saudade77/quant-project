"""Basic cleaning utilities for US market and fundamentals data."""

import pandas as pd

PRICE_REQUIRED_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume"]
FUNDAMENTALS_REQUIRED_COLUMNS = ["date", "ticker", "pe", "pb", "roe"]
BENCHMARK_REQUIRED_COLUMNS = ["date", "ticker", "close"]

PRICE_COLUMN_ALIASES = {
    "datetime": "date",
    "symbol": "ticker",
    "adj_close": "close",
    "adjusted_close": "close",
}

FUNDAMENTALS_COLUMN_ALIASES = {
    "datetime": "date",
    "symbol": "ticker",
}

BENCHMARK_COLUMN_ALIASES = {
    "datetime": "date",
    "symbol": "ticker",
}


def _normalize_columns(data: pd.DataFrame, aliases: dict[str, str]) -> pd.DataFrame:
    cleaned = data.copy()
    cleaned.columns = [str(column).strip().lower() for column in cleaned.columns]
    return cleaned.rename(columns=aliases)


def _ensure_columns(data: pd.DataFrame, required_columns: list[str]) -> pd.DataFrame:
    cleaned = data.copy()
    for column in required_columns:
        if column not in cleaned.columns:
            cleaned[column] = pd.NA
    return cleaned[required_columns]


def _sort_and_deduplicate(data: pd.DataFrame) -> pd.DataFrame:
    cleaned = data.copy()
    if {"date", "ticker"}.issubset(cleaned.columns):
        cleaned = cleaned.sort_values(["date", "ticker"]).drop_duplicates(subset=["date", "ticker"], keep="last")
    elif "date" in cleaned.columns:
        cleaned = cleaned.sort_values(["date"]).drop_duplicates(subset=["date"], keep="last")
    return cleaned.reset_index(drop=True)


def clean_price_data(price_data: pd.DataFrame) -> pd.DataFrame:
    """Normalize and clean US equity OHLCV data."""
    cleaned = _normalize_columns(price_data, PRICE_COLUMN_ALIASES)
    cleaned = _ensure_columns(cleaned, PRICE_REQUIRED_COLUMNS)
    cleaned["date"] = pd.to_datetime(cleaned["date"], errors="coerce")
    cleaned["ticker"] = cleaned["ticker"].astype("string").str.upper().str.strip()

    numeric_columns = ["open", "high", "low", "close", "volume"]
    for column in numeric_columns:
        cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")

    cleaned = cleaned.dropna(subset=["date", "ticker", "close"])
    cleaned = cleaned[cleaned["close"] > 0]
    return _sort_and_deduplicate(cleaned)


def clean_fundamentals_data(fundamentals_data: pd.DataFrame) -> pd.DataFrame:
    """Normalize and clean fundamentals data used by valuation and quality factors."""
    cleaned = _normalize_columns(fundamentals_data, FUNDAMENTALS_COLUMN_ALIASES)
    cleaned = _ensure_columns(cleaned, FUNDAMENTALS_REQUIRED_COLUMNS)
    cleaned["date"] = pd.to_datetime(cleaned["date"], errors="coerce")
    cleaned["ticker"] = cleaned["ticker"].astype("string").str.upper().str.strip()

    for column in ["pe", "pb", "roe"]:
        cleaned[column] = pd.to_numeric(cleaned[column], errors="coerce")

    cleaned = cleaned.dropna(subset=["date", "ticker"])
    return _sort_and_deduplicate(cleaned)


def clean_benchmark_data(benchmark_data: pd.DataFrame) -> pd.DataFrame:
    """Normalize and clean benchmark price data."""
    cleaned = _normalize_columns(benchmark_data, BENCHMARK_COLUMN_ALIASES)
    cleaned = _ensure_columns(cleaned, BENCHMARK_REQUIRED_COLUMNS)
    cleaned["date"] = pd.to_datetime(cleaned["date"], errors="coerce")
    cleaned["ticker"] = cleaned["ticker"].astype("string").str.upper().str.strip()
    cleaned["close"] = pd.to_numeric(cleaned["close"], errors="coerce")
    cleaned = cleaned.dropna(subset=["date", "ticker", "close"])
    cleaned = cleaned[cleaned["close"] > 0]
    return _sort_and_deduplicate(cleaned)
