"""Utilities for downloading and normalizing Yahoo Finance price data."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.data.cleaner import clean_benchmark_data, clean_price_data


def _import_yfinance():
    try:
        import yfinance as yf
    except ImportError as error:
        raise ImportError(
            "yfinance is required for Yahoo price downloads. Install dependencies with `pip install -r requirements.txt`."
        ) from error
    return yf


def _normalize_downloaded_prices(downloaded: pd.DataFrame) -> pd.DataFrame:
    if downloaded.empty:
        return pd.DataFrame(columns=["date", "ticker", "open", "high", "low", "close", "volume"])

    frame = downloaded.copy()
    if isinstance(frame.columns, pd.MultiIndex):
        frame = frame.stack(level=1, future_stack=True).reset_index()
        frame.columns = [str(column).strip().lower().replace(" ", "_") for column in frame.columns]
        frame = frame.rename(columns={"level_1": "ticker"})
    else:
        frame = frame.reset_index()
        frame.columns = [str(column).strip().lower().replace(" ", "_") for column in frame.columns]
        frame["ticker"] = pd.NA

    rename_map = {
        "date": "date",
        "datetime": "date",
        "ticker": "ticker",
        "open": "open",
        "high": "high",
        "low": "low",
        "volume": "volume",
    }
    frame = frame.rename(columns=rename_map)

    if "adj_close" in frame.columns:
        frame = frame.drop(columns=["close"], errors="ignore").rename(columns={"adj_close": "close"})
    elif "adj_close_" in frame.columns:
        frame = frame.drop(columns=["close"], errors="ignore").rename(columns={"adj_close_": "close"})
    elif "close" not in frame.columns:
        frame["close"] = pd.NA

    if "date" not in frame.columns and "index" in frame.columns:
        frame = frame.rename(columns={"index": "date"})

    if "ticker" not in frame.columns:
        frame["ticker"] = pd.NA

    return clean_price_data(frame)


def download_yahoo_prices(
    tickers: list[str],
    start_date: str,
    end_date: str,
    auto_adjust: bool = False,
) -> pd.DataFrame:
    """Download daily OHLCV price data from Yahoo Finance."""
    normalized_tickers = [str(ticker).upper().strip() for ticker in tickers if str(ticker).strip()]
    if not normalized_tickers:
        return pd.DataFrame(columns=["date", "ticker", "open", "high", "low", "close", "volume"])

    yf = _import_yfinance()
    downloaded = yf.download(
        tickers=normalized_tickers,
        start=start_date,
        end=end_date,
        auto_adjust=auto_adjust,
        progress=False,
        group_by="column",
        threads=True,
    )
    prices = _normalize_downloaded_prices(downloaded)

    if prices["ticker"].isna().all() and len(normalized_tickers) == 1 and not prices.empty:
        prices["ticker"] = normalized_tickers[0]
        prices = clean_price_data(prices)

    return prices


def build_benchmark_from_prices(price_data: pd.DataFrame, ticker: str = "SPY") -> pd.DataFrame:
    """Extract a benchmark close series from a standard price table."""
    normalized_ticker = str(ticker).upper().strip()
    benchmark = price_data[price_data["ticker"] == normalized_ticker][["date", "ticker", "close"]].copy()
    return clean_benchmark_data(benchmark)


def read_tickers_file(path: str | Path) -> list[str]:
    """Read a plain-text ticker list with one symbol per line."""
    content = Path(path).read_text(encoding="utf-8")
    return [line.strip().upper() for line in content.splitlines() if line.strip()]
