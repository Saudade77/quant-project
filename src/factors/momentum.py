"""Momentum factor implementations for short- and medium-term returns."""

import pandas as pd

from src.factors.base import FACTOR_OUTPUT_COLUMNS


def compute_momentum_factors(price_data: pd.DataFrame, windows: tuple[int, ...] = (20, 60)) -> pd.DataFrame:
    """Compute momentum factors from historical close prices."""
    if price_data.empty:
        return pd.DataFrame(columns=FACTOR_OUTPUT_COLUMNS)

    prices = price_data.copy()
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
    prices["ticker"] = prices["ticker"].astype("string").str.upper().str.strip()
    prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
    prices = prices.dropna(subset=["date", "ticker", "close"]).sort_values(["ticker", "date"])

    factor_frames: list[pd.DataFrame] = []
    for window in windows:
        factor_frame = prices[["date", "ticker", "close"]].copy()
        factor_frame["factor"] = f"mom_{window}"
        factor_frame["value"] = factor_frame.groupby("ticker")["close"].pct_change(periods=window)
        factor_frames.append(factor_frame[["date", "ticker", "factor", "value"]])

    return pd.concat(factor_frames, ignore_index=True) if factor_frames else pd.DataFrame(columns=FACTOR_OUTPUT_COLUMNS)
