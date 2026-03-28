"""Date handling helpers for trading-calendar-aware logic."""

import pandas as pd


def ensure_datetime(series: pd.Series) -> pd.Series:
    """Convert a series to pandas datetime."""
    return pd.to_datetime(series, errors="coerce")

