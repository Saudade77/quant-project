"""Quality factor implementations such as ROE-based signals."""

import pandas as pd

from src.factors.base import FACTOR_OUTPUT_COLUMNS


def compute_quality_factors(price_data: pd.DataFrame, fundamentals_data: pd.DataFrame) -> pd.DataFrame:
    """Compute a basic quality factor from fundamentals data."""
    if fundamentals_data.empty:
        return pd.DataFrame(columns=FACTOR_OUTPUT_COLUMNS)

    fundamentals = fundamentals_data.copy()
    fundamentals["date"] = pd.to_datetime(fundamentals["date"], errors="coerce")
    fundamentals["ticker"] = fundamentals["ticker"].astype("string").str.upper().str.strip()
    if "roe" not in fundamentals.columns:
        fundamentals["roe"] = pd.NA
    fundamentals["roe"] = pd.to_numeric(fundamentals["roe"], errors="coerce")

    quality = fundamentals[["date", "ticker", "roe"]].copy()
    quality["factor"] = "roe"
    quality["value"] = quality["roe"]
    factor_data = quality[["date", "ticker", "factor", "value"]]

    if not price_data.empty:
        available = price_data[["date", "ticker"]].drop_duplicates()
        factor_data = factor_data.merge(available, on=["date", "ticker"], how="inner")

    return factor_data.dropna(subset=["date", "ticker"]).reset_index(drop=True)
