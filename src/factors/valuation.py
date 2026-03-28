"""Valuation factor implementations such as BP and EP."""

import pandas as pd

from src.factors.base import FACTOR_OUTPUT_COLUMNS


def compute_valuation_factors(price_data: pd.DataFrame, fundamentals_data: pd.DataFrame) -> pd.DataFrame:
    """Compute basic valuation factors from fundamentals data.

    Supported outputs:
    - `bp`: book-to-price proxy computed as 1 / pb
    - `ep`: earnings-to-price proxy computed as 1 / pe
    """
    if fundamentals_data.empty:
        return pd.DataFrame(columns=FACTOR_OUTPUT_COLUMNS)

    fundamentals = fundamentals_data.copy()
    fundamentals["date"] = pd.to_datetime(fundamentals["date"], errors="coerce")
    fundamentals["ticker"] = fundamentals["ticker"].astype("string").str.upper().str.strip()

    for column in ["pe", "pb"]:
        if column in fundamentals.columns:
            fundamentals[column] = pd.to_numeric(fundamentals[column], errors="coerce")
        else:
            fundamentals[column] = pd.NA

    bp = fundamentals[["date", "ticker", "pb"]].copy()
    bp["factor"] = "bp"
    bp["value"] = 1 / bp["pb"].where(bp["pb"] > 0)

    ep = fundamentals[["date", "ticker", "pe"]].copy()
    ep["factor"] = "ep"
    ep["value"] = 1 / ep["pe"].where(ep["pe"] > 0)

    factor_data = pd.concat(
        [
            bp[["date", "ticker", "factor", "value"]],
            ep[["date", "ticker", "factor", "value"]],
        ],
        ignore_index=True,
    )

    if not price_data.empty:
        available = price_data[["date", "ticker"]].drop_duplicates()
        factor_data = factor_data.merge(available, on=["date", "ticker"], how="inner")

    return factor_data.dropna(subset=["date", "ticker"]).reset_index(drop=True)
