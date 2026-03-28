"""Universe construction for the US equity stock pool."""

import pandas as pd


def build_universe(
    price_data: pd.DataFrame,
    min_history_days: int = 60,
    min_price: float = 5.0,
    max_missing_ratio: float = 0.05,
) -> pd.DataFrame:
    """Construct a basic tradable universe from cleaned price data.

    This first version keeps only stocks that:
    - have at least `min_history_days` valid close observations
    - have an acceptable close-price missing ratio
    - trade above `min_price`
    """
    if price_data.empty:
        return price_data.copy()

    required_columns = {"date", "ticker", "close"}
    missing_columns = required_columns - set(price_data.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns for universe construction: {sorted(missing_columns)}")

    working = price_data.copy()
    working["date"] = pd.to_datetime(working["date"], errors="coerce")
    working["close"] = pd.to_numeric(working["close"], errors="coerce")
    working = working.dropna(subset=["date", "ticker"])

    grouped = working.groupby("ticker", dropna=False)
    summary = grouped["close"].agg(
        valid_days=lambda series: series.notna().sum(),
        total_days="size",
    )
    summary["missing_ratio"] = 1 - (summary["valid_days"] / summary["total_days"])

    eligible_tickers = summary.index[
        (summary["valid_days"] >= min_history_days)
        & (summary["missing_ratio"] <= max_missing_ratio)
    ]

    filtered = working[working["ticker"].isin(eligible_tickers)].copy()
    filtered = filtered[filtered["close"] >= min_price]
    filtered = filtered.sort_values(["date", "ticker"]).reset_index(drop=True)
    return filtered
