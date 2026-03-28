"""Rebalance scheduling utilities."""

import pandas as pd


def get_rebalance_dates(price_data: pd.DataFrame, frequency: str = "monthly") -> pd.Series:
    """Infer rebalance dates from available trading dates.

    Currently supported frequencies:
    - `monthly`: last available trading day of each month
    - `weekly`: last available trading day of each week
    - `daily`: every available trading day
    """
    if price_data.empty:
        return pd.Series(dtype="datetime64[ns]")

    if "date" not in price_data.columns:
        raise ValueError("price_data must contain a 'date' column")

    dates = pd.to_datetime(price_data["date"], errors="coerce").dropna().drop_duplicates().sort_values()
    if dates.empty:
        return pd.Series(dtype="datetime64[ns]")

    normalized_frequency = frequency.lower().strip()
    if normalized_frequency == "daily":
        return pd.Series(dates.to_numpy(), dtype="datetime64[ns]")

    date_frame = pd.DataFrame({"date": dates})
    if normalized_frequency == "monthly":
        period_key = date_frame["date"].dt.to_period("M")
    elif normalized_frequency == "weekly":
        period_key = date_frame["date"].dt.to_period("W")
    else:
        raise ValueError(f"Unsupported rebalance frequency: {frequency}")

    rebalance_dates = date_frame.groupby(period_key, sort=True)["date"].max()
    return rebalance_dates.reset_index(drop=True)
