"""Portfolio construction from selected stocks."""

import pandas as pd

from src.strategy.rebalance import get_rebalance_dates
from src.strategy.signal import select_top_names


def generate_target_portfolio(
    score_data: pd.DataFrame | None = None,
    top_n: int = 20,
    frequency: str = "monthly",
) -> pd.DataFrame:
    """Build a target portfolio from factor scores.

    The current version supports:
    - rebalance date inference from score dates
    - top-N stock selection
    - equal weighting within each rebalance date
    """
    if score_data is None or score_data.empty:
        return pd.DataFrame(columns=["rebalance_date", "ticker", "target_weight"])

    scores = score_data.copy()
    scores["date"] = pd.to_datetime(scores["date"], errors="coerce")
    scores = scores.dropna(subset=["date"])

    rebalance_dates = get_rebalance_dates(scores[["date"]], frequency=frequency)
    selected = select_top_names(scores[scores["date"].isin(rebalance_dates)], top_n=top_n)
    if selected.empty:
        return pd.DataFrame(columns=["rebalance_date", "ticker", "target_weight"])

    portfolio = selected.copy()
    counts = portfolio.groupby("rebalance_date")["ticker"].transform("count")
    portfolio["target_weight"] = 1.0 / counts
    return portfolio[["rebalance_date", "ticker", "target_weight"]].reset_index(drop=True)
