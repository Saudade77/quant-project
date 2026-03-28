"""Tests for signal generation and portfolio construction."""

import pandas as pd

from src.strategy.portfolio import generate_target_portfolio
from src.strategy.rebalance import get_rebalance_dates
from src.strategy.signal import select_top_names


def test_get_rebalance_dates_returns_month_end_trading_dates() -> None:
    dates = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2024-01-30",
                    "2024-01-31",
                    "2024-02-28",
                    "2024-02-29",
                ]
            )
        }
    )

    result = get_rebalance_dates(dates, frequency="monthly")

    assert list(result) == [pd.Timestamp("2024-01-31"), pd.Timestamp("2024-02-29")]


def test_select_top_names_picks_highest_scores_per_date() -> None:
    scores = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31", "2024-01-31", "2024-01-31"]),
            "ticker": ["AAPL", "MSFT", "NVDA"],
            "score": [0.8, 0.5, 0.9],
        }
    )

    result = select_top_names(scores, top_n=2)

    assert list(result["ticker"]) == ["NVDA", "AAPL"]


def test_generate_target_portfolio_returns_equal_weight_positions() -> None:
    scores = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2024-01-30",
                    "2024-01-31",
                    "2024-01-31",
                    "2024-02-28",
                    "2024-02-29",
                    "2024-02-29",
                ]
            ),
            "ticker": ["AAPL", "AAPL", "MSFT", "AAPL", "AAPL", "MSFT"],
            "score": [0.1, 0.8, 0.6, 0.2, 0.7, 0.9],
        }
    )

    result = generate_target_portfolio(scores, top_n=2, frequency="monthly")

    assert list(result.columns) == ["rebalance_date", "ticker", "target_weight"]
    assert set(result["rebalance_date"]) == {pd.Timestamp("2024-01-31"), pd.Timestamp("2024-02-29")}
    assert (result.groupby("rebalance_date")["target_weight"].sum() == 1.0).all()
