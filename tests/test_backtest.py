"""Tests for backtest engine and metrics."""

import pandas as pd
import pytest

from src.backtest.costs import estimate_transaction_costs
from src.backtest.engine import run_backtest
from src.backtest.metrics import summarize_performance


def test_estimate_transaction_costs_uses_turnover() -> None:
    trades = pd.DataFrame(
        {
            "rebalance_date": pd.to_datetime(["2024-01-31"]),
            "turnover": [1.5],
        }
    )

    result = estimate_transaction_costs(trades, commission_bps=10.0)

    assert result.iloc[0]["cost"] == 1.5 * 10.0 / 10000.0


def test_run_backtest_returns_expected_columns_and_nav() -> None:
    target_weights = pd.DataFrame(
        {
            "rebalance_date": pd.to_datetime(["2024-01-31", "2024-01-31"]),
            "ticker": ["AAPL", "MSFT"],
            "target_weight": [0.5, 0.5],
        }
    )
    price_data = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2024-01-31",
                    "2024-02-01",
                    "2024-02-02",
                    "2024-01-31",
                    "2024-02-01",
                    "2024-02-02",
                ]
            ),
            "ticker": ["AAPL", "AAPL", "AAPL", "MSFT", "MSFT", "MSFT"],
            "close": [100.0, 110.0, 121.0, 200.0, 210.0, 220.5],
        }
    )

    result = run_backtest(target_weights, price_data, commission_bps=0.0)

    assert list(result.columns) == ["date", "portfolio_return", "benchmark_return", "turnover", "cost", "nav"]
    assert list(result["date"]) == [pd.Timestamp("2024-02-01"), pd.Timestamp("2024-02-02")]
    assert result.iloc[0]["portfolio_return"] == pytest.approx(0.075)
    assert result.iloc[-1]["nav"] > 1.0


def test_summarize_performance_returns_core_metrics() -> None:
    backtest_result = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-02-01", "2024-02-02"]),
            "portfolio_return": [0.01, -0.02],
            "benchmark_return": [0.0, 0.0],
            "turnover": [0.0, 0.0],
            "cost": [0.0, 0.0],
            "nav": [1.01, 0.9898],
        }
    )

    summary = summarize_performance(backtest_result)

    assert set(summary) == {"total_return", "annual_return", "annual_volatility", "sharpe", "max_drawdown"}
    assert summary["total_return"] == backtest_result.iloc[-1]["nav"] - 1.0
