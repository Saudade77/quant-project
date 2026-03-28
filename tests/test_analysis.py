"""Tests for analysis modules."""

from pathlib import Path

import pandas as pd

from src.analysis.group_return import compute_group_returns
from src.analysis.ic import compute_ic_series
from src.analysis.report import save_placeholder_report


def test_compute_ic_series_returns_factor_level_metrics() -> None:
    factor_data = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31", "2024-01-31", "2024-01-31"]),
            "ticker": ["AAPL", "MSFT", "NVDA"],
            "factor": ["bp", "bp", "bp"],
            "value": [3.0, 2.0, 1.0],
        }
    )
    forward_returns = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31", "2024-01-31", "2024-01-31"]),
            "ticker": ["AAPL", "MSFT", "NVDA"],
            "forward_return": [0.03, 0.02, 0.01],
        }
    )

    result = compute_ic_series(factor_data, forward_returns)

    assert list(result.columns) == ["date", "factor", "ic", "rank_ic", "n"]
    assert result.iloc[0]["factor"] == "bp"
    assert result.iloc[0]["ic"] == 1.0


def test_compute_group_returns_builds_quantile_returns() -> None:
    score_data = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31"] * 4),
            "ticker": ["AAPL", "MSFT", "NVDA", "AMZN"],
            "score": [4.0, 3.0, 2.0, 1.0],
        }
    )
    forward_returns = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31"] * 4),
            "ticker": ["AAPL", "MSFT", "NVDA", "AMZN"],
            "forward_return": [0.04, 0.03, 0.02, 0.01],
        }
    )

    result = compute_group_returns(score_data, forward_returns, groups=2)

    assert list(result.columns) == ["date", "group", "group_return", "group_count", "long_short_return"]
    assert set(result["group"]) == {1, 2}
    assert result[result["group"] == 1]["group_return"].iloc[0] > result[result["group"] == 2]["group_return"].iloc[0]


def test_save_placeholder_report_writes_report_file(tmp_path: Path) -> None:
    backtest_result = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-02-01", "2024-02-02"]),
            "portfolio_return": [0.01, 0.02],
            "benchmark_return": [0.0, 0.0],
            "turnover": [0.5, 0.0],
            "cost": [0.001, 0.0],
            "nav": [1.01, 1.0302],
        }
    )
    ic_result = pd.DataFrame({"date": pd.to_datetime(["2024-01-31"]), "factor": ["bp"], "ic": [0.1], "rank_ic": [0.2], "n": [10]})
    group_result = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31", "2024-01-31"]),
            "group": [1, 2],
            "group_return": [0.03, 0.01],
            "group_count": [2, 2],
            "long_short_return": [0.02, 0.02],
        }
    )

    report_path = save_placeholder_report(
        backtest_result=backtest_result,
        ic_result=ic_result,
        group_return_result=group_result,
        output_dir=str(tmp_path),
    )

    assert report_path.exists()
    content = report_path.read_text(encoding="utf-8")
    assert "Performance Summary" in content
    assert "IC Summary" in content
    assert "Group Return Summary" in content
