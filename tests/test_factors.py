"""Tests for factor computation and preprocessing."""

import pandas as pd

from src.factors.composite import combine_factor_scores
from src.factors.momentum import compute_momentum_factors
from src.factors.preprocess import preprocess_factor_data
from src.factors.quality import compute_quality_factors
from src.factors.valuation import compute_valuation_factors


def test_preprocess_factor_data_with_empty_input() -> None:
    result = preprocess_factor_data([])
    assert list(result.columns) == ["date", "ticker", "factor", "value"]


def test_compute_valuation_factors_generates_bp_and_ep() -> None:
    prices = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31", "2024-01-31"]),
            "ticker": ["AAPL", "MSFT"],
            "close": [180.0, 400.0],
        }
    )
    fundamentals = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31", "2024-01-31"]),
            "ticker": ["AAPL", "MSFT"],
            "pb": [10.0, 20.0],
            "pe": [25.0, 40.0],
            "roe": [0.2, 0.25],
        }
    )

    result = compute_valuation_factors(prices, fundamentals)

    assert set(result["factor"].unique()) == {"bp", "ep"}
    assert len(result) == 4


def test_compute_momentum_factors_generates_expected_windows() -> None:
    prices = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=5, freq="D"),
            "ticker": ["AAPL"] * 5,
            "close": [100.0, 101.0, 102.0, 103.0, 104.0],
        }
    )

    result = compute_momentum_factors(prices, windows=(1, 3))

    assert set(result["factor"].unique()) == {"mom_1", "mom_3"}
    latest = result[(result["date"] == pd.Timestamp("2024-01-05")) & (result["factor"] == "mom_1")]
    assert latest["value"].iloc[0] == 104.0 / 103.0 - 1


def test_compute_quality_factors_generates_roe_exposures() -> None:
    prices = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31"]),
            "ticker": ["AAPL"],
            "close": [180.0],
        }
    )
    fundamentals = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31"]),
            "ticker": ["AAPL"],
            "roe": [0.2],
        }
    )

    result = compute_quality_factors(prices, fundamentals)

    assert list(result.columns) == ["date", "ticker", "factor", "value"]
    assert result.iloc[0]["factor"] == "roe"
    assert result.iloc[0]["value"] == 0.2


def test_preprocess_factor_data_zscores_each_cross_section() -> None:
    raw = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31", "2024-01-31", "2024-01-31"]),
            "ticker": ["AAPL", "MSFT", "NVDA"],
            "factor": ["bp", "bp", "bp"],
            "value": [1.0, 2.0, 3.0],
        }
    )

    result = preprocess_factor_data([raw])

    group = result[result["factor"] == "bp"]["value"]
    assert round(group.mean(), 10) == 0.0


def test_combine_factor_scores_averages_factor_values() -> None:
    factor_data = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-31", "2024-01-31", "2024-01-31", "2024-01-31"]),
            "ticker": ["AAPL", "AAPL", "MSFT", "MSFT"],
            "factor": ["bp", "roe", "bp", "roe"],
            "value": [1.0, 0.0, 0.0, 0.0],
        }
    )

    result = combine_factor_scores(factor_data)

    assert list(result.columns) == ["date", "ticker", "score"]
    assert result.iloc[0]["ticker"] == "AAPL"
    assert result.iloc[0]["score"] == 0.5
