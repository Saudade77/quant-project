"""Performance metrics for strategy evaluation."""

import numpy as np
import pandas as pd


def summarize_performance(backtest_result: pd.DataFrame) -> dict[str, float]:
    """Summarize performance from a daily backtest result."""
    if backtest_result.empty:
        return {
            "total_return": 0.0,
            "annual_return": 0.0,
            "annual_volatility": 0.0,
            "sharpe": 0.0,
            "max_drawdown": 0.0,
        }

    returns = pd.to_numeric(backtest_result["portfolio_return"], errors="coerce").fillna(0.0)
    nav = pd.to_numeric(backtest_result["nav"], errors="coerce").ffill().fillna(1.0)
    periods = len(returns)

    total_return = float(nav.iloc[-1] - 1.0)
    annual_return = float((nav.iloc[-1] ** (252 / periods)) - 1.0) if periods > 0 and nav.iloc[-1] > 0 else 0.0
    annual_volatility = float(returns.std(ddof=0) * np.sqrt(252)) if periods > 1 else 0.0
    sharpe = float(annual_return / annual_volatility) if annual_volatility > 0 else 0.0

    rolling_peak = nav.cummax()
    drawdown = (nav / rolling_peak) - 1.0
    max_drawdown = float(drawdown.min()) if not drawdown.empty else 0.0

    return {
        "total_return": total_return,
        "annual_return": annual_return,
        "annual_volatility": annual_volatility,
        "sharpe": sharpe,
        "max_drawdown": max_drawdown,
    }
