"""Transaction cost models for the backtest engine."""

import pandas as pd


def estimate_transaction_costs(trades: pd.DataFrame, commission_bps: float = 10.0) -> pd.DataFrame:
    """Estimate transaction costs from turnover or trade deltas."""
    trade_copy = trades.copy()
    if trade_copy.empty:
        return pd.DataFrame(columns=["rebalance_date", "turnover", "cost"])

    if "turnover" not in trade_copy.columns:
        if "delta_weight" in trade_copy.columns:
            trade_copy["turnover"] = trade_copy["delta_weight"].abs()
        else:
            trade_copy["turnover"] = 0.0

    commission_rate = commission_bps / 10000.0
    trade_copy["cost"] = trade_copy["turnover"].fillna(0.0) * commission_rate
    return trade_copy
