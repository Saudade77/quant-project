"""Portfolio backtest engine."""

import pandas as pd

from src.backtest.costs import estimate_transaction_costs


def _prepare_price_returns(price_data: pd.DataFrame) -> pd.DataFrame:
    prices = price_data.copy()
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
    prices["ticker"] = prices["ticker"].astype("string").str.upper().str.strip()
    prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
    prices = prices.dropna(subset=["date", "ticker", "close"]).sort_values(["ticker", "date"])
    prices["asset_return"] = prices.groupby("ticker")["close"].pct_change()
    return prices


def _prepare_target_weights(target_weights: pd.DataFrame) -> pd.DataFrame:
    weights = target_weights.copy()
    weights["rebalance_date"] = pd.to_datetime(weights["rebalance_date"], errors="coerce")
    weights["ticker"] = weights["ticker"].astype("string").str.upper().str.strip()
    weights["target_weight"] = pd.to_numeric(weights["target_weight"], errors="coerce")
    weights = weights.dropna(subset=["rebalance_date", "ticker", "target_weight"])
    return weights.sort_values(["rebalance_date", "ticker"]).reset_index(drop=True)


def _compute_turnover_by_rebalance(weights: pd.DataFrame) -> pd.DataFrame:
    if weights.empty:
        return pd.DataFrame(columns=["rebalance_date", "turnover"])

    rebalances = sorted(weights["rebalance_date"].drop_duplicates())
    previous_weights: dict[str, float] = {}
    rows: list[dict[str, float | pd.Timestamp]] = []

    for rebalance_date in rebalances:
        current_slice = weights[weights["rebalance_date"] == rebalance_date]
        current_weights = dict(zip(current_slice["ticker"], current_slice["target_weight"]))
        tickers = set(previous_weights) | set(current_weights)
        turnover = sum(abs(current_weights.get(ticker, 0.0) - previous_weights.get(ticker, 0.0)) for ticker in tickers)
        rows.append({"rebalance_date": rebalance_date, "turnover": turnover})
        previous_weights = current_weights

    return pd.DataFrame(rows)


def _compute_benchmark_returns(benchmark_data: pd.DataFrame | None, trading_dates: pd.Series) -> pd.DataFrame:
    if benchmark_data is None or benchmark_data.empty:
        return pd.DataFrame({"date": trading_dates, "benchmark_return": 0.0})

    benchmark = benchmark_data.copy()
    benchmark["date"] = pd.to_datetime(benchmark["date"], errors="coerce")
    benchmark["close"] = pd.to_numeric(benchmark["close"], errors="coerce")
    benchmark = benchmark.dropna(subset=["date", "close"]).sort_values(["date"])
    benchmark = benchmark.drop_duplicates(subset=["date"], keep="last")
    benchmark["benchmark_return"] = benchmark["close"].pct_change().fillna(0.0)
    return pd.DataFrame({"date": trading_dates}).merge(
        benchmark[["date", "benchmark_return"]],
        on="date",
        how="left",
    ).fillna({"benchmark_return": 0.0})


def run_backtest(
    target_weights: pd.DataFrame,
    price_data: pd.DataFrame,
    benchmark_data: pd.DataFrame | None = None,
    commission_bps: float = 10.0,
) -> pd.DataFrame:
    """Run a simple daily close-to-close backtest from target weights."""
    result_columns = ["date", "portfolio_return", "benchmark_return", "turnover", "cost", "nav"]
    if target_weights.empty or price_data.empty:
        return pd.DataFrame(columns=result_columns)

    weights = _prepare_target_weights(target_weights)
    prices = _prepare_price_returns(price_data)
    if weights.empty or prices.empty:
        return pd.DataFrame(columns=result_columns)

    trading_dates = pd.Series(sorted(prices["date"].drop_duplicates()))
    turnover = _compute_turnover_by_rebalance(weights)
    costs = estimate_transaction_costs(turnover, commission_bps=commission_bps)
    cost_by_rebalance = dict(zip(costs["rebalance_date"], costs["cost"]))
    turnover_by_rebalance = dict(zip(turnover["rebalance_date"], turnover["turnover"]))

    returns_wide = prices.pivot(index="date", columns="ticker", values="asset_return").sort_index()
    rebalance_dates = sorted(weights["rebalance_date"].drop_duplicates())

    rows: list[dict[str, float | pd.Timestamp]] = []
    for index, rebalance_date in enumerate(rebalance_dates):
        next_rebalance_date = rebalance_dates[index + 1] if index + 1 < len(rebalance_dates) else None
        active_dates = trading_dates[trading_dates > rebalance_date]
        if next_rebalance_date is not None:
            active_dates = active_dates[active_dates <= next_rebalance_date]
        first_active_date = active_dates.iloc[0] if not active_dates.empty else None

        current_weights = weights[weights["rebalance_date"] == rebalance_date].set_index("ticker")["target_weight"]
        if current_weights.empty:
            continue

        for active_date in active_dates:
            daily_returns = returns_wide.loc[active_date] if active_date in returns_wide.index else pd.Series(dtype=float)
            aligned_returns = daily_returns.reindex(current_weights.index).fillna(0.0)
            gross_return = float((aligned_returns * current_weights).sum())
            cost = float(cost_by_rebalance.get(rebalance_date, 0.0)) if active_date == first_active_date else 0.0
            turnover_value = float(turnover_by_rebalance.get(rebalance_date, 0.0)) if active_date == first_active_date else 0.0
            rows.append(
                {
                    "date": active_date,
                    "portfolio_return": gross_return - cost,
                    "turnover": turnover_value,
                    "cost": cost,
                }
            )

    if not rows:
        return pd.DataFrame(columns=result_columns)

    result = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    benchmark_returns = _compute_benchmark_returns(benchmark_data, result["date"])
    result = result.merge(benchmark_returns, on="date", how="left")
    result["benchmark_return"] = result["benchmark_return"].fillna(0.0)
    result["nav"] = (1.0 + result["portfolio_return"]).cumprod()
    return result[result_columns]
