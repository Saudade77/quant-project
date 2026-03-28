"""Reporting utilities for figures and summary tables."""

from pathlib import Path

import pandas as pd

from src.backtest.metrics import summarize_performance


def save_placeholder_report(
    backtest_result: pd.DataFrame,
    ic_result: pd.DataFrame | None = None,
    group_return_result: pd.DataFrame | None = None,
    output_dir: str = "outputs/reports",
    filename: str = "analysis_report.txt",
) -> Path:
    """Write a minimal text report and return its path."""
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)
    report_path = path / filename

    performance = summarize_performance(backtest_result)
    latest_nav = float(backtest_result["nav"].iloc[-1]) if not backtest_result.empty and "nav" in backtest_result.columns else 1.0

    lines = [
        "Multi-Factor Backtest Report",
        "",
        "Performance Summary",
        f"total_return: {performance['total_return']:.6f}",
        f"annual_return: {performance['annual_return']:.6f}",
        f"annual_volatility: {performance['annual_volatility']:.6f}",
        f"sharpe: {performance['sharpe']:.6f}",
        f"max_drawdown: {performance['max_drawdown']:.6f}",
        f"latest_nav: {latest_nav:.6f}",
    ]

    if ic_result is not None and not ic_result.empty:
        lines.extend(
            [
                "",
                "IC Summary",
                f"mean_ic: {pd.to_numeric(ic_result['ic'], errors='coerce').mean():.6f}",
                f"mean_rank_ic: {pd.to_numeric(ic_result['rank_ic'], errors='coerce').mean():.6f}",
            ]
        )

    if group_return_result is not None and not group_return_result.empty:
        lines.extend(
            [
                "",
                "Group Return Summary",
                f"average_long_short_return: {pd.to_numeric(group_return_result['long_short_return'], errors='coerce').mean():.6f}",
            ]
        )

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path
