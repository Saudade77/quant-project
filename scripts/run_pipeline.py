"""Pipeline entrypoint for the research workflow."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import yaml

from src.analysis.group_return import compute_group_returns
from src.analysis.ic import compute_ic_series
from src.analysis.report import save_placeholder_report
from src.backtest.engine import run_backtest
from src.backtest.metrics import summarize_performance
from src.data.loader import DataBundle, load_market_data
from src.data.universe import build_universe
from src.factors.composite import combine_factor_scores
from src.factors.momentum import compute_momentum_factors
from src.factors.preprocess import preprocess_factor_data
from src.factors.quality import compute_quality_factors
from src.factors.valuation import compute_valuation_factors
from src.strategy.portfolio import generate_target_portfolio

from scripts.make_sample_data import generate_sample_data


def _project_root() -> Path:
    return PROJECT_ROOT


def _load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _required_input_paths(project_root: Path) -> dict[str, Path]:
    paths_config = _load_yaml(project_root / "config" / "paths.yaml").get("paths", {})
    return {
        "market_data": project_root / paths_config["market_data"],
        "fundamentals_data": project_root / paths_config["fundamentals_data"],
        "benchmark_data": project_root / paths_config["benchmark_data"],
    }


def ensure_project_input_data(
    project_root: Path | None = None,
    auto_generate_sample: bool = True,
) -> dict[str, object]:
    """Ensure the project has the standard input tables required by the pipeline."""
    project_root = project_root or _project_root()
    required_paths = _required_input_paths(project_root)
    missing_paths = [path for path in required_paths.values() if not path.exists()]

    if not missing_paths:
        return {
            "source": "existing_project_data",
            "generated_sample_data": False,
            "paths": required_paths,
        }

    if auto_generate_sample:
        generate_sample_data(project_root)
        missing_after_generation = [path for path in required_paths.values() if not path.exists()]
        if missing_after_generation:
            missing_display = ", ".join(str(path) for path in missing_after_generation)
            raise FileNotFoundError(f"Missing required input data after sample generation: {missing_display}")
        return {
            "source": "generated_sample_data",
            "generated_sample_data": True,
            "paths": required_paths,
        }

    missing_display = ", ".join(str(path) for path in missing_paths)
    raise FileNotFoundError(f"Missing required input data: {missing_display}")


def _compute_forward_returns(price_data: pd.DataFrame, horizon: int = 5) -> pd.DataFrame:
    prices = price_data.copy()
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
    prices["ticker"] = prices["ticker"].astype("string").str.upper().str.strip()
    prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
    prices = prices.dropna(subset=["date", "ticker", "close"]).sort_values(["ticker", "date"])
    prices["forward_return"] = prices.groupby("ticker")["close"].shift(-horizon) / prices["close"] - 1.0
    return prices[["date", "ticker", "forward_return"]].dropna().reset_index(drop=True)


def run_pipeline() -> dict[str, object]:
    """Run the full research pipeline and persist the main report."""
    project_root = _project_root()
    config = _load_yaml(project_root / "config" / "default.yaml")
    input_data_state = ensure_project_input_data(project_root)

    bundle: DataBundle = load_market_data()
    universe = build_universe(
        bundle.prices,
        min_history_days=config["universe"]["min_history_days"],
        min_price=config["universe"]["min_price"],
        max_missing_ratio=config["universe"]["max_missing_ratio"],
    )

    factor_frames = [
        compute_valuation_factors(universe, bundle.fundamentals),
        compute_momentum_factors(universe),
        compute_quality_factors(universe, bundle.fundamentals),
    ]

    preprocessed = preprocess_factor_data(factor_frames)
    scores = combine_factor_scores(preprocessed)
    portfolio = generate_target_portfolio(
        scores,
        top_n=config["portfolio"]["top_n"],
        frequency=config["portfolio"]["rebalance_frequency"],
    )
    backtest_result = run_backtest(
        target_weights=portfolio,
        price_data=universe,
        benchmark_data=bundle.benchmark,
        commission_bps=config["backtest"]["commission_bps"],
    )

    forward_returns = _compute_forward_returns(universe, horizon=5)
    min_names_per_date = int(scores.groupby("date")["ticker"].nunique().min()) if not scores.empty else 0
    analysis_groups = max(2, min(5, min_names_per_date)) if min_names_per_date >= 2 else 2
    ic_result = compute_ic_series(preprocessed, forward_returns)
    group_return_result = compute_group_returns(scores, forward_returns, groups=analysis_groups)
    report_path = save_placeholder_report(backtest_result, ic_result, group_return_result)

    outputs_dir = project_root / "outputs" / "reports"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    scores.to_csv(outputs_dir / "scores.csv", index=False)
    portfolio.to_csv(outputs_dir / "portfolio.csv", index=False)
    backtest_result.to_csv(outputs_dir / "backtest_result.csv", index=False)
    ic_result.to_csv(outputs_dir / "ic_result.csv", index=False)
    group_return_result.to_csv(outputs_dir / "group_return_result.csv", index=False)

    return {
        "input_data_source": input_data_state["source"],
        "universe": universe,
        "scores": scores,
        "portfolio": portfolio,
        "backtest_result": backtest_result,
        "performance": summarize_performance(backtest_result),
        "ic_result": ic_result,
        "group_return_result": group_return_result,
        "report_path": report_path,
    }


def main() -> None:
    """Run the end-to-end research pipeline."""
    result = run_pipeline()

    print("Pipeline executed successfully.")
    print(f"Input data source: {result['input_data_source']}")
    print(f"Universe rows: {len(result['universe'])}")
    print(f"Score rows: {len(result['scores'])}")
    print(f"Portfolio rows: {len(result['portfolio'])}")
    print(f"Backtest rows: {len(result['backtest_result'])}")
    print(f"Report: {result['report_path']}")
    print(result["performance"])


if __name__ == "__main__":
    main()
