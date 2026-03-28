"""Tests for pipeline input preparation and real-data orchestration."""

from pathlib import Path

import yaml

from scripts.prepare_real_data import prepare_real_data
from scripts.run_pipeline import ensure_project_input_data


def test_ensure_project_input_data_uses_existing_project_tables(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    data_dir = tmp_path / "data" / "raw"
    config_dir.mkdir(parents=True)
    data_dir.mkdir(parents=True)

    with (config_dir / "paths.yaml").open("w", encoding="utf-8") as handle:
        yaml.safe_dump(
            {
                "paths": {
                    "market_data": "data/raw/us_equity_prices.parquet",
                    "fundamentals_data": "data/raw/us_equity_fundamentals.parquet",
                    "benchmark_data": "data/raw/us_benchmark.parquet",
                }
            },
            handle,
        )

    for filename in ["us_equity_prices.parquet", "us_equity_fundamentals.parquet", "us_benchmark.parquet"]:
        (data_dir / filename).write_text("placeholder", encoding="utf-8")

    state = ensure_project_input_data(project_root=tmp_path, auto_generate_sample=False)

    assert state["source"] == "existing_project_data"
    assert state["generated_sample_data"] is False


def test_ensure_project_input_data_generates_sample_when_missing(tmp_path: Path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    data_dir = tmp_path / "data" / "raw"
    config_dir.mkdir(parents=True)
    data_dir.mkdir(parents=True)

    with (config_dir / "paths.yaml").open("w", encoding="utf-8") as handle:
        yaml.safe_dump(
            {
                "paths": {
                    "market_data": "data/raw/us_equity_prices.parquet",
                    "fundamentals_data": "data/raw/us_equity_fundamentals.parquet",
                    "benchmark_data": "data/raw/us_benchmark.parquet",
                }
            },
            handle,
        )

    def fake_generate_sample_data(project_root: Path) -> None:
        for filename in ["us_equity_prices.parquet", "us_equity_fundamentals.parquet", "us_benchmark.parquet"]:
            (project_root / "data" / "raw" / filename).write_text("generated", encoding="utf-8")

    monkeypatch.setattr("scripts.run_pipeline.generate_sample_data", fake_generate_sample_data)

    state = ensure_project_input_data(project_root=tmp_path, auto_generate_sample=True)

    assert state["source"] == "generated_sample_data"
    assert state["generated_sample_data"] is True


def test_prepare_real_data_orchestrates_yahoo_and_sec_steps(tmp_path: Path, monkeypatch) -> None:
    called = {}

    def fake_fetch_sec_source_data(output_dir: str, user_agent: str | None, project_root: Path):
        called["fetch_sec_source_data"] = (output_dir, user_agent, project_root)
        sec_dir = project_root / output_dir
        sec_dir.mkdir(parents=True, exist_ok=True)
        return {
            "company_tickers": sec_dir / "company_tickers.json",
            "submissions": sec_dir / "submissions.zip",
            "companyfacts": sec_dir / "companyfacts.zip",
        }

    def fake_prepare_yahoo_price_data(**kwargs):
        called["prepare_yahoo_price_data"] = kwargs
        return {
            "market_data": kwargs["project_root"] / "data/raw/us_equity_prices.parquet",
            "benchmark_data": kwargs["project_root"] / "data/raw/us_benchmark.parquet",
        }

    def fake_prepare_sec_fundamentals_data(**kwargs):
        called["prepare_sec_fundamentals_data"] = kwargs
        return {
            "fundamentals_data": kwargs["project_root"] / "data/raw/us_equity_fundamentals.parquet",
        }

    monkeypatch.setattr("scripts.prepare_real_data.fetch_sec_source_data", fake_fetch_sec_source_data)
    monkeypatch.setattr("scripts.prepare_real_data.prepare_yahoo_price_data", fake_prepare_yahoo_price_data)
    monkeypatch.setattr("scripts.prepare_real_data.prepare_sec_fundamentals_data", fake_prepare_sec_fundamentals_data)

    written = prepare_real_data(
        tickers=["AAPL", "MSFT"],
        start_date="2024-01-01",
        end_date="2024-12-31",
        benchmark_ticker="SPY",
        sec_user_agent="example@example.com",
        project_root=tmp_path,
    )

    assert written["market_data"] == tmp_path / "data/raw/us_equity_prices.parquet"
    assert written["benchmark_data"] == tmp_path / "data/raw/us_benchmark.parquet"
    assert written["fundamentals_data"] == tmp_path / "data/raw/us_equity_fundamentals.parquet"
    assert called["prepare_sec_fundamentals_data"]["mapping_path"] == "data/raw/sec_source/company_tickers.json"
