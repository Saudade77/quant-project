"""Tests for building fundamentals from SEC raw source data."""

import json
import zipfile
from pathlib import Path

import pandas as pd
import yaml

from src.data.sec_fundamentals import (
    build_sec_fundamentals_dataset,
    build_sec_fundamentals_for_ticker,
    prepare_sec_fundamentals_data,
)


def _make_submissions_json() -> dict:
    return {
        "filings": {
            "recent": {
                "accessionNumber": [
                    "0000000001-23-000001",
                    "0000000001-23-000002",
                    "0000000001-23-000003",
                    "0000000001-24-000001",
                ],
                "filingDate": ["2023-05-10", "2023-08-10", "2023-11-10", "2024-02-10"],
                "acceptanceDateTime": [
                    "2023-05-10T16:00:00.000Z",
                    "2023-08-10T16:00:00.000Z",
                    "2023-11-10T16:00:00.000Z",
                    "2024-02-10T16:00:00.000Z",
                ],
            }
        }
    }


def _make_companyfacts_json() -> dict:
    return {
        "facts": {
            "us-gaap": {
                "NetIncomeLoss": {
                    "units": {
                        "USD": [
                            {"end": "2023-03-31", "filed": "2023-05-10", "form": "10-Q", "fp": "Q1", "fy": 2023, "accn": "0000000001-23-000001", "val": 10.0},
                            {"end": "2023-06-30", "filed": "2023-08-10", "form": "10-Q", "fp": "Q2", "fy": 2023, "accn": "0000000001-23-000002", "val": 11.0},
                            {"end": "2023-09-30", "filed": "2023-11-10", "form": "10-Q", "fp": "Q3", "fy": 2023, "accn": "0000000001-23-000003", "val": 12.0},
                            {"end": "2023-12-31", "filed": "2024-02-10", "form": "10-Q", "fp": "Q4", "fy": 2024, "accn": "0000000001-24-000001", "val": 13.0},
                        ]
                    }
                },
                "StockholdersEquity": {
                    "units": {
                        "USD": [
                            {"end": "2023-03-31", "filed": "2023-05-10", "form": "10-Q", "fp": "Q1", "fy": 2023, "accn": "0000000001-23-000001", "val": 100.0},
                            {"end": "2023-06-30", "filed": "2023-08-10", "form": "10-Q", "fp": "Q2", "fy": 2023, "accn": "0000000001-23-000002", "val": 110.0},
                            {"end": "2023-09-30", "filed": "2023-11-10", "form": "10-Q", "fp": "Q3", "fy": 2023, "accn": "0000000001-23-000003", "val": 120.0},
                            {"end": "2023-12-31", "filed": "2024-02-10", "form": "10-Q", "fp": "Q4", "fy": 2024, "accn": "0000000001-24-000001", "val": 130.0},
                        ]
                    }
                },
            },
            "dei": {
                "EntityCommonStockSharesOutstanding": {
                    "units": {
                        "shares": [
                            {"end": "2023-03-31", "filed": "2023-05-10", "form": "10-Q", "fp": "Q1", "fy": 2023, "accn": "0000000001-23-000001", "val": 10.0},
                            {"end": "2023-06-30", "filed": "2023-08-10", "form": "10-Q", "fp": "Q2", "fy": 2023, "accn": "0000000001-23-000002", "val": 10.0},
                            {"end": "2023-09-30", "filed": "2023-11-10", "form": "10-Q", "fp": "Q3", "fy": 2023, "accn": "0000000001-23-000003", "val": 10.0},
                            {"end": "2023-12-31", "filed": "2024-02-10", "form": "10-Q", "fp": "Q4", "fy": 2024, "accn": "0000000001-24-000001", "val": 10.0},
                        ]
                    }
                }
            },
        }
    }


def test_build_sec_fundamentals_for_ticker_computes_pe_pb_and_roe() -> None:
    prices = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-02-09", "2024-02-12", "2024-02-13"]),
            "close": [120.0, 130.0, 140.0],
        }
    )

    fundamentals = build_sec_fundamentals_for_ticker(
        ticker="AAPL",
        price_data=prices,
        companyfacts_json=_make_companyfacts_json(),
        submissions_json=_make_submissions_json(),
    )

    assert list(fundamentals.columns) == ["date", "ticker", "pe", "pb", "roe"]
    row = fundamentals[fundamentals["date"] == pd.Timestamp("2024-02-12")].iloc[0]
    assert row["ticker"] == "AAPL"
    assert row["pe"] == 1300.0 / 46.0
    assert row["pb"] == 1300.0 / 130.0
    assert row["roe"] == 46.0 / 125.0


def test_build_sec_fundamentals_dataset_reads_from_sec_archives(tmp_path: Path) -> None:
    prices = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-02-12"]),
            "ticker": ["AAPL"],
            "close": [130.0],
        }
    )
    ticker_mapping = pd.DataFrame({"ticker": ["AAPL"], "cik": [320193], "company_name": ["Apple Inc."]})

    submissions_zip = tmp_path / "submissions.zip"
    companyfacts_zip = tmp_path / "companyfacts.zip"

    with zipfile.ZipFile(submissions_zip, "w") as archive:
        archive.writestr("submissions/CIK0000320193.json", json.dumps(_make_submissions_json()))
    with zipfile.ZipFile(companyfacts_zip, "w") as archive:
        archive.writestr("companyfacts/CIK0000320193.json", json.dumps(_make_companyfacts_json()))

    fundamentals = build_sec_fundamentals_dataset(
        price_data=prices,
        ticker_mapping=ticker_mapping,
        submissions_zip_path=submissions_zip,
        companyfacts_zip_path=companyfacts_zip,
    )

    assert set(fundamentals["ticker"].unique()) == {"AAPL"}
    assert fundamentals.iloc[0]["pe"] == 1300.0 / 46.0


def test_prepare_sec_fundamentals_data_writes_standard_parquet(tmp_path: Path) -> None:
    config_dir = tmp_path / "config"
    data_raw_dir = tmp_path / "data" / "raw"
    sec_source_dir = data_raw_dir / "sec_source"
    config_dir.mkdir(parents=True)
    sec_source_dir.mkdir(parents=True)

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

    pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-02-12"]),
            "ticker": ["AAPL"],
            "open": [129.0],
            "high": [131.0],
            "low": [128.0],
            "close": [130.0],
            "volume": [1000],
        }
    ).to_parquet(data_raw_dir / "us_equity_prices.parquet", index=False)

    (sec_source_dir / "company_tickers.json").write_text(
        json.dumps({"0": {"ticker": "AAPL", "cik_str": 320193, "title": "Apple Inc."}}),
        encoding="utf-8",
    )
    with zipfile.ZipFile(sec_source_dir / "submissions.zip", "w") as archive:
        archive.writestr("submissions/CIK0000320193.json", json.dumps(_make_submissions_json()))
    with zipfile.ZipFile(sec_source_dir / "companyfacts.zip", "w") as archive:
        archive.writestr("companyfacts/CIK0000320193.json", json.dumps(_make_companyfacts_json()))

    written = prepare_sec_fundamentals_data(project_root=tmp_path)
    fundamentals = pd.read_parquet(written["fundamentals_data"])

    assert written["fundamentals_data"] == data_raw_dir / "us_equity_fundamentals.parquet"
    assert set(fundamentals["ticker"].unique()) == {"AAPL"}
    assert list(fundamentals.columns) == ["date", "ticker", "pe", "pb", "roe"]
