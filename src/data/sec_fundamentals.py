"""Build project-standard fundamentals from free SEC EDGAR source data."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml

from src.data.cleaner import clean_fundamentals_data
from src.data.sec import load_sec_ticker_mapping, read_json_from_zip

NET_INCOME_CONCEPTS = [
    ("us-gaap", "NetIncomeLoss"),
    ("us-gaap", "ProfitLoss"),
]

EQUITY_CONCEPTS = [
    ("us-gaap", "StockholdersEquity"),
    ("us-gaap", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
    ("us-gaap", "PartnersCapitalIncludingPortionAttributableToNoncontrollingInterest"),
]

SHARES_OUTSTANDING_CONCEPTS = [
    ("dei", "EntityCommonStockSharesOutstanding"),
    ("us-gaap", "CommonStockSharesOutstanding"),
]

QUARTERLY_FORMS = {"10-Q", "10-Q/A"}
ANNUAL_FORMS = {"10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A"}


def _load_paths_config(project_root: Path) -> dict:
    config_path = project_root / "config" / "paths.yaml"
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)["paths"]


def _normalize_accession(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    return str(value).strip()


def _normalize_datetime_series(values: object) -> pd.Series:
    """Convert datetimes to timezone-naive datetime64[ns] for stable merges."""
    series = pd.Series(values)
    normalized = pd.to_datetime(series, errors="coerce", utc=True)
    if hasattr(normalized.dt, "tz_convert"):
        normalized = normalized.dt.tz_convert(None)
    return normalized.astype("datetime64[ns]")


def _find_zip_member(path: str | Path, cik: int) -> str:
    normalized_cik = f"CIK{int(cik):010d}.json"
    from src.data.sec import list_zip_members  # imported lazily to keep dependency surface small

    members = list_zip_members(path)
    exact_matches = [member for member in members if member.endswith(normalized_cik)]
    if not exact_matches:
        raise FileNotFoundError(f"Could not find SEC archive member for {normalized_cik} in {path}")
    return exact_matches[0]


def _extract_companyfacts_entries(
    companyfacts_json: dict,
    concept_candidates: list[tuple[str, str]],
    unit_candidates: set[str] | None = None,
) -> pd.DataFrame:
    facts_root = companyfacts_json.get("facts", {})

    for taxonomy, concept in concept_candidates:
        taxonomy_root = facts_root.get(taxonomy, {})
        concept_root = taxonomy_root.get(concept)
        if not concept_root:
            continue

        rows: list[dict[str, object]] = []
        for unit, entries in concept_root.get("units", {}).items():
            if unit_candidates and unit not in unit_candidates:
                continue
            for entry in entries:
                rows.append(
                    {
                        "value": entry.get("val"),
                        "start": entry.get("start"),
                        "end": entry.get("end"),
                        "filed": entry.get("filed"),
                        "form": entry.get("form"),
                        "fp": entry.get("fp"),
                        "fy": entry.get("fy"),
                        "accn": _normalize_accession(entry.get("accn")),
                    }
                )
        if rows:
            frame = pd.DataFrame(rows)
            frame["end"] = _normalize_datetime_series(frame["end"])
            frame["filed"] = _normalize_datetime_series(frame["filed"])
            frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
            return frame.dropna(subset=["value"]).reset_index(drop=True)

    return pd.DataFrame(columns=["value", "start", "end", "filed", "form", "fp", "fy", "accn"])


def _load_submission_events(submissions_json: dict) -> pd.DataFrame:
    recent = submissions_json.get("filings", {}).get("recent", {})
    if not recent:
        return pd.DataFrame(columns=["accn", "event_date"])

    frame = pd.DataFrame(recent)
    if frame.empty:
        return pd.DataFrame(columns=["accn", "event_date"])

    if "accessionNumber" not in frame.columns:
        return pd.DataFrame(columns=["accn", "event_date"])

    frame["accn"] = frame["accessionNumber"].map(_normalize_accession)
    acceptance = _normalize_datetime_series(frame.get("acceptanceDateTime"))
    filing = _normalize_datetime_series(frame.get("filingDate"))
    frame["event_date"] = acceptance.fillna(filing).dt.normalize().astype("datetime64[ns]")
    return frame[["accn", "event_date"]].dropna(subset=["accn", "event_date"]).drop_duplicates("accn", keep="last")


def _attach_event_dates(facts: pd.DataFrame, submission_events: pd.DataFrame) -> pd.DataFrame:
    if facts.empty:
        return facts.assign(event_date=pd.NaT)

    enriched = facts.merge(submission_events, on="accn", how="left")
    enriched["event_date"] = (
        _normalize_datetime_series(enriched["event_date"])
        .fillna(enriched["filed"])
        .fillna(enriched["end"])
        .astype("datetime64[ns]")
    )
    return enriched.dropna(subset=["event_date"]).sort_values(["event_date", "end"]).reset_index(drop=True)


def _build_ttm_net_income_events(net_income_facts: pd.DataFrame) -> pd.DataFrame:
    if net_income_facts.empty:
        return pd.DataFrame(columns=["event_date", "net_income_ttm"])

    quarterly = net_income_facts[net_income_facts["form"].isin(QUARTERLY_FORMS)].copy()
    quarterly = quarterly.sort_values(["event_date", "end"]).drop_duplicates(subset=["end"], keep="last")

    annual = net_income_facts[net_income_facts["form"].isin(ANNUAL_FORMS)].copy()
    annual = annual.sort_values(["event_date", "end"]).drop_duplicates(subset=["end"], keep="last")

    rows: list[dict[str, object]] = []
    if not quarterly.empty:
        for _, quarter in quarterly.iterrows():
            trailing = quarterly[quarterly["event_date"] <= quarter["event_date"]].tail(4)
            if len(trailing) == 4:
                rows.append(
                    {
                        "event_date": quarter["event_date"],
                        "net_income_ttm": float(trailing["value"].sum()),
                    }
                )

    if not annual.empty:
        for _, year in annual.iterrows():
            rows.append(
                {
                    "event_date": year["event_date"],
                    "net_income_ttm": float(year["value"]),
                }
            )

    if not rows:
        return pd.DataFrame(columns=["event_date", "net_income_ttm"])

    result = pd.DataFrame(rows).sort_values("event_date").drop_duplicates(subset=["event_date"], keep="last")
    return result.reset_index(drop=True)


def _build_point_in_time_events(facts: pd.DataFrame, value_column: str) -> pd.DataFrame:
    if facts.empty:
        return pd.DataFrame(columns=["event_date", value_column])

    result = (
        facts.sort_values(["event_date", "end"])
        .drop_duplicates(subset=["event_date"], keep="last")[["event_date", "value"]]
        .rename(columns={"value": value_column})
        .reset_index(drop=True)
    )
    return result


def _build_equity_events(equity_facts: pd.DataFrame) -> pd.DataFrame:
    equity_events = _build_point_in_time_events(equity_facts, "book_equity")
    if equity_events.empty:
        return pd.DataFrame(columns=["event_date", "book_equity", "avg_book_equity"])

    equity_events["avg_book_equity"] = (equity_events["book_equity"] + equity_events["book_equity"].shift(1)) / 2.0
    equity_events["avg_book_equity"] = equity_events["avg_book_equity"].fillna(equity_events["book_equity"])
    return equity_events


def _align_events_to_prices(
    prices: pd.DataFrame,
    events: pd.DataFrame,
    value_columns: list[str],
) -> pd.DataFrame:
    base = prices[["date"]].sort_values("date").reset_index(drop=True)
    base["date"] = _normalize_datetime_series(base["date"]).dt.normalize().astype("datetime64[ns]")
    if events.empty:
        for column in value_columns:
            base[column] = pd.NA
        return base

    events = events.copy()
    events["event_date"] = _normalize_datetime_series(events["event_date"]).dt.normalize().astype("datetime64[ns]")
    aligned = pd.merge_asof(
        base,
        events.sort_values("event_date"),
        left_on="date",
        right_on="event_date",
        direction="backward",
    )
    return aligned.drop(columns=["event_date"], errors="ignore")


def build_sec_fundamentals_for_ticker(
    ticker: str,
    price_data: pd.DataFrame,
    companyfacts_json: dict,
    submissions_json: dict,
) -> pd.DataFrame:
    """Build daily research fundamentals for a single ticker from SEC source data."""
    prices = price_data.copy()
    if prices.empty:
        return pd.DataFrame(columns=["date", "ticker", "pe", "pb", "roe"])

    prices["date"] = _normalize_datetime_series(prices["date"]).dt.normalize().astype("datetime64[ns]")
    prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
    prices = prices.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)

    submission_events = _load_submission_events(submissions_json)

    net_income_facts = _attach_event_dates(
        _extract_companyfacts_entries(companyfacts_json, NET_INCOME_CONCEPTS, unit_candidates={"USD"}),
        submission_events,
    )
    equity_facts = _attach_event_dates(
        _extract_companyfacts_entries(companyfacts_json, EQUITY_CONCEPTS, unit_candidates={"USD"}),
        submission_events,
    )
    shares_facts = _attach_event_dates(
        _extract_companyfacts_entries(companyfacts_json, SHARES_OUTSTANDING_CONCEPTS, unit_candidates={"shares"}),
        submission_events,
    )

    income_events = _build_ttm_net_income_events(net_income_facts)
    equity_events = _build_equity_events(equity_facts)
    shares_events = _build_point_in_time_events(shares_facts, "shares_outstanding")

    aligned_income = _align_events_to_prices(prices, income_events, ["net_income_ttm"])
    aligned_equity = _align_events_to_prices(prices, equity_events, ["book_equity", "avg_book_equity"])
    aligned_shares = _align_events_to_prices(prices, shares_events, ["shares_outstanding"])

    fundamentals = prices[["date"]].copy()
    fundamentals["ticker"] = str(ticker).upper().strip()
    fundamentals["net_income_ttm"] = pd.to_numeric(aligned_income["net_income_ttm"], errors="coerce")
    fundamentals["book_equity"] = pd.to_numeric(aligned_equity["book_equity"], errors="coerce")
    fundamentals["avg_book_equity"] = pd.to_numeric(aligned_equity["avg_book_equity"], errors="coerce")
    fundamentals["shares_outstanding"] = pd.to_numeric(aligned_shares["shares_outstanding"], errors="coerce")
    fundamentals["market_cap"] = prices["close"] * fundamentals["shares_outstanding"]
    fundamentals["pe"] = fundamentals["market_cap"] / fundamentals["net_income_ttm"].where(fundamentals["net_income_ttm"] > 0)
    fundamentals["pb"] = fundamentals["market_cap"] / fundamentals["book_equity"].where(fundamentals["book_equity"] > 0)
    fundamentals["roe"] = fundamentals["net_income_ttm"] / fundamentals["avg_book_equity"].where(fundamentals["avg_book_equity"] != 0)

    return clean_fundamentals_data(fundamentals[["date", "ticker", "pe", "pb", "roe"]])


def build_sec_fundamentals_dataset(
    price_data: pd.DataFrame,
    ticker_mapping: pd.DataFrame,
    submissions_zip_path: str | Path,
    companyfacts_zip_path: str | Path,
) -> pd.DataFrame:
    """Build a standard fundamentals table for all tickers present in the price data."""
    if price_data.empty or ticker_mapping.empty:
        return pd.DataFrame(columns=["date", "ticker", "pe", "pb", "roe"])

    prices = price_data.copy()
    prices["ticker"] = prices["ticker"].astype("string").str.upper().str.strip()
    mapping = ticker_mapping.copy()
    mapping["ticker"] = mapping["ticker"].astype("string").str.upper().str.strip()

    rows: list[pd.DataFrame] = []
    for ticker in sorted(prices["ticker"].dropna().unique()):
        mapping_row = mapping[mapping["ticker"] == ticker]
        if mapping_row.empty:
            continue

        cik = int(mapping_row.iloc[0]["cik"])
        try:
            submissions_member = _find_zip_member(submissions_zip_path, cik)
            companyfacts_member = _find_zip_member(companyfacts_zip_path, cik)
            submissions_json = read_json_from_zip(submissions_zip_path, submissions_member)
            companyfacts_json = read_json_from_zip(companyfacts_zip_path, companyfacts_member)
        except FileNotFoundError:
            continue

        ticker_prices = prices[prices["ticker"] == ticker][["date", "close"]].copy()
        rows.append(build_sec_fundamentals_for_ticker(ticker, ticker_prices, companyfacts_json, submissions_json))

    if not rows:
        return pd.DataFrame(columns=["date", "ticker", "pe", "pb", "roe"])

    return pd.concat(rows, ignore_index=True)


def prepare_sec_fundamentals_data(
    project_root: Path | None = None,
    mapping_path: str = "data/raw/sec_source/company_tickers.json",
    submissions_zip_path: str = "data/raw/sec_source/submissions.zip",
    companyfacts_zip_path: str = "data/raw/sec_source/companyfacts.zip",
) -> dict[str, Path]:
    """Build and write the project's standard fundamentals table from SEC raw source data."""
    project_root = project_root or Path(__file__).resolve().parents[2]
    paths_config = _load_paths_config(project_root)

    price_path = project_root / paths_config["market_data"]
    fundamentals_path = project_root / paths_config["fundamentals_data"]

    prices = pd.read_parquet(price_path) if price_path.exists() else pd.DataFrame(columns=["date", "ticker", "close"])
    ticker_mapping = load_sec_ticker_mapping(project_root / mapping_path)
    fundamentals = build_sec_fundamentals_dataset(
        price_data=prices,
        ticker_mapping=ticker_mapping,
        submissions_zip_path=project_root / submissions_zip_path,
        companyfacts_zip_path=project_root / companyfacts_zip_path,
    )

    fundamentals_path.parent.mkdir(parents=True, exist_ok=True)
    fundamentals.to_parquet(fundamentals_path, index=False)
    return {"fundamentals_data": fundamentals_path}
