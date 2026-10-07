from pathlib import Path

import pandas as pd
import pytest

from fraud.contracts.transactions import CATEGORIES, RAW_COLUMNS, validate

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "sparkov_sample.csv"


@pytest.fixture(scope="module")
def sample() -> pd.DataFrame:
    return pd.read_csv(FIXTURE)


def test_fixture_matches_raw_contract(sample: pd.DataFrame) -> None:
    validate(sample, RAW_COLUMNS, "raw")


def test_fixture_size_is_small_but_real(sample: pd.DataFrame) -> None:
    assert 2_000 <= len(sample) <= 4_000


def test_fixture_covers_every_category(sample: pd.DataFrame) -> None:
    assert set(sample["category"]) == CATEGORIES


def test_fixture_spans_both_years(sample: pd.DataFrame) -> None:
    years = set(pd.to_datetime(sample["trans_date_trans_time"]).dt.year)

    assert years == {2019, 2020}


def test_fixture_has_fraud_episodes_in_both_years(sample: pd.DataFrame) -> None:
    fraud = sample[sample["is_fraud"] == 1]
    fraud_years = set(pd.to_datetime(fraud["trans_date_trans_time"]).dt.year)

    assert fraud["cc_num"].nunique() >= 2
    assert fraud_years == {2019, 2020}


def test_fixture_is_sorted_by_event_time_with_unique_ids(sample: pd.DataFrame) -> None:
    assert pd.to_datetime(sample["trans_date_trans_time"]).is_monotonic_increasing
    assert sample["trans_num"].is_unique
