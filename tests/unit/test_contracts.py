from collections.abc import Callable
from pathlib import Path

import pandas as pd
import pytest

from fraud.contracts.transactions import (
    CATEGORIES,
    RAW_COLUMNS,
    SILVER_COLUMNS,
    SchemaSkew,
    spark_schema,
    validate,
)

RAW_TRAIN_CSV = Path(__file__).resolve().parents[2] / "data" / "raw" / "fraudTrain.csv"


@pytest.fixture
def valid_silver() -> pd.DataFrame:
    """Two clean silver rows: one legit, one fraud."""
    return pd.DataFrame(
        {
            "trans_num": ["0b242abb623afc578575680df30655b9", "1f76529f8574734946361c461b024d99"],
            "card_hash": ["a" * 64, "b" * 64],
            "event_time": pd.to_datetime(["2019-01-01 00:00:18", "2019-01-01 00:00:44"]),
            "category": ["misc_net", "grocery_pos"],
            "amt": [4.97, 107.23],
            "merchant": ["Rippin, Kub and Mann", "Heller, Gutmann and Zieme"],
            "lat": [36.0788, 48.8878],
            "long": [-81.1781, -118.2105],
            "merch_lat": [36.011293, 49.159047],
            "merch_long": [-82.048315, -118.186462],
            "city_pop": [3495, 149],
            "state": ["NC", "WA"],
            "job": ["Psychologist, counselling", "Special educational needs teacher"],
            "is_fraud": [0, 1],
        }
    )


def failed_columns(error: SchemaSkew) -> set[str]:
    return {str(failure["column"]) for failure in error.failures}


def test_valid_silver_passes(valid_silver: pd.DataFrame) -> None:
    result = validate(valid_silver, SILVER_COLUMNS, "silver")

    pd.testing.assert_frame_equal(result, valid_silver)


def test_every_category_is_accepted(valid_silver: pd.DataFrame) -> None:
    df = pd.concat([valid_silver.iloc[[0]]] * len(CATEGORIES), ignore_index=True)
    df["category"] = sorted(CATEGORIES)
    df["trans_num"] = [f"{i:032x}" for i in range(len(df))]

    validate(df, SILVER_COLUMNS, "silver")


# --- The P1.T5 "Done when" cases, plus the other silver rules.
# Each case breaks one thing in a copy of the valid frame.


def _wrong_dtype(df: pd.DataFrame) -> None:
    df["amt"] = df["amt"].astype(str)


def _unknown_category(df: pd.DataFrame) -> None:
    df.loc[0, "category"] = "crypto"


def _negative_amount(df: pd.DataFrame) -> None:
    df.loc[0, "amt"] = -5.0


def _amount_too_large(df: pd.DataFrame) -> None:
    df.loc[0, "amt"] = 50_000.01


def _merchant_with_prefix(df: pd.DataFrame) -> None:
    df.loc[0, "merchant"] = "fraud_Rippin, Kub and Mann"


def _label_not_binary(df: pd.DataFrame) -> None:
    df.loc[0, "is_fraud"] = 2


def _duplicate_trans_num(df: pd.DataFrame) -> None:
    df.loc[1, "trans_num"] = df.loc[0, "trans_num"]


def _trans_num_too_long(df: pd.DataFrame) -> None:
    df.loc[0, "trans_num"] = "0" * 33


def _latitude_out_of_range(df: pd.DataFrame) -> None:
    df.loc[0, "lat"] = 91.0


def _null_in_required_column(df: pd.DataFrame) -> None:
    df.loc[0, "state"] = None


@pytest.mark.parametrize(
    ("break_frame", "column"),
    [
        (_wrong_dtype, "amt"),
        (_unknown_category, "category"),
        (_negative_amount, "amt"),
        (_amount_too_large, "amt"),
        (_merchant_with_prefix, "merchant"),
        (_label_not_binary, "is_fraud"),
        (_duplicate_trans_num, "trans_num"),
        (_trans_num_too_long, "trans_num"),
        (_latitude_out_of_range, "lat"),
        (_null_in_required_column, "state"),
    ],
)
def test_invalid_silver_raises_schema_skew(
    valid_silver: pd.DataFrame,
    break_frame: Callable[[pd.DataFrame], None],
    column: str,
) -> None:
    df = valid_silver.copy()
    break_frame(df)

    with pytest.raises(SchemaSkew) as exc_info:
        validate(df, SILVER_COLUMNS, "silver")

    assert exc_info.value.layer == "silver"
    assert column in failed_columns(exc_info.value)


def test_extra_column_raises_schema_skew(valid_silver: pd.DataFrame) -> None:
    df = valid_silver.assign(foo=1)

    with pytest.raises(SchemaSkew):
        validate(df, SILVER_COLUMNS, "silver")


def test_missing_column_raises_schema_skew(valid_silver: pd.DataFrame) -> None:
    df = valid_silver.drop(columns="card_hash")

    with pytest.raises(SchemaSkew):
        validate(df, SILVER_COLUMNS, "silver")


def test_all_failures_are_reported_together(valid_silver: pd.DataFrame) -> None:
    df = valid_silver.copy()
    _unknown_category(df)
    _negative_amount(df)
    _merchant_with_prefix(df)

    with pytest.raises(SchemaSkew) as exc_info:
        validate(df, SILVER_COLUMNS, "silver")

    assert {"category", "amt", "merchant"} <= failed_columns(exc_info.value)


def test_spark_schema_is_not_implemented_yet() -> None:
    with pytest.raises(NotImplementedError, match=r"P8\.T2"):
        spark_schema(SILVER_COLUMNS)


@pytest.mark.skipif(not RAW_TRAIN_CSV.exists(), reason="Sparkov dataset not downloaded")
def test_real_raw_csv_matches_raw_contract() -> None:
    df = pd.read_csv(RAW_TRAIN_CSV, nrows=10_000)

    validate(df, RAW_COLUMNS, "raw")
