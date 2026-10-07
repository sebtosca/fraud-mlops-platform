"""Transaction data contract: one neutral column spec for the raw and silver layers.

The spec is library-neutral on purpose. `pandas_schema()` turns it into a pandera schema now,
and `spark_schema()` will do the same for PySpark in P8.T2, so both validate the same rules.
"""

from dataclasses import dataclass
from typing import Any

import pandas as pd
import pandera.pandas as pa

# The 14 Sparkov merchant categories (measured on fraudTrain.csv).
CATEGORIES: frozenset[str] = frozenset(
    {
        "entertainment",
        "food_dining",
        "gas_transport",
        "grocery_net",
        "grocery_pos",
        "health_fitness",
        "home",
        "kids_pets",
        "misc_net",
        "misc_pos",
        "personal_care",
        "shopping_net",
        "shopping_pos",
        "travel",
    }
)

# Upper bound for `amt` (PLAN P1.T5). Real max: $28,948.90 (2019), $22,768.11 (2020), both
# `travel`. The 1.8x covariate Shift only scales gas_transport and grocery_pos (max < $400).
MAX_AMOUNT = 50_000.0


# --- Neutral checks: each backend (pandera-pandas now, pandera-pyspark later) translates these.


@dataclass(frozen=True, slots=True)
class InRange:
    """Value is between `min_value` and `max_value`, both inclusive."""

    min_value: float
    max_value: float


@dataclass(frozen=True, slots=True)
class IsIn:
    """Value is one of `values`."""

    values: frozenset[Any]


@dataclass(frozen=True, slots=True)
class Matches:
    """The whole string matches the regex `pattern`."""

    pattern: str


Check = InRange | IsIn | Matches


@dataclass(frozen=True, slots=True)
class ColumnSpec:
    """One column of a contract, described without reference to any DataFrame library."""

    name: str
    pandas_dtype: str
    spark_dtype: str
    nullable: bool = False
    unique: bool = False
    checks: tuple[Check, ...] = ()


# --- Raw layer: the 23 Sparkov CSV columns exactly as `pandas.read_csv` loads them.
# Types only; cleaning and value checks happen in silver.

RAW_COLUMNS: tuple[ColumnSpec, ...] = (
    ColumnSpec("Unnamed: 0", "int64", "bigint"),  # unnamed row index; dropped in silver
    ColumnSpec("trans_date_trans_time", "str", "string"),
    ColumnSpec("cc_num", "int64", "bigint"),  # raw card number (PII); hashed in silver
    ColumnSpec("merchant", "str", "string"),
    ColumnSpec("category", "str", "string"),
    ColumnSpec("amt", "float64", "double"),
    ColumnSpec("first", "str", "string"),
    ColumnSpec("last", "str", "string"),
    ColumnSpec("gender", "str", "string"),
    ColumnSpec("street", "str", "string"),
    ColumnSpec("city", "str", "string"),
    ColumnSpec("state", "str", "string"),
    ColumnSpec("zip", "int64", "bigint"),
    ColumnSpec("lat", "float64", "double"),
    ColumnSpec("long", "float64", "double"),
    ColumnSpec("city_pop", "int64", "bigint"),
    ColumnSpec("job", "str", "string"),
    ColumnSpec("dob", "str", "string"),
    ColumnSpec("trans_num", "str", "string"),
    ColumnSpec("unix_time", "int64", "bigint"),  # ignored for time; it is shifted vs. the date
    ColumnSpec("merch_lat", "float64", "double"),
    ColumnSpec("merch_long", "float64", "double"),
    ColumnSpec("is_fraud", "int64", "bigint"),
)

# --- Silver layer: the clean columns written by the ingest job (P1.T7).

_LATITUDE = (InRange(-90.0, 90.0),)
_LONGITUDE = (InRange(-180.0, 180.0),)

SILVER_COLUMNS: tuple[ColumnSpec, ...] = (
    ColumnSpec("trans_num", "str", "string", unique=True, checks=(Matches(r"[0-9a-f]{32}"),)),
    ColumnSpec("card_hash", "str", "string"),  # HMAC of cc_num; the raw number never reaches silver
    ColumnSpec("event_time", "datetime64[ns]", "timestamp"),  # naive, treated as UTC
    ColumnSpec("category", "str", "string", checks=(IsIn(CATEGORIES),)),
    ColumnSpec("amt", "float64", "double", checks=(InRange(0.0, MAX_AMOUNT),)),
    ColumnSpec("merchant", "str", "string", checks=(Matches(r"(?!fraud_).*"),)),
    ColumnSpec("lat", "float64", "double", checks=_LATITUDE),
    ColumnSpec("long", "float64", "double", checks=_LONGITUDE),
    ColumnSpec("merch_lat", "float64", "double", checks=_LATITUDE),
    ColumnSpec("merch_long", "float64", "double", checks=_LONGITUDE),
    ColumnSpec("city_pop", "int64", "bigint", checks=(InRange(0, float("inf")),)),
    ColumnSpec("state", "str", "string"),
    ColumnSpec("job", "str", "string"),
    ColumnSpec("is_fraud", "int64", "bigint", checks=(IsIn(frozenset({0, 1})),)),
)


class SchemaSkew(Exception):
    """Data does not match its contract. `failures` holds one dict per failed check."""

    def __init__(self, layer: str, failures: list[dict[str, Any]]) -> None:
        super().__init__(f"{layer} data violates its contract: {len(failures)} failure(s)")
        self.layer = layer
        self.failures = failures


# --- Backends


def _to_pandera_check(check: Check) -> pa.Check:
    match check:
        case InRange(min_value=lo, max_value=hi):
            return pa.Check.in_range(lo, hi)
        case IsIn(values=v):
            return pa.Check.isin(v)
        case Matches(pattern=p):
            return pa.Check.str_matches(rf"(?:{p})\Z")
        case _:
            raise TypeError(f"Unknown check type {check!r}")


def pandas_schema(columns: tuple[ColumnSpec, ...]) -> pa.DataFrameSchema:
    """Build a strict pandera schema: no extra columns, and no silent type conversion."""
    return pa.DataFrameSchema(
        {
            spec.name: pa.Column(
                spec.pandas_dtype,
                checks=[_to_pandera_check(check) for check in spec.checks],
                nullable=spec.nullable,
                unique=spec.unique,
            )
            for spec in columns
        },
        strict=True,
        coerce=False,
    )


def validate(df: pd.DataFrame, columns: tuple[ColumnSpec, ...], layer: str) -> pd.DataFrame:
    """Return `df` if it matches the contract; otherwise raise `SchemaSkew` with every failure."""
    try:
        return pandas_schema(columns).validate(df, lazy=True)
    except pa.errors.SchemaErrors as e:
        raise SchemaSkew(layer, e.failure_cases.to_dict("records")) from e


def spark_schema(columns: tuple[ColumnSpec, ...]) -> None:
    """PySpark version of `pandas_schema`, built from the same specs."""
    raise NotImplementedError("P8.T2")
