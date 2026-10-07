"""Build tests/fixtures/sparkov_sample.csv: a small, real Sparkov sample for tests and CI.

Rows are chosen as whole card histories, not scattered rows, because later phases compute
per-card features from each card's history:
- fraud episodes: for a few fraud cards per year, every Transaction from 14 days before the
  card's first fraud to 1 day after its last fraud;
- background: for other cards, every Transaction in Jan 1-14 of 2019 and of 2020.

The sample is deterministic for a given seed. Run: uv run python scripts/make_fixture.py
"""

import argparse
from datetime import timedelta
from pathlib import Path

import pandas as pd

from fraud.contracts.transactions import CATEGORIES, RAW_COLUMNS, validate

ROOT = Path(__file__).resolve().parents[1]
EPISODE_CARDS_PER_YEAR = 6
BACKGROUND_CARDS = 40
# datetime.timedelta, not pd.Timedelta: pandas 2.3 + NumPy 2.5 warns on every pd.Timedelta.
HISTORY_BEFORE_FRAUD = timedelta(days=14)
AFTER_LAST_FRAUD = timedelta(days=1)
MIN_ROWS, MAX_ROWS = 2_000, 4_000


def load_raw(raw_dir: Path) -> pd.DataFrame:
    frames = [pd.read_csv(raw_dir / name) for name in ("fraudTrain.csv", "fraudTest.csv")]
    df = pd.concat(frames, ignore_index=True)
    validate(df, RAW_COLUMNS, "raw")
    return df


def episode_rows(df: pd.DataFrame, event_time: pd.Series, seed: int) -> pd.DataFrame:
    """Whole fraud episodes, with 14 days of history, for a few cards per year."""
    fraud = df.loc[df["is_fraud"] == 1, ["cc_num"]].assign(t=event_time)
    episodes = fraud.groupby("cc_num")["t"].agg(["min", "max"])
    # Skip the rare cards with fraud in both years, so each episode belongs to one year.
    one_year = episodes["min"].dt.year == episodes["max"].dt.year
    episodes = episodes[one_year]

    picked = pd.concat(
        episodes[episodes["min"].dt.year == year]
        .sort_index()
        .sample(EPISODE_CARDS_PER_YEAR, random_state=seed)
        for year in (2019, 2020)
    )
    parts: list[pd.DataFrame] = []
    for cc_num, (first, last) in picked.iterrows():
        in_window = event_time.between(first - HISTORY_BEFORE_FRAUD, last + AFTER_LAST_FRAUD)
        parts.append(df.loc[(df["cc_num"] == cc_num) & in_window])
    return pd.concat(parts)


def background_rows(
    df: pd.DataFrame, event_time: pd.Series, exclude: set[int], seed: int
) -> pd.DataFrame:
    """Every Transaction in Jan 1-14 of each year, for cards that are not episode cards."""
    early_january = (event_time.dt.month == 1) & (event_time.dt.day <= 14)
    candidates = pd.Series(sorted(set(df.loc[early_january, "cc_num"]) - exclude))
    cards = set(candidates.sample(BACKGROUND_CARDS, random_state=seed))
    rows: pd.DataFrame = df.loc[early_january & df["cc_num"].isin(cards)]
    return rows


def build_sample(df: pd.DataFrame, seed: int) -> pd.DataFrame:
    event_time = pd.to_datetime(df["trans_date_trans_time"])
    episodes = episode_rows(df, event_time, seed)
    background = background_rows(df, event_time, set(episodes["cc_num"]), seed)

    sample = pd.concat([episodes, background]).drop_duplicates("trans_num")
    order = pd.to_datetime(sample["trans_date_trans_time"]).rename("_t")
    return (
        sample.assign(_t=order)
        .sort_values(["_t", "trans_num"], kind="stable")
        .drop(columns="_t")
        .reset_index(drop=True)
    )


def check_sample(sample: pd.DataFrame) -> None:
    years = set(pd.to_datetime(sample["trans_date_trans_time"]).dt.year)
    problems = []
    if not MIN_ROWS <= len(sample) <= MAX_ROWS:
        problems.append(f"{len(sample)} rows, expected {MIN_ROWS}-{MAX_ROWS}")
    if missing := CATEGORIES - set(sample["category"]):
        problems.append(f"missing categories: {sorted(missing)}")
    if years != {2019, 2020}:
        problems.append(f"years present: {sorted(years)}")
    if sample["is_fraud"].sum() == 0:
        problems.append("no fraud rows")
    if problems:
        raise SystemExit("Sample rejected: " + "; ".join(problems))
    validate(sample, RAW_COLUMNS, "raw")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw-dir", type=Path, default=ROOT / "data" / "raw")
    parser.add_argument(
        "--out", type=Path, default=ROOT / "tests" / "fixtures" / "sparkov_sample.csv"
    )
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    sample = build_sample(load_raw(args.raw_dir), args.seed)
    check_sample(sample)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    sample.to_csv(args.out, index=False)

    fraud_cards = sample.loc[sample["is_fraud"] == 1, "cc_num"].nunique()
    print(
        f"Wrote {args.out.relative_to(ROOT)}: {len(sample)} rows, "
        f"{sample['cc_num'].nunique()} cards ({fraud_cards} with fraud), "
        f"{int(sample['is_fraud'].sum())} fraud rows, {args.out.stat().st_size / 1024:.0f} KiB"
    )


if __name__ == "__main__":
    main()
