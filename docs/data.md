# Data: the Sparkov dataset

The platform trains on 2019 and replays 2020 from the Sparkov synthetic card-transaction dataset (ADR-0001). The data is never committed: `data/` is in `.gitignore`.

## Source
- Kaggle: [`kartik2112/fraud-detection`](https://www.kaggle.com/datasets/kartik2112/fraud-detection)
- Licence: CC0 (public domain). Generated with [Sparkov](https://github.com/namebrandon/Sparkov_Data_Generation).
- Two files: `fraudTrain.csv` (2019-01-01 to 2020-06-21) and `fraudTest.csv` (2020-06-21 to 2020-12-31), 23 columns each. The ingest job (P1.T7) concatenates them and splits by year itself.
- Names, addresses, dates of birth and card numbers are Faker-generated look-alikes. They are still handled as PII: card numbers are hashed at ingest and never logged.

## Download
1. Sign in to Kaggle.
2. Download the dataset, either:
   - **in the browser:** open the link above, click **Download**, then
     ```sh
     unzip ~/Downloads/archive.zip -d data/raw/
     ```
   - **or with the Kaggle CLI** (needs an API token from kaggle.com → Settings → API, saved at `~/.kaggle/kaggle.json`):
     ```sh
     uvx kaggle datasets download -d kartik2112/fraud-detection -p data/raw --unzip
     ```
3. Keep the file names as they are: the ingest job expects `data/raw/fraudTrain.csv` and `data/raw/fraudTest.csv`.
4. Delete the zip once the checks below pass.

## Check
```sh
wc -l data/raw/fraudTrain.csv data/raw/fraudTest.csv
```

| File | `wc -l` (with header) | Data rows |
|---|---|---|
| `fraudTrain.csv` | 1,296,676 | 1,296,675 |
| `fraudTest.csv` | 555,720 | 555,719 |

Both files should also pass the raw contract:
```sh
uv run python -c "
import pandas as pd
from fraud.contracts.transactions import RAW_COLUMNS, validate
for f in ['fraudTrain', 'fraudTest']:
    validate(pd.read_csv(f'data/raw/{f}.csv'), RAW_COLUMNS, 'raw'); print(f, 'OK')
"
```

> **Warning: truncated and altered mirrors.** Several copies of this dataset on Kaggle, Hugging Face and GitHub are cut short, or store `cc_num` as a float (which loses digits). If the row counts differ, or the contract reports `cc_num` as `float64`, download again from the Kaggle page above.

## Test fixture
`tests/fixtures/sparkov_sample.csv` is a committed sample of about 2,800 real rows, so tests and CI can use real data without the 500 MB download. It holds whole card histories rather than scattered rows:
- 12 fraud episodes (6 in 2019, 6 in 2020), each with every Transaction of that card from 14 days before its first fraud to 1 day after its last;
- 40 other cards, with every Transaction in January 1–14 of 2019 and of 2020.

It covers all 14 categories and both years. To regenerate it (deterministic for a given seed):
```sh
uv run python scripts/make_fixture.py            # default --seed 42
```
`tests/unit/test_fixture.py` checks the result.
