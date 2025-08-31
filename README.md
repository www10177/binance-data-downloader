# Binance Data Downloader

A simple command-line tool to download cryptocurrency data from the Binance public data endpoint.

## Installation

1.  Clone the repository.
2.  Install the dependencies using `uv`:

    ```bash
    uv pip install -r requirements.txt
    ```

## Configuration

1.  Create a `config.toml` file in the root of the project.
2.  Add the following content to the file:

    ```toml
    DEST = "/path/to/your/data/folder"
    symbols = ["BTCUSDT", "ETHUSDT"]
    data_types = ["bookDepth"]
    ```

    -   `DEST`: The destination folder where the data will be saved.
    -   `symbols`: A list of symbols to download.
    -   `data_types`: A list of data types to download. The available data types are: `bookDepth`, `bookTicker`, `trades`, `metrics`, `premiumIndexKlines`, `indexPriceKlines`.

## Usage

To download the data, run the following command:

```bash
bn-downloader UM download --start-date YYYYMMDD --end-date YYYYMMDD
```

-   `--start-date`: The start date in `YYYYMMDD` format.
-   `--end-date`: The end date in `YYYYMMDD` format. If not provided, it will be the same as the start date.

### Example

```bash
bn-downloader UM download --start-date 20250801 --end-date 20250805
```

This will download the `bookDepth` data for `BTCUSDT` and `ETHUSDT` from August 1st, 2025 to August 5th, 2025.
