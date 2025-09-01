import glob
import pathlib
import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import polars as pl
import requests
import toml
import typer
from loguru import logger

app = typer.Typer()

# Column names for different data types that may need conversion
PRICE_COLS = ["price", "open", "high", "low", "close"]
QTY_COLS = [
    "qty",
    "quantity",
    "volume",
    "quote_asset_volume",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
]


def load_config():
    """Loads the config.toml file."""
    try:
        with open("config.toml", "r") as f:
            return toml.load(f)
    except FileNotFoundError:
        logger.error(
            "config.toml not found. Please create one from config.toml.example."
        )
        raise typer.Exit(code=1)
    except Exception as e:
        logger.error(f"Error loading config.toml: {e}")
        raise typer.Exit(code=1)


config = load_config()

DATA_DIR = Path(config["DEST"])
logger.info(f"Using data directory: {DATA_DIR}")


def get_exchange_info():
    """Fetches exchange information from Binance Futures API."""
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching exchange info from Binance: {e}")
        return None


def get_symbol_precision(exchange_info, symbol):
    """Extracts price and quantity precision for a symbol."""
    if not exchange_info:
        return None, None

    for s in exchange_info["symbols"]:
        if s["symbol"] == symbol:
            return s.get("pricePrecision"), s.get("quantityPrecision")
    return None, None


def find_all_symbols_and_types(start_date, end_date):
    """Find all (symbol, data_type) pairs in the data directory for the date range."""
    found = set()
    for d in DATA_DIR.glob("*/*/*/*/*.csv"):
        try:
            parts = d.parts
            print(parts)
            yyyy = int(parts[-5])
            mm = int(parts[-4])
            dd = int(parts[-3])
            file_date = date(yyyy, mm, dd)
            print(file_date, start_date, end_date)
            if start_date <= file_date <= end_date:
                data_type = parts[-2]
                symbol = d.stem  # filename without .csv
                found.add((symbol, data_type))
        except Exception:
            continue
    print(found)
    return sorted(found)


def find_csv_files(symbol, data_type, start_date, end_date):
    """Finds CSV files for the symbol and data_type between start_date and end_date in DATA_DIR."""
    files = []
    pattern = "*/*/*/"
    if data_type:
        pattern += data_type + "/"
    else:
        pattern += "*/"

    if symbol:
        pattern += symbol + "*.csv"
    else:
        pattern += "*.csv"

    for d in DATA_DIR.glob(pattern):
        try:
            parts = d.parts
            yyyy = int(parts[-5])
            mm = int(parts[-4])
            dd = int(parts[-3])
            file_date = date(yyyy, mm, dd)
            if start_date <= file_date <= end_date:
                files.append((file_date, d))
        except Exception:
            continue
    files.sort()
    return [f[1] for f in files]


@app.command()
def convert(
    start_date: str = typer.Option(
        ..., "--start-date", "-s", help="Start date (YYYYMMDD)."
    ),
    end_date: str = typer.Option(None, "--end-date", "-e", help="End date (YYYYMMDD)."),
    symbol: Optional[str] = typer.Option(
        None,
        "--symbol",
        help="Trading symbol (e.g., BTCUSDT). If omitted, convert all symbols.",
    ),
    data_type: Optional[str] = typer.Option(
        None,
        "--type",
        help="Data type (e.g., trades, aggTrades, klines, etc.). If omitted, convert all types.",
    ),
    rm: bool = typer.Option(
        False, "--rm", help="Remove CSV files after successful conversion."
    ),
):
    """
    Converts CSV files in the data directory for a symbol, data type, and date range to Parquet file(s).
    If symbol or data_type is omitted, convert all found in the date range.
    """
    try:
        start_dt = datetime.strptime(start_date, "%Y%m%d").date()
        end_dt = (
            datetime.strptime(end_date, "%Y%m%d").date()
            if end_date is not None
            else start_dt
        )

        # Determine what to convert
        if symbol and data_type:
            jobs = [(symbol, data_type)]
        else:
            jobs = find_all_symbols_and_types(start_dt, end_dt)
            print(jobs)
            if symbol:
                jobs = [j for j in jobs if j[0] == symbol]
            if data_type:
                jobs = [j for j in jobs if j[1] == data_type]

        if not jobs:
            logger.error(
                "No matching symbol/data_type pairs found for the given range."
            )
            raise typer.Exit(code=1)

        for sym, dtype in jobs:
            csv_files = find_csv_files(sym, dtype, start_dt, end_dt)
            if not csv_files:
                logger.warning(
                    f"No CSV files found for {sym} {dtype} between {start_date} and {end_date} in {DATA_DIR}"
                )
                continue
            logger.info(
                f"Found {len(csv_files)} CSV files for {sym} {dtype} from {start_date} to {end_date}."
            )
            df = pl.concat([pl.read_csv(f, try_parse_dates=True) for f in csv_files])
            exchange_info = get_exchange_info()
            price_precision, qty_precision = get_symbol_precision(exchange_info, sym)
            for col_name in df.columns:
                if df[col_name].dtype in [pl.Float32, pl.Float64]:
                    scale = None
                    if col_name in PRICE_COLS and price_precision is not None:
                        scale = price_precision
                        logger.info(
                            f"Casting price column '{col_name}' to Decimal(scale={scale})"
                        )
                    elif col_name in QTY_COLS and qty_precision is not None:
                        scale = qty_precision
                        logger.info(
                            f"Casting quantity column '{col_name}' to Decimal(scale={scale})"
                        )
                    else:
                        scale = 8
                        logger.info(
                            f"Using default scale for '{col_name}': scale={scale}"
                        )
                    df = df.with_columns(
                        pl.col(col_name).cast(pl.Decimal(precision=None, scale=scale))
                    )
            output_file = DATA_DIR / f"{sym}-{dtype}-{start_date}_to_{end_date}.parquet"
            logger.info(f"Writing Parquet file to: {output_file}")
            df.write_parquet(output_file)
            logger.info(f"Conversion successful for {sym} {dtype}.")
            if rm:
                for f in csv_files:
                    try:
                        f.unlink()
                        logger.info(f"Removed {f}")
                    except Exception as e:
                        logger.warning(f"Failed to remove {f}: {e}")
    except Exception as e:
        logger.exception(f"An error occurred during conversion: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
