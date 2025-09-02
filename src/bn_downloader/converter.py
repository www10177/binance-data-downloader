from datetime import date, datetime
from pathlib import Path
from typing import Optional

import polars as pl
import requests
import toml
import typer
from loguru import logger

from bn_converter.schemas import SCHEMA

app = typer.Typer()

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
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching exchange info from Binance: {e}")
        return None


def get_symbol_precision(exchange_info, symbol):
    if not exchange_info:
        return None, None
    for s in exchange_info["symbols"]:
        if s["symbol"] == symbol:
            return s.get("pricePrecision"), s.get("quantityPrecision")
    return None, None


def find_all_symbols_and_types(start_date, end_date):
    found = set()
    for d in DATA_DIR.glob("*/*/*/*/*.csv"):
        try:
            parts = d.parts
            yyyy = int(parts[-5])
            mm = int(parts[-4])
            dd = int(parts[-3])
            file_date = date(yyyy, mm, dd)
            if start_date <= file_date <= end_date:
                data_type = parts[-2]
                symbol = d.stem
                found.add((symbol, data_type))
        except Exception:
            continue
    return sorted(found)


def find_csv_files(symbol, data_type, start_date, end_date):
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


def read_csv_with_preview(csv_file):
    with open(csv_file, "r") as f:
        preview = [next(f) for _ in range(2)]
    logger.info(f"Preview of {csv_file}:\n{''.join(preview)}")
    df = pl.read_csv(csv_file, try_parse_dates=True)
    df = df.with_columns([pl.col(col).cast(pl.Utf8) for col in df.columns])
    return df


def convert(
    start_date: str,
    end_date: Optional[str] = None,
    symbol: Optional[str] = None,
    data_type: Optional[str] = None,
    rm: bool = False,
):
    try:
        start_dt = datetime.strptime(start_date, "%Y%m%d").date()
        end_dt = (
            datetime.strptime(end_date, "%Y%m%d").date()
            if end_date is not None
            else start_dt
        )
        if symbol and data_type:
            jobs = [(symbol, data_type)]
        else:
            jobs = find_all_symbols_and_types(start_dt, end_dt)
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
            exchange_info = get_exchange_info()
            price_precision, qty_precision = get_symbol_precision(exchange_info, sym)
            for csv_file in csv_files:
                df = read_csv_with_preview(csv_file)
                for col_name in df.columns:
                    if col_name in PRICE_COLS + QTY_COLS:
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
                        try:
                            df = df.with_columns(
                                pl.col(col_name).cast(
                                    pl.Decimal(precision=None, scale=scale)
                                )
                            )
                        except Exception as e:
                            logger.warning(
                                f"Could not cast column '{col_name}' to Decimal: {e}"
                            )
                output_file = csv_file.with_suffix(".parquet")
                logger.info(f"Writing Parquet file to: {output_file}")
                df.write_parquet(output_file)
                logger.info(f"Conversion successful for {csv_file.name}.")
                if rm:
                    try:
                        csv_file.unlink()
                        logger.info(f"Removed {csv_file}")
                    except Exception as e:
                        logger.warning(f"Failed to remove {csv_file}: {e}")
    except Exception as e:
        logger.exception(f"An error occurred during conversion: {e}")
        raise typer.Exit(code=1)
