import glob
import os
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
from tqdm import tqdm

from .schemas import SCHEMA


def snake_to_pascal(snake_str: str) -> str:
    """Convert snake_case string to PascalCase."""
    if snake_str[0].islower():
        components = snake_str.split("_")
        return "".join(word.capitalize() for word in components)
    else:
        return snake_str


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


def setup_logging():
    config = load_config()
    log_dir = config.get("LOG_DIR", "./logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "log_{time:YYYY-MM-DD}.log")
    logger.add(log_path, rotation="10 MB")


setup_logging()

config = load_config()

DATA_DIR = Path(config["DEST"])
logger.info(f"Using data directory: {DATA_DIR}")


def find_all_symbols_and_types(start_date, end_date):
    """Find all (symbol, data_type) pairs in the data directory for the date range."""
    found = set()
    for d in DATA_DIR.glob("**/*.csv"):
        try:
            parts = d.parts
            yyyy = int(parts[-5])
            mm = int(parts[-4])
            dd = int(parts[-3])
            file_date = date(yyyy, mm, dd)
            if start_date <= file_date <= end_date:
                data_type = parts[-2]
                symbol = d.stem  # filename without .csv
                found.add((symbol, data_type))
        except Exception:
            continue
    return sorted(found)


def find_csv_files(symbol, data_type, start_date, end_date):
    """Finds CSV files for the symbol and data_type between start_date and end_date in DATA_DIR."""
    files = []
    pattern = "**/"
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


def convert(
    start_date: str,
    end_date: Optional[str] = None,
    symbol: Optional[str] = None,
    data_type: Optional[str] = None,
    rm: bool = False,
):
    """
    Converts CSV files in the data directory for a symbol, data type, and date range to Parquet file(s).
    If symbol or data_type is omitted, convert all found in the date range.
    """
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
        if symbol:
            jobs = [j for j in jobs if j[0] == symbol]
        if data_type:
            jobs = [j for j in jobs if j[1] == data_type]

    if not jobs:
        logger.error("No matching symbol/data_type pairs found for the given range.")
        raise typer.Exit(code=1)

    failed_files = []
    total_files = 0

    for sym, dtype in jobs:
        csv_files = find_csv_files(sym, dtype, start_dt, end_dt)
        if not csv_files:
            logger.warning(
                f"No CSV files found for {sym} {dtype} between {start_dt} and {end_dt} in {DATA_DIR}"
            )
            continue
        logger.info(
            f"Found {len(csv_files)} CSV files for {sym} {dtype} from {start_date} to {end_date}."
        )
        total_files += len(csv_files)
        for csv_file in csv_files:
            try:
                # Read CSV without schema first to get original column names
                df = pl.read_csv(csv_file, try_parse_dates=True)

                # Convert column names from snake_case to PascalCase
                column_mapping = {col: snake_to_pascal(col) for col in df.columns}
                df = df.rename(column_mapping)

                # Apply schema if available (now with PascalCase column names)
                schema = SCHEMA.get(dtype, None)
                if schema:
                    try:
                        # Cast columns to match schema types
                        for col_name, col_type in schema.items():
                            if col_name in df.columns:
                                df = df.with_columns(pl.col(col_name).cast(col_type))
                    except Exception as e:
                        logger.warning(f"Could not apply schema types for {dtype}: {e}")
                # Guess decimal scale for Utf8 columns
                for col_name in df.columns:
                    if df[col_name].dtype == pl.Utf8:
                        try:
                            sample = df[col_name].drop_nulls().head(10).to_list()
                            if all(
                                isinstance(x, str) and x.replace(".", "", 1).isdigit()
                                for x in sample
                            ):
                                max_scale = max(
                                    (len(x.split(".")[-1]) if "." in x else 0)
                                    for x in sample
                                )
                                scale = max_scale if max_scale > 0 else 8
                                df = df.with_columns(
                                    pl.col(col_name).cast(
                                        pl.Decimal(precision=None, scale=scale)
                                    )
                                )
                                logger.info(
                                    f"Guessed Decimal scale={scale} for column '{col_name}' from sample data."
                                )
                        except Exception as e:
                            logger.warning(
                                f"Could not guess decimal for column '{col_name}': {e}"
                            )
                output_file = csv_file.with_suffix(".parquet")
                logger.info(f"Writing Parquet file to: {output_file}")

                match dtype:
                    case "klines":
                        # Convert OpenTime and CloseTime to datetime
                        df = df.with_columns(
                            pl.col("OpenTime").cast(pl.Datetime("ms")),
                            pl.col("CloseTime").cast(pl.Datetime("ms")),
                        )
                    case "aggTrades":
                        # Convert TransactTime to datetime
                        df = df.with_columns(
                            pl.col("TransactTime").cast(pl.Datetime("ms"))
                        )
                    case "bookDepth":
                        # Convert Timestamp to datetime
                        df = df.with_columns(
                            pl.col("Timestamp").str.strptime(
                                pl.Datetime, format="%Y-%m-%d %H:%M:%S%.f"
                            )
                        )
                        df = df.pivot(
                            values=["Depth", "Notional"],
                            index=["Timestamp"],
                            columns="Percentage",
                        )
                    case "metrics":
                        # Convert CreateTime to datetime
                        df = df.with_columns(
                            pl.col("CreateTime").str.strptime(
                                pl.Datetime, format="%Y-%m-%d %H:%M:%S%.f"
                            )
                        )
                    case "indexPriceKlines":
                        df = df.with_columns(
                            pl.col("OpenTime").cast(pl.Datetime("ms")),
                            pl.col("CloseTime").cast(pl.Datetime("ms")),
                        )
                    case _:
                        pass

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
                failed_files.append(str(csv_file))

    # Check if any conversions failed
    if failed_files:
        logger.error(
            f"{len(failed_files)} out of {total_files} file conversions failed."
        )
        raise typer.Exit(code=1)
    else:
        logger.info(f"All {total_files} files converted successfully.")


def migrate():
    """
    Migrate all existing parquet files in DEST directory to convert column names from snake_case to PascalCase.
    """
    parquet_files = list(DATA_DIR.glob("**/*.parquet"))

    if not parquet_files:
        logger.info("No parquet files found to migrate.")
        return

    logger.info(f"Found {len(parquet_files)} parquet files to migrate.")

    for parquet_file in tqdm(parquet_files, desc="Migrating parquet files"):
        tqdm.set_description(f"Processing: {os.path.basename(parquet_file)}")
        try:
            # Read the existing parquet file
            df = pl.read_parquet(parquet_file)

            # Convert column names from snake_case to PascalCase
            column_mapping = {col: snake_to_pascal(col) for col in df.columns}

            # Check if any columns actually changed
            needs_conversion = any(
                original != converted for original, converted in column_mapping.items()
            )

            if needs_conversion:
                df = df.rename(column_mapping, strict=False)

            rename = dict(Quantity="Qty", TransactTime="TxnTime")
            needs_rename = any(original in df.columns for original in rename.keys())

            if needs_rename:
                df = df.rename(rename, strict=False)

            if needs_conversion or needs_rename:
                # Write back to the same file
                df.write_parquet(parquet_file)

        except Exception as e:
            logger.error(f"Failed to migrate {parquet_file}: {e}")

    logger.info("Migration completed!")
