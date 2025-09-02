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

from .schemas import SCHEMA

app = typer.Typer()


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


def find_all_symbols_and_types(start_date, end_date):
    """Find all (symbol, data_type) pairs in the data directory for the date range."""
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
                symbol = d.stem  # filename without .csv
                found.add((symbol, data_type))
        except Exception:
            continue
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
            for csv_file in csv_files:
                schema = SCHEMA.get(dtype, None)
                if schema:
                    try:
                        df = pl.read_csv(csv_file, try_parse_dates=True, schema=schema)
                    except Exception as e:
                        logger.warning(
                            f"Could not apply schema for {dtype}: {e}, falling back to default."
                        )
                        df = pl.read_csv(csv_file, try_parse_dates=True)
                else:
                    df = pl.read_csv(csv_file, try_parse_dates=True)
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
                        # Convert open_time and close_time to datetime
                        df = df.with_columns(
                            pl.col("open_time").cast(pl.Datetime("ms")),
                            pl.col("close_time").cast(pl.Datetime("ms")),
                        )
                    case "aggTrades":
                        # Convert transact_time to datetime
                        df = df.with_columns(
                            pl.col("transact_time").cast(pl.Datetime("ms"))
                        )
                    case "bookDepth":
                        # Convert timestamp to datetime
                        df = df.with_columns(
                            pl.col("timestamp").str.strptime(
                                pl.Datetime, format="%Y-%m-%d %H:%M:%S"
                            )
                        ).pivot(
                            values=["depth", "notional"],
                            index=["timestamp"],
                            columns="percentage",
                        )
                    case "metrics":
                        # Convert create_time to datetime
                        df = df.with_columns(
                            pl.col("create_time").str.strptime(
                                pl.Datetime, format="%Y-%m-%d %H:%M:%S"
                            )
                        )
                    case "indexPriceKlines":
                        df = df.with_columns(
                            pl.col("open_time").cast(pl.Datetime("ms")),
                            pl.col("close_time").cast(pl.Datetime("ms")),
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
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
