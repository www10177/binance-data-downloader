import typer
import polars as pl
from loguru import logger
import pathlib
import requests
import re

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


@app.command()
def convert(
    input_file: pathlib.Path = typer.Option(
        ...,
        "--input",
        "-i",
        help="Input CSV file path.",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
    ),
    output_file: pathlib.Path = typer.Option(
        ...,
        "--output",
        "-o",
        help="Output Parquet file path.",
        file_okay=True,
        dir_okay=False,
        writable=True,
        resolve_path=True,
    ),
):
    """
    Converts a CSV file to a Parquet file, casting float columns to decimal
    with precision fetched from Binance API.
    """
    try:
        # Extract symbol from filename (e.g., BTCUSDT-trades-2023-01-01.csv)
        match = re.match(r"^([A-Z]+)", input_file.name)
        if not match:
            logger.error(
                f"Could not extract symbol from filename: {input_file.name}. "
                "Filename must start with the symbol (e.g., 'BTCUSDT-')."
            )
            raise typer.Exit(code=1)

        symbol = match.group(1)
        logger.info(f"Extracted symbol '{symbol}' from filename.")

        exchange_info = get_exchange_info()
        price_precision, qty_precision = get_symbol_precision(exchange_info, symbol)

        if price_precision is None:
            logger.warning(
                f"Could not find precision info for symbol '{symbol}'. "
                "Using default decimal conversion."
            )

        logger.info(f"Reading CSV file: {input_file}")
        # Try to infer schema, but we will override it anyway
        df = pl.read_csv(input_file, try_parse_dates=True)

        for col_name in df.columns:
            # Check if column should be converted
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
                # Determine the scale for casting
                if col_name in PRICE_COLS and price_precision is not None:
                    scale = price_precision
                    logger.info(f"Casting price column '{col_name}' to Decimal(scale={scale})")
                elif col_name in QTY_COLS and qty_precision is not None:
                    scale = qty_precision
                    logger.info(f"Casting quantity column '{col_name}' to Decimal(scale={scale})")
                else:
                    # Fallback for columns not in PRICE_COLS/QTY_COLS or when API fails
                    scale = 8  # A reasonable default scale
                    logger.info(f"Using default scale for '{col_name}': scale={scale}")

                # Polars can infer precision if None. Scale is what matters for decimals.
                df = df.with_columns(
                    pl.col(col_name).cast(pl.Decimal(precision=None, scale=scale))
                )

        logger.info(f"Writing Parquet file to: {output_file}")
        df.write_parquet(output_file)
        logger.info("Conversion successful.")

    except Exception as e:
        logger.error(f"An error occurred during conversion: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
