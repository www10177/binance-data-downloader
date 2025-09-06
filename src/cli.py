from typing import Optional

import typer

from bn_converter.conv import convert, migrate
from bn_downloader.main import Binance, download

app = typer.Typer()


@app.command("download")
def cli_download(
    start_date: str = typer.Option(
        ..., "--start-date", "-s", help="Start date in YYYYMMDD format"
    ),
    end_date: Optional[str] = typer.Option(
        None,
        "--end-date",
        "-e",
        help="End date in YYYYMMDD format (defaults to start_date)",
    ),
    max_workers: int = typer.Option(
        4, "--max-workers", "-w", help="Number of worker threads to use."
    ),
    source: Binance = typer.Argument(
        help="data source ",
    ),
):
    download(start_date, end_date, max_workers, source)


@app.command("convert")
def cli_convert(
    start_date: str = typer.Option(
        ..., "--start-date", "-s", help="Start date (YYYYMMDD)."
    ),
    end_date: Optional[str] = typer.Option(
        None, "--end-date", "-e", help="End date (YYYYMMDD)."
    ),
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
    print(start_date, end_date, symbol, data_type, rm)
    convert(start_date, end_date, symbol, data_type, rm)


@app.command("migrate")
def cli_migrate():
    """Migrate all existing parquet files to PascalCase column names."""
    migrate()


if __name__ == "__main__":
    app()
