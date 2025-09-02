import typer

from .conv import convert

app = typer.Typer()


@app.command()
def cli_convert(
    start_date: str = typer.Option(
        ..., "--start-date", "-s", help="Start date (YYYYMMDD)."
    ),
    end_date: str = typer.Option(None, "--end-date", "-e", help="End date (YYYYMMDD)."),
    symbol: str = typer.Option(
        None,
        "--symbol",
        help="Trading symbol (e.g., BTCUSDT). If omitted, convert all symbols.",
    ),
    data_type: str = typer.Option(
        None,
        "--type",
        help="Data type (e.g., trades, aggTrades, klines, etc.). If omitted, convert all types.",
    ),
    rm: bool = typer.Option(
        False, "--rm", help="Remove CSV files after successful conversion."
    ),
):
    convert(start_date, end_date, symbol, data_type, rm)


if __name__ == "__main__":
    app()
