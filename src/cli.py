import typer
from bn_downloader.main import download
from bn_converter.conv import convert

app = typer.Typer()

@app.command("download")
def cli_download(
    start_date: str = typer.Option(..., "--start-date", "-s", help="Start date in YYYYMMDD format"),
    end_date: str = typer.Option(None, "--end-date", "-e", help="End date in YYYYMMDD format (defaults to start_date)"),
    max_workers: int = typer.Option(4, "--max-workers", "-w", help="Number of worker threads to use."),
):
    download(start_date, end_date, max_workers)

@app.command("convert")
def cli_convert(
    start_date: str = typer.Option(..., "--start-date", "-s", help="Start date (YYYYMMDD)."),
    end_date: str = typer.Option(None, "--end-date", "-e", help="End date (YYYYMMDD)."),
    symbol: str = typer.Option(None, "--symbol", help="Trading symbol (e.g., BTCUSDT). If omitted, convert all symbols."),
    data_type: str = typer.Option(None, "--type", help="Data type (e.g., trades, aggTrades, klines, etc.). If omitted, convert all types."),
    rm: bool = typer.Option(False, "--rm", help="Remove CSV files after successful conversion."),
):
    convert(start_date, end_date, symbol, data_type, rm)

if __name__ == "__main__":
    app()
