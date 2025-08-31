import os
import typer
from loguru import logger
from dotenv import load_dotenv
import requests
from datetime import datetime
import pathlib

load_dotenv()

DEST = os.getenv("DEST")

app = typer.Typer()

def download_file(url: str, dest_path: pathlib.Path):
    """Downloads a file from a URL to a destination path."""
    logger.info(f"Downloading {url} to {dest_path}")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.success(f"Successfully downloaded {url}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading {url}: {e}")
        raise

@app.command()
def download(
    date: str = typer.Option(
        datetime.now().strftime("%Y-%m-%d"),
        help="Date in YYYY-MM-DD format",
    ),
    symbol: str = typer.Option("BTCUSDT", help="Crypto symbol"),
):
    """
    Downloads book depth data for a given symbol and date.
    """
    if not DEST:
        logger.error("DEST environment variable not set. Please create a .env file.")
        raise typer.Exit(code=1)

    year, month, day = date.split("-")
    base_url = f"https://data.binance.vision/data/futures/um/daily/bookDepth/{symbol}/"
    file_name_zip = f"{symbol}-bookDepth-{date}.zip"
    file_name_checksum = f"{file_name_zip}.CHECKSUM"

    url_zip = f"{base_url}{file_name_zip}"
    url_checksum = f"{base_url}{file_name_checksum}"

    dest_dir = pathlib.Path(DEST) / year / month / day / "bookDepth"
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest_path_zip = dest_dir / f"{symbol}.zip"
    dest_path_checksum = dest_dir / f"{symbol}.zip.CHECKSUM"

    try:
        download_file(url_zip, dest_path_zip)
        download_file(url_checksum, dest_path_checksum)
    except Exception:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
