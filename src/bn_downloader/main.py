import concurrent.futures
import hashlib
import os
import pathlib
import zipfile
from datetime import datetime, timedelta

import requests
import toml
import typer
from loguru import logger
from tqdm import tqdm

from .source import Binance

app = typer.Typer()
um_app = typer.Typer()
app.add_typer(um_app, name="UM")


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
    log_path = os.path.join(log_dir, "log_{time}.log")
    logger.add(log_path, rotation="10 MB")


setup_logging()


def download_file(url: str, dest_path: pathlib.Path):
    """Downloads a file from a URL to a destination path."""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(dest_path, "wb") as f:
            for data in response.iter_content(chunk_size=1024):
                f.write(data)
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading {url}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error during download: {e}")
        raise


def verify_and_unzip(
    zip_path: pathlib.Path, checksum_path: pathlib.Path, has_interval: bool
):
    """Verifies the checksum of a zip file, unzips it, and deletes the original files."""
    try:
        with open(checksum_path, "r") as f:
            expected_checksum = f.read().split()[0]

        sha256 = hashlib.sha256()
        with open(zip_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256.update(byte_block)
        calculated_checksum = sha256.hexdigest()

        if expected_checksum == calculated_checksum:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                extracted_file_name = zip_ref.namelist()[0]
                zip_ref.extractall(zip_path.parent)
                extracted_file_path = zip_path.parent / extracted_file_name
                new_file_name = (
                    "-".join(extracted_file_name.split("-")[:2]) + ".csv"
                    if has_interval
                    else f"{zip_path.stem}.csv"
                )
                new_file_path = zip_path.parent / new_file_name
                logger.info(f"Extracted {extracted_file_name} to {new_file_name}")
                os.rename(extracted_file_path, new_file_path)
            checksum_path.unlink()
            zip_path.unlink()
        else:
            logger.error(
                f"Checksum mismatch for {zip_path}. Expected {expected_checksum}, got {calculated_checksum}"
            )
            raise ValueError(f"Checksum mismatch for {zip_path}")
    except Exception as e:
        logger.error(f"Error during verification and unzipping: {e}")
        raise


def process_task(args):
    current_date, symbol, data_type, dest, config, source, pbar = args
    try:
        date_str_url = current_date.strftime("%Y-%m-%d")
        year, month, day = (
            current_date.strftime("%Y"),
            current_date.strftime("%m"),
            current_date.strftime("%d"),
        )

        interval = config["interval"]
        base_url_prefix = source.get_base_url()

        if data_type in ["premiumIndexKlines", "indexPriceKlines", "klines"]:
            base_url = f"{base_url_prefix}/{data_type}/{symbol}/{interval}/"
            file_name_zip = f"{symbol}-{interval}-{date_str_url}.zip"
            has_inverval = True
        else:
            base_url = f"{base_url_prefix}/{data_type}/{symbol}/"
            file_name_zip = f"{symbol}-{data_type}-{date_str_url}.zip"
            has_inverval = False

        file_name_checksum = f"{file_name_zip}.CHECKSUM"

        url_zip = f"{base_url}{file_name_zip}"
        url_checksum = f"{base_url}{file_name_checksum}"

        dest_dir = pathlib.Path(dest) / source.value / year / month / day / data_type
        dest_dir.mkdir(parents=True, exist_ok=True)

        dest_path_zip = dest_dir / f"{symbol}.zip"
        dest_path_checksum = dest_dir / f"{symbol}.zip.CHECKSUM"

        download_file(url_zip, dest_path_zip)
        download_file(url_checksum, dest_path_checksum)
        verify_and_unzip(dest_path_zip, dest_path_checksum, has_inverval)
    except Exception:
        logger.error(f"Failed to download data for {symbol} on {date_str_url}")
    finally:
        pbar.update(1)


@um_app.command()
def download(start_date: str, end_date: str | None, max_workers: int, source: Binance):
    """
    Downloads book depth data for a given symbol and date range.
    """
    if end_date is None:
        end_date = start_date

    config = load_config()
    DEST = config.get("DEST")
    symbols = config.get("symbols", [])

    data_types = config.get("data_types", [])

    if not DEST or not symbols or not data_types:
        logger.error("DEST, symbols, or data_types not set in config.toml.")
        raise typer.Exit(code=1)

    try:
        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")
    except ValueError:
        logger.error("Invalid date format. Please use YYYYMMDD.")
        raise typer.Exit(code=1)

    delta = end - start

    tasks = []
    for i in range(delta.days + 1):
        current_date = end - timedelta(days=i)
        for symbol in symbols:
            for data_type in data_types:
                tasks.append((current_date, symbol, data_type, DEST, config, source))

    with tqdm(total=len(tasks), desc="Downloading data") as pbar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Pass pbar to each task
            tasks_with_pbar = [task + (pbar,) for task in tasks]
            executor.map(process_task, tasks_with_pbar)
