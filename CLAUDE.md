# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Binance Data Downloader is a command-line tool for downloading and converting cryptocurrency data from Binance's public data endpoint. The project consists of two main components:
- **bn_downloader**: Downloads raw data files (CSV) from Binance Vision data archive
- **bn_converter**: Converts CSV files to optimized Parquet format with proper schemas and data types

## Development Commands

### Installation and Setup
```bash
# Install dependencies using uv
uv sync
```

### Main Commands
```bash
# Download futures data (UM - USD-M futures)
bn download --start-date YYYYMMDD --end-date YYYYMMDD --max-workers 4 um

# Download spot data
bn download --start-date YYYYMMDD --end-date YYYYMMDD --max-workers 4 spot

# Skip download if file already exists in any format (.zip, .csv, .parquet)
bn download --start-date YYYYMMDD --end-date YYYYMMDD --skip-existed um

# Convert CSV files to Parquet format
bn convert --start-date YYYYMMDD --end-date YYYYMMDD --symbol BTCUSDT --type trades --rm

# Migrate existing parquet files to PascalCase column naming
bn migrate
```

### Running the CLI
The main CLI entry point is defined in `pyproject.toml` as `bn = "cli:app"`, which runs the Typer app from `src/cli.py`.

## Architecture

### Core Components

**CLI Layer (`src/cli.py`)**
- Uses Typer for command-line interface
- Delegates to main modules: `bn_downloader.main.download`, `bn_converter.conv.convert`, and `bn_converter.conv.migrate`
- Three main commands: `download`, `convert`, `migrate`
- Download command requires a source argument: `um` (futures) or `spot`

**Downloader Module (`src/bn_downloader/`)**
- `main.py`: Core downloading logic with concurrent processing
- `source.py`: Defines Binance enum with UM and SPOT sources
- Downloads from either futures (`https://data.binance.vision/data/futures/um/daily/`) or spot (`https://data.binance.vision/data/spot/daily/`) endpoints
- Handles file verification using SHA256 checksums
- `skip_existed` option to skip downloads if files exist in any format
- Organizes files by date structure: `DEST/YYYY/MM/DD/data_type/symbol.csv`

**Converter Module (`src/bn_converter/`)**
- `conv.py`: Handles CSV to Parquet conversion with schema enforcement
- `schemas.py`: Defines Polars schemas for different data types (klines, aggTrades, bookDepth, metrics, indexPriceKlines, trades)
- Includes `INDEX_PRICE_KLINES_SCHEMA` for index price klines data
- Converts column names from snake_case to PascalCase
- Applies data type casting and decimal precision inference

### Data Flow
1. **Download**: Raw data downloaded as ZIP files, verified, extracted to CSV
2. **Convert**: CSV files processed with schemas, column names converted to PascalCase, saved as Parquet
3. **Storage**: Files organized in hierarchical date structure under configured DEST directory

### Configuration
Uses `config.toml` in project root:
- `DEST`: Destination directory for downloaded data (e.g., "/mnt/WD16T/crypto-data")
- `symbols`: List of futures trading pairs (includes 1000BONKUSDT, WLFIUSDT style futures symbols)
- `spot_symbols`: List of spot trading pairs (cleaned up futures-specific symbols like 1000BONKUSDT → BONKUSDT)
- `data_types`: Futures data types include bookDepth, trades, metrics, premiumIndexKlines, indexPriceKlines, klines, aggTrades
- `spot_data_types`: Spot data types include trades, aggTrades, klines
- `interval`: Time interval for kline-related data types (e.g., "1m")

### Data Types Supported
- **klines**: OHLCV candlestick data with timestamp conversion
- **aggTrades**: Aggregated trade data with transaction timestamps
- **bookDepth**: Order book depth data with pivot transformation
- **metrics**: Various market metrics with timestamp parsing
- **indexPriceKlines**: Index price klines with timestamp handling
- **trades**, **bookTicker**, **premiumIndexKlines**: Standard data formats

### Key Features
- **Multi-Source Support**: Supports both futures (UM) and spot data from Binance Vision
- **Conditional Downloads**: `skip_existed` option to avoid re-downloading existing files
- **Concurrent Downloads**: Uses ThreadPoolExecutor for parallel downloading
- **Data Integrity**: SHA256 checksum verification for all downloaded files
- **Schema Enforcement**: Automatic type casting using predefined schemas including INDEX_PRICE_KLINES_SCHEMA
- **Column Standardization**: Converts snake_case to PascalCase for consistency
- **Decimal Precision**: Automatic decimal scale inference for numeric string columns
- **Migration Support**: Can upgrade existing parquet files to new naming conventions

### Logging
- Uses Loguru for structured logging
- Logs stored in `./logs/` directory with rotation (10MB files)
- Configured in both main modules for comprehensive error tracking

### Dependencies
- **uv**: Package management and virtual environment
- **polars-lts-cpu**: High-performance DataFrame library for data processing (LTS CPU version)
- **pyarrow**: Arrow-based columnar in-memory analytics for Parquet file support
- **typer**: Modern CLI framework
- **requests**: HTTP client for data downloads
- **loguru**: Advanced logging
- **tqdm**: Progress bars for long-running operations
- **toml**: Configuration file parsing