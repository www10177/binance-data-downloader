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
# Download data for specific date range
bn download --start-date YYYYMMDD --end-date YYYYMMDD --max-workers 4

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

**Downloader Module (`src/bn_downloader/`)**
- `main.py`: Core downloading logic with concurrent processing
- Downloads data from `https://data.binance.vision/data/futures/um/daily/`
- Handles file verification using SHA256 checksums
- Organizes files by date structure: `DEST/YYYY/MM/DD/data_type/symbol.csv`

**Converter Module (`src/bn_converter/`)**
- `conv.py`: Handles CSV to Parquet conversion with schema enforcement
- `schemas.py`: Defines Polars schemas for different data types (klines, aggTrades, bookDepth, metrics)
- Converts column names from snake_case to PascalCase
- Applies data type casting and decimal precision inference

### Data Flow
1. **Download**: Raw data downloaded as ZIP files, verified, extracted to CSV
2. **Convert**: CSV files processed with schemas, column names converted to PascalCase, saved as Parquet
3. **Storage**: Files organized in hierarchical date structure under configured DEST directory

### Configuration
Uses `config.toml` in project root:
- `DEST`: Destination directory for downloaded data
- `symbols`: List of trading pairs to download (e.g., ["BTCUSDT", "ETHUSDT"])
- `data_types`: Available types include bookDepth, bookTicker, trades, metrics, premiumIndexKlines, indexPriceKlines, klines, aggTrades
- `interval`: Time interval for kline-related data types (e.g., "1m")

### Data Types Supported
- **klines**: OHLCV candlestick data with timestamp conversion
- **aggTrades**: Aggregated trade data with transaction timestamps
- **bookDepth**: Order book depth data with pivot transformation
- **metrics**: Various market metrics with timestamp parsing
- **indexPriceKlines**: Index price klines with timestamp handling
- **trades**, **bookTicker**, **premiumIndexKlines**: Standard data formats

### Key Features
- **Concurrent Downloads**: Uses ThreadPoolExecutor for parallel downloading
- **Data Integrity**: SHA256 checksum verification for all downloaded files
- **Schema Enforcement**: Automatic type casting using predefined schemas
- **Column Standardization**: Converts snake_case to PascalCase for consistency
- **Decimal Precision**: Automatic decimal scale inference for numeric string columns
- **Migration Support**: Can upgrade existing parquet files to new naming conventions

### Logging
- Uses Loguru for structured logging
- Logs stored in `./logs/` directory with rotation (10MB files)
- Configured in both main modules for comprehensive error tracking

### Dependencies
- **uv**: Package management and virtual environment
- **polars**: High-performance DataFrame library for data processing
- **typer**: Modern CLI framework
- **requests**: HTTP client for data downloads
- **loguru**: Advanced logging
- **tqdm**: Progress bars for long-running operations