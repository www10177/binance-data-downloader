import polars as pl

# Schema for klines
KLINES_SCHEMA = {
    "open_time": pl.Int64,
    "open": pl.Utf8,
    "high": pl.Utf8,
    "low": pl.Utf8,
    "close": pl.Utf8,
    "volume": pl.Utf8,
    "close_time": pl.Int64,
    "quote_volume": pl.Utf8,
    "count": pl.UInt64,
    "taker_buy_volume": pl.Utf8,
    "taker_buy_quote_volume": pl.Utf8,
    "ignore": pl.UInt8,
}

# Schema for aggTrades
AGGTRADES_SCHEMA = {
    "agg_trade_id": pl.UInt64,
    "price": pl.Utf8,
    "quantity": pl.Utf8,
    "first_trade_id": pl.UInt64,
    "last_trade_id": pl.UInt64,
    "transact_time": pl.UInt64,
    "is_buyer_maker": pl.Boolean,
}

# Schema for bookDepth
BOOKDEPTH_SCHEMA = {
    "timestamp": pl.String,
    "percentage": pl.Int8,
    "depth": pl.Utf8,
    "notional": pl.Utf8,
}

# Schema for metrics
METRICS_SCHEMA = {
    "create_time": pl.String,
    "symbol": pl.String,
    "sum_open_interest": pl.Utf8,
    "sum_open_interest_value": pl.Utf8,
    "count_toptrader_long_short_ratio": pl.Utf8,
    "sum_toptrader_long_short_ratio": pl.Utf8,
    "count_long_short_ratio": pl.Utf8,
    "sum_taker_long_short_vol_ratio": pl.Utf8,
}

SCHEMA = {
    "klines": KLINES_SCHEMA,
    "aggTrades": AGGTRADES_SCHEMA,
    "bookDepth": BOOKDEPTH_SCHEMA,
    "metrics": METRICS_SCHEMA,
}
