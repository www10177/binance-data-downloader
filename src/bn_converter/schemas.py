from collections import OrderedDict

import polars as pl

# Schema for klines
KLINES_SCHEMA = OrderedDict(
    {
        "OpenTime": pl.Int64,
        "Open": pl.Utf8,
        "High": pl.Utf8,
        "Low": pl.Utf8,
        "Close": pl.Utf8,
        "Volume": pl.Utf8,
        "CloseTime": pl.Int64,
        "QuoteVolume": pl.Utf8,
        "Count": pl.UInt64,
        "TakerBuyVolume": pl.Utf8,
        "TakerBuyQuoteVolume": pl.Utf8,
        "Ignore": pl.UInt8,
    }
)

# Schema for aggTrades
AGGTRADES_SCHEMA = OrderedDict(
    {
        "AggTradeId": pl.UInt64,
        "Price": pl.Utf8,
        "Quantity": pl.Utf8,
        "FirstTradeId": pl.UInt64,
        "LastTradeId": pl.UInt64,
        "TransactTime": pl.UInt64,
        "IsBuyerMaker": pl.Boolean,
    }
)

# Schema for bookDepth
BOOKDEPTH_SCHEMA = OrderedDict(
    {
        "Timestamp": pl.String,
        "Percentage": pl.Int8,
        "Depth": pl.Utf8,
        "Notional": pl.Utf8,
    }
)

# Schema for metrics
METRICS_SCHEMA = OrderedDict(
    {
        "CreateTime": pl.String,
        "Symbol": pl.String,
        "SumOpenInterest": pl.Utf8,
        "SumOpenInterestValue": pl.Utf8,
        "CountToptraderLongShortRatio": pl.Utf8,
        "SumToptraderLongShortRatio": pl.Utf8,
        "CountLongShortRatio": pl.Utf8,
        "SumTakerLongShortVolRatio": pl.Utf8,
    }
)

INDEX_PRICE_KLINES_SCHEMA = OrderedDict(
    {
        "OpenTime": pl.Int64,
        "Open": pl.Utf8,
        "High": pl.Utf8,
        "Low": pl.Utf8,
        "Close": pl.Utf8,
        "Volume": pl.Utf8,
        "CloseTime": pl.Int64,
        "QuoteVolume": pl.Utf8,
        "Count": pl.UInt64,
        "TakerBuyVolume": pl.Utf8,
        "TakerBuyQuoteVolume": pl.Utf8,
        "Ignore": pl.UInt8,
    }
)

SCHEMA = OrderedDict(
    {
        "klines": KLINES_SCHEMA,
        "aggTrades": AGGTRADES_SCHEMA,
        "bookDepth": BOOKDEPTH_SCHEMA,
        "metrics": METRICS_SCHEMA,
        "indexPriceKlines": INDEX_PRICE_KLINES_SCHEMA,
    }
)
