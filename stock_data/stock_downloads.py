import datetime
import os

import yfinance
from alpaca.data import StockHistoricalDataClient, TimeFrame, StockBarsRequest
from retry_reloaded import retry
from urllib3.exceptions import ReadTimeoutError
from requests.exceptions import ReadTimeout

from stock_data.models import Stock
import stock_data as sd

alpaca_creds = {
    "api_key": os.getenv("ALPACA_API_KEY"),
    "secret_key": os.getenv("ALPACA_SECRET_KEY"),
}


@retry((ReadTimeout,))
def pull_from_alpaca(
    symbol: str, start: datetime.date, end: datetime.date, timeframe: TimeFrame
) -> list[Stock]:
    client = StockHistoricalDataClient(
        **alpaca_creds
    )  # make sure the API key is set in the environment
    bars_request = StockBarsRequest(
        symbol_or_symbols=symbol, start=start, end=end, timeframe=timeframe
    )
    bars = client.get_stock_bars(bars_request)
    if not bars[symbol]:
        return None
    return [
        Stock(
            symbol=symbol,
            date=bar.timestamp,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            trade_count=bar.trade_count,
            dividend=False,
        )
        for bar in bars[symbol]
    ]


@retry((ReadTimeoutError,))
def pull_from_yahoo(symbol, start, end, timeframe) -> list[Stock]:
    yahoo_timeframes = {
        str(TimeFrame.Day): "1d",
        str(TimeFrame.Minute): "1m",
        str(TimeFrame.Hour): "1h",
    }
    data = yfinance.download(
        symbol, start=start, end=end, interval=yahoo_timeframes[str(timeframe)]
    )
    return [
        Stock(
            symbol=symbol,
            date=date,
            open=float(data.loc[date, "Open"]),
            high=float(data.loc[date, "High"]),
            low=float(data.loc[date, "Low"]),
            close=float(data.loc[date, "Close"]),
            volume=float(data.loc[date, "Volume"]),
            trade_count=0,
            dividend=False,
        )
        for date in data.index
    ]


downloaders = [pull_from_yahoo, pull_from_alpaca]


def download_stock_data(symbol, start, end, timeframe):
    calendar = sd.create_calendar()
    request_end = calendar.addbusdays(end, 1)
    for downloader in downloaders:
        stock_data = downloader(symbol, start, request_end, timeframe)
        if stock_data:
            return stock_data
    return None
