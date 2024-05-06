from datetime import datetime
import os
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import (
    StockBarsRequest,
)
from alpaca.data.timeframe import TimeFrame
from alpaca.trading import TradingClient, GetAssetsRequest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from stock import Stock, Base

import contextlib

alpaca_creds = {
    "api_key": os.getenv("ALPACA_API_KEY"),
    "secret_key": os.getenv("ALPACA_SECRET_KEY"),
}


@contextlib.contextmanager
def open_session():
    try:
        password = os.getenv("DB_PASSWORD")
        db_url = f"postgresql://postgres:{password}@localhost:5432/stock_data"
        engine = create_engine(db_url, echo=True)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine, expire_on_commit=False)
        session = Session()
        yield session
    finally:
        session.close()


def fill_stock_data(session, symbol, start, end, timeframe=TimeFrame.Day):
    def populate_stock_data(session, symbol, start, end, timeframe):
        bars_request = StockBarsRequest(
            symbol_or_symbols=symbol, start=start, end=end, timeframe=timeframe
        )
        bars = client.get_stock_bars(bars_request)
        for bar in bars[symbol]:
            stock = Stock(
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
            session.add(stock)
            session.commit()

    client = StockHistoricalDataClient(
        **alpaca_creds
    )  # make sure the API key is set in the environment
    if isinstance(symbol, (list, tuple, set)):
        for s in symbol:
            populate_stock_data(session, s, start, end, timeframe)
    else:
        populate_stock_data(session, symbol, start, end, timeframe)


def fill_dividend_data(session, symbol, start, end):
    pass


if __name__ == "__main__":
    with open_session() as session:
        alpaca_client = TradingClient(**alpaca_creds, paper=False)
        request = GetAssetsRequest(asset_status="active", asset_class="us_equity")
        assets = alpaca_client.get_all_assets(request)
        symbols = {asset.symbol for asset in assets if asset.tradable}

        start = datetime(2021 - 5, 1, 1)
        end = datetime(2021, 5, 3)
        fill_stock_data(session, symbols, start, end)
