from datetime import datetime
import os

import dateutil.parser
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import (
    StockBarsRequest,
)
from alpaca.data.timeframe import TimeFrame
from alpaca.trading import TradingClient, GetAssetsRequest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from stock import Stock, Base, Dividends

import contextlib

from stock_data.dividend_annoucements import get_dividend_announcements

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


def fill_dividend_data(session, start, end):
    announcements = (
        a
        for a in get_dividend_announcements(start)
        if dateutil.parser.parse(a["ex_dividend_date"]).date() <= end.date()
    )
    for announcement in announcements:
        if "declaration_date" not in announcement:
            declaration_date = announcement["ex_dividend_date"]
        else:
            declaration_date = announcement["declaration_date"]

        if "pay_date" not in announcement:
            announcement["pay_date"] = announcement["ex_dividend_date"]

        dividend = Dividends(
            symbol=announcement["ticker"],
            ex_dividend_date=announcement["ex_dividend_date"],
            pay_date=announcement["pay_date"],
            record_date=announcement["record_date"],
            declared_date=declaration_date,
            cash_amount=announcement["cash_amount"],
            currency=announcement["currency"],
            frequency=announcement["frequency"] or "unknown",
        )
        session.add(dividend)
        session.commit()


if __name__ == "__main__":
    with open_session() as session:
        alpaca_client = TradingClient(**alpaca_creds, paper=False)
        request = GetAssetsRequest(asset_status="active", asset_class="us_equity")
        assets = alpaca_client.get_all_assets(request)
        symbols = {asset.symbol for asset in assets if asset.tradable}

        start = datetime(2021 - 5, 1, 1)
        end = datetime(2021, 5, 3)
        fill_stock_data(session, symbols, start, end)
        fill_dividend_data(session, start, end)
