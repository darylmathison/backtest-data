from datetime import datetime
import os

import dateutil.parser
import requests
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import (
    StockBarsRequest,
)
from business_calendar import Calendar, MO, TU, WE, TH, FR
from alpaca.data.timeframe import TimeFrame
from alpaca.trading import TradingClient, GetAssetsRequest, GetCalendarRequest
from retry_reloaded import retry
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from urllib3.exceptions import ReadTimeoutError

from stock_data.models import Stock, Base, Dividends, Holidays

import contextlib

from stock_data.dividend_annoucements import get_dividend_announcements

alpaca_creds = {
    "api_key": os.getenv("ALPACA_API_KEY"),
    "secret_key": os.getenv("ALPACA_SECRET_KEY"),
}


@contextlib.contextmanager
def open_session():
    dbsession = None
    try:
        password = os.getenv("DB_PASSWORD")
        db_url = f"postgresql://postgres:{password}@localhost:5432/stock_data"
        engine = create_engine(db_url)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine, expire_on_commit=False)
        dbsession = Session()
        yield dbsession
    finally:
        if dbsession:
            dbsession.close()


def find_existing_stocks(dbsession):
    return {stocks[0] for stocks in dbsession.query(Stock.symbol).distinct().all()}


def find_latest_ex_dividend_date(dbsession):
    ret = (
        dbsession.query(Dividends.ex_dividend_date)
        .order_by(Dividends.ex_dividend_date.desc())
        .first()
    )
    if ret is None:
        return datetime(1970, 1, 1).date()
    return ret[0]


def fill_stock_data(dbsession, symbol, start, end, timeframe=TimeFrame.Day):
    def populate_stock_data(symbol, start, end, timeframe):
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
            dbsession.add(stock)
            dbsession.commit()

    client = StockHistoricalDataClient(
        **alpaca_creds
    )  # make sure the API key is set in the environment
    if isinstance(symbol, (list, tuple, set)):
        for s in symbol:
            populate_stock_data(s, start, end, timeframe)
    else:
        populate_stock_data(symbol, start, end, timeframe)


@retry((ReadTimeoutError,))
def fill_dividend_data(dbsession, start, end):
    latest_date = find_latest_ex_dividend_date(dbsession)
    query_date = max(latest_date, start.date())
    for announcement in get_dividend_announcements(query_date):
        if dateutil.parser.parse(announcement["ex_dividend_date"]).date() > end.date():
            break

        if "declaration_date" not in announcement:
            declaration_date = announcement["ex_dividend_date"]
        else:
            declaration_date = announcement["declaration_date"]

        if "pay_date" not in announcement:
            announcement["pay_date"] = announcement["ex_dividend_date"]

        if "currency" not in announcement:
            announcement["currency"] = "USD"

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
        dbsession.add(dividend)
        dbsession.commit()


@retry((requests.exceptions.ConnectionError,))
def initial_fill_stocks(start, end):
    with open_session() as dbsession:
        alpaca_client = TradingClient(**alpaca_creds, paper=False)
        request = GetAssetsRequest(asset_status="active", asset_class="us_equity")
        assets = alpaca_client.get_all_assets(request)
        symbols = {asset.symbol for asset in assets if asset.tradable}
        current_symbols = find_existing_stocks(dbsession)
        symbols = symbols - current_symbols

        fill_stock_data(dbsession, symbols, start, end)


def fill_holidays(start, end):
    alpaca_client = TradingClient(**alpaca_creds, paper=False)
    calendar = Calendar(workdays=[MO, TU, WE, TH, FR])
    with open_session() as dbsession:
        calendar_request = GetCalendarRequest(start=start, end=end)
        market_days = {d.date for d in alpaca_client.get_calendar(calendar_request)}
        all_days = {d.date() for d in calendar.range(start, end)}
        holidays = all_days.difference(market_days)
        for holiday in holidays:
            dbsession.add(Holidays(date=holiday))
        dbsession.commit()


if __name__ == "__main__":
    end = datetime.now()
    end = datetime(end.year, end.month, end.day)
    years_back = 5
    start = datetime(end.year - years_back, 1, 1)
    initial_fill_stocks(start, end)

    with open_session() as dbsession:
        fill_dividend_data(dbsession, start, end)
    fill_holidays(start, end)
