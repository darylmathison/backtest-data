import logging
from datetime import datetime
import os

import dateutil.parser
import requests

from business_calendar import Calendar, MO, TU, WE, TH, FR
from alpaca.data.timeframe import TimeFrame
from alpaca.trading import TradingClient, GetAssetsRequest, GetCalendarRequest
from retry_reloaded import retry
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from urllib3.exceptions import ReadTimeoutError

from stock_data.models import Stock, Base, Dividends, Holidays, MarketDays, Assets
from stock_data.stock_downloads import download_stock_data
import contextlib

from stock_data.dividend_annoucements import get_dividend_announcements

from psycopg2.errors import UniqueViolation

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


def find_existing_stocks(dbsession, start=None, end=None):
    if start is not None and end is not None:
        return {
            stocks[0]
            for stocks in dbsession.query(Stock.symbol).filter(
                Stock.date.between(start, end).distinct().all()
            )
        }
    return {stocks[0] for stocks in dbsession.query(Stock.symbol).distinct().all()}


def does_this_announcement_exist(dbsession, date, symbol):
    return (
        dbsession.query(Dividends)
        .filter(Dividends.symbol == symbol, Dividends.ex_dividend_date == date)
        .first()
    )


def does_this_bar_exist(dbsession, date: datetime.date, symbol: str):
    return (
        dbsession.query(Stock)
        .filter(Stock.symbol == symbol, Stock.date == date)
        .first()
    )


def has_this_bar_been_downloaded(dbsession, symbol: str, date: datetime.date):
    asset = (
        dbsession.query(Assets)
        .filter(
            Assets.symbol == symbol, Assets.market_days.any(MarketDays.date == date)
        )
        .first()
    )
    if asset:
        return True
    return False


def mark_stock_as_downloaded(dbsession, symbol, date):
    asset = dbsession.query(Assets).filter(Assets.symbol == symbol).first()
    market_day = dbsession.query(MarketDays).filter(MarketDays.date == date).first()
    if asset is None:
        new_asset = Assets(symbol=symbol, downloaded=True)
        new_asset.market_days.append(market_day)
        dbsession.add(new_asset)
    else:
        asset.downloaded = True
        asset.market_days.append(market_day)
        dbsession.add(asset)
    dbsession.commit()


def fill_stock_data(dbsession, symbol, start, end, timeframe=TimeFrame.Day):
    def populate_stock_data(symbol, start, end, timeframe):
        stock_data = download_stock_data(symbol, start, end, timeframe)
        if stock_data:
            for stock in stock_data:
                if not does_this_bar_exist(dbsession, stock.date, stock.symbol):
                    dbsession.add(stock)
                    dbsession.commit()
                mark_stock_as_downloaded(dbsession, stock.symbol, stock.date.date())

    if isinstance(symbol, (list, tuple, set)):
        for s in symbol:
            populate_stock_data(s, start, end, timeframe)
    else:
        populate_stock_data(symbol, start, end, timeframe)


@retry((ReadTimeoutError,))
def fill_dividend_data(dbsession, start, end):
    for announcement in get_dividend_announcements(start):
        if dateutil.parser.parse(announcement["ex_dividend_date"]).date() > end.date():
            break
        try:
            if "declaration_date" not in announcement:
                declaration_date = announcement["ex_dividend_date"]
            else:
                declaration_date = announcement["declaration_date"]

            if "pay_date" not in announcement:
                announcement["pay_date"] = announcement["ex_dividend_date"]

            if "currency" not in announcement:
                announcement["currency"] = "None"

            if not does_this_announcement_exist(
                dbsession, announcement["ex_dividend_date"], announcement["ticker"]
            ):
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
        except UniqueViolation as e:
            logging.warning("Duplicate entry: %s", e)


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
    request_start = datetime(start.year - 1, 1, 1)
    alpaca_client = TradingClient(**alpaca_creds, paper=False)
    calendar = Calendar(workdays=[MO, TU, WE, TH, FR])
    with open_session() as dbsession:
        previous_holidays = {
            d[0]
            for d in dbsession.query(Holidays.date)
            .filter(Holidays.date.between(request_start, end))
            .all()
        }
        calendar_request = GetCalendarRequest(start=request_start.date(), end=end)
        market_days = {d.date for d in alpaca_client.get_calendar(calendar_request)}
        all_days = {d.date() for d in calendar.range(request_start, end)}
        holidays = all_days.difference(market_days) - previous_holidays
        for holiday in holidays:
            dbsession.add(Holidays(date=holiday))
        dbsession.commit()


def fill_market_days(start, end):
    request_start = datetime(start.year - 1, 1, 1)
    alpaca_client = TradingClient(**alpaca_creds, paper=False)
    with open_session() as dbsession:
        existing_market_days = {
            d[0]
            for d in dbsession.query(MarketDays.date)
            .filter(MarketDays.date.between(request_start.date(), end))
            .all()
        }
        calendar_request = GetCalendarRequest(start=request_start.date(), end=end)
        market_days = {d.date for d in alpaca_client.get_calendar(calendar_request)}
        for day in market_days:
            if day not in existing_market_days:
                dbsession.add(MarketDays(date=day))
        dbsession.commit()


if __name__ == "__main__":
    end = datetime.now()
    end = datetime(end.year, end.month, end.day)
    years_back = 10
    start = datetime(end.year - years_back, 1, 1)

    fill_market_days(start, end)
    fill_holidays(start, end)
    initial_fill_stocks(start, end)
    with open_session() as dbsession:
        fill_dividend_data(dbsession, start, end)
