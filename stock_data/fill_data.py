import logging
from datetime import datetime
import os
from typing import Type

import dateutil.parser
import requests
import yfinance as yf

from business_calendar import Calendar, MO, TU, WE, TH, FR
from alpaca.data.timeframe import TimeFrame
from alpaca.trading import TradingClient, GetAssetsRequest, GetCalendarRequest
from dateutil.relativedelta import relativedelta
from retry_reloaded import retry
from sqlalchemy import create_engine, or_, and_, not_
from sqlalchemy.orm import sessionmaker
from urllib3.exceptions import ReadTimeoutError

from stock_data import polygon_client
from stock_data.models import Stock, Base, Dividends, Holidays, MarketDays, Assets
from stock_data.stock_downloads import download_stock_data
import contextlib

from stock_data.polygon_client import get_dividend_announcements

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
        new_asset = Assets(symbol=symbol)
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


def months_from_date_to_now(date):
    now = datetime.now().date()
    r = relativedelta(now, date)
    return r.years * 12 + r.months


@retry((ReadTimeoutError,))
def fill_dividend_data(dbsession, start, end, assets: list[Type[Assets]]):
    for asset in assets:
        logging.info("Downloading %s", asset.symbol)
        asset_dividend_init = False
        for announcement in get_dividend_announcements(asset.symbol, start):
            if not asset.dividend and not asset_dividend_init:
                if "frequency" in announcement and (
                    announcement["frequency"] is not None
                    or announcement["frequency"] != 0
                ):
                    asset.dividend = True
                    number_of_months = months_from_date_to_now(asset.start_date)
                    asset.min_num_events = int(
                        number_of_months / (float(announcement["frequency"]) / 12.0)
                    )
                    logging.info(
                        "Expecting to download %s events from %s months and %s frequency",
                        asset.min_num_events,
                        number_of_months,
                        announcement["frequency"],
                    )
                    asset_dividend_init = True
                    dbsession.add(asset)

            if dateutil.parser.parse(announcement["ex_dividend_date"]).date() > end:
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
        if asset.dividend:
            asset_announcements = (
                dbsession.query(Dividends.symbol)
                .filter(Dividends.symbol == asset.symbol)
                .all()
            )
            asset.percentage_downloaded = float(
                len(asset_announcements) / asset.min_num_events
            )
            dbsession.add(asset)
        asset.dividend_checked = True
        dbsession.add(asset)
        dbsession.commit()


def find_start_date(asset: Assets, start: datetime.date):
    try:
        yahoo_asset = yf.Ticker(asset.symbol).history(period="max")
        if not yahoo_asset.empty:
            return max(yahoo_asset.index[0].date(), start)
        else:
            ticker_info = polygon_client.ticker_info(asset.symbol)
            if "listdate" in ticker_info:
                return max(dateutil.parser.parse(ticker_info["listdate"]).date(), start)
            else:
                return start
    except Exception as e:
        logging.error("Error finding start date for %s: %s", asset.symbol, e)
    finally:
        return start


def fill_assets(dbsession, start: datetime.date):
    request = GetAssetsRequest(asset_status="active", asset_class="us_equity")
    alpaca_client = TradingClient(**alpaca_creds, paper=False)
    alpaca_assets = alpaca_client.get_all_assets(request)
    already_loaded = set(row[0] for row in dbsession.query(Assets.symbol).all())
    assets = (
        Assets(symbol=asset.symbol)
        for asset in alpaca_assets
        if asset.tradable and asset.marginable
    )
    assets_to_load = (asset for asset in assets if asset.symbol not in already_loaded)
    for asset in assets_to_load:
        logging.info("Adding %s", asset.symbol)
        asset.start_date = find_start_date(asset, start)
        dbsession.add(asset)
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
    end = datetime(end.year, end.month, end.day).date()
    years_back = 10
    start = datetime(end.year - years_back, 1, 1).date()
    with open_session() as dbsession:
        fill_assets(dbsession, start)
        dividend_assets = (
            dbsession.query(Assets)
            .filter(not_(Assets.dividend_checked))
            .order_by(Assets.symbol)
            .all()
        )
        fill_dividend_data(dbsession, start, end, dividend_assets)
    # fill_market_days(start, end)
    # fill_holidays(start, end)
    # initial_fill_stocks(start, end)
    # with open_session() as dbsession:
    #     fill_dividend_data(dbsession, start, end)
