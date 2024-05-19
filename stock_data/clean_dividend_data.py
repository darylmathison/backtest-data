import datetime
import logging

import dateutil.parser
from retry_reloaded import retry
from sqlalchemy import and_, or_

from stock_data.models import Dividends, Assets
import stock_data.fill_data as fd
import collections
import stock_data.polygon_client as pc


def fix_high_percentage(dbsession, start_date, end_date):
    target_assets = (
        dbsession.query(Assets).filter(Assets.percentage_downloaded > 1.5).all()
    )
    for asset in target_assets:
        symbol = asset.symbol
        dividends = (
            dbsession.query(Dividends)
            .filter(Dividends.symbol == symbol)
            .order_by(Dividends.ex_dividend_date)
            .all()
        )
        if dividends:
            fd.fill_dividend_data(dbsession, symbol, start_date, end_date)
        dbsession.commit()


def fix_low_percentage(dbsession, start_date, end_date):
    target_assets = (
        dbsession.query(Assets).filter(Assets.percentage_downloaded < 0.5).all()
    )
    for asset in target_assets:
        symbol = asset.symbol
        dividends = (
            dbsession.query(Dividends)
            .filter(Dividends.symbol == symbol)
            .order_by(Dividends.ex_dividend_date)
            .all()
        )
        if dividends:
            last_dividend = dividends[-1].ex_dividend_date
            if last_dividend < end_date:
                fd.fill_dividend_data(dbsession, symbol, start_date, end_date)
        dbsession.commit()


def fix_bad_percentage(dbsession, start_date, end_date):
    target_assets = (
        dbsession.query(Assets)
        .filter(
            and_(
                Assets.dividend,
                Assets.min_num_events > 0,
                or_(
                    Assets.percentage_downloaded > 1.5,
                    Assets.percentage_downloaded < 0.5,
                ),
            )
        )
        .all()
    )
    for asset in target_assets:
        asset.min_num_events = 0
        asset.percentage_downloaded = 0.0
        asset.dividend = False
    fd.fill_dividend_data(dbsession, start_date, end_date, target_assets)
    dbsession.commit()


def fix_negative_min_events(dbsession, start_date, end_date):
    target_assets = dbsession.query(Assets).filter(Assets.min_num_events < 0).all()
    dbsession.query(Dividends).filter(
        Dividends.symbol.in_([asset.symbol for asset in target_assets])
    ).delete()
    fd.fill_dividend_data(dbsession, start_date, end_date, target_assets)
    dbsession.commit()


@retry((ConnectionError,))
def reload_low_percentage(dbsession, start_date, end_date):
    target_assets = (
        dbsession.query(Assets)
        .filter(and_(Assets.dividend, Assets.percentage_downloaded < 0.5))
        .all()
    )

    for asset in target_assets:
        logging.info(f"Reloading {asset.symbol}")
        ticker_start_date = fd.get_ticker_start_date(asset, start_date)
        asset.start_date = ticker_start_date
        dbsession.add(asset)
        dbsession.commit()

        total_months = fd.months_from_date_to_now(asset.start_date)
        symbol = asset.symbol
        dividends = (
            dbsession.query(Dividends)
            .filter(Dividends.symbol == symbol)
            .order_by(Dividends.ex_dividend_date)
            .all()
        )
        if dividends:
            length = len(dividends)
            frequency_candidates = set(
                map(
                    lambda x: float(x.frequency),
                    filter(
                        lambda x: x.frequency != "-1" and x.dividend_type == "CD",
                        dividends,
                    ),
                )
            )
            frequency = next((f for f in frequency_candidates), None)
            if len(frequency_candidates) == 1:
                asset.min_num_events = fd.calulate_num_event(total_months, frequency)
                if asset.min_num_events == 0 and length > 0:
                    asset.min_num_events = length
                    asset.percentage_downloaded = 1.0
                else:
                    asset.percentage_downloaded = length / asset.min_num_events
                dbsession.add(asset)
                dbsession.commit()
            elif len(frequency_candidates) > 1:
                print(f"Frequency too many found for {symbol}, {frequency_candidates}")
            else:
                print(f"Frequency not found for {symbol}")


def relook_at_too_many_dividend_types(dbsession, start_date, end_date):
    with open("../notebook/too_many.txt") as too_many_file:
        symbols = [l.strip() for l in too_many_file.readlines()]
        assets = dbsession.query(Assets).filter(Assets.symbol.in_(symbols)).all()
        for asset in assets:
            logging.info(f"Reloading {asset.symbol}")
            ticker_start_date = fd.get_ticker_start_date(asset, start_date)
            logging.info(f"ticker_start_date: {ticker_start_date}")
            asset.start_date = ticker_start_date
            total_months = fd.months_from_date_to_now(asset.start_date)
            dividends = asset.dividends
            if dividends:
                length = len(dividends)
                frequency_counter = collections.Counter(
                    map(
                        lambda x: x.frequency,
                        filter(
                            lambda x: x.frequency != "-1" and x.dividend_type == "CD",
                            dividends,
                        ),
                    )
                )
                frequency = float(frequency_counter.most_common(1)[0][0])
                asset.min_num_events = fd.calulate_num_event(total_months, frequency)
                if asset.min_num_events == 0 and length > 0:
                    asset.min_num_events = length
                    asset.percentage_downloaded = 1.0
                else:
                    asset.percentage_downloaded = length / asset.min_num_events
                dbsession.add(asset)
                dbsession.commit()


def main():
    years_back = 10
    end_date = datetime.datetime.now().date()
    start_date = datetime.date(year=end_date.year - years_back, month=1, day=1)
    with fd.open_session() as session:
        # fix_bad_percentage(session, start_date, end_date)
        # fix_negative_min_events(session, start_date, end_date)
        reload_low_percentage(session, start_date, end_date)
        relook_at_too_many_dividend_types(session, start_date, end_date)


if __name__ == "__main__":
    main()
