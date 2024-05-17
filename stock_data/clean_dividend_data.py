import datetime

from sqlalchemy import and_, or_

from stock_data.models import Dividends, Assets
import stock_data.fill_data as fd


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


def main():
    years_back = 10
    end_date = datetime.datetime.now().date()
    start_date = datetime.date(year=end_date.year - years_back, month=1, day=1)
    with fd.open_session() as session:
        fix_bad_percentage(session, start_date, end_date)
        fix_negative_min_events(session, start_date, end_date)


if __name__ == "__main__":
    main()
