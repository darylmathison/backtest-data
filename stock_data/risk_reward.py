import datetime
import logging
from typing import Any

import pandas as pd
from sqlalchemy import asc, and_

import stock_data as sd
import stock_data.fill_data as fd
from stock_data.models import (
    Dividends,
    Stock,
    RiskReward,
    Assets,
    Event,
)


# what to risk = prob of win/amount of loss - prob of loss/amount of gain


def dividend_stocks(dbsession) -> list[Any]:
    return [
        asset
        for asset in dbsession.query(Assets)
        .filter(
            and_(
                Assets.dividend,
                Assets.percentage_downloaded > 0.8,
                Assets.percentage_downloaded < 1.2,
            )
        )
        .all()
    ]


def get_stock(dbsession, symbol, date):
    calendar = sd.create_calendar()
    results = (
        dbsession.query(Stock)
        .filter(Stock.symbol == symbol, Stock.date == date)
        .first()
    )
    if results is None:
        logging.info("Filling stock data for %s on %s", symbol, date)
        fd.fill_stock_data(dbsession, symbol, date, calendar.addbusdays(date, 1))
        return (
            dbsession.query(Stock)
            .filter(Stock.symbol == symbol, Stock.date == date)
            .first()
        )

    return results


def purchase_price(dbsession, symbol, date):
    stock = get_stock(dbsession, symbol, date)
    if stock is None:
        return None
    return stock.open


def sell_price(dbsession, symbol, date):
    stock = get_stock(dbsession, symbol, date)
    if stock is None:
        return None
    return stock.close


def simulate_trade(row, dbsession, div_multiplier=1, stop_loss_percentage=0.1):
    event_days = (
        dbsession.query(Stock)
        .filter(
            and_(
                Stock.symbol == row["symbol"],
                Stock.date >= row["start_date"],
                Stock.date <= row["end_date"],
            )
        )
        .order_by(asc(Stock.date))
        .all()
    )
    if len(event_days) == 0:
        return 0
    beginning_price = event_days[0].open
    end_price = event_days[-1].close
    if beginning_price is None or end_price is None:
        return None

    profit_price = sd.convert_to_currency(
        div_multiplier * row["cash_amount"] + beginning_price
    )
    stop_loss = sd.convert_to_currency(beginning_price * (1 - stop_loss_percentage))

    for event in event_days:
        if event.high >= profit_price:
            return profit_price - beginning_price
        elif event.low <= stop_loss:
            return stop_loss - beginning_price

    return end_price - beginning_price + row["cash_amount"]


def process_all_securities(dbsession, assets, buy_days=5):
    end = datetime.date.today()
    start = datetime.date(end.year - 10, end.month, end.day)

    fd.fill_market_days(start, end)
    fd.fill_holidays(start, end)

    existing_evaluations = {
        symbol[0] for symbol in dbsession.query(RiskReward.symbol).all()
    }
    symbols_to_research = set((a.symbol for a in assets)) - existing_evaluations
    assets_to_research = [a for a in assets if a.symbol in symbols_to_research]
    for asset in assets_to_research:
        (
            _win_rate,
            loss_rate,
            avg_gain,
            avg_loss,
            percentage_downloaded,
            avg_dividend,
            div_multiplier,
            stop_loss_percentage,
        ) = backtest_security(dbsession, start, end, asset, buy_days)
        if _win_rate is not None and (avg_loss > 0 and avg_gain > 0):
            portion_to_risk = (_win_rate / avg_loss) - (loss_rate / avg_gain)
            risk_reward_row = RiskReward(
                symbol=asset.symbol,
                win_rate=_win_rate,
                avg_gain=avg_gain,
                loss_rate=loss_rate,
                avg_loss=avg_loss,
                percentage_downloaded=percentage_downloaded,
                avg_dividend=avg_dividend,
                portion_to_risk=portion_to_risk,
                last_update=datetime.datetime.now(),
                div_multiplier=div_multiplier,
                stop_loss_percentage=stop_loss_percentage,
            )
            dbsession.add(risk_reward_row)
            dbsession.commit()
    return pd.read_sql(
        dbsession.query(RiskReward).statement, dbsession.bind, index_col="id"
    )


def backtest_security(
    dbsession, start, end, asset, buy_days=5, div_multiplier=1, stop_loss_percentage=0.1
):
    null_return = [None] * 8
    num_of_months = fd.num_months_between_dates(asset.start_date, end)
    frequeny = fd.find_frequency(asset)
    if frequeny == -1:
        asset.dividend = False
        dbsession.add(asset)
        dbsession.commit()
        return null_return
    asset.min_num_events = fd.calulate_num_event(num_of_months, frequeny)

    div_data = [d for d in asset.dividends if d.ex_dividend_date < end]
    if len(div_data) < asset.min_num_events:
        fd.fill_dividend_data(dbsession, start, end, [asset])
    if len(asset.events) < len(asset.dividends):
        fd.fill_event_data(dbsession, start, end, buy_days, [asset])
    query = (
        dbsession.query(
            Assets.symbol, Dividends.cash_amount, Event.start_date, Event.end_date
        )
        .join(Dividends)
        .join(Event)
        .filter(Assets.symbol == asset.symbol)
    )
    divs = pd.read_sql(
        query.statement,
        dbsession.bind,
    )
    if len(divs) < 2:
        return null_return

    calendar = sd.create_calendar()
    divs["gain"] = divs[["symbol", "start_date", "end_date", "cash_amount"]].apply(
        simulate_trade, args=(dbsession, div_multiplier, stop_loss_percentage), axis=1
    )
    divs["purchase_price"] = divs["start_date"].map(
        lambda x: purchase_price(dbsession, asset.symbol, x)
    )
    divs["percent_gain"] = divs["gain"] / divs["purchase_price"]
    divs["win"] = divs["gain"] > 0
    if len(divs) == 0:
        _win_rate = 0
    else:
        _win_rate = divs["win"].sum() / len(divs)
    loss_rate = 1 - _win_rate
    if _win_rate > 0:
        avg_gain = divs["percent_gain"].where(divs["gain"] > 0).dropna().mean()
    else:
        avg_gain = 0
    if loss_rate > 0:
        avg_loss = (
            divs["percent_gain"]
            .where(divs["gain"] < 0)
            .dropna()
            .map(lambda x: abs(x))
            .mean()
        )
    else:
        avg_loss = 0

    percentage_downloaded = (
        dbsession.query(Assets.percentage_downloaded)
        .filter(Assets.symbol == asset.symbol)
        .first()[0]
    )
    return (
        _win_rate,
        loss_rate,
        avg_gain,
        avg_loss,
        percentage_downloaded,
        sd.convert_to_currency(divs["cash_amount"].mode()),
        div_multiplier,
        stop_loss_percentage,
    )


if __name__ == "__main__":
    with fd.open_session() as session:
        process_all_securities(session, dividend_stocks(session))
