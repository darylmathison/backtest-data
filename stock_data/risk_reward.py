from sqlalchemy import asc

import stock_data as sd
from stock_data.models import Dividends, Stock, RiskReward
import stock_data.fill_data as fd
import pandas as pd
import scipy.stats as stats
import logging
import datetime


# what to risk = prob of win/amount of loss - prob of loss/amount of gain


def dividend_stocks(dbsession):
    return {
        symbol[0]
        for symbol in dbsession.query(Stock.symbol)
        .filter(Stock.dividend)
        .distinct()
        .all()
    }


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


def simulate_trade(row, symbol, dbsession, num_days):
    event_days = (
        dbsession.query(Stock)
        .filter(
            Stock.symbol == symbol,
            Stock.date.between(row["purchase_date"], row["sell_date"]),
        )
        .order_by(asc(Stock.date))
        .all()
    )
    if len(event_days) < num_days:
        fd.fill_stock_data(dbsession, symbol, row["purchase_date"], row["sell_date"])
        event_days = (
            dbsession.query(Stock)
            .filter(
                Stock.symbol == symbol,
                Stock.date.between(row["purchase_date"], row["sell_date"]),
            )
            .order_by(asc(Stock.date))
            .all()
        )
    if len(event_days) < num_days:
        return None
    beginning_price = event_days[0].open
    end_price = event_days[-1].close
    if beginning_price is None or end_price is None:
        return None

    profit_price = sd.convert_to_currency(row["cash_amount"] + beginning_price)
    stop_loss = sd.convert_to_currency(beginning_price * 0.90)
    highest_price = max([event.high for event in event_days])
    lowest_price = min([event.low for event in event_days])

    if highest_price >= profit_price:
        return profit_price - beginning_price
    elif lowest_price <= stop_loss:
        return stop_loss - beginning_price

    return end_price - beginning_price + row["cash_amount"]


def process_all_securities(dbsession, symbols, buy_days=5):
    rows = []
    existing_evaluations = {
        symbol[0] for symbol in dbsession.query(RiskReward.symbol).all()
    }
    symbols_to_research = set(symbols) - existing_evaluations
    for symbol in symbols_to_research:
        _win_rate, loss_rate, avg_gain, avg_loss, sample_size, avg_dividend = win_rate(
            dbsession, symbol, buy_days
        )
        if _win_rate is not None and (avg_loss > 0 and avg_gain > 0):
            portion_to_risk = (_win_rate / avg_loss) - (loss_rate / avg_gain)
            risk_reward_row = RiskReward(
                symbol=symbol,
                win_rate=_win_rate,
                avg_gain=avg_gain,
                loss_rate=loss_rate,
                avg_loss=avg_loss,
                sample_size=sample_size,
                avg_dividend=avg_dividend,
                portion_to_risk=portion_to_risk,
                last_update=datetime.datetime.now(),
            )
            dbsession.add(risk_reward_row)
            dbsession.commit()
    return pd.read_sql(
        dbsession.query(RiskReward).statement, dbsession.bind, index_col="symbol"
    )


def win_rate(dbsession, symbol, buy_days=5):
    divs = pd.read_sql(
        dbsession.query(
            Dividends.symbol, Dividends.ex_dividend_date, Dividends.cash_amount
        )
        .filter(Dividends.symbol == symbol)
        .statement,
        dbsession.bind,
        index_col="ex_dividend_date",
    )
    if len(divs) < 2:
        return [None] * 6
    calendar = sd.create_calendar()
    divs["sell_date"] = divs.index.map(lambda x: calendar.addbusdays(x, -1))
    divs["purchase_date"] = divs.index.map(lambda x: calendar.addbusdays(x, -buy_days))
    divs["gain"] = divs[["sell_date", "purchase_date", "cash_amount"]].apply(
        simulate_trade, args=(symbol, dbsession, buy_days), axis=1
    )
    divs["purchase_price"] = divs["purchase_date"].map(
        lambda x: purchase_price(dbsession, symbol, x)
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
    return (
        _win_rate,
        loss_rate,
        avg_gain,
        avg_loss,
        len(divs),
        sd.convert_to_currency(divs["cash_amount"].mean()),
    )


if __name__ == "__main__":
    with fd.open_session() as session:
        print(win_rate(session, "CCEP", 5))
        # symbols = dividend_stocks(session)
        # df = process_all_securities(session, symbols, 5)
        # df["risk"] = (df["win_rate"] / df["avg_loss"]) - (
        #     df["loss_rate"] / df["avg_gain"]
        # )
        # print(df[df["symbol"] == "MO"])
        # print(
        #     df[
        #         [
        #             "symbol",
        #             "win_rate",
        #             "avg_gain",
        #             "loss_rate",
        #             "avg_loss",
        #             "risk",
        #             "sample_size",
        #         ]
        #     ]
        #     .where(df["sample_size"] > 20)
        #     .dropna()
        #     .sort_values("risk", ascending=False)
        #     .head(10)
        # )
