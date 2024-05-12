import stock_data as sd
from stock_data.models import Dividends, Stock
import stock_data.fill_data as fd
import pandas as pd
import scipy.stats

# what to risk = prob of win/amount of loss - prob of loss/amount of gain


def dividend_stocks(dbsession):
    return {
        symbol[0]
        for symbol in dbsession.query(Stock.symbol)
        .filter(Stock.dividend)
        .distinct()
        .all()
    }


def purchase_price(dbsession, symbol, date):
    stock = (
        dbsession.query(Stock)
        .filter(Stock.symbol == symbol, Stock.date == date)
        .first()
    )
    if stock is None:
        return None
    return stock.open


def sell_price(dbsession, symbol, date):
    stock = (
        dbsession.query(Stock)
        .filter(Stock.symbol == symbol, Stock.date == date)
        .first()
    )
    if stock is None:
        return None
    return stock.close


def collective_win(dbsession, symbols, buy_days=5):
    rows = []

    for symbol in symbols:
        _win_rate, loss_rate, avg_gain, avg_loss, sample_size, avg_dividend = win_rate(
            dbsession, symbol, buy_days
        )
        if _win_rate is not None and (avg_loss > 0 and avg_gain > 0):
            rows.append(
                [
                    symbol,
                    _win_rate,
                    loss_rate,
                    avg_gain,
                    avg_loss,
                    sample_size,
                    avg_dividend,
                ]
            )

    return pd.DataFrame(
        rows,
        columns=[
            "symbol",
            "win_rate",
            "loss_rate",
            "avg_gain",
            "avg_loss",
            "sample_size",
            "avg_dividend",
        ],
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
    divs["sell_price"] = divs["sell_date"].map(
        lambda x: sell_price(dbsession, symbol, x)
    )
    divs["purchase_date"] = divs.index.map(lambda x: calendar.addbusdays(x, -buy_days))
    divs["purchase_price"] = divs["purchase_date"].map(
        lambda x: purchase_price(dbsession, symbol, x)
    )
    # divs["gain"] = divs["sell_price"] - divs["purchase_price"] + divs["cash_amount"]
    divs["gain"] = divs["sell_price"] - divs["purchase_price"]
    divs["percent_gain"] = divs["gain"] / divs["purchase_price"]
    divs["win"] = divs["gain"] > 0
    if len(divs) == 0:
        _win_rate = 0
    else:
        _win_rate = divs["win"].sum() / len(divs)
    loss_rate = 1 - _win_rate
    if _win_rate > 0:
        avg_gain = scipy.stats.gmean(
            divs["percent_gain"].where(divs["gain"] > 0).dropna()
        )
    else:
        avg_gain = 0
    if loss_rate > 0:
        avg_loss = scipy.stats.gmean(
            divs["percent_gain"].where(divs["gain"] < 0).dropna().map(lambda x: abs(x))
        )
    else:
        avg_loss = 0
    return (
        _win_rate,
        loss_rate,
        avg_gain,
        avg_loss,
        len(divs),
        divs["cash_amount"].mean(),
    )


if __name__ == "__main__":
    with fd.open_session() as session:
        symbols = dividend_stocks(session)
        df = collective_win(session, symbols, 5)
        df["risk"] = (df["win_rate"] / df["avg_loss"]) - (
            df["loss_rate"] / df["avg_gain"]
        )
        print(df[df["symbol"] == "MO"])
        print(
            df[
                [
                    "symbol",
                    "win_rate",
                    "avg_gain",
                    "loss_rate",
                    "avg_loss",
                    "risk",
                    "sample_size",
                ]
            ]
            .where(df["sample_size"] > 20)
            .dropna()
            .sort_values("risk", ascending=False)
            .head(10)
        )
