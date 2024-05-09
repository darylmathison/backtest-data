from stock_data.models import Dividends, Stock
from stock_data.fill_data import open_session


def dividend_stocks(dbsession):
    return {
        symbol[0]
        for symbol in dbsession.query(Stock.symbol)
        .filter(Stock.symbol == Dividends.symbol)
        .distinct()
        .all()
    }


def win_rate(dbsession, symbol):
    return (
        dbsession.query(Dividends.ex_dividend_date)
        .filter(Dividends.symbol == symbol)
        .all()
    )


if __name__ == "__main__":
    with open_session() as session:
        print(win_rate(session, "MO"))
