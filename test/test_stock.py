import csv
import datetime
import json
import unittest

import dateutil.parser
import pytz
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import stock_data.models
from stock_data.models import Base, Stock, Dividends

DATABASE_URL = "sqlite:///:memory:"

raw_stock_data = """
2023-04-24,20.660000,20.790001,20.350000,20.480000,19.520357,1653600
2023-04-25,20.410000,20.650000,20.290001,20.629999,19.663332,2564600
2023-04-26,20.650000,20.750000,20.330000,20.389999,19.434574,1760500
2023-04-27,20.389999,20.950001,20.389999,20.879999,19.901615,2040600
2023-04-28,20.930000,21.480000,20.850000,21.330000,20.330530,2927000
2023-05-01,21.219999,21.410000,21.049999,21.100000,20.111307,1879000
2023-05-02,21.020000,21.580000,20.860001,21.090000,20.101776,3632000
2023-05-03,21.260000,21.410000,20.900000,20.910000,19.930212,2132800
2023-05-04,20.879999,21.049999,20.620001,20.969999,19.987398,1762100
2023-05-05,21.170000,21.379999,21.010000,21.360001,20.359125,2781600
2023-05-08,21.320000,21.340000,20.940001,21.180000,20.187561,2124900
2023-05-09,21.000000,21.000000,20.570000,20.809999,19.834896,1572400
2023-05-10,21.040001,21.110001,20.580000,20.700001,19.730049,1951500
2023-05-11,20.520000,20.650000,20.320000,20.520000,19.558485,2875600
"""

raw_dividend_data = """
[
  {
    "symbol": "AAVMY",
    "ex_dividend_date": "2024-04-26",
    "record_date": "2024-04-29",
    "pay_date": "2024-06-11",
    "cash_amount": 0.958352,
    "frequency": "4"
  },
  {
    "symbol": "ASML",
    "ex_dividend_date": "2024-04-26",
    "record_date": "2024-04-29",
    "pay_date": "2024-05-07",
    "cash_amount": 1.899275
  },
  {
    "symbol": "ATCOPRD",
    "ex_dividend_date": "2024-04-26",
    "record_date": "2024-04-29",
    "pay_date": "2024-04-30",
    "cash_amount": 0.496875
  },
  {
    "symbol": "ATCOPRH",
    "ex_dividend_date": "2024-04-26",
    "record_date": "2024-04-29",
    "pay_date": "2024-04-30",
    "cash_amount": 0.492188
  },
  {
    "symbol": "BASFY",
    "ex_dividend_date": "2024-04-26",
    "record_date": "2024-04-29",
    "pay_date": "2024-05-10",
    "cash_amount": 0.922845
  }
  ]"""


def convert_to_date(date: str):
    no_tz = dateutil.parser.parse(date)
    return datetime.datetime.fromtimestamp(
        no_tz.timestamp(), tz=pytz.timezone("US/Eastern")
    ).date()


class TestStock(unittest.TestCase):

    def setUp(self):
        engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(engine)
        self.session = sessionmaker(bind=engine)()
        fieldnames = ["date", "open", "high", "low", "close", "volume", "trade_count"]
        reader = csv.DictReader(raw_stock_data.splitlines(), fieldnames=fieldnames)
        for row in reader:
            stock_data_row = {
                "symbol": "BRX",
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
                "date": convert_to_date(row["date"]),
                "trade_count": row["trade_count"],
                "dividend": False,
            }
            stock = Stock(**stock_data_row)
            self.session.add(stock)

    def tearDown(self):
        self.session.close()

    def test_stock(self):
        stocks = self.session.query(Stock).all()
        filtered_rows = tuple(
            filter(
                lambda x: bool(x), map(lambda x: x.strip(), raw_stock_data.splitlines())
            )
        )
        self.assertEqual(len(filtered_rows), len(stocks))


class TestDividends(unittest.TestCase):

    def setUp(self):
        engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(engine)
        self.session = sessionmaker(bind=engine)()
        dividend_data = json.loads(raw_dividend_data)
        for row in dividend_data:
            dividend_data_row = {
                "symbol": row["symbol"],
                "ex_dividend_date": convert_to_date(row["ex_dividend_date"]),
                "record_date": convert_to_date(row["record_date"]),
                "pay_date": convert_to_date(row["pay_date"]),
                "cash_amount": row["cash_amount"],
                "currency": "USD",
                "frequency": "4",
                "declared_date": convert_to_date(row["ex_dividend_date"]),
                "dividend_type": "CD",
            }
            dividend = stock_data.models.Dividends(**dividend_data_row)
            self.session.add(dividend)

    def tearDown(self):
        self.session.close()

    def test_dividends(self):
        dividends = self.session.query(Dividends).all()
        json_data = json.loads(raw_dividend_data)
        self.assertEqual(len(json_data), len(dividends))


if __name__ == "__main__":
    unittest.main()
