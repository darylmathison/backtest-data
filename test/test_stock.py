import unittest

import stock_data.stock


class TestStock(unittest.TestCase):
    def test_stock(self):
        stock = stock_data.stock.Stock()
        stock.symbol


class TestDividends(unittest.TestCase):
    def test_dividends(self):
        dividends = stock_data.stock.Dividends()
        dividends.symbol
        dividends.ex_dividend_date


class TestCorrelation(unittest.TestCase):
    def test_correlation(self):
        correlation = stock_data.stock.Correlation()
        correlation.left
        correlation.right
        correlation.correlation


if __name__ == "__main__":
    unittest.main()
