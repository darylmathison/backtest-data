from sqlalchemy import String, REAL, Date, Boolean
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class Stock(Base):
    __tablename__ = "stocks"
    id = mapped_column(String, primary_key=True)
    symbol = mapped_column(String)
    open = mapped_column(REAL)
    high = mapped_column(REAL)
    low = mapped_column(REAL)
    close = mapped_column(REAL)
    volume = mapped_column(REAL)
    date = mapped_column(Date)
    trade_count = mapped_column(REAL)
    dividend = mapped_column(Boolean)


class Dividends(Base):
    __tablename__ = "dividends"
    id = mapped_column(String, primary_key=True)
    symbol = mapped_column(String)
    ex_dividend_date = mapped_column(Date)
    pay_date = mapped_column(Date)
    record_date = mapped_column(Date)
    declared_date = mapped_column(Date)
    cash_amount = mapped_column(REAL)
    currency = mapped_column(String)
    frequency = mapped_column(String)


class Correlation(Base):
    __tablename__ = "correlations"
    id = mapped_column(String, primary_key=True)
    right = mapped_column(String)
    left = mapped_column(String)
    correlation = mapped_column(REAL)


class Assets(Base):
    __tablename__ = "assets"
    id = mapped_column(String, primary_key=True)
    symbol = mapped_column(String)
    downloaded = mapped_column(Boolean)
