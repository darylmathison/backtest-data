import datetime

from sqlalchemy import String, REAL, Date, Boolean, UniqueConstraint, Index
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class Stock(Base):
    __tablename__ = "stocks"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String)
    open: Mapped[float] = mapped_column(REAL)
    high: Mapped[float] = mapped_column(REAL)
    low: Mapped[float] = mapped_column(REAL)
    close: Mapped[float] = mapped_column(REAL)
    volume: Mapped[float] = mapped_column(REAL)
    date: Mapped[datetime.date] = mapped_column(Date)
    trade_count: Mapped[float] = mapped_column(REAL)
    dividend: Mapped[bool] = mapped_column(Boolean)

    symbol_index = Index("stock_symbol_index", symbol)
    symbol_date_index = Index("stock_symbol_date_index", symbol, date)

    __table_args__ = (UniqueConstraint("symbol", "date", name="uix_symbol_date"),)


class Dividends(Base):
    __tablename__ = "dividends"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String)
    ex_dividend_date: Mapped[datetime.date] = mapped_column(Date)
    pay_date: Mapped[datetime.date] = mapped_column(Date)
    record_date: Mapped[datetime.date] = mapped_column(Date)
    declared_date: Mapped[datetime.date] = mapped_column(Date)
    cash_amount: Mapped[float] = mapped_column(REAL)
    currency: Mapped[str] = mapped_column(String)
    frequency: Mapped[str] = mapped_column(String)

    symbol_index = Index("dividends_symbol", symbol)
    symbol_date_index = Index(
        "dividends_symbol_ex_dividend_date", symbol, ex_dividend_date
    )

    __table_args__ = (
        UniqueConstraint(
            "symbol", "ex_dividend_date", name="uix_symbol_ex_dividend_date"
        ),
    )


class Correlation(Base):
    __tablename__ = "correlations"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    right: Mapped[str] = mapped_column(String)
    left: Mapped[str] = mapped_column(String)
    correlation: Mapped[float] = mapped_column(REAL)


class Assets(Base):
    __tablename__ = "assets"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String)
    downloaded: Mapped[bool] = mapped_column(Boolean)


class Holidays(Base):
    __tablename__ = "holidays"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    date: Mapped[datetime.date] = mapped_column(Date)
    __table_args__ = (UniqueConstraint("date", name="uix_date"),)
