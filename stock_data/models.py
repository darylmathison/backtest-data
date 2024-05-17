import datetime

from sqlalchemy import String, REAL, Date, Boolean, UniqueConstraint, Index
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import Table, Column, Integer, ForeignKey
from sqlalchemy.orm import relationship


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


assets_marketdays_link = Table(
    "assets_marketdays",
    Base.metadata,
    Column("assets_id", Integer, ForeignKey("assets.id")),
    Column("marketdays_id", Integer, ForeignKey("market_days.id")),
)


class Assets(Base):
    __tablename__ = "assets"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String)
    start_date: Mapped[datetime.date] = mapped_column(Date)
    min_num_events: Mapped[int] = mapped_column(Integer, default=0)
    percentage_downloaded: Mapped[float] = mapped_column(REAL, default=0.0)
    dividend: Mapped[bool] = mapped_column(Boolean, default=False)
    dividend_checked: Mapped[bool] = mapped_column(Boolean, default=False)

    market_days = relationship(
        "MarketDays", secondary=assets_marketdays_link, back_populates="assets"
    )

    __table_args__ = (UniqueConstraint("symbol", name="uix_assets_symbol"),)


class MarketDays(Base):
    __tablename__ = "market_days"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    date: Mapped[datetime.date] = mapped_column(Date)
    __table_args__ = (UniqueConstraint("date", name="uix_marketdays_date"),)

    assets = relationship(
        "Assets", secondary=assets_marketdays_link, back_populates="market_days"
    )


class Holidays(Base):
    __tablename__ = "holidays"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    date: Mapped[datetime.date] = mapped_column(Date)
    __table_args__ = (UniqueConstraint("date", name="uix_holidays_date"),)


class RiskReward(Base):
    __tablename__ = "risk_reward"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String)
    win_rate: Mapped[float] = mapped_column(REAL)
    loss_rate: Mapped[float] = mapped_column(REAL)
    avg_gain: Mapped[float] = mapped_column(REAL)
    avg_loss: Mapped[float] = mapped_column(REAL)
    percentage_downloaded: Mapped[float] = mapped_column(REAL)
    avg_dividend: Mapped[float] = mapped_column(REAL)
    last_update: Mapped[datetime.date] = mapped_column(Date)
    portion_to_risk: Mapped[float] = mapped_column(REAL, nullable=True)
    __table_args__ = (UniqueConstraint("symbol", name="uix_risk_reward_symbol"),)
