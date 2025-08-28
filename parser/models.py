from __future__ import annotations

from sqlalchemy import String, Integer, Numeric, Date, DateTime, func, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from database import Base



class SpimexTradingResult(Base):
    __tablename__ = "spimex_trading_results"
    __table_args__ = (
        UniqueConstraint("exchange_product_id", "date", name="uq_spimex_result_pid_date"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)

    # поля из бюллетеня
    exchange_product_id:   Mapped[str]  = mapped_column(String(32),  nullable=False, index=True)
    exchange_product_name: Mapped[str]  = mapped_column(String(512), nullable=False)
    delivery_basis_name:   Mapped[str]  = mapped_column(String(512), nullable=False)

    # производные поля из exchange_product_id
    oil_id:            Mapped[str] = mapped_column(String(4), nullable=False)   # [:4]
    delivery_basis_id: Mapped[str] = mapped_column(String(3), nullable=False)   # [4:7]
    delivery_type_id:  Mapped[str] = mapped_column(String(1), nullable=False)   # [-1]

    # численные
    volume: Mapped[float] = mapped_column(Numeric(18, 6), nullable=False)  # объём в единицах измерения
    total:  Mapped[float] = mapped_column(Numeric(18, 2), nullable=False)  # рубли
    count:  Mapped[int]   = mapped_column(Integer,        nullable=False)  # кол-во договоров

    # дата торгов (из файла/URL)
    date:   Mapped["date"] = mapped_column(Date, nullable=False)

    # сервисные
    created_on: Mapped["DateTime"] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_on: Mapped["DateTime"] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
