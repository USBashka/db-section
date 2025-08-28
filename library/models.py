from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Optional, List, Union

from sqlalchemy import (
    ForeignKey, String, Integer, Numeric, Date, Text, UniqueConstraint
)
from sqlalchemy.orm import (
    DeclarativeBase, Mapped, mapped_column, relationship
)



class Base(DeclarativeBase):
    pass


# ---------- Простые справочники ----------

class Genre(Base):
    __tablename__ = "genre"

    genre_id: Mapped[int] = mapped_column(primary_key=True)
    name_genre: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)

    books: Mapped[List["Book"]] = relationship(back_populates="genre")

    def __repr__(self) -> str:
        return f"<Genre {self.name_genre!r}>"


class Author(Base):
    __tablename__ = "author"

    author_id: Mapped[int] = mapped_column(primary_key=True)
    name_author: Mapped[str] = mapped_column(String(150), unique=True, nullable=False)

    books: Mapped[List["Book"]] = relationship(back_populates="author")

    def __repr__(self) -> str:
        return f"<Author {self.name_author!r}>"


class City(Base):
    __tablename__ = "city"

    city_id: Mapped[int] = mapped_column(primary_key=True)
    name_city: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    days_delivery: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    clients: Mapped[List["Client"]] = relationship(back_populates="city")


# ---------- Клиенты и заказы ----------

class Client(Base):
    __tablename__ = "client"

    client_id: Mapped[int] = mapped_column(primary_key=True)
    name_client: Mapped[str] = mapped_column(String(150), nullable=False, index=True)
    city_id: Mapped[int] = mapped_column(ForeignKey("city.city_id"), nullable=False)
    email: Mapped[Optional[str]] = mapped_column(String(255))

    city: Mapped[City] = relationship(back_populates="clients")
    buys: Mapped[List["Buy"]] = relationship(back_populates="client")


class Buy(Base):
    __tablename__ = "buy"

    buy_id: Mapped[int] = mapped_column(primary_key=True)
    buy_description: Mapped[Optional[str]] = mapped_column(Text)
    client_id: Mapped[int] = mapped_column(ForeignKey("client.client_id"), nullable=False)

    client: Mapped[Client] = relationship(back_populates="buys")
    buy_books: Mapped[List["BuyBook"]] = relationship(back_populates="buy", cascade="all, delete-orphan")
    steps: Mapped[List["BuyStep"]] = relationship(back_populates="buy", cascade="all, delete-orphan")


class Step(Base):
    __tablename__ = "step"

    step_id: Mapped[int] = mapped_column(primary_key=True)
    name_step: Mapped[str] = mapped_column(String(80), unique=True, nullable=False)

    buy_steps: Mapped[List["BuyStep"]] = relationship(back_populates="step")


# ---------- Книги и связки ----------

class Book(Base):
    __tablename__ = "book"
    __table_args__ = (
        UniqueConstraint("title", "author_id", name="uq_book_title_author"),
    )

    book_id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False, index=True)

    author_id: Mapped[int] = mapped_column(ForeignKey("author.author_id"), nullable=False, index=True)
    genre_id: Mapped[int] = mapped_column(ForeignKey("genre.genre_id"), nullable=False, index=True)

    price: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    amount: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    author: Mapped[Author] = relationship(back_populates="books")
    genre: Mapped[Genre] = relationship(back_populates="books")
    buy_books: Mapped[List["BuyBook"]] = relationship(back_populates="book")

    # Удобный конструктор:
    # Book("Чужак", max_frei, fantasy, 490, 56)
    # Book("Солярис", author_id, genre_id, 650, 10)
    def __init__(
        self,
        title: str,
        author: Union[Author, int],
        genre: Union[Genre, int],
        price: Union[int, float, Decimal],
        amount: int = 0,
    ) -> None:
        self.title = title

        if isinstance(author, Author):
            self.author = author
        else:
            self.author_id = int(author)

        if isinstance(genre, Genre):
            self.genre = genre
        else:
            self.genre_id = int(genre)

        self.price = Decimal(str(price))
        self.amount = int(amount)

    def __repr__(self) -> str:
        return f"<Book {self.title!r} by {getattr(self.author,'name_author',self.author_id)}>"


class BuyBook(Base):
    __tablename__ = "buy_book"

    buy_book_id: Mapped[int] = mapped_column(primary_key=True)
    buy_id: Mapped[int] = mapped_column(ForeignKey("buy.buy_id"), nullable=False)
    book_id: Mapped[int] = mapped_column(ForeignKey("book.book_id"), nullable=False)
    amount: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    buy: Mapped[Buy] = relationship(back_populates="buy_books")
    book: Mapped[Book] = relationship(back_populates="buy_books")


class BuyStep(Base):
    __tablename__ = "buy_step"

    buy_step_id: Mapped[int] = mapped_column(primary_key=True)
    buy_id: Mapped[int] = mapped_column(ForeignKey("buy.buy_id"), nullable=False)
    step_id: Mapped[int] = mapped_column(ForeignKey("step.step_id"), nullable=False)
    date_step_beg: Mapped[Optional[date]] = mapped_column(Date)
    date_step_end: Mapped[Optional[date]] = mapped_column(Date)

    buy: Mapped[Buy] = relationship(back_populates="steps")
    step: Mapped[Step] = relationship(back_populates="buy_steps")
