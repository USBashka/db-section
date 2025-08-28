from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session


import models
from models import Base, Author, Genre, Book
from database import engine



def init_db() -> None:
    Base.metadata.create_all(bind=engine)


def seed_if_empty() -> None:
    with Session(engine) as session:
        any_book = session.execute(select(Book.book_id).limit(1)).scalar_one_or_none()
        if any_book is not None:
            print("Книги уже есть — пропускаю наполнение.")
            return

        # ---- справочники ----
        max_frei = Author(name_author="Макс Фрай")
        lem = Author(name_author="Станислав Лем")
        strugatsky = Author(name_author="Братья Стругацкие")
        tolkien = Author(name_author="Дж. Р. Р. Толкин")

        fantasy = Genre(name_genre="Фэнтези")
        scifi = Genre(name_genre="Научная фантастика")
        adventure = Genre(name_genre="Приключения")

        session.add_all([max_frei, lem, strugatsky, tolkien, fantasy, scifi, adventure])
        session.flush()

        # ---- книги ----
        session.add_all([
            Book("Чужак", max_frei, fantasy, 490, 56),
            Book("Солярис", lem, scifi, 650, 12),
            Book("Трудно быть богом", strugatsky, scifi, 520, 20),
            Book("Хоббит, или Туда и обратно", tolkien, adventure, 780, 15),
            Book("Волонтёры вечности", max_frei, fantasy, 540, 18),
        ])
        session.commit()
        print("База инициализирована: добавлены 5 книг.")


def list_books() -> None:
    with Session(engine) as session:
        rows = session.query(Book).join(Book.author).join(Book.genre).all()
        for b in rows:
            print(
                f"{b.book_id:>3} | {b.title:32} | "
                f"{b.author.name_author:20} | {b.genre.name_genre:18} | "
                f"{b.price} ₽ | qty={b.amount}"
            )


if __name__ == "__main__":
    init_db()
    seed_if_empty()
    list_books()
