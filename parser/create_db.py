from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv
import os
import re



load_dotenv()

DB_NAME = os.environ["DB_NAME"]
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ["DB_PASS"]


safe = re.compile(r"^\w+$")
assert safe.match(DB_NAME), "Недопустимое имя БД"
assert safe.match(DB_USER), "Недопустимое имя пользователя"

admin_url = URL.create(
    "postgresql+psycopg",
    username=DB_USER, password=DB_PASS,
    host=DB_HOST, port=DB_PORT,
    database="postgres",
)


engine = create_engine(admin_url, isolation_level="AUTOCOMMIT", pool_pre_ping=True)

with engine.connect() as conn:
    exists = conn.execute(
        text("SELECT 1 FROM pg_database WHERE datname = :n"),
        {"n": DB_NAME},
    ).scalar()
    if not exists:
        conn.execute(text(
            f"CREATE DATABASE \"{DB_NAME}\" "
            f"WITH OWNER \"{DB_USER}\" ENCODING 'UTF8' TEMPLATE template0"
        ))
        print(f"Создана БД: {DB_NAME}")
    else:
        print(f"БД уже существует: {DB_NAME}")
