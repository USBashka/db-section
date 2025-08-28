from __future__ import annotations

import argparse
import datetime as dt
from typing import Iterable

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from database import engine, Base, SessionLocal
from models import SpimexTradingResult
from downloader import iter_daily_files
from parser import parse_bulletin_xls, to_records



def init_db() -> None:
    Base.metadata.create_all(bind=engine)

def upsert_results(session: Session, records: Iterable[dict]) -> int:
    """
    UPSERT по (exchange_product_id, date).
    Возвращает число затронутых строк (для логов).
    """
    records = list(records)
    if not records:
        return 0

    stmt = insert(SpimexTradingResult).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=["exchange_product_id", "date"],
        set_={
            "exchange_product_name": stmt.excluded.exchange_product_name,
            "delivery_basis_name":   stmt.excluded.delivery_basis_name,
            "oil_id":                stmt.excluded.oil_id,
            "delivery_basis_id":     stmt.excluded.delivery_basis_id,
            "delivery_type_id":      stmt.excluded.delivery_type_id,
            "volume":                stmt.excluded.volume,
            "total":                 stmt.excluded.total,
            "count":                 stmt.excluded.count,
            "updated_on":            stmt.excluded.updated_on,
        },
    )
    res = session.execute(stmt)
    return res.rowcount or 0

def main():
    parser = argparse.ArgumentParser(description="SPIMEX oil bulletin loader")
    parser.add_argument("--since", default="2023-01-01", help="Начало периода (YYYY-MM-DD), по умолчанию 2023-01-01")
    parser.add_argument("--until", help="Окончание периода (YYYY-MM-DD, не включительно). По умолчанию — сегодняшняя дата.")
    parser.add_argument("--time", default="162000", help="HHMMSS (по умолчанию 162000)")
    args = parser.parse_args()

    since = dt.datetime.strptime(args.since, "%Y-%m-%d").date()
    until = dt.datetime.strptime(args.until, "%Y-%m-%d").date() if args.until else dt.date.today()

    if since >= until:
        raise SystemExit("since должно быть раньше until")

    init_db()

    total_days = 0
    total_files = 0
    total_rows = 0

    with SessionLocal() as session:
        for day, url, content in iter_daily_files(since, until, time_str=args.time):
            total_days += 1
            try:
                df = parse_bulletin_xls(content, fallback_date=day)
            except Exception as e:
                print(f"[{day}] Пропуск из-за ошибки парсинга: {e}")
                continue

            records = to_records(df)
            if not records:
                print(f"[{day}] Нет строк с count>0 — пропускаю")
                continue

            upserted = upsert_results(session, records)
            session.commit()

            total_files += 1
            total_rows  += len(records)
            print(f"[{day}] OK: {len(records)} строк (upserted={upserted}) из {url}")

    print(f"Готово. Дней просмотрено: {total_days}, файлов загружено: {total_files}, строк записано: {total_rows}")

if __name__ == "__main__":
    main()
