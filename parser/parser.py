from __future__ import annotations

import io
import re
import datetime as dt
from typing import Any, Dict, List, Optional

import pandas as pd

# --- Канон: как мы называем нужные поля в итоговом DataFrame ---
CANON = {
    "exchange_product_id":   "Код Инструмента",
    "exchange_product_name": "Наименование Инструмента",
    "delivery_basis_name":   "Базис поставки",
    "volume":                "Объем Договоров в единицах измерения",
    "total":                 "Объем Договоров, руб.",
    "count":                 "Количество Договоров, шт.",
}

# Слабая нормализация заголовков (регистр/пробелы/вариации)
SYNONYMS = {
    "код инструмента": CANON["exchange_product_id"],
    "наименование инструмента": CANON["exchange_product_name"],
    "базис поставки": CANON["delivery_basis_name"],
    "объем договоров в единицах измерения": CANON["volume"],
    "объем договоров, руб.": CANON["total"],
    "объем договоров руб.": CANON["total"],
    "количество договоров, шт.": CANON["count"],
    "количество договоров шт.": CANON["count"],
}

# Маркер нужного блока и распознавание даты торгов
TON_MARKER = "Единица измерения: Метрическая тонна"
DATE_RE = re.compile(r"(\d{2}\.\d{2}\.\d{4})")  # 27.08.2025

# ---------- ВСПОМОГАТЕЛЬНЫЕ УТИЛИТЫ ----------

def _norm_text(x: Any) -> str:
    s = str(x or "")
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _norm_lower(x: Any) -> str:
    return _norm_text(x).lower()

def _norm_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Нормализуем имена столбцов к ожидаемым русским названиям."""
    cols = []
    for c in df.columns:
        key = _norm_lower(c)
        cols.append(SYNONYMS.get(key) or _norm_text(c))
    out = df.copy()
    out.columns = cols
    return out

def _row_has_header_keywords(row_vals: List[str]) -> bool:
    row_l = [_norm_lower(v) for v in row_vals]
    return any("код инструмента" in v for v in row_l)

def _is_next_section_marker(val_in_col_c: Any) -> bool:
    s = _norm_lower(val_in_col_c)
    return (
        "единица измерения" in s
        or "секция биржи" in s
        or s == ""  # полностью пустая ячейка в C — хороший кандидат на разрыв секции
    )

def _clean_numbers(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
              .str.replace("\xa0", "", regex=False)
              .str.replace(" ",   "", regex=False)
              .str.replace(",",   ".", regex=False)
              .pipe(pd.to_numeric, errors="coerce")
    )

# ---------- ОСНОВНАЯ ФУНКЦИЯ ----------

def parse_bulletin_xls(content: bytes, fallback_date: Optional[dt.date] = None) -> pd.DataFrame:
    """
    Извлекает из .xls таблицу, которая следует сразу ПОСЛЕ строки
    в колонке C со строкой 'Единица измерения: Метрическая тонна'.
    Возвращает DataFrame с колонками:
    exchange_product_id, exchange_product_name, delivery_basis_name, volume, total, count, date.
    """
    # Читаем все листы «как есть» (без заголовка), движок для .xls — xlrd
    sheets = pd.read_excel(io.BytesIO(content), sheet_name=None, header=None, engine="xlrd")

    # 1) Дата торгов из верхних строк (если есть)
    bulletin_date: Optional[dt.date] = None
    for sh in sheets.values():
        blob = " ".join(sh.astype(str).head(30).values.ravel())
        m = DATE_RE.search(blob)
        if m:
            bulletin_date = dt.datetime.strptime(m.group(1), "%d.%m.%Y").date()
            break
    if bulletin_date is None:
        bulletin_date = fallback_date or dt.date.today()

    blocks: List[pd.DataFrame] = []

    for raw in sheets.values():
        if raw.empty:
            continue

        n_rows, n_cols = raw.shape[0], raw.shape[1]
        # В колонке C (индекс 2) ищем маркеры «Единица измерения: Метрическая тонна»
        col_c = raw.iloc[:, 2] if n_cols > 2 else pd.Series([], dtype=object)
        marker_rows = col_c[col_c.astype(str).str.contains(TON_MARKER, case=False, regex=False, na=False)].index.tolist()

        # На некоторых файлах маркер может быть не в C (слияния ячеек/экспорт).
        # Сделаем мягкий fallback: если в C не нашли — ищем по всей строке.
        if not marker_rows:
            mask_any = raw.apply(
                lambda r: any(TON_MARKER.lower() in _norm_lower(x) for x in r),
                axis=1
            )
            marker_rows = raw.index[mask_any].tolist()

        for r in marker_rows:
            # 2) Найти строку-шапку: ближайшая ниже строка, где есть «Код Инструмента»
            header_row = None
            headers: List[str] = []
            for i in range(r + 1, min(r + 30, n_rows)):  # окно поиска шапки
                row_vals = [_norm_text(x) for x in raw.iloc[i].tolist()]
                if _row_has_header_keywords(row_vals):
                    header_row = i
                    headers = row_vals
                    break
            if header_row is None:
                continue  # не нашли шапку — пропускаем этот маркер

            # 3) Конец таблицы: до следующего маркера/секции/полностью пустой полосы
            end_row = None
            for j in range(header_row + 1, n_rows):
                val_c = raw.iat[j, 2] if n_cols > 2 else ""
                if _is_next_section_marker(val_c):
                    end_row = j
                    break
                # стоп-условие: три подряд полностью пустые строки
                if j + 2 < n_rows:
                    if all(raw.iloc[k].isna().all() for k in (j, j+1, j+2)):
                        end_row = j
                        break
            if end_row is None:
                end_row = n_rows

            # 4) Собираем блок и нормализуем заголовки
            block = raw.iloc[header_row + 1 : end_row].copy()
            block.columns = headers
            block = _norm_headers(block)
            block = block.dropna(how="all").dropna(axis=1, how="all")

            # 5) Оставляем только интересующие столбцы
            need_ru = [
                CANON["exchange_product_id"],
                CANON["exchange_product_name"],
                CANON["delivery_basis_name"],
                CANON["volume"],
                CANON["total"],
                CANON["count"],
            ]
            has = [c for c in need_ru if c in block.columns]
            if len(has) < 5:
                continue

            df = block[has].copy()

            # 6) Приведение чисел
            if CANON["volume"] in df.columns:
                df[CANON["volume"]] = _clean_numbers(df[CANON["volume"]])
            if CANON["total"] in df.columns:
                df[CANON["total"]]  = _clean_numbers(df[CANON["total"]])
            if CANON["count"] in df.columns:
                df[CANON["count"]]  = _clean_numbers(df[CANON["count"]])

            # 7) Фильтр: Количество договоров > 0
            if CANON["count"] in df.columns:
                df = df[df[CANON["count"]].fillna(0) > 0]

            # 8) Переименуем в канон-ключи и добавим дату
            rename = {v: k for k, v in CANON.items() if v in df.columns}
            df = df.rename(columns=rename)
            df["date"] = bulletin_date

            # 9) Очистим мусор: пустые коды
            df = df[df["exchange_product_id"].notna()]
            df["exchange_product_id"] = df["exchange_product_id"].astype(str).str.strip()

            if not df.empty:
                blocks.append(df)

    if not blocks:
        raise ValueError("Не удалось извлечь таблицу «Единица измерения: Метрическая тонна» (или нет строк с count>0).")

    out = pd.concat(blocks, ignore_index=True)

    # гарантируем наличие всех ожидаемых колонок
    for k in ["exchange_product_id","exchange_product_name","delivery_basis_name","volume","total","count","date"]:
        if k not in out.columns:
            out[k] = None

    return out.reset_index(drop=True)

# ---------- УТИЛИТЫ ДЛЯ ЗАПИСИ В БД ----------

def split_product_id(epid: str) -> Dict[str, str]:
    """oil_id = [:4], delivery_basis_id = [4:7], delivery_type_id = [-1]."""
    s = str(epid or "")
    return {
        "oil_id":            s[:4] if len(s) >= 4 else s,
        "delivery_basis_id": s[4:7] if len(s) >= 7 else "",
        "delivery_type_id":  s[-1]  if s else "",
    }

def to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Преобразуем DataFrame в список словарей для upsert-а в БД."""
    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        parts = split_product_id(row.get("exchange_product_id", ""))
        rows.append({
            "exchange_product_id":   row.get("exchange_product_id"),
            "exchange_product_name": row.get("exchange_product_name"),
            "delivery_basis_name":   row.get("delivery_basis_name"),
            "oil_id":                parts["oil_id"],
            "delivery_basis_id":     parts["delivery_basis_id"],
            "delivery_type_id":      parts["delivery_type_id"],
            "volume": float(row.get("volume") or 0.0),
            "total":  float(row.get("total")  or 0.0),
            "count":  int(row.get("count")   or 0),
            "date":   row.get("date"),
        })
    return rows
