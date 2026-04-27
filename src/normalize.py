# Модуль нормализации данных.
# Читает сырые CSV-файлы, очищает и приводит к единому виду.

import re
import pandas as pd


def normalize_column_names(df):
    """Приводит названия колонок к snake_case: нижний регистр, пробелы и спецсимволы заменяются на _."""
    new_cols = {}
    for col in df.columns:
        new_name = col.lower()
        new_name = re.sub(r"[^a-z0-9]+", "_", new_name)  # всё кроме букв и цифр → _
        new_name = re.sub(r"_+", "_", new_name)           # несколько _ подряд → один
        new_name = new_name.strip("_")                     # убираем _ в начале и конце
        new_cols[col] = new_name
    return df.rename(columns=new_cols)


def clean_text_columns(df, lower=True):
    """Убирает лишние пробелы в текстовых колонках, при lower=True переводит в нижний регистр.

    Использует pandas str-методы, которые корректно работают с NaN — не превращают их в строку 'nan'.
    """
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()
        if lower:
            df[col] = df[col].str.lower()
        df[col] = df[col].replace("", "unknown")  # пустые строки после strip → unknown
    return df


def fill_missing_values(df):
    """Заполняет пропуски: в текстовых колонках ставит 'unknown', числовые не трогает."""
    for col in df.columns:
        if not pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna("unknown")
    return df


def normalize_road_accidents(path):
    """Нормализует датасет дорожных аварий: очищает колонки, удаляет дубликаты, парсит дату."""
    print(f"  Читаем: {path}")
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)  # utf-8-sig убирает BOM в начале файла

    rows_before = len(df)
    missing_before = int(df.isnull().sum().sum())
    print(f"  Строк: {rows_before}, колонок: {len(df.columns)}, пропусков: {missing_before}")

    df = normalize_column_names(df)

    dupes = int(df.duplicated().sum())
    df = df.drop_duplicates()
    if dupes > 0:
        print(f"  Удалено дубликатов: {dupes}")

    df = clean_text_columns(df, lower=True)  # все текстовые поля в нижний регистр для единообразия
    df = fill_missing_values(df)

    # приводим дату аварии к типу datetime
    if "accident_date" in df.columns:
        df["accident_date"] = pd.to_datetime(
            df["accident_date"], dayfirst=True, errors="coerce"
        )

    missing_after = int(df.isnull().sum().sum())
    print(f"  После нормализации: {df.shape}, пропусков осталось: {missing_after}")
    return df


def normalize_world_events(path):
    """Нормализует датасет исторических событий: очищает колонки, убирает мусорные вставки 'Unknown'."""
    print(f"  Читаем: {path}")
    df = pd.read_csv(path, encoding="utf-8-sig")

    rows_before = len(df)
    missing_before = int(df.isnull().sum().sum())
    print(f"  Строк: {rows_before}, колонок: {len(df.columns)}, пропусков: {missing_before}")

    df = normalize_column_names(df)

    dupes = int(df.duplicated().sum())
    df = df.drop_duplicates()
    if dupes > 0:
        print(f"  Удалено дубликатов: {dupes}")

    df = clean_text_columns(df, lower=False)  # регистр сохраняем — исторические названия важны

    # в данных встречаются мусорные вставки слова "Unknown" внутри значений,
    # например "QutbUnknownudUnknowndin Aibak" вместо "Qutubuddin Aibak"
    # удаляем подстроку "Unknown" из всех текстовых полей
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.replace("Unknown", "", regex=False).str.strip()
        df[col] = df[col].replace("", "unknown")  # если осталась пустая строка → unknown

    df = fill_missing_values(df)

    # колонки year, date и month не приводим к datetime:
    # там присутствуют значения вида "2600 BC", "Unknown" и исторические записи без точных дат
    missing_after = int(df.isnull().sum().sum())
    print(f"  После нормализации: {df.shape}, пропусков осталось: {missing_after}")
    return df
