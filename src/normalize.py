import re
import pandas as pd


def normalize_column_names(df):
    # приводим названия колонок к нижнему регистру и заменяем пробелы/спецсимволы на _
    new_cols = {}
    for col in df.columns:
        new_name = col.lower()
        new_name = re.sub(r"[^a-z0-9]+", "_", new_name)
        new_name = new_name.strip("_")
        new_cols[col] = new_name
    df = df.rename(columns=new_cols)
    return df


def clean_text_columns(df):
    # для всех строковых колонок делаем strip и заменяем пустые/nan на "unknown"
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"nan": "unknown", "NaN": "unknown", "": "unknown", "None": "unknown"})
    return df


def fill_missing_values(df):
    # числовые пропуски заполняем медианой, текстовые — "unknown"
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            median = df[col].median()
            df[col] = df[col].fillna(median)
        else:
            df[col] = df[col].fillna("unknown")
    return df


def normalize_road_accidents(path):
    print(f"Читаем файл: {path}")
    # encoding='utf-8-sig' убирает BOM-символ в начале файла
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    print(f"Размер до нормализации: {df.shape}")

    df = normalize_column_names(df)
    df = df.drop_duplicates()
    df = clean_text_columns(df)
    df = fill_missing_values(df)

    # приводим дату к datetime (колонка называется accident_date после нормализации)
    if "accident_date" in df.columns:
        df["accident_date"] = pd.to_datetime(df["accident_date"], dayfirst=True, errors="coerce")

    # severity приводим к единому виду (Title Case)
    if "accident_severity" in df.columns:
        df["accident_severity"] = df["accident_severity"].str.title()

    print(f"Размер после нормализации: {df.shape}")
    return df


def normalize_world_events(path):
    print(f"Читаем файл: {path}")
    df = pd.read_csv(path, encoding="utf-8-sig")
    print(f"Размер до нормализации: {df.shape}")

    df = normalize_column_names(df)
    df = df.drop_duplicates()
    df = clean_text_columns(df)
    df = fill_missing_values(df)

    # year оставляем строкой, т.к. содержит значения вида "2600 BC"
    # outcome приводим к Title Case для единообразия
    if "outcome" in df.columns:
        df["outcome"] = df["outcome"].str.title()

    print(f"Размер после нормализации: {df.shape}")
    return df
